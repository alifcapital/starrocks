// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "io/shared_buffered_input_stream.h"

#include <gutil/strings/substitute.h>

#include "common/config.h"
#include "gutil/strings/fastmem.h"
#include "runtime/current_thread.h"
#include "util/runtime_profile.h"

namespace starrocks::io {

SharedBufferedInputStream::SharedBufferedInputStream(std::shared_ptr<SeekableInputStream> stream, std::string filename,
                                                     size_t file_size)
        : _stream(std::move(stream)), _filename(std::move(filename)), _file_size(file_size) {}

void SharedBufferedInputStream::SharedBuffer::align(int64_t align_size, int64_t file_size) {
    if (align_size != 0) {
        offset = raw_offset / align_size * align_size;
        int64_t end = std::min((raw_offset + raw_size + align_size - 1) / align_size * align_size, file_size);
        size = end - offset;
    } else {
        offset = raw_offset;
        size = raw_size;
    }
}

std::string SharedBufferedInputStream::SharedBuffer::debug_string() const {
    return strings::Substitute(
            "SharedBuffer raw_offset=$0, raw_size=$1, offset=$2, size=$3, ref_count=$4, buffer_capacity=$5", raw_offset,
            raw_size, offset, size, ref_count, buffer.capacity());
}

Status SharedBufferedInputStream::_sort_and_check_overlap(std::vector<IORange>& ranges) {
    // specify compare function is important. suppose we have zero range like [351,351],[351,356].
    // If we don't specify compare function, we may have [351,356],[351,351] which is bad order.
    std::sort(ranges.begin(), ranges.end(), [](const IORange& a, const IORange& b) {
        if (a.offset != b.offset) {
            return a.offset < b.offset;
        }
        return a.size < b.size;
    });

    // check io range is not overlapped.
    for (size_t i = 1; i < ranges.size(); i++) {
        if (ranges[i].offset < (ranges[i - 1].offset + ranges[i - 1].size)) {
            LOG(WARNING) << "io ranges are overlapped" << ranges[i].offset << " "
                         << ranges[i - 1].offset + ranges[i - 1].size;
            return Status::RuntimeError("io ranges are overlapped");
        }
    }

    return Status::OK();
}

void SharedBufferedInputStream::_merge_small_ranges(const std::vector<IORange>& small_ranges) {
    if (small_ranges.size() > 0) {
        auto update_map = [&](size_t from, size_t to) {
            // merge from [unmerge, i-1]
            int64_t ref_count = (to - from + 1);
            int64_t end = (small_ranges[to].offset + small_ranges[to].size);
            SharedBufferPtr sb(new SharedBuffer{.raw_offset = small_ranges[from].offset,
                                                .raw_size = end - small_ranges[from].offset,
                                                .ref_count = ref_count});
            sb->align(_align_size, _file_size);
            _map.insert(std::make_pair(sb->raw_offset + sb->raw_size, sb));
        };

        size_t unmerge = 0;
        for (size_t i = 1; i < small_ranges.size(); i++) {
            const auto& prev = small_ranges[i - 1];
            const auto& now = small_ranges[i];
            size_t now_end = now.offset + now.size;
            size_t prev_end = prev.offset + prev.size;
            if (((now_end - small_ranges[unmerge].offset) <= _options.max_buffer_size) &&
                (now.offset - prev_end) <= _options.max_dist_size) {
                continue;
            } else {
                update_map(unmerge, i - 1);
                unmerge = i;
            }
        }
        update_map(unmerge, small_ranges.size() - 1);
    }
}

void SharedBufferedInputStream::_add_large_range(const IORange& r) {
    // We want a reader that touches only a few pages of a large range not to load the whole range, so we cut
    // the range into consecutive pieces of at most max_buffer_size and load each piece on its first access.
    // With an align size we cut at multiples of it: we expect the aligned pieces not to overlap, so no block
    // is loaded twice. If max_buffer_size is smaller than the align size, we cannot do it and keep one buffer.
    int64_t step = 0;
    if (config::io_coalesce_split_large_range) {
        step = _align_size > 0 ? _options.max_buffer_size / _align_size * _align_size : _options.max_buffer_size;
    }

    const int64_t range_end = r.offset + r.size;
    int64_t begin = r.offset;
    while (begin < range_end) {
        int64_t end = range_end;
        if (step > 0) {
            const int64_t piece_start = _align_size > 0 ? begin / _align_size * _align_size : begin;
            end = std::min(range_end, piece_start + step);
        }
        SharedBufferPtr sb(new SharedBuffer{.raw_offset = begin,
                                            .raw_size = end - begin,
                                            .ref_count = 1,
                                            .large = true,
                                            .has_next_piece = end < range_end});
        sb->align(_align_size, _file_size);
        _map.insert(std::make_pair(end, sb));
        begin = end;
    }
}

Status SharedBufferedInputStream::_set_io_ranges_all_columns(const std::vector<IORange>& ranges) {
    if (ranges.size() == 0) {
        return Status::OK();
    }

    std::vector<IORange> check(ranges);
    RETURN_IF_ERROR(_sort_and_check_overlap(check));

    std::vector<IORange> small_ranges;
    for (const IORange& r : check) {
        if (r.size > _options.max_buffer_size) {
            _add_large_range(r);
        } else {
            small_ranges.emplace_back(r);
        }
    }

    _merge_small_ranges(small_ranges);
    _update_estimated_mem_usage();
    return Status::OK();
}

Status SharedBufferedInputStream::_set_io_ranges_active_and_lazy_columns(const std::vector<IORange>& ranges) {
    if (ranges.size() == 0) {
        return Status::OK();
    }

    // specify compare function is important. suppose we have zero range like [351,351],[351,356].
    // If we don't specify compare function, we may have [351,356],[351,351] which is bad order.
    std::vector<IORange> check(ranges);
    RETURN_IF_ERROR(_sort_and_check_overlap(check));

    std::vector<IORange> small_active_ranges;
    std::vector<bool> small_lazy_flag(ranges.size());
    small_lazy_flag.assign(ranges.size(), false);
    for (auto index = 0; index < check.size(); ++index) {
        const IORange& r = check[index];
        if (r.size > _options.max_buffer_size) {
            _add_large_range(r);
        } else {
            if (r.is_active) {
                small_active_ranges.emplace_back(r);
            } else {
                small_lazy_flag[index] = true;
            }
        }
    }

    if (small_active_ranges.size() > 0) {
        _merge_small_ranges(small_active_ranges);
    }

    std::vector<IORange> small_lazy_batch_ranges;
    for (auto index = 0; index < small_lazy_flag.size(); ++index) {
        if (!small_lazy_flag[index]) {
            // active column or big column
            continue;
        } else {
            // 1. there may be lazy_column locate in the middle of two active_columns,
            // such as active_column, lazy_column, active_column,
            // that two active_columns have merged and the lazy_column had be contained.
            const IORange& r = check[index];
            auto iter = _map.upper_bound(r.offset);
            if (iter != _map.end()) {
                SharedBufferPtr& sb = iter->second;
                if (sb->offset <= r.offset && sb->offset + sb->size >= r.offset + r.size) {
                    sb->ref_count++;
                    continue;
                }
            }
            small_lazy_batch_ranges.emplace_back(r);
            // 2. there also may be active_column locate in the middle of two lazy_columns,
            // in this case active_column may be contained in two shared_buffer，
            // we should prevent that
            if (index + 1 >= small_lazy_flag.size() || !small_lazy_flag[index + 1]) {
                _merge_small_ranges(small_lazy_batch_ranges);
                small_lazy_batch_ranges.clear();
            }
        }
    }

    _update_estimated_mem_usage();
    return Status::OK();
}

Status SharedBufferedInputStream::set_io_ranges(const std::vector<IORange>& ranges, bool coalesce_lazy_column) {
    if (coalesce_lazy_column || !config::io_coalesce_adaptive_lazy_active) {
        return _set_io_ranges_all_columns(ranges);
    } else {
        return _set_io_ranges_active_and_lazy_columns(ranges);
    }
}

StatusOr<SharedBufferedInputStream::SharedBufferPtr> SharedBufferedInputStream::find_shared_buffer(size_t offset,
                                                                                                   size_t count) {
    auto iter = _map.upper_bound(offset);
    if (iter == _map.end()) {
        return Status::RuntimeError("failed to find shared buffer based on offset");
    }
    const SharedBufferPtr& sb = iter->second;
    if ((sb->offset > offset) || (sb->offset + sb->size) < (offset + count)) {
        return Status::RuntimeError("bad construction of shared buffer");
    }
    return sb;
}

// find_shared_buffer() takes the first buffer whose key (the end of its raw range) is above the offset, and then
// checks that the buffer covers the bytes. We want every offset that `sb` serves to find the new buffer that
// covers it, and every offset in a hole between the ranges to find a buffer that does not cover it. So the raw
// range of a new buffer is the part of the raw range of `sb` that its range covers. The new keys grow with the
// ranges and are never above the key of `sb`, so they do not take offsets from the buffers around `sb`.
Status SharedBufferedInputStream::split_shared_buffer(const SharedBufferPtr& sb, const std::vector<IORange>& ranges) {
    auto old_iter = _map.find(sb->raw_offset + sb->raw_size);
    if (old_iter == _map.end() || old_iter->second != sb) {
        return Status::InvalidArgument("the shared buffer is not registered in the stream");
    }
    if (sb->buffer.capacity() != 0) {
        return Status::InvalidArgument("the shared buffer is loaded already");
    }
    if (ranges.empty()) {
        return Status::InvalidArgument("no range to split the shared buffer to");
    }

    const int64_t raw_begin = sb->raw_offset;
    const int64_t raw_end = sb->raw_offset + sb->raw_size;
    const int64_t end = sb->offset + sb->size;
    int64_t prev_end = sb->offset;
    std::vector<SharedBufferPtr> new_buffers;
    new_buffers.reserve(ranges.size());
    for (size_t i = 0; i < ranges.size(); ++i) {
        const IORange& r = ranges[i];
        const int64_t r_end = r.offset + r.size;
        const int64_t new_raw_begin = std::max(r.offset, raw_begin);
        const int64_t new_raw_end = std::min(r_end, raw_end);
        if (r.size <= 0 || r.offset < prev_end || r_end > end || new_raw_begin >= new_raw_end) {
            return Status::InvalidArgument(strings::Substitute("bad range [$0, $1) to split the shared buffer to, $2",
                                                               r.offset, r_end, sb->debug_string()));
        }
        // The key of `sb` is free, because we erase `sb` before we insert the new buffers.
        auto existing = _map.find(new_raw_end);
        if (existing != _map.end() && existing != old_iter) {
            return Status::InvalidArgument(strings::Substitute("a shared buffer with the key $0 exists already, $1",
                                                               new_raw_end, existing->second->debug_string()));
        }
        // We do not know which io ranges of `sb` fall into which new buffer, so each new buffer keeps the ref_count
        // of `sb` as an upper bound. The new buffers load a part of `sb`, so they keep the large flag of `sb` and
        // the bytes they load are counted as the bytes of a large buffer.
        // The next piece of the large io range follows the new buffer only if no hole is between them: the new buffer
        // reaches the end of `sb` and `sb` has a next piece, or the next new buffer begins where this one ends.
        // A read does not cross a hole, because the caller reads the bytes of the hole by other means.
        const bool has_next_piece = sb->large && (i + 1 < ranges.size() ? ranges[i + 1].offset == r_end
                                                                        : sb->has_next_piece && r_end == end);
        new_buffers.emplace_back(new SharedBuffer{.raw_offset = new_raw_begin,
                                                  .raw_size = new_raw_end - new_raw_begin,
                                                  .offset = r.offset,
                                                  .size = r.size,
                                                  .ref_count = sb->ref_count,
                                                  .large = sb->large,
                                                  .has_next_piece = has_next_piece,
                                                  .split = true});
        prev_end = r_end;
    }

    _map.erase(old_iter);
    for (const SharedBufferPtr& new_buffer : new_buffers) {
        _map.emplace(new_buffer->raw_offset + new_buffer->raw_size, new_buffer);
    }
    return Status::OK();
}

Status SharedBufferedInputStream::find_shared_pieces(size_t offset, size_t count,
                                                     std::vector<SharedBufferPtr>* pieces) {
    const size_t end = offset + count;
    size_t cursor = offset;
    // We walk over the buffers while each one starts where the previous one ends and the previous one has a next
    // piece, so we never join buffers of different io ranges.
    for (auto iter = _map.upper_bound(offset); iter != _map.end(); ++iter) {
        const SharedBufferPtr& sb = iter->second;
        const size_t sb_end = sb->offset + sb->size;
        if (sb->offset > cursor || sb_end <= cursor) {
            break;
        }
        if (pieces != nullptr) {
            pieces->push_back(sb);
        }
        if (sb_end >= end) {
            return Status::OK();
        }
        if (!sb->has_next_piece) {
            break;
        }
        cursor = sb_end;
    }
    return Status::RuntimeError("bad construction of shared buffer");
}

Status SharedBufferedInputStream::get_bytes(const uint8_t** buffer, size_t offset, size_t count,
                                            SharedBufferPtr shared_buffer) {
    if (!shared_buffer) {
        ASSIGN_OR_RETURN(auto ret, find_shared_buffer(offset, count));
        shared_buffer = ret;
    }

    SharedBuffer& sb = *shared_buffer;
    if (sb.buffer.capacity() == 0) {
        RETURN_IF_ERROR(CurrentThread::mem_tracker()->check_mem_limit("read into shared buffer"));
        SCOPED_RAW_TIMER(&_shared_io_timer);
        _shared_io_count += 1;
        _shared_io_bytes += sb.size;
        if (sb.large) {
            _large_io_count += 1;
            _large_io_bytes += sb.size;
        }
        if (sb.size > sb.raw_size) {
            // after called _deduplicate_shared_buffer(), sb.size maybe is larger than sb.raw_size
            // we will count how many extra bytes we read because of alignment.
            _shared_align_io_bytes += sb.size - sb.raw_size;
        }
        sb.buffer.reserve(sb.size);
        RETURN_IF_ERROR(_stream->read_at_fully(sb.offset, sb.buffer.data(), sb.size));
    }
    if (sb.large) {
        _large_used_bytes += count;
    }
    *buffer = sb.buffer.data() + offset - sb.offset;
    return Status::OK();
}

void SharedBufferedInputStream::release() {
    _map.clear();
}

void SharedBufferedInputStream::release_to_offset(int64_t offset) {
    auto it = _map.upper_bound(offset);
    _map.erase(_map.begin(), it);
}

Status SharedBufferedInputStream::read_at_fully(int64_t offset, void* out, int64_t count) {
    auto st = find_shared_buffer(offset, count);
    if (!st.ok()) {
        std::vector<SharedBufferPtr> pieces;
        if (find_shared_pieces(offset, count, &pieces).ok()) {
            // The read crosses the border of two pieces of a large range. We expect the neighbour pages to
            // need these pieces too, so we serve the read from the pieces and do not read the bytes again
            // with a direct io.
            char* dst = static_cast<char*>(out);
            int64_t cursor = offset;
            for (const SharedBufferPtr& piece : pieces) {
                const int64_t n = std::min<int64_t>(offset + count, piece->offset + piece->size) - cursor;
                const uint8_t* buffer = nullptr;
                RETURN_IF_ERROR(get_bytes(&buffer, cursor, n, piece));
                strings::memcpy_inlined(dst, buffer, n);
                dst += n;
                cursor += n;
            }
            return Status::OK();
        }
        SCOPED_RAW_TIMER(&_direct_io_timer);
        _direct_io_count += 1;
        _direct_io_bytes += count;
        RETURN_IF_ERROR(_stream->read_at_fully(offset, out, count));
        return Status::OK();
    }
    const uint8_t* buffer = nullptr;
    RETURN_IF_ERROR(get_bytes(&buffer, offset, count, st.value()));
    strings::memcpy_inlined(out, buffer, count);
    return Status::OK();
}

StatusOr<int64_t> SharedBufferedInputStream::get_size() {
    return _file_size;
}

StatusOr<int64_t> SharedBufferedInputStream::read(void* data, int64_t count) {
    auto n = _stream->read_at(_offset, data, count);
    RETURN_IF_ERROR(n);
    _offset += n.value();
    return n;
}

StatusOr<std::string_view> SharedBufferedInputStream::peek(int64_t count) {
    ASSIGN_OR_RETURN(auto ret, find_shared_buffer(_offset, count));
    if (ret->buffer.capacity() == 0) return Status::NotSupported("peek shared buffer empty");
    const uint8_t* buf = nullptr;
    RETURN_IF_ERROR(get_bytes(&buf, _offset, count, ret));
    return std::string_view((const char*)buf, count);
}

StatusOr<std::string_view> SharedBufferedInputStream::peek_shared_buffer(int64_t count,
                                                                         SharedBufferPtr* shared_buffer) {
    ASSIGN_OR_RETURN(auto ret, find_shared_buffer(_offset, count));
    if (ret->buffer.capacity() == 0) return Status::NotSupported("peek shared buffer empty");
    // This path does not call get_bytes, so we count the bytes the reader gets from a large buffer here.
    if (ret->large) {
        _large_used_bytes += count;
    }
    const uint8_t* buf = ret->buffer.data() + _offset - ret->offset;
    if (shared_buffer) {
        *shared_buffer = ret;
    }
    return std::string_view((const char*)buf, count);
}

void SharedBufferedInputStream::_update_estimated_mem_usage() {
    int64_t mem_usage = 0;
    for (const auto& [_, sb] : _map) {
        mem_usage += sb->size;
    }
    // in most cases, those data are compressed.
    // to read it, we need to decompress it, and let's say to add 50% overhead.
    mem_usage += mem_usage / 2;
    _estimated_mem_usage = std::max(mem_usage, _estimated_mem_usage);
}

int64_t SharedBufferedInputStream::current_range_ref_sum() const {
    int64_t ref = 0;
    for (const auto& [_, sb] : _map) {
        ref += sb->ref_count;
    }
    return ref;
}

// Out-of-line so gcov can attribute coverage to a concrete `.cpp` line.
IoStatsSnapshot SharedBufferedInputStream::get_io_stats_snapshot() const {
    return _stream->get_io_stats_snapshot();
}

} // namespace starrocks::io
