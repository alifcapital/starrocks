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

#include <algorithm>
#include <vector>

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

namespace {

// Sorts the ranges, clips them to [begin, end), drops the empty ones, and joins the ones that overlap or touch. We
// expect the provider to answer in the order of the file for most buffers, so the sort runs only for a range list that
// is not in order.
std::vector<SharedBufferedInputStream::IORange> normalize_ranges(std::vector<SharedBufferedInputStream::IORange> ranges,
                                                                 int64_t begin, int64_t end) {
    auto by_offset = [](const SharedBufferedInputStream::IORange& a, const SharedBufferedInputStream::IORange& b) {
        return a.offset < b.offset;
    };
    if (!std::is_sorted(ranges.begin(), ranges.end(), by_offset)) {
        std::sort(ranges.begin(), ranges.end(), by_offset);
    }
    std::vector<SharedBufferedInputStream::IORange> result;
    result.reserve(ranges.size());
    for (const auto& range : ranges) {
        const int64_t range_begin = std::max(range.offset, begin);
        const int64_t range_end = std::min(range.offset + range.size, end);
        if (range_begin >= range_end) {
            continue;
        }
        if (!result.empty() && range_begin <= result.back().offset + result.back().size) {
            result.back().size = std::max(result.back().offset + result.back().size, range_end) - result.back().offset;
        } else {
            result.emplace_back(range_begin, range_end - range_begin);
        }
    }
    return result;
}

// The ranges of [begin, end) that a buffer loads when the reader gets `skips` from elsewhere. `skips` are normalized
// to [begin, end) (see normalize_ranges()).
// - The bytes of [need_begin, need_end) are never cut.
// - With an align size we cut only at the borders of the blocks, because the data cache reads and writes whole blocks:
//   a skip range gives up its partial blocks at both ends, and a block with one byte to load is loaded whole.
// - Two runs are joined while the gap between them is at most max_gap and the joined range is at most
//   max_buffer_size. A run that is larger than max_buffer_size by itself stays one range.
std::vector<SharedBufferedInputStream::IORange> kept_ranges(
        int64_t begin, int64_t end, int64_t align_size, const std::vector<SharedBufferedInputStream::IORange>& skips,
        int64_t need_begin, int64_t need_end, int64_t max_gap, int64_t max_buffer_size) {
    std::vector<SharedBufferedInputStream::IORange> cuts;
    for (const auto& skip : skips) {
        const int64_t skip_end = skip.offset + skip.size;
        if (need_begin >= need_end || need_end <= skip.offset || need_begin >= skip_end) {
            cuts.push_back(skip);
            continue;
        }
        if (skip.offset < need_begin) {
            cuts.emplace_back(skip.offset, need_begin - skip.offset);
        }
        if (need_end < skip_end) {
            cuts.emplace_back(need_end, skip_end - need_end);
        }
    }

    if (align_size > 0) {
        std::vector<SharedBufferedInputStream::IORange> aligned_cuts;
        for (const auto& cut : cuts) {
            const int64_t cut_end = cut.offset + cut.size;
            // The ends of the buffer are borders of blocks, or the end of the file.
            const int64_t aligned_begin =
                    cut.offset == begin ? begin : (cut.offset + align_size - 1) / align_size * align_size;
            const int64_t aligned_end = cut_end == end ? end : cut_end / align_size * align_size;
            if (aligned_begin < aligned_end) {
                aligned_cuts.emplace_back(aligned_begin, aligned_end - aligned_begin);
            }
        }
        cuts.swap(aligned_cuts);
    }

    std::vector<SharedBufferedInputStream::IORange> runs;
    auto add_run = [&](int64_t run_begin, int64_t run_end) {
        if (run_begin < run_end) {
            runs.emplace_back(run_begin, run_end - run_begin);
        }
    };
    int64_t cursor = begin;
    for (const auto& cut : cuts) {
        add_run(cursor, cut.offset);
        cursor = cut.offset + cut.size;
    }
    add_run(cursor, end);

    std::vector<SharedBufferedInputStream::IORange> ranges;
    for (const auto& run : runs) {
        if (!ranges.empty()) {
            auto& last = ranges.back();
            const int64_t merged_size = run.offset + run.size - last.offset;
            if (run.offset - (last.offset + last.size) <= max_gap && merged_size <= max_buffer_size) {
                last.size = merged_size;
                continue;
            }
        }
        ranges.push_back(run);
    }
    return ranges;
}

} // namespace

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
Status SharedBufferedInputStream::split_shared_buffer(const SharedBufferPtr& sb, const std::vector<IORange>& ranges,
                                                      bool is_final) {
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
                                                  .split = is_final,
                                                  .planned = sb->planned});
        prev_end = r_end;
    }

    _map.erase(old_iter);
    for (const SharedBufferPtr& new_buffer : new_buffers) {
        _map.emplace(new_buffer->raw_offset + new_buffer->raw_size, new_buffer);
    }
    return Status::OK();
}

// A read is about to load `sb`, and we fear that the buffer holds bytes that the reader gets elsewhere. We ask the
// provider once, and keep as runs the bytes that the provider does not name. A read of a cut out byte finds no buffer
// and goes direct, so a byte that the provider named but the reader needs after all costs one more request and is
// never wrong.
bool SharedBufferedInputStream::plan_load(const SharedBufferPtr& sb, int64_t need_offset, int64_t need_size) {
    if (_skip_ranges_provider == nullptr || sb->planned || sb->buffer.capacity() != 0 || sb->size <= 0 ||
        !_skip_ranges_provider->enabled()) {
        return false;
    }
    sb->planned = true;
    _skip_ranges_plan_count += 1;
    SCOPED_RAW_TIMER(&_skip_ranges_timer);

    std::vector<IORange> skips;
    _skip_ranges_provider->find_skip_ranges(sb->offset, sb->size, &skips);

    // The provider can read through this stream, for example to get an index of the file, and that read can load or
    // split buffers. We look at the buffer again before we change anything.
    auto registered = _map.find(sb->raw_offset + sb->raw_size);
    if (registered == _map.end() || registered->second != sb) {
        return true;
    }
    if (skips.empty() || sb->buffer.capacity() != 0) {
        return false;
    }
    skips = normalize_ranges(std::move(skips), sb->offset, sb->offset + sb->size);
    if (skips.empty()) {
        return false;
    }
    for (const auto& named : skips) {
        _skip_ranges_named_bytes += named.size;
    }

    const std::vector<IORange> ranges =
            kept_ranges(sb->offset, sb->offset + sb->size, _align_size, skips, need_offset, need_offset + need_size,
                        config::io_coalesce_read_max_distance_size, _options.max_buffer_size);
    int64_t kept_bytes = 0;
    for (const auto& range : ranges) {
        kept_bytes += range.size;
    }
    if (ranges.empty() || kept_bytes == sb->size) {
        return false;
    }

    const int64_t skipped_bytes = sb->size - kept_bytes;
    Status st = split_shared_buffer(sb, ranges, sb->split);
    if (!st.ok()) {
        // The buffer is loaded whole then, which costs bytes but returns the right data.
        LOG(WARNING) << "Failed to split a shared buffer of " << _filename << ": " << st.message();
        return false;
    }
    _record_cut_spans(*sb, ranges);
    _skip_ranges_bytes += skipped_bytes;
    _skip_ranges_split_count += 1;
    return true;
}

void SharedBufferedInputStream::_record_cut_spans(const SharedBuffer& sb, const std::vector<IORange>& ranges) {
    // The reader asks for the bytes of the raw range only, so the bytes outside it are not worth a recovery.
    const int64_t raw_begin = sb.raw_offset;
    const int64_t raw_end = sb.raw_offset + sb.raw_size;
    auto add = [&](int64_t begin, int64_t end) {
        begin = std::max(begin, raw_begin);
        end = std::min(end, raw_end);
        if (begin < end) {
            _cut_spans[end] = begin;
        }
    };
    int64_t cursor = sb.offset;
    for (const IORange& range : ranges) {
        add(cursor, range.offset);
        cursor = range.offset + range.size;
    }
    add(cursor, sb.offset + sb.size);
}

// A read that no buffer covers lies in a cut span when the provider named the page and then lost it: a direct read
// of a page costs a request for the header and a request for the body, and the pages that follow cost the same. We
// read the span from the read to its end with one request instead, and the buffer serves the pages that follow. The
// buffer is not larger than max_buffer_size, with an align size it ends at a border of a block (unless the read
// ends later), and it never covers bytes of another buffer.
SharedBufferedInputStream::SharedBufferPtr SharedBufferedInputStream::_recover_cut_span(int64_t offset, int64_t count) {
    if (_cut_spans.empty() || count <= 0) {
        return nullptr;
    }
    // the first span that ends after the offset
    auto span = _cut_spans.upper_bound(offset);
    if (span == _cut_spans.end() || span->second > offset || span->first < offset + count) {
        return nullptr;
    }
    const int64_t span_begin = span->second;
    const int64_t span_end = span->first;
    int64_t end = std::min(span_end, offset + std::max(count, _options.max_buffer_size));
    if (end < span_end && _align_size > 0) {
        end = std::max(offset + count, end / _align_size * _align_size);
    }
    auto next = _map.upper_bound(offset);
    if (next != _map.end()) {
        if (next->second->offset <= offset) {
            return nullptr;
        }
        end = std::min(end, next->second->offset);
        if (end < offset + count) {
            return nullptr;
        }
    }
    if (_map.find(end) != _map.end()) {
        return nullptr;
    }

    SharedBufferPtr sb(new SharedBuffer{.raw_offset = offset,
                                        .raw_size = end - offset,
                                        .offset = offset,
                                        .size = end - offset,
                                        .ref_count = 1,
                                        .split = true,
                                        .planned = true});
    _map.emplace(end, sb);
    _cut_spans.erase(span);
    if (span_begin < offset) {
        _cut_spans.emplace(offset, span_begin);
    }
    if (end < span_end) {
        _cut_spans.emplace(span_end, end);
    }
    _skip_ranges_recover_count += 1;
    _skip_ranges_recover_bytes += end - offset;
    return sb;
}

bool SharedBufferedInputStream::_plan_pieces(std::vector<SharedBufferPtr>* pieces, size_t offset, size_t count) {
    bool changed = false;
    for (const SharedBufferPtr& piece : *pieces) {
        changed |= plan_load(piece, offset, count);
    }
    if (!changed) {
        return true;
    }
    pieces->clear();
    return find_shared_pieces(offset, count, pieces).ok();
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
        // The readers of a buffer take a capacity for a loaded buffer, and the data cache saves the bytes of a loaded
        // buffer. We fear that a read which fails leaves the reserved capacity behind, and then the next read serves
        // bytes that no one wrote. So we read into a vector of our own and give it to the buffer only after the read
        // succeeds. A failed read leaves the buffer unloaded, and the next read of the range loads it again.
        std::vector<uint8_t> loaded;
        loaded.reserve(sb.size);
        RETURN_IF_ERROR(_stream->read_at_fully(sb.offset, loaded.data(), sb.size));
        sb.buffer.swap(loaded);
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
    }
    if (sb.large) {
        _large_used_bytes += count;
    }
    *buffer = sb.buffer.data() + offset - sb.offset;
    return Status::OK();
}

void SharedBufferedInputStream::release() {
    _map.clear();
    _cut_spans.clear();
}

void SharedBufferedInputStream::release_to_offset(int64_t offset) {
    auto it = _map.upper_bound(offset);
    _map.erase(_map.begin(), it);
    _cut_spans.erase(_cut_spans.begin(), _cut_spans.upper_bound(offset));
}

Status SharedBufferedInputStream::read_at_fully(int64_t offset, void* out, int64_t count) {
    auto st = find_shared_buffer(offset, count);
    if (st.ok() && plan_load(st.value(), offset, count)) {
        // The buffer is replaced, and the bytes of the read stay in a buffer.
        st = find_shared_buffer(offset, count);
    }
    if (!st.ok()) {
        std::vector<SharedBufferPtr> pieces;
        if (find_shared_pieces(offset, count, &pieces).ok() && _plan_pieces(&pieces, offset, count)) {
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
        if (SharedBufferPtr recovered = _recover_cut_span(offset, count); recovered != nullptr) {
            const uint8_t* buffer = nullptr;
            RETURN_IF_ERROR(get_bytes(&buffer, offset, count, recovered));
            strings::memcpy_inlined(out, buffer, count);
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
