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
#include <mutex>

#include "common/config.h"
#include "gutil/strings/fastmem.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "util/runtime_profile.h"
#include "util/threadpool.h"

namespace starrocks::io {

// Global thread pool for S3 parallel reads (lazy singleton)
static std::once_flag s_parallel_read_pool_init_flag;
static std::unique_ptr<ThreadPool> s_parallel_read_thread_pool;

static ThreadPool* get_parallel_read_thread_pool() {
    std::call_once(s_parallel_read_pool_init_flag, []() {
        int max_threads = config::s3_read_thread_pool_size;
        if (max_threads <= 0) {
            return;
        }
        std::unique_ptr<ThreadPool> pool;
        Status status = ThreadPoolBuilder("s3_parallel_read_pool")
                                .set_min_threads(0)
                                .set_max_threads(max_threads)
                                .set_max_queue_size(max_threads * 4)
                                .set_idle_timeout(MonoDelta::FromMilliseconds(5000))
                                .build(&pool);
        if (status.ok()) {
            s_parallel_read_thread_pool = std::move(pool);
            LOG(INFO) << "S3 parallel read thread pool initialized with max_threads=" << max_threads;
        } else {
            LOG(WARNING) << "Failed to create S3 parallel read thread pool: " << status.message();
        }
    });
    return s_parallel_read_thread_pool.get();
}

SharedBufferedInputStream::SharedBufferedInputStream(std::shared_ptr<SeekableInputStream> stream, std::string filename,
                                                     size_t file_size)
        : _stream(std::move(stream)), _filename(std::move(filename)), _file_size(file_size) {}

SharedBufferedInputStream::~SharedBufferedInputStream() {
    // Wait for all active parallel read tasks to complete before destruction
    // This prevents use-after-free when callbacks try to access this object
    std::unique_lock<std::mutex> lock(_parallel_read_mutex);
    _shutting_down = true;
    _parallel_read_cv.wait(lock, [this]() { return _active_read_count == 0; });
}

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

Status SharedBufferedInputStream::_set_io_ranges_all_columns(const std::vector<IORange>& ranges) {
    if (ranges.size() == 0) {
        return Status::OK();
    }

    std::vector<IORange> check(ranges);
    RETURN_IF_ERROR(_sort_and_check_overlap(check));

    std::vector<IORange> small_ranges;
    for (const IORange& r : check) {
        if (r.size > _options.max_buffer_size) {
            SharedBufferPtr sb(new SharedBuffer{.raw_offset = r.offset, .raw_size = r.size, .ref_count = 1});
            sb->align(_align_size, _file_size);
            _map.insert(std::make_pair(sb->raw_offset + sb->raw_size, sb));
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
            SharedBufferPtr sb(new SharedBuffer{.raw_offset = r.offset, .raw_size = r.size, .ref_count = 1});
            sb->align(_align_size, _file_size);
            _map.insert(std::make_pair(sb->raw_offset + sb->raw_size, sb));
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
        RETURN_IF_ERROR(_set_io_ranges_all_columns(ranges));
    } else {
        RETURN_IF_ERROR(_set_io_ranges_active_and_lazy_columns(ranges));
    }

    // Start parallel reads for all SharedBuffers (sliding window)
    _start_parallel_reads();

    return Status::OK();
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

Status SharedBufferedInputStream::get_bytes(const uint8_t** buffer, size_t offset, size_t count,
                                            SharedBufferPtr shared_buffer) {
    if (!shared_buffer) {
        ASSIGN_OR_RETURN(auto ret, find_shared_buffer(offset, count));
        shared_buffer = ret;
    }

    SharedBuffer& sb = *shared_buffer;

    // Wait for parallel read to complete if it was started
    if (sb.read_started && sb.read_future.valid()) {
        SCOPED_RAW_TIMER(&_shared_io_timer);
        Status read_status = sb.read_future.get();
        sb.read_started = false;
        if (!read_status.ok()) {
            return read_status;
        }
        _shared_io_count += 1;
        _shared_io_bytes += sb.size;
        if (sb.size > sb.raw_size) {
            _shared_align_io_bytes += sb.size - sb.raw_size;
        }
    } else if (sb.buffer.capacity() == 0) {
        // Fallback: synchronous read if parallel read was not started
        RETURN_IF_ERROR(CurrentThread::mem_tracker()->check_mem_limit("read into shared buffer"));
        SCOPED_RAW_TIMER(&_shared_io_timer);
        _shared_io_count += 1;
        _shared_io_bytes += sb.size;
        if (sb.size > sb.raw_size) {
            _shared_align_io_bytes += sb.size - sb.raw_size;
        }
        sb.buffer.resize(sb.size);
        RETURN_IF_ERROR(_stream->read_at_fully(sb.offset, sb.buffer.data(), sb.size));
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

void SharedBufferedInputStream::_start_parallel_reads() {
    ThreadPool* pool = get_parallel_read_thread_pool();
    if (pool == nullptr) {
        return;
    }

    int32_t max_per_file = config::s3_parallel_read_workers_per_file;

    std::lock_guard<std::mutex> lock(_parallel_read_mutex);

    // Don't start new tasks if we're shutting down
    if (_shutting_down) {
        return;
    }

    for (auto& [_, sb] : _map) {
        if (_active_read_count >= max_per_file) {
            break;
        }
        if (sb->read_started) {
            continue;
        }

        _active_read_count++;
        sb->read_started = true;

        _submit_parallel_read(sb);
    }
}

void SharedBufferedInputStream::_submit_parallel_read(SharedBufferPtr sb) {
    auto promise = std::make_shared<std::promise<Status>>();
    sb->read_future = promise->get_future();

    auto stream = _stream;
    int64_t offset = sb->offset;
    int64_t size = sb->size;
    auto* self = this;

    auto status = get_parallel_read_thread_pool()->submit_func([promise, stream, sb, offset, size, self]() {
        sb->buffer.resize(size);
        Status read_status = stream->read_at_fully(offset, sb->buffer.data(), size);
        promise->set_value(read_status);

        // Sliding window: release slot and start next read
        self->_on_parallel_read_complete();
    });

    if (!status.ok()) {
        LOG(WARNING) << "Failed to submit parallel read task: " << status.message();
        promise->set_value(status);
        {
            std::lock_guard<std::mutex> lock(_parallel_read_mutex);
            _active_read_count--;
        }
        _parallel_read_cv.notify_all();
    }
}

void SharedBufferedInputStream::_on_parallel_read_complete() {
    {
        std::lock_guard<std::mutex> lock(_parallel_read_mutex);
        _active_read_count--;
    }
    // Notify destructor if waiting
    _parallel_read_cv.notify_all();
    // Start next parallel read task (sliding window)
    _start_parallel_reads();
}

} // namespace starrocks::io
