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

#ifdef WITH_IO_URING

#include "fs/io_uring_writable_file.h"

#include <cstring>
#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>

#include "common/config.h"
#include "common/logging.h"
#include "fmt/format.h"
#include "gutil/macros.h"
#include "io/io_error.h"

namespace starrocks {

IoUringWritableFile::IoUringWritableFile(std::string filename, int fd, uint64_t filesize, bool sync_on_close,
                                         io::IoUringContext* ctx)
        : _filename(std::move(filename)),
          _fd(fd),
          _ctx(ctx),
          _sync_on_close(sync_on_close),
          _next_write_offset(filesize),
          _committed_size(filesize) {
    FileSystem::on_file_write_open(this);
}

IoUringWritableFile::~IoUringWritableFile() {
    WARN_IF_ERROR(close(), "Failed to close file, file=" + _filename);
}

Status IoUringWritableFile::append(const Slice& data) {
    return appendv(&data, 1);
}

Status IoUringWritableFile::appendv(const Slice* data, size_t cnt) {
    if (!_ctx || !_ctx->is_initialized()) {
        return Status::InternalError("IoUringContext not available");
    }

    // Calculate total size needed
    size_t total_bytes = 0;
    for (size_t i = 0; i < cnt; ++i) {
        total_bytes += data[i].size;
    }

    if (total_bytes == 0) {
        return Status::OK();
    }

    // IMPORTANT: Copy data to an owned buffer before submitting to io_uring.
    // The caller expects synchronous semantics where they can reuse their buffer
    // immediately after append() returns. io_uring may still be DMA-ing from
    // the buffer when we return, so we must own the data. This matches the
    // VLDB'26 paper's buffer manager design which uses registered buffers
    // owned by the buffer pool rather than caller buffers.
    auto owned_buffer = std::make_unique<char[]>(total_bytes);
    char* dest = owned_buffer.get();
    for (size_t i = 0; i < cnt; ++i) {
        memcpy(dest, data[i].data, data[i].size);
        dest += data[i].size;
    }

    // Build single iovec pointing to our owned buffer
    struct iovec iov;
    iov.iov_base = owned_buffer.get();
    iov.iov_len = total_bytes;

    // Start batch mode if enabled and not already in batch
    if (config::enable_io_uring_batching && !_ctx->in_batch_mode()) {
        _ctx->start_batch();
        _batch_writes_count = 0;
    }

    // Record the offset for this write before incrementing
    uint64_t write_offset = _next_write_offset;

    // Submit the write using our owned buffer
    auto future = _ctx->submit_writev(_fd, &iov, 1, write_offset);

    // IMPORTANT: Check for immediate submission failure (e.g., SQ full).
    // When io_uring's submission queue is full, submit_writev returns a future
    // that is already ready with ResourceBusy error. We must detect this and
    // return error immediately - otherwise we'd report success but the data
    // would never be written.
    auto wait_status = future.wait_for(std::chrono::milliseconds(0));
    if (wait_status == bthreads::future_status::ready) {
        auto result = future.get();
        if (!result.ok()) {
            // Submission failed immediately - return error, don't update offsets
            return result.status();
        }
        // Submission succeeded and completed immediately (rare but possible)
        // Update committed size by actual bytes written
        size_t bytes_written = result.value();
        _next_write_offset += bytes_written;
        _committed_size = std::max(_committed_size, write_offset + bytes_written);
        _pending_sync = true;
        if (bytes_written < total_bytes) {
            // Short write - report error
            return Status::IOError(fmt::format("short write: expected {} bytes, got {}",
                                               total_bytes, bytes_written));
        }
        return Status::OK();
    }

    // Submission is in-flight - store pending write with offset info
    // We advance _next_write_offset optimistically so subsequent appends go to the right place
    // _committed_size will be updated when the write completes successfully
    _pending_writes.emplace_back(std::move(future), std::move(owned_buffer), total_bytes, write_offset);
    _next_write_offset += total_bytes;
    _pending_sync = true;
    _batch_writes_count++;

    // Submit batch if threshold reached
    RETURN_IF_ERROR(maybe_submit_batch());

    return Status::OK();
}

Status IoUringWritableFile::maybe_submit_batch() {
    if (!config::enable_io_uring_batching) {
        return Status::OK();
    }

    if (_batch_writes_count >= static_cast<size_t>(config::io_uring_max_batch_size)) {
        RETURN_IF_ERROR(_ctx->submit_batch());
        _batch_writes_count = 0;
    }

    return Status::OK();
}

Status IoUringWritableFile::wait_for_pending_writes() {
    if (_pending_writes.empty()) {
        return Status::OK();
    }

    // Submit any remaining batched operations
    if (_ctx && _ctx->in_batch_mode()) {
        RETURN_IF_ERROR(_ctx->submit_batch());
        _batch_writes_count = 0;
    }

    // Poll for completions until all pending writes are done
    // IMPORTANT: Check futures BEFORE blocking on poll_completions to avoid deadlock.
    // When io_uring SQ is full, submit_request sets the promise immediately with
    // ResourceBusy error without enqueuing an SQE. If we call poll_completions(1)
    // before checking these already-ready futures, we block forever waiting for
    // a CQE that will never arrive. See VLDB'26 paper Section 2.2 for details.
    Status first_error;
    while (!_pending_writes.empty()) {
        // First pass: harvest any already-ready futures (including immediate failures)
        // This prevents deadlock when SQ was full and promises were set immediately
        // When a PendingWrite is erased, its owned buffer is automatically freed
        bool has_pending_in_ring = false;
        auto it = _pending_writes.begin();
        while (it != _pending_writes.end()) {
            if (it->future.valid()) {
                auto status = it->future.wait_for(std::chrono::milliseconds(0));
                if (status == bthreads::future_status::ready) {
                    auto result = it->future.get();
                    if (!result.ok()) {
                        // Write failed - don't update _committed_size
                        if (first_error.ok()) {
                            first_error = result.status();
                        }
                    } else {
                        // Write succeeded - update _committed_size with actual bytes written
                        size_t bytes_written = result.value();
                        _committed_size = std::max(_committed_size, it->offset + bytes_written);
                        if (bytes_written < it->expected_bytes && first_error.ok()) {
                            first_error = Status::IOError(
                                    fmt::format("short write at offset {}: expected {} bytes, got {}",
                                                it->offset, it->expected_bytes, bytes_written));
                        }
                    }
                    // Erasing releases the owned buffer - safe now that I/O is complete
                    it = _pending_writes.erase(it);
                    continue;
                } else {
                    // This future is not ready - there's a real in-ring request
                    has_pending_in_ring = true;
                }
            }
            ++it;
        }

        // Only block on poll_completions if there are actual in-ring requests
        // Skip if all remaining futures were already ready (e.g., SQ full errors)
        // Double-check with pending_in_ring_count() for defense-in-depth
        if (has_pending_in_ring && _ctx && _ctx->pending_in_ring_count() > 0) {
            RETURN_IF_ERROR(_ctx->poll_completions(1));
        }
    }

    return first_error;
}

Status IoUringWritableFile::pre_allocate(uint64_t size) {
    uint64_t offset = std::max(_next_write_offset, _pre_allocated_size);
    int ret;
    RETRY_ON_EINTR(ret, fallocate(_fd, 0, offset, size));
    if (ret != 0) {
        if (errno == EOPNOTSUPP) {
            LOG(WARNING) << "The filesystem does not support fallocate().";
        } else if (errno == ENOSYS) {
            LOG(WARNING) << "The kernel does not implement fallocate().";
        } else {
            return io::io_error(_filename, errno);
        }
    }
    _pre_allocated_size = offset + size;
    return Status::OK();
}

Status IoUringWritableFile::close() {
    if (_closed) {
        return Status::OK();
    }

    FileSystem::on_file_write_close(this);
    Status s;

    // Wait for all pending writes to complete
    Status wait_status = wait_for_pending_writes();
    if (!wait_status.ok()) {
        s = wait_status;
    }

    // If we've allocated more space than we used, truncate to the actual committed size
    // Use _committed_size (not _next_write_offset) to avoid zero-filled holes from failed writes
    if (_committed_size < _pre_allocated_size) {
        int ret;
        RETRY_ON_EINTR(ret, ftruncate(_fd, _committed_size));
        if (ret != 0) {
            if (s.ok()) {
                s = io::io_error(_filename, errno);
            }
            _pending_sync = true;
        }
    }

    if (_sync_on_close) {
        Status sync_status = sync();
        if (!sync_status.ok()) {
            LOG(ERROR) << "Unable to Sync " << _filename << ": " << sync_status.to_string();
            if (s.ok()) {
                s = sync_status;
            }
        }
    }

    int ret;
    RETRY_ON_EINTR(ret, ::close(_fd));
    _closed = true;
    if (ret < 0) {
        if (s.ok()) {
            s = io::io_error(_filename, errno);
        }
    }

    return s;
}

Status IoUringWritableFile::flush(FlushMode mode) {
    // Wait for pending writes before flush
    RETURN_IF_ERROR(wait_for_pending_writes());

#if defined(__linux__)
    int flags = SYNC_FILE_RANGE_WRITE;
    if (mode == FLUSH_SYNC) {
        flags |= SYNC_FILE_RANGE_WAIT_BEFORE;
        flags |= SYNC_FILE_RANGE_WAIT_AFTER;
    }
    if (sync_file_range(_fd, 0, 0, flags) < 0) {
        return io::io_error(_filename, errno);
    }
#else
    if (mode == FLUSH_SYNC && fsync(_fd) < 0) {
        return io::io_error(_filename, errno);
    }
#endif
    return Status::OK();
}

Status IoUringWritableFile::sync() {
    // Wait for pending writes before sync
    RETURN_IF_ERROR(wait_for_pending_writes());

    if (_pending_sync) {
        _pending_sync = false;

        // Use io_uring for async fsync if available
        if (_ctx && _ctx->is_initialized()) {
            auto future = _ctx->submit_fsync(_fd, true /* datasync */);
            // Submit and wait for fsync
            if (_ctx->in_batch_mode()) {
                RETURN_IF_ERROR(_ctx->submit_batch());
            }
            RETURN_IF_ERROR(_ctx->poll_completions(1));
            auto result = future.get();
            if (!result.ok()) {
                return result.status();
            }
        } else {
            // Fallback to synchronous fdatasync
            int ret;
            RETRY_ON_EINTR(ret, fdatasync(_fd));
            if (ret != 0) {
                return io::io_error(_filename, errno);
            }
        }
    }
    return Status::OK();
}

uint64_t IoUringWritableFile::size() const {
    // Return committed size - the actual bytes successfully written to disk
    // Note: pending writes may increase this once they complete
    return _committed_size;
}

const std::string& IoUringWritableFile::filename() const {
    return _filename;
}

} // namespace starrocks

#endif // WITH_IO_URING
