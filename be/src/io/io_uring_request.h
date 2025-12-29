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

#pragma once

#ifdef WITH_IO_URING

#include <sys/uio.h>

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/statusor.h"
#include "util/bthreads/future.h"

namespace starrocks::io {

// Request type for io_uring operations
enum class IoUringRequestType : uint8_t {
    READ,         // pread operation
    WRITE,        // pwrite operation
    WRITEV,       // pwritev operation (vectored write)
    READV,        // preadv operation (vectored read)
    FSYNC,        // fsync/fdatasync operation
    READ_FIXED,   // pread with registered buffer (Phase 3)
    WRITE_FIXED,  // pwrite with registered buffer (Phase 3)
};

// Request structure for tracking in-flight io_uring I/O operations.
// Each request is associated with one SQE and will be completed via CQE.
struct IoUringRequest {
    // Type of I/O operation
    IoUringRequestType type;

    // File descriptor for the operation
    int fd;

    // Buffer for read/write operations (for non-vectored I/O)
    void* buffer{nullptr};

    // Size of the buffer (for non-vectored I/O)
    size_t size{0};

    // File offset for the operation
    int64_t offset{0};

    // Promise to fulfill when operation completes
    // For read/write: StatusOr<size_t> - bytes transferred or error
    // For fsync: Status - success or error
    bthreads::Promise<StatusOr<size_t>> promise;

    // Submit timestamp in nanoseconds (for latency metrics)
    int64_t submit_time_ns{0};

    // Unique request ID for tracking
    uint64_t request_id{0};

    // Vectored I/O support: iovec array for writev/readv
    // Owned by the request - will be valid until completion
    std::vector<struct iovec> iovecs;

    // Whether to sync data only (fdatasync) or also metadata (fsync)
    bool datasync_only{true};

    // Phase 3: Buffer index for registered buffer operations
    size_t buf_index{0};

    IoUringRequest() = default;

    // Construct a read request
    static IoUringRequest make_read(int fd, void* buf, size_t len, int64_t offset, uint64_t req_id) {
        IoUringRequest req;
        req.type = IoUringRequestType::READ;
        req.fd = fd;
        req.buffer = buf;
        req.size = len;
        req.offset = offset;
        req.request_id = req_id;
        return req;
    }

    // Construct a write request
    static IoUringRequest make_write(int fd, const void* buf, size_t len, int64_t offset, uint64_t req_id) {
        IoUringRequest req;
        req.type = IoUringRequestType::WRITE;
        req.fd = fd;
        req.buffer = const_cast<void*>(buf);
        req.size = len;
        req.offset = offset;
        req.request_id = req_id;
        return req;
    }

    // Construct a vectored write request
    // The iovecs are copied into the request to ensure they remain valid until completion
    static IoUringRequest make_writev(int fd, const struct iovec* iov, int iovcnt, int64_t offset, uint64_t req_id) {
        IoUringRequest req;
        req.type = IoUringRequestType::WRITEV;
        req.fd = fd;
        req.offset = offset;
        req.request_id = req_id;
        req.iovecs.assign(iov, iov + iovcnt);
        // Calculate total size for metrics
        req.size = 0;
        for (int i = 0; i < iovcnt; ++i) {
            req.size += iov[i].iov_len;
        }
        return req;
    }

    // Construct a vectored read request
    static IoUringRequest make_readv(int fd, const struct iovec* iov, int iovcnt, int64_t offset, uint64_t req_id) {
        IoUringRequest req;
        req.type = IoUringRequestType::READV;
        req.fd = fd;
        req.offset = offset;
        req.request_id = req_id;
        req.iovecs.assign(iov, iov + iovcnt);
        req.size = 0;
        for (int i = 0; i < iovcnt; ++i) {
            req.size += iov[i].iov_len;
        }
        return req;
    }

    // Construct an fsync request
    static IoUringRequest make_fsync(int fd, bool datasync, uint64_t req_id) {
        IoUringRequest req;
        req.type = IoUringRequestType::FSYNC;
        req.fd = fd;
        req.datasync_only = datasync;
        req.request_id = req_id;
        return req;
    }

    // Phase 3: Construct a fixed read request (using registered buffer)
    static IoUringRequest make_read_fixed(int fd, void* buf, size_t len, int64_t offset, size_t buf_idx,
                                          uint64_t req_id) {
        IoUringRequest req;
        req.type = IoUringRequestType::READ_FIXED;
        req.fd = fd;
        req.buffer = buf;
        req.size = len;
        req.offset = offset;
        req.buf_index = buf_idx;
        req.request_id = req_id;
        return req;
    }

    // Phase 3: Construct a fixed write request (using registered buffer)
    static IoUringRequest make_write_fixed(int fd, const void* buf, size_t len, int64_t offset, size_t buf_idx,
                                           uint64_t req_id) {
        IoUringRequest req;
        req.type = IoUringRequestType::WRITE_FIXED;
        req.fd = fd;
        req.buffer = const_cast<void*>(buf);
        req.size = len;
        req.offset = offset;
        req.buf_index = buf_idx;
        req.request_id = req_id;
        return req;
    }
};

} // namespace starrocks::io

#endif // WITH_IO_URING
