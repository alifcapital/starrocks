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

#include <liburing.h>

#include <atomic>
#include <memory>
#include <unordered_map>

#include "common/status.h"
#include "common/statusor.h"
#include "io/io_uring_request.h"
#include "util/bthreads/future.h"

namespace starrocks::io {

// IoUringContext manages a single io_uring instance for async I/O operations.
//
// Design: Ring-per-thread architecture (per VLDB'26 paper recommendation)
// - Each spill worker thread owns its own io_uring ring (via thread-local storage)
// - No mutex on hot path (submit/complete) - single thread owns the ring
// - Completion polling happens in the same thread (DeferTR mode)
//
// This avoids synchronization overhead and improves cache locality.
class IoUringContext {
public:
    IoUringContext();
    ~IoUringContext();

    // Non-copyable, non-movable (ring is not movable)
    IoUringContext(const IoUringContext&) = delete;
    IoUringContext& operator=(const IoUringContext&) = delete;
    IoUringContext(IoUringContext&&) = delete;
    IoUringContext& operator=(IoUringContext&&) = delete;

    // Check if io_uring is available on this system (kernel >= 5.1)
    static bool is_supported();

    // Initialize the io_uring ring with specified queue depth
    // @param queue_depth: Number of entries in submission/completion queues
    // @return Status::OK() on success, error status on failure
    Status init(size_t queue_depth = 256);

    // Shutdown and cleanup the ring
    void shutdown();

    // Check if the context is initialized and ready for use
    bool is_initialized() const { return _initialized.load(std::memory_order_acquire); }

    // Returns the number of requests currently in the io_uring ring awaiting completion.
    // This is useful to determine if poll_completions should block - if zero, there are
    // no CQEs to wait for and blocking would cause a deadlock.
    size_t pending_in_ring_count() const { return _pending_requests.size(); }

    // Submit a read operation
    // @param fd: File descriptor to read from
    // @param buf: Buffer to read into
    // @param len: Number of bytes to read
    // @param offset: File offset to read from
    // @return Future that will contain bytes read or error status
    bthreads::Future<StatusOr<size_t>> submit_read(int fd, void* buf, size_t len, int64_t offset);

    // Submit a write operation
    // @param fd: File descriptor to write to
    // @param buf: Buffer to write from
    // @param len: Number of bytes to write
    // @param offset: File offset to write at
    // @return Future that will contain bytes written or error status
    bthreads::Future<StatusOr<size_t>> submit_write(int fd, const void* buf, size_t len, int64_t offset);

    // Submit a vectored write operation (writev)
    // @param fd: File descriptor to write to
    // @param iov: Array of iovec structures
    // @param iovcnt: Number of iovec structures
    // @param offset: File offset to write at
    // @return Future that will contain bytes written or error status
    bthreads::Future<StatusOr<size_t>> submit_writev(int fd, const struct iovec* iov, int iovcnt, int64_t offset);

    // Submit an fsync operation
    // @param fd: File descriptor to sync
    // @param datasync: If true, use fdatasync (data only), else fsync (data + metadata)
    // @return Future that will contain success or error status
    bthreads::Future<StatusOr<size_t>> submit_fsync(int fd, bool datasync = true);

    // Poll for completed I/O operations
    // This should be called periodically by the owning thread to process completions.
    // @param min_complete: Minimum number of completions to wait for (0 = non-blocking)
    // @return Status::OK() on success, error status on failure
    Status poll_completions(int min_complete = 0);

    // Batching support: start a batch of operations
    // Operations submitted between start_batch() and submit_batch() are batched
    void start_batch();

    // Submit all batched operations at once
    // @return Status::OK() on success, error status on failure
    Status submit_batch();

    // Check if currently in batch mode
    bool in_batch_mode() const { return _batch_mode; }

    // Statistics for monitoring and debugging
    struct Stats {
        std::atomic<uint64_t> reads_submitted{0};
        std::atomic<uint64_t> writes_submitted{0};
        std::atomic<uint64_t> fsyncs_submitted{0};
        std::atomic<uint64_t> completions{0};
        std::atomic<uint64_t> errors{0};
        std::atomic<uint64_t> sq_full_count{0};  // Times SQ was full

        std::atomic<uint64_t> total_read_bytes{0};
        std::atomic<uint64_t> total_write_bytes{0};
        std::atomic<uint64_t> total_read_latency_ns{0};
        std::atomic<uint64_t> total_write_latency_ns{0};

        // Phase 2: Batching metrics
        std::atomic<uint64_t> batches_submitted{0};
        std::atomic<uint64_t> total_batch_size{0};  // Sum of all batch sizes
        std::atomic<uint64_t> syscalls_saved{0};    // Estimated syscalls saved by batching

        // Phase 3: Registered buffer metrics
        std::atomic<uint64_t> fixed_reads{0};   // Reads using registered buffers
        std::atomic<uint64_t> fixed_writes{0};  // Writes using registered buffers
    };

    const Stats& stats() const { return _stats; }

    // Phase 3: Registered buffer support
    // Register a set of buffers for zero-copy I/O operations
    // @param buffers: Vector of buffer descriptors (base, length)
    // @return Status::OK() on success
    Status register_buffers(const std::vector<struct iovec>& buffers);

    // Unregister previously registered buffers
    void unregister_buffers();

    // Check if buffers are registered
    bool has_registered_buffers() const { return _buffers_registered; }

    // Submit a read using registered buffer (IORING_OP_READ_FIXED)
    // @param buf_index: Index into registered buffer array
    bthreads::Future<StatusOr<size_t>> submit_read_fixed(int fd, size_t buf_index, size_t len, int64_t offset);

    // Submit a write using registered buffer (IORING_OP_WRITE_FIXED)
    bthreads::Future<StatusOr<size_t>> submit_write_fixed(int fd, size_t buf_index, size_t len, int64_t offset);

private:
    // Handle a single completion queue entry
    void handle_completion(struct io_uring_cqe* cqe);

    // Get next request ID
    uint64_t next_request_id() { return _next_request_id.fetch_add(1, std::memory_order_relaxed); }

    // Submit a request to the ring
    // @param request: The request to submit (moved into pending map)
    // @return Future for the operation result
    bthreads::Future<StatusOr<size_t>> submit_request(std::unique_ptr<IoUringRequest> request);

    // io_uring ring structure
    struct io_uring _ring;

    // Initialization state
    std::atomic<bool> _initialized{false};

    // Queue depth
    size_t _queue_depth{0};

    // Pending requests map: request_id -> request
    // No mutex needed - single thread owns the ring
    std::unordered_map<uint64_t, std::unique_ptr<IoUringRequest>> _pending_requests;

    // Request ID counter
    std::atomic<uint64_t> _next_request_id{1};

    // Batching state
    bool _batch_mode{false};
    size_t _batch_count{0};

    // Phase 3: Registered buffers state
    bool _buffers_registered{false};
    std::vector<struct iovec> _registered_buffers;

    // Statistics
    Stats _stats;
};

} // namespace starrocks::io

#endif // WITH_IO_URING
