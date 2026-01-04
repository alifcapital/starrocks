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

#include "io/io_uring_context.h"

#include <errno.h>
#include <sys/utsname.h>

#include "common/logging.h"
#include "io/io_profiler.h"
#include "util/time.h"

namespace starrocks::io {

IoUringContext::IoUringContext() {
    memset(&_ring, 0, sizeof(_ring));
}

IoUringContext::~IoUringContext() {
    shutdown();
}

bool IoUringContext::is_supported() {
    // Check kernel version >= 5.1 (when io_uring was introduced)
    // For production use, we recommend kernel >= 5.10 for stability
    struct utsname buf;
    if (uname(&buf) != 0) {
        return false;
    }

    int major = 0, minor = 0;
    if (sscanf(buf.release, "%d.%d", &major, &minor) != 2) {
        return false;
    }

    // io_uring was introduced in kernel 5.1
    // We require 5.10+ for stability and feature completeness
    if (major < 5 || (major == 5 && minor < 10)) {
        return false;
    }

    // Try to create a probe to check if io_uring is actually available
    struct io_uring_probe* probe = io_uring_get_probe();
    if (probe == nullptr) {
        return false;
    }

    // Check if basic operations are supported
    bool has_read = io_uring_opcode_supported(probe, IORING_OP_READ);
    bool has_write = io_uring_opcode_supported(probe, IORING_OP_WRITE);
    bool has_fsync = io_uring_opcode_supported(probe, IORING_OP_FSYNC);

    io_uring_free_probe(probe);

    return has_read && has_write && has_fsync;
}

Status IoUringContext::init(size_t queue_depth) {
    if (_initialized.load(std::memory_order_acquire)) {
        return Status::AlreadyExist("IoUringContext already initialized");
    }

    _queue_depth = queue_depth;

    // Initialize io_uring with the specified queue depth
    // We don't use IORING_SETUP_SQPOLL (kernel polling) since we poll manually
    int ret = io_uring_queue_init(queue_depth, &_ring, 0);
    if (ret < 0) {
        return Status::IOError(fmt::format("io_uring_queue_init failed: {}", strerror(-ret)));
    }

    _initialized.store(true, std::memory_order_release);
    LOG(INFO) << "IoUringContext initialized with queue_depth=" << queue_depth;
    return Status::OK();
}

void IoUringContext::shutdown() {
    if (!_initialized.exchange(false, std::memory_order_acq_rel)) {
        return;
    }

    // Complete any pending requests with error
    for (auto& [id, request] : _pending_requests) {
        request->promise.set_value(Status::Aborted("IoUringContext shutdown"));
    }
    _pending_requests.clear();

    // Unregister buffers before exiting the ring
    unregister_buffers();

    io_uring_queue_exit(&_ring);
    LOG(INFO) << "IoUringContext shutdown, stats: reads=" << _stats.reads_submitted.load()
              << " writes=" << _stats.writes_submitted.load() << " completions=" << _stats.completions.load()
              << " errors=" << _stats.errors.load() << " batches=" << _stats.batches_submitted.load()
              << " syscalls_saved=" << _stats.syscalls_saved.load()
              << " fixed_reads=" << _stats.fixed_reads.load() << " fixed_writes=" << _stats.fixed_writes.load();
}

bthreads::Future<StatusOr<size_t>> IoUringContext::submit_read(int fd, void* buf, size_t len, int64_t offset) {
    auto request = std::make_unique<IoUringRequest>(IoUringRequest::make_read(fd, buf, len, offset, next_request_id()));
    _stats.reads_submitted.fetch_add(1, std::memory_order_relaxed);
    return submit_request(std::move(request));
}

bthreads::Future<StatusOr<size_t>> IoUringContext::submit_write(int fd, const void* buf, size_t len, int64_t offset) {
    auto request =
            std::make_unique<IoUringRequest>(IoUringRequest::make_write(fd, buf, len, offset, next_request_id()));
    _stats.writes_submitted.fetch_add(1, std::memory_order_relaxed);
    return submit_request(std::move(request));
}

bthreads::Future<StatusOr<size_t>> IoUringContext::submit_writev(int fd, const struct iovec* iov, int iovcnt,
                                                                  int64_t offset) {
    auto request =
            std::make_unique<IoUringRequest>(IoUringRequest::make_writev(fd, iov, iovcnt, offset, next_request_id()));
    _stats.writes_submitted.fetch_add(1, std::memory_order_relaxed);
    return submit_request(std::move(request));
}

bthreads::Future<StatusOr<size_t>> IoUringContext::submit_fsync(int fd, bool datasync) {
    auto request = std::make_unique<IoUringRequest>(IoUringRequest::make_fsync(fd, datasync, next_request_id()));
    _stats.fsyncs_submitted.fetch_add(1, std::memory_order_relaxed);
    return submit_request(std::move(request));
}

bthreads::Future<StatusOr<size_t>> IoUringContext::submit_request(std::unique_ptr<IoUringRequest> request) {
    auto future = request->promise.get_future();

    if (!_initialized.load(std::memory_order_acquire)) {
        request->promise.set_value(Status::InternalError("IoUringContext not initialized"));
        return future;
    }

    // Get a submission queue entry
    struct io_uring_sqe* sqe = io_uring_get_sqe(&_ring);
    if (sqe == nullptr) {
        _stats.sq_full_count.fetch_add(1, std::memory_order_relaxed);
        request->promise.set_value(Status::ResourceBusy("io_uring submission queue full"));
        return future;
    }

    // Record submit time for latency tracking
    request->submit_time_ns = MonotonicNanos();
    uint64_t request_id = request->request_id;

    // Prepare the SQE based on request type
    switch (request->type) {
    case IoUringRequestType::READ:
        io_uring_prep_read(sqe, request->fd, request->buffer, request->size, request->offset);
        break;
    case IoUringRequestType::WRITE:
        io_uring_prep_write(sqe, request->fd, request->buffer, request->size, request->offset);
        break;
    case IoUringRequestType::WRITEV:
        io_uring_prep_writev(sqe, request->fd, request->iovecs.data(), request->iovecs.size(), request->offset);
        break;
    case IoUringRequestType::READV:
        io_uring_prep_readv(sqe, request->fd, request->iovecs.data(), request->iovecs.size(), request->offset);
        break;
    case IoUringRequestType::FSYNC:
        if (request->datasync_only) {
            io_uring_prep_fsync(sqe, request->fd, IORING_FSYNC_DATASYNC);
        } else {
            io_uring_prep_fsync(sqe, request->fd, 0);
        }
        break;
    case IoUringRequestType::READ_FIXED:
        io_uring_prep_read_fixed(sqe, request->fd, request->buffer, request->size, request->offset,
                                 request->buf_index);
        break;
    case IoUringRequestType::WRITE_FIXED:
        io_uring_prep_write_fixed(sqe, request->fd, request->buffer, request->size, request->offset,
                                  request->buf_index);
        break;
    }

    // Store request_id in user_data for retrieval on completion
    // Use io_uring_sqe_set_data (void*) for compatibility with older liburing versions
    // that don't have io_uring_sqe_set_data64
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(static_cast<uintptr_t>(request_id)));

    // Store request in pending map
    _pending_requests[request_id] = std::move(request);

    // Submit immediately if not in batch mode
    if (!_batch_mode) {
        int ret = io_uring_submit(&_ring);
        if (ret < 0) {
            // Submission failed - retrieve request and set error
            auto it = _pending_requests.find(request_id);
            if (it != _pending_requests.end()) {
                it->second->promise.set_value(Status::IOError(fmt::format("io_uring_submit failed: {}", strerror(-ret))));
                _pending_requests.erase(it);
            }
        }
    } else {
        _batch_count++;
    }

    return future;
}

void IoUringContext::start_batch() {
    _batch_mode = true;
    _batch_count = 0;
}

Status IoUringContext::submit_batch() {
    if (!_batch_mode) {
        return Status::OK();
    }

    _batch_mode = false;

    if (_batch_count == 0) {
        return Status::OK();
    }

    int ret = io_uring_submit(&_ring);
    if (ret < 0) {
        return Status::IOError(fmt::format("io_uring_submit (batch) failed: {}", strerror(-ret)));
    }

    // Phase 2: Update batch metrics
    _stats.batches_submitted.fetch_add(1, std::memory_order_relaxed);
    _stats.total_batch_size.fetch_add(_batch_count, std::memory_order_relaxed);
    // Each batch saves (_batch_count - 1) syscalls compared to individual submits
    if (_batch_count > 1) {
        _stats.syscalls_saved.fetch_add(_batch_count - 1, std::memory_order_relaxed);
    }

    VLOG(3) << "IoUring batch submitted: " << _batch_count << " operations, actually submitted: " << ret;
    _batch_count = 0;
    return Status::OK();
}

// Phase 3: Registered buffer support
Status IoUringContext::register_buffers(const std::vector<struct iovec>& buffers) {
    if (!_initialized.load(std::memory_order_acquire)) {
        return Status::InternalError("IoUringContext not initialized");
    }

    if (_buffers_registered) {
        return Status::AlreadyExist("Buffers already registered");
    }

    if (buffers.empty()) {
        return Status::InvalidArgument("Empty buffer list");
    }

    int ret = io_uring_register_buffers(&_ring, buffers.data(), buffers.size());
    if (ret < 0) {
        return Status::IOError(fmt::format("io_uring_register_buffers failed: {}", strerror(-ret)));
    }

    _registered_buffers = buffers;
    _buffers_registered = true;
    LOG(INFO) << "IoUringContext registered " << buffers.size() << " buffers";
    return Status::OK();
}

void IoUringContext::unregister_buffers() {
    if (!_buffers_registered) {
        return;
    }

    io_uring_unregister_buffers(&_ring);
    _registered_buffers.clear();
    _buffers_registered = false;
    LOG(INFO) << "IoUringContext unregistered buffers";
}

bthreads::Future<StatusOr<size_t>> IoUringContext::submit_read_fixed(int fd, size_t buf_index, size_t len,
                                                                      int64_t offset) {
    if (!_buffers_registered || buf_index >= _registered_buffers.size()) {
        bthreads::Promise<StatusOr<size_t>> promise;
        auto future = promise.get_future();
        promise.set_value(Status::InvalidArgument("Invalid buffer index or buffers not registered"));
        return future;
    }

    void* buf = _registered_buffers[buf_index].iov_base;
    auto request = std::make_unique<IoUringRequest>(
            IoUringRequest::make_read_fixed(fd, buf, len, offset, buf_index, next_request_id()));
    _stats.reads_submitted.fetch_add(1, std::memory_order_relaxed);
    _stats.fixed_reads.fetch_add(1, std::memory_order_relaxed);
    return submit_request(std::move(request));
}

bthreads::Future<StatusOr<size_t>> IoUringContext::submit_write_fixed(int fd, size_t buf_index, size_t len,
                                                                       int64_t offset) {
    if (!_buffers_registered || buf_index >= _registered_buffers.size()) {
        bthreads::Promise<StatusOr<size_t>> promise;
        auto future = promise.get_future();
        promise.set_value(Status::InvalidArgument("Invalid buffer index or buffers not registered"));
        return future;
    }

    void* buf = _registered_buffers[buf_index].iov_base;
    auto request = std::make_unique<IoUringRequest>(
            IoUringRequest::make_write_fixed(fd, buf, len, offset, buf_index, next_request_id()));
    _stats.writes_submitted.fetch_add(1, std::memory_order_relaxed);
    _stats.fixed_writes.fetch_add(1, std::memory_order_relaxed);
    return submit_request(std::move(request));
}

Status IoUringContext::poll_completions(int min_complete) {
    if (!_initialized.load(std::memory_order_acquire)) {
        return Status::InternalError("IoUringContext not initialized");
    }

    struct io_uring_cqe* cqe;
    unsigned completed = 0;

    if (min_complete > 0) {
        // Wait for at least min_complete completions
        int ret = io_uring_wait_cqe_nr(&_ring, &cqe, min_complete);
        if (ret < 0) {
            if (ret == -EINTR) {
                return Status::OK(); // Interrupted, try again later
            }
            return Status::IOError(fmt::format("io_uring_wait_cqe_nr failed: {}", strerror(-ret)));
        }
    }

    // Process all available completions
    while (io_uring_peek_cqe(&_ring, &cqe) == 0) {
        handle_completion(cqe);
        io_uring_cqe_seen(&_ring, cqe);
        completed++;
    }

    if (completed > 0) {
        VLOG(3) << "IoUring poll completed: " << completed << " operations";
    }

    return Status::OK();
}

void IoUringContext::handle_completion(struct io_uring_cqe* cqe) {
    // Use io_uring_cqe_get_data (void*) for compatibility with older liburing versions
    // that don't have io_uring_cqe_get_data64
    uint64_t request_id = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(io_uring_cqe_get_data(cqe)));

    auto it = _pending_requests.find(request_id);
    if (it == _pending_requests.end()) {
        LOG(ERROR) << "Unknown request_id in CQE: " << request_id;
        return;
    }

    auto& request = it->second;
    int64_t latency_ns = MonotonicNanos() - request->submit_time_ns;
    int32_t result = cqe->res;

    _stats.completions.fetch_add(1, std::memory_order_relaxed);

    if (result < 0) {
        // Error occurred
        _stats.errors.fetch_add(1, std::memory_order_relaxed);
        request->promise.set_value(Status::IOError(fmt::format("io_uring error: {}", strerror(-result))));
    } else {
        // Success
        size_t bytes = static_cast<size_t>(result);
        request->promise.set_value(bytes);

        // Update metrics via IOProfiler (thread-safe)
        switch (request->type) {
        case IoUringRequestType::READ:
        case IoUringRequestType::READV:
        case IoUringRequestType::READ_FIXED:
            IOProfiler::add_read(bytes, latency_ns);
            _stats.total_read_bytes.fetch_add(bytes, std::memory_order_relaxed);
            _stats.total_read_latency_ns.fetch_add(latency_ns, std::memory_order_relaxed);
            break;
        case IoUringRequestType::WRITE:
        case IoUringRequestType::WRITEV:
        case IoUringRequestType::WRITE_FIXED:
            IOProfiler::add_write(bytes, latency_ns);
            _stats.total_write_bytes.fetch_add(bytes, std::memory_order_relaxed);
            _stats.total_write_latency_ns.fetch_add(latency_ns, std::memory_order_relaxed);
            break;
        case IoUringRequestType::FSYNC:
            // fsync doesn't have bytes, but we track latency
            break;
        }
    }

    _pending_requests.erase(it);
}

} // namespace starrocks::io

#endif // WITH_IO_URING
