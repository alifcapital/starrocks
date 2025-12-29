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

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include "common/status.h"
#include "io/io_uring_context.h"

namespace starrocks::io {

// IoUringBufferPool manages a pool of pre-registered buffers for zero-copy I/O.
//
// Phase 3 implementation for registered buffer support:
// - Pre-allocates and registers buffers with io_uring
// - Provides buffer acquisition/release API
// - Enables IORING_OP_READ_FIXED/WRITE_FIXED operations
// - Per-thread pools avoid synchronization on hot path
//
// Usage:
//   auto pool = IoUringBufferPool::create(ctx, buffer_size, buffer_count);
//   auto [buf_index, ptr] = pool->acquire();
//   // Use ptr for I/O
//   auto future = ctx->submit_write_fixed(fd, buf_index, len, offset);
//   // After completion:
//   pool->release(buf_index);
class IoUringBufferPool {
public:
    // Create and initialize a buffer pool
    // @param ctx: IoUringContext to register buffers with
    // @param buffer_size: Size of each buffer in bytes
    // @param buffer_count: Number of buffers to allocate
    // @return Unique pointer to pool, or error status
    static StatusOr<std::unique_ptr<IoUringBufferPool>> create(IoUringContext* ctx, size_t buffer_size,
                                                                size_t buffer_count);

    ~IoUringBufferPool();

    // Non-copyable, non-movable
    IoUringBufferPool(const IoUringBufferPool&) = delete;
    IoUringBufferPool& operator=(const IoUringBufferPool&) = delete;

    // Acquire a buffer from the pool
    // Returns (buffer_index, buffer_pointer) or error if pool exhausted
    StatusOr<std::pair<size_t, void*>> acquire();

    // Release a buffer back to the pool
    void release(size_t buf_index);

    // Get buffer pointer by index
    void* get_buffer(size_t buf_index) const;

    // Get buffer size
    size_t buffer_size() const { return _buffer_size; }

    // Get total number of buffers
    size_t buffer_count() const { return _buffer_count; }

    // Get number of available buffers
    size_t available_count() const;

    // Check if pool is registered with io_uring
    bool is_registered() const { return _registered; }

private:
    IoUringBufferPool(IoUringContext* ctx, size_t buffer_size, size_t buffer_count);

    Status init();

    IoUringContext* _ctx;
    size_t _buffer_size;
    size_t _buffer_count;

    // Memory block (aligned for direct I/O)
    void* _memory{nullptr};

    // Buffer descriptors for io_uring registration
    std::vector<struct iovec> _buffers;

    // Free list: indices of available buffers
    std::vector<size_t> _free_list;
    mutable std::mutex _mutex;

    bool _registered{false};
};

// Thread-local buffer pool manager
class IoUringBufferPoolManager {
public:
    static IoUringBufferPoolManager* instance();

    // Get or create thread-local buffer pool
    // Returns nullptr if registered buffers are disabled or unavailable
    IoUringBufferPool* get_or_create_pool();

    // Release thread-local pool
    void release_pool();

    // Shutdown all pools
    void shutdown_all();

private:
    IoUringBufferPoolManager() = default;
    ~IoUringBufferPoolManager();

    static void thread_local_destructor(void* ptr);

    pthread_key_t _tls_key;
    bool _tls_key_created{false};

    mutable std::mutex _mutex;
    std::vector<IoUringBufferPool*> _all_pools;
    bool _shutdown{false};
};

} // namespace starrocks::io

#endif // WITH_IO_URING
