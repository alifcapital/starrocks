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

#include "io/io_uring_buffer_pool.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include "common/config.h"
#include "common/logging.h"
#include "io/io_uring_manager.h"

namespace starrocks::io {

// Alignment for direct I/O (typically 4KB)
static constexpr size_t kBufferAlignment = 4096;

IoUringBufferPool::IoUringBufferPool(IoUringContext* ctx, size_t buffer_size, size_t buffer_count)
        : _ctx(ctx), _buffer_size(buffer_size), _buffer_count(buffer_count) {}

IoUringBufferPool::~IoUringBufferPool() {
    if (_registered && _ctx) {
        _ctx->unregister_buffers();
    }
    if (_memory) {
        free(_memory);
        _memory = nullptr;
    }
}

StatusOr<std::unique_ptr<IoUringBufferPool>> IoUringBufferPool::create(IoUringContext* ctx, size_t buffer_size,
                                                                        size_t buffer_count) {
    if (ctx == nullptr || !ctx->is_initialized()) {
        return Status::InvalidArgument("IoUringContext not available");
    }

    if (buffer_size == 0 || buffer_count == 0) {
        return Status::InvalidArgument("Invalid buffer size or count");
    }

    // Round up buffer size to alignment
    size_t aligned_size = (buffer_size + kBufferAlignment - 1) & ~(kBufferAlignment - 1);

    auto pool = std::unique_ptr<IoUringBufferPool>(new IoUringBufferPool(ctx, aligned_size, buffer_count));
    RETURN_IF_ERROR(pool->init());
    return pool;
}

Status IoUringBufferPool::init() {
    // Allocate aligned memory for all buffers
    size_t total_size = _buffer_size * _buffer_count;
    int ret = posix_memalign(&_memory, kBufferAlignment, total_size);
    if (ret != 0) {
        return Status::MemoryAllocFailed(fmt::format("Failed to allocate {} bytes for buffer pool", total_size));
    }

    // Initialize memory to zero (optional, but helps with debugging)
    memset(_memory, 0, total_size);

    // Build iovec array for registration
    _buffers.resize(_buffer_count);
    char* ptr = static_cast<char*>(_memory);
    for (size_t i = 0; i < _buffer_count; ++i) {
        _buffers[i].iov_base = ptr + (i * _buffer_size);
        _buffers[i].iov_len = _buffer_size;
    }

    // Register buffers with io_uring
    RETURN_IF_ERROR(_ctx->register_buffers(_buffers));
    _registered = true;

    // Initialize free list with all buffer indices
    _free_list.reserve(_buffer_count);
    for (size_t i = 0; i < _buffer_count; ++i) {
        _free_list.push_back(i);
    }

    LOG(INFO) << "IoUringBufferPool initialized: " << _buffer_count << " buffers x " << _buffer_size << " bytes";
    return Status::OK();
}

StatusOr<std::pair<size_t, void*>> IoUringBufferPool::acquire() {
    std::lock_guard<std::mutex> lock(_mutex);

    if (_free_list.empty()) {
        return Status::ResourceBusy("Buffer pool exhausted");
    }

    size_t idx = _free_list.back();
    _free_list.pop_back();

    return std::make_pair(idx, _buffers[idx].iov_base);
}

void IoUringBufferPool::release(size_t buf_index) {
    if (buf_index >= _buffer_count) {
        LOG(ERROR) << "Invalid buffer index: " << buf_index;
        return;
    }

    std::lock_guard<std::mutex> lock(_mutex);
    _free_list.push_back(buf_index);
}

void* IoUringBufferPool::get_buffer(size_t buf_index) const {
    if (buf_index >= _buffer_count) {
        return nullptr;
    }
    return _buffers[buf_index].iov_base;
}

size_t IoUringBufferPool::available_count() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _free_list.size();
}

// IoUringBufferPoolManager implementation

IoUringBufferPoolManager::~IoUringBufferPoolManager() {
    shutdown_all();
    if (_tls_key_created) {
        pthread_key_delete(_tls_key);
    }
}

IoUringBufferPoolManager* IoUringBufferPoolManager::instance() {
    static IoUringBufferPoolManager instance;

    // Lazy initialization of TLS key
    static std::once_flag init_flag;
    std::call_once(init_flag, [&]() {
        int ret = pthread_key_create(&instance._tls_key, thread_local_destructor);
        if (ret == 0) {
            instance._tls_key_created = true;
        } else {
            LOG(ERROR) << "Failed to create pthread key for IoUringBufferPoolManager";
        }
    });

    return &instance;
}

IoUringBufferPool* IoUringBufferPoolManager::get_or_create_pool() {
    if (!_tls_key_created) {
        return nullptr;
    }

    // Check if registered buffers are enabled
    if (!config::enable_io_uring_registered_buffers) {
        return nullptr;
    }

    // Check for existing thread-local pool
    void* ptr = pthread_getspecific(_tls_key);
    if (ptr != nullptr) {
        return static_cast<IoUringBufferPool*>(ptr);
    }

    // Check if shutdown
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_shutdown) {
            return nullptr;
        }
    }

    // Get io_uring context
    auto* ctx = IoUringManager::instance()->get_or_create_context();
    if (ctx == nullptr || !ctx->is_initialized()) {
        return nullptr;
    }

    // Create new buffer pool
    auto pool_result = IoUringBufferPool::create(ctx, config::io_uring_registered_buffer_size,
                                                  config::io_uring_registered_buffer_count);
    if (!pool_result.ok()) {
        LOG(WARNING) << "Failed to create IoUringBufferPool: " << pool_result.status();
        return nullptr;
    }

    IoUringBufferPool* pool = pool_result.value().release();

    // Set thread-local storage
    int ret = pthread_setspecific(_tls_key, pool);
    if (ret != 0) {
        LOG(ERROR) << "Failed to set pthread specific: " << strerror(ret);
        delete pool;
        return nullptr;
    }

    // Register for tracking
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _all_pools.push_back(pool);
    }

    LOG(INFO) << "Created IoUringBufferPool for thread " << pthread_self();
    return pool;
}

void IoUringBufferPoolManager::release_pool() {
    if (!_tls_key_created) {
        return;
    }

    void* ptr = pthread_getspecific(_tls_key);
    if (ptr == nullptr) {
        return;
    }

    auto* pool = static_cast<IoUringBufferPool*>(ptr);
    pthread_setspecific(_tls_key, nullptr);

    // Remove from tracking
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = std::find(_all_pools.begin(), _all_pools.end(), pool);
        if (it != _all_pools.end()) {
            _all_pools.erase(it);
        }
    }

    delete pool;
    LOG(INFO) << "Released IoUringBufferPool for thread " << pthread_self();
}

void IoUringBufferPoolManager::shutdown_all() {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_shutdown) {
        return;
    }
    _shutdown = true;

    LOG(INFO) << "IoUringBufferPoolManager shutdown_all: " << _all_pools.size() << " pools";
    // Note: Don't delete pools here - thread_local_destructor handles that
}

void IoUringBufferPoolManager::thread_local_destructor(void* ptr) {
    if (ptr == nullptr) {
        return;
    }

    auto* pool = static_cast<IoUringBufferPool*>(ptr);
    auto* manager = IoUringBufferPoolManager::instance();

    // Remove from tracking
    {
        std::lock_guard<std::mutex> lock(manager->_mutex);
        auto it = std::find(manager->_all_pools.begin(), manager->_all_pools.end(), pool);
        if (it != manager->_all_pools.end()) {
            manager->_all_pools.erase(it);
        }
    }

    delete pool;
    LOG(INFO) << "IoUringBufferPool destroyed for thread (thread exit)";
}

} // namespace starrocks::io

#endif // WITH_IO_URING
