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

#include "io/io_uring_manager.h"

#include <pthread.h>

#include "common/config.h"
#include "common/logging.h"

namespace starrocks::io {

IoUringManager::IoUringManager() {
    // Create pthread key for thread-local storage
    int ret = pthread_key_create(&_tls_key, thread_local_destructor);
    if (ret != 0) {
        LOG(ERROR) << "Failed to create pthread key for IoUringManager: " << strerror(ret);
        return;
    }
    _tls_key_created = true;
}

IoUringManager::~IoUringManager() {
    shutdown_all();
    if (_tls_key_created) {
        pthread_key_delete(_tls_key);
    }
}

IoUringManager* IoUringManager::instance() {
    static IoUringManager instance;
    return &instance;
}

bool IoUringManager::is_enabled() const {
    // Check both config and kernel support
    if (!config::enable_io_uring_for_spill) {
        return false;
    }
    return IoUringContext::is_supported();
}

IoUringContext* IoUringManager::get_or_create_context() {
    if (!_tls_key_created) {
        return nullptr;
    }

    // Check if io_uring is enabled and supported
    if (!is_enabled()) {
        return nullptr;
    }

    // Check for existing thread-local context
    void* ptr = pthread_getspecific(_tls_key);
    if (ptr != nullptr) {
        return static_cast<IoUringContext*>(ptr);
    }

    // Check if we're shut down
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_shutdown) {
            return nullptr;
        }
    }

    // Create new context for this thread
    auto ctx = std::make_unique<IoUringContext>();
    Status st = ctx->init(config::io_uring_queue_depth);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to initialize IoUringContext: " << st.message();
        return nullptr;
    }

    IoUringContext* raw_ctx = ctx.release();

    // Set thread-local storage
    int ret = pthread_setspecific(_tls_key, raw_ctx);
    if (ret != 0) {
        LOG(ERROR) << "Failed to set pthread specific: " << strerror(ret);
        delete raw_ctx;
        return nullptr;
    }

    // Register for tracking
    register_context(raw_ctx);

    LOG(INFO) << "Created IoUringContext for thread " << pthread_self();
    return raw_ctx;
}

void IoUringManager::release_context() {
    if (!_tls_key_created) {
        return;
    }

    void* ptr = pthread_getspecific(_tls_key);
    if (ptr == nullptr) {
        return;
    }

    auto* ctx = static_cast<IoUringContext*>(ptr);

    // Clear thread-local storage
    pthread_setspecific(_tls_key, nullptr);

    // Unregister and delete
    unregister_context(ctx);
    ctx->shutdown();
    delete ctx;

    LOG(INFO) << "Released IoUringContext for thread " << pthread_self();
}

void IoUringManager::shutdown_all() {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_shutdown) {
        return;
    }
    _shutdown = true;

    LOG(INFO) << "IoUringManager shutdown_all: " << _all_contexts.size() << " contexts";

    // Shutdown all contexts (but don't delete - thread_local_destructor will do that)
    for (auto* ctx : _all_contexts) {
        ctx->shutdown();
    }

    // Note: We don't clear _all_contexts here because thread_local_destructor
    // will be called for each thread and needs to find the context in the list
}

void IoUringManager::register_context(IoUringContext* ctx) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_shutdown) {
        return;
    }
    _all_contexts.push_back(ctx);
}

void IoUringManager::unregister_context(IoUringContext* ctx) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = std::find(_all_contexts.begin(), _all_contexts.end(), ctx);
    if (it != _all_contexts.end()) {
        _all_contexts.erase(it);
    }
}

void IoUringManager::thread_local_destructor(void* ptr) {
    if (ptr == nullptr) {
        return;
    }

    auto* ctx = static_cast<IoUringContext*>(ptr);
    auto* manager = IoUringManager::instance();

    // Unregister from manager
    manager->unregister_context(ctx);

    // Shutdown and delete
    ctx->shutdown();
    delete ctx;

    LOG(INFO) << "IoUringContext destroyed for thread (thread exit)";
}

IoUringContext::Stats IoUringManager::get_aggregate_stats() const {
    IoUringContext::Stats aggregate;

    std::lock_guard<std::mutex> lock(_mutex);
    for (const auto* ctx : _all_contexts) {
        const auto& stats = ctx->stats();
        aggregate.reads_submitted += stats.reads_submitted.load(std::memory_order_relaxed);
        aggregate.writes_submitted += stats.writes_submitted.load(std::memory_order_relaxed);
        aggregate.fsyncs_submitted += stats.fsyncs_submitted.load(std::memory_order_relaxed);
        aggregate.completions += stats.completions.load(std::memory_order_relaxed);
        aggregate.errors += stats.errors.load(std::memory_order_relaxed);
        aggregate.sq_full_count += stats.sq_full_count.load(std::memory_order_relaxed);
        aggregate.total_read_bytes += stats.total_read_bytes.load(std::memory_order_relaxed);
        aggregate.total_write_bytes += stats.total_write_bytes.load(std::memory_order_relaxed);
        aggregate.total_read_latency_ns += stats.total_read_latency_ns.load(std::memory_order_relaxed);
        aggregate.total_write_latency_ns += stats.total_write_latency_ns.load(std::memory_order_relaxed);
        // Phase 2: Batching metrics
        aggregate.batches_submitted += stats.batches_submitted.load(std::memory_order_relaxed);
        aggregate.total_batch_size += stats.total_batch_size.load(std::memory_order_relaxed);
        aggregate.syscalls_saved += stats.syscalls_saved.load(std::memory_order_relaxed);
        // Phase 3: Registered buffer metrics
        aggregate.fixed_reads += stats.fixed_reads.load(std::memory_order_relaxed);
        aggregate.fixed_writes += stats.fixed_writes.load(std::memory_order_relaxed);
    }

    return aggregate;
}

size_t IoUringManager::active_context_count() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _all_contexts.size();
}

} // namespace starrocks::io

#endif // WITH_IO_URING
