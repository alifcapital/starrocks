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

#include <memory>
#include <mutex>
#include <vector>

#include "io/io_uring_context.h"

namespace starrocks::io {

// IoUringManager manages thread-local IoUringContext instances.
//
// Design: Ring-per-thread architecture
// - Each thread that needs async I/O gets its own IoUringContext
// - The manager tracks all contexts for global shutdown
// - Thread-local storage ensures no synchronization on the hot path
//
// Usage:
//   auto* ctx = IoUringManager::instance()->get_or_create_context();
//   if (ctx != nullptr && ctx->is_initialized()) {
//       auto future = ctx->submit_write(...);
//       ctx->poll_completions();
//       auto result = future.get();
//   }
class IoUringManager {
public:
    // Get the singleton instance
    static IoUringManager* instance();

    ~IoUringManager();

    // Non-copyable, non-movable
    IoUringManager(const IoUringManager&) = delete;
    IoUringManager& operator=(const IoUringManager&) = delete;

    // Get or create the thread-local IoUringContext.
    // Returns nullptr if io_uring is not supported or disabled.
    // The returned context is owned by the manager and valid until thread exit or shutdown.
    IoUringContext* get_or_create_context();

    // Release the thread-local context (called automatically on thread exit).
    // This can also be called explicitly to release resources early.
    void release_context();

    // Global shutdown - releases all contexts.
    // Should be called during BE shutdown.
    void shutdown_all();

    // Check if io_uring is enabled and supported
    bool is_enabled() const;

    // Get aggregate statistics from all contexts
    IoUringContext::Stats get_aggregate_stats() const;

    // Get the number of active contexts
    size_t active_context_count() const;

private:
    IoUringManager();

    // Register a context for tracking (called when creating thread-local context)
    void register_context(IoUringContext* ctx);

    // Unregister a context (called when thread exits)
    void unregister_context(IoUringContext* ctx);

    // Thread-local context destructor callback
    static void thread_local_destructor(void* ptr);

    // Mutex for protecting the contexts list (only used for registration/shutdown, not hot path)
    mutable std::mutex _mutex;

    // Track all active contexts for shutdown and stats aggregation
    std::vector<IoUringContext*> _all_contexts;

    // Whether the manager has been shut down
    bool _shutdown{false};

    // Pthread key for thread-local storage
    pthread_key_t _tls_key;
    bool _tls_key_created{false};
};

// Convenience function to get thread-local context
// Returns nullptr if io_uring is not available
inline IoUringContext* get_io_uring_context() {
    return IoUringManager::instance()->get_or_create_context();
}

} // namespace starrocks::io

#endif // WITH_IO_URING
