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
#include <string>
#include <vector>

#include "common/statusor.h"
#include "fs/fs.h"
#include "io/io_uring_context.h"
#include "util/bthreads/future.h"

namespace starrocks {

// IoUringWritableFile is a WritableFile implementation that uses io_uring for async I/O.
//
// This implementation is designed for spill-to-disk operations where:
// - Writes are sequential (appending to file)
// - High throughput is more important than individual operation latency
// - Batching multiple writes reduces syscall overhead
//
// Buffer Safety: This implementation copies caller data to internal buffers before
// submitting to io_uring. This ensures the caller can safely reuse their buffers
// immediately after append() returns, maintaining the synchronous WritableFile contract.
// The internal buffers are freed automatically when the write completes.
//
// The caller must ensure poll_completions() is called periodically to process completions.
// This is typically done after each batch of writes or before sync/close.
class IoUringWritableFile : public WritableFile {
public:
    // Create an IoUringWritableFile.
    // @param filename: Path to the file
    // @param fd: Open file descriptor (IoUringWritableFile takes ownership)
    // @param filesize: Current file size (for append mode)
    // @param sync_on_close: Whether to sync before closing
    // @param ctx: IoUringContext for async I/O (must remain valid for lifetime of file)
    IoUringWritableFile(std::string filename, int fd, uint64_t filesize, bool sync_on_close, io::IoUringContext* ctx);

    ~IoUringWritableFile() override;

    // WritableFile interface
    Status append(const Slice& data) override;
    Status appendv(const Slice* data, size_t cnt) override;
    Status pre_allocate(uint64_t size) override;
    Status close() override;
    Status flush(FlushMode mode) override;
    Status sync() override;
    uint64_t size() const override;
    const std::string& filename() const override;

private:
    // Wait for all pending write operations to complete
    Status wait_for_pending_writes();

    // Submit pending operations if batching threshold is reached
    Status maybe_submit_batch();

    // Represents a pending async write with its owned buffer.
    // The buffer must remain valid until the io_uring CQE arrives.
    struct PendingWrite {
        bthreads::Future<StatusOr<size_t>> future;
        std::unique_ptr<char[]> buffer;  // Owned copy of data being written
        size_t expected_bytes;            // Expected bytes to write
        uint64_t offset;                  // File offset where this write goes

        PendingWrite(bthreads::Future<StatusOr<size_t>>&& f, std::unique_ptr<char[]>&& buf,
                     size_t expected, uint64_t off)
                : future(std::move(f)), buffer(std::move(buf)), expected_bytes(expected), offset(off) {}
    };

    std::string _filename;
    int _fd;
    io::IoUringContext* _ctx;
    const bool _sync_on_close;
    bool _pending_sync{false};
    bool _closed{false};

    // File size tracking:
    // - _next_write_offset: where the next append will write (incremented on submission)
    // - _committed_size: highest offset with successfully completed writes
    // These are kept separate to correctly handle async I/O failures and short writes.
    // On close(), the file is truncated to _committed_size to avoid zero-filled holes.
    uint64_t _next_write_offset{0};
    uint64_t _committed_size{0};
    uint64_t _pre_allocated_size{0};

    // Track pending writes with their owned buffers for safe async I/O
    std::vector<PendingWrite> _pending_writes;

    // Batching state
    size_t _batch_writes_count{0};
};

} // namespace starrocks

#endif // WITH_IO_URING
