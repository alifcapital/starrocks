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

#include "io/io_uring_context.h"
#include "io/seekable_input_stream.h"

namespace starrocks::io {

// IoUringInputStream is a SeekableInputStream implementation that uses io_uring for async reads.
//
// This implementation is designed for spill restore operations where:
// - Reads may benefit from async I/O to hide latency
// - The caller can overlap computation with I/O by using read_async()
//
// Note: For simple sequential reads, the synchronous read() is still efficient
// because io_uring provides lower syscall overhead even for individual operations.
class IoUringInputStream : public SeekableInputStream {
public:
    // Create an IoUringInputStream.
    // @param fd: Open file descriptor (IoUringInputStream takes ownership if close_on_delete is set)
    // @param ctx: IoUringContext for async I/O (must remain valid for lifetime of stream)
    explicit IoUringInputStream(int fd, IoUringContext* ctx);

    ~IoUringInputStream() override;

    // Disable copy and move
    IoUringInputStream(const IoUringInputStream&) = delete;
    IoUringInputStream(IoUringInputStream&&) = delete;
    void operator=(const IoUringInputStream&) = delete;
    void operator=(IoUringInputStream&&) = delete;

    // SeekableInputStream interface

    // Read up to |count| bytes into |data|.
    // Returns the number of bytes actually read, which may be less than |count|.
    // Returns 0 on end-of-file.
    StatusOr<int64_t> read(void* data, int64_t count) override;

    // Get the total file size in bytes.
    StatusOr<int64_t> get_size() override;

    // Get the current stream position.
    StatusOr<int64_t> position() override { return _offset; }

    // Seek to the specified position.
    Status seek(int64_t offset) override;

    // Close the underlying file.
    // Returns error if close() fails.
    Status close();

    // By default, the file descriptor is not closed when the stream is destroyed.
    // Call set_close_on_delete(true) to close the fd on destruction.
    void set_close_on_delete(bool value) { _close_on_delete = value; }

    // Get the errno from the last failed I/O operation, or 0 if no error.
    int get_errno() const { return _errno; }

    // Async read interface for overlapping computation with I/O.
    // Returns a Future that will contain the number of bytes read or error.
    //
    // IMPORTANT: This method does NOT update the stream position.
    // After the future completes, the caller MUST call advance_offset()
    // with the actual number of bytes read to maintain correct stream position.
    //
    // Example:
    //   auto future = stream->read_async(buf, count);
    //   // ... do other work ...
    //   auto result = future.get();
    //   if (result.ok()) {
    //       stream->advance_offset(result.value());
    //   }
    bthreads::Future<StatusOr<size_t>> read_async(void* data, int64_t count);

    // Advance the stream position by |bytes| after an async read completes.
    // This should be called after read_async() completes with the actual bytes read.
    void advance_offset(size_t bytes) { _offset += bytes; }

private:
    int _fd;
    int _errno{0};
    int64_t _offset{0};
    bool _close_on_delete{false};
    bool _is_closed{false};
    IoUringContext* _ctx;
};

} // namespace starrocks::io

#endif // WITH_IO_URING
