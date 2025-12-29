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

#include "io/io_uring_input_stream.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/logging.h"
#include "gutil/macros.h"
#include "io/io_error.h"

namespace starrocks::io {

#define CHECK_IS_CLOSED(is_closed)                                                       \
    do {                                                                                 \
        if (UNLIKELY(is_closed)) return Status::InternalError("file has been close()d"); \
    } while (0)

IoUringInputStream::IoUringInputStream(int fd, IoUringContext* ctx) : _fd(fd), _ctx(ctx) {}

IoUringInputStream::~IoUringInputStream() {
    if (_close_on_delete && !_is_closed) {
        auto st = close();
        LOG_IF(ERROR, !st.ok()) << "close() failed: " << st;
    }
}

StatusOr<int64_t> IoUringInputStream::read(void* data, int64_t count) {
    CHECK_IS_CLOSED(_is_closed);

    if (!_ctx || !_ctx->is_initialized()) {
        // Fallback to synchronous pread if io_uring is not available
        ssize_t res;
        RETRY_ON_EINTR(res, ::pread(_fd, static_cast<char*>(data), count, _offset));
        if (UNLIKELY(res < 0)) {
            _errno = errno;
            return io_error("read", _errno);
        }
        _offset += res;
        return res;
    }

    // Submit read via io_uring
    auto future = _ctx->submit_read(_fd, data, count, _offset);

    // Submit and wait for completion
    if (_ctx->in_batch_mode()) {
        RETURN_IF_ERROR(_ctx->submit_batch());
    }

    // Poll until complete
    while (future.wait_for(std::chrono::milliseconds(0)) != bthreads::future_status::ready) {
        RETURN_IF_ERROR(_ctx->poll_completions(1));
    }

    auto result = future.get();
    if (!result.ok()) {
        return result.status();
    }

    size_t bytes_read = result.value();
    _offset += bytes_read;
    return static_cast<int64_t>(bytes_read);
}

bthreads::Future<StatusOr<size_t>> IoUringInputStream::read_async(void* data, int64_t count) {
    if (_is_closed) {
        bthreads::Promise<StatusOr<size_t>> promise;
        auto future = promise.get_future();
        promise.set_value(Status::InternalError("file has been close()d"));
        return future;
    }

    if (!_ctx || !_ctx->is_initialized()) {
        // Fallback: execute synchronously and return completed future
        bthreads::Promise<StatusOr<size_t>> promise;
        auto future = promise.get_future();

        ssize_t res;
        RETRY_ON_EINTR(res, ::pread(_fd, static_cast<char*>(data), count, _offset));
        if (UNLIKELY(res < 0)) {
            _errno = errno;
            promise.set_value(io_error("read", _errno));
        } else {
            _offset += res;
            promise.set_value(static_cast<size_t>(res));
        }
        return future;
    }

    // Submit read via io_uring (don't wait for completion)
    // NOTE: We capture the current offset for the read, but don't update _offset here.
    // The caller is responsible for updating the stream position after consuming
    // the future result, since we don't know how many bytes will actually be read
    // until the async operation completes.
    return _ctx->submit_read(_fd, data, count, _offset);
}

StatusOr<int64_t> IoUringInputStream::get_size() {
    CHECK_IS_CLOSED(_is_closed);
    struct stat st;
    auto res = ::fstat(_fd, &st);
    if (res != 0) {
        _errno = errno;
        return io_error("fstat", _errno);
    }
    return st.st_size;
}

Status IoUringInputStream::seek(int64_t offset) {
    if (offset < 0) {
        return Status::InvalidArgument(fmt::format("Invalid offset {}", offset));
    }
    _offset = offset;
    return Status::OK();
}

Status IoUringInputStream::close() {
    CHECK_IS_CLOSED(_is_closed);
    int res;
    RETRY_ON_EINTR(res, ::close(_fd));
    _is_closed = true;
    if (res != 0) {
        _errno = errno;
        return io_error("close", _errno);
    }
    return Status::OK();
}

#undef CHECK_IS_CLOSED

} // namespace starrocks::io

#endif // WITH_IO_URING
