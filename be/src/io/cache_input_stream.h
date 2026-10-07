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

#include <atomic>
#include <cerrno>
#include <memory>
#include <string>
#include <unordered_set>

#include "cache/disk_cache/block_cache.h"
#include "cache/disk_cache/io_buffer.h"
#include "io/shared_buffered_input_stream.h"

namespace starrocks::io {

class CacheInputStream : public SeekableInputStreamWrapper {
public:
    struct Stats {
        int64_t read_block_cache_ns = 0;
        int64_t write_block_cache_ns = 0;
        int64_t read_block_cache_count = 0;
        int64_t write_block_cache_count = 0;
        int64_t write_mem_cache_bytes = 0;
        int64_t write_disk_cache_bytes = 0;
        int64_t read_block_cache_bytes = 0;
        int64_t read_mem_cache_bytes = 0;
        int64_t read_disk_cache_bytes = 0;
        int64_t read_peer_cache_bytes = 0;
        int64_t read_peer_cache_count = 0;
        int64_t read_peer_cache_ns = 0;
        int64_t write_block_cache_bytes = 0;
        int64_t skip_read_cache_count = 0;
        int64_t skip_read_cache_bytes = 0;
        int64_t skip_read_peer_cache_count = 0;
        int64_t skip_read_peer_cache_bytes = 0;
        // AlreadyExist plus ResourceBusy. Dashboards read this sum, so we keep it next to the split below.
        int64_t skip_write_cache_count = 0;
        int64_t skip_write_cache_bytes = 0;
        int64_t write_cache_already_exist_count = 0;
        int64_t write_cache_already_exist_bytes = 0;
        int64_t write_cache_busy_count = 0;
        int64_t write_cache_busy_bytes = 0;
        int64_t write_cache_mem_limit_count = 0;
        int64_t write_cache_mem_limit_bytes = 0;
        int64_t write_cache_capacity_limit_count = 0;
        int64_t write_cache_capacity_limit_bytes = 0;
        // Writes of a block that the cache already rejected once in this stream. Such a write is also
        // counted again under its own result, so these show how much of the counters above is repeats.
        int64_t write_cache_retry_count = 0;
        int64_t write_cache_retry_bytes = 0;
        int64_t write_cache_fail_count = 0;
        int64_t write_cache_fail_bytes = 0;
        // Results of async writes, as the cache reports them to the write callback.
        int64_t async_write_done_count = 0;
        int64_t async_write_fail_count = 0;
        int64_t async_write_exist_count = 0;
        int64_t read_block_buffer_bytes = 0;
        int64_t read_block_buffer_count = 0;

        // Fold another stream's counters in. Used when a scan reads through more than one
        // CacheInputStream (e.g. CACHE SELECT routes reader-owned reads through a second
        // populate stream) and the per-stream stats must be reported as a single total.
        Stats& operator+=(const Stats& o) {
            read_block_cache_ns += o.read_block_cache_ns;
            write_block_cache_ns += o.write_block_cache_ns;
            read_block_cache_count += o.read_block_cache_count;
            write_block_cache_count += o.write_block_cache_count;
            write_mem_cache_bytes += o.write_mem_cache_bytes;
            write_disk_cache_bytes += o.write_disk_cache_bytes;
            read_block_cache_bytes += o.read_block_cache_bytes;
            read_mem_cache_bytes += o.read_mem_cache_bytes;
            read_disk_cache_bytes += o.read_disk_cache_bytes;
            read_peer_cache_bytes += o.read_peer_cache_bytes;
            read_peer_cache_count += o.read_peer_cache_count;
            read_peer_cache_ns += o.read_peer_cache_ns;
            write_block_cache_bytes += o.write_block_cache_bytes;
            skip_read_cache_count += o.skip_read_cache_count;
            skip_read_cache_bytes += o.skip_read_cache_bytes;
            skip_read_peer_cache_count += o.skip_read_peer_cache_count;
            skip_read_peer_cache_bytes += o.skip_read_peer_cache_bytes;
            skip_write_cache_count += o.skip_write_cache_count;
            skip_write_cache_bytes += o.skip_write_cache_bytes;
            write_cache_already_exist_count += o.write_cache_already_exist_count;
            write_cache_already_exist_bytes += o.write_cache_already_exist_bytes;
            write_cache_busy_count += o.write_cache_busy_count;
            write_cache_busy_bytes += o.write_cache_busy_bytes;
            write_cache_mem_limit_count += o.write_cache_mem_limit_count;
            write_cache_mem_limit_bytes += o.write_cache_mem_limit_bytes;
            write_cache_capacity_limit_count += o.write_cache_capacity_limit_count;
            write_cache_capacity_limit_bytes += o.write_cache_capacity_limit_bytes;
            write_cache_retry_count += o.write_cache_retry_count;
            write_cache_retry_bytes += o.write_cache_retry_bytes;
            write_cache_fail_count += o.write_cache_fail_count;
            write_cache_fail_bytes += o.write_cache_fail_bytes;
            async_write_done_count += o.async_write_done_count;
            async_write_fail_count += o.async_write_fail_count;
            async_write_exist_count += o.async_write_exist_count;
            read_block_buffer_bytes += o.read_block_buffer_bytes;
            read_block_buffer_count += o.read_block_buffer_count;
            return *this;
        }
    };

    explicit CacheInputStream(const std::shared_ptr<SharedBufferedInputStream>& stream, const std::string& filename,
                              size_t size, int64_t modification_time);

    ~CacheInputStream() override;

    StatusOr<int64_t> read(void* data, int64_t count) override;

    Status read_at_fully(int64_t offset, void* data, int64_t count) override;

    Status seek(int64_t offset) override;

    StatusOr<int64_t> position() override;

    StatusOr<int64_t> get_size() override;

    // The async write counters are copied in at each call, so a caller that keeps the reference
    // sees them as of its last call.
    const Stats& stats();

    void set_enable_populate_cache(bool v) { _enable_populate_cache = v; }

    void set_enable_async_populate_mode(bool v) { _enable_async_populate_mode = v; }

    void set_enable_block_buffer(bool v) { _enable_block_buffer = v; }

    void set_enable_cache_io_adaptor(bool v) { _enable_cache_io_adaptor = v; }

    void set_priority(const int8_t priority) { _priority = priority; }

    void set_frequency(const int8_t frequency) { _frequency = frequency; }

    void set_ttl_seconds(const uint64_t ttl_seconds) { _ttl_seconds = ttl_seconds; }

    void set_peer_cache_node(const std::string& peer_node);

    int64_t get_align_size() const;

    StatusOr<std::string_view> peek(int64_t count) override;

    Status skip(int64_t count) override {
        _offset += count;
        return _sb_stream->skip(count);
    }

protected:
    struct BlockBuffer {
        int64_t offset;
        IOBuffer buffer;
    };
    using SharedBufferPtr = SharedBufferedInputStream::SharedBufferPtr;

    // The cache runs an async write in its own threads and reports the result only to the write
    // callback, possibly after this stream is destroyed. So the callback holds these counters by
    // shared_ptr, and stats() copies them into Stats.
    struct AsyncWriteStats {
        std::atomic<int64_t> done_count{0};
        std::atomic<int64_t> fail_count{0};
        std::atomic<int64_t> exist_count{0};

        void record(int code) {
            if (code == 0) {
                done_count.fetch_add(1, std::memory_order_relaxed);
            } else if (code == EEXIST) {
                exist_count.fetch_add(1, std::memory_order_relaxed);
            } else {
                fail_count.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    // Read block from local, if not found, will return Status::NotFound();
    virtual Status _read_block_from_local(const int64_t offset, const int64_t size, char* out);
    // Read multiple blocks from remote
    virtual Status _read_blocks_from_remote(const int64_t offset, const int64_t size, char* out);
    Status _read_from_cache(const int64_t offset, const int64_t size, const int64_t block_offset,
                            const int64_t block_size, char* out);
    Status _read_peer_cache(off_t offset, size_t size, IOBuffer* iobuf, DiskCacheReadOptions* options);
    void _populate_to_cache(const char* src, int64_t offset, int64_t count, const SharedBufferPtr& sb);
    void _write_cache(int64_t offset, const IOBuffer& iobuf, DiskCacheWriteOptions* options);

    void _deduplicate_shared_buffer(const SharedBufferPtr& sb);
    bool _can_ignore_populate_error(const Status& status) const;
    bool _can_try_peer_cache();

    std::string _cache_key;
    std::string _filename;
    std::shared_ptr<SharedBufferedInputStream> _sb_stream;
    int64_t _offset;
    int64_t _buffer_size;
    std::string _buffer;
    // The file range [_buffer_offset, _buffer_offset + _buffer_valid_size) that `_buffer` holds after the
    // last read from remote storage. _buffer_valid_size == 0 means `_buffer` holds nothing usable.
    int64_t _buffer_offset = 0;
    int64_t _buffer_valid_size = 0;
    Stats _stats;
    int64_t _size;
    bool _enable_populate_cache = false;
    bool _enable_async_populate_mode = false;
    bool _enable_block_buffer = false;
    bool _enable_cache_io_adaptor = false;

    std::string _peer_host;
    int32_t _peer_port = 0;

    BlockCache* _cache = nullptr;
    int64_t _block_size = 0;
    std::unordered_map<int64_t, BlockBuffer> _block_map;
    int8_t _priority = 0;
    uint64_t _ttl_seconds = 0;
    int8_t _frequency = 0;

private:
    inline int64_t _calculate_remote_latency_per_block(int64_t io_bytes, int64_t read_time_ns);
    // Record already populated blocks, avoid duplicate populate
    std::unordered_set<int64_t> _already_populated_blocks{};
    // Blocks the cache rejected in this stream. Used only to count repeated writes of the same block.
    std::unordered_set<int64_t> _rejected_populate_blocks{};
    std::shared_ptr<AsyncWriteStats> _async_write_stats = std::make_shared<AsyncWriteStats>();
};

} // namespace starrocks::io
