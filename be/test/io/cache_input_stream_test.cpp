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

#include "io/cache_input_stream.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <cerrno>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "cache/datacache.h"
#include "cache/disk_cache/starcache_engine.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "fs/fs_util.h"
#include "runtime/exec_env.h"
#include "testutil/assert.h"

namespace starrocks::io {

class MockSeekableInputStream : public io::SeekableInputStream {
public:
    explicit MockSeekableInputStream(char* contents, int64_t size) : _contents(contents), _size(size) {}

    StatusOr<int64_t> read(void* data, int64_t count) override {
        count = std::min(count, _size - _offset);
        memcpy(data, &_contents[_offset], count);
        _offset += count;
        return count;
    }

    Status seek(int64_t position) override {
        _offset = std::min<int64_t>(position, _size);
        return Status::OK();
    }

    StatusOr<int64_t> position() override { return _offset; }

    StatusOr<int64_t> get_size() override { return _size; }

private:
    const char* _contents;
    int64_t _size;
    int64_t _offset{0};
};

// A cache that holds nothing and answers every write with the status the test sets. We use it to
// drive CacheInputStream through each populate result that a real cache returns only under load.
class FakeWriteResultCacheEngine : public LocalDiskCacheEngine {
public:
    bool is_initialized() const override { return true; }

    Status write(const std::string& key, const IOBuffer& buffer, DiskCacheWriteOptions* options) override {
        write_calls += 1;
        if (options != nullptr && options->async && options->callback) {
            if (defer_callbacks) {
                pending_callbacks.push_back(options->callback);
            } else {
                options->callback(async_code, "");
            }
        }
        return write_status;
    }

    Status read(const std::string& key, size_t off, size_t size, IOBuffer* buffer,
                DiskCacheReadOptions* options) override {
        return Status::NotFound("fake cache holds nothing");
    }

    bool exist(const std::string& key) const override { return false; }
    Status remove(const std::string& key) override { return Status::OK(); }
    Status update_disk_spaces(const std::vector<DirSpace>& spaces) override { return Status::OK(); }
    Status update_inline_cache_count_limit(int32_t limit) override { return Status::OK(); }
    const DataCacheDiskMetrics cache_metrics() const override { return {}; }
    void record_read_remote(size_t size, int64_t latency_us) override {}
    void record_read_cache(size_t size, int64_t latency_us) override {}
    Status shutdown() override { return Status::OK(); }
    bool has_disk_cache() const override { return false; }
    bool available() const override { return true; }
    void disk_spaces(std::vector<DirSpace>* spaces) const override {}
    size_t lookup_count() const override { return 0; }
    size_t hit_count() const override { return 0; }
    Status prune() override { return Status::OK(); }

    Status write_status = Status::OK();
    int async_code = 0;
    bool defer_callbacks = false;
    int64_t write_calls = 0;
    std::vector<std::function<void(int, const std::string&)>> pending_callbacks;
};

// A remote file that records every read it serves, so a test sees which ranges the stream downloads.
class CountingSeekableInputStream : public io::SeekableInputStream {
public:
    CountingSeekableInputStream(const char* contents, int64_t size) : _contents(contents), _size(size) {}

    StatusOr<int64_t> read(void* data, int64_t count) override {
        count = std::min(count, _size - _offset);
        memcpy(data, _contents + _offset, count);
        _offset += count;
        return count;
    }

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        if (offset < 0 || count < 0 || offset + count > _size) {
            return Status::EndOfFile("read out of the file");
        }
        reads.emplace_back(offset, count);
        bytes += count;
        memcpy(out, _contents + offset, count);
        return Status::OK();
    }

    Status seek(int64_t position) override {
        _offset = std::min<int64_t>(position, _size);
        return Status::OK();
    }

    StatusOr<int64_t> position() override { return _offset; }

    StatusOr<int64_t> get_size() override { return _size; }

    // (offset, size) of each read, in the order of the reads.
    std::vector<std::pair<int64_t, int64_t>> reads;
    int64_t bytes = 0;

private:
    const char* _contents;
    int64_t _size;
    int64_t _offset{0};
};

// A cache that keeps blocks in memory and counts what the stream asks it. The test fills it block by block,
// and the key sets below model a cache that refuses a read or loses a block at the worst moment.
class InMemoryCacheEngine : public LocalDiskCacheEngine {
public:
    bool is_initialized() const override { return true; }

    Status write(const std::string& key, const IOBuffer& buffer, DiskCacheWriteOptions* options) override {
        write_calls += 1;
        if (blocks.count(key) > 0 && (options == nullptr || !options->overwrite)) {
            return Status::AlreadyExist("block exists");
        }
        std::string data(buffer.size(), 0);
        buffer.copy_to(data.data());
        blocks[key] = std::move(data);
        if (options != nullptr && options->async && options->callback) {
            options->callback(0, "");
        }
        return Status::OK();
    }

    Status read(const std::string& key, size_t off, size_t size, IOBuffer* buffer,
                DiskCacheReadOptions* options) override {
        read_calls += 1;
        if (busy_keys.count(key) > 0) {
            return Status::ResourceBusy("cache read is refused");
        }
        auto it = blocks.find(key);
        if (it == blocks.end()) {
            return Status::NotFound("block is not in the cache");
        }
        if (off + size > it->second.size()) {
            return Status::InvalidArgument("read out of the block");
        }
        buffer->raw_buf().append(it->second.data() + off, size);
        return Status::OK();
    }

    bool exist(const std::string& key) const override {
        exist_calls += 1;
        auto it = blocks.find(key);
        if (it == blocks.end()) {
            return false;
        }
        if (evict_on_exist.count(key) > 0) {
            blocks.erase(it);
        }
        return true;
    }

    Status remove(const std::string& key) override {
        blocks.erase(key);
        return Status::OK();
    }
    Status update_disk_spaces(const std::vector<DirSpace>& spaces) override { return Status::OK(); }
    Status update_inline_cache_count_limit(int32_t limit) override { return Status::OK(); }
    const DataCacheDiskMetrics cache_metrics() const override { return {}; }
    void record_read_remote(size_t size, int64_t latency_us) override {}
    void record_read_cache(size_t size, int64_t latency_us) override {}
    Status shutdown() override { return Status::OK(); }
    bool has_disk_cache() const override { return false; }
    bool available() const override { return true; }
    void disk_spaces(std::vector<DirSpace>* spaces) const override {}
    size_t lookup_count() const override { return 0; }
    size_t hit_count() const override { return 0; }
    Status prune() override { return Status::OK(); }

    mutable std::map<std::string, std::string> blocks;
    // The read of these blocks answers ResourceBusy, like the IO adaptor does when remote storage is faster.
    std::set<std::string> busy_keys;
    // exist() answers true for these blocks and drops them right after, like an eviction between exist() and read().
    std::set<std::string> evict_on_exist;
    mutable int64_t exist_calls = 0;
    int64_t read_calls = 0;
    int64_t write_calls = 0;
};

class CacheInputStreamTest : public ::testing::Test {
public:
    static DiskCacheOptions cache_options() {
        DiskCacheOptions options;
        options.mem_space_size = 100 * MB;
        options.enable_checksum = false;
        options.max_concurrent_inserts = 1500000;
        options.max_flying_memory_mb = 100;
        options.block_size = block_size;
        options.skip_read_factor = 1.0;
        return options;
    }

    static void TearDownTestCase() {
        auto cache = BlockCache::instance();
        if (cache) {
            BlockCache::instance()->shutdown();
        }
    }

    void SetUp() override {
        _saved_enable_auto_adjust = config::enable_datacache_disk_auto_adjust;
        config::enable_datacache_disk_auto_adjust = false;

        DiskCacheOptions options = cache_options();
        auto block_cache = TestCacheUtils::create_cache(options);
        DataCache::GetInstance()->set_block_cache(block_cache);
    }
    void TearDown() override { config::enable_datacache_disk_auto_adjust = _saved_enable_auto_adjust; }

    static void read_stream_data(io::SeekableInputStream* stream, int64_t offset, int64_t size, char* data) {
        ASSERT_OK(stream->seek(offset));
        auto res = stream->read(data, size);
        ASSERT_TRUE(res.ok());
    }

    static void gen_test_data(char* data, int64_t size, int64_t block_size) {
        for (int i = 0; i <= (size - 1) / block_size; ++i) {
            int64_t offset = i * block_size;
            int64_t count = std::min(block_size, size - offset);
            memset(data + offset, 'a' + i, count);
        }
    }

    static bool check_data_content(char* data, int64_t size, char content) {
        for (int i = 0; i < size; ++i) {
            if (data[i] != content) {
                return false;
            }
        }
        return true;
    }

    static const int64_t block_size;

private:
    bool _saved_enable_auto_adjust = false;
};

const int64_t CacheInputStreamTest::block_size = 256 * 1024;

TEST_F(CacheInputStreamTest, test_aligned_read) {
    const int64_t block_count = 3;

    int64_t data_size = block_size * block_count;
    char data[data_size + 1];
    gen_test_data(data, data_size, block_size);

    const std::string file_name = "test_file1";
    std::shared_ptr<io::SeekableInputStream> stream(new MockSeekableInputStream(data, data_size));
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream(
            new io::SharedBufferedInputStream(stream, file_name, data_size));
    io::CacheInputStream cache_stream(sb_stream, file_name, data_size, 1000000);
    cache_stream.set_enable_populate_cache(true);
    auto& stats = cache_stream.stats();

    // first read from backend
    for (int i = 0; i < block_count; ++i) {
        char buffer[block_size];
        read_stream_data(&cache_stream, i * block_size, block_size, buffer);
        ASSERT_TRUE(check_data_content(buffer, block_size, 'a' + i));
    }
    ASSERT_EQ(stats.read_block_cache_count, 0);
    ASSERT_EQ(stats.write_block_cache_count, block_count);

    // first read from cache
    // We expect all blocks to come from the cache only through a fresh stream, because the first stream
    // still holds its last remotely read block in its own buffer and would serve that block from there.
    io::CacheInputStream cache_stream2(sb_stream, file_name, data_size, 1000000);
    cache_stream2.set_enable_populate_cache(true);
    auto& stats2 = cache_stream2.stats();
    for (int i = 0; i < block_count; ++i) {
        char buffer[block_size];
        read_stream_data(&cache_stream2, i * block_size, block_size, buffer);
        ASSERT_TRUE(check_data_content(buffer, block_size, 'a' + i));
    }
    ASSERT_EQ(stats2.read_block_cache_count, block_count);
    ASSERT_EQ(stats2.read_block_buffer_count, 0);
}

TEST_F(CacheInputStreamTest, test_random_read) {
    const int64_t block_count = 3;

    const int64_t data_size = block_size * block_count;
    char data[data_size + 1];
    gen_test_data(data, data_size, block_size);

    const std::string file_name = "test_file2";
    std::shared_ptr<io::SeekableInputStream> stream(new MockSeekableInputStream(data, data_size));
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream(
            new io::SharedBufferedInputStream(stream, file_name, data_size));
    io::CacheInputStream cache_stream(sb_stream, file_name, data_size, 1000000);
    cache_stream.set_enable_populate_cache(true);
    auto& stats = cache_stream.stats();

    // first read from backend
    for (int i = 0; i < block_count; ++i) {
        char buffer[block_size];
        read_stream_data(&cache_stream, i * block_size, block_size, buffer);
        ASSERT_TRUE(check_data_content(buffer, block_size, 'a' + i));
    }
    ASSERT_EQ(stats.read_block_cache_count, 0);
    ASSERT_EQ(stats.write_block_cache_count, block_count);

    // seek to a custom postion in second block, and read multiple block
    // We expect both blocks to come from the cache only through a fresh stream, because the first stream
    // still holds its last remotely read block in its own buffer and would serve that block from there.
    io::CacheInputStream cache_stream2(sb_stream, file_name, data_size, 1000000);
    cache_stream2.set_enable_populate_cache(true);
    auto& stats2 = cache_stream2.stats();
    int64_t off_in_block = 100;
    ASSERT_OK(cache_stream2.seek(block_size + off_in_block));
    ASSERT_EQ(cache_stream2.position().value(), block_size + off_in_block);

    char buffer[block_size * 2];
    auto res = cache_stream2.read(buffer, block_size * 2);
    ASSERT_TRUE(res.ok());

    ASSERT_TRUE(check_data_content(buffer, block_size - off_in_block, 'a' + 1));
    ASSERT_TRUE(check_data_content(buffer + block_size - off_in_block, block_size, 'a' + 2));

    ASSERT_EQ(stats2.read_block_cache_count, 2);
    ASSERT_EQ(stats2.read_block_buffer_count, 0);
}

TEST_F(CacheInputStreamTest, test_file_overwrite) {
    const int64_t block_count = 3;

    int64_t data_size = block_size * block_count;
    char data[data_size + 1];
    gen_test_data(data, data_size, block_size);

    const std::string file_name = "test_file3";
    std::shared_ptr<io::SeekableInputStream> stream(new MockSeekableInputStream(data, data_size));
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream(
            new io::SharedBufferedInputStream(stream, file_name, data_size));
    io::CacheInputStream cache_stream(sb_stream, file_name, data_size, 1000000);
    cache_stream.set_enable_populate_cache(true);
    auto& stats = cache_stream.stats();

    // first read from backend
    for (int i = 0; i < block_count; ++i) {
        char buffer[block_size];
        read_stream_data(&cache_stream, i * block_size, block_size, buffer);
        ASSERT_TRUE(check_data_content(buffer, block_size, 'a' + i));
    }
    ASSERT_EQ(stats.read_block_cache_count, 0);
    ASSERT_EQ(stats.write_block_cache_count, block_count);

    // first read from cache
    // We expect all blocks to come from the cache only through a fresh stream, because the first stream
    // still holds its last remotely read block in its own buffer and would serve that block from there.
    io::CacheInputStream cache_stream_same_file(sb_stream, file_name, data_size, 1000000);
    cache_stream_same_file.set_enable_populate_cache(true);
    auto& stats_same_file = cache_stream_same_file.stats();
    for (int i = 0; i < block_count; ++i) {
        char buffer[block_size];
        read_stream_data(&cache_stream_same_file, i * block_size, block_size, buffer);
        ASSERT_TRUE(check_data_content(buffer, block_size, 'a' + i));
    }
    ASSERT_EQ(stats_same_file.read_block_cache_count, block_count);
    ASSERT_EQ(stats_same_file.read_block_buffer_count, 0);

    // With different modification time, the old cache cannot be used
    io::CacheInputStream cache_stream2(sb_stream, file_name, data_size, 2000000);
    cache_stream2.set_enable_populate_cache(true);
    auto& stats2 = cache_stream2.stats();
    for (int i = 0; i < block_count; ++i) {
        char buffer[block_size];
        read_stream_data(&cache_stream2, i * block_size, block_size, buffer);
        ASSERT_TRUE(check_data_content(buffer, block_size, 'a' + i));
    }
    ASSERT_EQ(stats2.read_block_cache_count, 0);
}

TEST_F(CacheInputStreamTest, test_read_from_io_buffer) {
    const int64_t block_count = 1;

    int64_t data_size = block_size * block_count;
    char data[data_size + 1];
    gen_test_data(data, data_size, block_size);

    const std::string file_name = "test_file3";
    std::shared_ptr<io::SeekableInputStream> stream(new MockSeekableInputStream(data, data_size));
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream(
            new io::SharedBufferedInputStream(stream, file_name, data_size));
    io::CacheInputStream cache_stream(sb_stream, file_name, data_size, 1000);
    cache_stream.set_enable_populate_cache(true);
    cache_stream.set_enable_block_buffer(true);
    auto& stats = cache_stream.stats();

    // read from backend, cache the data
    char buffer[block_size];
    read_stream_data(&cache_stream, 0, block_size, buffer);
    ASSERT_TRUE(check_data_content(buffer, block_size, 'a'));
    ASSERT_EQ(stats.read_block_cache_count, 0);
    ASSERT_EQ(stats.write_block_cache_count, 1);

    // read the first 1024 bytes from cache, actually it will read the whole block from cache
    // and save it to block buffer.
    // We expect the cache read only through a fresh stream, because the first stream still holds the
    // remotely read block in its own buffer and would serve this read from there.
    io::CacheInputStream cache_stream2(sb_stream, file_name, data_size, 1000);
    cache_stream2.set_enable_populate_cache(true);
    cache_stream2.set_enable_block_buffer(true);
    auto& stats2 = cache_stream2.stats();
    read_stream_data(&cache_stream2, 0, 1024, buffer);
    ASSERT_TRUE(check_data_content(buffer, block_size, 'a'));
    ASSERT_EQ(stats2.read_block_cache_count, 1);
    ASSERT_EQ(stats2.read_block_buffer_count, 0);

    read_stream_data(&cache_stream2, 1024, 1024, buffer);
    ASSERT_TRUE(check_data_content(buffer, block_size, 'a'));
    ASSERT_EQ(stats2.read_block_cache_count, 1);
    ASSERT_EQ(stats2.read_block_buffer_count, 1);
}

TEST_F(CacheInputStreamTest, test_read_zero_copy) {
    int64_t data_size = block_size + 1024;
    char data[data_size + 1];
    gen_test_data(data, data_size, block_size);

    const std::string file_name = "test_file3";
    std::shared_ptr<io::SeekableInputStream> stream(new MockSeekableInputStream(data, data_size));
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream(
            new io::SharedBufferedInputStream(stream, file_name, data_size));
    io::CacheInputStream cache_stream(sb_stream, file_name, data_size, 1000);
    cache_stream.set_enable_populate_cache(true);
    cache_stream.set_enable_block_buffer(false);

    // read from backend, cache the data
    size_t count = data_size - 10;
    char buffer[count];
    read_stream_data(&cache_stream, 10, count, buffer);
    ASSERT_TRUE(check_data_content(buffer, block_size - 10, 'a'));
    ASSERT_TRUE(check_data_content(buffer + block_size - 10, 1024, 'b'));
}

TEST_F(CacheInputStreamTest, test_read_with_zero_range) {
    const int64_t block_count = 1;
    int64_t data_size = block_size * block_count;
    char data[data_size + 1];
    gen_test_data(data, data_size, block_size);

    const std::string file_name = "test_file4";
    std::shared_ptr<io::SeekableInputStream> stream(new MockSeekableInputStream(data, data_size));
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream(
            new io::SharedBufferedInputStream(stream, file_name, data_size));
    io::CacheInputStream cache_stream(sb_stream, file_name, data_size, 1000);
    cache_stream.set_enable_populate_cache(true);
    cache_stream.set_enable_block_buffer(true);
    auto& stats = cache_stream.stats();

    // read from backend, cache the data
    char buffer[block_size];
    read_stream_data(&cache_stream, 0, block_size, buffer);
    ASSERT_TRUE(check_data_content(buffer, block_size, 'a'));
    ASSERT_EQ(stats.read_block_cache_count, 0);
    ASSERT_EQ(stats.write_block_cache_count, 1);

    // try read zero length data, expect no crash
    read_stream_data(&cache_stream, 0, 0, nullptr);
    ASSERT_EQ(stats.read_block_cache_count, 0);
}

TEST_F(CacheInputStreamTest, test_read_with_adaptor) {
    const std::string cache_dir = "./cache_input_stream_cache_dir";
    fs::create_directories(cache_dir);

    DiskCacheOptions options = cache_options();
    // Because the cache adaptor only work for disk cache.
    options.dir_spaces.push_back({.path = cache_dir, .size = 300 * 1024 * 1024});
    auto block_cache = TestCacheUtils::create_cache(options);
    DataCache::GetInstance()->set_block_cache(block_cache);

    const int64_t block_count = 2;

    int64_t data_size = block_size * block_count;
    char data[data_size + 1];
    gen_test_data(data, data_size, block_size);

    const std::string file_name = "test_file5";
    std::shared_ptr<io::SeekableInputStream> stream(new MockSeekableInputStream(data, data_size));
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream(
            new io::SharedBufferedInputStream(stream, file_name, data_size));
    io::CacheInputStream cache_stream(sb_stream, file_name, data_size, 1000000);
    cache_stream.set_enable_populate_cache(true);
    cache_stream.set_enable_cache_io_adaptor(true);
    auto& stats = cache_stream.stats();

    const size_t read_size = block_size * block_count;
    sb_stream->_shared_io_bytes = read_size;
    sb_stream->_shared_io_timer = 10000;

    // first read from backend
    {
        char buffer[read_size];
        read_stream_data(&cache_stream, 0, read_size, buffer);
        ASSERT_TRUE(check_data_content(buffer, block_size, 'a'));
        ASSERT_TRUE(check_data_content(buffer + block_size, block_size, 'b'));
        ASSERT_EQ(stats.read_block_cache_count, 0);
        ASSERT_EQ(stats.write_block_cache_count, block_count);
    }

    auto cache = BlockCache::instance();
    const int kAdaptorWindowSize = 50;

    {
        // Record read latencyr to ensure cache latency > remote latency
        // so all blocks read from remote.
        for (size_t i = 0; i < kAdaptorWindowSize; ++i) {
            cache->record_read_local_cache(read_size, 1000000000);
            cache->record_read_remote_storage(read_size, 10, true);
        }
        // We expect the adaptor to decide this read only through a fresh stream, because a stream that
        // already holds both blocks in its own buffer would serve them without asking the cache.
        io::CacheInputStream cache_stream2(sb_stream, file_name, data_size, 1000000);
        cache_stream2.set_enable_populate_cache(true);
        cache_stream2.set_enable_cache_io_adaptor(true);
        auto& stats2 = cache_stream2.stats();
        char buffer[read_size];
        read_stream_data(&cache_stream2, 0, read_size, buffer);
        ASSERT_TRUE(check_data_content(buffer, block_size, 'a'));
        ASSERT_TRUE(check_data_content(buffer + block_size, block_size, 'b'));
        ASSERT_EQ(stats2.read_block_cache_count, 0);
        ASSERT_EQ(stats2.read_block_buffer_count, 0);
    }

    {
        // Record read latencyr to ensure cache latency < remote latency
        // so all blocks read from cache.
        for (size_t i = 0; i < kAdaptorWindowSize; ++i) {
            cache->record_read_local_cache(read_size, 10);
            cache->record_read_remote_storage(read_size, 1000000000, true);
        }
        // We expect a fresh stream here for the same reason: the previous stream read both blocks
        // from remote and now holds them in its own buffer.
        io::CacheInputStream cache_stream3(sb_stream, file_name, data_size, 1000000);
        cache_stream3.set_enable_populate_cache(true);
        cache_stream3.set_enable_cache_io_adaptor(true);
        auto& stats3 = cache_stream3.stats();
        char buffer[read_size];
        read_stream_data(&cache_stream3, 0, read_size, buffer);
        ASSERT_TRUE(check_data_content(buffer, block_size, 'a'));
        ASSERT_TRUE(check_data_content(buffer + block_size, block_size, 'b'));
        ASSERT_EQ(stats3.read_block_cache_count, block_count);
        ASSERT_EQ(stats3.read_block_buffer_count, 0);
    }
    fs::remove_all(cache_dir);
}

TEST_F(CacheInputStreamTest, test_read_with_shared_buffer) {
    const int64_t block_count = 2;

    int64_t data_size = block_size * block_count;
    char data[data_size + 1];
    gen_test_data(data, data_size, block_size);

    const std::string file_name = "test_file6";
    std::shared_ptr<io::SeekableInputStream> stream(new MockSeekableInputStream(data, data_size));
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream(
            new io::SharedBufferedInputStream(stream, file_name, data_size));
    io::CacheInputStream cache_stream(sb_stream, file_name, data_size, 1000000);
    cache_stream.set_enable_populate_cache(true);
    cache_stream.set_enable_block_buffer(true);

    // Add a dummy block buffer to check the duplicate shared buffer.
    CacheInputStream::BlockBuffer dummy_block_buffer;
    dummy_block_buffer.offset = 10000000;
    cache_stream._block_map[dummy_block_buffer.offset] = dummy_block_buffer;

    const size_t read_size = block_size * block_count;
    std::vector<SharedBufferedInputStream::IORange> io_ranges;
    io_ranges.emplace_back(0, read_size);
    sb_stream->set_io_ranges(io_ranges);

    // first read from backend
    {
        char buffer[read_size];
        read_stream_data(&cache_stream, 0, read_size, buffer);
        ASSERT_TRUE(check_data_content(buffer, block_size, 'a'));
        ASSERT_TRUE(check_data_content(buffer + block_size, block_size, 'b'));
        //ASSERT_EQ(stats.write_cache_count, block_count);
    }

    // second read from shared buffer
    {
        char buffer[read_size];
        read_stream_data(&cache_stream, 0, read_size, buffer);
        ASSERT_EQ(sb_stream->shared_io_bytes(), read_size);
    }
}

TEST_F(CacheInputStreamTest, test_peek) {
    const int64_t block_count = 2;

    int64_t data_size = block_size * block_count;
    char data[data_size + 1];
    gen_test_data(data, data_size, block_size);

    const std::string file_name = "test_file6";
    std::shared_ptr<io::SeekableInputStream> stream(new MockSeekableInputStream(data, data_size));
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream(
            new io::SharedBufferedInputStream(stream, file_name, data_size));
    io::CacheInputStream cache_stream(sb_stream, file_name, data_size, 1000000);
    cache_stream.set_enable_populate_cache(true);
    cache_stream.set_enable_block_buffer(true);
    cache_stream.set_enable_async_populate_mode(true);

    const size_t read_size = block_size * block_count;
    std::vector<SharedBufferedInputStream::IORange> io_ranges;
    io_ranges.emplace_back(0, read_size);
    sb_stream->set_io_ranges(io_ranges);

    // first read from backend
    {
        const size_t read_size = block_size;
        char buffer[read_size];
        read_stream_data(&cache_stream, 0, read_size, buffer);
        ASSERT_TRUE(check_data_content(buffer, block_size, 'a'));
    }

    // peek read from shared buffer
    {
        const size_t peek_size = block_size;
        auto res = cache_stream.peek(peek_size);
        ASSERT_TRUE(res.ok());
        auto str_view = res.value();
        ASSERT_EQ(str_view.length(), peek_size);
    }
}

TEST_F(CacheInputStreamTest, test_reuse_remote_buffer_with_async_populate) {
    // A file smaller than one block, like a small parquet file whose footer is read first.
    const int64_t data_size = 119 * 1024;
    char data[data_size + 1];
    for (int64_t i = 0; i < data_size; ++i) {
        data[i] = static_cast<char>(i % 251);
    }

    const std::string file_name = "test_reuse_remote_buffer_with_async_populate";
    std::shared_ptr<io::SeekableInputStream> stream(new MockSeekableInputStream(data, data_size));
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream(
            new io::SharedBufferedInputStream(stream, file_name, data_size));
    io::CacheInputStream cache_stream(sb_stream, file_name, data_size, 1000000);
    cache_stream.set_enable_populate_cache(true);
    cache_stream.set_enable_async_populate_mode(true);
    auto& stats = cache_stream.stats();

    // Read the tail like a footer read, it loads the whole block from remote.
    const int64_t tail_size = 48 * 1024;
    char tail[tail_size];
    read_stream_data(&cache_stream, data_size - tail_size, tail_size, tail);
    ASSERT_EQ(0, memcmp(tail, data + data_size - tail_size, tail_size));
    ASSERT_EQ(1, sb_stream->direct_io_count());
    ASSERT_EQ(data_size, sb_stream->direct_io_bytes());
    ASSERT_EQ(0, stats.read_block_buffer_count);

    // Read the head, it is inside the block just loaded, so it must not touch remote or cache again.
    const int64_t head_size = 32 * 1024;
    char head[head_size];
    read_stream_data(&cache_stream, 0, head_size, head);
    ASSERT_EQ(0, memcmp(head, data, head_size));
    ASSERT_EQ(1, sb_stream->direct_io_count());
    ASSERT_EQ(data_size, sb_stream->direct_io_bytes());
    ASSERT_EQ(1, stats.read_block_buffer_count);
    ASSERT_EQ(head_size, stats.read_block_buffer_bytes);
    ASSERT_EQ(0, stats.read_block_cache_count);
}

TEST_F(CacheInputStreamTest, test_reuse_remote_buffer_out_of_range_reads_remote) {
    const int64_t block_count = 2;
    const int64_t data_size = block_size * block_count - 1024;
    char data[data_size + 1];
    gen_test_data(data, data_size, block_size);

    const std::string file_name = "test_reuse_remote_buffer_out_of_range_reads_remote";
    std::shared_ptr<io::SeekableInputStream> stream(new MockSeekableInputStream(data, data_size));
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream(
            new io::SharedBufferedInputStream(stream, file_name, data_size));
    io::CacheInputStream cache_stream(sb_stream, file_name, data_size, 1000000);
    // Nothing is written to the cache, so a read that cannot be served from the buffer must go remote.
    cache_stream.set_enable_populate_cache(false);
    auto& stats = cache_stream.stats();

    char buffer[block_size];

    // The tail read loads only the second block into the buffer.
    read_stream_data(&cache_stream, block_size + 100, 1024, buffer);
    ASSERT_TRUE(check_data_content(buffer, 1024, 'b'));
    ASSERT_EQ(1, sb_stream->direct_io_count());

    // Inside the buffered range: no remote read.
    read_stream_data(&cache_stream, block_size + 2048, 1024, buffer);
    ASSERT_TRUE(check_data_content(buffer, 1024, 'b'));
    ASSERT_EQ(1, sb_stream->direct_io_count());
    ASSERT_EQ(1, stats.read_block_buffer_count);

    // Outside the buffered range: read remote, and the buffer now holds the first block.
    read_stream_data(&cache_stream, 0, 1024, buffer);
    ASSERT_TRUE(check_data_content(buffer, 1024, 'a'));
    ASSERT_EQ(2, sb_stream->direct_io_count());
    ASSERT_EQ(1, stats.read_block_buffer_count);

    read_stream_data(&cache_stream, 2048, 1024, buffer);
    ASSERT_TRUE(check_data_content(buffer, 1024, 'a'));
    ASSERT_EQ(2, sb_stream->direct_io_count());
    ASSERT_EQ(2, stats.read_block_buffer_count);

    // The second block is no longer buffered, so it is read from remote again.
    read_stream_data(&cache_stream, block_size + 100, 1024, buffer);
    ASSERT_TRUE(check_data_content(buffer, 1024, 'b'));
    ASSERT_EQ(3, sb_stream->direct_io_count());
    ASSERT_EQ(2, stats.read_block_buffer_count);
}

TEST_F(CacheInputStreamTest, test_try_peer_cache) {
    const int64_t block_count = 3;

    int64_t data_size = block_size * block_count;
    char data[data_size + 1];
    gen_test_data(data, data_size, block_size);

    const std::string file_name = "test_try_peer_cache";
    std::shared_ptr<io::SeekableInputStream> stream(new MockSeekableInputStream(data, data_size));
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream(
            new io::SharedBufferedInputStream(stream, file_name, data_size));
    io::CacheInputStream cache_stream(sb_stream, file_name, data_size, 1000000);
    cache_stream.set_enable_populate_cache(true);

    cache_stream.set_peer_cache_node("1.1.1.1:1");
    ASSERT_EQ(cache_stream._peer_host, "1.1.1.1");
    ASSERT_EQ(cache_stream._peer_port, 1);
    // Replace with a invalid ip for test
    cache_stream._peer_host = "127.0.0.1";
    auto& stats = cache_stream.stats();

    // first read from backend
    for (int i = 0; i < block_count; ++i) {
        char buffer[block_size];
        read_stream_data(&cache_stream, i * block_size, block_size, buffer);
        ASSERT_TRUE(check_data_content(buffer, block_size, 'a' + i));
    }
    ASSERT_EQ(stats.read_block_cache_count, 0);
    ASSERT_EQ(stats.write_block_cache_count, block_count);

    // first read from local cache
    // We expect all blocks to come from the local cache only through a fresh stream, because the first
    // stream still holds its last remotely read block in its own buffer and would serve that block from there.
    io::CacheInputStream cache_stream2(sb_stream, file_name, data_size, 1000000);
    cache_stream2.set_enable_populate_cache(true);
    cache_stream2.set_peer_cache_node("1.1.1.1:1");
    cache_stream2._peer_host = "127.0.0.1";
    auto& stats2 = cache_stream2.stats();
    for (int i = 0; i < block_count; ++i) {
        char buffer[block_size];
        read_stream_data(&cache_stream2, i * block_size, block_size, buffer);
        ASSERT_TRUE(check_data_content(buffer, block_size, 'a' + i));
    }
    ASSERT_EQ(stats2.read_block_cache_count, block_count);
    ASSERT_EQ(stats2.read_block_buffer_count, 0);
    ASSERT_EQ(stats2.read_peer_cache_count, 0);
}

// Reads blocks of a two block file one by one from a stream whose cache answers every write with
// `write_status`. Between two reads of the same block we read the other block, so the first block is
// no longer in the stream buffer and the stream goes to the cache and the remote file again.
struct WriteResultCase {
    explicit WriteResultCase(const std::string& file_name, Status write_status, int64_t block_size) {
        data.resize(block_size * 2);
        CacheInputStreamTest::gen_test_data(data.data(), data.size(), block_size);
        engine = std::make_shared<FakeWriteResultCacheEngine>();
        engine->write_status = std::move(write_status);
        BlockCacheOptions options;
        options.block_size = block_size;
        CHECK(block_cache.init(options, engine, nullptr).ok());

        std::shared_ptr<io::SeekableInputStream> stream(new MockSeekableInputStream(data.data(), data.size()));
        sb_stream = std::make_shared<io::SharedBufferedInputStream>(stream, file_name, data.size());
        cache_stream = std::make_unique<io::CacheInputStream>(sb_stream, file_name, data.size(), 1000000);
        cache_stream->set_enable_populate_cache(true);
        cache_stream->_cache = &block_cache;
    }

    void read_block(int64_t block_id, int64_t block_size) {
        std::string buffer(block_size, 0);
        CacheInputStreamTest::read_stream_data(cache_stream.get(), block_id * block_size, block_size, buffer.data());
        ASSERT_TRUE(CacheInputStreamTest::check_data_content(buffer.data(), block_size, 'a' + block_id));
    }

    std::string data;
    std::shared_ptr<FakeWriteResultCacheEngine> engine;
    BlockCache block_cache;
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream;
    std::unique_ptr<io::CacheInputStream> cache_stream;
};

TEST_F(CacheInputStreamTest, test_write_result_ok) {
    WriteResultCase c("test_write_result_ok", Status::OK(), block_size);
    c.read_block(0, block_size);
    c.read_block(1, block_size);
    c.read_block(0, block_size);

    const auto& stats = c.cache_stream->stats();
    ASSERT_EQ(2, c.engine->write_calls);
    ASSERT_EQ(2, stats.write_block_cache_count);
    ASSERT_EQ(2 * block_size, stats.write_block_cache_bytes);
    ASSERT_EQ(0, stats.skip_write_cache_count);
    ASSERT_EQ(0, stats.write_cache_retry_count);
    ASSERT_EQ(0, stats.write_cache_fail_count);
}

TEST_F(CacheInputStreamTest, test_write_result_already_exist) {
    WriteResultCase c("test_write_result_already_exist", Status::AlreadyExist("exist"), block_size);
    c.read_block(0, block_size);
    c.read_block(1, block_size);
    // The block is known to be in the cache, so the stream does not write it again.
    c.read_block(0, block_size);

    const auto& stats = c.cache_stream->stats();
    ASSERT_EQ(2, c.engine->write_calls);
    ASSERT_EQ(0, stats.write_block_cache_count);
    ASSERT_EQ(2, stats.write_cache_already_exist_count);
    ASSERT_EQ(2 * block_size, stats.write_cache_already_exist_bytes);
    ASSERT_EQ(2, stats.skip_write_cache_count);
    ASSERT_EQ(2 * block_size, stats.skip_write_cache_bytes);
    ASSERT_EQ(0, stats.write_cache_busy_count);
    ASSERT_EQ(0, stats.write_cache_retry_count);
    ASSERT_EQ(0, stats.write_cache_fail_count);
}

TEST_F(CacheInputStreamTest, test_write_result_busy_retry) {
    WriteResultCase c("test_write_result_busy_retry", Status::ResourceBusy("busy"), block_size);
    c.read_block(0, block_size);
    c.read_block(1, block_size);
    c.read_block(0, block_size);

    {
        const auto& stats = c.cache_stream->stats();
        ASSERT_EQ(3, c.engine->write_calls);
        ASSERT_EQ(0, stats.write_block_cache_count);
        ASSERT_EQ(3, stats.write_cache_busy_count);
        ASSERT_EQ(3 * block_size, stats.write_cache_busy_bytes);
        ASSERT_EQ(3, stats.skip_write_cache_count);
        ASSERT_EQ(3 * block_size, stats.skip_write_cache_bytes);
        ASSERT_EQ(0, stats.write_cache_already_exist_count);
        ASSERT_EQ(1, stats.write_cache_retry_count);
        ASSERT_EQ(block_size, stats.write_cache_retry_bytes);
        ASSERT_EQ(0, stats.write_cache_fail_count);
    }

    // Block 0 is rejected the second time too, so its next write is again a retry.
    c.read_block(1, block_size);
    c.read_block(0, block_size);
    const auto& stats = c.cache_stream->stats();
    ASSERT_EQ(5, c.engine->write_calls);
    ASSERT_EQ(5, stats.write_cache_busy_count);
    ASSERT_EQ(5, stats.skip_write_cache_count);
    ASSERT_EQ(3, stats.write_cache_retry_count);
    ASSERT_EQ(3 * block_size, stats.write_cache_retry_bytes);
}

TEST_F(CacheInputStreamTest, test_write_result_mem_limit) {
    WriteResultCase c("test_write_result_mem_limit", Status::MemoryLimitExceeded("mem"), block_size);
    c.read_block(0, block_size);
    c.read_block(1, block_size);
    c.read_block(0, block_size);

    const auto& stats = c.cache_stream->stats();
    ASSERT_EQ(3, c.engine->write_calls);
    ASSERT_EQ(3, stats.write_cache_mem_limit_count);
    ASSERT_EQ(3 * block_size, stats.write_cache_mem_limit_bytes);
    ASSERT_EQ(1, stats.write_cache_retry_count);
    ASSERT_EQ(0, stats.write_cache_capacity_limit_count);
    ASSERT_EQ(0, stats.skip_write_cache_count);
    ASSERT_EQ(0, stats.write_cache_fail_count);
}

TEST_F(CacheInputStreamTest, test_write_result_capacity_limit) {
    WriteResultCase c("test_write_result_capacity_limit", Status::CapacityLimitExceed("capacity"), block_size);
    c.read_block(0, block_size);
    c.read_block(1, block_size);
    c.read_block(0, block_size);

    const auto& stats = c.cache_stream->stats();
    ASSERT_EQ(3, c.engine->write_calls);
    ASSERT_EQ(3, stats.write_cache_capacity_limit_count);
    ASSERT_EQ(3 * block_size, stats.write_cache_capacity_limit_bytes);
    ASSERT_EQ(1, stats.write_cache_retry_count);
    ASSERT_EQ(0, stats.write_cache_mem_limit_count);
    ASSERT_EQ(0, stats.skip_write_cache_count);
    ASSERT_EQ(0, stats.write_cache_fail_count);
}

TEST_F(CacheInputStreamTest, test_write_result_other_error) {
    WriteResultCase c("test_write_result_other_error", Status::InternalError("broken"), block_size);
    c.read_block(0, block_size);
    c.read_block(1, block_size);
    c.read_block(0, block_size);

    const auto& stats = c.cache_stream->stats();
    ASSERT_EQ(3, stats.write_cache_fail_count);
    ASSERT_EQ(3 * block_size, stats.write_cache_fail_bytes);
    ASSERT_EQ(1, stats.write_cache_retry_count);
    ASSERT_EQ(0, stats.skip_write_cache_count);
}

TEST_F(CacheInputStreamTest, test_async_write_callback_results) {
    WriteResultCase c("test_async_write_callback_results", Status::OK(), block_size);
    c.cache_stream->set_enable_async_populate_mode(true);

    c.engine->async_code = 0;
    c.read_block(0, block_size);
    c.engine->async_code = EEXIST;
    c.read_block(1, block_size);

    {
        const auto& stats = c.cache_stream->stats();
        ASSERT_EQ(2, stats.write_block_cache_count);
        ASSERT_EQ(1, stats.async_write_done_count);
        ASSERT_EQ(1, stats.async_write_exist_count);
        ASSERT_EQ(0, stats.async_write_fail_count);
    }

    // The cache accepts the job, so the stream does not try this block again, but the job fails later.
    WriteResultCase c2("test_async_write_callback_results_2", Status::OK(), block_size);
    c2.cache_stream->set_enable_async_populate_mode(true);
    c2.engine->async_code = EBUSY;
    c2.read_block(0, block_size);
    c2.read_block(1, block_size);
    c2.read_block(0, block_size);
    const auto& stats = c2.cache_stream->stats();
    ASSERT_EQ(2, c2.engine->write_calls);
    ASSERT_EQ(2, stats.write_block_cache_count);
    ASSERT_EQ(0, stats.async_write_done_count);
    ASSERT_EQ(2, stats.async_write_fail_count);
    ASSERT_EQ(0, stats.write_cache_retry_count);
}

TEST_F(CacheInputStreamTest, test_async_write_callback_after_stream_destroyed) {
    WriteResultCase c("test_async_write_callback_after_stream_destroyed", Status::OK(), block_size);
    c.cache_stream->set_enable_async_populate_mode(true);
    c.engine->defer_callbacks = true;
    c.read_block(0, block_size);
    ASSERT_EQ(1u, c.engine->pending_callbacks.size());
    ASSERT_EQ(0, c.cache_stream->stats().async_write_done_count);

    // A result that arrives before the stream reports its stats is counted.
    c.engine->pending_callbacks[0](0, "");
    ASSERT_EQ(1, c.cache_stream->stats().async_write_done_count);

    // A result that arrives after the stream is gone is dropped without touching the stream.
    c.read_block(1, block_size);
    ASSERT_EQ(2u, c.engine->pending_callbacks.size());
    c.cache_stream.reset();
    c.engine->pending_callbacks[1](EBUSY, "busy");
}

using Reads = std::vector<std::pair<int64_t, int64_t>>;

// A file behind a remote stream that records its reads, read through a stream with shared buffers over a cache
// that the test fills block by block. The file bytes are not periodic in the block size, so a block read from
// a wrong place never matches.
struct SkipCachedBlocksCase {
    SkipCachedBlocksCase(const std::string& file_name, int64_t file_size, bool enable_block_buffer = false,
                         bool enable_async_populate = false) {
        data.resize(file_size);
        for (int64_t i = 0; i < file_size; ++i) {
            data[i] = static_cast<char>(i % 251);
        }
        engine = std::make_shared<InMemoryCacheEngine>();
        BlockCacheOptions options;
        options.block_size = CacheInputStreamTest::block_size;
        CHECK(block_cache.init(options, engine, nullptr).ok());

        remote = std::make_shared<CountingSeekableInputStream>(data.data(), file_size);
        sb_stream = std::make_shared<io::SharedBufferedInputStream>(remote, file_name, file_size);
        // The scanner aligns shared buffers to the block size of the cache.
        sb_stream->set_align_size(CacheInputStreamTest::block_size);
        cache_stream = std::make_unique<io::CacheInputStream>(sb_stream, file_name, file_size, 1000000);
        cache_stream->set_enable_populate_cache(true);
        cache_stream->set_enable_block_buffer(enable_block_buffer);
        cache_stream->set_enable_async_populate_mode(enable_async_populate);
        cache_stream->_cache = &block_cache;
        saved_coalesce_distance = config::io_coalesce_read_max_distance_size;
        // The tests of the split by the cache state expect a large io range to be one shared buffer. The tests of
        // the pieces of a large range turn the pieces on with enable_pieces().
        saved_split_large_range = config::io_coalesce_split_large_range;
        config::io_coalesce_split_large_range = false;
    }

    ~SkipCachedBlocksCase() {
        config::io_coalesce_read_max_distance_size = saved_coalesce_distance;
        config::io_coalesce_split_large_range = saved_split_large_range;
    }

    // An io range above `size` is cut into pieces of at most `size`. We use a multiple of the block size, so a piece
    // ends at a block border.
    void enable_pieces(int64_t size) {
        config::io_coalesce_split_large_range = true;
        auto options = sb_stream->coalesce_options();
        options.max_buffer_size = size;
        sb_stream->set_coalesce_options(options);
    }

    // The file range [offset, offset + size) is one io range, as the parquet reader registers it.
    void set_io_range(int64_t offset, int64_t size) {
        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        ranges.emplace_back(offset, size);
        CHECK(sb_stream->set_io_ranges(ranges).ok());
    }

    // The largest cached gap that the stream downloads to join two missing runs into one request. The case
    // restores the config when it is destroyed.
    void set_coalesce_distance(int64_t distance) {
        config::io_coalesce_read_max_distance_size = static_cast<int32_t>(distance);
    }

    // Puts the blocks into the cache, as an earlier scan of the file would do.
    void cache_blocks(const std::vector<int64_t>& block_ids) {
        const int64_t block_size = CacheInputStreamTest::block_size;
        for (int64_t id : block_ids) {
            const int64_t offset = id * block_size;
            const int64_t size = std::min<int64_t>(block_size, data.size() - offset);
            CHECK(block_cache.write(cache_stream->_cache_key, offset, size, data.data() + offset).ok());
        }
        engine->write_calls = 0;
    }

    std::string block_key(int64_t block_id) const { return fmt::format("{}/{}", cache_stream->_cache_key, block_id); }

    void read_and_check(int64_t offset, int64_t size) {
        std::string buffer(size, 0);
        CacheInputStreamTest::read_stream_data(cache_stream.get(), offset, size, buffer.data());
        ASSERT_EQ(0, memcmp(buffer.data(), data.data() + offset, size)) << "offset=" << offset << ", size=" << size;
    }

    std::string data;
    int32_t saved_coalesce_distance = 0;
    bool saved_split_large_range = true;
    std::shared_ptr<InMemoryCacheEngine> engine;
    BlockCache block_cache;
    std::shared_ptr<CountingSeekableInputStream> remote;
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream;
    std::unique_ptr<io::CacheInputStream> cache_stream;
};

TEST_F(CacheInputStreamTest, test_shared_buffer_cold_read_loads_whole_range) {
    const int64_t block_count = 8;
    SkipCachedBlocksCase c("test_shared_buffer_cold_read_loads_whole_range", block_size * block_count);
    c.set_io_range(0, block_size * block_count);
    const auto& stats = c.cache_stream->stats();

    // Nothing is cached, so one block read loads the whole range with one request.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(3 * block_size, block_size));
    ASSERT_EQ((Reads{{0, block_size * block_count}}), c.remote->reads);
    ASSERT_EQ(0, stats.skip_remote_read_bytes);
    // The cache is asked once about each block that the read does not need.
    ASSERT_EQ(block_count - 1, c.engine->exist_calls);

    // The loaded buffer serves the whole file. It is not loaded again and the cache is not asked again.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, block_size * block_count));
    ASSERT_EQ(1u, c.remote->reads.size());
    ASSERT_EQ(block_count - 1, c.engine->exist_calls);
    ASSERT_EQ(static_cast<size_t>(block_count), c.engine->blocks.size());
    ASSERT_EQ(0, stats.write_cache_already_exist_count);
}

TEST_F(CacheInputStreamTest, test_shared_buffer_skips_cached_blocks_around_one_missing_block) {
    const int64_t block_count = 8;
    SkipCachedBlocksCase c("test_shared_buffer_skips_cached_blocks_around_one_missing_block", block_size * block_count);
    c.set_io_range(0, block_size * block_count);
    c.cache_blocks({0, 1, 2, 4, 5, 6, 7});
    const auto& stats = c.cache_stream->stats();

    // Only block 3 is downloaded, not the whole range.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(3 * block_size, block_size));
    ASSERT_EQ((Reads{{3 * block_size, block_size}}), c.remote->reads);
    ASSERT_EQ(7 * block_size, stats.skip_remote_read_bytes);
    ASSERT_EQ(block_count - 1, c.engine->exist_calls);
    // The cached blocks are not written again, so the cache has nothing to answer AlreadyExist to.
    ASSERT_EQ(1, c.engine->write_calls);
    ASSERT_EQ(0, stats.write_cache_already_exist_count);

    // The rest of the file comes from the cache and the loaded block, with no new remote read.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, block_size * block_count));
    ASSERT_EQ(1u, c.remote->reads.size());
    ASSERT_EQ(7, stats.read_block_cache_count);
    ASSERT_EQ(block_count - 1, c.engine->exist_calls);
    ASSERT_EQ(1, c.engine->write_calls);
}

TEST_F(CacheInputStreamTest, test_shared_buffer_skips_cached_blocks_at_both_ends) {
    const int64_t block_count = 8;
    SkipCachedBlocksCase c("test_shared_buffer_skips_cached_blocks_at_both_ends", block_size * block_count);
    c.set_io_range(0, block_size * block_count);
    c.cache_blocks({0, 1, 6, 7});
    const auto& stats = c.cache_stream->stats();

    // Blocks 2..5 are not cached. They are loaded with one request, and the cached ends are not.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(3 * block_size, block_size));
    ASSERT_EQ((Reads{{2 * block_size, 4 * block_size}}), c.remote->reads);
    ASSERT_EQ(4 * block_size, stats.skip_remote_read_bytes);

    // The other missing blocks are in the loaded span already.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, block_size * block_count));
    ASSERT_EQ(1u, c.remote->reads.size());
    ASSERT_EQ(4, stats.read_block_cache_count);
    ASSERT_EQ(block_count - 1, c.engine->exist_calls);
    ASSERT_EQ(0, stats.write_cache_already_exist_count);
}

TEST_F(CacheInputStreamTest, test_shared_buffer_loads_each_missing_run_with_one_request) {
    const int64_t block_count = 8;
    SkipCachedBlocksCase c("test_shared_buffer_loads_each_missing_run_with_one_request", block_size * block_count);
    // The cached block 4 is a gap above the distance 0, so the two missing runs 1..3 and 5..7 are not joined.
    c.set_coalesce_distance(0);
    c.set_io_range(0, block_size * block_count);
    c.cache_blocks({0, 4});
    const auto& stats = c.cache_stream->stats();

    // The read needs block 2, so its run 1..3 is loaded with one request. The cached blocks 0 and 4 are not.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(2 * block_size, block_size));
    ASSERT_EQ((Reads{{1 * block_size, 3 * block_size}}), c.remote->reads);
    ASSERT_EQ(2 * block_size, stats.skip_remote_read_bytes);

    // The read of one block of the second run loads the whole run with one request, and not only that block.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(6 * block_size, block_size));
    ASSERT_EQ((Reads{{1 * block_size, 3 * block_size}, {5 * block_size, 3 * block_size}}), c.remote->reads);
    ASSERT_EQ(2, c.sb_stream->shared_io_count());
    ASSERT_EQ(0, c.sb_stream->direct_io_count());

    // Reading everything downloads each block that was not cached at the start exactly once. The cache is not
    // asked again when the second run is loaded.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, block_size * block_count));
    ASSERT_EQ(2u, c.remote->reads.size());
    ASSERT_EQ(6 * block_size, c.remote->bytes);
    ASSERT_EQ(2 * block_size, stats.skip_remote_read_bytes);
    ASSERT_EQ(block_count - 1, c.engine->exist_calls);
    ASSERT_EQ(static_cast<size_t>(block_count), c.engine->blocks.size());
}

TEST_F(CacheInputStreamTest, test_shared_buffer_loads_runs_around_a_large_cached_gap) {
    // 16 blocks are 4 MB. The cached gap of 5 blocks is 1.25 MB, which is above the default distance of 1 MB.
    const int64_t block_count = 16;
    SkipCachedBlocksCase c("test_shared_buffer_loads_runs_around_a_large_cached_gap", block_size * block_count);
    c.set_io_range(0, block_size * block_count);
    c.cache_blocks({5, 6, 7, 8, 9});
    const auto& stats = c.cache_stream->stats();

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(2 * block_size, block_size));
    ASSERT_EQ((Reads{{0, 5 * block_size}}), c.remote->reads);
    ASSERT_EQ(5 * block_size, stats.skip_remote_read_bytes);

    // The reads of the second run find its buffer, which the first of them loads with one request.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(12 * block_size, block_size));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(10 * block_size, block_size));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(15 * block_size, block_size));
    ASSERT_EQ((Reads{{0, 5 * block_size}, {10 * block_size, 6 * block_size}}), c.remote->reads);
    ASSERT_EQ(2, c.sb_stream->shared_io_count());
    ASSERT_EQ(0, c.sb_stream->direct_io_count());
    // No cached block is downloaded.
    ASSERT_EQ(11 * block_size, c.remote->bytes);
    ASSERT_EQ(block_count - 1, c.engine->exist_calls);
}

TEST_F(CacheInputStreamTest, test_shared_buffer_joins_runs_with_a_small_cached_gap) {
    const int64_t block_count = 10;
    SkipCachedBlocksCase c("test_shared_buffer_joins_runs_with_a_small_cached_gap", block_size * block_count);
    // The cached gap of blocks 4 and 5 is equal to the distance, so the runs 1..3 and 6..8 are joined.
    c.set_coalesce_distance(2 * block_size);
    c.set_io_range(0, block_size * block_count);
    c.cache_blocks({0, 4, 5, 9});
    const auto& stats = c.cache_stream->stats();

    // One request loads blocks 1..8 including the gap. Only the cached blocks 0 and 9 are skipped.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(2 * block_size, block_size));
    ASSERT_EQ((Reads{{1 * block_size, 8 * block_size}}), c.remote->reads);
    ASSERT_EQ(2 * block_size, stats.skip_remote_read_bytes);

    // The second run and the gap come from the loaded buffer. The cache has the gap blocks, so their populate
    // is answered with AlreadyExist.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, block_size * block_count));
    ASSERT_EQ(1u, c.remote->reads.size());
    ASSERT_EQ(2, stats.read_block_cache_count);
    ASSERT_EQ(2, stats.write_cache_already_exist_count);
    ASSERT_EQ(2 * block_size, stats.skip_remote_read_bytes);
    ASSERT_EQ(block_count - 1, c.engine->exist_calls);
}

TEST_F(CacheInputStreamTest, test_shared_buffer_does_not_join_runs_with_a_cached_gap_above_the_distance) {
    const int64_t block_count = 10;
    SkipCachedBlocksCase c("test_shared_buffer_does_not_join_runs_with_a_cached_gap_above_the_distance",
                           block_size * block_count);
    // The cached gap of blocks 4, 5 and 6 is above the distance by one block.
    c.set_coalesce_distance(2 * block_size);
    c.set_io_range(0, block_size * block_count);
    c.cache_blocks({0, 4, 5, 6, 9});
    const auto& stats = c.cache_stream->stats();

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(2 * block_size, block_size));
    ASSERT_EQ((Reads{{1 * block_size, 3 * block_size}}), c.remote->reads);
    ASSERT_EQ(5 * block_size, stats.skip_remote_read_bytes);

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(8 * block_size, block_size));
    ASSERT_EQ((Reads{{1 * block_size, 3 * block_size}, {7 * block_size, 2 * block_size}}), c.remote->reads);
    ASSERT_EQ(0, c.sb_stream->direct_io_count());
}

TEST_F(CacheInputStreamTest, test_shared_buffer_reads_coalesce_distance_at_each_split) {
    // Two shared buffers of 8 blocks, with one cached block in each. The config changes between their splits.
    const int64_t block_count = 16;
    SkipCachedBlocksCase c("test_shared_buffer_reads_coalesce_distance_at_each_split", block_size * block_count);
    io::SharedBufferedInputStream::CoalesceOptions coalesce;
    coalesce.max_buffer_size = 8 * block_size;
    c.sb_stream->set_coalesce_options(coalesce);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 8 * block_size);
    ranges.emplace_back(8 * block_size, 8 * block_size);
    ASSERT_OK(c.sb_stream->set_io_ranges(ranges));
    c.cache_blocks({3, 11});

    // The distance is below the cached gap of the first buffer, so block 3 is skipped.
    c.set_coalesce_distance(block_size - 1);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(1 * block_size, block_size));
    ASSERT_EQ((Reads{{0, 3 * block_size}}), c.remote->reads);

    // The distance is raised to the cached gap of the second buffer, so block 11 is downloaded with its runs.
    c.set_coalesce_distance(block_size);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(9 * block_size, block_size));
    ASSERT_EQ((Reads{{0, 3 * block_size}, {8 * block_size, 8 * block_size}}), c.remote->reads);
}

TEST_F(CacheInputStreamTest, test_shared_buffer_request_count_equals_number_of_missing_runs) {
    // Blocks 3k and 3k + 1 are missing and block 3k + 2 is cached. The cached blocks are above the distance 0.
    const int64_t block_count = 24;
    const int64_t run_count = 8;
    SkipCachedBlocksCase c("test_shared_buffer_request_count_equals_number_of_missing_runs", block_size * block_count);
    c.set_coalesce_distance(0);
    c.set_io_range(0, block_size * block_count);
    std::vector<int64_t> cached;
    for (int64_t k = 0; k < run_count; ++k) {
        cached.push_back(3 * k + 2);
    }
    c.cache_blocks(cached);
    const auto& stats = c.cache_stream->stats();

    // The reads go block by block. The first block of a run loads the run, and the second block finds it loaded.
    for (int64_t block_id = 0; block_id < block_count; ++block_id) {
        if (block_id % 3 != 2) {
            ASSERT_NO_FATAL_FAILURE(c.read_and_check(block_id * block_size, block_size));
        }
    }
    Reads expected;
    for (int64_t k = 0; k < run_count; ++k) {
        expected.emplace_back(3 * k * block_size, 2 * block_size);
    }
    ASSERT_EQ(expected, c.remote->reads);
    ASSERT_EQ(run_count, c.sb_stream->shared_io_count());
    ASSERT_EQ(0, c.sb_stream->direct_io_count());
    ASSERT_EQ(16 * block_size, c.remote->bytes);
    ASSERT_EQ(8 * block_size, stats.skip_remote_read_bytes);
    // The cache is asked once about each block that the first read does not need.
    ASSERT_EQ(block_count - 1, c.engine->exist_calls);
}

TEST_F(CacheInputStreamTest, test_shared_buffer_merged_buffers_stay_within_max_buffer_size) {
    // 96 blocks are 24 MB, one shared buffer. Every 8th block is cached, so a missing run has 7 blocks (1.75 MB)
    // and a cached gap has 1 block (256 KB), which is below the default distance of 1 MB. We do not want one
    // request for 24 MB, so the runs are joined only while the buffer stays within 8 MB (32 blocks).
    const int64_t block_count = 96;
    SkipCachedBlocksCase c("test_shared_buffer_merged_buffers_stay_within_max_buffer_size", block_size * block_count);
    c.set_io_range(0, block_size * block_count);
    std::vector<int64_t> cached;
    for (int64_t block_id = 7; block_id < block_count; block_id += 8) {
        cached.push_back(block_id);
    }
    c.cache_blocks(cached);
    const auto& stats = c.cache_stream->stats();

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, block_size * block_count));
    // Four runs and the three gaps between them are 31 blocks. A fifth run would make the buffer larger than
    // 32 blocks. The cached blocks 31, 63 and 95 are between the buffers.
    ASSERT_EQ((Reads{{0, 31 * block_size}, {32 * block_size, 31 * block_size}, {64 * block_size, 31 * block_size}}),
              c.remote->reads);
    ASSERT_EQ(3, c.sb_stream->shared_io_count());
    ASSERT_EQ(0, c.sb_stream->direct_io_count());
    ASSERT_EQ(3 * block_size, stats.skip_remote_read_bytes);
    // The first remote read needs blocks 0..6, so the cache is asked about the other 89 blocks, once.
    ASSERT_EQ(block_count - 7, c.engine->exist_calls);
    for (const auto& read : c.remote->reads) {
        ASSERT_LE(read.second, 8 * 1024 * 1024);
    }
    // Each block is in one request, except the cached blocks 31, 63 and 95 that no request covers. The cached
    // blocks inside a buffer are downloaded with it.
    for (int64_t block_id = 0; block_id < block_count; ++block_id) {
        const int64_t begin = block_id * block_size;
        int covering_reads = 0;
        for (const auto& read : c.remote->reads) {
            if (read.first <= begin && begin + block_size <= read.first + read.second) {
                covering_reads += 1;
            }
        }
        const bool cut_out = block_id == 31 || block_id == 63 || block_id == 95;
        ASSERT_EQ(cut_out ? 0 : 1, covering_reads) << "block " << block_id;
    }
}

TEST_F(CacheInputStreamTest, test_shared_buffer_runs_longer_than_max_buffer_size_stay_whole_and_apart) {
    // 80 blocks are 20 MB. The cached block 40 splits them into runs of 40 and 39 blocks, both longer than
    // 8 MB (32 blocks). The gap of one block is below the distance, but joining the runs would make a buffer
    // above 8 MB, so each run is one request.
    const int64_t block_count = 80;
    SkipCachedBlocksCase c("test_shared_buffer_runs_longer_than_max_buffer_size_stay_whole_and_apart",
                           block_size * block_count);
    c.set_io_range(0, block_size * block_count);
    c.cache_blocks({40});
    const auto& stats = c.cache_stream->stats();

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, block_size * block_count));
    ASSERT_EQ((Reads{{0, 40 * block_size}, {41 * block_size, 39 * block_size}}), c.remote->reads);
    ASSERT_EQ(2, c.sb_stream->shared_io_count());
    ASSERT_EQ(0, c.sb_stream->direct_io_count());
    ASSERT_EQ(block_size, stats.skip_remote_read_bytes);
}

TEST_F(CacheInputStreamTest, test_shared_buffer_read_spanning_two_split_buffers) {
    const int64_t block_count = 8;
    SkipCachedBlocksCase c("test_shared_buffer_read_spanning_two_split_buffers", block_size * block_count);
    c.set_coalesce_distance(0);
    c.set_io_range(0, block_size * block_count);
    // The missing runs are blocks 0..2 and 5..7, and the cached blocks 3 and 4 are between them.
    c.cache_blocks({3, 4});
    const auto& stats = c.cache_stream->stats();

    // The read starts in the first buffer and ends in the second one. The cached blocks come from the cache, and
    // each buffer is loaded with one request.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(2 * block_size + 100, 3 * block_size + 50));
    ASSERT_EQ((Reads{{0, 3 * block_size}, {5 * block_size, 3 * block_size}}), c.remote->reads);
    ASSERT_EQ(0, c.sb_stream->direct_io_count());
    ASSERT_EQ(2, stats.read_block_cache_count);
    ASSERT_EQ(2 * block_size, stats.skip_remote_read_bytes);
    ASSERT_EQ(block_count - 1, c.engine->exist_calls);

    // Both buffers serve the rest of the file.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, block_size * block_count));
    ASSERT_EQ(2u, c.remote->reads.size());
}

TEST_F(CacheInputStreamTest, test_shared_buffer_cut_out_blocks_are_read_from_cache_or_remote_directly) {
    const int64_t block_count = 8;
    SkipCachedBlocksCase c("test_shared_buffer_cut_out_blocks_are_read_from_cache_or_remote_directly",
                           block_size * block_count);
    c.set_coalesce_distance(0);
    c.set_io_range(0, block_size * block_count);
    // The cache has blocks 3 and 4, and loses block 4 after it said that the block exists.
    c.cache_blocks({3, 4});
    c.engine->evict_on_exist.insert(c.block_key(4));
    const auto& stats = c.cache_stream->stats();

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(1 * block_size, block_size));
    ASSERT_EQ((Reads{{0, 3 * block_size}}), c.remote->reads);

    // The buffers cover blocks 0..2 and 5..7. No buffer covers the cut out blocks 3 and 4 or a read that
    // reaches into them.
    ASSERT_TRUE(c.sb_stream->find_shared_buffer(0, 3 * block_size).ok());
    ASSERT_TRUE(c.sb_stream->find_shared_buffer(5 * block_size, 3 * block_size).ok());
    ASSERT_FALSE(c.sb_stream->find_shared_buffer(3 * block_size, block_size).ok());
    ASSERT_FALSE(c.sb_stream->find_shared_buffer(4 * block_size + 100, 100).ok());
    ASSERT_FALSE(c.sb_stream->find_shared_buffer(3 * block_size - 10, 20).ok());
    ASSERT_FALSE(c.sb_stream->find_shared_buffer(5 * block_size - 10, 20).ok());

    // Block 3 is in the cache, so the read needs no remote read.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(3 * block_size, block_size));
    ASSERT_EQ(1u, c.remote->reads.size());
    ASSERT_EQ(1, stats.read_block_cache_count);
    ASSERT_EQ(0, c.sb_stream->direct_io_count());

    // Block 4 is gone from the cache, and no buffer covers it, so it is read directly with right bytes.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(4 * block_size, block_size));
    ASSERT_EQ((Reads{{0, 3 * block_size}, {4 * block_size, block_size}}), c.remote->reads);
    ASSERT_EQ(1, c.sb_stream->direct_io_count());

    // The second run is still loaded with one request.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(6 * block_size, block_size));
    ASSERT_EQ((Reads{{0, 3 * block_size}, {4 * block_size, block_size}, {5 * block_size, 3 * block_size}}),
              c.remote->reads);
    ASSERT_EQ(1, c.sb_stream->direct_io_count());
    ASSERT_EQ(block_count - 1, c.engine->exist_calls);
}

TEST_F(CacheInputStreamTest, test_shared_buffer_peek_and_release_after_split) {
    const int64_t block_count = 8;
    SkipCachedBlocksCase c("test_shared_buffer_peek_and_release_after_split", block_size * block_count);
    c.set_coalesce_distance(0);
    c.set_io_range(0, block_size * block_count);
    c.cache_blocks({3, 4});

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(1 * block_size, block_size));
    ASSERT_EQ((Reads{{0, 3 * block_size}}), c.remote->reads);

    // The loaded buffer is served without a copy, as long as the peek stays inside it.
    ASSERT_OK(c.cache_stream->seek(1 * block_size));
    auto peeked = c.cache_stream->peek(block_size);
    ASSERT_TRUE(peeked.ok());
    ASSERT_EQ(0, memcmp(peeked.value().data(), c.data.data() + 1 * block_size, block_size));
    ASSERT_OK(c.cache_stream->seek(3 * block_size - 100));
    ASSERT_FALSE(c.cache_stream->peek(200).ok());

    // The second buffer is not loaded yet, so a peek of it is refused and loads nothing.
    ASSERT_OK(c.cache_stream->seek(5 * block_size));
    ASSERT_FALSE(c.cache_stream->peek(block_size).ok());
    ASSERT_EQ(1u, c.remote->reads.size());

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(6 * block_size, block_size));
    ASSERT_EQ((Reads{{0, 3 * block_size}, {5 * block_size, 3 * block_size}}), c.remote->reads);
    ASSERT_OK(c.cache_stream->seek(6 * block_size));
    auto peeked_second = c.cache_stream->peek(block_size);
    ASSERT_TRUE(peeked_second.ok());
    ASSERT_EQ(0, memcmp(peeked_second.value().data(), c.data.data() + 6 * block_size, block_size));

    // Releasing to offset 4 blocks drops the first buffer, which ends before it, and keeps the second one.
    c.sb_stream->release_to_offset(4 * block_size);
    ASSERT_FALSE(c.sb_stream->find_shared_buffer(1 * block_size, block_size).ok());
    ASSERT_TRUE(c.sb_stream->find_shared_buffer(6 * block_size, block_size).ok());

    // The blocks of the dropped buffer that the cache does not hold are read again, with right bytes.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, 3 * block_size));
    ASSERT_EQ((Reads{{0, 3 * block_size},
                     {5 * block_size, 3 * block_size},
                     {0, block_size},
                     {2 * block_size, block_size}}),
              c.remote->reads);

    c.sb_stream->release_to_offset(block_count * block_size);
    ASSERT_FALSE(c.sb_stream->find_shared_buffer(6 * block_size, block_size).ok());
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(5 * block_size, 3 * block_size));
}

TEST_F(CacheInputStreamTest, test_shared_buffer_block_evicted_after_exist_is_read_remote) {
    const int64_t block_count = 8;
    SkipCachedBlocksCase c("test_shared_buffer_block_evicted_after_exist_is_read_remote", block_size * block_count);
    c.set_io_range(0, block_size * block_count);
    c.cache_blocks({0, 1, 2, 4, 5, 6, 7});
    // The cache says that block 5 exists, and loses it before the stream reads it.
    c.engine->evict_on_exist.insert(c.block_key(5));
    const auto& stats = c.cache_stream->stats();

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(3 * block_size, block_size));
    ASSERT_EQ((Reads{{3 * block_size, block_size}}), c.remote->reads);
    ASSERT_EQ(0u, c.engine->blocks.count(c.block_key(5)));

    // The read of block 5 does not find it in the cache, and gets it from remote storage. All bytes are right.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, block_size * block_count));
    ASSERT_EQ((Reads{{3 * block_size, block_size}, {5 * block_size, block_size}}), c.remote->reads);
    ASSERT_EQ(6, stats.read_block_cache_count);
    // The buffer was not asked again, and block 5 is cached again.
    ASSERT_EQ(block_count - 1, c.engine->exist_calls);
    ASSERT_EQ(1u, c.engine->blocks.count(c.block_key(5)));
}

TEST_F(CacheInputStreamTest, test_shared_buffer_refused_cache_read_downloads_only_that_block) {
    const int64_t block_count = 8;
    SkipCachedBlocksCase c("test_shared_buffer_refused_cache_read_downloads_only_that_block", block_size * block_count);
    c.set_io_range(0, block_size * block_count);
    c.cache_blocks({0, 1, 2, 3, 4, 5, 6, 7});
    // The cache holds block 3, but refuses to read it.
    c.engine->busy_keys.insert(c.block_key(3));
    const auto& stats = c.cache_stream->stats();

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(3 * block_size, block_size));
    ASSERT_EQ((Reads{{3 * block_size, block_size}}), c.remote->reads);
    ASSERT_EQ(7 * block_size, stats.skip_remote_read_bytes);
    ASSERT_EQ(1, stats.skip_read_cache_count);
    // The cache holds the block, so we do not write it.
    ASSERT_EQ(0, c.engine->write_calls);
}

TEST_F(CacheInputStreamTest, test_shared_buffer_without_other_blocks_does_not_ask_cache) {
    const int64_t block_count = 4;
    SkipCachedBlocksCase c("test_shared_buffer_without_other_blocks_does_not_ask_cache", block_size * block_count);
    c.set_io_range(0, block_size * block_count);
    c.cache_blocks({0, 1, 2, 3});
    for (int64_t i = 0; i < block_count; ++i) {
        c.engine->busy_keys.insert(c.block_key(i));
    }
    const auto& stats = c.cache_stream->stats();

    // The read needs every block of the buffer, so no block is left to ask about.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, block_size * block_count));
    ASSERT_EQ((Reads{{0, block_size * block_count}}), c.remote->reads);
    ASSERT_EQ(0, c.engine->exist_calls);
    ASSERT_EQ(0, stats.skip_remote_read_bytes);
}

TEST_F(CacheInputStreamTest, test_shared_buffer_skips_cached_blocks_with_several_chunks) {
    // 40 blocks make one shared buffer of 10 MB, and a remote read goes in chunks of 16 blocks.
    const int64_t block_count = 40;
    SkipCachedBlocksCase c("test_shared_buffer_skips_cached_blocks_with_several_chunks", block_size * block_count);
    c.set_io_range(0, block_size * block_count);
    c.cache_blocks({0, 39});
    const auto& stats = c.cache_stream->stats();

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, block_size * block_count));
    ASSERT_EQ((Reads{{1 * block_size, 38 * block_size}}), c.remote->reads);
    ASSERT_EQ(2 * block_size, stats.skip_remote_read_bytes);
    ASSERT_EQ(2, stats.read_block_cache_count);
    // Block 0 and block 39 are the only blocks of the buffer that the read does not need.
    ASSERT_EQ(2, c.engine->exist_calls);
}

// Two shared buffers, [0, 3 blocks) and [3 blocks, end of file), and a last block that is shorter than a block.
// One read covers both buffers, and a later read overlaps the first one.
static void check_two_buffers_and_tail_block(const std::string& file_name, bool block_buffer, bool async_populate) {
    const int64_t bs = CacheInputStreamTest::block_size;
    const int64_t tail = 100 * 1024;
    const int64_t file_size = 6 * bs + tail;
    SkipCachedBlocksCase c(file_name, file_size, block_buffer, async_populate);

    io::SharedBufferedInputStream::CoalesceOptions coalesce;
    coalesce.max_dist_size = 0;
    coalesce.max_buffer_size = 3 * bs;
    c.sb_stream->set_coalesce_options(coalesce);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 3 * bs);
    ranges.emplace_back(3 * bs, file_size - 3 * bs);
    ASSERT_OK(c.sb_stream->set_io_ranges(ranges));

    c.set_coalesce_distance(0);
    c.cache_blocks({1, 4});
    const auto& stats = c.cache_stream->stats();

    // The missing blocks are 0, 2, 3, 5 and 6. The first buffer is split into block 0 and block 2, because the
    // cached block 1 is a gap above the distance 0. Blocks 2 and 3 are read with one request that no buffer
    // covers, because the request ends behind the buffer of block 2. The second buffer is cut to blocks 5 and 6:
    // block 4 was cached before the read, and block 3 is cached by the read of blocks 2 and 3.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, file_size));
    ASSERT_EQ((Reads{{0, bs}, {2 * bs, 2 * bs}, {5 * bs, bs + tail}}), c.remote->reads);
    ASSERT_EQ(4 * bs + tail, c.remote->bytes);
    ASSERT_EQ(3 * bs, stats.skip_remote_read_bytes);
    ASSERT_EQ(4, c.engine->exist_calls);

    // Reads that overlap the buffers and the short last block need no more remote reads.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(bs / 2, 5 * bs + 17));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(file_size - 1000, 1000));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(2 * bs - 5, 10));
    ASSERT_EQ(3u, c.remote->reads.size());
    ASSERT_EQ(4, c.engine->exist_calls);
    ASSERT_EQ(7u, c.engine->blocks.size());
}

TEST_F(CacheInputStreamTest, test_shared_buffer_skips_cached_blocks_with_two_buffers_and_tail_block) {
    check_two_buffers_and_tail_block("test_shared_buffer_two_buffers_and_tail_block", false, false);
}

TEST_F(CacheInputStreamTest, test_shared_buffer_skips_cached_blocks_with_block_buffer) {
    check_two_buffers_and_tail_block("test_shared_buffer_two_buffers_and_tail_block_with_block_buffer", true, false);
}

TEST_F(CacheInputStreamTest, test_shared_buffer_skips_cached_blocks_with_async_populate) {
    check_two_buffers_and_tail_block("test_shared_buffer_two_buffers_and_tail_block_with_async_populate", false, true);
}

TEST_F(CacheInputStreamTest, test_shared_buffer_peek_after_skipping_cached_blocks) {
    const int64_t block_count = 8;
    SkipCachedBlocksCase c("test_shared_buffer_peek_after_skipping_cached_blocks", block_size * block_count);
    c.set_io_range(0, block_size * block_count);
    c.cache_blocks({0, 1, 2, 4, 5, 6, 7});

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(3 * block_size, block_size));
    ASSERT_EQ((Reads{{3 * block_size, block_size}}), c.remote->reads);

    // The loaded block is served without a copy.
    ASSERT_OK(c.cache_stream->seek(3 * block_size));
    auto peeked = c.cache_stream->peek(block_size);
    ASSERT_TRUE(peeked.ok());
    ASSERT_EQ(0, memcmp(peeked.value().data(), c.data.data() + 3 * block_size, block_size));

    // A block outside the cut buffer is not loaded, and the caller reads it with a copy.
    ASSERT_OK(c.cache_stream->seek(2 * block_size));
    ASSERT_FALSE(c.cache_stream->peek(block_size).ok());
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(2 * block_size, block_size));
    ASSERT_EQ(1u, c.remote->reads.size());
}

TEST_F(CacheInputStreamTest, test_stats_merge_skip_remote_read_bytes) {
    CacheInputStream::Stats a;
    a.skip_remote_read_bytes = 5;
    CacheInputStream::Stats b;
    b.skip_remote_read_bytes = 7;
    a += b;
    ASSERT_EQ(12, a.skip_remote_read_bytes);
}

// The tests below use a file of 24 blocks and the coalesce buffer size of 8 blocks, so the io range of the whole
// file is cut into the pieces [0, 8), [8, 16) and [16, 24) blocks.

TEST_F(CacheInputStreamTest, test_large_range_pieces_skip_cached_blocks_inside_a_piece) {
    const int64_t block_count = 24;
    SkipCachedBlocksCase c("test_large_range_pieces_skip_cached_blocks_inside_a_piece", block_size * block_count);
    c.set_coalesce_distance(0);
    c.enable_pieces(8 * block_size);
    c.set_io_range(0, block_size * block_count);
    // The cache holds the blocks 10..12 of the second piece.
    c.cache_blocks({10, 11, 12});
    const auto& stats = c.cache_stream->stats();

    // The second piece is cut by the cache state into the runs 8..9 and 13..15, and the run of the read is loaded.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(9 * block_size, block_size));
    ASSERT_EQ((Reads{{8 * block_size, 2 * block_size}}), c.remote->reads);
    ASSERT_EQ(3 * block_size, stats.skip_remote_read_bytes);
    ASSERT_EQ(7, c.engine->exist_calls);
    ASSERT_EQ(0, c.sb_stream->direct_io_count());
    // The buffers made from a piece keep the large flag, so the counters see the bytes they load.
    ASSERT_EQ(1, c.sb_stream->large_io_count());
    ASSERT_EQ(2 * block_size, c.sb_stream->large_io_bytes());
    ASSERT_EQ(block_size, c.sb_stream->large_used_bytes());

    auto head = c.sb_stream->find_shared_buffer(8 * block_size, 2 * block_size);
    ASSERT_OK(head.status());
    ASSERT_TRUE(head.value()->large);
    ASSERT_TRUE(head.value()->split);
    ASSERT_FALSE(head.value()->has_next_piece);
    auto tail = c.sb_stream->find_shared_buffer(13 * block_size, 3 * block_size);
    ASSERT_OK(tail.status());
    ASSERT_TRUE(tail.value()->large);
    ASSERT_TRUE(tail.value()->split);
    ASSERT_TRUE(tail.value()->has_next_piece);
    ASSERT_FALSE(c.sb_stream->find_shared_buffer(10 * block_size, block_size).ok());
    // The other pieces are neither cut nor loaded, and the cache was not asked about their blocks.
    auto first = c.sb_stream->find_shared_buffer(0, 8 * block_size);
    ASSERT_OK(first.status());
    ASSERT_TRUE(first.value()->large);
    ASSERT_FALSE(first.value()->split);
    ASSERT_TRUE(first.value()->has_next_piece);
    auto last = c.sb_stream->find_shared_buffer(16 * block_size, 8 * block_size);
    ASSERT_OK(last.status());
    ASSERT_FALSE(last.value()->split);
    ASSERT_FALSE(last.value()->has_next_piece);

    // The second run is loaded with one request, and the cache is not asked again. The cached blocks come from the
    // cache.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(14 * block_size, block_size));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(10 * block_size, 3 * block_size));
    ASSERT_EQ((Reads{{8 * block_size, 2 * block_size}, {13 * block_size, 3 * block_size}}), c.remote->reads);
    ASSERT_EQ(3, stats.read_block_cache_count);
    ASSERT_EQ(7, c.engine->exist_calls);
    ASSERT_EQ(0, c.sb_stream->direct_io_count());
    ASSERT_EQ(2, c.sb_stream->large_io_count());
    ASSERT_EQ(5 * block_size, c.sb_stream->large_io_bytes());
}

TEST_F(CacheInputStreamTest, test_large_range_pieces_serve_a_remote_read_that_crosses_two_pieces) {
    const int64_t block_count = 24;
    SkipCachedBlocksCase c("test_large_range_pieces_serve_a_remote_read_that_crosses_two_pieces",
                           block_size * block_count);
    c.set_coalesce_distance(0);
    c.enable_pieces(8 * block_size);
    c.set_io_range(0, block_size * block_count);
    // The cache holds the blocks 0..3 in front of the read, and the blocks 12 and 13 behind it.
    c.cache_blocks({0, 1, 2, 3, 12, 13});
    const auto& stats = c.cache_stream->stats();

    // The blocks 6..9 are missing, and the read crosses the border of the first two pieces at the block 8. Each
    // piece is cut by the cache state, and the pieces serve the read with no direct read: the first piece loads
    // the blocks 4..7, and the second one loads the run 8..11.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(6 * block_size, 4 * block_size));
    ASSERT_EQ((Reads{{4 * block_size, 4 * block_size}, {8 * block_size, 4 * block_size}}), c.remote->reads);
    ASSERT_EQ(0, c.sb_stream->direct_io_count());
    ASSERT_EQ(2, c.sb_stream->shared_io_count());
    ASSERT_EQ(6 * block_size, stats.skip_remote_read_bytes);
    ASSERT_EQ(12, c.engine->exist_calls);
    ASSERT_EQ(2, c.sb_stream->large_io_count());
    ASSERT_EQ(8 * block_size, c.sb_stream->large_io_bytes());
    ASSERT_EQ(4 * block_size, c.sb_stream->large_used_bytes());

    // The run behind the cached blocks 12 and 13 is a buffer of its own, and it continues in the third piece.
    auto tail = c.sb_stream->find_shared_buffer(14 * block_size, 2 * block_size);
    ASSERT_OK(tail.status());
    ASSERT_TRUE(tail.value()->split);
    ASSERT_TRUE(tail.value()->has_next_piece);

    // The read of the rest of the file crosses the border of the second and the third piece too. No cached block is
    // downloaded, and the cache is asked about the blocks of the third piece not at all, because the read needs
    // all of them.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, block_size * block_count));
    ASSERT_EQ((Reads{{4 * block_size, 4 * block_size},
                     {8 * block_size, 4 * block_size},
                     {14 * block_size, 2 * block_size},
                     {16 * block_size, 8 * block_size}}),
              c.remote->reads);
    ASSERT_EQ(18 * block_size, c.remote->bytes);
    ASSERT_EQ(0, c.sb_stream->direct_io_count());
    ASSERT_EQ(12, c.engine->exist_calls);
}

TEST_F(CacheInputStreamTest, test_large_range_pieces_serve_a_cold_remote_read_that_crosses_two_pieces) {
    const int64_t block_count = 24;
    SkipCachedBlocksCase c("test_large_range_pieces_serve_a_cold_remote_read_that_crosses_two_pieces",
                           block_size * block_count);
    c.enable_pieces(8 * block_size);
    c.set_io_range(0, block_size * block_count);
    const auto& stats = c.cache_stream->stats();

    // Nothing is cached, so each piece is loaded whole with one request and the read has no direct read.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(6 * block_size, 4 * block_size));
    ASSERT_EQ((Reads{{0, 8 * block_size}, {8 * block_size, 8 * block_size}}), c.remote->reads);
    ASSERT_EQ(0, c.sb_stream->direct_io_count());
    ASSERT_EQ(0, stats.skip_remote_read_bytes);
    ASSERT_EQ(12, c.engine->exist_calls);
    ASSERT_EQ(2, c.sb_stream->large_io_count());
    ASSERT_EQ(16 * block_size, c.sb_stream->large_io_bytes());
    ASSERT_EQ(4 * block_size, c.sb_stream->large_used_bytes());

    // The loaded pieces serve their blocks again with no new request.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, 16 * block_size));
    ASSERT_EQ(2u, c.remote->reads.size());
}

TEST_F(CacheInputStreamTest, test_large_range_pieces_do_not_join_across_a_cached_gap) {
    const int64_t block_count = 24;
    SkipCachedBlocksCase c("test_large_range_pieces_do_not_join_across_a_cached_gap", block_size * block_count);
    c.set_coalesce_distance(0);
    c.enable_pieces(8 * block_size);
    c.set_io_range(0, block_size * block_count);
    // The cached block 7 is the last block of the first piece, between the missing blocks 6 and 8.
    c.cache_blocks({7});
    const auto& stats = c.cache_stream->stats();

    // The cached block 7 is read from the cache and not downloaded. The first piece loses its last block, so it has
    // no next piece any more, and the missing block 8 is read through the second piece.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(6 * block_size, 3 * block_size));
    ASSERT_EQ((Reads{{0, 7 * block_size}, {8 * block_size, 8 * block_size}}), c.remote->reads);
    ASSERT_EQ(0, c.sb_stream->direct_io_count());
    ASSERT_EQ(1, stats.read_block_cache_count);
    ASSERT_EQ(block_size, stats.skip_remote_read_bytes);

    auto first = c.sb_stream->find_shared_buffer(6 * block_size, block_size);
    ASSERT_OK(first.status());
    ASSERT_TRUE(first.value()->large);
    ASSERT_TRUE(first.value()->split);
    ASSERT_FALSE(first.value()->has_next_piece);
    ASSERT_FALSE(c.sb_stream->find_shared_pieces(7 * block_size - 100, 200, nullptr).ok());
    ASSERT_FALSE(c.sb_stream->find_shared_buffer(7 * block_size, block_size).ok());
}

} // namespace starrocks::io
