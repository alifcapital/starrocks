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

#include <gtest/gtest.h>

#include <cerrno>

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

} // namespace starrocks::io
