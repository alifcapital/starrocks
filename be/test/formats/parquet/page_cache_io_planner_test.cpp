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

#include "formats/parquet/page_cache_io_planner.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cache/mem_cache/lrucache_engine.h"
#include "cache/mem_cache/page_cache.h"
#include "common/config.h"
#include "formats/parquet/page_reader.h"
#include "formats/parquet/utils.h"
#include "testutil/assert.h"

namespace starrocks::parquet {

using IORange = io::SharedBufferedInputStream::IORange;

namespace {

// A chunk whose pages the test sets. The chunk answers only after it was allowed to load them, as a column that reads
// its offset index does, and it counts the questions and the loads.
struct FakeChunk {
    explicit FakeChunk(int64_t begin, std::vector<PageSpan> spans, bool loaded = false, int64_t first_data_page = -1)
            : begin(begin), spans(std::move(spans)), loaded(loaded), first_data_page(first_data_page) {}

    int64_t end() const { return spans.empty() ? begin : spans.back().offset + spans.back().size; }

    PageCacheIoChunk chunk() {
        PageCacheIoChunk result;
        result.begin = begin;
        result.end = end();
        result.first_data_page = first_data_page;
        result.pages = [this](bool allow_load) -> const std::vector<PageSpan>* {
            ++asks;
            if (!loaded) {
                if (!allow_load) {
                    return nullptr;
                }
                ++loads;
                if (!usable) {
                    return nullptr;
                }
                loaded = true;
            }
            return &spans;
        };
        return result;
    }

    int64_t begin;
    std::vector<PageSpan> spans;
    bool loaded;
    int64_t first_data_page;
    // false: the chunk has no offset index that can be used
    bool usable = true;
    int asks = 0;
    int loads = 0;
};

// The chunk of a dictionary page [0, 40) and four data pages of 100 bytes.
std::vector<PageSpan> chunk_pages(int64_t begin = 0) {
    return {{begin, 40}, {begin + 40, 100}, {begin + 140, 100}, {begin + 240, 100}, {begin + 340, 100}};
}

} // namespace

class PageCacheIoPlannerTest : public testing::Test {
public:
    void SetUp() override {
        MemCacheOptions options{.mem_space_size = 10 * 1024 * 1024};
        _engine = std::make_shared<LRUCacheEngine>();
        ASSERT_OK(_engine->init(options));
        _cache = std::make_shared<StoragePageCache>(_engine.get());
        _file_key = ParquetUtils::get_file_cache_key(CacheType::PAGE, "planner-file", 1000, 123456);
        _saved_enable = config::parquet_page_cache_aware_io_enable;
        _saved_distance = config::io_coalesce_read_max_distance_size;
        config::parquet_page_cache_aware_io_enable = true;
    }

    void TearDown() override {
        config::parquet_page_cache_aware_io_enable = _saved_enable;
        config::io_coalesce_read_max_distance_size = _saved_distance;
    }

protected:
    std::string key_of(uint64_t offset) const {
        std::string key = PageReader::make_page_cache_key(_file_key);
        PageReader::set_page_cache_key_offset(&key, offset);
        return key;
    }

    // The page cache holds the page that starts at the offset.
    void cache_page(uint64_t offset) {
        PageCacheHandle handle;
        ASSERT_OK(_cache->insert(key_of(offset), new std::vector<uint8_t>(16), MemCacheWriteOptions(), &handle));
    }

    std::unique_ptr<PageCacheIoPlanner> make_planner() {
        return std::make_unique<PageCacheIoPlanner>(_cache.get(), _file_key);
    }

    std::vector<std::pair<int64_t, int64_t>> find(PageCacheIoPlanner* planner, int64_t offset, int64_t size) {
        std::vector<IORange> ranges;
        planner->find_skip_ranges(offset, size, &ranges);
        std::vector<std::pair<int64_t, int64_t>> result;
        for (const auto& range : ranges) {
            result.emplace_back(range.offset, range.size);
        }
        return result;
    }

    using Ranges = std::vector<std::pair<int64_t, int64_t>>;

    std::shared_ptr<LRUCacheEngine> _engine;
    std::shared_ptr<StoragePageCache> _cache;
    std::string _file_key;
    bool _saved_enable = true;
    int32_t _saved_distance = 0;
};

// The key of a page is the key of the file and the offset of the page, which is the key that the page reader uses.
TEST_F(PageCacheIoPlannerTest, KeyOfAPageIsTheKeyOfTheFileAndItsOffset) {
    ASSERT_EQ(14u, _file_key.size());
    const std::string key = key_of(0x0102030405060708ULL);
    ASSERT_EQ(22u, key.size());
    ASSERT_EQ(_file_key, key.substr(0, 14));
    uint64_t offset = 0;
    memcpy(&offset, key.data() + 14, sizeof(offset));
    ASSERT_EQ(0x0102030405060708ULL, offset);
}

// A chunk whose pages are known: the planner names the pages of the page cache, in the order of the file.
TEST_F(PageCacheIoPlannerTest, NamesThePagesThatThePageCacheHolds) {
    FakeChunk chunk(0, chunk_pages(), true);
    auto planner = make_planner();
    planner->add_chunk(chunk.chunk());
    ASSERT_TRUE(find(planner.get(), 0, 440).empty());

    // the dictionary page, the second data page and the last one
    cache_page(0);
    cache_page(140);
    cache_page(340);
    ASSERT_EQ((Ranges{{0, 40}, {140, 100}, {340, 100}}), find(planner.get(), 0, 440));
    // the answer follows the cache: a page that the cache lost is not named
    ASSERT_OK(_engine->remove(key_of(140)));
    ASSERT_EQ((Ranges{{0, 40}, {340, 100}}), find(planner.get(), 0, 440));
}

// Only the pages that overlap the bytes of the buffer are probed.
TEST_F(PageCacheIoPlannerTest, ProbesOnlyThePagesOfTheBuffer) {
    FakeChunk chunk(0, chunk_pages(), true);
    FakeChunk other(1000, chunk_pages(1000), true);
    auto planner = make_planner();
    planner->add_chunk(chunk.chunk());
    planner->add_chunk(other.chunk());
    for (int64_t offset : {0, 40, 140, 240, 340, 1000, 1040, 1140, 1240, 1340}) {
        cache_page(offset);
    }

    // [150, 260) overlaps the pages [140, 240) and [240, 340) of the first chunk only. The other pages are cached too,
    // so a page that the planner looked up outside the buffer would be named.
    ASSERT_EQ((Ranges{{140, 200}}), find(planner.get(), 150, 110));
    ASSERT_EQ(0, other.asks);

    // a buffer between the chunks, and a buffer past the last one, hold no page
    ASSERT_TRUE(find(planner.get(), 440, 500).empty());
    ASSERT_TRUE(find(planner.get(), 1440, 500).empty());
    ASSERT_EQ(0, other.asks);

    // a buffer that holds two chunks: every page is cached, and the pages that touch are one range
    ASSERT_EQ((Ranges{{0, 440}, {1000, 440}}), find(planner.get(), 0, 1440));
}

// The pages of a run that the page cache holds are one range, so the stream sorts and joins fewer ranges.
TEST_F(PageCacheIoPlannerTest, NamesARunOfCachedPagesAsOneRange) {
    FakeChunk chunk(0, chunk_pages(), true);
    auto planner = make_planner();
    planner->add_chunk(chunk.chunk());
    cache_page(40);
    cache_page(140);
    cache_page(240);
    cache_page(340);
    ASSERT_EQ((Ranges{{40, 400}}), find(planner.get(), 0, 440));
    ASSERT_EQ((Ranges{{140, 200}}), find(planner.get(), 150, 150));
    // the ranges that the caller has in the list already are kept
    std::vector<IORange> ranges{IORange(7, 3)};
    planner->find_skip_ranges(0, 440, &ranges);
    ASSERT_EQ(2u, ranges.size());
    ASSERT_EQ(7, ranges[0].offset);
}

// The chunks come in the order of the columns, not of the file. The planner finds the chunks of a buffer all the same.
TEST_F(PageCacheIoPlannerTest, FindsChunksAddedOutOfOrder) {
    FakeChunk low(0, chunk_pages(), true);
    FakeChunk middle(1000, chunk_pages(1000), true);
    FakeChunk high(2000, chunk_pages(2000), true);
    auto planner = make_planner();
    planner->add_chunk(high.chunk());
    planner->add_chunk(low.chunk());
    planner->add_chunk(middle.chunk());
    for (int64_t offset : {40, 1040, 2040}) {
        cache_page(offset);
    }

    ASSERT_EQ((Ranges{{1040, 100}}), find(planner.get(), 1000, 440));
    ASSERT_EQ(0, low.asks);
    ASSERT_EQ(0, high.asks);
    ASSERT_EQ((Ranges{{40, 100}, {1040, 100}, {2040, 100}}), find(planner.get(), 0, 2440));
}

// We read the offset index of a chunk only when the page cache holds the first page of a chunk of the row group.
TEST_F(PageCacheIoPlannerTest, ReadsTheOffsetIndexesOnlyWhenThePageCacheHoldsAFirstPage) {
    FakeChunk first(0, chunk_pages());
    FakeChunk second(1000, chunk_pages(1000));
    {
        auto planner = make_planner();
        planner->add_chunk(first.chunk());
        planner->add_chunk(second.chunk());
        // the page cache holds a page of the first chunk, but not its first page
        cache_page(140);
        ASSERT_TRUE(find(planner.get(), 0, 1440).empty());
        ASSERT_TRUE(find(planner.get(), 0, 1440).empty());
        ASSERT_EQ(0, first.loads);
        ASSERT_EQ(0, second.loads);
        // the chunks were asked with no permission to load, and the answer of the gate is kept
        ASSERT_EQ(2, first.asks);
        ASSERT_EQ(2, second.asks);
    }
    {
        // a page cache that holds the first page of the second chunk opens the gate for both chunks
        cache_page(1000);
        auto planner = make_planner();
        planner->add_chunk(first.chunk());
        planner->add_chunk(second.chunk());
        ASSERT_EQ((Ranges{{140, 100}, {1000, 40}}), find(planner.get(), 0, 1440));
        ASSERT_EQ(1, first.loads);
        ASSERT_EQ(1, second.loads);

        // the chunks hold their pages now, and the planner does not ask them to load again
        ASSERT_EQ((Ranges{{140, 100}, {1000, 40}}), find(planner.get(), 0, 1440));
        ASSERT_EQ(1, first.loads);
        ASSERT_EQ(1, second.loads);
    }
}

// The footer gives the offset of the first data page after a dictionary page, so the gate opens when the page cache
// holds that page too, although it does not hold the dictionary page.
TEST_F(PageCacheIoPlannerTest, GateOpensForTheFirstDataPageToo) {
    {
        FakeChunk chunk(0, chunk_pages(), false, 40);
        auto planner = make_planner();
        planner->add_chunk(chunk.chunk());
        cache_page(40);
        ASSERT_EQ((Ranges{{40, 100}}), find(planner.get(), 0, 440));
        ASSERT_EQ(1, chunk.loads);
    }
    {
        // a page that only the offset index locates does not open the gate
        FakeChunk chunk(0, chunk_pages(), false, 40);
        auto planner = make_planner();
        planner->add_chunk(chunk.chunk());
        ASSERT_OK(_engine->remove(key_of(40)));
        cache_page(240);
        ASSERT_TRUE(find(planner.get(), 0, 440).empty());
        ASSERT_EQ(0, chunk.loads);
    }
}

// A chunk that cannot give its pages is skipped, and the other chunks are still named.
TEST_F(PageCacheIoPlannerTest, SkipsAChunkWithoutPages) {
    FakeChunk unusable(0, chunk_pages());
    unusable.usable = false;
    FakeChunk usable(1000, chunk_pages(1000), true);
    auto planner = make_planner();
    planner->add_chunk(unusable.chunk());
    planner->add_chunk(usable.chunk());
    cache_page(0);
    cache_page(1000);
    ASSERT_EQ((Ranges{{1000, 40}}), find(planner.get(), 0, 1440));
    ASSERT_EQ(1, unusable.loads);

    // a chunk without a function to ask is not added
    PageCacheIoChunk empty;
    empty.begin = 5000;
    empty.end = 6000;
    planner->add_chunk(empty);
    ASSERT_TRUE(find(planner.get(), 4000, 3000).empty());
}

// The reader that loads an offset index for the planner reads the stream, and the stream asks the planner about the
// buffer of the index. The planner names nothing then.
TEST_F(PageCacheIoPlannerTest, DoesNotAnswerWhileItIsAsked) {
    auto planner = make_planner();
    FakeChunk chunk(0, chunk_pages(), true);
    PageCacheIoChunk inner = chunk.chunk();
    Ranges nested;
    bool called = false;
    PageCacheIoChunk wrapped;
    wrapped.begin = inner.begin;
    wrapped.end = inner.end;
    wrapped.pages = [&](bool allow_load) {
        called = true;
        nested = find(planner.get(), 0, 440);
        return inner.pages(allow_load);
    };
    planner->add_chunk(wrapped);
    cache_page(0);
    ASSERT_EQ((Ranges{{0, 40}}), find(planner.get(), 0, 440));
    ASSERT_TRUE(called);
    ASSERT_TRUE(nested.empty());
    // the planner answers again after the ask
    nested.clear();
    ASSERT_EQ((Ranges{{0, 40}}), find(planner.get(), 0, 440));
}

// The planner only probes the page cache. A probe is not a lookup: the cache counts no lookup and no hit for it, so the
// metrics of the cache show the reads of the query only, and the probe does not refresh the pages that it finds.
TEST_F(PageCacheIoPlannerTest, ProbesDoNotChangeTheMetricsOfThePageCache) {
    FakeChunk chunk(0, chunk_pages(), true);
    auto planner = make_planner();
    planner->add_chunk(chunk.chunk());
    cache_page(0);
    cache_page(140);
    cache_page(340);

    const uint64_t lookups = _cache->get_lookup_count();
    const uint64_t hits = _cache->get_hit_count();
    ASSERT_EQ((Ranges{{0, 40}, {140, 100}, {340, 100}}), find(planner.get(), 0, 440));
    ASSERT_EQ((Ranges{{0, 40}, {140, 100}, {340, 100}}), find(planner.get(), 0, 440));
    ASSERT_EQ(lookups, _cache->get_lookup_count());
    ASSERT_EQ(hits, _cache->get_hit_count());

    // a lookup of the page reader is counted as before
    PageCacheHandle handle;
    ASSERT_TRUE(_cache->lookup(key_of(140), &handle));
    ASSERT_EQ(lookups + 1, _cache->get_lookup_count());
    ASSERT_EQ(hits + 1, _cache->get_hit_count());
}

// The config switches the planner off at once: no lookup, no read of an index.
TEST_F(PageCacheIoPlannerTest, ConfigSwitchesThePlannerOff) {
    FakeChunk chunk(0, chunk_pages());
    auto planner = make_planner();
    planner->add_chunk(chunk.chunk());
    cache_page(0);
    cache_page(140);

    config::parquet_page_cache_aware_io_enable = false;
    ASSERT_FALSE(planner->enabled());
    const uint64_t lookups = _cache->get_lookup_count();
    ASSERT_TRUE(find(planner.get(), 0, 440).empty());
    ASSERT_EQ(lookups, _cache->get_lookup_count());
    ASSERT_EQ(0, chunk.asks);
    ASSERT_EQ(0, chunk.loads);

    config::parquet_page_cache_aware_io_enable = true;
    ASSERT_TRUE(planner->enabled());
    ASSERT_EQ((Ranges{{0, 40}, {140, 100}}), find(planner.get(), 0, 440));
}

// The planner is a provider of the shared stream: the stream does not load the pages that the page cache holds.
TEST_F(PageCacheIoPlannerTest, StreamDoesNotLoadTheNamedPages) {
    constexpr int64_t kSize = 440;
    std::string contents(kSize + 100, 0);
    for (size_t i = 0; i < contents.size(); ++i) {
        contents[i] = static_cast<char>('a' + (i * 7) % 26);
    }
    config::io_coalesce_read_max_distance_size = 0;

    class RecordingStream final : public io::SeekableInputStream {
    public:
        explicit RecordingStream(std::string contents) : _contents(std::move(contents)) {}
        StatusOr<int64_t> read(void* data, int64_t count) override {
            count = std::min<int64_t>(count, _contents.size() - _offset);
            memcpy(data, _contents.data() + _offset, count);
            _offset += count;
            return count;
        }
        Status read_at_fully(int64_t offset, void* out, int64_t count) override {
            reads.emplace_back(offset, count);
            memcpy(out, _contents.data() + offset, count);
            return Status::OK();
        }
        Status seek(int64_t position) override {
            _offset = position;
            return Status::OK();
        }
        StatusOr<int64_t> position() override { return _offset; }
        StatusOr<int64_t> get_size() override { return _contents.size(); }
        std::vector<std::pair<int64_t, int64_t>> reads;

    private:
        std::string _contents;
        int64_t _offset = 0;
    };

    auto remote = std::make_shared<RecordingStream>(contents);
    io::SharedBufferedInputStream stream(remote, "planner-file", contents.size());
    ASSERT_OK(stream.set_io_ranges({IORange(0, kSize)}));

    FakeChunk chunk(0, chunk_pages(), true);
    auto planner = make_planner();
    planner->add_chunk(chunk.chunk());
    cache_page(0);
    cache_page(140);
    cache_page(240);
    stream.set_skip_ranges_provider(planner.get());

    // the dictionary page, and the second and the third data pages are cached: the first and the last data pages remain
    std::string out(100, 0);
    ASSERT_OK(stream.read_at_fully(40, out.data(), 100));
    ASSERT_OK(stream.read_at_fully(340, out.data(), 100));
    ASSERT_EQ((Ranges{{40, 100}, {340, 100}}), remote->reads);
    ASSERT_EQ(240, stream.skip_ranges_bytes());
    ASSERT_EQ(1, stream.skip_ranges_split_count());
}

} // namespace starrocks::parquet
