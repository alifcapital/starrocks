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

#include "formats/parquet/narrow_io_planner.h"

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "formats/parquet/parquet_test_util/recording_input_stream.h"
#include "io/shared_buffered_input_stream.h"
#include "testutil/assert.h"

namespace starrocks::parquet {

namespace {

constexpr int64_t KB = 1024;

using IORange = io::SharedBufferedInputStream::IORange;
// (begin, end) of each read that the file serves, as RecordingInputStream records them.
using Ranges = std::vector<std::pair<int64_t, int64_t>>;

// A file behind a recording stream, read through shared buffers that the planner of the narrowing advises. The bytes are
// not periodic in the alignment, so a read from a wrong place never matches. One column chunk takes the bytes
// [chunk_begin, chunk_end) of the file, and the test says what each read range of it needs.
struct NarrowCase {
    explicit NarrowCase(int64_t size, int64_t align_size = 0, int64_t chunk_begin = 0, int64_t chunk_end = -1,
                        int64_t data_begin = -1)
            : planner(&stats) {
        data.resize(size);
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = static_cast<char>(i % 251);
        }
        remote = std::make_shared<RecordingInputStream>(data);
        stream = std::make_shared<io::SharedBufferedInputStream>(remote, "narrow_case", data.size());
        if (align_size > 0) {
            stream->set_align_size(align_size);
        }
        stream->set_skip_ranges_provider(&planner);
        chunk = planner.add_chunk(chunk_begin, chunk_end < 0 ? size : chunk_end,
                                  data_begin < 0 ? chunk_begin : data_begin, [this](NarrowIoPlanner::Need* need) {
                                      ++compute_calls;
                                      *need = needs.at(current_range);
                                  });
    }

    void set_io_ranges(const std::vector<IORange>& ranges) { ASSERT_OK(stream->set_io_ranges(ranges)); }

    // The reader begins a read range that needs these byte runs. Bytes in front of dead_end that are not needed are dead.
    // [ahead_begin, ahead_end) is read ahead. The reader has read or jumped over the pages in front of read_until, and it
    // jumped over dead_bytes of them.
    void begin_range(std::vector<IORange> needed, int64_t dead_end, int64_t ahead_begin = 0, int64_t ahead_end = 0,
                     int64_t read_until = 0, int64_t dead_bytes = 0) {
        NarrowIoPlanner::Need need;
        need.needed = std::move(needed);
        need.dead_end = dead_end;
        need.ahead_begin = ahead_begin;
        need.ahead_end = ahead_end;
        needs.push_back(std::move(need));
        current_range = needs.size() - 1;
        planner.begin_range(chunk, read_until, dead_bytes);
    }
    void end_range() { planner.end_range(chunk); }

    void read_and_check(int64_t offset, int64_t size) {
        std::string buffer(size, 0);
        ASSERT_OK(stream->read_at_fully(offset, buffer.data(), size));
        ASSERT_EQ(0, memcmp(buffer.data(), data.data() + offset, size)) << "offset=" << offset << ", size=" << size;
    }

    std::string data;
    std::shared_ptr<RecordingInputStream> remote;
    std::shared_ptr<io::SharedBufferedInputStream> stream;
    HdfsScannerStats stats;
    NarrowIoPlanner planner;
    NarrowIoPlanner::Chunk* chunk = nullptr;
    std::vector<NarrowIoPlanner::Need> needs;
    size_t current_range = 0;
    int compute_calls = 0;
};

class NarrowIoPlannerTest : public ::testing::Test {
public:
    void SetUp() override {
        _saved_distance = config::io_coalesce_read_max_distance_size;
        _saved_enable = config::parquet_offset_index_narrow_io_enable;
        _saved_split = config::io_coalesce_split_large_range;
        _saved_ratio = config::parquet_offset_index_narrow_io_defer_min_dead_ratio;
        _saved_dict = config::parquet_offset_index_narrow_io_dict_defer_enable;
        // the gap that the stream loads with the runs around it
        config::io_coalesce_read_max_distance_size = 64 * KB;
        config::parquet_offset_index_narrow_io_enable = true;
        config::parquet_offset_index_narrow_io_defer_min_dead_ratio = 0.5;
        config::parquet_offset_index_narrow_io_dict_defer_enable = true;
    }

    void TearDown() override {
        config::io_coalesce_read_max_distance_size = _saved_distance;
        config::parquet_offset_index_narrow_io_enable = _saved_enable;
        config::io_coalesce_split_large_range = _saved_split;
        config::parquet_offset_index_narrow_io_defer_min_dead_ratio = _saved_ratio;
        config::parquet_offset_index_narrow_io_dict_defer_enable = _saved_dict;
    }

private:
    int32_t _saved_distance = 0;
    bool _saved_enable = true;
    bool _saved_split = true;
    double _saved_ratio = 0.5;
    bool _saved_dict = true;
};

// A provider that names a fixed range as skip, to test the chain.
class FixedProvider final : public io::SharedBufferedInputStream::SkipRangesProvider {
public:
    FixedProvider(int64_t offset, int64_t size) : _offset(offset), _size(size) {}
    void find_skip_ranges(int64_t, int64_t, std::vector<IORange>* ranges) override {
        ranges->emplace_back(_offset, _size);
    }

private:
    int64_t _offset;
    int64_t _size;
};

} // namespace

// The chunk is one buffer of 1 MB. The range needs the runs [100 KB, 150 KB), [300 KB, 320 KB) and [700 KB, 730 KB), and
// it ends at 730 KB. The bytes in front of the runs and between them are dead, and they are far apart (more than the
// distance of 64 KB), so each run is loaded with a request of its own. The bytes behind the range may be needed by a later
// range, so they are a buffer that is not loaded.
TEST_F(NarrowIoPlannerTest, test_dead_pages_are_not_loaded_and_the_rest_is_deferred) {
    NarrowCase c(1024 * KB);
    ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1024 * KB)}));
    c.begin_range({IORange(100 * KB, 50 * KB), IORange(300 * KB, 20 * KB), IORange(700 * KB, 30 * KB)}, 730 * KB);

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(100 * KB, 4 * KB));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(300 * KB, 4 * KB));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(710 * KB, 4 * KB));
    ASSERT_EQ((Ranges{{100 * KB, 150 * KB}, {300 * KB, 320 * KB}, {700 * KB, 730 * KB}}), c.remote->ranges());
    ASSERT_EQ(3, c.stream->shared_io_count());
    ASSERT_EQ(1, c.stream->skip_ranges_split_count());
    // [0, 100 KB), [150 KB, 300 KB) and [320 KB, 700 KB)
    ASSERT_EQ((100 + 150 + 380) * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ((100 + 150 + 380) * KB, c.stats.narrow_io_named_bytes);
    ASSERT_EQ(1, c.stats.narrow_io_named_buffers);
    // [730 KB, 1 MB)
    ASSERT_EQ(294 * KB, c.stream->skip_ranges_deferred_bytes());
    ASSERT_EQ(294 * KB, c.stats.narrow_io_deferred_bytes);
    ASSERT_EQ(0, c.stream->direct_io_count());
    ASSERT_EQ(1, c.compute_calls);
    ASSERT_GT(c.stats.narrow_io_plan_ns, 0);
    ASSERT_GT(c.stream->skip_ranges_timer(), 0);

    auto deferred = c.stream->find_shared_buffer(800 * KB, 100 * KB);
    ASSERT_OK(deferred.status());
    ASSERT_EQ(0u, deferred.value()->buffer.capacity());
    ASSERT_FALSE(deferred.value()->planned);
}

// Three consecutive ranges of a chunk of ten pages of 100 KB. Each range loads only its pages: the buffer of the chunk is
// split at the first range, the buffer that is not loaded is asked again for the second range, and so on. No read is
// direct.
TEST_F(NarrowIoPlannerTest, test_three_consecutive_ranges_load_only_their_pages) {
    NarrowCase c(1000 * KB);
    ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1000 * KB)}));

    // range 1 needs the page 1 and ends with the page 2
    c.begin_range({IORange(100 * KB, 100 * KB)}, 300 * KB);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(100 * KB, 100 * KB));
    c.end_range();
    ASSERT_EQ(Ranges{std::make_pair(100 * KB, 200 * KB)}, c.remote->ranges());
    ASSERT_EQ(200 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ(700 * KB, c.stream->skip_ranges_deferred_bytes());

    // range 2 needs the pages 4 and 7 and ends with the page 8: the buffer [300 KB, 1000 KB) is asked again
    c.begin_range({IORange(400 * KB, 100 * KB), IORange(700 * KB, 100 * KB)}, 900 * KB);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(400 * KB, 100 * KB));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(700 * KB, 100 * KB));
    c.end_range();
    ASSERT_EQ((Ranges{{100 * KB, 200 * KB}, {400 * KB, 500 * KB}, {700 * KB, 800 * KB}}), c.remote->ranges());
    // [300 KB, 400 KB), [500 KB, 700 KB) and [800 KB, 900 KB) are dead now
    ASSERT_EQ(600 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ(700 * KB + 100 * KB, c.stream->skip_ranges_deferred_bytes());

    // range 3 needs the page 9, the last one
    c.begin_range({IORange(900 * KB, 100 * KB)}, 1000 * KB);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(900 * KB, 100 * KB));
    c.end_range();
    ASSERT_EQ((Ranges{{100 * KB, 200 * KB}, {400 * KB, 500 * KB}, {700 * KB, 800 * KB}, {900 * KB, 1000 * KB}}),
              c.remote->ranges());
    ASSERT_EQ(4, c.stream->shared_io_count());
    ASSERT_EQ(400 * KB, c.stream->shared_io_bytes());
    ASSERT_EQ(600 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ(0, c.stream->direct_io_count());
    ASSERT_EQ(0, c.stream->skip_ranges_recover_count());
    ASSERT_EQ(3, c.compute_calls);
}

// The bytes behind the last needed page are read ahead with it, so the next range finds the page in the buffer: the second
// range loads nothing.
TEST_F(NarrowIoPlannerTest, test_read_ahead_is_loaded_with_the_run) {
    NarrowCase c(1000 * KB);
    ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1000 * KB)}));
    // range 1 needs the page 3 and ends with the page 3; the page 4 is read ahead
    c.begin_range({IORange(300 * KB, 100 * KB)}, 400 * KB, 400 * KB, 500 * KB);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(300 * KB, 100 * KB));
    c.end_range();
    ASSERT_EQ(Ranges{std::make_pair(300 * KB, 500 * KB)}, c.remote->ranges());
    ASSERT_EQ(500 * KB, c.stream->skip_ranges_deferred_bytes());

    c.begin_range({IORange(400 * KB, 100 * KB)}, 500 * KB);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(400 * KB, 100 * KB));
    c.end_range();
    ASSERT_EQ(Ranges{std::make_pair(300 * KB, 500 * KB)}, c.remote->ranges());
    ASSERT_EQ(0, c.stream->direct_io_count());
    // the second range read the loaded buffer, and the planner was not asked about it
    ASSERT_EQ(1, c.compute_calls);
}

// The reader is asked what a range needs only when the stream loads a buffer: a range whose bytes are in loaded buffers
// costs nothing. The answer for a range serves all buffers that the range loads.
TEST_F(NarrowIoPlannerTest, test_the_need_is_computed_only_when_a_buffer_is_loaded) {
    config::io_coalesce_split_large_range = true;
    NarrowCase c(1024 * KB);
    io::SharedBufferedInputStream::CoalesceOptions options;
    options.max_dist_size = 0;
    options.max_buffer_size = 256 * KB;
    c.stream->set_coalesce_options(options);
    ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1024 * KB)}));

    // ranges that load nothing
    c.begin_range({IORange(100 * KB, 10 * KB)}, 110 * KB);
    c.end_range();
    c.begin_range({IORange(200 * KB, 10 * KB)}, 210 * KB);
    c.end_range();
    ASSERT_EQ(0, c.compute_calls);
    ASSERT_EQ(0, c.stats.narrow_io_plan_ns);

    // a range that loads two pieces of a large range is computed once
    c.begin_range({IORange(100 * KB, 10 * KB), IORange(600 * KB, 10 * KB)}, 700 * KB);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(100 * KB, 10 * KB));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(600 * KB, 10 * KB));
    c.end_range();
    ASSERT_EQ(1, c.compute_calls);
    ASSERT_EQ((Ranges{{100 * KB, 110 * KB}, {600 * KB, 610 * KB}}), c.remote->ranges());
}

// A chunk that has shown that it needs most of its bytes does not defer: the buffer is loaded whole, with one request,
// and the next ranges do not pay a request each. A chunk that has shown nothing yet defers.
TEST_F(NarrowIoPlannerTest, test_a_dense_chunk_does_not_defer) {
    {
        // the ranges needed 300 KB of the first 300 KB
        NarrowCase c(1000 * KB);
        ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1000 * KB)}));
        c.begin_range({IORange(0, 100 * KB), IORange(100 * KB, 100 * KB), IORange(200 * KB, 100 * KB)}, 300 * KB);
        ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, 100 * KB));
        ASSERT_EQ(Ranges{std::make_pair(0, 1000 * KB)}, c.remote->ranges());
        ASSERT_EQ(0, c.stream->skip_ranges_deferred_bytes());
    }
    {
        // the range ends inside the page 0: nothing is dead and nothing was shown, so the chunk defers
        NarrowCase c(1000 * KB);
        ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1000 * KB)}));
        c.begin_range({IORange(0, 100 * KB)}, 0);
        ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, 100 * KB));
        ASSERT_EQ(Ranges{std::make_pair(0, 100 * KB)}, c.remote->ranges());
        ASSERT_EQ(900 * KB, c.stream->skip_ranges_deferred_bytes());
    }
    {
        // a ratio of 0 always defers
        config::parquet_offset_index_narrow_io_defer_min_dead_ratio = 0;
        NarrowCase c(1000 * KB);
        ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1000 * KB)}));
        c.begin_range({IORange(0, 100 * KB), IORange(100 * KB, 100 * KB), IORange(200 * KB, 100 * KB)}, 300 * KB);
        ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, 100 * KB));
        ASSERT_EQ(Ranges{std::make_pair(0, 300 * KB)}, c.remote->ranges());
        ASSERT_EQ(700 * KB, c.stream->skip_ranges_deferred_bytes());
    }
}

// A chunk of 40 pages of 100 KB, and a filter that selects a row in every page, one range for each page. The first range
// loads its page and reads the next page ahead, the second range is served from the buffer and never asks the planner, and
// the third range asks. The reader tells the planner what it passed for every range, so the planner sees that no page was
// dead, stops deferring, and the rest of the chunk is loaded with one request: two requests, not one for every page.
TEST_F(NarrowIoPlannerTest, test_a_scan_that_needs_all_pages_stops_deferring) {
    NarrowCase c(4000 * KB);
    ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 4000 * KB)}));

    // the range ends inside the page 0, and the page 1 is read ahead
    c.begin_range({IORange(0, 100 * KB)}, 0, 100 * KB, 200 * KB, 0, 0);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, 100 * KB));
    c.end_range();
    ASSERT_EQ(Ranges{std::make_pair(0, 200 * KB)}, c.remote->ranges());
    ASSERT_EQ(3800 * KB, c.stream->skip_ranges_deferred_bytes());

    // the page 1 is in the buffer: the planner is not asked
    c.begin_range({IORange(100 * KB, 100 * KB)}, 100 * KB, 200 * KB, 300 * KB, 100 * KB, 0);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(100 * KB, 100 * KB));
    c.end_range();
    ASSERT_EQ(1, c.compute_calls);

    // the page 2 is not, and the reader passed 200 KB and jumped over nothing
    c.begin_range({IORange(200 * KB, 100 * KB)}, 200 * KB, 300 * KB, 400 * KB, 200 * KB, 0);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(200 * KB, 100 * KB));
    c.end_range();
    ASSERT_EQ(2, c.compute_calls);
    ASSERT_EQ((Ranges{{0, 200 * KB}, {200 * KB, 4000 * KB}}), c.remote->ranges());
    ASSERT_EQ(3800 * KB, c.stream->skip_ranges_deferred_bytes());

    // the pages behind are in the buffer
    c.begin_range({IORange(2000 * KB, 100 * KB)}, 2000 * KB, 2100 * KB, 2200 * KB, 2000 * KB, 0);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(2000 * KB, 100 * KB));
    c.end_range();
    ASSERT_EQ(2u, c.remote->ranges().size());
    ASSERT_EQ(0, c.stream->direct_io_count());
}

// The same ranges, but the reader jumped over most of the pages in front of the third range: the chunk has many dead bytes,
// and the planner keeps deferring.
TEST_F(NarrowIoPlannerTest, test_a_scan_that_jumps_over_pages_keeps_deferring) {
    NarrowCase c(4000 * KB);
    ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 4000 * KB)}));
    c.begin_range({IORange(0, 100 * KB)}, 0, 100 * KB, 200 * KB, 0, 0);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, 100 * KB));
    c.end_range();
    c.begin_range({IORange(100 * KB, 100 * KB)}, 100 * KB, 200 * KB, 300 * KB, 100 * KB, 0);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(100 * KB, 100 * KB));
    c.end_range();

    // the reader passed 1000 KB and jumped over 800 KB of them
    c.begin_range({IORange(1000 * KB, 100 * KB)}, 1000 * KB, 1100 * KB, 1200 * KB, 1000 * KB, 800 * KB);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(1000 * KB, 100 * KB));
    c.end_range();
    // [200 KB, 1000 KB) is dead, and [1200 KB, 4 MB) is deferred
    ASSERT_EQ((Ranges{{0, 200 * KB}, {1000 * KB, 1200 * KB}}), c.remote->ranges());
    ASSERT_EQ(800 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ((3800 + 2800) * KB, c.stream->skip_ranges_deferred_bytes());
}

// A range counts its own pages too: it needs all the pages in front of its end, so it shows no dead bytes, also when the
// reader has not come far yet.
TEST_F(NarrowIoPlannerTest, test_the_pages_of_the_range_count_as_evidence) {
    NarrowCase c(1000 * KB);
    ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1000 * KB)}));
    // the range needs [100 KB, 300 KB) and ends at 300 KB, after the reader has read 100 KB
    c.begin_range({IORange(100 * KB, 200 * KB)}, 300 * KB, 0, 0, 100 * KB, 0);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(100 * KB, 100 * KB));
    // the bytes in front of the range are dead, and the rest of the chunk is loaded with the range, not deferred
    ASSERT_EQ(Ranges{std::make_pair(100 * KB, 1000 * KB)}, c.remote->ranges());
    ASSERT_EQ(100 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ(0, c.stream->skip_ranges_deferred_bytes());
    c.end_range();
}

// The planner asks again about a deferred buffer only while a range is being read: a read between the ranges loads the
// buffer whole, and the dead bytes of the last range stay cut.
TEST_F(NarrowIoPlannerTest, test_nothing_is_deferred_outside_a_range) {
    NarrowCase c(1000 * KB);
    ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1000 * KB)}));
    c.begin_range({IORange(100 * KB, 100 * KB)}, 300 * KB);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(100 * KB, 100 * KB));
    c.end_range();
    ASSERT_EQ(700 * KB, c.stream->skip_ranges_deferred_bytes());

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(500 * KB, 10 * KB));
    ASSERT_EQ((Ranges{{100 * KB, 200 * KB}, {300 * KB, 1000 * KB}}), c.remote->ranges());
    ASSERT_EQ(700 * KB, c.stream->skip_ranges_deferred_bytes());
    ASSERT_EQ(0, c.stream->direct_io_count());
}

// The data behind the dictionary page is deferred, and the first range loads only its pages from it.
TEST_F(NarrowIoPlannerTest, test_dictionary_read_defers_the_data) {
    NarrowCase c(1000 * KB);
    ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1000 * KB)}));
    // the dictionary page is [0, 100 KB), and the reader reads its header with 16 KB
    c.planner.begin_dictionary_read(0, 100 * KB, 1000 * KB);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, 16 * KB));
    c.planner.end_dictionary_read(0);
    ASSERT_EQ(Ranges{std::make_pair(0, 100 * KB)}, c.remote->ranges());
    ASSERT_EQ(900 * KB, c.stream->skip_ranges_deferred_bytes());
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(16 * KB, 84 * KB));
    ASSERT_EQ(1u, c.remote->ranges().size());

    // the first range needs the page 5
    c.begin_range({IORange(500 * KB, 100 * KB)}, 600 * KB);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(500 * KB, 100 * KB));
    c.end_range();
    ASSERT_EQ((Ranges{{0, 100 * KB}, {500 * KB, 600 * KB}}), c.remote->ranges());
    ASSERT_EQ(0, c.stream->direct_io_count());
    ASSERT_EQ(400 * KB, c.stream->skip_ranges_bytes());
}

// The data behind a small dictionary page is not deferred when it is smaller than 4 times the distance, because a buffer
// of its own costs a request when a read needs it. A chunk with a need keeps it, and a config switches it off.
TEST_F(NarrowIoPlannerTest, test_dictionary_read_conditions) {
    {
        NarrowCase c(1000 * KB);
        ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1000 * KB)}));
        // 200 KB behind the dictionary page, and 4 * 64 KB = 256 KB
        c.planner.begin_dictionary_read(0, 800 * KB, 1000 * KB);
        ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, 16 * KB));
        c.planner.end_dictionary_read(0);
        ASSERT_EQ(Ranges{std::make_pair(0, 1000 * KB)}, c.remote->ranges());
    }
    {
        config::parquet_offset_index_narrow_io_dict_defer_enable = false;
        NarrowCase c(1000 * KB);
        ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1000 * KB)}));
        c.planner.begin_dictionary_read(0, 100 * KB, 1000 * KB);
        ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, 16 * KB));
        c.planner.end_dictionary_read(0);
        ASSERT_EQ(Ranges{std::make_pair(0, 1000 * KB)}, c.remote->ranges());
        config::parquet_offset_index_narrow_io_dict_defer_enable = true;
    }
    {
        // a chunk that is being read keeps its need
        NarrowCase c(1000 * KB);
        ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1000 * KB)}));
        c.begin_range({IORange(500 * KB, 100 * KB)}, 600 * KB);
        c.planner.begin_dictionary_read(0, 100 * KB, 1000 * KB);
        ASSERT_NO_FATAL_FAILURE(c.read_and_check(500 * KB, 100 * KB));
        c.planner.end_dictionary_read(0);
        c.end_range();
        ASSERT_EQ(Ranges{std::make_pair(500 * KB, 600 * KB)}, c.remote->ranges());
        ASSERT_EQ(1, c.compute_calls);
    }
}

// The planner names nothing when the config is off, and the buffer is loaded whole.
TEST_F(NarrowIoPlannerTest, test_config_off) {
    config::parquet_offset_index_narrow_io_enable = false;
    NarrowCase c(1024 * KB);
    ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1024 * KB)}));
    c.begin_range({IORange(100 * KB, 50 * KB), IORange(300 * KB, 20 * KB), IORange(700 * KB, 30 * KB)}, 730 * KB);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(100 * KB, 4 * KB));
    ASSERT_EQ(Ranges{std::make_pair(0, 1024 * KB)}, c.remote->ranges());
    ASSERT_EQ(0, c.stream->skip_ranges_bytes());
    ASSERT_EQ(0, c.stats.narrow_io_named_buffers);
    ASSERT_EQ(0, c.compute_calls);
}

// A large io range is cut into pieces of at most the coalesce buffer size, and the stream asks about each piece on its
// own first load. The piece that holds the dead bytes is cut, the rest of the piece behind the range is deferred, and a
// piece that holds neither is loaded whole.
TEST_F(NarrowIoPlannerTest, test_pieces_of_a_large_range) {
    config::io_coalesce_split_large_range = true;
    NarrowCase c(1024 * KB);
    io::SharedBufferedInputStream::CoalesceOptions options;
    options.max_dist_size = 0;
    options.max_buffer_size = 256 * KB;
    c.stream->set_coalesce_options(options);
    // four pieces of 256 KB
    ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1024 * KB)}));
    // The range needs [300 KB, 320 KB) and ends there. The piece [256 KB, 512 KB) holds dead bytes and bytes for later.
    c.begin_range({IORange(300 * KB, 20 * KB)}, 320 * KB);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(300 * KB, 20 * KB));
    c.end_range();
    ASSERT_EQ(Ranges{std::make_pair(300 * KB, 320 * KB)}, c.remote->ranges());
    ASSERT_EQ(44 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ(192 * KB, c.stream->skip_ranges_deferred_bytes());
    auto deferred = c.stream->find_shared_buffer(400 * KB, 10 * KB);
    ASSERT_OK(deferred.status());
    ASSERT_TRUE(deferred.value()->large);
    ASSERT_TRUE(deferred.value()->has_next_piece);
    ASSERT_FALSE(deferred.value()->planned);

    // the next range needs a page in the deferred piece, and in the piece behind it
    c.begin_range({IORange(450 * KB, 20 * KB), IORange(600 * KB, 20 * KB)}, 620 * KB);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(450 * KB, 20 * KB));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(600 * KB, 20 * KB));
    c.end_range();
    ASSERT_EQ((Ranges{{300 * KB, 320 * KB}, {450 * KB, 470 * KB}, {600 * KB, 620 * KB}}), c.remote->ranges());
    ASSERT_EQ(0, c.stream->direct_io_count());

    // a piece without dead bytes is loaded whole
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(900 * KB, 10 * KB));
    ASSERT_EQ((Ranges{{300 * KB, 320 * KB}, {450 * KB, 470 * KB}, {600 * KB, 620 * KB}, {768 * KB, 1024 * KB}}),
              c.remote->ranges());
    ASSERT_EQ(0, c.stream->direct_io_count());
}

// With a data cache the stream is aligned to its blocks of 256 KB, and the cache reads whole blocks. The stream cuts and
// defers only whole blocks, so a block that holds a byte to load is loaded whole, also when it holds dead bytes: the page
// [1000 KB, 1050 KB) lies in the blocks [768 KB, 1024 KB) and [1024 KB, 1280 KB), and the block [0, 256 KB) holds the
// start of the chunk at 100 KB.
TEST_F(NarrowIoPlannerTest, test_cuts_and_deferred_buffers_keep_to_the_blocks) {
    constexpr int64_t kBlock = 256 * KB;
    NarrowCase c(2048 * KB, kBlock, 100 * KB, 1900 * KB);
    // the aligned range of the buffer is [0, 2 MB)
    ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(100 * KB, 1800 * KB)}));
    c.begin_range({IORange(1000 * KB, 50 * KB)}, 1050 * KB);

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(1000 * KB, 50 * KB));
    // the dead bytes [100 KB, 1000 KB) give up their partial blocks, and the whole blocks [256 KB, 768 KB) are cut; the
    // bytes behind the range, [1050 KB, 1900 KB), give up theirs, and the blocks [1280 KB, 1792 KB) are deferred
    ASSERT_EQ(Ranges{std::make_pair(768 * KB, 1280 * KB)}, c.remote->ranges());
    ASSERT_EQ(512 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ(512 * KB, c.stream->skip_ranges_deferred_bytes());
    auto deferred = c.stream->find_shared_buffer(1280 * KB, 512 * KB);
    ASSERT_OK(deferred.status());
    ASSERT_EQ(1280 * KB, deferred.value()->offset);
    ASSERT_EQ(512 * KB, deferred.value()->size);
    ASSERT_FALSE(deferred.value()->planned);
    c.end_range();

    // the blocks at the ends of the chunk are loaded whole
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(110 * KB, 10 * KB));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(1850 * KB, 10 * KB));
    ASSERT_EQ((Ranges{{768 * KB, 1280 * KB}, {0, 256 * KB}, {1792 * KB, 2048 * KB}}), c.remote->ranges());
    for (const auto& range : c.remote->ranges()) {
        ASSERT_EQ(0, range.first % kBlock);
        ASSERT_EQ(0, range.second % kBlock);
    }
    ASSERT_EQ(0, c.stream->direct_io_count());
}

// The stream has one provider. The chain asks all of them, a byte that one skips and the planner defers is skipped, and a
// buffer loads the bytes that none names.
TEST_F(NarrowIoPlannerTest, test_chain_of_providers) {
    NarrowCase c(1000 * KB);
    ASSERT_NO_FATAL_FAILURE(c.set_io_ranges({IORange(0, 1000 * KB)}));
    c.begin_range({IORange(100 * KB, 100 * KB)}, 300 * KB);
    // a second provider skips [500 KB, 600 KB), which the planner defers
    FixedProvider other(500 * KB, 100 * KB);
    SkipRangesProviderChain chain;
    ASSERT_TRUE(chain.empty());
    chain.add(nullptr);
    ASSERT_TRUE(chain.empty());
    chain.add(&c.planner);
    chain.add(&other);
    ASSERT_FALSE(chain.empty());
    c.stream->set_skip_ranges_provider(&chain);

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(100 * KB, 100 * KB));
    ASSERT_EQ(Ranges{std::make_pair(100 * KB, 200 * KB)}, c.remote->ranges());
    // [0, 100 KB) and [200 KB, 300 KB) are dead, [500 KB, 600 KB) is skipped by the other provider
    ASSERT_EQ(300 * KB, c.stream->skip_ranges_bytes());
    // [300 KB, 500 KB) and [600 KB, 1000 KB)
    ASSERT_EQ(600 * KB, c.stream->skip_ranges_deferred_bytes());
    ASSERT_FALSE(c.stream->find_shared_buffer(500 * KB, 100 * KB).ok());
    ASSERT_OK(c.stream->find_shared_buffer(300 * KB, 200 * KB).status());
    ASSERT_OK(c.stream->find_shared_buffer(600 * KB, 400 * KB).status());
    c.stream->set_skip_ranges_provider(nullptr);
}

} // namespace starrocks::parquet
