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

#include "io/shared_buffered_input_stream.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/config.h"
#include "io_test_base.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"

namespace starrocks::io {

class SharedBufferedInputStreamTest : public ::testing::Test {};

// A stream that records every read it serves, so a test sees which ranges the shared buffers load.
class RecordingInputStream : public io::SeekableInputStream {
public:
    explicit RecordingInputStream(std::string contents) : _contents(std::move(contents)) {}

    StatusOr<int64_t> read(void* data, int64_t count) override {
        count = std::min<int64_t>(count, _contents.size() - _offset);
        memcpy(data, &_contents[_offset], count);
        _offset += count;
        return count;
    }

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        if (offset < 0 || count < 0 || offset + count > static_cast<int64_t>(_contents.size())) {
            return Status::EndOfFile("read out of the file");
        }
        if (fail_next_reads > 0) {
            // The read leaves the bytes of `out` as they were, like a stream that fails before it fills them.
            fail_next_reads -= 1;
            failed_reads += 1;
            return Status::IOError("the remote read fails");
        }
        reads.emplace_back(offset, count);
        memcpy(out, _contents.data() + offset, count);
        return Status::OK();
    }

    Status seek(int64_t position) override {
        _offset = std::min<int64_t>(position, _contents.size());
        return Status::OK();
    }

    StatusOr<int64_t> position() override { return _offset; }

    StatusOr<int64_t> get_size() override { return _contents.size(); }

    // (offset, size) of each read that succeeded, in the order of the reads.
    std::vector<std::pair<int64_t, int64_t>> reads;
    // The next reads fail with an IO error and are not in `reads`.
    int64_t fail_next_reads = 0;
    int64_t failed_reads = 0;

private:
    std::string _contents;
    int64_t _offset{0};
};

using Reads = std::vector<std::pair<int64_t, int64_t>>;

// A file of 1 MB behind a recording stream, read through shared buffers aligned to 256 KB. The bytes are not
// periodic in the alignment, so a read from a wrong place never matches.
struct SplitCase {
    static constexpr int64_t KB = 1024;

    SplitCase() {
        data.resize(1024 * KB);
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = static_cast<char>(i % 251);
        }
        remote = std::make_shared<RecordingInputStream>(data);
        stream = std::make_shared<io::SharedBufferedInputStream>(remote, "split_case", data.size());
        stream->set_align_size(256 * KB);
    }

    // One buffer with the raw range [100 KB, 900 KB) and the aligned range [0, 1 MB).
    io::SharedBufferedInputStream::SharedBufferPtr register_buffer() {
        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        ranges.emplace_back(100 * KB, 800 * KB);
        CHECK(stream->set_io_ranges(ranges).ok());
        auto sb = stream->find_shared_buffer(100 * KB, 800 * KB);
        CHECK(sb.ok());
        return sb.value();
    }

    // The aligned ranges [0, 256 KB) and [512 KB, 768 KB). The bytes [256 KB, 512 KB) and [768 KB, 1 MB) are cut out.
    static std::vector<io::SharedBufferedInputStream::IORange> two_ranges() {
        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        ranges.emplace_back(0, 256 * KB);
        ranges.emplace_back(512 * KB, 256 * KB);
        return ranges;
    }

    void read_and_check(int64_t offset, int64_t size) {
        std::string buffer(size, 0);
        ASSERT_OK(stream->read_at_fully(offset, buffer.data(), size));
        ASSERT_EQ(0, memcmp(buffer.data(), data.data() + offset, size)) << "offset=" << offset << ", size=" << size;
    }

    std::string data;
    std::shared_ptr<RecordingInputStream> remote;
    std::shared_ptr<io::SharedBufferedInputStream> stream;
};

// A read that fails must not leave the buffer looking loaded, because the next read would serve the bytes that no one
// wrote. The buffer stays unloaded, the next read loads it again, and the counters count only the load that worked.
TEST_F(SharedBufferedInputStreamTest, test_failed_load_leaves_the_buffer_unloaded_and_the_next_read_retries) {
    constexpr int64_t KB = SplitCase::KB;
    SplitCase c;
    auto sb = c.register_buffer();
    ASSERT_EQ(0, sb->buffer.capacity());

    c.remote->fail_next_reads = 1;
    std::string out(10 * KB, 'x');
    ASSERT_FALSE(c.stream->read_at_fully(300 * KB, out.data(), out.size()).ok());
    ASSERT_EQ(1, c.remote->failed_reads);
    ASSERT_EQ(0, sb->buffer.capacity());
    ASSERT_EQ(0, c.stream->shared_io_count());
    ASSERT_EQ(0, c.stream->shared_io_bytes());
    // the buffer is still registered and unloaded, so a zero-copy peek does not serve it
    ASSERT_OK(c.stream->seek(300 * KB));
    ASSERT_TRUE(c.stream->peek(10 * KB).status().is_not_supported());

    // the same range again loads the buffer with a request of its own and returns the bytes of the file
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(300 * KB, 10 * KB));
    ASSERT_EQ((Reads{{0, 1024 * KB}}), c.remote->reads);
    ASSERT_LT(0, sb->buffer.capacity());
    ASSERT_EQ(1, c.stream->shared_io_count());
    ASSERT_EQ(1024 * KB, c.stream->shared_io_bytes());
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(880 * KB, 10 * KB));
    ASSERT_EQ(1u, c.remote->reads.size());
}

TEST_F(SharedBufferedInputStreamTest, test_split_shared_buffer_lookups) {
    constexpr int64_t KB = SplitCase::KB;
    SplitCase c;
    auto old_sb = c.register_buffer();
    ASSERT_EQ(0, old_sb->offset);
    ASSERT_EQ(1024 * KB, old_sb->size);
    ASSERT_FALSE(old_sb->split);

    ASSERT_OK(c.stream->split_shared_buffer(old_sb, SplitCase::two_ranges()));

    // The first buffer keeps the alignment padding in front of the raw range, and the raw range is the part of
    // the old raw range that it covers.
    auto first = c.stream->find_shared_buffer(50 * KB, 10 * KB);
    ASSERT_OK(first.status());
    ASSERT_NE(old_sb, first.value());
    ASSERT_TRUE(first.value()->split);
    ASSERT_EQ(100 * KB, first.value()->raw_offset);
    ASSERT_EQ(156 * KB, first.value()->raw_size);
    ASSERT_EQ(0, first.value()->offset);
    ASSERT_EQ(256 * KB, first.value()->size);
    ASSERT_EQ(old_sb->ref_count, first.value()->ref_count);
    ASSERT_EQ(0u, first.value()->buffer.capacity());
    ASSERT_OK(c.stream->find_shared_buffer(0, 256 * KB).status());

    auto second = c.stream->find_shared_buffer(512 * KB, 256 * KB);
    ASSERT_OK(second.status());
    ASSERT_NE(first.value(), second.value());
    ASSERT_TRUE(second.value()->split);
    ASSERT_EQ(512 * KB, second.value()->raw_offset);
    ASSERT_EQ(256 * KB, second.value()->raw_size);
    ASSERT_EQ(512 * KB, second.value()->offset);
    ASSERT_EQ(256 * KB, second.value()->size);

    // No buffer covers an offset in a cut out range, also when the read starts in a buffer and ends in the hole,
    // or starts in the hole and ends in a buffer.
    ASSERT_FALSE(c.stream->find_shared_buffer(256 * KB, 1).ok());
    ASSERT_FALSE(c.stream->find_shared_buffer(300 * KB, 10 * KB).ok());
    ASSERT_FALSE(c.stream->find_shared_buffer(256 * KB - 10, 20).ok());
    ASSERT_FALSE(c.stream->find_shared_buffer(512 * KB - 10, 20).ok());
    // The tail [768 KB, 1 MB) is inside the old raw range and in the old alignment padding.
    ASSERT_FALSE(c.stream->find_shared_buffer(768 * KB, 1).ok());
    ASSERT_FALSE(c.stream->find_shared_buffer(850 * KB, 10 * KB).ok());
    ASSERT_FALSE(c.stream->find_shared_buffer(950 * KB, 10 * KB).ok());
    ASSERT_FALSE(c.stream->find_shared_buffer(768 * KB - 10, 20).ok());

    // The old buffer is not registered any more.
    auto st = c.stream->split_shared_buffer(old_sb, SplitCase::two_ranges());
    ASSERT_TRUE(st.is_invalid_argument()) << st;
}

TEST_F(SharedBufferedInputStreamTest, test_split_shared_buffer_loads_each_buffer_with_one_request) {
    constexpr int64_t KB = SplitCase::KB;
    SplitCase c;
    ASSERT_OK(c.stream->split_shared_buffer(c.register_buffer(), SplitCase::two_ranges()));

    // A buffer that is not loaded cannot be peeked.
    ASSERT_OK(c.stream->seek(520 * KB));
    ASSERT_FALSE(c.stream->peek(100).ok());

    // A read in the second buffer loads only that buffer, with one request for all of its bytes.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(600 * KB, 100 * KB));
    ASSERT_EQ((Reads{{512 * KB, 256 * KB}}), c.remote->reads);
    ASSERT_EQ(1, c.stream->shared_io_count());
    ASSERT_EQ(256 * KB, c.stream->shared_io_bytes());
    ASSERT_EQ(0, c.stream->shared_align_io_bytes());
    ASSERT_EQ(0, c.stream->direct_io_count());

    ASSERT_OK(c.stream->seek(520 * KB));
    auto peeked = c.stream->peek(100);
    ASSERT_OK(peeked.status());
    ASSERT_EQ(0, memcmp(peeked.value().data(), c.data.data() + 520 * KB, 100));

    // The first buffer is loaded by one request, which includes the alignment padding.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(50 * KB, 10 * KB));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(200 * KB, 10 * KB));
    ASSERT_EQ((Reads{{512 * KB, 256 * KB}, {0, 256 * KB}}), c.remote->reads);
    ASSERT_EQ(2, c.stream->shared_io_count());
    ASSERT_EQ(100 * KB, c.stream->shared_align_io_bytes());
    ASSERT_EQ(0, c.stream->direct_io_count());

    // A read that spans both buffers and the hole between them, and a read in the hole, are direct reads.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(200 * KB, 400 * KB));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(300 * KB, 10 * KB));
    ASSERT_EQ((Reads{{512 * KB, 256 * KB}, {0, 256 * KB}, {200 * KB, 400 * KB}, {300 * KB, 10 * KB}}), c.remote->reads);
    ASSERT_EQ(2, c.stream->shared_io_count());
    ASSERT_EQ(2, c.stream->direct_io_count());
    ASSERT_EQ(410 * KB, c.stream->direct_io_bytes());

    // The loaded buffers serve their bytes again with no request.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, 256 * KB));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(512 * KB, 256 * KB));
    ASSERT_EQ(4u, c.remote->reads.size());
}

TEST_F(SharedBufferedInputStreamTest, test_split_shared_buffer_release_to_offset) {
    constexpr int64_t KB = SplitCase::KB;
    SplitCase c;
    ASSERT_OK(c.stream->split_shared_buffer(c.register_buffer(), SplitCase::two_ranges()));

    // The key of a buffer is the end of its raw range. The first buffer ends at 256 KB, and the second at 768 KB.
    c.stream->release_to_offset(255 * KB);
    ASSERT_OK(c.stream->find_shared_buffer(50 * KB, 10 * KB).status());
    c.stream->release_to_offset(256 * KB);
    ASSERT_FALSE(c.stream->find_shared_buffer(50 * KB, 10 * KB).ok());
    ASSERT_OK(c.stream->find_shared_buffer(600 * KB, 10 * KB).status());
    // A read of a released buffer is a direct read with the right bytes.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(50 * KB, 10 * KB));
    ASSERT_EQ(1, c.stream->direct_io_count());

    c.stream->release_to_offset(767 * KB);
    ASSERT_OK(c.stream->find_shared_buffer(600 * KB, 10 * KB).status());
    c.stream->release_to_offset(768 * KB);
    ASSERT_FALSE(c.stream->find_shared_buffer(600 * KB, 10 * KB).ok());
}

TEST_F(SharedBufferedInputStreamTest, test_split_shared_buffer_does_not_move_offsets_of_other_buffers) {
    constexpr int64_t KB = SplitCase::KB;
    SplitCase c;
    // Two raw ranges that are not joined. With the alignment, the first covers [0, 256 KB) and the second covers
    // [0, 512 KB), so the second overlaps the first.
    io::SharedBufferedInputStream::CoalesceOptions options;
    options.max_dist_size = 0;
    c.stream->set_coalesce_options(options);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 100 * KB);
    ranges.emplace_back(200 * KB, 100 * KB);
    ASSERT_OK(c.stream->set_io_ranges(ranges));
    auto first = c.stream->find_shared_buffer(10 * KB, 10 * KB);
    ASSERT_OK(first.status());
    ASSERT_EQ(0, first.value()->raw_offset);
    auto second = c.stream->find_shared_buffer(200 * KB, 100 * KB);
    ASSERT_OK(second.status());
    ASSERT_NE(first.value(), second.value());
    // The padding of the second buffer is read through it, where no other buffer takes the offset.
    ASSERT_EQ(second.value(), c.stream->find_shared_buffer(150 * KB, 10 * KB).value());

    // The second buffer drops its first 256 KB and keeps [256 KB, 512 KB).
    std::vector<io::SharedBufferedInputStream::IORange> keep;
    keep.emplace_back(256 * KB, 256 * KB);
    ASSERT_OK(c.stream->split_shared_buffer(second.value(), keep));

    // The first buffer is still found for its offsets, and the cut out padding of the second one is not found.
    ASSERT_EQ(first.value(), c.stream->find_shared_buffer(10 * KB, 10 * KB).value());
    ASSERT_FALSE(c.stream->find_shared_buffer(150 * KB, 10 * KB).ok());
    auto kept = c.stream->find_shared_buffer(260 * KB, 10 * KB);
    ASSERT_OK(kept.status());
    ASSERT_EQ(256 * KB, kept.value()->raw_offset);
    ASSERT_EQ(44 * KB, kept.value()->raw_size);
    ASSERT_EQ(256 * KB, kept.value()->offset);
    ASSERT_EQ(256 * KB, kept.value()->size);
}

TEST_F(SharedBufferedInputStreamTest, test_split_shared_buffer_rejects_bad_arguments) {
    constexpr int64_t KB = SplitCase::KB;
    SplitCase c;
    auto sb = c.register_buffer();
    using IORange = io::SharedBufferedInputStream::IORange;

    auto check_rejected = [&](const std::vector<IORange>& ranges) {
        auto st = c.stream->split_shared_buffer(sb, ranges);
        ASSERT_TRUE(st.is_invalid_argument()) << st;
        // Nothing changed.
        auto found = c.stream->find_shared_buffer(100 * KB, 800 * KB);
        ASSERT_OK(found.status());
        ASSERT_EQ(sb, found.value());
        ASSERT_EQ(0, sb->offset);
        ASSERT_EQ(1024 * KB, sb->size);
        ASSERT_FALSE(sb->split);
    };

    ASSERT_NO_FATAL_FAILURE(check_rejected({}));
    // A range that is empty, out of order, overlapping, or outside the aligned range of the buffer.
    ASSERT_NO_FATAL_FAILURE(check_rejected({IORange(0, 0)}));
    ASSERT_NO_FATAL_FAILURE(check_rejected({IORange(512 * KB, 128 * KB), IORange(0, 128 * KB)}));
    ASSERT_NO_FATAL_FAILURE(check_rejected({IORange(0, 300 * KB), IORange(256 * KB, 128 * KB)}));
    ASSERT_NO_FATAL_FAILURE(check_rejected({IORange(900 * KB, 200 * KB)}));
    // A range that lies in the alignment padding in front of the raw range has no part of the raw range.
    ASSERT_NO_FATAL_FAILURE(check_rejected({IORange(0, 50 * KB)}));

    // A buffer that is loaded cannot be split.
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(100 * KB, 10 * KB));
    ASSERT_TRUE(c.stream->split_shared_buffer(sb, SplitCase::two_ranges()).is_invalid_argument());
    ASSERT_EQ(1u, c.remote->reads.size());

    // A buffer that is released cannot be split.
    c.stream->release();
    ASSERT_TRUE(c.stream->split_shared_buffer(sb, SplitCase::two_ranges()).is_invalid_argument());
}

// Sets io_coalesce_split_large_range for the scope of a test and restores it at the end.
class SplitLargeRangeGuard {
public:
    explicit SplitLargeRangeGuard(bool value) : _old(config::io_coalesce_split_large_range) {
        config::io_coalesce_split_large_range = value;
    }
    ~SplitLargeRangeGuard() { config::io_coalesce_split_large_range = _old; }

private:
    bool _old;
};

PARALLEL_TEST(SharedBufferedInputStreamTest, test_release) {
    size_t len = 1 * 1024 * 1024; // 1MB
    const std::string rand_string = random_string(len);
    auto in = std::make_shared<TestInputStream>(rand_string, len);
    auto sb_stream = std::make_shared<io::SharedBufferedInputStream>(in, "test", len);
    sb_stream->set_align_size(256 * 1024); // 1024
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    // make two ranges one is active and another is lazy to avoid merging together.
    // 150k -> 520k
    auto r_active = io::SharedBufferedInputStream::IORange(150 * 1024, 370 * 1024, true);
    ranges.push_back(r_active);
    // 550k -> 650k
    auto r_lazy = io::SharedBufferedInputStream::IORange(550 * 1024, 100 * 1024, false);
    ranges.push_back(r_lazy);
    auto st = sb_stream->set_io_ranges(ranges, false);
    ASSERT_OK(st);
    // for this case, the first range is aligned to 0 -> 768k, the second range is aligned to 512k -> 768k
    // and now the first range is used and want to release
    // if release with aligned offset, both two sharedbuffers are released.
    sb_stream->release_to_offset(520 * 1024);
    auto sb = sb_stream->find_shared_buffer(550 * 1024, 100 * 1024);
    ASSERT_OK(sb.status());
}

TEST_F(SharedBufferedInputStreamTest, test_orc) {
    // the test expects one shared buffer for the large io range.
    SplitLargeRangeGuard guard(false);
    size_t len = 100 * 1024 * 1024; // 1MB
    const std::string rand_string = random_string(len);
    auto in = std::make_shared<TestInputStream>(rand_string, len);
    auto sb_stream = std::make_shared<io::SharedBufferedInputStream>(in, "test", len);
    sb_stream->set_align_size(256 * 1024); // 256kb
    std::vector<io::SharedBufferedInputStream::IORange> ranges;

    {
        // put lazy
        ranges.emplace_back(3, 1746 - 3, false);
        ranges.emplace_back(1978, 4125 - 1978, false);
        ranges.emplace_back(4288, 5235 - 4288, false);
        ranges.emplace_back(5523, 2833805 - 5523, false);
        ranges.emplace_back(2913460, 3261935 - 2913460, false);
        ranges.emplace_back(3295862, 22211037 - 3295862, false);
        ranges.emplace_back(22417540, 22417878 + 35 - 22417540, false);
    }

    {
        // put active
        ranges.emplace_back(1746, 1978 - 1746, true);
        ranges.emplace_back(4125, 4288 - 4125, true);
        ranges.emplace_back(5235, 5523 - 5235, true);
        ranges.emplace_back(2833805, 2913460 - 2833805, true);
        ranges.emplace_back(3261935, 3295862 - 3261935, true);
        ranges.emplace_back(22211037, 22417540 - 22211037, true);
    }

    auto st = sb_stream->set_io_ranges(ranges, false);
    ASSERT_TRUE(st.ok());

    // read active first
    auto sb = sb_stream->find_shared_buffer(1746, 1978 - 1746);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(1746, sb.value()->raw_offset);
    ASSERT_EQ(5523 - 1746, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(2833805, 2913460 - 2833805);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(2833805, sb.value()->raw_offset);
    ASSERT_EQ(3295862 - 2833805, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(22211037, 22417540 - 22211037);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(22211037, sb.value()->raw_offset);
    ASSERT_EQ(22417540 - 22211037, sb.value()->raw_size);

    // read lazy column
    sb = sb_stream->find_shared_buffer(3, 1746 - 3);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(1746, sb.value()->raw_offset);
    ASSERT_EQ(5523 - 1746, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(1978, 4125 - 1978);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(1746, sb.value()->raw_offset);
    ASSERT_EQ(5523 - 1746, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(4288, 5235 - 4288);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(1746, sb.value()->raw_offset);
    ASSERT_EQ(5523 - 1746, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(5523, 2833805 - 5523);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(5523, sb.value()->raw_offset);
    ASSERT_EQ(2833805 - 5523, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(2913460, 3261935 - 2913460);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(2833805, sb.value()->raw_offset);
    ASSERT_EQ(3295862 - 2833805, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(3295862, 22211037 - 3295862);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(3295862, sb.value()->raw_offset);
    ASSERT_EQ(22211037 - 3295862, sb.value()->raw_size);

    sb = sb_stream->find_shared_buffer(22417540, 22417878 + 35 - 22417540);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(22417540, sb.value()->raw_offset);
    ASSERT_EQ(22417878 + 35 - 22417540, sb.value()->raw_size);

    // clear previous stripe io range
    sb_stream->release_to_offset(22418414);

    ranges.clear();
    {
        // put active
        ranges.emplace_back(22420223, 22420420 - 22420223, true);
    }
    {
        // put lazy
        ranges.emplace_back(22418414, 22420223 - 22418414, false);
    }

    st = sb_stream->set_io_ranges(ranges, false);
    ASSERT_TRUE(st.ok());

    // get active
    sb = sb_stream->find_shared_buffer(22420223, 22420420 - 22420223);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(22420223, sb.value()->raw_offset);
    ASSERT_EQ(22420420 - 22420223, sb.value()->raw_size);

    // get lazy
    sb = sb_stream->find_shared_buffer(22418414, 22420223 - 22418414);
    ASSERT_TRUE(sb.ok());
    // std::cout << sb.value()->debug() << std::endl;
    ASSERT_EQ(22420223, sb.value()->raw_offset);
    ASSERT_EQ(22420420 - 22420223, sb.value()->raw_size);

    // check debug function
    ASSERT_EQ(
            "SharedBuffer raw_offset=22420223, raw_size=197, offset=22282240, size=262144, ref_count=2, "
            "buffer_capacity=0",
            sb.value()->debug_string());
}

// We want to see how many bytes we load for the buffers of ranges larger than max_buffer_size and how many
// of them the readers ask for, so the large counters must change only for reads served from such a buffer.
TEST_F(SharedBufferedInputStreamTest, test_large_buffer_counters) {
    // this test checks the counters of a large range that is one buffer, the pieces are tested below.
    SplitLargeRangeGuard guard(false);
    constexpr int64_t MB = 1024 * 1024;
    constexpr int64_t align_size = 256 * 1024;
    const int64_t len = 32 * MB;
    const std::string data = random_string(len);
    auto in = std::make_shared<TestInputStream>(data, len);
    auto sb_stream = std::make_shared<io::SharedBufferedInputStream>(in, "test", len);
    sb_stream->set_align_size(align_size);
    io::SharedBufferedInputStream::CoalesceOptions options;
    options.max_dist_size = 1 * MB;
    options.max_buffer_size = 4 * MB;
    sb_stream->set_coalesce_options(options);

    // a 10 MB range is larger than max_buffer_size, the two small ranges are far from each other and from it.
    const int64_t large_offset = 8 * MB + 100;
    const int64_t large_size = 10 * MB;
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(1000, 5000);
    ranges.emplace_back(large_offset, large_size);
    ranges.emplace_back(30 * MB, 2000);
    ASSERT_OK(sb_stream->set_io_ranges(ranges));

    ASSERT_EQ(0, sb_stream->large_io_count());
    ASSERT_EQ(0, sb_stream->large_io_bytes());
    ASSERT_EQ(0, sb_stream->large_used_bytes());

    auto large_sb = sb_stream->find_shared_buffer(large_offset, large_size);
    ASSERT_OK(large_sb.status());
    ASSERT_TRUE(large_sb.value()->large);
    // the range is aligned to [8 MB, 18 MB + 256 KB)
    const int64_t large_aligned_size = 10 * MB + align_size;
    ASSERT_EQ(8 * MB, large_sb.value()->offset);
    ASSERT_EQ(large_aligned_size, large_sb.value()->size);
    auto small_sb = sb_stream->find_shared_buffer(1000, 5000);
    ASSERT_OK(small_sb.status());
    ASSERT_FALSE(small_sb.value()->large);

    // read a few bytes of the large range: we load the whole buffer and use only the bytes we ask for.
    const uint8_t* buf = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buf, 12 * MB + 7, 100, nullptr));
    ASSERT_EQ(0, memcmp(buf, data.data() + 12 * MB + 7, 100));
    ASSERT_EQ(large_aligned_size, sb_stream->large_io_bytes());
    ASSERT_EQ(1, sb_stream->large_io_count());
    ASSERT_EQ(100, sb_stream->large_used_bytes());
    ASSERT_EQ(large_aligned_size, sb_stream->shared_io_bytes());

    // reads of small buffers do not change the large counters.
    std::string out(200, '\0');
    ASSERT_OK(sb_stream->read_at_fully(1500, out.data(), 200));
    ASSERT_EQ(0, memcmp(out.data(), data.data() + 1500, 200));
    ASSERT_OK(sb_stream->get_bytes(&buf, 30 * MB + 10, 50, nullptr));
    ASSERT_EQ(3, sb_stream->shared_io_count());
    ASSERT_EQ(large_aligned_size, sb_stream->large_io_bytes());
    ASSERT_EQ(1, sb_stream->large_io_count());
    ASSERT_EQ(100, sb_stream->large_used_bytes());

    // the buffer is loaded already, a second read from it only adds to the used bytes.
    ASSERT_OK(sb_stream->read_at_fully(15 * MB, out.data(), 200));
    ASSERT_EQ(0, memcmp(out.data(), data.data() + 15 * MB, 200));
    ASSERT_EQ(large_aligned_size, sb_stream->large_io_bytes());
    ASSERT_EQ(1, sb_stream->large_io_count());
    ASSERT_EQ(300, sb_stream->large_used_bytes());

    // peek reads from the loaded buffer, with and without get_bytes.
    ASSERT_OK(sb_stream->seek(16 * MB));
    ASSERT_OK(sb_stream->peek(30).status());
    ASSERT_EQ(330, sb_stream->large_used_bytes());
    ASSERT_OK(sb_stream->peek_shared_buffer(20, nullptr).status());
    ASSERT_EQ(350, sb_stream->large_used_bytes());
    ASSERT_EQ(large_aligned_size, sb_stream->large_io_bytes());
    ASSERT_EQ(1, sb_stream->large_io_count());
}

// The buffers that split_shared_buffer makes from a large buffer load a part of it, so their bytes must be counted
// as the bytes of a large buffer.
TEST_F(SharedBufferedInputStreamTest, test_split_shared_buffer_keeps_large) {
    // this test splits the buffer of a large range that is one buffer, the pieces are tested below.
    SplitLargeRangeGuard guard(false);
    constexpr int64_t KB = SplitCase::KB;
    SplitCase c;
    io::SharedBufferedInputStream::CoalesceOptions options;
    options.max_buffer_size = 512 * KB;
    c.stream->set_coalesce_options(options);

    // the range of 800 KB is larger than max_buffer_size.
    auto sb = c.register_buffer();
    ASSERT_TRUE(sb->large);
    ASSERT_OK(c.stream->split_shared_buffer(sb, SplitCase::two_ranges()));
    auto first = c.stream->find_shared_buffer(50 * KB, 10 * KB);
    ASSERT_OK(first.status());
    ASSERT_TRUE(first.value()->large);
    auto second = c.stream->find_shared_buffer(600 * KB, 10 * KB);
    ASSERT_OK(second.status());
    ASSERT_TRUE(second.value()->large);

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(600 * KB, 100 * KB));
    ASSERT_EQ(1, c.stream->large_io_count());
    ASSERT_EQ(256 * KB, c.stream->large_io_bytes());
    ASSERT_EQ(100 * KB, c.stream->large_used_bytes());
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(50 * KB, 10 * KB));
    ASSERT_EQ(2, c.stream->large_io_count());
    ASSERT_EQ(512 * KB, c.stream->large_io_bytes());
    ASSERT_EQ(110 * KB, c.stream->large_used_bytes());
    ASSERT_EQ(c.stream->shared_io_bytes(), c.stream->large_io_bytes());
}

constexpr int64_t kMB = 1024 * 1024;

// A stream over random data with align size 256 KB and max_buffer_size 4 MB.
class SplitTestStream {
public:
    explicit SplitTestStream(int64_t len) : data(random_string(len)) {
        auto in = std::make_shared<TestInputStream>(data, len);
        sb_stream = std::make_shared<io::SharedBufferedInputStream>(in, "test", len);
        sb_stream->set_align_size(256 * 1024);
        io::SharedBufferedInputStream::CoalesceOptions options;
        options.max_dist_size = 1 * kMB;
        options.max_buffer_size = 4 * kMB;
        sb_stream->set_coalesce_options(options);
    }

    void read_and_check(int64_t offset, int64_t count) {
        std::string out(count, '\0');
        ASSERT_OK(sb_stream->read_at_fully(offset, out.data(), count));
        ASSERT_EQ(0, memcmp(out.data(), data.data() + offset, count));
    }

    std::string data;
    std::shared_ptr<io::SharedBufferedInputStream> sb_stream;
};

// We want a reader that touches only a few pages of a large range to load only the pieces it touches.
TEST_F(SharedBufferedInputStreamTest, test_split_large_range) {
    SplitLargeRangeGuard guard(true);
    SplitTestStream s(48 * kMB);
    auto& sb_stream = s.sb_stream;

    // a 20 MB range is cut into five pieces of 4 MB, each small range stays one buffer.
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(1000, 5000);
    ranges.emplace_back(4 * kMB, 20 * kMB);
    ranges.emplace_back(40 * kMB, 2000);
    ASSERT_OK(sb_stream->set_io_ranges(ranges));
    ASSERT_EQ(7, sb_stream->current_range_ref_sum());

    for (int i = 0; i < 5; i++) {
        auto piece = sb_stream->find_shared_buffer(4 * kMB + i * 4 * kMB, 4 * kMB);
        ASSERT_OK(piece.status());
        ASSERT_TRUE(piece.value()->large);
        ASSERT_EQ(4 * kMB + i * 4 * kMB, piece.value()->raw_offset);
        ASSERT_EQ(4 * kMB, piece.value()->raw_size);
        ASSERT_EQ(4 * kMB + i * 4 * kMB, piece.value()->offset);
        ASSERT_EQ(4 * kMB, piece.value()->size);
        ASSERT_EQ(i < 4, piece.value()->has_next_piece);
    }
    auto small_1 = sb_stream->find_shared_buffer(1000, 5000);
    ASSERT_OK(small_1.status());
    ASSERT_FALSE(small_1.value()->large);
    ASSERT_FALSE(small_1.value()->has_next_piece);
    ASSERT_EQ(5000, small_1.value()->raw_size);
    auto small_2 = sb_stream->find_shared_buffer(40 * kMB, 2000);
    ASSERT_OK(small_2.status());
    ASSERT_FALSE(small_2.value()->large);
    ASSERT_FALSE(small_2.value()->has_next_piece);
    ASSERT_EQ(2000, small_2.value()->raw_size);

    // no buffer covers the whole range, but the consecutive pieces do. A range that leaves the io range is
    // not covered.
    ASSERT_FALSE(sb_stream->find_shared_buffer(4 * kMB, 20 * kMB).ok());
    std::vector<io::SharedBufferedInputStream::SharedBufferPtr> pieces;
    ASSERT_OK(sb_stream->find_shared_pieces(4 * kMB, 20 * kMB, &pieces));
    ASSERT_EQ(5, pieces.size());
    ASSERT_OK(sb_stream->find_shared_pieces(5 * kMB, 13 * kMB, nullptr));
    ASSERT_FALSE(sb_stream->find_shared_pieces(2 * kMB, 4 * kMB, nullptr).ok());
    ASSERT_FALSE(sb_stream->find_shared_pieces(20 * kMB, 6 * kMB, nullptr).ok());

    // a page in the middle of the range loads only the piece [12 MB, 16 MB).
    s.read_and_check(13 * kMB + 5, kMB);
    ASSERT_EQ(4 * kMB, sb_stream->large_io_bytes());
    ASSERT_EQ(1, sb_stream->large_io_count());
    ASSERT_EQ(kMB, sb_stream->large_used_bytes());
    ASSERT_EQ(4 * kMB, sb_stream->shared_io_bytes());
    ASSERT_EQ(0, sb_stream->direct_io_count());

    // a page that crosses the border at 16 MB loads the next piece and returns the right bytes.
    s.read_and_check(16 * kMB - 512 * 1024, kMB);
    ASSERT_EQ(8 * kMB, sb_stream->large_io_bytes());
    ASSERT_EQ(2, sb_stream->large_io_count());
    ASSERT_EQ(2 * kMB, sb_stream->large_used_bytes());
    ASSERT_EQ(0, sb_stream->direct_io_count());

    // peek cannot return a range that crosses a border, inside a loaded piece it works.
    ASSERT_OK(sb_stream->seek(16 * kMB - 100));
    ASSERT_FALSE(sb_stream->peek(200).ok());
    ASSERT_OK(sb_stream->seek(14 * kMB));
    auto peeked = sb_stream->peek(100);
    ASSERT_OK(peeked.status());
    ASSERT_EQ(0, memcmp(peeked.value().data(), s.data.data() + 14 * kMB, 100));
    ASSERT_EQ(2 * kMB + 100, sb_stream->large_used_bytes());

    // a piece is released when the reader is behind its end, the other pieces stay.
    sb_stream->release_to_offset(14 * kMB);
    ASSERT_FALSE(sb_stream->find_shared_buffer(5 * kMB, 100).ok());
    ASSERT_FALSE(sb_stream->find_shared_buffer(9 * kMB, 100).ok());
    ASSERT_OK(sb_stream->find_shared_buffer(13 * kMB, 100).status());
    ASSERT_EQ(4, sb_stream->current_range_ref_sum());
}

// We expect every byte of a range to be loaded once when the readers go over the whole range, whatever the
// size of a read is.
TEST_F(SharedBufferedInputStreamTest, test_split_large_range_sequential) {
    SplitLargeRangeGuard guard(true);
    SplitTestStream s(48 * kMB);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(4 * kMB, 20 * kMB);
    ASSERT_OK(s.sb_stream->set_io_ranges(ranges));

    const int64_t step = kMB + 12345;
    for (int64_t offset = 4 * kMB; offset < 24 * kMB; offset += step) {
        s.read_and_check(offset, std::min(step, 24 * kMB - offset));
    }
    ASSERT_EQ(5, s.sb_stream->large_io_count());
    ASSERT_EQ(20 * kMB, s.sb_stream->large_io_bytes());
    ASSERT_EQ(20 * kMB, s.sb_stream->large_used_bytes());
    ASSERT_EQ(20 * kMB, s.sb_stream->shared_io_bytes());
    ASSERT_EQ(0, s.sb_stream->shared_align_io_bytes());
    ASSERT_EQ(0, s.sb_stream->direct_io_count());
}

// We cut at multiples of the align size, so the aligned pieces do not overlap and the sum of their sizes is
// the size of the aligned range, also at the end of the file.
TEST_F(SharedBufferedInputStreamTest, test_split_large_range_unaligned) {
    SplitLargeRangeGuard guard(true);
    const int64_t len = 14 * kMB + 150;
    SplitTestStream s(len);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(4 * kMB + 100, 10 * kMB);
    ASSERT_OK(s.sb_stream->set_io_ranges(ranges));
    ASSERT_EQ(3, s.sb_stream->current_range_ref_sum());

    auto piece_1 = s.sb_stream->find_shared_buffer(4 * kMB + 100, 100);
    ASSERT_OK(piece_1.status());
    ASSERT_EQ(4 * kMB + 100, piece_1.value()->raw_offset);
    ASSERT_EQ(4 * kMB - 100, piece_1.value()->raw_size);
    ASSERT_EQ(4 * kMB, piece_1.value()->offset);
    ASSERT_EQ(4 * kMB, piece_1.value()->size);
    auto piece_2 = s.sb_stream->find_shared_buffer(9 * kMB, 100);
    ASSERT_OK(piece_2.status());
    ASSERT_EQ(8 * kMB, piece_2.value()->raw_offset);
    ASSERT_EQ(4 * kMB, piece_2.value()->raw_size);
    ASSERT_EQ(8 * kMB, piece_2.value()->offset);
    ASSERT_EQ(4 * kMB, piece_2.value()->size);
    auto piece_3 = s.sb_stream->find_shared_buffer(14 * kMB, 100);
    ASSERT_OK(piece_3.status());
    ASSERT_EQ(12 * kMB, piece_3.value()->raw_offset);
    ASSERT_EQ(2 * kMB + 100, piece_3.value()->raw_size);
    ASSERT_EQ(12 * kMB, piece_3.value()->offset);
    ASSERT_EQ(2 * kMB + 150, piece_3.value()->size);
    ASSERT_FALSE(piece_3.value()->has_next_piece);

    for (int64_t offset = 4 * kMB + 100; offset < 14 * kMB + 100; offset += kMB) {
        s.read_and_check(offset, kMB);
    }
    ASSERT_EQ(3, s.sb_stream->large_io_count());
    ASSERT_EQ(10 * kMB + 150, s.sb_stream->large_io_bytes());
    ASSERT_EQ(10 * kMB, s.sb_stream->large_used_bytes());
    ASSERT_EQ(150, s.sb_stream->shared_align_io_bytes());
    ASSERT_EQ(0, s.sb_stream->direct_io_count());
}

// With the config off a large range is one buffer.
TEST_F(SharedBufferedInputStreamTest, test_split_large_range_disabled) {
    SplitLargeRangeGuard guard(false);
    SplitTestStream s(48 * kMB);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(4 * kMB, 20 * kMB);
    ASSERT_OK(s.sb_stream->set_io_ranges(ranges));
    ASSERT_EQ(1, s.sb_stream->current_range_ref_sum());

    auto sb = s.sb_stream->find_shared_buffer(4 * kMB, 20 * kMB);
    ASSERT_OK(sb.status());
    ASSERT_TRUE(sb.value()->large);
    ASSERT_FALSE(sb.value()->has_next_piece);

    s.read_and_check(13 * kMB + 5, 100);
    ASSERT_EQ(20 * kMB, s.sb_stream->large_io_bytes());
    ASSERT_EQ(1, s.sb_stream->large_io_count());
    ASSERT_EQ(100, s.sb_stream->large_used_bytes());
}

// A read that crosses the border of two different io ranges is not served from the buffers, also when both
// ranges are cut into pieces.
TEST_F(SharedBufferedInputStreamTest, test_split_large_range_adjacent_ranges) {
    SplitLargeRangeGuard guard(true);
    SplitTestStream s(48 * kMB);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(4 * kMB, 8 * kMB);
    ranges.emplace_back(12 * kMB, 8 * kMB);
    ASSERT_OK(s.sb_stream->set_io_ranges(ranges));
    ASSERT_EQ(4, s.sb_stream->current_range_ref_sum());

    s.read_and_check(12 * kMB - 100, 200);
    ASSERT_EQ(1, s.sb_stream->direct_io_count());
    ASSERT_EQ(200, s.sb_stream->direct_io_bytes());
    ASSERT_EQ(0, s.sb_stream->large_io_count());
    ASSERT_EQ(0, s.sb_stream->shared_io_count());
}

// The active and lazy columns path cuts a large range into pieces too.
TEST_F(SharedBufferedInputStreamTest, test_split_large_range_active_and_lazy) {
    SplitLargeRangeGuard guard(true);
    SplitTestStream s(48 * kMB);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(1000, 5000, false);
    ranges.emplace_back(4 * kMB, 20 * kMB, true);
    ranges.emplace_back(40 * kMB, 2000, true);
    ASSERT_OK(s.sb_stream->set_io_ranges(ranges, false));
    ASSERT_EQ(7, s.sb_stream->current_range_ref_sum());

    auto lazy = s.sb_stream->find_shared_buffer(1000, 5000);
    ASSERT_OK(lazy.status());
    ASSERT_FALSE(lazy.value()->large);
    ASSERT_OK(s.sb_stream->find_shared_pieces(4 * kMB, 20 * kMB, nullptr));

    s.read_and_check(13 * kMB + 5, kMB);
    ASSERT_EQ(4 * kMB, s.sb_stream->large_io_bytes());
    ASSERT_EQ(1, s.sb_stream->large_io_count());
}

// split_shared_buffer cuts a piece further when the data cache holds a part of it. The new buffers keep the large
// flag, and a read crosses from one buffer to the next only where no hole is between them, because the bytes of
// the hole are read from the cache.
TEST_F(SharedBufferedInputStreamTest, test_split_shared_buffer_of_a_piece) {
    SplitLargeRangeGuard guard(true);
    SplitTestStream s(48 * kMB);
    auto& sb_stream = s.sb_stream;
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(4 * kMB, 20 * kMB);
    ASSERT_OK(sb_stream->set_io_ranges(ranges));

    // cut the bytes [12.5 MB, 13 MB) out of the piece [12 MB, 16 MB).
    auto piece = sb_stream->find_shared_buffer(13 * kMB, 100);
    ASSERT_OK(piece.status());
    ASSERT_TRUE(piece.value()->has_next_piece);
    std::vector<io::SharedBufferedInputStream::IORange> keep;
    keep.emplace_back(12 * kMB, 512 * 1024);
    keep.emplace_back(13 * kMB, 3 * kMB);
    ASSERT_OK(sb_stream->split_shared_buffer(piece.value(), keep));

    auto before = sb_stream->find_shared_buffer(10 * kMB, 100);
    ASSERT_OK(before.status());
    ASSERT_FALSE(before.value()->split);
    ASSERT_TRUE(before.value()->has_next_piece);
    // a hole follows the head, so the head has no next piece.
    auto head = sb_stream->find_shared_buffer(12 * kMB, 512 * 1024);
    ASSERT_OK(head.status());
    ASSERT_TRUE(head.value()->large);
    ASSERT_TRUE(head.value()->split);
    ASSERT_FALSE(head.value()->has_next_piece);
    ASSERT_EQ(12 * kMB, head.value()->raw_offset);
    ASSERT_EQ(512 * 1024, head.value()->raw_size);
    // the tail reaches the end of the piece, and the piece had a next piece.
    auto tail = sb_stream->find_shared_buffer(13 * kMB, 3 * kMB);
    ASSERT_OK(tail.status());
    ASSERT_TRUE(tail.value()->large);
    ASSERT_TRUE(tail.value()->split);
    ASSERT_TRUE(tail.value()->has_next_piece);
    ASSERT_EQ(13 * kMB, tail.value()->raw_offset);
    ASSERT_EQ(3 * kMB, tail.value()->raw_size);

    ASSERT_OK(sb_stream->find_shared_pieces(12 * kMB - 100, 200, nullptr));
    ASSERT_OK(sb_stream->find_shared_pieces(16 * kMB - 100, 200, nullptr));
    ASSERT_FALSE(sb_stream->find_shared_pieces(12 * kMB + 512 * 1024 - 100, 200, nullptr).ok());
    ASSERT_FALSE(sb_stream->find_shared_pieces(13 * kMB - 100, 200, nullptr).ok());
    ASSERT_FALSE(sb_stream->find_shared_pieces(12 * kMB + 512 * 1024, 1, nullptr).ok());

    // a read from the piece before into the head is served from the buffers.
    s.read_and_check(12 * kMB - 100, 200);
    ASSERT_EQ(0, sb_stream->direct_io_count());
    ASSERT_EQ(2, sb_stream->large_io_count());
    ASSERT_EQ(4 * kMB + 512 * 1024, sb_stream->large_io_bytes());
    ASSERT_EQ(200, sb_stream->large_used_bytes());

    // a read that reaches the hole is a direct read, and it does not load a buffer.
    s.read_and_check(13 * kMB - 100, 200);
    ASSERT_EQ(1, sb_stream->direct_io_count());
    ASSERT_EQ(200, sb_stream->direct_io_bytes());
    ASSERT_EQ(2, sb_stream->large_io_count());

    // a read from the tail into the next piece is served from the buffers, and the tail is loaded without the hole.
    s.read_and_check(16 * kMB - 100, 200);
    ASSERT_EQ(1, sb_stream->direct_io_count());
    ASSERT_EQ(4, sb_stream->large_io_count());
    ASSERT_EQ(4 * kMB + 512 * 1024 + 3 * kMB + 4 * kMB, sb_stream->large_io_bytes());
    ASSERT_EQ(400, sb_stream->large_used_bytes());
}

namespace {

// Names the ranges that the test sets, as the Parquet reader does for the pages that its page cache holds. It records
// every ask, and it can run a callback inside the ask, to read through the stream like a reader that loads an index.
class FixedSkipRanges final : public io::SharedBufferedInputStream::SkipRangesProvider {
public:
    bool enabled() const override { return is_enabled; }

    void find_skip_ranges(int64_t offset, int64_t size,
                          std::vector<io::SharedBufferedInputStream::IORange>* ranges) override {
        asks.emplace_back(offset, size);
        if (on_ask) {
            on_ask();
        }
        ranges->insert(ranges->end(), skip.begin(), skip.end());
    }

    void find_defer_ranges(int64_t offset, int64_t size,
                           std::vector<io::SharedBufferedInputStream::IORange>* ranges) override {
        ranges->insert(ranges->end(), defer.begin(), defer.end());
    }

    std::vector<io::SharedBufferedInputStream::IORange> skip;
    // The bytes that the provider defers.
    std::vector<io::SharedBufferedInputStream::IORange> defer;
    // (offset, size) of each ask, in the order of the asks.
    Reads asks;
    std::function<void()> on_ask;
    bool is_enabled = true;
};

enum PageName { kA, kB, kC, kD, kE, kF };

// A file of 2 MB behind a recording stream. The chunk of a column takes the first 600 KB of it: six pages A to F of
// 100 KB each, so the page p takes [p * 100 KB, (p + 1) * 100 KB). The bytes are random, so a read from a wrong place
// never matches.
struct PageCacheCase {
    static constexpr int64_t KB = 1024;
    static constexpr int64_t kPage = 100 * KB;

    explicit PageCacheCase(int64_t align_size = 0, int64_t distance = 0, int64_t file_size = 2048 * KB)
            : data(random_string(file_size)),
              remote(std::make_shared<RecordingInputStream>(data)),
              stream(std::make_shared<io::SharedBufferedInputStream>(remote, "page_cache_case", data.size())),
              saved_distance(config::io_coalesce_read_max_distance_size),
              saved_split_large_range(config::io_coalesce_split_large_range) {
        stream->set_align_size(align_size);
        // The io ranges of a test are not joined by the stream, so each of them is a buffer of its own.
        io::SharedBufferedInputStream::CoalesceOptions options;
        options.max_dist_size = 0;
        stream->set_coalesce_options(options);
        config::io_coalesce_read_max_distance_size = static_cast<int32_t>(distance);
        // The tests of the pieces turn the pieces on.
        config::io_coalesce_split_large_range = false;
        stream->set_skip_ranges_provider(&provider);
    }

    ~PageCacheCase() {
        config::io_coalesce_read_max_distance_size = saved_distance;
        config::io_coalesce_split_large_range = saved_split_large_range;
    }

    // The provider names these pages, as the page cache holds them.
    void name_pages(std::initializer_list<PageName> pages) {
        for (PageName page : pages) {
            provider.skip.emplace_back(page * kPage, kPage);
        }
    }

    // The provider defers these pages, as a reader that does not need them now but may need them later.
    void defer_pages(std::initializer_list<PageName> pages) {
        for (PageName page : pages) {
            provider.defer.emplace_back(page * kPage, kPage);
        }
    }

    // The io range [0, 600 KB) that the chunk takes.
    void set_chunk_range() { set_ranges({io::SharedBufferedInputStream::IORange(0, 6 * kPage)}); }

    void set_ranges(const std::vector<io::SharedBufferedInputStream::IORange>& ranges) {
        ASSERT_OK(stream->set_io_ranges(ranges));
    }

    void read_and_check(int64_t offset, int64_t size) {
        std::string buffer(size, 0);
        ASSERT_OK(stream->read_at_fully(offset, buffer.data(), size));
        ASSERT_EQ(0, memcmp(buffer.data(), data.data() + offset, size)) << "offset=" << offset << ", size=" << size;
    }

    void read_page_and_check(PageName page) { read_and_check(page * kPage, kPage); }

    // True if one shared buffer covers the bytes.
    bool covered(int64_t offset, int64_t size) { return stream->find_shared_buffer(offset, size).ok(); }
    bool page_covered(PageName page) { return covered(page * kPage, kPage); }

    std::string data;
    std::shared_ptr<RecordingInputStream> remote;
    std::shared_ptr<io::SharedBufferedInputStream> stream;
    int32_t saved_distance;
    bool saved_split_large_range;
    FixedSkipRanges provider;
};

} // namespace

// A column chunk of six pages A to F is one shared buffer, and the page cache holds B, C and E. The buffer loads the
// runs A, D and F, each with one request, and not the cached pages.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_load_only_the_runs_that_the_provider_does_not_name) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_chunk_range();
    c.name_pages({kB, kC, kE});
    auto old_sb = c.stream->find_shared_buffer(0, 600 * KB).value();
    ASSERT_FALSE(old_sb->planned);

    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_EQ((Reads{{0, 100 * KB}}), c.remote->reads);
    ASSERT_EQ((Reads{{0, 600 * KB}}), c.provider.asks);
    ASSERT_EQ(300 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ(300 * KB, c.stream->skip_ranges_named_bytes());
    ASSERT_EQ(1, c.stream->skip_ranges_split_count());

    // The runs D and F are loaded when a read needs them, each with one request. The provider is asked about each of them
    // when a read first takes a byte from it, and it names nothing there.
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kD));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kF));
    ASSERT_EQ((Reads{{0, 100 * KB}, {300 * KB, 100 * KB}, {500 * KB, 100 * KB}}), c.remote->reads);
    ASSERT_EQ((Reads{{0, 600 * KB}, {300 * KB, 100 * KB}, {500 * KB, 100 * KB}}), c.provider.asks);
    ASSERT_EQ(3, c.stream->shared_io_count());
    ASSERT_EQ(300 * KB, c.stream->shared_io_bytes());
    ASSERT_EQ(0, c.stream->shared_align_io_bytes());
    ASSERT_EQ(0, c.stream->direct_io_count());
    ASSERT_EQ(300 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ(1, c.stream->skip_ranges_split_count());

    // No buffer covers the cached pages, nor a read that reaches into them. The old buffer is not registered any more.
    ASSERT_TRUE(c.page_covered(kA));
    ASSERT_TRUE(c.page_covered(kD));
    ASSERT_TRUE(c.page_covered(kF));
    ASSERT_FALSE(c.page_covered(kB));
    ASSERT_FALSE(c.page_covered(kC));
    ASSERT_FALSE(c.page_covered(kE));
    ASSERT_FALSE(c.covered(100 * KB - 10, 20));
    ASSERT_FALSE(c.covered(300 * KB - 10, 20));
    ASSERT_FALSE(c.covered(0, 600 * KB));
    ASSERT_TRUE(c.stream->split_shared_buffer(old_sb, {io::SharedBufferedInputStream::IORange(0, 100 * KB)})
                        .is_invalid_argument());

    // The data cache has not chosen the bytes of the new buffers, so it is still free to cut them.
    for (PageName page : {kA, kD, kF}) {
        auto sb = c.stream->find_shared_buffer(page * PageCacheCase::kPage, PageCacheCase::kPage);
        ASSERT_OK(sb.status());
        ASSERT_FALSE(sb.value()->split);
        ASSERT_TRUE(sb.value()->planned);
    }

    // The loaded buffers serve their bytes again with no request.
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kD));
    ASSERT_EQ(3u, c.remote->reads.size());
}

// The page cache can evict a page after it said that it holds the page. No buffer covers the page, and it lies inside a
// cut span, so the read of it loads the rest of the span with one request and the bytes are right. A read that spans
// several buffers and the pages between them is a direct read.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_page_read_after_its_eviction_loads_its_cut_span) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_chunk_range();
    c.name_pages({kB, kC, kE});

    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_EQ(1u, c.remote->reads.size());
    ASSERT_EQ(0, c.stream->skip_ranges_recover_count());

    // the page C is read from remote storage with one request, and a buffer of its span is loaded
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kC));
    ASSERT_EQ((Reads{{0, 100 * KB}, {200 * KB, 100 * KB}}), c.remote->reads);
    ASSERT_EQ(0, c.stream->direct_io_count());
    ASSERT_EQ(1, c.stream->skip_ranges_recover_count());
    ASSERT_EQ(100 * KB, c.stream->skip_ranges_recover_bytes());
    ASSERT_EQ(2, c.stream->shared_io_count());
    ASSERT_TRUE(c.page_covered(kC));
    // the page B is in the span before the read, and nothing loaded it
    ASSERT_FALSE(c.page_covered(kB));

    // a read from A over B into C crosses a buffer and a cut page, so it is a direct read
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(50 * KB, 200 * KB));
    ASSERT_EQ(1, c.stream->direct_io_count());
    ASSERT_EQ(200 * KB, c.stream->direct_io_bytes());
    ASSERT_EQ(2, c.stream->shared_io_count());

    // the page B is a span of its own now, and the page E too
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kB));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kE));
    ASSERT_EQ(3, c.stream->skip_ranges_recover_count());
    ASSERT_EQ(5u, c.remote->reads.size());

    // the run D is not touched, and it is loaded once
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(310 * KB, 10 * KB));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(380 * KB, 10 * KB));
    ASSERT_EQ(5, c.stream->shared_io_count());
}

// The pages B, C and D are cached, and the cache evicts all of them after the plan. The first read of B loads the whole
// span with one request, and the pages C and D are served from it.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_evicted_pages_of_one_span_cost_one_request) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_chunk_range();
    c.name_pages({kB, kC, kD});

    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kB));
    ASSERT_EQ((Reads{{0, 100 * KB}, {100 * KB, 300 * KB}}), c.remote->reads);
    ASSERT_EQ(1, c.stream->skip_ranges_recover_count());
    ASSERT_EQ(300 * KB, c.stream->skip_ranges_recover_bytes());

    // header and body reads of every page of the span, as the page reader makes them
    for (PageName page : {kB, kC, kD}) {
        ASSERT_NO_FATAL_FAILURE(c.read_and_check(page * PageCacheCase::kPage, 20));
        ASSERT_NO_FATAL_FAILURE(c.read_and_check(page * PageCacheCase::kPage + 20, PageCacheCase::kPage - 20));
    }
    ASSERT_EQ(2u, c.remote->reads.size());
    ASSERT_EQ(0, c.stream->direct_io_count());
    ASSERT_EQ(1, c.stream->skip_ranges_recover_count());

    // the run E and F is loaded by its own request
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kF));
    ASSERT_EQ(3u, c.remote->reads.size());
    ASSERT_EQ(1, c.stream->skip_ranges_recover_count());
}

// The load of a span is not larger than max_buffer_size, and the rest of the span stays a span, so the next read of it
// loads the next part.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_recovery_stays_within_max_buffer_size) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    io::SharedBufferedInputStream::CoalesceOptions options;
    options.max_dist_size = 0;
    options.max_buffer_size = 200 * KB;
    c.stream->set_coalesce_options(options);
    c.set_chunk_range();
    c.name_pages({kB, kC, kD, kE});

    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kB));
    ASSERT_EQ((Reads{{0, 100 * KB}, {100 * KB, 200 * KB}}), c.remote->reads);
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kC));
    ASSERT_EQ(2u, c.remote->reads.size());
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kD));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kE));
    ASSERT_EQ((Reads{{0, 100 * KB}, {100 * KB, 200 * KB}, {300 * KB, 200 * KB}}), c.remote->reads);
    ASSERT_EQ(2, c.stream->skip_ranges_recover_count());
    ASSERT_EQ(400 * KB, c.stream->skip_ranges_recover_bytes());
    ASSERT_EQ(0, c.stream->direct_io_count());
}

// A read that is not inside one cut span is a direct read: it starts in a buffer, it ends in a buffer, or it lies in
// bytes that no plan cut out. A buffer without a plan cuts nothing.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_recovery_only_for_reads_inside_a_cut_span) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_chunk_range();
    c.name_pages({kB, kC});

    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    // a read that ends in the run D
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(250 * KB, 100 * KB));
    ASSERT_EQ(1, c.stream->direct_io_count());
    // a read that begins in the run A
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(90 * KB, 50 * KB));
    ASSERT_EQ(2, c.stream->direct_io_count());
    // bytes outside every io range
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(1500 * KB, 10 * KB));
    ASSERT_EQ(3, c.stream->direct_io_count());
    ASSERT_EQ(0, c.stream->skip_ranges_recover_count());
    // the read inside the span is recovered
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(120 * KB, 10 * KB));
    ASSERT_EQ(1, c.stream->skip_ranges_recover_count());
    ASSERT_EQ(3, c.stream->direct_io_count());
}

// With an align size the load of a span ends at a border of a block, unless the read ends later, so the data cache can
// save the blocks of the loaded buffer.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_recovery_ends_at_a_block_border) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c(64 * KB);
    io::SharedBufferedInputStream::CoalesceOptions options;
    options.max_dist_size = 0;
    options.max_buffer_size = 300 * KB;
    c.stream->set_coalesce_options(options);
    c.set_ranges({io::SharedBufferedInputStream::IORange(0, 1536 * KB)});
    c.provider.skip.emplace_back(64 * KB, 1024 * KB);

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, 4 * KB));
    ASSERT_EQ((Reads{{0, 64 * KB}}), c.remote->reads);

    // 128 KB + 300 KB is 428 KB, and the load ends at 384 KB
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(128 * KB, 10 * KB));
    ASSERT_EQ((Reads{{0, 64 * KB}, {128 * KB, 256 * KB}}), c.remote->reads);
    ASSERT_EQ(256 * KB, c.stream->skip_ranges_recover_bytes());
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(300 * KB, 50 * KB));
    ASSERT_EQ(2u, c.remote->reads.size());

    // 400 KB + 300 KB is 700 KB, and the load ends at 640 KB
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(400 * KB, 10 * KB));
    ASSERT_EQ((Reads{{0, 64 * KB}, {128 * KB, 256 * KB}, {400 * KB, 240 * KB}}), c.remote->reads);

    // a read that ends after the border of the block is loaded whole
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(700 * KB, 330 * KB));
    ASSERT_EQ((Reads{{0, 64 * KB}, {128 * KB, 256 * KB}, {400 * KB, 240 * KB}, {700 * KB, 330 * KB}}), c.remote->reads);
    ASSERT_EQ(0, c.stream->direct_io_count());
}

// A cut span is forgotten with its buffers.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_recovery_release_to_offset) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_chunk_range();
    c.name_pages({kB, kC, kE});
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));

    c.stream->release_to_offset(300 * KB);
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kC));
    ASSERT_EQ(1, c.stream->direct_io_count());
    ASSERT_EQ(0, c.stream->skip_ranges_recover_count());
    // the span of E is behind the offset, and it is kept
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kE));
    ASSERT_EQ(1, c.stream->skip_ranges_recover_count());

    c.stream->release();
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kE));
    ASSERT_EQ(2, c.stream->direct_io_count());
    ASSERT_EQ(1, c.stream->skip_ranges_recover_count());
}

// A failed load of the span is a failed read, the buffer stays unloaded, and the next read of the span loads it.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_recovery_survives_a_failed_read) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_chunk_range();
    c.name_pages({kB, kC});
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));

    c.remote->fail_next_reads = 1;
    std::string out(PageCacheCase::kPage, 0);
    ASSERT_FALSE(c.stream->read_at_fully(kB * PageCacheCase::kPage, out.data(), out.size()).ok());
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kB));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kC));
    ASSERT_EQ((Reads{{0, 100 * KB}, {100 * KB, 200 * KB}}), c.remote->reads);
    ASSERT_EQ(0, c.stream->direct_io_count());
}

// The provider can answer in any order, with ranges that overlap, touch, repeat, or lie outside the buffer. The stream
// sorts and joins them once, and the runs are the same as for the sorted answer.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_answer_in_any_order_with_overlaps) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_chunk_range();
    c.provider.skip.emplace_back(400 * KB, 100 * KB);
    c.provider.skip.emplace_back(200 * KB, 100 * KB);
    c.provider.skip.emplace_back(100 * KB, 100 * KB);
    c.provider.skip.emplace_back(150 * KB, 100 * KB);
    c.provider.skip.emplace_back(400 * KB, 100 * KB);
    c.provider.skip.emplace_back(550 * KB, 500 * KB);
    c.provider.skip.emplace_back(1500 * KB, 100 * KB);

    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_EQ((Reads{{0, 100 * KB}}), c.remote->reads);
    // [100, 300) KB, [400, 500) KB, and [550, 600) KB are named inside the buffer
    ASSERT_EQ(350 * KB, c.stream->skip_ranges_named_bytes());
    ASSERT_EQ(350 * KB, c.stream->skip_ranges_bytes());
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kD));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(500 * KB, 50 * KB));
    ASSERT_EQ((Reads{{0, 100 * KB}, {300 * KB, 100 * KB}, {500 * KB, 50 * KB}}), c.remote->reads);
}

// The planning takes time only when the stream asks the provider, once for a buffer, and the time includes the time of
// the provider.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_timer_counts_the_plan) {
    constexpr int64_t KB = PageCacheCase::KB;
    {
        PageCacheCase c;
        c.stream->set_skip_ranges_provider(nullptr);
        c.set_chunk_range();
        ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
        ASSERT_EQ(0, c.stream->skip_ranges_timer());
        ASSERT_EQ(0, c.stream->skip_ranges_plan_count());
    }
    PageCacheCase c;
    c.set_chunk_range();
    c.name_pages({kB, kC});
    c.provider.on_ask = [] { std::this_thread::sleep_for(std::chrono::milliseconds(2)); };
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    const int64_t timer = c.stream->skip_ranges_timer();
    ASSERT_LE(2'000'000, timer);
    ASSERT_EQ(1, c.stream->skip_ranges_plan_count());

    // the buffer of the read is planned already, the buffer D to F is asked about when the read takes a byte from it,
    // and a loaded buffer is not asked about
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kD));
    const int64_t timer_with_second_ask = c.stream->skip_ranges_timer();
    ASSERT_LE(timer + 2'000'000, timer_with_second_ask);
    ASSERT_EQ(2, c.stream->skip_ranges_plan_count());
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kF));
    ASSERT_EQ(2u, c.provider.asks.size());
    ASSERT_EQ(timer_with_second_ask, c.stream->skip_ranges_timer());
    ASSERT_EQ(2, c.stream->skip_ranges_plan_count());
    ASSERT_EQ(2, c.stream->shared_io_count());
    ASSERT_EQ(400 * KB, c.stream->shared_io_bytes());
}

// A provider that is switched off is not asked, and the stream does not time or mark anything: the buffer loads whole,
// and the buffers that load after the provider is on again are planned.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_provider_that_is_off_costs_nothing) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_ranges({io::SharedBufferedInputStream::IORange(0, 3 * PageCacheCase::kPage),
                  io::SharedBufferedInputStream::IORange(1000 * KB, 3 * PageCacheCase::kPage)});
    c.provider.skip.emplace_back(100 * KB, 100 * KB);
    c.provider.skip.emplace_back(1100 * KB, 100 * KB);
    c.provider.is_enabled = false;
    auto first = c.stream->find_shared_buffer(0, 300 * KB).value();

    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_EQ((Reads{{0, 300 * KB}}), c.remote->reads);
    ASSERT_TRUE(c.provider.asks.empty());
    ASSERT_FALSE(first->planned);
    ASSERT_EQ(0, c.stream->skip_ranges_plan_count());
    ASSERT_EQ(0, c.stream->skip_ranges_timer());
    ASSERT_EQ(0, c.stream->skip_ranges_named_bytes());

    // the switch is on again: the buffer that loads now is planned
    c.provider.is_enabled = true;
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(1000 * KB, 10 * KB));
    ASSERT_EQ((Reads{{0, 300 * KB}, {1000 * KB, 100 * KB}}), c.remote->reads);
    ASSERT_EQ(1, c.stream->skip_ranges_plan_count());
    ASSERT_EQ(1, c.provider.asks.size());
}

// The buffers that the plan makes keep the split flag of the buffer: a buffer that the data cache has answered for
// stays answered, and a buffer that it has not stays free to be cut by it.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_new_buffers_keep_the_split_flag) {
    constexpr int64_t KB = PageCacheCase::KB;
    for (bool split : {false, true}) {
        PageCacheCase c;
        c.set_chunk_range();
        c.name_pages({kB});
        auto sb = c.stream->find_shared_buffer(0, 600 * KB).value();
        sb->split = split;
        ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
        for (PageName page : {kA, kC}) {
            auto planned = c.stream->find_shared_buffer(page * PageCacheCase::kPage, PageCacheCase::kPage);
            ASSERT_OK(planned.status());
            ASSERT_NE(sb, planned.value());
            ASSERT_EQ(split, planned.value()->split);
            // the run of the read is planned, and the run C to F is asked about when a read takes a byte from it
            ASSERT_EQ(page == kA, planned.value()->planned);
        }
    }
}

// A gap between two runs is loaded when it is not larger than the coalesce distance, because we prefer to download it
// to a request of its own. The runs D and F have the cached page E between them.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_join_runs_with_a_small_gap) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c(0, 100 * KB);
    c.set_chunk_range();
    c.name_pages({kB, kC, kE});

    // The gap of B and C is 200 KB, above the distance, and the gap of E is 100 KB.
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kD));
    ASSERT_EQ((Reads{{0, 100 * KB}, {300 * KB, 300 * KB}}), c.remote->reads);
    // the gap of the page E is named, and loaded, and it is named again when the read of D asks about the run D to F
    ASSERT_EQ(400 * KB, c.stream->skip_ranges_named_bytes());
    ASSERT_EQ(200 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ(1, c.stream->skip_ranges_split_count());

    // F and E are in the loaded buffer already, and B and C are not.
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kE));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kF));
    ASSERT_EQ(2u, c.remote->reads.size());
    ASSERT_FALSE(c.page_covered(kB));
    ASSERT_FALSE(c.page_covered(kC));
    ASSERT_EQ(0, c.stream->direct_io_count());
}

// The distance is read from the config when the buffer is asked about, so a change takes effect at the next buffer.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_read_the_distance_at_each_plan) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    // two buffers of three pages, the page cache holds the middle page of each
    c.set_ranges({io::SharedBufferedInputStream::IORange(0, 3 * PageCacheCase::kPage),
                  io::SharedBufferedInputStream::IORange(1000 * KB, 3 * PageCacheCase::kPage)});
    c.provider.skip.emplace_back(1 * PageCacheCase::kPage, PageCacheCase::kPage);
    c.provider.skip.emplace_back(1000 * KB + PageCacheCase::kPage, PageCacheCase::kPage);

    config::io_coalesce_read_max_distance_size = static_cast<int32_t>(PageCacheCase::kPage - 1);
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_EQ((Reads{{0, PageCacheCase::kPage}}), c.remote->reads);

    config::io_coalesce_read_max_distance_size = static_cast<int32_t>(PageCacheCase::kPage);
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(1000 * KB, 10 * KB));
    ASSERT_EQ((Reads{{0, PageCacheCase::kPage}, {1000 * KB, 3 * PageCacheCase::kPage}}), c.remote->reads);
}

// The runs are joined while the buffer stays within the coalesce buffer size.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_joined_buffers_stay_within_max_buffer_size) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c(0, 100 * KB);
    io::SharedBufferedInputStream::CoalesceOptions options;
    options.max_dist_size = 100 * KB;
    options.max_buffer_size = 250 * KB;
    c.stream->set_coalesce_options(options);
    c.set_chunk_range();
    c.name_pages({kB, kC, kE});

    // D, E and F would be a buffer of 300 KB, so D and F stay apart. A and D are 200 KB apart.
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kD));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kF));
    ASSERT_EQ((Reads{{0, 100 * KB}, {300 * KB, 100 * KB}, {500 * KB, 100 * KB}}), c.remote->reads);
    ASSERT_EQ(300 * KB, c.stream->skip_ranges_bytes());
    // The pieces of the old buffer keep its large flag, because the old range is larger than max_buffer_size.
    ASSERT_EQ(300 * KB, c.stream->large_io_bytes());
}

// The read that loads the buffer needs its bytes, even if the provider names them, and a buffer that holds only the
// bytes of that read is the only one that the read loads.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_never_cut_the_bytes_of_the_read) {
    constexpr int64_t KB = PageCacheCase::KB;
    {
        PageCacheCase c;
        c.set_chunk_range();
        c.name_pages({kA, kB, kC, kD, kE, kF});
        ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kC));
        ASSERT_EQ((Reads{{200 * KB, 100 * KB}}), c.remote->reads);
        ASSERT_EQ(500 * KB, c.stream->skip_ranges_bytes());
        // the provider named the whole chunk, and the read of the page C kept its page
        ASSERT_EQ(600 * KB, c.stream->skip_ranges_named_bytes());
        ASSERT_TRUE(c.page_covered(kC));
        ASSERT_FALSE(c.page_covered(kB));
        ASSERT_FALSE(c.page_covered(kD));
        // the page B is evicted after the plan: its span is [0, 200 KB), and the read loads the rest of it
        ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kB));
        ASSERT_EQ(0, c.stream->direct_io_count());
        ASSERT_EQ(1, c.stream->skip_ranges_recover_count());
    }
    // a read that starts in B and ends in C leaves the part of B and C around it cut
    {
        PageCacheCase c;
        c.set_chunk_range();
        c.name_pages({kB, kC});
        ASSERT_NO_FATAL_FAILURE(c.read_and_check(150 * KB, 100 * KB));
        ASSERT_EQ((Reads{{150 * KB, 100 * KB}}), c.remote->reads);
        ASSERT_EQ(100 * KB, c.stream->skip_ranges_bytes());
        ASSERT_TRUE(c.covered(150 * KB, 100 * KB));
        ASSERT_FALSE(c.covered(100 * KB, 50 * KB));
        ASSERT_FALSE(c.covered(250 * KB, 50 * KB));
        ASSERT_TRUE(c.page_covered(kA));
        ASSERT_TRUE(c.covered(300 * KB, 300 * KB));
    }
}

// The provider is asked once for a buffer, also when it names nothing, and not for a buffer that is loaded.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_ask_once_for_each_buffer) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_ranges({io::SharedBufferedInputStream::IORange(0, 3 * PageCacheCase::kPage),
                  io::SharedBufferedInputStream::IORange(1000 * KB, 3 * PageCacheCase::kPage)});

    // it names nothing: the buffer stays as it is
    auto first = c.stream->find_shared_buffer(0, 300 * KB).value();
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kB));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kC));
    ASSERT_EQ((Reads{{0, 300 * KB}}), c.remote->reads);
    ASSERT_EQ((Reads{{0, 300 * KB}}), c.provider.asks);
    ASSERT_EQ(first, c.stream->find_shared_buffer(0, 300 * KB).value());
    ASSERT_TRUE(first->planned);
    ASSERT_FALSE(first->split);
    ASSERT_EQ(0, c.stream->skip_ranges_bytes());
    ASSERT_EQ(0, c.stream->skip_ranges_split_count());

    // the other buffer is asked on its own first read
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(1000 * KB, 10 * KB));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(1100 * KB, 10 * KB));
    ASSERT_EQ((Reads{{0, 300 * KB}, {1000 * KB, 300 * KB}}), c.remote->reads);
    ASSERT_EQ((Reads{{0, 300 * KB}, {1000 * KB, 300 * KB}}), c.provider.asks);

    // a buffer is not planned twice, also when the plan is called by hand
    auto third = c.stream->find_shared_buffer(1000 * KB, 300 * KB).value();
    ASSERT_FALSE(c.stream->plan_load(third, 0, 0));
    ASSERT_EQ(2u, c.provider.asks.size());
}

// Four lazy columns share one coalesced buffer [L1 L2 L3 L4] of 100 KB each. The provider knows the pages of L1 when the
// read of L1 loads the buffer, and it knows the pages of L2 only when the read of L2 first takes a byte from the buffer.
// The plan of the read of L1 leaves the run L2 to L4 unplanned, so the read of L2 asks about it and narrows L2, and the
// plan of L2 leaves the run behind L2 unplanned in the same way.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_ask_again_about_the_columns_that_share_a_buffer) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    io::SharedBufferedInputStream::CoalesceOptions options;
    options.max_dist_size = 0;
    options.max_buffer_size = 8 * 1024 * KB;
    c.stream->set_coalesce_options(options);
    c.set_ranges({io::SharedBufferedInputStream::IORange(0, 100 * KB),
                  io::SharedBufferedInputStream::IORange(100 * KB, 100 * KB),
                  io::SharedBufferedInputStream::IORange(200 * KB, 100 * KB),
                  io::SharedBufferedInputStream::IORange(300 * KB, 100 * KB)});
    ASSERT_TRUE(c.covered(0, 400 * KB));
    c.provider.on_ask = [&c] {
        c.provider.skip.clear();
        c.provider.defer.clear();
        if (c.provider.asks.size() == 1) {
            // L1: the reader needs its first 25 KB now and may need the rest later
            c.provider.defer.emplace_back(25 * KB, 75 * KB);
        } else if (c.provider.asks.size() == 2) {
            // L2: the reader needs its first 25 KB, and its pages from 125 KB to 175 KB are dead
            c.provider.skip.emplace_back(125 * KB, 50 * KB);
        }
    };

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, 25 * KB));
    ASSERT_EQ((Reads{{0, 25 * KB}}), c.remote->reads);
    ASSERT_EQ((Reads{{0, 400 * KB}}), c.provider.asks);

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(100 * KB, 25 * KB));
    ASSERT_EQ((Reads{{0, 25 * KB}, {100 * KB, 25 * KB}}), c.remote->reads);
    ASSERT_EQ((Reads{{0, 400 * KB}, {100 * KB, 300 * KB}}), c.provider.asks);
    ASSERT_EQ(50 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ(0, c.stream->direct_io_count());

    // the run behind L2 is asked about when a read takes a byte from it, and the provider names nothing there
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(300 * KB, 10 * KB));
    ASSERT_EQ((Reads{{0, 25 * KB}, {100 * KB, 25 * KB}, {175 * KB, 225 * KB}}), c.remote->reads);
    ASSERT_EQ(3u, c.provider.asks.size());
    ASSERT_EQ(3, c.stream->skip_ranges_plan_count());

    // the deferred part of L1 is a buffer of its own, and its read asks the provider again
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(50 * KB, 10 * KB));
    ASSERT_EQ((Reads{{0, 400 * KB}, {100 * KB, 300 * KB}, {175 * KB, 225 * KB}, {25 * KB, 75 * KB}}), c.provider.asks);
    ASSERT_EQ((Reads{{0, 25 * KB}, {100 * KB, 25 * KB}, {175 * KB, 225 * KB}, {25 * KB, 75 * KB}}), c.remote->reads);
}

// Without a provider, or after it is taken out, the buffer is loaded whole.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_without_a_provider_the_buffer_is_loaded_whole) {
    constexpr int64_t KB = PageCacheCase::KB;
    {
        PageCacheCase c;
        c.set_chunk_range();
        c.name_pages({kB, kC, kE});
        c.stream->set_skip_ranges_provider(nullptr);
        ASSERT_TRUE(c.stream->skip_ranges_provider() == nullptr);
        ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
        ASSERT_EQ((Reads{{0, 600 * KB}}), c.remote->reads);
        ASSERT_TRUE(c.provider.asks.empty());
        ASSERT_EQ(0, c.stream->skip_ranges_bytes());
    }
    // a buffer that was asked before the provider went away stays cut
    {
        PageCacheCase c;
        c.set_chunk_range();
        c.name_pages({kB, kC, kE});
        ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
        c.stream->set_skip_ranges_provider(nullptr);
        ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kF));
        ASSERT_EQ((Reads{{0, 100 * KB}, {500 * KB, 100 * KB}}), c.remote->reads);
    }
}

// With the align size of the data cache the buffers keep to the borders of the blocks: a cached range gives up the
// partial blocks at its ends, and a block that holds one byte to load is loaded whole.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_cut_at_the_borders_of_the_blocks) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c(256 * KB);
    c.set_chunk_range();
    // The raw range [0, 600 KB) is the buffer [0, 768 KB). The provider names [100 KB, 700 KB), of which the blocks
    // [256 KB, 512 KB) are whole.
    c.provider.skip.emplace_back(100 * KB, 600 * KB);
    auto old_sb = c.stream->find_shared_buffer(0, 600 * KB).value();
    ASSERT_EQ(768 * KB, old_sb->size);

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(0, 100 * KB));
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(550 * KB, 10 * KB));
    ASSERT_EQ((Reads{{0, 256 * KB}, {512 * KB, 256 * KB}}), c.remote->reads);
    ASSERT_EQ(256 * KB, c.stream->skip_ranges_bytes());
    // the blocks that the range covers in part are named and loaded, and the buffer [512 KB, 768 KB) is asked about
    // again when the second read takes a byte from it
    ASSERT_EQ(788 * KB, c.stream->skip_ranges_named_bytes());
    // the second buffer holds 88 KB that a read asked for and the padding of the alignment
    ASSERT_EQ(168 * KB, c.stream->shared_align_io_bytes());
    ASSERT_EQ(0, c.stream->direct_io_count());
    ASSERT_FALSE(c.covered(256 * KB, 1));
    ASSERT_FALSE(c.covered(511 * KB, 1));

    // a cached range that holds no whole block cuts nothing
    PageCacheCase small(256 * KB);
    small.set_chunk_range();
    small.provider.skip.emplace_back(200 * KB, 200 * KB);
    ASSERT_NO_FATAL_FAILURE(small.read_and_check(0, 100 * KB));
    ASSERT_EQ((Reads{{0, 768 * KB}}), small.remote->reads);
    ASSERT_EQ(0, small.stream->skip_ranges_split_count());
}

// A large io range is cut into pieces, and a piece that the provider names a part of is cut further. A read that
// crosses the border of two pieces plans both pieces.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_cut_a_piece_of_a_large_range) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    config::io_coalesce_split_large_range = true;
    io::SharedBufferedInputStream::CoalesceOptions options;
    options.max_dist_size = 0;
    options.max_buffer_size = 512 * KB;
    c.stream->set_coalesce_options(options);
    // the pieces [0, 512 KB), [512 KB, 1024 KB) and [1024 KB, 1536 KB)
    c.set_ranges({io::SharedBufferedInputStream::IORange(0, 1536 * KB)});
    c.provider.skip.emplace_back(520 * KB, 480 * KB);

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(600 * KB, 100 * KB));
    ASSERT_EQ((Reads{{600 * KB, 100 * KB}}), c.remote->reads);
    ASSERT_EQ((Reads{{512 * KB, 512 * KB}}), c.provider.asks);
    ASSERT_EQ(380 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ(100 * KB, c.stream->large_io_bytes());

    // the head of the piece and the tail of the piece are buffers of their own. The tail reaches the end of the piece,
    // so a read goes on from it into the next piece.
    auto head = c.stream->find_shared_buffer(512 * KB, 8 * KB);
    ASSERT_OK(head.status());
    ASSERT_TRUE(head.value()->large);
    ASSERT_FALSE(head.value()->has_next_piece);
    auto tail = c.stream->find_shared_buffer(1000 * KB, 24 * KB);
    ASSERT_OK(tail.status());
    ASSERT_TRUE(tail.value()->large);
    ASSERT_TRUE(tail.value()->has_next_piece);
    ASSERT_FALSE(c.covered(520 * KB, 1));
    ASSERT_FALSE(c.covered(999 * KB, 1));

    // the read crosses from the tail into the third piece: both are asked about, because the plan of the read of the
    // middle did not need the tail, and both are loaded
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(1020 * KB, 10 * KB));
    ASSERT_EQ((Reads{{600 * KB, 100 * KB}, {1000 * KB, 24 * KB}, {1024 * KB, 512 * KB}}), c.remote->reads);
    ASSERT_EQ((Reads{{512 * KB, 512 * KB}, {1000 * KB, 24 * KB}, {1024 * KB, 512 * KB}}), c.provider.asks);
    ASSERT_EQ(0, c.stream->direct_io_count());
}

// A read that crosses the border of two pieces, with the provider naming the bytes next to the read in the second
// piece.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_read_across_two_pieces_keeps_its_bytes) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    config::io_coalesce_split_large_range = true;
    io::SharedBufferedInputStream::CoalesceOptions options;
    options.max_dist_size = 0;
    options.max_buffer_size = 512 * KB;
    c.stream->set_coalesce_options(options);
    c.set_ranges({io::SharedBufferedInputStream::IORange(0, 1536 * KB)});
    c.provider.skip.emplace_back(520 * KB, 480 * KB);

    // [500 KB, 530 KB) starts in the first piece and ends in the second one, where the provider names most of the bytes
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(500 * KB, 30 * KB));
    ASSERT_EQ((Reads{{0, 512 * KB}, {512 * KB, 18 * KB}}), c.remote->reads);
    ASSERT_EQ(0, c.stream->direct_io_count());
    ASSERT_FALSE(c.covered(540 * KB, 1));
    ASSERT_TRUE(c.covered(1000 * KB, 24 * KB));
}

// The reader can peek into a loaded buffer. A buffer that is not loaded yet, and a cut out range, refuse the peek.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_peek) {
    PageCacheCase c;
    c.set_chunk_range();
    c.name_pages({kB, kC, kE});

    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_OK(c.stream->seek(kD * PageCacheCase::kPage + 10));
    ASSERT_FALSE(c.stream->peek(100).ok());
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kD));
    ASSERT_OK(c.stream->seek(kD * PageCacheCase::kPage + 10));
    auto peeked = c.stream->peek(100);
    ASSERT_OK(peeked.status());
    ASSERT_EQ(0, memcmp(peeked.value().data(), c.data.data() + kD * PageCacheCase::kPage + 10, 100));
    ASSERT_OK(c.stream->seek(kB * PageCacheCase::kPage + 10));
    ASSERT_FALSE(c.stream->peek(100).ok());
    ASSERT_OK(c.stream->seek(kD * PageCacheCase::kPage - 50));
    ASSERT_FALSE(c.stream->peek(100).ok());
    ASSERT_EQ(2u, c.remote->reads.size());
}

// release_to_offset frees the buffers that the cut made one by one, and the rest of the chunk reads as before.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_release_to_offset) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_chunk_range();
    c.name_pages({kB, kC, kE});
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));

    // The key of a buffer is the end of its raw range: 100 KB, 400 KB and 600 KB.
    c.stream->release_to_offset(399 * KB);
    ASSERT_FALSE(c.page_covered(kA));
    ASSERT_TRUE(c.page_covered(kD));
    c.stream->release_to_offset(400 * KB);
    ASSERT_FALSE(c.page_covered(kD));
    ASSERT_TRUE(c.page_covered(kF));
    // a read of a released buffer is a direct read with the right bytes
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_EQ(1, c.stream->direct_io_count());
    c.stream->release_to_offset(600 * KB);
    ASSERT_FALSE(c.page_covered(kF));
}

// The provider reads through the stream, as the reader does when it loads an index of the file for the plan. The read
// goes through another buffer, which is asked about on its own.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_provider_reads_another_buffer) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_ranges({io::SharedBufferedInputStream::IORange(0, 6 * PageCacheCase::kPage),
                  io::SharedBufferedInputStream::IORange(1000 * KB, 100 * KB)});
    c.name_pages({kB, kC, kE});
    c.provider.on_ask = [&c] {
        // only the ask for the chunk reads the other buffer
        if (c.provider.asks.size() == 1) {
            std::string buffer(10, 0);
            ASSERT_OK(c.stream->read_at_fully(1000 * KB, buffer.data(), 10));
            ASSERT_EQ(0, memcmp(buffer.data(), c.data.data() + 1000 * KB, 10));
        }
    };

    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_EQ((Reads{{0, 600 * KB}, {1000 * KB, 100 * KB}}), c.provider.asks);
    ASSERT_EQ((Reads{{1000 * KB, 100 * KB}, {0, 100 * KB}}), c.remote->reads);
    ASSERT_EQ(300 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ(1, c.stream->skip_ranges_split_count());
}

// The provider loads the buffer that it is asked about. The buffer cannot be cut any more, and is read as loaded.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_provider_loads_the_buffer) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_chunk_range();
    c.name_pages({kB, kC, kE});
    c.provider.on_ask = [&c] {
        std::string buffer(10, 0);
        ASSERT_OK(c.stream->read_at_fully(0, buffer.data(), 10));
    };

    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kD));
    ASSERT_EQ((Reads{{0, 600 * KB}}), c.remote->reads);
    ASSERT_EQ(1u, c.provider.asks.size());
    ASSERT_EQ(0, c.stream->skip_ranges_bytes());
    ASSERT_EQ(0, c.stream->skip_ranges_split_count());
    ASSERT_TRUE(c.page_covered(kB));
}

// Whatever the provider names, every read returns the bytes of the file: a byte that was cut out is read directly.
TEST_F(SharedBufferedInputStreamTest, test_skip_ranges_random_reads_return_the_bytes_of_the_file) {
    constexpr int64_t KB = PageCacheCase::KB;
    std::mt19937 gen(20260709);
    for (int iteration = 0; iteration < 300; ++iteration) {
        SCOPED_TRACE("iteration " + std::to_string(iteration));
        const int64_t align_size = (gen() % 2 == 0) ? 0 : 64 * KB;
        const int64_t distance = (gen() % 3) * 50 * KB;
        PageCacheCase c(align_size, distance, 1024 * KB);
        io::SharedBufferedInputStream::CoalesceOptions options;
        options.max_dist_size = distance;
        options.max_buffer_size = (gen() % 2 == 0) ? 8 * 1024 * KB : 200 * KB;
        c.stream->set_coalesce_options(options);
        config::io_coalesce_split_large_range = gen() % 2 == 0;

        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        int64_t cursor = gen() % (50 * KB);
        while (cursor < 900 * KB && ranges.size() < 3) {
            const int64_t size = 1 + gen() % (400 * KB);
            ranges.emplace_back(cursor, std::min(size, 1000 * KB - cursor));
            cursor += ranges.back().size + gen() % (150 * KB);
        }
        c.set_ranges(ranges);

        const int skip_count = gen() % 6;
        for (int i = 0; i < skip_count; ++i) {
            c.provider.skip.emplace_back(gen() % (1000 * KB), 1 + gen() % (200 * KB));
        }

        for (int read = 0; read < 12; ++read) {
            const auto& range = ranges[gen() % ranges.size()];
            const int64_t offset = range.offset + gen() % range.size;
            const int64_t size = 1 + gen() % std::min<int64_t>(range.offset + range.size - offset, 60 * KB);
            ASSERT_NO_FATAL_FAILURE(c.read_and_check(offset, size));
        }
        // every byte of the ranges can be read, whether a buffer holds it or not
        for (const auto& range : ranges) {
            ASSERT_NO_FATAL_FAILURE(c.read_and_check(range.offset, range.size));
        }
    }
}

// The provider defers the pages D, E and F: the read of the page A loads the run A, B and C, and the deferred pages are
// a buffer that is not loaded. The provider is asked again when a read first takes a byte from that buffer, and then it
// skips D, defers F, and the read of E loads only E.
TEST_F(SharedBufferedInputStreamTest, test_defer_ranges_become_buffers_that_are_asked_again) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_chunk_range();
    c.defer_pages({kD, kE, kF});

    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_EQ((Reads{{0, 300 * KB}}), c.remote->reads);
    ASSERT_EQ((Reads{{0, 600 * KB}}), c.provider.asks);
    ASSERT_EQ(0, c.stream->skip_ranges_bytes());
    ASSERT_EQ(300 * KB, c.stream->skip_ranges_deferred_bytes());
    ASSERT_EQ(1, c.stream->skip_ranges_deferred_count());
    ASSERT_EQ(1, c.stream->skip_ranges_split_count());
    auto deferred = c.stream->find_shared_buffer(300 * KB, 300 * KB);
    ASSERT_OK(deferred.status());
    ASSERT_EQ(0u, deferred.value()->buffer.capacity());
    ASSERT_FALSE(deferred.value()->planned);
    ASSERT_FALSE(deferred.value()->split);
    // the run is planned, and it is loaded
    auto run = c.stream->find_shared_buffer(0, 300 * KB);
    ASSERT_OK(run.status());
    ASSERT_TRUE(run.value()->planned);

    // The next read needs E only, and the provider knows now that D is dead and F is for later.
    c.provider.defer.clear();
    c.provider.skip.clear();
    c.name_pages({kD});
    c.defer_pages({kF});
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kE));
    ASSERT_EQ((Reads{{0, 300 * KB}, {400 * KB, 100 * KB}}), c.remote->reads);
    ASSERT_EQ((Reads{{0, 600 * KB}, {300 * KB, 300 * KB}}), c.provider.asks);
    ASSERT_EQ(100 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ(400 * KB, c.stream->skip_ranges_deferred_bytes());
    ASSERT_EQ(2, c.stream->skip_ranges_deferred_count());
    ASSERT_EQ(2, c.stream->skip_ranges_split_count());
    ASSERT_FALSE(c.page_covered(kD));
    ASSERT_TRUE(c.page_covered(kE));
    auto last = c.stream->find_shared_buffer(500 * KB, 100 * KB);
    ASSERT_OK(last.status());
    ASSERT_FALSE(last.value()->planned);

    // The provider names nothing for the page F, and the buffer is loaded whole, with the last request.
    c.provider.defer.clear();
    c.provider.skip.clear();
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kF));
    ASSERT_EQ((Reads{{0, 300 * KB}, {400 * KB, 100 * KB}, {500 * KB, 100 * KB}}), c.remote->reads);
    ASSERT_EQ(3u, c.provider.asks.size());
    ASSERT_EQ(3, c.stream->shared_io_count());
    ASSERT_EQ(0, c.stream->direct_io_count());
    ASSERT_EQ(0, c.stream->skip_ranges_recover_count());

    // A page of the dead range is read with one read of its cut span, and not with a direct read.
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kD));
    ASSERT_EQ((Reads{{0, 300 * KB}, {400 * KB, 100 * KB}, {500 * KB, 100 * KB}, {300 * KB, 100 * KB}}),
              c.remote->reads);
    ASSERT_EQ(0, c.stream->direct_io_count());
    ASSERT_EQ(1, c.stream->skip_ranges_recover_count());
    ASSERT_EQ(100 * KB, c.stream->skip_ranges_recover_bytes());
}

// The read that loads the buffer is never deferred, also when the provider defers the whole buffer.
TEST_F(SharedBufferedInputStreamTest, test_defer_never_defers_the_bytes_of_the_read) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_chunk_range();
    c.provider.defer.emplace_back(0, 600 * KB);

    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kC));
    ASSERT_EQ((Reads{{200 * KB, 100 * KB}}), c.remote->reads);
    ASSERT_EQ(500 * KB, c.stream->skip_ranges_deferred_bytes());
    ASSERT_EQ(2, c.stream->skip_ranges_deferred_count());
    ASSERT_EQ(0, c.stream->skip_ranges_bytes());
    for (PageName page : {kA, kB, kD, kE, kF}) {
        auto sb = c.stream->find_shared_buffer(page * PageCacheCase::kPage, PageCacheCase::kPage);
        ASSERT_OK(sb.status());
        ASSERT_FALSE(sb.value()->planned) << page;
        ASSERT_EQ(0u, sb.value()->buffer.capacity());
    }
    // the deferred bytes before and behind the run are two buffers, and no buffer covers the run and its neighbors
    ASSERT_FALSE(c.covered(100 * KB, 200 * KB));
    ASSERT_FALSE(c.covered(200 * KB, 200 * KB));
    // a read of a deferred byte is not a direct read
    c.provider.defer.clear();
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_EQ(0, c.stream->direct_io_count());
}

// A deferred range that is not larger than the coalesce distance, next to a run, is loaded with the run: a buffer of its
// own costs a request when a read needs it. A larger deferred range stays a buffer.
TEST_F(SharedBufferedInputStreamTest, test_defer_small_range_next_to_a_run_is_loaded_with_it) {
    constexpr int64_t KB = PageCacheCase::KB;
    // The page A is deferred and next to the read of the page B. It is not larger than the distance of 100 KB, so it is a
    // part of the run, and the pages C to F are 400 KB and a buffer.
    PageCacheCase c(0, 100 * KB);
    c.set_chunk_range();
    c.defer_pages({kA, kC, kD, kE, kF});
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kB));
    ASSERT_EQ((Reads{{0, 200 * KB}}), c.remote->reads);
    ASSERT_EQ(400 * KB, c.stream->skip_ranges_deferred_bytes());
    ASSERT_EQ(1, c.stream->skip_ranges_deferred_count());

    // The same behind the run: the page B is deferred and behind the read of the page A, so it is a part of the run, and
    // the run C joins it. The pages D to F are deferred.
    PageCacheCase d(0, 100 * KB);
    d.set_chunk_range();
    d.defer_pages({kB, kD, kE, kF});
    ASSERT_NO_FATAL_FAILURE(d.read_page_and_check(kA));
    ASSERT_EQ((Reads{{0, 300 * KB}}), d.remote->reads);
    ASSERT_EQ(300 * KB, d.stream->skip_ranges_deferred_bytes());
    ASSERT_EQ(1, d.stream->skip_ranges_deferred_count());

    // A deferred range larger than the distance stays a buffer: the pages B and C are 200 KB.
    PageCacheCase e(0, 100 * KB);
    e.set_chunk_range();
    e.defer_pages({kB, kC});
    ASSERT_NO_FATAL_FAILURE(e.read_page_and_check(kA));
    // A, D, E and F are runs; the gap between A and D is deferred, not cut, so they are not joined
    ASSERT_EQ((Reads{{0, 100 * KB}}), e.remote->reads);
    ASSERT_EQ(200 * KB, e.stream->skip_ranges_deferred_bytes());
    ASSERT_NO_FATAL_FAILURE(e.read_page_and_check(kD));
    ASSERT_EQ((Reads{{0, 100 * KB}, {300 * KB, 300 * KB}}), e.remote->reads);
}

// Skip wins over defer, and a skipped byte is a hole between the buffers.
TEST_F(SharedBufferedInputStreamTest, test_defer_and_skip_ranges_together) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_chunk_range();
    c.name_pages({kB});
    // the provider defers B, C and D, and skips B
    c.defer_pages({kB, kC, kD});
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    // A and the pages E and F are runs, B is cut, C and D are deferred
    ASSERT_EQ((Reads{{0, 100 * KB}}), c.remote->reads);
    ASSERT_EQ(100 * KB, c.stream->skip_ranges_bytes());
    ASSERT_EQ(200 * KB, c.stream->skip_ranges_deferred_bytes());
    ASSERT_FALSE(c.page_covered(kB));
    ASSERT_TRUE(c.page_covered(kC));
    ASSERT_TRUE(c.page_covered(kD));
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kE));
    ASSERT_EQ((Reads{{0, 100 * KB}, {400 * KB, 200 * KB}}), c.remote->reads);
    ASSERT_EQ(0, c.stream->direct_io_count());
}

// With an align size the deferred ranges give up their partial blocks, like the skipped ones, so every buffer that the
// split makes begins and ends at a border of a block.
TEST_F(SharedBufferedInputStreamTest, test_defer_ranges_keep_to_the_blocks) {
    constexpr int64_t KB = PageCacheCase::KB;
    constexpr int64_t kBlock = 256 * KB;
    PageCacheCase c(kBlock);
    // the aligned range of the buffer is [0, 2 MB)
    c.set_ranges({io::SharedBufferedInputStream::IORange(100 * KB, 1800 * KB)});
    c.provider.defer.emplace_back(300 * KB, 1200 * KB);

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(110 * KB, 10 * KB));
    // the deferred bytes [300 KB, 1500 KB) give up their partial blocks: [512 KB, 1280 KB) is deferred
    ASSERT_EQ((Reads{{0, 512 * KB}}), c.remote->reads);
    ASSERT_EQ(768 * KB, c.stream->skip_ranges_deferred_bytes());
    auto deferred = c.stream->find_shared_buffer(512 * KB, 768 * KB);
    ASSERT_OK(deferred.status());
    ASSERT_EQ(512 * KB, deferred.value()->offset);
    ASSERT_EQ(768 * KB, deferred.value()->size);
    ASSERT_FALSE(deferred.value()->planned);
    // the bytes behind the deferred range are a run, and it is loaded whole
    c.provider.defer.clear();
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(1600 * KB, 10 * KB));
    ASSERT_EQ((Reads{{0, 512 * KB}, {1280 * KB, 768 * KB}}), c.remote->reads);
    // the deferred buffer is asked about when it is read
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(600 * KB, 10 * KB));
    ASSERT_EQ((Reads{{0, 512 * KB}, {1280 * KB, 768 * KB}, {512 * KB, 768 * KB}}), c.remote->reads);
    ASSERT_EQ(0, c.stream->direct_io_count());
}

// A piece of a large io range is split to a run and a deferred buffer, which is still a piece of the large range: the
// next piece follows it, and a read across their border is served by both.
TEST_F(SharedBufferedInputStreamTest, test_defer_a_piece_of_a_large_range) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c(0, 0, 1024 * KB);
    io::SharedBufferedInputStream::CoalesceOptions options;
    options.max_dist_size = 0;
    options.max_buffer_size = 256 * KB;
    c.stream->set_coalesce_options(options);
    config::io_coalesce_split_large_range = true;
    // four pieces of 256 KB
    c.set_ranges({io::SharedBufferedInputStream::IORange(0, 1024 * KB)});
    c.provider.defer.emplace_back(300 * KB, 212 * KB);

    ASSERT_NO_FATAL_FAILURE(c.read_and_check(260 * KB, 10 * KB));
    ASSERT_EQ((Reads{{256 * KB, 44 * KB}}), c.remote->reads);
    auto deferred = c.stream->find_shared_buffer(300 * KB, 212 * KB);
    ASSERT_OK(deferred.status());
    ASSERT_TRUE(deferred.value()->large);
    ASSERT_TRUE(deferred.value()->has_next_piece);
    ASSERT_FALSE(deferred.value()->planned);

    // a read that crosses the end of the deferred buffer into the next piece loads both, and asks for each
    c.provider.defer.clear();
    ASSERT_NO_FATAL_FAILURE(c.read_and_check(500 * KB, 24 * KB));
    ASSERT_EQ((Reads{{256 * KB, 44 * KB}, {300 * KB, 212 * KB}, {512 * KB, 256 * KB}}), c.remote->reads);
    ASSERT_EQ(0, c.stream->direct_io_count());
}

// The time of the planning, the recovery of a cut span and the release. The deferred bytes are buffers, so a read of them is
// never a recovery, and the spans of the cut bytes are the only ones that the stream recovers.
TEST_F(SharedBufferedInputStreamTest, test_defer_plan_time_and_cut_spans) {
    constexpr int64_t KB = PageCacheCase::KB;
    PageCacheCase c;
    c.set_chunk_range();
    ASSERT_EQ(0, c.stream->skip_ranges_timer());
    c.name_pages({kB, kC});
    c.defer_pages({kE, kF});
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kA));
    ASSERT_GT(c.stream->skip_ranges_timer(), 0);
    ASSERT_EQ(1, c.stream->skip_ranges_plan_count());
    ASSERT_EQ(0, c.stream->skip_ranges_recover_count());
    ASSERT_EQ(200 * KB, c.stream->skip_ranges_deferred_bytes());

    // The pages E and F are a buffer, and a read of them is not a recovery. The page D is a run.
    c.provider.skip.clear();
    c.provider.defer.clear();
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kE));
    ASSERT_EQ(0, c.stream->skip_ranges_recover_count());
    ASSERT_EQ(0, c.stream->direct_io_count());

    // a read in the cut span of B and C loads the span with one read, and the page C is served from it
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kB));
    ASSERT_EQ(1, c.stream->skip_ranges_recover_count());
    ASSERT_EQ(200 * KB, c.stream->skip_ranges_recover_bytes());
    ASSERT_NO_FATAL_FAILURE(c.read_page_and_check(kC));
    ASSERT_EQ(1, c.stream->skip_ranges_recover_count());
    ASSERT_EQ(0, c.stream->direct_io_count());
    // the recovered span is a buffer now, and it does not overlap the deferred buffer
    ASSERT_TRUE(c.page_covered(kB));
    ASSERT_TRUE(c.page_covered(kC));
    ASSERT_EQ(0, c.stream->direct_io_count());

    // a read that begins in a buffer and ends in a cut span, and a read outside the buffers, are direct reads
    PageCacheCase d;
    d.set_chunk_range();
    d.name_pages({kB, kC});
    ASSERT_NO_FATAL_FAILURE(d.read_page_and_check(kA));
    ASSERT_NO_FATAL_FAILURE(d.read_and_check(90 * KB, 20 * KB));
    ASSERT_NO_FATAL_FAILURE(d.read_and_check(1000 * KB, 10 * KB));
    ASSERT_EQ(2, d.stream->direct_io_count());
    ASSERT_EQ(0, d.stream->skip_ranges_recover_count());

    // the cut spans are forgotten with the buffers around them
    d.stream->release_to_offset(600 * KB);
    ASSERT_NO_FATAL_FAILURE(d.read_page_and_check(kB));
    ASSERT_EQ(3, d.stream->direct_io_count());
    ASSERT_EQ(0, d.stream->skip_ranges_recover_count());
}

// Whatever the provider skips and defers, every read returns the bytes of the file.
TEST_F(SharedBufferedInputStreamTest, test_defer_ranges_random_reads_return_the_bytes_of_the_file) {
    constexpr int64_t KB = PageCacheCase::KB;
    std::mt19937 gen(20261009);
    for (int iteration = 0; iteration < 300; ++iteration) {
        SCOPED_TRACE("iteration " + std::to_string(iteration));
        const int64_t align_size = (gen() % 2 == 0) ? 0 : 64 * KB;
        const int64_t distance = (gen() % 3) * 50 * KB;
        PageCacheCase c(align_size, distance, 1024 * KB);
        io::SharedBufferedInputStream::CoalesceOptions options;
        options.max_dist_size = distance;
        options.max_buffer_size = (gen() % 2 == 0) ? 8 * 1024 * KB : 200 * KB;
        c.stream->set_coalesce_options(options);
        config::io_coalesce_split_large_range = gen() % 2 == 0;

        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        int64_t cursor = gen() % (50 * KB);
        while (cursor < 900 * KB && ranges.size() < 3) {
            const int64_t size = 1 + gen() % (400 * KB);
            ranges.emplace_back(cursor, std::min(size, 1000 * KB - cursor));
            cursor += ranges.back().size + gen() % (150 * KB);
        }
        c.set_ranges(ranges);

        for (int i = gen() % 4; i > 0; --i) {
            c.provider.skip.emplace_back(gen() % (1000 * KB), 1 + gen() % (200 * KB));
        }
        for (int i = gen() % 4; i > 0; --i) {
            c.provider.defer.emplace_back(gen() % (1000 * KB), 1 + gen() % (300 * KB));
        }

        for (int read = 0; read < 16; ++read) {
            if (read % 5 == 4) {
                // the provider changes its mind for the buffers that are asked about later
                c.provider.skip.clear();
                c.provider.defer.clear();
                for (int i = gen() % 3; i > 0; --i) {
                    c.provider.defer.emplace_back(gen() % (1000 * KB), 1 + gen() % (300 * KB));
                }
            }
            const auto& range = ranges[gen() % ranges.size()];
            const int64_t offset = range.offset + gen() % range.size;
            const int64_t size = 1 + gen() % std::min<int64_t>(range.offset + range.size - offset, 60 * KB);
            ASSERT_NO_FATAL_FAILURE(c.read_and_check(offset, size));
        }
        for (const auto& range : ranges) {
            ASSERT_NO_FATAL_FAILURE(c.read_and_check(range.offset, range.size));
        }
    }
}

} // namespace starrocks::io
