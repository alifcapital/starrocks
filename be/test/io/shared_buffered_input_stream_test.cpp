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

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

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

    // (offset, size) of each read, in the order of the reads.
    std::vector<std::pair<int64_t, int64_t>> reads;

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

} // namespace starrocks::io