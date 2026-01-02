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

#include <atomic>
#include <chrono>
#include <thread>

#include "common/config.h"
#include "io_test_base.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"

namespace starrocks::io {

// Mock stream with configurable delay for testing parallel read behavior
class MockDelayedInputStream : public SeekableInputStream {
public:
    MockDelayedInputStream(std::string data, int read_delay_ms = 0)
            : _data(std::move(data)), _read_delay_ms(read_delay_ms) {}

    StatusOr<int64_t> read(void* out, int64_t count) override {
        int64_t to_read = std::min(count, static_cast<int64_t>(_data.size()) - _offset);
        if (to_read > 0) {
            memcpy(out, _data.data() + _offset, to_read);
            _offset += to_read;
        }
        return to_read;
    }

    Status read_fully(void* out, int64_t count) override {
        ASSIGN_OR_RETURN(auto nread, read(out, count));
        if (nread < count) {
            return Status::IOError("Unexpected EOF");
        }
        return Status::OK();
    }

    Status seek(int64_t offset) override {
        _offset = offset;
        return Status::OK();
    }

    StatusOr<int64_t> position() override { return _offset; }

    StatusOr<int64_t> get_size() override { return static_cast<int64_t>(_data.size()); }

    Status skip(int64_t count) override {
        _offset += count;
        return Status::OK();
    }

    // Thread-safe positional read with configurable delay
    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        if (_read_delay_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(_read_delay_ms));
        }
        _read_count.fetch_add(1, std::memory_order_relaxed);

        if (offset + count > static_cast<int64_t>(_data.size())) {
            return Status::IOError("Read beyond EOF");
        }
        memcpy(out, _data.data() + offset, count);
        return Status::OK();
    }

    int read_count() const { return _read_count.load(std::memory_order_relaxed); }

private:
    std::string _data;
    int64_t _offset{0};
    int _read_delay_ms;
    std::atomic<int> _read_count{0};
};

// Helper to generate test data
static std::string generate_test_data(size_t size) {
    std::string data(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<char>('A' + (i % 26));
    }
    return data;
}

class SharedBufferedInputStreamTest : public ::testing::Test {
protected:
    static constexpr int64_t KB = 1024;
    static constexpr int64_t MB = 1024 * 1024;
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

// ==================== Parallel Read Tests ====================

// Test: Parallel read is triggered by set_io_ranges and get_bytes returns correct data
TEST_F(SharedBufferedInputStreamTest, test_parallel_read_data_integrity) {
    const size_t file_size = 8 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    // Create 4 IO ranges of 2MB each
    std::vector<SharedBufferedInputStream::IORange> ranges;
    for (int i = 0; i < 4; ++i) {
        ranges.emplace_back(i * 2 * MB, 2 * MB, true);
    }

    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    // Read all ranges - get_bytes() blocks until parallel read completes
    for (int i = 0; i < 4; ++i) {
        int64_t offset = i * 2 * MB;
        const uint8_t* buffer = nullptr;
        ASSERT_OK(sb_stream->get_bytes(&buffer, offset, 2 * MB, nullptr));

        // Verify data matches
        for (size_t j = 0; j < 2 * MB; ++j) {
            ASSERT_EQ(data[offset + j], static_cast<char>(buffer[j]))
                    << "Mismatch at offset " << (offset + j);
        }
    }
}

// Test: Parallel reads run concurrently (2 reads with 30ms delay complete in ~30ms, not 60ms)
TEST_F(SharedBufferedInputStreamTest, test_parallel_read_concurrent) {
    const size_t file_size = 8 * MB;
    const int delay_ms = 30;

    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data, delay_ms);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    // Create 2 IO ranges - both should read in parallel
    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 4 * MB, true);
    ranges.emplace_back(4 * MB, 4 * MB, true);

    auto start = std::chrono::steady_clock::now();
    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    // get_bytes() blocks until parallel read completes
    const uint8_t* buffer = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buffer, 0, 4 * MB, nullptr));
    ASSERT_OK(sb_stream->get_bytes(&buffer, 4 * MB, 4 * MB, nullptr));

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

    // If sequential: 2 * 30ms = 60ms. If parallel: ~30ms.
    // Allow overhead but should be less than sequential time.
    ASSERT_LT(elapsed_ms, delay_ms * 2) << "Parallel reads should complete faster than sequential";
}

// Test: Direct IO fallback works when not using set_io_ranges
TEST_F(SharedBufferedInputStreamTest, test_direct_io_fallback) {
    const size_t file_size = 4 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    // Read without setting IO ranges - should use direct read
    std::string result(file_size, '\0');
    ASSERT_OK(sb_stream->read_at_fully(0, result.data(), file_size));

    ASSERT_EQ(data, result);
    ASSERT_EQ(1, sb_stream->direct_io_count());
}

// Test: Shared buffers are created correctly with coalescing
TEST_F(SharedBufferedInputStreamTest, test_coalesce_io_ranges) {
    const size_t file_size = 16 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    // Set coalesce options
    SharedBufferedInputStream::CoalesceOptions opts;
    opts.max_dist_size = 1 * MB;    // Coalesce ranges within 1MB
    opts.max_buffer_size = 8 * MB;  // Max buffer 8MB
    sb_stream->set_coalesce_options(opts);

    // Create ranges that should be coalesced (close together)
    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 1 * MB, true);
    ranges.emplace_back(1 * MB + 100, 1 * MB, true);  // 100 bytes gap - should coalesce
    ranges.emplace_back(10 * MB, 1 * MB, true);       // Far away - separate buffer

    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    // First two ranges should be in same buffer
    auto sb1 = sb_stream->find_shared_buffer(0, 1 * MB);
    ASSERT_OK(sb1);
    auto sb2 = sb_stream->find_shared_buffer(1 * MB + 100, 1 * MB);
    ASSERT_OK(sb2);

    // They should be the same shared buffer (coalesced)
    ASSERT_EQ(sb1.value().get(), sb2.value().get());

    // Third range should be separate
    auto sb3 = sb_stream->find_shared_buffer(10 * MB, 1 * MB);
    ASSERT_OK(sb3);
    ASSERT_NE(sb1.value().get(), sb3.value().get());
}

// Test: 8 parallel reads with sliding window (8 workers per file)
TEST_F(SharedBufferedInputStreamTest, test_parallel_read_sliding_window) {
    const size_t file_size = 32 * MB;
    const int delay_ms = 50;

    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data, delay_ms);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    // Create 8 IO ranges of 4MB each
    std::vector<SharedBufferedInputStream::IORange> ranges;
    for (int i = 0; i < 8; ++i) {
        ranges.emplace_back(i * 4 * MB, 4 * MB, true);
    }

    auto start = std::chrono::steady_clock::now();
    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    // get_bytes() blocks until each range's parallel read completes
    for (int i = 0; i < 8; ++i) {
        const uint8_t* buffer = nullptr;
        ASSERT_OK(sb_stream->get_bytes(&buffer, i * 4 * MB, 4 * MB, nullptr));
    }

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

    // With 8 parallel workers, all 8 reads with 50ms delay should complete in ~50ms
    // Sequential would be 8 * 50ms = 400ms
    ASSERT_LT(elapsed_ms, 4 * delay_ms) << "8 parallel reads should complete much faster than sequential";
}

// Test: Release clears buffers
TEST_F(SharedBufferedInputStreamTest, test_release_clears_buffers) {
    const size_t file_size = 4 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    // Set IO ranges
    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 2 * MB, true);
    ranges.emplace_back(2 * MB, 2 * MB, true);
    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    // Read data first (this waits for parallel reads to complete)
    const uint8_t* buffer = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buffer, 0, 2 * MB, nullptr));

    // Release everything
    sb_stream->release();

    // Now find_shared_buffer should fail
    auto sb = sb_stream->find_shared_buffer(0, 1 * MB);
    ASSERT_FALSE(sb.ok());
}

// Test: Read statistics are tracked correctly
TEST_F(SharedBufferedInputStreamTest, test_io_statistics) {
    const size_t file_size = 4 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    // Set IO ranges
    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 2 * MB, true);
    ranges.emplace_back(2 * MB, 2 * MB, true);
    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    // Read both ranges - get_bytes() waits for parallel reads
    const uint8_t* buffer = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buffer, 0, 2 * MB, nullptr));
    ASSERT_OK(sb_stream->get_bytes(&buffer, 2 * MB, 2 * MB, nullptr));

    // Verify IO statistics
    ASSERT_EQ(2, sb_stream->shared_io_count());
    ASSERT_EQ(4 * MB, sb_stream->shared_io_bytes());
    ASSERT_EQ(0, sb_stream->direct_io_count());
}

// ==================== Chunked Parallel Read Tests ====================

// Test: Large buffer (>8MB) is split into chunks and read in parallel
TEST_F(SharedBufferedInputStreamTest, test_chunked_parallel_read) {
    const size_t file_size = 32 * MB;
    const int delay_ms = 50;

    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data, delay_ms);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    // Set max_buffer_size to 8MB (chunk size)
    SharedBufferedInputStream::CoalesceOptions opts;
    opts.max_buffer_size = 8 * MB;
    sb_stream->set_coalesce_options(opts);

    // Create one large 24MB range - should be split into 3 chunks of 8MB
    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 24 * MB, true);

    auto start = std::chrono::steady_clock::now();
    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    // get_bytes() waits for all 3 chunks to complete
    const uint8_t* buffer = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buffer, 0, 24 * MB, nullptr));

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

    // With 3 parallel chunk reads (each 50ms), should complete in ~50ms
    // Sequential would be 3 * 50ms = 150ms
    ASSERT_LT(elapsed_ms, 2 * delay_ms) << "Chunked parallel reads should be faster than sequential";

    // Verify data integrity
    for (size_t i = 0; i < 24 * MB; ++i) {
        ASSERT_EQ(data[i], static_cast<char>(buffer[i])) << "Mismatch at offset " << i;
    }

    // Verify mock was called 3 times (3 chunks)
    ASSERT_EQ(3, underlying->read_count());
}

// Test: Verify chunk boundaries are correct
// Also tests smart chunking: don't create tiny last chunks (< chunk_size/2)
TEST_F(SharedBufferedInputStreamTest, test_chunked_read_boundaries) {
    const size_t file_size = 20 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    SharedBufferedInputStream::CoalesceOptions opts;
    opts.max_buffer_size = 8 * MB;
    sb_stream->set_coalesce_options(opts);

    // 17MB range → 2 chunks: 8MB + 9MB (not 8+8+1, because 1MB < 4MB threshold)
    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 17 * MB, true);

    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    const uint8_t* buffer = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buffer, 0, 17 * MB, nullptr));

    // Verify data at chunk boundary (8MB)
    ASSERT_EQ(data[8 * MB - 1], static_cast<char>(buffer[8 * MB - 1]));   // end of chunk 1
    ASSERT_EQ(data[8 * MB], static_cast<char>(buffer[8 * MB]));           // start of chunk 2
    ASSERT_EQ(data[17 * MB - 1], static_cast<char>(buffer[17 * MB - 1])); // end of chunk 2

    // 2 read_at_fully calls (2 chunks: 8MB + 9MB)
    ASSERT_EQ(2, underlying->read_count());
}

// Test: Small buffer (<= max_buffer_size * 1.5) uses single chunk
TEST_F(SharedBufferedInputStreamTest, test_small_buffer_single_chunk) {
    const size_t file_size = 16 * MB;
    std::string data = generate_test_data(file_size);
    auto underlying = std::make_shared<MockDelayedInputStream>(data);

    auto sb_stream = std::make_shared<SharedBufferedInputStream>(underlying, "test", file_size);

    SharedBufferedInputStream::CoalesceOptions opts;
    opts.max_buffer_size = 8 * MB;
    sb_stream->set_coalesce_options(opts);

    // 12MB range <= 8MB * 1.5 = 12MB → 1 chunk (smart chunking avoids 8+4 split)
    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, 12 * MB, true);

    ASSERT_OK(sb_stream->set_io_ranges(ranges, true));

    const uint8_t* buffer = nullptr;
    ASSERT_OK(sb_stream->get_bytes(&buffer, 0, 12 * MB, nullptr));

    // Verify data
    for (size_t i = 0; i < 12 * MB; ++i) {
        ASSERT_EQ(data[i], static_cast<char>(buffer[i])) << "Mismatch at offset " << i;
    }

    // Only 1 read_at_fully call (1 chunk, not 8+4)
    ASSERT_EQ(1, underlying->read_count());
}

} // namespace starrocks::io