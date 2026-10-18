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

#include "connector/deletion_vector/deletion_vector.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <utility>
#include <vector>

#include "common/config.h"
#include "fs/fs.h"
#include "testutil/scoped_updater.h"
#include "util/base85.h"
#include "util/runtime_profile.h"

namespace starrocks {

class DeletionVectorTest : public testing::Test {
public:
    void SetUp() override { dv = std::make_shared<DeletionVector>(params); }

    HdfsScannerContext params;
    std::shared_ptr<DeletionVector> dv;
};

TEST_F(DeletionVectorTest, inlineDeletionVectorTest) {
    std::string encoded_inline_dv = "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L";
    SkipRowsContextPtr skipRowsContext = std::make_shared<SkipRowsContext>();
    Status status = dv->deserialized_inline_dv(encoded_inline_dv, skipRowsContext);
    ASSERT_TRUE(status.ok());

    uint64_t cardinality = skipRowsContext->deletion_bitmap->get_cardinality();
    vector<uint64_t> bitmap_vector(cardinality);
    skipRowsContext->deletion_bitmap->to_array(bitmap_vector);
    ASSERT_EQ(6, bitmap_vector.size());
    std::stringstream ss;
    for (int64_t rowid : bitmap_vector) {
        ss << rowid << " ";
    }
    ASSERT_EQ("3 4 7 11 18 29 ", ss.str());
}

TEST_F(DeletionVectorTest, absolutePathTest) {
    // test relative path
    params.table_specific.deletion_vector_descriptor = std::make_shared<TDeletionVectorDescriptor>();
    params.table_specific.deletion_vector_descriptor->__set_storageType("u");
    params.table_specific.deletion_vector_descriptor->__set_pathOrInlineDv("ab^-aqEH.-t@S}K{vb[*k^");
    dv = std::make_shared<DeletionVector>(params);

    std::string table_location = "s3://mytable";
    StatusOr<std::string> absolute_path = dv->get_absolute_path(table_location);
    ASSERT_TRUE(absolute_path.ok());
    ASSERT_EQ("s3://mytable/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", absolute_path.value());
    // test absolute path
    params.table_specific.deletion_vector_descriptor = std::make_shared<TDeletionVectorDescriptor>();
    params.table_specific.deletion_vector_descriptor->__set_storageType("p");
    params.table_specific.deletion_vector_descriptor->__set_pathOrInlineDv(
            "s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin");
    dv = std::make_shared<DeletionVector>(params);
    absolute_path = dv->get_absolute_path(table_location);
    ASSERT_TRUE(absolute_path.ok());
    ASSERT_EQ("s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", absolute_path.value());
}

using IORanges = std::vector<std::pair<int64_t, int64_t>>;

static IORanges build_io_ranges(int64_t offset, int64_t size_in_bytes, int64_t max_range_size) {
    IORanges ranges;
    for (const auto& range : DeletionVector::build_io_ranges(offset, size_in_bytes, max_range_size)) {
        ranges.emplace_back(range.offset, range.size);
    }
    return ranges;
}

// The reads of a deletion vector are the size field and then size_in_bytes bytes, so the ranges have to be exactly that
// region whatever the offset is, in pieces of at most the limit.
TEST_F(DeletionVectorTest, ioRangesCoverTheReadRegion) {
    constexpr int64_t kMaxBufferSize = 8 * 1024 * 1024;
    // The offset is above the size of the region, as the offset of a deletion vector is in a real file.
    ASSERT_EQ((IORanges{{1000, 48}}), build_io_ranges(1000, 44, kMaxBufferSize));
    // The offset is below the size of the region.
    ASSERT_EQ((IORanges{{10, 48}}), build_io_ranges(10, 44, kMaxBufferSize));
    ASSERT_EQ((IORanges{{0, 48}}), build_io_ranges(0, 44, kMaxBufferSize));
    // The region is exactly one piece, and one byte more than one piece.
    ASSERT_EQ((IORanges{{1000, 48}}), build_io_ranges(1000, 44, 48));
    ASSERT_EQ((IORanges{{1000, 47}, {1047, 1}}), build_io_ranges(1000, 44, 47));
    // The region is a multiple of the limit, and has a remainder.
    ASSERT_EQ((IORanges{{1000, 16}, {1016, 16}, {1032, 16}}), build_io_ranges(1000, 44, 16));
    ASSERT_EQ((IORanges{{1000, 20}, {1020, 20}, {1040, 8}}), build_io_ranges(1000, 44, 20));
    // A deletion vector without bytes still has the size field.
    ASSERT_EQ((IORanges{{1000, 4}}), build_io_ranges(1000, 0, 16));
    // A negative size has nothing to read.
    ASSERT_TRUE(build_io_ranges(1000, -4, 16).empty());
    ASSERT_TRUE(build_io_ranges(1000, -100, 16).empty());
    // A limit that is not positive does not split the region, and does not loop.
    ASSERT_EQ((IORanges{{1000, 48}}), build_io_ranges(1000, 44, 0));
    ASSERT_EQ((IORanges{{1000, 48}}), build_io_ranges(1000, 44, -1));

    // The ranges follow each other without a gap, start at the offset, end at the end of the region, and none of them
    // is larger than the limit.
    for (int64_t offset : {0, 7, 1000}) {
        for (int64_t size_in_bytes = 0; size_in_bytes <= 100; ++size_in_bytes) {
            for (int64_t max_range_size = 1; max_range_size <= 60; ++max_range_size) {
                const IORanges ranges = build_io_ranges(offset, size_in_bytes, max_range_size);
                ASSERT_FALSE(ranges.empty());
                int64_t expected_begin = offset;
                for (const auto& [begin, size] : ranges) {
                    ASSERT_EQ(expected_begin, begin);
                    ASSERT_GT(size, 0);
                    ASSERT_LE(size, max_range_size);
                    expected_begin = begin + size;
                }
                ASSERT_EQ(offset + DeletionVector::DV_SIZE_LENGTH + size_in_bytes, expected_begin);
            }
        }
    }
}

// A deletion vector that is stored in a file, behind other bytes.
class DeletionVectorFileTest : public testing::Test {
public:
    void SetUp() override {
        _dir = std::filesystem::temp_directory_path() /
               ("deletion_vector_test_" + std::to_string(getpid()) + "_" +
                ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::create_directories(_dir);

        // the decoded inline vector is the magic number and the bitmap of the rows 3 4 7 11 18 29
        auto decoded = base85_decode("^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L");
        ASSERT_TRUE(decoded.ok());
        _magic_and_bitmap = decoded.value();
    }

    void TearDown() override { std::filesystem::remove_all(_dir); }

    // Writes `offset` bytes, the size field (big endian), the magic number and the bitmap, and the checksum that
    // follows them, and reads the vector the way a scan does. The counters of the profile tell what the reads cost.
    void read_deletion_vector(int64_t offset, std::string* rows) {
        const int32_t size_in_bytes = _magic_and_bitmap.size();
        std::string file(offset, 'x');
        for (int shift = 24; shift >= 0; shift -= 8) {
            file.push_back(static_cast<char>((size_in_bytes >> shift) & 0xff));
        }
        file += _magic_and_bitmap;
        file.append(4, 'c');
        const std::string path = (_dir / "deletion_vector.bin").string();
        {
            std::ofstream out(path, std::ios::binary);
            out.write(file.data(), file.size());
        }

        _profile = std::make_unique<RuntimeProfile>("deletion_vector_test");
        _ctx = std::make_unique<HdfsScannerContext>();
        _ctx->fs = FileSystem::Default();
        _ctx->profile.runtime_profile = _profile.get();
        auto descriptor = std::make_shared<TDeletionVectorDescriptor>();
        descriptor->__set_storageType("p");
        descriptor->__set_pathOrInlineDv(path);
        descriptor->__set_offset(offset);
        descriptor->__set_sizeInBytes(size_in_bytes);
        _ctx->table_specific.deletion_vector_descriptor = descriptor;

        DeletionVector deletion_vector(*_ctx);
        auto skip_rows_ctx = std::make_shared<SkipRowsContext>();
        ASSERT_TRUE(deletion_vector.fill_row_indexes(skip_rows_ctx).ok());

        std::vector<uint64_t> row_ids(skip_rows_ctx->deletion_bitmap->get_cardinality());
        skip_rows_ctx->deletion_bitmap->to_array(row_ids);
        rows->clear();
        for (uint64_t row_id : row_ids) {
            *rows += std::to_string(row_id) + " ";
        }
    }

    int64_t counter(const std::string& name) {
        auto* profile_counter = _profile->get_counter(name);
        return profile_counter == nullptr ? -1 : profile_counter->value();
    }

    std::filesystem::path _dir;
    std::string _magic_and_bitmap;
    std::unique_ptr<HdfsScannerContext> _ctx;
    std::unique_ptr<RuntimeProfile> _profile;
};

// The scan reads the size field, the magic number and the bitmap one after the other. We expect the three reads to be
// served by one shared buffer, so the file is read once and no read is direct, wherever the vector is in the file.
TEST_F(DeletionVectorFileTest, readsOfTheVectorShareOneBuffer) {
    for (int64_t offset : {0, 5, 1000, 100000}) {
        std::string rows;
        ASSERT_NO_FATAL_FAILURE(read_deletion_vector(offset, &rows));
        ASSERT_EQ("3 4 7 11 18 29 ", rows) << offset;

        ASSERT_EQ(3, counter("DV_AppIOCounter")) << offset;
        ASSERT_EQ(0, counter("DV_DirectIOCount")) << offset;
        ASSERT_EQ(1, counter("DV_SharedIOCount")) << offset;
        ASSERT_EQ(1, counter("DV_FSIOCounter")) << offset;
        // the buffer is the region of the reads and nothing else
        const int64_t region_size = DeletionVector::DV_SIZE_LENGTH + static_cast<int64_t>(_magic_and_bitmap.size());
        ASSERT_EQ(region_size, counter("DV_SharedIOBytes")) << offset;
        ASSERT_EQ(region_size, counter("DV_FSIOBytesRead")) << offset;
    }
}

// A deletion vector that is larger than a piece: the size field and the magic number are served by the first piece,
// and the bitmap spans two pieces, so it is read directly. The vector is right, and the file is not read beyond
// the first piece and the bitmap.
TEST_F(DeletionVectorFileTest, readsOfALargeVectorStayInsideItsRegion) {
    SCOPED_UPDATE(int32_t, config::io_coalesce_read_max_buffer_size, 20);
    std::string rows;
    ASSERT_NO_FATAL_FAILURE(read_deletion_vector(1000, &rows));
    ASSERT_EQ("3 4 7 11 18 29 ", rows);

    ASSERT_EQ(1, counter("DV_SharedIOCount"));
    ASSERT_EQ(20, counter("DV_SharedIOBytes"));
    ASSERT_EQ(1, counter("DV_DirectIOCount"));
    ASSERT_EQ(static_cast<int64_t>(_magic_and_bitmap.size()) - DeletionVector::MAGIC_NUMBER_LENGTH,
              counter("DV_DirectIOBytes"));
}

} // namespace starrocks