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

#include "formats/parquet/statistics_helper.h"

#include <gtest/gtest.h>

#include <string>
#include <tuple>
#include <vector>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "formats/parquet/schema.h"
#include "gutil/casts.h"
#include "runtime/types.h"

namespace starrocks::parquet {

class StatisticsHelperTest : public testing::Test {};

TEST_F(StatisticsHelperTest, DecodeBooleanMinMax) {
    ParquetField field;
    field.physical_type = tparquet::Type::type::BOOLEAN;

    TypeDescriptor type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);

    MutableColumnPtr min_column = ColumnHelper::create_column(type, true);
    MutableColumnPtr max_column = ColumnHelper::create_column(type, true);

    // Parquet stats encode BOOLEAN as a single byte: 0x00 = false, 0x01 = true.
    std::vector<std::string> min_values{std::string("\x00", 1), std::string("\x01", 1)};
    std::vector<std::string> max_values{std::string("\x01", 1), std::string("\x01", 1)};
    std::vector<bool> null_pages{false, false};

    ASSERT_TRUE(
            StatisticsHelper::decode_value_into_column(min_column, min_values, null_pages, type, &field, "UTC").ok());
    ASSERT_TRUE(
            StatisticsHelper::decode_value_into_column(max_column, max_values, null_pages, type, &field, "UTC").ok());

    ASSERT_EQ(2, min_column->size());
    ASSERT_EQ(2, max_column->size());
    EXPECT_EQ(0, min_column->get(0).get_uint8());
    EXPECT_EQ(1, min_column->get(1).get_uint8());
    EXPECT_EQ(1, max_column->get(0).get_uint8());
    EXPECT_EQ(1, max_column->get(1).get_uint8());
}

TEST_F(StatisticsHelperTest, DecodeBooleanIgnoresPaddingBits) {
    ParquetField field;
    field.physical_type = tparquet::Type::type::BOOLEAN;

    TypeDescriptor type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);
    MutableColumnPtr column = ColumnHelper::create_column(type, true);

    // PLAIN BOOLEAN encoding is bit-packed LSB first, so only bit 0 carries the value.
    // bits 1-7 are unused padding.
    // A byte such as 0x02 or 0xFE must decode to false, and 0x03 or 0xFF to true,
    // regardless of the padding bits.
    std::vector<std::string> values{std::string("\x02", 1), std::string("\x03", 1), std::string("\xFE", 1),
                                    std::string("\xFF", 1)};
    std::vector<bool> null_pages{false, false, false, false};

    ASSERT_TRUE(StatisticsHelper::decode_value_into_column(column, values, null_pages, type, &field, "UTC").ok());

    ASSERT_EQ(4, column->size());
    EXPECT_EQ(0, column->get(0).get_uint8());
    EXPECT_EQ(1, column->get(1).get_uint8());
    EXPECT_EQ(0, column->get(2).get_uint8());
    EXPECT_EQ(1, column->get(3).get_uint8());
}

TEST_F(StatisticsHelperTest, DecodeBooleanNullPage) {
    ParquetField field;
    field.physical_type = tparquet::Type::type::BOOLEAN;

    TypeDescriptor type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);
    MutableColumnPtr column = ColumnHelper::create_column(type, true);

    std::vector<std::string> values{std::string(), std::string("\x01", 1)};
    std::vector<bool> null_pages{true, false};

    ASSERT_TRUE(StatisticsHelper::decode_value_into_column(column, values, null_pages, type, &field, "UTC").ok());

    ASSERT_EQ(2, column->size());
    EXPECT_TRUE(column->is_null(0));
    EXPECT_FALSE(column->is_null(1));
    EXPECT_EQ(1, column->get(1).get_uint8());
}

TEST_F(StatisticsHelperTest, DecodeBooleanEmptyValueReturnsCorruption) {
    ParquetField field;
    field.physical_type = tparquet::Type::type::BOOLEAN;

    TypeDescriptor type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);
    MutableColumnPtr column = ColumnHelper::create_column(type, true);

    std::vector<std::string> values{std::string()};
    std::vector<bool> null_pages{false};

    auto st = StatisticsHelper::decode_value_into_column(column, values, null_pages, type, &field, "UTC");
    EXPECT_TRUE(st.is_corruption()) << st.to_string();
}

// A statistic is a few bytes from the footer or the page index, and the decoder copies the size of the type
// from it. A value that is shorter must come back as an error and not be read past its end.
TEST_F(StatisticsHelperTest, DecodeShortFixedWidthValueReturnsCorruption) {
    for (auto [physical_type, logical_type, width] : std::vector<std::tuple<tparquet::Type::type, LogicalType, size_t>>{
                 {tparquet::Type::INT32, LogicalType::TYPE_INT, 4},
                 {tparquet::Type::INT64, LogicalType::TYPE_BIGINT, 8}}) {
        ParquetField field;
        field.physical_type = physical_type;
        TypeDescriptor type = TypeDescriptor::from_logical_type(logical_type);

        for (size_t length : {size_t(0), width - 1}) {
            MutableColumnPtr column = ColumnHelper::create_column(type, true);
            std::vector<std::string> values{std::string(length, '\x01')};
            std::vector<bool> null_pages{false};
            auto st = StatisticsHelper::decode_value_into_column(column, values, null_pages, type, &field, "UTC");
            EXPECT_TRUE(st.is_corruption()) << width << " " << length << " " << st.to_string();
        }

        MutableColumnPtr column = ColumnHelper::create_column(type, true);
        std::vector<std::string> values{std::string(width, '\x01')};
        std::vector<bool> null_pages{false};
        EXPECT_TRUE(StatisticsHelper::decode_value_into_column(column, values, null_pages, type, &field, "UTC").ok());
    }
}

// The decimal converter walks the bytes of a FIXED_LEN_BYTE_ARRAY column with a stride of the type length, so
// a statistic of any other length would shift the values after it.
TEST_F(StatisticsHelperTest, DecodeFixedLenByteArrayDecimalValueOfOtherLengthReturnsCorruption) {
    ParquetField field{};
    field.physical_type = tparquet::Type::FIXED_LEN_BYTE_ARRAY;
    field.type_length = 4;
    field.scale = 0;
    field.precision = 38;
    TypeDescriptor type = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 0);

    const std::string seven("\x00\x00\x00\x07", 4);
    {
        MutableColumnPtr column = ColumnHelper::create_column(type, true);
        std::vector<std::string> values{std::string(), seven};
        std::vector<bool> null_pages{true, false};
        ASSERT_TRUE(StatisticsHelper::decode_value_into_column(column, values, null_pages, type, &field, "UTC").ok());
        ASSERT_EQ(2, column->size());
        EXPECT_TRUE(column->is_null(0));
        auto* nullable = down_cast<NullableColumn*>(column.get());
        auto* data = down_cast<Decimal128Column*>(nullable->data_column_raw_ptr());
        EXPECT_EQ(7, static_cast<int64_t>(data->get_data()[1]));
    }
    for (size_t length : {size_t(1), size_t(3), size_t(5), size_t(16)}) {
        MutableColumnPtr column = ColumnHelper::create_column(type, true);
        std::vector<std::string> values{seven, std::string(length, '\x01')};
        std::vector<bool> null_pages{false, false};
        auto st = StatisticsHelper::decode_value_into_column(column, values, null_pages, type, &field, "UTC");
        EXPECT_TRUE(st.is_corruption()) << length << " " << st.to_string();
    }
}

} // namespace starrocks::parquet
