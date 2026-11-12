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

#include "formats/parquet/column_converter.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <limits>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/encoding_plain.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/statistics_helper.h"
#include "fs/fs.h"
#include "parquet_test_util/util.h"
#include "runtime/descriptor_helper.h"
#include "types/date_value.h"
#include "util/decimal_types.h"

namespace starrocks::parquet {

static HdfsScannerStats g_hdfs_stats{};

class ColumnConverterTest : public testing::Test {
public:
    ColumnConverterTest() = default;

    ~ColumnConverterTest() override = default;

protected:
    std::unique_ptr<RandomAccessFile> _create_file(const std::string& file_path) {
        return *FileSystem::Default()->new_random_access_file(file_path);
    }

    HdfsScannerContext* _create_scan_context() {
        auto* ctx = _pool.add(new HdfsScannerContext());
        auto* lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));

        ctx->lazy_column_coalesce_counter = lazy_column_coalesce_counter;
        ctx->timezone = "Asia/Shanghai";
        ctx->stats = &g_hdfs_stats;
        return ctx;
    }

    THdfsScanRange* _create_scan_range(const std::string& file_path, size_t scan_length = 0) {
        auto* scan_range = _pool.add(new THdfsScanRange());
        scan_range->relative_path = file_path;
        scan_range->file_length = std::filesystem::file_size(file_path);
        scan_range->offset = 4;
        scan_range->length = scan_length > 0 ? scan_length : scan_range->file_length;
        return scan_range;
    }

    static void check_chunk_values(ChunkPtr& chunk, const std::string& expected_value) {
        chunk->check_or_die();
        size_t mid = chunk->num_rows() / 2;
        for (size_t i = 0; i < chunk->num_rows(); i++) {
            if (i == mid) {
                EXPECT_EQ("[NULL]", chunk->debug_row(i));
            } else {
                EXPECT_EQ(expected_value, chunk->debug_row(i));
            }
        }
    }

    void check(const std::string& filepath, const TypeDescriptor& col_type, const std::string& col_name,
               const std::string& expected_value, const size_t expected_rows, bool is_failed = false,
               const std::string& timezone = "Asia/Shanghai") {
        auto file = _create_file(filepath);
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(filepath));

        // --------------init context---------------
        auto ctx = _create_scan_context();
        ctx->timezone = timezone;

        Utils::SlotDesc slot_descs[] = {{col_name, col_type}, {""}};

        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
        ctx->slot_descs = tuple_desc->slots();
        ctx->scan_range = (_create_scan_range(filepath));
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        if (is_failed) {
            EXPECT_TRUE(!status.ok());
            return;
        }
        if (!status.ok()) {
            std::cout << status.message() << std::endl;
        }
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(col_type, true), chunk->num_columns());

        status = file_reader->get_next(&chunk);
        ASSERT_TRUE(status.ok());

        size_t total_rows = chunk->num_rows();

        check_chunk_values(chunk, expected_value);

        while (!status.is_end_of_file()) {
            chunk->reset();
            status = file_reader->get_next(&chunk);
            if (!status.ok() && !status.is_end_of_file()) {
                std::cout << status.message() << std::endl;
                break;
            }
            check_chunk_values(chunk, expected_value);
            total_rows += chunk->num_rows();
        }

        EXPECT_EQ(expected_rows, total_rows);
    }

    RuntimeState* _runtime_state = nullptr;
    ObjectPool _pool;
    HdfsScannerContext _scanner_ctx;
};

TEST_F(ColumnConverterTest, TestByteArrayTest) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/byte_array.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "string";
        // string to char
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_CHAR);
            check(file_path, col_type, col_name, "['string_smith']", expected_rows);
        }
        // string to varchar
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            check(file_path, col_type, col_name, "['string_smith']", expected_rows);
        }
    }

    {
        const std::string col_name = "enum";
        // enum to char
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_CHAR);
            check(file_path, col_type, col_name, "['enum_smith']", expected_rows);
        }
        // enum to varchar
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            check(file_path, col_type, col_name, "['enum_smith']", expected_rows);
        }
    }

    {
        const std::string col_name = "decimal_binary";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL);
            check(file_path, col_type, col_name, "[6.7]", expected_rows, true);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL32);
            check(file_path, col_type, col_name, "[6.7]", expected_rows, true);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL64);
            check(file_path, col_type, col_name, "[6.7]", expected_rows, true);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128);
            check(file_path, col_type, col_name, "[6.7]", expected_rows, true);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMALV2);
            check(file_path, col_type, col_name, "[6.7]", expected_rows, true);
        }
    }

    {
        const std::string col_name = "json";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            check(file_path, col_type, col_name, R"(['{"name": "smith"}'])", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_CHAR);
            check(file_path, col_type, col_name, R"(['{"name": "smith"}'])", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_JSON);
            check(file_path, col_type, col_name, R"(['{"name": "smith"}'])", expected_rows, true);
        }
    }

    {
        const std::string col_name = "bson";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            check(file_path, col_type, col_name, R"(['{"name": "smith"}'])", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_CHAR);
            check(file_path, col_type, col_name, R"(['{"name": "smith"}'])", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_JSON);
            check(file_path, col_type, col_name, R"(['{"name": "smith"}'])", expected_rows, true);
        }
    }
}

TEST_F(ColumnConverterTest, Int32Test) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/int32.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "int8";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_TINYINT);
            check(file_path, col_type, col_name, "[-5]", expected_rows);
        }
    }
    {
        const std::string col_name = "int16";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_SMALLINT);
            check(file_path, col_type, col_name, "[-998]", expected_rows);
        }
    }
    {
        const std::string col_name = "int32";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
            check(file_path, col_type, col_name, "[-99998]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
            check(file_path, col_type, col_name, "[-99998]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DOUBLE);
            check(file_path, col_type, col_name, "[-99998]", expected_rows);
        }
    }
    {
        const std::string col_name = "time_millis";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_TIME);
            check(file_path, col_type, col_name, "[3600]", expected_rows);
        }
    }
    {
        const std::string col_name = "uint8";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_TINYINT);
            check(file_path, col_type, col_name, "[5]", expected_rows);
        }
    }
    {
        const std::string col_name = "uint16";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_SMALLINT);
            check(file_path, col_type, col_name, "[6767]", expected_rows);
        }
    }
    {
        const std::string col_name = "uint32";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
            check(file_path, col_type, col_name, "[67676767]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
            check(file_path, col_type, col_name, "[67676767]", expected_rows);
        }
    }
    {
        const std::string col_name = "date";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATE);
            check(file_path, col_type, col_name, "[2023-04-25]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2023-04-25 00:00:00]", expected_rows);
        }
    }
    {
        const std::string col_name = "decimal32";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL, -1, 9, 2);
            check(file_path, col_type, col_name, "[77.58]", expected_rows, true);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMALV2, -1, 9, 2);
            check(file_path, col_type, col_name, "[77.58]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL32, -1, 9, 2);
            check(file_path, col_type, col_name, "[77.58]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL64, -1, 9, 2);
            check(file_path, col_type, col_name, "[77.58]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 9, 1);
            check(file_path, col_type, col_name, "[77.5]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 9, 2);
            check(file_path, col_type, col_name, "[77.58]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 9, 3);
            check(file_path, col_type, col_name, "[77.580]", expected_rows);
        }
    }
}

TEST_F(ColumnConverterTest, Int64Test) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/int64.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "int64";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_TINYINT);
            check(file_path, col_type, col_name, "[78]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_SMALLINT);
            check(file_path, col_type, col_name, "[-25010]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
            check(file_path, col_type, col_name, "[-7758258]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
            check(file_path, col_type, col_name, "[-7758258]", expected_rows);
        }
    }
    {
        const std::string col_name = "uint64";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
            check(file_path, col_type, col_name, "[7758258]", expected_rows);
        }
    }
    {
        const std::string col_name = "decimal64";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMALV2, -1);
            check(file_path, col_type, col_name, "[77.58258]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL32, -1, 9, 5);
            check(file_path, col_type, col_name, "[77.58258]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL64, -1, 18, 5);
            check(file_path, col_type, col_name, "[77.58258]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 18, 5);
            check(file_path, col_type, col_name, "[77.58258]", expected_rows);
        }
    }
    {
        const std::string col_name = "time_micros";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_TIME);
            check(file_path, col_type, col_name, "[3600]", expected_rows);
        }
    }
    {
        const std::string col_name = "time_nanos";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_TIME);
            check(file_path, col_type, col_name, "[3.6e+06]", expected_rows);
        }
    }
    {
        const std::string col_name = "timestamp_millis";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2023-04-25 20:20:10]", expected_rows);
        }
    }
    {
        const std::string col_name = "timestamp_micros";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2023-04-25 20:20:10]", expected_rows);
        }
    }
    {
        const std::string col_name = "timestamp_nanos";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2023-04-25 20:20:10]", expected_rows);
        }
    }
}

TEST_F(ColumnConverterTest, FLBATest) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/fixed_len_byte_array.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "uuid";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            check(file_path, col_type, col_name, "['abcDeFGhijkLmnOp']", expected_rows);
        }
    }
    {
        const std::string col_name = "decimal_flba";
        // flba's decimal result maybe wrong
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL);
            check(file_path, col_type, col_name, "[680.96]", expected_rows, true);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMALV2, -1, 5, 2);
            check(file_path, col_type, col_name, "[682.56]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL32, -1, 5, 2);
            check(file_path, col_type, col_name, "[682.56]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL64, -1, 5, 2);
            check(file_path, col_type, col_name, "[682.56]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 5, 1);
            check(file_path, col_type, col_name, "[682.5]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 5, 2);
            check(file_path, col_type, col_name, "[682.56]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 5, 3);
            check(file_path, col_type, col_name, "[682.560]", expected_rows);
        }
    }
    {
        const std::string col_name = "interval";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_TIME);
            check(file_path, col_type, col_name, "[6809.6]", expected_rows, true);
        }
    }
}

// The conversion of a FLBA decimal reads type_length bytes of the source column for every non-null value.
// The type length comes from the footer and the values may come from corrupt statistics, so a column that
// holds fewer bytes than that must give an error instead of being read past its end.
TEST_F(ColumnConverterTest, FLBADecimalTypeLengthAndBytes) {
    const TypeDescriptor col_type = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 2);

    // An empty string in `values` is a NULL value.
    auto convert = [&](int32_t type_length, const std::vector<std::string>& values) -> Status {
        ParquetField field{};
        field.physical_type = tparquet::Type::FIXED_LEN_BYTE_ARRAY;
        field.type_length = type_length;
        field.scale = 2;
        field.precision = 38;

        std::unique_ptr<ColumnConverter> converter;
        RETURN_IF_ERROR(ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter));
        auto src = converter->create_src_column();
        bool appended = true;
        for (const auto& value : values) {
            if (value.empty()) {
                appended &= src->append_nulls(1);
            } else {
                appended &= src->append_strings(std::vector<Slice>{Slice(value)});
            }
        }
        if (!appended) {
            return Status::InternalError("append to the source column failed");
        }
        auto dst = ColumnHelper::create_column(col_type, true);
        return converter->convert(src.get(), dst.get());
    };

    const std::string value4(4, '\x01');
    ASSERT_TRUE(convert(4, {value4, value4}).ok());
    // a NULL value holds no bytes
    ASSERT_TRUE(convert(4, {value4, "", value4}).ok());
    // wider than the widest decimal: converted to NULL
    ASSERT_TRUE(convert(17, {std::string(17, '\x01')}).ok());

    // the type length is 4 but the values are shorter
    ASSERT_TRUE(convert(4, {std::string(2, '\x01')}).is_corruption());
    ASSERT_TRUE(convert(4, {value4, std::string(3, '\x01')}).is_corruption());
    ASSERT_TRUE(convert(16, {value4}).is_corruption());

    // the type length is not a width that the conversion supports
    ASSERT_TRUE(convert(0, {value4}).is_corruption());
    ASSERT_TRUE(convert(-1, {value4}).is_corruption());
    ASSERT_TRUE(convert(std::numeric_limits<int32_t>::min(), {value4}).is_corruption());
}

// The conversion of an INT32 to a DATE adds the epoch to every row. The data of a NULL row is not initialized
// and a corrupt file can hold any int32 as a day, so the sum can exceed int32. A build with UBSAN traps on a
// signed overflow, so the conversion must not overflow, and it must keep the NULL flags and the other rows.
TEST_F(ColumnConverterTest, Int32ToDateWithLargeValues) {
    ParquetField field{};
    field.physical_type = tparquet::Type::INT32;
    const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATE);
    std::unique_ptr<ColumnConverter> converter;
    ASSERT_TRUE(ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter).ok());

    // 2023-01-01, NULL with INT32_MAX, NULL with INT32_MIN, 1970-01-01, INT32_MAX, INT32_MIN
    const std::vector<int32_t> days{19358, std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::min(),
                                    0,     std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::min()};
    const std::vector<uint8_t> is_null{0, 1, 1, 0, 0, 0};
    auto data = FixedLengthColumn<int32_t>::create();
    auto nulls = NullColumn::create();
    for (size_t i = 0; i < days.size(); i++) {
        data->append(days[i]);
        nulls->append(is_null[i]);
    }
    auto src = NullableColumn::create(std::move(data), std::move(nulls));
    src->update_has_null();

    auto dst = ColumnHelper::create_column(col_type, true);
    ASSERT_TRUE(converter->convert(src.get(), dst.get()).ok());
    ASSERT_EQ(days.size(), dst->size());
    for (size_t i = 0; i < days.size(); i++) {
        EXPECT_EQ(is_null[i] != 0, dst->is_null(i)) << "row " << i;
    }
    EXPECT_EQ("2023-01-01", dst->get(0).get_date().to_string());
    EXPECT_EQ("1970-01-01", dst->get(3).get_date().to_string());
}

// The conversion to a decimal with a larger scale multiplies a value by a power of ten that can be 10^38, so
// the product of a large value exceeds int128. The conversion must not overflow, and it must keep the value
// that fits and the NULL flags.
TEST_F(ColumnConverterTest, PrimitiveToDecimalWithLargeScaleGap) {
    const TypeDescriptor col_type = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 38);
    ParquetField field{};
    field.physical_type = tparquet::Type::INT64;
    field.scale = 0;
    field.precision = 18;
    std::unique_ptr<ColumnConverter> converter;
    ASSERT_TRUE(ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter).ok());

    // 1, NULL with INT64_MAX, INT64_MAX, INT64_MIN
    const std::vector<int64_t> values{1, std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max(),
                                      std::numeric_limits<int64_t>::min()};
    const std::vector<uint8_t> is_null{0, 1, 0, 0};
    auto data = FixedLengthColumn<int64_t>::create();
    auto nulls = NullColumn::create();
    for (size_t i = 0; i < values.size(); i++) {
        data->append(values[i]);
        nulls->append(is_null[i]);
    }
    auto src = NullableColumn::create(std::move(data), std::move(nulls));
    src->update_has_null();

    auto dst = ColumnHelper::create_column(col_type, true);
    ASSERT_TRUE(converter->convert(src.get(), dst.get()).ok());
    ASSERT_EQ(values.size(), dst->size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(is_null[i] != 0, dst->is_null(i)) << "row " << i;
    }
    EXPECT_TRUE(dst->get(0).get_int128() == get_scale_factor<int128_t>(38));
}

// The same for a FIXED_LEN_BYTE_ARRAY decimal: the unscaled value is multiplied by the scale factor, and the
// value 0x7F..FF of the type length gives a product that exceeds int128.
TEST_F(ColumnConverterTest, FLBADecimalWithLargeScaleGap) {
    const TypeDescriptor col_type = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 38);
    // An empty string in the values is a NULL value.
    for (int32_t type_length : {4, 8, 9, 16}) {
        ParquetField field{};
        field.physical_type = tparquet::Type::FIXED_LEN_BYTE_ARRAY;
        field.type_length = type_length;
        field.scale = 0;
        field.precision = 38;
        std::unique_ptr<ColumnConverter> converter;
        ASSERT_TRUE(ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter).ok());

        std::string one(type_length, '\0');
        one[type_length - 1] = 1;
        std::string largest(type_length, '\xFF');
        largest[0] = '\x7F';
        std::string smallest(type_length, '\0');
        smallest[0] = '\x80';

        auto src = converter->create_src_column();
        for (const std::string& value : {one, std::string(), largest, smallest}) {
            if (value.empty()) {
                ASSERT_TRUE(src->append_nulls(1));
            } else {
                ASSERT_TRUE(src->append_strings(std::vector<Slice>{Slice(value)}));
            }
        }

        auto dst = ColumnHelper::create_column(col_type, true);
        ASSERT_TRUE(converter->convert(src.get(), dst.get()).ok()) << "type length " << type_length;
        ASSERT_EQ(4, dst->size());
        EXPECT_FALSE(dst->is_null(0));
        EXPECT_TRUE(dst->is_null(1));
        EXPECT_FALSE(dst->is_null(2));
        EXPECT_FALSE(dst->is_null(3));
        EXPECT_TRUE(dst->get(0).get_int128() == get_scale_factor<int128_t>(38)) << "type length " << type_length;
    }
}

// The scale factor of a decimal converter is read from a table of the powers 10^0..10^38, so a scale outside
// 0..76 and a negative precision must not reach the table. They give a Corruption that names the column and
// the file, for every physical type that holds a decimal. A scale above the precision is not an error.
TEST_F(ColumnConverterTest, DecimalFooterScaleAndPrecisionAreChecked) {
    struct Footer {
        int32_t scale;
        int32_t precision;
        bool valid;
    };
    const std::vector<Footer> footers{
            {0, 0, true},
            {2, 9, true},
            {9, 9, true},
            {38, 38, true},
            {76, 76, true},
            // precision 0 means that the footer has none
            {5, 0, true},
            // some writers put a scale above the precision
            {5, 3, true},
            {76, 1, true},
            {-1, 9, false},
            {-1, 0, false},
            {77, 80, false},
            {std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::max(), false},
            {std::numeric_limits<int32_t>::min(), 0, false},
            {1, -1, false},
    };
    const std::string file_name = "s3://bucket/dec.parquet";

    for (const TypeDescriptor& col_type : {TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 2),
                                           TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 2),
                                           TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 2),
                                           TypeDescriptor::from_logical_type(TYPE_DECIMALV2)}) {
        for (tparquet::Type::type physical_type :
             {tparquet::Type::INT32, tparquet::Type::INT64, tparquet::Type::FIXED_LEN_BYTE_ARRAY}) {
            for (const Footer& footer : footers) {
                ParquetField field{};
                field.name = "dec_col";
                field.physical_type = physical_type;
                field.type_length = 16;
                field.scale = footer.scale;
                field.precision = footer.precision;

                std::unique_ptr<ColumnConverter> converter;
                Status st = ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter, file_name);
                const std::string where = "scale " + std::to_string(footer.scale) + ", precision " +
                                          std::to_string(footer.precision) + ", physical type " +
                                          std::to_string(physical_type);
                if (footer.valid) {
                    EXPECT_TRUE(st.ok()) << where << ": " << st;
                } else {
                    EXPECT_TRUE(st.is_corruption()) << where << ": " << st;
                    EXPECT_NE(std::string::npos, st.to_string().find("dec_col")) << where << ": " << st;
                    EXPECT_NE(std::string::npos, st.to_string().find(file_name)) << where << ": " << st;
                }
            }
        }
    }

    // The scale of the destination decimal is checked too. TypeDescriptor::from_logical_type() gives a decimal the
    // default precision 27 and scale 9, which is a valid destination. A destination whose scale is outside 0..38,
    // or that has no scale (the default of a TypeDescriptor is -1), is rejected and never reaches the table.
    for (tparquet::Type::type physical_type :
         {tparquet::Type::INT32, tparquet::Type::INT64, tparquet::Type::FIXED_LEN_BYTE_ARRAY}) {
        ParquetField field{};
        field.name = "dec_col";
        field.physical_type = physical_type;
        field.type_length = 16;
        field.scale = 2;
        field.precision = 9;
        const std::string where = "physical type " + std::to_string(physical_type);

        std::unique_ptr<ColumnConverter> converter;
        Status st = ColumnConverterFactory::create_converter(field, TypeDescriptor::from_logical_type(TYPE_DECIMAL128),
                                                             "UTC", &converter, file_name);
        EXPECT_TRUE(st.ok()) << where << ": " << st;

        for (int32_t dst_scale :
             {-1, 39, 100, std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::min()}) {
            TypeDescriptor bad_type = TypeDescriptor::from_logical_type(TYPE_DECIMAL128);
            bad_type.scale = dst_scale;
            st = ColumnConverterFactory::create_converter(field, bad_type, "UTC", &converter, file_name);
            EXPECT_FALSE(st.ok()) << where << ", destination scale " << dst_scale;
            EXPECT_NE(std::string::npos, st.to_string().find("dec_col")) << where << ": " << st;
            EXPECT_NE(std::string::npos, st.to_string().find(file_name)) << where << ": " << st;
        }

        TypeDescriptor without_scale;
        without_scale.type = TYPE_DECIMAL128;
        st = ColumnConverterFactory::create_converter(field, without_scale, "UTC", &converter, file_name);
        EXPECT_FALSE(st.ok()) << where << ", destination without a scale";
    }
}

// The big-endian two's complement bytes of a decimal in a FIXED_LEN_BYTE_ARRAY of the given width.
static std::string decimal_flba_bytes(int128_t value, int width) {
    // A byte above the 16 bytes of the value repeats the sign.
    std::string bytes(width, value < 0 ? '\xFF' : '\0');
    for (int i = 0; i < width && i < 16; i++) {
        bytes[width - 1 - i] = static_cast<char>(static_cast<uint8_t>(static_cast<uint128_t>(value) >> (8 * i)));
    }
    return bytes;
}

// A footer with a scale above its precision is read, and its values are converted with that scale.
TEST_F(ColumnConverterTest, DecimalScaleAbovePrecisionIsRead) {
    const TypeDescriptor col_type = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 2);

    // INT64 with scale 5 and precision 3, to scale 2: 123456 / 10^3 = 123.
    {
        ParquetField field{};
        field.name = "dec_col";
        field.physical_type = tparquet::Type::INT64;
        field.scale = 5;
        field.precision = 3;
        std::unique_ptr<ColumnConverter> converter;
        ASSERT_TRUE(ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter).ok());

        auto data = FixedLengthColumn<int64_t>::create();
        auto nulls = NullColumn::create();
        for (int64_t value : {123456, 7}) {
            data->append(value);
            nulls->append(value == 7);
        }
        auto src = NullableColumn::create(std::move(data), std::move(nulls));
        src->update_has_null();
        auto dst = ColumnHelper::create_column(col_type, true);
        ASSERT_TRUE(converter->convert(src.get(), dst.get()).ok());
        ASSERT_EQ(2, dst->size());
        EXPECT_FALSE(dst->is_null(0));
        EXPECT_TRUE(dst->is_null(1));
        EXPECT_TRUE(dst->get(0).get_int128() == 123);
    }
    // FIXED_LEN_BYTE_ARRAY(4) with scale 4 and precision 2, to scale 2: 12345 / 10^2 = 123.
    {
        ParquetField field{};
        field.name = "dec_col";
        field.physical_type = tparquet::Type::FIXED_LEN_BYTE_ARRAY;
        field.type_length = 4;
        field.scale = 4;
        field.precision = 2;
        std::unique_ptr<ColumnConverter> converter;
        ASSERT_TRUE(ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter).ok());

        auto src = converter->create_src_column();
        const std::string value = decimal_flba_bytes(12345, 4);
        ASSERT_TRUE(src->append_strings(std::vector<Slice>{Slice(value)}));
        auto dst = ColumnHelper::create_column(col_type, true);
        ASSERT_TRUE(converter->convert(src.get(), dst.get()).ok());
        ASSERT_EQ(1, dst->size());
        EXPECT_FALSE(dst->is_null(0));
        EXPECT_TRUE(dst->get(0).get_int128() == 123);
    }
}

// A scale of the footer that is valid but more than 38 digits above the destination scale has no int128
// divisor, so every row of the column is NULL, as for a FIXED_LEN_BYTE_ARRAY wider than int128. A gap of
// exactly 38 digits is converted.
TEST_F(ColumnConverterTest, DecimalScaleGapBeyondInt128BecomesNull) {
    const TypeDescriptor col_type = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 0);
    const int128_t ten_pow_38 = get_scale_factor<int128_t>(38);

    auto create = [&](tparquet::Type::type physical_type, int32_t scale, int32_t type_length) {
        ParquetField field{};
        field.name = "dec_col";
        field.physical_type = physical_type;
        field.type_length = type_length;
        field.scale = scale;
        field.precision = scale;
        std::unique_ptr<ColumnConverter> converter;
        Status st = ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter);
        EXPECT_TRUE(st.ok()) << st;
        return converter;
    };

    auto convert_flba = [&](int32_t scale, int32_t type_length, const std::vector<std::string>& values) {
        auto converter = create(tparquet::Type::FIXED_LEN_BYTE_ARRAY, scale, type_length);
        // An empty string in the values is a NULL value.
        auto src = converter->create_src_column();
        for (const auto& value : values) {
            if (value.empty()) {
                EXPECT_TRUE(src->append_nulls(1));
            } else {
                EXPECT_TRUE(src->append_strings(std::vector<Slice>{Slice(value)}));
            }
        }
        auto dst = ColumnHelper::create_column(col_type, true);
        EXPECT_TRUE(converter->convert(src.get(), dst.get()).ok());
        return dst;
    };

    auto convert_int64 = [&](int32_t scale, const std::vector<int64_t>& values, const std::vector<uint8_t>& is_null) {
        auto converter = create(tparquet::Type::INT64, scale, 0);
        auto data = FixedLengthColumn<int64_t>::create();
        auto nulls = NullColumn::create();
        for (size_t i = 0; i < values.size(); i++) {
            data->append(values[i]);
            nulls->append(is_null[i]);
        }
        auto src = NullableColumn::create(std::move(data), std::move(nulls));
        src->update_has_null();
        auto dst = ColumnHelper::create_column(col_type, true);
        EXPECT_TRUE(converter->convert(src.get(), dst.get()).ok());
        return dst;
    };

    // FIXED_LEN_BYTE_ARRAY, gap of 38 digits: 10^38 / 10^38 = 1.
    {
        auto dst = convert_flba(38, 16, {decimal_flba_bytes(ten_pow_38, 16), "", decimal_flba_bytes(-ten_pow_38, 16)});
        ASSERT_EQ(3, dst->size());
        EXPECT_FALSE(dst->is_null(0));
        EXPECT_TRUE(dst->is_null(1));
        EXPECT_FALSE(dst->is_null(2));
        EXPECT_TRUE(dst->get(0).get_int128() == 1);
        EXPECT_TRUE(dst->get(2).get_int128() == -1);
    }
    // FIXED_LEN_BYTE_ARRAY of at most 16 bytes, gap of 39 digits: every row is NULL.
    {
        auto dst = convert_flba(39, 16, {decimal_flba_bytes(ten_pow_38, 16), "", decimal_flba_bytes(7, 16)});
        ASSERT_EQ(3, dst->size());
        for (size_t i = 0; i < 3; i++) {
            EXPECT_TRUE(dst->is_null(i)) << "row " << i;
        }
    }
    // FIXED_LEN_BYTE_ARRAY wider than int128, gap of 70 digits: every row is NULL.
    {
        auto dst = convert_flba(70, 32, {decimal_flba_bytes(7, 32), decimal_flba_bytes(-7, 32)});
        ASSERT_EQ(2, dst->size());
        EXPECT_TRUE(dst->is_null(0));
        EXPECT_TRUE(dst->is_null(1));
    }
    // INT64, gap of 38 digits: the quotient is converted, here 0.
    {
        auto dst = convert_int64(38, {1, 5, -5}, {0, 1, 0});
        ASSERT_EQ(3, dst->size());
        EXPECT_FALSE(dst->is_null(0));
        EXPECT_TRUE(dst->is_null(1));
        EXPECT_FALSE(dst->is_null(2));
        EXPECT_TRUE(dst->get(0).get_int128() == 0);
        EXPECT_TRUE(dst->get(2).get_int128() == 0);
    }
    // INT64, gap of 39 digits: every row is NULL.
    {
        auto dst = convert_int64(39, {1, 5, -5}, {0, 1, 0});
        ASSERT_EQ(3, dst->size());
        for (size_t i = 0; i < 3; i++) {
            EXPECT_TRUE(dst->is_null(i)) << "row " << i;
        }
    }
}

// A nullable FIXED_LEN_BYTE_ARRAY decimal column whose pages are dictionary encoded, with NULL rows before
// the rows that are not NULL. BinaryToDecimalConverter walks the bytes of the column with a stride of the
// type length and counts only the rows that are not NULL, so a NULL row must hold no byte, however the
// dictionary values were read into the column: directly, directly with a filter that drops a row, or as
// dictionary codes first.
TEST_F(ColumnConverterTest, DictEncodedNullableFLBADecimal) {
    const TypeDescriptor col_type = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 0);
    // the dictionary holds three values of 4 bytes, big endian
    const std::vector<int32_t> dict_values = {11, 22, 33};
    // rows:             NULL NULL 33 11 NULL 22
    // codes of the rows that are not NULL:   2  0      1
    const std::vector<int32_t> codes = {2, 0, 1};
    const std::vector<uint8_t> is_null = {1, 1, 0, 0, 1, 0};
    const size_t num_rows = is_null.size();

    NullInfos infos;
    infos.reset_with_capacity(num_rows);
    infos.num_nulls = 0;
    for (size_t i = 0; i < num_rows; ++i) {
        infos.nulls_data()[i] = is_null[i];
        infos.num_nulls += is_null[i];
    }
    // the runs NULL, value, NULL, value: more than two ranges, so the batched paths are used
    infos.num_ranges = 4;

    auto setup = [&](DictDecoder<Slice>* decoder, faststring* page) {
        std::string dict_page;
        for (int32_t value : dict_values) {
            for (int shift = 24; shift >= 0; shift -= 8) {
                dict_page.push_back(static_cast<char>((value >> shift) & 0xff));
            }
        }
        FLBAPlainDecoder plain;
        ASSERT_TRUE(plain.set_type_length(4).ok());
        ASSERT_TRUE(plain.set_data(Slice(dict_page.data(), dict_page.size())).ok());
        ASSERT_TRUE(decoder->set_dict(10, dict_values.size(), &plain).ok());

        faststring rle;
        RleEncoder<int32_t> encoder(&rle, 8);
        for (int32_t code : codes) {
            encoder.Put(code);
        }
        encoder.Flush();
        page->clear();
        page->push_back(8);
        page->append(rle.data(), rle.length());
        ASSERT_TRUE(decoder->set_data(Slice(page->data(), page->length())).ok());
    };

    // -1 is NULL
    auto convert_and_check = [&](Column* src, const std::vector<int64_t>& expected) {
        ParquetField field{};
        field.physical_type = tparquet::Type::FIXED_LEN_BYTE_ARRAY;
        field.type_length = 4;
        field.scale = 0;
        field.precision = 38;
        std::unique_ptr<ColumnConverter> converter;
        ASSERT_TRUE(ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter).ok());

        auto dst = ColumnHelper::create_column(col_type, true);
        Status st = converter->convert(src, dst.get());
        ASSERT_TRUE(st.ok()) << st.to_string();

        auto* nullable = down_cast<NullableColumn*>(dst.get());
        auto* data = down_cast<Decimal128Column*>(nullable->data_column_raw_ptr());
        ASSERT_EQ(expected.size(), nullable->size());
        for (size_t i = 0; i < expected.size(); ++i) {
            if (expected[i] < 0) {
                ASSERT_TRUE(nullable->is_null(i)) << "row " << i;
            } else {
                ASSERT_FALSE(nullable->is_null(i)) << "row " << i;
                ASSERT_EQ(expected[i], static_cast<int64_t>(data->get_data()[i])) << "row " << i;
            }
        }
    };

    // the values are read directly
    {
        DictDecoder<Slice> decoder;
        faststring page;
        setup(&decoder, &page);
        auto src = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        ASSERT_TRUE(decoder.next_batch_with_nulls(num_rows, infos, ColumnContentType::VALUE, src.get(), nullptr).ok());
        auto* binary = down_cast<BinaryColumn*>(src->data_column_raw_ptr());
        ASSERT_EQ(3 * 4U, binary->get_bytes().size());
        convert_and_check(src.get(), {-1, -1, 33, 11, -1, 22});
    }
    // the values are read directly, and a filter drops the row 3
    {
        DictDecoder<Slice> decoder;
        faststring page;
        setup(&decoder, &page);
        // force the path that applies the filter, whatever the size of the dictionary is
        decoder._dict_size_threshold = 0;
        const std::vector<uint8_t> filter = {1, 1, 1, 0, 1, 1};
        auto src = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        ASSERT_TRUE(decoder.next_batch_with_nulls(num_rows, infos, ColumnContentType::VALUE, src.get(), filter.data())
                            .ok());
        convert_and_check(src.get(), {-1, -1, 33, -1, -1, 22});
    }
    // the dictionary codes are read first, and the values are looked up later
    {
        DictDecoder<Slice> decoder;
        faststring page;
        setup(&decoder, &page);
        auto codes_column = NullableColumn::create(Int32Column::create(), NullColumn::create());
        ASSERT_TRUE(decoder.next_batch_with_nulls(num_rows, infos, ColumnContentType::DICT_CODE, codes_column.get(),
                                                  nullptr)
                            .ok());
        auto* codes_data = down_cast<Int32Column*>(codes_column->data_column_raw_ptr());

        auto src = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        ASSERT_TRUE(
                decoder.get_dict_values(codes_data->get_data(), *codes_column, ColumnHelper::get_data_column(src.get()))
                        .ok());
        src->swap_null_column(*codes_column);
        auto* binary = down_cast<BinaryColumn*>(src->data_column_raw_ptr());
        ASSERT_EQ(3 * 4U, binary->get_bytes().size());
        convert_and_check(src.get(), {-1, -1, 33, 11, -1, 22});
    }
}

TEST_F(ColumnConverterTest, Int96Test) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/int96.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "timestamp_int96";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2023-04-29 00:40:22.618760]", expected_rows);
        }
    }
}

TEST_F(ColumnConverterTest, Int96TimeZoneTest) {
    const std::string file_path =
            "./be/test/formats/parquet/test_data/column_converter/int96_timestamp_timezone.parquet";
    const size_t expected_rows = 2;

    {
        const std::string col_name = "time_with_new_york";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2019-04-17 04:00:00]", expected_rows);
        }
    }
    {
        const std::string col_name = "time_with_shanghai";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2019-04-17 04:00:00]", expected_rows);
        }
    }
    {
        const std::string col_name = "time_without_timezone";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2019-04-17 04:00:00]", expected_rows);
        }
    }
}

TEST_F(ColumnConverterTest, FloatTest) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/float.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "float";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_FLOAT);
            check(file_path, col_type, col_name, "[-5.55]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DOUBLE);
            check(file_path, col_type, col_name, "[-5.55]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
            check(file_path, col_type, col_name, "[-5.55]", expected_rows, true);
        }
    }
}

TEST_F(ColumnConverterTest, DoubleTest) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/double.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "double";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DOUBLE);
            check(file_path, col_type, col_name, "[-5.55556]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
            check(file_path, col_type, col_name, "[-5.55556]", expected_rows, true);
        }
    }
}

TEST_F(ColumnConverterTest, BooleanTest) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/boolean.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "boolean";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);
            check(file_path, col_type, col_name, "[1]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
            check(file_path, col_type, col_name, "[1]", expected_rows, true);
        }
    }
}

// Regression test for unsigned 32-bit integers physically stored as parquet INT32.
// A UINT_32 column (logical INTEGER bitWidth=32 isSigned=false, or the legacy
// UINT_32 converted type) must be loaded into a wider StarRocks column by
// ZERO-extending the raw 32 bits, not sign-extending. Before the fix the high-bit
// value 3000000000 (0xB2D05E00) was sign-extended to -1294967296 -> silent
// data corruption.
TEST_F(ColumnConverterTest, UnsignedInt32ZeroExtend) {
    // 3000000000 has bit 31 set; its int32 reinterpretation is negative.
    const int32_t kHighBits = static_cast<int32_t>(3000000000u); // == -1294967296
    const int64_t kExpectedUnsigned = 3000000000LL;

    auto make_src = [](int32_t bits) {
        auto data = FixedLengthColumn<int32_t>::create();
        data->get_data().push_back(bits);
        auto nulls = NullColumn::create();
        nulls->get_data().push_back(0);
        return NullableColumn::create(std::move(data), std::move(nulls));
    };

    // use_logical_type selects logicalType.INTEGER vs the legacy converted_type encoding.
    auto make_field = [](bool use_logical_type, bool is_signed) {
        ParquetField field;
        field.scale = 0;
        field.precision = 0;
        field.physical_type = tparquet::Type::INT32;
        if (use_logical_type) {
            field.schema_element.__isset.logicalType = true;
            field.schema_element.logicalType.__isset.INTEGER = true;
            field.schema_element.logicalType.INTEGER.bitWidth = 32;
            field.schema_element.logicalType.INTEGER.isSigned = is_signed;
        } else {
            field.schema_element.__isset.converted_type = true;
            field.schema_element.converted_type =
                    is_signed ? tparquet::ConvertedType::INT_32 : tparquet::ConvertedType::UINT_32;
        }
        return field;
    };

    auto run_bigint = [&](const ParquetField& field) -> int64_t {
        const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
        std::unique_ptr<ColumnConverter> converter;
        Status st = ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter);
        EXPECT_TRUE(st.ok()) << st.message();
        auto src = make_src(kHighBits);
        auto dst = ColumnHelper::create_column(col_type, true);
        st = converter->convert(src.get(), dst.get());
        EXPECT_TRUE(st.ok()) << st.message();
        auto* dst_nullable = down_cast<NullableColumn*>(dst.get());
        auto* dst_data = down_cast<FixedLengthColumn<int64_t>*>(dst_nullable->data_column_raw_ptr());
        return dst_data->get_data()[0];
    };

    // UINT_32 via modern logical type -> BIGINT must zero-extend.
    EXPECT_EQ(kExpectedUnsigned, run_bigint(make_field(/*use_logical_type=*/true, /*is_signed=*/false)));
    // UINT_32 via legacy converted type -> BIGINT must zero-extend.
    EXPECT_EQ(kExpectedUnsigned, run_bigint(make_field(/*use_logical_type=*/false, /*is_signed=*/false)));
    // Signed INT_32 with the same bits must still sign-extend (no regression).
    EXPECT_EQ(static_cast<int64_t>(kHighBits), run_bigint(make_field(/*use_logical_type=*/true, /*is_signed=*/true)));

    // A present-but-non-INTEGER logical type takes precedence over a contradictory
    // legacy UINT_32 converted type: the column must NOT be treated as unsigned.
    {
        ParquetField field;
        field.scale = 0;
        field.precision = 0;
        field.physical_type = tparquet::Type::INT32;
        field.schema_element.__isset.logicalType = true; // present, but INTEGER not set
        field.schema_element.__isset.converted_type = true;
        field.schema_element.converted_type = tparquet::ConvertedType::UINT_32;
        EXPECT_EQ(static_cast<int64_t>(kHighBits), run_bigint(field));
    }

    // UINT_32 -> DOUBLE must also widen via the unsigned value.
    {
        const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DOUBLE);
        ParquetField field = make_field(/*use_logical_type=*/false, /*is_signed=*/false);
        std::unique_ptr<ColumnConverter> converter;
        Status st = ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter);
        EXPECT_TRUE(st.ok()) << st.message();
        auto src = make_src(kHighBits);
        auto dst = ColumnHelper::create_column(col_type, true);
        st = converter->convert(src.get(), dst.get());
        EXPECT_TRUE(st.ok()) << st.message();
        auto* dst_nullable = down_cast<NullableColumn*>(dst.get());
        auto* dst_data = down_cast<FixedLengthColumn<double>*>(dst_nullable->data_column_raw_ptr());
        EXPECT_DOUBLE_EQ(static_cast<double>(kExpectedUnsigned), dst_data->get_data()[0]);
    }

    // UINT_32 -> DECIMAL128(38,0) must zero-extend the unscaled value.
    {
        const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 38, 0);
        ParquetField field = make_field(/*use_logical_type=*/false, /*is_signed=*/false);
        field.precision = 38;
        std::unique_ptr<ColumnConverter> converter;
        Status st = ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter);
        EXPECT_TRUE(st.ok()) << st.message();
        auto src = make_src(kHighBits);
        auto dst = ColumnHelper::create_column(col_type, true);
        st = converter->convert(src.get(), dst.get());
        EXPECT_TRUE(st.ok()) << st.message();
        auto* dst_nullable = down_cast<NullableColumn*>(dst.get());
        auto* dst_data = down_cast<Decimal128Column*>(dst_nullable->data_column_raw_ptr());
        EXPECT_EQ(kExpectedUnsigned, static_cast<int64_t>(dst_data->get_data()[0]));
    }
}

// Footer-statistics counterpart of the zero-extension fix. The converter is shared
// with statistics decoding, so an unsigned INT32 column's min/max must be validated
// with UNSIGNED Parquet sort order. Pre-1.10 parquet-mr writers stored the deprecated
// min/max with SIGNED ordering, which would invert once decoded as unsigned, so those
// legacy stats must be rejected for an unsigned column instead of trusted.
TEST_F(ColumnConverterTest, UnsignedInt32StatsRejectsLegacySignedOrder) {
    // Old parquet-mr writer (< 1.10.0): deprecated min/max are signed-ordered.
    tparquet::FileMetaData t_meta;
    t_meta.__set_version(1);
    t_meta.__set_num_rows(2);
    t_meta.__set_created_by("parquet-mr version 1.0.0 (build test)");
    tparquet::SchemaElement root_sch;
    root_sch.__set_name("schema");
    root_sch.__set_num_children(1);
    tparquet::SchemaElement leaf_sch;
    leaf_sch.__set_name("u");
    leaf_sch.__set_type(tparquet::Type::INT32);
    t_meta.__set_schema({root_sch, leaf_sch});
    FileMetaData file_meta;
    ASSERT_TRUE(file_meta.init(t_meta, false).ok());

    // Deprecated min/max present and differing (raw little-endian int32 bytes 1 and 2).
    tparquet::ColumnMetaData column_meta;
    column_meta.__set_type(tparquet::Type::INT32);
    tparquet::Statistics stats;
    stats.__set_min(std::string("\x01\x00\x00\x00", 4));
    stats.__set_max(std::string("\x02\x00\x00\x00", 4));
    column_meta.__set_statistics(stats);

    const TypeDescriptor bigint = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

    auto eval = [&](bool is_signed) {
        ParquetField field;
        field.physical_type = tparquet::Type::INT32;
        field.schema_element.__isset.logicalType = true;
        field.schema_element.logicalType.__isset.INTEGER = true;
        field.schema_element.logicalType.INTEGER.bitWidth = 32;
        field.schema_element.logicalType.INTEGER.isSigned = is_signed;
        std::vector<std::string> mins;
        std::vector<std::string> maxs;
        return StatisticsHelper::get_min_max_value(&file_meta, bigint, &column_meta, &field, mins, maxs);
    };

    // Unsigned column -> UNSIGNED sort order -> legacy signed-ordered stats are rejected.
    EXPECT_FALSE(eval(/*is_signed=*/false).ok());
    // Signed column -> SIGNED sort order -> legacy stats are accepted (no regression).
    EXPECT_TRUE(eval(/*is_signed=*/true).ok());
}

TEST_F(ColumnConverterTest, Int64_2_Timestamp) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/int64_2_timestamp.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "timestamp_millis";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2023-04-25 20:20:10]", expected_rows);
        }
    }
    {
        const std::string col_name = "timestamp_micros";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2023-04-25 20:20:10]", expected_rows);
        }
    }
}

// A pre-1970 (negative epoch tick) timestamp with a nonzero sub-second component must decode to the
// correct wall clock. C++ truncating division splits a negative tick into a too-high second and a
// negative sub-second; without a floor-borrow that negative sub-second corrupts the packed DATETIME.
TEST_F(ColumnConverterTest, Int64PreEpochTimestampSubSecond) {
    const std::string file_path =
            "./be/test/formats/parquet/test_data/column_converter/int64_timestamp_pre_epoch.parquet";
    const size_t expected_rows = 5;
    const std::string expected_value = "[1969-12-31 23:59:59.500000]";

    {
        const std::string col_name = "timestamp_millis";
        const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
        check(file_path, col_type, col_name, expected_value, expected_rows);
    }
    {
        const std::string col_name = "timestamp_micros";
        const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
        check(file_path, col_type, col_name, expected_value, expected_rows);
    }
}

TEST_F(ColumnConverterTest, Int96SignedNanosecondsFileRead) {
    const std::string path = "./be/test/formats/parquet/test_data/column_converter/int96_signed_nanoseconds.parquet";
    TypeDescriptor type = TYPE_DATETIME_DESC;
    for (const std::string zone : {"UTC", "Asia/Shanghai"}) {
        SCOPED_TRACE(fmt::format("zone={}", zone));
        const bool shifted = zone == "Asia/Shanghai";
        check(path, type, "before_epoch_us", shifted ? "[1970-01-01 07:59:59.999999]" : "[1969-12-31 23:59:59.999999]",
              5, false, zone);
        check(path, type, "before_epoch_half_second",
              shifted ? "[1970-01-01 07:59:59.500000]" : "[1969-12-31 23:59:59.500000]", 5, false, zone);
        check(path, type, "after_epoch", shifted ? "[1970-01-01 08:02:03.456789]" : "[1970-01-01 00:02:03.456789]", 5,
              false, zone);
    }
}

TEST_F(ColumnConverterTest, Int96SignedNanosecondsRoundTrip) {
    struct TestCase {
        int64_t nanos;
        uint32_t julian;
        Timestamp expected;
    };
    const std::vector<TestCase> cases = {
            {-1000, 2440588, timestamp::from_datetime(1969, 12, 31, 23, 59, 59, 999999)},
            {-86400000000000LL, 2440588, timestamp::from_datetime(1969, 12, 31, 0, 0, 0, 0)},
            {-86400000001000LL, 2440588, timestamp::from_datetime(1969, 12, 30, 23, 59, 59, 999999)},
            {-85636854775000LL, 2333837, timestamp::from_datetime(1677, 9, 21, 0, 12, 43, 145225)},
            {85636854775000LL, 2547339, timestamp::from_datetime(2262, 4, 11, 23, 47, 16, 854775)},
            {86399999999000LL, 2440587, timestamp::from_datetime(1969, 12, 31, 23, 59, 59, 999999)},
            {123456789000LL, 2440588, timestamp::from_datetime(1970, 1, 1, 0, 2, 3, 456789)},
            {-1, 2440588, timestamp::from_datetime(1969, 12, 31, 23, 59, 59, 999999)},
    };
    ParquetField field;
    field.physical_type = tparquet::Type::INT96;
    for (const auto& [zone, offset] : std::vector<std::pair<std::string, int>>{
                 {"UTC", 0}, {"+08:00", 8 * 3600}, {"-05:30", -(5 * 3600 + 30 * 60)}}) {
        SCOPED_TRACE(fmt::format("zone={}", zone));
        TypeDescriptor type(TYPE_DATETIME);
        std::unique_ptr<ColumnConverter> converter;
        ASSERT_TRUE(ColumnConverterFactory::create_converter(field, type, zone, &converter).ok());
        ASSERT_TRUE(converter->need_convert);
        auto src = converter->create_src_column();
        auto* src_nullable = down_cast<NullableColumn*>(src.get());
        auto* src_data = down_cast<FixedLengthColumn<int96_t>*>(src_nullable->data_column_raw_ptr());
        for (const auto& test : cases) {
            src_data->append(int96_t{static_cast<uint64_t>(test.nanos), test.julian});
            src_nullable->null_column_raw_ptr()->append(0);
        }
        ASSERT_TRUE(src_nullable->append_nulls(1));
        src_data->get_data().back() = {std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint32_t>::max()};
        auto dst = ColumnHelper::create_column(type, true);
        ASSERT_TRUE(converter->convert(src.get(), dst.get()).ok());
        ASSERT_EQ(cases.size() + 1, dst->size());
        ASSERT_TRUE(dst->has_null());
        ASSERT_TRUE(dst->is_null(cases.size()));
        const auto* dst_nullable = down_cast<const NullableColumn*>(dst.get());
        const auto& data = down_cast<const TimestampColumn*>(dst_nullable->data_column().get())->get_data();
        for (size_t i = 0; i < cases.size(); ++i) {
            ASSERT_FALSE(dst->is_null(i));
            const auto expected = timestamp::add<TimeUnit::SECOND>(cases[i].expected, offset);
            EXPECT_EQ(expected, data[i].timestamp()) << "row " << i;
        }
        // An all-NULL batch must not normalize payloads or look up their invalid days.
        auto all_null = converter->create_src_column();
        ASSERT_TRUE(all_null->append_nulls(1));
        auto* all_null_nullable = down_cast<NullableColumn*>(all_null.get());
        down_cast<FixedLengthColumn<int96_t>*>(all_null_nullable->data_column_raw_ptr())->get_data()[0] = {
                std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint32_t>::max()};
        auto null_dst = ColumnHelper::create_column(type, true);
        ASSERT_TRUE(converter->convert(all_null.get(), null_dst.get()).ok());
        ASSERT_EQ(1, null_dst->size());
        ASSERT_TRUE(null_dst->has_null());
        ASSERT_TRUE(null_dst->is_null(0));
    }
}

} // namespace starrocks::parquet
