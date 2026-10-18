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

#include "formats/parquet/parquet_file_writer.h"

#include <gtest/gtest.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#include <filesystem>
#include <memory>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/level_builder.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "testutil/assert.h"
#include "testutil/column_test_helper.h"

namespace starrocks::formats {

namespace {

MutableColumnPtr make_nullable_varchar_column(const std::string& prefix, const std::vector<uint8_t>& nulls) {
    std::vector<std::string> strings;
    for (size_t i = 0; i < nulls.size(); i++) {
        strings.push_back(prefix + std::to_string(i));
    }
    std::vector<Slice> slices;
    for (const auto& str : strings) {
        slices.emplace_back(str);
    }
    return ColumnTestHelper::build_nullable_column<Slice>(slices, nulls);
}

MutableColumnPtr make_timestamp_column(const std::vector<TimestampValue>& values, const std::vector<uint8_t>& nulls) {
    auto data = TimestampColumn::create();
    for (const auto& value : values) {
        Datum datum;
        datum.set_timestamp(value);
        data->append_datum(datum);
    }
    if (nulls.empty()) {
        return data;
    }
    auto null_column = NullColumn::create();
    null_column->append_numbers(nulls.data(), nulls.size());
    return NullableColumn::create(std::move(data), std::move(null_column));
}

// Compares the rows by value: a column that is written as NOT NULL is read back as a nullable column.
void assert_same_rows(const Chunk* expected, const Chunk* actual) {
    ASSERT_EQ(expected->num_columns(), actual->num_columns());
    ASSERT_EQ(expected->num_rows(), actual->num_rows());
    for (size_t col = 0; col < expected->num_columns(); col++) {
        const auto& expected_col = expected->get_column_by_index(col);
        const auto& actual_col = actual->get_column_by_index(col);
        for (size_t row = 0; row < expected->num_rows(); row++) {
            ASSERT_EQ(expected_col->debug_item(row), actual_col->debug_item(row))
                    << "column " << col << ", row " << row;
        }
    }
}

// Runs the level builder over one column with a callback that ignores the leaf columns.
Status write_with_level_builder(const TypeDescriptor& type_desc, const ::parquet::schema::NodePtr& node,
                                const ColumnPtr& col) {
    parquet::LevelBuilder level_builder(type_desc, node, TimezoneUtils::default_time_zone, false, false);
    RETURN_IF_ERROR(level_builder.init());
    parquet::LevelBuilderContext ctx(col->size());
    return level_builder.write(ctx, col, [](const parquet::LevelBuilderResult&) {});
}

// An output stream whose writes fail, like a file system that is full.
class FailingOutputStream final : public arrow::io::OutputStream {
public:
    arrow::Status Write(const void*, int64_t) override { return arrow::Status::IOError("disk is full"); }
    arrow::Status Write(const std::shared_ptr<arrow::Buffer>&) override {
        return arrow::Status::IOError("disk is full");
    }
    arrow::Result<int64_t> Tell() const override { return static_cast<int64_t>(0); }
    arrow::Status Close() override {
        _closed = true;
        return arrow::Status::OK();
    }
    bool closed() const override { return _closed; }

private:
    bool _closed = false;
};

} // namespace

class ParquetFileWriterTest : public testing::Test {
public:
    void SetUp() override {
        _runtime_state = _pool.add(new RuntimeState(TQueryGlobals()));
        _writer_options = std::make_shared<ParquetWriterOptions>();
        auto output_file = _fs.new_writable_file(_file_path).value();
        _output_stream = std::make_unique<parquet::ParquetOutputStream>(std::move(output_file));
    }

    void TearDown() override {}

protected:
    HdfsScannerContext _scanner_ctx;
    HdfsScannerContext* _create_scan_context(const std::vector<TypeDescriptor>& type_descs,
                                             const std::vector<std::string>& column_names = {}) {
        auto ctx = _pool.add(new HdfsScannerContext());

        ctx->lazy_column_coalesce_counter = &_lazy_column_coalesce_counter;

        std::vector<parquet::Utils::SlotDesc> slot_descs;
        for (size_t i = 0; i < type_descs.size(); i++) {
            auto name = column_names.empty() ? type_descs[i].debug_string() : column_names[i];
            slot_descs.push_back({name, type_descs[i]});
        }
        slot_descs.push_back({""});

        TupleDescriptor* tuple_desc =
                parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs.data());
        parquet::Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
        ctx->slot_descs = tuple_desc->slots();
        ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
        ctx->scan_range = _create_scan_range(_file_path, file_size);
        ctx->timezone = "Asia/Shanghai";
        ctx->stats = &_hdfs_stats;

        return ctx;
    }

    THdfsScanRange* _create_scan_range(const std::string& file_path, size_t file_length) {
        auto* scan_range = _pool.add(new THdfsScanRange());
        scan_range->relative_path = file_path;
        scan_range->file_length = file_length;
        scan_range->offset = 4;
        scan_range->length = file_length;

        return scan_range;
    }

    static std::vector<std::string> _make_type_names(const std::vector<TypeDescriptor>& type_descs) {
        std::vector<std::string> names;
        for (auto& desc : type_descs) {
            names.push_back(desc.debug_string());
        }
        return names;
    }

    ChunkPtr _read_chunk(const std::vector<TypeDescriptor>& type_descs,
                         const std::vector<std::string>& column_names = {}) {
        auto ctx = _create_scan_context(type_descs, column_names);
        ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
        ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
        auto file_reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), file_size);

        auto st = file_reader->init(ctx);
        if (!st.ok()) {
            std::cout << st.to_string() << std::endl;
            return nullptr;
        }

        auto read_chunk = std::make_shared<Chunk>();
        for (auto type_desc : type_descs) {
            auto col = ColumnHelper::create_column(type_desc, true);
            read_chunk->append_column(std::move(col), read_chunk->num_columns());
        }

        EXPECT_OK(file_reader->get_next(&read_chunk));
        return read_chunk;
    }

    // Writes the chunk, checks that the top level columns of the file are optional exactly where `nullable` says so,
    // reads the file back and compares every row with `expected`, which is the chunk itself by default.
    // `check_schema` can check more of the schema of the file.
    void _write_and_check_round_trip(
            const std::vector<TypeDescriptor>& type_descs, const std::vector<std::string>& column_names,
            const std::vector<bool>& nullable, Chunk* chunk, const Chunk* expected = nullptr,
            const std::function<void(const parquet::SchemaDescriptor&)>& check_schema = nullptr) {
        ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs, nullable, column_names));
        ASSERT_OK(writer->write(chunk));
        auto result = writer->close();
        ASSERT_OK(result.io_status);
        ASSERT_EQ(result.file_statistics.record_count, static_cast<int64_t>(chunk->num_rows()));

        {
            ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
            ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
            auto file_reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), file_size);
            ASSERT_OK(file_reader->init(_create_scan_context(type_descs, column_names)));
            const auto& schema = file_reader->get_file_metadata()->schema();
            ASSERT_EQ(schema.get_fields_size(), type_descs.size());
            for (size_t i = 0; i < type_descs.size(); i++) {
                EXPECT_EQ(schema.get_stored_column_by_field_idx(i)->is_nullable, nullable[i])
                        << "column " << column_names[i];
            }
            if (check_schema) {
                check_schema(schema);
            }
        }

        auto read_chunk = _read_chunk(type_descs, column_names);
        ASSERT_TRUE(read_chunk != nullptr);
        assert_same_rows(expected != nullptr ? expected : chunk, read_chunk.get());
    }

    // Writes a chunk whose only column holds a NULL in a required nested field and expects an error that names the
    // field. The only column is called "c", and `ids` marks the required fields.
    void _expect_nested_null_rejected(const TypeDescriptor& type_desc, const FileColumnId& ids, const ColumnPtr& column,
                                      const std::string& field_path) {
        _writer_options->column_ids = std::vector<FileColumnId>{ids};
        std::vector<TypeDescriptor> type_descs{type_desc};
        ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs, {true}, {"c"}));

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(column, 0);
        auto st = writer->write(chunk.get());
        ASSERT_FALSE(st.ok()) << "writing NULL into a required field must fail";
        EXPECT_NE(st.message().find("'" + field_path + "'"), std::string::npos) << st.message();
    }

    StatusOr<std::unique_ptr<ParquetFileWriter>> _create_writer(const std::vector<TypeDescriptor>& type_descs,
                                                                std::vector<bool> nullable = {},
                                                                std::vector<std::string> column_names = {});

    HdfsScannerStats _hdfs_stats;
    MemoryFileSystem _fs;
    std::string _file_path{"/dummy_file.parquet"};
    std::unique_ptr<parquet::ParquetOutputStream> _output_stream;
    RuntimeState* _runtime_state = nullptr;
    ObjectPool _pool;
    std::atomic<int> _lazy_column_coalesce_counter = 0;
    std::shared_ptr<ParquetWriterOptions> _writer_options;
    TCompressionType::type _compression_type = TCompressionType::NO_COMPRESSION;
};

StatusOr<std::unique_ptr<ParquetFileWriter>> ParquetFileWriterTest::_create_writer(
        const std::vector<TypeDescriptor>& type_descs, std::vector<bool> nullable,
        std::vector<std::string> column_names) {
    if (column_names.size() == 0) {
        column_names = _make_type_names(type_descs);
    }
    auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
    auto writer = std::make_unique<ParquetFileWriter>(
            _file_path, std::move(_output_stream), std::move(column_names), type_descs, std::move(column_evaluators),
            _compression_type, _writer_options, [] {}, std::move(nullable));
    if (Status st = writer->init(); st.ok()) {
        return writer;
    } else {
        return st;
    }
}

TEST_F(ParquetFileWriterTest, TestWriteIntegralTypes) {
    std::vector type_descs{TYPE_TINYINT_DESC, TYPE_SMALLINT_DESC, TYPE_INT_DESC, TYPE_BIGINT_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnTestHelper::build_nullable_column<int8_t>({INT8_MIN, INT8_MAX, 0, 1});
        auto col1 = ColumnTestHelper::build_nullable_column<int16_t>({INT16_MIN, INT16_MAX, 0, 1});
        auto col2 = ColumnTestHelper::build_nullable_column<int32_t>({INT32_MIN, INT32_MAX, 0, 1});
        auto col3 = ColumnTestHelper::build_nullable_column<int64_t>({INT64_MIN, INT64_MAX, 0, 1});

        chunk->append_column(std::move(col0), 0);
        chunk->append_column(std::move(col1), 1);
        chunk->append_column(std::move(col2), 2);
        chunk->append_column(std::move(col3), 3);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteDecimal) {
    std::vector type_descs{
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 5),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 9),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 10),
    };
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnHelper::create_column(type_descs[0], true);
        std::vector<int32_t> int32_nums{-999999, 999999, 0, 1};
        ASSERT_EQ(col0->append_numbers(int32_nums.data(), int32_nums.size() * sizeof(int32_t)), 4);
        chunk->append_column(std::move(col0), chunk->num_columns());

        auto col1 = ColumnHelper::create_column(type_descs[1], true);
        std::vector<int64_t> int64_nums{-999999999999, 999999999999, 0, 1};
        ASSERT_EQ(col1->append_numbers(int64_nums.data(), int64_nums.size() * sizeof(int64_t)), 4);
        chunk->append_column(std::move(col1), chunk->num_columns());

        auto col2 = ColumnHelper::create_column(type_descs[2], true);
        std::vector<int128_t> int128_nums{-999999999999, 999999999999, 0, 1};
        ASSERT_EQ(col2->append_numbers(int128_nums.data(), int128_nums.size() * sizeof(int128_t)), 4);
        chunk->append_column(std::move(col2), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteDecimalCompatibleWithHiveReader) {
    std::vector type_descs{
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 5),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 9),
            TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 1),
    };
    _writer_options->use_legacy_decimal_encoding = true;
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnHelper::create_column(type_descs[0], true);
        std::vector<int32_t> int32_nums{-999999, 999999, 0, 1};
        ASSERT_EQ(col0->append_numbers(int32_nums.data(), int32_nums.size() * sizeof(int32_t)), 4);
        chunk->append_column(std::move(col0), chunk->num_columns());

        auto col1 = ColumnHelper::create_column(type_descs[1], true);
        std::vector<int64_t> int64_nums{-999999999999, 999999999999, 0, 1};
        ASSERT_EQ(col1->append_numbers(int64_nums.data(), int64_nums.size() * sizeof(int64_t)), 4);
        chunk->append_column(std::move(col1), chunk->num_columns());

        auto col2 = ColumnHelper::create_column(type_descs[2], true);
        std::vector<int128_t> int128_nums{-999999999999, 999999999999, 0, 1};
        ASSERT_EQ(col2->append_numbers(int128_nums.data(), int128_nums.size() * sizeof(int128_t)), 4);
        chunk->append_column(std::move(col2), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteBoolean) {
    std::vector type_descs{TYPE_BOOLEAN_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_nullable_column<uint8_t>({0, 1, 1, 0}, {1, 0, 1, 0});
        chunk->append_column(std::move(col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteFloat) {
    std::vector type_descs{TYPE_FLOAT_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_nullable_column<float>({0.1, 1.1, 1.2, -99.9}, {1, 0, 1, 0});
        chunk->append_column(std::move(col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteDouble) {
    std::vector type_descs{TYPE_DOUBLE_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_nullable_column<double>({0.1, 1.1, 1.2, -99.9}, {1, 0, 1, 0});
        chunk->append_column(std::move(col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteDate) {
    std::vector type_descs{TYPE_DATE_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        // not-null column
        auto data_column = DateColumn::create();
        {
            Datum datum;
            datum.set_date(DateValue::create(1999, 9, 9));
            data_column->append_datum(datum);
            datum.set_date(DateValue::create(1999, 9, 10));
            data_column->append_datum(datum);
            datum.set_date(DateValue::create(1999, 9, 11));
            data_column->append_datum(datum);
            data_column->append_default();
        }

        auto null_column = ColumnTestHelper::build_column<uint8_t>({1, 0, 1, 0});
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteDatetime) {
    std::vector type_descs{TYPE_DATETIME_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        // not-null column
        auto data_column = TimestampColumn::create();
        {
            Datum datum;
            datum.set_timestamp(TimestampValue::create(2023, 9, 9, 23, 59, 59));
            data_column->append_datum(datum);
            datum.set_timestamp(TimestampValue::create(1999, 9, 10, 1, 1, 1));
            data_column->append_datum(datum);
            datum.set_timestamp(TimestampValue::create(1970, 1, 1, 0, 0, 0));
            data_column->append_datum(datum);
            datum.set_timestamp(TimestampValue::create(1970, 1, 1, 1, 1, 1));
            data_column->append_datum(datum);
        }

        auto null_column = ColumnTestHelper::build_column<uint8_t>({0, 0, 0, 0});
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteDatetimeCompatibleWithHiveReader) {
    std::vector type_descs{TYPE_DATETIME_DESC};
    _writer_options->use_int96_timestamp_encoding = true;
    _writer_options->time_zone = "Asia/Shanghai";
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        // not-null column
        auto data_column = TimestampColumn::create();
        {
            Datum datum;
            datum.set_timestamp(TimestampValue::create(1999, 9, 9, 0, 0, 0));
            data_column->append_datum(datum);
            datum.set_timestamp(TimestampValue::create(1999, 9, 10, 1, 1, 1));
            data_column->append_datum(datum);
            datum.set_timestamp(TimestampValue::create(1999, 9, 11, 2, 2, 2));
            data_column->append_datum(datum);
            // Daylight Saving Time value
            datum.set_timestamp(TimestampValue::create(1986, 8, 25, 0, 0, 0));
            data_column->append_datum(datum);
            data_column->append_default();
        }

        auto null_column = ColumnTestHelper::build_column<uint8_t>({1, 0, 1, 0, 0});
        auto nullable_column = NullableColumn::create(std::move(data_column), std::move(null_column));
        chunk->append_column(std::move(nullable_column), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 5);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 5);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteVarchar) {
    std::vector type_descs{TYPE_VARCHAR_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_nullable_column<Slice>({"hello", "world", "starrocks", "lakehouse"},
                                                                  {1, 0, 1, 0});
        chunk->append_column(std::move(col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteArray) {
    std::vector<TypeDescriptor> type_descs;
    type_descs.push_back(TYPE_INT_ARRAY_DESC);
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    // [1], NULL, [], [2, NULL, 3]
    auto chunk = std::make_shared<Chunk>();
    {
        auto elements_col = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, -99, 3}, {0, 0, 1, 0});
        auto offsets_col = ColumnTestHelper::build_column<uint32_t>({0, 1, 1, 1, 4});
        auto array_col = ArrayColumn::create(std::move(elements_col), std::move(offsets_col));
        auto null_col = ColumnTestHelper::build_column<uint8_t>({0, 1, 0, 0});
        auto nullable_col = NullableColumn::create(std::move(array_col), std::move(null_col));

        chunk->append_column(std::move(nullable_col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteStruct) {
    std::vector<TypeDescriptor> type_descs;
    auto type_int_struct = TypeDescriptor::from_logical_type(TYPE_STRUCT);
    type_int_struct.children = {TYPE_SMALLINT_DESC, TYPE_INT_DESC, TYPE_BIGINT_DESC};
    type_int_struct.field_names = {"a", "b", "c"};
    type_descs.push_back(type_int_struct);

    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto nullable_col_a = ColumnTestHelper::build_nullable_column<int16_t>({1, 2, -99, 3}, {0, 0, 1, 0});
        auto nullable_col_b = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, -99, 3}, {0, 0, 1, 0});
        auto nullable_col_c = ColumnTestHelper::build_nullable_column<int64_t>({1, 2, -99, 3}, {0, 0, 1, 0});
        auto null_column = ColumnTestHelper::build_column<uint8_t>({0, 0, 1, 0});

        Columns fields{std::move(nullable_col_a), std::move(nullable_col_b), std::move(nullable_col_c)};
        auto struct_column = StructColumn::create(std::move(fields), std::move(type_int_struct.field_names));
        auto nullable_col = NullableColumn::create(std::move(struct_column), std::move(null_column));

        chunk->append_column(std::move(nullable_col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteMap) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_map = TypeDescriptor::from_logical_type(TYPE_MAP);
    type_int_map.children.push_back(TYPE_INT_DESC);
    type_int_map.children.push_back(TYPE_INT_DESC);
    type_descs.push_back(type_int_map);
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    // [1 -> 1], NULL, [], [2 -> 2, 3 -> NULL, 4 -> 4]
    auto chunk = std::make_shared<Chunk>();
    {
        auto key_col = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3, 4}, {0, 0, 0, 0});
        auto value_col = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, -99, 4}, {0, 0, 1, 0});
        auto offsets_col = ColumnTestHelper::build_column<uint32_t>({0, 1, 1, 1, 4});
        auto null_col = ColumnTestHelper::build_column<uint8_t>({0, 1, 0, 0});

        auto map_col = MapColumn::create(std::move(key_col), std::move(value_col), std::move(offsets_col));
        auto nullable_col = NullableColumn::create(std::move(map_col), std::move(null_col));

        chunk->append_column(std::move(nullable_col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteMapOfNullKey) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_map = TypeDescriptor::from_logical_type(TYPE_MAP);
    type_int_map.children.push_back(TYPE_INT_DESC);
    type_int_map.children.push_back(TYPE_INT_DESC);
    type_descs.push_back(type_int_map);
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    // [NULL -> 1], NULL, [], [2 -> 2, 3 -> NULL, 4 -> 4]
    auto chunk = std::make_shared<Chunk>();
    {
        auto key_col = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3, 4}, {1, 0, 0, 0});
        auto value_col = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, -99, 4}, {0, 0, 1, 0});
        auto offsets_col = ColumnTestHelper::build_column<uint32_t>({0, 1, 1, 1, 4});
        auto null_col = ColumnTestHelper::build_column<uint8_t>({0, 1, 0, 0});

        auto map_col = MapColumn::create(std::move(key_col), std::move(value_col), std::move(offsets_col));
        auto nullable_col = NullableColumn::create(std::move(map_col), std::move(null_col));

        chunk->append_column(std::move(nullable_col), 0);
    }

    // write chunk
    ASSERT_ERROR(writer->write(chunk.get()));
}

TEST_F(ParquetFileWriterTest, TestWriteNestedArray) {
    // type_descs
    std::vector<TypeDescriptor> type_descs;
    auto type_int_array_array = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    type_int_array_array.children.push_back(TYPE_INT_ARRAY_DESC);
    type_descs.push_back(type_int_array_array);
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    // [[1], NULL, [], [2, NULL, 3]], [[4, 5], [6]], NULL
    auto chunk = std::make_shared<Chunk>();
    {
        auto int_col = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, -99, 3, 4, 5, 6}, {0, 0, 1, 0, 0, 0, 0});
        auto offsets_col = ColumnTestHelper::build_column<uint32_t>({0, 1, 1, 1, 4, 6, 7});
        auto array_data_col = ArrayColumn::create(std::move(int_col), std::move(offsets_col));
        auto array_null_col = ColumnTestHelper::build_column<uint8_t>({0, 1, 0, 0, 0, 0});
        auto array_col = NullableColumn::create(std::move(array_data_col), std::move(array_null_col));

        auto array_array_offsets_col = ColumnTestHelper::build_column<uint32_t>({0, 4, 6, 6});
        auto array_array_data_col = ArrayColumn::create(std::move(array_col), std::move(array_array_offsets_col));

        auto array_array_null_col = ColumnTestHelper::build_column<uint8_t>({0, 0, 1});
        auto array_array_col = NullableColumn::create(std::move(array_array_data_col), std::move(array_array_null_col));

        chunk->append_column(std::move(array_array_col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 3);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteVarbinary) {
    std::vector type_descs{TYPE_VARBINARY_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_nullable_column<Slice>({"hello", "world", "starrocks", "lakehouse"},
                                                                  {1, 0, 1, 0});
        chunk->append_column(std::move(col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestAllocatedBytes) {
    std::vector type_descs{TYPE_VARBINARY_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_nullable_column<Slice>({"hello", "world", "starrocks", "lakehouse"},
                                                                  {1, 0, 1, 0});
        chunk->append_column(std::move(col), 0);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    ASSERT_TRUE(writer->get_allocated_bytes() > 0);
    auto result = writer->close();
    ASSERT_TRUE(writer->get_allocated_bytes() == 0);

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 4);

    auto read_chunk = _read_chunk(type_descs);
    ASSERT_TRUE(read_chunk != nullptr);
    ASSERT_EQ(read_chunk->num_rows(), 4);
    parquet::Utils::assert_equal_chunk(chunk.get(), read_chunk.get());
}

TEST_F(ParquetFileWriterTest, TestFlushRowgroup) {
    std::vector type_descs{TYPE_BOOLEAN_DESC};
    _writer_options->rowgroup_size = 1;
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_nullable_column<uint8_t>({0, 1, 1, 0}, {1, 0, 1, 0});
        chunk->append_column(std::move(col), 0);
    }

    // write chunk twice
    ASSERT_OK(writer->write(chunk.get()));
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 8);
}

TEST_F(ParquetFileWriterTest, TestWriteWithFieldID) {
    std::vector type_descs{TYPE_BOOLEAN_DESC};
    _writer_options->column_ids = {FileColumnId{1, {}}};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        auto nullable_column = ColumnTestHelper::build_nullable_column<uint8_t>({0, 1, 1, 0}, {1, 0, 1, 0});
        chunk->append_column(std::move(nullable_column), 0);
    }

    // write chunk twice
    ASSERT_OK(writer->write(chunk.get()));
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 8);
}

TEST_F(ParquetFileWriterTest, TestWriteWithIcebergRowLineageFieldIDs) {
    std::vector type_descs{TYPE_INT_DESC, TYPE_VARCHAR_DESC, TYPE_BIGINT_DESC, TYPE_BIGINT_DESC};
    _writer_options->column_ids = {FileColumnId{1, {}}, FileColumnId{2, {}}, FileColumnId{2147483540, {}},
                                   FileColumnId{2147483539, {}}};
    ASSIGN_OR_ASSERT_FAIL(auto writer,
                          _create_writer(type_descs, {}, {"id", "name", "_row_id", "_last_updated_sequence_number"}));

    auto chunk = std::make_shared<Chunk>();
    {
        auto id_col = ColumnTestHelper::build_nullable_column<int32_t>({1, 2}, {0, 0});
        auto name_col = ColumnTestHelper::build_nullable_column<Slice>({"alice", "bob"}, {0, 0});
        auto row_id_col = ColumnTestHelper::build_nullable_column<int64_t>({10, 11}, {0, 0});
        auto seq_col = ColumnTestHelper::build_nullable_column<int64_t>({20, 21}, {0, 0});

        chunk->append_column(std::move(id_col), 0);
        chunk->append_column(std::move(name_col), 1);
        chunk->append_column(std::move(row_id_col), 2);
        chunk->append_column(std::move(seq_col), 3);
    }

    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 2);
}

TEST_F(ParquetFileWriterTest, TestUnknownCompression) {
    std::vector type_descs{TYPE_BOOLEAN_DESC};
    _compression_type = TCompressionType::UNKNOWN_COMPRESSION;
    ASSERT_ERROR(_create_writer(type_descs));
}

TEST_F(ParquetFileWriterTest, TestFactory) {
    std::vector type_descs{TYPE_BOOLEAN_DESC};

    auto column_names = _make_type_names(type_descs);
    auto column_evaluators = std::make_shared<std::vector<std::unique_ptr<ColumnEvaluator>>>(
            ColumnSlotIdEvaluator::from_types(type_descs));
    auto fs = std::make_shared<MemoryFileSystem>();
    auto factory = ParquetFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, {}, column_names, column_evaluators,
                                            std::nullopt, nullptr, nullptr);
    ASSERT_OK(factory.init());
    auto maybe_writer = factory.create(_file_path);
    ASSERT_OK(maybe_writer.status());
}

TEST_F(ParquetFileWriterTest, TestWriteJson) {
    std::vector type_descs{TYPE_JSON_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    // nullable column
    auto data_column = JsonColumn::create();
    auto json1 = JsonValue::parse(R"({"a": 1, "b": 2})");
    data_column->append(&json1.value());
    data_column->append(&json1.value());
    auto json2 = JsonValue::parse(R"({"a": 3})");
    data_column->append(&json2.value());

    auto null_column = ColumnTestHelper::build_column<uint8_t>({1, 0, 1});
    auto nullable_column = NullableColumn::create(data_column, null_column);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(nullable_column, 0);

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 3);

    // _read_chunk does not support read json
}

TEST_F(ParquetFileWriterTest, TestNullableColumnsAllRequired) {
    std::vector type_descs{TYPE_VARCHAR_DESC, TYPE_BIGINT_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs, {false, false}));

    auto chunk = std::make_shared<Chunk>();
    {
        // file_path column (VARCHAR) - non-null
        auto col0 = ColumnTestHelper::build_column<Slice>({"file1.parquet", "file2.parquet", "file3.parquet"});
        chunk->append_column(std::move(col0), 0);

        // pos column (BIGINT) - non-null
        auto col1 = ColumnTestHelper::build_column<int64_t>({100, 200, 300});
        chunk->append_column(std::move(col1), 1);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 3);

    // Verify Parquet schema has REQUIRED columns
    ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
    ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
    auto file_reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), file_size);
    auto ctx = _create_scan_context(type_descs);
    ASSERT_OK(file_reader->init(ctx));
    auto file_metadata = file_reader->get_file_metadata();

    // Check that both columns are REQUIRED
    for (int i = 0; i < file_metadata->schema().get_fields_size(); i++) {
        auto field = file_metadata->schema().get_stored_column_by_field_idx(i);
        auto repetition = field->schema_element.repetition_type;
        EXPECT_EQ(repetition, tparquet::FieldRepetitionType::REQUIRED)
                << "Column " << i << " should be REQUIRED but is " << repetition;
    }
}

TEST_F(ParquetFileWriterTest, TestNullableColumnsMixed) {
    std::vector type_descs{TYPE_VARCHAR_DESC, TYPE_BIGINT_DESC};

    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs, {true, false}));

    auto chunk = std::make_shared<Chunk>();
    {
        // file_path column (VARCHAR) - nullable
        auto nullable_col0 =
                ColumnTestHelper::build_nullable_column<Slice>({"file1.parquet", "file2.parquet", ""}, {0, 0, 1});
        chunk->append_column(std::move(nullable_col0), chunk->num_columns());

        // pos column (BIGINT) - non-null
        auto col1 = ColumnTestHelper::build_column<int64_t>({100, 200, 300});
        chunk->append_column(std::move(col1), chunk->num_columns());
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 3);

    // Verify Parquet schema has correct Repetition types
    ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
    ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
    auto file_reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), file_size);
    auto ctx = _create_scan_context(type_descs);
    ASSERT_OK(file_reader->init(ctx));
    auto file_metadata = file_reader->get_file_metadata();

    // Check first column is OPTIONAL
    auto col0_field = file_metadata->schema().get_stored_column_by_field_idx(0);
    auto repetition0 = col0_field->schema_element.repetition_type;
    EXPECT_EQ(repetition0, tparquet::FieldRepetitionType::OPTIONAL)
            << "Column 0 should be OPTIONAL but is " << repetition0;

    // Check second column is REQUIRED
    auto col1_field = file_metadata->schema().get_stored_column_by_field_idx(1);
    auto repetition1 = col1_field->schema_element.repetition_type;
    EXPECT_EQ(repetition1, tparquet::FieldRepetitionType::REQUIRED)
            << "Column 1 should be REQUIRED but is " << repetition1;
}

TEST_F(ParquetFileWriterTest, TestNullableColumnsDefaultEmpty) {
    std::vector type_descs{TYPE_VARCHAR_DESC, TYPE_BIGINT_DESC};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs));

    auto chunk = std::make_shared<Chunk>();
    {
        // file_path column (VARCHAR)
        auto col0 = ColumnTestHelper::build_column<Slice>({"file1.parquet", "file2.parquet", "file3.parquet"});
        chunk->append_column(std::move(col0), 0);

        // pos column (BIGINT)
        auto col1 = ColumnTestHelper::build_column<int64_t>({100, 200, 300});
        chunk->append_column(std::move(col1), 1);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 3);

    // Verify Parquet schema has OPTIONAL columns (backward compatibility)
    ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
    ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
    auto file_reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), file_size);
    auto ctx = _create_scan_context(type_descs);
    ASSERT_OK(file_reader->init(ctx));
    auto file_metadata = file_reader->get_file_metadata();

    // Check that both columns are OPTIONAL (default behavior)
    for (int i = 0; i < file_metadata->schema().get_fields_size(); i++) {
        auto field = file_metadata->schema().get_stored_column_by_field_idx(i);
        auto repetition = field->schema_element.repetition_type;
        EXPECT_EQ(repetition, tparquet::FieldRepetitionType::OPTIONAL)
                << "Column " << i << " should be OPTIONAL (default) but is " << repetition;
    }
}

TEST_F(ParquetFileWriterTest, TestIcebergDeleteFileColumnsRequired) {
    // Test specific use case: Iceberg position delete file with file_path and pos columns as REQUIRED
    std::vector type_descs{TYPE_VARCHAR_DESC, TYPE_BIGINT_DESC};
    std::vector nullable = {false, false};
    std::vector<std::string> column_names = {"file_path", "pos"};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs, nullable, column_names));

    auto chunk = std::make_shared<Chunk>();
    {
        // file_path column (VARCHAR) - REQUIRED for Iceberg delete files
        auto col0 = ColumnTestHelper::build_column<Slice>({"s3://bucket/table/data/file1.parquet",
                                                           "s3://bucket/table/data/file1.parquet",
                                                           "s3://bucket/table/data/file2.parquet"});
        chunk->append_column(std::move(col0), 0);

        // pos column (BIGINT) - REQUIRED for Iceberg delete files
        auto col1 = ColumnTestHelper::build_column<int64_t>({100, 200, 50});
        chunk->append_column(std::move(col1), 1);
    }

    // write chunk
    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();

    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 3);

    // Verify Parquet schema has REQUIRED columns for Iceberg delete file
    ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(_file_path));
    ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(_file_path));
    auto file_reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), file_size);
    auto ctx = _create_scan_context(type_descs);
    ASSERT_OK(file_reader->init(ctx));
    auto file_metadata = file_reader->get_file_metadata();

    // Verify column names
    EXPECT_EQ(file_metadata->schema().get_stored_column_by_field_idx(0)->name, "file_path");
    EXPECT_EQ(file_metadata->schema().get_stored_column_by_field_idx(1)->name, "pos");

    // Verify both columns are REQUIRED (Iceberg spec requirement)
    for (int i = 0; i < file_metadata->schema().get_fields_size(); i++) {
        auto field = file_metadata->schema().get_stored_column_by_field_idx(i);
        auto repetition = field->schema_element.repetition_type;
        EXPECT_EQ(repetition, tparquet::FieldRepetitionType::REQUIRED)
                << "Column " << field->name << " should be REQUIRED for Iceberg delete files but is " << repetition;
    }
}

TEST_F(ParquetFileWriterTest, TestRequiredColumnRejectsNull) {
    std::vector type_descs{TYPE_VARCHAR_DESC, TYPE_BIGINT_DESC};
    std::vector<std::string> column_names = {"a", "b"};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs, {true, false}, column_names));

    auto chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnTestHelper::build_nullable_column<Slice>({"x", "y", "z"}, {0, 0, 0});
        chunk->append_column(std::move(col0), chunk->num_columns());

        // "b" is declared REQUIRED but carries a null in row 1
        auto col1 = ColumnTestHelper::build_nullable_column<int64_t>({100, 0, 300}, {0, 1, 0});
        chunk->append_column(std::move(col1), chunk->num_columns());
    }

    auto st = writer->write(chunk.get());
    ASSERT_FALSE(st.ok()) << "writing NULL into a REQUIRED column must fail";
    EXPECT_TRUE(st.message().find("b") != std::string::npos)
            << "error should name the offending column, got: " << st.message();
}

TEST_F(ParquetFileWriterTest, TestRequiredColumnRejectsNullBeforeWritingAnyColumn) {
    // "a" OPTIONAL, "b" REQUIRED: only "b" is evaluated up front, so this pins that a violation
    // on "b" still stops "a" from reaching the row group, which would otherwise leave the
    // writer holding a row group with mismatched column chunk lengths.
    std::vector type_descs{TYPE_VARCHAR_DESC, TYPE_BIGINT_DESC};
    std::vector<std::string> column_names = {"a", "b"};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs, {true, false}, column_names));

    auto good_chunk = std::make_shared<Chunk>();
    {
        auto col0 = ColumnTestHelper::build_column<Slice>({"x", "y", "z"});
        good_chunk->append_column(std::move(col0), 0);
        auto col1 = ColumnTestHelper::build_column<int64_t>({1, 2, 3});
        good_chunk->append_column(std::move(col1), 1);
    }
    ASSERT_OK(writer->write(good_chunk.get()));
    ASSERT_NE(writer->_rowgroup_writer, nullptr);
    const int64_t buffered_before = writer->_rowgroup_writer->estimated_buffered_bytes();
    ASSERT_GT(buffered_before, 0);

    auto bad_chunk = std::make_shared<Chunk>();
    {
        // column "a" is perfectly valid ...
        auto col0 = ColumnTestHelper::build_nullable_column<Slice>({"p", "q", "r"}, {0, 0, 0});
        bad_chunk->append_column(std::move(col0), bad_chunk->num_columns());
        // ... but column "b" is not, so nothing from this chunk may reach the row group
        auto col1 = ColumnTestHelper::build_nullable_column<int64_t>({4, 0, 6}, {0, 1, 0});
        bad_chunk->append_column(std::move(col1), bad_chunk->num_columns());
    }
    ASSERT_FALSE(writer->write(bad_chunk.get()).ok());

    EXPECT_EQ(writer->_rowgroup_writer->estimated_buffered_bytes(), buffered_before)
            << "column \"a\" of the rejected chunk leaked into the row group";
}

TEST_F(ParquetFileWriterTest, TestWriteIcebergRequiredStructColumn) {
    // The layout of an Iceberg table that has a NOT NULL struct column with a nested struct. The Iceberg sink makes
    // the top level columns required as the table says and gives every column a field id.
    auto key_type = TypeDescriptor::create_struct_type({"id"}, {TYPE_BIGINT_DESC});
    auto cdc_type = TypeDescriptor::create_struct_type(
            {"op", "ts", "partition", "offset", "source", "target", "gtid", "binlog_file", "binlog_pos", "pgtxid",
             "lsn", "snapshot", "key"},
            {TYPE_VARCHAR_DESC, TYPE_DATETIME_DESC, TYPE_INT_DESC, TYPE_BIGINT_DESC, TYPE_VARCHAR_DESC,
             TYPE_VARCHAR_DESC, TYPE_VARCHAR_DESC, TYPE_VARCHAR_DESC, TYPE_BIGINT_DESC, TYPE_BIGINT_DESC,
             TYPE_BIGINT_DESC, TYPE_VARCHAR_DESC, key_type});
    std::vector<TypeDescriptor> type_descs{TYPE_BIGINT_DESC, TYPE_BIGINT_DESC,  TYPE_INT_DESC,      TYPE_TIME_DESC,
                                           TYPE_TIME_DESC,   TYPE_VARCHAR_DESC, TYPE_DATETIME_DESC, cdc_type};
    std::vector<std::string> column_names{"id",       "schedule_id", "week_day",  "start_time",
                                          "end_time", "_cdc_op",     "_cdc_tsms", "_cdc"};
    std::vector<bool> nullable{false, true, true, true, true, false, false, false};

    // field ids: 1-7 for the scalar columns, 8 for _cdc, 9-20 for its scalar fields, 21 for _cdc.key, 22 for its field
    std::vector<FileColumnId> cdc_field_ids;
    for (int32_t id = 9; id <= 20; id++) {
        cdc_field_ids.push_back(FileColumnId{id, {}});
    }
    cdc_field_ids.push_back(FileColumnId{21, {FileColumnId{22, {}}}});
    std::vector<FileColumnId> field_ids;
    for (int32_t id = 1; id <= 7; id++) {
        field_ids.push_back(FileColumnId{id, {}});
    }
    field_ids.push_back(FileColumnId{8, std::move(cdc_field_ids)});
    _writer_options->column_ids = std::move(field_ids);

    Columns cdc_fields{
            make_nullable_varchar_column("op", {0, 0, 0, 0, 1}),
            make_timestamp_column(
                    {TimestampValue::create(2024, 1, 1, 0, 0, 1), TimestampValue::create(2024, 1, 2, 3, 4, 5),
                     TimestampValue::create(1970, 1, 1, 0, 0, 0), TimestampValue::create(1999, 9, 10, 1, 1, 1),
                     TimestampValue::create(2023, 9, 9, 23, 59, 59)},
                    {0, 0, 1, 0, 0}),
            ColumnTestHelper::build_nullable_column<int32_t>({0, 1, 2, 3, 4}, {0, 0, 0, 0, 0}),
            ColumnTestHelper::build_nullable_column<int64_t>({100, 0, 102, 103, 104}, {0, 1, 0, 0, 0}),
            make_nullable_varchar_column("source", {0, 0, 0, 0, 0}),
            make_nullable_varchar_column("target", {1, 0, 0, 0, 1}),
            make_nullable_varchar_column("gtid", {0, 1, 1, 0, 0}),
            make_nullable_varchar_column("binlog_file", {0, 0, 0, 1, 0}),
            ColumnTestHelper::build_nullable_column<int64_t>({1, 2, 3, 4, 5}, {0, 0, 0, 0, 0}),
            ColumnTestHelper::build_nullable_column<int64_t>({10, 0, 30, 40, 50}, {0, 1, 0, 0, 0}),
            ColumnTestHelper::build_nullable_column<int64_t>({0, 0, 0, 0, 0}, {1, 1, 1, 1, 1}),
            make_nullable_varchar_column("snapshot", {0, 0, 0, 0, 0})};
    // _cdc.key is NULL in row 3, and _cdc.key.id is NULL in row 1
    Columns key_fields{ColumnTestHelper::build_nullable_column<int64_t>({100, 0, 300, 400, 500}, {0, 1, 0, 0, 0})};
    auto key_struct = StructColumn::create(std::move(key_fields), key_type.field_names);
    cdc_fields.emplace_back(
            NullableColumn::create(std::move(key_struct), ColumnTestHelper::build_column<uint8_t>({0, 0, 0, 1, 0})));
    auto cdc_column = StructColumn::create(std::move(cdc_fields), cdc_type.field_names);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnTestHelper::build_column<int64_t>({1, 2, 3, 4, 5}), 0);
    chunk->append_column(ColumnTestHelper::build_nullable_column<int64_t>({10, 0, 30, 40, 0}, {0, 1, 0, 0, 1}), 1);
    chunk->append_column(ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 0, 4, 5}, {0, 0, 1, 0, 0}), 2);
    chunk->append_column(ColumnTestHelper::build_nullable_column<double>({3600, 0, 7200, 0, 86399}, {0, 1, 0, 0, 0}),
                         3);
    chunk->append_column(ColumnTestHelper::build_nullable_column<double>({0, 60, 0, 120, 180}, {1, 0, 1, 0, 0}), 4);
    chunk->append_column(ColumnTestHelper::build_column<Slice>({"c", "u", "d", "c", "u"}), 5);
    chunk->append_column(
            make_timestamp_column(
                    {TimestampValue::create(2024, 5, 1, 10, 0, 0), TimestampValue::create(2024, 5, 2, 11, 0, 0),
                     TimestampValue::create(2024, 5, 3, 12, 0, 0), TimestampValue::create(2024, 5, 4, 13, 0, 0),
                     TimestampValue::create(2024, 5, 5, 14, 0, 0)},
                    {}),
            6);
    chunk->append_column(std::move(cdc_column), 7);

    _write_and_check_round_trip(type_descs, column_names, nullable, chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteRequiredAndOptionalStruct) {
    auto struct_type = TypeDescriptor::create_struct_type({"a", "b"}, {TYPE_INT_DESC, TYPE_VARCHAR_DESC});
    std::vector<TypeDescriptor> type_descs{struct_type, struct_type, struct_type};
    std::vector<std::string> column_names{"required_struct", "optional_struct", "optional_struct_without_nulls"};
    std::vector<bool> nullable{false, true, true};

    auto make_struct = [&]() {
        Columns fields{ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3, 4}, {0, 1, 0, 0}),
                       make_nullable_varchar_column("b", {0, 0, 1, 0})};
        return StructColumn::create(std::move(fields), struct_type.field_names);
    };

    // The first struct is required. The second is optional and has a NULL row. The third is optional, but its column
    // is not nullable.
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(make_struct(), 0);
    chunk->append_column(NullableColumn::create(make_struct(), ColumnTestHelper::build_column<uint8_t>({0, 1, 0, 0})),
                         1);
    chunk->append_column(make_struct(), 2);

    _write_and_check_round_trip(type_descs, column_names, nullable, chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteRequiredNestedStruct) {
    auto inner_type = TypeDescriptor::create_struct_type({"x", "y"}, {TYPE_BIGINT_DESC, TYPE_VARCHAR_DESC});
    auto outer_type = TypeDescriptor::create_struct_type({"a", "inner"}, {TYPE_INT_DESC, inner_type});
    std::vector<TypeDescriptor> type_descs{outer_type, outer_type};
    std::vector<std::string> column_names{"required_outer", "optional_outer"};
    std::vector<bool> nullable{false, true};

    // the inner struct is NULL in row 2, its field x is NULL in row 1
    auto make_outer = [&]() {
        Columns inner_fields{ColumnTestHelper::build_nullable_column<int64_t>({10, 0, 30, 40}, {0, 1, 0, 0}),
                             make_nullable_varchar_column("y", {0, 1, 0, 0})};
        auto inner = StructColumn::create(std::move(inner_fields), inner_type.field_names);
        Columns outer_fields{
                ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3, 4}, {0, 0, 0, 1}),
                NullableColumn::create(std::move(inner), ColumnTestHelper::build_column<uint8_t>({0, 0, 1, 0}))};
        return StructColumn::create(std::move(outer_fields), outer_type.field_names);
    };

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(make_outer(), 0);
    chunk->append_column(NullableColumn::create(make_outer(), ColumnTestHelper::build_column<uint8_t>({0, 1, 0, 0})),
                         1);

    _write_and_check_round_trip(type_descs, column_names, nullable, chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteRequiredAndOptionalArrayOfStruct) {
    auto element_type = TypeDescriptor::create_struct_type({"a", "b"}, {TYPE_INT_DESC, TYPE_VARCHAR_DESC});
    auto array_type = TypeDescriptor::create_array_type(element_type);
    std::vector<TypeDescriptor> type_descs{array_type, array_type};
    std::vector<std::string> column_names{"required_array", "optional_array"};
    std::vector<bool> nullable{false, true};

    // elements: {1, b0}, {2, NULL}, NULL, {4, b3}, {NULL, b4}
    auto make_elements = [&]() {
        Columns fields{ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3, 4, 0}, {0, 0, 0, 0, 1}),
                       make_nullable_varchar_column("b", {0, 1, 0, 0, 0})};
        auto element = StructColumn::create(std::move(fields), element_type.field_names);
        return NullableColumn::create(std::move(element), ColumnTestHelper::build_column<uint8_t>({0, 0, 1, 0, 0}));
    };

    // rows: [e0, e1], [], [e2, e3, e4] for the required array; [e0, e1], NULL, [e2, e3, e4] for the optional array
    auto required_array = ArrayColumn::create(make_elements(), ColumnTestHelper::build_column<uint32_t>({0, 2, 2, 5}));
    auto optional_array = ArrayColumn::create(make_elements(), ColumnTestHelper::build_column<uint32_t>({0, 2, 2, 5}));

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(required_array), 0);
    chunk->append_column(
            NullableColumn::create(std::move(optional_array), ColumnTestHelper::build_column<uint8_t>({0, 1, 0})), 1);

    _write_and_check_round_trip(type_descs, column_names, nullable, chunk.get());
}

TEST_F(ParquetFileWriterTest, TestWriteRequiredAndOptionalMapWithStructValue) {
    auto value_type = TypeDescriptor::create_struct_type({"a", "b"}, {TYPE_INT_DESC, TYPE_VARCHAR_DESC});
    auto map_type = TypeDescriptor::create_map_type(TYPE_INT_DESC, value_type);
    std::vector<TypeDescriptor> type_descs{map_type, map_type};
    std::vector<std::string> column_names{"required_map", "optional_map"};
    std::vector<bool> nullable{false, true};

    // entries: 1 -> {1, b0}, 2 -> {2, NULL}, 3 -> NULL, 4 -> {NULL, b3}
    auto make_map = [&]() {
        auto keys = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3, 4}, {0, 0, 0, 0});
        Columns fields{ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3, 0}, {0, 0, 0, 1}),
                       make_nullable_varchar_column("b", {0, 1, 0, 0})};
        auto value = StructColumn::create(std::move(fields), value_type.field_names);
        auto values = NullableColumn::create(std::move(value), ColumnTestHelper::build_column<uint8_t>({0, 0, 1, 0}));
        return MapColumn::create(std::move(keys), std::move(values),
                                 ColumnTestHelper::build_column<uint32_t>({0, 2, 2, 4}));
    };

    // rows: {e0, e1}, {}, {e2, e3} for the required map; {e0, e1}, NULL, {e2, e3} for the optional map
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(make_map(), 0);
    chunk->append_column(NullableColumn::create(make_map(), ColumnTestHelper::build_column<uint8_t>({0, 1, 0})), 1);

    _write_and_check_round_trip(type_descs, column_names, nullable, chunk.get());
}

TEST_F(ParquetFileWriterTest, TestRequiredStructRejectsNull) {
    auto struct_type = TypeDescriptor::create_struct_type({"a"}, {TYPE_INT_DESC});
    std::vector<TypeDescriptor> type_descs{struct_type};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs, {false}, {"s"}));

    Columns fields{ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3}, {0, 0, 0})};
    auto struct_column = StructColumn::create(std::move(fields), struct_type.field_names);
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(
            NullableColumn::create(std::move(struct_column), ColumnTestHelper::build_column<uint8_t>({0, 1, 0})), 0);

    auto st = writer->write(chunk.get());
    ASSERT_FALSE(st.ok()) << "writing NULL into a REQUIRED struct must fail";
    EXPECT_TRUE(st.message().find("'s'") != std::string::npos) << st.message();
}

TEST_F(ParquetFileWriterTest, TestLevelBuilderRejectsMismatchedSchemaNode) {
    auto struct_type = TypeDescriptor::create_struct_type({"a"}, {TYPE_INT_DESC});
    auto array_type = TypeDescriptor::create_array_type(TYPE_INT_DESC);

    auto make_int_node = [](const std::string& name) -> ::parquet::schema::NodePtr {
        return ::parquet::schema::PrimitiveNode::Make(name, ::parquet::Repetition::OPTIONAL,
                                                      ::parquet::LogicalType::Int(32, true), ::parquet::Type::INT32);
    };
    auto write = write_with_level_builder;

    ColumnPtr int_column = ColumnTestHelper::build_column<int32_t>({1, 2});

    // a struct type needs a group node
    ASSERT_ERROR(write(struct_type, make_int_node("s"), int_column));

    // a struct type of one field needs a group node of one field
    auto two_fields_node = ::parquet::schema::GroupNode::Make("s", ::parquet::Repetition::REQUIRED,
                                                              {make_int_node("a"), make_int_node("b")});
    ASSERT_ERROR(write(struct_type, two_fields_node, int_column));

    // the node of a struct matches, but the column is not a struct column
    auto struct_node = ::parquet::schema::GroupNode::Make("s", ::parquet::Repetition::REQUIRED, {make_int_node("a")});
    ASSERT_ERROR(write(struct_type, struct_node, int_column));

    // an array type needs the three level LIST layout, not a group that holds the element directly
    auto flat_list_node =
            ::parquet::schema::GroupNode::Make("l", ::parquet::Repetition::OPTIONAL, {make_int_node("e")});
    ASSERT_ERROR(write(array_type, flat_list_node, int_column));

    // a scalar type needs a primitive node
    ASSERT_ERROR(write(TYPE_INT_DESC, struct_node, int_column));
}

TEST_F(ParquetFileWriterTest, TestLevelBuilderRejectsMismatchedColumnClass) {
    auto make_node =
            [](::parquet::Type::type physical_type,
               const std::shared_ptr<const ::parquet::LogicalType>& logical_type) -> ::parquet::schema::NodePtr {
        return ::parquet::schema::PrimitiveNode::Make("a", ::parquet::Repetition::OPTIONAL, logical_type,
                                                      physical_type);
    };
    auto int_node = make_node(::parquet::Type::INT32, ::parquet::LogicalType::Int(32, true));
    auto string_node = make_node(::parquet::Type::BYTE_ARRAY, ::parquet::LogicalType::String());
    auto bool_node = make_node(::parquet::Type::BOOLEAN, ::parquet::LogicalType::None());
    auto datetime_node =
            make_node(::parquet::Type::INT64,
                      ::parquet::LogicalType::Timestamp(false, ::parquet::LogicalType::TimeUnit::unit::MICROS));

    ColumnPtr int_column = ColumnTestHelper::build_column<int32_t>({1, 2});
    ColumnPtr bigint_column = ColumnTestHelper::build_column<int64_t>({1, 2});
    ColumnPtr varchar_column = ColumnTestHelper::build_column<Slice>({"x", "y"});

    // the class of the column has to be the one of the logical type
    ASSERT_ERROR(write_with_level_builder(TYPE_INT_DESC, int_node, bigint_column));
    ASSERT_ERROR(write_with_level_builder(TYPE_VARCHAR_DESC, string_node, bigint_column));
    ASSERT_ERROR(write_with_level_builder(TYPE_BOOLEAN_DESC, bool_node, varchar_column));
    ASSERT_ERROR(write_with_level_builder(TYPE_DATETIME_DESC, datetime_node, bigint_column));

    ASSERT_OK(write_with_level_builder(TYPE_INT_DESC, int_node, int_column));
    ASSERT_OK(write_with_level_builder(TYPE_VARCHAR_DESC, string_node, varchar_column));
}

TEST_F(ParquetFileWriterTest, TestWriteConstColumns) {
    // A literal in the select list gives a constant column. The writer has to expand it, also when it is a field of
    // a struct or the elements of an array. The elements of an array are nullable, so only a NULL literal can be
    // their constant column.
    auto struct_type = TypeDescriptor::create_struct_type({"a", "b"}, {TYPE_INT_DESC, TYPE_VARCHAR_DESC});
    auto array_type = TypeDescriptor::create_array_type(TYPE_INT_DESC);
    std::vector<TypeDescriptor> type_descs{TYPE_INT_DESC, TYPE_VARCHAR_DESC, TYPE_BIGINT_DESC, struct_type, array_type};
    std::vector<std::string> column_names{"required_literal", "varchar_literal", "null_literal", "struct_with_literal",
                                          "array_with_literal"};
    std::vector<bool> nullable{false, true, true, true, true};
    const size_t num_rows = 4;

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_const_column<TYPE_INT>(42, num_rows), 0);
    chunk->append_column(ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("abc"), num_rows), 1);
    chunk->append_column(ColumnHelper::create_const_null_column(num_rows), 2);
    {
        Columns fields{ColumnHelper::create_const_column<TYPE_INT>(7, num_rows),
                       make_nullable_varchar_column("b", {0, 1, 0, 0})};
        chunk->append_column(StructColumn::create(std::move(fields), struct_type.field_names), 3);
    }
    // rows: [NULL], [], [NULL], [NULL]
    chunk->append_column(ArrayColumn::create(ColumnHelper::create_const_null_column(3),
                                             ColumnTestHelper::build_column<uint32_t>({0, 1, 1, 2, 3})),
                         4);

    auto expected = std::make_shared<Chunk>();
    expected->append_column(ColumnTestHelper::build_column<int32_t>({42, 42, 42, 42}), 0);
    expected->append_column(ColumnTestHelper::build_column<Slice>({"abc", "abc", "abc", "abc"}), 1);
    expected->append_column(ColumnTestHelper::build_nullable_column<int64_t>({0, 0, 0, 0}, {1, 1, 1, 1}), 2);
    {
        Columns fields{ColumnTestHelper::build_column<int32_t>({7, 7, 7, 7}),
                       make_nullable_varchar_column("b", {0, 1, 0, 0})};
        expected->append_column(StructColumn::create(std::move(fields), struct_type.field_names), 3);
    }
    expected->append_column(ArrayColumn::create(ColumnTestHelper::build_nullable_column<int32_t>({0, 0, 0}, {1, 1, 1}),
                                                ColumnTestHelper::build_column<uint32_t>({0, 1, 1, 2, 3})),
                            4);

    _write_and_check_round_trip(type_descs, column_names, nullable, chunk.get(), expected.get());
}

TEST_F(ParquetFileWriterTest, TestWriteNestedRequiredFields) {
    auto inner_type = TypeDescriptor::create_struct_type({"x", "y"}, {TYPE_BIGINT_DESC, TYPE_VARCHAR_DESC});
    auto s_type =
            TypeDescriptor::create_struct_type({"a", "b", "inner"}, {TYPE_INT_DESC, TYPE_VARCHAR_DESC, inner_type});
    auto element_type = TypeDescriptor::create_struct_type({"p", "q"}, {TYPE_INT_DESC, TYPE_VARCHAR_DESC});
    auto arr_type = TypeDescriptor::create_array_type(element_type);
    auto value_type = TypeDescriptor::create_struct_type({"u"}, {TYPE_INT_DESC});
    auto map_type = TypeDescriptor::create_map_type(TYPE_INT_DESC, value_type);
    std::vector<TypeDescriptor> type_descs{s_type, arr_type, map_type};
    std::vector<std::string> column_names{"s", "arr", "m"};
    std::vector<bool> nullable{true, true, true};

    // Required are s.a, s.inner, s.inner.x, arr.element, arr.element.p, m.value and m.value.u.
    _writer_options->column_ids = std::vector<FileColumnId>{
            FileColumnId{1,
                         {FileColumnId{2, {}, true}, FileColumnId{3, {}},
                          FileColumnId{4, {FileColumnId{5, {}, true}, FileColumnId{6, {}}}, true}}},
            FileColumnId{7, {FileColumnId{8, {FileColumnId{9, {}, true}, FileColumnId{10, {}}}, true}}},
            FileColumnId{11, {FileColumnId{12, {}}, FileColumnId{13, {FileColumnId{14, {}, true}}, true}}}};

    // Row 1 of s is NULL. The required fields a, inner and x are NULL there, which is allowed below a NULL struct.
    Columns inner_fields{ColumnTestHelper::build_nullable_column<int64_t>({10, 0, 30}, {0, 1, 0}),
                         make_nullable_varchar_column("y", {0, 0, 1})};
    auto inner = StructColumn::create(std::move(inner_fields), inner_type.field_names);
    Columns s_fields{ColumnTestHelper::build_nullable_column<int32_t>({1, 0, 3}, {0, 1, 0}),
                     make_nullable_varchar_column("b", {0, 0, 1}),
                     NullableColumn::create(std::move(inner), ColumnTestHelper::build_column<uint8_t>({0, 1, 0}))};
    auto s = StructColumn::create(std::move(s_fields), s_type.field_names);
    auto s_column = NullableColumn::create(std::move(s), ColumnTestHelper::build_column<uint8_t>({0, 1, 0}));

    // rows: [{1, q0}, {2, NULL}], NULL, [{3, q2}]
    Columns element_fields{ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3}, {0, 0, 0}),
                           make_nullable_varchar_column("q", {0, 1, 0})};
    auto element = StructColumn::create(std::move(element_fields), element_type.field_names);
    auto elements = NullableColumn::create(std::move(element), ColumnTestHelper::build_column<uint8_t>({0, 0, 0}));
    auto array = ArrayColumn::create(std::move(elements), ColumnTestHelper::build_column<uint32_t>({0, 2, 2, 3}));
    auto arr_column = NullableColumn::create(std::move(array), ColumnTestHelper::build_column<uint8_t>({0, 1, 0}));

    // rows: {1: {5}}, NULL, {2: {6}, 3: {7}}
    Columns value_fields{ColumnTestHelper::build_nullable_column<int32_t>({5, 6, 7}, {0, 0, 0})};
    auto value = StructColumn::create(std::move(value_fields), value_type.field_names);
    auto values = NullableColumn::create(std::move(value), ColumnTestHelper::build_column<uint8_t>({0, 0, 0}));
    auto map = MapColumn::create(ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3}, {0, 0, 0}),
                                 std::move(values), ColumnTestHelper::build_column<uint32_t>({0, 1, 1, 3}));
    auto m_column = NullableColumn::create(std::move(map), ColumnTestHelper::build_column<uint8_t>({0, 1, 0}));

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(s_column), 0);
    chunk->append_column(std::move(arr_column), 1);
    chunk->append_column(std::move(m_column), 2);

    auto check_schema = [](const parquet::SchemaDescriptor& schema) {
        const auto* s = schema.get_stored_column_by_field_idx(0);
        ASSERT_EQ(3U, s->children.size());
        EXPECT_FALSE(s->children[0].is_nullable);             // a
        EXPECT_TRUE(s->children[1].is_nullable);              // b
        EXPECT_FALSE(s->children[2].is_nullable);             // inner
        EXPECT_FALSE(s->children[2].children[0].is_nullable); // inner.x
        EXPECT_TRUE(s->children[2].children[1].is_nullable);  // inner.y

        const auto* arr = schema.get_stored_column_by_field_idx(1);
        const auto& element = arr->children[0];
        EXPECT_FALSE(element.is_nullable);
        EXPECT_FALSE(element.children[0].is_nullable); // p
        EXPECT_TRUE(element.children[1].is_nullable);  // q

        const auto* m = schema.get_stored_column_by_field_idx(2);
        EXPECT_FALSE(m->children[0].is_nullable); // key
        const auto& value = m->children[1];
        EXPECT_FALSE(value.is_nullable);
        EXPECT_FALSE(value.children[0].is_nullable); // u
    };

    _write_and_check_round_trip(type_descs, column_names, nullable, chunk.get(), nullptr, check_schema);
}

TEST_F(ParquetFileWriterTest, TestRequiredNestedStructFieldRejectsNullBeforeWritingAnyColumn) {
    auto struct_type = TypeDescriptor::create_struct_type({"a", "b"}, {TYPE_INT_DESC, TYPE_VARCHAR_DESC});
    std::vector<TypeDescriptor> type_descs{TYPE_INT_DESC, struct_type};
    std::vector<std::string> column_names{"first", "s"};
    _writer_options->column_ids = std::vector<FileColumnId>{
            FileColumnId{1, {}}, FileColumnId{2, {FileColumnId{3, {}, true}, FileColumnId{4, {}}}}};
    ASSIGN_OR_ASSERT_FAIL(auto writer, _create_writer(type_descs, {true, true}, column_names));

    // s.a is required
    auto make_chunk = [&](const std::vector<uint8_t>& a_nulls) {
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3}, {0, 0, 0}), 0);
        Columns fields{ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3}, a_nulls),
                       make_nullable_varchar_column("b", {0, 0, 0})};
        chunk->append_column(StructColumn::create(std::move(fields), struct_type.field_names), 1);
        return chunk;
    };

    ASSERT_OK(writer->write(make_chunk({0, 0, 0}).get()));
    ASSERT_NE(writer->_rowgroup_writer, nullptr);
    const int64_t buffered_before = writer->_rowgroup_writer->estimated_buffered_bytes();

    auto st = writer->write(make_chunk({0, 1, 0}).get());
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.message().find("'s.a'"), std::string::npos) << st.message();
    EXPECT_EQ(writer->_rowgroup_writer->estimated_buffered_bytes(), buffered_before)
            << "column \"first\" of the rejected chunk leaked into the row group";
}

TEST_F(ParquetFileWriterTest, TestRequiredNestedStructRejectsNull) {
    auto inner_type = TypeDescriptor::create_struct_type({"x"}, {TYPE_INT_DESC});
    auto struct_type = TypeDescriptor::create_struct_type({"inner"}, {inner_type});
    Columns inner_fields{ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3}, {0, 0, 0})};
    auto inner = StructColumn::create(std::move(inner_fields), inner_type.field_names);
    // s.inner is NULL in row 1, while s is not
    Columns fields{NullableColumn::create(std::move(inner), ColumnTestHelper::build_column<uint8_t>({0, 1, 0}))};
    auto column = StructColumn::create(std::move(fields), struct_type.field_names);

    _expect_nested_null_rejected(struct_type, FileColumnId{1, {FileColumnId{2, {FileColumnId{3, {}}}, true}}}, column,
                                 "c.inner");
}

TEST_F(ParquetFileWriterTest, TestRequiredArrayElementRejectsNull) {
    auto array_type = TypeDescriptor::create_array_type(TYPE_INT_DESC);
    // the second element of the first array is NULL
    auto column = ArrayColumn::create(ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3}, {0, 1, 0}),
                                      ColumnTestHelper::build_column<uint32_t>({0, 2, 3}));

    _expect_nested_null_rejected(array_type, FileColumnId{1, {FileColumnId{2, {}, true}}}, std::move(column),
                                 "c.element");
}

TEST_F(ParquetFileWriterTest, TestRequiredMapValueRejectsNull) {
    auto map_type = TypeDescriptor::create_map_type(TYPE_INT_DESC, TYPE_INT_DESC);
    auto column = MapColumn::create(ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 3}, {0, 0, 0}),
                                    ColumnTestHelper::build_nullable_column<int32_t>({4, 5, 6}, {0, 1, 0}),
                                    ColumnTestHelper::build_column<uint32_t>({0, 2, 3}));

    _expect_nested_null_rejected(map_type, FileColumnId{1, {FileColumnId{2, {}}, FileColumnId{3, {}, true}}},
                                 std::move(column), "c.value");
}

TEST_F(ParquetFileWriterTest, TestWriterRejectsMoreNamesThanColumns) {
    std::vector<TypeDescriptor> type_descs{TYPE_INT_DESC};
    ASSERT_ERROR(_create_writer(type_descs, {}, {"a", "b"}));
}

TEST_F(ParquetFileWriterTest, TestWriterRejectsMoreNullableFlagsThanColumns) {
    std::vector<TypeDescriptor> type_descs{TYPE_INT_DESC};
    ASSERT_ERROR(_create_writer(type_descs, {true, false}, {"a"}));
}

TEST_F(ParquetFileWriterTest, TestWriterRejectsFewerFieldIdsThanColumns) {
    std::vector<TypeDescriptor> type_descs{TYPE_INT_DESC, TYPE_BIGINT_DESC};
    _writer_options->column_ids = {FileColumnId{1, {}}};
    ASSERT_ERROR(_create_writer(type_descs, {}, {"a", "b"}));
}

TEST_F(ParquetFileWriterTest, TestInitReturnsErrorWhenOutputStreamFails) {
    std::vector<TypeDescriptor> type_descs{TYPE_INT_DESC};
    ParquetFileWriter writer(_file_path, std::make_shared<FailingOutputStream>(), {"a"}, type_descs,
                             ColumnSlotIdEvaluator::from_types(type_descs), _compression_type, _writer_options, [] {});

    // opening the writer writes the magic number of the file, which fails and throws in parquet-cpp
    auto st = writer.init();
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.message().find("disk is full"), std::string::npos) << st.message();

    // closing a writer that is not initialized reports an error instead of reading its file metadata
    auto result = writer.close();
    ASSERT_FALSE(result.io_status.ok());
}

TEST_F(ParquetFileWriterTest, TestColumnDictionaryEncodingDisabled) {
    // Test that disabling dictionary encoding for specific column works correctly
    // This test verifies the fix for Iceberg position delete file compression issue
    std::vector type_descs{TYPE_VARCHAR_DESC, TYPE_BIGINT_DESC};
    std::vector<std::string> column_names = {"file_path", "pos"};

    auto column_evaluators = std::make_shared<std::vector<std::unique_ptr<ColumnEvaluator>>>(
            ColumnSlotIdEvaluator::from_types(type_descs));
    // Use a unique file path for this test to avoid conflicts with _file_path
    std::string file_path = "/test_dict_disabled.parquet";
    auto fs = std::shared_ptr<MemoryFileSystem>(&_fs, [](MemoryFileSystem*) {});
    auto factory = ParquetFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, {}, column_names, column_evaluators,
                                            std::nullopt, nullptr, _runtime_state);

    // Disable dictionary encoding for "pos" column (simulating Iceberg delete file optimization)
    std::unordered_map<std::string, bool> column_dict_config;
    column_dict_config["pos"] = false;
    factory.set_column_dictionary_enabled(std::move(column_dict_config));

    ASSERT_OK(factory.init());

    auto maybe_writer = factory.create(file_path);
    ASSERT_OK(maybe_writer.status());

    auto writer = std::move(maybe_writer.value().writer);
    ASSERT_OK(writer->init());

    // Write monotonically increasing pos values (typical for position delete files)
    auto chunk = std::make_shared<Chunk>();
    {
        // file_path column - repeated values (good for dictionary encoding)
        auto col0 = ColumnTestHelper::build_column<Slice>({"s3://bucket/table/data/file1.parquet",
                                                           "s3://bucket/table/data/file1.parquet",
                                                           "s3://bucket/table/data/file1.parquet"});
        chunk->append_column(std::move(col0), 0);

        // pos column - monotonically increasing (poor for dictionary encoding, should use PLAIN)
        auto col1 = ColumnTestHelper::build_column<int64_t>({100, 200, 300});
        chunk->append_column(std::move(col1), 1);
    }

    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();
    ASSERT_OK(result.io_status);
    ASSERT_EQ(result.file_statistics.record_count, 3);

    // Verify encoding using StarRocks parquet FileReader
    ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(file_path));
    ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(file_path));
    auto file_reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), file_size);

    auto ctx = _create_scan_context(type_descs);
    ASSERT_OK(file_reader->init(ctx));
    auto file_metadata = file_reader->get_file_metadata();

    ASSERT_EQ(file_metadata->t_metadata().row_groups.size(), 1);

    // Check row group 0
    const auto& row_group = file_metadata->t_metadata().row_groups[0];

    // file_path column (index 0) - should use dictionary encoding
    const auto& file_path_col = row_group.columns[0];
    const auto& file_path_encodings = file_path_col.meta_data.encodings;
    bool has_dict_encoding_file_path = false;
    for (const auto& enc : file_path_encodings) {
        if (enc == tparquet::Encoding::RLE_DICTIONARY || enc == tparquet::Encoding::PLAIN_DICTIONARY) {
            has_dict_encoding_file_path = true;
            break;
        }
    }
    EXPECT_TRUE(has_dict_encoding_file_path) << "file_path column should use dictionary encoding (repeated values)";

    // pos column (index 1) - should NOT use dictionary encoding (disabled)
    const auto& pos_col = row_group.columns[1];
    const auto& pos_encodings = pos_col.meta_data.encodings;
    bool has_dict_encoding_pos = false;
    for (const auto& enc : pos_encodings) {
        if (enc == tparquet::Encoding::RLE_DICTIONARY || enc == tparquet::Encoding::PLAIN_DICTIONARY) {
            has_dict_encoding_pos = true;
            break;
        }
    }
    EXPECT_FALSE(has_dict_encoding_pos) << "pos column should NOT use dictionary encoding (configured to disable)";

    // Verify pos column uses PLAIN encoding
    bool has_plain_encoding = false;
    for (const auto& enc : pos_encodings) {
        if (enc == tparquet::Encoding::PLAIN) {
            has_plain_encoding = true;
            break;
        }
    }
    EXPECT_TRUE(has_plain_encoding) << "pos column should use PLAIN encoding";
}

TEST_F(ParquetFileWriterTest, TestColumnDictionaryEncodingEnabledByDefault) {
    // Test that dictionary encoding is enabled by default when not explicitly disabled
    std::vector type_descs{TYPE_BIGINT_DESC};
    std::vector<std::string> column_names = {"col1"};

    auto column_evaluators = std::make_shared<std::vector<std::unique_ptr<ColumnEvaluator>>>(
            ColumnSlotIdEvaluator::from_types(type_descs));
    // Use a unique file path for this test to avoid conflicts with _file_path
    std::string file_path = "/test_dict_default.parquet";
    auto fs = std::shared_ptr<MemoryFileSystem>(&_fs, [](MemoryFileSystem*) {});
    auto factory = ParquetFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, {}, column_names, column_evaluators,
                                            std::nullopt, nullptr, _runtime_state);

    // Do NOT call set_column_dictionary_enabled - should use default (enabled)
    ASSERT_OK(factory.init());

    auto maybe_writer = factory.create(file_path);
    ASSERT_OK(maybe_writer.status());

    auto writer = std::move(maybe_writer.value().writer);
    ASSERT_OK(writer->init());

    // Write repeated values (good for dictionary encoding)
    auto chunk = std::make_shared<Chunk>();
    {
        auto col = ColumnTestHelper::build_column<int64_t>({100, 100, 100, 100, 100});
        chunk->append_column(std::move(col), 0);
    }

    ASSERT_OK(writer->write(chunk.get()));
    auto result = writer->close();
    ASSERT_OK(result.io_status);

    // Verify encoding using StarRocks parquet FileReader
    ASSIGN_OR_ABORT(auto file, _fs.new_random_access_file(file_path));
    ASSIGN_OR_ABORT(auto file_size, _fs.get_file_size(file_path));
    auto file_reader = std::make_shared<parquet::FileReader>(config::vector_chunk_size, file.get(), file_size);

    auto ctx = _create_scan_context(type_descs);
    ASSERT_OK(file_reader->init(ctx));
    auto file_metadata = file_reader->get_file_metadata();

    const auto& row_group = file_metadata->t_metadata().row_groups[0];
    const auto& col_chunk = row_group.columns[0];
    const auto& encodings = col_chunk.meta_data.encodings;

    // Should have dictionary encoding (default behavior)
    bool has_dict_encoding = false;
    for (const auto& enc : encodings) {
        if (enc == tparquet::Encoding::RLE_DICTIONARY || enc == tparquet::Encoding::PLAIN_DICTIONARY) {
            has_dict_encoding = true;
            break;
        }
    }
    EXPECT_TRUE(has_dict_encoding) << "Column should use dictionary encoding by default";
}

} // namespace starrocks::formats
