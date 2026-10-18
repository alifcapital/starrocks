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

#include "connector/iceberg_chunk_sink.h"

#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <future>
#include <optional>
#include <thread>

#include "connector/connector_chunk_sink.h"
#include "connector/sink_memory_manager.h"
#include "exec/pipeline/fragment_context.h"
#include "formats/file_writer.h"
#include "formats/utils.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "testutil/assert.h"
#include "testutil/scoped_updater.h"
#include "util/defer_op.h"
#include "util/integer_util.h"

namespace starrocks::connector {
namespace {

using CommitResult = formats::FileWriter::CommitResult;
using WriterAndStream = formats::WriterAndStream;
using Stream = io::AsyncFlushOutputStream;
using ::testing::Return;
using ::testing::ByMove;
using ::testing::_;

class IcebergChunkSinkTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fragment_context = std::make_shared<pipeline::FragmentContext>();
        _fragment_context->set_runtime_state(std::make_shared<RuntimeState>());
        _runtime_state = _fragment_context->runtime_state();
    }

    void TearDown() override {}

    // Makes a descriptor table with one tuple of the columns the sink writes, and returns the id of the tuple.
    int create_tuple(const std::vector<TypeDescriptor>& column_types) {
        TDescriptorTableBuilder table_builder;
        TTupleDescriptorBuilder tuple_builder;
        for (size_t i = 0; i < column_types.size(); ++i) {
            TSlotDescriptorBuilder slot_builder;
            tuple_builder.add_slot(slot_builder.id(static_cast<int>(i))
                                           .type(column_types[i])
                                           .column_name("c" + std::to_string(i))
                                           .nullable(true)
                                           .build());
        }
        tuple_builder.build(&table_builder);
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size)
                      .ok());
        _runtime_state->set_desc_tbl(tbl);
        return 0;
    }

    ObjectPool _pool;
    std::shared_ptr<pipeline::FragmentContext> _fragment_context;
    RuntimeState* _runtime_state;
};

class MockFileWriterFactory : public formats::FileWriterFactory {
public:
    MOCK_METHOD(Status, init, (), (override));
    MOCK_METHOD(StatusOr<WriterAndStream>, create, (const std::string&), (const override));
};

class MockWriter final : public formats::FileWriter {
public:
    MOCK_METHOD(Status, init, (), (override));
    MOCK_METHOD(int64_t, get_written_bytes, (), (override));
    MOCK_METHOD(int64_t, get_allocated_bytes, (), (override));
    MOCK_METHOD(int64_t, get_flush_batch_size, (), (override));
    MOCK_METHOD(Status, write, (Chunk * chunk), (override));
    MOCK_METHOD(CommitResult, close, (), (override));
};

class MockFile final : public WritableFile {
public:
    MOCK_METHOD(Status, append, (const Slice& data), (override));
    MOCK_METHOD(Status, appendv, (const Slice* data, size_t cnt), (override));
    MOCK_METHOD(Status, pre_allocate, (uint64_t size), (override));
    MOCK_METHOD(Status, close, (), (override));
    MOCK_METHOD(Status, flush, (FlushMode mode), (override));
    MOCK_METHOD(Status, sync, (), (override));
    MOCK_METHOD(uint64_t, size, (), (const, override));
    MOCK_METHOD(const std::string&, filename, (), (const, override));
};

class MockPoller : public AsyncFlushStreamPoller {
public:
    MOCK_METHOD(void, enqueue, (std::shared_ptr<Stream> stream), (override));
};

TEST_F(IcebergChunkSinkTest, test_callback) {
    {
        std::vector<std::string> partition_column_names = {"k1"};
        std::vector<std::string> transform = {"identity"};
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators =
                ColumnSlotIdEvaluator::from_types({TypeDescriptor::from_logical_type(TYPE_VARCHAR)});
        auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
        auto location_provider = std::make_shared<LocationProvider>("base_path", "ffffff", 0, 0, "parquet");
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        EXPECT_CALL(*mock_writer_factory, create(::testing::_))
                .WillRepeatedly(::testing::Return(ByMove(StatusOr<WriterAndStream>(std::move(ws)))));

        auto partition_chunk_writer_ctx = std::make_shared<BufferPartitionChunkWriterContext>(
                BufferPartitionChunkWriterContext{mock_writer_factory, location_provider, 100, false});
        auto partition_chunk_writer_factory =
                std::make_unique<BufferPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
        auto sink = std::make_unique<connector::IcebergChunkSink>(
                partition_column_names, transform, std::move(partition_column_evaluators),
                std::move(partition_chunk_writer_factory), _runtime_state);
        auto poller = MockPoller();
        sink->set_io_poller(&poller);
        sink->init_profile();

        Columns partition_key_columns;
        ChunkPtr chunk = std::make_shared<Chunk>();
        std::vector<ChunkExtraColumnsMeta> extra_metas;
        std::string tmp = "abc";
        Datum datum;
        datum.set_slice(tmp);
        auto res = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
        res->append_datum(datum);
        auto ptr = ConstColumn::create(std::move(res), 1);
        partition_key_columns.emplace_back(ptr);
        extra_metas.push_back(ChunkExtraColumnsMeta{TYPE_VARCHAR_DESC, true /*useless*/, true /*useless*/});

        auto chunk_extra_data = std::make_shared<ChunkExtraColumnsData>(extra_metas, std::move(partition_key_columns));
        // Unlock during merging partition chunks into a full chunk.
        chunk->set_extra_data(chunk_extra_data);
        auto ret = sink->add(chunk);
        EXPECT_EQ(ret.ok(), true);
        sink->callback_on_commit(CommitResult{
                .io_status = Status::OK(),
                .format = formats::PARQUET,
                .file_statistics =
                        {
                                .record_count = 100,
                        },
                .location = "path/to/directory/data.parquet",
        }
                                         .set_extra_data("0"));
        sink->set_status(Status::OK());

        EXPECT_EQ(sink->is_finished(), true);
        EXPECT_EQ(sink->status().ok(), true);
        EXPECT_EQ(_runtime_state->num_rows_load_sink(), 100);
    }
}

TEST_F(IcebergChunkSinkTest, test_factory) {
    SCOPED_UPDATE(bool, config::enable_connector_sink_spill, false);
    IcebergChunkSinkProvider provider;

    {
        auto sink_ctx = std::make_shared<connector::IcebergChunkSinkContext>();
        sink_ctx->path = "/path/to/directory/";
        sink_ctx->column_names = {"k1", "k2"};
        sink_ctx->executor = nullptr;
        sink_ctx->format = formats::PARQUET; // iceberg sink only supports parquet
        sink_ctx->compression_type = TCompressionType::NO_COMPRESSION;
        sink_ctx->options = {}; // default for now
        sink_ctx->parquet_field_ids = {};
        sink_ctx->max_file_size = 1 << 30;
        sink_ctx->column_evaluators = ColumnSlotIdEvaluator::from_types(
                {TypeDescriptor::from_logical_type(TYPE_VARCHAR), TypeDescriptor::from_logical_type(TYPE_INT)});
        sink_ctx->fragment_context = _fragment_context.get();
        auto sink = provider.create_chunk_sink(sink_ctx, 0).value();
        SinkOperatorMemoryManager mm;
        sink->set_operator_mem_mgr(&mm);
        EXPECT_OK(sink->init());
    }

    {
        auto sink_ctx = std::make_shared<connector::IcebergChunkSinkContext>();
        sink_ctx->path = "/path/to/directory/";
        sink_ctx->column_names = {"k1", "k2"};
        sink_ctx->executor = nullptr;
        sink_ctx->format = "unknown";
        sink_ctx->compression_type = TCompressionType::NO_COMPRESSION;
        sink_ctx->options = {};
        sink_ctx->parquet_field_ids = {};
        sink_ctx->max_file_size = 1 << 30;
        sink_ctx->column_evaluators = ColumnSlotIdEvaluator::from_types(
                {TypeDescriptor::from_logical_type(TYPE_VARCHAR), TypeDescriptor::from_logical_type(TYPE_INT)});
        sink_ctx->fragment_context = _fragment_context.get();
        auto sink = provider.create_chunk_sink(sink_ctx, 0).value();
        SinkOperatorMemoryManager mm;
        sink->set_operator_mem_mgr(&mm);
        EXPECT_ERROR(sink->init()); // format is not supported
    }
}

// The spill writer aborts the process on a column type that has no storage field type (VARIANT), so the provider has
// to create the buffer writer factory for such a tuple, and the spill writer factory for the other tuples. TIME is
// described as DOUBLE by the writer, so a tuple with it uses the spill writer.
TEST_F(IcebergChunkSinkTest, test_factory_selects_writer_by_column_types) {
    IcebergChunkSinkProvider provider;

    // The tuple has |tuple_types| and the sink has the first |num_columns| columns of it.
    auto create_sink = [&](const std::vector<TypeDescriptor>& tuple_types, size_t num_columns, bool enable_spill,
                           int32_t driver_id) {
        SCOPED_UPDATE(bool, config::enable_connector_sink_spill, enable_spill);
        int tuple_desc_id = create_tuple(tuple_types);

        auto sink_ctx = std::make_shared<connector::IcebergChunkSinkContext>();
        sink_ctx->path = "/path/to/directory/";
        for (size_t i = 0; i < num_columns; ++i) {
            sink_ctx->column_names.push_back("c" + std::to_string(i));
        }
        sink_ctx->executor = nullptr;
        sink_ctx->format = formats::PARQUET;
        sink_ctx->compression_type = TCompressionType::NO_COMPRESSION;
        sink_ctx->options = {};
        sink_ctx->parquet_field_ids = {};
        sink_ctx->max_file_size = 1 << 30;
        sink_ctx->column_evaluators = ColumnSlotIdEvaluator::from_types(
                std::vector<TypeDescriptor>(tuple_types.begin(), tuple_types.begin() + num_columns));
        sink_ctx->fragment_context = _fragment_context.get();
        sink_ctx->tuple_desc_id = tuple_desc_id;
        return provider.create_chunk_sink(sink_ctx, driver_id).value();
    };
    auto uses_spill_writer = [](const ConnectorChunkSink& sink) {
        return dynamic_cast<const SpillPartitionChunkWriterFactory*>(sink.TEST_partition_chunk_writer_factory()) !=
               nullptr;
    };
    auto uses_buffer_writer = [](const ConnectorChunkSink& sink) {
        return dynamic_cast<const BufferPartitionChunkWriterFactory*>(sink.TEST_partition_chunk_writer_factory()) !=
               nullptr;
    };

    // A tuple with a nested struct and TIME columns, like the one of the table that crashed the CN.
    const TypeDescriptor struct_type = TypeDescriptor::create_struct_type(
            {"a", "b"}, {TYPE_INT_DESC, TypeDescriptor::create_struct_type({"c"}, {TYPE_VARCHAR_DESC})});
    const TypeDescriptor variant_type = TypeDescriptor::create_variant_type();
    std::vector<TypeDescriptor> supported_types = {TYPE_BIGINT_DESC, TYPE_INT_DESC,     TYPE_TIME_DESC,
                                                   TYPE_TIME_DESC,   TYPE_VARCHAR_DESC, TYPE_DATETIME_DESC,
                                                   struct_type};
    std::vector<TypeDescriptor> variant_types = supported_types;
    variant_types.push_back(variant_type);

    for (int32_t driver_id : {0, 1}) {
        auto sink = create_sink(supported_types, supported_types.size(), true, driver_id);
        EXPECT_TRUE(uses_spill_writer(*sink));
        EXPECT_FALSE(uses_buffer_writer(*sink));

        sink = create_sink(variant_types, variant_types.size(), true, driver_id);
        EXPECT_FALSE(uses_spill_writer(*sink));
        EXPECT_TRUE(uses_buffer_writer(*sink));

        // VARIANT inside a nested type is a column type without a storage field type as well.
        sink = create_sink({TYPE_BIGINT_DESC, TypeDescriptor::create_array_type(variant_type)}, 2, true, driver_id);
        EXPECT_FALSE(uses_spill_writer(*sink));
        EXPECT_TRUE(uses_buffer_writer(*sink));

        sink = create_sink(supported_types, supported_types.size(), false, driver_id);
        EXPECT_FALSE(uses_spill_writer(*sink));
        EXPECT_TRUE(uses_buffer_writer(*sink));

        // The tuple has more slots than the sink has columns: only the sink columns count.
        sink = create_sink(variant_types, supported_types.size(), true, driver_id);
        EXPECT_TRUE(uses_spill_writer(*sink));
        EXPECT_FALSE(uses_buffer_writer(*sink));
    }

    // The sink does not find its tuple: it uses the buffer writer instead of dereferencing a null tuple later.
    {
        SCOPED_UPDATE(bool, config::enable_connector_sink_spill, true);
        create_tuple(supported_types);
        auto sink_ctx = std::make_shared<connector::IcebergChunkSinkContext>();
        sink_ctx->path = "/path/to/directory/";
        sink_ctx->column_names = {"c0"};
        sink_ctx->format = formats::PARQUET;
        sink_ctx->compression_type = TCompressionType::NO_COMPRESSION;
        sink_ctx->max_file_size = 1 << 30;
        sink_ctx->column_evaluators = ColumnSlotIdEvaluator::from_types({TYPE_BIGINT_DESC});
        sink_ctx->fragment_context = _fragment_context.get();
        sink_ctx->tuple_desc_id = 12345;
        auto sink = provider.create_chunk_sink(sink_ctx, 0).value();
        EXPECT_FALSE(uses_spill_writer(*sink));
        EXPECT_TRUE(uses_buffer_writer(*sink));
    }

    // The tuple has fewer slots than the sink has columns: the sink is inconsistent, so it uses the buffer writer.
    {
        SCOPED_UPDATE(bool, config::enable_connector_sink_spill, true);
        create_tuple({TYPE_BIGINT_DESC});
        auto sink_ctx = std::make_shared<connector::IcebergChunkSinkContext>();
        sink_ctx->path = "/path/to/directory/";
        sink_ctx->column_names = {"c0", "c1"};
        sink_ctx->format = formats::PARQUET;
        sink_ctx->compression_type = TCompressionType::NO_COMPRESSION;
        sink_ctx->max_file_size = 1 << 30;
        sink_ctx->column_evaluators = ColumnSlotIdEvaluator::from_types({TYPE_BIGINT_DESC, TYPE_INT_DESC});
        sink_ctx->fragment_context = _fragment_context.get();
        sink_ctx->tuple_desc_id = 0;
        auto sink = provider.create_chunk_sink(sink_ctx, 0).value();
        EXPECT_FALSE(uses_spill_writer(*sink));
        EXPECT_TRUE(uses_buffer_writer(*sink));
    }
}

TEST_F(IcebergChunkSinkTest, test_generate_parquet_field_ids_required) {
    auto make_field = [](int32_t field_id, std::optional<bool> is_optional, std::vector<TIcebergSchemaField> children) {
        TIcebergSchemaField field;
        field.__set_field_id(field_id);
        if (is_optional.has_value()) {
            field.__set_is_optional(*is_optional);
        }
        if (!children.empty()) {
            field.__set_children(children);
        }
        return field;
    };

    // a struct column that is required and has a required, an optional and a field with no flag; an optional column
    std::vector<TIcebergSchemaField> fields{
            make_field(1, false, {make_field(2, false, {}), make_field(3, true, {}), make_field(4, std::nullopt, {})}),
            make_field(5, true, {})};
    auto ids = IcebergUtils::generate_parquet_field_ids(fields);

    ASSERT_EQ(2, ids.size());
    EXPECT_EQ(1, ids[0].field_id);
    EXPECT_TRUE(ids[0].required);
    EXPECT_FALSE(ids[1].required);
    ASSERT_EQ(3, ids[0].children.size());
    EXPECT_TRUE(ids[0].children[0].required);
    EXPECT_FALSE(ids[0].children[1].required);
    // an unset flag means optional
    EXPECT_FALSE(ids[0].children[2].required);
}

TEST_F(IcebergChunkSinkTest, test_utils) {
    int128_t val = 123;
    std::string str128 = integer_to_string(val);
    EXPECT_EQ("123", str128);
    str128 = integer_to_string(-val);
    EXPECT_EQ("-123", str128);

    auto format_dec = HiveUtils::format_decimal_value<int32_t>(123, 2);
    EXPECT_EQ("1.23", format_dec.value());
    format_dec = HiveUtils::format_decimal_value<int32_t>(123, 4);
    EXPECT_EQ("0.0123", format_dec.value());
    EXPECT_ERROR(HiveUtils::format_decimal_value<int32_t>(123, -1));

    {
        Columns partition_key_columns;
        ChunkPtr chunk = std::make_shared<Chunk>();
        std::vector<ChunkExtraColumnsMeta> extra_metas;
        {
            Datum datum;
            datum.set_int64(23);
            auto res = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TYPE_BIGINT_DESC, true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_int64(40);
            auto res = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TYPE_BIGINT_DESC, true /*useless*/, true /*useless*/});
        }
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators = ColumnSlotIdEvaluator::from_types(
                {TypeDescriptor::from_logical_type(TYPE_BIGINT), TypeDescriptor::from_logical_type(TYPE_VARCHAR)});
        auto chunk_extra_data = std::make_shared<ChunkExtraColumnsData>(extra_metas, std::move(partition_key_columns));
        // Unlock during merging partition chunks into a full chunk.
        chunk->set_extra_data(chunk_extra_data);
        std::vector<int8_t> field_is_null;
        auto ret = HiveUtils::iceberg_make_partition_name({"k1", "k2"}, partition_column_evaluators, {"day", "hour"},
                                                          chunk.get(), true, field_is_null);

        EXPECT_EQ("k1=1970-01-24/k2=1970-01-02-16/", ret.value());
        ret = HiveUtils::iceberg_make_partition_name({"k1", "k2"}, partition_column_evaluators, {"year", "month"},
                                                     chunk.get(), true, field_is_null);
        EXPECT_EQ("k1=1993/k2=1973-05/", ret.value());
    }

    {
        Columns partition_key_columns;
        ChunkPtr chunk = std::make_shared<Chunk>();
        std::string tmp = "abc";
        std::vector<ChunkExtraColumnsMeta> extra_metas;
        {
            Datum datum;
            datum.set_slice(tmp);
            auto res = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TYPE_VARCHAR_DESC, true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_date(DateValue::create(1999, 12, 31));
            auto res = ColumnHelper::create_column(TYPE_DATE_DESC, true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TYPE_DATE_DESC, true /*useless*/, true /*useless*/});
        }

        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators = ColumnSlotIdEvaluator::from_types(
                {TypeDescriptor::from_logical_type(TYPE_VARCHAR), TypeDescriptor::from_logical_type(TYPE_DATE)});

        auto chunk_extra_data = std::make_shared<ChunkExtraColumnsData>(extra_metas, std::move(partition_key_columns));
        // Unlock during merging partition chunks into a full chunk.
        chunk->set_extra_data(chunk_extra_data);
        std::vector<int8_t> field_is_null;
        auto ret = HiveUtils::iceberg_make_partition_name({"k1", "k2"}, partition_column_evaluators,
                                                          {"identity", "truncate"}, chunk.get(), true, field_is_null);
        EXPECT_EQ("k1=abc/k2=1999-12-31/", ret.value());
    }

    {
        Columns partition_key_columns;
        ChunkPtr chunk = std::make_shared<Chunk>();
        std::string tmp = "abc";
        auto time = TimestampValue::create(2023, 10, 31, 12, 0, 0);
        std::vector<ChunkExtraColumnsMeta> extra_metas;
        {
            Datum datum;
            datum.set_slice(tmp);
            auto res = ColumnHelper::create_column(TYPE_VARBINARY_DESC, true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TYPE_VARBINARY_DESC, true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_slice(tmp);
            auto res = ColumnHelper::create_column(TypeDescriptor::create_char_type(10), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(
                    ChunkExtraColumnsMeta{TypeDescriptor::create_char_type(10), true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_timestamp(time);
            auto res = ColumnHelper::create_column(TYPE_DATETIME_DESC, true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TYPE_DATETIME_DESC, true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_int8(12);
            auto res = ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_TINYINT), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TypeDescriptor(LogicalType::TYPE_TINYINT), true /*useless*/,
                                                        true /*useless*/});
        }

        {
            Datum datum;
            datum.set_int16(33);
            auto res = ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_SMALLINT), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TypeDescriptor(LogicalType::TYPE_SMALLINT), true /*useless*/,
                                                        true /*useless*/});
        }

        {
            Datum datum;
            datum.set_int32(33);
            auto res = ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_INT), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(
                    ChunkExtraColumnsMeta{TypeDescriptor(LogicalType::TYPE_INT), true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_int64(33);
            auto res = ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_BIGINT), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TypeDescriptor(LogicalType::TYPE_BIGINT), true /*useless*/,
                                                        true /*useless*/});
        }

        auto chunk_extra_data = std::make_shared<ChunkExtraColumnsData>(extra_metas, std::move(partition_key_columns));
        // Unlock during merging partition chunks into a full chunk.
        chunk->set_extra_data(chunk_extra_data);
        std::vector<int8_t> field_is_null;
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators = ColumnSlotIdEvaluator::from_types(
                {TypeDescriptor::from_logical_type(TYPE_VARBINARY), TypeDescriptor::from_logical_type(TYPE_CHAR),
                 TypeDescriptor::from_logical_type(TYPE_DATETIME), TypeDescriptor::from_logical_type(TYPE_TINYINT),
                 TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_BIGINT)});
        auto ret = HiveUtils::iceberg_make_partition_name(
                {"k1", "k2", "k3", "k4", "k5", "k6", "k7"}, partition_column_evaluators,
                {"bucket", "identity", "truncate", "truncate", "truncate", "truncate", "truncate"}, chunk.get(), true,
                field_is_null);
        EXPECT_EQ("k1=YWJj/k2=abc%20%20%20%20%20%20%20/k3=2023-10-31%2012%3A00%3A00/k4=12/k5=33/k6=33/k7=33/",
                  ret.value());
    }

    {
        Columns partition_key_columns;
        ChunkPtr chunk = std::make_shared<Chunk>();
        std::vector<ChunkExtraColumnsMeta> extra_metas;
        {
            Datum datum;
            datum.set_int32(1234);
            auto res = ColumnHelper::create_column(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 7, 2), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 7, 2),
                                                        true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_int64(3344);
            auto res = ColumnHelper::create_column(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 10, 2), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 10, 2),
                                                        true /*useless*/, true /*useless*/});
        }

        {
            Datum datum;
            datum.set_int128(7893);
            auto res = ColumnHelper::create_column(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 2), true);
            res->append_datum(datum);
            auto ptr = ConstColumn::create(std::move(res), 1);
            partition_key_columns.emplace_back(ptr);
            extra_metas.push_back(ChunkExtraColumnsMeta{TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 20, 2),
                                                        true /*useless*/, true /*useless*/});
        }

        auto chunk_extra_data = std::make_shared<ChunkExtraColumnsData>(extra_metas, std::move(partition_key_columns));
        // Unlock during merging partition chunks into a full chunk.
        std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators = ColumnSlotIdEvaluator::from_types(
                {TypeDescriptor::from_logical_type(TYPE_DECIMAL32), TypeDescriptor::from_logical_type(TYPE_DECIMAL64),
                 TypeDescriptor::from_logical_type(TYPE_DECIMAL128)});
        chunk->set_extra_data(chunk_extra_data);
        std::vector<int8_t> field_is_null;
        auto ret = HiveUtils::iceberg_make_partition_name({"k1", "k2", "k3"}, partition_column_evaluators,
                                                          {"identity", "truncate", "bucket"}, chunk.get(), true,
                                                          field_is_null);
        EXPECT_EQ("k1=12.34/k2=33.44/k3=78.93/", ret.value());
    }
}

} // namespace
} // namespace starrocks::connector
