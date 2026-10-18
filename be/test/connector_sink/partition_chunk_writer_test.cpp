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

#include "connector/partition_chunk_writer.h"

#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <future>
#include <map>
#include <optional>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "connector/connector_chunk_sink.h"
#include "connector/iceberg_chunk_sink.h"
#include "connector/sink_memory_manager.h"
#include "exec/pipeline/fragment_context.h"
#include "formats/file_writer.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "formats/utils.h"
#include "testutil/assert.h"
#include "util/await.h"
#include "util/defer_op.h"

namespace starrocks::connector {
namespace {

using CommitResult = formats::FileWriter::CommitResult;
using WriterAndStream = formats::WriterAndStream;
using Stream = io::AsyncFlushOutputStream;
using ::testing::Return;
using ::testing::ByMove;
using ::testing::_;

class PartitionChunkWriterTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fragment_context = std::make_shared<pipeline::FragmentContext>();
        _fragment_context->set_runtime_state(std::make_shared<RuntimeState>());
        _runtime_state = _fragment_context->runtime_state();
    }

    void TearDown() override {}

    // Defined below, once the mock writer/helper types it needs are declared
    void test_sorted_spill_with_null(bool is_asc, bool is_null_first, const std::vector<std::string>& expected);

    // Creates a spill writer that writes to the mock file writer. The caller creates and removes |fs_base_path|.
    std::shared_ptr<SpillPartitionChunkWriter> create_spill_writer(
            const std::string& fs_base_path, TupleDescriptor* tuple_desc,
            std::shared_ptr<std::vector<std::unique_ptr<ColumnEvaluator>>> column_evaluators,
            std::shared_ptr<SortOrdering> sort_ordering);

    std::shared_ptr<AsyncFlushStreamPoller> _writer_poller;
    bool _writer_commited = false;
    Status _writer_status;

    ObjectPool _pool;
    std::shared_ptr<pipeline::FragmentContext> _fragment_context;
    RuntimeState* _runtime_state;
};

class MockFileWriterFactory : public formats::FileWriterFactory {
public:
    MOCK_METHOD(Status, init, (), (override));
    MOCK_METHOD(StatusOr<WriterAndStream>, create, (const std::string&), (const override));
};

class WriterHelper {
public:
    static WriterHelper* instance() {
        static WriterHelper helper;
        return &helper;
    }

    Status write(Chunk* chunk) {
        if (!_tmp_chunk) {
            _tmp_chunk = chunk->clone_empty();
        }
        _tmp_chunk->append(*chunk, 0, chunk->num_rows());
        ;
        return Status::OK();
    }

    int64_t commit() {
        if (!_tmp_chunk) {
            return 0;
        }
        ChunkPtr result_chunk = _tmp_chunk->clone_empty(_tmp_chunk->num_rows());
        result_chunk->append(*_tmp_chunk, 0, _tmp_chunk->num_rows());
        _result_chunks.push_back(result_chunk);
        size_t num_rows = result_chunk->num_rows();
        _tmp_chunk.reset();
        return num_rows;
    }

    void reset() {
        if (_tmp_chunk) {
            _tmp_chunk.reset();
        }
        _result_chunks.clear();
    }

    int64_t written_bytes() { return _tmp_chunk ? _tmp_chunk->bytes_usage() : 0; }

    int64_t written_rows() { return _tmp_chunk != nullptr ? _tmp_chunk->num_rows() : 0; }

    int64_t result_rows() {
        int64_t num_rows = 0;
        for (auto& chunk : _result_chunks) {
            num_rows += chunk->num_rows();
        }
        return num_rows;
    }

    std::vector<ChunkPtr>& result_chunks() { return _result_chunks; }

private:
    WriterHelper() {}

    ChunkPtr _tmp_chunk = nullptr;
    std::vector<ChunkPtr> _result_chunks;
};

class MockWriter : public formats::FileWriter {
public:
    MockWriter() {}
    MOCK_METHOD(Status, init, (), (override));
    MOCK_METHOD(int64_t, get_allocated_bytes, (), (override));

    int64_t get_written_bytes() override { return WriterHelper::instance()->written_bytes(); }

    Status write(Chunk* chunk) override { return WriterHelper::instance()->write(chunk); }

    int64_t get_flush_batch_size() override { return _flush_batch_size; }

    void set_flush_batch_size(int64_t flush_batch_size) { _flush_batch_size = flush_batch_size; }

    CommitResult close() override {
        size_t num_rows = WriterHelper::instance()->commit();
        CommitResult commit_result = {
                .io_status = Status::OK(),
                .format = formats::PARQUET,
                .file_statistics =
                        {
                                .record_count = static_cast<int64_t>(num_rows),
                        },
                .location = "path/to/directory/data.parquet",
        };
        return commit_result;
    }

private:
    int64_t _flush_batch_size = 128L * 1024 * 1024; // 128MB
};

class MockFile : public WritableFile {
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

TEST_F(PartitionChunkWriterTest, buffer_partition_chunk_writer) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto writer_helper = WriterHelper::instance();
    {
        writer_helper->reset();
        // Create partition writer
        auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
        auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
        EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
            WriterAndStream ws;
            ws.writer = std::make_unique<MockWriter>();
            ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
            return ws;
        });

        auto partition_chunk_writer_ctx = std::make_shared<BufferPartitionChunkWriterContext>(
                BufferPartitionChunkWriterContext{mock_writer_factory, location_provider, 100, false});
        auto partition_chunk_writer_factory =
                std::make_unique<BufferPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
        std::vector<int8_t> partition_field_null_list;
        auto partition_writer = partition_chunk_writer_factory->create("c1", partition_field_null_list);
        bool commited = false;
        auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
        auto poller = MockPoller();
        partition_writer->set_io_poller(&poller);
        partition_writer->set_commit_callback(commit_callback);
        EXPECT_OK(partition_writer->init());

        // Create a chunk
        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 1);
        chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("aaa"));

        // Write chunk
        auto ret = partition_writer->write(chunk);
        EXPECT_EQ(ret.ok(), true);
        EXPECT_EQ(writer_helper->written_rows(), 1);
        EXPECT_EQ(writer_helper->result_rows(), 0);

        // Flush chunk
        ret = partition_writer->flush();
        EXPECT_EQ(ret.ok(), true);
        ret = partition_writer->wait_flush();
        EXPECT_EQ(ret.ok(), true);
        EXPECT_EQ(commited, true);
        EXPECT_EQ(partition_writer->is_finished(), true);
        EXPECT_EQ(partition_writer->get_written_bytes(), 0);
        EXPECT_EQ(partition_writer->get_flushable_bytes(), 0);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 1);
    }
    std::filesystem::remove_all(fs_base_path);
}

TEST_F(PartitionChunkWriterTest, spill_partition_chunk_writer) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");

    auto writer_helper = WriterHelper::instance();
    bool commited = false;
    Status status;

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    // Create partition writer
    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{{mock_writer_factory, location_provider, 100, false},
                                             nullptr,
                                             _fragment_context.get(),
                                             tuple_desc,
                                             nullptr,
                                             nullptr});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    EXPECT_OK(partition_writer->init());

    // Normal write and flush to file
    {
        writer_helper->reset();
        // Create a chunk
        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 1);
        chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("aaa"));

        // Write chunk
        auto ret = partition_writer->write(chunk);
        EXPECT_EQ(ret.ok(), true);
        EXPECT_GT(partition_writer->get_written_bytes(), 0);
        EXPECT_EQ(partition_writer->get_flushable_bytes(), chunk->bytes_usage());

        // Flush chunk
        ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        EXPECT_EQ(commited, true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 1);
    }

    // Write and spill
    {
        // Reset states
        writer_helper->reset();
        commited = false;
        status = Status::OK();

        // Create a chunk
        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 1);
        chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("aaa"));

        for (size_t i = 0; i < 3; ++i) {
            // Write chunk
            auto ret = partition_writer->write(chunk);
            EXPECT_EQ(ret.ok(), true);
            EXPECT_GT(partition_writer->get_written_bytes(), 0);

            // Flush chunk
            EXPECT_GT(partition_writer->get_flushable_bytes(), 0);
            ret = partition_writer->flush();
            EXPECT_EQ(ret.ok(), true);
            ret = partition_writer->wait_flush();
            EXPECT_EQ(ret.ok(), true);
            Awaitility()
                    .timeout(3 * 1000 * 1000) // 3s
                    .interval(300 * 1000)     // 300ms
                    .until([partition_writer]() {
                        return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
                    });

            EXPECT_EQ(partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed), 0);
            EXPECT_EQ(status.ok(), true);
        }

        // Merge spill blocks
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        // Wait for is_finished() to ensure all async operations (merge task and callback) are complete
        // When is_finished() is true, it guarantees both the merge task is done and commited flag is set
        Awaitility()
                .timeout(3 * 1000 * 1000) // 3s
                .interval(300 * 1000)     // 300ms
                .until([&]() { return partition_writer->is_finished(); });

        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(partition_writer->is_finished(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 3);
    }

    std::filesystem::remove_all(fs_base_path);
}

TEST_F(PartitionChunkWriterTest, spill_writer_for_complex_types) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");

    auto writer_helper = WriterHelper::instance();
    bool commited = false;
    Status status;

    parquet::Utils::SlotDesc c1{"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY)};
    c1.type.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    parquet::Utils::SlotDesc c2{"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_MAP)};
    c2.type.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    c2.type.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    parquet::Utils::SlotDesc c3{"c3", TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT)};
    c3.type.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR));
    c3.type.children.push_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    c3.type.field_names.emplace_back("c3_name");
    c3.type.field_names.emplace_back("c3_age");

    parquet::Utils::SlotDesc slot_descs[] = {c1, c2, c3, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    // Create partition writer
    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{mock_writer_factory, location_provider, 100, false, nullptr,
                                             _fragment_context.get(), tuple_desc, nullptr, nullptr});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    EXPECT_OK(partition_writer->init());

    // Write and spill
    {
        // Reset states
        writer_helper->reset();
        commited = false;
        status = Status::OK();

        for (size_t i = 0; i < 3; ++i) {
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 4);

            // Fill ARRAY<INT>
            {
                auto* nullable_column = down_cast<NullableColumn*>(chunk->get_column_raw_ptr_by_index(0));
                auto* col = down_cast<ArrayColumn*>(nullable_column->data_column_raw_ptr());
                auto* elements = col->elements_column_raw_ptr();
                auto* offsets = col->offsets_column_raw_ptr();

                // row 0: [1,2]
                elements->append_datum(Datum(int32_t(1)));
                elements->append_datum(Datum(int32_t(2)));

                // row 1: [3]
                offsets->append(2);
                elements->append_datum(Datum(int32_t(3)));

                // row 2: []
                offsets->append(3);

                // row 3: [4,5,6]
                offsets->append(3);
                elements->append_datum(Datum(int32_t(4)));
                elements->append_datum(Datum(int32_t(5)));
                elements->append_datum(Datum(int32_t(6)));
                offsets->append(elements->size());
                nullable_column->resize(4);
            }

            // Fill MAP<VARCHAR, INT>
            {
                auto* nullable_column = down_cast<NullableColumn*>(chunk->get_column_raw_ptr_by_index(1));
                auto* col = down_cast<MapColumn*>(nullable_column->data_column_raw_ptr());
                auto* keys = col->keys_column_raw_ptr();
                auto* values = col->values_column_raw_ptr();
                auto* offsets = col->offsets_column_raw_ptr();

                // row 0: {"k1":1,"k2":2}
                keys->append_datum(Datum(Slice("k1")));
                values->append_datum(Datum(int32_t(1)));
                keys->append_datum(Datum(Slice("k2")));
                values->append_datum(Datum(int32_t(2)));

                // row 1: {"k3":3}
                offsets->append(2);
                keys->append_datum(Datum(Slice("k3")));
                values->append_datum(Datum(int32_t(3)));

                // row 2: {}
                offsets->append(3);

                // row 3: {"k4":4}
                offsets->append(3);
                keys->append_datum(Datum(Slice("k4")));
                values->append_datum(Datum(int32_t(4)));
                offsets->append(keys->size());
                nullable_column->resize(4);
            }

            // Fill STRUCT\<c1:VARCHAR,c2:INT\>
            {
                auto* nullable_column = down_cast<NullableColumn*>(chunk->get_column_raw_ptr_by_index(2));
                auto* col = down_cast<StructColumn*>(nullable_column->data_column_raw_ptr());

                auto* f0 = col->field_column_raw_ptr(0);
                auto* f1 = col->field_column_raw_ptr(1);

                f0->append_datum(Datum(Slice("s1")));
                f1->append_datum(Datum(int32_t(10)));
                f0->append_datum(Datum(Slice("s2")));
                f1->append_datum(Datum(int32_t(20)));
                f0->append_datum(Datum(Slice("s3")));
                f1->append_datum(Datum(int32_t(30)));
                f0->append_datum(Datum(Slice("s4")));
                f1->append_datum(Datum(int32_t(40)));
                nullable_column->resize(4);
            }

            auto st = partition_writer->write(chunk);
            EXPECT_TRUE(st.ok());
            EXPECT_GT(partition_writer->get_written_bytes(), 0);

            st = partition_writer->flush();
            EXPECT_TRUE(st.ok());
            st = partition_writer->wait_flush();
            EXPECT_TRUE(st.ok());
            Awaitility().timeout(3 * 1000 * 1000).interval(300 * 1000).until([partition_writer]() {
                return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
            });

            EXPECT_EQ(partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed), 0);
            EXPECT_TRUE(status.ok());
        }

        // Merge spill blocks
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        // Wait for is_finished() to ensure all async operations (merge task and callback) are complete
        Awaitility()
                .timeout(3 * 1000 * 1000) // 3s
                .interval(300 * 1000)     // 300ms
                .until([&]() { return partition_writer->is_finished(); });

        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(partition_writer->is_finished(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        // Each chunk has 4 rows, 3 chunks in total
        EXPECT_EQ(writer_helper->result_rows(), 12);
    }

    std::filesystem::remove_all(fs_base_path);
}

TEST_F(PartitionChunkWriterTest, sort_column_asc) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto writer_helper = WriterHelper::instance();
    bool commited = false;
    Status status;

    // Create partition writer
    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    auto sort_ordering = std::make_shared<SortOrdering>();
    sort_ordering->sort_key_idxes = {0};
    sort_ordering->sort_descs.descs.emplace_back(true, false);
    const size_t max_file_size = 1073741824; // 1GB
    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{{mock_writer_factory, location_provider, max_file_size, false},
                                             nullptr,
                                             _fragment_context.get(),
                                             tuple_desc,
                                             nullptr,
                                             sort_ordering});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    EXPECT_OK(partition_writer->init());

    // Normal write and flush to file
    {
        writer_helper->reset();

        for (size_t i = 0; i < 3; ++i) {
            // Create a chunk
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 3);
            std::string suffix = std::to_string(3 - i);
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("ccc" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("bbb" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("aaa" + suffix));

            // Write chunk
            auto ret = partition_writer->write(chunk);
            EXPECT_EQ(ret.ok(), true);
            EXPECT_GT(partition_writer->get_written_bytes(), 0);
        }

        // Flush chunks directly
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 9);
        EXPECT_EQ(writer_helper->result_chunks().size(), 1);

        // Check the result order
        auto result_chunk = writer_helper->result_chunks()[0];
        auto column = result_chunk->get_column_by_index(0);
        std::string last_row;
        for (size_t i = 0; i < column->size(); ++i) {
            std::string cur_row = column->get(i).get_slice().to_string();
            LOG(INFO) << "(" << i << "): " << cur_row;
            if (!last_row.empty()) {
                EXPECT_GT(cur_row, last_row);
            }
            last_row = cur_row;
        }
    }

    // Write and spill multiple chunks
    {
        writer_helper->reset();
        commited = false;
        status = Status::OK();

        for (size_t i = 0; i < 3; ++i) {
            // Create a chunk
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 3);
            std::string suffix = std::to_string(3 - i);
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("ccc" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("bbb" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("aaa" + suffix));

            // Write chunk
            auto ret = partition_writer->write(chunk);
            EXPECT_EQ(ret.ok(), true);
            EXPECT_GT(partition_writer->get_written_bytes(), 0);

            // Flush chunk
            ret = partition_writer->flush();
            EXPECT_EQ(ret.ok(), true);
            ret = partition_writer->wait_flush();
            EXPECT_EQ(ret.ok(), true);
            Awaitility()
                    .timeout(3 * 1000 * 1000) // 3s
                    .interval(300 * 1000)     // 300ms
                    .until([partition_writer]() {
                        return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
                    });

            EXPECT_EQ(partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed), 0);
            EXPECT_EQ(status.ok(), true);
        }

        // Merge spill blocks
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        // Wait for is_finished() to ensure all async operations (merge task and callback) are complete
        Awaitility()
                .timeout(3 * 1000 * 1000) // 3s
                .interval(300 * 1000)     // 300ms
                .until([&]() { return partition_writer->is_finished(); });

        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 9);
        EXPECT_EQ(writer_helper->result_chunks().size(), 1);

        // Check the result order
        auto result_chunk = writer_helper->result_chunks()[0];
        auto column = result_chunk->get_column_by_index(0);
        std::string last_row;
        for (size_t i = 0; i < column->size(); ++i) {
            std::string cur_row = column->get(i).get_slice().to_string();
            LOG(INFO) << "(" << i << "): " << cur_row;
            if (!last_row.empty()) {
                EXPECT_GT(cur_row, last_row);
            }
            last_row = cur_row;
        }
    }

    std::filesystem::remove_all(fs_base_path);
}

TEST_F(PartitionChunkWriterTest, sort_column_desc) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto writer_helper = WriterHelper::instance();
    bool commited = false;
    Status status;

    // Create partition writer
    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    auto sort_ordering = std::make_shared<SortOrdering>();
    sort_ordering->sort_key_idxes = {0};
    sort_ordering->sort_descs.descs.emplace_back(false, false);
    const size_t max_file_size = 1073741824; // 1GB
    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{{mock_writer_factory, location_provider, max_file_size, false},
                                             nullptr,
                                             _fragment_context.get(),
                                             tuple_desc,
                                             nullptr,
                                             sort_ordering});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    EXPECT_OK(partition_writer->init());

    // Normal write and flush to file
    {
        writer_helper->reset();

        for (size_t i = 0; i < 3; ++i) {
            // Create a chunk
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 3);
            std::string suffix = std::to_string(3 - i);
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("ccc" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("bbb" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("aaa" + suffix));

            // Write chunk
            auto ret = partition_writer->write(chunk);
            EXPECT_EQ(ret.ok(), true);
            EXPECT_GT(partition_writer->get_written_bytes(), 0);
        }

        // Flush chunks directly
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 9);
        EXPECT_EQ(writer_helper->result_chunks().size(), 1);

        // Check the result order
        auto result_chunk = writer_helper->result_chunks()[0];
        auto column = result_chunk->get_column_by_index(0);
        std::string last_row;
        for (size_t i = 0; i < column->size(); ++i) {
            std::string cur_row = column->get(i).get_slice().to_string();
            LOG(INFO) << "(" << i << "): " << cur_row;
            if (!last_row.empty()) {
                EXPECT_LT(cur_row, last_row);
            }
            last_row = cur_row;
        }
    }

    // Write and spill multiple chunks
    {
        writer_helper->reset();
        commited = false;
        status = Status::OK();

        for (size_t i = 0; i < 3; ++i) {
            // Create a chunk
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 3);
            std::string suffix = std::to_string(3 - i);
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("ccc" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("bbb" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("aaa" + suffix));

            // Write chunk
            auto ret = partition_writer->write(chunk);
            EXPECT_EQ(ret.ok(), true);
            EXPECT_GT(partition_writer->get_written_bytes(), 0);

            // Flush chunk
            ret = partition_writer->flush();
            EXPECT_EQ(ret.ok(), true);
            ret = partition_writer->wait_flush();
            EXPECT_EQ(ret.ok(), true);
            Awaitility()
                    .timeout(3 * 1000 * 1000) // 3s
                    .interval(300 * 1000)     // 300ms
                    .until([partition_writer]() {
                        return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
                    });

            EXPECT_EQ(partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed), 0);
            EXPECT_EQ(status.ok(), true);
        }

        // Merge spill blocks
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        // Wait for is_finished() to ensure all async operations (merge task and callback) are complete
        Awaitility()
                .timeout(3 * 1000 * 1000) // 3s
                .interval(300 * 1000)     // 300ms
                .until([&]() { return partition_writer->is_finished(); });

        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 9);
        EXPECT_EQ(writer_helper->result_chunks().size(), 1);

        // Check the result order
        auto result_chunk = writer_helper->result_chunks()[0];
        auto column = result_chunk->get_column_by_index(0);
        std::string last_row;
        for (size_t i = 0; i < column->size(); ++i) {
            std::string cur_row = column->get(i).get_slice().to_string();
            LOG(INFO) << "(" << i << "): " << cur_row;
            if (!last_row.empty()) {
                EXPECT_LT(cur_row, last_row);
            }
            last_row = cur_row;
        }
    }

    std::filesystem::remove_all(fs_base_path);
}

TEST_F(PartitionChunkWriterTest, sort_multiple_columns) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {"c2", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto writer_helper = WriterHelper::instance();
    bool commited = false;
    Status status;

    // Create partition writer
    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    auto sort_ordering = std::make_shared<SortOrdering>();
    sort_ordering->sort_key_idxes = {0, 1};
    sort_ordering->sort_descs.descs.emplace_back(true, false);
    sort_ordering->sort_descs.descs.emplace_back(false, false);
    const size_t max_file_size = 1073741824; // 1GB
    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{{mock_writer_factory, location_provider, max_file_size, false},
                                             nullptr,
                                             _fragment_context.get(),
                                             tuple_desc,
                                             nullptr,
                                             sort_ordering});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    EXPECT_OK(partition_writer->init());

    // Write and spill multiple chunks
    {
        writer_helper->reset();

        for (size_t i = 0; i < 3; ++i) {
            // Create a chunk
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 3);
            std::string suffix = std::to_string(3 - i);
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("ccc" + suffix));
            chunk->get_column_raw_ptr_by_index(1)->append_datum(Slice("222" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("ccc" + suffix));
            chunk->get_column_raw_ptr_by_index(1)->append_datum(Slice("111" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("bbb" + suffix));
            chunk->get_column_raw_ptr_by_index(1)->append_datum(Slice("222" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("bbb" + suffix));
            chunk->get_column_raw_ptr_by_index(1)->append_datum(Slice("111" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("aaa" + suffix));
            chunk->get_column_raw_ptr_by_index(1)->append_datum(Slice("222" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("aaa" + suffix));
            chunk->get_column_raw_ptr_by_index(1)->append_datum(Slice("111" + suffix));

            // Write chunk
            auto ret = partition_writer->write(chunk);
            EXPECT_EQ(ret.ok(), true);
            EXPECT_GT(partition_writer->get_written_bytes(), 0);

            // Flush chunk
            ret = partition_writer->flush();
            EXPECT_EQ(ret.ok(), true);
            ret = partition_writer->wait_flush();
            EXPECT_EQ(ret.ok(), true);
            Awaitility()
                    .timeout(3 * 1000 * 1000) // 3s
                    .interval(300 * 1000)     // 300ms
                    .until([partition_writer]() {
                        return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
                    });

            EXPECT_EQ(partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed), 0);
            EXPECT_EQ(status.ok(), true);
        }

        // Merge spill blocks
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        // Wait for is_finished() to ensure all async operations (merge task and callback) are complete
        Awaitility()
                .timeout(3 * 1000 * 1000) // 3s
                .interval(300 * 1000)     // 300ms
                .until([&]() { return partition_writer->is_finished(); });

        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 18);
        EXPECT_EQ(writer_helper->result_chunks().size(), 1);

        // Check the result order
        auto result_chunk = writer_helper->result_chunks()[0];
        auto c1 = result_chunk->get_column_by_index(0);
        auto c2 = result_chunk->get_column_by_index(1);
        std::string c1_last_row;
        std::string c2_last_row;
        for (size_t i = 0; i < c1->size(); ++i) {
            std::string c1_cur_row = c1->get(i).get_slice().to_string();
            std::string c2_cur_row = c2->get(i).get_slice().to_string();
            LOG(INFO) << "(" << i << "): " << c1_cur_row << ", " << c2_cur_row;
            if (!c1_last_row.empty()) {
                EXPECT_GE(c1_cur_row, c1_last_row);
            }
            if (!c2_last_row.empty() && c1_cur_row == c1_last_row) {
                EXPECT_LE(c2_cur_row, c2_last_row);
            }
            c1_last_row = c1_cur_row;
            c2_last_row = c2_cur_row;
        }
    }

    std::filesystem::remove_all(fs_base_path);
}

TEST_F(PartitionChunkWriterTest, sort_column_with_schema_chunk) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    Fields fields;
    for (auto& slot : tuple_desc->slots()) {
        TypeDescriptor type_desc = slot->type();
        TypeInfoPtr type_info = get_type_info(type_desc.type, type_desc.precision, type_desc.scale);
        auto field = std::make_shared<Field>(slot->id(), slot->col_name(), type_info, slot->is_nullable());
        fields.push_back(field);
    }
    auto schema = std::make_shared<Schema>(std::move(fields), KeysType::DUP_KEYS, std::vector<uint32_t>(), nullptr);

    auto writer_helper = WriterHelper::instance();
    bool commited = false;
    Status status;

    // Create partition writer
    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    auto sort_ordering = std::make_shared<SortOrdering>();
    sort_ordering->sort_key_idxes = {0};
    sort_ordering->sort_descs.descs.emplace_back(true, false);
    const size_t max_file_size = 1073741823; // 1GB
    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{{mock_writer_factory, location_provider, max_file_size, false},
                                             nullptr,
                                             _fragment_context.get(),
                                             tuple_desc,
                                             nullptr,
                                             sort_ordering});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    EXPECT_OK(partition_writer->init());

    // Write and spill multiple chunks
    {
        writer_helper->reset();

        for (size_t i = 0; i < 3; ++i) {
            // Create a chunk
            ChunkPtr chunk = ChunkHelper::new_chunk(*schema, 3);
            std::string suffix = std::to_string(3 - i);
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("ccc" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("bbb" + suffix));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("aaa" + suffix));

            // Write chunk
            auto ret = partition_writer->write(chunk);
            EXPECT_EQ(ret.ok(), true);
            EXPECT_GT(partition_writer->get_written_bytes(), 0);

            // Flush chunk
            ret = partition_writer->flush();
            EXPECT_EQ(ret.ok(), true);
            ret = partition_writer->wait_flush();
            EXPECT_EQ(ret.ok(), true);
            Awaitility()
                    .timeout(3 * 1000 * 1000) // 3s
                    .interval(300 * 1000)     // 300ms
                    .until([partition_writer]() {
                        return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
                    });

            EXPECT_EQ(partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed), 0);
            EXPECT_EQ(status.ok(), true);
        }

        // Merge spill blocks
        auto ret = partition_writer->finish();
        EXPECT_EQ(ret.ok(), true);
        // Wait for is_finished() to ensure all async operations (merge task and callback) are complete
        Awaitility()
                .timeout(3 * 1000 * 1000) // 3s
                .interval(300 * 1000)     // 300ms
                .until([&]() { return partition_writer->is_finished(); });

        EXPECT_EQ(commited, true);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(writer_helper->written_rows(), 0);
        EXPECT_EQ(writer_helper->result_rows(), 9);
        EXPECT_EQ(writer_helper->result_chunks().size(), 1);

        // Check the result order
        auto result_chunk = writer_helper->result_chunks()[0];
        auto column = result_chunk->get_column_by_index(0);
        std::string last_row;
        for (size_t i = 0; i < column->size(); ++i) {
            std::string cur_row = column->get(i).get_slice().to_string();
            LOG(INFO) << "(" << i << "): " << cur_row;
            if (!last_row.empty()) {
                EXPECT_GT(cur_row, last_row);
            }
            last_row = cur_row;
        }
    }

    std::filesystem::remove_all(fs_base_path);
}

// Test connector sink profile metrics
TEST_F(PartitionChunkWriterTest, test_connector_sink_profile_metrics) {
    auto fs_base_path = "./ut_dir_connector_sink_profile";
    std::filesystem::create_directories(fs_base_path);

    auto writer_helper = WriterHelper::instance();
    bool commited = false;
    Status status;

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    // Create partition writer with profile
    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    // Create sink profile
    auto profile = _pool.add(new RuntimeProfile("TestProfile"));
    auto sink_profile = _pool.add(new ConnectorSinkProfile());
    sink_profile->runtime_profile = profile;

    // Initialize profile counters
    sink_profile->partition_writer_counter = ADD_COUNTER(profile, "PartitionWriters", TUnit::UNIT);
    sink_profile->write_file_counter = ADD_COUNTER(profile, "WriteFiles", TUnit::UNIT);
    sink_profile->write_file_record_counter = ADD_COUNTER(profile, "WriteFileRecords", TUnit::UNIT);
    sink_profile->write_file_bytes = ADD_COUNTER(profile, "WriteFileBytes", TUnit::BYTES);
    sink_profile->write_file_timer = ADD_TIMER(profile, "WriteFileTime");
    sink_profile->commit_file_timer = ADD_TIMER(profile, "CommitFileTime");
    sink_profile->sort_timer = ADD_TIMER(profile, "SortTime");
    sink_profile->spill_chunk_timer = ADD_TIMER(profile, "SpillChunkTime");
    sink_profile->merge_blocks_timer = ADD_TIMER(profile, "MergeBlocksTime");
    sink_profile->spilling_bytes_usage_peak = ADD_PEAK_COUNTER(profile, "SpillingBytesUsagePeak", TUnit::BYTES);

    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{mock_writer_factory, location_provider, 100, false, nullptr,
                                             _fragment_context.get(), tuple_desc, nullptr, nullptr});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    partition_writer->set_sink_profile(sink_profile);
    EXPECT_OK(partition_writer->init());

    // Write and verify profile metrics
    {
        writer_helper->reset();
        commited = false;
        status = Status::OK();

        // Write some chunks
        for (size_t i = 0; i < 3; ++i) {
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 3);
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("ccc" + std::to_string(i)));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("bbb" + std::to_string(i)));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("aaa" + std::to_string(i)));

            auto ret = partition_writer->write(chunk);
            EXPECT_OK(ret);
        }

        // Flush to trigger file operations
        auto ret = partition_writer->flush();
        EXPECT_OK(ret);
        ret = partition_writer->wait_flush();
        EXPECT_OK(ret);

        // Wait for async operations to complete
        Awaitility().timeout(3 * 1000 * 1000).interval(300 * 1000).until([partition_writer]() {
            return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
        });

        ret = partition_writer->finish();
        EXPECT_OK(ret);

        // Wait for is_finished() to ensure all async operations (merge task and callback) are complete
        Awaitility().timeout(3 * 1000 * 1000).interval(300 * 1000).until([&]() {
            return partition_writer->is_finished();
        });

        // Verify profile metrics are recorded
        EXPECT_GE(sink_profile->write_file_timer->value(), 0) << "WriteFileTime should be non-negative";
        EXPECT_GE(sink_profile->commit_file_timer->value(), 0) << "CommitFileTime should be non-negative";

        // Sorting metrics should be zero
        EXPECT_EQ(sink_profile->sort_timer->value(), 0) << "SortTime should be positive when sorting is enabled";

        // Spill operations should have been tracked
        EXPECT_GE(sink_profile->spill_chunk_timer->value(), 0) << "SpillChunkTime should be non-negative";
        EXPECT_GE(sink_profile->merge_blocks_timer->value(), 0) << "MergeBlocksTime should be non-negative";

        // Spilling memory peak should be tracked
        EXPECT_GE(sink_profile->spilling_bytes_usage_peak->value(), 0)
                << "SpillingBytesUsagePeak should be non-negative";
    }

    std::filesystem::remove_all(fs_base_path);
}

// Test buffer partition writer with profile metrics
TEST_F(PartitionChunkWriterTest, test_buffer_partition_writer_profile_metrics) {
    auto fs_base_path = "./ut_dir_buffer_writer_profile";
    std::filesystem::create_directories(fs_base_path);

    auto writer_helper = WriterHelper::instance();
    bool commited = false;
    Status status;

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc =
            parquet::Utils::create_tuple_descriptor(_fragment_context->runtime_state(), &_pool, slot_descs);

    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    // Create sink profile
    auto profile = _pool.add(new RuntimeProfile("TestProfile"));
    auto sink_profile = _pool.add(new ConnectorSinkProfile());
    sink_profile->runtime_profile = profile;
    sink_profile->write_file_timer = ADD_TIMER(profile, "WriteFileTime");
    sink_profile->commit_file_timer = ADD_TIMER(profile, "CommitFileTime");

    auto partition_chunk_writer_ctx = std::make_shared<BufferPartitionChunkWriterContext>(
            BufferPartitionChunkWriterContext{mock_writer_factory, location_provider, 100, false});
    auto partition_chunk_writer_factory =
            std::make_unique<BufferPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<BufferPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    partition_writer->set_sink_profile(sink_profile);
    EXPECT_OK(partition_writer->init());

    // Write chunks and verify metrics
    {
        writer_helper->reset();
        commited = false;
        status = Status::OK();

        // Write some chunks
        for (size_t i = 0; i < 2; ++i) {
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 3);
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("data" + std::to_string(i)));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("data" + std::to_string(i)));
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Slice("data" + std::to_string(i)));

            auto ret = partition_writer->write(chunk);
            EXPECT_OK(ret);
        }

        auto ret = partition_writer->finish();
        EXPECT_OK(ret);

        // Wait for is_finished() to ensure all async operations are complete
        // For BufferPartitionChunkWriter, is_finished() always returns true, but we use it for consistency
        Awaitility().timeout(3 * 1000 * 1000).interval(300 * 1000).until([&]() {
            return partition_writer->is_finished();
        });

        // Verify profile metrics are recorded
        EXPECT_GT(sink_profile->write_file_timer->value(), 0) << "WriteFileTime should be positive";
        EXPECT_GT(sink_profile->commit_file_timer->value(), 0) << "CommitFileTime should be positive";
    }

    std::filesystem::remove_all(fs_base_path);
}

// An Iceberg sort order may sort a column descending (IcebergTable.java fills TSortOrder.is_ascs from the
// table's sort fields), and the sorted write spills one sorted run per block and merges them back with a heap
// merge iterator. The run sort and the merge must agree on which end the NULLs go to, otherwise the merged
// file is not sorted at all.
void PartitionChunkWriterTest::test_sorted_spill_with_null(bool is_asc, bool is_null_first,
                                                           const std::vector<std::string>& expected) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");
    DeferOp cleanup([&]() { std::filesystem::remove_all(fs_base_path); });

    parquet::Utils::SlotDesc slot_descs[] = {{"c1", TYPE_VARCHAR_DESC}, {""}};
    TupleDescriptor* tuple_desc = parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);

    auto writer_helper = WriterHelper::instance();
    writer_helper->reset();
    bool commited = false;
    Status status;

    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    auto sort_ordering = std::make_shared<SortOrdering>();
    sort_ordering->sort_key_idxes = {0};
    sort_ordering->sort_descs.descs.emplace_back(is_asc, is_null_first);
    const size_t max_file_size = 1073741824; // 1GB
    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{{mock_writer_factory, location_provider, max_file_size, false},
                                             nullptr,
                                             _fragment_context.get(),
                                             tuple_desc,
                                             nullptr,
                                             sort_ordering});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    EXPECT_OK(partition_writer->init());

    // One spilled block per chunk, each holding both NULL and non-NULL rows
    for (size_t i = 0; i < 3; ++i) {
        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 3);
        std::string suffix = std::to_string(3 - i);
        auto* column = chunk->get_column_raw_ptr_by_index(0);
        column->append_datum(Slice("ccc" + suffix));
        column->append_nulls(1);
        column->append_datum(Slice("aaa" + suffix));

        EXPECT_OK(partition_writer->write(chunk));
        EXPECT_OK(partition_writer->flush());
        EXPECT_OK(partition_writer->wait_flush());
        Awaitility().timeout(3 * 1000 * 1000).interval(300 * 1000).until([partition_writer]() {
            return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
        });
        EXPECT_OK(status);
    }

    // Merge the spilled blocks
    EXPECT_OK(partition_writer->finish());
    Awaitility().timeout(3 * 1000 * 1000).interval(300 * 1000).until([&]() { return partition_writer->is_finished(); });
    EXPECT_EQ(commited, true);
    EXPECT_OK(status);
    ASSERT_EQ(writer_helper->result_chunks().size(), 1);

    auto result_chunk = writer_helper->result_chunks()[0];
    auto column = result_chunk->get_column_by_index(0);
    std::vector<std::string> actual;
    for (size_t i = 0; i < column->size(); ++i) {
        auto datum = column->get(i);
        actual.emplace_back(datum.is_null() ? "NULL" : datum.get_slice().to_string());
    }
    EXPECT_EQ(expected, actual);
}

TEST_F(PartitionChunkWriterTest, sorted_spill_desc_null_last) {
    test_sorted_spill_with_null(false, false, {"ccc3", "ccc2", "ccc1", "aaa3", "aaa2", "aaa1", "NULL", "NULL", "NULL"});
}

TEST_F(PartitionChunkWriterTest, sorted_spill_desc_null_first) {
    test_sorted_spill_with_null(false, true, {"NULL", "NULL", "NULL", "ccc3", "ccc2", "ccc1", "aaa3", "aaa2", "aaa1"});
}

TEST_F(PartitionChunkWriterTest, sorted_spill_asc_null_first) {
    test_sorted_spill_with_null(true, true, {"NULL", "NULL", "NULL", "aaa1", "aaa2", "aaa3", "ccc1", "ccc2", "ccc3"});
}

// Sorted spill of rows with several column types. We expect every column of a row to stay with its sort key after
// the rows go through the spill blocks and the merge.
TEST_F(PartitionChunkWriterTest, sorted_spill_with_multiple_column_types) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");
    DeferOp cleanup([&]() { std::filesystem::remove_all(fs_base_path); });

    parquet::Utils::SlotDesc slot_descs[] = {
            {"k", TYPE_BIGINT_DESC, 1},
            {"name", TYPE_VARCHAR_DESC, 2},
            {"score", TYPE_DOUBLE_DESC, 3},
            {"amount", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 4), 4},
            {""}};
    TupleDescriptor* tuple_desc = parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);

    auto writer_helper = WriterHelper::instance();
    writer_helper->reset();
    bool commited = false;
    Status status;

    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    auto sort_ordering = std::make_shared<SortOrdering>();
    sort_ordering->sort_key_idxes = {0};
    sort_ordering->sort_descs.descs.emplace_back(true, false);
    const size_t max_file_size = 1073741824; // 1GB
    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{{mock_writer_factory, location_provider, max_file_size, false},
                                             nullptr,
                                             _fragment_context.get(),
                                             tuple_desc,
                                             nullptr,
                                             sort_ordering});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    auto commit_callback = [&commited](const CommitResult& r) { commited = true; };
    auto error_handler = [&status](const Status& s) { status = s; };
    auto poller = MockPoller();
    partition_writer->set_io_poller(&poller);
    partition_writer->set_commit_callback(commit_callback);
    partition_writer->set_error_handler(error_handler);
    EXPECT_OK(partition_writer->init());

    // One spilled block per chunk, and the keys of the chunks interleave
    const std::vector<std::vector<int64_t>> keys_of_chunks = {{9, 3, 6}, {8, 2, 5}, {7, 1, 4}};
    for (const auto& keys : keys_of_chunks) {
        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, keys.size());
        for (int64_t key : keys) {
            std::string name = "name" + std::to_string(key);
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(key));
            chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(Slice(name)));
            chunk->get_column_raw_ptr_by_index(2)->append_datum(Datum(static_cast<double>(key) * 1.5));
            chunk->get_column_raw_ptr_by_index(3)->append_datum(Datum(key * 10000));
        }

        EXPECT_OK(partition_writer->write(chunk));
        EXPECT_OK(partition_writer->flush());
        EXPECT_OK(partition_writer->wait_flush());
        Awaitility().timeout(3 * 1000 * 1000).interval(300 * 1000).until([partition_writer]() {
            return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
        });
        EXPECT_OK(status);
    }

    // Merge the spilled blocks
    EXPECT_OK(partition_writer->finish());
    Awaitility().timeout(3 * 1000 * 1000).interval(300 * 1000).until([&]() { return partition_writer->is_finished(); });
    EXPECT_EQ(commited, true);
    EXPECT_OK(status);
    ASSERT_EQ(writer_helper->result_chunks().size(), 1);

    auto result_chunk = writer_helper->result_chunks()[0];
    ASSERT_EQ(result_chunk->num_rows(), 9);
    for (size_t i = 0; i < result_chunk->num_rows(); ++i) {
        int64_t key = result_chunk->get_column_by_index(0)->get(i).get_int64();
        EXPECT_EQ(key, static_cast<int64_t>(i + 1));
        EXPECT_EQ(result_chunk->get_column_by_index(1)->get(i).get_slice().to_string(), "name" + std::to_string(key));
        EXPECT_DOUBLE_EQ(result_chunk->get_column_by_index(2)->get(i).get_double(), static_cast<double>(key) * 1.5);
        EXPECT_EQ(result_chunk->get_column_by_index(3)->get(i).get_int64(), key * 10000);
    }
}

std::shared_ptr<SpillPartitionChunkWriter> PartitionChunkWriterTest::create_spill_writer(
        const std::string& fs_base_path, TupleDescriptor* tuple_desc,
        std::shared_ptr<std::vector<std::unique_ptr<ColumnEvaluator>>> column_evaluators,
        std::shared_ptr<SortOrdering> sort_ordering) {
    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });

    const size_t max_file_size = 1073741824; // 1GB
    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{{mock_writer_factory, location_provider, max_file_size, false},
                                             nullptr,
                                             _fragment_context.get(),
                                             tuple_desc,
                                             std::move(column_evaluators),
                                             std::move(sort_ordering)});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);
    std::vector<int8_t> partition_field_null_list;
    auto partition_writer = std::dynamic_pointer_cast<SpillPartitionChunkWriter>(
            partition_chunk_writer_factory->create("c1", partition_field_null_list));
    _writer_poller = std::make_shared<MockPoller>();
    _writer_commited = false;
    _writer_status = Status::OK();
    partition_writer->set_io_poller(_writer_poller.get());
    partition_writer->set_commit_callback([this](const CommitResult&) { _writer_commited = true; });
    partition_writer->set_error_handler([this](const Status& s) { _writer_status = s; });
    return partition_writer;
}

// The spill writer builds a storage Schema from the slots, so a column type without a storage field type (VARIANT and
// the nested types that contain it) must be rejected before the sink creates the writer, and must not abort the
// process. TIME has no storage field type either, but the writer describes it as DOUBLE, so it is accepted.
TEST_F(PartitionChunkWriterTest, spill_writer_check_tuple_supported) {
    // The types that have a storage field type are accepted, the nested ones included.
    {
        parquet::Utils::SlotDesc slot_descs[] = {
                {"c_boolean", TYPE_BOOLEAN_DESC},
                {"c_tinyint", TYPE_TINYINT_DESC},
                {"c_smallint", TYPE_SMALLINT_DESC},
                {"c_int", TYPE_INT_DESC},
                {"c_bigint", TYPE_BIGINT_DESC},
                {"c_largeint", TypeDescriptor(TYPE_LARGEINT)},
                {"c_float", TYPE_FLOAT_DESC},
                {"c_double", TYPE_DOUBLE_DESC},
                {"c_decimalv2", TypeDescriptor::create_decimalv2_type(27, 9)},
                {"c_decimal32", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 2)},
                {"c_decimal64", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 4)},
                {"c_decimal128", TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 6)},
                {"c_char", TYPE_CHAR_DESC},
                {"c_varchar", TYPE_VARCHAR_DESC},
                {"c_date", TYPE_DATE_DESC},
                {"c_datetime", TYPE_DATETIME_DESC},
                {"c_varbinary", TYPE_VARBINARY_DESC},
                {"c_json", TYPE_JSON_DESC},
                {"c_time", TYPE_TIME_DESC},
                {"c_array", TypeDescriptor::create_array_type(TYPE_INT_DESC)},
                {"c_map", TypeDescriptor::create_map_type(TYPE_VARCHAR_DESC, TYPE_INT_DESC)},
                {"c_struct", TypeDescriptor::create_struct_type({"a", "b"}, {TYPE_INT_DESC, TYPE_VARCHAR_DESC})},
                {"c_array_time", TypeDescriptor::create_array_type(TYPE_TIME_DESC)},
                {"c_map_time", TypeDescriptor::create_map_type(TYPE_VARCHAR_DESC, TYPE_TIME_DESC)},
                {"c_struct_time", TypeDescriptor::create_struct_type({"a", "b"}, {TYPE_INT_DESC, TYPE_TIME_DESC})},
                {""}};
        TupleDescriptor* tuple_desc = parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        EXPECT_OK(SpillPartitionChunkWriter::check_tuple_supported(tuple_desc, tuple_desc->slots().size()));
    }

    // VARIANT has no storage field type, alone or inside a nested type.
    const TypeDescriptor variant_type = TypeDescriptor::create_variant_type();
    const std::vector<std::pair<std::string, TypeDescriptor>> unsupported_columns = {
            {"c_variant", variant_type},
            {"c_array_variant", TypeDescriptor::create_array_type(variant_type)},
            {"c_map_variant", TypeDescriptor::create_map_type(TYPE_VARCHAR_DESC, variant_type)},
            {"c_struct_variant", TypeDescriptor::create_struct_type({"a", "b"}, {TYPE_INT_DESC, variant_type})},
            {"c_struct_array_variant",
             TypeDescriptor::create_struct_type({"a"}, {TypeDescriptor::create_array_type(variant_type)})}};
    for (const auto& [name, type] : unsupported_columns) {
        parquet::Utils::SlotDesc slot_descs[] = {{"id", TYPE_BIGINT_DESC}, {name, type}, {""}};
        TupleDescriptor* tuple_desc = parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Status st = SpillPartitionChunkWriter::check_tuple_supported(tuple_desc, 2);
        EXPECT_TRUE(st.is_not_supported()) << name << ": " << st;
        EXPECT_NE(std::string::npos, st.message().find(name)) << st;

        // Only the first |num_columns| slots carry the sink columns.
        EXPECT_OK(SpillPartitionChunkWriter::check_tuple_supported(tuple_desc, 1));
        // The tuple has fewer slots than the sink has columns.
        EXPECT_TRUE(SpillPartitionChunkWriter::check_tuple_supported(tuple_desc, 3).is_internal_error());
    }

    EXPECT_TRUE(SpillPartitionChunkWriter::check_tuple_supported(nullptr, 1).is_not_supported());
}

// A writer that is created for a tuple the sink should have rejected fails with an error when it has to create the
// merge chunk, in the sorted mode and in the mode that switches to the spill on a flush.
TEST_F(PartitionChunkWriterTest, spill_writer_returns_error_for_column_without_storage_type) {
    for (bool sorted : {false, true}) {
        std::string fs_base_path = "base_path";
        std::filesystem::create_directories(fs_base_path + "/c1");
        DeferOp cleanup([&]() { std::filesystem::remove_all(fs_base_path); });

        parquet::Utils::SlotDesc slot_descs[] = {
                {"id", TYPE_BIGINT_DESC, 1}, {"payload", TypeDescriptor::create_variant_type(), 2}, {""}};
        TupleDescriptor* tuple_desc = parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);

        WriterHelper::instance()->reset();
        std::shared_ptr<SortOrdering> sort_ordering;
        if (sorted) {
            sort_ordering = std::make_shared<SortOrdering>();
            sort_ordering->sort_key_idxes = {0};
            sort_ordering->sort_descs.descs.emplace_back(true, false);
        }
        auto partition_writer = create_spill_writer(fs_base_path, tuple_desc, nullptr, sort_ordering);
        EXPECT_OK(partition_writer->init());

        // The error comes before the rows are read, so the chunk has no rows.
        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 0);

        Status st;
        if (sorted) {
            // The sorted mode keeps the rows in memory from the first write.
            EXPECT_OK(partition_writer->write(chunk));
            st = partition_writer->flush();
            EXPECT_TRUE(st.is_not_supported()) << "sorted: " << sorted << ", " << st;
            st = partition_writer->finish();
            EXPECT_TRUE(st.is_not_supported()) << "sorted: " << sorted << ", " << st;
        } else {
            // The first flush leaves the mode that writes to the file directly, the second one spills.
            EXPECT_OK(partition_writer->write(chunk));
            EXPECT_OK(partition_writer->flush());
            EXPECT_OK(partition_writer->write(chunk));
            st = partition_writer->flush();
            EXPECT_TRUE(st.is_not_supported()) << "sorted: " << sorted << ", " << st;
        }
        EXPECT_NE(std::string::npos, st.message().find("payload")) << st;
    }
}

// TIME has no storage field type, but its column is a DoubleColumn. We expect the writer to spill, sort and merge
// TIME columns, also inside an array, as DOUBLE columns, and to hand the file writer the same TIME columns again.
TEST_F(PartitionChunkWriterTest, spill_writer_sorts_and_spills_time_columns) {
    struct Row {
        int64_t k;
        std::optional<double> start;
        std::optional<double> end;
        std::optional<std::vector<double>> times;
    };
    auto make_row = [](int64_t k, std::optional<double> start) {
        Row row{k, start, std::nullopt, std::nullopt};
        if (start.has_value()) {
            row.end = *start + 1800.0;
            row.times = std::vector<double>{*start, *start + 1.0};
        }
        return row;
    };
    // The start time is negative, null or up to the end of the day, and the rows of a chunk are not ordered.
    const std::vector<std::vector<Row>> rows_of_chunks = {
            {make_row(1, 64800.0), make_row(2, 3600.0), make_row(3, std::nullopt)},
            {make_row(4, 32400.0), make_row(5, 86399.0), make_row(6, 7200.0)},
            {make_row(7, -3600.0), make_row(8, 43200.0), make_row(9, 21600.0)}};
    // The sorted order of the keys: the start time ascending, the null first.
    const std::vector<int64_t> sorted_keys = {3, 7, 2, 6, 9, 4, 8, 1, 5};

    for (bool sorted : {true, false}) {
        std::string fs_base_path = "base_path";
        std::filesystem::create_directories(fs_base_path + "/c1");
        DeferOp cleanup([&]() { std::filesystem::remove_all(fs_base_path); });

        parquet::Utils::SlotDesc slot_descs[] = {{"k", TYPE_BIGINT_DESC, 1},
                                                 {"start_time", TYPE_TIME_DESC, 2},
                                                 {"end_time", TYPE_TIME_DESC, 3},
                                                 {"times", TypeDescriptor::create_array_type(TYPE_TIME_DESC), 4},
                                                 {""}};
        TupleDescriptor* tuple_desc = parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        ASSERT_OK(SpillPartitionChunkWriter::check_tuple_supported(tuple_desc, tuple_desc->slots().size()));

        auto writer_helper = WriterHelper::instance();
        writer_helper->reset();
        std::shared_ptr<SortOrdering> sort_ordering;
        if (sorted) {
            sort_ordering = std::make_shared<SortOrdering>();
            sort_ordering->sort_key_idxes = {1};
            sort_ordering->sort_descs.descs.emplace_back(true, true);
        }
        auto partition_writer = create_spill_writer(fs_base_path, tuple_desc, nullptr, sort_ordering);
        EXPECT_OK(partition_writer->init());

        // One spilled block per chunk
        for (const auto& rows : rows_of_chunks) {
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, rows.size());
            for (const auto& row : rows) {
                chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(row.k));
                for (size_t i = 1; i <= 2; ++i) {
                    const auto& value = i == 1 ? row.start : row.end;
                    if (value.has_value()) {
                        chunk->get_column_raw_ptr_by_index(i)->append_datum(Datum(*value));
                    } else {
                        chunk->get_column_raw_ptr_by_index(i)->append_nulls(1);
                    }
                }
                if (row.times.has_value()) {
                    DatumArray array;
                    for (double value : *row.times) {
                        array.emplace_back(Datum(value));
                    }
                    chunk->get_column_raw_ptr_by_index(3)->append_datum(Datum(array));
                } else {
                    chunk->get_column_raw_ptr_by_index(3)->append_nulls(1);
                }
            }

            EXPECT_OK(partition_writer->write(chunk));
            EXPECT_OK(partition_writer->flush());
            EXPECT_OK(partition_writer->wait_flush());
            Awaitility().timeout(3 * 1000 * 1000).interval(300 * 1000).until([partition_writer]() {
                return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
            });
            EXPECT_OK(_writer_status);
        }

        EXPECT_OK(partition_writer->finish());
        Awaitility().timeout(3 * 1000 * 1000).interval(300 * 1000).until([&]() {
            return partition_writer->is_finished();
        });
        EXPECT_EQ(_writer_commited, true);
        EXPECT_OK(_writer_status);

        // Read all the written rows back. The unsorted mode writes the first chunk directly and spills the others,
        // so it can write more than one chunk and in any order.
        std::vector<Row> actual;
        for (const auto& result_chunk : writer_helper->result_chunks()) {
            ASSERT_EQ(result_chunk->num_columns(), 4);
            // The file writer reads the TIME values of a DoubleColumn, the column of the TIME type.
            for (size_t i : {1, 2}) {
                const Column* data_column = ColumnHelper::get_data_column(result_chunk->get_column_by_index(i).get());
                EXPECT_NE(nullptr, dynamic_cast<const DoubleColumn*>(data_column));
            }
            for (size_t i = 0; i < result_chunk->num_rows(); ++i) {
                Row row;
                row.k = result_chunk->get_column_by_index(0)->get(i).get_int64();
                auto start = result_chunk->get_column_by_index(1)->get(i);
                auto end = result_chunk->get_column_by_index(2)->get(i);
                auto times = result_chunk->get_column_by_index(3)->get(i);
                if (!start.is_null()) {
                    row.start = start.get_double();
                }
                if (!end.is_null()) {
                    row.end = end.get_double();
                }
                if (!times.is_null()) {
                    row.times = std::vector<double>();
                    for (const auto& element : times.get_array()) {
                        row.times->push_back(element.get_double());
                    }
                }
                actual.push_back(std::move(row));
            }
        }
        ASSERT_EQ(actual.size(), 9) << "sorted: " << sorted;

        if (!sorted) {
            std::sort(actual.begin(), actual.end(), [](const Row& a, const Row& b) { return a.k < b.k; });
        }
        std::map<int64_t, Row> expected_by_key;
        for (const auto& rows : rows_of_chunks) {
            for (const auto& row : rows) {
                expected_by_key[row.k] = row;
            }
        }
        for (size_t i = 0; i < actual.size(); ++i) {
            int64_t expected_key = sorted ? sorted_keys[i] : static_cast<int64_t>(i + 1);
            const Row& expected = expected_by_key[expected_key];
            const Row& row = actual[i];
            EXPECT_EQ(row.k, expected_key) << "sorted: " << sorted << ", row: " << i;
            EXPECT_EQ(row.start, expected.start) << "key: " << expected_key;
            EXPECT_EQ(row.end, expected.end) << "key: " << expected_key;
            EXPECT_EQ(row.times, expected.times) << "key: " << expected_key;
        }
    }
}

// The sink columns are the first slots of the tuple. A tuple that has more slots than the sink has columns is spilled
// with the sink columns only, and the extra slot is not read, whatever its type is.
TEST_F(PartitionChunkWriterTest, spill_writer_uses_only_the_sink_columns_of_a_wider_tuple) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");
    DeferOp cleanup([&]() { std::filesystem::remove_all(fs_base_path); });

    parquet::Utils::SlotDesc slot_descs[] = {{"k", TYPE_BIGINT_DESC, 0},
                                             {"name", TYPE_VARCHAR_DESC, 1},
                                             {"hidden", TypeDescriptor::create_variant_type(), 2},
                                             {""}};
    TupleDescriptor* tuple_desc = parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    auto make_evaluators = [](const std::vector<TypeDescriptor>& types) {
        return std::make_shared<std::vector<std::unique_ptr<ColumnEvaluator>>>(
                ColumnSlotIdEvaluator::from_types(types));
    };
    auto make_sort_ordering = [](uint32_t key_idx) {
        auto sort_ordering = std::make_shared<SortOrdering>();
        sort_ordering->sort_key_idxes = {key_idx};
        sort_ordering->sort_descs.descs.emplace_back(true, false);
        return sort_ordering;
    };

    {
        auto writer_helper = WriterHelper::instance();
        writer_helper->reset();
        auto partition_writer =
                create_spill_writer(fs_base_path, tuple_desc, make_evaluators({TYPE_BIGINT_DESC, TYPE_VARCHAR_DESC}),
                                    make_sort_ordering(0));
        EXPECT_OK(partition_writer->init());

        const std::vector<std::vector<int64_t>> keys_of_chunks = {{9, 3, 6}, {8, 2, 5}, {7, 1, 4}};
        for (const auto& keys : keys_of_chunks) {
            ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, keys.size());
            for (int64_t key : keys) {
                std::string name = "name" + std::to_string(key);
                chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(key));
                chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(Slice(name)));
            }
            chunk->get_column_raw_ptr_by_index(2)->append_nulls(keys.size());

            EXPECT_OK(partition_writer->write(chunk));
            EXPECT_OK(partition_writer->flush());
            EXPECT_OK(partition_writer->wait_flush());
            Awaitility().timeout(3 * 1000 * 1000).interval(300 * 1000).until([partition_writer]() {
                return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
            });
            EXPECT_OK(_writer_status);
        }

        EXPECT_OK(partition_writer->finish());
        Awaitility().timeout(3 * 1000 * 1000).interval(300 * 1000).until([&]() {
            return partition_writer->is_finished();
        });
        EXPECT_EQ(_writer_commited, true);
        EXPECT_OK(_writer_status);

        ASSERT_EQ(writer_helper->result_chunks().size(), 1);
        auto result_chunk = writer_helper->result_chunks()[0];
        ASSERT_EQ(result_chunk->num_columns(), 2);
        ASSERT_EQ(result_chunk->num_rows(), 9);
        for (size_t i = 0; i < result_chunk->num_rows(); ++i) {
            int64_t key = result_chunk->get_column_by_index(0)->get(i).get_int64();
            EXPECT_EQ(key, static_cast<int64_t>(i + 1));
            EXPECT_EQ(result_chunk->get_column_by_index(1)->get(i).get_slice().to_string(),
                      "name" + std::to_string(key));
        }
    }

    // The tuple has fewer slots than the sink has columns: an error, and no read of a column that does not exist.
    {
        auto partition_writer = create_spill_writer(
                fs_base_path, tuple_desc,
                make_evaluators({TYPE_BIGINT_DESC, TYPE_VARCHAR_DESC, TYPE_INT_DESC, TYPE_INT_DESC}),
                make_sort_ordering(0));
        EXPECT_OK(partition_writer->init());
        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 0);
        EXPECT_OK(partition_writer->write(chunk));
        Status st = partition_writer->flush();
        EXPECT_TRUE(st.is_internal_error()) << st;
    }

    // The sort key is not a sink column.
    {
        auto partition_writer =
                create_spill_writer(fs_base_path, tuple_desc, make_evaluators({TYPE_BIGINT_DESC, TYPE_VARCHAR_DESC}),
                                    make_sort_ordering(2));
        EXPECT_OK(partition_writer->init());
        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 0);
        EXPECT_OK(partition_writer->write(chunk));
        Status st = partition_writer->flush();
        EXPECT_TRUE(st.is_internal_error()) << st;
    }

    // Without evaluators, the sink columns are all the slots, and a chunk has fewer columns than the schema.
    {
        parquet::Utils::SlotDesc two_column_descs[] = {
                {"k", TYPE_BIGINT_DESC, 0}, {"name", TYPE_VARCHAR_DESC, 1}, {""}};
        TupleDescriptor* two_column_tuple =
                parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, two_column_descs);
        parquet::Utils::SlotDesc one_column_descs[] = {{"k", TYPE_BIGINT_DESC, 0}, {""}};
        TupleDescriptor* one_column_tuple =
                parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, one_column_descs);
        auto partition_writer = create_spill_writer(fs_base_path, two_column_tuple, nullptr, make_sort_ordering(0));
        EXPECT_OK(partition_writer->init());
        ChunkPtr chunk = ChunkHelper::new_chunk(*one_column_tuple, 0);
        EXPECT_OK(partition_writer->write(chunk));
        Status st = partition_writer->flush();
        EXPECT_TRUE(st.is_internal_error()) << st;
    }
}

// Reads the column of a slot as the chunk has it. When |fail| is set, it fails on a chunk with |failing_rows| rows, to
// fail the merge after the evaluation of the other chunks. When |copy| is set, it returns a copy of the column, which
// the writer can not map back to a column of the chunk.
class TestSlotEvaluator final : public ColumnEvaluator {
public:
    TestSlotEvaluator(SlotId slot_id, TypeDescriptor type, std::shared_ptr<std::atomic<bool>> fail, size_t failing_rows,
                      bool copy)
            : _slot_id(slot_id),
              _type(std::move(type)),
              _fail(std::move(fail)),
              _failing_rows(failing_rows),
              _copy(copy) {}

    Status init() override { return Status::OK(); }

    std::unique_ptr<ColumnEvaluator> clone() const override {
        return std::make_unique<TestSlotEvaluator>(_slot_id, _type, _fail, _failing_rows, _copy);
    }

    TypeDescriptor type() const override { return _type; }

    StatusOr<ColumnPtr> evaluate(Chunk* chunk) override {
        if (_fail != nullptr && _fail->load() && chunk->num_rows() == _failing_rows) {
            return Status::InternalError("simulated evaluator failure");
        }
        ColumnPtr column = chunk->get_column_by_slot_id(_slot_id);
        if (_copy) {
            ColumnPtr copied = column->clone();
            return copied;
        }
        return column;
    }

private:
    SlotId _slot_id;
    TypeDescriptor _type;
    std::shared_ptr<std::atomic<bool>> _fail;
    size_t _failing_rows;
    bool _copy;
};

// The merge of the buffered chunks into one chunk can fail after it has read some of them. We expect the writer to keep
// every buffered chunk then, and to answer the calls that follow (a flush again, wait, finish, the destructor) with
// the same error or with a success, and never with a null chunk. Nothing may be lost either: after the cause of a
// failure is gone, the same rows are written.
TEST_F(PartitionChunkWriterTest, spill_writer_keeps_buffered_chunks_when_the_merge_fails) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");
    DeferOp cleanup([&]() { std::filesystem::remove_all(fs_base_path); });

    parquet::Utils::SlotDesc slot_descs[] = {{"k", TYPE_BIGINT_DESC, 0}, {"name", TYPE_VARCHAR_DESC, 1}, {""}};
    TupleDescriptor* tuple_desc = parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);

    auto fail = std::make_shared<std::atomic<bool>>(false);
    auto make_evaluators = [&](bool copy_name_column) {
        auto evaluators = std::make_shared<std::vector<std::unique_ptr<ColumnEvaluator>>>();
        evaluators->push_back(std::make_unique<TestSlotEvaluator>(0, TYPE_BIGINT_DESC, nullptr, 0, false));
        evaluators->push_back(
                std::make_unique<TestSlotEvaluator>(1, TYPE_VARCHAR_DESC, fail, 2 /* rows */, copy_name_column));
        return evaluators;
    };
    auto make_sort_ordering = []() {
        auto sort_ordering = std::make_shared<SortOrdering>();
        sort_ordering->sort_key_idxes = {0};
        sort_ordering->sort_descs.descs.emplace_back(true, false);
        return sort_ordering;
    };
    // The first chunk has one row and the second one has two rows, which the evaluator can tell apart.
    auto make_chunk = [&](const std::vector<std::pair<int64_t, std::string>>& rows) {
        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, rows.size());
        for (const auto& [k, name] : rows) {
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(k));
            chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(Slice(name)));
        }
        return chunk;
    };
    // The calls that the sink makes after a failed flush: finish, which flushes, waits and finishes the writers, and
    // then the destructor.
    auto call_like_the_sink = [](SpillPartitionChunkWriter* writer) {
        Status st = writer->flush();
        EXPECT_FALSE(st.ok());
        EXPECT_OK(writer->wait_flush());
        EXPECT_TRUE(writer->is_finished());
        st = writer->finish();
        EXPECT_FALSE(st.ok());
    };

    // The merge fails on the second chunk, after it has read the first one.
    {
        WriterHelper::instance()->reset();
        auto partition_writer =
                create_spill_writer(fs_base_path, tuple_desc, make_evaluators(false), make_sort_ordering());
        EXPECT_OK(partition_writer->init());
        EXPECT_OK(partition_writer->write(make_chunk({{2, "b"}})));
        EXPECT_OK(partition_writer->write(make_chunk({{1, "a"}, {3, "c"}})));

        fail->store(true);
        Status st = partition_writer->flush();
        EXPECT_TRUE(st.is_internal_error()) << st;
        EXPECT_TRUE(_writer_status.is_internal_error())
                << "the flush reports its error to the sink: " << _writer_status;
        call_like_the_sink(partition_writer.get());
        EXPECT_GT(partition_writer->get_flushable_bytes(), 0) << "the rows are still buffered";

        // The cause is gone: the same rows are spilled and written, in order.
        fail->store(false);
        _writer_status = Status::OK();
        EXPECT_OK(partition_writer->flush());
        EXPECT_OK(partition_writer->wait_flush());
        Awaitility().timeout(3 * 1000 * 1000).interval(300 * 1000).until([&]() {
            return partition_writer->_spilling_bytes_usage.load(std::memory_order_relaxed) == 0;
        });
        EXPECT_OK(partition_writer->finish());
        Awaitility().timeout(3 * 1000 * 1000).interval(300 * 1000).until([&]() {
            return partition_writer->is_finished();
        });
        EXPECT_EQ(_writer_commited, true);
        EXPECT_OK(_writer_status);

        auto writer_helper = WriterHelper::instance();
        ASSERT_EQ(writer_helper->result_chunks().size(), 1);
        auto result_chunk = writer_helper->result_chunks()[0];
        ASSERT_EQ(result_chunk->num_rows(), 3);
        const std::vector<std::string> expected_names = {"a", "b", "c"};
        for (size_t i = 0; i < 3; ++i) {
            EXPECT_EQ(result_chunk->get_column_by_index(0)->get(i).get_int64(), static_cast<int64_t>(i + 1));
            EXPECT_EQ(result_chunk->get_column_by_index(1)->get(i).get_slice().to_string(), expected_names[i]);
        }
    }

    // The evaluator gives a column that is not a column of the chunk. The writer finds it before it reads any chunk.
    {
        WriterHelper::instance()->reset();
        auto partition_writer =
                create_spill_writer(fs_base_path, tuple_desc, make_evaluators(true), make_sort_ordering());
        EXPECT_OK(partition_writer->init());
        EXPECT_OK(partition_writer->write(make_chunk({{2, "b"}})));
        EXPECT_OK(partition_writer->write(make_chunk({{1, "a"}, {3, "c"}})));

        Status st = partition_writer->flush();
        EXPECT_TRUE(st.is_internal_error()) << st;
        EXPECT_NE(std::string::npos, st.message().find("unknown column index")) << st;
        call_like_the_sink(partition_writer.get());
        // The writer still takes chunks, and fails again the same way.
        EXPECT_OK(partition_writer->write(make_chunk({{4, "d"}})));
        EXPECT_FALSE(partition_writer->flush().ok());
    }

    // The same without the sort: the chunks are written to the file directly when the sink finishes.
    {
        WriterHelper::instance()->reset();
        auto partition_writer = create_spill_writer(fs_base_path, tuple_desc, make_evaluators(true), nullptr);
        EXPECT_OK(partition_writer->init());
        EXPECT_OK(partition_writer->write(make_chunk({{2, "b"}})));
        // The first flush leaves the mode that writes to the file directly.
        EXPECT_OK(partition_writer->flush());
        EXPECT_OK(partition_writer->write(make_chunk({{1, "a"}, {3, "c"}})));
        EXPECT_FALSE(partition_writer->flush().ok());
        EXPECT_OK(partition_writer->wait_flush());
        // The rows are still in the writer, so finish() writes them to the file directly.
        EXPECT_OK(partition_writer->finish());
        EXPECT_EQ(WriterHelper::instance()->result_rows(), 3);
    }
}

// The same failure seen from the sink: the operator finishes the sink, which flushes every writer, and the fragment
// is cancelled. The sink is finished and rolled back again while it closes. None of it may reach a released chunk.
TEST_F(PartitionChunkWriterTest, sink_calls_after_a_failed_merge_do_not_crash) {
    std::string fs_base_path = "base_path";
    std::filesystem::create_directories(fs_base_path + "/c1");
    DeferOp cleanup([&]() { std::filesystem::remove_all(fs_base_path); });

    parquet::Utils::SlotDesc slot_descs[] = {{"k", TYPE_BIGINT_DESC, 0}, {"name", TYPE_VARCHAR_DESC, 1}, {""}};
    TupleDescriptor* tuple_desc = parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);

    auto mock_writer_factory = std::make_shared<MockFileWriterFactory>();
    EXPECT_CALL(*mock_writer_factory, create(::testing::_)).WillRepeatedly([](const std::string&) {
        WriterAndStream ws;
        ws.writer = std::make_unique<MockWriter>();
        ws.stream = std::make_unique<Stream>(std::make_unique<MockFile>(), nullptr, nullptr);
        return ws;
    });
    auto location_provider = std::make_shared<LocationProvider>(fs_base_path, "ffffff", 0, 0, "parquet");

    // The second column is a copy of the column of the chunk, which the writer can not map back to the chunk.
    auto evaluators = std::make_shared<std::vector<std::unique_ptr<ColumnEvaluator>>>();
    evaluators->push_back(std::make_unique<TestSlotEvaluator>(0, TYPE_BIGINT_DESC, nullptr, 0, false));
    evaluators->push_back(std::make_unique<TestSlotEvaluator>(1, TYPE_VARCHAR_DESC, nullptr, 0, true));
    auto sort_ordering = std::make_shared<SortOrdering>();
    sort_ordering->sort_key_idxes = {0};
    sort_ordering->sort_descs.descs.emplace_back(true, false);
    auto partition_chunk_writer_ctx = std::make_shared<SpillPartitionChunkWriterContext>(
            SpillPartitionChunkWriterContext{{mock_writer_factory, location_provider, 1073741824, false},
                                             nullptr,
                                             _fragment_context.get(),
                                             tuple_desc,
                                             evaluators,
                                             sort_ordering});
    auto partition_chunk_writer_factory =
            std::make_unique<SpillPartitionChunkWriterFactory>(partition_chunk_writer_ctx);

    std::vector<std::unique_ptr<ColumnEvaluator>> partition_column_evaluators;
    auto sink = std::make_unique<IcebergChunkSink>(std::vector<std::string>{}, std::vector<std::string>{},
                                                   std::move(partition_column_evaluators),
                                                   std::move(partition_chunk_writer_factory), _runtime_state);
    auto poller = MockPoller();
    sink->set_io_poller(&poller);
    sink->init_profile();

    for (int64_t k = 0; k < 3; ++k) {
        ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 1);
        std::string name = "name" + std::to_string(k);
        chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(k));
        chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(Slice(name)));
        EXPECT_OK(sink->add(chunk));
    }

    EXPECT_FALSE(sink->finish().ok());
    EXPECT_FALSE(sink->status().ok()) << "the writer reports its error to the sink";
    EXPECT_FALSE(sink->finish().ok());
    sink->rollback();
    EXPECT_TRUE(sink->is_finished());
}

} // namespace
} // namespace starrocks::connector
