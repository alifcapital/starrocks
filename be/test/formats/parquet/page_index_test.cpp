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

#include <gtest/gtest.h>

#include <filesystem>
#include <limits>
#include <random>
#include <tuple>
#include <utility>
#include <vector>

#include "column/column_helper.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "exprs/binary_predicate.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/group_reader.h"
#include "formats/parquet/parquet_block_split_bloom_filter.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "formats/parquet/parquet_ut_base.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/stored_column_reader_with_index.h"
#include "fs/fs.h"
#include "io/shared_buffered_input_stream.h"
#include "io/string_input_stream.h"
#include "util/thrift_util.h"

namespace starrocks::parquet {

static HdfsScannerStats g_hdfs_stats;
using starrocks::HdfsScannerContext;

class PageIndexTest : public testing::Test {
public:
    void SetUp() override { _runtime_state = _pool.add(new RuntimeState(TQueryGlobals())); }

    void TearDown() override {}

protected:
    std::unique_ptr<RandomAccessFile> _create_file(const std::string& file_path);

    HdfsScannerContext* _create_scan_context();

    THdfsScanRange* _create_scan_range(const std::string& file_path, size_t scan_length = 0);

    HdfsScannerContext* _create_file_random_read_context(const std::string& file_path);
    HdfsScannerContext* _create_file_only_c0_context(const std::string& file_path);
    HdfsScannerContext* _create_file_c0_c1_c2_context(const std::string& file_path);

    RuntimeState* _runtime_state = nullptr;
    ObjectPool _pool;

    HdfsScannerContext _scanner_ctx;
};

std::unique_ptr<RandomAccessFile> PageIndexTest::_create_file(const std::string& file_path) {
    return *FileSystem::Default()->new_random_access_file(file_path);
}

HdfsScannerContext* PageIndexTest::_create_scan_context() {
    auto* ctx = _pool.add(new HdfsScannerContext());
    auto* lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));
    ctx->lazy_column_coalesce_counter = lazy_column_coalesce_counter;

    ctx->timezone = "Asia/Shanghai";
    ctx->stats = &g_hdfs_stats;
    ctx->options.parquet_page_index_enable = true;
    ctx->options.parquet_bloom_filter_enable = true;
    return ctx;
}

HdfsScannerContext* PageIndexTest::_create_file_random_read_context(const std::string& file_path) {
    auto ctx = _create_scan_context();

    TypeDescriptor type_array(LogicalType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {"c3", type_array},
            {""},
    };

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = (_create_scan_range(file_path));

    return ctx;
}

HdfsScannerContext* PageIndexTest::_create_file_only_c0_context(const std::string& file_path) {
    auto ctx = _create_scan_context();

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {""},
    };
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = (_create_scan_range(file_path));

    return ctx;
}

HdfsScannerContext* PageIndexTest::_create_file_c0_c1_c2_context(const std::string& file_path) {
    auto ctx = _create_scan_context();

    // tuple desc
    Utils::SlotDesc slot_descs[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
            {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {""},
    };
    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = (_create_scan_range(file_path));

    return ctx;
}

THdfsScanRange* PageIndexTest::_create_scan_range(const std::string& file_path, size_t scan_length) {
    auto* scan_range = _pool.add(new THdfsScanRange());

    scan_range->relative_path = file_path;
    scan_range->file_length = std::filesystem::file_size(file_path);
    scan_range->offset = 4;
    scan_range->length = scan_length > 0 ? scan_length : scan_range->file_length;

    return scan_range;
}

TEST_F(PageIndexTest, TestRandomReadWith2PageSize) {
    std::random_device rd;
    std::mt19937 rng(rd());

    TypeDescriptor type_array(LogicalType::TYPE_ARRAY);
    type_array.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(
            ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true),
            chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(type_array, true), chunk->num_columns());

    // c0 = np.arange(1, 20001)
    // c1 = np.arange(20000, 0, -1)
    // data = {
    //     'c0': c0,
    //     'c1': c1
    // }
    // df = pd.DataFrame(data)
    // df_with_dict = pd.DataFrame({
    //     "c0": df["c0"],
    //     "c1": df["c1"],
    //     "c2": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else str(x["c0"] % 100), axis = 1),
    //     "c3": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else [x["c0"] % 1000, pd.NA, x["c1"] % 1000], axis = 1)
    // })
    const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

    // c0 = np.arange(1, 100001)
    // c1 = np.arange(100000, 0, -1)
    // data = {
    //     'c0': c0,
    //     'c1': c1
    // }
    // df = pd.DataFrame(data)
    // df_with_dict = pd.DataFrame({
    //     "c0": df["c0"],
    //     "c1": df["c1"],
    //     "c2": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else str(x["c0"] % 100), axis = 1),
    //     "c3": df.apply(lambda x: pd.NA if x["c0"] % 10 == 0 else [x["c0"] % 1000, pd.NA, x["c1"] % 1000], axis = 1)
    // })
    const std::string big_page_file = "./be/test/formats/parquet/test_data/page_index_big_page.parquet";
    // same data with above but without dictionary
    const std::string repeated_no_dict_file = "./be/test//formats/parquet/test_data/page_index_repeated_nodict.parquet";

    std::vector<std::string> files = {small_page_file, big_page_file, repeated_no_dict_file};

    // for small page 1000 values / page
    // for big page 10000 values / page
    for (size_t index = 0; index < 3; index++) {
        const std::string& file_path = files[index];
        std::cout << "file_path: " << file_path << std::endl;

        std::uniform_int_distribution<int> dist_small(1, 20000);
        std::uniform_int_distribution<int> dist_big(1, 100000);

        std::vector<int> oprands;
        size_t expected_row = 0;
        auto _print_predicate = [&](bool single) {
            std::stringstream ss;
            ss << "expected_row: " << expected_row << " predicate: c0 > " << oprands[0] << " and c0 < " << oprands[1];
            if (single) {
                return ss.str();
            }
            ss << " and c1 > " << oprands[2] << " and c1 < " << oprands[3];
            return ss.str();
        };

        std::vector<bool> single_or_not{true, false};

        for (bool single_flag : single_or_not) {
            // use 2 to save ci's time, change bigger to test more case
            for (int32_t i = 0; i < 2; i++) {
                oprands.clear();
                for (int32_t j = 0; j < 4; j++) {
                    int num = index == 0 ? dist_small(rng) : dist_big(rng);
                    oprands.emplace_back(num);
                }
                for (int k : std::vector<int>{0, 2}) {
                    if (oprands[k] > oprands[k + 1]) {
                        int temp = oprands[k];
                        oprands[k] = oprands[k + 1];
                        oprands[k + 1] = temp;
                    }
                }

                auto ctx = _create_file_random_read_context(file_path);
                auto file = _create_file(file_path);
                ctx->conjunct_ctxs_by_slot[0].clear();
                ctx->conjuncts.min_max_ctxs.clear();

                if (single_flag) {
                    Utils::SlotDesc min_max_slots[] = {
                            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0},
                            {""},
                    };
                    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

                    std::vector<TExpr> t_conjuncts;
                    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, oprands[0], &t_conjuncts);
                    ParquetUTBase::append_int_conjunct(TExprOpcode::LT, 0, oprands[1], &t_conjuncts);

                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                                        &ctx->conjuncts.min_max_ctxs);
                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                                        &ctx->conjunct_ctxs_by_slot[0]);

                    expected_row = std::max(oprands[1] - oprands[0] - 1, 0);
                } else {
                    ctx->conjunct_ctxs_by_slot[1].clear();
                    Utils::SlotDesc min_max_slots[] = {
                            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0},
                            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 1},
                            {""},
                    };
                    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

                    std::vector<TExpr> t_conjuncts;
                    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, oprands[0], &t_conjuncts);
                    ParquetUTBase::append_int_conjunct(TExprOpcode::LT, 0, oprands[1], &t_conjuncts);
                    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 1, oprands[2], &t_conjuncts);
                    ParquetUTBase::append_int_conjunct(TExprOpcode::LT, 1, oprands[3], &t_conjuncts);

                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts,
                                                        &ctx->conjuncts.min_max_ctxs);

                    std::vector<TExpr> t_conjuncts_slot0{t_conjuncts[0], t_conjuncts[1]};
                    std::vector<TExpr> t_conjuncts_slot1{t_conjuncts[2], t_conjuncts[3]};

                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot0,
                                                        &ctx->conjunct_ctxs_by_slot[0]);
                    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot1,
                                                        &ctx->conjunct_ctxs_by_slot[1]);

                    int low_bound = std::max(oprands[0], index == 0 ? 20001 - oprands[3] : 100001 - oprands[3]);
                    int up_bound = std::min(oprands[1], index == 0 ? 20001 - oprands[2] : 100001 - oprands[2]);
                    expected_row = std::max(up_bound - low_bound - 1, 0);
                }

                std::cout << "file path: " << file_path << ", " << _print_predicate(single_flag) << std::endl;

                auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                                std::filesystem::file_size(file_path));

                Status status = file_reader->init(ctx);
                ASSERT_TRUE(status.ok());
                size_t total_row_nums = 0;
                while (!status.is_end_of_file()) {
                    chunk->reset();
                    status = file_reader->get_next(&chunk);
                    chunk->check_or_die();
                    total_row_nums += chunk->num_rows();
                    if (!status.ok() && !status.is_end_of_file()) {
                        std::cout << status.message() << std::endl;
                        DCHECK(false) << "file path: " << file_path << ", " << _print_predicate(single_flag);
                    }
                    // check row value
                    if (chunk->num_rows() > 0) {
                        ColumnPtr c0 = chunk->get_column_by_index(0);
                        ColumnPtr c1 = chunk->get_column_by_index(1);
                        ColumnPtr c2 = chunk->get_column_by_index(2);
                        ColumnPtr c3 = chunk->get_column_by_index(3);
                        for (size_t row_index = 0; row_index < chunk->num_rows(); row_index++) {
                            int32_t c0_value = c0->get(row_index).get_int32();
                            int32_t c1_value = c1->get(row_index).get_int32();
                            bool flag = index == 0 ? c0_value + c1_value == 20001 : c0_value + c1_value == 100001;
                            if (c0_value % 10 == 0) {
                                flag &= c2->is_null(row_index);
                                flag &= c3->is_null(row_index);
                            } else {
                                flag &= (!c2->is_null(row_index));
                                flag &= (!c3->is_null(row_index));
                                if (!flag) {
                                    std::cout << "file path: " << file_path << ", " << _print_predicate(single_flag);
                                }
                                EXPECT_TRUE(flag);
                                std::string expected_string = std::to_string(c0_value % 100);
                                Slice expected_value = Slice(expected_string);
                                Slice c2_value = c2->get(row_index).get_slice();
                                flag &= (c2_value == expected_value);
                                DatumArray c3_value = c3->get(row_index).get_array();
                                flag &= (c3_value.size() == 3) && (!c3_value[0].is_null()) &&
                                        (c3_value[0].get_int32() == (c0_value % 1000)) && (c3_value[1].is_null()) &&
                                        (!c3_value[2].is_null()) && (c3_value[2].get_int32() == (c1_value % 1000));
                            }
                            if (!flag) {
                                std::cout << "file path: " << file_path << ", " << _print_predicate(single_flag);
                            }
                            EXPECT_TRUE(flag);
                        }
                    }
                }
                EXPECT_EQ(total_row_nums, expected_row);
            }
        }
    }
}

TEST_F(PageIndexTest, TestCollectIORangeWithPageIndex) {
    auto test = [&]() {
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(
                ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                chunk->num_columns());

        const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

        Utils::SlotDesc min_max_slots[] = {
                {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0},
                {""},
        };

        auto ctx = _create_file_only_c0_context(small_page_file);
        auto file = _create_file(small_page_file);
        ctx->conjunct_ctxs_by_slot[0].clear();
        ctx->conjuncts.min_max_ctxs.clear();
        ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

        std::vector<TExpr> t_conjuncts;
        ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, 5500, &t_conjuncts);
        ParquetUTBase::append_int_conjunct(TExprOpcode::LT, 0, 7500, &t_conjuncts);

        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->conjuncts.min_max_ctxs);
        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->conjunct_ctxs_by_slot[0]);

        // tuple desc
        Utils::SlotDesc slot_descs[] = {
                {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                {""},
        };
        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        std::vector<ExprContext*> all_conjuncts{};
        for (auto* expr : ctx->conjuncts.min_max_ctxs) {
            all_conjuncts.push_back(expr);
        }
        for (auto* expr : ctx->conjunct_ctxs_by_slot[0]) {
            all_conjuncts.push_back(expr);
        }
        ParquetUTBase::setup_conjuncts_manager(all_conjuncts, nullptr, tuple_desc, _runtime_state, ctx);

        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(small_page_file));

        Status status = file_reader->init(ctx);
        ASSERT_TRUE(status.ok());

        // two row groups, but one is filtered.
        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);
        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;

        file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset, ColumnIOType::PAGE_INDEX);
        // collect io of column index and offset index for active column.
        EXPECT_EQ(ranges.size(), 2);
        // offset_index_offset = 293436, offset_index_length = 113, column_index_offset = 291196, column_index_length = 211
        EXPECT_EQ(ranges[1].offset, 293436);
        EXPECT_EQ(ranges[1].size, 113);

        ranges.clear();
        end_offset = 0;

        file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);
        // 3 pages, 1 range 5000-8000
        EXPECT_EQ(file_reader->_row_group_readers[0]->_range.size(), 1);
        // only collect io of 3 pages, 5001-6000, 6001-7000, 7001-8000 and a dict page.
        EXPECT_EQ(ranges.size(), 4);
        // page 7001-8000: offset 50814, size 1660
        EXPECT_EQ(end_offset, 52474);

        size_t total_row_nums = 0;
        while (!status.is_end_of_file()) {
            chunk->reset();
            status = file_reader->get_next(&chunk);
            chunk->check_or_die();
            total_row_nums += chunk->num_rows();
        }
        EXPECT_EQ(total_row_nums, 1999);
    };

    test();
}

TEST_F(PageIndexTest, TestTwoColumnIntersectPageIndex) {
    auto test = [&]() {
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(
                ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                chunk->num_columns());
        chunk->append_column(
                ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                chunk->num_columns());
        chunk->append_column(
                ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true),
                chunk->num_columns());

        const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

        Utils::SlotDesc min_max_slots[] = {
                {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0},
                {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 1},
                {""},
        };

        auto ctx = _create_file_c0_c1_c2_context(small_page_file);
        auto file = _create_file(small_page_file);
        ctx->conjunct_ctxs_by_slot[0].clear();
        ctx->conjunct_ctxs_by_slot[1].clear();
        ctx->conjuncts.min_max_ctxs.clear();
        ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

        std::vector<TExpr> t_conjuncts;
        // c0: 1->20000, c0 > 5000
        ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, 5000, &t_conjuncts);
        // c1: 20000->1, c1 > 5000
        ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 1, 5000, &t_conjuncts);

        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->conjuncts.min_max_ctxs);

        std::vector<TExpr> t_conjuncts_slot0{t_conjuncts[0]};
        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot0, &ctx->conjunct_ctxs_by_slot[0]);

        std::vector<TExpr> t_conjuncts_slot1{t_conjuncts[1]};
        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot1, &ctx->conjunct_ctxs_by_slot[1]);

        // tuple desc
        Utils::SlotDesc slot_descs[] = {
                {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)},
                {"c2", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
                {""},
        };
        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        std::vector<ExprContext*> all_conjuncts{};
        for (auto* expr : ctx->conjuncts.min_max_ctxs) {
            all_conjuncts.push_back(expr);
        }
        for (auto* expr : ctx->conjunct_ctxs_by_slot[0]) {
            all_conjuncts.push_back(expr);
        }
        for (auto* expr : ctx->conjunct_ctxs_by_slot[1]) {
            all_conjuncts.push_back(expr);
        }
        ParquetUTBase::setup_conjuncts_manager(all_conjuncts, nullptr, tuple_desc, _runtime_state, ctx);

        auto shared_buffer = std::make_shared<io::SharedBufferedInputStream>(
                file->stream(), small_page_file, std::filesystem::file_size(small_page_file));
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(small_page_file), DataCacheOptions(),
                                                        shared_buffer.get());

        Status status = file_reader->init(ctx);
        ASSERT_TRUE(status.ok());

        // two row groups.
        EXPECT_EQ(file_reader->_row_group_readers.size(), 2);
        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;

        for (auto& r : file_reader->_row_group_readers) {
            r->collect_io_ranges(&ranges, &end_offset, ColumnIOType::PAGE_INDEX);
        }

        // collect io of column index and offset index for active column,
        // and offset index for lazy column
        // and two group collect together. (2 + 2 + 1) * 2 = 10
        EXPECT_EQ(ranges.size(), 10);

        ranges.clear();
        end_offset = 0;

        file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);
        // only collect io of 5 pages, 5001-6000, 6001-7000, 7001-8000, 8001-9000, 9001-10000 and a dict page.
        // three columns, (5 + 1) * 3 = 18
        EXPECT_EQ(ranges.size(), 18);

        EXPECT_EQ(shared_buffer->current_range_ref_sum(), 28);

        // The second row group is not prepare yet

        size_t total_row_nums = 0;
        while (!status.is_end_of_file()) {
            chunk->reset();
            status = file_reader->get_next(&chunk);
            chunk->check_or_die();
            total_row_nums += chunk->num_rows();
        }
        EXPECT_EQ(total_row_nums, 10000);
    };

    test();
}

TEST_F(PageIndexTest, TestPageIndexNoPageFiltered) {
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), true),
                         chunk->num_columns());
    chunk->append_column(
            ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true),
            chunk->num_columns());

    const std::string small_page_file = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

    Utils::SlotDesc min_max_slots[] = {
            {"c0", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 0},
            {"c1", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 1},
            {""},
    };

    auto ctx = _create_file_c0_c1_c2_context(small_page_file);
    auto file = _create_file(small_page_file);
    ctx->conjunct_ctxs_by_slot[0].clear();
    ctx->conjunct_ctxs_by_slot[1].clear();
    ctx->conjuncts.min_max_ctxs.clear();
    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

    std::vector<TExpr> t_conjuncts;
    // c0: 1->20000, c0 > 500
    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, 500, &t_conjuncts);
    // c1: 20000->1, c1 > 500
    ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 1, 500, &t_conjuncts);

    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->conjuncts.min_max_ctxs);

    std::vector<TExpr> t_conjuncts_slot0{t_conjuncts[0]};
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot0, &ctx->conjunct_ctxs_by_slot[0]);

    std::vector<TExpr> t_conjuncts_slot1{t_conjuncts[1]};
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts_slot1, &ctx->conjunct_ctxs_by_slot[1]);

    auto shared_buffer = std::make_shared<io::SharedBufferedInputStream>(file->stream(), small_page_file,
                                                                         std::filesystem::file_size(small_page_file));
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(small_page_file), DataCacheOptions(),
                                                    shared_buffer.get());

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    // two row groups.
    EXPECT_EQ(file_reader->_row_group_readers.size(), 2);
    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;

    for (auto& r : file_reader->_row_group_readers) {
        r->collect_io_ranges(&ranges, &end_offset, ColumnIOType::PAGE_INDEX);
    }

    // collect io of column index and offset index for active column,
    // and offset index for lazy column
    // and two group collect together. (2 + 2 + 1) * 2 = 10
    EXPECT_EQ(ranges.size(), 10);
    std::cout << "range ref sum:" << shared_buffer->current_range_ref_sum() << std::endl;

    ranges.clear();
    end_offset = 0;

    file_reader->_row_group_readers[0]->collect_io_ranges(&ranges, &end_offset);
    // only collect io of 1 chunk / column.
    // three columns, 1 * 3 = 3

    for (auto r : ranges) {
        std::cout << r.offset << " " << r.size << ' ' << r.is_active << std::endl;
    }
    EXPECT_EQ(ranges.size(), 3);

    EXPECT_EQ(shared_buffer->current_range_ref_sum(), 13);

    // The second row group is not prepare yet

    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
    }
    EXPECT_EQ(total_row_nums, 19000);
}

// Test for parquet files with empty null_counts in ColumnIndex.
// This can happen with some parquet writers that don't populate null_counts.
// The bug was accessing column_index.null_counts[i] without bounds checking.
TEST_F(PageIndexTest, TestEmptyNullCountsInColumnIndex) {
    const std::string file_path = "./be/test/formats/parquet/test_data/empty_null_counts_test.parquet";

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true),
                         chunk->num_columns());
    chunk->append_column(
            ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true),
            chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true),
                         chunk->num_columns());

    Utils::SlotDesc slot_descs[] = {
            {"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
            {"name", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)},
            {"value", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)},
            {""},
    };

    Utils::SlotDesc min_max_slots[] = {
            {"id", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), 0},
            {""},
    };

    auto ctx = _create_scan_context();
    auto file = _create_file(file_path);
    ctx->conjunct_ctxs_by_slot[0].clear();
    ctx->conjuncts.min_max_ctxs.clear();

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = _create_scan_range(file_path);
    ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

    std::vector<TExpr> t_conjuncts;
    // id > 2
    // this triggers page index filtering
    ParquetUTBase::append_bigint_conjunct(TExprOpcode::GT, 0, 2, &t_conjuncts);

    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->conjuncts.min_max_ctxs);
    ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->conjunct_ctxs_by_slot[0]);

    std::vector<ExprContext*> all_conjuncts;
    for (auto* expr : ctx->conjuncts.min_max_ctxs) {
        all_conjuncts.push_back(expr);
    }
    for (auto* expr : ctx->conjunct_ctxs_by_slot[0]) {
        all_conjuncts.push_back(expr);
    }
    ParquetUTBase::setup_conjuncts_manager(all_conjuncts, nullptr, tuple_desc, _runtime_state, ctx);

    auto shared_buffer = std::make_shared<io::SharedBufferedInputStream>(file->stream(), file_path,
                                                                         std::filesystem::file_size(file_path));
    auto file_reader =
            std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(file_path),
                                         DataCacheOptions(), shared_buffer.get());

    Status status = file_reader->init(ctx);
    ASSERT_TRUE(status.ok());

    size_t total_row_nums = 0;
    while (!status.is_end_of_file()) {
        chunk->reset();
        status = file_reader->get_next(&chunk);
        chunk->check_or_die();
        total_row_nums += chunk->num_rows();
    }
    EXPECT_GT(total_row_nums, 0);
}

namespace {

template <typename T>
std::string thrift_bytes(T obj) {
    ThriftSerializer ser(true, 100);
    uint32_t len = 0;
    uint8_t* buf = nullptr;
    CHECK(ser.serialize(&obj, &len, &buf).ok());
    return std::string(reinterpret_cast<char*>(buf), len);
}

std::string int_bytes(int32_t value) {
    return std::string(reinterpret_cast<const char*>(&value), sizeof(value));
}

tparquet::ColumnIndex make_column_index(size_t min_values, size_t max_values, size_t null_pages) {
    tparquet::ColumnIndex column_index;
    for (size_t i = 0; i < min_values; ++i) {
        column_index.min_values.push_back(int_bytes(static_cast<int32_t>(i * 10)));
    }
    for (size_t i = 0; i < max_values; ++i) {
        column_index.max_values.push_back(int_bytes(static_cast<int32_t>(i * 10 + 9)));
    }
    column_index.null_pages.assign(null_pages, false);
    column_index.boundary_order = tparquet::BoundaryOrder::ASCENDING;
    return column_index;
}

tparquet::PageLocation make_page_location(int64_t offset, int32_t compressed_page_size, int64_t first_row_index) {
    tparquet::PageLocation location;
    location.offset = offset;
    location.compressed_page_size = compressed_page_size;
    location.first_row_index = first_row_index;
    return location;
}

tparquet::OffsetIndex make_offset_index(const std::vector<tparquet::PageLocation>& locations) {
    tparquet::OffsetIndex offset_index;
    offset_index.page_locations = locations;
    return offset_index;
}

// An INT32 column chunk over a file in memory. The test sets the regions of the file that the chunk metadata
// points at.
class IndexEnv {
public:
    explicit IndexEnv(std::string file_bytes, bool known_file_size = true)
            : _file(std::make_shared<io::StringInputStream>(std::string(file_bytes)), "string-file") {
        _opts.stats = &_stats;
        _opts.file = &_file;
        _opts.file_size = known_file_size ? file_bytes.size() : 0;
        _field.name = "c";
        _field.physical_type = tparquet::Type::INT32;
        _field.is_nullable = true;
        _col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
        _meta_data.type = tparquet::Type::INT32;
        _meta_data.num_values = 10;
        _meta_data.codec = tparquet::CompressionCodec::UNCOMPRESSED;
        _chunk.__set_meta_data(_meta_data);
        _reader = std::make_unique<ScalarColumnReader>(&_field, &_chunk, &_col_type, _opts);
    }

    // The column index and the offset index are at the given regions of the file.
    void set_page_index(int64_t column_index_offset, int32_t column_index_length, int64_t offset_index_offset,
                        int32_t offset_index_length) {
        _chunk.__set_column_index_offset(column_index_offset);
        _chunk.__set_column_index_length(column_index_length);
        _chunk.__set_offset_index_offset(offset_index_offset);
        _chunk.__set_offset_index_length(offset_index_length);
    }

    // The row group that the chunk belongs to has num_rows rows.
    void set_row_group_rows(int64_t num_rows) {
        _row_group.num_rows = num_rows;
        _opts.row_group_meta = &_row_group;
    }

    // The column has null_count NULL values.
    void set_null_count(int64_t null_count) {
        tparquet::Statistics statistics;
        statistics.__set_null_count(null_count);
        _chunk.meta_data.__set_statistics(statistics);
    }

    StatusOr<bool> page_index_filter(uint64_t rg_num_rows) {
        SparseRange<uint64_t> row_ranges;
        return _reader->page_index_zone_map_filter({}, &row_ranges, CompoundNodeType::AND, 0, rg_num_rows);
    }

    ScalarColumnReader* reader() { return _reader.get(); }
    tparquet::ColumnChunk* chunk() { return &_chunk; }

private:
    HdfsScannerStats _stats;
    RandomAccessFile _file;
    ColumnReaderOptions _opts;
    ParquetField _field{};
    TypeDescriptor _col_type;
    tparquet::ColumnMetaData _meta_data;
    tparquet::ColumnChunk _chunk;
    tparquet::RowGroup _row_group;
    std::unique_ptr<ScalarColumnReader> _reader;
};

// A file that holds the column index, then the offset index.
struct PageIndexFile {
    std::string bytes;
    int32_t column_index_length;
    int32_t offset_index_length;
};

PageIndexFile make_page_index_file(const tparquet::ColumnIndex& column_index,
                                   const tparquet::OffsetIndex& offset_index) {
    PageIndexFile file;
    file.bytes = thrift_bytes(column_index);
    file.column_index_length = file.bytes.size();
    file.bytes += thrift_bytes(offset_index);
    file.offset_index_length = file.bytes.size() - file.column_index_length;
    // the pages that the offset index points at
    file.bytes.append(1024, '\0');
    return file;
}

std::vector<tparquet::PageLocation> two_pages() {
    return {make_page_location(100, 50, 0), make_page_location(200, 50, 5)};
}

} // namespace

// The column index and the offset index are optional and come from the footer. The reader takes the entry of
// each list by the position of the page, and sizes its buffers from the lengths in the footer, so it has to
// return an error when the lists differ in length or the regions are not in the file, and a caller goes on
// without the index.
TEST_F(PageIndexTest, ColumnIndexOfAnotherLengthThanTheOffsetIndex) {
    constexpr uint64_t kRows = 10;
    auto run = [&](const tparquet::ColumnIndex& column_index, const tparquet::OffsetIndex& offset_index) {
        PageIndexFile file = make_page_index_file(column_index, offset_index);
        IndexEnv env(file.bytes);
        env.set_page_index(0, file.column_index_length, file.column_index_length, file.offset_index_length);
        return env.page_index_filter(kRows);
    };

    // the lists have the length of the pages
    {
        auto result = run(make_column_index(2, 2, 2), make_offset_index(two_pages()));
        ASSERT_TRUE(result.ok()) << result.status().to_string();
        ASSERT_FALSE(result.value());
    }
    // max_values, null_pages and the pages of the offset index are shorter or longer than min_values
    for (const auto& [column_index, offset_index] :
         std::vector<std::pair<tparquet::ColumnIndex, tparquet::OffsetIndex>>{
                 {make_column_index(2, 1, 2), make_offset_index(two_pages())},
                 {make_column_index(2, 3, 2), make_offset_index(two_pages())},
                 {make_column_index(2, 2, 1), make_offset_index(two_pages())},
                 {make_column_index(2, 2, 3), make_offset_index(two_pages())},
                 {make_column_index(2, 2, 2), make_offset_index({make_page_location(100, 50, 0)})},
                 {make_column_index(2, 2, 2),
                  make_offset_index({make_page_location(100, 50, 0), make_page_location(200, 50, 5),
                                     make_page_location(300, 50, 7)})}}) {
        auto result = run(column_index, offset_index);
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().is_corruption()) << result.status().to_string();
    }
    // a page that starts after the last row of the row group
    {
        auto result = run(make_column_index(2, 2, 2),
                          make_offset_index({make_page_location(100, 50, 0), make_page_location(200, 50, kRows)}));
        ASSERT_TRUE(result.status().is_corruption()) << result.status().to_string();
    }
}

TEST_F(PageIndexTest, InvalidOffsetIndex) {
    const auto column_index = make_column_index(2, 2, 2);
    // the pages must be in the order of the file: the first row of the first page is 0, and the first rows
    // and the offsets grow; the offsets are in the file and the sizes are positive
    for (const auto& locations : std::vector<std::vector<tparquet::PageLocation>>{
                 {},
                 {make_page_location(100, 50, 1), make_page_location(200, 50, 5)},
                 {make_page_location(100, 50, 0), make_page_location(200, 50, 0)},
                 {make_page_location(100, 50, 5), make_page_location(200, 50, 0)},
                 {make_page_location(200, 50, 0), make_page_location(100, 50, 5)},
                 {make_page_location(-1, 50, 0), make_page_location(200, 50, 5)},
                 {make_page_location(100, 0, 0), make_page_location(200, 50, 5)},
                 {make_page_location(100, -1, 0), make_page_location(200, 50, 5)},
                 {make_page_location(100, 50, 0), make_page_location(1 << 30, 50, 5)},
                 {make_page_location(100, 50, 0), make_page_location(200, std::numeric_limits<int32_t>::max(), 5)}}) {
        PageIndexFile file = make_page_index_file(column_index, make_offset_index(locations));
        IndexEnv env(file.bytes);
        env.set_page_index(0, file.column_index_length, file.column_index_length, file.offset_index_length);

        auto result = env.page_index_filter(10);
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().is_corruption()) << result.status().to_string();

        // the failure leaves no index behind: the next call fails the same way and does not find an empty one
        auto offset_index = env.reader()->get_offset_index(0);
        ASSERT_FALSE(offset_index.ok());
        ASSERT_TRUE(env.reader()->_offset_index_ctx == nullptr);
    }
}

TEST_F(PageIndexTest, PageIndexRegionOutsideTheFile) {
    PageIndexFile file = make_page_index_file(make_column_index(2, 2, 2), make_offset_index(two_pages()));
    const int32_t file_size = file.bytes.size();
    const int32_t max_int = std::numeric_limits<int32_t>::max();

    // {column index offset, column index length, offset index offset, offset index length}
    for (const auto& region : std::vector<std::vector<int64_t>>{
                 {-1, file.column_index_length, file.column_index_length, file.offset_index_length},
                 {0, -1, file.column_index_length, file.offset_index_length},
                 {0, 0, file.column_index_length, file.offset_index_length},
                 {0, file_size + 1, file.column_index_length, file.offset_index_length},
                 {file_size + 1, 1, file.column_index_length, file.offset_index_length},
                 {0, max_int, file.column_index_length, file.offset_index_length},
                 {0, file.column_index_length, -1, file.offset_index_length},
                 {0, file.column_index_length, file.column_index_length, -1},
                 {0, file.column_index_length, file.column_index_length, 0},
                 {0, file.column_index_length, file.column_index_length, file_size},
                 {0, file.column_index_length, file.column_index_length, max_int}}) {
        // with and without the size of the file: the length that no real file has is stopped either way
        for (bool known_file_size : {true, false}) {
            IndexEnv env(file.bytes, known_file_size);
            env.set_page_index(region[0], region[1], region[2], region[3]);
            auto result = env.page_index_filter(10);
            ASSERT_FALSE(result.ok()) << region[0] << " " << region[1] << " " << region[2] << " " << region[3];
        }
    }

    // a footer without the length of the column index has no page index for this reader
    {
        IndexEnv env(file.bytes);
        env.chunk()->__set_column_index_offset(0);
        env.chunk()->__set_offset_index_offset(file.column_index_length);
        auto result = env.page_index_filter(10);
        ASSERT_TRUE(result.ok()) << result.status().to_string();
        ASSERT_FALSE(result.value());
    }
}

namespace {

// A reader of the column chunk that records what the reader with index asks of it.
class RecordingStoredColumnReader final : public StoredColumnReader {
public:
    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnContentType content_type,
                      Column* dst) override {
        read_ranges.push_back(range);
        return Status::OK();
    }
    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {}
    Status get_dict_values(Column* column) override { return Status::OK(); }
    Status get_dict_values(const Buffer<int32_t>& dict_codes, const NullableColumn& nulls, Column* column) override {
        return Status::OK();
    }
    Status load_specific_page(size_t cur_page_idx, uint64_t offset, uint64_t first_row) override {
        loaded_pages.push_back(cur_page_idx);
        return Status::OK();
    }

    std::vector<Range<uint64_t>> read_ranges;
    std::vector<size_t> loaded_pages;
};

SparseRange<uint64_t> make_range(uint64_t begin, uint64_t end) {
    SparseRange<uint64_t> range;
    range.add(Range<uint64_t>(begin, end));
    return range;
}

} // namespace

// The pages are selected once for a reader. A second call must not append the selection of the pages again, and
// must not wrap the reader a second time, because the reader with index takes its page count from the selection.
// GroupReader::prepare() calls select_offset_index() once for each reader, and no other code calls it, so this
// is a guard on the contract and not a path that a query takes today.
TEST_F(PageIndexTest, SelectOffsetIndexTwiceKeepsTheFirstSelection) {
    PageIndexFile file = make_page_index_file(make_column_index(2, 2, 2), make_offset_index(two_pages()));
    IndexEnv env(file.bytes);
    env.set_page_index(0, file.column_index_length, file.column_index_length, file.offset_index_length);
    ASSERT_TRUE(env.reader()->prepare().ok());

    auto* original = env.reader()->_reader.get();
    env.reader()->select_offset_index(make_range(0, 5), 0);
    auto* wrapper = dynamic_cast<StoredColumnReaderWithIndex*>(env.reader()->_reader.get());
    ASSERT_TRUE(wrapper != nullptr);
    ASSERT_EQ(original, wrapper->_inner_reader.get());
    ASSERT_EQ(2U, env.reader()->_offset_index_ctx->page_selected.size());
    ASSERT_TRUE(env.reader()->_offset_index_ctx->page_selected[0]);
    ASSERT_FALSE(env.reader()->_offset_index_ctx->page_selected[1]);

    // another range, selecting the other page: the first selection stays
    env.reader()->select_offset_index(make_range(5, 10), 0);
    ASSERT_TRUE(env.reader()->_reader.get() == wrapper);
    ASSERT_EQ(original, wrapper->_inner_reader.get());
    ASSERT_EQ(2U, env.reader()->_offset_index_ctx->page_selected.size());
    ASSERT_TRUE(env.reader()->_offset_index_ctx->page_selected[0]);
    ASSERT_FALSE(env.reader()->_offset_index_ctx->page_selected[1]);
    ASSERT_EQ(2U, wrapper->_page_num);
}

// The offset index must agree with the chunk: the pages cannot be more than the values of the chunk, and the last
// page cannot start after the last row of the row group. If it does not, the reader goes on without the index.
TEST_F(PageIndexTest, SelectOffsetIndexChecksThePagesAgainstTheChunk) {
    PageIndexFile file = make_page_index_file(make_column_index(2, 2, 2), make_offset_index(two_pages()));

    // {values of the chunk, rows of the row group, the index is used}
    for (const auto& [num_values, num_rows, used] : std::vector<std::tuple<int64_t, int64_t, bool>>{{10, 10, true},
                                                                                                    {2, 10, true},
                                                                                                    {1, 10, false},
                                                                                                    {0, 10, false},
                                                                                                    {10, 5, false},
                                                                                                    {10, 6, true},
                                                                                                    {10, 0, false}}) {
        IndexEnv env(file.bytes);
        env.set_page_index(0, file.column_index_length, file.column_index_length, file.offset_index_length);
        env.chunk()->meta_data.num_values = num_values;
        env.set_row_group_rows(num_rows);
        ASSERT_TRUE(env.reader()->prepare().ok());

        env.reader()->select_offset_index(make_range(0, 5), 0);
        const bool wrapped = dynamic_cast<StoredColumnReaderWithIndex*>(env.reader()->_reader.get()) != nullptr;
        ASSERT_EQ(used, wrapped) << num_values << " " << num_rows;
        // without the index, the reader reads the whole chunk
        ASSERT_EQ(used, env.reader()->_offset_index_ctx != nullptr) << num_values << " " << num_rows;
        if (!used) {
            std::vector<io::SharedBufferedInputStream::IORange> ranges;
            int64_t end_offset = 0;
            env.reader()->collect_column_io_range(&ranges, &end_offset, ColumnIOType::PAGES, true);
            ASSERT_EQ(1U, ranges.size());
        }
    }
}

// The reader with index relies on the page positions and the selection to agree with the offset index. When
// they do not, it must return an error in every build and not read the rows of another page.
TEST_F(PageIndexTest, ReaderWithIndexReturnsAnErrorWhenItsInvariantsBreak) {
    auto make_ctx = [](std::vector<bool> page_selected) {
        auto ctx = std::make_unique<ColumnOffsetIndexCtx>();
        ctx->rg_first_row = 100;
        ctx->offset_index = make_offset_index(
                {make_page_location(10, 10, 0), make_page_location(20, 10, 4), make_page_location(30, 10, 8)});
        ctx->page_selected = std::move(page_selected);
        return ctx;
    };
    auto make_reader = [](ColumnOffsetIndexCtx* ctx, RecordingStoredColumnReader** inner) {
        auto recording = std::make_unique<RecordingStoredColumnReader>();
        *inner = recording.get();
        return std::make_unique<StoredColumnReaderWithIndex>(std::move(recording), ctx, false);
    };

    // reads that go forward, over selected pages
    {
        auto ctx = make_ctx({true, false, true});
        RecordingStoredColumnReader* inner = nullptr;
        auto reader = make_reader(ctx.get(), &inner);
        ASSERT_TRUE(reader->read_range(Range<uint64_t>(100, 103), nullptr, ColumnContentType::VALUE, nullptr).ok());
        ASSERT_TRUE(reader->read_range(Range<uint64_t>(108, 110), nullptr, ColumnContentType::VALUE, nullptr).ok());
        ASSERT_EQ(2U, inner->read_ranges.size());
        ASSERT_EQ(std::vector<size_t>{2}, inner->loaded_pages);

        // a read that goes back to a page before the one that the reader is on
        Status st = reader->read_range(Range<uint64_t>(100, 103), nullptr, ColumnContentType::VALUE, nullptr);
        ASSERT_TRUE(st.is_internal_error()) << st.to_string();
        ASSERT_EQ(2U, inner->read_ranges.size());
    }
    // a read in a page that is not selected
    {
        auto ctx = make_ctx({true, false, true});
        RecordingStoredColumnReader* inner = nullptr;
        auto reader = make_reader(ctx.get(), &inner);
        Status st = reader->read_range(Range<uint64_t>(104, 106), nullptr, ColumnContentType::VALUE, nullptr);
        ASSERT_TRUE(st.is_internal_error()) << st.to_string();
        ASSERT_TRUE(inner->loaded_pages.empty());
    }
    // a selection that has another number of pages than the offset index
    for (const auto& page_selected : std::vector<std::vector<bool>>{{}, {true}, {true, true, true, true}}) {
        auto ctx = make_ctx(page_selected);
        RecordingStoredColumnReader* inner = nullptr;
        auto reader = make_reader(ctx.get(), &inner);
        Status st = reader->read_range(Range<uint64_t>(100, 103), nullptr, ColumnContentType::VALUE, nullptr);
        ASSERT_TRUE(st.is_internal_error()) << page_selected.size() << " " << st.to_string();
        ASSERT_TRUE(inner->read_ranges.empty());
    }
}

// The size of the bitset comes from the header of the bloom filter, and the filter reads whole blocks of 32
// bytes. A size that no real file has, or that is larger than the bytes that the file gives to the filter, must
// be an error, and the caller goes on without the filter.
class BloomFilterRegionTest : public testing::Test {
protected:
    // A header field that is not set stands for a kind that this version of the format does not have: the reader
    // of the header skips a field that it does not know, and the union has no field set.
    static std::string make_bloom_filter(int32_t num_bytes, size_t bitset_bytes, size_t trailing_bytes = 0,
                                         bool set_algorithm = true, bool set_hash = true, bool set_compression = true) {
        tparquet::BloomFilterHeader header;
        header.numBytes = num_bytes;
        if (set_algorithm) header.algorithm.__set_BLOCK(tparquet::SplitBlockAlgorithm());
        if (set_hash) header.hash.__set_XXHASH(tparquet::XxHash());
        if (set_compression) header.compression.__set_UNCOMPRESSED(tparquet::Uncompressed());
        std::string bytes = thrift_bytes(header);
        bytes.append(bitset_bytes, '\0');
        bytes.append(trailing_bytes, '\x7f');
        return bytes;
    }
};

TEST_F(BloomFilterRegionTest, ValidFilter) {
    const std::string bytes = make_bloom_filter(64, 64);
    for (int32_t length : {static_cast<int32_t>(bytes.size()), 0}) {
        IndexEnv env(bytes);
        env.set_null_count(3);
        ParquetBlockSplitBloomFilter bloom_filter;
        Status st = env.reader()->_init_column_bloom_filter(0, length, bloom_filter);
        ASSERT_TRUE(st.ok()) << length << " " << st.to_string();
        ASSERT_TRUE(bloom_filter.has_null());
        ASSERT_FALSE(bloom_filter.test_bytes("a", 1));
    }
}

// The flag of NULL values is the byte after the bitset, whatever the file holds there.
TEST_F(BloomFilterRegionTest, NullFlagIsTheByteAfterTheBitset) {
    const std::string bytes = make_bloom_filter(64, 64, /*trailing_bytes=*/8);
    for (int64_t null_count : {0, 3}) {
        IndexEnv env(bytes);
        env.set_null_count(null_count);
        ParquetBlockSplitBloomFilter bloom_filter;
        ASSERT_TRUE(env.reader()->_init_column_bloom_filter(0, bytes.size(), bloom_filter).ok());
        ASSERT_EQ(null_count > 0, bloom_filter.has_null());
    }
}

// A filter that is not a split block, xxHash, uncompressed filter would be probed with the wrong hash or layout and
// answer "not present" for values that are present, so the reader must not use it.
TEST_F(BloomFilterRegionTest, UnsupportedKindIsNotUsed) {
    for (const auto& [set_algorithm, set_hash, set_compression] : std::vector<std::tuple<bool, bool, bool>>{
                 {false, true, true}, {true, false, true}, {true, true, false}, {false, false, false}}) {
        const std::string bytes = make_bloom_filter(64, 64, 0, set_algorithm, set_hash, set_compression);
        for (int32_t length : {static_cast<int32_t>(bytes.size()), 0}) {
            IndexEnv env(bytes);
            ParquetBlockSplitBloomFilter bloom_filter;
            Status st = env.reader()->_init_column_bloom_filter(0, length, bloom_filter);
            ASSERT_TRUE(st.is_not_supported())
                    << set_algorithm << set_hash << set_compression << " " << length << " " << st.to_string();
        }
    }
}

TEST_F(BloomFilterRegionTest, InvalidSize) {
    for (int32_t num_bytes :
         {0, 1, 31, 33, 63, -1, std::numeric_limits<int32_t>::min(),
          static_cast<int32_t>(BloomFilter::MAXIMUM_BYTES) + 32, std::numeric_limits<int32_t>::max()}) {
        const std::string bytes = make_bloom_filter(num_bytes, 64);
        for (int32_t length : {static_cast<int32_t>(bytes.size()), 0}) {
            IndexEnv env(bytes);
            ParquetBlockSplitBloomFilter bloom_filter;
            Status st = env.reader()->_init_column_bloom_filter(0, length, bloom_filter);
            ASSERT_TRUE(st.is_corruption()) << num_bytes << " " << length << " " << st.to_string();
        }
    }
}

TEST_F(BloomFilterRegionTest, BitsetLongerThanTheBytesOfTheFilter) {
    // the header says 64 bytes, and the file has 40
    const std::string bytes = make_bloom_filter(64, 40);
    for (int32_t length : {static_cast<int32_t>(bytes.size()), 0}) {
        IndexEnv env(bytes);
        ParquetBlockSplitBloomFilter bloom_filter;
        Status st = env.reader()->_init_column_bloom_filter(0, length, bloom_filter);
        ASSERT_FALSE(st.ok()) << length;
    }
    // the region is shorter than a header
    {
        IndexEnv env(make_bloom_filter(64, 64));
        ParquetBlockSplitBloomFilter bloom_filter;
        ASSERT_FALSE(env.reader()->_init_column_bloom_filter(0, 5, bloom_filter).ok());
    }
}

TEST_F(BloomFilterRegionTest, RegionOutsideTheFile) {
    const std::string bytes = make_bloom_filter(64, 64);
    const int32_t size = bytes.size();
    // {offset, length}
    for (const auto& [offset, length] : std::vector<std::pair<int64_t, int32_t>>{
                 {-1, size}, {0, -1}, {0, size + 1}, {size, 0}, {size + 100, 10}, {int64_t{1} << 40, 0}}) {
        IndexEnv env(bytes);
        ParquetBlockSplitBloomFilter bloom_filter;
        Status st = env.reader()->_init_column_bloom_filter(offset, length, bloom_filter);
        ASSERT_TRUE(st.is_corruption()) << offset << " " << length << " " << st.to_string();
    }
    // An offset above the range of int32 is read as it is. The file holds the filter at offset 0, so a reader
    // that cut the offset to 32 bits would find it there.
    {
        IndexEnv env(bytes, /*known_file_size=*/false);
        ParquetBlockSplitBloomFilter bloom_filter;
        ASSERT_TRUE(env.reader()->_init_column_bloom_filter(0, size, bloom_filter).ok());
        ParquetBlockSplitBloomFilter far_bloom_filter;
        ASSERT_FALSE(env.reader()->_init_column_bloom_filter(int64_t{1} << 32, size, far_bloom_filter).ok());
    }
}

} // namespace starrocks::parquet
