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

#include "cache/datacache.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "column/column_helper.h"
#include "common/config.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "exprs/binary_predicate.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/group_reader.h"
#include "formats/parquet/offset_index_skip_advisor.h"
#include "formats/parquet/parquet_block_split_bloom_filter.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "formats/parquet/parquet_ut_base.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/stored_column_reader_with_index.h"
#include "fs/fs.h"
#include "io/cache_input_stream.h"
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
        init(known_file_size ? file_bytes.size() : 0);
    }

    // The file is read through the given stream.
    IndexEnv(std::shared_ptr<io::SeekableInputStream> stream, size_t file_size)
            : _file(std::move(stream), "string-file") {
        init(file_size);
    }

    void init(size_t file_size) {
        _opts.stats = &_stats;
        _opts.file = &_file;
        _opts.file_size = file_size;
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

    // The column is nested in a list, so it has repetition levels.
    void set_max_rep_level(int16_t max_rep_level) { _field.level_info.max_rep_level = max_rep_level; }

    // The feedback of the scan on the offset indexes, which must outlive the reader.
    void set_skip_advisor(OffsetIndexSkipAdvisor* advisor) { _opts.offset_index_skip_advisor = advisor; }

    HdfsScannerStats& stats() { return _stats; }

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
    bool supports_offset_index_skip() const override { return supports_skip; }
    int64_t offset_index_skipped_pages() const override { return skipped_pages; }
    void set_offset_index_skip(const ColumnOffsetIndexCtx* ctx, bool has_dict_page) override {
        ++skip_calls;
        skip_ctx = ctx;
        skip_has_dict_page = has_dict_page;
    }
    void set_page_locations(const std::vector<tparquet::PageLocation>* locations) override {
        ++page_locations_calls;
        page_locations = locations;
    }

    std::vector<Range<uint64_t>> read_ranges;
    int page_locations_calls = 0;
    const std::vector<tparquet::PageLocation>* page_locations = nullptr;
    std::vector<size_t> loaded_pages;
    bool supports_skip = true;
    int64_t skipped_pages = 0;
    int skip_calls = 0;
    const ColumnOffsetIndexCtx* skip_ctx = nullptr;
    bool skip_has_dict_page = false;
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

namespace {

// The pages of a chunk of 10 rows: [100, 150) and [150, 200), one after the other.
tparquet::PageEncodingStats make_encoding_stats(tparquet::PageType::type page_type, int32_t count) {
    tparquet::PageEncodingStats stats;
    stats.page_type = page_type;
    stats.encoding = tparquet::Encoding::PLAIN;
    stats.count = count;
    return stats;
}

std::vector<tparquet::PageLocation> contiguous_pages() {
    return {make_page_location(100, 50, 0), make_page_location(150, 50, 5)};
}

// A column whose chunk is made of the two pages of contiguous_pages() and 10 rows, with a stored reader that records
// what the raw reader asks of it.
class SkipIndexEnv {
public:
    explicit SkipIndexEnv(const tparquet::OffsetIndex& offset_index = make_offset_index(contiguous_pages()))
            : _file(make_page_index_file(make_column_index(2, 2, 2), offset_index)), _env(_file.bytes) {
        _env.set_page_index(0, _file.column_index_length, _file.column_index_length, _file.offset_index_length);
        _env.set_row_group_rows(10);
        _env.chunk()->meta_data.data_page_offset = 100;
        _env.chunk()->meta_data.total_compressed_size = 100;
        auto recording = std::make_unique<RecordingStoredColumnReader>();
        _inner = recording.get();
        _env.reader()->_reader = std::move(recording);
    }

    Status read(uint64_t begin, uint64_t end, const Filter* filter) {
        return _env.reader()->_read_stored_range(Range<uint64_t>(begin, end), filter, ColumnContentType::VALUE,
                                                 nullptr);
    }

    // True if the raw reader has handed the offset index to the stored reader.
    bool enabled() const { return _inner->skip_calls > 0; }
    bool loaded_index() { return _env.reader()->_offset_index_ctx != nullptr; }

    // The ranges that the column plans for the offset index alone.
    std::vector<io::SharedBufferedInputStream::IORange> planned_offset_index() {
        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        _env.reader()->collect_column_io_range(&ranges, &end_offset, ColumnIOType::OFFSET_INDEX, true);
        return ranges;
    }

    IndexEnv& env() { return _env; }
    HdfsScannerStats& stats() { return _env.stats(); }
    RecordingStoredColumnReader* inner() { return _inner; }

private:
    PageIndexFile _file;
    IndexEnv _env;
    RecordingStoredColumnReader* _inner = nullptr;
};

class ScopedOffsetIndexSkipConfig {
public:
    explicit ScopedOffsetIndexSkipConfig(bool value) : _old_value(config::parquet_offset_index_skip_enable) {
        config::parquet_offset_index_skip_enable = value;
    }
    ~ScopedOffsetIndexSkipConfig() { config::parquet_offset_index_skip_enable = _old_value; }

private:
    bool _old_value;
};

} // namespace

// The offset index is loaded for a read where the stored reader can use it: some rows of the range are dropped, or the
// range does not follow the last one. A column that never has such a read does not load it. The reader gets the index
// once, with the flag of the dictionary page.
TEST_F(PageIndexTest, OffsetIndexSkipIsEnabledAtTheFirstUsefulRead) {
    Filter with_zero(10, 1);
    with_zero[3] = 0;
    Filter all_ones(10, 1);

    // a filter that keeps every row, in a range that follows the first row of the group, and reads without a filter
    {
        SkipIndexEnv env;
        ASSERT_TRUE(env.read(0, 10, &all_ones).ok());
        ASSERT_TRUE(env.read(10, 20, nullptr).ok());
        ASSERT_TRUE(env.read(30, 40, nullptr).ok());
        ASSERT_FALSE(env.enabled());
        ASSERT_FALSE(env.loaded_index());
        ASSERT_EQ(3U, env.inner()->read_ranges.size());
    }
    // a filter that drops a row
    {
        SkipIndexEnv env;
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.enabled());
        ASSERT_EQ(env.env().reader()->_offset_index_ctx.get(), env.inner()->skip_ctx);
        ASSERT_FALSE(env.inner()->skip_has_dict_page);
        ASSERT_EQ(2U, env.inner()->skip_ctx->offset_index.page_locations.size());
        // the next reads do not hand it over again
        ASSERT_TRUE(env.read(10, 20, &with_zero).ok());
        ASSERT_EQ(1, env.inner()->skip_calls);
        ASSERT_EQ(2U, env.inner()->read_ranges.size());
    }
    // a gap between two ranges, with a filter that keeps every row
    {
        SkipIndexEnv env;
        Filter five_ones(5, 1);
        ASSERT_TRUE(env.read(0, 5, &five_ones).ok());
        ASSERT_FALSE(env.enabled());
        ASSERT_TRUE(env.read(5, 10, &five_ones).ok());
        ASSERT_FALSE(env.enabled());
        ASSERT_TRUE(env.read(20, 25, &five_ones).ok());
        ASSERT_TRUE(env.enabled());
    }
    // the first range of the group does not start at the first row
    {
        SkipIndexEnv env;
        Filter five_ones(5, 1);
        ASSERT_TRUE(env.read(3, 8, &five_ones).ok());
        ASSERT_TRUE(env.enabled());
    }
    // the chunk starts with a dictionary page: the bytes before the first page
    {
        SkipIndexEnv env;
        env.env().chunk()->meta_data.__set_dictionary_page_offset(60);
        env.env().chunk()->meta_data.total_compressed_size = 140;
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.enabled());
        ASSERT_TRUE(env.inner()->skip_has_dict_page);
    }
    // the footer agrees with the index: the encodings and the page counts of the encoding statistics
    for (bool dict : {false, true}) {
        SkipIndexEnv env;
        auto& meta = env.env().chunk()->meta_data;
        if (dict) {
            meta.__set_dictionary_page_offset(60);
            meta.total_compressed_size = 140;
        }
        meta.__set_encodings(
                dict ? std::vector<tparquet::Encoding::type>{tparquet::Encoding::PLAIN,
                                                             tparquet::Encoding::RLE_DICTIONARY}
                     : std::vector<tparquet::Encoding::type>{tparquet::Encoding::PLAIN, tparquet::Encoding::RLE});
        std::vector<tparquet::PageEncodingStats> stats;
        stats.push_back(make_encoding_stats(tparquet::PageType::DATA_PAGE, 1));
        stats.push_back(make_encoding_stats(tparquet::PageType::DATA_PAGE_V2, 1));
        if (dict) {
            stats.push_back(make_encoding_stats(tparquet::PageType::DICTIONARY_PAGE, 1));
        }
        meta.__set_encoding_stats(stats);
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.enabled()) << dict;
        ASSERT_EQ(dict, env.inner()->skip_has_dict_page);
    }
}

// The offset index is optional, and a column that cannot use it is read page by page without an error.
TEST_F(PageIndexTest, OffsetIndexSkipFallsBackWithoutAnError) {
    Filter with_zero(10, 1);
    with_zero[3] = 0;

    // Reads the column twice, and checks that the stored reader did not get the index and the reads were not refused.
    auto not_enabled = [&](SkipIndexEnv& env, bool loads_index) {
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.read(10, 20, &with_zero).ok());
        ASSERT_FALSE(env.enabled());
        ASSERT_EQ(loads_index, env.loaded_index());
        // the reads reached the stored reader
        ASSERT_EQ(2U, env.inner()->read_ranges.size());
        // and the column does not try again
        ASSERT_TRUE(env.env().reader()->_offset_index_skip_tried);
    };

    // the stored reader reads page by page only, like a Repeated reader
    {
        SkipIndexEnv env;
        env.inner()->supports_skip = false;
        ASSERT_NO_FATAL_FAILURE(not_enabled(env, false));
    }
    // the chunk has no offset index
    {
        SkipIndexEnv env;
        env.env().chunk()->__isset.offset_index_offset = false;
        ASSERT_NO_FATAL_FAILURE(not_enabled(env, false));
    }
    // an offset index that is not valid: the first rows of the pages do not grow
    {
        SkipIndexEnv env(make_offset_index({make_page_location(100, 50, 0), make_page_location(150, 50, 0)}));
        ASSERT_NO_FATAL_FAILURE(not_enabled(env, false));
    }
    // An offset index that does not fit the chunk: the first page is before its start, or the last page ends after
    // its end or before it.
    for (const auto& [data_page_offset, total_compressed_size] :
         std::vector<std::pair<int64_t, int64_t>>{{101, 99}, {99, 100}, {100, 99}, {100, 101}, {100, 50}}) {
        SkipIndexEnv env;
        env.env().chunk()->meta_data.data_page_offset = data_page_offset;
        env.env().chunk()->meta_data.total_compressed_size = total_compressed_size;
        ASSERT_NO_FATAL_FAILURE(not_enabled(env, true));
    }
    // The pages overlap, or leave a gap: the reader reads a chunk page after page, so the index does not describe it.
    for (const auto& pages : std::vector<std::vector<tparquet::PageLocation>>{
                 {make_page_location(100, 120, 0), make_page_location(150, 50, 5)},
                 {make_page_location(100, 40, 0), make_page_location(150, 50, 5)}}) {
        SkipIndexEnv env(make_offset_index(pages));
        ASSERT_NO_FATAL_FAILURE(not_enabled(env, true));
    }
    // a non-repeated column has one value for each row
    {
        SkipIndexEnv env;
        env.env().chunk()->meta_data.num_values = 9;
        ASSERT_NO_FATAL_FAILURE(not_enabled(env, true));
    }
    // the footer names a dictionary page, but the first page of the index starts at the chunk
    {
        SkipIndexEnv env;
        env.env().chunk()->meta_data.__set_dictionary_page_offset(100);
        ASSERT_NO_FATAL_FAILURE(not_enabled(env, true));
    }
    // the encodings of the chunk and the room for a dictionary page disagree, in both directions
    {
        SkipIndexEnv env;
        env.env().chunk()->meta_data.__set_encodings({tparquet::Encoding::PLAIN, tparquet::Encoding::RLE_DICTIONARY});
        ASSERT_NO_FATAL_FAILURE(not_enabled(env, true));
    }
    {
        SkipIndexEnv env;
        env.env().chunk()->meta_data.__set_dictionary_page_offset(60);
        env.env().chunk()->meta_data.total_compressed_size = 140;
        env.env().chunk()->meta_data.__set_encodings({tparquet::Encoding::PLAIN});
        ASSERT_NO_FATAL_FAILURE(not_enabled(env, true));
    }
    // The page counts of the encoding statistics are not the pages of the index. A chunk of one data page has no page
    // to skip, so the index is not even loaded for it.
    for (const auto& [data_pages, dict_pages] : std::vector<std::pair<int32_t, int32_t>>{{3, 0}, {1, 0}, {2, 1}}) {
        SkipIndexEnv env;
        std::vector<tparquet::PageEncodingStats> stats;
        stats.push_back(make_encoding_stats(tparquet::PageType::DATA_PAGE, data_pages));
        if (dict_pages > 0) {
            stats.push_back(make_encoding_stats(tparquet::PageType::DICTIONARY_PAGE, dict_pages));
        }
        env.env().chunk()->meta_data.__set_encoding_stats(stats);
        ASSERT_NO_FATAL_FAILURE(not_enabled(env, data_pages > 1));
    }
    // the config is off: the column does not even try
    {
        ScopedOffsetIndexSkipConfig config_guard(false);
        SkipIndexEnv env;
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_FALSE(env.enabled());
        ASSERT_FALSE(env.loaded_index());
        ASSERT_FALSE(env.env().reader()->_offset_index_skip_tried);
    }
}

namespace {

class ScopedGiveUpRowGroups {
public:
    explicit ScopedGiveUpRowGroups(int32_t value) : _old_value(config::parquet_offset_index_skip_give_up_row_groups) {
        config::parquet_offset_index_skip_give_up_row_groups = value;
    }
    ~ScopedGiveUpRowGroups() { config::parquet_offset_index_skip_give_up_row_groups = _old_value; }

private:
    int32_t _old_value;
};

// A column "a.b" like SkipIndexEnv has, with the feedback of a scan.
class FeedbackSkipIndexEnv : public SkipIndexEnv {
public:
    explicit FeedbackSkipIndexEnv(OffsetIndexSkipAdvisor* advisor, const std::string& column = "a") : SkipIndexEnv() {
        env().chunk()->meta_data.path_in_schema = {column, "b"};
        env().set_skip_advisor(advisor);
    }
};

} // namespace

// A chunk of at most one data page has no page to skip, so its offset index is not planned and not loaded. The
// encoding statistics tell the number of data pages; a footer without them, or with an empty list, says nothing, and
// the column goes on to load the index.
TEST_F(PageIndexTest, OffsetIndexIsNotLoadedForAChunkOfOneDataPage) {
    Filter with_zero(10, 1);
    with_zero[3] = 0;

    for (const auto& [stats, loads] : std::vector<std::pair<std::vector<tparquet::PageEncodingStats>, bool>>{
                 {{make_encoding_stats(tparquet::PageType::DATA_PAGE, 1)}, false},
                 {{make_encoding_stats(tparquet::PageType::DATA_PAGE_V2, 1)}, false},
                 {{make_encoding_stats(tparquet::PageType::DATA_PAGE, 2)}, true},
                 {{make_encoding_stats(tparquet::PageType::DATA_PAGE, 1),
                   make_encoding_stats(tparquet::PageType::DATA_PAGE_V2, 1)},
                  true},
                 {{make_encoding_stats(tparquet::PageType::DICTIONARY_PAGE, 1)}, false},
                 {{}, true}}) {
        SkipIndexEnv env;
        env.env().chunk()->meta_data.__set_encoding_stats(stats);
        ASSERT_EQ(loads ? 1U : 0U, env.planned_offset_index().size());
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_EQ(loads, env.loaded_index()) << stats.size();
        ASSERT_EQ(loads ? 1 : 0, env.stats().offset_index_skip_loads);
        ASSERT_EQ(0, env.stats().offset_index_skip_given_up);
        ASSERT_EQ(1U, env.inner()->read_ranges.size());
    }
    // without the statistics
    {
        SkipIndexEnv env;
        ASSERT_EQ(1U, env.planned_offset_index().size());
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.enabled());
    }
}

// We fear a column whose rows are not clustered, where no read can jump over a page, so every row group of the scan
// reads its offset index for nothing. A column that read the index in the first row groups of the scan and skipped no
// page stops reading it, and the scan counts the loads and the columns that gave up.
TEST_F(PageIndexTest, OffsetIndexIsNotLoadedAfterTheColumnSkippedNoPageInTheFirstRowGroups) {
    ScopedGiveUpRowGroups guard(2);
    Filter with_zero(10, 1);
    with_zero[3] = 0;
    OffsetIndexSkipAdvisor advisor;

    // The first row groups read the index, and the reader skips no page.
    for (int i = 0; i < 2; ++i) {
        FeedbackSkipIndexEnv env(&advisor);
        ASSERT_EQ(1U, env.planned_offset_index().size());
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.enabled());
        ASSERT_EQ(1, env.stats().offset_index_skip_loads);
        ASSERT_EQ(0, env.stats().offset_index_skip_given_up);
        ASSERT_EQ("a.b", env.env().reader()->_skip_column);
    }
    // the readers of the row groups are gone, and the scan has the feedback
    EXPECT_FALSE(advisor.should_load("a.b"));

    // The next row group does not plan the index, does not load it, and reads page by page without an error.
    {
        FeedbackSkipIndexEnv env(&advisor);
        ASSERT_TRUE(env.planned_offset_index().empty());
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.read(10, 20, &with_zero).ok());
        ASSERT_FALSE(env.enabled());
        ASSERT_FALSE(env.loaded_index());
        ASSERT_EQ(2U, env.inner()->read_ranges.size());
        ASSERT_EQ(0, env.stats().offset_index_skip_loads);
        ASSERT_EQ(1, env.stats().offset_index_skip_given_up);
        // the reader has nothing to report
        ASSERT_TRUE(env.env().reader()->_skip_advisor == nullptr);
    }
    // another column of the scan has its own feedback
    {
        FeedbackSkipIndexEnv env(&advisor, "other");
        ASSERT_EQ(1U, env.planned_offset_index().size());
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.enabled());
    }
    // and so has another scan
    {
        OffsetIndexSkipAdvisor other_scan;
        FeedbackSkipIndexEnv env(&other_scan);
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.enabled());
    }
    // the config can switch the giving up off
    {
        ScopedGiveUpRowGroups never(0);
        FeedbackSkipIndexEnv env(&advisor);
        ASSERT_EQ(1U, env.planned_offset_index().size());
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.enabled());
    }
}

// A column that skipped a page in a row group has shown that the index pays, so it is never given up, however many
// row groups skipped nothing after that.
TEST_F(PageIndexTest, OffsetIndexIsLoadedForEveryRowGroupOfAColumnThatSkippedAPage) {
    ScopedGiveUpRowGroups guard(2);
    Filter with_zero(10, 1);
    with_zero[3] = 0;
    OffsetIndexSkipAdvisor advisor;

    for (int i = 0; i < 10; ++i) {
        FeedbackSkipIndexEnv env(&advisor);
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok()) << i;
        ASSERT_TRUE(env.enabled()) << i;
        ASSERT_EQ(1, env.stats().offset_index_skip_loads) << i;
        // the first row group skips a page, the other ones do not
        env.inner()->skipped_pages = i == 0 ? 1 : 0;
    }
    EXPECT_TRUE(advisor.should_load("a.b"));
}

// An index that the reader loads and cannot use, because it does not fit the chunk, costs the read like any other, so
// it counts as a row group where no page was skipped. The column that has the bad index in every row group gives up.
TEST_F(PageIndexTest, OffsetIndexThatCannotBeUsedCountsAsAReadThatSkippedNoPage) {
    ScopedGiveUpRowGroups guard(2);
    Filter with_zero(10, 1);
    with_zero[3] = 0;
    OffsetIndexSkipAdvisor advisor;

    for (int i = 0; i < 2; ++i) {
        FeedbackSkipIndexEnv env(&advisor);
        env.env().chunk()->meta_data.num_values = 9;
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.loaded_index());
        ASSERT_FALSE(env.enabled());
        ASSERT_EQ(1, env.stats().offset_index_skip_loads);
    }
    EXPECT_FALSE(advisor.should_load("a.b"));
}

// The static page selection reads the offset index whatever the skip does, so an index that it has read is free for
// the skip: the column does not give it up, and does not count it.
TEST_F(PageIndexTest, OffsetIndexThatTheStaticSelectionReadIsNotPartOfTheFeedback) {
    ScopedGiveUpRowGroups guard(1);
    Filter with_zero(10, 1);
    with_zero[3] = 0;
    OffsetIndexSkipAdvisor advisor;
    advisor.observe("a.b", 0);
    ASSERT_FALSE(advisor.should_load("a.b"));

    {
        FeedbackSkipIndexEnv env(&advisor);
        ASSERT_TRUE(env.env().reader()->get_offset_index(0).ok());
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.enabled());
        ASSERT_EQ(0, env.stats().offset_index_skip_loads);
        ASSERT_EQ(0, env.stats().offset_index_skip_given_up);
        ASSERT_TRUE(env.env().reader()->_skip_advisor == nullptr);
    }
    // the feedback has not changed: one row group without a skipped page
    ASSERT_FALSE(advisor.should_load("a.b"));
    advisor.observe("a.b", 1);
    ASSERT_TRUE(advisor.should_load("a.b"));
}

// A scan that does not read the column indexes still reads the offset indexes to skip pages, and plans them alone. The
// offset index of a column that has repetition levels is not read for that, because it is read page by page.
TEST_F(PageIndexTest, OffsetIndexIsPlannedAlone) {
    PageIndexFile file = make_page_index_file(make_column_index(2, 2, 2), make_offset_index(two_pages()));
    auto collect = [&](IndexEnv& env, ColumnIOTypeFlags types, bool active) {
        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        env.reader()->collect_column_io_range(&ranges, &end_offset, types, active);
        return ranges;
    };
    auto set_regions = [&](IndexEnv& env) {
        env.set_page_index(0, file.column_index_length, file.column_index_length, file.offset_index_length);
    };

    IndexEnv env(file.bytes);
    set_regions(env);
    // the page index: the column index of an active column, and the offset index of every column
    ASSERT_EQ(2U, collect(env, ColumnIOType::PAGE_INDEX, true).size());
    ASSERT_EQ(1U, collect(env, ColumnIOType::PAGE_INDEX, false).size());
    // the offset index alone, for an active and for a lazy column
    for (bool active : {true, false}) {
        auto ranges = collect(env, ColumnIOType::OFFSET_INDEX, active);
        ASSERT_EQ(1U, ranges.size());
        ASSERT_EQ(file.column_index_length, ranges[0].offset);
        ASSERT_EQ(file.offset_index_length, ranges[0].size);
    }
    // the two flags together do not plan the offset index twice, because overlapping ranges are an error
    ASSERT_EQ(2U, collect(env, ColumnIOType::PAGE_INDEX | ColumnIOType::OFFSET_INDEX, true).size());
    // the pages are not planned by the flag
    ASSERT_TRUE(collect(env, ColumnIOType::INVALID, true).empty());

    // a column with repetition levels
    {
        IndexEnv repeated(file.bytes);
        set_regions(repeated);
        repeated.set_max_rep_level(1);
        ASSERT_TRUE(collect(repeated, ColumnIOType::OFFSET_INDEX, true).empty());
        ASSERT_EQ(1U, collect(repeated, ColumnIOType::PAGE_INDEX, false).size());
    }
    // a chunk without an offset index, and an offset index outside the file
    {
        IndexEnv no_index(file.bytes);
        ASSERT_TRUE(collect(no_index, ColumnIOType::OFFSET_INDEX, true).empty());
        IndexEnv outside(file.bytes);
        outside.set_page_index(0, file.column_index_length, file.bytes.size(), file.offset_index_length);
        ASSERT_TRUE(collect(outside, ColumnIOType::OFFSET_INDEX, true).empty());
    }
}

namespace {

// A shared stream over the file of the test, with the offset index of the file planned as an io range.
struct PlannedOffsetIndexStream {
    explicit PlannedOffsetIndexStream(const PageIndexFile& file, bool planned = true)
            : stream(std::make_shared<io::StringInputStream>(std::string(file.bytes)), "string-file",
                     file.bytes.size()) {
        if (planned) {
            std::vector<io::SharedBufferedInputStream::IORange> ranges;
            ranges.emplace_back(file.column_index_length, file.offset_index_length);
            CHECK(stream.set_io_ranges(ranges).ok());
        }
    }
    io::SharedBufferedInputStream stream;
};

} // namespace

// The static page selection reads the offset index for its own use, so the index is free for the skip. The page cache
// planner reads it for a plan that says nothing about the skip, so a column whose index the planner read keeps the
// advice and the feedback of the scan: it gives up when it skipped no page in the first row groups, it counts no load
// of the skip, and it reports its row group at the end.
TEST_F(PageIndexTest, OffsetIndexThatThePageCachePlannerReadIsPartOfTheFeedback) {
    ScopedGiveUpRowGroups guard(1);
    Filter with_zero(10, 1);
    with_zero[3] = 0;
    PageIndexFile file = make_page_index_file(make_column_index(2, 2, 2), make_offset_index(contiguous_pages()));
    PlannedOffsetIndexStream planned(file);
    OffsetIndexSkipAdvisor advisor;

    // the planner reads the index, and the skip uses it and reports that it skipped no page
    {
        FeedbackSkipIndexEnv env(&advisor);
        ASSERT_TRUE(env.env().reader()->_page_cache_io_spans(true, &planned.stream) != nullptr);
        ASSERT_EQ(1, env.stats().page_cache_io_index_loads);
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.enabled());
        ASSERT_EQ(0, env.stats().offset_index_skip_loads);
        ASSERT_EQ(0, env.stats().offset_index_skip_given_up);
        ASSERT_TRUE(env.env().reader()->_skip_advisor == &advisor);
        ASSERT_EQ("a.b", env.env().reader()->_skip_column);
    }
    ASSERT_FALSE(advisor.should_load("a.b"));

    // the column has given up: the index is in hand, but the skip does not use it, and the scan counts that
    {
        FeedbackSkipIndexEnv env(&advisor);
        ASSERT_TRUE(env.env().reader()->_page_cache_io_spans(true, &planned.stream) != nullptr);
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_FALSE(env.enabled());
        ASSERT_EQ(0, env.stats().offset_index_skip_loads);
        ASSERT_EQ(1, env.stats().offset_index_skip_given_up);
        ASSERT_TRUE(env.env().reader()->_skip_advisor == nullptr);
    }

    // a column that skipped a page is not given up, whoever read its index
    OffsetIndexSkipAdvisor paying;
    {
        FeedbackSkipIndexEnv env(&paying);
        ASSERT_TRUE(env.env().reader()->_page_cache_io_spans(true, &planned.stream) != nullptr);
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.enabled());
        env.inner()->skipped_pages = 1;
    }
    ASSERT_TRUE(paying.should_load("a.b"));

    // an index that the selection read is still free, even when the planner is asked about the pages later
    OffsetIndexSkipAdvisor selected;
    selected.observe("a.b", 0);
    {
        FeedbackSkipIndexEnv env(&selected);
        ASSERT_TRUE(env.env().reader()->get_offset_index(0).ok());
        ASSERT_TRUE(env.env().reader()->_page_cache_io_spans(false, nullptr) != nullptr);
        ASSERT_EQ(0, env.stats().page_cache_io_index_loads);
        ASSERT_TRUE(env.read(0, 10, &with_zero).ok());
        ASSERT_TRUE(env.enabled());
        ASSERT_TRUE(env.env().reader()->_skip_advisor == nullptr);
    }
}

// The planner of the page cache aware IO asks a column for the pages of its chunk, and the pages come from the offset
// index. A column that holds the index answers from it. A column that does not hold it reads it only when the planner
// allows that and the index is planned in the shared stream: a read of an index that is not planned is a remote read of
// its own. The answer of a column does not change after the first one, so the index is read once.
TEST_F(PageIndexTest, PageCacheIoSpansComeFromTheOffsetIndexOfTheChunk) {
    PageIndexFile file = make_page_index_file(make_column_index(2, 2, 2), make_offset_index(contiguous_pages()));
    auto make_env = [&](bool with_dict) {
        auto env = std::make_unique<IndexEnv>(file.bytes);
        env->set_page_index(0, file.column_index_length, file.column_index_length, file.offset_index_length);
        env->set_row_group_rows(10);
        env->chunk()->meta_data.data_page_offset = 100;
        env->chunk()->meta_data.total_compressed_size = 100;
        if (with_dict) {
            env->chunk()->meta_data.__set_dictionary_page_offset(60);
            env->chunk()->meta_data.total_compressed_size = 140;
        }
        return env;
    };
    auto expect_spans = [](const std::vector<PageSpan>* spans, const std::vector<std::pair<int64_t, int64_t>>& want) {
        ASSERT_TRUE(spans != nullptr);
        ASSERT_EQ(want.size(), spans->size());
        for (size_t i = 0; i < want.size(); ++i) {
            ASSERT_EQ(want[i].first, (*spans)[i].offset) << i;
            ASSERT_EQ(want[i].second, (*spans)[i].size) << i;
        }
    };

    PlannedOffsetIndexStream planned(file);
    PlannedOffsetIndexStream not_planned(file, false);
    {
        auto env = make_env(false);
        auto recording = std::make_unique<RecordingStoredColumnReader>();
        RecordingStoredColumnReader* inner = recording.get();
        env->reader()->_reader = std::move(recording);

        // the column holds no index, and the planner does not allow a read, or the index is not planned
        ASSERT_TRUE(env->reader()->_page_cache_io_spans(false, &planned.stream) == nullptr);
        ASSERT_TRUE(env->reader()->_page_cache_io_spans(true, nullptr) == nullptr);
        ASSERT_TRUE(env->reader()->_page_cache_io_spans(true, &not_planned.stream) == nullptr);
        ASSERT_EQ(0, env->stats().page_cache_io_index_loads);
        ASSERT_TRUE(env->reader()->_offset_index_ctx == nullptr);
        ASSERT_EQ(0, inner->page_locations_calls);

        // the index is planned: the column reads it once
        auto* spans = env->reader()->_page_cache_io_spans(true, &planned.stream);
        ASSERT_NO_FATAL_FAILURE(expect_spans(spans, {{100, 50}, {150, 50}}));
        ASSERT_EQ(1, env->stats().page_cache_io_index_loads);
        // the page reader of the chunk gets the sizes of the pages, so a header read stays inside its page
        ASSERT_EQ(1, inner->page_locations_calls);
        ASSERT_EQ(&env->reader()->_offset_index_ctx->offset_index.page_locations, inner->page_locations);
        ASSERT_EQ(spans, env->reader()->_page_cache_io_spans(false, nullptr));
        ASSERT_EQ(spans, env->reader()->_page_cache_io_spans(true, &planned.stream));
        ASSERT_EQ(1, env->stats().page_cache_io_index_loads);
        ASSERT_EQ(1, inner->page_locations_calls);
    }
    // the bytes before the first page of the index are the dictionary page
    {
        auto env = make_env(true);
        auto* spans = env->reader()->_page_cache_io_spans(true, &planned.stream);
        ASSERT_NO_FATAL_FAILURE(expect_spans(spans, {{60, 40}, {100, 50}, {150, 50}}));
    }
    // a column that holds its offset index already answers from it, with no read and no stream
    {
        auto env = make_env(false);
        ASSERT_TRUE(env->reader()->get_offset_index(0).ok());
        auto* spans = env->reader()->_page_cache_io_spans(false, nullptr);
        ASSERT_NO_FATAL_FAILURE(expect_spans(spans, {{100, 50}, {150, 50}}));
        ASSERT_EQ(0, env->stats().page_cache_io_index_loads);
    }
    // an index that does not fit the chunk is not used, and it is not read again
    {
        auto env = make_env(false);
        env->chunk()->meta_data.data_page_offset = 101;
        env->chunk()->meta_data.total_compressed_size = 99;
        ASSERT_TRUE(env->reader()->_page_cache_io_spans(true, &planned.stream) == nullptr);
        ASSERT_EQ(1, env->stats().page_cache_io_index_loads);
        ASSERT_TRUE(env->reader()->_page_cache_io_spans(true, &planned.stream) == nullptr);
        ASSERT_TRUE(env->reader()->_page_cache_io_spans(false, nullptr) == nullptr);
        ASSERT_EQ(1, env->stats().page_cache_io_index_loads);
    }
    // an index that is not valid, and a chunk without an index
    {
        PageIndexFile bad = make_page_index_file(
                make_column_index(2, 2, 2),
                make_offset_index({make_page_location(100, 50, 0), make_page_location(150, 50, 0)}));
        PlannedOffsetIndexStream bad_stream(bad);
        IndexEnv env(bad.bytes);
        env.set_page_index(0, bad.column_index_length, bad.column_index_length, bad.offset_index_length);
        env.set_row_group_rows(10);
        env.chunk()->meta_data.data_page_offset = 100;
        env.chunk()->meta_data.total_compressed_size = 100;
        ASSERT_TRUE(env.reader()->_page_cache_io_spans(true, &bad_stream.stream) == nullptr);
        ASSERT_TRUE(env.reader()->_page_cache_io_spans(true, &bad_stream.stream) == nullptr);
        // the read failed, and a read that failed is not a load
        ASSERT_EQ(0, env.stats().page_cache_io_index_loads);

        IndexEnv no_index(file.bytes);
        ASSERT_TRUE(no_index.reader()->_page_cache_io_spans(true, &planned.stream) == nullptr);
        ASSERT_EQ(0, no_index.stats().page_cache_io_index_loads);
    }
}

// The planner gets a chunk from each column that reads one: the bytes of the chunk, and the pages through the column.
TEST_F(PageIndexTest, ColumnCollectsItsChunkForTheFileReaderPlanner) {
    PageIndexFile file = make_page_index_file(make_column_index(2, 2, 2), make_offset_index(contiguous_pages()));
    PlannedOffsetIndexStream planned(file);
    IndexEnv env(file.bytes);
    env.set_page_index(0, file.column_index_length, file.column_index_length, file.offset_index_length);
    env.set_row_group_rows(10);
    env.chunk()->meta_data.data_page_offset = 100;
    env.chunk()->meta_data.total_compressed_size = 100;

    std::vector<PageCacheIoChunk> chunks;
    env.reader()->collect_page_cache_io_chunks(&planned.stream, &chunks);
    ASSERT_EQ(1U, chunks.size());
    ASSERT_EQ(100, chunks[0].begin);
    ASSERT_EQ(200, chunks[0].end);
    ASSERT_TRUE(chunks[0].pages(false) == nullptr);
    const std::vector<PageSpan>* spans = chunks[0].pages(true);
    ASSERT_TRUE(spans != nullptr);
    ASSERT_EQ(2U, spans->size());

    // the chunk begins at its dictionary page
    chunks.clear();
    env.chunk()->meta_data.__set_dictionary_page_offset(60);
    env.chunk()->meta_data.total_compressed_size = 140;
    env.reader()->collect_page_cache_io_chunks(&planned.stream, &chunks);
    ASSERT_EQ(1U, chunks.size());
    ASSERT_EQ(60, chunks[0].begin);
    ASSERT_EQ(200, chunks[0].end);

    // a chunk without bytes is not collected
    chunks.clear();
    env.chunk()->meta_data.total_compressed_size = 0;
    env.reader()->collect_page_cache_io_chunks(&planned.stream, &chunks);
    ASSERT_TRUE(chunks.empty());
}

// A predicate keeps nine rows of one page of a file with pages of 1000 values. The page index does not narrow the
// rows, so the lazy columns have nothing to read in the chunks before the rows, and they read the page of the rows
// without reading the pages before it. The rows are the same with the skipping off. The skipping does not depend on
// the page index: it works when the scan does not read the page index too.
TEST_F(PageIndexTest, OffsetIndexSkipJumpsToThePageOfTheSelectedRows) {
    const std::string file_path = "./be/test/formats/parquet/test_data/page_index_small_page.parquet";

    for (auto [enable, page_index] : std::vector<std::pair<bool, bool>>{{true, true}, {true, false}, {false, true}}) {
        SCOPED_TRACE(std::string(enable ? "skip on" : "skip off") + (page_index ? ", page index" : ", no page index"));
        ScopedOffsetIndexSkipConfig config_guard(enable);

        HdfsScannerStats stats;
        auto ctx = _create_file_c0_c1_c2_context(file_path);
        ctx->stats = &stats;
        ctx->options.parquet_page_index_enable = page_index;
        ctx->conjunct_ctxs_by_slot[0].clear();
        ctx->conjuncts.min_max_ctxs.clear();
        // c0: 1->20000, so the rows 15001..15009
        std::vector<TExpr> t_conjuncts;
        ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, 15000, &t_conjuncts);
        ParquetUTBase::append_int_conjunct(TExprOpcode::LT, 0, 15010, &t_conjuncts);
        ParquetUTBase::create_conjunct_ctxs(&_pool, _runtime_state, &t_conjuncts, &ctx->conjunct_ctxs_by_slot[0]);

        auto file = _create_file(file_path);
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(file_path));
        Status status = file_reader->init(ctx);
        ASSERT_TRUE(status.ok()) << status.to_string();

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

        size_t total_rows = 0;
        while (!status.is_end_of_file()) {
            chunk->reset();
            status = file_reader->get_next(&chunk);
            ASSERT_TRUE(status.ok() || status.is_end_of_file()) << status.to_string();
            chunk->check_or_die();
            for (size_t i = 0; i < chunk->num_rows(); ++i) {
                const int32_t c0 = chunk->get_column_by_index(0)->get(i).get_int32();
                ASSERT_TRUE(c0 > 15000 && c0 < 15010) << c0;
                ASSERT_EQ(20001 - c0, chunk->get_column_by_index(1)->get(i).get_int32());
                ASSERT_FALSE(chunk->get_column_by_index(2)->is_null(i));
                ASSERT_EQ(std::to_string(c0 % 100), chunk->get_column_by_index(2)->get(i).get_slice().to_string());
            }
            total_rows += chunk->num_rows();
        }
        ASSERT_EQ(9U, total_rows);
        if (enable) {
            ASSERT_GT(stats.page_skip_by_offset_index, 0);
            ASSERT_GT(stats.page_skip_by_offset_index_undecoded_bytes, 0);
        } else {
            ASSERT_EQ(0, stats.page_skip_by_offset_index);
            ASSERT_EQ(0, stats.page_skip_by_offset_index_undecoded_bytes);
        }
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

namespace {

// A remote file that counts the bytes it serves.
class CountingRemoteStream : public io::SeekableInputStream {
public:
    explicit CountingRemoteStream(std::string contents) : _contents(std::move(contents)) {}

    StatusOr<int64_t> read(void* data, int64_t count) override {
        count = std::min<int64_t>(count, _contents.size() - _offset);
        memcpy(data, _contents.data() + _offset, count);
        _offset += count;
        bytes += count;
        return count;
    }

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        if (offset < 0 || count < 0 || offset + count > static_cast<int64_t>(_contents.size())) {
            return Status::EndOfFile("read out of the file");
        }
        memcpy(out, _contents.data() + offset, count);
        bytes += count;
        return Status::OK();
    }

    Status seek(int64_t position) override {
        _offset = std::min<int64_t>(position, _contents.size());
        return Status::OK();
    }

    StatusOr<int64_t> position() override { return _offset; }

    StatusOr<int64_t> get_size() override { return _contents.size(); }

    int64_t bytes = 0;

private:
    std::string _contents;
    int64_t _offset = 0;
};

} // namespace

// A column whose column index, offset index and bloom filter are in the blocks 0, 1 and 3 of a file of four blocks of
// the data cache. A scan plans the three regions as one shared buffer of the four blocks, as the parquet reader does,
// and the stand reads them through the data cache.
class IndexDataCacheTest : public testing::Test {
public:
    static constexpr int64_t kBlock = 256 * 1024;
    static constexpr int64_t kFileSize = 4 * kBlock;
    static constexpr int64_t kOffsetIndexOffset = kBlock + 100;
    static constexpr int64_t kBloomFilterOffset = 3 * kBlock + 64;

    static void TearDownTestCase() {
        auto* cache = BlockCache::instance();
        if (cache != nullptr) {
            cache->shutdown();
        }
    }

    void SetUp() override {
        _saved_enable_auto_adjust = config::enable_datacache_disk_auto_adjust;
        config::enable_datacache_disk_auto_adjust = false;
        _saved_index_datacache_enable = config::parquet_index_datacache_enable;
        DataCache::GetInstance()->set_block_cache(
                TestCacheUtils::create_cache(TestCacheUtils::create_simple_options(kBlock, 100 * 1024 * 1024)));

        PageIndexFile index = make_page_index_file(make_column_index(2, 2, 2), make_offset_index(two_pages()));
        _column_index_length = index.column_index_length;
        _offset_index_length = index.offset_index_length;
        tparquet::BloomFilterHeader header;
        header.numBytes = 64;
        header.algorithm.__set_BLOCK(tparquet::SplitBlockAlgorithm());
        header.hash.__set_XXHASH(tparquet::XxHash());
        header.compression.__set_UNCOMPRESSED(tparquet::Uncompressed());
        std::string bloom_filter = thrift_bytes(header);
        bloom_filter.append(64, '\0');
        _bloom_filter_length = bloom_filter.size();

        std::string file(kFileSize, '\0');
        file.replace(0, _column_index_length, index.bytes.substr(0, _column_index_length));
        file.replace(kOffsetIndexOffset, _offset_index_length,
                     index.bytes.substr(_column_index_length, _offset_index_length));
        file.replace(kBloomFilterOffset, _bloom_filter_length, bloom_filter);
        _remote = std::make_shared<CountingRemoteStream>(std::move(file));
    }

    void TearDown() override {
        config::enable_datacache_disk_auto_adjust = _saved_enable_auto_adjust;
        config::parquet_index_datacache_enable = _saved_index_datacache_enable;
    }

protected:
    // A scan that opens the file: the streams and the column are new, the remote file and the data cache are shared.
    struct Scan {
        std::shared_ptr<io::SharedBufferedInputStream> sb_stream;
        std::shared_ptr<io::CacheInputStream> cache_stream;
        std::unique_ptr<IndexEnv> env;
    };

    Scan open_scan() {
        Scan scan;
        scan.sb_stream = std::make_shared<io::SharedBufferedInputStream>(_remote, "index-file", kFileSize);
        scan.cache_stream = std::make_shared<io::CacheInputStream>(scan.sb_stream, "index-file", kFileSize, 1000000);
        scan.cache_stream->set_enable_populate_cache(true);
        scan.sb_stream->set_align_size(scan.cache_stream->get_align_size());
        scan.env = std::make_unique<IndexEnv>(scan.cache_stream, kFileSize);
        scan.env->set_page_index(0, _column_index_length, kOffsetIndexOffset, _offset_index_length);
        scan.env->set_row_group_rows(10);

        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        ranges.emplace_back(0, _column_index_length);
        ranges.emplace_back(kOffsetIndexOffset, _offset_index_length);
        ranges.emplace_back(kBloomFilterOffset, _bloom_filter_length);
        CHECK(scan.sb_stream->set_io_ranges(ranges).ok());
        return scan;
    }

    // The first scan loads the offset index only, as the page skip does. The second scan reads the column index, the
    // offset index and the bloom filter.
    void check_two_scans(bool index_datacache_enable) {
        config::parquet_index_datacache_enable = index_datacache_enable;
        {
            Scan scan = open_scan();
            ASSERT_TRUE(scan.env->reader()->get_offset_index(0).ok());
            const auto& stats = scan.cache_stream->stats();
            // The read loads the buffer of the four blocks, and the switch decides if the other three are written.
            ASSERT_EQ(kFileSize, stats.page_index.remote_read_bytes);
            ASSERT_EQ(index_datacache_enable ? kFileSize : kBlock, stats.page_index.populate_bytes);
            ASSERT_EQ(0, stats.page_index.cache_read_bytes);
            ASSERT_EQ(0, stats.bloom_filter.remote_read_bytes);
        }
        const int64_t remote_bytes_after_first_scan = _remote->bytes;
        ASSERT_EQ(kFileSize, remote_bytes_after_first_scan);

        Scan scan = open_scan();
        auto filtered = scan.env->page_index_filter(10);
        ASSERT_TRUE(filtered.ok()) << filtered.status().to_string();
        ParquetBlockSplitBloomFilter bloom_filter;
        Status st =
                scan.env->reader()->_init_column_bloom_filter(kBloomFilterOffset, _bloom_filter_length, bloom_filter);
        ASSERT_TRUE(st.ok()) << st.to_string();

        const auto& stats = scan.cache_stream->stats();
        if (index_datacache_enable) {
            // All three regions are in the data cache, and the second scan downloads nothing.
            ASSERT_EQ(remote_bytes_after_first_scan, _remote->bytes);
            ASSERT_EQ(0, stats.page_index.remote_read_bytes);
            ASSERT_EQ(0, stats.bloom_filter.remote_read_bytes);
            ASSERT_GT(stats.page_index.cache_read_bytes, 0);
            ASSERT_GT(stats.bloom_filter.cache_read_bytes, 0);
        } else {
            // The column index is in a block that the first scan downloaded and did not write.
            ASSERT_GT(_remote->bytes, remote_bytes_after_first_scan);
            ASSERT_GT(stats.page_index.remote_read_bytes, 0);
        }
    }

    std::shared_ptr<CountingRemoteStream> _remote;
    int32_t _column_index_length = 0;
    int32_t _offset_index_length = 0;
    int32_t _bloom_filter_length = 0;

private:
    bool _saved_enable_auto_adjust = false;
    bool _saved_index_datacache_enable = true;
};

TEST_F(IndexDataCacheTest, SecondScanReadsTheIndexRegionsFromTheDataCache) {
    ASSERT_NO_FATAL_FAILURE(check_two_scans(true));
}

TEST_F(IndexDataCacheTest, SecondScanDownloadsTheBlocksThatTheFirstScanDidNotWriteWithoutTheSwitch) {
    ASSERT_NO_FATAL_FAILURE(check_two_scans(false));
}

} // namespace starrocks::parquet
