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

#include "formats/parquet/group_reader.h"

#include <formats/parquet/scalar_column_reader.h>
#include <gtest/gtest.h>
#include <testutil/assert.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/nullable_column.h"
#include "common/config.h"
#include "connector/deletion_vector/deletion_bitmap.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "exprs/arithmetic_expr.h"
#include "exprs/array_expr.h"
#include "exprs/array_map_expr.h"
#include "exprs/binary_predicate.h"
#include "exprs/column_ref.h"
#include "exprs/dictmapping_expr.h"
#include "exprs/expr.h"
#include "exprs/function_call_expr.h"
#include "exprs/lambda_function.h"
#include "exprs/literal.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
#include "formats/parquet/column_materializer.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/complex_column_reader.h"
#include "formats/parquet/lazy_materialization_context.h"
#include "formats/parquet/parquet_ut_base.h"
#include "formats/parquet/read_range_planner.h"
#include "formats/parquet/utils.h"
#include "formats/reserved_columns.h"
#include "fs/fs.h"
#include "runtime/descriptor_helper.h"
#include "runtime/global_dict/parser.h"
#include "runtime/runtime_state.h"
#include "simd/simd.h"
#include "storage/column_expr_predicate.h"
#include "storage/runtime_filter_predicate.h"
#include "storage/types.h"
#include "testutil/exprs_test_helper.h"
#include "testutil/scoped_updater.h"

namespace starrocks::parquet {

class MockInputStream : public io::SeekableInputStream {
public:
    StatusOr<int64_t> read(void* data, int64_t size) override { return size; }
    StatusOr<int64_t> position() override { return 0; }
    StatusOr<int64_t> get_size() override { return 0; }
    Status seek(int64_t offset) override { return Status::OK(); }
};

class MockColumnReader : public ColumnReader {
public:
    explicit MockColumnReader(tparquet::Type::type type) : ColumnReader(nullptr), _type(type) {}
    ~MockColumnReader() override = default;

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst_col) override {
        size_t num_rows = static_cast<size_t>(range.span_size());
        if (_step > 1) {
            return Status::EndOfFile("");
        }
        size_t start = 0;
        if (_step == 0) {
            start = 0;
            num_rows = 8;
        } else if (_step == 1) {
            start = 8;
            num_rows = 4;
        }

        auto dst = dst_col->as_mutable_ptr();
        if (_type == tparquet::Type::type::INT32) {
            _append_int32_column(dst.get(), start, num_rows);
        } else if (_type == tparquet::Type::type::INT64) {
            _append_int64_column(dst.get(), start, num_rows);
        } else if (_type == tparquet::Type::type::INT96) {
            _append_int96_column(dst.get(), start, num_rows);
        } else if (_type == tparquet::Type::type::BYTE_ARRAY) {
            _append_binary_column(dst.get(), start, num_rows);
        } else if (_type == tparquet::Type::type::FLOAT) {
            _append_float_column(dst.get(), start, num_rows);
        } else if (_type == tparquet::Type::type::DOUBLE) {
            _append_double_column(dst.get(), start, num_rows);
        }

        _step++;
        return Status::OK();
    }

    void set_need_parse_levels(bool need_parse_levels) override{};

    void get_levels(int16_t** def_levels, int16_t** rep_levels, size_t* num_levels) override {}

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {}

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {}

private:
    static void _append_int32_column(Column* column, size_t start, size_t num_rows) {
        for (int i = 0; i < num_rows; i++) {
            (*column).append_datum(i + static_cast<int32_t>(start));
        }
    }

    static void _append_int64_column(Column* column, size_t start, size_t num_rows) {
        for (int64_t i = 0; i < num_rows; i++) {
            (*column).append_datum(i + static_cast<int64_t>(start));
        }
    }

    static void _append_int96_column(Column* column, size_t start, size_t num_rows) {
        for (int64_t i = 0; i < num_rows; i++) {
            (*column).append_datum(i + static_cast<int64_t>(start));
        }
    }

    static void _append_binary_column(Column* column, size_t start, size_t num_rows) {
        for (size_t i = 0; i < num_rows; i++) {
            std::string str = std::string("str") + std::to_string(i + start);
            Slice slice;
            slice.data = str.data();
            slice.size = str.length();
            (*column).append_datum(slice);
        }
    }

    static void _append_float_column(Column* column, size_t start, size_t num_rows) {
        for (int64_t i = 0; i < num_rows; i++) {
            (*column).append_datum(0.5f * (i + start));
        }
    }

    static void _append_double_column(Column* column, size_t start, size_t num_rows) {
        for (int64_t i = 0; i < num_rows; i++) {
            (*column).append_datum(1.5 * (i + start));
        }
    }

    int _step = 0;
    tparquet::Type::type _type = tparquet::Type::type::INT32;
};

class MockPredicateErrorReader : public ColumnReader {
public:
    explicit MockPredicateErrorReader(const ParquetField* field) : ColumnReader(field) {}
    ~MockPredicateErrorReader() override = default;

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst_col) override {
        dst_col->as_mutable_ptr()->append_default(range.span_size());
        return Status::OK();
    }

    void set_need_parse_levels(bool need_parse_levels) override {}
    void get_levels(int16_t** def_levels, int16_t** rep_levels, size_t* num_levels) override {}
    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {}
    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {}

    StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                             CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                             const uint64_t rg_num_rows) const override {
        return Status::InternalError("mock row_group_zone_map_filter error");
    }

    StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) override {
        return Status::InternalError("mock page_index_zone_map_filter error");
    }
};

class MockIORangeColumnReader : public ColumnReader {
public:
    explicit MockIORangeColumnReader(std::vector<io::SharedBufferedInputStream::IORange> ranges)
            : ColumnReader(nullptr), _ranges(std::move(ranges)) {}
    ~MockIORangeColumnReader() override = default;

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst_col) override {
        dst_col->as_mutable_ptr()->append_default(range.span_size());
        return Status::OK();
    }

    void set_need_parse_levels(bool need_parse_levels) override {}
    void get_levels(int16_t** def_levels, int16_t** rep_levels, size_t* num_levels) override {}
    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {}

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {
        if (ranges == nullptr) {
            return;
        }
        for (const auto& range : _ranges) {
            ranges->emplace_back(range.offset, range.size, active);
            if (end_offset != nullptr) {
                *end_offset = std::max(*end_offset, range.offset + range.size);
            }
        }
    }

private:
    std::vector<io::SharedBufferedInputStream::IORange> _ranges;
};

class MockTempColumnReader : public ColumnReader {
public:
    explicit MockTempColumnReader() : ColumnReader(nullptr) {}

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter*, ColumnPtr& dst) override {
        if (_temp_col == nullptr) {
            _temp_col = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        }
        dst = _temp_col;
        _temp_col->as_mutable_raw_ptr()->append_datum(Datum(int32_t(42)));
        return Status::OK();
    }

    Status finalize_lazy_state(ColumnPtr& col) override {
        if (col.get() == _temp_col.get()) {
            _finalize_called = true;
            size_t n = _temp_col->size();
            auto dest = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
            dest->append_default(n);
            _temp_col->as_mutable_raw_ptr()->reset_column();
            col = std::move(dest);
        }
        return Status::OK();
    }

    Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) override {
        dst = src;
        return Status::OK();
    }

    void set_need_parse_levels(bool) override {}
    void get_levels(level_t**, level_t**, size_t*) override {}
    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>*, int64_t*, ColumnIOTypeFlags,
                                 bool) override {}
    void select_offset_index(const SparseRange<uint64_t>&, const uint64_t) override {}

    ColumnPtr _temp_col = nullptr;
    bool _finalize_called = false;
};

// An INT column that only the compound conjunct reads, so the group reader reads it lazily.
// value(row) is its value at a row of the row group. With is_output the query outputs the column, so
// the group reader reads it for the rows left even if no conjunct reads it.
struct CompoundLazyColumn {
    SlotId slot_id = 0;
    std::function<int32_t(uint64_t)> value;
    bool is_output = false;
};

// Setup of GroupReaderTest::_scan_with_compound_predicate(). The row group has num_rows rows, 12
// unless the test says otherwise. Column a (slot kActiveSlot) holds the row number and has the
// single-slot conjunct a >= active_min, so it is active. The lazy columns and the compound
// conjuncts over them are given by the test.
struct CompoundScanCase {
    static constexpr SlotId kActiveSlot = 700;

    using MakeConjunct = std::function<StatusOr<Expr*>(ObjectPool*, RuntimeState*)>;

    size_t num_rows = 12;
    // Rows asked for in each GroupReader::get_next() call.
    size_t rows_per_read = 12;
    int32_t active_min = 0;
    std::vector<CompoundLazyColumn> lazy_columns;
    // Creates the root of the compound conjunct. The setup prepares and opens it afterwards, so a
    // dictionary that the conjunct needs is registered in the runtime state here.
    MakeConjunct make_conjunct;
    // Compound conjuncts after the first one, in the order of evaluation.
    std::vector<MakeConjunct> more_conjuncts;
    // Rows of the row group (row numbers) that the deletion vector drops.
    std::vector<uint64_t> deleted_rows;
    // A join runtime filter that holds join_values and probes the column of join_filter_slot; no
    // filter when join_values is empty. With join_filter_ready false the filter is declared but
    // has not arrived yet.
    std::vector<int32_t> join_values;
    SlotId join_filter_slot = kActiveSlot;
    bool join_filter_ready = true;
    // With join_filter_ready false: the filter arrives after this many get_next() calls, as a filter that the
    // build side builds late does. -1 means that it never arrives.
    int install_filter_after_reads = -1;
    // The descriptor of the join filter has a sink, where the reader reports the cost of its lazy columns.
    bool enable_filter_scan_sink = false;
    // The join filter has no Bloom filter, as a filter that the build side could not afford the Bloom filter for, so it
    // keeps every value inside the range of join_values.
    bool join_filter_min_max_only = false;
    // The scan reads every lazy column of the output chunk as INT, which fails on a NULL. The group
    // reader fills a lazy column with NULL when no conjunct reads it and no output needs it, so a case
    // with such a column sets this to false and the scan collects no lazy values.
    bool collect_lazy_values = true;
};

struct CompoundScanResult {
    std::vector<int32_t> a_values;
    // One entry per lazy column, in the order of CompoundScanCase::lazy_columns.
    std::vector<std::vector<int32_t>> lazy_values;
    std::vector<size_t> lazy_read_counts;
    // Rows kept by the filter of each read of the column; -1 for a read without a filter.
    std::vector<std::vector<int64_t>> lazy_read_filter_rows;
    // What the group reader cached about the compound conjunct before the scan.
    bool conjunct_has_indirect_inputs = false;
    bool conjunct_has_dict_mapping = false;
    std::vector<SlotId> conjunct_slots;
    HdfsScannerStats stats;
    // The rows that each compound conjunct saw and kept, in the order of the conjuncts.
    std::vector<HdfsScannerContext::CompoundConjunctStats> conjunct_stats;
    // What the reader reported to the sink of the join filter, when the case has one.
    int64_t sink_lazy_rows = 0;
    int64_t sink_lazy_ns = 0;
};

class GroupReaderTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

private:
    StatusOr<CompoundScanResult> _scan_with_compound_predicate(const CompoundScanCase& scan_case);

    RandomAccessFile* _create_file();
    tparquet::ColumnChunk* _create_t_column_chunk(const std::string& file_path, tparquet::Type::type type);
    tparquet::RowGroup* _create_t_row_group(GroupReaderParam* param);
    tparquet::FileMetaData* _create_t_filemeta(GroupReaderParam* param);
    tparquet::SchemaElement* _create_root_schema_element(GroupReaderParam* param);
    tparquet::SchemaElement* _create_schema_element(const std::string& col_name, tparquet::Type::type type);
    Status _create_filemeta(FileMetaData** file_meta, GroupReaderParam* param);
    GroupReaderParam* _create_group_reader_param();
    // Prepares a row group whose active column (slot 0) has one range of 1 MiB at offset 0 and, if
    // `has_lazy_column`, whose lazy column (slot 1) has `lazy_ranges`. The row group reads from a new shared
    // buffered stream kept in _sb_stream, so the previous row group of the test must be destroyed first.
    StatusOr<std::unique_ptr<GroupReader>> _prepare_group_with_lazy_ranges(
            GroupReaderParam* param, bool has_lazy_column,
            std::vector<io::SharedBufferedInputStream::IORange> lazy_ranges);
    static ChunkPtr _create_chunk(GroupReaderParam* param);

    static void _check_int32_column(Column* column, size_t start, size_t count);
    static void _check_int64_column(Column* column, size_t start, size_t count);
    static void _check_int96_column(Column* column, size_t start, size_t count);
    static void _check_binary_column(Column* column, size_t start, size_t count);
    static void _check_float_column(Column* column, size_t start, size_t count);
    static void _check_double_column(Column* column, size_t start, size_t count);
    static void _check_chunk(GroupReaderParam* param, const ChunkPtr& chunk, size_t start, size_t count);

    // The number of rows of the row groups that _create_filemeta() describes.
    size_t _row_group_num_rows = 12;
    ObjectPool _pool;
    std::shared_ptr<io::SharedBufferedInputStream> _sb_stream;
};

ChunkPtr GroupReaderTest::_create_chunk(GroupReaderParam* param) {
    ChunkPtr chunk = std::make_shared<Chunk>();
    for (auto& column : param->read_cols) {
        auto c = ColumnHelper::create_column(column.slot_type(), true);
        chunk->append_column(std::move(c), column.slot_id());
    }
    return chunk;
}

void GroupReaderTest::_check_int32_column(Column* column, size_t start, size_t count) {
    ASSERT_EQ(column->size(), count);
    for (size_t i = 0; i < count; i++) {
        ASSERT_EQ(column->get(i).get_int32(), static_cast<int32_t>(start + i));
    }
}

void GroupReaderTest::_check_int64_column(Column* column, size_t start, size_t count) {
    ASSERT_EQ(column->size(), count);

    for (size_t i = 0; i < count; i++) {
        ASSERT_EQ(column->get(i).get_int64(), static_cast<int64_t>(start + i));
    }
}

void GroupReaderTest::_check_int96_column(Column* column, size_t start, size_t count) {
    ASSERT_EQ(column->size(), count);

    for (size_t i = 0; i < count; i++) {
        ASSERT_EQ(column->get(i).get_int64(), static_cast<int64_t>(start + i));
    }
}

void GroupReaderTest::_check_binary_column(Column* column, size_t start, size_t count) {
    for (size_t i = 0; i < count; i++) {
        auto check_slice = column->get(i).get_slice();
        std::string str = std::string("str") + std::to_string(i + start);
        Slice slice;
        slice.data = str.data();
        slice.size = str.length();
        ASSERT_TRUE(slice == check_slice);
    }
}

void GroupReaderTest::_check_float_column(Column* column, size_t start, size_t count) {
    for (size_t i = 0; i < count; i++) {
        float value = column->get(i).get_float();
        float exp = 0.5f * (start + i);
        ASSERT_FLOAT_EQ(exp, value);
    }
}

void GroupReaderTest::_check_double_column(Column* column, size_t start, size_t count) {
    for (size_t i = 0; i < count; i++) {
        double value = column->get(i).get_double();
        double exp = 1.5 * (start + i);
        ASSERT_DOUBLE_EQ(exp, value);
    }
}

void GroupReaderTest::_check_chunk(GroupReaderParam* param, const ChunkPtr& chunk, size_t start, size_t count) {
    ASSERT_EQ(param->read_cols.size(), chunk->num_columns());
    for (size_t i = 0; i < param->read_cols.size(); i++) {
        auto column = chunk->columns()[i]->as_mutable_ptr().get();
        auto _type = param->read_cols[i].type_in_parquet;
        size_t num_rows = count;

        if (_type == tparquet::Type::type::INT32) {
            _check_int32_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::INT64) {
            _check_int64_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::INT96) {
            _check_int96_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::BYTE_ARRAY) {
            _check_binary_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::FLOAT) {
            _check_float_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::DOUBLE) {
            _check_double_column(column, start, num_rows);
        }
    }
}

RandomAccessFile* GroupReaderTest::_create_file() {
    return _pool.add(new RandomAccessFile(std::make_shared<MockInputStream>(), "mock-random-access-file"));
}

tparquet::ColumnChunk* GroupReaderTest::_create_t_column_chunk(const std::string& file_path,
                                                               tparquet::Type::type type) {
    auto* column = _pool.add(new tparquet::ColumnChunk());

    column->__set_file_path(file_path);
    column->file_offset = 0;
    column->meta_data.type = type;
    column->meta_data.data_page_offset = 4;

    return column;
}

tparquet::RowGroup* GroupReaderTest::_create_t_row_group(GroupReaderParam* param) {
    // create column chunks
    std::vector<tparquet::ColumnChunk> cols;
    for (size_t i = 0; i < param->read_cols.size(); i++) {
        auto* col = _create_t_column_chunk("c" + std::to_string(i), param->read_cols[i].type_in_parquet);
        cols.emplace_back(*col);
    }

    // create row group
    auto* row_group = _pool.add(new tparquet::RowGroup());
    row_group->__set_columns(cols);
    row_group->__set_num_rows(_row_group_num_rows);

    return row_group;
}

tparquet::SchemaElement* GroupReaderTest::_create_root_schema_element(GroupReaderParam* param) {
    auto* element = _pool.add(new tparquet::SchemaElement());

    element->__set_num_children(param->read_cols.size());
    return element;
}

tparquet::SchemaElement* GroupReaderTest::_create_schema_element(const std::string& col_name,
                                                                 tparquet::Type::type type) {
    auto* element = _pool.add(new tparquet::SchemaElement());

    element->__set_type(type);
    element->__set_name(col_name);
    element->__set_num_children(0);

    return element;
}

tparquet::FileMetaData* GroupReaderTest::_create_t_filemeta(GroupReaderParam* param) {
    auto* file_meta = _pool.add(new tparquet::FileMetaData());

    // row group
    std::vector<tparquet::RowGroup> row_groups;
    for (size_t i = 0; i < 2; i++) {
        row_groups.emplace_back(*_create_t_row_group(param));
    }

    // schema elements
    std::vector<tparquet::SchemaElement> schema_elements;
    schema_elements.emplace_back(*_create_root_schema_element(param));
    for (size_t i = 0; i < param->read_cols.size(); i++) {
        std::string name = "c" + std::to_string(i);
        auto type = param->read_cols[i].type_in_parquet;
        schema_elements.emplace_back(*_create_schema_element(name, type));
    }

    // create file meta
    file_meta->__set_version(0);
    file_meta->__set_row_groups(row_groups);
    file_meta->__set_schema(schema_elements);

    return file_meta;
}

Status GroupReaderTest::_create_filemeta(FileMetaData** file_meta, GroupReaderParam* param) {
    auto* t_file_meta = _create_t_filemeta(param);

    *file_meta = _pool.add(new FileMetaData());
    return (*file_meta)->init(*t_file_meta, true);
}

static GroupReaderParam::Column _create_group_reader_param_of_column(ObjectPool* pool, int idx,
                                                                     tparquet::Type::type par_type,
                                                                     LogicalType prim_type) {
    SlotDescriptor* slot =
            pool->add(new SlotDescriptor(idx, fmt::format("col{}", idx), TypeDescriptor::from_logical_type(prim_type)));
    GroupReaderParam::Column c;
    c.idx_in_parquet = idx;
    c.type_in_parquet = par_type;
    c.slot_desc = slot;
    return c;
}

static HdfsScannerStats g_hdfs_stats;
GroupReaderParam* GroupReaderTest::_create_group_reader_param() {
    GroupReaderParam::Column c1 =
            _create_group_reader_param_of_column(&_pool, 0, tparquet::Type::type::INT32, LogicalType::TYPE_INT);
    GroupReaderParam::Column c2 =
            _create_group_reader_param_of_column(&_pool, 1, tparquet::Type::type::INT64, LogicalType::TYPE_BIGINT);
    GroupReaderParam::Column c3 = _create_group_reader_param_of_column(&_pool, 2, tparquet::Type::type::BYTE_ARRAY,
                                                                       LogicalType::TYPE_VARCHAR);
    GroupReaderParam::Column c4 =
            _create_group_reader_param_of_column(&_pool, 3, tparquet::Type::type::INT96, LogicalType::TYPE_DATETIME);
    GroupReaderParam::Column c5 =
            _create_group_reader_param_of_column(&_pool, 4, tparquet::Type::type::FLOAT, LogicalType::TYPE_FLOAT);
    GroupReaderParam::Column c6 =
            _create_group_reader_param_of_column(&_pool, 5, tparquet::Type::type::DOUBLE, LogicalType::TYPE_DOUBLE);

    // Minimal scanner context so GroupReader::_create_column_readers() can
    // dereference scanner_ctx->options without crashing.
    auto* scanner_ctx = _pool.add(new HdfsScannerContext());

    auto* param = _pool.add(new GroupReaderParam());
    param->read_cols.emplace_back(c1);
    param->read_cols.emplace_back(c2);
    param->read_cols.emplace_back(c3);
    param->read_cols.emplace_back(c4);
    param->read_cols.emplace_back(c5);
    param->read_cols.emplace_back(c6);
    param->stats = &g_hdfs_stats;
    param->scanner_ctx = scanner_ctx;
    return param;
}

StatusOr<std::unique_ptr<GroupReader>> GroupReaderTest::_prepare_group_with_lazy_ranges(
        GroupReaderParam* param, bool has_lazy_column,
        std::vector<io::SharedBufferedInputStream::IORange> lazy_ranges) {
    FileMetaData* file_meta;
    RETURN_IF_ERROR(_create_filemeta(&file_meta, param));
    param->chunk_size = config::vector_chunk_size;
    param->file = _create_file();
    param->file_metadata = file_meta;
    _sb_stream = std::make_shared<io::SharedBufferedInputStream>(std::make_shared<MockInputStream>(), "mock",
                                                                 1024L * 1024 * 1024);
    param->sb_stream = _sb_stream.get();

    auto group_reader = std::make_unique<GroupReader>(*param, 0, std::make_shared<SkipRowsContext>(), 0);
    RETURN_IF_ERROR(group_reader->init());
    group_reader->_column_readers.clear();
    group_reader->_column_materializer->clear_classification();
    group_reader->_column_materializer->add_active_column(0);
    group_reader->_column_readers.emplace(
            0, std::make_unique<MockIORangeColumnReader>(
                       std::vector<io::SharedBufferedInputStream::IORange>{{0, 1024 * 1024}}));
    if (has_lazy_column) {
        group_reader->_column_materializer->add_lazy_column(1);
        group_reader->_column_readers.emplace(1, std::make_unique<MockIORangeColumnReader>(std::move(lazy_ranges)));
    }
    RETURN_IF_ERROR(group_reader->prepare());
    return std::move(group_reader);
}

TEST_F(GroupReaderTest, TestInit) {
    // create file
    auto* file = _create_file();
    auto* param = _create_group_reader_param();

    // create file meta
    FileMetaData* file_meta;
    Status status = _create_filemeta(&file_meta, param);
    ASSERT_TRUE(status.ok());

    // create row group reader
    param->chunk_size = config::vector_chunk_size;
    param->file = file;
    param->file_metadata = file_meta;
    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    // init row group reader
    status = group_reader->init();
    ASSERT_TRUE(status.ok());
    status = group_reader->prepare();
    // timezone is empty
    ASSERT_FALSE(status.ok());
    //ASSERT_TRUE(status.is_end_of_file());
}

static void replace_column_readers(GroupReader* group_reader, GroupReaderParam* param) {
    group_reader->_column_readers.clear();
    group_reader->_column_materializer->clear_classification();
    for (size_t i = 0; i < param->read_cols.size(); i++) {
        auto r = std::make_unique<MockColumnReader>(param->read_cols[i].type_in_parquet);
        group_reader->_column_readers[i] = std::move(r);
        group_reader->_column_materializer->add_active_column(i);
    }
}

static void prepare_row_range(GroupReader* group_reader) {
    group_reader->_range =
            SparseRange<uint64_t>(group_reader->_row_group_first_row,
                                  group_reader->_row_group_first_row + group_reader->_row_group_metadata->num_rows);
    group_reader->_range_iter = group_reader->_range.new_iterator();
}

TEST_F(GroupReaderTest, TestGetNext) {
    // create file
    auto* file = _create_file();
    auto* param = _create_group_reader_param();

    // create file meta
    FileMetaData* file_meta;
    Status status = _create_filemeta(&file_meta, param);
    ASSERT_TRUE(status.ok());

    // create row group reader
    param->chunk_size = config::vector_chunk_size;
    param->file = file;
    param->file_metadata = file_meta;
    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    // init row group reader
    status = group_reader->init();
    ASSERT_TRUE(status.ok());
    status = group_reader->prepare();
    ASSERT_FALSE(status.ok());

    // replace column readers
    replace_column_readers(group_reader, param);
    // create chunk
    group_reader->_column_materializer->mutable_read_chunk() = _create_chunk(param);

    auto chunk = _create_chunk(param);

    prepare_row_range(group_reader);
    // get next
    size_t row_count = 8;
    status = group_reader->get_next(&chunk, &row_count);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(row_count, 8);
    _check_chunk(param, chunk, 0, 8);

    // reset and get next.
    chunk = _create_chunk(param);
    row_count = 8;
    status = group_reader->get_next(&chunk, &row_count);
    ASSERT_TRUE(status.is_end_of_file());
    ASSERT_EQ(row_count, 4);
    _check_chunk(param, chunk, 8, 4);
}

TEST_F(GroupReaderTest, ColumnReaderCreateTypeMismatch) {
    ParquetField field;
    field.name = "col0";
    field.type = ColumnType::ARRAY;

    TypeDescriptor col_type;
    col_type.type = LogicalType::TYPE_VARCHAR;

    ColumnReaderOptions options;
    auto st = ColumnReaderFactory::create(options, &field, col_type, nullptr);
    ASSERT_FALSE(st.ok()) << st;
    std::cout << st.status().message() << "\n";
}

TEST_F(GroupReaderTest, FixedValueColumnReaderTest) {
    auto col1 = std::make_unique<FixedValueColumnReader>(kNullDatum);
    ASSERT_OK(col1->prepare());
    col1->get_levels(nullptr, nullptr, nullptr);
    col1->set_need_parse_levels(false);
    col1->collect_column_io_range(nullptr, nullptr, ColumnIOType::PAGES, true);
    SparseRange<uint64_t> sparse_range;
    col1->select_offset_index(sparse_range, 100);
    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::create_varchar_type(100), true);
    Range<uint64_t> range(0, 100);
    ASSERT_TRUE(col1->read_range(range, nullptr, column).ok());

    TypeInfoPtr type_info = get_type_info(LogicalType::TYPE_INT);
    ColumnPredicate* is_null_predicate = _pool.add(new_column_null_predicate(type_info, 1, true));
    ColumnPredicate* is_not_null_predicate = _pool.add(new_column_null_predicate(type_info, 1, false));

    std::vector<const ColumnPredicate*> predicates;
    predicates.push_back(is_null_predicate);
    predicates.push_back(is_not_null_predicate);

    ASSERT_TRUE(col1->row_group_zone_map_filter(predicates, CompoundNodeType::AND, 1, 100).value());
    ASSERT_FALSE(col1->row_group_zone_map_filter(predicates, CompoundNodeType::OR, 1, 100).value());
}

TEST_F(GroupReaderTest, ParquetUtilsGetNonNullDataColumnAndRowConstNullable) {
    auto data = Int32Column::create();
    data->append(123);
    auto nulls = NullColumn::create();
    nulls->append(0);
    auto nullable = NullableColumn::create(std::move(data), std::move(nulls));
    auto const_nullable = ConstColumn::create(nullable, 4);

    const Column* out_column = nullptr;
    size_t out_row = 999;
    ASSERT_TRUE(ParquetUtils::get_non_null_data_column_and_row(const_nullable.get(), 3, &out_column, &out_row));
    ASSERT_NE(out_column, nullptr);
    ASSERT_TRUE(out_column->is_numeric());
    ASSERT_EQ(0, out_row);

    const Column* null_out_column = nullptr;
    size_t null_out_row = 0;
    auto null_data = Int32Column::create();
    null_data->append(7);
    auto all_nulls = NullColumn::create();
    all_nulls->append(1);
    auto all_null_nullable = NullableColumn::create(std::move(null_data), std::move(all_nulls));
    auto const_all_null = ConstColumn::create(all_null_nullable, 2);
    ASSERT_FALSE(
            ParquetUtils::get_non_null_data_column_and_row(const_all_null.get(), 1, &null_out_column, &null_out_row));
}

TEST_F(GroupReaderTest, ParquetUtilsHasNonNullValueConstNullable) {
    auto data = Int32Column::create();
    data->append(10);
    auto nulls = NullColumn::create();
    nulls->append(0);
    auto nullable = NullableColumn::create(std::move(data), std::move(nulls));
    auto const_nullable = ConstColumn::create(nullable, 8);
    ASSERT_TRUE(ParquetUtils::has_non_null_value(const_nullable.get(), 8));

    auto null_data = Int32Column::create();
    null_data->append(10);
    auto all_nulls = NullColumn::create();
    all_nulls->append(1);
    auto all_null_nullable = NullableColumn::create(std::move(null_data), std::move(all_nulls));
    auto const_all_null = ConstColumn::create(all_null_nullable, 8);
    ASSERT_FALSE(ParquetUtils::has_non_null_value(const_all_null.get(), 8));
}

TEST_F(GroupReaderTest, ParquetUtilsHasNonNullBinaryValueBranches) {
    auto str_data = BinaryColumn::create();
    str_data->append(Slice("abc"));
    auto str_nulls = NullColumn::create();
    str_nulls->append(0);
    auto str_nullable = NullableColumn::create(std::move(str_data), std::move(str_nulls));
    auto const_binary = ConstColumn::create(str_nullable, 3);
    ASSERT_TRUE(ParquetUtils::has_non_null_binary_value(const_binary.get(), 3));

    auto null_str_data = BinaryColumn::create();
    null_str_data->append(Slice("abc"));
    auto null_str_nulls = NullColumn::create();
    null_str_nulls->append(1);
    auto null_str_nullable = NullableColumn::create(std::move(null_str_data), std::move(null_str_nulls));
    auto const_null_binary = ConstColumn::create(null_str_nullable, 3);
    ASSERT_FALSE(ParquetUtils::has_non_null_binary_value(const_null_binary.get(), 3));

    auto int_data = Int32Column::create();
    int_data->append(1);
    auto int_nulls = NullColumn::create();
    int_nulls->append(0);
    auto int_nullable = NullableColumn::create(std::move(int_data), std::move(int_nulls));
    auto const_non_binary = ConstColumn::create(int_nullable, 3);
    ASSERT_FALSE(ParquetUtils::has_non_null_binary_value(const_non_binary.get(), 3));

    auto plain_binary = BinaryColumn::create();
    plain_binary->append(Slice("v"));
    ASSERT_TRUE(ParquetUtils::has_non_null_binary_value(plain_binary.get(), 1));
}

static StatusOr<const ColumnPredicate*> create_struct_subfield_eq_predicate(ObjectPool* pool, RuntimeState* state,
                                                                            ColumnId column_id, SlotId slot_id,
                                                                            const std::vector<std::string>& subfield,
                                                                            const std::string& value) {
    std::vector<TExprNode> nodes;

    TExprNode node0 = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::VARCHAR, TExprOpcode::EQ);
    node0.__set_is_monotonic(true);
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SUBFIELD_EXPR;
    node1.is_nullable = true;
    node1.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node1.num_children = 1;
    node1.used_subfield_names = subfield;
    node1.__isset.used_subfield_names = true;
    node1.__set_is_monotonic(true);
    nodes.emplace_back(node1);

    TExprNode node2;
    node2.node_type = TExprNodeType::SLOT_REF;
    node2.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node2.num_children = 0;
    TSlotRef t_slot_ref;
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    node2.__set_slot_ref(t_slot_ref);
    node2.is_nullable = true;
    node2.__set_is_monotonic(true);
    nodes.emplace_back(node2);

    TExprNode node3;
    node3.node_type = TExprNodeType::STRING_LITERAL;
    node3.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node3.num_children = 0;
    TStringLiteral string_literal;
    string_literal.value = value;
    node3.__set_string_literal(string_literal);
    node3.is_nullable = false;
    node3.__set_is_monotonic(true);
    nodes.emplace_back(node3);

    TExpr t_expr;
    t_expr.nodes = std::move(nodes);
    std::vector<TExpr> t_conjuncts{t_expr};
    std::vector<ExprContext*> conjunct_ctxs;
    RETURN_IF_ERROR(Expr::create_expr_trees(pool, t_conjuncts, &conjunct_ctxs, nullptr));
    RETURN_IF_ERROR(Expr::prepare(conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(conjunct_ctxs, state));
    ASSIGN_OR_RETURN(auto pred,
                     ColumnExprPredicate::make_column_expr_predicate(get_type_info(LogicalType::TYPE_VARCHAR),
                                                                     column_id, state, conjunct_ctxs[0], nullptr));
    return pred;
}

TEST_F(GroupReaderTest, StructColumnReaderRowGroupZoneMapFilterChildError) {
    ParquetField field;
    field.name = "col_struct";
    field.type = ColumnType::STRUCT;

    ParquetField child_field;
    child_field.name = "a";
    child_field.type = ColumnType::SCALAR;
    child_field.physical_type = tparquet::Type::BYTE_ARRAY;
    field.children.push_back(child_field);

    std::map<std::string, ColumnReaderPtr> child_readers;
    child_readers.emplace("a", std::make_unique<MockPredicateErrorReader>(&field.children[0]));
    StructColumnReader reader(&field, std::move(child_readers));

    RuntimeState runtime_state{TQueryGlobals()};
    ASSIGN_OR_ABORT(auto* pred, create_struct_subfield_eq_predicate(&_pool, &runtime_state, 0, 0, {"a"}, "x"));
    _pool.add(const_cast<ColumnPredicate*>(pred));

    auto res = reader.row_group_zone_map_filter({pred}, CompoundNodeType::AND, 0, 10);
    ASSERT_TRUE(res.ok());
    ASSERT_FALSE(res.value());
}

TEST_F(GroupReaderTest, StructColumnReaderPageIndexZoneMapFilterChildError) {
    ParquetField field;
    field.name = "col_struct";
    field.type = ColumnType::STRUCT;

    ParquetField child_field;
    child_field.name = "a";
    child_field.type = ColumnType::SCALAR;
    child_field.physical_type = tparquet::Type::BYTE_ARRAY;
    field.children.push_back(child_field);

    std::map<std::string, ColumnReaderPtr> child_readers;
    child_readers.emplace("a", std::make_unique<MockPredicateErrorReader>(&field.children[0]));
    StructColumnReader reader(&field, std::move(child_readers));

    RuntimeState runtime_state{TQueryGlobals()};
    ASSIGN_OR_ABORT(auto* pred, create_struct_subfield_eq_predicate(&_pool, &runtime_state, 0, 0, {"a"}, "x"));
    _pool.add(const_cast<ColumnPredicate*>(pred));

    SparseRange<uint64_t> row_ranges;
    auto res = reader.page_index_zone_map_filter({pred}, &row_ranges, CompoundNodeType::AND, 0, 10);
    ASSERT_TRUE(res.ok());
    ASSERT_FALSE(res.value());
    ASSERT_EQ(10, row_ranges.span_size());
}

TEST_F(GroupReaderTest, StructColumnReaderReadRangeMissingSubfieldReader) {
    ParquetField field;
    field.name = "col_struct";
    field.type = ColumnType::STRUCT;

    std::map<std::string, ColumnReaderPtr> child_readers;
    child_readers.emplace("a", std::make_unique<MockColumnReader>(tparquet::Type::INT32));
    StructColumnReader reader(&field, std::move(child_readers));

    TypeDescriptor struct_type = TypeDescriptor::create_struct_type({"b"}, {TYPE_INT_DESC});
    ColumnPtr dst = ColumnHelper::create_column(struct_type, true);
    auto st = reader.read_range(Range<uint64_t>(0, 1), nullptr, dst);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error());
}

TEST_F(GroupReaderTest, StructColumnReaderReadRangeAllSubfieldsMissingInFile) {
    ParquetField field;
    field.name = "col_struct";
    field.type = ColumnType::STRUCT;

    std::map<std::string, ColumnReaderPtr> child_readers;
    child_readers.emplace("a", nullptr);
    StructColumnReader reader(&field, std::move(child_readers));

    TypeDescriptor struct_type = TypeDescriptor::create_struct_type({"a"}, {TYPE_INT_DESC});
    ColumnPtr dst = ColumnHelper::create_column(struct_type, true);
    auto st = reader.read_range(Range<uint64_t>(0, 1), nullptr, dst);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error());
}

TEST_F(GroupReaderTest, StructColumnReaderFillDstColumnMissingSubfieldReader) {
    ParquetField field;
    field.name = "col_struct";
    field.type = ColumnType::STRUCT;

    std::map<std::string, ColumnReaderPtr> child_readers;
    child_readers.emplace("a", std::make_unique<MockColumnReader>(tparquet::Type::INT32));
    StructColumnReader reader(&field, std::move(child_readers));

    TypeDescriptor struct_type = TypeDescriptor::create_struct_type({"b"}, {TYPE_INT_DESC});
    ColumnPtr src = ColumnHelper::create_column(struct_type, true);
    ColumnPtr dst = ColumnHelper::create_column(struct_type, true);
    auto st = reader.fill_dst_column(dst, src);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error());
}

// ==================== Iceberg v3 Row Lineage Tests ====================

TEST_F(GroupReaderTest, TestGetExtendedBigIntValueNullScanRange) {
    auto* param = _create_group_reader_param();
    param->scan_range = nullptr;

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    auto result = group_reader->_get_extended_bigint_value(0);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_not_found());
}

TEST_F(GroupReaderTest, TestGetExtendedBigIntValueNoExtendedColumns) {
    auto* param = _create_group_reader_param();
    param->scan_range = _pool.add(new THdfsScanRange());

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    auto result = group_reader->_get_extended_bigint_value(0);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_not_found());
}

TEST_F(GroupReaderTest, TestGetExtendedBigIntValueSlotNotFound) {
    auto* param = _create_group_reader_param();
    auto* scan_range = new THdfsScanRange();
    scan_range->__set_extended_columns(std::map<int32_t, TExpr>());
    param->scan_range = _pool.add(scan_range);

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    auto result = group_reader->_get_extended_bigint_value(999);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_not_found());
}

TEST_F(GroupReaderTest, TestGetExtendedBigIntValueEmptyNodes) {
    auto* param = _create_group_reader_param();

    std::map<int32_t, TExpr> extended_columns;
    TExpr expr;
    expr.nodes = std::vector<TExprNode>();
    extended_columns[0] = expr;

    auto* scan_range = new THdfsScanRange();
    scan_range->__set_extended_columns(extended_columns);
    param->scan_range = _pool.add(scan_range);

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    auto result = group_reader->_get_extended_bigint_value(0);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_invalid_argument());
}

TEST_F(GroupReaderTest, TestGetExtendedBigIntValueWrongNodeType) {
    auto* param = _create_group_reader_param();

    std::map<int32_t, TExpr> extended_columns;
    TExpr expr;
    TExprNode node;
    node.node_type = TExprNodeType::BOOL_LITERAL;
    expr.nodes.push_back(node);
    extended_columns[0] = expr;

    auto* scan_range = new THdfsScanRange();
    scan_range->__set_extended_columns(extended_columns);
    param->scan_range = _pool.add(scan_range);

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    auto result = group_reader->_get_extended_bigint_value(0);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_invalid_argument());
}

TEST_F(GroupReaderTest, TestGetExtendedBigIntValueSuccess) {
    auto* param = _create_group_reader_param();

    std::map<int32_t, TExpr> extended_columns;
    TExpr expr;
    TExprNode node;
    node.node_type = TExprNodeType::INT_LITERAL;
    TIntLiteral int_literal;
    int_literal.value = 42;
    node.__set_int_literal(int_literal);
    expr.nodes.push_back(node);
    extended_columns[0] = expr;

    auto* scan_range = new THdfsScanRange();
    scan_range->__set_extended_columns(extended_columns);
    param->scan_range = _pool.add(scan_range);

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    auto result = group_reader->_get_extended_bigint_value(0);
    ASSERT_OK(result);
    ASSERT_EQ(result.value().get_int64(), 42);
}

TEST_F(GroupReaderTest, TestCreateReservedIcebergColumnReaderNotFound) {
    auto* param = _create_group_reader_param();

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    SlotDescriptor slot(0, "_row_id", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT));
    auto result = group_reader->_create_reserved_iceberg_column_reader(&slot, formats::kIcebergRowIdColumnId);
    ASSERT_OK(result);
    ASSERT_EQ(result.value(), nullptr);
}

TEST_F(GroupReaderTest, TestIcebergRowIdColumnReaderCreation) {
    auto* param = _create_group_reader_param();
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, formats::kIcebergRowIdColumnName,
                                                     TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    reserved_slots->push_back(row_id_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 0);
    ASSERT_OK(group_reader->init());
    ASSERT_NE(group_reader->_column_readers[100], nullptr);
}

TEST_F(GroupReaderTest, TestIcebergRowIdWithoutFirstRowIdReturnsNull) {
    auto* param = _create_group_reader_param();
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, formats::kIcebergRowIdColumnName,
                                                     TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    reserved_slots->push_back(row_id_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 0);
    ASSERT_OK(group_reader->init());

    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);
    ASSERT_OK(group_reader->_column_readers[100]->read_range(Range<uint64_t>(0, 4), nullptr, column));
    ASSERT_EQ(4, column->size());
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(column->get(i).is_null());
    }
}

TEST_F(GroupReaderTest, TestIcebergRowIdWithoutFirstRowIdUsesRowPositionForLookupPath) {
    // Legacy (non-lineage) GLM lookup: _row_id is the file-local row position. The read range is
    // file-local, so a row group starting at row 7 must emit 7..10 for its first four rows.
    auto* param = _create_group_reader_param();
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, formats::kIcebergRowIdColumnName,
                                                     TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    auto* scan_range_id_slot = _pool.add(
            new SlotDescriptor(101, "_scan_range_id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)));
    reserved_slots->push_back(row_id_slot);
    reserved_slots->push_back(scan_range_id_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    auto* scan_range = new THdfsScanRange();
    param->scan_range = _pool.add(scan_range);

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 7);
    ASSERT_OK(group_reader->init());

    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);
    ASSERT_OK(group_reader->_column_readers[100]->read_range(Range<uint64_t>(7, 11), nullptr, column));
    ASSERT_EQ(4, column->size());
    ASSERT_EQ(7, column->get(0).get_int64());
    ASSERT_EQ(8, column->get(1).get_int64());
    ASSERT_EQ(9, column->get(2).get_int64());
    ASSERT_EQ(10, column->get(3).get_int64());
}

TEST_F(GroupReaderTest, TestIcebergRowIdSecondRowGroupUsesFileLevelFirstRowId) {
    // Row lineage (v3): _row_id = scan_range->first_row_id + file-local position. For a row group
    // that does not start the file (first row 7 here) the base must stay the file-level
    // first_row_id; a row-group-level base would double-count the row-group start and emit
    // 1014..1017 instead of 1007..1010.
    auto* param = _create_group_reader_param();
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, formats::kIcebergRowIdColumnName,
                                                     TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    reserved_slots->push_back(row_id_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    auto* scan_range = new THdfsScanRange();
    scan_range->__set_first_row_id(1000);
    param->scan_range = _pool.add(scan_range);

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 7);
    ASSERT_OK(group_reader->init());

    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);
    ASSERT_OK(group_reader->_column_readers[100]->read_range(Range<uint64_t>(7, 11), nullptr, column));
    ASSERT_EQ(4, column->size());
    ASSERT_EQ(1007, column->get(0).get_int64());
    ASSERT_EQ(1008, column->get(1).get_int64());
    ASSERT_EQ(1009, column->get(2).get_int64());
    ASSERT_EQ(1010, column->get(3).get_int64());
}

TEST_F(GroupReaderTest, TestIcebergLastUpdatedSequenceNumberColumnReaderCreation) {
    auto* param = _create_group_reader_param();
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* seq_slot = _pool.add(new SlotDescriptor(101, formats::kIcebergLastUpdatedSequenceNumberColumnName,
                                                  TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    reserved_slots->push_back(seq_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    std::map<int32_t, TExpr> extended_columns;
    TExpr expr;
    TExprNode node;
    node.node_type = TExprNodeType::INT_LITERAL;
    TIntLiteral int_literal;
    int_literal.value = 100;
    node.__set_int_literal(int_literal);
    expr.nodes.push_back(node);
    extended_columns[101] = expr;

    auto* scan_range = new THdfsScanRange();
    scan_range->__set_extended_columns(extended_columns);
    param->scan_range = _pool.add(scan_range);

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 0);
    ASSERT_OK(group_reader->init());
    ASSERT_NE(group_reader->_column_readers[101], nullptr);
}

static tparquet::FileMetaData build_t_filemeta_with_iceberg_columns(ObjectPool* pool, const GroupReaderParam* param,
                                                                    bool include_row_id, bool include_seq_num) {
    tparquet::FileMetaData t_file_meta;

    size_t num_read_cols = param->read_cols.size();
    size_t extra_cols = (include_row_id ? 1 : 0) + (include_seq_num ? 1 : 0);
    size_t total_cols = num_read_cols + extra_cols;

    std::vector<tparquet::SchemaElement> schema_elements;

    tparquet::SchemaElement root;
    root.__set_num_children(static_cast<int32_t>(total_cols));
    schema_elements.push_back(root);

    for (size_t i = 0; i < num_read_cols; i++) {
        tparquet::SchemaElement elem;
        elem.__set_type(param->read_cols[i].type_in_parquet);
        elem.__set_name("c" + std::to_string(i));
        elem.__set_num_children(0);
        schema_elements.push_back(elem);
    }

    if (include_row_id) {
        tparquet::SchemaElement elem;
        elem.__set_type(tparquet::Type::INT64);
        elem.__set_name(formats::kIcebergRowIdColumnName);
        elem.__set_num_children(0);
        elem.__set_field_id(formats::kIcebergRowIdColumnId);
        schema_elements.push_back(elem);
    }

    if (include_seq_num) {
        tparquet::SchemaElement elem;
        elem.__set_type(tparquet::Type::INT64);
        elem.__set_name(formats::kIcebergLastUpdatedSequenceNumberColumnName);
        elem.__set_num_children(0);
        elem.__set_field_id(formats::kIcebergLastUpdatedSequenceNumberColumnId);
        schema_elements.push_back(elem);
    }

    std::vector<tparquet::RowGroup> row_groups;
    for (size_t rg = 0; rg < 2; rg++) {
        tparquet::RowGroup row_group;
        std::vector<tparquet::ColumnChunk> cols;
        for (size_t i = 0; i < total_cols; i++) {
            tparquet::ColumnChunk col;
            col.__set_file_path("c" + std::to_string(i));
            col.file_offset = 0;
            // the reserved columns are INT64
            col.meta_data.type = i < num_read_cols ? param->read_cols[i].type_in_parquet : tparquet::Type::INT64;
            col.meta_data.data_page_offset = 4;
            cols.push_back(col);
        }
        row_group.__set_columns(cols);
        row_group.__set_num_rows(12);
        row_groups.push_back(row_group);
    }

    t_file_meta.__set_version(0);
    t_file_meta.__set_row_groups(row_groups);
    t_file_meta.__set_schema(schema_elements);
    return t_file_meta;
}

TEST_F(GroupReaderTest, TestCreateReservedIcebergColumnReaderFound) {
    auto* param = _create_group_reader_param();

    // Build file metadata with _row_id physical column
    auto t_file_meta = build_t_filemeta_with_iceberg_columns(&_pool, param,
                                                             /*include_row_id=*/true, /*include_seq_num=*/false);
    auto* file_meta = _pool.add(new FileMetaData());
    ASSERT_OK(file_meta->init(t_file_meta, true));

    auto* file = _pool.add(new RandomAccessFile(std::make_shared<MockInputStream>(), "mock"));
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;
    param->scanner_ctx->timezone = "UTC";

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    SlotDescriptor slot(200, formats::kIcebergRowIdColumnName,
                        TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT));
    auto result = group_reader->_create_reserved_iceberg_column_reader(&slot, formats::kIcebergRowIdColumnId);
    ASSERT_OK(result);
    // Physical column found -> non-null reader
    ASSERT_NE(result.value(), nullptr);
}

TEST_F(GroupReaderTest, TestIcebergRowIdPhysicalColumnReaderCreation) {
    auto* param = _create_group_reader_param();

    // Set up reserved_field_slots with _row_id
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, formats::kIcebergRowIdColumnName,
                                                     TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    reserved_slots->push_back(row_id_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    // Build file metadata WITH physical _row_id column (field_id matches)
    auto t_file_meta = build_t_filemeta_with_iceberg_columns(&_pool, param,
                                                             /*include_row_id=*/true, /*include_seq_num=*/false);
    auto* file_meta = _pool.add(new FileMetaData());
    ASSERT_OK(file_meta->init(t_file_meta, true));

    auto* file = _pool.add(new RandomAccessFile(std::make_shared<MockInputStream>(), "mock"));
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 0);
    ASSERT_OK(group_reader->init());
    // The _row_id column reader should be created from the physical column (not IcebergRowIdReader)
    ASSERT_NE(group_reader->_column_readers[100], nullptr);
}

TEST_F(GroupReaderTest, TestIcebergLastUpdatedSeqNumPhysicalColumnReaderCreation) {
    auto* param = _create_group_reader_param();

    // Set up reserved_field_slots with _last_updated_sequence_number
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* seq_slot = _pool.add(new SlotDescriptor(101, formats::kIcebergLastUpdatedSequenceNumberColumnName,
                                                  TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    reserved_slots->push_back(seq_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    // Build file metadata WITH physical _last_updated_sequence_number column (field_id matches)
    auto t_file_meta = build_t_filemeta_with_iceberg_columns(&_pool, param,
                                                             /*include_row_id=*/false, /*include_seq_num=*/true);
    auto* file_meta = _pool.add(new FileMetaData());
    ASSERT_OK(file_meta->init(t_file_meta, true));

    auto* file = _pool.add(new RandomAccessFile(std::make_shared<MockInputStream>(), "mock"));
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 0);
    ASSERT_OK(group_reader->init());
    // The _last_updated_sequence_number reader should be created from the physical column
    ASSERT_NE(group_reader->_column_readers[101], nullptr);
}

TEST_F(GroupReaderTest, CollectIORangesDeduplicatesIdenticalRangesAcrossReaders) {
    auto* file = _create_file();
    auto* param = _create_group_reader_param();

    FileMetaData* file_meta;
    ASSERT_TRUE(_create_filemeta(&file_meta, param).ok());

    param->chunk_size = config::vector_chunk_size;
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    group_reader->_column_readers.clear();
    group_reader->_column_materializer->clear_classification();
    group_reader->_column_materializer->add_active_column(0);
    group_reader->_column_materializer->add_lazy_column(1);
    group_reader->_column_readers.emplace(0, std::make_unique<MockIORangeColumnReader>(
                                                     std::vector<io::SharedBufferedInputStream::IORange>{{100, 20}}));
    group_reader->_column_readers.emplace(
            1, std::make_unique<MockIORangeColumnReader>(
                       std::vector<io::SharedBufferedInputStream::IORange>{{100, 20}, {200, 10}}));

    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    group_reader->collect_io_ranges(&ranges, &end_offset, ColumnIOType::PAGES);

    ASSERT_EQ(2, ranges.size());
    EXPECT_EQ(100, ranges[0].offset);
    EXPECT_EQ(20, ranges[0].size);
    EXPECT_TRUE(ranges[0].is_active);
    EXPECT_EQ(200, ranges[1].offset);
    EXPECT_EQ(10, ranges[1].size);
    EXPECT_FALSE(ranges[1].is_active);
    EXPECT_EQ(210, end_offset);
}

TEST_F(GroupReaderTest, TestIcebergBothPhysicalColumnsCreation) {
    auto* param = _create_group_reader_param();

    // Set up reserved_field_slots with both _row_id and _last_updated_sequence_number
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, formats::kIcebergRowIdColumnName,
                                                     TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    auto* seq_slot = _pool.add(new SlotDescriptor(101, formats::kIcebergLastUpdatedSequenceNumberColumnName,
                                                  TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    reserved_slots->push_back(row_id_slot);
    reserved_slots->push_back(seq_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    // Build file metadata with both physical iceberg columns
    auto t_file_meta = build_t_filemeta_with_iceberg_columns(&_pool, param,
                                                             /*include_row_id=*/true, /*include_seq_num=*/true);
    auto* file_meta = _pool.add(new FileMetaData());
    ASSERT_OK(file_meta->init(t_file_meta, true));

    auto* file = _pool.add(new RandomAccessFile(std::make_shared<MockInputStream>(), "mock"));
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 0);
    ASSERT_OK(group_reader->init());
    // Both physical column readers should be created
    ASSERT_NE(group_reader->_column_readers[100], nullptr);
    ASSERT_NE(group_reader->_column_readers[101], nullptr);
}

TEST_F(GroupReaderTest, ReadRangePlannerDeduplicateMergesIdenticalRanges) {
    using IORange = io::SharedBufferedInputStream::IORange;
    std::vector<IORange> ranges;
    ranges.emplace_back(100, 50, true);
    ranges.emplace_back(100, 50, false);
    ranges.emplace_back(200, 30, true);
    ranges.emplace_back(200, 30, true);

    ReadRangePlanner::deduplicate(&ranges);

    // Two duplicates: (100,50) merged → is_active = true, (200,30) merged → is_active = true.
    ASSERT_EQ(2u, ranges.size());
    EXPECT_EQ(100, ranges[0].offset);
    EXPECT_EQ(50, ranges[0].size);
    EXPECT_TRUE(ranges[0].is_active);
    EXPECT_EQ(200, ranges[1].offset);
    EXPECT_EQ(30, ranges[1].size);
    EXPECT_TRUE(ranges[1].is_active);
}

// Covers: ReadRangePlanner::deduplicate sorts by size when offsets are equal.

TEST_F(GroupReaderTest, ReadRangePlannerDeduplicateSortsBySizeWhenOffsetsEqual) {
    using IORange = io::SharedBufferedInputStream::IORange;
    std::vector<IORange> ranges;
    // Same offset, different sizes → must be ordered by size.
    ranges.emplace_back(100, 50, true);
    ranges.emplace_back(100, 30, true);

    ReadRangePlanner::deduplicate(&ranges);

    ASSERT_EQ(2u, ranges.size());
    EXPECT_EQ(100, ranges[0].offset);
    EXPECT_EQ(30, ranges[0].size);
    EXPECT_EQ(100, ranges[1].offset);
    EXPECT_EQ(50, ranges[1].size);
}

// ── Lazy column io coalescing ────────────────────────────────────────────────

namespace {

constexpr int64_t kMiB = 1024 * 1024;

// The two counters that all row groups of one scan share.
struct LazyCoalesceCounters {
    std::atomic<int64_t> finished{0};
    std::atomic<int64_t> needed{0};

    void set(int64_t finished_value, int64_t needed_value) {
        finished.store(finished_value);
        needed.store(needed_value);
    }

    void attach(GroupReaderParam* param) {
        param->lazy_coalesce_row_groups_finished = &finished;
        param->lazy_coalesce_row_groups_needed = &needed;
    }
};

// The profile counters of coalescing decisions, read before and after a row group is prepared.
struct CoalesceDecisionStats {
    int64_t together = 0;
    int64_t together_bytes = 0;
    int64_t seperately = 0;

    static CoalesceDecisionStats current() {
        return {g_hdfs_stats.active_lazy_coalesce_together, g_hdfs_stats.active_lazy_coalesce_together_bytes,
                g_hdfs_stats.active_lazy_coalesce_seperately};
    }
};

} // namespace

// Covers: ReadRangePlanner::should_coalesce_active_lazy coalesces when the scan has no counters.
TEST_F(GroupReaderTest, ReadRangePlannerShouldCoalesceWithoutCountersIsTrue) {
    auto* param = _create_group_reader_param();

    std::unordered_map<int32_t, std::unique_ptr<ColumnReader>> readers;
    ReadRangePlanner planner(*param, &readers);

    EXPECT_TRUE(planner.should_coalesce_active_lazy(64 * kMiB));
}

// Covers: ReadRangePlanner::should_coalesce_active_lazy coalesces only if the share of row groups that need
//         lazy columns is bigger than lazy_bytes / (lazy_bytes + io_coalesce_read_max_distance_size).
TEST_F(GroupReaderTest, ReadRangePlannerShouldCoalesceByShareOfRowGroups) {
    SCOPED_UPDATE(int32_t, config::io_coalesce_read_max_distance_size, kMiB);
    SCOPED_UPDATE(bool, config::io_coalesce_adaptive_lazy_active, true);
    auto* param = _create_group_reader_param();
    LazyCoalesceCounters counters;
    counters.attach(param);

    std::unordered_map<int32_t, std::unique_ptr<ColumnReader>> readers;
    ReadRangePlanner planner(*param, &readers);

    // 4 of 10 row groups need lazy columns.
    counters.set(10, 4);
    // 128 KiB is wasted in 6 row groups, one request is saved in 4: coalesce.
    EXPECT_TRUE(planner.should_coalesce_active_lazy(128 * 1024));
    // 4 MiB is wasted in 6 row groups, one request is saved in 4: do not coalesce.
    EXPECT_FALSE(planner.should_coalesce_active_lazy(4 * kMiB));

    // For 4 MiB the share must be bigger than 80%: 8 of 10 is not enough, 9 of 10 is.
    counters.set(10, 8);
    EXPECT_FALSE(planner.should_coalesce_active_lazy(4 * kMiB));
    counters.set(10, 9);
    EXPECT_TRUE(planner.should_coalesce_active_lazy(4 * kMiB));

    // Without lazy ranges there is nothing to decide.
    counters.set(10, 0);
    EXPECT_TRUE(planner.should_coalesce_active_lazy(0));
}

namespace {

// A reader that notes the flags that the planner hands it.
class FlagsRecordingColumnReader : public MockIORangeColumnReader {
public:
    FlagsRecordingColumnReader() : MockIORangeColumnReader(std::vector<io::SharedBufferedInputStream::IORange>{}) {}

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {
        seen_types.push_back(types);
        MockIORangeColumnReader::collect_column_io_range(ranges, end_offset, types, active);
    }

    std::vector<ColumnIOTypeFlags> seen_types;
};

} // namespace

// The probe of the bloom filters visits the columns of the predicate tree only. We fear that the filter of another column is
// planned and loaded with the ranges next to it for nothing, so the planner hands the flag of the bloom filter to a
// column only when the tree has a predicate on it, and hands the other flags to every column.
TEST_F(GroupReaderTest, ReadRangePlannerPlansABloomFilterOnlyForAColumnWithAPredicate) {
    auto* param = _create_group_reader_param();
    // a predicate on the column with the slot id 1
    ColumnPredicate* predicate = new_column_eq_predicate(get_type_info(TYPE_BIGINT), 1, "5");
    param->scanner_ctx->predicates.predicate_free_pool.emplace_back(predicate);
    PredicateAndNode root;
    root.add_child(PredicateColumnNode(predicate));
    param->scanner_ctx->predicates.predicate_tree = PredicateTree::create(std::move(root));

    std::unordered_map<int32_t, std::unique_ptr<ColumnReader>> readers;
    auto without_predicate = std::make_unique<FlagsRecordingColumnReader>();
    auto with_predicate = std::make_unique<FlagsRecordingColumnReader>();
    auto* without_predicate_ptr = without_predicate.get();
    auto* with_predicate_ptr = with_predicate.get();
    readers.emplace(0, std::move(without_predicate));
    readers.emplace(1, std::move(with_predicate));
    ReadRangePlanner planner(*param, &readers);

    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    const ColumnIOTypeFlags kNone = ColumnIOType::INVALID;
    const ColumnIOTypeFlags kPageIndex = ColumnIOType::PAGE_INDEX;
    const ColumnIOTypeFlags kBloomFilter = ColumnIOType::BLOOM_FILTER;
    planner.collect_ranges({0, 1}, true, &ranges, &end_offset, kPageIndex | kBloomFilter);
    EXPECT_EQ((std::vector<ColumnIOTypeFlags>{kPageIndex}), without_predicate_ptr->seen_types);
    EXPECT_EQ((std::vector<ColumnIOTypeFlags>{kPageIndex | kBloomFilter}), with_predicate_ptr->seen_types);

    // the lazy columns are planned the same way
    planner.pre_plan_lazy_ranges({0, 1}, &end_offset, kBloomFilter);
    EXPECT_EQ(kNone, without_predicate_ptr->seen_types.back());
    EXPECT_EQ(kBloomFilter, with_predicate_ptr->seen_types.back());

    // a scan without a context has no tree, so no column has a predicate
    param->scanner_ctx = nullptr;
    planner.collect_ranges({1}, true, &ranges, &end_offset, kPageIndex | kBloomFilter);
    EXPECT_EQ(kPageIndex, with_predicate_ptr->seen_types.back());
}

// Covers: GroupReader::prepare coalesces small lazy ranges and keeps big lazy ranges apart for the same share
//         of row groups that need lazy columns.
TEST_F(GroupReaderTest, PrepareCoalescesSmallLazyRangesAndNotBigOnes) {
    SCOPED_UPDATE(int32_t, config::io_coalesce_read_max_distance_size, kMiB);
    SCOPED_UPDATE(bool, config::io_coalesce_adaptive_lazy_active, true);
    auto* param = _create_group_reader_param();
    LazyCoalesceCounters counters;
    counters.attach(param);

    // 4 of 10 row groups need lazy columns.
    counters.set(10, 4);
    {
        // 128 KiB of lazy bytes: the lazy range is read in the buffer of the active range.
        auto before = CoalesceDecisionStats::current();
        ASSIGN_OR_ABORT(auto group_reader, _prepare_group_with_lazy_ranges(param, true, {{kMiB, 128 * 1024}}));
        auto after = CoalesceDecisionStats::current();
        EXPECT_EQ(1, after.together - before.together);
        EXPECT_EQ(128 * 1024, after.together_bytes - before.together_bytes);
        EXPECT_EQ(0, after.seperately - before.seperately);
        EXPECT_EQ(1, _sb_stream->_map.size());
    }

    counters.set(10, 4);
    {
        // 4 MiB of lazy bytes: the lazy range gets a buffer of its own.
        auto before = CoalesceDecisionStats::current();
        ASSIGN_OR_ABORT(auto group_reader, _prepare_group_with_lazy_ranges(param, true, {{kMiB, 4 * kMiB}}));
        auto after = CoalesceDecisionStats::current();
        EXPECT_EQ(0, after.together - before.together);
        EXPECT_EQ(0, after.together_bytes - before.together_bytes);
        EXPECT_EQ(1, after.seperately - before.seperately);
        EXPECT_EQ(2, _sb_stream->_map.size());
    }
}

// Covers: GroupReader::prepare reads lazy ranges separately until kMinRowGroupsForLazyCoalesce row groups of
//         the scan have finished, even if all of them needed lazy columns.
TEST_F(GroupReaderTest, PrepareSeparatesLazyRangesBeforeMinimumRowGroups) {
    SCOPED_UPDATE(int32_t, config::io_coalesce_read_max_distance_size, kMiB);
    SCOPED_UPDATE(bool, config::io_coalesce_adaptive_lazy_active, true);
    auto* param = _create_group_reader_param();
    LazyCoalesceCounters counters;
    counters.attach(param);

    const int64_t min_row_groups = ReadRangePlanner::kMinRowGroupsForLazyCoalesce;
    counters.set(min_row_groups - 1, min_row_groups - 1);
    {
        auto before = CoalesceDecisionStats::current();
        ASSIGN_OR_ABORT(auto group_reader, _prepare_group_with_lazy_ranges(param, true, {{kMiB, 128 * 1024}}));
        auto after = CoalesceDecisionStats::current();
        EXPECT_EQ(0, after.together - before.together);
        EXPECT_EQ(1, after.seperately - before.seperately);
        EXPECT_EQ(2, _sb_stream->_map.size());
    }

    counters.set(min_row_groups, min_row_groups);
    {
        auto before = CoalesceDecisionStats::current();
        ASSIGN_OR_ABORT(auto group_reader, _prepare_group_with_lazy_ranges(param, true, {{kMiB, 128 * 1024}}));
        auto after = CoalesceDecisionStats::current();
        EXPECT_EQ(1, after.together - before.together);
        EXPECT_EQ(0, after.seperately - before.seperately);
        EXPECT_EQ(1, _sb_stream->_map.size());
    }
}

// Covers: io_coalesce_adaptive_lazy_active = false keeps meaning that lazy ranges are always coalesced.
TEST_F(GroupReaderTest, PrepareAlwaysCoalescesWhenAdaptiveLazyActiveIsOff) {
    SCOPED_UPDATE(int32_t, config::io_coalesce_read_max_distance_size, kMiB);
    auto* param = _create_group_reader_param();
    LazyCoalesceCounters counters;
    counters.attach(param);

    // No row group needed lazy columns so far and the lazy range is big.
    counters.set(100, 0);
    {
        SCOPED_UPDATE(bool, config::io_coalesce_adaptive_lazy_active, true);
        auto before = CoalesceDecisionStats::current();
        ASSIGN_OR_ABORT(auto group_reader, _prepare_group_with_lazy_ranges(param, true, {{kMiB, 4 * kMiB}}));
        auto after = CoalesceDecisionStats::current();
        EXPECT_EQ(0, after.together - before.together);
        EXPECT_EQ(1, after.seperately - before.seperately);
    }

    counters.set(100, 0);
    {
        SCOPED_UPDATE(bool, config::io_coalesce_adaptive_lazy_active, false);
        auto before = CoalesceDecisionStats::current();
        ASSIGN_OR_ABORT(auto group_reader, _prepare_group_with_lazy_ranges(param, true, {{kMiB, 4 * kMiB}}));
        auto after = CoalesceDecisionStats::current();
        EXPECT_EQ(1, after.together - before.together);
        EXPECT_EQ(4 * kMiB, after.together_bytes - before.together_bytes);
        EXPECT_EQ(0, after.seperately - before.seperately);
        EXPECT_EQ(1, _sb_stream->_map.size());
    }
}

// Covers: GroupReader::prepare reads io_coalesce_read_max_distance_size for each row group, so a new value
//         changes the decision for the next row group.
TEST_F(GroupReaderTest, PrepareFollowsChangeOfRequestPrice) {
    SCOPED_UPDATE(int32_t, config::io_coalesce_read_max_distance_size, kMiB);
    SCOPED_UPDATE(bool, config::io_coalesce_adaptive_lazy_active, true);
    auto* param = _create_group_reader_param();
    LazyCoalesceCounters counters;
    counters.attach(param);

    // 4 of 10 row groups need lazy columns and 1 MiB is read in vain by the others: the share must be bigger
    // than 50% while a request is worth 1 MiB.
    counters.set(10, 4);
    {
        auto before = CoalesceDecisionStats::current();
        ASSIGN_OR_ABORT(auto group_reader, _prepare_group_with_lazy_ranges(param, true, {{kMiB, kMiB}}));
        auto after = CoalesceDecisionStats::current();
        EXPECT_EQ(0, after.together - before.together);
        EXPECT_EQ(1, after.seperately - before.seperately);
    }

    // With a request worth 4 MiB the share must be bigger than 20%.
    config::io_coalesce_read_max_distance_size = 4 * kMiB;
    counters.set(10, 4);
    {
        auto before = CoalesceDecisionStats::current();
        ASSIGN_OR_ABORT(auto group_reader, _prepare_group_with_lazy_ranges(param, true, {{kMiB, kMiB}}));
        auto after = CoalesceDecisionStats::current();
        EXPECT_EQ(1, after.together - before.together);
        EXPECT_EQ(kMiB, after.together_bytes - before.together_bytes);
        EXPECT_EQ(0, after.seperately - before.seperately);
    }
}

// Covers: ReadRangePlanner::coalescable_lazy_bytes skips lazy ranges bigger than io_coalesce_read_max_buffer_size,
//         because the shared buffered stream gives them a buffer of their own whatever we decide.
TEST_F(GroupReaderTest, PrepareCountsOnlyLazyRangesThatCanBeCoalesced) {
    SCOPED_UPDATE(int32_t, config::io_coalesce_read_max_distance_size, kMiB);
    SCOPED_UPDATE(bool, config::io_coalesce_adaptive_lazy_active, true);
    auto* param = _create_group_reader_param();
    LazyCoalesceCounters counters;
    counters.attach(param);

    counters.set(10, 4);
    auto before = CoalesceDecisionStats::current();
    // A 64 KiB range next to the active range and a 20 MiB range, which is bigger than the 8 MiB buffer.
    ASSIGN_OR_ABORT(auto group_reader,
                    _prepare_group_with_lazy_ranges(param, true, {{kMiB, 64 * 1024}, {2 * kMiB, 20 * kMiB}}));
    auto after = CoalesceDecisionStats::current();
    EXPECT_EQ(64 * 1024, group_reader->_column_materializer->read_range_planner()->coalescable_lazy_bytes());
    // With 20 MiB counted the share would have to be above 95%, and the row group would be read separately.
    EXPECT_EQ(1, after.together - before.together);
    EXPECT_EQ(64 * 1024, after.together_bytes - before.together_bytes);
    EXPECT_EQ(0, after.seperately - before.seperately);
}

// Covers: GroupReader::prepare does not count a decision for a row group without lazy ranges.
TEST_F(GroupReaderTest, PrepareCountsNoDecisionWithoutLazyRanges) {
    auto* param = _create_group_reader_param();
    LazyCoalesceCounters counters;
    counters.attach(param);
    counters.set(10, 4);

    auto before = CoalesceDecisionStats::current();
    ASSIGN_OR_ABORT(auto group_reader, _prepare_group_with_lazy_ranges(param, false, {}));
    auto after = CoalesceDecisionStats::current();
    EXPECT_EQ(0, after.together - before.together);
    EXPECT_EQ(0, after.together_bytes - before.together_bytes);
    EXPECT_EQ(0, after.seperately - before.seperately);
}

// Covers: ~GroupReader counts a finished row group with lazy columns, and counts it as needing them only if a
//         lazy column was read; a row group without lazy columns is not counted.
TEST_F(GroupReaderTest, DestroyedRowGroupUpdatesLazyCoalesceCounters) {
    auto* param = _create_group_reader_param();
    LazyCoalesceCounters counters;
    counters.attach(param);
    const std::vector<io::SharedBufferedInputStream::IORange> lazy_ranges{{kMiB, 64 * 1024}};

    {
        // The lazy column is never read.
        ASSIGN_OR_ABORT(auto group_reader, _prepare_group_with_lazy_ranges(param, true, lazy_ranges));
    }
    EXPECT_EQ(1, counters.finished.load());
    EXPECT_EQ(0, counters.needed.load());

    {
        ASSIGN_OR_ABORT(auto group_reader, _prepare_group_with_lazy_ranges(param, true, lazy_ranges));
        ASSERT_OK(group_reader->_column_materializer->materialize_slot(1, Range<uint64_t>(0, 12), nullptr));
    }
    EXPECT_EQ(2, counters.finished.load());
    EXPECT_EQ(1, counters.needed.load());

    { ASSIGN_OR_ABORT(auto group_reader, _prepare_group_with_lazy_ranges(param, false, {})); }
    EXPECT_EQ(2, counters.finished.load());
    EXPECT_EQ(1, counters.needed.load());
}

// ── Temp Column Lifecycle Tests ──────────────────────────────────────────────

// Covers: ColumnMaterializer::materialize_slot finalizes lazy state
//         before caching, so _slot_cache always holds logical columns.

TEST_F(GroupReaderTest, TempColumnAccretesStaleWithoutReset) {
    MockTempColumnReader reader;

    ColumnPtr col1 = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
    ASSERT_OK(reader.read_range(Range<uint64_t>(0, 1), nullptr, col1));
    EXPECT_EQ(1u, col1->size()) << "First read: 1 row";

    // Simulate skipped fill: don't call finalize_lazy_state

    ColumnPtr col2 = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
    ASSERT_OK(reader.read_range(Range<uint64_t>(0, 1), nullptr, col2));
    EXPECT_EQ(2u, col2->size()) << "Without reset, second read accretes stale data: 1 stale + 1 new = 2 rows";

    // finalize_lazy_state produces n logical rows where n = temp size.
    // With stale accretion: temp has 2 rows, so finalize yields 2 rows.
    ASSERT_OK(reader.finalize_lazy_state(col2));
    EXPECT_EQ(2u, col2->size()) << "Stale accretion propagates: 2 logical rows from 2 temp rows";
    EXPECT_TRUE(reader._finalize_called);
    EXPECT_EQ(0u, reader._temp_col->size()) << "Temp column is empty after finalize";
}

// Contract test: finalize_lazy_state resets the temp column so that a
// subsequent read_range starts fresh — even when the mock itself does
// not reset before appending.  Verifies the finalize → reset → no-accretion
// lifecycle contract through the ColumnMaterializer pipeline.
//
// Production read_range() reset is exercised by integration tests.

static Status create_bigint_eq_conjunct_ctxs(ObjectPool* pool, RuntimeState* state, SlotId slot_id, int64_t value,
                                             std::vector<ExprContext*>* conjunct_ctxs) {
    TExprNode pred_node = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::BIGINT, TExprOpcode::EQ);
    TExprNode slot_ref = ExprsTestHelper::create_slot_expr_node_t<TYPE_BIGINT>(0, slot_id, true);
    TExprNode literal = ExprsTestHelper::create_literal<TYPE_BIGINT, int64_t>(value, false);

    TExpr expr;
    expr.nodes.emplace_back(pred_node);
    expr.nodes.emplace_back(slot_ref);
    expr.nodes.emplace_back(literal);

    std::vector<TExpr> conjunct_exprs{expr};
    RETURN_IF_ERROR(Expr::create_expr_trees(pool, conjunct_exprs, conjunct_ctxs, nullptr));
    RETURN_IF_ERROR(Expr::prepare(*conjunct_ctxs, state));
    DictOptimizeParser::disable_open_rewrite(conjunct_ctxs);
    RETURN_IF_ERROR(Expr::open(*conjunct_ctxs, state));
    return Status::OK();
}

static Status create_int_eq_conjunct_ctxs(ObjectPool* pool, RuntimeState* state, SlotId slot_id, int32_t value,
                                          std::vector<ExprContext*>* conjunct_ctxs) {
    TExprNode pred_node = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::INT, TExprOpcode::EQ);
    TExprNode slot_ref = ExprsTestHelper::create_slot_expr_node_t<TYPE_INT>(0, slot_id, true);
    TExprNode literal = ExprsTestHelper::create_literal<TYPE_INT, int32_t>(value, false);

    TExpr expr;
    expr.nodes.emplace_back(pred_node);
    expr.nodes.emplace_back(slot_ref);
    expr.nodes.emplace_back(literal);

    std::vector<TExpr> conjunct_exprs{expr};
    RETURN_IF_ERROR(Expr::create_expr_trees(pool, conjunct_exprs, conjunct_ctxs, nullptr));
    RETURN_IF_ERROR(Expr::prepare(*conjunct_ctxs, state));
    DictOptimizeParser::disable_open_rewrite(conjunct_ctxs);
    RETURN_IF_ERROR(Expr::open(*conjunct_ctxs, state));
    return Status::OK();
}

static Status create_varchar_eq_conjunct_ctxs(ObjectPool* pool, RuntimeState* state, SlotId slot_id,
                                              const std::string& value, std::vector<ExprContext*>* conjunct_ctxs) {
    TExprNode pred_node = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::VARCHAR, TExprOpcode::EQ);
    TExprNode slot_ref = ExprsTestHelper::create_slot_expr_node_t<TYPE_VARCHAR>(0, slot_id, true);
    TExprNode literal = ExprsTestHelper::create_literal<TYPE_VARCHAR, std::string>(value, false);

    TExpr expr;
    expr.nodes.emplace_back(pred_node);
    expr.nodes.emplace_back(slot_ref);
    expr.nodes.emplace_back(literal);

    std::vector<TExpr> conjunct_exprs{expr};
    RETURN_IF_ERROR(Expr::create_expr_trees(pool, conjunct_exprs, conjunct_ctxs, nullptr));
    RETURN_IF_ERROR(Expr::prepare(*conjunct_ctxs, state));
    DictOptimizeParser::disable_open_rewrite(conjunct_ctxs);
    RETURN_IF_ERROR(Expr::open(*conjunct_ctxs, state));
    return Status::OK();
}

class MockGlobalDictColumnReader : public ColumnReader {
public:
    MockGlobalDictColumnReader() : ColumnReader(nullptr) {}
    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter*, ColumnPtr& dst) override {
        if (_code_column == nullptr) {
            _code_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        }
        _code_column->as_mutable_raw_ptr()->reset_column();
        _code_column->as_mutable_raw_ptr()->append_datum(Datum(int32_t(100)));
        _code_column->as_mutable_raw_ptr()->append_datum(Datum(int32_t(200)));
        dst = _code_column;
        return Status::OK();
    }

    Status finalize_lazy_state(ColumnPtr& col) override {
        if (_finalize_called) return Status::OK();
        if (col.get() == _code_column.get()) {
            _finalize_called = true;
            auto logical = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
            logical->as_mutable_raw_ptr()->append_datum(Datum(Slice("hello")));
            logical->as_mutable_raw_ptr()->append_datum(Datum(Slice("world")));
            col = std::move(logical);
        }
        return Status::OK();
    }

    Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) override {
        if (src.get() == _code_column.get()) {
            auto logical = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
            logical->as_mutable_raw_ptr()->append_datum(Datum(Slice("decoded_hello")));
            logical->as_mutable_raw_ptr()->append_datum(Datum(Slice("decoded_world")));
            dst->as_mutable_raw_ptr()->swap_column(*logical);
            return Status::OK();
        }
        dst->as_mutable_raw_ptr()->swap_column(*(src->as_mutable_raw_ptr()));
        return Status::OK();
    }

    void set_need_parse_levels(bool) override {}
    void get_levels(level_t**, level_t**, size_t*) override {}
    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>*, int64_t*, ColumnIOTypeFlags,
                                 bool) override {}
    void select_offset_index(const SparseRange<uint64_t>&, const uint64_t) override {}

    ColumnPtr _code_column = nullptr;
    bool _finalize_called = false;
};

TEST_F(GroupReaderTest, ActiveGlobalDictEmitBypassesFillDstAfterCompoundFinalize) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;
    param->read_cols.clear();

    // Two active low-card-like slots, each with a single-slot conjunct.
    auto* slot_a =
            _pool.add(new SlotDescriptor(360, "dict_a", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)));
    auto* slot_b =
            _pool.add(new SlotDescriptor(361, "dict_b", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)));

    GroupReaderParam::Column ca{};
    ca.slot_desc = slot_a;
    GroupReaderParam::Column cb{};
    cb.slot_desc = slot_b;
    param->read_cols.emplace_back(ca);
    param->read_cols.emplace_back(cb);

    RuntimeState runtime_state{TQueryGlobals()};
    std::vector<ExprContext*> ctx_a, ctx_b;
    ASSERT_OK(create_varchar_eq_conjunct_ctxs(&_pool, &runtime_state, slot_a->id(), "x", &ctx_a));
    ASSERT_OK(create_varchar_eq_conjunct_ctxs(&_pool, &runtime_state, slot_b->id(), "y", &ctx_b));
    param->conjunct_ctxs_by_slot[slot_a->id()] = ctx_a;
    param->conjunct_ctxs_by_slot[slot_b->id()] = ctx_b;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    auto mock_a = std::make_unique<MockGlobalDictColumnReader>();
    auto mock_b = std::make_unique<MockGlobalDictColumnReader>();
    auto* mock_a_ptr = mock_a.get();
    auto* mock_b_ptr = mock_b.get();
    group_reader->_column_readers.emplace(slot_a->id(), std::move(mock_a));
    group_reader->_column_readers.emplace(slot_b->id(), std::move(mock_b));

    group_reader->_process_columns_and_conjunct_ctxs();
    ASSERT_OK(group_reader->_column_materializer->init_read_chunk());

    ASSERT_TRUE(group_reader->_column_materializer->active_slot_ids().size() > 0);

    size_t num_rows = 2;
    Range<uint64_t> full_range(0, num_rows);
    auto active_chunk = group_reader->_column_materializer->create_active_chunk();

    group_reader->_column_materializer->reset_read_chunk();

    // Simulate step 2a: read active columns → PHYSICAL (dict codes).
    Filter filter(num_rows, 1);
    ASSERT_OK(group_reader->_column_materializer->read_active_range(full_range, &filter, &active_chunk));

    // Simulate step 2b compound-conjunct prep: finalize all active columns
    // to LOGICAL.  fill_dst_column() handles already-logical columns via
    // pointer identity fallback (swap), so no _logical_slot_ids mark needed.
    for (int col_idx : group_reader->_column_materializer->active_column_indices()) {
        SlotId slot_id = param->read_cols[col_idx].slot_id();
        ASSERT_OK(group_reader->_column_materializer->finalize_active_slot(slot_id, active_chunk));
    }
    EXPECT_TRUE(mock_a_ptr->_finalize_called);
    EXPECT_TRUE(mock_b_ptr->_finalize_called);

    // Build output chunk and emit.  Before the fix, emit_physical_columns
    // would call fill_dst_column() on the already-logical global-dict columns
    // and fail with "not a dictionary code column".
    std::vector<SlotDescriptor*> dst_slots{slot_a, slot_b};
    auto dst_chunk = std::make_shared<Chunk>();
    for (auto* s : dst_slots) {
        auto col = ColumnHelper::create_column(s->type(), true);
        dst_chunk->append_column(col, s->id());
    }

    ASSERT_OK(group_reader->_column_materializer->emit_physical_columns(active_chunk, &dst_chunk));

    for (auto* s : dst_slots) {
        EXPECT_TRUE(dst_chunk->is_slot_exist(s->id()));
        EXPECT_EQ(2u, dst_chunk->get_column_by_slot_id(s->id())->size());
    }
}

// Covers: GroupReader::get_next Phase 2b late_materialize_skip_rows
//         when compound (scanner_ctxs) conjuncts filter all rows.

TEST_F(GroupReaderTest, LateMaterializeSkipRowsCompoundConjunctFiltersAll) {
    auto* file = _create_file();
    auto* param = _create_group_reader_param();
    param->chunk_size = config::vector_chunk_size;
    param->file = file;

    // Scanner conjunct on col0: INT32 = 999999 → all rows filtered.
    RuntimeState runtime_state{TQueryGlobals()};
    std::vector<ExprContext*> scanner_ctxs;
    ASSERT_OK(
            create_int_eq_conjunct_ctxs(&_pool, &runtime_state, param->read_cols[0].slot_id(), 999999, &scanner_ctxs));
    param->scanner_ctx->conjuncts.scanner_ctxs = scanner_ctxs;

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    ASSERT_OK(group_reader->init());
    group_reader->prepare();
    replace_column_readers(group_reader, param);
    group_reader->_column_materializer->mutable_read_chunk() = _create_chunk(param);
    prepare_row_range(group_reader);

    auto chunk = _create_chunk(param);
    size_t row_count = 8;
    auto status = group_reader->get_next(&chunk, &row_count);
    // All rows filtered by scanner_ctxs → get_next consumes all ranges and returns EOF.
    ASSERT_TRUE(status.is_end_of_file());
    // Skip count = first range (8) + second range (4).
    EXPECT_EQ(12, param->stats->late_materialize_skip_rows);
}

// Covers: HdfsScannerContext::evaluate_all_predicates evaluates
//         scanner_ctxs when slot-by-slot conjuncts are absent.

TEST_F(GroupReaderTest, EvaluateAllPredicatesWithScannerCtxs) {
    auto* scanner_ctx = _pool.add(new HdfsScannerContext());
    scanner_ctx->stats = &g_hdfs_stats;

    // Create a BIGINT chunk with 2 rows: values [0, 1].
    auto chunk = std::make_shared<Chunk>();
    auto col = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
    col->append_datum(Datum(int64_t(0)));
    col->append_datum(Datum(int64_t(1)));
    SlotId slot_id = 600;
    chunk->append_column(col, slot_id);

    // Scanner conjunct: col = 0 → filters row 1, keeps row 0.
    RuntimeState runtime_state{TQueryGlobals()};
    std::vector<ExprContext*> scanner_ctxs;
    ASSERT_OK(create_bigint_eq_conjunct_ctxs(&_pool, &runtime_state, slot_id, 0, &scanner_ctxs));
    scanner_ctx->conjuncts.scanner_ctxs = scanner_ctxs;

    ASSERT_OK(scanner_ctx->evaluate_all_predicates(&chunk));
    EXPECT_EQ(1u, chunk->num_rows());
    EXPECT_EQ(0, chunk->get_column_by_slot_id(slot_id)->get(0).get_int64());

    Expr::close(scanner_ctxs, &runtime_state);
}

// A lazy column reader that returns `span_size` default rows of BIGINT.
class MockLazyColumnReader : public ColumnReader {
public:
    MockLazyColumnReader() : ColumnReader(nullptr) {}
    ~MockLazyColumnReader() override = default;

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst_col) override {
        auto column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
        column->append_default(range.span_size());
        dst_col = std::move(column);
        return Status::OK();
    }

    void set_need_parse_levels(bool need_parse_levels) override {}
    void get_levels(int16_t** def_levels, int16_t** rep_levels, size_t* num_levels) override {}
    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {}
    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {}
};

namespace {

constexpr SlotId kActiveSlotId = 262;
constexpr SlotId kLazySlotId = 263;

// Sets two columns in `param`: an active one (kActiveSlotId) and a lazy one (kLazySlotId).
void set_active_and_lazy_columns(ObjectPool* pool, GroupReaderParam* param, bool lazy_is_output_column) {
    param->read_cols.clear();
    auto* active_slot = pool->add(
            new SlotDescriptor(kActiveSlotId, "a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    TSlotDescriptor t_lazy_slot = TSlotDescriptorBuilder()
                                          .type(LogicalType::TYPE_BIGINT)
                                          .column_name("b")
                                          .id(kLazySlotId)
                                          .is_output_column(lazy_is_output_column)
                                          .build();
    auto* lazy_slot = pool->add(new SlotDescriptor(t_lazy_slot));
    GroupReaderParam::Column active_column{};
    active_column.slot_desc = active_slot;
    GroupReaderParam::Column lazy_column{};
    lazy_column.slot_desc = lazy_slot;
    param->read_cols.emplace_back(active_column);
    param->read_cols.emplace_back(lazy_column);
}

// The active chunk of a 2 row range.
ChunkPtr make_active_chunk_of_two_rows() {
    auto chunk = std::make_shared<Chunk>();
    auto column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
    column->append_datum(Datum(int64_t(1)));
    column->append_datum(Datum(int64_t(2)));
    chunk->append_column(column, kActiveSlotId);
    return chunk;
}

} // namespace

// Covers: ColumnMaterializer::lazy_column_needed stays true once a chunk of the row group read a lazy column,
//         also when a later chunk reads none.
TEST_F(GroupReaderTest, LazyColumnNeededAccumulatesOverChunks) {
    auto* param = _create_group_reader_param();
    set_active_and_lazy_columns(&_pool, param, /*lazy_is_output_column=*/false);
    ColumnMaterializer::ColumnReaderMap column_readers;
    column_readers.emplace(kLazySlotId, std::make_unique<MockLazyColumnReader>());
    ColumnMaterializer materializer(*param, &column_readers);
    materializer.add_active_column(0);
    materializer.add_lazy_column(1);
    ASSERT_OK(materializer.init_read_chunk());

    const Range<uint64_t> range(0, 2);
    Range<uint64_t> no_range;
    Filter no_filter;

    // Chunk 1 reads no lazy column: the predicate-only lazy column is filled with defaults.
    materializer.reset_read_chunk();
    auto chunk = make_active_chunk_of_two_rows();
    ASSERT_OK(materializer.read_lazy_columns(range, no_range, no_filter, no_filter, false, chunk));
    EXPECT_FALSE(materializer.lazy_column_needed());

    // Chunk 2: a compound predicate reads the lazy column.
    materializer.reset_read_chunk();
    chunk = make_active_chunk_of_two_rows();
    ASSERT_OK(materializer.materialize_slot(kLazySlotId, range, nullptr));
    ASSERT_OK(materializer.read_lazy_columns(range, no_range, no_filter, no_filter, false, chunk));
    EXPECT_TRUE(materializer.lazy_column_needed());

    // Chunk 3 reads no lazy column.
    materializer.reset_read_chunk();
    chunk = make_active_chunk_of_two_rows();
    ASSERT_OK(materializer.read_lazy_columns(range, no_range, no_filter, no_filter, false, chunk));
    EXPECT_TRUE(materializer.lazy_column_needed());
}

// Covers: ColumnMaterializer::materialize_slot marks the row group as needing lazy columns when it reads one,
//         also if the chunk is filtered out before read_lazy_columns() runs.
TEST_F(GroupReaderTest, LazyColumnNeededIsSetByLazyReadOfFilteredChunk) {
    auto* param = _create_group_reader_param();
    set_active_and_lazy_columns(&_pool, param, /*lazy_is_output_column=*/false);
    ColumnMaterializer::ColumnReaderMap column_readers;
    column_readers.emplace(kLazySlotId, std::make_unique<MockLazyColumnReader>());
    ColumnMaterializer materializer(*param, &column_readers);
    materializer.add_active_column(0);
    materializer.add_lazy_column(1);
    ASSERT_OK(materializer.init_read_chunk());

    materializer.reset_read_chunk();
    ASSERT_OK(materializer.materialize_slot(kLazySlotId, Range<uint64_t>(0, 2), nullptr));
    EXPECT_TRUE(materializer.lazy_column_needed());
}

// Covers: ColumnMaterializer::read_lazy_columns marks the row group as needing lazy columns when it backfills an
//         output lazy column.
TEST_F(GroupReaderTest, LazyColumnNeededIsSetByBackfill) {
    auto* param = _create_group_reader_param();
    set_active_and_lazy_columns(&_pool, param, /*lazy_is_output_column=*/true);
    ColumnMaterializer::ColumnReaderMap column_readers;
    column_readers.emplace(kLazySlotId, std::make_unique<MockLazyColumnReader>());
    ColumnMaterializer materializer(*param, &column_readers);
    materializer.add_active_column(0);
    materializer.add_lazy_column(1);
    ASSERT_OK(materializer.init_read_chunk());
    EXPECT_FALSE(materializer.lazy_column_needed());

    const Range<uint64_t> range(0, 2);
    Range<uint64_t> no_range;
    Filter no_filter;
    materializer.reset_read_chunk();
    auto chunk = make_active_chunk_of_two_rows();
    ASSERT_OK(materializer.read_lazy_columns(range, no_range, no_filter, no_filter, false, chunk));
    EXPECT_TRUE(materializer.lazy_column_needed());
}

class MockSourceColumnReader : public ColumnReader {
public:
    explicit MockSourceColumnReader(ColumnPtr source_column)
            : ColumnReader(nullptr), _source_column(std::move(source_column)) {}
    ~MockSourceColumnReader() override = default;

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst_col) override {
        dst_col = _source_column;
        return Status::OK();
    }

    void set_need_parse_levels(bool need_parse_levels) override {}
    void get_levels(int16_t** def_levels, int16_t** rep_levels, size_t* num_levels) override {}
    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {}
    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {}

private:
    ColumnPtr _source_column;
};

class MockCountingColumnReader : public ColumnReader {
public:
    MockCountingColumnReader() : ColumnReader(nullptr) {}
    ~MockCountingColumnReader() override = default;

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst_col) override {
        _read_count++;
        dst_col->as_mutable_ptr()->append_default(range.span_size());
        return Status::OK();
    }

    void set_need_parse_levels(bool need_parse_levels) override {}
    void get_levels(int16_t** def_levels, int16_t** rep_levels, size_t* num_levels) override {}
    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {}
    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {}

    size_t read_count() const { return _read_count; }

private:
    size_t _read_count = 0;
};

// A BIGINT column reader over a fixed list of values. A row that the filter drops gets `placeholder`, as a
// decoder fills the rows of a page run where no row is kept. Without a placeholder it returns the real
// value of every row, as a column read for a row that a deletion vector removes does.
class MockFilteredBigintColumnReader : public ColumnReader {
public:
    MockFilteredBigintColumnReader(std::vector<int64_t> values, std::optional<int64_t> placeholder)
            : ColumnReader(nullptr), _values(std::move(values)), _placeholder(placeholder) {}
    ~MockFilteredBigintColumnReader() override = default;

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst_col) override {
        auto* dst = dst_col->as_mutable_raw_ptr();
        for (uint64_t i = 0; i < range.span_size(); ++i) {
            const bool dropped = filter != nullptr && (*filter)[i] == 0;
            const int64_t value = dropped && _placeholder.has_value() ? *_placeholder : _values[range.begin() + i];
            dst->append_datum(Datum(value));
        }
        return Status::OK();
    }

    void set_need_parse_levels(bool need_parse_levels) override {}
    void get_levels(int16_t** def_levels, int16_t** rep_levels, size_t* num_levels) override {}
    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {}
    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {}

private:
    std::vector<int64_t> _values;
    std::optional<int64_t> _placeholder;
};

static Status create_bigint_divide_conjunct_ctxs(ObjectPool* pool, RuntimeState* state, SlotId slot_id,
                                                 int64_t threshold, std::vector<ExprContext*>* conjunct_ctxs) {
    std::vector<TExpr> t_conjuncts;
    ParquetUTBase::append_bigint_divide_conjunct(TExprOpcode::GT, slot_id, 100, threshold, &t_conjuncts);
    RETURN_IF_ERROR(Expr::create_expr_trees(pool, t_conjuncts, conjunct_ctxs, nullptr));
    RETURN_IF_ERROR(Expr::prepare(*conjunct_ctxs, state));
    DictOptimizeParser::disable_open_rewrite(conjunct_ctxs);
    return Expr::open(*conjunct_ctxs, state);
}

static RuntimeState make_error_for_division_by_zero_state() {
    TQueryOptions options;
    options.__set_error_for_division_by_zero(true);
    return RuntimeState(TUniqueId(), options, TQueryGlobals(), nullptr);
}

// Covers: a conjunct on a column does not fail on a row that the filter dropped. Each of the two columns
//         has a conjunct 100 / col > 20, and the column that is read second is read under the filter of the
//         first one, so it holds 0 in the rows that the first one dropped. The columns are read in a random
//         order, so both orders must pass.
TEST_F(GroupReaderTest, SlotConjunctsSkipRowsDroppedByTheOtherColumn) {
    auto* param = _create_group_reader_param();
    param->read_cols.clear();

    auto* slot_a = _pool.add(new SlotDescriptor(270, "a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    auto* slot_b = _pool.add(new SlotDescriptor(271, "b", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    GroupReaderParam::Column ca{};
    ca.slot_desc = slot_a;
    GroupReaderParam::Column cb{};
    cb.slot_desc = slot_b;
    param->read_cols.emplace_back(ca);
    param->read_cols.emplace_back(cb);

    RuntimeState runtime_state = make_error_for_division_by_zero_state();
    std::vector<ExprContext*> ctx_a, ctx_b;
    ASSERT_OK(create_bigint_divide_conjunct_ctxs(&_pool, &runtime_state, slot_a->id(), 20, &ctx_a));
    ASSERT_OK(create_bigint_divide_conjunct_ctxs(&_pool, &runtime_state, slot_b->id(), 20, &ctx_b));

    // 100 / a > 20 keeps the rows 0, 2, 3, 5, and 100 / b > 20 keeps the rows 0, 3.
    ColumnMaterializer::ColumnReaderMap column_readers;
    column_readers.emplace(slot_a->id(), std::make_unique<MockFilteredBigintColumnReader>(
                                                 std::vector<int64_t>{4, 5, 2, 1, 5, 4}, int64_t(0)));
    column_readers.emplace(slot_b->id(), std::make_unique<MockFilteredBigintColumnReader>(
                                                 std::vector<int64_t>{3, 10, 5, 1, 10, 7}, int64_t(0)));

    ColumnMaterializer materializer(*param, &column_readers);
    materializer.add_active_column(0);
    materializer.add_active_column(1);
    materializer.rebuild_read_order_ctx();
    for (auto* ctx : ctx_a) {
        materializer.add_post_read_conjunct(slot_a->id(), ctx);
    }
    for (auto* ctx : ctx_b) {
        materializer.add_post_read_conjunct(slot_b->id(), ctx);
    }
    ASSERT_OK(materializer.init_read_chunk());
    auto chunk = materializer.create_active_chunk();

    Filter filter(6, 1);
    auto hit_count = materializer.read_active_range_round_by_round(Range<uint64_t>(0, 6), &filter, &chunk, nullptr);
    ASSERT_TRUE(hit_count.ok()) << hit_count.status();
    EXPECT_EQ(2u, hit_count.value());
    const std::vector<uint8_t> expected{1, 0, 0, 1, 0, 0};
    ASSERT_EQ(expected.size(), filter.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i], filter[i]) << "row " << i;
    }

    Expr::close(ctx_a, &runtime_state);
    Expr::close(ctx_b, &runtime_state);
}

// Covers: a conjunct does not fail on a row that a deletion vector removed before the column was read,
//         even when the reader returns the real value of that row. The value 0 of a deleted row would
//         make 100 / b fail.
TEST_F(GroupReaderTest, SlotConjunctSkipsRowsDeletedBeforeTheRead) {
    auto* param = _create_group_reader_param();
    param->read_cols.clear();

    auto* slot_b = _pool.add(new SlotDescriptor(272, "b", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    GroupReaderParam::Column cb{};
    cb.slot_desc = slot_b;
    param->read_cols.emplace_back(cb);

    RuntimeState runtime_state = make_error_for_division_by_zero_state();
    std::vector<ExprContext*> ctx_b;
    ASSERT_OK(create_bigint_divide_conjunct_ctxs(&_pool, &runtime_state, slot_b->id(), 20, &ctx_b));

    ColumnMaterializer::ColumnReaderMap column_readers;
    column_readers.emplace(slot_b->id(), std::make_unique<MockFilteredBigintColumnReader>(
                                                 std::vector<int64_t>{3, 0, 5, 0, 10}, std::nullopt));

    ColumnMaterializer materializer(*param, &column_readers);
    materializer.add_active_column(0);
    materializer.rebuild_read_order_ctx();
    for (auto* ctx : ctx_b) {
        materializer.add_post_read_conjunct(slot_b->id(), ctx);
    }
    ASSERT_OK(materializer.init_read_chunk());
    auto chunk = materializer.create_active_chunk();

    // The rows 1 and 3 are deleted. 100 / b > 20 keeps the row 0 only.
    Filter filter{1, 0, 1, 0, 1};
    auto hit_count = materializer.read_active_range_round_by_round(Range<uint64_t>(0, 5), &filter, &chunk, nullptr);
    ASSERT_TRUE(hit_count.ok()) << hit_count.status();
    EXPECT_EQ(1u, hit_count.value());
    const std::vector<uint8_t> expected{1, 0, 0, 0, 0};
    ASSERT_EQ(expected.size(), filter.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i], filter[i]) << "row " << i;
    }

    Expr::close(ctx_b, &runtime_state);
}

// Covers: the conjunct of a reserved field does not fail on a row that the filter dropped. It is
//         evaluated before the regular columns, on the filter that the deletion vector left.
TEST_F(GroupReaderTest, ReservedSlotConjunctSkipsRowsDeletedBeforeTheRead) {
    auto* param = _create_group_reader_param();
    param->read_cols.clear();

    auto* reserved_slot =
            _pool.add(new SlotDescriptor(273, "_pos", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    auto* active_slot =
            _pool.add(new SlotDescriptor(274, "a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    GroupReaderParam::Column ca{};
    ca.slot_desc = active_slot;
    param->read_cols.emplace_back(ca);
    param->scanner_ctx->reserved_field_slots = {reserved_slot};

    RuntimeState runtime_state = make_error_for_division_by_zero_state();
    std::vector<ExprContext*> conjunct_ctxs;
    ASSERT_OK(create_bigint_divide_conjunct_ctxs(&_pool, &runtime_state, reserved_slot->id(), 20, &conjunct_ctxs));

    ColumnMaterializer::ColumnReaderMap column_readers;
    column_readers.emplace(reserved_slot->id(), std::make_unique<MockFilteredBigintColumnReader>(
                                                        std::vector<int64_t>{3, 0, 5, 0, 10}, std::nullopt));
    column_readers.emplace(active_slot->id(), std::make_unique<MockCountingColumnReader>());

    ColumnMaterializer materializer(*param, &column_readers);
    materializer.add_active_column(0);
    materializer.rebuild_read_order_ctx();
    for (auto* ctx : conjunct_ctxs) {
        materializer.add_post_read_conjunct(reserved_slot->id(), ctx);
    }
    ASSERT_OK(materializer.init_read_chunk());
    auto chunk = materializer.create_active_chunk();

    // The rows 1 and 3 are deleted. 100 / _pos > 20 keeps the row 0 only.
    Filter filter{1, 0, 1, 0, 1};
    auto hit_count = materializer.read_active_range_round_by_round(Range<uint64_t>(0, 5), &filter, &chunk, nullptr);
    ASSERT_TRUE(hit_count.ok()) << hit_count.status();
    EXPECT_EQ(1u, hit_count.value());
    const std::vector<uint8_t> expected{1, 0, 0, 0, 0};
    ASSERT_EQ(expected.size(), filter.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i], filter[i]) << "row " << i;
    }

    Expr::close(conjunct_ctxs, &runtime_state);
}

// Covers: when the filter keeps every row the conjuncts run on the whole column.
TEST_F(GroupReaderTest, SlotConjunctKeepsEveryRowWhenFilterKeepsAll) {
    auto* param = _create_group_reader_param();
    param->read_cols.clear();

    auto* slot_b = _pool.add(new SlotDescriptor(275, "b", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    GroupReaderParam::Column cb{};
    cb.slot_desc = slot_b;
    param->read_cols.emplace_back(cb);

    RuntimeState runtime_state = make_error_for_division_by_zero_state();
    std::vector<ExprContext*> ctx_b;
    ASSERT_OK(create_bigint_divide_conjunct_ctxs(&_pool, &runtime_state, slot_b->id(), 20, &ctx_b));

    ColumnMaterializer::ColumnReaderMap column_readers;
    column_readers.emplace(slot_b->id(), std::make_unique<MockFilteredBigintColumnReader>(
                                                 std::vector<int64_t>{3, 4, 5, 6, 10}, int64_t(0)));

    ColumnMaterializer materializer(*param, &column_readers);
    materializer.add_active_column(0);
    materializer.rebuild_read_order_ctx();
    for (auto* ctx : ctx_b) {
        materializer.add_post_read_conjunct(slot_b->id(), ctx);
    }
    ASSERT_OK(materializer.init_read_chunk());
    auto chunk = materializer.create_active_chunk();

    // 100 / b is 33, 25, 20, 16, 10, so 100 / b > 20 keeps the rows 0 and 1.
    Filter filter(5, 1);
    auto hit_count = materializer.read_active_range_round_by_round(Range<uint64_t>(0, 5), &filter, &chunk, nullptr);
    ASSERT_TRUE(hit_count.ok()) << hit_count.status();
    EXPECT_EQ(2u, hit_count.value());
    const std::vector<uint8_t> expected{1, 1, 0, 0, 0};
    ASSERT_EQ(expected.size(), filter.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i], filter[i]) << "row " << i;
    }

    Expr::close(ctx_b, &runtime_state);
}

// Reads the BIGINT column b under `filter` as the only active column, and evaluates its conjuncts. A row that the
// filter drops gets `placeholder`. The conjuncts are `b = equal_to`, if it is set, and then 100 / b > 10, which fails
// on a row with b = 0. Returns the number of rows left, the filter that is left, and the number of sub-chunk copies
// that the evaluation made.
static void eval_bigint_slot_conjuncts(GroupReaderParam* param, ObjectPool* pool, SlotId slot_id,
                                       const std::vector<int64_t>& values, int64_t placeholder,
                                       std::optional<int64_t> equal_to, Filter* filter, size_t* hit_count,
                                       int64_t* subchunk_copies) {
    param->read_cols.clear();
    auto* slot =
            pool->add(new SlotDescriptor(slot_id, "b", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    GroupReaderParam::Column column{};
    column.slot_desc = slot;
    param->read_cols.emplace_back(column);

    RuntimeState runtime_state = make_error_for_division_by_zero_state();
    // Expr::create_expr_trees() clears the vector that it fills, so each helper gets its own vector.
    std::vector<ExprContext*> ctxs;
    if (equal_to.has_value()) {
        ASSERT_OK(create_bigint_eq_conjunct_ctxs(pool, &runtime_state, slot->id(), *equal_to, &ctxs));
    }
    std::vector<ExprContext*> divide_ctxs;
    ASSERT_OK(create_bigint_divide_conjunct_ctxs(pool, &runtime_state, slot->id(), 10, &divide_ctxs));
    ctxs.insert(ctxs.end(), divide_ctxs.begin(), divide_ctxs.end());
    ASSERT_EQ(equal_to.has_value() ? 2u : 1u, ctxs.size());

    ColumnMaterializer::ColumnReaderMap column_readers;
    column_readers.emplace(slot->id(), std::make_unique<MockFilteredBigintColumnReader>(values, placeholder));

    ColumnMaterializer materializer(*param, &column_readers);
    materializer.add_active_column(0);
    materializer.rebuild_read_order_ctx();
    for (auto* ctx : ctxs) {
        materializer.add_post_read_conjunct(slot->id(), ctx);
    }
    ASSERT_OK(materializer.init_read_chunk());
    auto chunk = materializer.create_active_chunk();

    auto result =
            materializer.read_active_range_round_by_round(Range<uint64_t>(0, values.size()), filter, &chunk, nullptr);
    ASSERT_TRUE(result.ok()) << result.status();
    *hit_count = result.value();
    *subchunk_copies = runtime_state.conditional_two_phase_stats()->subchunk_copies.load();

    Expr::close(ctxs, &runtime_state);
}

// Covers: the filter drops the rows 1 and 4, which hold a placeholder that the conjunct accepts. The conjunct runs
//         on the whole column, and no sub-chunk with the kept rows is copied. 100 / b > 10 keeps the rows 2, 3, 5.
TEST_F(GroupReaderTest, SlotConjunctEvaluatesWholeColumnWhenItSucceeds) {
    auto* param = _create_group_reader_param();
    Filter filter{1, 0, 1, 1, 0, 1};
    size_t hit_count = 0;
    int64_t subchunk_copies = -1;
    ASSERT_NO_FATAL_FAILURE(eval_bigint_slot_conjuncts(param, &_pool, 277, {20, 7, 5, 1, 7, 7}, 1, std::nullopt,
                                                       &filter, &hit_count, &subchunk_copies));
    EXPECT_EQ(3u, hit_count);
    const std::vector<uint8_t> expected{0, 0, 1, 1, 0, 1};
    ASSERT_EQ(expected.size(), filter.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i], filter[i]) << "row " << i;
    }
    EXPECT_EQ(0, subchunk_copies);
}

// Covers: the placeholder of a dropped row makes the conjunct fail on the whole column, so the conjunct runs again
//         on the kept rows, with one copy of them, and gives the same rows as above.
TEST_F(GroupReaderTest, SlotConjunctEvaluatesKeptRowsWhenWholeColumnFails) {
    auto* param = _create_group_reader_param();
    Filter filter{1, 0, 1, 1, 0, 1};
    size_t hit_count = 0;
    int64_t subchunk_copies = -1;
    ASSERT_NO_FATAL_FAILURE(eval_bigint_slot_conjuncts(param, &_pool, 278, {20, 7, 5, 1, 7, 7}, 0, std::nullopt,
                                                       &filter, &hit_count, &subchunk_copies));
    EXPECT_EQ(3u, hit_count);
    const std::vector<uint8_t> expected{0, 0, 1, 1, 0, 1};
    ASSERT_EQ(expected.size(), filter.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i], filter[i]) << "row " << i;
    }
    EXPECT_EQ(1, subchunk_copies);
}

// Covers: the first conjunct b = 5 runs on the whole column and clears the row 2. The second conjunct fails on the
//         placeholder 0 of the dropped rows, and the conjuncts run again on the kept rows. The row 2 stays cleared.
TEST_F(GroupReaderTest, SlotConjunctsKeepResultOfEarlierConjunctWhenWholeColumnFails) {
    auto* param = _create_group_reader_param();
    Filter filter{1, 0, 1, 1, 0, 1};
    size_t hit_count = 0;
    int64_t subchunk_copies = -1;
    ASSERT_NO_FATAL_FAILURE(eval_bigint_slot_conjuncts(param, &_pool, 279, {5, 7, 3, 5, 7, 5}, 0, int64_t(5), &filter,
                                                       &hit_count, &subchunk_copies));
    EXPECT_EQ(3u, hit_count);
    const std::vector<uint8_t> expected{1, 0, 0, 1, 0, 1};
    ASSERT_EQ(expected.size(), filter.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i], filter[i]) << "row " << i;
    }
    EXPECT_GT(subchunk_copies, 0);
}

// Covers: GroupReader::get_next with a deletion vector. The rows 3 and 7 hold b = 0, and the reader returns the
//         placeholder 0 for them, so 100 / b > 0 would fail on them. The rows that are left must come out.
TEST_F(GroupReaderTest, GetNextConjunctSkipsRowsOfDeletionVector) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;
    param->file = _create_file();
    param->chunk_size = config::vector_chunk_size;
    param->read_cols.clear();

    auto* slot_b = _pool.add(new SlotDescriptor(276, "b", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    GroupReaderParam::Column cb{};
    cb.slot_desc = slot_b;
    param->read_cols.emplace_back(cb);

    RuntimeState runtime_state = make_error_for_division_by_zero_state();
    std::vector<ExprContext*> ctx_b;
    ASSERT_OK(create_bigint_divide_conjunct_ctxs(&_pool, &runtime_state, slot_b->id(), 0, &ctx_b));
    param->conjunct_ctxs_by_slot[slot_b->id()] = ctx_b;

    roaring64_bitmap_t* bitmap = roaring64_bitmap_create();
    roaring64_bitmap_add(bitmap, 3);
    roaring64_bitmap_add(bitmap, 7);
    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    skip_rows_ctx->deletion_bitmap = std::make_shared<DeletionBitmap>(bitmap);

    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    group_reader->_column_readers.emplace(
            slot_b->id(), std::make_unique<MockFilteredBigintColumnReader>(
                                  std::vector<int64_t>{1, 2, 3, 0, 5, 6, 7, 0, 9, 10, 11, 12}, int64_t(0)));
    group_reader->_process_columns_and_conjunct_ctxs();
    ASSERT_OK(group_reader->_column_materializer->init_read_chunk());
    prepare_row_range(group_reader);

    auto chunk = _create_chunk(param);
    size_t row_count = 12;
    auto status = group_reader->get_next(&chunk, &row_count);
    ASSERT_TRUE(status.ok() || status.is_end_of_file()) << status;
    ASSERT_EQ(10u, row_count);
    const std::vector<int64_t> expected{1, 2, 3, 5, 6, 7, 9, 10, 11, 12};
    ASSERT_EQ(expected.size(), chunk->num_rows());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i], chunk->get_column_by_slot_id(slot_b->id())->get(i).get_int64()) << "row " << i;
    }

    Expr::close(ctx_b, &runtime_state);
}

// Covers: ColumnMaterializer::read_active_range_round_by_round returns as soon as a
//         reserved-slot conjunct filters out every row, without reading the regular
//         active columns with an all-zero filter.
TEST_F(GroupReaderTest, ReservedSlotConjunctFiltersAllSkipsActiveColumns) {
    auto* param = _create_group_reader_param();
    param->read_cols.clear();

    auto* reserved_slot =
            _pool.add(new SlotDescriptor(260, "_pos", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    auto* active_slot =
            _pool.add(new SlotDescriptor(261, "a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    GroupReaderParam::Column ca{};
    ca.slot_desc = active_slot;
    param->read_cols.emplace_back(ca);
    param->scanner_ctx->reserved_field_slots = {reserved_slot};

    // _pos = 42 matches none of the rows [1, 2].
    RuntimeState runtime_state{TQueryGlobals()};
    std::vector<ExprContext*> conjunct_ctxs;
    ASSERT_OK(create_bigint_eq_conjunct_ctxs(&_pool, &runtime_state, reserved_slot->id(), 42, &conjunct_ctxs));

    auto reserved_col = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
    reserved_col->append_datum(Datum(int64_t(1)));
    reserved_col->append_datum(Datum(int64_t(2)));

    ColumnMaterializer::ColumnReaderMap column_readers;
    column_readers.emplace(reserved_slot->id(), std::make_unique<MockSourceColumnReader>(reserved_col));
    auto counting_reader = std::make_unique<MockCountingColumnReader>();
    auto* active_reader = counting_reader.get();
    column_readers.emplace(active_slot->id(), std::move(counting_reader));

    ColumnMaterializer materializer(*param, &column_readers);
    materializer.add_active_column(0);
    materializer.rebuild_read_order_ctx();
    for (auto* ctx : conjunct_ctxs) {
        materializer.add_post_read_conjunct(reserved_slot->id(), ctx);
    }
    ASSERT_OK(materializer.init_read_chunk());
    auto chunk = materializer.create_active_chunk();

    Filter filter(2, 1);
    ASSIGN_OR_ABORT(size_t hit_count,
                    materializer.read_active_range_round_by_round(Range<uint64_t>(0, 2), &filter, &chunk, nullptr));
    EXPECT_EQ(0u, hit_count);
    EXPECT_EQ(0u, active_reader->read_count());
}

// Returns value(row) for each row, the row number when no function is given. A row outside the
// filter gets a placeholder, since a real reader may skip such a row and leave any value in its place.
class MockIntColumnReader : public ColumnReader {
public:
    static constexpr int32_t kPlaceholder = -1000;

    explicit MockIntColumnReader(std::function<int32_t(uint64_t)> value =
                                         [](uint64_t row) { return static_cast<int32_t>(row); })
            : ColumnReader(nullptr), _value(std::move(value)) {}
    ~MockIntColumnReader() override = default;

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst_col) override {
        ++read_count;
        read_filter_rows.push_back(filter == nullptr ? -1 : static_cast<int64_t>(SIMD::count_nonzero(*filter)));
        auto* column = dst_col->as_mutable_raw_ptr();
        for (uint64_t row = range.begin(); row < range.end(); ++row) {
            const bool selected = filter == nullptr || (*filter)[row - range.begin()] != 0;
            column->append_datum(Datum(selected ? _value(row) : kPlaceholder));
        }
        return Status::OK();
    }

    void set_need_parse_levels(bool need_parse_levels) override {}
    void get_levels(int16_t** def_levels, int16_t** rep_levels, size_t* num_levels) override {}
    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {}
    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {}

    size_t read_count = 0;
    std::vector<int64_t> read_filter_rows;

private:
    std::function<int32_t(uint64_t)> _value;
};

// "input is odd" over one INT child. It records every value it is evaluated on, so a test can
// tell which rows a compound predicate saw.
class OddIntPredicate final : public Expr {
public:
    OddIntPredicate() : Expr(TypeDescriptor(TYPE_BOOLEAN)) {}

    Expr* clone(ObjectPool* pool) const override { return pool->add(new OddIntPredicate(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override {
        ASSIGN_OR_RETURN(ColumnPtr input, get_child(0)->evaluate_checked(context, chunk));
        ColumnViewer<TYPE_INT> viewer(input);
        auto result = BooleanColumn::create();
        for (size_t i = 0; i < chunk->num_rows(); ++i) {
            const bool is_null = viewer.is_null(i);
            const int32_t value = is_null ? 0 : viewer.value(i);
            evaluated_values.push_back(value);
            result->append(static_cast<uint8_t>(!is_null && value % 2 != 0));
        }
        ColumnPtr output = std::move(result);
        return output;
    }

    std::vector<int32_t> evaluated_values;
};

// A predicate over one INT child. verdict(value) decides each row: 1 keeps it, 0 drops it and any
// other value gives NULL, which drops it too. It records every value it is evaluated on, so a test
// can tell which rows a compound predicate saw.
class IntVerdictPredicate final : public Expr {
public:
    explicit IntVerdictPredicate(std::function<int(int32_t)> verdict)
            : Expr(TypeDescriptor(TYPE_BOOLEAN)), _verdict(std::move(verdict)) {}

    Expr* clone(ObjectPool* pool) const override { return pool->add(new IntVerdictPredicate(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override {
        ASSIGN_OR_RETURN(ColumnPtr input, get_child(0)->evaluate_checked(context, chunk));
        ColumnViewer<TYPE_INT> viewer(input);
        auto values = BooleanColumn::create();
        auto nulls = NullColumn::create();
        for (size_t i = 0; i < chunk->num_rows(); ++i) {
            const int32_t value = viewer.is_null(i) ? 0 : viewer.value(i);
            evaluated_values.push_back(value);
            const int verdict = _verdict(value);
            values->append(static_cast<uint8_t>(verdict == 1));
            nulls->append(static_cast<uint8_t>(verdict != 1 && verdict != 0));
        }
        ColumnPtr output = NullableColumn::create(std::move(values), std::move(nulls));
        return output;
    }

    std::vector<int32_t> evaluated_values;

private:
    std::function<int(int32_t)> _verdict;
};

// A predicate that gives the same verdict for every row, as a constant column: 1 keeps the rows, 0
// drops them and any other value gives NULL. It records the number of rows of each evaluation.
class ConstantVerdictPredicate final : public Expr {
public:
    explicit ConstantVerdictPredicate(int verdict) : Expr(TypeDescriptor(TYPE_BOOLEAN)), _verdict(verdict) {}

    Expr* clone(ObjectPool* pool) const override { return pool->add(new ConstantVerdictPredicate(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override {
        evaluated_rows.push_back(chunk->num_rows());
        if (_verdict != 1 && _verdict != 0) {
            return ColumnHelper::create_const_null_column(chunk->num_rows());
        }
        return ColumnHelper::create_const_column<TYPE_BOOLEAN>(_verdict == 1, chunk->num_rows());
    }

    std::vector<size_t> evaluated_rows;

private:
    int _verdict;
};

// Passes the INT column of its child through and records every value that it sees, so a test can
// tell which rows a real expression tree was evaluated on. It changes no result.
class IntRecorderExpr final : public Expr {
public:
    IntRecorderExpr() : Expr(TypeDescriptor(TYPE_INT)) {}

    Expr* clone(ObjectPool* pool) const override { return pool->add(new IntRecorderExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override {
        ASSIGN_OR_RETURN(ColumnPtr column, get_child(0)->evaluate_checked(context, chunk));
        ColumnViewer<TYPE_INT> viewer(column);
        for (size_t i = 0; i < column->size(); ++i) {
            recorded_values.push_back(viewer.is_null(i) ? 0 : viewer.value(i));
        }
        return column;
    }

    std::vector<int32_t> recorded_values;
};

static constexpr SlotId kLambdaArgumentSlot = 100000;

static ColumnRef* create_int_column_ref(ObjectPool* pool, SlotId slot_id) {
    return pool->add(new ColumnRef(TYPE_INT_DESC, slot_id));
}

// array_contains(array_map(x -> x + recorder(b), [a]), target). The lambda captures b, and the
// recorder sees the values of b that the lambda reads.
static Expr* create_array_map_conjunct(ObjectPool* pool, SlotId a_slot, SlotId b_slot, int32_t target,
                                       IntRecorderExpr** recorder_out) {
    auto* recorder = pool->add(new IntRecorderExpr());
    recorder->add_child(create_int_column_ref(pool, b_slot));
    *recorder_out = recorder;

    TExprNode add_node;
    add_node.node_type = TExprNodeType::ARITHMETIC_EXPR;
    add_node.opcode = TExprOpcode::ADD;
    add_node.__isset.opcode = true;
    add_node.num_children = 2;
    add_node.type = gen_type_desc(TPrimitiveType::INT);
    auto* add = pool->add(VectorizedArithmeticExprFactory::from_thrift(add_node));
    add->add_child(create_int_column_ref(pool, kLambdaArgumentSlot));
    add->add_child(recorder);

    TExprNode lambda_node;
    lambda_node.node_type = TExprNodeType::LAMBDA_FUNCTION_EXPR;
    lambda_node.num_children = 2;
    lambda_node.type = gen_type_desc(TPrimitiveType::INT);
    auto* lambda = pool->add(new LambdaFunction(lambda_node));
    lambda->add_child(add);
    lambda->add_child(create_int_column_ref(pool, kLambdaArgumentSlot));

    TExprNode array_node;
    array_node.node_type = TExprNodeType::ARRAY_EXPR;
    array_node.num_children = 1;
    array_node.__set_is_nullable(true);
    array_node.type = TYPE_INT_ARRAY_DESC.to_thrift();
    auto* array = pool->add(ArrayExprFactory::from_thrift(array_node));
    array->add_child(create_int_column_ref(pool, a_slot));

    auto* array_map = pool->add(new ArrayMapExpr(TYPE_INT_ARRAY_DESC));
    array_map->add_child(lambda);
    array_map->add_child(array);

    TFunction fn;
    TFunctionName fn_name;
    fn_name.__set_function_name("array_contains");
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    fn.__set_has_var_args(false);
    // array_contains(ARRAY_INT, INT)
    fn.__set_fid(15000204);
    TExprNode contains_node;
    contains_node.node_type = TExprNodeType::FUNCTION_CALL;
    contains_node.num_children = 2;
    contains_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    contains_node.__set_fn(fn);
    auto* contains = pool->add(new VectorizedFunctionCallExpr(contains_node));
    contains->add_child(array_map);
    contains->add_child(
            pool->add(new VectorizedLiteral(ExprsTestHelper::create_literal<TYPE_INT, int32_t>(target, false))));
    return contains;
}

// recorder(a) <= dictmap(code), where dictmap(code) is the INT that the dictionary string of the
// code converts to. The dictionary mapping keeps its code column in code_slot, and the expression
// that it maps reads placeholder_slot.
static StatusOr<Expr*> create_dict_mapping_conjunct(ObjectPool* pool, SlotId a_slot, SlotId code_slot,
                                                    SlotId placeholder_slot, IntRecorderExpr** recorder_out) {
    TExprNode dict_node;
    dict_node.node_type = TExprNodeType::DICT_EXPR;
    dict_node.num_children = 2;
    dict_node.type = gen_type_desc(TPrimitiveType::INT);
    dict_node.__set_is_nullable(true);
    dict_node.__set_has_nullable_child(true);

    TExprNode cast_node;
    cast_node.node_type = TExprNodeType::CAST_EXPR;
    cast_node.num_children = 1;
    cast_node.type = gen_type_desc(TPrimitiveType::INT);
    cast_node.__set_child_type(TPrimitiveType::VARCHAR);
    cast_node.__set_is_nullable(true);

    TExprNode placeholder_node;
    placeholder_node.node_type = TExprNodeType::PLACEHOLDER_EXPR;
    placeholder_node.num_children = 0;
    placeholder_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
    placeholder_node.__set_is_nullable(true);
    TPlaceHolder holder;
    holder.__set_slot_id(placeholder_slot);
    holder.__set_nullable(true);
    placeholder_node.__set_vslot_ref(holder);

    TExpr dict_expr;
    dict_expr.nodes = {dict_node, ExprsTestHelper::create_slot_expr_node_t<TYPE_INT>(0, code_slot, true), cast_node,
                       placeholder_node};
    ExprContext* dict_ctx = nullptr;
    RETURN_IF_ERROR(Expr::create_expr_tree(pool, dict_expr, &dict_ctx, nullptr));

    auto* recorder = pool->add(new IntRecorderExpr());
    recorder->add_child(create_int_column_ref(pool, a_slot));
    *recorder_out = recorder;

    auto* le = pool->add(VectorizedBinaryPredicateFactory::from_thrift(
            ExprsTestHelper::create_binary_pred_node(TPrimitiveType::INT, TExprOpcode::LE)));
    le->add_child(recorder);
    le->add_child(dict_ctx->root());
    return le;
}

static Status prepare_and_open_conjunct(ObjectPool* pool, RuntimeState* state, Expr* root, ExprContext** ctx) {
    *ctx = pool->add(new ExprContext(root));
    RETURN_IF_ERROR((*ctx)->prepare(state));
    return (*ctx)->open(state);
}

static Status create_int_ge_conjunct_ctxs(ObjectPool* pool, RuntimeState* state, SlotId slot_id, int32_t value,
                                          std::vector<ExprContext*>* conjunct_ctxs) {
    TExprNode pred_node = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::INT, TExprOpcode::GE);
    TExprNode slot_ref = ExprsTestHelper::create_slot_expr_node_t<TYPE_INT>(0, slot_id, true);
    TExprNode literal = ExprsTestHelper::create_literal<TYPE_INT, int32_t>(value, false);

    TExpr expr;
    expr.nodes.emplace_back(pred_node);
    expr.nodes.emplace_back(slot_ref);
    expr.nodes.emplace_back(literal);

    std::vector<TExpr> conjunct_exprs{expr};
    RETURN_IF_ERROR(Expr::create_expr_trees(pool, conjunct_exprs, conjunct_ctxs, nullptr));
    RETURN_IF_ERROR(Expr::prepare(*conjunct_ctxs, state));
    DictOptimizeParser::disable_open_rewrite(conjunct_ctxs);
    RETURN_IF_ERROR(Expr::open(*conjunct_ctxs, state));
    return Status::OK();
}

static StatusOr<RuntimeFilterProbeDescriptor*> create_int_runtime_filter_desc(ObjectPool* pool, RuntimeState* state,
                                                                              SlotId slot_id) {
    TRuntimeFilterDescription desc;
    desc.__set_filter_id(1);
    desc.__set_has_remote_targets(false);
    desc.__set_build_plan_node_id(1);
    desc.__set_build_join_mode(TRuntimeFilterBuildJoinMode::BROADCAST);
    desc.__set_filter_type(TRuntimeFilterBuildType::TOPN_FILTER);
    TExpr col_ref = ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(slot_id, true);
    desc.__isset.plan_node_id_to_target_expr = true;
    desc.plan_node_id_to_target_expr.emplace(1, col_ref);

    auto* probe_desc = pool->add(new RuntimeFilterProbeDescriptor());
    RETURN_IF_ERROR(probe_desc->init(pool, desc, 1, state));
    return probe_desc;
}

StatusOr<CompoundScanResult> GroupReaderTest::_scan_with_compound_predicate(const CompoundScanCase& scan_case) {
    // Added to the pool first, so it is destroyed after the expression contexts that refer to it.
    auto* runtime_state = _pool.add(new RuntimeState(TQueryGlobals()));

    auto* param = _create_group_reader_param();
    _row_group_num_rows = scan_case.num_rows;
    FileMetaData* file_meta;
    RETURN_IF_ERROR(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;
    param->stats = _pool.add(new HdfsScannerStats());

    auto* slot_a = _pool.add(new SlotDescriptor(CompoundScanCase::kActiveSlot, "a", TYPE_INT_DESC));
    GroupReaderParam::Column ca{};
    ca.slot_desc = slot_a;
    param->read_cols = {ca};
    for (const auto& lazy : scan_case.lazy_columns) {
        // The constructor with a name and a type makes a slot that no query outputs.
        SlotDescriptor* slot = nullptr;
        if (lazy.is_output) {
            TSlotDescriptorBuilder builder;
            builder.id(lazy.slot_id)
                    .type(TYPE_INT_DESC)
                    .column_name(fmt::format("c{}", lazy.slot_id))
                    .is_output_column(true);
            slot = _pool.add(new SlotDescriptor(builder.build()));
        } else {
            slot = _pool.add(new SlotDescriptor(lazy.slot_id, fmt::format("c{}", lazy.slot_id), TYPE_INT_DESC));
        }
        GroupReaderParam::Column column{};
        column.slot_desc = slot;
        param->read_cols.push_back(column);
    }

    std::vector<ExprContext*> a_ctxs;
    RETURN_IF_ERROR(create_int_ge_conjunct_ctxs(&_pool, runtime_state, slot_a->id(), scan_case.active_min, &a_ctxs));
    param->conjunct_ctxs_by_slot[slot_a->id()] = a_ctxs;

    std::vector<CompoundScanCase::MakeConjunct> make_conjuncts = {scan_case.make_conjunct};
    make_conjuncts.insert(make_conjuncts.end(), scan_case.more_conjuncts.begin(), scan_case.more_conjuncts.end());
    param->scanner_ctx->conjuncts.scanner_ctxs.clear();
    for (const auto& make_conjunct : make_conjuncts) {
        ASSIGN_OR_RETURN(auto* compound, make_conjunct(&_pool, runtime_state));
        ExprContext* compound_ctx = nullptr;
        RETURN_IF_ERROR(prepare_and_open_conjunct(&_pool, runtime_state, compound, &compound_ctx));
        param->scanner_ctx->conjuncts.scanner_ctxs.push_back(compound_ctx);
    }

    RuntimeFilterProbeDescriptor* filter_desc = nullptr;
    const RuntimeFilter* late_filter = nullptr;
    std::shared_ptr<RuntimeFilterScanSink> filter_sink;
    if (!scan_case.join_values.empty()) {
        const SlotId probe_slot = scan_case.join_filter_slot;
        ASSIGN_OR_RETURN(auto* probe_desc, create_int_runtime_filter_desc(&_pool, runtime_state, probe_slot));
        filter_desc = probe_desc;
        auto* rf = _pool.add(new ComposedRuntimeBloomFilter<TYPE_INT>());
        if (scan_case.join_filter_min_max_only) {
            rf->get_membership_filter()->clear_bf();
        } else {
            rf->get_membership_filter()->init(10);
        }
        for (int32_t value : scan_case.join_values) {
            rf->insert(value);
        }
        if (scan_case.join_filter_ready) {
            probe_desc->set_runtime_filter(rf);
        } else {
            late_filter = rf;
        }
        if (scan_case.enable_filter_scan_sink) {
            filter_sink = probe_desc->enable_scan_sink();
        }
        auto& preds = param->scanner_ctx->predicates.runtime_filter_preds;
        preds = RuntimeFilterPredicates(0 /*driver_sequence*/);
        preds.add_predicate(_pool.add(new RuntimeFilterPredicate(probe_desc, probe_slot)));
        param->scanner_ctx->runtime_filter_preds = &preds;
        param->scanner_ctx->driver_sequence = 0;
    }

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    if (!scan_case.deleted_rows.empty()) {
        roaring64_bitmap_t* bitmap = roaring64_bitmap_create();
        for (uint64_t row : scan_case.deleted_rows) {
            roaring64_bitmap_add(bitmap, row);
        }
        skip_rows_ctx->deletion_bitmap = std::make_shared<DeletionBitmap>(bitmap);
    }
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    group_reader->_column_readers.emplace(slot_a->id(), std::make_unique<MockIntColumnReader>());
    std::vector<MockIntColumnReader*> lazy_readers;
    for (const auto& lazy : scan_case.lazy_columns) {
        auto reader = std::make_unique<MockIntColumnReader>(lazy.value);
        lazy_readers.push_back(reader.get());
        group_reader->_column_readers.emplace(lazy.slot_id, std::move(reader));
    }
    group_reader->_process_columns_and_conjunct_ctxs();
    // The scan reads the facts about the conjuncts from _compound_conjuncts, which init() also fills by this call.
    if (group_reader->_compound_conjuncts.size() != make_conjuncts.size()) {
        return Status::InternalError("the facts about the compound conjuncts were not collected");
    }
    RETURN_IF_ERROR(group_reader->_column_materializer->init_read_chunk());
    prepare_row_range(group_reader);

    CompoundScanResult result;
    result.conjunct_has_indirect_inputs = group_reader->_compound_conjuncts[0].has_indirect_inputs;
    result.conjunct_has_dict_mapping = group_reader->_compound_conjuncts[0].has_dict_mapping;
    result.conjunct_slots = group_reader->_compound_conjuncts[0].slots;
    result.lazy_values.resize(scan_case.lazy_columns.size());
    int reads = 0;
    while (true) {
        auto chunk = _create_chunk(param);
        size_t row_count = scan_case.rows_per_read;
        Status st = group_reader->get_next(&chunk, &row_count);
        if (!st.ok() && !st.is_end_of_file()) {
            return st;
        }
        if (late_filter != nullptr && ++reads == scan_case.install_filter_after_reads) {
            filter_desc->set_runtime_filter(late_filter);
        }
        for (size_t i = 0; i < row_count; ++i) {
            result.a_values.push_back(chunk->get_column_by_slot_id(slot_a->id())->get(i).get_int32());
            if (!scan_case.collect_lazy_values) {
                continue;
            }
            for (size_t col = 0; col < scan_case.lazy_columns.size(); ++col) {
                result.lazy_values[col].push_back(
                        chunk->get_column_by_slot_id(scan_case.lazy_columns[col].slot_id)->get(i).get_int32());
            }
        }
        if (st.is_end_of_file()) {
            break;
        }
    }
    for (auto* reader : lazy_readers) {
        result.lazy_read_counts.push_back(reader->read_count);
        result.lazy_read_filter_rows.push_back(reader->read_filter_rows);
    }
    result.stats = *param->stats;
    result.conjunct_stats = param->scanner_ctx->compound_conjunct_stats;
    if (filter_sink != nullptr) {
        result.sink_lazy_rows = filter_sink->lazy_read_rows();
        result.sink_lazy_ns = filter_sink->lazy_read_ns();
    }
    return result;
}

static int32_t row_as_value(uint64_t row) {
    return static_cast<int32_t>(row);
}

// Covers: GroupReader::_evaluate_compound_predicates evaluates the compound predicate only on
//         the rows that the column predicates kept, and reads its lazy column under that filter.
TEST_F(GroupReaderTest, CompoundPredicateSeesOnlyRowsLeftByColumnPredicates) {
    OddIntPredicate* odd = nullptr;
    CompoundScanCase scan_case;
    scan_case.active_min = 6;
    scan_case.lazy_columns = {{701, row_as_value}};
    scan_case.make_conjunct = [&odd](ObjectPool* pool, RuntimeState*) -> StatusOr<Expr*> {
        odd = pool->add(new OddIntPredicate());
        odd->add_child(create_int_column_ref(pool, 701));
        return odd;
    };

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({7, 9, 11}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({7, 9, 11}), result.lazy_values[0]);
    EXPECT_EQ(std::vector<int32_t>({6, 7, 8, 9, 10, 11}), odd->evaluated_values);
    EXPECT_FALSE(result.conjunct_has_indirect_inputs);
    EXPECT_EQ(std::vector<SlotId>({701}), result.conjunct_slots);
    EXPECT_EQ(6, result.stats.compound_pred_input_rows);
    EXPECT_EQ(3, result.stats.compound_pred_output_rows);
    EXPECT_EQ(6, result.stats.compound_pred_selected_rows);
    EXPECT_EQ(1u, result.lazy_read_counts[0]);
    EXPECT_EQ(std::vector<int64_t>({6}), result.lazy_read_filter_rows[0]);
}

// Covers: when every row passed the column predicates, the compound predicate runs on the
//         whole chunk without a copy and its lazy column is read without a filter.
TEST_F(GroupReaderTest, CompoundPredicateOnWholeChunkWhenAllRowsLeft) {
    OddIntPredicate* odd = nullptr;
    CompoundScanCase scan_case;
    scan_case.active_min = 0;
    scan_case.lazy_columns = {{701, row_as_value}};
    scan_case.make_conjunct = [&odd](ObjectPool* pool, RuntimeState*) -> StatusOr<Expr*> {
        odd = pool->add(new OddIntPredicate());
        odd->add_child(create_int_column_ref(pool, 701));
        return odd;
    };

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({1, 3, 5, 7, 9, 11}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({1, 3, 5, 7, 9, 11}), result.lazy_values[0]);
    EXPECT_EQ(std::vector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}), odd->evaluated_values);
    EXPECT_FALSE(result.conjunct_has_indirect_inputs);
    EXPECT_EQ(std::vector<SlotId>({701}), result.conjunct_slots);
    EXPECT_EQ(12, result.stats.compound_pred_input_rows);
    EXPECT_EQ(6, result.stats.compound_pred_output_rows);
    EXPECT_EQ(0, result.stats.compound_pred_selected_rows);
    EXPECT_EQ(std::vector<int64_t>({-1}), result.lazy_read_filter_rows[0]);
}

// array_contains(array_map(x -> x + b, [a]), 12) over a = row and b = 12 - row on even rows, 0 on
// odd rows: it holds on the even rows.
static CompoundScanCase create_array_map_scan_case(int32_t active_min, IntRecorderExpr** recorder) {
    CompoundScanCase scan_case;
    scan_case.active_min = active_min;
    scan_case.lazy_columns = {{701, [](uint64_t row) { return row % 2 == 0 ? static_cast<int32_t>(12 - row) : 0; }}};
    scan_case.make_conjunct = [recorder](ObjectPool* pool, RuntimeState*) -> StatusOr<Expr*> {
        return create_array_map_conjunct(pool, CompoundScanCase::kActiveSlot, 701, 12, recorder);
    };
    return scan_case;
}

// Covers: a compound conjunct with a lambda host reads the lazy column that its lambda captures
//         with Chunk::get_column_by_slot_id(), so the group reader reads that column before the
//         evaluation, under the filter of the rows that the column predicate kept.
TEST_F(GroupReaderTest, CompoundLambdaPredicateReadsCapturedLazyColumnOnSurvivingRows) {
    IntRecorderExpr* recorder = nullptr;
    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(create_array_map_scan_case(6, &recorder)));

    EXPECT_EQ(std::vector<int32_t>({6, 8, 10}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({6, 4, 2}), result.lazy_values[0]);
    // The lambda saw b of the rows 6..11 only, never the placeholder of a dropped row.
    EXPECT_EQ(std::vector<int32_t>({6, 0, 4, 0, 2, 0}), recorder->recorded_values);
    EXPECT_TRUE(result.conjunct_has_indirect_inputs);
    EXPECT_EQ(std::vector<SlotId>({CompoundScanCase::kActiveSlot, 701}), result.conjunct_slots);
    EXPECT_EQ(6, result.stats.compound_pred_input_rows);
    EXPECT_EQ(3, result.stats.compound_pred_output_rows);
    // b is read once, for the conjunct, and the backfill of the lazy columns reuses it.
    EXPECT_EQ(1u, result.lazy_read_counts[0]);
    EXPECT_EQ(std::vector<int64_t>({6}), result.lazy_read_filter_rows[0]);
}

// Covers: when no row is dropped, a compound conjunct with a lambda host gets its captured lazy
//         column read for the whole range, and the chunk it is evaluated on holds that column.
TEST_F(GroupReaderTest, CompoundLambdaPredicateReadsCapturedLazyColumnOnWholeChunk) {
    IntRecorderExpr* recorder = nullptr;
    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(create_array_map_scan_case(0, &recorder)));

    EXPECT_EQ(std::vector<int32_t>({0, 2, 4, 6, 8, 10}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({12, 10, 8, 6, 4, 2}), result.lazy_values[0]);
    EXPECT_EQ(std::vector<int32_t>({12, 0, 10, 0, 8, 0, 6, 0, 4, 0, 2, 0}), recorder->recorded_values);
    EXPECT_TRUE(result.conjunct_has_indirect_inputs);
    EXPECT_EQ(std::vector<SlotId>({CompoundScanCase::kActiveSlot, 701}), result.conjunct_slots);
    EXPECT_EQ(12, result.stats.compound_pred_input_rows);
    EXPECT_EQ(6, result.stats.compound_pred_output_rows);
    EXPECT_EQ(1u, result.lazy_read_counts[0]);
    EXPECT_EQ(std::vector<int64_t>({-1}), result.lazy_read_filter_rows[0]);
}

// Covers: get_slot_ids() of a prepared lambda host reports the slots that its lambda captures and
//         its input array reads, which are the slots that the group reader has to supply.
TEST_F(GroupReaderTest, PreparedLambdaHostReportsCapturedAndInputSlots) {
    auto* runtime_state = _pool.add(new RuntimeState(TQueryGlobals()));
    IntRecorderExpr* recorder = nullptr;
    Expr* root = create_array_map_conjunct(&_pool, CompoundScanCase::kActiveSlot, 701, 12, &recorder);
    ExprContext* ctx = nullptr;
    ASSERT_OK(prepare_and_open_conjunct(&_pool, runtime_state, root, &ctx));

    std::vector<SlotId> slots;
    root->get_slot_ids(&slots);
    std::sort(slots.begin(), slots.end());
    slots.erase(std::unique(slots.begin(), slots.end()), slots.end());
    EXPECT_EQ(std::vector<SlotId>({CompoundScanCase::kActiveSlot, 701}), slots);
    EXPECT_TRUE(root->has_indirect_inputs());
}

static constexpr SlotId kDictCodeSlot = 702;
static constexpr SlotId kDictPlaceholderSlot = 710;

// Makes the conjunct recorder(a) <= dictmap(code), with the dictionary that it needs registered in the
// runtime state. The code of the dictionary string "11 - row" is 12 - row.
static CompoundScanCase::MakeConjunct make_dict_mapping_conjunct(IntRecorderExpr** recorder) {
    return [recorder](ObjectPool* pool, RuntimeState* state) -> StatusOr<Expr*> {
        state->init_instance_mem_tracker();
        std::vector<int32_t> ids;
        std::vector<std::string> strings;
        for (int32_t code = 1; code <= 12; ++code) {
            ids.push_back(code);
            strings.push_back(std::to_string(code - 1));
        }
        TGlobalDict dict;
        dict.__set_columnId(kDictCodeSlot);
        dict.__set_ids(ids);
        dict.__set_strings(strings);
        RETURN_IF_ERROR(state->init_query_global_dict({dict}));
        return create_dict_mapping_conjunct(pool, CompoundScanCase::kActiveSlot, kDictCodeSlot, kDictPlaceholderSlot,
                                            recorder);
    };
}

// recorder(a) <= dictmap(code) over a = row, the code of the dictionary string "11 - row" is
// 12 - row: it holds on the rows 0..5.
static CompoundScanCase create_dict_mapping_scan_case(int32_t active_min, IntRecorderExpr** recorder) {
    CompoundScanCase scan_case;
    scan_case.active_min = active_min;
    scan_case.lazy_columns = {{kDictCodeSlot, [](uint64_t row) { return static_cast<int32_t>(12 - row); }}};
    scan_case.make_conjunct = make_dict_mapping_conjunct(recorder);
    return scan_case;
}

// Covers: a compound conjunct with a dictionary mapping reads its lazy code column with
//         Chunk::get_column_by_slot_id(), and get_slot_ids() of the mapping does not report that
//         slot, so the group reader reads the code column under the filter of the surviving rows.
TEST_F(GroupReaderTest, CompoundDictMappingPredicateReadsLazyCodeColumnOnSurvivingRows) {
    IntRecorderExpr* recorder = nullptr;
    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(create_dict_mapping_scan_case(3, &recorder)));

    EXPECT_EQ(std::vector<int32_t>({3, 4, 5}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({9, 8, 7}), result.lazy_values[0]);
    // The conjunct saw the rows 3..11 only. The code of a dropped row is a placeholder, which is
    // not a code of the dictionary.
    EXPECT_EQ(std::vector<int32_t>({3, 4, 5, 6, 7, 8, 9, 10, 11}), recorder->recorded_values);
    EXPECT_TRUE(result.conjunct_has_indirect_inputs);
    EXPECT_EQ(std::vector<SlotId>({CompoundScanCase::kActiveSlot, kDictCodeSlot, kDictPlaceholderSlot}),
              result.conjunct_slots);
    EXPECT_EQ(9, result.stats.compound_pred_input_rows);
    EXPECT_EQ(3, result.stats.compound_pred_output_rows);
    EXPECT_EQ(1u, result.lazy_read_counts[0]);
    EXPECT_EQ(std::vector<int64_t>({9}), result.lazy_read_filter_rows[0]);
}

// Covers: when no row is dropped, a compound conjunct with a dictionary mapping gets its lazy
//         code column read for the whole range and evaluated on the whole chunk.
TEST_F(GroupReaderTest, CompoundDictMappingPredicateReadsLazyCodeColumnOnWholeChunk) {
    IntRecorderExpr* recorder = nullptr;
    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(create_dict_mapping_scan_case(0, &recorder)));

    EXPECT_EQ(std::vector<int32_t>({0, 1, 2, 3, 4, 5}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({12, 11, 10, 9, 8, 7}), result.lazy_values[0]);
    EXPECT_EQ(std::vector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}), recorder->recorded_values);
    EXPECT_TRUE(result.conjunct_has_indirect_inputs);
    EXPECT_EQ(std::vector<SlotId>({CompoundScanCase::kActiveSlot, kDictCodeSlot, kDictPlaceholderSlot}),
              result.conjunct_slots);
    EXPECT_EQ(12, result.stats.compound_pred_input_rows);
    EXPECT_EQ(6, result.stats.compound_pred_output_rows);
    EXPECT_EQ(1u, result.lazy_read_counts[0]);
    EXPECT_EQ(std::vector<int64_t>({-1}), result.lazy_read_filter_rows[0]);
}

// Covers: slot_id() of a prepared dictionary mapping is its code column, while get_slot_ids()
//         reports the slot of the mapped expression.
TEST_F(GroupReaderTest, PreparedDictMappingKeepsCodeColumnApartFromReportedSlots) {
    auto* runtime_state = _pool.add(new RuntimeState(TQueryGlobals()));
    runtime_state->init_instance_mem_tracker();
    TGlobalDict dict;
    dict.__set_columnId(kDictCodeSlot);
    dict.__set_ids({1});
    dict.__set_strings({"0"});
    ASSERT_OK(runtime_state->init_query_global_dict({dict}));

    IntRecorderExpr* recorder = nullptr;
    ASSIGN_OR_ABORT(auto* root, create_dict_mapping_conjunct(&_pool, CompoundScanCase::kActiveSlot, kDictCodeSlot,
                                                             kDictPlaceholderSlot, &recorder));
    ExprContext* ctx = nullptr;
    ASSERT_OK(prepare_and_open_conjunct(&_pool, runtime_state, root, &ctx));

    auto* dict_mapping = down_cast<DictMappingExpr*>(root->get_child(1));
    EXPECT_EQ(kDictCodeSlot, dict_mapping->slot_id());
    std::vector<SlotId> slots;
    root->get_slot_ids(&slots);
    std::sort(slots.begin(), slots.end());
    EXPECT_EQ(std::vector<SlotId>({CompoundScanCase::kActiveSlot, kDictPlaceholderSlot}), slots);
    EXPECT_TRUE(root->has_indirect_inputs());
}

// "input is odd" over the lazy column 701, on top of the setup of CompoundScanCase. The test reads
// the values that the predicate was evaluated on from *odd after the scan.
static CompoundScanCase create_odd_scan_case(int32_t active_min, OddIntPredicate** odd) {
    CompoundScanCase scan_case;
    scan_case.active_min = active_min;
    scan_case.lazy_columns = {{701, row_as_value}};
    scan_case.make_conjunct = [odd](ObjectPool* pool, RuntimeState*) -> StatusOr<Expr*> {
        *odd = pool->add(new OddIntPredicate());
        (*odd)->add_child(create_int_column_ref(pool, 701));
        return *odd;
    };
    return scan_case;
}

// Covers: a join runtime filter that has arrived runs before the compound predicate on the lazy
//         column that it probes, so the predicate sees only the rows that the filter kept. The
//         column is read once, under the filter of the rows left by the column predicate, and the
//         compound predicate and the backfill of the lazy columns reuse it.
TEST_F(GroupReaderTest, JoinRuntimeFilterOnLazyColumnRunsBeforeCompoundPredicate) {
    OddIntPredicate* odd = nullptr;
    CompoundScanCase scan_case = create_odd_scan_case(2, &odd);
    scan_case.join_values = {9, 10};
    scan_case.join_filter_slot = 701;

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({9}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({9}), result.lazy_values[0]);
    EXPECT_EQ(10, result.stats.rf_cond_input_rows);
    EXPECT_EQ(2, result.stats.rf_cond_output_rows);
    EXPECT_EQ(std::vector<int32_t>({9, 10}), odd->evaluated_values);
    EXPECT_EQ(2, result.stats.compound_pred_input_rows);
    EXPECT_EQ(1, result.stats.compound_pred_output_rows);
    EXPECT_EQ(1u, result.lazy_read_counts[0]);
    EXPECT_EQ(std::vector<int64_t>({10}), result.lazy_read_filter_rows[0]);
}

// Covers: a join runtime filter on the active column runs before the compound predicate, and the
//         lazy column that the compound predicate reads is read under the filter of the rows that
//         the join filter kept.
TEST_F(GroupReaderTest, JoinRuntimeFilterOnActiveColumnRunsBeforeCompoundPredicate) {
    OddIntPredicate* odd = nullptr;
    CompoundScanCase scan_case = create_odd_scan_case(2, &odd);
    scan_case.join_values = {9, 10};
    scan_case.join_filter_slot = CompoundScanCase::kActiveSlot;

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({9}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({9}), result.lazy_values[0]);
    EXPECT_EQ(10, result.stats.rf_cond_input_rows);
    EXPECT_EQ(2, result.stats.rf_cond_output_rows);
    EXPECT_EQ(std::vector<int32_t>({9, 10}), odd->evaluated_values);
    EXPECT_EQ(2, result.stats.compound_pred_input_rows);
    EXPECT_EQ(1, result.stats.compound_pred_output_rows);
    EXPECT_EQ(1u, result.lazy_read_counts[0]);
    EXPECT_EQ(std::vector<int64_t>({2}), result.lazy_read_filter_rows[0]);
}

// Covers: when the join runtime filter has not arrived, the compound predicate runs first, on the
//         rows that the column predicate kept, and the join filter probes nothing.
TEST_F(GroupReaderTest, CompoundPredicateRunsFirstWhenJoinRuntimeFilterNotArrived) {
    OddIntPredicate* odd = nullptr;
    CompoundScanCase scan_case = create_odd_scan_case(2, &odd);
    scan_case.join_values = {9, 10};
    scan_case.join_filter_slot = 701;
    scan_case.join_filter_ready = false;

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({3, 5, 7, 9, 11}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({3, 5, 7, 9, 11}), result.lazy_values[0]);
    EXPECT_EQ(std::vector<int32_t>({2, 3, 4, 5, 6, 7, 8, 9, 10, 11}), odd->evaluated_values);
    EXPECT_EQ(10, result.stats.compound_pred_input_rows);
    EXPECT_EQ(5, result.stats.compound_pred_output_rows);
    EXPECT_EQ(0, result.stats.rf_cond_input_rows);
    EXPECT_EQ(1u, result.lazy_read_counts[0]);
    EXPECT_EQ(std::vector<int64_t>({10}), result.lazy_read_filter_rows[0]);
}

// Covers: a join runtime filter that the build side builds after the scan has started is found by the group
//         reader through the descriptor. The pass that reads before the filter exists runs as it did without a
//         filter, and the passes after it probe the filter before the compound predicate.
TEST_F(GroupReaderTest, JoinRuntimeFilterBuiltAfterTheScanStartedIsAppliedFromTheNextRead) {
    OddIntPredicate* odd = nullptr;
    CompoundScanCase scan_case = create_odd_scan_case(0, &odd);
    scan_case.rows_per_read = 4;
    scan_case.join_values = {9, 10};
    scan_case.join_filter_slot = 701;
    scan_case.join_filter_ready = false;
    scan_case.install_filter_after_reads = 1;

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    // The rows 0..3 are read before the filter exists. The filter drops the rows 4..7 and keeps 9 and 10 of the
    // rows 8..11, and the compound predicate keeps the odd one.
    EXPECT_EQ(std::vector<int32_t>({1, 3, 9}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({1, 3, 9}), result.lazy_values[0]);
    EXPECT_EQ(8, result.stats.rf_cond_input_rows);
    EXPECT_EQ(2, result.stats.rf_cond_output_rows);
    // The compound predicate saw the whole first pass, and only the rows that the filter kept in the last pass.
    EXPECT_EQ(std::vector<int32_t>({0, 1, 2, 3, 9, 10}), odd->evaluated_values);
}

// Covers: a join runtime filter that has its min/max range only, as a filter that the build side could not afford the
//         Bloom filter for, drops the values outside the range from the next read and keeps the ones inside it.
TEST_F(GroupReaderTest, MinMaxOnlyJoinRuntimeFilterBuiltAfterTheScanStartedDropsTheValuesOutsideItsRange) {
    OddIntPredicate* odd = nullptr;
    CompoundScanCase scan_case = create_odd_scan_case(0, &odd);
    scan_case.rows_per_read = 4;
    scan_case.join_values = {9, 11};
    scan_case.join_filter_min_max_only = true;
    scan_case.join_filter_slot = 701;
    scan_case.join_filter_ready = false;
    scan_case.install_filter_after_reads = 1;

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    // The rows 0..3 are read before the filter exists. The range 9..11 drops the rows 4..7 and 8 and keeps 9, 10 and 11,
    // and the compound predicate keeps the odd ones. A Bloom filter of 9 and 11 would not keep 10.
    EXPECT_EQ(std::vector<int32_t>({1, 3, 9, 11}), result.a_values);
    EXPECT_EQ(8, result.stats.rf_cond_input_rows);
    EXPECT_EQ(3, result.stats.rf_cond_output_rows);
    EXPECT_EQ(std::vector<int32_t>({0, 1, 2, 3, 9, 10, 11}), odd->evaluated_values);
}

// Covers: a join runtime filter that never arrives is never probed, and every pass runs as it does without a
//         filter.
TEST_F(GroupReaderTest, JoinRuntimeFilterThatNeverArrivesIsNeverProbed) {
    OddIntPredicate* odd = nullptr;
    CompoundScanCase scan_case = create_odd_scan_case(0, &odd);
    scan_case.rows_per_read = 4;
    scan_case.join_values = {9, 10};
    scan_case.join_filter_slot = 701;
    scan_case.join_filter_ready = false;
    scan_case.install_filter_after_reads = -1;

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({1, 3, 5, 7, 9, 11}), result.a_values);
    EXPECT_EQ(0, result.stats.rf_cond_input_rows);
}

// Covers: a lazy column that the group reader backfills for the rows left is reported to the sink of the join
//         filter, which a filter built later uses to estimate what a dropped row costs.
TEST_F(GroupReaderTest, ReaderReportsLazyColumnCostToTheSinkOfTheJoinFilter) {
    OddIntPredicate* odd = nullptr;
    CompoundScanCase scan_case = create_odd_scan_case(2, &odd);
    // An output column that no conjunct reads, so the group reader reads it only for the rows that are left.
    scan_case.lazy_columns.push_back({702, row_as_value, true});
    scan_case.join_values = {9, 10};
    scan_case.join_filter_slot = CompoundScanCase::kActiveSlot;
    scan_case.enable_filter_scan_sink = true;

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({9}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({9}), result.lazy_values[1]);
    EXPECT_EQ(1, result.sink_lazy_rows);
    EXPECT_GE(result.sink_lazy_ns, 0);
}

// Covers: when the join runtime filter drops every row, it ends the pass before the compound
//         predicate, which is never evaluated.
TEST_F(GroupReaderTest, JoinRuntimeFilterDroppingAllRowsSkipsCompoundPredicate) {
    OddIntPredicate* odd = nullptr;
    CompoundScanCase scan_case = create_odd_scan_case(2, &odd);
    scan_case.join_values = {100};
    scan_case.join_filter_slot = 701;

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_TRUE(result.a_values.empty());
    EXPECT_TRUE(odd->evaluated_values.empty());
    EXPECT_EQ(10, result.stats.rf_cond_input_rows);
    EXPECT_EQ(0, result.stats.rf_cond_output_rows);
    EXPECT_EQ(0, result.stats.compound_pred_input_rows);
    EXPECT_EQ(12, result.stats.late_materialize_skip_rows);
}

// Covers: a join runtime filter that drops rows runs before a compound conjunct with a lambda host
//         that captures the lazy column that the filter probes. The conjunct runs only on the rows
//         that the filter kept and gets the real values of that column, and the column is read
//         once, for the join filter.
TEST_F(GroupReaderTest, JoinRuntimeFilterBeforeCompoundLambdaPredicateOnLazyProbeColumn) {
    IntRecorderExpr* recorder = nullptr;
    CompoundScanCase scan_case = create_array_map_scan_case(2, &recorder);
    // b is 12 - row on the even rows and 0 on the odd rows. Of the rows 2..11 that the column
    // predicate kept, the filter drops the rows 2 and 4, where b is 10 and 8.
    scan_case.join_values = {0, 2, 4, 6};
    scan_case.join_filter_slot = 701;

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({6, 8, 10}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({6, 4, 2}), result.lazy_values[0]);
    EXPECT_EQ(10, result.stats.rf_cond_input_rows);
    EXPECT_EQ(8, result.stats.rf_cond_output_rows);
    // The lambda saw b of the rows 3 and 5..11, never the value of a dropped row or the placeholder
    // of the rows 0 and 1.
    EXPECT_EQ(std::vector<int32_t>({0, 0, 6, 0, 4, 0, 2, 0}), recorder->recorded_values);
    EXPECT_TRUE(result.conjunct_has_indirect_inputs);
    EXPECT_EQ(8, result.stats.compound_pred_input_rows);
    EXPECT_EQ(3, result.stats.compound_pred_output_rows);
    EXPECT_EQ(1u, result.lazy_read_counts[0]);
    EXPECT_EQ(std::vector<int64_t>({10}), result.lazy_read_filter_rows[0]);
}

// Covers: the same for a compound conjunct with a dictionary mapping, whose lazy code column is
//         the column that the join filter probes.
TEST_F(GroupReaderTest, JoinRuntimeFilterBeforeCompoundDictMappingPredicateOnLazyProbeColumn) {
    IntRecorderExpr* recorder = nullptr;
    CompoundScanCase scan_case = create_dict_mapping_scan_case(3, &recorder);
    // The code is 12 - row. Of the rows 3..11 that the column predicate kept, the filter keeps the
    // rows 4..8, where the code is 8..4.
    scan_case.join_values = {4, 5, 6, 7, 8};
    scan_case.join_filter_slot = kDictCodeSlot;

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({4, 5}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({8, 7}), result.lazy_values[0]);
    EXPECT_EQ(9, result.stats.rf_cond_input_rows);
    EXPECT_EQ(5, result.stats.rf_cond_output_rows);
    EXPECT_EQ(std::vector<int32_t>({4, 5, 6, 7, 8}), recorder->recorded_values);
    EXPECT_TRUE(result.conjunct_has_indirect_inputs);
    EXPECT_EQ(5, result.stats.compound_pred_input_rows);
    EXPECT_EQ(2, result.stats.compound_pred_output_rows);
    EXPECT_EQ(1u, result.lazy_read_counts[0]);
    EXPECT_EQ(std::vector<int64_t>({9}), result.lazy_read_filter_rows[0]);
}

// Covers: when the join runtime filter runs first and drops no row, and the column predicate
//         dropped none either, every row is a surviving row, so the compound predicate runs on the
//         whole chunk. It gets the lazy column that the join filter read.
TEST_F(GroupReaderTest, CompoundPredicateOnWholeChunkWhenJoinRuntimeFilterDropsNoRow) {
    OddIntPredicate* odd = nullptr;
    CompoundScanCase scan_case = create_odd_scan_case(0, &odd);
    scan_case.join_values = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    scan_case.join_filter_slot = 701;

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({1, 3, 5, 7, 9, 11}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({1, 3, 5, 7, 9, 11}), result.lazy_values[0]);
    EXPECT_EQ(12, result.stats.rf_cond_input_rows);
    EXPECT_EQ(12, result.stats.rf_cond_output_rows);
    EXPECT_EQ(std::vector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}), odd->evaluated_values);
    EXPECT_EQ(12, result.stats.compound_pred_input_rows);
    EXPECT_EQ(6, result.stats.compound_pred_output_rows);
    EXPECT_EQ(1u, result.lazy_read_counts[0]);
    EXPECT_EQ(std::vector<int64_t>({12}), result.lazy_read_filter_rows[0]);
}

// Covers: the same for a compound conjunct with a lambda host that captures the lazy column that
//         the join filter probes: the conjunct runs on the whole chunk, and the column is the one
//         that the join filter read, not read a second time.
TEST_F(GroupReaderTest, CompoundLambdaPredicateOnWholeChunkWhenJoinRuntimeFilterDropsNoRow) {
    IntRecorderExpr* recorder = nullptr;
    CompoundScanCase scan_case = create_array_map_scan_case(0, &recorder);
    // The filter holds every value of b: 12 - row on the even rows and 0 on the odd rows.
    scan_case.join_values = {0, 2, 4, 6, 8, 10, 12};
    scan_case.join_filter_slot = 701;

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({0, 2, 4, 6, 8, 10}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({12, 10, 8, 6, 4, 2}), result.lazy_values[0]);
    EXPECT_EQ(12, result.stats.rf_cond_input_rows);
    EXPECT_EQ(12, result.stats.rf_cond_output_rows);
    EXPECT_EQ(std::vector<int32_t>({12, 0, 10, 0, 8, 0, 6, 0, 4, 0, 2, 0}), recorder->recorded_values);
    EXPECT_TRUE(result.conjunct_has_indirect_inputs);
    EXPECT_EQ(12, result.stats.compound_pred_input_rows);
    EXPECT_EQ(6, result.stats.compound_pred_output_rows);
    EXPECT_EQ(1u, result.lazy_read_counts[0]);
    EXPECT_EQ(std::vector<int64_t>({12}), result.lazy_read_filter_rows[0]);
}

// The values in [from, to) that keep() accepts, in ascending order.
static std::vector<int32_t> values_where(int32_t from, int32_t to, const std::function<bool(int32_t)>& keep) {
    std::vector<int32_t> values;
    for (int32_t value = from; value < to; ++value) {
        if (keep(value)) {
            values.push_back(value);
        }
    }
    return values;
}

// Makes a compound conjunct that applies verdict to the column of slot_id, the lazy column 701 unless the
// test says otherwise, and returns the predicate in *predicate when the scan creates it.
static CompoundScanCase::MakeConjunct make_int_verdict_conjunct(IntVerdictPredicate** predicate,
                                                                std::function<int(int32_t)> verdict,
                                                                SlotId slot_id = 701) {
    return [predicate, verdict = std::move(verdict), slot_id](ObjectPool* pool, RuntimeState*) -> StatusOr<Expr*> {
        *predicate = pool->add(new IntVerdictPredicate(verdict));
        (*predicate)->add_child(create_int_column_ref(pool, slot_id));
        return *predicate;
    };
}

// Covers: the rows that a compound predicate keeps are the rows that the group reader outputs,
//         whether the predicate fails no row, a few rows, most rows or every row, with false or with
//         NULL. The row group has more rows than one SIMD batch of the row bookkeeping holds, and the
//         counters show that the selected rows were evaluated once.
TEST_F(GroupReaderTest, CompoundPredicateVerdictsOnManySelectedRows) {
    struct Verdict {
        const char* name;
        std::function<int(int32_t)> verdict;
    };
    const std::vector<Verdict> verdicts = {
            {"no row fails", [](int32_t) { return 1; }},
            {"one row fails and one is NULL", [](int32_t value) { return value == 50 ? 0 : (value == 77 ? -1 : 1); }},
            {"most rows fail or are NULL",
             [](int32_t value) { return value % 3 == 0 ? 1 : (value % 3 == 1 ? 0 : -1); }},
            {"every row fails", [](int32_t) { return 0; }},
            {"every row is NULL", [](int32_t) { return -1; }},
    };
    for (const auto& entry : verdicts) {
        const auto& verdict = entry.verdict;
        SCOPED_TRACE(entry.name);
        // The rows 10..99 pass the column predicate.
        IntVerdictPredicate* predicate = nullptr;
        CompoundScanCase scan_case;
        scan_case.num_rows = 100;
        scan_case.rows_per_read = 100;
        scan_case.active_min = 10;
        scan_case.lazy_columns = {{701, row_as_value}};
        scan_case.make_conjunct = make_int_verdict_conjunct(&predicate, verdict);

        ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
        const auto kept = values_where(10, 100, [&verdict](int32_t value) { return verdict(value) == 1; });
        EXPECT_EQ(kept, result.a_values);
        EXPECT_EQ(kept, result.lazy_values[0]);
        EXPECT_EQ(values_where(10, 100, [](int32_t) { return true; }), predicate->evaluated_values);
        EXPECT_EQ(90, result.stats.compound_pred_input_rows);
        EXPECT_EQ(90, result.stats.compound_pred_selected_rows);
        EXPECT_EQ(static_cast<int64_t>(kept.size()), result.stats.compound_pred_output_rows);
    }
}

// Covers: a compound predicate that gives the same verdict for every row, as a constant column,
//         keeps all selected rows or drops all of them.
TEST_F(GroupReaderTest, CompoundPredicateWithConstantVerdictOnSelectedRows) {
    for (int verdict : {1, 0, -1}) {
        SCOPED_TRACE(verdict);
        ConstantVerdictPredicate* predicate = nullptr;
        CompoundScanCase scan_case;
        scan_case.active_min = 1;
        // The conjunct reads no slot, so nothing reads the lazy column 701 and the group reader fills it
        // with NULL. We do not collect the values of a NULL column.
        scan_case.lazy_columns = {{701, row_as_value}};
        scan_case.collect_lazy_values = false;
        scan_case.make_conjunct = [&predicate, verdict](ObjectPool* pool, RuntimeState*) -> StatusOr<Expr*> {
            predicate = pool->add(new ConstantVerdictPredicate(verdict));
            return predicate;
        };

        ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
        EXPECT_EQ(std::vector<size_t>({11}), predicate->evaluated_rows);
        EXPECT_EQ(11, result.stats.compound_pred_input_rows);
        EXPECT_EQ(11, result.stats.compound_pred_selected_rows);
        if (verdict == 1) {
            EXPECT_EQ(values_where(1, 12, [](int32_t) { return true; }), result.a_values);
            EXPECT_EQ(11, result.stats.compound_pred_output_rows);
        } else {
            EXPECT_TRUE(result.a_values.empty());
            EXPECT_EQ(0, result.stats.compound_pred_output_rows);
            EXPECT_EQ(12, result.stats.late_materialize_skip_rows);
        }
    }
}

// Covers: the second compound conjunct is evaluated only on the rows that the first one kept, and
//         the rows count once in the counters.
TEST_F(GroupReaderTest, SecondCompoundPredicateSeesOnlyRowsKeptByTheFirst) {
    IntVerdictPredicate* first = nullptr;
    IntVerdictPredicate* second = nullptr;
    CompoundScanCase scan_case;
    scan_case.num_rows = 100;
    scan_case.rows_per_read = 100;
    scan_case.active_min = 10;
    scan_case.lazy_columns = {{701, row_as_value}};
    // The first conjunct fails the multiples of 10, which are few of the selected rows, and the
    // second one the multiples of 3.
    scan_case.make_conjunct = make_int_verdict_conjunct(&first, [](int32_t value) { return value % 10 == 0 ? 0 : 1; });
    scan_case.more_conjuncts = {
            make_int_verdict_conjunct(&second, [](int32_t value) { return value % 3 == 0 ? 0 : 1; })};

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    const auto kept_by_first = values_where(10, 100, [](int32_t value) { return value % 10 != 0; });
    const auto kept = values_where(10, 100, [](int32_t value) { return value % 10 != 0 && value % 3 != 0; });
    EXPECT_EQ(kept, result.a_values);
    EXPECT_EQ(kept, result.lazy_values[0]);
    EXPECT_EQ(values_where(10, 100, [](int32_t) { return true; }), first->evaluated_values);
    EXPECT_EQ(kept_by_first, second->evaluated_values);
    EXPECT_EQ(90, result.stats.compound_pred_input_rows);
    EXPECT_EQ(90, result.stats.compound_pred_selected_rows);
    EXPECT_EQ(static_cast<int64_t>(kept.size()), result.stats.compound_pred_output_rows);
}

// Sets the configs of the deferral of compound conjuncts for the scope of a test and restores them
// afterwards.
class ScopedDeferConfig {
public:
    ScopedDeferConfig(bool enable, double min_keep_ratio, int64_t min_observed_rows)
            : _enable(config::parquet_compound_predicate_defer_enable),
              _min_keep_ratio(config::parquet_compound_predicate_defer_min_keep_ratio),
              _min_observed_rows(config::parquet_compound_predicate_defer_min_observed_rows) {
        config::parquet_compound_predicate_defer_enable = enable;
        config::parquet_compound_predicate_defer_min_keep_ratio = min_keep_ratio;
        config::parquet_compound_predicate_defer_min_observed_rows = min_observed_rows;
    }

    ~ScopedDeferConfig() {
        config::parquet_compound_predicate_defer_enable = _enable;
        config::parquet_compound_predicate_defer_min_keep_ratio = _min_keep_ratio;
        config::parquet_compound_predicate_defer_min_observed_rows = _min_observed_rows;
    }

private:
    bool _enable;
    double _min_keep_ratio;
    int64_t _min_observed_rows;
};

// Covers: a deferred compound conjunct drops every row of the first pass while rows remain in the row
//         group. The next pass reads the next rows instead of an empty range, so the scan ends, and
//         the result is the same as without deferral.
TEST_F(GroupReaderTest, DeferredCompoundPredicateDropsWholePass) {
    ScopedDeferConfig defer(true, 0, 0);
    OddIntPredicate* odd = nullptr;
    // The passes are the rows {0, 1, 2}, {3, 4, 5}, ... The column predicate keeps only the row 2 of the
    // first pass, so the conjunct is deferred there, and it drops the row 2, which is even.
    CompoundScanCase scan_case = create_odd_scan_case(2, &odd);
    scan_case.rows_per_read = 3;

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({3, 5, 7, 9, 11}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({3, 5, 7, 9, 11}), result.lazy_values[0]);
    // The conjunct saw the row 2 in the output chunk, and the rows of the other passes in the reader.
    EXPECT_EQ(std::vector<int32_t>({2, 3, 4, 5, 6, 7, 8, 9, 10, 11}), odd->evaluated_values);
    EXPECT_EQ(1, result.stats.compound_pred_deferred_rows);
    EXPECT_EQ(0, result.stats.compound_pred_selected_rows);
    EXPECT_EQ(10, result.stats.compound_pred_input_rows);
    EXPECT_EQ(5, result.stats.compound_pred_output_rows);
    // The first pass reads the column for the deferred conjunct, under the filter of the row left.
    EXPECT_EQ(std::vector<int64_t>({1, -1, -1, -1}), result.lazy_read_filter_rows[0]);
}

// Covers: a deferred compound conjunct runs on the output chunk, whose rows are the rows that the
//         earlier stages kept, and the lazy column that only it reads has real values there. The result
//         is the same as without deferral, and the counters tell where the rows were evaluated.
TEST_F(GroupReaderTest, DeferredCompoundPredicateRunsOnOutputChunk) {
    auto scan = [this](bool defer, OddIntPredicate** odd) {
        ScopedDeferConfig defer_config(defer, 0, 0);
        ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(create_odd_scan_case(6, odd)));
        return result;
    };
    OddIntPredicate* reference_odd = nullptr;
    OddIntPredicate* deferred_odd = nullptr;
    const auto reference = scan(false, &reference_odd);
    const auto deferred = scan(true, &deferred_odd);

    EXPECT_EQ(std::vector<int32_t>({7, 9, 11}), reference.a_values);
    EXPECT_EQ(std::vector<int32_t>({7, 9, 11}), reference.lazy_values[0]);
    EXPECT_EQ(std::vector<int32_t>({6, 7, 8, 9, 10, 11}), reference_odd->evaluated_values);
    EXPECT_EQ(0, reference.stats.compound_pred_deferred_rows);
    EXPECT_EQ(6, reference.stats.compound_pred_selected_rows);
    EXPECT_EQ(6, reference.stats.compound_pred_input_rows);
    EXPECT_EQ(3, reference.stats.compound_pred_output_rows);

    EXPECT_EQ(reference.a_values, deferred.a_values);
    EXPECT_EQ(reference.lazy_values, deferred.lazy_values);
    EXPECT_EQ(reference_odd->evaluated_values, deferred_odd->evaluated_values);
    EXPECT_EQ(6, deferred.stats.compound_pred_deferred_rows);
    EXPECT_EQ(0, deferred.stats.compound_pred_selected_rows);
    EXPECT_EQ(6, deferred.stats.compound_pred_input_rows);
    EXPECT_EQ(3, deferred.stats.compound_pred_output_rows);
    // The column is read once, for the rows that the column predicate kept.
    EXPECT_EQ(1u, deferred.lazy_read_counts[0]);
    EXPECT_EQ(std::vector<int64_t>({6}), deferred.lazy_read_filter_rows[0]);
    ASSERT_EQ(1u, deferred.conjunct_stats.size());
    EXPECT_EQ(6, deferred.conjunct_stats[0].input_rows);
    EXPECT_EQ(3, deferred.conjunct_stats[0].output_rows);
}

// Covers: when no row of the pass is dropped, the conjunct runs on the whole chunk in the reader, and
//         its statistics still count the rows that it saw and kept.
TEST_F(GroupReaderTest, CompoundPredicateIsNotDeferredWhenNoRowIsDropped) {
    ScopedDeferConfig defer(true, 0, 0);
    OddIntPredicate* odd = nullptr;
    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(create_odd_scan_case(0, &odd)));

    EXPECT_EQ(std::vector<int32_t>({1, 3, 5, 7, 9, 11}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}), odd->evaluated_values);
    EXPECT_EQ(0, result.stats.compound_pred_deferred_rows);
    EXPECT_EQ(0, result.stats.compound_pred_selected_rows);
    EXPECT_EQ(12, result.stats.compound_pred_input_rows);
    EXPECT_EQ(6, result.stats.compound_pred_output_rows);
    ASSERT_EQ(1u, result.conjunct_stats.size());
    EXPECT_EQ(12, result.conjunct_stats[0].input_rows);
    EXPECT_EQ(6, result.conjunct_stats[0].output_rows);
}

// Covers: a compound conjunct with a lambda host follows its statistics as any conjunct does when the
//         query outputs a lazy column that the lambda does not read. It runs in the reader until it has
//         seen the rows that the config asks for and kept enough of them, and it is deferred after that,
//         so it needs its captured lazy column read for the output chunk. When it keeps too few rows, it
//         is never deferred.
TEST_F(GroupReaderTest, DeferredCompoundLambdaPredicateFollowsItsStatistics) {
    // The passes are the rows {0, 1, 2}, {3, 4, 5}, ... and the deletion vector drops the first row of
    // each pass. The lambda keeps the even rows: of the rows 1 and 2 it keeps the row 2, of the rows 4 and
    // 5 the row 4, and so on, one of two rows. The lambda does not read the output column 703, and
    // deferring the conjunct would read that column for rows that the conjunct drops.
    auto scan = [this](double min_keep_ratio, IntRecorderExpr** recorder) {
        ScopedDeferConfig defer(true, min_keep_ratio, 4);
        CompoundScanCase scan_case = create_array_map_scan_case(0, recorder);
        scan_case.lazy_columns.push_back({703, row_as_value, true});
        scan_case.rows_per_read = 3;
        scan_case.deleted_rows = {0, 3, 6, 9};
        ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
        return result;
    };

    // It kept every second row, which is enough for 0, so the passes {6, 7, 8} and {9, 10, 11} defer it
    // after the first two passes have shown 4 rows.
    IntRecorderExpr* recorder = nullptr;
    const auto deferred = scan(0, &recorder);
    EXPECT_EQ(std::vector<int32_t>({2, 4, 8, 10}), deferred.a_values);
    EXPECT_EQ(std::vector<int32_t>({10, 8, 4, 2}), deferred.lazy_values[0]);
    EXPECT_EQ(std::vector<int32_t>({2, 4, 8, 10}), deferred.lazy_values[1]);
    // The conjunct saw only the rows that the deletion vector kept, in the reader and in the output chunk.
    EXPECT_EQ(std::vector<int32_t>({0, 10, 8, 0, 0, 4, 2, 0}), recorder->recorded_values);
    EXPECT_TRUE(deferred.conjunct_has_indirect_inputs);
    EXPECT_FALSE(deferred.conjunct_has_dict_mapping);
    EXPECT_EQ(4, deferred.stats.compound_pred_selected_rows);
    EXPECT_EQ(4, deferred.stats.compound_pred_deferred_rows);
    EXPECT_EQ(8, deferred.stats.compound_pred_input_rows);
    EXPECT_EQ(4, deferred.stats.compound_pred_output_rows);
    EXPECT_EQ(std::vector<int64_t>({2, 2, 2, 2}), deferred.lazy_read_filter_rows[0]);
    ASSERT_EQ(1u, deferred.conjunct_stats.size());
    EXPECT_EQ(8, deferred.conjunct_stats[0].input_rows);
    EXPECT_EQ(4, deferred.conjunct_stats[0].output_rows);

    // The same scan with a keep ratio that the conjunct does not reach never defers it, and the rows
    // are the same.
    IntRecorderExpr* strict_recorder = nullptr;
    const auto in_reader = scan(0.9, &strict_recorder);
    EXPECT_EQ(deferred.a_values, in_reader.a_values);
    EXPECT_EQ(deferred.lazy_values, in_reader.lazy_values);
    EXPECT_EQ(recorder->recorded_values, strict_recorder->recorded_values);
    EXPECT_EQ(8, in_reader.stats.compound_pred_selected_rows);
    EXPECT_EQ(0, in_reader.stats.compound_pred_deferred_rows);
    EXPECT_EQ(8, in_reader.stats.compound_pred_input_rows);
    EXPECT_EQ(4, in_reader.stats.compound_pred_output_rows);
    ASSERT_EQ(1u, in_reader.conjunct_stats.size());
    EXPECT_EQ(8, in_reader.conjunct_stats[0].input_rows);
    EXPECT_EQ(4, in_reader.conjunct_stats[0].output_rows);
}

// Covers: a compound conjunct with a dictionary mapping is never deferred, whatever its statistics,
//         and it still runs in the reader on the rows that the column predicate kept.
TEST_F(GroupReaderTest, CompoundDictMappingPredicateIsNeverDeferred) {
    ScopedDeferConfig defer(true, 0, 0);
    IntRecorderExpr* recorder = nullptr;
    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(create_dict_mapping_scan_case(3, &recorder)));

    EXPECT_EQ(std::vector<int32_t>({3, 4, 5}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({9, 8, 7}), result.lazy_values[0]);
    EXPECT_EQ(std::vector<int32_t>({3, 4, 5, 6, 7, 8, 9, 10, 11}), recorder->recorded_values);
    EXPECT_TRUE(result.conjunct_has_dict_mapping);
    EXPECT_EQ(0, result.stats.compound_pred_deferred_rows);
    EXPECT_EQ(9, result.stats.compound_pred_selected_rows);
    EXPECT_EQ(9, result.stats.compound_pred_input_rows);
    EXPECT_EQ(3, result.stats.compound_pred_output_rows);
    EXPECT_EQ(std::vector<int64_t>({9}), result.lazy_read_filter_rows[0]);
    ASSERT_EQ(1u, result.conjunct_stats.size());
    EXPECT_EQ(9, result.conjunct_stats[0].input_rows);
    EXPECT_EQ(3, result.conjunct_stats[0].output_rows);
}

// Covers: a plain conjunct is deferred and a conjunct with a dictionary mapping stays in the reader, in
//         the same scan, in either order. The deferred conjunct runs on the output chunk, which holds
//         only the rows that the other conjunct kept, and the rows count once in the input counter.
TEST_F(GroupReaderTest, DeferredAndInReaderCompoundPredicatesInOneScan) {
    auto scan = [this](bool deferred_first, IntVerdictPredicate** plain, IntRecorderExpr** recorder) {
        ScopedDeferConfig defer(true, 0, 0);
        CompoundScanCase scan_case;
        scan_case.active_min = 3;
        scan_case.lazy_columns = {{701, row_as_value},
                                  {kDictCodeSlot, [](uint64_t row) { return static_cast<int32_t>(12 - row); }}};
        // The plain conjunct keeps the odd rows. The dictionary mapping one holds on the rows 0..5.
        auto plain_conjunct = make_int_verdict_conjunct(plain, [](int32_t value) { return value % 2 == 1 ? 1 : 0; });
        auto dict_conjunct = make_dict_mapping_conjunct(recorder);
        scan_case.make_conjunct = deferred_first ? plain_conjunct : dict_conjunct;
        scan_case.more_conjuncts = {deferred_first ? dict_conjunct : plain_conjunct};
        ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
        return result;
    };

    for (bool deferred_first : {true, false}) {
        SCOPED_TRACE(deferred_first);
        IntVerdictPredicate* plain = nullptr;
        IntRecorderExpr* recorder = nullptr;
        const auto result = scan(deferred_first, &plain, &recorder);

        EXPECT_EQ(std::vector<int32_t>({3, 5}), result.a_values);
        EXPECT_EQ(std::vector<int32_t>({3, 5}), result.lazy_values[0]);
        EXPECT_EQ(std::vector<int32_t>({9, 7}), result.lazy_values[1]);
        // The dictionary mapping conjunct saw the rows 3..11, and the plain one only the rows 3..5 that
        // the first one kept.
        EXPECT_EQ(std::vector<int32_t>({3, 4, 5, 6, 7, 8, 9, 10, 11}), recorder->recorded_values);
        EXPECT_EQ(std::vector<int32_t>({3, 4, 5}), plain->evaluated_values);
        EXPECT_EQ(9, result.stats.compound_pred_selected_rows);
        EXPECT_EQ(3, result.stats.compound_pred_deferred_rows);
        EXPECT_EQ(9, result.stats.compound_pred_input_rows);
        EXPECT_EQ(2, result.stats.compound_pred_output_rows);
        ASSERT_EQ(2u, result.conjunct_stats.size());
        const auto& plain_stats = result.conjunct_stats[deferred_first ? 0 : 1];
        const auto& dict_stats = result.conjunct_stats[deferred_first ? 1 : 0];
        EXPECT_EQ(3, plain_stats.input_rows);
        EXPECT_EQ(2, plain_stats.output_rows);
        EXPECT_EQ(9, dict_stats.input_rows);
        EXPECT_EQ(3, dict_stats.output_rows);
    }
}

// Covers: when several conjuncts are deferred, each one is evaluated only on the rows that the earlier
//         ones kept, and the chunk loses the rejected rows once.
TEST_F(GroupReaderTest, DeferredCompoundPredicatesSeeOnlyRowsKeptByEarlierOnes) {
    ScopedDeferConfig defer(true, 0, 0);
    IntVerdictPredicate* first = nullptr;
    IntVerdictPredicate* second = nullptr;
    CompoundScanCase scan_case;
    scan_case.active_min = 3;
    scan_case.lazy_columns = {{701, row_as_value}};
    scan_case.make_conjunct = make_int_verdict_conjunct(&first, [](int32_t value) { return value % 2 == 1 ? 1 : 0; });
    scan_case.more_conjuncts = {make_int_verdict_conjunct(&second, [](int32_t value) { return value >= 6 ? 1 : 0; })};

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({7, 9, 11}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({7, 9, 11}), result.lazy_values[0]);
    EXPECT_EQ(std::vector<int32_t>({3, 4, 5, 6, 7, 8, 9, 10, 11}), first->evaluated_values);
    EXPECT_EQ(std::vector<int32_t>({3, 5, 7, 9, 11}), second->evaluated_values);
    EXPECT_EQ(0, result.stats.compound_pred_selected_rows);
    EXPECT_EQ(9, result.stats.compound_pred_deferred_rows);
    EXPECT_EQ(9, result.stats.compound_pred_input_rows);
    EXPECT_EQ(3, result.stats.compound_pred_output_rows);
    ASSERT_EQ(2u, result.conjunct_stats.size());
    EXPECT_EQ(9, result.conjunct_stats[0].input_rows);
    EXPECT_EQ(5, result.conjunct_stats[0].output_rows);
    EXPECT_EQ(5, result.conjunct_stats[1].input_rows);
    EXPECT_EQ(3, result.conjunct_stats[1].output_rows);
}

// Three passes of 8 rows. The deletion vector drops the first row of each pass, so every pass drops a row
// and a compound conjunct can be deferred. Column a holds the row number, and the rows 1..7, 9..15 and
// 17..23 reach the compound conjuncts.
static CompoundScanCase create_three_pass_scan_case() {
    CompoundScanCase scan_case;
    scan_case.num_rows = 24;
    scan_case.rows_per_read = 8;
    scan_case.deleted_rows = {0, 8, 16};
    return scan_case;
}

static std::vector<int32_t> rows_of_three_passes() {
    return values_where(0, 24, [](int32_t value) { return value % 8 != 0; });
}

// Keeps the multiples of 4: the rows 4, 12 and 20 of the three passes, 3 of 21.
static int keep_multiples_of_4(int32_t value) {
    return value % 4 == 0 ? 1 : 0;
}

static int keep_from_10(int32_t value) {
    return value >= 10 ? 1 : 0;
}

// Covers: a compound conjunct that keeps 3 of 21 rows is deferred to the output chunk from the first pass
//         on, when the group reader reads no lazy column, although its statistics are far below the keep
//         ratio. Nothing is read for the rows that it drops, and the rows are the same as without deferral.
TEST_F(GroupReaderTest, SelectiveCompoundPredicateIsDeferredWhenNoLazyColumnIsRead) {
    auto scan = [this](bool enable, IntVerdictPredicate** predicate) {
        ScopedDeferConfig defer(enable, 0.9, 7);
        CompoundScanCase scan_case = create_three_pass_scan_case();
        scan_case.make_conjunct =
                make_int_verdict_conjunct(predicate, keep_multiples_of_4, CompoundScanCase::kActiveSlot);
        ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
        return result;
    };
    IntVerdictPredicate* reference_predicate = nullptr;
    IntVerdictPredicate* predicate = nullptr;
    const auto reference = scan(false, &reference_predicate);
    const auto result = scan(true, &predicate);

    EXPECT_EQ(std::vector<int32_t>({4, 12, 20}), reference.a_values);
    EXPECT_EQ(0, reference.stats.compound_pred_deferred_rows);
    EXPECT_EQ(21, reference.stats.compound_pred_selected_rows);

    EXPECT_EQ(reference.a_values, result.a_values);
    EXPECT_EQ(rows_of_three_passes(), predicate->evaluated_values);
    EXPECT_EQ(21, result.stats.compound_pred_deferred_rows);
    EXPECT_EQ(0, result.stats.compound_pred_selected_rows);
    EXPECT_EQ(21, result.stats.compound_pred_input_rows);
    EXPECT_EQ(3, result.stats.compound_pred_output_rows);
    ASSERT_EQ(1u, result.conjunct_stats.size());
    EXPECT_EQ(21, result.conjunct_stats[0].input_rows);
    EXPECT_EQ(3, result.conjunct_stats[0].output_rows);
}

// Covers: the same conjunct stays in the reader when the query outputs a lazy column that the conjunct does
//         not read. Deferring it would read that column for the rows that the conjunct drops, so its
//         statistics decide, and they are far below the keep ratio. The column is read for the row that is
//         left after the conjunct in each pass.
TEST_F(GroupReaderTest, SelectiveCompoundPredicateStaysInReaderWhenALazyOutputColumnIsRead) {
    ScopedDeferConfig defer(true, 0.9, 7);
    IntVerdictPredicate* predicate = nullptr;
    CompoundScanCase scan_case = create_three_pass_scan_case();
    scan_case.lazy_columns = {{701, row_as_value, true}};
    scan_case.make_conjunct = make_int_verdict_conjunct(&predicate, keep_multiples_of_4, CompoundScanCase::kActiveSlot);

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({4, 12, 20}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({4, 12, 20}), result.lazy_values[0]);
    EXPECT_EQ(rows_of_three_passes(), predicate->evaluated_values);
    EXPECT_EQ(0, result.stats.compound_pred_deferred_rows);
    EXPECT_EQ(21, result.stats.compound_pred_selected_rows);
    EXPECT_EQ(21, result.stats.compound_pred_input_rows);
    EXPECT_EQ(3, result.stats.compound_pred_output_rows);
    EXPECT_EQ(3u, result.lazy_read_counts[0]);
    EXPECT_EQ(std::vector<int64_t>({1, 1, 1}), result.lazy_read_filter_rows[0]);
}

// Covers: a compound conjunct whose only lazy input is its own column is deferred whatever it keeps, whether
//         the query outputs that column or not. The group reader reads the column for the rows that the
//         earlier stages left, which are the rows that it reads it for when the conjunct runs in the reader.
TEST_F(GroupReaderTest, SelectiveCompoundPredicateIsDeferredWhenItOnlyReadsItsOwnLazyColumn) {
    ScopedDeferConfig defer(true, 0.9, 7);
    for (bool is_output : {false, true}) {
        SCOPED_TRACE(is_output);
        IntVerdictPredicate* predicate = nullptr;
        CompoundScanCase scan_case = create_three_pass_scan_case();
        scan_case.lazy_columns = {{701, row_as_value, is_output}};
        scan_case.make_conjunct = make_int_verdict_conjunct(&predicate, keep_multiples_of_4);

        ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
        EXPECT_EQ(std::vector<int32_t>({4, 12, 20}), result.a_values);
        EXPECT_EQ(std::vector<int32_t>({4, 12, 20}), result.lazy_values[0]);
        EXPECT_EQ(rows_of_three_passes(), predicate->evaluated_values);
        EXPECT_EQ(21, result.stats.compound_pred_deferred_rows);
        EXPECT_EQ(0, result.stats.compound_pred_selected_rows);
        EXPECT_EQ(21, result.stats.compound_pred_input_rows);
        EXPECT_EQ(3, result.stats.compound_pred_output_rows);
        EXPECT_EQ(3u, result.lazy_read_counts[0]);
        EXPECT_EQ(std::vector<int64_t>({7, 7, 7}), result.lazy_read_filter_rows[0]);
    }
}

// Covers: a conjunct is not free to defer when another conjunct reads a lazy column that it does not read,
//         also when no query outputs that column. Deferring it would leave the rows that it drops to the
//         other conjunct, and the column would be read for them. So the first conjunct stays in the reader
//         and the column is read for the row that it leaves, while the second conjunct, which reads the
//         column itself, is deferred.
TEST_F(GroupReaderTest, CompoundPredicateIsNotFreeToDeferWhenAnotherOneReadsALazyColumn) {
    ScopedDeferConfig defer(true, 0.9, 7);
    for (bool is_output : {false, true}) {
        SCOPED_TRACE(is_output);
        IntVerdictPredicate* first = nullptr;
        IntVerdictPredicate* second = nullptr;
        CompoundScanCase scan_case = create_three_pass_scan_case();
        scan_case.lazy_columns = {{701, row_as_value, is_output}};
        scan_case.make_conjunct = make_int_verdict_conjunct(&first, keep_multiples_of_4, CompoundScanCase::kActiveSlot);
        scan_case.more_conjuncts = {make_int_verdict_conjunct(&second, keep_from_10)};

        ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
        EXPECT_EQ(std::vector<int32_t>({12, 20}), result.a_values);
        EXPECT_EQ(std::vector<int32_t>({12, 20}), result.lazy_values[0]);
        // The first conjunct saw the rows of the passes, the second one only the rows that the first kept.
        EXPECT_EQ(rows_of_three_passes(), first->evaluated_values);
        EXPECT_EQ(std::vector<int32_t>({4, 12, 20}), second->evaluated_values);
        EXPECT_EQ(3, result.stats.compound_pred_deferred_rows);
        EXPECT_EQ(21, result.stats.compound_pred_selected_rows);
        EXPECT_EQ(21, result.stats.compound_pred_input_rows);
        EXPECT_EQ(2, result.stats.compound_pred_output_rows);
        EXPECT_EQ(std::vector<int64_t>({1, 1, 1}), result.lazy_read_filter_rows[0]);
    }
}

// Covers: a conjunct that reads every lazy column is not free to defer when a later conjunct is not free,
//         since the later conjunct would stay in the reader and run on the rows that the deferred one
//         drops. Both conjuncts stay in the reader, and the second one sees only the rows that the first
//         one kept.
TEST_F(GroupReaderTest, CompoundPredicateIsNotFreeToDeferWhenALaterOneIsNotFree) {
    ScopedDeferConfig defer(true, 0.9, 7);
    IntVerdictPredicate* first = nullptr;
    IntVerdictPredicate* second = nullptr;
    CompoundScanCase scan_case = create_three_pass_scan_case();
    // The first conjunct reads the only lazy column, which the query outputs. The second one reads column a,
    // so it does not read that lazy column and is not free.
    scan_case.lazy_columns = {{701, row_as_value, true}};
    scan_case.make_conjunct = make_int_verdict_conjunct(&first, keep_multiples_of_4);
    scan_case.more_conjuncts = {make_int_verdict_conjunct(&second, keep_from_10, CompoundScanCase::kActiveSlot)};

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({12, 20}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({12, 20}), result.lazy_values[0]);
    EXPECT_EQ(rows_of_three_passes(), first->evaluated_values);
    EXPECT_EQ(std::vector<int32_t>({4, 12, 20}), second->evaluated_values);
    EXPECT_EQ(0, result.stats.compound_pred_deferred_rows);
    EXPECT_EQ(21, result.stats.compound_pred_selected_rows);
    EXPECT_EQ(21, result.stats.compound_pred_input_rows);
    EXPECT_EQ(2, result.stats.compound_pred_output_rows);
}

// Covers: a compound conjunct with a lambda host is deferred whatever it keeps when the lazy column that its
//         lambda captures is the only lazy column that is read. The lambda runs on the output chunk, and the
//         captured column is read for the rows that the earlier stages left, as it is for the conjunct in
//         the reader.
TEST_F(GroupReaderTest, CompoundLambdaPredicateIsDeferredWhenItReadsTheOnlyLazyColumn) {
    ScopedDeferConfig defer(true, 0.9, 4);
    IntRecorderExpr* recorder = nullptr;
    // The passes are the rows {0, 1, 2}, {3, 4, 5}, ... and the deletion vector drops the first row of each
    // pass. The lambda keeps the even rows, one of two, so its statistics are far below the keep ratio.
    CompoundScanCase scan_case = create_array_map_scan_case(0, &recorder);
    scan_case.rows_per_read = 3;
    scan_case.deleted_rows = {0, 3, 6, 9};

    ASSIGN_OR_ABORT(auto result, _scan_with_compound_predicate(scan_case));
    EXPECT_EQ(std::vector<int32_t>({2, 4, 8, 10}), result.a_values);
    EXPECT_EQ(std::vector<int32_t>({10, 8, 4, 2}), result.lazy_values[0]);
    EXPECT_EQ(std::vector<int32_t>({0, 10, 8, 0, 0, 4, 2, 0}), recorder->recorded_values);
    EXPECT_TRUE(result.conjunct_has_indirect_inputs);
    EXPECT_FALSE(result.conjunct_has_dict_mapping);
    EXPECT_EQ(8, result.stats.compound_pred_deferred_rows);
    EXPECT_EQ(0, result.stats.compound_pred_selected_rows);
    EXPECT_EQ(8, result.stats.compound_pred_input_rows);
    EXPECT_EQ(4, result.stats.compound_pred_output_rows);
    EXPECT_EQ(std::vector<int64_t>({2, 2, 2, 2}), result.lazy_read_filter_rows[0]);
    ASSERT_EQ(1u, result.conjunct_stats.size());
    EXPECT_EQ(8, result.conjunct_stats[0].input_rows);
    EXPECT_EQ(4, result.conjunct_stats[0].output_rows);
}

} // namespace starrocks::parquet
