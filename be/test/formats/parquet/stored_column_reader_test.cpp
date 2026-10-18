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

#include "formats/parquet/stored_column_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "column/column_helper.h"
#include "common/config.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/narrow_io_planner.h"
#include "formats/parquet/offset_index_skip_advisor.h"
#include "formats/parquet/parquet_test_util/recording_input_stream.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/stored_column_reader_with_index.h"
#include "formats/parquet/utils.h"
#include "fs/fs.h"
#include "gen_cpp/parquet_types.h"
#include "io/shared_buffered_input_stream.h"
#include "io/string_input_stream.h"
#include "runtime/types.h"
#include "storage/range.h"
#include "testutil/assert.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/rle_encoding.h"
#include "util/thrift_util.h"

namespace starrocks::parquet {
namespace {

constexpr size_t kNumRows = 256;
constexpr size_t kRowsPerPage = 64;

enum class ReaderCase { REQUIRED, OPTIONAL, OPTIONAL_WITH_LEVELS, REPEATED };

enum class FilterKind {
    // No filter is passed to the reader.
    NONE,
    // No row is selected.
    ZERO,
    // Every other row is selected.
    ALTERNATE,
    // The first zero_prefix rows are not selected, all the other rows are selected.
    ZERO_PREFIX,
};

struct ReadOp {
    uint64_t begin;
    uint64_t end;
    FilterKind filter;
    size_t zero_prefix = 0;
};

// One definition level, repetition level and, if the level carries a value, the value.
struct LevelEntry {
    level_t def;
    level_t rep;
    int32_t value;
};

using ExpectedValue = std::optional<int32_t>;

level_t max_def_level(ReaderCase reader_case) {
    switch (reader_case) {
    case ReaderCase::REQUIRED:
        return 0;
    case ReaderCase::OPTIONAL:
    case ReaderCase::OPTIONAL_WITH_LEVELS:
        return 1;
    case ReaderCase::REPEATED:
        return 3;
    }
    return 0;
}

level_t max_rep_level(ReaderCase reader_case) {
    return reader_case == ReaderCase::REPEATED ? 1 : 0;
}

int level_bit_width(level_t max_level) {
    int width = 0;
    while ((1 << width) < max_level + 1) {
        ++width;
    }
    return width;
}

// The levels of row r. A Required row has one value. An Optional row has one value, and every third row is null.
// A Repeated row is a list: null, empty, a list with one null element, a list with two values, or a list with a
// value, a null and a value. The element of the list has the definition levels 2 (null) and 3 (value).
std::vector<LevelEntry> make_row(ReaderCase reader_case, size_t r) {
    int32_t base = 1000 + static_cast<int32_t>(r) * 10;
    switch (reader_case) {
    case ReaderCase::REQUIRED:
        return {{0, 0, base}};
    case ReaderCase::OPTIONAL:
    case ReaderCase::OPTIONAL_WITH_LEVELS:
        if (r % 3 == 1) {
            return {{0, 0, 0}};
        }
        return {{1, 0, base}};
    case ReaderCase::REPEATED:
        switch (r % 5) {
        case 0:
            return {{0, 0, 0}};
        case 1:
            return {{1, 0, 0}};
        case 2:
            return {{2, 0, 0}};
        case 3:
            return {{3, 0, base}, {3, 1, base + 1}};
        default:
            return {{3, 0, base}, {2, 1, 0}, {3, 1, base + 2}};
        }
    }
    return {};
}

// The levels of a data page v1: the length of the encoded levels followed by the RLE / bit-packed hybrid.
std::string encode_levels(const std::vector<level_t>& levels, int bit_width) {
    faststring encoded;
    RleEncoder<int32_t> encoder(&encoded, bit_width);
    for (level_t level : levels) {
        encoder.Put(level);
    }
    encoder.Flush();
    std::string out;
    put_fixed32_le(&out, static_cast<uint32_t>(encoded.length()));
    out.append(reinterpret_cast<const char*>(encoded.data()), encoded.length());
    return out;
}

std::vector<tparquet::SchemaElement> make_schema(ReaderCase reader_case) {
    std::vector<tparquet::SchemaElement> schema;

    tparquet::SchemaElement root;
    root.__set_name("root");
    root.__set_num_children(1);
    schema.push_back(root);

    if (reader_case == ReaderCase::REPEATED) {
        // optional group c (LIST) { repeated group list { optional int32 element; } }
        tparquet::SchemaElement list;
        list.__set_name("c");
        list.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        list.__set_converted_type(tparquet::ConvertedType::LIST);
        list.__set_num_children(1);
        schema.push_back(list);

        tparquet::SchemaElement repeated;
        repeated.__set_name("list");
        repeated.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
        repeated.__set_num_children(1);
        schema.push_back(repeated);

        tparquet::SchemaElement element;
        element.__set_name("element");
        element.__set_type(tparquet::Type::INT32);
        element.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        element.__set_num_children(0);
        schema.push_back(element);
    } else {
        tparquet::SchemaElement leaf;
        leaf.__set_name("c");
        leaf.__set_type(tparquet::Type::INT32);
        leaf.__set_repetition_type(reader_case == ReaderCase::REQUIRED ? tparquet::FieldRepetitionType::REQUIRED
                                                                       : tparquet::FieldRepetitionType::OPTIONAL);
        leaf.__set_num_children(0);
        schema.push_back(leaf);
    }
    return schema;
}

// Builds a column chunk of INT32 values in memory, kRowsPerPage rows per data page, and reads it back through
// StoredColumnReader.
class StoredColumnReaderTest : public testing::TestWithParam<ReaderCase> {
protected:
    // record_boundary is what the page index sets: every page ends at the end of a record.
    void _init(bool record_boundary = false) {
        _reader_case = GetParam();
        _reader.reset();
        _rows.clear();
        _page_offsets.clear();
        _page_sizes.clear();
        for (size_t r = 0; r < kNumRows; ++r) {
            _rows.push_back(make_row(_reader_case, r));
        }

        std::string buffer;
        size_t num_levels = 0;
        for (size_t first_row = 0; first_row < kNumRows; first_row += kRowsPerPage) {
            std::string page;
            size_t page_levels = 0;
            ASSERT_NO_FATAL_FAILURE(_build_page(first_row, first_row + kRowsPerPage, &page, &page_levels));
            _page_offsets.push_back(buffer.size());
            _page_sizes.push_back(page.size());
            buffer += page;
            num_levels += page_levels;
        }

        _chunk = tparquet::ColumnChunk();
        _chunk.__isset.meta_data = true;
        _chunk.meta_data.type = tparquet::Type::INT32;
        _chunk.meta_data.path_in_schema = {"c"};
        _chunk.meta_data.codec = tparquet::CompressionCodec::UNCOMPRESSED;
        _chunk.meta_data.num_values = num_levels;
        _chunk.meta_data.total_compressed_size = buffer.size();
        _chunk.meta_data.total_uncompressed_size = buffer.size();
        _chunk.meta_data.data_page_offset = 0;

        _file = std::make_unique<RandomAccessFile>(std::make_shared<io::StringInputStream>(std::move(buffer)),
                                                   "stored_column_reader_test");

        tparquet::FileMetaData t_file_meta;
        t_file_meta.__set_version(1);
        t_file_meta.__set_num_rows(kNumRows);
        t_file_meta.__set_schema(make_schema(_reader_case));
        _file_meta = std::make_unique<FileMetaData>();
        ASSERT_OK(_file_meta->init(t_file_meta, true));

        const ParquetField* field = _file_meta->schema().get_stored_column_by_column_name("c");
        ASSERT_TRUE(field != nullptr);
        if (_reader_case == ReaderCase::REPEATED) {
            field = &field->children[0];
        }
        ASSERT_EQ(max_def_level(_reader_case), field->max_def_level());
        ASSERT_EQ(max_rep_level(_reader_case), field->max_rep_level());

        _opts = ColumnReaderOptions();
        _opts.stats = &_stats;
        _opts.file = _file.get();
        _opts.file_meta_data = _file_meta.get();
        _opts.chunk_size = 4096;
        _opts.datacache_options = nullptr;

        ASSERT_OK(StoredColumnReader::create(_opts, field, &_chunk, &_reader));
        if (record_boundary) {
            _reader->set_page_change_on_record_boundry();
        }
        if (_reader_case == ReaderCase::OPTIONAL_WITH_LEVELS) {
            _reader->set_need_parse_levels(true);
        }
    }

    // Wraps the reader like RawColumnReader::select_offset_index() does. Only the pages marked in page_selected may
    // contain rows that are read.
    void _use_page_index(const std::vector<bool>& page_selected) {
        ASSERT_EQ(_page_offsets.size(), page_selected.size());
        _index_ctx = std::make_unique<ColumnOffsetIndexCtx>();
        _index_ctx->rg_first_row = 0;
        _index_ctx->page_selected = page_selected;
        for (size_t p = 0; p < _page_offsets.size(); ++p) {
            tparquet::PageLocation location;
            location.offset = _page_offsets[p];
            location.compressed_page_size = _page_sizes[p];
            location.first_row_index = p * kRowsPerPage;
            _index_ctx->offset_index.page_locations.push_back(location);
        }
        _reader = std::make_unique<StoredColumnReaderWithIndex>(std::move(_reader), _index_ctx.get(), false);
    }

    void _run(const std::vector<ReadOp>& ops) {
        for (const ReadOp& op : ops) {
            ASSERT_NO_FATAL_FAILURE(_read_and_check(op));
        }
    }

    // Reads random ranges in ascending order. The ranges leave gaps, cross pages and use all kinds of filters.
    void _run_random(uint32_t seed) {
        static constexpr size_t kGaps[] = {0, 0, 0, 1, 2, 5, 20};
        std::mt19937 gen(seed);
        std::vector<ReadOp> ops;
        size_t pos = 0;
        while (pos < kNumRows) {
            size_t begin = std::min(pos + kGaps[gen() % 7], kNumRows);
            if (begin >= kNumRows) {
                break;
            }
            size_t end = std::min(begin + 1 + gen() % 70, kNumRows);
            ReadOp op{begin, end, static_cast<FilterKind>(gen() % 4)};
            op.zero_prefix = gen() % (end - begin + 1);
            ops.push_back(op);
            pos = end;
        }
        ASSERT_NO_FATAL_FAILURE(_run(ops));
    }

private:
    void _build_page(size_t first_row, size_t end_row, std::string* page, size_t* num_levels) {
        std::vector<level_t> defs;
        std::vector<level_t> reps;
        std::string values;
        for (size_t r = first_row; r < end_row; ++r) {
            for (const LevelEntry& entry : _rows[r]) {
                defs.push_back(entry.def);
                reps.push_back(entry.rep);
                if (entry.def == max_def_level(_reader_case)) {
                    put_fixed32_le(&values, static_cast<uint32_t>(entry.value));
                }
            }
        }

        std::string data;
        if (max_rep_level(_reader_case) > 0) {
            data += encode_levels(reps, level_bit_width(max_rep_level(_reader_case)));
        }
        if (max_def_level(_reader_case) > 0) {
            data += encode_levels(defs, level_bit_width(max_def_level(_reader_case)));
        }
        data += values;

        tparquet::DataPageHeader data_page_header;
        data_page_header.__set_num_values(static_cast<int32_t>(defs.size()));
        data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
        data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
        data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);

        tparquet::PageHeader page_header;
        page_header.__set_type(tparquet::PageType::DATA_PAGE);
        page_header.__set_uncompressed_page_size(static_cast<int32_t>(data.size()));
        page_header.__set_compressed_page_size(static_cast<int32_t>(data.size()));
        page_header.__set_data_page_header(data_page_header);

        ThriftSerializer serializer(true, 1024);
        uint32_t header_len = 0;
        uint8_t* header_buf = nullptr;
        ASSERT_OK(serializer.serialize(&page_header, &header_len, &header_buf));

        page->assign(reinterpret_cast<const char*>(header_buf), header_len);
        page->append(data);
        *num_levels = defs.size();
    }

    // The slots a row adds to the column that the reader fills: one for each value of a Required or Optional
    // column, and one for each element of a list for a Repeated column.
    std::vector<ExpectedValue> _row_slots(size_t r) const {
        std::vector<ExpectedValue> slots;
        for (const LevelEntry& entry : _rows[r]) {
            if (_reader_case == ReaderCase::REPEATED && entry.def < 2) {
                continue;
            }
            if (entry.def == max_def_level(_reader_case)) {
                slots.emplace_back(entry.value);
            } else {
                slots.emplace_back(std::nullopt);
            }
        }
        return slots;
    }

    static Filter _make_filter(const ReadOp& op) {
        Filter filter(op.end - op.begin, 0);
        for (size_t i = 0; i < filter.size(); ++i) {
            switch (op.filter) {
            case FilterKind::NONE:
            case FilterKind::ZERO:
                break;
            case FilterKind::ALTERNATE:
                filter[i] = (i % 2 == 0) ? 1 : 0;
                break;
            case FilterKind::ZERO_PREFIX:
                filter[i] = (i >= op.zero_prefix) ? 1 : 0;
                break;
            }
        }
        return filter;
    }

    void _read_and_check(const ReadOp& op) {
        SCOPED_TRACE("read [" + std::to_string(op.begin) + ", " + std::to_string(op.end) + ") with filter " +
                     std::to_string(static_cast<int>(op.filter)) + " zero_prefix " + std::to_string(op.zero_prefix));

        Filter filter;
        const Filter* filter_ptr = nullptr;
        if (op.filter != FilterKind::NONE) {
            filter = _make_filter(op);
            filter_ptr = &filter;
        }

        MutableColumnPtr column = ColumnHelper::create_column(TYPE_INT_DESC, _reader_case != ReaderCase::REQUIRED);
        ASSERT_OK(_reader->read_range(Range<uint64_t>(op.begin, op.end), filter_ptr, ColumnContentType::VALUE,
                                      column.get()));
        ASSERT_NO_FATAL_FAILURE(_check_values(*column.get(), op, filter_ptr));
        ASSERT_NO_FATAL_FAILURE(_check_levels(op, filter_ptr));
    }

    // We check only the rows that the filter selects: a page without a selected row gets default values.
    void _check_values(const Column& column, const ReadOp& op, const Filter* filter) {
        size_t slot_pos = 0;
        for (size_t r = op.begin; r < op.end; ++r) {
            bool selected = filter == nullptr || (*filter)[r - op.begin] != 0;
            for (const ExpectedValue& slot : _row_slots(r)) {
                ASSERT_LT(slot_pos, column.size());
                if (selected) {
                    ASSERT_EQ(slot.has_value(), !column.is_null(slot_pos)) << "row " << r;
                    if (slot.has_value()) {
                        ASSERT_EQ(*slot, column.get(slot_pos).get_int32()) << "row " << r;
                    }
                }
                ++slot_pos;
            }
        }
        ASSERT_EQ(slot_pos, column.size());
    }

    // Complex column readers build nulls and offsets from the levels, so the reader has to return the levels of every
    // row of the range. We check all levels of a Repeated column, and the levels of the selected rows of an Optional
    // column that parses levels.
    void _check_levels(const ReadOp& op, const Filter* filter) {
        if (_reader_case == ReaderCase::REQUIRED || _reader_case == ReaderCase::OPTIONAL) {
            return;
        }
        level_t* def_levels = nullptr;
        level_t* rep_levels = nullptr;
        size_t num_levels = 0;
        _reader->get_levels(&def_levels, &rep_levels, &num_levels);

        if (_reader_case == ReaderCase::OPTIONAL_WITH_LEVELS) {
            ASSERT_EQ(op.end - op.begin, num_levels);
            for (size_t r = op.begin; r < op.end; ++r) {
                if (filter == nullptr || (*filter)[r - op.begin] != 0) {
                    ASSERT_EQ(_rows[r][0].def, def_levels[r - op.begin]) << "row " << r;
                }
            }
            return;
        }

        std::vector<LevelEntry> expected;
        for (size_t r = op.begin; r < op.end; ++r) {
            expected.insert(expected.end(), _rows[r].begin(), _rows[r].end());
        }
        ASSERT_EQ(expected.size(), num_levels);
        for (size_t i = 0; i < expected.size(); ++i) {
            ASSERT_EQ(expected[i].def, def_levels[i]) << "level " << i;
            ASSERT_EQ(expected[i].rep, rep_levels[i]) << "level " << i;
        }
    }

    ReaderCase _reader_case = ReaderCase::REQUIRED;
    std::vector<std::vector<LevelEntry>> _rows;
    std::vector<size_t> _page_offsets;
    std::vector<size_t> _page_sizes;

    HdfsScannerStats _stats;
    tparquet::ColumnChunk _chunk;
    std::unique_ptr<RandomAccessFile> _file;
    std::unique_ptr<FileMetaData> _file_meta;
    ColumnReaderOptions _opts;
    std::unique_ptr<ColumnOffsetIndexCtx> _index_ctx;
    std::unique_ptr<StoredColumnReader> _reader;
};

// Read [0, 8), then [8, 16) without a selected row, then [16, 24): the values of the last read have to be the rows
// 16..23 of the file, not the rows that follow the first read.
TEST_P(StoredColumnReaderTest, ZeroRangeBetweenSelectedReads) {
    ASSERT_NO_FATAL_FAILURE(_init());
    ASSERT_NO_FATAL_FAILURE(_run({{0, 8, FilterKind::NONE}, {8, 16, FilterKind::ZERO}, {16, 24, FilterKind::NONE}}));
}

// The first read of the page has no selected row, the page is not loaded when the next read decodes it.
TEST_P(StoredColumnReaderTest, ZeroRangeIsFirstReadOfPage) {
    ASSERT_NO_FATAL_FAILURE(_init());
    ASSERT_NO_FATAL_FAILURE(_run({{0, 16, FilterKind::ZERO}, {16, 32, FilterKind::NONE}}));
}

TEST_P(StoredColumnReaderTest, ZeroRangeIsFirstReadOfSecondPage) {
    ASSERT_NO_FATAL_FAILURE(_init(true));
    ASSERT_NO_FATAL_FAILURE(_run({{0, 64, FilterKind::NONE},
                                  {64, 80, FilterKind::ZERO},
                                  {80, 96, FilterKind::NONE},
                                  {96, 100, FilterKind::ZERO},
                                  {100, 128, FilterKind::NONE}}));
}

TEST_P(StoredColumnReaderTest, ZeroRangeBetweenPartialFilters) {
    ASSERT_NO_FATAL_FAILURE(_init());
    ASSERT_NO_FATAL_FAILURE(_run({{0, 8, FilterKind::ALTERNATE},
                                  {8, 16, FilterKind::ZERO},
                                  {16, 24, FilterKind::ALTERNATE},
                                  {24, 40, FilterKind::NONE}}));
}

TEST_P(StoredColumnReaderTest, ConsecutiveZeroRanges) {
    ASSERT_NO_FATAL_FAILURE(_init());
    ASSERT_NO_FATAL_FAILURE(_run({{0, 4, FilterKind::NONE},
                                  {4, 8, FilterKind::ZERO},
                                  {8, 12, FilterKind::ZERO},
                                  {12, 20, FilterKind::NONE}}));
}

// Rows between two reads are skipped, before and after a range without a selected row.
TEST_P(StoredColumnReaderTest, SkippedRowsAroundZeroRange) {
    ASSERT_NO_FATAL_FAILURE(_init());
    ASSERT_NO_FATAL_FAILURE(_run({{0, 4, FilterKind::NONE},
                                  {6, 10, FilterKind::ZERO},
                                  {12, 20, FilterKind::NONE},
                                  {22, 26, FilterKind::ZERO},
                                  {30, 40, FilterKind::NONE}}));
}

// The range without a selected row ends at the end of the page, the next read starts in the next page.
TEST_P(StoredColumnReaderTest, ZeroRangeEndsAtPageEnd) {
    ASSERT_NO_FATAL_FAILURE(_init(true));
    ASSERT_NO_FATAL_FAILURE(_run({{0, 32, FilterKind::NONE}, {32, 64, FilterKind::ZERO}, {64, 96, FilterKind::NONE}}));
}

// The range without a selected row covers the rest of the first page and the start of the second page, which is not
// loaded. The next read decodes the second page.
TEST_P(StoredColumnReaderTest, ZeroRangeCrossesPageBoundary) {
    ASSERT_NO_FATAL_FAILURE(_init(true));
    ASSERT_NO_FATAL_FAILURE(_run({{0, 32, FilterKind::NONE}, {32, 96, FilterKind::ZERO}, {96, 128, FilterKind::NONE}}));
}

// The first portion of the range, in the loaded first page, has no selected row. The second portion, in the second
// page, has selected rows.
TEST_P(StoredColumnReaderTest, ZeroPortionThenSelectedPortion) {
    ASSERT_NO_FATAL_FAILURE(_init(true));
    ASSERT_NO_FATAL_FAILURE(
            _run({{0, 32, FilterKind::NONE}, {32, 96, FilterKind::ZERO_PREFIX, 40}, {96, 128, FilterKind::NONE}}));
}

// The page index skips the third page. A read in the second and in the fourth page starts with a range without a
// selected row, and the pages are entered through load_specific_page().
TEST_P(StoredColumnReaderTest, ZeroRangeWithPageIndex) {
    ASSERT_NO_FATAL_FAILURE(_init());
    ASSERT_NO_FATAL_FAILURE(_use_page_index({true, true, false, true}));
    ASSERT_NO_FATAL_FAILURE(_run({{0, 16, FilterKind::NONE},
                                  {16, 32, FilterKind::ZERO},
                                  {32, 40, FilterKind::NONE},
                                  {64, 80, FilterKind::ZERO},
                                  {80, 90, FilterKind::NONE},
                                  {192, 200, FilterKind::ZERO},
                                  {200, 210, FilterKind::NONE}}));
}

TEST_P(StoredColumnReaderTest, RandomReads) {
    for (uint32_t seed = 1; seed <= 20; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        ASSERT_NO_FATAL_FAILURE(_init(true));
        ASSERT_NO_FATAL_FAILURE(_run_random(seed));
    }
}

// The reader offers the decoder the filter of the rows that it decodes, whatever the share of the rows that the
// filter keeps, and leaves the decision to the decoder. A Repeated reader has more values than rows and offers
// none. The switch turns the offer off.
TEST_P(StoredColumnReaderTest, OffersTheFilterOfTheDecodedRowsToTheDecoder) {
    ASSERT_NO_FATAL_FAILURE(_init());
    auto* reader = static_cast<StoredColumnReaderImpl*>(_reader.get());
    // The filter keeps all rows but one.
    Filter filter(16, 1);
    filter[3] = 0;

    const FilterData* offered = reader->_convert_filter_row_to_value(&filter, 5);
    if (GetParam() == ReaderCase::REPEATED) {
        EXPECT_EQ(nullptr, offered);
    } else {
        EXPECT_EQ(filter.data() + 5, offered);
    }
    EXPECT_EQ(nullptr, reader->_convert_filter_row_to_value(nullptr, 5));

    const bool enabled = config::parquet_push_down_filter_to_decoder_enable;
    config::parquet_push_down_filter_to_decoder_enable = false;
    EXPECT_EQ(nullptr, reader->_convert_filter_row_to_value(&filter, 5));
    config::parquet_push_down_filter_to_decoder_enable = enabled;
}

// The reader counts the rows that the filter keeps once for a read range, and the decoder of the column chunk gets
// the share: the decoder that is created for the first page of the range, and the one that is already there for
// the next range. A range that crosses a page boundary gets the share of the whole range. A Repeated reader offers
// no filter, so it tells no share.
TEST_P(StoredColumnReaderTest, TellsTheDecoderTheShareOfTheReadRange) {
    ASSERT_NO_FATAL_FAILURE(_init(true));
    auto* reader = static_cast<StoredColumnReaderImpl*>(_reader.get());
    const bool offers_filter = GetParam() != ReaderCase::REPEATED;
    auto decoder_share = [&reader]() { return reader->_reader->_cur_decoder->_filter_share; };

    // The first read creates the decoder after the share is counted: the rows 0..127 are in two pages of 64 rows,
    // and every other row is kept, so the share is 0.5 for both pages.
    ASSERT_NO_FATAL_FAILURE(_run(std::vector<ReadOp>{ReadOp{0, 128, FilterKind::ALTERNATE}}));
    EXPECT_DOUBLE_EQ(offers_filter ? 0.5 : 1.0, decoder_share());

    // The decoder exists, and gets the share of the next range even when no row of it is kept.
    ASSERT_NO_FATAL_FAILURE(_run(std::vector<ReadOp>{ReadOp{128, 160, FilterKind::ZERO}}));
    EXPECT_DOUBLE_EQ(offers_filter ? 0.0 : 1.0, decoder_share());

    // The first 8 rows of the range are dropped and the other 24 are kept.
    ASSERT_NO_FATAL_FAILURE(_run(std::vector<ReadOp>{ReadOp{160, 192, FilterKind::ZERO_PREFIX, 8}}));
    EXPECT_DOUBLE_EQ(offers_filter ? 0.75 : 1.0, decoder_share());
}

// A NULL value in the page for a column that cannot hold NULL means that the file contradicts the table. The reader
// returns Corruption that names the column and the file, and a range without a NULL value still reads.
TEST_P(StoredColumnReaderTest, NullForNonNullableColumnIsCorruptionWithColumnAndFile) {
    if (GetParam() != ReaderCase::OPTIONAL && GetParam() != ReaderCase::OPTIONAL_WITH_LEVELS) {
        GTEST_SKIP() << "only the Optional readers get NULL values for a destination";
    }
    ASSERT_NO_FATAL_FAILURE(_init());
    // The row 1 is NULL.
    MutableColumnPtr column = ColumnHelper::create_column(TYPE_INT_DESC, false);
    Status st = _reader->read_range(Range<uint64_t>(0, 8), nullptr, ColumnContentType::VALUE, column.get());
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
    EXPECT_NE(std::string::npos, st.to_string().find("'c'")) << st.to_string();
    EXPECT_NE(std::string::npos, st.to_string().find("stored_column_reader_test")) << st.to_string();
}

// The rows 2 and 3 have a value, and the row 4 is NULL.
TEST_P(StoredColumnReaderTest, RangeWithoutNullReadsIntoNonNullableColumn) {
    if (GetParam() != ReaderCase::OPTIONAL && GetParam() != ReaderCase::OPTIONAL_WITH_LEVELS) {
        GTEST_SKIP() << "only the Optional readers get NULL values for a destination";
    }
    ASSERT_NO_FATAL_FAILURE(_init());
    MutableColumnPtr column = ColumnHelper::create_column(TYPE_INT_DESC, false);
    ASSERT_OK(_reader->read_range(Range<uint64_t>(2, 4), nullptr, ColumnContentType::VALUE, column.get()));
    ASSERT_EQ(2U, column->size());
    EXPECT_EQ(1000 + 2 * 10, column->get(0).get_int32());
    EXPECT_EQ(1000 + 3 * 10, column->get(1).get_int32());
}

INSTANTIATE_TEST_SUITE_P(AllReaders, StoredColumnReaderTest,
                         testing::Values(ReaderCase::REQUIRED, ReaderCase::OPTIONAL, ReaderCase::OPTIONAL_WITH_LEVELS,
                                         ReaderCase::REPEATED),
                         [](const testing::TestParamInfo<ReaderCase>& info) {
                             switch (info.param) {
                             case ReaderCase::REQUIRED:
                                 return std::string("Required");
                             case ReaderCase::OPTIONAL:
                                 return std::string("Optional");
                             case ReaderCase::OPTIONAL_WITH_LEVELS:
                                 return std::string("OptionalWithLevels");
                             case ReaderCase::REPEATED:
                                 return std::string("Repeated");
                             }
                             return std::string("Unknown");
                         });

// ---------------------------------------------------------------------------------------------------------------------
// Skipping the pages that hold no selected row through the offset index.
// ---------------------------------------------------------------------------------------------------------------------

// The value of row r: a Required column has a value in every row, and every third row of an Optional column is NULL.
ExpectedValue skip_row_value(ReaderCase reader_case, size_t r) {
    if (reader_case != ReaderCase::REQUIRED && r % 3 == 1) {
        return std::nullopt;
    }
    return 1000 + static_cast<int32_t>(r % 97) * 10;
}

struct SkipLayout {
    ReaderCase reader_case = ReaderCase::REQUIRED;
    // The pages are dictionary-encoded, and the chunk has a dictionary page. Otherwise they are PLAIN.
    bool dict = false;
    // The number of rows of each page. The pages have different sizes, so a page boundary does not fall at a
    // multiple of anything.
    std::vector<size_t> page_rows = {30, 50, 40, 64, 20, 60, 36, 40};
    // The chunk is read through a shared buffer of the stream that holds the whole chunk, with the planner of the
    // narrowing as its provider (NarrowIoPlanner). Otherwise the reader reads the stream of the chunk directly.
    bool shared_buffer = false;
};

// A filter of `rows` rows that selects the rows of the given [begin, end) pieces.
Filter make_filter(size_t rows, const std::vector<std::pair<size_t, size_t>>& selected) {
    Filter filter(rows, 0);
    for (const auto& [begin, end] : selected) {
        for (size_t i = begin; i < end; ++i) {
            filter[i] = 1;
        }
    }
    return filter;
}

// A filter of one of a few shapes: nothing, everything, a few rows, runs, one row, or the first and the last row.
Filter make_random_filter(std::mt19937* gen, size_t rows) {
    Filter filter(rows, 0);
    switch ((*gen)() % 6) {
    case 0:
        std::fill(filter.begin(), filter.end(), 1);
        break;
    case 1:
        break;
    case 2:
        for (size_t i = 0; i < rows; ++i) {
            filter[i] = (*gen)() % 50 == 0 ? 1 : 0;
        }
        break;
    case 3: {
        bool selected = (*gen)() % 2 == 0;
        for (size_t i = 0; i < rows;) {
            size_t run = 1 + (*gen)() % 80;
            for (size_t j = 0; j < run && i < rows; ++j, ++i) {
                filter[i] = selected ? 1 : 0;
            }
            selected = !selected;
        }
        break;
    }
    case 4:
        filter[(*gen)() % rows] = 1;
        break;
    default:
        filter[0] = 1;
        filter[rows - 1] = 1;
        break;
    }
    return filter;
}

// A column chunk of INT32 values in memory, with the page layout of a SkipLayout, an offset index built from its
// pages, and a reader over it. The stream of the chunk records the bytes that the reader takes, so a test can check
// which pages the reader touched. read() checks the values that it returns, and finish() checks the pages that were
// touched, the default values of the rows of the pages that were skipped, and the counters.
class SkipChunk {
public:
    SkipChunk() = default;
    SkipChunk(const SkipChunk&) = delete;
    SkipChunk& operator=(const SkipChunk&) = delete;

    // use_index: the reader skips pages through the offset index. page_selected: the reader is also wrapped with the
    // reader that follows a static selection of pages, like RawColumnReader::select_offset_index() does.
    void init(const SkipLayout& layout, bool use_index = true, const std::vector<bool>& page_selected = {}) {
        _layout = layout;
        const ReaderCase reader_case = _layout.reader_case;

        _num_rows = 0;
        for (size_t rows : _layout.page_rows) {
            _first_rows.push_back(_num_rows);
            _num_rows += rows;
        }
        _first_rows.push_back(_num_rows);
        for (size_t r = 0; r < _num_rows; ++r) {
            _values.push_back(skip_row_value(reader_case, r));
            if (_layout.dict && _values[r].has_value() && _dict_codes.count(*_values[r]) == 0) {
                _dict_codes[*_values[r]] = static_cast<int32_t>(_dict_values.size());
                _dict_values.push_back(*_values[r]);
            }
        }

        std::string buffer;
        if (_layout.dict) {
            std::string dict_data;
            for (int32_t value : _dict_values) {
                put_fixed32_le(&dict_data, static_cast<uint32_t>(value));
            }
            tparquet::DictionaryPageHeader dict_header;
            dict_header.__set_num_values(static_cast<int32_t>(_dict_values.size()));
            dict_header.__set_encoding(tparquet::Encoding::PLAIN);
            tparquet::PageHeader page_header;
            page_header.__set_type(tparquet::PageType::DICTIONARY_PAGE);
            page_header.__set_dictionary_page_header(dict_header);
            std::string dict_page;
            ASSERT_NO_FATAL_FAILURE(_serialize_page(&page_header, dict_data, &dict_page));
            _dict_size = dict_page.size();
            buffer += dict_page;
        }
        for (size_t p = 0; p < _layout.page_rows.size(); ++p) {
            std::string page;
            ASSERT_NO_FATAL_FAILURE(_build_page(_first_rows[p], _first_rows[p + 1], &page));
            _page_offsets.push_back(buffer.size());
            _page_sizes.push_back(page.size());
            buffer += page;
        }

        _chunk = tparquet::ColumnChunk();
        _chunk.__isset.meta_data = true;
        _chunk.meta_data.type = tparquet::Type::INT32;
        _chunk.meta_data.path_in_schema = {"c"};
        _chunk.meta_data.codec = tparquet::CompressionCodec::UNCOMPRESSED;
        _chunk.meta_data.num_values = _num_rows;
        _chunk.meta_data.total_compressed_size = buffer.size();
        _chunk.meta_data.total_uncompressed_size = buffer.size();
        _chunk.meta_data.data_page_offset = _dict_size;
        if (_layout.dict) {
            _chunk.meta_data.__set_dictionary_page_offset(0);
        }
        // The test gives the reader the offset index itself, and the chunk says that it has one.
        _chunk.__set_offset_index_offset(static_cast<int64_t>(buffer.size()));
        _chunk.__set_offset_index_length(1);

        _chunk_size = static_cast<int64_t>(buffer.size());
        _stream = std::make_shared<RecordingInputStream>(std::move(buffer));
        if (_layout.shared_buffer) {
            _sb_stream = std::make_shared<io::SharedBufferedInputStream>(_stream, "skip_chunk_test", _chunk_size);
            ASSERT_OK(_sb_stream->set_io_ranges({io::SharedBufferedInputStream::IORange(0, _chunk_size)}));
            _planner = std::make_unique<NarrowIoPlanner>(&_stats);
            _sb_stream->set_skip_ranges_provider(_planner.get());
            _file = std::make_unique<RandomAccessFile>(_sb_stream, "skip_chunk_test");
        } else {
            _file = std::make_unique<RandomAccessFile>(_stream, "skip_chunk_test");
        }

        tparquet::FileMetaData t_file_meta;
        t_file_meta.__set_version(1);
        t_file_meta.__set_num_rows(_num_rows);
        t_file_meta.__set_schema(make_schema(reader_case));
        _file_meta = std::make_unique<FileMetaData>();
        ASSERT_OK(_file_meta->init(t_file_meta, true));
        const ParquetField* field = _file_meta->schema().get_stored_column_by_column_name("c");
        ASSERT_TRUE(field != nullptr);
        ASSERT_EQ(max_def_level(reader_case), field->max_def_level());

        _row_group.num_rows = _num_rows;
        _opts = ColumnReaderOptions();
        _opts.stats = &_stats;
        _opts.file = _file.get();
        _opts.file_meta_data = _file_meta.get();
        _opts.row_group_meta = &_row_group;
        _opts.chunk_size = 4096;
        _opts.datacache_options = nullptr;
        _opts.narrow_io = _planner.get();
        ASSERT_OK(StoredColumnReader::create(_opts, field, &_chunk, &_reader));
        if (reader_case == ReaderCase::OPTIONAL_WITH_LEVELS) {
            _reader->set_need_parse_levels(true);
        }

        _ctx = ColumnOffsetIndexCtx();
        _ctx.rg_first_row = 0;
        for (size_t p = 0; p < _layout.page_rows.size(); ++p) {
            tparquet::PageLocation location;
            location.offset = _page_offsets[p];
            location.compressed_page_size = _page_sizes[p];
            location.first_row_index = _first_rows[p];
            _ctx.offset_index.page_locations.push_back(location);
        }
        if (!page_selected.empty()) {
            ASSERT_EQ(_layout.page_rows.size(), page_selected.size());
            _ctx.page_selected = page_selected;
            _reader = std::make_unique<StoredColumnReaderWithIndex>(std::move(_reader), &_ctx, _layout.dict);
        }
        if (use_index) {
            ASSERT_TRUE(_reader->supports_offset_index_skip());
            _reader->set_offset_index_skip(&_ctx, _layout.dict);
        }
    }

    size_t num_rows() const { return _num_rows; }
    size_t num_pages() const { return _layout.page_rows.size(); }
    size_t first_row(size_t page) const { return _first_rows[page]; }
    const HdfsScannerStats& stats() const { return _stats; }
    RecordingInputStream& stream() { return *_stream; }
    io::SharedBufferedInputStream& shared_stream() { return *_sb_stream; }
    // The reader loads the dictionary page, as a dictionary filter does before any range is read.
    Status load_dictionary() { return _reader->load_dictionary_page(); }
    // The chunk says that it has no offset index, and the scan gives the reader a feedback of the skipping.
    void hide_offset_index() { _chunk.__isset.offset_index_offset = false; }
    void set_skip_advisor(OffsetIndexSkipAdvisor* advisor) { _opts.offset_index_skip_advisor = advisor; }
    // The bytes of the chunk in the file: the whole chunk, the dictionary page, and a data page.
    int64_t chunk_size() const { return _chunk_size; }
    int64_t dict_size() const { return _dict_size; }
    int64_t page_begin(size_t page) const { return _page_offsets[page]; }
    int64_t page_end(size_t page) const { return _page_offsets[page] + _page_sizes[page]; }
    bool page_touched(size_t page) const {
        return _stream->touched(_page_offsets[page], _page_offsets[page] + _page_sizes[page]);
    }
    tparquet::OffsetIndex& offset_index() { return _ctx.offset_index; }
    // The values that the page reader counts as read, for a reader that is not wrapped.
    uint64_t num_values_read() {
        return static_cast<StoredColumnReaderImpl*>(_reader.get())->_reader->_page_reader->_num_values_read;
    }

    // Reads [begin, end) with the filter, which is nullptr for a read without a filter, and checks every row that the
    // filter selects. A page that holds a selected row is "needed".
    Status read_status(uint64_t begin, uint64_t end, const Filter* filter,
                       ColumnContentType content_type = ColumnContentType::VALUE) {
        _column = ColumnHelper::create_column(TYPE_INT_DESC, _layout.reader_case != ReaderCase::REQUIRED);
        return _reader->read_range(Range<uint64_t>(begin, end), filter, content_type, _column.get());
    }

    void read(uint64_t begin, uint64_t end, const Filter* filter,
              ColumnContentType content_type = ColumnContentType::VALUE) {
        SCOPED_TRACE("read [" + std::to_string(begin) + ", " + std::to_string(end) + ")");
        ASSERT_OK(read_status(begin, end, filter, content_type));
        ASSERT_EQ(end - begin, _column->size());

        Read record;
        record.begin = begin;
        record.end = end;
        for (size_t i = 0; i < end - begin; ++i) {
            if (filter != nullptr && (*filter)[i] == 0) {
                continue;
            }
            _needed.insert(_page_of(begin + i));
            ASSERT_NO_FATAL_FAILURE(_check_value(*_column, i, begin + i, content_type));
        }
        if (_layout.reader_case == ReaderCase::OPTIONAL_WITH_LEVELS) {
            level_t* def_levels = nullptr;
            level_t* rep_levels = nullptr;
            size_t num_levels = 0;
            _reader->get_levels(&def_levels, &rep_levels, &num_levels);
            // complex readers build their nulls from the levels, so there is one level for every row of the range
            ASSERT_EQ(end - begin, num_levels);
            record.levels.assign(def_levels, def_levels + num_levels);
            for (size_t i = 0; i < end - begin; ++i) {
                if (filter == nullptr || (*filter)[i] != 0) {
                    ASSERT_EQ(_values[begin + i].has_value() ? 1 : 0, record.levels[i]) << "row " << begin + i;
                }
            }
        }
        record.column = std::move(_column);
        _reads.push_back(std::move(record));
    }

    // Every page that holds a selected row was read, and every other page was not: not its header, not its data.
    void check_touched_pages() {
        for (size_t p = 0; p < _page_offsets.size(); ++p) {
            const bool needed = _needed.count(p) > 0;
            const bool touched = page_touched(p);
            ASSERT_EQ(needed, touched) << "page " << p << (needed ? " was not read" : " was read");
        }
        if (_layout.dict) {
            ASSERT_EQ(!_needed.empty(), _stream->touched(0, _dict_size));
        }
    }

    // A row of a page that no selected row needed has the default value: NULL, or zero in a Required column. Its
    // definition level is zero.
    void check_default_rows() {
        for (const Read& record : _reads) {
            for (size_t i = 0; i < record.end - record.begin; ++i) {
                if (_needed.count(_page_of(record.begin + i)) > 0) {
                    continue;
                }
                if (_layout.reader_case == ReaderCase::REQUIRED) {
                    ASSERT_EQ(0, record.column->get(i).get_int32()) << "row " << record.begin + i;
                } else {
                    ASSERT_TRUE(record.column->is_null(i)) << "row " << record.begin + i;
                }
                if (_layout.reader_case == ReaderCase::OPTIONAL_WITH_LEVELS) {
                    ASSERT_EQ(0, record.levels[i]) << "row " << record.begin + i;
                }
            }
        }
    }

    // The reader passed the pages below the last needed page that no read needed, and counted them.
    void check_counters() {
        int64_t pages = 0;
        int64_t bytes = 0;
        if (!_needed.empty()) {
            for (size_t p = 0; p < *_needed.rbegin(); ++p) {
                if (_needed.count(p) == 0) {
                    ++pages;
                    bytes += _page_sizes[p];
                }
            }
        }
        ASSERT_EQ(pages, _stats.page_skip_by_offset_index);
        ASSERT_EQ(bytes, _stats.page_skip_by_offset_index_undecoded_bytes);
        // the scan learns the pages that the reader skipped from the reader
        ASSERT_EQ(pages, _reader->offset_index_skipped_pages());
    }

    void finish() {
        ASSERT_NO_FATAL_FAILURE(check_touched_pages());
        ASSERT_NO_FATAL_FAILURE(check_default_rows());
        ASSERT_NO_FATAL_FAILURE(check_counters());
    }

private:
    struct Read {
        uint64_t begin = 0;
        uint64_t end = 0;
        MutableColumnPtr column;
        std::vector<level_t> levels;
    };

    size_t _page_of(size_t row) const {
        return std::upper_bound(_first_rows.begin(), _first_rows.end(), row) - _first_rows.begin() - 1;
    }

    void _check_value(const Column& column, size_t i, size_t r, ColumnContentType content_type) {
        const ExpectedValue& expected = _values[r];
        if (_layout.reader_case != ReaderCase::REQUIRED) {
            ASSERT_EQ(expected.has_value(), !column.is_null(i)) << "row " << r;
        }
        if (expected.has_value()) {
            const int32_t want = content_type == ColumnContentType::DICT_CODE ? _dict_codes.at(*expected) : *expected;
            ASSERT_EQ(want, column.get(i).get_int32()) << "row " << r;
        }
    }

    void _serialize_page(tparquet::PageHeader* page_header, const std::string& data, std::string* page) {
        page_header->__set_uncompressed_page_size(static_cast<int32_t>(data.size()));
        page_header->__set_compressed_page_size(static_cast<int32_t>(data.size()));
        ThriftSerializer serializer(true, 1024);
        uint32_t header_len = 0;
        uint8_t* header_buf = nullptr;
        ASSERT_OK(serializer.serialize(page_header, &header_len, &header_buf));
        page->assign(reinterpret_cast<const char*>(header_buf), header_len);
        page->append(data);
    }

    void _build_page(size_t first_row, size_t end_row, std::string* page) {
        const ReaderCase reader_case = _layout.reader_case;
        std::vector<level_t> defs;
        std::vector<int32_t> present;
        for (size_t r = first_row; r < end_row; ++r) {
            defs.push_back(_values[r].has_value() ? max_def_level(reader_case) : 0);
            if (_values[r].has_value()) {
                present.push_back(*_values[r]);
            }
        }

        std::string data;
        if (max_def_level(reader_case) > 0) {
            data += encode_levels(defs, level_bit_width(max_def_level(reader_case)));
        }
        if (_layout.dict) {
            int bit_width = 1;
            while ((static_cast<size_t>(1) << bit_width) < _dict_values.size()) {
                ++bit_width;
            }
            faststring encoded;
            RleEncoder<int32_t> encoder(&encoded, bit_width);
            for (int32_t value : present) {
                encoder.Put(_dict_codes.at(value));
            }
            encoder.Flush();
            data.push_back(static_cast<char>(bit_width));
            data.append(reinterpret_cast<const char*>(encoded.data()), encoded.length());
        } else {
            for (int32_t value : present) {
                put_fixed32_le(&data, static_cast<uint32_t>(value));
            }
        }

        tparquet::DataPageHeader data_page_header;
        data_page_header.__set_num_values(static_cast<int32_t>(defs.size()));
        data_page_header.__set_encoding(_layout.dict ? tparquet::Encoding::RLE_DICTIONARY : tparquet::Encoding::PLAIN);
        data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
        data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
        tparquet::PageHeader page_header;
        page_header.__set_type(tparquet::PageType::DATA_PAGE);
        page_header.__set_data_page_header(data_page_header);
        ASSERT_NO_FATAL_FAILURE(_serialize_page(&page_header, data, page));
    }

    SkipLayout _layout;
    size_t _num_rows = 0;
    // The first row of each page, then the number of rows.
    std::vector<size_t> _first_rows;
    std::vector<int64_t> _page_offsets;
    std::vector<int64_t> _page_sizes;
    int64_t _dict_size = 0;
    int64_t _chunk_size = 0;
    std::vector<ExpectedValue> _values;
    std::vector<int32_t> _dict_values;
    std::map<int32_t, int32_t> _dict_codes;

    std::shared_ptr<RecordingInputStream> _stream;
    std::shared_ptr<io::SharedBufferedInputStream> _sb_stream;
    std::unique_ptr<RandomAccessFile> _file;
    HdfsScannerStats _stats;
    std::unique_ptr<NarrowIoPlanner> _planner;
    tparquet::ColumnChunk _chunk;
    tparquet::RowGroup _row_group;
    std::unique_ptr<FileMetaData> _file_meta;
    ColumnReaderOptions _opts;
    ColumnOffsetIndexCtx _ctx;
    std::unique_ptr<StoredColumnReader> _reader;

    MutableColumnPtr _column;
    std::vector<Read> _reads;
    std::set<size_t> _needed;
};

// Sets the config of page skipping for the scope of a test.
class ScopedSkipConfig {
public:
    explicit ScopedSkipConfig(bool value) : _old_value(config::parquet_offset_index_skip_enable) {
        config::parquet_offset_index_skip_enable = value;
    }
    ~ScopedSkipConfig() { config::parquet_offset_index_skip_enable = _old_value; }

private:
    bool _old_value;
};

struct SkipParam {
    ReaderCase reader_case;
    bool dict;
};

// The pages are 30, 50, 40, 64, 20, 60, 36 and 40 rows, so they start at the rows 0, 30, 80, 120, 184, 204, 264 and
// 300.
class OffsetIndexSkipTest : public testing::TestWithParam<SkipParam> {
protected:
    SkipLayout layout() const {
        SkipLayout layout;
        layout.reader_case = GetParam().reader_case;
        layout.dict = GetParam().dict;
        return layout;
    }
};

// Reads that need the first, the third and the fifth page: the pages in between are not read.
TEST_P(OffsetIndexSkipTest, SkipsPagesInTheMiddle) {
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(layout()));
    Filter ones30(30, 1);
    Filter ones40(40, 1);
    Filter ones20(20, 1);
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, 30, &ones30));
    ASSERT_NO_FATAL_FAILURE(chunk.read(80, 120, &ones40));
    ASSERT_NO_FATAL_FAILURE(chunk.read(184, 204, &ones20));
    ASSERT_NO_FATAL_FAILURE(chunk.finish());
    ASSERT_EQ(2, chunk.stats().page_skip_by_offset_index);
}

// One range selects rows of the third and the sixth page only: the pages before them and between them are not read,
// and neither are the pages after the last selected row.
TEST_P(OffsetIndexSkipTest, SkipsTheFirstAndTheLastPages) {
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(layout()));
    Filter filter = make_filter(chunk.num_rows(), {{90, 100}, {210, 215}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, chunk.num_rows(), &filter));
    ASSERT_NO_FATAL_FAILURE(chunk.finish());
    ASSERT_EQ(4, chunk.stats().page_skip_by_offset_index);
}

// One selected row in every page: no page is skipped.
TEST_P(OffsetIndexSkipTest, SelectsOneRowPerPage) {
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(layout()));
    std::vector<std::pair<size_t, size_t>> selected;
    for (size_t p = 0; p < chunk.num_pages(); ++p) {
        selected.emplace_back(chunk.first_row(p) + 7, chunk.first_row(p) + 8);
    }
    Filter filter = make_filter(chunk.num_rows(), selected);
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, chunk.num_rows(), &filter));
    ASSERT_NO_FATAL_FAILURE(chunk.finish());
    ASSERT_EQ(0, chunk.stats().page_skip_by_offset_index);
}

// The last row of a page and the first row of the next one are selected: both pages are read, and the page after them
// that holds no selected row is not.
TEST_P(OffsetIndexSkipTest, SelectsRowsAtPageBoundaries) {
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(layout()));
    // the last row of the first page, the first row of the second page, the last row of the third page, and the first
    // row of the fifth page
    Filter filter = make_filter(chunk.num_rows(), {{29, 31}, {119, 120}, {184, 185}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, chunk.num_rows(), &filter));
    ASSERT_NO_FATAL_FAILURE(chunk.finish());
    // the second and the fourth page
    ASSERT_EQ(1, chunk.stats().page_skip_by_offset_index);
}

// Ranges without a selected row do not move the reader. The first range with a selected row jumps to its page, over all
// the pages before it, and the last range without a selected row does not read anything.
TEST_P(OffsetIndexSkipTest, NothingSelectedThenSomethingInALaterPage) {
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(layout()));
    Filter zeros60(60, 0);
    Filter zeros80(80, 0);
    Filter one_row = make_filter(60, {{26, 27}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, 60, &zeros60));
    ASSERT_NO_FATAL_FAILURE(chunk.read(60, 120, &zeros60));
    ASSERT_NO_FATAL_FAILURE(chunk.read(200, 260, &one_row));
    ASSERT_NO_FATAL_FAILURE(chunk.read(260, 340, &zeros80));
    ASSERT_NO_FATAL_FAILURE(chunk.finish());
    ASSERT_EQ(5, chunk.stats().page_skip_by_offset_index);
}

TEST_P(OffsetIndexSkipTest, NothingSelectedAtAll) {
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(layout()));
    Filter zeros(chunk.num_rows(), 0);
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, chunk.num_rows(), &zeros));
    ASSERT_NO_FATAL_FAILURE(chunk.finish());
    // not even the dictionary page was read
    ASSERT_TRUE(chunk.stream().ranges().empty());
}

TEST_P(OffsetIndexSkipTest, SelectsEveryRow) {
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(layout()));
    Filter ones(chunk.num_rows(), 1);
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, chunk.num_rows(), &ones));
    ASSERT_NO_FATAL_FAILURE(chunk.finish());
    ASSERT_EQ(0, chunk.stats().page_skip_by_offset_index);
}

// Ranges that are shorter than a page, in rising order with gaps in between: the reader keeps its place inside a page
// and jumps over the pages that the gaps and the unselected rows cover.
TEST_P(OffsetIndexSkipTest, ShortRangesWithGaps) {
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(layout()));
    Filter first = make_filter(10, {{0, 10}});
    Filter second = make_filter(10, {{3, 4}});
    Filter third = make_filter(25, {{20, 25}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(5, 15, &first));
    ASSERT_NO_FATAL_FAILURE(chunk.read(40, 50, &second));
    ASSERT_NO_FATAL_FAILURE(chunk.read(60, 85, &third));
    ASSERT_NO_FATAL_FAILURE(chunk.read(100, 110, &second));
    ASSERT_NO_FATAL_FAILURE(chunk.read(300, 310, &first));
    ASSERT_NO_FATAL_FAILURE(chunk.finish());
}

// A range with a different filter each time, over the whole chunk.
TEST_P(OffsetIndexSkipTest, RandomReads) {
    for (uint32_t seed = 1; seed <= 60; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        std::mt19937 gen(seed);
        SkipChunk chunk;
        ASSERT_NO_FATAL_FAILURE(chunk.init(layout()));
        static constexpr size_t kGaps[] = {0, 0, 0, 1, 5, 30, 80};
        size_t pos = 0;
        while (pos < chunk.num_rows()) {
            const size_t begin = std::min(pos + kGaps[gen() % 7], chunk.num_rows());
            if (begin >= chunk.num_rows()) {
                break;
            }
            const size_t end = std::min(begin + 1 + gen() % 120, chunk.num_rows());
            Filter filter = make_random_filter(&gen, end - begin);
            ASSERT_NO_FATAL_FAILURE(chunk.read(begin, end, &filter));
            pos = end;
        }
        ASSERT_NO_FATAL_FAILURE(chunk.finish());
    }
}

// Reads without a filter are not skipped and read what they need. Mixed with reads under a filter, every value is the
// value of its row.
TEST_P(OffsetIndexSkipTest, ReadsWithoutFilterAreMixedWithFilteredReads) {
    for (uint32_t seed = 1; seed <= 30; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        std::mt19937 gen(seed);
        SkipChunk chunk;
        ASSERT_NO_FATAL_FAILURE(chunk.init(layout()));
        size_t pos = 0;
        while (pos < chunk.num_rows()) {
            const size_t begin = std::min(pos + gen() % 40, chunk.num_rows());
            if (begin >= chunk.num_rows()) {
                break;
            }
            const size_t end = std::min(begin + 1 + gen() % 120, chunk.num_rows());
            if (gen() % 3 == 0) {
                ASSERT_NO_FATAL_FAILURE(chunk.read(begin, end, nullptr));
            } else {
                Filter filter = make_random_filter(&gen, end - begin);
                ASSERT_NO_FATAL_FAILURE(chunk.read(begin, end, &filter));
            }
            pos = end;
        }
    }
}

// Without the offset index, and with the config off, the reader reads the same values and skips no page.
TEST_P(OffsetIndexSkipTest, WithoutTheIndexOrTheConfigTheValuesAreTheSame) {
    for (bool use_index : {false, true}) {
        ScopedSkipConfig config_guard(!use_index);
        for (uint32_t seed = 1; seed <= 20; ++seed) {
            SCOPED_TRACE("seed " + std::to_string(seed));
            std::mt19937 gen(seed);
            SkipChunk chunk;
            ASSERT_NO_FATAL_FAILURE(chunk.init(layout(), use_index));
            size_t pos = 0;
            while (pos < chunk.num_rows()) {
                const size_t begin = std::min(pos + gen() % 40, chunk.num_rows());
                if (begin >= chunk.num_rows()) {
                    break;
                }
                const size_t end = std::min(begin + 1 + gen() % 120, chunk.num_rows());
                Filter filter = make_random_filter(&gen, end - begin);
                ASSERT_NO_FATAL_FAILURE(chunk.read(begin, end, &filter));
                pos = end;
            }
            ASSERT_EQ(0, chunk.stats().page_skip_by_offset_index);
            ASSERT_EQ(0, chunk.stats().page_skip_by_offset_index_undecoded_bytes);
        }
    }
}

// The offset index of the chunk is followed by the reader of the static page selection too: the pages that the
// selection does not contain are not read, and of the pages that it contains the filter reads the ones with a
// selected row.
TEST_P(OffsetIndexSkipTest, StaticPageSelectionAndFilter) {
    SkipChunk chunk;
    // the first and the second page, the fifth and the sixth, and the last
    ASSERT_NO_FATAL_FAILURE(chunk.init(layout(), true, {true, true, false, false, true, true, false, true}));
    // only the rows of the second page are selected, so the first page is skipped too
    Filter second_page = make_filter(80, {{30, 80}});
    // only the rows of the sixth page are selected, so the fifth is skipped
    Filter sixth_page = make_filter(70, {{20, 70}});
    Filter ones(40, 1);
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, 80, &second_page));
    ASSERT_NO_FATAL_FAILURE(chunk.read(190, 260, &sixth_page));
    ASSERT_NO_FATAL_FAILURE(chunk.read(300, 340, &ones));
    ASSERT_NO_FATAL_FAILURE(chunk.finish());
    // the pages 0, 2, 3, 4 and 6
    ASSERT_EQ(5, chunk.stats().page_skip_by_offset_index);
}

// The static page selection with reads that have no filter, and with reads that have one, in the order of the rows.
TEST_P(OffsetIndexSkipTest, StaticPageSelectionRandomReads) {
    for (uint32_t seed = 1; seed <= 30; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        std::mt19937 gen(seed);
        SkipChunk chunk;
        ASSERT_NO_FATAL_FAILURE(chunk.init(layout(), true, {true, true, false, false, true, true, false, true}));
        // the rows of the selected pages: [0, 80), [184, 264) and [300, 340)
        for (const auto& [range_begin, range_end] :
             std::vector<std::pair<size_t, size_t>>{{0, 80}, {184, 264}, {300, 340}}) {
            size_t pos = range_begin;
            while (pos < range_end) {
                const size_t begin = std::min(pos + gen() % 10, range_end);
                if (begin >= range_end) {
                    break;
                }
                const size_t end = std::min(begin + 1 + gen() % 60, range_end);
                if (gen() % 4 == 0) {
                    ASSERT_NO_FATAL_FAILURE(chunk.read(begin, end, nullptr));
                } else {
                    Filter filter = make_random_filter(&gen, end - begin);
                    ASSERT_NO_FATAL_FAILURE(chunk.read(begin, end, &filter));
                }
                pos = end;
            }
        }
        // the pages that the static selection does not contain are not read by any read
        for (size_t page : {2, 3, 6}) {
            ASSERT_FALSE(chunk.page_touched(page)) << "page " << page << " was read";
        }
    }
}

// A range of dictionary codes: the rows that no read selects in the pages that were skipped have no code.
TEST_P(OffsetIndexSkipTest, ReadsDictionaryCodes) {
    if (!GetParam().dict || GetParam().reader_case == ReaderCase::REQUIRED) {
        GTEST_SKIP() << "dictionary codes go to a nullable column of an encoded chunk";
    }
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(layout()));
    Filter first = make_filter(40, {{5, 6}, {30, 31}});
    Filter second = make_filter(70, {{60, 70}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, 40, &first, ColumnContentType::DICT_CODE));
    ASSERT_NO_FATAL_FAILURE(chunk.read(190, 260, &second, ColumnContentType::DICT_CODE));
    ASSERT_NO_FATAL_FAILURE(chunk.finish());
}

// An offset index that does not describe the page that the reader jumps to is an error, and not the rows of another
// page.
TEST_P(OffsetIndexSkipTest, OffsetIndexThatDisagreesWithAPageIsAnError) {
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(layout()));
    // the third page starts 5 rows later than it does
    chunk.offset_index().page_locations[2].first_row_index += 5;
    Filter filter = make_filter(10, {{0, 10}});
    Status st = chunk.read_status(90, 100, &filter);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
    ASSERT_NE(std::string::npos, st.message().find("skip_chunk_test")) << st.message();

    // the offset of a page is the offset of the dictionary page or of a page that is not the page of the index
    SkipChunk other;
    ASSERT_NO_FATAL_FAILURE(other.init(layout()));
    other.offset_index().page_locations[3].offset = other.offset_index().page_locations[2].offset;
    st = other.read_status(130, 140, &filter);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

// A page whose header the reader read and whose data it did not read, and that a jump leaves, is a skipped page, like
// the page that the reader leaves by reading the next header.
TEST_P(OffsetIndexSkipTest, PageSkipCounterCountsThePageThatAJumpLeaves) {
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(layout()));
    Filter zeros(10, 0);
    Filter ones(10, 1);
    {
        // Without the skipping the reader enters the first page for the rows of the range. No row is selected, so it
        // reads the header and not the data.
        ScopedSkipConfig config_guard(false);
        ASSERT_NO_FATAL_FAILURE(chunk.read(0, 10, &zeros));
    }
    ASSERT_EQ(0, chunk.stats().page_skip);
    // The jump to the third page leaves the first page and passes the second.
    ASSERT_NO_FATAL_FAILURE(chunk.read(100, 110, &ones));
    ASSERT_EQ(1, chunk.stats().page_skip);
    ASSERT_EQ(1, chunk.stats().page_skip_by_offset_index);
}

// The page reader ends a chunk by the values of the headers that it has read. After a jump it has not read the headers
// of the pages before the page, so the reader gives it the values of these pages.
TEST_P(OffsetIndexSkipTest, ValuesReadByThePageReaderFollowTheJumps) {
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(layout()));
    Filter sixth_page = make_filter(60, {{0, 60}});
    // the sixth page starts at the row 204 and has 60 rows
    ASSERT_NO_FATAL_FAILURE(chunk.read(204, 264, &sixth_page));
    ASSERT_EQ(264U, chunk.num_values_read());
    // the last page
    Filter last_page = make_filter(40, {{39, 40}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(300, 340, &last_page));
    ASSERT_EQ(340U, chunk.num_values_read());
}

// An offset index of three pages of 100 rows puts the second page at the rows [50, 150) and the third at [150, 300).
// The page that the reader lands on after a jump may have the rows that the index gives it, like the second page does,
// but every page that the reader enters is compared with the index, and the first and the third page are not what the
// index says. The reader returns Corruption with the file and the column, and not the values of other rows.
TEST_P(OffsetIndexSkipTest, EveryPageThatTheReaderEntersIsComparedWithTheIndex) {
    auto make_chunk = [&](SkipChunk* chunk) {
        SkipLayout layout = this->layout();
        layout.page_rows = {100, 100, 100};
        ASSERT_NO_FATAL_FAILURE(chunk->init(layout));
        chunk->offset_index().page_locations[1].first_row_index = 50;
        chunk->offset_index().page_locations[2].first_row_index = 150;
    };
    auto check_corruption = [&](const Status& st) {
        ASSERT_TRUE(st.is_corruption()) << st.to_string();
        ASSERT_NE(std::string::npos, st.message().find("skip_chunk_test")) << st.message();
        ASSERT_NE(std::string::npos, st.message().find("column 'c'")) << st.message();
    };

    // the reader walks into the first page
    {
        SkipChunk chunk;
        ASSERT_NO_FATAL_FAILURE(make_chunk(&chunk));
        Filter filter = make_filter(300, {{20, 21}});
        ASSERT_NO_FATAL_FAILURE(check_corruption(chunk.read_status(0, 300, &filter)));
    }
    // the reader jumps to the second page, which has the rows that the index gives it, and walks into the third
    {
        SkipChunk chunk;
        ASSERT_NO_FATAL_FAILURE(make_chunk(&chunk));
        Filter filter = make_filter(300, {{120, 121}, {160, 161}});
        ASSERT_NO_FATAL_FAILURE(check_corruption(chunk.read_status(0, 300, &filter)));
    }
    // a read without a filter walks over a gap, and crosses the first page
    {
        SkipChunk chunk;
        ASSERT_NO_FATAL_FAILURE(make_chunk(&chunk));
        ASSERT_NO_FATAL_FAILURE(check_corruption(chunk.read_status(150, 160, nullptr)));
    }
    // the config is off: the reader does not use the index and does not compare, so it reads the right rows
    {
        ScopedSkipConfig config_guard(false);
        SkipChunk chunk;
        ASSERT_NO_FATAL_FAILURE(make_chunk(&chunk));
        Filter filter = make_filter(300, {{120, 121}, {160, 161}});
        ASSERT_NO_FATAL_FAILURE(chunk.read(0, 300, &filter));
    }
}

// Sets the configs of the narrowing of the shared buffers for the scope of a test.
class ScopedNarrowConfig {
public:
    ScopedNarrowConfig(bool enable, int32_t distance, int64_t read_ahead, double min_dead_ratio = 0.5)
            : _enable(config::parquet_offset_index_narrow_io_enable),
              _distance(config::io_coalesce_read_max_distance_size),
              _read_ahead(config::parquet_offset_index_narrow_io_read_ahead_size),
              _min_dead_ratio(config::parquet_offset_index_narrow_io_defer_min_dead_ratio) {
        config::parquet_offset_index_narrow_io_enable = enable;
        config::io_coalesce_read_max_distance_size = distance;
        config::parquet_offset_index_narrow_io_read_ahead_size = read_ahead;
        config::parquet_offset_index_narrow_io_defer_min_dead_ratio = min_dead_ratio;
    }
    ~ScopedNarrowConfig() {
        config::parquet_offset_index_narrow_io_enable = _enable;
        config::io_coalesce_read_max_distance_size = _distance;
        config::parquet_offset_index_narrow_io_read_ahead_size = _read_ahead;
        config::parquet_offset_index_narrow_io_defer_min_dead_ratio = _min_dead_ratio;
    }

private:
    bool _enable;
    int32_t _distance;
    int64_t _read_ahead;
    double _min_dead_ratio;
};

using FileRanges = std::vector<std::pair<int64_t, int64_t>>;

// A chunk of 12 pages with 50 rows each, so a page starts at the row 50 * p. The chunk is read through a shared buffer
// of one io range, and the planner of the narrowing advises the buffer.
class OffsetIndexNarrowIoTest : public OffsetIndexSkipTest {
protected:
    SkipLayout narrow_layout() const {
        SkipLayout narrow = layout();
        narrow.page_rows.assign(12, 50);
        narrow.shared_buffer = true;
        return narrow;
    }

    static int64_t page_size(SkipChunk& chunk, size_t page) { return chunk.page_end(page) - chunk.page_begin(page); }

    // The file ranges that the stream loaded, as (begin, end), for the dictionary page when the layout has one and then
    // for the pages.
    FileRanges loaded(SkipChunk& chunk, std::initializer_list<size_t> pages) const {
        FileRanges ranges;
        if (GetParam().dict) {
            ranges.emplace_back(0, chunk.dict_size());
        }
        for (size_t page : pages) {
            ranges.emplace_back(chunk.page_begin(page), chunk.page_end(page));
        }
        return ranges;
    }
};

// A read that selects pages 2, 5 and 9 of 12: the stream loads the dictionary page and the runs of these pages only,
// with one request for each, and the reader gets the same values as without the narrowing.
TEST_P(OffsetIndexNarrowIoTest, LoadsOnlyTheNeededPages) {
    ScopedNarrowConfig config_guard(true, 8, -1);
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(narrow_layout()));
    Filter filter = make_filter(600, {{103, 104}, {250, 251}, {499, 500}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, 600, &filter));

    ASSERT_EQ(loaded(chunk, {2, 5, 9}), chunk.stream().ranges());
    ASSERT_NO_FATAL_FAILURE(chunk.finish());

    // The planner named every other byte of the chunk, and the stream did not load them.
    int64_t needed_bytes = chunk.dict_size();
    for (size_t page : {2, 5, 9}) {
        needed_bytes += page_size(chunk, page);
    }
    ASSERT_EQ(1, chunk.stats().narrow_io_named_buffers);
    ASSERT_EQ(chunk.chunk_size() - needed_bytes, chunk.stats().narrow_io_named_bytes);
    ASSERT_EQ(chunk.chunk_size() - needed_bytes, chunk.shared_stream().skip_ranges_bytes());
    ASSERT_EQ(1, chunk.shared_stream().skip_ranges_split_count());
    ASSERT_EQ(0, chunk.shared_stream().skip_ranges_deferred_bytes());
    ASSERT_EQ(GetParam().dict ? 4 : 3, chunk.shared_stream().shared_io_count());
    ASSERT_EQ(0, chunk.shared_stream().direct_io_count());
    ASSERT_EQ(0, chunk.shared_stream().skip_ranges_recover_count());
    ASSERT_GT(chunk.stats().narrow_io_plan_ns, 0);
}

// With the config off the first header read loads the whole chunk, as before, and the values are the same.
TEST_P(OffsetIndexNarrowIoTest, ConfigOffLoadsTheWholeChunk) {
    ScopedNarrowConfig config_guard(false, 8, -1);
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(narrow_layout()));
    Filter filter = make_filter(600, {{103, 104}, {250, 251}, {499, 500}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, 600, &filter));
    ASSERT_EQ(FileRanges{std::make_pair(static_cast<int64_t>(0), chunk.chunk_size())}, chunk.stream().ranges());
    ASSERT_NO_FATAL_FAILURE(chunk.check_default_rows());
    ASSERT_NO_FATAL_FAILURE(chunk.check_counters());
    ASSERT_EQ(0, chunk.stats().narrow_io_named_buffers);
    ASSERT_EQ(0, chunk.stats().narrow_io_plan_ns);
    ASSERT_EQ(0, chunk.shared_stream().skip_ranges_bytes());
}

// Three consecutive ranges: [0, 150) needs the page 1, [150, 450) needs the pages 4 and 7, and [450, 600) needs the
// page 10. Each range loads only its pages: the buffer of the chunk is split at the first range, the buffer that is not
// loaded is asked again for the second range, and so on. No read is direct, and the reader touches no other page.
TEST_P(OffsetIndexNarrowIoTest, ThreeConsecutiveRangesLoadOnlyTheirPages) {
    ScopedNarrowConfig config_guard(true, 8, 0);
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(narrow_layout()));

    Filter first = make_filter(150, {{60, 61}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, 150, &first));
    ASSERT_EQ(loaded(chunk, {1}), chunk.stream().ranges());
    // the pages 0 and 2 are dead, and the pages behind the range are deferred
    ASSERT_EQ(page_size(chunk, 0) + page_size(chunk, 2), chunk.shared_stream().skip_ranges_bytes());
    ASSERT_EQ(chunk.chunk_size() - chunk.page_begin(3), chunk.shared_stream().skip_ranges_deferred_bytes());

    Filter second = make_filter(300, {{60, 61}, {205, 206}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(150, 450, &second));
    ASSERT_EQ(loaded(chunk, {1, 4, 7}), chunk.stream().ranges());

    Filter third = make_filter(150, {{70, 71}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(450, 600, &third));
    ASSERT_EQ(loaded(chunk, {1, 4, 7, 10}), chunk.stream().ranges());

    ASSERT_NO_FATAL_FAILURE(chunk.finish());
    ASSERT_EQ(0, chunk.shared_stream().direct_io_count());
    ASSERT_EQ(0, chunk.shared_stream().skip_ranges_recover_count());
    // every byte that is not the dictionary page or one of the four pages was cut out
    int64_t loaded_pages = 0;
    for (size_t page : {1, 4, 7, 10}) {
        loaded_pages += page_size(chunk, page);
    }
    ASSERT_EQ(chunk.chunk_size() - chunk.dict_size() - loaded_pages, chunk.shared_stream().skip_ranges_bytes());
}

// The config of the page skipping can be switched off while a scan runs. The first range narrows the chunk and cuts the
// pages 0 and 2 out of the buffer, and the switch comes after it. The reader keeps skipping pages for this chunk, so it
// does not walk over the cut pages: the stream loads the same pages as without the switch, no read is direct, and no read
// recovers a cut span.
TEST_P(OffsetIndexNarrowIoTest, SwitchingTheSkipOffMidScanKeepsTheNarrowedChunkSkipping) {
    ScopedNarrowConfig config_guard(true, 8, 0);
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(narrow_layout()));

    Filter first = make_filter(150, {{60, 61}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, 150, &first));
    ASSERT_EQ(loaded(chunk, {1}), chunk.stream().ranges());
    ASSERT_EQ(page_size(chunk, 0) + page_size(chunk, 2), chunk.shared_stream().skip_ranges_bytes());

    {
        ScopedSkipConfig skip_guard(false);
        Filter second = make_filter(300, {{60, 61}, {205, 206}});
        ASSERT_NO_FATAL_FAILURE(chunk.read(150, 450, &second));
        ASSERT_EQ(loaded(chunk, {1, 4, 7}), chunk.stream().ranges());

        Filter third = make_filter(150, {{70, 71}});
        ASSERT_NO_FATAL_FAILURE(chunk.read(450, 600, &third));
        ASSERT_EQ(loaded(chunk, {1, 4, 7, 10}), chunk.stream().ranges());
    }
    ASSERT_NO_FATAL_FAILURE(chunk.finish());
    ASSERT_EQ(0, chunk.shared_stream().direct_io_count());
    ASSERT_EQ(0, chunk.shared_stream().skip_ranges_recover_count());
}

// The page that holds the end of the range also holds the rows of the next range, so it is not dead: it is deferred, and
// the next range, which starts in it, loads it from the buffer that is asked again.
TEST_P(OffsetIndexNarrowIoTest, PageThatStraddlesTheEndOfTheRangeIsDeferred) {
    ScopedNarrowConfig config_guard(true, 8, 0);
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(narrow_layout()));
    // the range [0, 270) ends in the middle of the page 5, which has the rows 250 to 299
    Filter first = make_filter(270, {{103, 104}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, 270, &first));
    ASSERT_EQ(loaded(chunk, {2}), chunk.stream().ranges());
    // the next range needs a row of the page 5
    Filter second = make_filter(30, {{15, 16}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(270, 300, &second));
    ASSERT_EQ(loaded(chunk, {2, 5}), chunk.stream().ranges());
    ASSERT_EQ(0, chunk.shared_stream().direct_io_count());
    ASSERT_NO_FATAL_FAILURE(chunk.finish());
}

// The first bytes behind the range are read ahead with its pages, up to whole pages: the range [0, 100) needs the page 1
// and ends with it, and the page 2 is read ahead, in the request of the page 1. The next range, which needs the page 2,
// loads nothing.
TEST_P(OffsetIndexNarrowIoTest, ReadAheadIsLoadedWithThePages) {
    // We want to test the read ahead, which the planner applies to a chunk that defers its pages. The range [0, 100)
    // jumps over the page 0 only, so the chunk defers when the page 0 is at least the min dead ratio of the pages 0 and
    // 1. In a column with nulls the page 1 holds a few more values than the page 0, so with the default ratio of a half
    // the share is just under it and the planner loads the rest of the chunk. So the test asks for a lower ratio.
    ScopedNarrowConfig config_guard(true, 8, 1, 0.25);
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(narrow_layout()));
    Filter first = make_filter(100, {{60, 61}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, 100, &first));
    FileRanges expected;
    if (GetParam().dict) {
        expected.emplace_back(0, chunk.dict_size());
    }
    expected.emplace_back(chunk.page_begin(1), chunk.page_end(2));
    ASSERT_EQ(expected, chunk.stream().ranges());

    Filter second = make_filter(50, {{20, 21}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(100, 150, &second));
    ASSERT_EQ(expected, chunk.stream().ranges());
    ASSERT_EQ(0, chunk.shared_stream().direct_io_count());
}

// A chunk whose ranges need all its pages does not defer: the first request loads the whole buffer, and the next ranges
// do not pay a request each.
TEST_P(OffsetIndexNarrowIoTest, DenseChunkIsLoadedWhole) {
    ScopedNarrowConfig config_guard(true, 8, 0);
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(narrow_layout()));
    Filter first = make_filter(300, {{10, 11}, {60, 61}, {110, 111}, {160, 161}, {210, 211}, {260, 261}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, 300, &first));
    ASSERT_EQ(FileRanges{std::make_pair(static_cast<int64_t>(0), chunk.chunk_size())}, chunk.stream().ranges());
    ASSERT_EQ(0, chunk.shared_stream().skip_ranges_deferred_bytes());
    ASSERT_EQ(0, chunk.shared_stream().skip_ranges_bytes());

    Filter second = make_filter(300, {{10, 11}, {120, 121}, {250, 251}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(300, 600, &second));
    ASSERT_EQ(1u, chunk.stream().ranges().size());
}

// Twenty ranges of 30 rows, each with one selected row, need every page of the chunk. The first range loads its page and
// the next page, which is read ahead. The ranges that the buffer serves never ask the planner, and the reader tells it
// what it passed, so the planner sees that no page was dead and stops deferring: the rest of the chunk is loaded with one
// request, not with one request for each page.
TEST_P(OffsetIndexNarrowIoTest, ScanThatNeedsAllPagesStopsDeferring) {
    ScopedNarrowConfig config_guard(true, 8, 1);
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(narrow_layout()));
    for (size_t begin = 0; begin < 600; begin += 30) {
        Filter filter = make_filter(30, {{15, 16}});
        ASSERT_NO_FATAL_FAILURE(chunk.read(begin, begin + 30, &filter));
    }
    ASSERT_NO_FATAL_FAILURE(chunk.check_default_rows());
    ASSERT_NO_FATAL_FAILURE(chunk.check_counters());
    // the dictionary page, the page 0 and the page 1 are one run, and the pages 2 to 11 are the other
    FileRanges expected;
    expected.emplace_back(0, chunk.page_end(1));
    expected.emplace_back(chunk.page_begin(2), chunk.chunk_size());
    ASSERT_EQ(expected, chunk.stream().ranges());
    ASSERT_EQ(0, chunk.shared_stream().direct_io_count());
}

// The reader reads the dictionary page before any range, as a dictionary filter does. The data behind it is deferred, so
// only the dictionary page is loaded, and the first range loads only its pages.
TEST_P(OffsetIndexNarrowIoTest, DictionaryReadLoadsOnlyTheDictionaryPage) {
    if (!GetParam().dict) {
        return;
    }
    ScopedNarrowConfig config_guard(true, 8, 0);
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(narrow_layout()));
    ASSERT_OK(chunk.load_dictionary());
    ASSERT_EQ(FileRanges{std::make_pair(static_cast<int64_t>(0), chunk.dict_size())}, chunk.stream().ranges());
    ASSERT_EQ(chunk.chunk_size() - chunk.dict_size(), chunk.shared_stream().skip_ranges_deferred_bytes());

    Filter filter = make_filter(600, {{480, 481}});
    ASSERT_NO_FATAL_FAILURE(chunk.read(0, 600, &filter));
    ASSERT_EQ(loaded(chunk, {9}), chunk.stream().ranges());
    ASSERT_EQ(0, chunk.shared_stream().direct_io_count());
    ASSERT_NO_FATAL_FAILURE(chunk.finish());
}

// With the dictionary defer off, the dictionary read loads the whole buffer.
TEST_P(OffsetIndexNarrowIoTest, DictionaryReadLoadsTheWholeBufferWhenDeferIsOff) {
    if (!GetParam().dict) {
        return;
    }
    ScopedNarrowConfig config_guard(true, 8, 0);
    const bool saved = config::parquet_offset_index_narrow_io_dict_defer_enable;
    config::parquet_offset_index_narrow_io_dict_defer_enable = false;
    SkipChunk chunk;
    ASSERT_NO_FATAL_FAILURE(chunk.init(narrow_layout()));
    ASSERT_OK(chunk.load_dictionary());
    ASSERT_EQ(FileRanges{std::make_pair(static_cast<int64_t>(0), chunk.chunk_size())}, chunk.stream().ranges());
    config::parquet_offset_index_narrow_io_dict_defer_enable = saved;
}

// The dictionary defer is for a column that skips pages through its offset index afterwards. A scan with the page skipping
// switched off, a chunk without an offset index and a column that the scan has given up on read the buffer whole, as the
// reader does not narrow it later.
TEST_P(OffsetIndexNarrowIoTest, DictionaryReadLoadsTheWholeBufferWhenTheColumnDoesNotSkipPages) {
    if (!GetParam().dict) {
        return;
    }
    ScopedNarrowConfig config_guard(true, 8, 0);
    {
        ScopedSkipConfig skip_guard(false);
        SkipChunk chunk;
        ASSERT_NO_FATAL_FAILURE(chunk.init(narrow_layout()));
        ASSERT_OK(chunk.load_dictionary());
        ASSERT_EQ(FileRanges{std::make_pair(static_cast<int64_t>(0), chunk.chunk_size())}, chunk.stream().ranges());
        ASSERT_EQ(0, chunk.shared_stream().skip_ranges_deferred_bytes());
    }
    {
        SkipChunk chunk;
        ASSERT_NO_FATAL_FAILURE(chunk.init(narrow_layout()));
        chunk.hide_offset_index();
        ASSERT_OK(chunk.load_dictionary());
        ASSERT_EQ(FileRanges{std::make_pair(static_cast<int64_t>(0), chunk.chunk_size())}, chunk.stream().ranges());
        ASSERT_EQ(0, chunk.shared_stream().skip_ranges_deferred_bytes());
    }
    {
        OffsetIndexSkipAdvisor advisor;
        for (int32_t i = 0; i < config::parquet_offset_index_skip_give_up_row_groups; ++i) {
            advisor.observe("c", 0);
        }
        SkipChunk chunk;
        ASSERT_NO_FATAL_FAILURE(chunk.init(narrow_layout()));
        chunk.set_skip_advisor(&advisor);
        ASSERT_OK(chunk.load_dictionary());
        ASSERT_EQ(FileRanges{std::make_pair(static_cast<int64_t>(0), chunk.chunk_size())}, chunk.stream().ranges());
        ASSERT_EQ(0, chunk.shared_stream().skip_ranges_deferred_bytes());
    }
    {
        // a column that skipped a page keeps deferring
        OffsetIndexSkipAdvisor advisor;
        for (int32_t i = 0; i < config::parquet_offset_index_skip_give_up_row_groups; ++i) {
            advisor.observe("c", 1);
        }
        SkipChunk chunk;
        ASSERT_NO_FATAL_FAILURE(chunk.init(narrow_layout()));
        chunk.set_skip_advisor(&advisor);
        ASSERT_OK(chunk.load_dictionary());
        ASSERT_EQ(FileRanges{std::make_pair(static_cast<int64_t>(0), chunk.dict_size())}, chunk.stream().ranges());
    }
}

// Ranges with gaps and a different filter each time, with the shared buffer cut and deferred by the planner: every
// selected row has its value, and the rows of the pages that nobody needs get the default values.
TEST_P(OffsetIndexNarrowIoTest, RandomReads) {
    for (int64_t read_ahead : {-1, 0, 150}) {
        for (int32_t distance : {8, 1000000}) {
            for (double min_dead_ratio : {0.0, 0.5, 1.0}) {
                ScopedNarrowConfig config_guard(true, distance, read_ahead, min_dead_ratio);
                for (uint32_t seed = 1; seed <= 20; ++seed) {
                    SCOPED_TRACE("read-ahead " + std::to_string(read_ahead) + " distance " + std::to_string(distance) +
                                 " ratio " + std::to_string(min_dead_ratio) + " seed " + std::to_string(seed));
                    std::mt19937 gen(seed);
                    SkipChunk chunk;
                    SkipLayout random_layout = layout();
                    random_layout.shared_buffer = true;
                    ASSERT_NO_FATAL_FAILURE(chunk.init(random_layout));
                    if (GetParam().dict && gen() % 2 == 0) {
                        ASSERT_OK(chunk.load_dictionary());
                    }
                    static constexpr size_t kGaps[] = {0, 0, 0, 1, 5, 30, 80};
                    size_t pos = 0;
                    while (pos < chunk.num_rows()) {
                        const size_t begin = std::min(pos + kGaps[gen() % 7], chunk.num_rows());
                        if (begin >= chunk.num_rows()) {
                            break;
                        }
                        const size_t end = std::min(begin + 1 + gen() % 120, chunk.num_rows());
                        Filter filter = make_random_filter(&gen, end - begin);
                        ASSERT_NO_FATAL_FAILURE(chunk.read(begin, end, &filter));
                        pos = end;
                    }
                    ASSERT_NO_FATAL_FAILURE(chunk.check_default_rows());
                    ASSERT_NO_FATAL_FAILURE(chunk.check_counters());
                }
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(AllSkipCases, OffsetIndexSkipTest,
                         testing::Values(SkipParam{ReaderCase::REQUIRED, false}, SkipParam{ReaderCase::REQUIRED, true},
                                         SkipParam{ReaderCase::OPTIONAL, false}, SkipParam{ReaderCase::OPTIONAL, true},
                                         SkipParam{ReaderCase::OPTIONAL_WITH_LEVELS, false},
                                         SkipParam{ReaderCase::OPTIONAL_WITH_LEVELS, true}),
                         [](const testing::TestParamInfo<SkipParam>& info) {
                             std::string name;
                             switch (info.param.reader_case) {
                             case ReaderCase::REQUIRED:
                                 name = "Required";
                                 break;
                             case ReaderCase::OPTIONAL:
                                 name = "Optional";
                                 break;
                             case ReaderCase::OPTIONAL_WITH_LEVELS:
                                 name = "OptionalWithLevels";
                                 break;
                             case ReaderCase::REPEATED:
                                 name = "Repeated";
                                 break;
                             }
                             return name + (info.param.dict ? "Dict" : "Plain");
                         });

INSTANTIATE_TEST_SUITE_P(AllNarrowIoCases, OffsetIndexNarrowIoTest,
                         testing::Values(SkipParam{ReaderCase::REQUIRED, false}, SkipParam{ReaderCase::REQUIRED, true},
                                         SkipParam{ReaderCase::OPTIONAL, false}, SkipParam{ReaderCase::OPTIONAL, true},
                                         SkipParam{ReaderCase::OPTIONAL_WITH_LEVELS, false},
                                         SkipParam{ReaderCase::OPTIONAL_WITH_LEVELS, true}),
                         [](const testing::TestParamInfo<SkipParam>& info) {
                             std::string name;
                             switch (info.param.reader_case) {
                             case ReaderCase::REQUIRED:
                                 name = "Required";
                                 break;
                             case ReaderCase::OPTIONAL:
                                 name = "Optional";
                                 break;
                             case ReaderCase::OPTIONAL_WITH_LEVELS:
                                 name = "OptionalWithLevels";
                                 break;
                             case ReaderCase::REPEATED:
                                 name = "Repeated";
                                 break;
                             }
                             return name + (info.param.dict ? "Dict" : "Plain");
                         });

// A struct with two Optional children reads both with the same range and filter, and takes the nulls of the struct
// from the levels of the first child, so each child has to return a level for every row of the range. The children
// are separate chunks with their own pages, so they skip different pages.
TEST(OffsetIndexSkipStructTest, TwoOptionalChildren) {
    for (bool dict : {false, true}) {
        for (uint32_t seed = 1; seed <= 30; ++seed) {
            SCOPED_TRACE("dict " + std::to_string(dict) + " seed " + std::to_string(seed));
            std::mt19937 gen(seed);
            SkipLayout first_layout;
            first_layout.reader_case = ReaderCase::OPTIONAL_WITH_LEVELS;
            first_layout.dict = dict;
            SkipLayout second_layout = first_layout;
            second_layout.page_rows = {70, 10, 90, 45, 30, 15, 80};
            SkipChunk first;
            SkipChunk second;
            ASSERT_NO_FATAL_FAILURE(first.init(first_layout));
            ASSERT_NO_FATAL_FAILURE(second.init(second_layout));
            ASSERT_EQ(first.num_rows(), second.num_rows());

            size_t pos = 0;
            while (pos < first.num_rows()) {
                const size_t begin = std::min(pos + gen() % 40, first.num_rows());
                if (begin >= first.num_rows()) {
                    break;
                }
                const size_t end = std::min(begin + 1 + gen() % 120, first.num_rows());
                Filter filter = make_random_filter(&gen, end - begin);
                ASSERT_NO_FATAL_FAILURE(first.read(begin, end, &filter));
                ASSERT_NO_FATAL_FAILURE(second.read(begin, end, &filter));
                pos = end;
            }
            ASSERT_NO_FATAL_FAILURE(first.finish());
            ASSERT_NO_FATAL_FAILURE(second.finish());
        }
    }
}

// Reads a REQUIRED FIXED_LEN_BYTE_ARRAY column from a chunk with the type length of the footer. The chunk holds
// one page of 16 bytes: a data page of four values, or a dictionary page that declares one value.
Status read_flba_column(int32_t type_length, bool dictionary_page, size_t* column_size) {
    const std::string data(16, 'x');
    tparquet::PageHeader page_header;
    if (dictionary_page) {
        tparquet::DictionaryPageHeader dict_page_header;
        dict_page_header.__set_num_values(1);
        dict_page_header.__set_encoding(tparquet::Encoding::PLAIN);
        page_header.__set_type(tparquet::PageType::DICTIONARY_PAGE);
        page_header.__set_dictionary_page_header(dict_page_header);
    } else {
        tparquet::DataPageHeader data_page_header;
        data_page_header.__set_num_values(4);
        data_page_header.__set_encoding(tparquet::Encoding::PLAIN);
        data_page_header.__set_definition_level_encoding(tparquet::Encoding::RLE);
        data_page_header.__set_repetition_level_encoding(tparquet::Encoding::RLE);
        page_header.__set_type(tparquet::PageType::DATA_PAGE);
        page_header.__set_data_page_header(data_page_header);
    }
    page_header.__set_uncompressed_page_size(static_cast<int32_t>(data.size()));
    page_header.__set_compressed_page_size(static_cast<int32_t>(data.size()));

    ThriftSerializer serializer(true, 1024);
    uint32_t header_len = 0;
    uint8_t* header_buf = nullptr;
    RETURN_IF_ERROR(serializer.serialize(&page_header, &header_len, &header_buf));
    std::string buffer(reinterpret_cast<const char*>(header_buf), header_len);
    buffer += data;

    tparquet::ColumnChunk chunk;
    chunk.__isset.meta_data = true;
    chunk.meta_data.type = tparquet::Type::FIXED_LEN_BYTE_ARRAY;
    chunk.meta_data.path_in_schema = {"wide"};
    chunk.meta_data.codec = tparquet::CompressionCodec::UNCOMPRESSED;
    chunk.meta_data.num_values = 4;
    chunk.meta_data.total_compressed_size = buffer.size();
    chunk.meta_data.total_uncompressed_size = buffer.size();
    chunk.meta_data.data_page_offset = 0;

    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "wide_flba_test");

    tparquet::SchemaElement root;
    root.__set_name("root");
    root.__set_num_children(1);
    tparquet::SchemaElement leaf;
    leaf.__set_name("wide");
    leaf.__set_type(tparquet::Type::FIXED_LEN_BYTE_ARRAY);
    leaf.__set_type_length(type_length);
    leaf.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    leaf.__set_num_children(0);
    tparquet::FileMetaData t_file_meta;
    t_file_meta.__set_version(1);
    t_file_meta.__set_num_rows(4);
    t_file_meta.__set_schema({root, leaf});
    FileMetaData file_meta;
    RETURN_IF_ERROR(file_meta.init(t_file_meta, true));

    HdfsScannerStats stats;
    ColumnReaderOptions opts;
    opts.stats = &stats;
    opts.file = &file;
    opts.file_meta_data = &file_meta;
    opts.chunk_size = 4096;
    opts.datacache_options = nullptr;

    std::unique_ptr<StoredColumnReader> reader;
    RETURN_IF_ERROR(StoredColumnReader::create(opts, file_meta.schema().get_stored_column_by_column_name("wide"),
                                               &chunk, &reader));
    MutableColumnPtr column = ColumnHelper::create_column(TypeDescriptor::create_varchar_type(100), false);
    Status st = reader->read_range(Range<uint64_t>(0, 4), nullptr, ColumnContentType::VALUE, column.get());
    *column_size = column->size();
    return st;
}

// The footer accepts a FIXED_LEN_BYTE_ARRAY column of any positive width, so the reader of a column that the query
// reads is the one that sees a width that is too large. We expect an error that names the column and the file,
// and no value in the column, for the data page and for the dictionary page.
TEST(StoredColumnReaderWideFlbaTest, TooLargeTypeLengthOfProjectedColumnIsCorruption) {
    size_t column_size = 0;
    ASSERT_OK(read_flba_column(4, false, &column_size));
    ASSERT_EQ(4U, column_size);

    for (int32_t type_length : {ParquetUtils::MAX_FLBA_TYPE_LENGTH + 1, std::numeric_limits<int32_t>::max()}) {
        for (bool dictionary_page : {false, true}) {
            SCOPED_TRACE("type length " + std::to_string(type_length) + (dictionary_page ? " dictionary" : " data"));
            column_size = 1;
            Status st = read_flba_column(type_length, dictionary_page, &column_size);
            ASSERT_TRUE(st.is_corruption()) << st.to_string();
            EXPECT_NE(std::string::npos, st.message().find(std::to_string(type_length))) << st.to_string();
            EXPECT_NE(std::string::npos, st.message().find("'wide'")) << st.to_string();
            EXPECT_NE(std::string::npos, st.message().find("wide_flba_test")) << st.to_string();
            EXPECT_EQ(0U, column_size);
        }
    }
}

} // namespace
} // namespace starrocks::parquet
