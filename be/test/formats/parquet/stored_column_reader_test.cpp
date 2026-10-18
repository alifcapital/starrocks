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
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "column/column_helper.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/stored_column_reader_with_index.h"
#include "formats/parquet/utils.h"
#include "fs/fs.h"
#include "gen_cpp/parquet_types.h"
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

} // namespace
} // namespace starrocks::parquet
