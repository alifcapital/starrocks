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
#include "formats/parquet/parquet_test_util/recording_input_stream.h"
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

        _stream = std::make_shared<RecordingInputStream>(std::move(buffer));
        _file = std::make_unique<RandomAccessFile>(_stream, "skip_chunk_test");

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
    std::vector<ExpectedValue> _values;
    std::vector<int32_t> _dict_values;
    std::map<int32_t, int32_t> _dict_codes;

    std::shared_ptr<RecordingInputStream> _stream;
    std::unique_ptr<RandomAccessFile> _file;
    HdfsScannerStats _stats;
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

} // namespace
} // namespace starrocks::parquet
