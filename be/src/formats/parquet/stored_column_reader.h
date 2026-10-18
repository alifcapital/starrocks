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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "column/vectorized_fwd.h"
#include "column_reader.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "formats/parquet/column_chunk_reader.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"
#include "gen_cpp/parquet_types.h"
#include "simd/simd.h"
#include "storage/range.h"

namespace tparquet {
class ColumnChunk;
} // namespace tparquet

namespace starrocks {
class Column;
class NullableColumn;

namespace parquet {
struct ParquetField;
} // namespace parquet
} // namespace starrocks

namespace starrocks::parquet {

class ColumnChunkReader;

class StoredColumnReader {
public:
    static Status create(const ColumnReaderOptions& opts, const ParquetField* field,
                         const tparquet::ColumnChunk* _chunk_metadata, std::unique_ptr<StoredColumnReader>* out);
    virtual ~StoredColumnReader() = default;

    // If need_levels is set, client will get all levels through get_levels function.
    // If need_levels is not set, read_records may not records levels information, this will
    // improve performance. So set this flag when you only needs it.
    // TODO(zc): to recosiderate to move this flag to StoredColumnReaderOptions
    // StoredColumnReaderOptions is shared by all StoredColumnReader, but we only want set StoredColumnReader specifically,
    // so currently we can't put need_parse_levels into StoredColumnReaderOptions.
    virtual void set_need_parse_levels(bool need_parse_levels) {}

    virtual Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnContentType content_type,
                              Column* dst) = 0;

    // This function can only be called after calling read_values. This function returns the
    // levels for last read_values.
    virtual void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) = 0;

    virtual Status get_dict_values(Column* column) = 0;

    virtual Status get_dict_values(const Buffer<int32_t>& dict_codes, const NullableColumn& nulls, Column* column) = 0;

    virtual Status load_dictionary_page() { return Status::InternalError("Not supported load_dictionary_page"); }

    virtual Status load_specific_page(size_t cur_page_idx, uint64_t offset, uint64_t first_row) {
        return Status::InternalError("Not supported load_specific_page");
    }

    virtual void set_page_num(size_t page_num) {}

    // The pages of the offset index of the chunk, which must outlive the reader. The page reader uses them to read a
    // page header with no more bytes than its page has, whether the reader skips pages or not.
    virtual void set_page_locations(const std::vector<tparquet::PageLocation>* page_locations) {}

    virtual void set_page_change_on_record_boundry() {}

    // A reader that supports offset index skipping, when it reads a range under a filter, finds the pages that hold
    // a selected row by their offsets in the offset index. It gives the rows of the other pages their default
    // values, as it does for a page without a selected row that it has loaded, and it neither reads their headers
    // nor decodes them.
    virtual bool supports_offset_index_skip() const { return false; }

    // Switches skipping on, with the offset index of the column chunk, which must outlive the reader. nullptr
    // switches it off. has_dict_page tells that the chunk has a dictionary page before its first data page.
    virtual void set_offset_index_skip(const ColumnOffsetIndexCtx* ctx, bool has_dict_page) {}

    // True if read_range() with this filter moves the reader to the page of the first selected row by itself, so the
    // caller does not need to load the page where the range starts.
    virtual bool seeks_by_offset_index(const Filter* filter) const { return false; }

    // The pages that this reader jumped over through the offset index.
    virtual int64_t offset_index_skipped_pages() const { return 0; }
};

class StoredColumnReaderImpl : public StoredColumnReader {
public:
    StoredColumnReaderImpl(const ColumnReaderOptions& opts) : _opts(opts) {}

    virtual ~StoredColumnReaderImpl() = default;

    // Reset internal state and ready for next read_values
    virtual void reset_levels() = 0;

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnContentType content_type,
                      Column* dst) override;

    Status get_dict_values(Column* column) override { return _reader->get_dict_values(column); }

    Status get_dict_values(const Buffer<int32_t>& dict_codes, const NullableColumn& nulls, Column* column) override {
        return _reader->get_dict_values(dict_codes, nulls, column);
    }

    Status load_dictionary_page() override { return _reader->load_dictionary_page(); }

    Status load_specific_page(size_t cur_page_idx, uint64_t offset, uint64_t first_row) override;

    void set_page_num(size_t page_num) override { _reader->set_page_num(page_num); }

    void set_page_locations(const std::vector<tparquet::PageLocation>* page_locations) override {
        _reader->set_page_locations(page_locations);
    }

    bool supports_offset_index_skip() const override { return _can_skip_pages(); }

    void set_offset_index_skip(const ColumnOffsetIndexCtx* ctx, bool has_dict_page) override;

    bool seeks_by_offset_index(const Filter* filter) const override { return _skips_pages(filter); }

    int64_t offset_index_skipped_pages() const override { return _skipped_pages; }

    static size_t count_not_null(level_t* def_levels, size_t num_parsed_levels, level_t max_def_level);

protected:
    virtual Status _next_page();
    virtual bool _cur_page_selected(size_t row_readed, const Filter* filter, size_t to_read);

    // True for the readers whose page holds one value for each row, so the first row of a page and the rows of a
    // page come from the offset index without reading the page.
    virtual bool _can_skip_pages() const { return false; }

    // The rows of the current page between the decoders and the first row of the next decode.
    size_t _rows_to_skip_in_cur_page() const {
        size_t num_rows_consumed = _reader->num_values() - _num_values_left_in_cur_page;
        DCHECK_GE(num_rows_consumed, _decoder_pos_in_cur_page);
        return num_rows_consumed - _decoder_pos_in_cur_page;
    }

    // for RequiredColumn, there is no need to get levels.
    // for RepeatedColumn, there is no possible to get default levels.
    // for OptionalColumn, we will override it.
    virtual void _append_default_levels(size_t row_nums) {}

    std::unique_ptr<ColumnChunkReader> _reader;
    // The offset index of the chunk, when skipping pages through it is switched on by set_offset_index_skip().
    const ColumnOffsetIndexCtx* _skip_index = nullptr;
    bool _skip_index_has_dict_page = false;
    // The last read gave default values to the rows at its end and did not move the reader over them, so the reader
    // is before the end of the range.
    bool _behind = false;
    int64_t _skipped_pages = 0;
    size_t _num_values_left_in_cur_page = 0;
    // The Required and Optional readers do not move the value decoder and the def level decoder over rows that are
    // skipped or filtered out, because the page may end before a later row is read. We keep the number of rows of the
    // current page that the decoders have passed, so the next decode first skips the rows in between.
    size_t _decoder_pos_in_cur_page = 0;
    const ColumnReaderOptions& _opts;
    bool _cur_page_loaded = false;
    uint64_t _read_cursor = _opts.first_row_index;
    static constexpr size_t BATCH_PROCESS_SIZE = 8192;
    // The chunk in the planner of the narrowing. It is set when the reader first reads a range through the offset index
    // with a selected row, and it stays set until the reader is gone. It is null while the reader has no planner or has
    // not read such a range. The stream may have cut pages out of the buffers of a chunk that is set (see
    // _skip_enabled()).
    NarrowIoPlanner::Chunk* _narrow_chunk = nullptr;
    // The range that the reader is reading, for the planner. They are valid only inside read_range().
    uint64_t _narrow_range_begin = 0;
    uint64_t _narrow_range_end = 0;
    const Filter* _narrow_filter = nullptr;
    size_t _narrow_first_selected = 0;
    size_t _narrow_num_filter_rows = 0;
    // The bytes of the pages that the jumps of the reader passed. The planner defers the pages behind a range only while
    // many bytes of the chunk are dead, and this counts them for every range, also for the ranges that need no new buffer.
    int64_t _narrow_dead_bytes = 0;

private:
    Status _next_selected_page(size_t records_to_read, ColumnContentType content_type, size_t* records_to_skip,
                               Column* dst);

    Status _lazy_load_page_rows(size_t batch_size, ColumnContentType content_type, Column* dst);

    Status _skip(uint64_t row_to_skip);
    // `row_readed` rows at the start of the range have been appended to dst already.
    Status _read(const Range<uint64_t>& range, const Filter* filter, ColumnContentType content_type, Column* dst,
                 size_t row_readed = 0);

    // The config can be switched off while a scan runs. We fear that a reader which walks its pages one after the other
    // after the switch reads the pages that the narrowing cut out of the buffers of its chunk, and the stream does not
    // hold them: the walk costs a request for a span of cut pages, at best, and two requests for a page, at worst. So
    // a chunk that the narrowing has registered keeps skipping pages until the reader is gone, and the switch takes
    // effect for the chunks that start after it.
    bool _skip_enabled() const { return config::parquet_offset_index_skip_enable || _narrow_chunk != nullptr; }
    // Page skipping through the offset index. It applies to a read under a filter, when the skipping is on and the
    // reader has the offset index.
    bool _skips_pages(const Filter* filter) const {
        return _skip_index != nullptr && filter != nullptr && _skip_enabled();
    }
    // A read without a filter goes through the offset index too when an earlier read left the reader behind its
    // range: the pages between the reader and the range may be pages that no read needs, and walking over them reads
    // their headers.
    bool _seeks_without_filter(const Filter* filter) const {
        return filter == nullptr && _behind && _skip_index != nullptr && _skip_enabled();
    }
    Status _read_skipping_pages(const Range<uint64_t>& range, const Filter* filter, ColumnContentType content_type,
                                Column* dst);
    Status _enter_next_page(const Range<uint64_t>& range, const Filter& filter, ColumnContentType content_type,
                            Column* dst, size_t* row_readed, size_t* unread_rows);
    Status _jump_to_page(size_t page_idx, size_t reader_page);
    // Tells the planner of the shared buffers that the reader begins to read the range (see NarrowIoPlanner). The planner
    // asks what the range needs, with _compute_narrow_need(), only when the stream is about to load a buffer of the chunk,
    // so we keep the range and the filter, which live until the read ends. `first_selected` is the first selected row of
    // the range, and `num_filter_rows` is the number of rows of the range that the filter covers. `first_unread_page` is
    // the first page that the reader has neither read nor jumped over.
    void _begin_narrow_io(const Range<uint64_t>& range, const Filter* filter, size_t first_selected,
                          size_t num_filter_rows, size_t first_unread_page);
    // What the range that _begin_narrow_io() was given needs from the chunk.
    void _compute_narrow_need(NarrowIoPlanner::Need* need);

    // The reader compares every page that it enters with the offset index while the skipping is on.
    bool _checks_pages() const { return _skip_index != nullptr && _skip_enabled(); }
    Status _check_entered_page();

    // input is target row, this function will convert row to values bases on _num_values_left_in_cur_page,
    // only convert in the current page, the return is the num of values that will be used and
    // the input target row pointer is changed to the result row that can be dealt in current page.
    virtual StatusOr<size_t> _convert_row_to_value(size_t* row);
    // to skip values bases no levels we need to collect_not_null_values when decoding levels,
    // for required column, we don't need levels information,
    // for optional column, we collect levels information lazily,
    // for repeated column, we collect levels information after _convert_row_to_value which had decoded the levels.
    virtual void _collect_not_null_values(size_t num_levels, bool lazy_flag) {}

    // Called before a decode. Required and Optional readers load the page data if needed and move the decoders over
    // the rows that were skipped or filtered out since the last decode. Repeated readers skip the values counted by
    // _collect_not_null_values().
    virtual Status _lazy_skip_values() = 0;
    virtual Status _read_values_on_levels(size_t num_values, starrocks::parquet::ColumnContentType content_type,
                                          starrocks::Column* dst, bool append_default,
                                          const FilterData* filter = nullptr) = 0;

    // We offer the filter of the rows that are decoded now to the decoder and let it decide whether to use it. The
    // decoder knows the encoding, the width of the values and the size of its dictionary. Required and Optional
    // readers have one value per row, so the filter of the rows is the filter of the values.
    virtual const FilterData* _convert_filter_row_to_value(const Filter* filter, size_t row_readed) {
        if (!filter || !config::parquet_push_down_filter_to_decoder_enable) {
            return nullptr;
        }
        return filter->data() + row_readed;
    }

    // The decoder decides by the share of the rows that the filter of the read range keeps. We count the rows once
    // for the range, before the first decode, and the column chunk reader tells the share to the decoder of the
    // page that is decoded, and to the next decoder when the range goes on in a page of another encoding. So the
    // pages, the batches and the runs of one range decide alike, and no decoder counts the filter.
    virtual void _set_filter_share(const Filter* filter) {
        if (!filter || filter->empty() || !config::parquet_push_down_filter_to_decoder_enable) {
            return;
        }
        _reader->set_filter_share(static_cast<double>(SIMD::count_nonzero(*filter)) /
                                  static_cast<double>(filter->size()));
    }
};

} // namespace starrocks::parquet
