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

#include <fmt/core.h>
#include <glog/logging.h>

#include <algorithm>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "column/column.h"
#include "column_reader.h"
#include "common/compiler_util.h"
#include "common/logging.h"
#include "common/status.h"
#include "formats/parquet/level_codec.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"
#include "fs/fs.h"
#include "simd/simd.h"
#include "util/defer_op.h"

namespace tparquet {
class ColumnChunk;
} // namespace tparquet

namespace starrocks::parquet {

class RepeatedStoredColumnReader : public StoredColumnReaderImpl {
public:
    RepeatedStoredColumnReader(const ColumnReaderOptions& opts) : StoredColumnReaderImpl(opts) {}
    ~RepeatedStoredColumnReader() override = default;

    Status init(const ParquetField* field, const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        _reader = std::make_unique<ColumnChunkReader>(_field->max_def_level(), _field->max_rep_level(),
                                                      _field->type_length, chunk_metadata, _opts);
        RETURN_IF_ERROR(_reader->init(_opts.chunk_size));
        return Status::OK();
    }

    void reset_levels() override;

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _reader->def_level_decoder().get_levels(def_levels, num_levels);
        _reader->rep_level_decoder().get_levels(rep_levels, num_levels);
    }

    Status load_specific_page(size_t cur_page_idx, uint64_t offset, uint64_t first_row) override;

    void set_page_change_on_record_boundry() override { _page_change_on_record_boundry = true; }

private:
    // Try to decode enough levels in levels buffer, if there are no enough levels, will throw InternalError msg.
    Status _decode_levels(size_t* num_rows, size_t* num_levels_parsed, level_t** def_levels);

    Status _delimit_rows(const level_t* rep_levels, size_t* num_rows, size_t* num_levels_parsed);

    void _consume_levels(size_t num_values) {
        _reader->def_level_decoder().consume_levels(num_values);
        _reader->rep_level_decoder().consume_levels(num_values);
    }

    Status _next_page() override;

    StatusOr<size_t> _convert_row_to_value(size_t* row) override;
    void _collect_not_null_values(size_t num_levels, bool lazy_flag) override;

    Status _lazy_skip_values() override;
    Status _read_values_on_levels(size_t num_values, starrocks::parquet::ColumnContentType content_type,
                                  starrocks::Column* dst, bool append_default, const FilterData* filter) override;

    bool _cur_page_selected(size_t row_readed, const Filter* filter, size_t to_read) override;

    const FilterData* _convert_filter_row_to_value(const Filter* filter, size_t row_readed) override {
        // if we need it, remember considering null value in ancestor
        // and the cost convert filter of row to value for repeated column
        return nullptr;
    }

    void _set_filter_share(const Filter* filter) override {}

private:
    const ParquetField* _field = nullptr;

    bool _meet_first_record = false;

    size_t _not_null_to_skip = 0;

    NullInfos _null_infos;

    // default is false, but if there is page index, it's true.
    // so that we don't need check next page to know the last record in current page is finished.
    bool _page_change_on_record_boundry = false;
};

class OptionalStoredColumnReader : public StoredColumnReaderImpl {
public:
    OptionalStoredColumnReader(const ColumnReaderOptions& opts) : StoredColumnReaderImpl(opts) {}
    ~OptionalStoredColumnReader() override = default;

    Status init(const ParquetField* field, const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        _reader = std::make_unique<ColumnChunkReader>(_field->max_def_level(), _field->max_rep_level(),
                                                      _field->type_length, chunk_metadata, _opts);
        RETURN_IF_ERROR(_reader->init(_opts.chunk_size));
        return Status::OK();
    }

    // Reset internal state and ready for next read_values
    void reset_levels() override;

    // If need_levels is set, client will get all levels through get_levels function.
    // If need_levels is not set, read_records may not records levels information, this will
    // improve performance. So set this flag when you only needs it.
    void set_need_parse_levels(bool needs_levels) override { _need_parse_levels = needs_levels; }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        // _needs_levels must be true
        DCHECK(_need_parse_levels);

        *rep_levels = nullptr;
        _reader->def_level_decoder().get_levels(def_levels, num_levels);
    }

private:
    Status _decode_levels(size_t* num_rows, size_t* num_levels_parsed, level_t** def_levels);

    void _consume_levels(size_t num_values) { _reader->def_level_decoder().consume_levels(num_values); }
    Status _lazy_skip_values() override;
    Status _read_values_on_levels(size_t num_values, starrocks::parquet::ColumnContentType content_type,
                                  starrocks::Column* dst, bool append_default, const FilterData* filter) override;

    void _append_default_levels(size_t row_nums) override {
        if (_need_parse_levels) {
            _reader->def_level_decoder().append_default_levels(row_nums);
        }
    }

    bool _can_skip_pages() const override { return true; }

    const ParquetField* _field = nullptr;

    // When the flag is false, the information of levels does not need to be materialized,
    // so that the advantages of RLE encoding can be fully utilized and a lot of overhead
    // can be saved in decoding.
    bool _need_parse_levels = false;

    NullInfos _null_infos;
};

class RequiredStoredColumnReader : public StoredColumnReaderImpl {
public:
    RequiredStoredColumnReader(const ColumnReaderOptions& opts) : StoredColumnReaderImpl(opts) {}
    ~RequiredStoredColumnReader() override = default;

    Status init(const ParquetField* field, const tparquet::ColumnChunk* chunk_metadata) {
        _field = field;
        _reader = std::make_unique<ColumnChunkReader>(_field->max_def_level(), _field->max_rep_level(),
                                                      _field->type_length, chunk_metadata, _opts);
        RETURN_IF_ERROR(_reader->init(_opts.chunk_size));
        return Status::OK();
    }

    void reset_levels() override {}

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        *def_levels = nullptr;
        *rep_levels = nullptr;
        *num_levels = 0;
    }

private:
    Status _lazy_skip_values() override;
    Status _read_values_on_levels(size_t num_values, starrocks::parquet::ColumnContentType content_type,
                                  starrocks::Column* dst, bool append_default, const FilterData* filter) override;
    bool _can_skip_pages() const override { return true; }
    const ParquetField* _field = nullptr;
};

void RepeatedStoredColumnReader::reset_levels() {
    _reader->def_level_decoder().reset();
    _reader->rep_level_decoder().reset();
}

Status RepeatedStoredColumnReader::_delimit_rows(const level_t* rep_levels, size_t* num_rows,
                                                 size_t* num_levels_parsed) {
    size_t levels_pos = 0;
    size_t avail_levels = *num_levels_parsed;

#ifndef NDEBUG
    std::stringstream ss;
    ss << "rep=[";
    for (int i = levels_pos; i < avail_levels; ++i) {
        ss << ", " << rep_levels[i];
    }
    ss << "]";
    VLOG_ROW << ss.str();
#endif

    if (!_meet_first_record) {
        _meet_first_record = true;
        if (UNLIKELY(rep_levels[levels_pos] != 0)) {
            return Status::InternalError("bad delimit_rows for repeated column");
        }
        levels_pos++;
    }

    size_t rows_read = 0;
    for (; levels_pos < avail_levels && rows_read < *num_rows; ++levels_pos) {
        rows_read += rep_levels[levels_pos] == 0;
    }

    if (rows_read == *num_rows) {
        // Notice, ++levels_pos in for-loop will take one step forward, so we need -1
        levels_pos--;
        // Had read enough rows, reset _meet_first_record for next read
        _meet_first_record = false;
        DCHECK_EQ(0, rep_levels[levels_pos]);
    } // else {
    //  means  rows_read < *num_rows, levels_pos >= _levels_decoded,
    //  so we need to decode more levels to obtain a complete line or
    //  we have read all the records in this column chunk.
    // }

    VLOG_ROW << "rows_reader=" << rows_read << ", level_parsed=" << levels_pos;
    *num_rows = rows_read;
    *num_levels_parsed = levels_pos;
    return Status::OK();
}

Status RepeatedStoredColumnReader::_decode_levels(size_t* num_rows, size_t* num_levels_parsed, level_t** def_levels) {
    level_t* rep_levels = nullptr;
    size_t avail_levels = _reader->rep_level_decoder().get_avail_levels(*num_rows, &rep_levels);
    if (UNLIKELY(avail_levels == 0)) {
        return Status::InternalError(fmt::format("num values left in cur page: {}, but no available rep levels",
                                                 _num_values_left_in_cur_page));
    }
    if (UNLIKELY(avail_levels != _reader->def_level_decoder().get_avail_levels(*num_rows, def_levels))) {
        return Status::InternalError("rep/def levels' length do not match");
    }
    RETURN_IF_ERROR(_delimit_rows(rep_levels, num_rows, &avail_levels));
    *num_levels_parsed = avail_levels;
    _consume_levels(*num_levels_parsed);
    return Status::OK();
}

void OptionalStoredColumnReader::reset_levels() {
    _reader->def_level_decoder().reset();
}

Status OptionalStoredColumnReader::_decode_levels(size_t* num_rows, size_t* num_levels_parsed, level_t** def_levels) {
    size_t avail_levels = _reader->def_level_decoder().get_avail_levels(*num_rows, def_levels);
    if (UNLIKELY(avail_levels == 0 || *num_rows > avail_levels)) {
        return Status::InternalError(
                fmt::format("def levels need to parsed: {}, def levels parsed: {}", *num_rows, avail_levels));
    }
    *num_levels_parsed = std::min(*num_rows, avail_levels);
    _consume_levels(*num_levels_parsed);
    return Status::OK();
}

Status StoredColumnReader::create(const ColumnReaderOptions& opts, const ParquetField* field,
                                  const tparquet::ColumnChunk* chunk_metadata,
                                  std::unique_ptr<StoredColumnReader>* out) {
    // The schema and the column chunk of the same column both declare its physical type. We pick the
    // decoders from the chunk and the type length and the converter from the schema, so a file where the
    // two differ would feed the decoders a width, a scale or a layout of another type. We stop such a
    // file here.
    if (chunk_metadata->meta_data.type != field->physical_type) {
        return Status::Corruption(
                fmt::format("Parquet column '{}' has type {} in the schema but {} in its column chunk, file: {}",
                            field->name, ::tparquet::to_string(field->physical_type),
                            ::tparquet::to_string(chunk_metadata->meta_data.type), opts.file->filename()));
    }

    if (field->max_rep_level() > 0) {
        std::unique_ptr<RepeatedStoredColumnReader> reader(new RepeatedStoredColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, chunk_metadata));
        *out = std::move(reader);
    } else if (field->max_def_level() > 0) {
        std::unique_ptr<OptionalStoredColumnReader> reader(new OptionalStoredColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, chunk_metadata));
        *out = std::move(reader);
    } else {
        std::unique_ptr<RequiredStoredColumnReader> reader(new RequiredStoredColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, chunk_metadata));
        *out = std::move(reader);
    }
    return Status::OK();
}

size_t StoredColumnReaderImpl::count_not_null(level_t* def_levels, size_t num_parsed_levels, level_t max_def_level) {
    // gcc 12+ auto-vectorises this into the same cmpeq + popcount sequence we used
    // to spell out by hand, on both AVX2 and NEON. Keep the loop scalar so the
    // hot path stays readable.
    size_t count = 0;
    for (size_t i = 0; i < num_parsed_levels; ++i) {
        if (def_levels[i] == max_def_level) {
            count++;
        }
    }
    return count;
}

Status StoredColumnReaderImpl::read_range(const Range<uint64_t>& range, const Filter* filter,
                                          ColumnContentType content_type, Column* dst) {
    // reset_levels() to prepare levels for new reading
    reset_levels();
    if (_skips_pages(filter) || _seeks_without_filter(filter)) {
        return _read_skipping_pages(range, filter, content_type, dst);
    }
    if (_read_cursor < range.begin()) {
        RETURN_IF_ERROR(_skip(range.begin() - _read_cursor));
    }
    return _read(range, filter, content_type, dst);
}

void StoredColumnReaderImpl::set_offset_index_skip(const ColumnOffsetIndexCtx* ctx, bool has_dict_page) {
    if (ctx != nullptr && !_can_skip_pages()) {
        return;
    }
    _skip_index = ctx;
    _skip_index_has_dict_page = has_dict_page;
    _reader->set_page_locations(ctx != nullptr ? &ctx->offset_index.page_locations : nullptr);
}

// We read a column under a filter, and we expect some of its pages to hold no selected row. We fear that reading
// the headers of such pages one by one touches the first bytes of each of them, and a stream that loads a chunk in
// parts (piece by piece, or by the blocks of the data cache) then loads a piece or a block for every page. A stream
// that loads the chunk as one buffer loads it all at the first header, and there we save only the work on the
// pages. So we find the first selected row of the range and look up its page in the offset index. If the pages
// between the reader and that page hold no row that we need, we give the rows before the page their default values
// and load the page by its offset.
//
// The reader keeps the invariant of the sequential read: the cursor is the row where the current page, or the next
// page of the stream when the current one is used up, continues. So when no row of the range is selected, we only
// append the default values and leave the reader where it is: a later read that needs a later page jumps over all
// of these pages at once, and a later read in the current page skips the rows in between as it does when it finds a
// gap between two ranges. A read without a filter comes here only when the reader is behind its range, and then every
// row of the range is selected.
Status StoredColumnReaderImpl::_read_skipping_pages(const Range<uint64_t>& range, const Filter* filter,
                                                    ColumnContentType content_type, Column* dst) {
    const size_t num_rows = range.span_size();
    const size_t num_filter_rows = filter != nullptr ? std::min(num_rows, filter->size()) : num_rows;
    const size_t first_selected = filter != nullptr ? SIMD::find_nonzero(*filter, 0) : 0;
    if (first_selected >= num_filter_rows) {
        _behind = true;
        return _read_values_on_levels(num_rows, content_type, dst, true);
    }

    const size_t reader_page = _skip_index->page_of_row(_read_cursor);
    // The planner asks us what the range needs only if the stream loads a buffer of the chunk during this read.
    const bool narrowed = _opts.narrow_io != nullptr && config::parquet_offset_index_narrow_io_enable;
    if (narrowed) {
        _begin_narrow_io(range, filter, first_selected, num_filter_rows,
                         _num_values_left_in_cur_page > 0 ? reader_page + 1 : reader_page);
    }
    DeferOp end_narrow_io([&] {
        if (narrowed) {
            _opts.narrow_io->end_range(_narrow_chunk);
        }
    });

    size_t row_readed = 0;
    const size_t target_page = _skip_index->page_of_row(range.begin() + first_selected);
    if (target_page > reader_page) {
        const uint64_t target_first_row = _skip_index->page_first_row(target_page);
        if (target_first_row > range.begin()) {
            row_readed = target_first_row - range.begin();
            RETURN_IF_ERROR(_read_values_on_levels(row_readed, content_type, dst, true));
        }
        RETURN_IF_ERROR(_jump_to_page(target_page, reader_page));
    }
    if (_read_cursor < range.begin() + row_readed) {
        RETURN_IF_ERROR(_skip(range.begin() + row_readed - _read_cursor));
    }
    return _read(range, filter, content_type, dst, row_readed);
}

// The jumps of this read go to the pages with a selected row, and the stream loads a shared buffer whole at the first
// byte that a read takes from it. We fear that the buffer holds pages that we do not need, and then they are downloaded
// in vain. So we register the range with the planner of the stream, which asks us what the range needs when the stream
// is about to load a buffer of the chunk (_compute_narrow_need()). We do not compute it here: a scan whose data cache
// holds the blocks never loads a buffer, and then the range costs us a few stores.
void StoredColumnReaderImpl::_begin_narrow_io(const Range<uint64_t>& range, const Filter* filter, size_t first_selected,
                                              size_t num_filter_rows, size_t first_unread_page) {
    const auto& pages = _skip_index->offset_index.page_locations;
    if (_narrow_chunk == nullptr) {
        const tparquet::ColumnMetaData& column_metadata = _reader->metadata();
        const int64_t chunk_begin = column_metadata.__isset.dictionary_page_offset
                                            ? column_metadata.dictionary_page_offset
                                            : column_metadata.data_page_offset;
        _narrow_chunk = _opts.narrow_io->add_chunk(chunk_begin, chunk_begin + column_metadata.total_compressed_size,
                                                   pages.front().offset,
                                                   [this](NarrowIoPlanner::Need* need) { _compute_narrow_need(need); });
    }
    _narrow_range_begin = range.begin();
    _narrow_range_end = range.end();
    _narrow_filter = filter;
    _narrow_first_selected = first_selected;
    _narrow_num_filter_rows = num_filter_rows;
    // The planner defers the pages behind the range only while many pages of the chunk are dead. We tell it how far we
    // have come and how many bytes we jumped over, for every range: the ranges that the stream serves from loaded buffers
    // do not ask the planner anything, and without them the planner would take their pages for dead ones.
    const int64_t read_until = first_unread_page < pages.size()
                                       ? pages[first_unread_page].offset
                                       : pages.back().offset + pages.back().compressed_page_size;
    _opts.narrow_io->begin_range(_narrow_chunk, read_until, _narrow_dead_bytes);
}

// We expect the reader to move forward in its chunk, so a page whose rows all lie in front of the end of the range is
// never read again, unless the range needs it: these pages are dead, and the stream cuts them out of the buffer. The
// pages behind the end of the range may be needed by a later range, and the stream defers them. The work is one pass
// over the pages of the range: the pages are found from the selection with one jump from each page to the next selected
// row, so the cost is the pages of the range and a scan of the filter bytes at most once, and no work for a row.
void StoredColumnReaderImpl::_compute_narrow_need(NarrowIoPlanner::Need* need) {
    const auto& pages = _skip_index->offset_index.page_locations;
    const tparquet::ColumnMetaData& column_metadata = _reader->metadata();
    const int64_t chunk_begin = column_metadata.__isset.dictionary_page_offset ? column_metadata.dictionary_page_offset
                                                                               : column_metadata.data_page_offset;

    // The dictionary page is the bytes before the first data page, and the reader loads it before the first jump, or when
    // it walks the chunk from its start.
    if (_skip_index_has_dict_page && !_reader->dictionary_page_parsed()) {
        need->needed.emplace_back(chunk_begin, pages.front().offset - chunk_begin);
    }
    // The pages with a selected row. `row` moves from the first selected row of a page to the first selected row of a
    // later page.
    size_t row = _narrow_first_selected;
    size_t last_needed_page = 0;
    bool any_needed_page = false;
    while (row < _narrow_num_filter_rows) {
        const size_t page = _skip_index->page_of_row(_narrow_range_begin + row);
        need->needed.emplace_back(pages[page].offset, pages[page].compressed_page_size);
        last_needed_page = page;
        any_needed_page = true;
        if (page + 1 >= pages.size()) {
            break;
        }
        const uint64_t next_first_row = _skip_index->page_first_row(page + 1);
        if (next_first_row >= _narrow_range_begin + _narrow_num_filter_rows) {
            break;
        }
        row = next_first_row - _narrow_range_begin;
        if (_narrow_filter != nullptr) {
            row = SIMD::find_nonzero(*_narrow_filter, row);
        }
    }

    // The page that holds the last row of the range is dead if it ends in front of the range end. Otherwise it holds
    // rows of the later ranges, and the pages in front of it are dead.
    const size_t last_page = _skip_index->page_of_row(_narrow_range_end - 1);
    bool last_page_end_known = false;
    uint64_t last_page_end_row = 0;
    if (last_page + 1 < pages.size()) {
        last_page_end_known = true;
        last_page_end_row = _skip_index->page_first_row(last_page + 1);
    } else if (_opts.row_group_meta != nullptr) {
        last_page_end_known = true;
        last_page_end_row = _skip_index->rg_first_row + _opts.row_group_meta->num_rows;
    }
    const bool last_page_is_dead = last_page_end_known && last_page_end_row <= _narrow_range_end;
    need->dead_end = pages[last_page].offset;
    if (last_page_is_dead) {
        need->dead_end += pages[last_page].compressed_page_size;
    }

    // The next range probably needs the pages behind this one, and a request for each of them costs more than a few
    // pages. So the first bytes behind the range are read ahead, up to whole pages. The read ahead begins at the page
    // that holds the end of the range when it also holds the rows of the next range.
    int64_t read_ahead = config::parquet_offset_index_narrow_io_read_ahead_size;
    if (read_ahead < 0) {
        read_ahead = config::io_coalesce_read_max_distance_size;
    }
    size_t ahead_page = last_page_is_dead ? last_page + 1 : last_page;
    if (any_needed_page) {
        ahead_page = std::max(ahead_page, last_needed_page + 1);
    }
    int64_t ahead_bytes = 0;
    size_t page = ahead_page;
    while (page < pages.size() && ahead_bytes < read_ahead) {
        ahead_bytes += pages[page].compressed_page_size;
        ++page;
    }
    if (page > ahead_page) {
        need->ahead_begin = pages[ahead_page].offset;
        need->ahead_end = pages[page - 1].offset + pages[page - 1].compressed_page_size;
    }
}

// The current page has no values left and the range has rows left. The next page of the stream starts at the next
// row of the range. When the first selected row of the rest of the range is in a later page, the pages in between
// hold no selected row, so we jump over them as _read_skipping_pages() does. When there is no selected row left, the
// rest of the range gets default values, and the reader stays at the start of the next page, which we do not need.
Status StoredColumnReaderImpl::_enter_next_page(const Range<uint64_t>& range, const Filter& filter,
                                                ColumnContentType content_type, Column* dst, size_t* row_readed,
                                                size_t* unread_rows) {
    const size_t num_rows = range.span_size();
    const size_t num_filter_rows = std::min(num_rows, filter.size());
    const size_t next_selected = SIMD::find_nonzero(filter, *row_readed);
    if (next_selected >= num_filter_rows) {
        *unread_rows = num_rows - *row_readed;
        RETURN_IF_ERROR(_read_values_on_levels(*unread_rows, content_type, dst, true));
        *row_readed = num_rows;
        return Status::OK();
    }

    const uint64_t next_row = range.begin() + *row_readed;
    const size_t next_page = _skip_index->page_of_row(next_row);
    const size_t target_page = _skip_index->page_of_row(range.begin() + next_selected);
    if (target_page <= next_page) {
        return _next_page();
    }
    const size_t skipped_rows = _skip_index->page_first_row(target_page) - next_row;
    RETURN_IF_ERROR(_read_values_on_levels(skipped_rows, content_type, dst, true));
    *row_readed += skipped_rows;
    return _jump_to_page(target_page, next_page);
}

// Loads the page by its offset, and counts the pages that the jump passes. reader_page is the page that holds the
// cursor of the reader: the page that is loaded, or the next page of the stream when the loaded one is used up.
Status StoredColumnReaderImpl::_jump_to_page(size_t page_idx, size_t reader_page) {
    const auto& pages = _skip_index->offset_index.page_locations;
    // The data pages need the dictionary, and the dictionary page is before the first of them.
    if (_skip_index_has_dict_page) {
        RETURN_IF_ERROR(_reader->load_dictionary_page());
    }
    const size_t first_passed_page = _num_values_left_in_cur_page > 0 ? reader_page + 1 : reader_page;
    // load_specific_page() compares the page with the offset index.
    RETURN_IF_ERROR(load_specific_page(page_idx, pages[page_idx].offset, _skip_index->page_first_row(page_idx)));

    // The headers of the pages before this page were not read, so we give the page reader the values of these pages
    // and this one. A page has one value for each row, so the values are the rows.
    _reader->set_num_values_read(pages[page_idx].first_row_index + _reader->num_values());

    if (page_idx > first_passed_page) {
        int64_t passed_bytes = 0;
        for (size_t i = first_passed_page; i < page_idx; ++i) {
            passed_bytes += pages[i].compressed_page_size;
        }
        _skipped_pages += page_idx - first_passed_page;
        _narrow_dead_bytes += passed_bytes;
        _opts.stats->page_skip_by_offset_index += page_idx - first_passed_page;
        _opts.stats->page_skip_by_offset_index_undecoded_bytes += passed_bytes;
    }
    return Status::OK();
}

Status StoredColumnReaderImpl::_skip(uint64_t rows_to_skip) {
    uint64_t skipped_row = 0;
    while (skipped_row < rows_to_skip) {
        uint64_t batch_to_skip = std::min(rows_to_skip - skipped_row, (uint64_t)BATCH_PROCESS_SIZE);
        size_t batch_skipped = 0;
        while (batch_skipped < batch_to_skip) {
            if (_num_values_left_in_cur_page > 0) {
                size_t skip_row = static_cast<size_t>(batch_to_skip) - batch_skipped;
                ASSIGN_OR_RETURN(size_t num_values, _convert_row_to_value(&skip_row));
                DCHECK_LE(num_values, _num_values_left_in_cur_page);
                _collect_not_null_values(num_values, num_values != _num_values_left_in_cur_page);
                _num_values_left_in_cur_page -= num_values;
                batch_skipped += skip_row;
            } else {
                RETURN_IF_ERROR(_next_page());
            }
        }
        skipped_row += batch_to_skip;
        // reset_levels() when batch_skipped completed, avoiding use too much memory
        reset_levels();
    }
    return Status::OK();
}

Status StoredColumnReaderImpl::_read(const Range<uint64_t>& range, const starrocks::Filter* filter,
                                     starrocks::parquet::ColumnContentType content_type, starrocks::Column* dst,
                                     size_t row_readed) {
    uint64_t row_count = range.span_size();
    _set_filter_share(filter);
    // The rows at the end of the range that got default values and were not taken from the pages, see
    // _enter_next_page(). The cursor stays before them.
    size_t unread_rows = 0;
    while (row_readed < row_count) {
        if (_num_values_left_in_cur_page > 0) {
            size_t to_read = static_cast<size_t>(row_count) - row_readed;
            ASSIGN_OR_RETURN(size_t num_values, _convert_row_to_value(&to_read));
            DCHECK_LE(num_values, _num_values_left_in_cur_page);
            if (_cur_page_selected(row_readed, filter, to_read)) {
                RETURN_IF_ERROR(_lazy_skip_values());
                auto* value_filter = _convert_filter_row_to_value(filter, row_readed);
                RETURN_IF_ERROR(_read_values_on_levels(num_values, content_type, dst, false, value_filter));
            } else {
                // No row of this portion is selected, so we append defaults and do not decode values. Required and
                // Optional readers skip these rows in the next _lazy_skip_values() through _decoder_pos_in_cur_page,
                // Repeated readers count the values to skip in _read_values_on_levels().
                RETURN_IF_ERROR(_read_values_on_levels(num_values, content_type, dst, true));
            }
            row_readed += to_read;
            _num_values_left_in_cur_page -= num_values;
        } else if (_skips_pages(filter)) {
            RETURN_IF_ERROR(_enter_next_page(range, *filter, content_type, dst, &row_readed, &unread_rows));
        } else {
            RETURN_IF_ERROR(_next_page());
        }
    }
    _read_cursor = range.end() - unread_rows;
    _behind = unread_rows > 0;
    return Status::OK();
}

StatusOr<size_t> StoredColumnReaderImpl::_convert_row_to_value(size_t* row) {
    *row = std::min(*row, _num_values_left_in_cur_page);
    return *row;
}

StatusOr<size_t> RepeatedStoredColumnReader::_convert_row_to_value(size_t* row) {
    size_t num_parsed_levels = 0;
    size_t parsed_row = 0;
    size_t target_row = *row;
    while (num_parsed_levels < _num_values_left_in_cur_page && parsed_row < target_row) {
        size_t row_to_parse = target_row - parsed_row;
        level_t* def_levels = nullptr;
        size_t level_parsed = 0;
        RETURN_IF_ERROR(_decode_levels(&row_to_parse, &level_parsed, &def_levels));
        num_parsed_levels += level_parsed;
        if (num_parsed_levels == _num_values_left_in_cur_page &&
            (_page_change_on_record_boundry || _reader->is_last_page())) {
            row_to_parse += 1;
            _meet_first_record = false;
        }
        parsed_row += row_to_parse;
    }
    *row = parsed_row;
    return num_parsed_levels;
}

Status RequiredStoredColumnReader::_lazy_skip_values() {
    if (!_cur_page_loaded) {
        RETURN_IF_ERROR(_reader->load_page());
        _cur_page_loaded = true;
    }
    size_t row_to_skip = _rows_to_skip_in_cur_page();
    RETURN_IF_ERROR(_reader->skip_values(row_to_skip));
    _decoder_pos_in_cur_page += row_to_skip;
    return Status::OK();
}

Status OptionalStoredColumnReader::_lazy_skip_values() {
    if (!_cur_page_loaded) {
        RETURN_IF_ERROR(_reader->load_page());
        _cur_page_loaded = true;
    }
    size_t row_to_skip = _rows_to_skip_in_cur_page();
    size_t values_to_skip = 0;
    size_t row_skipped = 0;
    while (row_skipped < row_to_skip) {
        level_t* def_levels = nullptr;
        size_t level_parsed = 0;
        size_t cur_to_skip = row_to_skip - row_skipped;
        // skip batch by batch to avoid big memory alloc
        size_t batch_to_skip = std::min(cur_to_skip, (size_t)BATCH_PROCESS_SIZE);
        RETURN_IF_ERROR(_decode_levels(&batch_to_skip, &level_parsed, &def_levels));
        values_to_skip += count_not_null(&def_levels[0], level_parsed, _field->max_def_level());
        row_skipped += level_parsed;
        // reset_levels() to avoiding using too much memory and prepare levels for new reading.
        reset_levels();
    }
    RETURN_IF_ERROR(_reader->skip_values(values_to_skip));
    _decoder_pos_in_cur_page += row_to_skip;
    return Status::OK();
}

Status RepeatedStoredColumnReader::_lazy_skip_values() {
    size_t to_skip = _not_null_to_skip;
    _not_null_to_skip = 0;
    return _reader->skip_values(to_skip);
}

Status RequiredStoredColumnReader::_read_values_on_levels(size_t num_values,
                                                          starrocks::parquet::ColumnContentType content_type,
                                                          starrocks::Column* dst, bool append_default,
                                                          const FilterData* filter) {
    if (append_default) {
        dst->append_default(num_values);
        return Status::OK();
    }
    RETURN_IF_ERROR(_reader->decode_values(num_values, content_type, dst, filter));
    _decoder_pos_in_cur_page += num_values;
    return Status::OK();
}

static void assign_nulls(int16_t* __restrict levels, uint16_t def_level, size_t n, uint8_t* __restrict is_nulls,
                         size_t* ranges_dst, size_t* num_nulls_dst) {
    size_t num_ranges = 1;
    size_t num_nulls{};
    int16_t max_def_level = def_level;
    size_t num_values = n;
    if (num_values > 0) {
        is_nulls[0] = levels[0] < max_def_level;
        num_nulls += is_nulls[0];
    }

    for (size_t i = 1; i < num_values; ++i) {
        is_nulls[i] = levels[i] < max_def_level;
        num_nulls += is_nulls[i];
        num_ranges += is_nulls[i] != is_nulls[i - 1];
    }

    *ranges_dst = num_ranges;
    *num_nulls_dst = num_nulls;
}

Status OptionalStoredColumnReader::_read_values_on_levels(size_t num_values,
                                                          starrocks::parquet::ColumnContentType content_type,
                                                          starrocks::Column* dst, bool append_default,
                                                          const FilterData* filter) {
    if (append_default) {
        _append_default_levels(num_values);
        dst->append_default(num_values);
        return Status::OK();
    } else {
        level_t* def_levels = nullptr;
        size_t level_parsed = 0;
        RETURN_IF_ERROR(_decode_levels(&num_values, &level_parsed, &def_levels));
        DCHECK_EQ(num_values, level_parsed);
        _null_infos.reset_with_capacity(num_values);
        size_t num_ranges = 1;
        size_t num_nulls{};
        // decode def levels
        level_t max_def_level = _field->max_def_level();
        uint8_t* __restrict is_nulls = _null_infos.nulls_data();
        assign_nulls(def_levels, max_def_level, num_values, is_nulls, &num_ranges, &num_nulls);
        _null_infos.num_ranges = num_ranges;
        _null_infos.num_nulls = num_nulls;
        RETURN_IF_ERROR(_reader->decode_values(num_values, _null_infos, content_type, dst, filter));
        _decoder_pos_in_cur_page += num_values;
        return Status::OK();
    }
}

Status RepeatedStoredColumnReader::_read_values_on_levels(size_t num_values,
                                                          starrocks::parquet::ColumnContentType content_type,
                                                          starrocks::Column* dst, bool append_default,
                                                          const FilterData* filter) {
    _null_infos.reset_with_capacity(num_values);

    int null_pos = 0;
    level_t* def_levels = _reader->def_level_decoder().get_forward_levels(num_values);
    uint8_t* __restrict is_nulls = _null_infos.nulls_data();
    int16_t* __restrict levels = def_levels;
    size_t num_nulls{};
    for (int i = 0; i < num_values; ++i) {
        level_t def_level = levels[i];
        is_nulls[null_pos] = (def_level < _field->max_def_level());
        // if current def level < ancestor def level, the ancestor will be not defined too, so that we don't
        // need to add null value to this column. Otherwise, we need to add null value to this column.
        bool has_value = (def_level >= _field->level_info.immediate_repeated_ancestor_def_level);
        num_nulls += is_nulls[null_pos] & has_value;
        null_pos += has_value;
    }
    _null_infos.num_nulls = num_nulls;
    if (append_default) {
        _collect_not_null_values(num_values, num_values != _num_values_left_in_cur_page);
        dst->append_default(null_pos);
        return Status::OK();
    } else if (null_pos != 0) {
        return _reader->decode_values(null_pos, _null_infos, content_type, dst, filter);
    } else {
        return Status::OK();
    }
}

void RepeatedStoredColumnReader::_collect_not_null_values(size_t num_levels, bool lazy_flag) {
    if (lazy_flag) {
        _not_null_to_skip += count_not_null(_reader->def_level_decoder().get_forward_levels(num_levels), num_levels,
                                            _field->max_def_level());
    }
}

Status StoredColumnReaderImpl::_next_page() {
    RETURN_IF_ERROR(_reader->next_page());
    RETURN_IF_ERROR(_reader->load_header());
    if (_reader->current_page_is_dict()) {
        RETURN_IF_ERROR(_reader->load_page());
        return _next_page();
    } else {
        _cur_page_loaded = false;
        _num_values_left_in_cur_page = _reader->num_values();
        _decoder_pos_in_cur_page = 0;
        if (_checks_pages()) {
            RETURN_IF_ERROR(_check_entered_page());
        }
    }
    return Status::OK();
}

Status RepeatedStoredColumnReader::_next_page() {
    RETURN_IF_ERROR(StoredColumnReaderImpl::_next_page());
    _not_null_to_skip = 0;
    return _reader->load_page();
}

bool StoredColumnReaderImpl::_cur_page_selected(size_t row_readed, const Filter* filter, size_t to_read) {
    if (!filter) {
        return true;
    }
    size_t start_row = row_readed;
    int end_row = std::min(start_row + to_read, filter->size()) - 1;
    return SIMD::find_nonzero(*filter, start_row) <= end_row;
}

bool RepeatedStoredColumnReader::_cur_page_selected(size_t row_readed, const Filter* filter, size_t to_read) {
    // for repeated column we can't judge the last row in the current page,
    // to_read is all complete row, row + 1 contains the last incomplete row
    return StoredColumnReaderImpl::_cur_page_selected(row_readed, filter, to_read + 1);
}

Status StoredColumnReaderImpl::load_specific_page(size_t cur_page_idx, uint64_t offset, uint64_t first_row) {
    _reader->set_next_read_page_idx(cur_page_idx);
    RETURN_IF_ERROR(_reader->seek_to_offset(offset));
    RETURN_IF_ERROR(_reader->next_page());
    RETURN_IF_ERROR(_reader->load_header());
    _cur_page_loaded = false;
    _num_values_left_in_cur_page = _reader->num_values();
    _decoder_pos_in_cur_page = 0;
    _read_cursor = first_row;
    if (_checks_pages()) {
        RETURN_IF_ERROR(_check_entered_page());
    }
    return Status::OK();
}

// A page has one value for each row, so the rows that the offset index gives to a page must equal the values in its
// header. The header of a page that the reader enters is parsed already, so the comparison costs nothing, and we make
// it for every page that the reader enters: after a jump, and when the reader walks to the next page, also in the
// walk over a gap. An index that contradicts a header puts the values of the page on rows that are not their rows.
// We cannot go on page by page when we find it: the rows before this page were placed by the index already (as
// default values, or as the values of an earlier page), and a jump has passed pages whose values nobody counted. So
// we return Corruption, and not the values.
Status StoredColumnReaderImpl::_check_entered_page() {
    const auto& pages = _skip_index->offset_index.page_locations;
    // after the header of a data page, the next index is the index of the page after it
    const size_t page_idx = _reader->next_read_page_idx() - 1;
    int64_t expected_rows = -1;
    if (page_idx < pages.size()) {
        if (page_idx + 1 < pages.size()) {
            expected_rows = pages[page_idx + 1].first_row_index - pages[page_idx].first_row_index;
        } else if (_opts.row_group_meta != nullptr) {
            expected_rows = _opts.row_group_meta->num_rows - pages[page_idx].first_row_index;
        }
    }
    if (UNLIKELY(_reader->current_page_is_dict() || page_idx >= pages.size() ||
                 (expected_rows >= 0 && static_cast<int64_t>(_reader->num_values()) != expected_rows))) {
        std::string column;
        for (const std::string& part : _reader->metadata().path_in_schema) {
            column += column.empty() ? part : "." + part;
        }
        return Status::Corruption(fmt::format(
                "Parquet page {} of column '{}' has {} values, but the offset index gives it {} rows, file: {}",
                page_idx, column, _reader->current_page_is_dict() ? 0 : _reader->num_values(), expected_rows,
                _opts.file->filename()));
    }
    return Status::OK();
}

Status RepeatedStoredColumnReader::load_specific_page(size_t cur_page_idx, uint64_t offset, uint64_t first_row) {
    RETURN_IF_ERROR(StoredColumnReaderImpl::load_specific_page(cur_page_idx, offset, first_row));
    _not_null_to_skip = 0;
    return _reader->load_page();
}
} // namespace starrocks::parquet
