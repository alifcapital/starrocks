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

#include "formats/parquet/scalar_column_reader.h"

#include "common/compiler_util.h"
#include "common/config.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/offset_index_skip_advisor.h"
#include "formats/parquet/parquet_block_split_bloom_filter.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/stored_column_reader_with_index.h"
#include "formats/parquet/utils.h"
#include "gutil/casts.h"
#include "io/cache_input_stream.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/global_dict/dict_column.h"
#include "runtime/types.h"
#include "simd/gather.h"
#include "simd/simd.h"
#include "statistics_helper.h"

namespace starrocks::parquet {

static std::string describe_column(const ParquetField* field) {
    return field != nullptr ? field->name : std::string("?");
}

// The column index, the offset index and the bloom filter are optional, and a caller that gets an error from
// one of them goes on without it. So we log the error here, where the file is known to be wrong.
static Status invalid_index(const std::string& message) {
    LOG(WARNING) << message;
    return Status::Corruption(message);
}

// A bloom filter is a header, then a bitset of at most BloomFilter::MAXIMUM_BYTES. The footer gives the
// length of both when it knows it, and 0 when it does not.
static constexpr int64_t kMaxBloomFilterRegionBytes = BloomFilter::MAXIMUM_BYTES + 1024;

// FixedValueColumnReader
Status FixedValueColumnReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    Column* dst_col = dst->as_mutable_raw_ptr();
    for (uint64_t i = range.begin(); i < range.end(); ++i) {
        dst_col->append_datum(_fixed_value);
    }
    return Status::OK();
}

StatusOr<bool> FixedValueColumnReader::row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                                 CompoundNodeType pred_relation,
                                                                 const uint64_t rg_first_row,
                                                                 const uint64_t rg_num_rows) const {
    ZoneMapDetail zone_map{_fixed_value, _fixed_value, _fixed_value.is_null()};
    return !PredicateFilterEvaluatorUtils::zonemap_satisfy(predicates, zone_map, pred_relation);
}

StatusOr<bool> FixedValueColumnReader::page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                                  SparseRange<uint64_t>* row_ranges,
                                                                  CompoundNodeType pred_relation,
                                                                  const uint64_t rg_first_row,
                                                                  const uint64_t rg_num_rows) {
    DCHECK(row_ranges->empty());
    ZoneMapDetail zone_map{_fixed_value, _fixed_value, _fixed_value.is_null()};

    // is_satisfy = true means no filter happened, return false
    // is_satisfy = false means entire row group can be filtered, filter happened, return true
    return !PredicateFilterEvaluatorUtils::zonemap_satisfy(predicates, zone_map, pred_relation);
}

// RawColumnReader

RawColumnReader::~RawColumnReader() {
    // The row group is read, and the scan learns whether the offset index that this reader read has paid.
    if (_skip_advisor != nullptr) {
        _skip_advisor->observe(_skip_column, _offset_index_skip_enabled ? _reader->offset_index_skipped_pages() : 0);
    }
}

void RawColumnReader::collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                              int64_t* end_offset, ColumnIOTypeFlags types, bool active) {
    const auto& column = *get_chunk_metadata();
    if ((types & ColumnIOType::PAGES) != 0) {
        const tparquet::ColumnMetaData& column_metadata = column.meta_data;
        if (_offset_index_ctx != nullptr && !_offset_index_ctx->page_selected.empty()) {
            // add dict page
            if (column_metadata.__isset.dictionary_page_offset) {
                auto r = io::SharedBufferedInputStream::IORange(
                        column_metadata.dictionary_page_offset,
                        column_metadata.data_page_offset - column_metadata.dictionary_page_offset, active);
                ranges->emplace_back(r);
                *end_offset = std::max(*end_offset, r.offset + r.size);
            }
            _offset_index_ctx->collect_io_range(ranges, end_offset, active);
        } else {
            int64_t offset = 0;
            if (column_metadata.__isset.dictionary_page_offset) {
                offset = column_metadata.dictionary_page_offset;
            } else {
                offset = column_metadata.data_page_offset;
            }
            int64_t size = column_metadata.total_compressed_size;
            auto r = io::SharedBufferedInputStream::IORange(offset, size, active);
            ranges->emplace_back(r);
            *end_offset = std::max(*end_offset, offset + size);
        }
    }
    if ((types & ColumnIOType::PAGE_INDEX) != 0) {
        // only active column need column index
        // the offset and length come from the footer, and a range that is not in the file is not read
        if (column.__isset.column_index_offset && active &&
            _check_region(column.column_index_offset, column.column_index_length, kMaxPageIndexBytes, "column index")
                    .ok()) {
            auto r = io::SharedBufferedInputStream::IORange(column.column_index_offset, column.column_index_length);
            ranges->emplace_back(r);
        }
        // all column need offset index
        if (column.__isset.offset_index_offset &&
            _check_region(column.offset_index_offset, column.offset_index_length, kMaxPageIndexBytes, "offset index")
                    .ok()) {
            auto r = io::SharedBufferedInputStream::IORange(column.offset_index_offset, column.offset_index_length);
            ranges->emplace_back(r);
        }
    } else if ((types & ColumnIOType::OFFSET_INDEX) != 0) {
        // We plan the offset index alone for a column that reads it to skip pages. A Repeated column reads page
        // by page, a chunk of one data page has no page to skip, and a column that the scan has seen skip no page
        // does not read it any more, so we do not plan the index for them.
        const ParquetField* field = get_column_parquet_field();
        if (column.__isset.offset_index_offset && field != nullptr && field->max_rep_level() == 0 &&
            _may_read_offset_index_for_skip() &&
            _check_region(column.offset_index_offset, column.offset_index_length, kMaxPageIndexBytes, "offset index")
                    .ok()) {
            auto r = io::SharedBufferedInputStream::IORange(column.offset_index_offset, column.offset_index_length);
            ranges->emplace_back(r);
        }
    }
    if ((types & ColumnIOType::BLOOM_FILTER) != 0) {
        _collect_bloom_filter_io_range(ranges);
    }
}

// We want the read of the filter in _init_column_bloom_filter() to land in a shared buffer, and to share a request with
// the ranges next to it. We fear that the stream loads a filter that no probe reads: it joins the ranges that are close
// to each other, so an unused filter next to a used range is loaded with it. So we plan the region only when the probe
// of this column (_row_group_bloom_filter()) is going to read it:
// - The column probes a filter, and its type can use the filter.
// - The footer gives the length. Without it the reader reads the header, and then the bitset, whose size is in the
//   header only. We cannot tell where the region ends, and a range of the header alone joins no other range, so we plan
//   nothing.
// - The region passes the check of the reader. We fear that a wrong region in the footer overlaps another range, which
//   makes set_io_ranges() fail for the whole file, while the reader would only go on without the filter.
// - The adaptive check does not skip the filter. It skips a filter that is larger than the bytes of the rows that the
//   probe keeps. The probe never keeps more than the row group, so a filter that is skipped for the row group is
//   skipped for any span.
void RawColumnReader::_collect_bloom_filter_io_range(
        std::vector<io::SharedBufferedInputStream::IORange>* ranges) const {
    const TypeDescriptor* col_type = _bloom_filter_type();
    const tparquet::ColumnChunk* chunk = get_chunk_metadata();
    const ParquetField* field = get_column_parquet_field();
    if (col_type == nullptr || chunk == nullptr || field == nullptr) {
        return;
    }
    const tparquet::ColumnMetaData& meta = chunk->meta_data;
    if (!meta.__isset.bloom_filter_offset || !meta.__isset.bloom_filter_length || meta.bloom_filter_length <= 0) {
        return;
    }
    if (!check_type_can_apply_bloom_filter(*col_type, *field)) {
        return;
    }
    const int64_t num_rows = _opts.row_group_meta != nullptr ? _opts.row_group_meta->num_rows : meta.num_values;
    if (!adaptive_judge_if_apply_bloom_filter(num_rows).value_or(true)) {
        return;
    }
    if (!_check_region(meta.bloom_filter_offset, meta.bloom_filter_length, kMaxBloomFilterRegionBytes, "bloom filter")
                 .ok()) {
        return;
    }
    ranges->emplace_back(meta.bloom_filter_offset, meta.bloom_filter_length);
}

Status RawColumnReader::_check_region(int64_t offset, int64_t length, int64_t max_length, const char* what) const {
    const uint64_t file_size = _opts.file_size;
    if (offset < 0 || length < 0 || length > max_length ||
        (file_size > 0 &&
         (static_cast<uint64_t>(offset) > file_size || static_cast<uint64_t>(length) > file_size - offset))) {
        return invalid_index(fmt::format(
                "Invalid {} of parquet column '{}', offset={}, length={}, file size={}, file: {}", what,
                describe_column(get_column_parquet_field()), offset, length, file_size, _opts.file->filename()));
    }
    return Status::OK();
}

Status RawColumnReader::_validate_offset_index(const tparquet::OffsetIndex& offset_index) const {
    const auto& pages = offset_index.page_locations;
    bool valid = !pages.empty() && pages[0].first_row_index == 0;
    for (size_t i = 0; valid && i < pages.size(); ++i) {
        const auto& page = pages[i];
        valid = page.offset >= 0 && page.compressed_page_size > 0 &&
                (_opts.file_size == 0 ||
                 (static_cast<uint64_t>(page.offset) <= _opts.file_size &&
                  static_cast<uint64_t>(page.compressed_page_size) <= _opts.file_size - page.offset)) &&
                (i == 0 || (page.first_row_index > pages[i - 1].first_row_index && page.offset > pages[i - 1].offset));
    }
    // Each page holds at least one value and starts at a row of the row group, so the pages cannot be more than
    // the values of the chunk, and the last page starts before the end of the row group.
    if (valid) {
        const int64_t num_values = get_chunk_metadata()->meta_data.num_values;
        valid = static_cast<int64_t>(pages.size()) <= num_values &&
                (_opts.row_group_meta == nullptr || pages.back().first_row_index < _opts.row_group_meta->num_rows);
    }
    if (!valid) {
        return invalid_index(fmt::format(
                "Invalid offset index of parquet column '{}' with {} pages, the chunk has {} values and the row "
                "group has {} rows, file: {}",
                describe_column(get_column_parquet_field()), pages.size(), get_chunk_metadata()->meta_data.num_values,
                _opts.row_group_meta != nullptr ? _opts.row_group_meta->num_rows : -1, _opts.file->filename()));
    }
    return Status::OK();
}

StatusOr<tparquet::OffsetIndex*> RawColumnReader::get_offset_index(const uint64_t rg_first_row) {
    if (_offset_index_ctx == nullptr) {
        // a footer written without the length of the offset index cannot be read as a whole
        if (!get_chunk_metadata()->__isset.offset_index_length) {
            return Status::NotSupported("The offset index has no length");
        }
        const int64_t offset_index_offset = get_chunk_metadata()->offset_index_offset;
        const int32_t offset_index_length = get_chunk_metadata()->offset_index_length;
        RETURN_IF_ERROR(_check_region(offset_index_offset, offset_index_length, kMaxPageIndexBytes, "offset index"));
        if (offset_index_length == 0) {
            return invalid_index(fmt::format("Empty offset index of parquet column '{}', file: {}",
                                             describe_column(get_column_parquet_field()), _opts.file->filename()));
        }
        std::vector<uint8_t> offset_index_data(offset_index_length);
        {
            io::MetadataReadScope scope(io::MetadataKind::PAGE_INDEX, config::parquet_index_datacache_enable);
            RETURN_IF_ERROR(
                    _opts.file->read_at_fully(offset_index_offset, offset_index_data.data(), offset_index_length));
        }

        // We keep the index only after it is read and checked, so that a failure leaves no half-built index
        // for the next caller to find.
        auto ctx = std::make_unique<ColumnOffsetIndexCtx>();
        ctx->rg_first_row = rg_first_row;
        uint32_t length = offset_index_length;
        RETURN_IF_ERROR(
                deserialize_thrift_msg(offset_index_data.data(), &length, TProtocolType::COMPACT, &ctx->offset_index));
        RETURN_IF_ERROR(_validate_offset_index(ctx->offset_index));
        _offset_index_ctx = std::move(ctx);
        _share_page_locations();
    }
    return &_offset_index_ctx->offset_index;
}

void RawColumnReader::_share_page_locations() {
    if (_reader != nullptr && _offset_index_ctx != nullptr) {
        _reader->set_page_locations(&_offset_index_ctx->offset_index.page_locations);
    }
}

void RawColumnReader::select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) {
    // The pages are selected once. The selection appends one entry per page, and the reader is wrapped with a
    // reader that takes its page count from that selection, so a second call would leave twice the pages of the
    // offset index in the selection and wrap the reader again. The caller calls once for a reader. If it
    // calls again, the first selection is the one that the I/O ranges and the reads follow.
    if (_offset_index_ctx != nullptr && !_offset_index_ctx->page_selected.empty()) {
        return;
    }
    if (range.size() == 0) {
        return;
    }
    if (_offset_index_ctx == nullptr) {
        if (!get_chunk_metadata()->__isset.offset_index_offset) {
            return;
        }
        auto st = get_offset_index(rg_first_row);
        if (!st.ok()) {
            return;
        }
    }
    size_t page_num = _offset_index_ctx->offset_index.page_locations.size();
    size_t range_size = range.size();

    size_t range_idx = 0;
    Range<uint64_t> r = range[range_idx++];
    for (size_t i = 0; i < page_num; i++) {
        int64_t first_row = _offset_index_ctx->offset_index.page_locations[i].first_row_index + rg_first_row;
        int64_t end_row = first_row;
        if (i != page_num - 1) {
            end_row = _offset_index_ctx->offset_index.page_locations[i + 1].first_row_index + rg_first_row;
        } else {
            // a little trick, we don't care about the real rows of the last page.
            if (range.end() < first_row) {
                _offset_index_ctx->page_selected.emplace_back(false);
                continue;
            } else {
                end_row = range.end();
            }
        }
        if (end_row <= r.begin()) {
            _offset_index_ctx->page_selected.emplace_back(false);
            continue;
        }
        while (first_row >= r.end() && range_idx < range_size) {
            r = range[range_idx++];
        }
        _offset_index_ctx->page_selected.emplace_back(first_row < r.end() && end_row > r.begin());
    }
    // The cache warm-up selects the pages to learn their IO ranges and reads no data, so it does not prepare the column
    // and the column has no stored reader. We fear that wrapping a missing reader crashes the warm-up, so we keep the
    // selection for the IO ranges and wrap nothing.
    if (_reader == nullptr) {
        return;
    }
    const tparquet::ColumnMetaData& column_metadata = get_chunk_metadata()->meta_data;
    bool has_dict_page = column_metadata.__isset.dictionary_page_offset;
    // be compatible with PARQUET-1850
    has_dict_page |= _offset_index_ctx->check_dictionary_page(column_metadata.data_page_offset);
    _reader = std::make_unique<StoredColumnReaderWithIndex>(std::move(_reader), _offset_index_ctx.get(), has_dict_page);
}

Status RawColumnReader::_read_stored_range(const Range<uint64_t>& range, const Filter* filter,
                                           ColumnContentType content_type, Column* dst) {
    // The offset index helps a read where some rows of the range are dropped, or where the reader has to pass rows
    // between this range and the last one. We expect many columns to never get such a read, so we load the offset
    // index at the first one, and the other columns do not load it.
    if (!_offset_index_skip_tried && filter != nullptr && config::parquet_offset_index_skip_enable &&
        (range.begin() > _read_end || SIMD::contain_zero(*filter))) {
        _enable_offset_index_skip();
    }
    _read_end = range.end();
    return _reader->read_range(range, filter, content_type, dst);
}

void RawColumnReader::_enable_offset_index_skip() {
    _offset_index_skip_tried = true;
    if (!_reader->supports_offset_index_skip() || !get_chunk_metadata()->__isset.offset_index_offset ||
        !_may_have_several_data_pages()) {
        return;
    }
    // We fear that the read of the offset index is wasted in every row group of a column that skips no page, so a
    // column that the scan has given up on does not read it. An index that the static page selection has already read
    // costs nothing more, so the scan neither stops it nor counts it. An index that the page cache planner has read is
    // the same for the cost of this read, but the scan still has to learn from the column whether the skip pays, so the
    // advice and the feedback work for it as for a column that reads the index here. Only a read here counts as a load
    // for the skip.
    if (_offset_index_ctx == nullptr || _offset_index_read_for_page_cache_io) {
        OffsetIndexSkipAdvisor* advisor = _opts.offset_index_skip_advisor;
        if (advisor != nullptr) {
            _skip_column = offset_index_feedback_key(get_chunk_metadata()->meta_data);
            if (!advisor->should_load(_skip_column)) {
                _opts.stats->offset_index_skip_given_up += 1;
                return;
            }
            _skip_advisor = advisor;
        }
        if (_offset_index_ctx == nullptr) {
            _opts.stats->offset_index_skip_loads += 1;
        }
    }
    // The index is optional, so an error leaves the column on the path that reads page by page. get_offset_index()
    // logs an index that is not valid, and we ask only once.
    if (!get_offset_index(_opts.first_row_index).ok() || !_offset_index_fits_chunk()) {
        return;
    }
    _offset_index_skip_enabled = true;
    const tparquet::ColumnMetaData& column_metadata = get_chunk_metadata()->meta_data;
    const int64_t chunk_start = column_metadata.__isset.dictionary_page_offset ? column_metadata.dictionary_page_offset
                                                                               : column_metadata.data_page_offset;
    // the bytes before the first data page are the dictionary page
    const bool has_dict_page = _offset_index_ctx->offset_index.page_locations.front().offset > chunk_start;
    _reader->set_offset_index_skip(_offset_index_ctx.get(), has_dict_page);
}

bool RawColumnReader::_may_have_several_data_pages() const {
    const tparquet::ColumnMetaData& column_metadata = get_chunk_metadata()->meta_data;
    if (!column_metadata.__isset.encoding_stats || column_metadata.encoding_stats.empty()) {
        return true;
    }
    int64_t data_pages = 0;
    for (const tparquet::PageEncodingStats& stats : column_metadata.encoding_stats) {
        if (stats.page_type == tparquet::PageType::DATA_PAGE || stats.page_type == tparquet::PageType::DATA_PAGE_V2) {
            data_pages += stats.count;
        }
    }
    return data_pages > 1;
}

bool RawColumnReader::_may_read_offset_index_for_skip() const {
    if (!_may_have_several_data_pages()) {
        return false;
    }
    OffsetIndexSkipAdvisor* advisor = _opts.offset_index_skip_advisor;
    return advisor == nullptr || advisor->should_load(offset_index_feedback_key(get_chunk_metadata()->meta_data));
}

bool RawColumnReader::_offset_index_fits_chunk() const {
    const tparquet::ColumnMetaData& column_metadata = get_chunk_metadata()->meta_data;
    const auto& pages = _offset_index_ctx->offset_index.page_locations;
    const int64_t chunk_start = column_metadata.__isset.dictionary_page_offset ? column_metadata.dictionary_page_offset
                                                                               : column_metadata.data_page_offset;
    if (pages.empty() || chunk_start < 0 || column_metadata.total_compressed_size < 0) {
        return false;
    }
    // A non-repeated column has one value for each row, and a page of it holds the rows that the offset index gives.
    if (_opts.row_group_meta != nullptr && column_metadata.num_values != _opts.row_group_meta->num_rows) {
        return false;
    }

    // The reader reads a chunk page after page, so the pages of the index follow each other without a gap. The first
    // one starts at the chunk, or after the dictionary page, and the last one ends where the chunk ends. The offsets
    // are not negative (_validate_offset_index checked it), so the sums fit in 64 bits.
    uint64_t page_end = chunk_start;
    for (size_t i = 0; i < pages.size(); ++i) {
        const int64_t offset = pages[i].offset;
        if (offset < 0 ||
            (i == 0 ? static_cast<uint64_t>(offset) < page_end : static_cast<uint64_t>(offset) != page_end)) {
            return false;
        }
        page_end = static_cast<uint64_t>(offset) + pages[i].compressed_page_size;
    }
    if (page_end != static_cast<uint64_t>(chunk_start) + column_metadata.total_compressed_size) {
        return false;
    }

    // The bytes before the first data page are the dictionary page, and the footer must agree: it names a dictionary
    // page exactly when there is room for one, the encodings of the chunk include a dictionary encoding exactly when
    // there is a dictionary page, and the page counts of the encoding statistics are the pages of the index.
    const bool has_dict_page = pages.front().offset > chunk_start;
    if (column_metadata.__isset.dictionary_page_offset && !has_dict_page) {
        return false;
    }
    if (!column_metadata.encodings.empty()) {
        const bool dict_encoded = std::ranges::any_of(column_metadata.encodings, [](tparquet::Encoding::type encoding) {
            return encoding == tparquet::Encoding::PLAIN_DICTIONARY || encoding == tparquet::Encoding::RLE_DICTIONARY;
        });
        if (dict_encoded != has_dict_page) {
            return false;
        }
    }
    if (column_metadata.__isset.encoding_stats) {
        int64_t data_pages = 0;
        int64_t dict_pages = 0;
        for (const tparquet::PageEncodingStats& stats : column_metadata.encoding_stats) {
            if (stats.page_type == tparquet::PageType::DATA_PAGE ||
                stats.page_type == tparquet::PageType::DATA_PAGE_V2) {
                data_pages += stats.count;
            } else if (stats.page_type == tparquet::PageType::DICTIONARY_PAGE) {
                dict_pages += stats.count;
            }
        }
        if (data_pages != static_cast<int64_t>(pages.size()) || dict_pages != (has_dict_page ? 1 : 0)) {
            return false;
        }
    }
    return true;
}

void RawColumnReader::collect_page_cache_io_chunks(io::SharedBufferedInputStream* stream,
                                                   std::vector<PageCacheIoChunk>* chunks) {
    const tparquet::ColumnMetaData& column_metadata = get_chunk_metadata()->meta_data;
    PageCacheIoChunk chunk;
    chunk.begin = column_metadata.__isset.dictionary_page_offset ? column_metadata.dictionary_page_offset
                                                                 : column_metadata.data_page_offset;
    if (chunk.begin < 0 || column_metadata.total_compressed_size <= 0) {
        return;
    }
    chunk.end = chunk.begin + column_metadata.total_compressed_size;
    if (column_metadata.__isset.dictionary_page_offset) {
        chunk.first_data_page = column_metadata.data_page_offset;
    }
    chunk.pages = [this, stream](bool allow_load) { return _page_cache_io_spans(allow_load, stream); };
    chunks->push_back(std::move(chunk));
}

bool RawColumnReader::_offset_index_is_planned(io::SharedBufferedInputStream* stream) const {
    const tparquet::ColumnChunk& chunk = *get_chunk_metadata();
    if (stream == nullptr || !chunk.__isset.offset_index_offset || !chunk.__isset.offset_index_length ||
        chunk.offset_index_offset < 0 || chunk.offset_index_length <= 0) {
        return false;
    }
    return stream->find_shared_buffer(chunk.offset_index_offset, chunk.offset_index_length).ok();
}

const std::vector<PageSpan>* RawColumnReader::_page_cache_io_spans(bool allow_load,
                                                                   io::SharedBufferedInputStream* stream) {
    if (_page_spans_state == PageSpansState::KNOWN) {
        return &_page_spans;
    }
    if (_page_spans_state == PageSpansState::UNUSABLE) {
        return nullptr;
    }
    if (_offset_index_ctx == nullptr) {
        if (!allow_load || !_offset_index_is_planned(stream)) {
            return nullptr;
        }
        // The index is optional, so an error leaves the column on the path that loads its chunk whole.
        // get_offset_index() logs an index that is not valid, and we ask only once.
        Status st;
        {
            SCOPED_RAW_TIMER(&_opts.stats->page_cache_io_index_load_ns);
            st = get_offset_index(_opts.first_row_index).status();
        }
        if (!st.ok()) {
            _page_spans_state = PageSpansState::UNUSABLE;
            return nullptr;
        }
        _opts.stats->page_cache_io_index_loads += 1;
        _offset_index_read_for_page_cache_io = true;
    }
    if (!_offset_index_fits_chunk()) {
        _page_spans_state = PageSpansState::UNUSABLE;
        return nullptr;
    }

    const tparquet::ColumnMetaData& column_metadata = get_chunk_metadata()->meta_data;
    const int64_t chunk_start = column_metadata.__isset.dictionary_page_offset ? column_metadata.dictionary_page_offset
                                                                               : column_metadata.data_page_offset;
    const auto& pages = _offset_index_ctx->offset_index.page_locations;
    _page_spans.reserve(pages.size() + 1);
    // the bytes before the first data page are the dictionary page
    if (pages.front().offset > chunk_start) {
        _page_spans.push_back(PageSpan{chunk_start, pages.front().offset - chunk_start});
    }
    for (const tparquet::PageLocation& page : pages) {
        _page_spans.push_back(PageSpan{page.offset, page.compressed_page_size});
    }
    _page_spans_state = PageSpansState::KNOWN;
    return &_page_spans;
}

bool RawColumnReader::column_all_pages_dict_encoded() const {
    // The Parquet spec allows for column chunks to have mixed encodings
    // where some data pages are dictionary-encoded and others are plain
    // encoded. For example, a Parquet file writer might start writing
    // a column chunk as dictionary encoded, but it will switch to plain
    // encoding if the dictionary grows too large.
    //
    // In order for dictionary filters to skip the entire row group,
    // the conjuncts must be evaluated on column chunks that are entirely
    // encoded with the dictionary encoding. There are two checks
    // available to verify this:
    // 1. The encoding_stats field on the column chunk metadata provides
    //    information about the number of data pages written in each
    //    format. This allows for a specific check of whether all the
    //    data pages are dictionary encoded.
    // 2. The encodings field on the column chunk metadata lists the
    //    encodings used. If this list contains the dictionary encoding
    //    and does not include unexpected encodings (i.e. encodings not
    //    associated with definition/repetition levels), then it is entirely
    //    dictionary encoded.
    const tparquet::ColumnMetaData& column_metadata = get_chunk_metadata()->meta_data;
    if (column_metadata.__isset.encoding_stats) {
        // Condition #1 above
        for (const tparquet::PageEncodingStats& enc_stat : column_metadata.encoding_stats) {
            if ((enc_stat.page_type == tparquet::PageType::DATA_PAGE ||
                 enc_stat.page_type == tparquet::PageType::DATA_PAGE_V2) &&
                (enc_stat.encoding != tparquet::Encoding::PLAIN_DICTIONARY &&
                 enc_stat.encoding != tparquet::Encoding::RLE_DICTIONARY) &&
                enc_stat.count > 0) {
                return false;
            }
        }
    } else {
        // Condition #2 above
        bool has_dict_encoding = false;
        bool has_nondict_encoding = false;
        for (const tparquet::Encoding::type& encoding : column_metadata.encodings) {
            if (encoding == tparquet::Encoding::PLAIN_DICTIONARY || encoding == tparquet::Encoding::RLE_DICTIONARY) {
                has_dict_encoding = true;
            }

            // RLE and BIT_PACKED are used for repetition/definition levels
            if (encoding != tparquet::Encoding::PLAIN_DICTIONARY && encoding != tparquet::Encoding::RLE_DICTIONARY &&
                encoding != tparquet::Encoding::RLE && encoding != tparquet::Encoding::BIT_PACKED) {
                has_nondict_encoding = true;
                break;
            }
        }
        // Not entirely dictionary encoded if:
        // 1. No dictionary encoding listed
        // OR
        // 2. Some non-dictionary encoding is listed
        if (!has_dict_encoding || has_nondict_encoding) {
            return false;
        }
    }

    return true;
}

StatusOr<bool> RawColumnReader::_row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                           CompoundNodeType pred_relation,
                                                           const TypeDescriptor& col_type, const uint64_t rg_first_row,
                                                           const uint64_t rg_num_rows) const {
    ASSIGN_OR_RETURN(auto match,
                     _row_group_zone_map_match(predicates, pred_relation, col_type, rg_first_row, rg_num_rows));
    return match == ZoneMapMatch::NONE;
}

StatusOr<ZoneMapMatch> RawColumnReader::_row_group_zone_map_match(const std::vector<const ColumnPredicate*>& predicates,
                                                                  CompoundNodeType pred_relation,
                                                                  const TypeDescriptor& col_type,
                                                                  const uint64_t rg_first_row,
                                                                  const uint64_t rg_num_rows) const {
    const auto filtered = ZoneMapMatch::UNKNOWN;
    if (!get_chunk_metadata()->meta_data.__isset.statistics || get_column_parquet_field() == nullptr) {
        // statistics is not existed, don't filter
        return filtered;
    }

    bool has_null = true;
    bool is_all_null = false;

    if (get_chunk_metadata()->meta_data.statistics.__isset.null_count) {
        has_null = get_chunk_metadata()->meta_data.statistics.null_count > 0;
        is_all_null = get_chunk_metadata()->meta_data.statistics.null_count == rg_num_rows;
    } else {
        return filtered;
    }

    std::optional<ZoneMapDetail> zone_map_detail = std::nullopt;

    // used to hold min/max slice values
    const MutableColumnPtr min_column = ColumnHelper::create_column(col_type, true);
    const MutableColumnPtr max_column = ColumnHelper::create_column(col_type, true);
    if (is_all_null) {
        // if the entire column's value is null, the min/max value not existed
        zone_map_detail = ZoneMapDetail{Datum{}, Datum{}, true};
        zone_map_detail->set_num_rows(rg_num_rows);
    } else {
        std::vector<string> min_values;
        std::vector<string> max_values;
        std::vector<bool> null_pages{false};
        Status st =
                StatisticsHelper::get_min_max_value(_opts.file_meta_data, col_type, &get_chunk_metadata()->meta_data,
                                                    get_column_parquet_field(), min_values, max_values);
        if (st.ok()) {
            st = StatisticsHelper::decode_value_into_column(min_column, min_values, null_pages, col_type,
                                                            get_column_parquet_field(), _opts.timezone);
            RETURN_IF(!st.ok(), filtered);
            st = StatisticsHelper::decode_value_into_column(max_column, max_values, null_pages, col_type,
                                                            get_column_parquet_field(), _opts.timezone);
            RETURN_IF(!st.ok(), filtered);

            zone_map_detail = ZoneMapDetail{min_column->get(0), max_column->get(0), has_null};
            zone_map_detail->set_num_rows(rg_num_rows);
        }
    }

    if (!zone_map_detail.has_value()) {
        // ZoneMapDetail not set, means select all, dont filter
        return filtered;
    }

    if (!PredicateFilterEvaluatorUtils::zonemap_satisfy(predicates, *zone_map_detail, pred_relation)) {
        return ZoneMapMatch::NONE;
    }
    return PredicateFilterEvaluatorUtils::zonemap_all_match(predicates, *zone_map_detail, pred_relation)
                   ? ZoneMapMatch::ALL
                   : ZoneMapMatch::UNKNOWN;
}

StatusOr<bool> RawColumnReader::_page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                            SparseRange<uint64_t>* row_ranges,
                                                            CompoundNodeType pred_relation,
                                                            const TypeDescriptor& col_type, const uint64_t rg_first_row,
                                                            const uint64_t rg_num_rows) {
    const tparquet::ColumnChunk* chunk_meta = get_chunk_metadata();
    if (!chunk_meta->__isset.column_index_offset || !chunk_meta->__isset.offset_index_offset ||
        !chunk_meta->__isset.meta_data) {
        // no page index, dont filter
        return false;
    }

    SCOPED_RAW_TIMER(&_opts.stats->page_index_ns);

    // a footer written without the length of the column index cannot be read as a whole
    if (!chunk_meta->__isset.column_index_length) {
        return false;
    }

    // get column index
    const int64_t column_index_offset = chunk_meta->column_index_offset;
    const int32_t column_index_length = chunk_meta->column_index_length;
    RETURN_IF_ERROR(_check_region(column_index_offset, column_index_length, kMaxPageIndexBytes, "column index"));
    if (column_index_length == 0) {
        return invalid_index(fmt::format("Empty column index of parquet column '{}', file: {}",
                                         describe_column(get_column_parquet_field()), _opts.file->filename()));
    }

    std::vector<uint8_t> page_index_data(column_index_length);
    {
        io::MetadataReadScope scope(io::MetadataKind::PAGE_INDEX, config::parquet_index_datacache_enable);
        RETURN_IF_ERROR(_opts.file->read_at_fully(column_index_offset, page_index_data.data(), column_index_length));
    }

    tparquet::ColumnIndex column_index;
    uint32_t length = column_index_length;
    RETURN_IF_ERROR(deserialize_thrift_msg(page_index_data.data(), &length, TProtocolType::COMPACT, &column_index));

    ASSIGN_OR_RETURN(const tparquet::OffsetIndex* offset_index, get_offset_index(rg_first_row));

    // The pages are the entries of the column index, and the code below takes the matching entry of every
    // list by the position, so the lists must be as long as the pages, and each page must start inside the
    // row group.
    const size_t page_num = column_index.min_values.size();
    if (column_index.max_values.size() != page_num || column_index.null_pages.size() != page_num ||
        offset_index->page_locations.size() != page_num ||
        (page_num > 0 && static_cast<uint64_t>(offset_index->page_locations.back().first_row_index) >= rg_num_rows)) {
        return invalid_index(fmt::format(
                "Inconsistent page index of parquet column '{}': {} min values, {} max values, {} null pages, {} "
                "pages in the offset index, row group has {} rows, file: {}",
                describe_column(get_column_parquet_field()), page_num, column_index.max_values.size(),
                column_index.null_pages.size(), offset_index->page_locations.size(), rg_num_rows,
                _opts.file->filename()));
    }
    const std::vector<bool> null_pages = column_index.null_pages;

    MutableColumnPtr min_column = ColumnHelper::create_column(col_type, true);
    MutableColumnPtr max_column = ColumnHelper::create_column(col_type, true);
    // deal with min_values
    auto st = StatisticsHelper::decode_value_into_column(min_column, column_index.min_values, null_pages, col_type,
                                                         get_column_parquet_field(), _opts.timezone);
    if (!st.ok()) {
        // swallow error status
        LOG(INFO) << "Error when decode min/max statistics, type " << col_type.debug_string();
        return false;
    }
    // deal with max_values
    st = StatisticsHelper::decode_value_into_column(max_column, column_index.max_values, null_pages, col_type,
                                                    get_column_parquet_field(), _opts.timezone);
    if (!st.ok()) {
        // swallow error status
        LOG(INFO) << "Error when decode min/max statistics, type " << col_type.debug_string();
        return false;
    }

    DCHECK_EQ(page_num, min_column->size());
    DCHECK_EQ(page_num, max_column->size());

    // fill ZoneMapDetail
    std::vector<ZoneMapDetail> zone_map_details{};
    for (size_t i = 0; i < page_num; i++) {
        if (null_pages[i]) {
            // all null
            zone_map_details.emplace_back(Datum{}, Datum{}, true);
        } else {
            // null_counts is optional in parquet - if unavailable, conservatively assume nulls exist
            bool has_null = i >= column_index.null_counts.size() || column_index.null_counts[i] > 0;
            zone_map_details.emplace_back(min_column->get(i), max_column->get(i), has_null);
        }
    }

    // select all pages by default
    Filter selected(page_num, 1);
    for (size_t i = 0; i < page_num; i++) {
        selected[i] = PredicateFilterEvaluatorUtils::zonemap_satisfy(predicates, zone_map_details[i], pred_relation);
    }

    if (!SIMD::contain_zero(selected)) {
        // no page has been filtered
        return false;
    }

    for (int i = 0; i < page_num; i++) {
        if (selected[i]) {
            int64_t first_row = offset_index->page_locations[i].first_row_index + rg_first_row;
            int64_t end_row = first_row;
            if (i != page_num - 1) {
                end_row = offset_index->page_locations[i + 1].first_row_index + rg_first_row;
            } else {
                end_row = rg_first_row + rg_num_rows;
            }
            row_ranges->add(Range<uint64_t>(first_row, end_row));
        }
    }
    return true;
}

Status RawColumnReader::_init_column_bloom_filter(int64_t offset, int32_t length, BloomFilter& bloom_filter) const {
    RETURN_IF_ERROR(_check_region(offset, length, kMaxBloomFilterRegionBytes, "bloom filter"));
    if (length == 0) {
        RETURN_IF_ERROR(
                _check_region(offset, SBBF_HEADER_SIZE_ESTIMATE, kMaxBloomFilterRegionBytes, "bloom filter header"));
    }

    // Both reads below are reads of the bloom filter region.
    io::MetadataReadScope scope(io::MetadataKind::BLOOM_FILTER, config::parquet_index_datacache_enable);
    std::vector<char> bloom_filter_data;
    tparquet::BloomFilterHeader header;
    uint32_t header_len = SBBF_HEADER_SIZE_ESTIMATE;
    if (length > 0) {
        // the header cannot be longer than the region
        header_len = std::min<uint32_t>(header_len, length);
        bloom_filter_data.resize(length);
        RETURN_IF_ERROR(_opts.file->read_at_fully(offset, bloom_filter_data.data(), length));
    } else {
        // if length is not set, read the header first;
        bloom_filter_data.resize(header_len);
        RETURN_IF_ERROR(_opts.file->read_at_fully(offset, bloom_filter_data.data(), header_len));
    }

    RETURN_IF_ERROR(deserialize_thrift_msg(reinterpret_cast<const uint8*>(bloom_filter_data.data()), &header_len,
                                           TProtocolType::COMPACT, &header));

    // The header names the algorithm, the hash and the compression of the filter, and we know one of each: the
    // split block algorithm, xxHash and no compression. A filter of another kind would be probed with the wrong
    // hash or layout and answer "not present" for values that are present, so we do not use it. A kind that this
    // version of the format does not have is not set in any of the fields.
    if (!header.algorithm.__isset.BLOCK || !header.hash.__isset.XXHASH || !header.compression.__isset.UNCOMPRESSED) {
        return Status::NotSupported(
                fmt::format("The bloom filter of parquet column '{}' is not a split block, xxHash, uncompressed "
                            "filter, file: {}",
                            describe_column(get_column_parquet_field()), _opts.file->filename()));
    }

    // The bitset is a sequence of blocks of BloomFilter::MINIMUM_BYTES, and a probe reads a whole block, so
    // a bitset that is shorter than a block, or that is not made of blocks, cannot be probed. The size is
    // also what we allocate and copy, so a size that no real file has is rejected.
    const uint32_t num_bytes = header.numBytes;
    if (header.numBytes < 0 || num_bytes < BloomFilter::MINIMUM_BYTES || num_bytes > BloomFilter::MAXIMUM_BYTES ||
        num_bytes % BloomFilter::MINIMUM_BYTES != 0) {
        return invalid_index(fmt::format("Invalid bloom filter size {} of parquet column '{}', file: {}",
                                         header.numBytes, describe_column(get_column_parquet_field()),
                                         _opts.file->filename()));
    }
    const size_t bitset_end = static_cast<size_t>(header_len) + num_bytes;
    if (length > 0) {
        if (bitset_end > static_cast<size_t>(length)) {
            return invalid_index(
                    fmt::format("Bloom filter of {} bytes after a header of {} bytes does not fit in the {} bytes "
                                "of parquet column '{}', file: {}",
                                num_bytes, header_len, length, describe_column(get_column_parquet_field()),
                                _opts.file->filename()));
        }
    } else {
        RETURN_IF_ERROR(_check_region(offset + header_len, num_bytes, BloomFilter::MAXIMUM_BYTES, "bloom filter"));
        bloom_filter_data.resize(bitset_end);
        RETURN_IF_ERROR(
                _opts.file->read_at_fully(offset + header_len, bloom_filter_data.data() + header_len, num_bytes));
    }

    // The byte after the bitset is the flag of NULL values. We cut the data there, so that the flag is
    // the byte that BloomFilter::init takes, whatever the region holds after the bitset.
    bloom_filter_data.resize(bitset_end + 1);
    if (get_chunk_metadata()->meta_data.__isset.statistics &&
        get_chunk_metadata()->meta_data.statistics.__isset.null_count) {
        if (get_chunk_metadata()->meta_data.statistics.null_count > 0) {
            bloom_filter_data.back() = 1;
        } else {
            bloom_filter_data.back() = 0;
        }
    } else if (get_column_parquet_field()->is_nullable) {
        bloom_filter_data.back() = 1; //set has null as default, to avoid `column is null` to filter the group.
    } else {
        bloom_filter_data.back() = 0;
    }

    return bloom_filter.init(bloom_filter_data.data() + header_len, num_bytes + 1, Hasher::HashStrategy::XXHASH64, 0);
}

StatusOr<bool> RawColumnReader::_row_group_bloom_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                        CompoundNodeType pred_relation, const TypeDescriptor& col_type,
                                                        const uint64_t rg_first_row, const uint64_t rg_num_rows) const {
    bool filtered = false;
    if (get_chunk_metadata() == nullptr) {
        return filtered;
    }
    auto& column_metadata = get_chunk_metadata()->meta_data;
    bool has_bloom_filter = column_metadata.__isset.bloom_filter_offset;
    if (!has_bloom_filter) {
        // bloom filter is not existed, select all, dont filtered
        return filtered = false;
    } else if (get_column_parquet_field() == nullptr) {
        // which scenerio will reach here? is it unexpected?
        return filtered = false;
    }

    bool applicable = check_type_can_apply_bloom_filter(col_type, *get_column_parquet_field());
    if (applicable) {
        int64_t offset = column_metadata.bloom_filter_offset;
        ParquetBlockSplitBloomFilter bloom_filter;
        //parquet format version>= 2.10 has the bloom filter length info.
        if (predicates.empty()) {
            return filtered = false;
        } else {
            bool has_bloom_filter_pred =
                    std::ranges::any_of(predicates, [](auto& pred) { return pred->support_original_bloom_filter(); });

            if (has_bloom_filter_pred) {
                if (column_metadata.__isset.bloom_filter_length) {
                    RETURN_IF_ERROR(
                            _init_column_bloom_filter(offset, column_metadata.bloom_filter_length, bloom_filter));
                } else {
                    RETURN_IF_ERROR(_init_column_bloom_filter(offset, 0, bloom_filter));
                }
            } else {
                return filtered = false;
            }
        }
        // The predicates of a TINYINT or SMALLINT column probe one or two bytes, and the INT32 column of the file
        // hashed four.
        if (col_type.type == TYPE_TINYINT || col_type.type == TYPE_SMALLINT) {
            bloom_filter.hash_narrow_ints_as_int32();
        }

        if (pred_relation == CompoundNodeType::AND) {
            filtered = std::ranges::any_of(predicates, [&bloom_filter](auto& pred) {
                //TODO: implement parquet_bloom_filter(bloom_filter, col_type, parquet_logical_type, parquet_physical_type) interface,
                //      and change the value data type to parquet value data type.
                return pred->support_original_bloom_filter() && !pred->original_bloom_filter(&bloom_filter);
            });
        } else {
            filtered = std::ranges::all_of(predicates, [&bloom_filter](auto& pred) {
                return pred->support_original_bloom_filter() && !pred->original_bloom_filter(&bloom_filter);
            });
        }
    }

    return filtered;
}

// If `column` still refers to one of the reader's PHYSICAL columns, a previous
// fill_dst_column() was skipped (e.g. the whole range was filtered out and
// GroupReader::get_next() continued without filling).  _restore_physical_column
// at the top of read_range() repairs this state.

Status RawColumnReader::_restore_physical_column(ColumnPtr& column) {
    if (!_is_physical_column(column)) {
        return Status::OK();
    }
    if (UNLIKELY(_logical_dst == nullptr)) {
        return Status::InternalError("Parquet reader found a physical column without a saved logical destination");
    }
    column->as_mutable_raw_ptr()->reset_column();
    _logical_dst->as_mutable_raw_ptr()->reset_column();
    column = _logical_dst;
    _logical_dst = nullptr;
    return Status::OK();
}

// ScalarColumnReader

Status ScalarColumnReader::prepare() {
    RETURN_IF_ERROR(ColumnConverterFactory::create_converter(
            *get_column_parquet_field(), *_col_type, _opts.timezone, &_converter,
            _opts.file != nullptr ? _opts.file->filename() : std::string()));
    if (_can_lazy_dict_decode && _converter->need_convert) {
        _can_lazy_dict_decode = false;
    }
    return RawColumnReader::prepare();
}

Status ScalarColumnReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    DCHECK(get_column_parquet_field()->is_nullable ? dst->is_nullable() : true);
    RETURN_IF_ERROR(_restore_physical_column(dst));
    bool need_lazy_decode =
            _dict_filter_ctx != nullptr || (_can_lazy_dict_decode && filter != nullptr &&
                                            SIMD::count_nonzero(*filter) * 1.0 / filter->size() < FILTER_RATIO);
    ColumnContentType content_type = !need_lazy_decode ? ColumnContentType::VALUE : ColumnContentType::DICT_CODE;
    auto need_lazy_covert = _can_lazy_convert && _converter->need_convert;
    if (need_lazy_decode) {
        return _read_range_impl<true, false>(range, filter, content_type, dst);
    } else if (need_lazy_covert) {
        return _read_range_impl<false, true>(range, filter, content_type, dst);
    } else {
        return _read_range_impl<false, false>(range, filter, content_type, dst);
    }
}

bool ScalarColumnReader::try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                                const std::vector<std::string>& sub_field_path, const size_t& layer) {
    if (sub_field_path.size() != layer) {
        return false;
    }

    if (!_col_type->is_string_type()) {
        return false;
    }

    if (column_all_pages_dict_encoded()) {
        if (_dict_filter_ctx == nullptr) {
            _dict_filter_ctx = std::make_unique<ColumnDictFilterContext>();
            _dict_filter_ctx->is_decode_needed = is_decode_needed;
            _dict_filter_ctx->sub_field_path = sub_field_path;
            _dict_filter_ctx->slot_id = slotId;
        }
        _dict_filter_ctx->conjunct_ctxs.push_back(ctx);
        return true;
    } else {
        return false;
    }
}

Status ScalarColumnReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
    // Dispatch on which temp column src actually is — the truth — not a recomputable flag.
    if (_code_column != nullptr && src.get() == _code_column.get()) {
        return _fill_dst_column_impl<true, false>(dst, src);
    } else if (_tmp_intermediate_column != nullptr && src.get() == _tmp_intermediate_column.get()) {
        return _fill_dst_column_impl<false, true>(dst, src);
    } else {
        return _fill_dst_column_impl<false, false>(dst, src);
    }
}

template <bool LAZY_DICT_DECODE, bool LAZY_CONVERT>
Status ScalarColumnReader::_read_range_impl(const Range<uint64_t>& range, const Filter* filter,
                                            ColumnContentType content_type, ColumnPtr& dst) {
    if constexpr (LAZY_DICT_DECODE) {
        if (_code_column == nullptr) {
            _code_column = ColumnHelper::create_column(
                    TypeDescriptor::from_logical_type(ColumnDictFilterContext::kDictCodePrimitiveType), true);
        }
        _code_column->as_mutable_raw_ptr()->reset_column();
        _logical_dst = dst;
        dst = _code_column;
        dst->as_mutable_raw_ptr()->reserve(range.span_size());
        SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
        return _read_stored_range(range, filter, content_type, dst->as_mutable_raw_ptr());
    } else {
        if (!_converter->need_convert) {
            SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
            return _read_stored_range(range, filter, content_type, dst->as_mutable_raw_ptr());
        } else {
            if (_tmp_intermediate_column == nullptr) {
                _tmp_intermediate_column = _converter->create_src_column();
            }
            _tmp_intermediate_column->as_mutable_raw_ptr()->reset_column();
            _tmp_intermediate_column->as_mutable_raw_ptr()->reserve(range.span_size());
            {
                SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
                RETURN_IF_ERROR(_read_stored_range(range, filter, content_type,
                                                   _tmp_intermediate_column->as_mutable_raw_ptr()));
            }
            if constexpr (!LAZY_CONVERT) {
                {
                    SCOPED_RAW_TIMER(&_opts.stats->column_convert_ns);
                    RETURN_IF_ERROR(_converter->convert(_tmp_intermediate_column, dst->as_mutable_raw_ptr()));
                }
                _tmp_intermediate_column->as_mutable_raw_ptr()->reset_column();
                return Status::OK();
            } else {
                _logical_dst = dst;
                dst = _tmp_intermediate_column;
                return Status::OK();
            }
        }
    }
}

Status ScalarColumnReader::_dict_decode(ColumnPtr& dst, ColumnPtr& src) {
    Column* dict_values = ColumnHelper::get_data_column(dst->as_mutable_raw_ptr());
    dict_values->reserve(src->size());

    // decode dict code to dict values.
    // note that in dict code, there could be null value.
    const ColumnPtr& dict_codes = src;
    auto* codes_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(dict_codes->as_mutable_raw_ptr());
    auto* codes_column =
            ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(codes_nullable_column->data_column_raw_ptr());
    RETURN_IF_ERROR(_reader->get_dict_values(codes_column->get_data(), *codes_nullable_column, dict_values));
    DCHECK_EQ(dict_codes->size(), dict_values->size());
    if (dst->is_nullable()) {
        auto* nullable_values = down_cast<NullableColumn*>(dst->as_mutable_raw_ptr());
        nullable_values->swap_null_column(*(codes_nullable_column->as_mutable_raw_ptr()));
    }
    src->as_mutable_raw_ptr()->reset_column();
    return Status::OK();
}

template <bool LAZY_DICT_DECODE, bool LAZY_CONVERT>
Status ScalarColumnReader::_fill_dst_column_impl(ColumnPtr& dst, ColumnPtr& src) {
    if constexpr (LAZY_DICT_DECODE) {
        if (UNLIKELY(!_is_dict_code_column(src))) {
            return Status::InternalError(
                    "Parquet lazy dictionary decode source column is not a dictionary code column");
        }
        if (_dict_filter_ctx != nullptr && !_dict_filter_ctx->is_decode_needed) {
            // reset_column() guarantees a clean dst regardless of any prior
            // set_num_rows / resize call (e.g. via append_side_columns_to_chunk).
            dst->as_mutable_raw_ptr()->reset_column();
            dst->as_mutable_raw_ptr()->append_default(src->size());
            src->as_mutable_raw_ptr()->reset_column();
        } else {
            static_assert(!LAZY_CONVERT, "LAZY_DICT_DECODE && LAZY_CONVERT == true is not supported by now");
            if (_converter->need_convert) {
                if (_tmp_intermediate_column == nullptr) {
                    _tmp_intermediate_column = _converter->create_src_column();
                }
                _tmp_intermediate_column->as_mutable_raw_ptr()->reset_column();
                RETURN_IF_ERROR(_dict_decode(_tmp_intermediate_column, src));
                {
                    SCOPED_RAW_TIMER(&_opts.stats->column_convert_ns);
                    RETURN_IF_ERROR(_converter->convert(_tmp_intermediate_column.get(), dst->as_mutable_raw_ptr()));
                }
                _tmp_intermediate_column->as_mutable_raw_ptr()->reset_column();
            } else {
                dst->as_mutable_raw_ptr()->reset_column();
                RETURN_IF_ERROR(_dict_decode(dst, src));
            }
        }
        src = _logical_dst;
        _logical_dst = nullptr;
    } else {
        if constexpr (LAZY_CONVERT) {
            if (UNLIKELY(!_is_intermediate_column(src))) {
                return Status::InternalError("Parquet lazy conversion source column is not an intermediate column");
            }
            {
                SCOPED_RAW_TIMER(&_opts.stats->column_convert_ns);
                RETURN_IF_ERROR(_converter->convert(src, dst->as_mutable_raw_ptr()));
            }
            src->as_mutable_raw_ptr()->reset_column();
            src = _logical_dst;
            _logical_dst = nullptr;
        } else {
            dst->as_mutable_raw_ptr()->swap_column(*(src->as_mutable_raw_ptr()));
        }
    }
    return Status::OK();
}

bool ScalarColumnReader::_is_dict_code_column(const ColumnPtr& column) const {
    return _code_column != nullptr && column.get() == _code_column.get();
}

bool ScalarColumnReader::_is_intermediate_column(const ColumnPtr& column) const {
    return _tmp_intermediate_column != nullptr && column.get() == _tmp_intermediate_column.get();
}

Status ScalarColumnReader::finalize_lazy_state(ColumnPtr& col) {
    if (_logical_dst != nullptr) {
        if (col.get() == _code_column.get()) {
            auto saved = std::move(_logical_dst);
            _logical_dst = nullptr;
            if (_converter->need_convert) {
                if (_tmp_intermediate_column == nullptr) {
                    _tmp_intermediate_column = _converter->create_src_column();
                }
                _tmp_intermediate_column->as_mutable_raw_ptr()->reset_column();
                RETURN_IF_ERROR(_dict_decode(_tmp_intermediate_column, col));
                {
                    SCOPED_RAW_TIMER(&_opts.stats->column_convert_ns);
                    RETURN_IF_ERROR(_converter->convert(_tmp_intermediate_column.get(), saved->as_mutable_raw_ptr()));
                }
                _tmp_intermediate_column->as_mutable_raw_ptr()->reset_column();
            } else {
                RETURN_IF_ERROR(_dict_decode(saved, col));
            }
            col = std::move(saved);
        } else if (col.get() == _tmp_intermediate_column.get()) {
            auto saved = std::move(_logical_dst);
            _logical_dst = nullptr;
            {
                SCOPED_RAW_TIMER(&_opts.stats->column_convert_ns);
                RETURN_IF_ERROR(_converter->convert(col, saved->as_mutable_raw_ptr()));
            }
            col->as_mutable_raw_ptr()->reset_column();
            col = std::move(saved);
        }
    }
    return Status::OK();
}

// LowCardColumnReader

Status LowCardColumnReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    DCHECK(get_column_parquet_field()->is_nullable ? dst->is_nullable() : true);
    RETURN_IF_ERROR(_restore_physical_column(dst));
    ColumnContentType content_type = ColumnContentType::DICT_CODE;

    if (_code_column == nullptr) {
        _code_column = ColumnHelper::create_column(
                TypeDescriptor::from_logical_type(ColumnDictFilterContext::kDictCodePrimitiveType), true);
    }
    _code_column->as_mutable_raw_ptr()->reset_column();
    _logical_dst = dst;
    dst = _code_column;
    dst->as_mutable_raw_ptr()->reserve(range.span_size());

    {
        SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
        return _read_stored_range(range, filter, content_type, dst->as_mutable_raw_ptr());
    }
}

bool LowCardColumnReader::try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                                 const std::vector<std::string>& sub_field_path, const size_t& layer) {
    if (sub_field_path.size() != layer) {
        return false;
    }

    if (column_all_pages_dict_encoded()) {
        if (_dict_filter_ctx == nullptr) {
            _dict_filter_ctx = std::make_unique<ColumnDictFilterContext>();
            _dict_filter_ctx->is_decode_needed = is_decode_needed;
            _dict_filter_ctx->sub_field_path = sub_field_path;
            _dict_filter_ctx->slot_id = slotId;
        }
        _dict_filter_ctx->conjunct_ctxs.push_back(ctx);
        return true;
    } else {
        return false;
    }
}

Status LowCardColumnReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
    if (UNLIKELY(!_is_dict_code_column(src))) {
        // src is already LOGICAL (e.g. finalized by compound conjuncts).
        // Swap directly — no dict-code decoding needed.
        dst->as_mutable_raw_ptr()->swap_column(*(src->as_mutable_raw_ptr()));
        return Status::OK();
    }
    size_t num_rows = src->size();
    if (!_code_convert_map.has_value()) {
        RETURN_IF_ERROR(_check_current_dict());
    }

    dst->as_mutable_raw_ptr()->resize(src->size());

    const ColumnPtr& dict_codes = src;
    auto* codes_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(dict_codes->as_mutable_raw_ptr());
    auto* codes_column =
            ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(codes_nullable_column->data_column_raw_ptr());
    const auto* null_data_ptr = codes_nullable_column->immutable_null_column_data().data();

    auto& codes = codes_column->get_data();
    if (codes_nullable_column->has_null()) {
        for (size_t i = 0; i < num_rows; i++) {
            // if null, we assign dict code 0
            // null = 0, mask = 0xffffffff
            // null = 1, mask = 0x00000000
            uint32_t mask = ~(static_cast<uint32_t>(-null_data_ptr[i]));
            codes[i] = mask & codes[i];
        }
    }

    auto* dst_data_column = down_cast<LowCardDictColumn*>(ColumnHelper::get_data_column(dst->as_mutable_raw_ptr()));
    SIMDGather::gather(dst_data_column->get_data().data(), _code_convert_map->data(), codes.data(),
                       _code_convert_map->size(), num_rows);

    if (dst->is_nullable()) {
        auto* nullable_dst = down_cast<NullableColumn*>(dst->as_mutable_raw_ptr());
        nullable_dst->swap_null_column(*codes_nullable_column);
    }

    src->as_mutable_raw_ptr()->reset_column();
    src = _logical_dst;
    _logical_dst = nullptr;

    return Status::OK();
}

Status LowCardColumnReader::finalize_lazy_state(ColumnPtr& col) {
    if (_logical_dst != nullptr && col.get() == _code_column.get()) {
        auto saved = std::move(_logical_dst);
        _logical_dst = nullptr;
        RETURN_IF_ERROR(fill_dst_column(saved, col));
        col = std::move(saved);
    }
    return Status::OK();
}

bool LowCardColumnReader::_is_dict_code_column(const ColumnPtr& column) const {
    return _code_column != nullptr && column.get() == _code_column.get();
}

Status LowCardColumnReader::_check_current_dict() {
    std::vector<int16_t> code_convert_map;

    // create dict value chunk for evaluation.
    auto dict_value_column = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
    RETURN_IF_ERROR(_reader->get_dict_values(dict_value_column.get()));

    size_t dict_size = dict_value_column->size();

    code_convert_map.resize(dict_size + 2);
    std::fill(code_convert_map.begin(), code_convert_map.end(), 0);
    auto* local_to_global = code_convert_map.data();

    auto viewer = ColumnViewer<TYPE_VARCHAR>(dict_value_column);

    for (int i = 0; i < dict_size; ++i) {
        auto slice = viewer.value(i);
        auto res = _dict->find(slice);
        if (res == _dict->end()) {
            // error message format used to extract info, carefully
            return Status::GlobalDictNotMatch(fmt::format("SlotId: {}, FileName: {} , file doesn't match global dict. ",
                                                          _slot_id, _opts.file->filename()));
        } else {
            local_to_global[i] = res->second;
        }
    }

#ifdef DEBUG
    std::stringstream ss;
    ss << "dict mapping: ";
    for (int i = 0; i < dict_size; ++i) {
        ss << code_convert_map[i] << " ";
    }
    LOG(INFO) << ss.str();
#endif

    _code_convert_map = std::move(code_convert_map);

    return Status::OK();
}

const TypeDescriptor* LowCardColumnReader::_bloom_filter_type() const {
    return &TYPE_VARCHAR_DESC;
}

// LowRowsColumnReader

Status LowRowsColumnReader::read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) {
    DCHECK(get_column_parquet_field()->is_nullable ? dst->is_nullable() : true);
    RETURN_IF_ERROR(_restore_physical_column(dst));
    ColumnContentType content_type = ColumnContentType::VALUE;

    if (_raw_string_column == nullptr) {
        _raw_string_column = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
    }
    _raw_string_column->as_mutable_raw_ptr()->reset_column();
    _logical_dst = dst;
    dst = _raw_string_column;
    dst->as_mutable_raw_ptr()->reserve(range.span_size());

    {
        SCOPED_RAW_TIMER(&_opts.stats->column_read_ns);
        return _read_stored_range(range, filter, content_type, dst->as_mutable_raw_ptr());
    }
}

Status LowRowsColumnReader::fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
    if (UNLIKELY(!_is_physical_column(src))) {
        // src is already LOGICAL — swap directly
        dst->as_mutable_raw_ptr()->swap_column(*(src->as_mutable_raw_ptr()));
        return Status::OK();
    }
    dst->as_mutable_raw_ptr()->resize(src->size());

    const ColumnPtr& readed_column = src;
    auto* nullable_string_column = ColumnHelper::as_raw_column<NullableColumn>(readed_column);
    auto* binary_column = ColumnHelper::as_raw_column<BinaryColumn>(nullable_string_column->data_column_raw_ptr());
    auto* dst_data_column = down_cast<LowCardDictColumn*>(ColumnHelper::get_data_column(dst->as_mutable_raw_ptr()));
    for (size_t i = 0; i < src->size(); i++) {
        if (src->is_null(i)) {
            dst_data_column->get_data()[i] = 1;
            continue;
        }
        const auto& slice = binary_column->get_slice(i);
        auto res = _dict->find(slice);
        if (res == _dict->end()) {
            // error message format used to extract info, carefully
            return Status::GlobalDictNotMatch(fmt::format("SlotId: {}, FileName: {} , file doesn't match global dict. ",
                                                          _slot_id, _opts.file->filename()));
        } else {
            dst_data_column->get_data()[i] = res->second;
        }
    }

    if (dst->is_nullable()) {
        auto* nullable_dst = down_cast<NullableColumn*>(dst->as_mutable_raw_ptr());
        nullable_dst->swap_null_column(*(nullable_string_column->as_mutable_raw_ptr()));
    }

    src->as_mutable_raw_ptr()->reset_column();
    src = _logical_dst;
    _logical_dst = nullptr;

    return Status::OK();
}

Status LowRowsColumnReader::finalize_lazy_state(ColumnPtr& col) {
    if (_logical_dst != nullptr && col.get() == _raw_string_column.get()) {
        auto saved = std::move(_logical_dst);
        _logical_dst = nullptr;
        RETURN_IF_ERROR(fill_dst_column(saved, col));
        col = std::move(saved);
    }
    return Status::OK();
}

} // namespace starrocks::parquet