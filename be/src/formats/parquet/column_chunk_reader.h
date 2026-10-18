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

#include <stddef.h>

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/encoding.h"
#include "formats/parquet/level_codec.h"
#include "formats/parquet/page_reader.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"
#include "fs/fs.h"
#include "gen_cpp/parquet_types.h"
#include "util/compression/block_compression.h"
#include "util/runtime_profile.h"
#include "util/slice.h"
#include "util/stopwatch.hpp"

namespace starrocks {
class NullableColumn;

namespace io {
class SeekableInputStream;
} // namespace io
} // namespace starrocks

namespace starrocks::parquet {

struct ColumnReaderOptions;

class ColumnChunkReader {
public:
    ColumnChunkReader(level_t max_def_level, level_t max_rep_level, int32_t type_length,
                      const tparquet::ColumnChunk* column_chunk, const ColumnReaderOptions& opts);
    ~ColumnChunkReader();

    Status init(int chunk_size);

    Status load_header();

    Status load_page();

    Status skip_values(size_t num) {
        if (num == 0) {
            return Status::OK();
        }
        return _cur_decoder->skip(num);
    }

    Status next_page();

    bool is_last_page() { return _page_reader->is_last_page(); }

    bool current_page_is_dict();

    uint32_t num_values() const { return _num_values; }

    LevelDecoder& def_level_decoder() { return _def_level_decoder; }
    LevelDecoder& rep_level_decoder() { return _rep_level_decoder; }

    // The share of the rows of the current read range that the filter keeps. The decoder of the current page gets
    // it now, and the decoder of every page that is parsed later, until the next call.
    void set_filter_share(double share) {
        _filter_share = share;
        if (_cur_decoder != nullptr) {
            _cur_decoder->set_filter_share(share);
        }
    }

    Status decode_values(size_t n, const NullInfos& null_infos, ColumnContentType content_type, Column* dst,
                         const FilterData* filter = nullptr) {
        SCOPED_RAW_TIMER(&_opts.stats->value_decode_ns);
        if (_current_row_group_no_null || _current_page_no_null) {
            return _cur_decoder->next_batch(n, content_type, dst, filter);
        }
        if (UNLIKELY(null_infos.num_nulls > 0 && !dst->is_nullable())) {
            return _null_for_non_nullable_column(null_infos.num_nulls, n);
        }
        return _cur_decoder->next_batch_with_nulls(n, null_infos, content_type, dst, filter);
    }

    Status decode_values(size_t n, ColumnContentType content_type, Column* dst, const FilterData* filter = nullptr) {
        SCOPED_RAW_TIMER(&_opts.stats->value_decode_ns);
        return _cur_decoder->next_batch(n, content_type, dst, filter);
    }

    const tparquet::ColumnMetaData& metadata() const { return _chunk_metadata->meta_data; }

    Status get_dict_values(Column* column) {
        RETURN_IF_ERROR(_try_load_dictionary());
        return _cur_decoder->get_dict_values(column);
    }

    Status get_dict_values(const Buffer<int32_t>& dict_codes, const NullableColumn& nulls, Column* column) {
        RETURN_IF_ERROR(_try_load_dictionary());
        return _cur_decoder->get_dict_values(dict_codes, nulls, column);
    }

    Status seek_to_offset(const uint64_t off) {
        // We leave the page that we are on. If its data was not read, next_page() would count it as a skipped
        // page, so we count it here too: the header was read and the data was skipped.
        if (_page_parse_state == PAGE_HEADER_PARSED) {
            _opts.stats->page_skip += 1;
        }
        RETURN_IF_ERROR(_page_reader->seek_to_offset(off));
        _page_parse_state = INITIALIZED;
        return Status::OK();
    }

    void set_page_num(size_t page_num) { _page_reader->set_page_num(page_num); }

    void set_next_read_page_idx(size_t cur_page_idx) { _page_reader->set_next_read_page_idx(cur_page_idx); }

    size_t next_read_page_idx() const { return _page_reader->next_read_page_idx(); }

    void set_num_values_read(uint64_t num_values_read) { _page_reader->set_num_values_read(num_values_read); }

    void set_page_locations(const std::vector<tparquet::PageLocation>* page_locations) {
        _page_reader->set_page_locations(page_locations);
    }

    Status load_dictionary_page();

    // True when the dictionary page of the chunk was read and parsed.
    bool dictionary_page_parsed() const { return _dict_page_parsed; }

private:
    Status _parse_page_header();
    // The error for a batch with NULL values that is read into a column that cannot hold NULL. The decoders give an
    // error for it too, but only the reader knows the file and the column.
    Status _null_for_non_nullable_column(size_t num_nulls, size_t num_values) const;
    // The dotted path of the column and the file name, for an error that only the reader can attach to its column.
    std::string _column_name() const;
    std::string _file_name() const;
    Status _set_type_length(Decoder* decoder) const;
    Status _parse_page_data();

    Status _parse_data_page(tparquet::PageType::type page_type);
    Status _parse_dict_page();
    Status _try_load_dictionary();
    int64_t _begin_dictionary_read();

private:
    enum PageParseState {
        INITIALIZED,
        PAGE_HEADER_PARSED,
        PAGE_LEVELS_PARSED,
        PAGE_DATA_PARSED,
    };

    level_t _max_def_level = 0;
    level_t _max_rep_level = 0;
    bool _current_row_group_no_null = false;
    bool _current_page_no_null = false;
    int32_t _type_length = 0;
    const tparquet::ColumnChunk* _chunk_metadata = nullptr;
    const ColumnReaderOptions& _opts;
    std::unique_ptr<PageReader> _page_reader;
    io::SeekableInputStream* _stream;

    LevelDecoder _def_level_decoder;
    LevelDecoder _rep_level_decoder;

    int _chunk_size = 0;
    size_t _num_values = 0;

    PageParseState _page_parse_state = INITIALIZED;

    bool _dict_page_parsed = false;
    Decoder* _cur_decoder = nullptr;
    double _filter_share = 1.0;
    std::unordered_map<int, std::unique_ptr<Decoder>> _decoders;
};

} // namespace starrocks::parquet
