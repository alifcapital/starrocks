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

#include "formats/parquet/stored_column_reader_with_index.h"

#include <glog/logging.h>

#include "common/compiler_util.h"
#include "gen_cpp/parquet_types.h"
#include "gutil/strings/substitute.h"
#include "util/defer_op.h"

namespace starrocks {
class Column;
} // namespace starrocks

namespace starrocks::parquet {

Status StoredColumnReaderWithIndex::read_range(const Range<uint64_t>& range, const Filter* filter,
                                               ColumnContentType content_type, Column* dst) {
    const auto& page_locations = _offset_index_ctx->offset_index.page_locations;
    // The reader indexes the offset index with the page positions that it keeps, and it moves them forward
    // only. We expect the page positions to be pages of the offset index, and a read to start at or after
    // the page that the reader is on. If a broken offset index or a caller breaks that, the reader would read
    // the rows of another page or past the end of the index, so it returns an error.
    if (UNLIKELY(_page_num == 0 || _page_num != page_locations.size() || _cur_page_idx >= _page_num)) {
        return Status::InternalError(
                strings::Substitute("The reader has $0 pages and is on page $1, but the offset index has $2 pages",
                                    _page_num, _cur_page_idx, page_locations.size()));
    }
    if (UNLIKELY(range.begin() < page_locations[_cur_page_idx].first_row_index + _offset_index_ctx->rg_first_row)) {
        return Status::InternalError(strings::Substitute(
                "The range starts at row $0, before row $1 where the current page $2 starts", range.begin(),
                page_locations[_cur_page_idx].first_row_index + _offset_index_ctx->rg_first_row, _cur_page_idx));
    }
    size_t stop_page_idx = _cur_page_idx;
    while (stop_page_idx < _page_num - 1 &&
           range.end() >= _offset_index_ctx->offset_index.page_locations[stop_page_idx + 1].first_row_index +
                                  _offset_index_ctx->rg_first_row) {
        stop_page_idx++;
    }

    // for next read
    DeferOp op([&]() { _cur_page_idx = stop_page_idx; });

    // deal with dict page
    // reader move stream cursor, move reader row cursor, load specific_page
    if (_has_dict_page && !_dict_page_loaded) {
        RETURN_IF_ERROR(_inner_reader->load_dictionary_page());
        _dict_page_loaded = true;
    }

    if (_cur_page_idx == _page_num - 1 ||
        range.begin() < _offset_index_ctx->offset_index.page_locations[_cur_page_idx + 1].first_row_index +
                                _offset_index_ctx->rg_first_row) {
        return _inner_reader->read_range(range, filter, content_type, dst);
    } else {
        while (_cur_page_idx < _page_num - 1 &&
               range.begin() >= _offset_index_ctx->offset_index.page_locations[_cur_page_idx + 1].first_row_index +
                                        _offset_index_ctx->rg_first_row) {
            _cur_page_idx++;
        }
        // A page that no selected range touches was not collected for reading. A read that lands in it
        // means that the selection and the reads disagree.
        if (UNLIKELY(!_offset_index_ctx->page_selected[_cur_page_idx])) {
            return Status::InternalError(strings::Substitute("The range [$0, $1) is in page $2, which is not selected",
                                                             range.begin(), range.end(), _cur_page_idx));
        }

        // A reader that skips pages through the offset index moves to the page of the first selected row by itself.
        // The rows at the start of the range may be not selected, so that page may be a later one, and we do not
        // want to read the header of the page where the range starts. So we leave the move to such a reader.
        if (!_inner_reader->seeks_by_offset_index(filter)) {
            RETURN_IF_ERROR(_inner_reader->load_specific_page(
                    _cur_page_idx, _offset_index_ctx->offset_index.page_locations[_cur_page_idx].offset,
                    _offset_index_ctx->offset_index.page_locations[_cur_page_idx].first_row_index +
                            _offset_index_ctx->rg_first_row));
        }
        return _inner_reader->read_range(range, filter, content_type, dst);
    }
}

} // namespace starrocks::parquet
