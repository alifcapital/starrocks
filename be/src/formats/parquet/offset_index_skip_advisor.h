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
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/config.h"

namespace starrocks::parquet {

// Scan-local performance feedback for the columns that read their offset index to skip pages. The read of an offset
// index is a remote read in every row group of every file, and it pays only when a read can jump over a page. We
// fear a column whose rows are not clustered (unsorted data, one selected row in every page), where no page can be
// skipped, so the reads are wasted in every row group. So a column that read its offset index in the first
// parquet_offset_index_skip_give_up_row_groups row groups of the scan and skipped no page in any of them does not
// read it for the rest of the scan. A column that skipped a page once never gives up. The state is keyed by the
// path of the column, and it is shared by the data sources of one scan node (HiveDataSourceProvider). Never a proof:
// a column that does not read the index reads page by page, and the rows stay the same.
class OffsetIndexSkipAdvisor {
public:
    // False once the column has given up.
    bool should_load(const std::string& column) {
        const int32_t row_groups = config::parquet_offset_index_skip_give_up_row_groups;
        if (row_groups <= 0) {
            return true;
        }
        std::lock_guard lock(_mutex);
        auto it = _history.find(column);
        return it == _history.end() || it->second.skipped_pages > 0 || it->second.row_groups < row_groups;
    }

    // A row group of the column read its offset index, and the reader skipped skipped_pages pages through it.
    void observe(const std::string& column, int64_t skipped_pages) {
        std::lock_guard lock(_mutex);
        auto it = _history.find(column);
        if (it == _history.end()) {
            // A table with very many columns must not grow query memory without limit. Forgetting feedback only
            // makes a column read its offset index again.
            if (_history.size() == kMaxColumns) {
                _history.erase(_history.begin());
            }
            it = _history.emplace(column, History{}).first;
        }
        ++it->second.row_groups;
        it->second.skipped_pages += skipped_pages;
    }

private:
    static constexpr size_t kMaxColumns = 4096;

    struct History {
        int64_t row_groups = 0;
        int64_t skipped_pages = 0;
    };
    std::mutex _mutex;
    std::unordered_map<std::string, History> _history;
};

} // namespace starrocks::parquet
