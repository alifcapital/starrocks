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

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"
#include "formats/parquet/utils.h"
#include "io/shared_buffered_input_stream.h"

namespace starrocks::parquet {

struct GroupReaderParam;
class ColumnReader;

// Plans and coalesces IO ranges for Parquet column readers.
//
// Active ranges are planned and submitted during row-group prepare().
// Lazy ranges are pre-computed but may be submitted on demand (currently
// submitted together with active ranges during prepare; deferred lazy
// submission will be enabled when SharedBufferedInputStream supports
// incremental range addition).
//
// Used by ColumnMaterializer; not called directly by expression evaluation.
class ReadRangePlanner {
public:
    using ColumnReaderMap = std::unordered_map<SlotId, std::unique_ptr<ColumnReader>>;
    using IORange = io::SharedBufferedInputStream::IORange;

    ReadRangePlanner(const GroupReaderParam& param, ColumnReaderMap* column_readers);

    // Collect IO ranges for a set of column indices into `out`.  `types`
    // controls whether PAGE_INDEX or PAGES ranges are collected.
    // `is_active` tags the collected ranges.
    void collect_ranges(const std::vector<int>& column_indices, bool is_active, std::vector<IORange>* out,
                        int64_t* end_offset, ColumnIOTypeFlags types);

    // Deduplicate exact-duplicate ranges; if two ranges share the same
    // (offset, size), the active flag is OR-ed.
    static void deduplicate(std::vector<IORange>* ranges);

    // Row groups of a scan that must finish before we trust the share of row groups that need their lazy
    // columns. With fewer, the share is noise (one row group gives 0% or 100%), and at the start of a scan
    // every running row group is prepared before any of them finishes. Below this number we read lazy
    // ranges separately: it wastes no transfer and costs at most one extra request per row group that
    // needs them.
    static constexpr int64_t kMinRowGroupsForLazyCoalesce = 4;

    // Bytes of the lazy ranges that the shared buffered stream can merge into the buffers of active ranges.
    // A range bigger than io_coalesce_read_max_buffer_size always gets its own buffer, which is read only
    // when a column needs it, so it costs nothing whether we coalesce or not and we do not count it.
    int64_t coalescable_lazy_bytes() const;

    // Whether the lazy ranges of this row group are read in the same requests as the active ranges.
    // `lazy_bytes` is coalescable_lazy_bytes().
    //
    // Coalescing saves one request for each row group that needs its lazy columns and downloads
    // `lazy_bytes` in vain for each row group that does not. io_coalesce_read_max_distance_size is the
    // number of bytes we accept to download in vain to save one request, so we use it as the price of a
    // request. With p the share of finished row groups that needed lazy columns, we expect to save
    // p * price and to waste (1 - p) * lazy_bytes, and we coalesce only when the saving is bigger.
    // Without lazy ranges there is nothing to decide, and with io_coalesce_adaptive_lazy_active off we
    // always coalesce.
    bool should_coalesce_active_lazy(int64_t lazy_bytes) const;

    // Plan active ranges now (called during prepare).
    // Returns the planned active ranges.
    const std::vector<IORange>& active_ranges() const { return _active_ranges; }

    // Pre-compute lazy ranges (called during prepare).
    // The ranges are stored; actual stream submission is deferred.
    void pre_plan_lazy_ranges(const std::vector<int>& lazy_column_indices, int64_t* end_offset,
                              ColumnIOTypeFlags types);

    // Whether lazy ranges have been planned (non-empty set).
    bool has_lazy_ranges() const { return !_lazy_ranges.empty(); }

    // Retrieve pre-planned lazy ranges for on-demand submission.
    const std::vector<IORange>& lazy_ranges() const { return _lazy_ranges; }

private:
    const GroupReaderParam& _param;
    ColumnReaderMap* _column_readers = nullptr;

    std::vector<IORange> _active_ranges;
    std::vector<IORange> _lazy_ranges;
};

} // namespace starrocks::parquet
