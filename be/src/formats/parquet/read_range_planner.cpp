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

#include "formats/parquet/read_range_planner.h"

#include <algorithm>
#include <atomic>

#include "common/config.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/group_reader.h"

namespace starrocks::parquet {

ReadRangePlanner::ReadRangePlanner(const GroupReaderParam& param, ColumnReaderMap* column_readers)
        : _param(param), _column_readers(column_readers) {}

void ReadRangePlanner::collect_ranges(const std::vector<int>& column_indices, bool is_active, std::vector<IORange>* out,
                                      int64_t* end_offset, ColumnIOTypeFlags types) {
    // The probe of the bloom filters visits the columns of the predicate tree only. We fear that a filter of another
    // column is planned, never read, and loaded with the ranges next to it, so we plan the filter of a column only
    // when the tree has a predicate on it.
    const PredicateTree* predicate_tree =
            _param.scanner_ctx != nullptr ? &_param.scanner_ctx->predicates.predicate_tree : nullptr;
    for (const auto& index : column_indices) {
        const auto& column = _param.read_cols[index];
        ColumnIOTypeFlags column_types = types;
        if ((types & ColumnIOType::BLOOM_FILTER) != 0 &&
            (predicate_tree == nullptr || !predicate_tree->contains_column(static_cast<ColumnId>(column.slot_id())))) {
            column_types &= ~ColumnIOType::BLOOM_FILTER;
        }
        (*_column_readers)[column.slot_id()]->collect_column_io_range(out, end_offset, column_types, is_active);
    }
}

void ReadRangePlanner::deduplicate(std::vector<IORange>* ranges) {
    if (ranges == nullptr || ranges->size() <= 1) {
        return;
    }
    std::sort(ranges->begin(), ranges->end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.offset != rhs.offset) {
            return lhs.offset < rhs.offset;
        }
        if (lhs.size != rhs.size) {
            return lhs.size < rhs.size;
        }
        return lhs.is_active < rhs.is_active;
    });
    size_t write_idx = 0;
    for (size_t read_idx = 1; read_idx < ranges->size(); ++read_idx) {
        auto& current = (*ranges)[write_idx];
        const auto& next = (*ranges)[read_idx];
        if (current.offset == next.offset && current.size == next.size) {
            current.is_active = current.is_active || next.is_active;
            continue;
        }
        ++write_idx;
        if (write_idx != read_idx) {
            (*ranges)[write_idx] = next;
        }
    }
    ranges->erase(ranges->begin() + static_cast<std::ptrdiff_t>(write_idx + 1), ranges->end());
}

int64_t ReadRangePlanner::coalescable_lazy_bytes() const {
    const int64_t max_buffer_size = config::io_coalesce_read_max_buffer_size;
    int64_t bytes = 0;
    for (const auto& range : _lazy_ranges) {
        if (range.size <= max_buffer_size) {
            bytes += range.size;
        }
    }
    return bytes;
}

bool ReadRangePlanner::should_coalesce_active_lazy(int64_t lazy_bytes) const {
    if (!config::io_coalesce_adaptive_lazy_active || lazy_bytes <= 0) {
        return true;
    }
    if (_param.lazy_coalesce_row_groups_finished == nullptr || _param.lazy_coalesce_row_groups_needed == nullptr) {
        return true;
    }
    const int64_t finished = _param.lazy_coalesce_row_groups_finished->load(std::memory_order_relaxed);
    if (finished < kMinRowGroupsForLazyCoalesce) {
        return false;
    }
    // The two counters are read one after the other, so the second may be ahead of the first.
    const int64_t needed = std::min(_param.lazy_coalesce_row_groups_needed->load(std::memory_order_relaxed), finished);
    // needed / finished > lazy_bytes / (lazy_bytes + price), without a division.
    const double price = config::io_coalesce_read_max_distance_size;
    return static_cast<double>(needed) * price > static_cast<double>(finished - needed) * lazy_bytes;
}

void ReadRangePlanner::pre_plan_lazy_ranges(const std::vector<int>& lazy_column_indices, int64_t* end_offset,
                                            ColumnIOTypeFlags types) {
    _lazy_ranges.clear();
    collect_ranges(lazy_column_indices, false, &_lazy_ranges, end_offset, types);
}

} // namespace starrocks::parquet
