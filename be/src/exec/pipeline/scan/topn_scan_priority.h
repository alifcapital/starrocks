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

namespace starrocks {
struct THdfsScanRange;

namespace pipeline {

// NULLS FIRST files precede bounded files; files without a bound come last.
struct TopnScanPriority {
    int rank = 2;
    int64_t value = 0;

    int compare(const TopnScanPriority& other, bool desc) const {
        if (rank != other.rank) return rank < other.rank ? -1 : 1;
        if (rank == 1 && value != other.value) {
            return (desc ? value > other.value : value < other.value) ? -1 : 1;
        }
        return 0;
    }
};

TopnScanPriority topn_scan_priority(const THdfsScanRange& range, int32_t slot_id, bool desc, bool nulls_first);

} // namespace pipeline
} // namespace starrocks
