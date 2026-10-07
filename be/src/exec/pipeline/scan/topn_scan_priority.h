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
#include <string>

namespace starrocks {
struct THdfsScanRange;

namespace pipeline {

// NULLS FIRST files precede bounded files; files without a bound come last.
struct TopnScanPriority {
    int rank = 2;
    int64_t value = 0;
    // A VARCHAR/CHAR key carries its bound as raw bytes in string_value instead of value.
    bool is_string = false;
    std::string string_value;

    int compare(const TopnScanPriority& other, bool desc) const {
        if (rank != other.rank) return rank < other.rank ? -1 : 1;
        if (rank != 1) return 0;
        int order;
        if (is_string != other.is_string) {
            // One scan has one reorder slot, so its bounds have one kind. This only keeps the order total.
            order = is_string ? 1 : -1;
        } else if (is_string) {
            // We want the order of the VARCHAR sort: unsigned bytes, a shorter prefix first.
            // std::string::compare gives that order, because char_traits<char> compares as unsigned char.
            order = string_value.compare(other.string_value);
        } else {
            order = value < other.value ? -1 : (value > other.value ? 1 : 0);
        }
        if (order == 0) return 0;
        return (desc ? order > 0 : order < 0) ? -1 : 1;
    }
};

TopnScanPriority topn_scan_priority(const THdfsScanRange& range, int32_t slot_id, bool desc, bool nulls_first);

} // namespace pipeline
} // namespace starrocks
