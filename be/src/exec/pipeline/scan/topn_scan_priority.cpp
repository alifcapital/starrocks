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

#include "exec/pipeline/scan/topn_scan_priority.h"

#include "gen_cpp/PlanNodes_types.h"

namespace starrocks::pipeline {

TopnScanPriority topn_scan_priority(const THdfsScanRange& range, int32_t slot_id, bool desc, bool nulls_first) {
    TopnScanPriority priority;
    bool has_null = false;
    bool all_null = false;
    bool has_bound = false;
    int64_t key = 0;

    if (range.__isset.min_max_values) {
        const auto& min_max_values = range.min_max_values;
        auto it = min_max_values.find(slot_id);
        if (it != min_max_values.end()) {
            const TExprMinMaxValue& v = it->second;
            has_null = v.has_null;
            all_null = v.all_null;
            if (!all_null) {
                // Integer, date and timestamp encodings preserve order: max for DESC, min for ASC.
                if (desc && v.__isset.max_int_value) {
                    key = v.max_int_value;
                    has_bound = true;
                } else if (!desc && v.__isset.min_int_value) {
                    key = v.min_int_value;
                    has_bound = true;
                }
            }
        }
    }

    if (nulls_first && (all_null || has_null)) {
        priority.rank = 0; // nulls lead -> serve first
    } else if (has_bound) {
        priority.rank = 1;
        priority.value = key;
    } else {
        priority.rank = 2; // no usable bound -> serve last
    }
    return priority;
}

} // namespace starrocks::pipeline
