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

#include <atomic>
#include <cstdint>

namespace starrocks {

class ExprContext;
class RuntimeProfile;

// Work done by conditional two-phase evaluation (CASE / IF / IFNULL / COALESCE) in one fragment instance.
//
// We want a query profile to show whether two-phase evaluation runs, on how many rows, and how much it copies.
// All driver threads of a fragment instance share one RuntimeState, and one ExprContext can be evaluated by
// several drivers at once, so the counters are relaxed atomics. We expect a few updates per evaluated chunk,
// never per row, so the cost stays small next to the evaluation itself.
struct ConditionalTwoPhaseStats {
    // Calls that routed rows to branches and assembled the result.
    std::atomic<int64_t> calls{0};
    // Calls that returned the first branch directly because its guard was true on every row.
    std::atomic<int64_t> all_true_shortcuts{0};
    // Rows of the chunks passed to both kinds of calls.
    std::atomic<int64_t> input_rows{0};
    // Rows evaluated through Expr::evaluate_selected, i.e. the rows routed to a two-phase branch.
    std::atomic<int64_t> selected_rows{0};
    // Rows evaluated over the whole chunk: cheap branches, branches that got every row, the shortcut branch.
    std::atomic<int64_t> full_rows{0};
    // Copies of the routed rows that evaluate_selected makes for a branch.
    std::atomic<int64_t> subchunk_copies{0};
    std::atomic<int64_t> subchunk_copied_columns{0};
    // Copies that took every column of the chunk, because the branch inputs were unknown or were all lazy.
    std::atomic<int64_t> subchunk_whole_chunk_copies{0};
    std::atomic<int64_t> subchunk_copied_bytes{0};
    // Lazy columns read on demand for a copy, and the routed rows cut from them. Cache hits are not counted.
    std::atomic<int64_t> lazy_provide_calls{0};
    std::atomic<int64_t> lazy_provide_rows{0};
    // Wall time of the outermost two-phase calls; a conditional nested in a branch is part of its parent's time.
    std::atomic<int64_t> time_ns{0};
    // Wall time spent assembling results, summed over all calls.
    std::atomic<int64_t> assemble_time_ns{0};

    static void add(std::atomic<int64_t>& counter, int64_t value) {
        counter.fetch_add(value, std::memory_order_relaxed);
    }

    // Copies the current values into a "ConditionalTwoPhase" counter group of the profile. Does nothing while
    // two-phase evaluation has not run, so profiles of other queries stay unchanged.
    void update_profile(RuntimeProfile* profile) const;
};

// Returns nullptr when the context has no RuntimeState (FE constant folding, some loads and tests); callers
// then do not count.
ConditionalTwoPhaseStats* conditional_two_phase_stats(ExprContext* context);

} // namespace starrocks
