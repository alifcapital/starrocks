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
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "column/column.h"
#include "common/status.h"
#include "exec/pipeline/hashjoin/local_runtime_filter_feedback.h"
#include "gen_cpp/PlanNodes_types.h"
#include "types/logical_type.h"

namespace starrocks {
class MemTracker;
class ObjectPool;
class RuntimeFilter;
class RuntimeFilterScanSink;
class RuntimeProfile;
} // namespace starrocks

namespace starrocks::pipeline {

// What decides whether the build side keeps a local Bloom filter back, or builds nothing as it does without the
// deferral.
struct DeferredRfEligibilityInput {
    TJoinOp::type join_type = TJoinOp::INNER_JOIN;
    bool has_other_join_conjuncts = false;
    bool skew_join = false;
    // One hash table holds all keys of the build side, and every prober of the JOIN reads it.
    bool broadcast_join = false;
    bool broadcast_filter = false;
    // The filter is split by partition of the build side: by hash partition or by colocate bucket.
    bool partitioned_filter = false;
    bool key_type_has_bloom_filter = false;
    // The distinct keys of the build side, which is what the Bloom filter is sized for, and the most distinct keys that
    // a global runtime filter may build a Bloom filter for. A global runtime filter counts its elements the same way: it
    // sums the numbers that its partial filters were initialized with and compares the sum with this limit.
    size_t ndv = 0;
    size_t max_ndv = UINT64_MAX;
};

struct DeferredRfEligibility {
    bool defer = false;
    const char* reason = "";
    // False when the filter is kept back without a Bloom filter, because it would hold more keys than the limit. As a
    // global runtime filter does above the limit, it keeps its min/max range only.
    bool bloom = true;
};

// A filter is kept back only where one filter built from one hash table is the filter that the planner asks for, and
// where the share of probe rows with a pair can be read from the lookups:
// - INNER JOIN and LEFT SEMI JOIN drop the probe rows without a pair and have a broadcast filter. Their lookups return
//   a row for every pair, except a LEFT SEMI JOIN without conditions on the pairs, which returns a row for every probe
//   row with a pair.
// - RIGHT OUTER, RIGHT SEMI and RIGHT ANTI JOIN drop them too, but the planner only shuffles them, so their filters are
//   partitioned or grouped by colocate bucket, and we do not defer those.
// - LEFT OUTER, FULL OUTER and LEFT ANTI JOIN keep the probe rows without a pair and have no filter.
// - A skew join splits the probe rows between two joins, and a filter of one of them may drop rows of the other.
// A filter for more distinct keys than the limit is kept back too, without a Bloom filter.
DeferredRfEligibility decide_deferred_rf_eligibility(const DeferredRfEligibilityInput& input);

// The child section of a profile where the numbers of one deferred filter go.
std::string deferred_rf_section_name(int32_t filter_id);

// What the decision knows about the filter and the scan besides the measurement window.
struct DeferredRfInputs {
    // Distinct keys of the build side, which is the size of the Bloom filter.
    size_t ndv = 0;
    size_t build_rows = 0;
    // LEFT SEMI JOIN returns a probe row once, so its output rows are the matched probe rows. INNER JOIN returns a
    // probe row once for every build row with its key.
    bool semi_join = false;
    // Cost in ns to put one key into the filter, which is also the cost to check one probe row.
    double key_ns = 0;
    // Cost in ns that the readers of the scan spend on the lazy columns of one row that passed all conditions.
    double reader_lazy_ns_per_row = 0;
    // Rows that all scan operators of the filter have emitted so far.
    int64_t scan_rows_seen = 0;
};

struct DeferredRfDecision {
    bool build = false;
    const char* reason = "";
    // Share of the probe rows that found a pair in the hash table.
    double match_ratio = 0;
    // Work in ns that one dropped row does not cost: the scan, the operators before the JOIN and the lookup.
    double saved_ns_per_row = 0;
    // Where the remaining rows come from: the rows that the scan emitted so far, spread over the scan ranges that it
    // started, times the scan ranges that it has not started.
    int64_t scan_rows_seen = 0;
    int64_t morsels_taken = 0;
    int64_t morsels_remaining = 0;
    // The batch of scan ranges that the queues had: the number of times morsels were put in them.
    int64_t ranges_batch = 0;
    DeferredRfTrigger trigger = DeferredRfTrigger::WINDOW;
    // Where the remaining rows come from.
    enum class RemainingSource { RANGES, ROW_COUNTS };
    RemainingSource remaining_source = RemainingSource::RANGES;
    // With ROW_COUNTS: the rows that the storage counts, and the share of them that the scan emits.
    RemainingRawRows remaining_raw;
    double emitted_per_raw_row = 0;
    int64_t remaining_rows = 0;
    double benefit_ns = 0;
    double cost_ns = 0;
};

// We build the filter when the work that it spares exceeds its cost by margin. The work is what the dropped rows
// would cost: remaining rows * (1 - match ratio) * saved_ns_per_row. The cost is to put every distinct key into the
// filter and to check every remaining row against it.
//
// The remaining rows are what the scan ranges that the queues have now promise. While the scan gets more ranges this
// is a lower bound, so we expect the benefit to grow when they arrive, and a filter that does not pay off is not
// refused: it waits and is asked again.
DeferredRfDecision decide_deferred_runtime_filter(const DeferredRfWindow& window, const DeferredRfInputs& inputs,
                                                  double margin);

// Cost in ns to hash one key into the filter, or to check one row against it. A fixed-width key costs one hash and
// one update of a cache line. A string key costs more with its length because the whole value is hashed.
double estimate_deferred_rf_key_ns(LogicalType type, double avg_key_bytes);

// A local Bloom filter that the build side did not build because its build side is large. The key columns of the hash
// table are kept until the filter is built or refused. The filter is built from them by the first driver whose
// measurement window shows that it pays off, and is published to the probe descriptors, which every scan operator and
// reader of the filter reads on each chunk.
//
// A window that shows that the filter does not pay off yet leaves it waiting with its keys, because we expect scan
// ranges that have not arrived, or a better measurement, to change the answer. Only the end of the probe, a
// cancelled query and a build that spills end the wait without a filter.
//
// The query has to afford the Bloom filter. When the keys are more than a global runtime filter builds a Bloom filter
// for, or when the memory of the query does not fit the Bloom filter, we publish the filter with its min/max range
// only, as the merger of global runtime filters does for a partial filter that is too large. Any other error of the
// build fails the query, as the build of a filter that is not deferred does.
//
// The min/max range of the filter is built together with the Bloom filter, from the same pass over the keys: a
// separate pass at build time costs a scan of the build side and delays the start of the probe side.
class DeferredLocalRuntimeFilter final : public DeferredRfDecider {
public:
    struct Params {
        int32_t filter_id = 0;
        LogicalType build_type = TYPE_UNKNOWN;
        int8_t join_mode = 0;
        bool eq_null = false;
        bool semi_join = false;
        size_t ndv = 0;
        size_t build_rows = 0;
        double key_ns = 0;
        double benefit_margin = 2.0;
        // Set when the filter has no Bloom filter, for a reason that we know before the build. The filter keeps its
        // min/max range only, as a global runtime filter does above its limit.
        const char* skip_bloom_reason = nullptr;
        // Key column of every hash table of the build side, with the row that the hash table reserves in front.
        Columns columns;
        // The filter lives as long as the fragment, because the readers use it without owning it.
        ObjectPool* pool = nullptr;
        // The memory of the filter is charged to the query, whichever driver builds it.
        std::shared_ptr<MemTracker> mem_tracker;
        // The sinks of the probe descriptors, where the readers report the cost of their lazy columns.
        std::vector<std::shared_ptr<RuntimeFilterScanSink>> scan_sinks;
        // Puts the filter where the scan operators and readers find it.
        std::function<void(const RuntimeFilter*)> publish;
        std::function<bool()> cancelled;
    };

    explicit DeferredLocalRuntimeFilter(Params params);
    ~DeferredLocalRuntimeFilter() override = default;

    int32_t filter_id() const override { return _params.filter_id; }
    State state() const override { return _state.load(std::memory_order_acquire); }
    // A driver that finds another driver in the middle of the decision does not wait: it gets WAITING and asks for
    // the state again with its next window.
    State submit(const DeferredRfWindow& window) override;
    void observe_scan_rows(int64_t rows) override { _scan_rows.fetch_add(rows, std::memory_order_relaxed); }
    bool input_changed() const override { return _input_changed.load(std::memory_order_relaxed); }
    // The error is written before the state, so it is final once the state is FAILED.
    Status error() const override { return state() == State::FAILED ? _error : Status::OK(); }
    void report(RuntimeProfile* profile) const override;

    // The hash table is closed, or the JOIN does not probe it any more, so nobody will ask for the filter. A
    // filter that is not built is refused, and the key columns are released.
    void cancel() override;

    // True while the filter holds the key columns, which is until it is built, refused or cancelled.
    bool holds_keys() const;
    const DeferredRfDecision& decision() const { return _decision; }
    // Why the built filter has a min/max range only, or null when it has a Bloom filter too.
    const char* bloom_skipped_reason() const { return _bloom_skipped_reason; }
    // How many times the filter was evaluated, and why it was not built the last time that it was not.
    int64_t evaluations() const { return _evaluations.load(std::memory_order_relaxed); }
    std::string last_wait_reason() const;
    int64_t scan_rows_seen() const { return _scan_rows.load(std::memory_order_relaxed); }

private:
    DeferredRfDecision _decide(const DeferredRfWindow& window) const;
    Status _build_and_publish();
    void _finish(State state, const char* reason);

    Params _params;
    std::atomic<State> _state{State::WAITING};
    std::atomic<int64_t> _scan_rows{0};
    std::atomic<int64_t> _evaluations{0};
    std::atomic<bool> _input_changed{false};

    mutable std::mutex _mutex;
    DeferredRfDecision _decision;
    const char* _last_wait_reason = "";
    const char* _bloom_skipped_reason = nullptr;
    int64_t _build_ns = 0;
    // The gain per remaining row of the evaluation by a window before this one.
    double _previous_gain_per_row = 0;
    bool _has_previous_gain = false;
    Status _error;
};

} // namespace starrocks::pipeline
