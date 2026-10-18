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

#include "exec/pipeline/hashjoin/deferred_local_runtime_filter.h"

#include <algorithm>
#include <cmath>
#include <new>

#include "exec/pipeline/hashjoin/hash_joiner_fwd.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
#include "exprs/runtime_filter_scan_sink.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "util/failpoint/fail_point.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

DEFINE_FAIL_POINT(deferred_rf_bloom_alloc_fails);

double estimate_deferred_rf_key_ns(LogicalType type, double avg_key_bytes) {
    // One hash and one update of a cache line of the filter.
    constexpr double kFixedWidthKeyNs = 10.0;
    // A string is hashed over its whole length.
    constexpr double kStringByteNs = 0.3;
    if (is_string_type(type)) {
        return kFixedWidthKeyNs + kStringByteNs * std::max(0.0, avg_key_bytes);
    }
    return kFixedWidthKeyNs;
}

DeferredRfEligibility decide_deferred_rf_eligibility(const DeferredRfEligibilityInput& input) {
    if (input.join_type == TJoinOp::RIGHT_OUTER_JOIN || input.join_type == TJoinOp::RIGHT_SEMI_JOIN ||
        input.join_type == TJoinOp::RIGHT_ANTI_JOIN) {
        return {false, "right join has a partitioned filter"};
    }
    if (input.join_type != TJoinOp::INNER_JOIN && input.join_type != TJoinOp::LEFT_SEMI_JOIN) {
        return {false, "join type is not supported"};
    }
    if (input.skew_join) {
        return {false, "skew join"};
    }
    if (!input.broadcast_join) {
        return {false, "join is not a broadcast join"};
    }
    if (!input.broadcast_filter || input.partitioned_filter) {
        return {false, "filter is partitioned"};
    }
    if (!input.key_type_has_bloom_filter) {
        return {false, "key type has no Bloom filter"};
    }
    return {true, "deferred", input.ndv <= input.max_ndv};
}

DeferredRfDecision decide_deferred_runtime_filter(const DeferredRfWindow& window, const DeferredRfInputs& inputs,
                                                  double margin) {
    DeferredRfDecision decision;
    decision.scan_rows_seen = inputs.scan_rows_seen;
    decision.morsels_taken = window.morsels_taken;
    decision.morsels_remaining = window.morsels_remaining;
    decision.ranges_batch = window.ranges_version;
    decision.trigger = window.trigger;
    if (inputs.ndv == 0) {
        decision.reason = "empty build side";
        return decision;
    }
    if (window.rows <= 0) {
        decision.reason = "no rows measured";
        return decision;
    }
    // The rows of a scan range are the rows that the scan emitted per range that it started. A scan that has not
    // started a range cannot have emitted a row, so this is a window that does not tell anything yet.
    if (window.morsels_taken <= 0) {
        decision.reason = "no scan range started";
        return decision;
    }

    const double rows = static_cast<double>(window.rows);
    const double duplicates =
            inputs.semi_join ? 1.0 : std::max(1.0, static_cast<double>(inputs.build_rows) / inputs.ndv);
    decision.match_ratio = std::min(1.0, static_cast<double>(window.probe_output_rows) / rows / duplicates);
    decision.saved_ns_per_row =
            (window.lookup_ns + window.intermediate_ns + window.scan_ns) / rows + inputs.reader_lazy_ns_per_row;
    if (window.remaining_raw.known && window.remaining_raw.rows_read > 0) {
        // The readers know the rows of their files. The scan emits the rows that pass its conditions, and the rows that
        // are left to scan are counted before them, so we scale them by the share that was emitted so far. The rows
        // that the scan has read and the drivers have not seen yet are in the buffers of the scan, which lowers the share
        // and keeps the estimate a lower bound.
        decision.remaining_source = DeferredRfDecision::RemainingSource::ROW_COUNTS;
        decision.remaining_raw = window.remaining_raw;
        decision.emitted_per_raw_row = std::min(
                1.0, static_cast<double>(inputs.scan_rows_seen) / static_cast<double>(window.remaining_raw.rows_read));
        decision.remaining_rows =
                static_cast<int64_t>(static_cast<double>(window.remaining_raw.total()) * decision.emitted_per_raw_row);
    } else {
        // Without row counts we take every range for one that the scan has read to its end, so the rows of a range are
        // the rows that the scan emitted per range that it started.
        decision.remaining_rows = static_cast<int64_t>(static_cast<double>(inputs.scan_rows_seen) *
                                                       static_cast<double>(window.morsels_remaining) /
                                                       static_cast<double>(window.morsels_taken));
    }
    if (decision.remaining_rows <= 0) {
        decision.reason = "nothing left to scan";
        return decision;
    }

    const double remaining = static_cast<double>(decision.remaining_rows);
    decision.benefit_ns = remaining * (1.0 - decision.match_ratio) * decision.saved_ns_per_row;
    decision.cost_ns = (static_cast<double>(inputs.ndv) + remaining) * inputs.key_ns;
    decision.build = decision.benefit_ns > margin * decision.cost_ns;
    decision.reason = decision.build ? "benefit exceeds cost" : "benefit below cost";
    return decision;
}

DeferredLocalRuntimeFilter::DeferredLocalRuntimeFilter(Params params) : _params(std::move(params)) {}

DeferredRfDecision DeferredLocalRuntimeFilter::_decide(const DeferredRfWindow& window) const {
    DeferredRfInputs inputs;
    inputs.ndv = _params.ndv;
    inputs.build_rows = _params.build_rows;
    inputs.semi_join = _params.semi_join;
    inputs.key_ns = _params.key_ns;
    inputs.scan_rows_seen = scan_rows_seen();
    int64_t lazy_ns = 0;
    int64_t lazy_rows = 0;
    for (const auto& sink : _params.scan_sinks) {
        lazy_ns += sink->lazy_read_ns();
        lazy_rows += sink->lazy_read_rows();
    }
    // A few rows say nothing about the cost of a row, and we would rather ignore the readers than trust them.
    constexpr int64_t kMinLazyRows = 1024;
    if (lazy_rows >= kMinLazyRows) {
        inputs.reader_lazy_ns_per_row = static_cast<double>(lazy_ns) / lazy_rows;
    }
    return decide_deferred_runtime_filter(window, inputs, _params.benefit_margin);
}

DeferredRfDecider::State DeferredLocalRuntimeFilter::submit(const DeferredRfWindow& window) {
    std::unique_lock lock(_mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        return State::WAITING;
    }
    if (state() != State::WAITING) {
        return state();
    }
    if (_params.cancelled && _params.cancelled()) {
        _finish(State::REFUSED, "cancelled");
        return State::REFUSED;
    }

    _decision = _decide(window);
    _evaluations.fetch_add(1, std::memory_order_relaxed);
    if (window.trigger == DeferredRfTrigger::WINDOW) {
        // The window shows a different gain per remaining row than the one before, so the driver measures again soon.
        const double gain = (1.0 - _decision.match_ratio) * _decision.saved_ns_per_row;
        const double largest = std::max(gain, _previous_gain_per_row);
        constexpr double kHysteresis = 0.10;
        _input_changed.store(_has_previous_gain && std::abs(gain - _previous_gain_per_row) > kHysteresis * largest,
                             std::memory_order_relaxed);
        _previous_gain_per_row = gain;
        _has_previous_gain = true;
    }
    if (!_decision.build) {
        // We expect scan ranges that have not arrived, or a better measurement, to change the answer. The hash table
        // lives as long as the probe, so the filter keeps its keys and the next event asks again.
        _last_wait_reason = _decision.reason;
        return State::WAITING;
    }

    Status status;
    {
        SCOPED_RAW_TIMER(&_build_ns);
        status = _build_and_publish();
    }
    if (!status.ok()) {
        // The error goes to the query: the operators of the drivers return it. The keys are released, because nobody
        // will build from them.
        _error = std::move(status);
        _decision.build = false;
        _finish(State::FAILED, "build failed");
        return State::FAILED;
    }
    _finish(State::BUILT, _decision.reason);
    return State::BUILT;
}

namespace {

// Runs f in a scope where an allocation that the query cannot afford returns a status instead of failing the query. The
// scopes do not nest: the end of one sets the thread back to a state where an allocation is not caught.
template <typename F>
Status run_catching_alloc(F&& f) {
    Status status;
    TRY_CATCH_ALLOC_SCOPE_START()
    status = f();
    TRY_CATCH_ALLOC_SCOPE_END()
    return status;
}

} // namespace

Status DeferredLocalRuntimeFilter::_build_and_publish() {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_params.mem_tracker.get());
    auto create = [&](RuntimeFilter** filter) {
        return run_catching_alloc([&]() -> Status {
            *filter = RuntimeFilterHelper::create_runtime_bloom_filter(_params.pool, _params.build_type,
                                                                       _params.join_mode);
            return *filter == nullptr ? Status::NotSupported("no runtime bloom filter for this key type")
                                      : Status::OK();
        });
    };
    RuntimeFilter* filter = nullptr;
    RETURN_IF_ERROR(create(&filter));

    const char* skipped = _params.skip_bloom_reason;
    if (skipped == nullptr) {
        // The directory of the Bloom filter is the one big allocation of the build. If the query cannot afford it, we
        // publish the min/max range alone.
        Status allocated = run_catching_alloc([&]() -> Status {
            FAIL_POINT_TRIGGER_EXECUTE(deferred_rf_bloom_alloc_fails, { throw std::bad_alloc(); });
            filter->get_membership_filter()->init(_params.ndv);
            return Status::OK();
        });
        if (allocated.is_mem_limit_exceeded()) {
            skipped = "memory limit";
            // The failed filter has a size and no directory, so we start from a new one.
            RETURN_IF_ERROR(create(&filter));
        } else {
            RETURN_IF_ERROR(allocated);
        }
    }
    if (skipped != nullptr) {
        // A Bloom filter without a directory takes no keys, and the same pass fills the min/max range.
        filter->get_membership_filter()->clear_bf();
        _bloom_skipped_reason = skipped;
    }
    // The same pass fills the Bloom filter and the min/max range.
    RETURN_IF_ERROR(run_catching_alloc([&]() -> Status {
        return RuntimeFilterHelper::fill_runtime_filter(_params.columns, _params.build_type, filter,
                                                        kHashJoinKeyColumnOffset, _params.eq_null);
    }));
    // The filter is complete before anyone can see it.
    if (_params.publish) {
        _params.publish(filter);
    }
    return Status::OK();
}

void DeferredLocalRuntimeFilter::_finish(State state, const char* reason) {
    _decision.reason = reason;
    // The hash table keeps its own references to the key columns for as long as the JOIN needs them.
    _params.columns.clear();
    _state.store(state, std::memory_order_release);
}

void DeferredLocalRuntimeFilter::cancel() {
    std::lock_guard lock(_mutex);
    if (state() != State::WAITING) {
        // The filter is final and has released the keys.
        return;
    }
    _finish(State::REFUSED, "cancelled");
}

bool DeferredLocalRuntimeFilter::holds_keys() const {
    std::lock_guard lock(_mutex);
    return !_params.columns.empty();
}

std::string DeferredLocalRuntimeFilter::last_wait_reason() const {
    std::lock_guard lock(_mutex);
    return _last_wait_reason;
}

std::string deferred_rf_section_name(int32_t filter_id) {
    return "DeferredRuntimeFilter (filter_id=" + std::to_string(filter_id) + ")";
}

void DeferredLocalRuntimeFilter::report(RuntimeProfile* profile) const {
    if (profile == nullptr) return;
    RuntimeProfile* section = profile->create_child(deferred_rf_section_name(_params.filter_id));
    auto counter = [&](const char* name, TUnit::type unit, int64_t value) {
        auto* c = ADD_COUNTER_SKIP_MERGE(section, name, unit, TCounterMergeType::SKIP_ALL);
        COUNTER_SET(c, value);
    };

    const State current = state();
    const int64_t evaluations = this->evaluations();
    // A driver that evaluates holds the lock while it writes the numbers, and the filter that is final does not change.
    // We take the lock without waiting, and report the state alone if a driver is in the middle of an evaluation.
    std::unique_lock lock(_mutex, std::try_to_lock);
    if (current == State::WAITING && (!lock.owns_lock() || evaluations == 0)) {
        section->add_info_string("State", evaluations == 0 ? "waiting: not evaluated yet" : "waiting");
        counter("Evaluations", TUnit::UNIT, evaluations);
        return;
    }

    const std::string reason = _decision.reason;
    std::string state_text = "waiting: " + reason;
    if (current == State::BUILT) {
        const char* trigger =
                _decision.trigger == DeferredRfTrigger::WINDOW ? "a window of the probe" : "new scan ranges";
        // A filter without a Bloom filter says why, in place of the reason of the decision, which is the same for all.
        state_text = _bloom_skipped_reason == nullptr ? std::string("built late by ") + trigger + ": " + reason
                                                      : std::string("built min/max only by ") + trigger +
                                                                ": bloom skipped: " + _bloom_skipped_reason;
    } else if (current == State::REFUSED) {
        state_text = "refused: " + reason;
    } else if (current == State::FAILED) {
        state_text = "failed: " + reason;
    }
    section->add_info_string("State", state_text);
    // The last time the filter was not built, whether it waits now, was built or ended later.
    if (*_last_wait_reason != '\0') {
        section->add_info_string("LastWaitReason", _last_wait_reason);
    }
    counter("Evaluations", TUnit::UNIT, evaluations);
    counter("Ndv", TUnit::UNIT, static_cast<int64_t>(_params.ndv));
    counter("MatchPermille", TUnit::UNIT, static_cast<int64_t>(_decision.match_ratio * 1000));
    counter("SavedNsPerRow", TUnit::UNIT, static_cast<int64_t>(_decision.saved_ns_per_row));
    counter("RemainingRows", TUnit::UNIT, _decision.remaining_rows);
    if (_decision.remaining_source == DeferredRfDecision::RemainingSource::ROW_COUNTS) {
        const auto& raw = _decision.remaining_raw;
        section->add_info_string(
                "RemainingRowsFrom",
                strings::Substitute("row counts of the files: $0 rows in opened ranges, $1 rows in taken ranges "
                                    "without a row count (by bytes), $2 rows in ranges not started, of which the "
                                    "scan emits $3 per mille; ranges that the queues hold now",
                                    raw.open_ranges, raw.pending_ranges, raw.not_started_ranges,
                                    static_cast<int64_t>(_decision.emitted_per_raw_row * 1000)));
    } else {
        section->add_info_string("RemainingRowsFrom",
                                 "ranges: rows seen * ranges not started / ranges started, over the ranges that the "
                                 "queues hold now; no reader knows the rows of its file");
    }
    counter("BenefitTime", TUnit::TIME_NS, static_cast<int64_t>(_decision.benefit_ns));
    counter("CostTime", TUnit::TIME_NS, static_cast<int64_t>(_decision.cost_ns));
    counter("BuildTime", TUnit::TIME_NS, _build_ns);
    // The counters that show how the remaining rows were found, until the estimate is checked on the stand.
    counter("ScanRowsSeen", TUnit::UNIT, _decision.scan_rows_seen);
    counter("RangesStarted", TUnit::UNIT, _decision.morsels_taken);
    counter("RangesNotStarted", TUnit::UNIT, _decision.morsels_remaining);
    counter("RangesBatch", TUnit::UNIT, _decision.ranges_batch);
    if (!_error.ok()) {
        section->add_info_string("Error", _error.to_string());
    }
}

} // namespace starrocks::pipeline
