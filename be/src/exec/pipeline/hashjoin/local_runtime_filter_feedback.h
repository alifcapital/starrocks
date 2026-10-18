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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exec/pipeline/scan/scan_range_ledger.h"

namespace starrocks {
class RuntimeProfile;
}

namespace starrocks::pipeline {

// What makes a filter evaluate again. Only an event that can raise the gain does: the probe closed a window, which
// can show a different share of rows with a pair or a different cost per row, or the scan got scan ranges, which
// raises the rows that are left. The progress of the scan never does, because it only lowers the rows that are left.
enum class DeferredRfTrigger { WINDOW, NEW_RANGES };

// One driver's measurement window of the JOIN and the scan below it, taken while the filter does not exist yet. The
// window ends only after every row that the scan emitted has completed its lookup.
struct DeferredRfWindow {
    DeferredRfTrigger trigger = DeferredRfTrigger::WINDOW;
    int64_t chunks = 0;
    // Rows that the scan emitted in the window. Without a filter all of them reach the JOIN.
    int64_t rows = 0;
    // Work in ns of the hash table lookup and of the operators between the scan and the JOIN, for these rows.
    int64_t lookup_ns = 0;
    int64_t intermediate_ns = 0;
    // Work in ns that the scan operator spent to emit these rows.
    int64_t scan_ns = 0;
    // Rows that the lookups of these rows returned, before any other condition of the JOIN.
    int64_t probe_output_rows = 0;
    // Scan ranges that the scan has started, and the ones that it has not started yet. While the scan gets more
    // ranges, the second number is a lower bound of what is left.
    int64_t morsels_taken = 0;
    int64_t morsels_remaining = 0;
    // The rows that the taken ranges have not read yet, when the readers know the rows of their files. A scan that
    // reads a few large files takes all of them at once, so the ranges alone say that nothing is left.
    RemainingRawRows remaining_raw;
    // Changes when the scan gets scan ranges, or learns that no more are to come.
    int64_t ranges_version = 0;
};

// A filter that is built after the scan has started. It builds itself from a window that shows that it pays off, and
// otherwise waits: the hash table lives as long as the probe, so the filter can be built and published at any time
// before the probe ends.
class DeferredRfDecider {
public:
    // FAILED is a filter whose build returned an error, which the operators of the drivers return to the query.
    enum class State { WAITING, BUILT, REFUSED, FAILED };

    virtual ~DeferredRfDecider() = default;
    virtual int32_t filter_id() const = 0;
    virtual State state() const = 0;
    // Returns the state after the window. WAITING means that another driver is deciding right now, or that the filter
    // does not pay off yet and is asked again with the next window, or when the scan gets new scan ranges.
    virtual State submit(const DeferredRfWindow& window) = 0;
    virtual void observe_scan_rows(int64_t rows) = 0;
    // True when the last evaluation saw a gain per remaining row that differs from the one before by more than the
    // hysteresis of the adaptive filters, so the driver measures again soon.
    virtual bool input_changed() const { return false; }
    // The error of a filter that FAILED.
    virtual Status error() const { return Status::OK(); }
    // Nobody will use the filter any more, so a filter that is not built is refused. A filter that is built stays.
    virtual void cancel() {}
    // Adds a section with the state of the filter and the numbers of its last evaluation to the profile of the JOIN.
    virtual void report(RuntimeProfile*) const {}
};

// One scan and its consuming JOIN share this state within a pipeline driver.
// A measurement ends only after every emitted row has completed its lookup.
//
// A driver starts in the DEFERRED phase when the build side kept a filter back: no filter exists, so the window
// only measures what a row costs without it, and gives every closed window to the filter, which builds itself when
// that pays off. A filter that does not pay off yet keeps waiting: we expect scan ranges that have not arrived to
// make it pay off, and readers that have not reported the cost of their lazy columns to make the rows look more
// expensive. So the driver goes on measuring and gives the filter the next window, and when the scan gets new scan
// ranges it asks the filter again at once. The windows of a waiting filter follow the schedule of the adaptive
// filters: the first one has 64 chunks, the next ones wait 256 chunks, and the wait grows four times while the gain
// per row stays the same, up to 16384 chunks, and goes back to 256 when it changes. A driver whose window did not
// decide takes the answer of the driver that did, and goes on as if the filter had existed from its first row.
class LocalRuntimeFilterFeedback {
public:
    struct Totals {
        int64_t input_rows[2] = {};
        int64_t filter_ns[2] = {};
        int64_t lookup_ns[2] = {};
        int64_t intermediate_ns[2] = {};
        int64_t decisions = 0;
        int64_t switches = 0;
    };

    // Returns the filters that the build side kept back, or nothing when the driver has to measure ordinary filters.
    using DeferredResolver = std::function<std::vector<std::shared_ptr<DeferredRfDecider>>()>;
    // Adds what only the operators of the driver know to the window, before it goes to the filters.
    using WindowFiller = std::function<void(DeferredRfWindow*)>;
    // The version of the scan ranges alone, which the driver reads on every chunk, so it is cheaper than the filler.
    using RangesVersionReader = std::function<int64_t()>;

    bool enabled() const { return _enabled; }
    bool use_filter() const {
        return !_enabled || _phase == Phase::INITIAL_ON || _phase == Phase::MEASURE_ON ||
               _phase == Phase::AFTER_BUILD || (_phase == Phase::WAIT && _preferred_on);
    }
    bool needs_drain() const {
        return _enabled && (_phase == Phase::AFTER_BUILD || (_chunks >= window_chunks() && _rows >= kMinRows));
    }
    int64_t outstanding_rows() const { return _outstanding; }
    int recheck_interval() const { return _interval; }
    const Totals& totals() const { return _totals; }
    void disable() { _enabled = false; }

    // The build side decides whether a filter was kept back after the scan driver is prepared, so the driver asks at
    // its first chunk, which is after the build side has finished.
    void set_deferred_resolver(DeferredResolver resolver, WindowFiller filler,
                               RangesVersionReader ranges_version = nullptr) {
        _resolver = std::move(resolver);
        _filler = std::move(filler);
        _ranges_version = std::move(ranges_version);
    }
    void resolve_deferred_once() {
        if (_resolved || !_resolver || !_enabled) return;
        _resolved = true;
        _deciders = _resolver();
        if (_deciders.empty()) return;
        _phase = Phase::DEFERRED;
        apply_deferred_states();
    }
    bool deferred() const { return _phase == Phase::DEFERRED; }
    // The scan got scan ranges, or learned that no more are to come, after the last window was given to the filters.
    // We expect the answer "not yet", which was based on the ranges of that window, to change, so the filters
    // evaluate the same measurement again with the new progress of the scan, and the driver does not wait for the next
    // window to close.
    void request_retry() {
        if (!_enabled || _phase != Phase::DEFERRED || !_has_last_window || !_filler) return;
        _filler(&_last_window);
        _last_window.trigger = DeferredRfTrigger::NEW_RANGES;
        for (const auto& decider : _deciders) (void)decider->submit(_last_window);
        apply_deferred_states();
    }
    // The JOIN probes through other probers than the one that this feedback counts, for example because it spills,
    // so we expect no window to end. We stop measuring, and a filter that was kept back and is not built is
    // refused, because no window will measure it. A filter that is built stays, and the scan evaluates it as it does
    // without the feedback.
    void stop() {
        if (_stopped) return;
        resolve_deferred_once();
        for (const auto& decider : _deciders) decider->cancel();
        _stopped = true;
        disable();
    }
    bool stopped() const { return _stopped; }
    // True when the feedback is off because every filter that was kept back ended without a filter, which is a
    // decision and not a failure of the accounting.
    bool deferral_refused() const { return _deferral_refused; }
    // True when a filter that was kept back failed to build. The operators return deferral_status() to the query.
    bool deferral_failed() const { return _deferral_failed; }
    Status deferral_status() const {
        for (const auto& decider : _deciders) {
            Status status = decider->error();
            if (!status.ok()) return status;
        }
        return Status::OK();
    }
    const std::vector<std::shared_ptr<DeferredRfDecider>>& deciders() const { return _deciders; }
    // True when the filter is kept back and does not exist: it is not built yet or it was refused. The scan must
    // not take its absence for a failure.
    bool awaiting_deferred(int32_t filter_id) const {
        for (const auto& decider : _deciders) {
            if (decider->filter_id() == filter_id) return decider->state() != DeferredRfDecider::State::BUILT;
        }
        return false;
    }

    // scan_ns is the work of the scan operator itself to emit the chunk.
    void observe_filter(int64_t input_rows, int64_t output_rows, int64_t filter_ns, int64_t scan_ns = 0) {
        if (!_enabled) return;
        if (input_rows < 0 || output_rows < 0 || output_rows > input_rows || filter_ns < 0 || scan_ns < 0) {
            disable();
            return;
        }
        if (_phase == Phase::DEFERRED) {
            for (const auto& decider : _deciders) decider->observe_scan_rows(input_rows);
        }
        const bool on = use_filter();
        _totals.input_rows[on] += input_rows;
        _totals.filter_ns[on] += filter_ns;
        _outstanding += output_rows;
        _rows += input_rows;
        _passed += output_rows;
        _filter_ns += filter_ns;
        _scan_ns += scan_ns;
        ++_chunks;
        maybe_finish_window();
        if (_phase == Phase::DEFERRED) {
            retry_when_ranges_changed();
            apply_deferred_states();
        }
    }

    // output_rows is what the lookups of the completed rows returned.
    void observe_lookup(int64_t completed_rows, int64_t lookup_ns, int64_t intermediate_ns = 0,
                        int64_t output_rows = 0) {
        if (!_enabled) return;
        if (completed_rows < 0 || completed_rows > _outstanding || lookup_ns < 0 || intermediate_ns < 0 ||
            output_rows < 0) {
            disable();
            return;
        }
        _outstanding -= completed_rows;
        _lookup_ns += lookup_ns;
        _intermediate_ns += intermediate_ns;
        _output_rows += output_rows;
        _totals.lookup_ns[use_filter()] += lookup_ns;
        _totals.intermediate_ns[use_filter()] += intermediate_ns;
        maybe_finish_window();
    }

private:
    // DEFERRED: the filter is kept back, the window measures the JOIN without it.
    // AFTER_BUILD: the filter was built while rows that were looked up without it were still in the operators. The
    // driver drains them, because their lookup work does not belong to a window with the filter.
    enum class Phase { INITIAL_ON, INITIAL_OFF, WAIT, MEASURE_ON, MEASURE_OFF, DEFERRED, AFTER_BUILD };
    static constexpr int kWindowChunks = 64;
    static constexpr int64_t kMinRows = 131072;
    static constexpr int kInitialInterval = 256;
    static constexpr int kMaxInterval = 16384;
    int window_chunks() const {
        if (_phase == Phase::WAIT) return _interval;
        return _phase == Phase::DEFERRED ? _deferred_chunks : kWindowChunks;
    }

    void reset_window() {
        _chunks = 0;
        _rows = _passed = _filter_ns = _lookup_ns = _intermediate_ns = _scan_ns = _output_rows = 0;
    }

    // Takes what the filters decided. A filter that is built is applied by every driver, whichever window built it.
    void apply_deferred_states() {
        if (_deciders.empty()) return;
        bool any_built = false;
        bool all_refused = true;
        for (const auto& decider : _deciders) {
            const auto state = decider->state();
            any_built |= state == DeferredRfDecider::State::BUILT;
            _deferral_failed |= state == DeferredRfDecider::State::FAILED;
            all_refused &= state == DeferredRfDecider::State::REFUSED || state == DeferredRfDecider::State::FAILED;
        }
        if (all_refused) {
            _deferral_refused = true;
            disable();
        } else if (any_built) {
            // The rows that are still in the operators were looked up without the filter, so the first window with
            // the filter starts when they are gone.
            _phase = _outstanding == 0 ? Phase::INITIAL_ON : Phase::AFTER_BUILD;
            reset_window();
        }
    }

    void finish_deferred_window() {
        DeferredRfWindow window;
        window.chunks = _chunks;
        window.rows = _rows;
        window.lookup_ns = _lookup_ns;
        window.intermediate_ns = _intermediate_ns;
        window.scan_ns = _scan_ns;
        window.probe_output_rows = _output_rows;
        if (_filler) _filler(&window);
        _last_window = window;
        _has_last_window = true;
        bool input_changed = false;
        for (const auto& decider : _deciders) {
            (void)decider->submit(window);
            input_changed |= decider->input_changed();
        }
        reset_window();
        apply_deferred_states();
        // A window ends with a pause of the scan and a short chunk, so while the filter waits we space the windows as
        // the windows of the adaptive filters are: they wait longer while the gain stays the same, and go back to
        // the shortest wait when it changes.
        if (_phase == Phase::DEFERRED) {
            const bool first_window = _deferred_chunks == kWindowChunks;
            _deferred_chunks =
                    input_changed || first_window ? kInitialInterval : std::min(_deferred_chunks * 4, kMaxInterval);
        }
    }

    // The queues of the scan are read for a few counters, which costs far less than a chunk, so we look at every chunk.
    void retry_when_ranges_changed() {
        if (!_has_last_window || !_filler) return;
        int64_t version = 0;
        if (_ranges_version) {
            version = _ranges_version();
        } else {
            DeferredRfWindow progress;
            _filler(&progress);
            version = progress.ranges_version;
        }
        if (version != _last_window.ranges_version) request_retry();
    }

    void maybe_finish_window() {
        if (!needs_drain() || _outstanding != 0) return;
        if (_phase == Phase::AFTER_BUILD) {
            _phase = Phase::INITIAL_ON;
            reset_window();
            return;
        }
        if (_phase == Phase::DEFERRED) {
            finish_deferred_window();
            return;
        }
        const double downstream = static_cast<double>(_lookup_ns) + _intermediate_ns;
        const double cost = (_filter_ns + downstream) / _rows;
        const bool was_on = use_filter();
        switch (_phase) {
        case Phase::INITIAL_ON: {
            _on_cost = cost;
            const double estimated_off = _passed > 0 ? downstream / _passed : 0;
            const bool try_off = _passed > 0 && (_filter_ns > downstream || cost > estimated_off * 1.10);
            _phase = try_off ? Phase::INITIAL_OFF : Phase::WAIT;
            _totals.decisions += !try_off;
            break;
        }
        case Phase::INITIAL_OFF:
            _preferred_on = !(cost < _on_cost * 0.90);
            _phase = Phase::WAIT;
            ++_totals.decisions;
            break;
        case Phase::WAIT:
            _phase = Phase::MEASURE_ON;
            break;
        case Phase::MEASURE_ON:
            _on_cost = cost;
            _phase = Phase::MEASURE_OFF;
            break;
        case Phase::MEASURE_OFF: {
            bool next = _preferred_on;
            if (_on_cost < cost * 0.90) next = true;
            if (cost < _on_cost * 0.90) next = false;
            _interval = next == _preferred_on ? std::min(_interval * 4, kMaxInterval) : kInitialInterval;
            _preferred_on = next;
            _phase = Phase::WAIT;
            ++_totals.decisions;
            break;
        }
        case Phase::DEFERRED:
        case Phase::AFTER_BUILD:
            break;
        }
        _totals.switches += was_on != use_filter();
        reset_window();
    }

    bool _enabled = true;
    bool _preferred_on = true;
    Phase _phase = Phase::INITIAL_ON;
    int _interval = kInitialInterval;
    int _deferred_chunks = kWindowChunks;
    int _chunks = 0;
    int64_t _outstanding = 0;
    int64_t _rows = 0;
    int64_t _passed = 0;
    int64_t _filter_ns = 0;
    int64_t _lookup_ns = 0;
    int64_t _intermediate_ns = 0;
    int64_t _scan_ns = 0;
    int64_t _output_rows = 0;
    double _on_cost = 0;
    Totals _totals;

    DeferredResolver _resolver;
    WindowFiller _filler;
    RangesVersionReader _ranges_version;
    // The last window that was given to the filters, to decide again from it when the scan gets new scan ranges.
    DeferredRfWindow _last_window;
    bool _has_last_window = false;
    bool _resolved = false;
    bool _deferral_refused = false;
    bool _deferral_failed = false;
    bool _stopped = false;
    std::vector<std::shared_ptr<DeferredRfDecider>> _deciders;
};

} // namespace starrocks::pipeline
