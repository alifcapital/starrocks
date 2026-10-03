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

namespace starrocks::pipeline {

// One scan and its consuming JOIN share this state within a pipeline driver.
// A measurement ends only after every emitted row has completed its lookup.
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

    bool enabled() const { return _enabled; }
    bool use_filter() const {
        return !_enabled || _phase == Phase::INITIAL_ON || _phase == Phase::MEASURE_ON ||
               (_phase == Phase::WAIT && _preferred_on);
    }
    bool needs_drain() const { return _enabled && _chunks >= window_chunks() && _rows >= kMinRows; }
    int64_t outstanding_rows() const { return _outstanding; }
    int recheck_interval() const { return _interval; }
    const Totals& totals() const { return _totals; }
    void disable() { _enabled = false; }

    void observe_filter(int64_t input_rows, int64_t output_rows, int64_t filter_ns) {
        if (!_enabled) return;
        if (input_rows < 0 || output_rows < 0 || output_rows > input_rows || filter_ns < 0) {
            disable();
            return;
        }
        const bool on = use_filter();
        _totals.input_rows[on] += input_rows;
        _totals.filter_ns[on] += filter_ns;
        _outstanding += output_rows;
        _rows += input_rows;
        _passed += output_rows;
        _filter_ns += filter_ns;
        ++_chunks;
        maybe_finish_window();
    }

    void observe_lookup(int64_t completed_rows, int64_t lookup_ns, int64_t intermediate_ns = 0) {
        if (!_enabled) return;
        if (completed_rows < 0 || completed_rows > _outstanding || lookup_ns < 0 || intermediate_ns < 0) {
            disable();
            return;
        }
        _outstanding -= completed_rows;
        _lookup_ns += lookup_ns;
        _intermediate_ns += intermediate_ns;
        _totals.lookup_ns[use_filter()] += lookup_ns;
        _totals.intermediate_ns[use_filter()] += intermediate_ns;
        maybe_finish_window();
    }

private:
    enum class Phase { INITIAL_ON, INITIAL_OFF, WAIT, MEASURE_ON, MEASURE_OFF };
    static constexpr int kWindowChunks = 64;
    static constexpr int64_t kMinRows = 131072;
    static constexpr int kInitialInterval = 256;
    static constexpr int kMaxInterval = 16384;
    int window_chunks() const { return _phase == Phase::WAIT ? _interval : kWindowChunks; }

    void maybe_finish_window() {
        if (!needs_drain() || _outstanding != 0) return;
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
        }
        _totals.switches += was_on != use_filter();
        _chunks = 0;
        _rows = _passed = _filter_ns = _lookup_ns = _intermediate_ns = 0;
    }

    bool _enabled = true;
    bool _preferred_on = true;
    Phase _phase = Phase::INITIAL_ON;
    int _interval = kInitialInterval;
    int _chunks = 0;
    int64_t _outstanding = 0;
    int64_t _rows = 0;
    int64_t _passed = 0;
    int64_t _filter_ns = 0;
    int64_t _lookup_ns = 0;
    int64_t _intermediate_ns = 0;
    double _on_cost = 0;
    Totals _totals;
};

} // namespace starrocks::pipeline
