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
#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>

namespace starrocks::pipeline {
using std::chrono::milliseconds;
using std::chrono::steady_clock;
class TopnRfBackPressure {
    enum Phase { PH_UNTHROTTLE, PH_THROTTLE, PH_PASS_THROUGH };

    template <typename T>
    class ScaleGenerator {
    public:
        ScaleGenerator(T initial_value, T delta, double factor, std::function<bool(T)> next_cb)
                : initial_value(initial_value), delta(delta), factor(factor), next_cb(next_cb), value(initial_value) {}

        T limit() { return value; }
        void next() {
            value += delta;
            value *= factor;
        }
        bool has_next() { return next_cb(value); }

    private:
        const T initial_value;
        const T delta;
        const double factor;
        const std::function<bool(T)> next_cb;
        T value;
    };

public:
    void update_selectivity(double selectivity) { _current_selectivity = selectivity; }
    void inc_num_rows(size_t num_rows) { _current_num_rows += num_rows; }

    // Latched once the TopN runtime filter has actually arrived at the scan. The sole purpose of
    // throttling is to wait for that arrival; once the filter is present (and prunes at storage)
    // there is nothing left to wait for, so release regardless of observed per-chunk selectivity --
    // which goes stale when storage-level zonemap pruning makes the pulled chunks empty.
    void notify_rf_arrived() { _rf_arrived = true; }

    // Steady-clock deadline (ms since epoch) of the current throttle window while in PH_THROTTLE,
    // or -1 otherwise. The scan operator arms an event-scheduler timer at this deadline so the driver
    // is woken when the window ends, instead of relying on the fallback poller.
    int64_t current_throttle_deadline() const { return _phase == PH_THROTTLE ? _current_throttle_deadline : -1; }

    // The scan calls this when rows first reach it. From then on the TopN above can build its filter,
    // and the scan waits for it: it throttles and limits its IO tasks. The round and throttle-time
    // budgets advance only after enough rows pass the scan. We fear a selective scan that never lets
    // enough rows pass: the TopN never builds the filter, and the scan reads the whole table with
    // limited IO. So we stop waiting when the filter has not arrived within _throttle_time_upper_bound
    // ms after the first call.
    void start_wait() {
        if (_wait_start_ms < 0) {
            _wait_start_ms = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        }
    }

    // True once back-pressure has permanently stopped throttling -- either the RF arrived, or it did
    // not arrive before the round/time/row budget was exhausted or the wait passed its time bound.
    // Read-only: unlike should_throttle() it does not advance the state machine. Used to release the
    // scan IO-task clamp when there is nothing left to wait for, even though no RF materialized.
    bool is_pass_through() const { return _phase == PH_PASS_THROUGH || _wait_expired(); }

    bool should_throttle() {
        if (_phase == PH_PASS_THROUGH) {
            return false;
        } else if (_rf_arrived || !_round_limiter.has_next() || !_throttle_time_limiter.has_next() ||
                   !_num_rows_limiter.has_next() || _current_selectivity <= _selectivity_lower_bound ||
                   _current_total_throttle_time >= _throttle_time_upper_bound || _wait_expired()) {
            _phase = PH_PASS_THROUGH;
            return false;
        }

        if (_phase == PH_UNTHROTTLE) {
            if (_current_num_rows <= _num_rows_limiter.limit()) {
                return false;
            }
            _phase = PH_THROTTLE;
            _current_throttle_deadline = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count() +
                                         _throttle_time_limiter.limit();
            return true;
        } else {
            auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
            if (now < _current_throttle_deadline) {
                return true;
            }
            _phase = PH_UNTHROTTLE;
            _current_num_rows = 0;
            _current_total_throttle_time += _throttle_time_limiter.limit();
            _round_limiter.next();
            _throttle_time_limiter.next();
            _num_rows_limiter.next();
            return false;
        }
    }

    TopnRfBackPressure(double selectivity_lower_bound, int64_t throttle_time_upper_bound, int max_rounds,
                       int64_t throttle_time, size_t num_rows)
            : _selectivity_lower_bound(selectivity_lower_bound),
              _throttle_time_upper_bound(throttle_time_upper_bound),
              _round_limiter(0, 1, 1.0, [max_rounds](int r) { return r < max_rounds; }),
              _throttle_time_limiter(throttle_time, 0, 2.0, [](int64_t) { return true; }),
              _num_rows_limiter(num_rows, 0, 2.0, [](size_t n) { return n < std::numeric_limits<size_t>::max() / 2; }) {
    }

private:
    // We expect users to set the bound to the maximum value to disable it. start + bound would
    // overflow then, so we keep the start time and compare the elapsed time.
    bool _wait_expired() const {
        if (_wait_start_ms < 0) {
            return false;
        }
        const int64_t now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return now - _wait_start_ms >= _throttle_time_upper_bound;
    }

    const double _selectivity_lower_bound;
    const int64_t _throttle_time_upper_bound;
    Phase _phase{PH_UNTHROTTLE};
    ScaleGenerator<int> _round_limiter;
    ScaleGenerator<int64_t> _throttle_time_limiter;
    ScaleGenerator<int64_t> _num_rows_limiter;
    int64_t _current_throttle_deadline{-1};
    int64_t _current_total_throttle_time{0};
    size_t _current_num_rows{0};
    double _current_selectivity{1.0};
    bool _rf_arrived{false};
    int64_t _wait_start_ms{-1};
};

} // namespace starrocks::pipeline
