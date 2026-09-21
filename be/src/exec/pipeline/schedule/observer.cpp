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

#include "exec/pipeline/schedule/observer.h"

#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/schedule/common.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
static bool is_any_ready(PipelineDriver* driver) {
    auto sink = driver->sink_operator();
    auto source = driver->source_operator();
    return sink->is_finished() || sink->need_input() || source->is_finished() || source->has_output();
}

static bool is_sink_ready(PipelineDriver* driver) {
    auto sink = driver->sink_operator();
    return sink->is_finished() || sink->need_input();
}

static bool is_source_ready(PipelineDriver* driver) {
    auto source = driver->source_operator();
    return source->is_finished() || source->has_output();
}

void PipelineObserver::_do_update(int event) {
    auto driver = _driver;
    auto token = driver->acquire_schedule_token();
    auto* event_scheduler = driver->fragment_ctx()->event_scheduler();
    bool ready = false;
    {
        // Fast-path checks and trace logging can allocate, free buffers or submit spill I/O.
        // Restore the previous tracker before publishing the driver: an executor may immediately
        // finish its fragment and destroy the runtime state and its memory counters.
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(driver->runtime_state()->instance_mem_tracker());
        auto sink = driver->sink_operator();
        auto source = driver->source_operator();

        if (!driver->is_finished() && !driver->pending_finish()) {
            TRACE_SCHEDULE_LOG << "notify driver:" << driver << " state:" << driver->driver_state()
                               << " event:" << event << " in_block_queue:" << driver->is_in_blocked()
                               << " source finished:" << source->is_finished()
                               << " operator has output:" << source->has_output()
                               << " sink finished:" << sink->is_finished() << " sink need input:" << sink->need_input()
                               << ":" << driver->to_readable_string();
        }

        if (driver->is_in_blocked()) {
            // Preserve the existing 4.1 scheduling condition; this patch scopes memory accounting.
            bool pipeline_block = driver->driver_state() != DriverState::INPUT_EMPTY ||
                                  driver->driver_state() != DriverState::OUTPUT_FULL;
            if (pipeline_block || _is_cancel_changed(event)) {
                ready = true;
            } else if (_is_all_changed(event)) {
                ready = is_any_ready(driver);
            } else if (_is_source_changed(event)) {
                ready = is_source_ready(driver);
            } else if (_is_sink_changed(event)) {
                ready = is_sink_ready(driver);
            } else {
                // nothing to do
            }
        } else {
            driver->set_need_check_reschedule(true);
        }
    }
    if (ready) {
        event_scheduler->try_schedule(driver);
    }
}

std::string Observable::to_string() const {
    std::string str;
    for (auto* observer : _observers) {
        str += observer->driver()->to_readable_string() + "\n";
    }
    return str;
}

void Observable::notify_runtime_filter_timeout() {
    for (auto* observer : _observers) {
        observer->driver()->set_all_global_rf_timeout();
        observer->source_trigger();
    }
}

} // namespace starrocks::pipeline