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

#include "exec/workgroup/scan_executor.h"

#include <algorithm>

#include "exec/pipeline/pipeline_metrics.h"
#include "exec/workgroup/scan_task_queue.h"
#include "runtime/current_thread.h"
#include "util/starrocks_metrics.h"

namespace starrocks::workgroup {

ScanExecutor::ScanExecutor(std::unique_ptr<ThreadPool> thread_pool, std::unique_ptr<ScanTaskQueue> task_queue,
                           pipeline::ScanExecutorMetrics* metrics)
        : _task_queue(std::move(task_queue)), _thread_pool(std::move(thread_pool)), _metrics(metrics) {
    _metrics->thread_pool.monitor(this, [this] {
        // The target follows configuration changes right away, whereas the pool was sized once at
        // startup; workers submitted beyond that size only queue up and never become live threads,
        // so the target is capped by it.
        const int32_t expected = std::min(_num_threads_setter.expect_num(), _thread_pool->max_threads());
        return pipeline::ScanThreadPoolMetrics::Sample{static_cast<uint64_t>(std::max(0, expected)),
                                                       static_cast<uint64_t>(std::max(0, _thread_pool->num_threads()))};
    });
}

ScanExecutor::~ScanExecutor() {
    // The executors of a resource group with exclusive ones are destroyed when the group goes
    // away, while the metrics instance is process-wide, so the pool must not be left behind in it.
    _stop_monitoring();
}

void ScanExecutor::close() {
    _task_queue->close();
    _thread_pool->shutdown();
    _stop_monitoring();
}

// _metrics stays set: submit and force_submit still update it after close().
void ScanExecutor::_stop_monitoring() {
    if (_monitoring.exchange(false)) {
        _metrics->thread_pool.unmonitor(this);
    }
}

void ScanExecutor::initialize(int num_threads) {
    _num_threads_setter.set_actual_num(num_threads);
    for (auto i = 0; i < num_threads; ++i) {
        (void)_thread_pool->submit_func([this]() { this->worker_thread(); });
    }
}

void ScanExecutor::change_num_threads(int32_t num_threads) {
    int32_t old_num_threads = 0;
    if (!_num_threads_setter.adjust_expect_num(num_threads, &old_num_threads)) {
        return;
    }
    for (int i = old_num_threads; i < num_threads; ++i) {
        if (_num_threads_setter.should_expand()) {
            (void)_thread_pool->submit_func([this]() { this->worker_thread(); });
        }
    }
}

void ScanExecutor::worker_thread() {
    // This executor is dedicated to query scan/IO tasks.
    SET_MODULE_TYPE(ThreadModuleType::QUERY);
    auto* current_thread = Thread::current_thread();
    while (true) {
        if (_num_threads_setter.should_shrink()) {
            break;
        }

        if (current_thread != nullptr) {
            current_thread->set_idle(true);
        }
        auto maybe_task = _task_queue->take();
        if (current_thread != nullptr) {
            current_thread->set_idle(false);
        }
        // A cancelled take returns no task, and only a task that submit or force_submit counted may be uncounted
        // here. Otherwise the pending counter drops below zero once for every worker thread when the queue closes.
        if (maybe_task.status().is_cancelled()) {
            return;
        }
        _metrics->pending_tasks.increment(-1);
        auto& task = maybe_task.value();

        _metrics->running_tasks.increment(1);
        int64_t time_spent_ns = 0;
        {
            SCOPED_RAW_TIMER(&time_spent_ns);
            task.run();
        }
        if (current_thread != nullptr) {
            current_thread->inc_finished_tasks();
        }
        _task_queue->update_statistics(task, time_spent_ns);
        _metrics->running_tasks.increment(-1);
        _metrics->finished_tasks.increment(1);
        _metrics->execution_time.increment(task.query_type(), time_spent_ns);

        // The task yielded. A task with a yield function may need another executor to go on (a spill IO task moves
        // between the local and the remote IO executor), so we give it to that function. A task without one is
        // queued again in this executor. We use force_submit for it, because taking the task decreased the pending
        // counter and we fear the counter drifts below zero if the task is not counted again.
        if (!task.is_finished()) {
            if (task.has_yield_function()) {
                task.execute_yield_function();
            } else {
                force_submit(std::move(task));
            }
        }
    }
}

bool ScanExecutor::submit(ScanTask task) {
    // The queue of a closed executor refuses the task, so we return false and the caller handles it like a full
    // queue.
    bool ret = _task_queue->try_offer(std::move(task));
    _metrics->pending_tasks.increment(ret);
    return ret;
}

void ScanExecutor::force_submit(ScanTask task) {
    // A worker of another executor can yield a task into this executor after it was closed. The queue refuses the
    // task, which cannot run here, and force_submit has no way to tell its owner, so we log it and count nothing.
    // We expect a closed executor only while the BE shuts down, where the tasks already queued in the executor are
    // dropped as well.
    if (!_task_queue->force_put(std::move(task))) {
        LOG(WARNING) << "drop a scan task, the scan executor is closed";
        return;
    }
    _metrics->pending_tasks.increment(1);
}

void ScanExecutor::bind_cpus(const CpuUtil::CpuIds& cpuids, const std::vector<CpuUtil::CpuIds>& borrowed_cpuids) {
    _thread_pool->bind_cpus(cpuids, borrowed_cpuids);
}

int64_t ScanExecutor::num_tasks() const {
    return _task_queue->size();
}

} // namespace starrocks::workgroup
