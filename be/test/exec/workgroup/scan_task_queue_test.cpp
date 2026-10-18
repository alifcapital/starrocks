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

#include "exec/workgroup/scan_task_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/pipeline_metrics.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/work_group.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"

namespace starrocks::workgroup {

// The executor has one worker thread, so the thread a task runs on tells which executor ran it.
static std::unique_ptr<ScanExecutor> new_single_thread_executor(
        const std::string& name, pipeline::ScanExecutorMetrics* metrics,
        std::unique_ptr<ScanTaskQueue> queue = std::make_unique<PriorityScanTaskQueue>(100)) {
    std::unique_ptr<ThreadPool> thread_pool;
    CHECK_OK(ThreadPoolBuilder(name).set_min_threads(0).set_max_threads(1).set_max_queue_size(100).build(&thread_pool));
    auto executor = std::make_unique<ScanExecutor>(std::move(thread_pool), std::move(queue), metrics);
    executor->initialize(1);
    return executor;
}

static std::thread::id worker_thread_id(ScanExecutor* executor) {
    std::promise<std::thread::id> id;
    CHECK(executor->submit(ScanTask([&](auto&) { id.set_value(std::this_thread::get_id()); })));
    return id.get_future().get();
}

PARALLEL_TEST(ScanExecutorTest, test_yield_function_resubmits_to_other_executor) {
    pipeline::ScanExecutorMetrics local_metrics;
    pipeline::ScanExecutorMetrics remote_metrics;
    auto local = new_single_thread_executor("scan_yield_local", &local_metrics);
    auto remote = new_single_thread_executor("scan_yield_remote", &remote_metrics);
    DeferOp op([&]() {
        local->close();
        remote->close();
    });
    auto local_tid = worker_thread_id(local.get());
    auto remote_tid = worker_thread_id(remote.get());
    ASSERT_NE(local_tid, remote_tid);

    // The task yields once. Its yield function submits it to the remote executor, so the second run is there.
    std::vector<std::thread::id> run_on;
    std::promise<void> done;
    ScanTask task(
            nullptr,
            [&](auto& ctx) {
                ctx.total_yield_point_cnt = 2;
                run_on.push_back(std::this_thread::get_id());
                if (++ctx.yield_point == 2) {
                    done.set_value();
                }
            },
            [&](ScanTask&& yielded) { remote->force_submit(std::move(yielded)); });

    ASSERT_TRUE(local->submit(std::move(task)));
    done.get_future().get();

    ASSERT_EQ(2, run_on.size());
    ASSERT_EQ(local_tid, run_on[0]);
    ASSERT_EQ(remote_tid, run_on[1]);
    // The yielded run is accounted once, in the executor it ran in, and the task is pending in no executor. The
    // local worker thread ran the probe task and the first run of the task, and it finished the probe accounting
    // before it took the task.
    ASSERT_EQ(2, local_metrics.finished_tasks.value());
    ASSERT_EQ(0, local_metrics.running_tasks.value());
    ASSERT_EQ(0, local_metrics.pending_tasks.value());
    ASSERT_EQ(0, remote_metrics.pending_tasks.value());
}

PARALLEL_TEST(ScanExecutorTest, test_yield_function_resubmits_to_same_executor) {
    pipeline::ScanExecutorMetrics metrics;
    auto executor = new_single_thread_executor("scan_yield_same", &metrics);
    DeferOp op([&]() { executor->close(); });
    auto tid = worker_thread_id(executor.get());

    // The yield function puts the task back into the executor it ran in. We expect every yield to be handed to the
    // function exactly once and counted as pending exactly once.
    std::atomic_int yield_function_calls = 0;
    std::vector<std::thread::id> run_on;
    std::promise<void> done;
    ScanTask task(
            nullptr,
            [&](auto& ctx) {
                ctx.total_yield_point_cnt = 3;
                run_on.push_back(std::this_thread::get_id());
                if (++ctx.yield_point == 3) {
                    done.set_value();
                }
            },
            [&](ScanTask&& yielded) {
                yield_function_calls++;
                executor->force_submit(std::move(yielded));
            });

    ASSERT_TRUE(executor->submit(std::move(task)));
    done.get_future().get();

    ASSERT_EQ(2, yield_function_calls.load());
    ASSERT_EQ(3, run_on.size());
    for (const auto& run_tid : run_on) {
        ASSERT_EQ(tid, run_tid);
    }
    ASSERT_EQ(0, metrics.pending_tasks.value());
}

PARALLEL_TEST(ScanExecutorTest, test_yield_without_yield_function_stays_in_executor) {
    pipeline::ScanExecutorMetrics local_metrics;
    pipeline::ScanExecutorMetrics remote_metrics;
    auto local = new_single_thread_executor("scan_yield_local", &local_metrics);
    auto remote = new_single_thread_executor("scan_yield_remote", &remote_metrics);
    DeferOp op([&]() {
        local->close();
        remote->close();
    });
    auto local_tid = worker_thread_id(local.get());
    auto remote_tid = worker_thread_id(remote.get());
    ASSERT_NE(local_tid, remote_tid);

    // Nothing tells where a task without a yield function goes next, so it is queued again in the same executor.
    std::vector<std::thread::id> run_on;
    std::promise<void> done;
    ScanTask task(nullptr, [&](auto& ctx) {
        ctx.total_yield_point_cnt = 3;
        run_on.push_back(std::this_thread::get_id());
        if (++ctx.yield_point == 3) {
            done.set_value();
        }
    });
    ASSERT_FALSE(task.has_yield_function());

    ASSERT_TRUE(local->submit(std::move(task)));
    done.get_future().get();

    ASSERT_EQ(3, run_on.size());
    for (const auto& run_tid : run_on) {
        ASSERT_EQ(local_tid, run_tid);
    }
    // Taking the task decreased the pending counter of the local executor, so every queueing again counts it.
    ASSERT_EQ(0, local_metrics.pending_tasks.value());
    ASSERT_EQ(0, remote_metrics.pending_tasks.value());
}

namespace {

// Its destructor tells when the last owner of the yield function's closure is gone.
struct YieldCapture {
    explicit YieldCapture(std::atomic<bool>* destroyed) : destroyed(destroyed) {}
    ~YieldCapture() { destroyed->store(true); }

    std::string text = std::string(64, 'y');
    std::atomic<bool>* destroyed;
};

} // namespace

PARALLEL_TEST(ScanExecutorTest, test_yield_function_reads_captures_after_handing_over_task) {
    pipeline::ScanExecutorMetrics local_metrics;
    pipeline::ScanExecutorMetrics remote_metrics;
    std::atomic<bool> capture_destroyed = false;
    std::string text_read;
    bool task_destroyed_first = false;
    std::promise<void> yield_returned;
    auto local = new_single_thread_executor("scan_yield_local", &local_metrics);
    auto remote = new_single_thread_executor("scan_yield_remote", &remote_metrics);
    DeferOp op([&]() {
        local->close();
        remote->close();
    });

    // The remote executor finishes the task and destroys it, with its yield function, while the yield function
    // still runs in the local executor. The closure of the yield function is on the heap, so we expect the call to
    // run a copy of the function: the capture must stay alive until the call returns.
    auto sentinel = std::make_shared<int>(0);
    std::weak_ptr<int> sentinel_ref = sentinel;
    auto capture = std::make_shared<YieldCapture>(&capture_destroyed);
    ScanTask task(
            nullptr,
            [sentinel](auto& ctx) {
                ctx.total_yield_point_cnt = 2;
                ++ctx.yield_point;
            },
            [capture, sentinel_ref, target = remote.get(), destroyed = &capture_destroyed, text_out = &text_read,
             task_destroyed_out = &task_destroyed_first, returned = &yield_returned](ScanTask&& yielded) {
                // The closure can be freed during the hand-over if the call does not run a copy. We read what we
                // need from it first, so that this shows in the assertions below and not as a crash.
                auto* destroyed_flag = destroyed;
                auto* text_dest = text_out;
                auto* task_destroyed_dest = task_destroyed_out;
                auto* returned_promise = returned;
                auto sentinel_watch = sentinel_ref;
                target->force_submit(std::move(yielded));
                auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
                while (!sentinel_watch.expired() && std::chrono::steady_clock::now() < deadline) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                *task_destroyed_dest = sentinel_watch.expired();
                if (!destroyed_flag->load()) {
                    *text_dest = capture->text;
                }
                returned_promise->set_value();
            });
    // The task owns the only references now.
    sentinel.reset();
    capture.reset();

    ASSERT_TRUE(local->submit(std::move(task)));
    yield_returned.get_future().get();

    ASSERT_TRUE(task_destroyed_first);
    ASSERT_EQ(std::string(64, 'y'), text_read);
}

PARALLEL_TEST(ScanExecutorTest, test_submit_to_closed_executor) {
    pipeline::ScanExecutorMetrics local_metrics;
    pipeline::ScanExecutorMetrics remote_metrics;
    auto local = new_single_thread_executor("scan_yield_local", &local_metrics);
    auto remote = new_single_thread_executor("scan_yield_remote", &remote_metrics);
    DeferOp op([&]() {
        local->close();
        remote->close();
    });
    auto local_tid = worker_thread_id(local.get());

    // The target executor is closed before the local one, as ExecEnv does when it releases resources before a core
    // dump.
    remote->close();

    // A closed executor rejects the task or drops it, and it destroys the task without running it.
    std::atomic_int runs = 0;
    auto submit_sentinel = std::make_shared<int>(0);
    std::weak_ptr<int> submit_ref = submit_sentinel;
    ASSERT_FALSE(remote->submit(ScanTask([&runs, submit_sentinel](auto&) { runs++; })));
    submit_sentinel.reset();
    ASSERT_TRUE(submit_ref.expired());

    auto force_sentinel = std::make_shared<int>(0);
    std::weak_ptr<int> force_ref = force_sentinel;
    remote->force_submit(ScanTask([&runs, force_sentinel](auto&) { runs++; }));
    force_sentinel.reset();
    ASSERT_TRUE(force_ref.expired());

    // A worker of the local executor yields a task into the closed executor. The worker must go on with its queue.
    std::promise<void> yielded_into_closed;
    ScanTask task(
            nullptr,
            [&](auto& ctx) {
                ctx.total_yield_point_cnt = 2;
                runs++;
                ++ctx.yield_point;
            },
            [&](ScanTask&& yielded) {
                remote->force_submit(std::move(yielded));
                yielded_into_closed.set_value();
            });
    ASSERT_TRUE(local->submit(std::move(task)));
    yielded_into_closed.get_future().get();

    ASSERT_EQ(local_tid, worker_thread_id(local.get()));
    ASSERT_EQ(1, runs.load());
    ASSERT_EQ(0, remote_metrics.pending_tasks.value());
}

// A closed queue refuses every task, whatever the queue is. The tasks have no workgroup: a closed
// WorkGroupScanTaskQueue must refuse before it looks at the workgroup of the task.
PARALLEL_TEST(ScanTaskQueueTest, test_closed_queue_refuses_tasks) {
    std::vector<std::unique_ptr<ScanTaskQueue>> queues;
    queues.emplace_back(std::make_unique<PriorityScanTaskQueue>(100));
    queues.emplace_back(std::make_unique<WorkGroupScanTaskQueue>(ScanSchedEntityType::OLAP));
    queues.emplace_back(std::make_unique<WorkGroupScanTaskQueue>(ScanSchedEntityType::CONNECTOR));
    for (auto& queue : queues) {
        queue->close();
        ASSERT_FALSE(queue->try_offer(ScanTask([](auto&) {})));
        ASSERT_FALSE(queue->force_put(ScanTask([](auto&) {})));
        ASSERT_EQ(0, queue->size());
        ASSERT_TRUE(queue->take().status().is_cancelled());
    }
}

// The executor of the production queue type: the tasks are refused after close(), and nothing is counted as pending.
PARALLEL_TEST(ScanExecutorTest, test_work_group_queue_executor_refuses_tasks_after_close) {
    pipeline::ScanExecutorMetrics metrics;
    auto executor = new_single_thread_executor("scan_wg_queue", &metrics,
                                               std::make_unique<WorkGroupScanTaskQueue>(ScanSchedEntityType::OLAP));
    executor->close();

    auto sentinel = std::make_shared<int>(0);
    std::weak_ptr<int> sentinel_ref = sentinel;
    ASSERT_FALSE(executor->submit(ScanTask([sentinel](auto&) {})));
    executor->force_submit(ScanTask([sentinel](auto&) {}));
    sentinel.reset();
    ASSERT_TRUE(sentinel_ref.expired());
    ASSERT_EQ(0, executor->num_tasks());
    ASSERT_EQ(0, metrics.pending_tasks.value());
}

// The worker threads leave take() without a task when the queue closes. That must not count as a taken task.
PARALLEL_TEST(ScanExecutorTest, test_close_keeps_pending_tasks_balanced) {
    pipeline::ScanExecutorMetrics metrics;
    auto executor = new_single_thread_executor("scan_close", &metrics);
    worker_thread_id(executor.get());
    executor->close();
    ASSERT_EQ(0, metrics.pending_tasks.value());
}

PARALLEL_TEST(ScanExecutorTest, test_yield) {
    auto queue = std::make_unique<PriorityScanTaskQueue>(100);
    std::unique_ptr<ThreadPool> thread_pool;
    ASSERT_OK(ThreadPoolBuilder("scan_yield")
                      .set_min_threads(0)
                      .set_max_threads(4)
                      .set_max_queue_size(100)
                      .build(&thread_pool));
    pipeline::ScanExecutorMetrics metrics;
    auto executor = std::make_unique<ScanExecutor>(std::move(thread_pool), std::move(queue), &metrics);
    DeferOp op([&]() { executor->close(); });
    executor->initialize(4);

    std::promise<int> a;
    std::string res;
    ScanTask scan_task([&](auto& ctx) {
        ctx.total_yield_point_cnt = 4;
        DCHECK_LT(ctx.yield_point, ctx.total_yield_point_cnt);
        switch (ctx.yield_point) {
        case 0:
            ctx.yield_point++;
            res += "0";
            return;
        case 1:
            ctx.yield_point++;
            res += "1";
            return;
        case 2:
            ctx.yield_point++;
            res += "2";
            return;
        case 3:
            ctx.yield_point++;
            res += "3";
            a.set_value(1);
            return;
        }
    });

    ASSERT_TRUE(executor->submit(std::move(scan_task)));
    a.get_future().get();
    ASSERT_EQ(res, "0123");

    // test overloaded
    std::atomic_int finished_tasks = 0;
    size_t submit_tasks = 0;
    std::mutex mutex;
    std::condition_variable cv;
    for (size_t i = 0; i < 100; ++i) {
        ScanTask overload_task([&](auto& ctx) {
            ctx.total_yield_point_cnt = 2;
            DCHECK_LT(ctx.yield_point, ctx.total_yield_point_cnt);
            if (ctx.yield_point == 1) {
                std::lock_guard guard(mutex);
                finished_tasks++;
                cv.notify_one();
            }
            ctx.yield_point++;
        });
        submit_tasks += executor->submit(std::move(overload_task));
    }
    std::unique_lock lock(mutex);
    cv.wait(lock, [&]() { return submit_tasks == finished_tasks.load(); });
    ASSERT_EQ(submit_tasks, finished_tasks.load());
}

} // namespace starrocks::workgroup
