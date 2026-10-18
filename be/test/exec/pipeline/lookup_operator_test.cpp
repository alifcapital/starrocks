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

#include "exec/pipeline/lookup_operator.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_metrics.h"
#include "exec/pipeline/query_context.h"
#include "exec/workgroup/pipeline_executor_set.h"
#include "exec/workgroup/work_group.h"
#include "runtime/exec_env.h"
#include "runtime/lookup_stream_mgr.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {

// Records how the operator answers a request.
class RecordingRequestContext final : public LookUpRequestContext {
public:
    explicit RecordingRequestContext(TupleId tuple_id) : _tuple_id(tuple_id) {}

    TupleId request_tuple_id() const override { return _tuple_id; }
    Status collect_input_columns(ChunkPtr chunk) override { return Status::OK(); }
    StatusOr<size_t> fill_response(const ChunkPtr& result_chunk, SlotId source_id_slot,
                                   const std::vector<SlotDescriptor*>& slots, size_t start_offset) override {
        return start_offset;
    }
    void callback(const Status& status) override {
        num_callbacks++;
        last_status = status;
    }

    int num_callbacks = 0;
    Status last_status;

private:
    TupleId _tuple_id;
};

class LookUpOperatorTest : public ::testing::Test {
protected:
    static constexpr TupleId kRequestTupleId = 7;

    void SetUp() override {
        _state.set_query_ctx(_query_ctx.get());
        // The observer of a driver reads the runtime state and the event scheduler of the fragment context before it
        // checks whether the driver is ready, so a driver needs a fragment context with a runtime state. The fixture
        // owns the runtime state, so the shared_ptr must not delete it.
        _state.init_instance_mem_tracker();
        _state.set_fragment_ctx(&_fragment_ctx);
        _fragment_ctx.set_runtime_state(std::shared_ptr<RuntimeState>(&_state, [](RuntimeState*) {}));
        _row_pos_descs.emplace(kRequestTupleId, &_row_pos_desc);
        _dispatcher = std::make_shared<LookUpDispatcher>(TUniqueId(), 1, std::vector<TupleId>{kRequestTupleId});
        _factory = std::make_unique<LookUpOperatorFactory>(1, 1, _row_pos_descs, _dispatcher, 1);
    }

    void TearDown() override {
        if (_op != nullptr) {
            _op->close(&_state);
        }
    }

    // The IO task's completion callback calls defer_notify() from a scan thread; call it directly here.
    void notify_from_io_task() { down_cast<LookUpOperator*>(_op.get())->defer_notify(); }

    std::shared_ptr<QueryContext> _query_ctx = std::make_shared<QueryContext>();
    RuntimeState _state;
    FragmentContext _fragment_ctx;
    IcebergV3RowPositionDescriptor _row_pos_desc{1, {}, {}};
    phmap::flat_hash_map<TupleId, RowPositionDescriptor*> _row_pos_descs;
    std::shared_ptr<LookUpDispatcher> _dispatcher;
    std::unique_ptr<LookUpOperatorFactory> _factory;
    OperatorPtr _op;
    std::unique_ptr<PipelineDriver> _driver;
};

// Under the poll scheduler (e.g. exec_mode='etl' enables wait-dependent events), drivers never get an observer.
// Finishing an IO task used to dereference the null observer and crash the BE.
TEST_F(LookUpOperatorTest, io_task_notify_without_event_scheduler) {
    _state.set_enable_event_scheduler(false);
    _op = _factory->create(1, 0);
    ASSERT_EQ(nullptr, _op->observer());
    ASSERT_OK(_op->prepare(&_state));

    notify_from_io_task();
}

// Under the event scheduler the finished IO task must still wake the driver, or the lookup source hangs.
TEST_F(LookUpOperatorTest, io_task_notify_wakes_driver_with_event_scheduler) {
    _state.set_enable_event_scheduler(true);
    _op = _factory->create(1, 0);
    // In 4.1 the observer is concrete. Verify its reschedule signal on a real, unblocked driver.
    _driver = std::make_unique<PipelineDriver>(Operators{_op}, _query_ctx.get(), &_fragment_ctx, nullptr, -1);
    _op->set_observer(_driver->observer());
    ASSERT_OK(_op->prepare(&_state));

    ASSERT_FALSE(_driver->need_check_reschedule());
    notify_from_io_task();
    EXPECT_TRUE(_driver->need_check_reschedule());
}

// The scan executor of the workgroup is closed, so it rejects the IO task of the operator.
class LookUpOperatorRejectedSubmitTest : public LookUpOperatorTest {
protected:
    void SetUp() override {
        LookUpOperatorTest::SetUp();
        _metrics = std::make_unique<PipelineExecutorMetrics>();
        _executor_conf = std::make_unique<workgroup::PipelineExecutorSetConfig>(1, 1, 1, 1, CpuUtil::CpuIds{}, false,
                                                                                false, _metrics.get());
        _executors = std::make_unique<workgroup::PipelineExecutorSet>(
                *_executor_conf, "lookup_rejected", CpuUtil::CpuIds{}, std::vector<CpuUtil::CpuIds>{});
        ASSERT_OK(_executors->start());
        _executors->close();

        auto parent = GlobalEnv::GetInstance()->query_pool_mem_tracker_shared();
        _workgroup = std::make_shared<workgroup::WorkGroup>(
                "lookup_wg", workgroup::WorkGroup::DEFAULT_WG_ID, workgroup::WorkGroup::DEFAULT_VERSION, 4, 100.0, 0,
                1.0, workgroup::WorkGroupType::WG_DEFAULT, workgroup::WorkGroup::DEFAULT_MEM_POOL);
        _workgroup->init(parent);
        _workgroup->set_shared_executors(_executors.get());
        _fragment_ctx.set_workgroup(_workgroup);
    }

    void TearDown() override {
        LookUpOperatorTest::TearDown();
        // The fragment context of the base fixture outlives the executors, which the workgroup points to.
        _fragment_ctx.set_workgroup(nullptr);
    }

    // The members are destroyed in the reverse order: the executors need the config and the metrics until the end.
    std::unique_ptr<PipelineExecutorMetrics> _metrics;
    std::unique_ptr<workgroup::PipelineExecutorSetConfig> _executor_conf;
    std::unique_ptr<workgroup::PipelineExecutorSet> _executors;
    workgroup::WorkGroupPtr _workgroup;
};

// The operator took the requests from the dispatcher before it submitted the IO task. A rejected task never runs, so
// the operator has to answer the requests, clear the running state and fail by itself. Otherwise the requester and
// the operator wait forever.
TEST_F(LookUpOperatorRejectedSubmitTest, rejected_io_task_fails_the_requests_and_the_operator) {
    _state.set_enable_event_scheduler(false);
    _op = _factory->create(1, 0);
    ASSERT_OK(_op->prepare(&_state));

    auto request = std::make_shared<RecordingRequestContext>(kRequestTupleId);
    ASSERT_OK(_dispatcher->add_request(request));
    ASSERT_TRUE(_op->has_output());

    auto chunk = _op->pull_chunk(&_state);
    ASSERT_FALSE(chunk.ok());
    ASSERT_TRUE(chunk.status().is_service_unavailable()) << chunk.status();
    ASSERT_EQ(1, request->num_callbacks);
    ASSERT_TRUE(request->last_status.is_service_unavailable()) << request->last_status;

    // The failed operator reports the error again, and it can finish: no IO task is counted as running.
    ASSERT_FALSE(_op->pull_chunk(&_state).ok());
    ASSERT_EQ(1, request->num_callbacks);
    ASSERT_OK(_op->set_finishing(&_state));
    ASSERT_TRUE(_op->is_finished());
}

} // namespace starrocks::pipeline
