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

#include "exec/pipeline/scan/olap_scan_operator.h"

#include <chrono>
#include <thread>

#include "exec/olap_scan_node.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/pipeline/scan/olap_chunk_source.h"
#include "exec/pipeline/scan/olap_scan_prepare_operator.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/in_const_predicate.hpp"
#include "gtest/gtest.h"
#include "runtime/descriptors.h"
#include "storage/tablet_schema_helper.h"
#include "testutil/column_test_helper.h"
#include "util/table_metrics.h"

namespace starrocks::pipeline {

namespace {

void expect_sample_counter(RuntimeProfile* profile, const char* name, TUnit::type unit, int64_t value) {
    auto it = profile->_counter_map.find(name);
    ASSERT_NE(it, profile->_counter_map.end()) << name;
    EXPECT_EQ(it->second.second, "SegmentRead") << name;
    EXPECT_EQ(it->second.first->type(), unit) << name;
    EXPECT_EQ(it->second.first->value(), value) << name;
}

} // namespace

class OlapScanOperatorTest : public ::testing::Test {
public:
    void SetUp() override;

protected:
    ObjectPool _object_pool;
    RuntimeState _runtime_state;
    TDescriptorTable _thrift_tbl;
    const int64_t _chunk_size = 4096;
    DescriptorTbl* _tbl = nullptr;
    TPlanNode _tnode;
    ChunkBufferLimiterPtr _chunk_buffer_limiter;
    QueryContext _query_ctx;
};

void OlapScanOperatorTest::SetUp() {
    TTableDescriptor t_table_desc;
    t_table_desc.id = 1;
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    _thrift_tbl.tableDescriptors.emplace_back(t_table_desc);

    TTupleDescriptor t_tuple_desc;
    t_tuple_desc.id = 1;
    t_tuple_desc.tableId = 1;
    _thrift_tbl.tupleDescriptors.emplace_back(t_tuple_desc);

    _tnode.row_tuples.emplace_back(1);

    Status st = DescriptorTbl::create(&_runtime_state, &_object_pool, _thrift_tbl, &_tbl, _chunk_size);
    ASSERT_TRUE(st.ok());

    _runtime_state.set_desc_tbl(_tbl);
    _chunk_buffer_limiter = std::make_unique<UnlimitedChunkBufferLimiter>();

    _query_ctx.init_mem_tracker(-1, GlobalEnv::GetInstance()->process_mem_tracker());
    _runtime_state.set_query_ctx(&_query_ctx);
}

// Reproduce remote IO outstanding while TopN temporarily permits only one task.
// The driver must wait for IO instead of repeatedly calling pull_chunk with no
// buffered input and no capacity to submit another task.
TEST_F(OlapScanOperatorTest, topn_io_cap_controls_readiness) {
    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    scan_node._io_tasks_per_scan_operator = 4;
    auto ctx_factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));
    OlapScanOperatorFactory factory(1, &scan_node, ctx_factory);
    auto op = std::make_shared<OlapScanOperator>(&factory, 1, 0, 1, &scan_node, ctx_factory->get_or_create(0));
    Morsels morsels;
    morsels.emplace_back(std::make_unique<ScanMorsel>(1, TScanRange{}));
    FixedMorselQueue queue(std::move(morsels));
    op->add_morsel_queue(&queue);
    op->_topn_filter_back_pressure = std::make_unique<TopnRfBackPressure>(0.1, 100, 8, 8, 1024);
    op->_topn_io_task_limit = 1;
    op->_num_running_io_tasks = 1;
    EXPECT_TRUE(op->ScanOperator::has_output()); // No rows yet: allow remote IO startup.
    op->_op_pull_rows = 4096;
    EXPECT_FALSE(op->ScanOperator::has_output());

    // Completion allows the driver to submit the next IO task.
    op->_num_running_io_tasks = 0;
    EXPECT_TRUE(op->ScanOperator::has_output());

    // Respect configurable caps, including disabled and greater-than-normal caps.
    op->_num_running_io_tasks = 1;
    op->_topn_io_task_limit = 2;
    EXPECT_TRUE(op->ScanOperator::has_output());
    op->_num_running_io_tasks = 2;
    EXPECT_FALSE(op->ScanOperator::has_output());
    op->_topn_io_task_limit = 0;
    EXPECT_TRUE(op->ScanOperator::has_output());
    op->_topn_io_task_limit = 8;
    op->_num_running_io_tasks = 4;
    EXPECT_FALSE(op->ScanOperator::has_output());

    // RF arrival releases the cap; bounded-wait exhaustion must do so as well.
    op->_topn_io_task_limit = 1;
    op->_num_running_io_tasks = 1;
    op->_topn_filter_back_pressure->notify_rf_arrived();
    EXPECT_TRUE(op->ScanOperator::has_output());
    op->_topn_filter_back_pressure = std::make_unique<TopnRfBackPressure>(0.1, 100, 0, 8, 1024);
    EXPECT_TRUE(op->ScanOperator::has_output());

    // Too few rows to start a throttle round: the time bound of the wait releases the cap.
    op->_topn_filter_back_pressure = std::make_unique<TopnRfBackPressure>(0.1, 100, 8, 8, 1024);
    op->_topn_filter_back_pressure->start_wait();
    EXPECT_FALSE(op->ScanOperator::has_output());
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    EXPECT_TRUE(op->ScanOperator::has_output());
    op->_topn_filter_back_pressure.reset();
    EXPECT_TRUE(op->ScanOperator::has_output());
    op->_num_running_io_tasks = 0;
    scan_node.close(&_runtime_state);
}

TEST_F(OlapScanOperatorTest, test_finish_sequence) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("OlapScanPrepareOperator::prepare",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("ScanOperatorFactory::prepare",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("OlapScanContext::parse_conjuncts",
                                          [](void* arg) { *(Status*)arg = Status::EndOfFile(""); });

    Morsels morsels;
    FixedMorselQueue morsel_queue(std::move(morsels));

    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    auto scan_ctx_factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));

    // create operator factory
    OlapScanPrepareOperatorFactory scan_prepare_operator_factory(1, 1, &scan_node, scan_ctx_factory);
    Status st = scan_prepare_operator_factory.prepare(&_runtime_state);
    ASSERT_TRUE(st.ok());

    OlapScanOperatorFactory scan_operator_factory(1, &scan_node, scan_ctx_factory);
    st = scan_operator_factory.prepare(&_runtime_state);
    ASSERT_TRUE(st.ok());

    // create operator
    auto scan_prepare_operator = scan_prepare_operator_factory.create(1, 0);
    ASSERT_TRUE(scan_prepare_operator != nullptr);
    down_cast<OlapScanPrepareOperator*>(scan_prepare_operator.get())->add_morsel_queue(&morsel_queue);

    auto scan_operator = scan_operator_factory.create(1, 0);
    ASSERT_TRUE(scan_operator != nullptr);

    // operator prepare
    st = scan_prepare_operator->prepare(&_runtime_state);
    ASSERT_TRUE(st.ok());

    // pull chunk
    SyncPoint::GetInstance()->SetCallBack("OlapScnPrepareOperator::pull_chunk::before_set_finished",
                                          [&scan_operator](void* arg) { ASSERT_FALSE(scan_operator->has_output()); });
    SyncPoint::GetInstance()->SetCallBack("OlapScnPrepareOperator::pull_chunk::after_set_finished",
                                          [&scan_operator](void* arg) { ASSERT_FALSE(scan_operator->has_output()); });
    SyncPoint::GetInstance()->SetCallBack("OlapScnPrepareOperator::pull_chunk::after_set_prepare_finished",
                                          [&scan_operator](void* arg) { ASSERT_FALSE(scan_operator->has_output()); });

    auto ret = scan_prepare_operator->pull_chunk(&_runtime_state);
    ASSERT_TRUE(ret.status().is_end_of_file());

    scan_node.close(&_runtime_state);

    SyncPoint::GetInstance()->DisableProcessing();
}

// Each sample counter must report its own statistic. SampleTime used to be fed sample_population_size,
// so a block/page count was rendered as a duration in the profile.
TEST_F(OlapScanOperatorTest, sample_counters_report_their_own_statistic) {
    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    auto scan_ctx_factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));
    OlapScanOperatorFactory scan_operator_factory(1, &scan_node, scan_ctx_factory);
    auto scan_operator = std::make_shared<OlapScanOperator>(&scan_operator_factory, 1, 0, 1, &scan_node,
                                                            scan_ctx_factory->get_or_create(0));
    TScanRange scan_range;
    auto chunk_source = scan_operator->create_chunk_source(std::make_unique<ScanMorsel>(1, scan_range), 0);
    auto* olap_chunk_source = down_cast<OlapChunkSource*>(chunk_source.get());

    ASSERT_TRUE(olap_chunk_source->ChunkSource::prepare(&_runtime_state).ok());
    olap_chunk_source->_runtime_state = &_runtime_state;
    olap_chunk_source->_init_counter(&_runtime_state);

    FragmentContext fragment_ctx;
    _runtime_state.set_fragment_ctx(&fragment_ctx);

    // _update_counter() only reads the reader statistics and the table metrics, so a reader over an empty
    // schema is enough to check how the sample statistics are mapped onto the profile counters.
    olap_chunk_source->_table_metrics = std::make_shared<TableMetrics>(1, false);
    olap_chunk_source->_reader = std::make_shared<TabletReader>(nullptr, Version(0, 1), Schema(),
                                                                TabletSchemaHelper::create_tablet_schema());

    olap_chunk_source->_params.sample_options.__set_enable_sampling(true);
    olap_chunk_source->_params.sample_options.__set_sample_method(SampleMethod::BY_BLOCK);
    olap_chunk_source->_params.sample_options.__set_probability_percent(10);

    // Distinct values so that a counter fed from the wrong statistic is unambiguous.
    auto* stats = olap_chunk_source->_reader->mutable_stats();
    stats->sample_time_ns = 111;
    stats->sample_build_histogram_time_ns = 222;
    stats->sample_size = 333;
    stats->sample_population_size = 444;
    stats->sample_build_histogram_count = 555;

    olap_chunk_source->_update_counter();

    auto* profile = olap_chunk_source->_runtime_profile;
    expect_sample_counter(profile, "SampleTime", TUnit::TIME_NS, 111);
    expect_sample_counter(profile, "SampleBuildHistogramTime", TUnit::TIME_NS, 222);
    expect_sample_counter(profile, "SampleSize", TUnit::UNIT, 333);
    expect_sample_counter(profile, "SamplePopulationSize", TUnit::UNIT, 444);
    expect_sample_counter(profile, "SampleBuildHistogramCount", TUnit::UNIT, 555);

    scan_node.close(&_runtime_state);
}

// There is deliberately no join downstream: losing a deferred filter must change this test's result.
TEST_F(OlapScanOperatorTest, heavy_runtime_in_filter_is_applied_after_materialization) {
    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    scan_node.set_heavy_expr_slot_ids({2});
    scan_node._io_tasks_per_scan_operator = 0;
    auto scan_ctx_factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));
    OlapScanOperatorFactory factory(1, &scan_node, scan_ctx_factory);
    RuntimeFilterHub hub;
    factory._runtime_filter_hub = &hub;
    SharedMorselQueueFactory morsels(std::make_unique<FixedMorselQueue>(Morsels{}), 1);
    factory.set_morsel_queue_factory(&morsels);

    auto make_filter = [&](SlotId slot) {
        auto* ref = _object_pool.add(new ColumnRef(TYPE_INT_DESC, slot));
        VectorizedInConstPredicateBuilder builder(&_runtime_state, &_object_pool, ref);
        builder.use_as_join_runtime_filter();
        EXPECT_TRUE(builder.create().ok());
        builder.add_values(ColumnTestHelper::build_column(std::vector<int32_t>{2}), 0);
        return builder.get_in_const_predicate();
    };
    auto* physical_filter = make_filter(1);
    auto* heavy_filter = make_filter(2);
    ASSERT_TRUE(physical_filter->prepare(&_runtime_state).ok());
    ASSERT_TRUE(physical_filter->open(&_runtime_state).ok());
    ASSERT_TRUE(heavy_filter->prepare(&_runtime_state).ok());
    ASSERT_TRUE(heavy_filter->open(&_runtime_state).ok());
    factory.get_runtime_in_filters() = {physical_filter, heavy_filter};

    auto op = std::make_shared<OlapScanOperator>(&factory, 1, 0, 1, &scan_node, scan_ctx_factory->get_or_create(0));
    op->add_morsel_queue(morsels.create(0));
    // No storage I/O is needed: emulate a ChunkSource that has already materialized synthetic slot 2.
    op->_peak_buffer_size_counter = op->_unique_metrics->AddHighWaterMarkCounter(
            "TestBufferSize", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));
    op->_peak_buffer_memory_usage = op->_unique_metrics->AddHighWaterMarkCounter(
            "TestBufferBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    auto query_ctx = std::make_shared<QueryContext>();
    op->_query_ctx = query_ctx;
    op->set_precondition_ready(&_runtime_state);
    ASSERT_EQ(op->runtime_in_filters(), (std::vector<ExprContext*>{physical_filter}));
    ASSERT_EQ(op->_post_scan_runtime_in_filters, (std::vector<ExprContext*>{heavy_filter}));

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnTestHelper::build_column(std::vector<int32_t>{10, 20, 30}), 1);
    chunk->append_column(ColumnTestHelper::build_column(std::vector<int32_t>{1, 2, 3}), 2);
    chunk->owner_info().set_owner_id(42, true);
    op->get_chunk_buffer().put(0, chunk, nullptr);
    auto result = op->pull_chunk(&_runtime_state);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_NE(result.value(), nullptr);
    ASSERT_EQ(result.value()->num_rows(), 1);
    EXPECT_EQ(result.value()->get_column_by_slot_id(1)->get(0).get_int32(), 20);
    EXPECT_EQ(result.value()->get_column_by_slot_id(2)->get(0).get_int32(), 2);
    EXPECT_EQ(result.value()->owner_info().owner_id(), 42);
    EXPECT_TRUE(result.value()->owner_info().is_last_chunk());
    ASSERT_NE(op->_common_metrics->get_counter("ConjunctsInputRows"), nullptr);
    ASSERT_NE(op->_common_metrics->get_counter("ConjunctsOutputRows"), nullptr);
    EXPECT_EQ(op->_common_metrics->get_counter("ConjunctsInputRows")->value(), 3);
    EXPECT_EQ(op->_common_metrics->get_counter("ConjunctsOutputRows")->value(), 1);
    EXPECT_NE(op->_common_metrics->get_counter("ConjunctsTime"), nullptr);

    // Empty results must retain EOS ownership, otherwise a downstream query cache may wait forever.
    auto rejected = std::make_shared<Chunk>();
    rejected->append_column(ColumnTestHelper::build_column(std::vector<int32_t>{7}), 2);
    rejected->owner_info().set_owner_id(43, true);
    op->get_chunk_buffer().put(0, rejected, nullptr);
    auto empty = op->pull_chunk(&_runtime_state);
    ASSERT_TRUE(empty.ok()) << empty.status();
    ASSERT_NE(empty.value(), nullptr);
    EXPECT_TRUE(empty.value()->is_empty());
    EXPECT_EQ(empty.value()->owner_info().owner_id(), 43);
    EXPECT_TRUE(empty.value()->owner_info().is_last_chunk());
    EXPECT_EQ(op->_common_metrics->get_counter("ConjunctsInputRows")->value(), 4);
    EXPECT_EQ(op->_common_metrics->get_counter("ConjunctsOutputRows")->value(), 1);

    op->close(&_runtime_state);
    ASSERT_NE(op->_common_metrics->get_counter("RuntimeInFilterNum"), nullptr);
    EXPECT_EQ(op->_common_metrics->get_counter("RuntimeInFilterNum")->value(), 2);
    physical_filter->close(&_runtime_state);
    heavy_filter->close(&_runtime_state);
    scan_node.close(&_runtime_state);
}

} // namespace starrocks::pipeline
