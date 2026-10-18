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

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "exec/hash_join_components.h"
#include "exec/hash_joiner.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"
#include "exec/pipeline/hashjoin/local_runtime_filter_feedback.h"
#include "exec/pipeline/hashjoin/spillable_hash_join_build_operator.h"
#include "exec/pipeline/hashjoin/spillable_hash_join_probe_operator.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller_factory.h"
#include "exprs/column_ref.h"
#include "exprs/literal.h"
#include "exprs/runtime_filter_bank.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "util/defer_op.h"
#include "util/failpoint/fail_point.h"

namespace starrocks {

class HashJoinFeedbackTest : public ::testing::Test {
protected:
    void check_completed_rows(bool partitioned) {
        auto* fp = failpoint::FailPointRegistry::GetInstance()->get("always_use_partition_join");
        ASSERT_NE(nullptr, fp);
        PFailPointTriggerMode mode;
        mode.set_mode(partitioned ? FailPointTriggerModeType::ENABLE : FailPointTriggerModeType::DISABLE);
        fp->setMode(mode);
        DeferOp reset_failpoint([&] {
            mode.set_mode(FailPointTriggerModeType::DISABLE);
            fp->setMode(mode);
        });

        ObjectPool pool;
        TQueryOptions options;
        options.__set_batch_size(4096);
        RuntimeState state(TUniqueId(), options, TQueryGlobals(), nullptr);
        state.init_instance_mem_tracker();
        TDescriptorTableBuilder descriptors;
        for (int i = 0; i < 2; ++i) {
            TTupleDescriptorBuilder tuple;
            tuple.add_slot(
                    TSlotDescriptorBuilder().type(TYPE_INT).column_name("k").column_pos(0).nullable(false).build());
            tuple.build(&descriptors);
        }
        DescriptorTbl* table = nullptr;
        ASSERT_OK(DescriptorTbl::create(&state, &pool, descriptors.desc_tbl(), &table, 4096));
        RowDescriptor probe_desc(*table, {0});
        RowDescriptor build_desc(*table, {1});
        const auto type = TypeDescriptor::from_logical_type(TYPE_INT);
        auto* probe_expr = pool.add(new ExprContext(pool.add(new ColumnRef(type, 0))));
        auto* build_expr = pool.add(new ExprContext(pool.add(new ColumnRef(type, 1))));
        ASSERT_OK(probe_expr->prepare(&state));
        ASSERT_OK(build_expr->prepare(&state));
        ASSERT_OK(probe_expr->open(&state));
        ASSERT_OK(build_expr->open(&state));
        DeferOp close_exprs([&] {
            probe_expr->close(&state);
            build_expr->close(&state);
        });

        THashJoinNode node;
        node.__set_join_op(TJoinOp::INNER_JOIN);
        node.__set_distribution_mode(TJoinDistributionMode::BROADCAST);
        HashJoinerParam param(&pool, node, {false}, {build_expr}, {probe_expr}, {}, {}, build_desc, probe_desc,
                              TPlanNodeType::OLAP_SCAN_NODE, TPlanNodeType::OLAP_SCAN_NODE, true, {}, {1}, {0}, 1,
                              TJoinDistributionMode::BROADCAST, false, partitioned, false, {},
                              TExprOpcode::INVALID_OPCODE, nullptr, nullptr);
        RuntimeProfile profile("join");
        HashJoiner join(param);
        ASSERT_OK(join.prepare_builder(&state, &profile));
        ASSERT_OK(join.prepare_prober(&state, &profile));
        for (int batch = 0; batch < 2; ++batch) {
            ASSERT_OK(join.append_chunk_to_ht(&state, chunk(1, 2400, 16)));
        }
        ASSERT_OK(join.build_ht(&state));
        join.reference_hash_table(&join);
        join.track_completed_probe_rows();
        join.enter_probe_phase();
        int tables = 0;
        join.hash_join_builder()->visitHt([&](auto*) { ++tables; });
        ASSERT_EQ(partitioned ? 16 : 1, tables);

        int64_t input_rows = 0;
        int64_t total_output_rows = 0;
        for (int rows : {113, 200, 4000}) {
            ASSERT_TRUE(join.need_input());
            ASSERT_OK(join.push_chunk(&state, chunk(0, rows, 32)));
            // Accepting input is not the same as having performed its lookups.
            EXPECT_EQ(input_rows, join.completed_probe_rows());
            input_rows += rows;
            ASSERT_OK(join.drain_probe_input(&state));
            int64_t output_rows = 0;
            int pulls = 0;
            while (join.has_output()) {
                ASSERT_LT(++pulls, 1000);
                auto result = join.pull_chunk(&state);
                ASSERT_OK(result.status());
                output_rows += result.value()->num_rows();
                EXPECT_LE(join.completed_probe_rows(), input_rows);
                if (join.completed_probe_rows() == input_rows) {
                    EXPECT_FALSE(join.has_output());
                }
            }
            int64_t matches = 0;
            for (int i = 0; i < rows; ++i) matches += i % 32 < 16;
            EXPECT_EQ(matches * 300, output_rows);
            // The JOIN has no other condition, so the lookups returned what it output.
            total_output_rows += output_rows;
            EXPECT_EQ(total_output_rows, join.probe_output_rows());
            EXPECT_EQ(input_rows, join.completed_probe_rows());
            EXPECT_GT(pulls, 1); // Duplicate matches require multiple output chunks.
            EXPECT_TRUE(join.need_input());
            EXPECT_FALSE(join.is_done());
        }

        // EOF must also drain a fresh partial batch after a measurement boundary.
        ASSERT_OK(join.push_chunk(&state, chunk(0, 17, 32)));
        ASSERT_OK(join.probe_input_finished(&state));
        join.enter_post_probe_phase();
        int64_t output_rows = 0;
        while (join.has_output()) {
            auto result = join.pull_chunk(&state);
            ASSERT_OK(result.status());
            output_rows += result.value()->num_rows();
        }
        EXPECT_EQ(16 * 300, output_rows);
        EXPECT_EQ(total_output_rows + output_rows, join.probe_output_rows());
        EXPECT_EQ(input_rows + 17, join.completed_probe_rows());
        EXPECT_TRUE(join.is_done());
    }

    static ChunkPtr chunk(SlotId slot, int rows, int keys) {
        auto column = Int32Column::create();
        for (int i = 0; i < rows; ++i) column->append(i % keys);
        auto result = std::make_shared<Chunk>();
        result->append_column(std::move(column), slot);
        return result;
    }
};

TEST_F(HashJoinFeedbackTest, SingleTableCountsCompletedInputOnce) {
    check_completed_rows(false);
}

TEST_F(HashJoinFeedbackTest, PartitionedDrainPreservesDuplicatesAndAcceptsMoreInput) {
    check_completed_rows(true);
}

TEST_F(HashJoinFeedbackTest, EmptyFilterOutputDrainsPreviouslyBufferedRows) {
    RuntimeState state;
    state.set_chunk_size(4096);
    pipeline::ChunkAccumulateOperatorFactory factory(1, 1);
    auto accumulator = factory.create(1, 0);
    ASSERT_OK(accumulator->prepare(&state));
    auto feedback = std::make_shared<pipeline::LocalRuntimeFilterFeedback>();
    accumulator->set_local_runtime_filter_feedback(feedback);

    feedback->observe_filter(4096, 100, 100000);
    ASSERT_OK(accumulator->push_chunk(&state, chunk(0, 100, 16)));
    ASSERT_FALSE(accumulator->has_output());
    // The driver drops empty chunks, so there are no more accumulator pushes.
    for (int i = 1; i < 64; ++i) feedback->observe_filter(4096, 0, 100000);
    ASSERT_TRUE(feedback->needs_drain());
    ASSERT_EQ(100, feedback->outstanding_rows());

    ASSERT_OK(pipeline::HashJoinProbeOperator::drain_local_runtime_filter_input(&state, accumulator.get()));
    ASSERT_TRUE(accumulator->has_output());
    auto result = accumulator->pull_chunk(&state);
    ASSERT_OK(result.status());
    ASSERT_EQ(100, result.value()->num_rows());
    feedback->observe_lookup(100, 10000);
    EXPECT_FALSE(feedback->needs_drain());
    EXPECT_FALSE(feedback->use_filter());
    EXPECT_EQ(0, feedback->outstanding_rows());
    EXPECT_TRUE(accumulator->need_input());
    EXPECT_FALSE(accumulator->is_finished());

    // Draining a window must not turn into EOF or lose subsequent input.
    feedback->observe_filter(4096, 4096, 0);
    ASSERT_OK(accumulator->push_chunk(&state, chunk(0, 4096, 16)));
    result = accumulator->pull_chunk(&state);
    ASSERT_OK(result.status());
    EXPECT_EQ(4096, result.value()->num_rows());
    feedback->observe_lookup(4096, 10000);
    ASSERT_OK(accumulator->set_finishing(&state));
    EXPECT_TRUE(accumulator->is_finished());
}

TEST_F(HashJoinFeedbackTest, MeasurementDrainDoesNotFlushUnrelatedAccumulator) {
    RuntimeState state;
    state.set_chunk_size(4096);
    pipeline::ChunkAccumulateOperatorFactory factory(1, 1);
    auto accumulator = factory.create(1, 0);
    ASSERT_OK(accumulator->prepare(&state));
    ASSERT_OK(accumulator->push_chunk(&state, chunk(0, 100, 16)));
    ASSERT_OK(pipeline::HashJoinProbeOperator::drain_local_runtime_filter_input(&state, accumulator.get()));
    EXPECT_FALSE(accumulator->has_output());
    ASSERT_OK(accumulator->set_finishing(&state));
    auto result = accumulator->pull_chunk(&state);
    ASSERT_OK(result.status());
    EXPECT_EQ(100, result.value()->num_rows());
}

TEST_F(HashJoinFeedbackTest, MeasurementDrainPreservesBothBufferedChunks) {
    RuntimeState state;
    state.set_chunk_size(4096);
    pipeline::ChunkAccumulateOperatorFactory factory(1, 1);
    auto accumulator = factory.create(1, 0);
    ASSERT_OK(accumulator->prepare(&state));
    auto feedback = std::make_shared<pipeline::LocalRuntimeFilterFeedback>();
    accumulator->set_local_runtime_filter_feedback(feedback);
    for (int i = 0; i < 62; ++i) feedback->observe_filter(4096, 0, 100000);
    for (int rows : {3000, 2000}) {
        feedback->observe_filter(4096, rows, 100000);
        ASSERT_OK(accumulator->push_chunk(&state, chunk(0, rows, 16)));
    }
    ASSERT_TRUE(feedback->needs_drain());
    for (int rows : {3000, 2000}) {
        ASSERT_OK(pipeline::HashJoinProbeOperator::drain_local_runtime_filter_input(&state, accumulator.get()));
        ASSERT_TRUE(accumulator->has_output());
        auto result = accumulator->pull_chunk(&state);
        ASSERT_OK(result.status());
        EXPECT_EQ(rows, result.value()->num_rows());
        feedback->observe_lookup(rows, 10000);
    }
    EXPECT_FALSE(feedback->needs_drain());
    EXPECT_EQ(0, feedback->outstanding_rows());
    EXPECT_TRUE(accumulator->need_input());
    EXPECT_FALSE(accumulator->is_finished());
    ASSERT_OK(accumulator->set_finishing(&state));
    EXPECT_TRUE(accumulator->is_finished());
}

namespace {

// A filter that the build side kept back and that nobody has decided.
class FakeDecider final : public pipeline::DeferredRfDecider {
public:
    int32_t filter_id() const override { return 7; }
    State state() const override { return _state; }
    State submit(const pipeline::DeferredRfWindow&) override { return _state; }
    void observe_scan_rows(int64_t) override {}
    void cancel() override {
        if (_state == State::WAITING) _state = State::REFUSED;
    }
    void set_state(State state) { _state = state; }

private:
    State _state = State::WAITING;
};

// A filter whose build fails with the first window that it gets.
class FailingDecider final : public pipeline::DeferredRfDecider {
public:
    int32_t filter_id() const override { return 7; }
    State state() const override { return _state; }
    State submit(const pipeline::DeferredRfWindow&) override {
        _state = State::FAILED;
        return _state;
    }
    void observe_scan_rows(int64_t) override {}
    Status error() const override {
        return _state == State::FAILED ? Status::MemoryLimitExceeded("the filter does not fit") : Status::OK();
    }

private:
    State _state = State::WAITING;
};

class TestFactory final : public pipeline::OperatorFactory {
public:
    TestFactory() : OperatorFactory(1, "test", 1) {}
    pipeline::OperatorPtr create(int32_t, int32_t) override { return nullptr; }
};

} // namespace

// The probe operators of a JOIN with a hash table that is ready to probe: 4800 build rows over 16 keys.
class ProbeFeedbackTest : public ::testing::Test {
protected:
    void SetUp() override {
        _failpoint = failpoint::FailPointRegistry::GetInstance()->get("always_use_partition_join");
    }

    void TearDown() override {
        if (_probe_expr != nullptr) _probe_expr->close(_state.get());
        if (_build_expr != nullptr) _build_expr->close(_state.get());
        set_partitioned(false);
    }

    void set_partitioned(bool partitioned) {
        ASSERT_NE(nullptr, _failpoint);
        PFailPointTriggerMode mode;
        mode.set_mode(partitioned ? FailPointTriggerModeType::ENABLE : FailPointTriggerModeType::DISABLE);
        _failpoint->setMode(mode);
    }

    // The JOIN of the test. The build side has the keys 0..15, 300 times each; the probe chunks of the tests have 64
    // keys, so one probe row of four has a pair.
    void init(bool partitioned, TJoinOp::type join_op = TJoinOp::INNER_JOIN, bool with_other_conjunct = false,
              bool with_runtime_filter = false) {
        ASSERT_NO_FATAL_FAILURE(set_partitioned(partitioned));
        TQueryOptions options;
        options.__set_batch_size(4096);
        if (_max_filter_ndv > 0) options.__set_global_runtime_filter_build_max_size(_max_filter_ndv);
        _state = std::make_unique<RuntimeState>(TUniqueId(), options, TQueryGlobals(), nullptr);
        _state->init_instance_mem_tracker();
        TDescriptorTableBuilder descriptors;
        for (int i = 0; i < 2; ++i) {
            TTupleDescriptorBuilder tuple;
            tuple.add_slot(
                    TSlotDescriptorBuilder().type(TYPE_INT).column_name("k").column_pos(0).nullable(false).build());
            tuple.build(&descriptors);
        }
        DescriptorTbl* table = nullptr;
        ASSERT_OK(DescriptorTbl::create(_state.get(), &_pool, descriptors.desc_tbl(), &table, 4096));
        _probe_desc = std::make_unique<RowDescriptor>(*table, std::vector<TTupleId>{0});
        _build_desc = std::make_unique<RowDescriptor>(*table, std::vector<TTupleId>{1});
        const auto type = TypeDescriptor::from_logical_type(TYPE_INT);
        _probe_expr = _pool.add(new ExprContext(_pool.add(new ColumnRef(type, 0))));
        _build_expr = _pool.add(new ExprContext(_pool.add(new ColumnRef(type, 1))));
        ASSERT_OK(_probe_expr->prepare(_state.get()));
        ASSERT_OK(_build_expr->prepare(_state.get()));
        ASSERT_OK(_probe_expr->open(_state.get()));
        ASSERT_OK(_build_expr->open(_state.get()));

        std::vector<ExprContext*> other_conjuncts;
        if (with_other_conjunct) {
            // A condition on the pairs that every pair meets.
            auto* literal = _pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_BOOLEAN>(true, 1),
                                                            TypeDescriptor(TYPE_BOOLEAN)));
            auto* conjunct = _pool.add(new ExprContext(literal));
            ASSERT_OK(conjunct->prepare(_state.get()));
            ASSERT_OK(conjunct->open(_state.get()));
            other_conjuncts.push_back(conjunct);
        }
        std::list<RuntimeFilterBuildDescriptor*> build_filters;
        if (with_runtime_filter) {
            _rf_desc._filter_id = 7;
            _rf_desc._build_expr_ctx = _build_expr;
            _rf_desc._build_expr_order = 0;
            _rf_desc._has_consumer = true;
            _rf_desc._has_remote_targets = false;
            // The planner's estimate of the distinct keys, which is what the filter is sized for.
            _rf_desc._estimated_build_ndv = 16;
            _rf_desc._join_mode = TRuntimeFilterBuildJoinMode::BROADCAST;
            build_filters.push_back(&_rf_desc);
        }
        // A semi JOIN gives only the columns of the probe side.
        std::set<SlotId> build_output_slots =
                join_op == TJoinOp::LEFT_SEMI_JOIN ? std::set<SlotId>{} : std::set<SlotId>{1};
        _node.__set_join_op(join_op);
        _node.__set_distribution_mode(TJoinDistributionMode::BROADCAST);
        _param.reset(new HashJoinerParam(&_pool, _node, {false}, {_build_expr}, {_probe_expr}, other_conjuncts, {},
                                         *_build_desc, *_probe_desc, TPlanNodeType::OLAP_SCAN_NODE,
                                         TPlanNodeType::OLAP_SCAN_NODE, true, build_filters, build_output_slots, {0}, 1,
                                         TJoinDistributionMode::BROADCAST, false, partitioned, false, {},
                                         TExprOpcode::INVALID_OPCODE, nullptr, nullptr));
        _joiner = std::make_shared<HashJoiner>(*_param);
        ASSERT_OK(_joiner->prepare_builder(_state.get(), &_profile));
        ASSERT_OK(_joiner->prepare_prober(_state.get(), &_profile));
        for (int batch = 0; batch < 2; ++batch) {
            ASSERT_OK(_joiner->append_chunk_to_ht(_state.get(), chunk(1, 2400, 16)));
        }
        ASSERT_OK(_joiner->build_ht(_state.get()));
        _joiner->enter_probe_phase();

        // The spillable probe asks the build side whether it has spilled, and the build side has not.
        _spiller = _spiller_factory->create(spill::SpilledOptions(4, false));
        _joiner->set_spiller(_spiller);
    }

    // The build side arms its filters as it finishes. The limit of 0 sends every filter to the deferral.
    std::shared_ptr<pipeline::DeferredLocalRuntimeFilter> arm_through_the_joiner() {
        EXPECT_TRUE(_joiner->_create_runtime_bloom_filters(_state.get(), 0).ok());
        return _joiner->_deferred_rfs.empty() ? nullptr : _joiner->_deferred_rfs.back();
    }

    // A filter that the build side kept back over the 16 keys of the hash table.
    std::shared_ptr<pipeline::DeferredLocalRuntimeFilter> arm_deferred_filter(const char* skip_bloom_reason = nullptr) {
        auto column = Int32Column::create();
        column->append(0); // the row that the hash table reserves in front of the keys
        for (int i = 0; i < 16; ++i) column->append(i);
        pipeline::DeferredLocalRuntimeFilter::Params params;
        params.filter_id = 7;
        params.build_type = TYPE_INT;
        params.join_mode = TRuntimeFilterBuildJoinMode::BROADCAST;
        params.ndv = 16;
        params.build_rows = 16;
        params.key_ns = 10;
        params.benefit_margin = 2.0;
        params.skip_bloom_reason = skip_bloom_reason;
        params.columns = {std::move(column)};
        params.pool = &_pool;
        params.publish = [this](const RuntimeFilter* filter) { _published = filter; };
        auto deferred = std::make_shared<pipeline::DeferredLocalRuntimeFilter>(std::move(params));
        _joiner->_deferred_rfs.push_back(deferred);
        return deferred;
    }

    // The window of a driver whose JOIN looks up slowly and finds a pair for few rows.
    static pipeline::DeferredRfWindow expensive_window() {
        pipeline::DeferredRfWindow window;
        window.chunks = 64;
        window.rows = 262144;
        window.lookup_ns = window.rows * 1000;
        window.probe_output_rows = window.rows / 16;
        window.morsels_taken = 10;
        window.morsels_remaining = 90;
        return window;
    }

    // The window of a driver whose JOIN finds a pair for every row, so the filter would drop nothing.
    static pipeline::DeferredRfWindow no_gain_window() {
        pipeline::DeferredRfWindow window = expensive_window();
        window.probe_output_rows = window.rows;
        return window;
    }

    template <class Probe>
    std::unique_ptr<Probe> make_probe() {
        return std::make_unique<Probe>(&_factory, 1, "probe", 1, 0, _joiner, _joiner);
    }

    static ChunkPtr chunk(SlotId slot, int rows, int keys) {
        auto column = Int32Column::create();
        for (int i = 0; i < rows; ++i) column->append(i % keys);
        auto result = std::make_shared<Chunk>();
        result->append_column(std::move(column), slot);
        return result;
    }

    // The section where the profile of the build side says why the filter was not kept back.
    RuntimeProfile* not_deferred_section() { return _profile.get_child(pipeline::deferred_rf_section_name(7)); }

    // The rows of the chunk that the published filter keeps.
    ChunkPtr apply_published_filter(const ChunkPtr& all_rows) {
        RuntimeFilter::RunningContext context;
        context.use_merged_selection = false;
        context.compatibility = false;
        context.selection.assign(all_rows->num_rows(), 1);
        _published->evaluate(all_rows->get_column_by_slot_id(0).get(), &context);
        auto kept_rows = std::make_shared<Chunk>();
        auto column = Int32Column::create();
        const auto& values = down_cast<const Int32Column*>(all_rows->get_column_by_slot_id(0).get())->get_data();
        for (size_t row = 0; row < all_rows->num_rows(); ++row) {
            if (context.selection[row]) column->append(values[row]);
        }
        kept_rows->append_column(std::move(column), 0);
        return kept_rows;
    }

    // The rows that the JOIN gives for the chunk, each as text, in a fixed order.
    std::vector<std::string> join_rows(const ChunkPtr& input) {
        auto probe = make_probe<pipeline::HashJoinProbeOperator>();
        std::vector<std::string> rows;
        EXPECT_TRUE(probe->push_chunk(_state.get(), input).ok());
        int pulls = 0;
        while (probe->has_output() && ++pulls < 100000) {
            auto result = probe->pull_chunk(_state.get());
            EXPECT_TRUE(result.ok());
            if (!result.ok() || result.value() == nullptr) break;
            for (size_t row = 0; row < result.value()->num_rows(); ++row) {
                rows.push_back(result.value()->debug_row(row));
            }
        }
        std::sort(rows.begin(), rows.end());
        return rows;
    }

    // Pushes one chunk that the scan has emitted, as the driver does, and checks that a drain reaches the lookups.
    template <class Probe>
    void check_drain_reaches_probe() {
        auto probe = make_probe<Probe>();
        ASSERT_TRUE(pipeline::HashJoinProbeOperator::measures_local_runtime_filter(probe.get()));
        auto feedback = std::make_shared<pipeline::LocalRuntimeFilterFeedback>();
        probe->_probe_rf_feedback = feedback;

        feedback->observe_filter(113, 113, 0);
        ASSERT_TRUE(probe->need_input());
        ASSERT_OK(probe->push_chunk(_state.get(), chunk(0, 113, 32)));
        // Accepting input is not the same as having performed its lookups: the partitioned prober holds the rows
        // until it is drained.
        EXPECT_FALSE(probe->has_output());
        ASSERT_OK(pipeline::HashJoinProbeOperator::drain_local_runtime_filter_input(_state.get(), probe.get()));
        EXPECT_TRUE(probe->has_output());
        int pulls = 0;
        while (probe->has_output()) {
            ASSERT_LT(++pulls, 1000);
            auto result = probe->pull_chunk(_state.get());
            ASSERT_OK(result.status());
        }
        EXPECT_EQ(0, feedback->outstanding_rows());
        EXPECT_EQ(113, _joiner->completed_probe_rows());
        EXPECT_TRUE(feedback->enabled());
        EXPECT_FALSE(feedback->stopped());
    }

    // The build side has spilled, and the JOIN is asked to probe: the feedback has to stop and refuse the filter
    // that is not decided, and the driver is not asked to drain rows that nobody will look up.
    template <class Step>
    void check_spill_stops_feedback(Step&& step) {
        auto probe = make_probe<pipeline::SpillableHashJoinProbeOperator>();
        auto feedback = std::make_shared<pipeline::LocalRuntimeFilterFeedback>();
        auto decider = std::make_shared<FakeDecider>();
        feedback->set_deferred_resolver(
                [decider]() { return std::vector<std::shared_ptr<pipeline::DeferredRfDecider>>{decider}; }, nullptr);
        feedback->resolve_deferred_once();
        ASSERT_TRUE(feedback->deferred());
        probe->_probe_rf_feedback = feedback;
        // 64 chunks are out of the scan and wait for their lookups, so the driver would have to drain them.
        for (int i = 0; i < 64; ++i) feedback->observe_filter(4096, 4096, 0);
        ASSERT_TRUE(feedback->needs_drain());

        // The rows that the build side spilled make the JOIN a spilled one.
        _spiller->mutable_spilled_append_rows() = 4800;
        step(*probe);

        EXPECT_TRUE(feedback->stopped());
        EXPECT_FALSE(feedback->enabled());
        // A decision to stop is not a refusal of the filter by a window, and it is not broken accounting.
        EXPECT_FALSE(feedback->deferral_refused());
        EXPECT_EQ(pipeline::DeferredRfDecider::State::REFUSED, decider->state());
        // No driver is asked to drain rows that nobody will look up.
        EXPECT_FALSE(feedback->needs_drain());
        ASSERT_NE(nullptr, probe->unique_metrics()->get_info_string("LocalRfFallbackReason"));
        EXPECT_EQ("join spilled", *probe->unique_metrics()->get_info_string("LocalRfFallbackReason"));
        ASSERT_OK(pipeline::HashJoinProbeOperator::drain_local_runtime_filter_input(_state.get(), probe.get()));
    }

    ObjectPool _pool;
    int64_t _max_filter_ndv = 0;
    RuntimeFilterBuildDescriptor _rf_desc;
    std::unique_ptr<RuntimeState> _state;
    std::unique_ptr<RowDescriptor> _probe_desc;
    std::unique_ptr<RowDescriptor> _build_desc;
    ExprContext* _probe_expr = nullptr;
    ExprContext* _build_expr = nullptr;
    THashJoinNode _node;
    std::unique_ptr<HashJoinerParam> _param;
    RuntimeProfile _profile{"join"};
    HashJoinerPtr _joiner;
    std::shared_ptr<spill::SpillerFactory> _spiller_factory = std::make_shared<spill::SpillerFactory>();
    std::shared_ptr<spill::Spiller> _spiller;
    TestFactory _factory;
    failpoint::FailPoint* _failpoint = nullptr;
    const RuntimeFilter* _published = nullptr;
};

// Covers: the spillable probe is measured like the plain one while its JOIN has not spilled, and the drain that the
//         driver orders reaches its lookups.
TEST_F(ProbeFeedbackTest, DrainReachesTheSpillableProbeThatHasNotSpilled) {
    ASSERT_NO_FATAL_FAILURE(init(true));
    check_drain_reaches_probe<pipeline::SpillableHashJoinProbeOperator>();
}

TEST_F(ProbeFeedbackTest, DrainReachesThePlainProbe) {
    ASSERT_NO_FATAL_FAILURE(init(true));
    check_drain_reaches_probe<pipeline::HashJoinProbeOperator>();
}

TEST_F(ProbeFeedbackTest, OnlyProbesOfTheJoinAreMeasured) {
    ASSERT_NO_FATAL_FAILURE(init(false));
    auto plain = make_probe<pipeline::HashJoinProbeOperator>();
    auto spillable = make_probe<pipeline::SpillableHashJoinProbeOperator>();
    pipeline::ChunkAccumulateOperatorFactory factory(1, 1);
    auto accumulate = factory.create(1, 0);
    EXPECT_TRUE(pipeline::HashJoinProbeOperator::measures_local_runtime_filter(plain.get()));
    EXPECT_TRUE(pipeline::HashJoinProbeOperator::measures_local_runtime_filter(spillable.get()));
    EXPECT_FALSE(pipeline::HashJoinProbeOperator::measures_local_runtime_filter(accumulate.get()));
}

// Covers: the JOIN is found spilled when it is asked to take a chunk, to give one out or to finish, and the feedback
//         stops at the first of them. The spill cannot start after the build side has finished, so these are the
//         places where a spilled JOIN shows itself. The calls go through the same check, and the tests of pull and
//         finish run the entry points, while the push would need a prepared probe spiller.
TEST_F(ProbeFeedbackTest, SpilledJoinStopsTheFeedbackAtPush) {
    ASSERT_NO_FATAL_FAILURE(init(false));
    check_spill_stops_feedback(
            [](pipeline::SpillableHashJoinProbeOperator& probe) { EXPECT_TRUE(probe._probes_through_spill()); });
}

TEST_F(ProbeFeedbackTest, SpilledJoinStopsTheFeedbackAtPull) {
    ASSERT_NO_FATAL_FAILURE(init(false));
    check_spill_stops_feedback([this](pipeline::SpillableHashJoinProbeOperator& probe) {
        // A batch of build partitions is loading, so the pull parks before it needs the spill readers.
        probe._processing_partitions.push_back(nullptr);
        probe._batch_state.store(pipeline::BatchState::kLoading);
        auto result = probe.pull_chunk(_state.get());
        EXPECT_TRUE(result.ok());
        EXPECT_EQ(nullptr, result.value());
    });
}

TEST_F(ProbeFeedbackTest, SpilledJoinStopsTheFeedbackAtFinishing) {
    ASSERT_NO_FATAL_FAILURE(init(false));
    check_spill_stops_feedback([this](pipeline::SpillableHashJoinProbeOperator& probe) {
        EXPECT_TRUE(probe.set_finishing(_state.get()).ok());
    });
}

// Covers: a JOIN that has not spilled keeps its feedback through the same calls.
TEST_F(ProbeFeedbackTest, JoinThatHasNotSpilledKeepsTheFeedback) {
    ASSERT_NO_FATAL_FAILURE(init(false));
    auto probe = make_probe<pipeline::SpillableHashJoinProbeOperator>();
    auto feedback = std::make_shared<pipeline::LocalRuntimeFilterFeedback>();
    probe->_probe_rf_feedback = feedback;
    feedback->observe_filter(10, 10, 0);
    ASSERT_OK(probe->push_chunk(_state.get(), chunk(0, 10, 32)));
    while (probe->has_output()) {
        ASSERT_OK(probe->pull_chunk(_state.get()).status());
    }
    ASSERT_OK(probe->set_finishing(_state.get()));
    EXPECT_TRUE(feedback->enabled());
    EXPECT_FALSE(feedback->stopped());
    EXPECT_EQ(nullptr, probe->unique_metrics()->get_info_string("LocalRfFallbackReason"));
}

// Covers: the window that the lookups of the probe close builds the deferred filter, and the build fails. The probe
//         returns the error to the driver instead of going on without the filter.
TEST_F(ProbeFeedbackTest, ProbeReturnsTheErrorOfAFilterThatFailedToBuild) {
    ASSERT_NO_FATAL_FAILURE(init(false));
    auto probe = make_probe<pipeline::HashJoinProbeOperator>();
    auto feedback = std::make_shared<pipeline::LocalRuntimeFilterFeedback>();
    auto decider = std::make_shared<FailingDecider>();
    feedback->set_deferred_resolver(
            [decider]() { return std::vector<std::shared_ptr<pipeline::DeferredRfDecider>>{decider}; }, nullptr);
    feedback->resolve_deferred_once();
    probe->_probe_rf_feedback = feedback;

    Status status;
    for (int i = 0; i < 64 && status.ok(); ++i) {
        feedback->observe_filter(4096, 4096, 0);
        status = probe->push_chunk(_state.get(), chunk(0, 4096, 4096));
        while (status.ok() && probe->has_output()) {
            status = probe->pull_chunk(_state.get()).status();
        }
    }

    ASSERT_FALSE(status.ok());
    EXPECT_TRUE(status.is_mem_limit_exceeded());
    EXPECT_TRUE(feedback->deferral_failed());
    // The other probes of the JOIN find the failure before their lookups, whatever their feedback is doing.
    auto other = make_probe<pipeline::HashJoinProbeOperator>();
    other->_probe_rf_feedback = feedback;
    EXPECT_FALSE(other->push_chunk(_state.get(), chunk(0, 10, 4096)).ok());
}

// The rows that the JOIN gives for the chunk, each as text, in a fixed order.
// The chunk has 4096 probe rows with the keys 0..63, 64 rows for every key.
// Covers: the filter that is built late is a filter of probe rows that cannot have a pair, so the JOIN gives the same
//         rows with it as without it.
// Here 16 of the 64 keys are in the hash table, so a quarter of the probe rows have a pair.
#define CHECK_SAME_ROWS_OF(join_op, with_conjunct, expected_rows, skip_bloom_reason, expect_bloom)  \
    do {                                                                                            \
        ASSERT_NO_FATAL_FAILURE(init(false, join_op, with_conjunct));                               \
        auto deferred = arm_deferred_filter(skip_bloom_reason);                                     \
        deferred->observe_scan_rows(262144);                                                        \
        ASSERT_EQ(pipeline::DeferredRfDecider::State::BUILT, deferred->submit(expensive_window())); \
        ASSERT_NE(nullptr, _published);                                                             \
        EXPECT_EQ(expect_bloom, _published->get_membership_filter()->can_use_bf());                 \
        auto all_rows = chunk(0, 4096, 64);                                                         \
        auto kept_rows = apply_published_filter(all_rows);                                          \
        ASSERT_LT(kept_rows->num_rows(), all_rows->num_rows());                                     \
        ASSERT_GE(kept_rows->num_rows(), all_rows->num_rows() / 4);                                 \
        const auto without_filter = join_rows(all_rows);                                            \
        const auto with_filter = join_rows(kept_rows);                                              \
        EXPECT_EQ(static_cast<size_t>(expected_rows), without_filter.size());                       \
        EXPECT_EQ(without_filter, with_filter);                                                     \
    } while (false)

#define CHECK_SAME_ROWS(join_op, with_conjunct, expected_rows) \
    CHECK_SAME_ROWS_OF(join_op, with_conjunct, expected_rows, nullptr, true)

TEST_F(ProbeFeedbackTest, InnerJoinGivesTheSameRowsWithTheFilterThatWasBuiltLate) {
    // 16 keys with a pair, 64 probe rows per key, 300 build rows per key.
    CHECK_SAME_ROWS(TJoinOp::INNER_JOIN, false, 16 * 64 * 300);
}

TEST_F(ProbeFeedbackTest, LeftSemiJoinGivesTheSameRowsWithTheFilterThatWasBuiltLate) {
    CHECK_SAME_ROWS(TJoinOp::LEFT_SEMI_JOIN, false, 16 * 64);
}

TEST_F(ProbeFeedbackTest, LeftSemiJoinWithConditionsOnThePairsGivesTheSameRowsWithTheFilterThatWasBuiltLate) {
    CHECK_SAME_ROWS(TJoinOp::LEFT_SEMI_JOIN, true, 16 * 64);
}

// Covers: the filter for more distinct keys than the limit of the global filters has its min/max range only, and a filter
//         that the query could not afford the memory of the Bloom filter for has it too. The JOIN gives the same rows
//         with such a filter as without it.
TEST_F(ProbeFeedbackTest, InnerJoinGivesTheSameRowsWithAMinMaxFilterForMoreKeysThanTheLimit) {
    CHECK_SAME_ROWS_OF(TJoinOp::INNER_JOIN, false, 16 * 64 * 300, "NDV > cap", false);
}

TEST_F(ProbeFeedbackTest, LeftSemiJoinGivesTheSameRowsWithAMinMaxFilterForMoreKeysThanTheLimit) {
    CHECK_SAME_ROWS_OF(TJoinOp::LEFT_SEMI_JOIN, false, 16 * 64, "NDV > cap", false);
}

TEST_F(ProbeFeedbackTest, InnerJoinGivesTheSameRowsWithAMinMaxFilterWhenTheMemoryDoesNotFitTheBloomFilter) {
    auto* fail_point = failpoint::FailPointRegistry::GetInstance()->get("deferred_rf_bloom_alloc_fails");
    ASSERT_NE(nullptr, fail_point);
    PFailPointTriggerMode mode;
    mode.set_mode(FailPointTriggerModeType::ENABLE);
    fail_point->setMode(mode);
    DeferOp reset([&] {
        mode.set_mode(FailPointTriggerModeType::DISABLE);
        fail_point->setMode(mode);
    });
    CHECK_SAME_ROWS_OF(TJoinOp::INNER_JOIN, false, 16 * 64 * 300, nullptr, false);
}

// Covers: the share of probe rows with a pair that the deferred filter reads from the lookups. The JOIN returns a row
//         for every pair, except a LEFT SEMI JOIN without conditions on the pairs, which returns a row for every probe
//         row with a pair, and the filter takes that into account. All three read one quarter.
#define CHECK_MATCH_RATIO(join_op, with_conjunct, expected_semi)                                    \
    do {                                                                                            \
        ASSERT_NO_FATAL_FAILURE(init(false, join_op, with_conjunct, /*with_runtime_filter=*/true)); \
        auto deferred = arm_through_the_joiner();                                                   \
        ASSERT_NE(nullptr, deferred);                                                               \
        EXPECT_EQ(expected_semi, deferred->_params.semi_join);                                      \
        auto probe = make_probe<pipeline::HashJoinProbeOperator>();                                 \
        probe->_probe_rf_feedback = std::make_shared<pipeline::LocalRuntimeFilterFeedback>();       \
        ASSERT_OK(probe->push_chunk(_state.get(), chunk(0, 4096, 64)));                             \
        while (probe->has_output()) ASSERT_OK(probe->pull_chunk(_state.get()).status());            \
        ASSERT_EQ(4096, _joiner->completed_probe_rows());                                           \
        pipeline::DeferredRfWindow window;                                                          \
        window.chunks = 64;                                                                         \
        window.rows = _joiner->completed_probe_rows();                                              \
        window.probe_output_rows = _joiner->probe_output_rows();                                    \
        window.morsels_taken = 1;                                                                   \
        window.morsels_remaining = 1;                                                               \
        pipeline::DeferredRfInputs inputs;                                                          \
        inputs.ndv = deferred->_params.ndv;                                                         \
        inputs.build_rows = deferred->_params.build_rows;                                           \
        inputs.semi_join = deferred->_params.semi_join;                                             \
        inputs.key_ns = 10;                                                                         \
        inputs.scan_rows_seen = window.rows;                                                        \
        const auto decision = pipeline::decide_deferred_runtime_filter(window, inputs, 2.0);        \
        EXPECT_NEAR(0.25, decision.match_ratio, 0.001);                                             \
    } while (false)

TEST_F(ProbeFeedbackTest, InnerJoinReadsTheShareOfProbeRowsWithAPairFromTheLookups) {
    CHECK_MATCH_RATIO(TJoinOp::INNER_JOIN, false, false);
}

TEST_F(ProbeFeedbackTest, LeftSemiJoinReadsTheShareOfProbeRowsWithAPairFromTheLookups) {
    CHECK_MATCH_RATIO(TJoinOp::LEFT_SEMI_JOIN, false, true);
}

TEST_F(ProbeFeedbackTest, LeftSemiJoinWithConditionsOnThePairsReadsTheShareFromThePairs) {
    CHECK_MATCH_RATIO(TJoinOp::LEFT_SEMI_JOIN, true, false);
}

// Covers: the build side keeps the filter back with the keys of every partition of the hash table, whatever the
//         number of partitions is.
#define CHECK_KEYS_OF_EVERY_PARTITION(partitioned)                                                            \
    do {                                                                                                      \
        ASSERT_NO_FATAL_FAILURE(init(partitioned, TJoinOp::INNER_JOIN, false, /*with_runtime_filter=*/true)); \
        auto deferred = arm_through_the_joiner();                                                             \
        ASSERT_NE(nullptr, deferred);                                                                         \
        EXPECT_TRUE(deferred->holds_keys());                                                                  \
        size_t partitions = 0;                                                                                \
        _joiner->_hash_join_builder->visitHt([&](JoinHashTable*) { ++partitions; });                          \
        EXPECT_EQ(partitions, deferred->_params.columns.size());                                              \
        /* Every partition reserves its first row in front of the keys. */                                    \
        size_t keys = 0;                                                                                      \
        for (const auto& column : deferred->_params.columns) keys += column->size() - 1;                      \
        EXPECT_EQ(4800u, keys);                                                                               \
        EXPECT_EQ(4800u, deferred->_params.build_rows);                                                       \
    } while (false)

TEST_F(ProbeFeedbackTest, DeferredFilterHoldsTheKeysOfTheHashTable) {
    CHECK_KEYS_OF_EVERY_PARTITION(false);
}

TEST_F(ProbeFeedbackTest, DeferredFilterHoldsTheKeysOfEveryPartitionOfAPartitionedHashTable) {
    CHECK_KEYS_OF_EVERY_PARTITION(true);
}

// Covers: a filter for more distinct keys than the limit of the global runtime filters is kept back without a Bloom
//         filter, and builds with its min/max range only.
TEST_F(ProbeFeedbackTest, FilterWithMoreKeysThanTheGlobalFilterLimitIsDeferredWithoutABloomFilter) {
    // The planner expects 16 distinct keys.
    _max_filter_ndv = 15;
    ASSERT_NO_FATAL_FAILURE(init(false, TJoinOp::INNER_JOIN, false, /*with_runtime_filter=*/true));

    auto deferred = arm_through_the_joiner();

    ASSERT_NE(nullptr, deferred);
    EXPECT_STREQ("NDV > cap", deferred->_params.skip_bloom_reason);
    EXPECT_EQ(nullptr, not_deferred_section());
}

TEST_F(ProbeFeedbackTest, FilterWithAsManyKeysAsTheGlobalFilterLimitKeepsItsBloomFilter) {
    _max_filter_ndv = 16;
    ASSERT_NO_FATAL_FAILURE(init(false, TJoinOp::INNER_JOIN, false, /*with_runtime_filter=*/true));
    auto deferred = arm_through_the_joiner();
    ASSERT_NE(nullptr, deferred);
    EXPECT_EQ(nullptr, deferred->_params.skip_bloom_reason);
    EXPECT_EQ(nullptr, not_deferred_section());
}

TEST_F(ProbeFeedbackTest, WithoutALimitTheFilterIsDeferred) {
    ASSERT_NO_FATAL_FAILURE(init(false, TJoinOp::INNER_JOIN, false, /*with_runtime_filter=*/true));
    EXPECT_NE(nullptr, arm_through_the_joiner());
}

// Covers: a JOIN that keeps the probe rows without a pair has no filter to keep back, and the profile says that the
//         type is the reason.
TEST_F(ProbeFeedbackTest, FilterOfAJoinThatKeepsUnmatchedProbeRowsIsNotDeferred) {
    ASSERT_NO_FATAL_FAILURE(init(false, TJoinOp::LEFT_OUTER_JOIN, false, /*with_runtime_filter=*/true));
    EXPECT_EQ(nullptr, arm_through_the_joiner());
    ASSERT_NE(nullptr, not_deferred_section());
    EXPECT_EQ("not deferred: join type is not supported", *not_deferred_section()->get_info_string("State"));
}

// Covers: the filter of a layout that splits it by partition of the build side is not deferred.
TEST_F(ProbeFeedbackTest, FilterOfAPipelineLevelMultiPartitionedLayoutIsNotDeferred) {
    ASSERT_NO_FATAL_FAILURE(init(false, TJoinOp::INNER_JOIN, false, /*with_runtime_filter=*/true));
    _rf_desc._layout._pipeline_level_multi_partitioned = true;
    EXPECT_EQ(nullptr, arm_through_the_joiner());
    ASSERT_NE(nullptr, not_deferred_section());
    EXPECT_EQ("not deferred: filter is partitioned", *not_deferred_section()->get_info_string("State"));
}

TEST_F(ProbeFeedbackTest, FilterOfAColocateLayoutIsNotDeferred) {
    ASSERT_NO_FATAL_FAILURE(init(false, TJoinOp::INNER_JOIN, false, /*with_runtime_filter=*/true));
    _rf_desc._num_colocate_partition = 4;
    EXPECT_EQ(nullptr, arm_through_the_joiner());
    ASSERT_NE(nullptr, not_deferred_section());
    EXPECT_EQ("not deferred: filter is partitioned", *not_deferred_section()->get_info_string("State"));
}

// Covers: the build side starts to spill, which resets the hash table and frees its key columns. A deferred filter
//         that is not decided is refused first, and does not build from the keys afterwards.
TEST_F(ProbeFeedbackTest, SpillStartRefusesTheUndecidedFilterBeforeTheHashTableIsReset) {
    ASSERT_NO_FATAL_FAILURE(init(false));
    auto deferred = arm_deferred_filter();
    ASSERT_TRUE(deferred->holds_keys());
    auto build = std::make_unique<pipeline::SpillableHashJoinBuildOperator>(&_factory, 1, "build", 1, 0, _joiner,
                                                                            nullptr, TJoinDistributionMode::BROADCAST);

    ASSERT_OK(build->_convert_hash_map_to_chunk().status());

    EXPECT_EQ(pipeline::DeferredRfDecider::State::REFUSED, deferred->state());
    EXPECT_FALSE(deferred->holds_keys());
    // A window that arrives later finds the decision made.
    deferred->observe_scan_rows(262144);
    EXPECT_EQ(pipeline::DeferredRfDecider::State::REFUSED, deferred->submit(expensive_window()));
    EXPECT_EQ(nullptr, _published);
}

// Covers: the filter waits with its keys because windows showed no gain, and then the build side starts to spill. The
//         keys are released before the hash table is reset, and the windows that come later build nothing.
TEST_F(ProbeFeedbackTest, SpillStartWhileTheFilterWaitsRefusesItAndReleasesTheKeys) {
    ASSERT_NO_FATAL_FAILURE(init(false));
    auto deferred = arm_deferred_filter();
    deferred->observe_scan_rows(262144);
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(pipeline::DeferredRfDecider::State::WAITING, deferred->submit(no_gain_window()));
    }
    ASSERT_TRUE(deferred->holds_keys());
    ASSERT_EQ(10, deferred->evaluations());
    auto build = std::make_unique<pipeline::SpillableHashJoinBuildOperator>(&_factory, 1, "build", 1, 0, _joiner,
                                                                            nullptr, TJoinDistributionMode::BROADCAST);

    ASSERT_OK(build->_convert_hash_map_to_chunk().status());

    EXPECT_EQ(pipeline::DeferredRfDecider::State::REFUSED, deferred->state());
    EXPECT_FALSE(deferred->holds_keys());
    // A window that would pay off arrives after the spill and finds the filter refused.
    EXPECT_EQ(pipeline::DeferredRfDecider::State::REFUSED, deferred->submit(expensive_window()));
    EXPECT_EQ(10, deferred->evaluations());
    EXPECT_EQ(nullptr, _published);
}

// Covers: a filter that was built before the spill stays built.
TEST_F(ProbeFeedbackTest, SpillStartKeepsABuiltFilter) {
    ASSERT_NO_FATAL_FAILURE(init(false));
    auto deferred = arm_deferred_filter();
    deferred->observe_scan_rows(262144);
    ASSERT_EQ(pipeline::DeferredRfDecider::State::BUILT, deferred->submit(expensive_window()));
    ASSERT_NE(nullptr, _published);
    auto build = std::make_unique<pipeline::SpillableHashJoinBuildOperator>(&_factory, 1, "build", 1, 0, _joiner,
                                                                            nullptr, TJoinDistributionMode::BROADCAST);

    ASSERT_OK(build->_convert_hash_map_to_chunk().status());

    EXPECT_EQ(pipeline::DeferredRfDecider::State::BUILT, deferred->state());
}

// Covers: the hash table is closed with a filter that waits.
TEST_F(ProbeFeedbackTest, ClosingTheJoinRefusesTheUndecidedFilter) {
    ASSERT_NO_FATAL_FAILURE(init(false));
    auto deferred = arm_deferred_filter();
    _joiner->close(_state.get());
    EXPECT_EQ(pipeline::DeferredRfDecider::State::REFUSED, deferred->state());
    EXPECT_FALSE(deferred->holds_keys());
}

} // namespace starrocks
