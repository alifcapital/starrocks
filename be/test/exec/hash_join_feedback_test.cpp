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

#include "column/fixed_length_column.h"
#include "exec/hash_join_components.h"
#include "exec/hash_joiner.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"
#include "exec/pipeline/hashjoin/local_runtime_filter_feedback.h"
#include "exprs/column_ref.h"
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

} // namespace starrocks
