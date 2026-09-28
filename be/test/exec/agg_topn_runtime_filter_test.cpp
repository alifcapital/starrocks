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

#include <limits>

#include "common/object_pool.h"
#include "exec/agg_runtime_filter_builder.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"
#include "testutil/column_test_helper.h"

namespace starrocks {

// Tests use the normal -fno-access-control test build to seed an empty heap, then exercise
// the production incremental updater. NULLs are distinct groups on other grouping columns.
TEST(AggTopNRuntimeFilterTest, WaitsForKNonNullCandidates) {
    for (bool asc : {false, true}) {
        ObjectPool pool;
        RuntimeFilterBuildDescriptor desc;
        desc._build_expr_order = 0;
        desc._limit = 3;
        desc._is_asc = asc;
        desc._is_nulls_first = false;
        AggTopNRuntimeFilterBuilder builder(&desc, TYPE_INT);
        auto* rf = MinMaxRuntimeFilter<TYPE_INT>::create_full_range_with_null(&pool);
        builder._runtime_filter = rf;
        if (asc) {
            builder._heap_builder = pool.add(new THeapBuilder<TYPE_INT, std::less<int32_t>>(std::less<int32_t>()));
        } else {
            builder._heap_builder =
                    pool.add(new THeapBuilder<TYPE_INT, std::greater<int32_t>>(std::greater<int32_t>()));
        }
        builder.update({ColumnTestHelper::build_nullable_column(std::vector<int32_t>{0, 0, 10},
                                                                std::vector<uint8_t>{1, 1, 0})},
                       Filter{1, 1, 1});
        EXPECT_EQ(std::numeric_limits<int32_t>::lowest(), rf->min());
        EXPECT_EQ(std::numeric_limits<int32_t>::max(), rf->max());
        builder.update(
                {ColumnTestHelper::build_nullable_column(std::vector<int32_t>{20, 30}, std::vector<uint8_t>{0, 0})},
                Filter{1, 1});
        if (asc) {
            EXPECT_EQ(30, rf->max());
        } else {
            EXPECT_EQ(10, rf->min());
        }
    }
}

// A min/max-only filter marks its membership component always-true. Its range still
// needs evaluation without TopN backpressure, both during resampling and between samples.
TEST(AggTopNRuntimeFilterTest, ProbeUsesDynamicRangeWithoutBackpressure) {
    ObjectPool pool;
    RuntimeState state;
    ColumnRef slot(TypeDescriptor(TYPE_INT), 1);
    ExprContext expression(&slot);
    ASSERT_TRUE(expression.prepare(&state).ok());
    ASSERT_TRUE(expression.open(&state).ok());
    RuntimeFilterProbeDescriptor descriptor;
    descriptor._probe_expr_ctx = &expression;
    descriptor._is_stream_build_filter = true;
    auto* filter = MinMaxRuntimeFilter<TYPE_INT>::create_full_range_with_null(&pool);
    filter->update_min_max<false>(2);
    ASSERT_TRUE(filter->always_true());
    descriptor._runtime_filter.store(filter);
    RuntimeFilterProbeCollector collector;
    collector._runtime_state = &state;
    collector._descriptors.emplace(0, &descriptor);
    for (bool storage_pushdown : {false, true}) {
        // A TopN range pushed into page pruning must still filter surviving rows.
        descriptor.set_has_push_down_to_storage(storage_pushdown);
        RuntimeMembershipFilterEvalContext context;
        for (int i = 0; i < 2; ++i) {
            Chunk chunk;
            chunk.append_column(ColumnTestHelper::build_column<int32_t>({1, 2, 3, 4}), 1);
            collector.do_evaluate(&chunk, context);
            ASSERT_EQ(2, chunk.num_rows());
            EXPECT_EQ(1, chunk.get_column_by_slot_id(1)->get(0).get_int32());
            EXPECT_EQ(2, chunk.get_column_by_slot_id(1)->get(1).get_int32());
        }
    }
    // Do not reinterpret an inactive membership filter, or evaluate TopN twice
    // when the caller explicitly runs the separate TopN-only pass.
    for (bool stream : {false, true}) {
        descriptor._is_stream_build_filter = stream;
        RuntimeMembershipFilterEvalContext excluded;
        if (stream) excluded.mode = RuntimeMembershipFilterEvalContext::Mode::M_WITHOUT_TOPN;
        Chunk chunk;
        chunk.append_column(ColumnTestHelper::build_column<int32_t>({1, 2, 3, 4}), 1);
        collector.do_evaluate(&chunk, excluded);
        EXPECT_EQ(4, chunk.num_rows());
    }
}

} // namespace starrocks
