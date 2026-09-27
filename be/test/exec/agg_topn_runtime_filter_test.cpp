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

} // namespace starrocks
