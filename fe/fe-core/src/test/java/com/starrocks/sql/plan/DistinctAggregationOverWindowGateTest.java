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

package com.starrocks.sql.plan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DistinctAggregationOverWindowGateTest extends PlanTestBase {

    @Test
    public void testOverallWindowsWithoutDistinctAggregationKeepTheWindowPlan() throws Exception {
        String[] queries = {
                "select v1, sum(v2) over (partition by v3) from t0",
                "select sum(v2) over () from t0",
                "select v1, sum(v2) over (partition by v3), max(v1) over (partition by v3) from t0",
                "select sum(v2) over (partition by v3), count(v1) over (partition by v2) from t0",
                "select v1, row_number() over (partition by v3) , count(v2) over (partition by v3) from t0",
        };
        for (String sql : queries) {
            String plan = getFragmentPlan(sql);
            Assertions.assertTrue(plan.contains("ANALYTIC"), sql + "\n" + plan);
            Assertions.assertFalse(plan.contains("JOIN"), sql + "\n" + plan);
            Assertions.assertFalse(plan.contains("MultiCastDataSinks"), sql + "\n" + plan);
        }
    }

    @Test
    public void testDistinctAggregationOverOverallWindowIsStillRewritten() throws Exception {
        String plan = getFragmentPlan("select v1, v2, v3, count(distinct v3) over (partition by v1, v2) from t0");
        Assertions.assertTrue(plan.contains("HASH JOIN"), plan);
    }
}
