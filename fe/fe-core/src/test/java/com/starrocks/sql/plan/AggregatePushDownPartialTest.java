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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// An aggregate pushed below a join is a partial aggregate: a streaming local phase with no exchange and no global
// phase, under an aggregate above the join that merges its rows.
public class AggregatePushDownPartialTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        connectContext.getSessionVariable().setCboPushDownAggregateMode(1);
        connectContext.getSessionVariable().setCboPushDownAggregate("partial");
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setCboPushDownAggregateMode(-1);
        PlanTestBase.afterClass();
    }

    @Test
    public void testPushedAggregateIsStreamingLocalPhase() throws Exception {
        String sql = "select t1.v5, sum(t0.v2), max(t0.v3), min(t0.v3) from t0 join t1 on t0.v1 = t1.v4 group by t1.v5";
        String plan = getFragmentPlan(sql);
        // The streaming phase is right on the scan, with no exchange between them, and the aggregate above the join
        // merges its rows.
        assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: min(3: v3), sum(2: v2), max(3: v3)\n" +
                "  |  group by: 1: v1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0");
        assertContains(plan, "  |  output: sum(11: sum), max(12: max), min(10: min)\n" +
                "  |  group by: 5: v5");
    }

    @Test
    public void testExactFormStillAvailable() throws Exception {
        connectContext.getSessionVariable().setCboPushDownAggregate("global");
        try {
            String sql = "select t1.v5, sum(t0.v2) from t0 join t1 on t0.v1 = t1.v4 group by t1.v5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                    "  |  output: sum(2: v2)\n" +
                    "  |  group by: 1: v1\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregate("partial");
        }
    }

    @Test
    public void testPartialSurvivesPruningOfGroupByKeys() throws Exception {
        // The key of the pushed aggregate would hold the constant c, which is not a key of any group. Whichever
        // rule drops it, the aggregate stays partial.
        String sql = "select x.c, sum(x.v2) from (select v1, v2, 1 as c from t0) x join t1 on x.v1 = t1.v4 " +
                "group by x.c";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: 1: v1\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }
}
