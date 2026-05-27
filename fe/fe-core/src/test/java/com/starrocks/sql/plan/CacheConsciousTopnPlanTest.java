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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// Verifies the FE gate (MarkCacheConsciousTopnRule): the agg node is flagged only for
// count(*) DESC LIMIT k over a global aggregation, and left untouched for every shape the
// fused operator cannot handle. The flag surfaces in explain as "cache-conscious topn".
public class CacheConsciousTopnPlanTest extends PlanTestBase {
    private static final String MARKER = "cache-conscious topn";

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @AfterEach
    public void resetSessionVar() {
        connectContext.getSessionVariable().setEnableCacheConsciousTopn(false);
    }

    @Test
    public void testFlagSetForCountStarTopn() throws Exception {
        connectContext.getSessionVariable().setEnableCacheConsciousTopn(true);
        String plan = getFragmentPlan("select v1, count(*) c from t0 group by v1 order by c desc limit 10");
        assertContains(plan, MARKER + ": limit=10");
    }

    @Test
    public void testDisabledByDefault() throws Exception {
        // Session var off: the rule is not even registered, so the plan is identical to today.
        String plan = getFragmentPlan("select v1, count(*) c from t0 group by v1 order by c desc limit 10");
        assertNotContains(plan, MARKER);
    }

    @Test
    public void testRejectAscending() throws Exception {
        // count(*) is monotone, so only the DESC head is the top-n; ASC must not flip.
        connectContext.getSessionVariable().setEnableCacheConsciousTopn(true);
        String plan = getFragmentPlan("select v1, count(*) c from t0 group by v1 order by c asc limit 10");
        assertNotContains(plan, MARKER);
    }

    @Test
    public void testRejectHaving() throws Exception {
        connectContext.getSessionVariable().setEnableCacheConsciousTopn(true);
        String plan = getFragmentPlan(
                "select v1, count(*) c from t0 group by v1 having count(*) > 5 order by c desc limit 10");
        assertNotContains(plan, MARKER);
    }

    @Test
    public void testRejectMultipleAggregations() throws Exception {
        connectContext.getSessionVariable().setEnableCacheConsciousTopn(true);
        String plan = getFragmentPlan(
                "select v1, count(*) c, sum(v2) s from t0 group by v1 order by c desc limit 10");
        assertNotContains(plan, MARKER);
    }

    @Test
    public void testRejectCountDistinct() throws Exception {
        connectContext.getSessionVariable().setEnableCacheConsciousTopn(true);
        String plan = getFragmentPlan(
                "select v1, count(distinct v2) c from t0 group by v1 order by c desc limit 10");
        assertNotContains(plan, MARKER);
    }

    @Test
    public void testRejectOffset() throws Exception {
        connectContext.getSessionVariable().setEnableCacheConsciousTopn(true);
        String plan = getFragmentPlan("select v1, count(*) c from t0 group by v1 order by c desc limit 5, 10");
        assertNotContains(plan, MARKER);
    }

    @Test
    public void testRejectNoLimit() throws Exception {
        connectContext.getSessionVariable().setEnableCacheConsciousTopn(true);
        String plan = getFragmentPlan("select v1, count(*) c from t0 group by v1 order by c desc");
        assertNotContains(plan, MARKER);
    }
}
