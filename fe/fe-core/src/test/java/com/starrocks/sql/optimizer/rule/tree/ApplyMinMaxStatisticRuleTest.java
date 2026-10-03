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

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.statistics.IMinMaxStatsMgr;
import com.starrocks.sql.optimizer.statistics.StatsVersion;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Expectations;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class ApplyMinMaxStatisticRuleTest extends PlanTestBase {
    private static void mockMinMax() {
        final IMinMaxStatsMgr minMaxStatsMgr = IMinMaxStatsMgr.internalInstance();
        new Expectations(minMaxStatsMgr) {
            {
                minMaxStatsMgr.getStats((ColumnIdentifier) any, (StatsVersion) any);
                result = Optional.of(new IMinMaxStatsMgr.ColumnMinMax("0", "65535"));
                minTimes = 0;
            }
        };
    }

    @Test
    void groupByScanColumnGetsMinMaxStats() throws Exception {
        mockMinMax();
        String plan = getVerboseExplain("select v1, count(*) from t0 group by v1");
        assertContains(plan, "group by min-max stats:");
    }

    @Test
    void groupByKeysOfBothJoinedScansGetMinMaxStats() throws Exception {
        mockMinMax();
        String plan = getVerboseExplain(
                "select t0.v1, t1.v4, count(*) from t0 join t1 on t0.v2 = t1.v5 group by t0.v1, t1.v4");
        assertContains(plan, "group by min-max stats:");
    }

    @Test
    void groupByKeyOfOneJoinedScanGetsMinMaxStats() throws Exception {
        mockMinMax();
        String plan = getVerboseExplain(
                "select t0.v1, count(*) from t0 join t1 on t0.v2 = t1.v5 group by t0.v1");
        assertContains(plan, "group by min-max stats:");
    }

    @Test
    void globalAggregateHasNoGroupByMinMaxStats() throws Exception {
        mockMinMax();
        String plan = getVerboseExplain("select count(*), sum(v1) from t0");
        assertNotContains(plan, "group by min-max stats:");
    }

    @Test
    void groupByExpressionHasNoMinMaxStats() throws Exception {
        mockMinMax();
        String plan = getVerboseExplain("select v1 + 1, count(*) from t0 group by v1 + 1");
        assertNotContains(plan, "group by min-max stats:");
    }

    @Test
    void queryWithoutAggregateIsPlanned() throws Exception {
        mockMinMax();
        String plan = getVerboseExplain("select t0.v1, t1.v4 from t0 join t1 on t0.v2 = t1.v5");
        assertNotContains(plan, "group by min-max stats:");
    }
}
