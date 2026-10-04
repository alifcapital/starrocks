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

import com.starrocks.catalog.LocalTablet;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.statistics.ColumnMinMaxMgr;
import com.starrocks.sql.optimizer.statistics.IMinMaxStatsMgr;
import com.starrocks.sql.optimizer.statistics.StatsVersion;
import com.starrocks.thrift.TExplainLevel;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The meta-scan rewrite may answer only some aggregates from cached min/max values or tablet row counts.
// We expect every query output to survive: an aggregate that cannot be answered, for example because the
// cached value does not cast to the column type, stays a real aggregate next to the constants.
class AggregateMetaPartialReplacementTest extends PlanTestBase {
    @Test
    void mixedCacheHitAndMissPreserveBothOutputsWithRuleValidation() throws Exception {
        new MockUp<ColumnMinMaxMgr>() {
            @Mock
            public Optional<IMinMaxStatsMgr.ColumnMinMax> getStats(ColumnIdentifier identifier, StatsVersion version) {
                return identifier.getColumnName().getId().equals("v3")
                        ? Optional.of(new IMinMaxStatsMgr.ColumnMinMax("1", "200")) : Optional.empty();
            }
        };

        ExecPlan result = planWithRuleValidation("SELECT MAX(v2), MIN(v3) FROM t0");
        assertEquals(2, result.getOutputExprs().size());
        assertContains(result.getExplainString(TExplainLevel.NORMAL),
                "<slot 4> : 4: max", "<slot 5> : 1", "output: max(2: v2)");
    }

    @Test
    void failedMinCastRetainsAggregateWhileCountFolds() throws Exception {
        cache("not-a-number", "not-a-number");
        proveTabletCounts();

        ExecPlan result = planWithRuleValidation("SELECT MIN(v2), COUNT(*) FROM t0");
        assertEquals(2, result.getOutputExprs().size());
        String plan = result.getExplainString(TExplainLevel.NORMAL);
        assertContains(plan, "output: min(2: v2)");
        assertNumericProjection(plan);
        assertNotContains(plan, "count(", "MetaScan");
    }

    @Test
    void allFailedCastsKeepTheMetaScanPlan() throws Exception {
        cache("not-a-number", "not-a-number");

        ExecPlan result = planWithRuleValidation("SELECT MIN(v2), MAX(v3) FROM t0");
        assertEquals(2, result.getOutputExprs().size());
        String plan = result.getExplainString(TExplainLevel.NORMAL);
        assertContains(plan, "MetaScan", "min(min_v2)", "max(max_v3)");
        assertNotContains(plan, "constant exprs:");
    }

    @Test
    void allFoldedAggregatesStillProduceOneConstantRow() throws Exception {
        cache("1", "200");
        proveTabletCounts();

        ExecPlan result = planWithRuleValidation("SELECT MIN(v2), MAX(v3), COUNT(*) FROM t0");
        assertEquals(3, result.getOutputExprs().size());
        String plan = result.getExplainString(TExplainLevel.NORMAL);
        assertContains(plan, "<slot 4> : 1", "<slot 5> : 200", "constant exprs:");
        assertNumericProjection(plan);
        assertNotContains(plan, "AGGREGATE", "MetaScan", "OlapScanNode");
    }

    private ExecPlan planWithRuleValidation(String sql) throws Exception {
        boolean oldRewrite = connectContext.getSessionVariable().isEnableRewriteSimpleAggToMetaScan();
        int oldPartitionLimit = connectContext.getSessionVariable().getScanOlapPartitionNumLimit();
        boolean oldDebug = connectContext.getSessionVariable().enableOptimizerRuleDebug();
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(true);
        connectContext.getSessionVariable().setScanOlapPartitionNumLimit(0);
        connectContext.getSessionVariable().setEnableOptimizerRuleDebug(true);
        try {
            return getExecPlan(sql);
        } finally {
            connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(oldRewrite);
            connectContext.getSessionVariable().setScanOlapPartitionNumLimit(oldPartitionLimit);
            connectContext.getSessionVariable().setEnableOptimizerRuleDebug(oldDebug);
        }
    }

    private static void cache(String min, String max) {
        new MockUp<ColumnMinMaxMgr>() {
            @Mock
            public Optional<IMinMaxStatsMgr.ColumnMinMax> getStats(ColumnIdentifier identifier, StatsVersion version) {
                return Optional.of(new IMinMaxStatsMgr.ColumnMinMax(min, max));
            }
        };
    }

    private static void proveTabletCounts() {
        new MockUp<LocalTablet>() {
            @Mock
            public long getRowCountAtVersion(long version) {
                return 1L;
            }
        };
    }

    private static void assertNumericProjection(String plan) {
        assertTrue(Pattern.compile("(?m)^\\s*\\|\\s*<slot \\d+> : \\d+\\s*$").matcher(plan).find(), plan);
    }
}
