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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.ImmutableList;
import com.starrocks.schema.MTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A transparent materialized view puts its definition into the plan when some partitions of the view are stale, and
 * the rewrite runs after the optimizer has looked for the readers of VALUES in the query. A reference table that the
 * definition writes as VALUES is read by the join of the definition, so it must get its statistics like a reference
 * table that the query writes itself. A VALUES that nothing reads must not get them because of the rewrite.
 */
public class ValuesStatisticsMaterializedViewTest extends MVTestBase {
    private static MTable base;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ValuesStatisticsCalls.install();
        base = new MTable("values_mv_base", "k1",
                ImmutableList.of("k1 INT", "k2 string", "v1 INT"),
                "k1",
                ImmutableList.of(
                        "PARTITION `p1` VALUES LESS THAN ('3')",
                        "PARTITION `p2` VALUES LESS THAN ('6')"));
    }

    // The view has p1 refreshed and p2 stale, so a query on it reads the view for p1 and the definition for p2.
    private void withStaleView(String definition, StarRocksAssert.ExceptionRunnable runner) {
        starRocksAssert.withTable(base, () -> {
            cluster.runSql("test", "insert into values_mv_base values (1, '1', 1), (4, '2', 1)");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 PARTITION BY (k1) "
                            + "DISTRIBUTED BY HASH(k1) REFRESH DEFERRED MANUAL "
                            + "PROPERTIES ('transparent_mv_rewrite_mode' = 'true') AS " + definition,
                    () -> {
                        starRocksAssert.refreshMvPartition(
                                "REFRESH MATERIALIZED VIEW mv0 PARTITION START ('1') END ('3')");
                        runner.run();
                    });
        });
    }

    private static final String VALUES_VIEW = "SELECT b.k1, b.k2, d.district FROM values_mv_base b "
            + "JOIN (VALUES ('1', 101), ('2', 102), ('4', 103)) AS d(name, district) ON b.k2 = d.name";
    private static final String PLAIN_VIEW = "SELECT k1, k2, v1 FROM values_mv_base";

    private static OptExpression find(OptExpression plan, Class<?> operator) {
        if (operator.isInstance(plan.getOp())) {
            return plan;
        }
        for (OptExpression input : plan.getInputs()) {
            OptExpression found = find(input, operator);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private ExecPlan plan(String sql) throws Exception {
        ExecPlan plan = UtFrameUtils.getFragmentPlanWithTrace(connectContext, sql, null).second.first;
        // The plan reads the view and the base table of its definition: the transparent rewrite has applied.
        String text = plan.getExplainString(TExplainLevel.NORMAL);
        Assertions.assertTrue(text.contains("mv0") && text.contains("values_mv_base"), text);
        return plan;
    }

    private ColumnStatistic valuesColumn(ExecPlan plan, int position) {
        OptExpression values = find(plan.getPhysicalPlan(), PhysicalValuesOperator.class);
        Assertions.assertNotNull(values, plan.getExplainString(TExplainLevel.COSTS));
        ColumnRefOperator column = ((PhysicalValuesOperator) values.getOp()).getColumnRefSet().get(position);
        return values.getStatistics().getColumnStatistic(column);
    }

    @Test
    public void testValuesOfTheViewDefinitionHaveStatistics() throws Exception {
        withStaleView(VALUES_VIEW, () -> {
            String sql = "select district, count(*) from mv0 group by district";
            Assertions.assertTrue(ValuesStatisticsCalls.during(() -> plan(sql)) > 0, sql);

            ExecPlan plan = plan(sql);
            ColumnStatistic name = valuesColumn(plan, 0);
            Assertions.assertEquals(3, name.getDistinctValuesCount());
            ColumnStatistic district = valuesColumn(plan, 1);
            Assertions.assertEquals(3, district.getDistinctValuesCount());
            Assertions.assertEquals(101, district.getMinValue());
            Assertions.assertEquals(103, district.getMaxValue());
        });
    }

    @Test
    public void testViewWithoutValuesComputesNothing() throws Exception {
        withStaleView(PLAIN_VIEW, () -> {
            String sql = "select k2, count(*) from mv0 group by k2";
            Assertions.assertEquals(0, ValuesStatisticsCalls.during(() -> plan(sql)), sql);
            Assertions.assertNull(find(plan(sql).getPhysicalPlan(), PhysicalValuesOperator.class));
        });
    }

    @Test
    public void testValuesOfTheQueryWithoutReaderAreNotComputedAfterTheRewrite() throws Exception {
        withStaleView(PLAIN_VIEW, () -> {
            // Nothing reads the list: the view, the definition and the list are only the inputs of a UNION ALL.
            String sql = "select k1, k2, v1 from mv0 union all select * from (values (7, 'x', 1)) t(a, b, c)";
            Assertions.assertEquals(0, ValuesStatisticsCalls.during(() -> plan(sql)), sql);
        });
    }

    @Test
    public void testQueryWithoutTheRewriteComputesNothing() throws Exception {
        // The query does not read a view, so no rewrite puts a definition into the plan.
        String sql = "select * from (values (7, 'x', 1)) t(a, b, c)";
        Assertions.assertEquals(0, ValuesStatisticsCalls.during(() -> UtFrameUtils.getFragmentPlanWithTrace(
                connectContext, sql, null)), sql);
    }
}
