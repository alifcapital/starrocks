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

import com.google.common.collect.Sets;
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.MultiColumnCombinedStatistics;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import mockit.Expectations;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AggregatePushDownWithCostTest extends PlanWithCostTestBase {
    @BeforeEach
    public void before() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0");
        OlapTable t1 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t1");
        OlapTable t2 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t2");
        OlapTable t3 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t3");

        long t0Rows = 1000_000_000L;
        long t1Rows = 1000L;
        long t2Rows = 10_000_000L;
        long t3Rows = 100_000_000L;

        setTableStatistics(t0, t0Rows);
        setTableStatistics(t1, t1Rows);
        setTableStatistics(t2, t2Rows);
        setTableStatistics(t3, t3Rows);

        StatisticStorage ss = GlobalStateMgr.getCurrentState().getStatisticStorage();
        new Expectations(ss) {
            {
                ss.getColumnStatistic(t0, "v1");
                result = new ColumnStatistic(1, 2, 0, 4, t0Rows / 300.0);
                minTimes = 0;
                ss.getColumnStatistic(t0, "v2");
                result = new ColumnStatistic(1, 4000000, 0, 4, t0Rows / 2000.0);
                minTimes = 0;
                ss.getColumnStatistic(t0, "v3");
                result = new ColumnStatistic(1, 2000000, 0, 4, t0Rows / 300.0);
                minTimes = 0;

                ss.getColumnStatistic(t1, "v4");
                result = new ColumnStatistic(1, 2, 0, 4, t1Rows);
                minTimes = 0;
                ss.getColumnStatistic(t1, "v5");
                result = new ColumnStatistic(1, 100000, 0, 4, t1Rows / 100.0);
                minTimes = 0;
                ss.getColumnStatistic(t1, "v6");
                result = new ColumnStatistic(1, 200000, 0, 4, t1Rows / 1000.0);
                minTimes = 0;

                ss.getColumnStatistic(t2, "v7");
                result = new ColumnStatistic(1, 2, 0, 4, t2Rows / 200.0);
                minTimes = 0;
                ss.getColumnStatistic(t2, "v8");
                result = new ColumnStatistic(1, 100000, 0, 4, t2Rows / 2000.0);
                minTimes = 0;
                ss.getColumnStatistic(t2, "v9");
                result = new ColumnStatistic(1, 200000, 0, 4, t2Rows / 20000.0);
                minTimes = 0;

                ss.getColumnStatistic(t3, "v10");
                result = new ColumnStatistic(1, 2, 0, 4, t3Rows / 10.0);
                minTimes = 0;
                ss.getColumnStatistic(t3, "v11");
                result = new ColumnStatistic(1, 100000, 0, 4, t3Rows / 200.0);
                minTimes = 0;
                ss.getColumnStatistic(t3, "v12");
                result = new ColumnStatistic(1, 200000, 0, 4, t3Rows / 20000.0);
                minTimes = 0;
            }
        };

        connectContext.getSessionVariable().setCboPushDownAggregateMode(0);
        connectContext.getSessionVariable().setCboPushDownAggregateOnBroadcastJoin(true);
        // These cases check where an aggregate is pushed, on plans of the exact form of a pushed aggregate.
        connectContext.getSessionVariable().setCboPushDownAggregate("global");
    }

    @AfterEach
    public void after() {
        connectContext.getSessionVariable().setCboPushDownAggregate("partial");
    }

    @Test
    public void testAggAfterNonBroadcastJoin() throws Exception {
        String sql;
        String plan;

        // Pushed below the joins the aggregate groups t0 by the join keys (v1, v2): about 3.3 million times 0.5
        // million combinations for 1 billion rows, so it would not reduce the rows and stays above the joins.
        sql = "select " +
                "/*+SET_VAR(cbo_push_down_aggregate_mode=0,cbo_push_down_aggregate_on_broadcast_join=true)*/ sum(v3)\n" +
                "from \n" +
                "    t0 \n" +
                "    join t3 on t0.v1 = t3.v10\n" +
                "    join t2 on t0.v2 = t2.v7\n" +
                "group by t2.v9, t3.v11";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "group by: 1: v1, 2: v2");

        // The join key v2 alone has 0.5 million values for 1 billion rows, so an aggregate on it reduces the rows
        // that reach the join 2000 times and is pushed below the join.
        sql = "select " +
                "/*+SET_VAR(cbo_push_down_aggregate_mode=0,cbo_push_down_aggregate_on_broadcast_join=true)*/ sum(v3)\n" +
                "from t0 join t2 on t0.v2 = t2.v7\n" +
                "group by t2.v9";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  |  group by: 2: v2\n" +
                "  |  \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");
    }

    @Test
    public void testAggOnBroadcastJoin() throws Exception {
        String sql;
        String plan;

        String sqlTemplate1 = "select " +
                "/*+SET_VAR(cbo_push_down_aggregate_mode=%d,cbo_push_down_aggregate_on_broadcast_join=%s)*/ sum(v3)\n" +
                "from \n" +
                "    t0 \n" +
                "    join t1 on t0.v1 = t1.v4\n" +
                "    join t2 on t0.v2 = t2.v7\n" +
                "group by t2.v9, t1.v5";
        String sqlTemplate2 = "select " +
                "/*+SET_VAR(cbo_push_down_aggregate_mode=%d,cbo_push_down_aggregate_on_broadcast_join=%s)*/ sum(v3)\n" +
                "from \n" +
                "    t0 \n" +
                "    join t2 on t0.v2 = t2.v7\n" +
                "    join t1 on t0.v1 = t1.v4\n" +
                "group by t2.v9, t1.v5";

        // t1 has 1000 rows, so t0 join t1 is a small broadcast join. The aggregate is not pushed right below it, and
        // above it the key (v2, v5) has as many groups as the 0.3 million rows of the join.
        sql = String.format(sqlTemplate1, 0, "true");
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "group by: 2: v2, 5: v5");
        assertNotContains(plan, "group by: 1: v1, 2: v2");

        // Without the broadcast join handling the candidate is t0 grouped by (v1, v2), which does not reduce.
        sql = String.format(sqlTemplate1, 0, "false");
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "group by: 1: v1, 2: v2");

        sql = String.format(sqlTemplate2, 0, "true");
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "group by: 2: v2, 8: v5");
        assertNotContains(plan, "group by: 1: v1, 2: v2");

        sql = String.format(sqlTemplate2, 1, "false");
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 1: v1, 2: v2\n" +
                "  |  \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");

        sql = String.format(sqlTemplate2, 2, "false");
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: 1: v1, 2: v2\n" +
                "  |  \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");
    }

    @Test
    public void testAggAboveSmallBroadcastJoin() throws Exception {
        // Right below the small broadcast join t0 join t1 the aggregate would group t0 by v1, and it is never pushed
        // there. Above that join it groups by (v5, v6) of t1, about 10 groups for 0.3 million rows, so it is pushed
        // to that place, below the join with t2.
        String sql = "select " +
                "/*+SET_VAR(cbo_push_down_aggregate_mode=0,cbo_push_down_aggregate_on_broadcast_join=true)*/ sum(v3)\n" +
                "from t0 join t1 on t0.v1 = t1.v4 join t2 on t1.v6 = t2.v8\n" +
                "group by t1.v5";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "group by: 1: v1\n");
        assertContains(plan, "  |  group by: 5: v5, 6: v6\n" +
                "  |  \n" +
                "  4:HASH JOIN");
    }

    @Test
    public void testAggOnUnion() throws Exception {
        String sql;
        String plan;

        // Each child of the UNION joins t0 with t1, a small broadcast join, and then with t2. The aggregate is not
        // pushed right below the small broadcast join, and above it the key (v2, v5) has as many groups as rows, so
        // in auto mode nothing is pushed into the children. The rewrite of a pushed aggregate through a UNION is
        // covered by the preagg-pushdown and agg-pushdown plan files of AggregatePushDownTest.
        sql = "select sum(v2)\n" +
                "from (\n" +
                "select v2, v5\n" +
                "from \n" +
                "    t0 \n" +
                "    join t1 on t0.v1 = t1.v4\n" +
                "    join t2 on t0.v2 = t2.v7\n" +
                "union\n" +
                "select v2, v5\n" +
                "from \n" +
                "    t0 \n" +
                "    join t1 on t0.v1 = t1.v4\n" +
                "    join t2 on t0.v2 = t2.v7\n" +
                ") t\n" +
                "group by v5";
        plan = getFragmentPlan(sql);
        Assertions.assertEquals(3, StringUtils.countMatches(plan, ":AGGREGATE "), plan);
    }

    @Test
    public void testAggWithMultiColumnStats() throws Exception {
        StatisticStorage ss = GlobalStateMgr.getCurrentState().getStatisticStorage();
        OlapTable t0 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test").getTable("t0");
        OlapTable t3 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test").getTable("t3");

        new Expectations(ss) {
            {
                // for multi-column statistics
                ss.getMultiColumnCombinedStatistics(t0.getId());
                result = new MultiColumnCombinedStatistics(Sets.newHashSet(t0.getColumn("v1").getUniqueId(),
                        t0.getColumn("v3").getUniqueId()), 5555555);
                minTimes = 0;

                ss.getMultiColumnCombinedStatistics(t3.getId());
                result = new MultiColumnCombinedStatistics(Sets.newHashSet(t3.getColumn("v10").getUniqueId(),
                        t3.getColumn("v11").getUniqueId()), 100_000);
                minTimes = 0;

            }
        };

        String sql = "select count(1) from t0 group by v1, v3";
        String plan = getCostExplain(sql);
        assertCContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: count[(1); args: TINYINT; result: BIGINT; args nullable: false; result nullable: false]\n" +
                "  |  group by: [1: v1, BIGINT, true], [3: v3, BIGINT, true]\n" +
                "  |  cardinality: 5555555\n" +
                "  |  column statistics: \n" +
                "  |  * v1-->[1.0, 2.0, 0.0, 4.0, 3333333.3333333335] ESTIMATE\n" +
                "  |  * v3-->[1.0, 2000000.0, 0.0, 4.0, 3333333.3333333335] ESTIMATE\n" +
                "  |  * count-->[0.0, 1.0E9, 0.0, 8.0, 5555555.0] ESTIMATE");

        sql = "select count(1) from t0 group by v1, abs(v3)";
        plan = getCostExplain(sql);
        assertCContains(plan, "3:Project\n" +
                "  |  output columns:\n" +
                "  |  5 <-> [5: count, BIGINT, false]\n" +
                "  |  cardinality: 1000000000");

        sql = "select count(1) from t3 group by v10, v11, v12";
        plan = getCostExplain(sql);
        assertCContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  aggregate: count[(1); args: TINYINT; result: BIGINT; args nullable: false; result nullable: false]\n" +
                "  |  group by: [1: v10, BIGINT, true], [2: v11, BIGINT, true], [3: v12, BIGINT, true]\n" +
                "  |  cardinality: 100000000");

        connectContext.getSessionVariable().setCboPushDownAggregateOnBroadcastJoin(false);
        sql = "select sum(t3.v12) from t3 join t1 on t3.v10=t1.v4 group by t3.v11";
        plan = getFragmentPlan(sql);
        assertCContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v12)\n" +
                "  |  group by: 1: v10, 2: v11\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3");

        // (v10, v11) has 0.1 million groups, and v12 adds 5000 values: the key has as many groups as rows.
        sql = "select sum(t3.v12) from t3 join t1 on t3.v10=t1.v4 group by t3.v11, t3.v12";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "group by: 1: v10, 2: v11, 3: v12");
        plan = getCostExplain(sql);
        assertCContains(plan, "column statistics: \n" +
                "     * v10-->[1.0, 2.0, 0.0, 4.0, 1.0E7] ESTIMATE\n" +
                "     * v11-->[1.0, 100000.0, 0.0, 4.0, 500000.0] ESTIMATE\n" +
                "     * v12-->[1.0, 200000.0, 0.0, 4.0, 5000.0] ESTIMATE\n" +
                "     multi-column statistics: \n" +
                "     * [v10, v11]-->[ndv=100000]");
        connectContext.getSessionVariable().setCboPushDownAggregateOnBroadcastJoin(true);
    }

    @Test
    public void testPushDownAggCaseWhenWithNonNullConstElseDoesNotCrash() throws Exception {
        // A multi-WHEN CASE with non-constant THENs and a non-null constant ELSE used to trip a
        // Preconditions.checkState in PushDownAggregateRewriter.rewriteProject (IllegalStateException)
        // when the aggregate was pushed below a broadcast join (large probe t0, small build t1). The
        // collector only validated THEN clauses; the non-null ELSE must also forbid push-down.
        String sql = "select t0.v1, " +
                "sum(case when t0.v3 > 0 then t0.v2 when t0.v3 < 0 then t0.v2 + 1 else 5 end) " +
                "from t0 join [broadcast] t1 on t0.v1 = t1.v4 group by t0.v1";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("t0"), plan);
    }
}
