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

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.planner.HashJoinNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

// The scan gets a range derived from an OR of its predicate, so that it can skip data. The OR implies the range, so
// the range must not lower the estimate of the scan below the estimate of the OR.
public class OrDerivedRangeEstimateTest extends PlanTestBase {
    private static final long ROWS = 394_000_000L;
    private static StatisticStorage old;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE or_derived (id int, s int, u bigint) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE or_derived_dim (id int, x int) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        OlapTable table = (OlapTable) starRocksAssert.getTable("test", "or_derived");
        setTableStatistics(table, ROWS);
        table.getAllPartitions().forEach(p -> UtFrameUtils.setPartitionVersion(p, 2));
        OlapTable dim = (OlapTable) starRocksAssert.getTable("test", "or_derived_dim");
        setTableStatistics(dim, 1_000_000L);
        dim.getAllPartitions().forEach(p -> UtFrameUtils.setPartitionVersion(p, 2));
        GlobalStateMgr state = connectContext.getGlobalStateMgr();
        old = state.getStatisticStorage();
        state.setStatisticStorage(new StatisticStorage() {
            @Override
            public ColumnStatistic getColumnStatistic(Table t, String column) {
                if (column.equalsIgnoreCase("s")) {
                    return new ColumnStatistic(0, 22, 0, 4, 20);
                }
                if (column.equalsIgnoreCase("u")) {
                    return new ColumnStatistic(1, 1e10, 0, 8, 1e7);
                }
                if (column.equalsIgnoreCase("x")) {
                    return new ColumnStatistic(0, 1e6, 0, 4, 1e6);
                }
                return new ColumnStatistic(0, 1e9, 0, 4, 1e6);
            }

            @Override
            public List<ColumnStatistic> getColumnStatistics(Table t, List<String> columns) {
                return columns.stream().map(c -> getColumnStatistic(t, c)).toList();
            }

            @Override
            public void addColumnStatistic(Table t, String c, ColumnStatistic statistic) {
            }
        });
    }

    @AfterAll
    public static void dropTables() throws Exception {
        connectContext.getGlobalStateMgr().setStatisticStorage(old);
        starRocksAssert.dropTable("or_derived");
        starRocksAssert.dropTable("or_derived_dim");
    }

    private void assertScanRows(String sql, String derived, double share) throws Exception {
        ExecPlan plan = getExecPlan(sql);
        String explain = plan.getExplainString(TExplainLevel.COSTS);
        // The derived range still reaches the scan, so the scan can skip data by it.
        Assertions.assertTrue(explain.contains(derived), explain);
        double expected = ROWS * share;
        Assertions.assertEquals(expected, plan.getScanNodes().get(0).getCardinality(), expected * 0.01, explain);
    }

    private static void collectJoins(PlanNode node, List<PlanNode> joins) {
        if (node instanceof HashJoinNode) {
            joins.add(node);
        }
        for (PlanNode child : node.getChildren()) {
            collectJoins(child, joins);
        }
    }

    @Test
    public void testRangeDerivedFromOrKeepsTheEstimateOfTheOr() throws Exception {
        // s = 2 keeps 1/20 of the rows and s IN (0, 1) 2/20; the scan also gets s IN (2, 0, 1).
        assertScanRows("select count(*) from or_derived where (s = 2 and u > 0) or s in (0, 1)",
                "IN (2, 0, 1)", 0.15);
        assertScanRows("select count(*) from or_derived where (s = 2 and u > 0) or s = 0",
                "IN (2, 0)", 0.10);
    }

    @Test
    public void testRangeNarrowsItsColumn() throws Exception {
        String explain = getCostExplain("select s from or_derived where (s = 2 and u > 0) or s in (0, 1)");
        // The OR keeps the NDV of s, the derived range narrows it to its three values.
        Assertions.assertTrue(explain.contains("* s-->[0.0, 2.0, 0.0, 4.0, 3.0]"), explain);
    }

    @Test
    public void testRangePushedDownAloneIsEstimated() throws Exception {
        // The OR reads both tables and stays above the join; the scan of or_derived gets only the derived range,
        // and the range is the only estimate of the condition there.
        ExecPlan plan = getExecPlan("select count(*) from or_derived t join or_derived_dim d on t.id = d.id "
                + "where (t.s = 2 and d.x > 0) or t.s in (0, 1)");
        String explain = plan.getExplainString(TExplainLevel.COSTS);
        ScanNode scan = plan.getScanNodes().stream()
                .filter(node -> node.getCardinality() > 10_000_000).findFirst().orElseThrow();
        double expected = ROWS * 0.15;
        Assertions.assertEquals(expected, scan.getCardinality(), expected * 0.01, explain);
        // The OR above the join is estimated on the columns that the range narrowed: s has the values 0, 1 and 2,
        // and the OR keeps nearly all rows of the scan.
        List<PlanNode> joins = new ArrayList<>();
        plan.getFragments().forEach(fragment -> collectJoins(fragment.getPlanRoot(), joins));
        Assertions.assertFalse(joins.isEmpty(), explain);
        Assertions.assertEquals(expected, joins.get(0).getCardinality(), expected * 0.01, explain);
    }
}
