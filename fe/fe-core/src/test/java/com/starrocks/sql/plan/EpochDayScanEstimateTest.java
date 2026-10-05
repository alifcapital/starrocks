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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

// A filter on a function of a column is estimated by the bound on the column that follows from it, whatever form
// the rewrite rules give the filter, and the bound that the scan holds to skip files is not counted again.
public class EpochDayScanEstimateTest extends PlanTestBase {
    private static final long ROWS = 1_928_199_653L;
    private static final double TS_MIN = Utils.getLongFromDateTime(LocalDateTime.of(2024, 1, 1, 0, 0));
    private static final double TS_MAX = Utils.getLongFromDateTime(LocalDateTime.of(2024, 12, 31, 0, 0));
    private static StatisticStorage old;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE epoch_days (id int, d int, ts datetime) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        OlapTable table = (OlapTable) starRocksAssert.getTable("test", "epoch_days");
        setTableStatistics(table, ROWS);
        table.getAllPartitions().forEach(p -> UtFrameUtils.setPartitionVersion(p, 2));
        GlobalStateMgr state = connectContext.getGlobalStateMgr();
        old = state.getStatisticStorage();
        state.setStatisticStorage(new StatisticStorage() {
            @Override
            public ColumnStatistic getColumnStatistic(Table ignored, String column) {
                if (column.equalsIgnoreCase("d")) {
                    // 2017-01-31 .. 2026-10-01
                    return new ColumnStatistic(17197, 20727, 0, 4, 805);
                }
                if (column.equalsIgnoreCase("ts")) {
                    return new ColumnStatistic(TS_MIN, TS_MAX, 0, 8, 366);
                }
                return new ColumnStatistic(0, 1e9, 0, 4, 1e8);
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
    public static void dropEpochDays() throws Exception {
        connectContext.getGlobalStateMgr().setStatisticStorage(old);
        starRocksAssert.dropTable("epoch_days");
    }

    @Test
    public void testFilterOnEpochDayExpressionKeepsTheShareOfTheRange() throws Exception {
        // SimplifiedDateColumnPredicateRule turns DATE(...) BETWEEN into two comparisons of days_add(...), and the
        // scan also gets the bounds on d. 2025-01-01 is day 20089, the column ends at day 20727.
        String sql = "select id from epoch_days "
                + "where date(date_add('1970-01-01', d)) between '2025-01-01' and '2026-12-31'";
        ExecPlan plan = getExecPlan(sql);
        String explain = plan.getExplainString(TExplainLevel.COSTS);
        Assertions.assertTrue(explain.contains(">= 20089"), explain);
        double expected = (double) ROWS * (20727 - 20089) / (20727 - 17197);
        Assertions.assertEquals(expected, plan.getScanNodes().get(0).getCardinality(), expected * 0.02, explain);
    }

    @Test
    public void testDateFunctionsInAFilterAreEstimatedByTheColumn() throws Exception {
        // Each filter holds exactly when ts >= '2024-07-01 00:00:00'
        double expected = (double) ROWS * (TS_MAX - Utils.getLongFromDateTime(LocalDateTime.of(2024, 7, 1, 0, 0)))
                / (TS_MAX - TS_MIN);
        for (String filter : List.of(
                "hours_add(ts, 1) >= '2024-07-01 01:00:00'",
                "months_add(ts, 1) >= '2024-08-01 00:00:00'",
                "years_sub(ts, 1) >= '2023-07-01 00:00:00'",
                "last_day(ts) >= '2024-07-31'")) {
            ExecPlan plan = getExecPlan("select id from epoch_days where " + filter);
            Assertions.assertEquals(expected, plan.getScanNodes().get(0).getCardinality(), expected * 0.02,
                    filter + "\n" + plan.getExplainString(TExplainLevel.COSTS));
        }
    }
}
