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
import com.starrocks.planner.AggregationNode;
import com.starrocks.planner.PlanFragment;
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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * The groups of a GROUP BY on date_format of a date are the periods of the format in the range of the date, and not the
 * rows or the distinct values of the date. The table has 906,000,000 rows over the days from 2018-11-20 to 2026-03-05,
 * which are 89 months and 9 years.
 */
public class DateFormatGroupByEstimateTest extends PlanTestBase {
    private static final long ROWS = 906_000_000L;
    private static final LocalDate FIRST = LocalDate.of(2018, 11, 20);
    private static final LocalDate LAST = LocalDate.of(2026, 3, 5);
    private static StatisticStorage old;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE fmt_days (id int, d int, day date, ts datetime) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        OlapTable table = (OlapTable) starRocksAssert.getTable("test", "fmt_days");
        setTableStatistics(table, ROWS);
        table.getAllPartitions().forEach(p -> UtFrameUtils.setPartitionVersion(p, 2));
        GlobalStateMgr state = connectContext.getGlobalStateMgr();
        old = state.getStatisticStorage();
        state.setStatisticStorage(new StatisticStorage() {
            @Override
            public ColumnStatistic getColumnStatistic(Table ignored, String column) {
                if (column.equalsIgnoreCase("d")) {
                    return new ColumnStatistic(FIRST.toEpochDay(), LAST.toEpochDay(), 0, 4, 100_000);
                }
                if (column.equalsIgnoreCase("day")) {
                    return new ColumnStatistic(Utils.getLongFromDateTime(FIRST.atStartOfDay()),
                            Utils.getLongFromDateTime(LAST.atStartOfDay()), 0, 4, 100_000);
                }
                if (column.equalsIgnoreCase("ts")) {
                    return new ColumnStatistic(Utils.getLongFromDateTime(FIRST.atStartOfDay()),
                            Utils.getLongFromDateTime(LAST.atTime(12, 0)), 0, 8, 100_000);
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
    public static void dropTable() throws Exception {
        connectContext.getGlobalStateMgr().setStatisticStorage(old);
        starRocksAssert.dropTable("fmt_days");
    }

    private void assertGroups(double expected, String sql) throws Exception {
        ExecPlan plan = getExecPlan(sql);
        List<AggregationNode> aggregations = new ArrayList<>();
        for (PlanFragment fragment : plan.getFragments()) {
            fragment.getPlanRoot().collect(AggregationNode.class, aggregations);
        }
        Assertions.assertFalse(aggregations.isEmpty(), sql);
        for (AggregationNode aggregation : aggregations) {
            Assertions.assertEquals(expected, aggregation.getCardinality(), 0.5,
                    sql + "\n" + plan.getExplainString(TExplainLevel.COSTS));
        }
    }

    @Test
    public void testGroupsOfTheMonthsOfADate() throws Exception {
        assertGroups(89, "select date_format(day, '%Y-%m-01'), count(*) from fmt_days group by 1");
        assertGroups(89, "select date_format(day, '%Y-%m'), count(*) from fmt_days group by 1");
        assertGroups(89, "select date_format(ts, '%Y-%m-01 00:00:00'), count(*) from fmt_days group by 1");
    }

    @Test
    public void testGroupsOfTheYearsAndOfTheFieldsThatRepeat() throws Exception {
        assertGroups(9, "select date_format(day, '%Y'), count(*) from fmt_days group by 1");
        assertGroups(12, "select date_format(day, '%m'), count(*) from fmt_days group by 1");
        assertGroups(7, "select date_format(day, '%W'), count(*) from fmt_days group by 1");
    }

    private double scanRows(String sql) throws Exception {
        return getExecPlan(sql).getScanNodes().get(0).getCardinality();
    }

    @Test
    public void testEqualityAndInequalityOfTheMonthOfADashboardAddUpToTheTable() throws Exception {
        String month = "seconds_add(cast(date_format(last_day(date_add('1970-01-01', d)), '%Y-%m-01 00:00:00') "
                + "as datetime), 0)";
        double equal = scanRows("select id from fmt_days where " + month + " = '2025-08-01 00:00:00'");
        double notEqual = scanRows("select id from fmt_days where " + month + " != '2025-08-01 00:00:00'");
        double negated = scanRows("select id from fmt_days where not (" + month + " = '2025-08-01 00:00:00')");
        // The bounds on the day numbers keep the 31 days of August, which the model of a range counts as 30 of the
        // 2662 days between the ends, and the other months are counted from the 89 distinct values of the strings
        Assertions.assertEquals(ROWS * 31 / 2662, equal, ROWS * 0.003);
        Assertions.assertEquals(ROWS, equal + notEqual, ROWS * 0.02);
        Assertions.assertEquals(ROWS, equal + negated, ROWS * 0.02);
    }

    @Test
    public void testGroupsOfTheWrapperOfADashboard() throws Exception {
        // ADDDATE(DATE_FORMAT(x, '%Y-%m-01 00:00:00'), INTERVAL 0 SECOND) on a day number, the month of its last day
        assertGroups(89, "select seconds_add(cast(date_format(last_day(date_add('1970-01-01', d)), "
                + "'%Y-%m-01 00:00:00') as datetime), 0) as m, count(*) from fmt_days group by m");
    }
}
