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

import com.starrocks.common.FeConstants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Plan tests for the comparisons of date_format with a string. The inverse of an ordered format replaces the condition,
 * in the filter and in the list of columns of a query, so it holds only where it is exact: a format whose strings are
 * not ordered, or that the BE reads as another date than the start of the period, keeps date_format in the plan, and no
 * partition is pruned by it. The tables have a partition for 2012, where a wrong reading of a two digit year would put
 * the rows.
 */
public class DateFormatRewriteTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("CREATE TABLE `df_time` (\n"
                + "  `id` bigint NOT NULL,\n"
                + "  `ts` datetime NOT NULL,\n"
                + "  `v` bigint NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`id`, `ts`)\n"
                + "PARTITION BY RANGE(`ts`)\n"
                + "(PARTITION p2012 VALUES [('2012-01-01'), ('2013-01-01')),\n"
                + "PARTITION p202607 VALUES [('2026-07-01'), ('2026-08-01')),\n"
                + "PARTITION p202608 VALUES [('2026-08-01'), ('2026-09-01')),\n"
                + "PARTITION p202609 VALUES [('2026-09-01'), ('2026-10-01')))\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");
        starRocksAssert.withTable("CREATE TABLE `df_day` (\n"
                + "  `id` bigint NOT NULL,\n"
                + "  `d` date NOT NULL,\n"
                + "  `v` bigint NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`id`, `d`)\n"
                + "PARTITION BY RANGE(`d`)\n"
                + "(PARTITION p2012 VALUES [('2012-01-01'), ('2013-01-01')),\n"
                + "PARTITION p202607 VALUES [('2026-07-01'), ('2026-08-01')),\n"
                + "PARTITION p202608 VALUES [('2026-08-01'), ('2026-09-01')),\n"
                + "PARTITION p202609 VALUES [('2026-09-01'), ('2026-10-01')))\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");
    }

    @Test
    public void testOrderedFormatReplacesTheConditionAndPrunes() throws Exception {
        String plan = getFragmentPlan(
                "select * from df_time where date_format(ts, '%Y-%m-%dT%H:%i:%s') >= '2026-08-01T00:00:00'");
        assertContains(plan, "partitions=2/4");
        assertNotContains(plan, "date_format");
        plan = getFragmentPlan("select * from df_time where date_format(ts, '%Y-%m-01 00:00:00') = '2026-08-01 00:00:00'");
        assertContains(plan, "partitions=1/4");
        assertNotContains(plan, "date_format");
        // the value in the list of columns is the same condition, NULL included
        plan = getFragmentPlan("select date_format(ts, '%Y-%m-01 00:00:00') = '2026-08-01 00:00:00' from df_time");
        assertNotContains(plan, "date_format");
    }

    @Test
    public void testFormatThatCannotBeInvertedKeepsTheCondition() throws Exception {
        // The strings are not in the order of the dates, or the format does not render a date that the BE reads as the
        // period: no replacement, no pruning, and the rows of 2012 stay.
        String[][] cases = {
                {"date_format(ts, '%d.%m.%Y') = '01.08.2026'"},
                {"date_format(ts, '%e%c%Y') = '182026'"},
                {"date_format(ts, '%d.%m.%y') = '01.08.26'"},
                {"date_format(ts, '%m/%d/%Y %h') = '08/01/2026 05'"},
                {"date_format(ts, '%y-%m') = '26-08'"},
                {"date_format(ts, '%Y-%j') >= '2026-213'"},
                {"date_format(ts, '%W') = 'Saturday'"}};
        for (String[] condition : cases) {
            String plan = getFragmentPlan("select * from df_time where " + condition[0]);
            assertContains(plan, "date_format");
            assertContains(plan, "partitions=4/4");
            plan = getFragmentPlan("select " + condition[0] + " from df_time");
            assertContains(plan, "date_format");
        }
    }

    @Test
    public void testCastOfADateFormatThatTheBackendReadsOtherwiseHasNoBounds() throws Exception {
        // 10 digits are read with a year of two digits, and a month without a day gives NULL: the cast is not the start
        // of the period of the string, so nothing of it prunes, whatever the inverse of the string says
        String[] conditions = {
                "cast(date_format(ts, '%Y%m%d%H') as datetime) >= '2026-08-01 00:00:00'",
                "cast(date_format(ts, '%Y-%m') as datetime) = '2026-08-01 00:00:00'",
                "cast(date_format(ts, '%Y') as datetime) = '2026-01-01 00:00:00'",
                "cast(date_format(ts, '%Y%m') as datetime) >= '2026-08-01 00:00:00'"};
        for (String condition : conditions) {
            String plan = getFragmentPlan("select * from df_time where " + condition);
            assertContains(plan, "date_format");
            assertContains(plan, "partitions=4/4");
            plan = getFragmentPlan("select " + condition + " from df_time");
            assertContains(plan, "date_format");
        }
    }

    @Test
    public void testCastOfADateFormatThatTheBackendReadsAsThePeriodPrunesTheScan() throws Exception {
        // The condition stays, as the cast can give NULL, and the bounds on the column let the scan skip partitions
        String plan = getFragmentPlan(
                "select * from df_time where cast(date_format(ts, '%Y-%m-01 00:00:00') as datetime) = '2026-08-01 00:00:00'");
        assertContains(plan, "partitions=1/4");
        assertContains(plan, "date_format");
        // a month that starts on or after the 15th of August is September or later
        plan = getFragmentPlan("select * from df_time where cast(date_format(ts, '%Y-%m-01 00:00:00') as datetime) "
                + ">= '2026-08-15 00:00:00'");
        assertContains(plan, "partitions=1/4");
        assertContains(plan, "date_format");
        // <> bounds the months on both sides of the month
        plan = getFragmentPlan("select * from df_time where cast(date_format(ts, '%Y-%m-01 00:00:00') as datetime) "
                + "!= '2026-08-01 00:00:00'");
        assertContains(plan, "partitions=3/4");
        assertContains(plan, "date_format");
        // a time inside a month is the string of no month, and every row differs from it
        plan = getFragmentPlan("select * from df_time where cast(date_format(ts, '%Y-%m-01 00:00:00') as datetime) "
                + "!= '2026-08-15 00:00:00'");
        assertContains(plan, "partitions=4/4");
    }

    @Test
    public void testFormatBelowTheDayOnADate() throws Exception {
        // A DATE renders as its midnight. A constant with a time of the day is no date: the equality holds for no row, the
        // inequality for every row, and a range rounds to the day. The point on the date would be another condition.
        String plan = getFragmentPlan("select * from df_day where date_format(d, '%Y-%m-%d %H:00:00') = '2026-08-01 00:00:00'");
        assertContains(plan, "2: d = '2026-08-01'");
        assertNotContains(plan, "date_format");
        plan = getFragmentPlan("select * from df_day where date_format(d, '%Y-%m-%d %H:00:00') = '2026-08-01 05:00:00'");
        assertContains(plan, "0:EMPTYSET");
        plan = getFragmentPlan("select * from df_day where date_format(d, '%Y-%m-%d %H:00:00') != '2026-08-01 05:00:00'");
        assertContains(plan, "date_format");
        assertContains(plan, "partitions=4/4");
        plan = getFragmentPlan("select * from df_day where date_format(d, '%Y-%m-%d %H:00:00') >= '2026-08-01 05:00:00'");
        assertContains(plan, "2: d >= '2026-08-02'");
        assertNotContains(plan, "date_format");
        plan = getFragmentPlan("select * from df_day where date_format(d, '%Y-%m-%d %H:00:00') < '2026-08-01 05:00:00'");
        assertContains(plan, "2: d < '2026-08-02'");
        // the value in the list of columns
        plan = getFragmentPlan("select date_format(d, '%Y-%m-%d %H:00:00') = '2026-08-01 05:00:00' from df_day");
        assertNotContains(plan, "date_format");
        plan = getFragmentPlan("select date_format(d, '%Y-%m-%d %H:00:00') != '2026-08-01 05:00:00' from df_day");
        assertContains(plan, "date_format");
    }
}
