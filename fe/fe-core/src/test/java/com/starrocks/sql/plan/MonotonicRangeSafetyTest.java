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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class MonotonicRangeSafetyTest extends PlanTestNoneDBBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        FeConstants.runningUnitTest = true;
        starRocksAssert.withDatabase("range_safety").useDatabase("range_safety");
        for (String[] bounds : new String[][] {
                {"month_shift", "2024-01-29 12:00:00", "2024-01-30 00:00:00"},
                {"quarter_shift", "2024-01-30 12:00:00", "2024-01-31 00:00:00"},
                {"year_shift", "2024-02-28 12:00:00", "2024-02-29 00:00:00"}}) {
            starRocksAssert.withTable("CREATE TABLE " + bounds[0] + " (ts DATETIME, id INT) DUPLICATE KEY(ts) "
                    + "PARTITION BY RANGE(ts) (PARTITION p VALUES [('" + bounds[1] + "'), ('" + bounds[2] + "'))) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        }
        starRocksAssert.withTable("CREATE TABLE ev (d DATE, id INT) DUPLICATE KEY(d) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE fv (id INT, v FLOAT) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE sv (id INT, s VARCHAR(40)) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE times (ts DATETIME, id INT) DUPLICATE KEY(ts) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')");
    }

    @ParameterizedTest
    @ValueSource(strings = {"months_add(ts, 1)", "add_months(ts, 1)", "months_sub(ts, -1)",
            "date_trunc('hour', months_add(ts, 1))"})
    public void testMonthShiftPartitionEnvelope(String expression) throws Exception {
        // ts=January 29 at 23:00 matches, although its image is above both partition endpoint images.
        // Check both the function and its string CAST: the latter cannot use the inverse scan rewrite.
        assertContains(getFragmentPlan("select * from month_shift where " + expression
                + " >= '2024-02-29 20:00:00'"), "partitions=1/1");
        assertContains(getFragmentPlan("select * from month_shift where cast(" + expression
                + " as varchar) >= '2024-02-29 20:00:00'"), "partitions=1/1");
        // We still prune a genuinely disjoint interval instead of disabling this optimization.
        assertContains(getFragmentPlan("select * from month_shift where cast(" + expression
                + " as varchar) >= '2024-03-01 00:00:00'"), "partitions=0/1");
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "quarters_add(ts, 1)|quarter_shift|2024-04-30 20:00:00|2024-05-01 00:00:00",
            "quarters_sub(ts, -1)|quarter_shift|2024-04-30 20:00:00|2024-05-01 00:00:00",
            "years_add(ts, 1)|year_shift|2025-02-28 20:00:00|2025-03-01 00:00:00",
            "years_sub(ts, -1)|year_shift|2025-02-28 20:00:00|2025-03-01 00:00:00"})
    public void testQuarterAndYearShiftPartitionEnvelope(String expression, String table, String keep, String prune)
            throws Exception {
        String sql = "select * from " + table + " where cast(" + expression + " as varchar) >= '";
        assertContains(getFragmentPlan(sql + keep + "'"), "partitions=1/1");
        assertContains(getFragmentPlan(sql + prune + "'"), "partitions=0/1");
    }

    @ParameterizedTest
    @CsvSource({"2024-03-05,2024-03-06,20240304", "2024-03-07,2024-03-08,20240308",
            "2024-03-05,2024-03-06,20240303", "2024-03-05,2024-03-05,20240305"})
    public void testFloatImageDoesNotContradictMatchingTarget(String lo, String hi, int value) throws Exception {
        // Binary32 rounds 20240305 down and 20240307 up. The constant on the receiving side may
        // need rounding too. All these matching rows must survive domain intersection.
        String plan = getFragmentPlan("select ev.id from ev join fv on ev.id=fv.id "
                + "and fv.v=cast(date_format(ev.d, '%Y%m%d') as float) where ev.d between '" + lo
                + "' and '" + hi + "' and fv.v=cast(" + value + " as float)");
        assertNotContains(plan, "EMPTYSET");
        assertContains(plan, "TABLE: ev", "TABLE: fv");
    }

    @Test
    public void testStringJoinRequiresStoredFormat() throws Exception {
        var variables = connectContext.getSessionVariable();
        boolean oldJoin = variables.isEnableStringDateJoinPruning();
        boolean oldPushdown = variables.isEnableStringDatePredicatePushdown();
        String oldFormat = variables.getStringDatePredicateFormat();
        try {
            variables.setEnableStringDateJoinPruning(true);
            variables.setEnableStringDatePredicatePushdown(false);
            variables.setStringDatePredicateFormat("");
            String sql = "select sv.id from sv join times f on sv.id=f.id and f.ts=cast(sv.s as datetime) "
                    + "where sv.s between '2024-03-01 12:00:00' and '2024-03-02 12:00:00'";
            // A stored ISO value 2024-03-01T01:00:00 passes the space-separated string bounds.
            // Inferring the stored format from those bounds would incorrectly reject its matching f.ts.
            assertNotContains(getFragmentPlan(sql), "ts >= '2024-03-01 12:00:00'");
            variables.setStringDatePredicateFormat("%Y-%m-%dT%H:%i:%s");
            assertNotContains(getFragmentPlan(sql), "ts >= '2024-03-01 12:00:00'");
            // The declared format enables the JOIN path independently of inverse string pushdown.
            assertContains(getFragmentPlan(sql.replace("01 12:00:00", "01T12:00:00")
                    .replace("02 12:00:00", "02T12:00:00")), "ts >= '2024-03-01 12:00:00'");
        } finally {
            variables.setEnableStringDateJoinPruning(oldJoin);
            variables.setEnableStringDatePredicatePushdown(oldPushdown);
            variables.setStringDatePredicateFormat(oldFormat);
        }
    }
}
