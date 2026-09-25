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
import com.starrocks.common.util.StringDateFormat;
import com.starrocks.qe.SessionVariable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class StringDatePredicatePushdownTest extends PlanTestBase {
    private TimeZone savedJvmZone;
    private SessionVariable saved;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("CREATE TABLE string_date_plain (id int, s varchar(32)) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE string_date_source (id int, d date, n int, s varchar(32)) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        for (StringDateFormat format : StringDateFormat.values()) {
            StringBuilder partitions = new StringBuilder();
            for (int month = 1; month <= 4; month++) {
                if (month > 1) {
                    partitions.append(',');
                }
                partitions.append("PARTITION p").append(month).append(" VALUES IN ('")
                        .append(format.format(LocalDateTime.of(2024, month, 1, 0, 0))).append("')");
            }
            starRocksAssert.withTable("CREATE TABLE string_date_" + format.name()
                    + " (s varchar(32), id int) PARTITION BY LIST(s) (" + partitions + ") "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        }
    }

    @BeforeEach
    public void saveVariables() {
        savedJvmZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        saved = connectContext.getSessionVariable();
        connectContext.setSessionVariable((SessionVariable) saved.clone());
        connectContext.getSessionVariable().setEnableStringDatePredicatePushdown(true);
        connectContext.getSessionVariable().setStringDatePredicateFormat("%Y%m%d");
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    public void restoreVariables() {
        TimeZone.setDefault(savedJvmZone);
        connectContext.setSessionVariable(saved);
    }

    @Test
    public void testWhereFunctionPushesToUnpartitionedScan() throws Exception {
        String sql = "select * from string_date_plain where year(cast(s as date)) = 2024";
        connectContext.getSessionVariable().setEnableStringDatePredicatePushdown(false);
        String off = getFragmentPlan(sql);
        assertNotContains(off, "s >= '20240101'");
        connectContext.getSessionVariable().setEnableStringDatePredicatePushdown(true);
        String on = getFragmentPlan(sql);
        assertContains(on, "s >= '20240101'", "s < '20250101'", "CAST(");
        savePlans("unpartitioned", sql, off, on);
    }

    @ParameterizedTest
    @EnumSource(StringDateFormat.class)
    public void testDateFunctionPrunesStringPartitions(StringDateFormat format) throws Exception {
        connectContext.getSessionVariable().setStringDatePredicateFormat(format.getSqlFormat());
        String sql = "select * from string_date_" + format.name()
                + " where date_format(cast(s as date), '%Y%m') = '202403'";
        connectContext.getSessionVariable().setEnableStringDatePredicatePushdown(false);
        String off = getFragmentPlan(sql);
        // LIST pruning can evaluate the CAST on each stored partition value even without raw bounds.
        assertContains(off, "partitions=1/4");
        connectContext.getSessionVariable().setEnableStringDatePredicatePushdown(true);
        String on = getFragmentPlan(sql);
        assertContains(on, "partitions=1/4");
        String plain = getFragmentPlan("select * from string_date_plain "
                + "where date_format(cast(s as date), '%Y%m') = '202403'");
        assertContains(plain, "s >= '" + format.format(LocalDateTime.of(2024, 3, 1, 0, 0)) + "'",
                "s < '" + format.format(LocalDateTime.of(2024, 4, 1, 0, 0)) + "'");
        savePlans("where-" + format.name(), sql, off, on);
    }

    @ParameterizedTest
    @EnumSource(StringDateFormat.class)
    public void testJoinDateIntAndStringToStringPartitions(StringDateFormat format) throws Exception {
        var variables = connectContext.getSessionVariable();
        variables.setStringDatePredicateFormat(format.getSqlFormat());
        variables.setEnableStringDateJoinPruning(true);
        for (String source : new String[] {"date", "int", "varchar"}) {
            String day = source.equals("date") ? "e.d" : source.equals("int") ? "cast(e.n as date)" : "cast(e.s as date)";
            String filter = source.equals("date") ? "e.d between '2024-03-05' and '2024-04-10'"
                    : source.equals("int") ? "e.n between 20240305 and 20240410"
                    : "e.s between '" + format.format(LocalDateTime.of(2024, 3, 5, 0, 0))
                            + "' and '" + format.format(LocalDateTime.of(2024, 4, 10, 0, 0)) + "'";
            String sql = "select f.id from string_date_" + format.name() + " f join string_date_source e "
                    + "on f.id=e.id and cast(f.s as date)=date_trunc('month', " + day + ") where " + filter;
            variables.setEnableStringDatePredicatePushdown(false);
            String off = getFragmentPlan(sql);
            int expectedOff = source.equals("varchar") && (format.hasUtcSuffix()
                    || format.getPrecision() == java.time.temporal.ChronoUnit.MICROS)
                    ? 4 : 2;
            assertContains(off, "partitions=" + expectedOff + "/4");
            variables.setEnableStringDatePredicatePushdown(true);
            String on = getFragmentPlan(sql);
            assertContains(on, "partitions=2/4");
            savePlans("join-" + source + "-" + format.name(), sql, off, on);
        }
    }

    @Test
    public void testExplicitDateCastCoversWholeDay() throws Exception {
        connectContext.getSessionVariable().setStringDatePredicateFormat("%Y-%m-%dT%H:%i:%s.%f");
        String plan = getFragmentPlan("select * from string_date_plain where cast(s as date) = '2024-02-29'");
        assertContains(plan, "s >= '2024-02-29T00:00:00.000000'", "s < '2024-03-01T00:00:00.000000'", "CAST(");
    }

    @Test
    public void testImplicitDatetimeCastAndFractionalBoundary() throws Exception {
        connectContext.getSessionVariable().setStringDatePredicateFormat("%Y-%m-%d %H:%i:%s");
        String plan = getFragmentPlan("select * from string_date_plain "
                + "where s >= cast('2024-03-01 12:00:00.500000' as datetime)");
        assertContains(plan, "s >= '2024-03-01 12:00:01'");
    }

    @Test
    public void testNullAndValueContextsRemainExpressions() throws Exception {
        String plan = getFragmentPlan("select (cast(s as date) = '2024-03-01') is null from string_date_plain");
        assertContains(plan, "CAST(");
        assertNotContains(plan, "s >= '20240301'");
        plan = getFragmentPlan("select * from string_date_plain where cast(s as date) is null or id = 1");
        assertNotContains(plan, "s >=", "s <");
        plan = getFragmentPlan("select * from string_date_plain where cast(s as date) <=> null");
        assertNotContains(plan, "s >=", "s <");
    }

    @Test
    public void testSqlSettingsValidateFormat() throws Exception {
        connectContext.executeSql("set string_date_predicate_format = '%Y-%m-%d'");
        connectContext.executeSql("set enable_string_date_predicate_pushdown = true");
        assertContains(getFragmentPlan("select * from string_date_plain where year(cast(s as date))=2024"),
                "s >= '2024-01-01'", "s < '2025-01-01'");
        connectContext.executeSql("set string_date_predicate_format = '%d-%m-%Y'");
        assertTrue(connectContext.getState().isError());
        assertContains(connectContext.getState().getErrorMessage(), "string_date_predicate_format");
        connectContext.getState().reset();
    }

    @ParameterizedTest
    @EnumSource(StringDateFormat.class)
    public void testConvertTzPushesNamedZoneBoundsInEachEncoding(StringDateFormat format) throws Exception {
        var variables = connectContext.getSessionVariable();
        variables.setStringDatePredicateFormat(format.getSqlFormat());
        String sql = "select * from string_date_plain where "
                + "convert_tz(cast(s as datetime), 'UTC', 'Asia/Dushanbe') >= '2024-03-01 00:00:00'";
        variables.setEnableStringDatePredicatePushdown(false);
        String off = getFragmentPlan(sql);
        variables.setEnableStringDatePredicatePushdown(true);
        String on = getFragmentPlan(sql);
        LocalDateTime lower = format.getPrecision() == java.time.temporal.ChronoUnit.DAYS
                ? LocalDateTime.of(2024, 3, 1, 0, 0) : LocalDateTime.of(2024, 2, 29, 19, 0);
        assertContains(on, "s >= '" + format.format(lower) + "'", "convert_tz(");
        assertNotContains(off, "s >= '");
        savePlans("convert-tz-" + format.name(), sql, off, on);
    }

    @Test
    public void testProductionUtcMicrosecondsAndExplicitTimezoneConversion() throws Exception {
        var variables = connectContext.getSessionVariable();
        variables.setStringDatePredicateFormat("%Y-%m-%dT%H:%i:%s.%fZ");
        String sql = "select * from string_date_plain where cast(s as datetime) "
                + ">= '2026-09-24 19:22:17.500000' and cast(s as datetime) < '2026-09-25 00:00:00'";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "s >= '2026-09-24T19:22:17.500000Z'", "s < '2026-09-25T00:00:00.000000Z'");
        String converted = getFragmentPlan("select * from string_date_plain where "
                + "convert_tz(cast(s as datetime), 'UTC', 'Asia/Dushanbe') >= '2026-09-25 00:00:00'");
        assertContains(converted, "s >= '2026-09-24T19:00:00.000000Z'", "convert_tz(");
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        assertNotContains(getFragmentPlan(sql), "s >= '", "s < '");
        savePlans("production-utc", sql, "", plan);
    }

    @Test
    public void testConvertTzRetainsFiltersNearDstAndInValueContexts() throws Exception {
        connectContext.getSessionVariable().setStringDatePredicateFormat("%Y-%m-%d %H:%i:%s");
        String dst = getFragmentPlan("select * from string_date_plain where "
                + "convert_tz(cast(s as datetime), 'UTC', 'America/New_York') >= '2024-11-03 01:30:00'");
        assertContains(dst, "convert_tz(");
        assertNotContains(dst, "s >= '");
        String value = getFragmentPlan("select (convert_tz(cast(s as datetime), 'UTC', 'Asia/Dushanbe') "
                + ">= '2024-03-01 00:00:00') is null from string_date_plain");
        assertContains(value, "convert_tz(");
        assertNotContains(value, "s >= '");
    }

    private static void savePlans(String name, String sql, String off, String on) throws Exception {
        String output = System.getProperty("string.date.plan.dir");
        if (output != null) {
            Path directory = Path.of(output);
            Files.createDirectories(directory);
            Files.writeString(directory.resolve(name + ".sql"), sql);
            Files.writeString(directory.resolve(name + ".off.plan"), off);
            Files.writeString(directory.resolve(name + ".on.plan"), on);
        }
    }
}
