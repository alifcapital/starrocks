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

import com.starrocks.catalog.Table;
import com.starrocks.connector.iceberg.ScalarOperatorToIcebergExpr;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rewrite.StringDateFormatProperty;
import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Bounds on VARCHAR columns of dates whose Iceberg table declares their format, see StringDateFormatProperty. The
 * mock tables are in iceberg0.string_date_db, see MockIcebergMetadata.mockStringDateTables.
 */
public class StringDatePredicateIcebergTest extends ConnectorPlanTestBase {
    private static final String FORMATS = "iceberg0.string_date_db.strdate_formats";
    private static final String ISO = "iceberg0.string_date_db.strdate_iso";
    private static final String COMPACT = "iceberg0.string_date_db.strdate_compact";
    private static final String PLAIN = "iceberg0.string_date_db.strdate_plain";

    private TimeZone savedJvmZone;
    private SessionVariable saved;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE strdate_source (id bigint, d date, n int, s varchar(32)) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE strdate_target (id bigint, d date) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE strdate_partitions (s varchar(32), id bigint) PARTITION BY LIST(s) ("
                + "PARTITION p1 VALUES IN ('20240101'), PARTITION p2 VALUES IN ('20240201'), "
                + "PARTITION p3 VALUES IN ('20240301'), PARTITION p4 VALUES IN ('20240401')) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    @BeforeEach
    public void saveVariables() {
        savedJvmZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        saved = connectContext.getSessionVariable();
        connectContext.setSessionVariable((SessionVariable) saved.clone());
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    public void restoreVariables() {
        TimeZone.setDefault(savedJvmZone);
        connectContext.setSessionVariable(saved);
    }

    private static String minMaxPredicates(String plan) {
        return Arrays.stream(plan.split("\n")).map(String::trim).filter(line -> line.startsWith("MIN/MAX PREDICATES"))
                .collect(Collectors.joining("\n"));
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
            "d_dash|cast(d_dash as date) = '2024-03-05'|d_dash >= '2024-03-05'|d_dash < '2024-03-06'",
            "d_compact|cast(d_compact as date) = '2024-03-05'|d_compact >= '20240305'|d_compact < '20240306'",
            "dt_space|cast(dt_space as datetime) >= '2024-03-05 10:20:30.5' and cast(dt_space as datetime) < "
                    + "'2024-03-06'|dt_space >= '2024-03-05 10:20:31'|dt_space < '2024-03-06 00:00:00'",
            "dt_compact|cast(dt_compact as datetime) > '2024-03-05 10:20:30'"
                    + "|dt_compact >= '20240305102031'|CAST(",
            "dt_slash_minute|cast(dt_slash_minute as datetime) between '2024-03-05 10:20:30' and '2024-03-06 10:20:00'"
                    + "|dt_slash_minute >= '2024/03/05 10:21'|dt_slash_minute < '2024/03/06 10:21'",
            "ts_iso|cast(ts_iso as date) <= '2024-03-05'|ts_iso < '2024-03-06T00:00:00'|CAST(",
            "ts_iso_micro|cast(ts_iso_micro as datetime) < '2024-03-05 10:20:30'"
                    + "|ts_iso_micro < '2024-03-05T10:20:30.000000'|CAST(",
            "ts_iso_micro_z|cast(ts_iso_micro_z as datetime) >= '2024-03-05 10:20:30'"
                    + "|ts_iso_micro_z >= '2024-03-05T10:20:30.000000Z'|CAST(",
            "m_month|cast(m_month as date) >= '2024-03-15'|m_month >= '2024-04'|CAST(",
            "y_year|cast(y_year as datetime) < '2024-01-01 00:00:01'|y_year < '2025'|CAST(",
            "MixedCase|cast(MixedCase as date) > '2024-03-05'|MixedCase >= '2024-03-06'|CAST("})
    public void testScanBoundForEachFormat(String column, String where, String bound, String other) throws Exception {
        String plan = getFragmentPlan("select id from " + FORMATS + " where " + where);
        assertContains(plan, "CAST(", bound, other);
        assertContains(minMaxPredicates(plan), bound);
    }

    @Test
    public void testTwoTablesWithDifferentFormats() throws Exception {
        String plan = getFragmentPlan("select a.id from " + ISO + " a join " + COMPACT + " b on a.id = b.id "
                + "where cast(a.s as date) = '2024-03-05' and cast(b.s as date) = '2024-03-05'");
        assertContains(plan, "s >= '2024-03-05T00:00:00'", "s < '2024-03-06T00:00:00'",
                "s >= '20240305'", "s < '20240306'");
        assertNotContains(plan, "s >= '2024-03-05'", "s >= '20240305T");
    }

    @Test
    public void testColumnWithoutFormat() throws Exception {
        assertNotContains(getFragmentPlan("select id from " + FORMATS
                + " where cast(s_none as date) = '2024-03-05'"), "s_none >=", "s_none <");
        assertNotContains(getFragmentPlan("select id from " + PLAIN
                + " where cast(s as date) = '2024-03-05'"), "s >=", "s <");
        // A table of the internal catalog has no format.
        assertNotContains(getFragmentPlan("select id from strdate_source where cast(s as date) = '2024-03-05'"),
                "s >=", "s <");
    }

    @Test
    public void testInvalidFormatWarnsOnceAndGivesNoBound() throws Exception {
        WarnAppender appender = new WarnAppender();
        org.apache.logging.log4j.core.Logger logger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(StringDateFormatProperty.class);
        appender.start();
        logger.addAppender(appender);
        try {
            for (int i = 0; i < 2; i++) {
                assertNotContains(getFragmentPlan("select id from " + FORMATS + " where cast(s_bad as date) = "
                        + "'2024-03-05' and cast(s_garbage as date) = '2024-03-05' "
                        + "and cast(s_two_digit_year as date) = '2024-03-05'"),
                        "s_bad >=", "s_garbage >=", "s_two_digit_year >=");
            }
        } finally {
            logger.removeAppender(appender);
            appender.stop();
        }
        assertEquals(1, appender.messages.stream().filter(m -> m.contains("string-date-format.s_bad")).count(),
                appender.messages.toString());
        assertEquals(1, appender.messages.stream().filter(m -> m.contains("string-date-format.s_garbage")).count(),
                appender.messages.toString());
        assertEquals(1, appender.messages.stream().filter(m -> m.contains("string-date-format.s_two_digit_year")
                && m.contains("the BE casts")).count(), appender.messages.toString());
    }

    @Test
    public void testJoinTransferNeedsFormatOfSourceTable() throws Exception {
        String join = "select f.id from strdate_target f join %s e on f.id = e.id and f.d = cast(e.%s as date) "
                + "where e.%s between '%s' and '%s'";
        assertContains(getFragmentPlan(String.format(join, COMPACT, "s", "s", "20240305", "20240410")),
                "d >= '2024-03-05'", "d <= '2024-04-10'");
        assertContains(getFragmentPlan(String.format(join, ISO, "s", "s", "2024-03-05T00:00:00",
                "2024-04-10T00:00:00")), "d >= '2024-03-05'", "d <= '2024-04-10'");
        // The bounds of the domain must be strings of the format.
        assertNotContains(getFragmentPlan(String.format(join, ISO, "s", "s", "2024-03-05", "2024-04-10")),
                "d >= '2024-03-05'");
        assertNotContains(getFragmentPlan(String.format(join, PLAIN, "s", "s", "20240305", "20240410")),
                "d >= '2024-03-05'");
        assertNotContains(getFragmentPlan(String.format(join, "strdate_source", "s", "s", "20240305", "20240410")),
                "d >= '2024-03-05'");
        String expression = "(select id, substr(s, 1, 8) s8 from " + COMPACT + ")";
        assertNotContains(getFragmentPlan(String.format(join, expression, "s8", "s8", "20240305", "20240410")),
                "d >= '2024-03-05'");
        String union = "(select id, s from " + COMPACT + " union all select id, s from " + COMPACT + ")";
        assertNotContains(getFragmentPlan(String.format(join, union, "s", "s", "20240305", "20240410")),
                "d >= '2024-03-05'");
    }

    @Test
    public void testJoinToStringPartitionsFromEachSource() throws Exception {
        for (String[] source : new String[][] {
                {"strdate_source", "e.d", "e.d between '2024-03-05' and '2024-04-10'"},
                {"strdate_source", "cast(e.n as date)", "e.n between 20240305 and 20240410"},
                {COMPACT, "cast(e.s as date)", "e.s between '20240305' and '20240410'"}}) {
            String sql = "select f.id from strdate_partitions f join " + source[0] + " e "
                    + "on f.id = e.id and cast(f.s as date) = date_trunc('month', " + source[1] + ") where "
                    + source[2];
            assertContains(getFragmentPlan(sql), "partitions=2/4");
        }
        assertContains(getFragmentPlan("select f.id from strdate_partitions f join " + PLAIN + " e "
                + "on f.id = e.id and cast(f.s as date) = date_trunc('month', cast(e.s as date)) "
                + "where e.s between '20240305' and '20240410'"), "partitions=4/4");
    }

    @Test
    public void testSwitchTurnsOffScanBoundsAndJoinTransfer() throws Exception {
        String scan = "select id from " + COMPACT + " where cast(s as date) = '2024-03-05'";
        String join = "select f.id from strdate_target f join " + COMPACT + " e on f.id = e.id "
                + "and f.d = cast(e.s as date) where e.s between '20240305' and '20240410'";
        assertContains(getFragmentPlan(scan), "s >= '20240305'");
        assertContains(getFragmentPlan(join), "d >= '2024-03-05'");
        connectContext.getSessionVariable().setEnableStringDateFormatProperty(false);
        assertNotContains(getFragmentPlan(scan), "s >= '20240305'");
        assertNotContains(getFragmentPlan(join), "d >= '2024-03-05'");
    }

    @Test
    public void testUtcFormatNeedsUtcJvm() throws Exception {
        String sql = "select id from " + FORMATS + " where cast(ts_iso_micro_z as datetime) "
                + ">= '2026-09-24 19:22:17.500000' and cast(ts_iso_micro_z as datetime) < '2026-09-25 00:00:00'";
        assertContains(getFragmentPlan(sql), "ts_iso_micro_z >= '2026-09-24T19:22:17.500000Z'",
                "ts_iso_micro_z < '2026-09-25T00:00:00.000000Z'");
        String converted = getFragmentPlan("select id from " + FORMATS + " where "
                + "convert_tz(cast(ts_iso_micro_z as datetime), 'UTC', 'Asia/Dushanbe') >= '2026-09-25 00:00:00'");
        assertContains(converted, "ts_iso_micro_z >= '2026-09-24T19:00:00.000000Z'", "convert_tz(");
        String join = "select f.id from strdate_target f join " + FORMATS + " e on f.id = e.id "
                + "and f.d = cast(e.ts_iso_micro_z as date) where e.ts_iso_micro_z "
                + "between '2024-03-05T00:00:00.000000Z' and '2024-04-10T00:00:00.000000Z'";
        assertContains(getFragmentPlan(join), "d >= '2024-03-05'");
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        assertNotContains(getFragmentPlan(sql), "ts_iso_micro_z >= '", "ts_iso_micro_z < '");
        assertNotContains(getFragmentPlan(join), "d >= '2024-03-05'");
    }

    @Test
    public void testDateCastCoversWholeDayAndImplicitCastRoundsFraction() throws Exception {
        assertContains(getFragmentPlan("select id from " + FORMATS + " where cast(ts_iso_micro as date) = "
                + "'2024-02-29'"), "ts_iso_micro >= '2024-02-29T00:00:00.000000'",
                "ts_iso_micro < '2024-03-01T00:00:00.000000'", "CAST(");
        assertContains(getFragmentPlan("select id from " + FORMATS
                + " where dt_space >= cast('2024-03-01 12:00:00.500000' as datetime)"),
                "dt_space >= '2024-03-01 12:00:01'");
    }

    @Test
    public void testNullAndValueContextsRemainExpressions() throws Exception {
        String plan = getFragmentPlan("select (cast(d_compact as date) = '2024-03-01') is null from " + FORMATS);
        assertContains(plan, "CAST(");
        assertNotContains(plan, "d_compact >= '20240301'");
        assertNotContains(getFragmentPlan("select id from " + FORMATS
                + " where cast(d_compact as date) is null or id = 1"), "d_compact >=", "d_compact <");
        assertNotContains(getFragmentPlan("select id from " + FORMATS + " where cast(d_compact as date) <=> null"),
                "d_compact >=", "d_compact <");
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
            "d_dash|2024-03-01", "d_compact|20240301", "dt_space|2024-02-29 19:00:00",
            "dt_compact|20240229190000", "dt_slash_minute|2024/02/29 19:00", "ts_iso|2024-02-29T19:00:00",
            "ts_iso_micro|2024-02-29T19:00:00.000000", "ts_iso_micro_z|2024-02-29T19:00:00.000000Z",
            "m_month|2024-03", "y_year|2025"})
    public void testConvertTzBoundInEachFormat(String column, String lower) throws Exception {
        String sql = "select id from " + FORMATS + " where convert_tz(cast(" + column + " as datetime), 'UTC', "
                + "'Asia/Dushanbe') >= '2024-03-01 00:00:00'";
        assertContains(getFragmentPlan(sql), column + " >= '" + lower + "'", "convert_tz(");
    }

    @Test
    public void testConvertTzRetainsFiltersNearDstAndInValueContexts() throws Exception {
        String dst = getFragmentPlan("select id from " + FORMATS + " where "
                + "convert_tz(cast(dt_space as datetime), 'UTC', 'America/New_York') >= '2024-11-03 01:30:00'");
        assertContains(dst, "convert_tz(");
        assertNotContains(dst, "dt_space >= '");
        String value = getFragmentPlan("select (convert_tz(cast(dt_space as datetime), 'UTC', 'Asia/Dushanbe') "
                + ">= '2024-03-01 00:00:00') is null from " + FORMATS);
        assertContains(value, "convert_tz(");
        assertNotContains(value, "dt_space >= '");
    }

    @Test
    public void testQueryColumnsAreClearedAfterTheRule() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(connectContext, "iceberg0", "string_date_db", "strdate_compact");
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator column = factory.create("s", table.getColumn("s").getType(), true);
        factory.updateColumnRefToColumns(column, table.getColumn("s"), table);
        ColumnRefFactory outer = new ColumnRefFactory();
        StringDateFormatProperty.withQueryColumns(outer, () -> {
            assertNull(StringDateFormatProperty.ofQueryColumn(column));
            assertThrows(IllegalStateException.class, () -> StringDateFormatProperty.withQueryColumns(factory, () -> {
                assertNotNull(StringDateFormatProperty.ofQueryColumn(column));
                throw new IllegalStateException();
            }));
            // The outer factory is back, and it does not know the column.
            assertNull(StringDateFormatProperty.ofQueryColumn(column));
            return null;
        });
        assertThrows(IllegalStateException.class, () -> StringDateFormatProperty.withQueryColumns(factory, () -> {
            throw new IllegalStateException();
        }));
        assertNull(StringDateFormatProperty.ofQueryColumn(column));
    }

    private Expression storageFilter(String sql) throws Exception {
        IcebergScanNode scan = (IcebergScanNode) getExecPlan(sql).getScanNodes().get(0);
        return new ScalarOperatorToIcebergExpr().convert(
                Utils.extractConjuncts(scan.getIcebergJobPlanningPredicate()),
                new ScalarOperatorToIcebergExpr.IcebergContext(scan.getIcebergTable().getNativeTable().schema().asStruct()));
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
            "d_dash|2024-03-01|2024-04-01", "d_compact|20240301|20240401",
            "dt_space|2024-03-01 00:00:00|2024-04-01 00:00:00", "dt_slash_minute|2024/03/01 00:00|2024/04/01 00:00",
            "ts_iso_micro_z|2024-03-01T00:00:00.000000Z|2024-04-01T00:00:00.000000Z", "m_month|2024-03|2024-04"})
    public void testOnlyDeclaredStringBoundsReachIceberg(String column, String lower, String upper) throws Exception {
        String sql = "select id from " + FORMATS + " where date_format(cast(" + column + " as date), '%Y%m') = '202403'";
        Expression on = storageFilter(sql);
        assertContains(on.toString(), lower, upper);
        connectContext.getSessionVariable().setEnableStringDateFormatProperty(false);
        assertEquals(Expression.Operation.TRUE, storageFilter(sql).op());
    }

    @Test
    public void testNullableParsedDateCannotBecomeStringNullCheck() throws Exception {
        assertEquals(Expression.Operation.TRUE, storageFilter(
                "select id from " + FORMATS + " where cast(d_compact as date) is null").op());
    }

    private static final class WarnAppender extends AbstractAppender {
        private final List<String> messages = new ArrayList<>();

        private WarnAppender() {
            super("string-date-format-warn", null, null);
        }

        @Override
        public synchronized void append(LogEvent event) {
            if (event.getLevel() == Level.WARN) {
                messages.add(event.getMessage().getFormattedMessage());
            }
        }
    }
}
