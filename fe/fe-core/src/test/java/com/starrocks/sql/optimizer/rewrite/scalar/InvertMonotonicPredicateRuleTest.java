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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.collect.ImmutableList;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDateTime;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class InvertMonotonicPredicateRuleTest {

    private final InvertMonotonicPredicateRule rule = new InvertMonotonicPredicateRule();

    private final ColumnRefOperator dtCol = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
    private final ColumnRefOperator dateCol = new ColumnRefOperator(2, DateType.DATE, "d", true);

    private ScalarOperator dateTrunc(String unit, ColumnRefOperator col, BinaryType cmp, LocalDateTime value) {
        CallOperator call = new CallOperator("date_trunc", col.getType(),
                ImmutableList.of(ConstantOperator.createVarchar(unit), col));
        ConstantOperator constant = col.getType().isDate()
                ? ConstantOperator.createDate(value) : ConstantOperator.createDatetime(value);
        return new BinaryPredicateOperator(cmp, call, constant);
    }

    private ScalarOperator apply(ScalarOperator predicate) {
        return rule.apply(predicate, null);
    }

    private static Stream<Arguments> caseTable() {
        // cmp, value, expected predicate text on the DATETIME column (month granularity)
        return Stream.of(
                Arguments.of(BinaryType.EQ, "2024-03-01T00:00",
                        "1: ts >= 2024-03-01 00:00:00 AND 1: ts < 2024-04-01 00:00:00"),
                Arguments.of(BinaryType.EQ, "2024-03-15T00:00",
                        "1: ts >= 2024-03-01 00:00:00 AND 1: ts < 2024-03-01 00:00:00"),
                Arguments.of(BinaryType.GE, "2024-03-01T00:00", "1: ts >= 2024-03-01 00:00:00"),
                Arguments.of(BinaryType.GE, "2024-03-15T00:00", "1: ts >= 2024-04-01 00:00:00"),
                Arguments.of(BinaryType.GT, "2024-03-01T00:00", "1: ts >= 2024-04-01 00:00:00"),
                Arguments.of(BinaryType.LE, "2024-03-01T00:00", "1: ts < 2024-04-01 00:00:00"),
                Arguments.of(BinaryType.LT, "2024-03-01T00:00", "1: ts < 2024-03-01 00:00:00"),
                Arguments.of(BinaryType.LT, "2024-03-15T00:00", "1: ts < 2024-04-01 00:00:00"));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("caseTable")
    public void testDateTruncCaseTable(BinaryType cmp, String value, String expected) {
        ScalarOperator result = apply(dateTrunc("month", dtCol, cmp, LocalDateTime.parse(value)));
        assertEquals(expected, result.toString());
    }

    @Test
    public void testDatetimeMaxRefused() {
        // nextUpperDateTime clamps at the DATETIME max and cannot provide the exclusive
        // upper bound: every shape must keep the original predicate
        for (BinaryType cmp : new BinaryType[] {BinaryType.EQ, BinaryType.GE, BinaryType.GT,
                BinaryType.LE, BinaryType.LT}) {
            ScalarOperator predicate = dateTrunc("day", dtCol, cmp, LocalDateTime.of(9999, 12, 31, 0, 0));
            assertSame(predicate, apply(predicate), cmp.toString());
        }
    }

    @Test
    public void testDayPointOnDateColumnKeepsEq() {
        ScalarOperator result = apply(dateTrunc("day", dateCol, BinaryType.EQ, LocalDateTime.of(2024, 3, 5, 0, 0)));
        assertEquals("2: d = 2024-03-05", result.toString());
    }

    @Test
    public void testIntraDayUnitOnDateColumnRefused() {
        // an hour period has intra-day bounds, unrepresentable as DATE constants
        ScalarOperator predicate = dateTrunc("hour", dateCol, BinaryType.GE, LocalDateTime.of(2024, 3, 5, 0, 0));
        assertSame(predicate, apply(predicate));
    }

    @Test
    public void testNonConstantUnitRefused() {
        CallOperator call = new CallOperator("date_trunc", DateType.DATETIME,
                ImmutableList.of(new ColumnRefOperator(3, VarcharType.VARCHAR, "unit", true), dtCol));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 3, 1, 0, 0)));
        assertSame(predicate, apply(predicate));
    }

    @Test
    public void testNullConstantRefused() {
        CallOperator call = new CallOperator("date_trunc", DateType.DATETIME,
                ImmutableList.of(ConstantOperator.createVarchar("month"), dtCol));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, call,
                ConstantOperator.createNull(DateType.DATETIME));
        assertSame(predicate, apply(predicate));
    }

    @Test
    public void testToIso8601Inverts() {
        // DATE renders as %Y-%m-%d: a bijection, every comparison carries over
        CallOperator dateCall = new CallOperator("to_iso8601", VarcharType.VARCHAR, ImmutableList.of(dateCol));
        ScalarOperator result = apply(new BinaryPredicateOperator(BinaryType.EQ, dateCall,
                ConstantOperator.createVarchar("2024-03-05")));
        assertEquals("2: d = 2024-03-05", result.toString());
        result = apply(new BinaryPredicateOperator(BinaryType.NE, dateCall,
                ConstantOperator.createVarchar("2024-03-05")));
        assertEquals("2: d != 2024-03-05", result.toString());
        // non-canonical constants refuse
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, dateCall,
                ConstantOperator.createVarchar("2024-3-05"));
        assertSame(predicate, apply(predicate));

        // DATETIME renders with 'T' and six fraction digits
        CallOperator dtCall = new CallOperator("to_iso8601", VarcharType.VARCHAR, ImmutableList.of(dtCol));
        result = apply(new BinaryPredicateOperator(BinaryType.GE, dtCall,
                ConstantOperator.createVarchar("2024-03-05T10:30:00.123456")));
        assertEquals(BinaryType.GE, ((BinaryPredicateOperator) result).getBinaryType());
        assertEquals(LocalDateTime.of(2024, 3, 5, 10, 30, 0, 123456000),
                ((ConstantOperator) result.getChild(1)).getDatetime());
        // a short fraction is not the canonical rendering
        predicate = new BinaryPredicateOperator(BinaryType.GE, dtCall,
                ConstantOperator.createVarchar("2024-03-05T10:30:00.5"));
        assertSame(predicate, apply(predicate));
    }

    @Test
    public void testUnixTimestampInverts() {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("+08:00");
        ctx.setThreadLocalInfo();
        try {
            CallOperator call = new CallOperator("unix_timestamp", IntegerType.BIGINT, ImmutableList.of(dtCol));
            // 1700000000 is 2023-11-15 06:13:20 at +08:00; the fold truncates to seconds
            ScalarOperator result = apply(new BinaryPredicateOperator(BinaryType.EQ, call,
                    ConstantOperator.createBigint(1700000000L)));
            assertEquals("1: ts >= 2023-11-15 06:13:20 AND 1: ts < 2023-11-15 06:13:21", result.toString());
            // GE cuts the upper clamp tail: unix_timestamp is 0 past the max, never >= N
            result = apply(new BinaryPredicateOperator(BinaryType.GE, call,
                    ConstantOperator.createBigint(1700000000L)));
            assertEquals("1: ts >= 2023-11-15 06:13:20 AND 1: ts <= 9999-12-31 15:59:59", result.toString());
            // the LE preimage contains both clamp tails and is not an interval
            ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.LE, call,
                    ConstantOperator.createBigint(1700000000L));
            assertSame(predicate, apply(predicate));
            // 0 is the clamp plateau itself
            predicate = new BinaryPredicateOperator(BinaryType.EQ, call, ConstantOperator.createBigint(0L));
            assertSame(predicate, apply(predicate));

            // 1730611800 is 01:30 inside the New York fall-back overlap: two epochs render
            // to that wall clock, the bound is ambiguous
            ctx.getSessionVariable().setTimeZone("America/New_York");
            predicate = new BinaryPredicateOperator(BinaryType.EQ, call,
                    ConstantOperator.createBigint(1730611800L));
            assertSame(predicate, apply(predicate));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testFromUnixTimeInverts() {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("+08:00");
        ctx.setThreadLocalInfo();
        try {
            ColumnRefOperator epoch = new ColumnRefOperator(3, IntegerType.BIGINT, "ep", true);
            CallOperator dayFormat = new CallOperator("from_unixtime", VarcharType.VARCHAR,
                    ImmutableList.of(epoch, ConstantOperator.createVarchar("%Y-%m-%d")));
            // the day 2024-03-05 at +08:00 is the epoch period [1709568000, 1709654400)
            ScalarOperator result = apply(new BinaryPredicateOperator(BinaryType.EQ, dayFormat,
                    ConstantOperator.createVarchar("2024-03-05")));
            assertEquals("3: ep >= 1709568000 AND 3: ep < 1709654400", result.toString());
            // LE keeps the below-zero rendering tail out
            result = apply(new BinaryPredicateOperator(BinaryType.LE, dayFormat,
                    ConstantOperator.createVarchar("2024-03-05")));
            assertEquals("3: ep < 1709654400 AND 3: ep >= 0", result.toString());

            // the one-argument form renders the SR datetime string: a one-second period
            CallOperator plain = new CallOperator("from_unixtime", VarcharType.VARCHAR, ImmutableList.of(epoch));
            result = apply(new BinaryPredicateOperator(BinaryType.EQ, plain,
                    ConstantOperator.createVarchar("2024-03-05 10:30:00")));
            assertEquals("3: ep >= 1709605800 AND 3: ep < 1709605801", result.toString());

            // an INT column cannot hold the max-epoch guard value: the guard is dropped,
            // the type itself ends inside the valid range
            ColumnRefOperator intEpoch = new ColumnRefOperator(4, IntegerType.INT, "epi", true);
            CallOperator overInt = new CallOperator("from_unixtime", VarcharType.VARCHAR,
                    ImmutableList.of(intEpoch, ConstantOperator.createVarchar("%Y-%m-%d")));
            result = apply(new BinaryPredicateOperator(BinaryType.GE, overInt,
                    ConstantOperator.createVarchar("2024-03-05")));
            assertEquals("4: epi >= 1709568000", result.toString());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testNePeriodInvertsToDisjunction() {
        // NULL-safe: for a NULL ts both the original and OR-of-comparisons yield NULL
        ScalarOperator result = apply(dateTrunc("month", dtCol, BinaryType.NE, LocalDateTime.of(2024, 3, 1, 0, 0)));
        assertEquals("1: ts < 2024-03-01 00:00:00 OR 1: ts >= 2024-04-01 00:00:00", result.toString());
    }

    @Test
    public void testNeMisalignedRefused() {
        // a misaligned constant renders from no period: TRUE for every non-NULL ts,
        // which has no bare-column comparison form
        ScalarOperator predicate = dateTrunc("month", dtCol, BinaryType.NE, LocalDateTime.of(2024, 3, 15, 0, 0));
        assertSame(predicate, apply(predicate));
    }

    @Test
    public void testMonthShiftRefused() {
        CallOperator call = new CallOperator("months_add", DateType.DATETIME,
                ImmutableList.of(dtCol, ConstantOperator.createInt(1)));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.LE, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 2, 29, 0, 0)));
        assertSame(predicate, apply(predicate));
    }

    @Test
    public void testDayShiftInverts() {
        // days_add overflows only at the top of the domain: LE keeps the region away from
        // the tail and needs no guard, GE gets one, EQ needs none (its preimage is one valid
        // point)
        CallOperator call = new CallOperator("days_add", DateType.DATETIME,
                ImmutableList.of(dtCol, ConstantOperator.createInt(3)));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.LE, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 3, 8, 0, 0)));
        assertEquals("1: ts <= 2024-03-05 00:00:00", apply(predicate).toString());

        predicate = new BinaryPredicateOperator(BinaryType.GE, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 3, 8, 0, 0)));
        assertEquals("1: ts >= 2024-03-05 00:00:00 AND 1: ts <= 9999-12-28 23:59:59",
                apply(predicate).toString());

        predicate = new BinaryPredicateOperator(BinaryType.EQ, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 3, 8, 0, 0)));
        assertEquals("1: ts = 2024-03-05 00:00:00", apply(predicate).toString());
    }

    @Test
    public void testPeriodHelperMatchesEvaluatorFold() {
        // the inverter builds bounds with SyncPartitionUtils while the image side folds via
        // ScalarOperatorFunctions.dateTrunc - two implementations of the same floor; if they
        // ever disagree the inverter produces wrong bounds. Checked on the units with
        // nontrivial floors.
        for (String unit : new String[] {"week", "quarter", "month", "year"}) {
            for (LocalDateTime probe : new LocalDateTime[] {
                    LocalDateTime.of(2024, 3, 6, 10, 30),     // Wednesday
                    LocalDateTime.of(2024, 3, 10, 23, 59),    // Sunday
                    LocalDateTime.of(2024, 2, 29, 0, 0)}) {   // leap day
                LocalDateTime helper = com.starrocks.sql.common.SyncPartitionUtils.getLowerDateTime(probe, unit);
                ConstantOperator folded = com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions.dateTrunc(
                        ConstantOperator.createVarchar(unit), ConstantOperator.createDatetime(probe));
                assertEquals(folded.getDatetime(), helper, unit + " at " + probe);
            }
        }
    }

    @Test
    public void testYearInverts() {
        CallOperator call = new CallOperator("year", DateType.DATETIME, ImmutableList.of(dtCol));
        assertEquals("1: ts >= 2024-01-01 00:00:00 AND 1: ts < 2025-01-01 00:00:00",
                apply(new BinaryPredicateOperator(BinaryType.EQ, call,
                        ConstantOperator.createInt(2024))).toString());
        assertEquals("1: ts >= 2024-01-01 00:00:00",
                apply(new BinaryPredicateOperator(BinaryType.GE, call,
                        ConstantOperator.createInt(2024))).toString());
        // out of the year domain / no next period start at 9999: keep the predicate
        ScalarOperator outOfRange = new BinaryPredicateOperator(BinaryType.EQ, call,
                ConstantOperator.createInt(10000));
        assertSame(outOfRange, apply(outOfRange));
        ScalarOperator lastYear = new BinaryPredicateOperator(BinaryType.GE, call,
                ConstantOperator.createInt(9999));
        assertSame(lastYear, apply(lastYear));
    }

    @Test
    public void testToDateInverts() {
        CallOperator call = new CallOperator("to_date", DateType.DATE, ImmutableList.of(dtCol));
        assertEquals("1: ts >= 2024-03-05 00:00:00 AND 1: ts < 2024-03-06 00:00:00",
                apply(new BinaryPredicateOperator(BinaryType.EQ, call,
                        ConstantOperator.createDate(LocalDateTime.of(2024, 3, 5, 0, 0)))).toString());
        assertEquals("1: ts < 2024-03-06 00:00:00",
                apply(new BinaryPredicateOperator(BinaryType.LE, call,
                        ConstantOperator.createDate(LocalDateTime.of(2024, 3, 5, 0, 0)))).toString());
    }

    @Test
    public void testDatediffInverts() {
        // the column in the first position increases: same comparison, day period at C + N
        CallOperator colFirst = new CallOperator("datediff", DateType.DATETIME, ImmutableList.of(
                dtCol, ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 1, 0, 0))));
        assertEquals("1: ts >= 2024-01-11 00:00:00",
                apply(new BinaryPredicateOperator(BinaryType.GE, colFirst,
                        ConstantOperator.createInt(10))).toString());
        assertEquals("1: ts >= 2024-01-11 00:00:00 AND 1: ts < 2024-01-12 00:00:00",
                apply(new BinaryPredicateOperator(BinaryType.EQ, colFirst,
                        ConstantOperator.createInt(10))).toString());
        // the column in the second position decreases: comparison flips, period at C - N
        CallOperator constFirst = new CallOperator("datediff", DateType.DATETIME, ImmutableList.of(
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 1, 0, 0)), dtCol));
        assertEquals("1: ts < 2023-12-23 00:00:00",
                apply(new BinaryPredicateOperator(BinaryType.GE, constFirst,
                        ConstantOperator.createInt(10))).toString());
    }

    @Test
    public void testDatediffTwoVariableArgsRefused() {
        // both arguments carry the column: day of month rises and drops within each month,
        // not a monotonic chain
        CallOperator trunc = new CallOperator("date_trunc", DateType.DATETIME,
                ImmutableList.of(ConstantOperator.createVarchar("month"), dtCol));
        CallOperator sawtooth = new CallOperator("datediff", DateType.DATETIME,
                ImmutableList.of(dtCol, trunc));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE, sawtooth,
                ConstantOperator.createInt(10));
        assertSame(predicate, apply(predicate));
    }

    @Test
    public void testMonthFormatInverts() {
        CallOperator call = new CallOperator("date_format", VarcharType.VARCHAR,
                ImmutableList.of(dtCol, ConstantOperator.createVarchar("%Y%m")));
        assertEquals("1: ts >= 2024-03-01 00:00:00 AND 1: ts < 2024-04-01 00:00:00",
                apply(new BinaryPredicateOperator(BinaryType.EQ, call,
                        ConstantOperator.createVarchar("202403"))).toString());
        assertEquals("1: ts >= 0001-01-01 00:00:00 AND 1: ts < 0001-02-01 00:00:00",
                apply(new BinaryPredicateOperator(BinaryType.EQ, call,
                        ConstantOperator.createVarchar("000101"))).toString());
        // month 13, month 0, non-canonical length: strict admission keeps the predicate
        for (String bad : new String[] {"999913", "202400", "20243", "2024-03"}) {
            ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, call,
                    ConstantOperator.createVarchar(bad));
            assertSame(predicate, apply(predicate), bad);
        }
    }

    @Test
    public void testDayShiftDownwardGuard() {
        // days_sub overflows at the bottom: LE opens toward that tail and gets the guard
        CallOperator call = new CallOperator("days_sub", DateType.DATETIME,
                ImmutableList.of(dtCol, ConstantOperator.createInt(3)));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.LE, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 3, 8, 0, 0)));
        assertEquals("1: ts <= 2024-03-11 00:00:00 AND 1: ts >= 0000-01-04 00:00:00",
                apply(predicate).toString());

        predicate = new BinaryPredicateOperator(BinaryType.GE, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 3, 8, 0, 0)));
        assertEquals("1: ts >= 2024-03-11 00:00:00", apply(predicate).toString());
    }

    @Test
    public void testDayShiftAtDomainBoundaryRefused() {
        // the opposite shift days_sub('0000-01-02', 3) leaves the supported range and folds
        // to NULL; the predicate must stay unrewritten
        CallOperator call = new CallOperator("days_add", DateType.DATETIME,
                ImmutableList.of(dtCol, ConstantOperator.createInt(3)));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE, call,
                ConstantOperator.createDatetime(LocalDateTime.of(0, 1, 2, 0, 0)));
        assertSame(predicate, apply(predicate));
    }
}
