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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.ImmutableList;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The cast of date_format(y, f) to a DATETIME has a string of known layout when the strings of f are ordered, exact on
 * every row and read by the BE as the start of their period. The bounds on the string, and on y below it, are derived
 * from a comparison of the cast; for any other format they are not, as a bound on a string that the BE reads another way
 * drops rows of a scan.
 */
public class DateFormatBoundsTest {
    private final ColumnRefOperator ts = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
    private final ColumnRefOperator days = new ColumnRefOperator(2, IntegerType.INT, "date", true);

    @BeforeEach
    public void setUp() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("+00:00");
        context.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private CallOperator dateFormat(ScalarOperator argument, String format) {
        return new CallOperator("date_format", VarcharType.VARCHAR,
                ImmutableList.of(argument, ConstantOperator.createVarchar(format)));
    }

    private BinaryPredicateOperator castCompare(String format, BinaryType type, String constant) {
        return new BinaryPredicateOperator(type, new CastOperator(DateType.DATETIME, dateFormat(ts, format)),
                ConstantOperator.createDatetime(LocalDateTime.parse(constant.replace(' ', 'T'))));
    }

    @Test
    public void testComparisonOfTheCastBoundsTheString() {
        String format = "%Y-%m-01 00:00:00";
        assertEquals("date_format(1: ts, " + format + ") >= 2026-09-01 00:00:00",
                StringDatePredicateDerivation.derive(castCompare(format, BinaryType.GE, "2026-08-15 00:00:00"),
                        unused -> null).toString());
        assertEquals("date_format(1: ts, " + format + ") >= 2026-08-01 00:00:00",
                StringDatePredicateDerivation.derive(castCompare(format, BinaryType.GE, "2026-08-01 00:00:00"),
                        unused -> null).toString());
        assertEquals("date_format(1: ts, " + format + ") >= 2026-09-01 00:00:00",
                StringDatePredicateDerivation.derive(castCompare(format, BinaryType.GT, "2026-08-01 00:00:00"),
                        unused -> null).toString());
        assertEquals("date_format(1: ts, " + format + ") < 2026-09-01 00:00:00",
                StringDatePredicateDerivation.derive(castCompare(format, BinaryType.LE, "2026-08-01 00:00:00"),
                        unused -> null).toString());
        assertEquals("date_format(1: ts, " + format + ") >= 2026-08-01 00:00:00 AND date_format(1: ts, " + format
                        + ") < 2026-09-01 00:00:00",
                StringDatePredicateDerivation.derive(castCompare(format, BinaryType.EQ, "2026-08-01 00:00:00"),
                        unused -> null).toString());
        // no string of the format is the cast of a time inside a month
        BinaryPredicateOperator inside = castCompare(format, BinaryType.EQ, "2026-08-15 00:00:00");
        assertSame(inside, StringDatePredicateDerivation.derive(inside, unused -> null));
    }

    // The BE gives NULL for a month without a day, reads ten digits with a year of two digits, and reads a year alone as
    // NULL: the cast is not the start of the period of the string for these formats
    @ParameterizedTest
    @ValueSource(strings = {"%Y%m%d%H", "%Y-%m", "%Y%m", "%Y", "%d.%m.%Y", "%y-%m", "%m/%d/%Y %h", "%Y-%j", "year"})
    public void testFormatThatTheBackendReadsOtherwiseHasNoBound(String format) {
        for (BinaryType type : new BinaryType[] {BinaryType.GE, BinaryType.EQ, BinaryType.LT}) {
            BinaryPredicateOperator predicate = castCompare(format, type, "2026-08-01 00:00:00");
            assertSame(predicate, StringDatePredicateDerivation.derive(predicate, unused -> null), format);
            assertSame(predicate, MonotonicFilterDerivation.addScanBounds(predicate, unused -> null), format);
            assertNull(MonotonicFilterDerivation.columnBoundsForEstimate(predicate), format);
            assertFalse(MonotonicFilterDerivation.reducesToColumnBounds(predicate), format);
        }
    }

    @Test
    public void testFormatThatTheBackendDoesNotRenderOnEveryRowHasNoBound() {
        // The BE trims the spaces at the end of a string when it casts it, so the strings of this format are read as the
        // start of their month; but it gives NULL for a string of 127 bytes, so the cast is NULL for the rows that it
        // renders so long, and the bounds on the column would hold rows that the comparison does not
        // the format renders 19 bytes besides its spaces
        String fits = "%Y-%m-01 00:00:00" + " ".repeat(107);
        String tooLong = "%Y-%m-01 00:00:00" + " ".repeat(108);
        assertNotNull(MonotonicFilterDerivation.columnBoundsForEstimate(
                castCompare(fits, BinaryType.EQ, "2026-08-01 00:00:00")));
        BinaryPredicateOperator predicate = castCompare(tooLong, BinaryType.EQ, "2026-08-01 00:00:00");
        assertNull(MonotonicFilterDerivation.columnBoundsForEstimate(predicate));
        assertSame(predicate, StringDatePredicateDerivation.derive(predicate, unused -> null));
    }

    @Test
    public void testFormatsThatTheBackendReadsAsThePeriodStartHaveBounds() {
        for (String format : new String[] {"%Y-%m-01 00:00:00", "%Y-01-01 00:00:00", "%Y-%m-%d 00:00:00",
                "%Y-%m-%d %H:00:00", "%Y-%m-%d %H:%i:00", "%Y-%m-%dT%H:%i:%s", "%Y-%m-%d %H:%i:%S", "%Y-%m-%d"}) {
            // the start of a year is the start of the period of every format
            BinaryPredicateOperator predicate = castCompare(format, BinaryType.GE, "2026-01-01 00:00:00");
            MonotonicFilterDerivation.ColumnBounds bounds = MonotonicFilterDerivation.columnBoundsForEstimate(predicate);
            assertTrue(bounds != null && !bounds.wider(), format);
            assertEquals("1: ts >= 2026-01-01 00:00:00", Utils.compoundAnd(bounds.bounds()).toString(), format);
        }
    }

    @Test
    public void testCastOfAMonthBoundsTheDatetimeExactly() {
        String format = "%Y-%m-01 00:00:00";
        MonotonicFilterDerivation.ColumnBounds bounds = MonotonicFilterDerivation.columnBoundsForEstimate(
                castCompare(format, BinaryType.EQ, "2026-08-01 00:00:00"));
        assertFalse(bounds.wider());
        assertEquals("1: ts >= 2026-08-01 00:00:00 AND 1: ts < 2026-09-01 00:00:00",
                Utils.compoundAnd(bounds.bounds()).toString());
        // a time inside the month is the string of no month
        assertNull(MonotonicFilterDerivation.columnBoundsForEstimate(
                castCompare(format, BinaryType.EQ, "2026-08-15 00:00:00")));
        // the bounds are the bounds of the scan too, and the condition stays
        BinaryPredicateOperator original = castCompare(format, BinaryType.EQ, "2026-08-01 00:00:00");
        ScalarOperator withBounds = MonotonicFilterDerivation.addScanBounds(original, unused -> null);
        assertTrue(Utils.extractConjuncts(withBounds).contains(original));
        assertEquals(3, Utils.extractConjuncts(withBounds).size());
        assertSame(original, new com.starrocks.sql.optimizer.rewrite.scalar.InvertMonotonicPredicateRule()
                .apply(original, null));
    }

    // The chain of the dashboard: the day number of the column, the end of its month, the month as text, the text as a
    // time, and a shift by zero seconds
    private CallOperator chain(boolean shifted) {
        CallOperator date = new CallOperator("days_add", DateType.DATETIME,
                ImmutableList.of(ConstantOperator.createDatetime(LocalDateTime.of(1970, 1, 1, 0, 0)), days));
        CallOperator lastDay = new CallOperator("last_day", DateType.DATE, ImmutableList.of(date));
        CastOperator month = new CastOperator(DateType.DATETIME, dateFormat(lastDay, "%Y-%m-01 00:00:00"));
        return shifted ? new CallOperator("seconds_add", DateType.DATETIME,
                ImmutableList.of(month, ConstantOperator.createInt(0))) : month;
    }

    @Test
    public void testChainOfTheDashboardBoundsTheDayNumbers() {
        long from = LocalDate.of(2026, 8, 1).toEpochDay();
        // the inverse of days_add on an INT column writes the end of the days as the last day
        long last = LocalDate.of(2026, 9, 1).toEpochDay() - 1;
        for (boolean shifted : new boolean[] {true, false}) {
            BinaryPredicateOperator original = new BinaryPredicateOperator(BinaryType.EQ, chain(shifted),
                    ConstantOperator.createDatetime(LocalDateTime.of(2026, 8, 1, 0, 0)));
            MonotonicFilterDerivation.ColumnBounds bounds = MonotonicFilterDerivation.columnBoundsForEstimate(original);
            assertFalse(bounds.wider(), "shifted " + shifted);
            assertEquals("2: date >= " + from + " AND 2: date <= " + last, Utils.compoundAnd(bounds.bounds()).toString(),
                    "shifted " + shifted);
            ScalarOperator withBounds = MonotonicFilterDerivation.addScanBounds(original, unused -> null);
            List<ScalarOperator> added = Utils.extractConjuncts(withBounds).stream()
                    .filter(conjunct -> conjunct != original).toList();
            assertEquals("2: date >= " + from + " AND 2: date <= " + last, Utils.compoundAnd(added).toString(),
                    "shifted " + shifted);
            assertTrue(added.stream().allMatch(ScalarOperator::isNotEvalEstimate));
        }
    }

    @Test
    public void testNotEqualOfTheChainBoundsTheDaysOutsideTheMonth() {
        // The inverse of <> is an OR of two bounds, and each of them goes down the chain to the day numbers. A shift
        // has no inverse of <>, so the chain with a shift has no bounds.
        long from = LocalDate.of(2026, 8, 1).toEpochDay();
        long next = LocalDate.of(2026, 9, 1).toEpochDay();
        BinaryPredicateOperator original = new BinaryPredicateOperator(BinaryType.NE, chain(false),
                ConstantOperator.createDatetime(LocalDateTime.of(2026, 8, 1, 0, 0)));
        MonotonicFilterDerivation.ColumnBounds bounds = MonotonicFilterDerivation.columnBoundsForEstimate(original);
        assertFalse(bounds.wider());
        assertEquals("2: date <= " + (from - 1) + " OR 2: date >= " + next, Utils.compoundAnd(bounds.bounds()).toString());
        assertNull(MonotonicFilterDerivation.columnBoundsForEstimate(new BinaryPredicateOperator(BinaryType.NE,
                chain(true), ConstantOperator.createDatetime(LocalDateTime.of(2026, 8, 1, 0, 0)))));
    }
}
