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
import com.starrocks.common.util.DatePeriod;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * The period that the registry gives to a call: the periods of the dates that its value is constant on. A call whose
 * constants the registry cannot read has no period, and a function that is not constant on a period of the calendar
 * is not given one.
 */
public class MonotonicFunctionGranularityTest {
    private static final ColumnRefOperator TIME = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
    private static final ColumnRefOperator DAY = new ColumnRefOperator(2, DateType.DATE, "d", true);
    private static final ColumnRefOperator TEXT = new ColumnRefOperator(3, VarcharType.VARCHAR, "unit", true);
    private static final LocalDateTime SOME_TIME = LocalDateTime.of(2026, 3, 5, 13, 14, 15);

    private static CallOperator call(String name, Type type, ScalarOperator... arguments) {
        return new CallOperator(name, type, ImmutableList.copyOf(arguments));
    }

    private static ConstantOperator text(String value) {
        return ConstantOperator.createVarchar(value);
    }

    private static Optional<DatePeriod> period(CallOperator call) {
        return MonotonicFunctionRegistry.granularity(call);
    }

    private static void assertSame(DatePeriod expected, CallOperator call) {
        DatePeriod actual = period(call).orElseThrow(() -> new AssertionError(call.toString()));
        for (LocalDateTime time : new LocalDateTime[] {SOME_TIME, SOME_TIME.plusDays(1), SOME_TIME.plusDays(40),
                SOME_TIME.plusDays(400), SOME_TIME.plusHours(5), SOME_TIME.plusSeconds(61)}) {
            Assertions.assertEquals(expected.start(time), actual.start(time), call + " " + time);
            Assertions.assertEquals(expected.next(time), actual.next(time), call + " " + time);
        }
    }

    private static DatePeriod unit(String name) {
        return DatePeriod.ofUnit(name).orElseThrow();
    }

    @Test
    public void testDateTruncHasThePeriodOfItsUnit() {
        for (String unit : new String[] {"second", "minute", "hour", "day", "week", "month", "quarter", "year"}) {
            assertSame(unit(unit), call("date_trunc", DateType.DATETIME, text(unit), TIME));
        }
        assertSame(unit("month"), call("date_trunc", DateType.DATE, text("month"), DAY));
        // the unit is read as the fold of the planner does, in lower case
        assertSame(unit("month"), call("date_trunc", DateType.DATETIME, text("MONTH"), TIME));
        Assertions.assertTrue(period(call("date_trunc", DateType.DATETIME, text("millisecond"), TIME)).isEmpty());
        Assertions.assertTrue(period(call("date_trunc", DateType.DATETIME, text("decade"), TIME)).isEmpty());
        Assertions.assertTrue(period(call("date_trunc", DateType.DATETIME, TEXT, TIME)).isEmpty());
        Assertions.assertTrue(period(call("date_trunc", DateType.DATETIME, ConstantOperator.createNull(VarcharType.VARCHAR),
                TIME)).isEmpty());
    }

    @Test
    public void testLastDayHasTheMonthTheQuarterOrTheYear() {
        assertSame(unit("month"), call("last_day", DateType.DATE, TIME));
        assertSame(unit("month"), call("last_day", DateType.DATE, TIME, text("month")));
        assertSame(unit("quarter"), call("last_day", DateType.DATE, TIME, text("quarter")));
        assertSame(unit("quarter"), call("last_day", DateType.DATE, DAY, text("Quarter")));
        assertSame(unit("year"), call("last_day", DateType.DATE, TIME, text("year")));
        // the BE does not know the other units
        Assertions.assertTrue(period(call("last_day", DateType.DATE, TIME, text("week"))).isEmpty());
        Assertions.assertTrue(period(call("last_day", DateType.DATE, TIME, text("day"))).isEmpty());
        Assertions.assertTrue(period(call("last_day", DateType.DATE, TIME, TEXT)).isEmpty());
        Assertions.assertTrue(period(call("last_day", DateType.DATE, TIME, text("month"), text("month"))).isEmpty());
    }

    @Test
    public void testNextDayHasTheWeekThatStartsOnItsDay() {
        for (String[] day : new String[][] {
                {"Sunday", "SUNDAY"}, {"Mon", "MONDAY"}, {"Tu", "TUESDAY"}, {"Wednesday", "WEDNESDAY"},
                {"Thu", "THURSDAY"}, {"Fr", "FRIDAY"}, {"Saturday", "SATURDAY"}}) {
            assertSame(DatePeriod.weeks(DayOfWeek.valueOf(day[1])), call("next_day", DateType.DATE, TIME, text(day[0])));
        }
        // the week of date_trunc starts on a Monday, and the week of next_day on the day that it looks for
        Assertions.assertNotEquals(unit("week").start(SOME_TIME),
                period(call("next_day", DateType.DATE, TIME, text("Sunday"))).orElseThrow().start(SOME_TIME));
        // the fold reads the name as it is written, 'sunday' is no day for it
        Assertions.assertTrue(period(call("next_day", DateType.DATE, TIME, text("sunday"))).isEmpty());
        Assertions.assertTrue(period(call("next_day", DateType.DATE, TIME, text("Someday"))).isEmpty());
        Assertions.assertTrue(period(call("next_day", DateType.DATE, TIME, TEXT)).isEmpty());
    }

    @Test
    public void testPreviousDayHasTheWeekThatStartsAfterItsDay() {
        // previous_day(x, 'Sunday') = G holds for x from G + 1, a Monday, to G + 7
        assertSame(DatePeriod.weeks(DayOfWeek.MONDAY), call("previous_day", DateType.DATE, TIME, text("Sunday")));
        assertSame(DatePeriod.weeks(DayOfWeek.SUNDAY), call("previous_day", DateType.DATE, TIME, text("Sat")));
        assertSame(DatePeriod.weeks(DayOfWeek.WEDNESDAY), call("previous_day", DateType.DATE, TIME, text("Tuesday")));
        Assertions.assertTrue(period(call("previous_day", DateType.DATE, TIME, text("friday"))).isEmpty());
    }

    @Test
    public void testDateFormatHasThePeriodOfItsFinestField() {
        Object[][] cases = {
                {"%Y", "year"}, {"%y", "year"}, {"%Y-%m", "month"}, {"%M %Y", "month"}, {"%Y-%m-01", "month"},
                {"%Y-%m-%d", "day"}, {"%W", "day"}, {"%j", "day"}, {"%x", "day"}, {"%Y-%m-%d %H", "hour"}, {"%p", "hour"},
                {"%T", "second"}, {"%Y-%m-%d %H:%i", "minute"}, {"%Y-%m-%d %H:%i:%s", "second"}};
        for (Object[] row : cases) {
            assertSame(unit((String) row[1]), call("date_format", VarcharType.VARCHAR, TIME, text((String) row[0])));
        }
        // a DATE is rendered as its midnight: the fields below the day do not tell its dates apart
        assertSame(unit("day"), call("date_format", VarcharType.VARCHAR, DAY, text("%Y-%m-%d %H:%i:%s")));
        assertSame(unit("month"), call("date_format", VarcharType.VARCHAR, DAY, text("%Y-%m %H")));
        Assertions.assertTrue(period(call("date_format", VarcharType.VARCHAR, DAY, text("%H"))).orElseThrow().isSingle());
        Assertions.assertTrue(period(call("date_format", VarcharType.VARCHAR, TIME, text("no fields"))).orElseThrow()
                .isSingle());
        // the fraction of a second has no period to count, and the format must be a text for a date
        Assertions.assertTrue(period(call("date_format", VarcharType.VARCHAR, TIME, text("%Y-%m-%d %H:%i:%s.%f"))).isEmpty());
        Assertions.assertTrue(period(call("date_format", VarcharType.VARCHAR, TIME, TEXT)).isEmpty());
        Assertions.assertTrue(period(call("date_format", VarcharType.VARCHAR,
                new ColumnRefOperator(4, IntegerType.INT, "n", true), text("%Y"))).isEmpty());
    }

    @Test
    public void testFunctionsWithoutAPeriodHaveNone() {
        // time_slice cuts from the year 1 in steps of n units, but no inverse of the registry states where
        Assertions.assertTrue(period(call("time_slice", DateType.DATETIME, TIME, ConstantOperator.createInt(5),
                text("day"))).isEmpty());
        // date and to_date are the cast of a DATETIME to a DATE: their statistics are those of the cast
        Assertions.assertTrue(period(call("date", DateType.DATE, TIME)).isEmpty());
        Assertions.assertTrue(period(call("to_date", DateType.DATE, TIME)).isEmpty());
        Assertions.assertTrue(period(call("hours_add", DateType.DATETIME, TIME, ConstantOperator.createInt(5))).isEmpty());
        Assertions.assertTrue(period(call("year", IntegerType.INT, TIME)).isEmpty());
        Assertions.assertTrue(period(call("months_add", DateType.DATETIME, TIME, ConstantOperator.createInt(1))).isEmpty());
    }
}
