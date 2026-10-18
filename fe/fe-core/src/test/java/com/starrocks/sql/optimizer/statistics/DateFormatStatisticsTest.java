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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Function;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * The statistics of date_format(x, f). A key of a GROUP BY and a comparison of the string read them, so we expect the
 * distinct values to be the periods of the finest field of the format in the range of x, and no more than the values the
 * fields can take. The tests use a column of dates from 2018-11-20 to 2026-03-05, whose distinct values are more than the
 * days of the range: with the periods cut to the first day of a month there are 89 months and 9 years, and the months
 * counted from the dates themselves would be 88 and 8.
 */
public class DateFormatStatisticsTest {
    private static final double ROWS = 1_000_000;
    private static final LocalDate FIRST = LocalDate.of(2018, 11, 20);
    private static final LocalDate LAST = LocalDate.of(2026, 3, 5);
    private static final double DAYS = ChronoUnit.DAYS.between(FIRST, LAST) + 1;
    private static final ColumnRefOperator DATE = new ColumnRefOperator(1, DateType.DATE, "d", true);
    private static final ColumnRefOperator TIME = new ColumnRefOperator(2, DateType.DATETIME, "ts", true);
    private static final ColumnRefOperator NUMBER = new ColumnRefOperator(3, IntegerType.INT, "date", true);

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

    private static double seconds(LocalDateTime time) {
        return Utils.getLongFromDateTime(time);
    }

    private static ColumnStatistic range(LocalDateTime min, LocalDateTime max, double nulls, double distinct) {
        return ColumnStatistic.builder().setMinValue(seconds(min)).setMaxValue(seconds(max)).setNullsFraction(nulls)
                .setAverageRowSize(8).setDistinctValuesCount(distinct).build();
    }

    private static Statistics table() {
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(DATE, range(FIRST.atStartOfDay(), LAST.atStartOfDay(), 0.1, 100_000))
                .addColumnStatistic(TIME, range(FIRST.atStartOfDay(), LAST.atTime(12, 30, 45), 0.2, 100_000))
                .addColumnStatistic(NUMBER, new ColumnStatistic(FIRST.toEpochDay(), LAST.toEpochDay(), 0, 4, 100_000))
                .build();
    }

    private static Function function(String name, Type... arguments) {
        return ExprUtils.getBuiltinFunction(name, arguments, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    }

    private static CallOperator dateFormat(ScalarOperator argument, String format) {
        return new CallOperator("date_format", VarcharType.VARCHAR,
                ImmutableList.of(argument, ConstantOperator.createVarchar(format)),
                function("date_format", argument.getType(), VarcharType.VARCHAR));
    }

    private static ColumnStatistic of(ScalarOperator argument, String format) {
        return ExpressionStatisticCalculator.calculate(dateFormat(argument, format), table());
    }

    private static double distinct(ScalarOperator argument, String format) {
        return of(argument, format).getDistinctValuesCount();
    }

    @Test
    public void testMonthsOfTheRangeWhateverTheFormatWritesAroundThem() {
        for (String format : new String[] {"%Y-%m", "%Y-%m-01", "%Y-%m-01 00:00:00", "%Y%m", "%b %Y", "%M, %Y"}) {
            Assertions.assertEquals(89, distinct(DATE, format), format);
            Assertions.assertEquals(89, distinct(TIME, format), format);
        }
    }

    @Test
    public void testYearsOfTheRange() {
        for (String format : new String[] {"%Y", "%y", "%Y-01-01", "year %Y"}) {
            Assertions.assertEquals(9, distinct(DATE, format), format);
        }
    }

    @Test
    public void testFieldsThatRepeatHaveTheValuesTheyCanTake() {
        Assertions.assertEquals(12, distinct(DATE, "%m"));
        Assertions.assertEquals(12, distinct(DATE, "%M"));
        Assertions.assertEquals(31, distinct(DATE, "%d"));
        Assertions.assertEquals(7, distinct(DATE, "%W"));
        Assertions.assertEquals(7, distinct(DATE, "%a"));
        Assertions.assertEquals(366, distinct(DATE, "%j"));
        Assertions.assertEquals(54, distinct(DATE, "%u"));
        Assertions.assertEquals(DAYS, distinct(DATE, "%Y-%m-%d"));
        // the name of the month and the year together
        Assertions.assertEquals(89, distinct(DATE, "%M %Y"));
    }

    @Test
    public void testTheYearOfAWeekCountsTheWeeksOfTheRange() {
        // the week of 2019-12-30 is in the year 2020, so one year of the range would give 1
        ColumnStatistic year = ColumnStatistic.builder().setMinValue(seconds(LocalDateTime.of(2019, 1, 1, 0, 0)))
                .setMaxValue(seconds(LocalDateTime.of(2019, 12, 30, 0, 0))).setNullsFraction(0)
                .setAverageRowSize(4).setDistinctValuesCount(365).build();
        Statistics statistics = Statistics.builder().setOutputRowCount(ROWS).addColumnStatistic(DATE, year).build();
        for (String format : new String[] {"%x", "%X"}) {
            double distinct = ExpressionStatisticCalculator.calculate(dateFormat(DATE, format), statistics)
                    .getDistinctValuesCount();
            Assertions.assertTrue(distinct >= 2, format + " " + distinct);
        }
    }

    @Test
    public void testFieldsBelowTheDayAreOneValueForADate() {
        // a DATE is rendered as its midnight
        Assertions.assertEquals(1, distinct(DATE, "%p"));
        Assertions.assertEquals(1, distinct(DATE, "%H"));
        Assertions.assertEquals(1, distinct(DATE, "%i"));
        Assertions.assertEquals(1, distinct(DATE, "constant"));
        Assertions.assertEquals(DAYS, distinct(DATE, "%Y-%m-%d %H:%i:%s"));
        Assertions.assertEquals(DAYS, distinct(DATE, "%Y-%m-%d %H:00:00"));
    }

    @Test
    public void testFieldsBelowTheDayOfADatetime() {
        // from 2026-03-01 00:00:00 to 2026-03-05 12:30:45, 109 hours
        ColumnStatistic hours = range(LocalDateTime.of(2026, 3, 1, 0, 0), LocalDateTime.of(2026, 3, 5, 12, 30, 45), 0,
                100_000);
        Statistics statistics = Statistics.builder().setOutputRowCount(ROWS).addColumnStatistic(TIME, hours).build();
        double[] expected = {24, 2, 60, 60, 109};
        String[] formats = {"%H", "%p", "%i", "%s", "%Y-%m-%d %H"};
        for (int i = 0; i < formats.length; i++) {
            Assertions.assertEquals(expected[i], ExpressionStatisticCalculator.calculate(dateFormat(TIME, formats[i]),
                    statistics).getDistinctValuesCount(), formats[i]);
        }
        // the fraction of a second has no period to count, and the distinct values of the argument are the bound
        Assertions.assertEquals(100_000, ExpressionStatisticCalculator.calculate(
                dateFormat(TIME, "%Y-%m-%d %H:%i:%s.%f"), statistics).getDistinctValuesCount());
    }

    @Test
    public void testDistinctValuesOfTheArgumentAndTheRowsBoundTheStrings() {
        ColumnStatistic fewDays = range(FIRST.atStartOfDay(), LAST.atStartOfDay(), 0, 10);
        Statistics statistics = Statistics.builder().setOutputRowCount(ROWS).addColumnStatistic(DATE, fewDays).build();
        Assertions.assertEquals(10, ExpressionStatisticCalculator.calculate(dateFormat(DATE, "%Y-%m"), statistics)
                .getDistinctValuesCount());
        Assertions.assertEquals(5, ExpressionStatisticCalculator.calculate(dateFormat(DATE, "%Y-%m-%d"), table(), 5)
                .getDistinctValuesCount());
    }

    @Test
    public void testNoRangeCountsThePeriodsOfTheType() {
        ColumnStatistic unbounded = ColumnStatistic.builder().setMinValue(Double.NEGATIVE_INFINITY)
                .setMaxValue(Double.POSITIVE_INFINITY).setNullsFraction(0).setAverageRowSize(4)
                .setDistinctValuesCount(1_000_000).build();
        Statistics statistics = Statistics.builder().setOutputRowCount(ROWS * 10).addColumnStatistic(DATE, unbounded).build();
        // the months of the years 0 to 9999 are 120000, and the argument has fewer values than the rows
        Assertions.assertEquals(120_000, ExpressionStatisticCalculator.calculate(dateFormat(DATE, "%Y-%m"), statistics)
                .getDistinctValuesCount());
        Assertions.assertEquals(12, ExpressionStatisticCalculator.calculate(dateFormat(DATE, "%m"), statistics)
                .getDistinctValuesCount());
        // the days of the type, 3,652,425, when the argument has more distinct values than that
        ColumnStatistic many = ColumnStatistic.builder().setMinValue(Double.NEGATIVE_INFINITY)
                .setMaxValue(Double.POSITIVE_INFINITY).setNullsFraction(0).setAverageRowSize(4)
                .setDistinctValuesCount(50_000_000).build();
        Statistics big = Statistics.builder().setOutputRowCount(100_000_000).addColumnStatistic(DATE, many).build();
        Assertions.assertEquals(3_652_425, ExpressionStatisticCalculator.calculate(dateFormat(DATE, "%Y-%m-%d"), big)
                .getDistinctValuesCount());
        // the fields below the day are one value of a DATE, whatever its range is
        for (String format : new String[] {"%H", "%p", "%i:%s", "no fields"}) {
            Assertions.assertEquals(1, ExpressionStatisticCalculator.calculate(dateFormat(DATE, format), statistics)
                    .getDistinctValuesCount(), format);
        }
        Assertions.assertNull(ExpressionStatisticCalculator.calculate(dateFormat(DATE, "%Y-%m-01 00:00:00"), statistics)
                .getStringBounds());
    }

    @Test
    public void testNullsAreThoseOfTheArgument() {
        Assertions.assertEquals(0.1, of(DATE, "%Y-%m").getNullsFraction(), 1e-9);
        Assertions.assertEquals(0.2, of(TIME, "%d.%m.%Y").getNullsFraction(), 1e-9);
    }

    @Test
    public void testFormatThatNeverFitsTheBufferOfTheBackendIsNull() {
        // 127 bytes of text and a field are 128 bytes of every string, which the BE does not write
        for (String format : new String[] {"", "%Y" + "x".repeat(127), "x".repeat(128)}) {
            ColumnStatistic statistic = of(DATE, format);
            Assertions.assertEquals(1, statistic.getNullsFraction(), format.length() + " bytes");
            Assertions.assertEquals(0, statistic.getDistinctValuesCount(), format.length() + " bytes");
        }
        // 127 bytes of string fit, and a long format of escapes renders short strings
        Assertions.assertEquals(0.1, of(DATE, "%Y" + "x".repeat(126)).getNullsFraction(), 1e-9);
        Assertions.assertEquals(0.1, of(DATE, "%%".repeat(70)).getNullsFraction(), 1e-9);
        Assertions.assertEquals(1, of(DATE, "%%".repeat(70)).getDistinctValuesCount());
    }

    @Test
    public void testBoundsOfTheStringsOfAFormatThatTheBackendReadsAsThePeriodStart() {
        StringBounds months = of(DATE, "%Y-%m-01 00:00:00").getStringBounds();
        Assertions.assertEquals(new StringBounds("2018-11-01 00:00:00", "2026-03-01 00:00:00", StringBounds.WHOLE, false),
                months);
        Assertions.assertEquals(new StringBounds("2018-11-20", "2026-03-05", StringBounds.WHOLE, false),
                of(DATE, "%Y-%m-%d").getStringBounds());
        // a DATE has the midnight in the fields below the day
        Assertions.assertEquals(new StringBounds("2018-11-20 00:00:00", "2026-03-05 00:00:00", StringBounds.WHOLE, false),
                of(DATE, "%Y-%m-%d %H:00:00").getStringBounds());
        Assertions.assertEquals(new StringBounds("2018-11-20T00:00:00", "2026-03-05T12:30:45", StringBounds.WHOLE, false),
                of(TIME, "%Y-%m-%dT%H:%i:%s").getStringBounds());
        Assertions.assertEquals(new StringBounds("2018-11-20 00:00:00", "2026-03-05 12:00:00", StringBounds.WHOLE, false),
                of(TIME, "%Y-%m-%d %H:00:00").getStringBounds());
    }

    @Test
    public void testNoBoundsWhereTheBackendReadsTheStringAnotherWay() {
        // the BE casts a month without a day to NULL, reads ten digits with a year of two digits, and these strings do not
        // sort as their dates
        for (String format : new String[] {"%Y-%m", "%Y", "%Y%m", "%Y%m%d%H", "%d.%m.%Y", "%y-%m", "%W", "%m/%d/%Y %h"}) {
            Assertions.assertNull(of(DATE, format).getStringBounds(), format);
        }
    }

    @Test
    public void testCastOfTheStringsToADateReadsTheBounds() {
        ColumnStatistic datetime = ExpressionStatisticCalculator.calculate(
                new CastOperator(DateType.DATETIME, dateFormat(DATE, "%Y-%m-01 00:00:00")), table());
        Assertions.assertEquals(seconds(LocalDateTime.of(2018, 11, 1, 0, 0)), datetime.getMinValue());
        Assertions.assertEquals(seconds(LocalDateTime.of(2026, 3, 1, 0, 0)), datetime.getMaxValue());
        Assertions.assertEquals(89, datetime.getDistinctValuesCount());
        ColumnStatistic date = ExpressionStatisticCalculator.calculate(
                new CastOperator(DateType.DATE, dateFormat(DATE, "%Y-%m-01")), table());
        Assertions.assertEquals(seconds(LocalDateTime.of(2018, 11, 1, 0, 0)), date.getMinValue());
        Assertions.assertEquals(seconds(LocalDateTime.of(2026, 3, 1, 0, 0)), date.getMaxValue());
        Assertions.assertEquals(89, date.getDistinctValuesCount());
        // the BE gives NULL for the cast of '%Y-%m', so the cast has no range
        ColumnStatistic unread = ExpressionStatisticCalculator.calculate(
                new CastOperator(DateType.DATETIME, dateFormat(DATE, "%Y-%m")), table());
        Assertions.assertTrue(unread.isInfiniteRange());
    }

    // The month of a day number as a dashboard writes it, with a shift by zero seconds around it:
    // seconds_add(CAST(date_format(last_day(days_add('1970-01-01', date)), '%Y-%m-01 00:00:00') AS DATETIME), 0)
    private static ScalarOperator wrapper(String format) {
        CallOperator date = new CallOperator("days_add", DateType.DATETIME,
                ImmutableList.of(ConstantOperator.createDatetime(LocalDateTime.of(1970, 1, 1, 0, 0)), NUMBER),
                function("days_add", DateType.DATETIME, IntegerType.INT));
        CallOperator lastDay = new CallOperator("last_day", DateType.DATE, ImmutableList.of(date),
                function("last_day", DateType.DATETIME));
        CastOperator month = new CastOperator(DateType.DATETIME, dateFormat(lastDay, format));
        return new CallOperator("seconds_add", DateType.DATETIME,
                ImmutableList.of(month, ConstantOperator.createInt(0)),
                function("seconds_add", DateType.DATETIME, IntegerType.INT));
    }

    @Test
    public void testTheMonthsOfTheWrapperOfADashboardAreTheGroups() {
        ColumnStatistic groups = ExpressionStatisticCalculator.calculate(wrapper("%Y-%m-01 00:00:00"), table());
        Assertions.assertEquals(89, groups.getDistinctValuesCount());
        Assertions.assertEquals(seconds(LocalDateTime.of(2018, 11, 1, 0, 0)), groups.getMinValue());
        Assertions.assertEquals(seconds(LocalDateTime.of(2026, 3, 1, 0, 0)), groups.getMaxValue());
    }
}
