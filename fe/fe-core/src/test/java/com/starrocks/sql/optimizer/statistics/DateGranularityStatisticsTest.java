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
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Functions that take one value in a period of the calendar have the distinct values of the periods of the range of their
 * argument, whichever way they write the period: date_trunc('month'), last_day, and the formats of date_format that
 * tell the months apart count the same 89 months from 2018-11-20 to 2026-03-05, and the 9 years, the 30 quarters.
 * next_day and previous_day count the weeks that start on the day they look for, and date and to_date have the
 * statistics of the cast to a DATE.
 */
public class DateGranularityStatisticsTest {
    private static final double ROWS = 1_000_000;
    private static final ColumnRefOperator DATE = new ColumnRefOperator(1, DateType.DATE, "d", true);
    private static final ColumnRefOperator TIME = new ColumnRefOperator(2, DateType.DATETIME, "ts", true);
    private static final ColumnRefOperator WEEKEND = new ColumnRefOperator(3, DateType.DATE, "weekend", true);
    private static final LocalDate FIRST = LocalDate.of(2018, 11, 20);
    private static final LocalDate LAST = LocalDate.of(2026, 3, 5);

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

    private static ColumnStatistic range(LocalDateTime min, LocalDateTime max, double distinct) {
        return ColumnStatistic.builder().setMinValue(seconds(min)).setMaxValue(seconds(max)).setNullsFraction(0.1)
                .setAverageRowSize(8).setDistinctValuesCount(distinct).build();
    }

    // Distinct values of the argument far above the periods and the days of the range
    private static Statistics table() {
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(DATE, range(FIRST.atStartOfDay(), LAST.atStartOfDay(), 100_000))
                .addColumnStatistic(TIME, range(FIRST.atStartOfDay(), LAST.atTime(12, 30, 45), 100_000))
                // 2026-03-07 is a Saturday and 2026-03-08 a Sunday
                .addColumnStatistic(WEEKEND, range(LocalDate.of(2026, 3, 7).atStartOfDay(),
                        LocalDate.of(2026, 3, 8).atStartOfDay(), 1000))
                .build();
    }

    private static Function function(String name, Type... arguments) {
        return ExprUtils.getBuiltinFunction(name, arguments, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    }

    private static CallOperator call(String name, Type type, ScalarOperator... arguments) {
        Type[] types = ImmutableList.copyOf(arguments).stream().map(ScalarOperator::getType).toArray(Type[]::new);
        return new CallOperator(name, type, ImmutableList.copyOf(arguments), function(name, types));
    }

    private static ConstantOperator text(String value) {
        return ConstantOperator.createVarchar(value);
    }

    private static ColumnStatistic stat(ScalarOperator expression) {
        return ExpressionStatisticCalculator.calculate(expression, table());
    }

    @Test
    public void testTheSameMonthsWhateverFunctionWritesThem() {
        ScalarOperator[] months = {
                call("date_trunc", DateType.DATE, text("month"), DATE),
                call("last_day", DateType.DATE, DATE),
                call("last_day", DateType.DATE, DATE, text("month")),
                call("date_format", VarcharType.VARCHAR, DATE, text("%Y-%m")),
                call("date_format", VarcharType.VARCHAR, DATE, text("%Y-%m-01")),
                call("date_format", VarcharType.VARCHAR, DATE, text("%Y-%m-01 00:00:00"))};
        for (ScalarOperator month : months) {
            Assertions.assertEquals(89, stat(month).getDistinctValuesCount(), month.toString());
        }
        ScalarOperator[] datetimeMonths = {
                call("date_trunc", DateType.DATETIME, text("month"), TIME),
                call("last_day", DateType.DATE, TIME),
                call("date_format", VarcharType.VARCHAR, TIME, text("%Y-%m"))};
        for (ScalarOperator month : datetimeMonths) {
            Assertions.assertEquals(89, stat(month).getDistinctValuesCount(), month.toString());
        }
    }

    @Test
    public void testQuartersAndYearsOfLastDay() {
        Assertions.assertEquals(30, stat(call("last_day", DateType.DATE, DATE, text("quarter"))).getDistinctValuesCount());
        Assertions.assertEquals(9, stat(call("last_day", DateType.DATE, DATE, text("year"))).getDistinctValuesCount());
        Assertions.assertEquals(30, stat(call("date_trunc", DateType.DATE, text("quarter"), DATE)).getDistinctValuesCount());
        Assertions.assertEquals(9, stat(call("date_format", VarcharType.VARCHAR, DATE, text("%Y"))).getDistinctValuesCount());
    }

    @Test
    public void testRangeAndNullsOfTheImageStay() {
        ColumnStatistic lastDay = stat(call("last_day", DateType.DATE, DATE));
        // the last days of the months of the range, 2018-11-30 and 2026-03-31
        Assertions.assertEquals(seconds(LocalDate.of(2018, 11, 30).atStartOfDay()), lastDay.getMinValue());
        Assertions.assertEquals(seconds(LocalDate.of(2026, 3, 31).atStartOfDay()), lastDay.getMaxValue());
        Assertions.assertEquals(0.1, lastDay.getNullsFraction(), 1e-9);
    }

    @Test
    public void testDistinctValuesOfTheArgumentBoundTheMonths() {
        ColumnStatistic few = range(FIRST.atStartOfDay(), LAST.atStartOfDay(), 10);
        Statistics statistics = Statistics.builder().setOutputRowCount(ROWS).addColumnStatistic(DATE, few).build();
        Assertions.assertEquals(10, ExpressionStatisticCalculator.calculate(call("last_day", DateType.DATE, DATE),
                statistics).getDistinctValuesCount());
    }

    @Test
    public void testWeeksStartOnTheDayThatNextDayAndPreviousDayLookFor() {
        ScalarOperator weekend = new CastOperator(DateType.DATETIME, WEEKEND);
        // Saturday and Sunday are in one week that starts on a Monday, and in two that start on a Sunday
        Assertions.assertEquals(1, stat(call("date_trunc", DateType.DATE, text("week"), WEEKEND)).getDistinctValuesCount());
        Assertions.assertEquals(2, stat(call("next_day", DateType.DATE, weekend, text("Sunday"))).getDistinctValuesCount());
        Assertions.assertEquals(1, stat(call("next_day", DateType.DATE, weekend, text("Monday"))).getDistinctValuesCount());
        // previous_day(x, 'Saturday') is the week that starts on a Sunday, previous_day(x, 'Sunday') one that starts on a Monday
        Assertions.assertEquals(2, stat(call("previous_day", DateType.DATE, weekend, text("Saturday"))).getDistinctValuesCount());
        Assertions.assertEquals(1, stat(call("previous_day", DateType.DATE, weekend, text("Sunday"))).getDistinctValuesCount());
    }

    @Test
    public void testNextDayOfAFullRangeHasItsWeeks() {
        // 2018-11-20 is a Tuesday and 2026-03-05 a Thursday: the weeks from the Monday 2018-11-19 to the Monday 2026-03-02
        // and from the Sunday 2018-11-18 to the Sunday 2026-03-01 are 381 each
        ScalarOperator range = new CastOperator(DateType.DATETIME, DATE);
        Assertions.assertEquals(381, ChronoUnit.DAYS.between(LocalDate.of(2018, 11, 19), LocalDate.of(2026, 3, 2)) / 7 + 1);
        Assertions.assertEquals(381, stat(call("next_day", DateType.DATE, range, text("Sunday"))).getDistinctValuesCount());
        Assertions.assertEquals(381, stat(call("date_trunc", DateType.DATE, text("week"), DATE)).getDistinctValuesCount());
    }

    private static void assertSameHistogram(Histogram expected, Histogram actual) {
        if (expected == null || actual == null) {
            Assertions.assertSame(expected, actual);
            return;
        }
        Assertions.assertEquals(expected.getMCV(), actual.getMCV());
        Assertions.assertEquals(expected.getBuckets().size(), actual.getBuckets().size());
        for (int i = 0; i < expected.getBuckets().size(); i++) {
            Bucket a = expected.getBuckets().get(i);
            Bucket b = actual.getBuckets().get(i);
            Assertions.assertEquals(a.getLower(), b.getLower(), "lower " + i);
            Assertions.assertEquals(a.getUpper(), b.getUpper(), "upper " + i);
            Assertions.assertEquals(a.getCount(), b.getCount(), "count " + i);
            Assertions.assertEquals(a.getUpperRepeats(), b.getUpperRepeats(), "repeats " + i);
            Assertions.assertEquals(a.getDistinctCount(), b.getDistinctCount(), "distinct " + i);
        }
    }

    private static Statistics timesWithHistogram(double distinct) {
        List<Bucket> buckets = new ArrayList<>();
        buckets.add(new Bucket(seconds(LocalDateTime.of(2024, 1, 1, 6, 0)), seconds(LocalDateTime.of(2024, 1, 10, 18, 0)),
                400L, 10L, 40L));
        buckets.add(new Bucket(seconds(LocalDateTime.of(2024, 1, 10, 19, 0)), seconds(LocalDateTime.of(2024, 2, 20, 8, 0)),
                700L, 5L, 120L));
        buckets.add(new Bucket(seconds(LocalDateTime.of(2024, 2, 20, 9, 0)), seconds(LocalDateTime.of(2024, 3, 10, 18, 0)),
                900L, 20L, 80L));
        ColumnStatistic times = ColumnStatistic.builder().setMinValue(seconds(LocalDateTime.of(2024, 1, 1, 6, 0)))
                .setMaxValue(seconds(LocalDateTime.of(2024, 3, 10, 18, 0))).setNullsFraction(0.1).setAverageRowSize(8)
                .setDistinctValuesCount(distinct)
                .setHistogram(new Histogram(buckets, Map.of("2024-01-05 10:00:00", 50L))).build();
        return Statistics.builder().setOutputRowCount(1000).addColumnStatistic(TIME, times).build();
    }

    @Test
    public void testDateAndToDateHaveTheStatisticsOfTheCastToADate() {
        for (double distinct : new double[] {10, 61, 500, 1000}) {
            Statistics statistics = timesWithHistogram(distinct);
            ColumnStatistic cast = ExpressionStatisticCalculator.calculate(new CastOperator(DateType.DATE, TIME), statistics);
            for (String name : new String[] {"date", "to_date"}) {
                ColumnStatistic date = ExpressionStatisticCalculator.calculate(call(name, DateType.DATE, TIME), statistics);
                Assertions.assertEquals(cast.getMinValue(), date.getMinValue(), name);
                Assertions.assertEquals(cast.getMaxValue(), date.getMaxValue(), name);
                Assertions.assertEquals(cast.getDistinctValuesCount(), date.getDistinctValuesCount(), name + " " + distinct);
                Assertions.assertEquals(cast.getNullsFraction(), date.getNullsFraction(), 1e-9, name);
                assertSameHistogram(cast.getHistogram(), date.getHistogram());
            }
            // 2024-01-01 to 2024-03-10: 31 + 29 + 10 days
            Assertions.assertEquals(Math.min(distinct, 70), cast.getDistinctValuesCount(), "" + distinct);
        }
        Assertions.assertNotNull(ExpressionStatisticCalculator.calculate(call("date", DateType.DATE, TIME),
                timesWithHistogram(500)).getHistogram());
    }
}
