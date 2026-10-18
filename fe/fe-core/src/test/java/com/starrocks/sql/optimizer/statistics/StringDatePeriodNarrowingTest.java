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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.TimeZone;

/**
 * A period of a column of strings of dates, as c LIKE '2026-08-1%', narrows the statistics of the column to the
 * strings of the period, and the next condition on the column or on its cast reads that narrowed histogram. The column
 * is that of DailyStrings: 864 rows in each day from 2026-07-01 to 2026-09-30.
 */
public class StringDatePeriodNarrowingTest {
    private TimeZone savedZone;

    @BeforeEach
    public void setUp() {
        savedZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        new ConnectContext().setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        TimeZone.setDefault(savedZone);
        ConnectContext.remove();
    }

    private static ScalarOperator like(String pattern) {
        return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, DailyStrings.TEXT,
                ConstantOperator.createVarchar(pattern));
    }

    private static ScalarOperator text(BinaryType type, String value) {
        return new BinaryPredicateOperator(type, DailyStrings.TEXT, ConstantOperator.createVarchar(value));
    }

    private static ScalarOperator time(BinaryType type, LocalDateTime value) {
        return new BinaryPredicateOperator(type, new CastOperator(DateType.DATETIME, DailyStrings.TEXT),
                ConstantOperator.createDatetime(value));
    }

    private static Statistics estimated(ScalarOperator predicate) {
        return PredicateStatisticsCalculator.withStringDateFormats(
                column -> DailyStrings.TEXT.equals(column) ? DailyStrings.FORMAT : null,
                () -> PredicateStatisticsCalculator.statisticsCalculate(predicate, DailyStrings.statistics()));
    }

    private static double rows(ScalarOperator predicate) {
        return estimated(predicate).getOutputRowCount();
    }

    // The strings of the column that start with the prefix
    private static long exact(String prefix) {
        long rows = 0;
        for (long row = 0; row < (long) DailyStrings.ROWS_OF_DAY * DailyStrings.DAYS; row++) {
            if (DailyStrings.at(row).startsWith(prefix)) {
                rows++;
            }
        }
        return rows;
    }

    @Test
    public void testPrefixOfADayDigitHoldsItsDays() {
        // 10 days, 9 days, 2 days and the one day 2026-09-30 of the prefix 2026-09-3
        Assertions.assertEquals(10 * 864, exact("2026-08-1"));
        Assertions.assertEquals(9 * 864, exact("2026-08-0"));
        for (String prefix : List.of("2026-08-1", "2026-08-0", "2026-07-2", "2026-07-3", "2026-09-1", "2026-09-3")) {
            Assertions.assertEquals(exact(prefix), rows(like(prefix + "%")), 4, prefix);
        }
    }

    // The wider prefix after the prefix keeps its rows. The prefix after the wider one finds the bucket at each end of
    // the wider one cut by the middle of its rows, as the estimate of strings cuts a bucket, so its rows are off by half
    // a bucket of 3000 rows at most.
    @Test
    public void testPrefixAndTheWiderPrefixThatHoldsItHaveTheRowsOfThePrefix() {
        for (String prefix : List.of("2026-08-1", "2026-08-0", "2026-07-2", "2026-09-1", "2026-09-3")) {
            double alone = rows(like(prefix + "%"));
            for (String wider : List.of(prefix.substring(0, 8), prefix.substring(0, 7), prefix.substring(0, 5),
                    prefix.substring(0, 4))) {
                Assertions.assertEquals(alone, rows(Utils.compoundAnd(like(prefix + "%"), like(wider + "%"))), 1e-6,
                        prefix + " and " + wider);
                Assertions.assertEquals(alone, rows(Utils.compoundAnd(like(wider + "%"), like(prefix + "%"))),
                        DailyStrings.ROWS_OF_BUCKET / 2.0, wider + " and " + prefix);
            }
        }
    }

    @Test
    public void testPrefixAndAComparisonOfItsCast() {
        double alone = rows(like("2026-08-1%"));
        // The cast starts before the period, and ends after it: the period is all that stays
        for (ScalarOperator within : List.of(time(BinaryType.GE, LocalDateTime.of(2026, 8, 5, 0, 0)),
                time(BinaryType.LT, LocalDateTime.of(2026, 9, 1, 0, 0)),
                time(BinaryType.GE, LocalDateTime.of(2026, 8, 10, 0, 0)))) {
            Assertions.assertEquals(alone, rows(Utils.compoundAnd(like("2026-08-1%"), within)), 2, within.toString());
        }
        // The cast cuts the period at 2026-08-15: its last five days
        ScalarOperator late = time(BinaryType.GE, LocalDateTime.of(2026, 8, 15, 0, 0));
        // The histogram of the period holds 9000 rows for the 8640 of it, the middle of each bucket at its ends
        Assertions.assertEquals(5 * 864, rows(Utils.compoundAnd(like("2026-08-1%"), late)), 5 * 864 * 0.05);
        // And the cast before the period holds none of it
        ScalarOperator early = time(BinaryType.LT, LocalDateTime.of(2026, 8, 10, 0, 0));
        Assertions.assertTrue(rows(Utils.compoundAnd(like("2026-08-1%"), early)) <= 1);
    }

    @Test
    public void testComparisonsWithAConstantThatCutsAFieldHoldTheDaysAfterIt() {
        // The strings above 2026-08-1 are those from 2026-08-10, and those below 2026-08-2 end with 2026-08-19
        ScalarOperator period = Utils.compoundAnd(text(BinaryType.GE, "2026-08-1"), text(BinaryType.LT, "2026-08-2"));
        Assertions.assertEquals(exact("2026-08-1"), rows(period), 4);
        ScalarOperator wider = Utils.compoundAnd(text(BinaryType.GE, "2026-08"), text(BinaryType.LT, "2026-09"));
        Assertions.assertEquals(exact("2026-08"), rows(wider), 4);
        Assertions.assertEquals(rows(period), rows(Utils.compoundAnd(period, wider)), 1e-6);
        Assertions.assertEquals(rows(period), rows(Utils.compoundAnd(wider, period)),
                DailyStrings.ROWS_OF_BUCKET / 2.0);
        // A string is above its own start: the strings above 2026-08-1 are those of 2026-08-1x and the later ones
        Assertions.assertEquals(exact("2026-08-1") + exact("2026-08-2") + exact("2026-08-3") + exact("2026-09"),
                rows(text(BinaryType.GE, "2026-08-1")), 4);
        Assertions.assertEquals(exact("2026-07") + exact("2026-08-0"), rows(text(BinaryType.LE, "2026-08-1")), 4);
        Assertions.assertEquals(exact("2026-08-1") + exact("2026-08-2") + exact("2026-08-3") + exact("2026-09"),
                rows(text(BinaryType.GT, "2026-08-1")), 4);
        Assertions.assertEquals(exact("2026-07") + exact("2026-08-0"), rows(text(BinaryType.LT, "2026-08-1")), 4);
    }

    @Test
    public void testNarrowedColumnHoldsStringsOfTheFormatAndACastWithHistogram() {
        Statistics narrowed = estimated(like("2026-08-1%"));
        Histogram histogram = narrowed.getColumnStatistic(DailyStrings.TEXT).getHistogram();
        Assertions.assertTrue(histogram.hasStringValues());
        List<Bucket> buckets = histogram.getBuckets();
        Assertions.assertEquals("2026-08-10 00:00:00", ((StringBucket) buckets.get(0)).getLowerString());
        Assertions.assertEquals("2026-08-20 00:00:00", ((StringBucket) buckets.get(buckets.size() - 1)).getUpperString());
        for (Type type : List.of(DateType.DATE, DateType.DATETIME)) {
            ColumnStatistic cast = ExpressionStatisticCalculator.calculate(new CastOperator(type, DailyStrings.TEXT),
                    narrowed);
            Assertions.assertNotNull(cast.getHistogram(), type.toString());
            Assertions.assertEquals(histogram.getTotalRows(), cast.getHistogram().getTotalRows(), type.toString());
        }
    }

    @Test
    public void testConstantOfTheFormatKeepsItsOwnBound() {
        // A string of the format is a bound as it is: the day 2026-08-10 and the seconds before 2026-08-20
        Statistics narrowed = estimated(Utils.compoundAnd(text(BinaryType.GE, "2026-08-10 00:00:00"),
                text(BinaryType.LT, "2026-08-20 00:00:00")));
        List<Bucket> buckets = narrowed.getColumnStatistic(DailyStrings.TEXT).getHistogram().getBuckets();
        Assertions.assertEquals("2026-08-10 00:00:00", ((StringBucket) buckets.get(0)).getLowerString());
        Assertions.assertEquals("2026-08-20 00:00:00", ((StringBucket) buckets.get(buckets.size() - 1)).getUpperString());
        // The strict ends stay as they are written as well: > starts at the constant and not at the next second
        StringBounds bounds = estimated(Utils.compoundAnd(text(BinaryType.GT, "2026-08-10 00:00:00"),
                text(BinaryType.LE, "2026-08-19 23:59:59"))).getColumnStatistic(DailyStrings.TEXT).getStringBounds();
        Assertions.assertEquals("2026-08-10 00:00:00", bounds.min());
        Assertions.assertEquals("2026-08-19 23:59:59", bounds.max());
    }

    @Test
    public void testBoundsOfTheNarrowedColumnAreThoseOfThePeriod() {
        Statistics narrowed = estimated(like("2026-08-1%"));
        StringBounds bounds = narrowed.getColumnStatistic(DailyStrings.TEXT).getStringBounds();
        Assertions.assertEquals("2026-08-10 00:00:00", bounds.min());
        Assertions.assertEquals("2026-08-19 23:59:59", bounds.max());
        // The days of the cast, and the distinct values of them, are those of the period and not of the table
        ColumnStatistic days = ExpressionStatisticCalculator.calculate(new CastOperator(DateType.DATE,
                DailyStrings.TEXT), narrowed);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2026, 8, 10, 0, 0)), days.getMinValue());
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2026, 8, 19, 0, 0)), days.getMaxValue());
        Assertions.assertEquals(10, days.getDistinctValuesCount());
        bounds = estimated(like("2026-08%")).getColumnStatistic(DailyStrings.TEXT).getStringBounds();
        Assertions.assertEquals("2026-08-01 00:00:00", bounds.min());
        Assertions.assertEquals("2026-08-31 23:59:59", bounds.max());
        // A lower bound alone leaves the upper one of the column, as an upper bound leaves the lower one
        bounds = estimated(text(BinaryType.GE, "2026-09-1")).getColumnStatistic(DailyStrings.TEXT).getStringBounds();
        Assertions.assertEquals("2026-09-10 00:00:00", bounds.min());
        Assertions.assertEquals(DailyStrings.at((long) DailyStrings.ROWS_OF_DAY * DailyStrings.DAYS - 1), bounds.max());
        bounds = estimated(text(BinaryType.LE, "2026-07-1")).getColumnStatistic(DailyStrings.TEXT).getStringBounds();
        Assertions.assertEquals(DailyStrings.at(0), bounds.min());
        Assertions.assertEquals("2026-07-09 23:59:59", bounds.max());
        // A string is above its own start and below the strings that start with it, so > narrows as >= and < as <=
        bounds = estimated(text(BinaryType.GT, "2026-09-1")).getColumnStatistic(DailyStrings.TEXT).getStringBounds();
        Assertions.assertEquals("2026-09-10 00:00:00", bounds.min());
        Assertions.assertEquals(DailyStrings.at((long) DailyStrings.ROWS_OF_DAY * DailyStrings.DAYS - 1), bounds.max());
        bounds = estimated(text(BinaryType.LT, "2026-07-1")).getColumnStatistic(DailyStrings.TEXT).getStringBounds();
        Assertions.assertEquals(DailyStrings.at(0), bounds.min());
        Assertions.assertEquals("2026-07-09 23:59:59", bounds.max());
    }

    @Test
    public void testBoundsOfAPeriodWithNoStringStayAsTheyAre() {
        // 2026-06 is before the first string: the period holds none, and the bounds do not cross
        StringBounds bounds = estimated(like("2026-06%")).getColumnStatistic(DailyStrings.TEXT).getStringBounds();
        Assertions.assertEquals(DailyStrings.at(0), bounds.min());
        Assertions.assertEquals(DailyStrings.at((long) DailyStrings.ROWS_OF_DAY * DailyStrings.DAYS - 1), bounds.max());
    }

    @Test
    public void testBoundsThatMayBeCutOrIncrementedAreNotNarrowed() {
        for (StringBounds original : List.of(
                new StringBounds(DailyStrings.at(0), DailyStrings.at(79_487), StringBounds.UNKNOWN, false),
                new StringBounds(DailyStrings.at(0), DailyStrings.at(79_487), StringBounds.WHOLE, true))) {
            ColumnStatistic statistic = ColumnStatistic.buildFrom(DailyStrings.statistics()
                    .getColumnStatistic(DailyStrings.TEXT)).setStringBounds(original).build();
            Statistics narrowed = PredicateStatisticsCalculator.withStringDateFormats(
                    column -> DailyStrings.TEXT.equals(column) ? DailyStrings.FORMAT : null,
                    () -> PredicateStatisticsCalculator.statisticsCalculate(like("2026-08-1%"),
                            DailyStrings.statistics(statistic)));
            Assertions.assertEquals(original, narrowed.getColumnStatistic(DailyStrings.TEXT).getStringBounds());
        }
    }
}
