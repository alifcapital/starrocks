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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TimeZone;
import java.util.function.Function;

/**
 * A histogram spreads the rows of a bucket over its range, so the day d, or the integer d, is the range from d up to
 * the next one: a comparison x <= d keeps the rows of d, and x > d does not. The columns have 100 rows for each of 91
 * values, in 13 buckets of 7 values, 600 rows spread over the first six and 100 rows on the last.
 */
public class HistogramClosedEndTest {
    private static final ColumnRefOperator DAY = new ColumnRefOperator(1, DateType.DATE, "day", true);
    private static final ColumnRefOperator NUMBER = new ColumnRefOperator(2, IntegerType.INT, "number", true);
    private static final ColumnRefOperator BIG = new ColumnRefOperator(3, IntegerType.BIGINT, "big", true);
    private static final ColumnRefOperator REAL = new ColumnRefOperator(4, FloatType.DOUBLE, "real", true);
    private static final LocalDate FIRST_DAY = LocalDate.of(2026, 7, 1);
    private static final int VALUES = 91;
    private static final int ROWS_OF_VALUE = 100;

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

    private static double seconds(int day) {
        return Utils.getLongFromDateTime(FIRST_DAY.plusDays(day).atStartOfDay());
    }

    // The values of the column from 0 to 90 as the statistics hold them: days as seconds, numbers as they are
    private static Histogram histogram(Function<Integer, Double> value) {
        List<Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < 13; i++) {
            buckets.add(new Bucket(value.apply(i * 7), value.apply(i * 7 + 6), 700L * (i + 1), (long) ROWS_OF_VALUE, 7L));
        }
        return new Histogram(buckets, Map.of());
    }

    private static ColumnStatistic statistic(Function<Integer, Double> value) {
        return ColumnStatistic.builder().setMinValue(value.apply(0)).setMaxValue(value.apply(VALUES - 1))
                .setNullsFraction(0).setAverageRowSize(8).setDistinctValuesCount(VALUES)
                .setHistogram(histogram(value)).build();
    }

    private static Statistics table() {
        return Statistics.builder().setOutputRowCount((double) VALUES * ROWS_OF_VALUE)
                .addColumnStatistic(DAY, statistic(HistogramClosedEndTest::seconds))
                .addColumnStatistic(NUMBER, statistic(Integer::doubleValue))
                .addColumnStatistic(BIG, statistic(Integer::doubleValue))
                .addColumnStatistic(REAL, statistic(Integer::doubleValue))
                .build();
    }

    private static ConstantOperator constant(ColumnRefOperator column, int value) {
        if (column == DAY) {
            return ConstantOperator.createDate(FIRST_DAY.plusDays(value).atStartOfDay());
        }
        if (column == NUMBER) {
            return ConstantOperator.createInt(value);
        }
        if (column == BIG) {
            return ConstantOperator.createBigint(value);
        }
        return ConstantOperator.createDouble(value);
    }

    private static ScalarOperator compare(BinaryType type, ColumnRefOperator column, int value) {
        return new BinaryPredicateOperator(type, column, constant(column, value));
    }

    private static double rows(ScalarOperator predicate) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, table()).getOutputRowCount();
    }

    private static final List<ColumnRefOperator> COUNTABLE = List.of(DAY, NUMBER, BIG);

    @Test
    public void testClosedEndKeepsTheRowsOfItsValue() {
        for (ColumnRefOperator column : COUNTABLE) {
            for (int value = 0; value < VALUES; value++) {
                String message = column.getName() + " <= " + value;
                Assertions.assertEquals((double) ROWS_OF_VALUE * (value + 1), rows(compare(BinaryType.LE, column, value)),
                        2, message);
                Assertions.assertEquals((double) ROWS_OF_VALUE * value, rows(compare(BinaryType.LT, column, value)),
                        2, column.getName() + " < " + value);
            }
        }
    }

    @Test
    public void testOpenLowerEndDropsTheRowsOfItsValue() {
        for (ColumnRefOperator column : COUNTABLE) {
            // Past the last value no bucket stays, and the estimate takes a share of the whole range
            for (int value = 0; value < VALUES - 1; value++) {
                Assertions.assertEquals((double) ROWS_OF_VALUE * (VALUES - 1 - value),
                        rows(compare(BinaryType.GT, column, value)), 2, column.getName() + " > " + value);
            }
            for (int value = 0; value < VALUES; value++) {
                Assertions.assertEquals((double) ROWS_OF_VALUE * (VALUES - value),
                        rows(compare(BinaryType.GE, column, value)), 2, column.getName() + " >= " + value);
            }
        }
    }

    @Test
    public void testRangesOfDaysAndNumbersKeepTheRowsOfAllTheirValues() {
        for (ColumnRefOperator column : COUNTABLE) {
            for (int from = 0; from < VALUES; from += 3) {
                for (int to = from; to < VALUES; to += 4) {
                    String message = column.getName() + " " + from + ".." + to;
                    Assertions.assertEquals((double) ROWS_OF_VALUE * (to - from + 1), rows(Utils.compoundAnd(
                            compare(BinaryType.GE, column, from), compare(BinaryType.LE, column, to))), 3, message);
                    Assertions.assertEquals((double) ROWS_OF_VALUE * (to - from), rows(Utils.compoundAnd(
                            compare(BinaryType.GT, column, from), compare(BinaryType.LE, column, to))), 3, message);
                    Assertions.assertEquals((double) ROWS_OF_VALUE * (to - from), rows(Utils.compoundAnd(
                            compare(BinaryType.GE, column, from), compare(BinaryType.LT, column, to))), 3, message);
                }
            }
        }
    }

    @Test
    public void testClosedRangeIsTheHalfOpenRangeToTheNextValue() {
        for (ColumnRefOperator column : COUNTABLE) {
            for (int from = 0; from < VALUES - 1; from += 2) {
                for (int to = from; to < VALUES - 1; to += 3) {
                    Assertions.assertEquals(rows(Utils.compoundAnd(compare(BinaryType.GE, column, from),
                            compare(BinaryType.LT, column, to + 1))), rows(Utils.compoundAnd(
                            compare(BinaryType.GE, column, from), compare(BinaryType.LE, column, to))), 1e-9,
                            column.getName() + " " + from + ".." + to);
                    Assertions.assertEquals(rows(Utils.compoundAnd(compare(BinaryType.GE, column, from + 1),
                            compare(BinaryType.LE, column, to + 1))), rows(Utils.compoundAnd(
                            compare(BinaryType.GT, column, from), compare(BinaryType.LE, column, to + 1))), 1e-9,
                            column.getName() + " " + from + ".." + to);
                }
            }
        }
    }

    // A DOUBLE has values between 3 and 4: a comparison cuts the range at 3, and the end is no value to keep
    @Test
    public void testColumnOfNumbersWithFractionsKeepsItsRange() {
        Assertions.assertEquals(300, rows(compare(BinaryType.LE, REAL, 3)), 2);
        Assertions.assertEquals(300, rows(compare(BinaryType.LT, REAL, 3)), 2);
        Assertions.assertEquals(8800, rows(compare(BinaryType.GT, REAL, 3)), 2);
        Assertions.assertEquals(8800, rows(compare(BinaryType.GE, REAL, 3)), 2);
    }

    @Test
    public void testUpdateOfTheHistogramAtTheEndsOfTheValues() {
        ColumnStatistic number = table().getColumnStatistic(NUMBER);
        // At the last value of a bucket: its rows are in for <=, out for <
        Histogram upTo6 = BinaryPredicateStatisticCalculator.updateHistWithLessThan(number,
                Optional.of(ConstantOperator.createInt(6)), true).orElseThrow();
        Histogram below6 = BinaryPredicateStatisticCalculator.updateHistWithLessThan(number,
                Optional.of(ConstantOperator.createInt(6)), false).orElseThrow();
        Assertions.assertEquals(700, upTo6.getTotalRows());
        Assertions.assertEquals(600, below6.getTotalRows());
        // Inside a bucket: 3 is the range 3 to 4 of the six that hold the 600 rows, so 0 to 3 hold 400 of them
        Histogram upTo3 = BinaryPredicateStatisticCalculator.updateHistWithLessThan(number,
                Optional.of(ConstantOperator.createInt(3)), true).orElseThrow();
        Assertions.assertEquals(400, upTo3.getTotalRows(), 1);
        // The bucket ends on the value 3, with the 100 rows of it
        Assertions.assertEquals(3, upTo3.getBuckets().get(0).getUpper());
        Assertions.assertEquals(100, upTo3.getBuckets().get(0).getUpperRepeats());
        Histogram after3 = BinaryPredicateStatisticCalculator.updateHistWithGreaterThan(number,
                Optional.of(ConstantOperator.createInt(3)), false).orElseThrow();
        Assertions.assertEquals(9100 - 400, after3.getTotalRows(), 1);
        Assertions.assertEquals(4, after3.getBuckets().get(0).getLower());
        Histogram from3 = BinaryPredicateStatisticCalculator.updateHistWithGreaterThan(number,
                Optional.of(ConstantOperator.createInt(3)), true).orElseThrow();
        Assertions.assertEquals(9100 - 300, from3.getTotalRows(), 1);
        Assertions.assertEquals(3, from3.getBuckets().get(0).getLower());
    }

    // A most common value of the histogram is the day or the integer itself: it stays for <= and >= of its value only
    @Test
    public void testMostCommonValuesFollowTheClosedEnd() {
        // One bucket of 0 to 20 with 2000 rows on the first twenty values and 100 on the last, and two hot values
        Histogram histogram = new Histogram(List.of(new Bucket(0, 20, 2100L, 100L, 21L)),
                Map.of("5", 1000L, "6", 2000L));
        ColumnStatistic statistic = ColumnStatistic.builder().setMinValue(0).setMaxValue(20).setNullsFraction(0)
                .setAverageRowSize(4).setDistinctValuesCount(21).setHistogram(histogram).build();
        Statistics statistics = Statistics.builder().setOutputRowCount(5100)
                .addColumnStatistic(NUMBER, statistic).build();
        // 6 of the 20 ranges of 100 rows up to 6, and the hot value 5
        Assertions.assertEquals(600 + 1000, estimate(BinaryType.LE, 5, statistics), 2);
        Assertions.assertEquals(600 + 1000, estimate(BinaryType.LT, 6, statistics), 2);
        // 14 of the 20 ranges from 6, the 100 rows of 20, and the hot value 6
        Assertions.assertEquals(1400 + 100 + 2000, estimate(BinaryType.GT, 5, statistics), 2);
        Assertions.assertEquals(1400 + 100 + 2000, estimate(BinaryType.GE, 6, statistics), 2);
        Assertions.assertEquals(1300 + 100, estimate(BinaryType.GT, 6, statistics), 2);
        Assertions.assertEquals(700 + 1000 + 2000, estimate(BinaryType.LE, 6, statistics), 2);
    }

    private static double estimate(BinaryType type, int value, Statistics statistics) {
        return PredicateStatisticsCalculator.statisticsCalculate(compare(type, NUMBER, value), statistics)
                .getOutputRowCount();
    }

    // The rows of a random column of 400 integers, skewed towards zero, in the buckets of an equi-height histogram: a
    // bucket ends on a value with the rows of it as repeats. For every d, x <= d of the integers is closer to the
    // count of the rows than the cut at d of a DOUBLE column with the same histogram.
    @Test
    public void testClosedEndIsCloserToTheCountOfTheRowsThanTheCut() {
        Random random = new Random(20261018);
        int domain = 400;
        int[] counts = new int[domain];
        for (int i = 0; i < 100_000; i++) {
            double uniform = random.nextDouble();
            counts[(int) (uniform * uniform * domain)]++;
        }
        List<Bucket> buckets = new ArrayList<>();
        long cumulative = 0;
        long inBucket = 0;
        long distinct = 0;
        int lower = -1;
        for (int value = 0; value < domain; value++) {
            if (counts[value] == 0) {
                continue;
            }
            lower = lower < 0 ? value : lower;
            inBucket += counts[value];
            distinct++;
            if (inBucket >= 1000 || value == domain - 1) {
                cumulative += inBucket;
                buckets.add(new Bucket(lower, value, cumulative, (long) counts[value], distinct));
                lower = -1;
                inBucket = 0;
                distinct = 0;
            }
        }
        Histogram histogram = new Histogram(buckets, Map.of());
        ColumnStatistic statistic = ColumnStatistic.builder().setMinValue(0).setMaxValue(domain - 1)
                .setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(domain).setHistogram(histogram)
                .build();
        Statistics statistics = Statistics.builder().setOutputRowCount(100_000)
                .addColumnStatistic(NUMBER, statistic).addColumnStatistic(REAL, statistic).build();
        double errorOfIntegers = 0;
        double errorOfDoubles = 0;
        long exact = 0;
        for (int d = 0; d < domain - 1; d++) {
            exact += counts[d];
            double integers = PredicateStatisticsCalculator.statisticsCalculate(compare(BinaryType.LE, NUMBER, d),
                    statistics).getOutputRowCount();
            double doubles = PredicateStatisticsCalculator.statisticsCalculate(compare(BinaryType.LE, REAL, d),
                    statistics).getOutputRowCount();
            errorOfIntegers += Math.abs(integers - exact);
            errorOfDoubles += Math.abs(doubles - exact);
        }
        Assertions.assertTrue(errorOfIntegers < errorOfDoubles,
                "integers " + errorOfIntegers + ", doubles " + errorOfDoubles);
    }

    // The statistics are doubles: an integer past 2^53 has no next value among them, and x <= d keeps the rows of the
    // bucket that ends on d, the rows of d included
    @Test
    public void testIntegerWithoutNextValueAmongDoublesKeepsItsClosedEnd() {
        double lower = 0x1p60;
        double upper = 0x1p60 + 2048;
        Histogram histogram = new Histogram(List.of(new Bucket(lower, upper, 1000L, 100L, 5L)), Map.of());
        Statistics statistics = Statistics.builder().setOutputRowCount(1000).addColumnStatistic(BIG,
                ColumnStatistic.builder().setMinValue(lower).setMaxValue(upper).setNullsFraction(0)
                        .setAverageRowSize(8).setDistinctValuesCount(5).setHistogram(histogram).build()).build();
        Assertions.assertEquals(1000, PredicateStatisticsCalculator.statisticsCalculate(
                new BinaryPredicateOperator(BinaryType.LE, BIG, ConstantOperator.createBigint((long) upper)),
                statistics).getOutputRowCount(), 1e-9);
        Assertions.assertEquals(900, PredicateStatisticsCalculator.statisticsCalculate(
                new BinaryPredicateOperator(BinaryType.LT, BIG, ConstantOperator.createBigint((long) upper)),
                statistics).getOutputRowCount(), 1e-9);
    }

    // The column after x <= d ends on d and after x > d starts on d + 1, whatever type of days or integers it is
    @Test
    public void testColumnAfterTheComparisonStartsAndEndsOnItsValues() {
        for (ColumnRefOperator column : COUNTABLE) {
            ColumnStatistic upTo = PredicateStatisticsCalculator.statisticsCalculate(compare(BinaryType.LE, column, 10),
                    table()).getColumnStatistic(column);
            ColumnStatistic after = PredicateStatisticsCalculator.statisticsCalculate(compare(BinaryType.GT, column, 10),
                    table()).getColumnStatistic(column);
            Assertions.assertEquals(valueOf(column, 10), upTo.getHistogram().getBuckets()
                    .get(upTo.getHistogram().getBuckets().size() - 1).getUpper(), column.getName());
            Assertions.assertEquals(valueOf(column, 10), upTo.getMaxValue(), column.getName());
            Assertions.assertEquals(valueOf(column, 11), after.getHistogram().getBuckets().get(0).getLower(),
                    column.getName());
            Assertions.assertEquals(valueOf(column, 11), after.getMinValue(), column.getName());
        }
    }

    private static double valueOf(ColumnRefOperator column, int value) {
        return column == DAY ? seconds(value) : value;
    }
}
