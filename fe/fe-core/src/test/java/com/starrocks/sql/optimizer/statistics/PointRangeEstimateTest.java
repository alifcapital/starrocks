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

import com.starrocks.catalog.Function;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Both sides of a range on one column of whole numbers or days that keep one value are an equality on it: [a, a],
 * [a, a + 1), (a - 1, a + 1) and the like, and also when a function of the column that reduces to bounds on it gives the
 * sides. The estimate is the one of the equality with the same number wherever a is against the range of the
 * statistics, which can be older than the data; a column without statistics keeps its ranges. The table has 1,000,000
 * rows, and the column of 805 values has the share of one value of 1/805, or the head and the buckets of a histogram.
 */
public class PointRangeEstimateTest {
    private static final double ROWS = 1_000_000;
    private static final ColumnRefOperator D = new ColumnRefOperator(1, IntegerType.INT, "d", true);
    private static final ColumnRefOperator HISTOGRAM = new ColumnRefOperator(2, IntegerType.INT, "h", true);
    private static final ColumnRefOperator DAY = new ColumnRefOperator(3, DateType.DATE, "day", true);
    private static final ColumnRefOperator STATUS = new ColumnRefOperator(4, IntegerType.INT, "status", true);
    private static final ColumnRefOperator BIG = new ColumnRefOperator(5, IntegerType.BIGINT, "big", true);
    private static final ColumnRefOperator UNKNOWN = new ColumnRefOperator(6, IntegerType.INT, "u", true);
    // The columns of the table with an MCV list of status and gate: the head holds every row.
    private static final ColumnRefOperator S = new ColumnRefOperator(7, IntegerType.INT, "s", true);
    private static final ColumnRefOperator G = new ColumnRefOperator(8, IntegerType.INT, "g", true);
    private static final ColumnRefOperator AMOUNT = new ColumnRefOperator(9, IntegerType.BIGINT, "amount", true);
    // The days of a table whose statistics end before its data: 813 values from 17197 to 20737, a histogram and an MCV
    // list of the three hot days.
    private static final ColumnRefOperator STALE = new ColumnRefOperator(11, IntegerType.INT, "stale", true);

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.createDefaultCtx();
    }

    private static Statistics table() {
        // 17197 to 20727 days since 1970, and the same 805 days of 2024-01-01 to 2026-03-15 as dates.
        double from = Utils.getLongFromDateTime(LocalDateTime.of(2024, 1, 1, 0, 0));
        double to = Utils.getLongFromDateTime(LocalDateTime.of(2026, 3, 15, 0, 0));
        // 994,500 rows in two buckets and 5500 rows in two most common values: the rows of the table.
        Histogram histogram = new Histogram(List.of(new Bucket(17197, 20000, 500_000L, 100L, 400L),
                new Bucket(20001, 20727, 994_500L, 100L, 400L)), Map.of("20468", 3000L, "20469", 2500L));
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(D, new ColumnStatistic(17197, 20727, 0, 4, 805))
                .addColumnStatistic(HISTOGRAM, ColumnStatistic.builder().setMinValue(17197).setMaxValue(20727)
                        .setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(805).setHistogram(histogram)
                        .build())
                .addColumnStatistic(DAY, new ColumnStatistic(from, to, 0, 4, 805))
                .addColumnStatistic(STATUS, new ColumnStatistic(0, 5, 0, 4, 6))
                .addColumnStatistic(BIG, new ColumnStatistic(0, 1e12, 0, 8, 805))
                .addColumnStatistic(UNKNOWN, ColumnStatistic.unknown())
                .addColumnStatistic(S, new ColumnStatistic(1, 3, 0, 4, 3))
                .addColumnStatistic(G, new ColumnStatistic(0, 1, 0, 4, 2))
                .addColumnStatistic(AMOUNT, new ColumnStatistic(0, 1e6, 0, 8, 5000))
                .addColumnStatistic(STALE, ColumnStatistic.builder().setMinValue(17197).setMaxValue(20737)
                        .setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(813).setHistogram(new Histogram(
                                List.of(new Bucket(17197, 20000, 500_000L, 100L, 400L),
                                        new Bucket(20001, 20737, 988_000L, 100L, 400L)),
                                Map.of("20737", 5000L, "20726", 4000L, "20728", 3000L))).build())
                .addMultiColumnStatistics(Set.of(STALE), new MultiColumnCombinedStats(813, (long) ROWS, List.of(STALE),
                        List.of(new MultiColumnCombinedStats.McvEntry(List.of("20737"), 5000, List.of(5000L)),
                                new MultiColumnCombinedStats.McvEntry(List.of("20726"), 4000, List.of(4000L)),
                                new MultiColumnCombinedStats.McvEntry(List.of("20728"), 3000, List.of(3000L))),
                        List.of(0L)))
                .addMultiColumnStatistics(Set.of(S, G), new MultiColumnCombinedStats(6, (long) ROWS,
                        List.of(S, G), List.of(entry("1", "0", 400_000, 500_000, 750_000),
                        entry("1", "1", 100_000, 500_000, 250_000), entry("2", "0", 300_000, 400_000, 750_000),
                        entry("2", "1", 100_000, 400_000, 250_000), entry("3", "0", 50_000, 100_000, 750_000),
                        entry("3", "1", 50_000, 100_000, 250_000)), List.of(0L, 0L)))
                .build();
    }

    private static MultiColumnCombinedStats.McvEntry entry(String status, String gate, long count, long statusCount,
                                                           long gateCount) {
        return new MultiColumnCombinedStats.McvEntry(List.of(status, gate), count, List.of(statusCount, gateCount));
    }

    private static double rows(ScalarOperator predicate) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, table()).getOutputRowCount();
    }

    private static ScalarOperator compare(BinaryType type, ScalarOperator column, ConstantOperator value) {
        return new BinaryPredicateOperator(type, column, value);
    }

    private static ConstantOperator number(int value) {
        return ConstantOperator.createInt(value);
    }

    private static ConstantOperator day(int year, int month, int dayOfMonth) {
        return ConstantOperator.createDate(LocalDateTime.of(year, month, dayOfMonth, 0, 0));
    }

    // The forms of the range that keeps the value a.
    private static List<ScalarOperator> forms(ColumnRefOperator column, ConstantOperator below, ConstantOperator a,
                                              ConstantOperator above) {
        return List.of(
                Utils.compoundAnd(compare(BinaryType.GE, column, a), compare(BinaryType.LE, column, a)),
                Utils.compoundAnd(compare(BinaryType.LE, column, a), compare(BinaryType.GE, column, a)),
                Utils.compoundAnd(compare(BinaryType.GE, column, a), compare(BinaryType.LT, column, above)),
                Utils.compoundAnd(compare(BinaryType.GT, column, below), compare(BinaryType.LE, column, a)),
                Utils.compoundAnd(compare(BinaryType.GT, column, below), compare(BinaryType.LT, column, above)),
                // A side that does not change the value, and the same side twice.
                Utils.compoundAnd(List.of(compare(BinaryType.GE, column, a), compare(BinaryType.LE, column, a),
                        compare(BinaryType.GE, column, below), compare(BinaryType.LE, column, above))),
                Utils.compoundAnd(List.of(compare(BinaryType.GE, column, a), compare(BinaryType.GE, column, a),
                        compare(BinaryType.LE, column, a))));
    }

    @Test
    public void testPointRangeIsTheEqualityOnAnIntegerColumn() {
        double equality = rows(compare(BinaryType.EQ, D, number(20468)));
        Assertions.assertEquals(ROWS / 805, equality, 1e-6);
        for (ScalarOperator form : forms(D, number(20467), number(20468), number(20469))) {
            Assertions.assertEquals(equality, rows(form), 1e-6, form.toString());
        }
    }

    @Test
    public void testPointRangeIsTheEqualityOnTheHistogramOfAColumn() {
        // In the head: the rows of the value.
        Assertions.assertEquals(3000, rows(compare(BinaryType.EQ, HISTOGRAM, number(20468))), 1e-6);
        for (ScalarOperator form : forms(HISTOGRAM, number(20467), number(20468), number(20469))) {
            Assertions.assertEquals(3000, rows(form), 1e-6, form.toString());
        }
        // Outside of it: the rows of the equality, from the bucket.
        double bucket = rows(compare(BinaryType.EQ, HISTOGRAM, number(20500)));
        Assertions.assertTrue(bucket > 1 && bucket < 5000, String.valueOf(bucket));
        for (ScalarOperator form : forms(HISTOGRAM, number(20499), number(20500), number(20501))) {
            Assertions.assertEquals(bucket, rows(form), 1e-6, form.toString());
        }
    }

    @Test
    public void testPointRangeIsTheEqualityOnADate() {
        double equality = rows(compare(BinaryType.EQ, DAY, day(2025, 6, 15)));
        Assertions.assertEquals(ROWS / 805, equality, 1e-6);
        for (ScalarOperator form : forms(DAY, day(2025, 6, 14), day(2025, 6, 15), day(2025, 6, 16))) {
            Assertions.assertEquals(equality, rows(form), 1e-6, form.toString());
        }
    }

    @Test
    public void testPointRangeNextToAnotherCondition() {
        ScalarOperator status = compare(BinaryType.EQ, STATUS, number(3));
        double equality = rows(Utils.compoundAnd(status, compare(BinaryType.EQ, D, number(20468))));
        for (ScalarOperator form : forms(D, number(20467), number(20468), number(20469))) {
            Assertions.assertEquals(equality, rows(Utils.compoundAnd(status, form)), 1e-6, form.toString());
            Assertions.assertEquals(equality, rows(Utils.compoundAnd(form, status)), 1e-6, form.toString());
        }
    }

    @Test
    public void testRangesThatAreNotOneValueKeepTheirEstimate() {
        // [a, a + 1] keeps two values and (a, a + 1) none: the sides narrow the column one after the other.
        ScalarOperator two = Utils.compoundAnd(compare(BinaryType.GE, D, number(20468)),
                compare(BinaryType.LE, D, number(20469)));
        Statistics afterLower = PredicateStatisticsCalculator.statisticsCalculate(
                compare(BinaryType.GE, D, number(20468)), table());
        Assertions.assertEquals(PredicateStatisticsCalculator.statisticsCalculate(
                compare(BinaryType.LE, D, number(20469)), afterLower).getOutputRowCount(), rows(two), 1e-6);
        ScalarOperator empty = Utils.compoundAnd(compare(BinaryType.GT, D, number(20468)),
                compare(BinaryType.LT, D, number(20469)));
        Assertions.assertNotEquals(rows(compare(BinaryType.EQ, D, number(20468))), rows(empty), 1.0);
        // A lower side above the upper one keeps nothing, and is not an equality.
        ScalarOperator inverted = Utils.compoundAnd(compare(BinaryType.GE, D, number(20469)),
                compare(BinaryType.LE, D, number(20468)));
        Assertions.assertNotEquals(rows(compare(BinaryType.EQ, D, number(20468))), rows(inverted), 1.0);
        // Two columns do not make one.
        ScalarOperator crossed = Utils.compoundAnd(compare(BinaryType.GE, D, number(20468)),
                compare(BinaryType.LE, STATUS, number(20468)));
        Assertions.assertNotEquals(rows(compare(BinaryType.EQ, D, number(20468))), rows(crossed), 1.0);
    }

    @Test
    public void testValuesAtTheEndsOfTheType() {
        // The point at the largest int is an equality; > the largest and < the smallest are not read.
        ScalarOperator top = Utils.compoundAnd(compare(BinaryType.GE, D, number(Integer.MAX_VALUE)),
                compare(BinaryType.LE, D, number(Integer.MAX_VALUE)));
        Assertions.assertEquals(rows(compare(BinaryType.EQ, D, number(Integer.MAX_VALUE))), rows(top), 1e-6);
        ScalarOperator bottom = Utils.compoundAnd(compare(BinaryType.GT, D, number(Integer.MIN_VALUE)),
                compare(BinaryType.LE, D, number(Integer.MIN_VALUE + 1)));
        Assertions.assertTrue(rows(bottom) >= 0);
        // A BIGINT column.
        ScalarOperator big = Utils.compoundAnd(compare(BinaryType.GE, BIG, ConstantOperator.createBigint(7_000_000_000L)),
                compare(BinaryType.LT, BIG, ConstantOperator.createBigint(7_000_000_001L)));
        Assertions.assertEquals(rows(compare(BinaryType.EQ, BIG, ConstantOperator.createBigint(7_000_000_000L))),
                rows(big), 1e-6);
    }

    // The rows of the two sides one after the other, the estimate of a range on a column.
    private static double ranges(ColumnRefOperator column, ConstantOperator low, ConstantOperator high) {
        Statistics afterLower = PredicateStatisticsCalculator.statisticsCalculate(
                compare(BinaryType.GE, column, low), table());
        return PredicateStatisticsCalculator.statisticsCalculate(compare(BinaryType.LE, column, high), afterLower)
                .getOutputRowCount();
    }

    private static ScalarOperator point(ColumnRefOperator column, ConstantOperator value) {
        return Utils.compoundAnd(compare(BinaryType.GE, column, value), compare(BinaryType.LE, column, value));
    }

    @Test
    public void testColumnWithoutStatisticsKeepsItsRanges() {
        // The equality of a column without statistics takes the default share of the rows, which a point of the
        // range has no reason to take: the two sides cut the range, as the sides of any two values do.
        double point = rows(point(UNKNOWN, number(20468)));
        Assertions.assertEquals(ranges(UNKNOWN, number(20468), number(20468)), point, 1e-6);
        Assertions.assertNotEquals(rows(compare(BinaryType.EQ, UNKNOWN, number(20468))), point, 1.0);
        for (int a : new int[] {30000, 10000, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
            Assertions.assertEquals(ranges(UNKNOWN, number(a), number(a)), rows(point(UNKNOWN, number(a))), 1e-6,
                    String.valueOf(a));
            Assertions.assertNotEquals(rows(compare(BinaryType.EQ, UNKNOWN, number(a))), rows(point(UNKNOWN, number(a))),
                    1.0, String.valueOf(a));
        }
        // The two values next to each other are the same kind of estimate, and not fewer rows than the point.
        double two = rows(Utils.compoundAnd(compare(BinaryType.GE, UNKNOWN, number(20468)),
                compare(BinaryType.LE, UNKNOWN, number(20469))));
        Assertions.assertTrue(point <= two, point + " " + two);
    }

    // The estimate of every form of the point a is the estimate of the equality, with the same number.
    private static void checkPoint(ColumnRefOperator column, int a) {
        double equality = rows(compare(BinaryType.EQ, column, number(a)));
        Assertions.assertEquals(equality, rows(point(column, number(a))), 0.0, column.getName() + " " + a);
        if (a > Integer.MIN_VALUE + 1 && a < Integer.MAX_VALUE - 1) {
            for (ScalarOperator form : forms(column, number(a - 1), number(a), number(a + 1))) {
                Assertions.assertEquals(equality, rows(form), 0.0, column.getName() + " " + a + ": " + form);
            }
        }
    }

    @Test
    public void testPointIsTheEqualityWhereverItIsAgainstTheRangeOfTheStatistics() {
        // The equality of a value that the histogram does not hold takes the share of one of the values outside of
        // the head, whether the value is in the range of the column or not: 994,500 rows of 803 values.
        Assertions.assertEquals(994_500.0 / 803, rows(compare(BinaryType.EQ, HISTOGRAM, number(30000))), 1e-6);
        // The ends of the range, a value inside of it, two steps and far past each end, and the ends of the type.
        for (int a : new int[] {17197, 20727, 20500, 20728, 20729, 20730, 17195, 30000, 10000, Integer.MAX_VALUE,
                Integer.MIN_VALUE}) {
            checkPoint(HISTOGRAM, a);
        }
        Assertions.assertEquals(3000, rows(point(HISTOGRAM, number(20468))), 1e-6);
        // Two values past the range are two values: the sides keep the estimate of the ranges.
        Assertions.assertEquals(ranges(HISTOGRAM, number(30000), number(30001)), rows(Utils.compoundAnd(
                compare(BinaryType.GE, HISTOGRAM, number(30000)), compare(BinaryType.LE, HISTOGRAM, number(30001)))),
                1e-6);
    }

    @Test
    public void testPointNextToTheEqualitiesOfAnMcvListIsTheEquality() {
        ScalarOperator status = compare(BinaryType.EQ, S, number(1));
        ScalarOperator gate = compare(BinaryType.EQ, G, number(0));
        ScalarOperator amount = compare(BinaryType.EQ, AMOUNT, ConstantOperator.createBigint(1000));
        double equality = rows(Utils.compoundAnd(List.of(status, gate, amount)));
        // The head of status and gate holds 400,000 rows of the pair, and amount takes its share with the decay.
        Assertions.assertTrue(equality > 10 && equality < 400_000, String.valueOf(equality));
        ScalarOperator point = point(AMOUNT, ConstantOperator.createBigint(1000));
        // The point goes the way of the equality wherever it stands among the conjuncts.
        Assertions.assertEquals(equality, rows(Utils.compoundAnd(List.of(status, gate, point))), 1e-6);
        Assertions.assertEquals(equality, rows(Utils.compoundAnd(List.of(point, status, gate))), 1e-6);
        Assertions.assertEquals(equality, rows(Utils.compoundAnd(List.of(status, point, gate))), 1e-6);
        Assertions.assertEquals(equality, rows(Utils.compoundAnd(List.of(status, gate,
                compare(BinaryType.GE, AMOUNT, ConstantOperator.createBigint(1000)),
                compare(BinaryType.LT, AMOUNT, ConstantOperator.createBigint(1001))))), 1e-6);
    }

    // days_add('1970-01-01', d): the day of the column as a DATETIME.
    private static ScalarOperator dayOf(ColumnRefOperator column) {
        Type[] types = {DateType.DATETIME, IntegerType.INT};
        Function function = ExprUtils.getBuiltinFunction("days_add", types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator("days_add", DateType.DATETIME,
                List.of(ConstantOperator.createDatetime(LocalDateTime.of(1970, 1, 1, 0, 0)), column), function);
    }

    private static ConstantOperator time(int month, int dayOfMonth) {
        return ConstantOperator.createDatetime(LocalDateTime.of(2026, month, dayOfMonth, 0, 0));
    }

    // date(days_add('1970-01-01', d)), the form of DATE(DATE_ADD('1970-01-01', INTERVAL d DAY)).
    private static ScalarOperator dateOfDay(ColumnRefOperator column) {
        Type[] types = {DateType.DATETIME};
        Function function = ExprUtils.getBuiltinFunction("date", types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator("date", DateType.DATE, List.of(dayOf(column)), function);
    }

    @Test
    public void testBetweenTheSameDaysOfTheFunctionIsTheEquality() {
        double equality = rows(compare(BinaryType.EQ, D, number(20468)));
        ScalarOperator between = Utils.compoundAnd(compare(BinaryType.GE, dateOfDay(D), day(2026, 1, 15)),
                compare(BinaryType.LE, dateOfDay(D), day(2026, 1, 15)));
        Assertions.assertEquals(equality, rows(between), 1e-6);
        // Two days are not one.
        ScalarOperator two = Utils.compoundAnd(compare(BinaryType.GE, dateOfDay(D), day(2026, 1, 15)),
                compare(BinaryType.LE, dateOfDay(D), day(2026, 1, 16)));
        Assertions.assertTrue(rows(two) != equality);
    }

    @Test
    public void testPointGivenByAFunctionOfTheColumnIsTheEquality() {
        // 2026-01-15 is the day 20468 since 1970.
        double equality = rows(compare(BinaryType.EQ, D, number(20468)));
        Assertions.assertEquals(ROWS / 805, equality, 1e-6);
        ScalarOperator oneDay = Utils.compoundAnd(compare(BinaryType.GE, dayOf(D), time(1, 15)),
                compare(BinaryType.LT, dayOf(D), time(1, 16)));
        Assertions.assertEquals(equality, rows(oneDay), 1e-6);
        // Both orders, the closed form and a side on the column next to a side on the function.
        Assertions.assertEquals(equality, rows(Utils.compoundAnd(compare(BinaryType.LT, dayOf(D), time(1, 16)),
                compare(BinaryType.GE, dayOf(D), time(1, 15)))), 1e-6);
        Assertions.assertEquals(equality, rows(Utils.compoundAnd(compare(BinaryType.GE, dayOf(D), time(1, 15)),
                compare(BinaryType.LE, D, number(20468)))), 1e-6);
        // Two days are two values: the sides keep the estimate of the ranges.
        ScalarOperator twoDays = Utils.compoundAnd(compare(BinaryType.GE, dayOf(D), time(1, 15)),
                compare(BinaryType.LT, dayOf(D), time(1, 17)));
        Assertions.assertTrue(rows(twoDays) != equality);
    }

    @Test
    public void testPointOnStatisticsThatEndBeforeTheDataIsTheEquality() {
        // The statistics end at 20737 and the data goes on to 20739: the rows of a day after the end are those of a value
        // that is not in the head, (1,000,000 - 12,000) / 810, as the equality takes them.
        double tail = 988_000.0 / 810;
        Assertions.assertEquals(tail, rows(compare(BinaryType.EQ, STALE, number(20739))), 1e-6);
        for (int a : new int[] {20738, 20739, 20740, 25000, 30000, 17196, 10000, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
            checkPoint(STALE, a);
            Assertions.assertEquals(tail, rows(point(STALE, number(a))), 1e-6, String.valueOf(a));
        }
        // The days of the head and of the buckets, and the ends of the range.
        Assertions.assertEquals(5000, rows(point(STALE, number(20737))), 1e-6);
        Assertions.assertEquals(4000, rows(point(STALE, number(20726))), 1e-6);
        for (int a : new int[] {17197, 20000, 20001, 20737}) {
            checkPoint(STALE, a);
        }
        // Two days that are the end of the data are not a point: they keep the estimate of the two sides.
        ScalarOperator twoDays = Utils.compoundAnd(compare(BinaryType.GE, STALE, number(20738)),
                compare(BinaryType.LE, STALE, number(20739)));
        Assertions.assertEquals(ranges(STALE, number(20738), number(20739)), rows(twoDays), 1e-6);
    }

    @Test
    public void testPointGivenByAFunctionOfTheColumnIsTheEqualityPastTheRange() {
        // The days of d are 17197 to 20727. The sides of one day through days_add are the equality on d.
        for (int a : new int[] {20727, 20728, 20730, 25000, 17196, 10000}) {
            ConstantOperator from = ConstantOperator.createDatetime(LocalDate.ofEpochDay(a).atStartOfDay());
            ConstantOperator to = ConstantOperator.createDatetime(LocalDate.ofEpochDay(a + 1L).atStartOfDay());
            ScalarOperator oneDay = Utils.compoundAnd(compare(BinaryType.GE, dayOf(D), from),
                    compare(BinaryType.LT, dayOf(D), to));
            Assertions.assertEquals(rows(compare(BinaryType.EQ, D, number(a))), rows(oneDay), 0.0, String.valueOf(a));
        }
    }
}
