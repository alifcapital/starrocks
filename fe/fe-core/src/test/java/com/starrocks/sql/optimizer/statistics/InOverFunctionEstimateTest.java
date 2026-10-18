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
import com.starrocks.common.util.StringDateFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Supplier;

/**
 * IN over a function of a column that its bounds on the column read, over a string date column and over a prefix of it
 * is the OR of the equalities: the equality of such an expression with a constant is estimated by the bound on the
 * column, by the cast of the string or by the period of the prefix, and the IN had the default of an IN. NOT IN is the
 * rows where the expression is not NULL less those of the IN. The table has 1,000,000 rows, and d is the day since 1970
 * of 805 values from 17197 to 20727.
 */
public class InOverFunctionEstimateTest {
    private static final double ROWS = 1_000_000;
    private static final ColumnRefOperator D = new ColumnRefOperator(1, IntegerType.INT, "d", true);
    private static final ColumnRefOperator TEXT = new ColumnRefOperator(2, VarcharType.VARCHAR, "created_at", true);
    private static final ColumnRefOperator E = new ColumnRefOperator(3, IntegerType.INT, "e", true);
    private static final ColumnRefOperator F = new ColumnRefOperator(4, VarcharType.VARCHAR, "f", true);
    private static final ColumnRefOperator A = new ColumnRefOperator(5, IntegerType.INT, "a", true);
    private static final StringDateFormat ISO = StringDateFormat.parse("%Y-%m-%dT%H:%i:%s.%fZ");

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

    private static Statistics days() {
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(D, new ColumnStatistic(17197, 20727, 0, 4, 805)).build();
    }

    // The strings of 9 days of 2400 rows with one bucket a day, and a hot value of 5000 rows.
    private static Statistics strings() {
        List<Bucket> buckets = new ArrayList<>();
        for (int day = 11; day <= 19; day++) {
            buckets.add(new StringBucket(String.format("2026-08-%02dT00:00:00.000000Z", day),
                    String.format("2026-08-%02dT23:00:00.000000Z", day), (day - 10) * 2400L, 100, 24));
        }
        Histogram histogram = Histogram.forStrings(buckets, Map.of("2026-08-15T06:30:00.000000Z", 5000L));
        return Statistics.builder().setOutputRowCount(9 * 2400 + 5000)
                .addColumnStatistic(TEXT, ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(27)
                        .setDistinctValuesCount(9 * 24 + 1)
                        .setStringBounds(new StringBounds("2026-08-11T00:00:00.000000Z",
                                "2026-08-19T23:00:00.000000Z", 200, false)).setHistogram(histogram).build())
                .build();
    }

    // days_add('1970-01-01', d), the day of the column as a DATETIME.
    private static ScalarOperator day() {
        Type[] types = {DateType.DATETIME, IntegerType.INT};
        Function function = ExprUtils.getBuiltinFunction("days_add", types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator("days_add", DateType.DATETIME,
                List.of(ConstantOperator.createDatetime(LocalDateTime.of(1970, 1, 1, 0, 0)), D), function);
    }

    private static ConstantOperator date(int month, int dayOfMonth) {
        return ConstantOperator.createDatetime(LocalDateTime.of(2026, month, dayOfMonth, 0, 0));
    }

    private static ScalarOperator eq(ScalarOperator left, ScalarOperator right) {
        return new BinaryPredicateOperator(BinaryType.EQ, left, right);
    }

    private static ScalarOperator in(boolean not, ScalarOperator left, ScalarOperator... values) {
        List<ScalarOperator> children = new ArrayList<>();
        children.add(left);
        children.addAll(List.of(values));
        return new InPredicateOperator(not, children.toArray(new ScalarOperator[0]));
    }

    private static <T> T inScope(Supplier<T> action) {
        return PredicateStatisticsCalculator.withStringDateFormats(column -> TEXT.equals(column) ? ISO : null, action);
    }

    private static double rows(ScalarOperator predicate) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, days()).getOutputRowCount();
    }

    private static double stringRows(ScalarOperator predicate) {
        return inScope(() -> PredicateStatisticsCalculator.statisticsCalculate(predicate, strings())
                .getOutputRowCount());
    }

    @Test
    public void testInOverTheDayIsTheOrOfItsEqualities() {
        // The day 2026-01-15 is 20468 and 2026-01-16 is 20469; the equality has the share of one value of 805.
        double one = rows(eq(day(), date(1, 15)));
        Assertions.assertEquals(ROWS / 805, one, 1e-6);
        Assertions.assertEquals(one, rows(eq(D, ConstantOperator.createInt(20468))), 1e-6);
        // The OR of two equalities of one expression takes the rows of the first and of the second: no row of a day is
        // among those of another.
        double or = rows(Utils.compoundOr(eq(day(), date(1, 15)), eq(day(), date(1, 16))));
        Assertions.assertEquals(2 * one, or, 1e-6);
        Assertions.assertEquals(or, rows(in(false, day(), date(1, 15), date(1, 16))), 1e-6);
        // Three values, a repeated one, and a NULL that matches nothing.
        Assertions.assertEquals(or, rows(in(false, day(), date(1, 15), date(1, 16), date(1, 15))), 1e-6);
        Assertions.assertEquals(or, rows(in(false, day(), date(1, 15), date(1, 16),
                ConstantOperator.createNull(DateType.DATETIME))), 1e-6);
        Assertions.assertEquals(one, rows(in(false, day(), date(1, 15))), 1e-6);
        // The IN on the column itself is of the same size.
        Assertions.assertEquals(or, rows(in(false, D, ConstantOperator.createInt(20468), ConstantOperator.createInt(20469))),
                1e-6);
    }

    @Test
    public void testInOfDaysIsTheSumOfTheEstimatesOfEachDay() {
        // The days of a column of whole numbers: each has the share of one value, and k days have k shares.
        double one = rows(eq(D, ConstantOperator.createInt(20468)));
        List<ScalarOperator> values = new ArrayList<>();
        double previous = 0;
        for (int k = 1; k <= 20; k++) {
            values.add(ConstantOperator.createInt(20467 + k));
            double rows = rows(in(false, D, values.toArray(new ScalarOperator[0])));
            Assertions.assertEquals(k * one, rows, 1e-6, "k = " + k);
            Assertions.assertTrue(rows > previous);
            previous = rows;
        }
        // The days of a function of the column.
        List<ScalarOperator> dates = new ArrayList<>();
        previous = 0;
        for (int k = 1; k <= 20; k++) {
            dates.add(ConstantOperator.createDatetime(LocalDateTime.of(2026, 1, 14, 0, 0).plusDays(k)));
            double rows = rows(in(false, day(), dates.toArray(new ScalarOperator[0])));
            Assertions.assertEquals(k * one, rows, 1e-6, "k = " + k);
            Assertions.assertTrue(rows > previous);
            previous = rows;
        }
        // The days of the first ten characters of a column of strings: the sum of the estimates of the days, of the
        // days of the table and of the days it does not hold.
        ScalarOperator prefix = prefixOf("left", 10);
        List<ScalarOperator> days = new ArrayList<>();
        double sum = 0;
        previous = 0;
        for (int day = 1; day <= 25; day++) {
            String text = String.format("2026-08-%02d", day);
            sum += stringRows(eq(prefix, text(text)));
            days.add(text(text));
            double rows = stringRows(in(false, prefix, days.toArray(new ScalarOperator[0])));
            Assertions.assertEquals(Math.min(sum, strings().getOutputRowCount()), rows, 1e-6, "day " + day);
            Assertions.assertTrue(rows >= previous);
            previous = rows;
        }
        // The days of the table are 2400 rows each and the hot one has 5000 more: 26600 rows, all of the table.
        Assertions.assertEquals(strings().getOutputRowCount(), previous, 1e-6);
        Assertions.assertEquals(2400 + 7400, stringRows(in(false, prefix, text("2026-08-14"), text("2026-08-15"))), 1e-6);
    }

    // The days with the columns of the groups and of the parts: e from 0 to 100 and f of 20 values, which the groups
    // read, and a of 100 values.
    private static Statistics wide() {
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(D, new ColumnStatistic(17197, 20727, 0, 4, 805))
                .addColumnStatistic(E, new ColumnStatistic(0, 100, 0, 4, 101))
                .addColumnStatistic(F, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 20))
                .addColumnStatistic(A, new ColumnStatistic(1, 100, 0, 4, 100)).build();
    }

    private static double wideRows(ScalarOperator predicate) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, wide()).getOutputRowCount();
    }

    // (e >= 0 OR f = 'Xi'), the group that keeps every row: the OR of two expressions.
    private static List<ScalarOperator> groups(int count) {
        List<ScalarOperator> groups = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            groups.add(Utils.compoundOr(new BinaryPredicateOperator(BinaryType.GE, E, ConstantOperator.createInt(0)),
                    eq(F, ConstantOperator.createVarchar("X" + i))));
        }
        return groups;
    }

    private static ScalarOperator andGroups(ScalarOperator predicate, int count) {
        List<ScalarOperator> conjuncts = new ArrayList<>();
        conjuncts.add(predicate);
        conjuncts.addAll(groups(count));
        return Utils.compoundAnd(conjuncts);
    }

    private static ScalarOperator[] ints(int... values) {
        ScalarOperator[] constants = new ScalarOperator[values.length];
        for (int i = 0; i < values.length; i++) {
            constants[i] = ConstantOperator.createInt(values[i]);
        }
        return constants;
    }

    private static ScalarOperator interval(int from, int to) {
        return Utils.compoundAnd(new BinaryPredicateOperator(BinaryType.GE, D, ConstantOperator.createInt(from)),
                new BinaryPredicateOperator(BinaryType.LT, D, ConstantOperator.createInt(to)));
    }

    @Test
    public void testInOverTheDayNextToManyOrGroupsIsNotAnotherGroup() {
        // The groups keep every row, so the IN of two days is two values of 805 whatever the number of groups, as the IN
        // on the column is. The OR that the IN becomes is the OR of one expression, which costs a linear number of
        // estimates, and it must not take the place of the sixteenth group.
        double two = 2 * ROWS / 805;
        ScalarOperator inDays = in(false, day(), date(1, 15), date(1, 16));
        ScalarOperator inColumn = in(false, D, ints(20468, 20469));
        for (int groups : List.of(0, 1, 15, 16)) {
            Assertions.assertEquals(two, wideRows(andGroups(inDays, groups)), 1e-6, "groups " + groups);
            Assertions.assertEquals(two, wideRows(andGroups(inColumn, groups)), 1e-6, "groups " + groups);
        }
        // The OR of intervals of one expression is such an OR as well; the two intervals have no common point and add.
        ScalarOperator first = interval(18000, 18010);
        ScalarOperator second = interval(19000, 19010);
        double sum = wideRows(first) + wideRows(second);
        Assertions.assertEquals(2 * ROWS * 10 / 3530, sum, 1e-6);
        for (int groups : List.of(0, 15, 16)) {
            Assertions.assertEquals(sum, wideRows(andGroups(Utils.compoundOr(first, second), groups)), 1e-6,
                    "groups " + groups);
        }
    }

    @Test
    public void testOrOfTwoExpressionsIsStillAGroup() {
        // The OR of d = 20468 and e = 5 is a group: with 15 other groups there are 16 and the estimate is of the OR, with
        // 16 others there are 17 and the estimate is the one of an OR of many groups, the greater of the two parts.
        ScalarOperator or = Utils.compoundOr(eq(D, ConstantOperator.createInt(20468)),
                eq(E, ConstantOperator.createInt(5)));
        double regular = wideRows(or);
        Assertions.assertEquals(regular, wideRows(andGroups(or, 15)), 1e-6);
        Assertions.assertEquals(Math.max(ROWS / 805, ROWS / 101), wideRows(andGroups(or, 16)), 1e-6);
        Assertions.assertTrue(regular > wideRows(andGroups(or, 16)));
    }

    private static ScalarOperator partOfDays(int a, boolean byFunction) {
        ScalarOperator days = byFunction ? in(false, day(), date(1, 15), date(1, 16), date(1, 17))
                : in(false, D, ints(20468, 20469, 20470));
        return Utils.compoundAnd(eq(A, ConstantOperator.createInt(a)), days);
    }

    private static ScalarOperator orOfParts(int count, boolean byFunction) {
        List<ScalarOperator> parts = new ArrayList<>();
        for (int a = 1; a <= count; a++) {
            parts.add(partOfDays(a, byFunction));
        }
        return Utils.compoundOr(parts);
    }

    @Test
    public void testOrOfSixteenPartsWithAnInOverTheDayIsNotBelowItsSubsetOfEight() {
        // Each part is a = k AND the IN of three days; the IN becomes an OR of one expression inside each part. They are
        // not groups: the OR of sixteen parts is one, and is estimated as the OR of eight is, by adding the parts.
        for (boolean byFunction : List.of(true, false)) {
            double eight = wideRows(orOfParts(8, byFunction));
            double sixteen = wideRows(orOfParts(16, byFunction));
            Assertions.assertTrue(sixteen > eight, byFunction + " " + sixteen + " " + eight);
        }
        // The same parts with the IN on the column, which is no OR, give the same estimates.
        Assertions.assertEquals(wideRows(orOfParts(8, false)), wideRows(orOfParts(8, true)), 1e-6);
        Assertions.assertEquals(wideRows(orOfParts(16, false)), wideRows(orOfParts(16, true)), 1e-6);
        // Each part is 3 values of 805 in 1 of 100 values of a: 37.27 rows. The estimate of an OR of parts that are not
        // apart takes from the sum the rows that each part has in common with the earlier ones, so it is below the sum.
        double part = ROWS / 100 * 3 / 805;
        Assertions.assertEquals(part, wideRows(partOfDays(1, true)), 1e-6);
        Assertions.assertEquals(291.1366459627329, wideRows(orOfParts(8, true)), 1e-6);
        Assertions.assertEquals(581.2732919254659, wideRows(orOfParts(16, true)), 1e-6);
        Assertions.assertTrue(wideRows(orOfParts(8, true)) < 8 * part);
        Assertions.assertTrue(wideRows(orOfParts(16, true)) < 16 * part);
    }

    @Test
    public void testOrderOfTheValuesDoesNotChangeTheEstimate() {
        Assertions.assertEquals(rows(in(false, day(), date(1, 15), date(1, 16))),
                rows(in(false, day(), date(1, 16), date(1, 15))), 1e-6);
    }

    @Test
    public void testLongListKeepsTheDefault() {
        // 1025 values are one more than the list that is estimated as equalities: the default of an IN, the half of the
        // rows.
        int limit = 1024;
        ScalarOperator[] values = new ScalarOperator[limit + 1];
        for (int i = 0; i < values.length; i++) {
            values[i] = ConstantOperator.createDatetime(LocalDateTime.of(2000, 1, 1, 0, 0).plusDays(i));
        }
        Assertions.assertEquals(ROWS / 2, rows(in(false, day(), values)), 1e-6);
        // 1024 values are equalities: the one row an estimate takes for each of the days that the table does not hold,
        // and not the half of the rows.
        ScalarOperator[] equalities = Arrays.copyOf(values, limit);
        Assertions.assertEquals(limit, rows(in(false, day(), equalities)), 1e-6);
    }

    @Test
    public void testInOfTheLongestListIsEstimatedInLessThanHalfASecond() {
        int limit = 1024;
        // By a monotone function of the column: 500 days of the column, the last ones, and the days after them.
        ScalarOperator[] dates = new ScalarOperator[limit];
        List<ScalarOperator> dateEqualities = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            dates[i] = ConstantOperator.createDatetime(java.time.LocalDate.ofEpochDay(20727 - 499 + i).atStartOfDay());
            dateEqualities.add(eq(day(), dates[i]));
        }
        // By a prefix of a string date: the last day of the table, which has 2400 rows, and the days after it.
        ScalarOperator prefix = prefixOf("left", 10);
        ScalarOperator[] days = new ScalarOperator[limit];
        List<ScalarOperator> dayEqualities = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            days[i] = text(java.time.LocalDate.of(2026, 8, 19).plusDays(i).toString());
            dayEqualities.add(eq(prefix, days[i]));
        }
        // The first estimate of a JVM loads the classes and runs cold, which is not what the planner of a running FE
        // pays: we time the second one.
        long start = System.nanoTime();
        double inRows = rows(in(false, day(), dates));
        long coldFunctionMillis = (System.nanoTime() - start) / 1_000_000;
        start = System.nanoTime();
        double stringRows = stringRows(in(false, prefix, days));
        long coldPrefixMillis = (System.nanoTime() - start) / 1_000_000;
        start = System.nanoTime();
        rows(in(false, day(), dates));
        long functionMillis = (System.nanoTime() - start) / 1_000_000;
        start = System.nanoTime();
        stringRows(in(false, prefix, days));
        long prefixMillis = (System.nanoTime() - start) / 1_000_000;
        System.out.println("MEASURED IN of " + limit + " values: function " + coldFunctionMillis + " ms cold, " + functionMillis
                + " ms; prefix of a string date " + coldPrefixMillis + " ms cold, " + prefixMillis + " ms");
        Assertions.assertTrue(functionMillis < 500, "function " + functionMillis + " ms");
        Assertions.assertTrue(prefixMillis < 500, "prefix " + prefixMillis + " ms");
        // The OR of the equalities has the same rows: 500 days of 805 values of the column and the one row an estimate
        // takes for each of the others; the 2400 rows of the last day of the table and one row for each day after it.
        Assertions.assertEquals(rows(Utils.compoundOr(dateEqualities)), inRows, 1e-6);
        Assertions.assertEquals(500 * ROWS / 805 + (limit - 500), inRows, 1e-6);
        Assertions.assertEquals(stringRows(Utils.compoundOr(dayEqualities)), stringRows, 1e-6);
        Assertions.assertEquals(2400 + (limit - 1), stringRows, 1e-6);
    }

    @Test
    public void testNotInIsTheRestOfTheRowsOfTheInOverTheDay() {
        ScalarOperator in = in(false, day(), date(1, 15), date(1, 16));
        ScalarOperator notIn = in(true, day(), date(1, 15), date(1, 16));
        Assertions.assertEquals(ROWS - rows(in), rows(notIn), 1e-6);
        // NOT of the IN is the same predicate once the optimizer turns it into NOT IN, and as an estimate of NOT.
        Assertions.assertEquals(rows(notIn), rows(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT, in)), 1e-6);
        // A repeated value and the order do not change it, and a NULL in the list leaves no row, only the one row that
        // an estimate keeps: x NOT IN (a, NULL) is never true.
        Assertions.assertEquals(rows(notIn), rows(in(true, day(), date(1, 16), date(1, 15), date(1, 16))), 1e-6);
        Assertions.assertEquals(1, rows(in(true, day(), date(1, 15), ConstantOperator.createNull(DateType.DATETIME))),
                1e-6);
        // The column itself.
        Assertions.assertEquals(ROWS - rows(in(false, D, ConstantOperator.createInt(20468),
                ConstantOperator.createInt(20469))), rows(in(true, D, ConstantOperator.createInt(20468),
                ConstantOperator.createInt(20469))), 1e-6);
    }

    @Test
    public void testNotInWithANullInTheListIsFalse() {
        ConstantOperator nullInt = ConstantOperator.createNull(IntegerType.INT);
        ConstantOperator nullText = ConstantOperator.createNull(VarcharType.VARCHAR);
        // Never true, whatever the left side is: the estimate of a false condition, which keeps the one row an estimate
        // keeps. A column of whole numbers, with the NULL at any place of the list, and the list of a NULL alone.
        double none = rows(ConstantOperator.FALSE);
        Assertions.assertEquals(1, none, 1e-6);
        ConstantOperator a = ConstantOperator.createInt(20468);
        ConstantOperator b = ConstantOperator.createInt(20469);
        for (ScalarOperator notIn : List.of(in(true, D, a, nullInt), in(true, D, nullInt, a), in(true, D, a, nullInt, b),
                in(true, D, nullInt), in(true, D, nullInt, nullInt))) {
            Assertions.assertEquals(none, rows(notIn), 1e-6, notIn.toString());
        }
        // A function of the column, a string date column, a prefix of it, and an expression no bound is read from.
        Assertions.assertEquals(none, rows(in(true, day(), date(1, 15), ConstantOperator.createNull(DateType.DATETIME))),
                1e-6);
        Assertions.assertEquals(none, stringRows(in(true, TEXT, text("2026-08-15T06:30:00.000000Z"), nullText)), 1e-6);
        Assertions.assertEquals(none, stringRows(in(true, prefixOf("left", 10), text("2026-08-15"), nullText)), 1e-6);
        Assertions.assertEquals(none, stringRows(in(true, call("lower", TEXT), text("a"), nullText)), 1e-6);
        Assertions.assertEquals(none, stringRows(in(true, TEXT, nullText)), 1e-6);
        // Within an OR the false part adds nothing, and a NOT of it keeps what the estimate of NOT keeps.
        ScalarOperator equality = eq(D, a);
        Assertions.assertEquals(rows(equality), rows(Utils.compoundOr(in(true, D, a, nullInt), equality)), 1e-6);
        // An IN with a NULL, and a NOT IN without one, are estimated as before.
        Assertions.assertEquals(rows(in(false, D, a)), rows(in(false, D, a, nullInt)), 1e-6);
        Assertions.assertEquals(ROWS - 2 * ROWS / 805, rows(in(true, D, a, b)), 1e-6);
    }

    @Test
    public void testInAndNotInKeepTheNonNullRowsTogether() {
        // 20% of the rows of d are NULL: a NULL day is in neither of the two.
        Statistics withNulls = Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(D, new ColumnStatistic(17197, 20727, 0.2, 4, 805)).build();
        ScalarOperator in = in(false, day(), date(1, 15), date(1, 16), date(2, 1));
        ScalarOperator notIn = in(true, day(), date(1, 15), date(1, 16), date(2, 1));
        double inRows = PredicateStatisticsCalculator.statisticsCalculate(in, withNulls).getOutputRowCount();
        double notInRows = PredicateStatisticsCalculator.statisticsCalculate(notIn, withNulls).getOutputRowCount();
        Assertions.assertEquals(ROWS * 0.8, inRows + notInRows, 1e-6);
        Assertions.assertTrue(inRows > 0 && notInRows > 0.7 * ROWS);
    }

    @Test
    public void testNotInOverAStringDateIsTheRestOfTheRows() {
        ConstantOperator hot = ConstantOperator.createVarchar("2026-08-15T06:30:00.000000Z");
        ConstantOperator other = ConstantOperator.createVarchar("2026-08-14T10:00:00.000000Z");
        double all = strings().getOutputRowCount();
        Assertions.assertEquals(all - stringRows(in(false, TEXT, hot, other)), stringRows(in(true, TEXT, hot, other)),
                1e-6);
        Assertions.assertEquals(all - 5000, stringRows(in(true, TEXT, hot)), 1e-6);
    }

    private static ScalarOperator call(String name, ScalarOperator... arguments) {
        Type[] types = new Type[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            types[i] = arguments[i].getType();
        }
        Function function = ExprUtils.getBuiltinFunction(name, types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(name, VarcharType.VARCHAR, List.of(arguments), function);
    }

    private static ScalarOperator prefixOf(String name, int length) {
        return name.startsWith("sub")
                ? call(name, TEXT, ConstantOperator.createInt(1), ConstantOperator.createInt(length))
                : call(name, TEXT, ConstantOperator.createInt(length));
    }

    private static ConstantOperator text(String value) {
        return ConstantOperator.createVarchar(value);
    }

    @Test
    public void testInOverAPrefixOfAStringDateIsTheOrOfItsPeriods() {
        for (String name : List.of("left", "strleft", "substr", "substring")) {
            ScalarOperator prefix = prefixOf(name, 10);
            ScalarOperator in = in(false, prefix, text("2026-08-14"), text("2026-08-16"));
            double or = stringRows(Utils.compoundOr(eq(prefix, text("2026-08-14")), eq(prefix, text("2026-08-16"))));
            Assertions.assertEquals(or, stringRows(in), 1e-6, name);
            // The two days of the table: the period of each day, as the LIKE of the day.
            double days = stringRows(eq(TEXT, text("2026-08-14T10:00:00.000000Z")));
            Assertions.assertTrue(or > 2 * days, or + " " + days);
            Assertions.assertTrue(or < stringRows(in(false, prefix, text("2026-08-14"), text("2026-08-15"),
                    text("2026-08-16"), text("2026-08-17"))), name);
            // The IN is far from the default of an IN, the half of the rows.
            Assertions.assertNotEquals(strings().getOutputRowCount() / 2, stringRows(in), 1.0, name);
            // NOT IN is the rest of the rows.
            Assertions.assertEquals(strings().getOutputRowCount() - or, stringRows(in(true, prefix, text("2026-08-14"),
                    text("2026-08-16"))), 1e-6, name);
        }
        // A month of the first seven characters.
        ScalarOperator month = prefixOf("left", 7);
        Assertions.assertEquals(stringRows(eq(month, text("2026-08"))), stringRows(in(false, month, text("2026-08"))), 1e-6);
    }

    @Test
    public void testLongListOverAPrefixOfAStringDateKeepsTheDefault() {
        ScalarOperator prefix = prefixOf("left", 10);
        int limit = 1024;
        ScalarOperator[] values = new ScalarOperator[limit + 1];
        for (int i = 0; i < values.length; i++) {
            values[i] = text(java.time.LocalDate.of(2027, 1, 1).plusDays(i).toString());
        }
        // 1024 days, which the table does not hold: the one row an estimate takes for each.
        Assertions.assertEquals(limit, stringRows(in(false, prefix, Arrays.copyOf(values, limit))), 1e-6);
        Assertions.assertEquals(strings().getOutputRowCount() / 2, stringRows(in(false, prefix, values)), 1e-6);
    }

    @Test
    public void testInOverAStringDateIsTheOrOfItsEqualities() {
        ConstantOperator hot = ConstantOperator.createVarchar("2026-08-15T06:30:00.000000Z");
        ConstantOperator other = ConstantOperator.createVarchar("2026-08-14T10:00:00.000000Z");
        double or = stringRows(Utils.compoundOr(eq(TEXT, hot), eq(TEXT, other)));
        Assertions.assertEquals(or, stringRows(in(false, TEXT, hot, other)), 1e-6);
        Assertions.assertEquals(or, stringRows(in(false, TEXT, other, hot)), 1e-6);
        // The hot value of the head has its 5000 rows.
        Assertions.assertEquals(5000, stringRows(eq(TEXT, hot)), 1e-6);
        Assertions.assertTrue(or > 5000);
    }
}
