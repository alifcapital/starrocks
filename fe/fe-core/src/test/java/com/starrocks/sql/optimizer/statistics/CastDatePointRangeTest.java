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
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;

/**
 * The sides of one day of the cast of a column of strings to DATE, as CAST(created_at AS DATE) >= a AND
 * CAST(created_at AS DATE) <= a, are its equality: a DATE has no value between a and a + 1. The column is that of
 * DailyStrings.
 */
public class CastDatePointRangeTest {
    private static final ColumnRefOperator TEXT = DailyStrings.TEXT;
    private static final LocalDate FIRST_DAY = DailyStrings.FIRST_DAY;
    private static final int DAYS = DailyStrings.DAYS;
    private static final int ROWS_OF_DAY = DailyStrings.ROWS_OF_DAY;

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

    private static Statistics created() {
        return DailyStrings.statistics();
    }

    private static Statistics created(ColumnStatistic statistic) {
        return DailyStrings.statistics(statistic);
    }

    private static ScalarOperator onDay(BinaryType type, LocalDate value) {
        return new BinaryPredicateOperator(type, new CastOperator(DateType.DATE, TEXT),
                ConstantOperator.createDate(value.atStartOfDay()));
    }

    private static ScalarOperator closed(LocalDate from, LocalDate to) {
        return Utils.compoundAnd(onDay(BinaryType.GE, from), onDay(BinaryType.LE, to));
    }

    private static ScalarOperator halfOpen(LocalDate from, LocalDate to) {
        return Utils.compoundAnd(onDay(BinaryType.GE, from), onDay(BinaryType.LT, to.plusDays(1)));
    }

    private static double rows(ScalarOperator predicate) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, created()).getOutputRowCount();
    }

    // The sides of a point on the cast are its equality, and the sides of no value make the conjunction false
    private static ScalarOperator normalized(ScalarOperator predicate, Statistics statistics) {
        return EstimatePredicateNormalizer.normalize(predicate, statistics);
    }

    private static boolean isFalse(ScalarOperator operator) {
        return operator instanceof ConstantOperator constant && !constant.isNull() && !constant.getBoolean();
    }

    @Test
    public void testSidesOfOneDayOfACastAreItsEquality() {
        LocalDate value = LocalDate.of(2026, 8, 17);
        ScalarOperator equality = onDay(BinaryType.EQ, value);
        Statistics statistics = created();
        Assertions.assertEquals(equality, normalized(closed(value, value), statistics));
        Assertions.assertEquals(equality, normalized(Utils.compoundAnd(onDay(BinaryType.LE, value),
                onDay(BinaryType.GE, value)), statistics));
        Assertions.assertEquals(equality, normalized(halfOpen(value, value), statistics));
        Assertions.assertEquals(equality, normalized(Utils.compoundAnd(onDay(BinaryType.GT, value.minusDays(1)),
                onDay(BinaryType.LT, value.plusDays(1))), statistics));
        // The sides of the point stand among the conditions on other columns
        ScalarOperator other = new BinaryPredicateOperator(BinaryType.EQ, TEXT, ConstantOperator.createVarchar("x"));
        Assertions.assertEquals(Utils.compoundAnd(equality, other),
                normalized(Utils.compoundAnd(List.of(onDay(BinaryType.GE, value), other, onDay(BinaryType.LE, value))),
                        statistics));
    }

    @Test
    public void testSidesOfNoDayOfACastAreFalse() {
        LocalDate value = LocalDate.of(2026, 8, 17);
        Statistics statistics = created();
        Assertions.assertTrue(isFalse(normalized(Utils.compoundAnd(onDay(BinaryType.GT, value),
                onDay(BinaryType.LT, value.plusDays(1))), statistics)));
        Assertions.assertTrue(isFalse(normalized(Utils.compoundAnd(onDay(BinaryType.GE, value.plusDays(1)),
                onDay(BinaryType.LE, value)), statistics)));
        // The sides keep no day whether the cast has statistics or not
        Statistics unknown = created(ColumnStatistic.unknown());
        Assertions.assertTrue(isFalse(normalized(Utils.compoundAnd(onDay(BinaryType.GT, value),
                onDay(BinaryType.LT, value.plusDays(1))), unknown)));
    }

    @Test
    public void testRangesThatKeepMoreThanADayStay() {
        LocalDate value = LocalDate.of(2026, 8, 17);
        Statistics statistics = created();
        for (ScalarOperator range : List.of(closed(value, value.plusDays(1)),
                Utils.compoundAnd(onDay(BinaryType.GT, value), onDay(BinaryType.LT, value.plusDays(3))),
                Utils.compoundAnd(onDay(BinaryType.GE, value), onDay(BinaryType.LT, value.plusDays(2))))) {
            Assertions.assertSame(range, normalized(range, statistics));
        }
        // A point without statistics of the cast keeps its ranges: the equality would take a default share there
        Statistics unknown = created(ColumnStatistic.unknown());
        ScalarOperator point = closed(value, value);
        Assertions.assertSame(point, normalized(point, unknown));
    }

    @Test
    public void testOnlyCastsToWholeNumbersAndDaysAreRead() {
        LocalDate value = LocalDate.of(2026, 8, 17);
        Statistics statistics = created();
        // A DATETIME has times between a and a + 1
        ScalarOperator time = new CastOperator(DateType.DATETIME, TEXT);
        ScalarOperator seconds = Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.GE, time, ConstantOperator.createDatetime(value.atStartOfDay())),
                new BinaryPredicateOperator(BinaryType.LE, time, ConstantOperator.createDatetime(value.atStartOfDay())));
        Assertions.assertSame(seconds, normalized(seconds, statistics));
        // A source that gives another value each time it is evaluated: the two sides are not on one value
        CallOperator random = new CallOperator("random", FloatType.DOUBLE, List.of());
        ScalarOperator number = new CastOperator(IntegerType.BIGINT, random);
        ScalarOperator between = Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.GT, number, ConstantOperator.createBigint(5)),
                new BinaryPredicateOperator(BinaryType.LT, number, ConstantOperator.createBigint(6)));
        Assertions.assertSame(between, normalized(between, statistics));
    }

    @Test
    public void testCastToWholeNumbersIsReadAsDays() {
        // A number written as text, cast to BIGINT, has no value between 5 and 6
        ScalarOperator number = new CastOperator(IntegerType.BIGINT, TEXT);
        ScalarOperator between = Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.GT, number, ConstantOperator.createBigint(5)),
                new BinaryPredicateOperator(BinaryType.LT, number, ConstantOperator.createBigint(6)));
        Assertions.assertTrue(isFalse(normalized(between, created())));
    }

    // The last day of the type has no next day, and the sides of a point there are its equality
    @Test
    public void testSidesOfTheLastDayOfTheType() {
        LocalDate last = LocalDate.of(9999, 12, 31);
        Assertions.assertEquals(rows(onDay(BinaryType.EQ, last)), rows(Utils.compoundAnd(onDay(BinaryType.GE, last),
                onDay(BinaryType.LE, last))), 1e-9);
        double ranged = rows(Utils.compoundAnd(onDay(BinaryType.GT, last.minusDays(3)), onDay(BinaryType.LE, last)));
        Assertions.assertTrue(ranged >= 0 && ranged <= (double) ROWS_OF_DAY * DAYS, "" + ranged);
        // A range above the last day keeps no day
        Assertions.assertTrue(rows(Utils.compoundAnd(onDay(BinaryType.GT, last), onDay(BinaryType.LE, last))) <= 1);
    }

    // A point of the cast is estimated as the equality, which the estimate of an interval of one day also gives
    @Test
    public void testPointOfADayHasTheRowsOfItsEquality() {
        List<LocalDate> days = new ArrayList<>();
        for (int i = 0; i < DAYS; i++) {
            days.add(FIRST_DAY.plusDays(i));
        }
        for (LocalDate value : days) {
            double equality = rows(onDay(BinaryType.EQ, value));
            Assertions.assertEquals(equality, rows(closed(value, value)), 1e-9, value.toString());
            Assertions.assertEquals(equality, rows(halfOpen(value, value)), 1e-9, value.toString());
            Assertions.assertEquals(equality, rows(Utils.compoundAnd(onDay(BinaryType.GT, value.minusDays(1)),
                    onDay(BinaryType.LT, value.plusDays(1)))), 1e-9, value.toString());
            Assertions.assertEquals(ROWS_OF_DAY, equality, 2, value.toString());
        }
    }

    @Test
    public void testSidesOfAnImplicitAndAWrittenCastAreOneExpression() {
        LocalDate value = LocalDate.of(2026, 8, 17);
        ScalarOperator implicit = new CastOperator(DateType.DATE, TEXT, true);
        ConstantOperator day = ConstantOperator.createDate(value.atStartOfDay());
        ScalarOperator mixed = Utils.compoundAnd(onDay(BinaryType.GE, value),
                new BinaryPredicateOperator(BinaryType.LE, implicit, day));
        Assertions.assertEquals(onDay(BinaryType.EQ, value), normalized(mixed, created()));
        ScalarOperator reversed = Utils.compoundAnd(new BinaryPredicateOperator(BinaryType.LE, implicit, day),
                onDay(BinaryType.GE, value));
        ScalarOperator equality = normalized(reversed, created());
        Assertions.assertTrue(equality instanceof BinaryPredicateOperator binary
                && binary.getBinaryType() == BinaryType.EQ && binary.getChild(1).equals(day), "" + equality);
    }

    @Test
    public void testSidesWithAConstantOfAnotherTypeStay() {
        // A DATETIME constant against a DATE has times of the day that the days do not hold
        LocalDate value = LocalDate.of(2026, 8, 17);
        ScalarOperator mixed = Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.GE, new CastOperator(DateType.DATE, TEXT),
                        ConstantOperator.createDatetime(value.atStartOfDay())),
                new BinaryPredicateOperator(BinaryType.LE, new CastOperator(DateType.DATE, TEXT),
                        ConstantOperator.createDatetime(value.atStartOfDay())));
        Assertions.assertSame(mixed, normalized(mixed, created()));
    }

    // The rewrite keeps the rows: every random conjunction of ranges of a cast to BIGINT keeps the same values of the
    // column as the conjunction the normalizer writes, NULL included
    @Test
    public void testRewriteKeepsTheRowsOfEveryValue() {
        ColumnRefOperator number = new ColumnRefOperator(5, IntegerType.INT, "number", true);
        Statistics statistics = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(number, new ColumnStatistic(-2, 6, 0.1, 4, 9)).build();
        ScalarOperator wide = new CastOperator(IntegerType.BIGINT, number);
        BinaryType[] types = {BinaryType.GE, BinaryType.GT, BinaryType.LE, BinaryType.LT};
        Random random = new Random(20261018);
        int rewritten = 0;
        int falsified = 0;
        for (int round = 0; round < 20_000; round++) {
            List<ScalarOperator> sides = new ArrayList<>();
            int size = 2 + random.nextInt(3);
            for (int i = 0; i < size; i++) {
                sides.add(new BinaryPredicateOperator(types[random.nextInt(types.length)], wide,
                        ConstantOperator.createBigint(random.nextInt(9) - 2)));
            }
            ScalarOperator original = Utils.compoundAnd(sides);
            ScalarOperator normal = normalized(original, statistics);
            rewritten += normal != original ? 1 : 0;
            falsified += isFalse(normal) ? 1 : 0;
            for (long value = -5; value <= 9; value++) {
                Map<ColumnRefOperator, Long> row = new HashMap<>();
                row.put(number, value);
                Assertions.assertEquals(RowEvaluator.keeps(original, row), RowEvaluator.keeps(normal, row),
                        original + " became " + normal + " on " + value);
            }
            Map<ColumnRefOperator, Long> nulls = new HashMap<>();
            nulls.put(number, null);
            Assertions.assertFalse(RowEvaluator.keeps(normal, nulls), normal.toString());
        }
        Assertions.assertTrue(rewritten > 3000 && falsified > 500, rewritten + " rewritten, " + falsified + " false");
    }
}
