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
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/**
 * A comparison of the month of a day number, as a dashboard writes it: date_format(last_day(days_add('1970-01-01',
 * date)), '%Y-%m-01 00:00:00') cast to a DATETIME and compared with the first day of a month, with or without a shift by
 * zero seconds around it. We expect the estimate to read it as the bounds on the day numbers that follow from it, as for
 * any function of a column, and not to take the statistics of the string, which has none. The table has 906,000,000
 * rows over the days from 2024-01-01 to 2026-12-31.
 */
public class DateFormatChainEstimateTest {
    private static final double ROWS = 906_000_000;
    private static final ColumnRefOperator DAYS = new ColumnRefOperator(1, IntegerType.INT, "date", true);
    private static final ColumnRefOperator OTHER = new ColumnRefOperator(2, IntegerType.INT, "other", true);
    private static final long FIRST = LocalDate.of(2024, 1, 1).toEpochDay();
    private static final long LAST = LocalDate.of(2026, 12, 31).toEpochDay();

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

    private static Statistics table() {
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(DAYS, new ColumnStatistic(FIRST, LAST, 0, 4, LAST - FIRST + 1))
                .addColumnStatistic(OTHER, new ColumnStatistic(0, 9, 0, 4, 10)).build();
    }

    private static CallOperator month(boolean shifted) {
        CallOperator date = new CallOperator("days_add", DateType.DATETIME,
                ImmutableList.of(ConstantOperator.createDatetime(LocalDateTime.of(1970, 1, 1, 0, 0)), DAYS));
        CallOperator lastDay = new CallOperator("last_day", DateType.DATE, ImmutableList.of(date));
        CastOperator text = new CastOperator(DateType.DATETIME, new CallOperator("date_format", VarcharType.VARCHAR,
                ImmutableList.of(lastDay, ConstantOperator.createVarchar("%Y-%m-01 00:00:00"))));
        return shifted ? new CallOperator("seconds_add", DateType.DATETIME,
                ImmutableList.of(text, ConstantOperator.createInt(0))) : text;
    }

    private static ConstantOperator first(int year, int month) {
        return ConstantOperator.createDatetime(LocalDateTime.of(year, month, 1, 0, 0));
    }

    private static ScalarOperator isMonth(boolean shifted, int year, int month) {
        return new BinaryPredicateOperator(BinaryType.EQ, month(shifted), first(year, month));
    }

    private static double rows(ScalarOperator predicate) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, table()).getOutputRowCount();
    }

    private static double daysOfRange() {
        return LAST - FIRST;
    }

    // The days from the first of a month to the first of the next month, as a share of the days of the table
    private static double share(int year, int month, int months) {
        LocalDate from = LocalDate.of(year, month, 1);
        return (from.plusMonths(months).toEpochDay() - from.toEpochDay()) / daysOfRange();
    }

    // The rows of the days of a month written as the bounds on the day numbers that the chain gives: the first day and
    // the last one. The model of a range counts the distance between the ends, so a month of 31 days counts 30, and the
    // chain must give what these bounds give.
    private static double rowsOfTheDays(int year, int month) {
        LocalDate from = LocalDate.of(year, month, 1);
        ScalarOperator first = new BinaryPredicateOperator(BinaryType.GE, DAYS,
                ConstantOperator.createInt((int) from.toEpochDay()));
        ScalarOperator last = new BinaryPredicateOperator(BinaryType.LE, DAYS,
                ConstantOperator.createInt((int) from.plusMonths(1).toEpochDay() - 1));
        return rows(Utils.compoundAnd(first, last));
    }

    // a day of the table
    private static double oneDay() {
        return ROWS / daysOfRange();
    }

    @Test
    public void testMonthIsItsDaysWhetherOrNotAShiftIsAroundIt() {
        for (boolean shifted : new boolean[] {true, false}) {
            double rows = rows(isMonth(shifted, 2026, 8));
            Assertions.assertEquals(rowsOfTheDays(2026, 8), rows, 1, "shifted " + shifted);
            // and it is the days of the month, up to the day that the model of a range leaves out
            Assertions.assertEquals(ROWS * share(2026, 8, 1), rows, oneDay() * 1.5, "shifted " + shifted);
        }
    }

    @Test
    public void testTwoMonthsInAnInAndInAnOr() {
        double expected = rowsOfTheDays(2026, 8) + rowsOfTheDays(2025, 3);
        for (boolean shifted : new boolean[] {true, false}) {
            ScalarOperator in = new InPredicateOperator(false, month(shifted), first(2026, 8), first(2025, 3));
            Assertions.assertEquals(expected, rows(in), oneDay() * 0.1, "in, shifted " + shifted);
            ScalarOperator or = Utils.compoundOr(isMonth(shifted, 2026, 8), isMonth(shifted, 2025, 3));
            Assertions.assertEquals(expected, rows(or), oneDay() * 0.1, "or, shifted " + shifted);
        }
    }

    @Test
    public void testMonthNextToARangeOfTwoYears() {
        // 24 months from 2024-09-01: the month lies inside, so the rows are those of the month, in either order
        ScalarOperator from = new BinaryPredicateOperator(BinaryType.GE, DAYS,
                ConstantOperator.createInt((int) LocalDate.of(2024, 9, 1).toEpochDay()));
        ScalarOperator to = new BinaryPredicateOperator(BinaryType.LT, DAYS,
                ConstantOperator.createInt((int) LocalDate.of(2026, 9, 1).toEpochDay()));
        for (boolean shifted : new boolean[] {true, false}) {
            ScalarOperator august = isMonth(shifted, 2026, 8);
            double expected = rowsOfTheDays(2026, 8);
            Assertions.assertEquals(expected, rows(Utils.compoundAnd(List.of(from, to, august))), oneDay() * 0.1,
                    "last, shifted " + shifted);
            Assertions.assertEquals(expected, rows(Utils.compoundAnd(List.of(august, from, to))), oneDay() * 0.1,
                    "first, shifted " + shifted);
            Assertions.assertEquals(expected, rows(Utils.compoundAnd(List.of(from, august, to))), oneDay() * 0.1,
                    "between, shifted " + shifted);
        }
    }

    @Test
    public void testMonthNextToAConditionOnAnotherColumn() {
        ScalarOperator other = new BinaryPredicateOperator(BinaryType.EQ, OTHER, ConstantOperator.createInt(5));
        for (boolean shifted : new boolean[] {true, false}) {
            double expected = rowsOfTheDays(2026, 8) / 10;
            Assertions.assertEquals(expected, rows(Utils.compoundAnd(isMonth(shifted, 2026, 8), other)), expected * 0.02,
                    "shifted " + shifted);
        }
    }

    @Test
    public void testNegationAndInequalityAddUpToTheTable() {
        for (boolean shifted : new boolean[] {true, false}) {
            ScalarOperator equal = isMonth(shifted, 2026, 8);
            ScalarOperator negated = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, equal);
            Assertions.assertEquals(ROWS, rows(equal) + rows(negated), ROWS * 0.0001, "not shifted " + shifted);
        }
        // <> goes down the chain as an OR of two bounds; the days on both sides count the distance to each end, which is
        // a day less at each of the two ends of the month than the days of the table.
        ScalarOperator equal = isMonth(false, 2026, 8);
        ScalarOperator notEqual = new BinaryPredicateOperator(BinaryType.NE, month(false), first(2026, 8));
        Assertions.assertEquals(ROWS, rows(equal) + rows(notEqual), oneDay() * 2.1);
        // A shift has no inverse of <>, so the comparison keeps the estimate of an expression with no statistics
        ScalarOperator shiftedNotEqual = new BinaryPredicateOperator(BinaryType.NE, month(true), first(2026, 8));
        Assertions.assertTrue(rows(shiftedNotEqual) > 0);
    }

    @Test
    public void testMonthOfAnotherLayoutIsNotAnEstimateOfTheDays() {
        // A format that the BE reads another way has no bounds, and the comparison keeps the estimate of an expression
        // with no statistics: it does not become the days of the month
        CastOperator text = new CastOperator(DateType.DATETIME, new CallOperator("date_format", VarcharType.VARCHAR,
                ImmutableList.of(new CallOperator("last_day", DateType.DATE, ImmutableList.of(
                        new CallOperator("days_add", DateType.DATETIME,
                                ImmutableList.of(ConstantOperator.createDatetime(LocalDateTime.of(1970, 1, 1, 0, 0)), DAYS)))),
                        ConstantOperator.createVarchar("%Y-%m"))));
        double rows = rows(new BinaryPredicateOperator(BinaryType.EQ, text, first(2026, 8)));
        Assertions.assertTrue(Math.abs(rows - ROWS * share(2026, 8, 1)) > ROWS * 0.01, "" + rows);
    }
}
