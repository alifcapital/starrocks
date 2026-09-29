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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.collect.ImmutableList;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.MonotonicFunctionRegistry;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;
import com.starrocks.type.DateType;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sweeps every day in a window around the constant and checks that the inverted predicate
 * accepts exactly the days the original comparison accepts. The fold is the reference
 * semantics; a mismatch on any day, comparison, or alignment fails the sweep.
 */
public class MonotonicInverseBruteForceTest {

    private final InvertMonotonicPredicateRule rule = new InvertMonotonicPredicateRule();
    private final ColumnRefOperator dateCol = new ColumnRefOperator(1, DateType.DATE, "d", true);

    private static final BinaryType[] ALL_CMP = {BinaryType.EQ, BinaryType.NE, BinaryType.GE,
            BinaryType.GT, BinaryType.LE, BinaryType.LT};

    private boolean evalPredicate(ScalarOperator predicate, ConstantOperator x) {
        if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compound = (CompoundPredicateOperator) predicate;
            boolean left = evalPredicate(compound.getChild(0), x);
            boolean right = evalPredicate(compound.getChild(1), x);
            return compound.isAnd() ? left && right : left || right;
        }
        BinaryPredicateOperator binary = (BinaryPredicateOperator) predicate;
        ConstantOperator bound = (ConstantOperator) binary.getChild(1);
        int cmp = x.compareTo(bound);
        switch (binary.getBinaryType()) {
            case EQ: return cmp == 0;
            case NE: return cmp != 0;
            case GE: return cmp >= 0;
            case GT: return cmp > 0;
            case LE: return cmp <= 0;
            case LT: return cmp < 0;
            default: throw new IllegalStateException(binary.getBinaryType().toString());
        }
    }

    private boolean evalOriginal(ConstantOperator image, BinaryType cmp, ConstantOperator constant) {
        int c = image.compareTo(constant);
        switch (cmp) {
            case EQ: return c == 0;
            case NE: return c != 0;
            case GE: return c >= 0;
            case GT: return c > 0;
            case LE: return c <= 0;
            case LT: return c < 0;
            default: throw new IllegalStateException(cmp.toString());
        }
    }

    private void sweep(CallOperator call, Function<ConstantOperator, ConstantOperator> fold,
                       ConstantOperator constant, LocalDate windowCenter) {
        int inverted = 0;
        for (BinaryType cmp : ALL_CMP) {
            ScalarOperator predicate = new BinaryPredicateOperator(cmp, call, constant);
            ScalarOperator result = rule.apply(predicate, null);
            MonotonicFunctionRegistry.PredicateInverse filter = MonotonicFunctionRegistry.filterInverse(call.getFnName());
            if (result == predicate && filter != null) {
                // These non-overflowing day samples also satisfy the filter preimage.
                // Separate tests check NULL/overflow and retention of the original predicate.
                result = filter.invert(call, dateCol, cmp, constant).orElse(predicate);
            }
            if (result == predicate) {
                continue;
            }
            inverted++;
            for (int offset = -20; offset <= 20; offset++) {
                ConstantOperator x = ConstantOperator.createDate(
                        windowCenter.plusDays(offset).atStartOfDay());
                boolean expected = evalOriginal(fold.apply(x), cmp, constant);
                boolean actual = evalPredicate(result, x);
                assertEquals(expected, actual,
                        call.getFnName() + " " + cmp + " " + constant + " at x=" + x + " -> " + result);
            }
        }
        assertTrue(inverted > 0, call.getFnName() + " " + constant + ": nothing inverted");
    }

    @Test
    public void testLastDayMonthSweep() {
        CallOperator call = new CallOperator("last_day", DateType.DATE, ImmutableList.of(dateCol));
        Function<ConstantOperator, ConstantOperator> fold = x -> ScalarOperatorFunctions.lastDay(x);
        // aligned: a month end; misaligned: a mid-month day
        sweep(call, fold, ConstantOperator.createDate(LocalDateTime.of(2024, 3, 31, 0, 0)), LocalDate.of(2024, 3, 31));
        sweep(call, fold, ConstantOperator.createDate(LocalDateTime.of(2024, 3, 15, 0, 0)), LocalDate.of(2024, 3, 15));
    }

    @Test
    public void testLastDayQuarterSweep() {
        CallOperator call = new CallOperator("last_day", DateType.DATE,
                ImmutableList.of(dateCol, ConstantOperator.createVarchar("quarter")));
        Function<ConstantOperator, ConstantOperator> fold =
                x -> ScalarOperatorFunctions.lastDay(x, ConstantOperator.createVarchar("quarter"));
        sweep(call, fold, ConstantOperator.createDate(LocalDateTime.of(2024, 3, 31, 0, 0)), LocalDate.of(2024, 3, 31));
        sweep(call, fold, ConstantOperator.createDate(LocalDateTime.of(2024, 2, 10, 0, 0)), LocalDate.of(2024, 2, 10));
    }

    @Test
    public void testNextDaySweep() {
        CallOperator call = new CallOperator("next_day", DateType.DATE,
                ImmutableList.of(dateCol, ConstantOperator.createVarchar("Monday")));
        Function<ConstantOperator, ConstantOperator> fold =
                x -> ScalarOperatorFunctions.nextDay(x, ConstantOperator.createVarchar("Monday"));
        // 2024-03-18 is a Monday; 2024-03-20 is a Wednesday
        sweep(call, fold, ConstantOperator.createDate(LocalDateTime.of(2024, 3, 18, 0, 0)), LocalDate.of(2024, 3, 18));
        sweep(call, fold, ConstantOperator.createDate(LocalDateTime.of(2024, 3, 20, 0, 0)), LocalDate.of(2024, 3, 20));
    }

    @Test
    public void testPreviousDaySweep() {
        CallOperator call = new CallOperator("previous_day", DateType.DATE,
                ImmutableList.of(dateCol, ConstantOperator.createVarchar("Friday")));
        Function<ConstantOperator, ConstantOperator> fold =
                x -> ScalarOperatorFunctions.previousDay(x, ConstantOperator.createVarchar("Friday"));
        // 2024-03-22 is a Friday; 2024-03-25 is a Monday
        sweep(call, fold, ConstantOperator.createDate(LocalDateTime.of(2024, 3, 22, 0, 0)), LocalDate.of(2024, 3, 22));
        sweep(call, fold, ConstantOperator.createDate(LocalDateTime.of(2024, 3, 25, 0, 0)), LocalDate.of(2024, 3, 25));
    }

    @Test
    public void testToDaysSweep() {
        CallOperator call = new CallOperator("to_days", com.starrocks.type.IntegerType.INT,
                ImmutableList.of(dateCol));
        Function<ConstantOperator, ConstantOperator> fold = x -> ScalarOperatorFunctions.to_days(x);
        // to_days('2024-03-15') = 739325
        ConstantOperator constant = ScalarOperatorFunctions.to_days(
                ConstantOperator.createDate(LocalDateTime.of(2024, 3, 15, 0, 0)));
        sweep(call, fold, constant, LocalDate.of(2024, 3, 15));
    }

    @Test
    public void testMonthShiftSweep() {
        ConstantOperator one = ConstantOperator.createInt(1);
        CallOperator monthsAdd = new CallOperator("months_add", DateType.DATE, ImmutableList.of(dateCol, one));
        Function<ConstantOperator, ConstantOperator> addOne = x -> ScalarOperatorFunctions.monthsAdd(x, one);
        // 2024-01-29, 2024-01-30 and 2024-01-31 all map to 2024-02-29
        sweep(monthsAdd, addOne, date(2024, 2, 29), LocalDate.of(2024, 1, 30));
        // no date maps to 2024-03-31: February ends on the 29th, March 1 maps to April 1
        sweep(monthsAdd, addOne, date(2024, 3, 31), LocalDate.of(2024, 3, 1));
        sweep(monthsAdd, addOne, date(2024, 3, 15), LocalDate.of(2024, 2, 15));

        CallOperator monthsSub = new CallOperator("months_sub", DateType.DATE, ImmutableList.of(dateCol, one));
        Function<ConstantOperator, ConstantOperator> subOne =
                x -> ScalarOperatorFunctions.monthsAdd(x, ConstantOperator.createInt(-1));
        // 2024-03-29, 2024-03-30 and 2024-03-31 all map to 2024-02-29
        sweep(monthsSub, subOne, date(2024, 2, 29), LocalDate.of(2024, 3, 30));
        // no date maps to 2024-03-31: April ends on the 30th
        sweep(monthsSub, subOne, date(2024, 3, 31), LocalDate.of(2024, 5, 1));

        CallOperator yearsAdd = new CallOperator("years_add", DateType.DATE, ImmutableList.of(dateCol, one));
        // 2024-02-28 and 2024-02-29 both map to 2025-02-28
        sweep(yearsAdd, x -> ScalarOperatorFunctions.yearsAdd(x, one), date(2025, 2, 28), LocalDate.of(2024, 2, 28));

        CallOperator quartersAdd = new CallOperator("quarters_add", DateType.DATE, ImmutableList.of(dateCol, one));
        // 2024-03-30 and 2024-03-31 both map to 2024-06-30
        sweep(quartersAdd, x -> ScalarOperatorFunctions.monthsAdd(x, ConstantOperator.createInt(3)),
                date(2024, 6, 30), LocalDate.of(2024, 3, 30));
    }

    @Test
    public void testMonthShiftOnDatetimeKeepsEveryMatchingRow() {
        // On DATETIME the time of day is kept, so the matching rows are not a range. The bound
        // covers only the date part; we check that it keeps every row the predicate keeps.
        ColumnRefOperator tsCol = new ColumnRefOperator(2, DateType.DATETIME, "ts", true);
        CallOperator call = new CallOperator("months_add", DateType.DATETIME,
                ImmutableList.of(tsCol, ConstantOperator.createInt(1)));
        MonotonicFunctionRegistry.PredicateInverse inverse = MonotonicFunctionRegistry.filterInverse("months_add");
        LocalDateTime[] constants = {LocalDateTime.of(2024, 2, 29, 12, 0), LocalDateTime.of(2024, 2, 29, 0, 0),
                LocalDateTime.of(2024, 3, 31, 0, 0), LocalDateTime.of(2024, 3, 15, 6, 30)};
        for (LocalDateTime constant : constants) {
            ConstantOperator value = ConstantOperator.createDatetime(constant);
            for (BinaryType cmp : ALL_CMP) {
                ScalarOperator bound = inverse.invert(call, tsCol, cmp, value).orElse(null);
                if (bound == null) {
                    continue;
                }
                for (LocalDateTime x = LocalDateTime.of(2024, 1, 15, 0, 0); x.isBefore(LocalDateTime.of(2024, 4, 15, 0, 0));
                        x = x.plusMinutes(30)) {
                    ConstantOperator shifted = ConstantOperator.createDatetime(x.plusMonths(1));
                    ConstantOperator row = ConstantOperator.createDatetime(x);
                    if (evalOriginal(shifted, cmp, value)) {
                        assertTrue(evalPredicate(bound, row), cmp + " " + constant + " at x=" + x + " -> " + bound);
                    }
                }
            }
        }
    }

    private static ConstantOperator date(int year, int month, int day) {
        return ConstantOperator.createDate(LocalDateTime.of(year, month, day, 0, 0));
    }

    @Test
    public void testDateTruncMonthSweepSanity() {
        // the shared window machinery itself, pinned on the oldest inverter
        List<ScalarOperator> args = ImmutableList.of(ConstantOperator.createVarchar("month"), dateCol);
        CallOperator call = new CallOperator("date_trunc", DateType.DATE, args);
        Function<ConstantOperator, ConstantOperator> fold =
                x -> ScalarOperatorFunctions.dateTrunc(ConstantOperator.createVarchar("month"), x);
        sweep(call, fold, ConstantOperator.createDate(LocalDateTime.of(2024, 3, 1, 0, 0)), LocalDate.of(2024, 3, 1));
        sweep(call, fold, ConstantOperator.createDate(LocalDateTime.of(2024, 3, 15, 0, 0)), LocalDate.of(2024, 3, 15));
    }
}
