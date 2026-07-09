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
