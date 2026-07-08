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
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDateTime;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class InvertMonotonicPredicateRuleTest {

    private final InvertMonotonicPredicateRule rule = new InvertMonotonicPredicateRule();

    private final ColumnRefOperator dtCol = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
    private final ColumnRefOperator dateCol = new ColumnRefOperator(2, DateType.DATE, "d", true);

    private ScalarOperator dateTrunc(String unit, ColumnRefOperator col, BinaryType cmp, LocalDateTime value) {
        CallOperator call = new CallOperator("date_trunc", col.getType(),
                ImmutableList.of(ConstantOperator.createVarchar(unit), col));
        ConstantOperator constant = col.getType().isDate()
                ? ConstantOperator.createDate(value) : ConstantOperator.createDatetime(value);
        return new BinaryPredicateOperator(cmp, call, constant);
    }

    private ScalarOperator apply(ScalarOperator predicate) {
        return rule.apply(predicate, null);
    }

    private static Stream<Arguments> caseTable() {
        // cmp, value, expected predicate text on the DATETIME column (month granularity)
        return Stream.of(
                Arguments.of(BinaryType.EQ, "2024-03-01T00:00",
                        "1: ts >= 2024-03-01 00:00:00 AND 1: ts < 2024-04-01 00:00:00"),
                Arguments.of(BinaryType.EQ, "2024-03-15T00:00",
                        "1: ts >= 2024-03-01 00:00:00 AND 1: ts < 2024-03-01 00:00:00"),
                Arguments.of(BinaryType.GE, "2024-03-01T00:00", "1: ts >= 2024-03-01 00:00:00"),
                Arguments.of(BinaryType.GE, "2024-03-15T00:00", "1: ts >= 2024-04-01 00:00:00"),
                Arguments.of(BinaryType.GT, "2024-03-01T00:00", "1: ts >= 2024-04-01 00:00:00"),
                Arguments.of(BinaryType.LE, "2024-03-01T00:00", "1: ts < 2024-04-01 00:00:00"),
                Arguments.of(BinaryType.LT, "2024-03-01T00:00", "1: ts < 2024-03-01 00:00:00"),
                Arguments.of(BinaryType.LT, "2024-03-15T00:00", "1: ts < 2024-04-01 00:00:00"));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("caseTable")
    public void testDateTruncCaseTable(BinaryType cmp, String value, String expected) {
        ScalarOperator result = apply(dateTrunc("month", dtCol, cmp, LocalDateTime.parse(value)));
        assertEquals(expected, result.toString());
    }

    @Test
    public void testDatetimeMaxRefused() {
        // nextUpperDateTime clamps at the DATETIME max and cannot provide the exclusive
        // upper bound: every shape must keep the original predicate
        for (BinaryType cmp : new BinaryType[] {BinaryType.EQ, BinaryType.GE, BinaryType.GT,
                BinaryType.LE, BinaryType.LT}) {
            ScalarOperator predicate = dateTrunc("day", dtCol, cmp, LocalDateTime.of(9999, 12, 31, 0, 0));
            assertSame(predicate, apply(predicate), cmp.toString());
        }
    }

    @Test
    public void testDayPointOnDateColumnKeepsEq() {
        ScalarOperator result = apply(dateTrunc("day", dateCol, BinaryType.EQ, LocalDateTime.of(2024, 3, 5, 0, 0)));
        assertEquals("2: d = 2024-03-05", result.toString());
    }

    @Test
    public void testIntraDayUnitOnDateColumnRefused() {
        // an hour period has intra-day bounds, unrepresentable as DATE constants
        ScalarOperator predicate = dateTrunc("hour", dateCol, BinaryType.GE, LocalDateTime.of(2024, 3, 5, 0, 0));
        assertSame(predicate, apply(predicate));
    }

    @Test
    public void testNonConstantUnitRefused() {
        CallOperator call = new CallOperator("date_trunc", DateType.DATETIME,
                ImmutableList.of(new ColumnRefOperator(3, VarcharType.VARCHAR, "unit", true), dtCol));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 3, 1, 0, 0)));
        assertSame(predicate, apply(predicate));
    }

    @Test
    public void testNullConstantRefused() {
        CallOperator call = new CallOperator("date_trunc", DateType.DATETIME,
                ImmutableList.of(ConstantOperator.createVarchar("month"), dtCol));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, call,
                ConstantOperator.createNull(DateType.DATETIME));
        assertSame(predicate, apply(predicate));
    }

    @Test
    public void testNeRefused() {
        ScalarOperator predicate = dateTrunc("month", dtCol, BinaryType.NE, LocalDateTime.of(2024, 3, 1, 0, 0));
        assertSame(predicate, apply(predicate));
    }

    @Test
    public void testMonthShiftRefused() {
        CallOperator call = new CallOperator("months_add", DateType.DATETIME,
                ImmutableList.of(dtCol, ConstantOperator.createInt(1)));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.LE, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 2, 29, 0, 0)));
        assertSame(predicate, apply(predicate));
    }

    @Test
    public void testDayShiftInverts() {
        // days_add overflows only at the top of the domain: LE keeps the region away from
        // the tail and needs no guard, GE gets one, EQ maps to the total zone by construction
        CallOperator call = new CallOperator("days_add", DateType.DATETIME,
                ImmutableList.of(dtCol, ConstantOperator.createInt(3)));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.LE, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 3, 8, 0, 0)));
        assertEquals("1: ts <= 2024-03-05 00:00:00", apply(predicate).toString());

        predicate = new BinaryPredicateOperator(BinaryType.GE, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 3, 8, 0, 0)));
        assertEquals("1: ts >= 2024-03-05 00:00:00 AND 1: ts <= 9999-12-28 23:59:59",
                apply(predicate).toString());

        predicate = new BinaryPredicateOperator(BinaryType.EQ, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 3, 8, 0, 0)));
        assertEquals("1: ts = 2024-03-05 00:00:00", apply(predicate).toString());
    }

    @Test
    public void testDayShiftDownwardGuard() {
        // days_sub overflows at the bottom: LE opens toward that tail and gets the guard
        CallOperator call = new CallOperator("days_sub", DateType.DATETIME,
                ImmutableList.of(dtCol, ConstantOperator.createInt(3)));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.LE, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 3, 8, 0, 0)));
        assertEquals("1: ts <= 2024-03-11 00:00:00 AND 1: ts >= 0000-01-04 00:00:00",
                apply(predicate).toString());

        predicate = new BinaryPredicateOperator(BinaryType.GE, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 3, 8, 0, 0)));
        assertEquals("1: ts >= 2024-03-11 00:00:00", apply(predicate).toString());
    }

    @Test
    public void testDayShiftAtDomainBoundaryRefused() {
        // the opposite shift days_sub('0000-01-02', 3) leaves the supported range and folds
        // to NULL; the predicate must stay unrewritten
        CallOperator call = new CallOperator("days_add", DateType.DATETIME,
                ImmutableList.of(dtCol, ConstantOperator.createInt(3)));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE, call,
                ConstantOperator.createDatetime(LocalDateTime.of(0, 1, 2, 0, 0)));
        assertSame(predicate, apply(predicate));
    }
}
