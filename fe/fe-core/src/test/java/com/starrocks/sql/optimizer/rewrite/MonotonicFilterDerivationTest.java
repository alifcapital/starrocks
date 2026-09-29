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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.scalar.InvertMonotonicPredicateRule;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MonotonicFilterDerivationTest {
    private final ColumnRefOperator epoch = new ColumnRefOperator(1, IntegerType.BIGINT, "ep", true);
    private ConnectContext ctx;

    @BeforeEach
    public void setUp() {
        ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("+00:00");
        ctx.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private ScalarOperator comparison(String fn, BinaryType cmp, String value) {
        return new BinaryPredicateOperator(cmp,
                new CallOperator(fn, VarcharType.VARCHAR, ImmutableList.of(epoch),
                        new Function(new FunctionName(fn), new Type[] {IntegerType.BIGINT}, VarcharType.VARCHAR, false)),
                ConstantOperator.createVarchar(value));
    }

    private List<ScalarOperator> addedBounds(ScalarOperator original) {
        ScalarOperator result = MonotonicFilterDerivation.addScanBounds(original);
        assertTrue(Utils.extractConjuncts(result).contains(original));
        assertSame(original, new InvertMonotonicPredicateRule().apply(original, null));
        assertSame(result, MonotonicFilterDerivation.addScanBounds(result));
        return Utils.extractConjuncts(result).stream().filter(p -> !p.equals(original)).toList();
    }

    @Test
    public void testPartialFunctionsRemainInValueAndNullContexts() {
        ScalarOperator comparison = comparison("from_unixtime", BinaryType.EQ, "2024-03-05 10:30:00");
        IsNullPredicateOperator isNull = new IsNullPredicateOperator(false, comparison);
        ScalarOperator rewritten = new ScalarOperatorRewriter().rewrite(isNull.clone(),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        assertEquals(isNull, rewritten);
        assertSame(isNull, MonotonicFilterDerivation.addScanBounds(isNull));
        CompoundPredicateOperator not = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, comparison);
        assertSame(not, MonotonicFilterDerivation.addScanBounds(not));
        assertFalse(addedBounds(comparison).isEmpty());
    }

    @Test
    public void testMillisecondsIncludeNegativeRemainder() {
        List<ScalarOperator> bounds = addedBounds(comparison("from_unixtime_ms", BinaryType.EQ,
                "1970-01-01 00:00:00"));
        assertEquals("1: ep >= -999 AND 1: ep < 1000", Utils.compoundAnd(bounds).toString());
    }

    @Test
    public void testMillisecondsIncludeLastSecondRemainder() {
        List<ScalarOperator> bounds = addedBounds(comparison("from_unixtime_ms", BinaryType.GE,
                "2024-03-05 10:30:00"));
        assertTrue(bounds.stream().anyMatch(p -> p instanceof BinaryPredicateOperator
                && ((BinaryPredicateOperator) p).getBinaryType() == BinaryType.LE
                && ((ConstantOperator) p.getChild(1)).getBigint() == 253402243199999L));
    }

    @Test
    public void testUnixTimestampIncludesWholeFinalSecond() {
        ColumnRefOperator ts = new ColumnRefOperator(2, DateType.DATETIME, "ts", true);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE,
                new CallOperator("unix_timestamp", IntegerType.BIGINT, ImmutableList.of(ts)),
                ConstantOperator.createBigint(1700000000));
        ScalarOperator inverse = new InvertMonotonicPredicateRule().apply(predicate, null);
        ScalarOperator cap = Utils.extractConjuncts(inverse).stream()
                .filter(p -> ((BinaryPredicateOperator) p).getBinaryType() == BinaryType.LT).findFirst().orElseThrow();
        assertEquals(LocalDateTime.of(9999, 12, 31, 8, 0), ((ConstantOperator) cap.getChild(1)).getDatetime());
        // The BE drops microseconds before comparing epoch seconds to MAX_UNIX_TIMESTAMP.
        assertTrue(LocalDateTime.of(9999, 12, 31, 7, 59, 59, 500000000)
                .isBefore(((ConstantOperator) cap.getChild(1)).getDatetime()));
    }

    @Test
    public void testLegacyIntUnixTimestampIsNotTreatedAsBigint() {
        ColumnRefOperator ts = new ColumnRefOperator(2, DateType.DATETIME, "ts", true);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE,
                new CallOperator("unix_timestamp", IntegerType.INT, ImmutableList.of(ts)),
                ConstantOperator.createInt(1700000000));
        assertSame(predicate, new InvertMonotonicPredicateRule().apply(predicate, null));
    }

    private static final LocalDateTime EPOCH_START = LocalDateTime.of(1970, 1, 1, 0, 0);

    private static CallOperator dayShift(String fn, ColumnRefOperator days) {
        return new CallOperator(fn, DateType.DATETIME,
                ImmutableList.of(ConstantOperator.createDatetime(EPOCH_START), days));
    }

    private static boolean holds(BinaryType cmp, int c) {
        switch (cmp) {
            case EQ: return c == 0;
            case GE: return c >= 0;
            case GT: return c > 0;
            case LE: return c <= 0;
            case LT: return c < 0;
            default: throw new IllegalStateException(cmp.toString());
        }
    }

    // For every n around the bound, the bound on n holds exactly when base +/- n days compares true.
    @Test
    public void testDayShiftAmountBoundsMatchComparison() {
        ColumnRefOperator days = new ColumnRefOperator(3, IntegerType.INT, "d", true);
        List<LocalDateTime> values = List.of(
                LocalDateTime.of(2024, 1, 1, 0, 0),
                LocalDateTime.of(2024, 1, 1, 10, 30),
                LocalDateTime.of(1969, 12, 30, 23, 59, 59),
                LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1000));
        BinaryType[] comparisons = {BinaryType.EQ, BinaryType.GE, BinaryType.GT, BinaryType.LE, BinaryType.LT};
        for (String fn : List.of("days_add", "date_add", "adddate", "days_sub", "date_sub", "subdate")) {
            int sign = fn.contains("add") ? 1 : -1;
            CallOperator call = dayShift(fn, days);
            assertSame(days, MonotonicFunctionRegistry.dataChildOf(call));
            MonotonicFunctionRegistry.PredicateInverse inverse = MonotonicFunctionRegistry.filterInverse(fn);
            for (LocalDateTime value : values) {
                ConstantOperator constant = ConstantOperator.createDatetime(value);
                assertTrue(inverse.invert(call, days, BinaryType.NE, constant).isEmpty());
                long center = sign * Math.floorDiv(
                        Duration.between(EPOCH_START, value).toSeconds(), 86400L);
                for (BinaryType cmp : comparisons) {
                    Optional<ScalarOperator> bound = inverse.invert(call, days, cmp, constant);
                    boolean aligned = value.toLocalTime().equals(LocalTime.MIDNIGHT);
                    if (cmp == BinaryType.EQ && !aligned) {
                        assertTrue(bound.isEmpty(), fn + " " + cmp + " " + value);
                        continue;
                    }
                    BinaryPredicateOperator predicate = (BinaryPredicateOperator) bound.orElseThrow();
                    assertSame(days, predicate.getChild(0));
                    int k = ((ConstantOperator) predicate.getChild(1)).getInt();
                    for (long n = center - 3; n <= center + 3; n++) {
                        LocalDateTime shifted = EPOCH_START.plusDays(sign * n);
                        boolean expected = holds(cmp, shifted.compareTo(value));
                        boolean actual = holds(predicate.getBinaryType(), Long.compare(n, k));
                        assertEquals(expected, actual, fn + " " + cmp + " " + value + " n=" + n + " -> " + predicate);
                    }
                }
            }
        }
    }

    @Test
    public void testDayShiftAmountBoundIsAddedToTheScan() {
        ColumnRefOperator days = new ColumnRefOperator(3, IntegerType.INT, "d", true);
        List<ScalarOperator> bounds = addedBounds(new BinaryPredicateOperator(BinaryType.GE,
                dayShift("days_add", days), ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 1, 0, 0))));
        assertEquals("3: d >= 19723", Utils.compoundAnd(bounds).toString());
    }

    @Test
    public void testDayFloorOfDayShiftBoundsTheAmount() {
        // to_date('1970-01-01' + INTERVAL d DAY) BETWEEN '2024-01-01' AND '2024-01-31'
        ColumnRefOperator days = new ColumnRefOperator(3, IntegerType.INT, "d", true);
        CallOperator day = new CallOperator("to_date", DateType.DATE, ImmutableList.of(dayShift("days_add", days)));
        ScalarOperator predicate = Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.GE, day,
                        ConstantOperator.createDate(LocalDateTime.of(2024, 1, 1, 0, 0))),
                new BinaryPredicateOperator(BinaryType.LE, day,
                        ConstantOperator.createDate(LocalDateTime.of(2024, 1, 31, 0, 0))));
        List<String> conjuncts = Utils.extractConjuncts(MonotonicFilterDerivation.addScanBounds(predicate)).stream()
                .map(ScalarOperator::toString).toList();
        assertTrue(conjuncts.contains("3: d >= 19723"), conjuncts.toString());
        assertTrue(conjuncts.contains("3: d <= 19753"), conjuncts.toString());
    }

    @Test
    public void testDayShiftAmountRequiresIntColumn() {
        ConstantOperator value = ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 1, 0, 0));
        ColumnRefOperator bigDays = new ColumnRefOperator(4, IntegerType.BIGINT, "b", true);
        assertTrue(MonotonicFunctionRegistry.filterInverse("days_add")
                .invert(dayShift("days_add", bigDays), bigDays, BinaryType.GE, value).isEmpty());
    }

    @Test
    public void testBoundsOnExpressionsAreNotAdded() {
        CallOperator abs = new CallOperator("abs", IntegerType.BIGINT, ImmutableList.of(epoch),
                new Function(new FunctionName("abs"), new Type[] {IntegerType.BIGINT}, IntegerType.BIGINT, false));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ,
                new CallOperator("from_unixtime", VarcharType.VARCHAR, ImmutableList.of(abs),
                        new Function(new FunctionName("from_unixtime"), new Type[] {IntegerType.BIGINT},
                                VarcharType.VARCHAR, false)),
                ConstantOperator.createVarchar("2024-03-05 10:30:00"));
        assertSame(predicate, MonotonicFilterDerivation.addScanBounds(predicate));

        ScalarOperator columnBound = comparison("from_unixtime", BinaryType.EQ, "2024-03-05 10:30:00");
        ScalarOperator both = Utils.compoundAnd(predicate, columnBound);
        List<ScalarOperator> added = Utils.extractConjuncts(MonotonicFilterDerivation.addScanBounds(both)).stream()
                .filter(p -> !p.equals(predicate) && !p.equals(columnBound)).toList();
        assertEquals("1: ep >= 1709634600 AND 1: ep < 1709634601", Utils.compoundAnd(added).toString());
    }

    @Test
    public void testDisableLeavesPartialPredicatesAlone() {
        ctx.getSessionVariable().setEnableMonotonicPredicateRewrite(false);
        ScalarOperator predicate = comparison("from_unixtime", BinaryType.GE, "2024-03-05 10:30:00");
        assertSame(predicate, MonotonicFilterDerivation.addScanBounds(predicate));
    }
}
