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

import java.time.LocalDateTime;
import java.util.List;

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

    @Test
    public void testDisableLeavesPartialPredicatesAlone() {
        ctx.getSessionVariable().setEnableMonotonicPredicateRewrite(false);
        ScalarOperator predicate = comparison("from_unixtime", BinaryType.GE, "2024-03-05 10:30:00");
        assertSame(predicate, MonotonicFilterDerivation.addScanBounds(predicate));
    }
}
