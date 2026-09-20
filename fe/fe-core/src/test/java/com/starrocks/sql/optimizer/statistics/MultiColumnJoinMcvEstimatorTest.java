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

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalDouble;

public class MultiColumnJoinMcvEstimatorTest {
    private static final ColumnRefOperator L_STATUS = new ColumnRefOperator(1, VarcharType.VARCHAR, "status", true);
    private static final ColumnRefOperator L_GATE = new ColumnRefOperator(2, IntegerType.INT, "gate", true);
    private static final ColumnRefOperator R_STATUS = new ColumnRefOperator(3, VarcharType.VARCHAR, "status", true);
    private static final ColumnRefOperator R_GATE = new ColumnRefOperator(4, IntegerType.BIGINT, "gate", true);

    private static MultiColumnCombinedStats left() {
        return new MultiColumnCombinedStats(10, 1000, List.of(L_STATUS, L_GATE), List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("a", "1"), 500),
                new MultiColumnCombinedStats.McvEntry(List.of("b", "2"), 300)));
    }

    private static MultiColumnCombinedStats right() {
        // The key columns come in the other order; gate is a BIGINT whose text form differs.
        return new MultiColumnCombinedStats(20, 2000, List.of(R_GATE, R_STATUS), List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("1.0", "a"), 800),
                new MultiColumnCombinedStats.McvEntry(List.of("3", "c"), 400)));
    }

    @Test
    public void testHeadsJoinExactlyAndTailsFollowTheNdv() {
        OptionalDouble selectivity = MultiColumnJoinMcvEstimator.estimateSelectivity(
                left(), List.of(L_STATUS, L_GATE), 0, right(), List.of(R_STATUS, R_GATE), 0);
        // (a, 1) matches: 0.5 * 0.4. Left to right: the unmatched 0.3 of the left head meets the right
        // tail 0.4 over 18 tail tuples, the left tail 0.2 meets the right tail and unmatched head 0.6
        // over 19 tuples. Right to left is larger, so the left to right estimate stands.
        double expected = 0.2 + 0.3 * 0.4 / 18 + 0.2 * 0.6 / 19;
        Assertions.assertEquals(expected, selectivity.orElseThrow(), 1e-12);
    }

    @Test
    public void testNullComponentsNeverJoinAndNullsShrinkTheTail() {
        MultiColumnCombinedStats leftWithNull = new MultiColumnCombinedStats(10, 1000, List.of(L_STATUS, L_GATE), List.of(
                new MultiColumnCombinedStats.McvEntry(Arrays.asList("a", null), 500),
                new MultiColumnCombinedStats.McvEntry(List.of("b", "2"), 300)));
        OptionalDouble selectivity = MultiColumnJoinMcvEstimator.estimateSelectivity(
                leftWithNull, List.of(L_STATUS, L_GATE), 0.5, right(), List.of(R_STATUS, R_GATE), 0);
        // No head match. Left to right: the unmatched left head 0.3 meets the right tail 0.4 over 18;
        // the NULL rows are 0.5, all of them in the head, so the left tail 0.2 is non-NULL and meets
        // 0.4 + 0.6 over 20.
        double expected = 0.3 * 0.4 / 18 + 0.2 * 1.0 / 20;
        Assertions.assertEquals(expected, selectivity.orElseThrow(), 1e-12);
    }

    @Test
    public void testNeedsAnMcvListExactlyOnTheKey() {
        MultiColumnCombinedStats ndvOnly = new MultiColumnCombinedStats(10);
        Assertions.assertTrue(MultiColumnJoinMcvEstimator.estimateSelectivity(
                ndvOnly, List.of(L_STATUS, L_GATE), 0, right(), List.of(R_STATUS, R_GATE), 0).isEmpty());
        Assertions.assertTrue(MultiColumnJoinMcvEstimator.estimateSelectivity(
                left(), List.of(L_STATUS), 0, right(), List.of(R_STATUS), 0).isEmpty());
        Assertions.assertEquals("1", MultiColumnJoinMcvEstimator.canonical(IntegerType.INT, "1"));
        Assertions.assertEquals("1", MultiColumnJoinMcvEstimator.canonical(IntegerType.BIGINT, "1.0"));
        Assertions.assertEquals("9007199254740993",
                MultiColumnJoinMcvEstimator.canonical(IntegerType.BIGINT, "9007199254740993"));
        Assertions.assertEquals("x", MultiColumnJoinMcvEstimator.canonical(VarcharType.VARCHAR, "x"));
        Assertions.assertEquals("x", MultiColumnJoinMcvEstimator.canonical(IntegerType.BIGINT, "x"));
    }
}
