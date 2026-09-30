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

import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class McvCastProjectionTest {
    private static final ColumnRefOperator SOURCE = new ColumnRefOperator(1, VarcharType.VARCHAR, "text", true);
    private static final ColumnRefOperator OUTPUT = new ColumnRefOperator(2, IntegerType.BIGINT, "number", true);
    private static final CastOperator CAST = new CastOperator(IntegerType.BIGINT, SOURCE);

    @BeforeEach
    public void setUp() {
        UtFrameUtils.createDefaultCtx();
    }

    private Statistics input(boolean complete) {
        List<MultiColumnCombinedStats.McvEntry> values = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("1"), 400),
                new MultiColumnCombinedStats.McvEntry(List.of("01"), 200),
                new MultiColumnCombinedStats.McvEntry(List.of("bad"), complete ? 300 : 100),
                new MultiColumnCombinedStats.McvEntry(Collections.singletonList(null), complete ? 100 : 0));
        return Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(SOURCE, ColumnStatistic.builder().setDistinctValuesCount(complete ? 3 : 100)
                        .setNullsFraction(complete ? 0.1 : 0).setMinString("01").setMaxString("bad")
                        .setMinValue(0).setMaxValue(100).build())
                .addMultiColumnStatistics(Set.of(SOURCE), new MultiColumnCombinedStats(complete ? 4 : 100,
                        1000, List.of(SOURCE), values, List.of(complete ? 100L : 0L))).build();
    }

    @Test
    public void testAbsentGroupsSkipProjectionAndPredicateTraversal() {
        Statistics empty = Statistics.builder().setOutputRowCount(123)
                .addColumnStatistic(SOURCE, ColumnStatistic.unknown()).build();
        Map<ColumnRefOperator, ScalarOperator> projection = org.mockito.Mockito.mock(Map.class);
        ScalarOperator predicate = org.mockito.Mockito.mock(ScalarOperator.class);
        Assertions.assertTrue(McvStatisticsPropagation.project(projection, empty).isEmpty());
        Assertions.assertTrue(McvStatisticsPropagation.completeHeadRows(predicate, empty).isEmpty());
        Assertions.assertNull(McvCastStatistics.derive(OUTPUT, predicate, empty));
        org.mockito.Mockito.verifyNoInteractions(projection, predicate);
        Assertions.assertSame(empty, McvStatisticsPropagation.afterJoin(empty, empty, true));
        Assertions.assertSame(empty, McvStatisticsPropagation.afterJoin(empty, empty, false));
    }

    @Test
    public void testNdvOnlyGroupsStillProjectWithoutCastPreparation() {
        MultiColumnCombinedStats ndv = new MultiColumnCombinedStats(17);
        Map<ColumnRefOperator, ScalarOperator> projection = org.mockito.Mockito.mock(Map.class);
        Assertions.assertNull(McvCastStatistics.projectGroup(projection, ndv));
        org.mockito.Mockito.verifyNoInteractions(projection);
        Statistics input = Statistics.builder().setOutputRowCount(123)
                .addMultiColumnStatistics(Set.of(SOURCE), ndv).build();
        Assertions.assertSame(ndv, McvStatisticsPropagation.project(Map.of(OUTPUT, SOURCE), input)
                .get(Set.of(OUTPUT)));
        Assertions.assertSame(ndv, McvStatisticsPropagation.afterJoin(input, input, true)
                .getMultiColumnCombinedStats().get(Set.of(SOURCE)));
    }

    @Test
    public void testOrdinaryStatsAgreeWithConvertedMcv() {
        ColumnStatistic complete = ExpressionStatisticCalculator.calculate(CAST, input(true));
        Assertions.assertEquals(1, complete.getDistinctValuesCount());
        Assertions.assertEquals(0.4, complete.getNullsFraction(), 1e-9);
        Assertions.assertEquals(1, complete.getMinValue());
        Assertions.assertEquals(1, complete.getMaxValue());
        ColumnStatistic partial = ExpressionStatisticCalculator.calculate(CAST, input(false));
        Assertions.assertEquals(98, partial.getDistinctValuesCount());
        Assertions.assertEquals(0.1, partial.getNullsFraction(), 1e-9);
        Assertions.assertNull(partial.getMinString());
        Assertions.assertNull(partial.getMaxString());
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, partial.getMinValue());
        Assertions.assertEquals(Double.POSITIVE_INFINITY, partial.getMaxValue());
        Assertions.assertEquals(IntegerType.BIGINT.getTypeSize(), partial.getAverageRowSize());
    }

    @Test
    public void testJointProjectionPreservesCorrelationAndMergesCollisions() {
        ColumnRefOperator status = new ColumnRefOperator(3, VarcharType.VARCHAR, "status", false);
        MultiColumnCombinedStats joint = new MultiColumnCombinedStats(4, 1000, List.of(SOURCE, status), List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("1", "approved"), 400),
                new MultiColumnCombinedStats.McvEntry(List.of("01", "approved"), 200),
                new MultiColumnCombinedStats.McvEntry(List.of("bad", "failed"), 300),
                new MultiColumnCombinedStats.McvEntry(java.util.Arrays.asList(null, "failed"), 100)));
        Statistics input = Statistics.buildFrom(input(true))
                .addColumnStatistic(status, ColumnStatistic.builder().setDistinctValuesCount(2).build())
                .setMultiColumnStatistics(Map.of(Set.of(SOURCE, status), joint)).build();
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> projected = McvStatisticsPropagation.project(
                Map.of(OUTPUT, CAST, status, status), input);
        MultiColumnCombinedStats result = projected.get(Set.of(OUTPUT, status));
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.getNdv());
        Assertions.assertEquals(List.of(400L, 0L), result.getNullCounts());
        Assertions.assertEquals(600, result.getMcv().get(0).getCount());
        Statistics output = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(OUTPUT, ExpressionStatisticCalculator.calculate(CAST, input))
                .addColumnStatistic(status, input.getColumnStatistic(status)).setMultiColumnStatistics(projected).build();
        ScalarOperator predicate = new com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator(
                com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType.AND,
                new com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator(
                        com.starrocks.sql.ast.expression.BinaryType.EQ, OUTPUT,
                        com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createBigint(1)),
                new com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator(
                        com.starrocks.sql.ast.expression.BinaryType.EQ, status,
                        com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createVarchar("failed")));
        Assertions.assertEquals(0, McvStatisticsPropagation.completeHeadRows(predicate, output).orElseThrow());
        ScalarOperator originalPredicate = predicate.clone();
        originalPredicate.getChild(0).setChild(0, CAST);
        Assertions.assertEquals(McvStatisticsPropagation.completeHeadRows(originalPredicate, input).orElseThrow(),
                McvStatisticsPropagation.completeHeadRows(predicate, output).orElseThrow());
        ColumnRefOperator rhsKey = new ColumnRefOperator(4, IntegerType.BIGINT, "rhs_key", true);
        ColumnRefOperator rhsStatus = new ColumnRefOperator(5, VarcharType.VARCHAR, "rhs_status", false);
        MultiColumnCombinedStats right = new MultiColumnCombinedStats(1, 10, List.of(rhsKey, rhsStatus), List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("1", "approved"), 10)));
        Assertions.assertEquals(0.6, MultiColumnJoinMcvEstimator.estimateSelectivity(result,
                List.of(OUTPUT, status), 0.4, right, List.of(rhsKey, rhsStatus), 0).orElseThrow(), 1e-9);
        ColumnRefOperator renamed = new ColumnRefOperator(6, IntegerType.BIGINT, "renamed", true);
        Assertions.assertNotNull(McvStatisticsPropagation.project(Map.of(renamed, OUTPUT, status, status), output)
                .get(Set.of(renamed, status)));
        // Keeping both original and converted columns produces one joint group, not alias combinations.
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> aliases = McvStatisticsPropagation.project(
                Map.of(SOURCE, SOURCE, OUTPUT, CAST, status, status), input);
        Assertions.assertTrue(aliases.containsKey(Set.of(SOURCE, OUTPUT, status)));
        Assertions.assertEquals(3, aliases.size());
        Assertions.assertEquals(4, joint.getMcv().size());
    }

    @Test
    public void testPartialJointDoesNotInventExactMarginals() {
        ColumnRefOperator status = new ColumnRefOperator(3, VarcharType.VARCHAR, "status", false);
        MultiColumnCombinedStats group = new MultiColumnCombinedStats(100, 1000, List.of(SOURCE, status), List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("1", "approved"), 100, List.of(500L, 600L)),
                new MultiColumnCombinedStats.McvEntry(List.of("01", "approved"), 100, List.of(300L, 600L))));
        MultiColumnCombinedStats result = McvCastStatistics.projectGroup(Map.of(OUTPUT, CAST, status, status), group);
        Assertions.assertEquals(99, result.getNdv());
        Assertions.assertEquals(200, result.getMcv().get(0).getCount());
        Assertions.assertFalse(result.getMcv().get(0).hasComponentCounts());
        Assertions.assertTrue(result.getNullCounts().isEmpty());
    }

    @Test
    public void testWideningKeepsExistingTailHistogram() {
        ColumnRefOperator number = new ColumnRefOperator(7, IntegerType.INT, "integer", true);
        Histogram histogram = new Histogram(List.of(new Bucket(2, 100, 600L, 10L)), Map.of("1", 400L));
        Statistics input = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(number, ColumnStatistic.builder().setDistinctValuesCount(100)
                        .setHistogram(histogram).build())
                .addMultiColumnStatistics(Set.of(number), new MultiColumnCombinedStats(100, 1000, List.of(number),
                        List.of(new MultiColumnCombinedStats.McvEntry(List.of("1"), 400)))).build();
        Assertions.assertSame(histogram, ExpressionStatisticCalculator.calculate(
                new CastOperator(IntegerType.BIGINT, number), input).getHistogram());
    }
}
