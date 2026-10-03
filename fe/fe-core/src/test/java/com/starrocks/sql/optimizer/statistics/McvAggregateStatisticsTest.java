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
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class McvAggregateStatisticsTest {
    private ColumnRefFactory factory;
    private ColumnRefOperator key;
    private ColumnRefOperator other;
    private ColumnRefOperator extra;

    @BeforeEach
    public void setUp() {
        UtFrameUtils.createDefaultCtx();
        factory = new ColumnRefFactory();
        key = factory.create("k", IntegerType.INT, true);
        other = factory.create("v", IntegerType.INT, true);
        extra = factory.create("extra", IntegerType.INT, true);
    }

    private Statistics input(boolean nullKey) {
        List<MultiColumnCombinedStats.McvEntry> head = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("1", "10"), 900, List.of(950L, 900L)),
                new MultiColumnCombinedStats.McvEntry(List.of("1", "20"), 50, List.of(950L, 50L)),
                new MultiColumnCombinedStats.McvEntry(Arrays.asList(nullKey ? null : "2", "30"), 50,
                        List.of(50L, 50L)));
        return Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(key, ColumnStatistic.builder().setDistinctValuesCount(nullKey ? 1 : 2)
                        .setNullsFraction(nullKey ? 0.05 : 0).build())
                .addColumnStatistic(other, ColumnStatistic.builder().setDistinctValuesCount(3).build())
                .addColumnStatistic(extra, ColumnStatistic.builder().setDistinctValuesCount(10).build())
                .addMultiColumnStatistics(Set.of(key, other),
                        new MultiColumnCombinedStats(3, 1000, List.of(key, other), head))
                .build();
    }

    private Statistics aggregate(Statistics input, AggType type, ColumnRefOperator... keys) {
        Group child = new Group(0);
        child.setStatistics(input);
        GroupExpression expression = new GroupExpression(
                new LogicalAggregationOperator(type, List.of(keys), Map.of()), List.of(child));
        expression.setGroup(new Group(1));
        ExpressionContext context = new ExpressionContext(expression);
        new StatisticsCalculator(context, factory, OptimizerFactory.mockContext(ConnectContext.get(), factory))
                .estimatorStats();
        return context.getStatistics();
    }

    private MultiColumnCombinedStats distribution(Statistics statistics, ColumnRefOperator... keys) {
        MultiColumnCombinedStats group = statistics.getMultiColumnCombinedStats().get(Set.of(keys));
        Assertions.assertNotNull(group, "GROUP BY must retain a derived distribution");
        Assertions.assertTrue(group.hasDistribution());
        return group;
    }

    @Test
    public void testGroupingSubsetDeduplicatesProjectedValues() {
        Statistics output = aggregate(input(false), AggType.GLOBAL, key);
        MultiColumnCombinedStats group = distribution(output, key);
        Assertions.assertEquals(2, group.getRowCount());
        Assertions.assertEquals(2, group.getNdv());
        Assertions.assertEquals(Map.of("1", 1L, "2", 1L), group.getMcv().stream().collect(
                Collectors.toMap(entry -> entry.getValues().get(0), MultiColumnCombinedStats.McvEntry::getCount)));
        RuntimeFilterStatistics probe = RuntimeFilterStatistics.from(key, output.getColumnStatistic(key),
                output.getMultiColumnCombinedStats().values(), output.getOutputRowCount());
        MultiColumnCombinedStats singleton = new MultiColumnCombinedStats(1, 1, List.of(key),
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("1"), 1)));
        RuntimeFilterStatistics build = RuntimeFilterStatistics.from(key, ColumnStatistic.unknown(),
                List.of(singleton), 1);
        Assertions.assertEquals(0.5, build.probePassFraction(probe, false).orElseThrow(), 1e-9);
    }

    @Test
    public void testGroupingCollapsesSignedFloatingPointZero() {
        ColumnRefOperator floating = factory.create("floating", FloatType.DOUBLE, false);
        Statistics input = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(floating, ColumnStatistic.builder().setDistinctValuesCount(2).build())
                .addMultiColumnStatistics(Set.of(floating), new MultiColumnCombinedStats(2, 1000,
                        List.of(floating), List.of(
                                new MultiColumnCombinedStats.McvEntry(List.of("-0.0"), 900),
                                new MultiColumnCombinedStats.McvEntry(List.of("0.0"), 100))))
                .build();
        Statistics output = aggregate(input, AggType.GLOBAL, floating);
        MultiColumnCombinedStats group = distribution(output, floating);
        Assertions.assertEquals(1, group.getRowCount());
        Assertions.assertEquals(1, group.getMcv().size());
        Assertions.assertEquals(1, output.getColumnStatistic(floating).getDistinctValuesCount());
    }

    @Test
    public void testGroupingTupleRecountsMarginalsAndNulls() {
        Statistics output = aggregate(input(true), AggType.GLOBAL, other, key);
        MultiColumnCombinedStats group = distribution(output, other, key);
        Assertions.assertEquals(List.of(other, key), group.getColumns());
        Assertions.assertEquals(3, group.getRowCount());
        Assertions.assertEquals(List.of(0L, 1L), group.getNullCounts());
        for (MultiColumnCombinedStats.McvEntry entry : group.getMcv()) {
            Assertions.assertEquals(1, entry.getCount());
            Assertions.assertEquals(List.of(1L, entry.getValues().get(1) == null ? 1L : 2L),
                    entry.getComponentCounts());
        }
        Assertions.assertEquals(1.0 / 3, output.getColumnStatistic(key).getNullsFraction(), 1e-9);
        Assertions.assertEquals(1, output.getColumnStatistic(key).getDistinctValuesCount());
    }

    @Test
    public void testPartialSingletonUsesDistinctCountsAndRetainsNullGroup() {
        Statistics input = Statistics.builder().setOutputRowCount(10000)
                .addColumnStatistic(key, ColumnStatistic.builder().setDistinctValuesCount(99)
                        .setNullsFraction(0.01).build())
                .addMultiColumnStatistics(Set.of(key), new MultiColumnCombinedStats(100, 10000, List.of(key),
                        List.of(new MultiColumnCombinedStats.McvEntry(List.of("1"), 9000)), List.of(100L)))
                .build();
        MultiColumnCombinedStats group = distribution(aggregate(input, AggType.GLOBAL, key), key);
        Assertions.assertEquals(100, group.getRowCount());
        Assertions.assertEquals(List.of(1L), group.getNullCounts());
        Assertions.assertTrue(group.getMcv().stream().allMatch(entry -> entry.getCount() == 1));
        Assertions.assertEquals(2, group.getMcv().size());
    }

    @Test
    public void testPartialTupleDoesNotReuseInputMarginalCounts() {
        Statistics input = Statistics.buildFrom(input(false))
                .addMultiColumnStatistics(Set.of(key, other), new MultiColumnCombinedStats(100, 1000,
                        List.of(key, other), List.of(new MultiColumnCombinedStats.McvEntry(
                                List.of("1", "10"), 900, List.of(950L, 900L)))))
                .build();
        MultiColumnCombinedStats group = distribution(aggregate(input, AggType.GLOBAL, key, other), key, other);
        Assertions.assertEquals(100, group.getRowCount());
        Assertions.assertEquals(1, group.getMcv().get(0).getCount());
        Assertions.assertTrue(group.getMcv().get(0).getComponentCounts().isEmpty());
    }

    @Test
    public void testCompleteCoveringGroupWinsOverPartialSingleton() {
        Statistics input = Statistics.buildFrom(input(false))
                .addMultiColumnStatistics(Set.of(key), new MultiColumnCombinedStats(2, 1000, List.of(key),
                        List.of(new MultiColumnCombinedStats.McvEntry(List.of("1"), 950))))
                .build();
        MultiColumnCombinedStats group = distribution(aggregate(input, AggType.GLOBAL, key), key);
        Assertions.assertEquals(2, group.getMcv().size());
        Assertions.assertTrue(group.getMcv().stream().allMatch(entry -> entry.getCount() == 1));
    }

    @Test
    public void testRepeatedGroupingDoesNotReapplyInputWeights() {
        Statistics once = aggregate(input(false), AggType.GLOBAL, key, other);
        Statistics twice = aggregate(once, AggType.GLOBAL, key);
        Assertions.assertEquals(2, distribution(twice, key).getRowCount());
        Assertions.assertTrue(distribution(twice, key).getMcv().stream().allMatch(entry -> entry.getCount() == 1));
    }

    @Test
    public void testUnknownGroupingColumnAndLocalStageDoNotInventDistribution() {
        Assertions.assertTrue(aggregate(input(false), AggType.GLOBAL, key, extra)
                .getMultiColumnCombinedStats().isEmpty());
        Assertions.assertTrue(aggregate(input(false), AggType.LOCAL, key)
                .getMultiColumnCombinedStats().isEmpty());
    }

    @Test
    public void testDisabledMcvDoesNotDeriveFrequencies() {
        ConnectContext.get().getSessionVariable().setCboEnableMcvEstimate(false);
        try {
            Assertions.assertTrue(aggregate(input(false), AggType.GLOBAL, key)
                    .getMultiColumnCombinedStats().isEmpty());
        } finally {
            ConnectContext.get().getSessionVariable().setCboEnableMcvEstimate(true);
        }
    }
}
