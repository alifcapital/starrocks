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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.cost.CostModel;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopNAggregationCostTest {
    private final ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
    private final ColumnRefOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
    private final ColumnRefOperator c = new ColumnRefOperator(3, IntegerType.INT, "c", true);
    private final List<Ordering> asc = List.of(new Ordering(a, true, false));

    private Statistics stats(double aNdv, long groupNdv, Histogram histogram, double nulls) {
        return Statistics.builder().setOutputRowCount(1000000)
                .addColumnStatistic(a, ColumnStatistic.builder().setMinValue(0).setMaxValue(99999)
                        .setDistinctValuesCount(aNdv).setAverageRowSize(4).setNullsFraction(nulls)
                        .setHistogram(histogram).build())
                .addColumnStatistic(b, new ColumnStatistic(0, 99999, 0, 4, 100000))
                .addColumnStatistic(c, new ColumnStatistic(0, 99999, 0, 4, 100000))
                .addMultiColumnStatistics(Set.of(a, b), new MultiColumnCombinedStats(groupNdv)).build();
    }

    @Test
    void smallPeerSetsKeepSortLargePeerSetsKeepOnlyFilter() {
        Statistics rare = stats(100000, 100000, null, 0);
        Statistics many = stats(2, 100000, null, 0);
        assertFalse(TopNAggregationCost.preferFilterOnly(rare, rare, List.of(a, b), asc, 10));
        assertTrue(TopNAggregationCost.preferFilterOnly(many, many, List.of(a, b), asc, 10));
    }

    @Test
    void jointNdvReplacesIndependentProductAndWorksForOrderTuple() {
        Statistics source = stats(1000, 2000, null, 0);
        assertEquals(2000, TopNAggregationCost.distinct(source, Set.of(a, b)));
        source = Statistics.buildFrom(source)
                .addMultiColumnStatistics(Set.of(a, b), new MultiColumnCombinedStats(2))
                .addMultiColumnStatistics(Set.of(a, b, c), new MultiColumnCombinedStats(100000)).build();
        assertTrue(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b, c),
                List.of(new Ordering(a, true, false), new Ordering(b, false, true)), 10));
    }

    @Test
    void leadingMcvDependsOnDirection() {
        Statistics source = stats(100000, 100000,
                new Histogram(List.of(new Bucket(1, 99999, 100000L, 1L)), Map.of("0", 900000L)), 0);
        assertTrue(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b), asc, 10));
        assertFalse(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b),
                List.of(new Ordering(a, false, false)), 10));
    }

    @Test
    void leadingBucketAndNullsAreAlsoConsidered() {
        Statistics source = stats(100000, 100000,
                new Histogram(List.of(new Bucket(0, 0, 900000L, 900000L),
                        new Bucket(1, 99999, 1000000L, 1L)), Map.of()), 0);
        assertTrue(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b), asc, 10));
        source = stats(100000, 100000, null, 0.9);
        assertTrue(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b),
                List.of(new Ordering(a, true, true)), 10));
        assertFalse(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b), asc, 10));
    }

    @Test
    void unknownStatisticsDoNotTurnOffPeerPreservingPushdown() {
        Statistics source = Statistics.builder().setOutputRowCount(1000000)
                .addColumnStatistic(a, ColumnStatistic.unknown()).build();
        assertFalse(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b), asc, 10));
        assertFalse(TopNAggregationCost.preferFilterOnly(null, null, List.of(a, b), asc, 10));
    }
    @Test
    void rankEstimateIncludesPeersAndRfOnlyUsesFirstOrderingKey() {
        Statistics source = stats(2, 100000, null, 0);
        assertEquals(50009, TopNAggregationCost.estimateRetainedGroups(source, 100000, asc, 10));
        assertEquals(1, TopNAggregationCost.estimateRetainedGroups(source, 1, asc, 10));
        double unary = TopNAggregationCost.estimateFilterSelectivity(source, List.of(a, b), asc, 10);
        double tuple = TopNAggregationCost.estimateFilterSelectivity(source, List.of(a, b),
                List.of(new Ordering(a, true, false), new Ordering(b, true, false)), 10);
        assertEquals(0.50009, unary);
        assertEquals(unary, tuple);
        assertEquals(1, TopNAggregationCost.estimateFilterSelectivity(null, List.of(a, b), asc, 10));
        assertTrue(Double.isNaN(TopNAggregationCost.estimateRetainedGroups(null, 100000, asc, 10)));
    }

    @Test
    void genericLimitClampMustNotEraseRankPeersEstimate() {
        ColumnRefFactory factory = new ColumnRefFactory();
        OptExpression input = OptExpression.create(new LogicalValuesOperator(List.of(a, b)));
        input.setStatistics(Statistics.buildFrom(stats(2, 100000, null, 0)).setOutputRowCount(100000).build());
        for (TopNType type : List.of(TopNType.RANK, TopNType.ROW_NUMBER)) {
            LogicalTopNOperator topn = new LogicalTopNOperator.Builder().setOrderByElements(asc)
                    .setSortPhase(SortPhase.PARTIAL).setTopNType(type).setLimit(10).build();
            topn.setTopNPushDownAgg();
            ExpressionContext context = new ExpressionContext(OptExpression.create(topn, input));
            new StatisticsCalculator(context, factory, OptimizerFactory.mockContext(factory)).estimatorStats();
            assertEquals(type == TopNType.RANK ? 50009 : 10, context.getStatistics().getOutputRowCount());
        }
    }

    @Test
    void filterEstimateKeepsNullRowsRegardlessOfOrdering() {
        for (boolean nullsFirst : List.of(false, true)) {
            List<Ordering> ordering = List.of(new Ordering(a, true, nullsFirst));
            assertEquals(1, TopNAggregationCost.estimateFilterSelectivity(
                    stats(1, 100000, null, 0.9), List.of(a, b), ordering, 10));
            assertTrue(TopNAggregationCost.estimateFilterSelectivity(
                    stats(10000, 100000, null, 0.9), List.of(a, b), ordering, 10) >= 0.9);
        }
    }

    @Test
    void forcedLocalAggregationAndRankBufferAreNotDiscountedAway() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext connection = new ConnectContext();
        connection.getSessionVariable().setEnableLocalShuffleAgg(false);
        connection.getSessionVariable().setPipelineDop(8);
        connection.setThreadLocalInfo();
        try {
            OptExpression input = OptExpression.create(new LogicalValuesOperator(List.of(a, b)));
            input.setStatistics(stats(2, 100000, null, 0));
            Statistics aggregateStats = Statistics.buildFrom(input.getStatistics()).setOutputRowCount(100000).build();
            PhysicalHashAggregateOperator agg = new PhysicalHashAggregateOperator(AggType.LOCAL, List.of(a, b),
                    List.of(a, b), Map.of(), true, -1, null, null);
            OptExpression expression = OptExpression.create(agg, input);
            expression.setStatistics(aggregateStats);
            double streamingMemory = CostModel.calculateCostEstimate(new ExpressionContext(expression)).getMemoryCost();
            agg.setTopNLocalAgg(true);
            // Forced preaggregation is required even in mode 0, without aggregate RF metadata.
            assertTrue(aggregateStats.getComputeSize() <=
                    CostModel.calculateCostEstimate(new ExpressionContext(expression)).getMemoryCost());
            assertTrue(streamingMemory < aggregateStats.getComputeSize());
            expression.setStatistics(Statistics.buildFrom(aggregateStats).setOutputRowCount(400000).build());
            assertEquals(input.getStatistics().getComputeSize(),
                    CostModel.calculateCostEstimate(new ExpressionContext(expression)).getCpuCost());
            expression.setStatistics(aggregateStats);
            agg.setTopNSortInfo(new LogicalTopNOperator.TopNSortInfo(asc, SortPhase.PARTIAL, TopNType.RANK, 10, 0));
            assertTrue(aggregateStats.getComputeSize() <=
                    CostModel.calculateCostEstimate(new ExpressionContext(expression)).getMemoryCost());
            PhysicalTopNOperator rank = new PhysicalTopNOperator(new OrderSpec(asc), 10, 0, List.of(), -1,
                    SortPhase.PARTIAL, TopNType.RANK, false, false, true, null, null, Map.of());
            rank.setTopNPushDownAgg();
            OptExpression sort = OptExpression.create(rank, expression);
            sort.setStatistics(Statistics.buildFrom(aggregateStats).setOutputRowCount(50009).build());
            var cost = CostModel.calculateCostEstimate(new ExpressionContext(sort));
            assertTrue(cost.getCpuCost() > 0);
            assertTrue(cost.getMemoryCost() > sort.getStatistics().getComputeSize());
            assertTrue(cost.getCpuCost() > TopNAggregationCost.sortCpu(100000,
                    aggregateStats.getAvgRowSize(), 4, 50009));
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    void shortStreamsCannotClaimSteadyStateRuntimeFilterSavings() {
        assertEquals(1, TopNAggregationCost.includeFilterWarmup(0.01, 524288, 8, 4096));
        assertEquals(0.2575, TopNAggregationCost.includeFilterWarmup(0.01, 4194304, 8, 4096));
        assertEquals(1, TopNAggregationCost.includeFilterWarmup(1, 4194304, 8, 4096));
        assertEquals(1, TopNAggregationCost.includeFilterWarmup(0.01, Double.NaN, 8, 4096));
    }

    @Test
    void rareLeadingRowsCanHaveManyDistinctBoundaryGroups() {
        Statistics source = stats(2, 100000,
                new Histogram(List.of(new Bucket(1, 1, 990000L, 990000L)), Map.of("0", 10000L)), 0);
        assertEquals(50009, TopNAggregationCost.estimateRetainedGroups(source, 100000, asc, 10));
        assertEquals(0.01, TopNAggregationCost.estimateFilterSelectivity(source, List.of(a, b), asc, 10));
        assertTrue(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b), asc, 10));
        assertEquals(0, TopNAggregationCost.filterCompactionWork(0.5, 524288, 8, 4096));
        assertEquals(0.375, TopNAggregationCost.filterCompactionWork(0.5, 4194304, 8, 4096));
    }

    @Test
    void localHashTablesDuplicateGroupsBeforeFinalShuffle() {
        assertEquals(2000, TopNAggregationCost.concurrentLocalGroups(524288, 2000, 1));
        assertEquals(16000, TopNAggregationCost.concurrentLocalGroups(524288, 2000, 8), 1);
        assertEquals(524288, TopNAggregationCost.concurrentLocalGroups(524288, 524288, 8));
        double groups = TopNAggregationCost.concurrentLocalGroups(524288, 248455, 8);
        assertTrue(groups > 400000 && groups <= 524288);
    }

}
