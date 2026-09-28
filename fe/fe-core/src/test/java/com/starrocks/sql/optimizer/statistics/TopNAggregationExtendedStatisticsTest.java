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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.statistic.JoinStatisticsMeta;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopNAggregationExtendedStatisticsTest {
    private final ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
    private final ColumnRefOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
    private final ColumnRefOperator c = new ColumnRefOperator(3, IntegerType.INT, "c", true);
    private final List<Ordering> asc = List.of(new Ordering(a, true, false));

    private Statistics distribution(List<MultiColumnCombinedStats.McvEntry> head, long rows) {
        return Statistics.builder().setOutputRowCount(rows)
                .addColumnStatistic(a, new ColumnStatistic(0, 99, 0, 4, 100))
                .addColumnStatistic(b, new ColumnStatistic(0, 99, 0, 4, 100))
                .addColumnStatistic(c, new ColumnStatistic(0, 99, 0, 4, 100))
                .addMultiColumnStatistics(Set.of(a, b),
                        new MultiColumnCombinedStats(100, rows, List.of(a, b), head, List.of(0L, 0L))).build();
    }

    private MultiColumnCombinedStats.McvEntry entry(long rows, String x, String y) {
        return new MultiColumnCombinedStats.McvEntry(java.util.Arrays.asList(x, y), rows);
    }

    @Test
    void hotRowsAreNotManyGroupsAndRfUsesOriginalRowMass() {
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        entries.add(entry(999901, "0", "0"));
        for (int i = 1; i < 100; i++) {
            entries.add(entry(1, Integer.toString(i), Integer.toString(i)));
        }
        Statistics source = distribution(entries, 1000000);
        Statistics groups = TopNAggregationCost.groupDistribution(source, List.of(a, b), 100);
        assertEquals(10, TopNAggregationCost.estimateRetainedGroups(groups, 100, asc, 10));
        assertEquals(0.99991, TopNAggregationCost.estimateFilterSelectivity(source, List.of(a, b), asc, 10), 1e-9);
        assertEquals(0.00001, TopNAggregationCost.estimateFilterSelectivity(source, List.of(a, b),
                List.of(new Ordering(a, false, false)), 10), 1e-9);
        assertEquals(999901, source.getMultiColumnCombinedStats().get(Set.of(a, b)).getMcv().get(0).getCount());
    }

    @Test
    void rareRowsCanFormTheWholeBoundaryPeerGroup() {
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            entries.add(entry(i < 90 ? 1 : 1000, i < 90 ? "0" : "1", Integer.toString(i)));
        }
        Statistics source = Statistics.buildFrom(distribution(entries, 10090))
                .addColumnStatistic(a, new ColumnStatistic(0, 1, 0, 4, 2)).build();
        Statistics groups = TopNAggregationCost.groupDistribution(source, List.of(a, b), 100);
        assertEquals(90, TopNAggregationCost.estimateRetainedGroups(groups, 100, asc, 10));
        assertEquals(90.0 / 10090, TopNAggregationCost.estimateFilterSelectivity(source, List.of(a, b), asc, 10), 1e-9);
        assertEquals(10, TopNAggregationCost.estimateRetainedGroups(groups, 100,
                List.of(new Ordering(a, false, false)), 10));
        assertEquals(10, TopNAggregationCost.estimateRetainedGroups(groups, 100,
                List.of(new Ordering(a, true, false), new Ordering(b, false, false)), 10));
    }

    @Test
    void nullableBoundaryKeepsNullRowsForRfInBothDirections() {
        Statistics source = distribution(List.of(entry(800, null, "1"), entry(100, "0", "2"),
                entry(100, "1", "3")), 1000);
        source = Statistics.buildFrom(source)
                .addColumnStatistic(a, new ColumnStatistic(0, 1, 0.8, 4, 2))
                .addMultiColumnStatistics(Set.of(a, b), new MultiColumnCombinedStats(3, 1000, List.of(a, b),
                        source.getMultiColumnCombinedStats().get(Set.of(a, b)).getMcv(), List.of(800L, 0L))).build();
        for (boolean nullsFirst : List.of(true, false)) {
            for (boolean ascending : List.of(true, false)) {
                assertEquals(0.9, TopNAggregationCost.estimateFilterSelectivity(source, List.of(a, b),
                        List.of(new Ordering(a, ascending, nullsFirst)), 1), 1e-9);
            }
        }
    }

    @Test
    void projectedMcvNdvAndFilteredSlicesReachTheEstimator() {
        var tuples = List.of(new MultiColumnCombinedStats.McvEntry(List.of("0", "0", "1"), 50),
                new MultiColumnCombinedStats.McvEntry(List.of("0", "0", "2"), 50),
                new MultiColumnCombinedStats.McvEntry(List.of("1", "1", "3"), 50),
                new MultiColumnCombinedStats.McvEntry(List.of("1", "1", "4"), 50));
        Statistics source = Statistics.buildFrom(distribution(List.of(), 200)).setMultiColumnStatistics(Map.of())
                .addMultiColumnStatistics(Set.of(a, b, c),
                        new MultiColumnCombinedStats(4, 200, List.of(a, b, c), tuples, List.of(0L, 0L, 0L))).build();
        assertEquals(2, TopNAggregationCost.distinct(source, Set.of(a, b)));
        var filtered = PredicateStatisticsCalculator.statisticsCalculate(
                BinaryPredicateOperator.eq(c, ConstantOperator.createInt(1)), source);
        assertEquals(1, TopNAggregationCost.distinct(filtered, Set.of(a, b)));
        assertEquals(1, TopNAggregationCost.estimateFilterSelectivity(filtered, List.of(a, b), asc, 10));
    }

    @Test
    void partialHeadDoesNotInventAMarginalAndDisabledMcvFallsBack() {
        Statistics source = distribution(List.of(entry(400000, "0", "1")), 1000000);
        assertTrue(Double.isNaN(TopNAggregationCost.estimateLeadingPeers(source, asc.get(0), 100)));
        source = Statistics.buildFrom(source).addMultiColumnStatistics(Set.of(a, b),
                new MultiColumnCombinedStats(100, 1000000, List.of(a, b),
                        List.of(new MultiColumnCombinedStats.McvEntry(List.of("0", "1"),
                                400000, List.of(800000L, 400000L))))).build();
        assertEquals(80, TopNAggregationCost.estimateLeadingPeers(source, asc.get(0), 100));
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setCboEnableMcvEstimate(false);
        context.setThreadLocalInfo();
        try {
            assertTrue(Double.isNaN(TopNAggregationCost.estimateLeadingPeers(source, asc.get(0), 100)));
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    void partialDistinctPeersCanUseExactInputComponentFrequency() {
        List<MultiColumnCombinedStats.McvEntry> head = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            head.add(new MultiColumnCombinedStats.McvEntry(List.of("0", Integer.toString(i)),
                    1000, List.of(800000L, 1000L)));
        }
        Statistics source = distribution(head, 1000000);
        assertEquals(0.8, TopNAggregationCost.estimateFilterSelectivity(source, List.of(a, b), asc, 10), 1e-9);
    }

    @Test
    void rankStatisticsDerivationUsesInputMcvThroughLocalAggregation() {
        Statistics source = distribution(List.of(entry(10, "0", "1"), entry(20, "0", "2"),
                entry(30, "1", "3")), 60);
        source = Statistics.buildFrom(source).addMultiColumnStatistics(Set.of(a, b),
                new MultiColumnCombinedStats(3, 60, List.of(a, b),
                        source.getMultiColumnCombinedStats().get(Set.of(a, b)).getMcv(), List.of(0L, 0L))).build();
        var input = OptExpression.create(new LogicalValuesOperator(List.of(a, b)));
        input.setStatistics(source);
        var agg = OptExpression.create(new LogicalAggregationOperator(AggType.LOCAL, List.of(a, b), Map.of()), input);
        var factory = new ColumnRefFactory();
        var optimizer = OptimizerFactory.mockContext(factory);
        var aggregateContext = new ExpressionContext(agg);
        new StatisticsCalculator(aggregateContext, factory, optimizer).estimatorStats();
        agg.setStatistics(aggregateContext.getStatistics());
        var topn = new LogicalTopNOperator.Builder().setOrderByElements(asc).setTopNType(TopNType.RANK)
                .setSortPhase(SortPhase.PARTIAL).setLimit(1).build();
        topn.setTopNPushDownAgg();
        var context = new ExpressionContext(OptExpression.create(topn, agg));
        new StatisticsCalculator(context, factory, optimizer).estimatorStats();
        assertEquals(2, context.getStatistics().getOutputRowCount());
    }
    @Test
    void stringBoundaryUsesBinaryUtf8OrderAndWorksWithoutBasicStatistics() {
        var text = new ColumnRefOperator(4, VarcharType.VARCHAR, "text", false);
        // UTF-16 would put the supplementary character first; BE's UTF-8 order puts U+E000 first.
        String low = new String(Character.toChars(0xE000));
        String high = "😀";
        Statistics source = Statistics.builder().setOutputRowCount(100)
                .addColumnStatistic(text, ColumnStatistic.unknown())
                .addColumnStatistic(b, ColumnStatistic.unknown())
                .addMultiColumnStatistics(Set.of(text, b), new MultiColumnCombinedStats(3, 100, List.of(text, b),
                        List.of(entry(10, low, "1"), entry(20, low, "2"), entry(70, high, "3")), List.of(0L, 0L)))
                .build();
        List<Ordering> order = List.of(new Ordering(text, true, false));
        var grouped = TopNAggregationCost.groupDistribution(source, List.of(text, b), 3);
        assertEquals(2, TopNAggregationCost.estimateRetainedGroups(grouped, 3, order, 1));
        assertEquals(0.3, TopNAggregationCost.estimateFilterSelectivity(source, List.of(text, b), order, 1), 1e-9);
    }

    @Test
    void joinDegreeRefinesScanNdvAndJoinedCardinalityIsNotRescaled() throws Exception {
        int old = Config.statistic_join_optimizer_budget_ms;
        Config.statistic_join_optimizer_budget_ms = 10000;
        try {
            long[][] frequencies = { {100, 1}, {1, 1000}};
            List<JoinStatisticsData.Source> sources = new ArrayList<>();
            List<List<JoinStatisticsBasis.Slice>> sides = new ArrayList<>();
            List<JoinStatisticsScope> scans = new ArrayList<>();
            for (int side = 0; side < 2; side++) {
                var degree = JoinStatisticsEstimateTest.degree(frequencies[side]);
                sources.add(new JoinStatisticsData.Source("uuid" + side, 1, degree.getRowCount(),
                        List.of("predicate"), List.of(VarcharType.VARCHAR), List.of(List.of("value")),
                        new long[] {degree.getRowCount()}, Map.of(0, List.of(degree))));
                sides.add(List.of(new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(frequencies[side]),
                        new double[0][], false)));
                Table table = Mockito.mock(Table.class);
                Mockito.when(table.getUUID()).thenReturn("uuid" + side);
                ColumnRefOperator key = side == 0 ? a : b;
                scans.add(JoinStatisticsScope.scan(table, Map.of(key, new Column("id", IntegerType.BIGINT)),
                        degree.getRowCount()));
            }
            var data = new JoinStatisticsData(1, 2, sources, List.of(new JoinStatisticsBasis(0, List.of(0, 1),
                    sides, List.of(), new JoinStatisticsHeadKeys(new long[] {7, 9}))));
            var planner = new JoinStatisticsPlanner();
            var definitions = JoinStatisticsPlanner.class.getDeclaredField("definitions");
            definitions.setAccessible(true);
            definitions.set(planner, List.of(new JoinStatisticsMeta(1, JoinStatisticsEstimateTest.definition(2),
                    2, 1, 1, "test", 1)));
            var snapshots = JoinStatisticsPlanner.class.getDeclaredField("snapshots");
            snapshots.setAccessible(true);
            ((Map<Long, Optional<JoinStatisticsData>>) snapshots.get(planner)).put(1L, Optional.of(data));
            Statistics source = Statistics.builder().setOutputRowCount(101)
                    .addColumnStatistic(a, new ColumnStatistic(7, 9, 0, 8, 101))
                    .setJoinStatisticsScope(scans.get(0)).setJoinStatisticsPlanner(planner).build();
            assertEquals(2, TopNAggregationCost.distinct(source, Set.of(a)));
            var joined = JoinStatisticsScope.join(scans.get(0), scans.get(1), JoinOperator.INNER_JOIN,
                    BinaryPredicateOperator.eq(a, b));
            double rows = planner.estimate(joined).orElseThrow();
            source = Statistics.buildFrom(source).setOutputRowCount(rows).setJoinStatisticsScope(joined).build();
            double groups = TopNAggregationCost.distinct(source, Set.of(a));
            assertTrue(groups <= rows);
            assertEquals(rows, source.getOutputRowCount());
            assertTrue(Double.isFinite(TopNAggregationCost.estimateFilterSelectivity(source, List.of(a), asc, 1)));
        } finally {
            Config.statistic_join_optimizer_budget_ms = old;
        }
    }

}
