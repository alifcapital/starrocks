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
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExternalMcvStatsAttachTest {
    @BeforeAll
    public static void beforeAll() {
        UtFrameUtils.createDefaultCtx();
    }
    private static final ColumnRefOperator STATUS = new ColumnRefOperator(1, VarcharType.VARCHAR, "status", true);
    private static final ColumnRefOperator GATE = new ColumnRefOperator(2, IntegerType.INT, "gate", true);
    private static final ColumnRefOperator EXTRA = new ColumnRefOperator(4, IntegerType.INT, "extra", true);

    private static final List<MultiColumnCombinedStats.McvEntry> MCV = List.of(
            new MultiColumnCombinedStats.McvEntry(List.of("approved", "0", "0"), 500, List.of(600L, 620L, 750L)));

    private static ExternalMcvStatistics.Group group(List<String> columns, long rows, long ndv,
                                                      List<MultiColumnCombinedStats.McvEntry> mcv) {
        return new ExternalMcvStatistics.Group(columns, rows, ndv, mcv, List.of(),
                java.util.Collections.nCopies(columns.size(), 0L));
    }

    private static Map<ColumnRefOperator, Column> read(ColumnRefOperator... refs) {
        Map<ColumnRefOperator, Column> map = new HashMap<>();
        for (ColumnRefOperator ref : refs) {
            map.put(ref, new Column(ref.getName(), ref.getType()));
        }
        return map;
    }

    private static Statistics attach(List<ExternalMcvStatistics.Group> groups,
                                     Map<ColumnRefOperator, Column> read) {
        return StatisticsCalcUtils.attachExternalMcvStats(Statistics.builder().setOutputRowCount(1000).build(),
                new ExternalMcvStatistics(groups), read);
    }

    @Test
    public void testSingletonCarriesBucketsAndNullsIntoColumnStatistics() {
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("gate"), 1000, 23,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("0"), 500, List.of(500L)),
                        new MultiColumnCombinedStats.McvEntry(Arrays.asList((String) null), 100, List.of(100L))),
                List.of(List.of("1", "10", "100", "10", "10"),
                        List.of("20", "30", "400", "10", "11")), List.of(100L));
        Statistics statistics = attach(List.of(group), read(GATE));
        ColumnStatistic column = statistics.getColumnStatistic(GATE);
        Assertions.assertFalse(column.isUnknown());
        Assertions.assertEquals(0.1, column.getNullsFraction(), 1e-9);
        Assertions.assertEquals(22, column.getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(900, column.getHistogram().getTotalRows());
        Assertions.assertEquals(2, column.getHistogram().getBuckets().size());
        Assertions.assertEquals(29L, column.getHistogram().getRowCountInBucket(25, 22, true).orElseThrow());
        Assertions.assertEquals(0, column.getMinValue(), 1e-9);
        Assertions.assertEquals(30, column.getMaxValue(), 1e-9);
        Statistics point = PredicateStatisticsCalculator.statisticsCalculate(
                new BinaryPredicateOperator(BinaryType.EQ, GATE, ConstantOperator.createInt(25)), statistics);
        Assertions.assertEquals(29, point.getOutputRowCount(), 1e-6);
        Assertions.assertEquals(1, StatisticsCalculator.computeGroupByStatistics(List.of(GATE), point,
                new HashMap<>()), 1e-6);
        Statistics range = PredicateStatisticsCalculator.statisticsCalculate(
                new BinaryPredicateOperator(BinaryType.GE, GATE, ConstantOperator.createInt(20)), statistics);
        Assertions.assertEquals(300, range.getOutputRowCount(), 1e-6);
        Assertions.assertEquals(11, StatisticsCalculator.computeGroupByStatistics(List.of(GATE), range,
                new HashMap<>()), 1e-6);
    }

    @Test
    public void testOneRowMcvUsesItsFrequencyWithAnIncompleteHead() {
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("status"), 1000, 102,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("approved"), 800, List.of(800L)),
                        new MultiColumnCombinedStats.McvEntry(List.of("rare"), 1, List.of(1L))),
                List.of(), List.of(0L));
        Statistics statistics = attach(List.of(group), read(STATUS));
        Statistics filtered = PredicateStatisticsCalculator.statisticsCalculate(
                new BinaryPredicateOperator(BinaryType.EQ, STATUS, ConstantOperator.createVarchar("rare")), statistics);
        Assertions.assertEquals(1, filtered.getOutputRowCount(), 1e-6);
    }

    @Test
    public void testGroupsWithUnreadColumnsAreKeptForTheirMcv() {
        Statistics statistics = attach(List.of(
                group(List.of("status", "gate", "type"), 1000, 12, MCV),
                group(List.of("status", "extra", "type"), 1000, 30, List.of()),
                group(List.of("gate", "extra"), 1000, 20, List.of()),
                group(List.of("status", "type"), 1000, 4, MCV)),
                read(STATUS, GATE, EXTRA));
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups = statistics.getMultiColumnCombinedStats();
        Assertions.assertEquals(4, groups.size());
        Assertions.assertTrue(groups.get(Set.of(STATUS, EXTRA)).hasDistribution());

        // (status, type) read by status only: kept for its MCV list under the one read column.
        MultiColumnCombinedStats single = groups.get(Set.of(STATUS));
        Assertions.assertEquals(Arrays.asList(STATUS, null), single.getColumns());
        Assertions.assertFalse(single.isComplete());
        Assertions.assertEquals(MCV, single.getMcv());

        MultiColumnCombinedStats partial = groups.get(Set.of(STATUS, GATE));
        Assertions.assertEquals(Arrays.asList(STATUS, GATE, null), partial.getColumns());
        Assertions.assertFalse(partial.isComplete());
        Assertions.assertTrue(partial.hasMcv());
        Assertions.assertEquals(MCV, partial.getMcv());

        MultiColumnCombinedStats complete = groups.get(Set.of(GATE, EXTRA));
        Assertions.assertTrue(complete.isComplete());
        Assertions.assertEquals(20, complete.getNdv());

        // NDV lookups see the complete group only.
        Assertions.assertNull(statistics.getLargestSubsetMCStats(Set.of(STATUS, GATE)));
        Assertions.assertEquals(Set.of(GATE, EXTRA),
                statistics.getLargestSubsetMCStats(Set.of(STATUS, GATE, EXTRA)).first);
    }

    @Test
    public void testCompleteGroupWinsOverOneWithUnreadColumns() {
        List<MultiColumnCombinedStats.McvEntry> pairMcv = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("approved", "0"), 550, List.of(600L, 620L)));
        Statistics statistics = attach(List.of(
                group(List.of("status", "gate", "type"), 1000, 12, MCV),
                group(List.of("status", "gate"), 1000, 5, pairMcv)),
                read(STATUS, GATE));
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups = statistics.getMultiColumnCombinedStats();
        Assertions.assertEquals(1, groups.size());
        MultiColumnCombinedStats stats = groups.get(Set.of(STATUS, GATE));
        Assertions.assertTrue(stats.isComplete());
        Assertions.assertEquals(5, stats.getNdv());
        Assertions.assertEquals(pairMcv, stats.getMcv());
        Assertions.assertEquals(Set.of(STATUS, GATE), statistics.getLargestSubsetMCStats(Set.of(STATUS, GATE)).first);
    }

    @Test
    public void testGroupsReadByOneColumnAttach() {
        List<MultiColumnCombinedStats.McvEntry> single = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("approved"), 600, List.of(600L)));
        Statistics statistics = attach(List.of(
                group(List.of("status"), 1000, 3, single),
                group(List.of("gate", "extra", "type"), 1000, 12, MCV),
                group(List.of("type"), 1000, 4, List.of())),
                read(STATUS, GATE));
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups = statistics.getMultiColumnCombinedStats();
        Assertions.assertEquals(2, groups.size());

        MultiColumnCombinedStats own = groups.get(Set.of(STATUS));
        Assertions.assertEquals(List.of(STATUS), own.getColumns());
        Assertions.assertTrue(own.isComplete());
        Assertions.assertEquals(3, own.getNdv());
        Assertions.assertEquals(single, own.getMcv());

        MultiColumnCombinedStats partial = groups.get(Set.of(GATE));
        Assertions.assertEquals(Arrays.asList(GATE, null, null), partial.getColumns());
        Assertions.assertFalse(partial.isComplete());
        Assertions.assertEquals(MCV, partial.getMcv());
    }

    @Test
    public void testNothingToAttachLeavesKnownStatisticsAlone() {
        Statistics input = Statistics.builder().setOutputRowCount(1000)
                .setStatsSource(Statistics.StatsSource.TABLE_METADATA).build();
        Statistics statistics = StatisticsCalcUtils.attachExternalMcvStats(input,
                new ExternalMcvStatistics(List.of(
                        group(List.of("status", "gate", "type"), 1000, 12, MCV),
                        group(List.of("gate", "type"), 1000, 12, List.of()))),
                read(EXTRA));
        Assertions.assertSame(input, statistics);
        // Even an empty head retains useful exact NULL counts for the read columns.
        statistics = StatisticsCalcUtils.attachExternalMcvStats(input,
                new ExternalMcvStatistics(List.of(
                        group(List.of("status", "gate"), 1000, 12, List.of()))),
                read(STATUS, EXTRA));
        Assertions.assertTrue(statistics.getMultiColumnCombinedStats().get(Set.of(STATUS)).hasDistribution());
    }

    @Test
    public void testCollectedRowsDoNotRequireBasicStatisticsOrAReadMcvColumn() {
        ExternalMcvStatistics cached = new ExternalMcvStatistics(List.of(
                group(List.of("status"), 1000, 3, List.of())));
        Statistics unknown = Statistics.builder().setOutputRowCount(1)
                .addColumnStatistic(STATUS, ColumnStatistic.unknown()).build();
        Statistics estimated = StatisticsCalcUtils.attachExternalMcvStats(unknown, cached, read(STATUS));
        Assertions.assertEquals(1000, estimated.getOutputRowCount());
        Assertions.assertEquals(Statistics.StatsSource.ANALYZE, estimated.getStatsSource());
        // COUNT(*) may read no column of any collected group, but still needs the table cardinality.
        estimated = StatisticsCalcUtils.attachExternalMcvStats(unknown, cached, read(EXTRA));
        Assertions.assertEquals(1000, estimated.getOutputRowCount());
        Assertions.assertTrue(estimated.getMultiColumnCombinedStats().isEmpty());

        Statistics pruned = Statistics.buildFrom(unknown).setOutputRowCount(10)
                .setStatsSource(Statistics.StatsSource.TABLE_METADATA).build();
        estimated = StatisticsCalcUtils.attachExternalMcvStats(pruned, cached, read(STATUS));
        Assertions.assertEquals(10, estimated.getOutputRowCount());
        Assertions.assertEquals(Statistics.StatsSource.TABLE_METADATA, estimated.getStatsSource());
    }
}
