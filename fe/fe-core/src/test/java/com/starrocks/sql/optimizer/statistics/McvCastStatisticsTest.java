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
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class McvCastStatisticsTest {
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
                        .setNullsFraction(complete ? 0.1 : 0).build())
                .addMultiColumnStatistics(Set.of(SOURCE), new MultiColumnCombinedStats(complete ? 4 : 100,
                        1000, List.of(SOURCE), values, List.of(complete ? 100L : 0L))).build();
    }

    @Test
    public void testCompleteCastMergesValuesAndNewNulls() {
        MultiColumnCombinedStats group = McvCastStatistics.derive(OUTPUT, CAST, input(true));
        Assertions.assertNotNull(group);
        Assertions.assertEquals(2, group.getNdv());
        Assertions.assertEquals(List.of(400L), group.getNullCounts());
        Assertions.assertEquals(600, group.getMcv().stream().filter(e -> e.getValues().get(0) != null)
                .mapToLong(MultiColumnCombinedStats.McvEntry::getCount).sum());
        RuntimeFilterStatistics cast = RuntimeFilterStatistics.fromExpression(CAST, input(true));
        Assertions.assertEquals(1, cast.getNdv());
        RuntimeFilterStatistics build = RuntimeFilterStatistics.from(OUTPUT,
                ColumnStatistic.builder().setDistinctValuesCount(1).build(), List.of(), 1);
        Assertions.assertEquals(0.6, build.probePassFraction(cast, false).orElseThrow(), 1e-9);
    }

    @Test
    public void testPartialCastRetainsTailEstimateAndAdjustsObservedCollisions() {
        MultiColumnCombinedStats group = McvCastStatistics.derive(OUTPUT, CAST, input(false));
        Assertions.assertNotNull(group);
        Assertions.assertEquals(99, group.getNdv()); // 98 non-NULL + one known NULL value
        Assertions.assertEquals(List.of(100L), group.getNullCounts());
        Assertions.assertEquals(700, group.getMcv().stream().mapToLong(MultiColumnCombinedStats.McvEntry::getCount).sum());
        Assertions.assertEquals(98, RuntimeFilterStatistics.fromExpression(CAST, input(false)).getNdv());
    }

    @Test
    public void testBasicNdvSurvivesWithoutMcv() {
        Statistics basic = Statistics.buildFrom(input(false)).setMultiColumnStatistics(Map.of()).build();
        Assertions.assertEquals(100, RuntimeFilterStatistics.fromExpression(CAST, basic).getNdv());
    }

    @Test
    public void testProjectionAndExpressionUseTheSameDistribution() {
        Statistics input = input(true);
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> projected = McvStatisticsPropagation.project(
                Map.of(SOURCE, SOURCE, OUTPUT, CAST), input);
        MultiColumnCombinedStats group = projected.get(Set.of(OUTPUT));
        Assertions.assertNotNull(group);
        RuntimeFilterStatistics fromSlot = RuntimeFilterStatistics.from(OUTPUT,
                ExpressionStatisticCalculator.calculate(CAST, input), List.of(group), 1000);
        RuntimeFilterStatistics fromExpression = RuntimeFilterStatistics.fromExpression(CAST, input);
        Assertions.assertEquals(fromSlot.getNdv(), fromExpression.getNdv());
        Assertions.assertEquals(fromSlot.probePassFraction(fromExpression, true).orElseThrow(),
                fromExpression.probePassFraction(fromSlot, true).orElseThrow(), 1e-9);
        Assertions.assertEquals(4, input.getMultiColumnCombinedStats().get(Set.of(SOURCE)).getMcv().size());
        Assertions.assertEquals(400, input.getMultiColumnCombinedStats().get(Set.of(SOURCE)).getMcv().get(0).getCount());
    }

    @Test
    public void testCastChainDoesNotUnwrapBackToOriginalText() {
        ScalarOperator roundTrip = new CastOperator(VarcharType.VARCHAR, CAST);
        ColumnRefOperator textOutput = new ColumnRefOperator(3, VarcharType.VARCHAR, "converted_text", true);
        MultiColumnCombinedStats group = McvCastStatistics.derive(textOutput, roundTrip, input(true));
        Assertions.assertEquals(2, group.getNdv());
        Assertions.assertEquals(List.of(400L), group.getNullCounts());
        Assertions.assertTrue(group.getMcv().stream().allMatch(e -> e.getValues().get(0) == null
                || e.getValues().get(0).equals("1")));
    }

    @Test
    public void testTupleMarginalIsCountedOnce() {
        ColumnRefOperator other = new ColumnRefOperator(3, IntegerType.INT, "other", false);
        MultiColumnCombinedStats joint = new MultiColumnCombinedStats(1000, 1000, List.of(SOURCE, other), List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("1", "10"), 100, List.of(500L, 100L)),
                new MultiColumnCombinedStats.McvEntry(List.of("1", "20"), 100, List.of(500L, 100L)),
                new MultiColumnCombinedStats.McvEntry(List.of("01", "30"), 100, List.of(300L, 100L))));
        Statistics input = Statistics.buildFrom(input(false)).setMultiColumnStatistics(Map.of(Set.of(SOURCE, other), joint))
                .addColumnStatistic(other, ColumnStatistic.builder().setDistinctValuesCount(100).build()).build();
        MultiColumnCombinedStats cast = McvCastStatistics.derive(OUTPUT, CAST, input);
        Assertions.assertEquals(800, cast.getMcv().get(0).getCount());
        Assertions.assertEquals(99, cast.getNdv());
    }

    @Test
    public void testDisabledMcvKeepsBasicFallback() {
        ConnectContext.get().getSessionVariable().setCboEnableMcvEstimate(false);
        Assertions.assertNull(McvCastStatistics.derive(OUTPUT, CAST, input(true)));
        Assertions.assertEquals(3, RuntimeFilterStatistics.fromExpression(CAST, input(true)).getNdv());
    }

    @Test
    public void testDecimalRoundingMergesKnownValues() {
        DecimalType target = new DecimalType(PrimitiveType.DECIMAL64, 10, 1);
        ColumnRefOperator output = new ColumnRefOperator(2, target, "rounded", true);
        Statistics input = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(SOURCE, ColumnStatistic.builder().setDistinctValuesCount(2).build())
                .addMultiColumnStatistics(Set.of(SOURCE), new MultiColumnCombinedStats(2, 1000, List.of(SOURCE), List.of(
                        new MultiColumnCombinedStats.McvEntry(List.of("1.01"), 400),
                        new MultiColumnCombinedStats.McvEntry(List.of("1.04"), 600)))).build();
        MultiColumnCombinedStats result = McvCastStatistics.derive(output, new CastOperator(target, SOURCE), input);
        Assertions.assertEquals(1, result.getNdv());
        Assertions.assertEquals(1000, result.getMcv().get(0).getCount());
    }

    @Test
    public void testDateDistributionCanBeConsumedByAnotherCast() {
        ColumnRefOperator date = new ColumnRefOperator(2, DateType.DATE, "date", true);
        Statistics input = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(SOURCE, ColumnStatistic.builder().setDistinctValuesCount(1).build())
                .addMultiColumnStatistics(Set.of(SOURCE), new MultiColumnCombinedStats(1, 1000, List.of(SOURCE),
                        List.of(new MultiColumnCombinedStats.McvEntry(List.of("2025-01-02"), 1000)))).build();
        MultiColumnCombinedStats dates = McvCastStatistics.derive(date, new CastOperator(DateType.DATE, SOURCE), input);
        Assertions.assertTrue(dates.getMcv().get(0).getValues().get(0).startsWith("2025-01-02"));
        Statistics projected = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(date, ColumnStatistic.builder().setDistinctValuesCount(1).build())
                .addMultiColumnStatistics(Set.of(date), dates).build();
        MultiColumnCombinedStats back = McvCastStatistics.derive(SOURCE,
                new CastOperator(VarcharType.VARCHAR, date), projected);
        Assertions.assertEquals(1, back.getNdv());
        Assertions.assertEquals(List.of(0L), back.getNullCounts());
        Assertions.assertTrue(back.getMcv().get(0).getValues().get(0).startsWith("2025-01-02"));
    }
}
