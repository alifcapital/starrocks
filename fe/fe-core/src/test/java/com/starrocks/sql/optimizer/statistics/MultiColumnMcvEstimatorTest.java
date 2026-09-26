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

import com.starrocks.catalog.Function;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MultiColumnMcvEstimatorTest {
    private static final double ROWS = 1000;

    private static final ColumnRefOperator STATUS = new ColumnRefOperator(1, VarcharType.VARCHAR, "status", true);
    private static final ColumnRefOperator GATE = new ColumnRefOperator(2, IntegerType.INT, "gate", true);
    private static final ColumnRefOperator TYPE = new ColumnRefOperator(3, IntegerType.INT, "type", true);
    private static final ColumnRefOperator EXTRA = new ColumnRefOperator(4, IntegerType.INT, "extra", true);

    @BeforeAll
    public static void beforeAll() {
        UtFrameUtils.createDefaultCtx();
    }

    // status, gate, type: the group; 80% of the rows are in the four most common tuples.
    private static Statistics statisticsWithMcv() {
        List<MultiColumnCombinedStats.McvEntry> mcv = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("approved", "0", "0"), 500),
                new MultiColumnCombinedStats.McvEntry(List.of("declined", "1", "0"), 200),
                new MultiColumnCombinedStats.McvEntry(List.of("approved", "0", "1"), 50),
                new MultiColumnCombinedStats.McvEntry(Arrays.asList(null, "2", "2"), 50));
        return Statistics.builder()
                .setOutputRowCount(ROWS)
                .addColumnStatistic(STATUS, ColumnStatistic.builder()
                        .setDistinctValuesCount(2).setNullsFraction(0.05).setAverageRowSize(8).build())
                .addColumnStatistic(GATE, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(3).setDistinctValuesCount(4).setNullsFraction(0).setAverageRowSize(4)
                        .build())
                .addColumnStatistic(TYPE, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(3).setDistinctValuesCount(4).setNullsFraction(0).setAverageRowSize(4)
                        .build())
                .addColumnStatistic(EXTRA, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(9).setDistinctValuesCount(10).setNullsFraction(0).setAverageRowSize(4)
                        .build())
                .addMultiColumnStatistics(Set.of(STATUS, GATE, TYPE),
                        new MultiColumnCombinedStats(12, 1000, List.of(STATUS, GATE, TYPE), mcv))
                .build();
    }

    // The same head with the rows each component value holds in its own column.
    private static List<MultiColumnCombinedStats.McvEntry> mcvWithComponentCounts() {
        return List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("approved", "0", "0"), 500, List.of(600L, 620L, 750L)),
                new MultiColumnCombinedStats.McvEntry(List.of("declined", "1", "0"), 200, List.of(300L, 250L, 750L)),
                new MultiColumnCombinedStats.McvEntry(List.of("approved", "0", "1"), 50, List.of(600L, 620L, 100L)),
                new MultiColumnCombinedStats.McvEntry(Arrays.asList(null, "2", "2"), 50, List.of(50L, 60L, 60L)));
    }

    private static Statistics statisticsWithComponentCounts() {
        return Statistics.buildFrom(statisticsWithMcv())
                .addMultiColumnStatistics(Set.of(STATUS, GATE, TYPE),
                        new MultiColumnCombinedStats(12, 1000, List.of(STATUS, GATE, TYPE), mcvWithComponentCounts()))
                .build();
    }

    // The query reads status and gate only; type is a null placeholder in the component order.
    private static Statistics statisticsWithUnreadColumn(List<MultiColumnCombinedStats.McvEntry> mcv) {
        return Statistics.builder()
                .setOutputRowCount(ROWS)
                .addColumnStatistic(STATUS, ColumnStatistic.builder()
                        .setDistinctValuesCount(2).setNullsFraction(0.05).setAverageRowSize(8).build())
                .addColumnStatistic(GATE, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(3).setDistinctValuesCount(4).setNullsFraction(0).setAverageRowSize(4)
                        .build())
                .addMultiColumnStatistics(Set.of(STATUS, GATE),
                        new MultiColumnCombinedStats(12, 1000, Arrays.asList(STATUS, GATE, null), mcv))
                .build();
    }

    private static BinaryPredicateOperator eq(ColumnRefOperator column, ConstantOperator constant) {
        return new BinaryPredicateOperator(BinaryType.EQ, column, constant);
    }

    private static ScalarOperator and(ScalarOperator... predicates) {
        return Utils.compoundAnd(List.of(predicates));
    }

    private static double sel(ScalarOperator predicate, Statistics statistics) {
        return StatisticsEstimateUtils.getPredicateSelectivity(predicate, statistics);
    }

    private static double clampTail(double value, double mcvTotal) {
        return Math.min(Math.max(0.0, 1.0 - mcvTotal), Math.max(0.0, value));
    }

    private static double estimateRows(ScalarOperator predicate, Statistics statistics) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics).getOutputRowCount();
    }

    private static Statistics completeStatusDistribution() {
        return Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(STATUS, ColumnStatistic.builder().setDistinctValuesCount(3)
                        .setNullsFraction(0).setAverageRowSize(8).build())
                .addMultiColumnStatistics(Set.of(STATUS), new MultiColumnCombinedStats(3, 1000, List.of(STATUS),
                        List.of(new MultiColumnCombinedStats.McvEntry(List.of("approved"), 800, List.of(800L)),
                                new MultiColumnCombinedStats.McvEntry(List.of("failed"), 150, List.of(150L)),
                                new MultiColumnCombinedStats.McvEntry(List.of("pending"), 50, List.of(50L)))))
                .build();
    }

    @Test
    public void testCorrelatedFilterReplacesUnconditionalSingletonForRuntimeFilter() {
        Statistics input = correlatedGateDistribution(false);
        Statistics filtered = PredicateStatisticsCalculator.statisticsCalculate(
                eq(STATUS, ConstantOperator.createVarchar("approved")), input);
        MultiColumnCombinedStats marginal = filtered.getMultiColumnCombinedStats().get(Set.of(GATE));
        Assertions.assertEquals(100, marginal.getRowCount(), 1e-9);
        Assertions.assertEquals(1, marginal.getNdv());
        Assertions.assertEquals(List.of("2"), marginal.getMcv().get(0).getValues());
        RuntimeFilterStatistics build = RuntimeFilterStatistics.from(GATE, filtered.getColumnStatistic(GATE),
                filtered.getMultiColumnCombinedStats().values(), filtered.getOutputRowCount());
        RuntimeFilterStatistics probe = RuntimeFilterStatistics.from(GATE, input.getColumnStatistic(GATE),
                List.of(input.getMultiColumnCombinedStats().get(Set.of(GATE))), 1000);
        Assertions.assertEquals(0.1, build.probePassFraction(probe, false).orElseThrow(), 1e-9);
        Assertions.assertEquals(1000, input.getMultiColumnCombinedStats().get(Set.of(GATE)).getRowCount(), 1e-9);
    }

    @Test
    public void testEqualityProjectsConditionalHeadAndResidualWithoutInventingMarginalCounts() {
        Statistics input = correlatedGateDistribution(true);
        ScalarOperator approved = eq(STATUS, ConstantOperator.createVarchar("approved"));
        Statistics filtered = McvStatisticsPropagation.filter(approved, input,
                Statistics.buildFrom(input).setOutputRowCount(300).build());
        MultiColumnCombinedStats marginal = filtered.getMultiColumnCombinedStats().get(Set.of(GATE));
        Assertions.assertEquals(300, marginal.getRowCount(), 1e-9);
        Assertions.assertEquals(3, marginal.getNdv());
        Assertions.assertEquals(1, marginal.getMcv().size());
        Assertions.assertEquals(List.of("2"), marginal.getMcv().get(0).getValues());
        // (2, approved) is an entire tuple frequency. It cannot occur again in the residual.
        Assertions.assertEquals(100, marginal.getMcv().get(0).getCount());
        Assertions.assertEquals(3, filtered.getColumnStatistic(GATE).getDistinctValuesCount(), 1e-9);
        Statistics repeated = McvStatisticsPropagation.filter(approved, filtered, filtered);
        Assertions.assertEquals(300, repeated.getMultiColumnCombinedStats().get(Set.of(GATE)).getRowCount(), 1e-9);
    }

    @Test
    public void testPartialHeadWithSeveralAcceptedStatusesDoesNotClaimExactGateMarginal() {
        Statistics input = correlatedGateDistribution(true);
        ScalarOperator predicate = new InPredicateOperator(false, STATUS,
                ConstantOperator.createVarchar("approved"), ConstantOperator.createVarchar("pending"));
        Statistics filtered = McvStatisticsPropagation.filter(predicate, input,
                Statistics.buildFrom(input).setOutputRowCount(300).build());
        Assertions.assertFalse(filtered.getMultiColumnCombinedStats().containsKey(Set.of(GATE)));
        Assertions.assertTrue(filtered.getMultiColumnCombinedStats().get(Set.of(STATUS, GATE)).getMcv()
                .stream().noneMatch(MultiColumnCombinedStats.McvEntry::hasComponentCounts));
    }

    private static Statistics correlatedGateDistribution(boolean residual) {
        long otherRows = residual ? 700 : 900;
        return Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(STATUS, ColumnStatistic.builder().setDistinctValuesCount(residual ? 3 : 2)
                        .setNullsFraction(0).build())
                .addColumnStatistic(GATE, ColumnStatistic.builder().setDistinctValuesCount(residual ? 4 : 2)
                        .setNullsFraction(0).build())
                .addMultiColumnStatistics(Set.of(STATUS, GATE), new MultiColumnCombinedStats(residual ? 4 : 2,
                        1000, List.of(STATUS, GATE), List.of(
                                new MultiColumnCombinedStats.McvEntry(List.of("declined", "1"), otherRows),
                                new MultiColumnCombinedStats.McvEntry(List.of("approved", "2"), 100))))
                .addMultiColumnStatistics(Set.of(GATE), new MultiColumnCombinedStats(residual ? 4 : 2,
                        1000, List.of(GATE), List.of(new MultiColumnCombinedStats.McvEntry(List.of("1"), otherRows),
                                new MultiColumnCombinedStats.McvEntry(List.of("2"), 100))))
                .build();
    }

    @Test
    public void testConditionalProjectionPreservesNullMembership() {
        Statistics input = Statistics.buildFrom(correlatedGateDistribution(true))
                .addColumnStatistic(GATE, ColumnStatistic.builder().setDistinctValuesCount(3).setNullsFraction(0.1).build())
                .addMultiColumnStatistics(Set.of(STATUS, GATE), new MultiColumnCombinedStats(4, 1000,
                        List.of(STATUS, GATE), List.of(
                                new MultiColumnCombinedStats.McvEntry(List.of("declined", "1"), 700),
                                new MultiColumnCombinedStats.McvEntry(Arrays.asList("approved", null), 100))))
                .build();
        Statistics filtered = McvStatisticsPropagation.filter(eq(STATUS, ConstantOperator.createVarchar("approved")),
                input, Statistics.buildFrom(input).setOutputRowCount(300).build());
        MultiColumnCombinedStats marginal = filtered.getMultiColumnCombinedStats().get(Set.of(GATE));
        Assertions.assertEquals(List.of(100L), marginal.getNullCounts());
        Assertions.assertEquals(2, filtered.getColumnStatistic(GATE).getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(1.0 / 3, filtered.getColumnStatistic(GATE).getNullsFraction(), 1e-9);
    }

    @Test
    public void testTripleGroupRequiresBothOtherComponentsFixedForResidualProjection() {
        Statistics input = statisticsWithMcv();
        ScalarOperator predicate = and(eq(STATUS, ConstantOperator.createVarchar("approved")),
                eq(TYPE, ConstantOperator.createInt(0)));
        Statistics output = Statistics.buildFrom(input).setOutputRowCount(600).build();
        Statistics filtered = McvStatisticsPropagation.filter(predicate, input, output);
        MultiColumnCombinedStats marginal = filtered.getMultiColumnCombinedStats().get(Set.of(GATE));
        Assertions.assertEquals(600, marginal.getRowCount(), 1e-9);
        Assertions.assertEquals(500, marginal.getMcv().get(0).getCount());
        Assertions.assertEquals(4, marginal.getNdv());
        Statistics partiallyFixed = McvStatisticsPropagation.filter(
                eq(STATUS, ConstantOperator.createVarchar("approved")), input, output);
        Assertions.assertFalse(partiallyFixed.getMultiColumnCombinedStats().containsKey(Set.of(GATE)));
    }

    @Test
    public void testGeneratedNullChecksPreserveOtherKeyDistribution() {
        Statistics input = Statistics.buildFrom(completeStatusDistribution())
                .addColumnStatistic(EXTRA, ColumnStatistic.builder().setDistinctValuesCount(100)
                        .setNullsFraction(0).build()).build();
        ScalarOperator extraNotNull = new IsNullPredicateOperator(true, EXTRA);
        Statistics filtered = PredicateStatisticsCalculator.statisticsCalculate(
                and(new IsNullPredicateOperator(true, STATUS), extraNotNull), input);
        Assertions.assertEquals(1000, filtered.getOutputRowCount(), 1e-6);
        Assertions.assertEquals(800, estimateRows(eq(STATUS, ConstantOperator.createVarchar("approved")), filtered), 1e-6);

        // A real filter on the MCV key still derives its conditional distribution.
        ScalarOperator approved = eq(STATUS, ConstantOperator.createVarchar("approved"));
        filtered = PredicateStatisticsCalculator.statisticsCalculate(and(approved, extraNotNull), input);
        Assertions.assertEquals(800, filtered.getOutputRowCount(), 1e-6);
        Assertions.assertEquals(800, estimateRows(approved, filtered), 1e-6);
    }

    @Test
    public void testExternalNullCheckWithUnknownOrNonzeroNullsStillDropsConditionalMcv() {
        for (ColumnStatistic basic : List.of(ColumnStatistic.unknown(),
                ColumnStatistic.builder().setDistinctValuesCount(100).setNullsFraction(0.1).build())) {
            Statistics input = Statistics.buildFrom(completeStatusDistribution())
                    .addColumnStatistic(EXTRA, basic).build();
            Statistics filtered = PredicateStatisticsCalculator.statisticsCalculate(
                    and(new IsNullPredicateOperator(true, STATUS), new IsNullPredicateOperator(true, EXTRA)), input);
            Assertions.assertTrue(filtered.getMultiColumnCombinedStats().isEmpty());
        }
    }

    @Test
    public void testDisjointPredicatesOnCompleteHeadAddTheirMasses() {
        ScalarOperator approved = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE,
                STATUS, ConstantOperator.createVarchar("app%"));
        ScalarOperator failed = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE,
                STATUS, ConstantOperator.createVarchar("fail%"));
        Assertions.assertEquals(950, estimateRows(Utils.compoundOr(List.of(approved, failed)),
                completeStatusDistribution()), 1e-6);
    }

    @Test
    public void testFilteredGroupByUsesSurvivingDistinctValues() {
        Statistics filtered = PredicateStatisticsCalculator.statisticsCalculate(
                new InPredicateOperator(false, STATUS, ConstantOperator.createVarchar("approved"),
                        ConstantOperator.createVarchar("failed")), completeStatusDistribution());
        Map<ColumnRefOperator, ColumnStatistic> grouped = new HashMap<>();
        Assertions.assertEquals(2, StatisticsCalculator.computeGroupByStatistics(List.of(STATUS), filtered, grouped), 1e-9);
        Assertions.assertNull(grouped.get(STATUS).getHistogram());
    }

    @Test
    public void testRepeatedFilterDoesNotApplyUnconditionalFrequencyAgain() {
        ScalarOperator approved = eq(STATUS, ConstantOperator.createVarchar("approved"));
        Statistics filtered = PredicateStatisticsCalculator.statisticsCalculate(approved, completeStatusDistribution());
        Assertions.assertEquals(800, estimateRows(approved, filtered), 1e-6);
    }

    @Test
    public void testFilteredDistributionFollowsAnAlias() {
        ColumnRefOperator alias = new ColumnRefOperator(99, VarcharType.VARCHAR, "renamed_status", true);
        Statistics input = completeStatusDistribution();
        Statistics projected = Statistics.builder().setOutputRowCount(input.getOutputRowCount())
                .addColumnStatistic(alias, input.getColumnStatistic(STATUS))
                .addMultiColumnStatistics(McvStatisticsPropagation.project(Map.of(alias, STATUS), input)).build();
        ScalarOperator approved = eq(alias, ConstantOperator.createVarchar("approved"));
        Statistics filtered = PredicateStatisticsCalculator.statisticsCalculate(approved, projected);
        Assertions.assertEquals(800, estimateRows(approved, filtered), 1e-6);
        Assertions.assertEquals(1, StatisticsCalculator.computeGroupByStatistics(List.of(alias), filtered,
                new HashMap<>()), 1e-9);
    }

    @Test
    public void testBareBooleanPredicatePreservesConditionalRuntimeFilterDistribution() {
        ColumnRefOperator flag = new ColumnRefOperator(100, BooleanType.BOOLEAN, "active", true);
        Statistics input = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(STATUS, ColumnStatistic.builder().setDistinctValuesCount(3).build())
                .addColumnStatistic(flag, ColumnStatistic.builder().setDistinctValuesCount(2)
                        .setNullsFraction(0.15).build())
                .addMultiColumnStatistics(Set.of(STATUS, flag),
                        new MultiColumnCombinedStats(3, 1000, List.of(STATUS, flag), List.of(
                                new MultiColumnCombinedStats.McvEntry(List.of("hot", "1"), 700),
                                new MultiColumnCombinedStats.McvEntry(List.of("cold", "0"), 150),
                                new MultiColumnCombinedStats.McvEntry(Arrays.asList("unknown", null), 150))))
                .build();
        RuntimeFilterStatistics probe = RuntimeFilterStatistics.from(STATUS, input.getColumnStatistic(STATUS),
                input.getMultiColumnCombinedStats().values(), input.getOutputRowCount());
        List<ScalarOperator> predicates = List.of(flag,
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, flag),
                eq(flag, ConstantOperator.createBoolean(true)));
        double[] fractions = {0.7, 0.15, 0.7};
        for (int i = 0; i < predicates.size(); i++) {
            Statistics filtered = PredicateStatisticsCalculator.statisticsCalculate(predicates.get(i), input);
            RuntimeFilterStatistics build = RuntimeFilterStatistics.from(STATUS, filtered.getColumnStatistic(STATUS),
                    filtered.getMultiColumnCombinedStats().values(), filtered.getOutputRowCount());
            Assertions.assertEquals(1000 * fractions[i], filtered.getOutputRowCount(), 1e-6);
            Assertions.assertEquals(1, build.getNdv(), 1e-6);
            Assertions.assertEquals(fractions[i], build.probePassFraction(probe, false).orElseThrow(), 1e-6);
        }
    }

    @Test
    public void testBooleanHeadEvaluationKeepsSqlNullSemantics() {
        Statistics statistics = Statistics.buildFrom(completeStatusDistribution())
                .addColumnStatistic(STATUS, ColumnStatistic.builder().setDistinctValuesCount(2)
                        .setNullsFraction(0.15).setAverageRowSize(8).build())
                .addMultiColumnStatistics(Set.of(STATUS), new MultiColumnCombinedStats(3, 1000, List.of(STATUS),
                        List.of(new MultiColumnCombinedStats.McvEntry(List.of("approved"), 700, List.of(700L)),
                                new MultiColumnCombinedStats.McvEntry(List.of("failed"), 150, List.of(150L)),
                                new MultiColumnCombinedStats.McvEntry(Arrays.asList((String) null), 150, List.of(150L)))))
                .build();
        ScalarOperator approved = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE,
                STATUS, ConstantOperator.createVarchar("app%"));
        ScalarOperator notApproved = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, approved);
        Assertions.assertEquals(150, estimateRows(notApproved, statistics), 1e-6);
        Assertions.assertEquals(850, estimateRows(Utils.compoundOr(List.of(approved, notApproved)), statistics), 1e-6);
    }

    @Test
    public void testSingleKeyJoinConsumesMcvBeforeAndAfterFiltering() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator left = factory.create("left_status", VarcharType.VARCHAR, false);
        ColumnRefOperator right = factory.create("right_status", VarcharType.VARCHAR, false);
        factory.updateColumnToRelationIds(left.getId(), 1);
        factory.updateColumnToRelationIds(right.getId(), 2);
        List<MultiColumnCombinedStats.McvEntry> head = completeStatusDistribution()
                .getMultiColumnCombinedStats().get(Set.of(STATUS)).getMcv();
        Statistics leftStats = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(left, completeStatusDistribution().getColumnStatistic(STATUS))
                .addMultiColumnStatistics(Set.of(left), new MultiColumnCombinedStats(3, 1000, List.of(left), head)).build();
        Statistics rightStats = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(right, completeStatusDistribution().getColumnStatistic(STATUS))
                .addMultiColumnStatistics(Set.of(right), new MultiColumnCombinedStats(3, 1000, List.of(right), head)).build();
        StatisticsCalculator calculator = new StatisticsCalculator(null, factory, null);
        BinaryPredicateOperator equality = new BinaryPredicateOperator(BinaryType.EQ, left, right);
        Assertions.assertEquals(665000, calculator.estimateInnerJoinStatistics(cross(leftStats, rightStats),
                List.of(equality), leftStats, rightStats).getOutputRowCount(), 1e-6);
        leftStats = PredicateStatisticsCalculator.statisticsCalculate(new InPredicateOperator(false, left,
                ConstantOperator.createVarchar("approved"), ConstantOperator.createVarchar("failed")), leftStats);
        rightStats = PredicateStatisticsCalculator.statisticsCalculate(new InPredicateOperator(false, right,
                ConstantOperator.createVarchar("approved"), ConstantOperator.createVarchar("failed")), rightStats);
        Assertions.assertEquals(662500, calculator.estimateInnerJoinStatistics(cross(leftStats, rightStats),
                List.of(equality), leftStats, rightStats).getOutputRowCount(), 1e-6);
        ColumnRefOperator aliasLeft = factory.create("cte_left", VarcharType.VARCHAR, true);
        ColumnRefOperator aliasRight = factory.create("cte_right", VarcharType.VARCHAR, true);
        Assertions.assertEquals(-1, factory.getRelationId(aliasLeft.getId()));
        Statistics cteLeft = Statistics.builder().setOutputRowCount(leftStats.getOutputRowCount())
                .addColumnStatistic(aliasLeft, leftStats.getColumnStatistic(left))
                .addMultiColumnStatistics(McvStatisticsPropagation.project(Map.of(aliasLeft, left), leftStats)).build();
        Statistics cteRight = Statistics.builder().setOutputRowCount(rightStats.getOutputRowCount())
                .addColumnStatistic(aliasRight, rightStats.getColumnStatistic(right))
                .addMultiColumnStatistics(McvStatisticsPropagation.project(Map.of(aliasRight, right), rightStats)).build();
        Assertions.assertEquals(662500, calculator.estimateInnerJoinStatistics(cross(cteLeft, cteRight),
                List.of(new BinaryPredicateOperator(BinaryType.EQ, aliasRight, aliasLeft)), cteLeft, cteRight)
                .getOutputRowCount(), 1e-6);
    }

    private static Statistics cross(Statistics left, Statistics right) {
        return Statistics.builder().setOutputRowCount(left.getOutputRowCount() * right.getOutputRowCount())
                .addColumnStatistics(left.getColumnStatistics()).addColumnStatistics(right.getColumnStatistics())
                .addMultiColumnStatistics(left.getMultiColumnCombinedStats())
                .addMultiColumnStatistics(right.getMultiColumnCombinedStats()).build();
    }

    @Test
    public void testNullCountsRemainUsableOutsideTheHead() {
        Statistics statistics = Statistics.buildFrom(completeStatusDistribution())
                .addMultiColumnStatistics(Set.of(STATUS), new MultiColumnCombinedStats(3, 1000, List.of(STATUS),
                        List.of(new MultiColumnCombinedStats.McvEntry(List.of("approved"), 800, List.of(800L))),
                        List.of(100L))).build();
        Assertions.assertEquals(100, estimateRows(new IsNullPredicateOperator(STATUS), statistics), 1e-6);
        ScalarOperator other = new BinaryPredicateOperator(BinaryType.NE, STATUS, ConstantOperator.createVarchar("approved"));
        Statistics filtered = PredicateStatisticsCalculator.statisticsCalculate(other, statistics);
        Assertions.assertEquals(100, filtered.getOutputRowCount(), 1e-6);
        Assertions.assertEquals(1, StatisticsCalculator.computeGroupByStatistics(List.of(STATUS), filtered,
                new HashMap<>()), 1e-6);

        Statistics emptyHead = Statistics.buildFrom(statistics)
                .addMultiColumnStatistics(Set.of(STATUS),
                        new MultiColumnCombinedStats(901, 1000, List.of(STATUS), List.of(), List.of(100L))).build();
        Assertions.assertEquals(100, estimateRows(new IsNullPredicateOperator(STATUS), emptyHead), 1e-6);
    }

    @Test
    public void testFullEqualityHitReturnsExactHeadCount() {
        Statistics statistics = statisticsWithMcv();
        ScalarOperator predicate = and(eq(STATUS, ConstantOperator.createVarchar("approved")),
                eq(GATE, ConstantOperator.createInt(0)), eq(TYPE, ConstantOperator.createInt(0)));
        // The only matching tuple is the predicate itself, so independence attributes to it exactly the
        // independence estimate of the predicate: nothing is left for the tail.
        Assertions.assertEquals(500, estimateRows(predicate, statistics), 1e-6);
    }

    @Test
    public void testSubsetOfGroupProjectsHeadAndAddsTail() {
        Statistics statistics = statisticsWithMcv();
        ScalarOperator predicate = and(eq(STATUS, ConstantOperator.createVarchar("approved")),
                eq(GATE, ConstantOperator.createInt(0)));

        double pStatus = sel(eq(STATUS, ConstantOperator.createVarchar("approved")), statistics);
        double pGate = sel(eq(GATE, ConstantOperator.createInt(0)), statistics);
        double pType0 = sel(eq(TYPE, ConstantOperator.createInt(0)), statistics);
        double pType1 = sel(eq(TYPE, ConstantOperator.createInt(1)), statistics);
        double simple = pStatus * pGate;
        double base = pStatus * pGate * pType0 + pStatus * pGate * pType1;
        double expected = 0.55 + clampTail(simple - base, 0.8);

        Assertions.assertEquals(ROWS * expected, estimateRows(predicate, statistics), 1e-6);
        Assertions.assertTrue(estimateRows(predicate, statistics) >= 550);
    }

    @Test
    public void testComponentCountsGiveExactSharesAndCapTheTail() {
        Statistics statistics = statisticsWithComponentCounts();
        ScalarOperator predicate = and(eq(STATUS, ConstantOperator.createVarchar("approved")),
                eq(GATE, ConstantOperator.createInt(0)));
        // Head: 0.55. Independence with the exact shares: 0.6 * 0.62 = 0.372 for the conjunction,
        // 0.6 * 0.62 * 0.75 + 0.6 * 0.62 * 0.1 = 0.3162 for the matching tuples, so 0.0558 is left for
        // the tail; but the rows with gate = 0 outside the matching tuples are only 0.62 - 0.55 = 0.05.
        Assertions.assertEquals(600, estimateRows(predicate, statistics), 1e-6);

        // IN over both gate values known to the head: independence leaves 0.6 * 0.87 - 0.3162 > 0.2 for
        // the tail, clamped to the tail mass 0.2; the status rows outside the matching tuples cap it at
        // 0.6 - 0.55 = 0.05.
        InPredicateOperator in = new InPredicateOperator(false, GATE, ConstantOperator.createInt(0),
                ConstantOperator.createInt(1));
        Assertions.assertEquals(600, estimateRows(and(eq(STATUS, ConstantOperator.createVarchar("approved")), in),
                statistics), 1e-6);

        // A value the head does not know falls back to the single-column estimate; the cap from the
        // status share, 0.6, does not bind.
        ScalarOperator unknown = and(eq(STATUS, ConstantOperator.createVarchar("approved")),
                eq(GATE, ConstantOperator.createInt(3)));
        double pGate3 = sel(eq(GATE, ConstantOperator.createInt(3)), statistics);
        Assertions.assertEquals(ROWS * clampTail(0.6 * pGate3, 0.8), estimateRows(unknown, statistics), 1e-6);
    }

    @Test
    public void testGroupWithUnreadColumnAnswersTheReadColumns() {
        Statistics statistics = statisticsWithUnreadColumn(mcvWithComponentCounts());
        ScalarOperator predicate = and(eq(STATUS, ConstantOperator.createVarchar("approved")),
                eq(GATE, ConstantOperator.createInt(0)));
        Assertions.assertTrue(MultiColumnMcvEstimator.estimate(Utils.extractConjuncts(predicate), statistics).isPresent());
        Assertions.assertEquals(600, estimateRows(predicate, statistics), 1e-6);
        // The combined NDV of the whole group says nothing about the two read columns.
        Assertions.assertNull(statistics.getLargestSubsetMCStats(Set.of(STATUS, GATE)));

        // Without component counts the independence share of a tuple needs the unread column's statistics.
        List<MultiColumnCombinedStats.McvEntry> mcv = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("approved", "0", "0"), 500),
                new MultiColumnCombinedStats.McvEntry(List.of("declined", "1", "0"), 200));
        Assertions.assertTrue(MultiColumnMcvEstimator.estimate(Utils.extractConjuncts(predicate),
                statisticsWithUnreadColumn(mcv)).isEmpty());
    }

    @Test
    public void testInAndRangePredicatesFilterTheHead() {
        Statistics statistics = statisticsWithMcv();
        double pStatus = sel(eq(STATUS, ConstantOperator.createVarchar("approved")), statistics);
        double pGate0 = sel(eq(GATE, ConstantOperator.createInt(0)), statistics);
        double pType0 = sel(eq(TYPE, ConstantOperator.createInt(0)), statistics);
        double pType1 = sel(eq(TYPE, ConstantOperator.createInt(1)), statistics);

        InPredicateOperator in = new InPredicateOperator(false, TYPE, ConstantOperator.createInt(0),
                ConstantOperator.createInt(1));
        ScalarOperator inPredicate = and(eq(STATUS, ConstantOperator.createVarchar("approved")), in);
        double inExpected = 0.55 + clampTail(pStatus * sel(in, statistics)
                - (pStatus * pGate0 * pType0 + pStatus * pGate0 * pType1), 0.8);
        Assertions.assertEquals(ROWS * inExpected, estimateRows(inPredicate, statistics), 1e-6);

        BinaryPredicateOperator lt = new BinaryPredicateOperator(BinaryType.LT, TYPE, ConstantOperator.createInt(1));
        ScalarOperator rangePredicate = and(eq(STATUS, ConstantOperator.createVarchar("approved")), lt);
        double rangeExpected = 0.5 + clampTail(pStatus * sel(lt, statistics) - pStatus * pGate0 * pType0, 0.8);
        Assertions.assertEquals(ROWS * rangeExpected, estimateRows(rangePredicate, statistics), 1e-6);
    }

    @Test
    public void testIsNullMatchesNullTupleComponent() {
        Statistics statistics = statisticsWithMcv();
        IsNullPredicateOperator isNull = new IsNullPredicateOperator(false, STATUS);
        ScalarOperator predicate = and(isNull, eq(GATE, ConstantOperator.createInt(2)));

        double pGate2 = sel(eq(GATE, ConstantOperator.createInt(2)), statistics);
        double pType2 = sel(eq(TYPE, ConstantOperator.createInt(2)), statistics);
        double simple = sel(isNull, statistics) * pGate2;
        double base = 0.05 * pGate2 * pType2;
        double expected = 0.05 + clampTail(simple - base, 0.8);
        Assertions.assertEquals(ROWS * expected, estimateRows(predicate, statistics), 1e-6);
    }

    @Test
    public void testFullEqualityMissIsBoundedByTheTail() {
        Statistics statistics = statisticsWithMcv();
        ScalarOperator predicate = and(eq(STATUS, ConstantOperator.createVarchar("declined")),
                eq(GATE, ConstantOperator.createInt(0)), eq(TYPE, ConstantOperator.createInt(0)));

        double simple = sel(eq(STATUS, ConstantOperator.createVarchar("declined")), statistics)
                * sel(eq(GATE, ConstantOperator.createInt(0)), statistics)
                * sel(eq(TYPE, ConstantOperator.createInt(0)), statistics);
        double uniformTail = 0.2 / (12 - 4);
        double minHeadShare = 0.05;
        double expected = Math.max(1.0 / ROWS, Math.min(simple, Math.min(uniformTail, minHeadShare)));
        Assertions.assertEquals(ROWS * expected, estimateRows(predicate, statistics), 1e-6);
    }

    @Test
    public void testColumnsOutsideTheGroupDecayOnTopOfTheHead() {
        Statistics statistics = statisticsWithMcv();
        ScalarOperator predicate = and(eq(STATUS, ConstantOperator.createVarchar("approved")),
                eq(GATE, ConstantOperator.createInt(0)), eq(TYPE, ConstantOperator.createInt(0)),
                eq(EXTRA, ConstantOperator.createInt(5)));
        double pExtra = sel(eq(EXTRA, ConstantOperator.createInt(5)), statistics);
        boolean decay = ConnectContext.get().getSessionVariable().isUseCorrelatedPredicateEstimate();
        double expected = 0.5 * Math.pow(pExtra, decay ? 0.5 : 1.0);
        Assertions.assertEquals(ROWS * expected, estimateRows(predicate, statistics), 1e-6);
    }

    @Test
    public void testOneGroupColumnAmongOthersIsEstimatedFromTheHead() {
        Statistics statistics = statisticsWithMcv();
        ScalarOperator predicate = and(eq(STATUS, ConstantOperator.createVarchar("approved")),
                eq(EXTRA, ConstantOperator.createInt(5)));
        Optional<MultiColumnMcvEstimator.Result> result =
                MultiColumnMcvEstimator.estimate(Utils.extractConjuncts(predicate), statistics);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(Set.of(Utils.extractConjuncts(predicate).get(0)), result.get().getConsumed());
        Assertions.assertTrue(estimateRows(predicate, statistics) > 0);
    }

    @Test
    public void testLonePredicateOnGroupColumnUsesTheHeadAndTheExactShare() {
        Statistics statistics = statisticsWithComponentCounts();
        // The approved head tuples hold 0.55; the column holds approved in 0.6 of the rows.
        Assertions.assertEquals(600, estimateRows(eq(STATUS, ConstantOperator.createVarchar("approved")), statistics),
                1e-6);
        // Not approved and not NULL: 1 - 0.6 - 0.05; the declined tuple holds 0.2 of it.
        Assertions.assertEquals(350, estimateRows(new BinaryPredicateOperator(BinaryType.NE, STATUS,
                ConstantOperator.createVarchar("approved")), statistics), 1e-6);
        Assertions.assertEquals(900, estimateRows(new InPredicateOperator(false, STATUS,
                ConstantOperator.createVarchar("approved"), ConstantOperator.createVarchar("declined")), statistics), 1e-6);
        Assertions.assertEquals(50, estimateRows(new InPredicateOperator(true, STATUS,
                ConstantOperator.createVarchar("approved"), ConstantOperator.createVarchar("declined")), statistics), 1e-6);
        Assertions.assertEquals(50, estimateRows(new IsNullPredicateOperator(false, STATUS), statistics), 1e-6);
        // The column statistics come from the plain estimate of the predicate.
        Statistics estimated = PredicateStatisticsCalculator.statisticsCalculate(
                eq(STATUS, ConstantOperator.createVarchar("approved")), statistics);
        Assertions.assertEquals(0, estimated.getColumnStatistic(STATUS).getNullsFraction(), 1e-9);

        // LIKE alone: the matching head tuples; the plain LIKE estimate leaves nothing for the tail.
        LikePredicateOperator like = new LikePredicateOperator(STATUS, ConstantOperator.createVarchar("app%"));
        double rows = estimateRows(like, statistics);
        Assertions.assertTrue(rows >= 550 - 1e-6 && rows <= 750 + 1e-6, String.valueOf(rows));

        // The plain estimates the MCV estimate is built from do not use the MCV lists themselves.
        double plain = PredicateStatisticsCalculator.statisticsCalculate(
                eq(STATUS, ConstantOperator.createVarchar("approved")), statistics, false).getOutputRowCount();
        Assertions.assertEquals(ROWS * sel(eq(STATUS, ConstantOperator.createVarchar("approved")), statistics), plain,
                1e-6);
        Assertions.assertNotEquals(600, plain, 1e-6);
    }

    @Test
    public void testLonePredicateOnTheOnlyReadColumnOfAGroup() {
        // The query reads gate only; status and type are placeholders.
        Statistics statistics = Statistics.builder()
                .setOutputRowCount(ROWS)
                .addColumnStatistic(GATE, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(3).setDistinctValuesCount(4).setNullsFraction(0).setAverageRowSize(4)
                        .build())
                .addMultiColumnStatistics(Set.of(GATE),
                        new MultiColumnCombinedStats(12, 1000, Arrays.asList(null, GATE, null), mcvWithComponentCounts()))
                .build();
        // gate = 0 holds 0.62 of the rows; the head tuples with it hold 0.55.
        Assertions.assertEquals(620, estimateRows(eq(GATE, ConstantOperator.createInt(0)), statistics), 1e-6);
        Assertions.assertEquals(620, estimateRows(new InPredicateOperator(false, GATE, ConstantOperator.createInt(0)),
                statistics), 1e-6);
        Assertions.assertEquals(380, estimateRows(new BinaryPredicateOperator(BinaryType.NE, GATE,
                ConstantOperator.createInt(0)), statistics), 1e-6);
    }

    // A group of one column: its own MCV list with the exact counts of the values.
    private static Statistics statisticsWithSingleColumnGroup(Statistics base, long approvedRows) {
        List<MultiColumnCombinedStats.McvEntry> mcv = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("approved"), approvedRows, List.of(approvedRows)),
                new MultiColumnCombinedStats.McvEntry(List.of("declined"), 300, List.of(300L)));
        return Statistics.buildFrom(base)
                .addMultiColumnStatistics(Set.of(STATUS), new MultiColumnCombinedStats(3, 1000, List.of(STATUS), mcv))
                .build();
    }

    @Test
    public void testSingleColumnGroup() {
        Statistics statistics = statisticsWithSingleColumnGroup(Statistics.buildFrom(statisticsWithMcv())
                .addColumnStatistic(STATUS, ColumnStatistic.builder()
                        .setDistinctValuesCount(3).setNullsFraction(0.05).setAverageRowSize(8).build())
                .build(), 600);
        Assertions.assertEquals(600, estimateRows(eq(STATUS, ConstantOperator.createVarchar("approved")), statistics),
                1e-6);
        // A value outside the head: the tail mass 0.1 over the one tail value, within the smallest head share.
        Assertions.assertEquals(100, estimateRows(eq(STATUS, ConstantOperator.createVarchar("other")), statistics),
                1e-6);
        // The narrowest group projects: two head values plus the one tail value.
        Assertions.assertEquals(3, MultiColumnMcvEstimator.projectedNdv(List.of(STATUS), statistics).orElseThrow(), 1e-9);
        // GROUP BY the one column takes the group's own distinct count, not the single-column estimate.
        Assertions.assertEquals(3, StatisticsCalculator.computeGroupByStatistics(List.of(STATUS), statistics,
                new HashMap<>()), 1e-9);

        // Among groups covering the same predicate columns, the narrowest answers.
        Statistics both = statisticsWithSingleColumnGroup(statisticsWithComponentCounts(), 700);
        Assertions.assertEquals(700, estimateRows(eq(STATUS, ConstantOperator.createVarchar("approved")), both), 1e-6);
        Assertions.assertEquals(600, estimateRows(and(eq(STATUS, ConstantOperator.createVarchar("approved")),
                eq(GATE, ConstantOperator.createInt(0))), both), 1e-6);
    }

    @Test
    public void testSessionVariableDisablesTheEstimate() {
        Statistics statistics = statisticsWithMcv();
        ScalarOperator predicate = and(eq(STATUS, ConstantOperator.createVarchar("approved")),
                eq(GATE, ConstantOperator.createInt(0)), eq(TYPE, ConstantOperator.createInt(0)));
        ConnectContext.get().getSessionVariable().setCboEnableMcvEstimate(false);
        try {
            Assertions.assertTrue(
                    MultiColumnMcvEstimator.estimate(Utils.extractConjuncts(predicate), statistics).isEmpty());
            double pStatus = sel(eq(STATUS, ConstantOperator.createVarchar("approved")), statistics);
            double pGate = sel(eq(GATE, ConstantOperator.createInt(0)), statistics);
            double pType = sel(eq(TYPE, ConstantOperator.createInt(0)), statistics);
            double expected = Math.max(Math.min((1.0 / 12) * (1 - 0.05), Math.min(pStatus, Math.min(pGate, pType))),
                    pStatus * pGate * pType);
            Assertions.assertEquals(ROWS * expected, estimateRows(predicate, statistics), 1e-6);
        } finally {
            ConnectContext.get().getSessionVariable().setCboEnableMcvEstimate(true);
        }
    }

    @Test
    public void testNdvOnlyStatsKeepTheCombinedNdvPath() {
        Statistics statistics = Statistics.buildFrom(statisticsWithMcv())
                .addMultiColumnStatistics(Set.of(STATUS, GATE, TYPE), new MultiColumnCombinedStats(12))
                .build();
        ScalarOperator predicate = and(eq(STATUS, ConstantOperator.createVarchar("approved")),
                eq(GATE, ConstantOperator.createInt(0)));
        Assertions.assertTrue(MultiColumnMcvEstimator.estimate(Utils.extractConjuncts(predicate), statistics).isEmpty());
    }

    @Test
    public void testGroupByProjectsTheHeadOntoItsColumns() {
        Statistics statistics = statisticsWithMcv();
        // Three distinct (status, gate) projections among the four head tuples; the eight tail tuples
        // project at the same rate.
        Assertions.assertEquals(9, MultiColumnMcvEstimator.projectedNdv(List.of(STATUS, GATE), statistics).orElseThrow(),
                1e-9);
        Assertions.assertEquals(9, MultiColumnMcvEstimator.projectedNdv(List.of(TYPE), statistics).orElseThrow(), 1e-9);
        Assertions.assertTrue(MultiColumnMcvEstimator.projectedNdv(List.of(STATUS, EXTRA), statistics).isEmpty());

        // Every head tuple projects to a distinct (status, type): 4 + 8, within the product 3 * 4 of the
        // single-column counts with the NULL of status.
        Map<ColumnRefOperator, ColumnStatistic> groupStatistics = new HashMap<>();
        Assertions.assertEquals(12, StatisticsCalculator.computeGroupByStatistics(List.of(STATUS, TYPE), statistics,
                groupStatistics), 1e-9);
        // The product of the single-column counts caps the projection.
        Assertions.assertEquals(3, StatisticsCalculator.computeGroupByStatistics(List.of(STATUS), statistics,
                groupStatistics), 1e-9);
        // The whole group has its exact count.
        Assertions.assertEquals(12, StatisticsCalculator.computeGroupByStatistics(List.of(STATUS, GATE, TYPE),
                statistics, groupStatistics), 1e-9);
    }

    @Test
    public void testCastAndFunctionOfGroupColumnsEvaluateOnTheHead() {
        Statistics statistics = statisticsWithComponentCounts();
        ScalarOperator castPredicate = and(eq(STATUS, ConstantOperator.createVarchar("approved")),
                new BinaryPredicateOperator(BinaryType.EQ, new CastOperator(VarcharType.VARCHAR, GATE),
                        ConstantOperator.createVarchar("0")));
        // Head 0.55. The cast equality has no exact share, so its single-column estimate stands in;
        // the exact 0.6 of status caps the tail at 0.05.
        double castRows = estimateRows(castPredicate, statistics);
        Assertions.assertTrue(castRows >= 550 - 1e-6 && castRows <= 600 + 1e-6, String.valueOf(castRows));

        Function upper = ExprUtils.getBuiltinFunction("upper", new Type[] {VarcharType.VARCHAR},
                Function.CompareMode.IS_IDENTICAL);
        ScalarOperator callPredicate = and(
                new BinaryPredicateOperator(BinaryType.EQ, new CallOperator("upper", VarcharType.VARCHAR, List.of(STATUS), upper),
                        ConstantOperator.createVarchar("APPROVED")),
                eq(GATE, ConstantOperator.createInt(0)));
        Optional<MultiColumnMcvEstimator.Result> result =
                MultiColumnMcvEstimator.estimate(Utils.extractConjuncts(callPredicate), statistics);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(2, result.get().getConsumed().size());
        // The head gives 0.55; the exact 0.62 of gate = 0 caps the tail at 0.07.
        double rows = estimateRows(callPredicate, statistics);
        Assertions.assertTrue(rows >= 550 - 1e-6 && rows <= 620 + 1e-6, String.valueOf(rows));
        Assertions.assertEquals(Optional.of(true), MultiColumnMcvEstimator.matchesComponent(
                Utils.extractConjuncts(callPredicate).get(0), STATUS, "approved"));
        Assertions.assertEquals(Optional.of(false), MultiColumnMcvEstimator.matchesComponent(
                Utils.extractConjuncts(callPredicate).get(0), STATUS, null));

        // An expression of two group columns is left to the regular estimation; the status equality
        // beside it is still answered from the head.
        ScalarOperator twoColumns = and(eq(STATUS, ConstantOperator.createVarchar("approved")),
                new BinaryPredicateOperator(BinaryType.EQ, new CallOperator("add", IntegerType.INT, List.of(GATE, TYPE)),
                        ConstantOperator.createInt(0)));
        result = MultiColumnMcvEstimator.estimate(Utils.extractConjuncts(twoColumns), statistics);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(Set.of(Utils.extractConjuncts(twoColumns).get(0)), result.get().getConsumed());
    }

    @Test
    public void testLikeFiltersTheHead() {
        Statistics statistics = statisticsWithComponentCounts();
        LikePredicateOperator like = new LikePredicateOperator(STATUS, ConstantOperator.createVarchar("app%"));
        ScalarOperator predicate = and(like, eq(GATE, ConstantOperator.createInt(0)));
        // (approved, 0, *) holds 0.55; gate = 0 has the exact share 0.62, so the tail is at most 0.07.
        double rows = estimateRows(predicate, statistics);
        Assertions.assertTrue(rows >= 550 - 1e-6 && rows <= 620 + 1e-6, String.valueOf(rows));
        Assertions.assertEquals(Optional.of(true), MultiColumnMcvEstimator.matches(like, VarcharType.VARCHAR, "approved"));
        Assertions.assertEquals(Optional.of(false), MultiColumnMcvEstimator.matches(like, VarcharType.VARCHAR, "declined"));
        Assertions.assertEquals(Optional.of(false), MultiColumnMcvEstimator.matches(like, VarcharType.VARCHAR, null));
    }

    @Test
    public void testValueComparisonByType() {
        Assertions.assertEquals(Optional.of(true), MultiColumnMcvEstimator.matches(
                eq(GATE, ConstantOperator.createInt(7)), IntegerType.INT, "7"));
        Assertions.assertEquals(Optional.of(false), MultiColumnMcvEstimator.matches(
                eq(GATE, ConstantOperator.createInt(7)), IntegerType.INT, "8"));
        Assertions.assertEquals(Optional.of(true), MultiColumnMcvEstimator.matches(
                new BinaryPredicateOperator(BinaryType.GE, GATE, ConstantOperator.createInt(7)), IntegerType.INT, "8"));
        Assertions.assertEquals(Optional.of(false), MultiColumnMcvEstimator.matches(
                new BinaryPredicateOperator(BinaryType.NE, GATE, ConstantOperator.createInt(7)), IntegerType.INT, "7"));
        // A NULL component never satisfies a comparison.
        Assertions.assertEquals(Optional.of(false), MultiColumnMcvEstimator.matches(
                new BinaryPredicateOperator(BinaryType.NE, GATE, ConstantOperator.createInt(7)), IntegerType.INT, null));
        Assertions.assertEquals(Optional.of(true), MultiColumnMcvEstimator.matches(
                new IsNullPredicateOperator(true, GATE), IntegerType.INT, "1"));

        // Decimals and floats compare as numbers, whatever the text form; strings compare bytewise.
        DecimalType amountType = new DecimalType(PrimitiveType.DECIMAL64, 10, 2);
        Assertions.assertEquals(Optional.of(0), MultiColumnMcvEstimator.compare(
                amountType, "1.50", ConstantOperator.createDecimal(new java.math.BigDecimal("1.5"), amountType)));
        Assertions.assertEquals(Optional.of(0), MultiColumnMcvEstimator.compare(
                FloatType.DOUBLE, "1.50", ConstantOperator.createDouble(1.5)));
        Assertions.assertEquals(Optional.of(-1), MultiColumnMcvEstimator.compare(
                VarcharType.VARCHAR, "apple", ConstantOperator.createVarchar("banana")));
        // Bytewise over UTF-8 as the BE: an emoji (F0 9F ...) sorts above a fullwidth letter (EF BC ...),
        // while UTF-16 units would put its surrogate first.
        String emoji = new String(Character.toChars(0x1F600));
        String fullwidthA = new String(Character.toChars(0xFF21));
        Assertions.assertEquals(Optional.of(1), MultiColumnMcvEstimator.compare(
                VarcharType.VARCHAR, emoji, ConstantOperator.createVarchar(fullwidthA)));
        Assertions.assertTrue(emoji.compareTo(fullwidthA) < 0);
        Assertions.assertEquals(Optional.of(0), MultiColumnMcvEstimator.compare(
                DateType.DATE, "2024-01-05", ConstantOperator.createDate(LocalDateTime.of(2024, 1, 5, 0, 0))));
        Assertions.assertEquals(Optional.of(0), MultiColumnMcvEstimator.compare(
                BooleanType.BOOLEAN, "1", ConstantOperator.createBoolean(true)));
        Assertions.assertTrue(MultiColumnMcvEstimator.compare(
                DateType.DATE, "not a date", ConstantOperator.createDate(LocalDateTime.of(2024, 1, 5, 0, 0))).isEmpty());

        // Integers and decimals compare exactly, beyond what a double can tell apart.
        Assertions.assertEquals(Optional.of(-1), MultiColumnMcvEstimator.compare(
                IntegerType.BIGINT, "9007199254740992", ConstantOperator.createBigint(9007199254740993L)));
        Assertions.assertEquals(Optional.of(0), MultiColumnMcvEstimator.compare(
                IntegerType.BIGINT, "9007199254740993", ConstantOperator.createBigint(9007199254740993L)));
        DecimalType wideType = new DecimalType(PrimitiveType.DECIMAL128, 38, 20);
        Assertions.assertEquals(Optional.of(1), MultiColumnMcvEstimator.compare(wideType, "1.00000000000000000002",
                ConstantOperator.createDecimal(new java.math.BigDecimal("1.00000000000000000001"), wideType)));
        Assertions.assertTrue(MultiColumnMcvEstimator.compare(IntegerType.BIGINT, "x",
                ConstantOperator.createBigint(1)).isEmpty());
    }

    @Test
    public void testUnreadableTupleValueDisablesTheEstimate() {
        List<MultiColumnCombinedStats.McvEntry> mcv = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("approved", "x"), 500));
        Statistics statistics = Statistics.builder()
                .setOutputRowCount(ROWS)
                .addColumnStatistic(STATUS, ColumnStatistic.builder()
                        .setDistinctValuesCount(2).setNullsFraction(0).setAverageRowSize(8).build())
                .addColumnStatistic(GATE, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(3).setDistinctValuesCount(4).setNullsFraction(0).setAverageRowSize(4)
                        .build())
                .addMultiColumnStatistics(Set.of(STATUS, GATE),
                        new MultiColumnCombinedStats(4, 1000, List.of(STATUS, GATE), mcv))
                .build();
        ScalarOperator predicate = and(eq(STATUS, ConstantOperator.createVarchar("approved")),
                eq(GATE, ConstantOperator.createInt(0)));
        Assertions.assertTrue(MultiColumnMcvEstimator.estimate(Utils.extractConjuncts(predicate), statistics).isEmpty());
        // The regular combined-NDV estimation still applies.
        Assertions.assertTrue(estimateRows(predicate, statistics) > 0);
    }
}
