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
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.planner.SlotId;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.statistic.JoinStatisticsMeta;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

class JoinStatisticsPlannerTest {
    private final ColumnRefOperator status = new ColumnRefOperator(1, VarcharType.VARCHAR, "status", false);
    private final ColumnRefOperator gate = new ColumnRefOperator(2, IntegerType.INT, "gate", false);
    private final ColumnRefOperator other = new ColumnRefOperator(3, IntegerType.INT, "other", false);
    private final Map<ColumnRefOperator, JoinStatisticsScope.ColumnOrigin> columns = Map.of(
            status, new JoinStatisticsScope.ColumnOrigin("uuid0", "status", status.getType()),
            gate, new JoinStatisticsScope.ColumnOrigin("uuid0", "gate", gate.getType()),
            other, new JoinStatisticsScope.ColumnOrigin("uuid0", "other", other.getType()));

    @Test
    void missingInCombinationIsCheckedWithinTheOtherPredicateContext() {
        var source = new JoinStatisticsData.Source("uuid0", 1, 100, List.of("status", "gate"),
                List.of(status.getType(), gate.getType()), List.of(List.of("approved", "0"), List.of("failed", "1")),
                new long[] {90, 10}, Map.of());
        var in = new com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator(false, status,
                ConstantOperator.createVarchar("approved"), ConstantOperator.createVarchar("failed"));
        var filter = new BinaryPredicateOperator(BinaryType.EQ, gate, ConstantOperator.createInt(0));
        var selected = JoinStatisticsPlanner.select(source,
                new JoinStatisticsScope.Source("uuid0", 95, List.of(in, filter)), columns);
        Assertions.assertNotNull(selected);
        Assertions.assertTrue(selected.isExtrapolated(), "failed exists, but not in the collected gate=0 slice");
        Assertions.assertEquals(95, selected.rowLimit());
        Assertions.assertArrayEquals(new double[] {1, 0.5}, selected.sliceWeights(2));
        var allAndNew = new com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator(false, status,
                ConstantOperator.createVarchar("approved"), ConstantOperator.createVarchar("failed"),
                ConstantOperator.createVarchar("new"));
        var all = JoinStatisticsPlanner.select(source,
                new JoinStatisticsScope.Source("uuid0", 150, List.of(allAndNew)), columns);
        Assertions.assertTrue(all.isExtrapolated());
        Assertions.assertArrayEquals(new double[] {1.5, 1.5}, all.sliceWeights(2));
        var empty = new JoinStatisticsData.Source("uuid0", -1, 0, List.of("status"), List.of(status.getType()),
                List.of(), new long[0], Map.of());
        Assertions.assertNull(JoinStatisticsPlanner.select(empty,
                new JoinStatisticsScope.Source("uuid0", 50000, List.of(in)), columns));
    }

    @Test
    void physicalRfGateUsesCorrelatedMembershipAndSnapshotRows() throws Exception {
        int oldBudget = Config.statistic_join_optimizer_budget_ms;
        Config.statistic_join_optimizer_budget_ms = 5000;
        try {
            long[][] counts = { {2, 30, 4}, {100, 0, 1}};
            List<JoinStatisticsData.Source> sources = new ArrayList<>();
            for (int side = 0; side < 2; side++) {
                var degree = JoinStatisticsEstimateTest.degree(counts[side]);
                sources.add(new JoinStatisticsData.Source("uuid" + side, 1, degree.getRowCount(),
                        List.of("predicate"), List.of(VarcharType.VARCHAR), List.of(List.of("value")),
                        new long[] {degree.getRowCount()}, Map.of(0, List.of(degree))));
            }
            var data = new JoinStatisticsData(1, 2, sources, List.of(new JoinStatisticsBasis(0, List.of(0, 1),
                    List.of(List.of(slice(counts[0])), List.of(slice(counts[1]))))));
            var meta = new JoinStatisticsMeta(1, JoinStatisticsEstimateTest.definition(2), 2, 1, 1, "test", 1);
            var planner = new JoinStatisticsPlanner();
            var definitions = JoinStatisticsPlanner.class.getDeclaredField("definitions");
            definitions.setAccessible(true);
            definitions.set(planner, List.of(meta));
            var snapshots = JoinStatisticsPlanner.class.getDeclaredField("snapshots");
            snapshots.setAccessible(true);
            @SuppressWarnings("unchecked")
            var loaded = (Map<Long, Optional<JoinStatisticsData>>) snapshots.get(planner);
            loaded.put(1L, Optional.of(data));
            List<PlanNode> nodes = new ArrayList<>();
            for (int side = 0; side < 2; side++) {
                var column = new ColumnRefOperator(side + 1, IntegerType.BIGINT, "id", false);
                Table table = Mockito.mock(Table.class);
                Mockito.when(table.getUUID()).thenReturn("uuid" + side);
                var scope = JoinStatisticsScope.scan(table, Map.of(column, new Column("id", column.getType())), 1000);
                PlanNode node = new PlanNode(new PlanNodeId(side), "test") {
                    @Override
                    protected void toThrift(TPlanNode message) {
                    }
                };
                node.computeStatistics(Statistics.builder().setOutputRowCount(1000)
                        .addColumnStatistic(column, ColumnStatistic.builder().setDistinctValuesCount(1000).build())
                        .setJoinStatisticsScope(scope).setJoinStatisticsPlanner(planner).build());
                nodes.add(node);
            }
            var probeSlot = new SlotRef(new SlotId(1));
            var build = nodes.get(1).getRuntimeFilterStatistics(new SlotRef(new SlotId(2)));
            var probe = nodes.get(0).getRuntimeFilterStatistics(probeSlot);
            Assertions.assertEquals(2, build.getNdv());
            Assertions.assertEquals(6.0 / 36, build.probePassFraction(probe, false).orElseThrow(), 1e-6,
                    "Use frequency times presence divided by snapshot rows, not JOIN rows or basic rows");
            SessionVariable session = new SessionVariable();
            var rf = new RuntimeFilterDescription(session);
            rf.setBuildKeyStatistics(build);
            Assertions.assertTrue(rf.canProbeUse(nodes.get(0), probeSlot, null),
                    "Local RF is selective even though the ordinary NDV ratio is two thirds");
            Assertions.assertEquals(2.0 / 3, build.probePassFraction(probe, true).orElseThrow(), 1e-6,
                    "Null-safe RF must retain the existing fallback until null correlations are collected");
        } finally {
            Config.statistic_join_optimizer_budget_ms = oldBudget;
        }
    }

    @Test
    void normalizedBooleanPredicatesSelectExactSlicesIncludingFalseAndExcludingNull() {
        var active = new ColumnRefOperator(9, BooleanType.BOOLEAN, "active", true);
        var source = new JoinStatisticsData.Source("uuid0", 1, 6, List.of("active"), List.of(BooleanType.BOOLEAN),
                List.of(List.of("true"), List.of("false"), java.util.Arrays.asList((String) null)),
                new long[] {2, 3, 1}, Map.of(0, List.of(JoinStatisticsEstimateTest.degree(2, 0),
                        JoinStatisticsEstimateTest.degree(0, 3), JoinStatisticsEstimateTest.degree(1, 0))));
        var origins = Map.of(active, new JoinStatisticsScope.ColumnOrigin("uuid0", "active", active.getType()));
        var yes = JoinStatisticsPlanner.select(source, new JoinStatisticsScope.Source("uuid0", 6, List.of(active)), origins);
        var no = JoinStatisticsPlanner.select(source, new JoinStatisticsScope.Source("uuid0", 6,
                List.of(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, active))), origins);
        Assertions.assertNotNull(yes);
        Assertions.assertNotNull(no);
        Assertions.assertEquals(2, yes.keyStatistics(source, 0).rows());
        Assertions.assertEquals(3, no.keyStatistics(source, 0).rows());
    }

    @Test
    void completedPlanningCannotStartAnotherStatisticsLoad() {
        JoinStatisticsPlanner planner = new JoinStatisticsPlanner();
        planner.finishPlanning();
        Assertions.assertFalse(planner.hasDefinitions());
        Assertions.assertTrue(planner.estimate(null).isEmpty());
    }

    @Test
    void constantProjectionDoesNotLoseOtherColumnOriginsOrFail() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getUUID()).thenReturn("uuid0");
        var scope = JoinStatisticsScope.scan(table, Map.of(gate, new Column("gate", gate.getType())), 100);
        var output = new ColumnRefOperator(11, IntegerType.INT, "constant", false);
        var projected = scope.project(Map.of(gate, gate, output, ConstantOperator.createInt(1)));
        Assertions.assertEquals(scope.getColumns().get(gate), projected.getColumns().get(gate));
        Assertions.assertFalse(projected.getColumns().containsKey(output));
        Assertions.assertNull(JoinStatisticsScope.join(scope, projected, JoinOperator.INNER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ, gate, gate)), "Self-join aliases need separate source identities");
    }

    @Test
    void joinEstimateExcludesPredicateThatTheOrdinaryVisitorAppliesAfterwards() {
        Table leftTable = Mockito.mock(Table.class);
        Table rightTable = Mockito.mock(Table.class);
        Mockito.when(leftTable.getUUID()).thenReturn("left");
        Mockito.when(rightTable.getUUID()).thenReturn("right");
        var left = JoinStatisticsScope.scan(leftTable, Map.of(gate, new Column("id", gate.getType())), 10);
        var right = JoinStatisticsScope.scan(rightTable, Map.of(other, new Column("id", other.getType())), 10);
        var join = new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ, gate, other));
        join.setPredicate(new BinaryPredicateOperator(BinaryType.GT, gate, ConstantOperator.createInt(5)));
        ExpressionContext context = Mockito.mock(ExpressionContext.class);
        Mockito.when(context.getOp()).thenReturn(join);
        Mockito.when(context.getChildStatistics(0)).thenReturn(Statistics.builder().setOutputRowCount(10)
                .setJoinStatisticsScope(left).build());
        Mockito.when(context.getChildStatistics(1)).thenReturn(Statistics.builder().setOutputRowCount(10)
                .setJoinStatisticsScope(right).build());
        Assertions.assertTrue(JoinStatisticsScope.derive(context, false).getSources().get("left").predicates().isEmpty());
        Assertions.assertEquals(1, JoinStatisticsScope.derive(context).getSources().get("left").predicates().size());
    }

    private JoinStatisticsData data() {
        var source = new JoinStatisticsData.Source("uuid0", 1, 11, List.of("status", "gate", "type"),
                List.of(VarcharType.VARCHAR, IntegerType.INT, IntegerType.INT),
                List.of(List.of("approved", "0", "0"), List.of("approved", "0", "1"), List.of("pending", "1", "0")),
                new long[] {2, 3, 6}, Map.of(0, List.of(JoinStatisticsEstimateTest.degree(2, 0),
                        JoinStatisticsEstimateTest.degree(0, 3), JoinStatisticsEstimateTest.degree(1, 5))));
        var build = new JoinStatisticsData.Source("uuid1", 2, 2, List.of(), List.of(), List.of(List.of()),
                new long[] {2}, Map.of(0, List.of(JoinStatisticsEstimateTest.degree(0, 2))));
        return new JoinStatisticsData(1, 2, List.of(source, build), List.of(new JoinStatisticsBasis(0, List.of(0, 1),
                List.of(List.of(slice(2, 0), slice(0, 3), slice(1, 5)), List.of(slice(0, 2))))));
    }

    private JoinStatisticsBasis.Slice slice(long... values) {
        return new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(values), new double[0][], false);
    }

    @Test
    void partialPredicateUsesTheUnionOfStoredFullTuples() {
        var predicates = List.<ScalarOperator>of(new BinaryPredicateOperator(BinaryType.EQ, status,
                ConstantOperator.createVarchar("approved")),
                new BinaryPredicateOperator(BinaryType.EQ, gate, ConstantOperator.createInt(0)));
        JoinStatisticsData data = data();
        var selection = JoinStatisticsPlanner.select(data.getSources().get(0),
                new JoinStatisticsScope.Source("uuid0", 5, predicates), columns);
        Assertions.assertNotNull(selection);
        Assertions.assertEquals(5, selection.keyStatistics(data.getSources().get(0), 0).rows());
        Assertions.assertNull(selection.keyStatistics(data.getSources().get(0), 0).degree(),
                "Two slices do not imply that their key sets are disjoint");
        var selections = List.of(selection, new JoinStatisticsEstimate.Selection(new int[] {0}, 0, 2));
        Assertions.assertEquals(6, JoinStatisticsEstimate.estimate(JoinStatisticsEstimateTest.definition(2),
                data, selections, 3, 3, TimeUnit.SECONDS.toNanos(5)).orElseThrow(), 1e-5);
        Assertions.assertEquals(3, JoinStatisticsEstimate.estimate(JoinStatisticsEstimateTest.definition(2),
                data, selections, 3, 1, TimeUnit.SECONDS.toNanos(5)).orElseThrow(), 1e-5);
    }

    @Test
    void fullTupleSuppliesItsOwnNdvAndRfDenominator() {
        var type = new ColumnRefOperator(4, IntegerType.INT, "type", false);
        var origins = new java.util.HashMap<>(columns);
        origins.put(type, new JoinStatisticsScope.ColumnOrigin("uuid0", "type", type.getType()));
        var predicates = List.<ScalarOperator>of(new BinaryPredicateOperator(BinaryType.EQ, status,
                ConstantOperator.createVarchar("approved")),
                new BinaryPredicateOperator(BinaryType.EQ, gate, ConstantOperator.createInt(0)),
                new BinaryPredicateOperator(BinaryType.EQ, type, ConstantOperator.createInt(0)));
        var source = data().getSources().get(0);
        var selection = JoinStatisticsPlanner.select(source, new JoinStatisticsScope.Source("uuid0", 100, predicates), origins);
        var key = selection.keyStatistics(source, 0);
        Assertions.assertEquals(2, key.rows(), "Use the collected slice, not the unrelated basic estimate of 100 rows");
        Assertions.assertEquals(1, key.degree().getDistinctCount());
    }

    @Test
    void additionalColumnKeepsKnownDistributionAsConstraintAndUsesLocalRowEstimate() {
        var predicates = List.<ScalarOperator>of(new BinaryPredicateOperator(BinaryType.EQ, status,
                ConstantOperator.createVarchar("approved")),
                new BinaryPredicateOperator(BinaryType.EQ, other, ConstantOperator.createInt(10)));
        JoinStatisticsData data = data();
        var selection = JoinStatisticsPlanner.select(data.getSources().get(0),
                new JoinStatisticsScope.Source("uuid0", 1, predicates), columns);
        Assertions.assertNotNull(selection);
        var selections = List.of(selection, new JoinStatisticsEstimate.Selection(new int[] {0}, 0, 2));
        double estimate = JoinStatisticsEstimate.estimate(JoinStatisticsEstimateTest.definition(2), data, selections,
                3, 3, TimeUnit.SECONDS.toNanos(5)).orElseThrow();
        Assertions.assertEquals(2, estimate, 1e-5);
    }

    @Test
    void emptySupportedHeadDoesNotInventEmptyTableWhenPredicateMassIsUncovered() {
        var covered = JoinStatisticsCodecTest.fixture().getSources().get(0);
        var uncovered = new JoinStatisticsData.Source(covered.getTableUuid(), covered.getSnapshot(), 11,
                covered.getColumns(), covered.getTypes(), covered.getTuples(), new long[] {5, 5}, covered.getDegrees());
        var predicate = new BinaryPredicateOperator(BinaryType.EQ, status, ConstantOperator.createVarchar("failed"));
        var selection = JoinStatisticsPlanner.select(uncovered,
                new JoinStatisticsScope.Source("transactions-uuid", 0, List.of(predicate)),
                Map.of(status, new JoinStatisticsScope.ColumnOrigin("transactions-uuid", "status", status.getType())));
        Assertions.assertNull(selection);
    }
    @Test
    void extraPredicateCapsRemainInCollectionUnitsDuringGrowth() {
        var stored = data().getSources().get(0);
        var predicate = new BinaryPredicateOperator(BinaryType.EQ, other, ConstantOperator.createInt(1));
        var scan = new JoinStatisticsScope.Source("uuid0", 20, List.of(predicate), "uuid0",
                new JoinStatisticsTableState(110, 3));
        var selection = JoinStatisticsPlanner.select(stored, scan, columns);
        Assertions.assertNotNull(selection);
        Assertions.assertEquals(2, selection.rowLimit(), 1e-6,
                "20 current rows correspond to 2 collected rows; final estimate scales back by ten");
        var partial = new JoinStatisticsScope.Source("uuid0", 50,
                List.of(new BinaryPredicateOperator(BinaryType.EQ, status,
                        ConstantOperator.createVarchar("approved"))), "uuid0", new JoinStatisticsTableState(110, 3));
        Assertions.assertEquals(5, JoinStatisticsPlanner.select(stored, partial, columns).rowLimit(), 1e-6);
    }

}
