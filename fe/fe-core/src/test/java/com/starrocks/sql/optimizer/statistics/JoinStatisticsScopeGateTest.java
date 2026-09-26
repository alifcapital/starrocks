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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.statistic.JoinStatisticsMeta;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class JoinStatisticsScopeGateTest {
    private static JoinStatisticsPlanner planner() throws Exception {
        var planner = new JoinStatisticsPlanner();
        var field = JoinStatisticsPlanner.class.getDeclaredField("definitions");
        field.setAccessible(true);
        field.set(planner, List.of(new JoinStatisticsMeta(1, JoinStatisticsEstimateTest.definition(2),
                1, 1, 1, "test", 1)));
        return planner;
    }

    private static Table table(boolean iceberg, String uuid) {
        Table table = iceberg ? Mockito.mock(IcebergTable.class) : Mockito.mock(OlapTable.class);
        Mockito.when(table.getUUID()).thenReturn(uuid);
        return table;
    }

    private static Map<ColumnRefOperator, Column> register(ColumnRefFactory factory, Table table, int width) {
        Map<ColumnRefOperator, Column> refs = new HashMap<>();
        for (int i = 0; i < width; i++) {
            var ref = factory.create("c" + i, IntegerType.BIGINT, false);
            var column = new Column(ref.getName(), ref.getType());
            factory.updateColumnRefToColumns(ref, column, table);
            refs.put(ref, column);
        }
        return refs;
    }

    private static ExpressionContext scan(Table table, Map<ColumnRefOperator, Column> refs) {
        Operator op;
        if (table instanceof IcebergTable) {
            var iceberg = Mockito.mock(LogicalIcebergScanOperator.class);
            Mockito.when(iceberg.getTable()).thenReturn(table);
            Mockito.when(iceberg.getMORParam()).thenReturn(IcebergMORParams.EMPTY);
            Mockito.when(iceberg.getTvrVersionRange()).thenReturn(TvrTableSnapshot.of(1L));
            Mockito.when(iceberg.getColRefToColumnMetaMap()).thenReturn(refs);
            op = iceberg;
        } else {
            var nativeScan = Mockito.mock(LogicalOlapScanOperator.class);
            Mockito.when(nativeScan.getTable()).thenReturn(table);
            Mockito.when(nativeScan.getColRefToColumnMetaMap()).thenReturn(refs);
            op = nativeScan;
        }
        Mockito.when(op.getLimit()).thenReturn(Operator.DEFAULT_LIMIT);
        var expression = OptExpression.create(op);
        expression.setStatistics(Statistics.builder().setOutputRowCount(100).build());
        return new ExpressionContext(expression);
    }

    private static void calculate(ExpressionContext context, ColumnRefFactory factory, JoinStatisticsPlanner planner) {
        var optimizer = Mockito.mock(OptimizerContext.class);
        Mockito.when(optimizer.getJoinStatisticsPlanner()).thenReturn(planner);
        new StatisticsCalculator(context, factory, optimizer).estimatorStats();
    }

    @Test
    void unrelatedWideNativeAndIcebergQueriesDoNotDeriveOrCopyStatistics() throws Exception {
        for (boolean iceberg : List.of(false, true)) {
            var planner = planner();
            var factory = new ColumnRefFactory();
            var table = table(iceberg, "unrelated");
            var context = scan(table, register(factory, table, 512));
            var original = context.getStatistics();
            // accept() is inert on this mocked scan; the ordinary estimator's result is already installed.
            for (int i = 0; i < 100; i++) {
                calculate(context, factory, planner);
                Assertions.assertSame(original, context.getStatistics());
            }
            Mockito.verify(context.getOp(), Mockito.never()).getLimit();
            Mockito.verify(table, Mockito.times(1)).getUUID();
        }
    }

    @Test
    void relevantQueryRetainsUncoveredSources() throws Exception {
        for (boolean iceberg : List.of(false, true)) {
            var planner = planner();
            var factory = new ColumnRefFactory();
            List<ExpressionContext> scans = new ArrayList<>();
            List<ColumnRefOperator> columns = new ArrayList<>();
            for (String uuid : List.of("uuid0", "uuid1", "uncovered")) {
                var table = table(iceberg, uuid);
                var refs = register(factory, table, 1);
                columns.add(refs.keySet().iterator().next());
                scans.add(scan(table, refs));
            }
            // Start with C, which is not in the AB definition. It must not lose provenance.
            for (int i : new int[] {2, 0, 1}) {
                calculate(scans.get(i), factory, planner);
                var scope = scans.get(i).getStatistics().getJoinStatisticsScope();
                Assertions.assertNotNull(scope);
                Assertions.assertEquals(1, scope.getSources().size());
            }
            var ab = join(scans.get(0), scans.get(1), columns.get(0), columns.get(1), planner, factory);
            var abc = join(ab, scans.get(2), columns.get(1), columns.get(2), planner, factory);
            Assertions.assertEquals(Set.of("uuid0", "uuid1", "uncovered"),
                    abc.getStatistics().getJoinStatisticsScope().getSources().keySet());
        }
    }

    private static ExpressionContext join(ExpressionContext left, ExpressionContext right,
                                          ColumnRefOperator a, ColumnRefOperator b,
                                          JoinStatisticsPlanner planner, ColumnRefFactory factory) {
        var expression = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ, a, b)), left.getOptExpression(), right.getOptExpression());
        // ExpressionContext statistics are separate from the OptExpression until installed by the caller.
        left.getOptExpression().setStatistics(left.getStatistics());
        right.getOptExpression().setStatistics(right.getStatistics());
        expression.setStatistics(Statistics.builder().setOutputRowCount(100).build());
        var context = new ExpressionContext(expression);
        var scope = planner.deriveScope(context, factory, false);
        Assertions.assertNotNull(scope);
        context.setStatistics(Statistics.buildFrom(context.getStatistics())
                .setJoinStatisticsScope(scope).setJoinStatisticsPlanner(planner).build());
        return context;
    }

    @Test
    void rewritingTableMappingInvalidatesNegativeDecisionEvenWithoutAddingColumns() throws Exception {
        var planner = planner();
        var factory = new ColumnRefFactory();
        var unrelated = table(false, "unrelated");
        var refs = register(factory, unrelated, 1);
        Assertions.assertNull(planner.deriveScope(scan(unrelated, refs), factory, true));
        var relevant = table(true, "uuid0");
        refs.forEach((ref, column) -> factory.updateColumnRefToColumns(ref, column, relevant));
        Assertions.assertNotNull(planner.deriveScope(scan(relevant, refs), factory, true));
        var another = table(false, "uuid1");
        Assertions.assertNotNull(planner.deriveScope(scan(another, register(factory, another, 1)), factory, true));
    }

    @Test
    void provenanceFailuresInBothPathsDisableOnlyOptionalStatistics() throws Exception {
        for (boolean postProcessing : List.of(false, true)) {
            var planner = planner();
            var factory = new ColumnRefFactory();
            var table = table(false, "uuid0");
            var context = scan(table, register(factory, table, 1));
            var valid = planner.deriveScope(context, factory, true);
            Assertions.assertNotNull(valid);
            context.setStatistics(Statistics.buildFrom(context.getStatistics())
                    .setJoinStatisticsScope(valid).setJoinStatisticsPlanner(planner).build());
            Mockito.when(context.getOp().getLimit()).thenThrow(new IllegalArgumentException("broken provenance"));
            Assertions.assertNull(planner.deriveScope(context, factory, postProcessing));
            Assertions.assertFalse(planner.hasDefinitions());
            calculate(context, factory, planner);
            Assertions.assertEquals(100, context.getStatistics().getOutputRowCount());
            Assertions.assertNull(context.getStatistics().getJoinStatisticsScope());
            Assertions.assertNull(context.getStatistics().getJoinStatisticsPlanner());
            Assertions.assertTrue(planner.estimate(valid).isEmpty());
        }
    }

    @Test
    void ordinaryEstimatorFailureIsNotSwallowed() throws Exception {
        var planner = planner();
        var factory = new ColumnRefFactory();
        var table = table(false, "uuid0");
        var context = scan(table, register(factory, table, 1));
        Mockito.doThrow(new IllegalStateException("ordinary estimation"))
                .when(context.getOp()).accept(Mockito.any(), Mockito.any());
        Assertions.assertThrows(IllegalStateException.class, () -> calculate(context, factory, planner));
        Assertions.assertTrue(planner.hasDefinitions());
    }

    @Test
    void operatorWithoutStatisticsIsLeftUntouched() throws Exception {
        var planner = planner();
        var context = new ExpressionContext(OptExpression.create(Mockito.mock(Operator.class)));
        calculate(context, new ColumnRefFactory(), planner);
        Assertions.assertNull(context.getStatistics());
        Assertions.assertTrue(planner.hasDefinitions());
        Mockito.verify(context.getOp(), Mockito.never()).getLimit();
    }

    @Test
    void missingFactoryMetadataRemainsConservativeAndFatalErrorsAreNotSwallowed() throws Exception {
        var planner = planner();
        var factory = new ColumnRefFactory();
        var table = table(false, "uuid0");
        var context = scan(table, register(factory, table, 1));
        Assertions.assertNotNull(planner.deriveScope(context, null, true));
        Assertions.assertNotNull(planner.deriveScope(context, new ColumnRefFactory(), true));
        Mockito.when(context.getOp().getLimit()).thenThrow(new AssertionError("fatal"));
        Assertions.assertThrows(AssertionError.class, () -> planner.deriveScope(context, factory, true));
    }
}
