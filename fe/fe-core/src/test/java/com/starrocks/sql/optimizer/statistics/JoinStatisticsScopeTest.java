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
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rule.implementation.UnionImplementationRule;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

class JoinStatisticsScopeTest {
    private final ColumnRefOperator input = new ColumnRefOperator(1, IntegerType.INT, "id", true);
    private final ColumnRefOperator output = new ColumnRefOperator(2, IntegerType.INT, "id", true);

    private OptExpression scan(IcebergMORParams params) {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getUUID()).thenReturn("original-table");
        LogicalIcebergScanOperator scan = Mockito.mock(LogicalIcebergScanOperator.class);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getLimit()).thenReturn(Operator.DEFAULT_LIMIT);
        Mockito.when(scan.getMORParam()).thenReturn(params);
        Mockito.when(scan.getTvrVersionRange()).thenReturn(TvrTableSnapshot.of(42L));
        Mockito.when(scan.getColRefToColumnMetaMap()).thenReturn(Map.of(input, new Column("id", IntegerType.INT)));
        Mockito.when(scan.getPredicate()).thenReturn(new BinaryPredicateOperator(BinaryType.GE, input,
                ConstantOperator.createInt(0)));
        OptExpression expression = OptExpression.create(scan);
        expression.setStatistics(Statistics.builder().setOutputRowCount(10).build());
        return expression;
    }

    private OptExpression union(OptExpression first, boolean fromRewrite) {
        var operator = new LogicalUnionOperator(List.of(output), List.of(List.of(input), List.of(input)),
                true, fromRewrite);
        OptExpression expression = OptExpression.create(operator, first, scan(IcebergMORParams.DATA_FILE_WITH_EQ_DELETE));
        expression.setStatistics(Statistics.builder().setOutputRowCount(80).build());
        return expression;
    }

    @Test
    void nativeScanRestoresPrunedPredicatesAndRejectsRestrictedOrSampledInputs() {
        var table = Mockito.mock(com.starrocks.catalog.OlapTable.class);
        Mockito.when(table.getUUID()).thenReturn("native-table");
        Mockito.when(table.getBaseIndexMetaId()).thenReturn(1L);
        var scan = new com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator(table,
                Map.of(input, new Column("id", IntegerType.INT)), Map.of(), null, Operator.DEFAULT_LIMIT, null);
        var partitionPredicate = new BinaryPredicateOperator(BinaryType.GE, input, ConstantOperator.createInt(1));
        scan = com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator.builder().withOperator(scan)
                .setPrunedPartitionPredicates(List.of(partitionPredicate)).build();
        var expression = OptExpression.create(scan);
        expression.setStatistics(Statistics.builder().setOutputRowCount(10).build());
        var scope = JoinStatisticsScope.derive(new ExpressionContext(expression));
        Assertions.assertEquals(List.of(partitionPredicate), scope.getSources().get("native-table").predicates());
        var restricted = com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator.builder().withOperator(scan)
                .setHintsTabletIds(List.of(123L)).build();
        var restrictedExpression = OptExpression.create(restricted);
        restrictedExpression.setStatistics(expression.getStatistics());
        Assertions.assertNull(JoinStatisticsScope.derive(new ExpressionContext(restrictedExpression)));
        var historical = com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator.builder().withOperator(scan)
                .setGtid(123L).build();
        var historicalExpression = OptExpression.create(historical);
        historicalExpression.setStatistics(expression.getStatistics());
        Assertions.assertNull(JoinStatisticsScope.derive(new ExpressionContext(historicalExpression)));
    }

    @Test
    void restoresOnlyTheCompleteEqualityDeleteResultWithPushedPredicates() {
        var partial = scan(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE);
        Assertions.assertNull(JoinStatisticsScope.derive(new ExpressionContext(partial)));
        Assertions.assertNull(JoinStatisticsScope.derive(new ExpressionContext(scan(IcebergMORParams.DATA_FILE_WITH_EQ_DELETE))));
        var predicate = new BinaryPredicateOperator(BinaryType.LT, input, ConstantOperator.createInt(9));
        var filtered = OptExpression.create(new LogicalFilterOperator(predicate), partial);
        var expression = union(filtered, true);
        var scope = JoinStatisticsScope.derive(new ExpressionContext(expression));
        Assertions.assertNotNull(scope);
        Assertions.assertEquals("original-table", scope.getColumns().get(output).tableUuid());
        Assertions.assertEquals(80, scope.getSources().get("original-table").estimatedRows(),
                "Use the complete result, not ten rows in the data-file subset");
        Assertions.assertEquals(2, scope.getSources().get("original-table").predicates().size());
        Assertions.assertTrue(scope.getSources().get("original-table").predicates().contains(predicate));

        OptExpression physical = new UnionImplementationRule().transform(expression, null).get(0);
        physical.setStatistics(expression.getStatistics());
        Assertions.assertTrue(((PhysicalUnionOperator) physical.getOp()).isFromIcebergEqualityDeleteRewrite());
        Assertions.assertEquals(scope.getSources(), JoinStatisticsScope.derive(new ExpressionContext(physical)).getSources());
    }

    @Test
    void ordinaryUnionAndChangedBranchMeaningCannotAcquireWholeTableProvenance() {
        var partial = scan(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE);
        Assertions.assertNull(JoinStatisticsScope.derive(new ExpressionContext(union(partial, false))));
        Assertions.assertNull(JoinStatisticsScope.derive(new ExpressionContext(union(scan(IcebergMORParams.EMPTY), true))));
        Mockito.when(partial.getOp().hasLimit()).thenReturn(true);
        Assertions.assertNull(JoinStatisticsScope.derive(new ExpressionContext(union(partial, true))));
        var changed = OptExpression.create(Mockito.mock(Operator.class), scan(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE));
        Assertions.assertNull(JoinStatisticsScope.derive(new ExpressionContext(union(changed, true))));
    }
}
