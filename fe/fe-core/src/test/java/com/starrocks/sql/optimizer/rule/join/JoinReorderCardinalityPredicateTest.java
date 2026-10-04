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

package com.starrocks.sql.optimizer.rule.join;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// JoinReorderCardinalityPreserving reads unique keys only from equalities between two column refs.
// We expect any other ON predicate, such as an equality over casts or a range predicate, to be skipped
// by that check and kept by the reorder, so ReorderJoinRule must return a join with every input conjunct.
public class JoinReorderCardinalityPredicateTest {
    private ConnectContext previousContext;

    @BeforeEach
    public void saveContext() {
        previousContext = ConnectContext.get();
    }

    @AfterEach
    public void restoreContext() {
        if (previousContext == null) {
            ConnectContext.remove();
        } else {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    public void castEqualityIsKept() {
        Fixture fixture = new Fixture();
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(IntegerType.BIGINT, fixture.leftRef),
                new CastOperator(IntegerType.BIGINT, fixture.rightRef));
        fixture.rewrite(predicate);
    }

    @Test
    public void columnToCastEqualityIsKept() {
        Fixture fixture = new Fixture();
        fixture.rewrite(new BinaryPredicateOperator(BinaryType.EQ,
                fixture.leftRef, new CastOperator(IntegerType.INT, fixture.rightRef)));
    }

    @Test
    public void castToColumnEqualityIsKept() {
        Fixture fixture = new Fixture();
        fixture.rewrite(new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(IntegerType.INT, fixture.leftRef), fixture.rightRef));
    }

    @Test
    public void castRangePredicateIsKept() {
        Fixture fixture = new Fixture();
        fixture.rewrite(new BinaryPredicateOperator(BinaryType.LT,
                new CastOperator(IntegerType.BIGINT, fixture.leftRef),
                new CastOperator(IntegerType.BIGINT, fixture.rightRef)));
    }

    private static class Fixture {
        private final ColumnRefFactory factory = new ColumnRefFactory();
        private final OptimizerContext context = OptimizerFactory.mockContext(factory);
        private final ColumnRefOperator leftRef = factory.create("left_k", IntegerType.INT, false);
        private final ColumnRefOperator rightRef = factory.create("right_k", IntegerType.INT, false);
        private final Column key = new Column("k", IntegerType.INT);
        private final IcebergTable table = mock(IcebergTable.class);
        private final OptExpression left;
        private final OptExpression right;

        Fixture() {
            context.getSessionVariable().setEnableUKFKOpt(false);
            context.getSessionVariable().setEnableUKFKJoinReorder(false);
            context.getConnectContext().setThreadLocalInfo();
            when(table.getId()).thenReturn(37L);
            when(table.getName()).thenReturn("unique_keys");
            when(table.getPartitionColumns()).thenReturn(List.of());
            when(table.hasUniqueConstraints()).thenReturn(true);
            when(table.hasForeignKeyConstraints()).thenReturn(false);
            when(table.getUniqueConstraints()).thenReturn(List.of(
                    new UniqueConstraint(null, null, "unique_keys", List.of(key.getColumnId()))));
            when(table.getColumn(key.getColumnId())).thenReturn(key);
            left = scan(leftRef);
            right = scan(rightRef);
        }

        private OptExpression scan(ColumnRefOperator ref) {
            OptExpression scan = OptExpression.create(new LogicalIcebergScanOperator(table,
                    Map.of(ref, key), Map.of(key, ref), -1, null));
            scan.deriveLogicalPropertyItself();
            scan.setStatistics(Statistics.builder().setOutputRowCount(10)
                    .addColumnStatistic(ref, new ColumnStatistic(1, 10, 0, 4, 10)).build());
            return scan;
        }

        private void rewrite(ScalarOperator predicate) {
            OptExpression input = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, predicate),
                    left, right);
            input.deriveLogicalPropertyItself();
            MultiJoinNode flattened = MultiJoinNode.toMultiJoinNode(input);
            assertEquals(List.of(left, right), List.copyOf(flattened.getAtoms()));
            assertEquals(Utils.extractConjuncts(predicate), flattened.getPredicates());
            assertTrue(flattened.checkDependsPredicate());
            assertTrue(flattened.getExpressionMap().isEmpty());

            OptExpression result = new ReorderJoinRule().rewrite(input, context);
            assertNotSame(input, result);
            LogicalJoinOperator join = result.getOp().cast();
            assertEquals(JoinOperator.INNER_JOIN, join.getJoinType());
            List<ScalarOperator> originals = Utils.extractConjuncts(predicate);
            List<ScalarOperator> actual = Utils.extractConjuncts(join.getOnPredicate());
            assertEquals(originals.size(), actual.size());
            for (ScalarOperator original : originals) {
                assertTrue(actual.stream().anyMatch(value -> value == original));
            }
            assertEquals(new ColumnRefSet(List.of(leftRef, rightRef)), result.getOutputColumns());
            assertEquals(Set.of(left, right), Set.copyOf(result.getInputs()));
            assertSame(predicate, ((LogicalJoinOperator) input.getOp()).getOnPredicate());
            assertEquals(Map.of(leftRef, key),
                    ((LogicalIcebergScanOperator) left.getOp()).getColRefToColumnMetaMap());
            assertEquals(Map.of(rightRef, key),
                    ((LogicalIcebergScanOperator) right.getOp()).getColRefToColumnMetaMap());
        }
    }
}
