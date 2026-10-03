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

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.tree.PreAggregateTurnOnRule.PreAggregationContext;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Below a join, PreAggregateTurnOnRule can keep pre-aggregation only for an equality join between the two inputs.
 * We check which joins clear the aggregation state and which groupings go down to the side with the aggregation.
 */
class PreAggregateTurnOnJoinTest {
    private final ColumnRefFactory factory = new ColumnRefFactory();
    private final ColumnRefOperator a1 = factory.create("a1", IntegerType.INT, true);
    private final ColumnRefOperator a2 = factory.create("a2", IntegerType.INT, true);
    private final ColumnRefOperator b1 = factory.create("b1", IntegerType.INT, true);
    private final ColumnRefOperator b2 = factory.create("b2", IntegerType.INT, true);

    private OptExpression leaf(ColumnRefOperator... columns) {
        List<ColumnRefOperator> refs = List.of(columns);
        OptExpression leaf = OptExpression.create(new PhysicalValuesOperator(refs, List.of(), -1, null, null));
        leaf.setLogicalProperty(new LogicalProperty(new ColumnRefSet(refs)));
        return leaf;
    }

    private OptExpression join(JoinOperator type, ScalarOperator onPredicate, ScalarOperator predicate) {
        return OptExpression.create(new PhysicalHashJoinOperator(type, onPredicate, "", -1, predicate, null, null,
                null, null), leaf(a1, a2), leaf(b1, b2));
    }

    private static ScalarOperator add(ScalarOperator left, ScalarOperator right) {
        return new CallOperator("add", IntegerType.INT, List.of(left, right));
    }

    private static CallOperator sum(ScalarOperator argument) {
        return new CallOperator("sum", IntegerType.INT, List.of(argument));
    }

    private PreAggregationContext context(ScalarOperator aggregationInput) {
        PreAggregationContext context = new PreAggregationContext();
        context.aggregations = new ArrayList<>(List.of(sum(aggregationInput)));
        context.groupings = new ArrayList<>(List.of(a1, add(a1, b1), b1, b2));
        return context;
    }

    // The visitor is a private class of the rule, so the test reaches it by reflection.
    private static void visit(OptExpression join, PreAggregationContext context) throws Exception {
        Class<?> visitorClass = Class.forName(PreAggregateTurnOnRule.class.getName() + "$PreAggregateVisitor");
        Constructor<?> constructor = visitorClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        Method method = visitorClass.getDeclaredMethod("visitPhysicalJoin", OptExpression.class,
                PreAggregationContext.class);
        method.setAccessible(true);
        method.invoke(constructor.newInstance(), join, context);
    }

    @Test
    void equalityJoinKeepsOnlyTheGroupingsOfTheSideThatOwnsTheAggregation() throws Exception {
        ScalarOperator on = BinaryPredicateOperator.eq(a1, b1);
        ScalarOperator filter = BinaryPredicateOperator.gt(b2, ConstantOperator.createInt(0));

        PreAggregationContext left = context(a2);
        visit(join(JoinOperator.INNER_JOIN, on, filter), left);
        assertFalse(left.notPreAggregationJoin);
        // sum(a2) uses the left input, so only the groupings that use left columns go down to the left scan.
        assertEquals(List.of(a1, add(a1, b1)), left.groupings);
        assertEquals(1, left.aggregations.size());
        assertEquals(List.of(on, filter), left.joinPredicates);

        PreAggregationContext right = context(b2);
        visit(join(JoinOperator.INNER_JOIN, on, null), right);
        assertFalse(right.notPreAggregationJoin);
        assertEquals(List.of(add(a1, b1), b1, b2), right.groupings);
        assertEquals(List.of(on), right.joinPredicates);
    }

    @Test
    void joinsWithoutAnEqualityBetweenTheInputsClearTheAggregationState() throws Exception {
        ScalarOperator equality = BinaryPredicateOperator.eq(a1, b1);
        ScalarOperator[] onPredicates = {
                null,
                BinaryPredicateOperator.lt(a1, b1),
                // Both operands come from the left input.
                BinaryPredicateOperator.eq(a1, a2),
                // Only an OR of equalities is not an equality conjunct.
                CompoundPredicateOperator.or(equality, BinaryPredicateOperator.eq(a2, b2)),
        };
        for (ScalarOperator on : onPredicates) {
            PreAggregationContext context = context(a2);
            visit(join(JoinOperator.INNER_JOIN, on, null), context);
            assertTrue(context.notPreAggregationJoin, String.valueOf(on));
            assertTrue(context.groupings.isEmpty());
            assertTrue(context.aggregations.isEmpty());
            assertTrue(context.joinPredicates.isEmpty());
        }

        PreAggregationContext cross = context(a2);
        visit(join(JoinOperator.CROSS_JOIN, equality, null), cross);
        assertTrue(cross.notPreAggregationJoin);
        assertTrue(cross.groupings.isEmpty());
        assertTrue(cross.aggregations.isEmpty());

        // An equality among other conjuncts is enough.
        PreAggregationContext conjunction = context(a2);
        visit(join(JoinOperator.INNER_JOIN, CompoundPredicateOperator.and(BinaryPredicateOperator.lt(a1, b1),
                equality), null), conjunction);
        assertFalse(conjunction.notPreAggregationJoin);
        assertEquals(List.of(a1, add(a1, b1)), conjunction.groupings);
    }
}
