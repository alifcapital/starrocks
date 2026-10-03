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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeriveRangeJoinPredicateRuleTest {
    private ConnectContext previous;
    private ColumnRefFactory factory;
    private OptimizerContext context;
    private ColumnRefOperator lower;
    private ColumnRefOperator upper;
    private ColumnRefOperator key;
    private OptExpression left;
    private OptExpression right;
    private OptExpression input;

    @BeforeEach
    public void setUp() {
        previous = ConnectContext.get();
        ConnectContext connection = new ConnectContext();
        connection.getSessionVariable().setCboDeriveRangeJoinPredicate(true);
        connection.setThreadLocalInfo();
        factory = new ColumnRefFactory();
        context = OptimizerFactory.mockContext(connection, factory);

        // The interval columns come from the left child and the bounded key comes from the right child:
        // lower < key AND key < upper.
        lower = factory.create("lower", IntegerType.INT, false);
        upper = factory.create("upper", IntegerType.INT, false);
        key = factory.create("right_key", IntegerType.INT, false);
        left = OptExpression.create(new LogicalValuesOperator(List.of(lower, upper),
                List.of(List.of(ConstantOperator.createInt(0), ConstantOperator.createInt(10)))));
        right = OptExpression.create(new LogicalValuesOperator(List.of(key),
                List.of(List.of(ConstantOperator.createInt(5)))));
        left.deriveLogicalPropertyItself();
        right.deriveLogicalPropertyItself();
        ScalarOperator on = Utils.compoundAnd(new BinaryPredicateOperator(BinaryType.LT, lower, key),
                new BinaryPredicateOperator(BinaryType.LT, key, upper));
        input = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, on), left, right);
        input.deriveLogicalPropertyItself();
    }

    @AfterEach
    public void tearDown() {
        if (previous == null) {
            ConnectContext.remove();
        } else {
            previous.setThreadLocalInfo();
        }
    }

    @Test
    public void rightChildAnchorUsesRightChildStatistics() {
        // The anchor statistic exists only on the right child. We expect the rule to read it from there,
        // not to fail on the left child, and to bound the left interval columns with the key range.
        left.setStatistics(Statistics.builder().setOutputRowCount(1)
                .addColumnStatistic(lower, constantStats(0))
                .addColumnStatistic(upper, constantStats(10)).build());
        right.setStatistics(Statistics.builder().setOutputRowCount(1)
                .addColumnStatistic(key, constantStats(5)).build());

        List<OptExpression> result = transform();
        assertEquals(1, result.size());
        List<ScalarOperator> bounds = Utils.extractConjuncts(result.get(0).inputAt(0).getOp().getPredicate());
        assertEquals(2, bounds.size());
        assertBound(bounds.get(0), BinaryType.LE, lower);
        assertBound(bounds.get(1), BinaryType.GE, upper);
    }

    @Test
    public void anchorWithoutStatisticsOnBothSidesKeepsTheJoin() {
        // No child has a statistic for the anchor. We expect the rule to skip it and return the input unchanged.
        left.setStatistics(Statistics.builder().setOutputRowCount(1)
                .addColumnStatistic(lower, constantStats(0))
                .addColumnStatistic(upper, constantStats(10)).build());
        right.setStatistics(Statistics.builder().setOutputRowCount(1).build());

        List<OptExpression> result = transform();
        assertEquals(1, result.size());
        assertSame(input, result.get(0));
        assertNull(left.getOp().getPredicate());
        assertNull(right.getOp().getPredicate());
    }

    private List<OptExpression> transform() {
        DeriveRangeJoinPredicateRule rule = new DeriveRangeJoinPredicateRule();
        assertTrue(rule.check(input, context));
        return assertDoesNotThrow(() -> rule.transform(input, context));
    }

    private static ColumnStatistic constantStats(int value) {
        return ColumnStatistic.builder().setMinValue(value).setMaxValue(value).setNullsFraction(0)
                .setAverageRowSize(4).setDistinctValuesCount(1)
                .setMinString(Integer.toString(value)).setMaxString(Integer.toString(value)).build();
    }

    private static void assertBound(ScalarOperator scalar, BinaryType type, ColumnRefOperator column) {
        BinaryPredicateOperator bound = (BinaryPredicateOperator) scalar;
        assertEquals(type, bound.getBinaryType());
        assertEquals(column, bound.getChild(0));
        CastOperator constant = (CastOperator) bound.getChild(1);
        assertEquals(IntegerType.INT, constant.getType());
        assertEquals("5", ((ConstantOperator) constant.getChild(0)).getVarchar());
    }
}
