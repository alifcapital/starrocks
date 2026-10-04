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
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// ScalarApply2JoinRule turns a correlated scalar subquery into a join on the correlation predicate. We expect it
// to accept only EQ predicates joined by AND, in any nesting, and to refuse everything else with the
// non-equality error instead of building a wrong join.
public class ScalarApplyCorrelationPredicateTest {
    private ConnectContext previous;
    private ConnectContext connection;
    private ColumnRefFactory factory;
    private OptimizerContext context;
    private List<ColumnRefOperator> leftColumns;
    private ColumnRefOperator rightColumn;
    private OptExpression left;
    private OptExpression right;

    @BeforeEach
    public void setUp() {
        previous = ConnectContext.get();
        connection = new ConnectContext();
        connection.setThreadLocalInfo();
        factory = new ColumnRefFactory();
        context = OptimizerFactory.mockContext(connection, factory);
        leftColumns = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            leftColumns.add(factory.create("l" + i, IntegerType.INT, true));
        }
        rightColumn = factory.create("r0", IntegerType.INT, true);
        left = OptExpression.create(new LogicalValuesOperator(leftColumns));
        right = OptExpression.create(new LogicalValuesOperator(List.of(rightColumn)));
        left.deriveLogicalPropertyItself();
        right.deriveLogicalPropertyItself();
    }

    @AfterEach
    public void tearDown() {
        if (previous == null) {
            ConnectContext.remove();
        } else {
            previous.setThreadLocalInfo();
        }
    }

    private LogicalApplyOperator.Builder apply(ColumnRefOperator output, ScalarOperator correlation) {
        return LogicalApplyOperator.builder().setOutput(output).setSubqueryOperator(rightColumn)
                .setCorrelationColumnRefs(List.of(leftColumns.get(0))).setCorrelationConjuncts(correlation);
    }

    private ScalarOperator eq(ScalarOperator l, ScalarOperator r) {
        return new BinaryPredicateOperator(BinaryType.EQ, l, r);
    }

    private ScalarOperator and(ScalarOperator l, ScalarOperator r) {
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, l, r);
    }

    private ScalarOperator or(ScalarOperator l, ScalarOperator r) {
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, l, r);
    }

    @Test
    public void correlationAdmitsOnlyConjunctionsOfEq() {
        ColumnRefOperator output = factory.create("out", IntegerType.INT, true);
        ScalarOperator eq0 = eq(leftColumns.get(0), rightColumn);
        ScalarOperator eq1 = eq(leftColumns.get(1), rightColumn);
        ScalarOperator eq2 = eq(leftColumns.get(2), rightColumn);
        ScalarOperator gt = new BinaryPredicateOperator(BinaryType.GT, leftColumns.get(1), rightColumn);
        ScalarOperator eqForNull = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, leftColumns.get(1), rightColumn);
        ScalarOperator notEq = CompoundPredicateOperator.not(eq1);

        List<ScalarOperator> accepted = List.of(
                eq0,
                and(eq0, eq1),
                and(and(eq0, eq1), eq2),
                and(eq0, and(eq1, eq2)));
        for (ScalarOperator correlation : accepted) {
            LogicalApplyOperator op = apply(output, correlation).setNeedCheckMaxRows(false).build();
            new ScalarApply2JoinRule().transform(OptExpression.create(op, left, right), context);
        }

        List<ScalarOperator> rejected = List.of(
                gt,
                eqForNull,
                notEq,
                or(eq0, eq1),
                and(eq0, gt),
                and(gt, eq0),
                and(eq0, and(eq1, eqForNull)),
                and(and(eq0, eq1), or(eq1, eq2)),
                and(eq0, ConstantOperator.createBoolean(true)));
        for (ScalarOperator correlation : rejected) {
            LogicalApplyOperator op = apply(output, correlation).setNeedCheckMaxRows(false).build();
            SemanticException e = assertThrows(SemanticException.class, () -> new ScalarApply2JoinRule()
                    .transform(OptExpression.create(op, left, right), context));
            assertTrue(e.getMessage().contains(SubqueryUtils.EXIST_NON_EQ_PREDICATE));
        }
    }
}
