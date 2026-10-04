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

package com.starrocks.sql.plan;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rule.transformation.ScalarApply2AnalyticRule;
import com.starrocks.type.IntegerType;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

// ScalarApply2AnalyticRule clones the subquery expressions into the window and replaces inner columns by
// their outer peers. A CASE keeps its clauses in one child list, and the layout differs for simple and searched
// CASE and with or without ELSE, so we check every layout: each clone must keep the clause order and must not
// share mutable children with the source or with another clone.
public class ScalarApplyCaseCloneTest extends PlanTestBase {
    @Test
    public void testSearchedCaseWithElse() throws Exception {
        checkClone(false, true);
    }

    @Test
    public void testSearchedCaseWithoutElse() throws Exception {
        checkClone(false, false);
    }

    @Test
    public void testSimpleCaseWithElse() throws Exception {
        checkClone(true, true);
    }

    @Test
    public void testSimpleCaseWithoutElse() throws Exception {
        checkClone(true, false);
    }

    private static void checkClone(boolean simple, boolean hasElse) throws Exception {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator source = factory.create("source", IntegerType.INT, false);
        ColumnRefOperator outer = factory.create("outer", IntegerType.INT, false);
        OlapTable table = new OlapTable();
        factory.updateColumnRefToColumns(source, new Column("c", IntegerType.INT, false), table);
        factory.updateColumnRefToColumns(outer, new Column("c", IntegerType.INT, false), table);
        Map<ColumnRefOperator, ColumnRefOperator> peers = new HashMap<>();
        ScalarOperatorVisitor<ScalarOperator, Void> shuttle = shuttle(factory, peers, outer);
        ConstantOperator one = ConstantOperator.createInt(1);
        ConstantOperator two = ConstantOperator.createInt(2);
        ScalarOperator firstWhen = simple ? one : new BinaryPredicateOperator(BinaryType.GT, source, one);
        ScalarOperator secondWhen = simple ? two : new BinaryPredicateOperator(BinaryType.LT, source, two);
        CaseWhenOperator nested = new CaseWhenOperator(IntegerType.INT, null, two,
                List.of(new BinaryPredicateOperator(BinaryType.EQ, source, one), source));
        CallOperator firstThen = new CallOperator("add", IntegerType.INT, List.of(source, nested));
        CallOperator secondThen = new CallOperator("subtract", IntegerType.INT, List.of(source, two));
        CallOperator elseClause = new CallOperator("add", IntegerType.INT, List.of(source, one));
        CaseWhenOperator input = new CaseWhenOperator(IntegerType.INT, simple ? source : null,
                hasElse ? elseClause : null, List.of(firstWhen, firstThen, secondWhen, secondThen));
        CaseWhenOperator first = (CaseWhenOperator) input.accept(shuttle, null);
        CaseWhenOperator second = (CaseWhenOperator) input.accept(shuttle, null);

        assertCloneTree(input, first, source, outer);
        assertCloneTree(input, second, source, outer);
        assertEquals(Map.of(source, outer), peers);
        assertNotSame(first.getThenClause(0), second.getThenClause(0));
        assertNotSame(first.getThenClause(0).getChild(1), second.getThenClause(0).getChild(1));
        first.getThenClause(0).setChild(0, ConstantOperator.createInt(99));
        assertSame(source, input.getThenClause(0).getChild(0));
        assertSame(outer, second.getThenClause(0).getChild(0));
        CaseWhenOperator firstNested = (CaseWhenOperator) first.getThenClause(0).getChild(1);
        firstNested.setThenClause(0, ConstantOperator.createInt(77));
        assertSame(source, nested.getThenClause(0));
        assertSame(outer, ((CaseWhenOperator) second.getThenClause(0).getChild(1)).getThenClause(0));
        assertSame(outer, first.getThenClause(1).getChild(0));
        assertSame(two, first.getThenClause(1).getChild(1));
    }

    private static void assertCloneTree(ScalarOperator input, ScalarOperator cloned,
                                       ColumnRefOperator source, ColumnRefOperator outer) {
        if (input == source) {
            assertSame(outer, cloned);
            return;
        }
        if (input instanceof ConstantOperator) {
            assertSame(input, cloned);
            return;
        }
        assertNotSame(input, cloned);
        assertEquals(input.getClass(), cloned.getClass());
        assertEquals(input.getType(), cloned.getType());
        if (input instanceof CaseWhenOperator expected) {
            CaseWhenOperator actual = (CaseWhenOperator) cloned;
            assertEquals(expected.hasCase(), actual.hasCase());
            assertEquals(expected.hasElse(), actual.hasElse());
            assertEquals(expected.getWhenClauseSize(), actual.getWhenClauseSize());
        }
        assertEquals(input.getChildren().size(), cloned.getChildren().size());
        for (int i = 0; i < input.getChildren().size(); i++) {
            assertCloneTree(input.getChild(i), cloned.getChild(i), source, outer);
        }
    }

    @SuppressWarnings("unchecked")
    private static ScalarOperatorVisitor<ScalarOperator, Void> shuttle(ColumnRefFactory factory,
            Map<ColumnRefOperator, ColumnRefOperator> peers, ColumnRefOperator outer) throws Exception {
        Class<?> type = Class.forName(ScalarApply2AnalyticRule.class.getName() + "$ScalarOperatorCloneShuttle");
        Constructor<?> constructor = type.getDeclaredConstructor(ColumnRefFactory.class, Map.class, Map.class, Set.class);
        constructor.setAccessible(true);
        return (ScalarOperatorVisitor<ScalarOperator, Void>) constructor.newInstance(
                factory, peers, Map.of(), Set.of(outer));
    }

    @Test
    public void testSearchedCaseSubqueryPreservesBothResults() throws Exception {
        checkSqlCase(false, "case when v6 > 50 then v5 when v6 < -10 then v4 else v6 end");
    }

    @Test
    public void testSimpleCaseSubqueryPreservesBothResults() throws Exception {
        checkSqlCase(true, "case v6 when 1 then v5 when 2 then v4 else v6 end");
    }

    private void checkSqlCase(boolean simple, String expression) throws Exception {
        List<CaseWhenOperator> rewrittenCases = new ArrayList<>();
        new MockUp<ScalarApply2AnalyticRule>() {
            @Mock
            public List<OptExpression> transform(Invocation invocation, OptExpression input,
                                                 OptimizerContext context) {
                List<OptExpression> results = invocation.proceed();
                results.forEach(result -> collectWindowCases(result, rewrittenCases));
                return results;
            }
        };
        String sql = "select * from t0, t1 where t0.v1 = t1.v4 " +
                "and t0.v2 < 5 and t1.v5 > 10 and t0.v3 < " +
                "(select max(" + expression + ") from t1 where t0.v1 = t1.v4 and t1.v5 > 10)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "ANALYTIC", "CASE", "THEN", "ELSE");
        assertFalse(rewrittenCases.isEmpty(), "we expect the rule to rewrite this CASE into the window");
        for (CaseWhenOperator actual : rewrittenCases) {
            assertEquals(simple, actual.hasCase());
            assertTrue(actual.hasElse());
            assertEquals(2, actual.getWhenClauseSize());
            assertEquals("v5", ((ColumnRefOperator) actual.getThenClause(0)).getName());
            assertEquals("v4", ((ColumnRefOperator) actual.getThenClause(1)).getName());
            assertEquals("v6", ((ColumnRefOperator) actual.getElseClause()).getName());
            if (simple) {
                assertEquals("v6", ((ColumnRefOperator) actual.getCaseClause()).getName());
                assertEquals("1", actual.getWhenClause(0).toString());
                assertEquals("2", actual.getWhenClause(1).toString());
            } else {
                BinaryPredicateOperator firstWhen = (BinaryPredicateOperator) actual.getWhenClause(0);
                BinaryPredicateOperator secondWhen = (BinaryPredicateOperator) actual.getWhenClause(1);
                assertEquals(BinaryType.GT, firstWhen.getBinaryType());
                assertEquals(BinaryType.LT, secondWhen.getBinaryType());
                assertEquals("v6", ((ColumnRefOperator) firstWhen.getChild(0)).getName());
                assertEquals("v6", ((ColumnRefOperator) secondWhen.getChild(0)).getName());
                assertEquals("50", firstWhen.getChild(1).toString());
                assertEquals("-10", secondWhen.getChild(1).toString());
            }
        }
    }

    private static void collectWindowCases(OptExpression expression, List<CaseWhenOperator> cases) {
        if (expression.getOp() instanceof LogicalWindowOperator window) {
            window.getWindowCall().values().forEach(call -> cases.addAll(Utils.collect(call, CaseWhenOperator.class)));
        }
        expression.getInputs().forEach(child -> collectWindowCases(child, cases));
    }
}
