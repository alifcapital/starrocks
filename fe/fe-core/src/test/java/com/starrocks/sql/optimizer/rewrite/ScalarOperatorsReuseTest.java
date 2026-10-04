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


package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Config;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.tree.exprreuse.ScalarOperatorsReuse;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ScalarOperatorsReuseTest {
    private static final Logger LOG = LogManager.getLogger(ScalarOperatorsReuseTest.class);

    private ColumnRefFactory columnRefFactory;

    @BeforeEach
    public void init() {
        columnRefFactory = new ColumnRefFactory();
    }

    @Test
    public void testTwoAdd() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", IntegerType.INT, true);

        CallOperator add1 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(column1, ConstantOperator.createInt(2)));

        CallOperator add2 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(add1, ConstantOperator.createInt(3)));

        List<ScalarOperator> oldOperators = Lists.newArrayList(add1, add2);

        List<ScalarOperator> newOperators = ScalarOperatorsReuse.rewriteOperators(oldOperators, columnRefFactory);

        add2.setChild(0, columnRefFactory.getColumnRef(2));
        List<ScalarOperator> exceptResult = Lists.newArrayList(
                columnRefFactory.getColumnRef(2),
                add2);

        assertEquals(exceptResult, newOperators);
    }

    @Test
    public void testThreeAdd() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", IntegerType.INT, true);
        ColumnRefOperator column2 = columnRefFactory.create("t2", IntegerType.INT, true);

        CallOperator add1 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(column1, column2));

        CallOperator add2 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(add1, ConstantOperator.createInt(3)));

        CallOperator add3 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(add2, ConstantOperator.createInt(1)));

        List<ScalarOperator> oldOperators = Lists.newArrayList(add1, add2, add3);

        List<ScalarOperator> newOperators = ScalarOperatorsReuse.rewriteOperators(oldOperators, columnRefFactory);

        add3.setChild(0, columnRefFactory.getColumnRef(4));
        List<ScalarOperator> exceptResult = Lists.newArrayList(
                columnRefFactory.getColumnRef(3),
                columnRefFactory.getColumnRef(4),
                add3);

        assertEquals(exceptResult, newOperators);
    }

    @Test
    public void testNoRedundantCommonScalarOperators() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", IntegerType.INT, true);
        ColumnRefOperator column2 = columnRefFactory.create("t2", IntegerType.INT, true);
        ColumnRefOperator column3 = columnRefFactory.create("t3", IntegerType.INT, true);

        CallOperator add1 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(column1, ConstantOperator.createInt(1)));

        CallOperator add2 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(add1, column2));

        CallOperator add3 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(add2, column3));

        CallOperator multi = new CallOperator("multi", IntegerType.INT,
                Lists.newArrayList(add3, ConstantOperator.createInt(2)));

        List<ScalarOperator> oldOperators = Lists.newArrayList(add1, add3, multi);

        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubScalarOperators =
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, oldOperators, columnRefFactory);

        // FixMe(kks): This case could improve
        assertEquals(commonSubScalarOperators.size(), 3);
    }

    @Test
    public void testCollectCommonScalarOperators() {
        ColumnRefOperator column1 = columnRefFactory.create("a", IntegerType.INT, true);
        ColumnRefOperator column2 = columnRefFactory.create("b", IntegerType.INT, true);
        ColumnRefOperator column3 = columnRefFactory.create("c", IntegerType.INT, true);
        ColumnRefOperator column4 = columnRefFactory.create("d", IntegerType.INT, true);

        CallOperator addAB = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(column1, column2));

        CallOperator addABC = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(addAB, column3));

        CallOperator addBC = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(column2, column3));

        CallOperator addBCD = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(addBC, column4));

        List<ScalarOperator> oldOperators = Lists.newArrayList(addABC, addBCD);

        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubScalarOperators =
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, oldOperators, columnRefFactory);

        // FixMe(kks): could we improve this case?
        assertTrue(commonSubScalarOperators.isEmpty());
    }

    @Test
    public void testNonDeterministicFuncCommonUsed() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", IntegerType.INT, true);
        ColumnRefOperator column2 = columnRefFactory.create("t2", IntegerType.INT, true);
        ColumnRefOperator column3 = columnRefFactory.create("t3", IntegerType.INT, true);

        CallOperator add1 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(column1, new CallOperator(FunctionSet.RANDOM, FloatType.DOUBLE, Lists.newArrayList())));

        CallOperator add2 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(add1, column2));

        CallOperator add3 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(add2, column3));

        CallOperator add4 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(add3, ConstantOperator.createInt(1)));

        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubScalarOperators =
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, ImmutableList.of(add1, add2, add3, add4),
                        columnRefFactory);
        assertFalse(commonSubScalarOperators.isEmpty());
    }

    @Test
    public void testNonDeterministicFuncNotCommonUsed() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", IntegerType.INT, true);
        ColumnRefOperator column2 = columnRefFactory.create("t2", IntegerType.INT, true);
        ColumnRefOperator column3 = columnRefFactory.create("t3", IntegerType.INT, true);

        CallOperator add1 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(column1, ConstantOperator.createInt(1)));

        CallOperator add2 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(add1, column2));

        CallOperator add3 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(add2, new CallOperator(FunctionSet.RANDOM, FloatType.DOUBLE, Lists.newArrayList())));

        CallOperator add4 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(add3, column3));

        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubScalarOperators =
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, ImmutableList.of(add1, add2, add3, add4),
                        columnRefFactory);
        assertEquals(3, commonSubScalarOperators.size());
    }

    @Test
    public void testLambdaFunctionWithoutLambdaArguments() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", IntegerType.INT, true);
        ColumnRefOperator arg = columnRefFactory.create("x", IntegerType.INT, true, true);


        CallOperator multi = new CallOperator("multi", IntegerType.INT,
                Lists.newArrayList(column1, ConstantOperator.createInt(2)));

        CallOperator multi1 = new CallOperator("multi", IntegerType.INT,
                Lists.newArrayList(column1, ConstantOperator.createInt(2)));

        CallOperator add1 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(multi, multi1));

        CallOperator add2 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(add1, arg));
        // x-> t1 * 2 + t1 *2 + x
        List<ScalarOperator> oldOperators = Lists.newArrayList(add2);

        // reuse lambda argument non-related sub expressions : t1*2
        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubScalarOperators =
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, oldOperators, columnRefFactory);
        assertEquals(commonSubScalarOperators.size(), 1);
    }

    @Test
    public void testLambdaFunctionScalarOperatorsWithLambdaArguments() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", IntegerType.INT, true);
        ColumnRefOperator arg = columnRefFactory.create("x", IntegerType.INT, true, true);


        CallOperator multi = new CallOperator("multi", IntegerType.INT,
                Lists.newArrayList(arg, ConstantOperator.createInt(2)));

        CallOperator multi1 = new CallOperator("multi", IntegerType.INT,
                Lists.newArrayList(arg, ConstantOperator.createInt(2)));

        CallOperator add1 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(multi, multi1));

        CallOperator add2 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(add1, column1));
        // x-> x * 2 + x *2 + t1
        List<ScalarOperator> oldOperators = Lists.newArrayList(add2);

        // reuse lambda argument related sub expressions : x*2
        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubScalarOperators =
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, oldOperators, columnRefFactory);
        assertEquals(commonSubScalarOperators.size(), 1);

    }

    private CallOperator add(ScalarOperator left, ScalarOperator right) {
        return new CallOperator("add", IntegerType.INT, Lists.newArrayList(left, right));
    }

    private List<ScalarOperator> commonExpressions(ScalarOperator expression) {
        return ScalarOperatorsReuse.collectCommonSubScalarOperators(null, List.of(expression), columnRefFactory)
                .values().stream().flatMap(level -> level.keySet().stream()).toList();
    }

    @Test
    public void testRealLambdaHoistsCapturedColumnsButKeepsCurrentArgumentsAndLocalRefs() {
        ColumnRefOperator captured = columnRefFactory.create("captured", IntegerType.INT, true);
        ColumnRefOperator argument = columnRefFactory.create("x", IntegerType.INT, true, true);
        ColumnRefOperator local = columnRefFactory.create("local", IntegerType.INT, true);
        CallOperator independent = add(captured, ConstantOperator.createInt(2));
        CallOperator dependent = add(argument, ConstantOperator.createInt(3));
        CallOperator localDependent = add(local, ConstantOperator.createInt(4));
        CallOperator body = add(add(independent, independent.clone()),
                add(add(dependent, dependent.clone()), add(localDependent, localDependent.clone())));
        LambdaFunctionOperator lambda = new LambdaFunctionOperator(List.of(argument), body, IntegerType.INT);
        lambda.addColumnToExpr(Map.of(local, add(argument, ConstantOperator.createInt(1))));
        ScalarOperator before = lambda.clone();
        List<ScalarOperator> common = commonExpressions(lambda);
        assertTrue(common.stream().anyMatch(independent::equals));
        assertFalse(common.stream().anyMatch(expression -> expression.getUsedColumns().contains(argument.getId())));
        assertFalse(common.stream().anyMatch(expression -> expression.getUsedColumns().contains(local.getId())));
        assertEquals(before, lambda);
        assertSame(body, lambda.getLambdaExpr());
        assertEquals(1, lambda.getColumnRefMap().size());
    }

    @Test
    public void testNestedLambdaKeepsOuterArgumentsAndOuterLocalReferences() {
        ColumnRefOperator captured = columnRefFactory.create("captured", IntegerType.INT, true);
        ColumnRefOperator outerArgument = columnRefFactory.create("x", IntegerType.INT, true, true);
        ColumnRefOperator innerArgument = columnRefFactory.create("y", IntegerType.INT, true, true);
        ColumnRefOperator outerLocal = columnRefFactory.create("outer_local", IntegerType.INT, true);
        CallOperator independent = add(captured, ConstantOperator.createInt(2));
        CallOperator outerDependent = add(outerArgument, ConstantOperator.createInt(3));
        CallOperator localDependent = add(outerLocal, ConstantOperator.createInt(4));
        CallOperator innerBody = add(add(independent, independent.clone()),
                add(add(outerDependent, outerDependent.clone()), add(localDependent, localDependent.clone())));
        LambdaFunctionOperator inner = new LambdaFunctionOperator(List.of(innerArgument), innerBody, IntegerType.INT);
        LambdaFunctionOperator outer = new LambdaFunctionOperator(List.of(outerArgument), inner, IntegerType.INT);
        outer.addColumnToExpr(Map.of(outerLocal, add(outerArgument, ConstantOperator.createInt(1))));
        ScalarOperator before = outer.clone();
        List<ScalarOperator> common = commonExpressions(outer);
        assertTrue(common.stream().anyMatch(independent::equals));
        assertFalse(common.stream().anyMatch(expression -> expression.getUsedColumns().contains(outerArgument.getId())));
        assertFalse(common.stream().anyMatch(expression -> expression.getUsedColumns().contains(outerLocal.getId())));
        assertEquals(before, outer);
        assertSame(inner, outer.getLambdaExpr());
        assertSame(innerBody, inner.getLambdaExpr());
    }

    @Test
    public void testNestedLocalDefinitionPropagatesCapturedArgumentDependency() {
        ColumnRefOperator outerArgument = columnRefFactory.create("x", IntegerType.INT, true, true);
        ColumnRefOperator innerArgument = columnRefFactory.create("y", IntegerType.INT, true, true);
        ColumnRefOperator innerLocal = columnRefFactory.create("inner_local", IntegerType.INT, true);
        CallOperator innerBody = add(innerLocal, ConstantOperator.createInt(1));
        LambdaFunctionOperator inner = new LambdaFunctionOperator(List.of(innerArgument), innerBody, IntegerType.INT);
        inner.addColumnToExpr(Map.of(innerLocal, add(outerArgument, ConstantOperator.createInt(2))));
        CallOperator wrapper = new CallOperator("wrapper", IntegerType.INT, List.of(inner));
        LambdaFunctionOperator outer = new LambdaFunctionOperator(List.of(outerArgument), wrapper, IntegerType.INT);
        // The visible inner expression uses only a local ref; its definition captures x.
        // Hoisting wrapper out of the outer lambda would put that definition outside x's scope.
        assertTrue(commonExpressions(outer).isEmpty());
        assertSame(wrapper, outer.getLambdaExpr());
        assertSame(innerBody, inner.getLambdaExpr());
        assertEquals(add(outerArgument, ConstantOperator.createInt(2)), inner.getColumnRefMap().get(innerLocal));
    }

    private ScalarOperator generateCompoundPredicateOperator(ColumnRefOperator columnRefOperator,
                                                             int orNum) {
        ScalarOperator result = columnRefOperator;
        for (int i = 0; i < orNum; i++) {
            result = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                    result, ConstantOperator.createInt(i));
        }
        return result;
    }

    @Test
    public void testScalarOperatorDepth() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", IntegerType.INT, true);
        ColumnRefOperator column2 = columnRefFactory.create("t2", IntegerType.INT, true);
        ScalarOperator or1 = generateCompoundPredicateOperator(column1, Config.max_scalar_operator_optimize_depth - 1);
        ScalarOperator or2 = generateCompoundPredicateOperator(column2, Config.max_scalar_operator_optimize_depth - 1);
        assertEquals(0, column1.getDepth());
        assertEquals(0, column2.getDepth());
        assertEquals(Config.max_scalar_operator_optimize_depth - 1, or1.getDepth());
        assertEquals(Config.max_scalar_operator_optimize_depth - 1, or2.getDepth());

        ColumnRefOperator arg = columnRefFactory.create("x", IntegerType.INT, true, true);
        assertEquals(0, arg.getDepth());
        CallOperator multi = new CallOperator("multi", IntegerType.INT,
                Lists.newArrayList(arg, ConstantOperator.createInt(2)));
        assertEquals(1, multi.getDepth());

        CallOperator multi1 = new CallOperator("multi", IntegerType.INT,
                Lists.newArrayList(arg, ConstantOperator.createInt(2)));
        assertEquals(1, multi1.getDepth());
        CallOperator add1 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(multi, multi1));
        assertEquals(2, add1.getDepth());

        CallOperator add3 = new CallOperator("add", IntegerType.INT,
                Lists.newArrayList(multi, or1));
        assertEquals(Config.max_scalar_operator_optimize_depth, add3.getDepth());
    }

    @Test
    public void testScalarOperatorIncrDepth() {
        ColumnRefOperator column1 = columnRefFactory.create("t1", IntegerType.INT, true);
        assertEquals(0, column1.getDepth());

        ColumnRefOperator column2 = columnRefFactory.create("t2", IntegerType.INT, true);
        assertEquals(0, column1.getDepth());

        // mock construct
        column1.incrDepth(column2);
        assertEquals(1, column1.getDepth());

        column1.incrDepth(column2, column2);
        assertEquals(2, column1.getDepth());

        column1.incrDepth(ImmutableList.of(column2, column2));
        assertEquals(3, column1.getDepth());
    }

    @Test
    public void testCaseWhenWithTooManyChildren1() {
        final int prev = Config.max_scalar_operator_flat_children;
        Config.max_scalar_operator_flat_children = 0;
        ColumnRefOperator column1 = columnRefFactory.create("t1", IntegerType.INT, true);
        ColumnRefOperator column2 = columnRefFactory.create("t2", IntegerType.INT, true);
        ScalarOperator or1 = generateCompoundPredicateOperator(column1, Config.max_scalar_operator_optimize_depth - 1);
        ScalarOperator or2 = generateCompoundPredicateOperator(column2, Config.max_scalar_operator_optimize_depth - 1);

        CaseWhenOperator cwo1 = new CaseWhenOperator(IntegerType.INT, or1, ConstantOperator.createInt(0),
                Lists.newArrayList(or1, ConstantOperator.createInt(0), or2, ConstantOperator.createInt(1)));
        CaseWhenOperator cwo2 = new CaseWhenOperator(IntegerType.INT, or1, ConstantOperator.createInt(0),
                Lists.newArrayList(or1, ConstantOperator.createInt(2), or2, ConstantOperator.createInt(3)));

        List<ScalarOperator> oldOperators = Lists.newArrayList(cwo1, cwo2);
        List<ScalarOperator> newOperators = ScalarOperatorsReuse.rewriteOperators(oldOperators, columnRefFactory);
        assertEquals(newOperators.size(), 2);

        Map<Integer, Map<ScalarOperator, ColumnRefOperator>> commonSubScalarOperators =
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, oldOperators, columnRefFactory);
        assertEquals(0, commonSubScalarOperators.size());
        Config.max_scalar_operator_flat_children = prev;
    }

    @Test
    public void testReuseCommutativeCompoundPredicates() {
        // Regression test: when two distinct OperatorIds (different child group order, e.g.
        // `a AND b` vs `b AND a`) both qualify as common subexpressions, their rewritten
        // ScalarOperators are considered equal by CompoundPredicateOperator#equals (which
        // normalizes child order). The previous ImmutableMap.Builder put failed with
        // "Multiple entries with same key".
        ColumnRefOperator c1 = columnRefFactory.create("c1", IntegerType.INT, true);
        ColumnRefOperator c2 = columnRefFactory.create("c2", IntegerType.INT, true);

        BinaryPredicateOperator a = new BinaryPredicateOperator(BinaryType.GT, c1, ConstantOperator.createInt(1));
        BinaryPredicateOperator b = new BinaryPredicateOperator(BinaryType.GT, c2, ConstantOperator.createInt(2));

        // Two copies of `a AND b` and two copies of `b AND a` — each pair makes its own OperatorId
        // qualify as duplicated/common at the same depth.
        CompoundPredicateOperator andAB1 =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, a, b);
        CompoundPredicateOperator andAB2 =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, a, b);
        CompoundPredicateOperator andBA1 =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, b, a);
        CompoundPredicateOperator andBA2 =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, b, a);

        List<ScalarOperator> oldOperators = Lists.newArrayList(andAB1, andAB2, andBA1, andBA2);
        // Should not throw IllegalArgumentException("Multiple entries with same key").
        List<ScalarOperator> newOperators = ScalarOperatorsReuse.rewriteOperators(oldOperators, columnRefFactory);
        assertEquals(4, newOperators.size());
    }

    @Test
    public void testCaseWhenWithTooManyChildren2() {
        final int prev = Config.max_scalar_operator_flat_children;
        Config.max_scalar_operator_flat_children = 0;
        ColumnRefOperator column1 = columnRefFactory.create("t1", IntegerType.INT, true);
        ColumnRefOperator column2 = columnRefFactory.create("t2", IntegerType.INT, true);
        ScalarOperator or1 = generateCompoundPredicateOperator(column1, 2000);
        ScalarOperator or2 = generateCompoundPredicateOperator(column2, 2000);

        CaseWhenOperator cwo1 = new CaseWhenOperator(IntegerType.INT, or1, ConstantOperator.createInt(0),
                Lists.newArrayList(or1, ConstantOperator.createInt(0), or2, ConstantOperator.createInt(1)));
        CaseWhenOperator cwo2 = new CaseWhenOperator(IntegerType.INT, or1, ConstantOperator.createInt(0),
                Lists.newArrayList(or1, ConstantOperator.createInt(2), or2, ConstantOperator.createInt(3)));

        List<ScalarOperator> oldOperators = Lists.newArrayList(cwo1, cwo2);
        try {
            List<ScalarOperator> newOperators = ScalarOperatorsReuse.rewriteOperators(oldOperators, columnRefFactory);
            assertEquals(newOperators.size(), 2);
            for (int i = 0; i < newOperators.size(); i++) {
                assertTrue(newOperators.get(i).equals(oldOperators.get(i)));
            }
        } catch (Exception e) {
            fail();
        }
        Config.max_scalar_operator_flat_children = prev;
    }

    private static CallOperator call(String name, ScalarOperator... children) {
        return new CallOperator(name, IntegerType.INT, Lists.newArrayList(children));
    }

    private static CallOperator addOne(ScalarOperator child) {
        return call("add", child, ConstantOperator.createInt(1));
    }

    private static CompoundPredicateOperator compound(CompoundPredicateOperator.CompoundType type,
                                                      ScalarOperator left, ScalarOperator right) {
        return new CompoundPredicateOperator(type, left, right);
    }

    private List<ScalarOperator> commonKeys(List<ScalarOperator> roots) {
        return ScalarOperatorsReuse.collectCommonSubScalarOperators(null, roots, columnRefFactory)
                .values().stream().flatMap(level -> level.keySet().stream()).toList();
    }

    @Test
    public void commutativeAndOrChildrenShareOneCommonOperator() {
        // AND and OR are commutative, so p AND q and q AND p are one common expression.
        ColumnRefOperator a = columnRefFactory.create("a", IntegerType.INT, true);
        ColumnRefOperator b = columnRefFactory.create("b", IntegerType.INT, true);
        BinaryPredicateOperator p = new BinaryPredicateOperator(BinaryType.GT, a, ConstantOperator.createInt(1));
        BinaryPredicateOperator q = new BinaryPredicateOperator(BinaryType.GT, b, ConstantOperator.createInt(2));
        for (CompoundPredicateOperator.CompoundType type : List.of(CompoundPredicateOperator.CompoundType.AND,
                CompoundPredicateOperator.CompoundType.OR)) {
            List<ScalarOperator> roots = List.of(compound(type, p, q), compound(type, q, p),
                    compound(type, p.clone(), q.clone()), compound(type, q.clone(), p.clone()));
            List<Map<ScalarOperator, ColumnRefOperator>> levels = new ArrayList<>(
                    ScalarOperatorsReuse.collectCommonSubScalarOperators(null, roots, columnRefFactory).values());
            // p and q repeat too, so the first level holds p and q and the second level holds one compound.
            assertEquals(2, levels.size());
            assertEquals(2, levels.get(0).size());
            Map<ScalarOperator, ColumnRefOperator> level = levels.get(1);
            assertEquals(1, level.size());
            ColumnRefOperator refP = levels.get(0).get(p);
            ColumnRefOperator refQ = levels.get(0).get(q);
            assertEquals(compound(type, refP, refQ), level.keySet().iterator().next());

            List<ScalarOperator> rewritten = ScalarOperatorsReuse.rewriteOperators(roots, columnRefFactory);
            assertEquals(4, rewritten.size());
            for (ScalarOperator operator : rewritten) {
                assertTrue(operator.isColumnRef());
                assertEquals(rewritten.get(0), operator);
            }
        }
    }

    @Test
    public void andAndOrOverSameChildrenStayDistinct() {
        ColumnRefOperator a = columnRefFactory.create("a", IntegerType.INT, true);
        ColumnRefOperator b = columnRefFactory.create("b", IntegerType.INT, true);
        BinaryPredicateOperator p = new BinaryPredicateOperator(BinaryType.GT, a, ConstantOperator.createInt(1));
        BinaryPredicateOperator q = new BinaryPredicateOperator(BinaryType.GT, b, ConstantOperator.createInt(2));
        List<ScalarOperator> roots = List.of(
                compound(CompoundPredicateOperator.CompoundType.AND, q, p),
                compound(CompoundPredicateOperator.CompoundType.OR, q, p),
                compound(CompoundPredicateOperator.CompoundType.AND, p, q),
                compound(CompoundPredicateOperator.CompoundType.OR, p, q));
        List<Map<ScalarOperator, ColumnRefOperator>> levels = new ArrayList<>(
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, roots, columnRefFactory).values());
        assertEquals(2, levels.size());
        ColumnRefOperator refP = levels.get(0).get(p);
        ColumnRefOperator refQ = levels.get(0).get(q);
        assertEquals(2, levels.get(1).size());
        assertTrue(levels.get(1).containsKey(compound(CompoundPredicateOperator.CompoundType.AND, refP, refQ)));
        assertTrue(levels.get(1).containsKey(compound(CompoundPredicateOperator.CompoundType.OR, refP, refQ)));
    }

    @Test
    public void notAndNonCommutativeOperatorsKeepChildOrder() {
        ColumnRefOperator a = columnRefFactory.create("a", IntegerType.INT, true);
        ColumnRefOperator b = columnRefFactory.create("b", IntegerType.INT, true);
        // a - b and b - a are different values, so only the repeated a - b is common.
        List<ScalarOperator> roots = List.of(call("subtract", a, b), call("subtract", b, a),
                call("subtract", a, b), call("subtract", a, a), call("subtract", b, b));
        assertEquals(List.of(call("subtract", a, b)), commonKeys(roots));

        CompoundPredicateOperator not = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, a);
        assertEquals(List.of(not), commonKeys(List.of(not, not.clone())));
    }

    @Test
    public void commonOperatorsKeepInsertionOrderAndColumnIdOrder() {
        // Column ids of common expressions appear in the plan, so we expect them in the order the repeats are found.
        ColumnRefOperator a = columnRefFactory.create("a", IntegerType.INT, true);
        ColumnRefOperator b = columnRefFactory.create("b", IntegerType.INT, true);
        CallOperator p = call("add", a, b);
        CallOperator q = call("subtract", a, b);
        CallOperator r = call("multiply", p, q);
        // The second occurrences arrive in the order p, q, r.
        List<ScalarOperator> roots = List.of(q, p, p.clone(), q.clone(), r, r.clone());
        int firstNewId = b.getId() + 1;
        List<Map<ScalarOperator, ColumnRefOperator>> levels = new ArrayList<>(
                ScalarOperatorsReuse.collectCommonSubScalarOperators(null, roots, columnRefFactory).values());
        assertEquals(2, levels.size());
        assertEquals(List.of(p, q), new ArrayList<>(levels.get(0).keySet()));
        List<ColumnRefOperator> refs = new ArrayList<>(levels.get(0).values());
        assertEquals(firstNewId, refs.get(0).getId());
        assertEquals(firstNewId + 1, refs.get(1).getId());
        Map<ScalarOperator, ColumnRefOperator> top = levels.get(1);
        assertEquals(1, top.size());
        assertEquals(call("multiply", refs.get(0), refs.get(1)), top.keySet().iterator().next());
        assertEquals(firstNewId + 2, top.values().iterator().next().getId());
    }

    @Test
    public void lambdaSiblingsKeepIsolatedArgumentDependencies() {
        // An expression that uses a lambda argument cannot move out of the lambda. Its siblings that do not
        // use the argument are still common expressions.
        ColumnRefOperator t = columnRefFactory.create("t", IntegerType.INT, true);
        ColumnRefOperator arg = columnRefFactory.create("x", IntegerType.INT, true, true);
        CallOperator body = call("f", addOne(arg), addOne(arg.clone()), addOne(t), addOne(t));
        LambdaFunctionOperator lambda = new LambdaFunctionOperator(List.of(arg), body, IntegerType.INT);
        assertEquals(List.of(addOne(t)), commonKeys(List.of(lambda)));
    }

    @Test
    public void projectionKeepsExpressionWhenTwoOutputsRewriteToTheSameColumnRef() {
        ColumnRefOperator a = columnRefFactory.create("a", IntegerType.INT, true);
        ColumnRefOperator b = columnRefFactory.create("b", IntegerType.INT, true);
        ColumnRefOperator o1 = columnRefFactory.create("o1", IntegerType.INT, true);
        ColumnRefOperator o2 = columnRefFactory.create("o2", IntegerType.INT, true);
        ColumnRefOperator o3 = columnRefFactory.create("o3", IntegerType.INT, true);
        ColumnRefOperator o4 = columnRefFactory.create("o4", IntegerType.INT, true);
        ColumnRefOperator o5 = columnRefFactory.create("o5", IntegerType.INT, true);
        ColumnRefOperator o6 = columnRefFactory.create("o6", IntegerType.INT, true);
        Map<ColumnRefOperator, ScalarOperator> outputs = new LinkedHashMap<>();
        outputs.put(o1, addOne(a));
        outputs.put(o2, addOne(a));
        outputs.put(o3, call("multiply", addOne(a), b));
        outputs.put(o4, b);
        outputs.put(o5, b);
        outputs.put(o6, addOne(a));
        Projection result = ScalarOperatorsReuse.rewriteProjectionOrLambdaExpr(new Projection(outputs),
                columnRefFactory);

        assertEquals(1, result.getCommonSubOperatorMap().size());
        ColumnRefOperator common = result.getCommonSubOperatorMap().keySet().iterator().next();
        assertEquals(addOne(a), result.getCommonSubOperatorMap().get(common));
        Map<ColumnRefOperator, ScalarOperator> rewritten = result.getColumnRefMap();
        // Two outputs must not be the same bare column ref, because the BE cannot share one column between
        // two outputs. So only the first output takes the common column, and later ones keep their expression.
        assertEquals(common, rewritten.get(o1));
        assertEquals(addOne(a), rewritten.get(o2));
        assertEquals(call("multiply", common, b), rewritten.get(o3));
        assertEquals(b, rewritten.get(o4));
        assertEquals(b, rewritten.get(o5));
        assertEquals(addOne(a), rewritten.get(o6));
        assertEquals(List.of(o1, o2, o3, o4, o5, o6), new ArrayList<>(rewritten.keySet()));
    }
}
