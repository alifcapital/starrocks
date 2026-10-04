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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ConvertToEqualForNullRuleTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("getConvertToEqualForNullSqlList")
    void testToEqualForNull(String sql, String expectedPlan) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, expectedPlan);
    }

    private static Stream<Arguments> getConvertToEqualForNullSqlList() {
        List<Arguments> sqlList = Lists.newArrayList();
        sqlList.add(Arguments.of("select * from t0 join t1 on v1 = v4 or v1 is null and v4 is null",
                "equal join conjunct: 1: v1 <=> 4: v4"));
        sqlList.add(Arguments.of("select * from t0 join t1 on v1 = v4 or v1 is not null and v4 is null",
                "other join predicates: (1: v1 = 4: v4) OR ((1: v1 IS NOT NULL) AND (4: v4 IS NULL))"));
        sqlList.add(Arguments.of("select * from t0 join t1 on v2 = v4 or v1 is not null and v4 is null",
                "other join predicates: (2: v2 = 4: v4) OR ((1: v1 IS NOT NULL) AND (4: v4 IS NULL))"));
        sqlList.add(Arguments.of("select * from t0 join t1 on v4 = v1 or v1 is null and v4 is null",
                "equal join conjunct: 1: v1 <=> 4: v4"));
        sqlList.add(Arguments.of("select * from t0 join t1 on v4 = v1 or v4 is null and v1 is null",
                "equal join conjunct: 1: v1 <=> 4: v4"));
        sqlList.add(Arguments.of("select * from t0 join t1 on v4 is null and v1 is null or v1 = v4 ",
                "equal join conjunct: 1: v1 <=> 4: v4"));
        sqlList.add(Arguments.of("select * from t0 join t1 on (v4 is null and v1 is null or v1 = v4) and v1 > v5 ",
                "equal join conjunct: 1: v1 <=> 4: v4"));
        sqlList.add(Arguments.of("select * from t0 join t1 on (abs(v4) is null and abs(v1) is null or abs(v4) = abs(v1))" +
                        " and v1 > v5 ", "equal join conjunct: 8: abs <=> 7: abs"));
        sqlList.add(Arguments.of("select * from t0 join t1 on (abs(v4) is null and abs(v1) is null or abs(v4) = abs(v1)) " +
                        "and v1 > v5 ", "equal join conjunct: 8: abs <=> 7: abs"));
        sqlList.add(Arguments.of("select * from t0 join t1 on (abs(v4) is null and abs(v1) is null or abs(v4) = abs(v1)) " +
                "and v1 > v5 join t2 on abs(v4) = v7 or v7 is null and abs(v4) is null",
                "equal join conjunct: 10: abs <=> 11: abs"));
        return sqlList.stream();
    }

    private static ScalarOperator rewrite(ScalarOperator predicate) {
        LogicalJoinOperator join = new LogicalJoinOperator(JoinOperator.INNER_JOIN, predicate);
        ScalarOperator snapshot = predicate.clone();
        List<OptExpression> result = new ConvertToEqualForNullRule().transform(OptExpression.create(join), null);
        assertEquals(snapshot, join.getOnPredicate(), "The rule must not change the input predicate");
        return ((LogicalJoinOperator) result.get(0).getOp()).getOnPredicate();
    }

    // Builds a = b OR (x IS NULL AND y IS NULL), or the same with the OR operands swapped.
    private static ScalarOperator candidate(ScalarOperator a, ScalarOperator b, ScalarOperator x,
                                             ScalarOperator y, boolean reversed, BinaryType type, boolean notNull) {
        BinaryPredicateOperator equality = new BinaryPredicateOperator(type, a, b);
        CompoundPredicateOperator nulls = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                new IsNullPredicateOperator(notNull, x), new IsNullPredicateOperator(y));
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                reversed ? nulls : equality, reversed ? equality : nulls);
    }

    @Test
    void nullChecksMustCoverExactlyTheEqualityOperands() {
        // a = b OR (x IS NULL AND y IS NULL) is a <=> b only when {x, y} is the same set as {a, b}.
        // Repeated operands are the risky case: a = a OR (a IS NULL AND b IS NULL) must not match.
        ScalarOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ScalarOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        ScalarOperator expression = new CallOperator("abs", IntegerType.INT, List.of(a));
        List<ScalarOperator> operands = List.of(a, b, expression, expression.clone());
        for (ScalarOperator left : operands) {
            for (ScalarOperator right : operands) {
                for (ScalarOperator nullLeft : operands) {
                    for (ScalarOperator nullRight : operands) {
                        boolean matches = ImmutableSet.of(left, right).equals(ImmutableSet.of(nullLeft, nullRight));
                        for (boolean reversed : new boolean[] {false, true}) {
                            ScalarOperator predicate =
                                    candidate(left, right, nullLeft, nullRight, reversed, BinaryType.EQ, false);
                            ScalarOperator actual = rewrite(predicate);
                            if (matches) {
                                assertEquals(new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, left, right), actual);
                                assertSame(left, actual.getChild(0));
                                assertSame(right, actual.getChild(1));
                            } else {
                                assertSame(predicate, actual);
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    void otherComparisonsAndShapesKeepThePredicate() {
        ScalarOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ScalarOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        for (boolean reversed : new boolean[] {false, true}) {
            for (BinaryType type : new BinaryType[] {BinaryType.NE, BinaryType.LT, BinaryType.GE}) {
                ScalarOperator predicate = candidate(a, b, a, b, reversed, type, false);
                assertSame(predicate, rewrite(predicate));
            }
            ScalarOperator notNull = candidate(a, b, a, b, reversed, BinaryType.EQ, true);
            assertSame(notNull, rewrite(notNull));
        }
        BinaryPredicateOperator equality = new BinaryPredicateOperator(BinaryType.EQ, a, b);
        CompoundPredicateOperator nulls = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                new IsNullPredicateOperator(a), new IsNullPredicateOperator(b));
        for (ScalarOperator predicate : List.of(equality,
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, equality, equality),
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, nulls, nulls),
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, a, equality))) {
            assertSame(predicate, rewrite(predicate));
        }
    }
}
