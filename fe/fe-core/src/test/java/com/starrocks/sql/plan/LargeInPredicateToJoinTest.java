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

import com.google.common.collect.Lists;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.qe.SessionVariableConstants;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LargeInPredicate;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.sql.optimizer.operator.logical.LogicalRawValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRawValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LargeInConstants;
import com.starrocks.sql.optimizer.operator.scalar.LargeInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LargeInPredicateToJoinTest extends PlanTestBase {

    @BeforeEach
    public void before() {
        connectContext.getSessionVariable().setLargeInPredicateThreshold(3);
    }

    @AfterEach
    public void after() {
        connectContext.getSessionVariable().setLargeInPredicateThreshold(100000);
    }

    private void assertLargeInTransformation(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, "LEFT SEMI JOIN");
        assertContains(plan, "RAW_VALUES");
    }

    private void assertLargeNotInTransformation(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, "LEFT ANTI JOIN");
        assertContains(plan, "RAW_VALUES");
    }

    private void assertNoLargeInTransformation(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "LEFT SEMI JOIN");
        assertNotContains(plan, "LEFT ANTI JOIN");
        assertNotContains(plan, "RAW_VALUES");
    }

    private void assertLargeInException(String sql, String expectedMessage) {
        ExceptionChecker.expectThrowsWithMsg(LargeInPredicateException.class,
                expectedMessage, () -> getFragmentPlan(sql));
    }


    // ========== Basic Transformation Tests ==========

    @Test
    public void testBasicTransformations() throws Exception {
        // Test basic IN transformation
        assertLargeInTransformation("select * from t0 where v1 in (1, 2, 3, 4) and v2 = 3 and v3 < 6");
        
        // Test basic NOT IN transformation
        assertLargeNotInTransformation("select * from t0 where v1 not in (1, 2, 3, 4)");
    }

    @Test
    public void testSupportedDataTypes() throws Exception {
        // Test integer types (smallint, int, bigint) with BIGINT constants
        assertLargeInTransformation("select * from tall where tb in (1, 2, 3, 4)"); // smallint
        assertLargeInTransformation("select * from tall where tc in (100, 200, 300, 400)"); // int
        assertLargeInTransformation("select * from tall where td in (1000, 2000, 3000, 4000)"); // bigint
        
        // Test string types (varchar, char) with string constants
        assertLargeInTransformation("select * from tall where ta in ('a', 'b', 'c', 'd')"); // varchar
        assertLargeInTransformation("select * from tall where tt in ('str1', 'str2', 'str3', 'str4')"); // char

        // Constants of another type compare as in the InPredicate with the same list
        assertLargeInTransformation("select * from tall where ta in (1, 2, 3, 4)"); // varchar with integers
        assertLargeInTransformation("select * from tall where tc in ('1', '2', '3', '4')"); // int with strings
        assertLargeInTransformation(
                "select * from tall where th in ('2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04')"); // datetime
        assertLargeInTransformation(
                "select * from tall where ti in ('2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04')"); // date
        assertLargeInTransformation("select * from test_all_type where id_decimal in (1.5, 2.25, 3, 4)"); // decimal
        assertLargeInTransformation("select * from tall where td in (-1, -2, 3, 4)"); // negative numbers
        assertLargeInTransformation(
                "select * from tall where ta in (20218840600116072801, 20218840600116072802, 3, 4)"); // out of BIGINT
    }

    @Test
    public void testComparisonType() throws Exception {
        // The values have the type of the compared column, so the column is not cast
        String plan = getFragmentPlan("select * from test_all_type where t1c in ('1', '2', '3', '4')");
        assertContains(plan, "constant type: INT");
        assertContains(plan, "equal join conjunct: 3: t1c = 11: const_value");

        plan = getFragmentPlan("select * from test_all_type where t1a in (1, 2, 3, 4)");
        assertContains(plan, "constant type: VARCHAR");
        assertContains(plan, "equal join conjunct: 1: t1a = 11: const_value");

        plan = getFragmentPlan("select * from test_all_type where t1b in (1, 2, 3, 4)");
        assertContains(plan, "constant type: SMALLINT");
        assertContains(plan, "equal join conjunct: 2: t1b = 11: const_value");

        plan = getFragmentPlan("select * from test_all_type where id_date in " +
                "('2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04')");
        assertContains(plan, "constant type: DATE");
        assertContains(plan, "equal join conjunct: 9: id_date = 11: const_value");

        plan = getFragmentPlan("select * from test_all_type where id_decimal in (1.50, 2.25, 3.00, 4.00)");
        assertContains(plan, "constant type: DECIMAL64(18,2)");
        assertContains(plan, "equal join conjunct: 10: id_decimal = 11: const_value");

        // A constant that is not an INT: the comparison is in the common type, as in the InPredicate
        plan = getFragmentPlan("select * from test_all_type where t1c in ('01', '2', '3', '4')");
        assertContains(plan, "constant type: VARCHAR");
        assertContains(plan, "equal join conjunct: 12: cast = 11: const_value");
    }

    @Test
    public void testValuesInThrift() throws Exception {
        String plan = getThriftPlan("select * from test_all_type where id_decimal in (1.5, 2.25, 3, 4)");
        assertContains(plan, "string_values:[1.50, 2.25, 3.00, 4.00]");

        plan = getThriftPlan("select * from test_all_type where id_datetime in " +
                "('2023-01-01', '2023-01-02 03:04:05', '2023-01-02 03:04:05.000007', '2023-01-04')");
        assertContains(plan, "string_values:[2023-01-01 00:00:00, 2023-01-02 03:04:05, " +
                "2023-01-02 03:04:05.000007, 2023-01-04 00:00:00]");

        plan = getThriftPlan("select * from test_all_type where t1c in ('1', '2', '3', '4')");
        assertContains(plan, "long_values:[1, 2, 3, 4]");
    }

    @Test
    public void testComplexExpressions() throws Exception {
        // Test arithmetic expressions
        String sql1 = "select * from t0 where (v1 + 1) in (2, 3, 4, 5)";
        String plan1 = getFragmentPlan(sql1);
        assertContains(plan1, "LEFT SEMI JOIN");
        assertContains(plan1, "RAW_VALUES");
        assertContains(plan1, "equal join conjunct: 5: add = 4: const_value");

        // Test function calls with casting
        String sql2 = "select * from t0 where cast(abs(v1) as bigint) in (1, 2, 3, 4)";
        String plan2 = getFragmentPlan(sql2);
        assertContains(plan2, "LEFT SEMI JOIN");
        assertContains(plan2, "RAW_VALUES");
        assertContains(plan2, "equal join conjunct: 5: cast = 4: const_value");

        // Test other complex expressions
        assertLargeInTransformation("select * from tall where upper(ta) in ('A', 'B', 'C', 'D')");
        assertLargeInTransformation("select * from t0 where cast(v1 as string) in ('1', '2', '3', '4')");
        assertLargeInTransformation("select * from tall where cast(tb as bigint) in (10, 20, 30, 40)");
        assertLargeInTransformation("select * from t0 where coalesce(v1, v2) in (1, 2, 3, 4)");
        
        // Test CASE expression
        String sql3 = "select * from t0 where case when v2 > 100 then v1 else v2 end in (1, 2, 3, 4)";
        String plan3 = getFragmentPlan(sql3);
        assertContains(plan3, "LEFT SEMI JOIN");
        assertContains(plan3, "RAW_VALUES");
        assertContains(plan3, "equal join conjunct: 5: if = 4: const_value");
    }

    @Test
    public void testSQLContexts() throws Exception {
        // Test in different SQL contexts
        assertLargeInTransformation("select v1, count(*) from t0 group by v1 having v1 in (1, 2, 3, 4)"); // HAVING
        assertLargeInTransformation("select * from t1 where v4 in (select v1 from t0 where v1 in (1, 2, 3, 4))"); // Subquery
        assertLargeInTransformation("select * from t0 where v1 in (1, 2, 3, 4) order by v2 limit 10"); // ORDER BY + LIMIT
        assertLargeInTransformation("with cte as (select * from t0 where v1 in (1, 2, 3, 4)) select * from cte"); // CTE
        
        // Test with JOINs
        String joinSql = "select * from t0 join t1 on t0.v1 = t1.v4 where t0.v1 in (1, 2, 3, 4)";
        String joinPlan = getFragmentPlan(joinSql);
        assertContains(joinPlan, "LEFT SEMI JOIN");
        assertContains(joinPlan, "RAW_VALUES");
        assertContains(joinPlan, "INNER JOIN"); // Original join should still exist
    }

    @Test
    public void testPredicateCombination() throws Exception {
        // Test LargeInPredicate combined with other predicates using AND
        String sql = "select * from t0 where v1 in (1, 2, 3, 4) and v2 > 100";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: const_value\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: v2 > 100");
    }

    @Test
    public void testSpecialValues() throws Exception {
        // Test with duplicate values
        assertLargeInTransformation("select * from t0 where v1 in (1, 1, 2, 2, 3, 3)");
        
        // Test with special characters in strings
        assertLargeInTransformation("select * from tall where ta in ('a\\'b', 'c\"d', 'e\\\\f', 'g\\nh')");
        
        // Test NULL handling - should NOT use LargeInPredicate
        assertNoLargeInTransformation("select * from t0 where v1 in (1, 2, 3, NULL)");
    }

    @Test
    public void testAdvancedSQLFeatures() throws Exception {
        // Test with window functions
        String windowSql = "select v1, row_number() over (order by v2) from t0 where v1 in (1, 2, 3, 4)";
        String windowPlan = getFragmentPlan(windowSql);
        assertContains(windowPlan, "LEFT SEMI JOIN");
        assertContains(windowPlan, "RAW_VALUES");
        assertContains(windowPlan, "ANALYTIC");
        
        // Test with UNION
        String unionSql = "select * from t0 where v1 in (1, 2, 3, 4) union all select * from t0 where v1 in (5, 6, 7, 8)";
        String unionPlan = getFragmentPlan(unionSql);
        assertContains(unionPlan, "LEFT SEMI JOIN");
        assertContains(unionPlan, "RAW_VALUES");
        assertContains(unionPlan, "UNION");
        
        // Test with set operations
        assertLargeInTransformation("select * from t0 where v1 in (1, 2, 3, 4) intersect select * from t0 where v2 > 100");
        
        // Test with complex aggregation
        assertLargeInTransformation("select v1, count(*), sum(v2), avg(v3) from t0 where v1 in (1, 2, 3, 4)" +
                " group by v1 having count(*) > 1");
        
        // Test with arithmetic expressions
        assertLargeInTransformation("select * from t0 where (v1 * v2 + v3) in (100, 200, 300, 400)");
        
        // Test broadcast hint
        String broadcastSql = "select * from t0 where v1 in (1, 2, 3, 4)";
        String broadcastPlan = getFragmentPlan(broadcastSql);
        assertContains(broadcastPlan, "LEFT SEMI JOIN");
        assertContains(broadcastPlan, "RAW_VALUES");
        assertContains(broadcastPlan, "BROADCAST");
    }


    // ========== Error Cases and Limitations ==========

    @Test
    public void testLimitationsAndExceptions() {
        // A LargeInPredicate inside another expression of a predicate
        assertLargeInException(
                "select * from t0 where v1 in (1, 2, 3, 4) or v2 > 100",
                "LargeInPredicate is supported only as a conjunct of the predicate of an operator");
        assertLargeInException(
                "select * from t0 where case when v1 in (1, 2, 3, 4) then v2 else v3 end > 100",
                "LargeInPredicate is supported only as a conjunct of the predicate of an operator");

        // A LargeInPredicate out of a predicate
        assertLargeInException(
                "select v1 in (1, 2, 3, 4) from t0",
                "LargeInPredicate is supported only as a conjunct of the predicate of an operator, transformed 0 of 1");
        assertLargeInException(
                "select sum(case when v1 in (1, 2, 3, 4) then v2 else 0 end) from t0",
                "LargeInPredicate is supported only as a conjunct of the predicate of an operator, transformed 0 of 1");
        assertLargeInException(
                "select * from t0 join t1 on t0.v1 = t1.v4 and t0.v2 + t1.v5 in (1, 2, 3, 4)",
                "LargeInPredicate is supported only as a conjunct of the predicate of an operator, transformed 0 of 1");

        // A comparison type that RAW VALUES does not hold
        assertLargeInException(
                "select * from tall where te in (1.1, 2.2, 3.3, 4.4)",
                "LargeInPredicate does not support comparison type");
    }

    @Test
    public void testEqBaseType() throws Exception {
        String plan = getFragmentPlan("select * from test_all_type where t1c in ('a', '01', 'c', 'd')");
        assertContains(plan, "constant type: VARCHAR");
        assertContains(plan, "equal join conjunct: 12: cast = 11: const_value");

        String eqBaseType = connectContext.getSessionVariable().getCboEqBaseType();
        connectContext.getSessionVariable().setCboEqBaseType(SessionVariableConstants.DECIMAL);
        try {
            plan = getFragmentPlan("select * from test_all_type where t1c in ('01', '2', '3', '4')");
            assertContains(plan, "constant type: DECIMAL128(38,9)");
            assertContains(plan, "equal join conjunct: 12: cast = 11: const_value");

            // A constant that the InPredicate leaves to BE as a CAST
            assertLargeInException("select * from tall where tc in ('a', 'b', 'c', 'd')",
                    "does not fold to decimal(38, 9)");
        } finally {
            connectContext.getSessionVariable().setCboEqBaseType(eqBaseType);
        }
    }

    // LargeInConstants compares as the InPredicate with the same list after ImplicitCastRule and FoldConstantsRule:
    // same comparison type and values, or a LargeInPredicateException where the InPredicate keeps a CAST for BE or
    // compares in a type that RAW VALUES does not hold.
    @Test
    public void testConstantsAsInPredicate() throws Exception {
        List<Type> columnTypes = List.of(IntegerType.TINYINT, IntegerType.SMALLINT, IntegerType.INT,
                IntegerType.BIGINT, IntegerType.LARGEINT, TypeFactory.createVarcharType(20),
                TypeFactory.createCharType(10), TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 4),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 6), DateType.DATE, DateType.DATETIME,
                FloatType.FLOAT, FloatType.DOUBLE, BooleanType.BOOLEAN);
        List<List<LiteralExpr>> lists = List.of(
                List.of(new IntLiteral(1), new IntLiteral(2), new IntLiteral(127)),
                List.of(new IntLiteral(1), new IntLiteral(40000), new IntLiteral(3000000000L)),
                List.of(new IntLiteral(-1), new IntLiteral(0), new IntLiteral(-128)),
                List.of(new IntLiteral(20240102), new IntLiteral(20240103)),
                List.of(new DecimalLiteral("1.5"), new DecimalLiteral("-2.25"), new IntLiteral(3)),
                List.of(new DecimalLiteral("1.50"), new DecimalLiteral("2.00")),
                List.of(new DecimalLiteral("1234567.891"), new DecimalLiteral("0.001")),
                List.of(new DecimalLiteral("12345678901234567890.12"), new IntLiteral(1)),
                List.of(new LargeIntLiteral("20218840600116072801"), new IntLiteral(1)),
                List.of(new FloatLiteral("1.5"), new IntLiteral(2)),
                List.of(new StringLiteral("1"), new StringLiteral("2"), new StringLiteral("127")),
                List.of(new StringLiteral("01"), new StringLiteral("2")),
                List.of(new StringLiteral("-5"), new StringLiteral("1.5"), new StringLiteral("2.25")),
                List.of(new StringLiteral("a"), new StringLiteral("b")),
                List.of(new StringLiteral(" 7"), new StringLiteral("8 ")),
                List.of(new StringLiteral("2024-01-02"), new StringLiteral("20240103")),
                List.of(new StringLiteral("2024-01-02 03:04:05"), new StringLiteral("2024-01-02 03:04:05.000007")),
                List.of(new StringLiteral("2024-02-30"), new StringLiteral("2024-01-02")),
                List.of(new StringLiteral("true"), new StringLiteral("0")));

        String eqBaseType = connectContext.getSessionVariable().getCboEqBaseType();
        int resolved = 0;
        int refused = 0;
        try {
            for (String base : List.of(SessionVariableConstants.DECIMAL, SessionVariableConstants.VARCHAR,
                    SessionVariableConstants.DOUBLE)) {
                connectContext.getSessionVariable().setCboEqBaseType(base);
                for (Type columnType : columnTypes) {
                    for (List<LiteralExpr> list : lists) {
                        if (assertConstantsAsInPredicate(columnType, list, base)) {
                            resolved++;
                        } else {
                            refused++;
                        }
                    }
                }
            }
        } finally {
            connectContext.getSessionVariable().setCboEqBaseType(eqBaseType);
        }
        assertTrue(resolved > 0 && refused > 0, resolved + " resolved, " + refused + " refused");
    }

    private static boolean assertConstantsAsInPredicate(Type columnType, List<LiteralExpr> list, String base) {
        ColumnRefOperator column = new ColumnRefOperator(100, columnType, "c", true);
        List<ConstantOperator> constants = list.stream()
                .map(l -> ConstantOperator.createObject(l.getRealObjectValue(), l.getType()))
                .collect(Collectors.toList());
        String name = columnType.toSql() + " IN " + constants + " with " + base;

        List<ScalarOperator> children = Lists.newArrayList(column);
        children.addAll(constants);
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        ScalarOperator in = rewriter.rewrite(new InPredicateOperator(false, children),
                ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE);
        in = rewriter.rewrite(in, ScalarOperatorRewriter.FOLD_CONSTANT_RULES);
        assertTrue(in instanceof InPredicateOperator, name + ": " + in);
        Type comparisonType = in.getChild(0).getType();
        List<ScalarOperator> inConstants = in.getChildren().subList(1, in.getChildren().size());
        PrimitiveType primitiveType = comparisonType.getPrimitiveType();
        boolean held = primitiveType == PrimitiveType.TINYINT || primitiveType == PrimitiveType.SMALLINT
                || primitiveType == PrimitiveType.INT || primitiveType == PrimitiveType.BIGINT
                || primitiveType.isCharFamily() || primitiveType == PrimitiveType.DECIMAL32
                || primitiveType == PrimitiveType.DECIMAL64 || primitiveType == PrimitiveType.DECIMAL128
                || primitiveType == PrimitiveType.DATE || primitiveType == PrimitiveType.DATETIME;
        boolean folded = inConstants.stream().allMatch(ScalarOperator::isConstantRef);
        boolean allNull = inConstants.stream().allMatch(c -> c.isConstantRef() && ((ConstantOperator) c).isNull());

        LargeInConstants values;
        try {
            values = LargeInConstants.resolve(column, constants);
        } catch (LargeInPredicateException e) {
            assertTrue(!held || !folded || allNull, name + " is refused: " + e.getMessage() + ", IN is " + in);
            return false;
        }
        assertTrue(held && folded, name + " is resolved, IN is " + in);
        assertTrue(values.getType().matchesType(comparisonType), name + ": " + values.getType().toSql());

        List<Object> expected = new ArrayList<>();
        boolean hasNull = false;
        for (ScalarOperator operator : inConstants) {
            ConstantOperator constant = (ConstantOperator) operator;
            if (constant.isNull()) {
                hasNull = true;
            } else if (comparisonType.isIntegerType()) {
                expected.add(((Number) constant.getValue()).longValue());
            } else if (comparisonType.isStringType()) {
                expected.add(constant.getVarchar());
            } else if (comparisonType.isDecimalV3()) {
                expected.add(constant.getDecimal());
            } else {
                expected.add(constant.getDatetime());
            }
        }
        assertEquals(expected, values.getValues(), name);
        assertEquals(hasNull, values.hasNull(), name);
        return true;
    }

    @Test
    public void testNullConstant() throws Exception {
        String eqBaseType = connectContext.getSessionVariable().getCboEqBaseType();
        connectContext.getSessionVariable().setCboEqBaseType(SessionVariableConstants.DECIMAL);
        try {
            // 10^38 does not fit DECIMAL128(38,9) and folds to NULL, as in the InPredicate
            String sql = "select * from test_all_type where t1c %s ('01', '2', '3', '100000000000000000000000000000000000000')";
            String plan = getFragmentPlan(String.format(sql, "in"));
            assertContains(plan, "LEFT SEMI JOIN");
            assertContains(plan, "constant count: 3");

            // NOT IN with NULL is never true
            plan = getFragmentPlan(String.format(sql, "not in"));
            assertNotContains(plan, "RAW_VALUES");
            assertContains(plan, "EMPTYSET");
        } finally {
            connectContext.getSessionVariable().setCboEqBaseType(eqBaseType);
        }
    }

    @Test
    public void testConjunctPositions() throws Exception {
        // Every LargeInPredicate conjunct becomes a join
        String plan = getFragmentPlan("select * from t0 where v1 in (1, 2, 3, 4) and v2 in (10, 20, 30, 40)");
        assertEquals(2, StringUtils.countMatches(plan, "LEFT SEMI JOIN"));
        assertEquals(2, StringUtils.countMatches(plan, "RAW_VALUES"));

        // OR in another conjunct
        plan = getFragmentPlan("select * from t0 where v1 in (1, 2, 3, 4) and (v2 > 1 or v3 < 2)");
        assertContains(plan, "LEFT SEMI JOIN");
        assertContains(plan, "PREDICATES: (2: v2 > 1) OR (3: v3 < 2)");

        // NOT of IN is NOT IN
        assertLargeNotInTransformation("select * from t0 where not (v1 in (1, 2, 3, 4))");
        assertLargeInTransformation("select * from t0 where not (v1 not in (1, 2, 3, 4))");

        // A condition of a join on one side is pushed to its scan
        plan = getFragmentPlan("select * from t0 join t1 on t0.v1 = t1.v4 and t1.v5 in (1, 2, 3, 4)");
        assertContains(plan, "LEFT SEMI JOIN");
        assertContains(plan, "equal join conjunct: 5: v5 = 7: const_value");
    }

    @Test
    public void testJoinOverScan() throws Exception {
        // IN on the null side of an outer join rejects NULL, so the outer join is an inner join, and the
        // semi join is on the scan that the IN filters
        String plan = getFragmentPlan("select * from t0 left join t1 on t0.v1 = t1.v4 where t1.v5 in (1, 2, 3, 4)");
        assertContains(plan, "INNER JOIN");
        assertNotContains(plan, "LEFT OUTER JOIN");
        assertContains(plan, "equal join conjunct: 5: v5 = 7: const_value");

        plan = getFragmentPlan("select * from t0 left join t1 on t0.v1 = t1.v4 where t1.v5 not in (1, 2, 3, 4)");
        assertContains(plan, "INNER JOIN");
        assertContains(plan, "NULL AWARE LEFT ANTI JOIN");
    }

    @Test
    public void testFallbackScenarios() throws Exception {
        // Test small IN list (below threshold)
        connectContext.getSessionVariable().setLargeInPredicateThreshold(10);
        try {
            assertNoLargeInTransformation("select * from t0 where v1 in (1, 2)");
        } finally {
            connectContext.getSessionVariable().setLargeInPredicateThreshold(3);
        }
        
        // Test disabled feature
        connectContext.getSessionVariable().setEnableLargeInPredicate(false);
        try {
            String sql = "select * from t0 where v1 in (1, 2, 3, 4)";
            String plan = getFragmentPlan(sql);
            assertNotContains(plan, "LEFT SEMI JOIN");
            assertNotContains(plan, "RAW_VALUES");
            assertContains(plan, "PREDICATES");
        } finally {
            connectContext.getSessionVariable().setEnableLargeInPredicate(true);
        }
    }

    @Test
    public void testComplexScenarios() throws Exception {
        // Test nested subqueries
        assertLargeInTransformation("select * from t0 where v1 in (select v4 from t1 where v4 in (1, 2, 3, 4))");
        
        // Test multi-level JOINs
        String multiJoinSql = "select * from t0 join t1 on t0.v1 = t1.v4 join tall on t1.v5 = tall.tb" +
                " where t0.v1 in (1, 2, 3, 4)";
        String multiJoinPlan = getFragmentPlan(multiJoinSql);
        assertContains(multiJoinPlan, "LEFT SEMI JOIN");
        assertContains(multiJoinPlan, "RAW_VALUES");
        assertContains(multiJoinPlan, "INNER JOIN");
        
        // Test complex join conditions
        String complexJoinSql = "select * from t0 join t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5 where t0.v1 in (1, 2, 3, 4)";
        String complexJoinPlan = getFragmentPlan(complexJoinSql);
        assertContains(complexJoinPlan, "LEFT SEMI JOIN");
        assertContains(complexJoinPlan, "RAW_VALUES");
        assertContains(complexJoinPlan, "INNER JOIN");
        
        // Test window function with partitioning
        String windowPartitionSql = "select v1, v2, row_number() over (partition by v1 order by v2) from t0 " +
                "where v1 in (1, 2, 3, 4)";
        String windowPartitionPlan = getFragmentPlan(windowPartitionSql);
        assertContains(windowPartitionPlan, "LEFT SEMI JOIN");
        assertContains(windowPartitionPlan, "RAW_VALUES");
        assertContains(windowPartitionPlan, "ANALYTIC");
    }

    @Test
    public void testCardinality() throws Exception {
        String sql = "select * from t0 where v1 in (1, 2, 3, 4, 5, 6, 7, 8, 9)";
        String plan = getCostExplain(sql);
        assertContains(plan, "1:RAW_VALUES\n" +
                "     RAW VALUES\n" +
                "     constant count: 9\n" +
                "     constant type: BIGINT\n" +
                "     sample values: 1, 2, 3, 4, 5, 6, 7, 8, 9\n" +
                "     cardinality: 9\n" +
                "     column statistics: \n" +
                "     * const_value-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN");
    }

    @Test
    public void testRuntimeFilter() throws Exception {
        String sql = "select * from t0 where v1 in (1, 2, 3, 4)";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "4:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [1: v1, BIGINT, true] = [4: const_value, BIGINT, false]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (4: const_value), remote = false\n" +
                "  |  output columns: 1, 2, 3\n" +
                "  |  cardinality: 4");
        assertContains(plan, "0:OlapScanNode\n" +
                "     table: t0, rollup: t0\n" +
                "     preAggregation: on\n" +
                "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                "     tabletList=\n" +
                "     actualRows=0, avgRowSize=3.0\n" +
                "     cardinality: 1\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 0, probe_expr = (1: v1)");
    }

    @Test
    public void testPrint() throws Exception {
        String sql = "select * from t0 where v1 in (1, 2, 3, 4)";
        String plan = getLogicalFragmentPlan(sql);
        assertContains(plan, "RAW_VALUES(constantType=BIGINT, count=4, sample=1, 2, 3, 4)");
    }

    @Test
    public void testRawValuesOperatorMethods() {
        List<ColumnRefOperator> columnRefs1 = Lists.newArrayList(
                new ColumnRefOperator(1, IntegerType.BIGINT, "const_value", true)
        );
        List<ColumnRefOperator> columnRefs2 = Lists.newArrayList(
                new ColumnRefOperator(2, IntegerType.BIGINT, "const_value", true)
        );
        
        Type intType = IntegerType.BIGINT;
        Type stringType = VarcharType.VARCHAR;
        String rawText1 = "1, 2, 3, 4";
        String rawText2 = "5, 6, 7, 8";
        String longRawText = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25";
        
        List<Object> rawConstants1 = Lists.newArrayList(1L, 2L, 3L, 4L);
        List<Object> rawConstants2 = Lists.newArrayList(5L, 6L, 7L, 8L);
        List<Object> stringConstants = Lists.newArrayList("a", "b", "c", "d");
        
        // Test LogicalRawValuesOperator
        LogicalRawValuesOperator logical1 = new LogicalRawValuesOperator(columnRefs1, intType, rawText1, rawConstants1, 4);
        LogicalRawValuesOperator logical2 = new LogicalRawValuesOperator(columnRefs1, intType, rawText1, rawConstants1, 4);
        LogicalRawValuesOperator logical3 = new LogicalRawValuesOperator(columnRefs2, intType, rawText2, rawConstants2, 4);
        LogicalRawValuesOperator logical4 = new LogicalRawValuesOperator(columnRefs1, stringType, rawText1, stringConstants, 4);
        LogicalRawValuesOperator logicalLong = new LogicalRawValuesOperator(columnRefs1, intType, longRawText, rawConstants1, 4);
        
        // Test equals method
        assertEquals(logical1, logical2);
        assertNotEquals(logical1, logical3);
        assertNotEquals(logical1, logical4);
        assertNotEquals(null, logical1);
        assertNotEquals("not an operator", logical1);
        assertEquals(logical1, logical1); // self equality
        
        // Test hashCode method
        assertEquals(logical1.hashCode(), logical2.hashCode());
        assertNotEquals(logical1.hashCode(), logical3.hashCode());
        
        // Test toString method
        String toString1 = logical1.toString();
        assertContains(toString1, "LogicalRawValues");
        assertContains(toString1, "constantType=BIGINT");
        assertContains(toString1, "count=4");
        
        // Test long text truncation in toString
        String toStringLong = logicalLong.toString();
        assertContains(toStringLong, "...");
        
        // Test PhysicalRawValuesOperator
        PhysicalRawValuesOperator physical1 = new PhysicalRawValuesOperator(columnRefs1, intType, rawText1, rawConstants1, 4);
        PhysicalRawValuesOperator physical2 = new PhysicalRawValuesOperator(columnRefs1, intType, rawText1, rawConstants1, 4);
        PhysicalRawValuesOperator physical3 = new PhysicalRawValuesOperator(columnRefs2, intType, rawText2, rawConstants2, 4);
        PhysicalRawValuesOperator physical4 = new PhysicalRawValuesOperator(
                columnRefs1, stringType, rawText1, stringConstants, 4);
        PhysicalRawValuesOperator physicalLong = new PhysicalRawValuesOperator(
                columnRefs1, intType, longRawText, rawConstants1, 4);
        
        // Test equals method for PhysicalRawValuesOperator
        assertEquals(physical1, physical2);
        assertNotEquals(physical1, physical3);
        assertNotEquals(physical1, physical4);
        assertNotEquals(null, physical1);
        assertNotEquals("not an operator", physical1);
        assertEquals(physical1, physical1); // self equality
        
        // Test hashCode method for PhysicalRawValuesOperator
        assertEquals(physical1.hashCode(), physical2.hashCode());
        assertNotEquals(physical1.hashCode(), physical3.hashCode());
        
        // Test toString method for PhysicalRawValuesOperator
        String physicalToString1 = physical1.toString();
        assertContains(physicalToString1, "PhysicalRawValues");
        assertContains(physicalToString1, "constantType=BIGINT");
        assertContains(physicalToString1, "count=4");
        
        // Test long text truncation in toString for PhysicalRawValuesOperator
        String physicalToStringLong = physicalLong.toString();
        assertContains(physicalToStringLong, "...");
        
        // Test getter methods coverage
        assertEquals(columnRefs1, logical1.getColumnRefSet());
        assertEquals(intType, logical1.getConstantType());
        assertEquals(rawText1, logical1.getRawText());
        assertEquals(rawConstants1, logical1.getRawConstantList());
        assertEquals(4, logical1.getConstantCount());
        
        assertEquals(intType, physical1.getConstantType());
        assertEquals(rawText1, physical1.getRawText());
        assertEquals(rawConstants1, physical1.getRawConstantList());
        assertEquals(4, physical1.getConstantCount());
        assertEquals(columnRefs1, physical1.getColumnRefSet());
    }

    @Test
    public void testLargeInPredicateMethods() {

        Type intType = IntegerType.BIGINT;
        String rawText1 = "1, 2, 3, 4";
        String rawText2 = "5, 6, 7, 8";
        List<Object> rawConstants1 = Lists.newArrayList(1L, 2L, 3L, 4L);
        List<Object> rawConstants2 = Lists.newArrayList(5L, 6L, 7L, 8L);
        List<Object> stringConstants = Lists.newArrayList("a", "b", "c", "d");
        
        SlotRef slotRef1 = new SlotRef(null, "v1");
        SlotRef slotRef2 = new SlotRef(null, "v2");
        List<Expr> inList1 = Lists.newArrayList(new IntLiteral(1L, IntegerType.BIGINT));
        List<Expr> inList2 = Lists.newArrayList(new IntLiteral(2L, IntegerType.BIGINT));
        List<Expr> stringInList = Lists.newArrayList(new StringLiteral("a"));

        LargeInPredicate largeIn1 = new LargeInPredicate(slotRef1, rawText1, rawConstants1, 4, false, inList1, null);
        LargeInPredicate largeIn2 = new LargeInPredicate(slotRef1, rawText1, rawConstants1, 4, false, inList1, null);
        LargeInPredicate largeIn3 = new LargeInPredicate(slotRef2, rawText2, rawConstants2, 4, false, inList2, null);
        LargeInPredicate largeIn4 = new LargeInPredicate(slotRef1, rawText1, stringConstants, 4, true, stringInList, null);

        // Test LargeInPredicate equals method
        assertEquals(largeIn1, largeIn2);
        assertNotEquals(largeIn1, largeIn3);
        assertNotEquals(largeIn1, largeIn4);
        assertNotEquals(null, largeIn1);
        assertNotEquals("not a predicate", largeIn1);
        assertEquals(largeIn1, largeIn1);

        // Test LargeInPredicate hashCode method
        assertEquals(largeIn1.hashCode(), largeIn2.hashCode());
        assertNotEquals(largeIn1.hashCode(), largeIn3.hashCode());

        // Test LargeInPredicate toString method
        String largeInToString1 = largeIn1.toString();
        assertContains(largeInToString1, "LargeInPredicate");

        // Test LargeInPredicate getter methods
        assertEquals(rawText1, largeIn1.getRawText());
        assertEquals(rawConstants1, largeIn1.getRawConstantList());
        assertEquals(4, largeIn1.getConstantCount());
        assertEquals(4, largeIn1.getInElementNum());

        // Test LargeInPredicateOperator by directly creating instances
        ColumnRefOperator columnRef1 = new ColumnRefOperator(1, IntegerType.BIGINT, "v1", true);
        ColumnRefOperator columnRef2 = new ColumnRefOperator(2, IntegerType.BIGINT, "v2", true);
        
        List<ScalarOperator> children1 = Lists.newArrayList(columnRef1);
        List<ScalarOperator> children2 = Lists.newArrayList(columnRef2);
        
        LargeInConstants constants1 = LargeInConstants.resolve(columnRef1, toConstants(rawConstants1));
        LargeInConstants constants2 = LargeInConstants.resolve(columnRef2, toConstants(rawConstants2));
        LargeInPredicateOperator largeInOp1 = new LargeInPredicateOperator(rawText1, constants1, false, children1);
        LargeInPredicateOperator largeInOp2 = new LargeInPredicateOperator(rawText1, constants1, false, children1);
        LargeInPredicateOperator largeInOp3 = new LargeInPredicateOperator(rawText2, constants2, false, children2);
        LargeInPredicateOperator largeInOp4 = new LargeInPredicateOperator(rawText1, constants1, true, children1);

        // Test LargeInPredicateOperator equals method
        assertEquals(largeInOp1, largeInOp2);
        assertNotEquals(largeInOp1, largeInOp3);
        assertNotEquals(largeInOp1, largeInOp4);
        assertNotEquals(null, largeInOp1);
        assertEquals(largeInOp1, largeInOp1);

        // Test LargeInPredicateOperator hashCode method
        assertEquals(largeInOp1.hashCode(), largeInOp2.hashCode());
        assertNotEquals(largeInOp1.hashCode(), largeInOp3.hashCode());

        // Test LargeInPredicateOperator toString method
        String largeInOpToString1 = largeInOp1.toString();
        assertContains(largeInOpToString1, "v1 IN");

        // Test LargeInPredicateOperator getter methods
        assertEquals(rawText1, largeInOp1.getRawText());
        assertEquals(rawConstants1, largeInOp1.getConstants().getValues());
        assertEquals(4, largeInOp1.getConstantCount());
        assertEquals(intType, largeInOp1.getConstants().getType());
        assertFalse(largeInOp1.isNotIn());
    }

    private static List<ConstantOperator> toConstants(List<Object> values) {
        return values.stream().map(v -> ConstantOperator.createBigint((Long) v)).collect(Collectors.toList());
    }
}
