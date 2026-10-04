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


package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class SimplifiedPredicateRuleTest extends PlanTestBase {
    private static final ConstantOperator OI_NULL = ConstantOperator.createNull(IntegerType.INT);
    private static final ConstantOperator OI_100 = ConstantOperator.createInt(100);
    private static final ConstantOperator OI_200 = ConstantOperator.createInt(200);
    private static final ConstantOperator OI_300 = ConstantOperator.createInt(300);

    private static final ConstantOperator OB_FALSE = ConstantOperator.createBoolean(false);
    private static final ConstantOperator OB_TRUE = ConstantOperator.createBoolean(true);

    private SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

    @BeforeAll
    public static void beforeAll() throws Exception {
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `test_timestamp` (\n" +
                "  `id` bigint NULL COMMENT \"\",\n" +
                "  `ts` bigint NULL COMMENT \"unix timestamp\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test
    public void applyCaseWhen() {
        CaseWhenOperator cwo1 = new CaseWhenOperator(IntegerType.INT,
                new ColumnRefOperator(1, IntegerType.INT, "id", true), null,
                Lists.newArrayList(ConstantOperator.createInt(1), ConstantOperator.createVarchar("test"),
                        ConstantOperator.createInt(2), ConstantOperator.createVarchar("test2")));
        assertEquals(cwo1, rule.apply(cwo1, null));

        CaseWhenOperator cwo2 = new CaseWhenOperator(IntegerType.INT, ConstantOperator.createNull(BooleanType.BOOLEAN), null,
                Lists.newArrayList(ConstantOperator.createInt(1), ConstantOperator.createVarchar("test")));
        assertEquals(OI_NULL, rule.apply(cwo2, null));

        CaseWhenOperator cwo3 = new CaseWhenOperator(IntegerType.INT,
                ConstantOperator.createNull(BooleanType.BOOLEAN), OI_100,
                Lists.newArrayList(ConstantOperator.createInt(1), ConstantOperator.createVarchar("test")));
        assertEquals(OI_100, rule.apply(cwo3, null));

        CaseWhenOperator cwo4 = new CaseWhenOperator(IntegerType.INT, null, null,
                Lists.newArrayList(new ColumnRefOperator(1, BooleanType.BOOLEAN, "id", true), OI_200,
                        new ColumnRefOperator(2, BooleanType.BOOLEAN, "id", true), OI_100));
        assertEquals(cwo4, rule.apply(cwo4, null));

        CaseWhenOperator cwo5 = new CaseWhenOperator(IntegerType.INT, null, null,
                Lists.newArrayList(OB_FALSE, OI_200, OB_TRUE, OI_300));
        assertEquals(OI_300, rule.apply(cwo5, null));

        CaseWhenOperator cwo6 = new CaseWhenOperator(IntegerType.INT, null, null,
                Lists.newArrayList(OB_FALSE, OI_200, OI_NULL, OI_300));
        assertEquals(OI_NULL, rule.apply(cwo6, null));

        CaseWhenOperator cwo7 = new CaseWhenOperator(IntegerType.INT, null, OI_100,
                Lists.newArrayList(OB_FALSE, OI_200, OI_NULL, OI_300));
        assertEquals(OI_100, rule.apply(cwo7, null));
    }

    @Test
    public void applyLike() {
        SimplifiedPredicateRule rule = new SimplifiedPredicateRule();

        ScalarOperator operator = new LikePredicateOperator(new ColumnRefOperator(1, VarcharType.VARCHAR, "name", true),
                ConstantOperator.createVarchar("zxcv"));
        ScalarOperator result = rule.apply(operator, null);

        assertEquals(OperatorType.BINARY, result.getOpType());
        assertEquals(BinaryType.EQ, ((BinaryPredicateOperator) result).getBinaryType());
        assertEquals(ConstantOperator.createVarchar("zxcv"), result.getChild(1));

        operator = new LikePredicateOperator(new ColumnRefOperator(1, VarcharType.VARCHAR, "name", true),
                ConstantOperator.createVarchar("%zxcv"));
        result = rule.apply(operator, null);
        assertEquals(OperatorType.LIKE, result.getOpType());

        operator = new LikePredicateOperator(new ColumnRefOperator(1, VarcharType.VARCHAR, "name", true),
                ConstantOperator.createVarchar("_zxcv"));
        result = rule.apply(operator, null);
        assertEquals(OperatorType.LIKE, result.getOpType());

        // test for none-string right child
        operator = new LikePredicateOperator(new ColumnRefOperator(1, VarcharType.VARCHAR, "name", true),
                ConstantOperator.createBoolean(false));
        result = rule.apply(operator, null);
        assertEquals(OperatorType.LIKE, result.getOpType());

        // pattern with backslash escape should NOT be simplified to EQ,
        // because LIKE treats '\\' as an escaped literal '\', while EQ treats it as two backslashes
        operator = new LikePredicateOperator(new ColumnRefOperator(1, VarcharType.VARCHAR, "name", true),
                ConstantOperator.createVarchar("star\\\\"));
        result = rule.apply(operator, null);
        assertEquals(OperatorType.LIKE, result.getOpType());

        operator = new LikePredicateOperator(new ColumnRefOperator(1, VarcharType.VARCHAR, "name", true),
                ConstantOperator.createVarchar("abc\\\\def"));
        result = rule.apply(operator, null);
        assertEquals(OperatorType.LIKE, result.getOpType());
    }

    @Test
    public void applyHourFromUnixTime() throws Exception {
        starRocksAssert.query("SELECT hour(from_unixtime(ts)) FROM test_timestamp")
                .explainContains("hour_from_unixtime");

        starRocksAssert.query("SELECT hour(ts) FROM test_timestamp")
                .explainWithout("hour_from_unixtime");

        starRocksAssert.query("SELECT hour(from_unixtime(ts, '%Y-%m-%d %H:%i:%s')) FROM test_timestamp")
                .explainWithout("hour_from_unixtime");
    }

    @Test
    public void applyHourToDatetimeRewrite() throws Exception {
        starRocksAssert.query("SELECT hour(to_datetime(ts)) FROM test_timestamp")
                .explainContains("hour_from_unixtime");

        starRocksAssert.query("SELECT hour(to_datetime(ts, 0)) FROM test_timestamp")
                .explainContains("hour_from_unixtime");

        starRocksAssert.query("SELECT hour(to_datetime(ts, 3)) FROM test_timestamp")
                .explainContains("hour_from_unixtime", "/ 1000");

        starRocksAssert.query("SELECT hour(to_datetime(ts, 6)) FROM test_timestamp")
                .explainContains("hour_from_unixtime", "/ 1000000");

        starRocksAssert.query("SELECT hour(to_datetime(ts, 4)) FROM test_timestamp")
                .explainWithout("hour_from_unixtime");
    }

    @Test
    public void hourOverAnotherFunctionIsKept() throws Exception {
        // A function between hour() and the unix time conversion changes the hour, so the rewrite must not skip it.
        for (String sql : new String[] {
                "SELECT hour(hours_add(from_unixtime(ts), 5)) FROM test_timestamp",
                "SELECT hour(convert_tz(from_unixtime(ts), 'UTC', 'Asia/Shanghai')) FROM test_timestamp",
                "SELECT hour(hours_add(to_datetime(ts), 5)) FROM test_timestamp"}) {
            starRocksAssert.query(sql).explainWithout("hour_from_unixtime");
        }
    }

    private static ColumnRefOperator boolCol(int id) {
        return new ColumnRefOperator(id, BooleanType.BOOLEAN, "b" + id, true);
    }

    private static ColumnRefOperator intCol(int id) {
        return new ColumnRefOperator(id, IntegerType.INT, "i" + id, true);
    }

    @Test
    public void nullSafeEqualityOfSameVariableIsTrueOnlyForNullSafeEquality() {
        // x <=> x is TRUE also when x is NULL. x = x, x != x and the other comparisons are NULL for a NULL x,
        // so they must stay.
        ColumnRefOperator a = intCol(1);
        ScalarOperator sum = new CallOperator("add", IntegerType.INT, List.of(a, ConstantOperator.createInt(1)));
        for (ScalarOperator side : List.of(a, sum)) {
            assertEquals(ConstantOperator.createBoolean(true),
                    rule.apply(new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, side, side), null));
            for (BinaryType type : List.of(BinaryType.EQ, BinaryType.NE, BinaryType.LT, BinaryType.GE)) {
                BinaryPredicateOperator predicate = new BinaryPredicateOperator(type, side, side);
                assertSame(predicate, rule.apply(predicate, null));
            }
        }
        BinaryPredicateOperator different = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, a, intCol(2));
        assertSame(different, rule.apply(different, null));
        // A constant is not a variable, so it keeps its comparison.
        BinaryPredicateOperator constants = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        assertSame(constants, rule.apply(constants, null));
    }

    @Test
    public void caseWhenKeepsTheHashSetJudgeForCharAndVarchar() {
        // CHAR 'a' equals VARCHAR 'a' but hashes differently, so the values are not considered the same.
        ConstantOperator varcharA = ConstantOperator.createVarchar("a");
        ConstantOperator charA = ConstantOperator.createChar("a");
        CaseWhenOperator operator = new CaseWhenOperator(VarcharType.VARCHAR, null, charA,
                Lists.newArrayList(boolCol(1), varcharA));
        assertSame(operator, rule.simplifiedCaseWhenConstClause(operator));
    }

    @Test
    public void caseWhenRemovalDoesNotLeakIntoACopyOfTheOperator() {
        // The rule removes clauses in place. A copy made before must keep all of its clauses.
        CaseWhenOperator operator = new CaseWhenOperator(IntegerType.INT, null, ConstantOperator.createInt(4),
                Lists.newArrayList(boolCol(1), ConstantOperator.createInt(1), ConstantOperator.createBoolean(true),
                        ConstantOperator.createInt(2), boolCol(2), ConstantOperator.createInt(3)));
        CaseWhenOperator copy = new CaseWhenOperator(IntegerType.INT, operator);
        List<ScalarOperator> before = List.copyOf(copy.getChildren());

        // WHEN TRUE makes the following WHEN and the ELSE unreachable.
        assertSame(operator, rule.simplifiedCaseWhenConstClause(operator));
        assertEquals(1, operator.getWhenClauseSize());
        assertEquals(boolCol(1), operator.getWhenClause(0));
        assertEquals(ConstantOperator.createInt(1), operator.getThenClause(0));
        assertEquals(ConstantOperator.createInt(2), operator.getElseClause());

        assertEquals(before, copy.getChildren());
        assertEquals(3, copy.getWhenClauseSize());
        assertEquals(ConstantOperator.createInt(4), copy.getElseClause());
    }

    @Test
    public void caseWhenWithConstantCaseDropsNonMatchingConstantWhens() {
        ColumnRefOperator condition = intCol(1);
        CaseWhenOperator operator = new CaseWhenOperator(VarcharType.VARCHAR, ConstantOperator.createInt(1),
                ConstantOperator.createVarchar("z"),
                Lists.newArrayList(ConstantOperator.createInt(2), ConstantOperator.createVarchar("x"),
                        condition, ConstantOperator.createVarchar("y")));
        assertSame(operator, rule.simplifiedCaseWhenConstClause(operator));
        assertEquals(1, operator.getWhenClauseSize());
        assertSame(condition, operator.getWhenClause(0));
        assertEquals(ConstantOperator.createVarchar("y"), operator.getThenClause(0));
        assertEquals(ConstantOperator.createVarchar("z"), operator.getElseClause());
        assertEquals(ConstantOperator.createInt(1), operator.getCaseClause());
    }

    private static CallOperator shift(String name, ScalarOperator input, ScalarOperator amount) {
        return new CallOperator(name, DateType.DATE, Lists.newArrayList(input, amount), mock(Function.class));
    }

    @Test
    public void timeShiftsAreRejectedWithoutChangingTheCall() {
        ColumnRefOperator date = new ColumnRefOperator(1, DateType.DATE, "d", true);
        CallOperator inner = shift("days_add", date, ConstantOperator.createInt(1));

        CallOperator onColumn = shift("days_add", date, ConstantOperator.createInt(1));
        assertSame(onColumn, rule.apply(onColumn, null));

        CallOperator nonConstantAmount = shift("days_add", inner, intCol(2));
        assertSame(nonConstantAmount, rule.apply(nonConstantAmount, null));

        CallOperator bigintAmount = shift("days_add", inner, ConstantOperator.createBigint(1));
        assertSame(bigintAmount, rule.apply(bigintAmount, null));
    }

    @Test
    public void sameDirectionTimeShiftsAreMerged() {
        ColumnRefOperator date = new ColumnRefOperator(1, DateType.DATE, "d", true);
        // The merged call looks up its function. The shifts here use mocked functions, so the lookup is mocked too.
        try (MockedStatic<ExprUtils> ignored = mockStatic(ExprUtils.class)) {
            CallOperator add = shift("days_add", shift("days_add", date, ConstantOperator.createInt(5)),
                    ConstantOperator.createInt(2));
            ScalarOperator merged = rule.apply(add, null);
            assertEquals("days_add", ((CallOperator) merged).getFnName());
            assertEquals(List.of(date, ConstantOperator.createInt(7)), merged.getChildren());

            CallOperator sub = shift("hours_sub", shift("hours_sub", date, ConstantOperator.createInt(5)),
                    ConstantOperator.createInt(2));
            merged = rule.apply(sub, null);
            assertEquals("hours_sub", ((CallOperator) merged).getFnName());
            assertEquals(List.of(date, ConstantOperator.createInt(7)), merged.getChildren());

            // Shifts that cancel out leave the input.
            CallOperator cancelled = shift("seconds_add", shift("seconds_add", date, ConstantOperator.createInt(0)),
                    ConstantOperator.createInt(0));
            assertSame(date, rule.apply(cancelled, null));
        }
    }
}
