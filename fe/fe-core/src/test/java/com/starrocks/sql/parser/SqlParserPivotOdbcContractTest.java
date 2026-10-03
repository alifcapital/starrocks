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

package com.starrocks.sql.parser;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PivotAggregation;
import com.starrocks.sql.ast.PivotRelation;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.OdbcScalarFunctionCall;
import com.starrocks.sql.ast.expression.Parameter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlParserPivotOdbcContractTest {
    private ConnectContext previousContext;

    @BeforeEach
    void installParserContext() {
        previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setThreadLocalInfo();
    }

    @AfterEach
    void restoreParserContext() {
        if (previousContext == null) {
            ConnectContext.remove();
        } else {
            previousContext.setThreadLocalInfo();
        }
    }

    private static StatementBase parse(String sql) {
        SessionVariable session = new SessionVariable();
        session.setSqlMode(0);
        return SqlParser.parseOneWithStarRocksDialect(sql, session);
    }

    private static SelectRelation relation(String sql) {
        return (SelectRelation) ((QueryStatement) parse(sql)).getQueryRelation();
    }

    private static Expr expression(String sql) {
        return relation(sql).getSelectList().getItems().get(0).getExpr();
    }

    private static NodePosition span(String sql, String call) {
        int start = sql.indexOf(call);
        int end = start + call.lastIndexOf(')');
        assertTrue(start >= 0);
        int line = 1;
        int col = 0;
        int endLine = 1;
        int endCol = 0;
        for (int i = 0; i < end; i++) {
            if (i == start) {
                line = endLine;
                col = endCol;
            }
            if (sql.charAt(i) == '\n') {
                endLine++;
                endCol = 0;
            } else {
                endCol++;
            }
        }
        return new NodePosition(line, col, endLine, endCol);
    }

    private static void assertPosition(NodePosition expected, NodePosition actual) {
        assertEquals(List.of(expected.getLine(), expected.getCol(), expected.getEndLine(), expected.getEndCol()),
                List.of(actual.getLine(), actual.getCol(), actual.getEndLine(), actual.getEndCol()));
    }

    private static void assertCallError(String sql, String call, String detail) throws ReflectiveOperationException {
        ParsingException failure = assertThrows(ParsingException.class, () -> parse(sql));
        assertTrue(failure.getDetailMsg().contains(detail), failure.getMessage());
        Field position = ParsingException.class.getDeclaredField("pos");
        position.setAccessible(true);
        assertPosition(span(sql, call), (NodePosition) position.get(failure));
    }

    @Test
    void preservesPivotFunctionMeasureAliasAndSourceSpan() {
        String call = "sum(v)";
        String sql = "SELECT *\nFROM t PIVOT (" + call + " AS 'RawI' FOR k IN (1,2))";
        PivotRelation pivot = (PivotRelation) relation(sql).getRelation();
        assertEquals(1, pivot.getAggregateFunctions().size());
        assertEquals(2, pivot.getPivotValues().size());
        PivotAggregation measure = pivot.getAggregateFunctions().get(0);
        assertEquals("RawI", measure.getAlias());
        assertEquals("sum", measure.getFunctionCallExpr().getFunctionName());
        assertPosition(span(sql, call), measure.getFunctionCallExpr().getPos());
    }

    @Test
    void rejectsRewrittenPivotMeasuresWithSqlErrorsAtInnerCall() throws Exception {
        for (String call : List.of("ISNULL(v)", "TIMESTAMPADD(DAY,1,dt)", "MAP(1,2)", "USER()")) {
            String sql = "SELECT *\nFROM t PIVOT (" + call + " AS m FOR k IN (1))";
            assertCallError(sql, call, "Measure expression in PIVOT must use aggregate function");
        }
    }

    @Test
    void mapsInformationFunctionsWithoutLosingMultilineInnerCallSpan() {
        for (String call : List.of("USER()", "DATABASE()", "CURRENT_USER()", "`user`()", "db.user()")) {
            String sql = "SELECT\n {fn /*comment*/ " + call + "}";
            Expr result = expression(sql);
            assertTrue(result instanceof InformationFunction);
            assertPosition(span(sql, call), result.getPos());
        }
    }

    @Test
    void preservesLoweredArithmeticOperatorsAndCallSpans() {
        List<String> calls = List.of("bitand(6,3)", "bitor(4,1)", "`mod`(5,2)", "db.`mod`(5,2)");
        List<ArithmeticExpr.Operator> operators = List.of(ArithmeticExpr.Operator.BITAND,
                ArithmeticExpr.Operator.BITOR, ArithmeticExpr.Operator.MOD, ArithmeticExpr.Operator.MOD);
        for (int i = 0; i < calls.size(); i++) {
            String sql = "SELECT {fn " + calls.get(i) + "}";
            ArithmeticExpr result = (ArithmeticExpr) expression(sql);
            assertEquals(operators.get(i), result.getOp());
            assertPosition(span(sql, calls.get(i)), result.getPos());
        }
    }

    @Test
    void retainsInheritedMapperWhitelistAndNestedScalarCalls() throws Exception {
        Expr nested = expression("SELECT {fn ucase({fn lcase(a)})} FROM t");
        assertTrue(nested instanceof FunctionCallExpr);
        assertTrue(nested.getChild(0) instanceof FunctionCallExpr);
        for (String call : List.of("map(1,2)", "isnull(a)", "timestampadd(DAY,1,a)", "sum(a)")) {
            assertCallError("SELECT\n {fn " + call + "} FROM t", call, "Invalid odbc scalar function");
        }
    }

    @Test
    void rejectsOverBeforeMappingRewritesThatBypassWindowConstruction() throws Exception {
        for (String call : List.of("bitand(1,2) OVER ()", "`mod`(5,2) OVER ()",
                "db.bitand(1,2) OVER ()", "abs(1) OVER ()")) {
            assertCallError("SELECT {fn " + call + "}", call, "ODBC scalar functions do not support OVER");
        }
    }

    @Test
    void preservesPreparedArithmeticParameterIdentityAcrossRepeatedBindings() {
        PrepareStmt prepare = (PrepareStmt) parse("PREPARE p FROM SELECT {fn bitand(?,?)}");
        SelectRelation relation = (SelectRelation) ((QueryStatement) prepare.getInnerStmt()).getQueryRelation();
        ArithmeticExpr result = (ArithmeticExpr) relation.getSelectList().getItems().get(0).getExpr();
        assertEquals(2, prepare.getParameters().size());
        for (long base : List.of(100L, 200L)) {
            List<Expr> values = List.of(new IntLiteral(base), new IntLiteral(base + 1));
            prepare.assignValues(values);
            for (int i = 0; i < values.size(); i++) {
                Parameter parameter = (Parameter) result.getChild(i);
                assertSame(parameter, prepare.getParameters().get(i));
                assertEquals(i, parameter.getSlotId());
                assertSame(values.get(i), parameter.getExpr());
            }
        }
    }

    @Test
    void keepsConstructorCompatibilityAndExplicitErrorPosition() throws Exception {
        NodePosition inner = new NodePosition(2, 5, 2, 14);
        FunctionCallExpr call = new FunctionCallExpr("abs", List.of(new IntLiteral(1)), inner);
        OdbcScalarFunctionCall oldConstructor = new OdbcScalarFunctionCall(call);
        assertSame(call, oldConstructor.mappingFunction());
        assertSame(inner, oldConstructor.getPos());
        NodePosition explicit = new NodePosition(4, 8, 4, 20);
        Expr invalid = new ArithmeticExpr(ArithmeticExpr.Operator.ADD, new IntLiteral(1), new IntLiteral(2));
        OdbcScalarFunctionCall withPosition = new OdbcScalarFunctionCall(invalid, explicit);
        assertSame(explicit, withPosition.getPos());
        ParsingException failure = assertThrows(ParsingException.class, withPosition::mappingFunction);
        Field position = ParsingException.class.getDeclaredField("pos");
        position.setAccessible(true);
        assertSame(explicit, position.get(failure));
    }
}
