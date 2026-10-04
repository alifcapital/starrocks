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
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.IntLiteral;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlParserAggregateContractTest {
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

    private static SessionVariable session() {
        SessionVariable session = new SessionVariable();
        session.setSqlMode(0);
        return session;
    }

    private static StatementBase parse(String sql) {
        return SqlParser.parseOneWithStarRocksDialect(sql, session());
    }

    private static FunctionCallExpr function(String sql) {
        Expr expression = ((SelectRelation) ((QueryStatement) parse(sql)).getQueryRelation())
                .getSelectList().getItems().get(0).getExpr();
        return expression instanceof AnalyticExpr analytic ? analytic.getFnCall() : (FunctionCallExpr) expression;
    }

    private static Object rawOrder(FunctionCallExpr function) throws ReflectiveOperationException {
        // The getter turns an empty list into null, hiding aggregate versus generic constructor choice.
        Field field = FunctionParams.class.getDeclaredField("orderByElements");
        field.setAccessible(true);
        return field.get(function.getParams());
    }

    private static Object invalidCalls(PostProcessListener listener) throws ReflectiveOperationException {
        Field field = PostProcessListener.class.getDeclaredField("invalidCallContexts");
        field.setAccessible(true);
        return field.get(listener);
    }

    private static StarRocksParser parser(String sql, PostProcessListener listener) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(SqlTextStream.create(sql)));
        lexer.setSqlMode(0);
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ErrorHandler());
        StarRocksParser parser = new StarRocksParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorHandler());
        parser.removeParseListeners();
        parser.addParseListener(listener);
        return parser;
    }

    @Test
    void restoresEmptyAllCountAndGenericArrayAggDistinctIncludingWindows() throws Exception {
        for (String suffix : List.of("", " OVER ()")) {
            FunctionCallExpr count = function("SELECT COUNT(ALL)" + suffix);
            assertEquals("count", count.getFunctionName());
            assertEquals(0, count.getChildren().size());
            assertFalse(count.isDistinct());
            assertNotNull(rawOrder(count));

            FunctionCallExpr array = function("SELECT ARRAY_AGG_DISTINCT()" + suffix);
            assertEquals("array_agg_distinct", array.getFunctionName());
            assertEquals(0, array.getChildren().size());
            assertFalse(array.isDistinct());
            assertNull(rawOrder(array));
        }
    }

    @Test
    void retainsGenericAndAggregateConstructorCategoriesByArity() throws Exception {
        for (String head : List.of("AVG", "MIN", "MAX", "SUM", "ARRAY_AGG", "ARRAY_AGG_DISTINCT")) {
            String genericName = head.toLowerCase(Locale.ROOT);
            for (String args : List.of("", "1,2")) {
                FunctionCallExpr generic = function("SELECT " + head + "(" + args + ")");
                assertEquals(genericName, generic.getFunctionName());
                assertFalse(generic.isDistinct());
                assertNull(rawOrder(generic));
            }
            FunctionCallExpr aggregate = function("SELECT " + head + "(1)");
            assertEquals(head.equals("ARRAY_AGG_DISTINCT") ? "array_agg" : genericName, aggregate.getFunctionName());
            assertEquals(head.equals("ARRAY_AGG_DISTINCT"), aggregate.isDistinct());
            assertNotNull(rawOrder(aggregate));
        }
        assertNull(rawOrder(function("SELECT GROUP_CONCAT()")));
        assertNotNull(rawOrder(function("SELECT GROUP_CONCAT(1,2)")));
        assertNull(rawOrder(function("SELECT `AVG`(1)")));
        assertNull(rawOrder(function("SELECT db.AVG(1)")));
    }

    @Test
    void rejectsQuantifiedMultipleArgumentsInsteadOfExtendingAggregateGrammar() {
        for (String head : List.of("AVG", "MIN", "MAX", "SUM", "ARRAY_AGG")) {
            for (String quantifier : List.of("ALL", "DISTINCT")) {
                ParsingException failure = assertThrows(ParsingException.class,
                        () -> parse("SELECT " + head + "(" + quantifier + " 1,2)"));
                assertTrue(failure.getMessage().contains("original aggregate/generic contract"));
            }
        }
        assertThrows(ParsingException.class, () -> parse("SELECT ARRAY_AGG_DISTINCT(ALL 1)"));
        assertThrows(ParsingException.class, () -> parse("SELECT SUM(*)"));
        assertThrows(ParsingException.class, () -> parse("SELECT db.SUM(DISTINCT 1)"));
    }

    @Test
    void preservesCountHintsArrayArgumentsAndAggregateOrdering() {
        assertEquals(1, function("SELECT COUNT(DISTINCT [skew] a)").getChildren().size());
        assertEquals(1, function("SELECT COUNT(ALL [1,2])").getChildren().size());
        assertEquals(1, function("SELECT ARRAY_AGG([1,2])").getChildren().size());
        FunctionCallExpr array = function("SELECT ARRAY_AGG(a ORDER BY b)");
        assertEquals(1, array.getParams().getOrderByElements().size());
        assertThrows(ParsingException.class, () -> parse("SELECT MIN(a ORDER BY b)"));
        assertThrows(ParsingException.class, () -> parse("SELECT COUNT(a SEPARATOR ',')"));
    }

    @Test
    void validatesDiscardedSubtreesAndSyntaxBeforeHintInterpretation() {
        for (String sql : List.of("SELECT a FROM (SELECT a ORDER BY AVG(ALL 1,2)) q",
                "SELECT /*+ SET_VAR(sql_mode='bogus') */ AVG(ALL 1,2)")) {
            ParsingException failure = assertThrows(ParsingException.class, () -> parse(sql));
            assertTrue(failure.getMessage().contains("original aggregate/generic contract"));
        }
    }

    @Test
    void doesNotAllocateInvalidCollectionForValidCallsAndResetsAbandonedState() throws Exception {
        PostProcessListener listener = new PostProcessListener(10000, 10000);
        StarRocksParser parser = parser("SELECT AVG(1), COUNT(ALL), SUM(1,2)", listener);
        parser.sqlStatements();
        listener.validateTupleContexts();
        assertNull(invalidCalls(listener));

        parser = parser("SELECT AVG(ALL 1,2), SUM(1), MAX(DISTINCT 3,4)", listener);
        parser.sqlStatements();
        assertEquals(2, ((List<?>) invalidCalls(listener)).size());
        assertThrows(ParsingException.class, listener::validateTupleContexts);
        listener.resetTupleContexts();
        assertNull(invalidCalls(listener));

        parser = parser("SELECT AVG(ALL 1,2), f(", listener);
        parser.setErrorHandler(new StarRocksBailErrorStrategy());
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        StarRocksParser abandoned = parser;
        RuntimeException failure = assertThrows(RuntimeException.class, abandoned::sqlStatements);
        assertTrue(failure instanceof ParseCancellationException || failure instanceof ParsingException);
        assertNotNull(invalidCalls(listener));
        listener.resetTupleContexts();
        StarRocksParser retry = parser("SELECT AVG(1)", listener);
        retry.setErrorHandler(new StarRocksDefaultErrorStrategy());
        retry.getInterpreter().setPredictionMode(PredictionMode.LL);
        retry.sqlStatements();
        listener.validateTupleContexts();
        assertNull(invalidCalls(listener));
    }

    @Test
    void reportsMalformedCallsWithoutLeakingIncompleteCallbackFailures() {
        for (String sql : List.of("SELECT AVG(", "SELECT AVG(ALL", "SELECT f(",
                "SELECT COUNT(DISTINCT [skew]", "SELECT AVG(ALL 1,2), f(")) {
            assertThrows(ParsingException.class, () -> parse(sql));
        }
        assertEquals(AggregateCallSyntax.GENERIC,
                AggregateCallSyntax.classify(new StarRocksParser.SimpleFunctionCallContext(
                        new StarRocksParser.FunctionCallContext(null, 0))));
    }

    @Test
    void preservesLexicalBindingIdentityAcrossAggregateVisitOrder() {
        for (String order : List.of("?", "b + ?")) {
            PrepareStmt prepare = (PrepareStmt) parse("SELECT ARRAY_AGG(? ORDER BY " + order + ") ORDER BY ?");
            SelectRelation relation = (SelectRelation) ((QueryStatement) prepare.getInnerStmt()).getQueryRelation();
            FunctionCallExpr aggregate = (FunctionCallExpr) relation.getSelectList().getItems().get(0).getExpr();
            assertEquals(3, prepare.getParameters().size());
            assertSame(aggregate.getChild(0), prepare.getParameters().get(0));
            if (order.equals("?")) {
                // Constant aggregate ORDER items disappear from the AST, but keep a binding slot.
                assertNull(aggregate.getParams().getOrderByElements());
                assertEquals(0, aggregate.getParams().getOrderByElemNum());
            } else {
                assertSame(aggregate.getParams().getOrderByElements().get(0).getExpr().getChild(1),
                        prepare.getParameters().get(1));
            }
            assertSame(relation.getOrderBy().get(0).getExpr(), prepare.getParameters().get(2));
            List<Expr> markers = List.of(new IntLiteral(11), new IntLiteral(22), new IntLiteral(33));
            prepare.assignValues(markers);
            for (int i = 0; i < markers.size(); i++) {
                assertEquals(i, prepare.getParameters().get(i).getSlotId());
                assertSame(markers.get(i), prepare.getParameters().get(i).getExpr());
            }
        }
    }
}
