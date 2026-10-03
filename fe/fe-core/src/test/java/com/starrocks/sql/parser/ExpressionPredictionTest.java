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

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.MultiInPredicate;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.dfa.DFAState;
import org.junit.jupiter.api.Test;

import java.util.IdentityHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpressionPredictionTest {
    private StarRocksParser parser(String sql, PredictionMode mode) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        lexer.setSqlMode(SqlModeHelper.MODE_DEFAULT);
        StarRocksParser parser = new StarRocksParser(new CommonTokenStream(lexer));
        ATN atn = parser.getATN();
        DFA[] decisions = new DFA[atn.getNumberOfDecisions()];
        for (int i = 0; i < decisions.length; i++) {
            decisions[i] = new DFA(atn.getDecisionState(i), i);
        }
        // Use an independent cold cache so earlier tests cannot hide prediction growth.
        parser.setInterpreter(new ParserATNSimulator(parser, atn, decisions, new PredictionContextCache()));
        parser.getInterpreter().setPredictionMode(mode);
        parser.setErrorHandler(new BailErrorStrategy());
        return parser;
    }

    private AstBuilder builder() {
        return new AstBuilder(SqlModeHelper.MODE_DEFAULT, false, new IdentityHashMap<>());
    }

    private Expr expression(String sql, PredictionMode mode) {
        StarRocksParser parser = parser(sql, mode);
        return (Expr) builder().visit(parser.expressionSingleton().expression());
    }

    private long cachedConfigurations(int depth, PredictionMode mode) {
        String sql = "x";
        for (int i = 0; i < depth; i++) {
            sql = "CASE WHEN a AND (" + sql + ") THEN 1 ELSE 0 END";
        }
        StarRocksParser parser = parser(sql, mode);
        Expr expr = (Expr) builder().visit(parser.expressionSingleton().expression());
        assertTrue(expr.getChildren().size() > 0);
        long configurations = 0;
        for (DFA decision : parser.getInterpreter().decisionToDFA) {
            for (DFAState state : decision.states.values()) {
                configurations += state.configs.size();
            }
        }
        return configurations;
    }

    @Test
    void nestedExpressionsDoNotExpandThePredictionCache() {
        for (PredictionMode mode : new PredictionMode[] {PredictionMode.SLL, PredictionMode.LL}) {
            long shallow = cachedConfigurations(4, mode);
            long deep = cachedConfigurations(32, mode);
            // The token shapes are the same at each depth. The cache should describe
            // those shapes rather than retain a new prediction path for every nesting level.
            assertTrue(deep <= shallow + 1024, mode + ": shallow=" + shallow + ", deep=" + deep);
        }
    }

    @Test
    void groupedExpressionsAndTupleInKeepTheirMeaning() {
        for (PredictionMode mode : new PredictionMode[] {PredictionMode.SLL, PredictionMode.LL}) {
            assertEquals(expression("a + b * c", mode), expression("(a + (b * c))", mode));
            for (String operator : new String[] {"IN", "NOT IN"}) {
                Expr expr = expression("((a + 1), CASE WHEN b THEN c ELSE d END) " + operator +
                        " (SELECT x, y FROM t)", mode);
                MultiInPredicate in = assertInstanceOf(MultiInPredicate.class, expr);
                assertEquals(2, in.getNumberOfColumns());
                assertEquals(operator.equals("NOT IN"), in.isNotIn());
            }
        }
    }

    @Test
    void parenthesizedIntervalIsNotAFunctionWithAnImplicitAlias() {
        for (PredictionMode mode : new PredictionMode[] {PredictionMode.SLL, PredictionMode.LL}) {
            StarRocksParser parser = parser("SELECT d + INTERVAL (x + 1) DAY", mode);
            QueryStatement statement = (QueryStatement) builder().visit(parser.singleStatement());
            SelectListItem item = ((SelectRelation) statement.getQueryRelation()).getSelectList().getItems().get(0);
            TimestampArithmeticExpr expr = assertInstanceOf(TimestampArithmeticExpr.class, item.getExpr());
            assertEquals("DAY", expr.getTimeUnitIdent());
            assertNull(item.getAlias());

            Expr add = expression("date_add(d, INTERVAL (-1) MINUTE)", mode);
            assertEquals("MINUTE", assertInstanceOf(TimestampArithmeticExpr.class, add).getTimeUnitIdent());
            assertInstanceOf(FunctionCallExpr.class, expression("interval(x)", mode));
            assertInstanceOf(FunctionCallExpr.class, expression("`interval`(x)", mode));
        }
    }

    @Test
    void temporalBoundsAcceptNestedExpressionsWithoutConsumingTheSeparator() {
        String sql = "SELECT * FROM t FOR SYSTEM_TIME BETWEEN " +
                "(NOW() - INTERVAL (CASE WHEN a AND b THEN 1 ELSE 2 END) DAY) AND NOW()";
        for (PredictionMode mode : new PredictionMode[] {PredictionMode.SLL, PredictionMode.LL}) {
            StarRocksParser parser = parser(sql, mode);
            assertInstanceOf(QueryStatement.class, builder().visit(parser.singleStatement()));
        }
    }

    @Test
    void timestampArithmeticKeepsAllUnitKeywordsAndArgumentOrder() {
        for (PredictionMode mode : new PredictionMode[] {PredictionMode.SLL, PredictionMode.LL}) {
            for (String function : new String[] {"timestampadd", "timestampdiff"}) {
                for (String unit : new String[] {"YEAR", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE",
                        "SECOND", "QUARTER", "MILLISECOND", "MICROSECOND"}) {
                    // The last two units are reserved keywords, so a generic expression
                    // argument cannot replace the grammar's unitIdentifier argument.
                    TimestampArithmeticExpr result = assertInstanceOf(TimestampArithmeticExpr.class,
                            expression(function + " /* head */ ( /* unit */ " + unit + ", 7, d)", mode));
                    assertEquals(function, result.getFuncName());
                    assertEquals(unit, result.getTimeUnitIdent());
                    assertEquals(expression("d", mode), result.getChild(0));
                    assertEquals(expression("7", mode), result.getChild(1));
                }
            }
        }
    }

    @Test
    void timestampCallsKeepTheirOverClauseForSemanticAnalysis() {
        for (PredictionMode mode : new PredictionMode[] {PredictionMode.SLL, PredictionMode.LL}) {
            for (String function : new String[] {"timestampadd", "timestampdiff"}) {
                // Parsing must retain OVER even when the analyzer may later reject
                // the function as non-analytic. Dropping it changes the user's SQL.
                AnalyticExpr result = assertInstanceOf(AnalyticExpr.class,
                        expression(function + "(DAY, 7, d) OVER (ORDER BY d)", mode));
                assertEquals(function, result.getFnCall().getFnName().toString());
                assertEquals(1, result.getOrderByElements().size());
                assertEquals(3, result.getFnCall().getChildren().size());
            }
        }
    }

    @Test
    void passwordFoldingOnlyAppliesToItsOriginalStringSyntax() {
        for (PredictionMode mode : new PredictionMode[] {PredictionMode.SLL, PredictionMode.LL}) {
            assertInstanceOf(StringLiteral.class, expression("PASSWORD('value')", mode));
            AnalyticExpr result = assertInstanceOf(AnalyticExpr.class,
                    expression("PASSWORD('value') OVER (ORDER BY d)", mode));
            assertEquals("PASSWORD", result.getFnCall().getFnName().toString());
            assertEquals(1, result.getOrderByElements().size());
            // COLLATE is discarded by its own AST visitor, but that does not
            // make the original argument match PASSWORD's special string syntax.
            assertInstanceOf(FunctionCallExpr.class,
                    expression("PASSWORD('value' COLLATE utf8_general_ci)", mode));
        }
    }

    @Test
    void reservedSpecialCallsKeepTheirOriginalSyntaxRestrictions() {
        for (PredictionMode mode : new PredictionMode[] {PredictionMode.SLL, PredictionMode.LL}) {
            for (String call : new String[] {"CHAR(65)", "IF(a, b, c)", "LEFT(a, 1)", "LIKE(a, b)",
                    "MOD(a, b)", "REGEXP(a, b)", "REPLACE(a, b, c)", "RIGHT(a, 1)", "RLIKE(a, b)"}) {
                assertInstanceOf(FunctionCallExpr.class, expression(call, mode));
                // These reserved names had no generic OVER syntax in the original grammar.
                assertThrows(RuntimeException.class, () -> expression(call + " OVER ()", mode));
            }
            for (String call : new String[] {"CHAR()", "CHAR(a, b)", "LEFT(a)", "LIKE(a)",
                    "MOD(a)", "REGEXP(a)", "RIGHT(a)", "RLIKE(a)"}) {
                assertThrows(RuntimeException.class, () -> expression(call, mode));
            }
            assertInstanceOf(AnalyticExpr.class, expression("`left`(a, 1) OVER ()", mode));
            assertInstanceOf(AnalyticExpr.class, expression("minute(a) OVER ()", mode));
        }
    }
}
