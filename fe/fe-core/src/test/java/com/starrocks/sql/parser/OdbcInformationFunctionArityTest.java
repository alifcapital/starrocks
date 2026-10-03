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
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.OdbcScalarFunctionCall;
import com.starrocks.sql.ast.expression.Parameter;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OdbcInformationFunctionArityTest {
    // These eleven forms were accepted before RB-017, with the argument subtree absent
    // from the resulting InformationFunction. Ordinary generic calls retain their arguments.
    private static final List<String> CHANGED_SQL = List.of(
            "SELECT {fn USER(1)}", "SELECT {fn USER(?)}",
            "SELECT {fn `USER`(1)}", "SELECT {fn `USER`(?)}",
            "SELECT {fn db.USER(1)}", "SELECT {fn db.USER(?)}",
            "SELECT {fn `DATABASE`(1)}", "SELECT {fn `DATABASE`(?)}",
            "SELECT {fn `CURRENT_USER`(1)}", "SELECT {fn `CURRENT_USER`(?)}",
            "PREPARE p FROM SELECT {fn USER(?)}, ?");

    private StatementBase statement(String sql, PredictionMode mode) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(SqlTextStream.create(sql)));
        lexer.setSqlMode(SqlModeHelper.MODE_DEFAULT);
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ErrorHandler());
        StarRocksParser parser = new StarRocksParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorHandler());
        parser.setErrorHandler(new StarRocksDefaultErrorStrategy());
        parser.getInterpreter().setPredictionMode(mode);
        PostProcessListener listener = new PostProcessListener(100_000, 100_000);
        parser.addParseListener(listener);
        StarRocksParser.SqlStatementsContext tree = parser.sqlStatements();
        listener.validateTupleContexts();
        var context = tree.singleStatement(0);
        AstBuilder builder = new AstBuilder(SqlModeHelper.MODE_DEFAULT, false, new IdentityHashMap<>());
        builder.initializeParameterContext(LexicalParameterContext.forRule(parser, context));
        return (StatementBase) builder.visitSingleStatement(context);
    }

    private Expr item(StatementBase statement, int index) {
        QueryStatement query = (QueryStatement) (statement instanceof PrepareStmt prepare ?
                prepare.getInnerStmt() : statement);
        return ((SelectRelation) query.getQueryRelation()).getSelectList().getItems().get(index).getExpr();
    }

    private void rejectsAtInnerCall(String sql, PredictionMode mode) {
        ParsingException failure = assertThrows(ParsingException.class, () -> statement(sql, mode));
        assertTrue(failure.getDetailMsg().startsWith("Invalid odbc scalar function"));
        int start = sql.indexOf("{fn ") + 4;
        int stop = sql.indexOf('}', start) - 1;
        assertTrue(failure.getMessage().contains("from line 1, column " + start +
                " to line 1, column " + stop), failure.getMessage());
    }

    @Test
    void rejectsAllPreviouslyDiscardedArgumentFormsAtTheInnerCall() {
        for (PredictionMode mode : new PredictionMode[] {PredictionMode.SLL, PredictionMode.LL}) {
            for (String sql : CHANGED_SQL) {
                rejectsAtInnerCall(sql, mode);
            }
            for (String name : List.of("db.`DATABASE`", "db.`CURRENT_USER`")) {
                rejectsAtInnerCall("SELECT {fn " + name + "(1)}", mode);
                rejectsAtInnerCall("SELECT {fn " + name + "(?)}", mode);
            }
        }
    }

    @Test
    void preservesZeroArgumentInformationCallsAndOrdinaryGenericArguments() {
        for (PredictionMode mode : new PredictionMode[] {PredictionMode.SLL, PredictionMode.LL}) {
            for (String name : List.of("USER", "`USER`", "db.USER", "DATABASE", "`DATABASE`",
                    "db.`DATABASE`", "CURRENT_USER", "`CURRENT_USER`", "db.`CURRENT_USER`")) {
                Expr result = item(statement("SELECT {fn " + name + "()}", mode), 0);
                assertInstanceOf(InformationFunction.class, result);
                assertEquals(0, result.getChildren().size());
            }
            for (String name : List.of("USER", "`USER`", "db.USER", "`DATABASE`",
                    "db.`DATABASE`", "`CURRENT_USER`", "db.`CURRENT_USER`")) {
                FunctionCallExpr literal = assertInstanceOf(FunctionCallExpr.class,
                        item(statement("SELECT " + name + "(1)", mode), 0));
                assertEquals(1, literal.getChildren().size());
                assertEquals(1, assertInstanceOf(IntLiteral.class, literal.getChild(0)).getLongValue());
                FunctionCallExpr parameter = assertInstanceOf(FunctionCallExpr.class,
                        item(statement("SELECT " + name + "(?)", mode), 0));
                assertEquals(0, assertInstanceOf(Parameter.class, parameter.getChild(0)).getSlotId());
            }
            // DATABASE and CURRENT_USER are reserved tokens, not unquoted qualified-name identifiers.
            for (String sql : List.of("SELECT {fn db.DATABASE()}", "SELECT {fn db.CURRENT_USER()}")) {
                assertThrows(ParsingException.class, () -> statement(sql, mode));
            }
        }
    }

    @Test
    void rejectsInvalidModifiersAndMetadataOnlyMapperArguments() {
        for (PredictionMode mode : new PredictionMode[] {PredictionMode.SLL, PredictionMode.LL}) {
            for (String name : List.of("USER", "`USER`", "db.USER")) {
                for (String arguments : List.of("*", "DISTINCT", "ALL", "ORDER BY 1",
                        "DISTINCT 1", "ALL 1")) {
                    // The original call-category check rejects these before ODBC mapping.
                    assertThrows(ParsingException.class,
                            () -> statement("SELECT " + name + "(" + arguments + ")", mode));
                    assertThrows(ParsingException.class,
                            () -> statement("SELECT {fn " + name + "(" + arguments + ")}", mode));
                }
            }
        }
        // Direct mapper callers must not bypass arity checking with an empty child list.
        for (FunctionParams parameters : List.of(FunctionParams.createStarParam(),
                new FunctionParams(true, new ArrayList<Expr>()))) {
            FunctionCallExpr call = new FunctionCallExpr("user", parameters, new NodePosition(1, 11, 1, 17));
            ParsingException failure = assertThrows(ParsingException.class,
                    () -> new OdbcScalarFunctionCall(call).mappingFunction());
            assertTrue(failure.getDetailMsg().startsWith("Invalid odbc scalar function"));
        }
    }

    @Test
    void cannotReturnAPreparedStatementWithADetachedOdbcParameter() {
        for (PredictionMode mode : new PredictionMode[] {PredictionMode.SLL, PredictionMode.LL}) {
            rejectsAtInnerCall("PREPARE p FROM SELECT {fn USER(?)}, ?", mode);
            PrepareStmt ordinary = assertInstanceOf(PrepareStmt.class,
                    statement("PREPARE p FROM SELECT USER(?), ?", mode));
            assertEquals(List.of(0, 1), ordinary.getParameters().stream().map(Parameter::getSlotId).toList());
            FunctionCallExpr call = assertInstanceOf(FunctionCallExpr.class, item(ordinary, 0));
            assertSame(ordinary.getParameters().get(0), call.getChild(0));
            assertSame(ordinary.getParameters().get(1), item(ordinary, 1));
            List<Expr> values = List.of(new IntLiteral(101), new IntLiteral(202));
            ordinary.assignValues(values);
            for (int index = 0; index < values.size(); index++) {
                assertSame(values.get(index), ordinary.getParameters().get(index).getExpr());
            }
            PrepareStmt validOdbc = assertInstanceOf(PrepareStmt.class,
                    statement("PREPARE p FROM SELECT {fn USER()}, ?", mode));
            assertInstanceOf(InformationFunction.class, item(validOdbc, 0));
            assertEquals(1, validOdbc.getParameters().size());
            assertSame(validOdbc.getParameters().get(0), item(validOdbc, 1));
        }
    }
}
