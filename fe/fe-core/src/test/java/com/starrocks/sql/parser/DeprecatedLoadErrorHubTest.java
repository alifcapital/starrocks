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

import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.StatementBase;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.junit.jupiter.api.Test;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeprecatedLoadErrorHubTest {
    private static final String MESSAGE = "SET LOAD ERRORS HUB is no longer supported";
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
        var tree = parser.sqlStatements();
        listener.validateTupleContexts();
        var context = tree.singleStatement(0);
        AstBuilder builder = new AstBuilder(SqlModeHelper.MODE_DEFAULT, false, new IdentityHashMap<>());
        builder.initializeParameterContext(LexicalParameterContext.forRule(parser, context));
        return (StatementBase) builder.visitSingleStatement(context);
    }
    @Test
    void publicAndBothPredictionModesRejectDeprecatedClause() {
        Locale saved = Locale.getDefault();
        try {
            for (Locale locale : List.of(Locale.ENGLISH, Locale.forLanguageTag("tr-TR"))) {
                Locale.setDefault(locale);
                for (String root : List.of("ALTER TABLE t ", "ALTER SYSTEM ")) {
                    for (String suffix : List.of("", " PROPERTIES('K'='v')")) {
                        String sql = root + "SET LOAD ERRORS HUB" + suffix;
                        ParsingException publicError = assertThrows(ParsingException.class,
                                () -> SqlParser.parse(sql, new SessionVariable()));
                        assertEquals(MESSAGE, publicError.getDetailMsg());
                        for (PredictionMode mode : List.of(PredictionMode.SLL, PredictionMode.LL)) {
                            ParsingException error = assertThrows(ParsingException.class, () -> statement(sql, mode));
                            assertEquals(MESSAGE, error.getDetailMsg());
                            assertTrue(error.getMessage().contains("Getting syntax error"));
                        }
                    }
                }
            }
        } finally {
            Locale.setDefault(saved);
        }
    }
    @Test
    void malformedTailPrecedesDeprecatedDiagnosticAndOrdinarySetRemainsAccepted() {
        for (PredictionMode mode : List.of(PredictionMode.SLL, PredictionMode.LL)) {
            ParsingException error = assertThrows(ParsingException.class,
                    () -> statement("ALTER TABLE t SET LOAD ERRORS HUB junk", mode));
            assertNotEquals(MESSAGE, error.getDetailMsg());
            assertNotNull(statement("ALTER TABLE t SET ('K'='v')", mode));
        }
    }
}
