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
import com.starrocks.sql.ast.AbstractBackupStmt;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRef;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.junit.jupiter.api.Test;

import java.util.IdentityHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackupRestorePartitionSelectorTest {
    private static final List<String> COMMANDS = List.of("BACKUP SNAPSHOT db.snap TO repo",
            "RESTORE SNAPSHOT db.snap FROM repo");
    private static final List<String> OBJECT_PREFIXES = List.of("", "TABLE ", "TABLES ");
    private static final PredictionMode[] MODES = {PredictionMode.SLL, PredictionMode.LL};
    private static final String KEY_ERROR = "Key partition selectors are not supported for BACKUP or RESTORE";

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

    private TableRef table(String sql, PredictionMode mode) {
        return ((AbstractBackupStmt) statement(sql, mode)).getTableRefs().get(0);
    }

    @Test
    void retainsWholeTableAndRestoreAlias() {
        for (PredictionMode mode : MODES) {
            for (String command : COMMANDS) {
                for (String prefix : OBJECT_PREFIXES) {
                    String alias = command.startsWith("RESTORE") ? " AS alias_t" : "";
                    TableRef table = table(command + " ON (" + prefix + "t" + alias + ")", mode);
                    assertNull(table.getPartitionRef());
                    assertEquals(alias.isEmpty() ? "t" : "alias_t", table.getAlias());
                }
            }
        }
    }

    @Test
    void retainsNamedTemporaryAndClausePositions() {
        for (PredictionMode mode : MODES) {
            for (String command : COMMANDS) {
                for (String prefix : OBJECT_PREFIXES) {
                    for (boolean temporary : new boolean[] {false, true}) {
                        String sql = command + " ON (" + prefix + "t " +
                                (temporary ? "TEMPORARY " : "") + "PARTITION(p1,p2))";
                        PartitionRef selector = table(sql, mode).getPartitionRef();
                        assertEquals(List.of("p1", "p2"), selector.getPartitionNames());
                        assertEquals(temporary, selector.isTemp());
                        assertFalse(selector.isKeyPartitionNames());
                        assertTrue(selector.getPartitionColNames().isEmpty());
                        assertTrue(selector.getPartitionColValues().isEmpty());
                        assertEquals(sql.indexOf(temporary ? "TEMPORARY" : "PARTITION"), selector.getPos().getCol());
                        assertEquals(sql.length() - 2, selector.getPos().getEndCol());
                    }
                }
            }
        }
    }

    @Test
    void rejectsLiteralNullParameterAndIntervalKeys() {
        for (PredictionMode mode : MODES) {
            for (String command : COMMANDS) {
                for (String prefix : OBJECT_PREFIXES) {
                    for (String selector : List.of("k=1", "k=1,m=2", "k=NULL", "k=?", "k=INTERVAL 1 DAY")) {
                        String sql = command + " ON (" + prefix + "t PARTITION(" + selector + "))";
                        ParsingException failure = assertThrows(ParsingException.class, () -> statement(sql, mode));
                        assertEquals(KEY_ERROR, failure.getDetailMsg());
                        // visitKeyPartitionList currently supplies NodePosition.ZERO.
                        assertEquals("Getting syntax error. Detail message: " + KEY_ERROR + ".", failure.getMessage());
                    }
                }
            }
        }
    }

    @Test
    void preservesBackupAliasRejectionBeforeSelector() {
        for (PredictionMode mode : MODES) {
            for (String prefix : OBJECT_PREFIXES) {
                String sql = "BACKUP SNAPSHOT db.snap TO repo ON (" + prefix + "t PARTITION(k=1) AS alias_t)";
                ParsingException failure = assertThrows(ParsingException.class, () -> statement(sql, mode));
                assertFalse(failure.getDetailMsg().contains(KEY_ERROR));
                assertTrue(failure.getDetailMsg().toLowerCase(java.util.Locale.ROOT).contains("alias"));
            }
        }
    }

    @Test
    void preservesMalformedTailBeforeSelector() {
        for (PredictionMode mode : MODES) {
            for (String command : COMMANDS) {
                String sql = command + " ON (t PARTITION(k=1)) garbage";
                ParsingException failure = assertThrows(ParsingException.class, () -> statement(sql, mode));
                assertFalse(failure.getDetailMsg().contains(KEY_ERROR));
            }
        }
    }

    @Test
    void preservesInvalidDateConstructionBeforeSelector() {
        for (PredictionMode mode : MODES) {
            for (String command : COMMANDS) {
                String sql = command + " ON (t PARTITION(k=DATE 'bad'))";
                ParsingException failure = assertThrows(ParsingException.class, () -> statement(sql, mode));
                assertEquals("Invalid date literal bad", failure.getDetailMsg());
            }
        }
    }

    @Test
    void preservesConflictingDatabaseBeforeSelector() {
        for (PredictionMode mode : MODES) {
            for (String sql : List.of("BACKUP DATABASE d SNAPSHOT other.snap TO repo ON (t PARTITION(k=1))",
                    "RESTORE SNAPSHOT other.snap FROM repo DATABASE d ON (t PARTITION(k=1))")) {
                ParsingException failure = assertThrows(ParsingException.class, () -> statement(sql, mode));
                assertFalse(failure.getDetailMsg().contains(KEY_ERROR));
            }
        }
    }
}
