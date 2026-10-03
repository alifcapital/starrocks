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
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.StatementBase;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.junit.jupiter.api.Test;

import java.util.IdentityHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdminRepairPartitionSelectorTest {
    private static final List<String> COMMANDS = List.of("ADMIN REPAIR TABLE", "ADMIN CANCEL REPAIR TABLE");
    private static final PredictionMode[] MODES = {PredictionMode.SLL, PredictionMode.LL};

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

    private PartitionRef partition(StatementBase statement) {
        return statement instanceof AdminRepairTableStmt repair ? repair.getPartitionRef() :
                ((AdminCancelRepairTableStmt) statement).getPartitionRef();
    }

    private void rejectsKey(String command, String selector, PredictionMode mode) {
        String sql = command + " t " + selector;
        ParsingException error = assertThrows(ParsingException.class, () -> statement(sql, mode));
        assertEquals("Key partition selectors are not supported for " + command, error.getDetailMsg());
        // visitKeyPartitionList currently creates a selector with NodePosition.ZERO.
        assertEquals("Getting syntax error. Detail message: Key partition selectors are not supported for " +
                command + ".", error.getMessage());
    }

    @Test
    void retainsWholeTableRepairAndCancellationWithoutSelector() {
        for (PredictionMode mode : MODES) {
            for (String command : COMMANDS) {
                assertNull(partition(statement(command + " t", mode)));
            }
        }
    }

    @Test
    void retainsNamedPartitionAstAndClausePositions() {
        for (PredictionMode mode : MODES) {
            for (String command : COMMANDS) {
                String sql = command + " t PARTITION(p1,p2)";
                PartitionRef selector = partition(statement(sql, mode));
                assertEquals(List.of("p1", "p2"), selector.getPartitionNames());
                assertFalse(selector.isKeyPartitionNames());
                assertFalse(selector.isTemp());
                assertEquals(sql.indexOf("PARTITION"), selector.getPos().getCol());
                assertEquals(sql.length() - 1, selector.getPos().getEndCol());
            }
        }
    }

    @Test
    void retainsTemporaryNamedPartitions() {
        for (PredictionMode mode : MODES) {
            for (String command : COMMANDS) {
                PartitionRef selector = partition(statement(command + " t TEMPORARY PARTITIONS p1,p2", mode));
                assertEquals(List.of("p1", "p2"), selector.getPartitionNames());
                assertTrue(selector.isTemp());
                assertFalse(selector.isKeyPartitionNames());
            }
        }
    }

    @Test
    void rejectsSingleAndMultipleKeySelectorsForBothCommands() {
        for (PredictionMode mode : MODES) {
            for (String command : COMMANDS) {
                rejectsKey(command, "PARTITION(k=1)", mode);
                rejectsKey(command, "PARTITION(k=1,k2=2)", mode);
            }
        }
    }

    @Test
    void rejectsParameterKeySelectorsBeforeLosingTheirValues() {
        for (PredictionMode mode : MODES) {
            for (String command : COMMANDS) {
                rejectsKey(command, "PARTITION(k=?)", mode);
            }
        }
    }

    @Test
    void malformedTailWinsOverKeySelectorValidation() {
        for (PredictionMode mode : MODES) {
            for (String command : COMMANDS) {
                ParsingException error = assertThrows(ParsingException.class,
                        () -> statement(command + " t PARTITION(k=1) garbage", mode));
                assertNotEquals("Key partition selectors are not supported for " + command, error.getDetailMsg());
                assertTrue(error.getDetailMsg().contains("garbage"), error.getDetailMsg());
            }
        }
    }

    @Test
    void keyValueConstructorFailurePrecedesUnsupportedSelectorGuard() {
        for (PredictionMode mode : MODES) {
            for (String command : COMMANDS) {
                ParsingException error = assertThrows(ParsingException.class,
                        () -> statement(command + " t PARTITION(k=DATE 'bad')", mode));
                assertNotEquals("Key partition selectors are not supported for " + command, error.getDetailMsg());
                assertTrue(error.getDetailMsg().contains("bad"), error.getDetailMsg());
            }
        }
    }
}
