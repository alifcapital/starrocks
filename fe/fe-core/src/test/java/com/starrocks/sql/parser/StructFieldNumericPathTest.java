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
import com.starrocks.sql.ast.AddFieldClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.DropFieldClause;
import com.starrocks.sql.ast.StatementBase;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.junit.jupiter.api.Test;

import java.util.IdentityHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Digit-starting field names must survive both grammar prediction modes. */
class StructFieldNumericPathTest {
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
    private AlterClause clause(String tail, PredictionMode mode) {
        return ((AlterTableStmt) statement("ALTER TABLE t " + tail, mode)).getAlterClauseList().get(0);
    }
    @Test
    void numericComponentIsRetainedForAddAndDrop() {
        for (PredictionMode mode : List.of(PredictionMode.SLL, PredictionMode.LL)) {
            AddFieldClause add = (AddFieldClause) clause("MODIFY COLUMN c ADD FIELD a.123abc INT", mode);
            assertEquals("123abc", add.getFieldName());
            assertEquals(List.of("a"), add.getNestedParentFieldNames());
            assertTrue(add.getPos().isZero());
            assertNull(add.getFieldDesc().getPos());
            DropFieldClause drop = (DropFieldClause) clause("MODIFY COLUMN c DROP FIELD a.123abc.[*].leaf", mode);
            assertEquals("leaf", drop.getFieldName());
            assertEquals(List.of("a", "123abc", "[*]"), drop.getNestedParentFieldNames());
            assertTrue(drop.getPos().isZero());
        }
    }
    @Test
    void quotedDotAndSpacedDotKeepDistinctPaths() {
        for (PredictionMode mode : List.of(PredictionMode.SLL, PredictionMode.LL)) {
            for (String command : List.of("ADD", "DROP")) {
                String suffix = command.equals("ADD") ? " INT" : "";
                AlterClause quoted = clause("MODIFY COLUMN c " + command + " FIELD `a.123abc`" + suffix, mode);
                AlterClause spaced = clause("MODIFY COLUMN c " + command + " FIELD a . 123abc" + suffix, mode);
                if (quoted instanceof AddFieldClause) {
                    assertEquals("a.123abc", ((AddFieldClause) quoted).getFieldName());
                    assertEquals(List.of(), ((AddFieldClause) quoted).getNestedParentFieldNames());
                    assertEquals("123abc", ((AddFieldClause) spaced).getFieldName());
                    assertEquals(List.of("a"), ((AddFieldClause) spaced).getNestedParentFieldNames());
                } else {
                    assertEquals("a.123abc", ((DropFieldClause) quoted).getFieldName());
                    assertEquals(List.of(), ((DropFieldClause) quoted).getNestedParentFieldNames());
                    assertEquals("123abc", ((DropFieldClause) spaced).getFieldName());
                    assertEquals(List.of("a"), ((DropFieldClause) spaced).getNestedParentFieldNames());
                }
            }
        }
    }
}
