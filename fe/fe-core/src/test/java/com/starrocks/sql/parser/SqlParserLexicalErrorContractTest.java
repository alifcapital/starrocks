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

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PrepareStmt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlParserLexicalErrorContractTest {
    private ConnectContext previousContext;
    private boolean previousConcurrent;
    private boolean previousCache;
    private SessionVariable session;

    @BeforeEach
    void installParserContext() {
        previousContext = ConnectContext.get();
        previousConcurrent = Config.enable_concurrent_parse_optimization;
        previousCache = Config.enable_parser_context_cache;
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setThreadLocalInfo();
        session = context.getSessionVariable();
        session.setSqlDialect("StarRocks");
        session.setEnableDialectDowngrade(false);
        session.setSqlMode(0);
    }

    @AfterEach
    void restoreParserContext() {
        Config.enable_concurrent_parse_optimization = previousConcurrent;
        Config.enable_parser_context_cache = previousCache;
        if (previousContext == null) {
            ConnectContext.remove();
        } else {
            previousContext.setThreadLocalInfo();
        }
    }

    private static void assertLexicalError(int line, int column, Executable action) {
        ParsingException error = assertThrows(ParsingException.class, action);
        assertTrue(error.getDetailMsg().startsWith("token recognition error at:"));
        assertEquals(new ParsingException(error.getDetailMsg(), new NodePosition(line, column)).getMessage(),
                error.getMessage());
    }

    @Test
    void statementsRejectCharactersThatTheLexerPreviouslySkipped() {
        for (boolean concurrent : List.of(false, true)) {
            Config.enable_concurrent_parse_optimization = concurrent;
            assertLexicalError(1, 8, () -> SqlParser.parse("SELECT 1#comment", session));
            assertLexicalError(1, 8, () -> SqlParser.parse("SELECT 1#comment", 0));
            assertLexicalError(1, 8, () -> SqlParser.parseSingleStatement("SELECT 1#comment", 0));
            assertLexicalError(1, 8, () -> SqlParser.parseFirstStatement("SELECT 1#comment", 0));
            assertLexicalError(1, 8, () -> SqlParser.parseOneWithStarRocksDialect("SELECT 1#comment", session));
            assertLexicalError(2, 0, () -> SqlParser.parse("SELECT 1\n#comment", session));
            assertLexicalError(1, 7, () -> SqlParser.parse("SELECT 'unterminated", session));
        }
    }

    @Test
    void expressionAndSchemaEntryPointsRejectLexicalErrors() {
        assertLexicalError(1, 1, () -> SqlParser.parseExpression("1#x", session));
        assertLexicalError(1, 1, () -> SqlParser.parseSqlToExpr("1#x", 0));
        assertLexicalError(1, 1, () -> SqlParser.parseSqlToExprs("1#x,2", session));
        assertLexicalError(1, 9, () -> SqlParser.parseImportColumns("COLUMNS(a#)", 0));
        assertLexicalError(1, 5, () -> SqlParser.parseFilesSchema("k INT#"));
    }

    @Test
    void quotedPrepareRejectsChildErrorsAndRetainsParameterSlots() {
        assertLexicalError(1, 8, () -> SqlParser.parseSingleStatement("PREPARE p FROM 'SELECT 1#comment'", 0));
        assertLexicalError(1, 15, () -> SqlParser.parseSingleStatement("PREPARE p FROM 'SELECT 1", 0));
        PrepareStmt prepare = (PrepareStmt) SqlParser.parseSingleStatement(
                "PREPARE p FROM 'SELECT ? ORDER BY ?'", 0);
        assertEquals(2, prepare.getParameters().size());
    }

    @Test
    void validCommentsAndLiteralsKeepTheirCharacters() {
        for (String sql : List.of("SELECT 1", "SELECT '#' AS x", "SELECT /* # */ 1",
                "SELECT 1 -- #\n", "PREPARE p FROM 'SELECT /* # */ 1'")) {
            assertEquals(1, SqlParser.parse(sql, session).size());
        }
        assertNotNull(SqlParser.parseExpression("1+2", session));
        assertNotNull(SqlParser.parseSqlToExpr("'x#y'", 0));
        assertEquals(2, SqlParser.parseSqlToExprs("1,2", session).size());
        assertNotNull(SqlParser.parseImportColumns("COLUMNS(a=1)", 0));
        assertEquals(1, SqlParser.parseFilesSchema("k INT").size());
    }
}
