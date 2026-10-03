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
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.StructType;
import org.antlr.v4.runtime.Token;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlParserBackQuotedIdentifierContractTest {
    private static final long[] MODES = {
        0L, 2L, 131072L, 131074L, 8589934592L, 8589934594L, 8590065664L, 8590065666L,
        68719476736L, 68719476738L, 68719607808L, 68719607810L,
        77309411328L, 77309411330L, 77309542400L, 77309542402L
    };
    private ConnectContext previousContext;
    private boolean previousCaseInsensitive;
    private SessionVariable session;

    @BeforeEach
    void installParserContext() {
        previousContext = ConnectContext.get();
        previousCaseInsensitive = GlobalVariable.enableTableNameCaseInsensitive;
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setThreadLocalInfo();
        session = context.getSessionVariable();
        session.setSqlDialect("StarRocks");
        session.setEnableDialectDowngrade(false);
    }

    @AfterEach
    void restoreParserContext() {
        GlobalVariable.enableTableNameCaseInsensitive = previousCaseInsensitive;
        if (previousContext == null) {
            ConnectContext.remove();
        } else {
            previousContext.setThreadLocalInfo();
        }
    }

    private StatementBase parse(String sql) {
        return SqlParser.parseOneWithStarRocksDialect(sql, session);
    }

    private SelectRelation select(String sql) {
        return (SelectRelation) ((QueryStatement) parse(sql)).getQueryRelation();
    }

    private SelectListItem first(String sql) {
        return select(sql).getSelectList().getItems().get(0);
    }

    @Test
    void escapedIdentifiersAndAliasesKeepLiteralCharactersAndTokenSpans() {
        for (long mode : MODES) {
            session.setSqlMode(mode);
            for (String[] pair : new String[][] {
                {"`a``b`", "a`b"}, {"`a````b`", "a``b"}, {"``", ""},
                {"` a b `", " a b "}, {"`资源😀``I`", "资源😀`I"},
                {"`a\\b`", "a\\b"}, {"`plain`", "plain"}
            }) {
                String sql = "SELECT " + pair[0];
                SlotRef slot = (SlotRef) first(sql).getExpr();
                assertEquals(pair[1], slot.getColumnName(), sql);
                assertTrue(slot.isBackQuoted(), sql);
                assertEquals(1, slot.getPos().getLine(), sql);
                assertEquals(7, slot.getPos().getCol(), sql);
                assertEquals(1, slot.getPos().getEndLine(), sql);
                assertEquals(7, slot.getPos().getEndCol(), sql);
                assertEquals(pair[1], first("SELECT 1 AS " + pair[0]).getAlias(), sql);

                StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(SqlTextStream.create(sql)));
                lexer.setSqlMode(mode);
                Token token;
                do {
                    token = lexer.nextToken();
                } while (token.getType() != StarRocksLexer.BACKQUOTED_IDENTIFIER && token.getType() != Token.EOF);
                assertEquals(StarRocksLexer.BACKQUOTED_IDENTIFIER, token.getType(), sql);
                assertEquals(pair[0], token.getText(), sql);
                assertEquals(7, token.getStartIndex(), sql);
                assertEquals(7 + pair[0].codePointCount(0, pair[0].length()) - 1, token.getStopIndex(), sql);
                assertEquals(1, token.getLine(), sql);
                assertEquals(7, token.getCharPositionInLine(), sql);
            }
        }
    }

    @Test
    void qualifiedTablesAndFunctionNamesKeepEscapedPairsWithRootCaseRules() {
        for (long mode : MODES) {
            session.setSqlMode(mode);
            for (boolean insensitive : new boolean[] {false, true}) {
                GlobalVariable.enableTableNameCaseInsensitive = insensitive;
                String sql = "SELECT 1 FROM `Cat``I`.`Db``I`.`Tbl``I`";
                TableRelation table = (TableRelation) select(sql).getRelation();
                assertEquals(insensitive ? "cat`i" : "Cat`I", table.getName().getCatalog(), sql);
                assertEquals(insensitive ? "db`i" : "Db`I", table.getName().getDb(), sql);
                assertEquals(insensitive ? "tbl`i" : "Tbl`I", table.getName().getTbl(), sql);
                FunctionCallExpr function = (FunctionCallExpr) first("SELECT `Db``I`.`Fn``I`(1)").getExpr();
                assertEquals(List.of("Db`I", "Fn`I"), function.getFnName().getParts());
                assertEquals("Fn`I".toLowerCase(Locale.ROOT), function.getFunctionName());
            }
        }
    }

    @Test
    void nestedStructUsesIdentifierHelperAndStringsKeepBackticks() {
        for (long mode : MODES) {
            session.setSqlMode(mode);
            CastExpr cast = (CastExpr) first(
                    "SELECT CAST(NULL AS STRUCT<`outer``I` STRUCT<`资源😀``inner` INT>>)").getExpr();
            StructType outer = (StructType) cast.getTargetTypeDef().getType();
            assertEquals("outer`I", outer.getFields().get(0).getName());
            StructType inner = (StructType) outer.getFields().get(0).getType();
            assertEquals("资源😀`inner", inner.getFields().get(0).getName());
            assertEquals("a``b", ((StringLiteral) first("SELECT 'a``b'").getExpr()).getStringValue());
            assertEquals("a``b", ((StringLiteral) first("SELECT \"a``b\"").getExpr()).getStringValue());
            assertEquals("plain", ((SlotRef) first("SELECT plain").getExpr()).getColumnName());
        }
    }

    @Test
    void inlinePrepareNameRetainsSeparateRawTokenContract() {
        for (long mode : MODES) {
            session.setSqlMode(mode);
            PrepareStmt prepared = (PrepareStmt) parse("PREPARE `p``I` FROM SELECT `a``b`");
            assertEquals("`p``I`", prepared.getName());
            SelectRelation body = (SelectRelation) ((QueryStatement) prepared.getInnerStmt()).getQueryRelation();
            SlotRef slot = (SlotRef) body.getSelectList().getItems().get(0).getExpr();
            assertEquals("a`b", slot.getColumnName());
            assertTrue(prepared.getParameters().isEmpty());
        }
    }
}
