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
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.GrantRevokePrivilegeObjects;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Public SQL-to-AST regression only; never analyzes types or executes privileges. */
class SqlParserStructFieldNameContractTest {
    private static final String DETAIL = "Nested field paths are not allowed in STRUCT type declarations; "
            + "quote a literal field name";
    private static final long[] MODES = {
        0L, 2L, 131072L, 131074L, 8589934592L, 8589934594L, 8590065664L, 8590065666L,
        68719476736L, 68719476738L, 68719607808L, 68719607810L,
        77309411328L, 77309411330L, 77309542400L, 77309542402L
    };
    private ConnectContext previous;
    private boolean previousCaseInsensitive;
    private SessionVariable session;

    @BeforeEach
    void installParserContext() {
        previous = ConnectContext.get();
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
        if (previous == null) {
            ConnectContext.remove();
        } else {
            previous.setThreadLocalInfo();
        }
    }

    private StatementBase parse(String sql) {
        return SqlParser.parseOneWithStarRocksDialect(sql, session);
    }

    private StructType cast(String declaration) {
        QueryStatement query = (QueryStatement) parse("SELECT CAST(NULL AS " + declaration + ")");
        SelectRelation select = (SelectRelation) query.getQueryRelation();
        CastExpr cast = (CastExpr) select.getSelectList().getItems().get(0).getExpr();
        return (StructType) cast.getTargetTypeDef().getType();
    }

    private StructType function(String declaration) throws Exception {
        BaseGrantRevokePrivilegeStmt statement = (BaseGrantRevokePrivilegeStmt) parse(
                "GRANT USAGE ON FUNCTION f(" + declaration + ") TO ROLE r");
        var field = BaseGrantRevokePrivilegeStmt.class.getDeclaredField("objectsUnResolved");
        field.setAccessible(true);
        GrantRevokePrivilegeObjects objects = (GrantRevokePrivilegeObjects) field.get(statement);
        return (StructType) objects.getFunctionArgsDefs().get(0).getArgTypeDefs().get(0).getType();
    }

    private static void single(StructType type, String name) {
        assertEquals(1, type.getFields().size());
        assertEquals(name, type.getFields().get(0).getName());
        assertEquals(0, type.getFields().get(0).getPosition());
        assertEquals(IntegerType.INT, type.getFields().get(0).getType());
    }

    @Test
    void namesAndExplicitNestingRemainLiteralInBothEntrypoints() throws Exception {
        for (long mode : MODES) {
            session.setSqlMode(mode);
            for (boolean insensitive : new boolean[] {false, true}) {
                GlobalVariable.enableTableNameCaseInsensitive = insensitive;
                for (String[] item : new String[][] {
                        {"I", "I"}, {"[*]", "[*]"}, {"`a.b`", "a.b"},
                        {"`资源😀``x.y`", "资源😀`x.y"}}) {
                    String declaration = "STRUCT<" + item[0] + " INT>";
                    single(cast(declaration), item[1]);
                    single(function(declaration), item[1]);
                }
                for (StructType outer : List.of(cast("STRUCT<a STRUCT<b INT>>"),
                        function("STRUCT<a STRUCT<b INT>>"))) {
                    assertEquals("a", outer.getFields().get(0).getName());
                    single((StructType) outer.getFields().get(0).getType(), "b");
                }
            }
        }
    }

    @Test
    void compoundDeclarationsHavePreciseSemanticErrorsAfterSyntaxAcceptance() throws Exception {
        for (long mode : MODES) {
            session.setSqlMode(mode);
            for (String prefix : List.of("SELECT CAST(NULL AS ", "GRANT USAGE ON FUNCTION f(")) {
                String suffix = prefix.startsWith("SELECT") ? ")" : ") TO ROLE r";
                for (String[] item : new String[][] { {"a.b", "b"}, {"a.123b", ".123b"},
                        {"a.[*].b", "b"}, {"`😀`.`资源`", "`资源`"}}) {
                    String sql = prefix + "STRUCT<" + item[0] + " INT>" + suffix;
                    ParsingException error = assertThrows(ParsingException.class, () -> parse(sql), sql);
                    assertEquals(DETAIL, error.getDetailMsg(), sql);
                    var field = ParsingException.class.getDeclaredField("pos");
                    field.setAccessible(true);
                    NodePosition position = (NodePosition) field.get(error);
                    int first = sql.indexOf(item[0]);
                    int last = first + item[0].lastIndexOf(item[1]);
                    assertEquals(1, position.getLine(), sql);
                    assertEquals(1, position.getEndLine(), sql);
                    assertEquals(sql.codePointCount(0, first), position.getCol(), sql);
                    assertEquals(sql.codePointCount(0, last), position.getEndCol(), sql);
                }
            }
        }
    }

    @Test
    void outerSyntaxAndEarlierConstructorsKeepTheirErrorPriority() {
        for (long mode : MODES) {
            session.setSqlMode(mode);
            for (String prefix : List.of("SELECT CAST(NULL AS ", "GRANT USAGE ON FUNCTION f(")) {
                String suffix = prefix.startsWith("SELECT") ? ")" : ") TO ROLE r";
                String invalidName = prefix + "STRUCT<a.b VARCHAR(2147483648)>" + suffix;
                assertEquals(DETAIL, assertThrows(ParsingException.class, () -> parse(invalidName)).getDetailMsg());
                String malformed = invalidName + " +";
                ParsingException syntax = assertThrows(ParsingException.class, () -> parse(malformed));
                assertFalse(DETAIL.equals(syntax.getDetailMsg()), malformed);
                String earlierWidth = prefix + "STRUCT<a VARCHAR(2147483648),b.c INT>" + suffix;
                assertEquals("For input string: \"2147483648\"",
                        assertThrows(NumberFormatException.class, () -> parse(earlierWidth)).getMessage());
            }
        }
    }
}
