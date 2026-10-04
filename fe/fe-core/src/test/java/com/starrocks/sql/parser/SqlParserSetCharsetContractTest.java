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
import com.starrocks.sql.ast.SetNamesVar;
import com.starrocks.sql.ast.SetStmt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlParserSetCharsetContractTest {
    private ConnectContext previousContext;
    private SessionVariable session;

    @BeforeEach
    void installParserContext() {
        previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setThreadLocalInfo();
        session = context.getSessionVariable();
        session.setSqlDialect("StarRocks");
        session.setEnableDialectDowngrade(false);
    }

    @AfterEach
    void restoreParserContext() {
        if (previousContext == null) {
            ConnectContext.remove();
        } else {
            previousContext.setThreadLocalInfo();
        }
    }

    private SetStmt parse(String sql) {
        return (SetStmt) SqlParser.parseOneWithStarRocksDialect(sql, session);
    }

    @Test
    void allCharsetSpellingsRetainExplicitValueAndDefault() {
        for (long mode : new long[] {0, 2, 131072, 131074}) {
            session.setSqlMode(mode);
            for (String spelling : List.of("CHAR SET", "CHARSET", "CHARACTER SET", "NAMES")) {
                for (String value : List.of("latin1", "'MiXeD'", "`MiXeD`")) {
                    SetNamesVar item = (SetNamesVar) parse("SET " + spelling + " " + value).getSetListItems().get(0);
                    assertEquals(value.equals("latin1") ? "latin1" : "MiXeD", item.getCharset());
                    assertEquals("DEFAULT", item.getCollate());
                }
                SetNamesVar defaults = (SetNamesVar) parse("SET " + spelling + " DEFAULT").getSetListItems().get(0);
                assertNull(defaults.getCharset());
                assertEquals("DEFAULT", defaults.getCollate());
            }
        }
    }

    @Test
    void mixedSetItemsKeepExactStopTokenPositions() {
        session.setSqlMode(0);
        String sql = "SET autocommit=DEFAULT, SESSION `MiXeD`=ON, @@global.autocommit=ALL";
        SetStmt statement = parse(sql);
        assertEquals(64, statement.getPos().getEndCol());
        assertEquals(15, statement.getSetListItems().get(0).getPos().getEndCol());
        assertEquals(sql.indexOf("=ON") + 1, statement.getSetListItems().get(1).getPos().getEndCol());
        assertEquals(64, statement.getSetListItems().get(2).getPos().getEndCol());
    }

    @Test
    void malformedCharsetSuffixKeepsSyntaxErrorOwnership() {
        session.setSqlMode(0);
        assertThrows(ParsingException.class, () -> parse("SET CHARACTER SET latin1 COLLATE utf8"));
        ParsingException error = assertThrows(ParsingException.class, () -> parse("SET PROPERTY 'bad'='1' trailing"));
        org.junit.jupiter.api.Assertions.assertFalse(error.getDetailMsg()
                .equals("Please use ALTER USER syntax to set user properties."));
    }
}
