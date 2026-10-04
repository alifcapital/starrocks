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
import com.starrocks.sql.ast.GroupByClause;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlParserEmptyGroupingContractTest {
    private ConnectContext previousContext;

    @BeforeEach
    void installParserContext() {
        previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setThreadLocalInfo();
    }

    @AfterEach
    void restoreParserContext() {
        if (previousContext == null) {
            ConnectContext.remove();
        } else {
            previousContext.setThreadLocalInfo();
        }
    }

    private static QueryStatement parse(String sql) {
        SessionVariable session = new SessionVariable();
        session.setSqlMode(0);
        return (QueryStatement) SqlParser.parseOneWithStarRocksDialect(sql, session);
    }

    private static NodePosition clausePosition(String sql, String family) {
        int start = sql.indexOf(family);
        int stop = sql.indexOf(')', start);
        int line = 1;
        int col = 0;
        int startLine = 0;
        int startCol = 0;
        for (int i = 0; i <= stop; i++) {
            if (i == start) {
                startLine = line;
                startCol = col;
            }
            if (i == stop) {
                return new NodePosition(startLine, startCol, line, col);
            }
            if (sql.charAt(i) == '\n') {
                line++;
                col = 0;
            } else {
                col++;
            }
        }
        throw new AssertionError("Missing grouping clause in fixture");
    }

    private static void assertEmptyClause(String sql, String family) {
        String detail = family + " requires at least one grouping expression";
        ParsingException error = assertThrows(ParsingException.class, () -> parse(sql));
        assertEquals(detail, error.getDetailMsg());
        assertEquals(new ParsingException(detail, clausePosition(sql, family)).getMessage(), error.getMessage());
    }

    @Test
    void rejectsEmptyRollupAndCubeAtClausePosition() {
        for (String family : List.of("ROLLUP", "CUBE")) {
            assertEmptyClause("SELECT a FROM t GROUP BY " + family + "()", family);
        }
    }

    @Test
    void emptyCommentsAndMultilineClausesRetainFullPosition() {
        for (String family : List.of("ROLLUP", "CUBE")) {
            assertEmptyClause("SELECT a FROM t GROUP BY " + family + "(/* empty */)", family);
            assertEmptyClause("SELECT a FROM t GROUP BY " + family + "(\n/* empty */\n)", family);
        }
    }

    @Test
    void nonemptyGroupingAndEmptyGroupingSetsRemainValid() {
        for (String family : List.of("ROLLUP", "CUBE")) {
            SelectRelation relation = (SelectRelation) parse(
                    "SELECT a FROM t GROUP BY " + family + "(a,b)").getQueryRelation();
            assertEquals(family, relation.getGroupByClause().getGroupingType().name());
            assertEquals(2, relation.getGroupByClause().getGroupingExprs().size());
        }
        SelectRelation sets = (SelectRelation) parse(
                "SELECT a FROM t GROUP BY GROUPING SETS(())").getQueryRelation();
        assertEquals(GroupByClause.GroupingType.GROUPING_SETS, sets.getGroupByClause().getGroupingType());
        assertEquals(1, sets.getGroupByClause().getGroupingSetList().size());
        assertTrue(sets.getGroupByClause().getGroupingSetList().get(0).isEmpty());
    }

    @Test
    void nestedCteAndUnionEmptyClausesUseTheirOwnPosition() {
        for (String family : List.of("ROLLUP", "CUBE")) {
            for (String sql : List.of(
                    "SELECT * FROM (SELECT a FROM t GROUP BY " + family + "()) q",
                    "WITH q AS (SELECT a FROM t GROUP BY " + family + "()) SELECT * FROM q",
                    "SELECT a FROM t UNION ALL SELECT a FROM t GROUP BY " + family + "()")) {
                assertEmptyClause(sql, family);
            }
        }
    }

    @Test
    void malformedTailSyntaxPrecedesGroupingBuilderValidation() {
        for (String family : List.of("ROLLUP", "CUBE")) {
            for (String tail : List.of(" HAVING", " ORDER BY", " LIMIT")) {
                ParsingException error = assertThrows(ParsingException.class,
                        () -> parse("SELECT a FROM t GROUP BY " + family + "()" + tail));
                assertFalse(error.getDetailMsg().equals(family + " requires at least one grouping expression"));
            }
        }
    }

    @Test
    void earlierSelectFromWhereAndOrderConstructorErrorsArePreserved() {
        for (String family : List.of("ROLLUP", "CUBE")) {
            for (String sql : List.of(
                    "SELECT DATE 'not-a-date' FROM t GROUP BY " + family + "()",
                    "SELECT * FROM DUAL GROUP BY " + family + "()",
                    "SELECT a FROM t WHERE DATE 'not-a-date' GROUP BY " + family + "()",
                    "SELECT a FROM t GROUP BY " + family + "() ORDER BY DATE 'not-a-date'")) {
                ParsingException error = assertThrows(ParsingException.class, () -> parse(sql));
                assertFalse(error.getDetailMsg().equals(family + " requires at least one grouping expression"));
            }
        }
    }
}
