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
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmbeddedQueryClauseTest {
    private static final List<String> PREFIXES = List.of("INSERT INTO t ", "CREATE TABLE t AS ",
            "CREATE VIEW v AS ", "ALTER VIEW v AS ", "CREATE MATERIALIZED VIEW mv AS ");
    private static final List<String> QUERIES = List.of("EXPLAIN SELECT 1", "EXPLAIN VERBOSE SELECT 1",
            "TRACE TIMES SELECT 1", "SELECT 1 INTO OUTFILE 's3://b/p'");

    @Test
    void rejectTopLevelQueryClausesInEmbeddedQueries() {
        for (String prefix : PREFIXES) {
            for (String query : QUERIES) {
                ParsingException error = assertThrows(ParsingException.class,
                        () -> SqlParser.parse(prefix + query, new SessionVariable()), prefix + query);
                assertTrue(error.getMessage().contains(
                        "EXPLAIN, TRACE and INTO OUTFILE are not supported in an embedded query"), error.getMessage());
            }
        }
    }

    @Test
    void keepTopLevelForms() {
        SessionVariable session = new SessionVariable();
        assertTrue(((QueryStatement) SqlParser.parse("EXPLAIN SELECT 1", session).get(0)).isExplain());
        assertInstanceOf(QueryStatement.class, SqlParser.parse("SELECT 1 INTO OUTFILE 's3://b/p'", session).get(0));
        assertTrue(SqlParser.parse("EXPLAIN INSERT INTO t SELECT 1", session).get(0).isExplain());
        assertInstanceOf(InsertStmt.class, SqlParser.parse("INSERT INTO t SELECT 1", session).get(0));
    }
}
