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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SyncMaterializedViewClauseTest {
    private static final String QUERY = " AS SELECT k1, sum(v1) FROM t GROUP BY k1";

    private static void assertRejected(String sql, String message) {
        ParsingException error = assertThrows(ParsingException.class,
                () -> SqlParser.parse(sql, new SessionVariable()), sql);
        assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    @Test
    void rejectClausesThatSyncViewDrops() {
        for (String head : List.of("CREATE MATERIALIZED VIEW mv (c1, c2)",
                "CREATE MATERIALIZED VIEW mv (c1 COMMENT 'x', c2)",
                "CREATE MATERIALIZED VIEW mv (c1, c2, INDEX i (c1) USING BITMAP)",
                "CREATE MATERIALIZED VIEW mv COMMENT 'x'",
                "CREATE MATERIALIZED VIEW mv ORDER BY (k1)")) {
            assertRejected(head + QUERY, "SYNC refresh type");
        }
    }

    @Test
    void rejectDuplicateOrderBy() {
        assertRejected("CREATE MATERIALIZED VIEW mv REFRESH ASYNC ORDER BY (k1) ORDER BY (k1)" + QUERY, "ORDER BY");
    }

    @Test
    void keepSupportedForms() {
        assertDoesNotThrow(() -> SqlParser.parse("CREATE MATERIALIZED VIEW mv" + QUERY, new SessionVariable()));
        assertDoesNotThrow(() -> SqlParser.parse(
                "CREATE MATERIALIZED VIEW mv (c1, c2) COMMENT 'x' REFRESH ASYNC ORDER BY (c1)" + QUERY,
                new SessionVariable()));
    }
}
