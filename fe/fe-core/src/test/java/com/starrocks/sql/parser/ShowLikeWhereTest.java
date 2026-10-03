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

class ShowLikeWhereTest {
    private static final List<String> STATEMENTS = List.of("SHOW DATABASES", "SHOW SCHEMAS", "SHOW TABLES",
            "SHOW FULL TABLES", "SHOW TEMPORARY TABLES", "SHOW COLUMNS FROM t", "SHOW TABLE STATUS",
            "SHOW MATERIALIZED VIEWS", "SHOW VARIABLES", "SHOW GLOBAL VARIABLES");

    @Test
    void rejectLikeTogetherWithWhere() {
        for (String statement : STATEMENTS) {
            String sql = statement + " LIKE 'a%' WHERE x = 'b'";
            ParsingException error = assertThrows(ParsingException.class,
                    () -> SqlParser.parse(sql, new SessionVariable()), sql);
            assertTrue(error.getMessage().contains("LIKE and WHERE cannot be used together"), error.getMessage());
        }
    }

    @Test
    void keepLikeOrWhereAlone() {
        for (String statement : STATEMENTS) {
            assertDoesNotThrow(() -> SqlParser.parse(statement + " LIKE 'a%'", new SessionVariable()));
            assertDoesNotThrow(() -> SqlParser.parse(statement + " WHERE x = 'b'", new SessionVariable()));
        }
    }
}
