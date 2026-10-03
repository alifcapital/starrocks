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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InsertTableFunctionTargetTest {
    private static final List<String> TARGETS = List.of("FILES('path' = 's3://b/p', 'format' = 'parquet')",
            "BLACKHOLE()");

    private static void assertRejected(String sql, String message) {
        ParsingException error = assertThrows(ParsingException.class, () -> SqlParser.parse(sql, new SessionVariable()));
        assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    @Test
    void rejectClausesTheTargetWouldIgnore() {
        for (String target : TARGETS) {
            assertRejected("INSERT OVERWRITE " + target + " SELECT 1",
                    "INSERT OVERWRITE is not supported for FILES() or BLACKHOLE()");
            assertRejected("INSERT INTO " + target + " (a) SELECT 1",
                    "A column list or BY NAME is not supported for FILES() or BLACKHOLE()");
            assertRejected("INSERT INTO " + target + " BY NAME SELECT 1 AS a",
                    "A column list or BY NAME is not supported for FILES() or BLACKHOLE()");
            assertRejected("INSERT INTO " + target + " PROPERTIES ('timeout' = '10') SELECT 1",
                    "PROPERTIES is not supported for FILES() or BLACKHOLE()");
        }
    }

    @Test
    void keepSupportedForms() {
        for (String target : TARGETS) {
            assertInstanceOf(InsertStmt.class,
                    SqlParser.parse("INSERT INTO " + target + " SELECT 1", new SessionVariable()).get(0));
            assertInstanceOf(InsertStmt.class,
                    SqlParser.parse("INSERT INTO " + target + " WITH LABEL l1 SELECT 1", new SessionVariable()).get(0));
        }
    }
}
