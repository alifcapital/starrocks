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

class ExplainedInsertJobTest {
    @Test
    void rejectExplainInStoredInsert() {
        for (String sql : List.of("SUBMIT TASK AS EXPLAIN INSERT INTO t SELECT 1",
                "SUBMIT TASK t1 AS EXPLAIN VERBOSE INSERT INTO t SELECT 1",
                "CREATE PIPE p AS EXPLAIN INSERT INTO t SELECT * FROM FILES('path' = 's3://b/p')")) {
            ParsingException error = assertThrows(ParsingException.class,
                    () -> SqlParser.parse(sql, new SessionVariable()), sql);
            assertTrue(error.getMessage().contains("EXPLAIN is not supported in the INSERT of"), error.getMessage());
        }
    }

    @Test
    void keepPlainInsert() {
        assertDoesNotThrow(() -> SqlParser.parse("SUBMIT TASK AS INSERT INTO t SELECT 1", new SessionVariable()));
        assertDoesNotThrow(() -> SqlParser.parse(
                "CREATE PIPE p AS INSERT INTO t SELECT * FROM FILES('path' = 's3://b/p')", new SessionVariable()));
    }
}
