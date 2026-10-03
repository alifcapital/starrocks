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

class PartitionsInAdvanceTest {
    private static final String RANGES = " (START ('2024-01-01') END ('2024-02-01') EVERY (INTERVAL 1 DAY))";

    @Test
    void rejectRangesForGeneratedColumnPartitionExpressions() {
        for (String sql : List.of(
                "CREATE TABLE t (k varchar(10), d date) PARTITION BY substr(k, 1, 2)" + RANGES,
                "CREATE TABLE t PARTITION BY substr(k, 1, 2)" + RANGES + " AS SELECT 'a' AS k")) {
            ParsingException error = assertThrows(ParsingException.class,
                    () -> SqlParser.parse(sql, new SessionVariable()), sql);
            assertTrue(error.getMessage().contains("Creating partitions in advance is only supported"),
                    error.getMessage());
        }
    }

    @Test
    void keepSupportedForms() {
        assertDoesNotThrow(() -> SqlParser.parse(
                "CREATE TABLE t (k varchar(10), d date) PARTITION BY date_trunc('day', d)" + RANGES,
                new SessionVariable()));
        assertDoesNotThrow(() -> SqlParser.parse(
                "CREATE TABLE t (k varchar(10), d date) PARTITION BY substr(k, 1, 2)", new SessionVariable()));
    }
}
