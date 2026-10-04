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

class DuplicatePropertyKeyTest {
    @Test
    void rejectDuplicateKey() {
        for (String sql : List.of(
                "CREATE TABLE t (k int) PROPERTIES ('replication_num' = '1', 'replication_num' = '3')",
                "ALTER TABLE t SET ('a' = '1', 'a' = '2')",
                "INSERT INTO FILES('path' = 's3://b/p', 'PATH' = 's3://b/q') SELECT 1",
                "LOAD LABEL l (DATA INFILE('s3://b/p') INTO TABLE t) PROPERTIES ('timeout' = '1', 'timeout' = '2')")) {
            ParsingException error = assertThrows(ParsingException.class,
                    () -> SqlParser.parse(sql, new SessionVariable()), sql);
            assertTrue(error.getMessage().contains("Duplicate property key"), error.getMessage());
        }
    }

    @Test
    void keepDistinctKeys() {
        assertDoesNotThrow(() -> SqlParser.parse(
                "CREATE TABLE t (k int) PROPERTIES ('replication_num' = '1', 'storage_medium' = 'SSD')",
                new SessionVariable()));
    }
}
