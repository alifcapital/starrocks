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
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AlterStorageVolumePropertiesTest {
    @Test
    void keepEverySetClause() {
        AlterStorageVolumeStmt stmt = (AlterStorageVolumeStmt) SqlParser.parse(
                "ALTER STORAGE VOLUME v SET (\"a\" = \"1\"), COMMENT = 'c', SET (\"b\" = \"2\")",
                new SessionVariable()).get(0);
        assertEquals(Map.of("a", "1", "b", "2"), stmt.getProperties());
        assertEquals("c", stmt.getComment());
    }
}
