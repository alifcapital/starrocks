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
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilesRelationAliasTest {
    @Test
    void keepTheAlias() {
        QueryStatement statement = (QueryStatement) SqlParser.parse(
                "SELECT f.a FROM FILES('path' = 's3://b/p', 'format' = 'parquet') AS f", new SessionVariable()).get(0);
        FileTableFunctionRelation files =
                (FileTableFunctionRelation) ((SelectRelation) statement.getQueryRelation()).getRelation();
        assertEquals("f", files.getAlias().getTbl());
    }

    @Test
    void rejectColumnAliases() {
        ParsingException error = assertThrows(ParsingException.class, () -> SqlParser.parse(
                "SELECT * FROM FILES('path' = 's3://b/p', 'format' = 'parquet') AS f(a, b)", new SessionVariable()));
        assertTrue(error.getMessage().contains("Column aliases are not supported for FILES()"), error.getMessage());
    }
}
