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
import com.starrocks.sql.ast.QueryStatement;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

class SqlModeHintTest {
    @Test
    void keepSqlModeHintNextToOtherSetVarHints() {
        // The builder reads the hints from an identity map, so their order differs between runs.
        String sql = "SELECT /*+ SET_VAR(sql_mode = 'SORT_NULLS_LAST') */ a FROM "
                + "(SELECT /*+ SET_VAR(query_timeout = 10) */ a FROM t) x ORDER BY a";
        for (int i = 0; i < 50; i++) {
            QueryStatement statement = (QueryStatement) SqlParser.parse(sql, new SessionVariable()).get(0);
            assertFalse(statement.getQueryRelation().getOrderBy().get(0).getNullsFirstParam());
        }
    }
}
