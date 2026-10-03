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
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryPeriodTextTest {
    private static String periodText(String sql) {
        QueryStatement statement = (QueryStatement) SqlParser.parse(sql, new SessionVariable()).get(0);
        return ((TableRelation) ((SelectRelation) statement.getQueryRelation()).getRelation()).getQueryPeriodString();
    }

    @Test
    void keepTheClauseAsWritten() {
        assertEquals("for system_time as of now() - interval 1 day",
                periodText("select * from t for system_time as of now() - interval 1 day"));
        assertEquals("FOR SYSTEM_TIME BETWEEN (NOW() - INTERVAL 1 YEAR) AND NOW()",
                periodText("SELECT * FROM t FOR SYSTEM_TIME BETWEEN (NOW() - INTERVAL 1 YEAR) AND NOW()"));
    }
}
