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
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShiftOperatorPrecedenceTest {
    private static String print(String sql) {
        return AstToSQLBuilder.toSQL(SqlParser.parse(sql, new SessionVariable()).get(0));
    }

    @Test
    void shiftsAreOneLevelFromLeftToRight() {
        assertEquals("SELECT (8 BITSHIFTRIGHT 1) BITSHIFTLEFT 2", print("SELECT 8 BITSHIFTRIGHT 1 BITSHIFTLEFT 2"));
        assertEquals("SELECT (8 BITSHIFTLEFT 1) BITSHIFTRIGHT 2", print("SELECT 8 BITSHIFTLEFT 1 BITSHIFTRIGHT 2"));
        assertEquals("SELECT (8 BITSHIFTRIGHTLOGICAL 1) BITSHIFTLEFT 2",
                print("SELECT 8 BITSHIFTRIGHTLOGICAL 1 BITSHIFTLEFT 2"));
        assertEquals("SELECT (1 | 2) BITSHIFTLEFT 3", print("SELECT 1 | 2 BITSHIFTLEFT 3"));
        String predicate = print("SELECT * FROM t WHERE 8 BITSHIFTRIGHT 1 BITSHIFTLEFT 2 IN (1)");
        assertTrue(predicate.contains("(8 BITSHIFTRIGHT 1) BITSHIFTLEFT 2"), predicate);
    }
}
