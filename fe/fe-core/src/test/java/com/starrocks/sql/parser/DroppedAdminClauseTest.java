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

class DroppedAdminClauseTest {
    private static void assertRejected(String sql, String message) {
        ParsingException error = assertThrows(ParsingException.class,
                () -> SqlParser.parse(sql, new SessionVariable()), sql);
        assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    @Test
    void rejectPredicateOnLoadWarningsOfUrl() {
        for (String clause : List.of(" WHERE a = 1", " ORDER BY a", " LIMIT 10")) {
            assertRejected("SHOW LOAD WARNINGS ON 'http://x'" + clause, "does not support WHERE, ORDER BY or LIMIT");
        }
        assertDoesNotThrow(() -> SqlParser.parse("SHOW LOAD WARNINGS ON 'http://x'", new SessionVariable()));
    }

    @Test
    void rejectTwoDatabasesInShowLoadJobs() {
        for (String statement : List.of("SHOW ROUTINE LOAD", "SHOW ALL ROUTINE LOAD", "SHOW STREAM LOAD")) {
            assertRejected(statement + " FOR db1.job FROM db2", "Specify the database either in FOR or in FROM");
            assertDoesNotThrow(() -> SqlParser.parse(statement + " FOR db1.job", new SessionVariable()));
            assertDoesNotThrow(() -> SqlParser.parse(statement + " FOR job FROM db2", new SessionVariable()));
        }
    }

    @Test
    void rejectDistributionInModifyPartition() {
        assertRejected("ALTER TABLE t MODIFY PARTITION DISTRIBUTED BY HASH(k) BUCKETS 4",
                "MODIFY PARTITION does not support DISTRIBUTED BY");
    }

    @Test
    void rejectTimesWhenDisablingFailPoint() {
        assertRejected("ADMIN DISABLE FAILPOINT 'fp' WITH 3 TIMES", "does not support TIMES or PROBABILITY");
        assertRejected("ADMIN DISABLE FAILPOINT 'fp' WITH 0.5 PROBABILITY", "does not support TIMES or PROBABILITY");
        assertDoesNotThrow(() -> SqlParser.parse("ADMIN ENABLE FAILPOINT 'fp' WITH 3 TIMES", new SessionVariable()));
    }
}
