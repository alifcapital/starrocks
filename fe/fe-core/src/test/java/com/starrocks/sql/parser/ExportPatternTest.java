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

class ExportPatternTest {
    @Test
    void rejectLike() {
        for (String sql : List.of("SHOW EXPORT LIKE 'label%'", "SHOW EXPORT FROM db LIKE 'label%'",
                "CANCEL EXPORT LIKE 'label%'")) {
            ParsingException error = assertThrows(ParsingException.class,
                    () -> SqlParser.parse(sql, new SessionVariable()), sql);
            assertTrue(error.getMessage().contains("LIKE is not supported for"), error.getMessage());
        }
    }

    @Test
    void keepWhere() {
        assertDoesNotThrow(() -> SqlParser.parse("SHOW EXPORT WHERE STATE = 'FINISHED'", new SessionVariable()));
        assertDoesNotThrow(() -> SqlParser.parse("CANCEL EXPORT WHERE queryid = 'x'", new SessionVariable()));
    }
}
