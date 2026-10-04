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
import com.starrocks.sql.ast.UpdateStmt;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UpdateFromPivotTest {
    @Test
    void rejectPivotInUpdateFrom() {
        ParsingException error = assertThrows(ParsingException.class, () -> SqlParser.parse(
                "UPDATE dst SET v = 1 FROM src PIVOT (sum(x) FOR k IN (1, 2)) WHERE dst.id = src.id",
                new SessionVariable()));
        assertTrue(error.getMessage().contains("PIVOT is not supported in the FROM clause of UPDATE"),
                error.getMessage());
    }

    @Test
    void keepUpdateFromWithoutPivot() {
        assertInstanceOf(UpdateStmt.class, SqlParser.parse(
                "UPDATE dst SET v = 1 FROM src WHERE dst.id = src.id", new SessionVariable()).get(0));
    }
}
