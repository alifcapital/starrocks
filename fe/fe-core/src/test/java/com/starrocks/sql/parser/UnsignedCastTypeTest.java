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
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UnsignedCastTypeTest {
    @Test
    void castToUnsignedIsBigint() {
        for (String type : List.of("UNSIGNED", "UNSIGNED INT", "UNSIGNED INTEGER", "SIGNED", "SIGNED INTEGER")) {
            QueryStatement statement = (QueryStatement) SqlParser.parse(
                    "SELECT CAST(3000000000 AS " + type + ")", new SessionVariable()).get(0);
            CastExpr cast = (CastExpr) ((SelectRelation) statement.getQueryRelation()).getSelectList().getItems()
                    .get(0).getExpr();
            assertEquals(IntegerType.BIGINT, cast.getTargetTypeDef().getType(), type);
        }
    }
}
