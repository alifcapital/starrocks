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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DefaultRewriteRulesColumnRefTest {
    // SqlToScalarOperatorTranslator does not rewrite a translated column ref, because no default rule changes one.
    @Test
    public void testDefaultRulesKeepAColumnRef() {
        List<Type> types = List.of(IntegerType.INT, IntegerType.BIGINT, VarcharType.VARCHAR, BooleanType.BOOLEAN,
                JsonType.JSON);
        for (Type type : types) {
            for (boolean nullable : List.of(true, false)) {
                ColumnRefOperator column = new ColumnRefOperator(1, type, "c", nullable);
                ScalarOperator rewritten = new ScalarOperatorRewriter().rewrite(column,
                        ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                Assertions.assertSame(column, rewritten, type + " nullable " + nullable);
            }
        }
    }
}
