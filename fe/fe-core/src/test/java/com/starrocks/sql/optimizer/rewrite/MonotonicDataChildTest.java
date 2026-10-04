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

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class MonotonicDataChildTest {
    private final ColumnRefOperator dateCol = new ColumnRefOperator(1, DateType.DATE, "d", true);
    private final ColumnRefOperator otherCol = new ColumnRefOperator(2, IntegerType.INT, "n", true);
    private final ConstantOperator one = ConstantOperator.createInt(1);

    private static CallOperator call(String name, ScalarOperator... args) {
        return new CallOperator(name, DateType.DATE, Lists.newArrayList(args));
    }

    @Test
    public void testSingleColumnInAdmittedPosition() {
        assertSame(dateCol, MonotonicFunctionRegistry.dataChildOf(call("days_add", dateCol, one)));
        assertSame(dateCol, MonotonicFunctionRegistry.dataChildOf(call("DAYS_ADD", dateCol, one)));
        assertSame(otherCol, MonotonicFunctionRegistry.dataChildOf(
                call("days_add", ConstantOperator.createDate(LocalDateTime.of(2024, 1, 1, 0, 0)), otherCol)));
        assertSame(dateCol, MonotonicFunctionRegistry.dataChildOf(
                call("date_trunc", ConstantOperator.createVarchar("day"), dateCol)));
    }

    @Test
    public void testExpressionOverOneColumnIsTheDataChild() {
        CallOperator shifted = call("days_add", dateCol, one);
        assertSame(shifted, MonotonicFunctionRegistry.dataChildOf(call("year", shifted)));
    }

    @Test
    public void testZeroOrSeveralColumnReferencesAreRefused() {
        assertNull(MonotonicFunctionRegistry.dataChildOf(call("days_add", ConstantOperator.createInt(5), one)));
        assertNull(MonotonicFunctionRegistry.dataChildOf(call("days_add", dateCol, otherCol)));
        // the same column twice is still two occurrences
        assertNull(MonotonicFunctionRegistry.dataChildOf(call("datediff", dateCol, dateCol)));
        assertNull(MonotonicFunctionRegistry.dataChildOf(
                call("days_add", call("days_add", dateCol, one), dateCol)));
    }

    @Test
    public void testNotAdmittedFunctionPositionOrNullConstantIsRefused() {
        assertNull(MonotonicFunctionRegistry.dataChildOf(call("abs", dateCol)));
        assertNull(MonotonicFunctionRegistry.dataChildOf(
                call("date_trunc", dateCol, ConstantOperator.createVarchar("day"))));
        assertNull(MonotonicFunctionRegistry.dataChildOf(
                call("days_add", dateCol, ConstantOperator.createNull(IntegerType.INT))));
        assertNull(MonotonicFunctionRegistry.dataChildOf(
                call("date_format", dateCol, new ColumnRefOperator(3, VarcharType.VARCHAR, "f", true))));
    }
}
