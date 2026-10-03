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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StatisticsCastEvaluatorTest {
    @Test
    public void testBooleanSemanticsFromBeColumnOracle() {
        for (long value : new long[] {-2, -1, 0, 1, 2}) {
            Assertions.assertEquals(value != 0, StatisticsCastEvaluator.cast(ConstantOperator.createBigint(value),
                    BooleanType.BOOLEAN).orElseThrow().getBoolean());
        }
        for (String value : new String[] {"2", "-2", "true", " 01 ", "+2"}) {
            Assertions.assertTrue(StatisticsCastEvaluator.cast(ConstantOperator.createVarchar(value),
                    BooleanType.BOOLEAN).orElseThrow().getBoolean());
        }
        for (String value : new String[] {"1.5", "1e3", "2147483648", "bad", ""}) {
            Assertions.assertTrue(StatisticsCastEvaluator.cast(ConstantOperator.createVarchar(value),
                    BooleanType.BOOLEAN).orElseThrow().isNull());
        }
    }

    @Test
    public void testIntegerBoundariesAndScientificNotation() {
        Assertions.assertTrue(StatisticsCastEvaluator.cast(ConstantOperator.createDouble(-128.9),
                IntegerType.TINYINT).orElseThrow().isNull());
        Assertions.assertEquals(127, StatisticsCastEvaluator.cast(ConstantOperator.createDouble(127.9),
                IntegerType.TINYINT).orElseThrow().getTinyInt());
        Assertions.assertEquals(Long.MIN_VALUE, StatisticsCastEvaluator.cast(ConstantOperator.createDouble(-0x1p63),
                IntegerType.BIGINT).orElseThrow().getBigint());
        Assertions.assertEquals(100000000, StatisticsCastEvaluator.cast(ConstantOperator.createDouble(1e8),
                IntegerType.BIGINT).orElseThrow().getBigint());
        Assertions.assertEquals(-1, StatisticsCastEvaluator.cast(ConstantOperator.createDouble(-1.7),
                IntegerType.BIGINT).orElseThrow().getBigint());
        Assertions.assertTrue(StatisticsCastEvaluator.cast(ConstantOperator.createDouble(1e20),
                IntegerType.BIGINT).orElseThrow().isNull());
        Assertions.assertEquals(Long.MIN_VALUE, StatisticsCastEvaluator.cast(
                ConstantOperator.createVarchar("-9223372036854775808"), IntegerType.BIGINT).orElseThrow().getBigint());
        for (String value : new String[] {"9223372036854775808", "1.5", "1e3"}) {
            Assertions.assertTrue(StatisticsCastEvaluator.cast(ConstantOperator.createVarchar(value),
                    IntegerType.BIGINT).orElseThrow().isNull());
        }
        Assertions.assertTrue(StatisticsCastEvaluator.cast(
                ConstantOperator.createVarchar("170141183460469231731687303715884105728"),
                IntegerType.LARGEINT).orElseThrow().isNull());
    }

    @Test
    public void testUnsupportedIsNotSqlNull() {
        Assertions.assertTrue(StatisticsCastEvaluator.cast(ConstantOperator.createVarchar("20250102"),
                DateType.DATE).isEmpty());
        Assertions.assertTrue(StatisticsCastEvaluator.cast(ConstantOperator.createVarchar("2025-02-29"),
                DateType.DATE).orElseThrow().isNull());
        Assertions.assertFalse(StatisticsCastEvaluator.cast(ConstantOperator.createVarchar("2024-02-29"),
                DateType.DATE).orElseThrow().isNull());
        Assertions.assertTrue(StatisticsCastEvaluator.cast(ConstantOperator.createDouble(1e20),
                VarcharType.VARCHAR).isEmpty());
        Assertions.assertTrue(StatisticsCastEvaluator.cast(ConstantOperator.createNull(DateType.DATE),
                IntegerType.INT).orElseThrow().isNull());
    }

    @Test
    public void testDecimalRoundingAndExtremeExponent() {
        com.starrocks.type.DecimalType type = new com.starrocks.type.DecimalType(
                com.starrocks.type.PrimitiveType.DECIMAL64, 10, 1);
        Assertions.assertEquals(new java.math.BigDecimal("-1.3"), StatisticsCastEvaluator.cast(
                ConstantOperator.createVarchar("-1.25"), type).orElseThrow().getDecimal());
        Assertions.assertEquals(new java.math.BigDecimal("1000.0"), StatisticsCastEvaluator.cast(
                ConstantOperator.createVarchar("1e3"), type).orElseThrow().getDecimal());
        Assertions.assertEquals(new java.math.BigDecimal("0.0"), StatisticsCastEvaluator.cast(
                ConstantOperator.createVarchar("1e-10000000"), type).orElseThrow().getDecimal());
        Assertions.assertTrue(StatisticsCastEvaluator.cast(ConstantOperator.createVarchar("1e10000000"),
                type).orElseThrow().isNull());
    }

    @Test
    public void testFloatHeadMatchesStoredFloatAndWideningRetainsItsValue() {
        var type = com.starrocks.type.FloatType.FLOAT;
        ConstantOperator number = StatisticsCastEvaluator.cast(ConstantOperator.createVarchar("0.1"), type).orElseThrow();
        Assertions.assertEquals(MultiColumnJoinMcvEstimator.canonical(type, "0.1"),
                MultiColumnJoinMcvEstimator.canonical(type, number.toString()));
        var source = new com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator(1, type, "f", true);
        var cast = new com.starrocks.sql.optimizer.operator.scalar.CastOperator(com.starrocks.type.FloatType.DOUBLE, source);
        Assertions.assertEquals((double) 0.1f, MultiColumnMcvEstimator.evaluate(cast, source, "0.1").orElseThrow().getDouble());
    }
}
