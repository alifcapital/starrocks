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

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LargeInConstantsTest {
    private LargeInConstants resolve(Type type, ConstantOperator... values) {
        return LargeInConstants.resolve(new ColumnRefOperator(1, type, "c", true), List.of(values));
    }

    @Test
    public void testIntegersAndNull() {
        LargeInConstants constants = resolve(IntegerType.BIGINT,
                ConstantOperator.createBigint(Long.MAX_VALUE), ConstantOperator.createBigint(Long.MIN_VALUE),
                ConstantOperator.createBigint(Long.MAX_VALUE), ConstantOperator.createBigint(0),
                ConstantOperator.createNull(IntegerType.BIGINT), ConstantOperator.createBigint(Long.MIN_VALUE));
        assertEquals(List.of(Long.MAX_VALUE, Long.MIN_VALUE, 0L), constants.getValues());
        assertTrue(constants.hasNull());
    }

    @Test
    public void testStringHashCollisionsAndDistinctSpellings() {
        // Aa and BB share a Java hash code but must both survive. A string comparison also distinguishes 01 and 1.
        LargeInConstants constants = resolve(VarcharType.VARCHAR,
                ConstantOperator.createVarchar("Aa"), ConstantOperator.createVarchar("BB"),
                ConstantOperator.createVarchar("Aa"), ConstantOperator.createVarchar("01"),
                ConstantOperator.createVarchar("1"), ConstantOperator.createVarchar(""));
        assertEquals(List.of("Aa", "BB", "01", "1", ""), constants.getValues());
    }

    @ParameterizedTest
    @ValueSource(ints = {9, 18, 38})
    public void testDecimalStorageScale(int precision) {
        PrimitiveType primitive = precision == 9 ? PrimitiveType.DECIMAL32
                : precision == 18 ? PrimitiveType.DECIMAL64 : PrimitiveType.DECIMAL128;
        Type type = TypeFactory.createDecimalV3Type(primitive, precision, 2);
        LargeInConstants constants = resolve(type,
                ConstantOperator.createDecimal(new BigDecimal("1.0"), type),
                ConstantOperator.createDecimal(new BigDecimal("1.00"), type),
                ConstantOperator.createDecimal(new BigDecimal("1.009"), type),
                ConstantOperator.createDecimal(new BigDecimal("-1.009"), type),
                ConstantOperator.createDecimal(new BigDecimal("-1.00"), type),
                ConstantOperator.createDecimal(new BigDecimal("1.01"), type));
        // Packing a DECIMAL truncates toward zero. Hash keys must represent those packed values, including sign.
        assertEquals(List.of(new BigDecimal("1.00"), new BigDecimal("-1.00"), new BigDecimal("1.01")),
                constants.getValues());
    }

    @Test
    public void testDateAndDatetimePrecision() {
        LocalDateTime day = LocalDateTime.of(2024, 1, 2, 0, 0);
        LargeInConstants dates = resolve(DateType.DATE,
                ConstantOperator.createDate(day), ConstantOperator.createDate(day.plusDays(1)),
                ConstantOperator.createDate(day), ConstantOperator.createDate(day.plusHours(1)));
        assertEquals(List.of(day, day.plusDays(1)), dates.getValues());
        LargeInConstants timestamps = resolve(DateType.DATETIME,
                ConstantOperator.createDatetime(day.withNano(1000)),
                ConstantOperator.createDatetime(day.withNano(1999)),
                ConstantOperator.createDatetime(day.withNano(2000)),
                ConstantOperator.createDatetime(day.withNano(1000)));
        assertEquals(List.of(day.withNano(1000), day.withNano(2000)), timestamps.getValues());
    }

    @Test
    public void testAllNullStillUsesFallback() {
        assertThrows(LargeInPredicateException.class, () -> resolve(IntegerType.INT,
                ConstantOperator.createNull(IntegerType.INT), ConstantOperator.createNull(IntegerType.INT)));
    }
}
