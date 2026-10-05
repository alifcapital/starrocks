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

package com.starrocks.sql.optimizer;

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
import java.util.OptionalDouble;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConstantOperatorUtilsTest {


    @Test
    public void getDoubleValue() {

        ConstantOperator constant0 = ConstantOperator.createTinyInt((byte) 1);
        ConstantOperator constant1 = ConstantOperator.createInt(1000);
        ConstantOperator constant2 = ConstantOperator.createSmallInt((short) 12);
        ConstantOperator constant3 = ConstantOperator.createBigint(1000000);

        ConstantOperator constant4 = ConstantOperator.createFloat(1.5);
        ConstantOperator constant5 = ConstantOperator.createDouble(6.789);

        ConstantOperator constant6 = ConstantOperator.createBoolean(true);
        ConstantOperator constant7 = ConstantOperator.createBoolean(false);

        ConstantOperator constant8 = ConstantOperator.createDate(LocalDateTime.of(2003, 10, 11, 23, 56, 25));
        ConstantOperator constant9 = ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 11, 23, 56, 25));
        ConstantOperator constant10 = ConstantOperator.createTime(124578990d);

        ConstantOperator constant11 = ConstantOperator.createVarchar("123");

        assertEquals(ConstantOperatorUtils.getDoubleValue(constant0), 1, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant1), 1000, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant2), 12, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant3), 1000000, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant4), 1.5, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant5), 6.789, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant6), 1, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant7), 0, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant8), 1065887785, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant9), 1065887785, 0.1);
        assertEquals(ConstantOperatorUtils.getDoubleValue(constant10), 124578990, 0.1);

        assertEquals(ConstantOperatorUtils.getDoubleValue(constant11), Double.NaN, 0.0);

    }

    // The reference compares the type with each scalar type in turn.
    private static OptionalDouble chainOfComparisons(ConstantOperator constantOperator) {
        if (BooleanType.BOOLEAN.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getBoolean() ? 1.0 : 0.0);
        } else if (IntegerType.TINYINT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getTinyInt());
        } else if (IntegerType.SMALLINT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getSmallint());
        } else if (IntegerType.INT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getInt());
        } else if (IntegerType.BIGINT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getBigint());
        } else if (IntegerType.LARGEINT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getLargeInt().doubleValue());
        } else if (FloatType.FLOAT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getFloat());
        } else if (FloatType.DOUBLE.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getDouble());
        } else if (DateType.DATE.equals(constantOperator.getType())) {
            return OptionalDouble.of(Utils.getLongFromDateTime(constantOperator.getDate()));
        } else if (DateType.DATETIME.equals(constantOperator.getType())) {
            return OptionalDouble.of(Utils.getLongFromDateTime(constantOperator.getDatetime()));
        } else if (DateType.TIME.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getTime());
        } else if (constantOperator.getType().isDecimalOfAnyVersion()) {
            return OptionalDouble.of(constantOperator.getDecimal().doubleValue());
        }
        return OptionalDouble.empty();
    }

    @Test
    public void doubleValueMatchesTheChainOfTypeComparisons() {
        List<ConstantOperator> constants = List.of(
                ConstantOperator.createBoolean(true), ConstantOperator.createBoolean(false),
                ConstantOperator.createTinyInt((byte) -3), ConstantOperator.createSmallInt((short) 300),
                ConstantOperator.createInt(Integer.MIN_VALUE), ConstantOperator.createBigint(Long.MAX_VALUE),
                ConstantOperator.createLargeInt(new BigInteger("170141183460469231731687303715884105727")),
                ConstantOperator.createFloat(1.5), ConstantOperator.createDouble(-0.0),
                ConstantOperator.createDate(LocalDateTime.of(2026, 10, 5, 0, 0)),
                ConstantOperator.createDatetime(LocalDateTime.of(2026, 10, 5, 12, 34, 56)),
                ConstantOperator.createTime(3600),
                ConstantOperator.createDecimal(new BigDecimal("1.25"), new DecimalType(PrimitiveType.DECIMAL32, 9, 2)),
                ConstantOperator.createDecimal(new BigDecimal("1.25"), new DecimalType(PrimitiveType.DECIMAL64, 18, 2)),
                ConstantOperator.createDecimal(new BigDecimal("1.25"),
                        new DecimalType(PrimitiveType.DECIMAL128, 38, 20)),
                ConstantOperator.createDecimal(new BigDecimal("1.25"), new DecimalType(PrimitiveType.DECIMALV2, 27, 9)),
                ConstantOperator.createVarchar("1"), ConstantOperator.createChar("1"));
        for (ConstantOperator constant : constants) {
            assertEquals(chainOfComparisons(constant), ConstantOperatorUtils.doubleValueFromConstant(constant),
                    constant.getType().toString());
        }
    }

}
