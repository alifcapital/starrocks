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
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;

import java.util.OptionalDouble;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;

/**
 * TYPE            |  JAVA_TYPE
 * TYPE_INVALID    |    null
 * TYPE_NULL       |    null
 * TYPE_BOOLEAN    |    boolean
 * TYPE_TINYINT    |    byte
 * TYPE_SMALLINT   |    short
 * TYPE_INT        |    int
 * TYPE_BIGINT     |    long
 * TYPE_LARGEINT   |    BigInteger
 * TYPE_FLOAT      |    double
 * TYPE_DOUBLE     |    double
 * TYPE_DATE       |    LocalDateTime
 * TYPE_DATETIME   |    LocalDateTime
 * TYPE_TIME       |    LocalDateTime
 * TYPE_DECIMAL    |    BigDecimal
 * TYPE_DECIMALV2  |    BigDecimal
 * TYPE_VARCHAR    |    String
 * TYPE_CHAR       |    String
 * TYPE_HLL        |    NOT_SUPPORT
 * TYPE_BITMAP     |    NOT_SUPPORT
 * TYPE_PERCENTILE |    NOT_SUPPORT
 */
public class ConstantOperatorUtils {

    public static double getDoubleValue(ConstantOperator constantOperator) {
        OptionalDouble optionalDouble = doubleValueFromConstant(constantOperator);
        if (optionalDouble.isPresent()) {
            return optionalDouble.getAsDouble();
        } else {
            return Double.NaN;
        }
    }

    public static OptionalDouble doubleValueFromConstant(ConstantOperator constantOperator) {
        Type type = constantOperator.getType();
        // A scalar type equals another one of the same primitive type when it has no length, precision or scale,
        // which holds for all types below. We expect this to run once per literal of an IN list, so we switch
        // on the primitive type instead of comparing with each type in turn.
        if (type instanceof ScalarType) {
            switch (type.getPrimitiveType()) {
                case BOOLEAN:
                    return OptionalDouble.of(constantOperator.getBoolean() ? 1.0 : 0.0);
                case TINYINT:
                    return OptionalDouble.of(constantOperator.getTinyInt());
                case SMALLINT:
                    return OptionalDouble.of(constantOperator.getSmallint());
                case INT:
                    return OptionalDouble.of(constantOperator.getInt());
                case BIGINT:
                    return OptionalDouble.of(constantOperator.getBigint());
                case LARGEINT:
                    return OptionalDouble.of(constantOperator.getLargeInt().doubleValue());
                case FLOAT:
                    return OptionalDouble.of(constantOperator.getFloat());
                case DOUBLE:
                    return OptionalDouble.of(constantOperator.getDouble());
                case DATE:
                    return OptionalDouble.of(getLongFromDateTime(constantOperator.getDate()));
                case DATETIME:
                    return OptionalDouble.of(getLongFromDateTime(constantOperator.getDatetime()));
                case TIME:
                    return OptionalDouble.of(constantOperator.getTime());
                default:
                    break;
            }
        }
        if (type.isDecimalOfAnyVersion()) {
            return OptionalDouble.of(constantOperator.getDecimal().doubleValue());
        }
        return OptionalDouble.empty();
    }
}
