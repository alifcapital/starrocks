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
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.type.InvalidType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The constants of a LargeInPredicateOperator as values of one type, the type of the RAW VALUES column that
 * LargeInPredicateToJoinRule joins with.
 * <p>
 * The comparison type and the values are those of the InPredicate with the same list after ImplicitCastRule and
 * FoldConstantsRule:
 * <ol>
 *   <li>If every constant has the type of the compared expression, or the compared expression is a variable and
 *   every constant converts to its type with Utils.tryCastConstant, the comparison is in the type of the compared
 *   expression.</li>
 *   <li>Otherwise the comparison is in the type of TypeManager.getCompatibleTypeForBetweenAndIn, and every constant
 *   is folded to that type with ConstantOperator.castTo.</li>
 * </ol>
 * A constant that does not fold stays a CAST in the InPredicate, and BE evaluates it. resolve throws
 * LargeInPredicateException for such a list and for a comparison type that RAW VALUES does not hold, and the
 * statement is planned again with the InPredicate. RAW VALUES does not hold FLOAT and DOUBLE: the hash join compares
 * their keys by bits, so -0.0 does not match 0.0 there as it does in IN.
 * <p>
 * A NULL constant is not a value. IN never matches it, and NOT IN with it is never true.
 */
public final class LargeInConstants {
    private final Type type;
    private final List<Object> values;
    private final boolean hasNull;

    private LargeInConstants(Type type, List<Object> values, boolean hasNull) {
        this.type = type;
        this.values = values;
        this.hasNull = hasNull;
    }

    /**
     * The type of the values. It is VARCHAR for a string comparison and the decimal type of the comparison with the
     * largest precision for a decimal comparison: every value fits it, and it matches the comparison type.
     */
    public Type getType() {
        return type;
    }

    /**
     * The values that are not NULL: Long for integer types, String for string types, BigDecimal for decimal types
     * and LocalDateTime for DATE and DATETIME.
     */
    public List<Object> getValues() {
        return values;
    }

    public boolean hasNull() {
        return hasNull;
    }

    public static LargeInConstants resolve(ScalarOperator compareExpr, List<ConstantOperator> constants) {
        Type compareType = compareExpr.getType();
        List<ConstantOperator> resolved = null;
        if (constants.stream().allMatch(c -> compareType.matchesType(c.getType()))) {
            resolved = constants;
        } else if (compareExpr.isVariable()) {
            // A constant that already has the type fails tryCastConstant as well, as in ImplicitCastRule
            resolved = new ArrayList<>(constants.size());
            for (ConstantOperator constant : constants) {
                Optional<ScalarOperator> cast = Utils.tryCastConstant(constant, compareType);
                if (cast.isEmpty()) {
                    resolved = null;
                    break;
                }
                resolved.add((ConstantOperator) cast.get());
            }
        }

        Type comparisonType = compareType;
        if (resolved == null) {
            List<Type> types = new ArrayList<>(constants.size() + 1);
            types.add(compareType);
            constants.forEach(c -> types.add(c.getType()));
            comparisonType = TypeManager.getCompatibleTypeForBetweenAndIn(types, false);
            if (comparisonType == InvalidType.INVALID) {
                throw new LargeInPredicateException("LargeInPredicate has no comparison type for %s",
                        compareType.toSql());
            }
            resolved = new ArrayList<>(constants.size());
            for (ConstantOperator constant : constants) {
                resolved.add(fold(constant, comparisonType));
            }
        }
        return toValues(comparisonType, resolved);
    }

    // FoldConstantsRule.visitCastOperator of a CAST of the constant to the type
    private static ConstantOperator fold(ConstantOperator constant, Type type) {
        if (constant.getType().matchesType(type)) {
            return constant;
        }
        if (constant.isNull()) {
            return ConstantOperator.createNull(type);
        }
        return constant.castTo(type).orElseThrow(() -> new LargeInPredicateException(
                "LargeInPredicate constant %s does not fold to %s", constant, type.toSql()));
    }

    private static LargeInConstants toValues(Type comparisonType, List<ConstantOperator> constants) {
        PrimitiveType primitiveType = comparisonType.getPrimitiveType();
        Type valueType;
        if (primitiveType == PrimitiveType.TINYINT || primitiveType == PrimitiveType.SMALLINT
                || primitiveType == PrimitiveType.INT || primitiveType == PrimitiveType.BIGINT
                || primitiveType == PrimitiveType.DATE || primitiveType == PrimitiveType.DATETIME) {
            valueType = comparisonType;
        } else if (primitiveType.isCharFamily()) {
            valueType = VarcharType.VARCHAR;
        } else if (primitiveType == PrimitiveType.DECIMAL32 || primitiveType == PrimitiveType.DECIMAL64
                || primitiveType == PrimitiveType.DECIMAL128) {
            valueType = TypeFactory.createDecimalV3Type(primitiveType,
                    PrimitiveType.getMaxPrecisionOfDecimal(primitiveType),
                    ((ScalarType) comparisonType).getScalarScale());
        } else {
            throw new LargeInPredicateException("LargeInPredicate does not support comparison type %s",
                    comparisonType.toSql());
        }

        List<Object> values = new ArrayList<>(constants.size());
        boolean hasNull = false;
        for (ConstantOperator constant : constants) {
            if (constant.isNull()) {
                hasNull = true;
            } else if (valueType.isIntegerType()) {
                values.add(((Number) constant.getValue()).longValue());
            } else if (valueType.isStringType()) {
                values.add(constant.getVarchar());
            } else if (valueType.isDecimalV3()) {
                values.add(constant.getDecimal());
            } else {
                values.add(constant.getDatetime());
            }
        }
        if (values.isEmpty()) {
            throw new LargeInPredicateException("LargeInPredicate has only NULL constants");
        }
        return new LargeInConstants(valueType, values, hasNull);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LargeInConstants that = (LargeInConstants) o;
        return hasNull == that.hasNull && type.equals(that.type) && values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, values, hasNull);
    }
}
