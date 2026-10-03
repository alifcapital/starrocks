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
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Optional;
import java.util.regex.Pattern;

/** Empty means unsupported semantics, not a failed SQL conversion (which returns a typed NULL). */
final class StatisticsCastEvaluator {
    private static final Pattern INTEGER = Pattern.compile("[+-]?[0-9]+");
    private static final Pattern DECIMAL = Pattern.compile("[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?");
    private static final Pattern ISO_DATE = Pattern.compile(
            "[0-9]{4}-[0-9]{2}-[0-9]{2}( [0-9]{2}:[0-9]{2}:[0-9]{2})?");

    private StatisticsCastEvaluator() {
    }

    static Optional<ConstantOperator> cast(ConstantOperator value, Type target) {
        Type source = value.getType();
        if (value.isNull()) {
            return Optional.of(ConstantOperator.createNull(target));
        }
        if (source.equals(target)) {
            return Optional.of(value);
        }
        String text = source.isBoolean() ? (value.getBoolean() ? "1" : "0") : value.toString();
        boolean number = source.isNumericType() || source.isBoolean();
        if (source.isFloatingPointType() && !Double.isFinite(Double.parseDouble(text))) {
            return Optional.empty();
        }
        boolean supported = false;
        try {
            if (target.isBoolean() && (number || source.isStringType())) {
                supported = true;
                if (number) {
                    return Optional.of(ConstantOperator.createBoolean(new BigDecimal(text).signum() != 0));
                }
                text = text.trim();
                if (text.equalsIgnoreCase("true") || text.equalsIgnoreCase("false")) {
                    return Optional.of(ConstantOperator.createBoolean(Boolean.parseBoolean(text)));
                }
                // BE parses VARCHAR -> BOOLEAN as int32, then tests against zero.
                if (!INTEGER.matcher(text).matches()) {
                    return Optional.of(ConstantOperator.createNull(target));
                }
                return Optional.of(ConstantOperator.createBoolean(Integer.parseInt(text) != 0));
            }
            if (target.isFixedPointType() && (number || source.isStringType())) {
                supported = true;
                if (!number && !INTEGER.matcher(text.trim()).matches()) {
                    return Optional.of(ConstantOperator.createNull(target));
                }
                int bits = target.getTypeSize() * 8;
                BigDecimal numeric = source.isFloatingPointType()
                        ? new BigDecimal(source.isFloat() ? (double) (float) Double.parseDouble(text)
                                : Double.parseDouble(text)) : new BigDecimal(text.trim());
                if (source.isFloatingPointType()
                        && (numeric.compareTo(new BigDecimal(BigInteger.ONE.shiftLeft(bits - 1).negate())) < 0
                        || numeric.compareTo(new BigDecimal(BigInteger.ONE.shiftLeft(bits - 1))) >= 0)) {
                    return Optional.of(ConstantOperator.createNull(target));
                }
                BigInteger integer = numeric.toBigInteger();
                if (integer.bitLength() >= bits) {
                    return Optional.of(ConstantOperator.createNull(target));
                }
                return ConstantOperator.createVarchar(integer.toString()).castTo(target);
            }
            if (target.isStringType() && (source.isStringType() || source.isFixedPointType()
                    || source.isBoolean() || source.isDateType())) {
                // BE does not truncate VARCHAR/CHAR casts to the declared length.
                return Optional.of(ConstantOperator.createChar(text, target));
            }
            if (target.isDecimalV3() && (source.isStringType() || source.isFixedPointType()
                    || source.isDecimalV3() || source.isBoolean())) {
                supported = true;
                if (!DECIMAL.matcher(text.trim()).matches()) {
                    return Optional.of(ConstantOperator.createNull(target));
                }
                BigDecimal decimal = new BigDecimal(text.trim());
                int scale = ((ScalarType) target).getScalarScale();
                long digits = (long) decimal.precision() - decimal.scale() + scale;
                if (decimal.signum() != 0 && digits > target.getTypeSize() * 8L) {
                    return Optional.of(ConstantOperator.createNull(target));
                }
                // Avoid constructing enormous powers of ten for extreme textual exponents.
                decimal = digits < 0 ? BigDecimal.ZERO.setScale(scale) : decimal.setScale(scale, RoundingMode.HALF_UP);
                // DecimalV3 uses the physical signed integer width, including rounding carry.
                if (decimal.unscaledValue().bitLength() >= target.getTypeSize() * 8) {
                    return Optional.of(ConstantOperator.createNull(target));
                }
                return Optional.of(ConstantOperator.createDecimal(decimal, target));
            }
            if (target.isDateType() && source.isDateType()) {
                return value.castTo(target);
            }
            if (target.isDateType() && source.isStringType()) {
                // Only the unambiguous ISO representation is evaluated here. BE also accepts
                // other date spellings: unsupported spellings must not be invented NULLs.
                if (!ISO_DATE.matcher(text.trim()).matches()) {
                    return Optional.empty();
                }
                supported = true;
                return Optional.of(value.castTo(target).orElse(ConstantOperator.createNull(target)));
            }
            if (target.isFloatingPointType() && (number || source.isStringType())) {
                supported = true;
                if (source.isStringType() && !DECIMAL.matcher(text.trim()).matches()) {
                    return Optional.of(ConstantOperator.createNull(target));
                }
                double result = Double.parseDouble(text);
                if (target.isFloat()) {
                    result = (float) result;
                }
                if (!Double.isFinite(result)) {
                    return source.isStringType() ? Optional.of(ConstantOperator.createNull(target)) : Optional.empty();
                }
                return Optional.of(target.isFloat() ? ConstantOperator.createFloat(result)
                        : ConstantOperator.createDouble(result));
            }
        } catch (NumberFormatException e) {
            if (supported) {
                return Optional.of(ConstantOperator.createNull(target));
            }
        }
        return Optional.empty();
    }
}
