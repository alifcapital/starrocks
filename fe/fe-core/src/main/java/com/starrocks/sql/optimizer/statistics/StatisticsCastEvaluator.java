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
import com.starrocks.type.DateType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * One value cast as the BE casts it at run time. We expect the statistics of a cast to be built from values of the
 * source, so each value must come out as the BE computes it, including the values the BE turns into NULL.
 * Empty means unsupported semantics, not a failed SQL conversion (which returns a typed NULL).
 */
final class StatisticsCastEvaluator {
    private static final Pattern INTEGER = Pattern.compile("[+-]?[0-9]+");
    private static final BigInteger[] POW10 = new BigInteger[80];

    static {
        POW10[0] = BigInteger.ONE;
        for (int i = 1; i < POW10.length; i++) {
            POW10[i] = POW10[i - 1].multiply(BigInteger.TEN);
        }
    }

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
        try {
            if (target.isBoolean()) {
                return toBoolean(value, source, target);
            }
            if (target.isFixedPointType()) {
                return toInteger(value, source, target);
            }
            if (target.isFloatingPointType()) {
                return toFloatingPoint(value, source, target);
            }
            if (target.isDecimalV3()) {
                return toDecimal(value, source, (ScalarType) target);
            }
            if (target.isDateType()) {
                return toDate(value, source, target);
            }
            if (target.isStringType()) {
                return toText(value, source, target);
            }
        } catch (RuntimeException e) {
            // A failure here is a gap of the port, not a NULL of the BE
            return Optional.empty();
        }
        return Optional.empty();
    }

    private static Optional<ConstantOperator> toBoolean(ConstantOperator value, Type source, Type target) {
        boolean number = source.isNumericType();
        if (!number && !source.isStringType()) {
            return Optional.empty();
        }
        String text = value.toString();
        if (source.isFloatingPointType() && !Double.isFinite(value.getDouble())) {
            return Optional.empty();
        }
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
        try {
            return Optional.of(ConstantOperator.createBoolean(Integer.parseInt(text) != 0));
        } catch (NumberFormatException e) {
            return Optional.of(ConstantOperator.createNull(target));
        }
    }

    // TINYINT to LARGEINT: a narrowing of an integer or a floating point value outside the type is NULL
    // (UNARY_FN_CAST_VALID), a date or datetime wraps around (UNARY_FN_CAST).
    private static Optional<ConstantOperator> toInteger(ConstantOperator value, Type source, Type target) {
        int bits = integerBits(target);
        BigInteger result;
        if (source.isBoolean()) {
            result = value.getBoolean() ? BigInteger.ONE : BigInteger.ZERO;
        } else if (source.isFixedPointType()) {
            result = integerValue(value);
        } else if (source.isFloatingPointType()) {
            double number = floatingValue(value);
            double bound = Math.scalb(1.0, bits - 1);
            if (!(number >= -bound && number < bound)) {
                return Optional.of(ConstantOperator.createNull(target));
            }
            result = new BigDecimal(number).toBigInteger();
        } else if (source.isDecimalV3()) {
            result = value.getDecimal().toBigInteger();
        } else if (source.isDate()) {
            result = BigInteger.valueOf(wrap(dateLiteral(value.getDate()), bits));
        } else if (source.isDatetime()) {
            result = BigInteger.valueOf(wrap(datetimeLiteral(value.getDatetime()), bits));
        } else if (source.isStringType()) {
            result = parseInteger(text(value), bits);
        } else {
            return Optional.empty();
        }
        if (result == null || result.bitLength() >= bits) {
            return Optional.of(ConstantOperator.createNull(target));
        }
        return Optional.of(integerConstant(result, target));
    }

    private static Optional<ConstantOperator> toFloatingPoint(ConstantOperator value, Type source, Type target) {
        boolean toFloat = target.isFloat();
        double result;
        if (source.isBoolean()) {
            result = value.getBoolean() ? 1 : 0;
        } else if (source.isFixedPointType()) {
            BigInteger integer = integerValue(value);
            result = toFloat ? integer.floatValue() : integer.doubleValue();
        } else if (source.isFloatingPointType()) {
            result = floatingValue(value);
            if (toFloat && (result < -Float.MAX_VALUE || result > Float.MAX_VALUE)) {
                return Optional.of(ConstantOperator.createNull(target));
            }
        } else if (source.isDecimalV3()) {
            BigDecimal decimal = value.getDecimal();
            result = decimal.unscaledValue().doubleValue() / POW10[decimal.scale()].doubleValue();
        } else if (source.isDate()) {
            result = dateLiteral(value.getDate());
        } else if (source.isDatetime()) {
            result = datetimeLiteral(value.getDatetime());
        } else if (source.isStringType()) {
            Double parsed = parseFloatingPoint(text(value));
            if (parsed == null) {
                return Optional.of(ConstantOperator.createNull(target));
            }
            result = parsed;
        } else {
            return Optional.empty();
        }
        if (toFloat) {
            result = (float) result;
        }
        if (!Double.isFinite(result)) {
            return Optional.of(ConstantOperator.createNull(target));
        }
        return Optional.of(toFloat ? ConstantOperator.createFloat(result) : ConstantOperator.createDouble(result));
    }

    // DECIMAL32 to DECIMAL256: a value that does not fit the physical integer of the type is NULL, the declared
    // precision is not checked (DecimalV3Cast in runtime/decimalv3.h).
    private static Optional<ConstantOperator> toDecimal(ConstantOperator value, Type source, ScalarType target) {
        int scale = target.getScalarScale();
        int bits = decimalBits(target);
        BigInteger unscaled;
        if (source.isBoolean()) {
            unscaled = (value.getBoolean() ? BigInteger.ONE : BigInteger.ZERO).multiply(POW10[scale]);
        } else if (source.isFixedPointType()) {
            unscaled = integerValue(value).multiply(POW10[scale]);
        } else if (source.isFloatingPointType()) {
            unscaled = decimalFromFloatingPoint(floatingValue(value), scale, bits);
        } else if (source.isDecimalV3()) {
            // A smaller scale rounds half away from zero
            unscaled = value.getDecimal().setScale(scale, RoundingMode.HALF_UP).unscaledValue();
        } else if (source.isDate()) {
            unscaled = BigInteger.valueOf(dateLiteral(value.getDate())).multiply(POW10[scale]);
        } else if (source.isDatetime()) {
            unscaled = BigInteger.valueOf(datetimeLiteral(value.getDatetime())).multiply(POW10[scale]);
        } else if (source.isStringType()) {
            unscaled = parseDecimal(text(value), scale, decimalDigits(target));
        } else {
            return Optional.empty();
        }
        if (unscaled == null || unscaled.bitLength() >= bits) {
            return Optional.of(ConstantOperator.createNull(target));
        }
        return Optional.of(ConstantOperator.createDecimal(new BigDecimal(unscaled, scale), target));
    }

    private static Optional<ConstantOperator> toDate(ConstantOperator value, Type source, Type target) {
        if (source.isDateType()) {
            return value.castTo(target);
        }
        boolean toDate = target.isDate();
        LocalDateTime result;
        if (source.isStringType()) {
            result = StatisticsDatetimeParser.parse(text(value));
        } else if (source.isFixedPointType()) {
            BigInteger integer = integerValue(value);
            // CAST AS DATETIME takes positive values only, both read the low 64 bits as a signed integer
            result = !toDate && integer.signum() <= 0 ? null : StatisticsDatetimeParser.fromNumber(integer.longValue());
        } else if (source.isFloatingPointType()) {
            double number = floatingValue(value);
            // The integer part, toward zero; a value past the 14 digits of a datetime is NULL either way
            result = Math.abs(number) >= 1e15 || (!toDate && !(number > 0)) ? null
                    : StatisticsDatetimeParser.fromNumber((long) number);
        } else if (source.isDecimalV3()) {
            BigInteger integer = value.getDecimal().toBigInteger();
            result = integer.bitLength() >= 64 ? null : StatisticsDatetimeParser.fromNumber(integer.longValue());
        } else {
            return Optional.empty();
        }
        if (result == null) {
            return Optional.of(ConstantOperator.createNull(target));
        }
        // The BE holds microseconds up to 9999-12-31 23:59:59.999999, past the checked range of the FE
        return Optional.of(toDate ? ConstantOperator.createDate(result.toLocalDate().atStartOfDay())
                : ConstantOperator.createDatetime(result, DateType.DATETIME));
    }

    private static Optional<ConstantOperator> toText(ConstantOperator value, Type source, Type target) {
        String text;
        if (source.isStringType()) {
            // BE does not truncate VARCHAR/CHAR casts to the declared length.
            text = text(value);
        } else if (source.isBoolean()) {
            text = value.getBoolean() ? "1" : "0";
        } else if (source.isFixedPointType()) {
            text = integerValue(value).toString();
        } else if (source.isDate()) {
            text = formatDate(value.getDate());
        } else if (source.isDatetime()) {
            text = formatDatetime(value.getDatetime());
        } else {
            return Optional.empty();
        }
        return Optional.of(ConstantOperator.createChar(text, target));
    }

    // YYYY-MM-DD with a four digit year, as date::to_string writes it
    static String formatDate(LocalDateTime value) {
        StringBuilder text = new StringBuilder(10);
        appendPadded(text, value.getYear(), 4).append('-');
        appendPadded(text, value.getMonthValue(), 2).append('-');
        return appendPadded(text, value.getDayOfMonth(), 2).toString();
    }

    // YYYY-MM-DD HH:MM:SS with six digits of microseconds when they are not zero, as TimestampValue::to_string
    static String formatDatetime(LocalDateTime value) {
        StringBuilder text = new StringBuilder(26).append(formatDate(value)).append(' ');
        appendPadded(text, value.getHour(), 2).append(':');
        appendPadded(text, value.getMinute(), 2).append(':');
        appendPadded(text, value.getSecond(), 2);
        int micros = value.getNano() / 1000;
        if (micros != 0) {
            appendPadded(text.append('.'), micros, 6);
        }
        return text.toString();
    }

    private static StringBuilder appendPadded(StringBuilder text, int value, int width) {
        String digits = Integer.toString(value);
        for (int i = digits.length(); i < width; i++) {
            text.append('0');
        }
        return text.append(digits);
    }

    // DateValue::to_date_literal: YYYYMMDD
    static long dateLiteral(LocalDateTime date) {
        return date.getYear() * 10000L + date.getMonthValue() * 100L + date.getDayOfMonth();
    }

    // TimestampValue::to_timestamp_literal: YYYYMMDDHHMMSS without the microseconds
    static long datetimeLiteral(LocalDateTime datetime) {
        return dateLiteral(datetime) * 1000000L + datetime.getHour() * 10000L + datetime.getMinute() * 100L
                + datetime.getSecond();
    }

    private static long wrap(long value, int bits) {
        switch (bits) {
            case 8:
                return (byte) value;
            case 16:
                return (short) value;
            case 32:
                return (int) value;
            default:
                return value;
        }
    }

    static int integerBits(Type type) {
        switch (type.getPrimitiveType()) {
            case TINYINT:
                return 8;
            case SMALLINT:
                return 16;
            case INT:
                return 32;
            case BIGINT:
                return 64;
            default:
                return 128;
        }
    }

    static int decimalBits(Type type) {
        PrimitiveType primitive = type.getPrimitiveType();
        if (primitive == PrimitiveType.DECIMAL32) {
            return 32;
        }
        if (primitive == PrimitiveType.DECIMAL64) {
            return 64;
        }
        return primitive == PrimitiveType.DECIMAL256 ? 256 : 128;
    }

    // decimal_precision_limit: the digits the physical integer of the type holds
    private static int decimalDigits(Type type) {
        switch (decimalBits(type)) {
            case 32:
                return 9;
            case 64:
                return 18;
            case 256:
                return 76;
            default:
                return 38;
        }
    }

    static ConstantOperator integerConstant(BigInteger value, Type target) {
        switch (target.getPrimitiveType()) {
            case TINYINT:
                return ConstantOperator.createTinyInt(value.byteValue());
            case SMALLINT:
                return ConstantOperator.createSmallInt(value.shortValue());
            case INT:
                return ConstantOperator.createInt(value.intValue());
            case BIGINT:
                return ConstantOperator.createBigint(value.longValue());
            default:
                return ConstantOperator.createLargeInt(value);
        }
    }

    private static BigInteger integerValue(ConstantOperator value) {
        switch (value.getType().getPrimitiveType()) {
            case TINYINT:
                return BigInteger.valueOf(value.getTinyInt());
            case SMALLINT:
                return BigInteger.valueOf(value.getSmallint());
            case INT:
                return BigInteger.valueOf(value.getInt());
            case BIGINT:
                return BigInteger.valueOf(value.getBigint());
            default:
                return value.getLargeInt();
        }
    }

    // A FLOAT holds a float in the BE: the statistics can hold a double that no float equals
    private static double floatingValue(ConstantOperator value) {
        return value.getType().isFloat() ? (float) value.getFloat() : value.getDouble();
    }

    private static String text(ConstantOperator value) {
        return (String) value.getValue();
    }

    // StringParser::is_whitespace
    private static boolean isWhitespace(char c) {
        return c == ' ' || (c >= 9 && c <= 13);
    }

    /**
     * StringParser::string_to_int: leading and trailing white space, a sign and at least one digit. Null when the
     * BE gives NULL, as for '1.5', '1e3' or a value outside the type.
     */
    static BigInteger parseInteger(String text, int bits) {
        int begin = 0;
        int end = text.length();
        while (begin < end && isWhitespace(text.charAt(begin))) {
            begin++;
        }
        while (end > begin && isWhitespace(text.charAt(end - 1))) {
            end--;
        }
        int digits = begin;
        if (digits < end && (text.charAt(digits) == '-' || text.charAt(digits) == '+')) {
            digits++;
        }
        if (digits == end) {
            return null;
        }
        for (int i = digits; i < end; i++) {
            if (!StatisticsDatetimeParser.isDigit(text.charAt(i))) {
                return null;
            }
        }
        BigInteger value = new BigInteger(text.substring(digits, end));
        if (text.charAt(begin) == '-') {
            value = value.negate();
        }
        return value.bitLength() >= bits ? null : value;
    }

    /**
     * StringParser::string_to_float, which the BE casts to NULL when it fails or gives an infinity or NaN; null for
     * NULL. An underflow to zero may differ from the BE and is not supported.
     */
    static Double parseFloatingPoint(String text) {
        int begin = 0;
        int end = text.length();
        while (begin < end && isWhitespace(text.charAt(begin))) {
            begin++;
        }
        while (end > begin && isWhitespace(text.charAt(end - 1))) {
            end--;
        }
        boolean negative = false;
        if (begin < end && (text.charAt(begin) == '-' || text.charAt(begin) == '+')) {
            negative = text.charAt(begin) == '-';
            begin++;
        }
        if (begin == end) {
            return null;
        }
        boolean exponent = false;
        boolean nonZero = false;
        for (int i = begin; i < end; i++) {
            char c = text.charAt(i);
            if (StatisticsDatetimeParser.isDigit(c)) {
                nonZero |= c != '0' && !exponent;
            } else if (c == 'e' || c == 'E') {
                if (exponent) {
                    return null;
                }
                exponent = true;
            } else if (c == '-' || c == '+') {
                if (i == begin || (text.charAt(i - 1) != 'e' && text.charAt(i - 1) != 'E')) {
                    return null;
                }
            } else if (c != '.') {
                // Includes inf and nan, which the BE parses and then casts to NULL
                return null;
            }
        }
        double value;
        try {
            value = Double.parseDouble(text.substring(begin, end));
        } catch (NumberFormatException e) {
            return null;
        }
        if (!Double.isFinite(value)) {
            return null;
        }
        if (value == 0 && nonZero) {
            throw new UnsupportedOperationException("underflow of " + text);
        }
        return negative ? -value : value;
    }

    /**
     * StringParser::string_to_decimal with the digits of the physical type as its precision: the unscaled value
     * of the scale, or null for NULL. Digits past the precision are dropped, a smaller scale rounds half up, '.' is
     * zero and an exponent below the range of int8 gives zero.
     */
    static BigInteger parseDecimal(String text, int typeScale, int typePrecision) {
        int begin = 0;
        int end = text.length();
        while (begin < end && isWhitespace(text.charAt(begin))) {
            begin++;
        }
        while (end > begin && isWhitespace(text.charAt(end - 1))) {
            end--;
        }
        boolean negative = false;
        if (begin < end && (text.charAt(begin) == '-' || text.charAt(begin) == '+')) {
            negative = text.charAt(begin) == '-';
            begin++;
        }
        boolean foundValue = false;
        while (begin < end && text.charAt(begin) == '0') {
            foundValue = true;
            begin++;
        }
        int scale = 0;
        int foundDot = 0;
        if (begin < end && text.charAt(begin) == '.') {
            foundDot = 1;
            begin++;
            while (begin < end && text.charAt(begin) == '0') {
                foundValue = true;
                scale++;
                begin++;
            }
        }
        int precision = 0;
        boolean foundExponent = false;
        int exponent = 0;
        BigInteger value = BigInteger.ZERO;
        for (int i = begin; i < end; i++) {
            char c = text.charAt(i);
            if (StatisticsDatetimeParser.isDigit(c)) {
                foundValue = true;
                if (typePrecision > precision) {
                    value = value.multiply(BigInteger.TEN).add(BigInteger.valueOf(c - '0'));
                }
                precision++;
                scale += foundDot;
            } else if (c == '.' && foundDot == 0) {
                foundDot = 1;
            } else if ((c == 'e' || c == 'E') && !foundExponent) {
                foundExponent = true;
                // The exponent is an int8: a sign and digits only. Below its range the BE returns zero, above it
                // NULL.
                int p = i + 1;
                boolean minus = p < end && text.charAt(p) == '-';
                if (p < end && (minus || text.charAt(p) == '+')) {
                    p++;
                }
                if (p == end) {
                    return null;
                }
                for (int k = p; k < end; k++) {
                    if (!StatisticsDatetimeParser.isDigit(text.charAt(k))) {
                        return null;
                    }
                }
                BigInteger parsed = new BigInteger(text.substring(p, end));
                if (minus) {
                    parsed = parsed.negate();
                }
                if (parsed.compareTo(BigInteger.valueOf(Byte.MIN_VALUE)) < 0) {
                    return BigInteger.ZERO;
                }
                if (parsed.compareTo(BigInteger.valueOf(Byte.MAX_VALUE)) > 0) {
                    return null;
                }
                exponent = parsed.intValue();
                break;
            } else {
                return null;
            }
        }
        int truncated = precision - typePrecision;
        if (exponent > scale) {
            int shift = exponent - scale;
            precision += shift;
            if (shift > typePrecision) {
                return null;
            }
            value = value.multiply(POW10[shift]);
            scale = 0;
        } else {
            scale -= exponent;
        }
        if (scale > precision) {
            precision = scale;
        }
        if (precision - scale > typePrecision - typeScale) {
            return null;
        } else if (scale > typeScale) {
            int shift = scale - typeScale;
            if (truncated > 0) {
                shift -= truncated;
            }
            if (shift > 0) {
                if (shift <= typePrecision) {
                    BigInteger[] division = value.divideAndRemainder(POW10[shift]);
                    value = division[0];
                    if (division[1].compareTo(POW10[shift].shiftRight(1)) >= 0) {
                        value = value.add(BigInteger.ONE);
                    }
                } else {
                    value = BigInteger.ZERO;
                }
            }
        } else if (!foundValue && foundDot == 0) {
            return null;
        }
        if (typeScale > scale) {
            value = value.multiply(POW10[typeScale - scale]);
        }
        return negative ? value.negate() : value;
    }

    /**
     * DecimalV3Cast::from_float: (integer) (10^scale * value + 0.5 * sign) in double arithmetic, not exact decimal
     * rounding; null when the value does not fit the physical integer.
     */
    static BigInteger decimalFromFloatingPoint(double value, int scale, int bits) {
        double scaled = POW10[scale].doubleValue() * value + (value >= 0 ? 0.5 : -0.5);
        double bound = Math.scalb(1.0, bits - 1);
        if (!(scaled >= -bound && scaled < bound)) {
            return null;
        }
        BigInteger result = new BigDecimal(scaled).toBigInteger();
        // For 32 and 64 bits the BE reads the largest and the smallest integer as an overflow
        BigInteger largest = BigInteger.ONE.shiftLeft(bits - 1);
        if (bits <= 64 && (result.equals(largest.subtract(BigInteger.ONE)) || result.equals(largest.negate()))) {
            return null;
        }
        return result;
    }
}
