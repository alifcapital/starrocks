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

import com.starrocks.sql.optimizer.Utils;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * What a CAST between two types does to the order of the values, as the BE computes the cast. The statistics of a
 * cast and the estimates of comparisons over it follow from this order: a cast that keeps the order maps the range
 * and the histogram of its source, a cast that does not keeps only the facts that do not depend on the order.
 * MonotonicImage decides which casts the pushdown may invert; that is a separate decision with stricter needs.
 */
public final class CastStatisticsRules {
    public enum Order {
        /** Every value stays the same value, so the cast has the statistics of its source. */
        PRESERVED,
        /** Distinct values stay distinct and in order. */
        STRICT,
        /** Values stay in order, and several can become one, such as the times of one day. */
        NON_DECREASING,
        /**
         * A number read as a date: the order holds inside the eight digit dates and inside the fourteen digit
         * datetimes, and breaks between them and in the shorter forms.
         */
        SEGMENTED,
        /** Text read as a date: the order holds only among strings of one layout. */
        LAYOUT,
        /** The cast reorders the values, as a number written as text: '10' < '9'. */
        NONE,
        /** The statistics do not cover the pair, such as JSON, TIME or BOOLEAN. */
        UNSUPPORTED
    }

    // A DATE as the integer YYYYMMDD and a DATETIME as YYYYMMDDHHMMSS: the segments where a number read as a date
    // keeps its order, see date::standardize_date.
    static final double DATE_SEGMENT_LOW = 10000101;
    static final double DATE_SEGMENT_HIGH = 99991231;
    static final double DATETIME_SEGMENT_LOW = 10000101000000.0;
    static final double DATETIME_SEGMENT_HIGH = 99991231235959.0;

    private CastStatisticsRules() {
    }

    public static Order order(Type from, Type to) {
        if (from.equals(to)) {
            return Order.PRESERVED;
        }
        if (!from.isScalarType() || !to.isScalarType()) {
            return Order.UNSUPPORTED;
        }
        if (from.isStringType()) {
            if (to.isStringType()) {
                // The BE does not cut a string to the declared length
                return Order.PRESERVED;
            }
            if (to.isDateType()) {
                return Order.LAYOUT;
            }
            return isNumber(to) ? Order.NONE : Order.UNSUPPORTED;
        }
        if (to.isStringType()) {
            if (from.isDateType()) {
                // A four digit year and fixed width fields: the text sorts as the date
                return Order.STRICT;
            }
            return isNumber(from) ? Order.NONE : Order.UNSUPPORTED;
        }
        if (from.isDate()) {
            if (to.isDatetime()) {
                return Order.STRICT;
            }
            if (to.isFixedPointType()) {
                // YYYYMMDD wraps around in TINYINT and SMALLINT
                return to.getTypeSize() >= 4 ? Order.STRICT : Order.NONE;
            }
            if (to.isDouble() || to.isDecimalV3()) {
                return Order.STRICT;
            }
            return to.isFloat() ? Order.NON_DECREASING : Order.UNSUPPORTED;
        }
        if (from.isDatetime()) {
            if (to.isDate()) {
                return Order.NON_DECREASING;
            }
            if (to.isFixedPointType()) {
                // YYYYMMDDHHMMSS wraps around below BIGINT
                return to.getTypeSize() >= 8 ? Order.NON_DECREASING : Order.NONE;
            }
            return to.isFloatingPointType() || to.isDecimalV3() ? Order.NON_DECREASING : Order.UNSUPPORTED;
        }
        // A DECIMALV2 is no number of these rules, but a wide enough DECIMAL of either version holds all its values
        if (from.isDecimalOfAnyVersion() && to.isDecimalOfAnyVersion() && preservesNumbers(from, to)) {
            return Order.PRESERVED;
        }
        if (!isNumber(from)) {
            return Order.UNSUPPORTED;
        }
        if (to.isDateType()) {
            return Order.SEGMENTED;
        }
        if (!isNumber(to)) {
            return Order.UNSUPPORTED;
        }
        if (preservesNumbers(from, to)) {
            return Order.PRESERVED;
        }
        // An integer that fits the target keeps its value; one that does not becomes NULL.
        if (from.isFixedPointType() && (to.isFixedPointType() || to.isDecimalV3())) {
            return Order.STRICT;
        }
        return Order.NON_DECREASING;
    }

    /** Whether the cast keeps every value as it is, so the statistics of the source are those of the cast. */
    public static boolean preservesValues(Type from, Type to) {
        return order(from, to) == Order.PRESERVED;
    }

    /**
     * Whether a comparison of the cast of a column with a constant bounds the column for the estimate: the
     * statistics of the cast hold the range of the column mapped through the cast. Text read as a date counts, as
     * its statistics have a range only when the bounds of the column share one layout.
     */
    public static boolean boundsColumn(Type from, Type to) {
        Order order = order(from, to);
        return order == Order.PRESERVED || order == Order.STRICT || order == Order.NON_DECREASING
                || order == Order.LAYOUT;
    }

    /**
     * The most distinct values of a type between two statistic values of it: days for a DATE, steps of the scale
     * for a DECIMAL and integers for an integer; no limit for other types. A DATETIME holds microseconds, so its
     * seconds are no limit, see secondsInRange.
     */
    static double valuesInRange(Type type, double min, double max) {
        if (!(max >= min) || Double.isInfinite(max - min)) {
            return Double.POSITIVE_INFINITY;
        }
        if (type.isDate()) {
            return Math.floor((max - min) / (24.0 * 3600) + 0.5) + 1;
        }
        if (type.isFixedPointType()) {
            return Math.floor(max - min) + 1;
        }
        if (type.isDecimalV3()) {
            return Math.floor((max - min) * Math.pow(10, ((ScalarType) type).getScalarScale()) + 0.5) + 1;
        }
        return Double.POSITIVE_INFINITY;
    }

    /**
     * Whether the cast can make one value of distinct values of its source, as the times of one day cast to DATE or
     * a number that loses its fraction. Text read as a date merges values when it holds a time, see
     * StringDateLayout.hasTime; the type alone does not say it.
     */
    static boolean mergesValues(Type from, Type to) {
        Order order = order(from, to);
        if (order == Order.SEGMENTED) {
            return !from.isFixedPointType() && !(from.isDecimalV3() && ((ScalarType) from).getScalarScale() == 0);
        }
        if (order != Order.NON_DECREASING) {
            return false;
        }
        if (from.isDecimalV3() && to.isDecimalV3()) {
            // A narrower DECIMAL of the same scale keeps the values that fit and turns the others into NULL
            return ((ScalarType) to).getScalarScale() < ((ScalarType) from).getScalarScale();
        }
        return true;
    }

    /**
     * The value after a statistic value of a type whose values valuesInRange counts: the next day, integer or
     * step of the scale; NaN for other types.
     */
    static double nextValue(Type type, double value) {
        if (type.isDate()) {
            Optional<LocalDateTime> day = Utils.getDatetimeFromStatistic(Math.floor(value));
            return day.isEmpty() ? Double.NaN : Utils.getLongFromDateTime(day.get().toLocalDate()
                    .plusDays(1).atStartOfDay());
        }
        if (type.isFixedPointType()) {
            return value + 1;
        }
        if (type.isDecimalV3()) {
            return value + Math.pow(10, -((ScalarType) type).getScalarScale());
        }
        return Double.NaN;
    }

    // The whole seconds between two DATETIME statistic values: the most distinct values of a cast that drops the
    // microseconds.
    static double secondsInRange(double min, double max) {
        return !(max >= min) || Double.isInfinite(max - min) ? Double.POSITIVE_INFINITY : Math.floor(max - min) + 1;
    }

    // Numbers the statistics cover: integers, FLOAT, DOUBLE and DECIMAL; not BOOLEAN and not DECIMALV2.
    static boolean isNumber(Type type) {
        return type.isFixedPointType() || type.isFloatingPointType() || type.isDecimalV3();
    }

    // Widening integers and decimals, and the casts to FLOAT and DOUBLE that hold every value exactly.
    private static boolean preservesNumbers(Type from, Type to) {
        if (from.isDecimalOfAnyVersion() && to.isDecimalOfAnyVersion()) {
            ScalarType source = (ScalarType) from;
            ScalarType target = (ScalarType) to;
            return source.getScalarPrecision() > 0 && target.getScalarPrecision() > 0
                    && target.getScalarScale() >= source.getScalarScale()
                    && target.getScalarPrecision() - target.getScalarScale()
                        >= source.getScalarPrecision() - source.getScalarScale();
        }
        return (from.isFixedPointType() && to.isFixedPointType() && to.getTypeSize() >= from.getTypeSize())
                || (from.isFloat() && to.isDouble())
                || (from.isFixedPointType() && from.getTypeSize() <= 4 && to.isDouble())
                || (from.isFixedPointType() && from.getTypeSize() <= 2 && to.isFloat());
    }
}
