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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.util.DateFormatModel;
import com.starrocks.common.util.StringDateFormat;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.StatisticsDatetimeParser;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.function.Function;

/**
 * Bounds on a string that holds dates in a known layout, from a comparison of its cast to a date. The layout comes from
 * a column of VARCHAR with an explicitly declared encoding, or from date_format(y, f) with a format that is ordered,
 * that the BE renders on every row and reads as the start of its period, see DateFormatModel: one derivation serves both.
 */
public final class StringDatePredicateDerivation {
    private StringDatePredicateDerivation() {
    }

    // The reading of the BE for each format of date_format, asked once: it casts a few probe strings. We expect a few
    // formats in the queries of a cluster.
    private static final Cache<String, Boolean> READ_AS_PERIOD_START = Caffeine.newBuilder().maximumSize(1000).build();

    /**
     * The model of the layout of the strings an expression gives, or null when it has none known. A column of VARCHAR
     * has the layout its table declares, and date_format(y, f) has the layout of f when its strings are ordered, exact on
     * every row and read by the BE as the start of the period of the string.
     */
    public static DateFormatModel layoutOf(ScalarOperator source,
                                           Function<ColumnRefOperator, StringDateFormat> formats) {
        if (source instanceof ColumnRefOperator column) {
            if (!column.getType().isVarchar()) {
                return null;
            }
            StringDateFormat format = formats.apply(column);
            return format == null ? null : format.getModel();
        }
        return renderedLayout(source);
    }

    /**
     * The layout of date_format(y, f) when its strings are ordered, exact on every row and read by the BE as the start
     * of their period; null for any other expression. The check does not allocate once the format is known.
     */
    public static DateFormatModel renderedLayout(ScalarOperator source) {
        if (!(source instanceof CallOperator call) || source instanceof CastOperator
                || call.getChildren().size() != 2 || !FunctionSet.DATE_FORMAT.equalsIgnoreCase(call.getFnName())
                || !(call.getChild(1) instanceof ConstantOperator format) || format.isNull()
                || !format.getType().isStringType()) {
            return null;
        }
        DateFormatModel model = DateFormatModel.of(format.getVarchar());
        if (!model.isOrdered() || !model.isExactOnAllRows()) {
            return null;
        }
        boolean readAsPeriodStart = READ_AS_PERIOD_START.get(model.getFormat(),
                key -> model.isReadAsPeriodStartBy(StatisticsDatetimeParser::parse));
        return readAsPeriodStart ? model : null;
    }

    /**
     * @param formats the declared format of a column, or null when the column has none
     */
    public static ScalarOperator derive(BinaryPredicateOperator predicate,
                                        Function<ColumnRefOperator, StringDateFormat> formats) {
        if (!(predicate.getChild(0) instanceof CastOperator cast) || !cast.getType().isDateType()
                || !(predicate.getChild(1) instanceof ConstantOperator constant) || constant.isNull()
                || !constant.getType().isDateType()) {
            return predicate;
        }
        ScalarOperator source = cast.getChild(0);
        DateFormatModel format = layoutOf(source, formats);
        if (format == null || !format.isSupportedInCurrentTimezone()) {
            return predicate;
        }

        // DATE drops the time even when the string contains seconds or microseconds.
        // A string of months or years holds less than a day, and its bound is cut to that unit.
        ChronoUnit precision = format.precision();
        if (cast.getType().isDate() && precision.compareTo(ChronoUnit.DAYS) < 0) {
            precision = ChronoUnit.DAYS;
        }
        LocalDateTime value = constant.getDatetime();
        LocalDateTime floor = StringDateFormat.truncate(value, precision);
        LocalDateTime next = floor.plus(1, precision);
        LocalDateTime ceil = floor.equals(value) ? floor : next;
        ScalarOperator result;
        switch (predicate.getBinaryType()) {
            case GE:
                result = bound(source, format, BinaryType.GE, ceil);
                break;
            case GT:
                result = bound(source, format, BinaryType.GE, next);
                break;
            case LT:
                result = bound(source, format, BinaryType.LT, ceil);
                break;
            case LE:
                result = bound(source, format, BinaryType.LT, next);
                break;
            case EQ:
            case EQ_FOR_NULL:
                if (!floor.equals(value)) {
                    return predicate;
                }
                result = Utils.compoundAnd(bound(source, format, BinaryType.GE, floor),
                        bound(source, format, BinaryType.LT, next));
                break;
            case NE:
                // We expect <> to be as exact as =, so that the two estimates add up to the rows. That holds for
                // date_format, whose strings all parse. A column with a declared format can hold strings that the cast
                // turns into NULL, and we leave its <> as it is.
                ScalarOperator below = bound(source, format, BinaryType.LT, floor);
                ScalarOperator above = bound(source, format, BinaryType.GE, next);
                if (source instanceof ColumnRefOperator || !floor.equals(value) || below == null || above == null) {
                    return predicate;
                }
                result = Utils.compoundOr(below, above);
                break;
            default:
                return predicate;
        }
        return result == null ? predicate : result;
    }

    private static ScalarOperator bound(ScalarOperator source, DateFormatModel format, BinaryType comparison,
                                        LocalDateTime value) {
        // Year 10000 requires a fifth digit, which breaks the declared lexical order.
        // At the type boundary, keep only the representable side and the original predicate.
        if (value.getYear() < 0 || value.getYear() > 9999) {
            return null;
        }
        return new BinaryPredicateOperator(comparison, source, ConstantOperator.createVarchar(format.render(value)));
    }
}
