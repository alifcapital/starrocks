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

import com.starrocks.common.util.StringDateFormat;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.function.Function;

/** Necessary scan bounds for VARCHAR date inputs with an explicitly declared encoding. */
public final class StringDatePredicateDerivation {
    private StringDatePredicateDerivation() {
    }

    /**
     * @param formats the declared format of a column, or null when the column has none
     */
    public static ScalarOperator derive(BinaryPredicateOperator predicate,
                                        Function<ColumnRefOperator, StringDateFormat> formats) {
        if (!(predicate.getChild(0) instanceof CastOperator cast)
                || !(cast.getChild(0) instanceof ColumnRefOperator column) || !column.getType().isVarchar()
                || !cast.getType().isDateType()
                || !(predicate.getChild(1) instanceof ConstantOperator constant) || constant.isNull()
                || !constant.getType().isDateType()) {
            return predicate;
        }
        StringDateFormat format = formats.apply(column);
        if (format == null || !format.isSupportedInCurrentTimezone()) {
            return predicate;
        }

        // DATE drops the time even when the string contains seconds or microseconds.
        // A string of months or years holds less than a day, and its bound is cut to that unit.
        ChronoUnit precision = format.getPrecision();
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
                result = bound(column, format, BinaryType.GE, ceil);
                break;
            case GT:
                result = bound(column, format, BinaryType.GE, next);
                break;
            case LT:
                result = bound(column, format, BinaryType.LT, ceil);
                break;
            case LE:
                result = bound(column, format, BinaryType.LT, next);
                break;
            case EQ:
            case EQ_FOR_NULL:
                if (!floor.equals(value)) {
                    return predicate;
                }
                result = Utils.compoundAnd(bound(column, format, BinaryType.GE, floor),
                        bound(column, format, BinaryType.LT, next));
                break;
            default:
                return predicate;
        }
        return result == null ? predicate : result;
    }

    private static ScalarOperator bound(ColumnRefOperator column, StringDateFormat format,
                                        BinaryType comparison, LocalDateTime value) {
        // Year 10000 requires a fifth digit, which breaks the declared lexical order.
        // At the type boundary, keep only the representable side and the original predicate.
        if (value.getYear() < 0 || value.getYear() > 9999) {
            return null;
        }
        return new BinaryPredicateOperator(comparison, column, ConstantOperator.createVarchar(format.format(value)));
    }
}
