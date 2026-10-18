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

import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

/**
 * The value of a condition over integers on one row, as SQL reads it: true, false, or null when a NULL makes it
 * unknown. A row is kept only by a condition that is true. The conditions are comparisons of integer columns, of casts
 * of them to wider integers and of integer constants, and AND, OR and NOT of them.
 */
final class RowEvaluator {
    private RowEvaluator() {
    }

    static Boolean evaluate(ScalarOperator operator, Map<ColumnRefOperator, Long> row) {
        if (operator instanceof ConstantOperator constant && !constant.isNull()
                && constant.getType().isBoolean()) {
            return constant.getBoolean();
        }
        if (operator instanceof CompoundPredicateOperator compound) {
            Boolean first = evaluate(compound.getChild(0), row);
            if (compound.isNot()) {
                return first == null ? null : !first;
            }
            Boolean second = evaluate(compound.getChild(1), row);
            if (compound.isAnd()) {
                if (Boolean.FALSE.equals(first) || Boolean.FALSE.equals(second)) {
                    return false;
                }
                return first == null || second == null ? null : true;
            }
            if (Boolean.TRUE.equals(first) || Boolean.TRUE.equals(second)) {
                return true;
            }
            return first == null || second == null ? null : false;
        }
        if (operator instanceof BinaryPredicateOperator binary) {
            Long left = value(binary.getChild(0), row);
            Long right = value(binary.getChild(1), row);
            if (left == null || right == null) {
                return null;
            }
            int order = Long.compare(left, right);
            return switch (binary.getBinaryType()) {
                case EQ -> order == 0;
                case NE -> order != 0;
                case LT -> order < 0;
                case LE -> order <= 0;
                case GT -> order > 0;
                case GE -> order >= 0;
                default -> throw new IllegalArgumentException(binary.toString());
            };
        }
        throw new IllegalArgumentException(operator.toString());
    }

    static boolean keeps(ScalarOperator operator, Map<ColumnRefOperator, Long> row) {
        return Boolean.TRUE.equals(evaluate(operator, row));
    }

    private static Long value(ScalarOperator operator, Map<ColumnRefOperator, Long> row) {
        if (operator instanceof ColumnRefOperator column) {
            return row.get(column);
        }
        if (operator instanceof CastOperator cast) {
            return value(cast.getChild(0), row);
        }
        if (operator instanceof ConstantOperator constant) {
            if (constant.isNull()) {
                return null;
            }
            return switch (constant.getType().getPrimitiveType()) {
                case INT -> (long) constant.getInt();
                case BIGINT -> constant.getBigint();
                default -> throw new IllegalArgumentException(operator.toString());
            };
        }
        throw new IllegalArgumentException(operator.toString());
    }
}
