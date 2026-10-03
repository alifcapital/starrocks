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

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.type.Type;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Predicate preparation scoped to one derivation, never retained in the statistics cache. */
final class McvPredicateEvaluator {
    // ScalarOperator.hashCode/equals can themselves walk a long IN list. Identity is sufficient here.
    private final Map<ScalarOperator, Prepared> predicates = new IdentityHashMap<>();

    ColumnRefOperator column(ScalarOperator predicate) {
        return prepared(predicate).column;
    }

    Optional<Boolean> matchesComponent(ScalarOperator predicate, ColumnRefOperator column, String value) {
        Prepared prepared = prepared(predicate);
        if (prepared.membership == null && prepared.like == null) {
            return MultiColumnMcvEstimator.matchesComponent(predicate, column, value);
        }
        ScalarOperator expression = predicate.getChild(0);
        if (!expression.isColumnRef()) {
            Optional<ConstantOperator> result = MultiColumnMcvEstimator.evaluate(expression, column, value);
            if (result.isEmpty()) {
                return Optional.empty();
            }
            value = result.get().isNull() ? null : MultiColumnMcvEstimator.constantText(result.get());
        }
        return prepared.like != null ? value == null ? Optional.of(false) : prepared.like.tryMatch(value)
                : prepared.membership.matches(value);
    }

    private Prepared prepared(ScalarOperator predicate) {
        return predicates.computeIfAbsent(predicate, Prepared::new);
    }

    private static final class Prepared {
        private final ColumnRefOperator column;
        private final Membership membership;
        private final LikePatternEstimator.LikePattern like;

        private Prepared(ScalarOperator predicate) {
            column = MultiColumnMcvEstimator.predicateColumn(predicate);
            membership = column != null && predicate instanceof InPredicateOperator
                    ? new Membership((InPredicateOperator) predicate) : null;
            like = column != null && predicate instanceof LikePredicateOperator pattern
                    ? LikePatternEstimator.pattern(pattern).orElse(null) : null;
        }
    }

    private static final class Membership {
        private final Type type;
        private final boolean negated;
        private final boolean empty;
        private final Set<Object> keys = new HashSet<>();
        private boolean complete = true;

        private Membership(InPredicateOperator predicate) {
            type = predicate.getChild(0).getType();
            negated = predicate.isNotIn();
            empty = predicate.getChildren().size() == 1;
            for (int i = 1; i < predicate.getChildren().size(); i++) {
                ConstantOperator constant = (ConstantOperator) predicate.getChild(i);
                Object key = equalityKey(type, constant.getType(), MultiColumnMcvEstimator.constantText(constant));
                if (key == null) {
                    // Keep the old short-circuit behavior: a hit before an unreadable constant is valid,
                    // but a miss cannot be decided, even if a later constant would have matched.
                    complete = false;
                    break;
                }
                keys.add(key);
            }
        }

        private Optional<Boolean> matches(String value) {
            if (value == null) {
                return Optional.of(false);
            }
            if (empty) {
                return Optional.of(negated);
            }
            Object key = equalityKey(type, type, value);
            if (key == null) {
                return Optional.empty();
            }
            boolean found = keys.contains(key);
            return found || complete ? Optional.of(negated != found) : Optional.empty();
        }
    }

    /** Keys implement exactly the equality relation of MultiColumnMcvEstimator.compare. */
    static Object equalityKey(Type comparisonType, Type valueType, String value) {
        if (value == null) {
            return null;
        }
        try {
            if (comparisonType.isBoolean()) {
                return "1".equals(value) || "true".equalsIgnoreCase(value);
            }
            if (comparisonType.isFixedPointType() || comparisonType.isDecimalV3() || comparisonType.isDecimalV2()) {
                return new BigDecimal(value).stripTrailingZeros();
            }
            if (comparisonType.isNumericType() || comparisonType.isDate() || comparisonType.isDatetime()) {
                // Double.equals has the same equality semantics as Double.compare, including NaN and signed zero.
                return StatisticUtils.convertStatisticsToDouble(valueType, value).orElse(null);
            }
            // Match byte equality even for strings containing malformed UTF-16 surrogate sequences.
            return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            return null;
        }
    }
}
