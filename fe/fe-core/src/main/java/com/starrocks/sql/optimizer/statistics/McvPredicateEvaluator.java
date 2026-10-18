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
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Predicate preparation scoped to one derivation, never retained in the statistics cache. A condition on a virtual
 * column of the statistics on an expression is written by the planner for the estimate only and does not change,
 * and the derivations of one query match the head of x with it again, so its preparation and its result on each
 * value are kept in the cache of the query.
 */
final class McvPredicateEvaluator {
    private static final int MAX_RESULTS = 4096;

    // ScalarOperator.hashCode/equals can themselves walk a long IN list. Identity is sufficient here.
    private final Map<ScalarOperator, Local> predicates = new IdentityHashMap<>();
    private final ExpressionStatisticsCache cache = ExpressionStatisticsCache.current();

    // A prepared condition with the work budget its LIKE spends in this derivation, over the values in their order.
    private record Local(Prepared prepared, LikePatternEstimator.Work work) {
    }

    ColumnRefOperator column(ScalarOperator predicate) {
        return local(predicate).prepared().column;
    }

    Optional<Boolean> matchesComponent(ScalarOperator predicate, ColumnRefOperator column, String value) {
        Local local = local(predicate);
        Prepared prepared = local.prepared();
        if (prepared.results != null && column.equals(prepared.column) && value != null) {
            Optional<Boolean> known = prepared.results.get(value);
            if (known == null) {
                known = match(prepared, predicate, column, value, local.work());
                if (prepared.results.size() < MAX_RESULTS && prepared.results.putIfAbsent(value, known) == null) {
                    cache.grow(1);
                }
            }
            return known;
        }
        return match(prepared, predicate, column, value, local.work());
    }

    private static Optional<Boolean> match(Prepared prepared, ScalarOperator predicate, ColumnRefOperator column,
                                           String value, LikePatternEstimator.Work work) {
        if (prepared.membership == null && prepared.like == null) {
            return MultiColumnMcvEstimator.matchesComponent(predicate, column, value, work);
        }
        ScalarOperator expression = predicate.getChild(0);
        if (!expression.isColumnRef()) {
            Optional<ConstantOperator> result = MultiColumnMcvEstimator.evaluate(expression, column, value);
            if (result.isEmpty()) {
                return Optional.empty();
            }
            value = result.get().isNull() ? null : MultiColumnMcvEstimator.constantText(result.get());
        }
        return prepared.like != null ? value == null ? Optional.of(false) : prepared.like.tryMatch(value, work)
                : prepared.membership.matches(value);
    }

    private Local local(ScalarOperator predicate) {
        Local local = predicates.get(predicate);
        if (local != null) {
            return local;
        }
        ColumnRefOperator column = MultiColumnMcvEstimator.predicateColumn(predicate);
        Prepared prepared;
        if (cache != null && column != null && ExpressionStatisticsEstimator.isVirtualColumn(column)) {
            long size = ExpressionStatisticsCache.units(predicate);
            if (predicate instanceof LikePredicateOperator && predicate.getChild(1) instanceof ConstantOperator pattern
                    && !pattern.isNull() && pattern.getType().isStringType()) {
                // A LIKE keeps its compiled pattern, which is about as long as the pattern text.
                size += pattern.getVarchar().length();
            }
            long units = size;
            prepared = cache.part(List.of("prepared", new ExpressionStatisticsCache.Identity(predicate)),
                    () -> new Prepared(predicate, column, true), created -> units);
        } else {
            prepared = new Prepared(predicate, column, false);
        }
        local = new Local(prepared, new LikePatternEstimator.Work());
        predicates.put(predicate, local);
        return local;
    }

    private static final class Prepared {
        private final ColumnRefOperator column;
        private final Membership membership;
        private final LikePatternEstimator.LikePattern like;
        // The result on each value, kept for a condition on a virtual column in the cache of a query only. The
        // result of a LIKE depends on the budget the derivation has left, so it is not kept.
        private final Map<String, Optional<Boolean>> results;

        private Prepared(ScalarOperator predicate, ColumnRefOperator column, boolean kept) {
            this.column = column;
            membership = column != null && predicate instanceof InPredicateOperator
                    ? new Membership((InPredicateOperator) predicate) : null;
            like = column != null && predicate instanceof LikePredicateOperator pattern
                    ? LikePatternEstimator.pattern(pattern).orElse(null) : null;
            results = kept && !(predicate instanceof LikePredicateOperator) ? new ConcurrentHashMap<>() : null;
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
            // Match byte equality of the UTF-8 forms even for strings containing malformed UTF-16 surrogate
            // sequences: the UTF-8 form is valid and decodes back to one string per byte sequence, and a string
            // without surrogates decodes back to itself, so it is its own key.
            for (int i = 0; i < value.length(); i++) {
                if (Character.isSurrogate(value.charAt(i))) {
                    return new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                }
            }
            return value;
        } catch (RuntimeException e) {
            return null;
        }
    }
}
