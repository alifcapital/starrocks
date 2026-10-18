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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.type.Type;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
 *
 * A condition on a column itself is matched with the values of a head converted once to the type of the column, which
 * the head keeps, see TypedValue and PreparedMcvTuples#typedValue, and with its constants converted once when it is
 * prepared. The conversions are the ones of MultiColumnMcvEstimator#compare and #evaluate, so the result is the same;
 * a condition whose constant cannot be read the same way takes the plain evaluation.
 */
final class McvPredicateEvaluator {
    private static final int MAX_RESULTS = 4096;

    // The operand of a condition on a value: not evaluated here, NULL, a value, or TRUE / FALSE for a BOOLEAN.
    static final byte OPERAND_EMPTY = 0;
    static final byte OPERAND_NULL = 1;
    static final byte OPERAND_VALUE = 2;
    static final byte OPERAND_TRUE = 3;
    static final byte OPERAND_FALSE = 4;

    // A key compare() cannot read: the comparison is not evaluated.
    private static final Object FAILED = new Object();
    // A comparison of keys that compare() would not evaluate.
    private static final int NO_ORDER = Integer.MIN_VALUE;

    // ScalarOperator.hashCode/equals can themselves walk a long IN list. Identity is sufficient here.
    private final Map<ScalarOperator, Local> predicates = new IdentityHashMap<>();
    private final ExpressionStatisticsCache cache = ExpressionStatisticsCache.current();

    // A prepared condition with the work budget its LIKE spends in this derivation, over the values in their order.
    private record Local(Prepared prepared, LikePatternEstimator.Work work) {
    }

    // How compare() reads a value of a type: as a boolean, an exact number, a double, or UTF-8 bytes.
    private enum Order { BOOLEAN, EXACT, NUMBER, BYTES }

    private static Order orderOf(Type type) {
        if (type.isBoolean()) {
            return Order.BOOLEAN;
        }
        if (type.isFixedPointType() || type.isDecimalV3() || type.isDecimalV2()) {
            return Order.EXACT;
        }
        if (type.isNumericType() || type.isDate() || type.isDatetime()) {
            return Order.NUMBER;
        }
        return Order.BYTES;
    }

    /**
     * A stored head value converted to a type: the operand that MultiColumnMcvEstimator#evaluate gives for the
     * column, the key compare() reads it as, and its key of equality. A conversion that fails is kept as the failure
     * compare() and evaluate() report, so the value never needs the plain evaluation.
     */
    static final class TypedValue {
        private final byte operand;
        private final Object key;
        private final Object equality;

        private TypedValue(byte operand, Object key, Object equality) {
            this.operand = operand;
            this.key = key;
            this.equality = equality;
        }

        // An upper bound of the heap the converted value with its map entry holds, for the weight of the cache: the
        // objects and their headers in the constant part, and per UTF-16 unit of the text up to three UTF-8 bytes of
        // the key of the order and two bytes of the copy of a text with a surrogate the key of equality holds. A
        // number key holds fewer bytes than the text it is read from.
        static long retainedBytes(String value) {
            return 256L + 5L * value.length();
        }

        static TypedValue of(Type type, String value) {
            return new TypedValue(operand(type, value), orderKey(orderOf(type), type, value),
                    equalityKey(type, type, value));
        }

        // As MultiColumnMcvEstimator#evaluate gives the column itself: the cast of the text to the type.
        private static byte operand(Type type, String value) {
            try {
                Optional<ConstantOperator> cast = StatisticsCastEvaluator.cast(ConstantOperator.createVarchar(value),
                        type);
                if (cast.isEmpty()) {
                    return OPERAND_EMPTY;
                }
                if (cast.get().isNull()) {
                    return OPERAND_NULL;
                }
                if (cast.get().getType().isBoolean()) {
                    return cast.get().getBoolean() ? OPERAND_TRUE : OPERAND_FALSE;
                }
                return OPERAND_VALUE;
            } catch (RuntimeException e) {
                return OPERAND_EMPTY;
            }
        }

        // The left side of MultiColumnMcvEstimator#compare.
        private static Object orderKey(Order order, Type type, String value) {
            switch (order) {
                case BOOLEAN:
                    return booleanValue(value);
                case EXACT:
                    try {
                        return new BigDecimal(value);
                    } catch (NumberFormatException e) {
                        return FAILED;
                    }
                case NUMBER:
                    try {
                        return StatisticUtils.convertStatisticsToDouble(type, value).<Object>map(number -> number)
                                .orElse(FAILED);
                    } catch (RuntimeException e) {
                        return FAILED;
                    }
                default:
                    return value.getBytes(StandardCharsets.UTF_8);
            }
        }
    }

    /**
     * The right side of MultiColumnMcvEstimator#compare for a constant: null when compare() would not read the
     * constant the same way, such as a text form that throws, so the condition takes the plain evaluation.
     */
    private static Object constantKey(Order order, ConstantOperator constant) {
        try {
            switch (order) {
                case BOOLEAN:
                    return booleanValue(MultiColumnMcvEstimator.constantText(constant));
                case EXACT: {
                    String text = MultiColumnMcvEstimator.constantText(constant);
                    try {
                        return new BigDecimal(text);
                    } catch (NumberFormatException e) {
                        return FAILED;
                    }
                }
                case NUMBER:
                    try {
                        return StatisticUtils.convertStatisticsToDouble(constant.getType(),
                                MultiColumnMcvEstimator.constantText(constant)).<Object>map(number -> number)
                                .orElse(FAILED);
                    } catch (RuntimeException e) {
                        return FAILED;
                    }
                default:
                    return MultiColumnMcvEstimator.constantText(constant).getBytes(StandardCharsets.UTF_8);
            }
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static Double booleanValue(String text) {
        return "1".equals(text) || "true".equalsIgnoreCase(text) ? 1.0 : 0.0;
    }

    // The sign of MultiColumnMcvEstimator#compare over the keys, NO_ORDER when it gives none.
    private static int compareKeys(Order order, Object left, Object right) {
        switch (order) {
            case BOOLEAN:
                return Double.compare((Double) left, (Double) right);
            case EXACT:
                if (left == FAILED || right == FAILED) {
                    return NO_ORDER;
                }
                return Integer.signum(((BigDecimal) left).compareTo((BigDecimal) right));
            case NUMBER:
                if (left == FAILED || right == FAILED) {
                    return NO_ORDER;
                }
                return Double.compare((Double) left, (Double) right);
            default:
                return Integer.signum(Arrays.compareUnsigned((byte[]) left, (byte[]) right));
        }
    }

    ColumnRefOperator column(ScalarOperator predicate) {
        return local(predicate).prepared().column;
    }

    /**
     * Whether a component value satisfies the condition, see MultiColumnMcvEstimator#matchesComponent.
     *
     * @param head     the head the value is read from, null when it is not a head value
     * @param position the position of the value in the tuples of the head
     */
    Optional<Boolean> matchesComponent(ScalarOperator predicate, ColumnRefOperator column, String value,
                                       PreparedMcvTuples head, int position) {
        Local local = local(predicate);
        Prepared prepared = local.prepared();
        if (prepared.results != null && column.equals(prepared.column) && value != null) {
            Optional<Boolean> known = prepared.results.get(value);
            if (known == null) {
                known = match(prepared, predicate, column, value, local.work(), head, position);
                if (prepared.results.size() < MAX_RESULTS && prepared.results.putIfAbsent(value, known) == null) {
                    cache.grow(1);
                }
            }
            return known;
        }
        return match(prepared, predicate, column, value, local.work(), head, position);
    }

    /**
     * The operand an expression of the column gives on a value, as MultiColumnMcvEstimator#evaluate: OPERAND_EMPTY
     * when it cannot be evaluated, OPERAND_NULL, OPERAND_TRUE / OPERAND_FALSE for a BOOLEAN, or OPERAND_VALUE.
     */
    static byte operand(ScalarOperator expression, ColumnRefOperator column, String value, PreparedMcvTuples head,
                        int position) {
        if (expression.isColumnRef()) {
            if (value == null) {
                return OPERAND_NULL;
            }
            if (head != null) {
                try {
                    return head.typedValue(position, column.getType(), value).operand;
                } catch (RuntimeException e) {
                    // We do not expect the converted values to fail, but the plain evaluation below does not need them.
                }
            }
        }
        Optional<ConstantOperator> result = MultiColumnMcvEstimator.evaluate(expression, column, value);
        if (result.isEmpty()) {
            return OPERAND_EMPTY;
        }
        if (result.get().isNull()) {
            return OPERAND_NULL;
        }
        if (result.get().getType().isBoolean()) {
            return result.get().getBoolean() ? OPERAND_TRUE : OPERAND_FALSE;
        }
        return OPERAND_VALUE;
    }

    private static Optional<Boolean> match(Prepared prepared, ScalarOperator predicate, ColumnRefOperator column,
                                           String value, LikePatternEstimator.Work work, PreparedMcvTuples head,
                                           int position) {
        if (head != null && value != null && predicate.getChild(0).isColumnRef()) {
            try {
                if (prepared.comparison != null && prepared.comparison.type.equals(column.getType())) {
                    TypedValue typed = head.typedValue(position, prepared.comparison.type, value);
                    return prepared.comparison.matches(typed.key);
                }
                if (prepared.membership != null) {
                    return prepared.membership.matchesKey(head.typedValue(position, prepared.membership.type, value)
                            .equality);
                }
            } catch (RuntimeException e) {
                // We do not expect the converted values to fail, but the plain evaluation below does not need them.
            }
        }
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
        // A comparison of the column itself with a constant, with the constant converted once; null otherwise.
        private final Comparison comparison;
        // The result on each value, kept for a condition on a virtual column in the cache of a query only. The
        // result of a LIKE depends on the budget the derivation has left, so it is not kept.
        private final Map<String, Optional<Boolean>> results;

        private Prepared(ScalarOperator predicate, ColumnRefOperator column, boolean kept) {
            this.column = column;
            membership = column != null && predicate instanceof InPredicateOperator
                    ? new Membership((InPredicateOperator) predicate) : null;
            like = column != null && predicate instanceof LikePredicateOperator pattern
                    ? LikePatternEstimator.pattern(pattern).orElse(null) : null;
            comparison = column != null && predicate instanceof BinaryPredicateOperator binary
                    && binary.getChild(0).isColumnRef() ? Comparison.of(binary, column.getType()) : null;
            results = kept && !(predicate instanceof LikePredicateOperator) ? new ConcurrentHashMap<>() : null;
        }
    }

    // column op constant with the constant read once as MultiColumnMcvEstimator#compare reads it.
    private record Comparison(Type type, Order order, Object constant, BinaryType op) {
        static Comparison of(BinaryPredicateOperator predicate, Type type) {
            if (!(predicate.getChild(1) instanceof ConstantOperator constant)) {
                return null;
            }
            Order order = orderOf(type);
            Object key = constantKey(order, constant);
            return key == null ? null : new Comparison(type.clone(), order, key, predicate.getBinaryType());
        }

        // As MultiColumnMcvEstimator#matches on a non-NULL value whose key is given.
        Optional<Boolean> matches(Object key) {
            int cmp = compareKeys(order, key, constant);
            if (cmp == NO_ORDER) {
                return Optional.empty();
            }
            switch (op) {
                case EQ:
                    return Optional.of(cmp == 0);
                case NE:
                    return Optional.of(cmp != 0);
                case LT:
                    return Optional.of(cmp < 0);
                case LE:
                    return Optional.of(cmp <= 0);
                case GT:
                    return Optional.of(cmp > 0);
                case GE:
                    return Optional.of(cmp >= 0);
                default:
                    return Optional.empty();
            }
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
            return matchesKey(empty ? null : equalityKey(type, type, value));
        }

        // On a non-NULL value with the given key of equality, null when the value cannot be read.
        private Optional<Boolean> matchesKey(Object key) {
            if (empty) {
                return Optional.of(negated);
            }
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
