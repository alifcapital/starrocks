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
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * The parts of an OR, taken in turn, that have no row in common with the parts before them, told by their form alone.
 * The estimate of an OR takes the rows of a part that the earlier parts already hold from the estimate of the part over
 * the statistics they left, which for the equalities of one expression and the intervals of it is the rows of a share
 * of the expression again, and we fear that this share is taken for rows that are not there: an IN of days is the OR of
 * the equalities of days, and the rows of a day are not among those of another. So the rows of a part that is apart from
 * every earlier part are added to theirs whole.
 * <p>
 * A part is a set of values of one expression e: e = v, or the comparisons of e with constants that are ANDed, which
 * are an interval with its ends open or closed, or e LIKE 'p%' of a string e, the interval of the strings from p up to
 * the first string after every string that starts with p, in the order of the statistics. Two parts are apart when
 * their values have none in common: another v, a v outside the interval, intervals with no common point; two LIKE
 * parts are apart when neither prefix starts with the other. Anything else is not told apart: a part of another form,
 * another expression, constants that cannot be compared exactly, an interval with no value. A part that is not of the
 * form leaves every later part not told apart as well, as it can have rows in common with them.
 */
final class DisjointOrParts {
    // The values e keeps from lower to upper; a null end is open without bound.
    private record Range(ScalarOperator expression, ConstantOperator lower, boolean lowerOpen, ConstantOperator upper,
                         boolean upperOpen) {
    }

    // The earlier parts, every one of the form and of the expression of the first one; null before the first part.
    private ScalarOperator expression;
    private final List<Range> earlier = new ArrayList<>();
    private boolean known = true;

    /**
     * Takes the next part of the OR, and tells whether it has no row in common with any part before it.
     */
    boolean add(ScalarOperator part) {
        if (!known) {
            return false;
        }
        Range range = rangeOf(part);
        if (range == null || expression != null && !expression.equals(range.expression())) {
            known = false;
            return false;
        }
        expression = range.expression();
        boolean apart = true;
        for (Range other : earlier) {
            apart &= isApart(other, range);
        }
        earlier.add(range);
        return apart;
    }

    private static Range rangeOf(ScalarOperator part) {
        if (part instanceof LikePredicateOperator like && !like.isRegexp() && like.getChild(0).getType().isStringType()) {
            // The strings that start with p are those from p up to the first string after all of them.
            Optional<String> prefix = LikePatternEstimator.pattern(like)
                    .flatMap(LikePatternEstimator.LikePattern::fixedPrefix);
            if (prefix.isEmpty()) {
                return null;
            }
            String end = ExpressionStatisticsEstimator.successor(prefix.get());
            return new Range(PredicateStatisticsCalculator.intervalKey(like.getChild(0)),
                    ConstantOperator.createVarchar(prefix.get()), false,
                    end == null ? null : ConstantOperator.createVarchar(end), true);
        }
        if (part instanceof BinaryPredicateOperator equality && equality.getBinaryType() == BinaryType.EQ) {
            if (!(equality.getChild(1) instanceof ConstantOperator value) || value.isNull()) {
                return null;
            }
            return new Range(PredicateStatisticsCalculator.intervalKey(equality.getChild(0)), value, false, value, false);
        }
        ScalarOperator expression = null;
        ConstantOperator lower = null;
        boolean lowerOpen = false;
        ConstantOperator upper = null;
        boolean upperOpen = false;
        List<ScalarOperator> conjuncts = part instanceof CompoundPredicateOperator compound && compound.isAnd()
                ? Utils.extractConjuncts(part) : List.of(part);
        for (ScalarOperator conjunct : conjuncts) {
            if (!(conjunct instanceof BinaryPredicateOperator comparison) || !comparison.getBinaryType().isRange()
                    || !(comparison.getChild(1) instanceof ConstantOperator value) || value.isNull()) {
                return null;
            }
            ScalarOperator key = PredicateStatisticsCalculator.intervalKey(comparison.getChild(0));
            if (expression != null && !expression.equals(key)) {
                return null;
            }
            expression = key;
            BinaryType type = comparison.getBinaryType();
            boolean isLower = type == BinaryType.GE || type == BinaryType.GT;
            boolean open = type == BinaryType.GT || type == BinaryType.LT;
            ConstantOperator current = isLower ? lower : upper;
            if (current == null) {
                if (isLower) {
                    lower = value;
                    lowerOpen = open;
                } else {
                    upper = value;
                    upperOpen = open;
                }
                continue;
            }
            OptionalInt order = compare(value, current);
            if (order.isEmpty()) {
                return null;
            }
            // The tighter end: the higher lower end, the lower upper end, the open one when they are equal.
            int tighter = isLower ? order.getAsInt() : -order.getAsInt();
            boolean currentOpen = isLower ? lowerOpen : upperOpen;
            if (tighter > 0 || tighter == 0 && open && !currentOpen) {
                if (isLower) {
                    lower = value;
                    lowerOpen = open;
                } else {
                    upper = value;
                    upperOpen = open;
                }
            }
        }
        if (lower != null && upper != null) {
            OptionalInt order = compare(lower, upper);
            // An interval with no value is a part of no rows, which is not told here.
            if (order.isEmpty() || order.getAsInt() > 0 || order.getAsInt() == 0 && (lowerOpen || upperOpen)) {
                return null;
            }
        }
        return new Range(expression, lower, lowerOpen, upper, upperOpen);
    }

    private static boolean isApart(Range first, Range second) {
        return isBelow(first, second) || isBelow(second, first);
    }

    // Whether every value of the first is below every value of the second.
    private static boolean isBelow(Range first, Range second) {
        if (first.upper() == null || second.lower() == null) {
            return false;
        }
        OptionalInt order = compare(first.upper(), second.lower());
        if (order.isEmpty()) {
            return false;
        }
        return order.getAsInt() < 0 || order.getAsInt() == 0 && (first.upperOpen() || second.lowerOpen());
    }

    // The order of two constants when it is exact: strings by the order of the statistics, and the constants of one
    // type by their values. The constants of two types, which the cast of one may round, are not compared.
    private static OptionalInt compare(ConstantOperator first, ConstantOperator second) {
        try {
            if (first.getType().isStringType() && second.getType().isStringType()) {
                return OptionalInt.of(Integer.signum(StringBucket.compare(first.getVarchar(), second.getVarchar())));
            }
            if (first.getType().getPrimitiveType() == second.getType().getPrimitiveType()) {
                return OptionalInt.of(Integer.signum(first.compareTo(second)));
            }
        } catch (RuntimeException e) {
            return OptionalInt.empty();
        }
        return OptionalInt.empty();
    }
}
