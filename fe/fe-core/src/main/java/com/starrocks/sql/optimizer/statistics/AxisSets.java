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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * The atoms over a virtual column x that stand for values of one axis: x = v and x IN (...) are values of x, and
 * x LIKE 'p%' and substr(x, 1, n) IN (...) with prefixes of n characters are prefixes of n characters of x. The estimate
 * of one atom filters the head of x once, so the estimate of hundreds of atoms of one axis costs hundreds of passes,
 * while the estimate of the set of them is one pass: an IN over x, the prefix IN of ExpressionStatisticsEstimator. We
 * fear the passes more than we want the atoms apart, so the atoms of one axis in an OR are one IN, and the negations
 * of them in an AND are one NOT IN. The set keeps the rows of the atoms: the OR of x = a and x = b is x IN (a, b), and
 * the AND of x <> a and x <> b is x NOT IN (a, b); the prefix of n characters of x starts with p exactly when x does if p
 * has n characters.
 */
final class AxisSets {
    private AxisSets() {
    }

    /**
     * The axis of a set: the values of x (length 0) or the prefixes of the given length (a prefix of n characters has
     * length n).
     */
    record Axis(boolean prefix, int length) {
    }

    /** The values an atom has on its axis. */
    record Member(Axis axis, List<ConstantOperator> values) {
    }

    /**
     * The values an atom stands for on an axis of x, or empty when it is not such an atom. With negated true the atom
     * is a negation (x <> v, x NOT IN (...), NOT of a prefix atom), whose values are those it excludes.
     */
    static Optional<Member> memberOf(ScalarOperator atom, ColumnRefOperator x, boolean negated) {
        if (negated) {
            if (atom instanceof BinaryPredicateOperator binary && binary.getBinaryType() == BinaryType.NE) {
                return equalityValue(binary, x);
            }
            if (atom instanceof InPredicateOperator in && in.isNotIn()) {
                return inValues(in, x);
            }
            if (atom instanceof CompoundPredicateOperator not && not.isNot()) {
                Optional<Member> inner = memberOf(not.getChild(0), x, false);
                return inner.isPresent() && inner.get().axis().prefix() ? inner : Optional.empty();
            }
            return Optional.empty();
        }
        if (atom instanceof BinaryPredicateOperator binary && binary.getBinaryType() == BinaryType.EQ) {
            return equalityValue(binary, x);
        }
        if (atom instanceof InPredicateOperator in && !in.isNotIn()) {
            Optional<Member> values = inValues(in, x);
            return values.isPresent() ? values : prefixValues(in, x);
        }
        if (atom instanceof LikePredicateOperator like && !like.isRegexp() && x.equals(like.getChild(0))) {
            Optional<String> prefix = LikePatternEstimator.pattern(like)
                    .flatMap(LikePatternEstimator.LikePattern::fixedPrefix);
            if (prefix.isPresent() && !prefix.get().isEmpty()) {
                int length = prefix.get().codePointCount(0, prefix.get().length());
                return Optional.of(new Member(new Axis(true, length), List.of(ConstantOperator.createVarchar(prefix.get()))));
            }
        }
        return Optional.empty();
    }

    private static Optional<Member> equalityValue(BinaryPredicateOperator binary, ColumnRefOperator x) {
        if (x.equals(binary.getChild(0)) && binary.getChild(1) instanceof ConstantOperator value && !value.isNull()) {
            return Optional.of(new Member(new Axis(false, 0), List.of(value)));
        }
        return Optional.empty();
    }

    private static Optional<Member> inValues(InPredicateOperator in, ColumnRefOperator x) {
        if (in.isSubquery() || !x.equals(in.getChild(0))) {
            return Optional.empty();
        }
        List<ConstantOperator> values = constants(in);
        return values == null ? Optional.empty() : Optional.of(new Member(new Axis(false, 0), values));
    }

    // substr(x, 1, n) IN (prefixes of n characters)
    private static Optional<Member> prefixValues(InPredicateOperator in, ColumnRefOperator x) {
        if (in.isSubquery() || !(in.getChild(0) instanceof CallOperator call) || call.getClass() != CallOperator.class
                || !FunctionSet.SUBSTR.equals(call.getFnName().toLowerCase(Locale.ROOT)) || call.getChildren().size() != 3
                || !x.equals(call.getChild(0)) || !(call.getChild(1) instanceof ConstantOperator start)
                || !(call.getChild(2) instanceof ConstantOperator count) || start.isNull() || count.isNull()
                || !start.getType().isInt() || !count.getType().isInt() || start.getInt() != 1 || count.getInt() < 1) {
            return Optional.empty();
        }
        List<ConstantOperator> values = constants(in);
        if (values == null) {
            return Optional.empty();
        }
        for (ConstantOperator value : values) {
            if (!value.getType().isStringType() || value.getVarchar().codePointCount(0, value.getVarchar().length())
                    != count.getInt()) {
                return Optional.empty();
            }
        }
        return Optional.of(new Member(new Axis(true, count.getInt()), values));
    }

    // The values of an IN list when all of them are constants that are not NULL; null otherwise.
    private static List<ConstantOperator> constants(InPredicateOperator in) {
        List<ConstantOperator> values = new ArrayList<>(in.getChildren().size() - 1);
        for (int i = 1; i < in.getChildren().size(); i++) {
            if (!(in.getChild(i) instanceof ConstantOperator value) || value.isNull()) {
                return null;
            }
            values.add(value);
        }
        return values;
    }

    /**
     * The atom that stands for the values of several atoms of one axis: x IN (...) for the values of x, and
     * substr(x, 1, n) IN (...) for the prefixes of n characters, or the negation of it, a NOT IN or NOT of the prefix IN,
     * for the atoms that are negations. The values are those of the atoms without repeats, in the order of their first
     * appearance. Null when the constants have more than one type.
     */
    static ScalarOperator setOf(Axis axis, List<Member> members, ColumnRefOperator x, boolean negated) {
        Set<ConstantOperator> values = new LinkedHashSet<>();
        for (Member member : members) {
            values.addAll(member.values());
        }
        List<ScalarOperator> children = new ArrayList<>(values.size() + 1);
        children.add(axis.prefix() ? PrefixDerivationRule.substr(x, 1, axis.length()) : x);
        children.addAll(values);
        boolean sameType = values.stream().allMatch(value -> value.getType().equals(children.get(1).getType()));
        if (!sameType) {
            return null;
        }
        if (!axis.prefix()) {
            return new InPredicateOperator(negated, children.toArray(new ScalarOperator[0]));
        }
        ScalarOperator in = new InPredicateOperator(false, children.toArray(new ScalarOperator[0]));
        return negated ? new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, in) : in;
    }
}
