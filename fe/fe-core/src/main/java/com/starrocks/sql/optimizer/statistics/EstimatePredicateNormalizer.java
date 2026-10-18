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
import com.starrocks.common.util.StringDateFormat;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.ConstantOperatorUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.MonotonicFilterDerivation;
import com.starrocks.type.BooleanType;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

/**
 * A condition written in the forms the estimators of conditions know. One condition can be written in several ways
 * that keep the same rows: a REGEXP of anchored prefixes is an OR of LIKE conditions, left(c, n) = 'p' on a column of
 * dates is c LIKE 'p%', and an IN over an expression is an OR of equalities. We fear that each estimator would repeat
 * these rewrites in its own path and read them differently, so the predicate is rewritten once, before any estimate
 * reads it, and the estimators see only the forms below. The rewritten predicate is a copy used by the estimate
 * alone; the predicate of the query and the plan do not change.
 * <ul>
 * <li>A REGEXP of anchored prefixes, see #anchoredPrefixes, is the OR of c LIKE 'p%' for its prefixes.</li>
 * <li>left(c, n), strleft(c, n), substr(c, 1, n) or substring(c, 1, n) = 'p' with p of n characters, on a column of
 * strings whose table declares the format of its dates with a day, is c LIKE 'p%'.</li>
 * <li>f(c) IN (v1, ..., vk) with k up to IN_AS_OR_LIMIT is the OR of f(c) = vi, for a function f whose comparison
 * with a constant reads bounds on its column, for a prefix of a column of dates and for the column of dates itself.
 * NOT IN stays: its estimate is the complement of the IN and belongs to the estimate of the IN.</li>
 * <li>The ranges of one column of whole numbers or days in a conjunction that keep one value are the equality of the
 * column with it, when the column has statistics.</li>
 * </ul>
 * We fear that a rewrite which serves one source of the estimate hides the condition from the others, so only the
 * rewrites that every source reads without losing information are made here. c LIKE 'p%' on a column of dates is not
 * among them: the statistics of the column read it as the interval c >= 'p' AND c < successor(p), while the statistics
 * on expressions read the prefix, and a prefix longer than they hold would be read as a range of the prefix they hold.
 * The estimate of the LIKE chooses the interval for the statistics of the column.
 * The rules apply until no rule applies, so the result of one is read by the next. An atom that the estimate skips is
 * left as it is, and a rewritten atom takes the flags of the atom it replaces. The same object comes back when
 * nothing is rewritten, so a predicate in the normal form is returned as it is, and the check for a condition to
 * rewrite does not allocate: the estimate of a head value of an MCV list asks for the plain estimate of a condition
 * many times.
 */
final class EstimatePredicateNormalizer {
    private static final Logger LOG = LogManager.getLogger(EstimatePredicateNormalizer.class);

    // The characters that mean more than themselves in a regular expression, and that a backslash makes literal.
    private static final String REGEXP_META = ".*+?()[]{}|\\$^";

    private EstimatePredicateNormalizer() {
    }

    /**
     * The predicate in the forms above, or the same object when nothing is rewritten. We fear that a rewrite fails on
     * a condition we did not expect, and an exception would fail the planning of the query, so any failure returns
     * the predicate as it is.
     */
    static ScalarOperator normalize(ScalarOperator predicate, Statistics statistics) {
        if (predicate == null) {
            return null;
        }
        try {
            return hasCandidate(predicate) ? new Normalization(statistics).rewrite(predicate) : predicate;
        } catch (RuntimeException e) {
            LOG.debug("Failed to normalize a predicate for the estimate", e);
            return predicate;
        }
    }

    // The values of the IN list that are not NULL, without duplicates, and the left side, for an IN that is the OR of
    // the equalities of its values.
    record InEqualities(ScalarOperator left, List<ScalarOperator> values) {
    }

    /**
     * The left side and the values of an IN or NOT IN over an expression whose equality with a constant the estimate
     * reads from the statistics of its column, see the rules above; empty for any other one. A NULL in the list is
     * not a value: it matches no row. NOT IN with a NULL in the list is never true, which the estimate of the IN reads
     * before it asks for the equalities.
     */
    static Optional<InEqualities> inEqualities(InPredicateOperator predicate) {
        List<ScalarOperator> children = predicate.getChildren();
        if (predicate.isSubquery() || children.size() < 2
                || children.size() - 1 > StatisticsEstimateCoefficient.IN_AS_OR_LIMIT) {
            return Optional.empty();
        }
        ScalarOperator left = predicate.getChild(0);
        List<ScalarOperator> values = new ArrayList<>(children.size() - 1);
        for (int i = 1; i < children.size(); i++) {
            if (!(children.get(i) instanceof ConstantOperator constant)) {
                return Optional.empty();
            }
            if (!constant.isNull() && !values.contains(constant)) {
                values.add(constant);
            }
        }
        if (values.isEmpty()) {
            return Optional.empty();
        }
        boolean reads;
        PrefixCall prefix = prefixCall(left);
        if (prefix != null) {
            reads = isDateColumn(prefix.column());
        } else if (left instanceof CallOperator && !(left instanceof CastOperator)) {
            reads = MonotonicFilterDerivation.columnBoundsForEstimate(
                    new BinaryPredicateOperator(BinaryType.EQ, left, values.get(0))) != null;
        } else if (left instanceof ColumnRefOperator column && column.getType().isStringType()) {
            StringDateFormat format = PredicateStatisticsCalculator.stringDateFormat(column);
            reads = format != null && format.hasDay();
        } else {
            reads = false;
        }
        return reads ? Optional.of(new InEqualities(left, values)) : Optional.empty();
    }

    /** left = v for each value. */
    static List<ScalarOperator> equalities(ScalarOperator left, List<ScalarOperator> values) {
        List<ScalarOperator> equalities = new ArrayList<>(values.size());
        for (ScalarOperator value : values) {
            equalities.add(new BinaryPredicateOperator(BinaryType.EQ, left, value));
        }
        return equalities;
    }

    // The checks below find a condition to rewrite without allocating. They may answer yes for a condition that the
    // rewrite then leaves as it is, never no for one it rewrites.

    private static boolean hasCandidate(ScalarOperator operator) {
        if (operator.isNotEvalEstimate()) {
            return false;
        }
        if (operator instanceof CompoundPredicateOperator compound) {
            if (compound.isAnd()) {
                return andHasCandidate(compound, new int[1]);
            }
            return childrenHaveCandidate(operator);
        }
        if (isBooleanIf(operator)) {
            return childrenHaveCandidate(operator);
        }
        return atomHasCandidate(operator);
    }

    private static boolean childrenHaveCandidate(ScalarOperator operator) {
        for (ScalarOperator child : operator.getChildren()) {
            if (hasCandidate(child)) {
                return true;
            }
        }
        return false;
    }

    // The conjuncts of a chain of ANDs: a candidate in one of them, or two sides of a point among them.
    private static boolean andHasCandidate(ScalarOperator operator, int[] sides) {
        if (operator.isNotEvalEstimate()) {
            return false;
        }
        if (operator instanceof CompoundPredicateOperator compound && compound.isAnd()) {
            return andHasCandidate(compound.getChild(0), sides) || andHasCandidate(compound.getChild(1), sides);
        }
        if (isPointSide(operator)) {
            return ++sides[0] >= 2;
        }
        return hasCandidate(operator);
    }

    private static boolean atomHasCandidate(ScalarOperator atom) {
        if (atom instanceof LikePredicateOperator like) {
            return like.isRegexp() && like.getChild(1) instanceof ConstantOperator pattern && !pattern.isNull()
                    && pattern.getType().isStringType() && pattern.getVarchar().startsWith("^");
        }
        if (atom instanceof BinaryPredicateOperator binary) {
            return binary.getBinaryType() == BinaryType.EQ && prefixCall(binary.getChild(0)) != null
                    && binary.getChild(1) instanceof ConstantOperator;
        }
        if (atom instanceof InPredicateOperator in) {
            ScalarOperator left = in.getChild(0);
            return !in.isNotIn() && !in.isSubquery() && in.getChildren().size() >= 2
                    && in.getChildren().size() - 1 <= StatisticsEstimateCoefficient.IN_AS_OR_LIMIT
                    && ((left instanceof CallOperator && !(left instanceof CastOperator))
                    || (left instanceof ColumnRefOperator && left.getType().isStringType()));
        }
        return false;
    }

    private static boolean isBooleanIf(ScalarOperator operator) {
        return operator instanceof CallOperator call && call.getType() == BooleanType.BOOLEAN
                && call.getFnName().equalsIgnoreCase(FunctionSet.IF);
    }

    // A range comparison of a column of whole numbers or days, or of a function of a column, with a constant: it can be
    // a side of a point.
    private static boolean isPointSide(ScalarOperator operator) {
        return operator instanceof BinaryPredicateOperator binary && binary.getBinaryType().isRange()
                && binary.getChild(1).isConstantRef()
                && ((binary.getChild(0).isColumnRef() && isDiscreteType(binary.getChild(0).getType()))
                || (binary.getChild(0) instanceof CallOperator && !(binary.getChild(0) instanceof CastOperator)));
    }

    // A type of whole numbers or days: a range on a column of it can keep one value.
    private static boolean isDiscreteType(Type type) {
        return type.isTinyint() || type.isSmallint() || type.isInt() || type.isBigint() || type.isDate();
    }

    // A column of strings whose table declares the format of its dates with a day, and that is not a virtual column.
    private static boolean isDateColumn(ColumnRefOperator column) {
        if (!column.getType().isStringType() || ExpressionStatisticsEstimator.isVirtualColumn(column)) {
            return false;
        }
        StringDateFormat format = PredicateStatisticsCalculator.stringDateFormat(column);
        return format != null && format.hasDay();
    }

    // left(column, n), strleft(column, n), substr(column, 1, n) or substring(column, 1, n): the column, and the
    // position of n among the arguments
    private record PrefixCall(ColumnRefOperator column, int lengthPosition) {
    }

    private static PrefixCall prefixCall(ScalarOperator operator) {
        if (!(operator instanceof CallOperator call) || call.getClass() != CallOperator.class
                || !(call.getChild(0) instanceof ColumnRefOperator column)) {
            return null;
        }
        String name = call.getFnName().toLowerCase(Locale.ROOT);
        if (call.getChildren().size() == 2 && (name.equals(FunctionSet.LEFT) || name.equals(FunctionSet.STRLEFT))) {
            return new PrefixCall(column, 1);
        }
        if (call.getChildren().size() == 3 && (name.equals(FunctionSet.SUBSTR) || name.equals(FunctionSet.SUBSTRING))
                && isInt(call.getChild(1), 1)) {
            return new PrefixCall(column, 2);
        }
        return null;
    }

    private static boolean isInt(ScalarOperator operator, int expected) {
        OptionalDouble number = operator instanceof ConstantOperator constant && !constant.isNull()
                ? ConstantOperatorUtils.doubleValueFromConstant(constant) : OptionalDouble.empty();
        return number.isPresent() && number.getAsDouble() == expected;
    }

    /**
     * The prefixes of a REGEXP predicate of anchored prefixes, see anchoredPrefixes(String); empty for any other
     * predicate.
     */
    static Optional<List<String>> anchoredPrefixes(LikePredicateOperator predicate) {
        if (!predicate.isRegexp() || predicate.getChildren().size() != 2 || !predicate.getChild(1).isConstantRef()) {
            return Optional.empty();
        }
        ConstantOperator constant = (ConstantOperator) predicate.getChild(1);
        if (constant.isNull() || !constant.getType().isStringType()) {
            return Optional.empty();
        }
        return anchoredPrefixes(constant.getVarchar());
    }

    /**
     * The prefixes of a regular expression that is the alternatives p1, p2 and so on, each of them a ^ and then
     * characters that stand for themselves, as ^42894|^40406. A string matches it exactly when it starts with one of
     * the prefixes, which is what c LIKE 'p1%' OR c LIKE 'p2%' keeps, so we estimate it so. A backslash before one of
     * the characters of a regular expression makes it a literal, as in ^a\.b. Empty for any other expression: an
     * alternative that is not anchored, a group, a class, a repeat, an end anchor, a backslash before another
     * character.
     */
    static Optional<List<String>> anchoredPrefixes(String regexp) {
        int[] points = regexp.codePoints().toArray();
        List<String> prefixes = new ArrayList<>();
        StringBuilder prefix = null;
        for (int i = 0; i < points.length; i++) {
            int point = points[i];
            if (prefix == null) {
                if (point != '^') {
                    return Optional.empty();
                }
                prefix = new StringBuilder();
            } else if (point == '|') {
                prefixes.add(prefix.toString());
                prefix = null;
            } else if (point == '\\') {
                if (i + 1 >= points.length || REGEXP_META.indexOf(points[i + 1]) < 0) {
                    return Optional.empty();
                }
                prefix.appendCodePoint(points[++i]);
            } else if (REGEXP_META.indexOf(point) >= 0) {
                return Optional.empty();
            } else {
                prefix.appendCodePoint(point);
            }
        }
        if (prefix == null) {
            return Optional.empty();
        }
        prefixes.add(prefix.toString());
        return Optional.of(prefixes);
    }

    /** The LIKE pattern of the strings that start with a prefix. */
    static String likeOfPrefix(String prefix) {
        return PrefixDerivationRule.escapeLike(prefix) + "%";
    }

    /** left LIKE 'p%', the strings that start with the prefix. */
    static ScalarOperator likeOfPrefix(ScalarOperator left, String prefix) {
        return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, left,
                ConstantOperator.createVarchar(likeOfPrefix(prefix)));
    }

    /** The predicate of anchored prefixes as the OR of LIKE conditions on the same left side. */
    static ScalarOperator likesOfPrefixes(ScalarOperator left, List<String> prefixes) {
        List<ScalarOperator> likes = new ArrayList<>(prefixes.size());
        for (String prefix : prefixes) {
            likes.add(likeOfPrefix(left, prefix));
        }
        return Utils.compoundOr(likes);
    }

    // The flags of the replaced atom, on the condition that replaces it and on the compound conditions inside it.
    private static void inheritFlags(ScalarOperator from, ScalarOperator to) {
        to.setNotEvalEstimate(from.isNotEvalEstimate());
        to.setFromPredicateRangeDerive(from.isFromPredicateRangeDerive());
        to.setRedundant(from.isRedundant());
        to.setIsPushdown(from.isPushdown());
        to.setCorrelated(from.isCorrelated());
        to.setJoinDerived(from.isJoinDerived());
        if (to instanceof CompoundPredicateOperator) {
            for (ScalarOperator child : to.getChildren()) {
                inheritFlags(from, child);
            }
        }
    }

    private static CompoundPredicateOperator withChildren(CompoundPredicateOperator compound,
                                                          List<ScalarOperator> children) {
        CompoundPredicateOperator rebuilt = new CompoundPredicateOperator(compound.getCompoundType(), children);
        rebuilt.setNotEvalEstimate(compound.isNotEvalEstimate());
        rebuilt.setFromPredicateRangeDerive(compound.isFromPredicateRangeDerive());
        rebuilt.setRedundant(compound.isRedundant());
        rebuilt.setIsPushdown(compound.isPushdown());
        rebuilt.setCorrelated(compound.isCorrelated());
        rebuilt.setJoinDerived(compound.isJoinDerived());
        return rebuilt;
    }

    private static final class Normalization {
        private final Statistics statistics;

        private Normalization(Statistics statistics) {
            this.statistics = statistics;
        }

        // The same object when nothing is rewritten.
        private ScalarOperator rewrite(ScalarOperator operator) {
            if (operator.isNotEvalEstimate()) {
                return operator;
            }
            if (operator instanceof CompoundPredicateOperator compound) {
                return compound.isAnd() ? rewriteAnd(compound) : rewriteChildren(compound);
            }
            if (isBooleanIf(operator)) {
                return rewriteIf((CallOperator) operator);
            }
            return rewriteAtom(operator);
        }

        private ScalarOperator rewriteChildren(CompoundPredicateOperator compound) {
            List<ScalarOperator> children = new ArrayList<>(compound.getChildren().size());
            boolean changed = false;
            for (ScalarOperator child : compound.getChildren()) {
                ScalarOperator rewritten = rewrite(child);
                changed |= rewritten != child;
                children.add(rewritten);
            }
            return changed ? withChildren(compound, children) : compound;
        }

        // The three arguments of IF(condition, then, else) are conditions of the estimate.
        private ScalarOperator rewriteIf(CallOperator call) {
            List<ScalarOperator> arguments = new ArrayList<>(call.getChildren().size());
            boolean changed = false;
            for (ScalarOperator argument : call.getChildren()) {
                ScalarOperator rewritten = rewrite(argument);
                changed |= rewritten != argument;
                arguments.add(rewritten);
            }
            if (!changed) {
                return call;
            }
            // The copy of a call copies its arguments, so the ones that stay are set again.
            ScalarOperator copy = call.clone();
            for (int i = 0; i < arguments.size(); i++) {
                copy.setChild(i, arguments.get(i));
            }
            return copy;
        }

        private ScalarOperator rewriteAtom(ScalarOperator atom) {
            ScalarOperator replacement = replacementOf(atom);
            if (replacement == null) {
                return atom;
            }
            inheritFlags(atom, replacement);
            return rewrite(replacement);
        }

        // The condition that replaces an atom by one rule, or null when no rule applies.
        private ScalarOperator replacementOf(ScalarOperator atom) {
            if (!PredicateStatisticsCalculator.needsEvalEstimate(atom, statistics)) {
                return null;
            }
            if (atom instanceof LikePredicateOperator like) {
                return like.isRegexp() ? regexpAsLikes(like) : null;
            }
            if (atom instanceof BinaryPredicateOperator binary) {
                return prefixEqualityAsLike(binary);
            }
            if (atom instanceof InPredicateOperator in && !in.isNotIn()) {
                return inAsEqualities(in);
            }
            return null;
        }

        private static ScalarOperator regexpAsLikes(LikePredicateOperator regexp) {
            return anchoredPrefixes(regexp).map(prefixes -> likesOfPrefixes(regexp.getChild(0), prefixes)).orElse(null);
        }

        private static ScalarOperator prefixEqualityAsLike(BinaryPredicateOperator predicate) {
            if (predicate.getBinaryType() != BinaryType.EQ || !(predicate.getChild(1) instanceof ConstantOperator value)
                    || value.isNull() || !value.getType().isStringType()) {
                return null;
            }
            PrefixCall call = prefixCall(predicate.getChild(0));
            if (call == null || !isDateColumn(call.column())) {
                return null;
            }
            String prefix = value.getVarchar();
            if (!isInt(predicate.getChild(0).getChild(call.lengthPosition()), prefix.codePointCount(0, prefix.length()))) {
                return null;
            }
            return likeOfPrefix(call.column(), prefix);
        }

        private static ScalarOperator inAsEqualities(InPredicateOperator in) {
            return inEqualities(in).map(found -> Utils.compoundOr(equalities(found.left(), found.values())))
                    .orElse(null);
        }

        // The conjuncts of a chain of ANDs are rewritten one by one, and the ranges of one column that keep one value
        // are replaced together.
        private ScalarOperator rewriteAnd(CompoundPredicateOperator and) {
            List<ScalarOperator> conjuncts = new ArrayList<>();
            PredicateStatisticsCalculator.ImpliedRanges.collectConjuncts(and, conjuncts);
            List<ScalarOperator> rewritten = new ArrayList<>(conjuncts.size());
            boolean changed = false;
            for (ScalarOperator conjunct : conjuncts) {
                ScalarOperator conjunctRewritten = rewrite(conjunct);
                changed |= conjunctRewritten != conjunct;
                rewritten.add(conjunctRewritten);
            }
            List<ScalarOperator> withPoints = withPointEqualities(rewritten);
            if (withPoints != rewritten) {
                return Utils.compoundAnd(withPoints);
            }
            return changed ? rebuildAnd(and, rewritten.iterator()) : and;
        }

        // The chain of ANDs of the original with its conjuncts taken in turn from the rewritten ones; a node whose
        // conjuncts are the same objects stays.
        private static ScalarOperator rebuildAnd(ScalarOperator original, Iterator<ScalarOperator> conjuncts) {
            if (original instanceof CompoundPredicateOperator and && and.isAnd() && !and.isNotEvalEstimate()) {
                ScalarOperator first = rebuildAnd(and.getChild(0), conjuncts);
                ScalarOperator second = rebuildAnd(and.getChild(1), conjuncts);
                if (first == and.getChild(0) && second == and.getChild(1)) {
                    return and;
                }
                return withChildren(and, new ArrayList<>(List.of(first, second)));
            }
            return conjuncts.next();
        }

        /**
         * Both sides of a range on a column of whole numbers or days that keep one value, such as date >= a AND
         * date <= a, are the equality date = a: a column has no value between a and a + 1. We want it estimated as the
         * equality, by its path (the MCV list, the histogram or 1 / NDV) and with the decay it has next to the
         * equalities of other columns, while two ranges cut the range of the column twice and give a point no width.
         * So the sides are replaced by the equality before the conjunction is estimated, wherever the value is against
         * the range of the statistics: they can be older than the data, and the equality decides what to do with a
         * value outside of it. A column without statistics keeps its ranges: there the equality takes its default
         * share, and the ranges take the rows of the range. The same list when no sides are replaced.
         */
        private List<ScalarOperator> withPointEqualities(List<ScalarOperator> conjuncts) {
            Map<ColumnRefOperator, DiscreteInterval> columns = null;
            for (int i = 0; i < conjuncts.size(); i++) {
                DiscreteSide side = discreteSide(conjuncts.get(i));
                if (side != null) {
                    if (columns == null) {
                        columns = new LinkedHashMap<>();
                    }
                    columns.computeIfAbsent(side.column(), key -> new DiscreteInterval()).add(side, i);
                }
            }
            if (columns == null) {
                return conjuncts;
            }
            ScalarOperator[] equalities = new ScalarOperator[conjuncts.size()];
            boolean[] dropped = new boolean[conjuncts.size()];
            boolean changed = false;
            for (Map.Entry<ColumnRefOperator, DiscreteInterval> entry : columns.entrySet()) {
                ScalarOperator equality = entry.getValue().equality(entry.getKey());
                if (equality == null || !hasKnownStatistics(entry.getKey())) {
                    continue;
                }
                List<Integer> sides = entry.getValue().indexes;
                inheritFlags(conjuncts.get(sides.get(0)), equality);
                equalities[sides.get(0)] = equality;
                for (int i = 1; i < sides.size(); i++) {
                    dropped[sides.get(i)] = true;
                }
                changed = true;
            }
            if (!changed) {
                return conjuncts;
            }
            List<ScalarOperator> result = new ArrayList<>(conjuncts.size());
            for (int i = 0; i < conjuncts.size(); i++) {
                if (!dropped[i]) {
                    result.add(equalities[i] != null ? equalities[i] : conjuncts.get(i));
                }
            }
            return result;
        }

        // Whether the column has statistics. The statistics can be older than the data, so the equality decides what to
        // do with a value outside of their range, and the point takes the same estimate as it.
        private boolean hasKnownStatistics(ColumnRefOperator column) {
            ColumnStatistic statistic = statistics.getColumnStatistics().get(column);
            return statistic != null && !statistic.isUnknown();
        }

        // A range comparison of a column of whole numbers or days with a constant of its type: the first and the last
        // value it keeps. A comparison of a function of the column, such as days_add('1970-01-01', date) >= a, is read
        // as the bound on the column that it reduces to, when that bound keeps exactly its rows.
        private DiscreteSide discreteSide(ScalarOperator conjunct) {
            if (!(conjunct instanceof BinaryPredicateOperator written) || !written.getBinaryType().isRange()
                    || !PredicateStatisticsCalculator.needsEvalEstimate(conjunct, statistics)) {
                return null;
            }
            BinaryPredicateOperator binary = written;
            if (written.getChild(0) instanceof CallOperator && !(written.getChild(0) instanceof CastOperator)
                    && written.getChild(1).isConstantRef()) {
                MonotonicFilterDerivation.ColumnBounds bounds =
                        MonotonicFilterDerivation.columnBoundsForEstimate(written);
                if (bounds == null || bounds.wider() || bounds.bounds().size() != 1
                        || !(bounds.bounds().get(0) instanceof BinaryPredicateOperator bound)
                        || !bound.getBinaryType().isRange()) {
                    return null;
                }
                binary = bound;
            }
            if (!(binary.getChild(0) instanceof ColumnRefOperator column)
                    || !(binary.getChild(1) instanceof ConstantOperator constant) || constant.isNull()
                    || !constant.getType().equals(column.getType())) {
                return null;
            }
            Long value = discreteValue(constant);
            if (value == null) {
                return null;
            }
            BinaryType type = binary.getBinaryType();
            if (type == BinaryType.GE || type == BinaryType.LE) {
                return new DiscreteSide(column, type == BinaryType.GE, value);
            }
            // > a keeps from a + 1 and < a keeps up to a - 1; a bound at the end of the longs is not read.
            if (value == Long.MAX_VALUE || value == Long.MIN_VALUE) {
                return null;
            }
            return new DiscreteSide(column, type == BinaryType.GT, type == BinaryType.GT ? value + 1 : value - 1);
        }

        // The value of a constant of a whole number type or of a DATE as a long; null for any other type.
        private static Long discreteValue(ConstantOperator constant) {
            Type type = constant.getType();
            if (type.isTinyint()) {
                return (long) constant.getTinyInt();
            } else if (type.isSmallint()) {
                return (long) constant.getSmallint();
            } else if (type.isInt()) {
                return (long) constant.getInt();
            } else if (type.isBigint()) {
                return constant.getBigint();
            } else if (type.isDate()) {
                return constant.getDatetime().toLocalDate().toEpochDay();
            }
            return null;
        }

        // The constant of the type of the column with the value, or null when the type cannot hold it.
        private static ConstantOperator discreteConstant(Type type, long value) {
            if (type.isTinyint()) {
                return value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE
                        ? ConstantOperator.createTinyInt((byte) value) : null;
            } else if (type.isSmallint()) {
                return value >= Short.MIN_VALUE && value <= Short.MAX_VALUE
                        ? ConstantOperator.createSmallInt((short) value) : null;
            } else if (type.isInt()) {
                return value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE
                        ? ConstantOperator.createInt((int) value) : null;
            } else if (type.isBigint()) {
                return ConstantOperator.createBigint(value);
            } else if (type.isDate() && value >= LocalDate.of(0, 1, 1).toEpochDay()
                    && value <= LocalDate.of(9999, 12, 31).toEpochDay()) {
                return ConstantOperator.createDate(LocalDate.ofEpochDay(value).atStartOfDay());
            }
            return null;
        }

        // The first value (lower) or the last value (not lower) that a range comparison of a column keeps.
        private record DiscreteSide(ColumnRefOperator column, boolean lower, long value) {
        }

        // The ranges of one column of whole numbers or days in a conjunction, by the positions of their conjuncts: the
        // values they keep are from the highest first value to the lowest last value.
        private static final class DiscreteInterval {
            private final List<Integer> indexes = new ArrayList<>(2);
            private Long first;
            private Long last;

            private void add(DiscreteSide side, int index) {
                indexes.add(index);
                if (side.lower()) {
                    first = first == null ? side.value() : Math.max(first, side.value());
                } else {
                    last = last == null ? side.value() : Math.min(last, side.value());
                }
            }

            // column = v when the ranges keep the one value v, null otherwise
            private ScalarOperator equality(ColumnRefOperator column) {
                if (first == null || last == null || !first.equals(last)) {
                    return null;
                }
                ConstantOperator value = discreteConstant(column.getType(), first);
                return value == null ? null : new BinaryPredicateOperator(BinaryType.EQ, column, value);
            }
        }
    }
}
