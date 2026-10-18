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
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.TreeSet;

/**
 * The plain estimate of conditions on substrings of a virtual column x of the statistics on an expression, over
 * the string histogram of x. Only x gets these estimates: the estimate of an ordinary column does not change.
 *
 * substr(x, s, n) op constant for s > 1: the head values are evaluated one by one. A bucket whose bounds agree in
 * the first s + n - 1 characters has one substring, seen on its bounds, so it matches as a whole or not at all. In
 * another bucket the strings are taken as uniform over the combinations of characters up to position s + n - 1
 * that lie between the bounds in the order of strings: the first position where the bounds differ takes every
 * character between the bound characters, and a later position the characters the head and the bucket bounds hold
 * at the positions of the substring. Positions after the end of the longer bound are not counted, so the work does
 * not depend on n; when more than 64 positions remain, the condition is not estimated here.
 *
 * substr(x, 1, n) IN (p1, ..., pk) with every pi of length n: one pass over x, the same model as the estimate of
 * one prefix by StringHistogramEstimator for each pi, without an OR of k LIKE conditions. The rows the prefixes
 * take from one bucket are at most the rows of the bucket.
 */
final class ExpressionStatisticsEstimator {
    private ExpressionStatisticsEstimator() {
    }

    /** Whether the operator is a virtual column of the statistics on an expression; their ids are negative. */
    static boolean isVirtualColumn(ScalarOperator operator) {
        return operator instanceof ColumnRefOperator && ((ColumnRefOperator) operator).getId() < 0;
    }

    /**
     * The share of the rows a negation of a condition on one virtual column can hold: the rows where x is not
     * NULL when the condition is NULL on a NULL x, as a comparison, IN or LIKE is: in SQL NOT NULL is NULL, so the
     * negation does not hold on these rows, and the condition does not count them either. A condition that is
     * TRUE or FALSE on NULL, such as x <=> 'a' or x IS NULL, counts the NULL rows on one side, so the negation
     * takes all rows. 1 for any other condition.
     */
    static double negationShare(ScalarOperator condition, Statistics statistics) {
        ColumnRefOperator column = singleVirtualColumn(condition);
        if (column == null || !isNullOnNull(condition, column)) {
            return 1;
        }
        ColumnStatistic statistic = statistics.getColumnStatistics().get(column);
        return statistic == null ? 1 : Math.max(0, 1 - statistic.getNullsFraction());
    }

    // The virtual column a condition reads, null when it reads another column or more than one.
    private static ColumnRefOperator singleVirtualColumn(ScalarOperator condition) {
        List<ColumnRefOperator> columns = Utils.extractColumnRef(condition);
        if (columns.isEmpty() || !columns.stream().allMatch(column -> column.equals(columns.get(0)))
                || !isVirtualColumn(columns.get(0))) {
            return null;
        }
        return columns.get(0);
    }

    /**
     * Whether an operator that reads only the column is NULL whenever the column is NULL, read from its structure as
     * the constant folding would fold it; empty when the structure does not tell, or when the operator reads another
     * column, which the folding leaves unfolded. It answers the common conditions without a rewrite.
     */
    static Optional<Boolean> nullOnNull(ScalarOperator operator, ColumnRefOperator column) {
        for (ColumnRefOperator read : Utils.extractColumnRef(operator)) {
            if (!read.equals(column)) {
                return Optional.empty();
            }
        }
        return nullWithColumn(operator, column);
    }

    private static Optional<Boolean> nullWithColumn(ScalarOperator operator, ColumnRefOperator column) {
        if (operator instanceof ColumnRefOperator) {
            return Optional.of(operator.equals(column));
        }
        if (operator instanceof ConstantOperator) {
            return Optional.of(((ConstantOperator) operator).isNull());
        }
        if (operator instanceof com.starrocks.sql.optimizer.operator.scalar.CastOperator
                || operator instanceof CompoundPredicateOperator && ((CompoundPredicateOperator) operator).isNot()) {
            return nullWithColumn(operator.getChild(0), column);
        }
        if (operator instanceof IsNullPredicateOperator) {
            return Optional.of(false);
        }
        if (operator instanceof InPredicateOperator) {
            if (((InPredicateOperator) operator).isSubquery()) {
                return Optional.empty();
            }
            // NULL IN (...) is NULL; otherwise a NULL in the list decides.
            Optional<Boolean> left = nullWithColumn(operator.getChild(0), column);
            return left.isPresent() && left.get() ? left : Optional.empty();
        }
        boolean strict = operator instanceof LikePredicateOperator
                || operator instanceof BinaryPredicateOperator
                && ((BinaryPredicateOperator) operator).getBinaryType() != BinaryType.EQ_FOR_NULL;
        if (!strict && operator.getClass() == CallOperator.class) {
            com.starrocks.catalog.Function function = ((CallOperator) operator).getFunction();
            strict = function != null && !function.isUdf()
                    && !FunctionSet.ASSERT_TRUE.equals(function.getFunctionName().getFunction())
                    && !com.starrocks.server.GlobalStateMgr.getCurrentState()
                    .isNotAlwaysNullResultWithNullParamFunction(function.getFunctionName().getFunction());
        }
        if (!strict) {
            return Optional.empty();
        }
        // A NULL argument of a strict operator makes it NULL; without one the structure does not tell.
        for (ScalarOperator child : operator.getChildren()) {
            Optional<Boolean> isNull = nullWithColumn(child, column);
            if (isNull.isPresent() && isNull.get()) {
                return isNull;
            }
        }
        return Optional.empty();
    }

    // Whether the condition folds to NULL when the column is NULL.
    private static boolean isNullOnNull(ScalarOperator condition, ColumnRefOperator column) {
        Optional<Boolean> structural = nullOnNull(condition, column);
        if (structural.isPresent()) {
            return structural.get();
        }
        Map<ColumnRefOperator, ScalarOperator> nulls = Map.of(column, ConstantOperator.createNull(column.getType()));
        ScalarOperator folded = new ScalarOperatorRewriter().rewrite(new ReplaceColumnRefRewriter(nulls).rewrite(condition),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        return folded.isConstantRef() && ((ConstantOperator) folded).isNull();
    }

    /**
     * The statistics after NOT of a condition on one virtual column, with a statistic of x of its own: no NULL
     * rows, the head without the values on which the condition holds, and the buckets without the ranges of the
     * prefixes of a prefix condition. We fear that the next condition on x reads the distribution before the
     * negation, or none at all when the MCV propagation sees the same histogram, so the order of the conditions
     * would change the estimate. The negated statistics are returned as they are for any other condition.
     */
    static Statistics withNegatedVirtualColumn(ScalarOperator condition, Statistics input, Statistics negated) {
        ColumnRefOperator column = singleVirtualColumn(condition);
        if (column == null) {
            return negated;
        }
        ColumnStatistic statistic = input.getColumnStatistics().get(column);
        if (statistic == null || !isNullOnNull(condition, column)) {
            return negated;
        }
        Histogram histogram = statistic.getHistogram();
        if (histogram != null && histogram.hasStringValues()) {
            Core core = core("negation", condition, statistic, () -> negatedCore(condition, column, statistic));
            if (core == null) {
                return negated;
            }
            ColumnStatistic updated = ColumnStatistic.buildFrom(statistic).setHistogram(core.filtered())
                    .setNullsFraction(0).setDistinctValuesCount(core.distinct()).build();
            return Statistics.buildFrom(negated).addColumnStatistic(column, updated).build();
        }
        // Without a string histogram the distribution stays, but the NULL rows are gone, and a histogram of its own
        // keeps the MCV propagation from dropping it.
        Histogram copy = histogram == null ? null : histogram.getBuckets().isEmpty()
                ? new Histogram(histogram.getMCV()) : new Histogram(histogram.getBuckets(), histogram.getMCV());
        ColumnStatistic.Builder updated = ColumnStatistic.buildFrom(statistic).setNullsFraction(0);
        ColumnStatistic after = negated.getColumnStatistics().get(column);
        if (after != null) {
            updated.setDistinctValuesCount(after.getDistinctValuesCount());
        }
        return Statistics.buildFrom(negated).addColumnStatistic(column, updated.setHistogram(copy).build()).build();
    }

    // The head of x without the values on which the condition holds, the buckets without its prefix ranges, and the
    // NDV left; null when the condition cannot be evaluated on a head value.
    private static Core negatedCore(ScalarOperator condition, ColumnRefOperator column, ColumnStatistic statistic) {
        Histogram histogram = statistic.getHistogram();
        LinkedHashMap<String, Long> head = new LinkedHashMap<>();
        McvPredicateEvaluator evaluator = new McvPredicateEvaluator();
        List<String> prefixes = prefixesOf(condition);
        List<String> likePrefixes = column.getType().isStringType() ? likePrefixes(condition) : null;
        for (Map.Entry<String, Long> value : histogram.getMCV().entrySet()) {
            Optional<Boolean> holds = likePrefixes != null && value.getKey() != null
                    ? Optional.of(likePrefixes.stream().noneMatch(prefix -> value.getKey().startsWith(prefix)))
                    : McvStatisticsPropagation.negationHolds(condition, column, value.getKey(), evaluator);
            if (holds.isEmpty()) {
                return null;
            }
            if (holds.get()) {
                head.put(value.getKey(), value.getValue());
            }
        }
        Histogram remaining = Histogram.forStrings(prefixes.isEmpty() ? histogram.getBuckets()
                : withoutPrefixes(histogram.getBuckets(), prefixes), McvDistribution.adopt(head));
        long removedHead = histogram.getMCV().size() - head.size();
        long removedBuckets = distinctOf(histogram.getBuckets()) - distinctOf(remaining.getBuckets());
        double distinct = Math.max(head.size(), statistic.getDistinctValuesCount() - removedHead - removedBuckets);
        return new Core(0, distinct, remaining);
    }

    /**
     * The statistics after x IS [NOT] NULL with a statistic of x of its own, for the same reason as after NOT: the
     * non-NULL rows keep the distribution of x, the NULL rows have none.
     */
    static Statistics withNullCheckedVirtualColumn(IsNullPredicateOperator predicate, Statistics input,
                                                   Statistics output) {
        if (!isVirtualColumn(predicate.getChild(0))) {
            return output;
        }
        ColumnRefOperator column = (ColumnRefOperator) predicate.getChild(0);
        ColumnStatistic statistic = input.getColumnStatistics().get(column);
        ColumnStatistic after = output.getColumnStatistics().get(column);
        if (statistic == null || after == null || statistic.getHistogram() == null
                || !statistic.getHistogram().hasStringValues()) {
            return output;
        }
        Histogram histogram = predicate.isNotNull()
                ? Histogram.forStrings(statistic.getHistogram().getBuckets(), statistic.getHistogram().getMCV())
                : Histogram.forStrings(List.of(), Map.of());
        ColumnStatistic updated = ColumnStatistic.buildFrom(after).setHistogram(histogram)
                .setNullsFraction(predicate.isNotNull() ? 0 : 1)
                .setDistinctValuesCount(predicate.isNotNull() ? statistic.getDistinctValuesCount() : 0).build();
        return Statistics.buildFrom(output).addColumnStatistic(column, updated).build();
    }

    /**
     * The prefixes of x LIKE 'p%' or substr(x, 1, n) IN (prefixes of length n), and of every such disjunct of an OR:
     * the values that start with them are the values on which the condition holds. Empty for any other condition.
     */
    static List<String> prefixesOf(ScalarOperator condition) {
        if (condition instanceof CompoundPredicateOperator compound && compound.isOr()) {
            List<String> prefixes = new ArrayList<>();
            for (ScalarOperator disjunct : Utils.extractDisjunctive(condition)) {
                prefixes.addAll(prefixesOf(disjunct));
            }
            return prefixes;
        }
        if (condition instanceof LikePredicateOperator like && !like.isRegexp() && like.getChild(0).isColumnRef()
                && like.getChild(1) instanceof ConstantOperator pattern && !pattern.isNull()) {
            Optional<String> prefix = LikePatternEstimator.pattern(like)
                    .flatMap(LikePatternEstimator.LikePattern::fixedPrefix);
            String escaped = prefix.map(PrefixDerivationRule::escapeLike).orElse(null);
            String text = pattern.getVarchar();
            // 'p%' and 'p%%' both hold on every value that starts with p.
            if (escaped != null && text.startsWith(escaped) && text.length() > escaped.length()
                    && text.substring(escaped.length()).chars().allMatch(c -> c == '%')) {
                return List.of(prefix.get());
            }
            return List.of();
        }
        if (condition instanceof InPredicateOperator in && !in.isNotIn() && !in.isSubquery()) {
            ScalarOperator left = in.getChild(0);
            if (left instanceof CallOperator && left.getChildren().size() == 3 && left.getChild(0).isColumnRef()
                    && FunctionSet.SUBSTR.equals(((CallOperator) left).getFnName().toLowerCase(Locale.ROOT))) {
                Integer start = intConstant(left.getChild(1));
                Integer count = intConstant(left.getChild(2));
                List<String> prefixes = new ArrayList<>();
                for (int i = 1; i < in.getChildren().size(); i++) {
                    ScalarOperator child = in.getChild(i);
                    if (start == null || start != 1 || count == null || !(child instanceof ConstantOperator)
                            || ((ConstantOperator) child).isNull() || !child.getType().isStringType()
                            || length(((ConstantOperator) child).getVarchar()) != count) {
                        return List.of();
                    }
                    prefixes.add(((ConstantOperator) child).getVarchar());
                }
                return prefixes;
            }
        }
        return List.of();
    }

    /**
     * The prefixes of x LIKE 'p%' or of an OR of such conditions, null for any other condition. On a value that is
     * not NULL such a LIKE is TRUE exactly when the value starts with p, as the LIKE pattern matches it, so NOT of the
     * condition holds when the value starts with none of them.
     */
    private static List<String> likePrefixes(ScalarOperator condition) {
        List<String> prefixes = new ArrayList<>();
        for (ScalarOperator disjunct : Utils.extractDisjunctive(condition)) {
            if (!(disjunct instanceof LikePredicateOperator)) {
                return null;
            }
            List<String> prefix = prefixesOf(disjunct);
            if (prefix.isEmpty()) {
                return null;
            }
            prefixes.addAll(prefix);
        }
        return prefixes.isEmpty() ? null : prefixes;
    }

    /**
     * The buckets without the strings that start with any of the prefixes, in one pass: the ranges [p, the next
     * string after p) are sorted and joined, and each bucket is cut by the ranges it meets, from the lowest up, as
     * cutting the ranges one at a time would.
     */
    private static List<Bucket> withoutPrefixes(List<Bucket> buckets, List<String> prefixes) {
        TreeSet<String> sorted = new TreeSet<>(StringBucket::compare);
        sorted.addAll(prefixes);
        List<String> starts = new ArrayList<>();
        List<String> ends = new ArrayList<>();
        for (String prefix : sorted) {
            String end = successor(prefix);
            int last = starts.size() - 1;
            if (last >= 0 && (ends.get(last) == null || StringBucket.compare(prefix, ends.get(last)) < 0)) {
                // Inside the range of a shorter prefix.
                if (ends.get(last) != null && (end == null || StringBucket.compare(end, ends.get(last)) > 0)) {
                    ends.set(last, end);
                }
                continue;
            }
            starts.add(prefix);
            ends.add(end);
        }
        List<Bucket> result = new ArrayList<>();
        long cumulative = 0;
        long previous = 0;
        int first = 0;
        for (Bucket raw : buckets) {
            StringBucket bucket = (StringBucket) raw;
            long mass = bucket.getCount() - previous;
            previous = bucket.getCount();
            if (mass <= 0) {
                continue;
            }
            while (first < starts.size() && ends.get(first) != null
                    && StringBucket.compare(ends.get(first), bucket.getLowerString()) <= 0) {
                first++;
            }
            if (first >= starts.size() || StringBucket.compare(starts.get(first), bucket.getUpperString()) > 0) {
                // No range meets the bucket: it stays whole, the same object while no rows were cut before it.
                if (cumulative + mass == bucket.getCount() && bucket.getDistinctCount().isPresent()) {
                    result.add(bucket);
                    cumulative += mass;
                } else {
                    cumulative = append(result, new StringBucket(bucket.getLowerString(), bucket.getUpperString(),
                            mass, bucket.getUpperRepeats(), bucket.getDistinctCount().orElse(1L),
                            bucket.isLowerInclusive(), bucket.isUpperInclusive()), cumulative);
                }
                continue;
            }
            StringBucket rest = new StringBucket(bucket.getLowerString(), bucket.getUpperString(), mass,
                    bucket.getUpperRepeats(), bucket.getDistinctCount().orElse(1L), bucket.isLowerInclusive(),
                    bucket.isUpperInclusive());
            for (int i = first; i < starts.size() && rest != null
                    && StringBucket.compare(starts.get(i), bucket.getUpperString()) <= 0; i++) {
                cumulative = append(result, StringHistogramEstimator.clip(rest, rest.getCount(), 0, starts.get(i),
                        BinaryType.LT, false), cumulative);
                rest = ends.get(i) == null ? null
                        : StringHistogramEstimator.clip(rest, rest.getCount(), 0, ends.get(i), BinaryType.GE, false);
            }
            cumulative = append(result, rest, cumulative);
        }
        return result;
    }

    // Adds a piece of one bucket, counted from zero, after the cumulative rows so far; null adds nothing.
    private static long append(List<Bucket> result, StringBucket piece, long cumulative) {
        if (piece == null || piece.getCount() <= 0) {
            return cumulative;
        }
        cumulative += piece.getCount();
        result.add(new StringBucket(piece.getLowerString(), piece.getUpperString(), cumulative,
                piece.getUpperRepeats(), piece.getDistinctCount().orElse(1L), piece.isLowerInclusive(),
                piece.isUpperInclusive()));
        return cumulative;
    }

    private static long distinctOf(List<Bucket> buckets) {
        long distinct = 0;
        for (Bucket bucket : buckets) {
            distinct += bucket.getDistinctCount().orElse(0L);
        }
        return distinct;
    }

    // What a condition on a substring of x gives over the histogram of x, before the rows of the input.
    private record Core(double matched, double distinct, Histogram filtered) {
    }

    /** x LIKE 'p%' over the string histogram of x, as StringHistogramEstimator.prefix estimates it. */
    static Statistics prefix(ScalarOperator like, ColumnRefOperator column, ColumnStatistic statistic, String prefix,
                             Statistics input) {
        Histogram filtered = core("prefix", like, statistic,
                () -> new Core(0, 0, StringHistogramEstimator.prefixHistogram(statistic, prefix))).filtered();
        return StringHistogramEstimator.prefixResult(column, statistic, input, filtered);
    }

    // The conditions on virtual columns are written by the planner for the estimate only and do not change, and the
    // attach of a scan and the cores are kept for the query, so the derivations of one query ask for the same
    // condition on the same histogram of x many times; the statistic around it is built again by each derivation. A
    // core reads the histogram and the NDV of x, so it is kept in the cache of the query by the identity of the
    // condition and of the histogram, and the NDV; null when the condition is not estimated here, which is not kept.
    private static Core core(String kind, ScalarOperator predicate, ColumnStatistic statistic,
                             java.util.function.Supplier<Core> compute) {
        ExpressionStatisticsCache cache = ExpressionStatisticsCache.current();
        if (cache == null) {
            return compute.get();
        }
        Histogram histogram = statistic.getHistogram();
        return cache.part(List.of(kind, new ExpressionStatisticsCache.Identity(predicate),
                        new ExpressionStatisticsCache.Identity(histogram), statistic.getDistinctValuesCount()), compute,
                core -> ExpressionStatisticsCache.units(predicate) + ExpressionStatisticsCache.units(histogram)
                        + ExpressionStatisticsCache.units(core.filtered()));
    }

    /** The estimate of a condition on a substring of a virtual column; empty for any other condition. */
    static Optional<Statistics> estimate(ScalarOperator predicate, Statistics input) {
        if (!(predicate instanceof BinaryPredicateOperator) && !(predicate instanceof InPredicateOperator)) {
            return Optional.empty();
        }
        ScalarOperator left = predicate.getChild(0);
        if (!(left instanceof CallOperator) || left.getClass() != CallOperator.class
                || !FunctionSet.SUBSTR.equals(((CallOperator) left).getFnName().toLowerCase(Locale.ROOT))
                || left.getChildren().size() != 3 || !isVirtualColumn(left.getChild(0))) {
            return Optional.empty();
        }
        Integer start = intConstant(left.getChild(1));
        Integer count = intConstant(left.getChild(2));
        ColumnRefOperator column = (ColumnRefOperator) left.getChild(0);
        ColumnStatistic statistic = input.getColumnStatistics().get(column);
        if (start == null || count == null || start < 1 || count < 0 || statistic == null
                || statistic.getHistogram() == null || !statistic.getHistogram().hasStringValues()) {
            return Optional.empty();
        }
        List<String> constants = new ArrayList<>();
        for (int i = 1; i < predicate.getChildren().size(); i++) {
            ScalarOperator child = predicate.getChild(i);
            if (!(child instanceof ConstantOperator) || ((ConstantOperator) child).isNull()
                    || !child.getType().isStringType()) {
                return Optional.empty();
            }
            constants.add(((ConstantOperator) child).getVarchar());
        }
        if (predicate instanceof InPredicateOperator) {
            boolean negated = ((InPredicateOperator) predicate).isNotIn();
            if (start == 1 && !negated && constants.stream().allMatch(value -> length(value) == count)) {
                return Optional.of(result(column, statistic, input, core("prefix in", predicate, statistic,
                        () -> prefixIn(statistic, count, prefixes(predicate, constants)))));
            }
            if (start == 1) {
                return Optional.empty();
            }
            Match in = negated ? Match.NOT_IN : Match.IN;
            return Optional.ofNullable(core("substring", predicate, statistic,
                    () -> substring(statistic, start, count, in, constants).orElse(null)))
                    .map(core -> result(column, statistic, input, core));
        }
        if (start == 1) {
            return Optional.empty();
        }
        BinaryType type = ((BinaryPredicateOperator) predicate).getBinaryType();
        Match match = switch (type) {
            case EQ -> Match.IN;
            case NE -> Match.NOT_IN;
            case LT -> Match.LT;
            case LE -> Match.LE;
            case GT -> Match.GT;
            case GE -> Match.GE;
            default -> null;
        };
        if (match == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(core("substring", predicate, statistic,
                () -> substring(statistic, start, count, match, constants).orElse(null)))
                .map(core -> result(column, statistic, input, core));
    }

    private enum Match { IN, NOT_IN, LT, LE, GT, GE }

    private static Integer intConstant(ScalarOperator operator) {
        if (!(operator instanceof ConstantOperator) || ((ConstantOperator) operator).isNull()
                || !operator.getType().isInt()) {
            return null;
        }
        return ((ConstantOperator) operator).getInt();
    }

    // The code points of a string; the common string of one Java char per code point takes no stream.
    static int[] codePoints(String value) {
        int count = value.codePointCount(0, value.length());
        int[] points = new int[count];
        if (count == value.length()) {
            for (int i = 0; i < count; i++) {
                points[i] = value.charAt(i);
            }
            return points;
        }
        for (int i = 0, offset = 0; i < count; i++) {
            int point = value.codePointAt(offset);
            points[i] = point;
            offset += Character.charCount(point);
        }
        return points;
    }

    private static int length(String value) {
        return value.codePointCount(0, value.length());
    }

    // The characters from position start (1-based), at most count of them, as the BE substr gives them.
    private static int[] substr(int[] points, int start, int count) {
        int from = Math.min(points.length, start - 1);
        int to = (int) Math.min(points.length, (long) from + count);
        int[] result = new int[to - from];
        System.arraycopy(points, from, result, 0, result.length);
        return result;
    }

    // The distinct prefixes of an IN in the order of strings, the first string after each, and the set of them.
    private record Prefixes(List<String> sorted, String[] successors, Set<String> lookup) {
    }

    // The prefixes of an IN depend on the IN only, so they are kept in the cache of the query for every histogram of
    // x the IN is estimated on.
    private static Prefixes prefixes(ScalarOperator in, List<String> constants) {
        java.util.function.Supplier<Prefixes> compute = () -> {
            TreeSet<String> sorted = new TreeSet<>(StringBucket::compare);
            sorted.addAll(constants);
            List<String> prefixes = new ArrayList<>(sorted);
            String[] successors = new String[prefixes.size()];
            for (int i = 0; i < successors.length; i++) {
                successors[i] = successor(prefixes.get(i));
            }
            return new Prefixes(prefixes, successors, new HashSet<>(prefixes));
        };
        ExpressionStatisticsCache cache = ExpressionStatisticsCache.current();
        return cache == null ? compute.get() : cache.part(List.of("prefixes", new ExpressionStatisticsCache.Identity(in)),
                compute, prefixes -> ExpressionStatisticsCache.units(in) + 3L * prefixes.sorted().size());
    }

    private static Core prefixIn(ColumnStatistic statistic, int count, Prefixes sortedPrefixes) {
        Histogram histogram = statistic.getHistogram();
        List<String> prefixes = sortedPrefixes.sorted();
        String[] successors = sortedPrefixes.successors();
        Set<String> lookup = sortedPrefixes.lookup();
        Map<String, Long> head = new LinkedHashMap<>();
        histogram.getMCV().forEach((value, rows) -> {
            int[] points = codePoints(value);
            if (points.length >= count && lookup.contains(new String(points, 0, count))) {
                head.put(value, rows);
            }
        });
        List<Bucket> pieces = new ArrayList<>();
        long cumulative = 0;
        long previous = 0;
        int first = 0;
        double partDistinct = 0;
        for (Bucket raw : histogram.getBuckets()) {
            StringBucket bucket = (StringBucket) raw;
            long mass = bucket.getCount() - previous;
            previous = bucket.getCount();
            if (mass <= 0) {
                continue;
            }
            // The ranges [p, successor of p) are disjoint and sorted, and so are the buckets: a range that ends
            // before this bucket ends before every later bucket.
            while (first < prefixes.size()) {
                String end = successors[first];
                if (end != null && StringBucket.compare(end, bucket.getLowerString()) <= 0) {
                    first++;
                } else {
                    break;
                }
            }
            // Each prefix takes its share of the bucket by the model of one prefix. The shares of many prefixes
            // together can hold more rows than the bucket, so all of them shrink in proportion, rows and NDV alike,
            // and no prefix loses its rows to the prefixes before it.
            StringBucket single = new StringBucket(bucket.getLowerString(), bucket.getUpperString(), mass,
                    bucket.getUpperRepeats(), bucket.getDistinctCount().orElse(1L), bucket.isLowerInclusive(),
                    bucket.isUpperInclusive());
            List<StringBucket> parts = new ArrayList<>();
            List<Long> partRows = new ArrayList<>();
            double total = 0;
            for (int i = first; i < prefixes.size()
                    && StringBucket.compare(prefixes.get(i), bucket.getUpperString()) <= 0; i++) {
                StringBucket part = prefix(single, mass, prefixes.get(i), successors[i]);
                if (part != null && part.getCount() > 0) {
                    parts.add(part);
                    partRows.add(part.getCount());
                    total += part.getCount();
                }
            }
            double scale = total > mass ? mass / total : 1;
            double running = 0;
            long start = cumulative;
            for (int i = 0; i < parts.size(); i++) {
                StringBucket part = parts.get(i);
                running += partRows.get(i);
                long end = start + Math.round(running * scale);
                long rows = end - cumulative;
                if (rows <= 0) {
                    continue;
                }
                cumulative = end;
                double ndv = part.getDistinctCount().orElse(1L) * scale;
                partDistinct += ndv;
                pieces.add(new StringBucket(part.getLowerString(), part.getUpperString(), cumulative,
                        Math.min(rows, Math.round(part.getUpperRepeats() * scale)), Math.max(1, Math.round(ndv)),
                        part.isLowerInclusive(), part.isUpperInclusive()));
            }
        }
        Histogram filtered = Histogram.forStrings(pieces, head);
        double distinct = Math.min(statistic.getDistinctValuesCount(), head.size() + partDistinct);
        return new Core(StringHistogramEstimator.rows(filtered), distinct, filtered);
    }

    // The part of one bucket with the given prefix, as StringHistogramEstimator.prefix cuts it; null when no row is
    // left.
    private static StringBucket prefix(StringBucket bucket, long mass, String prefix, String upper) {
        StringBucket part = StringHistogramEstimator.clip(bucket, mass, 0, prefix, BinaryType.GE, false);
        return part == null || upper == null ? part
                : StringHistogramEstimator.clip(part, part.getCount(), 0, upper, BinaryType.LT, false);
    }

    // The first string after every string with the prefix; null when no such string exists.
    static String successor(String prefix) {
        int[] points = codePoints(prefix);
        for (int i = points.length - 1; i >= 0; i--) {
            if (points[i] < Character.MAX_CODE_POINT) {
                int successor = points[i] + 1;
                if (successor == Character.MIN_SURROGATE) {
                    successor = Character.MAX_SURROGATE + 1;
                }
                return new String(points, 0, i) + new String(Character.toChars(successor));
            }
        }
        return null;
    }

    private static Optional<Core> substring(ColumnStatistic statistic, int start, int count, Match match,
                                            List<String> constants) {
        Histogram histogram = statistic.getHistogram();
        long end = start + (long) count - 1;
        List<int[]> values = new ArrayList<>();
        Set<String> distinctConstants = new HashSet<>();
        for (String constant : constants) {
            if (distinctConstants.add(constant)) {
                values.add(codePoints(constant));
            }
        }
        Bounds bounds = bounds(histogram);
        int positions = (int) Math.min(end, bounds.longest());
        if (positions > MAX_POSITIONS) {
            return Optional.empty();
        }
        // The characters the head and the bucket bounds hold at the positions of the substring.
        int[][] characters = {new int[16]};
        int[] size = {0};
        histogram.getMCV().keySet().forEach(value -> addPositions(characters, size, value, start, end));
        for (int i = 0; i < bounds.lower().length; i++) {
            addPositions(characters, size, bounds.lower()[i], start, end);
            addPositions(characters, size, bounds.upper()[i], start, end);
        }
        int[] alphabet = sortedDistinct(characters[0], size[0]);
        // The walk over each bucket takes each position once per constant, and a search in the alphabet each time.
        double work = (double) histogram.getBuckets().size() * positions * (values.size() + 1)
                * (1 + Math.log(alphabet.length + 1)) + alphabet.length;
        if (work > WORK_BUDGET) {
            return Optional.empty();
        }

        Map<String, Long> head = new LinkedHashMap<>();
        histogram.getMCV().forEach((value, rows) -> {
            if (matchesValue(substr(codePoints(value), start, count), match, values, distinctConstants)) {
                head.put(value, rows);
            }
        });
        List<Bucket> buckets = new ArrayList<>();
        double bucketRows = 0;
        double bucketDistinct = 0;
        long previous = 0;
        long cumulative = 0;
        for (int index = 0; index < histogram.getBuckets().size(); index++) {
            StringBucket bucket = (StringBucket) histogram.getBuckets().get(index);
            long mass = bucket.getCount() - previous;
            previous = bucket.getCount();
            if (mass <= 0) {
                continue;
            }
            int[] lower = bounds.lower()[index];
            int[] upper = bounds.upper()[index];
            long repeats = Math.min(mass, bucket.getUpperRepeats());
            OptionalDouble share = share(lower, upper, bucket.isLowerInclusive(), bucket.isUpperInclusive(), start,
                    positions, alphabet, match, values, distinctConstants);
            if (share.isEmpty()) {
                return Optional.empty();
            }
            double rows = matchesValue(substr(upper, start, count), match, values, distinctConstants) ? repeats : 0;
            rows += (mass - repeats) * share.getAsDouble();
            if (rows <= 0) {
                continue;
            }
            bucketRows += rows;
            double distinct = Math.max(1, bucket.getDistinctCount().orElse(1L) * rows / mass);
            bucketDistinct += distinct;
            cumulative += Math.max(1, Math.round(rows));
            buckets.add(new StringBucket(bucket.getLowerString(), bucket.getUpperString(), cumulative, 0,
                    (long) Math.ceil(distinct), bucket.isLowerInclusive(), bucket.isUpperInclusive()));
        }
        Histogram filtered = Histogram.forStrings(buckets, head);
        double headRows = head.values().stream().mapToLong(Long::longValue).sum();
        return Optional.of(new Core(headRows + bucketRows, head.size() + bucketDistinct, filtered));
    }

    // Adds the characters of a value at the positions of the substring to a growing array of the given size.
    // The code points of the bounds of each bucket, and the length of the longest head value or bound.
    private record Bounds(int[][] lower, int[][] upper, int longest) {
    }

    // The bounds depend on the histogram only, so they are kept in the cache of the query for every condition on a
    // substring of x.
    private static Bounds bounds(Histogram histogram) {
        java.util.function.Supplier<Bounds> compute = () -> {
            List<Bucket> buckets = histogram.getBuckets();
            int[][] lower = new int[buckets.size()][];
            int[][] upper = new int[buckets.size()][];
            // A string of a bucket can be longer than its bounds: positions are counted up to the longest value seen.
            int longest = 0;
            for (String value : histogram.getMCV().keySet()) {
                longest = Math.max(longest, length(value));
            }
            for (int i = 0; i < buckets.size(); i++) {
                lower[i] = codePoints(((StringBucket) buckets.get(i)).getLowerString());
                upper[i] = codePoints(((StringBucket) buckets.get(i)).getUpperString());
                longest = Math.max(longest, Math.max(lower[i].length, upper[i].length));
            }
            return new Bounds(lower, upper, longest);
        };
        ExpressionStatisticsCache cache = ExpressionStatisticsCache.current();
        return cache == null ? compute.get() : cache.part(List.of("bounds", new ExpressionStatisticsCache.Identity(
                histogram)), compute, bounds -> 3 * ExpressionStatisticsCache.units(histogram));
    }

    private static void addPositions(int[][] characters, int[] size, String value, int start, long end) {
        addPositions(characters, size, codePoints(value), start, end);
    }

    private static void addPositions(int[][] characters, int[] size, int[] points, int start, long end) {
        for (int i = start - 1; i < end && i < points.length; i++) {
            if (size[0] == characters[0].length) {
                characters[0] = Arrays.copyOf(characters[0], 2 * size[0]);
            }
            characters[0][size[0]++] = points[i];
        }
    }

    private static int[] sortedDistinct(int[] values, int size) {
        int[] sorted = Arrays.copyOf(values, size);
        Arrays.sort(sorted);
        int distinct = 0;
        for (int i = 0; i < sorted.length; i++) {
            if (i == 0 || sorted[i] != sorted[i - 1]) {
                sorted[distinct++] = sorted[i];
            }
        }
        return Arrays.copyOf(sorted, distinct);
    }

    // IN and NOT IN look the substring up among the texts of the constants: a head can hold many values, and an IN
    // many constants.
    private static boolean matchesValue(int[] substring, Match match, List<int[]> values, Set<String> texts) {
        if (match == Match.IN || match == Match.NOT_IN) {
            return texts.contains(new String(substring, 0, substring.length)) == (match == Match.IN);
        }
        boolean anyEqual = false;
        for (int[] value : values) {
            int cmp = compare(substring, value);
            switch (match) {
                case IN:
                case NOT_IN:
                    anyEqual |= cmp == 0;
                    break;
                case LT:
                    return cmp < 0;
                case LE:
                    return cmp <= 0;
                case GT:
                    return cmp > 0;
                default:
                    return cmp >= 0;
            }
        }
        return match == Match.IN ? anyEqual : !anyEqual;
    }

    private static int compare(int[] left, int[] right) {
        for (int i = 0; i < left.length && i < right.length; i++) {
            if (left[i] != right[i]) {
                return Integer.compare(left[i], right[i]);
            }
        }
        return Integer.compare(left.length, right.length);
    }

    // The most positions the count of combinations between bucket bounds walks; a longer substring is not estimated.
    private static final int MAX_POSITIONS = 64;

    // The most buckets times constants times positions the estimate of one condition on a substring walks; a larger
    // condition takes the plain estimate, so the planning time stays bounded.
    private static final double WORK_BUDGET = 20_000_000;

    private static final int LESS = 0;
    private static final int EQUAL = 1;
    private static final int GREATER = 2;
    // The states of the walk: whether the combination is equal to the lower bound so far, to the upper one, and the
    // order of its substring with the value, in one array.
    private static final int STATES = 12;

    private static int state(int tightLow, int tightHigh, int order) {
        return (tightLow * 2 + tightHigh) * 3 + order;
    }

    /**
     * The share of the strings between the bounds whose substring matches: the combinations of characters on the
     * first positions that lie between the bounds and match, over all combinations between the bounds. A bound that
     * is excluded is not one of the combinations. Empty when the count is not a finite share.
     */
    private static OptionalDouble share(int[] lowerBound, int[] upperBound, boolean lowerIncluded,
                                        boolean upperIncluded, int start, int positions, int[] alphabet,
                                        Match match, List<int[]> values, Set<String> texts) {
        // Strings are compared on the positions counted; a bound longer than them is cut there, and the strings
        // that start with the cut bound are still between the bounds.
        int lowerLength = Math.min(lowerBound.length, positions);
        int upperLength = Math.min(upperBound.length, positions);
        boolean excludeLower = !lowerIncluded && lowerBound.length == positions;
        boolean excludeUpper = !upperIncluded && upperBound.length == positions;
        int common = 0;
        while (common < lowerLength && common < upperLength && lowerBound[common] == upperBound[common]) {
            common++;
        }
        if (common == lowerLength && common == upperLength && lowerBound.length == upperBound.length
                && common == lowerBound.length) {
            // Both bounds are the same string: it is the only value of the bucket.
            return OptionalDouble.of(matchesValue(substr(lowerBound, start, Integer.MAX_VALUE), match, values, texts)
                    && !excludeLower && !excludeUpper ? 1 : 0);
        }
        if (common >= positions) {
            // The bounds agree on every position counted: every string between them has the same substring.
            return OptionalDouble.of(matchesValue(substr(Arrays.copyOf(lowerBound, lowerLength), start,
                    Integer.MAX_VALUE), match, values, texts) ? 1 : 0);
        }
        if (common >= upperLength) {
            // The upper bound is a proper prefix of the lower one: no string lies between them.
            return OptionalDouble.of(0);
        }
        int[] lower = Arrays.copyOf(lowerBound, lowerLength);
        int[] upper = Arrays.copyOf(upperBound, upperLength);
        Characters[] sets = positionSets(lower, upper, positions, common, alphabet);
        int first = Math.min(start - 1, positions);
        double[] initial = new double[STATES];
        initial[state(1, 1, EQUAL)] = 1;
        // The positions before the substring do not depend on the value, so they are walked once. On a position
        // where the bounds agree the walk keeps all its weight on the state equal to both, so it starts after them.
        double[] atFirst = walk(initial, Math.min(common, first), first, lower, upper, first, sets, null);
        double total = finish(walk(atFirst, first, positions, lower, upper, first, sets, null), null, positions - first,
                excludeLower, excludeUpper)[EQUAL];
        double matching = 0;
        switch (match) {
            case IN:
            case NOT_IN:
                for (int[] value : values) {
                    matching += equalWeight(atFirst, first, lower, upper, sets, value, excludeLower, excludeUpper);
                }
                if (match == Match.NOT_IN) {
                    matching = total - matching;
                }
                break;
            default:
                int[] value = values.get(0);
                double[] order = finish(walk(atFirst, first, positions, lower, upper, first, sets, value), value,
                        positions - first, excludeLower, excludeUpper);
                matching = match == Match.LT ? order[LESS] : match == Match.LE ? order[LESS] + order[EQUAL]
                        : match == Match.GT ? order[GREATER] : order[GREATER] + order[EQUAL];
        }
        if (!(total > 0) || !Double.isFinite(total) || !Double.isFinite(matching)) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(Math.max(0, Math.min(1, matching / total)));
    }

    // The weight of the combinations whose substring from position first is the value: one character per position.
    private static double equalWeight(double[] atFirst, int first, int[] lower, int[] upper, Characters[] sets,
                                      int[] value, boolean excludeLower, boolean excludeUpper) {
        int positions = sets.length;
        if (value.length != positions - first) {
            return 0;
        }
        // The weight by whether the combination is equal to the lower bound so far and to the upper one.
        double[] current = new double[4];
        for (int tightLow = 0; tightLow < 2; tightLow++) {
            for (int tightHigh = 0; tightHigh < 2; tightHigh++) {
                current[tightLow * 2 + tightHigh] = atFirst[state(tightLow, tightHigh, EQUAL)];
            }
        }
        for (int i = first; i < positions; i++) {
            int character = value[i - first];
            Characters set = sets[i];
            if (!set.contains(character)) {
                return 0;
            }
            double weight = 1.0 / set.size();
            double[] next = new double[4];
            for (int tightLow = 0; tightLow < 2; tightLow++) {
                for (int tightHigh = 0; tightHigh < 2; tightHigh++) {
                    double mass = current[tightLow * 2 + tightHigh];
                    if (mass == 0) {
                        continue;
                    }
                    boolean low = tightLow == 1 && i < lower.length;
                    boolean high = tightHigh == 1;
                    if ((high && i >= upper.length) || (low && character < lower[i])
                            || (high && character > upper[i])) {
                        continue;
                    }
                    next[(low && character == lower[i] ? 2 : 0) + (high && character == upper[i] ? 1 : 0)] += mass * weight;
                }
            }
            current = next;
        }
        double weight = 0;
        for (int tightLow = 0; tightLow < 2; tightLow++) {
            for (int tightHigh = 0; tightHigh < 2; tightHigh++) {
                if (!(excludeLower && tightLow == 1) && !(excludeUpper && tightHigh == 1)) {
                    weight += current[tightLow * 2 + tightHigh];
                }
            }
        }
        return weight;
    }

    /**
     * The number of strings of the length of the longer bound between two prefixes, both included unless the upper
     * one is excluded, by the same model of characters as the share of a substring. A lower prefix shorter than the
     * upper one is a value of its own. Infinite when the prefixes are longer than {@link #MAX_POSITIONS}.
     */
    static double prefixCount(String lowerText, String upperText, boolean upperIncluded, int[] alphabet) {
        int[] lower = codePoints(lowerText);
        int[] upper = codePoints(upperText);
        int positions = Math.max(lower.length, upper.length);
        if (positions > MAX_POSITIONS) {
            return Double.POSITIVE_INFINITY;
        }
        int common = 0;
        while (common < lower.length && common < upper.length && lower[common] == upper[common]) {
            common++;
        }
        if (common >= positions) {
            return 1;
        }
        if (common >= upper.length) {
            return 0;
        }
        if (common == positions - 1 && lower.length == positions && upper.length == positions) {
            // The bounds differ at the last position only, which takes every character between them.
            return Math.max(0, upper[common] - lower[common] + 1 - (upperIncluded ? 0 : 1));
        }
        Characters[] sets = positionSets(lower, upper, positions, common, alphabet);
        double[] initial = new double[STATES];
        initial[state(1, 1, EQUAL)] = 1;
        double weight = finish(walk(initial, 0, positions, lower, upper, positions, sets, null), null, 0, false,
                false)[EQUAL];
        double count = 0;
        if (weight > 0) {
            double log = Math.log(weight);
            for (Characters set : sets) {
                log += Math.log(set.size());
            }
            count = Math.rint(Math.exp(log));
        }
        if (!upperIncluded && upper.length == positions) {
            count -= 1;
        }
        if (lower.length < positions) {
            count += 1;
        }
        return Math.max(0, count);
    }

    /**
     * The characters each position can hold between the bounds, which agree on the first common positions. The
     * sorted alphabet is shared by all positions, each of which adds at most its two bound characters to it.
     */
    private static Characters[] positionSets(int[] lower, int[] upper, int positions, int common, int[] alphabet) {
        Characters[] sets = new Characters[positions];
        for (int i = 0; i < positions; i++) {
            if (i < common) {
                sets[i] = Characters.of(new int[] {lower[i]}, -1, -1);
            } else if (i == common) {
                int high = upper[i];
                int low = i < lower.length ? lower[i] : alphabet.length == 0 ? high : Math.min(high, alphabet[0]);
                sets[i] = Characters.between(low, high);
            } else {
                sets[i] = Characters.of(alphabet, i < lower.length ? lower[i] : -1, i < upper.length ? upper[i] : -1);
            }
        }
        return sets;
    }

    /**
     * The weight of the combinations w between the bounds from position from to position to, w[i] from the set of
     * position i, by the state: whether w is equal to the lower bound so far, to the upper bound so far, and the
     * order of the substring of w from position first with the value, [LESS, EQUAL, GREATER]. Without a value the
     * order stays EQUAL. Each choice at position i weighs 1 / the size of its set, so the weights stay within
     * [0, 1] and their ratios are the ratios of the counts of combinations.
     */
    private static double[] walk(double[] states, int from, int to, int[] lower, int[] upper, int first,
                                 Characters[] sets, int[] value) {
        double[] current = states;
        long[] special = new long[3];
        for (int i = from; i < to; i++) {
            double[] next = new double[STATES];
            Characters set = sets[i];
            double weight = 1.0 / set.size();
            for (int tightLow = 0; tightLow < 2; tightLow++) {
                for (int tightHigh = 0; tightHigh < 2; tightHigh++) {
                    for (int order = 0; order < 3; order++) {
                        double mass = current[state(tightLow, tightHigh, order)];
                        if (mass == 0) {
                            continue;
                        }
                        boolean low = tightLow == 1 && i < lower.length;
                        boolean high = tightHigh == 1;
                        if (high && i >= upper.length) {
                            // w goes on after the whole upper bound, so it is above it.
                            continue;
                        }
                        long lowest = low ? lower[i] : Long.MIN_VALUE;
                        long highest = high ? upper[i] : Long.MAX_VALUE;
                        if (lowest > highest) {
                            continue;
                        }
                        int fixed = order;
                        long target = Long.MIN_VALUE;
                        if (value != null && i >= first && order == EQUAL) {
                            int j = i - first;
                            if (j >= value.length) {
                                fixed = GREATER;
                            } else {
                                target = value[j];
                            }
                        }
                        boolean compare = target != Long.MIN_VALUE;
                        // The characters that change the state: the bound characters and the character of the value.
                        int specials = 0;
                        if (low) {
                            special[specials++] = lowest;
                        }
                        if (high && (specials == 0 || special[0] != highest)) {
                            special[specials++] = highest;
                        }
                        if (compare && target >= lowest && target <= highest
                                && (specials < 1 || special[0] != target) && (specials < 2 || special[1] != target)) {
                            special[specials++] = target;
                        }
                        long specialBelow = 0;
                        long specialAbove = 0;
                        for (int k = 0; k < specials; k++) {
                            long character = special[k];
                            if (!set.contains(character)) {
                                continue;
                            }
                            int nextLow = low && character == lowest ? 1 : 0;
                            int nextHigh = high && character == highest ? 1 : 0;
                            int nextOrder = compare ? character < target ? LESS
                                    : character == target ? EQUAL : GREATER : fixed;
                            next[state(nextLow, nextHigh, nextOrder)] += mass * weight;
                            if (!compare || character < target) {
                                specialBelow++;
                            } else if (character > target) {
                                specialAbove++;
                            }
                        }
                        if (compare) {
                            long below = set.count(lowest, Math.min(highest, target - 1)) - specialBelow;
                            long above = set.count(Math.max(lowest, target + 1), highest) - specialAbove;
                            next[state(0, 0, LESS)] += mass * weight * below;
                            next[state(0, 0, GREATER)] += mass * weight * above;
                        } else {
                            next[state(0, 0, fixed)] += mass * weight * (set.count(lowest, highest) - specialBelow);
                        }
                    }
                }
            }
            current = next;
        }
        return current;
    }

    /**
     * The weights by order at the end of the walk, without a bound that is excluded. A value longer than the
     * substring of the given length is above it when the substring is its prefix.
     */
    private static double[] finish(double[] states, int[] value, int length, boolean excludeLower,
                                   boolean excludeUpper) {
        double[] result = new double[3];
        for (int tightLow = 0; tightLow < 2; tightLow++) {
            for (int tightHigh = 0; tightHigh < 2; tightHigh++) {
                if ((excludeLower && tightLow == 1) || (excludeUpper && tightHigh == 1)) {
                    continue;
                }
                for (int order = 0; order < 3; order++) {
                    int at = order == EQUAL && value != null && value.length > Math.max(0, length) ? LESS : order;
                    result[at] += states[state(tightLow, tightHigh, order)];
                }
            }
        }
        return result;
    }

    // The characters a position can hold: a sorted list with at most two more characters, or every code point
    // between two characters.
    private static final class Characters {
        private final int[] sorted;
        private final int[] added;
        private final long low;
        private final long high;

        private Characters(int[] sorted, int[] added, long low, long high) {
            this.sorted = sorted;
            this.added = added;
            this.low = low;
            this.high = high;
        }

        // The sorted characters and the given ones, -1 for none.
        static Characters of(int[] sorted, int first, int second) {
            int[] added = new int[2];
            int count = 0;
            for (int character : new int[] {first, second}) {
                if (character >= 0 && Arrays.binarySearch(sorted, character) < 0
                        && (count == 0 || added[0] != character)) {
                    added[count++] = character;
                }
            }
            return new Characters(sorted, Arrays.copyOf(added, count), 0, -1);
        }

        static Characters between(int low, int high) {
            return new Characters(null, null, low, high);
        }

        long size() {
            return sorted != null ? sorted.length + added.length : high - low + 1;
        }

        boolean contains(long character) {
            if (sorted == null) {
                return character >= low && character <= high;
            }
            if (character < Integer.MIN_VALUE || character > Integer.MAX_VALUE) {
                return false;
            }
            for (int other : added) {
                if (other == character) {
                    return true;
                }
            }
            return Arrays.binarySearch(sorted, (int) character) >= 0;
        }

        // The characters from one to another, both included.
        long count(long from, long to) {
            if (from > to) {
                return 0;
            }
            if (sorted == null) {
                return Math.max(0, Math.min(to, high) - Math.max(from, low) + 1);
            }
            long count = indexAbove(to) - (from == Long.MIN_VALUE ? 0 : indexAbove(from - 1));
            for (int other : added) {
                if (other >= from && other <= to) {
                    count++;
                }
            }
            return count;
        }

        // The number of sorted characters not above the bound.
        private int indexAbove(long bound) {
            if (bound < Integer.MIN_VALUE) {
                return 0;
            }
            if (bound >= Integer.MAX_VALUE) {
                return sorted.length;
            }
            int index = Arrays.binarySearch(sorted, (int) bound);
            return index >= 0 ? index + 1 : -index - 1;
        }
    }

    // The estimate of a condition that does not hold on NULL, with the filtered distribution of the column.
    private static Statistics result(ColumnRefOperator column, ColumnStatistic statistic, Statistics input, Core core) {
        double matched = core.matched();
        double distinct = core.distinct();
        Histogram filtered = core.filtered();
        double share = Math.max(0, Math.min(1, matched / Math.max(1, StringHistogramEstimator.rows(
                statistic.getHistogram()))));
        double rows = input.getOutputRowCount() * (1 - statistic.getNullsFraction()) * share;
        ColumnStatistic updated = ColumnStatistic.buildFrom(statistic).setHistogram(filtered).setNullsFraction(0)
                .setDistinctValuesCount(Math.max(0, distinct)).build();
        return Statistics.buildFrom(input).setOutputRowCount(rows).addColumnStatistic(column, updated).build();
    }
}
