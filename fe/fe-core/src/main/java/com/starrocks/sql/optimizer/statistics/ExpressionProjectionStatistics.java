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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The statistics of a projection right above a scan with statistics on expressions: an output that is an
 * expression f of the statistics, or a substring of a prefix f = substr(c, 1, K), gets its distribution from x.
 * We use x only when its statistics describe the rows of the scan: no estimated condition of the scan reads the
 * columns of f, or every such condition is written over x. With a condition on c itself, as in
 * WHERE c = '10312345' GROUP BY left(c, 3), x describes the whole table, and the projection is estimated as before.
 */
final class ExpressionProjectionStatistics {
    private static final Logger LOG = LogManager.getLogger(ExpressionProjectionStatistics.class);

    private ExpressionProjectionStatistics() {
    }

    /** The statistics of the projected expression from x, null when no statistics on expressions describe it. */
    static ColumnStatistic calculate(ScalarOperator projection, ScanExpressionStatistics expressions,
                                     double outputRows) {
        if (expressions == null || projection.isColumnRef() || projection.isConstantRef()
                || Utils.extractColumnRef(projection).stream().noneMatch(expressions.getSourceColumns()::contains)) {
            return null;
        }
        ColumnStatistic statistic = expressions.getCache().projection(projection, expressions,
                () -> compute(projection, expressions));
        if (statistic == null) {
            return null;
        }
        double ndv = Math.max(1, Math.min(statistic.getDistinctValuesCount(), Math.max(1, outputRows)));
        return ColumnStatistic.buildFrom(statistic).setDistinctValuesCount(ndv).build();
    }

    // The statistics before the bound by the rows of the output, which can differ for one scan with a LIMIT.
    private static ColumnStatistic compute(ScalarOperator projection, ScanExpressionStatistics expressions) {
        try {
            ExpressionStatisticsCache cache = expressions.getCache();
            ScalarOperator canonical = cache.canonical(projection);
            ExpressionStatisticsCanonicalizer.Key key = cache.key(canonical);
            for (ScanExpressionStatistics.Entry entry : expressions.getEntries()) {
                if (!expressions.describesScanRows(entry.getColumn())) {
                    continue;
                }
                int[] positions = null;
                if (!entry.getKey().equals(key)) {
                    PrefixDerivationRule rule = PrefixDerivationRule.of(entry);
                    positions = rule == null ? null : rule.positionsOver(canonical);
                    if (positions == null) {
                        continue;
                    }
                }
                ColumnStatistic statistic = filteredStatistic(entry, expressions);
                if (statistic == null || positions == null) {
                    return statistic;
                }
                return substring(statistic, positions[0], positions[1], cache);
            }
        } catch (RuntimeException e) {
            LOG.warn("Failed to derive the statistics of projection {} from statistics on expressions of table {}",
                    projection, expressions.getTableName(), e);
        }
        return null;
    }

    /**
     * x after the scan filter. The estimate of an OR keeps the histogram of its first branch, so with an OR over x
     * the statistics are built again from x before the filter: the head values on which the condition holds and
     * the buckets that meet at least one branch of it. An OR with a part on a real column holds rows that have any
     * value of x, as we cannot tell which values the rows of that part have, so it narrows neither the head nor the
     * buckets of x; the NDV of x is bounded by the one the estimate of the scan gave.
     */
    private static ColumnStatistic filteredStatistic(ScanExpressionStatistics.Entry entry,
                                                     ScanExpressionStatistics expressions) {
        if (!expressions.virtualPredicateHasOr() || expressions.getVirtualPredicate() == null) {
            return entry.getStatistic();
        }
        ColumnRefOperator x = entry.getColumn();
        List<ScalarOperator> conditions = new ArrayList<>();
        boolean partOnColumn = false;
        boolean orWithColumn = false;
        for (ScalarOperator conjunct : Utils.extractConjuncts(expressions.getVirtualPredicate())) {
            List<ColumnRefOperator> columns = Utils.extractColumnRef(conjunct);
            if (!columns.isEmpty() && columns.stream().allMatch(x::equals)) {
                conditions.add(conjunct);
            } else if (columns.contains(x)) {
                partOnColumn = true;
                orWithColumn |= ExpressionStatisticsMatcher.containsOr(conjunct);
            }
        }
        ColumnStatistic unfiltered = entry.getUnfilteredStatistic();
        if (conditions.isEmpty() && !orWithColumn) {
            return entry.getStatistic();
        }
        Histogram histogram = unfiltered.getHistogram();
        if (histogram == null) {
            return null;
        }
        Map<String, Long> head = new LinkedHashMap<>();
        // Each LIKE spends one work budget over the head values, in their order.
        Map<ScalarOperator, LikePatternEstimator.Work> works = new java.util.IdentityHashMap<>();
        histogram.getMCV().forEach((value, rows) -> {
            if (conditions.stream().allMatch(condition -> truth(condition, x, value, works) != Boolean.FALSE)) {
                head.put(value, rows);
            }
        });
        List<Bucket> buckets = new ArrayList<>();
        long previous = 0;
        long cumulative = 0;
        double distinct = head.size();
        for (Bucket bucket : histogram.getBuckets()) {
            long mass = bucket.getCount() - previous;
            previous = bucket.getCount();
            if (mass <= 0) {
                continue;
            }
            if (!(bucket instanceof StringBucket)) {
                // Rows of unknown values can meet every branch, so they stay with the values of x outside the head.
                cumulative += mass;
                buckets.add(new UnknownRangeBucket(cumulative));
                distinct += Math.min(mass, unknownDistinct(unfiltered));
                continue;
            }
            StringBucket string = (StringBucket) bucket;
            if (conditions.stream().allMatch(condition -> meets(condition, string))) {
                cumulative += mass;
                buckets.add(new StringBucket(string.getLowerString(), string.getUpperString(), cumulative,
                        string.getUpperRepeats(), string.getDistinctCount().orElse(1L), string.isLowerInclusive(),
                        string.isUpperInclusive()));
                distinct += string.getDistinctCount().orElse(1L);
            }
        }
        Histogram rebuilt = histogram.hasStringValues() ? Histogram.forStrings(buckets, head) : new Histogram(buckets, head);
        // A condition with a part on a real column also limits x, but the values of x cannot tell which rows that part
        // keeps. We want the NDV not above the one the estimate of the scan gave after all its conditions.
        double bound = partOnColumn ? Math.min(unfiltered.getDistinctValuesCount(),
                entry.getStatistic().getDistinctValuesCount()) : unfiltered.getDistinctValuesCount();
        // A condition over x alone is never true on a NULL of x. An OR with a part on a column can be true on it.
        double nulls = conditions.isEmpty() ? entry.getStatistic().getNullsFraction() : 0;
        return ColumnStatistic.buildFrom(unfiltered).setHistogram(rebuilt).setNullsFraction(nulls)
                .setDistinctValuesCount(Math.max(1, Math.min(distinct, bound))).build();
    }

    // The values of x outside its head.
    private static double unknownDistinct(ColumnStatistic statistic) {
        return Math.max(0, statistic.getDistinctValuesCount() - statistic.getHistogram().getMCV().size());
    }

    // Whether a constant condition holds: TRUE does, FALSE and NULL do not.
    private static boolean isTrue(ConstantOperator constant) {
        return !constant.isNull() && constant.getType().isBoolean() && constant.getBoolean();
    }

    /**
     * Whether the condition holds on a value of x: TRUE, FALSE, or null when it cannot be evaluated. We keep a value
     * on which the condition is unknown, so the negation of an unknown condition stays unknown.
     */
    private static Boolean truth(ScalarOperator condition, ColumnRefOperator x, String value,
                                 Map<ScalarOperator, LikePatternEstimator.Work> works) {
        if (condition instanceof ConstantOperator) {
            return isTrue((ConstantOperator) condition);
        }
        if (condition instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compound = (CompoundPredicateOperator) condition;
            Boolean left = truth(condition.getChild(0), x, value, works);
            if (compound.isNot()) {
                return left == null ? null : !left;
            }
            Boolean right = truth(condition.getChild(1), x, value, works);
            if (compound.isAnd()) {
                return Boolean.FALSE.equals(left) || Boolean.FALSE.equals(right) ? Boolean.FALSE
                        : left == null || right == null ? null : Boolean.TRUE;
            }
            return Boolean.TRUE.equals(left) || Boolean.TRUE.equals(right) ? Boolean.TRUE
                    : left == null || right == null ? null : Boolean.FALSE;
        }
        return MultiColumnMcvEstimator.matchesComponent(condition, x, value,
                works.computeIfAbsent(condition, leaf -> new LikePatternEstimator.Work())).orElse(null);
    }

    // Whether a bucket of x can hold a value on which the condition holds; anything not understood can.
    private static boolean meets(ScalarOperator condition, StringBucket bucket) {
        if (condition instanceof ConstantOperator) {
            return isTrue((ConstantOperator) condition);
        }
        if (condition instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compound = (CompoundPredicateOperator) condition;
            if (compound.isNot()) {
                return true;
            }
            boolean left = meets(condition.getChild(0), bucket);
            boolean right = meets(condition.getChild(1), bucket);
            return compound.isAnd() ? left && right : left || right;
        }
        if (condition instanceof LikePredicateOperator && condition.getChild(0).isColumnRef()) {
            Optional<String> prefix = LikePatternEstimator.pattern((LikePredicateOperator) condition)
                    .flatMap(LikePatternEstimator.LikePattern::fixedPrefix);
            return prefix.map(value -> meetsPrefix(value, bucket)).orElse(true);
        }
        if (condition instanceof BinaryPredicateOperator && condition.getChild(0).isColumnRef()) {
            String value = text(condition.getChild(1));
            if (value == null) {
                return true;
            }
            BinaryType type = ((BinaryPredicateOperator) condition).getBinaryType();
            int low = StringBucket.compare(bucket.getLowerString(), value);
            int high = StringBucket.compare(bucket.getUpperString(), value);
            return switch (type) {
                case EQ -> low <= 0 && high >= 0;
                case LT -> low < 0;
                case LE -> low <= 0;
                case GT -> high > 0;
                case GE -> high >= 0;
                default -> true;
            };
        }
        if (condition instanceof InPredicateOperator && !((InPredicateOperator) condition).isNotIn()) {
            if (!condition.getChild(0).isColumnRef()) {
                // Only substr(x, 1, n) IN (prefixes of length n) bounds the values; any other function of x can
                // take its values anywhere in the bucket.
                List<String> prefixes = ExpressionStatisticsEstimator.prefixesOf(condition);
                return prefixes.isEmpty() || prefixes.stream().anyMatch(prefix -> meetsPrefix(prefix, bucket));
            }
            for (int i = 1; i < condition.getChildren().size(); i++) {
                String value = text(condition.getChild(i));
                if (value == null) {
                    return true;
                }
                if (StringBucket.compare(bucket.getLowerString(), value) <= 0
                        && StringBucket.compare(bucket.getUpperString(), value) >= 0) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    private static boolean meetsPrefix(String prefix, StringBucket bucket) {
        String end = ExpressionStatisticsEstimator.successor(prefix);
        return StringBucket.compare(prefix, bucket.getUpperString()) <= 0
                && (end == null || StringBucket.compare(end, bucket.getLowerString()) > 0);
    }

    private static boolean reachesBelow(String bound, int count, java.util.BitSet alphabet) {
        int[] points = ExpressionStatisticsEstimator.codePoints(bound);
        int smallest = alphabet.isEmpty() ? 0 : alphabet.nextSetBit(0);
        for (int i = count; i < points.length; i++) {
            if (points[i] > smallest) {
                return true;
            }
        }
        return false;
    }

    private static String text(ScalarOperator operator) {
        if (!(operator instanceof ConstantOperator) || ((ConstantOperator) operator).isNull()
                || !operator.getType().isStringType()) {
            return null;
        }
        return ((ConstantOperator) operator).getVarchar();
    }

    private static String substr(String value, int start, int count) {
        int[] points = ExpressionStatisticsEstimator.codePoints(value);
        int from = Math.min(points.length, start - 1);
        int to = (int) Math.min(points.length, (long) from + count);
        return new String(points, from, to - from);
    }

    // The distribution of substr over x, which depends only on the histogram and the NDV of x.
    private record Mapped(Histogram histogram, double distinct) {
    }

    /**
     * substr(x, s, n) from the statistics of x: the head values are mapped by substr and equal results add up.
     * For s = 1 the order is kept, so a bucket maps to the prefixes between the prefixes of its bounds: a bucket of
     * one prefix joins the head entry of that prefix, and a prefix that the head or an earlier bucket has is counted
     * once. For s > 1 the rows outside the head are one bucket of unknown bounds, with at most as many values as
     * strings of length n over the characters seen at these positions. The values of x outside the head whose
     * bounds are unknown count as the NDV of x less its head, within the same bounds and the rows. The whole NDV is
     * at most the NDV of x. The derivations of one query map the same histogram of x again, so the mapping is kept
     * in the cache of the query by the identity of the histogram and the NDV, which are all it reads.
     */
    private static ColumnStatistic substring(ColumnStatistic statistic, int start, int count,
                                             ExpressionStatisticsCache cache) {
        Histogram histogram = statistic.getHistogram();
        if (histogram == null) {
            return null;
        }
        Mapped mapped = cache.part(List.of("projection", new ExpressionStatisticsCache.Identity(histogram),
                        statistic.getDistinctValuesCount(), start, count), () -> map(statistic, start, count),
                result -> ExpressionStatisticsCache.units(histogram) + ExpressionStatisticsCache.units(result.histogram()));
        return ColumnStatistic.buildFrom(statistic).setHistogram(mapped.histogram())
                .setDistinctValuesCount(mapped.distinct())
                .setMinValue(Double.NEGATIVE_INFINITY).setMaxValue(Double.POSITIVE_INFINITY).build();
    }

    private static Mapped map(ColumnStatistic statistic, int start, int count) {
        Histogram histogram = statistic.getHistogram();
        Map<String, Long> head = new LinkedHashMap<>();
        histogram.getMCV().forEach((value, rows) -> head.merge(substr(value, start, count), rows, Long::sum));
        int headDistinct = head.size();
        java.util.BitSet alphabet = new java.util.BitSet();
        for (String value : head.keySet()) {
            for (int point : ExpressionStatisticsEstimator.codePoints(value)) {
                alphabet.set(point);
            }
        }
        for (Bucket bucket : histogram.getBuckets()) {
            if (bucket instanceof StringBucket) {
                for (String bound : List.of(((StringBucket) bucket).getLowerString(),
                        ((StringBucket) bucket).getUpperString())) {
                    for (int point : ExpressionStatisticsEstimator.codePoints(substr(bound, start, count))) {
                        alphabet.set(point);
                    }
                }
            }
        }
        int[] characters = alphabet.stream().toArray();
        // The prefixes counted so far: those of the head, and those the buckets added. The buckets are sorted and do
        // not overlap, so a prefix added by an earlier bucket is not above the lower prefix of a later one, and
        // only the last added can be in its range.
        String[] headPrefixes = head.keySet().toArray(new String[0]);
        java.util.Arrays.sort(headPrefixes, StringBucket::compare);
        java.util.Set<String> headSet = new java.util.HashSet<>(head.keySet());
        String lastAdded = null;
        List<Bucket> buckets = new ArrayList<>();
        double bucketDistinct = 0;
        long bucketRows = 0;
        long unknownRows = 0;
        long previous = 0;
        for (Bucket bucket : histogram.getBuckets()) {
            long mass = bucket.getCount() - previous;
            previous = bucket.getCount();
            if (mass <= 0) {
                continue;
            }
            if (!(bucket instanceof StringBucket)) {
                unknownRows += mass;
                continue;
            }
            StringBucket string = (StringBucket) bucket;
            long ndv = string.getDistinctCount().orElse(1L);
            if (start > 1) {
                bucketRows += mass;
                bucketDistinct += ndv;
                continue;
            }
            String lower = substr(string.getLowerString(), 1, count);
            String upper = substr(string.getUpperString(), 1, count);
            // Strings below an excluded upper bound start with its prefix only when the bound goes on after the
            // prefix with a character above the smallest one seen: below 10450000 are strings of 104, below
            // 10400000 or 104 there are none.
            boolean upperReached = string.isUpperInclusive()
                    || reachesBelow(string.getUpperString(), count, alphabet);
            double prefixes = ExpressionStatisticsEstimator.prefixCount(lower, upper, upperReached, characters);
            if (prefixes <= 1) {
                if (!headSet.contains(lower) && !lower.equals(lastAdded)) {
                    bucketDistinct += 1;
                }
                lastAdded = lower;
                head.merge(lower, mass, Long::sum);
                continue;
            }
            int known = countBetween(headPrefixes, lower, upper, upperReached)
                    + (lower.equals(lastAdded) && !headSet.contains(lower) ? 1 : 0);
            double fresh = Math.max(0, Math.min(ndv, prefixes - known));
            bucketDistinct += fresh;
            bucketRows += mass;
            buckets.add(new StringBucket(lower, upper, bucketRows, upperReached ? Math.min(mass, string.getUpperRepeats())
                    : 0, Math.max(1, (long) Math.ceil(fresh)), true, upperReached));
            lastAdded = upperReached ? upper : lower;
        }
        double strings = Math.pow(Math.max(1, alphabet.cardinality()), count);
        if (start > 1) {
            bucketDistinct = Math.min(bucketDistinct, strings);
        }
        double tailDistinct = 0;
        if (unknownRows > 0) {
            tailDistinct = Math.min(Math.min(unknownDistinct(statistic), unknownRows), strings);
        }
        Histogram mapped;
        if (start == 1 && unknownRows == 0 && histogram.hasStringValues()) {
            mapped = Histogram.forStrings(buckets, head);
        } else if (bucketRows + unknownRows == 0) {
            mapped = Histogram.forStrings(List.of(), head);
        } else {
            long headRows = head.values().stream().mapToLong(Long::longValue).sum();
            mapped = Histogram.ofSingleBucket(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                    headRows + bucketRows + unknownRows, head);
        }
        double distinct = Math.min(statistic.getDistinctValuesCount(), headDistinct + bucketDistinct + tailDistinct);
        return new Mapped(mapped, Math.max(1, distinct));
    }

    // The sorted strings from one to another, the upper one included or not.
    private static int countBetween(String[] sorted, String lower, String upper, boolean upperIncluded) {
        int from = java.util.Arrays.binarySearch(sorted, lower, StringBucket::compare);
        from = from >= 0 ? from : -from - 1;
        int to = java.util.Arrays.binarySearch(sorted, upper, StringBucket::compare);
        to = to >= 0 ? (upperIncluded ? to + 1 : to) : -to - 1;
        return Math.max(0, to - from);
    }
}
