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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * The work of the statistics on expressions that one query repeats: the statistics of a group are derived again
 * for each plan alternative, and the same predicate and projection objects come back. The cache lives as long as
 * the optimizer context of the query, so nothing it holds outlives the query. It also keeps the estimate of the
 * predicate of a scan of a table with MCV statistics, see #scanEstimate.
 *
 * We fear that a query with long predicates or many scans makes the cache large, so it is bounded by an estimate of
 * what it holds, not by the number of entries: one unit per operator of a kept expression, per bucket and head value
 * of a kept histogram, and per kept result of a condition on one value. An entry above the bound is not kept, and the
 * cache is cleared when the next entry does not fit.
 */
public final class ExpressionStatisticsCache {
    private static final long MAX_UNITS = 200_000;

    // The cache of the estimate that runs on this thread, see within(): the estimators of conditions on virtual
    // columns get their statistics without the context of the query.
    private static final ThreadLocal<ExpressionStatisticsCache> CURRENT = new ThreadLocal<>();

    private final Map<ScalarOperator, ScalarOperator> canonical = new IdentityHashMap<>();
    private final Map<ScalarOperator, Optional<ExpressionStatisticsCanonicalizer.Key>> keys = new IdentityHashMap<>();
    // A match is kept by the identity of the predicate: equal predicates can differ in the order and the flags of
    // their conjuncts, which the match reads, and the written copy refers to the conjuncts of its predicate.
    private final Map<ScalarOperator, Map<MatchKey, Optional<ExpressionStatisticsMatcher.Result>>> matches =
            new IdentityHashMap<>();
    // The rewrites of a query build the predicate again as new objects. A predicate of equal conjuncts in the same
    // order, with the same flags, on the same statistics takes the same match, written over its own conjunct objects,
    // so the estimates kept for the written conditions are found again.
    private final Map<StructuralKey, Optional<ExpressionStatisticsMatcher.Result>> structuralMatches = new HashMap<>();
    private final Map<ScalarOperator, Map<ScanExpressionStatistics, Optional<ColumnStatistic>>> projections =
            new IdentityHashMap<>();
    private final Map<List<Object>, ScanExpressionStatistics.Attached> attachments = new HashMap<>();
    private final Map<List<Object>, Object> parts = new HashMap<>();
    private final Map<ScanEstimateKey, Statistics> scanEstimates = new HashMap<>();
    private final Map<List<Object>, Statistics> mcvAttachments = new HashMap<>();
    private long units;

    /** An object compared by identity in a key. */
    record Identity(Object object) {
        @Override
        public boolean equals(Object other) {
            return other instanceof Identity && ((Identity) other).object == object;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(object);
        }
    }

    /**
     * What a match reads besides the predicate: the statistics on expressions, whether a column of the scan has
     * unknown statistics, and the flags of each conjunct. An operator can take its flags after it is built, so they
     * are read on every lookup.
     */
    private record MatchKey(Identity expressions, boolean anyUnknown, List<Boolean> flags) {
    }

    // The conjuncts compared one by one in their order, which the equality of an AND does not keep.
    private record StructuralKey(List<ScalarOperator> conjuncts, MatchKey match) {
    }

    /**
     * Runs an estimate with this cache as the one the estimators of conditions on virtual columns use. The thread
     * holds the cache only while the estimate runs.
     */
    <T> T within(Supplier<T> estimate) {
        ExpressionStatisticsCache outer = CURRENT.get();
        CURRENT.set(this);
        try {
            return estimate.get();
        } finally {
            if (outer == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(outer);
            }
        }
    }

    /** The cache of the estimate that runs on this thread, null outside of within(). */
    static ExpressionStatisticsCache current() {
        return CURRENT.get();
    }

    /** The canonical form of an expression, computed once per expression object. */
    synchronized ScalarOperator canonical(ScalarOperator operator) {
        ScalarOperator result = canonical.get(operator);
        if (result == null) {
            result = ExpressionStatisticsCanonicalizer.canonicalize(operator);
            put(canonical, operator, result, units(operator) + units(result));
        }
        return result;
    }

    /** The key of a canonical form, computed once per canonical object; null when it has no key. */
    synchronized ExpressionStatisticsCanonicalizer.Key key(ScalarOperator canonicalForm) {
        Optional<ExpressionStatisticsCanonicalizer.Key> result = keys.get(canonicalForm);
        if (result == null) {
            result = Optional.ofNullable(ExpressionStatisticsCanonicalizer.keyOf(canonicalForm));
            put(keys, canonicalForm, result, 2 * units(canonicalForm));
        }
        return result.orElse(null);
    }

    ExpressionStatisticsMatcher.Result match(ScalarOperator predicate, ScanExpressionStatistics expressions,
                                             Statistics statistics,
                                             Supplier<ExpressionStatisticsMatcher.Result> compute) {
        boolean anyUnknown = statistics.getColumnStatistics().values().stream().anyMatch(ColumnStatistic::isUnknown);
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        List<Boolean> flags = new ArrayList<>();
        for (ScalarOperator conjunct : conjuncts) {
            flags.add(conjunct.isNotEvalEstimate());
            flags.add(conjunct.isFromPredicateRangeDerive());
        }
        MatchKey key = new MatchKey(new Identity(expressions), anyUnknown, flags);
        StructuralKey structuralKey = new StructuralKey(conjuncts, key);
        long size = 2 * units(predicate) + flags.size();
        synchronized (this) {
            Map<MatchKey, Optional<ExpressionStatisticsMatcher.Result>> byKey = matches.get(predicate);
            if (byKey != null && byKey.containsKey(key)) {
                return byKey.get(key).orElse(null);
            }
            Optional<ExpressionStatisticsMatcher.Result> known = structuralMatches.get(structuralKey);
            if (known != null) {
                ExpressionStatisticsMatcher.Result result = known.map(found -> found.forConjuncts(conjuncts))
                        .orElse(null);
                put(matches, predicate, new HashMap<>(), size).put(key, Optional.ofNullable(result));
                return result;
            }
        }
        ExpressionStatisticsMatcher.Result result = compute.get();
        synchronized (this) {
            put(matches, predicate, new HashMap<>(), size).put(key, Optional.ofNullable(result));
            put(structuralMatches, structuralKey, Optional.ofNullable(result), size);
        }
        return result;
    }

    ColumnStatistic projection(ScalarOperator expression, ScanExpressionStatistics expressions,
                               Supplier<ColumnStatistic> compute) {
        synchronized (this) {
            Map<ScanExpressionStatistics, Optional<ColumnStatistic>> byScan = projections.get(expression);
            if (byScan != null && byScan.containsKey(expressions)) {
                return byScan.get(expressions).orElse(null);
            }
        }
        ColumnStatistic result = compute.get();
        synchronized (this) {
            put(projections, expression, new IdentityHashMap<>(),
                    units(expression) + units(result == null ? null : result.getHistogram()))
                    .put(expressions, Optional.ofNullable(result));
        }
        return result;
    }

    /** The attach of a scan, once per key of the objects and values it reads. */
    ScanExpressionStatistics.Attached attached(List<Object> key,
                                                Supplier<ScanExpressionStatistics.Attached> compute) {
        synchronized (this) {
            ScanExpressionStatistics.Attached known = attachments.get(key);
            if (known != null) {
                return known;
            }
        }
        ScanExpressionStatistics.Attached result = compute.get();
        long size = key.size();
        if (result.expressions() != null) {
            for (ScanExpressionStatistics.Entry entry : result.expressions().getEntries()) {
                size += units(entry.getExpression()) + units(entry.getStatistic().getHistogram());
            }
        }
        synchronized (this) {
            return put(attachments, key, result, size);
        }
    }

    /**
     * A part of an estimate, computed once per key; units gives the size of the part with its key. A null part is
     * not kept, so a failed computation runs again.
     */
    @SuppressWarnings("unchecked")
    <T> T part(List<Object> key, Supplier<T> compute, ToLongFunction<T> units) {
        synchronized (this) {
            Object known = parts.get(key);
            if (known != null) {
                return (T) known;
            }
        }
        T result = compute.get();
        if (result == null) {
            return null;
        }
        synchronized (this) {
            return (T) put(parts, key, result, units.applyAsLong(result));
        }
    }

    /**
     * The estimate of the predicate of a scan over the statistics of the scan, computed once per key, see
     * ScanEstimateKey. We expect the planner to derive the statistics of one scan many times: the memo derives the
     * scan for each plan alternative, and the rules that reorder joins derive it again outside of the memo. Every
     * derivation estimates the same predicate over the same statistics of the table, and with an MCV list each
     * estimate evaluates the head many times. A predicate the key cannot compare is estimated every time; a failure
     * to build the key leaves the estimate as it is.
     */
    Statistics scanEstimate(ScalarOperator predicate, Statistics statistics, Supplier<Statistics> compute) {
        ScanEstimateKey key;
        try {
            key = ScanEstimateKey.of(predicate, statistics);
        } catch (RuntimeException e) {
            key = null;
        }
        if (key == null) {
            return compute.get();
        }
        synchronized (this) {
            Statistics known = scanEstimates.get(key);
            if (known != null) {
                return known;
            }
        }
        Statistics result = compute.get();
        ScanEstimateKey kept;
        try {
            kept = key.copy();
        } catch (RuntimeException e) {
            return result;
        }
        long size = units(predicate) + units(result);
        synchronized (this) {
            return put(scanEstimates, kept, result, size);
        }
    }

    /**
     * The statistics of a scan with the MCV statistics of its table attached, see
     * StatisticsCalcUtils#withExternalMcvStats, computed once per key. We expect the planner to derive each scan many
     * times in a query, and the attach builds the column groups and the statistics of their elements every time. The
     * key holds what the attach reads: the table and its MCV statistics by identity, the column references of the
     * scan with their columns in the order of the scan, every value of the statistics it attaches to, and the session
     * values, see ScanEstimateKey. Two copies of one CTE read the table through other column references and get other
     * keys. A result that is the input statistics itself is not kept, and a failure to build the key leaves the
     * attach as it is.
     */
    Statistics mcvAttachment(Table table, ExternalMcvStatistics mcv, Map<ColumnRefOperator, Column> columns,
                             Statistics statistics, Supplier<Statistics> compute) {
        List<Object> key;
        try {
            List<Object> scanColumns = new ArrayList<>(2 * columns.size());
            columns.forEach((column, meta) -> {
                scanColumns.add(ScanEstimateKey.ColumnKey.of(column));
                scanColumns.add(new Identity(meta));
            });
            key = List.of(new Identity(table), new Identity(mcv), scanColumns, ScanEstimateKey.content(statistics),
                    ScanEstimateKey.session());
        } catch (RuntimeException e) {
            return compute.get();
        }
        synchronized (this) {
            Statistics known = mcvAttachments.get(key);
            if (known != null) {
                return known;
            }
        }
        Statistics result = compute.get();
        if (result == statistics) {
            return result;
        }
        // The heads of the column groups belong to the MCV statistics of the table, and every attach of the table
        // shares them. We fear that counting a head for each kept attach clears the cache early, and a clear makes
        // the next attach create virtual columns with new ids, so we count one unit per group.
        long size = key.size() + columns.size() + 1 + result.getColumnStatistics().size()
                + result.getMultiColumnCombinedStats().size();
        synchronized (this) {
            return put(mcvAttachments, key, result, size);
        }
    }

    /** The size of statistics in units: the column statistics and the head tuples of the column groups. */
    static long units(Statistics statistics) {
        long count = 1 + statistics.getColumnStatistics().size();
        for (MultiColumnCombinedStats group : statistics.getMultiColumnCombinedStats().values()) {
            count += 1 + group.getMcv().size();
        }
        return count;
    }

    /** Counts units that an entry kept earlier takes as it grows. */
    synchronized void grow(long added) {
        units += added;
        if (units > MAX_UNITS) {
            clear();
        }
    }

    /** The size of an expression in units: its operators. */
    static long units(ScalarOperator operator) {
        if (operator == null) {
            return 0;
        }
        long count = 0;
        Deque<ScalarOperator> pending = new ArrayDeque<>();
        pending.push(operator);
        while (!pending.isEmpty()) {
            ScalarOperator next = pending.pop();
            count++;
            for (ScalarOperator child : next.getChildren()) {
                pending.push(child);
            }
        }
        return count;
    }

    /** The size of a histogram in units: its buckets and head values. */
    static long units(Histogram histogram) {
        return histogram == null ? 0 : 1 + histogram.getBuckets().size() + histogram.getMCV().size();
    }

    // Adds a value unless the key has one, and returns the value of the key. A value that does not fit the bound is
    // returned without being kept.
    private <K, V> V put(Map<K, V> map, K key, V value, long size) {
        V existing = map.get(key);
        if (existing != null) {
            return existing;
        }
        long counted = Math.max(1, size);
        if (counted > MAX_UNITS) {
            return value;
        }
        if (units + counted > MAX_UNITS) {
            clear();
        }
        units += counted;
        map.put(key, value);
        return value;
    }

    private void clear() {
        canonical.clear();
        keys.clear();
        matches.clear();
        structuralMatches.clear();
        projections.clear();
        attachments.clear();
        parts.clear();
        scanEstimates.clear();
        mcvAttachments.clear();
        units = 0;
    }
}
