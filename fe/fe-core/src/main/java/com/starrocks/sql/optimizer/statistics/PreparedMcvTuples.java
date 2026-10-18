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

import com.starrocks.type.Type;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.Set;

/**
 * Owned tuple distribution shared by the external cache and its planner bindings. Entries, including
 * duplicates and NULL components, retain their encounter order. Derived distributions own a new
 * snapshot. The list interface keeps dump serialization and existing tuple consumers compatible.
 */
public final class PreparedMcvTuples extends AbstractList<MultiColumnCombinedStats.McvEntry> implements RandomAccess {
    private static final PreparedMcvTuples EMPTY = new PreparedMcvTuples(List.of(), 0, 0, List.of(), null);

    private final List<MultiColumnCombinedStats.McvEntry> entries;
    private final int width;
    private final List<Long> nullCounts;
    private final double rowCount;
    private final long totalRowsLong;
    private final double totalRows;
    private final double sequentialTotalRows;
    private final double[] shares;
    private final double totalShare;
    private final double minShare;
    private final double[] nullRows;
    private final boolean[] hasNull;
    private volatile List<Map<String, Long>> componentCounts;
    // The head whose converted values this head reads: this head, or the head whose values it holds at the same
    // positions, so the derived heads of one query read the values the shared head converted once.
    private final PreparedMcvTuples typedOwner;
    // Per position, the distinct values of the position converted to one type. Owned by typedOwner only.
    private volatile TypedSlot[] typedSlots;

    private record TypedSlot(Type type, Map<String, McvPredicateEvaluator.TypedValue> values) {
    }

    private PreparedMcvTuples(List<MultiColumnCombinedStats.McvEntry> source, int width,
                              double rowCount, List<Long> nullCounts, PreparedMcvTuples valuesOf) {
        this.typedOwner = valuesOf != null && valuesOf.width == width ? valuesOf.typedOwner : this;
        this.entries = List.copyOf(source);
        this.width = width;
        this.rowCount = rowCount;
        this.nullCounts = List.copyOf(nullCounts);
        this.shares = new double[entries.size()];
        this.nullRows = new double[width];
        this.hasNull = new boolean[width];
        DoubleSummaryStatistics sum = new DoubleSummaryStatistics();
        DoubleSummaryStatistics[] nullSums = new DoubleSummaryStatistics[width];
        for (int i = 0; i < width; i++) {
            nullSums[i] = new DoubleSummaryStatistics();
        }
        long longSum = 0;
        double sequentialSum = 0;
        double shareSum = 0;
        double minimum = 1;
        for (int t = 0; t < entries.size(); t++) {
            MultiColumnCombinedStats.McvEntry entry = entries.get(t);
            long count = entry.getCount();
            longSum += count;
            sequentialSum += count;
            sum.accept(count);
            double share = count / rowCount;
            shares[t] = share;
            shareSum += share;
            minimum = Math.min(minimum, share);
            for (int i = 0; i < Math.min(width, entry.getValues().size()); i++) {
                if (entry.getValues().get(i) == null) {
                    hasNull[i] = true;
                    nullSums[i].accept(count);
                }
            }
        }
        totalRowsLong = longSum;
        totalRows = sum.getSum();
        sequentialTotalRows = sequentialSum;
        totalShare = shareSum;
        minShare = minimum;
        for (int i = 0; i < width; i++) {
            nullRows[i] = nullSums[i].getSum();
        }
    }

    public static PreparedMcvTuples copyOf(List<MultiColumnCombinedStats.McvEntry> source, int width,
                                           double rowCount, List<Long> nullCounts) {
        return copyOf(source, width, rowCount, nullCounts, null);
    }

    /**
     * @param valuesOf a head whose values the source holds at the same positions, such as the head a filter keeps
     *                 tuples of; null when the values are new
     */
    static PreparedMcvTuples copyOf(List<MultiColumnCombinedStats.McvEntry> source, int width,
                                    double rowCount, List<Long> nullCounts, PreparedMcvTuples valuesOf) {
        if (source.isEmpty() && width == 0 && rowCount == 0 && nullCounts.isEmpty()) {
            return EMPTY;
        }
        if (source instanceof PreparedMcvTuples prepared) {
            if (prepared.width == width
                    && Double.doubleToLongBits(prepared.rowCount) == Double.doubleToLongBits(rowCount)
                    && prepared.nullCounts.equals(nullCounts)) {
                return prepared;
            }
            valuesOf = prepared;
        }
        return new PreparedMcvTuples(source, width, rowCount, nullCounts, valuesOf);
    }

    @Override
    public MultiColumnCombinedStats.McvEntry get(int index) {
        return entries.get(index);
    }

    @Override
    public int size() {
        return entries.size();
    }

    // Preserve the original reduction semantics: long overflow, compensated stream sum, and
    // sequential double addition are intentionally distinct for large counts.
    public long getTotalRowsLong() {
        return totalRowsLong;
    }

    public double getTotalRows() {
        return totalRows;
    }

    public double getSequentialTotalRows() {
        return sequentialTotalRows;
    }

    public double getShare(int tuple) {
        return shares[tuple];
    }

    public double getTotalShare() {
        return totalShare;
    }

    public double getMinShare() {
        return minShare;
    }

    public double getNullRows(int position) {
        return nullRows[position];
    }

    public boolean hasNull(int position) {
        return hasNull[position];
    }

    /** Same first-component-count wins and explicit NULL-count override as the estimator. */
    public List<Map<String, Long>> getComponentCounts() {
        List<Map<String, Long>> prepared = componentCounts;
        if (prepared == null) {
            synchronized (this) {
                prepared = componentCounts;
                if (prepared == null) {
                    List<Map<String, Long>> result = new ArrayList<>(width);
                    for (int i = 0; i < width; i++) {
                        result.add(new HashMap<>());
                    }
                    for (MultiColumnCombinedStats.McvEntry entry : entries) {
                        if (entry.hasComponentCounts()) {
                            for (int i = 0; i < width; i++) {
                                result.get(i).putIfAbsent(entry.getValues().get(i), entry.getComponentCounts().get(i));
                            }
                        }
                    }
                    for (int i = 0; i < width; i++) {
                        if (nullCounts.size() == width) {
                            result.get(i).put(null, nullCounts.get(i));
                        }
                        result.set(i, Collections.unmodifiableMap(result.get(i)));
                    }
                    prepared = List.copyOf(result);
                    componentCounts = prepared;
                }
            }
        }
        return prepared;
    }

    /**
     * A stored value of the position converted to the type, see McvPredicateEvaluator.TypedValue. We expect the
     * conditions of every derivation of every query to read the same head values, so each position keeps its
     * distinct values converted to the type last asked for, shared by the queries; a value the head does not hold
     * is converted on each call.
     */
    McvPredicateEvaluator.TypedValue typedValue(int position, Type type, String value) {
        Map<String, McvPredicateEvaluator.TypedValue> values = typedOwner.typedValues(position, type);
        McvPredicateEvaluator.TypedValue typed = values == null ? null : values.get(value);
        return typed != null ? typed : McvPredicateEvaluator.TypedValue.of(type, value);
    }

    private Map<String, McvPredicateEvaluator.TypedValue> typedValues(int position, Type type) {
        if (position < 0 || position >= width) {
            return null;
        }
        TypedSlot[] slots = typedSlots;
        TypedSlot slot = slots == null ? null : slots[position];
        if (slot != null && slot.type().equals(type)) {
            return slot.values();
        }
        synchronized (this) {
            slots = typedSlots;
            slot = slots == null ? null : slots[position];
            if (slot != null && slot.type().equals(type)) {
                return slot.values();
            }
            Map<String, McvPredicateEvaluator.TypedValue> values = new HashMap<>();
            for (MultiColumnCombinedStats.McvEntry entry : entries) {
                String value = position < entry.getValues().size() ? entry.getValues().get(position) : null;
                if (value != null && !values.containsKey(value)) {
                    values.put(value, McvPredicateEvaluator.TypedValue.of(type, value));
                }
            }
            // The slots array is replaced, never written after it is published.
            TypedSlot[] updated = slots == null ? new TypedSlot[width] : slots.clone();
            updated[position] = new TypedSlot(type.clone(), Collections.unmodifiableMap(values));
            typedSlots = updated;
            return updated[position].values();
        }
    }

    /** Additional storage, excluding tuple entries already weighed by ExternalMcvStatistics. */
    public long retainedIndexBytes() {
        // Reserve all component maps and converted values before publication: Caffeine does not reweigh lazy
        // values. Keys and boxed component counts are shared with the immutable entries/nullCounts.
        long typed = 96L * width;
        for (int i = 0; i < width; i++) {
            Set<String> values = new HashSet<>();
            for (MultiColumnCombinedStats.McvEntry entry : entries) {
                if (i < entry.getValues().size() && entry.getValues().get(i) != null
                        && values.add(entry.getValues().get(i))) {
                    typed += McvPredicateEvaluator.TypedValue.retainedBytes(entry.getValues().get(i));
                }
            }
        }
        return 256L + 8L * size() + 192L * width + 64L * width * size() + typed;
    }
}
