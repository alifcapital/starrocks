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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Statistics of a column group: the number of distinct value tuples and, when collected, the most
 * common tuples with their row counts.
 */
public class MultiColumnCombinedStats {
    /**
     * One most common value tuple. The values are the text form of the column values in the order of
     * {@link MultiColumnCombinedStats#getColumns()}; a null element stands for a NULL column value.
     * The component counts, when collected, are the rows whose column holds the component value on
     * its own, whatever the other columns hold; for a null component, the rows whose column is NULL.
     */
    public static class McvEntry {
        private final List<String> values;
        private final long count;
        private final List<Long> componentCounts;

        public McvEntry(List<String> values, long count) {
            this(values, count, Collections.emptyList());
        }

        public McvEntry(List<String> values, long count, List<Long> componentCounts) {
            this.values = Collections.unmodifiableList(new ArrayList<>(values));
            this.count = count;
            this.componentCounts = List.copyOf(componentCounts);
        }

        public List<String> getValues() {
            return values;
        }

        public long getCount() {
            return count;
        }

        /** Empty when the counts were not collected. */
        public List<Long> getComponentCounts() {
            return componentCounts;
        }

        public boolean hasComponentCounts() {
            return !componentCounts.isEmpty() && componentCounts.size() == values.size();
        }

        @Override
        public String toString() {
            return values + ":" + count;
        }
    }

    private final long ndv;
    // Rows of the table when the MCV list was collected; MCV counts are shares of it. 0 without an MCV list.
    private final double rowCount;
    // Order of the tuple components in the MCV list, null for a component whose column the query does
    // not read. Empty without an MCV list.
    private final List<ColumnRefOperator> columns;
    private final PreparedMcvTuples mcv;
    private volatile MultiColumnMcvEstimator.ComponentShares componentShares;
    private volatile MultiColumnJoinMcvEstimator.PreparedHead joinHead;
    private RuntimeFilterStatistics.PreparedHead[] runtimeFilterHeads;
    private final List<Long> nullCounts;
    private final int readColumns;

    public MultiColumnCombinedStats(long ndv) {
        this(ndv, 0, Collections.emptyList(), Collections.emptyList());
    }

    public MultiColumnCombinedStats(long ndv, double rowCount, List<ColumnRefOperator> columns, List<McvEntry> mcv) {
        this(ndv, rowCount, columns, mcv, List.of());
    }

    public MultiColumnCombinedStats(long ndv, double rowCount, List<ColumnRefOperator> columns, List<McvEntry> mcv,
                                    List<Long> nullCounts) {
        this.ndv = ndv;
        this.rowCount = rowCount;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.mcv = PreparedMcvTuples.copyOf(mcv, columns.size(), rowCount, nullCounts);
        this.nullCounts = List.copyOf(nullCounts);
        this.readColumns = (int) columns.stream().filter(Objects::nonNull).count();
    }

    public long getNdv() {
        return ndv;
    }

    public double getRowCount() {
        return rowCount;
    }

    public List<ColumnRefOperator> getColumns() {
        return columns;
    }

    public List<McvEntry> getMcv() {
        return mcv;
    }

    public PreparedMcvTuples getMcvDistribution() {
        return mcv;
    }

    MultiColumnMcvEstimator.ComponentShares getComponentShares() {
        MultiColumnMcvEstimator.ComponentShares prepared = componentShares;
        if (prepared == null) {
            synchronized (this) {
                prepared = componentShares;
                if (prepared == null) {
                    prepared = new MultiColumnMcvEstimator.ComponentShares(this);
                    componentShares = prepared;
                }
            }
        }
        return prepared;
    }

    // One query-bound slot, not an unbounded cache of key permutations in the external catalog.
    synchronized MultiColumnJoinMcvEstimator.PreparedHead getJoinHead(List<ColumnRefOperator> key) {
        if (joinHead == null || !joinHead.matches(this, key)) {
            joinHead = new MultiColumnJoinMcvEstimator.PreparedHead(this, key);
        }
        return joinHead;
    }

    synchronized RuntimeFilterStatistics.PreparedHead getRuntimeFilterHead(int position,
                                                                           com.starrocks.type.Type type) {
        if (runtimeFilterHeads == null) {
            runtimeFilterHeads = new RuntimeFilterStatistics.PreparedHead[columns.size()];
        }
        RuntimeFilterStatistics.PreparedHead prepared = runtimeFilterHeads[position];
        if (prepared == null || !prepared.matches(type)) {
            prepared = new RuntimeFilterStatistics.PreparedHead(this, position, type);
            runtimeFilterHeads[position] = prepared;
        }
        return prepared;
    }

    public List<Long> getNullCounts() {
        return nullCounts;
    }

    public boolean hasDistribution() {
        return rowCount > 0 && readColumns > 0 && (!mcv.isEmpty() || nullCounts.size() == columns.size());
    }

    /**
     * Whether the query reads every column of the group. The combined NDV counts the tuples of the
     * whole group, so only a complete group answers NDV questions; the MCV list answers predicates
     * on the columns that are read either way.
     */
    public boolean isComplete() {
        return readColumns == columns.size();
    }

    /** Whether the MCV list can answer predicates: it exists and the query reads a column of the group. */
    public boolean hasMcv() {
        return rowCount > 0 && !mcv.isEmpty() && readColumns > 0;
    }

    @Override
    public String toString() {
        // We print MCV details only when an MCV list exists, so plans with plain multi-column NDV statistics look the
        // same with and without MCV collection.
        StringJoiner joiner = new StringJoiner(", ", "[", "]").add("ndv=" + ndv);
        if (!mcv.isEmpty()) {
            joiner.add("rowCount=" + rowCount).add("mcv=" + mcv.size());
        }
        return joiner.toString();
    }
}
