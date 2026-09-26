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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Known heavy equality tuples on the current operator, including a partially retained MCV head. */
public final class SkewJoinStatistics {
    public record Entry(List<String> values, double rows) {
        public Entry {
            values = Collections.unmodifiableList(new ArrayList<>(values));
        }
    }

    public record Distribution(double rows, List<Entry> entries, String source, double maximumOmittedRows) {
        public Distribution(double rows, List<Entry> entries, String source) {
            this(rows, entries, source, 0);
        }

        public Distribution {
            entries = List.copyOf(entries);
        }

        public boolean leavesHeavierKey(double maximumSelectedRows, double singleThreshold) {
            return maximumOmittedRows > 0 && rows > 0
                    && maximumOmittedRows / rows >= singleThreshold
                    && maximumOmittedRows >= maximumSelectedRows;
        }
    }

    private SkewJoinStatistics() { }

    public static Distribution find(Statistics stats, List<ColumnRefOperator> columns, int limit) {
        if (stats == null || columns.isEmpty() || limit <= 0
                || !Double.isFinite(stats.getOutputRowCount()) || stats.getOutputRowCount() <= 0) {
            return null;
        }
        Distribution best = null;
        for (var group : stats.getMultiColumnCombinedStats().values()) {
            if (!group.hasMcv() || !Double.isFinite(group.getRowCount()) || group.getRowCount() <= 0
                    || !group.getColumns().containsAll(columns)) {
                continue;
            }
            int[] positions = columns.stream().mapToInt(group.getColumns()::indexOf).toArray();
            Map<List<String>, Double> counts = new HashMap<>();
            for (var entry : group.getMcv()) {
                List<String> tuple = new ArrayList<>();
                for (int position : positions) {
                    tuple.add(entry.getValues().get(position));
                }
                // Counts may still be in ANALYZE units after growth or an unrelated filter.
                // Preserve the known head's proportions; do not assign the unseen tail to these keys.
                counts.merge(tuple, entry.getCount() / group.getRowCount() * stats.getOutputRowCount(), Double::sum);
            }
            var entries = counts.entrySet().stream().map(e -> new Entry(e.getKey(), e.getValue()))
                    .sorted(Comparator.comparingDouble(Entry::rows).reversed()
                            .thenComparing(e -> e.values().toString())).limit(limit).toList();
            best = better(best, new Distribution(stats.getOutputRowCount(), entries, "MCV"));
        }
        if (stats.getJoinStatisticsPlanner() != null) {
            best = better(best, stats.getJoinStatisticsPlanner().skewStatistics(
                    stats.getJoinStatisticsScope(), columns, stats.getOutputRowCount(), limit));
        }
        return best;
    }

    private static Distribution better(Distribution a, Distribution b) {
        if (b == null || b.rows() <= 0 || !Double.isFinite(b.rows())) {
            return a;
        }
        double mass = b.entries().stream().mapToDouble(Entry::rows).sum() / b.rows();
        return a == null || mass > a.entries().stream().mapToDouble(Entry::rows).sum() / a.rows() ? b : a;
    }

    /** Estimated replication of retained keys, in current input rows; unknown keys are not included. */
    public static double overlappingRows(Distribution distribution, List<ColumnRefOperator> columns,
                                         List<List<ConstantOperator>> hotKeys) {
        if (distribution == null) {
            return 0;
        }
        double rows = 0;
        for (var entry : distribution.entries()) {
            var values = constants(entry, columns);
            if (!values.isEmpty() && hotKeys.contains(values)) {
                rows += entry.rows();
            }
        }
        return rows;
    }

    public static List<ConstantOperator> constants(Entry entry, List<ColumnRefOperator> columns) {
        if (entry.values().size() != columns.size()) {
            return List.of();
        }
        List<ConstantOperator> result = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            String value = entry.values().get(i);
            var type = columns.get(i).getType();
            if (value == null) {
                result.add(ConstantOperator.createNull(type));
            } else {
                var converted = ConstantOperator.createVarchar(value).castTo(type);
                if (converted.isEmpty() || converted.get().isNull()) {
                    return List.of();
                }
                result.add(converted.get());
            }
        }
        return result;
    }
}
