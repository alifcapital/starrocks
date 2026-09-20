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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

/**
 * Statistics of some partitions of an external table, as _statistics_.external_column_statistics
 * holds them: for each partition and column, the rows, the distinct values, the NULL rows and the
 * bounds at collection time. Summed over the partitions a scan reads, they give the rows and the
 * column shape of that scan; the table-level statistics keep the histograms and bound the distinct
 * values. The cache holds one entry per partition, so only the partitions scans ask for are loaded.
 */
public class ExternalPartitionStatistics {
    public static final ExternalPartitionStatistics EMPTY = new ExternalPartitionStatistics(Collections.emptyMap());

    public static class ColumnStats {
        private final long rowCount;
        private final long ndv;
        private final long nullCount;
        // Bounds as stored: the text of the value, empty when the partition has no non-NULL value.
        private final String min;
        private final String max;

        public ColumnStats(long rowCount, long ndv, long nullCount, String min, String max) {
            this.rowCount = rowCount;
            this.ndv = ndv;
            this.nullCount = nullCount;
            this.min = min;
            this.max = max;
        }

        public long getRowCount() {
            return rowCount;
        }

        public long getNdv() {
            return ndv;
        }

        public long getNullCount() {
            return nullCount;
        }

        public String getMin() {
            return min;
        }

        public String getMax() {
            return max;
        }
    }

    // Partition name -> column name -> statistics.
    private final Map<String, Map<String, ColumnStats>> partitions;

    public ExternalPartitionStatistics(Map<String, Map<String, ColumnStats>> partitions) {
        this.partitions = partitions;
    }

    public boolean isEmpty() {
        return partitions.isEmpty();
    }

    public Set<String> getPartitionNames() {
        return partitions.keySet();
    }

    public Map<String, ColumnStats> getPartition(String partitionName) {
        return partitions.get(partitionName);
    }

    /**
     * The table-level statistics restricted to the given partitions. Rows and NULL rows are summed
     * and the bounds merged over the partitions that have statistics; a partition without them counts
     * as an average one. The distinct values are the sum over the partitions, capped by the
     * table-level count and by the rows. Empty when no given partition has statistics.
     */
    public Optional<Statistics> aggregate(Statistics tableStatistics, Map<ColumnRefOperator, Column> columns,
                                          Collection<String> partitionNames) {
        Set<String> selected = new LinkedHashSet<>(partitionNames);
        List<Map<String, ColumnStats>> known = new ArrayList<>();
        for (String name : selected) {
            Map<String, ColumnStats> partition = partitions.get(name);
            if (partition != null && !partition.isEmpty()) {
                known.add(partition);
            }
        }
        if (known.isEmpty()) {
            return Optional.empty();
        }
        double rows = 0;
        for (Map<String, ColumnStats> partition : known) {
            long partitionRows = 0;
            for (ColumnStats stats : partition.values()) {
                partitionRows = Math.max(partitionRows, stats.getRowCount());
            }
            rows += partitionRows;
        }
        rows *= (double) selected.size() / known.size();

        Statistics.Builder builder = Statistics.buildFrom(tableStatistics).setOutputRowCount(rows);
        for (Map.Entry<ColumnRefOperator, Column> entry : columns.entrySet()) {
            ColumnStatistic base = tableStatistics.getColumnStatistics().get(entry.getKey());
            if (base == null || base.isUnknown()) {
                continue;
            }
            String columnName = entry.getValue().getName();
            Type type = entry.getValue().getType();
            int found = 0;
            double nulls = 0;
            double ndv = 0;
            double min = Double.POSITIVE_INFINITY;
            double max = Double.NEGATIVE_INFINITY;
            for (Map<String, ColumnStats> partition : known) {
                ColumnStats stats = partition.get(columnName);
                if (stats == null) {
                    continue;
                }
                found++;
                nulls += stats.getNullCount();
                ndv += stats.getNdv();
                OptionalDouble low = parseBound(type, stats.getMin());
                OptionalDouble high = parseBound(type, stats.getMax());
                if (low.isPresent() && high.isPresent()) {
                    min = Math.min(min, low.getAsDouble());
                    max = Math.max(max, high.getAsDouble());
                }
            }
            if (found == 0) {
                continue;
            }
            double scale = (double) selected.size() / found;
            double distinct = Math.min(base.getDistinctValuesCount(), ndv * scale);
            ColumnStatistic.Builder column = ColumnStatistic.buildFrom(base)
                    .setDistinctValuesCount(Math.max(1, Math.min(distinct, Math.max(1, rows))));
            if (rows > 0) {
                column.setNullsFraction(Math.min(1.0, nulls * scale / rows));
            }
            if (min <= max) {
                column.setMinValue(min).setMaxValue(max);
            }
            builder.addColumnStatistic(entry.getKey(), column.build());
        }
        return Optional.of(builder.build());
    }

    // A stored bound as a number; empty for strings, for an empty text and for a text that does not parse.
    static OptionalDouble parseBound(Type type, String text) {
        if (text == null || text.isEmpty() || !type.canStatistic() || type.getPrimitiveType().isCharFamily()) {
            return OptionalDouble.empty();
        }
        try {
            Optional<Double> value = StatisticUtils.convertStatisticsToDouble(type, text);
            return value.map(OptionalDouble::of).orElse(OptionalDouble.empty());
        } catch (RuntimeException e) {
            return OptionalDouble.empty();
        }
    }
}
