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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** A compact immutable result for the exact requested scan domain. */
public final class ExternalStatisticsAggregate {
    public final double rowCount;
    public final Map<String, ColumnStatistic> columns;
    public final Map<String, Integer> coveredPartitions;
    public final Map<String, String> sourceTypes;
    public final int requestedPartitions;
    public final int knownPartitions;
    private final boolean completeCoverage;
    private final boolean unknownColumns;

    private ExternalStatisticsAggregate(double rowCount, Map<String, ColumnStatistic> columns,
                                        Map<String, Integer> coveredPartitions, Map<String, String> sourceTypes,
                                        int requestedPartitions, int knownPartitions) {
        this(rowCount, columns, coveredPartitions, sourceTypes, requestedPartitions, knownPartitions,
                coveredPartitions.values().stream().allMatch(count -> count == requestedPartitions),
                columns.values().stream().anyMatch(ColumnStatistic::isUnknown));
    }

    private ExternalStatisticsAggregate(double rowCount, Map<String, ColumnStatistic> columns,
                                        Map<String, Integer> coveredPartitions, Map<String, String> sourceTypes,
                                        int requestedPartitions, int knownPartitions,
                                        boolean completeCoverage, boolean unknownColumns) {
        this.completeCoverage = completeCoverage;
        this.unknownColumns = unknownColumns;
        this.rowCount = rowCount;
        this.columns = Map.copyOf(columns);
        this.coveredPartitions = Map.copyOf(coveredPartitions);
        this.sourceTypes = Map.copyOf(sourceTypes);
        this.requestedPartitions = requestedPartitions;
        this.knownPartitions = knownPartitions;
    }

    public boolean isEmpty() {
        return knownPartitions == 0;
    }

    public boolean hasCompleteCoverage() {
        return completeCoverage;
    }

    public boolean hasUnknownColumns() {
        return unknownColumns;
    }

    public static ExternalStatisticsAggregate fromTableRow(ExternalStatisticsRequest request,
                                                           ExternalTableStatistics table) {
        if (table == null) {
            return new ExternalStatisticsAggregate(0, Map.of(), Map.of(), Map.of(), 1, 0, false, true);
        }
        double rows = 0;
        boolean missing = false;
        boolean unknown = false;
        boolean known = false;
        for (String column : request.columns) {
            ExternalColumnStatistics.Summary summary = table.summaries.get(column);
            if (summary == null) {
                missing = true;
                unknown = true;
            } else {
                rows = Math.max(rows, summary.rowCount);
                known = true;
                unknown |= summary.statistic.isUnknown();
            }
        }
        // Map.copyOf preserves these already immutable maps; no per-column repacking on the planner path.
        return new ExternalStatisticsAggregate(rows, table.columns, table.coverage, table.sourceTypes,
                1, known ? 1 : 0, !missing, unknown);
    }

    public static final class Builder {
        private final ExternalStatisticsRequest request;
        private final Map<String, Long> partitionRows = new HashMap<>();
        private final Map<String, ColumnAggregate> columns = new HashMap<>();

        public Builder(ExternalStatisticsRequest request) {
            this.request = request;
            request.columns.forEach(column -> columns.put(column, new ColumnAggregate()));
        }

        // Different bounded load lanes may finish simultaneously; each union has one writer at a time.
        public synchronized void add(Map<ExternalStatisticsCacheKey,
                Optional<ExternalColumnStatistics>> batch) {
            batch.forEach((key, optional) -> optional.ifPresent(cached -> {
                if (cached instanceof ExternalPartitionStatisticsBlocks.Block block) {
                    int coverage = 0;
                    for (int i = 0; i < block.key.partitions.size(); i++) {
                        if (block.rows(i) >= 0) {
                            coverage++;
                            partitionRows.merge(block.key.partitions.get(i), block.rows(i), Math::max);
                        }
                    }
                    if (coverage > 0) {
                        columns.get(key.columnName).add(block.summary, coverage);
                    }
                } else if (cached instanceof ExternalColumnStatistics.Partition value) {
                    partitionRows.merge(key.partitionName, value.getRowCount(), Math::max);
                    columns.get(key.columnName).add(value, 1);
                } else {
                    throw new IllegalArgumentException("Expected partition HLL statistics: " + key);
                }
            }));
        }

        public synchronized ExternalStatisticsAggregate build() {
            double rows = partitionRows.values().stream().mapToDouble(Long::doubleValue).sum();
            if (!partitionRows.isEmpty()) {
                rows *= (double) request.partitions.size() / partitionRows.size();
            }
            Map<String, ColumnStatistic> result = new HashMap<>();
            Map<String, Integer> coverage = new HashMap<>();
            Map<String, String> types = new HashMap<>();
            for (Map.Entry<String, ColumnAggregate> entry : columns.entrySet()) {
                ColumnAggregate column = entry.getValue();
                coverage.put(entry.getKey(), column.count);
                if (column.sourceType != null) {
                    types.put(entry.getKey(), column.sourceType);
                }
                if (column.count == 0) {
                    result.put(entry.getKey(), ColumnStatistic.unknown());
                    continue;
                }
                double ndv = column.hll.estimate() * ((double) request.partitions.size() / column.count);
                // Missing coverage is explicitly retained above. Extrapolation is an estimate,
                // never evidence of an empty partition or a global distribution for this column.
                ColumnStatistic statistic = ColumnStatistic.builder()
                        .setMinValue(column.min <= column.max ? column.min : Double.NEGATIVE_INFINITY)
                        .setMaxValue(column.min <= column.max ? column.max : Double.POSITIVE_INFINITY)
                        .setDistinctValuesCount(Math.max(1, Math.min(ndv, Math.max(1, rows))))
                        .setAverageRowSize(column.dataSize / Math.max(1, column.rows))
                        .setNullsFraction(Math.min(1, column.nulls / Math.max(1, column.rows)))
                        .build();
                result.put(entry.getKey(), statistic);
            }
            return new ExternalStatisticsAggregate(rows, result, coverage, types,
                    request.partitions.size(), partitionRows.size());
        }
    }

    private static final class ColumnAggregate {
        private final StatisticsHll.Union hll = new StatisticsHll.Union();
        private String sourceType;
        private int count;
        private double rows;
        private double dataSize;
        private double nulls;
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;

        private void add(ExternalColumnStatistics.Partition stats, int coverage) {
            sourceType = sourceType == null ? stats.getSourceType() :
                    sourceType.equals(stats.getSourceType()) ? sourceType : "";
            count += coverage;
            rows += stats.getRowCount();
            dataSize += stats.getDataSize();
            nulls += stats.getNullCount();
            min = Math.min(min, stats.getMinValue());
            max = Math.max(max, stats.getMaxValue());
            hll.merge(stats.getHll());
        }
    }
}
