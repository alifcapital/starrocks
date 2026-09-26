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

import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;

import java.util.Optional;
import java.util.OptionalDouble;

/** Immutable values for the two scopes of the shared external statistics cache. */
public sealed interface ExternalColumnStatistics permits ExternalColumnStatistics.Partition,
        ExternalColumnStatistics.Summary, ExternalPartitionStatisticsBlocks.Block, ExternalPartitionStatisticsBlocks.Directory {
    String getSourceType();

    int retainedBytes();

    final class Partition implements ExternalColumnStatistics {
        private final String sourceType;
        private final long rowCount;
        private final double dataSize;
        private final long nullCount;
        private final StatisticsHll hll;
        private final double minValue;
        private final double maxValue;

        public Partition(TStatisticData data, Type type) {
            if (!data.isSetRowCount() || !data.isSetDataSize() || !data.isSetNullCount() || !data.isSetHll()
                    || data.rowCount < 0 || data.dataSize < 0 || data.nullCount < 0) {
                throw new IllegalArgumentException("Incomplete or invalid external partition statistics");
            }
            sourceType = type.toSql();
            rowCount = data.rowCount;
            dataSize = data.dataSize;
            nullCount = data.nullCount;
            hll = StatisticsHll.fromSerialized(data.getHll());
            // Parse once on load. No string-range inference is introduced for basic statistics.
            minValue = parseBound(type, data.min).orElse(Double.POSITIVE_INFINITY);
            maxValue = parseBound(type, data.max).orElse(Double.NEGATIVE_INFINITY);
        }

        Partition(String sourceType, long rowCount, double dataSize, long nullCount, StatisticsHll hll,
                  double minValue, double maxValue) {
            this.sourceType = sourceType;
            this.rowCount = rowCount;
            this.dataSize = dataSize;
            this.nullCount = nullCount;
            this.hll = hll;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        public String getSourceType() {
            return sourceType;
        }

        public long getRowCount() {
            return rowCount;
        }

        public double getDataSize() {
            return dataSize;
        }

        public long getNullCount() {
            return nullCount;
        }

        public StatisticsHll getHll() {
            return hll;
        }

        public double getMinValue() {
            return minValue;
        }

        public double getMaxValue() {
            return maxValue;
        }

        public int retainedBytes() {
            return 96 + 2 * sourceType.length() + hll.retainedBytes();
        }
    }

    /** Prepared table-wide result. No sketch is retained or merged on the planner's warm path. */
    final class Summary implements ExternalColumnStatistics {
        public final double rowCount;
        public final long rawRowCount;
        public final String updateTime;
        public final ColumnStatistic statistic;
        public final String sourceType;
        public final int requestedPartitions;
        public final int coveredPartitions;

        public Summary(ConnectorTableColumnStats raw, ConnectorTableColumnStats estimated, String sourceType) {
            this.rowCount = estimated.getRowCount();
            this.rawRowCount = raw.getRowCount();
            this.updateTime = raw.getUpdateTime();
            this.statistic = estimated.getColumnStatistic();
            this.sourceType = sourceType;
            // Coverage of a BE aggregate is one table-wide value, not a count of loaded partitions.
            this.requestedPartitions = 1;
            this.coveredPartitions = 1;
        }

        @Override
        public String getSourceType() {
            return sourceType;
        }

        @Override
        public int retainedBytes() {
            // Includes the prepared ColumnStatistic, its empty collections, and source-type string.
            return 280 + 2 * sourceType.length() + (updateTime == null ? 0 : 40 + 2 * updateTime.length());
        }
    }

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
