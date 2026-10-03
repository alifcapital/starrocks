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

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.statistic.StatisticUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;

public class Histogram {
    private static final Logger LOG = LogManager.getLogger(Histogram.class);

    private final List<Bucket> buckets;
    private final boolean stringValues;
    private final McvDistribution mcv;

    /**
     * Buckets carry the rows outside the MCVs. Passing none warns: row count estimation degrades to
     * the MCV rows alone.
     */
    public Histogram(List<Bucket> buckets, Map<String, Long> mcv) {
        this.stringValues = buckets != null && !buckets.isEmpty() && buckets.get(0) instanceof StringBucket;
        this.mcv = McvDistribution.copyOf(mcv);
        if (buckets != null && !buckets.isEmpty()) {
            this.buckets = List.copyOf(buckets);
        } else {
            LOG.debug("Histogram built without buckets, so its total row count covers the rows in its {} MCV "
                    + "entries only. Buckets are needed for accurate row count estimation. If the MCV row counts "
                    + "already cover every row, use Histogram(Map) instead.", this.mcv.size());
            this.buckets = List.of();
        }
    }

    /**
     * For a histogram with no buckets, where the caller has established that the MCVs cover every
     * row, or has reported that buckets could not be estimated.
     */
    public Histogram(Map<String, Long> mcv) {
        this(mcv, false);
    }

    private Histogram(Map<String, Long> mcv, boolean stringValues) {
        this.stringValues = stringValues;
        this.mcv = McvDistribution.copyOf(mcv);
        this.buckets = List.of();
    }

    public static Histogram forStrings(List<Bucket> buckets, Map<String, Long> mcv) {
        return buckets.isEmpty() ? new Histogram(mcv, true) : new Histogram(buckets, mcv);
    }

    public static Histogram ofSingleBucket(double minValue, double maxValue, double nonNullRowCount,
                                          Map<String, Long> mcv) {
        McvDistribution prepared = McvDistribution.copyOf(mcv);
        long mcvRows = prepared.getTotalRows();
        long nonMcvRows = Math.max(0L, Math.round(nonNullRowCount) - mcvRows);
        if (nonMcvRows == 0) {
            return new Histogram(prepared);
        }
        if (!Double.isFinite(minValue) || !Double.isFinite(maxValue)) {
            return new Histogram(List.of(
                    new UnknownRangeBucket(nonMcvRows)), prepared);
        }
        return new Histogram(List.of(new Bucket(minValue, maxValue, nonMcvRows, 0L)), prepared);
    }

    public boolean hasUnknownRange() {
        return buckets.stream().anyMatch(bucket -> bucket instanceof UnknownRangeBucket && bucket.getCount() > 0);
    }

    public boolean hasStringValues() {
        return stringValues;
    }

    public long getTotalRows() {
        long totalRows = 0;
        if (!buckets.isEmpty()) {
            totalRows += buckets.get(buckets.size() - 1).getCount();
        }
        totalRows += mcv.getTotalRows();
        return Math.max(1, totalRows);
    }

    @Nonnull
    public List<Bucket> getBuckets() {
        return buckets;
    }

    @Nonnull
    public Map<String, Long> getMCV() {
        return mcv;
    }

    public McvDistribution getMcvDistribution() {
        return mcv;
    }

    public String getMcvString() {
        int printMcvSize = 5;
        StringBuilder sb = new StringBuilder();
        sb.append("MCV: [");
        for (int rank = 0; rank < Math.min(printMcvSize, mcv.size()); rank++) {
            sb.append("[").append(mcv.getKeyByFrequency(rank)).append(":")
                    .append(mcv.getCountByFrequency(rank)).append("]");
        }
        sb.append("]");
        return sb.toString();
    }

    public Optional<Long> getRowCountInBucket(ConstantOperator constantOperator, double totalDistinctCount) {
        if (hasStringValues() && constantOperator.getType().isStringType() && !constantOperator.isNull()) {
            double rows = StringHistogramEstimator.pointRows(this, constantOperator.getVarchar());
            return rows > 0 ? Optional.of(Math.max(1L, Math.round(rows))) : Optional.empty();
        }
        Optional<Double> valueOpt = StatisticUtils.convertStatisticsToDouble(constantOperator.getType(),
                constantOperator.toString());
        if (valueOpt.isEmpty()) {
            return Optional.empty();
        }

        return getRowCountInBucket(valueOpt.get(), totalDistinctCount, constantOperator.getType().isFixedPointType());
    }

    public Optional<Long> getRowCountInBucket(double value, double distinctValuesCount, boolean useFixedPointEstimation) {
        int left = 0;
        int right = buckets.size() - 1;
        while (left <= right) {
            int mid = (left + right) / 2;
            Bucket bucket = buckets.get(mid);

            long prevRowCount = 0;
            if (mid > 0) {
                prevRowCount = buckets.get(mid - 1).getCount();
            }

            Optional<Long> rowCountOfBucket = bucket.getRowCountInBucket(value, prevRowCount,
                    distinctValuesCount / buckets.size(), useFixedPointEstimation);
            if (rowCountOfBucket.isPresent()) {
                return rowCountOfBucket;
            }

            if (value < bucket.getLower()) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return Optional.empty();
    }
}
