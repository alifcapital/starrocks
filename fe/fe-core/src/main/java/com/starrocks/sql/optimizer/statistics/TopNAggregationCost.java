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

import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.cost.CostEstimate;
import com.starrocks.sql.optimizer.cost.CostModel;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.statistic.StatisticUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Cost choice only: neither NDV nor histograms may justify dropping boundary peers. */
public final class TopNAggregationCost {
    private TopNAggregationCost() {
    }

    public static boolean preferFilterOnly(Statistics source, Statistics aggregate,
                                          List<ColumnRefOperator> groupKeys, List<Ordering> ordering, long limit) {
        if (source == null || aggregate == null || limit <= 0 || ordering.isEmpty()) {
            return false; // Unknown costs retain the correct WITH TIES pushdown.
        }
        double groups = distinct(source, new HashSet<>(groupKeys));
        double retained = estimateRetainedGroups(source, groups, ordering, limit);
        if (!Double.isFinite(retained)) {
            return false;
        }
        double rowBytes = Math.max(1, aggregate.getAvgRowSize());
        double keyBytes = ordering.stream().map(Ordering::getColumnRef)
                .map(source.getColumnStatistics()::get)
                .mapToDouble(c -> c == null || c.isUnknown() ? 8 : Math.max(1, c.getAverageRowSize())).sum();
        // Compare incremental local sort CPU/buffer with shuffle bytes avoided. The partial
        // aggregation and its RF are shared by both alternatives. Use the existing cost weights.
        double sortCost = CostModel.getRealCost(CostEstimate.of(
                sortCpu(groups, rowBytes, keyBytes, retained), retained * rowBytes, 0));
        double savedShuffle = CostModel.getRealCost(CostEstimate.of(0, 0, (groups - retained) * rowBytes));
        return sortCost >= savedShuffle;
    }

    /** Materialize payload rows and compare ordering keys while maintaining the retained prefix. */
    public static double sortCpu(double groups, double rowBytes, double keyBytes, double retained) {
        double comparisons = Math.log(Math.max(2, Math.min(groups, retained))) / Math.log(2);
        return groups * (Math.max(1, rowBytes) + Math.max(1, keyBytes) * comparisons);
    }

    /** Expected rank cardinality, not a correctness bound: an entire peer group can exceed K. */
    public static double estimateRetainedGroups(Statistics source, double groups, List<Ordering> ordering, long limit) {
        if (source == null || !Double.isFinite(groups) || groups <= 0 || ordering.isEmpty() || limit <= 0) {
            return Double.NaN;
        }
        double prefix = distinct(source, ordering.stream().map(Ordering::getColumnRef).collect(Collectors.toSet()));
        if (!Double.isFinite(prefix)) {
            return Double.NaN;
        }
        double peers = Math.max(1, groups / Math.max(1, prefix));
        if (ordering.size() == 1) {
            double edgePeers = estimateLeadingPeers(source, ordering.get(0), groups);
            if (Double.isFinite(edgePeers) && edgePeers >= limit) {
                peers = Math.max(peers, edgePeers);
            }
        }
        return Math.min(groups, limit + peers - 1);
    }

    /** The aggregate RF filters only the first ORDER BY key, even for a tuple ordering. */
    public static double estimateFilterSelectivity(Statistics source, List<ColumnRefOperator> groupKeys,
                                                   List<Ordering> ordering, long limit) {
        if (source == null || ordering.isEmpty()) {
            return 1;
        }
        double groups = distinct(source, new HashSet<>(groupKeys));
        Ordering first = ordering.get(0);
        ColumnStatistic column = source.getColumnStatistics().get(first.getColumnRef());
        if (column == null || column.isUnknown() || !Double.isFinite(groups)) {
            return 1;
        }
        double nulls = column.getNullsFraction();
        if (!Double.isFinite(nulls) || nulls < 0 || nulls >= 1) {
            return 1;
        }
        // The BE heap excludes NULL, while its inclusive RF always lets NULL pass,
        // including NULLS LAST. Estimate its non-NULL boundary separately from local RANK.
        Statistics nonNull = Statistics.buildFrom(source).addColumnStatistic(first.getColumnRef(),
                ColumnStatistic.buildFrom(column).setNullsFraction(0).build()).build();
        double nonNullGroups = groups * (1 - nulls);
        double retained = estimateRetainedGroups(nonNull, nonNullGroups, List.of(first), limit);
        // A large boundary peer set need not contain many INPUT rows. When the leading
        // value is expected to supply K candidates, use its histogram frequency for RF CPU
        // selectivity, independently of the conservative peer-buffer size estimate.
        double leading = estimateLeadingPeers(nonNull, first, nonNullGroups);
        if (Double.isFinite(leading) && leading >= limit) {
            retained = leading;
        }
        return Double.isFinite(retained) ? Math.min(1, nulls + (1 - nulls) * retained / nonNullGroups) : 1;
    }

    /** Conservative startup cost for the normal probe path (RuntimeFilterProbeCollector::do_evaluate). */
    public static double includeFilterWarmup(double selectivity, double rows, double drivers, int chunkSize) {
        if (!Double.isFinite(rows) || rows <= 0 || !Double.isFinite(drivers) || drivers <= 0 || chunkSize <= 0) {
            return 1;
        }
        double warmupRows = Math.min(rows, 32.0 * chunkSize * drivers);
        return (warmupRows + (rows - warmupRows) * selectivity) / rows;
    }

    /** Passing rows also compact their payload columns after the RF selection is evaluated. */
    public static double filterCompactionWork(double selectivity, double rows, double drivers, int chunkSize) {
        double startupFraction = includeFilterWarmup(0, rows, drivers, chunkSize);
        return (1 - startupFraction) * selectivity;
    }

    /** Expected occupied groups across independent local hash tables, bounded by rows and shared group NDV. */
    public static double concurrentLocalGroups(double rows, double groups, double drivers) {
        if (!Double.isFinite(rows) || !Double.isFinite(groups) || !Double.isFinite(drivers) ||
                rows <= 0 || groups <= 0 || drivers <= 1) {
            return groups;
        }
        // Occupancy estimate: a global group can occur in several drivers. Hash shuffle before
        // final aggregation removes that overlap; forced local preaggregation cannot do so.
        double capacity = groups * drivers;
        return Math.min(rows, Math.max(groups, -capacity * Math.expm1(-rows / capacity)));
    }

    static double distinct(Statistics statistics, Set<ColumnRefOperator> columns) {
        double rows = statistics.getOutputRowCount();
        if (!Double.isFinite(rows) || rows <= 0 || columns.isEmpty()) {
            return Double.NaN;
        }
        Pair<Set<ColumnRefOperator>, MultiColumnCombinedStats> combined = statistics.getLargestSubsetMCStats(columns);
        Set<ColumnRefOperator> remaining = new HashSet<>(columns);
        double ndv = 1;
        if (combined != null && combined.second.getNdv() > 0) {
            ndv = Math.min(rows, combined.second.getNdv());
            remaining.removeAll(combined.first);
        }
        for (ColumnRefOperator column : remaining) {
            ColumnStatistic stat = statistics.getColumnStatistics().get(column);
            if (stat == null || stat.isUnknown() || !Double.isFinite(stat.getDistinctValuesCount())) {
                return Double.NaN;
            }
            double values = Math.max(1, stat.getDistinctValuesCount()) + (stat.getNullsFraction() > 0 ? 1 : 0);
            ndv = Math.min(rows, ndv * values);
        }
        return Math.min(rows, ndv);
    }

    // Model groups as distributed proportionally to source rows for a known leading value.
    // This is a cost heuristic, not conditional distinct statistics. Repeated rows may overestimate
    // peers, which only selects the RF-only plan; it cannot change the query result.
    static double estimateLeadingPeers(Statistics source, Ordering ordering, double groups) {
        ColumnStatistic stat = source.getColumnStatistics().get(ordering.getColumnRef());
        if (stat == null || stat.isUnknown()) {
            return Double.NaN;
        }
        if (ordering.isNullsFirst() && stat.getNullsFraction() > 0) {
            return groups * stat.getNullsFraction();
        }
        Histogram histogram = stat.getHistogram();
        if (histogram == null) {
            return Double.NaN;
        }
        double edge = ordering.isAscending() ? stat.getMinValue() : stat.getMaxValue();
        String stringEdge = ordering.isAscending() ? stat.getMinString() : stat.getMaxString();
        double frequency = 0;
        for (Map.Entry<String, Long> entry : histogram.getMCV().entrySet()) {
            if (stringEdge != null && stringEdge.equals(entry.getKey())) {
                frequency = Math.max(frequency, entry.getValue());
            } else if (Double.isFinite(edge) && ordering.getColumnRef().getType().canStatistic()) {
                // Conversion may not be available for strings/complex types; never guess their ordering.
                var value = StatisticUtils.convertStatisticsToDouble(ordering.getColumnRef().getType(), entry.getKey());
                if (value.isPresent() && Double.compare(value.get(), edge) == 0) {
                    frequency = Math.max(frequency, entry.getValue());
                }
            }
        }
        if (Double.isFinite(edge)) {
            frequency = Math.max(frequency, histogram.getRowCountInBucket(edge,
                    stat.getDistinctValuesCount(), ordering.getColumnRef().getType().isFixedPointType()).orElse(0L));
        }
        return groups * (1 - stat.getNullsFraction()) * frequency / histogram.getTotalRows();
    }
}
