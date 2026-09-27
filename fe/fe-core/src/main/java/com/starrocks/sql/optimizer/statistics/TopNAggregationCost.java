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
        double prefix = distinct(source, ordering.stream().map(Ordering::getColumnRef).collect(Collectors.toSet()));
        if (!Double.isFinite(groups) || !Double.isFinite(prefix)) {
            return false;
        }
        double peers = Math.max(1, groups / Math.max(1, prefix));
        // A univariate histogram cannot describe conditional NDV for a multi-column ORDER BY.
        if (ordering.size() == 1) {
            double edgePeers = estimateLeadingPeers(source, ordering.get(0), groups);
            if (Double.isFinite(edgePeers) && edgePeers >= limit) {
                peers = Math.max(peers, edgePeers);
            }
        }
        double retained = Math.min(groups, limit + peers - 1);
        double rowBytes = Math.max(1, aggregate.getAvgRowSize());
        double keyBytes = ordering.stream().map(Ordering::getColumnRef)
                .map(source.getColumnStatistics()::get)
                .mapToDouble(c -> c == null || c.isUnknown() ? 8 : Math.max(1, c.getAverageRowSize())).sum();
        // Compare incremental local sort CPU/buffer with shuffle bytes avoided. The partial
        // aggregation and its RF are shared by both alternatives. Use the existing cost weights.
        double sortCost = CostModel.getRealCost(CostEstimate.of(groups * keyBytes, retained * rowBytes, 0));
        double savedShuffle = CostModel.getRealCost(CostEstimate.of(0, 0, (groups - retained) * rowBytes));
        return sortCost >= savedShuffle;
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
