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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.cost.CostEstimate;
import com.starrocks.sql.optimizer.cost.CostModel;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.statistic.StatisticUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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
        double retained = estimateRetainedGroups(groupDistribution(source, groupKeys, groups), groups, ordering, limit);
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
        RankEstimate exact = mcvRank(source, groups, ordering, limit, false);
        if (exact != null) {
            return exact.retained();
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
        if (!Double.isFinite(groups)) {
            return 1;
        }
        // Row frequencies drive RF savings; distinct group frequencies drive the boundary.
        // A million duplicates of one tuple still supply only one aggregate group to the heap.
        Statistics grouped = groupDistribution(source, groupKeys, groups);
        // A complete group dictionary locates the boundary, but RF selectivity must be evaluated
        // on INPUT rows with their original multiplicities. NULLs pass the BE filter in either order.
        RankEstimate boundary = mcvRank(grouped, groups, List.of(first), limit, true);
        if (boundary != null) {
            if (boundary.first() == null) {
                return 1;
            }
            var range = new BinaryPredicateOperator(first.isAscending() ? BinaryType.LE : BinaryType.GE,
                    first.getColumnRef(), boundary.first());
            var predicate = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, range,
                    new IsNullPredicateOperator(false, first.getColumnRef()));
            return Math.min(1, PredicateStatisticsCalculator.statisticsCalculate(predicate, source)
                    .getOutputRowCount() / source.getOutputRowCount());
        }
        if (column == null || column.isUnknown()) {
            return 1;
        }
        double nulls = column.getNullsFraction();
        if (!Double.isFinite(nulls) || nulls < 0 || nulls >= 1) {
            return 1;
        }
        ColumnStatistic groupColumn = grouped.getColumnStatistics().get(first.getColumnRef());
        double groupNulls = groupColumn.getNullsFraction();
        if (!Double.isFinite(groupNulls) || groupNulls < 0 || groupNulls >= 1) {
            return 1;
        }
        // The BE heap excludes NULL, while its inclusive RF always lets NULL pass.
        Statistics nonNull = Statistics.buildFrom(grouped).addColumnStatistic(first.getColumnRef(),
                ColumnStatistic.buildFrom(groupColumn).setNullsFraction(0).build()).build();
        double nonNullGroups = groups * (1 - groupNulls);
        double retained = estimateRetainedGroups(nonNull, nonNullGroups, List.of(first), limit);
        double leadingGroups = estimateLeadingPeers(nonNull, first, nonNullGroups);
        // Distinct retained GROUP BY tuples give a lower bound on boundary peers, even with a tail.
        // If that alone fills K, a saved input marginal can still estimate RF row selectivity.
        double knownGroups = leadingMcvShare(nonNull, first,
                nonNull.getColumnStatistic(first.getColumnRef()), true) * nonNullGroups;
        if (Double.isFinite(knownGroups)) {
            leadingGroups = Double.isFinite(leadingGroups) ? Math.max(leadingGroups, knownGroups) : knownGroups;
        }
        if (Double.isFinite(leadingGroups) && leadingGroups >= limit) {
            double leadingRows = estimateLeadingPeers(source, new Ordering(first.getColumnRef(),
                    first.isAscending(), false), source.getOutputRowCount());
            if (Double.isFinite(leadingRows)) {
                return Math.min(1, nulls + leadingRows / source.getOutputRowCount());
            }
        }
        return Double.isFinite(retained) ? Math.min(1, nulls + (1 - nulls) * retained / nonNullGroups) : 1;
    }

    /** Union of GROUP BY tuples, before the cost model accounts for copies across local drivers. */
    static Statistics groupDistribution(Statistics input, List<ColumnRefOperator> keys, double groups) {
        if (!Double.isFinite(groups) || groups <= 0) {
            return input;
        }
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> ndvs = new HashMap<>();
        input.getMultiColumnCombinedStats().forEach((columns, stats) -> {
            if (stats.getColumns().isEmpty() && keys.containsAll(columns)) {
                ndvs.put(columns, stats);
            }
        });
        Statistics output = Statistics.buildFrom(input).setOutputRowCount(groups)
                .setMultiColumnStatistics(ndvs).build();
        return McvAggregateStatistics.derive(keys, input, output);
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
        // Use the same conditional/projected NDV as GROUP BY, including an MCV superset.
        // Never multiply base-table NDVs again on top of an already estimated JOIN cardinality.
        var combined = statistics.getLargestSubsetMCStats(columns);
        var projected = MultiColumnMcvEstimator.projectedNdv(columns, statistics);
        if (projected.isEmpty() && statistics.getJoinStatisticsPlanner() != null) {
            Statistics.Builder conditional = null;
            for (ColumnRefOperator column : columns) {
                if (combined != null && combined.first.contains(column)) {
                    continue;
                }
                var key = statistics.getJoinStatisticsPlanner().keyStatistics(statistics.getJoinStatisticsScope(), column);
                var basic = statistics.getColumnStatistics().get(column);
                if (key == null || key.degree() == null || basic == null) {
                    continue;
                }
                if (conditional == null) {
                    conditional = Statistics.buildFrom(statistics);
                }
                var degree = key.degree();
                double nulls = degree.getRowCount() > 0 ? degree.getNullCount() / (double) degree.getRowCount() : 0;
                conditional.addColumnStatistic(column, ColumnStatistic.buildFrom(basic)
                        .setDistinctValuesCount(degree.getDistinctCount()).setNullsFraction(nulls)
                        .setType(ColumnStatistic.StatisticType.ESTIMATE).build());
            }
            if (conditional != null) {
                statistics = conditional.build();
            }
        }
        if (projected.isEmpty()) {
            for (ColumnRefOperator column : columns) {
                if (combined != null && combined.first.contains(column)) {
                    continue;
                }
                ColumnStatistic stat = statistics.getColumnStatistics().get(column);
                if (stat == null || stat.isUnknown() || !Double.isFinite(stat.getDistinctValuesCount())) {
                    return Double.NaN;
                }
            }
        }
        if (!statistics.getColumnStatistics().keySet().containsAll(columns)) {
            return Double.NaN;
        }
        return StatisticsCalculator.computeGroupByStatistics(
                columns.stream().sorted(Comparator.comparingInt(ColumnRefOperator::getId)).toList(),
                statistics, new HashMap<>());
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
        double mcv = leadingMcvShare(source, ordering, stat, false);
        if (Double.isFinite(mcv)) {
            return groups * mcv;
        }
        Histogram histogram = stat.getHistogram();
        if (histogram == null || histogram.getTotalRows() <= 0) {
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
    private record Peer(List<ConstantOperator> values, double groups) { }

    /** A complete distribution of output groups locates the rank boundary in that statistics snapshot. */
    private record RankEstimate(double retained, ConstantOperator first) { }

    private static RankEstimate mcvRank(Statistics source, double groups, List<Ordering> ordering,
                                        long limit, boolean skipNull) {
        if (!MultiColumnMcvEstimator.isEnabled()) {
            return null;
        }
        for (var distribution : source.getMultiColumnCombinedStats().values()) {
            if (!distribution.hasMcv() || distribution.getRowCount() != groups
                    || distribution.getMcv().stream().mapToDouble(MultiColumnCombinedStats.McvEntry::getCount).sum()
                    != distribution.getRowCount()) {
                continue;
            }
            int[] positions = ordering.stream().mapToInt(o -> distribution.getColumns().indexOf(o.getColumnRef()))
                    .toArray();
            if (java.util.Arrays.stream(positions).anyMatch(i -> i < 0)) {
                continue;
            }
            List<Peer> peers = new ArrayList<>();
            for (var entry : distribution.getMcv()) {
                List<ConstantOperator> values = new ArrayList<>();
                for (int i = 0; i < positions.length; i++) {
                    String text = entry.getValues().get(positions[i]);
                    var type = ordering.get(i).getColumnRef().getType();
                    if (!type.isNumericType() && !type.isStringType() && !type.isDateType() && !type.isBoolean()) {
                        return null;
                    }
                    var value = text == null ? ConstantOperator.createNull(type)
                            : ConstantOperator.createVarchar(text).castTo(type).orElse(null);
                    if (value == null || (text != null && value.isNull())) {
                        return null;
                    }
                    values.add(value);
                }
                if (!skipNull || !values.get(0).isNull()) {
                    peers.add(new Peer(values, entry.getCount()));
                }
            }
            Comparator<Peer> comparator = (a, b) -> {
                for (int i = 0; i < ordering.size(); i++) {
                    var x = a.values().get(i);
                    var y = b.values().get(i);
                    var order = ordering.get(i);
                    int comparison;
                    if (x.isNull() || y.isNull()) {
                        comparison = x.isNull() == y.isNull() ? 0 : x.isNull() == order.isNullsFirst() ? -1 : 1;
                    } else {
                        comparison = x.getType().isStringType() ? StringBucket.compare(x.getVarchar(), y.getVarchar())
                                : x.compareTo(y);
                        if (!order.isAscending()) {
                            comparison = -Integer.signum(comparison);
                        }
                    }
                    if (comparison != 0) {
                        return comparison;
                    }
                }
                return 0;
            };
            peers.sort(comparator);
            double retained = 0;
            Peer boundary = null;
            for (Peer peer : peers) {
                if (retained >= limit && comparator.compare(boundary, peer) != 0) {
                    break;
                }
                retained += peer.groups();
                boundary = peer;
            }
            return new RankEstimate(Math.min(groups, retained), boundary == null ? null : boundary.values().get(0));
        }
        return null;
    }

    // A component count describes the whole marginal, even when the joint head is partial.
    // Summing only retained tuples is valid for a complete head or for a singleton distribution.
    private static double leadingMcvShare(Statistics source, Ordering ordering, ColumnStatistic stat,
                                          boolean allowLowerBound) {
        if (!MultiColumnMcvEstimator.isEnabled()) {
            return Double.NaN;
        }
        double best = Double.NaN;
        int bestWidth = Integer.MAX_VALUE;
        for (var group : source.getMultiColumnCombinedStats().values()) {
            int position = group.getColumns().indexOf(ordering.getColumnRef());
            if (!group.hasMcv() || position < 0 || group.getColumns().size() > bestWidth) {
                continue;
            }
            double covered = 0;
            double matched = 0;
            double marginal = Double.NaN;
            boolean found = false;
            for (var entry : group.getMcv()) {
                covered += entry.getCount();
                String value = entry.getValues().get(position);
                if (!matchesEdge(value, ordering, stat)) {
                    continue;
                }
                found = true;
                matched += entry.getCount();
                if (entry.hasComponentCounts()) {
                    marginal = entry.getComponentCounts().get(position);
                }
            }
            if (!found) {
                continue; // Missing from an MCV head does not mean absent from the distribution.
            }
            double frequency = Double.isFinite(marginal) ? marginal
                    : allowLowerBound || group.getColumns().size() == 1 || covered >= group.getRowCount()
                    ? matched : Double.NaN;
            if (Double.isFinite(frequency)) {
                double nullRows = group.getNullCounts().size() == group.getColumns().size()
                        ? group.getNullCounts().get(position) : 0;
                best = nullRows > 0 && group.getRowCount() > nullRows
                        ? (1 - stat.getNullsFraction()) * frequency / (group.getRowCount() - nullRows)
                        : frequency / group.getRowCount();
                best = Math.min(1, best);
                bestWidth = group.getColumns().size();
            }
        }
        return best;
    }

    private static boolean matchesEdge(String value, Ordering ordering, ColumnStatistic stat) {
        if (value == null) {
            return false;
        }
        String text = ordering.isAscending() ? stat.getMinString() : stat.getMaxString();
        if (text != null) {
            return text.equals(value);
        }
        double edge = ordering.isAscending() ? stat.getMinValue() : stat.getMaxValue();
        if (!Double.isFinite(edge) || !ordering.getColumnRef().getType().canStatistic()) {
            return false;
        }
        var converted = StatisticUtils.convertStatisticsToDouble(ordering.getColumnRef().getType(), value);
        return converted.isPresent() && Double.compare(converted.get(), edge) == 0;
    }

}
