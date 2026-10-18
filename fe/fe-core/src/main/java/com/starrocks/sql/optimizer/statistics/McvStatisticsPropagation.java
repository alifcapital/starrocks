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

import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

/** Derives distributions for a filtered relation; collected table distributions remain immutable. */
final class McvStatisticsPropagation {
    private McvStatisticsPropagation() {
    }

    private enum Truth { TRUE, FALSE, UNKNOWN }

    static Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> project(
            Map<ColumnRefOperator, ? extends ScalarOperator> projection, Statistics input) {
        if (input.getMultiColumnCombinedStats().isEmpty()) {
            return Map.of();
        }
        Map<ColumnRefOperator, ColumnRefOperator> aliases = new HashMap<>();
        projection.forEach((output, expression) -> {
            if (expression instanceof ColumnRefOperator) {
                aliases.putIfAbsent((ColumnRefOperator) expression, output);
            }
        });
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> result = new HashMap<>();
        input.getMultiColumnCombinedStats().forEach((key, group) -> {
            Set<ColumnRefOperator> mapped = new HashSet<>();
            for (ColumnRefOperator column : key) {
                if (aliases.containsKey(column)) {
                    mapped.add(aliases.get(column));
                }
            }
            if (mapped.isEmpty()) {
                return;
            }
            if (group.getColumns().isEmpty()) {
                if (mapped.size() == key.size()) {
                    result.put(mapped, group);
                }
            } else {
                List<ColumnRefOperator> columns = group.getColumns().stream().map(aliases::get).toList();
                MultiColumnCombinedStats projected = new MultiColumnCombinedStats(group.getNdv(),
                        group.getRowCount(), columns, group.getMcv(), group.getNullCounts());
                result.merge(mapped, projected, StatisticsCalcUtils::preferMultiColumnStats);
            }
        });
        for (MultiColumnCombinedStats group : input.getMultiColumnCombinedStats().values()) {
            MultiColumnCombinedStats converted = McvCastStatistics.projectGroup(projection, group);
            if (converted != null) {
                Set<ColumnRefOperator> columns = new HashSet<>(converted.getColumns());
                columns.remove(null);
                result.merge(columns, converted, StatisticsCalcUtils::preferMultiColumnStats);
            }
        }
        projection.forEach((output, expression) -> {
            if (expression instanceof CastOperator) {
                MultiColumnCombinedStats cast = McvCastStatistics.derive(output, expression, input);
                if (cast != null) {
                    result.merge(Set.of(output), cast, StatisticsCalcUtils::preferMultiColumnStats);
                }
            }
        });
        return result;
    }

    static Statistics afterJoin(Statistics input, Statistics statistics, boolean inner) {
        if (statistics.getMultiColumnCombinedStats().isEmpty()) {
            return statistics;
        }
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups = new HashMap<>();
        Statistics.Builder builder = Statistics.buildFrom(statistics);
        statistics.getMultiColumnCombinedStats().forEach((columns, group) -> {
            // Native combined NDV has no frequency distribution. Keep its existing semantics.
            // MCV frequencies would need reweighting by the other input's matches (and by the
            // unmatched population for outer/anti joins), which an input distribution cannot supply.
            if (group.getColumns().isEmpty()) {
                groups.put(columns, group);
            } else {
                for (ColumnRefOperator column : columns) {
                    ColumnStatistic output = statistics.getColumnStatistics().get(column);
                    ColumnStatistic original = input.getColumnStatistics().get(column);
                    if (output != null && original != null
                            && (!inner || output.getHistogram() == original.getHistogram())) {
                        builder.addColumnStatistic(column, ColumnStatistic.buildFrom(output).setHistogram(null).build());
                    }
                }
            }
        });
        return builder.setMultiColumnStatistics(groups).build();
    }

    /** A complete head is a finite distribution, including SQL's UNKNOWN truth value for NULLs. */
    static OptionalDouble completeHeadRows(ScalarOperator predicate, Statistics statistics) {
        if (statistics.getMultiColumnCombinedStats().isEmpty()
                || !MultiColumnMcvEstimator.isEnabled() || predicate.isNotEvalEstimate()) {
            return OptionalDouble.empty();
        }
        Set<ColumnRefOperator> used = new HashSet<>(Utils.extractColumnRef(predicate));
        MultiColumnCombinedStats best = null;
        for (MultiColumnCombinedStats group : statistics.getMultiColumnCombinedStats().values()) {
            double head = group.getMcvDistribution().getTotalRows();
            if (group.hasMcv() && group.getColumns().containsAll(used) && head == group.getRowCount()
                    && (best == null || group.getColumns().size() < best.getColumns().size())) {
                best = group;
            }
        }
        if (best == null) {
            return OptionalDouble.empty();
        }
        double matching = 0;
        McvPredicateEvaluator evaluator = new McvPredicateEvaluator();
        for (MultiColumnCombinedStats.McvEntry entry : best.getMcv()) {
            Optional<Truth> truth = evaluate(predicate, best.getColumns(), entry.getValues(), best.getMcvDistribution(),
                    evaluator);
            if (truth.isEmpty()) {
                return OptionalDouble.empty();
            }
            if (truth.get() == Truth.TRUE) {
                matching += entry.getCount();
            }
        }
        return OptionalDouble.of(statistics.getOutputRowCount() * matching / best.getRowCount());
    }

    /**
     * The filter by an OR whose parts arrive one by one. The estimate of an OR filters the distributions by the OR of
     * its first i parts for every i. We fear that evaluating that whole OR on every tuple of the head at each step
     * makes the work grow with the square of the number of parts: some hundred parts cost seconds of planning. So
     * for each tuple we keep the truth of the OR of the parts so far, and a new part adds only its own truth to it.
     * The result of filter is the same as of McvStatisticsPropagation#filter on the OR of the parts.
     */
    static final class DisjunctionFilter {
        private final Statistics input;
        private final McvPredicateEvaluator evaluator = new McvPredicateEvaluator();
        private final List<ScalarOperator> parts = new ArrayList<>();
        private final Set<ColumnRefOperator> columns = new HashSet<>();
        private final Map<MultiColumnCombinedStats, GroupTruths> groups = new IdentityHashMap<>();
        // The OR of the parts as a chain, which the filter reads only as a predicate that is an OR; it is not walked.
        private ScalarOperator disjunction;

        DisjunctionFilter(Statistics input) {
            this.input = input;
        }

        void add(ScalarOperator part) {
            parts.add(part);
            columns.addAll(Utils.extractColumnRef(part));
            disjunction = disjunction == null ? part : new CompoundPredicateOperator(
                    CompoundPredicateOperator.CompoundType.OR, disjunction, part);
        }

        /** The filter of the rows by the OR of the parts added so far. */
        Statistics filter(Statistics output) {
            return McvStatisticsPropagation.filter(disjunction, input, output, parts.size() < 2 ? null : this, false);
        }

        // The truth of the OR of the parts so far on each head tuple of a group and on its NULL tuple. A part that
        // cannot be evaluated on a tuple leaves the OR not evaluable, as in the OR evaluated whole.
        private final class GroupTruths {
            private final Truth[] tuples;
            private int evaluated;
            private boolean unsupported;
            private Truth nullTuple;
            private int nullEvaluated;
            private boolean nullUnsupported;

            private GroupTruths(int size) {
                tuples = new Truth[size];
            }

            private void evaluate(MultiColumnCombinedStats group) {
                while (evaluated < parts.size() && !unsupported) {
                    ScalarOperator part = parts.get(evaluated++);
                    int position = 0;
                    for (MultiColumnCombinedStats.McvEntry entry : group.getMcv()) {
                        Optional<Truth> truth = McvStatisticsPropagation.evaluate(part, group.getColumns(),
                                entry.getValues(), group.getMcvDistribution(), evaluator);
                        if (truth.isEmpty()) {
                            unsupported = true;
                            return;
                        }
                        tuples[position] = or(tuples[position], truth.get());
                        position++;
                    }
                }
            }

            private void evaluateNull(MultiColumnCombinedStats group) {
                while (nullEvaluated < parts.size() && !nullUnsupported) {
                    Optional<Truth> truth = McvStatisticsPropagation.evaluate(parts.get(nullEvaluated++),
                            group.getColumns(), java.util.Collections.singletonList(null), null, evaluator);
                    if (truth.isEmpty()) {
                        nullUnsupported = true;
                        return;
                    }
                    nullTuple = or(nullTuple, truth.get());
                }
            }
        }

        private static Truth or(Truth left, Truth right) {
            if (left == null) {
                return right;
            }
            return left == Truth.TRUE || right == Truth.TRUE ? Truth.TRUE
                    : left == Truth.UNKNOWN || right == Truth.UNKNOWN ? Truth.UNKNOWN : Truth.FALSE;
        }

        private GroupTruths truthsOf(MultiColumnCombinedStats group) {
            return groups.computeIfAbsent(group, key -> new GroupTruths(key.getMcv().size()));
        }

        // The truth of the OR on the tuple at a position of the head, empty when it cannot be evaluated.
        private Optional<Truth> truth(MultiColumnCombinedStats group, int position) {
            GroupTruths truths = truthsOf(group);
            truths.evaluate(group);
            return truths.unsupported ? Optional.empty() : Optional.of(truths.tuples[position]);
        }

        // The truth of the OR on the tuple of NULLs, empty when it cannot be evaluated.
        private Optional<Truth> truthOfNull(MultiColumnCombinedStats group) {
            GroupTruths truths = truthsOf(group);
            truths.evaluateNull(group);
            return truths.nullUnsupported ? Optional.empty() : Optional.of(truths.nullTuple);
        }
    }

    static Statistics filter(ScalarOperator predicate, Statistics input, Statistics output) {
        return filter(predicate, input, output, null, false);
    }

    /**
     * The filter of the conjuncts that the estimate of a conjunction took as independent of each other: those of one
     * MCV group, those of another, and the equalities of columns that have none. We expect a conjunct of other columns
     * not to change the distribution of a group, as the estimate that multiplied their shares took it, so a group is
     * filtered by its own conjuncts alone, and its tuples and tail are scaled to the rows of the output, as the other
     * statistics of it describe those rows. The filter of any other conjunction does not take it so: a NOT NULL check
     * of a column with NULLs may correlate with the group.
     */
    static Statistics filterIndependent(ScalarOperator predicate, Statistics input, Statistics output) {
        return filter(predicate, input, output, null, true);
    }

    // disjunction: the parts of the predicate, which is their OR, with the truths of the OR kept; null otherwise
    // independent: whether the conjuncts of other columns than the group are independent of it, see filterIndependent
    private static Statistics filter(ScalarOperator predicate, Statistics input, Statistics output,
                                     DisjunctionFilter disjunction, boolean independent) {
        if (input.getMultiColumnCombinedStats().isEmpty() || input.getOutputRowCount() <= 0) {
            return output;
        }
        Statistics.Builder builder = Statistics.buildFrom(output);
        Set<ColumnRefOperator> exactColumns = new HashSet<>();
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups = new HashMap<>();
        Set<Set<ColumnRefOperator>> unchangedGroups = new HashSet<>();
        Set<ColumnRefOperator> conditionedColumns = new HashSet<>();
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> conditionalMarginals = new HashMap<>();
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        McvPredicateEvaluator evaluator = new McvPredicateEvaluator();
        for (Map.Entry<Set<ColumnRefOperator>, MultiColumnCombinedStats> item
                : input.getMultiColumnCombinedStats().entrySet()) {
            MultiColumnCombinedStats group = item.getValue();
            // A generated NOT NULL check on another key must not erase this distribution when
            // the input statistics estimate that the check removes no rows. Checks on this group's
            // own columns still use its MCV, which may carry more precise NULL information.
            ScalarOperator groupPredicate = Utils.compoundAnd(conjuncts.stream()
                    .filter(conjunct -> !nonFilteringExternalNullCheck(conjunct, item.getKey(), input)).toList());
            if (groupPredicate == null) {
                groups.put(item.getKey(), group);
                unchangedGroups.add(item.getKey());
                continue;
            }
            Set<ColumnRefOperator> used = disjunction == null ? new HashSet<>(Utils.extractColumnRef(groupPredicate))
                    : disjunction.columns;
            if (!group.hasDistribution() || java.util.Collections.disjoint(used, item.getKey())) {
                // No evidence of a change in this group's distribution. The row-count bound is
                // applied by consumers, independently of the units of its stored counts.
                groups.put(item.getKey(), group);
                if (group.hasDistribution()) {
                    unchangedGroups.add(item.getKey());
                }
                continue;
            }
            conditionedColumns.addAll(item.getKey());
            // A predicate touching this group can change every component's marginal distribution.
            // Keep a histogram only if the predicate estimator actually derived a new one; a
            // complete surviving head below can supply exact conditional marginals again.
            for (ColumnRefOperator column : item.getKey()) {
                ColumnStatistic before = input.getColumnStatistics().get(column);
                ColumnStatistic after = output.getColumnStatistics().get(column);
                if (before != null && after != null && before.getHistogram() == after.getHistogram()
                        && !exactColumns.contains(column)) {
                    builder.addColumnStatistic(column, ColumnStatistic.buildFrom(after).setHistogram(null).build());
                }
            }
            // The correlation with a predicate outside the group is unknown. Retaining the old head here would
            // incorrectly claim that its unconditional frequencies still hold. When the conjuncts are independent of
            // each other, see filterIndependent, the head is filtered by the conjuncts of the group alone, but a
            // conjunct that reads a column of the group and a column outside it is still of unknown correlation.
            boolean partial = !item.getKey().containsAll(used);
            double ownRows = -1;
            if (partial) {
                ScalarOperator own = independent && disjunction == null ? withinGroup(groupPredicate, item.getKey()) : null;
                if (own == null) {
                    continue;
                }
                groupPredicate = own;
                // The estimate of the conjuncts of the group is made a second time: the value of an expression on a tuple
                // of the head is evaluated once for the estimate that runs, see McvPredicateEvaluator#within. On a head
                // of 1000 timestamps the bounds of a period on hours_add(created, 5) next to a group take 11 ms more for 2
                // bounds and 165 ms more for 64 bounds (22 and 354 ms in all, McvPlanningTimeTest).
                ownRows = PredicateStatisticsCalculator.statisticsCalculate(own, input).getOutputRowCount();
            }
            List<MultiColumnCombinedStats.McvEntry> surviving = new ArrayList<>();
            double oldHead = group.getMcvDistribution().getSequentialTotalRows();
            double head = 0;
            boolean supported = true;
            int position = 0;
            for (MultiColumnCombinedStats.McvEntry entry : group.getMcv()) {
                Optional<Truth> match = disjunction == null
                        ? evaluate(groupPredicate, group.getColumns(), entry.getValues(), group.getMcvDistribution(),
                        evaluator)
                        : disjunction.truth(group, position++);
                if (match.isEmpty()) {
                    supported = false;
                    break;
                }
                if (match.get() == Truth.TRUE) {
                    surviving.add(entry);
                    head += entry.getCount();
                }
            }
            if (!supported) {
                continue;
            }
            double oldTail = Math.max(0, group.getRowCount() - oldHead);
            double rows = group.getRowCount() * (partial ? ownRows : output.getOutputRowCount())
                    / input.getOutputRowCount();
            double tail = Math.min(oldTail, Math.max(0, rows - head));
            // A complete head gives both the exact filtered mass and exact filtered NDV.
            rows = head + tail;
            if (rows <= 0) {
                continue;
            }
            if (partial) {
                // The group describes the rows of its own conjuncts, while the rest of the statistics describe those of
                // the output. We expect the conjuncts of other columns not to change the shares of the tuples, so the
                // head and the tail are scaled to the rows of the output, and the group is the distribution of them.
                double target = output.getOutputRowCount();
                // A complete head stays complete: its counts add up to the rows. A head that is not still leaves a tail.
                long headTarget = tail == 0 ? Math.round(target)
                        : Math.min(Math.round(head * target / rows), (long) Math.ceil(target) - 1);
                surviving = scaledTo(surviving, head, headTarget);
                head = headTarget;
                tail = tail == 0 ? 0 : target - headTarget;
                rows = head + tail;
            }
            long tailNdv = Math.max(0, group.getNdv() - group.getMcv().size());
            if (group.getColumns().size() == 1 && !group.getNullCounts().isEmpty()
                    && group.getNullCounts().get(0) > 0
                    && !group.getMcvDistribution().hasNull(0)
                    && (disjunction == null
                    ? evaluate(groupPredicate, group.getColumns(), java.util.Collections.singletonList(null), null,
                    evaluator) : disjunction.truthOfNull(group)).filter(truth -> truth == Truth.TRUE).isEmpty()) {
                // Global NDV includes NULL even when it was outside the retained head.
                tailNdv = Math.max(0, tailNdv - 1);
            }
            tailNdv = Math.min(tailNdv, (long) Math.ceil(tail));
            List<Map<String, Long>> marginals = new ArrayList<>();
            for (int i = 0; i < group.getColumns().size(); i++) {
                marginals.add(new HashMap<>());
            }
            if (tail == 0) {
                for (MultiColumnCombinedStats.McvEntry entry : surviving) {
                    for (int i = 0; i < marginals.size(); i++) {
                        marginals.get(i).merge(entry.getValues().get(i), entry.getCount(), Long::sum);
                    }
                }
            }
            List<MultiColumnCombinedStats.McvEntry> headEntries = new ArrayList<>();
            for (MultiColumnCombinedStats.McvEntry entry : surviving) {
                List<Long> counts = new ArrayList<>();
                if (tail == 0) {
                    for (int i = 0; i < marginals.size(); i++) {
                        counts.add(marginals.get(i).get(entry.getValues().get(i)));
                    }
                }
                // With an unknown residual, the old exact marginal counts are no longer exact. A tuple whose counts are
                // those it has is the same tuple: entries are not changed.
                headEntries.add(counts.equals(entry.getComponentCounts()) ? entry
                        : new MultiColumnCombinedStats.McvEntry(entry.getValues(), entry.getCount(), counts));
            }
            List<Long> nullCounts = tail == 0
                    ? marginals.stream().map(counts -> counts.getOrDefault(null, 0L)).toList() : List.of();
            long distinct = (long) Math.min(surviving.size() + tailNdv, distinctBound(groupPredicate, group.getColumns()));
            // With conjuncts of other columns the statistics of the columns are those of the output, which they also
            // filtered: the distributions of the group alone would count rows that the output does not have.
            if (!partial && tail == 0) {
                for (int i = 0; i < group.getColumns().size(); i++) {
                    ColumnRefOperator column = group.getColumns().get(i);
                    if (column == null || !output.getColumnStatistics().containsKey(column)) {
                        continue;
                    }
                    List<MultiColumnCombinedStats.McvEntry> values = new ArrayList<>();
                    marginals.get(i).forEach((value, count) -> values.add(new MultiColumnCombinedStats.McvEntry(
                            java.util.Collections.singletonList(value), count, List.of(count))));
                    ExternalMcvStatistics.Group marginal = ExternalMcvStatistics.Group.derived(List.of(column.getName()),
                            (long) rows, values.size(), values, List.of(nullCounts.get(i)));
                    marginal.columnStatistic(column.getType(), output.getColumnStatistic(column))
                            .ifPresent(statistic -> {
                                builder.addColumnStatistic(column, statistic);
                                exactColumns.add(column);
                            });
                }
            } else if (!partial && group.getColumns().size() == 1) {
                ColumnRefOperator column = group.getColumns().get(0);
                ColumnStatistic before = input.getColumnStatistics().get(column);
                ColumnStatistic after = output.getColumnStatistics().get(column);
                Optional<ColumnStatistic> filtered = filteredHistogram(before, after);
                if (filtered.isPresent()) {
                    if (!exactColumns.contains(column)) {
                        builder.addColumnStatistic(column, filtered.get());
                    }
                    distinct = Math.min(distinct, (long) Math.ceil(filtered.get().getDistinctValuesCount())
                            + (filtered.get().getNullsFraction() > 0 ? 1 : 0));
                }
            }
            MultiColumnCombinedStats conditional = new MultiColumnCombinedStats(Math.max(surviving.size(), distinct),
                    rows, group.getColumns(), headEntries, nullCounts, group.getMcvDistribution());
            groups.put(item.getKey(), conditional);
            if (!partial && group.getColumns().size() > 1) {
                for (ColumnRefOperator column : item.getKey()) {
                    conditionalMarginal(conditional, column, groupPredicate, output).ifPresent(marginal ->
                            conditionalMarginals.merge(Set.of(column), marginal,
                                    StatisticsCalcUtils::preferMultiColumnStats));
                }
            }
        }
        // A correlated filter can change a column without naming it. Do not let an unchanged
        // singleton (or another overlapping group) override its newly conditioned distribution.
        unchangedGroups.stream().filter(key -> !java.util.Collections.disjoint(key, conditionedColumns))
                .forEach(groups::remove);
        conditionalMarginals.replaceAll((key, marginal) -> groups.containsKey(key)
                ? StatisticsCalcUtils.preferMultiColumnStats(groups.get(key), marginal) : marginal);
        groups.putAll(conditionalMarginals);
        conditionalMarginals.forEach((key, marginal) -> {
            ColumnRefOperator column = marginal.getColumns().get(0);
            ColumnStatistic basic = output.getColumnStatistics().get(column);
            if (basic == null) {
                return;
            }
            double nulls = marginal.getNullCounts().isEmpty() ? basic.getNullsFraction()
                    : marginal.getNullCounts().get(0) / marginal.getRowCount();
            ColumnStatistic updated = ColumnStatistic.buildFrom(basic).setHistogram(null)
                    .setNullsFraction(nulls).setDistinctValuesCount(Math.max(0, marginal.getNdv() - (nulls > 0 ? 1 : 0)))
                    .build();
            if (marginal.getMcvDistribution().getTotalRowsLong()
                    == marginal.getRowCount()) {
                updated = ExternalMcvStatistics.Group.derived(List.of(column.getName()), (long) marginal.getRowCount(),
                        marginal.getNdv(), marginal.getMcv(), marginal.getNullCounts())
                        .columnStatistic(column.getType(), updated).orElse(updated);
            }
            builder.addColumnStatistic(column, updated);
        });
        return builder.setMultiColumnStatistics(groups).build();
    }

    /**
     * The tuples with counts in proportion to their own that add up to the target: the counts are rounded down and the
     * rest is given one by one to the counts with the largest remainders. The counts rounded one by one would not add up
     * to the rows of a complete head, and the head would then be taken for one that is not. The tuples that are left
     * with no row are not kept.
     */
    private static List<MultiColumnCombinedStats.McvEntry> scaledTo(List<MultiColumnCombinedStats.McvEntry> entries,
                                                                    double total, long target) {
        if (entries.isEmpty()) {
            return entries;
        }
        double factor = target / total;
        long[] counts = new long[entries.size()];
        double[] remainders = new double[entries.size()];
        long sum = 0;
        for (int i = 0; i < counts.length; i++) {
            double exact = entries.get(i).getCount() * factor;
            counts[i] = (long) Math.floor(exact);
            remainders[i] = exact - counts[i];
            sum += counts[i];
        }
        Integer[] order = new Integer[counts.length];
        for (int i = 0; i < order.length; i++) {
            order[i] = i;
        }
        java.util.Arrays.sort(order, (left, right) -> Double.compare(remainders[right], remainders[left]));
        for (int k = 0; sum < target && k < order.length; k++, sum++) {
            counts[order[k]]++;
        }
        List<MultiColumnCombinedStats.McvEntry> scaled = new ArrayList<>(entries.size());
        for (int i = 0; i < counts.length; i++) {
            // A tuple that has no row in the output is no tuple of the group: its consumers count the tuples, as the
            // groups of an aggregate, the entries of a skewed join and the NDV.
            if (counts[i] > 0) {
                scaled.add(new MultiColumnCombinedStats.McvEntry(entries.get(i).getValues(), counts[i], List.of()));
            }
        }
        return scaled;
    }

    // The conjuncts of the predicate that read columns of the group, joined, when each reads only columns of it; the
    // conjuncts that read none are left out. Null when a conjunct reads a column of the group and one outside it, and
    // when none reads a column of it.
    private static ScalarOperator withinGroup(ScalarOperator predicate, Set<ColumnRefOperator> group) {
        List<ScalarOperator> within = new ArrayList<>();
        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            List<ColumnRefOperator> columns = Utils.extractColumnRef(conjunct);
            if (columns.stream().noneMatch(group::contains)) {
                continue;
            }
            if (!group.containsAll(columns)) {
                return null;
            }
            within.add(conjunct);
        }
        return Utils.compoundAnd(within);
    }

    private static Optional<MultiColumnCombinedStats> conditionalMarginal(MultiColumnCombinedStats group,
            ColumnRefOperator column, ScalarOperator predicate, Statistics input) {
        boolean completeHead = group.getMcvDistribution().getTotalRowsLong()
                == group.getRowCount();
        // If every other component is fixed, projecting the remaining component is one-to-one.
        // A retained tuple is its entire frequency: that value cannot occur again in the tail.
        // With several accepted values of another component, only a complete head gives marginals.
        if (!completeHead && distinctBound(predicate,
                group.getColumns().stream().filter(other -> !column.equals(other)).toList()) != 1) {
            return Optional.empty();
        }
        int position = group.getColumns().indexOf(column);
        Map<String, Long> counts = new HashMap<>();
        group.getMcv().forEach(entry -> counts.merge(entry.getValues().get(position), entry.getCount(), Long::sum));
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        counts.forEach((value, count) -> entries.add(new MultiColumnCombinedStats.McvEntry(
                java.util.Collections.singletonList(value), count, List.of(count))));
        ColumnStatistic basic = input.getColumnStatistics().get(column);
        boolean noNulls = !column.isNullable() || (basic != null && !basic.isUnknown() && basic.getNullsFraction() == 0);
        List<Long> nulls = completeHead || counts.containsKey(null) || noNulls
                ? List.of(counts.getOrDefault(null, 0L)) : List.of();
        long ndv = completeHead ? counts.size() : group.getNdv();
        if (!completeHead && basic != null && !basic.isUnknown()) {
            ndv = Math.min(ndv, (long) Math.ceil(basic.getDistinctValuesCount()) + (noNulls ? 0 : 1));
        }
        return Optional.of(new MultiColumnCombinedStats(Math.max(counts.size(), ndv), group.getRowCount(),
                List.of(column), entries, nulls));
    }

    private static boolean nonFilteringExternalNullCheck(ScalarOperator predicate, Set<ColumnRefOperator> columns,
                                                         Statistics input) {
        if (!(predicate instanceof IsNullPredicateOperator check) || !check.isNotNull()
                || !(check.getChild(0) instanceof ColumnRefOperator column) || columns.contains(column)) {
            return false;
        }
        ColumnStatistic basic = input.getColumnStatistics().get(column);
        return !column.isNullable() || (basic != null && !basic.isUnknown() && basic.getNullsFraction() == 0);
    }

    /** Preserve residual NDVs when the ordinary range estimator trims a singleton histogram. */
    private static Optional<ColumnStatistic> filteredHistogram(ColumnStatistic before, ColumnStatistic after) {
        if (before == null || after == null || before.getHistogram() == null || after.getHistogram() == null
                || before.getHistogram() == after.getHistogram()) {
            return Optional.empty();
        }
        Histogram original = before.getHistogram();
        Histogram filtered = after.getHistogram();
        if (original.hasStringValues() || filtered.hasUnknownRange()) {
            // The string estimator retains interval endpoints and residual NDVs while trimming.
            return Optional.of(after);
        }
        List<Bucket> buckets = new ArrayList<>();
        double distinct = filtered.getMCV().size();
        long previous = 0;
        int cursor = 0;
        long originalPrevious = 0;
        for (Bucket bucket : filtered.getBuckets()) {
            long mass = bucket.getCount() - previous;
            previous = bucket.getCount();
            Long ndv = null;
            while (cursor < original.getBuckets().size()) {
                Bucket source = original.getBuckets().get(cursor);
                long originalMass = source.getCount() - originalPrevious;
                if (source.getLower() <= bucket.getLower() && source.getUpper() >= bucket.getUpper()
                        && source.getDistinctCount().isPresent() && originalMass > 0) {
                    // Exact for a retained bucket; a trimmed bucket assumes uniform residual values.
                    ndv = Math.min(mass, Math.max(0, (long) Math.ceil(source.getDistinctCount().get()
                            * Math.min(1, mass / (double) originalMass))));
                    break;
                }
                if (source.getLower() > bucket.getLower()) {
                    break;
                }
                originalPrevious = source.getCount();
                cursor++;
            }
            if (ndv == null) {
                return Optional.empty();
            }
            distinct += ndv;
            buckets.add(new Bucket(bucket.getLower(), bucket.getUpper(), bucket.getCount(), bucket.getUpperRepeats(), ndv));
        }
        return Optional.of(ColumnStatistic.buildFrom(after).setDistinctValuesCount(distinct)
                .setHistogram(new Histogram(buckets, filtered.getMCV())).build());
    }

    private static double distinctBound(ScalarOperator predicate, List<ColumnRefOperator> columns) {
        Map<ColumnRefOperator, Double> bounds = new HashMap<>();
        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            if (conjunct.getChildren().isEmpty() || !conjunct.getChild(0).isColumnRef()) {
                continue;
            }
            double bound = Double.POSITIVE_INFINITY;
            if (conjunct instanceof BinaryPredicateOperator
                    && ((BinaryPredicateOperator) conjunct).getBinaryType().isEqual()
                    && conjunct.getChild(1).isConstantRef()) {
                bound = 1;
            } else if (conjunct instanceof InPredicateOperator && !((InPredicateOperator) conjunct).isNotIn()) {
                bound = conjunct.getChildren().subList(1, conjunct.getChildren().size()).stream().distinct().count();
            } else if (conjunct instanceof IsNullPredicateOperator && !((IsNullPredicateOperator) conjunct).isNotNull()) {
                bound = 1;
            }
            bounds.merge((ColumnRefOperator) conjunct.getChild(0), bound, Math::min);
        }
        double product = 1;
        for (ColumnRefOperator column : columns) {
            product *= bounds.getOrDefault(column, Double.POSITIVE_INFINITY);
        }
        return product;
    }

    /**
     * Whether NOT of a condition on one column holds on a value of the column: the condition is FALSE there.
     * Empty when the condition cannot be evaluated on the value.
     */
    static Optional<Boolean> negationHolds(ScalarOperator condition, ColumnRefOperator column, String value,
                                           McvPredicateEvaluator evaluator) {
        return evaluate(condition, List.of(column), java.util.Collections.singletonList(value), null, evaluator)
                .map(truth -> truth == Truth.FALSE);
    }

    /**
     * The truth of the predicate on one tuple.
     *
     * @param head the head the values are a tuple of, null when they are not
     */
    private static Optional<Truth> evaluate(ScalarOperator predicate, List<ColumnRefOperator> columns,
                                           List<String> values, PreparedMcvTuples head,
                                           McvPredicateEvaluator evaluator) {
        // Boolean equality with TRUE is normalized to the column itself by the optimizer.
        // Keep the same conditional distribution for WHERE flag and WHERE flag = TRUE.
        if (predicate instanceof ColumnRefOperator column && column.getType().isBoolean()) {
            int position = columns.indexOf(column);
            if (position < 0) {
                return Optional.empty();
            }
            byte operand = McvPredicateEvaluator.operand(column, column, values.get(position), head, position);
            return operand == McvPredicateEvaluator.OPERAND_EMPTY ? Optional.empty()
                    : Optional.of(operand == McvPredicateEvaluator.OPERAND_NULL ? Truth.UNKNOWN
                    : operand == McvPredicateEvaluator.OPERAND_TRUE ? Truth.TRUE : Truth.FALSE);
        }
        if (predicate instanceof ConstantOperator) {
            ConstantOperator constant = (ConstantOperator) predicate;
            if (constant.isNull()) {
                return Optional.of(Truth.UNKNOWN);
            }
            return constant.getType().isBoolean()
                    ? Optional.of(constant.getBoolean() ? Truth.TRUE : Truth.FALSE) : Optional.empty();
        }
        if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compound = (CompoundPredicateOperator) predicate;
            Optional<Truth> left = evaluate(predicate.getChild(0), columns, values, head, evaluator);
            if (left.isEmpty()) {
                return Optional.empty();
            }
            if (compound.isNot()) {
                return Optional.of(left.get() == Truth.UNKNOWN ? Truth.UNKNOWN
                        : left.get() == Truth.TRUE ? Truth.FALSE : Truth.TRUE);
            }
            Optional<Truth> right = evaluate(predicate.getChild(1), columns, values, head, evaluator);
            if (right.isEmpty()) {
                return Optional.empty();
            }
            if (compound.isAnd()) {
                return Optional.of(left.get() == Truth.FALSE || right.get() == Truth.FALSE ? Truth.FALSE
                        : left.get() == Truth.UNKNOWN || right.get() == Truth.UNKNOWN ? Truth.UNKNOWN : Truth.TRUE);
            }
            return Optional.of(left.get() == Truth.TRUE || right.get() == Truth.TRUE ? Truth.TRUE
                    : left.get() == Truth.UNKNOWN || right.get() == Truth.UNKNOWN ? Truth.UNKNOWN : Truth.FALSE);
        }
        ColumnRefOperator column = evaluator.column(predicate);
        int position = columns.indexOf(column);
        if (column == null || position < 0) {
            return Optional.empty();
        }
        String value = values.get(position);
        // A value of a string column is its own operand, which is not NULL.
        boolean plainString = value != null && predicate.getChild(0).isColumnRef() && column.getType().isStringType();
        if (!(predicate instanceof IsNullPredicateOperator) && !plainString) {
            byte operand = McvPredicateEvaluator.operand(predicate.getChild(0), column, value, head, position);
            if (operand == McvPredicateEvaluator.OPERAND_EMPTY) {
                return Optional.empty();
            }
            if (operand == McvPredicateEvaluator.OPERAND_NULL) {
                return Optional.of(Truth.UNKNOWN);
            }
        }
        return evaluator.matchesComponent(predicate, column, value, head, position)
                .map(match -> match ? Truth.TRUE : Truth.FALSE);
    }
}
