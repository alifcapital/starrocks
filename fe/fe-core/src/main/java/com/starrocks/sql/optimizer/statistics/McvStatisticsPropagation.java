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
            double head = group.getMcv().stream().mapToDouble(MultiColumnCombinedStats.McvEntry::getCount).sum();
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
            Optional<Truth> truth = evaluate(predicate, best.getColumns(), entry.getValues(), evaluator);
            if (truth.isEmpty()) {
                return OptionalDouble.empty();
            }
            if (truth.get() == Truth.TRUE) {
                matching += entry.getCount();
            }
        }
        return OptionalDouble.of(statistics.getOutputRowCount() * matching / best.getRowCount());
    }

    static Statistics filter(ScalarOperator predicate, Statistics input, Statistics output) {
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
            Set<ColumnRefOperator> used = new HashSet<>(Utils.extractColumnRef(groupPredicate));
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
            if (!item.getKey().containsAll(used)) {
                // The correlation with a predicate outside the group is unknown. Retaining the old
                // head here would incorrectly claim that its unconditional frequencies still hold.
                continue;
            }
            List<MultiColumnCombinedStats.McvEntry> surviving = new ArrayList<>();
            double oldHead = 0;
            double head = 0;
            boolean supported = true;
            for (MultiColumnCombinedStats.McvEntry entry : group.getMcv()) {
                oldHead += entry.getCount();
                Optional<Truth> match = evaluate(groupPredicate, group.getColumns(), entry.getValues(), evaluator);
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
            double rows = group.getRowCount() * output.getOutputRowCount() / input.getOutputRowCount();
            double tail = Math.min(oldTail, Math.max(0, rows - head));
            // A complete head gives both the exact filtered mass and exact filtered NDV.
            rows = head + tail;
            if (rows <= 0) {
                continue;
            }
            long tailNdv = Math.max(0, group.getNdv() - group.getMcv().size());
            if (group.getColumns().size() == 1 && !group.getNullCounts().isEmpty()
                    && group.getNullCounts().get(0) > 0
                    && group.getMcv().stream().noneMatch(entry -> entry.getValues().get(0) == null)
                    && evaluate(groupPredicate, group.getColumns(), java.util.Collections.singletonList(null), evaluator)
                            .filter(truth -> truth == Truth.TRUE).isEmpty()) {
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
                // With an unknown residual, the old exact marginal counts are no longer exact.
                headEntries.add(new MultiColumnCombinedStats.McvEntry(entry.getValues(), entry.getCount(), counts));
            }
            List<Long> nullCounts = tail == 0
                    ? marginals.stream().map(counts -> counts.getOrDefault(null, 0L)).toList() : List.of();
            long distinct = (long) Math.min(surviving.size() + tailNdv, distinctBound(groupPredicate, group.getColumns()));
            if (tail == 0) {
                for (int i = 0; i < group.getColumns().size(); i++) {
                    ColumnRefOperator column = group.getColumns().get(i);
                    if (column == null || !output.getColumnStatistics().containsKey(column)) {
                        continue;
                    }
                    List<MultiColumnCombinedStats.McvEntry> values = new ArrayList<>();
                    marginals.get(i).forEach((value, count) -> values.add(new MultiColumnCombinedStats.McvEntry(
                            java.util.Collections.singletonList(value), count, List.of(count))));
                    ExternalMcvStatistics.Group marginal = new ExternalMcvStatistics.Group(List.of(column.getName()),
                            (long) rows, values.size(), values, List.of(), List.of(nullCounts.get(i)));
                    marginal.columnStatistic(column.getType(), output.getColumnStatistic(column))
                            .ifPresent(statistic -> {
                                builder.addColumnStatistic(column, statistic);
                                exactColumns.add(column);
                            });
                }
            } else if (group.getColumns().size() == 1) {
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
                    rows, group.getColumns(), headEntries, nullCounts);
            groups.put(item.getKey(), conditional);
            if (group.getColumns().size() > 1) {
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
            if (marginal.getMcv().stream().mapToLong(MultiColumnCombinedStats.McvEntry::getCount).sum()
                    == marginal.getRowCount()) {
                updated = new ExternalMcvStatistics.Group(List.of(column.getName()), (long) marginal.getRowCount(),
                        marginal.getNdv(), marginal.getMcv(), List.of(), marginal.getNullCounts())
                        .columnStatistic(column.getType(), updated).orElse(updated);
            }
            builder.addColumnStatistic(column, updated);
        });
        return builder.setMultiColumnStatistics(groups).build();
    }

    private static Optional<MultiColumnCombinedStats> conditionalMarginal(MultiColumnCombinedStats group,
            ColumnRefOperator column, ScalarOperator predicate, Statistics input) {
        boolean completeHead = group.getMcv().stream().mapToLong(MultiColumnCombinedStats.McvEntry::getCount).sum()
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

    private static Optional<Truth> evaluate(ScalarOperator predicate, List<ColumnRefOperator> columns,
                                           List<String> values, McvPredicateEvaluator evaluator) {
        // Boolean equality with TRUE is normalized to the column itself by the optimizer.
        // Keep the same conditional distribution for WHERE flag and WHERE flag = TRUE.
        if (predicate instanceof ColumnRefOperator column && column.getType().isBoolean()) {
            int position = columns.indexOf(column);
            if (position < 0) {
                return Optional.empty();
            }
            return MultiColumnMcvEstimator.evaluate(column, column, values.get(position))
                    .map(value -> value.isNull() ? Truth.UNKNOWN : value.getBoolean() ? Truth.TRUE : Truth.FALSE);
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
            Optional<Truth> left = evaluate(predicate.getChild(0), columns, values, evaluator);
            if (left.isEmpty()) {
                return Optional.empty();
            }
            if (compound.isNot()) {
                return Optional.of(left.get() == Truth.UNKNOWN ? Truth.UNKNOWN
                        : left.get() == Truth.TRUE ? Truth.FALSE : Truth.TRUE);
            }
            Optional<Truth> right = evaluate(predicate.getChild(1), columns, values, evaluator);
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
        if (!(predicate instanceof IsNullPredicateOperator)) {
            Optional<ConstantOperator> operand = MultiColumnMcvEstimator.evaluate(predicate.getChild(0), column, value);
            if (operand.isEmpty()) {
                return Optional.empty();
            }
            if (operand.get().isNull()) {
                return Optional.of(Truth.UNKNOWN);
            }
        }
        return evaluator.matchesComponent(predicate, column, value)
                .map(match -> match ? Truth.TRUE : Truth.FALSE);
    }
}
