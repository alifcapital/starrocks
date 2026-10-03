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

import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Transforms retained values; unseen cast collisions remain an NDV approximation. */
final class McvCastStatistics {
    private McvCastStatistics() {
    }

    static ColumnRefOperator sourceColumn(ScalarOperator expression) {
        while (expression instanceof CastOperator) {
            expression = expression.getChild(0);
        }
        return expression instanceof ColumnRefOperator ? (ColumnRefOperator) expression : null;
    }

    /** One output group per input group, including repeated aliases; no alias Cartesian product. */
    static MultiColumnCombinedStats projectGroup(Map<ColumnRefOperator, ? extends ScalarOperator> projection,
                                                 MultiColumnCombinedStats group) {
        if (!group.hasMcv() || !MultiColumnMcvEstimator.isEnabled()) {
            return null;
        }
        List<ColumnRefOperator> columns = new ArrayList<>();
        List<ScalarOperator> expressions = new ArrayList<>();
        List<Integer> positions = new ArrayList<>();
        boolean cast = false;
        List<? extends Map.Entry<ColumnRefOperator, ? extends ScalarOperator>> outputs = projection.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(java.util.Comparator.comparingInt(ColumnRefOperator::getId))).toList();
        for (int i = 0; i < group.getColumns().size(); i++) {
            ColumnRefOperator source = group.getColumns().get(i);
            int before = columns.size();
            if (source != null) {
                for (Map.Entry<ColumnRefOperator, ? extends ScalarOperator> entry : outputs) {
                    if (source.equals(sourceColumn(entry.getValue()))) {
                        columns.add(entry.getKey());
                        expressions.add(entry.getValue());
                        positions.add(i);
                        cast |= entry.getValue() instanceof CastOperator;
                    }
                }
            }
            if (before == columns.size()) {
                // Keep unread components so partial heads do not masquerade as exact marginals.
                columns.add(null);
                expressions.add(null);
                positions.add(i);
            }
        }
        if (!cast) {
            return null;
        }
        Map<List<String>, Long> tuples = new java.util.LinkedHashMap<>();
        for (MultiColumnCombinedStats.McvEntry entry : group.getMcv()) {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                int position = positions.get(i);
                String value = entry.getValues().get(position);
                if (expressions.get(i) instanceof CastOperator) {
                    Optional<ConstantOperator> converted = MultiColumnMcvEstimator.evaluate(
                            expressions.get(i), group.getColumns().get(position), value);
                    if (converted.isEmpty()) {
                        return null;
                    }
                    value = converted.get().isNull() ? null : MultiColumnMcvEstimator.constantText(converted.get());
                    if (value != null && columns.get(i).getType().isNumericType()) {
                        value = MultiColumnJoinMcvEstimator.canonical(columns.get(i).getType(), value);
                    }
                }
                values.add(value);
            }
            tuples.merge(values, entry.getCount(), Long::sum);
        }
        boolean complete = group.getMcvDistribution().getTotalRowsLong()
                == group.getRowCount();
        List<Map<String, Long>> marginals = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            marginals.add(new HashMap<>());
        }
        if (complete) {
            tuples.forEach((values, count) -> {
                for (int i = 0; i < values.size(); i++) {
                    marginals.get(i).merge(values.get(i), count, Long::sum);
                }
            });
        }
        List<MultiColumnCombinedStats.McvEntry> head = new ArrayList<>();
        tuples.forEach((values, count) -> {
            List<Long> counts = new ArrayList<>();
            if (complete) {
                for (int i = 0; i < values.size(); i++) {
                    counts.add(marginals.get(i).get(values.get(i)));
                }
            }
            head.add(new MultiColumnCombinedStats.McvEntry(values, count, counts));
        });
        long ndv = complete ? head.size() : Math.max(head.size(), group.getNdv() - group.getMcv().size() + head.size());
        // Unknown tail conversions cannot supply exact marginal frequencies or NULL counts.
        List<Long> nulls = complete ? marginals.stream().map(m -> m.getOrDefault(null, 0L)).toList() : List.of();
        return new MultiColumnCombinedStats(ndv, group.getRowCount(), columns, head, nulls);
    }

    static MultiColumnCombinedStats derive(ColumnRefOperator output, ScalarOperator expression, Statistics input) {
        if (input.getMultiColumnCombinedStats().isEmpty() || !MultiColumnMcvEstimator.isEnabled()) {
            return null;
        }
        ColumnRefOperator source = sourceColumn(expression);
        if (source == null) {
            return null;
        }
        MultiColumnCombinedStats best = null;
        Map<String, Long> values = null;
        long knownRows = -1;
        boolean complete = false;
        for (MultiColumnCombinedStats group : input.getMultiColumnCombinedStats().values()) {
            int position = group.getColumns().indexOf(source);
            if (position < 0 || !group.hasMcv()) {
                continue;
            }
            boolean full = group.getMcvDistribution().getTotalRows()
                    == group.getRowCount();
            Map<String, Long> candidate = new HashMap<>();
            for (MultiColumnCombinedStats.McvEntry entry : group.getMcv()) {
                String value = entry.getValues().get(position);
                if (group.getColumns().size() == 1 || full) {
                    candidate.merge(value, entry.getCount(), Long::sum);
                } else if (entry.hasComponentCounts()) {
                    candidate.putIfAbsent(value, entry.getComponentCounts().get(position));
                }
            }
            long mass = candidate.values().stream().mapToLong(Long::longValue).sum();
            if (best == null || (full && !complete) || (full == complete && mass / group.getRowCount()
                    > knownRows / best.getRowCount())) {
                best = group;
                values = candidate;
                knownRows = mass;
                complete = full;
            }
        }
        if (best == null || values.isEmpty()) {
            return null;
        }
        int position = best.getColumns().indexOf(source);
        long nulls = values.getOrDefault(null, 0L);
        if (best.getNullCounts().size() == best.getColumns().size()) {
            nulls = best.getNullCounts().get(position);
        } else if (!complete && !input.getColumnStatistics().getOrDefault(source, ColumnStatistic.unknown()).isUnknown()) {
            nulls = Math.max(nulls, Math.round(input.getColumnStatistics().getOrDefault(source, ColumnStatistic.unknown())
                    .getNullsFraction() * best.getRowCount()));
        }
        values.remove(null);
        nulls = Math.min(nulls, Math.max(0, Math.round(best.getRowCount())
                - values.values().stream().mapToLong(Long::longValue).sum()));
        boolean sourceHasNull = nulls > 0;
        Map<String, Long> converted = new HashMap<>();
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            Optional<ConstantOperator> result = MultiColumnMcvEstimator.evaluate(expression, source, entry.getKey());
            if (result.isEmpty()) {
                return null;
            }
            if (result.get().isNull()) {
                nulls += entry.getValue();
            } else {
                String value = MultiColumnMcvEstimator.constantText(result.get());
                if (output.getType().isNumericType()) {
                    value = MultiColumnJoinMcvEstimator.canonical(output.getType(), value);
                }
                converted.merge(value, entry.getValue(), Long::sum);
            }
        }
        double ndv = input.getColumnStatistics().getOrDefault(source, ColumnStatistic.unknown()).isUnknown() ? -1
                : input.getColumnStatistics().getOrDefault(source, ColumnStatistic.unknown()).getDistinctValuesCount();
        if (best.getColumns().size() == 1) {
            ndv = Math.max(0, best.getNdv() - (sourceHasNull ? 1 : 0));
        }
        if (complete) {
            ndv = converted.size();
        } else if (ndv >= 0 && Double.isFinite(ndv)) {
            // Account for observed collisions/invalid values, retaining the existing estimate for
            // unseen values. The tail can also collide or become NULL; it is not known exactly.
            ndv = Math.max(converted.size(), ndv - values.size() + converted.size());
        } else {
            return null;
        }
        List<MultiColumnCombinedStats.McvEntry> head = new ArrayList<>();
        converted.forEach((value, count) -> head.add(new MultiColumnCombinedStats.McvEntry(List.of(value), count)));
        if (nulls > 0) {
            head.add(new MultiColumnCombinedStats.McvEntry(Collections.singletonList(null), nulls));
        }
        return new MultiColumnCombinedStats((long) Math.ceil(ndv) + (nulls > 0 ? 1 : 0),
                best.getRowCount(), List.of(output), head, List.of(nulls));
    }
}
