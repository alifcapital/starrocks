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

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

/** The NDV and MCV distribution of one runtime filter key at its build or probe operator. */
public final class RuntimeFilterStatistics {
    private final Type type;
    private final double ndv;
    private final double nullFraction;
    private final Map<String, Double> head;
    private final boolean completeHead;

    private RuntimeFilterStatistics(Type type, double ndv, double nullFraction, Map<String, Double> head) {
        this.type = type;
        this.ndv = ndv;
        this.nullFraction = clamp(nullFraction);
        this.head = Map.copyOf(head);
        this.completeHead = mass(head) >= 1 - this.nullFraction - 1e-9;
    }

    public double getNdv() {
        return ndv;
    }

    public static RuntimeFilterStatistics fromExpression(ScalarOperator expression, Statistics input) {
        ColumnRefOperator source = McvCastStatistics.sourceColumn(expression);
        if (source == null) {
            return null;
        }
        ColumnRefOperator output = new ColumnRefOperator(source.getId(), expression.getType(),
                source.getName(), expression.isNullable());
        ColumnStatistic basic = ExpressionStatisticCalculator.calculate(expression, input);
        MultiColumnCombinedStats cast = McvCastStatistics.derive(output, expression, input);
        return from(output, basic, cast == null ? List.of() : List.of(cast), input.getOutputRowCount());
    }

    public RuntimeFilterStatistics boundByRows(double rows) {
        if (rows == 0) {
            return new RuntimeFilterStatistics(type, 0, 0, Map.of());
        }
        if (rows < 0 || ndv <= rows) {
            return this;
        }
        return new RuntimeFilterStatistics(type, rows, nullFraction, rows < head.size() ? Map.of() : head);
    }

    public static RuntimeFilterStatistics from(ColumnRefOperator column, ColumnStatistic basic,
                                               Collection<MultiColumnCombinedStats> groups, double rows) {
        double ndv = basic.isUnknown() ? -1 : basic.getDistinctValuesCount();
        double nulls = basic.isUnknown() || !Double.isFinite(basic.getNullsFraction()) ? 0 : basic.getNullsFraction();
        Map<String, Double> head = new HashMap<>();
        if (MultiColumnMcvEstimator.isEnabled()) {
            for (MultiColumnCombinedStats group : groups) {
                int position = group.getColumns().indexOf(column);
                if (position < 0 || !group.hasDistribution()) {
                    continue;
                }
                Map<String, Double> candidate = new HashMap<>();
                boolean single = group.getColumns().size() == 1;
                boolean completeHead = group.getMcv().stream().mapToDouble(
                        MultiColumnCombinedStats.McvEntry::getCount).sum() == group.getRowCount();
                double groupNulls = nulls;
                if (group.getNullCounts().size() == group.getColumns().size()) {
                    groupNulls = group.getNullCounts().get(position) / group.getRowCount();
                }
                if (completeHead) {
                    groupNulls = group.getMcv().stream()
                            .filter(entry -> entry.getValues().get(position) == null)
                            .mapToDouble(MultiColumnCombinedStats.McvEntry::getCount).sum() / group.getRowCount();
                }
                for (MultiColumnCombinedStats.McvEntry entry : group.getMcv()) {
                    String value = entry.getValues().get(position);
                    if (single || completeHead) {
                        add(candidate, column.getType(), value, entry.getCount() / group.getRowCount());
                    } else if (entry.hasComponentCounts() && value != null) {
                        // A component can occur in several head tuples; its marginal is counted once.
                        candidate.putIfAbsent(canonical(column.getType(), value),
                                entry.getComponentCounts().get(position) / group.getRowCount());
                    }
                }
                if (mass(candidate) > mass(head) || (head.isEmpty() && (single || completeHead))) {
                    head = candidate;
                    nulls = groupNulls;
                    if (single) {
                        ndv = Math.max(0, group.getNdv() - (groupNulls > 0 ? 1 : 0));
                    } else if (completeHead) {
                        ndv = candidate.size();
                    }
                }
            }
        }
        if (!Double.isFinite(ndv) || ndv < 0) {
            ndv = -1;
        } else {
            ndv = Math.max(head.size(), ndv);
        }
        double mass = mass(head);
        if (mass > 1 - nulls && mass > 0) {
            double scale = Math.max(0, 1 - nulls) / mass;
            head.replaceAll((key, value) -> value * scale);
        }
        return new RuntimeFilterStatistics(column.getType(), ndv, nulls, head).boundByRows(rows);
    }

    /** Estimates membership, so build-side duplicates do not multiply probe rows. */
    public OptionalDouble probePassFraction(RuntimeFilterStatistics probe, boolean nullSafe) {
        if (probe == null || ndv < 0 || probe.ndv < 0 || !comparable(type, probe.type)) {
            return OptionalDouble.empty();
        }
        double matchedMass = 0;
        int matches = 0;
        for (Map.Entry<String, Double> entry : probe.head.entrySet()) {
            if (head.containsKey(entry.getKey())) {
                matchedMass += entry.getValue();
                matches++;
            }
        }
        // Unlisted build keys are spread over the remaining probe domain. Known build head keys
        // can match only the probe tail or a matching probe head. These are containment estimates.
        double buildTailNdv = mass(head) >= 1 - nullFraction - 1e-9 ? 0 : Math.max(0, ndv - head.size());
        double probeTailNdv = Math.max(0, probe.ndv - probe.head.size());
        double unmatchedProbeKeys = probe.head.size() - matches;
        double remainingProbeKeys = Math.max(0, probe.ndv - matches);
        double headPass = remainingProbeKeys > 0 ? clamp(buildTailNdv / remainingProbeKeys) : 0;
        double tailBuildKeys = Math.max(0, head.size() - matches + buildTailNdv - unmatchedProbeKeys * headPass);
        double tailPass = probeTailNdv > 0 ? clamp(tailBuildKeys / probeTailNdv) : 0;
        double result = matchedMass + Math.max(0, mass(probe.head) - matchedMass) * headPass
                + Math.max(0, 1 - probe.nullFraction - mass(probe.head)) * tailPass;
        if (nullSafe && nullFraction > 0) {
            result += probe.nullFraction;
        }
        return OptionalDouble.of(clamp(result));
    }

    /** Membership known from retained statistics, not from an assumed overlap of unseen domains. */
    OptionalDouble knownMembership(Type probeType, String value, boolean nullSafe) {
        if (!comparable(type, probeType)) {
            return OptionalDouble.empty();
        }
        boolean complete = completeHead;
        if (value == null) {
            if (!nullSafe) {
                return OptionalDouble.of(0);
            }
            if (nullFraction > 0) {
                return OptionalDouble.of(1);
            }
            return complete ? OptionalDouble.of(0) : OptionalDouble.empty();
        }
        if (head.containsKey(canonical(probeType, value))) {
            return OptionalDouble.of(1);
        }
        return complete || ndv == 0 ? OptionalDouble.of(0) : OptionalDouble.empty();
    }

    private static boolean comparable(Type left, Type right) {
        return left.equals(right) || (exactNumeric(left) && exactNumeric(right))
                || (left.isStringType() && right.isStringType());
    }

    private static boolean exactNumeric(Type type) {
        return type.isFixedPointType() || type.isDecimalV2() || type.isDecimalV3();
    }

    private static String canonical(Type type, String value) {
        try {
            if (type.isFloatingPointType() && Double.parseDouble(value) == 0) {
                return "0";
            }
        } catch (NumberFormatException ignored) {
            return value;
        }
        return MultiColumnJoinMcvEstimator.canonical(type, value);
    }

    private static void add(Map<String, Double> head, Type type, String value, double share) {
        if (value != null && Double.isFinite(share) && share > 0) {
            head.merge(canonical(type, value), share, Double::sum);
        }
    }

    private static double mass(Map<String, Double> head) {
        return head.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    private static double clamp(double value) {
        return Math.max(0, Math.min(1, value));
    }
}
