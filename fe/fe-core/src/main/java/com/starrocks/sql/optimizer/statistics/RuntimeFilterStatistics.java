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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

/** The NDV and MCV distribution of one runtime filter key at its build or probe operator. */
public final class RuntimeFilterStatistics {
    /**
     * How the NDV and MCV statistics of the keys estimate the probe rows that pass a filter, when the JOIN
     * statistics cannot answer. CORRELATED assumes that the keys of the smaller side are among the keys of the
     * other side. INDEPENDENT assumes that both sides take their keys independently from the source domain of the
     * keys, so a build side that a filter reduced to a few keys of its column passes few probe rows. OFF does not
     * estimate from NDV and MCV statistics.
     */
    public enum NdvEstimate {
        CORRELATED, INDEPENDENT, OFF;

        // A mistyped session variable must not fail queries, so an unknown value means the default.
        public static NdvEstimate parse(String value) {
            if (value != null) {
                for (NdvEstimate estimate : values()) {
                    if (estimate.name().equalsIgnoreCase(value.trim())) {
                        return estimate;
                    }
                }
            }
            return INDEPENDENT;
        }
    }

    private final Type type;
    private final double ndv;
    // The NDV of the key column before filters and joins reduced it, never below ndv; -1 when ndv is unknown.
    private final double sourceNdv;
    private final double nullFraction;
    private final Map<String, Double> head;
    private final boolean completeHead;
    private final double headMass;
    private final JoinKey joinKey;

    private record JoinKey(JoinStatisticsPlanner planner, JoinStatisticsScope scope, ColumnRefOperator column, double rows) {
    }

    private RuntimeFilterStatistics(Type type, double ndv, double sourceNdv, double nullFraction,
                                    Map<String, Double> head) {
        this(type, ndv, sourceNdv, nullFraction, head, null);
    }

    private RuntimeFilterStatistics(Type type, double ndv, double sourceNdv, double nullFraction,
                                    Map<String, Double> head, JoinKey joinKey) {
        this.type = type;
        this.ndv = ndv;
        this.sourceNdv = ndv < 0 ? -1 : Math.max(ndv, sourceNdv);
        this.nullFraction = clamp(nullFraction);
        this.head = Map.copyOf(head);
        this.headMass = mass(this.head);
        this.completeHead = mass(head) >= 1 - this.nullFraction - 1e-9;
        this.joinKey = joinKey;
    }

    public RuntimeFilterStatistics withJoinStatistics(JoinStatisticsPlanner planner, JoinStatisticsScope scope,
                                                      ColumnRefOperator column, double rows) {
        if (planner == null || scope == null || column == null || !scope.getColumns().containsKey(column) || rows <= 0) {
            return this;
        }
        JoinStatisticsPlanner.KeyStatistics key = planner.keyStatistics(scope, column);
        double keyNdv = key == null || key.degree() == null ? ndv : key.degree().getDistinctCount();
        double keyNulls = key == null || key.degree() == null || key.rows() == 0 ? nullFraction
                : key.degree().getNullCount() / (double) Math.max(1, key.degree().getRowCount());
        double probeRows = key == null ? rows : key.rows();
        return new RuntimeFilterStatistics(type, keyNdv, sourceNdv, keyNulls, head,
                new JoinKey(planner, scope, column, probeRows));
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
            return new RuntimeFilterStatistics(type, 0, sourceNdv, 0, Map.of());
        }
        if (rows < 0 || ndv <= rows) {
            return this;
        }
        return new RuntimeFilterStatistics(type, rows, sourceNdv, nullFraction,
                rows < head.size() ? Map.of() : head, joinKey);
    }

    public static RuntimeFilterStatistics from(ColumnRefOperator column, ColumnStatistic basic,
                                               Collection<MultiColumnCombinedStats> groups, double rows) {
        double ndv = basic.isUnknown() ? -1 : basic.getDistinctValuesCount();
        double sourceNdv = basic.isUnknown() ? -1 : basic.getSourceDistinctValuesCount();
        double nulls = basic.isUnknown() || !Double.isFinite(basic.getNullsFraction()) ? 0 : basic.getNullsFraction();
        Map<String, Double> head = Map.of();
        double headMass = 0;
        if (MultiColumnMcvEstimator.isEnabled()) {
            for (MultiColumnCombinedStats group : groups) {
                int position = group.getColumns().indexOf(column);
                if (position < 0 || !group.hasDistribution()) {
                    continue;
                }
                PreparedHead prepared = group.getRuntimeFilterHead(position, column.getType());
                Map<String, Double> candidate = prepared.head;
                boolean single = group.getColumns().size() == 1;
                boolean completeHead = group.getMcvDistribution().getTotalRows() == group.getRowCount();
                double groupNulls = nulls;
                if (group.getNullCounts().size() == group.getColumns().size()) {
                    groupNulls = group.getNullCounts().get(position) / group.getRowCount();
                }
                if (completeHead) {
                    groupNulls = group.getMcvDistribution().getNullRows(position) / group.getRowCount();
                }
                if (prepared.mass > headMass || (head.isEmpty() && (single || completeHead))) {
                    head = candidate;
                    headMass = prepared.mass;
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
        double mass = headMass;
        if (mass > 1 - nulls && mass > 0) {
            double scale = Math.max(0, 1 - nulls) / mass;
            Map<String, Double> scaled = new HashMap<>();
            head.forEach((key, value) -> scaled.put(key, value * scale));
            head = scaled;
        }
        return new RuntimeFilterStatistics(column.getType(), ndv, Double.isFinite(sourceNdv) ? sourceNdv : -1, nulls, head)
                .boundByRows(rows);
    }

    /** A bounded, type-specific index on a planner binding; no query state enters the catalog cache. */
    static final class PreparedHead {
        private final Type type;
        private final Map<String, Double> head;
        private final double mass;

        PreparedHead(MultiColumnCombinedStats group, int position, Type type) {
            this.type = type.clone();
            Map<String, Double> values = new HashMap<>();
            boolean single = group.getColumns().size() == 1;
            boolean complete = group.getMcvDistribution().getTotalRows() == group.getRowCount();
            for (int t = 0; t < group.getMcv().size(); t++) {
                MultiColumnCombinedStats.McvEntry entry = group.getMcv().get(t);
                String value = entry.getValues().get(position);
                if (single || complete) {
                    add(values, type, value, group.getMcvDistribution().getShare(t));
                } else if (entry.hasComponentCounts() && value != null) {
                    values.putIfAbsent(canonical(type, value),
                            entry.getComponentCounts().get(position) / group.getRowCount());
                }
            }
            this.mass = mass(values);
            this.head = Collections.unmodifiableMap(values);
        }

        boolean matches(Type candidate) {
            return type.equals(candidate);
        }
    }

    /** The CORRELATED estimate, see {@link #probePassFraction(RuntimeFilterStatistics, boolean, NdvEstimate)}. */
    public OptionalDouble probePassFraction(RuntimeFilterStatistics probe, boolean nullSafe) {
        return probePassFraction(probe, nullSafe, NdvEstimate.CORRELATED);
    }

    /**
     * Estimates membership, so build-side duplicates do not multiply probe rows. JOIN statistics answer first in
     * every mode, because they were collected from the joined rows themselves. Otherwise the MCV heads and the NDV
     * of the keys estimate the fraction as the mode says.
     */
    public OptionalDouble probePassFraction(RuntimeFilterStatistics probe, boolean nullSafe, NdvEstimate mode) {
        if (!nullSafe && probe != null && joinKey != null && probe.joinKey != null
                && joinKey.planner == probe.joinKey.planner) {
            OptionalDouble rows = joinKey.planner.membership(joinKey.scope, joinKey.column,
                    probe.joinKey.scope, probe.joinKey.column);
            if (rows.isPresent()) {
                return OptionalDouble.of(probe.joinKey.rows == 0 ? 0 : clamp(rows.getAsDouble() / probe.joinKey.rows));
            }
        }
        if (mode == NdvEstimate.OFF || probe == null || ndv < 0 || probe.ndv < 0 || !comparable(type, probe.type)) {
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
        // can match only the probe tail or a matching probe head. CORRELATED takes the remaining probe keys as
        // that domain, which assumes that the build keys are among them, the largest overlap the NDVs allow.
        double buildTailNdv = headMass >= 1 - nullFraction - 1e-9 ? 0 : Math.max(0, ndv - head.size());
        double probeTailNdv = Math.max(0, probe.ndv - probe.head.size());
        double remainingProbeKeys = Math.max(0, probe.ndv - matches);
        double result = matchedMass
                + keyPass(probe, matches, matchedMass, buildTailNdv, remainingProbeKeys, probeTailNdv);
        if (mode == NdvEstimate.INDEPENDENT && sourceNdv >= 0 && probe.sourceNdv >= 0) {
            // INDEPENDENT spreads the build keys outside the build head over the source domain outside it, and
            // the probe tail over the source domain outside the probe head. No estimate can pass more rows than
            // the largest overlap, so we keep the smaller one.
            double domain = Math.max(sourceNdv, probe.sourceNdv);
            result = Math.min(result, matchedMass + keyPass(probe, matches, matchedMass, buildTailNdv,
                    Math.max(remainingProbeKeys, domain - head.size()),
                    Math.max(probeTailNdv, domain - probe.head.size())));
        }
        if (nullSafe && nullFraction > 0) {
            result += probe.nullFraction;
        }
        return OptionalDouble.of(clamp(result));
    }

    // The fraction of probe rows outside the matched heads whose keys the build side has, with the build tail keys
    // spread over headDomain and the build keys outside the probe head spread over tailDomain.
    private double keyPass(RuntimeFilterStatistics probe, int matches, double matchedMass, double buildTailNdv,
                           double headDomain, double tailDomain) {
        double unmatchedProbeKeys = probe.head.size() - matches;
        double headPass = headDomain > 0 ? clamp(buildTailNdv / headDomain) : 0;
        double tailBuildKeys = Math.max(0, head.size() - matches + buildTailNdv - unmatchedProbeKeys * headPass);
        double tailPass = tailDomain > 0 ? clamp(tailBuildKeys / tailDomain) : 0;
        return Math.max(0, probe.headMass - matchedMass) * headPass
                + Math.max(0, 1 - probe.nullFraction - probe.headMass) * tailPass;
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
