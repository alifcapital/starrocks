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

import com.starrocks.statistic.JoinStatisticsDefinition;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalDouble;

/** Builds one entropy problem for a query subgraph, including bag multiplicities and RF projection. */
public final class JoinStatisticsEstimate {
    public static final class Selection {
        private final int[] slices;
        private final long residualRows;
        private final double rowLimit;
        private final boolean exactRows;
        private int[] extrapolatedSlices = new int[0];
        private double extrapolatedWeight;
        private java.util.Map<JoinStatisticsBasis, JoinStatisticsExtrapolation> extrapolations;
        private double[] weights;

        Selection(int[] slices, long residualRows, double rowLimit, int[] remaining, double weight) {
            this(slices, residualRows, rowLimit, false);
            this.extrapolatedSlices = remaining.clone();
            this.extrapolatedWeight = weight;
        }

        boolean isExtrapolated() {
            return extrapolatedSlices.length > 0;
        }

        JoinStatisticsCorrelation.Slice project(JoinStatisticsBasis basis, int side,
                JoinStatisticsCorrelation.Evaluation evaluation, int arity, int power, boolean presence) {
            if (!isExtrapolated()) {
                return evaluation.select(basis, side, slices).project(arity, power, presence);
            }
            if (extrapolations == null) {
                extrapolations = new java.util.IdentityHashMap<>();
            }
            var distribution = extrapolations.computeIfAbsent(basis, ignored -> {
                var known = evaluation.select(basis, side, slices);
                var remaining = evaluation.select(basis, side, extrapolatedSlices);
                return new JoinStatisticsExtrapolation(known, remaining, extrapolatedWeight);
            });
            return distribution.project(arity, power, presence);
        }

        double[] sliceWeights(int size) {
            if (weights != null) {
                return weights;
            }
            weights = new double[size];
            for (int id : slices) {
                weights[id] = 1;
            }
            for (int id : extrapolatedSlices) {
                weights[id] += extrapolatedWeight;
            }
            return weights;
        }

        public Selection(int[] slices, long residualRows, double rowLimit) {
            this(slices, residualRows, rowLimit, false);
        }

        Selection(int[] slices, long residualRows, double rowLimit, boolean exactRows) {
            if (residualRows < 0 || !Double.isFinite(rowLimit) || rowLimit < 0) {
                throw new IllegalArgumentException("Invalid JOIN statistics selection");
            }
            this.slices = slices.clone();
            Arrays.sort(this.slices);
            for (int i = 0; i < this.slices.length; i++) {
                if (this.slices[i] < 0 || (i > 0 && this.slices[i] == this.slices[i - 1])) {
                    throw new IllegalArgumentException("Invalid JOIN statistics slice selection");
                }
            }
            this.residualRows = residualRows;
            this.rowLimit = rowLimit;
            this.exactRows = exactRows;
        }

        JoinStatisticsPlanner.KeyStatistics keyStatistics(JoinStatisticsData.Source source, int domain) {
            if (!exactRows) {
                return null;
            }
            DegreeStatistics degree = slices.length == 1 ? source.getDegrees().get(domain).get(slices[0]) : null;
            return new JoinStatisticsPlanner.KeyStatistics(rowLimit, degree);
        }

        long nullRows(JoinStatisticsData.Source source, int domain) {
            long result = 0;
            for (int slice : slices) {
                result = Math.addExact(result, source.getDegrees().get(domain).get(slice).getNullCount());
            }
            return result;
        }

        long[] head(JoinStatisticsBasis basis, int side) {
            long[] result = new long[basis.getSlices(side).get(0).getHead().size()];
            for (int slice : slices) {
                basis.getSlices(side).get(slice).getHead().addTo(result);
            }
            return result;
        }

        boolean hasExactRows() {
            return exactRows;
        }

        double rowLimit() {
            return rowLimit;
        }

        double knownMatches(JoinStatisticsBasis basis, int side, Selection support, int supportSide,
                            JoinStatisticsCorrelation.Evaluation evaluation) {
            double matched = evaluation.select(basis, side, slices).getHead().product(
                    evaluation.select(basis, supportSide, support.slices).getHead(), false, true, 1);
            // A single build slice has exact membership in the stored pair matrix, including the tail.
            // With several build slices, memberships may overlap: subtract only the known head union.
            if (support.slices.length == 1) {
                OptionalDouble pair = side < supportSide
                        ? basis.pairEstimate(side, supportSide, slices, support.slices, 2)
                        : basis.pairEstimate(supportSide, side, support.slices, slices, 1);
                if (pair.isPresent()) {
                    matched = pair.getAsDouble();
                }
            }
            return Math.min(rowLimit, matched);
        }

        double maximumFrequency(JoinStatisticsBasis basis, int side, Selection support, int supportSide,
                                JoinStatisticsCorrelation.Evaluation evaluation) {
            if (isExtrapolated()) {
                return rowLimit; // Unknown rows cannot establish uniqueness or a hard fanout bound.
            }
            var frequencies = evaluation.select(basis, side, slices);
            double maximum = support != null && support.residualRows == 0
                    ? frequencies.maximumOnSupport(evaluation.select(basis, supportSide, support.slices))
                    : frequencies.maximumFrequencyBound();
            return maximum + residualRows;
        }
    }

    private JoinStatisticsEstimate() {
    }

    /** Source and output masks refer to definition source positions, not to entropy attribute positions. */
    public static OptionalDouble estimate(JoinStatisticsDefinition definition, JoinStatisticsData data,
                                           List<Selection> selections, int sourceMask, int outputMask, long budgetNanos) {
        int[] domains = new int[definition.getDomains().size()];
        for (int d = 0; d < domains.length; d++) {
            for (int source : definition.getDomains().get(d).getColumns().keySet()) {
                domains[d] |= (1 << source) & sourceMask;
            }
        }
        return estimate(definition, data, selections, sourceMask, outputMask, domains, budgetNanos);
    }

    public static OptionalDouble estimate(JoinStatisticsDefinition definition, JoinStatisticsData data,
                                           List<Selection> selections, int sourceMask, int outputMask,
                                           int[] domainSources, long budgetNanos) {
        return estimate(definition, data, selections, sourceMask, outputMask, domainSources, budgetNanos,
                new JoinStatisticsCorrelation.Evaluation());
    }

    static OptionalDouble estimate(JoinStatisticsDefinition definition, JoinStatisticsData data,
                                   List<Selection> selections, int sourceMask, int outputMask,
                                   int[] domainSources, long budgetNanos, JoinStatisticsCorrelation.Evaluation evaluation) {
        if (outputMask == 0) {
            throw new IllegalArgumentException("Empty JOIN output mask");
        }
        long start = System.nanoTime();
        Prepared prepared = prepare(definition, data, selections, sourceMask, outputMask, domainSources,
                budgetNanos, evaluation, true);
        return prepared == null ? OptionalDouble.empty()
                : prepared.model.estimate(prepared.objective, budgetNanos - (System.nanoTime() - start));
    }

    record Prepared(JoinStatisticsEntropyModel model, int[] rows, int[] keys, int objective) { }

    static Prepared prepare(JoinStatisticsDefinition definition, JoinStatisticsData data,
                            List<Selection> selections, int sourceMask, int outputMask, int[] domainSources,
                            long budgetNanos, JoinStatisticsCorrelation.Evaluation evaluation, boolean reduceCommonKey) {
        if (selections.size() != data.getSources().size() || (outputMask & sourceMask) != outputMask) {
            throw new IllegalArgumentException("Invalid JOIN statistics subgraph");
        }
        long start = System.nanoTime();
        int domains = definition.getDomains().size();
        int[] relations = new int[selections.size()];
        boolean[] active = new boolean[domains];
        for (int domain = 0; domain < domains; domain++) {
            active[domain] = Integer.bitCount(domainSources[domain] & sourceMask) >= 2;
        }
        int attributes = 0;
        int[] keyMasks = new int[domains];
        for (int domain = 0; domain < domains; domain++) {
            if (active[domain]) {
                keyMasks[domain] = 1 << attributes++;
            }
        }
        boolean commonKeyStar = reduceCommonKey && attributes == 1;
        for (int domain = 0; domain < domains; domain++) {
            if (active[domain] && (domainSources[domain] & sourceMask) != sourceMask) {
                commonKeyStar = false;
            }
        }
        int[] rowMasks = new int[selections.size()];
        for (int source = 0; source < selections.size(); source++) {
            if ((sourceMask & (1 << source)) != 0) {
                rowMasks[source] = 1 << attributes++;
            }
        }
        // A pair selected from a four-table definition must not pay for unused entropy attributes.
        JoinStatisticsEntropyModel model = commonKeyStar ? JoinStatisticsEntropyModel.commonKeyStar(attributes)
                : new JoinStatisticsEntropyModel(attributes);
        int objective = 0;
        for (int source = 0; source < selections.size(); source++) {
            if ((sourceMask & (1 << source)) == 0) {
                continue;
            }
            Selection selected = selections.get(source);
            if (selected == null) {
                return null;
            }
            int row = rowMasks[source];
            relations[source] = row;
            for (int domain = 0; domain < domains; domain++) {
                if (active[domain] && (domainSources[domain] & (1 << source)) != 0) {
                    relations[source] |= keyMasks[domain];
                }
            }
            model.addFunctionalDependency(row, relations[source]);
            model.addCardinality(relations[source], Math.ceil(selected.rowLimit));
            if ((outputMask & (1 << source)) != 0) {
                objective |= relations[source];
            }
            for (int domain = 0; domain < domains; domain++) {
                if (active[domain] && (relations[source] & keyMasks[domain]) != 0) {
                    DegreeStatistics degree = union(data.getSources().get(source), domain, selected);
                    model.addDegree(keyMasks[domain], relations[source], degree);
                }
            }
        }
        for (JoinStatisticsBasis basis : data.getBases()) {
            if (!active[basis.getDomain()]) {
                continue;
            }
            int participants = basis.getSources().size();
            int key = keyMasks[basis.getDomain()];
            for (int side = 0; side < participants; side++) {
                int source = basis.getSources().get(side);
                if ((domainSources[basis.getDomain()] & sourceMask & (1 << source)) != 0) {
                    Selection selection = selections.get(source);
                    double maximum = selection.maximumFrequency(basis, side, null, 0, evaluation);
                    for (int other = 0; other < participants; other++) {
                        int otherSource = basis.getSources().get(other);
                        if (other != side && (domainSources[basis.getDomain()] & sourceMask & (1 << otherSource)) != 0
                                && selections.get(otherSource).residualRows == 0) {
                            maximum = Math.min(maximum, selection.maximumFrequency(basis, side,
                                    selections.get(otherSource), other, evaluation));
                        }
                    }
                    model.addMaximumFrequency(key, relations[source], maximum == 0 ? 0 : Math.max(1, maximum));
                }
            }
            for (int subset = 1; subset < (1 << participants); subset++) {
                if (Integer.bitCount(subset) < 2) {
                    continue;
                }
                List<Integer> sides = new java.util.ArrayList<>();
                List<Integer> group = new java.util.ArrayList<>();
                boolean applicable = true;
                int projected = 0;
                for (int side = 0; side < participants; side++) {
                    if ((subset & (1 << side)) == 0) {
                        continue;
                    }
                    int source = basis.getSources().get(side);
                    applicable &= (domainSources[basis.getDomain()] & sourceMask & (1 << source)) != 0;
                    if ((outputMask & (1 << source)) == 0) {
                        projected |= 1 << group.size();
                    }
                    sides.add(side);
                    group.add(source);
                }
                if (!applicable) {
                    continue;
                }
                int size = group.size();
                // A pair's saved membership products also constrain INNER JOIN: they bound each
                // participating source and the common key support. For larger subsets retain only
                // the requested projection; never enumerate/persist all multi-table roles.
                boolean extrapolated = group.stream().anyMatch(source -> selections.get(source).isExtrapolated());
                // Expectations of cardinality and support describe random outcomes, not simultaneous
                // hard constraints of one realized relation. Use only the requested first-order projection.
                int[] roles = extrapolated ? new int[] {projected} : size == 2 ? new int[] {0, 1, 2, 3}
                        : projected == 0 ? new int[] {0} : new int[] {0, projected};
                for (int presence : roles) {
                    for (int power = 1; power <= 3; power++) {
                        if (extrapolated && power != 1) {
                            continue; // Expected products are not higher moments of a sampled distribution.
                        }
                        if (presence != 0 && presence != projected && power != 1) {
                            continue;
                        }
                        if (System.nanoTime() - start >= budgetNanos || Thread.currentThread().isInterrupted()) {
                            return null;
                        }
                        List<List<JoinStatisticsCorrelation.Slice>> rows = new java.util.ArrayList<>();
                        JoinStatisticsCorrelation.Slice[] selected = new JoinStatisticsCorrelation.Slice[size];
                        int[] keys = new int[size];
                        int[] relationMasks = new int[size];
                        int[] powers = new int[size];
                        Arrays.fill(keys, key);
                        Arrays.fill(powers, power);
                        for (int side = 0; side < size; side++) {
                            int source = group.get(side);
                            boolean membership = (presence & (1 << side)) != 0;
                            relationMasks[side] = membership ? key : relations[source];
                            selected[side] = selections.get(source).project(basis, sides.get(side), evaluation,
                                    size, power, membership);
                            rows.add(List.of(selected[side]));
                        }
                        var distribution = new JoinStatisticsCorrelation(rows, presence, power);
                        var correlation = new JoinStatisticsData.Correlation(basis.getDomain(), group, distribution);
                        double bound = evaluation.estimateShared(distribution, selected);
                        if (size == 2 && power == 1) {
                            Selection left = selections.get(group.get(0));
                            Selection right = selections.get(group.get(1));
                            OptionalDouble pair = extrapolated
                                    ? ((presence & 1) == 0 || !left.isExtrapolated())
                                            && ((presence & 2) == 0 || !right.isExtrapolated())
                                            ? basis.weightedPairEstimate(sides.get(0), sides.get(1),
                                            left.sliceWeights(basis.getSlices(sides.get(0)).size()),
                                            right.sliceWeights(basis.getSlices(sides.get(1)).size()), presence,
                                            budgetNanos - (System.nanoTime() - start)) : OptionalDouble.empty()
                                    : basis.pairEstimate(sides.get(0), sides.get(1), left.slices, right.slices, presence);
                            if (pair.isPresent()) {
                                bound = Math.min(bound, pair.getAsDouble());
                            }
                        }
                        bound += residualBound(data, correlation, selections);
                        if (Double.isFinite(bound)) {
                            model.addCorrelation(key, keys, relationMasks, powers, bound == 0 ? 0 : Math.max(1, bound));
                        }
                    }
                }
            }
        }
        for (JoinStatisticsData.IntraCorrelation intra : data.getIntraCorrelations()) {
            if ((sourceMask & (1 << intra.getSource())) == 0 || !active[intra.getLeftDomain()]
                    || !active[intra.getRightDomain()]
                    || (relations[intra.getSource()] & keyMasks[intra.getLeftDomain()]) == 0
                    || (relations[intra.getSource()] & keyMasks[intra.getRightDomain()]) == 0) {
                continue;
            }
            Selection selected = selections.get(intra.getSource());
            if (selected.residualRows != 0) {
                continue;
            }
            int left = keyMasks[intra.getLeftDomain()];
            int right = keyMasks[intra.getRightDomain()];
            double support = 0;
            for (int slice : selected.slices) {
                support += intra.getSupport(slice);
            }
            model.addCardinality(left | right, support);
            // Marginal frequencies gain cross terms when slices are merged; per-slice moments cannot be added.
            if (selected.slices.length == 1) {
                for (int power = 1; power <= 3; power++) {
                    model.addCorrelation(left | right, new int[] {left, right},
                            new int[] {relations[intra.getSource()], relations[intra.getSource()]},
                            new int[] {power, power}, intra.getMoment(selected.slices[0], power));
                }
            }
        }
        return new Prepared(model, rowMasks, keyMasks, objective);
    }

    private static DegreeStatistics union(JoinStatisticsData.Source source, int domain, Selection selected) {
        List<DegreeStatistics> degrees = source.getDegrees().get(domain);
        long rows = selected.residualRows;
        long nulls = 0;
        long distinct = selected.residualRows;
        long maximum = selected.residualRows;
        double[] roots = new double[10];
        Arrays.fill(roots, selected.residualRows);
        for (int slice : selected.slices) {
            DegreeStatistics degree = degrees.get(slice);
            rows = Math.addExact(rows, degree.getRowCount());
            nulls = Math.addExact(nulls, degree.getNullCount());
            distinct = Math.addExact(distinct, degree.getDistinctCount());
            maximum = Math.addExact(maximum, degree.getMaximumFrequency());
            for (int power = 1; power <= 10; power++) {
                roots[power - 1] += Math.pow(degree.getMoment(power), 1.0 / power);
            }
        }
        double[] moments = new double[10];
        moments[0] = rows - nulls;
        for (int power = 2; power <= 10; power++) {
            moments[power - 1] = Math.max(moments[power - 2], Math.pow(roots[power - 1], power));
        }
        return new DegreeStatistics(rows, nulls, distinct, maximum, moments);
    }

    private static double residualBound(JoinStatisticsData data, JoinStatisticsData.Correlation correlation,
                                         List<Selection> selections) {
        if (correlation.getSources().stream().allMatch(source ->
                selections.get(source).residualRows == 0 || selections.get(source).isExtrapolated())) {
            return 0;
        }
        if (correlation.getSources().stream().anyMatch(source -> selections.get(source).isExtrapolated())) {
            return Double.POSITIVE_INFINITY; // The early return above handled fully modeled residuals.
        }
        int size = correlation.getSources().size();
        JoinStatisticsCorrelation distribution = correlation.getDistribution();
        double[] covered = new double[size];
        double[] residual = new double[size];
        boolean any = false;
        for (int side = 0; side < size; side++) {
            int source = correlation.getSources().get(side);
            Selection selected = selections.get(source);
            boolean presence = (distribution.getPresenceMask() & (1 << side)) != 0;
            long unknown = selected.isExtrapolated() ? 0 : selected.residualRows;
            any |= unknown > 0;
            int exponent = presence ? 1 : distribution.getPower();
            int normPower = presence ? size : exponent * size;
            double coveredRoot = 0;
            for (int slice : selected.slices) {
                DegreeStatistics degree = data.getSources().get(source).getDegrees().get(correlation.getDomain()).get(slice);
                double moment = presence ? degree.getDistinctCount() : normPower <= DegreeStatistics.MOMENT_COUNT
                        ? degree.getMoment(normPower)
                        : degree.getMoment(1) * Math.pow(degree.getMaximumFrequency(), normPower - 1);
                coveredRoot += Math.pow(moment, 1.0 / normPower);
            }
            double missingRoot = presence ? Math.pow(unknown, 1.0 / size) : unknown;
            covered[side] = Math.pow(coveredRoot, exponent);
            // Expand (covered + missing)^p, including same-key cross terms. Subtracting two large
            // powered norms loses small residuals; the positive binomial terms avoid cancellation.
            residual[side] = missingRoot;
            if (exponent == 2) {
                residual[side] = 2 * coveredRoot * missingRoot + missingRoot * missingRoot;
            } else if (exponent == 3) {
                residual[side] = 3 * coveredRoot * coveredRoot * missingRoot
                        + 3 * coveredRoot * missingRoot * missingRoot + missingRoot * missingRoot * missingRoot;
            }
        }
        if (!any) {
            return 0;
        }
        double bound = 0;
        for (int subset = 1; subset < (1 << size); subset++) {
            double product = 1;
            for (int side = 0; side < size; side++) {
                product *= (subset & (1 << side)) == 0 ? covered[side] : residual[side];
            }
            bound += product;
        }
        return bound;
    }
}
