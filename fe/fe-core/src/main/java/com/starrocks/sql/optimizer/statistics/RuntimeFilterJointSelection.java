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
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

/** Selects independently admitted component filters by their incremental rejection on a joint probe head. */
public final class RuntimeFilterJointSelection {
    public record Key(ScalarOperator probe, RuntimeFilterStatistics build, boolean nullSafe) {
    }

    private RuntimeFilterJointSelection() {
    }

    public static BitSet select(List<Key> keys, Collection<MultiColumnCombinedStats> groups, double minRejection) {
        BitSet keep = new BitSet(keys.size());
        keep.set(0, keys.size());
        if (keys.size() < 2 || !MultiColumnMcvEstimator.isEnabled()) {
            return keep;
        }
        MultiColumnCombinedStats best = null;
        List<Integer> positions = List.of();
        double bestMass = -1;
        String bestOrder = "";
        for (MultiColumnCombinedStats group : groups) {
            if (!group.hasMcv() || !Double.isFinite(group.getRowCount()) || group.getRowCount() <= 0) {
                continue;
            }
            List<Integer> covered = new ArrayList<>();
            for (int k = 0; k < keys.size(); k++) {
                Key key = keys.get(k);
                ColumnRefOperator source = McvCastStatistics.sourceColumn(key.probe());
                if (source != null && key.build() != null && group.getColumns().contains(source)) {
                    covered.add(k);
                }
            }
            if (covered.size() < 2) {
                continue;
            }
            double mass = group.getMcvDistribution().getTotalRows()
                    / group.getRowCount();
            String order = group.getColumns().stream().map(c -> c == null ? "_" : Integer.toString(c.getId()))
                    .collect(java.util.stream.Collectors.joining(","));
            if (covered.size() > positions.size()
                    || (covered.size() == positions.size() && (mass > bestMass
                    || (mass == bestMass && order.compareTo(bestOrder) < 0)))) {
                best = group;
                positions = covered;
                bestMass = mass;
                bestOrder = order;
            }
        }
        if (best == null || bestMass > 1 + 1e-9) {
            return keep;
        }
        int tuples = best.getMcv().size();
        double[] weights = new double[tuples];
        for (int t = 0; t < tuples; t++) {
            weights[t] = best.getMcvDistribution().getShare(t);
        }
        // -1 is unknown. Never multiply marginal probabilities to invent tuple independence.
        byte[][] membership = new byte[positions.size()][tuples];
        for (int k = 0; k < positions.size(); k++) {
            Key key = keys.get(positions.get(k));
            ColumnRefOperator source = McvCastStatistics.sourceColumn(key.probe());
            int column = best.getColumns().indexOf(source);
            for (int t = 0; t < tuples; t++) {
                String raw = best.getMcv().get(t).getValues().get(column);
                OptionalDouble pass;
                if (key.probe() instanceof ColumnRefOperator) {
                    pass = key.build().knownMembership(key.probe().getType(), raw, key.nullSafe());
                } else {
                    Optional<ConstantOperator> value = MultiColumnMcvEstimator.evaluate(key.probe(), source, raw);
                    pass = value.isEmpty() ? OptionalDouble.empty() : key.build().knownMembership(
                            key.probe().getType(), value.get().isNull() ? null
                                    : MultiColumnMcvEstimator.constantText(value.get()), key.nullSafe());
                }
                membership[k][t] = pass.isEmpty() ? -1 : (byte) pass.getAsDouble();
            }
        }
        boolean[] certainlyAlive = new boolean[tuples];
        boolean[] possiblyAlive = new boolean[tuples];
        Arrays.fill(certainlyAlive, true);
        Arrays.fill(possiblyAlive, true);
        BitSet remaining = new BitSet(positions.size());
        remaining.set(0, positions.size());
        double tail = Math.max(0, 1 - bestMass);
        boolean first = true;
        while (!remaining.isEmpty()) {
            int chosen = -1;
            double mostCertainRejection = -1;
            for (int k = remaining.nextSetBit(0); k >= 0; k = remaining.nextSetBit(k + 1)) {
                double certainPass = 0;
                double possibleRejection = tail;
                double certainRejection = 0;
                for (int t = 0; t < tuples; t++) {
                    if (certainlyAlive[t] && membership[k][t] == 1) {
                        certainPass += weights[t];
                    }
                    if (possiblyAlive[t] && membership[k][t] != 1) {
                        possibleRejection += weights[t];
                    }
                    if (certainlyAlive[t] && membership[k][t] == 0) {
                        certainRejection += weights[t];
                    }
                }
                // Give all unseen mass to the candidate's benefit. Discard it only if even that
                // cannot meet the threshold on rows surviving the filters already selected.
                double denominator = certainPass + possibleRejection;
                double maximumRejection = denominator > 0 ? possibleRejection / denominator : 0;
                if (!first && (maximumRejection == 0 || maximumRejection + 1e-9 < minRejection)) {
                    continue;
                } else if (certainRejection > mostCertainRejection) {
                    mostCertainRejection = certainRejection;
                    chosen = k;
                }
            }
            if (chosen < 0) {
                for (int k = remaining.nextSetBit(0); k >= 0; k = remaining.nextSetBit(k + 1)) {
                    keep.clear(positions.get(k));
                }
                break;
            }
            remaining.clear(chosen);
            for (int t = 0; t < tuples; t++) {
                certainlyAlive[t] &= membership[chosen][t] == 1;
                possiblyAlive[t] &= membership[chosen][t] != 0;
            }
            first = false;
        }
        return keep;
    }
}
