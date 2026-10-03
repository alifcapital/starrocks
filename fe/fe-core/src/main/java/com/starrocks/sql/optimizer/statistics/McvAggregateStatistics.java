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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Derives frequencies of output groups, independently of the input tuples' multiplicities. */
final class McvAggregateStatistics {
    private McvAggregateStatistics() {
    }

    static Statistics derive(List<ColumnRefOperator> keys, Statistics input, Statistics output) {
        if (!MultiColumnMcvEstimator.isEnabled() || keys.isEmpty()) {
            return output;
        }
        MultiColumnCombinedStats source = null;
        boolean complete = false;
        double bestCoverage = -1;
        for (MultiColumnCombinedStats candidate : input.getMultiColumnCombinedStats().values()) {
            if (!candidate.hasMcv() || !candidate.getColumns().containsAll(keys)) {
                continue;
            }
            double candidateCoverage = coverage(candidate);
            boolean candidateComplete = candidateCoverage == 1;
            if (source == null || (candidateComplete && !complete)
                    || (candidateComplete == complete
                    && (candidate.getColumns().size() < source.getColumns().size()
                    || (candidate.getColumns().size() == source.getColumns().size()
                    && candidateCoverage > bestCoverage)))) {
                source = candidate;
                complete = candidateComplete;
                bestCoverage = candidateCoverage;
            }
        }
        if (source == null) {
            return output;
        }
        List<Integer> positions = new ArrayList<>();
        for (ColumnRefOperator key : keys) {
            positions.add(source.getColumns().indexOf(key));
        }
        Set<List<String>> tuples = new LinkedHashSet<>();
        for (MultiColumnCombinedStats.McvEntry entry : source.getMcv()) {
            List<String> tuple = new ArrayList<>(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                String value = entry.getValues().get(positions.get(i));
                tuple.add(value != null && keys.get(i).getType().isNumericType()
                        ? MultiColumnJoinMcvEstimator.canonical(keys.get(i).getType(), value) : value);
            }
            tuples.add(tuple);
        }
        boolean singleton = keys.size() == 1;
        // NULL outside the retained input head still produces exactly one singleton output group.
        boolean knownNullCounts = source.getNullCounts().size() == source.getColumns().size();
        if (singleton) {
            ColumnStatistic basic = input.getColumnStatistic(keys.get(0));
            boolean hasNull = knownNullCounts ? source.getNullCounts().get(positions.get(0)) > 0
                    : !basic.isUnknown() && basic.getNullsFraction() > 0;
            if (hasNull) {
                tuples.add(Collections.singletonList(null));
            }
        }
        long groups = complete ? tuples.size()
                : Math.max(tuples.size(), (long) Math.ceil(output.getOutputRowCount()));
        List<Map<String, Long>> marginals = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            marginals.add(new HashMap<>());
        }
        for (List<String> tuple : tuples) {
            for (int i = 0; i < keys.size(); i++) {
                marginals.get(i).merge(tuple.get(i), 1L, Long::sum);
            }
        }
        List<MultiColumnCombinedStats.McvEntry> head = new ArrayList<>();
        for (List<String> tuple : tuples) {
            List<Long> componentCounts = new ArrayList<>();
            if (complete || singleton) {
                for (int i = 0; i < keys.size(); i++) {
                    componentCounts.add(marginals.get(i).get(tuple.get(i)));
                }
            }
            // Input marginal row counts cannot tell how many distinct output tuples contain a value.
            head.add(new MultiColumnCombinedStats.McvEntry(tuple, 1, componentCounts));
        }
        List<Long> nullCounts = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            if (complete || (singleton && marginals.get(i).containsKey(null))) {
                nullCounts.add(marginals.get(i).getOrDefault(null, 0L));
            } else if (knownNullCounts && source.getNullCounts().get(positions.get(i)) == 0) {
                nullCounts.add(0L);
            } else if (!keys.get(i).isNullable() || (!input.getColumnStatistic(keys.get(i)).isUnknown()
                    && input.getColumnStatistic(keys.get(i)).getNullsFraction() == 0)) {
                nullCounts.add(0L);
            } else {
                nullCounts.clear();
                break;
            }
        }
        Statistics.Builder builder = Statistics.buildFrom(output).addMultiColumnStatistics(Set.copyOf(keys),
                new MultiColumnCombinedStats(groups, groups, keys, head, nullCounts));
        for (int i = 0; i < keys.size(); i++) {
            ColumnRefOperator key = keys.get(i);
            ColumnStatistic.Builder column = ColumnStatistic.buildFrom(output.getColumnStatistic(key)).setHistogram(null);
            if (complete) {
                column.setDistinctValuesCount(marginals.get(i).size() - (marginals.get(i).containsKey(null) ? 1 : 0));
                column.setNullsFraction(marginals.get(i).getOrDefault(null, 0L) / (double) groups);
            } else if (singleton) {
                boolean hasNull = marginals.get(i).containsKey(null);
                column.setDistinctValuesCount(groups - (hasNull ? 1 : 0));
                if (!nullCounts.isEmpty()) {
                    column.setNullsFraction(nullCounts.get(0) / (double) groups);
                }
            }
            builder.addColumnStatistic(key, column.build());
        }
        return builder.build();
    }

    private static double coverage(MultiColumnCombinedStats group) {
        return group.getMcvDistribution().getTotalRows()
                / group.getRowCount();
    }

}
