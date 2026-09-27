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

import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopNAggregationCostTest {
    private final ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
    private final ColumnRefOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
    private final ColumnRefOperator c = new ColumnRefOperator(3, IntegerType.INT, "c", true);
    private final List<Ordering> asc = List.of(new Ordering(a, true, false));

    private Statistics stats(double aNdv, long groupNdv, Histogram histogram, double nulls) {
        return Statistics.builder().setOutputRowCount(1000000)
                .addColumnStatistic(a, ColumnStatistic.builder().setMinValue(0).setMaxValue(99999)
                        .setDistinctValuesCount(aNdv).setAverageRowSize(4).setNullsFraction(nulls)
                        .setHistogram(histogram).build())
                .addColumnStatistic(b, new ColumnStatistic(0, 99999, 0, 4, 100000))
                .addColumnStatistic(c, new ColumnStatistic(0, 99999, 0, 4, 100000))
                .addMultiColumnStatistics(Set.of(a, b), new MultiColumnCombinedStats(groupNdv)).build();
    }

    @Test
    void smallPeerSetsKeepSortLargePeerSetsKeepOnlyFilter() {
        Statistics rare = stats(100000, 100000, null, 0);
        Statistics many = stats(2, 100000, null, 0);
        assertFalse(TopNAggregationCost.preferFilterOnly(rare, rare, List.of(a, b), asc, 10));
        assertTrue(TopNAggregationCost.preferFilterOnly(many, many, List.of(a, b), asc, 10));
    }

    @Test
    void jointNdvReplacesIndependentProductAndWorksForOrderTuple() {
        Statistics source = stats(1000, 2000, null, 0);
        assertEquals(2000, TopNAggregationCost.distinct(source, Set.of(a, b)));
        source = Statistics.buildFrom(source)
                .addMultiColumnStatistics(Set.of(a, b), new MultiColumnCombinedStats(2))
                .addMultiColumnStatistics(Set.of(a, b, c), new MultiColumnCombinedStats(100000)).build();
        assertTrue(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b, c),
                List.of(new Ordering(a, true, false), new Ordering(b, false, true)), 10));
    }

    @Test
    void leadingMcvDependsOnDirection() {
        Statistics source = stats(100000, 100000,
                new Histogram(List.of(new Bucket(1, 99999, 100000L, 1L)), Map.of("0", 900000L)), 0);
        assertTrue(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b), asc, 10));
        assertFalse(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b),
                List.of(new Ordering(a, false, false)), 10));
    }

    @Test
    void leadingBucketAndNullsAreAlsoConsidered() {
        Statistics source = stats(100000, 100000,
                new Histogram(List.of(new Bucket(0, 0, 900000L, 900000L),
                        new Bucket(1, 99999, 1000000L, 1L)), Map.of()), 0);
        assertTrue(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b), asc, 10));
        source = stats(100000, 100000, null, 0.9);
        assertTrue(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b),
                List.of(new Ordering(a, true, true)), 10));
        assertFalse(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b), asc, 10));
    }

    @Test
    void unknownStatisticsDoNotTurnOffPeerPreservingPushdown() {
        Statistics source = Statistics.builder().setOutputRowCount(1000000)
                .addColumnStatistic(a, ColumnStatistic.unknown()).build();
        assertFalse(TopNAggregationCost.preferFilterOnly(source, source, List.of(a, b), asc, 10));
        assertFalse(TopNAggregationCost.preferFilterOnly(null, null, List.of(a, b), asc, 10));
    }
}
