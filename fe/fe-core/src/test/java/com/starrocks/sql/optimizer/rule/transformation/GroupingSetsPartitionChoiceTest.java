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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GroupingSetsPartitionChoiceTest {
    @Test
    public void testChoiceMatchesStableSort() {
        // The repeat input is shuffled by the group-by column with the largest NDV. We expect the first such
        // column in group-by order when NDVs tie, columns with unknown statistics to be skipped, and NaN and
        // signed zero NDVs to be ordered as Double.compare orders them, as a stable descending sort would do.
        Random random = new Random(20261002);
        double[] values = {-0.0, 0.0, 1, 10, 100, Double.POSITIVE_INFINITY, Double.NaN};
        for (int trial = 0; trial < 200; trial++) {
            List<ColumnRefOperator> refs = new ArrayList<>();
            Statistics.Builder builder = Statistics.builder().setOutputRowCount(1000);
            int count = random.nextInt(65);
            for (int i = 0; i < count; i++) {
                ColumnRefOperator ref = new ColumnRefOperator(i + 1, IntegerType.INT, "c" + i, true);
                refs.add(ref);
                ColumnStatistic statistic = random.nextInt(4) == 0 ? ColumnStatistic.unknown()
                        : new ColumnStatistic(0, 100, 0, 8, values[random.nextInt(values.length)]);
                builder.addColumnStatistic(ref, statistic);
            }
            Statistics statistics = builder.build();
            List<ColumnRefOperator> original = new ArrayList<>(refs);
            ColumnRefOperator expected = refs.stream()
                    .filter(ref -> !statistics.getColumnStatistic(ref).isUnknown())
                    .sorted((a, b) -> Double.compare(statistics.getColumnStatistic(b).getDistinctValuesCount(),
                            statistics.getColumnStatistic(a).getDistinctValuesCount()))
                    .findFirst().orElse(null);
            Assertions.assertSame(expected, PushDownAggregateGroupingSetsRule.findLargestNdvColumn(refs, statistics));
            Assertions.assertEquals(original, refs);
        }
    }

    @Test
    public void testEmptyUnknownAndFirstTie() {
        ColumnRefOperator first = new ColumnRefOperator(1, IntegerType.INT, "first", true);
        ColumnRefOperator second = new ColumnRefOperator(2, IntegerType.INT, "second", true);
        Statistics unknown = Statistics.builder().setOutputRowCount(100)
                .addColumnStatistic(first, ColumnStatistic.unknown()).build();
        Assertions.assertNull(PushDownAggregateGroupingSetsRule.findLargestNdvColumn(List.of(), unknown));
        Assertions.assertNull(PushDownAggregateGroupingSetsRule.findLargestNdvColumn(List.of(first), unknown));
        ColumnStatistic statistic = new ColumnStatistic(0, 100, 0, 8, 100);
        Statistics equal = Statistics.builder().setOutputRowCount(100)
                .addColumnStatistic(first, statistic).addColumnStatistic(second, statistic).build();
        Assertions.assertSame(first,
                PushDownAggregateGroupingSetsRule.findLargestNdvColumn(List.of(first, second), equal));
        Assertions.assertSame(second,
                PushDownAggregateGroupingSetsRule.findLargestNdvColumn(List.of(second, first), equal));
    }
}
