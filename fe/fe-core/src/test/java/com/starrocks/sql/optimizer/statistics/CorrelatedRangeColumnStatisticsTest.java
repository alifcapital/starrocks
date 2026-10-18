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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

// Ranges on several columns that the estimate with an MCV list consumes at once: each column takes only the ranges on
// it, so the row count of independent columns does not bound the distinct values of a correlated one.
public class CorrelatedRangeColumnStatisticsTest {
    @BeforeEach
    public void setUp() {
        new ConnectContext().setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testRangesOnCorrelatedColumnsKeepTheDistinctValuesOfEach() {
        // created and closed are nearly equal. The MCV list of the pair holds the 100 newest pairs with 500 rows
        // each, all of them in both ranges, so the estimate with the list keeps about 6% of the rows, not the 1% that
        // independent selectivities give. Each column keeps the distinct values of its own range, bounded by these
        // rows. The list holds half of the rows of each range: if it held all of them, a range on one column would
        // leave only the pairs of the list, and the column would rightly keep their 100 values.
        ColumnRefOperator created = new ColumnRefOperator(4, IntegerType.BIGINT, "created", true);
        ColumnRefOperator closed = new ColumnRefOperator(5, IntegerType.BIGINT, "closed", true);
        List<MultiColumnCombinedStats.McvEntry> pairs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String value = Integer.toString(999_900 + i);
            pairs.add(new MultiColumnCombinedStats.McvEntry(List.of(value, value), 500, List.of(500L, 500L)));
        }
        Statistics statistics = Statistics.builder().setOutputRowCount(1_000_000)
                .addColumnStatistic(created, new ColumnStatistic(0, 1_000_000, 0, 8, 1_000_000))
                .addColumnStatistic(closed, new ColumnStatistic(0, 1_000_000, 0, 8, 1_000_000))
                .addMultiColumnStatistics(Set.of(created, closed), new MultiColumnCombinedStats(1_000_000,
                        1_000_000, List.of(created, closed), pairs, List.of(0L, 0L)))
                .build();
        ScalarOperator ranges = Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.GE, created, ConstantOperator.createBigint(900_000)),
                new BinaryPredicateOperator(BinaryType.GE, closed, ConstantOperator.createBigint(900_000)));
        Statistics filtered = PredicateStatisticsCalculator.statisticsCalculate(ranges, statistics);
        Assertions.assertTrue(filtered.getOutputRowCount() > 50_000, String.valueOf(filtered.getOutputRowCount()));
        Assertions.assertTrue(filtered.getColumnStatistic(created).getDistinctValuesCount() > 50_000,
                filtered.getColumnStatistic(created).toString());
    }
}
