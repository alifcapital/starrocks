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

import com.starrocks.catalog.Column;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// After a GROUP BY a key comes once per group, so a filter on the key above the aggregate counts the distinct values
// of the key, not its rows.
public class GroupKeyHistogramTest {
    private static final ColumnRefOperator DAYS = new ColumnRefOperator(1, IntegerType.INT, "date", true);

    @BeforeEach
    public void setUp() {
        new ConnectContext().setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    // 100 days with 100 rows up to day 20000, 300 days with 300 rows up to day 20400, and ten days of 2026 with 60
    // rows each
    private static Histogram days() {
        Map<String, Long> mcv = new HashMap<>();
        for (int day = 20500; day < 20510; day++) {
            mcv.put(Integer.toString(day), 60L);
        }
        return new Histogram(List.of(new Bucket(17197, 20000, 100L, 1L, 100L),
                new Bucket(20001, 20400, 400L, 1L, 300L)), mcv);
    }

    private static Statistics.Builder input(Histogram histogram) {
        return Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(DAYS, ColumnStatistic.builder().setMinValue(17197).setMaxValue(20509)
                        .setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(410)
                        .setHistogram(histogram).build());
    }

    @Test
    public void testFilterAboveTheAggregateCountsDistinctValues() {
        Statistics input = input(days()).build();
        Map<ColumnRefOperator, ColumnStatistic> keys = new HashMap<>();
        double groups = StatisticsCalculator.computeGroupByStatistics(List.of(DAYS), input, keys);
        Assertions.assertEquals(410, groups, 1);
        Histogram distinct = keys.get(DAYS).getHistogram();
        Assertions.assertEquals(410, distinct.getTotalRows());
        Assertions.assertEquals(1L, distinct.getMCV().get("20500"));

        // date >= 20089 keeps the part of the second bucket from day 20089, 234 of its 300 days, and the ten days of
        // the list
        Statistics aggregated = Statistics.builder().setOutputRowCount(groups).addColumnStatistic(DAYS, keys.get(DAYS))
                .build();
        ScalarOperator from2025 = new BinaryPredicateOperator(BinaryType.GE, DAYS, ConstantOperator.createInt(20089));
        double expected = 300.0 * (20400 - 20089) / (20400 - 20001) + 10;
        Assertions.assertEquals(expected,
                PredicateStatisticsCalculator.statisticsCalculate(from2025, aggregated).getOutputRowCount(),
                expected * 0.02);
    }

    @Test
    public void testKeyOfSeveralKeysKeepsItsHistogram() {
        // With an account in every group, a day stands in as many groups as it has rows, so the share of the days is
        // not the share of the groups; the key keeps the histogram of its input
        ColumnRefOperator account = new ColumnRefOperator(2, IntegerType.BIGINT, "account_id", true);
        Histogram histogram = days();
        Statistics input = input(histogram).addColumnStatistic(account, ColumnStatistic.builder().setMinValue(1)
                .setMaxValue(1000).setNullsFraction(0).setAverageRowSize(8).setDistinctValuesCount(1000).build()).build();
        Map<ColumnRefOperator, ColumnStatistic> keys = new HashMap<>();
        StatisticsCalculator.computeGroupByStatistics(List.of(DAYS, account), input, keys);
        Assertions.assertSame(histogram, keys.get(DAYS).getHistogram());
    }

    @Test
    public void testBucketsWithoutTheirDistinctValuesHoldTheDaysOfTheirRange() {
        // Every day is there, while the rows are skewed: 100,000 rows on the 2,804 days to day 20000, 900,000 rows on
        // the 400 days to day 20400. The buckets have no count of distinct values, as the buckets a range filter
        // leaves. date >= 20089 above the GROUP BY keeps 311 of the 400 recent days, not a share of the rows.
        Histogram histogram = new Histogram(List.of(new Bucket(17197, 20000, 100_000L, 0L),
                new Bucket(20001, 20400, 1_000_000L, 0L)), Map.of());
        Statistics input = Statistics.builder().setOutputRowCount(1_000_000)
                .addColumnStatistic(DAYS, ColumnStatistic.builder().setMinValue(17197).setMaxValue(20400)
                        .setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(3204)
                        .setHistogram(histogram).build())
                .build();
        Map<ColumnRefOperator, ColumnStatistic> keys = new HashMap<>();
        double groups = StatisticsCalculator.computeGroupByStatistics(List.of(DAYS), input, keys);
        Statistics aggregated = Statistics.builder().setOutputRowCount(groups).addColumnStatistic(DAYS, keys.get(DAYS))
                .build();
        ScalarOperator from2025 = new BinaryPredicateOperator(BinaryType.GE, DAYS, ConstantOperator.createInt(20089));
        double expected = 400.0 * (20400 - 20089) / (20400 - 20001);
        Assertions.assertEquals(expected,
                PredicateStatisticsCalculator.statisticsCalculate(from2025, aggregated).getOutputRowCount(),
                expected * 0.05);
    }

    @Test
    public void testBucketOfDatesHoldsItsDays() {
        // Ten days of 1,000 rows, without a count of distinct values: ten groups
        Histogram histogram = new Histogram(List.of(new Bucket(
                Utils.getLongFromDateTime(LocalDateTime.of(2025, 3, 1, 0, 0)),
                Utils.getLongFromDateTime(LocalDateTime.of(2025, 3, 10, 0, 0)), 1000L, 0L)), Map.of());
        Histogram distinct = StatisticsCalculator.distinctValueHistogram(histogram, 100, DateType.DATE);
        Assertions.assertEquals(10, distinct.getTotalRows());
    }

    @Test
    public void testGroupsOfAKeyDoNotKeepTheHistogramOfItsRows() {
        // The groups that the cost of a TopN over an aggregate builds from the input of the aggregate keep the MCV
        // list of the key, with one group for each value, but not the histogram of the rows
        List<MultiColumnCombinedStats.McvEntry> mcv = new ArrayList<>();
        for (int day = 20500; day < 20510; day++) {
            mcv.add(new MultiColumnCombinedStats.McvEntry(List.of(Integer.toString(day)), 60, List.of(60L)));
        }
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("date"), 1000, 410, mcv,
                List.of(List.of("17197", "20000", "100", "1", "100"), List.of("20001", "20400", "400", "1", "300")),
                List.of(0L));
        Statistics basic = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(DAYS, new ColumnStatistic(17197, 20509, 0, 4, 410)).build();
        Statistics input = StatisticsCalcUtils.attachExternalMcvStats(basic, new ExternalMcvStatistics(List.of(group)),
                Map.of(DAYS, new Column("date", IntegerType.INT)));
        Assertions.assertNotNull(input.getColumnStatistic(DAYS).getHistogram());
        Statistics groups = TopNAggregationCost.groupDistribution(input, List.of(DAYS), 410);
        Assertions.assertNull(groups.getColumnStatistic(DAYS).getHistogram());
        // The aggregate itself gives the key the histogram of its distinct values
        Map<ColumnRefOperator, ColumnStatistic> keys = new HashMap<>();
        StatisticsCalculator.computeGroupByStatistics(List.of(DAYS), input, keys);
        Statistics aggregated = McvAggregateStatistics.derive(List.of(DAYS), input,
                Statistics.builder().setOutputRowCount(410).addColumnStatistic(DAYS, keys.get(DAYS)).build());
        Assertions.assertEquals(410, aggregated.getColumnStatistic(DAYS).getHistogram().getTotalRows());
    }
}
