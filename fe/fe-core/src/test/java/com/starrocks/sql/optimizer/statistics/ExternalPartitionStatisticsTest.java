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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExternalPartitionStatisticsTest {
    private static final ColumnRefOperator DT = new ColumnRefOperator(1, DateType.DATE, "dt", true);
    private static final ColumnRefOperator STATUS = new ColumnRefOperator(2, VarcharType.VARCHAR, "status", true);
    private static final ColumnRefOperator AMOUNT = new ColumnRefOperator(3, IntegerType.BIGINT, "amount", true);
    private static final ColumnRefOperator EXTRA = new ColumnRefOperator(4, IntegerType.INT, "extra", true);

    private static Map<ColumnRefOperator, Column> columns() {
        Map<ColumnRefOperator, Column> columns = new HashMap<>();
        for (ColumnRefOperator ref : List.of(DT, STATUS, AMOUNT, EXTRA)) {
            columns.put(ref, new Column(ref.getName(), ref.getType()));
        }
        return columns;
    }

    private static double date(String text) {
        return StatisticUtils.convertStatisticsToDouble(DateType.DATE, text).orElseThrow();
    }

    private static Statistics tableStatistics() {
        return Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(DT, ColumnStatistic.builder().setMinValue(date("2023-01-01"))
                        .setMaxValue(date("2024-12-31")).setDistinctValuesCount(30).setNullsFraction(0)
                        .setAverageRowSize(4).build())
                .addColumnStatistic(STATUS, ColumnStatistic.builder().setDistinctValuesCount(3).setNullsFraction(0.2)
                        .setAverageRowSize(8).build())
                .addColumnStatistic(AMOUNT, ColumnStatistic.builder().setMinValue(0).setMaxValue(100000)
                        .setDistinctValuesCount(1000).setNullsFraction(0).setAverageRowSize(8).build())
                .addColumnStatistic(EXTRA, ColumnStatistic.builder().setMinValue(0).setMaxValue(9)
                        .setDistinctValuesCount(10).setNullsFraction(0).setAverageRowSize(4).build())
                .build();
    }

    private static ExternalPartitionStatistics partitionStatistics() {
        return ExternalPartitionStatsCacheLoader.parse(Arrays.asList(
                List.of("dt=2024-01-01", "dt", "100", "1", "0", "2024-01-01", "2024-01-01"),
                List.of("dt=2024-01-01", "status", "100", "3", "5", "", ""),
                List.of("dt=2024-01-01", "amount", "100", "50", "0", "1", "500"),
                List.of("dt=2024-01-02", "dt", "200", "1", "0", "2024-01-02", "2024-01-02"),
                List.of("dt=2024-01-02", "status", "200", "3", "10", "", ""),
                List.of("dt=2024-01-02", "amount", "200", "80", "0", "10", "900"),
                List.of("dt=2024-01-09", "dt", "300", "1", "0", "2024-01-09", "2024-01-09"),
                Arrays.asList(null, "dt", "1", "1", "0", "", ""),
                List.of("dt=2024-01-10", "dt", "not a number", "1", "0", "", "")));
    }

    @Test
    public void testParseKeepsWellFormedRowsOnly() {
        ExternalPartitionStatistics statistics = partitionStatistics();
        Assertions.assertEquals(3, statistics.getPartitionNames().size());
        ExternalPartitionStatistics.ColumnStats amount = statistics.getPartition("dt=2024-01-02").get("amount");
        Assertions.assertEquals(200, amount.getRowCount());
        Assertions.assertEquals(80, amount.getNdv());
        Assertions.assertEquals("10", amount.getMin());
        Assertions.assertEquals("900", amount.getMax());
        Assertions.assertTrue(ExternalPartitionStatsCacheLoader.parse(List.of()).isEmpty());
    }

    @Test
    public void testAggregateSumsTheSelectedPartitions() {
        Statistics table = tableStatistics();
        Optional<Statistics> selected = partitionStatistics().aggregate(table, columns(),
                List.of("dt=2024-01-01", "dt=2024-01-02"));
        Assertions.assertTrue(selected.isPresent());
        Statistics statistics = selected.get();
        Assertions.assertEquals(300, statistics.getOutputRowCount(), 1e-9);

        ColumnStatistic dt = statistics.getColumnStatistic(DT);
        Assertions.assertEquals(date("2024-01-01"), dt.getMinValue(), 1e-9);
        Assertions.assertEquals(date("2024-01-02"), dt.getMaxValue(), 1e-9);
        Assertions.assertEquals(2, dt.getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(0, dt.getNullsFraction(), 1e-9);

        // Strings keep the table-level bounds; the distinct values are capped by the table-level count.
        ColumnStatistic status = statistics.getColumnStatistic(STATUS);
        Assertions.assertEquals(3, status.getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(0.05, status.getNullsFraction(), 1e-9);
        Assertions.assertEquals(table.getColumnStatistic(STATUS).getMinValue(), status.getMinValue());

        ColumnStatistic amount = statistics.getColumnStatistic(AMOUNT);
        Assertions.assertEquals(1, amount.getMinValue(), 1e-9);
        Assertions.assertEquals(900, amount.getMaxValue(), 1e-9);
        Assertions.assertEquals(130, amount.getDistinctValuesCount(), 1e-9);

        // A column no partition has statistics for keeps the table-level statistics.
        Assertions.assertSame(table.getColumnStatistic(EXTRA), statistics.getColumnStatistic(EXTRA));
    }

    @Test
    public void testPartitionsWithoutStatisticsCountAsAverageOnes() {
        Optional<Statistics> selected = partitionStatistics().aggregate(tableStatistics(), columns(),
                List.of("dt=2024-01-01", "dt=2024-01-02", "dt=2024-02-01"));
        Assertions.assertTrue(selected.isPresent());
        Statistics statistics = selected.get();
        Assertions.assertEquals(450, statistics.getOutputRowCount(), 1e-9);
        Assertions.assertEquals(3, statistics.getColumnStatistic(DT).getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(0.05, statistics.getColumnStatistic(STATUS).getNullsFraction(), 1e-9);
        Assertions.assertEquals(195, statistics.getColumnStatistic(AMOUNT).getDistinctValuesCount(), 1e-9);

        // A partition whose statistics cover only some columns scales the others by its own count.
        selected = partitionStatistics().aggregate(tableStatistics(), columns(),
                List.of("dt=2024-01-02", "dt=2024-01-09"));
        Assertions.assertEquals(500, selected.get().getOutputRowCount(), 1e-9);
        Assertions.assertEquals(2, selected.get().getColumnStatistic(DT).getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(160, selected.get().getColumnStatistic(AMOUNT).getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(0.04, selected.get().getColumnStatistic(STATUS).getNullsFraction(), 1e-9);
    }

    @Test
    public void testNoSelectedPartitionWithStatisticsGivesNothing() {
        Assertions.assertTrue(partitionStatistics().aggregate(tableStatistics(), columns(),
                List.of("dt=2024-03-01")).isEmpty());
        Assertions.assertTrue(ExternalPartitionStatistics.EMPTY.aggregate(tableStatistics(), columns(),
                List.of("dt=2024-01-01")).isEmpty());
    }

    @Test
    public void testParseBound() {
        Assertions.assertEquals(date("2024-01-05"),
                ExternalPartitionStatistics.parseBound(DateType.DATE, "2024-01-05").orElseThrow(), 1e-9);
        Assertions.assertEquals(42, ExternalPartitionStatistics.parseBound(IntegerType.BIGINT, "42").orElseThrow(), 1e-9);
        Assertions.assertTrue(ExternalPartitionStatistics.parseBound(IntegerType.BIGINT, "").isEmpty());
        Assertions.assertTrue(ExternalPartitionStatistics.parseBound(IntegerType.BIGINT, "x").isEmpty());
        Assertions.assertTrue(ExternalPartitionStatistics.parseBound(VarcharType.VARCHAR, "abc").isEmpty());
        Assertions.assertTrue(ExternalPartitionStatistics.parseBound(DateType.DATE, "not a date").isEmpty());
    }
}
