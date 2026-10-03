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

import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class ExternalPartitionStatisticsTest {
    static TStatisticData row(String partition, String column, long rows, int ndv, long nulls, String min, String max) {
        ByteBuffer hll = ByteBuffer.allocate(2 + ndv * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        hll.put((byte) 1).put((byte) ndv);
        for (int i = 0; i < ndv; i++) {
            hll.putLong(i);
        }
        return new TStatisticData().setPartitionName(partition).setColumnName(column).setRowCount(rows)
                .setDataSize(rows * 8).setNullCount(nulls).setMin(min).setMax(max).setHll(hll.array());
    }

    private static ExternalStatisticsAggregate aggregate(List<String> partitions) {
        ExternalStatisticsRequest request = new ExternalStatisticsRequest("table", partitions, List.of("amount", "missing"));
        ExternalStatisticsAggregate.Builder builder = new ExternalStatisticsAggregate.Builder(request);
        Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> batch = new HashMap<>();
        batch.put(new ExternalStatisticsCacheKey("table", "p=1", "amount"), Optional.of(
                new ExternalColumnStatistics.Partition(row("p=1", "amount", 100, 50, 5, "1", "500"), IntegerType.BIGINT)));
        batch.put(new ExternalStatisticsCacheKey("table", "p=2", "amount"), Optional.of(
                new ExternalColumnStatistics.Partition(row("p=2", "amount", 200, 80, 10, "10", "900"), IntegerType.BIGINT)));
        builder.add(batch);
        return builder.build();
    }

    @Test
    void aggregatesSelectedPartitionsWithoutGlobalStatisticsAndUnionsNdv() {
        ExternalStatisticsAggregate result = aggregate(List.of("p=1", "p=2"));
        Assertions.assertEquals(300, result.rowCount);
        ColumnStatistic amount = result.columns.get("amount");
        Assertions.assertEquals(80, amount.getDistinctValuesCount(), "Overlapping sets must be unioned, not summed to 130");
        Assertions.assertEquals(1, amount.getMinValue());
        Assertions.assertEquals(900, amount.getMaxValue());
        Assertions.assertEquals(8, amount.getAverageRowSize());
        Assertions.assertEquals(0.05, amount.getNullsFraction(), 1e-9);
        Assertions.assertTrue(result.columns.get("missing").isUnknown());
        Assertions.assertEquals(0, result.coveredPartitions.get("missing"));
        Assertions.assertFalse(result.hasCompleteCoverage());
    }

    @Test
    void missingCoverageIsExplicitAndNeverCountsAsEmptyRows() {
        ExternalStatisticsAggregate result = aggregate(List.of("p=1", "p=2", "p=3"));
        Assertions.assertEquals(450, result.rowCount);
        Assertions.assertEquals(120, result.columns.get("amount").getDistinctValuesCount());
        Assertions.assertEquals(2, result.knownPartitions);
        Assertions.assertEquals(3, result.requestedPartitions);
        Assertions.assertEquals(2, result.coveredPartitions.get("amount"));
        ExternalStatisticsAggregate absent = new ExternalStatisticsAggregate.Builder(
                new ExternalStatisticsRequest("table", List.of("missing"), List.of("amount"))).build();
        Assertions.assertTrue(absent.isEmpty());
        Assertions.assertTrue(absent.columns.get("amount").isUnknown());
    }

    @Test
    void parsesTypedBoundsOnceAndKeepsBasicStringsUnbounded() {
        for (Type type : List.of(IntegerType.BIGINT, VarcharType.VARCHAR)) {
            ExternalStatisticsAggregate.Builder builder = new ExternalStatisticsAggregate.Builder(
                    new ExternalStatisticsRequest("table", List.of("p=1"), List.of("c")));
            builder.add(Map.of(new ExternalStatisticsCacheKey("table", "p=1", "c"), Optional.of(
                    new ExternalColumnStatistics.Partition(row("p=1", "c", 100, 10, 0, "9", "10"), type))));
            ColumnStatistic stats = builder.build().columns.get("c");
            Assertions.assertEquals(type == IntegerType.BIGINT ? 9 : Double.NEGATIVE_INFINITY, stats.getMinValue());
            Assertions.assertEquals(type == IntegerType.BIGINT ? 10 : Double.POSITIVE_INFINITY, stats.getMaxValue());
        }
        Assertions.assertTrue(ExternalColumnStatistics.parseBound(DateType.DATE, "2026-01-01").isPresent());
        Assertions.assertTrue(ExternalColumnStatistics.parseBound(DateType.DATE, "bad date").isEmpty());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ExternalColumnStatistics.Partition(new TStatisticData(), IntegerType.BIGINT));
    }
    @Test
    void blockCoverageRejectsCorruptionAndExpiryCannotBeRenewedByDirectoryRepacking() {
        ExternalStatisticsCacheKey key = ExternalStatisticsCacheKey.block("table", "c", List.of("p=1", "p=2"));
        ExternalColumnStatistics.Partition summary = new ExternalColumnStatistics.Partition(
                row("", "c", 100, 10, 0, "1", "10"), IntegerType.BIGINT);
        var block = ExternalPartitionStatisticsBlocks.Block.fromRows(key, summary, "[[\"p=1\",100]]");
        Assertions.assertEquals(100, block.rows(0));
        Assertions.assertEquals(-1, block.rows(1));
        for (String invalid : List.of("[[\"p=1\",50]]", "[[\"p=3\",100]]",
                "[[\"p=1\",50],[\"p=1\",50]]", "[[\"p=1\",-1]]")) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> ExternalPartitionStatisticsBlocks.Block.fromRows(key, summary, invalid));
        }
        long now = System.nanoTime();
        long expiredAt = now + java.util.concurrent.TimeUnit.SECONDS.toNanos(
                com.starrocks.common.Config.statistic_update_interval_sec * 2L) + 1_000_000;
        var directory = new ExternalPartitionStatisticsBlocks.Directory().withBlocks(List.of(block), now);
        Assertions.assertEquals(1, directory.covering(key.partitions, java.util.Set.copyOf(key.partitions), now).size());
        Assertions.assertTrue(directory.covering(key.partitions, java.util.Set.copyOf(key.partitions), expiredAt).isEmpty());
        Assertions.assertTrue(directory.withBlocks(List.of(), expiredAt)
                .covering(key.partitions, java.util.Set.copyOf(key.partitions), now).isEmpty());
    }

}
