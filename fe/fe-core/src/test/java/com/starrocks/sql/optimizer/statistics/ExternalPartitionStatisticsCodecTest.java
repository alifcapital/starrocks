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

import com.github.luben.zstd.Zstd;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

class ExternalPartitionStatisticsCodecTest {
    @Test
    void binaryRoundTripPreservesPreparedBoundsAndHllUnionForEveryColumn() {
        Map<String, Type> types = Map.of("число", IntegerType.BIGINT, "date", DateType.DATE, "s", VarcharType.VARCHAR);
        List<TStatisticData> rows = List.of(
                ExternalPartitionStatisticsTest.row("p", "число", 100, 30, 5, "-12", "9007199254740993"),
                ExternalPartitionStatisticsTest.row("p", "date", 100, 20, 0, "2026-01-01", "2026-09-28"),
                ExternalPartitionStatisticsTest.row("p", "s", 100, 10, 4, "", ""));
        var packed = ExternalPartitionStatistics.decode(ExternalPartitionStatistics.encode(rows, types).orElseThrow());
        for (var row : rows) {
            var expected = new ExternalColumnStatistics.Partition(row, types.get(row.columnName));
            var actual = packed.columns.get(row.columnName);
            Assertions.assertEquals(expected.getRowCount(), actual.getRowCount());
            Assertions.assertEquals(expected.getDataSize(), actual.getDataSize());
            Assertions.assertEquals(expected.getNullCount(), actual.getNullCount());
            Assertions.assertEquals(expected.getSourceType(), actual.getSourceType());
            Assertions.assertEquals(expected.getMinValue(), actual.getMinValue());
            Assertions.assertEquals(expected.getMaxValue(), actual.getMaxValue());
            var union = new StatisticsHll.Union();
            union.merge(expected.getHll());
            double before = union.estimate();
            union.merge(actual.getHll());
            Assertions.assertEquals(before, union.estimate());
        }
        Assertions.assertTrue(packed.retainedBytes() > rows.stream().mapToInt(row -> row.getHll().length).sum());
        Assertions.assertTrue(ExternalPartitionStatistics.decode(
                ExternalPartitionStatistics.encode(List.of(), Map.of()).orElseThrow()).columns.isEmpty());
    }

    @Test
    void corruptPayloadsFailBeforeCachePublicationAndOversizeHasCellFallback() {
        var row = ExternalPartitionStatisticsTest.row("p", "c", 100, 10, 0, "1", "9");
        var types = Map.<String, Type>of("c", IntegerType.BIGINT);
        byte[] encoded = ExternalPartitionStatistics.encode(List.of(row), types).orElseThrow();
        byte[] raw = Zstd.decompress(encoded, (int) Zstd.decompressedSize(encoded));
        byte[] version = raw.clone();
        version[0] = 0;
        byte[] badCount = raw.clone();
        ByteBuffer.wrap(badCount).putInt(4, Integer.MAX_VALUE);
        byte[] badLength = raw.clone();
        ByteBuffer.wrap(badLength).putInt(8, Integer.MAX_VALUE);
        for (byte[] bad : List.of(version, badCount, badLength, Arrays.copyOf(raw, raw.length - 1),
                Arrays.copyOf(raw, raw.length + 1))) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> ExternalPartitionStatistics.decode(Zstd.compress(bad, 1)));
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> ExternalPartitionStatistics.decode(new byte[0]));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ExternalPartitionStatistics.decode(
                ExternalPartitionStatistics.encode(List.of(row, row), types).orElseThrow()));
        Assertions.assertTrue(ExternalPartitionStatistics.encode(java.util.Collections.nCopies(10001, row), types).isEmpty());
        byte[] full = new byte[16385];
        full[0] = 3;
        row.setHll(full);
        Assertions.assertTrue(ExternalPartitionStatistics.encode(java.util.Collections.nCopies(1100, row), types).isEmpty());
    }
}
