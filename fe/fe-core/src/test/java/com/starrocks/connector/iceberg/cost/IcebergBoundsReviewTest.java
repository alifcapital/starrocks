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

package com.starrocks.connector.iceberg.cost;

import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Review probes of commit 5: a file that leaves out the bounds of a column must drop the bounds of the column.
public class IcebergBoundsReviewTest {
    private static final Types.NestedField NUMBER = Types.NestedField.optional(1, "n", Types.IntegerType.get());
    private static final Types.NestedField TEXT = Types.NestedField.optional(2, "s", Types.StringType.get());

    // The second file has no metric maps at all (DataFile.lowerBounds() and nullValueCounts() are null, as for a
    // file written without metrics). IcebergStatisticProvider.updateIcebergFileStats passes toMap(null) == null.
    @Test
    public void testFileWithoutAnyMetricsDropsBounds() throws Exception {
        Map<Integer, Type.PrimitiveType> types = Map.of(1, Types.IntegerType.get(), 2, Types.StringType.get());
        IcebergFileStats stats = new IcebergFileStats(types, List.of(NUMBER, TEXT), 10, 100,
                new HashMap<>(Map.of(1, 1, 2, "a")), new HashMap<>(Map.of(1, 9, 2, "c")),
                new HashMap<>(Map.of(1, 0L, 2, 0L)), Map.of());
        IcebergStatisticProvider provider = new IcebergStatisticProvider();
        Method min = IcebergStatisticProvider.class.getDeclaredMethod("updateSummaryMin", IcebergFileStats.class,
                List.class, Map.class, Map.class, long.class);
        Method max = IcebergStatisticProvider.class.getDeclaredMethod("updateSummaryMax", IcebergFileStats.class,
                List.class, Map.class, Map.class, long.class);
        min.setAccessible(true);
        max.setAccessible(true);
        // The second file holds n from 100 to 200 and s from 'x' to 'z', without any metrics
        stats.incrementRecordCount(10);
        min.invoke(provider, stats, List.of(), null, null, 10L);
        max.invoke(provider, stats, List.of(), null, null, 10L);
        stats.updateNullCount(null);

        ColumnStatistic number = provider.buildColumnStatistic(1, new Column("n", IntegerType.INT), stats, Map.of(),
                null);
        ColumnStatistic text = provider.buildColumnStatistic(2, new Column("s", VarcharType.VARCHAR), stats, Map.of(),
                MetricsModes.Full.get());
        String report = "n [" + number.getMinValue() + ", " + number.getMaxValue() + "], s [" + text.getMinString()
                + ", " + text.getMaxString() + "], s bounds " + text.getStringBounds()
                + ", hasValidColumnMetrics " + stats.hasValidColumnMetrics();
        System.out.println("REVIEW " + report);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, number.getMaxValue(), report);
        Assertions.assertNull(text.getMaxString(), report);
        Assertions.assertNull(text.getStringBounds(), report);
    }

    // An identity partition column with a NULL partition: the file of the NULL partition has no bounds for the
    // column because all its values are NULL, which is no missing bound. The other file holds all the values.
    @Test
    public void testNullPartitionKeepsBoundsOfIdentityColumn() throws Exception {
        Types.NestedField region = Types.NestedField.optional(2, "region", Types.StringType.get());
        Types.NestedField day = Types.NestedField.optional(3, "p", Types.IntegerType.get());
        // b is partitioned by a bucket transform, so it stays among the non-partition columns
        Types.NestedField bucketed = Types.NestedField.optional(4, "b", Types.IntegerType.get());
        Map<Integer, Type.PrimitiveType> types = Map.of(1, Types.IntegerType.get(), 2, Types.StringType.get(),
                3, Types.IntegerType.get(), 4, Types.IntegerType.get());
        List<PartitionField> fields = PartitionSpec.builderFor(new Schema(NUMBER, region, day, bucketed))
                .identity("region").identity("p").bucket("b", 4).build().fields();
        IcebergFileStats stats = new IcebergFileStats(types, List.of(NUMBER, bucketed), 10, 100,
                new HashMap<>(Map.of(1, 1, 2, "a", 3, 5, 4, 1)), new HashMap<>(Map.of(1, 9, 2, "c", 3, 5, 4, 100)),
                new HashMap<>(Map.of(1, 0L, 2, 0L, 3, 0L, 4, 0L)), Map.of());
        IcebergStatisticProvider provider = new IcebergStatisticProvider();
        Method min = IcebergStatisticProvider.class.getDeclaredMethod("updateSummaryMin", IcebergFileStats.class,
                List.class, Map.class, Map.class, long.class);
        Method max = IcebergStatisticProvider.class.getDeclaredMethod("updateSummaryMax", IcebergFileStats.class,
                List.class, Map.class, Map.class, long.class);
        min.setAccessible(true);
        max.setAccessible(true);
        // The second file is the partition region = NULL, p = NULL: 10 rows, no bounds for region and p
        stats.incrementRecordCount(10);
        Map<Integer, Long> nulls = Map.of(1, 0L, 2, 10L, 3, 10L, 4, 10L);
        min.invoke(provider, stats, fields, Map.of(1, 1), nulls, 10L);
        max.invoke(provider, stats, fields, Map.of(1, 9), nulls, 10L);
        stats.updateNullCount(nulls);

        ColumnStatistic text = provider.buildColumnStatistic(2, new Column("region", VarcharType.VARCHAR), stats,
                Map.of(), MetricsModes.Full.get());
        ColumnStatistic number = provider.buildColumnStatistic(3, new Column("p", IntegerType.INT), stats, Map.of(),
                null);
        ColumnStatistic hashed = provider.buildColumnStatistic(4, new Column("b", IntegerType.INT), stats, Map.of(),
                null);
        String report = "region [" + text.getMinString() + ", " + text.getMaxString() + "], p ["
                + number.getMinValue() + ", " + number.getMaxValue() + "], b [" + hashed.getMinValue() + ", "
                + hashed.getMaxValue() + "], corrupted " + stats.getCorruptedStats() + ", raw min "
                + stats.getMinValues() + ", raw max " + stats.getMaxValues();
        System.out.println("REVIEW " + report);
        Assertions.assertEquals("c", text.getMaxString(), report);
        Assertions.assertEquals(5, number.getMaxValue(), report);
        Assertions.assertEquals(100, hashed.getMaxValue(), report);
    }
}
