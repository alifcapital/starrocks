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
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The kept MCV attachment of a scan equals the attachment computed for the scan: two scans that differ in any input of
 * the attach get their own attachment from one query cache.
 */
public class McvAttachmentKeyTest {
    private static final Column COLUMN_A = new Column("a", IntegerType.INT);
    private static final Column COLUMN_A2 = new Column("a", IntegerType.INT);
    private static final Column COLUMN_B = new Column("b", IntegerType.INT);
    private static final ColumnRefOperator A = new ColumnRefOperator(1, IntegerType.INT, "a", true);
    private static final ColumnRefOperator B = new ColumnRefOperator(2, IntegerType.INT, "b", true);

    private static final ExternalMcvStatistics MCV = new ExternalMcvStatistics(List.of(
            new ExternalMcvStatistics.Group(List.of("a"), 1000, 10,
                    List.of(new MultiColumnCombinedStats.McvEntry(List.of("1"), 500, List.of(500L)),
                            new MultiColumnCombinedStats.McvEntry(List.of("2"), 300, List.of(300L))),
                    List.of(), List.of(0L)),
            new ExternalMcvStatistics.Group(List.of("a", "b"), 1000, 50,
                    List.of(new MultiColumnCombinedStats.McvEntry(List.of("1", "7"), 400, List.of(500L, 450L)),
                            new MultiColumnCombinedStats.McvEntry(List.of("2", "8"), 200, List.of(300L, 250L))),
                    List.of(), List.of(0L, 0L))));

    private static ColumnStatistic column(double ndv, Histogram histogram) {
        return ColumnStatistic.builder().setMinValue(0).setMaxValue(100).setNullsFraction(0).setAverageRowSize(4)
                .setDistinctValuesCount(ndv).setHistogram(histogram).setType(ColumnStatistic.StatisticType.ESTIMATE)
                .build();
    }

    private static Statistics input(double rowCount, Statistics.StatsSource source, Map<ColumnRefOperator, ?> columns,
                                    Histogram histogramOfB) {
        Statistics.Builder builder = Statistics.builder().setOutputRowCount(rowCount).setStatsSource(source);
        columns.keySet().forEach(ref -> builder.addColumnStatistic(ref,
                column(20, ref.getId() == 2 ? histogramOfB : null)));
        return builder.build();
    }

    private static Map<ColumnRefOperator, Column> scan(ColumnRefOperator... refs) {
        Map<ColumnRefOperator, Column> columns = new LinkedHashMap<>();
        for (ColumnRefOperator ref : refs) {
            columns.put(ref, ref.getId() == 2 ? COLUMN_B : ref.getId() == 3 ? COLUMN_A2 : COLUMN_A);
        }
        return columns;
    }

    // Attaches first, then second through one cache, and checks that second gets the attachment computed for it.
    private static void assertKeptAttachmentMatches(Map<ColumnRefOperator, Column> firstColumns, Statistics first,
                                                    Map<ColumnRefOperator, Column> secondColumns, Statistics second) {
        ExpressionStatisticsCache cache = new ExpressionStatisticsCache();
        Statistics firstAttached = cache.mcvAttachment(null, MCV, firstColumns, first,
                () -> StatisticsCalcUtils.attachExternalMcvStats(first, MCV, firstColumns));
        Assertions.assertNotSame(first, firstAttached);
        Statistics kept = cache.mcvAttachment(null, MCV, secondColumns, second,
                () -> StatisticsCalcUtils.attachExternalMcvStats(second, MCV, secondColumns));
        Statistics computed = StatisticsCalcUtils.attachExternalMcvStats(second, MCV, secondColumns);
        Assertions.assertEquals(ScanEstimateKey.content(computed), ScanEstimateKey.content(kept));
    }

    @Test
    public void testSameInputsHit() {
        ExpressionStatisticsCache cache = new ExpressionStatisticsCache();
        AtomicInteger computed = new AtomicInteger();
        Map<ColumnRefOperator, Column> columns = scan(A, B);
        Statistics first = input(5000, Statistics.StatsSource.ANALYZE, columns, null);
        Statistics firstAttached = cache.mcvAttachment(null, MCV, columns, first, () -> {
            computed.incrementAndGet();
            return StatisticsCalcUtils.attachExternalMcvStats(first, MCV, columns);
        });
        // Other objects of the same values.
        Map<ColumnRefOperator, Column> again = scan(new ColumnRefOperator(1, IntegerType.INT, "a", true),
                new ColumnRefOperator(2, IntegerType.INT, "b", true));
        Statistics second = input(5000, Statistics.StatsSource.ANALYZE, again, null);
        Statistics secondAttached = cache.mcvAttachment(null, MCV, again, second, () -> {
            computed.incrementAndGet();
            return StatisticsCalcUtils.attachExternalMcvStats(second, MCV, again);
        });
        Assertions.assertEquals(1, computed.get());
        Assertions.assertSame(firstAttached, secondAttached);
    }

    @Test
    public void testRowCountIsInTheKey() {
        Map<ColumnRefOperator, Column> columns = scan(A, B);
        assertKeptAttachmentMatches(columns, input(5000, Statistics.StatsSource.ANALYZE, columns, null),
                columns, input(7000, Statistics.StatsSource.ANALYZE, columns, null));
    }

    @Test
    public void testStatsSourceIsInTheKey() {
        Map<ColumnRefOperator, Column> columns = scan(A, B);
        assertKeptAttachmentMatches(columns, input(777, Statistics.StatsSource.ANALYZE, columns, null),
                columns, input(777, Statistics.StatsSource.NONE, columns, null));
    }

    @Test
    public void testHistogramIsInTheKey() {
        Map<ColumnRefOperator, Column> columns = scan(A, B);
        Histogram histogram = new Histogram(List.of(new Bucket(0, 100, 900L, 10L)), Map.of());
        assertKeptAttachmentMatches(columns, input(5000, Statistics.StatsSource.ANALYZE, columns, null),
                columns, input(5000, Statistics.StatsSource.ANALYZE, columns, histogram));
    }

    @Test
    public void testColumnTypeIsInTheKey() {
        Map<ColumnRefOperator, Column> columns = scan(A, B);
        Map<ColumnRefOperator, Column> varchar = scan(A, new ColumnRefOperator(2, VarcharType.VARCHAR, "b", true));
        assertKeptAttachmentMatches(columns, input(5000, Statistics.StatsSource.ANALYZE, columns, null),
                varchar, input(5000, Statistics.StatsSource.ANALYZE, varchar, null));
    }

    @Test
    public void testColumnNameIsInTheKey() {
        Map<ColumnRefOperator, Column> columns = scan(A, B);
        Map<ColumnRefOperator, Column> renamed = scan(A, new ColumnRefOperator(2, IntegerType.INT, "c", true));
        assertKeptAttachmentMatches(columns, input(5000, Statistics.StatsSource.ANALYZE, columns, null),
                renamed, input(5000, Statistics.StatsSource.ANALYZE, renamed, null));
    }

    // The attach keeps the head of a group from the MCV statistics of the table. We expect the kept attach not to count
    // the head, so a large head does not clear what the cache kept before.
    @Test
    public void testSharedHeadDoesNotClearTheCache() {
        List<MultiColumnCombinedStats.McvEntry> head = new ArrayList<>();
        for (int i = 0; i < 60_000; i++) {
            head.add(new MultiColumnCombinedStats.McvEntry(List.of(Integer.toString(i), Integer.toString(i)), 10,
                    List.of(10L, 10L)));
        }
        ExternalMcvStatistics mcv = new ExternalMcvStatistics(List.of(new ExternalMcvStatistics.Group(
                List.of("a", "b"), 10_000_000, 100_000, head, List.of(), List.of(0L, 0L))));
        Map<ColumnRefOperator, Column> columns = scan(A, B);
        Statistics statistics = input(5000, Statistics.StatsSource.ANALYZE, columns, null);
        ExpressionStatisticsCache cache = new ExpressionStatisticsCache();
        AtomicInteger computed = new AtomicInteger();
        cache.part(List.of("earlier"), () -> computed.incrementAndGet(), value -> 150_000);
        Statistics attached = cache.mcvAttachment(null, mcv, columns, statistics,
                () -> StatisticsCalcUtils.attachExternalMcvStats(statistics, mcv, columns));
        Assertions.assertSame(mcv.getGroups().get(0).getMcv(),
                attached.getMultiColumnCombinedStats().values().iterator().next().getMcv());
        cache.part(List.of("earlier"), () -> computed.incrementAndGet(), value -> 150_000);
        Assertions.assertEquals(1, computed.get());
    }

    // Two references of one name: the first in the order of the scan takes the group.
    @Test
    public void testColumnOrderIsInTheKey() {
        ColumnRefOperator other = new ColumnRefOperator(3, IntegerType.INT, "a", true);
        Map<ColumnRefOperator, Column> first = scan(A, other, B);
        Map<ColumnRefOperator, Column> second = scan(other, A, B);
        assertKeptAttachmentMatches(first, input(5000, Statistics.StatsSource.ANALYZE, first, null),
                second, input(5000, Statistics.StatsSource.ANALYZE, second, null));
    }
}
