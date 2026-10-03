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

package com.starrocks.statistic;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.sql.ast.StatisticsType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.Snapshot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExternalMcvStatisticsCollectJobTest {
    @Test
    public void testBothPassesAndGroupsKeepTheCapturedSnapshot() {
        IcebergTable table = mock(IcebergTable.class);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        Snapshot first = mock(Snapshot.class);
        Snapshot second = mock(Snapshot.class);
        when(table.getName()).thenReturn("t");
        when(table.getNativeTable()).thenReturn(nativeTable);
        when(nativeTable.currentSnapshot()).thenReturn(first);
        when(first.snapshotId()).thenReturn(123L);
        when(second.snapshotId()).thenReturn(456L);
        ExternalMcvStatisticsCollectJob job = sourceJob(table);
        String source = job.captureSource();
        when(nativeTable.currentSnapshot()).thenReturn(second);
        for (List<String> columns : List.of(List.of("status"), List.of("status", "gate"))) {
            String from = job.buildFromClause(columns, source);
            String sketch = job.buildSketchSQL(from);
            String count = job.buildExactCountSQL(from, List.of(), columns.size());
            Assertions.assertTrue(sketch.contains("FROM `iceberg`.`db`.`t` FOR VERSION AS OF 123)"));
            Assertions.assertTrue(count.contains("FROM `iceberg`.`db`.`t` FOR VERSION AS OF 123)"));
        }
        verify(nativeTable, times(1)).currentSnapshot();
    }

    @Test
    public void testEmptyIcebergSnapshotAndOtherConnectors() {
        IcebergTable iceberg = mock(IcebergTable.class);
        when(iceberg.getName()).thenReturn("t");
        when(iceberg.getNativeTable()).thenReturn(mock(org.apache.iceberg.Table.class));
        Assertions.assertNull(sourceJob(iceberg).captureSource());
        Table other = mock(Table.class);
        when(other.getName()).thenReturn("t");
        Assertions.assertEquals("`iceberg`.`db`.`t`", sourceJob(other).captureSource());
    }

    private static ExternalMcvStatisticsCollectJob sourceJob(Table table) {
        return new ExternalMcvStatisticsCollectJob("iceberg", new Database(1, "db"), table,
                List.of("status", "gate"), List.of(VarcharType.VARCHAR, IntegerType.INT),
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Map.of(),
                List.of(StatisticsType.MCDISTINCT, StatisticsType.MCV), List.of(List.of("status", "gate")));
    }

    @Test
    public void testBucketLimitAlsoProtectsCollectionFromInvalidConfiguration() {
        int previous = Config.statistic_mcv_bucket_num;
        try {
            for (int buckets : List.of(0, -1, 10001, Integer.MAX_VALUE)) {
                Config.statistic_mcv_bucket_num = buckets;
                var singleton = new ExternalMcvStatisticsCollectJob("hive0", null, null,
                        List.of("id"), List.of(IntegerType.INT), StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.ONCE, Map.of(), List.of(StatisticsType.MCV), List.of(List.of("id")));
                Assertions.assertThrows(IllegalArgumentException.class, () -> singleton.collect(null, null));
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> newJob(Map.of()).buildSketchSQL(" FROM t", true));
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> newJob(Map.of(StatsConstants.MCV_BUCKET_NUM, Integer.toString(buckets)))
                                .buildSketchSQL(" FROM t", true));
            }
            Config.statistic_mcv_bucket_num = 10000;
            Assertions.assertTrue(newJob(Map.of()).buildSketchSQL(" FROM t", true)
                    .contains("ds_kll_quantiles(x, 10000)"));
            Assertions.assertTrue(newJob(Map.of(StatsConstants.MCV_BUCKET_NUM, "1"))
                    .buildSketchSQL(" FROM t", true).contains("ds_kll_quantiles(x, 1)"));
        } finally {
            Config.statistic_mcv_bucket_num = previous;
        }
    }

    @Test
    public void testSingletonCollectsBoundsAndCountsTheSameHead() {
        ExternalMcvStatisticsCollectJob job = newJob(Map.of(StatsConstants.MCV_SIZE, "7",
                StatsConstants.MCV_BUCKET_NUM, "16"));
        Assertions.assertTrue(job.buildSketchSQL(" FROM t", true).contains("ds_kll_quantiles(x, 16)"));
        String exact = job.buildExactCountSQL(" FROM t", List.of("7", "\\N"), 1, true, List.of("0", "10"));
        Assertions.assertTrue(exact.contains("histogram_by_bounds(x, '[\"7\"]', '[\"0\",\"10\"]')"));
        Assertions.assertEquals(List.of(List.of("1", "10", "100", "10", "10"),
                        List.of("20", "30", "400", "10", "11")),
                ExternalMcvStatisticsCollectJob.parseBuckets("{\"buckets\":[[\"1\",\"10\",\"100\",\"10\",\"10\"],"
                        + "[\"20\",\"30\",\"300\",\"10\",\"11\"]]}"));
    }

    private static ExternalMcvStatisticsCollectJob newJob(Map<String, String> properties) {
        return new ExternalMcvStatisticsCollectJob("hive0", null, null,
                List.of("status", "gate"), List.of(VarcharType.VARCHAR, IntegerType.INT),
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, properties,
                List.of(StatisticsType.MCDISTINCT, StatisticsType.MCV), List.of(List.of("status", "gate")));
    }

    @Test
    public void testSketchSQLUsesMcvSizeAndSketchParameters() {
        String from = " FROM (SELECT stats_tuple_key(cast(`status` as varchar), cast(`gate` as varchar)) AS k"
                + " FROM `hive0`.`db`.`t`) t";
        ExternalMcvStatisticsCollectJob job = newJob(Map.of());
        Assertions.assertEquals("SELECT count(*), ds_frequent_items(k, " + Config.statistic_mcv_size + ", "
                        + Config.statistic_mcv_sketch_lg_map_size + "), ds_hll_count_distinct(k, 17)" + from,
                job.buildSketchSQL(from));

        job = newJob(Map.of(StatsConstants.MCV_SIZE, "7"));
        Assertions.assertTrue(job.buildSketchSQL(from).startsWith("SELECT count(*), ds_frequent_items(k, 7, "));
    }

    @Test
    public void testExactCountSQLCountsTuplesAndComponents() {
        ExternalMcvStatisticsCollectJob job = newJob(Map.of());
        String sql = job.buildExactCountSQL(" FROM t", List.of("approved#0", "it's#\\N", "declined#0"), 2);
        Assertions.assertEquals("SELECT histogram_by_bounds(k, '[\"approved#0\",\"it''s#\\\\\\\\N\",\"declined#0\"]', '[]'),"
                + " count(*), histogram_by_bounds(v0, '[\"approved\",\"it''s\",\"declined\"]', '[]'), count(v0),"
                + " histogram_by_bounds(v1, '[\"0\"]', '[]'), count(v1) FROM t", sql);

        Assertions.assertThrows(IllegalStateException.class,
                () -> job.buildExactCountSQL(" FROM t", List.of("approved#0#1"), 2));
    }

    @Test
    public void testProjectionExposesKeyAndComponents() {
        Assertions.assertEquals("stats_tuple_key(cast(`status` as varchar), cast(`gate` as varchar)) AS k,"
                        + " cast(`status` as varchar) AS v0, cast(`gate` as varchar) AS v1",
                ExternalMcvStatisticsCollectJob.buildProjection(null, List.of("status", "gate")));
    }

    @Test
    public void testParseFrequentItemsKeepsOrder() {
        Assertions.assertEquals(List.of("approved#0#0", "declined#1#0", "\\N#0#0"),
                ExternalMcvStatisticsCollectJob.parseFrequentItems(
                        "[[\"approved#0#0\",\"540\"],[\"declined#1#0\",\"100\"],[\"\\\\N#0#0\",\"7\"]]"));
        Assertions.assertTrue(ExternalMcvStatisticsCollectJob.parseFrequentItems("[]").isEmpty());
        Assertions.assertTrue(ExternalMcvStatisticsCollectJob.parseFrequentItems(null).isEmpty());
    }

    @Test
    public void testParseExactCountsDecodesTuplesAndSortsByCount() {
        List<String> row = List.of(
                "{\"mcv\":[[\"approved#0#0\",\"540\"],[\"declined#1#0\",\"600\"],[\"\\\\N#0#0\",\"7\"],"
                        + "[\"gone#9#9\",\"0\"]],\"buckets\":[]}",
                "2000",
                "{\"mcv\":[[\"approved\",\"700\"],[\"declined\",\"900\"],[\"gone\",\"0\"]],\"buckets\":[]}", "1990",
                "{\"mcv\":[[\"0\",\"1000\"],[\"1\",\"950\"],[\"9\",\"0\"]],\"buckets\":[]}", "2000",
                "{\"mcv\":[[\"0\",\"1800\"],[\"9\",\"0\"]],\"buckets\":[]}", "2000");
        ExternalMcvStatisticsCollectJob.GroupStatistics statistics =
                ExternalMcvStatisticsCollectJob.parseExactCounts(row, 3, 12);
        Assertions.assertEquals(2000, statistics.rowCount);
        Assertions.assertEquals(12, statistics.ndv);
        List<ExternalMcvStatisticsCollectJob.McvTuple> mcv = statistics.mcv;
        Assertions.assertEquals(3, mcv.size());
        Assertions.assertEquals(List.of("declined", "1", "0"), mcv.get(0).values);
        Assertions.assertEquals(600, mcv.get(0).count);
        Assertions.assertEquals(List.of(900L, 950L, 1800L), mcv.get(0).componentCounts);
        Assertions.assertEquals(List.of("approved", "0", "0"), mcv.get(1).values);
        Assertions.assertEquals(List.of(700L, 1000L, 1800L), mcv.get(1).componentCounts);
        // A NULL component is counted from count(*) and the column's count().
        Assertions.assertEquals(Arrays.asList(null, "0", "0"), mcv.get(2).values);
        Assertions.assertEquals(7, mcv.get(2).count);
        Assertions.assertEquals(List.of(10L, 1000L, 1800L), mcv.get(2).componentCounts);

        // A component count below its tuple count is raised to the tuple count.
        row = List.of("{\"mcv\":[[\"a#1\",\"50\"]],\"buckets\":[]}", "100",
                "{\"mcv\":[[\"a\",\"20\"]],\"buckets\":[]}", "100", "{\"mcv\":[],\"buckets\":[]}", "100");
        statistics = ExternalMcvStatisticsCollectJob.parseExactCounts(row, 2, 3);
        Assertions.assertEquals(List.of(50L, 50L), statistics.mcv.get(0).componentCounts);

        Assertions.assertThrows(IllegalStateException.class, () ->
                ExternalMcvStatisticsCollectJob.parseExactCounts(
                        List.of("{\"mcv\":[[\"approved#0\",\"1\"]],\"buckets\":[]}", "1",
                                "{\"mcv\":[],\"buckets\":[]}", "1", "{\"mcv\":[],\"buckets\":[]}", "1",
                                "{\"mcv\":[],\"buckets\":[]}", "1"), 3, 1));
    }

    @Test
    public void testOneColumnGroup() {
        Assertions.assertEquals("stats_tuple_key(cast(`status` as varchar)) AS k, cast(`status` as varchar) AS v0",
                ExternalMcvStatisticsCollectJob.buildProjection(null, List.of("status")));
        ExternalMcvStatisticsCollectJob job = newJob(Map.of());
        Assertions.assertEquals("SELECT histogram_by_bounds(k, '[\"approved\",\"\\\\\\\\N\"]', '[]'), count(*),"
                        + " histogram_by_bounds(v0, '[\"approved\"]', '[]'), count(v0) FROM t",
                job.buildExactCountSQL(" FROM t", List.of("approved", "\\N"), 1));
        ExternalMcvStatisticsCollectJob.GroupStatistics statistics = ExternalMcvStatisticsCollectJob.parseExactCounts(
                List.of("{\"mcv\":[[\"approved\",\"600\"],[\"\\\\N\",\"50\"]],\"buckets\":[]}", "1000",
                        "{\"mcv\":[[\"approved\",\"600\"]],\"buckets\":[]}", "950"), 1, 3);
        Assertions.assertEquals(2, statistics.mcv.size());
        Assertions.assertEquals(List.of("approved"), statistics.mcv.get(0).values);
        Assertions.assertEquals(List.of(600L), statistics.mcv.get(0).componentCounts);
        Assertions.assertEquals(Arrays.asList((String) null), statistics.mcv.get(1).values);
        Assertions.assertEquals(List.of(50L), statistics.mcv.get(1).componentCounts);
    }

    @Test
    public void testMcvJsonRoundTrip() {
        List<ExternalMcvStatisticsCollectJob.McvTuple> mcv = List.of(
                new ExternalMcvStatisticsCollectJob.McvTuple(List.of("approved", "0"), 540, List.of(700L, 900L)),
                new ExternalMcvStatisticsCollectJob.McvTuple(Arrays.asList(null, "it's \"quoted\""), 7));
        String json = ExternalMcvStatisticsCollectJob.buildMcvJson(mcv);
        Assertions.assertEquals("[[[\"approved\",\"0\"],\"540\",[\"700\",\"900\"]],"
                + "[[null,\"it's \\\"quoted\\\"\"],\"7\"]]", json);
        Assertions.assertEquals("[\"status\",\"gate\"]",
                ExternalMcvStatisticsCollectJob.buildColumnNamesJson(List.of("status", "gate")));
        // The storage key is a fixed-length digest that does not depend on the order the group was given in.
        String columnIds = ExternalMcvStatisticsCollectJob.buildColumnIds(List.of("status", "gate"));
        Assertions.assertEquals(32, columnIds.length());
        Assertions.assertEquals(columnIds, ExternalMcvStatisticsCollectJob.buildColumnIds(List.of("gate", "status")));
        Assertions.assertNotEquals(columnIds, ExternalMcvStatisticsCollectJob.buildColumnIds(List.of("sta", "tus#gate")));
        Assertions.assertEquals("[]", ExternalMcvStatisticsCollectJob.buildMcvJson(List.of()));
    }
}
