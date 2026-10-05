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

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.statistics.ConnectorTableColumnKey;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatisticsType;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.ExternalHistogramStatsMeta;
import com.starrocks.statistic.HistogramStatsMeta;
import com.starrocks.statistic.MultiColumnStatsMeta;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lookups of statistics that exist for few tables must answer "none" from the AnalyzeMgr meta, without
 * the statistics table state check and without the cache. With meta they behave as before.
 */
public class RareStatisticsLookupTest {
    private static final long TABLE_ID = 7_700_000_001L;
    private static final String CATALOG = "rare_stats_catalog";
    private static final Histogram HISTOGRAM = new Histogram(Map.of("x", 100L));

    private final AtomicInteger stateChecks = new AtomicInteger();
    private final AtomicInteger blacklistChecks = new AtomicInteger();
    private final AtomicInteger loads = new AtomicInteger();
    private boolean oldSync;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @BeforeEach
    public void beforeEach() {
        oldSync = Config.enable_sync_statistics_load;
        Config.enable_sync_statistics_load = true;
        new MockUp<StatisticUtils>() {
            @Mock
            public boolean checkStatisticTableStateNormal() {
                stateChecks.incrementAndGet();
                return true;
            }

            @Mock
            public boolean statisticTableBlackListCheck(long tableId) {
                blacklistChecks.incrementAndGet();
                return false;
            }
        };
    }

    @AfterEach
    public void afterEach() {
        Config.enable_sync_statistics_load = oldSync;
    }

    private static AnalyzeMgr analyzeMgr() {
        return GlobalStateMgr.getCurrentState().getAnalyzeMgr();
    }

    private static ExternalHistogramStatsMeta externalHistogram(String table, String column) {
        return new ExternalHistogramStatsMeta(CATALOG, "db", table, column, StatsConstants.AnalyzeType.HISTOGRAM,
                LocalDateTime.now(), Map.of());
    }

    private static HistogramStatsMeta histogramMeta(long tableId, String column) {
        return new HistogramStatsMeta(1, tableId, column, StatsConstants.AnalyzeType.HISTOGRAM,
                LocalDateTime.now(), Map.of());
    }

    private static MultiColumnStatsMeta multiColumn(long tableId, Set<Integer> columns) {
        return new MultiColumnStatsMeta(1, tableId, columns, StatsConstants.AnalyzeType.FULL,
                List.of(StatisticsType.MCDISTINCT), LocalDateTime.now(), Map.of());
    }

    private AsyncLoadingCache<ColumnStatsCacheKey, Optional<Histogram>> countingHistogramCache() {
        AsyncCacheLoader<ColumnStatsCacheKey, Optional<Histogram>> loader = (key, executor) -> {
            loads.incrementAndGet();
            return CompletableFuture.completedFuture(Optional.of(HISTOGRAM));
        };
        return Caffeine.newBuilder().executor(Runnable::run).buildAsync(loader);
    }

    private void expectExternalTable(Table table, String name) {
        new Expectations() {
            {
                table.isAnalyzableExternalTable();
                result = true;
                minTimes = 0;
                table.getCatalogName();
                result = CATALOG;
                minTimes = 0;
                table.getCatalogDBName();
                result = "db";
                minTimes = 0;
                table.getName();
                result = name;
                minTimes = 0;
                table.getUUID();
                result = CATALOG + ".db." + name + ".uuid";
                minTimes = 0;
            }
        };
    }

    @Test
    public void testNativeHistogramWithoutMetaSkipsStateCheckAndCache(@Mocked Table table) {
        new Expectations() {
            {
                table.getId();
                result = TABLE_ID;
                minTimes = 0;
            }
        };
        CachedStatisticStorage storage = new CachedStatisticStorage();
        AsyncLoadingCache<ColumnStatsCacheKey, Optional<Histogram>> cache = countingHistogramCache();
        Deencapsulation.setField(storage, "histogramCache", cache);

        // The map is empty.
        Assertions.assertTrue(storage.getHistogramStatistics(table, List.of("c", "d")).isEmpty());

        // The map has an entry for another table and for another column of this table.
        HistogramStatsMeta otherTable = histogramMeta(TABLE_ID + 1, "c");
        HistogramStatsMeta otherColumn = histogramMeta(TABLE_ID, "e");
        analyzeMgr().replayAddHistogramStatsMeta(otherTable);
        analyzeMgr().replayAddHistogramStatsMeta(otherColumn);
        try {
            Assertions.assertTrue(storage.getHistogramStatistics(table, List.of("c", "d")).isEmpty());
        } finally {
            analyzeMgr().replayRemoveHistogramStatsMeta(otherTable);
            analyzeMgr().replayRemoveHistogramStatsMeta(otherColumn);
        }
        Assertions.assertEquals(0, stateChecks.get());
        Assertions.assertEquals(0, blacklistChecks.get());
        Assertions.assertEquals(0, loads.get());
        Assertions.assertEquals(0, cache.synchronous().estimatedSize());
    }

    @Test
    public void testNativeHistogramWithMetaLoadsOnlyColumnsWithMeta(@Mocked Table table) {
        new Expectations() {
            {
                table.getId();
                result = TABLE_ID;
                minTimes = 0;
            }
        };
        CachedStatisticStorage storage = new CachedStatisticStorage();
        AsyncLoadingCache<ColumnStatsCacheKey, Optional<Histogram>> cache = countingHistogramCache();
        Deencapsulation.setField(storage, "histogramCache", cache);
        HistogramStatsMeta meta = histogramMeta(TABLE_ID, "c");
        analyzeMgr().replayAddHistogramStatsMeta(meta);
        try {
            Map<String, Histogram> result = storage.getHistogramStatistics(table, List.of("c", "d"));
            Assertions.assertEquals(Map.of("c", HISTOGRAM), result);
            Assertions.assertEquals(1, stateChecks.get());
            Assertions.assertEquals(1, blacklistChecks.get());
            Assertions.assertEquals(1, loads.get());
        } finally {
            analyzeMgr().replayRemoveHistogramStatsMeta(meta);
        }
    }

    @Test
    public void testConnectorHistogramWithoutMetaSkipsCache(@Mocked Table table) {
        expectExternalTable(table, "t");
        CachedStatisticStorage storage = new CachedStatisticStorage();
        AsyncCacheLoader<ConnectorTableColumnKey, Optional<Histogram>> loader = (key, executor) -> {
            loads.incrementAndGet();
            return CompletableFuture.completedFuture(Optional.of(HISTOGRAM));
        };
        AsyncLoadingCache<ConnectorTableColumnKey, Optional<Histogram>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(loader);
        Deencapsulation.setField(storage, "connectorHistogramCache", cache);

        Assertions.assertTrue(storage.getConnectorHistogramStatistics(table, List.of("c", "d")).isEmpty());

        // Meta of another table of the same database does not make this table look up the cache.
        analyzeMgr().replayAddExternalHistogramStatsMeta(externalHistogram("other", "c"));
        try {
            Assertions.assertTrue(storage.getConnectorHistogramStatistics(table, List.of("c", "d")).isEmpty());
            Assertions.assertEquals(0, loads.get());
            Assertions.assertEquals(0, cache.synchronous().estimatedSize());
            new Verifications() {
                {
                    table.getUUID();
                    times = 0;
                }
            };

            // With meta for one column the lookup is the one it was before: all requested columns go to the cache.
            analyzeMgr().replayAddExternalHistogramStatsMeta(externalHistogram("t", "c"));
            Map<String, Histogram> result = storage.getConnectorHistogramStatistics(table, List.of("c", "d"));
            Assertions.assertEquals(Map.of("c", HISTOGRAM, "d", HISTOGRAM), result);
            Assertions.assertEquals(2, loads.get());

            // Removing the meta, as a follower does when it replays the journal, brings the fast path back.
            analyzeMgr().replayRemoveExternalHistogramStatsMeta(externalHistogram("t", "c"));
            Assertions.assertFalse(analyzeMgr().hasExternalHistogramStatsMeta(table));
            Assertions.assertTrue(storage.getConnectorHistogramStatistics(table, List.of("c", "d")).isEmpty());
            Assertions.assertEquals(2, loads.get());
        } finally {
            analyzeMgr().replayRemoveExternalHistogramStatsMeta(externalHistogram("other", "c"));
            analyzeMgr().replayRemoveExternalHistogramStatsMeta(externalHistogram("t", "c"));
        }
    }

    @Test
    public void testConnectorHistogramOfTableThatCannotBeAnalyzed(@Mocked Table table) {
        new Expectations() {
            {
                table.isAnalyzableExternalTable();
                result = false;
                minTimes = 0;
            }
        };
        analyzeMgr().replayAddExternalHistogramStatsMeta(externalHistogram("t", "c"));
        try {
            Assertions.assertFalse(analyzeMgr().hasExternalHistogramStatsMeta(table));
        } finally {
            analyzeMgr().replayRemoveExternalHistogramStatsMeta(externalHistogram("t", "c"));
        }
    }

    @Test
    public void testMultiColumnWithoutMetaSkipsChecksAndCache() {
        CachedStatisticStorage storage = new CachedStatisticStorage();
        AsyncCacheLoader<Long, Optional<MultiColumnCombinedStatistics>> loader = (key, executor) -> {
            loads.incrementAndGet();
            return CompletableFuture.completedFuture(
                    Optional.of(new MultiColumnCombinedStatistics(Set.of(1, 2), 42)));
        };
        AsyncLoadingCache<Long, Optional<MultiColumnCombinedStatistics>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(loader);
        Deencapsulation.setField(storage, "multiColumnStats", cache);

        Assertions.assertSame(MultiColumnCombinedStatistics.EMPTY, storage.getMultiColumnCombinedStatistics(TABLE_ID));
        analyzeMgr().replayAddMultiColumnStatsMeta(multiColumn(TABLE_ID + 1, Set.of(1, 2)));
        try {
            Assertions.assertSame(MultiColumnCombinedStatistics.EMPTY,
                    storage.getMultiColumnCombinedStatistics(TABLE_ID));
            Assertions.assertEquals(0, stateChecks.get());
            Assertions.assertEquals(0, blacklistChecks.get());
            Assertions.assertEquals(0, loads.get());
            Assertions.assertEquals(0, cache.synchronous().estimatedSize());

            analyzeMgr().replayAddMultiColumnStatsMeta(multiColumn(TABLE_ID, Set.of(1, 2)));
            MultiColumnCombinedStatistics result = storage.getMultiColumnCombinedStatistics(TABLE_ID);
            Assertions.assertEquals(Map.of(Set.of(1, 2), 42L), result.getDistinctCounts());
            Assertions.assertEquals(1, stateChecks.get());
            Assertions.assertEquals(1, blacklistChecks.get());
            Assertions.assertEquals(1, loads.get());

            analyzeMgr().replayRemoveMultiColumnStatsMeta(multiColumn(TABLE_ID, Set.of(1, 2)));
            cache.synchronous().invalidateAll();
            Assertions.assertSame(MultiColumnCombinedStatistics.EMPTY, storage.getMultiColumnCombinedStatistics(TABLE_ID));
            Assertions.assertEquals(1, loads.get());
        } finally {
            analyzeMgr().replayRemoveMultiColumnStatsMeta(multiColumn(TABLE_ID + 1, Set.of(1, 2)));
            analyzeMgr().replayRemoveMultiColumnStatsMeta(multiColumn(TABLE_ID, Set.of(1, 2)));
        }
    }

    @Test
    public void testExternalMcvWithoutMetaSkipsStateCheckAndCache(@Mocked Table table) {
        expectExternalTable(table, "t");
        new Expectations() {
            {
                table.isIcebergTable();
                result = true;
                minTimes = 0;
            }
        };
        CachedStatisticStorage storage = new CachedStatisticStorage();
        AsyncCacheLoader<String, Optional<ExternalMcvStatistics>> loader = (key, executor) -> {
            loads.incrementAndGet();
            return CompletableFuture.completedFuture(Optional.empty());
        };
        AsyncLoadingCache<String, Optional<ExternalMcvStatistics>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(loader);
        Deencapsulation.setField(storage, "externalMcvStats", cache);

        Assertions.assertSame(ExternalMcvStatistics.EMPTY, storage.getExternalMcvStatistics(table));
        Assertions.assertEquals(0, stateChecks.get());
        Assertions.assertEquals(0, loads.get());
        Assertions.assertEquals(0, cache.synchronous().estimatedSize());
    }

    @Test
    public void testIndexesFollowTheMetaMaps() throws Exception {
        AnalyzeMgr manager = new AnalyzeMgr();
        Long tableId = TABLE_ID;

        // Replaying an update of the same meta must not count it twice.
        MultiColumnStatsMeta first = multiColumn(TABLE_ID, Set.of(1, 2));
        MultiColumnStatsMeta second = multiColumn(TABLE_ID, Set.of(3));
        Assertions.assertFalse(manager.hasMultiColumnStatsMeta(tableId));
        manager.replayAddMultiColumnStatsMeta(first);
        manager.replayAddMultiColumnStatsMeta(first);
        manager.replayAddMultiColumnStatsMeta(second);
        Assertions.assertTrue(manager.hasMultiColumnStatsMeta(tableId));
        manager.replayRemoveMultiColumnStatsMeta(first);
        Assertions.assertTrue(manager.hasMultiColumnStatsMeta(tableId), "The other column group remains");
        manager.replayRemoveMultiColumnStatsMeta(first);
        Assertions.assertTrue(manager.hasMultiColumnStatsMeta(tableId));
        manager.replayRemoveMultiColumnStatsMeta(second);
        Assertions.assertFalse(manager.hasMultiColumnStatsMeta(tableId));
        Assertions.assertTrue(manager.getMultiColumnStatsMetaMap().isEmpty());

        // The index is rebuilt from the image.
        manager.replayAddMultiColumnStatsMeta(first);
        manager.replayAddExternalHistogramStatsMeta(externalHistogram("t", "c"));
        manager.replayAddExternalHistogramStatsMeta(externalHistogram("t", "d"));
        var image = new UtFrameUtils.PseudoImage();
        manager.save(image.getImageWriter());
        AnalyzeMgr restored = new AnalyzeMgr();
        var reader = image.getMetaBlockReader();
        try {
            restored.load(reader);
        } finally {
            reader.close();
        }
        Assertions.assertTrue(restored.hasMultiColumnStatsMeta(tableId));
        Assertions.assertFalse(restored.hasMultiColumnStatsMeta(tableId + 1));
        Assertions.assertEquals(2, restored.getExternalHistogramStatsMetaMap().size());
        restored.replayRemoveExternalHistogramStatsMeta(externalHistogram("t", "c"));
        restored.replayRemoveExternalHistogramStatsMeta(externalHistogram("t", "d"));
        Assertions.assertTrue(restored.getExternalHistogramStatsMetaMap().isEmpty());
    }

    @Test
    public void testExternalHistogramIndexFollowsTheMetaMap(@Mocked Table table) {
        expectExternalTable(table, "t");
        AnalyzeMgr manager = new AnalyzeMgr();
        Assertions.assertFalse(manager.hasExternalHistogramStatsMeta(table));
        manager.replayAddExternalHistogramStatsMeta(externalHistogram("t", "c"));
        manager.replayAddExternalHistogramStatsMeta(externalHistogram("t", "c"));
        manager.replayAddExternalHistogramStatsMeta(externalHistogram("t", "d"));
        Assertions.assertTrue(manager.hasExternalHistogramStatsMeta(table));
        manager.replayRemoveExternalHistogramStatsMeta(externalHistogram("t", "c"));
        Assertions.assertTrue(manager.hasExternalHistogramStatsMeta(table), "Another column remains");
        manager.replayRemoveExternalHistogramStatsMeta(externalHistogram("t", "d"));
        Assertions.assertFalse(manager.hasExternalHistogramStatsMeta(table));
        Assertions.assertTrue(manager.getExternalHistogramStatsMetaMap().isEmpty());
    }

    @Test
    public void testExternalHistogramIndexIgnoresCaseOfNames(@Mocked Table table) {
        expectExternalTable(table, "t");
        AnalyzeMgr manager = new AnalyzeMgr();
        ExternalHistogramStatsMeta upper = new ExternalHistogramStatsMeta(CATALOG.toUpperCase(), "DB", "T", "c",
                StatsConstants.AnalyzeType.HISTOGRAM, LocalDateTime.now(), Map.of());
        manager.replayAddExternalHistogramStatsMeta(upper);
        Assertions.assertTrue(manager.hasExternalHistogramStatsMeta(table));
        manager.replayRemoveExternalHistogramStatsMeta(upper);
        Assertions.assertFalse(manager.hasExternalHistogramStatsMeta(table));
        Assertions.assertTrue(manager.getExternalHistogramStatsMetaMap().isEmpty());
    }
}
