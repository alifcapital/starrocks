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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.statistics.ConnectorColumnStatsCacheLoader;
import com.starrocks.connector.statistics.ConnectorTableColumnKey;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.connector.statistics.ConnectorTableTriggerAnalyzeMgr;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.ExternalMcvStatsMeta;
import com.starrocks.statistic.HistogramStatsMeta;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class StatisticsCacheLoadFailureTest {
    private static final String UUID = "iceberg.db.t.uuid";
    private static final ConnectorTableColumnKey KEY = new ConnectorTableColumnKey(UUID, "c");
    private final AtomicBoolean ready = new AtomicBoolean(true);
    private final AtomicBoolean available = new AtomicBoolean(true);
    private final AtomicBoolean queryFails = new AtomicBoolean(false);
    private final AtomicInteger queries = new AtomicInteger();
    private List<TStatisticData> partitionRows = List.of();
    private final List<String> partitionQueries = new ArrayList<>();
    private boolean oldUnitStatistics;
    private boolean oldSync;
    private int oldBlockSize;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @BeforeEach
    public void beforeEach() {
        oldUnitStatistics = FeConstants.enableUnitStatistics;
        oldSync = Config.enable_sync_statistics_load;
        oldBlockSize = Config.external_statistics_partition_block_size;
        Config.external_statistics_partition_block_size = 1; // Exercise the individual-partition loader contract.
        FeConstants.enableUnitStatistics = false;
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isReady() {
                return ready.get();
            }
        };
        new MockUp<StatisticUtils>() {
            @Mock
            public boolean checkStatisticTables(List<String> tables) {
                return available.get();
            }

            @Mock
            public boolean checkStatisticTableStateNormal() {
                return true;
            }
        };
        new MockUp<ConnectorColumnStatsCacheLoader>() {
            @Mock
            public List<TStatisticData> queryStatisticsData(ConnectContext context, String uuid, List<String> columns) {
                queries.incrementAndGet();
                if (queryFails.get()) {
                    throw new IllegalStateException("injected statistics query failure");
                }
                return partitionRows;
            }
        };
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<TStatisticData> queryStatisticSync(ConnectContext context, Long dbId, Long tableId,
                                                           List<String> columns) {
                queries.incrementAndGet();
                if (queryFails.get()) {
                    throw new IllegalStateException("injected native statistics query failure");
                }
                return List.of();
            }

            @Mock
            public List<TStatisticData> queryHistogram(ConnectContext context, Long tableId, List<String> columns) {
                queries.incrementAndGet();
                if (queryFails.get()) {
                    throw new IllegalStateException("injected native histogram query failure");
                }
                return List.of();
            }

            @Mock
            public List<List<String>> executeStatisticJsonDQL(ConnectContext context, String sql) {
                queries.incrementAndGet();
                partitionQueries.add(sql);
                if (queryFails.get()) {
                    throw new IllegalStateException("injected statistics query failure");
                }
                return List.of();
            }

            @Mock
            public List<TStatisticData> executeStatisticDQL(ConnectContext context, String sql) {
                queries.incrementAndGet();
                partitionQueries.add(sql);
                if (queryFails.get()) {
                    throw new IllegalStateException("injected binary statistics query failure");
                }
                return partitionRows;
            }
        };

    }

    private void mockPartitionMetadata() {
        mockPartitionMetadata(false);
    }

    private void mockPartitionMetadata(boolean unpartitioned) {
        Table partitionTable = org.mockito.Mockito.mock(Table.class);
        org.mockito.Mockito.when(partitionTable.getColumn(org.mockito.Mockito.anyString()))
                .thenAnswer(call -> new Column(call.getArgument(0), IntegerType.BIGINT));
        org.mockito.Mockito.when(partitionTable.isUnPartitioned()).thenReturn(unpartitioned);
        new MockUp<StatisticsUtils>() {
            @Mock
            public Table getTableByUUID(ConnectContext context, String uuid) {
                return partitionTable;
            }

            @Mock
            public ConnectorTableColumnStats estimateColumnStatistics(Table table, String column, ConnectorTableColumnStats raw) {
                return raw;
            }
        };
    }

    @Test
    public void testUnifiedTableLoaderUsesBeAggregatesAndGroupsDifferentTables() {
        Table table = org.mockito.Mockito.mock(Table.class);
        org.mockito.Mockito.when(table.getColumn(org.mockito.Mockito.anyString()))
                .thenAnswer(call -> new Column(call.getArgument(0), IntegerType.BIGINT));
        new MockUp<StatisticsUtils>() {
            @Mock
            public Table getTableByUUID(ConnectContext context, String uuid) {
                return table;
            }

            @Mock
            public ConnectorTableColumnStats estimateColumnStatistics(Table t, String column, ConnectorTableColumnStats raw) {
                return new ConnectorTableColumnStats(raw.getColumnStatistic(), raw.getRowCount() * 2, raw.getUpdateTime());
            }
        };
        List<String> tables = new ArrayList<>();
        new MockUp<ConnectorColumnStatsCacheLoader>() {
            @Mock
            public List<TStatisticData> queryStatisticsData(ConnectContext context, String uuid, List<String> columns) {
                tables.add(uuid);
                // A scalar BE aggregate has no HLL payload or partition key.
                return List.of(new TStatisticData().setColumnName("c").setRowCount(100).setDataSize(800)
                        .setNullCount(10).setCountDistinct(7).setMin("1").setMax("9").setUpdateTime("2026-09-21 00:00:00"));
            }
        };
        String other = "iceberg.db.other.uuid";
        var key = ExternalStatisticsCacheKey.table(UUID, "c");
        var missing = ExternalStatisticsCacheKey.table(UUID, "missing");
        var otherKey = ExternalStatisticsCacheKey.table(other, "c");
        var loaded = new ExternalStatisticsCacheLoader().asyncLoadAll(List.of(key, missing, otherKey), Runnable::run).join();
        Assertions.assertEquals(List.of(UUID, other), tables);
        Assertions.assertEquals(Optional.empty(), loaded.get(missing));
        for (var present : List.of(key, otherKey)) {
            var summary = (ExternalColumnStatistics.Summary) loaded.get(present).orElseThrow();
            Assertions.assertEquals(200, summary.rowCount);
            Assertions.assertEquals(100, summary.rawRowCount);
            Assertions.assertEquals(7, summary.statistic.getDistinctValuesCount());
            Assertions.assertEquals(8, summary.statistic.getAverageRowSize());
            Assertions.assertEquals(0.1, summary.statistic.getNullsFraction(), 0.0001);
            Assertions.assertEquals("2026-09-21 00:00:00", summary.updateTime);
        }
        Assertions.assertTrue(partitionQueries.isEmpty(), "Whole-table loading must not query partition HLLs");
    }

    @AfterEach
    public void afterEach() {
        FeConstants.enableUnitStatistics = oldUnitStatistics;
        Config.enable_sync_statistics_load = oldSync;
        Config.external_statistics_partition_block_size = oldBlockSize;
        ConnectContext.remove();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBasicStartupFailureIsRetried(boolean bulk) {
        AsyncLoadingCache<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ConnectorColumnStatsCacheLoader());
        ready.set(false);
        Assertions.assertThrows(CompletionException.class, () -> loadBasic(cache, bulk));
        Assertions.assertEquals(0, queries.get());
        ready.set(true);
        loadBasic(cache, bulk);
        loadBasic(cache, bulk);
        Assertions.assertEquals(1, queries.get(), "A successful empty read must remain cached");
        Assertions.assertEquals(Optional.empty(), cache.get(KEY).join());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBasicQueryFailureIsRetried(boolean bulk) {
        AsyncLoadingCache<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ConnectorColumnStatsCacheLoader());
        queryFails.set(true);
        Assertions.assertThrows(CompletionException.class, () -> loadBasic(cache, bulk));
        queryFails.set(false);
        loadBasic(cache, bulk);
        loadBasic(cache, bulk);
        Assertions.assertEquals(2, queries.get());
    }

    private static void loadBasic(
            AsyncLoadingCache<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> cache, boolean bulk) {
        if (bulk) {
            cache.getAll(List.of(KEY)).join();
        } else {
            cache.get(KEY).join();
        }
    }

    @Test
    public void testBasicFailedRefreshPreservesLastGoodValue() {
        AsyncLoadingCache<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ConnectorColumnStatsCacheLoader());
        Optional<ConnectorTableColumnStats> old = Optional.of(new ConnectorTableColumnStats(
                ColumnStatistic.builder().setDistinctValuesCount(5).build(), 100, "2026-09-21 00:00:00"));
        cache.synchronous().put(KEY, old);
        queryFails.set(true);
        cache.synchronous().refresh(KEY);
        Assertions.assertSame(old, cache.get(KEY).join());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMcvUnavailableIsRetried(boolean beforeReady) {
        AsyncLoadingCache<String, Optional<ExternalMcvStatistics>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ExternalMcvStatsCacheLoader());
        ready.set(!beforeReady);
        available.set(beforeReady);
        Assertions.assertThrows(CompletionException.class, () -> cache.get(UUID).join());
        Assertions.assertEquals(0, queries.get());
        ready.set(true);
        available.set(true);
        Assertions.assertEquals(Optional.empty(), cache.get(UUID).join());
        cache.get(UUID).join();
        Assertions.assertEquals(1, queries.get());
    }

    @Test
    public void testMcvLoadPreparesPresentColumnsAndToleratesRemovedColumns(@Mocked Table table) {
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<List<String>> executeStatisticJsonDQL(ConnectContext context, String sql) {
                return List.of(
                        List.of("[\"c\"]", "100", "1", "[[[\"x\"],\"100\",[\"100\"]]]", "[]", "[0]"),
                        List.of("[\"removed\"]", "100", "1", "[[[\"x\"],\"100\",[\"100\"]]]", "[]", "[0]"));
            }
        };
        new MockUp<StatisticsUtils>() {
            @Mock
            public Table getTableByUUID(ConnectContext context, String uuid) {
                return table;
            }
        };
        new Expectations() {{
                table.getColumn("c");
                result = new Column("c", VarcharType.VARCHAR);
                table.getColumn("removed");
                result = null;
                table.getName();
                result = "t";
            }};
        ExternalMcvStatistics stats = new ExternalMcvStatsCacheLoader().asyncLoad(UUID, Runnable::run).join().orElseThrow();
        Assertions.assertEquals(2, stats.getGroups().size());
        ExternalMcvStatistics.Group present = stats.getGroups().get(0);
        Assertions.assertNotNull(Deencapsulation.getField(present, "preparedColumn"));
        Assertions.assertTrue(present.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown())
                .orElseThrow().getHistogram().hasStringValues());
        Assertions.assertNull(Deencapsulation.getField(stats.getGroups().get(1), "preparedColumn"));
    }

    @Test
    public void testMcvFailedRefreshPreservesLastGoodValue() {
        AsyncLoadingCache<String, Optional<ExternalMcvStatistics>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ExternalMcvStatsCacheLoader());
        Optional<ExternalMcvStatistics> old = Optional.of(new ExternalMcvStatistics(List.of(
                new ExternalMcvStatistics.Group(List.of("c"), 100, 1,
                        List.of(new MultiColumnCombinedStats.McvEntry(List.of("x"), 100, List.of(100L))),
                        List.of(), List.of(0L)))));
        cache.synchronous().put(UUID, old);
        available.set(false);
        cache.synchronous().refresh(UUID);
        Assertions.assertSame(old, cache.get(UUID).join());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPartitionUnavailableIsRetried(boolean beforeReady) {
        mockPartitionMetadata();
        AsyncLoadingCache<ExternalStatisticsCacheKey,
                Optional<ExternalColumnStatistics>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ExternalStatisticsCacheLoader());
        ExternalStatisticsCacheKey key = new ExternalStatisticsCacheKey(UUID, "p=1", "c");
        ready.set(!beforeReady);
        available.set(beforeReady);
        Assertions.assertThrows(CompletionException.class, () -> cache.getAll(List.of(key)).join());
        Assertions.assertEquals(0, queries.get());
        ready.set(true);
        available.set(true);
        Assertions.assertEquals(Optional.empty(), cache.getAll(List.of(key)).join().get(key));
        cache.getAll(List.of(key)).join();
        Assertions.assertEquals(1, queries.get());
    }

    @Test
    public void testPartitionCacheLoadsOnlyMissingColumnsAndInvalidatesAll() {
        mockPartitionMetadata();
        Config.enable_sync_statistics_load = true;
        CachedStatisticStorage storage = new CachedStatisticStorage();
        AsyncLoadingCache<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ExternalStatisticsCacheLoader());
        Deencapsulation.setField(storage, "externalStatisticsCache", cache);
        partitionRows = List.of(ExternalPartitionStatisticsTest.row("p=1", "a", 100, 10, 0, "1", "10"));
        ExternalStatisticsAggregate first = storage.loadExternalStatistics(
                new ExternalStatisticsRequest(UUID, List.of("p=1"), List.of("a"))).join();
        Assertions.assertEquals(10, first.columns.get("a").getDistinctValuesCount());
        Assertions.assertEquals(1, queries.get());
        Assertions.assertTrue(partitionQueries.get(0).contains("column_name IN ('a')"));

        partitionRows = List.of(ExternalPartitionStatisticsTest.row("p=1", "b", 100, 20, 0, "1", "20"));
        ExternalStatisticsAggregate both = storage.loadExternalStatistics(
                new ExternalStatisticsRequest(UUID, List.of("p=1"), List.of("a", "b", "missing"))).join();
        Assertions.assertEquals(3, both.columns.size());
        Assertions.assertTrue(both.columns.get("missing").isUnknown());
        Assertions.assertEquals(first.columns.get("a").getDistinctValuesCount(), both.columns.get("a").getDistinctValuesCount());
        Assertions.assertEquals(2, queries.get());
        Assertions.assertFalse(partitionQueries.get(1).contains("'a'"), "Cached column must not be fetched again");
        Assertions.assertTrue(partitionQueries.get(1).contains("'b'"));
        Assertions.assertTrue(partitionQueries.get(1).contains("'missing'"));
        storage.loadExternalStatistics(new ExternalStatisticsRequest(UUID, List.of("p=1"), List.of("a", "b", "missing"))).join();
        Assertions.assertEquals(2, queries.get(), "Known absence must also be cached");

        ExternalStatisticsCacheKey pending = new ExternalStatisticsCacheKey(UUID, "p=2", "a");
        cache.put(pending, new CompletableFuture<>());
        ExternalStatisticsCacheKey unrelated = new ExternalStatisticsCacheKey("another", "p=1", "a");
        cache.put(unrelated, CompletableFuture.completedFuture(Optional.empty()));
        storage.expireExternalPartitionStatistics(UUID);
        Assertions.assertEquals(1, cache.asMap().size());
        Assertions.assertTrue(cache.asMap().containsKey(unrelated));
    }

    @Test
    public void testUnpartitionedLoadPreparesOneBeSummaryWithoutBinaryPartitionQuery() throws Exception {
        mockPartitionMetadata(true);
        partitionRows = List.of(ExternalPartitionStatisticsTest.row("", "a", 100, 50, 0, "1", "100").setCountDistinct(50));
        CachedStatisticStorage storage = new CachedStatisticStorage();
        ExternalStatisticsRequest request = new ExternalStatisticsRequest(UUID, List.of(""), List.of("a"), true, true);
        ExternalStatisticsAggregate first = storage.loadExternalStatistics(request).get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(50, first.columns.get("a").getDistinctValuesCount());
        Assertions.assertEquals(1, queries.get());
        Assertions.assertTrue(partitionQueries.isEmpty(), "Unpartitioned TABLE load must not query binary partition statistics");
        Assertions.assertSame(first.columns.get("a"),
                storage.loadExternalStatistics(request).get(5, TimeUnit.SECONDS).columns.get("a"));
        Assertions.assertEquals(1, queries.get());
        Assertions.assertEquals(1, storage.externalStatisticsCache.asMap().size());
        Assertions.assertTrue(storage.externalStatisticsCache.asMap().values().iterator().next().join().orElseThrow()
                instanceof ExternalColumnStatistics.Summary);
    }

    @Test
    public void testScopedLoadsPreserveQueryTriggeredAnalyzeWithoutAdditionalReads() throws Exception {
        mockPartitionMetadata(true);
        partitionRows = List.of(ExternalPartitionStatisticsTest.row("", "a", 100, 50, 0, "1", "100").setCountDistinct(50));
        AtomicInteger checks = new AtomicInteger();
        CompletableFuture<Void> observed = new CompletableFuture<>();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        new MockUp<ConnectorTableTriggerAnalyzeMgr>() {
            @Mock
            public void checkAndUpdateScopedTableStats(String uuid, Map<String, ColumnStatistic> columns,
                                                      double rows, boolean whole) {
                Assertions.assertEquals(UUID, uuid);
                Assertions.assertEquals(100, rows);
                Assertions.assertTrue(whole);
                Assertions.assertEquals(50, columns.get("a").getDistinctValuesCount());
                checks.incrementAndGet();
                observed.complete(null);
            }
        };
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = UtFrameUtils.createDefaultCtx();
        context.setThreadLocalInfo();
        CachedStatisticStorage storage = new CachedStatisticStorage();
        try {
            ExternalStatisticsRequest request = new ExternalStatisticsRequest(UUID, List.of(""), List.of("a"), true, true);
            storage.loadExternalStatistics(request).get(5, TimeUnit.SECONDS);
            observed.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(1, queries.get());
            Deencapsulation.setField(context.getSessionVariable(), "enableQueryTriggerAnalyze", false);
            storage.loadExternalStatistics(request).get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(1, checks.get());
            Deencapsulation.setField(context.getSessionVariable(), "enableQueryTriggerAnalyze", true);
            queryFails.set(true);
            storage.expireExternalPartitionStatistics(UUID);
            Assertions.assertThrows(ExecutionException.class,
                    () -> storage.loadExternalStatistics(request).get(5, TimeUnit.SECONDS));
            Assertions.assertEquals(1, checks.get(), "A failed read must not trigger collection as if statistics were absent");
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testPartitionFailedRefreshRetainsColumnAndRetriesNewColumn() {
        mockPartitionMetadata();
        AsyncLoadingCache<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new ExternalStatisticsCacheLoader());
        ExternalStatisticsCacheKey key = new ExternalStatisticsCacheKey(UUID, "p=1", "a");
        partitionRows = List.of(ExternalPartitionStatisticsTest.row("p=1", "a", 100, 10, 0, "1", "10"));
        Optional<ExternalColumnStatistics> old = cache.get(key).join();
        queryFails.set(true);
        cache.synchronous().refresh(key);
        Assertions.assertSame(old, cache.get(key).join());
        ExternalStatisticsCacheKey missing = new ExternalStatisticsCacheKey(UUID, "p=1", "b");
        Assertions.assertThrows(CompletionException.class, () -> cache.get(missing).join());
        queryFails.set(false);
        partitionRows = List.of(ExternalPartitionStatisticsTest.row("p=1", "b", 100, 20, 0, "1", "20"));
        StatisticsHll.Union union = new StatisticsHll.Union();
        union.merge(((ExternalColumnStatistics.Partition) cache.get(missing).join().orElseThrow()).getHll());
        Assertions.assertEquals(20, union.estimate());
    }

    @Test
    public void testConcurrentIdenticalRequestsReserveOneWholeBatch() throws Exception {
        ConcurrentLinkedQueue<Map<ExternalStatisticsCacheKey,
                Optional<ExternalColumnStatistics>>> batches = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<CompletableFuture<Map<ExternalStatisticsCacheKey,
                Optional<ExternalColumnStatistics>>>> pending = new ConcurrentLinkedQueue<>();
        AsyncLoadingCache<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(new AsyncCacheLoader<>() {
                    @Override
                    public CompletableFuture<Optional<ExternalColumnStatistics>> asyncLoad(
                            ExternalStatisticsCacheKey key, Executor executor) {
                        throw new AssertionError("Must batch concurrent loads");
                    }

                    @Override
                    public CompletableFuture<Map<ExternalStatisticsCacheKey,
                            Optional<ExternalColumnStatistics>>> asyncLoadAll(
                            Iterable<? extends ExternalStatisticsCacheKey> keys, Executor executor) {
                        Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> result =
                                new java.util.HashMap<>();
                        keys.forEach(key -> result.put(key, Optional.empty()));
                        batches.add(result);
                        CompletableFuture<Map<ExternalStatisticsCacheKey,
                                Optional<ExternalColumnStatistics>>> future = new CompletableFuture<>();
                        pending.add(future);
                        return future;
                    }
                });
        CachedStatisticStorage storage = new CachedStatisticStorage();
        Deencapsulation.setField(storage, "externalStatisticsCache", cache);
        ExternalStatisticsRequest request = new ExternalStatisticsRequest(UUID,
                java.util.stream.IntStream.range(0, 1000).mapToObj(i -> "p=" + i).toList(), List.of("c"));
        ExecutorService clients = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < 20; round++) {
                cache.synchronous().invalidateAll();
                batches.clear();
                pending.clear();
                CyclicBarrier start = new CyclicBarrier(2);
                java.util.concurrent.Callable<CompletableFuture<ExternalStatisticsAggregate>> task = () -> {
                    start.await();
                    return storage.loadExternalStatistics(request);
                };
                var first = clients.submit(task);
                var second = clients.submit(task);
                CompletableFuture<ExternalStatisticsAggregate> a = first.get(10, TimeUnit.SECONDS);
                CompletableFuture<ExternalStatisticsAggregate> b = second.get(10, TimeUnit.SECONDS);
                Assertions.assertEquals(1, batches.size(), "Concurrent readers must not split one SQL batch");
                Assertions.assertEquals(1000, batches.peek().size());
                pending.remove().complete(batches.remove());
                Assertions.assertTrue(a.join().isEmpty());
                Assertions.assertTrue(b.join().isEmpty());
            }
        } finally {
            clients.shutdownNow();
        }
    }

    @Test
    public void testPartitionRefreshBatchesFailuresAndInvalidation() {
        boolean oldRefresh = Config.enable_statistic_cache_refresh_after_write;
        long oldInterval = Config.statistic_update_interval_sec;
        try {
            Config.enable_statistic_cache_refresh_after_write = true;
            Config.statistic_update_interval_sec = 3600;
            AtomicLong nanos = new AtomicLong();
            AsyncLoadingCache<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> cache =
                    Caffeine.newBuilder().expireAfterWrite(2, TimeUnit.DAYS).ticker(nanos::get)
                            .executor(Runnable::run).buildAsync((key, executor) -> {
                                throw new AssertionError("All requested cells are already cached");
                            });
            CachedStatisticStorage storage = new CachedStatisticStorage();
            Deencapsulation.setField(storage, "externalStatisticsCache", cache);
            ExternalStatisticsRequest request = new ExternalStatisticsRequest(UUID, List.of("p=1"), List.of("a", "b"));
            ExternalStatisticsCacheKey a = new ExternalStatisticsCacheKey(UUID, "p=1", "a");
            ExternalStatisticsCacheKey b = new ExternalStatisticsCacheKey(UUID, "p=1", "b");
            Optional<ExternalColumnStatistics> old = Optional.of(new ExternalColumnStatistics.Partition(
                    ExternalPartitionStatisticsTest.row("p=1", "a", 100, 10, 0, "1", "10"), IntegerType.BIGINT));
            cache.synchronous().putAll(Map.of(a, old, b, old));
            nanos.set(TimeUnit.HOURS.toNanos(2));
            List<List<ExternalStatisticsCacheKey>> batches = new ArrayList<>();
            List<CompletableFuture<Map<ExternalStatisticsCacheKey,
                    Optional<ExternalColumnStatistics>>>> pending = new ArrayList<>();
            new MockUp<ExternalStatisticsCacheLoader>() {
                @Mock
                public CompletableFuture<Map<ExternalStatisticsCacheKey,
                        Optional<ExternalColumnStatistics>>> asyncLoadAll(
                        Iterable<? extends ExternalStatisticsCacheKey> keys, Executor executor) {
                    List<ExternalStatisticsCacheKey> batch = new ArrayList<>();
                    keys.forEach(batch::add);
                    batches.add(batch);
                    CompletableFuture<Map<ExternalStatisticsCacheKey,
                            Optional<ExternalColumnStatistics>>> future = new CompletableFuture<>();
                    pending.add(future);
                    return future;
                }
            };
            Assertions.assertEquals(100, storage.loadExternalStatistics(request).join().rowCount);
            Assertions.assertEquals(1, batches.size());
            Assertions.assertEquals(java.util.Set.of(a, b), java.util.Set.copyOf(batches.get(0)));
            storage.loadExternalStatistics(request).join();
            Assertions.assertEquals(1, batches.size(), "Readers share the pending batch refresh");
            pending.get(0).completeExceptionally(new IllegalStateException("injected refresh failure"));
            Assertions.assertSame(old, cache.synchronous().getIfPresent(a));
            storage.loadExternalStatistics(request).join();
            Assertions.assertEquals(2, batches.size(), "A failed refresh can be retried");
            storage.expireExternalPartitionStatistics(UUID);
            cache.synchronous().put(a, Optional.empty());
            pending.get(1).complete(Map.of(a, old, b, old));
            Assertions.assertEquals(Optional.empty(), cache.synchronous().getIfPresent(a));
            Assertions.assertNull(cache.synchronous().getIfPresent(b), "Old refresh must not resurrect invalidated cells");
        } finally {
            Config.enable_statistic_cache_refresh_after_write = oldRefresh;
            Config.statistic_update_interval_sec = oldInterval;
        }
    }

    @Test
    public void testPartitionLoadsBoundInflightBatchesAndDoNotTruncateOnEviction() {
        List<List<ExternalStatisticsCacheKey>> batches = new ArrayList<>();
        List<CompletableFuture<Map<ExternalStatisticsCacheKey,
                Optional<ExternalColumnStatistics>>>> pending = new ArrayList<>();
        AsyncLoadingCache<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> cache =
                Caffeine.newBuilder().maximumSize(10).executor(Runnable::run).buildAsync(new AsyncCacheLoader<>() {
                    @Override
                    public CompletableFuture<Optional<ExternalColumnStatistics>> asyncLoad(
                            ExternalStatisticsCacheKey key, Executor executor) {
                        throw new AssertionError("Must batch partition loads");
                    }

                    @Override
                    public CompletableFuture<Map<ExternalStatisticsCacheKey,
                            Optional<ExternalColumnStatistics>>> asyncLoadAll(
                            Iterable<? extends ExternalStatisticsCacheKey> keys, Executor executor) {
                        List<ExternalStatisticsCacheKey> batch = new ArrayList<>();
                        keys.forEach(batch::add);
                        batches.add(batch);
                        CompletableFuture<Map<ExternalStatisticsCacheKey,
                                Optional<ExternalColumnStatistics>>> future = new CompletableFuture<>();
                        pending.add(future);
                        return future;
                    }
                });
        CachedStatisticStorage storage = new CachedStatisticStorage();
        Deencapsulation.setField(storage, "externalStatisticsCache", cache);
        List<String> partitions = java.util.stream.IntStream.range(0, 9000).mapToObj(i -> "p=" + i).toList();
        CompletableFuture<ExternalStatisticsAggregate> result = storage.loadExternalStatistics(
                new ExternalStatisticsRequest(UUID, partitions, List.of("c")));
        Assertions.assertFalse(result.isDone());
        Assertions.assertEquals(2, batches.size(), "Only two batches may be in flight for this request");
        ExternalColumnStatistics.Partition value = new ExternalColumnStatistics.Partition(
                ExternalPartitionStatisticsTest.row("", "c", 100, 10, 0, "1", "10"), IntegerType.BIGINT);
        for (int i = 0; i < 3; i++) {
            Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> loaded = new java.util.HashMap<>();
            batches.get(i).forEach(key -> loaded.put(key, Optional.of(value)));
            pending.get(i).complete(loaded);
        }
        Assertions.assertEquals(3, batches.size());
        ExternalStatisticsAggregate all = result.join();
        Assertions.assertTrue(all.hasCompleteCoverage());
        Assertions.assertEquals(9000, all.knownPartitions);
        Assertions.assertEquals(900000, all.rowCount);
        Assertions.assertEquals(10, all.columns.get("c").getDistinctValuesCount());
        cache.synchronous().cleanUp();
        Assertions.assertTrue(cache.synchronous().estimatedSize() <= 10);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    public void testNativeQueryFailureIsRetried(boolean histogram, boolean bulk) {
        if (histogram) {
            checkNativeRetry(new ColumnHistogramStatsCacheLoader(), bulk);
        } else {
            checkNativeRetry(new ColumnBasicStatsCacheLoader(), bulk);
        }
    }

    private <V> void checkNativeRetry(AsyncCacheLoader<ColumnStatsCacheKey, Optional<V>> loader, boolean bulk) {
        AsyncLoadingCache<ColumnStatsCacheKey, Optional<V>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(loader);
        ColumnStatsCacheKey key = new ColumnStatsCacheKey(123L, "c");
        queryFails.set(true);
        Assertions.assertThrows(CompletionException.class, () -> {
            if (bulk) {
                cache.getAll(List.of(key)).join();
            } else {
                cache.get(key).join();
            }
        });
        queryFails.set(false);
        if (bulk) {
            cache.getAll(List.of(key)).join();
        } else {
            cache.get(key).join();
        }
        Assertions.assertEquals(Optional.empty(), cache.get(key).join());
        Assertions.assertEquals(2, queries.get(), "Retry failures but cache a successful empty read");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testNativeFailedRefreshPreservesLastGoodValue(boolean histogram) {
        if (histogram) {
            checkNativeRefresh(new ColumnHistogramStatsCacheLoader(), new Histogram(Map.of("x", 100L)));
        } else {
            checkNativeRefresh(new ColumnBasicStatsCacheLoader(),
                    ColumnStatistic.builder().setDistinctValuesCount(5).build());
        }
    }

    private <V> void checkNativeRefresh(AsyncCacheLoader<ColumnStatsCacheKey, Optional<V>> loader, V value) {
        AsyncLoadingCache<ColumnStatsCacheKey, Optional<V>> cache =
                Caffeine.newBuilder().executor(Runnable::run).buildAsync(loader);
        ColumnStatsCacheKey key = new ColumnStatsCacheKey(123L, "c");
        Optional<V> old = Optional.of(value);
        cache.synchronous().put(key, old);
        queryFails.set(true);
        cache.synchronous().refresh(key);
        Assertions.assertSame(old, cache.get(key).join());
        Assertions.assertEquals(1, queries.get());
    }

    @Test
    public void testExplicitRefreshPreservesValueOnFailureButInstallsConfirmedAbsence(@Mocked Table table) {
        UtFrameUtils.createDefaultCtx().setThreadLocalInfo();
        ColumnStatsCacheKey key = new ColumnStatsCacheKey(100000001L, "c");
        AtomicReference<CompletableFuture<Map<ColumnStatsCacheKey, Optional<ColumnStatistic>>>> load =
                new AtomicReference<>(new CompletableFuture<>());
        new Expectations() {{
                table.getId();
                result = 100000001L;
            }};
        new MockUp<ColumnBasicStatsCacheLoader>() {
            @Mock
            public CompletableFuture<Map<ColumnStatsCacheKey, Optional<ColumnStatistic>>> asyncLoadAll(
                    Iterable<? extends ColumnStatsCacheKey> keys, Executor executor) {
                return load.get();
            }
        };
        CachedStatisticStorage storage = new CachedStatisticStorage();
        AsyncLoadingCache<ColumnStatsCacheKey, Optional<ColumnStatistic>> cache =
                Deencapsulation.getField(storage, "columnStatistics");
        Optional<ColumnStatistic> old = Optional.of(ColumnStatistic.builder().setDistinctValuesCount(5).build());
        cache.synchronous().put(key, old);
        storage.refreshColumnStatistics(table, List.of("c"), false);
        Assertions.assertTrue(load.get().getNumberOfDependents() > 0, "Refresh must register the load result");
        load.get().completeExceptionally(new IllegalStateException("injected refresh failure"));
        Assertions.assertSame(old, cache.get(key).join());

        load.set(new CompletableFuture<>());
        storage.refreshColumnStatistics(table, List.of("c"), false);
        load.get().complete(Map.of(key, Optional.empty()));
        Assertions.assertEquals(Optional.empty(), cache.get(key).join());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testHistogramQueryRetryPausePreservesWarmValue(boolean warm, @Mocked Table table) {
        Config.enable_sync_statistics_load = true;
        long id = 100000001L;
        ColumnStatsCacheKey key = new ColumnStatsCacheKey(id, "c");
        new Expectations() {{
                table.getId();
                result = id;
            }};
        HistogramStatsMeta meta = new HistogramStatsMeta(1, id, "c", StatsConstants.AnalyzeType.HISTOGRAM,
                LocalDateTime.now(), Map.of());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().getHistogramStatsMetaMap().put(new Pair<>(id, "c"), meta);
        try {
            CachedStatisticStorage storage = new CachedStatisticStorage();
            ColumnHistogramStatsCacheLoader loader = Deencapsulation.getField(storage, "histogramLoader");
            AtomicLong clock = new AtomicLong();
            Deencapsulation.setField(loader, "backoff", new StatisticsLoadBackoff(clock::get));
            AsyncLoadingCache<ColumnStatsCacheKey, Optional<Histogram>> cache = Caffeine.newBuilder()
                    .executor(Runnable::run).buildAsync(loader);
            Deencapsulation.setField(storage, "histogramCache", cache);
            Histogram old = new Histogram(Map.of("x", 100L));
            queryFails.set(true);
            if (warm) {
                cache.synchronous().put(key, Optional.of(old));
                cache.synchronous().refresh(key);
            } else {
                Assertions.assertTrue(storage.getHistogramStatistics(table, List.of("c")).isEmpty());
                Assertions.assertNull(cache.synchronous().policy().getIfPresentQuietly(key),
                        "A failed read must not become a negative cache entry");
            }
            Map<String, Histogram> expected = warm ? Map.of("c", old) : Map.of();
            for (int i = 0; i < 100; ++i) {
                Assertions.assertEquals(expected, storage.getHistogramStatistics(table, List.of("c")));
            }
            Assertions.assertEquals(1, queries.get(), "Query retries must pause during an outage");
            clock.set(TimeUnit.SECONDS.toNanos(59));
            storage.getHistogramStatistics(table, List.of("c"));
            Assertions.assertEquals(1, queries.get());
            queryFails.set(false);
            if (warm) {
                // Explicit ANALYZE refresh must bypass the pause and install confirmed absence.
                storage.refreshHistogramStatistics(table, List.of("c"), true);
            } else {
                clock.set(TimeUnit.SECONDS.toNanos(60));
                storage.getHistogramStatistics(table, List.of("c"));
            }
            Assertions.assertEquals(2, queries.get());
            Assertions.assertEquals(Optional.empty(), cache.synchronous().policy().getIfPresentQuietly(key));
        } finally {
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().getHistogramStatsMetaMap().remove(new Pair<>(id, "c"));
        }
    }

    @Test
    public void testMcvMetadataGateAndOutageRetry(@Mocked Table table) {
        Config.enable_sync_statistics_load = true;
        AtomicReference<String> uuid = new AtomicReference<>(UUID);
        new Expectations() {{
                table.isIcebergTable();
                result = true;
                table.getCatalogName();
                result = "iceberg";
                table.getCatalogDBName();
                result = "db";
                table.getName();
                result = "t";
            }};
        new MockUp<Table>() {
            @Mock
            public String getUUID() {
                return uuid.get();
            }
        };
        AnalyzeMgr mgr = GlobalStateMgr.getCurrentState().getAnalyzeMgr();
        ExternalMcvStatsMeta meta = new ExternalMcvStatsMeta("iceberg", "db", "t", List.of("c"),
                StatsConstants.AnalyzeType.FULL, List.of(), LocalDateTime.now(), Map.of());
        meta.setTableUUID(UUID);
        ExternalMcvStatsMeta second = new ExternalMcvStatsMeta("iceberg", "db", "t", List.of("d"),
                StatsConstants.AnalyzeType.FULL, List.of(), LocalDateTime.now(), Map.of());
        second.setTableUUID(UUID);
        CachedStatisticStorage storage = new CachedStatisticStorage();
        ExternalMcvStatsCacheLoader loader = Deencapsulation.getField(storage, "externalMcvLoader");
        AtomicLong clock = new AtomicLong();
        Deencapsulation.setField(loader, "backoff", new StatisticsLoadBackoff(clock::get));
        AsyncLoadingCache<String, Optional<ExternalMcvStatistics>> cache = Caffeine.newBuilder()
                .executor(Runnable::run).buildAsync(loader);
        Deencapsulation.setField(storage, "externalMcvStats", cache);
        try {
            Assertions.assertSame(ExternalMcvStatistics.EMPTY, storage.getExternalMcvStatistics(table));
            storage.prefetchExternalMcvStatistics(table);
            Assertions.assertEquals(0, queries.get());
            Assertions.assertTrue(cache.asMap().isEmpty(), "Never analyzed: do not even cache absence");
            mgr.replayAddExternalMcvStatsMeta(meta);
            mgr.replayAddExternalMcvStatsMeta(meta); // Replayed updates must not leak a reference count.
            mgr.replayAddExternalMcvStatsMeta(second);
            mgr.replayRemoveExternalMcvStatsMeta(meta);
            Assertions.assertTrue(mgr.hasExternalMcvStatsMeta(table), "Another column group remains");
            uuid.set(UUID + ".recreated");
            Assertions.assertFalse(mgr.hasExternalMcvStatsMeta(table));
            uuid.set(UUID);
            queryFails.set(true);
            storage.getExternalMcvStatistics(table);
            Assertions.assertEquals(1, queries.get());
            for (int i = 0; i < 100; ++i) {
                storage.getExternalMcvStatistics(table);
                storage.prefetchExternalMcvStatistics(table);
            }
            Assertions.assertEquals(1, queries.get());
            Assertions.assertTrue(cache.asMap().isEmpty());
            ExternalMcvStatistics old = new ExternalMcvStatistics(List.of());
            cache.synchronous().put(UUID, Optional.of(old));
            Assertions.assertSame(old, storage.getExternalMcvStatistics(table));
            queryFails.set(false);
            clock.set(TimeUnit.SECONDS.toNanos(60));
            cache.synchronous().invalidate(UUID);
            storage.getExternalMcvStatistics(table);
            Assertions.assertEquals(2, queries.get());
            Assertions.assertEquals(Optional.empty(), cache.synchronous().policy().getIfPresentQuietly(UUID));
            mgr.replayRemoveExternalMcvStatsMeta(second);
            Assertions.assertFalse(mgr.hasExternalMcvStatsMeta(table));
            // Images predating persisted UUIDs still permit loading by the table name.
            meta.setTableUUID(null);
            mgr.replayAddExternalMcvStatsMeta(meta);
            Assertions.assertTrue(mgr.hasExternalMcvStatsMeta(table));
        } finally {
            mgr.replayRemoveExternalMcvStatsMeta(meta);
            mgr.replayRemoveExternalMcvStatsMeta(second);
        }
        Assertions.assertFalse(mgr.hasExternalMcvStatsMeta(table));
    }

    // Complete only when the storage API waits: deterministically distinguish sync and async paths
    // without sleeping or relying on background executor timing.
    private static class CompleteOnWait<T> extends CompletableFuture<T> {
        private final T value;

        CompleteOnWait(T value) {
            this.value = value;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            complete(value);
            return super.get();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMcvHonorsSyncLoad(boolean sync, @Mocked Table table,
                                    @Mocked AsyncLoadingCache<String, Optional<ExternalMcvStatistics>> cache) {
        new MockUp<AnalyzeMgr>() {
            @Mock
            public boolean hasExternalMcvStatsMeta(Table ignored) {
                return true;
            }
        };
        Config.enable_sync_statistics_load = sync;
        ExternalMcvStatistics stats = new ExternalMcvStatistics(List.of());
        CompletableFuture<Optional<ExternalMcvStatistics>> future = new CompleteOnWait<>(Optional.of(stats));
        new Expectations() {{
                table.getUUID();
                result = UUID;
                cache.get(UUID);
                result = future;
            }};
        CachedStatisticStorage storage = new CachedStatisticStorage();
        Deencapsulation.setField(storage, "externalMcvStats", cache);
        Assertions.assertSame(sync ? stats : ExternalMcvStatistics.EMPTY, storage.getExternalMcvStatistics(table));
        Assertions.assertEquals(sync, future.isDone());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBasicHonorsSyncLoad(boolean sync, @Mocked Table table) throws Exception {
        Config.enable_sync_statistics_load = sync;
        ExternalStatisticsCacheKey key = ExternalStatisticsCacheKey.table(UUID, "c");
        ConnectorTableColumnStats value = new ConnectorTableColumnStats(
                ColumnStatistic.builder().setDistinctValuesCount(5).build(), 100, "2026-09-22 00:00:00");
        ExternalColumnStatistics stats = new ExternalColumnStatistics.Summary(value, value, "BIGINT");
        CompletableFuture<Optional<ExternalColumnStatistics>> pending = new CompletableFuture<>();
        new Expectations() {{
                table.getUUID();
                result = UUID;
                table.getId();
                result = 100000001L;
            }};
        CachedStatisticStorage storage = new CachedStatisticStorage();
        storage.externalStatisticsCache.put(key, pending);
        if (sync) {
            CompletableFuture<List<ConnectorTableColumnStats>> result = CompletableFuture.supplyAsync(() ->
                    storage.getConnectorTableStatistics(table, List.of("c")));
            Assertions.assertFalse(result.isDone());
            pending.complete(Optional.of(stats));
            Assertions.assertEquals(100, result.get(5, TimeUnit.SECONDS).get(0).getRowCount());
        } else {
            Assertions.assertTrue(storage.getConnectorTableStatistics(table, List.of("c")).get(0).isUnknown());
            pending.complete(Optional.of(stats));
            Assertions.assertEquals(100, storage.getConnectorTableStatistics(table, List.of("c")).get(0).getRowCount());
        }
    }

    @Test
    public void testPrefetchSharesMissingLoadsAndRetriesFailures(@Mocked Table first, @Mocked Table second) {
        Config.enable_sync_statistics_load = true;
        String secondUUID = "iceberg.db.u.uuid";
        new Expectations() {{
                first.getUUID();
                result = UUID;
                first.getId();
                result = 100000001L;
                second.getUUID();
                result = secondUUID;
                second.getId();
                result = 100000002L;
            }};
        List<List<ExternalStatisticsCacheKey>> batches = new ArrayList<>();
        List<CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>>> pending = new ArrayList<>();
        new MockUp<ExternalStatisticsCacheLoader>() {
            @Mock
            public CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>> asyncLoadAll(
                    Iterable<? extends ExternalStatisticsCacheKey> keys, Executor executor) {
                List<ExternalStatisticsCacheKey> batch = new ArrayList<>();
                keys.forEach(batch::add);
                batches.add(batch);
                CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>> future =
                        new CompletableFuture<>();
                pending.add(future);
                return future;
            }
        };
        ExternalStatisticsCacheKey key = ExternalStatisticsCacheKey.table(UUID, "c");
        ExternalStatisticsCacheKey missing = ExternalStatisticsCacheKey.table(UUID, "missing");
        ExternalStatisticsCacheKey other = ExternalStatisticsCacheKey.table(secondUUID, "c");
        CachedStatisticStorage storage = new CachedStatisticStorage();
        var cache = storage.externalStatisticsCache;
        cache.put(key, CompletableFuture.completedFuture(Optional.empty()));
        storage.prefetchConnectorTableStatistics(first, List.of("c", "missing"));
        storage.prefetchConnectorTableStatistics(second, List.of("c"));
        storage.prefetchConnectorTableStatistics(first, List.of("c", "missing"));
        Assertions.assertEquals(List.of(List.of(missing), List.of(other)), batches);
        Assertions.assertFalse(pending.get(0).isDone());
        Assertions.assertFalse(pending.get(1).isDone());
        pending.get(0).complete(Map.of(missing, Optional.empty()));
        pending.get(1).completeExceptionally(new IllegalStateException("read failed"));
        Assertions.assertNull(cache.getIfPresent(other));
        storage.prefetchConnectorTableStatistics(first, List.of("c", "missing"));
        storage.prefetchConnectorTableStatistics(second, List.of("c"));
        Assertions.assertEquals(3, batches.size());
        Assertions.assertEquals(List.of(other), batches.get(2));
        pending.get(2).complete(Map.of(other, Optional.empty()));
    }
    @Test
    public void partitionBlockOverloadIsBoundedIsolatedAndRetryable() throws Exception {
        mockPartitionMetadata();
        int threads = Config.external_statistics_partition_load_threads;
        int queue = Config.external_statistics_partition_load_queue_size;
        Config.external_statistics_partition_block_size = 64;
        Config.external_statistics_partition_load_threads = 1;
        Config.external_statistics_partition_load_queue_size = 1;
        CachedStatisticStorage storage = new CachedStatisticStorage();
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<TStatisticData> queryExternalPartitionBlocks(ConnectContext context, String uuid,
                    List<ExternalStatisticsCacheKey> blocks, Map<String, com.starrocks.type.Type> types) throws Exception {
                entered.countDown();
                Assertions.assertTrue(release.await(5, TimeUnit.SECONDS));
                return List.of();
            }
        };
        ExecutorService nativePool = Deencapsulation.getField(storage, "statsCacheRefresherExecutor");
        ExecutorService partitionPool = Deencapsulation.getField(storage, "externalPartitionStatsExecutor");
        try {
            List<String> names = java.util.stream.IntStream.range(0, 128).mapToObj(i -> "p=" + i).toList();
            CompletableFuture<ExternalStatisticsAggregate> first = storage.loadExternalStatistics(
                    new ExternalStatisticsRequest(UUID, names, List.of("a")));
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            CompletableFuture<ExternalStatisticsAggregate> second = storage.loadExternalStatistics(
                    new ExternalStatisticsRequest(UUID, names, List.of("b")));
            ExternalStatisticsRequest thirdRequest = new ExternalStatisticsRequest(UUID, names, List.of("c"));
            CompletableFuture<ExternalStatisticsAggregate> rejected = storage.loadExternalStatistics(thirdRequest);
            ExecutionException failure = Assertions.assertThrows(ExecutionException.class,
                    () -> rejected.get(1, TimeUnit.SECONDS));
            Throwable cause = failure.getCause();
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            Assertions.assertInstanceOf(java.util.concurrent.RejectedExecutionException.class, cause);
            Assertions.assertEquals(42, nativePool.submit(() -> 42).get(1, TimeUnit.SECONDS),
                    "partition queue must not occupy the executor serving native statistics");
            release.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            Assertions.assertTrue(storage.loadExternalStatistics(thirdRequest).get(5, TimeUnit.SECONDS).isEmpty());
            Assertions.assertTrue(((Map<?, ?>) Deencapsulation.getField(storage, "pendingBlocks")).isEmpty());
        } finally {
            release.countDown();
            nativePool.shutdownNow();
            partitionPool.shutdownNow();
            Config.external_statistics_partition_load_threads = threads;
            Config.external_statistics_partition_load_queue_size = queue;
        }
    }

}
