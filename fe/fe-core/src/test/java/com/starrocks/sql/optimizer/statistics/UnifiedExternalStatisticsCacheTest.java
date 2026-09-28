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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

class UnifiedExternalStatisticsCacheTest {
    private static final String TABLE = "iceberg.db.t.uuid";
    private final Map<ExternalStatisticsCacheKey, ExternalColumnStatistics> source = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<List<ExternalStatisticsCacheKey>> reads = new ConcurrentLinkedQueue<>();
    private final AtomicReference<CompletableFuture<Void>> gate = new AtomicReference<>();
    private boolean oldRefresh;
    private int oldThreads;
    private int oldBlockSize;
    private long oldInterval;
    private CachedStatisticStorage storage;
    private ExecutorService worker;

    @BeforeEach
    void beforeEach() {
        oldBlockSize = Config.external_statistics_partition_block_size;
        Config.external_statistics_partition_block_size = 64;
        oldRefresh = Config.enable_statistic_cache_refresh_after_write;
        oldThreads = Config.statistic_cache_thread_pool_size;
        oldInterval = Config.statistic_update_interval_sec;
        Config.enable_statistic_cache_refresh_after_write = true;
        Config.statistic_cache_thread_pool_size = 1;
        Config.statistic_update_interval_sec = 3600;
        new MockUp<StatisticUtils>() {
            @Mock
            public boolean checkStatisticTableStateNormal() {
                return true;
            }
        };
        new MockUp<ExternalStatisticsCacheLoader>() {
            @Mock
            public CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>> asyncLoadAll(
                    Iterable<? extends ExternalStatisticsCacheKey> keys, Executor executor) {
                List<ExternalStatisticsCacheKey> batch = new ArrayList<>();
                keys.forEach(batch::add);
                reads.add(batch);
                CompletableFuture<Void> ready = gate.get();
                if (ready == null) {
                    ready = CompletableFuture.completedFuture(null);
                }
                return ready.thenApplyAsync(ignored -> {
                    Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> values = new ConcurrentHashMap<>();
                    batch.forEach(key -> {
                        if (key.scope == ExternalStatisticsCacheKey.Scope.BLOCK) {
                            Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> raw = new java.util.HashMap<>();
                            key.partitions.forEach(name -> {
                                ExternalStatisticsCacheKey part = new ExternalStatisticsCacheKey(TABLE, name, key.columnName);
                                raw.put(part, Optional.ofNullable(source.get(part)));
                            });
                            values.put(key, Optional.of(ExternalPartitionStatisticsBlocks.Block.merge(key, raw)));
                        } else {
                            values.put(key, Optional.ofNullable(source.get(key)));
                        }
                    });
                    return values;
                }, executor);
            }
        };
        storage = new CachedStatisticStorage();
        worker = Deencapsulation.getField(storage, "statsCacheRefresherExecutor");
    }

    @AfterEach
    void afterEach() throws Exception {
        ExecutorService partitionWorker = Deencapsulation.getField(storage, "externalPartitionStatsExecutor");
        partitionWorker.shutdownNow();
        partitionWorker.awaitTermination(5, TimeUnit.SECONDS);
        worker.shutdownNow();
        worker.awaitTermination(5, TimeUnit.SECONDS);
        Config.external_statistics_partition_block_size = oldBlockSize;
        Config.enable_statistic_cache_refresh_after_write = oldRefresh;
        Config.statistic_cache_thread_pool_size = oldThreads;
        Config.statistic_update_interval_sec = oldInterval;
    }

    private ExternalColumnStatistics.Partition value(String partition, String column, long rows, int ndv) {
        return new ExternalColumnStatistics.Partition(
                ExternalPartitionStatisticsTest.row(partition, column, rows, ndv, 0, "1", "100"), IntegerType.BIGINT);
    }

    private void put(String partition, String column, long rows, int ndv) {
        var value = value(partition, column, rows, ndv);
        source.put(new ExternalStatisticsCacheKey(TABLE, partition, column), value);
        var key = ExternalStatisticsCacheKey.partitionRow(TABLE, partition);
        Map<String, ExternalColumnStatistics.Partition> columns = new java.util.HashMap<>();
        if (source.get(key) instanceof ExternalPartitionStatistics previous) {
            columns.putAll(previous.columns);
        }
        columns.put(column, value);
        source.put(key, new ExternalPartitionStatistics(columns));
    }

    private ExternalColumnStatistics.Summary summary(long rows, int ndv) {
        ConnectorTableColumnStats value = new ConnectorTableColumnStats(
                ColumnStatistic.builder().setDistinctValuesCount(ndv).build(), rows, "2026-09-22 00:00:00");
        return new ExternalColumnStatistics.Summary(value, value, "BIGINT");
    }

    private void putTable(String column, long rows, int ndv) {
        ExternalStatisticsCacheKey key = ExternalStatisticsCacheKey.tableRow(TABLE);
        Map<String, ExternalColumnStatistics.Summary> columns = new java.util.HashMap<>();
        if (source.containsKey(key)) {
            columns.putAll(((ExternalTableStatistics) source.get(key)).summaries);
        }
        columns.put(column, summary(rows, ndv));
        source.put(key, new ExternalTableStatistics(columns));
    }

    private ExternalStatisticsRequest whole(List<String> partitions, String... columns) {
        return new ExternalStatisticsRequest(TABLE, partitions, List.of(columns), true);
    }

    private ExternalStatisticsAggregate load(ExternalStatisticsRequest request) throws Exception {
        return storage.loadExternalStatistics(request).get(5, TimeUnit.SECONDS);
    }

    private AsyncLoadingCache<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> cache() {
        return Deencapsulation.getField(storage, "externalStatisticsCache");
    }

    @Test
    void wholeTableLoadsOnlyScalarsAndSelectedRangesLoadOnlyTheirSketchesWithOneWorker() throws Exception {
        put("p=1", "a", 100, 50);
        put("p=2", "a", 200, 80);
        putTable("a", 300, 80);
        putTable("b", 300, 3);
        List<String> partitions = List.of("p=1", "p=2");
        ExternalStatisticsAggregate first = load(whole(partitions, "a"));
        Assertions.assertEquals(300, first.rowCount);
        Assertions.assertEquals(80, first.columns.get("a").getDistinctValuesCount());
        Assertions.assertEquals(1, reads.size());
        Assertions.assertEquals(1, cache().asMap().size(), "Whole-table reads must never load partition sketches");
        Assertions.assertTrue(reads.peek().stream().allMatch(ExternalStatisticsCacheKey::isTable));
        ExternalStatisticsAggregate repeated = load(whole(List.of("p=1", "p=2", "p=3"), "a"));
        Assertions.assertSame(first.columns.get("a"), repeated.columns.get("a"));
        Assertions.assertEquals(1, reads.size());
        ExternalStatisticsRequest selectedRequest = new ExternalStatisticsRequest(TABLE, List.of("p=1"), List.of("a"));
        ExternalStatisticsAggregate selected = load(selectedRequest);
        Assertions.assertEquals(50, selected.columns.get("a").getDistinctValuesCount());
        Assertions.assertEquals(2, reads.size());
        Assertions.assertEquals(2, cache().asMap().size(), "Both scopes share one cache");
        load(selectedRequest);
        Assertions.assertEquals(2, reads.size());
        ExternalStatisticsAggregate extended = load(whole(partitions, "a", "b"));
        Assertions.assertSame(first.columns.get("a"), extended.columns.get("a"));
        Assertions.assertEquals(2, reads.size(), "Another column must reuse the whole-table load");
        Assertions.assertEquals(3, extended.columns.get("b").getDistinctValuesCount());
        Assertions.assertNotEquals(ExternalStatisticsCacheKey.tableRow(TABLE),
                new ExternalStatisticsCacheKey(TABLE, "", "a"));
        var eviction = cache().synchronous().policy().eviction().orElseThrow();
        eviction.setMaximum(1000);
        cache().synchronous().cleanUp();
        Assertions.assertTrue(eviction.weightedSize().orElseThrow() <= 1000);
    }

    @Test
    void unpartitionedTableRetainsOnlyPreparedSummaries() throws Exception {
        ExternalStatisticsCacheKey key = ExternalStatisticsCacheKey.tableRow(TABLE);
        putTable("a", 100, 50);
        ExternalStatisticsRequest request = new ExternalStatisticsRequest(TABLE, List.of(""), List.of("a"), true, true);
        ExternalStatisticsAggregate first = load(request);
        Assertions.assertEquals(50, first.columns.get("a").getDistinctValuesCount());
        Assertions.assertEquals(List.of(key), reads.peek());
        Assertions.assertEquals(1, cache().asMap().size());
        Assertions.assertTrue(cache().asMap().get(key).join().orElseThrow() instanceof ExternalTableStatistics);
        Assertions.assertSame(first.columns.get("a"), load(request).columns.get("a"));
        Assertions.assertEquals(1, reads.size());
    }

    @Test
    void plannerAndAnalyzeUseTheSameTableEntriesAndRefreshInvalidatesBothScopes() throws Exception {
        com.starrocks.catalog.Table table = org.mockito.Mockito.mock(com.starrocks.catalog.Table.class);
        org.mockito.Mockito.when(table.getUUID()).thenReturn(TABLE);
        org.mockito.Mockito.when(table.getId()).thenReturn(123456L);
        putTable("a", 100, 10);
        put("p=1", "a", 100, 10);
        ExternalStatisticsRequest request = whole(List.of("p=1"), "a");
        ExternalStatisticsAggregate first = load(request);
        Assertions.assertSame(first.columns.get("a"),
                storage.getConnectorTableStatisticsSync(table, List.of("a")).get(0).getColumnStatistic());
        storage.prefetchConnectorTableStatistics(table, List.of("a"));
        Assertions.assertEquals(1, reads.size(), "Old public APIs must reuse the TABLE entry, without another cache/load");
        load(new ExternalStatisticsRequest(TABLE, List.of("p=1"), List.of("a")));
        Assertions.assertEquals(2, cache().asMap().size());
        putTable("a", 300, 30);
        storage.refreshConnectorTableColumnStatistics(table, List.of("a"), true);
        Assertions.assertEquals(300, load(request).rowCount);
        Assertions.assertEquals(3, reads.size());
        Assertions.assertEquals(1, cache().asMap().size());
        Assertions.assertNull(cache().getIfPresent(new ExternalStatisticsCacheKey(TABLE, "p=1", "a")));
    }

    private AtomicLong useClock() {
        AtomicLong nanos = new AtomicLong();
        AsyncLoadingCache<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> cache =
                Caffeine.newBuilder().expireAfterWrite(2, TimeUnit.HOURS).ticker(nanos::get)
                        .executor(worker).buildAsync(new ExternalStatisticsCacheLoader());
        Deencapsulation.setField(storage, "externalStatisticsCache", cache);
        return nanos;
    }

    private void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        Assertions.assertTrue(condition.getAsBoolean(), "Asynchronous cache operation did not finish");
    }

    private boolean noRefreshes() {
        Map<?, ?> refreshing = Deencapsulation.getField(storage, "refreshingExternalStatistics");
        return refreshing.isEmpty();
    }

    @Test
    void summaryRefreshSharesTheScalarReadAndPreservesLastSuccessOnFailure() throws Exception {
        AtomicLong clock = useClock();
        put("p=1", "a", 100, 10);
        putTable("a", 100, 10);
        ExternalStatisticsRequest request = whole(List.of("p=1"), "a");
        ExternalStatisticsAggregate first = load(request);
        // Caffeine publishes write timestamps in its completion callbacks, after the caller may return.
        worker.submit(() -> { }).get(5, TimeUnit.SECONDS);
        clock.set(TimeUnit.MINUTES.toNanos(90));
        putTable("a", 200, 20);
        CompletableFuture<Void> failed = new CompletableFuture<>();
        gate.set(failed);
        Assertions.assertSame(first.columns.get("a"), load(request).columns.get("a"));
        await(() -> reads.size() == 2);
        Assertions.assertSame(first.columns.get("a"), load(request).columns.get("a"));
        Assertions.assertEquals(2, reads.size(), "Readers must share the pending scalar refresh");
        failed.completeExceptionally(new IllegalStateException("injected BE failure"));
        await(this::noRefreshes);
        ExternalStatisticsCacheKey key = ExternalStatisticsCacheKey.tableRow(TABLE);
        Assertions.assertSame(first.columns.get("a"),
                ((ExternalTableStatistics) cache().asMap().get(key).join().orElseThrow()).columns.get("a"));
        CompletableFuture<Void> succeeded = new CompletableFuture<>();
        gate.set(succeeded);
        Assertions.assertEquals(100, load(request).rowCount);
        await(() -> reads.size() == 3);
        succeeded.complete(null);
        await(this::noRefreshes);
        Assertions.assertEquals(200, load(request).rowCount);
        Assertions.assertEquals(20, load(request).columns.get("a").getDistinctValuesCount());
        Assertions.assertEquals(3, reads.size());
    }

    @Test
    void invalidateBothScopesWithoutResurrectingAnOldSummaryRefresh() throws Exception {
        AtomicLong clock = useClock();
        put("p=1", "a", 100, 10);
        putTable("a", 100, 10);
        ExternalStatisticsRequest request = whole(List.of("p=1"), "a");
        load(request);
        // Caffeine publishes write timestamps in its completion callbacks, after the caller may return.
        worker.submit(() -> { }).get(5, TimeUnit.SECONDS);
        clock.set(TimeUnit.MINUTES.toNanos(90));
        CompletableFuture<Void> pending = new CompletableFuture<>();
        gate.set(pending);
        load(request);
        await(() -> reads.size() == 2);
        storage.invalidateConnectorTableColumnStatistics(TABLE, List.of("a"));
        Assertions.assertTrue(cache().asMap().isEmpty());
        ExternalStatisticsCacheKey key = ExternalStatisticsCacheKey.tableRow(TABLE);
        ExternalTableStatistics newer = new ExternalTableStatistics(Map.of("a", summary(900, 90)));
        cache().synchronous().put(key, Optional.of(newer));
        pending.complete(null);
        // Drain dependent completions on the one-worker executor before checking the conditional publication.
        worker.submit(() -> { }).get(5, TimeUnit.SECONDS);
        Assertions.assertSame(newer, cache().asMap().get(key).join().orElseThrow());
        Assertions.assertNull(cache().getIfPresent(new ExternalStatisticsCacheKey(TABLE, "p=1", "a")));
    }

    @Test
    void invalidationDuringColdLoadDoesNotPublishOldPartitionOrSummary() throws Exception {
        put("p=1", "a", 100, 10);
        putTable("a", 100, 10);
        CompletableFuture<Void> pending = new CompletableFuture<>();
        gate.set(pending);
        CompletableFuture<ExternalStatisticsAggregate> first = storage.loadExternalStatistics(whole(List.of("p=1"), "a"));
        await(() -> reads.size() == 1);
        storage.invalidateConnectorTableColumnStatistics(TABLE, List.of("a"));
        pending.complete(null);
        Assertions.assertEquals(100, first.get(5, TimeUnit.SECONDS).rowCount);
        Assertions.assertTrue(cache().asMap().isEmpty(), "An invalidated in-flight result is only for its original caller");
    }

    @Test
    void selectedPartitionAggregationSurvivesEvictionOfItsCells() throws Exception {
        List<String> partitions = java.util.stream.IntStream.range(0, 9000).mapToObj(i -> "p=" + i).toList();
        partitions.forEach(partition -> put(partition, "a", 100, 10));
        AsyncLoadingCache<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> cache =
                Caffeine.newBuilder().maximumSize(10).executor(worker).buildAsync(new ExternalStatisticsCacheLoader());
        Deencapsulation.setField(storage, "externalStatisticsCache", cache);
        ExternalStatisticsAggregate result = load(new ExternalStatisticsRequest(TABLE, partitions, List.of("a")));
        Assertions.assertEquals(900000, result.rowCount);
        Assertions.assertEquals(10, result.columns.get("a").getDistinctValuesCount());
        Assertions.assertTrue(result.hasCompleteCoverage());
        Assertions.assertEquals(3, reads.size());
        Assertions.assertTrue(reads.stream().allMatch(batch -> batch.size() <= 4096));
        cache.synchronous().cleanUp();
        Assertions.assertTrue(cache.asMap().size() <= 10);
    }
    private List<String> names(int start, int end) {
        return java.util.stream.IntStream.range(start, end).mapToObj(i -> String.format("p=%05d", i)).toList();
    }

    private void assertSameEstimate(ExternalStatisticsAggregate expected, ExternalStatisticsAggregate actual) {
        Assertions.assertEquals(expected.rowCount, actual.rowCount);
        Assertions.assertEquals(expected.knownPartitions, actual.knownPartitions);
        Assertions.assertEquals(expected.coveredPartitions, actual.coveredPartitions);
        expected.columns.forEach((name, column) -> {
            ColumnStatistic got = actual.columns.get(name);
            Assertions.assertEquals(column.isUnknown(), got.isUnknown());
            Assertions.assertEquals(column.getDistinctValuesCount(), got.getDistinctValuesCount());
            Assertions.assertEquals(column.getNullsFraction(), got.getNullsFraction());
            Assertions.assertEquals(column.getAverageRowSize(), got.getAverageRowSize());
            Assertions.assertEquals(column.getMinValue(), got.getMinValue());
            Assertions.assertEquals(column.getMaxValue(), got.getMaxValue());
        });
    }

    private ExternalStatisticsAggregate reference(ExternalStatisticsRequest request) {
        ExternalStatisticsAggregate.Builder builder = new ExternalStatisticsAggregate.Builder(request);
        request.columns.forEach(column -> request.partitions.forEach(partition -> {
            ExternalStatisticsCacheKey key = new ExternalStatisticsCacheKey(TABLE, partition, column);
            builder.add(Map.of(key, Optional.ofNullable(source.get(key))));
        }));
        return builder.build();
    }

    @Test
    void slidingAndSparseRangesReuseBlocksWithoutDoubleCountingOrInventingCoverage() throws Exception {
        for (int i = 0; i < 400; i++) {
            String name = names(i, i + 1).get(0);
            if (i % 3 != 0) {
                put(name, "a", i + 1, 10);
            }
            if (i % 5 != 0) {
                put(name, "b", i + 7, 20);
            }
        }
        for (List<String> parts : List.of(names(0, 256), names(1, 257), names(64, 192),
                names(100, 120), names(0, 256).stream().filter(n -> n.hashCode() % 2 == 0).toList())) {
            ExternalStatisticsRequest request = new ExternalStatisticsRequest(TABLE, parts, List.of("a", "b", "absent"));
            assertSameEstimate(reference(request), load(request));
        }
        reads.clear();
        ExternalStatisticsRequest covered = new ExternalStatisticsRequest(TABLE, names(64, 192), List.of("a", "b", "absent"));
        assertSameEstimate(reference(covered), load(covered));
        Assertions.assertTrue(reads.isEmpty(), "complete blocks, including negative coverage, must be reused");
    }

    @Test
    void warmSinglesAreCompactedWithoutSqlAndNarrowRequestsStillWork() throws Exception {
        List<String> names = names(0, 128);
        names.forEach(name -> put(name, "a", 100, 10));
        for (int i = 0; i < names.size(); i += 8) {
            load(new ExternalStatisticsRequest(TABLE, names.subList(i, i + 8), List.of("a")));
        }
        reads.clear();
        ExternalStatisticsRequest broad = new ExternalStatisticsRequest(TABLE, names, List.of("a"));
        assertSameEstimate(reference(broad), load(broad));
        Assertions.assertTrue(reads.isEmpty());
        // Remove the original singles: subsequent broad queries must now be served by the compact blocks.
        cache().asMap().keySet().removeIf(key -> key.scope == ExternalStatisticsCacheKey.Scope.PARTITION
                || key.scope == ExternalStatisticsCacheKey.Scope.PARTITION_ROW);
        assertSameEstimate(reference(broad), load(broad));
        Assertions.assertTrue(reads.isEmpty());
        ExternalStatisticsRequest narrow = new ExternalStatisticsRequest(TABLE, names.subList(3, 6), List.of("a"));
        assertSameEstimate(reference(narrow), load(narrow));
        Assertions.assertEquals(3, reads.stream().mapToInt(List::size).sum());
    }

    @Test
    void parallelBlockLoadsAreSharedAndInvalidatedLoadsCannotPublishIntoNewGeneration() throws Exception {
        List<String> names = names(0, 128);
        names.forEach(name -> put(name, "a", 100, 10));
        ExternalStatisticsRequest request = new ExternalStatisticsRequest(TABLE, names, List.of("a"));
        CompletableFuture<Void> pending = new CompletableFuture<>();
        gate.set(pending);
        CompletableFuture<ExternalStatisticsAggregate> first = storage.loadExternalStatistics(request);
        CompletableFuture<ExternalStatisticsAggregate> same = storage.loadExternalStatistics(request);
        Assertions.assertEquals(1, reads.size(), "identical concurrent ranges must share their load");
        storage.invalidateConnectorTableColumnStatistics(TABLE, List.of("a"));
        CompletableFuture<ExternalStatisticsAggregate> fresh = storage.loadExternalStatistics(request);
        Assertions.assertEquals(2, reads.size(), "new ANALYZE generation must not reuse a pre-invalidation load");
        pending.complete(null);
        assertSameEstimate(reference(request), first.get(5, TimeUnit.SECONDS));
        assertSameEstimate(reference(request), same.get(5, TimeUnit.SECONDS));
        assertSameEstimate(reference(request), fresh.get(5, TimeUnit.SECONDS));
        Assertions.assertTrue(((Map<?, ?>) Deencapsulation.getField(storage, "pendingBlocks")).isEmpty());
        reads.clear();
        assertSameEstimate(reference(request), load(request));
        Assertions.assertTrue(reads.isEmpty());
    }

    @Test
    void sparseFirstDoesNotPermanentlyPreventDenseBlocks() throws Exception {
        names(0, 512).forEach(name -> put(name, "a", 100, 10));
        List<String> sparse = java.util.stream.IntStream.range(0, 512).filter(i -> i % 4 == 0)
                .mapToObj(i -> names(i, i + 1).get(0)).toList();
        load(new ExternalStatisticsRequest(TABLE, sparse, List.of("a")));
        ExternalStatisticsRequest dense = new ExternalStatisticsRequest(TABLE, names(0, 512), List.of("a"));
        assertSameEstimate(reference(dense), load(dense));
        reads.clear();
        assertSameEstimate(reference(dense), load(dense));
        Assertions.assertTrue(reads.isEmpty(), "sparse envelopes must not block dense cache reuse");
    }

    @Test
    void authoritativeMissingTableColumnAvoidsPartitionLoads() throws Exception {
        cache().put(ExternalStatisticsCacheKey.tableRow(TABLE),
                CompletableFuture.completedFuture(Optional.empty()));
        ExternalStatisticsAggregate result = load(new ExternalStatisticsRequest(TABLE, names(0, 2000), List.of("absent")));
        Assertions.assertTrue(result.isEmpty());
        Assertions.assertTrue(reads.isEmpty());
    }

    @Test
    void fillingGapsMustNotCreateBlocksSpanningAlreadyCoveredRuns() throws Exception {
        names(0, 400).forEach(name -> put(name, "a", 100, 10));
        load(new ExternalStatisticsRequest(TABLE, names(0, 200), List.of("a")));
        load(new ExternalStatisticsRequest(TABLE, names(13, 213), List.of("a")));
        ExternalStatisticsRequest all = new ExternalStatisticsRequest(TABLE, names(0, 400), List.of("a"));
        assertSameEstimate(reference(all), load(all));
        reads.clear();
        assertSameEstimate(reference(all), load(all));
        Assertions.assertTrue(reads.isEmpty(), "a filled range must be reusable after sliding requests");
        var eviction = cache().synchronous().policy().eviction().orElseThrow();
        eviction.setMaximum(2048);
        cache().synchronous().cleanUp();
        Assertions.assertTrue(eviction.weightedSize().orElseThrow() <= 2048, "blocks share the byte budget");
        assertSameEstimate(reference(all), load(all));
        cache().synchronous().cleanUp();
        Assertions.assertTrue(eviction.weightedSize().orElseThrow() <= 2048);
    }

    @Test
    void staleBlocksRefreshAsynchronouslyWithoutLosingGenerationOrCoverage() throws Exception {
        List<String> parts = names(0, 128);
        parts.forEach(name -> put(name, "a", 100, 10));
        ExternalStatisticsRequest request = new ExternalStatisticsRequest(TABLE, parts, List.of("a"));
        Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> raw = new java.util.HashMap<>();
        source.forEach((key, value) -> raw.put(key, Optional.of(value)));
        ExternalStatisticsCacheKey key = ExternalStatisticsCacheKey.block(TABLE, "a", parts);
        long now = System.nanoTime();
        var stale = ExternalPartitionStatisticsBlocks.Block.merge(key, raw,
                now - TimeUnit.SECONDS.toNanos(Config.statistic_update_interval_sec + 1));
        var directory = new ExternalPartitionStatisticsBlocks.Directory().withBlocks(List.of(stale), now);
        cache().put(ExternalStatisticsCacheKey.directory(TABLE, "a"),
                CompletableFuture.completedFuture(Optional.of(directory)));
        parts.forEach(name -> put(name, "a", 200, 20));
        CompletableFuture<Void> pending = new CompletableFuture<>();
        gate.set(pending);
        Assertions.assertEquals(12800, load(request).rowCount, "refresh must not block a valid cached read");
        Assertions.assertEquals(1, reads.size());
        pending.complete(null);
        await(() -> ((Map<?, ?>) Deencapsulation.getField(storage, "pendingBlocks")).isEmpty());
        assertSameEstimate(reference(request), load(request));
    }

    @Test
    void packedRowsShareLoadsAcrossColumnsAndRefreshAsOneGeneration() throws Exception {
        put("p=1", "a", 100, 10);
        put("p=1", "b", 100, 20);
        CompletableFuture<Void> pending = new CompletableFuture<>();
        gate.set(pending);
        var a = storage.loadExternalStatistics(new ExternalStatisticsRequest(TABLE, List.of("p=1"), List.of("a")));
        var b = storage.loadExternalStatistics(new ExternalStatisticsRequest(TABLE, List.of("p=1"), List.of("b")));
        Assertions.assertEquals(1, reads.size());
        pending.complete(null);
        Assertions.assertEquals(10, a.get(5, TimeUnit.SECONDS).columns.get("a").getDistinctValuesCount());
        Assertions.assertEquals(20, b.get(5, TimeUnit.SECONDS).columns.get("b").getDistinctValuesCount());
        Assertions.assertEquals(1, cache().asMap().size(), "No duplicate per-cell cache entries for packed data");
        put("p=1", "a", 200, 30);
        storage.invalidateConnectorTableColumnStatistics(TABLE, List.of("a"));
        var updated = load(new ExternalStatisticsRequest(TABLE, List.of("p=1"), List.of("a", "b")));
        Assertions.assertEquals(200, updated.rowCount);
        Assertions.assertEquals(30, updated.columns.get("a").getDistinctValuesCount());
        Assertions.assertEquals(20, updated.columns.get("b").getDistinctValuesCount());
        Assertions.assertEquals(2, reads.size());
    }

    @Test
    void missingPackedCopyUsesCellsAndReadFailuresAreNotCachedAsAbsence() throws Exception {
        put("p=1", "a", 100, 10);
        source.remove(ExternalStatisticsCacheKey.partitionRow(TABLE, "p=1"));
        var request = new ExternalStatisticsRequest(TABLE, List.of("p=1"), List.of("a"));
        gate.set(CompletableFuture.failedFuture(new IllegalStateException("unavailable")));
        Assertions.assertThrows(java.util.concurrent.ExecutionException.class, () -> load(request));
        await(() -> cache().getIfPresent(ExternalStatisticsCacheKey.partitionRow(TABLE, "p=1")) == null);
        gate.set(null);
        Assertions.assertEquals(100, load(request).rowCount);
        int count = reads.size();
        Assertions.assertEquals(100, load(request).rowCount);
        Assertions.assertEquals(count, reads.size());
    }

}
