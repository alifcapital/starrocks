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
import com.starrocks.sql.ast.StatisticsType;
import com.starrocks.sql.optimizer.statistics.StatisticsCachePreloader.InternalTable;
import com.starrocks.sql.optimizer.statistics.StatisticsCachePreloader.Result;
import com.starrocks.sql.optimizer.statistics.StatisticsCachePreloader.Source;
import com.starrocks.sql.optimizer.statistics.StatisticsCachePreloader.Timing;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.ExternalMcvStatsMeta;
import com.starrocks.statistic.StatsConstants;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatisticsCachePreloaderTest {
    private static final String NO_ALIVE_NODE =
            "Statistics query fail | Error Message [No alive backend or compute node in warehouse default_warehouse.]";

    private static final class FakeSource implements Source {
        volatile boolean ready = true;
        AtomicInteger partitionListingFailures = new AtomicInteger();
        List<String> externalUuids = List.of();
        Map<String, List<String>> partitions = Map.of();
        List<String> mcvUuids = List.of();
        List<InternalTable> internalTables = List.of();
        List<BooleanSupplier> joinLoads = List.of();
        AtomicLong joinEvictions = new AtomicLong();

        @Override
        public boolean ready() {
            return ready;
        }

        @Override
        public List<String> externalTableUuids() {
            return externalUuids;
        }

        @Override
        public Map<String, List<String>> externalPartitions(Collection<String> uuids) {
            if (partitionListingFailures.getAndDecrement() > 0) {
                throw new IllegalStateException(NO_ALIVE_NODE);
            }
            return partitions;
        }

        @Override
        public List<String> mcvTableUuids() {
            return mcvUuids;
        }

        @Override
        public List<InternalTable> internalTables() {
            return internalTables;
        }

        @Override
        public List<BooleanSupplier> joinLoads() {
            return joinLoads;
        }

        @Override
        public long joinEvictions() {
            return joinEvictions.get();
        }
    }

    // Every key costs one weight unit, so maxWeight is the number of entries the cache keeps.
    private static AsyncLoadingCache<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> externalCache(
            long maxWeight, List<List<ExternalStatisticsCacheKey>> loadedBatches) {
        return externalCache(maxWeight, loadedBatches, keys -> false);
    }

    // A batch for which the predicate returns true is not loaded and fails like a statistics query with no alive node.
    private static AsyncLoadingCache<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> externalCache(
            long maxWeight, List<List<ExternalStatisticsCacheKey>> loadedBatches,
            Predicate<List<ExternalStatisticsCacheKey>> fails) {
        AsyncCacheLoader<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> loader =
                new AsyncCacheLoader<>() {
                    @Override
                    public @NonNull CompletableFuture<Optional<ExternalColumnStatistics>> asyncLoad(
                            @NonNull ExternalStatisticsCacheKey key, @NonNull Executor executor) {
                        loadedBatches.add(List.of(key));
                        return CompletableFuture.completedFuture(Optional.empty());
                    }

                    @Override
                    public @NonNull CompletableFuture<Map<ExternalStatisticsCacheKey,
                            Optional<ExternalColumnStatistics>>> asyncLoadAll(
                            @NonNull Iterable<? extends ExternalStatisticsCacheKey> keys,
                            @NonNull Executor executor) {
                        List<ExternalStatisticsCacheKey> batch = new ArrayList<>();
                        Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> result = new HashMap<>();
                        keys.forEach(key -> {
                            batch.add(key);
                            result.put(key, Optional.empty());
                        });
                        if (fails.test(batch)) {
                            return CompletableFuture.failedFuture(new IllegalStateException(NO_ALIVE_NODE));
                        }
                        loadedBatches.add(batch);
                        return CompletableFuture.completedFuture(result);
                    }
                };
        return Caffeine.newBuilder().executor(Runnable::run).recordStats()
                .maximumWeight(maxWeight).weigher((key, value) -> 1).buildAsync(loader);
    }

    // The first loads of the cache fail like a statistics query with no alive node, as many as failures says, and
    // the later ones succeed with an empty value. loadCounts counts the successful loads of every key.
    private static <K, V> AsyncLoadingCache<K, Optional<V>> flakyCache(AtomicInteger failures,
                                                                       Map<K, Integer> loadCounts) {
        AsyncCacheLoader<K, Optional<V>> loader = new AsyncCacheLoader<>() {
            @Override
            public @NonNull CompletableFuture<Optional<V>> asyncLoad(@NonNull K key, @NonNull Executor executor) {
                return asyncLoadAll(List.of(key), executor).thenApply(loaded -> loaded.get(key));
            }

            @Override
            public @NonNull CompletableFuture<Map<K, Optional<V>>> asyncLoadAll(
                    @NonNull Iterable<? extends K> keys, @NonNull Executor executor) {
                if (failures.getAndDecrement() > 0) {
                    return CompletableFuture.failedFuture(new IllegalStateException(NO_ALIVE_NODE));
                }
                Map<K, Optional<V>> result = new HashMap<>();
                keys.forEach(key -> {
                    loadCounts.merge(key, 1, Integer::sum);
                    result.put(key, Optional.empty());
                });
                return CompletableFuture.completedFuture(result);
            }
        };
        return Caffeine.newBuilder().executor(Runnable::run).buildAsync(loader);
    }

    private static List<String> uuids(int count) {
        List<String> uuids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            uuids.add("ice.db.t" + i);
        }
        return uuids;
    }

    private static void waitFor(BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 1000 && !condition.getAsBoolean(); i++) {
            Thread.sleep(5);
        }
        assertTrue(condition.getAsBoolean());
    }

    private static StatisticsCachePreloader preloader(CachedStatisticStorage storage, Source source) {
        return new StatisticsCachePreloader(storage, source, new Timing(1000, 5, 5, 20, 2000));
    }

    @Test
    @Timeout(10)
    void externalLoadStopsAtFirstEviction() {
        CachedStatisticStorage storage = new CachedStatisticStorage();
        List<List<ExternalStatisticsCacheKey>> batches = new CopyOnWriteArrayList<>();
        storage.externalStatisticsCache = externalCache(6, batches);
        FakeSource source = new FakeSource();
        source.externalUuids = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            source.externalUuids.add("ice.db.t" + i);
        }

        preloader(storage, source).run();

        int loadedKeys = batches.stream().mapToInt(List::size).sum();
        assertTrue(loadedKeys >= 8, "The first batches fit or fill the cache: " + loadedKeys);
        assertTrue(loadedKeys < 40, "Loading must stop once the cache evicts: " + loadedKeys);
        assertTrue(storage.externalStatisticsCache.synchronous().stats().evictionCount() > 0);
    }

    @Test
    @Timeout(10)
    void partitionRowsLoadPerTableInBatchesOfEightAndKeepCachedEntries() {
        CachedStatisticStorage storage = new CachedStatisticStorage();
        List<List<ExternalStatisticsCacheKey>> batches = new CopyOnWriteArrayList<>();
        storage.externalStatisticsCache = externalCache(1000, batches);
        ExternalStatisticsCacheKey present = ExternalStatisticsCacheKey.partitionRow("ice.db.a", "p0");
        CompletableFuture<Optional<ExternalColumnStatistics>> keptFuture =
                CompletableFuture.completedFuture(Optional.empty());
        storage.externalStatisticsCache.put(present, keptFuture);

        FakeSource source = new FakeSource();
        List<String> a = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            a.add("p" + i);
        }
        source.partitions = Map.of("ice.db.a", a, "ice.db.b", List.of("q0"));

        preloader(storage, source).run();

        for (List<ExternalStatisticsCacheKey> batch : batches) {
            assertTrue(batch.size() <= StatisticsCachePreloader.EXTERNAL_PARTITION_BATCH_SIZE);
            assertEquals(1, batch.stream().map(key -> key.tableUUID).distinct().count(),
                    "A batch holds the rows of one table");
            batch.forEach(key -> assertEquals(ExternalStatisticsCacheKey.Scope.PARTITION_ROW, key.scope));
        }
        assertEquals(20, batches.stream().filter(batch -> batch.get(0).tableUUID.equals("ice.db.a"))
                .mapToInt(List::size).sum() + 1, "p0 was already cached and is not loaded again");
        assertEquals(1, batches.stream().filter(batch -> batch.get(0).tableUUID.equals("ice.db.b")).count());
        assertSame(keptFuture, storage.externalStatisticsCache.asMap().get(present));
    }

    @Test
    void errorInOneBatchDoesNotStopTheRest() {
        List<List<Integer>> batches = StatisticsCachePreloader.chunk(List.of(1, 2, 3, 4, 5, 6), 2);
        List<Integer> seen = new ArrayList<>();
        Result<Integer> result = StatisticsCachePreloader.loadBatches(batches, batch -> {
            if (batch.contains(3)) {
                throw new IllegalStateException("table is gone");
            }
            seen.addAll(batch);
            return batch.size();
        }, size -> null, false, () -> true);

        assertEquals(List.of(1, 2, 5, 6), seen);
        assertEquals(4, result.loaded());
        assertEquals(1, result.failedBatches());
        assertNull(result.stopReason());
        assertEquals(List.of(List.of(3, 4)), result.unloaded());
    }

    @Test
    void failedBatchIsLoadedKeyByKeyAndOnlyTheBadKeyStaysUnloaded() {
        List<List<String>> batches = StatisticsCachePreloader.chunk(List.of("a", "gone", "c", "d"), 4);
        Result<String> result = StatisticsCachePreloader.loadBatches(batches, batch -> {
            if (batch.contains("gone")) {
                throw new IllegalStateException("table is gone");
            }
            return batch.size();
        }, size -> null, true, () -> true);

        assertEquals(3, result.loaded());
        assertEquals(2, result.failedBatches());
        assertEquals(List.of(List.of("gone")), result.unloaded());
    }

    @Test
    void stopCheckEndsLoadingBeforeTheNextBatch() {
        AtomicInteger loads = new AtomicInteger();
        Result<Integer> result = StatisticsCachePreloader.loadBatches(
                StatisticsCachePreloader.chunk(List.of(1, 2, 3, 4), 1),
                batch -> {
                    loads.incrementAndGet();
                    return batch.size();
                },
                size -> loads.get() >= 2 ? StatisticsCachePreloader.STOP_EVICTION : null, false, () -> true);

        assertEquals(2, loads.get());
        assertEquals(2, result.loaded());
        assertEquals(StatisticsCachePreloader.STOP_EVICTION, result.stopReason());
    }

    @Test
    void stopThatEndsByItselfLeavesTheUntriedBatchesUnloaded() {
        AtomicInteger loads = new AtomicInteger();
        Result<Integer> result = StatisticsCachePreloader.loadBatches(
                StatisticsCachePreloader.chunk(List.of(1, 2, 3, 4), 1),
                batch -> {
                    loads.incrementAndGet();
                    return batch.size();
                },
                size -> loads.get() >= 2 ? StatisticsCachePreloader.STOP_BACKOFF : null, false, () -> true);

        assertEquals(StatisticsCachePreloader.STOP_BACKOFF, result.stopReason());
        assertEquals(List.of(List.of(3), List.of(4)), result.unloaded());
    }

    @Test
    void evictionStopKeepsOnlyTheFailedBatchesForTheCaller() {
        AtomicInteger loads = new AtomicInteger();
        Result<Integer> result = StatisticsCachePreloader.loadBatches(
                StatisticsCachePreloader.chunk(List.of(1, 2, 3, 4), 1),
                batch -> {
                    if (loads.incrementAndGet() == 1) {
                        throw new IllegalStateException(NO_ALIVE_NODE);
                    }
                    return batch.size();
                },
                size -> loads.get() >= 2 ? StatisticsCachePreloader.STOP_EVICTION : null, false, () -> true);

        assertEquals(StatisticsCachePreloader.STOP_EVICTION, result.stopReason());
        assertEquals(List.of(List.of(1)), result.unloaded());
    }

    @Test
    void failureWhileNotReadyEndsThePassAndLeavesEverythingUnloaded() {
        AtomicInteger attempts = new AtomicInteger();
        Result<Integer> result = StatisticsCachePreloader.loadBatches(
                StatisticsCachePreloader.chunk(List.of(1, 2, 3, 4, 5, 6), 2),
                batch -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException(NO_ALIVE_NODE);
                }, size -> null, true, () -> false);

        assertEquals(1, attempts.get(), "No other batch is tried and the failed one is not split");
        assertEquals(1, result.failedBatches());
        assertEquals(StatisticsCachePreloader.STOP_NOT_READY, result.stopReason());
        assertEquals(List.of(List.of(1, 2), List.of(3, 4), List.of(5, 6)), result.unloaded());
    }

    @Test
    void sizeBoundedCacheStopsBeforeTheNextBatchOverflowsIt() {
        AsyncLoadingCache<Long, Optional<Long>> cache = Caffeine.newBuilder().executor(Runnable::run)
                .maximumSize(10).buildAsync((key, executor) -> CompletableFuture.completedFuture(Optional.of(key)));
        for (long i = 0; i < 8; i++) {
            cache.synchronous().put(i, Optional.of(i));
        }
        StatisticsCachePreloader.StopCheck stop = StatisticsCachePreloader.sizeCacheStop(cache);

        assertNull(stop.reason(2));
        assertEquals(StatisticsCachePreloader.STOP_EVICTION, stop.reason(3));
    }

    @Test
    void internalBatchesHoldTheKeysOfOneTable() {
        List<String> wide = new ArrayList<>();
        for (int i = 0; i < StatisticsCachePreloader.COLUMN_BATCH_SIZE + 44; i++) {
            wide.add("c" + i);
        }
        List<Long> partitions = new ArrayList<>();
        for (long i = 0; i < StatisticsCachePreloader.ROW_COUNT_BATCH_SIZE + 5; i++) {
            partitions.add(i);
        }
        List<InternalTable> tables = List.of(new InternalTable(1, wide, partitions),
                new InternalTable(2, List.of("x", "y"), List.of(7L)));

        List<List<ColumnStatsCacheKey>> columnBatches = StatisticsCachePreloader.columnBatches(tables);
        assertEquals(List.of(StatisticsCachePreloader.COLUMN_BATCH_SIZE, 44, 2),
                columnBatches.stream().map(List::size).toList());
        columnBatches.forEach(batch ->
                assertEquals(1, batch.stream().map(key -> key.tableId).distinct().count()));

        List<List<TableStatsCacheKey>> rowBatches = StatisticsCachePreloader.rowCountBatches(tables);
        assertEquals(List.of(StatisticsCachePreloader.ROW_COUNT_BATCH_SIZE, 5, 1),
                rowBatches.stream().map(List::size).toList());
        rowBatches.forEach(batch ->
                assertEquals(1, batch.stream().map(TableStatsCacheKey::getTableId).distinct().count()));
    }

    @Test
    void metasWithoutUuidAreSkipped() {
        ExternalBasicStatsMeta old = new ExternalBasicStatsMeta();
        ExternalBasicStatsMeta empty = new ExternalBasicStatsMeta();
        empty.setTableUUID("");
        ExternalBasicStatsMeta older = new ExternalBasicStatsMeta("c", "d", "t1", List.of("a"),
                StatsConstants.AnalyzeType.FULL, LocalDateTime.of(2026, 1, 1, 0, 0), Map.of());
        older.setTableUUID("ice.d.t1");
        ExternalBasicStatsMeta newer = new ExternalBasicStatsMeta("c", "d", "t2", List.of("a"),
                StatsConstants.AnalyzeType.FULL, LocalDateTime.of(2026, 2, 1, 0, 0), Map.of());
        newer.setTableUUID("ice.d.t2");

        assertEquals(List.of("ice.d.t2", "ice.d.t1"),
                StatisticsCachePreloader.tableUuidsByRecency(List.of(old, empty, older, newer)));
    }

    @Test
    void mcvMetasWithoutUuidAreSkipped() {
        AnalyzeMgr analyzeMgr = new AnalyzeMgr();
        ExternalMcvStatsMeta withUuid = new ExternalMcvStatsMeta("c", "d", "t1", List.of("a"),
                StatsConstants.AnalyzeType.FULL, List.of(StatisticsType.MCV), LocalDateTime.now(), Map.of());
        withUuid.setTableUUID("ice.d.t1");
        ExternalMcvStatsMeta withoutUuid = new ExternalMcvStatsMeta("c", "d", "t2", List.of("a"),
                StatsConstants.AnalyzeType.FULL, List.of(StatisticsType.MCV), LocalDateTime.now(), Map.of());
        analyzeMgr.replayAddExternalMcvStatsMeta(withUuid);
        analyzeMgr.replayAddExternalMcvStatsMeta(withoutUuid);

        assertEquals(List.of("ice.d.t1"), analyzeMgr.getExternalMcvTableUuids());
    }

    @Test
    @Timeout(10)
    void joinLoadStopsWhenTheJoinCacheEvicts() {
        CachedStatisticStorage storage = new CachedStatisticStorage();
        FakeSource source = new FakeSource();
        AtomicInteger loads = new AtomicInteger();
        BooleanSupplier load = () -> {
            loads.incrementAndGet();
            source.joinEvictions.incrementAndGet();
            return true;
        };
        source.joinLoads = List.of(load, load, load);

        preloader(storage, source).run();

        assertEquals(1, loads.get());
    }

    @Test
    @Timeout(10)
    void nothingIsLoadedWhenStatisticsNeverGetReady() {
        CachedStatisticStorage storage = new CachedStatisticStorage();
        List<List<ExternalStatisticsCacheKey>> batches = new CopyOnWriteArrayList<>();
        storage.externalStatisticsCache = externalCache(1000, batches);
        FakeSource source = new FakeSource();
        source.ready = false;
        source.externalUuids = List.of("ice.db.a");

        StatisticsCachePreloader preloader =
                new StatisticsCachePreloader(storage, source, new Timing(30, 5, 5, 20, 2000));
        assertFalse(preloader.awaitReady(30));
        preloader.run();

        assertTrue(batches.isEmpty());
    }

    private static Map<ExternalStatisticsCacheKey, Long> loadsPerKey(List<List<ExternalStatisticsCacheKey>> batches) {
        return batches.stream().flatMap(List::stream)
                .collect(Collectors.groupingBy(key -> key, Collectors.counting()));
    }

    @Test
    @Timeout(10)
    void failedBatchesAreRetriedAndLoadedKeysAreNotLoadedAgain() {
        CachedStatisticStorage storage = new CachedStatisticStorage();
        List<List<ExternalStatisticsCacheKey>> batches = new CopyOnWriteArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        // The first five loads find no alive compute node.
        storage.externalStatisticsCache = externalCache(1000, batches, keys -> calls.incrementAndGet() <= 5);
        FakeSource source = new FakeSource();
        source.externalUuids = uuids(10);

        preloader(storage, source).run();

        Map<ExternalStatisticsCacheKey, Long> loads = loadsPerKey(batches);
        assertEquals(10, loads.size(), "Every key is loaded after the retries");
        loads.values().forEach(count -> assertEquals(1L, count, "A key that loaded is not loaded again"));
        assertTrue(calls.get() > 5);
    }

    @Test
    @Timeout(10)
    void everyCacheRetriesItsFailedBatches() {
        CachedStatisticStorage storage = new CachedStatisticStorage();
        List<List<ExternalStatisticsCacheKey>> externalBatches = new CopyOnWriteArrayList<>();
        AtomicInteger externalCalls = new AtomicInteger();
        storage.externalStatisticsCache =
                externalCache(1000, externalBatches, keys -> externalCalls.incrementAndGet() <= 3);
        Map<String, Integer> mcvLoads = new ConcurrentHashMap<>();
        storage.externalMcvStats = flakyCache(new AtomicInteger(2), mcvLoads);
        Map<ColumnStatsCacheKey, Integer> columnLoads = new ConcurrentHashMap<>();
        storage.columnStatistics = flakyCache(new AtomicInteger(2), columnLoads);
        Map<TableStatsCacheKey, Integer> rowCountLoads = new ConcurrentHashMap<>();
        storage.tableStatsCache = flakyCache(new AtomicInteger(2), rowCountLoads);

        FakeSource source = new FakeSource();
        source.externalUuids = List.of("ice.db.a", "ice.db.b");
        source.partitions = Map.of("ice.db.a", List.of("p0", "p1"));
        // Listing the partitions is a statistics query and fails in the first two rounds.
        source.partitionListingFailures.set(2);
        source.mcvUuids = List.of("ice.db.a", "ice.db.b");
        source.internalTables = List.of(new InternalTable(1, List.of("c0", "c1"), List.of(10L, 11L)));
        AtomicInteger joinCalls = new AtomicInteger();
        // The join load returns false when it left nothing in the cache.
        source.joinLoads = List.of(() -> joinCalls.incrementAndGet() > 2);

        preloader(storage, source).run();

        Map<ExternalStatisticsCacheKey, Long> external = loadsPerKey(externalBatches);
        assertEquals(Set.of(ExternalStatisticsCacheKey.tableRow("ice.db.a"),
                ExternalStatisticsCacheKey.tableRow("ice.db.b"),
                ExternalStatisticsCacheKey.partitionRow("ice.db.a", "p0"),
                ExternalStatisticsCacheKey.partitionRow("ice.db.a", "p1")), external.keySet());
        external.values().forEach(count -> assertEquals(1L, count));
        assertEquals(Map.of("ice.db.a", 1, "ice.db.b", 1), mcvLoads);
        assertEquals(Map.of(new ColumnStatsCacheKey(1, "c0"), 1, new ColumnStatsCacheKey(1, "c1"), 1), columnLoads);
        assertEquals(Map.of(new TableStatsCacheKey(1, 10), 1, new TableStatsCacheKey(1, 11), 1), rowCountLoads);
        assertEquals(3, joinCalls.get(), "The join load runs until it succeeds and not after that");
    }

    @Test
    @Timeout(10)
    void retryRoundsWaitUntilTheSourceIsReadyAgain() throws Exception {
        CachedStatisticStorage storage = new CachedStatisticStorage();
        List<List<ExternalStatisticsCacheKey>> batches = new CopyOnWriteArrayList<>();
        FakeSource source = new FakeSource();
        source.externalUuids = uuids(6);
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger callsWhileDown = new AtomicInteger();
        List<Thread> recoveries = new CopyOnWriteArrayList<>();
        storage.externalStatisticsCache = externalCache(1000, batches, keys -> {
            if (!source.ready) {
                callsWhileDown.incrementAndGet();
            }
            if (calls.incrementAndGet() > 1) {
                return false;
            }
            // The compute node goes down while the first batch loads and comes back after a while.
            source.ready = false;
            Thread recovery = new Thread(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    return;
                }
                source.ready = true;
            });
            recoveries.add(recovery);
            recovery.start();
            return true;
        });

        preloader(storage, source).run();
        for (Thread recovery : recoveries) {
            recovery.join();
        }

        assertEquals(0, callsWhileDown.get(), "No load starts while the source is not ready");
        Map<ExternalStatisticsCacheKey, Long> loads = loadsPerKey(batches);
        assertEquals(6, loads.size());
        loads.values().forEach(count -> assertEquals(1L, count));
    }

    @Test
    @Timeout(10)
    void loadingStartsOnlyWhenTheSourceBecomesReady() throws Exception {
        CachedStatisticStorage storage = new CachedStatisticStorage();
        List<List<ExternalStatisticsCacheKey>> batches = new CopyOnWriteArrayList<>();
        FakeSource source = new FakeSource();
        source.ready = false;
        source.externalUuids = uuids(6);
        AtomicInteger callsWhileDown = new AtomicInteger();
        storage.externalStatisticsCache = externalCache(1000, batches, keys -> {
            if (!source.ready) {
                callsWhileDown.incrementAndGet();
            }
            return false;
        });
        Thread recovery = new Thread(() -> {
            try {
                Thread.sleep(60);
            } catch (InterruptedException e) {
                return;
            }
            source.ready = true;
        });
        recovery.start();

        preloader(storage, source).run();
        recovery.join();

        assertEquals(0, callsWhileDown.get());
        assertEquals(6, loadsPerKey(batches).size());
    }

    @Test
    @Timeout(10)
    void aKeyThatAlwaysFailsStopsAtTheRetryWindow() {
        CachedStatisticStorage storage = new CachedStatisticStorage();
        List<List<ExternalStatisticsCacheKey>> batches = new CopyOnWriteArrayList<>();
        ExternalStatisticsCacheKey bad = ExternalStatisticsCacheKey.tableRow("ice.db.t1");
        AtomicInteger badAttempts = new AtomicInteger();
        // The source is ready all the time, so the failure has nothing to do with availability.
        storage.externalStatisticsCache = externalCache(1000, batches, keys -> {
            if (keys.contains(bad)) {
                badAttempts.incrementAndGet();
                return true;
            }
            return false;
        });
        FakeSource source = new FakeSource();
        source.externalUuids = uuids(6);

        long start = System.nanoTime();
        new StatisticsCachePreloader(storage, source, new Timing(1000, 2, 5, 20, 200)).run();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(elapsedMillis >= 190, "The retries go on until the window ends: " + elapsedMillis);
        assertTrue(elapsedMillis < 5000, "The retries end with the window: " + elapsedMillis);
        assertTrue(badAttempts.get() >= 5, "The key is retried: " + badAttempts.get());
        assertTrue(badAttempts.get() < 100, "The pause between rounds grows: " + badAttempts.get());
        Map<ExternalStatisticsCacheKey, Long> loads = loadsPerKey(batches);
        assertEquals(5, loads.size(), "The other keys are loaded");
        assertFalse(loads.containsKey(bad));
        loads.values().forEach(count -> assertEquals(1L, count));
    }

    @Test
    @Timeout(10)
    void interruptionEndsTheRetries() throws Exception {
        CachedStatisticStorage storage = new CachedStatisticStorage();
        AtomicInteger attempts = new AtomicInteger();
        storage.externalStatisticsCache = externalCache(1000, new CopyOnWriteArrayList<>(), keys -> {
            attempts.incrementAndGet();
            return true;
        });
        FakeSource source = new FakeSource();
        source.externalUuids = uuids(2);
        StatisticsCachePreloader preloader =
                new StatisticsCachePreloader(storage, source, new Timing(1000, 2, 20, 20, 60_000));
        Thread worker = new Thread(preloader);
        worker.start();

        // The first pass makes three attempts, and every round after it two more.
        waitFor(() -> attempts.get() >= 7);
        worker.interrupt();
        worker.join(5000);

        assertFalse(worker.isAlive(), "The worker does not wait for the end of a 60 s window");
        int attemptsAtEnd = attempts.get();
        Thread.sleep(100);
        assertEquals(attemptsAtEnd, attempts.get(), "No load starts after the interruption");
    }

    @Test
    @Timeout(10)
    void stopCheckStillStopsTheRetries() throws Exception {
        CachedStatisticStorage storage = new CachedStatisticStorage();
        List<List<ExternalStatisticsCacheKey>> batches = new CopyOnWriteArrayList<>();
        FakeSource source = new FakeSource();
        source.externalUuids = uuids(6);
        AtomicInteger calls = new AtomicInteger();
        List<Thread> fillers = new CopyOnWriteArrayList<>();
        storage.externalStatisticsCache = externalCache(6, batches, keys -> {
            if (calls.incrementAndGet() > 1) {
                return false;
            }
            // The first load fails. While the preloader waits, other entries fill the cache and evict some.
            source.ready = false;
            Thread filler = new Thread(() -> {
                for (int i = 0; i < 20; i++) {
                    storage.externalStatisticsCache.synchronous()
                            .put(ExternalStatisticsCacheKey.tableRow("ice.db.other" + i), Optional.empty());
                }
                source.ready = true;
            });
            fillers.add(filler);
            filler.start();
            return true;
        });

        preloader(storage, source).run();
        for (Thread filler : fillers) {
            filler.join();
        }

        assertEquals(1, calls.get(), "The cache is full, so the failed batch is not retried");
        assertTrue(batches.isEmpty());
    }
}
