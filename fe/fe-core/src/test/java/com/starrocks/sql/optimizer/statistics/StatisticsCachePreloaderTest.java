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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatisticsCachePreloaderTest {
    private static final class FakeSource implements Source {
        boolean ready = true;
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
                        loadedBatches.add(batch);
                        return CompletableFuture.completedFuture(result);
                    }
                };
        return Caffeine.newBuilder().executor(Runnable::run).recordStats()
                .maximumWeight(maxWeight).weigher((key, value) -> 1).buildAsync(loader);
    }

    private static StatisticsCachePreloader preloader(CachedStatisticStorage storage, Source source) {
        return new StatisticsCachePreloader(storage, source, 1000, 5);
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
        Result result = StatisticsCachePreloader.loadBatches(batches, batch -> {
            if (batch.contains(3)) {
                throw new IllegalStateException("table is gone");
            }
            seen.addAll(batch);
            return batch.size();
        }, size -> null, false);

        assertEquals(List.of(1, 2, 5, 6), seen);
        assertEquals(4, result.loaded());
        assertEquals(1, result.failedBatches());
        assertNull(result.stopReason());
    }

    @Test
    void failedBatchCanBeRetriedKeyByKey() {
        List<List<String>> batches = StatisticsCachePreloader.chunk(List.of("a", "gone", "c", "d"), 4);
        Result result = StatisticsCachePreloader.loadBatches(batches, batch -> {
            if (batch.contains("gone")) {
                throw new IllegalStateException("table is gone");
            }
            return batch.size();
        }, size -> null, true);

        assertEquals(3, result.loaded());
        assertEquals(2, result.failedBatches());
    }

    @Test
    void stopCheckEndsLoadingBeforeTheNextBatch() {
        AtomicInteger loads = new AtomicInteger();
        Result result = StatisticsCachePreloader.loadBatches(StatisticsCachePreloader.chunk(List.of(1, 2, 3, 4), 1),
                batch -> {
                    loads.incrementAndGet();
                    return batch.size();
                },
                size -> loads.get() >= 2 ? StatisticsCachePreloader.STOP_EVICTION : null, false);

        assertEquals(2, loads.get());
        assertEquals(2, result.loaded());
        assertEquals(StatisticsCachePreloader.STOP_EVICTION, result.stopReason());
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

        StatisticsCachePreloader preloader = new StatisticsCachePreloader(storage, source, 30, 5);
        assertFalse(preloader.awaitReady());
        preloader.run();

        assertTrue(batches.isEmpty());
    }
}
