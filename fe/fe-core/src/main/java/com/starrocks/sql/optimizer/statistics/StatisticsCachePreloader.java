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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.JoinStatisticsMeta;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Fills the statistics caches from the statistics tables after FE start, so the first queries do not plan
 * with unknown statistics while the lazy loads run. Every cache is filled through its own loader and
 * stops at its first eviction: a full cache would only lose entries for the ones loaded after that.
 * Batches are small and awaited one by one, so the shared loader executors stay free for queries.
 * A failure is logged and never reaches FE start or queries.
 */
public final class StatisticsCachePreloader implements Runnable {
    private static final Logger LOG = LogManager.getLogger(StatisticsCachePreloader.class);
    private static final AtomicBoolean STARTED = new AtomicBoolean();

    // The query path loads partition rows eight at a time because a row can carry about 1 MB.
    static final int EXTERNAL_PARTITION_BATCH_SIZE = 8;
    static final int EXTERNAL_TABLE_BATCH_SIZE = 4;
    static final int COLUMN_BATCH_SIZE = 256;
    static final int ROW_COUNT_BATCH_SIZE = 1024;

    static final String STOP_EVICTION = "eviction";
    static final String STOP_BACKOFF = "load backoff";
    static final String STOP_INTERRUPTED = "interrupted";

    private static final long BATCH_TIMEOUT_SECONDS = 300;
    private static final long JOIN_LOAD_TIMEOUT_MILLIS = 120_000;
    private static final long READY_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(10);
    private static final long READY_POLL_MILLIS = 1000;

    /** What the preloader needs to know about the catalog and the statistics tables. */
    interface Source {
        boolean ready();

        List<String> externalTableUuids();

        /** Partition names by table UUID, the most recently updated partitions first. */
        Map<String, List<String>> externalPartitions(Collection<String> uuids) throws Exception;

        List<String> mcvTableUuids();

        List<InternalTable> internalTables();

        /** One load per join statistics object; a load returns true when the data is in the cache. */
        List<BooleanSupplier> joinLoads();

        long joinEvictions();
    }

    record InternalTable(long tableId, List<String> columns, List<Long> partitionIds) {
    }

    record Result(long loaded, int failedBatches, String stopReason) {
    }

    @FunctionalInterface
    interface BatchLoader<K> {
        /** Loads one batch, waits for it and returns the number of loaded entries. */
        int load(List<K> batch) throws Exception;
    }

    @FunctionalInterface
    interface StopCheck {
        /** Returns why loading must stop before a batch of the given size, or null to go on. */
        String reason(int nextBatchSize);
    }

    private final CachedStatisticStorage storage;
    private final Source source;
    private final long readyTimeoutMillis;
    private final long readyPollMillis;

    StatisticsCachePreloader(CachedStatisticStorage storage, Source source, long readyTimeoutMillis,
                             long readyPollMillis) {
        this.storage = storage;
        this.source = source;
        this.readyTimeoutMillis = readyTimeoutMillis;
        this.readyPollMillis = readyPollMillis;
    }

    /** Starts the worker once per process. A storage other than the cached one has nothing to fill. */
    public static void startOnce(StatisticStorage statisticStorage) {
        if (!(statisticStorage instanceof CachedStatisticStorage cached)) {
            return;
        }
        if (!Config.statistic_preload_on_start_basic && !Config.statistic_preload_on_start_mcv
                && !Config.statistic_preload_on_start_join) {
            return;
        }
        if (!STARTED.compareAndSet(false, true)) {
            return;
        }
        try {
            Thread worker = new Thread(new StatisticsCachePreloader(cached, new CatalogSource(),
                    READY_TIMEOUT_MILLIS, READY_POLL_MILLIS), "statistics-cache-preloader");
            worker.setDaemon(true);
            worker.start();
        } catch (Throwable e) {
            LOG.warn("Failed to start the statistics cache preloader", e);
        }
    }

    @Override
    public void run() {
        try {
            if (!awaitReady()) {
                return;
            }
            if (Config.statistic_preload_on_start_basic) {
                runSection("external statistics", this::preloadExternal);
            }
            if (Config.statistic_preload_on_start_mcv) {
                runSection("external MCV statistics", this::preloadMcv);
            }
            if (Config.statistic_preload_on_start_basic) {
                runSection("internal statistics", this::preloadInternal);
            }
            if (Config.statistic_preload_on_start_join) {
                runSection("join statistics", this::preloadJoin);
            }
        } catch (Throwable e) {
            LOG.warn("The statistics cache preloader stopped on an error", e);
        }
    }

    private interface Section {
        void run() throws Exception;
    }

    private static void runSection(String name, Section section) {
        try {
            section.run();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.warn("Failed to preload {}", name, e);
        }
    }

    boolean awaitReady() {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(readyTimeoutMillis);
        while (true) {
            try {
                if (source.ready()) {
                    return true;
                }
            } catch (Exception e) {
                LOG.debug("Statistics are not ready for the cache preload", e);
            }
            if (System.nanoTime() >= deadline) {
                LOG.warn("Gave up waiting for the statistics tables, the statistics caches are not preloaded");
                return false;
            }
            try {
                Thread.sleep(readyPollMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    private void preloadExternal() throws Exception {
        // Table rows and partition rows share one cache, so one check covers both.
        StopCheck stop = weightedCacheStop(storage.externalStatisticsCache);

        long start = System.nanoTime();
        List<String> uuids = source.externalTableUuids();
        List<ExternalStatisticsCacheKey> tableKeys = new ArrayList<>();
        uuids.forEach(uuid -> tableKeys.add(ExternalStatisticsCacheKey.tableRow(uuid)));
        // One table that cannot be loaded (dropped, or its UUID changed) must not cost its batch mates.
        Result tables = loadBatches(chunk(tableKeys, EXTERNAL_TABLE_BATCH_SIZE),
                batch -> await(storage.externalStatisticsCache.getAll(batch)), stop, true);
        logResult("external table rows", tables, start);

        start = System.nanoTime();
        Map<String, List<String>> partitions = source.externalPartitions(uuids);
        List<List<ExternalStatisticsCacheKey>> batches = new ArrayList<>();
        partitions.forEach((uuid, names) -> {
            List<ExternalStatisticsCacheKey> keys = new ArrayList<>();
            names.forEach(name -> keys.add(ExternalStatisticsCacheKey.partitionRow(uuid, name)));
            batches.addAll(chunk(keys, EXTERNAL_PARTITION_BATCH_SIZE));
        });
        Result rows = loadBatches(batches, batch -> await(storage.loadExternalPartitionBatch(batch)), stop, false);
        logResult("external partition rows", rows, start);
    }

    private void preloadMcv() {
        StopCheck full = weightedCacheStop(storage.externalMcvStats);
        // A failed MCV load starts a retry pause. Loading on would only repeat the failure.
        StopCheck stop = size -> storage.externalMcvBackoffActive() ? STOP_BACKOFF : full.reason(size);

        long start = System.nanoTime();
        List<List<String>> batches = chunk(source.mcvTableUuids(), 1);
        Result result = loadBatches(batches,
                batch -> storage.externalMcvStats.get(batch.get(0)).get(BATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .isPresent() ? 1 : 0, stop, false);
        logResult("external MCV statistics", result, start);
    }

    private void preloadInternal() {
        List<InternalTable> tables = source.internalTables();

        long start = System.nanoTime();
        Result columns = loadBatches(columnBatches(tables),
                batch -> await(storage.columnStatistics.getAll(batch)), sizeCacheStop(storage.columnStatistics), false);
        logResult("internal column statistics", columns, start);

        start = System.nanoTime();
        Result rowCounts = loadBatches(rowCountBatches(tables),
                batch -> await(storage.tableStatsCache.getAll(batch)), sizeCacheStop(storage.tableStatsCache), false);
        logResult("internal row counts", rowCounts, start);
    }

    private void preloadJoin() {
        long start = System.nanoTime();
        List<BooleanSupplier> loads = source.joinLoads();
        if (loads.isEmpty()) {
            logResult("join statistics", new Result(0, 0, null), start);
            return;
        }
        long evictionsAtStart = source.joinEvictions();
        Result result = loadBatches(chunk(loads, 1), batch -> batch.get(0).getAsBoolean() ? 1 : 0,
                size -> source.joinEvictions() > evictionsAtStart ? STOP_EVICTION : null, false);
        logResult("join statistics", result, start);
    }

    /** Every batch holds the keys of one table, because the loaders of these caches expect that. */
    static List<List<ColumnStatsCacheKey>> columnBatches(List<InternalTable> tables) {
        List<List<ColumnStatsCacheKey>> batches = new ArrayList<>();
        for (InternalTable table : tables) {
            List<ColumnStatsCacheKey> keys = new ArrayList<>();
            table.columns().forEach(column -> keys.add(new ColumnStatsCacheKey(table.tableId(), column)));
            batches.addAll(chunk(keys, COLUMN_BATCH_SIZE));
        }
        return batches;
    }

    static List<List<TableStatsCacheKey>> rowCountBatches(List<InternalTable> tables) {
        List<List<TableStatsCacheKey>> batches = new ArrayList<>();
        for (InternalTable table : tables) {
            List<TableStatsCacheKey> keys = new ArrayList<>();
            table.partitionIds().forEach(id -> keys.add(new TableStatsCacheKey(table.tableId(), id)));
            batches.addAll(chunk(keys, ROW_COUNT_BATCH_SIZE));
        }
        return batches;
    }

    static <T> List<List<T>> chunk(List<T> items, int size) {
        List<List<T>> chunks = new ArrayList<>();
        for (int offset = 0; offset < items.size(); offset += size) {
            chunks.add(items.subList(offset, Math.min(items.size(), offset + size)));
        }
        return chunks;
    }

    /**
     * Loads the batches one after another and stops when the check says so. A failed batch is logged and
     * skipped. With splitOnFailure the keys of a failed batch are retried one by one.
     */
    static <K> Result loadBatches(List<List<K>> batches, BatchLoader<K> loader, StopCheck stop,
                                  boolean splitOnFailure) {
        long loaded = 0;
        int failed = 0;
        String reason = null;
        for (List<K> batch : batches) {
            if (Thread.currentThread().isInterrupted()) {
                reason = STOP_INTERRUPTED;
                break;
            }
            reason = stop.reason(batch.size());
            if (reason != null) {
                break;
            }
            try {
                loaded += loader.load(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                reason = STOP_INTERRUPTED;
                break;
            } catch (Exception e) {
                failed++;
                LOG.warn("Failed to preload a batch of {} statistics entries starting at {}",
                        batch.size(), batch.get(0), e);
                if (splitOnFailure && batch.size() > 1) {
                    for (K key : batch) {
                        try {
                            loaded += loader.load(List.of(key));
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            reason = STOP_INTERRUPTED;
                            break;
                        } catch (Exception single) {
                            failed++;
                            LOG.warn("Failed to preload the statistics entry {}", key, single);
                        }
                    }
                    if (reason != null) {
                        break;
                    }
                }
            }
        }
        if (reason == null) {
            // The last batch may have been the one that filled the cache.
            reason = stop.reason(0);
        }
        return new Result(loaded, failed, reason);
    }

    /**
     * For a cache bounded by weight with statistics on: the cache is full when it evicted an entry since
     * the check was created, or when its weight already reached the maximum.
     */
    static StopCheck weightedCacheStop(AsyncLoadingCache<?, ?> cache) {
        long evictionsAtStart = cache.synchronous().stats().evictionCount();
        return size -> {
            if (cache.synchronous().stats().evictionCount() > evictionsAtStart) {
                return STOP_EVICTION;
            }
            return cache.synchronous().policy().eviction()
                    .filter(eviction -> eviction.weightedSize().orElse(0) >= eviction.getMaximum())
                    .map(eviction -> STOP_EVICTION).orElse(null);
        };
    }

    /**
     * For a cache bounded by entry count without statistics: we cannot see an eviction, so we stop when the
     * next batch would not fit.
     */
    static StopCheck sizeCacheStop(AsyncLoadingCache<?, ?> cache) {
        return size -> cache.synchronous().policy().eviction()
                .filter(eviction -> cache.synchronous().estimatedSize() + size > eviction.getMaximum())
                .map(eviction -> STOP_EVICTION).orElse(null);
    }

    /** Metas written before the UUID was stored have none, and the UUID cannot be rebuilt from the statistics tables. */
    static List<String> tableUuidsByRecency(Collection<ExternalBasicStatsMeta> metas) {
        return metas.stream()
                .filter(meta -> meta.getTableUUID() != null && !meta.getTableUUID().isEmpty())
                .sorted(Comparator.comparing(ExternalBasicStatsMeta::getUpdateTime,
                        Comparator.nullsLast(Comparator.<LocalDateTime>reverseOrder())))
                .map(ExternalBasicStatsMeta::getTableUUID)
                .distinct()
                .toList();
    }

    private static int await(CompletableFuture<? extends Map<?, ?>> future) throws Exception {
        return future.get(BATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS).size();
    }

    private static void logResult(String cache, Result result, long startNanos) {
        LOG.info("Statistics preload of {}: loaded {} entries in {} ms, {} failed batches, stopped on {}",
                cache, result.loaded(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos),
                result.failedBatches(), result.stopReason() == null ? "nothing left to load" : result.stopReason());
    }

    private static final class CatalogSource implements Source {
        @Override
        public boolean ready() {
            return GlobalStateMgr.getCurrentState().isReady() && StatisticUtils.checkStatisticTableStateNormal();
        }

        @Override
        public List<String> externalTableUuids() {
            return tableUuidsByRecency(
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().getExternalBasicStatsMetaMap().values());
        }

        @Override
        public Map<String, List<String>> externalPartitions(Collection<String> uuids) throws Exception {
            // The statistics tables keep a hash of the UUID, so only the metas can tell the real one.
            Map<String, String> uuidByHash = new HashMap<>();
            uuids.forEach(uuid -> uuidByHash.put(StatisticUtils.hashTableUuidForPkStorage(uuid), uuid));
            if (uuidByHash.isEmpty()) {
                return Map.of();
            }
            if (!StatisticUtils.checkStatisticTables(List.of(StatsConstants.EXTERNAL_PARTITION_STATISTICS_TABLE_NAME))) {
                throw new IllegalStateException("External partition statistics store is not ready");
            }
            ConnectContext context = StatisticUtils.buildConnectContext();
            context.setThreadLocalInfo();
            try {
                List<List<String>> rows = new StatisticExecutor().executeStatisticJsonDQL(context,
                        "SELECT table_uuid, partition_name FROM " + StatsConstants.STATISTICS_DB_NAME + "."
                                + StatsConstants.EXTERNAL_PARTITION_STATISTICS_TABLE_NAME
                                + " ORDER BY update_time DESC");
                Map<String, List<String>> result = new LinkedHashMap<>();
                for (List<String> row : rows) {
                    String uuid = uuidByHash.get(row.get(0));
                    if (uuid != null && row.get(1) != null) {
                        result.computeIfAbsent(uuid, ignored -> new ArrayList<>()).add(row.get(1));
                    }
                }
                return result;
            } finally {
                ConnectContext.remove();
            }
        }

        @Override
        public List<String> mcvTableUuids() {
            return GlobalStateMgr.getCurrentState().getAnalyzeMgr().getExternalMcvTableUuids();
        }

        @Override
        public List<InternalTable> internalTables() {
            List<BasicStatsMeta> metas = new ArrayList<>(
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().values());
            metas.sort(Comparator.comparing(BasicStatsMeta::getUpdateTime,
                    Comparator.nullsLast(Comparator.<LocalDateTime>reverseOrder())));
            List<InternalTable> tables = new ArrayList<>();
            for (BasicStatsMeta meta : metas) {
                try {
                    if (StatisticUtils.statisticTableBlackListCheck(meta.getTableId())) {
                        continue;
                    }
                    Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(meta.getDbId(), meta.getTableId());
                    if (!(table instanceof OlapTable)) {
                        continue;
                    }
                    List<String> columns = table.getBaseSchema().stream()
                            .filter(column -> column.getType().canStatistic())
                            .map(Column::getName).toList();
                    List<Long> partitionIds = table.getPartitions().stream().map(Partition::getId).toList();
                    tables.add(new InternalTable(meta.getTableId(), columns, partitionIds));
                } catch (Exception e) {
                    LOG.warn("Failed to list the statistics of table {} for the cache preload", meta.getTableId(), e);
                }
            }
            return tables;
        }

        @Override
        public List<BooleanSupplier> joinLoads() {
            AnalyzeMgr analyzeMgr = GlobalStateMgr.getCurrentState().getAnalyzeMgr();
            List<JoinStatisticsMeta> metas = analyzeMgr.getJoinStatisticsRegistry().snapshot().stream()
                    .filter(meta -> meta.getGeneration() > 0).toList();
            if (metas.isEmpty()) {
                return List.of();
            }
            return metas.stream()
                    .map(meta -> (BooleanSupplier) () -> analyzeMgr.getJoinStatisticsManager()
                            .inspect(meta, JOIN_LOAD_TIMEOUT_MILLIS).isPresent())
                    .toList();
        }

        @Override
        public long joinEvictions() {
            return GlobalStateMgr.getCurrentState().getAnalyzeMgr().getJoinStatisticsManager()
                    .getCacheMetrics().evictions();
        }
    }
}
