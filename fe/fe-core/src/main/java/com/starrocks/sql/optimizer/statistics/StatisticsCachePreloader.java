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
import com.starrocks.qe.scheduler.DefaultWorkerProvider;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Fills the statistics caches from the statistics tables after FE start, so the first queries do not plan
 * with unknown statistics while the lazy loads run. Every cache is filled through its own loader and
 * stops at its first eviction: a full cache would only lose entries for the ones loaded after that.
 * Batches are small and awaited one by one, so the shared loader executors stay free for queries.
 * We expect the compute nodes to restart together with the FE, and the statistics queries fail while none is
 * alive. So the preload starts only when a worker that can run them is alive. A batch that fails is retried:
 * after one pass over all caches we run rounds over the failed batches with a growing pause, until a time
 * window ends, and then log what stays unloaded. A failure is logged and never reaches FE start or queries.
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
    static final String STOP_NOT_READY = "statistics not ready";
    static final String STOP_INTERRUPTED = "interrupted";

    private static final long BATCH_TIMEOUT_SECONDS = 300;
    private static final long JOIN_LOAD_TIMEOUT_MILLIS = 120_000;
    private static final long READY_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(10);
    private static final long READY_POLL_MILLIS = 1000;

    /** What the preloader needs to know about the catalog and the statistics tables. */
    interface Source {
        /**
         * True when the catalog and the statistics tables are usable and a compute node or backend that can run
         * the statistics queries is alive.
         */
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

    /**
     * What one pass over a list of batches did. The unloaded batches are the failed ones and, when the pass
     * stopped for a reason that goes away by itself, the ones it did not reach.
     */
    record Result<K>(long loaded, int failedBatches, String stopReason, List<List<K>> unloaded) {
    }

    /** All waits of the preloader. The retry waits start at retryStartMillis and double up to retryMaxMillis. */
    record Timing(long readyTimeoutMillis, long readyPollMillis, long retryStartMillis, long retryMaxMillis,
                  long retryWindowMillis) {
        Timing {
            // A start of zero stays zero when doubled and would make the retries spin.
            retryStartMillis = Math.max(1, retryStartMillis);
            retryMaxMillis = Math.max(retryStartMillis, retryMaxMillis);
        }
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
    private final Timing timing;
    // The caches that still have entries to retry after their first pass.
    private final List<CacheLoad<?>> unfinished = new ArrayList<>();

    StatisticsCachePreloader(CachedStatisticStorage storage, Source source, Timing timing) {
        this.storage = storage;
        this.source = source;
        this.timing = timing;
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
            Timing timing = new Timing(READY_TIMEOUT_MILLIS, READY_POLL_MILLIS,
                    TimeUnit.SECONDS.toMillis(Config.statistic_preload_on_start_retry_backoff_start_sec),
                    TimeUnit.SECONDS.toMillis(Config.statistic_preload_on_start_retry_backoff_max_sec),
                    TimeUnit.SECONDS.toMillis(Config.statistic_preload_on_start_retry_window_sec));
            Thread worker = new Thread(new StatisticsCachePreloader(cached, new CatalogSource(), timing),
                    "statistics-cache-preloader");
            worker.setDaemon(true);
            worker.start();
        } catch (Throwable e) {
            LOG.warn("Failed to start the statistics cache preloader", e);
        }
    }

    @Override
    public void run() {
        try {
            if (!awaitReady(timing.readyTimeoutMillis())) {
                LOG.warn("Gave up waiting for the statistics tables, the statistics caches are not preloaded");
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
            retryUnfinished();
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

    /** Waits until the source is ready, for at most timeoutMillis. It checks once even when the timeout is 0. */
    boolean awaitReady(long timeoutMillis) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            if (sourceReady()) {
                return true;
            }
            if (System.nanoTime() >= deadline || !pause(timing.readyPollMillis())) {
                return false;
            }
        }
    }

    private boolean sourceReady() {
        try {
            return source.ready();
        } catch (Exception e) {
            LOG.debug("Statistics are not ready for the cache preload", e);
            return false;
        }
    }

    /** Sleeps, and returns false when the thread is interrupted. */
    private static boolean pause(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void preloadExternal() {
        // Table rows and partition rows share one cache, so one check covers both.
        StopCheck stop = weightedCacheStop(storage.externalStatisticsCache);

        List<String> uuids = source.externalTableUuids();
        List<ExternalStatisticsCacheKey> tableKeys = new ArrayList<>();
        uuids.forEach(uuid -> tableKeys.add(ExternalStatisticsCacheKey.tableRow(uuid)));
        // One table that cannot be loaded (dropped, or its UUID changed) must not cost its batch mates.
        load("external table rows", () -> chunk(tableKeys, EXTERNAL_TABLE_BATCH_SIZE),
                batch -> await(storage.externalStatisticsCache.getAll(batch)), stop, true);

        // Listing the partitions is a statistics query too, so a failure of it is retried like a failed batch.
        load("external partition rows", () -> partitionBatches(uuids),
                batch -> await(storage.loadExternalPartitionBatch(batch)), stop, false);
    }

    private List<List<ExternalStatisticsCacheKey>> partitionBatches(List<String> uuids) throws Exception {
        List<List<ExternalStatisticsCacheKey>> batches = new ArrayList<>();
        source.externalPartitions(uuids).forEach((uuid, names) -> {
            List<ExternalStatisticsCacheKey> keys = new ArrayList<>();
            names.forEach(name -> keys.add(ExternalStatisticsCacheKey.partitionRow(uuid, name)));
            batches.addAll(chunk(keys, EXTERNAL_PARTITION_BATCH_SIZE));
        });
        return batches;
    }

    private void preloadMcv() {
        StopCheck full = weightedCacheStop(storage.externalMcvStats);
        // A failed MCV load starts a retry pause. We stop the pass, because loading on would only repeat the
        // failure, and the next round runs after the pause.
        StopCheck stop = size -> storage.externalMcvBackoffActive() ? STOP_BACKOFF : full.reason(size);

        load("external MCV statistics", () -> chunk(source.mcvTableUuids(), 1),
                batch -> storage.externalMcvStats.get(batch.get(0)).get(BATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .isPresent() ? 1 : 0, stop, false);
    }

    private void preloadInternal() {
        List<InternalTable> tables = source.internalTables();

        load("internal column statistics", () -> columnBatches(tables),
                batch -> await(storage.columnStatistics.getAll(batch)), sizeCacheStop(storage.columnStatistics), false);
        load("internal row counts", () -> rowCountBatches(tables),
                batch -> await(storage.tableStatsCache.getAll(batch)), sizeCacheStop(storage.tableStatsCache), false);
    }

    private void preloadJoin() {
        List<BooleanSupplier> loads = source.joinLoads();
        if (loads.isEmpty()) {
            LOG.info("Statistics preload of join statistics: nothing to load");
            return;
        }
        long evictionsAtStart = source.joinEvictions();
        // A load that returns false left nothing in the cache: its read failed, timed out, or is paused after an
        // earlier failure. We count it as failed so that it is retried.
        load("join statistics", () -> chunk(loads, 1), batch -> {
            if (!batch.get(0).getAsBoolean()) {
                throw new IllegalStateException("The join statistics are not in the cache");
            }
            return 1;
        }, size -> source.joinEvictions() > evictionsAtStart ? STOP_EVICTION : null, false);
    }

    /** Runs the first pass over one cache and keeps it for the retry rounds when something is left. */
    private <K> void load(String name, Callable<List<List<K>>> plan, BatchLoader<K> loader, StopCheck stop,
                          boolean splitOnFailure) {
        CacheLoad<K> load = new CacheLoad<>(name, plan, loader, stop, splitOnFailure);
        if (!load.runRound()) {
            unfinished.add(load);
        }
    }

    /**
     * We fear that the first pass met an outage, for example no alive compute node, and that other failures have
     * a cause that does not go away. So we run rounds over what is left with a pause that doubles up to a cap,
     * and wait for the source to be ready before each round, because a node may be down again. The window bounds
     * the rounds, so a key that never loads costs only the rounds until the window ends.
     */
    private void retryUnfinished() {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timing.retryWindowMillis());
        long pauseMillis = timing.retryStartMillis();
        String why = null;
        while (!unfinished.isEmpty()) {
            long left = millisUntil(deadline);
            if (Thread.currentThread().isInterrupted()) {
                why = STOP_INTERRUPTED;
                break;
            }
            if (left <= 0) {
                why = "the retry window of " + TimeUnit.MILLISECONDS.toSeconds(timing.retryWindowMillis())
                        + " s ended";
                break;
            }
            // The last pause is cut at the window end, so the last round runs there.
            if (!pause(Math.min(pauseMillis, left))) {
                continue;
            }
            pauseMillis = Math.min(pauseMillis * 2, timing.retryMaxMillis());
            if (!awaitReady(Math.min(timing.readyTimeoutMillis(), Math.max(0, millisUntil(deadline))))) {
                continue;
            }
            unfinished.removeIf(CacheLoad::runRound);
        }
        for (CacheLoad<?> load : unfinished) {
            load.giveUp(why);
        }
    }

    private static long millisUntil(long deadlineNanos) {
        return TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
    }

    /**
     * One cache that is filled in rounds. Round 0 loads everything, and each later round loads what the rounds
     * before did not load. The plan lists the batches on the first round, and runs again on the next one when
     * it fails.
     */
    private final class CacheLoad<K> {
        private final String name;
        private final Callable<List<List<K>>> plan;
        private final BatchLoader<K> loader;
        private final StopCheck stop;
        private final boolean splitOnFailure;
        private List<List<K>> pending;
        private int round;

        private CacheLoad(String name, Callable<List<List<K>>> plan, BatchLoader<K> loader, StopCheck stop,
                          boolean splitOnFailure) {
            this.name = name;
            this.plan = plan;
            this.loader = loader;
            this.stop = stop;
            this.splitOnFailure = splitOnFailure;
        }

        /** Runs one round and returns true when nothing is left to retry. */
        private boolean runRound() {
            long start = System.nanoTime();
            int current = round++;
            if (pending == null) {
                try {
                    pending = plan.call();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                } catch (Exception e) {
                    LOG.warn("Failed to list the batches of {} in round {}", name, current, e);
                    return false;
                }
            }
            int retried = current == 0 ? 0 : pending.size();
            Result<K> result = loadBatches(pending, loader, stop, splitOnFailure,
                    StatisticsCachePreloader.this::sourceReady);
            // A full cache is not an error. We load nothing more into it, so nothing is left to retry.
            pending = STOP_EVICTION.equals(result.stopReason()) ? List.of() : result.unloaded();
            LOG.info("Statistics preload of {}, round {}: retried {} batches, loaded {} entries in {} ms, "
                            + "{} failed batches, {} batches left to retry, stopped on {}",
                    name, current, retried, result.loaded(),
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start), result.failedBatches(),
                    pending.size(), result.stopReason() == null ? "nothing left to load" : result.stopReason());
            return pending.isEmpty();
        }

        private void giveUp(String why) {
            if (pending == null) {
                LOG.warn("Statistics preload of {} gave up after {} rounds because {}: the batches were never listed",
                        name, round, why);
                return;
            }
            long keys = pending.stream().mapToLong(List::size).sum();
            LOG.warn("Statistics preload of {} gave up after {} rounds because {}: {} batches with {} entries stay "
                            + "unloaded, the first one starts at {}",
                    name, round, why, pending.size(), keys, pending.get(0).get(0));
        }
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
     * returned in unloaded, so the caller can retry it. With splitOnFailure the keys of a failed batch are
     * loaded one by one, so one bad key does not cost the others.
     * We fear that a failure comes from an outage, and then every other batch would fail and fill the log. So
     * after a failure we ask whether the source is still ready, and if not, we stop and leave the rest unloaded.
     */
    static <K> Result<K> loadBatches(List<List<K>> batches, BatchLoader<K> loader, StopCheck stop,
                                     boolean splitOnFailure, BooleanSupplier ready) {
        long loaded = 0;
        int failed = 0;
        String reason = null;
        List<List<K>> unloaded = new ArrayList<>();
        Deque<List<K>> queue = new ArrayDeque<>(batches);
        while (!queue.isEmpty()) {
            if (Thread.currentThread().isInterrupted()) {
                reason = STOP_INTERRUPTED;
                break;
            }
            List<K> batch = queue.peekFirst();
            reason = stop.reason(batch.size());
            if (reason != null) {
                break;
            }
            queue.removeFirst();
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
                if (!ready.getAsBoolean()) {
                    unloaded.add(batch);
                    reason = STOP_NOT_READY;
                    break;
                }
                if (splitOnFailure && batch.size() > 1) {
                    for (int i = batch.size() - 1; i >= 0; i--) {
                        queue.addFirst(List.of(batch.get(i)));
                    }
                } else {
                    unloaded.add(batch);
                }
            }
        }
        if (reason == null) {
            // The last batch may have been the one that filled the cache.
            reason = stop.reason(0);
        }
        if (endsByItself(reason)) {
            unloaded.addAll(queue);
        }
        return new Result<>(loaded, failed, reason, unloaded);
    }

    /** A load backoff and an outage end by themselves, so the batches they kept us from loading are worth a retry. */
    private static boolean endsByItself(String reason) {
        return STOP_BACKOFF.equals(reason) || STOP_NOT_READY.equals(reason);
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

    private static final class CatalogSource implements Source {
        @Override
        public boolean ready() {
            return GlobalStateMgr.getCurrentState().isReady() && statisticsWorkerAvailable()
                    && StatisticUtils.checkStatisticTableStateNormal();
        }

        /**
         * The statistics queries fail while planning when no worker can run them. We expect them to run in the
         * background warehouse in shared-data mode (StatisticUtils.buildConnectContext picks it) and on any
         * backend or compute node otherwise. The scheduler takes only a worker that is alive and not blocklisted,
         * so we wait for such a worker.
         */
        private static boolean statisticsWorkerAvailable() {
            GlobalStateMgr state = GlobalStateMgr.getCurrentState();
            if (RunMode.isSharedDataMode()) {
                WarehouseManager warehouses = state.getWarehouseMgr();
                return warehouses.getAliveWarehouseComputeNodes(warehouses.getBackgroundComputeResource()).stream()
                        .anyMatch(DefaultWorkerProvider::isWorkerAvailable);
            }
            return state.getNodeMgr().getClusterInfo().backendAndComputeNodeStream()
                    .anyMatch(DefaultWorkerProvider::isWorkerAvailable);
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
