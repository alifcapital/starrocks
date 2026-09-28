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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Striped;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.connector.statistics.ConnectorHistogramColumnStatsCacheLoader;
import com.starrocks.connector.statistics.ConnectorTableColumnKey;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.memory.estimate.Estimator;
import com.starrocks.metric.StatisticsCacheMetrics;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.ColumnStatsMeta;
import com.starrocks.statistic.StatisticUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.stream.Collectors;


public class CachedStatisticStorage implements StatisticStorage, MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(CachedStatisticStorage.class);
    private static final int EXTERNAL_STATS_BATCH_SIZE = 4096;

    private final Executor statsCacheRefresherExecutor = Executors.newFixedThreadPool(Config.statistic_cache_thread_pool_size,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("stats-cache-refresher-%d").build());

    AsyncLoadingCache<TableStatsCacheKey, Optional<Long>> tableStatsCache =
            createAsyncLoadingCache(new TableStatsCacheLoader());

    AsyncLoadingCache<ColumnStatsCacheKey, Optional<ColumnStatistic>> columnStatistics =
            createAsyncLoadingCache(new ColumnBasicStatsCacheLoader());

    AsyncLoadingCache<ColumnStatsCacheKey, Optional<PartitionStats>> partitionStatistics =
            createAsyncLoadingCache(new PartitionStatsCacheLoader());

    private final ColumnHistogramStatsCacheLoader histogramLoader = new ColumnHistogramStatsCacheLoader();
    AsyncLoadingCache<ColumnStatsCacheKey, Optional<Histogram>> histogramCache =
            createAsyncLoadingCache(histogramLoader);

    AsyncLoadingCache<ConnectorTableColumnKey, Optional<Histogram>> connectorHistogramCache =
            createAsyncLoadingCache(new ConnectorHistogramColumnStatsCacheLoader());

    AsyncLoadingCache<Long, Optional<MultiColumnCombinedStatistics>> multiColumnStats =
            createAsyncLoadingCache(new MultiColumnCombinedStatsCacheLoader());

    private final ExternalMcvStatsCacheLoader externalMcvLoader = new ExternalMcvStatsCacheLoader();
    // Keyed by table UUID.
    AsyncLoadingCache<String, Optional<ExternalMcvStatistics>> externalMcvStats =
            createExternalMcvStatisticsCache(Config.statistic_mcv_cache_max_bytes,
                    statsCacheRefresherExecutor, externalMcvLoader);

    private final Executor externalPartitionStatsExecutor =
            com.starrocks.common.ThreadPoolManager.newDaemonFixedThreadPoolWithAbortPolicy(
                    Math.max(1, Config.external_statistics_partition_load_threads),
                    Math.max(1, Config.external_statistics_partition_load_queue_size), "external-partition-stats", false);

    private record BlockLoadKey(ExternalStatisticsCacheKey key, Object generation) {
    }

    private final Map<BlockLoadKey, CompletableFuture<Optional<ExternalColumnStatistics>>> pendingBlocks =
            new ConcurrentHashMap<>();

    private final Striped<Lock> externalStatisticsLoadLocks = Striped.lock(64);

    private final Semaphore externalStatisticsRefreshPermits =
            new Semaphore(Math.max(1, Config.statistic_cache_thread_pool_size));

    private final Map<ExternalStatisticsCacheKey, ExternalStatisticsRefresh>
            refreshingExternalStatistics = new ConcurrentHashMap<>();

    AsyncLoadingCache<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>
            externalStatisticsCache = createExternalStatisticsCache();

    @Override
    public void refreshCacheLimits() {
        updateCacheMaximum(externalStatisticsCache, Config.external_statistics_cache_max_bytes);
        updateCacheMaximum(externalMcvStats, Config.statistic_mcv_cache_max_bytes);
    }

    private static void updateCacheMaximum(AsyncLoadingCache<?, ?> cache, long maximumBytes) {
        cache.synchronous().policy().eviction().ifPresent(eviction -> {
            if (eviction.getMaximum() != maximumBytes) {
                eviction.setMaximum(maximumBytes);
            }
        });
    }

    @Override
    public Map<Long, Optional<Long>> getTableStatistics(Long tableId, Collection<Partition> partitions) {
        // get Statistics Table column info, just return default column statistics
        if (StatisticUtils.statisticTableBlackListCheck(tableId)) {
            return partitions.stream().collect(Collectors.toMap(Partition::getId, p -> Optional.empty()));
        }

        List<TableStatsCacheKey> keys = partitions.stream().map(p -> new TableStatsCacheKey(tableId, p.getId()))
                .collect(Collectors.toList());

        try {
            CompletableFuture<Map<TableStatsCacheKey, Optional<Long>>> result = tableStatsCache.getAll(keys);
            if (Config.enable_sync_statistics_load) {
                result.get();
            }
            if (result.isDone()) {
                Map<TableStatsCacheKey, Optional<Long>> data = result.get();
                return keys.stream().collect(Collectors.toMap(TableStatsCacheKey::getPartitionId,
                        k -> data.getOrDefault(k, Optional.empty())));
            }
        } catch (InterruptedException e) {
            LOG.warn("Failed to execute tableStatsCache.getAll", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("Faied to execute tableStatsCache.getAll", e);
        }
        return partitions.stream().collect(Collectors.toMap(Partition::getId, p -> Optional.empty()));
    }

    @Override
    public void refreshTableStatistic(Table table, boolean isSync) {
        List<TableStatsCacheKey> statsCacheKeyList = new ArrayList<>();
        for (Partition partition : table.getPartitions()) {
            statsCacheKeyList.add(new TableStatsCacheKey(table.getId(), partition.getId()));
        }

        try {
            TableStatsCacheLoader loader = new TableStatsCacheLoader();
            CompletableFuture<Map<TableStatsCacheKey, Optional<Long>>> future = loader.asyncLoadAll(statsCacheKeyList,
                    statsCacheRefresherExecutor);
            if (isSync) {
                Map<TableStatsCacheKey, Optional<Long>> result = future.get();
                tableStatsCache.synchronous().putAll(result);
            } else {
                refreshCacheOnSuccess(future, result -> tableStatsCache.synchronous().putAll(result));
            }
        } catch (InterruptedException e) {
            LOG.warn("Failed to execute refreshTableStatistic", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("Failed to execute refreshTableStatistic", e);
        }
    }

    @Override
    public void refreshColumnStatistics(Table table, List<String> columns, boolean isSync) {
        Preconditions.checkState(table != null);

        // get Statistics Table column info, just return default column statistics
        if (StatisticUtils.statisticTableBlackListCheck(table.getId()) ||
                !StatisticUtils.checkStatisticTableStateNormal()) {
            return;
        }

        List<ColumnStatsCacheKey> cacheKeys = new ArrayList<>();
        long tableId = table.getId();
        for (String column : columns) {
            cacheKeys.add(new ColumnStatsCacheKey(tableId, column));
        }

        try {
            ColumnBasicStatsCacheLoader loader = new ColumnBasicStatsCacheLoader();
            CompletableFuture<Map<ColumnStatsCacheKey, Optional<ColumnStatistic>>> future =
                    loader.asyncLoadAll(cacheKeys, statsCacheRefresherExecutor);
            if (isSync) {
                Map<ColumnStatsCacheKey, Optional<ColumnStatistic>> result = future.get();
                columnStatistics.synchronous().putAll(result);
            } else {
                refreshCacheOnSuccess(future, result -> columnStatistics.synchronous().putAll(result));
            }
        } catch (Exception e) {
            LOG.warn("Failed to refresh getColumnStatistics", e);
        }
    }

    @Override
    public void refreshHistogramStatistics(Table table, List<String> columns, boolean isSync) {
        Preconditions.checkState(table != null);

        if (StatisticUtils.statisticTableBlackListCheck(table.getId()) ||
                !StatisticUtils.checkStatisticTableStateNormal()) {
            return;
        }

        List<ColumnStatsCacheKey> cacheKeys = new ArrayList<>();
        long tableId = table.getId();
        for (String column : columns) {
            cacheKeys.add(new ColumnStatsCacheKey(tableId, column));
        }

        try {
            ColumnHistogramStatsCacheLoader loader = new ColumnHistogramStatsCacheLoader();
            CompletableFuture<Map<ColumnStatsCacheKey, Optional<Histogram>>> future =
                    loader.asyncLoadAll(cacheKeys, statsCacheRefresherExecutor);
            if (isSync) {
                Map<ColumnStatsCacheKey, Optional<Histogram>> result = future.get();
                histogramCache.synchronous().putAll(result);
            } else {
                refreshCacheOnSuccess(future, result -> histogramCache.synchronous().putAll(result));
            }
        } catch (Exception e) {
            LOG.warn("Failed to refresh histogram", e);
        }
    }

    @Override
    public void prefetchConnectorTableStatistics(Table table, List<String> columns) {
        if (columns.isEmpty() || StatisticUtils.statisticTableBlackListCheck(table.getId())
                || !StatisticUtils.checkStatisticTableStateNormal()) {
            return;
        }
        try {
            loadExternalStatistics(tableRequest(table, columns));
        } catch (Exception e) {
            LOG.warn("Failed to prefetch connector column statistics for {}", table.getName(), e);
        }
    }

    @Override
    public void prefetchExternalMcvStatistics(Table table) {
        if (!GlobalStateMgr.getCurrentState().getAnalyzeMgr().hasExternalMcvStatsMeta(table)
                || externalMcvLoader.backoff.active()) {
            return;
        }
        if (StatisticUtils.statisticTableBlackListCheck(table.getId()) || !StatisticUtils.checkStatisticTableStateNormal()) {
            return;
        }
        try {
            externalMcvStats.get(table.getUUID());
        } catch (Exception e) {
            LOG.warn("Failed to prefetch external MCV statistics for {}", table.getName(), e);
        }
    }

    private static ExternalStatisticsRequest tableRequest(Table table, List<String> columns) {
        // TABLE is a distinct scope, not a synthetic partition. No partition enumeration is needed.
        return new ExternalStatisticsRequest(table.getUUID(), List.of(), columns, true, table.isUnPartitioned());
    }

    @Override
    public List<ConnectorTableColumnStats> getConnectorTableStatistics(Table table, List<String> columns) {
        return getConnectorTableStatistics(table, columns, Config.enable_sync_statistics_load, false);
    }

    @Override
    public ColumnStatistic getCachedConnectorTableColumnStatistic(Table table, String column) {
        Optional<ExternalColumnStatistics> cached = externalStatisticsCache.synchronous().policy()
                .getIfPresentQuietly(ExternalStatisticsCacheKey.table(table.getUUID(), column));
        return cached != null && cached.orElse(null) instanceof ExternalColumnStatistics.Summary summary
                ? summary.statistic : ColumnStatistic.unknown();
    }

    @Override
    public List<ConnectorTableColumnStats> getConnectorTableStatisticsSync(Table table, List<String> columns) {
        return getConnectorTableStatistics(table, columns, true, true);
    }

    private List<ConnectorTableColumnStats> getConnectorTableStatistics(Table table, List<String> columns,
                                                                       boolean sync, boolean rawRowCount) {
        Preconditions.checkNotNull(table);
        if ((!rawRowCount && StatisticUtils.statisticTableBlackListCheck(table.getId()))
                || !StatisticUtils.checkStatisticTableStateNormal()) {
            return getDefaultConnectorTableStatistics(columns);
        }
        try {
            ExternalStatisticsRequest request = tableRequest(table, columns);
            CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>> future =
                    loadExternalTableSummaries(request);
            observeExternalStatisticsLoad(request, future.thenApply(values ->
                    ExternalStatisticsAggregate.fromTableSummaries(request, values)));
            Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> values =
                    sync ? future.get() : future.getNow(null);
            if (values == null) {
                return getDefaultConnectorTableStatistics(columns);
            }
            List<ConnectorTableColumnStats> result = new ArrayList<>();
            for (String column : columns) {
                ExternalColumnStatistics.Summary summary = (ExternalColumnStatistics.Summary) values
                        .getOrDefault(ExternalStatisticsCacheKey.table(table.getUUID(), column), Optional.empty()).orElse(null);
                result.add(summary == null ? ConnectorTableColumnStats.unknown() : new ConnectorTableColumnStats(
                        summary.statistic, rawRowCount ? summary.rawRowCount : (long) summary.rowCount, summary.updateTime));
            }
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted loading external table statistics", e);
        } catch (Exception e) {
            LOG.warn("Failed to load external table statistics", e);
        }
        return getDefaultConnectorTableStatistics(columns);
    }

    @Override
    public void expireConnectorTableColumnStatistics(Table table, List<String> columns) {
        if (table != null && columns != null) {
            expireExternalPartitionStatistics(table.getUUID(), columns);
        }
    }

    @Override
    public void invalidateConnectorTableColumnStatistics(String tableUUID, List<String> columns) {
        if (columns != null) {
            expireExternalPartitionStatistics(tableUUID, columns);
        }
    }

    @Override
    public void refreshConnectorTableColumnStatistics(Table table, List<String> columns, boolean isSync) {
        Preconditions.checkNotNull(table);
        expireExternalPartitionStatistics(table.getUUID(), columns);
        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return;
        }
        // Reserve the replacement in the same cache. Caffeine prevents an invalidated in-flight
        // load from publishing itself again after a later ANALYZE or DROP.
        try {
            CompletableFuture<ExternalStatisticsAggregate> load = loadExternalStatistics(tableRequest(table, columns));
            if (isSync) {
                load.get();
            } else {
                load.exceptionally(error -> {
                    LOG.warn("Failed to refresh external table statistics", error);
                    return null;
                });
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted refreshing external table statistics", e);
        } catch (Exception e) {
            LOG.warn("Failed to refresh external table statistics", e);
        }
    }

    @Override
    public ColumnStatistic getColumnStatistic(Table table, String column) {
        Preconditions.checkState(table != null);

        // get Statistics Table column info, just return default column statistics
        if (StatisticUtils.statisticTableBlackListCheck(table.getId())) {
            return ColumnStatistic.unknown();
        }

        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return ColumnStatistic.unknown();
        }
        try {
            CompletableFuture<Optional<ColumnStatistic>> result =
                        columnStatistics.get(new ColumnStatsCacheKey(table.getId(), column));
            if (Config.enable_sync_statistics_load) {
                result.get();
            }
            if (result.isDone()) {
                Optional<ColumnStatistic> realResult;
                realResult = result.get();
                return realResult.orElseGet(ColumnStatistic::unknown);
            } else {
                return ColumnStatistic.unknown();
            }
        } catch (Exception e) {
            LOG.warn("Failed to execute getColumnStatistic", e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return ColumnStatistic.unknown();
        }
    }

    // ColumnStatistic List sequence is guaranteed to be consistent with Columns
    @Override
    public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
        Preconditions.checkState(table != null);

        // get Statistics Table column info, just return default column statistics
        if (StatisticUtils.statisticTableBlackListCheck(table.getId())) {
            return getDefaultColumnStatisticList(columns);
        }

        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return getDefaultColumnStatisticList(columns);
        }

        List<ColumnStatsCacheKey> cacheKeys = new ArrayList<>();
        long tableId = table.getId();
        for (String column : columns) {
            cacheKeys.add(new ColumnStatsCacheKey(tableId, column));
        }

        try {
            CompletableFuture<Map<ColumnStatsCacheKey, Optional<ColumnStatistic>>> result =
                    columnStatistics.getAll(cacheKeys);
            if (Config.enable_sync_statistics_load) {
                result.get();
            }
            if (result.isDone()) {
                List<ColumnStatistic> columnStatistics = new ArrayList<>();
                Map<ColumnStatsCacheKey, Optional<ColumnStatistic>> realResult;
                realResult = result.get();
                for (String column : columns) {
                    Optional<ColumnStatistic> columnStatistic =
                            realResult.getOrDefault(new ColumnStatsCacheKey(tableId, column), Optional.empty());
                    if (columnStatistic.isPresent()) {
                        columnStatistics.add(columnStatistic.get());
                    } else {
                        columnStatistics.add(ColumnStatistic.unknown());
                    }
                }
                return columnStatistics;
            } else {
                return getDefaultColumnStatisticList(columns);
            }
        } catch (Exception e) {
            LOG.warn("Failed to execute getColumnStatistics", e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return getDefaultColumnStatisticList(columns);
        }
    }

    /**
     *
     */
    @VisibleForTesting
    public Map<String, PartitionStats> getColumnNDVForPartitions(Table table, List<String> columns) {

        BasicStatsMeta meta = GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                .getTableBasicStatsMeta(table.getId());
        List<ColumnStatsCacheKey> cacheKeys = columns.stream().filter(column -> {
            ColumnStatsMeta columnMeta = meta == null ? null : meta.getAnalyzedColumns().get(column);
            return columnMeta == null || !columnMeta.usesSampleStatisticsTable();
        }).map(column -> new ColumnStatsCacheKey(table.getId(), column)).toList();
        if (cacheKeys.isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>> resultFuture =
                    partitionStatistics.getAll(cacheKeys);
            if (Config.enable_sync_statistics_load) {
                resultFuture.get();
            }

            if (resultFuture.isDone()) {
                Map<ColumnStatsCacheKey, Optional<PartitionStats>> result = resultFuture.get();

                Map<String, PartitionStats> columnStatistics = Maps.newHashMap();
                result.forEach((k, v) ->
                        v.ifPresent(partitionStats -> columnStatistics.put(k.column, partitionStats)));
                return columnStatistics;
            }
            return Collections.emptyMap();
        } catch (InterruptedException e) {
            LOG.warn("Get partition NDV interrupted", e);
            Thread.currentThread().interrupt();
            return Collections.emptyMap();
        } catch (Exception e) {
            LOG.warn("Get partition NDV failed", e);
            return Collections.emptyMap();
        }
    }

    /**
     * We don't really maintain all statistics for partition, as most of them are not necessary.
     * Currently, the only partition-level statistics is DistinctCount, which may differs a lot among partitions
     */
    @Override
    public Map<Long, List<ColumnStatistic>> getColumnStatisticsOfPartitionLevel(Table table, List<Long> partitions,
                                                                                List<String> columns) {

        Preconditions.checkState(table != null);

        // get Statistics Table column info, just return default column statistics
        if (StatisticUtils.statisticTableBlackListCheck(table.getId())) {
            return null;
        }
        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return null;
        }

        List<ColumnStatistic> columnStatistics = getColumnStatistics(table, columns);
        Map<String, PartitionStats> columnNDVForPartitions = getColumnNDVForPartitions(table, columns);
        if (MapUtils.isEmpty(columnNDVForPartitions)) {
            return null;
        }

        Map<Long, List<ColumnStatistic>> result = Maps.newHashMap();
        for (long partition : partitions) {
            List<ColumnStatistic> newStatistics = Lists.newArrayList();
            for (int i = 0; i < columns.size(); i++) {
                ColumnStatistic columnStatistic = columnStatistics.get(i);
                PartitionStats partitionStats = columnNDVForPartitions.get(columns.get(i));
                if (partitionStats == null) {
                    // some of the columns miss statistics
                    return null;
                }
                if (!partitionStats.getDistinctCount().containsKey(partition)) {
                    // some of the partitions miss statistics
                    return null;
                }
                double distinctCount = partitionStats.getDistinctCount().get(partition);
                double nullFraction = partitionStats.getNullFraction().get(partition);
                ColumnStatistic newStats = ColumnStatistic.buildFrom(columnStatistic)
                        .setDistinctValuesCount(distinctCount)
                        .setNullsFraction(nullFraction).build();
                newStatistics.add(newStats);
            }
            result.put(partition, newStatistics);
        }
        return result;
    }

    @Override
    public void expireTableAndColumnStatistics(Table table, List<String> columns) {
        List<TableStatsCacheKey> tableStatsCacheKeys = Lists.newArrayList();
        for (Partition partition : table.getPartitions()) {
            tableStatsCacheKeys.add(new TableStatsCacheKey(table.getId(), partition.getId()));
        }
        tableStatsCache.synchronous().invalidateAll(tableStatsCacheKeys);

        if (columns == null) {
            return;
        }
        List<ColumnStatsCacheKey> allKeys = Lists.newArrayList();
        for (String column : columns) {
            ColumnStatsCacheKey key = new ColumnStatsCacheKey(table.getId(), column);
            allKeys.add(key);
        }
        columnStatistics.synchronous().invalidateAll(allKeys);
    }

    @Override
    public void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic) {
        this.columnStatistics.synchronous()
                .put(new ColumnStatsCacheKey(table.getId(), column), Optional.of(columnStatistic));
    }

    @Override
    public void addHistogramStatistics(Table table, String column, Histogram histogram) {
        this.histogramCache.synchronous()
                .put(new ColumnStatsCacheKey(table.getId(), column), Optional.of(histogram));
    }

    @Override
    public void addMultiColumnStatistics(Table table, MultiColumnCombinedStatistics statistics) {
        this.multiColumnStats.synchronous().put(table.getId(), Optional.of(statistics));
    }

    @Override
    public void addExternalMcvStatistics(Table table, ExternalMcvStatistics statistics) {
        this.externalMcvStats.synchronous().put(table.getUUID(), Optional.of(statistics));
    }

    @Override
    public Map<String, Histogram> getHistogramStatistics(Table table, List<String> columns) {
        Preconditions.checkState(table != null);

        // Skip loading histogram statistics when we are inside a statistics-collect connection
        // (recursion guard) or when the target is a statistics-internal table, or when the
        // statistics tables are not in a healthy state. Without this guard a histogram-collect
        // INSERT that holds the histogram_statistics READ lock would synchronously load the
        // histogram of its own source table, and that loader re-acquires the histogram_statistics
        // READ lock -> self-deadlock. This mirrors the guard already present in getColumnStatistics.
        if (StatisticUtils.statisticTableBlackListCheck(table.getId())) {
            return Maps.newHashMap();
        }
        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return Maps.newHashMap();
        }

        List<String> columnHasHistogram = new ArrayList<>();
        for (String columnName : columns) {
            if (GlobalStateMgr.getCurrentState().getAnalyzeMgr().getHistogramStatsMetaMap()
                    .get(new Pair<>(table.getId(), columnName)) != null) {
                columnHasHistogram.add(columnName);
            }
        }

        List<ColumnStatsCacheKey> cacheKeys = new ArrayList<>();
        long tableId = table.getId();
        for (String columnName : columnHasHistogram) {
            cacheKeys.add(new ColumnStatsCacheKey(tableId, columnName));
        }

        // Quiet reads are important: ordinary getIfPresent can trigger another failed refresh.
        // Explicit refresh after ANALYZE bypasses this short pause and can recover immediately.
        if (histogramLoader.backoff.active()) {
            Map<String, Histogram> cached = new HashMap<>();
            for (ColumnStatsCacheKey key : cacheKeys) {
                Optional<Histogram> value = histogramCache.synchronous().policy().getIfPresentQuietly(key);
                if (value != null) {
                    value.ifPresent(histogram -> cached.put(key.column, histogram));
                }
            }
            return cached;
        }

        try {
            CompletableFuture<Map<ColumnStatsCacheKey, Optional<Histogram>>> result = histogramCache.getAll(cacheKeys);
            if (Config.enable_sync_statistics_load) {
                result.get();
            }
            if (result.isDone()) {
                Map<ColumnStatsCacheKey, Optional<Histogram>> realResult;
                realResult = result.get();

                Map<String, Histogram> histogramStats = new HashMap<>();
                for (String columnName : columns) {
                    Optional<Histogram> histogramStatistics =
                            realResult.getOrDefault(new ColumnStatsCacheKey(tableId, columnName), Optional.empty());
                    histogramStatistics.ifPresent(histogram -> histogramStats.put(columnName, histogram));
                }
                return histogramStats;
            } else {
                return Maps.newHashMap();
            }
        } catch (Exception e) {
            LOG.debug("Failed to execute getHistogramStatistics", e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return Maps.newHashMap();
        }
    }

    @Override
    public Map<String, Histogram> getConnectorHistogramStatistics(Table table, List<String> columns) {
        Preconditions.checkState(table != null);

        List<ConnectorTableColumnKey> cacheKeys = new ArrayList<>();
        for (String columnName : columns) {
            cacheKeys.add(new ConnectorTableColumnKey(table.getUUID(), columnName));
        }

        try {
            CompletableFuture<Map<ConnectorTableColumnKey, Optional<Histogram>>> result =
                    connectorHistogramCache.getAll(cacheKeys);
            if (result.isDone()) {
                Map<ConnectorTableColumnKey, Optional<Histogram>> realResult = result.get();

                Map<String, Histogram> histogramStats = Maps.newHashMap();
                for (String columnName : columns) {
                    Optional<Histogram> histogramStatistics =
                            realResult.getOrDefault(new ConnectorTableColumnKey(table.getUUID(), columnName), Optional.empty());
                    histogramStatistics.ifPresent(histogram -> histogramStats.put(columnName, histogram));
                }
                return histogramStats;
            } else {
                return Maps.newHashMap();
            }
        } catch (Exception e) {
            LOG.warn("Failed to execute getConnectorHistogramStatistics", e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return Maps.newHashMap();
        }
    }

    @Override
    public void expireHistogramStatistics(Long tableId, List<String> columns) {
        Preconditions.checkNotNull(columns);

        List<ColumnStatsCacheKey> allKeys = Lists.newArrayList();
        for (String column : columns) {
            ColumnStatsCacheKey key = new ColumnStatsCacheKey(tableId, column);
            allKeys.add(key);
        }
        histogramCache.synchronous().invalidateAll(allKeys);
    }


    @Override
    public void expireConnectorHistogramStatistics(Table table, List<String> columns) {
        if (table == null || columns == null) {
            return;
        }
        List<ConnectorTableColumnKey> allKeys = Lists.newArrayList();
        for (String column : columns) {
            ConnectorTableColumnKey key = new ConnectorTableColumnKey(table.getUUID(), column);
            allKeys.add(key);
        }
        connectorHistogramCache.synchronous().invalidateAll(allKeys);
    }

    @Override
    public void invalidateConnectorHistogramStatistics(String tableUUID, List<String> columns) {
        if (tableUUID == null || tableUUID.isEmpty() || columns == null) {
            return;
        }
        List<ConnectorTableColumnKey> allKeys = columns.stream()
                .map(column -> new ConnectorTableColumnKey(tableUUID, column))
                .collect(Collectors.toList());
        connectorHistogramCache.synchronous().invalidateAll(allKeys);
    }

    private List<ColumnStatistic> getDefaultColumnStatisticList(List<String> columns) {
        List<ColumnStatistic> columnStatisticList = new ArrayList<>();
        for (int i = 0; i < columns.size(); ++i) {
            columnStatisticList.add(ColumnStatistic.unknown());
        }
        return columnStatisticList;
    }

    private List<ConnectorTableColumnStats> getDefaultConnectorTableStatistics(List<String> columns) {
        List<ConnectorTableColumnStats> connectorTableColumnStatsList = new ArrayList<>();
        for (int i = 0; i < columns.size(); ++i) {
            connectorTableColumnStatsList.add(ConnectorTableColumnStats.unknown());
        }
        return connectorTableColumnStatsList;
    }

    public MultiColumnCombinedStatistics getMultiColumnCombinedStatistics(Long tableId) {
        if (StatisticUtils.statisticTableBlackListCheck(tableId) ||
                !StatisticUtils.checkStatisticTableStateNormal()) {
            return MultiColumnCombinedStatistics.EMPTY;
        }

        try {
            CompletableFuture<Optional<MultiColumnCombinedStatistics>> result = multiColumnStats.get(tableId);
            if (Config.enable_sync_statistics_load) {
                result.get();
            }
            if (result.isDone()) {
                Optional<MultiColumnCombinedStatistics> data = result.get();
                return data.orElse(MultiColumnCombinedStatistics.EMPTY);
            }
        } catch (InterruptedException e) {
            LOG.warn("Failed to execute tableStatsCache.getAll", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("Faied to execute tableStatsCache.getAll", e);
        }
        return MultiColumnCombinedStatistics.EMPTY;
    }

    @Override
    public void refreshMultiColumnStatistics(Long tableId,  boolean isSync) {
        try {
            if (StatisticUtils.statisticTableBlackListCheck(tableId) ||
                    !StatisticUtils.checkStatisticTableStateNormal()) {
                return;
            }

            MultiColumnCombinedStatsCacheLoader loader = new MultiColumnCombinedStatsCacheLoader();
            CompletableFuture<Optional<MultiColumnCombinedStatistics>> future =
                    loader.asyncLoad(tableId, statsCacheRefresherExecutor);
            if (isSync) {
                Optional<MultiColumnCombinedStatistics> result = future.get();
                multiColumnStats.synchronous().put(tableId, result);
            } else {
                refreshCacheOnSuccess(future, result -> multiColumnStats.synchronous().put(tableId, result));
            }
        } catch (InterruptedException e) {
            LOG.warn("Failed to execute refresh multi-column combined statistics", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("Failed to execute refresh multi-column combined statistics", e);
        }
    }

    public void expireMultiColumnStatistics(Long tableId) {
        Preconditions.checkNotNull(tableId);
        multiColumnStats.synchronous().invalidate(tableId);
    }

    @Override
    public ExternalMcvStatistics getExternalMcvStatistics(Table table) {
        if (table == null || !GlobalStateMgr.getCurrentState().getAnalyzeMgr().hasExternalMcvStatsMeta(table)
                || !StatisticUtils.checkStatisticTableStateNormal()) {
            return ExternalMcvStatistics.EMPTY;
        }
        if (externalMcvLoader.backoff.active()) {
            Optional<ExternalMcvStatistics> cached = externalMcvStats.synchronous().policy()
                    .getIfPresentQuietly(table.getUUID());
            return cached == null ? ExternalMcvStatistics.EMPTY : cached.orElse(ExternalMcvStatistics.EMPTY);
        }
        try {
            CompletableFuture<Optional<ExternalMcvStatistics>> result =
                    externalMcvStats.get(table.getUUID());
            if (Config.enable_sync_statistics_load) {
                result.get();
            }
            if (result.isDone()) {
                return result.get().orElse(ExternalMcvStatistics.EMPTY);
            }
        } catch (InterruptedException e) {
            LOG.debug("Failed to load external multi-column statistics", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.debug("Failed to load external multi-column statistics", e);
        }
        return ExternalMcvStatistics.EMPTY;
    }

    @Override
    public CompletableFuture<ExternalStatisticsAggregate> loadExternalStatistics(ExternalStatisticsRequest request) {
        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return CompletableFuture.failedFuture(new IllegalStateException("External statistics table is not ready"));
        }
        if (!request.wholeTable) {
            return observeExternalStatisticsLoad(request, loadExternalPartitions(request));
        }
        return observeExternalStatisticsLoad(request, loadExternalTableSummaries(request).thenApply(values ->
                ExternalStatisticsAggregate.fromTableSummaries(request, values)));
    }

    private CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>> loadExternalTableSummaries(
            ExternalStatisticsRequest request) {
        List<ExternalStatisticsCacheKey> keys = request.columns.stream()
                .map(column -> ExternalStatisticsCacheKey.table(request.tableUUID, column)).toList();
        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }
        CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>> loaded;
        Lock lock = externalStatisticsLoadLocks.get(request.tableUUID);
        lock.lock();
        try {
            loaded = externalStatisticsCache.getAll(keys,
                    (missing, executor) -> loadExternalStatisticsBatches(missing));
        } finally {
            lock.unlock();
        }
        return loaded.thenApply(values -> {
            refreshExternalStatisticsBatch(keys);
            return values;
        });
    }

    private CompletableFuture<ExternalStatisticsAggregate> observeExternalStatisticsLoad(ExternalStatisticsRequest request,
            CompletableFuture<ExternalStatisticsAggregate> load) {
        ConnectContext context = ConnectContext.get();
        if (context != null && !context.isStatisticsConnection() && !context.isStatisticsJob()
                && context.getSessionVariable().isEnableQueryTriggerAnalyze()) {
            load.thenAcceptAsync(aggregate -> {
                if (GlobalStateMgr.getCurrentState().isLeader()) {
                    GlobalStateMgr.getCurrentState().getConnectorTableTriggerAnalyzeMgr().checkAndUpdateScopedTableStats(
                            request.tableUUID, aggregate.columns, aggregate.rowCount, request.wholeTable);
                }
            }, statsCacheRefresherExecutor).exceptionally(error -> {
                if (!load.isCompletedExceptionally()) {
                    LOG.warn("Failed to check scoped statistics for query-triggered ANALYZE", error);
                }
                return null;
            });
        }
        return load;
    }

    private CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>> loadExternalStatisticsBatches(
            Iterable<? extends ExternalStatisticsCacheKey> keys) {
        List<ExternalStatisticsCacheKey> all = new ArrayList<>();
        keys.forEach(all::add);
        Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> result = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> lanes = new ArrayList<>(List.of(
                CompletableFuture.completedFuture(null), CompletableFuture.completedFuture(null)));
        int lane = 0;
        for (int offset = 0; offset < all.size(); offset += EXTERNAL_STATS_BATCH_SIZE) {
            List<ExternalStatisticsCacheKey> batch =
                    all.subList(offset, Math.min(all.size(), offset + EXTERNAL_STATS_BATCH_SIZE));
            lanes.set(lane, lanes.get(lane).thenCompose(ignored ->
                    new ExternalStatisticsCacheLoader(externalPartitionStatsExecutor)
                            .asyncLoadAll(batch, statsCacheRefresherExecutor)).thenAccept(result::putAll));
            lane = (lane + 1) % lanes.size();
        }
        return CompletableFuture.allOf(lanes.toArray(new CompletableFuture<?>[0])).thenApply(ignored -> result);
    }

    private CompletableFuture<ExternalStatisticsAggregate> loadExternalPartitions(ExternalStatisticsRequest request) {
        int blockSize = Math.max(1, Math.min(EXTERNAL_STATS_BATCH_SIZE, Config.external_statistics_partition_block_size));
        if (blockSize <= 1 || request.partitions.size() <= 64) {
            return loadExternalPartitionsIndividually(request);
        }
        ExternalStatisticsAggregate.Builder aggregate = new ExternalStatisticsAggregate.Builder(request);
        Set<String> membership = Set.copyOf(request.partitions);
        Map<ExternalStatisticsCacheKey, Object> generations = new HashMap<>();
        List<ExternalStatisticsCacheKey> rawLoads = new ArrayList<>();
        List<ExternalStatisticsCacheKey> blockLoads = new ArrayList<>();
        List<ExternalStatisticsCacheKey> refreshBlocks = new ArrayList<>();
        int refreshMembers = 0;
        long now = System.nanoTime();
        for (String column : request.columns) {
            CompletableFuture<Optional<ExternalColumnStatistics>> tableSummary = externalStatisticsCache.getIfPresent(
                    ExternalStatisticsCacheKey.table(request.tableUUID, column));
            if (tableSummary != null && tableSummary.isDone() && !tableSummary.isCompletedExceptionally()
                    && tableSummary.join().isEmpty()) {
                // A successful table-wide read found no statistics for this column. Do not issue
                // thousands of known-empty partition lookups. ANALYZE invalidates this absence too.
                continue;
            }
            ExternalStatisticsCacheKey directoryKey = ExternalStatisticsCacheKey.directory(request.tableUUID, column);
            CompletableFuture<Optional<ExternalColumnStatistics>> directoryFuture = externalStatisticsCache.asMap()
                    .compute(directoryKey, (ignored, previous) -> {
                        ExternalPartitionStatisticsBlocks.Directory old = previous == null
                                ? new ExternalPartitionStatisticsBlocks.Directory()
                                : (ExternalPartitionStatisticsBlocks.Directory) previous.join().orElseThrow();
                        ExternalPartitionStatisticsBlocks.Directory refined = old.refine(request.partitions);
                        return previous != null && old == refined ? previous
                                : CompletableFuture.completedFuture(Optional.of(refined));
                    });
            ExternalPartitionStatisticsBlocks.Directory directory =
                    (ExternalPartitionStatisticsBlocks.Directory) directoryFuture.join().orElseThrow();
            generations.put(directoryKey, directory.generation);
            Set<String> covered = new java.util.HashSet<>();
            for (ExternalPartitionStatisticsBlocks.Block block : directory.covering(request.partitions, membership, now)) {
                aggregate.add(Map.of(block.key, Optional.of(block)));
                covered.addAll(block.key.partitions);
                if (block.needsRefresh(now) && refreshMembers + block.key.partitions.size() <= EXTERNAL_STATS_BATCH_SIZE) {
                    refreshBlocks.add(block.key);
                    refreshMembers += block.key.partitions.size();
                }
            }
            List<String> missing = new ArrayList<>();
            Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> warm = new LinkedHashMap<>();
            for (String partition : request.partitions) {
                if (covered.contains(partition)) {
                    continue;
                }
                ExternalStatisticsCacheKey key = new ExternalStatisticsCacheKey(request.tableUUID, partition, column);
                CompletableFuture<Optional<ExternalColumnStatistics>> cached = externalStatisticsCache.getIfPresent(key);
                if (cached != null && !cached.isCompletedExceptionally()) {
                    if (cached.isDone()) {
                        warm.put(key, cached.join());
                    } else {
                        // Reuse an already running per-partition load instead of issuing a block query for it.
                        rawLoads.add(key);
                    }
                } else {
                    missing.add(partition);
                }
            }
            // Compact existing singles once on a wide request. This reuses their HLLs without SQL;
            // later wide requests merge a few blocks instead of thousands of individual sketches.
            List<String> warmNames = warm.keySet().stream().map(key -> key.partitionName).toList();
            Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> compacted = new HashMap<>();
            for (List<String> names : partitionRuns(request.partitions, warmNames, blockSize)) {
                if (names.size() <= 8) {
                    for (String name : names) {
                        ExternalStatisticsCacheKey key = new ExternalStatisticsCacheKey(request.tableUUID, name, column);
                        aggregate.add(Map.of(key, warm.get(key)));
                    }
                } else {
                    ExternalStatisticsCacheKey key = ExternalStatisticsCacheKey.block(request.tableUUID, column, names);
                    long oldestAge = 0;
                    for (String name : names) {
                        ExternalStatisticsCacheKey cell = new ExternalStatisticsCacheKey(request.tableUUID, name, column);
                        long age = externalStatisticsCache.synchronous().policy().expireAfterWrite()
                                .flatMap(policy -> policy.ageOf(cell)).map(java.time.Duration::toNanos).orElse(0L);
                        oldestAge = Math.max(oldestAge, age);
                    }
                    ExternalPartitionStatisticsBlocks.Block block =
                            ExternalPartitionStatisticsBlocks.Block.merge(key, warm, now - oldestAge);
                    compacted.put(key, Optional.of(block));
                    aggregate.add(Map.of(key, Optional.of(block)));
                }
            }
            publishBlocks(generations, compacted);
            refreshExternalStatisticsBatch(new ArrayList<>(warm.keySet()));
            if (missing.size() <= 8) {
                missing.forEach(name -> rawLoads.add(new ExternalStatisticsCacheKey(request.tableUUID, name, column)));
            } else {
                for (List<String> names : partitionRuns(request.partitions, missing, blockSize)) {
                    blockLoads.add(ExternalStatisticsCacheKey.block(request.tableUUID, column, names));
                }
            }
        }
        if (!refreshBlocks.isEmpty() && externalStatisticsRefreshPermits.tryAcquire()) {
            loadBlockBatch(refreshBlocks, generations).whenComplete((ignored, error) -> {
                externalStatisticsRefreshPermits.release();
                if (error != null) {
                    LOG.debug("Failed to refresh external statistics blocks for {}", request.tableUUID, error);
                }
            });
        }
        List<CompletableFuture<Void>> lanes = new ArrayList<>(List.of(
                CompletableFuture.completedFuture(null), CompletableFuture.completedFuture(null)));
        int lane = 0;
        for (int offset = 0; offset < rawLoads.size(); offset += EXTERNAL_STATS_BATCH_SIZE) {
            schedulePartitionBatch(lanes, lane, rawLoads.subList(offset,
                    Math.min(rawLoads.size(), offset + EXTERNAL_STATS_BATCH_SIZE)), aggregate);
            lane = (lane + 1) % lanes.size();
        }
        List<ExternalStatisticsCacheKey> batch = new ArrayList<>();
        int members = 0;
        for (ExternalStatisticsCacheKey key : blockLoads) {
            if (members + key.partitions.size() > EXTERNAL_STATS_BATCH_SIZE && !batch.isEmpty()) {
                scheduleBlockBatch(lanes, lane, List.copyOf(batch), generations, aggregate);
                lane = (lane + 1) % lanes.size();
                batch.clear();
                members = 0;
            }
            batch.add(key);
            members += key.partitions.size();
        }
        if (!batch.isEmpty()) {
            scheduleBlockBatch(lanes, lane, List.copyOf(batch), generations, aggregate);
        }
        return CompletableFuture.allOf(lanes.toArray(new CompletableFuture<?>[0])).thenApply(ignored -> aggregate.build());
    }

    private static List<List<String>> partitionRuns(List<String> requested, List<String> selected, int blockSize) {
        Set<String> membership = Set.copyOf(selected);
        List<List<String>> runs = new ArrayList<>();
        List<String> run = new ArrayList<>();
        for (String partition : requested) {
            if (!membership.contains(partition)) {
                if (!run.isEmpty()) {
                    runs.add(List.copyOf(run));
                    run.clear();
                }
            } else {
                run.add(partition);
                if (run.size() == blockSize) {
                    runs.add(List.copyOf(run));
                    run.clear();
                }
            }
        }
        if (!run.isEmpty()) {
            runs.add(List.copyOf(run));
        }
        return runs;
    }

    private void scheduleBlockBatch(List<CompletableFuture<Void>> lanes, int lane,
            List<ExternalStatisticsCacheKey> batch, Map<ExternalStatisticsCacheKey, Object> generations,
            ExternalStatisticsAggregate.Builder aggregate) {
        lanes.set(lane, lanes.get(lane).thenCompose(ignored -> loadBlockBatch(batch, generations))
                .thenAccept(aggregate::add));
    }

    private CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>> loadBlockBatch(
            List<ExternalStatisticsCacheKey> batch, Map<ExternalStatisticsCacheKey, Object> generations) {
        Map<ExternalStatisticsCacheKey, CompletableFuture<Optional<ExternalColumnStatistics>>> requested = new HashMap<>();
        Map<BlockLoadKey, CompletableFuture<Optional<ExternalColumnStatistics>>> owned = new HashMap<>();
        Lock reservation = externalStatisticsLoadLocks.get(batch.get(0).tableUUID);
        reservation.lock();
        try {
            for (ExternalStatisticsCacheKey key : batch) {
                Object generation = generations.get(ExternalStatisticsCacheKey.directory(key.tableUUID, key.columnName));
                BlockLoadKey loadKey = new BlockLoadKey(key, generation);
                CompletableFuture<Optional<ExternalColumnStatistics>> future = new CompletableFuture<>();
                CompletableFuture<Optional<ExternalColumnStatistics>> previous = pendingBlocks.putIfAbsent(loadKey, future);
                requested.put(key, previous == null ? future : previous);
                if (previous == null) {
                    owned.put(loadKey, future);
                }
            }
        } finally {
            reservation.unlock();
        }
        if (!owned.isEmpty()) {
            CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>> load;
            try {
                load = new ExternalStatisticsCacheLoader(externalPartitionStatsExecutor).asyncLoadAll(
                        owned.keySet().stream().map(BlockLoadKey::key).toList(), externalPartitionStatsExecutor);
            } catch (RuntimeException e) {
                load = CompletableFuture.failedFuture(e);
            }
            load.whenComplete((values, error) -> {
                try {
                    if (error == null) {
                        publishBlocks(generations, values);
                    }
                    owned.forEach((key, future) -> {
                        pendingBlocks.remove(key, future);
                        if (error == null) {
                            future.complete(values.getOrDefault(key.key(), Optional.empty()));
                        } else {
                            future.completeExceptionally(error);
                        }
                    });
                } catch (Throwable failure) {
                    owned.values().forEach(future -> future.completeExceptionally(failure));
                } finally {
                    owned.forEach((key, future) -> pendingBlocks.remove(key, future));
                }
            });
        }
        return CompletableFuture.allOf(requested.values().toArray(new CompletableFuture<?>[0])).thenApply(ignored -> {
            Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> result = new HashMap<>();
            requested.forEach((key, future) -> result.put(key, future.join()));
            return result;
        });
    }

    private void publishBlocks(Map<ExternalStatisticsCacheKey, Object> generations,
            Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> values) {
        Map<ExternalStatisticsCacheKey, List<ExternalPartitionStatisticsBlocks.Block>> columns = new HashMap<>();
        values.forEach((key, value) -> value.ifPresent(stats -> {
            ExternalStatisticsCacheKey directory = ExternalStatisticsCacheKey.directory(key.tableUUID, key.columnName);
            columns.computeIfAbsent(directory, ignored -> new ArrayList<>())
                    .add((ExternalPartitionStatisticsBlocks.Block) stats);
        }));
        columns.forEach((key, blocks) -> externalStatisticsCache.asMap().computeIfPresent(key, (ignored, previous) -> {
            ExternalPartitionStatisticsBlocks.Directory directory =
                    (ExternalPartitionStatisticsBlocks.Directory) previous.join().orElseThrow();
            // ANALYZE/DROP/schema invalidation or eviction must not allow old loads to repopulate the cache.
            if (directory.generation != generations.get(key)) {
                return previous;
            }
            return CompletableFuture.completedFuture(Optional.of(directory.withBlocks(blocks, System.nanoTime())));
        }));
    }

    private CompletableFuture<ExternalStatisticsAggregate> loadExternalPartitionsIndividually(ExternalStatisticsRequest request) {
        ExternalStatisticsAggregate.Builder aggregate = new ExternalStatisticsAggregate.Builder(request);
        // Bound both transport batches and simultaneously retained load results despite cache eviction.
        // At most about 64 MiB of full HLL payload per batch; ordinary multi-column requests stay together.
        List<CompletableFuture<Void>> lanes = new ArrayList<>(List.of(
                CompletableFuture.completedFuture(null), CompletableFuture.completedFuture(null)));
        List<ExternalStatisticsCacheKey> batch = new ArrayList<>(EXTERNAL_STATS_BATCH_SIZE);
        int lane = 0;
        for (String column : request.columns) {
            for (String partition : request.partitions) {
                batch.add(new ExternalStatisticsCacheKey(request.tableUUID, partition, column));
                if (batch.size() == EXTERNAL_STATS_BATCH_SIZE) {
                    schedulePartitionBatch(lanes, lane, batch, aggregate);
                    lane = (lane + 1) % lanes.size();
                    batch = new ArrayList<>(EXTERNAL_STATS_BATCH_SIZE);
                }
            }
        }
        if (!batch.isEmpty()) {
            schedulePartitionBatch(lanes, lane, batch, aggregate);
        }
        return CompletableFuture.allOf(lanes.toArray(new CompletableFuture<?>[0])).thenApply(ignored -> aggregate.build());
    }

    private void schedulePartitionBatch(List<CompletableFuture<Void>> lanes, int lane,
            List<ExternalStatisticsCacheKey> batch, ExternalStatisticsAggregate.Builder aggregate) {
        lanes.set(lane, lanes.get(lane).thenCompose(ignored -> loadExternalPartitionBatch(batch))
                .thenAccept(loaded -> {
                    refreshExternalStatisticsBatch(batch);
                    aggregate.add(loaded);
                }));
    }

    private CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>>
            loadExternalPartitionBatch(List<ExternalStatisticsCacheKey> batch) {
        // Caffeine claims missing keys individually. Serialize only key reservation, not I/O or union.
        Lock lock = externalStatisticsLoadLocks.get(batch.get(0).tableUUID);
        lock.lock();
        try {
            return externalStatisticsCache.getAll(batch);
        } finally {
            lock.unlock();
        }
    }

    private static final class ExternalStatisticsRefresh {
        private final CompletableFuture<Optional<ExternalColumnStatistics>> previous;

        private ExternalStatisticsRefresh(CompletableFuture<Optional<ExternalColumnStatistics>> previous) {
            this.previous = previous;
        }
    }

    private void refreshExternalStatisticsBatch(List<ExternalStatisticsCacheKey> keys) {
        if (!Config.enable_statistic_cache_refresh_after_write || keys.isEmpty()) {
            return;
        }
        Map<ExternalStatisticsCacheKey, ExternalStatisticsRefresh> claimed = new HashMap<>();
        Lock lock = externalStatisticsLoadLocks.get(keys.get(0).tableUUID);
        lock.lock();
        try {
            for (ExternalStatisticsCacheKey key : keys) {
                CompletableFuture<Optional<ExternalColumnStatistics>> previous = externalStatisticsCache.asMap().get(key);
                if (previous == null || !previous.isDone() || previous.isCompletedExceptionally()) {
                    continue;
                }
                ExternalStatisticsRefresh existing = refreshingExternalStatistics.get(key);
                if (existing != null && existing.previous == previous) {
                    continue;
                }
                long age = externalStatisticsCache.synchronous().policy().expireAfterWrite()
                        .map(policy -> policy.ageOf(key, TimeUnit.SECONDS).orElse(-1)).orElse(-1L);
                if (age < Config.statistic_update_interval_sec) {
                    continue;
                }
                ExternalStatisticsRefresh refresh = new ExternalStatisticsRefresh(previous);
                refreshingExternalStatistics.put(key, refresh);
                claimed.put(key, refresh);
            }
        } finally {
            lock.unlock();
        }
        if (!claimed.isEmpty()) {
            scheduleExternalStatisticsRefresh(claimed);
        }
    }

    private void scheduleExternalStatisticsRefresh(Map<ExternalStatisticsCacheKey, ExternalStatisticsRefresh> refreshing) {
        if (!externalStatisticsRefreshPermits.tryAcquire()) {
            completeExternalStatisticsRefresh(refreshing, null,
                    new java.util.concurrent.RejectedExecutionException("Statistics refresh capacity is in use"), false);
            return;
        }
        try {
            CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>> load =
                    loadExternalStatisticsBatches(refreshing.keySet());
            load.whenComplete((loaded, error) ->
                    completeExternalStatisticsRefresh(refreshing, loaded, error, true));
        } catch (RuntimeException e) {
            completeExternalStatisticsRefresh(refreshing, null, e, true);
        }
    }

    private void completeExternalStatisticsRefresh(Map<ExternalStatisticsCacheKey, ExternalStatisticsRefresh> refreshing,
            Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> loaded, Throwable error, boolean releasePermit) {
        if (error != null) {
            LOG.warn("Failed to refresh external statistics batch", error);
        }
        refreshing.forEach((key, refresh) -> {
            if (error == null && loaded.containsKey(key)) {
                // Do not undo ANALYZE invalidation, eviction or a newer concurrent load.
                externalStatisticsCache.asMap().replace(key, refresh.previous,
                        CompletableFuture.completedFuture(loaded.get(key)));
            }
            refreshingExternalStatistics.remove(key, refresh);
        });
        if (releasePermit) {
            externalStatisticsRefreshPermits.release();
        }
    }

    private AsyncLoadingCache<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> createExternalStatisticsCache() {
        Caffeine<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> builder = Caffeine.newBuilder()
                .expireAfterWrite(Config.statistic_update_interval_sec * 2, TimeUnit.SECONDS)
                .maximumWeight(Config.external_statistics_cache_max_bytes)
                .recordStats()
                .weigher((ExternalStatisticsCacheKey key, Optional<ExternalColumnStatistics> value) -> {
                    long bytes = 192L + 2L * (key.tableUUID.length() + key.columnName.length() + key.partitionName.length())
                            + value.map(ExternalColumnStatistics::retainedBytes).orElse(0);
                    return (int) Math.min(Integer.MAX_VALUE, bytes);
                })
                .executor(statsCacheRefresherExecutor);
        // Automatic per-key refresh would generate SQL per cell. Both scopes use batched refresh above.
        return builder.buildAsync(new ExternalStatisticsCacheLoader(externalPartitionStatsExecutor));
    }

    @Override
    public void expireExternalPartitionStatistics(String tableUUID) {
        expireExternalPartitionStatistics(tableUUID, null);
    }

    private void expireExternalPartitionStatistics(String tableUUID, List<String> columns) {
        if (tableUUID == null || tableUUID.isEmpty()) {
            return;
        }
        Set<String> selected = columns == null ? null : Set.copyOf(columns);
        externalStatisticsCache.asMap().keySet().removeIf(key -> key.tableUUID.equals(tableUUID)
                && (selected == null || selected.contains(key.columnName)));
        refreshingExternalStatistics.keySet().removeIf(key -> key.tableUUID.equals(tableUUID)
                && (selected == null || selected.contains(key.columnName)));
    }

    @Override
    public void expireExternalMcvStatistics(String tableUUID) {
        if (tableUUID == null || tableUUID.isEmpty()) {
            return;
        }
        externalMcvStats.synchronous().invalidate(tableUUID);
    }

    @Override
    public void refreshExternalMcvStatistics(String tableUUID, boolean isSync) {
        if (tableUUID == null || tableUUID.isEmpty() || !StatisticUtils.checkStatisticTableStateNormal()) {
            return;
        }
        try {
            ExternalMcvStatsCacheLoader loader = new ExternalMcvStatsCacheLoader();
            CompletableFuture<Optional<ExternalMcvStatistics>> future =
                    loader.asyncLoad(tableUUID, statsCacheRefresherExecutor);
            if (isSync) {
                externalMcvStats.synchronous().put(tableUUID, future.get());
            } else {
                refreshCacheOnSuccess(future, result -> externalMcvStats.synchronous().put(tableUUID, result));
            }
        } catch (InterruptedException e) {
            LOG.warn("Failed to refresh external multi-column statistics", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("Failed to refresh external multi-column statistics", e);
        }
    }

    @Override
    public Map<String, StatisticsCacheMetrics> getCacheMetrics() {
        return Map.of("external_basic", StatisticsCacheMetrics.snapshot(
                        externalStatisticsCache.synchronous()),
                "external_mcv", StatisticsCacheMetrics.snapshot(externalMcvStats.synchronous()));
    }

    @Override
    public long estimateSize() {
        return Estimator.estimate(tableStatsCache.synchronous().asMap(), 20) +
                Estimator.estimate(columnStatistics.synchronous().asMap(), 20) +
                Estimator.estimate(partitionStatistics.synchronous().asMap(), 20) +
                Estimator.estimate(histogramCache.synchronous().asMap(), 20) +
                Estimator.estimate(connectorHistogramCache.synchronous().asMap(), 20) +
                Estimator.estimate(multiColumnStats.synchronous().asMap(), 20) +
                Estimator.estimate(externalMcvStats.synchronous().asMap(), 20) +
                externalStatisticsCache.synchronous().policy().eviction().map(e -> e.weightedSize().orElse(0)).orElse(0L);
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.<String, Long>builder()
                .put("TableStats", tableStatsCache.synchronous().estimatedSize())
                .put("ColumnStats", columnStatistics.synchronous().estimatedSize())
                .put("PartitionStats", partitionStatistics.synchronous().estimatedSize())
                .put("HistogramStats", histogramCache.synchronous().estimatedSize())
                .put("ConnectorHistogramStats", connectorHistogramCache.synchronous().estimatedSize())
                .put("MultiColumnCombinedStats", multiColumnStats.synchronous().estimatedSize())
                .put("ExternalMcvStats", externalMcvStats.synchronous().estimatedSize())
                .put("ExternalColumnStats", externalStatisticsCache.synchronous().estimatedSize())
                .build();
    }

    private <T> void refreshCacheOnSuccess(CompletableFuture<T> future, Consumer<T> updateCache) {
        future.whenComplete((result, error) -> {
            if (error != null) {
                // Preserve the last successful value; a failed read does not establish absence.
                LOG.warn("Failed to refresh statistics cache", error);
                return;
            }
            updateCache.accept(result);
        });
    }

    static AsyncLoadingCache<String, Optional<ExternalMcvStatistics>> createExternalMcvStatisticsCache(
            long maximumBytes, Executor executor, AsyncCacheLoader<String, Optional<ExternalMcvStatistics>> loader) {
        if (maximumBytes <= 0) {
            throw new IllegalArgumentException("MCV statistics cache byte limit must be positive");
        }
        Caffeine<String, Optional<ExternalMcvStatistics>> builder = Caffeine.newBuilder()
                .expireAfterWrite(Config.statistic_update_interval_sec * 2, TimeUnit.SECONDS)
                .maximumWeight(maximumBytes)
                .recordStats()
                .weigher((String key, Optional<ExternalMcvStatistics> value) -> externalMcvCacheWeight(key, value))
                .executor(executor);
        if (Config.enable_statistic_cache_refresh_after_write) {
            builder.refreshAfterWrite(Config.statistic_update_interval_sec, TimeUnit.SECONDS);
        }
        return builder.buildAsync(loader);
    }

    static int externalMcvCacheWeight(String key, Optional<ExternalMcvStatistics> value) {
        long bytes = 192L + 2L * key.length() + value.map(ExternalMcvStatistics::retainedBytes).orElse(0L);
        return (int) Math.min(Integer.MAX_VALUE, bytes);
    }

    private <K, V> AsyncLoadingCache<K, V> createAsyncLoadingCache(AsyncCacheLoader<K, V> cacheLoader) {
        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder()
                .expireAfterWrite(Config.statistic_update_interval_sec * 2, TimeUnit.SECONDS)
                .maximumSize(Config.statistic_cache_columns)
                .executor(statsCacheRefresherExecutor);
        
        // Only enable refreshAfterWrite if the config is enabled
        if (Config.enable_statistic_cache_refresh_after_write) {
            cacheBuilder.refreshAfterWrite(Config.statistic_update_interval_sec, TimeUnit.SECONDS);
        }
        
        return cacheBuilder.buildAsync(cacheLoader);
    }
}
