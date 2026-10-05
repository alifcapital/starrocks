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

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.io.Writable;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatisticsType;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

public class AnalyzeMgr implements Writable {
    private final JoinStatisticsRegistry joinStatisticsRegistry = new JoinStatisticsRegistry((meta, drop, apply) ->
            GlobalStateMgr.getCurrentState().getEditLog().logJoinStatistics(meta, drop, wal -> apply.run()));
    private volatile JoinStatisticsManager joinStatisticsManager;

    public JoinStatisticsRegistry getJoinStatisticsRegistry() {
        return joinStatisticsRegistry;
    }

    public void replayJoinStatistics(JoinStatisticsMeta meta, boolean drop) {
        joinStatisticsRegistry.replay(meta, drop);
        JoinStatisticsManager manager = joinStatisticsManager;
        if (manager != null) {
            manager.invalidateCache(meta.getId());
        }
    }

    public synchronized JoinStatisticsManager getJoinStatisticsManager() {
        if (joinStatisticsManager == null) {
            joinStatisticsManager = new JoinStatisticsManager(joinStatisticsRegistry);
            com.starrocks.memory.MemoryUsageTracker.registerMemoryTracker("Statistics", joinStatisticsManager);
        }
        return joinStatisticsManager;
    }

    public void revokeJoinStatisticsCollections() {
        JoinStatisticsManager manager = joinStatisticsManager;
        if (manager != null) {
            manager.revokeCollections();
        }
    }
    private static final Logger LOG = LogManager.getLogger(AnalyzeMgr.class);
    public static final String USER_CANCEL_MESSAGE = "kill analyze";
    private static final Pair<Long, Long> CHECK_ALL_TABLES =
            new Pair<>(StatsConstants.DEFAULT_ALL_ID, StatsConstants.DEFAULT_ALL_ID);
    public static final String IS_MULTI_COLUMN_STATS = "is_multi_column_stats";

    private final Map<Long, AnalyzeJob> analyzeJobMap;
    private final Map<Long, AnalyzeStatus> analyzeStatusMap;
    private final Map<Long, BasicStatsMeta> basicStatsMetaMap;
    private final Map<StatsMetaKey, ExternalBasicStatsMeta> externalBasicStatsMetaMap;
    private final Map<Pair<Long, String>, HistogramStatsMeta> histogramStatsMetaMap;
    private final Map<StatsMetaColumnKey, ExternalHistogramStatsMeta> externalHistogramStatsMetaMap;
    private final Map<MultiColumnStatsKey, MultiColumnStatsMeta> multiColumnStatsMetaMap;
    private final Map<ExternalMcvStatsKey, ExternalMcvStatsMeta> externalMcvStatsMetaMap;
    // The two indexes below are updated together with externalHistogramStatsMetaMap and
    // multiColumnStatsMetaMap. Query planning asks "does this table have any" for every scan, and
    // the answer is almost always no, so it must not scan or build a key per column.
    private final Map<StatsMetaKey, Set<StatsMetaColumnKey>> externalHistogramTables =
            new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<Long, Set<MultiColumnStatsKey>> multiColumnTables =
            new java.util.concurrent.ConcurrentHashMap<>();
    // Table-level lookup avoids scanning every collected column group during query planning.
    private final Map<StatsMetaKey, Map<ExternalMcvStatsKey, ExternalMcvStatsMeta>> externalMcvTables =
            new java.util.concurrent.ConcurrentHashMap<>();

    // ConnectContext of all currently running analyze tasks
    private final Map<Long, ConnectContext> connectionMap = Maps.newConcurrentMap();
    // Cancellation markers for analyze tasks. Used to make KILL ANALYZE deterministic even if it misses the SQL window.
    private final Set<Long> cancelledAnalyzeIds = java.util.concurrent.ConcurrentHashMap.newKeySet();
    // only first load of table will trigger analyze, so we don't need limit thread pool queue size
    private static final ExecutorService ANALYZE_TASK_THREAD_POOL = ThreadPoolManager.newDaemonFixedThreadPool(
            Config.statistic_analyze_task_pool_size, Integer.MAX_VALUE,
            "analyze-task-concurrency-pool", true);

    private final Set<Long> dropPartitionIds = new ConcurrentSkipListSet<>();
    private final List<Pair<Long, Long>> checkTableIds = Lists.newArrayList(CHECK_ALL_TABLES);

    private LocalDateTime lastCleanTime;

    public AnalyzeMgr() {
        analyzeJobMap = Maps.newConcurrentMap();
        analyzeStatusMap = Maps.newConcurrentMap();
        basicStatsMetaMap = Maps.newConcurrentMap();
        externalBasicStatsMetaMap = Maps.newConcurrentMap();
        histogramStatsMetaMap = Maps.newConcurrentMap();
        externalHistogramStatsMetaMap = Maps.newConcurrentMap();
        multiColumnStatsMetaMap = Maps.newConcurrentMap();
        externalMcvStatsMetaMap = Maps.newConcurrentMap();
    }

    public AnalyzeJob getAnalyzeJob(long id) {
        return analyzeJobMap.get(id);
    }

    public AnalyzeStatus getAnalyzeStatus(long id) {
        return analyzeStatusMap.get(id);
    }

    public synchronized void addAnalyzeJob(AnalyzeJob job) throws AlreadyExistsException {
        for (AnalyzeJob analyzeJob : analyzeJobMap.values()) {
            if (job instanceof ExternalAnalyzeJob extended && extended.isExtendedStatistics()
                    && analyzeJob instanceof ExternalAnalyzeJob existing && existing.isExtendedStatistics()) {
                if (ExtendedStatisticsSchedule.sameTarget(extended, existing)) {
                    throw new AlreadyExistsException("A schedule already exists for this statistics target; drop it first");
                }
                continue;
            }
            try {
                if (analyzeJob.getCatalogName().equals(job.getCatalogName()) &&
                        analyzeJob.getDbName().equals(job.getDbName()) &&
                        analyzeJob.getColumns().stream().sorted().collect(Collectors.toList())
                        .equals(job.getColumns().stream().sorted().collect(Collectors.toList())) &&
                        analyzeJob.getTableName().equals(job.getTableName()) &&
                        analyzeJob.getAnalyzeType().equals(job.getAnalyzeType()) &&
                        analyzeJob.getScheduleType().equals(job.getScheduleType()) &&
                        analyzeJob.getProperties().equals(job.getProperties())) {
                    throw new AlreadyExistsException("AnalyzeJob Already Exists");
                }
            } catch (MetaNotFoundException e) {
                LOG.warn("add analyze job failed", e);
            }
        }

        long id = GlobalStateMgr.getCurrentState().getNextId();
        job.setId(id);
        GlobalStateMgr.getCurrentState().getEditLog().logAddAnalyzeJob(job,
                wal -> analyzeJobMap.put(id, job));
    }

    public synchronized void updateAnalyzeJobWithoutLog(AnalyzeJob job) {
        if (!(job instanceof ExternalAnalyzeJob external && external.isExtendedStatistics())
                || analyzeJobMap.containsKey(job.getId())) {
            analyzeJobMap.put(job.getId(), job);
        }
    }

    public synchronized void updateAnalyzeJobWithLog(AnalyzeJob job) {
        if (job instanceof ExternalAnalyzeJob external && external.isExtendedStatistics()
                && !analyzeJobMap.containsKey(job.getId())) {
            return;
        }
        GlobalStateMgr.getCurrentState().getEditLog().logAddAnalyzeJob(job,
                wal -> analyzeJobMap.put(job.getId(), job));
    }

    public synchronized void removeAnalyzeJob(long id) {
        if (id == -1) {
            List<Long> keysToRemove = new ArrayList<>(analyzeJobMap.keySet());
            for (Long key : keysToRemove) {
                AnalyzeJob job = analyzeJobMap.get(key);
                GlobalStateMgr.getCurrentState().getEditLog()
                        .logRemoveAnalyzeJob(job, wal -> analyzeJobMap.remove(key));
            }
        }
        if (analyzeJobMap.containsKey(id)) {
            AnalyzeJob job = analyzeJobMap.get(id);
            GlobalStateMgr.getCurrentState().getEditLog()
                    .logRemoveAnalyzeJob(job, wal -> analyzeJobMap.remove(id));
        }
    }

    public void removeJoinAnalyzeJobs(long objectId) {
        for (ExternalAnalyzeJob job : getAllExternalAnalyzeJobList()) {
            if (job.getAnalyzeType() == StatsConstants.AnalyzeType.JOIN && job.getJoinStatisticsId() == objectId) {
                removeAnalyzeJob(job.getId());
            }
        }
    }

    public void removeMcvAnalyzeJobs(String catalog, String db, String table, List<String> columns) {
        for (ExternalAnalyzeJob job : getAllExternalAnalyzeJobList()) {
            if (job.getAnalyzeType() == StatsConstants.AnalyzeType.MCV && job.getCatalogName().equals(catalog)
                    && job.getDbName().equals(db) && job.getTableName().equals(table)
                    && (columns == null || new java.util.HashSet<>(columns).equals(new java.util.HashSet<>(job.getColumns())))) {
                removeAnalyzeJob(job.getId());
            }
        }
    }

    public List<AnalyzeJob> getAllAnalyzeJobList() {
        return Lists.newLinkedList(analyzeJobMap.values());
    }

    public List<NativeAnalyzeJob> getAllNativeAnalyzeJobList() {
        return analyzeJobMap.values().stream().filter(AnalyzeJob::isNative).map(job -> (NativeAnalyzeJob) job).
                collect(Collectors.toList());
    }

    public List<ExternalAnalyzeJob> getAllExternalAnalyzeJobList() {
        return analyzeJobMap.values().stream().filter(job -> !job.isNative()).map(job -> (ExternalAnalyzeJob) job).
                collect(Collectors.toList());
    }

    public void replayAddAnalyzeJob(AnalyzeJob job) {
        analyzeJobMap.put(job.getId(), job);
    }

    public void replayRemoveAnalyzeJob(AnalyzeJob job) {
        analyzeJobMap.remove(job.getId());
    }

    public void addAnalyzeStatus(AnalyzeStatus status) {
        GlobalStateMgr.getCurrentState().getEditLog()
                .logAddAnalyzeStatus(status, wal -> analyzeStatusMap.put(status.getId(), status));
    }

    public void replayAddAnalyzeStatus(AnalyzeStatus status) {
        analyzeStatusMap.put(status.getId(), status);
    }

    public void replayRemoveAnalyzeStatus(AnalyzeStatus status) {
        analyzeStatusMap.remove(status.getId());
    }

    public Map<Long, AnalyzeStatus> getAnalyzeStatusMap() {
        return analyzeStatusMap;
    }

    public void clearExpiredAnalyzeStatus() {
        List<AnalyzeStatus> expireList = Lists.newArrayList();
        for (AnalyzeStatus analyzeStatus : analyzeStatusMap.values()) {
            LocalDateTime now = LocalDateTime.now();
            if (analyzeStatus.getStartTime().plusSeconds(Config.statistic_analyze_status_keep_second).isBefore(now)) {
                expireList.add(analyzeStatus);
            }
        }
        for (AnalyzeStatus status : expireList) {
            GlobalStateMgr.getCurrentState().getEditLog()
                    .logRemoveAnalyzeStatus(status, wal -> analyzeStatusMap.remove(status.getId()));
        }
    }

    public void dropAnalyzeStatus(Long tableId) {
        List<AnalyzeStatus> expireList = Lists.newArrayList();
        for (AnalyzeStatus analyzeStatus : analyzeStatusMap.values()) {
            if (analyzeStatus.isNative() &&
                    ((NativeAnalyzeStatus) analyzeStatus).getTableId() == tableId) {
                expireList.add(analyzeStatus);
            }
        }
        for (AnalyzeStatus status : expireList) {
            GlobalStateMgr.getCurrentState().getEditLog()
                    .logRemoveAnalyzeStatus(status, wal -> analyzeStatusMap.remove(status.getId()));
        }
    }

    public void dropExternalAnalyzeStatus(String tableUUID) {
        List<AnalyzeStatus> expireList = analyzeStatusMap.values().stream().
                filter(status -> status instanceof ExternalAnalyzeStatus).
                filter(status -> ((ExternalAnalyzeStatus) status).getTableUUID().equals(tableUUID)).
                toList();

        for (AnalyzeStatus status : expireList) {
            GlobalStateMgr.getCurrentState().getEditLog()
                    .logRemoveAnalyzeStatus(status, wal -> analyzeStatusMap.remove(status.getId()));
        }
    }

    public void dropExternalBasicStatsData(String tableUUID) {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        statisticExecutor.dropExternalTableStatistics(StatisticUtils.buildConnectContext(), tableUUID);
    }

    public void dropExternalBasicStatsData(String catalogName, String dbName, String tableName) {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        statisticExecutor.dropExternalTableStatistics(StatisticUtils.buildConnectContext(), catalogName, dbName, tableName);
    }

    public void dropAnalyzeJob(String catalogName, String dbName, String tblName) {
        List<AnalyzeJob> expireList = Lists.newArrayList();
        try {
            for (AnalyzeJob analyzeJob : analyzeJobMap.values()) {
                if (analyzeJob.getCatalogName().equals(catalogName) &&
                        analyzeJob.getDbName().equals(dbName) &&
                        analyzeJob.getTableName().equals(tblName)) {
                    expireList.add(analyzeJob);
                }
            }
        } catch (MetaNotFoundException e) {
            LOG.warn("drop analyze job failed", e);
        }
        for (AnalyzeJob job : expireList) {
            GlobalStateMgr.getCurrentState().getEditLog()
                    .logRemoveAnalyzeJob(job, wal -> analyzeJobMap.remove(job.getId()));
        }
    }

    public void addBasicStatsMeta(BasicStatsMeta basicStatsMeta) {
        GlobalStateMgr.getCurrentState().getEditLog().logAddBasicStatsMeta(
                basicStatsMeta, wal -> basicStatsMetaMap.put(basicStatsMeta.getTableId(), basicStatsMeta));
    }

    public void replayAddBasicStatsMeta(BasicStatsMeta basicStatsMeta) {
        basicStatsMetaMap.put(basicStatsMeta.getTableId(), basicStatsMeta);
    }

    public void addExternalBasicStatsMeta(ExternalBasicStatsMeta basicStatsMeta) {
        GlobalStateMgr.getCurrentState().getEditLog().logAddExternalBasicStatsMeta(basicStatsMeta,
                wal -> externalBasicStatsMetaMap.put(new StatsMetaKey(basicStatsMeta.getCatalogName(),
                        basicStatsMeta.getDbName(),
                        basicStatsMeta.getTableName()),
                        basicStatsMeta));
    }

    public void replayAddExternalBasicStatsMeta(ExternalBasicStatsMeta basicStatsMeta) {
        externalBasicStatsMetaMap.put(new StatsMetaKey(basicStatsMeta.getCatalogName(),
                basicStatsMeta.getDbName(), basicStatsMeta.getTableName()), basicStatsMeta);
    }

    public void removeExternalBasicStatsMeta(String catalogName, String dbName, String tableName) {
        StatsMetaKey key = new StatsMetaKey(catalogName, dbName, tableName);
        if (externalBasicStatsMetaMap.containsKey(key)) {
            ExternalBasicStatsMeta basicStatsMeta = externalBasicStatsMetaMap.get(key);
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveExternalBasicStatsMeta(
                    basicStatsMeta, wal -> externalBasicStatsMetaMap.remove(key));
        }
    }

    public void removeExternalHistogramStatsMeta(String catalogName, String dbName, String tableName, List<String> columns) {
        for (String column : columns) {
            StatsMetaColumnKey histogramKey = new StatsMetaColumnKey(catalogName, dbName, tableName, column);
            if (externalHistogramStatsMetaMap.containsKey(histogramKey)) {
                GlobalStateMgr.getCurrentState().getEditLog().logRemoveExternalHistogramStatsMeta(
                        externalHistogramStatsMetaMap.get(histogramKey),
                        wal -> removeExternalHistogramStatsMetaKey(histogramKey));
            }
        }
    }

    public void replayRemoveExternalBasicStatsMeta(ExternalBasicStatsMeta basicStatsMeta) {
        externalBasicStatsMetaMap.remove(new StatsMetaKey(basicStatsMeta.getCatalogName(),
                basicStatsMeta.getDbName(), basicStatsMeta.getTableName()));
    }

    public void addMultiColumnStatsMeta(MultiColumnStatsMeta meta) {
        GlobalStateMgr.getCurrentState().getEditLog().logAddMultiColumnStatsMeta(meta, wal -> {
            putMultiColumnStatsMeta(
                    new MultiColumnStatsKey(meta.getTableId(), meta.getColumnIds(), meta.getStatsTypes()), meta);
        });
    }

    public void replayAddMultiColumnStatsMeta(MultiColumnStatsMeta meta) {
        putMultiColumnStatsMeta(
                new MultiColumnStatsKey(meta.getTableId(), meta.getColumnIds(), meta.getStatsTypes()), meta);
    }

    public void replayRemoveMultiColumnStatsMeta(MultiColumnStatsMeta meta) {
        removeMultiColumnStatsMetaKey(
                new MultiColumnStatsKey(meta.getTableId(), meta.getColumnIds(), meta.getStatsTypes()));
    }

    private synchronized void putMultiColumnStatsMeta(MultiColumnStatsKey key, MultiColumnStatsMeta meta) {
        multiColumnStatsMetaMap.put(key, meta);
        multiColumnTables.computeIfAbsent(key.getTableId(), ignored -> java.util.concurrent.ConcurrentHashMap.newKeySet())
                .add(key);
    }

    private synchronized void removeMultiColumnStatsMetaKey(MultiColumnStatsKey key) {
        multiColumnStatsMetaMap.remove(key);
        Set<MultiColumnStatsKey> keys = multiColumnTables.get(key.getTableId());
        if (keys != null) {
            keys.remove(key);
            if (keys.isEmpty()) {
                multiColumnTables.remove(key.getTableId());
            }
        }
    }

    /** True when some multi-column statistics were collected for the table. Does not allocate. */
    public boolean hasMultiColumnStatsMeta(Long tableId) {
        return multiColumnTables.containsKey(tableId);
    }

    public Map<MultiColumnStatsKey, MultiColumnStatsMeta> getMultiColumnStatsMetaMap() {
        return multiColumnStatsMetaMap;
    }

    public Map<ExternalMcvStatsKey, ExternalMcvStatsMeta> getExternalMcvStatsMetaMap() {
        return externalMcvStatsMetaMap;
    }

    public void addExternalMcvStatsMeta(ExternalMcvStatsMeta meta) {
        GlobalStateMgr.getCurrentState().getEditLog().logAddExternalMcvStatsMeta(meta,
                wal -> replayAddExternalMcvStatsMeta(meta));
    }

    public synchronized void replayAddExternalMcvStatsMeta(ExternalMcvStatsMeta meta) {
        meta.validate();
        ExternalMcvStatsKey key = ExternalMcvStatsKey.of(meta);
        externalMcvStatsMetaMap.put(key, meta);
        externalMcvTables.computeIfAbsent(key.getTableKey(), ignored -> new java.util.concurrent.ConcurrentHashMap<>())
                .put(key, meta);
    }

    public synchronized void replayRemoveExternalMcvStatsMeta(ExternalMcvStatsMeta meta) {
        ExternalMcvStatsKey key = ExternalMcvStatsKey.of(meta);
        externalMcvStatsMetaMap.remove(key);
        Map<ExternalMcvStatsKey, ExternalMcvStatsMeta> groups = externalMcvTables.get(key.getTableKey());
        if (groups != null) {
            groups.remove(key);
            if (groups.isEmpty()) {
                externalMcvTables.remove(key.getTableKey());
            }
        }
    }

    public void removeExternalMcvStatsMeta(String catalogName, String dbName, String tableName) {
        StatsMetaKey tableKey = new StatsMetaKey(catalogName, dbName, tableName);
        for (Map.Entry<ExternalMcvStatsKey, ExternalMcvStatsMeta> entry :
                Lists.newArrayList(externalMcvStatsMetaMap.entrySet())) {
            if (entry.getKey().getTableKey().equals(tableKey)) {
                GlobalStateMgr.getCurrentState().getEditLog().logRemoveExternalMcvStatsMeta(entry.getValue(),
                        wal -> replayRemoveExternalMcvStatsMeta(entry.getValue()));
            }
        }
    }

    public boolean hasExternalMcvStatsMeta(Table table) {
        if (externalMcvTables.isEmpty() || (!table.isHiveTable() && !table.isIcebergTable())) {
            return false;
        }
        Map<ExternalMcvStatsKey, ExternalMcvStatsMeta> groups = externalMcvTables.get(
                new StatsMetaKey(table.getCatalogName(), table.getCatalogDBName(), table.getName()));
        if (groups == null) {
            return false;
        }
        String uuid = table.getUUID();
        return groups.values().stream().anyMatch(meta -> meta.getTableUUID() == null
                || meta.getTableUUID().isEmpty() || meta.getTableUUID().equals(uuid));
    }

    public void refreshExternalMcvStatisticsCache(String tableUUID, boolean isSync) {
        GlobalStateMgr.getCurrentState().getStatisticStorage().refreshExternalMcvStatistics(tableUUID, isSync);
    }

    /**
     * Replay handler for the external multi-column stats meta journals on followers: the cached copy is
     * stale either way, so it is dropped and reloaded lazily. Journals written without a table UUID fall
     * back to resolving the table.
     */
    public void replayExpireExternalMcvStatsCache(ExternalMcvStatsMeta meta) {
        String tableUUID = meta.getTableUUID();
        if (tableUUID == null || tableUUID.isEmpty()) {
            try {
                Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                        .getTable(new ConnectContext(), meta.getCatalogName(), meta.getDbName(), meta.getTableName());
                if (table == null) {
                    return;
                }
                tableUUID = table.getUUID();
            } catch (Exception e) {
                LOG.warn("Failed to resolve table {}.{}.{} to expire its multi-column statistics cache",
                        meta.getCatalogName(), meta.getDbName(), meta.getTableName(), e);
                return;
            }
        }
        GlobalStateMgr.getCurrentState().getStatisticStorage().expireExternalMcvStatistics(tableUUID);
    }

    public void dropExternalMcvStatsMetaAndData(String catalogName, String dbName, String tableName) {
        removeMcvAnalyzeJobs(catalogName, dbName, tableName, null);
        new StatisticExecutor().dropExternalMcvStatistics(StatisticUtils.buildConnectContext(), catalogName,
                dbName, tableName);
        removeExternalMcvStatsMeta(catalogName, dbName, tableName);
    }

    public void dropExternalMcvStatsMetaAndData(ConnectContext statsConnectCtx, TableName tableName,
                                                        Table table) {
        removeMcvAnalyzeJobs(tableName.getCatalog(), tableName.getDb(), tableName.getTbl(), null);
        var lock = ExtendedStatisticsSchedule.mcvLock(table.getUUID());
        lock.lock();
        try {
            new StatisticExecutor().dropExternalMcvStatistics(statsConnectCtx, table.getUUID());
            removeExternalMcvStatsMeta(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
        } finally {
            lock.unlock();
        }
    }

    public void dropExternalMcvStatsMetaAndData(ConnectContext statsConnectCtx, TableName tableName,
                                              Table table, List<String> columnNames) {
        removeMcvAnalyzeJobs(tableName.getCatalog(), tableName.getDb(), tableName.getTbl(), columnNames);
        var lock = ExtendedStatisticsSchedule.mcvLock(table.getUUID());
        lock.lock();
        try {
            new StatisticExecutor().dropExternalMcvStatistics(statsConnectCtx, table.getUUID(), columnNames);
            ExternalMcvStatsKey key = new ExternalMcvStatsKey(tableName.getCatalog(), tableName.getDb(),
                    tableName.getTbl(), columnNames);
            ExternalMcvStatsMeta meta = externalMcvStatsMetaMap.get(key);
            if (meta != null) {
                GlobalStateMgr.getCurrentState().getEditLog().logRemoveExternalMcvStatsMeta(meta,
                        wal -> replayRemoveExternalMcvStatsMeta(meta));
            }
        } finally {
            lock.unlock();
        }
    }

    public void refreshBasicStatisticsCache(Long dbId, Long tableId, List<String> columns, boolean async) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) {
            return;
        }

        if (async) {
            GlobalStateMgr.getCurrentState().getStatisticStorage().refreshTableStatistic(table, false);
            GlobalStateMgr.getCurrentState().getStatisticStorage().refreshColumnStatistics(table, columns, false);
        } else {
            GlobalStateMgr.getCurrentState().getStatisticStorage().refreshTableStatistic(table, true);
            GlobalStateMgr.getCurrentState().getStatisticStorage().refreshColumnStatistics(table, columns, true);
        }
    }

    public void expireTableAndColumnStatistics(Long dbId, Long tableId, List<String> columns) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) {
            return;
        }
        GlobalStateMgr.getCurrentState().getStatisticStorage().expireTableAndColumnStatistics(table, columns);
    }

    public void expireConnectorTableAndColumnStatistics(String catalogName, String dbName,
                                                        String tableName, List<String> columns) {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(new ConnectContext(), catalogName, dbName, tableName);
        if (table == null) {
            return;
        }

        GlobalStateMgr.getCurrentState().getStatisticStorage().expireConnectorTableColumnStatistics(table, columns);
    }

    public void refreshConnectorTableBasicStatisticsCache(String catalogName, String dbName, String tableName,
                                                          List<String> columns, boolean async) {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(new ConnectContext(), catalogName, dbName, tableName);
        if (table == null) {
            return;
        }
        GlobalStateMgr.getCurrentState().getStatisticStorage()
                .refreshConnectorTableColumnStatistics(table, columns, !async);
    }

    public void refreshMultiColumnStatisticsCache(long tableId, boolean isSync) {
        GlobalStateMgr.getCurrentState().getStatisticStorage().refreshMultiColumnStatistics(tableId, isSync);
    }

    public void replayRemoveBasicStatsMeta(BasicStatsMeta basicStatsMeta) {
        basicStatsMetaMap.remove(basicStatsMeta.getTableId());
    }

    public BasicStatsMeta getTableBasicStatsMeta(long tableId) {
        return basicStatsMetaMap.get(tableId);
    }

    public Map<Long, BasicStatsMeta> getBasicStatsMetaMap() {
        return basicStatsMetaMap;
    }

    public long getExistUpdateRows(Long tableId) {
        BasicStatsMeta existInfo =  basicStatsMetaMap.get(tableId);
        return existInfo == null ? 0 : existInfo.getTotalRows();
    }

    public Map<StatsMetaKey, ExternalBasicStatsMeta> getExternalBasicStatsMetaMap() {
        return externalBasicStatsMetaMap;
    }

    public ExternalBasicStatsMeta getExternalTableBasicStatsMeta(String catalogName, String dbName, String tableName) {
        return externalBasicStatsMetaMap.get(new StatsMetaKey(catalogName, dbName, tableName));
    }

    public List<HistogramStatsMeta> getHistogramMetaByTable(long tableId) {
        return histogramStatsMetaMap.entrySet().stream()
                .filter(x -> x.getKey().first == tableId)
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    public void addHistogramStatsMeta(HistogramStatsMeta histogramStatsMeta) {
        GlobalStateMgr.getCurrentState().getEditLog().logAddHistogramStatsMeta(histogramStatsMeta, wal -> {
            histogramStatsMetaMap.put(
                    new Pair<>(histogramStatsMeta.getTableId(), histogramStatsMeta.getColumn()), histogramStatsMeta);

        });
    }

    public void replayAddHistogramStatsMeta(HistogramStatsMeta histogramStatsMeta) {
        histogramStatsMetaMap.put(
                new Pair<>(histogramStatsMeta.getTableId(), histogramStatsMeta.getColumn()), histogramStatsMeta);
    }

    public void refreshHistogramStatisticsCache(Long dbId, Long tableId, List<String> columns, boolean async) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (null == db) {
            return;
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (null == table) {
            return;
        }

        GlobalStateMgr.getCurrentState().getStatisticStorage().refreshHistogramStatistics(table, columns, !async);
    }

    public void replayRemoveHistogramStatsMeta(HistogramStatsMeta histogramStatsMeta) {
        histogramStatsMetaMap.remove(new Pair<>(histogramStatsMeta.getTableId(), histogramStatsMeta.getColumn()));
    }

    public Map<Pair<Long, String>, HistogramStatsMeta> getHistogramStatsMetaMap() {
        return histogramStatsMetaMap;
    }

    public Map<StatsMetaColumnKey, ExternalHistogramStatsMeta> getExternalHistogramStatsMetaMap() {
        return externalHistogramStatsMetaMap;
    }

    public void addExternalHistogramStatsMeta(ExternalHistogramStatsMeta histogramStatsMeta) {
        GlobalStateMgr.getCurrentState().getEditLog().logAddExternalHistogramStatsMeta(histogramStatsMeta, wal -> {
            putExternalHistogramStatsMeta(histogramStatsMeta);
        });
    }

    public void replayAddExternalHistogramStatsMeta(ExternalHistogramStatsMeta histogramStatsMeta) {
        putExternalHistogramStatsMeta(histogramStatsMeta);
    }

    public void replayRemoveExternalHistogramStatsMeta(ExternalHistogramStatsMeta histogramStatsMeta) {
        removeExternalHistogramStatsMetaKey(new StatsMetaColumnKey(histogramStatsMeta.getCatalogName(),
                histogramStatsMeta.getDbName(), histogramStatsMeta.getTableName(), histogramStatsMeta.getColumn()));
    }

    private synchronized void putExternalHistogramStatsMeta(ExternalHistogramStatsMeta histogramStatsMeta) {
        StatsMetaColumnKey key = new StatsMetaColumnKey(histogramStatsMeta.getCatalogName(),
                histogramStatsMeta.getDbName(), histogramStatsMeta.getTableName(), histogramStatsMeta.getColumn());
        externalHistogramStatsMetaMap.put(key, histogramStatsMeta);
        externalHistogramTables.computeIfAbsent(lowerCaseTableKey(key.getTableKey()),
                ignored -> java.util.concurrent.ConcurrentHashMap.newKeySet()).add(key);
    }

    private synchronized void removeExternalHistogramStatsMetaKey(StatsMetaColumnKey key) {
        externalHistogramStatsMetaMap.remove(key);
        StatsMetaKey tableKey = lowerCaseTableKey(key.getTableKey());
        Set<StatsMetaColumnKey> columns = externalHistogramTables.get(tableKey);
        if (columns != null) {
            columns.remove(key);
            if (columns.isEmpty()) {
                externalHistogramTables.remove(tableKey);
            }
        }
    }

    // The meta is written with the names of the ANALYZE target and read with the names the table reports.
    // We only use the index to decide that a table has no histogram, so we compare names without case:
    // a name that differs only in case must not hide existing histograms.
    private static StatsMetaKey lowerCaseTableKey(StatsMetaKey key) {
        return lowerCaseTableKey(key.getCatalogName(), key.getDbName(), key.getTableName());
    }

    private static StatsMetaKey lowerCaseTableKey(String catalogName, String dbName, String tableName) {
        return new StatsMetaKey(lower(catalogName), lower(dbName), lower(tableName));
    }

    private static String lower(String name) {
        return name == null ? null : name.toLowerCase(java.util.Locale.ROOT);
    }

    /**
     * True when ANALYZE HISTOGRAM was run on some column of the external table. The key is the one the
     * histogram meta is written with: catalog, database and table name, compared without case. Tables of
     * types that cannot be analyzed never have histogram meta.
     */
    public boolean hasExternalHistogramStatsMeta(Table table) {
        if (externalHistogramTables.isEmpty() || !table.isAnalyzableExternalTable()) {
            return false;
        }
        return externalHistogramTables.containsKey(
                lowerCaseTableKey(table.getCatalogName(), table.getCatalogDBName(), table.getName()));
    }

    public void refreshConnectorTableHistogramStatisticsCache(String catalogName, String dbName, String tableName,
                                                              List<String> columns, boolean async) {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(new ConnectContext(), catalogName, dbName, tableName);
        if (table == null) {
            return;
        }

        GlobalStateMgr.getCurrentState().getStatisticStorage().expireConnectorHistogramStatistics(table, columns);
        if (async) {
            GlobalStateMgr.getCurrentState().getStatisticStorage().getConnectorHistogramStatistics(table, columns);
        } else {
            GlobalStateMgr.getCurrentState().getStatisticStorage().getConnectorHistogramStatisticsSync(table, columns);
        }
    }

    public void expireConnectorTableHistogramStatisticsCache(String catalogName, String dbName, String tableName,
                                                              List<String> columns) {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(new ConnectContext(), catalogName, dbName, tableName);
        if (table == null) {
            return;
        }
        GlobalStateMgr.getCurrentState().getStatisticStorage().expireConnectorHistogramStatistics(table, columns);
    }

    /**
     * Replay handler for OP_ADD_EXTERNAL_BASIC_STATS_META on followers.
     * <p>
     * Prefer invalidating the connector cache by the table UUID persisted in the journal, so that replay
     * never resolves external table metadata (which may block on HMS/object storage and stall the replayer).
     * The fresh statistics are reloaded lazily on the next query. Fall back to the legacy metadata-based
     * refresh only for journals written before the UUID was persisted.
     */
    public void replayRefreshExternalBasicStatsCache(ExternalBasicStatsMeta basicStatsMeta) {
        if (invalidateConnectorColumnStatsByUUID(basicStatsMeta.getTableUUID(), basicStatsMeta.getColumns())) {
            return;
        }
        refreshConnectorTableBasicStatisticsCache(basicStatsMeta.getCatalogName(), basicStatsMeta.getDbName(),
                basicStatsMeta.getTableName(), basicStatsMeta.getColumns(), true);
    }

    /**
     * Replay handler for OP_REMOVE_EXTERNAL_BASIC_STATS_META on followers.
     * See {@link #replayRefreshExternalBasicStatsCache} for the UUID-first / legacy-fallback rationale.
     */
    public void replayExpireExternalBasicStatsCache(ExternalBasicStatsMeta basicStatsMeta) {
        if (invalidateConnectorColumnStatsByUUID(basicStatsMeta.getTableUUID(), basicStatsMeta.getColumns())) {
            return;
        }
        expireConnectorTableAndColumnStatistics(basicStatsMeta.getCatalogName(), basicStatsMeta.getDbName(),
                basicStatsMeta.getTableName(), basicStatsMeta.getColumns());
    }

    /**
     * Replay handler for OP_ADD_EXTERNAL_HISTOGRAM_STATS_META on followers.
     * See {@link #replayRefreshExternalBasicStatsCache} for the UUID-first / legacy-fallback rationale.
     */
    public void replayRefreshExternalHistogramStatsCache(ExternalHistogramStatsMeta histogramStatsMeta) {
        List<String> columns = Lists.newArrayList(histogramStatsMeta.getColumn());
        if (invalidateConnectorHistogramStatsByUUID(histogramStatsMeta.getTableUUID(), columns)) {
            return;
        }
        refreshConnectorTableHistogramStatisticsCache(histogramStatsMeta.getCatalogName(),
                histogramStatsMeta.getDbName(), histogramStatsMeta.getTableName(), columns, true);
    }

    /**
     * Replay handler for OP_REMOVE_EXTERNAL_HISTOGRAM_STATS_META on followers.
     * See {@link #replayRefreshExternalBasicStatsCache} for the UUID-first / legacy-fallback rationale.
     */
    public void replayExpireExternalHistogramStatsCache(ExternalHistogramStatsMeta histogramStatsMeta) {
        List<String> columns = Lists.newArrayList(histogramStatsMeta.getColumn());
        if (invalidateConnectorHistogramStatsByUUID(histogramStatsMeta.getTableUUID(), columns)) {
            return;
        }
        expireConnectorTableHistogramStatisticsCache(histogramStatsMeta.getCatalogName(),
                histogramStatsMeta.getDbName(), histogramStatsMeta.getTableName(), columns);
    }

    private boolean invalidateConnectorColumnStatsByUUID(String tableUUID, List<String> columns) {
        // Feature off (default) or legacy journal without a UUID: fall back to the legacy eager refresh.
        if (!Config.enable_external_stats_lazy_refresh_on_replay || tableUUID == null || tableUUID.isEmpty()) {
            return false;
        }
        GlobalStateMgr.getCurrentState().getStatisticStorage()
                .invalidateConnectorTableColumnStatistics(tableUUID, columns);
        return true;
    }

    private boolean invalidateConnectorHistogramStatsByUUID(String tableUUID, List<String> columns) {
        // Feature off (default) or legacy journal without a UUID: fall back to the legacy eager refresh.
        if (!Config.enable_external_stats_lazy_refresh_on_replay || tableUUID == null || tableUUID.isEmpty()) {
            return false;
        }
        GlobalStateMgr.getCurrentState().getStatisticStorage()
                .invalidateConnectorHistogramStatistics(tableUUID, columns);
        return true;
    }

    public void clearStatisticFromDroppedTable() {
        clearStatisticFromNativeDroppedTable();
        clearStatisticFromExternalDroppedTable();
    }

    public void clearStatisticFromNativeDroppedTable() {
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();
        Set<Long> tables = new HashSet<>();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                continue;
            }

            for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                /*
                 * If the meta contains statistical information, but the data is empty,
                 * it means that the table has been truncate or insert overwrite, and it is set to empty,
                 * so it is treated as a table that has been deleted here.
                 */
                if (!StatisticUtils.isEmptyTable(table)) {
                    tables.add(table.getId());
                }
            }
        }

        Set<Long> tableIdHasDeleted = new HashSet<>(basicStatsMetaMap.keySet());
        tableIdHasDeleted.removeAll(tables);
        if (tableIdHasDeleted.isEmpty()) {
            return;
        }

        int exprLimit = Config.expr_children_limit / 2;
        List<Long> batchTableIds = tableIdHasDeleted.stream()
                .distinct()
                .limit(exprLimit)
                .collect(Collectors.toList());

        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        try (var guard = statsConnectCtx.bindScope()) {
            statsConnectCtx.setStatisticsConnection(true);
            statsConnectCtx.getSessionVariable().setExprChildrenLimit(batchTableIds.size() * 3);

            dropBasicStatsMetaAndData(statsConnectCtx, batchTableIds);
            dropHistogramStatsMetaAndData(statsConnectCtx, batchTableIds);
            dropMultiColumnStatsMetaAndData(statsConnectCtx, batchTableIds);
        }
    }

    public void clearStatisticFromExternalDroppedTable() {
        Map<Pair<String, String>, List<StatsMetaKey>> tablesByDatabase = new HashMap<>();
        for (StatsMetaKey key : externalBasicStatsMetaMap.keySet()) {
            tablesByDatabase.computeIfAbsent(Pair.create(key.getCatalogName(), key.getDbName()),
                    ignored -> new ArrayList<>()).add(key);
        }
        List<StatsMetaKey> droppedTables = new ArrayList<>();
        ConnectContext context = new ConnectContext();
        for (Map.Entry<Pair<String, String>, List<StatsMetaKey>> entry : tablesByDatabase.entrySet()) {
            String catalogName = entry.getKey().first;
            String dbName = entry.getKey().second;
            try {
                Optional<ConnectorMetadata> metadata = GlobalStateMgr.getCurrentState().getMetadataMgr()
                        .getOptionalMetadata(catalogName);
                if (metadata.isEmpty()) {
                    LOG.warn("Cannot resolve catalog {}, keep statistics for database {}", catalogName, dbName);
                    continue;
                }
                // Materialize the complete listing before removing anything. For Iceberg Glue this
                // reads catalog pages, not each table's metadata.json or manifests.
                Set<String> tableNames = new HashSet<>(metadata.get().listTableNames(context, dbName));
                for (StatsMetaKey key : entry.getValue()) {
                    if (!tableNames.contains(key.getTableName())) {
                        LOG.info("Table {}.{}.{} not listed, clear its statistics",
                                catalogName, dbName, key.getTableName());
                        droppedTables.add(key);
                    }
                }
            } catch (Exception e) {
                // A failed or partial listing is not evidence that any table was dropped.
                LOG.warn("Failed to list tables in {}.{}, keep statistics for this database", catalogName, dbName, e);
            }
        }

        for (StatsMetaKey droppedTable : droppedTables) {
            dropExternalBasicStatsMetaAndData(droppedTable.getCatalogName(), droppedTable.getDbName(),
                    droppedTable.getTableName());
            dropExternalHistogramStatsMetaAndData(droppedTable.getCatalogName(), droppedTable.getDbName(),
                    droppedTable.getTableName());
            dropExternalMcvStatsMetaAndData(droppedTable.getCatalogName(), droppedTable.getDbName(),
                    droppedTable.getTableName());
        }
    }

    public void recordDropPartition(long partitionId) {
        dropPartitionIds.add(partitionId);
    }

    public void clearStatisticFromDroppedPartition() {
        clearStaleStatsWhenStarted();
        clearStalePartitionStats();
        dropPartitionStatistics();
    }

    private void dropPartitionStatistics() {
        if (dropPartitionIds.isEmpty()) {
            return;
        }

        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        try (var scope = statsConnectCtx.bindScope()) {
            statsConnectCtx.setStatisticsConnection(true);
            List<Long> pids =
                    dropPartitionIds.stream().limit(Config.expr_children_limit / 2).collect(Collectors.toList());

            StatisticExecutor executor = new StatisticExecutor();
            if (executor.dropPartitionStatistics(statsConnectCtx, pids)) {
                pids.forEach(dropPartitionIds::remove);
            }
        }
    }

    private void clearStalePartitionStats() {
        // It means FE is restarted, the previous step had cleared the stats.
        if (lastCleanTime == null) {
            lastCleanTime = LocalDateTime.now();
            return;
        }

        //  do the clear task once every 12 hours.
        if (Duration.between(lastCleanTime, LocalDateTime.now()).toSeconds() < Config.clear_stale_stats_interval_sec) {
            return;
        }

        List<Table> tables = Lists.newArrayList();
        LocalDateTime workTime = LocalDateTime.now();
        for (Map.Entry<Long, AnalyzeStatus> entry : analyzeStatusMap.entrySet()) {
            AnalyzeStatus analyzeStatus = entry.getValue();
            LocalDateTime endTime = analyzeStatus.getEndTime();
            // After the last cleanup, if a table has successfully undergone a statistics collection,
            // and the collection completion time is after the last cleanup time,
            // then during the next cleanup process, the stale column statistics would be cleared.
            if (analyzeStatus instanceof NativeAnalyzeStatus
                    && analyzeStatus.getStatus() == StatsConstants.ScheduleStatus.FINISH
                    && Duration.between(endTime, lastCleanTime).toMinutes() < 30) {
                NativeAnalyzeStatus nativeAnalyzeStatus = (NativeAnalyzeStatus) analyzeStatus;
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(nativeAnalyzeStatus.getDbId());
                if (db != null && GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(db.getId(), nativeAnalyzeStatus.getTableId()) != null) {
                    tables.add(GlobalStateMgr.getCurrentState().getLocalMetastore()
                                .getTable(db.getId(), nativeAnalyzeStatus.getTableId()));
                }
            }
        }

        if (tables.isEmpty()) {
            lastCleanTime = workTime;
        }

        List<Long> tableIds = Lists.newArrayList();
        List<Long> partitionIds = Lists.newArrayList();
        int exprLimit = Config.expr_children_limit / 2;
        for (Table table : tables) {
            List<Long> pids = table.getPartitions().stream().map(Partition::getId).collect(Collectors.toList());
            if (pids.size() > exprLimit) {
                tableIds.clear();
                partitionIds.clear();
                tableIds.add(table.getId());
                partitionIds.addAll(pids);
                break;
            } else if ((tableIds.size() + partitionIds.size() + pids.size()) > exprLimit) {
                break;
            }
            tableIds.add(table.getId());
            partitionIds.addAll(pids);
        }

        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        try (var scope = statsConnectCtx.bindScope()) {
            statsConnectCtx.setStatisticsConnection(true);
            StatisticExecutor executor = new StatisticExecutor();
            statsConnectCtx.getSessionVariable().setExprChildrenLimit(partitionIds.size() * 3);
            boolean res = executor.dropTableInvalidPartitionStatistics(statsConnectCtx, tableIds, partitionIds);
            if (!res) {
                LOG.debug("failed to clean stale column statistics before time: {}", lastCleanTime);
            }
            lastCleanTime = LocalDateTime.now();
        }
    }

    private void clearStaleStatsWhenStarted() {
        if (!Config.statistic_check_expire_partition || checkTableIds.isEmpty()) {
            return;
        }

        if (checkTableIds.contains(CHECK_ALL_TABLES)) {
            checkTableIds.clear();
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();
            for (Long dbId : dbIds) {
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
                if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                    continue;
                }

                for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                    if (table == null || !(table.isOlapOrCloudNativeTable() || table.isMaterializedView())) {
                        continue;
                    }
                    checkTableIds.add(new Pair<>(dbId, table.getId()));
                }
            }

        }

        List<Pair<Long, Long>> checkDbTableIds = Lists.newArrayList();
        List<Long> checkPartitionIds = Lists.newArrayList();
        ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
        statsConnectCtx.setStatisticsConnection(true);

        int exprLimit = Config.expr_children_limit / 2;
        for (Pair<Long, Long> dbTableId : checkTableIds) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbTableId.first);
            if (null == db) {
                continue;
            }

            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), dbTableId.second);
            if (table == null) {
                continue;
            }

            List<Long> pids = table.getPartitions().stream().map(Partition::getId).collect(Collectors.toList());

            // SQL parse will limit expr number, so we need modify it in the session
            // Of course, it's low probability to reach the limit
            if (pids.size() > exprLimit) {
                checkDbTableIds.clear();
                checkPartitionIds.clear();
                checkDbTableIds.add(dbTableId);
                checkPartitionIds.addAll(pids);
                break;
            } else if ((checkDbTableIds.size() + checkPartitionIds.size() + pids.size()) > exprLimit) {
                break;
            }

            checkDbTableIds.add(dbTableId);
            checkPartitionIds.addAll(pids);
        }

        if (checkDbTableIds.isEmpty() || checkPartitionIds.isEmpty()) {
            return;
        }

        try (var scope = statsConnectCtx.bindScope()) {
            StatisticExecutor executor = new StatisticExecutor();
            List<Long> tables = checkDbTableIds.stream().map(p -> p.second).collect(Collectors.toList());

            statsConnectCtx.getSessionVariable().setExprChildrenLimit(checkPartitionIds.size() * 3);
            if (executor.dropTableInvalidPartitionStatistics(statsConnectCtx, tables, checkPartitionIds)) {
                checkDbTableIds.forEach(checkTableIds::remove);
            }
        }
    }

    public void dropBasicStatsMetaAndData(ConnectContext statsConnectCtx, List<Long> tableIds) {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        try (var guard = statsConnectCtx.bindScope()) {
            if (tableIds == null || tableIds.isEmpty()) {
                return;
            }
            List<Long> distinctTableIds = tableIds.stream().distinct().collect(Collectors.toList());
            List<BasicStatsMeta> metasToRemove = distinctTableIds.stream()
                    .map(basicStatsMetaMap::get)
                    .filter(java.util.Objects::nonNull)
                    .collect(Collectors.toList());
            if (metasToRemove.isEmpty()) {
                return;
            }

            // Both types of tables need to be deleted, because there may have been a switch of
            // collecting statistics types, leaving some discarded statistics data.
            boolean sampleOk = statisticExecutor.dropTableStatistics(statsConnectCtx, distinctTableIds,
                    StatsConstants.AnalyzeType.SAMPLE);
            boolean fullOk = statisticExecutor.dropTableStatistics(statsConnectCtx, distinctTableIds,
                    StatsConstants.AnalyzeType.FULL);
            if (sampleOk && fullOk) {
                GlobalStateMgr.getCurrentState().getEditLog().logRemoveBasicStatsMetaBatch(metasToRemove, wal -> {
                    for (BasicStatsMeta meta : metasToRemove) {
                        basicStatsMetaMap.remove(meta.getTableId());
                    }
                });
            }
        }
    }

    public void dropHistogramStatsMetaAndData(ConnectContext statsConnectCtx, List<Long> tableIds) {
        if (tableIds == null || tableIds.isEmpty()) {
            return;
        }
        List<Long> distinctTableIdsToDelete = tableIds.stream().distinct().collect(Collectors.toList());
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        boolean ok = statisticExecutor.dropHistogramByTableIds(statsConnectCtx, distinctTableIdsToDelete);
        if (!ok) {
            return;
        }

        Set<Long> tableIdSet = new HashSet<>(distinctTableIdsToDelete);
        List<Pair<Long, String>> keysToRemove = new ArrayList<>();
        List<HistogramStatsMeta> metasToRemove = new ArrayList<>();
        for (Map.Entry<Pair<Long, String>, HistogramStatsMeta> entry : histogramStatsMetaMap.entrySet()) {
            if (tableIdSet.contains(entry.getKey().first)) {
                keysToRemove.add(entry.getKey());
                metasToRemove.add(entry.getValue());
            }
        }
        if (metasToRemove.isEmpty()) {
            return;
        }
        GlobalStateMgr.getCurrentState().getEditLog().logRemoveHistogramStatsMetaBatch(metasToRemove, wal -> {
            for (Pair<Long, String> key : keysToRemove) {
                histogramStatsMetaMap.remove(key);
            }
        });
    }

    public void dropMultiColumnStatsMetaAndData(ConnectContext statsConnectCtx, List<Long> tableIds) {
        if (tableIds == null || tableIds.isEmpty()) {
            return;
        }
        List<Long> distinctTableIdsToDelete = tableIds.stream().distinct().collect(Collectors.toList());
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        boolean ok = statisticExecutor.dropTableMultiColumnStatistics(statsConnectCtx, distinctTableIdsToDelete);
        if (!ok) {
            return;
        }

        Set<Long> tableIdSet = new HashSet<>(distinctTableIdsToDelete);
        List<MultiColumnStatsKey> keysToRemove = new ArrayList<>();
        List<MultiColumnStatsMeta> metasToRemove = new ArrayList<>();
        for (Map.Entry<MultiColumnStatsKey, MultiColumnStatsMeta> entry : multiColumnStatsMetaMap.entrySet()) {
            MultiColumnStatsKey key = entry.getKey();
            if (tableIdSet.contains(key.tableId)) {
                keysToRemove.add(key);
                metasToRemove.add(entry.getValue());
            }
        }
        if (metasToRemove.isEmpty()) {
            return;
        }
        GlobalStateMgr.getCurrentState().getEditLog().logRemoveMultiColumnStatsMetaBatch(metasToRemove, wal -> {
            for (MultiColumnStatsKey key : keysToRemove) {
                removeMultiColumnStatsMetaKey(key);
            }
        });
    }

    public void dropExternalBasicStatsMetaAndData(String catalogName, String dbName, String tableName) {
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().dropExternalBasicStatsData(catalogName, dbName, tableName);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().removeExternalBasicStatsMeta(catalogName, dbName, tableName);
    }

    public void dropExternalHistogramStatsMetaAndData(String catalogName, String dbName, String tableName) {
        List<String> columns = Lists.newArrayList();
        StatsMetaKey tableKey = new StatsMetaKey(catalogName, dbName, tableName);
        for (StatsMetaColumnKey histogramKey : externalHistogramStatsMetaMap.keySet()) {
            if (histogramKey.getTableKey().equals(tableKey)) {
                columns.add(histogramKey.getColumnName());
            }
        }

        dropExternalHistogramStatsMetaAndData(catalogName, dbName, tableName, columns);
    }

    public void dropExternalHistogramStatsMetaAndData(String catalogName, String dbName, String tableName,
                                                      List<String> columns) {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        statisticExecutor.dropExternalHistogram(StatisticUtils.buildConnectContext(), catalogName, dbName, tableName,
                columns);

        removeExternalHistogramStatsMeta(catalogName, dbName, tableName, columns);
    }

    public void dropExternalHistogramStatsMetaAndData(ConnectContext statsConnectCtx,
                                                      TableName tableName, Table table,
                                                      List<String> columns) {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        statisticExecutor.dropExternalHistogram(statsConnectCtx, table.getUUID(), columns);

        removeExternalHistogramStatsMeta(tableName.getCatalog(), tableName.getDb(), tableName.getTbl(), columns);
    }

    public void registerConnection(long analyzeID, ConnectContext ctx) {
        connectionMap.put(analyzeID, ctx);
    }

    public void unregisterConnection(long analyzeID, boolean killExecutor) {
        ConnectContext context = connectionMap.remove(analyzeID);
        cancelledAnalyzeIds.remove(analyzeID);
        if (killExecutor) {
            if (context != null) {
                context.kill(false, "kill analyze unregisterConnection");
            } else {
                throw new SemanticException("There is no running task with analyzeId " + analyzeID);
            }
        }
    }

    public void killConnection(long analyzeID) {
        AnalyzeStatus status = analyzeStatusMap.get(analyzeID);
        if (status instanceof ExternalAnalyzeStatus external && external.getType() == StatsConstants.AnalyzeType.JOIN
                && external.getStatus() == StatsConstants.ScheduleStatus.RUNNING
                && external.getJoinCollectionGeneration() > 0) {
            getJoinStatisticsManager().cancelCollection(external.getJoinCollectionGeneration());
            return;
        }
        ConnectContext context = connectionMap.get(analyzeID);
        if (context == null) {
            throw new SemanticException("There is no running task with analyzeId " + analyzeID);
        }
        cancelledAnalyzeIds.add(analyzeID);
        context.kill(false, USER_CANCEL_MESSAGE);
    }

    public boolean isAnalyzeCancelled(long analyzeID) {
        return cancelledAnalyzeIds.contains(analyzeID);
    }

    public void killAllPendingTasks() {
        if (ANALYZE_TASK_THREAD_POOL instanceof ThreadPoolExecutor executor) {
            BlockingQueue<Runnable> queue = executor.getQueue();
            List<Runnable> tasksToRemove = new ArrayList<>();

            for (Runnable task : queue) {
                if (task instanceof CancelableAnalyzeTask cancellableTask) {
                    cancellableTask.cancel();
                    tasksToRemove.add(task);
                }
            }

            queue.removeAll(tasksToRemove);
            LOG.info("Cancelled {} CancelableAnalyzeTask from queue", tasksToRemove.size());
        }
    }

    public ExecutorService getAnalyzeTaskThreadPool() {
        return ANALYZE_TASK_THREAD_POOL;
    }

    public void updateLoadRows(TransactionState transactionState) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(transactionState.getDbId());
        if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
            return;
        }
        TxnCommitAttachment attachment = transactionState.getTxnCommitAttachment();
        if (attachment instanceof RLTaskTxnCommitAttachment) {
            if (!transactionState.getTableIdList().isEmpty()) {
                long tableId = transactionState.getTableIdList().get(0);
                long loadedRows = ((RLTaskTxnCommitAttachment) attachment).getLoadedRows();
                updateBasicStatsMeta(db.getId(), tableId, loadedRows);
            }
        } else if (attachment instanceof ManualLoadTxnCommitAttachment) {
            if (!transactionState.getTableIdList().isEmpty()) {
                long tableId = transactionState.getTableIdList().get(0);
                long loadedRows = ((ManualLoadTxnCommitAttachment) attachment).getLoadedRows();
                updateBasicStatsMeta(db.getId(), tableId, loadedRows);
            }
        } else if (attachment instanceof LoadJobFinalOperation) {
            LoadJobFinalOperation loadJobFinalOperation = (LoadJobFinalOperation) attachment;
            loadJobFinalOperation.getLoadingStatus().travelTableCounters(
                    kv -> updateBasicStatsMeta(db.getId(), kv.getKey(), kv.getValue().get(TableMetricsEntity.TABLE_LOAD_ROWS))
            );
        } else if (attachment instanceof InsertTxnCommitAttachment) {
            if (!transactionState.getTableIdList().isEmpty()) {
                long tableId = transactionState.getTableIdList().get(0);
                long loadRows = ((InsertTxnCommitAttachment) attachment).getLoadedRows();
                if (loadRows == 0) {
                    OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                                .getTable(db.getId(), tableId);
                    loadRows = table != null ? table.getRowCount() : 0;
                }
                updateBasicStatsMeta(db.getId(), tableId, loadRows);
            }
        }
    }




    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        List<JoinStatisticsMeta> joinStatistics = joinStatisticsRegistry.snapshot();
        List<AnalyzeStatus> analyzeStatuses = getAnalyzeStatusMap().values().stream()
                .distinct().collect(Collectors.toList());

        int numJson = 1 + analyzeJobMap.size()
                + 1 + analyzeStatuses.size()
                + 1 + basicStatsMetaMap.size()
                + 1 + histogramStatsMetaMap.size()
                + 1 + externalBasicStatsMetaMap.size()
                + 1 + externalHistogramStatsMetaMap.size()
                + 1 + multiColumnStatsMetaMap.size()
                + 1 + externalMcvStatsMetaMap.size()
                + 1 + joinStatistics.size();

        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.ANALYZE_MGR, numJson);

        writer.writeInt(analyzeJobMap.size());
        for (AnalyzeJob analyzeJob : analyzeJobMap.values()) {
            writer.writeJson(analyzeJob);
        }

        writer.writeInt(analyzeStatuses.size());
        for (AnalyzeStatus analyzeStatus : analyzeStatuses) {
            writer.writeJson(analyzeStatus);
        }

        writer.writeInt(basicStatsMetaMap.size());
        for (BasicStatsMeta basicStatsMeta : basicStatsMetaMap.values()) {
            writer.writeJson(basicStatsMeta);
        }

        writer.writeInt(histogramStatsMetaMap.size());
        for (HistogramStatsMeta histogramStatsMeta : histogramStatsMetaMap.values()) {
            writer.writeJson(histogramStatsMeta);
        }

        writer.writeInt(externalBasicStatsMetaMap.size());
        for (ExternalBasicStatsMeta basicStatsMeta : externalBasicStatsMetaMap.values()) {
            writer.writeJson(basicStatsMeta);
        }

        writer.writeInt(externalHistogramStatsMetaMap.size());
        for (ExternalHistogramStatsMeta histogramStatsMeta : externalHistogramStatsMetaMap.values()) {
            writer.writeJson(histogramStatsMeta);
        }


        writer.writeInt(multiColumnStatsMetaMap.size());
        for (MultiColumnStatsMeta multiColumnStatsMeta : multiColumnStatsMetaMap.values()) {
            writer.writeJson(multiColumnStatsMeta);
        }

        writer.writeInt(externalMcvStatsMetaMap.size());
        for (ExternalMcvStatsMeta meta : externalMcvStatsMetaMap.values()) {
            writer.writeJson(meta);
        }

        writer.writeInt(joinStatistics.size());
        for (JoinStatisticsMeta meta : joinStatistics) {
            writer.writeJson(meta);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        reader.readCollection(AnalyzeJob.class, this::replayAddAnalyzeJob);

        reader.readCollection(AnalyzeStatus.class, this::replayAddAnalyzeStatus);

        reader.readCollection(BasicStatsMeta.class, this::replayAddBasicStatsMeta);

        reader.readCollection(HistogramStatsMeta.class, this::replayAddHistogramStatsMeta);

        reader.readCollection(ExternalBasicStatsMeta.class, this::replayAddExternalBasicStatsMeta);

        reader.readCollection(ExternalHistogramStatsMeta.class, this::replayAddExternalHistogramStatsMeta);

        reader.readCollection(MultiColumnStatsMeta.class, this::replayAddMultiColumnStatsMeta);

        // Catch only application errors after a complete record has been decoded. Reader failures
        // (including malformed/truncated images) and VM errors must still abort image loading.
        reader.readCollection(ExternalMcvStatsMeta.class, meta -> {
            try {
                replayAddExternalMcvStatsMeta(meta);
            } catch (RuntimeException e) {
                LOG.error("Skipping invalid external MCV statistics metadata while loading image: {}.{}.{}",
                        meta == null ? null : meta.getCatalogName(), meta == null ? null : meta.getDbName(),
                        meta == null ? null : meta.getTableName(), e);
            }
        });
        reader.readCollection(JoinStatisticsMeta.class, meta -> {
            try {
                replayJoinStatistics(meta, false);
            } catch (RuntimeException e) {
                LOG.error("Skipping invalid JOIN statistics metadata while loading image: id={}, name={}",
                        meta == null ? null : meta.getId(),
                        meta == null || meta.getDefinition() == null ? null : meta.getDefinition().getName(), e);
            }
        });
    }

    private void updateBasicStatsMeta(long dbId, long tableId, long loadedRows) {
        BasicStatsMeta basicStatsMeta =
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getTableBasicStatsMeta(tableId);
        if (basicStatsMeta == null) {
            // first load without analyze op, we need fill a meta with loaded rows for cardinality estimation
            BasicStatsMeta meta = new BasicStatsMeta(dbId, tableId, Lists.newArrayList(),
                    StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(),
                    StatsConstants.buildInitStatsProp(), loadedRows);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().put(tableId, meta);
        } else {
            basicStatsMeta.increaseDeltaRows(loadedRows);
        }
    }

    private static class SerializeData {
        @SerializedName("analyzeJobs")
        public List<NativeAnalyzeJob> jobs;

        @SerializedName("analyzeStatus")
        public List<NativeAnalyzeStatus> nativeStatus;

        @SerializedName("basicStatsMeta")
        public List<BasicStatsMeta> basicStatsMeta;

        @SerializedName("histogramStatsMeta")
        public List<HistogramStatsMeta> histogramStatsMeta;

        @SerializedName("multiColumnStatsMeta")
        public List<MultiColumnStatsMeta> multiColumnStatsMeta;
    }

    public static class StatsMetaKey {
        private final String catalogName;
        private final String dbName;
        private final String tableName;

        public StatsMetaKey(String catalogName, String dbName, String tableName) {
            this.catalogName = catalogName;
            this.dbName = dbName;
            this.tableName = tableName;
        }

        public String getCatalogName() {
            return catalogName;
        }

        public String getDbName() {
            return dbName;
        }

        public String getTableName() {
            return tableName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StatsMetaKey)) {
                return false;
            }
            StatsMetaKey that = (StatsMetaKey) o;
            return Objects.equal(catalogName, that.catalogName) &&
                    Objects.equal(dbName, that.dbName) &&
                    Objects.equal(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(catalogName, dbName, tableName);
        }
    }

    public static class StatsMetaColumnKey {
        private final StatsMetaKey tableKey;
        private final String columnName;
        public StatsMetaColumnKey(String catalogName, String dbName, String tableName, String columnName) {
            this.tableKey = new StatsMetaKey(catalogName, dbName, tableName);
            this.columnName = columnName;
        }

        public StatsMetaKey getTableKey() {
            return tableKey;
        }

        public String getColumnName() {
            return columnName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StatsMetaColumnKey)) {
                return false;
            }
            StatsMetaColumnKey that = (StatsMetaColumnKey) o;
            return Objects.equal(tableKey, that.tableKey) &&
                    Objects.equal(columnName, that.columnName);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(tableKey, columnName);
        }
    }

    public static class ExternalMcvStatsKey {
        private final StatsMetaKey tableKey;
        // Case-insensitive like column names; the order of the group does not matter.
        private final Set<String> columnNames;

        public ExternalMcvStatsKey(String catalogName, String dbName, String tableName,
                                           Collection<String> columnNames) {
            this.tableKey = new StatsMetaKey(catalogName, dbName, tableName);
            this.columnNames = new HashSet<>();
            for (String columnName : columnNames) {
                this.columnNames.add(columnName.toLowerCase());
            }
        }

        public static ExternalMcvStatsKey of(ExternalMcvStatsMeta meta) {
            return new ExternalMcvStatsKey(meta.getCatalogName(), meta.getDbName(), meta.getTableName(),
                    meta.getColumnNames());
        }

        public StatsMetaKey getTableKey() {
            return tableKey;
        }

        public Set<String> getColumnNames() {
            return columnNames;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ExternalMcvStatsKey)) {
                return false;
            }
            ExternalMcvStatsKey that = (ExternalMcvStatsKey) o;
            return Objects.equal(tableKey, that.tableKey) && Objects.equal(columnNames, that.columnNames);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(tableKey, columnNames);
        }
    }

    public static class MultiColumnStatsKey {
        private final long tableId;
        private final Set<Integer> columnIds;
        private final List<StatisticsType> statisticsTypes;

        public MultiColumnStatsKey(long tableId, Set<Integer> columnIds, List<StatisticsType> statisticsTypes) {
            this.tableId = tableId;
            this.columnIds = columnIds;
            this.statisticsTypes = statisticsTypes;
        }

        public long getTableId() {
            return tableId;
        }

        public Set<Integer> getColumnIds() {
            return columnIds;
        }

        public List<StatisticsType> getStatisticsTypes() {
            return statisticsTypes;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MultiColumnStatsKey that = (MultiColumnStatsKey) o;
            return tableId == that.tableId && java.util.Objects.equals(columnIds, that.columnIds) &&
                    java.util.Objects.equals(statisticsTypes, that.statisticsTypes);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(tableId, columnIds, statisticsTypes);
        }
    }
}
