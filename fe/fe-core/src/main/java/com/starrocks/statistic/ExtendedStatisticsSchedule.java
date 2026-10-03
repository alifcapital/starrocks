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

import com.google.common.util.concurrent.Striped;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.StatisticsType;
import com.starrocks.sql.common.MetaUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/** Explicit recurring MCV/JOIN targets, using the existing analyze-job journal and collector. */
public final class ExtendedStatisticsSchedule {
    // Fixed storage; never retain a lock per table, job or user-supplied name.
    private static final Striped<Lock> MCV_LOCKS = Striped.lock(256);

    private ExtendedStatisticsSchedule() {
    }

    public static void checkJoinAnalyzePrivilege(ConnectContext context, String name, long id) {
        var meta = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getJoinStatisticsRegistry().get(name);
        if (meta != null && meta.getId() == id) {
            meta.getDefinition().getSources().forEach(source ->
                    com.starrocks.sql.analyzer.Authorizer.checkActionForAnalyzeStatement(context, source.getTableName()));
        }
    }

    public static boolean canInspectJoin(ConnectContext context, String name, long id) {
        var meta = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getJoinStatisticsRegistry().get(name);
        if (meta == null || meta.getId() != id) {
            return false;
        }
        try {
            for (var source : meta.getDefinition().getSources()) {
                com.starrocks.sql.analyzer.Authorizer.checkTableAction(context, source.getTableName(),
                        com.starrocks.authorization.PrivilegeType.SELECT);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static Lock mcvLock(String tableUuid) {
        return MCV_LOCKS.get(tableUuid);
    }

    public static void validateProperties(Map<String, String> properties, boolean mcv) {
        Set<String> allowed = mcv ? Set.of(StatsConstants.PROP_COLLECT_INTERVAL_SEC_KEY,
                StatsConstants.MCV_SIZE, StatsConstants.MCV_BUCKET_NUM)
                : Set.of(StatsConstants.PROP_COLLECT_INTERVAL_SEC_KEY);
        for (String key : properties.keySet()) {
            if (!allowed.contains(key)) {
                throw new SemanticException("Unsupported scheduled statistics property: " + key);
            }
        }
        if (properties.containsKey(StatsConstants.PROP_COLLECT_INTERVAL_SEC_KEY)) {
            try {
                long seconds = Long.parseLong(properties.get(StatsConstants.PROP_COLLECT_INTERVAL_SEC_KEY));
                if (seconds < 1 || seconds > 31536000) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                throw new SemanticException("collect_interval_sec must be between 1 and 31536000");
            }
        }
    }

    public static ExternalAnalyzeJob create(CreateAnalyzeJobStmt stmt, ConnectContext context) {
        var meta = stmt.getJoinStatistics();
        TableName name = meta == null
                ? new TableName(stmt.getCatalogName(), stmt.getDbName(), stmt.getTableName())
                : meta.getDefinition().getSources().get(0).getTableName();
        Table table = MetaUtils.getSessionAwareTable(context, null, name);
        ExternalAnalyzeJob job = new ExternalAnalyzeJob(name.getCatalog(), name.getDb(), name.getTbl(),
                meta == null ? stmt.getColumnNames() : List.of(),
                meta == null ? stmt.getColumnTypes() : List.of(),
                meta == null ? StatsConstants.AnalyzeType.MCV : StatsConstants.AnalyzeType.JOIN,
                StatsConstants.ScheduleType.SCHEDULE, Map.copyOf(stmt.getProperties()),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
        job.setTargetUuid(table.getUUID());
        job.setInitialExtendedCollection(Config.enable_trigger_analyze_job_immediate);
        if (meta != null) {
            job.setJoinStatisticsTarget(meta);
        }
        return job;
    }

    public static void trigger(ExternalAnalyzeJob job) {
        if (!Config.enable_trigger_analyze_job_immediate) {
            return;
        }
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().getAnalyzeTaskThreadPool().execute(() -> {
            ConnectContext context = StatisticUtils.buildStatisticsCollectContext();
            try (var scope = context.bindScope()) {
                run(job, context, new StatisticExecutor(), true);
            }
        });
    }

    static boolean sameTarget(ExternalAnalyzeJob left, ExternalAnalyzeJob right) {
        if (left.getAnalyzeType() != right.getAnalyzeType()) {
            return false;
        }
        if (left.getAnalyzeType() == StatsConstants.AnalyzeType.JOIN) {
            return left.getJoinStatisticsId() == right.getJoinStatisticsId();
        }
        return java.util.Objects.equals(left.getTargetUuid(), right.getTargetUuid())
                && new java.util.HashSet<>(left.getColumns()).equals(new java.util.HashSet<>(right.getColumns()));
    }

    static long interval(ExternalAnalyzeJob job) {
        return Long.parseLong(job.getProperties().getOrDefault(StatsConstants.PROP_COLLECT_INTERVAL_SEC_KEY,
                Long.toString(Config.statistic_auto_collect_large_table_interval)));
    }

    public static void run(ExternalAnalyzeJob job, ConnectContext context, StatisticExecutor executor, boolean immediate) {
        // Do not block the auto collector behind a long immediate run.
        if (!job.beginExtendedRun()) {
            return;
        }
        try {
            var state = GlobalStateMgr.getCurrentState();
            AnalyzeMgr mgr = state.getAnalyzeMgr();
            if (!state.isLeader() || !Config.enable_statistic_collect || mgr.getAnalyzeJob(job.getId()) != job
                    || (immediate && !job.isInitialExtendedCollection())) {
                return;
            }
            long seconds = interval(job);
            LocalDateTime now = AutoStatisticsSchedule.now();
            AutoStatisticsSchedule.Attempt attempt = null;
            if (!immediate) {
                if (Config.enable_statistic_auto_collect_staggered_schedule) {
                    attempt = job.getCollectSchedule().due("extended:" + job.getId(), seconds, now,
                            AutoStatisticsSchedule.Window.current(), job.isInitialExtendedCollection());
                    if (job.getCollectSchedule().takeDirty()) {
                        mgr.updateAnalyzeJobWithLog(job);
                    }
                    if (attempt == null) {
                        return;
                    }
                } else if (!job.getWorkTime().equals(LocalDateTime.MIN)
                        && job.getWorkTime().plusSeconds(seconds).isAfter(now)) {
                    return;
                }
            }
            if (!StatisticAutoCollector.checkoutAnalyzeTime()) {
                return;
            }
            Lock lock = job.getAnalyzeType() == StatsConstants.AnalyzeType.MCV ? mcvLock(job.getTargetUuid()) : null;
            if (lock != null && !lock.tryLock()) {
                return;
            }
            ExternalAnalyzeStatus joinStatus = null;
            try {
                if (mgr.getAnalyzeJob(job.getId()) != job) {
                    return;
                }
                job.setStatus(StatsConstants.ScheduleStatus.RUNNING);
                job.setReason("");
                mgr.updateAnalyzeJobWithoutLog(job);
                if (job.getAnalyzeType() == StatsConstants.AnalyzeType.JOIN) {
                    var meta = mgr.getJoinStatisticsRegistry().get(job.getJoinStatisticsName());
                    if (meta == null || meta.getId() != job.getJoinStatisticsId()) {
                        throw new IllegalStateException("Scheduled JOIN statistics target was dropped or replaced");
                    }
                    joinStatus = new ExternalAnalyzeStatus(state.getNextId(), job.getCatalogName(), job.getDbName(),
                            job.getTableName(), job.getTargetUuid(), List.of(), StatsConstants.AnalyzeType.JOIN,
                            StatsConstants.ScheduleType.SCHEDULE, job.getProperties(), now);
                    joinStatus.setJoinStatisticsTarget(job);
                    joinStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
                    mgr.addAnalyzeStatus(joinStatus);
                    joinStatus.setStatus(StatsConstants.ScheduleStatus.RUNNING);
                    mgr.getJoinStatisticsManager().analyze(job.getJoinStatisticsName(), false,
                            joinStatus::setJoinCollectionGeneration, job.getJoinStatisticsId());
                } else {
                    TableName name = new TableName(job.getCatalogName(), job.getDbName(), job.getTableName());
                    Table table = MetaUtils.getSessionAwareTable(context, null, name);
                    if (!table.getUUID().equals(job.getTargetUuid())) {
                        throw new IllegalStateException("Scheduled MCV table was replaced; recreate the job");
                    }
                    var db = state.getMetadataMgr().getDb(context, name.getCatalog(), name.getDb());
                    // Resolve current schema, not types captured before a schema change.
                    var types = job.getColumns().stream().map(column -> {
                        var value = table.getColumn(column);
                        if (value == null || !value.getType().canStatistic() || value.getType().isComplexType()
                                || value.getType().isJsonType() || value.getType().isOnlyMetricType()) {
                            throw new IllegalStateException("Scheduled MCV column is missing or unsupported: " + column);
                        }
                        return value.getType();
                    }).toList();
                    var collect = StatisticsCollectJobFactory.buildExternalMcvStatisticsCollectJob(name.getCatalog(),
                            db, table, job.getColumns(), types, StatsConstants.AnalyzeType.FULL,
                            StatsConstants.ScheduleType.SCHEDULE, job.getProperties(), List.of(StatisticsType.MCV),
                            List.of(job.getColumns()));
                    var status = new ExternalAnalyzeStatus(state.getNextId(), name.getCatalog(), name.getDb(), name.getTbl(),
                            table.getUUID(), job.getColumns(), StatsConstants.AnalyzeType.MCV,
                            StatsConstants.ScheduleType.SCHEDULE, job.getProperties(), now);
                    status.setStatus(StatsConstants.ScheduleStatus.FAILED);
                    mgr.addAnalyzeStatus(status);
                    executor.collectStatistics(context, collect, status, true, true);
                    if (status.getStatus() != StatsConstants.ScheduleStatus.FINISH) {
                        throw new IllegalStateException(status.getReason());
                    }
                }
                job.setStatus(StatsConstants.ScheduleStatus.FINISH);
            } catch (Exception e) {
                job.setStatus(StatsConstants.ScheduleStatus.FAILED);
                job.setReason(e.getMessage());
            } finally {
                try {
                    job.setInitialExtendedCollection(false);
                    job.setWorkTime(AutoStatisticsSchedule.now());
                    if (joinStatus != null) {
                        joinStatus.setStatus(job.getStatus());
                        joinStatus.setReason(job.getReason());
                        joinStatus.setEndTime(job.getWorkTime());
                        joinStatus.setProgress(job.getStatus() == StatsConstants.ScheduleStatus.FINISH ? 100 : 0);
                        mgr.addAnalyzeStatus(joinStatus);
                    }
                    if (attempt != null) {
                        attempt.complete(job.getWorkTime());
                    }
                    mgr.updateAnalyzeJobWithLog(job);
                } finally {
                    if (lock != null) {
                        lock.unlock();
                    }
                }
            }
        } finally {
            job.endExtendedRun();
        }
    }
}
