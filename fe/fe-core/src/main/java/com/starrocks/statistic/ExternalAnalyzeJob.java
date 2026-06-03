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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.io.Writable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants.AnalyzeType;
import com.starrocks.statistic.StatsConstants.ScheduleStatus;
import com.starrocks.statistic.StatsConstants.ScheduleType;
import com.starrocks.type.Type;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ExternalAnalyzeJob implements AnalyzeJob, Writable {
    @SerializedName("id")
    private long id;

    @SerializedName("catalogName")
    private String catalogName;

    @SerializedName("dbName")
    private String dbName;

    @SerializedName("tableName")
    private String tableName;

    // Empty is all column
    @SerializedName("columns")
    private List<String> columns;

    @SerializedName("columnTypes")
    private List<Type> columnTypes;

    @SerializedName("type")
    private AnalyzeType type;

    @SerializedName("scheduleType")
    private ScheduleType scheduleType;

    @SerializedName("properties")
    private Map<String, String> properties;

    @SerializedName("status")
    private ScheduleStatus status;

    @SerializedName("workTime")
    private LocalDateTime workTime;

    @SerializedName("reason")
    private String reason;

    // Scheduled time of the job's next collection under the staggered schedule (see
    // Config.enable_statistic_auto_collect_staggered_schedule). Null until the first successful
    // collection splays it randomly within the collect interval; afterwards each collection slides
    // it by exactly one interval from the actual collection time.
    // volatile: read/written by both the auto collector and the immediate-run thread without a
    // common lock; the snapshot guard in advanceCollectSchedule keeps a lost race benign.
    @SerializedName("nextCollectTime")
    private volatile LocalDateTime nextCollectTime;

    // The collect interval nextCollectTime was computed with; a mismatch with the currently
    // effective interval triggers a re-splay.
    @SerializedName("collectIntervalUsed")
    private volatile long collectIntervalUsed;

    public ExternalAnalyzeJob(String catalogName, String dbName, String tableName, List<String> columnNames,
                              List<Type> columnTypes, AnalyzeType type,
                              ScheduleType scheduleType, Map<String, String> properties, ScheduleStatus status,
                              LocalDateTime workTime) {
        this.id = -1;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.columns = columnNames;
        this.columnTypes = columnTypes;
        this.type = type;
        this.scheduleType = scheduleType;
        this.properties = properties;
        this.status = status;
        this.workTime = workTime;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public boolean isNative() {
        return false;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public void setId(long id) {
        this.id = id;
    }

    @Override
    public List<String> getColumns() {
        return columns;
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public AnalyzeType getAnalyzeType() {
        return type;
    }

    @Override
    public ScheduleType getScheduleType() {
        return scheduleType;
    }

    @Override
    public LocalDateTime getWorkTime() {
        return workTime;
    }

    @Override
    public void setWorkTime(LocalDateTime workTime) {
        this.workTime = workTime;
    }

    @Override
    public String getReason() {
        return reason;
    }

    @Override
    public void setReason(String reason) {
        this.reason = reason;
    }

    @Override
    public ScheduleStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(ScheduleStatus status) {
        this.status = status;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public boolean isAnalyzeAllDb() {
        return dbName == null;
    }

    @Override
    public boolean isAnalyzeAllTable() {
        return tableName == null;
    }

    public LocalDateTime getNextCollectTime() {
        return nextCollectTime;
    }

    public void setNextCollectTime(LocalDateTime nextCollectTime) {
        this.nextCollectTime = nextCollectTime;
    }

    public long getCollectIntervalUsed() {
        return collectIntervalUsed;
    }

    public void setCollectIntervalUsed(long collectIntervalUsed) {
        this.collectIntervalUsed = collectIntervalUsed;
    }

    @Override
    public List<StatisticsCollectJob> instantiateJobs() {
        return StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(this);
    }

    @Override
    public void run(ConnectContext statsConnectContext, StatisticExecutor statisticExecutor,
                    List<StatisticsCollectJob> jobs) {
        setStatus(StatsConstants.ScheduleStatus.RUNNING);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateAnalyzeJobWithoutLog(this);

        boolean hasFailedCollectJob = false;
        StatisticsCollectJob succeededCollectJob = null;
        for (StatisticsCollectJob statsJob : jobs) {
            if (!StatisticAutoCollector.checkoutAnalyzeTime()) {
                break;
            }
            AnalyzeStatus analyzeStatus = new ExternalAnalyzeStatus(GlobalStateMgr.getCurrentState().getNextId(),
                    statsJob.getCatalogName(), statsJob.getDb().getFullName(), statsJob.getTable().getName(),
                    statsJob.getTable().getUUID(), statsJob.getColumnNames(), statsJob.getAnalyzeType(),
                    statsJob.getScheduleType(), statsJob.getProperties(), LocalDateTime.now());
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);

            statisticExecutor.collectStatistics(statsConnectContext, statsJob, analyzeStatus, true, true /* resetWarehouse */);
            if (analyzeStatus.getStatus().equals(StatsConstants.ScheduleStatus.FAILED)) {
                setStatus(StatsConstants.ScheduleStatus.FAILED);
                setWorkTime(LocalDateTime.now());
                setReason(analyzeStatus.getReason());
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateAnalyzeJobWithLog(this);
                hasFailedCollectJob = true;
                break;
            }
            succeededCollectJob = statsJob;
        }

        if (!hasFailedCollectJob) {
            setStatus(ScheduleStatus.FINISH);
            setWorkTime(LocalDateTime.now());
            if (succeededCollectJob != null) {
                advanceCollectSchedule(succeededCollectJob);
            }
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateAnalyzeJobWithLog(this);
        }
    }

    // synchronized: the snapshot guard below is check-then-act on shared schedule fields; without
    // mutual exclusion two runs finishing together could both pass it and the second would
    // overwrite the first one's update (e.g. replace the initial random splay)
    synchronized void advanceCollectSchedule(StatisticsCollectJob succeededCollectJob) {
        // jobs expanded to several tables stay on the legacy schedule (see the factory)
        if (isAnalyzeAllDb() || isAnalyzeAllTable()) {
            return;
        }
        if (!Config.enable_statistic_auto_collect_staggered_schedule && nextCollectTime == null) {
            return;
        }
        if (!Objects.equals(succeededCollectJob.getScheduleSnapshot(), nextCollectTime)) {
            // a concurrent run of this job already advanced the schedule while we were collecting -
            // enable_trigger_analyze_job_immediate starts a run on a separate thread right after
            // CREATE ANALYZE, racing the auto collector. Do not overwrite the other run's update
            // (it may hold the initial random splay). The worst a lost race can cause is one
            // redundant collection or one skipped slide; the next due pass repairs the schedule.
            return;
        }
        // an existing schedule keeps sliding even while the flag is off, so re-enabling it later
        // does not see a stale deadline and re-collect right after a legacy-mode collection
        LocalDateTime collectTime = LocalDateTime.now();
        if (nextCollectTime == null) {
            if (succeededCollectJob.getEffectiveIntervalSeconds() <= 0) {
                // a zero-length schedule must not exist (the factory keeps such jobs on the legacy
                // path). This also covers the first collection of a large table without statistics:
                // its row count is unknown until stats exist, so the interval resolves to the
                // small-table default of 0 - the splay is then established by the next collection.
                return;
            }
            // first collection: splay the next collect time randomly within the interval, so jobs
            // created (or collected for the first time) together spread across it instead of staying
            // synchronized
            collectIntervalUsed = succeededCollectJob.getEffectiveIntervalSeconds();
            nextCollectTime = StatisticUtils.splayNextCollectTime(collectTime, collectIntervalUsed);
        } else if (!succeededCollectJob.isOffScheduleCollect()) {
            // a due collection slides the schedule by exactly one interval from the actual collect
            // time. Off-schedule collections (columns that had no stats yet) keep it untouched - the
            // context is captured at instantiation time, completion may legitimately cross the deadline.
            // collectIntervalUsed may be stale if the effective interval changed while the stats meta
            // was absent (no re-splay runs on that path); the next factory pass detects the mismatch
            // and re-splays.
            nextCollectTime = collectTime.plusSeconds(collectIntervalUsed);
        }
    }

    @Override
    public String toString() {
        return "ExternalAnalyzeJob{" +
                "id=" + id +
                ", dbName=" + dbName +
                ", tableName=" + tableName +
                ", columns=" + columns +
                ", type=" + type +
                ", scheduleType=" + scheduleType +
                ", properties=" + properties +
                ", status=" + status +
                ", workTime=" + workTime +
                ", reason='" + reason + '\'' +
                ", nextCollectTime=" + nextCollectTime +
                ", collectIntervalUsed=" + collectIntervalUsed +
                '}';
    }
}
