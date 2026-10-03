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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.sample.ColumnSampleManager;
import com.starrocks.statistic.sample.ColumnStats;
import com.starrocks.statistic.sample.DistributionColumnStats;
import com.starrocks.statistic.sample.SampleInfo;
import com.starrocks.statistic.sample.TabletSampleManager;
import com.starrocks.statistic.sample.TabletStats;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NativeSampleNdvTest extends PlanTestBase {
    @Test
    public void sampleStorageSurvivesConfigChangeAndMetadataReplay() {
        boolean before = Config.statistic_use_meta_statistics;
        try {
            for (boolean sampleTable : List.of(true, false)) {
                ColumnStatsMeta meta = new ColumnStatsMeta("v1", StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now());
                meta.setSampleStatisticsTable(sampleTable);
                ColumnStatsMeta restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(meta), ColumnStatsMeta.class);
                for (boolean flag : List.of(true, false)) {
                    Config.statistic_use_meta_statistics = flag;
                    Assertions.assertEquals(sampleTable, restored.usesSampleStatisticsTable());
                }
            }
            ColumnStatsMeta legacy = new ColumnStatsMeta("v1", StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now());
            Config.statistic_use_meta_statistics = true;
            Assertions.assertFalse(legacy.usesSampleStatisticsTable());
            Config.statistic_use_meta_statistics = false;
            Assertions.assertTrue(legacy.usesSampleStatisticsTable());
            ColumnStatsMeta full = new ColumnStatsMeta("v1", StatsConstants.AnalyzeType.FULL, LocalDateTime.now());
            Assertions.assertFalse(full.usesSampleStatisticsTable());
        } finally {
            Config.statistic_use_meta_statistics = before;
        }
    }

    @Test
    public void nativeSampleAndFullUseTheirOwnStorage() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        List<String> columns = List.of("v1", "v2");
        List<Type> types = columns.stream().map(n -> table.getColumn(n).getType()).toList();
        for (StatsConstants.AnalyzeType mode : List.of(StatsConstants.AnalyzeType.SAMPLE, StatsConstants.AnalyzeType.FULL)) {
            HyperStatisticsCollectJob job = new HyperStatisticsCollectJob(db, table, table.getAllPartitionIds(),
                    columns, types, mode, StatsConstants.ScheduleType.ONCE, Map.of(), true);
            Assertions.assertEquals(mode == StatsConstants.AnalyzeType.SAMPLE, job.usesSampleStatisticsTable("v1"));
        }
    }

    @Test
    public void metaSampleCollectsFrequencySqlWithMetaBounds() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        List<String> columns = List.of("v1", "v2");
        List<Type> types = columns.stream().map(n -> table.getColumn(n).getType()).toList();
        SampleInfo info = new SampleInfo(
                1, 100, 10000, List.of(new TabletStats(100L, 1L, 10000)),
                List.of(), List.of(), List.of());
        new MockUp<TabletSampleManager>() {
            @Mock
            public SampleInfo generateSampleInfo() {
                return info;
            }
        };
        List<String> sqls = new ArrayList<>();
        new MockUp<StatisticsCollectJob>() {
            @Mock
            protected void collectStatisticSync(String sql, ConnectContext context, AnalyzeStatus status) {
                sqls.add(sql);
            }
        };
        NativeAnalyzeStatus status = new NativeAnalyzeStatus(9898, db.getId(), table.getId(), columns,
                StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.ONCE, Map.of(), LocalDateTime.now());
        HyperStatisticsCollectJob job = new HyperStatisticsCollectJob(db, table, table.getAllPartitionIds(),
                columns, types, StatsConstants.AnalyzeType.SAMPLE, StatsConstants.ScheduleType.ONCE, Map.of(), true);
        job.collect(connectContext, status);
        Assertions.assertEquals(1, sqls.size());
        String sql = sqls.get(0);
        Assertions.assertTrue(sql.contains(StatsConstants.SAMPLE_STATISTICS_TABLE_NAME));
        Assertions.assertTrue(sql.contains("GROUP BY t0.column_key"));
        Assertions.assertTrue(sql.contains("[_META_]"));
        Assertions.assertTrue(sql.contains("MAX(meta_bounds.total_rows)"));
        Assertions.assertTrue(sql.contains("meta_bounds.max_value"));
        Assertions.assertEquals(100, status.getProgress());
        SqlParser.parse(sql, connectContext.getSessionVariable());
    }

    @Test
    public void partitionedHashKeyDoesNotAssumeDisjointSupportsAndWideColumnsStayIsolated() throws Exception {
        starRocksAssert.withTable("CREATE TABLE ndv_sample_partition_guard "
                + "(p INT, id BIGINT, wide VARCHAR(4096), small VARCHAR(16)) DUPLICATE KEY(p,id) "
                + "PARTITION BY RANGE(p) (PARTITION p0 VALUES LESS THAN ('1'), PARTITION p1 VALUES LESS THAN ('2')) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES('replication_num'='1')");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable("test", "ndv_sample_partition_guard");
        List<String> columns = List.of("p", "id", "wide", "small");
        List<Type> types = columns.stream().map(n -> table.getColumn(n).getType()).toList();
        long old = Config.statistics_large_string_column_merge_threshold;
        try {
            Config.statistics_large_string_column_merge_threshold = 512;
            ColumnSampleManager manager =
                    ColumnSampleManager.init(columns, types, table, new SampleInfo());
            for (List<ColumnStats> batch : manager.splitPrimitiveTypeStats()) {
                for (ColumnStats col : batch) {
                    Assertions.assertFalse(col instanceof DistributionColumnStats);
                    if (col.getColumnName().equals("wide")) {
                        Assertions.assertEquals(1, batch.size());
                    }
                }
            }
        } finally {
            Config.statistics_large_string_column_merge_threshold = old;
            starRocksAssert.dropTable("ndv_sample_partition_guard");
        }
    }

    @Test
    public void loaderSeparatesScalarSampleFromHllAndRejectsStalePartitionHll() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        BasicStatsMeta before = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getTableBasicStatsMeta(table.getId());
        BasicStatsMeta meta = new BasicStatsMeta(db.getId(), table.getId(), List.of("v1", "v2", "v3"),
                StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(), Map.of());
        for (String column : List.of("v1", "v2", "v3")) {
            ColumnStatsMeta cm = new ColumnStatsMeta(column, StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now());
            cm.setSampleStatisticsTable(column.equals("v1"));
            meta.addColumnStatsMeta(cm);
        }
        List<String> sqls = new ArrayList<>();
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<TStatisticData> executeStatisticDQL(ConnectContext context, String sql) {
                sqls.add(sql);
                return List.of();
            }
        };
        try {
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddBasicStatsMeta(meta);
            StatisticExecutor executor = new StatisticExecutor();
            executor.queryStatisticSync(connectContext, db.getId(), table.getId(), List.of("v1", "v2", "v3"));
            Assertions.assertEquals(2, sqls.size());
            Assertions.assertTrue(sqls.stream().anyMatch(s ->
                    s.contains(StatsConstants.SAMPLE_STATISTICS_TABLE_NAME) && s.contains("'v1'")));
            Assertions.assertTrue(sqls.stream().anyMatch(s -> s.contains("column_statistics") && !s.contains("'v1'")));
            sqls.clear();
            executor.queryPartitionLevelColumnNDV(connectContext, table.getId(), List.of(), List.of("v1"));
            Assertions.assertTrue(sqls.isEmpty());
            executor.queryPartitionLevelColumnNDV(connectContext, table.getId(), List.of(), List.of("v1", "v2"));
            Assertions.assertEquals(1, sqls.size());
            Assertions.assertFalse(sqls.get(0).contains("'v1'"));
        } finally {
            if (before != null) {
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddBasicStatsMeta(before);
            } else {
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayRemoveBasicStatsMeta(meta);
            }
        }
    }
}
