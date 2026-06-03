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

import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

class StatisticUtilsTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        UtFrameUtils.createMinStarRocksCluster();
        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
        UtFrameUtils.addMockBackend(123);
        UtFrameUtils.addMockBackend(124);
    }

    @Test
    void alterSystemTableReplicationNumIfNecessary() {
        // 1. Has sufficient backends
        new MockUp<SystemInfoService>() {
            @Mock
            public int getRetainedBackendNumber() {
                return 100;
            }
        };
        final String tableName = "column_statistics";
        Assertions.assertTrue(StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName));
        Assertions.assertFalse(StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName));
        Assertions.assertEquals("3",
                starRocksAssert.getTable(StatsConstants.STATISTICS_DB_NAME, tableName).getProperties().get(
                        "replication_num"));

        // 2. change default_replication_num
        Config.default_replication_num = 1;
        Assertions.assertTrue(StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName));
        Assertions.assertFalse(StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName));
        Assertions.assertEquals("1",
                starRocksAssert.getTable(StatsConstants.STATISTICS_DB_NAME, tableName).getProperties().get(
                        "replication_num"));
        Config.default_replication_num = 3;
        Assertions.assertTrue(StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName));

        // 3. Has no sufficient backends
        new MockUp<SystemInfoService>() {
            @Mock
            public int getRetainedBackendNumber() {
                return 1;
            }
        };
        Assertions.assertTrue(StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName));
        Assertions.assertFalse(StatisticUtils.alterSystemTableReplicationNumIfNecessary(tableName));
        Assertions.assertEquals("1",
                starRocksAssert.getTable(StatsConstants.STATISTICS_DB_NAME, tableName).getProperties().get(
                        "replication_num"));
    }

    @Test
    void splayNextCollectTime() {
        LocalDateTime from = LocalDateTime.of(2026, 6, 3, 12, 0, 0);
        long week = 7 * 24 * 3600;
        for (int i = 0; i < 100; i++) {
            LocalDateTime next = StatisticUtils.splayNextCollectTime(from, week);
            Assertions.assertTrue(next.isAfter(from));
            Assertions.assertFalse(next.isAfter(from.plusSeconds(week)));
        }
        // non-positive interval: no splay
        Assertions.assertEquals(from, StatisticUtils.splayNextCollectTime(from, 0));
        Assertions.assertEquals(from, StatisticUtils.splayNextCollectTime(from, -1));
    }

    @Test
    void externalAnalyzeJobScheduleFields() {
        // a job written before the staggered schedule existed: fields default to null/0
        String oldJson = "{\"clazz\":\"ExternalAnalyzeJob\",\"id\":7,\"catalogName\":\"c\",\"dbName\":\"d\","
                + "\"tableName\":\"t\",\"type\":\"FULL\",\"scheduleType\":\"SCHEDULE\",\"status\":\"PENDING\"}";
        ExternalAnalyzeJob job = (ExternalAnalyzeJob) GsonUtils.GSON.fromJson(oldJson, AnalyzeJob.class);
        Assertions.assertNull(job.getNextCollectTime());
        Assertions.assertEquals(0, job.getCollectIntervalUsed());

        // the schedule survives a gson roundtrip (the journal/image serialization path)
        job.setNextCollectTime(LocalDateTime.of(2026, 6, 10, 3, 0, 0));
        job.setCollectIntervalUsed(604800);
        ExternalAnalyzeJob copy = (ExternalAnalyzeJob) GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(job, AnalyzeJob.class), AnalyzeJob.class);
        Assertions.assertEquals(job.getNextCollectTime(), copy.getNextCollectTime());
        Assertions.assertEquals(604800, copy.getCollectIntervalUsed());
    }
}
