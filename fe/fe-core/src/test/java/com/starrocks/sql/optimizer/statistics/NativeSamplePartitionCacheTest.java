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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.ColumnStatsMeta;
import com.starrocks.statistic.StatsConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class NativeSamplePartitionCacheTest extends PlanTestBase {
    @Test
    public void recollectedScalarSampleCannotUseWarmOldPartitionHll() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        BasicStatsMeta before = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getTableBasicStatsMeta(table.getId());
        BasicStatsMeta meta = new BasicStatsMeta(db.getId(), table.getId(), List.of("v1", "v2"),
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), Map.of());
        ColumnStatsMeta sample = new ColumnStatsMeta("v1", StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now());
        sample.setSampleStatisticsTable(true);
        meta.addColumnStatsMeta(sample);
        meta.addColumnStatsMeta(new ColumnStatsMeta("v2", StatsConstants.AnalyzeType.FULL, LocalDateTime.now()));
        CachedStatisticStorage storage = new CachedStatisticStorage();
        PartitionStats oldPartition = new PartitionStats();
        oldPartition.getDistinctCount().put(1L, 12.0);
        for (String col : List.of("v1", "v2")) {
            storage.partitionStatistics.put(new ColumnStatsCacheKey(table.getId(), col),
                    CompletableFuture.completedFuture(Optional.of(oldPartition)));
        }
        try {
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddBasicStatsMeta(meta);
            Assertions.assertTrue(storage.getColumnNDVForPartitions(table, List.of("v1")).isEmpty());
            Map<String, PartitionStats> result = storage.getColumnNDVForPartitions(table, List.of("v1", "v2"));
            Assertions.assertEquals(List.of("v2"), List.copyOf(result.keySet()));
            Assertions.assertSame(oldPartition, result.get("v2"));
        } finally {
            if (before != null) {
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddBasicStatsMeta(before);
            } else {
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayRemoveBasicStatsMeta(meta);
            }
        }
    }
}
