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

package com.starrocks.connector.statistics;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.ColumnStatsMeta;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.UtFrameUtils;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class ScopedStatisticsTriggerAnalyzeTest {
    @BeforeAll
    static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @Test
    void selectedPartitionsUseCollectionMetadataAndDoNotMakeLargeTablesLookSmall(@Mocked Table table) {
        new Expectations() {
            {
                table.isAnalyzableExternalTable();
                result = true;
                table.getName();
                result = "t";
            }
        };
        ExternalBasicStatsMeta meta = new ExternalBasicStatsMeta();
        LocalDateTime now = LocalDateTime.now();
        meta.addColumnStatsMeta(new ColumnStatsMeta("fresh", StatsConstants.AnalyzeType.FULL, now));
        meta.addColumnStatsMeta(new ColumnStatsMeta("middle", StatsConstants.AnalyzeType.FULL, now.minusSeconds(120)));
        meta.addColumnStatsMeta(new ColumnStatsMeta("old", StatsConstants.AnalyzeType.FULL, now.minusHours(2)));
        Set<String> queued = new HashSet<>();
        new MockUp<StatisticsUtils>() {
            @Mock
            public Triple<String, Database, Table> getTableTripleByUUID(ConnectContext ctx, String uuid) {
                return Triple.of("iceberg", new Database(1, "db"), table);
            }
        };
        new MockUp<AnalyzeMgr>() {
            @Mock
            public ExternalBasicStatsMeta getExternalTableBasicStatsMeta(String catalog, String db, String name) {
                return meta;
            }
        };
        new MockUp<ConnectorAnalyzeTaskQueue>() {
            @Mock
            public boolean addPendingTask(String uuid, ConnectorAnalyzeTask task) {
                queued.addAll(task.getColumns());
                return true;
            }
        };
        long small = Config.connector_table_query_trigger_analyze_small_table_interval;
        long large = Config.connector_table_query_trigger_analyze_large_table_interval;
        try {
            Config.connector_table_query_trigger_analyze_small_table_interval = 60;
            Config.connector_table_query_trigger_analyze_large_table_interval = 3600;
            ConnectorTableTriggerAnalyzeMgr manager = new ConnectorTableTriggerAnalyzeMgr();
            ColumnStatistic known = ColumnStatistic.builder().setDistinctValuesCount(10).build();
            Map<String, ColumnStatistic> columns = Map.of("fresh", ColumnStatistic.unknown(), "middle", known,
                    "old", known, "missing", known);
            manager.checkAndUpdateScopedTableStats("iceberg.db.t.uuid", columns, 1, false);
            Assertions.assertEquals(Set.of("old", "missing"), queued);
            queued.clear();
            manager.checkAndUpdateScopedTableStats("iceberg.db.t.uuid", columns, 1, true);
            Assertions.assertEquals(Set.of("fresh", "middle", "old", "missing"), queued);
        } finally {
            Config.connector_table_query_trigger_analyze_small_table_interval = small;
            Config.connector_table_query_trigger_analyze_large_table_interval = large;
        }
    }
}
