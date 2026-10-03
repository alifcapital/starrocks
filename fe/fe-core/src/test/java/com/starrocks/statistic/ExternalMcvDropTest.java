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

import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StatisticsType;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ExternalMcvDropTest {
    private static ConnectContext context;
    private static Table table;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();
        context = UtFrameUtils.createDefaultCtx();
        context.setQueryId(UUIDUtil.genUUID());
        ConnectorPlanTestBase.mockHiveCatalog(context);
        table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, "hive0", "tpch", "customer");
    }

    @AfterAll
    public static void afterAll() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static ExternalMcvStatsMeta meta(List<String> columns) {
        ExternalMcvStatsMeta meta = new ExternalMcvStatsMeta("hive0", "tpch", "customer", columns,
                StatsConstants.AnalyzeType.FULL, List.of(StatisticsType.MCV), LocalDateTime.now(), Map.of());
        meta.setTableUUID(table.getUUID());
        return meta;
    }

    @Test
    public void dropOneGroupPreservesOtherGroupsAndOnlyExpiresMcvCache() throws Exception {
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        AnalyzeMgr manager = state.getAnalyzeMgr();
        ExternalMcvStatsMeta pair = meta(List.of("c_name", "c_phone"));
        ExternalMcvStatsMeta singleton = meta(List.of("c_name"));
        ExternalMcvStatsMeta superset = meta(List.of("c_name", "c_phone", "c_custkey"));
        for (ExternalMcvStatsMeta entry : List.of(pair, singleton, superset)) {
            manager.replayAddExternalMcvStatsMeta(entry);
        }
        List<List<String>> deleted = new ArrayList<>();
        List<String> deletedTables = new ArrayList<>();
        new MockUp<StatisticExecutor>() {
            @Mock
            public void dropExternalMcvStatistics(ConnectContext ctx, String uuid, List<String> columns) {
                Assertions.assertEquals(table.getUUID(), uuid);
                deleted.add(columns);
            }

            @Mock
            public void dropExternalMcvStatistics(ConnectContext ctx, String uuid) {
                deletedTables.add(uuid);
            }
        };
        StatisticStorage previous = state.getStatisticStorage();
        StatisticStorage storage = mock(StatisticStorage.class);
        state.setStatisticStorage(storage);
        try {
            context.getState().reset();
            StatementBase statement = UtFrameUtils.parseStmtWithNewParser(
                    "DROP MCV STATS hive0.tpch.customer (C_PHONE, c_name)", context);
            StmtExecutor.newInternalExecutor(context, statement).execute();
            Assertions.assertFalse(context.getState().isError(), context.getState().getErrorMessage());
            Assertions.assertEquals(List.of(List.of("c_phone", "c_name")), deleted);
            Assertions.assertFalse(manager.getExternalMcvStatsMetaMap().containsKey(AnalyzeMgr.ExternalMcvStatsKey.of(pair)));
            Assertions.assertSame(singleton,
                    manager.getExternalMcvStatsMetaMap().get(AnalyzeMgr.ExternalMcvStatsKey.of(singleton)));
            Assertions.assertSame(superset,
                    manager.getExternalMcvStatsMetaMap().get(AnalyzeMgr.ExternalMcvStatsKey.of(superset)));
            // Repeating a group drop is harmless; omitting the list still removes all MCV groups.
            StmtExecutor.newInternalExecutor(context, statement).execute();
            Assertions.assertFalse(context.getState().isError(), context.getState().getErrorMessage());
            Assertions.assertEquals(2, deleted.size());
            Assertions.assertTrue(deletedTables.isEmpty());
            StatementBase dropAll = UtFrameUtils.parseStmtWithNewParser("DROP MCV STATS hive0.tpch.customer", context);
            StmtExecutor.newInternalExecutor(context, dropAll).execute();
            Assertions.assertFalse(context.getState().isError(), context.getState().getErrorMessage());
            Assertions.assertEquals(List.of(table.getUUID()), deletedTables);
            for (ExternalMcvStatsMeta entry : List.of(pair, singleton, superset)) {
                Assertions.assertFalse(manager.getExternalMcvStatsMetaMap()
                        .containsKey(AnalyzeMgr.ExternalMcvStatsKey.of(entry)));
            }
            verify(storage, times(3)).expireExternalMcvStatistics(table.getUUID());
            verifyNoMoreInteractions(storage);
        } finally {
            state.setStatisticStorage(previous);
            for (ExternalMcvStatsMeta entry : List.of(pair, singleton, superset)) {
                manager.replayRemoveExternalMcvStatsMeta(entry);
            }
        }
    }

    @Test
    public void plainDropPreservesMcvAndHistograms() throws Exception {
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        AnalyzeMgr manager = state.getAnalyzeMgr();
        ExternalMcvStatsMeta mcv = meta(List.of("c_name", "c_phone"));
        ExternalBasicStatsMeta basic = new ExternalBasicStatsMeta("hive0", "tpch", "customer", List.of("c_name"),
                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), Map.of());
        ExternalHistogramStatsMeta histogram = new ExternalHistogramStatsMeta("hive0", "tpch", "customer", "c_name",
                StatsConstants.AnalyzeType.HISTOGRAM, LocalDateTime.now(), Map.of());
        manager.replayAddExternalBasicStatsMeta(basic);
        manager.replayAddExternalMcvStatsMeta(mcv);
        manager.replayAddExternalHistogramStatsMeta(histogram);
        List<String> deletedBasic = new ArrayList<>();
        new MockUp<StatisticExecutor>() {
            @Mock
            public void dropExternalTableStatistics(ConnectContext ctx, String uuid) {
                deletedBasic.add(uuid);
            }

            @Mock
            public void dropExternalMcvStatistics(ConnectContext ctx, String uuid) {
                Assertions.fail("Plain DROP STATS must not delete MCV data");
            }

            @Mock
            public void dropExternalHistogram(ConnectContext ctx, String uuid, List<String> columns) {
                Assertions.fail("Plain DROP STATS must not delete histogram data");
            }
        };
        StatisticStorage previous = state.getStatisticStorage();
        StatisticStorage storage = mock(StatisticStorage.class);
        state.setStatisticStorage(storage);
        try {
            context.getState().reset();
            StatementBase statement = UtFrameUtils.parseStmtWithNewParser("DROP STATS hive0.tpch.customer", context);
            StmtExecutor.newInternalExecutor(context, statement).execute();
            Assertions.assertFalse(context.getState().isError(), context.getState().getErrorMessage());
            Assertions.assertEquals(List.of(table.getUUID()), deletedBasic);
            Assertions.assertNull(manager.getExternalTableBasicStatsMeta("hive0", "tpch", "customer"));
            Assertions.assertSame(mcv, manager.getExternalMcvStatsMetaMap().get(AnalyzeMgr.ExternalMcvStatsKey.of(mcv)));
            Assertions.assertTrue(manager.hasExternalMcvStatsMeta(table));
            Assertions.assertTrue(manager.getExternalHistogramStatsMetaMap().containsValue(histogram));
            verify(storage).expireConnectorTableColumnStatistics(table,
                    table.getBaseSchema().stream().map(com.starrocks.catalog.Column::getName).toList());
            verifyNoMoreInteractions(storage);
        } finally {
            state.setStatisticStorage(previous);
            manager.replayRemoveExternalBasicStatsMeta(basic);
            manager.replayRemoveExternalMcvStatsMeta(mcv);
            manager.replayRemoveExternalHistogramStatsMeta(histogram);
        }
    }

    @Test
    public void failedDeletePreservesMetadata() {
        AnalyzeMgr manager = new AnalyzeMgr();
        ExternalMcvStatsMeta pair = meta(List.of("c_name", "c_phone"));
        manager.replayAddExternalMcvStatsMeta(pair);
        new MockUp<StatisticExecutor>() {
            @Mock
            public void dropExternalMcvStatistics(ConnectContext ctx, String uuid, List<String> columns) {
                throw new IllegalStateException("storage unavailable");
            }
        };
        Assertions.assertThrows(IllegalStateException.class, () -> manager.dropExternalMcvStatsMetaAndData(context,
                new TableName("hive0", "tpch", "customer"), table, List.of("c_name", "c_phone")));
        Assertions.assertSame(pair, manager.getExternalMcvStatsMetaMap().get(AnalyzeMgr.ExternalMcvStatsKey.of(pair)));
    }

    @Test
    public void deleteReportsExecutionErrorInsteadOfSuccess() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() {
                ctx.getState().setError("delete failed");
            }
        };
        Assertions.assertThrows(IllegalStateException.class, () -> new StatisticExecutor()
                .dropExternalMcvStatistics(ctx, table.getUUID(), List.of("c_name")));
    }
}
