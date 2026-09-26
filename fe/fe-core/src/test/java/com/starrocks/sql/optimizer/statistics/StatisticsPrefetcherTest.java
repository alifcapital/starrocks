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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StatisticsPrefetcherTest {
    private boolean oldSync;
    private ConnectContext oldContext;
    private ConnectContext context;
    private OptimizerContext optimizerContext;
    private final Map<String, List<String>> requests = new LinkedHashMap<>();
    private final List<String> mcvs = new ArrayList<>();
    private final List<ExternalStatisticsRequest> scopedRequests = new ArrayList<>();
    private int nextColumnId;

    @BeforeAll
    static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @BeforeEach
    void beforeEach() {
        oldSync = Config.enable_sync_statistics_load;
        oldContext = ConnectContext.get();
        Config.enable_sync_statistics_load = true;
        context = UtFrameUtils.createDefaultCtx();
        context.setThreadLocalInfo();
        optimizerContext = OptimizerFactory.initContext(context, new ColumnRefFactory());
        StatisticStorage storage = new StatisticStorage() {
            @Override
            public void addColumnStatistic(Table table, String column, ColumnStatistic value) {
                throw new AssertionError("Prefetch must not write statistics");
            }

            @Override
            public ColumnStatistic getColumnStatistic(Table table, String column) {
                throw new AssertionError("Prefetch must not call a blocking getter");
            }

            @Override
            public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                throw new AssertionError("Prefetch must not call a blocking getter");
            }

            @Override
            public void prefetchConnectorTableStatistics(Table table, List<String> columns) {
                Assertions.assertNull(requests.put(table.getUUID(), columns), "One batch per source table");
            }

            @Override
            public CompletableFuture<ExternalStatisticsAggregate> loadExternalStatistics(ExternalStatisticsRequest request) {
                scopedRequests.add(request);
                return new CompletableFuture<>();
            }

            @Override
            public void prefetchExternalMcvStatistics(Table table) {
                mcvs.add(table.getUUID());
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StatisticStorage getStatisticStorage() {
                return storage;
            }
        };
    }

    @AfterEach
    void afterEach() {
        Config.enable_sync_statistics_load = oldSync;
        if (oldContext == null) {
            ConnectContext.remove();
        } else {
            oldContext.setThreadLocalInfo();
        }
    }

    private OptExpression scan(String uuid, boolean external, String... names) {
        Table table = mock(Table.class);
        when(table.isAnalyzableExternalTable()).thenReturn(external);
        when(table.getUUID()).thenReturn(uuid);
        LogicalScanOperator scan = mock(LogicalScanOperator.class);
        when(scan.getTable()).thenReturn(table);
        Map<ColumnRefOperator, Column> columns = new LinkedHashMap<>();
        for (String name : names) {
            columns.put(new ColumnRefOperator(++nextColumnId, IntegerType.INT, name, true), new Column(name, IntegerType.INT));
        }
        when(scan.getColRefToColumnMetaMap()).thenReturn(columns);
        return OptExpression.create(scan);
    }

    @Test
    void mergesSelfJoinColumnsAndStartsAllTables() {
        OptExpression root = OptExpression.create(new LogicalTreeAnchorOperator(),
                scan("iceberg.db.t.id1", true, "a", "b"), scan("iceberg.db.u.id2", true, "x"),
                scan("iceberg.db.t.id1", true, "b", "c"), scan("native", false, "ignored"));
        StatisticsPrefetcher.prefetch(root, optimizerContext);
        Assertions.assertEquals(Map.of("iceberg.db.t.id1", List.of("a", "b", "c"),
                "iceberg.db.u.id2", List.of("x")), requests);
        Assertions.assertEquals(List.of("iceberg.db.t.id1", "iceberg.db.u.id2"), mcvs);
    }

    @Test
    void scopedPrefetchStartsBothTablesWithoutWaitingOrLoadingGlobalBasics() {
        context.getSessionVariable().setCboEnablePartitionAwareExternalStatistics(true);
        OptExpression first = scan("iceberg.db.t.id1", true, "a", "b");
        OptExpression second = scan("iceberg.db.u.id2", true, "x");
        OptExpression alias = scan("iceberg.db.t.id1", true, "b", "c");
        for (OptExpression expression : List.of(first, second, alias)) {
            Table table = ((LogicalScanOperator) expression.getOp()).getTable();
            when(table.getId()).thenReturn(123456L);
            when(table.isIcebergTable()).thenReturn(true);
            when(table.isUnPartitioned()).thenReturn(true);
        }
        OptExpression tree = OptExpression.create(new LogicalTreeAnchorOperator(), first, second, alias);
        StatisticsPrefetcher.prefetch(tree, optimizerContext);
        // Neither future completes: both scans must have been scheduled before any estimator waits.
        Assertions.assertEquals(List.of(
                new ExternalStatisticsRequest("iceberg.db.t.id1", List.of(), List.of("a", "b", "c"), true, true),
                new ExternalStatisticsRequest("iceberg.db.u.id2", List.of(), List.of("x"), true, true)), scopedRequests);
        Assertions.assertTrue(requests.isEmpty());
        StatisticsPrefetcher.prefetch(tree, optimizerContext);
        Assertions.assertEquals(2, scopedRequests.size(), "Repeated optimizer phases reuse query-local futures");
    }

    @Test
    void incrementalIcebergScansDoNotPrefetchWholePartitionOrMcvStatistics() {
        OptExpression first = scan("iceberg.db.t.id1", true, "a");
        OptExpression second = scan("iceberg.db.u.id2", true, "x");
        for (OptExpression expression : List.of(first, second)) {
            LogicalScanOperator scan = (LogicalScanOperator) expression.getOp();
            when(scan.getTable().isIcebergTable()).thenReturn(true);
            when(scan.getTvrVersionRange()).thenReturn(TvrTableDelta.of(122, 123));
        }
        when(((LogicalScanOperator) first.getOp()).getTable().isUnPartitioned()).thenReturn(true);
        for (boolean scoped : List.of(false, true)) {
            context.getSessionVariable().setCboEnablePartitionAwareExternalStatistics(scoped);
            StatisticsPrefetcher.prefetch(OptExpression.create(new LogicalTreeAnchorOperator(), first, second), optimizerContext);
        }
        Assertions.assertTrue(requests.isEmpty());
        Assertions.assertTrue(scopedRequests.isEmpty());
        Assertions.assertTrue(mcvs.isEmpty());
    }

    @Test
    void leavesAsynchronousLoadingDemandDriven() {
        Config.enable_sync_statistics_load = false;
        StatisticsPrefetcher.prefetch(scan("iceberg.db.t.id1", true, "a"), optimizerContext);
        Assertions.assertTrue(requests.isEmpty());
        Assertions.assertTrue(mcvs.isEmpty());
    }

    @Test
    void skipsStatisticsQueriesAndCollectionJobs() {
        OptExpression scan = scan("iceberg.db.t.id1", true, "a");
        context.setStatisticsConnection(true);
        StatisticsPrefetcher.prefetch(scan, optimizerContext);
        context.setStatisticsConnection(false);
        context.setStatisticsJob(true);
        StatisticsPrefetcher.prefetch(scan, optimizerContext);
        Assertions.assertTrue(requests.isEmpty());
        Assertions.assertTrue(mcvs.isEmpty());
    }

    @Test
    void respectsDisabledMcvEstimation() {
        context.getSessionVariable().setCboEnableMcvEstimate(false);
        StatisticsPrefetcher.prefetch(scan("iceberg.db.t.id1", true, "a"), optimizerContext);
        Assertions.assertEquals(1, requests.size());
        Assertions.assertTrue(mcvs.isEmpty());
    }
}
