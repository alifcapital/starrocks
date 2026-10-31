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

package com.starrocks.connector.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.datacache.DataCacheSelectExecutor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class IcebergMetadataRefreshFooterPrefetcherTest {
    private boolean previousMultiWarehouse;
    private ConnectContext caller;
    private ExecutorService executor;
    private final Deque<Runnable> queue = new ArrayDeque<>();

    @BeforeEach
    public void setUp() {
        previousMultiWarehouse = Config.enable_multi_warehouse;
        Config.enable_multi_warehouse = true;
        caller = UtFrameUtils.createDefaultCtx();
        caller.getSessionVariable().setEnableIcebergMetadataRefreshFooterPrefetch(true);
        caller.getSessionVariable().setWarehouseName("footer_test_wh");
        executor = Mockito.mock(ExecutorService.class);
        Mockito.doAnswer(invocation -> {
            queue.addLast(invocation.getArgument(0));
            return null;
        }).when(executor).execute(Mockito.any(Runnable.class));
    }

    @AfterEach
    public void tearDown() {
        Config.enable_multi_warehouse = previousMultiWarehouse;
        ConnectContext.remove();
    }

    private void warmup() {
        warmup(100L);
    }

    private void warmup(long baseSnapshotId) {
        IcebergMetadataRefreshFooterPrefetcher.warmup("iceberg", "db", "table", baseSnapshotId, caller, executor);
    }

    // Records the base snapshot id every cache select of the prefetcher runs with.
    private static List<Long> recordBases(MockedStatic<DataCacheSelectExecutor> cacheSelect, Runnable duringRun) {
        List<Long> bases = new ArrayList<>();
        cacheSelect.when(() -> DataCacheSelectExecutor.cacheSelect(Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> {
                    ConnectContext ctx = invocation.getArgument(1);
                    bases.add(ctx.getSessionVariable().getCacheSelectFooterBaseSnapshotId());
                    if (bases.size() == 1 && duringRun != null) {
                        duringRun.run();
                    }
                    return null;
                });
        return bases;
    }

    @Test
    public void testDisabledDoesNotQueueWork() {
        caller.getSessionVariable().setEnableIcebergMetadataRefreshFooterPrefetch(false);
        warmup();
        IcebergMetadataRefreshFooterPrefetcher.warmup("iceberg", "db", "table", 100L, null, executor);
        Mockito.verifyNoInteractions(executor);
    }

    @Test
    public void testWarmupCarriesBaseSnapshotOnlyOnItsOwnSessionVariable() {
        try (MockedStatic<DataCacheSelectExecutor> cacheSelect = Mockito.mockStatic(DataCacheSelectExecutor.class)) {
            List<Long> bases = recordBases(cacheSelect, null);
            warmup(42L);
            queue.removeFirst().run();
            Assertions.assertEquals(List.of(42L), bases);
            Assertions.assertEquals(SessionVariable.CACHE_SELECT_FOOTER_BASE_NOT_SET,
                    caller.getSessionVariable().getCacheSelectFooterBaseSnapshotId());
        }
    }

    @Test
    public void testWarmupWithoutBaseWarmsEveryFile() {
        try (MockedStatic<DataCacheSelectExecutor> cacheSelect = Mockito.mockStatic(DataCacheSelectExecutor.class)) {
            List<Long> bases = recordBases(cacheSelect, null);
            warmup(SessionVariable.CACHE_SELECT_FOOTER_BASE_NOT_SET);
            queue.removeFirst().run();
            Assertions.assertEquals(List.of(SessionVariable.CACHE_SELECT_FOOTER_BASE_NOT_SET), bases);
        }
    }

    @Test
    public void testPendingRequestsMergeIntoTheOldestBase() {
        try (MockedStatic<DataCacheSelectExecutor> cacheSelect = Mockito.mockStatic(DataCacheSelectExecutor.class)) {
            List<Long> bases = recordBases(cacheSelect, null);
            warmup(5L);
            warmup(6L);
            warmup(7L);
            Assertions.assertEquals(1, queue.size());
            queue.removeFirst().run();
            // The files added by the refreshes that queued 5 and 6 are still cold, so the one run covers them.
            Assertions.assertEquals(List.of(5L), bases);
            Assertions.assertTrue(queue.isEmpty());
        }
    }

    @Test
    public void testPendingRequestForEveryFileAbsorbsTheOther() {
        long everyFile = SessionVariable.CACHE_SELECT_FOOTER_BASE_NOT_SET;
        for (long[] order : List.of(new long[] {everyFile, 6L}, new long[] {6L, everyFile})) {
            try (MockedStatic<DataCacheSelectExecutor> cacheSelect =
                         Mockito.mockStatic(DataCacheSelectExecutor.class)) {
                List<Long> bases = recordBases(cacheSelect, null);
                warmup(order[0]);
                warmup(order[1]);
                queue.removeFirst().run();
                Assertions.assertEquals(List.of(everyFile), bases);
                Assertions.assertTrue(queue.isEmpty());
            }
        }
    }

    @Test
    public void testRequestQueuedDuringRunKeepsItsOwnBase() {
        try (MockedStatic<DataCacheSelectExecutor> cacheSelect = Mockito.mockStatic(DataCacheSelectExecutor.class)) {
            List<Long> bases = recordBases(cacheSelect, () -> {
                warmup(6L);
                warmup(7L);
            });
            warmup(5L);
            queue.removeFirst().run();
            // The running warmup may have planned already, so the request of 6 is not merged into it.
            Assertions.assertEquals(List.of(5L), bases);
            Assertions.assertEquals(1, queue.size());
            queue.removeFirst().run();
            Assertions.assertEquals(List.of(5L, 6L), bases);
            Assertions.assertTrue(queue.isEmpty());
        }
    }

    @Test
    public void testCacheSelectSubContextKeepsFooterVariables() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.getSessionVariable().setCacheSelectFooterOnly(true);
        ctx.getSessionVariable().setCacheSelectFooterBaseSnapshotId(42L);
        DataCacheSelectStatement stmt = Mockito.mock(DataCacheSelectStatement.class);
        Mockito.when(stmt.getCatalog()).thenReturn("iceberg");

        SessionVariable sub = DataCacheSelectExecutor.buildCacheSelectConnectContext(stmt, ctx, true)
                .getSessionVariable();

        Assertions.assertTrue(sub.isCacheSelectFooterOnly());
        Assertions.assertEquals(42L, sub.getCacheSelectFooterBaseSnapshotId());
        Assertions.assertNotSame(ctx.getSessionVariable(), sub);
    }

    @Test
    public void testFooterBaseVariableDefaultsToNotSet() {
        Assertions.assertEquals(SessionVariable.CACHE_SELECT_FOOTER_BASE_NOT_SET,
                new SessionVariable().getCacheSelectFooterBaseSnapshotId());
        Assertions.assertFalse(new SessionVariable().isCacheSelectFooterOnly());
    }

    @Test
    public void testQueuedRefreshesCoalesceAndKeepCallerSettingsPrivate() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (MockedStatic<DataCacheSelectExecutor> cacheSelect = Mockito.mockStatic(DataCacheSelectExecutor.class)) {
            cacheSelect.when(() -> DataCacheSelectExecutor.cacheSelect(Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> {
                        DataCacheSelectStatement stmt = invocation.getArgument(0);
                        ConnectContext ctx = invocation.getArgument(1);
                        SessionVariable sv = ctx.getSessionVariable();
                        Assertions.assertEquals("iceberg", stmt.getCatalog());
                        Assertions.assertEquals("iceberg", ctx.getCurrentCatalog());
                        Assertions.assertEquals("footer_test_wh", ctx.getCurrentWarehouseName());
                        Assertions.assertTrue(sv.isCacheSelectFooterOnly());
                        Assertions.assertNotSame(caller.getSessionVariable(), sv);
                        calls.incrementAndGet();
                        return null;
                    });
            warmup();
            warmup();
            Assertions.assertEquals(1, queue.size());
            // Changing the caller after submission must not affect the queued request.
            caller.getSessionVariable().setWarehouseName("another_wh");
            queue.removeFirst().run();
            Assertions.assertEquals(1, calls.get());
            Assertions.assertFalse(caller.getSessionVariable().isCacheSelectFooterOnly());
            Assertions.assertTrue(queue.isEmpty());
        }
    }

    @Test
    public void testRefreshDuringWarmupQueuesOneMoreRun() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (MockedStatic<DataCacheSelectExecutor> cacheSelect = Mockito.mockStatic(DataCacheSelectExecutor.class)) {
            cacheSelect.when(() -> DataCacheSelectExecutor.cacheSelect(Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> {
                        if (calls.incrementAndGet() == 1) {
                            warmup();
                            warmup();
                        }
                        return null;
                    });
            warmup();
            queue.removeFirst().run();
            Assertions.assertEquals(1, calls.get());
            Assertions.assertEquals(1, queue.size());
            queue.removeFirst().run();
            Assertions.assertEquals(2, calls.get());
            Assertions.assertTrue(queue.isEmpty());
            warmup();
            queue.removeFirst().run();
            Assertions.assertEquals(3, calls.get());
        }
    }

    @Test
    public void testRejectedSubmissionDoesNotBlockLaterRefresh() throws Exception {
        Mockito.doThrow(new RejectedExecutionException()).doAnswer(invocation -> {
            queue.addLast(invocation.getArgument(0));
            return null;
        }).when(executor).execute(Mockito.any(Runnable.class));
        try (MockedStatic<DataCacheSelectExecutor> cacheSelect = Mockito.mockStatic(DataCacheSelectExecutor.class)) {
            warmup();
            Assertions.assertTrue(queue.isEmpty());
            warmup();
            Assertions.assertEquals(1, queue.size());
            queue.removeFirst().run();
            cacheSelect.verify(() -> DataCacheSelectExecutor.cacheSelect(Mockito.any(), Mockito.any()));
        }
    }

    @Test
    public void testFailedWarmupDoesNotBlockLaterRefresh() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (MockedStatic<DataCacheSelectExecutor> cacheSelect = Mockito.mockStatic(DataCacheSelectExecutor.class)) {
            cacheSelect.when(() -> DataCacheSelectExecutor.cacheSelect(Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> {
                        calls.incrementAndGet();
                        throw new IllegalStateException("test warmup failure");
                    });
            warmup();
            Assertions.assertDoesNotThrow(() -> queue.removeFirst().run());
            warmup();
            Assertions.assertDoesNotThrow(() -> queue.removeFirst().run());
            Assertions.assertEquals(2, calls.get());
        }
    }
    @Test
    public void testRefreshWithNoNewManifestsStillWarmsFooters() {
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        IcebergCatalogProperties properties = new IcebergCatalogProperties(Map.of(
                IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive",
                IcebergCatalogProperties.HIVE_METASTORE_URIS, "thrift://localhost:9083"));
        CachingIcebergCatalog catalog = new CachingIcebergCatalog("iceberg", delegate, properties, executor);
        BaseTable oldTable = tableWithSnapshot("old-metadata", 1);
        BaseTable updatedTable = tableWithSnapshot("new-metadata", 2);
        Mockito.when(delegate.getTable(caller, "db", "table")).thenReturn(updatedTable);
        Cache<CachingIcebergCatalog.IcebergTableName, Table> tables =
                Deencapsulation.getField(catalog, "tables");
        tables.put(new CachingIcebergCatalog.IcebergTableName("db", "table"), oldTable);
        // Only a table that clients query is warmed.
        Map<CachingIcebergCatalog.IcebergTableName, Long> access =
                Deencapsulation.getField(catalog, "tableLatestAccessTime");
        access.put(new CachingIcebergCatalog.IcebergTableName("db", "table"), System.currentTimeMillis());
        LoadingCache<CachingIcebergCatalog.IcebergTableName, Map<String, Partition>> partitions =
                Deencapsulation.getField(catalog, "partitionCache");
        partitions.put(new CachingIcebergCatalog.IcebergTableName("db", "table", 2), Collections.emptyMap());
        try (MockedStatic<IcebergMetadataRefreshFooterPrefetcher> prefetch =
                     Mockito.mockStatic(IcebergMetadataRefreshFooterPrefetcher.class)) {
            catalog.refreshTable("db", "table", caller, executor);
            // The table cached before the refresh had snapshot 1, so the warmup is told to cover what came after it.
            prefetch.verify(() -> IcebergMetadataRefreshFooterPrefetcher.warmup(
                    Mockito.eq("iceberg"), Mockito.eq("db"), Mockito.eq("table"), Mockito.eq(1L),
                    Mockito.same(caller), Mockito.any(ExecutorService.class)));
            Assertions.assertSame(updatedTable,
                    tables.getIfPresent(new CachingIcebergCatalog.IcebergTableName("db", "table")));
        } finally {
            catalog.shutdown();
        }
    }

    private static BaseTable tableWithSnapshot(String location, long snapshotId) {
        TableOperations operations = Mockito.mock(TableOperations.class);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(operations.current()).thenReturn(metadata);
        Mockito.when(metadata.metadataFileLocation()).thenReturn(location);
        Mockito.when(metadata.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(metadata.snapshots()).thenReturn(Collections.singletonList(snapshot));
        Mockito.when(snapshot.snapshotId()).thenReturn(snapshotId);
        Mockito.when(snapshot.dataManifests(Mockito.any())).thenReturn(Collections.emptyList());
        return new BaseTable(operations, "db.table");
    }

}
