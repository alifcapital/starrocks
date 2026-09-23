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

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.starrocks.common.jmockit.Deencapsulation;
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
import java.util.Collections;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class IcebergMetadataRefreshFooterPrefetcherTest {
    private ConnectContext caller;
    private ExecutorService executor;
    private final Deque<Runnable> queue = new ArrayDeque<>();

    @BeforeEach
    public void setUp() {
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
        ConnectContext.remove();
    }

    private void warmup() {
        IcebergMetadataRefreshFooterPrefetcher.warmup("iceberg", "db", "table", caller, executor);
    }

    @Test
    public void testDisabledDoesNotQueueWork() {
        caller.getSessionVariable().setEnableIcebergMetadataRefreshFooterPrefetch(false);
        warmup();
        IcebergMetadataRefreshFooterPrefetcher.warmup("iceberg", "db", "table", null, executor);
        Mockito.verifyNoInteractions(executor);
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
        LoadingCache<CachingIcebergCatalog.IcebergTableName, Table> tables =
                Deencapsulation.getField(catalog, "tables");
        tables.put(new CachingIcebergCatalog.IcebergTableName("db", "table"), oldTable);
        LoadingCache<CachingIcebergCatalog.IcebergTableName, Map<String, Partition>> partitions =
                Deencapsulation.getField(catalog, "partitionCache");
        partitions.put(new CachingIcebergCatalog.IcebergTableName("db", "table", 2), Collections.emptyMap());
        try (MockedStatic<IcebergMetadataRefreshFooterPrefetcher> prefetch =
                     Mockito.mockStatic(IcebergMetadataRefreshFooterPrefetcher.class)) {
            catalog.refreshTable("db", "table", caller, executor);
            prefetch.verify(() -> IcebergMetadataRefreshFooterPrefetcher.warmup(
                    Mockito.eq("iceberg"), Mockito.eq("db"), Mockito.eq("table"),
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
