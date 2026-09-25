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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.iceberg.CachingIcebergCatalog.IcebergTableName;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.qe.ConnectContext;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StarRocksIcebergTableScan;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ActiveIcebergCacheTest {
    private final ExecutorService executor = Executors.newFixedThreadPool(3);
    private final IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
    private final IcebergTableName key = new IcebergTableName("db", "tbl");

    @AfterEach
    void cleanup() throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    private CachingIcebergCatalog catalog(boolean metadataCache) {
        Map<String, String> properties = new HashMap<>();
        properties.put("iceberg.catalog.type", "hive");
        properties.put("enable_iceberg_metadata_cache", String.valueOf(metadataCache));
        return new CachingIcebergCatalog("test", delegate, new IcebergCatalogProperties(properties), executor);
    }

    private BaseTable table(String location, ManifestFile... manifests) {
        TableOperations ops = Mockito.mock(TableOperations.class);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(ops.current()).thenReturn(metadata);
        Mockito.when(metadata.metadataFileLocation()).thenReturn(location);
        Mockito.when(metadata.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(metadata.snapshots()).thenReturn(List.of(snapshot));
        Mockito.when(snapshot.snapshotId()).thenReturn(7L);
        Mockito.when(snapshot.timestampMillis()).thenReturn(1L);
        Mockito.when(snapshot.dataManifests(Mockito.any())).thenReturn(List.of(manifests));
        return new BaseTable(ops, "db.tbl");
    }

    private ManifestFile manifest(String path) {
        ManifestFile manifest = Mockito.mock(ManifestFile.class);
        Mockito.when(manifest.path()).thenReturn(path);
        Mockito.when(manifest.snapshotId()).thenReturn(1L);
        Mockito.when(manifest.existingFilesCount()).thenReturn(0);
        Mockito.when(manifest.addedFilesCount()).thenReturn(1);
        return manifest;
    }

    private Cache<IcebergTableName, Table> tables(CachingIcebergCatalog catalog) {
        return Deencapsulation.getField(catalog, "tables");
    }

    private Cache<String, Set<DataFile>> files(CachingIcebergCatalog catalog) {
        return Deencapsulation.getField(catalog, "dataFileCache");
    }

    private Map<IcebergTableName, Long> times(CachingIcebergCatalog catalog, String field) {
        return Deencapsulation.getField(catalog, field);
    }

    private StarRocksIcebergTableScan scan(BaseTable table) {
        StarRocksIcebergTableScan scan = Mockito.mock(StarRocksIcebergTableScan.class);
        Mockito.when(delegate.getTableScan(Mockito.same(table), Mockito.any())).thenReturn(scan);
        Mockito.when(scan.planWith(Mockito.any())).thenReturn(scan);
        Mockito.when(scan.useSnapshot(7L)).thenReturn(scan);
        return scan;
    }

    @Test
    void cacheHitCannotOverwriteConcurrentRefresh() throws Exception {
        CachingIcebergCatalog catalog = catalog(false);
        BaseTable oldTable = table("old");
        BaseTable newTable = table("new");
        Cache<IcebergTableName, Table> original = tables(catalog);
        original.put(key, oldTable);
        Cache<IcebergTableName, Table> intercepted = Mockito.spy(original);
        CountDownLatch hitRead = new CountDownLatch(1);
        CountDownLatch releaseHit = new CountDownLatch(1);
        AtomicBoolean first = new AtomicBoolean(true);
        Mockito.doAnswer(inv -> {
            Table value = original.getIfPresent(key);
            if (first.getAndSet(false)) {
                hitRead.countDown();
                assertTrue(releaseHit.await(5, TimeUnit.SECONDS));
            }
            return value;
        }).when(intercepted).getIfPresent(key);
        Deencapsulation.setField(catalog, "tables", intercepted);
        Mockito.when(delegate.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl"))).thenReturn(newTable);
        Future<Table> hit = executor.submit(() -> catalog.getTable(new ConnectContext(), "db", "tbl"));
        try {
            assertTrue(hitRead.await(5, TimeUnit.SECONDS));
            catalog.refreshTable("db", "tbl", new ConnectContext(), executor);
        } finally {
            releaseHit.countDown();
        }
        assertSame(oldTable, hit.get(5, TimeUnit.SECONDS));
        assertSame(newTable, original.getIfPresent(key));
    }

    @Test
    void simultaneousMissesShareOneLoad() throws Exception {
        CachingIcebergCatalog catalog = catalog(false);
        BaseTable table = table("initial");
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);
        CountDownLatch secondMiss = new CountDownLatch(1);
        AtomicReference<Thread> secondThread = new AtomicReference<>();
        Cache<IcebergTableName, Table> original = tables(catalog);
        Cache<IcebergTableName, Table> intercepted = Mockito.spy(original);
        Mockito.doAnswer(inv -> {
            Table value = original.getIfPresent(key);
            if (Thread.currentThread() == secondThread.get() && value == null) {
                secondMiss.countDown();
            }
            return value;
        }).when(intercepted).getIfPresent(key);
        Deencapsulation.setField(catalog, "tables", intercepted);
        Mockito.when(delegate.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl"))).thenAnswer(inv -> {
            loading.countDown();
            assertTrue(releaseLoad.await(5, TimeUnit.SECONDS));
            return table;
        });
        Future<Table> first = executor.submit(() -> catalog.getTable(new ConnectContext(), "db", "tbl"));
        assertTrue(loading.await(5, TimeUnit.SECONDS));
        Future<Table> second = executor.submit(() -> {
            secondThread.set(Thread.currentThread());
            return catalog.getTable(new ConnectContext(), "db", "tbl");
        });
        try {
            assertTrue(secondMiss.await(5, TimeUnit.SECONDS));
        } finally {
            releaseLoad.countDown();
        }
        assertSame(table, first.get(5, TimeUnit.SECONDS));
        assertSame(table, second.get(5, TimeUnit.SECONDS));
        Mockito.verify(delegate, Mockito.times(1)).getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl"));
    }

    @Test
    void unchangedSnapshotRewarmsOldEvictedManifestWithoutRereadingCompleteEntries() {
        CachingIcebergCatalog catalog = catalog(true);
        ManifestFile missing = manifest("old-manifest");
        ManifestFile cached = manifest("cached-manifest");
        BaseTable table = table("unchanged", missing, cached);
        tables(catalog).put(key, table);
        DataFile data = Mockito.mock(DataFile.class);
        files(catalog).put(cached.path(), Set.of(data));
        Mockito.when(delegate.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl"))).thenReturn(table);
        StarRocksIcebergTableScan scan = scan(table);
        Mockito.doAnswer(inv -> {
            files(catalog).put(missing.path(), Set.of(data));
            return null;
        }).when(scan).refreshDataFileCache(List.of(missing));
        times(catalog, "tableLatestAccessTime").put(key, System.currentTimeMillis());
        // The manifest's snapshot is older than this refresh, and absent from TableMetadata.snapshot(id).
        times(catalog, "tableLatestRefreshTime").put(key, System.currentTimeMillis());
        times(catalog, "tableLatestSnapshotTime").put(key, 1L);
        catalog.refreshCatalog();
        catalog.refreshCatalog();
        Mockito.verify(scan, Mockito.times(1)).refreshDataFileCache(List.of(missing));
        Mockito.verify(delegate, Mockito.times(2)).getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl"));
        // Eviction after a successful warm must be repaired on the next pass too.
        files(catalog).invalidate(missing.path());
        catalog.refreshCatalog();
        Mockito.verify(scan, Mockito.times(2)).refreshDataFileCache(List.of(missing));
    }

    @Test
    void freshMetadataStillWarmsIncompleteManifestWithoutCatalogRead() {
        CachingIcebergCatalog catalog = catalog(true);
        ManifestFile manifest = manifest("partial");
        BaseTable table = table("fresh", manifest);
        tables(catalog).put(key, table);
        files(catalog).put(manifest.path(), Set.of());
        long now = System.currentTimeMillis();
        times(catalog, "tableLatestAccessTime").put(key, now);
        times(catalog, "tableLatestRefreshTime").put(key, now);
        times(catalog, "tableLatestSnapshotTime").put(key, now);
        StarRocksIcebergTableScan scan = scan(table);
        catalog.refreshCatalog();
        Mockito.verify(scan).refreshDataFileCache(List.of(manifest));
        Mockito.verify(delegate, Mockito.never()).getTable(Mockito.any(), Mockito.anyString(), Mockito.anyString());
        assertEquals(now, times(catalog, "tableLatestRefreshTime").get(key));
        assertEquals(now, times(catalog, "tableLatestAccessTime").get(key));
    }

    @Test
    void inactiveTableIsNotWarmed() {
        CachingIcebergCatalog catalog = catalog(true);
        BaseTable table = table("inactive", manifest("old"));
        tables(catalog).put(key, table);
        times(catalog, "tableLatestAccessTime").put(key, 1L);
        catalog.refreshCatalog();
        assertFalse(tables(catalog).asMap().containsKey(key));
        Mockito.verifyNoInteractions(delegate);
    }

    @Test
    void emptySnapshotAndDisabledMetadataCacheDoNotPlanManifests() {
        CachingIcebergCatalog catalog = catalog(false);
        BaseTable table = table("unchanged");
        tables(catalog).put(key, table);
        Mockito.when(delegate.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl"))).thenReturn(table);
        catalog.refreshTable("db", "tbl", new ConnectContext(), executor);
        Mockito.verify(delegate, Mockito.never()).getTableScan(Mockito.any(), Mockito.any());
        CachingIcebergCatalog empty = catalog(true);
        Mockito.when(table.operations().current().currentSnapshot()).thenReturn(null);
        tables(empty).put(key, table);
        empty.refreshTable("db", "tbl", new ConnectContext(), executor);
        Mockito.verify(delegate, Mockito.never()).getTableScan(Mockito.any(), Mockito.any());
    }

    @Test
    void internalLookupDoesNotRecordClientActivity() {
        CachingIcebergCatalog catalog = catalog(false);
        ConnectContext previous = ConnectContext.get();
        ConnectContext internal = new ConnectContext();
        internal.setThreadLocalInfo();
        try {
            BaseTable current = table("current");
            Mockito.when(delegate.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl")))
                    .thenReturn(current);
            catalog.getTable(internal, "db", "tbl");
            assertTrue(times(catalog, "tableLatestAccessTime").isEmpty());
            internal.setCommand(MysqlCommand.COM_QUERY);
            catalog.getTable(internal, "db", "tbl");
            assertTrue(times(catalog, "tableLatestAccessTime").containsKey(key));
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }
}
