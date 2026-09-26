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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.LogUtil;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.CachingIcebergCatalog.IcebergTableName;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.connector.iceberg.IcebergCatalogProperties.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;

public class CachingIcebergCatalogTest {
    private static final String CATALOG_NAME = "iceberg_catalog";
    public static final IcebergCatalogProperties DEFAULT_CATALOG_PROPERTIES;
    public static final Map<String, String> DEFAULT_CONFIG = new HashMap<>();
    public static ConnectContext connectContext;

    static {
        DEFAULT_CONFIG.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732"); // non-exist ip, prevent to connect local service
        DEFAULT_CONFIG.put(ICEBERG_CATALOG_TYPE, "hive");
        DEFAULT_CATALOG_PROPERTIES = new IcebergCatalogProperties(DEFAULT_CONFIG);
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    private BaseTable mockRefreshCandidate(long snapshotId, String location) {
        BaseTable table = Mockito.mock(BaseTable.class);
        TableOperations ops = Mockito.mock(TableOperations.class);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(table.operations()).thenReturn(ops);
        Mockito.when(ops.current()).thenReturn(metadata);
        Mockito.when(metadata.metadataFileLocation()).thenReturn(location);
        Mockito.when(table.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.snapshotId()).thenReturn(snapshotId);
        Mockito.when(snapshot.dataManifests(Mockito.any())).thenReturn(List.of());
        Mockito.when(snapshot.deleteManifests(Mockito.any())).thenReturn(List.of());
        return table;
    }

    @Test
    public void testInvalidateAllPartitionSnapshots() {
        ExecutorService workers = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog(
                    CATALOG_NAME, Mockito.mock(IcebergCatalog.class), DEFAULT_CATALOG_PROPERTIES, workers);
            Cache<IcebergTableName, Map<String, Partition>> partitions =
                    Deencapsulation.getField(catalog, "partitionCache");
            IcebergTableName other = new IcebergTableName("db", "other", 1L);
            for (boolean fullReset : List.of(false, true)) {
                for (long snapshot : List.of(-1L, 1L, 2L)) {
                    partitions.put(new IcebergTableName("db", "tbl", snapshot), Map.of());
                }
                partitions.put(other, Map.of());
                if (fullReset) {
                    catalog.invalidateCache("DB", "TBL");
                } else {
                    catalog.invalidatePartitionCache("DB", "TBL");
                }
                Assertions.assertEquals(Set.of(other), partitions.asMap().keySet());
            }
            IcebergTableName tableKey = new IcebergTableName("db", "tbl");
            IcebergTableName snapshotKey = new IcebergTableName("db", "tbl", 1L);
            Assertions.assertNotEquals(tableKey, snapshotKey);
            Assertions.assertNotEquals(snapshotKey, tableKey);
        } finally {
            workers.shutdownNow();
        }
    }

    @Test
    public void testRefreshPublishesOnlyAfterWarmup() throws Exception {
        for (boolean sameMetadata : List.of(false, true)) {
            IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
            ExecutorService workers = Executors.newFixedThreadPool(2);
            CountDownLatch warming = new CountDownLatch(1);
            CountDownLatch finish = new CountDownLatch(1);
            try {
                CachingIcebergCatalog catalog = new CachingIcebergCatalog(
                        CATALOG_NAME, delegate, DEFAULT_CATALOG_PROPERTIES, workers);
                BaseTable oldTable = mockRefreshCandidate(1L, "old.json");
                BaseTable candidate = mockRefreshCandidate(sameMetadata ? 1L : 2L,
                        sameMetadata ? "old.json" : "new.json");
                Cache<IcebergTableName, Table> tables = Deencapsulation.getField(catalog, "tables");
                tables.put(new IcebergTableName("db", "tbl"), oldTable);
                Cache<IcebergTableName, Map<String, Partition>> partitions =
                        Deencapsulation.getField(catalog, "partitionCache");
                IcebergTableName staleKey = new IcebergTableName("db", "tbl", 99L);
                IcebergTableName currentKey = new IcebergTableName("db", "tbl", -1L);
                IcebergTableName otherTableKey = new IcebergTableName("db", "other", 99L);
                partitions.put(staleKey, Map.of());
                partitions.put(currentKey, Map.of());
                partitions.put(otherTableKey, Map.of());
                Mockito.when(delegate.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl")))
                        .thenReturn(candidate);
                Mockito.when(delegate.getPartitions(Mockito.any(), Mockito.anyLong(), Mockito.any()))
                        .thenAnswer(inv -> {
                            Assertions.assertSame(candidate, ((IcebergTable) inv.getArgument(0)).getNativeTable());
                            return Map.of();
                        });
                Mockito.when(candidate.currentSnapshot().deleteManifests(Mockito.any())).thenAnswer(inv -> {
                    warming.countDown();
                    Assertions.assertTrue(finish.await(5, TimeUnit.SECONDS));
                    return List.of();
                });
                java.util.concurrent.Future<?> refresh = workers.submit(
                        () -> catalog.refreshTable("db", "tbl", new ConnectContext(), workers));
                Assertions.assertTrue(warming.await(5, TimeUnit.SECONDS));
                java.util.concurrent.Future<Table> read = workers.submit(
                        () -> catalog.getTable(new ConnectContext(), "db", "tbl"));
                Assertions.assertSame(oldTable, read.get(2, TimeUnit.SECONDS),
                        "A query must not wait for candidate warmup or observe the cold candidate");
                Assertions.assertNotNull(partitions.getIfPresent(staleKey));
                Assertions.assertNotNull(partitions.getIfPresent(currentKey));
                finish.countDown();
                refresh.get(5, TimeUnit.SECONDS);
                Assertions.assertNull(partitions.getIfPresent(staleKey));
                Assertions.assertNull(partitions.getIfPresent(currentKey));
                Assertions.assertNotNull(partitions.getIfPresent(otherTableKey));
                Assertions.assertNotNull(partitions.getIfPresent(
                        new IcebergTableName("db", "tbl", sameMetadata ? 1L : 2L)));
                Assertions.assertSame(candidate, catalog.getTable(new ConnectContext(), "db", "tbl"));
                Mockito.verify(delegate).getPartitions(Mockito.any(), Mockito.eq(sameMetadata ? 1L : 2L), Mockito.any());
            } finally {
                finish.countDown();
                workers.shutdownNow();
            }
        }
    }

    @Test
    public void testBackgroundWarmupFailureRetainsPreviousSnapshotAndRetries() {
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        ExecutorService workers = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog(
                    CATALOG_NAME, delegate, DEFAULT_CATALOG_PROPERTIES, workers);
            BaseTable oldTable = mockRefreshCandidate(1L, "old.json");
            BaseTable candidate = mockRefreshCandidate(2L, "new.json");
            Cache<IcebergTableName, Table> tables = Deencapsulation.getField(catalog, "tables");
            IcebergTableName key = new IcebergTableName("db", "tbl");
            tables.put(key, oldTable);
            Map<IcebergTableName, Long> access = Deencapsulation.getField(catalog, "tableLatestAccessTime");
            access.put(key, System.currentTimeMillis());
            Map<IcebergTableName, Long> refreshed = Deencapsulation.getField(catalog, "tableLatestRefreshTime");
            refreshed.put(key, 1L);
            Mockito.when(delegate.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl")))
                    .thenReturn(candidate);
            Mockito.when(delegate.getPartitions(Mockito.any(), Mockito.anyLong(), Mockito.any())).thenReturn(Map.of());
            Mockito.when(candidate.currentSnapshot().deleteManifests(Mockito.any()))
                    .thenThrow(new RuntimeException("S3 unavailable"));
            catalog.refreshCatalog();
            Assertions.assertSame(oldTable, tables.getIfPresent(key));
            Assertions.assertEquals(1L, refreshed.get(key));
            Snapshot candidateSnapshot = candidate.currentSnapshot();
            Mockito.doReturn(List.of()).when(candidateSnapshot).deleteManifests(Mockito.any());
            catalog.refreshCatalog();
            Assertions.assertSame(candidate, tables.getIfPresent(key));
            Assertions.assertTrue(refreshed.get(key) > 1L);
        } finally {
            workers.shutdownNow();
        }
    }

    @Test
    public void testWarmDeletesIndependentlyAndRefillEvictedEntries() {
        for (String dataBudget : List.of("0", "0.1")) {
            Map<String, String> properties = new HashMap<>(DEFAULT_CONFIG);
            properties.put("iceberg_data_file_cache_memory_usage_ratio", dataBudget);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                CachingIcebergCatalog catalog = Mockito.spy(new CachingIcebergCatalog(CATALOG_NAME,
                        Mockito.mock(IcebergCatalog.class), new IcebergCatalogProperties(properties), executor));
                BaseTable table = Mockito.mock(BaseTable.class);
                Snapshot snapshot = Mockito.mock(Snapshot.class);
                Mockito.when(table.currentSnapshot()).thenReturn(snapshot);
                Mockito.when(snapshot.snapshotId()).thenReturn(42L);
                ManifestFile manifest = Mockito.mock(ManifestFile.class);
                Mockito.when(manifest.path()).thenReturn("deletes.avro");
                Mockito.when(manifest.addedFilesCount()).thenReturn(2);
                Mockito.when(manifest.existingFilesCount()).thenReturn(0);
                Mockito.when(snapshot.dataManifests(Mockito.any())).thenReturn(List.of());
                Mockito.when(snapshot.deleteManifests(Mockito.any())).thenReturn(List.of(manifest));
                com.github.benmanes.caffeine.cache.LoadingCache<IcebergTableName, Map<String, Partition>> partitions =
                        Deencapsulation.getField(catalog, "partitionCache");
                partitions.put(new IcebergTableName("db", "tbl", 42L), Map.of());
                Cache<String, java.util.Set<org.apache.iceberg.DeleteFile>> deletes =
                        Deencapsulation.getField(catalog, "deleteFileCache");
                org.apache.iceberg.StarRocksIcebergTableScan scan =
                        Mockito.mock(org.apache.iceberg.StarRocksIcebergTableScan.class, Mockito.RETURNS_SELF);
                Mockito.doReturn(scan).when(scan).planWith(executor);
                Mockito.doReturn(scan).when(scan).useSnapshot(42L);
                Mockito.doReturn(scan).when(catalog).getTableScan(Mockito.eq(table), Mockito.any());
                java.util.Set<org.apache.iceberg.DeleteFile> complete = java.util.Set.of(
                        Mockito.mock(org.apache.iceberg.DeleteFile.class), Mockito.mock(org.apache.iceberg.DeleteFile.class));
                Mockito.doAnswer(invocation -> {
                    deletes.put(manifest.path(), complete);
                    return null;
                }).when(scan).refreshDeleteFileCache(List.of(manifest));
                Deencapsulation.invoke(catalog, "warmCurrentSnapshot", table, "db", "tbl", executor);
                Deencapsulation.invoke(catalog, "warmCurrentSnapshot", table, "db", "tbl", executor);
                Mockito.verify(scan).refreshDeleteFileCache(List.of(manifest));
                deletes.invalidate(manifest.path());
                Deencapsulation.invoke(catalog, "warmCurrentSnapshot", table, "db", "tbl", executor);
                deletes.put(manifest.path(), java.util.Set.of(complete.iterator().next()));
                Deencapsulation.invoke(catalog, "warmCurrentSnapshot", table, "db", "tbl", executor);
                Mockito.verify(scan, Mockito.times(3)).refreshDeleteFileCache(List.of(manifest));
                Mockito.verify(scan, Mockito.never()).refreshDataFileCache(Mockito.any());
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    public void testNormalCreateAndDropDBTable(@Mocked IcebergCatalog icebergCatalog)
            throws MetaNotFoundException {
        new Expectations() {
            {
                icebergCatalog.createDB(connectContext, "test", (Map<String, String>) any);
                result = null;
                minTimes = 0;

                icebergCatalog.dropDB(connectContext, "test");
                result = null;
                minTimes = 0;

                icebergCatalog.dropTable(connectContext, "test", "table", anyBoolean);
                result = true;
                minTimes = 0;
            }
        };
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        cachingIcebergCatalog.createDB(connectContext, "test", new HashMap<>());
        cachingIcebergCatalog.dropDB(connectContext, "test");
        cachingIcebergCatalog.dropTable(connectContext, "test", "table", true);
        cachingIcebergCatalog.invalidateCache("test", "table");
        cachingIcebergCatalog.invalidatePartitionCache("test", "table");
    }

    @Test
    public void testListPartitionNames(@Mocked IcebergCatalog icebergCatalog) throws Exception {
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);
        Table nativeTable = createBaseTableWithManifests(1, 0, spec);
        new Expectations() {
            {
                icebergCatalog.getTable((ConnectContext) any, "db", "test");
                result = nativeTable;
                minTimes = 0;
            }
        };
        ExecutorService partitionExecutor = Executors.newSingleThreadExecutor();
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, partitionExecutor);
        IcebergTable table =
                IcebergTable.builder().setSrTableName("test_sr")
                .setCatalogDBName("db").setCatalogTableName("test").setNativeTable(nativeTable).build();

        Assertions.assertFalse(nativeTable.spec().isUnpartitioned());
        {
            ConnectorMetadataRequestContext requestContext = new ConnectorMetadataRequestContext();
            SessionVariable sv = ConnectContext.getSessionVariableOrDefault();
            sv.setEnableConnectorAsyncListPartitions(true);
            requestContext.setQueryMVRewrite(true);
            List<String> res = cachingIcebergCatalog.listPartitionNames(table, requestContext, null);
            Assertions.assertNull(res);
            partitionExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
        }
        {
            ConnectorMetadataRequestContext requestContext = new ConnectorMetadataRequestContext();
            SessionVariable sv = ConnectContext.getSessionVariableOrDefault();
            sv.setEnableConnectorAsyncListPartitions(false);
            requestContext.setQueryMVRewrite(true);
            List<String> res = cachingIcebergCatalog.listPartitionNames(table, requestContext, null);
            Assertions.assertEquals(res.size(), 0);
        }
        partitionExecutor.shutdownNow();
    }

    @Test
    public void testGetPartitionsUsesCurrentSnapshotMicrosForUnpartitionedFallback() {
        IcebergCatalog catalog = new IcebergCatalog() {
            @Override
            public IcebergCatalogType getIcebergCatalogType() {
                return IcebergCatalogType.HIVE_CATALOG;
            }

            @Override
            public List<String> listAllDatabases(ConnectContext context) {
                return List.of();
            }

            @Override
            public Database getDB(ConnectContext context, String dbName) {
                return null;
            }

            @Override
            public List<String> listTables(ConnectContext context, String dbName) {
                return List.of();
            }

            @Override
            public void renameTable(ConnectContext context, String dbName, String tblName, String newTblName) {
            }

            @Override
            public Table getTable(ConnectContext context, String dbName, String tableName) {
                throw new UnsupportedOperationException();
            }
        };

        Table nativeTable = Mockito.mock(Table.class);
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        PartitionsTable partitionsTable = Mockito.mock(PartitionsTable.class);
        TableScan tableScan = Mockito.mock(TableScan.class);

        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(spec.isUnpartitioned()).thenReturn(true);
        Mockito.when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(nativeTable.name()).thenReturn("db.test");
        Mockito.when(snapshot.timestampMillis()).thenReturn(1234L);
        Mockito.when(snapshot.sequenceNumber()).thenReturn(9L);
        Mockito.when(partitionsTable.newScan()).thenReturn(tableScan);
        Mockito.when(tableScan.planFiles()).thenReturn(CloseableIterable.empty());

        try (MockedStatic<MetadataTableUtils> metadataTableUtils = Mockito.mockStatic(MetadataTableUtils.class)) {
            metadataTableUtils.when(() -> MetadataTableUtils.createMetadataTableInstance(
                    nativeTable, MetadataTableType.PARTITIONS)).thenReturn(partitionsTable);

            IcebergTable table = IcebergTable.builder()
                    .setSrTableName("test")
                    .setCatalogDBName("db")
                    .setCatalogTableName("test")
                    .setNativeTable(nativeTable)
                    .build();

            Map<String, Partition> partitions = catalog.getPartitions(table, -1, null);
            Partition partition = partitions.get(IcebergCatalog.EMPTY_PARTITION_NAME);

            Assertions.assertNotNull(partition);
            Assertions.assertEquals(TimeUnit.MICROSECONDS, partition.getModifiedTimeUnit());
            Assertions.assertEquals(TimeUnit.MILLISECONDS.toMicros(1234L), partition.getModifiedTime());
            Assertions.assertEquals(9L, partition.getVersion());
        }
    }

    @Test
    public void testPartitionCacheCountedInEstimateSize(@Mocked IcebergCatalog icebergCatalog) {
        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);
        Table nativeTable = createBaseTableWithManifests(1, 0, spec);
        Map<String, Partition> partitionMap = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            partitionMap.put("dt=part-" + i, new Partition(1234L, 1L));
        }
        new Expectations() {
            {
                icebergCatalog.getTable((ConnectContext) any, "db", "test");
                result = nativeTable;
                minTimes = 0;
                icebergCatalog.getPartitions((IcebergTable) any, anyLong, null);
                result = partitionMap;
                minTimes = 0;
            }
        };
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        IcebergTable table = IcebergTable.builder().setSrTableName("test")
                .setCatalogDBName("db").setCatalogTableName("test").setNativeTable(nativeTable).build();

        long before = cachingIcebergCatalog.estimateSize();
        cachingIcebergCatalog.getPartitions(table, 1L, null);
        long after = cachingIcebergCatalog.estimateSize();
        Assertions.assertTrue(after > before,
                "partitionCache must be counted in estimateSize; before=" + before + " after=" + after);
    }

    @Test
    public void testGetDB(@Mocked IcebergCatalog icebergCatalog, @Mocked Database db) {
        new Expectations() {
            {
                icebergCatalog.getDB(connectContext, "test");
                result = db;
                minTimes = 1;
            }
        };
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        Assertions.assertEquals(db, cachingIcebergCatalog.getDB(connectContext, "test"));
        Assertions.assertEquals(db, cachingIcebergCatalog.getDB(connectContext, "test"));
    }


    @Test
    public void testGetTable(@Mocked IcebergCatalog icebergCatalog) {
        Table nativeTable = createBaseTableWithManifests(1, 1);
        new Expectations() {
            {
                icebergCatalog.getTable(connectContext, "test", "table");
                result = nativeTable;
                minTimes = 1;
            }
        };
        //test for cache
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        Assertions.assertEquals(nativeTable, cachingIcebergCatalog.getTable(connectContext, "test", "table"));
        Assertions.assertEquals(nativeTable, cachingIcebergCatalog.getTable(connectContext, "test", "table"));
        cachingIcebergCatalog.invalidateCache("test", "table");
    }

    @Test
    public void testGetTableIOError(@Mocked IcebergCatalog icebergCatalog) {
        new Expectations() {
            {
                icebergCatalog.getTable(connectContext, "test", "table");
                result = new RuntimeException(new java.io.IOException("io failure"));
            }
        };

        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        StarRocksConnectorException ex = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> cachingIcebergCatalog.getTable(connectContext, "test", "table"));
        String expectedPrefix = "Failed to get iceberg table iceberg_catalog.test.table";
        Assertions.assertTrue(ex.getMessage().contains(expectedPrefix));
        Assertions.assertTrue(LogUtil.getUnwoundExceptionMessage(ex).contains("io failure"));
    }

    private int getStaticIntField(String fieldName) {
        try {
            java.lang.reflect.Field f = CachingIcebergCatalog.class.getDeclaredField(fieldName);
            f.setAccessible(true);
            return f.getInt(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Table createBaseTableWithManifests(int snapshotCount, int manifestCount) {
        return createBaseTableWithManifests(snapshotCount, manifestCount, null);
    }

    private Table createBaseTableWithManifests(int snapshotCount, int manifestCount, PartitionSpec spec) {
        TableOperations ops = Mockito.mock(TableOperations.class);
        TableMetadata meta = Mockito.mock(TableMetadata.class);
        Snapshot currentSnapshot = Mockito.mock(Snapshot.class);

        List<Snapshot> snapshots = new ArrayList<>();
        for (int i = 0; i < snapshotCount; i++) {
            snapshots.add(Mockito.mock(Snapshot.class));
        }
        List<ManifestFile> manifests = new ArrayList<>();
        for (int i = 0; i < manifestCount; i++) {
            manifests.add(Mockito.mock(ManifestFile.class));
        }
        String uuid = UUID.randomUUID().toString();
        Mockito.when(ops.current()).thenReturn(meta);
        Mockito.when(meta.snapshots()).thenReturn(snapshots);
        Mockito.when(meta.currentSnapshot()).thenReturn(currentSnapshot);
        Mockito.when(meta.metadataFileLocation()).thenReturn("metadata-" + uuid);
        Mockito.when(meta.uuid()).thenReturn(uuid);
        if (spec != null) {
            Mockito.when(meta.spec()).thenReturn(spec);
        }
        Mockito.when(currentSnapshot.allManifests(Mockito.any())).thenReturn(manifests);

        return new BaseTable(ops, "db.tbl");
    }

    @Test
    public void testInvalidateCache(@Mocked IcebergCatalog icebergCatalog) {
        Table nativeTable = createBaseTableWithManifests(1, 1);
        new Expectations() {
            {
                icebergCatalog.getTable(connectContext, "db1", "tbl1");
                result = nativeTable;
                times = 2; // Called twice: once for initial cache, once after invalidation
            }
        };

        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());

        // First call - populates cache
        Table t1 = cachingIcebergCatalog.getTable(connectContext, "db1", "tbl1");
        Assertions.assertEquals(nativeTable, t1);

        // Invalidate cache
        cachingIcebergCatalog.invalidateCache("db1", "tbl1");

        // Second call - should hit delegate again because cache was invalidated
        Table t2 = cachingIcebergCatalog.getTable(connectContext, "db1", "tbl1");
        Assertions.assertEquals(nativeTable, t2);
    }

    @Test
    public void testTableCacheEnabled_hitsDelegateOnce(@Mocked IcebergCatalog delegate,
                                                       @Mocked IcebergCatalogProperties props,
                                                       @Mocked ConnectContext ctx) throws Exception {
        Table nativeTable = createBaseTableWithManifests(1, 1);
        new Expectations() {
            {
                props.isEnableIcebergMetadataCache(); 
                result = true;
                props.isEnableIcebergTableCache(); 
                result = true;
                props.getIcebergTableCacheTtlSec();
                result = 24L * 60 * 60;
                props.getIcebergDataFileCacheMemoryUsageRatio(); 
                result = 0.0;
                props.getIcebergDeleteFileCacheMemoryUsageRatio(); 
                result = 0.0;
                props.getIcebergTableCacheMemoryUsageRatio();
                result = 1;

                delegate.getTable(ctx, "db1", "t1"); 
                result = nativeTable; 
                minTimes = 0;
            }
        };

        ExecutorService es = Executors.newFixedThreadPool(5);
        try {
            CachingIcebergCatalog catalog =
                    new CachingIcebergCatalog("iceberg0", delegate, props, es);

            org.apache.iceberg.Table r1 = catalog.getTable(ctx, "db1", "t1");
            org.apache.iceberg.Table r2 = catalog.getTable(ctx, "db1", "t1");

            org.junit.jupiter.api.Assertions.assertSame(r1, r2);

            new Verifications() {
                {
                    delegate.getTable(ctx, "db1", "t1"); 
                    times = 1;
                }
            };
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    public void testTableCacheDisabled_hitsDelegateTwice(@Mocked IcebergCatalog delegate,
                                                         @Mocked IcebergCatalogProperties props,
                                                         @Mocked ConnectContext ctx) throws Exception {
        Table nativeTable1 = createBaseTableWithManifests(1, 1);
        Table nativeTable2 = createBaseTableWithManifests(1, 1);
        new Expectations() {
            {
                props.isEnableIcebergMetadataCache(); 
                result = true;
                props.isEnableIcebergTableCache(); 
                result = false;
                props.getIcebergTableCacheTtlSec();
                result = 60;
                props.getIcebergDataFileCacheMemoryUsageRatio(); 
                result = 0.0;
                props.getIcebergDeleteFileCacheMemoryUsageRatio(); 
                result = 0.0;

                delegate.getTable(ctx, "db1", "t1"); 
                result = nativeTable1;
                minTimes = 0;
            }
        };

        ExecutorService es = Executors.newFixedThreadPool(5);
        try {
            CachingIcebergCatalog catalog =
                    new CachingIcebergCatalog("iceberg0", delegate, props, es);

            org.apache.iceberg.Table r1 = catalog.getTable(ctx, "db1", "t1");
            org.apache.iceberg.Table r2 = catalog.getTable(ctx, "db1", "t1");

            new Verifications() {
                {
                    delegate.getTable(ctx, "db1", "t1"); 
                    times = 2; //caffeine has a diff with guava here
                }
            };
        } finally {
            es.shutdownNow();
        }
    }

    @Test
    public void testEstimateCountReflectsTableCache(@Mocked IcebergCatalog icebergCatalog) {
        Table nativeTable = createBaseTableWithManifests(1, 1);
        new Expectations() {
            {
                icebergCatalog.getTable(connectContext, "db2", "tbl2");
                result = nativeTable;
                times = 1;
            }
        };
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        cachingIcebergCatalog.getTable(connectContext, "db2", "tbl2");
        Map<String, Long> counts = cachingIcebergCatalog.estimateCount();
        Assertions.assertEquals(1L, counts.get("Table"));
    }

    @Test
    public void testGetTableBypassCacheForRestCatalogWhenAuthToken(@Mocked IcebergRESTCatalog restCatalog) {
        ConnectContext ctx = new ConnectContext();
        ctx.setAuthToken("token");
        Table nativeTable = createBaseTableWithManifests(1, 1);
        new Expectations() {
            {
                restCatalog.getTable(ctx, "db3", "tbl3");
                result = nativeTable;
                times = 2;
            }
        };

        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, restCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        Assertions.assertEquals(nativeTable, cachingIcebergCatalog.getTable(ctx, "db3", "tbl3"));
        Assertions.assertEquals(nativeTable, cachingIcebergCatalog.getTable(ctx, "db3", "tbl3"));
    }

    @Test
    public void testRestCatalogWithoutAuthTokenUsesCache(@Mocked IcebergRESTCatalog restCatalog) {
        // A REST catalog (including one with vended credentials) is served from the cache: the delegate
        // is hit once and the second getTable() is a cache hit. Guards the revert of the #69434 bypass.
        ConnectContext ctx = new ConnectContext();
        Table nativeTable = createBaseTableWithManifests(1, 1);
        new Expectations() {
            {
                restCatalog.getTable(ctx, "db4", "tbl4");
                result = nativeTable;
                times = 1;
            }
        };

        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, restCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());

        Assertions.assertSame(nativeTable, cachingIcebergCatalog.getTable(ctx, "db4", "tbl4"));
        Assertions.assertSame(nativeTable, cachingIcebergCatalog.getTable(ctx, "db4", "tbl4"));
    }

    @Test
    public void testRefreshTableRenewsCredentialsWhenMetadataUnchanged(@Mocked IcebergCatalog delegate) {
        ConnectContext ctx = new ConnectContext();
        Table cachedTable = createBaseTableWithManifests(1, 1);
        Table reloadedTable = createBaseTableWithManifests(1, 1);
        // Identical metadata location: no snapshot change, so the background refresh keeps the
        // partition/file caches but must still swap in the reloaded table to pick up renewed credentials.
        String sharedLocation = "s3://bucket/metadata/v1.metadata.json";
        Mockito.when(((BaseTable) cachedTable).operations().current().metadataFileLocation()).thenReturn(sharedLocation);
        Mockito.when(((BaseTable) reloadedTable).operations().current().metadataFileLocation()).thenReturn(sharedLocation);

        AtomicInteger calls = new AtomicInteger();
        new Expectations() {
            {
                delegate.getTable((ConnectContext) any, "db1", "t1");
                result = new Delegate<Table>() {
                    Table get(ConnectContext c, String db, String tbl) {
                        return calls.getAndIncrement() == 0 ? cachedTable : reloadedTable;
                    }
                };
            }
        };

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CachingIcebergCatalog catalog = new CachingIcebergCatalog(CATALOG_NAME, delegate,
                DEFAULT_CATALOG_PROPERTIES, executor);
        Cache<IcebergTableName, Table> tableCache = Deencapsulation.getField(catalog, "tables");
        IcebergTableName key = new IcebergTableName("db1", "t1");

        Assertions.assertSame(cachedTable, catalog.getTable(ctx, "db1", "t1"));
        catalog.refreshTable("db1", "t1", ctx, executor);
        Assertions.assertSame(reloadedTable, tableCache.getIfPresent(key));
    }

    @Test
    public void testRestTableCacheTtlIsCapped(@Mocked IcebergRESTCatalog restCatalog,
                                              @Mocked IcebergCatalog hiveCatalog) {
        // REST catalog: the table cache hard-expiry is capped regardless of the table cache
        // TTL, so an idle vended-credential entry cannot outlive its token.
        CachingIcebergCatalog restCaching = new CachingIcebergCatalog(CATALOG_NAME, restCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        Cache<IcebergTableName, Table> restCache = Deencapsulation.getField(restCaching, "tables");
        long restTtl = restCache.policy().expireAfterWrite().get().getExpiresAfter(TimeUnit.SECONDS);
        Assertions.assertTrue(restTtl <= 3000, "REST table cache TTL must be capped, was " + restTtl);

        // Non-REST catalog: TTL stays at the configured table cache TTL (no credential to protect).
        CachingIcebergCatalog hiveCaching = new CachingIcebergCatalog(CATALOG_NAME, hiveCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        Cache<IcebergTableName, Table> hiveCache = Deencapsulation.getField(hiveCaching, "tables");
        long hiveTtl = hiveCache.policy().expireAfterAccess().get().getExpiresAfter(TimeUnit.SECONDS);
        Assertions.assertEquals(DEFAULT_CATALOG_PROPERTIES.getIcebergTableCacheTtlSec(), hiveTtl,
                "non-REST table cache TTL must equal the configured table cache TTL");
    }

    @Test
    public void testGetCatalogPropertiesDelegatesToWrappedCatalog() {
        Map<String, String> expectedProperties = new HashMap<>();
        expectedProperties.put("s3.access-key-id", "test-key");
        expectedProperties.put("s3.secret-access-key", "test-secret");

        // Use Mockito for this test since JMockit doesn't properly handle default interface methods
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        Mockito.when(delegate.getCatalogProperties()).thenReturn(expectedProperties);

        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, delegate,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());

        Map<String, String> actualProperties = cachingIcebergCatalog.getCatalogProperties();
        Assertions.assertEquals(expectedProperties, actualProperties);
        Assertions.assertEquals("test-key", actualProperties.get("s3.access-key-id"));
        Assertions.assertEquals("test-secret", actualProperties.get("s3.secret-access-key"));

        // Verify that getCatalogProperties was called on the delegate
        Mockito.verify(delegate).getCatalogProperties();
    }

    /**
     * Two concurrent refreshTable calls on the SAME table must be serialized: the second
     * waits for the first to finish before entering its critical section.
     */
    @Test
    public void testRefreshTableSameTableIsSerializedNotParallel() throws Exception {
        AtomicInteger concurrentRefreshes = new AtomicInteger(0);
        AtomicInteger maxConcurrentRefreshes = new AtomicInteger(0);
        CountDownLatch firstStarted = new CountDownLatch(1);

        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        Mockito.when(delegate.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl")))
                .thenAnswer(inv -> {
                    int current = concurrentRefreshes.incrementAndGet();
                    maxConcurrentRefreshes.accumulateAndGet(current, Math::max);
                    firstStarted.countDown();
                    Thread.sleep(80);
                    concurrentRefreshes.decrementAndGet();

                    TableOperations ops = Mockito.mock(TableOperations.class);
                    TableMetadata meta = Mockito.mock(TableMetadata.class);
                    Mockito.when(ops.current()).thenReturn(meta);
                    Mockito.when(meta.metadataFileLocation()).thenReturn("loc-" + UUID.randomUUID());
                    Snapshot snap = Mockito.mock(Snapshot.class);
                    Mockito.when(snap.snapshotId()).thenReturn(1L);
                    Mockito.when(snap.dataManifests(Mockito.any())).thenReturn(List.of());
                    Mockito.when(meta.currentSnapshot()).thenReturn(snap);
                    return new BaseTable(ops, "db.tbl");
                });

        CachingIcebergCatalog catalog = new CachingIcebergCatalog(
                CATALOG_NAME, delegate, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());

        // Populate cache with an initial table so refreshTable enters the update branch.
        TableOperations initOps = Mockito.mock(TableOperations.class);
        TableMetadata initMeta = Mockito.mock(TableMetadata.class);
        Snapshot initSnap = Mockito.mock(Snapshot.class);
        Mockito.when(initOps.current()).thenReturn(initMeta);
        Mockito.when(initMeta.metadataFileLocation()).thenReturn("loc-initial");
        Mockito.when(initMeta.currentSnapshot()).thenReturn(initSnap);
        Mockito.when(initSnap.snapshotId()).thenReturn(0L);
        Mockito.when(initSnap.dataManifests(Mockito.any())).thenReturn(List.of());
        BaseTable initTable = new BaseTable(initOps, "db.tbl");
        Cache<IcebergTableName, Table> tables1 = Deencapsulation.getField(catalog, "tables");
        tables1.put(new IcebergTableName("db", "tbl"), initTable);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        ConnectContext ctx = new ConnectContext();

        pool.submit(() -> catalog.refreshTable("db", "tbl", ctx, null));
        firstStarted.await(2, TimeUnit.SECONDS);
        pool.submit(() -> catalog.refreshTable("db", "tbl", ctx, null));

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        Assertions.assertEquals(1, maxConcurrentRefreshes.get(),
                "concurrent refreshes on same table must be serialized (max concurrent should be 1)");
    }

    /**
     * Two concurrent refreshTable calls on DIFFERENT tables must not block each other:
     * both should overlap in time.
     *
     * <p>A CyclicBarrier forces both threads to rendezvous inside the mock before either
     * proceeds, so the "max concurrent" reading is deterministic even under GC pressure.
     * No wall-clock assertion is used.
     */
    @Test
    public void testRefreshTableDifferentTablesRunInParallel() throws Exception {
        AtomicInteger maxConcurrent = new AtomicInteger(0);
        AtomicInteger concurrent = new AtomicInteger(0);
        // Barrier ensures both threads have incremented the counter before either continues.
        CyclicBarrier barrier = new CyclicBarrier(2);

        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        Mockito.when(delegate.getTable(Mockito.any(), Mockito.eq("db"), Mockito.anyString()))
                .thenAnswer(inv -> {
                    int c = concurrent.incrementAndGet();
                    maxConcurrent.accumulateAndGet(c, Math::max);
                    // Wait until the other thread also reaches this point, guaranteeing overlap.
                    //
                    // Why 5 s: in the happy path both threads reach here in microseconds (the
                    // code path is a handful of ConcurrentHashMap ops + one synchronized block
                    // on different lock objects).  The timeout only fires when the lock is
                    // catalog-wide and permanently blocks the second thread — the regression we
                    // want to catch.
                    //
                    // Trade-off: a shorter timeout reduces false-negative latency when the
                    // regression is present, but risks a false-positive (flaky failure) under
                    // extreme GC pressure.  5 s is conservative enough to absorb even a full
                    // GC pause while still keeping test feedback fast.
                    try {
                        barrier.await(5, TimeUnit.SECONDS);
                    } catch (BrokenBarrierException | java.util.concurrent.TimeoutException e) {
                        throw new RuntimeException(
                                "Barrier timed out — second thread never reached the barrier. "
                                        + "This indicates catalog-wide locking is blocking concurrent "
                                        + "refreshes of different tables.", e);
                    }
                    concurrent.decrementAndGet();

                    TableOperations ops = Mockito.mock(TableOperations.class);
                    TableMetadata meta = Mockito.mock(TableMetadata.class);
                    Mockito.when(ops.current()).thenReturn(meta);
                    Mockito.when(meta.metadataFileLocation()).thenReturn("loc-" + UUID.randomUUID());
                    Snapshot snap = Mockito.mock(Snapshot.class);
                    Mockito.when(snap.snapshotId()).thenReturn(1L);
                    Mockito.when(snap.dataManifests(Mockito.any())).thenReturn(List.of());
                    Mockito.when(meta.currentSnapshot()).thenReturn(snap);
                    return new BaseTable(ops, "db." + inv.getArgument(2));
                });

        CachingIcebergCatalog catalog = new CachingIcebergCatalog(
                CATALOG_NAME, delegate, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());

        Cache<IcebergTableName, Table> tables2 = Deencapsulation.getField(catalog, "tables");
        // Pre-populate cache for both tables so refreshTable enters the update branch.
        for (String tbl : List.of("tbl1", "tbl2")) {
            TableOperations initOps = Mockito.mock(TableOperations.class);
            TableMetadata initMeta = Mockito.mock(TableMetadata.class);
            Snapshot initSnap = Mockito.mock(Snapshot.class);
            Mockito.when(initOps.current()).thenReturn(initMeta);
            Mockito.when(initMeta.metadataFileLocation()).thenReturn("loc-initial-" + tbl);
            Mockito.when(initMeta.currentSnapshot()).thenReturn(initSnap);
            Mockito.when(initSnap.snapshotId()).thenReturn(0L);
            Mockito.when(initSnap.dataManifests(Mockito.any())).thenReturn(List.of());
            tables2.put(new IcebergTableName("db", tbl), new BaseTable(initOps, "db." + tbl));
        }

        ExecutorService pool = Executors.newFixedThreadPool(2);
        ConnectContext ctx = new ConnectContext();

        pool.submit(() -> catalog.refreshTable("db", "tbl1", ctx, null));
        pool.submit(() -> catalog.refreshTable("db", "tbl2", ctx, null));

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        // The barrier above guarantees deterministic overlap: if the lock were catalog-wide,
        // the second thread would block on synchronized() and the barrier would time out,
        // causing the test to fail with BrokenBarrierException before reaching this line.
        Assertions.assertEquals(2, maxConcurrent.get(),
                "refreshes on different tables should overlap (max concurrent should be 2)");
    }

    @Test
    public void testLoadLargePartitionSetTriggersDiagnosticLog(@Mocked IcebergCatalog delegate,
                                                               @Mocked IcebergCatalogProperties props,
                                                               @Mocked ConnectContext ctx) throws Exception {
        // Build a partition map exceeding PARTITION_LOAD_LOG_THRESHOLD (10000) so the diagnostic
        // INFO branch in the partition cache loader is exercised.
        Map<String, Partition> bigPartitions = new HashMap<>();
        for (int i = 0; i <= 10000; i++) {
            bigPartitions.put("p" + i, new Partition(0L, 0L));
        }

        PartitionSpec spec = Mockito.mock(PartitionSpec.class);
        Mockito.when(spec.fields()).thenReturn(java.util.Collections.emptyList());
        Mockito.when(spec.isUnpartitioned()).thenReturn(false);

        BaseTable nativeTable = (BaseTable) createBaseTableWithManifests(1, 0, spec);
        TableMetadata meta = nativeTable.operations().current();
        Mockito.when(meta.specsById()).thenReturn(Map.of(0, spec));

        Snapshot currentSnap = meta.currentSnapshot();
        Map<String, String> summary = new HashMap<>();
        summary.put(SnapshotSummary.TOTAL_DATA_FILES_PROP, "5");
        summary.put(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0");
        Mockito.when(currentSnap.snapshotId()).thenReturn(42L);
        Mockito.when(currentSnap.summary()).thenReturn(summary);

        new Expectations() {
            {
                props.isEnableIcebergMetadataCache();
                result = true;
                props.getIcebergTableCacheTtlSec();
                result = 60L;
                props.isEnableIcebergTableCache();
                result = true;
                props.getIcebergTableCacheMemoryUsageRatio();
                result = 1.0;
                props.getIcebergDataFileCacheMemoryUsageRatio();
                result = 0.0;
                props.getIcebergDeleteFileCacheMemoryUsageRatio();
                result = 0.0;

                delegate.getTable((ConnectContext) any, "db", "t");
                result = nativeTable;
                minTimes = 0;

                delegate.getPartitions((IcebergTable) any, -1L, null);
                result = bigPartitions;
                minTimes = 1;
            }
        };

        ExecutorService es = Executors.newSingleThreadExecutor();
        try {
            CachingIcebergCatalog catalog = new CachingIcebergCatalog("c0", delegate, props, es);
            IcebergTable icebergTable = IcebergTable.builder()
                    .setSrTableName("t")
                    .setCatalogDBName("db")
                    .setCatalogTableName("t")
                    .setNativeTable(nativeTable)
                    .build();

            // snapshotId == -1 forces the loader to fall back to nativeTable.currentSnapshot()
            // for the logged snapshot id and summary.
            Map<String, Partition> result = catalog.getPartitions(icebergTable, -1L, null);
            Assertions.assertEquals(10001, result.size());
        } finally {
            es.shutdownNow();
        }
    }
}
