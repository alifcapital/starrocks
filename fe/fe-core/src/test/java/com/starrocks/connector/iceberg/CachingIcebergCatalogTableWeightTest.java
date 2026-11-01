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
import com.starrocks.memory.estimate.Estimator;
import com.starrocks.qe.ConnectContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class CachingIcebergCatalogTableWeightTest {
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    private static final IcebergTableName KEY = new IcebergTableName("db", "tbl");

    @TempDir
    File tempDir;

    private ExecutorService workers;
    private IcebergCatalog delegate;
    private CachingIcebergCatalog catalog;

    @BeforeEach
    public void setUp() {
        workers = Executors.newFixedThreadPool(2);
        delegate = Mockito.mock(IcebergCatalog.class);
        catalog = new CachingIcebergCatalog("iceberg_catalog", delegate,
                CachingIcebergCatalogTest.DEFAULT_CATALOG_PROPERTIES, workers);
    }

    @AfterEach
    public void tearDown() {
        workers.shutdownNow();
    }

    // A table operations object that holds an object shared with other tables, like a catalog client or
    // a file IO with its caches.
    private static class SharedStateOps implements TableOperations {
        private final TableMetadata metadata;
        private final Object shared;

        SharedStateOps(TableMetadata metadata, Object shared) {
            this.metadata = metadata;
            this.shared = shared;
        }

        @Override
        public TableMetadata current() {
            return metadata;
        }

        @Override
        public TableMetadata refresh() {
            return metadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata updated) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIO io() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String metadataFileLocation(String fileName) {
            return fileName;
        }

        @Override
        public LocationProvider locationProvider() {
            throw new UnsupportedOperationException();
        }
    }

    private static TableMetadata emptyMetadata() {
        return TableMetadata.newTableMetadata(SCHEMA, PartitionSpec.unpartitioned(), "file:/tmp/tbl", Map.of());
    }

    private static BaseTable tableSharing(Object shared) {
        return new BaseTable(new SharedStateOps(emptyMetadata(), shared), "tbl");
    }

    // Caffeine applies the weight of a written entry to the size of the cache when it drains its write
    // buffer, so we drain it before we read the size.
    private static long weightedSize(Cache<IcebergTableName, Table> tables) {
        tables.cleanUp();
        return tables.policy().eviction().orElseThrow().weightedSize().orElseThrow();
    }

    private int weigh(Table table) {
        return Deencapsulation.invoke(catalog, "weighTableEntry", KEY, table);
    }

    // We fear that weighing a table walks objects that other threads change while they serve other tables,
    // and fails the load of a healthy table with ConcurrentModificationException. So the weight must not
    // read anything but the metadata of the table.
    @Test
    public void testWeightDoesNotReadSharedObjectsThatOtherThreadsModify() throws Exception {
        LinkedList<Object> shared = new LinkedList<>();
        for (int i = 0; i < 2000; i++) {
            shared.add(new Object());
        }
        BaseTable table = tableSharing(shared);

        AtomicBoolean stop = new AtomicBoolean(false);
        Thread modifier = new Thread(() -> {
            while (!stop.get()) {
                shared.add(new Object());
                shared.removeFirst();
            }
        });
        modifier.setDaemon(true);
        modifier.start();
        try {
            long deadline = System.nanoTime() + 3_000_000_000L;
            while (System.nanoTime() < deadline) {
                weigh(table);
            }
        } finally {
            stop.set(true);
            modifier.join();
        }
    }

    // We want two tables that use the same client or file IO to be charged only for their own metadata,
    // so the weight must not depend on the size of what they share.
    @Test
    public void testWeightDoesNotDependOnSharedObjects() {
        byte[] bigShared = new byte[8 * 1024 * 1024];
        BaseTable first = tableSharing(bigShared);
        BaseTable second = tableSharing(bigShared);
        BaseTable noShared = tableSharing(new byte[0]);

        Assertions.assertEquals(weigh(noShared), weigh(first));
        Assertions.assertEquals(weigh(first), weigh(second));
        Assertions.assertTrue(weigh(first) < 1024 * 1024);
    }

    // The snapshots supplier of a lazily loaded REST table captures the catalog client. We want it not to be
    // charged to the table.
    @Test
    public void testSnapshotsSupplierIsNotWalked() {
        byte[] capturedByClient = new byte[8 * 1024 * 1024];
        TableMetadata lazy = TableMetadata.buildFrom(emptyMetadata())
                .setSnapshotsSupplier(() -> {
                    Assertions.assertNotNull(capturedByClient);
                    return List.of();
                })
                .discardChanges()
                .build();

        Assertions.assertTrue(Estimator.estimate(lazy) < 1024 * 1024);
        Assertions.assertTrue(weigh(new BaseTable(new SharedStateOps(lazy, null), "tbl")) < 1024 * 1024);
    }

    private BaseTable createTableWithSnapshots() {
        String location = new File(tempDir, "tbl").getAbsolutePath();
        Table created = new HadoopTables(new Configuration()).create(SCHEMA, PartitionSpec.unpartitioned(), location);
        for (int i = 0; i < 3; i++) {
            DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(location + "/data/f" + i + ".parquet")
                    .withFileSizeInBytes(1024)
                    .withRecordCount(10)
                    .build();
            created.newAppend().appendFile(file).commit();
        }
        // A table loaded from the metadata file has snapshots whose manifest lists are not read yet.
        return (BaseTable) new HadoopTables(new Configuration()).load(location);
    }

    // We fear that the memory tracker walks the cached tables and reads what they share. So the size that the
    // catalog reports for its table cache must be the sum of the cache weights, whatever the tables share.
    @Test
    public void testEstimateSizeDoesNotWalkSharedObjects() {
        Cache<IcebergTableName, Table> tables = Deencapsulation.getField(catalog, "tables");
        long empty = catalog.estimateSize();

        tables.put(KEY, tableSharing(new byte[8 * 1024 * 1024]));

        long weights = weightedSize(tables);
        Assertions.assertTrue(weights > 0);
        Assertions.assertEquals(empty + weights, catalog.estimateSize());
    }

    // We want the budget of the cache to count the manifest lists that reads load into the snapshots of a
    // cached table, so the weight must grow with them, and putting the same table again must make the cache
    // weigh it once more.
    @Test
    public void testManifestListsLoadedByReadsCountAfterPutOfSameTable() {
        BaseTable table = createTableWithSnapshots();
        Cache<IcebergTableName, Table> tables = Deencapsulation.getField(catalog, "tables");

        int before = weigh(table);
        Assertions.assertEquals(
                Estimator.estimate(KEY) + Estimator.estimate(table.operations().current()), before);
        tables.put(KEY, table);
        long cachedBefore = weightedSize(tables);
        Assertions.assertEquals(before, cachedBefore);

        for (Snapshot snapshot : table.snapshots()) {
            Assertions.assertFalse(snapshot.dataManifests(table.io()).isEmpty());
            snapshot.deleteManifests(table.io());
        }

        int after = weigh(table);
        Assertions.assertTrue(after > before, "weight " + after + " must exceed " + before);
        // The cache has not weighed the table again yet.
        Assertions.assertEquals(cachedBefore, weightedSize(tables));

        tables.put(KEY, table);
        Assertions.assertEquals(after, weightedSize(tables));
    }

    // We want the table cache to hold only BaseTable, because the weigher and the refresh read its operations.
    // So a table of another kind is returned to the caller and is not cached.
    @Test
    public void testTableThatIsNotBaseTableIsNotCached() {
        Table other = Mockito.mock(Table.class);
        Mockito.when(delegate.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl"))).thenReturn(other);
        Cache<IcebergTableName, Table> tables = Deencapsulation.getField(catalog, "tables");

        Table loaded = catalog.getTable(new ConnectContext(), "db", "tbl");

        Assertions.assertSame(other, loaded);
        Assertions.assertNull(tables.getIfPresent(KEY));
    }

    @Test
    public void testBaseTableIsCached() {
        BaseTable table = tableSharing(null);
        Mockito.when(delegate.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl"))).thenReturn(table);
        Cache<IcebergTableName, Table> tables = Deencapsulation.getField(catalog, "tables");

        Assertions.assertSame(table, catalog.getTable(new ConnectContext(), "db", "tbl"));
        Assertions.assertSame(table, tables.getIfPresent(KEY));
    }
}
