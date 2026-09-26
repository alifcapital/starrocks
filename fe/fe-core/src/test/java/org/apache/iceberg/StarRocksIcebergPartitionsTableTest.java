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

package org.apache.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.connector.PlanMode;
import com.starrocks.connector.iceberg.StarRocksIcebergTableScanContext;
import com.starrocks.connector.iceberg.TestTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class StarRocksIcebergPartitionsTableTest {
    @TempDir
    Path tempDir;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "part", Types.StringType.get()));

    private DataFile file(PartitionSpec spec, String name, String partition) {
        DataFiles.Builder builder = DataFiles.builder(spec).withPath(name).withFileSizeInBytes(100)
                .withRecordCount(10).withFormat(FileFormat.PARQUET);
        if (spec.isPartitioned()) {
            builder.withPartitionPath(partition);
        }
        return builder.build();
    }

    private List<String> rows(TableScan scan) throws Exception {
        List<String> result = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                try (CloseableIterable<StructLike> rows = task.asDataTask().rows()) {
                    for (StructLike row : rows) {
                        result.add(encode(row));
                    }
                }
            }
        }
        Collections.sort(result);
        return result;
    }

    private String encode(StructLike row) {
        List<String> cells = new ArrayList<>();
        for (int i = 0; i < row.size(); i++) {
            Object value = row.get(i, Object.class);
            cells.add(value instanceof StructLike ? encode((StructLike) value) : String.valueOf(value));
        }
        return cells.toString();
    }

    @Test
    public void testPartitionsAndFilePlanningShareManifestReads() throws Exception {
        for (boolean partitioned : List.of(false, true)) {
            PartitionSpec spec = partitioned ? PartitionSpec.builderFor(SCHEMA).identity("part").build()
                    : PartitionSpec.unpartitioned();
            BaseTable table = Mockito.spy(TestTables.create(tempDir.resolve(UUID.randomUUID().toString()).toFile(),
                    UUID.randomUUID().toString(), SCHEMA, spec, 2));
            table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();
            table.newAppend().appendFile(file(spec, "a.parquet", "part=a")).commit();
            long firstSnapshot = table.currentSnapshot().snapshotId();
            table.newAppend().appendFile(file(spec, "b.parquet", "part=b")).commit();
            if (partitioned) {
                table.updateSpec().addField("id").commit();
                table.newAppend().appendFile(file(table.spec(), "c.parquet", "part=c/id=1")).commit();
            }
            DeleteFile pos = FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
                    .withPath("pos.parquet").withFormat(FileFormat.PARQUET).withFileSizeInBytes(50)
                    .withRecordCount(1).withPartition(file(table.spec(), "dummy.parquet", "part=c/id=1").partition())
                    .build();
            DeleteFile eq = FileMetadata.deleteFileBuilder(table.spec()).ofEqualityDeletes(1)
                    .withPath("eq.parquet").withFormat(FileFormat.PARQUET).withFileSizeInBytes(50)
                    .withRecordCount(2).withPartition(pos.partition()).build();
            table.newRowDelta().addDeletes(pos).addDeletes(eq).commit();
            long currentSnapshot = table.currentSnapshot().snapshotId();
            List<String> expected = rows(new PartitionsTable(table).newScan().useSnapshot(currentSnapshot));
            List<String> expectedOld = rows(new PartitionsTable(table).newScan().useSnapshot(firstSnapshot));
            FileIO io = Mockito.spy(table.io());
            Mockito.doReturn(io).when(table).io();
            Cache<String, Set<DataFile>> data = Caffeine.newBuilder().build();
            Cache<String, Set<DeleteFile>> deletes = Caffeine.newBuilder().build();
            StarRocksIcebergTableScanContext context = new StarRocksIcebergTableScanContext(
                    "catalog", "db", "tbl", PlanMode.LOCAL);
            context.setDataFileCache(data);
            context.setDeleteFileCache(deletes);
            context.setDataFileCacheWithMetrics(true);
            context.setMetaFileCacheMap(new ConcurrentHashMap<>());
            StarRocksIcebergTableScan scan = new StarRocksIcebergTableScan(
                    table, table.schema(), TableScanContext.empty(), context);
            Assertions.assertEquals(expected, rows(scan.newPartitionScan().useSnapshot(currentSnapshot)));
            for (ManifestFile manifest : table.currentSnapshot().allManifests(io)) {
                Mockito.verify(io, Mockito.times(1)).newInputFile(manifest.path());
                Assertions.assertNotNull(manifest.content() == ManifestContent.DATA
                        ? data.getIfPresent(manifest.path()) : deletes.getIfPresent(manifest.path()));
            }
            for (ManifestFile manifest : table.currentSnapshot().allManifests(io)) {
                Mockito.doThrow(new AssertionError("A cached manifest must not be reopened"))
                        .when(io).newInputFile(manifest.path());
            }
            Assertions.assertEquals(expected, rows(scan.newPartitionScan().useSnapshot(currentSnapshot)));
            try (CloseableIterable<FileScanTask> tasks = scan.useSnapshot(currentSnapshot).planFiles()) {
                long records = 0;
                for (FileScanTask task : tasks) {
                    records += task.file().recordCount();
                }
                Assertions.assertEquals(partitioned ? 30 : 20, records);
            }
            // The reverse order must also reuse the manifest: SELECT first, then partition lookup.
            Mockito.doCallRealMethod().when(io).newInputFile(Mockito.anyString());
            data.invalidateAll();
            deletes.invalidateAll();
            try (CloseableIterable<FileScanTask> tasks = scan.useSnapshot(currentSnapshot).planFiles()) {
                tasks.forEach(task -> Assertions.assertEquals(10L, task.file().recordCount()));
            }
            for (ManifestFile manifest : table.currentSnapshot().allManifests(io)) {
                Mockito.doThrow(new AssertionError("Partition lookup must reuse SELECT's manifest cache"))
                        .when(io).newInputFile(manifest.path());
            }
            Assertions.assertEquals(expected, rows(scan.newPartitionScan().useSnapshot(currentSnapshot)));
            // Historical reads must still use the historical manifest set and entry snapshot IDs.
            Mockito.doCallRealMethod().when(io).newInputFile(Mockito.anyString());
            Assertions.assertEquals(expectedOld, rows(scan.newPartitionScan().useSnapshot(firstSnapshot)));
            table.expireSnapshots().expireSnapshotId(firstSnapshot).cleanExpiredFiles(false).commit();
            Assertions.assertNull(table.snapshot(firstSnapshot));
            List<String> afterExpiry = rows(new PartitionsTable(table).newScan().useSnapshot(currentSnapshot));
            for (ManifestFile manifest : table.currentSnapshot().allManifests(io)) {
                Mockito.doThrow(new AssertionError("Expired entry snapshots must not cause an Avro reread"))
                        .when(io).newInputFile(manifest.path());
            }
            Assertions.assertEquals(afterExpiry, rows(scan.newPartitionScan().useSnapshot(currentSnapshot)));
            Mockito.doCallRealMethod().when(io).newInputFile(Mockito.anyString());
            data.invalidateAll();
            deletes.invalidateAll();
            context.setOnlyReadCache(true);
            StarRocksIcebergTableScan readOnly = new StarRocksIcebergTableScan(
                    table, table.schema(), TableScanContext.empty(), context);
            Assertions.assertEquals(afterExpiry, rows(readOnly.newPartitionScan().useSnapshot(currentSnapshot)));
            Assertions.assertTrue(data.asMap().isEmpty());
            Assertions.assertTrue(deletes.asMap().isEmpty());
        }
    }
}
