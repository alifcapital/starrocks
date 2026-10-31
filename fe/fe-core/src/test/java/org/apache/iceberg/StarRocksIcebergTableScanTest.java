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
import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.PlanMode;
import com.starrocks.connector.iceberg.DataFileWrapper;
import com.starrocks.connector.iceberg.StarRocksIcebergTableScanContext;
import com.starrocks.connector.iceberg.TestTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class StarRocksIcebergTableScanTest {
    @Test
    public void testWarmDeleteManifestKeepsBothKindsAndSequenceNumbers(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.unpartitioned();
        org.apache.iceberg.io.FileIO io = Mockito.mock(org.apache.iceberg.io.FileIO.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(io.newInputFile(Mockito.anyString())).thenAnswer(
                invocation -> Files.localInput(new java.io.File((String) invocation.getArgument(0))));
        Mockito.when(io.newInputFile(Mockito.anyString(), Mockito.anyLong())).thenAnswer(
                invocation -> Files.localInput(new java.io.File((String) invocation.getArgument(0))));
        ManifestWriter<DeleteFile> writer = ManifestFiles.writeDeleteManifest(
                2, spec, Files.localOutput(dir.resolve("delete.avro").toFile()), 42L);
        try (writer) {
            writer.add(FileMetadata.deleteFileBuilder(spec).ofEqualityDeletes(1)
                    .withPath("eq.parquet").withFormat(FileFormat.PARQUET).withRecordCount(2)
                    .withFileSizeInBytes(100).build(), 7L);
            writer.add(FileMetadata.deleteFileBuilder(spec).ofPositionDeletes()
                    .withPath("pos.parquet").withFormat(FileFormat.PARQUET).withRecordCount(1)
                    .withFileSizeInBytes(100).build(), 8L);
        }
        ManifestFile manifest = writer.toManifestFile();
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.io()).thenReturn(io);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.specs()).thenReturn(java.util.Map.of(spec.specId(), spec));
        Cache<String, Set<DeleteFile>> cache = com.github.benmanes.caffeine.cache.Caffeine.newBuilder().build();
        com.starrocks.connector.iceberg.StarRocksIcebergTableScanContext context =
                new com.starrocks.connector.iceberg.StarRocksIcebergTableScanContext(
                        "catalog", "db", "tbl", com.starrocks.connector.PlanMode.LOCAL);
        context.setDeleteFileCache(cache);
        context.setMetaFileCacheMap(new java.util.concurrent.ConcurrentHashMap<>());
        StarRocksIcebergTableScan scan = new StarRocksIcebergTableScan(table, schema, TableScanContext.empty(), context);
        scan.refreshDeleteFileCache(java.util.List.of(manifest));
        Set<DeleteFile> files = StarRocksIcebergTableScan.getCompleteCachedFiles(cache, manifest);
        Assertions.assertNotNull(files);
        Assertions.assertEquals(2, files.size());
        Assertions.assertTrue(files.stream().anyMatch(f -> f.content() == FileContent.EQUALITY_DELETES &&
                f.equalityFieldIds().equals(java.util.List.of(1)) && f.dataSequenceNumber() == 7L));
        Assertions.assertTrue(files.stream().anyMatch(f -> f.content() == FileContent.POSITION_DELETES &&
                f.dataSequenceNumber() == 8L));
        // A failed refill must not replace a valid entry with a placeholder or partial set.
        java.nio.file.Files.delete(dir.resolve("delete.avro"));
        Assertions.assertThrows(RuntimeException.class, () -> scan.refreshDeleteFileCache(java.util.List.of(manifest)));
        Assertions.assertSame(files, cache.getIfPresent(manifest.path()));
        io.close();
    }

    @Test
    public void testGetCompleteCachedFilesReturnsMatchingCacheEntry() {
        ManifestFile manifest = mockManifestFile("matching-manifest", 1, 1);

        @SuppressWarnings("unchecked")
        Cache<String, Set<DataFile>> cache = Mockito.mock(Cache.class);
        Set<DataFile> files = Set.of(Mockito.mock(DataFile.class), Mockito.mock(DataFile.class));
        Mockito.when(cache.getIfPresent(manifest.path())).thenReturn(files);

        Set<DataFile> cachedFiles = StarRocksIcebergTableScan.getCompleteCachedFiles(cache, manifest);

        Assertions.assertSame(files, cachedFiles);
        Mockito.verify(cache, Mockito.never()).invalidate(manifest.path());
    }

    @Test
    public void testGetCompleteCachedFilesInvalidatesPartialCacheEntry() {
        ManifestFile manifest = mockManifestFile("partial-manifest", 2, 1);

        @SuppressWarnings("unchecked")
        Cache<String, Set<DataFile>> cache = Mockito.mock(Cache.class);
        Set<DataFile> files = Set.of(Mockito.mock(DataFile.class), Mockito.mock(DataFile.class));
        Mockito.when(cache.getIfPresent(manifest.path())).thenReturn(files);

        Set<DataFile> cachedFiles = StarRocksIcebergTableScan.getCompleteCachedFiles(cache, manifest);

        Assertions.assertNull(cachedFiles);
        Mockito.verify(cache, Mockito.times(1)).invalidate(manifest.path());
    }

    @Test
    public void testGetCompleteCachedFilesRejectsEmptyPlaceholderWhenCountsUnknown() {
        ManifestFile manifest = mockManifestFile("placeholder-manifest", null, null);

        @SuppressWarnings("unchecked")
        Cache<String, Set<DataFile>> cache = Mockito.mock(Cache.class);
        Set<DataFile> files = Set.of();
        Mockito.when(cache.getIfPresent(manifest.path())).thenReturn(files);

        Set<DataFile> cachedFiles = StarRocksIcebergTableScan.getCompleteCachedFiles(cache, manifest);

        Assertions.assertNull(cachedFiles);
        Mockito.verify(cache, Mockito.times(1)).invalidate(manifest.path());
    }

    @Test
    public void testGetCompleteCachedFilesKeepsNonEmptyCacheWhenCountsUnknown() {
        ManifestFile manifest = mockManifestFile("unknown-count-manifest", null, null);

        @SuppressWarnings("unchecked")
        Cache<String, Set<DeleteFile>> cache = Mockito.mock(Cache.class);
        Set<DeleteFile> files = Set.of(Mockito.mock(DeleteFile.class));
        Mockito.when(cache.getIfPresent(manifest.path())).thenReturn(files);

        Set<DeleteFile> cachedFiles = StarRocksIcebergTableScan.getCompleteCachedFiles(cache, manifest);

        Assertions.assertSame(files, cachedFiles);
        Mockito.verify(cache, Mockito.never()).invalidate(manifest.path());
    }

    @Test
    public void testStatsKeepColumnIdsCoversPartitionAndSortColumns() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "data", Types.StringType.get()),
                Types.NestedField.required(3, "extra", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("data").build();
        // Two sort orders to cover sort-order evolution: "id" exists only in the older order.
        SortOrder oldOrder = SortOrder.builderFor(schema).asc("id").build();
        SortOrder currentOrder = SortOrder.builderFor(schema).asc("data").build();
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.spec()).thenReturn(spec);
        Mockito.when(table.specs()).thenReturn(ImmutableMap.of(spec.specId(), spec));
        Mockito.when(table.sortOrders()).thenReturn(ImmutableMap.of(1, oldOrder, 2, currentOrder));

        Set<Integer> keep = StarRocksIcebergTableScan.statsKeepColumnIds(table, schema);

        Assertions.assertTrue(keep.contains(2), "partition source column kept");
        Assertions.assertTrue(keep.contains(1), "sort source column from an evolved order kept");
        Assertions.assertFalse(keep.contains(3), "non-key column dropped");
    }

    // The on-read completeness check compares only cardinality, so a cache entry that lost one live file
    // but is padded back to the expected size is served as "complete" and silently drops rows.
    @Test
    public void testCompletenessCheckIsContentBlindToSizeMatchingWrongFileSet() {
        // Manifest advertises three live files; the true set is {f1, f2, f3}.
        ManifestFile manifest = mockManifestFile("three-live-files-manifest", 1, 2);
        DataFile f1 = Mockito.mock(DataFile.class);
        DataFile f2 = Mockito.mock(DataFile.class);
        DataFile f3 = Mockito.mock(DataFile.class);

        // Corrupted entry: f3 is missing but a stale file pads the size back to three.
        DataFile stale = Mockito.mock(DataFile.class);
        Set<DataFile> corrupted = Set.of(f1, f2, stale);
        Assertions.assertFalse(corrupted.contains(f3), "precondition: a genuine live file is absent");

        Assertions.assertTrue(StarRocksIcebergTableScan.isCompleteCachedFiles(manifest, corrupted),
                "size-only check accepts a wrong-content set of the expected size -- the check is content-blind");

        @SuppressWarnings("unchecked")
        Cache<String, Set<DataFile>> cache = Mockito.mock(Cache.class);
        Mockito.when(cache.getIfPresent(manifest.path())).thenReturn(corrupted);

        Set<DataFile> served = StarRocksIcebergTableScan.getCompleteCachedFiles(cache, manifest);
        Assertions.assertSame(corrupted, served, "the corrupted entry is served to the query verbatim");
        Mockito.verify(cache, Mockito.never()).invalidate(manifest.path());
    }

    private static final Schema FOOTER_SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    private static DataFile footerDataFile(String name) {
        return DataFiles.builder(PartitionSpec.unpartitioned()).withPath(name).withFileSizeInBytes(100)
                .withRecordCount(10).withFormat(FileFormat.PARQUET).build();
    }

    private static BaseTable footerTable(java.nio.file.Path dir, boolean mergeManifestsOnEveryAppend) {
        BaseTable table = TestTables.create(dir.resolve(UUID.randomUUID().toString()).toFile(),
                UUID.randomUUID().toString(), FOOTER_SCHEMA, PartitionSpec.unpartitioned(), 2);
        if (mergeManifestsOnEveryAppend) {
            table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();
        }
        return table;
    }

    private static long append(BaseTable table, String name) {
        table.newAppend().appendFile(footerDataFile(name)).commit();
        return table.currentSnapshot().snapshotId();
    }

    private static StarRocksIcebergTableScanContext footerContext(PlanMode planMode,
                                                                  Cache<String, Set<DataFile>> dataFileCache) {
        StarRocksIcebergTableScanContext context =
                new StarRocksIcebergTableScanContext("catalog", "db", "tbl", planMode);
        if (dataFileCache != null) {
            context.setDataFileCache(dataFileCache);
            context.setDeleteFileCache(com.github.benmanes.caffeine.cache.Caffeine.newBuilder().build());
            context.setMetaFileCacheMap(new ConcurrentHashMap<>());
        }
        return context;
    }

    private static StarRocksIcebergTableScanContext footerContext(Cache<String, Set<DataFile>> dataFileCache) {
        return footerContext(PlanMode.LOCAL, dataFileCache);
    }

    private static Set<String> planned(BaseTable table, long snapshotId, StarRocksIcebergTableScanContext context)
            throws Exception {
        StarRocksIcebergTableScan scan =
                new StarRocksIcebergTableScan(table, table.schema(), TableScanContext.empty(), context);
        Set<String> paths = new TreeSet<>();
        try (CloseableIterable<FileScanTask> tasks = scan.useSnapshot(snapshotId).planFiles()) {
            for (FileScanTask task : tasks) {
                paths.add(task.file().path().toString());
            }
        }
        return paths;
    }

    private static Cache<String, Set<DataFile>> newDataFileCache() {
        return com.github.benmanes.caffeine.cache.Caffeine.newBuilder().build();
    }

    // The scan reads manifests with the data file cache absent, cold and already filled, and each of them
    // must plan the same files.
    private static void assertFooterPlan(BaseTable table, long snapshotId, long baseSnapshotId,
                                         Set<String> expected) throws Exception {
        StarRocksIcebergTableScanContext uncached = footerContext(null);
        uncached.setFooterBaseSnapshotId(baseSnapshotId);
        Assertions.assertEquals(expected, planned(table, snapshotId, uncached), "without the data file cache");

        Cache<String, Set<DataFile>> cache = newDataFileCache();
        StarRocksIcebergTableScanContext cold = footerContext(cache);
        cold.setFooterBaseSnapshotId(baseSnapshotId);
        Assertions.assertEquals(expected, planned(table, snapshotId, cold), "with a cold data file cache");

        // A restricted plan must leave complete cache entries, since ordinary queries read them as they are.
        Set<String> all = planned(table, snapshotId, footerContext(null));
        Assertions.assertEquals(all, planned(table, snapshotId, footerContext(cache)));
        for (ManifestFile manifest : table.snapshot(snapshotId).dataManifests(table.io())) {
            Assertions.assertNotNull(StarRocksIcebergTableScan.getCompleteCachedFiles(cache, manifest));
        }

        StarRocksIcebergTableScanContext warm = footerContext(cache);
        warm.setFooterBaseSnapshotId(baseSnapshotId);
        Assertions.assertEquals(expected, planned(table, snapshotId, warm), "with a warm data file cache");
    }

    @Test
    public void testFooterWarmupPlansOnlyFilesAddedAfterBase(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        BaseTable table = footerTable(dir, false);
        append(table, "a.parquet");
        long base = append(table, "b.parquet");
        append(table, "c.parquet");
        long current = append(table, "d.parquet");

        assertFooterPlan(table, current, base, Set.of("c.parquet", "d.parquet"));
        // Nothing was added after the current snapshot itself.
        assertFooterPlan(table, current, current, Set.of());
    }

    @Test
    public void testFooterWarmupDropsOldEntriesOfMergedManifest(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        BaseTable table = footerTable(dir, true);
        append(table, "a.parquet");
        long base = append(table, "b.parquet");
        append(table, "c.parquet");
        long current = append(table, "d.parquet");

        // The merge rewrote the old files into the manifest of the newest snapshot as existing entries; without
        // this the test would not reach the entry filter.
        List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
        Assertions.assertEquals(1, manifests.size());
        Assertions.assertEquals(current, manifests.get(0).snapshotId());
        Assertions.assertEquals(3, manifests.get(0).existingFilesCount());

        assertFooterPlan(table, current, base, Set.of("c.parquet", "d.parquet"));
    }

    @Test
    public void testFooterWarmupSkipsFilesKeptByManifestRewrite(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        BaseTable table = footerTable(dir, false);
        append(table, "a.parquet");
        long base = append(table, "b.parquet");
        // Without a cluster function Iceberg keeps the manifests as they are.
        table.rewriteManifests().rewriteIf(manifest -> true).clusterBy(file -> "all").commit();
        long rewritten = table.currentSnapshot().snapshotId();

        // The rewrite wrote every manifest and kept both files as existing entries; without this the test would
        // not reach manifests of a new snapshot that hold only old files.
        List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
        Assertions.assertTrue(manifests.stream().allMatch(manifest -> manifest.snapshotId() == rewritten));
        Assertions.assertEquals(2, manifests.stream().mapToInt(ManifestFile::existingFilesCount).sum());

        // The rewrite adds no file, so there is nothing to warm.
        assertFooterPlan(table, rewritten, base, Set.of());
        long current = append(table, "c.parquet");
        assertFooterPlan(table, current, base, Set.of("c.parquet"));
    }

    @Test
    public void testFooterWarmupPlansNothingWhenBaseIsNotAnAncestor(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        BaseTable table = footerTable(dir, false);
        long first = append(table, "a.parquet");
        long abandoned = append(table, "b.parquet");
        table.manageSnapshots().rollbackTo(first).commit();
        long current = append(table, "c.parquet");

        // The base is on a history the table left, so we cannot tell which files are new.
        assertFooterPlan(table, current, abandoned, Set.of());
        // A base that the table does not know, as after the snapshot expired.
        assertFooterPlan(table, current, 123456789L, Set.of());
        assertFooterPlan(table, current, first, Set.of("c.parquet"));
    }

    @Test
    public void testScanWithoutFooterRestrictionPlansEveryFile(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        for (boolean merge : List.of(false, true)) {
            BaseTable table = footerTable(dir, merge);
            append(table, "a.parquet");
            append(table, "b.parquet");
            long current = append(table, "c.parquet");
            Set<String> all = Set.of("a.parquet", "b.parquet", "c.parquet");

            StarRocksIcebergTableScanContext context = footerContext(null);
            Assertions.assertNull(context.getFooterBaseSnapshotId());
            Assertions.assertEquals(all, planned(table, current, context));
            Assertions.assertEquals(all, planned(table, current, footerContext(newDataFileCache())));
        }
    }

    @Test
    public void testFooterWarmupPlansLocallyWhateverThePlanMode(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        BaseTable table = footerTable(dir, false);
        append(table, "a.parquet");
        long base = append(table, "b.parquet");
        long current = append(table, "c.parquet");

        // Distributed planning would hand the snapshot to the BE, which ignores the manifests chosen here.
        for (Cache<String, Set<DataFile>> cache : java.util.Arrays.asList(null, newDataFileCache())) {
            StarRocksIcebergTableScanContext context = footerContext(PlanMode.DISTRIBUTED, cache);
            context.setFooterBaseSnapshotId(base);
            Assertions.assertEquals(Set.of("c.parquet"), planned(table, current, context));
        }
    }

    @Test
    public void testFooterWarmupKeepsCachedFilesWithoutProvenance(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws Exception {
        BaseTable table = footerTable(dir, true);
        append(table, "a.parquet");
        long base = append(table, "b.parquet");
        long current = append(table, "c.parquet");

        Cache<String, Set<DataFile>> cache = newDataFileCache();
        Assertions.assertEquals(3, planned(table, current, footerContext(cache)).size());
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        Set<DataFile> withProvenance = cache.getIfPresent(manifest.path());
        Assertions.assertNotNull(withProvenance);
        Assertions.assertTrue(withProvenance.stream()
                .allMatch(file -> ((DataFileWrapper) file).entrySnapshotId() != null));
        StarRocksIcebergTableScanContext restricted = footerContext(cache);
        restricted.setFooterBaseSnapshotId(base);
        Assertions.assertEquals(Set.of("c.parquet"), planned(table, current, restricted));

        // A file cached by a reader that did not record the snapshot of its entry could be a new file, and
        // dropping it would leave its footer cold, so it is warmed even if it is old.
        Set<DataFile> withoutProvenance = new HashSet<>();
        for (DataFile file : withProvenance) {
            withoutProvenance.add(DataFileWrapper.wrap(file.copy()));
        }
        cache.put(manifest.path(), withoutProvenance);
        StarRocksIcebergTableScanContext unknown = footerContext(cache);
        unknown.setFooterBaseSnapshotId(base);
        Assertions.assertEquals(Set.of("a.parquet", "b.parquet", "c.parquet"), planned(table, current, unknown));
    }

    private ManifestFile mockManifestFile(String path, Integer existingFilesCount, Integer addedFilesCount) {
        ManifestFile manifest = Mockito.mock(ManifestFile.class);
        Mockito.when(manifest.path()).thenReturn(path);
        Mockito.when(manifest.existingFilesCount()).thenReturn(existingFilesCount);
        Mockito.when(manifest.addedFilesCount()).thenReturn(addedFilesCount);
        return manifest;
    }
}
