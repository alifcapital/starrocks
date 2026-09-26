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
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.iceberg.types.Types.NestedField.required;

public class ManifestReaderTest {
    private static final Schema TEST_SCHEMA =
            new Schema(required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));
    private static final PartitionSpec TEST_SPEC = PartitionSpec.unpartitioned();
    private static final FileIO FILE_IO = new LocalFileIO();
    private static final List<String> SCAN_COLUMNS_WITHOUT_STATS = List.of(
            DataFile.CONTENT.name(),
            DataFile.FILE_PATH.name(),
            DataFile.FILE_FORMAT.name(),
            DataFile.PARTITION_NAME,
            DataFile.RECORD_COUNT.name(),
            DataFile.FILE_SIZE.name(),
            DataFile.SPLIT_OFFSETS.name(),
            DataFile.SORT_ORDER_ID.name(),
            DataFile.SPEC_ID.name());

    @TempDir
    Path tempDir;

    @Test
    public void testFillCacheIfNeededWritesCompleteFilesWhenCacheEntryDisappearsMidIteration() throws IOException {
        DataFile file1 = newDataFile("data-file-1.parquet", 10L);
        DataFile file2 = newDataFile("data-file-2.parquet", 20L);
        ManifestFile manifest = writeManifest("complete-cache.avro", file1, file2);

        @SuppressWarnings("unchecked")
        Cache<String, Set<DataFile>> dataFileCache = Mockito.mock(Cache.class);
        Set<DataFile> placeholder = ConcurrentHashMap.newKeySet();
        AtomicInteger getCount = new AtomicInteger(0);
        Mockito.when(dataFileCache.getIfPresent(manifest.path()))
                .thenAnswer(invocation -> getCount.getAndIncrement() == 0 ? placeholder : null);

        ConcurrentHashMap<String, Set<DataFile>> stored = new ConcurrentHashMap<>();
        Mockito.when(dataFileCache.asMap()).thenReturn(stored);

        ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, Map.of(TEST_SPEC.specId(), TEST_SPEC))
                .select(ManifestReader.ALL_COLUMNS)
                .dataFileCache(dataFileCache)
                .cacheWithMetrics(false);

        try (CloseableIterable<ManifestEntry<DataFile>> entries = reader.liveEntries();
                CloseableIterator<ManifestEntry<DataFile>> iterator = entries.iterator()) {
            while (iterator.hasNext()) {
                iterator.next();
            }
        }

        Mockito.verify(dataFileCache, Mockito.times(1)).getIfPresent(manifest.path());
        Mockito.verify(dataFileCache, Mockito.times(1)).asMap();
        Assertions.assertTrue(placeholder.isEmpty(), "placeholder should stay empty until the full manifest is ready");
        Assertions.assertNotNull(stored.get(manifest.path()), "a fully materialized cache entry should be published on close");
        Assertions.assertEquals(Set.of(file1.location(), file2.location()),
                stored.get(manifest.path()).stream().map(DataFile::location).collect(Collectors.toSet()));
    }

    @Test
    public void testFillCacheWithMetricsProjectsStatsWhenScanDoesNotRequestStats() throws IOException {
        DataFile file = newDataFileWithStats("data-file-with-stats.parquet", 10L);
        ManifestFile manifest = writeManifest("cache-with-stats.avro", file);

        @SuppressWarnings("unchecked")
        Cache<String, Set<DataFile>> dataFileCache = Mockito.mock(Cache.class);
        Set<DataFile> placeholder = ConcurrentHashMap.newKeySet();
        Mockito.when(dataFileCache.getIfPresent(manifest.path())).thenReturn(placeholder);

        ConcurrentHashMap<String, Set<DataFile>> stored = new ConcurrentHashMap<>();
        Mockito.when(dataFileCache.asMap()).thenReturn(stored);

        ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, Map.of(TEST_SPEC.specId(), TEST_SPEC))
                .select(SCAN_COLUMNS_WITHOUT_STATS)
                .dataFileCache(dataFileCache)
                .cacheWithMetrics(true);

        try (CloseableIterable<ManifestEntry<DataFile>> entries = reader.liveEntries();
                CloseableIterator<ManifestEntry<DataFile>> iterator = entries.iterator()) {
            while (iterator.hasNext()) {
                iterator.next();
            }
        }

        Assertions.assertNotNull(stored.get(manifest.path()), "cache entry should be published after full consumption");
        DataFile cachedFile = stored.get(manifest.path()).iterator().next();
        Assertions.assertNotNull(cachedFile.lowerBounds(), "cached data file should keep lower bounds");
        Assertions.assertEquals(file.lowerBounds(), cachedFile.lowerBounds());
        Assertions.assertEquals(file.upperBounds(), cachedFile.upperBounds());
        Assertions.assertEquals(file.valueCounts(), cachedFile.valueCounts());
        Assertions.assertEquals(file.nullValueCounts(), cachedFile.nullValueCounts());
    }

    @Test
    public void testFillCacheIfNeededSkipsPublishingPartialDataOnEarlyClose() throws IOException {
        DataFile file1 = newDataFile("partial-file-1.parquet", 10L);
        DataFile file2 = newDataFile("partial-file-2.parquet", 20L);
        ManifestFile manifest = writeManifest("partial-close.avro", file1, file2);

        @SuppressWarnings("unchecked")
        Cache<String, Set<DataFile>> dataFileCache = Mockito.mock(Cache.class);
        Set<DataFile> placeholder = ConcurrentHashMap.newKeySet();
        Mockito.when(dataFileCache.getIfPresent(manifest.path())).thenReturn(placeholder);

        ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, Map.of(TEST_SPEC.specId(), TEST_SPEC))
                .select(ManifestReader.ALL_COLUMNS)
                .dataFileCache(dataFileCache)
                .cacheWithMetrics(false);

        try (CloseableIterable<ManifestEntry<DataFile>> entries = reader.liveEntries();
                CloseableIterator<ManifestEntry<DataFile>> iterator = entries.iterator()) {
            Assertions.assertTrue(iterator.hasNext());
            iterator.next();
        }

        Mockito.verify(dataFileCache, Mockito.times(1)).getIfPresent(manifest.path());
        Mockito.verify(dataFileCache, Mockito.never()).asMap();
        Assertions.assertTrue(placeholder.isEmpty(), "partial iteration must not leak partially cached files");
    }

    private StarRocksIcebergTableScan cachedScan(ManifestFile manifest, Cache<String, Set<DataFile>> cache,
                                                FileIO io, boolean selective) {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.name()).thenReturn("db.tbl");
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(table.schema()).thenReturn(TEST_SCHEMA);
        PartitionSpec currentSpec = selective
                ? PartitionSpec.builderFor(TEST_SCHEMA).withSpecId(1).identity("data").build() : TEST_SPEC;
        Mockito.when(table.spec()).thenReturn(currentSpec);
        Mockito.when(table.specs()).thenReturn(selective
                ? Map.of(TEST_SPEC.specId(), TEST_SPEC, currentSpec.specId(), currentSpec)
                : Map.of(TEST_SPEC.specId(), TEST_SPEC));
        Mockito.when(table.io()).thenReturn(io);
        Mockito.when(table.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.snapshotId()).thenReturn(1L);
        Mockito.when(snapshot.dataManifests(Mockito.any())).thenReturn(List.of(manifest));
        Mockito.when(snapshot.deleteManifests(Mockito.any())).thenReturn(List.of());
        Mockito.when(table.sortOrders()).thenReturn(selective
                ? Map.of(1, SortOrder.builderFor(TEST_SCHEMA).asc("data").build()) : Map.of());
        com.starrocks.connector.iceberg.StarRocksIcebergTableScanContext context =
                new com.starrocks.connector.iceberg.StarRocksIcebergTableScanContext(
                        "catalog", "db", "tbl", com.starrocks.connector.PlanMode.LOCAL);
        context.setDataFileCache(cache);
        context.setDataFileCacheWithMetrics(true);
        context.setMetaFileCacheMap(new ConcurrentHashMap<>());
        return new StarRocksIcebergTableScan(table, TEST_SCHEMA, TableScanContext.empty(), context);
    }

    @Test
    public void testFullStatsReusedWithoutOpeningManifest() throws IOException {
        DataFile file = newDataFileWithStats("full.parquet", 10L);
        ManifestFile manifest = writeManifest("full.avro", file);
        Cache<String, Set<DataFile>> cache = com.github.benmanes.caffeine.cache.Caffeine.newBuilder().build();
        FileIO io = Mockito.spy(new LocalFileIO());
        StarRocksIcebergTableScan scan = cachedScan(manifest, cache, io, false);
        scan.refreshDataFileCache(List.of(manifest));
        Assertions.assertTrue(com.starrocks.connector.iceberg.DataFileWrapper.hasFullColumnStats(
                cache.getIfPresent(manifest.path())));
        Mockito.doThrow(new AssertionError("Warm full statistics must not read S3"))
                .when(io).newInputFile(Mockito.anyString());
        try (CloseableIterable<FileScanTask> tasks = scan.includeColumnStats().planFiles()) {
            FileScanTask task = tasks.iterator().next();
            Assertions.assertEquals(file.lowerBounds(), task.file().lowerBounds());
            Assertions.assertEquals(file.nullValueCounts(), task.file().nullValueCounts());
        }
    }

    @Test
    public void testUnpartitionedSortedIdentifierTableKeepsFullStats() throws IOException {
        for (boolean warmup : List.of(false, true)) {
            DataFile file = newDataFileWithStats("unpartitioned-" + warmup + ".parquet", 10L);
            ManifestFile manifest = writeManifest("unpartitioned-" + warmup + ".avro", file);
            Cache<String, Set<DataFile>> cache = com.github.benmanes.caffeine.cache.Caffeine.newBuilder().build();
            FileIO io = Mockito.spy(new LocalFileIO());
            StarRocksIcebergTableScan scan = cachedScan(manifest, cache, io, false);
            Schema schema = new Schema(TEST_SCHEMA.columns(), Set.of(2));
            Mockito.when(scan.table().schema()).thenReturn(schema);
            Mockito.when(scan.table().sortOrders()).thenReturn(
                    Map.of(1, SortOrder.builderFor(schema).asc("data").build()));
            // Even a historical partition spec must not truncate a currently unpartitioned table.
            PartitionSpec oldSpec = PartitionSpec.builderFor(schema).withSpecId(1).identity("data").build();
            Mockito.when(scan.table().specs()).thenReturn(Map.of(0, TEST_SPEC, 1, oldSpec));
            Assertions.assertTrue(StarRocksIcebergTableScan.statsKeepColumnIds(scan.table(), schema).isEmpty());
            if (warmup) {
                scan.refreshDataFileCache(List.of(manifest));
            } else {
                try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                    tasks.forEach(task -> Assertions.assertEquals(10L, task.file().recordCount()));
                }
            }
            Set<DataFile> cached = cache.getIfPresent(manifest.path());
            Assertions.assertTrue(com.starrocks.connector.iceberg.DataFileWrapper.hasFullColumnStats(cached));
            Assertions.assertEquals(file.lowerBounds(), cached.iterator().next().lowerBounds());
            Mockito.doThrow(new AssertionError("Full statistics must be reused without reopening the manifest"))
                    .when(io).newInputFile(Mockito.anyString());
            try (CloseableIterable<FileScanTask> tasks = scan.includeColumnStats().planFiles()) {
                Assertions.assertEquals(file.lowerBounds(), tasks.iterator().next().file().lowerBounds());
            }
        }
    }

    @Test
    public void testPartialStatsUpgradedAndNotDowngradedByWarmup() throws IOException {
        DataFile file = newDataFileWithStats("upgrade.parquet", 10L);
        ManifestFile manifest = writeManifest("upgrade.avro", file);
        Cache<String, Set<DataFile>> cache = com.github.benmanes.caffeine.cache.Caffeine.newBuilder().build();
        FileIO io = Mockito.spy(new LocalFileIO());
        StarRocksIcebergTableScan scan = cachedScan(manifest, cache, io, true);
        scan.refreshDataFileCache(List.of(manifest));
        Assertions.assertFalse(com.starrocks.connector.iceberg.DataFileWrapper.hasFullColumnStats(
                cache.getIfPresent(manifest.path())));
        // Partitioning and sorting on column 2 do not keep the statistics of column 1.
        DataFile partial = cache.getIfPresent(manifest.path()).iterator().next();
        Assertions.assertTrue(partial.lowerBounds() == null || !partial.lowerBounds().containsKey(1));
        try (CloseableIterable<FileScanTask> tasks = scan.includeColumnStats().planFiles()) {
            for (FileScanTask task : tasks) {
                Assertions.assertEquals(file.lowerBounds(), task.file().lowerBounds());
            }
        }
        Set<DataFile> full = cache.getIfPresent(manifest.path());
        Assertions.assertTrue(com.starrocks.connector.iceberg.DataFileWrapper.hasFullColumnStats(full));
        scan.refreshDataFileCache(List.of(manifest));
        Assertions.assertSame(full, cache.getIfPresent(manifest.path()), "ordinary warmup must not downgrade full stats");
        Mockito.doThrow(new AssertionError("Upgraded statistics must not read S3"))
                .when(io).newInputFile(Mockito.anyString());
        try (CloseableIterable<FileScanTask> tasks = scan.includeColumnStats().planFiles()) {
            Assertions.assertEquals(file.lowerBounds(), tasks.iterator().next().file().lowerBounds());
        }
    }

    @Test
    public void testMixedManifestsReadOnlyTheOneMissingFullStats() throws IOException {
        ManifestFile warm = writeManifest("already-full.avro", newDataFileWithStats("warm.parquet", 10L));
        ManifestFile partial = writeManifest("still-partial.avro", newDataFileWithStats("partial.parquet", 20L));
        Cache<String, Set<DataFile>> cache = com.github.benmanes.caffeine.cache.Caffeine.newBuilder().build();
        FileIO io = Mockito.spy(new LocalFileIO());
        cachedScan(warm, cache, io, false).refreshDataFileCache(List.of(warm));
        StarRocksIcebergTableScan scan = cachedScan(partial, cache, io, true);
        scan.refreshDataFileCache(List.of(partial));
        Mockito.when(scan.table().currentSnapshot().dataManifests(Mockito.any())).thenReturn(List.of(warm, partial));
        Mockito.clearInvocations(io);
        Mockito.doThrow(new AssertionError("The full manifest must not be reopened"))
                .when(io).newInputFile(warm.path());
        long rows = 0;
        try (CloseableIterable<FileScanTask> tasks = scan.includeColumnStats().planFiles()) {
            for (FileScanTask task : tasks) {
                rows += task.file().recordCount();
            }
        }
        Assertions.assertEquals(30L, rows);
        Mockito.verify(io, Mockito.atLeastOnce()).newInputFile(partial.path());
        Assertions.assertTrue(com.starrocks.connector.iceberg.DataFileWrapper.hasFullColumnStats(
                cache.getIfPresent(partial.path())));
    }

    @Test
    public void testWriterMissingMetricsAreStillComplete() throws IOException {
        ManifestFile manifest = writeManifest("no-metrics.avro", newDataFile("no-metrics.parquet", 10L));
        Cache<String, Set<DataFile>> cache = com.github.benmanes.caffeine.cache.Caffeine.newBuilder().build();
        FileIO io = Mockito.spy(new LocalFileIO());
        StarRocksIcebergTableScan scan = cachedScan(manifest, cache, io, false);
        scan.refreshDataFileCache(List.of(manifest));
        Assertions.assertTrue(com.starrocks.connector.iceberg.DataFileWrapper.hasFullColumnStats(
                cache.getIfPresent(manifest.path())));
        Mockito.doThrow(new AssertionError("Writer omitted metrics; rereading cannot recover them"))
                .when(io).newInputFile(Mockito.anyString());
        try (CloseableIterable<FileScanTask> tasks = scan.includeColumnStats().planFiles()) {
            Assertions.assertEquals(10L, tasks.iterator().next().file().recordCount());
        }
    }

    private ManifestFile writeManifest(String manifestFileName, DataFile... dataFiles) throws IOException {
        File manifestFile = tempDir.resolve(manifestFileName).toFile();
        OutputFile outputFile = FILE_IO.newOutputFile(manifestFile.getCanonicalPath());
        ManifestWriter<DataFile> writer = ManifestFiles.write(1, TEST_SPEC, outputFile, 1L);
        try {
            for (DataFile dataFile : dataFiles) {
                writer.add(dataFile);
            }
        } finally {
            writer.close();
        }
        return writer.toManifestFile();
    }

    private DataFile newDataFile(String fileName, long recordCount) {
        return DataFiles.builder(TEST_SPEC)
                .withPath(tempDir.resolve(fileName).toString())
                .withFileSizeInBytes(64L)
                .withRecordCount(recordCount)
                .build();
    }

    private DataFile newDataFileWithStats(String fileName, long recordCount) {
        Metrics metrics = new Metrics(
                recordCount,
                Map.of(1, 4L),
                Map.of(1, recordCount),
                Map.of(1, 0L),
                null,
                Map.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1)),
                Map.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 10)));
        return DataFiles.builder(TEST_SPEC)
                .withPath(tempDir.resolve(fileName).toString())
                .withFileSizeInBytes(64L)
                .withRecordCount(recordCount)
                .withMetrics(metrics)
                .build();
    }

    private static final class LocalFileIO implements FileIO {
        @Override
        public InputFile newInputFile(String path) {
            return Files.localInput(path);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            return Files.localOutput(path);
        }

        @Override
        public void deleteFile(String path) {
            if (!new File(path).delete()) {
                throw new RuntimeIOException("Failed to delete file: " + path);
            }
        }

        @Override
        public Map<String, String> properties() {
            return Maps.newHashMap();
        }
    }
}
