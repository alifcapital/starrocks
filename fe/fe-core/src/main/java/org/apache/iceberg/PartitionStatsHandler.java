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

// copy from https://github.com/apache/iceberg/blob/apache-iceberg-1.9.0/core/src/main/java/org/apache/iceberg/PartitionStatsHandler.java
// removed write methods, kept only read and compute methods for StarRocks getPartitions()

import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class PartitionStatsHandler {

    private PartitionStatsHandler() {}

    public static final int PARTITION_FIELD_ID = 1;
    public static final String PARTITION_FIELD_NAME = "partition";
    public static final NestedField SPEC_ID = NestedField.required(2, "spec_id", IntegerType.get());
    public static final NestedField DATA_RECORD_COUNT =
            NestedField.required(3, "data_record_count", LongType.get());
    public static final NestedField DATA_FILE_COUNT =
            NestedField.required(4, "data_file_count", IntegerType.get());
    public static final NestedField TOTAL_DATA_FILE_SIZE_IN_BYTES =
            NestedField.required(5, "total_data_file_size_in_bytes", LongType.get());
    public static final NestedField POSITION_DELETE_RECORD_COUNT =
            NestedField.optional(6, "position_delete_record_count", LongType.get());
    public static final NestedField POSITION_DELETE_FILE_COUNT =
            NestedField.optional(7, "position_delete_file_count", IntegerType.get());
    public static final NestedField EQUALITY_DELETE_RECORD_COUNT =
            NestedField.optional(8, "equality_delete_record_count", LongType.get());
    public static final NestedField EQUALITY_DELETE_FILE_COUNT =
            NestedField.optional(9, "equality_delete_file_count", IntegerType.get());
    public static final NestedField TOTAL_RECORD_COUNT =
            NestedField.optional(10, "total_record_count", LongType.get());
    public static final NestedField LAST_UPDATED_AT =
            NestedField.optional(11, "last_updated_at", LongType.get());
    public static final NestedField LAST_UPDATED_SNAPSHOT_ID =
            NestedField.optional(12, "last_updated_snapshot_id", LongType.get());

    public static Schema schema(StructType unifiedPartitionType) {
        Preconditions.checkState(!unifiedPartitionType.fields().isEmpty(), "Table must be partitioned");
        return new Schema(
                NestedField.required(PARTITION_FIELD_ID, PARTITION_FIELD_NAME, unifiedPartitionType),
                SPEC_ID,
                DATA_RECORD_COUNT,
                DATA_FILE_COUNT,
                TOTAL_DATA_FILE_SIZE_IN_BYTES,
                POSITION_DELETE_RECORD_COUNT,
                POSITION_DELETE_FILE_COUNT,
                EQUALITY_DELETE_RECORD_COUNT,
                EQUALITY_DELETE_FILE_COUNT,
                TOTAL_RECORD_COUNT,
                LAST_UPDATED_AT,
                LAST_UPDATED_SNAPSHOT_ID);
    }

    public static CloseableIterable<PartitionStats> readPartitionStatsFile(
            Schema schema, InputFile inputFile) {
        Preconditions.checkArgument(schema != null, "Invalid schema: null");
        Preconditions.checkArgument(inputFile != null, "Invalid input file: null");

        FileFormat fileFormat = FileFormat.fromFileName(inputFile.location());
        Preconditions.checkArgument(
                fileFormat != null, "Unable to determine format of file: %s", inputFile.location());

        CloseableIterable<StructLike> records =
                InternalData.read(fileFormat, inputFile).project(schema).build();
        return CloseableIterable.transform(records, PartitionStatsHandler::recordToPartitionStats);
    }

    private static PartitionStats recordToPartitionStats(StructLike record) {
        int pos = 0;
        PartitionStats stats =
                new PartitionStats(
                        record.get(pos++, StructLike.class),
                        record.get(pos++, Integer.class));
        for (; pos < record.size(); pos++) {
            stats.set(pos, record.get(pos, Object.class));
        }
        return stats;
    }

    public static PartitionStatisticsFile latestStatsFile(Table table, long snapshotId) {
        List<PartitionStatisticsFile> partitionStatisticsFiles = table.partitionStatisticsFiles();
        if (partitionStatisticsFiles.isEmpty()) {
            return null;
        }

        Map<Long, PartitionStatisticsFile> stats =
                partitionStatisticsFiles.stream()
                        .collect(Collectors.toMap(PartitionStatisticsFile::snapshotId, file -> file));
        for (Snapshot snapshot : SnapshotUtil.ancestorsOf(snapshotId, table::snapshot)) {
            if (stats.containsKey(snapshot.snapshotId())) {
                return stats.get(snapshot.snapshotId());
            }
        }

        // A stats file exists but isn't accessible from the current snapshot chain.
        // It may belong to a different snapshot reference (like a branch or tag).
        // Falling back to legacy computation.
        return null;
    }

    public static Collection<PartitionStats> computeAndMergeStatsIncremental(
            Table table,
            Snapshot toSnapshot,
            StructType partitionType,
            PartitionStatisticsFile previousStatsFile) {
        PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());

        try (CloseableIterable<PartitionStats> oldStats =
                     readPartitionStatsFile(
                             schema(partitionType),
                             table.io().newInputFile(previousStatsFile.path()))) {
            oldStats.forEach(
                    partitionStats ->
                            statsMap.put(partitionStats.specId(), partitionStats.partition(), partitionStats));
        } catch (Exception exception) {
            throw new RuntimeException("Failed to read previous stats file", exception);
        }

        PartitionMap<PartitionStats> incrementalStatsMap =
                computeStatsDiff(table, table.snapshot(previousStatsFile.snapshotId()), toSnapshot);

        incrementalStatsMap.forEach(
                (key, value) ->
                        statsMap.merge(
                                Pair.of(key.first(), partitionDataToRecord((PartitionData) key.second())),
                                value,
                                (existingEntry, newEntry) -> {
                                    existingEntry.appendStats(newEntry);
                                    return existingEntry;
                                }));

        return statsMap.values();
    }

    private static GenericRecord partitionDataToRecord(PartitionData data) {
        GenericRecord record = GenericRecord.create(data.getPartitionType());
        for (int index = 0; index < record.size(); index++) {
            record.set(index, data.get(index));
        }
        return record;
    }

    public static PartitionMap<PartitionStats> computeStatsDiff(
            Table table, Snapshot fromSnapshot, Snapshot toSnapshot) {
        Iterable<Snapshot> snapshots =
                SnapshotUtil.ancestorsBetween(
                        toSnapshot.snapshotId(), fromSnapshot.snapshotId(), table::snapshot);

        List<ManifestFile> manifests =
                StreamSupport.stream(snapshots.spliterator(), false)
                        .flatMap(
                                snapshot ->
                                        snapshot.allManifests(table.io()).stream()
                                                .filter(file -> file.snapshotId().equals(snapshot.snapshotId())))
                        .collect(Collectors.toList());

        return computeStats(table, manifests, true);
    }

    public static PartitionMap<PartitionStats> computeStats(
            Table table, List<ManifestFile> manifests, boolean incremental) {
        StructType partitionType = Partitioning.partitionType(table);
        Queue<PartitionMap<PartitionStats>> statsByManifest = Queues.newConcurrentLinkedQueue();
        Tasks.foreach(manifests)
                .stopOnFailure()
                .throwFailureWhenFinished()
                .executeWith(ThreadPools.getWorkerPool())
                .run(
                        manifest ->
                                statsByManifest.add(
                                        collectStatsForManifest(table, manifest, partitionType, incremental)));

        PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
        for (PartitionMap<PartitionStats> stats : statsByManifest) {
            mergePartitionMap(stats, statsMap);
        }

        return statsMap;
    }

    private static PartitionMap<PartitionStats> collectStatsForManifest(
            Table table, ManifestFile manifest, StructType partitionType, boolean incremental) {
        List<String> projection = BaseScan.scanColumns(manifest.content());
        try (ManifestReader<?> reader = ManifestFiles.open(manifest, table.io()).select(projection)) {
            PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
            int specId = manifest.partitionSpecId();
            PartitionSpec spec = table.specs().get(specId);
            PartitionData keyTemplate = new PartitionData(partitionType);

            for (ManifestEntry<?> entry : reader.entries()) {
                ContentFile<?> file = entry.file();
                StructLike coercedPartition =
                        PartitionUtil.coercePartition(partitionType, spec, file.partition());
                StructLike key = keyTemplate.copyFor(coercedPartition);
                Snapshot snapshot = table.snapshot(entry.snapshotId());
                PartitionStats stats =
                        statsMap.computeIfAbsent(
                                specId,
                                ((PartitionData) file.partition()).copy(),
                                () -> new PartitionStats(key, specId));
                if (entry.isLive()) {
                    if (!incremental || entry.status() == ManifestEntry.Status.ADDED) {
                        stats.liveEntry(file, snapshot);
                    }
                } else {
                    if (incremental) {
                        stats.deletedEntryForIncrementalCompute(file, snapshot);
                    } else {
                        stats.deletedEntry(snapshot);
                    }
                }
            }

            return statsMap;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void mergePartitionMap(
            PartitionMap<PartitionStats> fromMap, PartitionMap<PartitionStats> toMap) {
        fromMap.forEach(
                (key, value) ->
                        toMap.merge(
                                key,
                                value,
                                (existingEntry, newEntry) -> {
                                    existingEntry.appendStats(newEntry);
                                    return existingEntry;
                                }));
    }
}
