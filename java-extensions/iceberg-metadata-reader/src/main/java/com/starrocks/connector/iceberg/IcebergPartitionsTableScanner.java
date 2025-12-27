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

import com.google.common.collect.ImmutableList;
import com.starrocks.connector.share.iceberg.IcebergPartitionUtils;
import com.starrocks.connector.share.iceberg.IcebergPredicateInfo;
import com.starrocks.connector.share.iceberg.ManifestFileBean;
import com.starrocks.connector.share.iceberg.PartitionStatsSplitBean;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestEntryScanHelper;
import org.apache.iceberg.ManifestEntryWrapper;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStats;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.PartitionStatsScanHelper;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.StructProjection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.iceberg.util.SerializationUtil.deserializeFromBase64;

public class IcebergPartitionsTableScanner extends AbstractIcebergMetadataScanner {
    private static final Logger LOG = LogManager.getLogger(IcebergPartitionsTableScanner.class);
    protected static final List<String> SCAN_COLUMNS =
            ImmutableList.of("content", "partition", "file_size_in_bytes", "record_count");

    private final String manifestBean;
    private final String predicateInfo;
    private ManifestFile manifestFile;
    private CloseableIterator<ManifestEntryWrapper> reader;
    private List<PartitionField> partitionFields;
    private Long lastUpdateTime;
    private Long lastUpdatedSnapshotId;
    private Integer spedId;
    private Schema schema;
    private GenericRecord reusedRecord;
    private Iterator<PartitionStats> statsIterator;
    private boolean usingStatsFile;
    private Types.StructType unifiedPartitionType;
    private Expression predicate;
    private boolean predicateAlwaysTrue;
    private boolean predicateIsPartition;
    private boolean updateOnly;
    private final Map<Integer, Evaluator> evaluatorCache = new HashMap<>();
    private final Map<Integer, StructProjection> unifiedProjectionCache = new HashMap<>();
    private final Map<Long, Long> snapshotTimeCache = new HashMap<>();
    private long fallbackScanStartMs;
    private long fallbackEntries;
    private long fallbackMatched;
    private boolean fallbackLogged;
    private boolean fallbackExhausted;

    public IcebergPartitionsTableScanner(int fetchSize, Map<String, String> params) {
        super(fetchSize, params);
        this.manifestBean = params.get("split_info");
        this.predicateInfo = params.getOrDefault("serialized_predicate", "");
    }

    @Override
    public void doOpen() {
        Object split = deserializeFromBase64(manifestBean);
        this.schema = table.schema();
        this.partitionFields = IcebergPartitionUtils.getAllPartitionFields(table);
        this.reusedRecord = GenericRecord.create(getResultType());
        this.unifiedPartitionType = Partitioning.partitionType(table);
        if (predicateInfo.isEmpty()) {
            this.predicate = Expressions.alwaysTrue();
            this.predicateIsPartition = false;
        } else {
            Object predicateObj = deserializeFromBase64(predicateInfo);
            if (predicateObj instanceof IcebergPredicateInfo) {
                IcebergPredicateInfo predicateInfo = (IcebergPredicateInfo) predicateObj;
                this.predicate = predicateInfo.predicate();
                this.predicateIsPartition = predicateInfo.partitionPredicate();
            } else {
                this.predicate = (Expression) predicateObj;
                this.predicateIsPartition = false;
            }
        }
        this.predicateAlwaysTrue = predicate == Expressions.alwaysTrue();

        if (split instanceof PartitionStatsSplitBean) {
            this.usingStatsFile = true;
            PartitionStatsSplitBean statsSplit = (PartitionStatsSplitBean) split;
            this.updateOnly = statsSplit.isUpdateMode();
            LOG.debug("[IcebergPartitions] scanner using stats file. mode={}, stats_snapshot={}, target_snapshot={}, "
                            + "has_incremental={}",
                    statsSplit.getMode(), statsSplit.getStatsSnapshotId(), statsSplit.getTargetSnapshotId(),
                    statsSplit.hasIncrementalManifests());
            initStatsIterator(statsSplit);
        } else {
            this.manifestFile = (ManifestFile) split;
            this.spedId = manifestFile.partitionSpecId();
            this.lastUpdateTime = null;
            this.lastUpdatedSnapshotId = null;
            this.fallbackScanStartMs = System.currentTimeMillis();
            this.fallbackEntries = 0;
            this.fallbackMatched = 0;
            this.fallbackLogged = false;
            this.fallbackExhausted = false;
        }
    }

    @Override
    public int doGetNext() {
        int numRows = 0;
        while (numRows < getTableSize()) {
            if (usingStatsFile) {
                PartitionStats stat = nextMatchingStat();
                if (stat == null) {
                    break;
                }
                for (int i = 0; i < requiredFields.length; i++) {
                    Object fieldData = getFromStats(requiredFields[i], stat);
                    if (fieldData == null) {
                        appendData(i, null);
                    } else {
                        ColumnValue fieldValue = new IcebergMetadataColumnValue(fieldData, timezone);
                        appendData(i, fieldValue);
                    }
                }
            } else {
                ManifestEntryWrapper entry = nextMatchingEntry();
                if (entry == null) {
                    break;
                }
                ContentFile<?> file = entry.file();
                lastUpdatedSnapshotId = entry.snapshotId();
                lastUpdateTime = lookupSnapshotTime(lastUpdatedSnapshotId);
                for (int i = 0; i < requiredFields.length; i++) {
                    Object fieldData = get(requiredFields[i], file);
                    if (fieldData == null) {
                        appendData(i, null);
                    } else {
                        ColumnValue fieldValue = new IcebergMetadataColumnValue(fieldData, timezone);
                        appendData(i, fieldValue);
                    }
                }
            }
            numRows++;
        }
        return numRows;
    }

    @Override
    public void doClose() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (!usingStatsFile && !fallbackLogged) {
            logFallbackScan(false);
        }
        reusedRecord = null;
    }

    @Override
    protected void initReader() {
        if (usingStatsFile) {
            return;
        }
        Map<Integer, PartitionSpec> specs = table.specs();
        reader = ManifestEntryScanHelper.open(manifestFile, fileIO, specs, SCAN_COLUMNS);
    }

    private Types.StructType getResultType() {
        List<Types.NestedField> fields = new ArrayList<>();
        for (PartitionField partitionField : partitionFields) {
            int id = partitionField.fieldId();
            String name = partitionField.name();
            Type type = partitionField.transform().getResultType(schema.findType(partitionField.sourceId()));
            Types.NestedField nestedField = Types.NestedField.optional(id, name, type);
            fields.add(nestedField);
        }
        return Types.StructType.of(fields);
    }

    private Object get(String columnName, ContentFile<?> file) {
        FileContent content = file.content();
        PartitionSpec spec = table.specs().get(file.specId());
        PartitionData partitionData = (PartitionData) file.partition();
        switch (columnName) {
            case "partition_value":
                return getPartitionValues(partitionData, spec.partitionType());
            case "spec_id":
                return spedId;
            case "record_count":
                return content == FileContent.DATA ? file.recordCount() : 0;
            case "file_count":
                return content == FileContent.DATA ? 1L : 0L;
            case "total_data_file_size_in_bytes":
                return content == FileContent.DATA ? file.fileSizeInBytes() : 0;
            case "position_delete_record_count":
                return content == FileContent.POSITION_DELETES ? file.recordCount() : 0;
            case "position_delete_file_count":
                return content == FileContent.POSITION_DELETES ? 1L : 0L;
            case "equality_delete_record_count":
                return content == FileContent.EQUALITY_DELETES ? file.recordCount() : 0;
            case "equality_delete_file_count":
                return content == FileContent.EQUALITY_DELETES ? 1L : 0L;
            case "last_updated_at":
                return lastUpdateTime;
            case "last_updated_snapshot_id":
                return lastUpdatedSnapshotId;
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }

    private Object getFromStats(String columnName, PartitionStats stat) {
        if (updateOnly) {
            boolean specMatchesCurrent = stat.specId() == table.spec().specId();
            switch (columnName) {
                case "partition_value":
                    return getPartitionValues(stat.partition(), unifiedPartitionType);
                case "spec_id":
                    return stat.specId();
                case "last_updated_at":
                    return specMatchesCurrent ? stat.lastUpdatedAt() : null;
                case "last_updated_snapshot_id":
                    return specMatchesCurrent ? stat.lastUpdatedSnapshotId() : null;
                case "record_count":
                case "file_count":
                case "total_data_file_size_in_bytes":
                case "position_delete_record_count":
                case "position_delete_file_count":
                case "equality_delete_record_count":
                case "equality_delete_file_count":
                    return 0L;
                default:
                    throw new IllegalArgumentException("Unrecognized column name " + columnName);
            }
        }

        switch (columnName) {
            case "partition_value":
                return getPartitionValues(stat.partition(), unifiedPartitionType);
            case "spec_id":
                return stat.specId();
            case "record_count":
                return stat.dataRecordCount();
            case "file_count":
                return (long) stat.dataFileCount();
            case "total_data_file_size_in_bytes":
                return stat.totalDataFileSizeInBytes();
            case "position_delete_record_count":
                return stat.positionDeleteRecordCount();
            case "position_delete_file_count":
                return (long) stat.positionDeleteFileCount();
            case "equality_delete_record_count":
                return stat.equalityDeleteRecordCount();
            case "equality_delete_file_count":
                return (long) stat.equalityDeleteFileCount();
            case "last_updated_at":
                return stat.lastUpdatedAt();
            case "last_updated_snapshot_id":
                return stat.lastUpdatedSnapshotId();
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }

    private Object getPartitionValues(StructLike partitionData, Types.StructType partitionType) {
        List<Types.NestedField> fileFields = partitionType.fields();
        Map<Integer, Integer> fieldIdToPos = new HashMap<>();
        for (int i = 0; i < fileFields.size(); i++) {
            fieldIdToPos.put(fileFields.get(i).fieldId(), i);
        }

        for (PartitionField partitionField : partitionFields) {
            Integer fieldId = partitionField.fieldId();
            String name = partitionField.name();
            if (fieldIdToPos.containsKey(fieldId)) {
                int pos = fieldIdToPos.get(fieldId);
                Type fieldType = fileFields.get(pos).type();
                Object partitionValue = partitionData.get(pos, Object.class);
                if (partitionField.transform().isIdentity() && Types.TimestampType.withZone().equals(fieldType)) {
                    partitionValue = partitionValue == null ? null : ((long) partitionValue) / 1000;
                }
                reusedRecord.setField(name, partitionValue);
            } else {
                reusedRecord.setField(name, null);
            }
        }
        return reusedRecord;
    }

    private void initStatsIterator(PartitionStatsSplitBean statsSplit) {
        long startMs = System.currentTimeMillis();
        try {
            if (statsSplit.isDeltaMode()) {
                PartitionMap<PartitionStats> deltaMap = computeDeltaStats(statsSplit);
                LOG.debug("[IcebergPartitions] stats delta only. delta_partitions={}, elapsed_ms={}",
                        deltaMap.size(), System.currentTimeMillis() - startMs);
                this.statsIterator = deltaMap.values().iterator();
                return;
            }

            PartitionMap<PartitionStats> statsMap = readStatsFileToMap(statsSplit.getStatsFilePath());
            int baseCount = statsMap.size();
            if (statsSplit.isBaseMode()) {
                LOG.debug("[IcebergPartitions] stats base only. base_partitions={}, elapsed_ms={}",
                        baseCount, System.currentTimeMillis() - startMs);
                this.statsIterator = statsMap.values().iterator();
                return;
            }

            if (statsSplit.hasIncrementalManifests()) {
                List<ManifestFile> manifests = new ArrayList<>();
                for (ManifestFileBean bean : statsSplit.getIncrementalManifests()) {
                    manifests.add(bean);
                }
                LOG.debug("[IcebergPartitions] stats incremental apply. manifests_to_apply={}", manifests.size());
                long deltaReadStartMs = System.currentTimeMillis();
                PartitionMap<PartitionStats> incrementalMap =
                        PartitionStatsScanHelper.computeStatsFromManifests(
                                table, manifests, unifiedPartitionType, true);
                long deltaReadMs = System.currentTimeMillis() - deltaReadStartMs;
                int incrementalCount = incrementalMap.size();
                long mergeStartMs = System.currentTimeMillis();
                mergeIncrementalStats(statsMap, incrementalMap);
                long mergeMs = System.currentTimeMillis() - mergeStartMs;
                LOG.debug("[IcebergPartitions] stats incremental applied. base_partitions={}, incremental_partitions={}, "
                                + "merged_partitions={}, delta_read_ms={}, merge_ms={}, elapsed_ms={}",
                        baseCount, incrementalCount, statsMap.size(),
                        deltaReadMs, mergeMs, System.currentTimeMillis() - startMs);
            } else {
                LOG.debug("[IcebergPartitions] stats file only. base_partitions={}, elapsed_ms={}",
                        baseCount, System.currentTimeMillis() - startMs);
            }
            this.statsIterator = statsMap.values().iterator();
        } catch (Exception e) {
            throw new RuntimeException("Failed to read partition stats", e);
        }
    }

    private PartitionMap<PartitionStats> computeDeltaStats(PartitionStatsSplitBean statsSplit) {
        if (!statsSplit.hasIncrementalManifests()) {
            return PartitionMap.create(table.specs());
        }
        List<ManifestFile> manifests = new ArrayList<>();
        for (ManifestFileBean bean : statsSplit.getIncrementalManifests()) {
            manifests.add(bean);
        }
        LOG.debug("[IcebergPartitions] stats delta apply. manifests_to_apply={}", manifests.size());
        long deltaReadStartMs = System.currentTimeMillis();
        PartitionMap<PartitionStats> deltaMap =
                PartitionStatsScanHelper.computeStatsFromManifests(table, manifests, unifiedPartitionType, true);
        long deltaReadMs = System.currentTimeMillis() - deltaReadStartMs;
        LOG.debug("[IcebergPartitions] stats delta read. delta_partitions={}, delta_read_ms={}",
                deltaMap.size(), deltaReadMs);
        return deltaMap;
    }

    private PartitionMap<PartitionStats> readStatsFileToMap(String statsFilePath) throws IOException {
        int formatVersion = TableUtil.formatVersion(table);
        Schema schema = PartitionStatsHandler.schema(unifiedPartitionType, formatVersion);
        PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
        try (CloseableIterable<PartitionStats> statsIterable =
                     PartitionStatsHandler.readPartitionStatsFile(schema, table.io().newInputFile(statsFilePath))) {
            for (PartitionStats stat : statsIterable) {
                statsMap.put(stat.specId(), stat.partition(), stat);
            }
        }
        return statsMap;
    }

    private PartitionStats nextMatchingStat() {
        while (statsIterator != null && statsIterator.hasNext()) {
            PartitionStats stat = statsIterator.next();
            if (matchesPredicate(stat.specId(), stat.partition(), unifiedPartitionType)) {
                return stat;
            }
        }
        return null;
    }

    private ManifestEntryWrapper nextMatchingEntry() {
        while (reader != null && reader.hasNext()) {
            ManifestEntryWrapper entry = reader.next();
            fallbackEntries++;
            ContentFile<?> file = entry.file();
            PartitionSpec spec = table.specs().get(file.specId());
            if (spec == null) {
                continue;
            }
            if (matchesPredicate(file.specId(), file.partition(), spec.partitionType())) {
                fallbackMatched++;
                return entry;
            }
        }
        if (reader != null && !fallbackLogged) {
            fallbackExhausted = true;
            logFallbackScan(true);
        }
        return null;
    }

    private boolean matchesPredicate(int specId, StructLike partitionData, Types.StructType sourceType) {
        if (predicateAlwaysTrue) {
            return true;
        }
        PartitionSpec spec = table.specs().get(specId);
        if (spec == null) {
            return false;
        }
        Evaluator evaluator = evaluatorCache.computeIfAbsent(specId, id -> {
            Expression projection = predicate;
            if (!predicateIsPartition) {
                try {
                    projection = Projections.inclusive(spec, false).project(predicate);
                } catch (org.apache.iceberg.exceptions.ValidationException e) {
                    LOG.debug("[IcebergPartitions] predicate projection failed, using partition filter directly. " +
                                    "spec_id={}, predicate={}",
                            specId, predicate);
                }
            }
            return new Evaluator(spec.partitionType(), projection, false);
        });

        StructLike evalData = partitionData;
        if (!spec.partitionType().equals(sourceType)) {
            StructProjection projection = unifiedProjectionCache.computeIfAbsent(
                    specId,
                    id -> StructProjection.createAllowMissing(spec.partitionType(), sourceType));
            evalData = projection.wrap(partitionData);
        }
        return evaluator.eval(evalData);
    }

    private Long lookupSnapshotTime(Long snapshotId) {
        if (snapshotId == null) {
            return null;
        }
        if (snapshotTimeCache.containsKey(snapshotId)) {
            return snapshotTimeCache.get(snapshotId);
        }
        Long timestamp = null;
        if (table.snapshot(snapshotId) != null) {
            timestamp = table.snapshot(snapshotId).timestampMillis();
        }
        snapshotTimeCache.put(snapshotId, timestamp);
        return timestamp;
    }

    private void mergeIncrementalStats(
            PartitionMap<PartitionStats> base, PartitionMap<PartitionStats> incremental) {
        incremental.forEach(
                (key, value) ->
                        base.merge(
                                Pair.of(key.first(), partitionDataToRecord((PartitionData) key.second())),
                                value,
                                (existingEntry, newEntry) -> {
                                    existingEntry.appendStats(newEntry);
                                    return existingEntry;
                                }));
    }

    private GenericRecord partitionDataToRecord(PartitionData data) {
        GenericRecord record = GenericRecord.create(data.getPartitionType());
        for (int index = 0; index < record.size(); index++) {
            record.set(index, data.get(index));
        }
        return record;
    }

    private void logFallbackScan(boolean completed) {
        fallbackLogged = true;
        long elapsedMs = System.currentTimeMillis() - fallbackScanStartMs;
        String manifestPath = manifestFile == null ? "null" : manifestFile.path().toString();
        LOG.debug("[IcebergPartitions] fallback scan. manifest={}, entries={}, matched_entries={}, completed={}, " +
                        "elapsed_ms={}",
                manifestPath, fallbackEntries, fallbackMatched, completed && fallbackExhausted, elapsedMs);
    }
}
