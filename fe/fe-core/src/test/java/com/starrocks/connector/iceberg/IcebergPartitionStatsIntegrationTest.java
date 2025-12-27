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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStats;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.StructProjection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IcebergPartitionStatsIntegrationTest extends TableTestBase {
    private static final String CATALOG_NAME = "iceberg_catalog";
    private static final String DB_NAME = "db";
    private static final String TABLE_NAME = "tbl";

    @Test
    public void testPartitionsFromStatsFileIncrementalMatchesPartitionsTable() throws Exception {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.refresh();
        Snapshot baseSnapshot = mockedNativeTableB.currentSnapshot();

        File statsFilePath = new File(temp, "partition-stats.parquet");
        PartitionStatisticsFile statsFile =
                writeStatsFile(mockedNativeTableB, baseSnapshot, statsFilePath);
        mockedNativeTableB.updatePartitionStatistics().setPartitionStatistics(statsFile).commit();

        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();
        long targetSnapshotId = mockedNativeTableB.currentSnapshot().snapshotId();

        IcebergTable icebergTable =
                new IcebergTable(
                        1,
                        "srTableName",
                        CATALOG_NAME,
                        "resource",
                        DB_NAME,
                        TABLE_NAME,
                        "",
                        Lists.newArrayList(),
                        mockedNativeTableB,
                        Maps.newHashMap());

        IcebergCatalog catalog = new TestIcebergCatalog();
        Map<String, Partition> statsResult = catalog.getPartitions(icebergTable, targetSnapshotId, null);
        Map<String, Partition> manifestResult =
                loadPartitionsViaPartitionsTable(mockedNativeTableB, targetSnapshotId);

        assertPartitionMapsEqual(manifestResult, statsResult);
    }

    private PartitionStatisticsFile writeStatsFile(
            Table table, Snapshot snapshot, File statsFilePath) throws IOException {
        List<ManifestFile> manifests = snapshot.allManifests(table.io());
        PartitionMap<PartitionStats> statsMap = PartitionStatsHandler.computeStats(table, manifests, false);

        OutputFile outputFile = table.io().newOutputFile(statsFilePath.getAbsolutePath());
        Schema schema = PartitionStatsHandler.schema(Partitioning.partitionType(table));
        try (FileAppender<PartitionStats> appender =
                     org.apache.iceberg.InternalData.write(FileFormat.PARQUET, outputFile)
                             .schema(schema)
                             .build()) {
            for (PartitionStats stat : statsMap.values()) {
                appender.add(stat);
            }
        }

        return org.apache.iceberg.ImmutableGenericPartitionStatisticsFile.builder()
                .path(statsFilePath.getAbsolutePath())
                .snapshotId(snapshot.snapshotId())
                .fileSizeInBytes(statsFilePath.length())
                .build();
    }

    private Map<String, Partition> loadPartitionsViaPartitionsTable(Table nativeTable, long snapshotId) {
        PartitionsTable partitionsTable = (PartitionsTable) MetadataTableUtils
                .createMetadataTableInstance(nativeTable, MetadataTableType.PARTITIONS);
        TableScan tableScan = partitionsTable.newScan();
        if (snapshotId != -1) {
            tableScan = tableScan.useSnapshot(snapshotId);
        }

        Map<String, Partition> partitionMap = Maps.newHashMap();
        try (CloseableIterable<FileScanTask> tasks = tableScan.planFiles()) {
            for (FileScanTask task : tasks) {
                CloseableIterable<StructLike> rows = task.asDataTask().rows();
                for (StructLike row : rows) {
                    StructProjection partitionData = row.get(0, StructProjection.class);
                    int specId = row.get(1, Integer.class);
                    PartitionSpec spec = nativeTable.specs().get(specId);
                    String partitionName =
                            PartitionUtil.convertIcebergPartitionToPartitionName(nativeTable, spec, partitionData);
                    Long lastUpdatedAt = row.get(9, Long.class);
                    long lastUpdated =
                            lastUpdatedAt != null ? lastUpdatedAt : nativeTable.currentSnapshot().timestampMillis();
                    partitionMap.put(partitionName, new Partition(lastUpdated, specId));
                }
            }
        } catch (IOException e) {
            throw new StarRocksConnectorException("Failed to read partitions table", e);
        }

        return partitionMap;
    }

    private void assertPartitionMapsEqual(Map<String, Partition> expected, Map<String, Partition> actual) {
        Assertions.assertEquals(expected.keySet(), actual.keySet());
        for (Map.Entry<String, Partition> entry : expected.entrySet()) {
            Partition expectedPartition = entry.getValue();
            Partition actualPartition = actual.get(entry.getKey());
            Assertions.assertNotNull(actualPartition);
            Assertions.assertEquals(expectedPartition.getModifiedTime(), actualPartition.getModifiedTime());
            Assertions.assertEquals(expectedPartition.getSpecId(), actualPartition.getSpecId());
        }
    }

    private static final class TestIcebergCatalog implements IcebergCatalog {
        @Override
        public IcebergCatalogType getIcebergCatalogType() {
            return IcebergCatalogType.HIVE_CATALOG;
        }

        @Override
        public List<String> listAllDatabases(ConnectContext context) {
            return Lists.newArrayList();
        }

        @Override
        public com.starrocks.catalog.Database getDB(ConnectContext context, String dbName) {
            return null;
        }

        @Override
        public List<String> listTables(ConnectContext context, String dbName) {
            return Lists.newArrayList();
        }

        @Override
        public void renameTable(ConnectContext context, String dbName, String tblName, String newTblName)
                throws StarRocksConnectorException {
            throw new StarRocksConnectorException("renameTable not supported in test catalog");
        }

        @Override
        public org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                throws StarRocksConnectorException {
            throw new StarRocksConnectorException("getTable not supported in test catalog");
        }

        @Override
        public Map<String, String> loadNamespaceMetadata(ConnectContext context, org.apache.iceberg.catalog.Namespace ns) {
            return Maps.newHashMap();
        }
    }
}
