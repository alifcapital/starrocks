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

import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.IcebergTableDescription;
import com.starrocks.catalog.Table;
import com.starrocks.planner.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.thrift.TTableDescriptor;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Detached display metadata: no Iceberg Table, snapshots, credentials or FileIO are retained. */
final class IcebergDiscoveryTable extends Table implements IcebergTableDescription {
    private final String catalogName;
    private final String dbName;
    private final List<String> partitionColumns;
    private final PartitionSpec partitionSpec;
    private final SortOrder sortOrder;
    private final List<Integer> sortKeyIndexes;
    private final Map<String, String> properties;
    private final String location;
    private final int formatVersion;

    private IcebergDiscoveryTable(IcebergTable source, String catalogName, String dbName) {
        super(source.getId(), source.getName(), TableType.ICEBERG, new ArrayList<>(source.getFullSchema()));
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.partitionColumns = new ArrayList<>(source.getPartitionColumnNames());
        partitionSpec = source.getNativeTable().spec();
        sortOrder = source.getSortOrder();
        sortKeyIndexes = List.copyOf(source.getSortKeyIndexes());
        properties = Map.copyOf(source.getProperties());
        location = source.getTableLocation();
        formatVersion = source.getFormatVersion();
        setComment(source.getComment());
    }

    static IcebergDiscoveryTable from(org.apache.iceberg.Table table, String catalogName,
                                      String dbName, String tableName, String catalogType) {
        return new IcebergDiscoveryTable(
                IcebergApiConverter.toIcebergTable(table, catalogName, dbName, tableName, catalogType),
                catalogName, dbName);
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getCatalogDBName() {
        return dbName;
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return partitionColumns;
    }

    @Override
    public boolean isUnPartitioned() {
        return partitionSpec.isUnpartitioned();
    }

    @Override
    public List<String> getPartitionColumnNamesWithTransform() {
        return IcebergApiConverter.toPartitionFields(partitionSpec, false);
    }

    @Override
    public SortOrder getSortOrder() {
        return sortOrder;
    }

    @Override
    public List<Integer> getSortKeyIndexes() {
        return sortKeyIndexes;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String getTableLocation() {
        return location;
    }

    @Override
    public int getFormatVersion() {
        return formatVersion;
    }

    @Override
    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        throw new UnsupportedOperationException("Discovery schemas cannot be used to scan data");
    }
}
