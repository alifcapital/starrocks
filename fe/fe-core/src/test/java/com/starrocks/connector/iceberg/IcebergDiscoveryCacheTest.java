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
import com.starrocks.qe.ConnectContext;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

class IcebergDiscoveryCacheTest {
    private final java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();

    @org.junit.jupiter.api.AfterEach
    void cleanup() {
        executor.shutdownNow();
    }

    private Table table(String column) {
        Schema schema = new Schema(Types.NestedField.optional(1, column, Types.StringType.get(), "comment"));
        org.apache.iceberg.TableMetadata metadata = org.apache.iceberg.TableMetadata.newTableMetadata(schema,
                PartitionSpec.builderFor(schema).identity(column).build(),
                org.apache.iceberg.SortOrder.builderFor(schema).asc(column).build(),
                "s3://test/table", Map.of("format-version", "2", "comment", "table comment"));
        metadata = Mockito.spy(metadata);
        Mockito.doReturn("s3://test/" + column + ".metadata.json").when(metadata).metadataFileLocation();
        org.apache.iceberg.TableOperations operations = Mockito.mock(org.apache.iceberg.TableOperations.class);
        Mockito.when(operations.current()).thenReturn(metadata);
        return new org.apache.iceberg.BaseTable(operations, "db.tbl");
    }

    @Test
    void discoveryDoesNotPopulateFullCacheAndReadsDoNotPostponeSchemaRefresh() {
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        Mockito.when(delegate.getIcebergCatalogType()).thenReturn(IcebergCatalogType.GLUE_CATALOG);
        Table before = table("before");
        Table after = table("after");
        Mockito.when(delegate.getTable(any(), eq("db"), eq("tbl"))).thenReturn(before, after);
        CachingIcebergCatalog catalog = new CachingIcebergCatalog("cat", delegate,
                new IcebergCatalogProperties(Map.of("iceberg.catalog.type", "glue")), executor);
        AtomicLong clock = new AtomicLong();
        Deencapsulation.setField(catalog, "discoveryClock", (LongSupplier) clock::get);
        ConnectContext context = new ConnectContext();
        com.starrocks.catalog.Table first = catalog.getTableForDiscovery(context, "cat", "db", "tbl");
        assertEquals("before", first.getFullSchema().get(0).getName());
        assertEquals(List.of("before"), first.getPartitionColumnNames());
        assertEquals("comment", first.getFullSchema().get(0).getComment());
        assertEquals("cat", first.getCatalogName());
        assertEquals(com.starrocks.sql.analyzer.AstToStringBuilder.getExternalCatalogTableDdlStmt(
                        IcebergApiConverter.toIcebergTable(before, "cat", "db", "tbl", "GLUE_CATALOG")),
                com.starrocks.sql.analyzer.AstToStringBuilder.getExternalCatalogTableDdlStmt(first));
        assertThrows(UnsupportedOperationException.class, () -> first.toThrift(List.of()));
        clock.set(TimeUnit.MINUTES.toNanos(59));
        assertSame(first, catalog.getTableForDiscovery(context, "cat", "db", "tbl"));
        clock.set(TimeUnit.MINUTES.toNanos(61));
        assertEquals("after", catalog.getTableForDiscovery(context, "cat", "db", "tbl")
                .getFullSchema().get(0).getName());
        Cache<IcebergTableName, Table> tables = Deencapsulation.getField(catalog, "tables");
        assertNull(tables.getIfPresent(new IcebergTableName("db", "tbl")));
        Map<?, ?> activity = Deencapsulation.getField(catalog, "tableLatestAccessTime");
        assertEquals(0, activity.size());
        Mockito.verify(delegate, Mockito.times(2)).getTable(any(), eq("db"), eq("tbl"));
        Mockito.verify(delegate, Mockito.never()).getTableScan(any(), any());
    }

    @Test
    void coldExplicitRefreshAndInvalidationDiscardDiscoverySchema() {
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        Mockito.when(delegate.getIcebergCatalogType()).thenReturn(IcebergCatalogType.GLUE_CATALOG);
        Table a = table("a");
        Table b = table("b");
        Table c = table("c");
        Mockito.when(delegate.getTable(any(), eq("db"), eq("tbl"))).thenReturn(a, b, c);
        CachingIcebergCatalog catalog = new CachingIcebergCatalog("cat", delegate,
                new IcebergCatalogProperties(Map.of("iceberg.catalog.type", "glue")), executor);
        ConnectContext context = new ConnectContext();
        assertEquals("a", catalog.getTableForDiscovery(context, "cat", "db", "tbl")
                .getFullSchema().get(0).getName());
        catalog.refreshTable("db", "tbl", context);
        assertEquals("b", catalog.getTableForDiscovery(context, "cat", "db", "tbl")
                .getFullSchema().get(0).getName());
        catalog.invalidateCache("db", "tbl");
        assertEquals("c", catalog.getTableForDiscovery(context, "cat", "db", "tbl")
                .getFullSchema().get(0).getName());
    }
    @Test
    void dataCommitKeepsDescriptionButTableInvalidationRemovesIt() {
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        Mockito.when(delegate.getIcebergCatalogType()).thenReturn(IcebergCatalogType.GLUE_CATALOG);
        Table a = table("a");
        Table b = table("b");
        Mockito.when(delegate.getTable(any(), eq("db"), eq("tbl"))).thenReturn(a, b);
        CachingIcebergCatalog catalog = new CachingIcebergCatalog("cat", delegate,
                new IcebergCatalogProperties(Map.of("iceberg.catalog.type", "glue")), executor);
        ConnectContext context = new ConnectContext();
        com.starrocks.catalog.Table first = catalog.getTableForDiscovery(context, "cat", "db", "tbl");
        catalog.invalidateTableCache("db", "tbl");
        assertSame(first, catalog.getTableForDiscovery(context, "cat", "db", "tbl"));
        catalog.invalidateCache("db", "tbl");
        assertEquals("b", catalog.getTableForDiscovery(context, "cat", "db", "tbl").getFullSchema().get(0).getName());
    }

    @Test
    void descriptionRetainsCurrentDefinitionOnly() {
        for (int count : new int[] {20, 40}) {
            java.util.ArrayList<Types.NestedField> fields = new java.util.ArrayList<>();
            for (int i = 1; i <= count; i++) {
                fields.add(Types.NestedField.optional(i, "column_" + i, Types.StringType.get(), "column comment " + i));
            }
            Schema schema = new Schema(fields);
            org.apache.iceberg.TableMetadata metadata = org.apache.iceberg.TableMetadata.newTableMetadata(schema,
                    PartitionSpec.builderFor(schema).bucket("column_1", 16).build(),
                    org.apache.iceberg.SortOrder.builderFor(schema).desc("column_2").build(),
                    "s3://test/db/table", Map.of("format-version", "2", "write.format.default", "parquet"));
            org.apache.iceberg.TableOperations ops = Mockito.mock(org.apache.iceberg.TableOperations.class);
            Mockito.when(ops.current()).thenReturn(metadata);
            Table nativeTable = new org.apache.iceberg.BaseTable(ops, "db.tbl");
            IcebergDiscoveryTable description = IcebergDiscoveryTable.from(nativeTable, "cat", "db", "tbl", "GLUE_CATALOG");
            assertEquals(com.starrocks.sql.analyzer.AstToStringBuilder.getExternalCatalogTableDdlStmt(
                            IcebergApiConverter.toIcebergTable(nativeTable, "cat", "db", "tbl", "GLUE_CATALOG")),
                    com.starrocks.sql.analyzer.AstToStringBuilder.getExternalCatalogTableDdlStmt(description));
            Mockito.verify(ops, Mockito.never()).io();
            long estimate = com.starrocks.memory.estimate.Estimator.estimate(description);
            System.out.println("Discovery description: columns=" + count + ", estimatedBytes=" + estimate
                    + ", estimated5000TablesMiB=" + (estimate * 5000.0 / 1024 / 1024));
        }
    }

    @Test
    void warmRefreshPublishesNewDescriptionWithoutAnotherLoad() {
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        Mockito.when(delegate.getIcebergCatalogType()).thenReturn(IcebergCatalogType.GLUE_CATALOG);
        Table before = table("before");
        Table after = table("after");
        Mockito.when(delegate.getTable(any(), eq("db"), eq("tbl"))).thenReturn(before, after);
        CachingIcebergCatalog catalog = new CachingIcebergCatalog("cat", delegate,
                new IcebergCatalogProperties(Map.of("iceberg.catalog.type", "glue")), executor);
        ConnectContext context = new ConnectContext();
        catalog.getTable(context, "db", "tbl");
        assertEquals("before", catalog.getTableForDiscovery(context, "cat", "db", "tbl")
                .getFullSchema().get(0).getName());
        catalog.refreshTable("db", "tbl", context);
        assertEquals("after", catalog.getTableForDiscovery(context, "cat", "db", "tbl")
                .getFullSchema().get(0).getName());
        Mockito.verify(delegate, Mockito.times(2)).getTable(any(), eq("db"), eq("tbl"));
    }

    @Test
    void retiringInactiveDataCacheDoesNotEvictFrequentlyDiscoveredDescription() {
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        Mockito.when(delegate.getIcebergCatalogType()).thenReturn(IcebergCatalogType.GLUE_CATALOG);
        Table current = table("a");
        Mockito.when(delegate.getTable(any(), eq("db"), eq("tbl"))).thenReturn(current);
        CachingIcebergCatalog catalog = new CachingIcebergCatalog("cat", delegate,
                new IcebergCatalogProperties(Map.of("iceberg.catalog.type", "glue")), executor);
        ConnectContext context = new ConnectContext();
        catalog.getTable(context, "db", "tbl");
        com.starrocks.catalog.Table description = catalog.getTableForDiscovery(context, "cat", "db", "tbl");
        catalog.refreshCatalog();
        Cache<IcebergTableName, Table> tables = Deencapsulation.getField(catalog, "tables");
        assertNull(tables.getIfPresent(new IcebergTableName("db", "tbl")));
        assertSame(description, catalog.getTableForDiscovery(context, "cat", "db", "tbl"));
        Mockito.verify(delegate, Mockito.times(1)).getTable(any(), eq("db"), eq("tbl"));
    }

}
