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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IcebergStatisticsPartitionPruningTest {
    private final IcebergCatalog catalog = mock(IcebergCatalog.class);
    private final Table nativeTable = mock(Table.class);
    private final IcebergTable table = mock(IcebergTable.class);
    private final IcebergMetadata metadata = new IcebergMetadata("test", null, catalog, null,
            new IcebergCatalogProperties(Map.of("iceberg.catalog.type", "hive")));

    private void setup(PartitionSpec spec) {
        when(table.getCatalogDBName()).thenReturn("db");
        when(table.getCatalogTableName()).thenReturn("t");
        when(table.getNativeTable()).thenReturn(nativeTable);
        when(table.getReadSchema()).thenReturn(spec.schema());
        when(nativeTable.spec()).thenReturn(spec);
        when(nativeTable.schema()).thenReturn(spec.schema());
        when(nativeTable.specs()).thenReturn(Map.of(spec.specId(), spec));
    }

    private Partition partition(PartitionSpec spec, Object value) {
        IcebergPartitionData data = new IcebergPartitionData(1);
        data.set(0, value);
        Partition result = new Partition(1L, spec.specId());
        result.setValues(spec, spec.partitionType(), data);
        // Reader-owned buffers must not remain reachable through the cache.
        data.set(0, null);
        return result;
    }

    private List<String> select(ScalarOperator predicate, long limit, long snapshot) {
        List<String> names = metadata.getScannedPartitionNames(table, predicate, limit, TvrTableSnapshot.of(snapshot));
        Map<?, ?> splits = Deencapsulation.getField(metadata, "splitTasks");
        assertTrue(splits.isEmpty(), "statistics must leave file planning lazy");
        verify(nativeTable, never()).newScan();
        verify(nativeTable, never()).io();
        return names;
    }

    @Test
    void identityNullEmptyAndCacheMissAreDifferentDomains() {
        Schema schema = new Schema(Types.NestedField.optional(1, "p", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        setup(spec);
        when(catalog.getCachedPartitions(table, 7L)).thenReturn(Map.of(
                "p=1", partition(spec, 1), "p=2", partition(spec, 2), "p=null", partition(spec, null)));
        ColumnRefOperator ref = new ColumnRefOperator(1, IntegerType.INT, "p", true);
        assertEquals(List.of("p=2"), select(new BinaryPredicateOperator(BinaryType.EQ, ref,
                ConstantOperator.createInt(2)), -1, 7));
        assertEquals(List.of("p=null"), select(new IsNullPredicateOperator(ref), -1, 7));
        assertEquals(List.of(), select(new BinaryPredicateOperator(BinaryType.EQ, ref,
                ConstantOperator.createInt(3)), -1, 7));
        when(catalog.getCachedPartitions(table, 8L)).thenReturn(null);
        assertNull(select(ConstantOperator.TRUE, -1, 8));
        when(catalog.getCachedPartitions(table, 8L)).thenReturn(Map.of());
        assertEquals(List.of(), select(ConstantOperator.TRUE, -1, 8));
        assertNull(metadata.getScannedPartitionNames(table, ConstantOperator.TRUE, -1, TvrTableDelta.of(7, 8)));
    }

    @Test
    void dayTransformProjectsSourcePredicateAndLimitDoesNotTruncatePartitions() {
        Schema schema = new Schema(Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("ts").build();
        setup(spec);
        int sep1 = (int) LocalDate.of(2026, 9, 1).toEpochDay();
        when(catalog.getCachedPartitions(table, 7L)).thenReturn(Map.of(
                "ts_day=2026-08-31", partition(spec, sep1 - 1),
                "ts_day=2026-09-01", partition(spec, sep1),
                "ts_day=2026-09-02", partition(spec, sep1 + 1)));
        ColumnRefOperator ts = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
        ScalarOperator filter = new BinaryPredicateOperator(BinaryType.GE, ts,
                ConstantOperator.createDatetime(LocalDateTime.of(2026, 9, 1, 12, 0)));
        assertEquals(List.of("ts_day=2026-09-01", "ts_day=2026-09-02"),
                select(filter, -1, 7).stream().sorted().toList());
        assertEquals(select(filter, -1, 7), select(filter, 1, 7));
    }

    @Test
    void evolvedSpecsUseTheirOwnTypedDomainAndIncompleteEntriesFallBack() {
        Schema schema = new Schema(Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()));
        PartitionSpec day = PartitionSpec.builderFor(schema).withSpecId(1).day("ts").build();
        PartitionSpec month = PartitionSpec.builderFor(schema).withSpecId(2).month("ts").build();
        setup(month);
        when(nativeTable.specs()).thenReturn(Map.of(1, day, 2, month));
        Map<String, Partition> directory = new HashMap<>();
        directory.put("ts_day=2026-08-31", partition(day, (int) LocalDate.of(2026, 8, 31).toEpochDay()));
        directory.put("ts_month=2026-09", partition(month, (2026 - 1970) * 12 + 8));
        when(catalog.getCachedPartitions(table, 7L)).thenReturn(directory);
        ColumnRefOperator ts = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
        ScalarOperator filter = new BinaryPredicateOperator(BinaryType.GE, ts,
                ConstantOperator.createDatetime(LocalDateTime.of(2026, 9, 1, 0, 0)));
        assertEquals(List.of("ts_month=2026-09"), select(filter, -1, 7));
        directory.get("ts_month=2026-09").clearValues();
        assertNull(select(filter, -1, 7), "ambiguous names/legacy entries cannot prove an empty domain");
    }
}
