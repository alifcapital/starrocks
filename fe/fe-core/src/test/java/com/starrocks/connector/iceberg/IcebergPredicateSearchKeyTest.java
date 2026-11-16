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
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IcebergPredicateSearchKeyTest {

    // The partition predicate of a statistics query: ts >= from AND ts < to, with the column ref of the first
    // conjunct nullable or not as given.
    private static ScalarOperator dayRange(boolean firstNullable, int day) {
        ColumnRefOperator first = new ColumnRefOperator(29, DateType.DATETIME, "_cdc_tsms", firstNullable);
        ColumnRefOperator second = new ColumnRefOperator(29, DateType.DATETIME, "_cdc_tsms", false);
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                new BinaryPredicateOperator(BinaryType.GE, first,
                        ConstantOperator.createDatetime(LocalDateTime.of(2026, 10, day, 0, 0))),
                new BinaryPredicateOperator(BinaryType.LT, second,
                        ConstantOperator.createDatetime(LocalDateTime.of(2026, 10, day + 1, 0, 0))));
    }

    private static IcebergPredicateSearchKey key(ScalarOperator predicate, boolean columnStats) {
        return IcebergPredicateSearchKey.of("db", "t", GetRemoteFilesParams.newBuilder()
                .setPredicate(predicate).setTableVersionRange(TvrTableSnapshot.of(7L))
                .setEnableColumnStats(columnStats).build());
    }

    @Test
    void keysIgnoreWhetherColumnRefsAreNullable() {
        IcebergPredicateSearchKey notNull = key(dayRange(false, 21), true);
        IcebergPredicateSearchKey nullable = key(dayRange(true, 21), true);
        assertEquals(notNull, nullable);
        assertEquals(notNull.hashCode(), nullable.hashCode());

        assertNotEquals(notNull, key(dayRange(true, 20), true));
        assertNotEquals(notNull, key(dayRange(true, 21), false));
        assertNotEquals(notNull, key(null, true));
        assertEquals(key(null, true), key(null, true));
    }

    @Test
    void scanTakesTheFilesPlannedForStatisticsDespiteNullability() {
        IcebergCatalog catalog = mock(IcebergCatalog.class);
        Table nativeTable = mock(Table.class);
        IcebergTable table = mock(IcebergTable.class);
        when(table.getCatalogDBName()).thenReturn("db");
        when(table.getCatalogTableName()).thenReturn("t");
        when(table.getNativeTable()).thenReturn(nativeTable);
        IcebergMetadata metadata = new IcebergMetadata("test", null, catalog, null,
                new IcebergCatalogProperties(Map.of("iceberg.catalog.type", "hive")));

        // The statistics of the scan planned the files with the column ref of the first conjunct not nullable.
        List<FileScanTask> planned = new ArrayList<>(List.of(mock(FileScanTask.class), mock(FileScanTask.class)));
        Map<IcebergPredicateSearchKey, List<FileScanTask>> splitTasks =
                Deencapsulation.getField(metadata, "splitTasks");
        IcebergPredicateSearchKey statisticsKey = key(dayRange(false, 21), true);
        splitTasks.put(statisticsKey, planned);
        Set<IcebergPredicateSearchKey> scannedTables = Deencapsulation.getField(metadata, "scannedTables");
        scannedTables.add(statisticsKey);

        // The scan node of the same query holds the predicate with that column ref nullable.
        List<RemoteFileInfo> files = metadata.getRemoteFiles(table, GetRemoteFilesParams.newBuilder()
                .setPredicate(dayRange(true, 21)).setTableVersionRange(TvrTableSnapshot.of(7L)).build());

        assertEquals(2, files.size());
        assertSame(planned.get(0), ((IcebergRemoteFileInfo) files.get(0)).getFileScanTask());
        verify(nativeTable, never()).newScan();
    }
}
