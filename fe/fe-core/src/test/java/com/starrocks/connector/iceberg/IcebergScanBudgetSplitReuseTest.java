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
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IcebergScanBudgetSplitReuseTest {

    private static FileScanTask task(long length) {
        DataFile file = mock(DataFile.class);
        when(file.fileSizeInBytes()).thenReturn(length);
        when(file.recordCount()).thenReturn(10L);
        FileScanTask task = mock(FileScanTask.class);
        when(task.file()).thenReturn(file);
        when(task.length()).thenReturn(length);
        return task;
    }

    @Test
    void budgetedScanTakesItsFilesFromAListThisQueryPlanned() {
        IcebergCatalog catalog = mock(IcebergCatalog.class);
        Table nativeTable = mock(Table.class);
        IcebergTable table = mock(IcebergTable.class);
        when(table.getCatalogDBName()).thenReturn("db");
        when(table.getCatalogTableName()).thenReturn("t");
        when(table.getNativeTable()).thenReturn(nativeTable);
        IcebergMetadata metadata = new IcebergMetadata("test", null, catalog, null,
                new IcebergCatalogProperties(Map.of("iceberg.catalog.type", "hive")));

        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE,
                new ColumnRefOperator(1, IntegerType.INT, "p", true), ConstantOperator.createInt(1));
        GetRemoteFilesParams full = GetRemoteFilesParams.newBuilder()
                .setPredicate(predicate).setTableVersionRange(TvrTableSnapshot.of(7L)).build();
        List<FileScanTask> planned = new ArrayList<>(List.of(task(100), task(100), task(100), task(100)));
        Map<IcebergPredicateSearchKey, List<FileScanTask>> splitTasks =
                Deencapsulation.getField(metadata, "splitTasks");
        splitTasks.put(IcebergPredicateSearchKey.of("db", "t", full), planned);

        GetRemoteFilesParams budgeted = GetRemoteFilesParams.newBuilder()
                .setPredicate(predicate).setTableVersionRange(TvrTableSnapshot.of(7L)).setScanFilesCap(2).build();
        List<RemoteFileInfo> files = metadata.getRemoteFiles(table, budgeted);

        assertEquals(2, files.size());
        assertSame(planned.get(0), ((IcebergRemoteFileInfo) files.get(0)).getFileScanTask());
        assertSame(planned.get(1), ((IcebergRemoteFileInfo) files.get(1)).getFileScanTask());
        // The full list stays as it was for the ordinary scans of the query, and nothing was planned again.
        assertEquals(4, splitTasks.get(IcebergPredicateSearchKey.of("db", "t", full)).size());
        verify(nativeTable, never()).newScan();
    }
}
