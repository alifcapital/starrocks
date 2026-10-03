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

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.catalog.IcebergTable;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// Column filters of a scan are built on first read. Partition pruning reads them, so we expect every
// change of the predicate or the table to drop the built filters, and copies of a scan not to share them.
public class LogicalScanColumnFiltersTest {
    private static ScalarOperator predicate(int value) {
        return new InPredicateOperator(false, new ColumnRefOperator(1, IntegerType.INT, "k", true),
                ConstantOperator.createInt(value), ConstantOperator.createInt(value + 1));
    }

    private static IcebergTable table() {
        IcebergTable table = mock(IcebergTable.class);
        when(table.getPartitionColumns()).thenReturn(List.of());
        return table;
    }

    private static LogicalIcebergScanOperator scan() {
        return new LogicalIcebergScanOperator(table(), Map.of(), Map.of(), -1, predicate(1));
    }

    private static long firstLiteral(LogicalScanOperator scan) {
        return scan.getColumnFilters().get("k").getInPredicateLiterals().get(0).getLongValue();
    }

    @Test
    public void filtersAreBuiltOnReadAndCopiesDoNotShareThem() {
        LogicalIcebergScanOperator scan = scan();
        Assertions.assertNull(scan.columnFilters);
        var original = scan.getColumnFilters();
        Assertions.assertEquals(1, original.get("k").getInPredicateLiterals().get(0).getLongValue());
        LogicalIcebergScanOperator copy = new LogicalIcebergScanOperator.Builder()
                .withOperator(scan).setLimit(10).build();
        Assertions.assertNull(copy.columnFilters);
        var copied = copy.getColumnFilters();
        Assertions.assertNotSame(original.get("k"), copied.get("k"));
        copied.get("k").getInPredicateLiterals().clear();
        Assertions.assertEquals(2, original.get("k").getInPredicateLiterals().size());
    }

    @Test
    public void predicateAndTableChangesDropBuiltFilters() {
        LogicalIcebergScanOperator scan = scan();
        scan.getColumnFilters();
        scan.setPredicate(predicate(8));
        Assertions.assertNull(scan.columnFilters);
        Assertions.assertEquals(8, firstLiteral(scan));
        scan.setTable(table());
        Assertions.assertNull(scan.columnFilters);
        Assertions.assertEquals(8, firstLiteral(scan));
        scan.setPredicate(null);
        Assertions.assertTrue(scan.getColumnFilters().isEmpty());
    }

    @Test
    public void builderPredicateChangeAndExplicitRebuildAreVisible() {
        LogicalIcebergScanOperator scan = scan();
        scan.getColumnFilters();
        LogicalIcebergScanOperator copy = new LogicalIcebergScanOperator.Builder()
                .withOperator(scan).setPredicate(predicate(20)).build();
        Assertions.assertEquals(20, firstLiteral(copy));
        copy.getPredicate().setChild(1, ConstantOperator.createInt(30));
        copy.buildColumnFilters(copy.getPredicate());
        Assertions.assertEquals(30, firstLiteral(copy));
        Assertions.assertEquals(1, firstLiteral(scan));
    }
}
