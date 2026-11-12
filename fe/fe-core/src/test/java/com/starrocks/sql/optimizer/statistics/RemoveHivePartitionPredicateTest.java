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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

// A Hive or Hudi connector counts the rows of the selected partitions, so the partition conjuncts are dropped from the
// selectivity of the scan only when its statistics come from the connector.
public class RemoveHivePartitionPredicateTest {
    private static final Statistics TABLE_METADATA =
            Statistics.builder().setStatsSource(Statistics.StatsSource.TABLE_METADATA).build();
    private static final Statistics ANALYZE =
            Statistics.builder().setStatsSource(Statistics.StatsSource.ANALYZE).build();
    private static final Statistics NONE = Statistics.builder().build();

    private final StatisticsCalculator calculator = new StatisticsCalculator();
    private final ColumnRefOperator id = new ColumnRefOperator(1, IntegerType.INT, "id", true);
    private final ColumnRefOperator dt = new ColumnRefOperator(2, StringType.STRING, "dt", true);

    private <T extends Table> T table(Class<T> type) {
        T table = Mockito.mock(type);
        // a mutable list, as a Hive table hands it out, so a caller growing it would show up
        Mockito.when(table.getPartitionColumnNames()).thenReturn(new ArrayList<>(List.of("dt")));
        return table;
    }

    private void checkFollowsScanStatsSource(Table table, Function<ScalarOperator, LogicalScanOperator> scan) {
        ScalarOperator dtEq = new BinaryPredicateOperator(BinaryType.EQ, dt, ConstantOperator.createVarchar("2026-11-01"));
        ScalarOperator idEq = new BinaryPredicateOperator(BinaryType.EQ, id, ConstantOperator.createInt(1));
        ScalarOperator predicate = Utils.compoundAnd(dtEq, idEq);

        Assertions.assertEquals(idEq, calculator.removePartitionPredicate(predicate, scan.apply(predicate),
                TABLE_METADATA));
        Assertions.assertEquals(ConstantOperator.TRUE, calculator.removePartitionPredicate(dtEq, scan.apply(dtEq),
                TABLE_METADATA));
        // ANALYZE statistics, also the ones restricted to the selected partitions, keep the predicate, and so do
        // default statistics, which are not counted from the selected partitions
        for (Statistics statistics : new Statistics[] {ANALYZE, NONE, null}) {
            Assertions.assertEquals(predicate, calculator.removePartitionPredicate(predicate, scan.apply(predicate),
                    statistics));
        }
        Assertions.assertEquals(List.of("dt"), table.getPartitionColumnNames());
    }

    private Map<ColumnRefOperator, Column> refToColumn() {
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        refToColumn.put(id, new Column("id", IntegerType.INT, true));
        refToColumn.put(dt, new Column("dt", StringType.STRING, true));
        return refToColumn;
    }

    private Map<Column, ColumnRefOperator> columnToRef(Map<ColumnRefOperator, Column> refToColumn) {
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        refToColumn.forEach((ref, column) -> columnToRef.put(column, ref));
        return columnToRef;
    }

    @Test
    public void testHive() {
        HiveTable table = table(HiveTable.class);
        checkFollowsScanStatsSource(table, predicate -> {
            Map<ColumnRefOperator, Column> refToColumn = refToColumn();
            return new LogicalHiveScanOperator(table, refToColumn, columnToRef(refToColumn), Operator.DEFAULT_LIMIT,
                    predicate);
        });
    }

    @Test
    public void testHudi() {
        HudiTable table = table(HudiTable.class);
        checkFollowsScanStatsSource(table, predicate -> {
            Map<ColumnRefOperator, Column> refToColumn = refToColumn();
            return new LogicalHudiScanOperator(table, refToColumn, columnToRef(refToColumn), Operator.DEFAULT_LIMIT,
                    predicate);
        });
    }
}
