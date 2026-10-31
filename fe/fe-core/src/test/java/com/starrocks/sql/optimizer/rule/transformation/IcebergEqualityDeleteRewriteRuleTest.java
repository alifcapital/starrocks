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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.iceberg.IcebergDeleteSchema;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergMORParams.EqDeleteScope;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.catalog.IcebergTable.DATA_SEQUENCE_NUMBER;
import static com.starrocks.catalog.IcebergTable.FILE_PATH;
import static com.starrocks.catalog.IcebergTable.PARTITION_ID;

public class IcebergEqualityDeleteRewriteRuleTest {

    // The LEFT ANTI JOIN that applies equality deletes must compare identity columns with null-safe
    // equals (EQ_FOR_NULL / <=>), not plain EQ: per the Iceberg spec a NULL value in a delete column
    // matches a NULL row value, and plain EQ would evaluate NULL = NULL -> UNKNOWN and silently keep
    // the row alive. The $data_sequence_number bound must stay a strict LT.
    @Test
    public void testBuildOnPredicateUsesNullSafeEqualsForIdentityColumns() {
        ColumnRefOperator leftId = new ColumnRefOperator(1, IntegerType.INT, "id", true);
        ColumnRefOperator leftSeq = new ColumnRefOperator(2, IntegerType.BIGINT, DATA_SEQUENCE_NUMBER, true);
        ColumnRefOperator rightId = new ColumnRefOperator(3, IntegerType.INT, "id", true);
        ColumnRefOperator rightSeq = new ColumnRefOperator(4, IntegerType.BIGINT, DATA_SEQUENCE_NUMBER, true);

        Map<String, ColumnRefOperator> leftCols = new HashMap<>();
        leftCols.put("id", leftId);
        leftCols.put(DATA_SEQUENCE_NUMBER, leftSeq);

        ScalarOperator onPredicate = IcebergEqualityDeleteScanBuilder.buildOnPredicate(
                leftCols, List.of(rightId, rightSeq));

        Map<String, BinaryType> byColumn = new HashMap<>();
        collectBinaryTypes(onPredicate, byColumn);

        Assertions.assertEquals(BinaryType.EQ_FOR_NULL, byColumn.get("id"),
                "identity column must use null-safe equals so NULL-key rows are deleted");
        Assertions.assertEquals(BinaryType.LT, byColumn.get(DATA_SEQUENCE_NUMBER),
                "data sequence number bound must remain a strict less-than");
    }

    @Test
    public void testPredicateOwnsMutableArgumentsWithoutChangingScanReferences() {
        ColumnRefOperator left = new ColumnRefOperator(1, IntegerType.INT, "id", true);
        ColumnRefOperator right = new ColumnRefOperator(2, IntegerType.INT, "id", true);
        BinaryPredicateOperator predicate = (BinaryPredicateOperator)
                IcebergEqualityDeleteScanBuilder.buildOnPredicate(Map.of("id", left), List.of(right));
        Assertions.assertEquals(left, predicate.getChild(0));
        Assertions.assertEquals(right, predicate.getChild(1));
        Assertions.assertNotSame(left, predicate.getChild(0));
        Assertions.assertNotSame(right, predicate.getChild(1));
        ((ColumnRefOperator) predicate.getChild(0)).setNullable(false);
        ((ColumnRefOperator) predicate.getChild(1)).setNullable(false);
        Assertions.assertTrue(left.isNullable());
        Assertions.assertTrue(right.isNullable());
        ColumnRefOperator replacement = new ColumnRefOperator(3, IntegerType.INT, "other", true);
        predicate.setChild(0, replacement);
        Assertions.assertSame(replacement, predicate.getChild(0));
        Assertions.assertNull(IcebergEqualityDeleteScanBuilder.buildOnPredicate(Map.of(), List.of()));
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> IcebergEqualityDeleteScanBuilder.buildOnPredicate(Map.of(), List.of(right)));
    }

    // Time travel to a snapshot from before the equality key was renamed: the tables the rewrite derives must keep
    // the snapshot read view, and the key must resolve by field id under that schema, so both the data-file scan
    // and the delete-file scan read the key with the snapshot's name.
    @Test
    public void testTimeTravelBeforeEqualityKeyRename() {
        Schema snapshotSchema = new Schema(0,
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "p", Types.StringType.get()));
        Schema currentSchema = new Schema(1,
                Types.NestedField.optional(1, "id_renamed", Types.IntegerType.get()),
                Types.NestedField.optional(2, "p", Types.StringType.get()));
        PartitionSpec unpartitioned = PartitionSpec.unpartitioned();
        Table nativeTable = Mockito.mock(Table.class);
        Mockito.when(nativeTable.schema()).thenReturn(currentSchema);
        Mockito.when(nativeTable.spec()).thenReturn(unpartitioned);
        Mockito.when(nativeTable.specs()).thenReturn(Map.of(0, unpartitioned));
        Mockito.when(nativeTable.properties()).thenReturn(Map.of());

        IcebergTable current = new IcebergTable(1, "tb", "catalog", "resource", "db", "tb", "",
                IcebergApiConverter.toFullSchemas(currentSchema, nativeTable), nativeTable, Map.of());
        IcebergTable snapshotTable = current.withReadMetadata(snapshotSchema, Map.of(0, unpartitioned));

        ColumnRefFactory factory = new ColumnRefFactory();
        Map<ColumnRefOperator, Column> colRefToColumn = new LinkedHashMap<>();
        Map<Column, ColumnRefOperator> columnToColRef = new HashMap<>();
        for (Column column : snapshotTable.getFullSchema()) {
            ColumnRefOperator ref = factory.create(column.getName(), column.getType(), true);
            colRefToColumn.put(ref, column);
            columnToColRef.put(column, ref);
        }
        LogicalIcebergScanOperator scan = new LogicalIcebergScanOperator(snapshotTable, colRefToColumn,
                columnToColRef, -1, null, TvrTableSnapshot.of(Optional.of(1L)));
        scan.setDeleteSchemas(Set.of(IcebergDeleteSchema.of(List.of(1), 0)));

        List<OptExpression> result = new IcebergEqualityDeleteRewriteRule().transform(
                OptExpression.create(scan), OptimizerFactory.mockContext(factory));

        // union all <- project <- anti join (data files with deletes, equality delete files)
        OptExpression antiJoin = result.get(0).inputAt(1).inputAt(0);
        LogicalIcebergScanOperator withDeleteScan = antiJoin.inputAt(0).getOp().cast();
        LogicalIcebergEqualityDeleteScanOperator eqDeleteScan = antiJoin.inputAt(1).getOp().cast();
        Assertions.assertSame(snapshotSchema, ((IcebergTable) withDeleteScan.getTable()).getReadSchema());
        Assertions.assertSame(snapshotSchema, ((IcebergTable) eqDeleteScan.getTable()).getReadSchema());
        Assertions.assertTrue(withDeleteScan.getColumnNameToColRefMap().containsKey("id"));
        Assertions.assertTrue(eqDeleteScan.getOutputColumns().stream().anyMatch(ref -> ref.getName().equals("id")));
    }

    private static final Schema PRUNING_SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "p", Types.IntegerType.get()),
            Types.NestedField.optional(3, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "s", Types.StringType.get()),
            Types.NestedField.optional(5, PARTITION_ID, Types.IntegerType.get()));

    private static final Map<String, Column> PRUNING_SCAN_COLUMNS = Map.of(
            "id", new Column("id", IntegerType.INT),
            "p", new Column("p", IntegerType.INT),
            "ts", new Column("ts", DateType.DATETIME),
            "s", new Column("s", StringType.STRING),
            PARTITION_ID, new Column(PARTITION_ID, IntegerType.INT));

    private static Set<String> pruningColumns(Set<IcebergDeleteSchema> deleteSchemas, Map<String, Column> scanColumns,
                                              EqDeleteScope scope, PartitionSpec... specs) {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(PRUNING_SCHEMA);
        Map<Integer, PartitionSpec> bySpecId = new HashMap<>();
        for (PartitionSpec spec : specs) {
            bySpecId.put(spec.specId(), spec);
        }
        Mockito.when(table.specs()).thenReturn(bySpecId);
        return IcebergEqualityDeleteScanBuilder.runtimeFilterPruningColumns(table, PRUNING_SCHEMA, deleteSchemas,
                IcebergMORParams.ofEqDelete(List.of(1), scope), scanColumns);
    }

    private static Set<String> pruningColumns(Set<IcebergDeleteSchema> deleteSchemas, PartitionSpec... specs) {
        return pruningColumns(deleteSchemas, PRUNING_SCAN_COLUMNS, EqDeleteScope.PARTITIONED, specs);
    }

    // A partitioned leg keeps its $partition_id match, so any identity field of a scanned column that every spec of
    // its delete files has may let a runtime filter skip the delete files of the partitions it rejects.
    @Test
    public void testRuntimeFilterPruningColumns() {
        List<Integer> byId = List.of(1);
        PartitionSpec byP = PartitionSpec.builderFor(PRUNING_SCHEMA).withSpecId(0).identity("p").build();
        Assertions.assertEquals(Set.of("p"), pruningColumns(Set.of(IcebergDeleteSchema.of(byId, 0)), byP));
        Assertions.assertEquals(Set.of("p", "s"), pruningColumns(Set.of(IcebergDeleteSchema.of(byId, 0)),
                PartitionSpec.builderFor(PRUNING_SCHEMA).withSpecId(0).identity("p").identity("s").build()));
        // a non-identity field next to an identity one leaves the identity one
        Assertions.assertEquals(Set.of("p"), pruningColumns(Set.of(IcebergDeleteSchema.of(byId, 0)),
                PartitionSpec.builderFor(PRUNING_SCHEMA).withSpecId(0).identity("p").bucket("id", 4).build()));
        // deletes of another key do not count
        Assertions.assertEquals(Set.of("p"), pruningColumns(
                Set.of(IcebergDeleteSchema.of(byId, 0), IcebergDeleteSchema.of(List.of(2), 7)), byP));

        // several specs: only a column that is an identity field in every spec of the leg's delete files
        PartitionSpec unpartitioned = PartitionSpec.builderFor(PRUNING_SCHEMA).withSpecId(0).build();
        PartitionSpec byPAndBucket = PartitionSpec.builderFor(PRUNING_SCHEMA).withSpecId(1).identity("p")
                .bucket("id", 4).build();
        PartitionSpec byBucket = PartitionSpec.builderFor(PRUNING_SCHEMA).withSpecId(2).bucket("id", 4).build();
        Assertions.assertEquals(Set.of("p"), pruningColumns(
                Set.of(IcebergDeleteSchema.of(byId, 0), IcebergDeleteSchema.of(byId, 1)), byP, byPAndBucket));
        Assertions.assertEquals(Set.of(), pruningColumns(
                Set.of(IcebergDeleteSchema.of(byId, 1), IcebergDeleteSchema.of(byId, 2)), byPAndBucket, byBucket));
        // a global delete belongs to the global leg; the partitioned leg looks at partitioned specs only
        Assertions.assertEquals(Set.of("p"), pruningColumns(
                Set.of(IcebergDeleteSchema.of(byId, 0), IcebergDeleteSchema.of(byId, 1)), unpartitioned, byPAndBucket));
        Assertions.assertEquals(Set.of(), pruningColumns(Set.of(IcebergDeleteSchema.of(byId, 0)),
                PRUNING_SCAN_COLUMNS, EqDeleteScope.GLOBAL, unpartitioned));

        // a field not named like its column, a column the query does not read, a timestamp, which depends on the
        // session time zone, and a real column named like an extended column
        Assertions.assertEquals(Set.of(), pruningColumns(Set.of(IcebergDeleteSchema.of(byId, 0)),
                PartitionSpec.builderFor(PRUNING_SCHEMA).withSpecId(0).identity("p", "p_part").build()));
        Assertions.assertEquals(Set.of(), pruningColumns(Set.of(IcebergDeleteSchema.of(byId, 0)),
                Map.of("id", new Column("id", IntegerType.INT)), EqDeleteScope.PARTITIONED, byP));
        Assertions.assertEquals(Set.of(), pruningColumns(Set.of(IcebergDeleteSchema.of(byId, 0)),
                PartitionSpec.builderFor(PRUNING_SCHEMA).withSpecId(0).identity("ts").build()));
        Assertions.assertEquals(Set.of(), pruningColumns(Set.of(IcebergDeleteSchema.of(byId, 0)),
                PartitionSpec.builderFor(PRUNING_SCHEMA).withSpecId(0).identity(PARTITION_ID).build()));
    }

    private static OptExpression rewritePartitionedById(boolean withRowLocator) {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "p", Types.StringType.get()));
        PartitionSpec byP = PartitionSpec.builderFor(schema).withSpecId(0).identity("p").build();
        Table nativeTable = Mockito.mock(Table.class);
        Mockito.when(nativeTable.schema()).thenReturn(schema);
        Mockito.when(nativeTable.spec()).thenReturn(byP);
        Mockito.when(nativeTable.specs()).thenReturn(Map.of(0, byP));
        Mockito.when(nativeTable.properties()).thenReturn(Map.of());
        IcebergTable table = new IcebergTable(1, "tb", "catalog", "resource", "db", "tb", "",
                IcebergApiConverter.toFullSchemas(schema, nativeTable), nativeTable, Map.of());

        ColumnRefFactory factory = new ColumnRefFactory();
        Map<ColumnRefOperator, Column> colRefToColumn = new LinkedHashMap<>();
        Map<Column, ColumnRefOperator> columnToColRef = new HashMap<>();
        List<Column> columns = new ArrayList<>(table.getFullSchema());
        if (withRowLocator) {
            columns.add(new Column(FILE_PATH, StringType.STRING, true));
        }
        for (Column column : columns) {
            ColumnRefOperator ref = factory.create(column.getName(), column.getType(), true);
            colRefToColumn.put(ref, column);
            columnToColRef.put(column, ref);
        }
        LogicalIcebergScanOperator scan = new LogicalIcebergScanOperator(table, colRefToColumn, columnToColRef, -1,
                null, TvrTableSnapshot.of(Optional.of(1L)));
        scan.setDeleteSchemas(Set.of(IcebergDeleteSchema.of(List.of(1), 0)));
        return new IcebergEqualityDeleteRewriteRule().transform(OptExpression.create(scan),
                OptimizerFactory.mockContext(factory)).get(0);
    }

    // The delete scan of the partitioned leg takes p from the partition value and the anti join matches it with
    // `<=>`, so a runtime filter on p may reach the delete scan; a DML scan, which reads row locators, gets none.
    @Test
    public void testPartitionedLegMatchesPruningColumn() {
        OptExpression antiJoin = rewritePartitionedById(false).inputAt(1).inputAt(0);
        LogicalIcebergScanOperator withDeleteScan = antiJoin.inputAt(0).getOp().cast();
        LogicalIcebergEqualityDeleteScanOperator eqDeleteScan = antiJoin.inputAt(1).getOp().cast();
        Assertions.assertEquals(Set.of("p"), eqDeleteScan.getRuntimeFilterPruningColumns());
        Assertions.assertTrue(eqDeleteScan.getOutputColumns().stream().anyMatch(ref -> ref.getName().equals("p")));
        Assertions.assertTrue(withDeleteScan.getColumnNameToColRefMap().containsKey("p"));
        Map<String, BinaryType> byColumn = new HashMap<>();
        collectBinaryTypes(((LogicalJoinOperator) antiJoin.getOp()).getOnPredicate(), byColumn);
        Assertions.assertEquals(BinaryType.EQ_FOR_NULL, byColumn.get("p"));
        Assertions.assertEquals(BinaryType.EQ_FOR_NULL, byColumn.get(PARTITION_ID));

        LogicalIcebergEqualityDeleteScanOperator dmlDeleteScan =
                rewritePartitionedById(true).inputAt(1).inputAt(0).inputAt(1).getOp().cast();
        Assertions.assertEquals(Set.of(), dmlDeleteScan.getRuntimeFilterPruningColumns());
        Assertions.assertTrue(dmlDeleteScan.getOutputColumns().stream().noneMatch(ref -> ref.getName().equals("p")));
    }

    // Walk the AND tree and index each leaf comparison by the name of its right-hand (delete-table) column.
    private void collectBinaryTypes(ScalarOperator op, Map<String, BinaryType> out) {
        if (op instanceof BinaryPredicateOperator binary) {
            ColumnRefOperator rightCol = (ColumnRefOperator) binary.getChild(1);
            out.put(rightCol.getName(), binary.getBinaryType());
            return;
        }
        for (ScalarOperator child : op.getChildren()) {
            collectBinaryTypes(child, out);
        }
    }
}
