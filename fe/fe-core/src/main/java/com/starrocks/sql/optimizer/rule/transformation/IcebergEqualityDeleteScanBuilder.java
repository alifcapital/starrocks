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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergDeleteSchema;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergMORParams.EqDeleteScope;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarbinaryType;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.IcebergTable.DATA_SEQUENCE_NUMBER;
import static com.starrocks.catalog.IcebergTable.EQUALITY_DELETE_TABLE_COMMENT;
import static com.starrocks.catalog.IcebergTable.PARTITION_ID;
import static com.starrocks.catalog.IcebergTable.SPEC_ID;

// Shared by the read path (IcebergEqualityDeleteRewriteRule, anti-join) and the equality-delete ->
// position-delete conversion procedure (semi-join). Centralized so the null-safe match predicate and
// the GLOBAL/PARTITIONED scope split stay identical on both, and conversion can't drift from how reads
// decide which rows an equality delete removes.
public class IcebergEqualityDeleteScanBuilder {
    private static final Set<String> EXTENDED_COLUMN_NAMES = Set.of(DATA_SEQUENCE_NUMBER, SPEC_ID, PARTITION_ID);

    private IcebergEqualityDeleteScanBuilder() {
    }

    // One leg per distinct (equalityIds, scope). Scope mirrors Iceberg's DeleteFileIndex routing: a
    // delete file under an unpartitioned spec is GLOBAL (applies to every data file), otherwise
    // PARTITIONED (applies only within its own (specId, partition), matched via the $partition_id key).
    // distinct() collapses partitioned specs sharing an equality-id set into one PARTITIONED leg, which
    // is how partition evolution is handled.
    public static List<IcebergMORParams> splitIntoLegs(Set<IcebergDeleteSchema> deleteSchemas,
                                                       Map<Integer, PartitionSpec> specs) {
        return deleteSchemas.stream()
                .map(schema -> IcebergMORParams.ofEqDelete(schema.equalityIds(),
                        specs.get(schema.specId()).isUnpartitioned() ? EqDeleteScope.GLOBAL : EqDeleteScope.PARTITIONED))
                .distinct()
                .collect(Collectors.toList());
    }

    public static boolean hasPartitionedLeg(List<IcebergMORParams> legs) {
        return legs.stream().anyMatch(leg -> leg.getEqDeleteScope() == EqDeleteScope.PARTITIONED);
    }

    public static ScalarOperator buildOnPredicate(Map<String, ColumnRefOperator> leftCols,
                                                  List<ColumnRefOperator> rightCols) {
        List<ScalarOperator> onPredicates = new ArrayList<>(rightCols.size());
        for (ColumnRefOperator rightColRef : rightCols) {
            String icebergIdentifierColumnName = rightColRef.getName();
            ColumnRefOperator leftColRef = leftCols.get(icebergIdentifierColumnName);
            if (leftColRef == null) {
                throw new StarRocksConnectorException("can not find iceberg identifier column {}",
                        rightColRef.getName());
            }
            // Use EQ_FOR_NULL (null-safe equals) for identity columns to match Iceberg spec:
            // "A null value in a delete column matches a row if the row's value is null"
            BinaryType binaryType = icebergIdentifierColumnName.equals(DATA_SEQUENCE_NUMBER)
                    ? BinaryType.LT
                    : BinaryType.EQ_FOR_NULL;
            // Keep argument ownership independent from the scan refs without cloning a temporary predicate.
            BinaryPredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(binaryType,
                    leftColRef.clone(), rightColRef.clone());
            onPredicates.add(binaryPredicateOperator);
        }

        return Utils.createCompound(CompoundPredicateOperator.CompoundType.AND, onPredicates);
    }

    public static LogicalIcebergEqualityDeleteScanOperator buildEqualityDeleteScanOperator(
            IcebergMORParams leg,
            IcebergTable icebergTable,
            ColumnRefFactory columnRefFactory,
            ScalarOperator originPredicate,
            IcebergTableMORParams tableFullMorParams,
            TvrVersionRange tvrVersionRange,
            Set<String> runtimeFilterPruningColumns) {
        List<Integer> equalityIds = leg.getEqualityIds();
        boolean partitionLeg = leg.getEqDeleteScope() == EqDeleteScope.PARTITIONED;
        String equalityDeleteTableName = buildEqualityDeleteTableName(icebergTable, equalityIds)
                + "_" + leg.getEqDeleteScope();
        List<String> columnNames = equalityIds.stream()
                .map(id -> findEqualityColumnName(icebergTable, id))
                .toList();
        List<Column> deleteColumns = columnNames.stream().map(icebergTable::getColumn).collect(Collectors.toList());
        // A pruning column outside the key gets a delete-side slot that holds the identity partition value of each
        // delete file, and buildOnPredicate matches it with `<=>` like a key column.
        runtimeFilterPruningColumns.stream()
                .filter(name -> !columnNames.contains(name))
                .sorted()
                .forEach(name -> deleteColumns.add(icebergTable.getColumn(name)));
        IcebergTable equalityDeleteTable = new IcebergTable(
                ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asLong(), equalityDeleteTableName,
                icebergTable.getCatalogName(), icebergTable.getResourceName(), icebergTable.getCatalogDBName(),
                icebergTable.getCatalogTableName(), EQUALITY_DELETE_TABLE_COMMENT, deleteColumns,
                icebergTable.getNativeTable(), ImmutableMap.of());
        equalityDeleteTable.inheritReadView(icebergTable);

        ImmutableMap.Builder<ColumnRefOperator, Column> colRefToColumn = ImmutableMap.builder();
        ImmutableMap.Builder<Column, ColumnRefOperator> columnToColRef = ImmutableMap.builder();
        for (Column column : deleteColumns) {
            ColumnRefOperator columnRef = columnRefFactory.create(column.getName(), column.getType(), true);
            colRefToColumn.put(columnRef, column);
            columnToColRef.put(column, columnRef);
        }

        // The partitioned leg carries $partition_id and joins on it; the global leg does not, so it
        // applies to every data file (Iceberg global delete semantics).
        fillExtendedColumns(columnRefFactory, colRefToColumn, columnToColRef, partitionLeg, icebergTable);
        LogicalIcebergEqualityDeleteScanOperator eqScanOp = new LogicalIcebergEqualityDeleteScanOperator(
                equalityDeleteTable, colRefToColumn.build(), columnToColRef.build(), -1, null, tvrVersionRange);
        eqScanOp.setOriginPredicate(originPredicate);
        eqScanOp.setTableFullMORParams(tableFullMorParams);
        eqScanOp.setMORParams(leg);
        eqScanOp.setRuntimeFilterPruningColumns(runtimeFilterPruningColumns);
        return eqScanOp;
    }

    // The identity partition columns that let a runtime filter on the data column skip the delete files of the
    // partitions it rejects, for a partitioned leg. A partitioned delete applies only to data files of its own spec
    // and partition, and the leg matches that with $partition_id. If a column is an identity field in every spec of
    // the leg's delete files, `data.c <=> delete.c` holds for every pair that $partition_id matches, so adding it
    // keeps the result and lets a filter that rejects c = v drop the deletes of partition v: the join that built
    // the filter drops the data rows with c = v anyway (JoinNode pushes only such filters into a delete scan). The
    // delete scan takes c from the partition value of the delete file, so the field must carry the column's name,
    // and the type must read back exactly: a timestamp partition value depends on the session time zone.
    public static Set<String> runtimeFilterPruningColumns(Table table, Schema readSchema,
                                                          Set<IcebergDeleteSchema> deleteSchemas,
                                                          IcebergMORParams leg, Map<String, Column> scanColumns) {
        if (leg.getEqDeleteScope() != EqDeleteScope.PARTITIONED) {
            return Set.of();
        }
        List<PartitionSpec> legSpecs = deleteSchemas.stream()
                .filter(deleteSchema -> deleteSchema.equalityIds().equals(leg.getEqualityIds()))
                .map(deleteSchema -> table.specs().get(deleteSchema.specId()))
                .filter(spec -> spec != null && spec.isPartitioned())
                .distinct()
                .toList();
        Set<String> columns = null;
        for (PartitionSpec spec : legSpecs) {
            Set<String> specColumns = new HashSet<>();
            for (PartitionField field : spec.fields()) {
                String name = readSchema.findColumnName(field.sourceId());
                if (field.transform().isIdentity() && name != null && field.name().equals(name) &&
                        EXTENDED_COLUMN_NAMES.stream().noneMatch(name::equalsIgnoreCase) &&
                        isRuntimeFilterPruningType(scanColumns.get(name))) {
                    specColumns.add(name);
                }
            }
            if (columns == null) {
                columns = specColumns;
            } else {
                columns.retainAll(specColumns);
            }
        }
        return columns == null ? Set.of() : Set.copyOf(columns);
    }

    private static boolean isRuntimeFilterPruningType(Column column) {
        return column != null && (column.getType().isIntegerType() || column.getType().isDate() ||
                column.getType().isStringType());
    }

    public static void fillExtendedColumns(ColumnRefFactory columnRefFactory,
                                           ImmutableMap.Builder<ColumnRefOperator, Column> newColRefToColumnMetaMapBuilder,
                                           ImmutableMap.Builder<Column, ColumnRefOperator> newColumnMetaToColRefMapBuilder,
                                           boolean addPartitionId,
                                           IcebergTable icebergTable) {
        Column column = new Column(DATA_SEQUENCE_NUMBER, IntegerType.BIGINT, true);
        ColumnRefOperator columnRef = buildColumnRef(column, columnRefFactory, icebergTable);
        newColRefToColumnMetaMapBuilder.put(columnRef, column);
        newColumnMetaToColRefMapBuilder.put(column, columnRef);

        // $partition_id scopes a partitioned equality delete to its own (specId, partition). It replaces
        // the old $spec_id-only handling, which distinguished spec identity but not partition value.
        if (addPartitionId) {
            Column partitionIdColumn = new Column(PARTITION_ID, VarbinaryType.VARBINARY, true);
            ColumnRefOperator partitionIdColumnRef = buildColumnRef(partitionIdColumn, columnRefFactory, icebergTable);
            newColRefToColumnMetaMapBuilder.put(partitionIdColumnRef, partitionIdColumn);
            newColumnMetaToColRefMapBuilder.put(partitionIdColumn, partitionIdColumnRef);
        }
    }

    public static ColumnRefOperator buildColumnRef(Column originalCol, ColumnRefFactory factory, IcebergTable table) {
        int relationId = factory.getNextRelationId();
        ColumnRefOperator newColumnRef = factory.create(originalCol.getName(), originalCol.getType(), true);
        factory.updateColumnToRelationIds(newColumnRef.getId(), relationId);
        factory.updateColumnRefToColumns(newColumnRef, originalCol, table);
        return newColumnRef;
    }

    // A time-travel scan reads with the schema of its snapshot, so a key column renamed or dropped since then
    // has another name or none in the current schema. We resolve equality columns by field id under the schema
    // the scan reads with, and fail with a clear error for an id it does not have.
    public static String findEqualityColumnName(IcebergTable icebergTable, int fieldId) {
        String name = icebergTable.getReadSchema().findColumnName(fieldId);
        if (name == null) {
            throw new StarRocksConnectorException("Equality delete column with field id %s is not in the read schema " +
                    "of table %s", fieldId, icebergTable.getCatalogTableName());
        }
        return name;
    }

    // equality table name format : origin_table_name + "_eq_delete_" + [pk1_pk2_pk3_..._]
    public static String buildEqualityDeleteTableName(IcebergTable icebergTable, List<Integer> equalityIds) {
        return icebergTable.getCatalogTableName() + "_eq_delete_" + equalityIds.stream()
                .map(id -> findEqualityColumnName(icebergTable, id))
                .collect(Collectors.joining("_"));
    }
}
