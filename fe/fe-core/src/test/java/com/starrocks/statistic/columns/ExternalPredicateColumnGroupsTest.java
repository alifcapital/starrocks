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

package com.starrocks.statistic.columns;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ExternalPredicateColumnGroupsTest {
    private final ColumnRefFactory factory = new ColumnRefFactory();
    private final ExternalPredicateColumnGroups groups = new ExternalPredicateColumnGroups();
    private final PredicateColumnsMgr manager = new PredicateColumnsMgr();
    private boolean unitTest;

    @BeforeEach
    void before() {
        unitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    void after() {
        FeConstants.runningUnitTest = unitTest;
    }

    private IcebergTable table(String name) {
        IcebergTable table = mock(IcebergTable.class);
        when(table.getUUID()).thenReturn("iceberg.db." + name);
        when(table.getCatalogName()).thenReturn("iceberg");
        when(table.getCatalogDBName()).thenReturn("db");
        when(table.getCatalogTableName()).thenReturn(name);
        return table;
    }

    private ColumnRefOperator column(IcebergTable table, int relation, String name) {
        ColumnRefOperator ref = factory.create(name, IntegerType.INT, true);
        factory.updateColumnRefToColumns(ref, new Column(name, IntegerType.INT), table);
        factory.updateColumnToRelationIds(ref.getId(), relation);
        return ref;
    }

    private Set<List<String>> columns() {
        return groups.snapshot().stream().map(ExternalColumnGroupUsage::columns).collect(Collectors.toSet());
    }

    private BinaryPredicateOperator eq(ColumnRefOperator left, ColumnRefOperator right) {
        return new BinaryPredicateOperator(BinaryType.EQ, left, right);
    }

    @Test
    void separateQueriesAndReorderedRepeatedPredicates() {
        IcebergTable table = table("transactions");
        var status = column(table, 1, "status");
        var gate = column(table, 1, "dest_acc_gate");
        var type = column(table, 1, "dest_acc_type");
        var user = column(table, 1, "user_id");
        var date = column(table, 1, "created_at");
        groups.record(List.of(status, gate, type), ColumnUsage.UseCase.PREDICATE, factory, null);
        groups.record(List.of(user, date), ColumnUsage.UseCase.PREDICATE, factory, null);
        groups.record(List.of(type, status, gate, status), ColumnUsage.UseCase.PREDICATE, factory, null);
        assertEquals(Set.of(List.of("dest_acc_gate", "dest_acc_type", "status"),
                List.of("created_at", "user_id")), columns());
    }

    @Test
    void selfJoinDoesNotMergeAliases() {
        IcebergTable table = table("users");
        var a = column(table, 1, "id");
        var b = column(table, 1, "country");
        var c = column(table, 2, "parent_id");
        var d = column(table, 2, "parent_country");
        groups.recordJoin(List.of(eq(a, c), eq(d, b)), factory, null);
        assertEquals(Set.of(List.of("country", "id"), List.of("parent_country", "parent_id")), columns());
    }

    @Test
    void independentJoinPartnersStaySeparate() {
        IcebergTable a = table("a");
        IcebergTable b = table("b");
        IcebergTable c = table("c");
        groups.recordJoin(List.of(eq(column(a, 1, "a1"), column(b, 2, "b1")),
                eq(column(b, 2, "b2"), column(a, 1, "a2")),
                eq(column(a, 1, "a3"), column(c, 3, "c1"))), factory, null);
        assertEquals(Set.of(List.of("a1", "a2"), List.of("b1", "b2"), List.of("a3"), List.of("c1")), columns());
    }

    @Test
    void filtersOnTwoScanInstancesAreNotCombined() {
        IcebergTable table = table("t");
        groups.record(List.of(column(table, 1, "x"), column(table, 2, "y")),
                ColumnUsage.UseCase.PREDICATE, factory, null);
        assertEquals(Set.of(List.of("x"), List.of("y")), columns());
    }

    @Test
    void projectionAliasesPreserveScanIdentity() {
        IcebergTable table = table("t");
        var x = column(table, 1, "x");
        var y = column(table, 1, "y");
        var alias = factory.create("alias", IntegerType.INT, true);
        OptExpression expression = OptExpression.create(new LogicalProjectOperator(Map.of(alias, x)));
        groups.record(List.of(alias, y), ColumnUsage.UseCase.GROUP_BY, factory, expression);
        assertEquals(Set.of(List.of("x", "y")), columns());
    }

    @Test
    void unknownLineageDoesNotCreatePartialGroup() {
        var x = column(table("t"), 1, "x");
        var unknown = factory.create("unknown", IntegerType.INT, true);
        groups.record(List.of(x, unknown), ColumnUsage.UseCase.PREDICATE, factory, null);
        assertTrue(groups.snapshot().isEmpty());
    }

    @Test
    void missingRelationIdentityDoesNotCombineAliases() {
        var ref = factory.create("x", IntegerType.INT, true);
        factory.updateColumnRefToColumns(ref, new Column("x", IntegerType.INT), table("t"));
        groups.record(List.of(ref), ColumnUsage.UseCase.PREDICATE, factory, null);
        assertTrue(groups.snapshot().isEmpty());
    }

    @Test
    void purposeIsPartOfIdentityAndColumnsAreImmutable() {
        var x = column(table("t"), 1, "x");
        groups.record(List.of(x), ColumnUsage.UseCase.PREDICATE, factory, null);
        groups.record(List.of(x), ColumnUsage.UseCase.GROUP_BY, factory, null);
        assertEquals(2, groups.snapshot().size());
        assertThrows(UnsupportedOperationException.class, () -> groups.snapshot().get(0).columns().add("y"));
    }

    @Test
    void existingManagerHooksRecordFilterAndWindowGroups() {
        IcebergTable table = table("t");
        var x = column(table, 1, "x");
        var y = column(table, 1, "y");
        manager.recordPredicateColumns(Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.EQ, x, ConstantOperator.createInt(1)),
                new BinaryPredicateOperator(BinaryType.EQ, y, ConstantOperator.createInt(2))), factory, null);
        manager.recordWindowPartitionBy(List.of(x, y), factory, null);
        var result = manager.queryExternalPredicateColumnGroups(table);
        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(group -> group.columns().equals(List.of("x", "y"))));
        assertEquals(2, manager.queryExternalPredicateColumns(table).size());
    }

    @Test
    void distinctArgumentsAndGroupingKeysRemainSeparate() {
        IcebergTable table = table("t");
        var x = column(table, 1, "x");
        var y = column(table, 1, "y");
        var z = column(table, 1, "z");
        var first = factory.create("first", IntegerType.BIGINT, true);
        var second = factory.create("second", IntegerType.BIGINT, true);
        manager.recordGroupByColumns(Map.of(
                first, new CallOperator("count", IntegerType.BIGINT, List.of(x, y), null, true),
                second, new CallOperator("count", IntegerType.BIGINT, List.of(z), null, true)),
                List.of(y, z), factory, null);
        var result = manager.queryExternalPredicateColumnGroups(table);
        assertEquals(3, result.size());
        assertEquals(Set.of(List.of("x", "y"), List.of("z")), result.stream()
                .filter(group -> group.useCase() == ColumnUsage.UseCase.DISTINCT)
                .map(ExternalColumnGroupUsage::columns).collect(Collectors.toSet()));
        assertEquals(List.of("y", "z"), result.stream()
                .filter(group -> group.useCase() == ColumnUsage.UseCase.GROUP_BY).findFirst().orElseThrow().columns());
    }

    @Test
    void unionOutputDoesNotMergeDifferentScanInstances() {
        IcebergTable table = table("t");
        var left = column(table, 1, "x");
        var right = column(table, 2, "y");
        var output = factory.create("union", IntegerType.INT, true);
        OptExpression union = OptExpression.create(new LogicalUnionOperator(
                List.of(output), List.of(List.of(left), List.of(right)), true));
        groups.record(List.of(output), ColumnUsage.UseCase.PREDICATE, factory, union);
        assertTrue(groups.snapshot().isEmpty());
    }

    @Test
    void historyRetainsWideSetsForBasicStatisticsEvenBeyondMcvLimit() {
        int previous = Config.statistics_max_multi_column_combined_num;
        try {
            Config.statistics_max_multi_column_combined_num = 2;
            IcebergTable table = table("t");
            groups.record(List.of(column(table, 1, "x"), column(table, 1, "y"), column(table, 1, "z")),
                    ColumnUsage.UseCase.PREDICATE, factory, null);
            assertEquals(Set.of(List.of("x", "y", "z")), columns());
        } finally {
            Config.statistics_max_multi_column_combined_num = previous;
        }
    }

    @Test
    void basicAndMcvShareSetsWithoutSyntheticSingletons() {
        IcebergTable table = table("t");
        var x = column(table, 1, "x");
        var y = column(table, 1, "y");
        var z = column(table, 1, "z");
        manager.recordScanColumns(Map.of(x, new Column("x", IntegerType.INT),
                y, new Column("y", IntegerType.INT), z, new Column("z", IntegerType.INT)), table, null);
        manager.recordPredicateColumns(Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.EQ, x, ConstantOperator.createInt(1)),
                new BinaryPredicateOperator(BinaryType.EQ, y, ConstantOperator.createInt(2))), factory, null);
        assertEquals(List.of(List.of("x", "y")), manager.queryExternalPredicateColumnGroups(table).stream()
                .map(ExternalColumnGroupUsage::columns).toList());
        assertEquals(List.of("x", "y"), manager.queryExternalPredicateColumns(table));
        manager.recordPredicateColumns(new BinaryPredicateOperator(BinaryType.EQ, z, ConstantOperator.createInt(3)),
                factory, null);
        assertEquals(Set.of(List.of("x", "y"), List.of("z")), manager.queryExternalPredicateColumnGroups(table).stream()
                .map(ExternalColumnGroupUsage::columns).collect(Collectors.toSet()));
        assertEquals(List.of("x", "y", "z"), manager.queryExternalPredicateColumns(table));
    }

    @Test
    void separatelyObservedSingletonSurvivesAlongsideCompoundSet() {
        IcebergTable table = table("t");
        var x = column(table, 1, "x");
        var y = column(table, 1, "y");
        manager.recordGroupByColumns(Map.of(), List.of(x, y), factory, null);
        manager.recordGroupByColumns(Map.of(), List.of(x), factory, null);
        assertEquals(Set.of(List.of("x", "y"), List.of("x")), manager.queryExternalPredicateColumnGroups(table).stream()
                .map(ExternalColumnGroupUsage::columns).collect(Collectors.toSet()));
        assertEquals(List.of("x", "y"), manager.queryExternalPredicateColumns(table));
    }

    @Test
    void disablingCollectionStopsNewGroups() {
        boolean previous = Config.enable_external_predicate_columns_collection;
        try {
            Config.enable_external_predicate_columns_collection = false;
            groups.record(List.of(column(table("t"), 1, "x")), ColumnUsage.UseCase.PREDICATE, factory, null);
            assertTrue(groups.snapshot().isEmpty());
        } finally {
            Config.enable_external_predicate_columns_collection = previous;
        }
    }

    @Test
    void namesCannotCollideByConcatenation() {
        var first = ExternalColumnGroupUsage.of(table("t"), List.of("a,b", "c"),
                ColumnUsage.UseCase.PREDICATE, LocalDateTime.now());
        var second = ExternalColumnGroupUsage.of(table("t"), List.of("a", "b,c"),
                ColumnUsage.UseCase.PREDICATE, LocalDateTime.now());
        assertNotEquals(first.groupId(), second.groupId());
        assertFalse(first.columnsJson().isEmpty());
    }
}
