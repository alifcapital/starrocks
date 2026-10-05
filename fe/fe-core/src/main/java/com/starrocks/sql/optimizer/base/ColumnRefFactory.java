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

package com.starrocks.sql.optimizer.base;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.LambdaArgument;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class ColumnRefFactory {
    private int nextId = 1;
    // The unique id for each scan operator
    // For table a join table a, the two unique ids for table a is different
    private int nextRelationId = 1;
    private long tableMappingVersion;
    private long sourceMappingVersion;
    private final List<ColumnRefOperator> columnRefs = Lists.newArrayList();
    private final Map<Integer, Integer> columnToRelationIds = Maps.newHashMap();
    private final Map<ColumnRefOperator, Column> columnRefToColumns = Maps.newHashMap();
    private final Map<ColumnRefOperator, Table> columnRefToTable = Maps.newHashMap();

    // Cache of ColumnRefOperators resolved for LambdaArgument AST nodes during this planning session.
    // Keyed by AST identity so that the same lambda argument referenced multiple times within a plan
    // resolves to the same ColumnRefOperator id. Lifetime matches this factory: a re-plan creates a
    // fresh factory and thus a fresh cache, which prevents stale ids from leaking across plans
    // (see issue #72831 / PR #72832).
    private final Map<LambdaArgument, ColumnRefOperator> lambdaArgRefs = new IdentityHashMap<>();

    // introduced to used to get unique id for query,
    // now used to identify nondeterministic function.
    // do not reuse nextId because it will affect many UTs.
    private int id = 1;

    // We count the LargeInPredicateOperators that SqlToScalarOperatorTranslator created with this factory: we want to
    // notice one that ends up where the rule does not transform it. LargeInPredicateToJoinRule checks that it turned
    // as many of them into joins.
    private int largeInPredicateCount = 0;

    public Map<ColumnRefOperator, Column> getColumnRefToColumns() {
        return columnRefToColumns;
    }

    public void addLargeInPredicate() {
        largeInPredicateCount++;
    }

    public int getLargeInPredicateCount() {
        return largeInPredicateCount;
    }

    public ColumnRefOperator create(Expr expression, Type type, boolean nullable) {
        String nameHint = "expr";
        if (expression instanceof SlotRef) {
            nameHint = ((SlotRef) expression).getColumnName();
        } else if (expression instanceof FunctionCallExpr) {
            nameHint = ((FunctionCallExpr) expression).getFnRef().getFnName().toString();
        } else if (expression instanceof CaseExpr) {
            nameHint = "case";
        } else if (expression instanceof CastExpr) {
            nameHint = "cast";
        }
        return create(nextId++, nameHint, type, nullable, false);
    }

    public ColumnRefOperator create(ScalarOperator operator, Type type, boolean nullable) {
        String nameHint = "expr";
        if (operator.isColumnRef()) {
            nameHint = ((ColumnRefOperator) operator).getName();
        } else if (operator instanceof CallOperator) {
            if (operator instanceof CaseWhenOperator) {
                nameHint = "case";
            } else if (operator instanceof CastOperator) {
                nameHint = "cast";
            } else {
                nameHint = ((CallOperator) operator).getFnName();
            }
        }
        return create(nextId++, nameHint, type, nullable, false);
    }

    public ColumnRefOperator create(String name, Type type, boolean nullable) {
        return create(nextId++, name, type, nullable, false);
    }

    public ColumnRefOperator create(String name, Type type, boolean nullable, boolean isLambdaArg) {
        return create(nextId++, name, type, nullable, isLambdaArg);
    }

    private ColumnRefOperator create(int id, String name, Type type, boolean nullable, boolean isLambdaArg) {
        ColumnRefOperator columnRef = new ColumnRefOperator(id, type, name, nullable, isLambdaArg);
        columnRefs.add(columnRef);
        return columnRef;
    }

    public ColumnRefOperator getColumnRef(int id) {
        return columnRefs.get(id - 1);
    }

    public Set<ColumnRefOperator> getColumnRefs(ColumnRefSet columnRefSet) {
        Set<ColumnRefOperator> columnRefOperators = Sets.newHashSet();
        for (int idx : columnRefSet.getColumnIds()) {
            columnRefOperators.add(getColumnRef(idx));
        }
        return columnRefOperators;
    }

    // The map is a default HashMap filled in ascending id order, and its iteration order can reach plan projections.
    public Map<ColumnRefOperator, ScalarOperator> getIdentityColumnRefMap(ColumnRefSet columnRefSet) {
        Map<ColumnRefOperator, ScalarOperator> identityMap = Maps.newHashMap();
        for (int idx : columnRefSet.getColumnIds()) {
            ColumnRefOperator columnRef = getColumnRef(idx);
            identityMap.put(columnRef, columnRef);
        }
        return identityMap;
    }

    public List<ColumnRefOperator> getColumnRefs() {
        return columnRefs;
    }

    public void updateColumnRefToColumns(ColumnRefOperator columnRef, Column column, Table table) {
        boolean changed = columnRefToColumns.put(columnRef, column) != column;
        if (columnRefToTable.put(columnRef, table) != table) {
            tableMappingVersion++;
            changed = true;
        }
        if (changed) {
            sourceMappingVersion++;
        }
    }

    public Column getColumn(ColumnRefOperator columnRef) {
        return columnRefToColumns.get(columnRef);
    }

    public Pair<Table, Column> getTableAndColumn(ColumnRefOperator columnRef) {
        Column column = getColumn(columnRef);
        if (column == null) {
            return null;
        }
        return Pair.create(columnRefToTable.get(columnRef), column);
    }

    public void updateColumnToRelationIds(int columnId, int tableId) {
        Integer previous = columnToRelationIds.put(columnId, tableId);
        if (previous == null || previous != tableId) {
            sourceMappingVersion++;
        }
    }

    public Integer getRelationId(int id) {
        return columnToRelationIds.getOrDefault(id, -1);
    }

    public int getNextRelationId() {
        return nextRelationId++;
    }

    public Map<Integer, Integer> getColumnToRelationIds() {
        return columnToRelationIds;
    }

    /** Changes when rewrites register or replace a column's source table. */
    public long getTableMappingVersion() {
        return tableMappingVersion;
    }

    /**
     * Changes whenever the column, table or relation that a column ref comes from is registered or replaced.
     * Whoever caches what it derived from these mappings can keep the result while the version stays the same.
     */
    public long getSourceMappingVersion() {
        return sourceMappingVersion;
    }

    public Map<ColumnRefOperator, Table> getColumnRefToTable() {
        return columnRefToTable;
    }

    public Table getTableForColumn(int columnId) {
        return columnRefToTable.get(getColumnRef(columnId));
    }

    public int getNextUniqueId() {
        return id++;
    }

    public ColumnRefOperator computeLambdaArgRefIfAbsent(LambdaArgument arg,
                                                         Function<LambdaArgument, ColumnRefOperator> creator) {
        return lambdaArgRefs.computeIfAbsent(arg, creator);
    }
}
