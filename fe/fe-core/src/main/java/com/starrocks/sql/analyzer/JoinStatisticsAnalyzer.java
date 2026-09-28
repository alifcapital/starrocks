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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.JoinStatisticsStmt;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.statistics.JoinStatisticsData;
import com.starrocks.statistic.JoinStatisticsDefinition;
import com.starrocks.statistic.JoinStatisticsMeta;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class JoinStatisticsAnalyzer {
    private JoinStatisticsAnalyzer() {
    }

    public static void analyze(JoinStatisticsStmt statement, ConnectContext context) {
        if (statement.getName() != null) {
            FeNameFormat.checkCommonName("JOIN statistics", statement.getName());
        }
        if (statement.getAction() == JoinStatisticsStmt.Action.SHOW) {
            return;
        }
        if (statement.getAction() != JoinStatisticsStmt.Action.CREATE) {
            JoinStatisticsMeta meta = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getJoinStatisticsRegistry()
                    .get(statement.getName());
            if (meta == null) {
                if (statement.getAction() == JoinStatisticsStmt.Action.DROP && statement.isIfExists()) {
                    return;
                }
                throw new SemanticException("Unknown JOIN statistics: %s", statement.getName());
            }
            statement.setDefinition(meta.getDefinition());
            return;
        }
        for (Map.Entry<String, String> property : statement.getProperties().entrySet()) {
            if (!StatsConstants.MCV_SIZE.equals(property.getKey())) {
                throw new SemanticException("Unknown JOIN statistics property: %s", property.getKey());
            }
            try {
                if (Integer.parseInt(property.getValue()) <= 0
                        || Integer.parseInt(property.getValue()) > JoinStatisticsData.MAX_SLICES) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                throw new SemanticException("mcv_size must be between 1 and %s", JoinStatisticsData.MAX_SLICES);
            }
        }
        if (statement.getQuery().hasOutFileClause()
                || !(statement.getQuery().getQueryRelation() instanceof SelectRelation select)) {
            throw new SemanticException("JOIN statistics require SELECT columns FROM tables JOIN ... ON keys");
        }
        Analyzer.analyze(statement.getQuery(), context);
        if (select.hasWhereClause() || select.hasAggregation() || select.hasAnalyticInfo() || select.hasWithClause()
                || select.hasLimit() || select.hasOrderByClause() || select.hasHavingClause()) {
            throw new SemanticException("JOIN statistics definitions cannot contain filters, grouping, windows or limits; "
                    + "list predicate columns in SELECT");
        }
        List<TableRelation> tables = new ArrayList<>();
        List<Expr> equalities = new ArrayList<>();
        collect(select.getRelation(), tables, equalities);
        if (tables.size() < 2 || tables.size() > 4) {
            throw new SemanticException("JOIN statistics require two to four tables");
        }
        List<Set<String>> predicates = new ArrayList<>();
        for (TableRelation table : tables) {
            if ((!table.getTable().isIcebergTable() && !table.getTable().isNativeTableOrMaterializedView())
                    || table.getQueryPeriod() != null || table.getSampleClause() != null
                    || table.hasTableHints() || table.getPartitionPredicate() != null) {
                throw new SemanticException("JOIN statistics require complete Iceberg or native tables without sampling or hints");
            }
            predicates.add(new LinkedHashSet<>());
        }
        for (SelectListItem item : select.getSelectList().getItems()) {
            if (item.isStar() || !(item.getExpr() instanceof SlotRef slot)) {
                throw new SemanticException("JOIN statistics SELECT must list scalar predicate columns");
            }
            int source = owner(slot, tables);
            Column column = tables.get(source).getTable().getColumn(slot.getColumnName());
            if (!supported(column.getType())) {
                throw new SemanticException("Unsupported JOIN statistics predicate type: %s", column.getType().toSql());
            }
            if (!predicates.get(source).add(column.getName())) {
                throw new SemanticException("Duplicate JOIN statistics predicate column: %s", column.getName());
            }
        }

        List<Map<Integer, String>> groups = new ArrayList<>();
        List<Type> types = new ArrayList<>();
        for (Expr equality : equalities) {
            if (!(equality instanceof BinaryPredicate binary) || binary.getOp() != BinaryType.EQ) {
                throw new SemanticException("JOIN statistics ON requires equality between join columns");
            }
            SlotRef left = keySlot(binary.getChild(0));
            SlotRef right = keySlot(binary.getChild(1));
            int leftSource = owner(left, tables);
            int rightSource = owner(right, tables);
            if (leftSource == rightSource) {
                throw new SemanticException("JOIN statistics ON must connect different tables");
            }
            Type common = TypeManager.getCompatibleTypeForBinary(false,
                    binary.getChild(0).getType(), binary.getChild(1).getType());
            if (!supported(common) || common.isFloatingPointType()) {
                throw new SemanticException("Unsupported JOIN statistics key type: %s", common.toSql());
            }
            String leftColumn = tables.get(leftSource).getTable().getColumn(left.getColumnName()).getName();
            String rightColumn = tables.get(rightSource).getTable().getColumn(right.getColumnName()).getName();
            Map<Integer, String> combined = new LinkedHashMap<>();
            combined.put(leftSource, leftColumn);
            combined.put(rightSource, rightColumn);
            for (int i = groups.size() - 1; i >= 0; i--) {
                Map<Integer, String> group = groups.get(i);
                boolean overlap = combined.entrySet().stream()
                        .anyMatch(entry -> entry.getValue().equals(group.get(entry.getKey())));
                if (!overlap) {
                    continue;
                }
                if (types.get(i).isIntegerType() && common.isIntegerType()) {
                    common = TypeManager.getCompatibleTypeForBinary(false, types.get(i), common);
                } else if (!types.get(i).equals(common)) {
                    throw new SemanticException("JOIN statistics key domain must use the same comparison type");
                }
                for (Map.Entry<Integer, String> entry : group.entrySet()) {
                    String previous = combined.putIfAbsent(entry.getKey(), entry.getValue());
                    if (previous != null && !previous.equals(entry.getValue())) {
                        throw new SemanticException("A JOIN key domain cannot contain two columns of the same table");
                    }
                }
                groups.remove(i);
                types.remove(i);
            }
            groups.add(combined);
            types.add(common);
        }
        if (groups.isEmpty()) {
            throw new SemanticException("JOIN statistics require equality keys");
        }
        Set<Integer> reached = new HashSet<>();
        reached.add(0);
        for (int pass = 0; pass < tables.size(); pass++) {
            for (Map<Integer, String> group : groups) {
                if (group.keySet().stream().anyMatch(reached::contains)) {
                    reached.addAll(group.keySet());
                }
            }
        }
        if (reached.size() != tables.size()) {
            throw new SemanticException("JOIN statistics tables must form a connected join graph");
        }
        List<JoinStatisticsDefinition.Source> sources = new ArrayList<>();
        Set<String> identities = new HashSet<>();
        for (int i = 0; i < tables.size(); i++) {
            if (predicates.get(i).size() > 32) {
                throw new SemanticException("JOIN statistics support at most 32 predicate columns per source");
            }
            TableName name = tables.get(i).getName();
            String uuid = tables.get(i).getTable().getUUID();
            String role = identities.add(uuid) ? uuid : "role:" + i + ":" + uuid;
            sources.add(new JoinStatisticsDefinition.Source(name.getCatalog(), name.getDb(), name.getTbl(),
                    role, uuid, List.copyOf(predicates.get(i))));
        }
        List<JoinStatisticsDefinition.KeyDomain> domains = new ArrayList<>();
        for (int i = 0; i < groups.size(); i++) {
            Map<Integer, List<String>> columns = new LinkedHashMap<>();
            for (var entry : groups.get(i).entrySet()) {
                Type actual = tables.get(entry.getKey()).getTable().getColumn(entry.getValue()).getType();
                if (!JoinStatisticsDefinition.matchesKeyType(actual, types.get(i).toSql())) {
                    throw new SemanticException("JOIN statistics key conversion is not supported: %s to %s",
                            actual.toSql(), types.get(i).toSql());
                }
                columns.put(entry.getKey(), List.of(entry.getValue()));
            }
            domains.add(new JoinStatisticsDefinition.KeyDomain(columns, List.of(types.get(i).toSql())));
        }
        domains = JoinStatisticsDefinition.tupleDomains(domains);
        if (domains.size() > JoinStatisticsDefinition.MAX_DOMAINS) {
            throw new SemanticException("JOIN statistics support at most %s distinct equality-key domains; "
                    + "components of the same tuple key count as one domain", JoinStatisticsDefinition.MAX_DOMAINS);
        }
        statement.setDefinition(new JoinStatisticsDefinition(statement.getName(), sources, domains, statement.getProperties()));
    }

    private static boolean supported(Type type) {
        return type.canStatistic() && !type.isComplexType() && !type.isJsonType() && !type.isOnlyMetricType();
    }

    private static SlotRef keySlot(Expr expression) {
        if (expression instanceof CastExpr cast && cast.isImplicit()
                && cast.getChild(0).getType().isIntegerType() && cast.getType().isIntegerType()
                && cast.getType().getTypeSize() >= cast.getChild(0).getType().getTypeSize()) {
            expression = cast.getChild(0);
        }
        if (!(expression instanceof SlotRef slot)) {
            throw new SemanticException("JOIN statistics keys must be columns with compatible types");
        }
        return slot;
    }

    private static int owner(SlotRef slot, List<TableRelation> tables) {
        int owner = -1;
        TableName qualifier = slot.getTblName();
        for (int i = 0; i < tables.size(); i++) {
            TableRelation table = tables.get(i);
            TableName resolved = table.getResolveTableName();
            if (qualifier != null && (!qualifier.getTbl().equalsIgnoreCase(resolved.getTbl())
                    || (qualifier.getDb() != null && !qualifier.getDb().equals(resolved.getDb()))
                    || (qualifier.getCatalog() != null && !qualifier.getCatalog().equals(resolved.getCatalog())))) {
                continue;
            }
            if (table.getTable().getColumn(slot.getColumnName()) != null) {
                if (owner >= 0) {
                    throw new SemanticException("Ambiguous JOIN statistics column: %s", slot.getColumnName());
                }
                owner = i;
            }
        }
        if (owner < 0) {
            throw new SemanticException("Unknown JOIN statistics column: %s", slot.getColumnName());
        }
        return owner;
    }

    private static void collect(Relation relation, List<TableRelation> tables, List<Expr> equalities) {
        if (relation instanceof TableRelation table) {
            tables.add(table);
        } else if (relation instanceof JoinRelation join && join.getJoinOp().isInnerJoin()
                && join.getOnPredicate() != null && join.getJoinHint().isEmpty()) {
            collect(join.getLeft(), tables, equalities);
            collect(join.getRight(), tables, equalities);
            conjuncts(join.getOnPredicate(), equalities);
        } else {
            throw new SemanticException("JOIN statistics definitions require explicit INNER JOIN ... ON without hints");
        }
    }

    private static void conjuncts(Expr expression, List<Expr> output) {
        if (expression instanceof CompoundPredicate compound && compound.getOp() == CompoundPredicate.Operator.AND) {
            conjuncts(expression.getChild(0), output);
            conjuncts(expression.getChild(1), output);
        } else {
            output.add(expression);
        }
    }
}
