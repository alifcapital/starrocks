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

package com.starrocks.sql.optimizer.transformer;

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.View;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.ast.expression.AnalyticWindowBoundary;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.ArrowExpr;
import com.starrocks.sql.ast.expression.BetweenPredicate;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.CollectionElementExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.ExistsPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FieldReference;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.LargeStringLiteral;
import com.starrocks.sql.ast.expression.LikePredicate;
import com.starrocks.sql.ast.expression.LimitElement;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.ast.expression.NamedArgument;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.SubfieldExpr;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr;
import com.starrocks.sql.ast.expression.UserVariableExpr;
import com.starrocks.sql.ast.expression.VarBinaryLiteral;
import com.starrocks.sql.ast.expression.VariableExpr;
import com.starrocks.thrift.TFunctionBinaryType;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Finds FROM subqueries of one query that compute the same rows, so that the transformer can turn each group into
 * one CTE and let the cost model choose between reuse and inline.
 *
 * <p>Two subqueries are identical when their analyzed bodies have the same structural key, see {@link KeyBuilder}.
 * The key does not come from printed SQL: a printer is free to leave out what it does not need for reading, and
 * every such omission would merge subqueries that read different data. Output columns are matched by position.
 *
 * <p>We collect only subqueries that the root reaches through FROM clauses, CTE definitions and set operations.
 * A subquery under an expression subquery can read columns of an outer query, so two copies with the same key
 * can still return different rows. A FROM subquery never reads the columns of the relations next to it: the
 * analyzer resolves a LATERAL subquery in the scope of the enclosing query and allows a LATERAL join only with a
 * table function, which the key does not take. So a subquery that the root reaches this way sees no outer columns,
 * its plan does not depend on where it is planned, and the transformer can plan the first copy alone as the
 * producer.
 */
public class IdenticalSubqueryCollector {
    private static final class Candidate {
        private final SubqueryRelation relation;
        // the nearest enclosing candidate
        private final Candidate parent;
        private boolean rejected;
        private boolean chosen;
        private int key;

        private Candidate(SubqueryRelation relation, Candidate parent) {
            this.relation = relation;
            this.parent = parent;
        }
    }

    private final List<Candidate> candidates = new ArrayList<>();
    private final Set<SubqueryRelation> seen = Collections.newSetFromMap(new IdentityHashMap<>());

    private IdenticalSubqueryCollector() {
    }

    public static boolean isEnabled(ConnectContext session) {
        SessionVariable sessionVariable = session.getSessionVariable();
        // cbo_cte_reuse is also false without the pipeline engine. The incremental refresh rewrite and the stream
        // planner of an mv rewrite the plan operator by operator and have no rule for a CTE, so we leave their
        // plans as they are.
        return sessionVariable.isCboCTEReuseIdenticalSubqueries() && sessionVariable.isCboCteReuse()
                && !sessionVariable.isEnableIVMRefresh() && !sessionVariable.isMVPlanner();
    }

    /**
     * Returns groups of identical subqueries, each group in the order of the query text. A group comes before any
     * group whose subqueries contain its subqueries. A subquery inside a member of a group is never a member of a
     * later group. Returns an empty list when the feature is off.
     */
    public static List<List<SubqueryRelation>> collect(QueryRelation root, ConnectContext session) {
        if (!isEnabled(session)) {
            return List.of();
        }
        IdenticalSubqueryCollector collector = new IdenticalSubqueryCollector();
        Walker walker = collector.new Walker();
        walker.rootCteDefinitions.addAll(root.getCteRelations());
        walker.visit(root);
        return collector.chooseGroups();
    }

    private List<List<SubqueryRelation>> chooseGroups() {
        KeyBuilder keyBuilder = new KeyBuilder();
        Map<Integer, List<Candidate>> byKey = new HashMap<>();
        for (Candidate candidate : candidates) {
            if (candidate.rejected) {
                continue;
            }
            candidate.key = keyBuilder.bodyKey(candidate.relation.getQueryStatement());
            if (candidate.key == KeyBuilder.REJECTED) {
                candidate.rejected = true;
                continue;
            }
            byKey.computeIfAbsent(candidate.key, k -> new ArrayList<>()).add(candidate);
        }

        // We go in the order of the query text, so an enclosing subquery is chosen before the subqueries inside
        // it, and those are left out: the producer of the enclosing CTE computes them once already.
        List<List<SubqueryRelation>> groups = new ArrayList<>();
        for (Candidate candidate : candidates) {
            if (candidate.rejected || candidate.chosen || isInsideChosen(candidate)) {
                continue;
            }
            List<Candidate> group = byKey.get(candidate.key).stream()
                    .filter(c -> c == candidate || (!c.chosen && !isInsideChosen(c)))
                    .collect(Collectors.toList());
            if (group.size() < 2) {
                continue;
            }
            group.forEach(c -> c.chosen = true);
            groups.add(group.stream().map(c -> c.relation).collect(Collectors.toList()));
        }
        return groups;
    }

    private static boolean isInsideChosen(Candidate candidate) {
        for (Candidate parent = candidate.parent; parent != null; parent = parent.parent) {
            if (parent.chosen) {
                return true;
            }
        }
        return false;
    }

    // Finds the candidates in the order of the query text
    private class Walker extends AstTraverser<Void, Void> {
        private Candidate current;
        // Above zero, we walk where a subquery is not a candidate: under an expression subquery, see the class
        // comment, or in a CTE definition of the root, see rootCteDefinitions.
        private int noCandidateDepth;
        // The transformer plans the CTE of the root before the anchors of the synthetic CTE exist, so a
        // subquery in their definitions could not become a consumer.
        private final Set<CTERelation> rootCteDefinitions = Collections.newSetFromMap(new IdentityHashMap<>());

        private void visitWithoutCandidates(ParseNode node) {
            noCandidateDepth++;
            try {
                visit(node);
            } finally {
                noCandidateDepth--;
            }
        }

        @Override
        public Void visitSubqueryRelation(SubqueryRelation node, Void context) {
            // The transformer keys the synthetic CTE by the subquery object, so one object is one candidate
            if (noCandidateDepth > 0 || !seen.add(node)) {
                return super.visitSubqueryRelation(node, context);
            }
            Candidate candidate = new Candidate(node, current);
            candidates.add(candidate);
            // A relation that checks for at most one row stands for a scalar subquery, which we leave as is. A
            // relation that a security policy wraps around a table or view is not a copy that the user wrote: we
            // do not want every table that is read twice under a policy to become a CTE.
            candidate.rejected = node.isAssertRows() || node.isSecurityPolicyView();
            Candidate saved = current;
            current = candidate;
            try {
                super.visitSubqueryRelation(node, context);
            } finally {
                current = saved;
            }
            return null;
        }

        @Override
        public Void visitSubqueryExpr(Subquery node, Void context) {
            visitWithoutCandidates(node.getQueryStatement());
            return null;
        }

        @Override
        public Void visitCTE(CTERelation node, Void context) {
            if (!node.isResolvedInFromClause() && rootCteDefinitions.contains(node)) {
                visitWithoutCandidates(node.getCteQueryStatement());
                return null;
            }
            if (node.isResolvedInFromClause()) {
                // the definition is walked once, where it is defined
                return null;
            }
            return super.visitCTE(node, context);
        }
    }

    /**
     * Builds the structural key of an analyzed query body. The key holds every field that the transformer reads
     * from a node; a node class that the key does not know rejects the whole body, so that a missing field cannot
     * merge two different subqueries.
     *
     * <ul>
     * <li>A column reference is the field that the analyzer resolved it to: the scope level and the position of the
     *   field in it, the catalog, database and table that the field comes from, the column name, type and struct
     *   path. The relations are in the key in the order of the query, so a position names one column. Aliases of
     *   relations and names of CTE are not in the key.</li>
     * <li>A table is its id (its uuid and snapshot for an external catalog) with partitions, tablets, replicas and
     *   version; a view is its id and definition; a CTE reference is the key of the CTE body with the column list
     *   of the CTE.</li>
     * <li>A literal is its type and full value; a function is its resolved signature.</li>
     * <li>The output column names of every query block are in the key, the alias of the subquery itself is not.</li>
     * </ul>
     *
     * <p>We reject bodies whose rows can differ between two evaluations in one query: non-deterministic functions,
     * user defined functions, system tables, table functions, JDBC and other tables without a snapshot, samples.
     * We also reject hints, because a hint inside one copy is a request for that copy only.
     */
    private static final class KeyBuilder {
        private static final int REJECTED = -1;

        // Each distinct body key gets a number, so an enclosing body refers to a nested body by its number and the
        // total work stays linear in the size of the query
        private final Map<String, Integer> interned = new HashMap<>();
        private final Map<QueryStatement, Integer> bodyKeys = new IdentityHashMap<>();

        // Where the transformer reads each field of a scope from, see column()
        private final Map<Scope, List<String>> origins = new IdentityHashMap<>();

        private StringBuilder sb;
        private boolean rejected;
        // The scope that the analyzer resolved the column references of the current expressions in
        private Scope scope;

        int bodyKey(QueryStatement body) {
            Integer key = bodyKeys.get(body);
            if (key != null) {
                return key;
            }
            StringBuilder savedSb = sb;
            boolean savedRejected = rejected;
            Scope savedScope = scope;
            sb = new StringBuilder();
            rejected = false;
            queryRelation(body.getQueryRelation());
            key = rejected ? REJECTED : interned.computeIfAbsent(sb.toString(), k -> interned.size());
            sb = savedSb;
            rejected = savedRejected;
            scope = savedScope;
            bodyKeys.put(body, key);
            return key;
        }

        private void reject() {
            rejected = true;
        }

        // Each token is written with its length, so that no two different lists of tokens give one key
        private KeyBuilder add(Object token) {
            String text = String.valueOf(token);
            sb.append(text.length()).append(':').append(text);
            return this;
        }

        private void nested(QueryStatement body) {
            int key = bodyKey(body);
            if (key == REJECTED) {
                reject();
            }
            add("body").add(key);
        }

        // ------------------------------------------- Relations -------------------------------------------

        private void relation(Relation relation) {
            if (rejected) {
                return;
            }
            if (relation == null || relation.getScope() == null) {
                reject();
                return;
            }
            // The alias of a relation is not in the key: column references are keyed by the column that they
            // resolve to, see column()
            Class<?> type = relation.getClass();
            add(type.getSimpleName());
            add(relation.getExplicitColumnNames());
            outputFields(relation.getScope());
            if (type == SelectRelation.class || type == UnionRelation.class || type == ExceptRelation.class
                    || type == IntersectRelation.class || type == ValuesRelation.class) {
                queryRelation((QueryRelation) relation);
            } else if (type == SubqueryRelation.class) {
                SubqueryRelation subquery = (SubqueryRelation) relation;
                add(subquery.isAssertRows());
                withClause(subquery);
                nested(subquery.getQueryStatement());
                orderByAndLimit(subquery, subquery.getScope());
            } else if (type == JoinRelation.class) {
                join((JoinRelation) relation);
            } else if (type == TableRelation.class) {
                table((TableRelation) relation);
            } else if (type == ViewRelation.class) {
                ViewRelation view = (ViewRelation) relation;
                View catalogView = view.getView();
                add(tableNameText(view.getName())).add(catalogView.getId()).add(catalogView.getInlineViewDef());
                nested(view.getQueryStatement());
            } else if (type == CTERelation.class && ((CTERelation) relation).isResolvedInFromClause()) {
                CTERelation cte = (CTERelation) relation;
                if (cte.isRecursive()) {
                    reject();
                    return;
                }
                add(cte.getColumnOutputNames());
                nested(cte.getCteQueryStatement());
            } else {
                // table functions, files(), pivot and other relations
                reject();
            }
            if (type != JoinRelation.class) {
                String origin = type == TableRelation.class
                        ? "table:" + tableNameText(((TableRelation) relation).getName())
                        : "relation:" + type.getSimpleName();
                origins.put(relation.getScope(),
                        Collections.nCopies(relation.getScope().getRelationFields().getAllFields().size(), origin));
            }
        }

        private static String tableNameText(TableName name) {
            return name == null ? "none" : name.getCatalog() + "." + name.getDb() + "." + name.getTbl();
        }

        // Output names are compared in lower case, as StarRocks compares column names. The consumer keeps its own
        // scope, so the names that the query shows stay as each copy writes them.
        private void outputFields(Scope fieldsScope) {
            for (Field field : fieldsScope.getRelationFields().getAllFields()) {
                add("field").add(lowerCase(field.getName())).add(field.getType());
            }
        }

        private static String lowerCase(String name) {
            return name == null ? null : name.toLowerCase();
        }

        private void queryRelation(QueryRelation relation) {
            if (rejected) {
                return;
            }
            if (relation.getScope() == null) {
                reject();
                return;
            }
            Class<?> type = relation.getClass();
            add(type.getSimpleName());
            outputFields(relation.getScope());
            add(relation.getColumnOutputNames().stream().map(KeyBuilder::lowerCase).collect(Collectors.toList()));
            withClause(relation);
            if (type == SelectRelation.class) {
                select((SelectRelation) relation);
            } else if (type == UnionRelation.class || type == ExceptRelation.class
                    || type == IntersectRelation.class) {
                SetOperationRelation setOperation = (SetOperationRelation) relation;
                add(setOperation.getQualifier()).add(setOperation.getRelations().size());
                setOperation.getRelations().forEach(this::relation);
                orderByAndLimit(relation, relation.getScope());
            } else if (type == ValuesRelation.class) {
                ValuesRelation values = (ValuesRelation) relation;
                add(values.getOutputColumnTypes()).add(values.isNullValues());
                for (List<Expr> row : values.getRows()) {
                    exprs("row", row);
                }
                orderByAndLimit(relation, relation.getScope());
            } else if (type == SubqueryRelation.class) {
                relation(relation);
            } else {
                reject();
            }
        }

        // The name of a CTE is not in the key: a reference is keyed by the body and the column list of the CTE
        private void withClause(QueryRelation relation) {
            if (relation.isHasRecursiveCTE()) {
                reject();
                return;
            }
            add("with").add(relation.getCteRelations().size());
            for (CTERelation definition : relation.getCteRelations()) {
                if (definition.isRecursive()) {
                    reject();
                    return;
                }
                add(definition.getColumnOutputNames());
                nested(definition.getCteQueryStatement());
            }
        }

        private void orderByAndLimit(QueryRelation relation, Scope orderScope) {
            Scope saved = scope;
            scope = orderScope;
            orderBy(relation.getOrderBy());
            scope = saved;
            LimitElement limit = relation.getLimit();
            if (limit == null) {
                add("nolimit");
            } else {
                add("limit").add(limit.getLimit()).add(limit.getOffset());
            }
        }

        private void select(SelectRelation select) {
            if (CollectionUtils.isNotEmpty(select.getSelectList().getHintNodes())
                    || CollectionUtils.isNotEmpty(select.getGroupingFunctionCallExprs())) {
                reject();
                return;
            }
            add(select.isDistinct());
            relation(select.getRelation());
            if (rejected) {
                return;
            }
            // The analyzer resolves the clauses before ORDER BY in the scope of the FROM relation
            Scope sourceScope = select.getRelation().getScope();
            Scope saved = scope;
            scope = sourceScope;
            add("output");
            for (Expr output : select.getOutputExpression()) {
                // SELECT * gives field references; we key them as the columns that they stand for, so that SELECT *
                // and the list of the same columns have one key
                if (output instanceof FieldReference) {
                    FieldReference reference = (FieldReference) output;
                    List<Field> fields = sourceScope.getRelationFields().getAllFields();
                    if (reference.getFieldIndex() < 0 || reference.getFieldIndex() >= fields.size()) {
                        reject();
                        return;
                    }
                    column(sourceScope, 0, reference.getFieldIndex(), fields.get(reference.getFieldIndex()).getType(),
                            null);
                } else {
                    expr(output);
                }
            }
            add("where");
            expr(select.getPredicate());
            exprs("groupby", select.getGroupBy());
            add("groupingsets");
            if (select.getGroupingSetsList() == null) {
                add("none");
            } else {
                for (List<Expr> set : select.getGroupingSetsList()) {
                    exprs("set", set);
                }
            }
            exprs("aggregate", select.getAggregate());
            add("having");
            expr(select.getHaving());
            exprs("outputanalytic", select.getOutputAnalytic());
            exprs("ordersource", select.getOrderSourceExpressions());
            // and ORDER BY in the order scope, which sees the output columns first
            Scope orderScope = select.getOrderScope() != null ? select.getOrderScope() : sourceScope;
            scope = orderScope;
            exprs("orderbyanalytic", select.getOrderByAnalytic());
            exprs("orderbyexpr", select.getOrderByExpressions());
            scope = saved;
            orderByAndLimit(select, orderScope);
        }

        private void join(JoinRelation join) {
            if ((join.getJoinHint() != null && !join.getJoinHint().isEmpty()) || join.getSkewColumn() != null
                    || join.isLateral()) {
                reject();
                return;
            }
            add(join.getJoinOp()).add(join.isImplicit()).add(join.getUsingColNames());
            relation(join.getLeft());
            relation(join.getRight());
            if (rejected) {
                return;
            }
            // The analyzer resolves ON in the fields of both sides, which can differ from the output of the join,
            // for example of a semi join
            Scope left = join.getLeft().getScope();
            Scope right = join.getRight().getScope();
            Scope onScope = new Scope(RelationId.anonymous(),
                    left.getRelationFields().joinWith(right.getRelationFields()));
            onScope.setParent(scope);
            List<String> both = new ArrayList<>(originsOf(left));
            both.addAll(originsOf(right));
            origins.put(onScope, both);
            int outputSize = join.getScope().getRelationFields().getAllFields().size();
            if (outputSize == both.size()) {
                origins.put(join.getScope(), both);
            } else if (outputSize == originsOf(left).size() && join.getJoinOp().isLeftSemiAntiJoin()) {
                origins.put(join.getScope(), originsOf(left));
            } else if (outputSize == originsOf(right).size() && join.getJoinOp().isRightSemiAntiJoin()) {
                origins.put(join.getScope(), originsOf(right));
            }
            Scope saved = scope;
            scope = onScope;
            add("on");
            expr(join.getOnPredicate());
            scope = saved;
        }

        private List<String> originsOf(Scope fieldsScope) {
            List<String> known = origins.get(fieldsScope);
            return known != null ? known
                    : Collections.nCopies(fieldsScope.getRelationFields().getAllFields().size(), "none");
        }

        private void table(TableRelation node) {
            Table table = node.getTable();
            if (table == null || !isSnapshotTable(table) || node.hasTableHints() || node.isMetaQuery()
                    || node.isBinlogQuery() || node.isCacheStatsQuery() || node.isSyncMVQuery()
                    || node.getPartitionPredicate() != null || node.getSampleClause() != null) {
                // A sample without a seed draws a new seed for every relation
                reject();
                return;
            }
            if (table.isNativeTableOrMaterializedView()) {
                // The planner reads one copy of a native table per table id, see OlapTableCollector
                add("native").add(table.getId());
                if (table instanceof OlapTable) {
                    add(((OlapTable) table).getBaseIndexMetaId());
                }
            } else {
                // The analyzer loads an external table once per relation, so we compare the table and the snapshot
                // that the transformer will read from it
                add("external").add(table.getType()).add(table.getUUID()).add(externalTableVersion(node, table));
            }
            add(tableNameText(node.getName()));
            PartitionRef partitions = node.getPartitionNames();
            if (partitions == null) {
                add("allpartitions");
            } else {
                add(partitions.getPartitionNames()).add(partitions.isTemp()).add(partitions.getPartitionColNames());
                exprs("partitionvalues", partitions.getPartitionColValues());
            }
            add(node.getTabletIds()).add(node.getReplicaIds()).add(node.getQueryPeriodString())
                    .add(node.getTvrVersionRange()).add(node.getGtid());
        }

        // ------------------------------------------- Expressions -------------------------------------------

        private void exprs(String name, List<? extends Expr> exprs) {
            add(name);
            if (exprs == null) {
                add("none");
                return;
            }
            add(exprs.size());
            exprs.forEach(this::expr);
        }

        private void orderBy(List<OrderByElement> elements) {
            add("orderby");
            if (elements == null) {
                add("none");
                return;
            }
            add(elements.size());
            for (OrderByElement element : elements) {
                add(element.getIsAsc()).add(element.getNullsFirstParam());
                expr(element.getExpr());
            }
        }

        // Finds the field that the analyzer resolved the slot to: the innermost scope with a field of the relation
        // and the name recorded on the slot. A slot that does not resolve this way rejects the body.
        private void slot(SlotRef slot) {
            int up = 0;
            for (Scope level = scope; level != null; level = level.getParent(), up++) {
                List<Field> fields = level.getRelationFields().getAllFields();
                int match = -1;
                for (int i = 0; i < fields.size(); i++) {
                    Field field = fields.get(i);
                    if (Objects.equals(field.getRelationAlias(), slot.getTblNameWithoutAnalyzed())
                            && field.getName() != null && field.getName().equalsIgnoreCase(slot.getColumnName())) {
                        if (match >= 0) {
                            reject();
                            return;
                        }
                        match = i;
                    }
                }
                if (match >= 0) {
                    column(level, up, match, slot.getType(), slot.getUsedStructFieldPos());
                    return;
                }
            }
            reject();
        }

        // A column is the scope level and position of its field, the table that the field comes from, and its name
        private void column(Scope level, int up, int index, Object type, List<Integer> structPath) {
            Field field = level.getRelationFields().getAllFields().get(index);
            List<String> levelOrigins = origins.get(level);
            add("column").add(up).add(index).add(levelOrigins == null ? "none" : levelOrigins.get(index))
                    .add(field.getName().toLowerCase()).add(type).add(structPath == null ? List.of() : structPath);
        }

        private void expr(Expr expr) {
            if (rejected) {
                return;
            }
            if (expr == null) {
                add("null");
                return;
            }
            Class<?> type = expr.getClass();
            if (CollectionUtils.isNotEmpty(expr.getHints())) {
                reject();
                return;
            }
            if (type == SlotRef.class) {
                // After analysis the table name of a slot is the alias of the relation that it resolved to
                slot((SlotRef) expr);
                return;
            }
            add(type.getSimpleName()).add(expr.getType()).add(expr.getIgnoreNulls());
            if (type == FieldReference.class) {
                FieldReference reference = (FieldReference) expr;
                add(reference.getFieldIndex()).add(reference.getTblName());
                return;
            }
            if (literal(expr)) {
                return;
            }
            if (type == FunctionCallExpr.class) {
                function((FunctionCallExpr) expr);
            } else if (type == AnalyticExpr.class) {
                analytic((AnalyticExpr) expr);
                return;
            } else if (type == CastExpr.class) {
                add(((CastExpr) expr).isImplicit());
            } else if (type == ArithmeticExpr.class) {
                add(((ArithmeticExpr) expr).getOp());
            } else if (type == BinaryPredicate.class) {
                add(((BinaryPredicate) expr).getOp());
            } else if (type == CompoundPredicate.class) {
                add(((CompoundPredicate) expr).getOp());
            } else if (type == InPredicate.class) {
                add(((InPredicate) expr).isNotIn());
            } else if (type == IsNullPredicate.class) {
                add(((IsNullPredicate) expr).isNotNull());
            } else if (type == LikePredicate.class) {
                add(((LikePredicate) expr).getOp());
            } else if (type == BetweenPredicate.class) {
                add(((BetweenPredicate) expr).isNotBetween());
            } else if (type == CaseExpr.class) {
                add(((CaseExpr) expr).hasCaseExpr()).add(((CaseExpr) expr).hasElseExpr());
            } else if (type == TimestampArithmeticExpr.class) {
                TimestampArithmeticExpr timestamp = (TimestampArithmeticExpr) expr;
                add(timestamp.getFuncName()).add(timestamp.getOp()).add(timestamp.getTimeUnitIdent())
                        .add(timestamp.isIntervalFirst());
            } else if (type == ExistsPredicate.class) {
                add(((ExistsPredicate) expr).isNotExists());
            } else if (type == Subquery.class) {
                nested(((Subquery) expr).getQueryStatement());
            } else if (type == InformationFunction.class) {
                InformationFunction function = (InformationFunction) expr;
                add(function.getFuncType()).add(function.getStrValue()).add(function.getIntValue());
            } else if (type == VariableExpr.class) {
                VariableExpr variable = (VariableExpr) expr;
                add(variable.getSetType()).add(variable.getName()).add(variable.getValue());
            } else if (type == UserVariableExpr.class) {
                UserVariableExpr variable = (UserVariableExpr) expr;
                add(variable.getName());
                expr(variable.getValue());
            } else if (type == SubfieldExpr.class) {
                add(((SubfieldExpr) expr).getFieldNames()).add(((SubfieldExpr) expr).isCopyFlag());
            } else if (type == CollectionElementExpr.class) {
                add(((CollectionElementExpr) expr).isCheckIsOutOfBounds());
            } else if (type == NamedArgument.class) {
                add(((NamedArgument) expr).getName());
            } else if (type != ArrayExpr.class && type != MapExpr.class && type != ArrowExpr.class) {
                // lambdas, dictionaries, prepared statement parameters and every other expression
                reject();
                return;
            }
            exprs("children", expr.getChildren());
        }

        private boolean literal(Expr expr) {
            Class<?> type = expr.getClass();
            if (type == IntLiteral.class) {
                add(((IntLiteral) expr).getValue());
            } else if (type == LargeIntLiteral.class) {
                add(((LargeIntLiteral) expr).getValue());
            } else if (type == FloatLiteral.class) {
                add(Double.toString(((FloatLiteral) expr).getValue()));
            } else if (type == DecimalLiteral.class) {
                add(((DecimalLiteral) expr).getValue());
            } else if (type == StringLiteral.class || type == LargeStringLiteral.class) {
                add(((StringLiteral) expr).getValue());
            } else if (type == DateLiteral.class) {
                add(((DateLiteral) expr).getStringValue());
            } else if (type == BoolLiteral.class) {
                add(((BoolLiteral) expr).getValue());
            } else if (type == NullLiteral.class) {
                add("NULL");
            } else if (type == VarBinaryLiteral.class) {
                add(HexFormat.of().formatHex(((VarBinaryLiteral) expr).getValue()));
            } else {
                return false;
            }
            return true;
        }

        private void function(FunctionCallExpr call) {
            if (!isStableFunction(call)) {
                reject();
                return;
            }
            Function fn = call.getFn();
            add(fn.signatureString()).add(fn.getReturnType()).add(fn.getBinaryType())
                    .add(call.isMergeAggFn()).add(call.isAnalyticFnCall());
            FunctionParams params = call.getParams();
            if (params == null) {
                add("noparams");
            } else {
                add(params.isStar()).add(params.isDistinct()).add(params.getExprsNames());
                orderBy(params.getOrderByElements());
            }
        }

        private void analytic(AnalyticExpr analytic) {
            if (analytic.getPartitionHint() != null || analytic.getSkewHint() != null) {
                reject();
                return;
            }
            add(analytic.isUseHashBasedPartition()).add(analytic.isSkewed());
            expr(analytic.getFnCall());
            exprs("partition", analytic.getPartitionExprs());
            orderBy(analytic.getOrderByElements());
            AnalyticWindow window = analytic.getWindow();
            if (window == null) {
                add("nowindow");
                return;
            }
            add(window.getType());
            boundary(window.getLeftBoundary());
            boundary(window.getRightBoundary());
        }

        private void boundary(AnalyticWindowBoundary boundary) {
            if (boundary == null) {
                add("noboundary");
                return;
            }
            add(boundary.getBoundaryType());
            expr(boundary.getExpr());
        }
    }

    // Returns the version range that RelationTransformer.visitTable reads, with the same call
    private static String externalTableVersion(TableRelation node, Table table) {
        if (node.getTvrVersionRange() != null) {
            return node.getTvrVersionRange().toString();
        }
        if (node.getQueryPeriod() != null) {
            // resolved at transform time from the clause, which is part of the key already
            return "period";
        }
        TvrVersionRange range = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTableVersionRange(node.getName().getDb(), table, Optional.empty(), Optional.empty());
        return String.valueOf(range);
    }

    private static boolean isSnapshotTable(Table table) {
        return table.isNativeTableOrMaterializedView() || table.isHiveTable() || table.isIcebergTable()
                || table.isHudiTable() || table.isDeltalakeTable() || table.isPaimonTable();
    }

    // Returns true when two calls with the same arguments in one query return the same value
    private static boolean isStableFunction(FunctionCallExpr call) {
        Function fn = call.getFn();
        if (fn == null || fn.isUdf()) {
            return false;
        }
        // A Java or Python UDF has its own binary type. Some functions that the analyzer builds itself have none.
        if (fn.getBinaryType() != null && fn.getBinaryType() != TFunctionBinaryType.BUILTIN) {
            return false;
        }
        String name = fn.functionName().toLowerCase();
        if (FunctionSet.nonDeterministicFunctions.contains(name)
                || FunctionSet.nonDeterministicFunctions.contains(call.getFunctionName().toLowerCase())) {
            return false;
        }
        if (FunctionSet.nonDeterministicTimeFunctions.contains(name)) {
            // unix_timestamp(x) converts its argument. The other calls read the query start time, and the
            // optimizer folds them to one constant for the whole query when all arguments are constants.
            if (name.equals(FunctionSet.UNIX_TIMESTAMP) && !call.getChildren().isEmpty()) {
                return true;
            }
            return call.getChildren().stream().allMatch(ExprUtils::isLiteral);
        }
        return true;
    }
}
