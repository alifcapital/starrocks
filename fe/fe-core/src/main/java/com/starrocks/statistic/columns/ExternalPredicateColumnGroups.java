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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.CatalogMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSetOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.lang.ref.WeakReference;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Bounded recent observations. Recording never performs SQL or counts optimizer derivations as queries. */
public class ExternalPredicateColumnGroups {
    // History is advisory and bounded by estimated bytes, including wide sets needed by basic stats.
    // An observation can be evicted before the next flush under excessive workload diversity.
    static final int MAX_HISTORY_BYTES = 16 * 1024 * 1024;
    static final int MAX_GROUPS = 10000;
    private final Cache<String, ExternalColumnGroupUsage> recent = Caffeine.newBuilder()
            .maximumWeight(MAX_HISTORY_BYTES)
            .weigher((String key, ExternalColumnGroupUsage group) -> group.estimatedMemoryBytes()).build();

    // The optimizer derives the statistics of a query again for every plan alternative, so it records the same
    // groups of one query many times. Each record hashes the table UUID, builds the group key and merges into the
    // shared cache. The record of a group says only that a query used it, so we record each group once per query.
    // A query is identified by its column ref factory, which the thread holds weakly. A group is identified by
    // the fields of its cache key: the table UUID, the use case and the columns.
    private final ThreadLocal<QueryGroups> queryGroups = new ThreadLocal<>();

    private static final class QueryGroups {
        private final WeakReference<ColumnRefFactory> query;
        private final Set<GroupKey> recorded = new HashSet<>();

        private QueryGroups(ColumnRefFactory query) {
            this.query = new WeakReference<>(query);
        }
    }

    private record GroupKey(String tableUuid, ColumnUsage.UseCase useCase, List<String> columns) { }

    private record Source(Table table, int relation, String column) { }
    private record Relation(String uuid, int relation) { }
    private record JoinPair(Relation left, Relation right) { }

    public void record(List<ColumnRefOperator> refs, ColumnUsage.UseCase useCase,
                       ColumnRefFactory factory, OptExpression expression) {
        if (!enabled()) {
            return;
        }
        List<Source> sources = new ArrayList<>();
        for (ColumnRefOperator ref : refs) {
            if (!resolve(ref, factory, expression, new HashSet<>(), sources)) {
                // Do not advertise a partial group when an expression's lineage is ambiguous.
                return;
            }
        }
        recordSources(sources, useCase, factory);
    }

    public void recordJoin(List<BinaryPredicateOperator> predicates, ColumnRefFactory factory, OptExpression expression) {
        if (!enabled()) {
            return;
        }
        Map<JoinPair, List<Source>> joins = new HashMap<>();
        for (BinaryPredicateOperator predicate : predicates) {
            List<Source> left = resolveExpression(predicate.getChild(0), factory, expression);
            List<Source> right = resolveExpression(predicate.getChild(1), factory, expression);
            if (left.isEmpty() || right.isEmpty() || relations(left).size() != 1 || relations(right).size() != 1) {
                continue;
            }
            Relation a = relation(left.get(0));
            Relation b = relation(right.get(0));
            if (a.equals(b)) {
                continue;
            }
            // Equality operands can be reversed independently for each component.
            JoinPair pair = a.relation < b.relation ? new JoinPair(a, b) : new JoinPair(b, a);
            List<Source> group = joins.computeIfAbsent(pair, ignored -> new ArrayList<>());
            group.addAll(left);
            group.addAll(right);
        }
        joins.values().forEach(sources -> recordSources(sources, ColumnUsage.UseCase.JOIN, factory));
    }

    private List<Source> resolveExpression(ScalarOperator scalar, ColumnRefFactory factory, OptExpression expression) {
        List<Source> sources = new ArrayList<>();
        for (ColumnRefOperator ref : Utils.extractColumnRef(scalar)) {
            if (!resolve(ref, factory, expression, new HashSet<>(), sources)) {
                return List.of();
            }
        }
        return sources;
    }

    private static Set<Relation> relations(List<Source> sources) {
        Set<Relation> result = new HashSet<>();
        sources.forEach(source -> result.add(relation(source)));
        return result;
    }

    private static Relation relation(Source source) {
        return new Relation(source.table.getUUID(), source.relation);
    }

    private void recordSources(List<Source> sources, ColumnUsage.UseCase useCase, ColumnRefFactory factory) {
        Map<Relation, List<Source>> groups = new HashMap<>();
        sources.forEach(source -> groups.computeIfAbsent(relation(source), ignored -> new ArrayList<>()).add(source));
        LocalDateTime now = null;
        for (List<Source> group : groups.values()) {
            Table table = group.get(0).table;
            if (!CatalogMgr.isExternalCatalog(table.getCatalogName()) || table.isTemporaryTable()) {
                continue;
            }
            List<String> names = group.stream().map(Source::column).distinct().sorted().toList();
            if (!firstInQuery(factory, table, useCase, names)) {
                continue;
            }
            if (now == null) {
                now = TimeUtils.getSystemNow();
            }
            recordColumns(table, names, useCase, now);
        }
    }

    // False when this query already recorded the group.
    private boolean firstInQuery(ColumnRefFactory factory, Table table, ColumnUsage.UseCase useCase,
                                 List<String> columns) {
        if (factory == null) {
            return true;
        }
        QueryGroups groups = queryGroups.get();
        if (groups == null || groups.query.get() != factory) {
            groups = new QueryGroups(factory);
            queryGroups.set(groups);
        }
        return groups.recorded.add(new GroupKey(table.getUUID(), useCase, columns));
    }

    public void recordColumns(Table table, List<String> columns, ColumnUsage.UseCase useCase) {
        recordColumns(table, columns, useCase, TimeUtils.getSystemNow());
    }

    private void recordColumns(Table table, List<String> columns, ColumnUsage.UseCase useCase, LocalDateTime now) {
        // Metadata relations describe connector internals, not tables we can ANALYZE.
        // Their catalog database/table accessors need not be implemented.
        if (!enabled() || columns.isEmpty() || table.isMetadataTable()
                || !CatalogMgr.isExternalCatalog(table.getCatalogName())
                || table.isTemporaryTable()) {
            return;
        }
        ExternalColumnGroupUsage usage = ExternalColumnGroupUsage.of(table, columns, useCase, now);
        recent.asMap().merge(usage.key(), usage, ExternalColumnGroupUsage::newest);
    }

    private boolean resolve(ColumnRefOperator ref, ColumnRefFactory factory, OptExpression expression,
                            Set<ColumnRefOperator> visiting, List<Source> result) {
        if (!visiting.add(ref)) {
            return false;
        }
        try {
            var base = factory.getTableAndColumn(ref);
            int relation = factory.getRelationId(ref.getId());
            if (base != null && relation >= 0) {
                result.add(new Source(base.first, relation, base.second.getName()));
                return true;
            }
            return resolveInPlan(ref, factory, expression, visiting, result);
        } finally {
            visiting.remove(ref);
        }
    }

    private boolean resolveInPlan(ColumnRefOperator ref, ColumnRefFactory factory, OptExpression expression,
                                  Set<ColumnRefOperator> visiting, List<Source> result) {
        if (expression == null || expression.getOp() instanceof LogicalSetOperator) {
            // UNION outputs can describe several scan instances; do not merge their lineages.
            return false;
        }
        ScalarOperator definition = null;
        if (expression.getOp().getProjection() != null) {
            definition = expression.getOp().getProjection().resolveColumnRef(ref);
        }
        if (definition == null && expression.getOp() instanceof LogicalProjectOperator project) {
            definition = project.getColumnRefMap().get(ref);
        }
        if (definition != null && !definition.equals(ref)) {
            List<ColumnRefOperator> inputs = Utils.extractColumnRef(definition);
            if (inputs.isEmpty()) {
                return false;
            }
            for (ColumnRefOperator input : inputs) {
                if (!resolve(input, factory, expression, visiting, result)) {
                    return false;
                }
            }
            return true;
        }
        for (OptExpression child : expression.getInputs()) {
            List<Source> resolved = new ArrayList<>();
            if (resolveInPlan(ref, factory, child, visiting, resolved)) {
                result.addAll(resolved);
                return true;
            }
        }
        return false;
    }

    private static boolean enabled() {
        return Config.enable_predicate_columns_collection && Config.enable_external_predicate_columns_collection;
    }

    public List<ExternalColumnGroupUsage> snapshot() {
        LocalDateTime cutoff = Config.statistic_external_predicate_columns_ttl_hours < 0 ? LocalDateTime.MIN
                : TimeUtils.getSystemNow().minusHours(Config.statistic_external_predicate_columns_ttl_hours);
        recent.asMap().values().removeIf(group -> group.lastUsed().isBefore(cutoff));
        return List.copyOf(recent.asMap().values());
    }

    public void clear() {
        recent.invalidateAll();
    }
}
