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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Provenance of a supported relational subgraph. Unsupported operators explicitly end the scope. */
public final class JoinStatisticsScope {
    public record ColumnOrigin(String tableUuid, String name, Type type) {
    }

    public record Source(String tableUuid, double estimatedRows, List<ScalarOperator> predicates,
                         String physicalUuid, JoinStatisticsTableState tableState) {
        public Source(String tableUuid, double estimatedRows, List<ScalarOperator> predicates) {
            this(tableUuid, estimatedRows, predicates, tableUuid, JoinStatisticsTableState.UNKNOWN);
        }

        public Source(String tableUuid, double estimatedRows, List<ScalarOperator> predicates, String physicalUuid) {
            this(tableUuid, estimatedRows, predicates, physicalUuid, JoinStatisticsTableState.UNKNOWN);
        }

        public Source {
            predicates = List.copyOf(predicates);
        }
    }

    public record Equality(ColumnOrigin left, ColumnOrigin right) {
    }

    private final Map<String, Source> sources;
    private final Map<ColumnRefOperator, ColumnOrigin> columns;
    private final Set<Equality> equalities;
    private final Set<String> outputs;
    private final Map<String, Set<ColumnOrigin>> groupedKeys;

    private JoinStatisticsScope(Map<String, Source> sources, Map<ColumnRefOperator, ColumnOrigin> columns,
                                Set<Equality> equalities, Set<String> outputs) {
        this(sources, columns, equalities, outputs, Map.of());
    }

    private JoinStatisticsScope(Map<String, Source> sources, Map<ColumnRefOperator, ColumnOrigin> columns,
                                Set<Equality> equalities, Set<String> outputs,
                                Map<String, Set<ColumnOrigin>> groupedKeys) {
        this.groupedKeys = Map.copyOf(groupedKeys);
        this.sources = Map.copyOf(sources);
        this.columns = Map.copyOf(columns);
        this.equalities = Set.copyOf(equalities);
        this.outputs = Set.copyOf(outputs);
    }

    public Map<String, Source> getSources() {
        return sources;
    }

    public Map<ColumnRefOperator, ColumnOrigin> getColumns() {
        return columns;
    }

    public Set<Equality> getEqualities() {
        return equalities;
    }

    public Set<String> getOutputs() {
        return outputs;
    }

    JoinStatisticsScope restrict(Set<String> included) {
        Map<String, Source> selected = new HashMap<>();
        sources.forEach((id, source) -> {
            if (included.contains(id)) {
                selected.put(id, source);
            }
        });
        Set<Equality> edges = new HashSet<>();
        for (Equality equality : equalities) {
            if (selected.containsKey(equality.left.tableUuid) && selected.containsKey(equality.right.tableUuid)) {
                edges.add(equality);
            }
        }
        Set<String> visible = new HashSet<>(outputs);
        visible.retainAll(selected.keySet());
        Map<String, Set<ColumnOrigin>> grouped = new HashMap<>(groupedKeys);
        grouped.keySet().retainAll(included);
        return new JoinStatisticsScope(selected, columns, edges, visible, grouped);
    }

    static boolean isFullSnapshot(TvrVersionRange range) {
        // Iceberg represents explicit FOR VERSION AS OF as Delta@[MIN,snapshot]. It reads the
        // entire snapshot; only a bounded interval has incremental semantics.
        return range instanceof TvrTableSnapshot
                || (range instanceof TvrTableDelta && range.from.isMin() && !range.to.isMin());
    }

    public static JoinStatisticsScope derive(ExpressionContext context) {
        return derive(context, true);
    }

    static JoinStatisticsScope derive(ExpressionContext context, boolean includePostProcessing) {
        Operator operator = context.getOp();
        if (operator.getLimit() != Operator.DEFAULT_LIMIT) {
            return null;
        }
        JoinStatisticsScope result;
        if (operator instanceof LogicalOlapScanOperator scan) {
            if (scan.getSample() != null || scan.getPartitionNames() != null || !scan.getHintsTabletIds().isEmpty()
                    || scan.getGtid() != 0
                    || scan.getSelectedIndexMetaId() != ((OlapTable) scan.getTable()).getBaseIndexMetaId()) {
                return null;
            }
            result = scan(scan.getTable(), scan.getColRefToColumnMetaMap(), context.getStatistics().getOutputRowCount());
            result = result.filter(Utils.compoundAnd(scan.getPrunedPartitionPredicates()),
                    context.getStatistics().getOutputRowCount());
        } else if (operator instanceof PhysicalOlapScanOperator scan) {
            if (scan.getSample() != null || scan.getGtid() != 0
                    || scan.getSelectedIndexMetaId() != ((OlapTable) scan.getTable()).getBaseIndexMetaId()) {
                return null;
            }
            // Preserve the logical scan's guard for explicit partition/tablet restrictions.
            result = context.getStatistics().getJoinStatisticsScope();
            if (result == null) {
                return null;
            }
        } else if (operator instanceof LogicalIcebergScanOperator scan) {
            if (!isFullSnapshot(scan.getTvrVersionRange())
                    || !IcebergMORParams.EMPTY.equals(scan.getMORParam())) {
                return null;
            }
            result = scan(scan.getTable(), scan.getColRefToColumnMetaMap(), context.getStatistics().getOutputRowCount(),
                    JoinStatisticsTableState.read(scan.getTable(), scan.getTvrVersionRange()));
        } else if (operator instanceof PhysicalIcebergScanOperator scan) {
            if (!isFullSnapshot(scan.getTvrVersionRange())
                    || !IcebergMORParams.EMPTY.equals(scan.getMORParams())) {
                return null;
            }
            result = scan(scan.getTable(), scan.getColRefToColumnMetaMap(), context.getStatistics().getOutputRowCount(),
                    JoinStatisticsTableState.read(scan.getTable(), scan.getTvrVersionRange()));
        } else if (operator instanceof LogicalUnionOperator union && union.isUnionAll()
                && union.isFromIcebergEqualityDeleteRewrite()) {
            result = equalityDeleteUnion(context, union.getOutputColumnRefOp(), union.getChildOutputColumns());
        } else if (operator instanceof PhysicalUnionOperator union && union.isUnionAll()
                && union.isFromIcebergEqualityDeleteRewrite()) {
            result = equalityDeleteUnion(context, union.getOutputColumnRefOp(), union.getChildOutputColumns());
        } else if (operator instanceof LogicalAggregationOperator aggregate) {
            result = context.getChildStatistics(0).getJoinStatisticsScope();
            if (result != null && aggregate.getType().isGlobal()) {
                result = result.groupBy(aggregate.getGroupingKeys());
            }
        } else if (operator instanceof PhysicalHashAggregateOperator aggregate) {
            result = context.getChildStatistics(0).getJoinStatisticsScope();
            if (result != null && aggregate.getType().isGlobal()) {
                result = result.groupBy(aggregate.getGroupBys());
            }
        } else if (operator instanceof LogicalJoinOperator join) {
            result = deriveJoin(context, join.getJoinType(), join.getOnPredicate());
        } else if (operator instanceof PhysicalJoinOperator join) {
            result = deriveJoin(context, join.getJoinType(), join.getOnPredicate());
        } else if (operator instanceof LogicalFilterOperator || operator instanceof PhysicalFilterOperator
                || operator instanceof LogicalProjectOperator || operator instanceof PhysicalProjectOperator
                || operator instanceof PhysicalDistributionOperator) {
            result = context.getChildStatistics(0).getJoinStatisticsScope();
        } else {
            return null;
        }
        if (result == null || !includePostProcessing) {
            return result;
        }
        result = result.filter(operator.getPredicate(),
                context.getStatistics() == null ? Double.POSITIVE_INFINITY : context.getStatistics().getOutputRowCount());
        if (result == null) {
            return null;
        }
        if (operator instanceof LogicalProjectOperator project) {
            result = result.project(project.getColumnRefMap());
        } else if (operator instanceof PhysicalProjectOperator project) {
            result = result.project(project.getColumnRefMap());
        }
        Projection projection = operator.getProjection();
        return projection == null ? result : result.project(projection.getColumnRefMap());
    }

    private static JoinStatisticsScope deriveJoin(ExpressionContext context, JoinOperator kind, ScalarOperator on) {
        Statistics left = context.getChildStatistics(0);
        Statistics right = context.getChildStatistics(1);
        if (kind == JoinOperator.LEFT_OUTER_JOIN && left.getJoinStatisticsPlanner() != null
                && left.getJoinStatisticsPlanner().preservesOuterRows(
                        left.getJoinStatisticsScope(), right.getJoinStatisticsScope(), on)) {
            // Only retained-side origins survive. A WHERE referring to the optional side ends
            // this scope rather than being attached to the unfiltered preserved source.
            return left.getJoinStatisticsScope();
        }
        if (kind == JoinOperator.RIGHT_OUTER_JOIN && right.getJoinStatisticsPlanner() != null
                && right.getJoinStatisticsPlanner().preservesOuterRows(
                        right.getJoinStatisticsScope(), left.getJoinStatisticsScope(), on)) {
            return right.getJoinStatisticsScope();
        }
        return join(left.getJoinStatisticsScope(), right.getJoinStatisticsScope(), kind, on);
    }

    /**
     * Only the complete union emitted by IcebergEqualityDeleteRewriteRule represents the original table.
     * Its first branch retains the original table identity and the predicates (including later pushdown).
     * Read that provenance without attaching full-table statistics to either partial data-file scan.
     * Aggregation, limits or any other subsequent change of the branch's meaning end this scope.
     */
    private static JoinStatisticsScope equalityDeleteUnion(ExpressionContext context, List<ColumnRefOperator> outputs,
                                                           List<List<ColumnRefOperator>> children) {
        if (context.arity() != 2 || children.size() != 2 || children.get(0).size() != outputs.size()) {
            return null;
        }
        JoinStatisticsScope source = equalityDeleteSource(childContext(context, 0),
                context.getStatistics().getOutputRowCount(), 0);
        if (source == null) {
            return null;
        }
        Map<ColumnRefOperator, ScalarOperator> projection = new HashMap<>();
        for (int i = 0; i < outputs.size(); i++) {
            projection.put(outputs.get(i), children.get(0).get(i));
        }
        return source.project(projection);
    }

    private static ExpressionContext childContext(ExpressionContext context, int index) {
        return context.getOptExpression() != null
                ? new ExpressionContext(context.getOptExpression().inputAt(index))
                : new ExpressionContext(context.getGroupExpression().inputAt(index).getFirstLogicalExpression());
    }

    private static JoinStatisticsScope equalityDeleteSource(ExpressionContext context, double rows, int depth) {
        Operator operator = context.getOp();
        if (depth > 16 || operator.hasLimit()) {
            return null;
        }
        JoinStatisticsScope result;
        if (operator instanceof LogicalIcebergScanOperator scan
                && isFullSnapshot(scan.getTvrVersionRange())
                && IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE.equals(scan.getMORParam())) {
            result = scan(scan.getTable(), scan.getColRefToColumnMetaMap(), rows,
                    JoinStatisticsTableState.read(scan.getTable(), scan.getTvrVersionRange()));
        } else if (operator instanceof PhysicalIcebergScanOperator scan
                && isFullSnapshot(scan.getTvrVersionRange())
                && IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE.equals(scan.getMORParams())) {
            result = scan(scan.getTable(), scan.getColRefToColumnMetaMap(), rows,
                    JoinStatisticsTableState.read(scan.getTable(), scan.getTvrVersionRange()));
        } else if (context.arity() == 1 && (operator instanceof LogicalFilterOperator
                || operator instanceof PhysicalFilterOperator || operator instanceof LogicalProjectOperator
                || operator instanceof PhysicalProjectOperator || operator instanceof PhysicalDistributionOperator)) {
            result = equalityDeleteSource(childContext(context, 0), rows, depth + 1);
        } else {
            return null;
        }
        if (result == null) {
            return null;
        }
        result = result.filter(operator.getPredicate(), rows);
        if (result == null) {
            return null;
        }
        if (operator instanceof LogicalProjectOperator project) {
            result = result.project(project.getColumnRefMap());
        } else if (operator instanceof PhysicalProjectOperator project) {
            result = result.project(project.getColumnRefMap());
        }
        return operator.getProjection() == null ? result : result.project(operator.getProjection().getColumnRefMap());
    }

    static JoinStatisticsScope scan(Table table, Map<ColumnRefOperator, Column> references, double rows) {
        return scan(table, references, rows, JoinStatisticsTableState.read(table, null));
    }

    static JoinStatisticsScope scan(Table table, Map<ColumnRefOperator, Column> references, double rows,
                                    JoinStatisticsTableState state) {
        String uuid = table.getUUID();
        Map<ColumnRefOperator, ColumnOrigin> columns = new HashMap<>();
        references.forEach((ref, column) -> columns.put(ref, new ColumnOrigin(uuid, column.getName(), column.getType())));
        return new JoinStatisticsScope(Map.of(uuid, new Source(uuid, rows, List.of(), uuid, state)),
                columns, Set.of(), Set.of(uuid));
    }

    static JoinStatisticsScope join(JoinStatisticsScope left, JoinStatisticsScope right, JoinOperator kind,
                                    ScalarOperator condition) {
        if (left == null || right == null || !(kind.isInnerJoin() || kind.isSemiJoin())
                || left.sources.size() + right.sources.size() > JoinStatisticsEntropyModel.MAX_ATTRIBUTES - 1) {
            return null;
        }
        Map<String, String> aliases = new HashMap<>();
        for (String source : right.sources.keySet()) {
            if (left.sources.containsKey(source)) {
                int identity = right.columns.entrySet().stream().filter(e -> e.getValue().tableUuid.equals(source))
                        .mapToInt(e -> e.getKey().getId()).min().orElse(-1);
                if (identity < 0) {
                    return null;
                }
                String role = "role:" + identity + ":" + right.sources.get(source).physicalUuid;
                if (left.sources.containsKey(role) || right.sources.containsKey(role)) {
                    return null;
                }
                aliases.put(source, role);
            }
        }
        if (!aliases.isEmpty()) {
            right = right.rename(aliases);
        }
        Map<String, Source> sources = new HashMap<>(left.sources);
        sources.putAll(right.sources);
        Map<ColumnRefOperator, ColumnOrigin> columns = new HashMap<>(left.columns);
        columns.putAll(right.columns);
        Set<Equality> equalities = new HashSet<>(left.equalities);
        equalities.addAll(right.equalities);
        for (ScalarOperator conjunct : Utils.extractConjuncts(condition)) {
            if (!(conjunct instanceof BinaryPredicateOperator binary) || binary.getBinaryType() != BinaryType.EQ) {
                return null;
            }
            ColumnOrigin a = columns.get(sourceColumn(binary.getChild(0)));
            ColumnOrigin b = columns.get(sourceColumn(binary.getChild(1)));
            if (a == null || b == null || a.tableUuid.equals(b.tableUuid)) {
                return null;
            }
            equalities.add(canonical(a, b));
        }
        Map<String, Set<ColumnOrigin>> grouped = new HashMap<>(left.groupedKeys);
        grouped.putAll(right.groupedKeys);
        for (var entry : grouped.entrySet()) {
            Set<ColumnOrigin> connected = new HashSet<>();
            for (Equality edge : equalities) {
                if (edge.left.tableUuid.equals(entry.getKey())) {
                    connected.add(edge.left);
                }
                if (edge.right.tableUuid.equals(entry.getKey())) {
                    connected.add(edge.right);
                }
            }
            if (!connected.containsAll(entry.getValue())) {
                return null; // GROUP BY (x,y) is not unique when the subsequent JOIN only uses x.
            }
        }
        Set<String> outputs = new HashSet<>();
        if (kind != JoinOperator.RIGHT_SEMI_JOIN) {
            outputs.addAll(left.outputs);
        }
        if (kind != JoinOperator.LEFT_SEMI_JOIN) {
            outputs.addAll(right.outputs);
        }
        return outputs.isEmpty() ? null : new JoinStatisticsScope(sources, columns, equalities, outputs, grouped);
    }

    private JoinStatisticsScope rename(Map<String, String> aliases) {
        java.util.function.Function<ColumnOrigin, ColumnOrigin> rename = origin -> new ColumnOrigin(
                aliases.getOrDefault(origin.tableUuid, origin.tableUuid), origin.name, origin.type);
        Map<String, Source> relations = new HashMap<>();
        sources.forEach((id, source) -> {
            String role = aliases.getOrDefault(id, id);
            relations.put(role, new Source(role, source.estimatedRows, source.predicates,
                    source.physicalUuid, source.tableState));
        });
        Map<ColumnRefOperator, ColumnOrigin> origins = new HashMap<>();
        columns.forEach((column, origin) -> origins.put(column, rename.apply(origin)));
        Set<Equality> edges = new HashSet<>();
        equalities.forEach(edge -> edges.add(canonical(rename.apply(edge.left), rename.apply(edge.right))));
        Set<String> visible = new HashSet<>();
        outputs.forEach(id -> visible.add(aliases.getOrDefault(id, id)));
        Map<String, Set<ColumnOrigin>> grouped = new HashMap<>();
        groupedKeys.forEach((id, keys) -> grouped.put(aliases.getOrDefault(id, id),
                keys.stream().map(rename).collect(java.util.stream.Collectors.toUnmodifiableSet())));
        return new JoinStatisticsScope(relations, origins, edges, visible, grouped);
    }

    JoinStatisticsScope groupBy(List<ColumnRefOperator> keys) {
        if (sources.size() != 1 || keys.isEmpty()) {
            return null;
        }
        String source = sources.keySet().iterator().next();
        Set<ColumnOrigin> grouping = new HashSet<>();
        Map<ColumnRefOperator, ColumnOrigin> origins = new HashMap<>();
        for (ColumnRefOperator key : keys) {
            ColumnOrigin origin = columns.get(key);
            if (origin == null || !origin.tableUuid.equals(source)) {
                return null;
            }
            grouping.add(origin);
            origins.put(key, origin);
        }
        // At a subsequent equality JOIN the grouped relation contributes key membership,
        // not original-row multiplicity. Never attach origins to aggregate result columns.
        return new JoinStatisticsScope(sources, origins, equalities, Set.of(), Map.of(source, Set.copyOf(grouping)));
    }

    public JoinStatisticsScope filter(ScalarOperator predicate, double rows) {
        if (predicate == null) {
            return this;
        }
        Map<String, Source> filtered = new HashMap<>(sources);
        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            List<ColumnRefOperator> refs = Utils.extractColumnRef(conjunct);
            if (refs.isEmpty()) {
                return null;
            }
            String table = null;
            for (ColumnRefOperator ref : refs) {
                ColumnOrigin column = columns.get(ref);
                if (column == null || (table != null && !table.equals(column.tableUuid))) {
                    return null;
                }
                table = column.tableUuid;
            }
            Source source = filtered.get(table);
            List<ScalarOperator> predicates = new ArrayList<>(source.predicates);
            if (!predicates.contains(conjunct)) {
                predicates.add(conjunct.clone());
            }
            // An output cardinality after JOIN is not a cardinality of a participating base relation.
            double estimate = sources.size() == 1 && groupedKeys.isEmpty() ? rows : source.estimatedRows;
            filtered.put(table, new Source(table, estimate, predicates, source.physicalUuid, source.tableState));
        }
        return new JoinStatisticsScope(filtered, columns, equalities, outputs, groupedKeys);
    }

    JoinStatisticsScope project(Map<ColumnRefOperator, ScalarOperator> projection) {
        Map<ColumnRefOperator, ColumnOrigin> projected = new HashMap<>(columns);
        projection.forEach((output, expression) -> {
            ColumnRefOperator input = sourceColumn(expression);
            ColumnOrigin original = input == null ? null : columns.get(input);
            if (original != null) {
                projected.put(output, original);
            } else {
                projected.remove(output);
            }
        });
        return new JoinStatisticsScope(sources, projected, equalities, outputs, groupedKeys);
    }

    public static ColumnRefOperator sourceColumn(ScalarOperator expression) {
        if (expression instanceof ColumnRefOperator column) {
            return column;
        }
        if (expression instanceof CastOperator cast && cast.getChild(0).getType().isIntegerType()
                && cast.getType().isIntegerType() && cast.getType().getTypeSize() >= cast.getChild(0).getType().getTypeSize()) {
            return sourceColumn(cast.getChild(0));
        }
        return null;
    }

    private static Equality canonical(ColumnOrigin a, ColumnOrigin b) {
        int compare = a.tableUuid.compareTo(b.tableUuid);
        if (compare == 0) {
            compare = a.name.compareTo(b.name);
        }
        return compare <= 0 ? new Equality(a, b) : new Equality(b, a);
    }
}
