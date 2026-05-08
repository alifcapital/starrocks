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

package com.starrocks.authorization;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.catalog.BasicTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.View;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.Pair;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.OptimizerOptions;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSetOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.MVTransformerContext;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.TransformerContext;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ColumnPrivilege {
    public static void check(ConnectContext context, QueryStatement stmt, List<TableName> excludeTables) {
        if (stmt == null) {
            return;
        }

        Map<TableName, Table> tableNameTableObj = Maps.newHashMap();
        Map<Table, TableName> tableObjectToTableName = Maps.newHashMap();
        new TableNameCollector(tableNameTableObj, tableObjectToTableName).visit(stmt);

        for (Map.Entry<TableName, Table> entry : tableNameTableObj.entrySet()) {
            TableName tableName = entry.getKey();
            Table table = entry.getValue();

            if (excludeTables.contains(tableName) || table instanceof MetadataTable) {
                continue;
            }

            if (table instanceof SystemTable && ((SystemTable) table).requireOperatePrivilege()) {
                try {
                    Authorizer.checkSystemAction(context, PrivilegeType.OPERATE);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(
                            InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
                }
            }
        }

        Set<TableName> tableUsedExternalAccessController = new HashSet<>();
        for (TableName tableName : tableNameTableObj.keySet()) {
            String catalog = tableName.getCatalog() == null ?
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
            if (Authorizer.getInstance().getAccessControlOrDefault(catalog) instanceof ExternalAccessController) {
                tableUsedExternalAccessController.add(tableName);
            }
        }

        Map<TableName, Map<String, EnumSet<ColumnAccessKind>>> scanColumns = new HashMap<>();
        OptExpression optimizedPlan;
        if (!tableUsedExternalAccessController.isEmpty()) {
            /*
             * The column access privilege of the query need to use the pruned column list.
             * Because the unused columns will not check the column access privilege.
             * For example, the table contains two columns v1 and v2, and user u1 only has
             * the access privilege to v1, the select v1 from (select * from tbl) t can be checked because v2 has been pruned.
             */
            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            LogicalPlan logicalPlan;
            MVTransformerContext mvTransformerContext = MVTransformerContext.of(context, true);
            TransformerContext transformerContext = new TransformerContext(columnRefFactory, context, mvTransformerContext);
            logicalPlan = new RelationTransformer(transformerContext).transformWithSelectLimit(stmt.getQueryRelation());

            OptimizerOptions optimizerOptions = new OptimizerOptions(OptimizerOptions.OptimizerStrategy.RULE_BASED);
            optimizerOptions.disableRule(RuleType.GP_SINGLE_TABLE_MV_REWRITE);
            optimizerOptions.disableRule(RuleType.GP_MULTI_TABLE_MV_REWRITE);
            optimizerOptions.disableRule(RuleType.GP_PRUNE_EMPTY_OPERATOR);
            Optimizer optimizer =
                    OptimizerFactory.create(OptimizerFactory.initContext(context, columnRefFactory, optimizerOptions));
            optimizedPlan = optimizer.optimize(logicalPlan.getRoot(),
                    new PhysicalPropertySet(), new ColumnRefSet(logicalPlan.getOutputColumn()));

            ColumnAccessCollector collector =
                    new ColumnAccessCollector(tableObjectToTableName, scanColumns);
            optimizedPlan.getOp().accept(collector, optimizedPlan, null);
            // The columns that flow out of the root of the plan are what the user actually sees in
            // the result rows — mark them as PROJECTION. Everything else stays AGG_ARG/JOIN_KEY/FILTER.
            // For UPDATE/DELETE the target table is in `excludeTables` and is therefore not audited,
            // so synthetic "all-columns-of-target" projections in the rewritten plan do not pollute
            // the audit. Read-side tables inside SET/WHERE subqueries still flow through the root
            // output and correctly pick up PROJECTION — matches "user de facto read this column".
            collector.markRootOutputProjection(logicalPlan.getOutputColumn());
        }

        for (Map.Entry<TableName, Table> entry : tableNameTableObj.entrySet()) {
            TableName tableName = entry.getKey();
            Table table = entry.getValue();

            if (excludeTables.contains(tableName) || table instanceof MetadataTable) {
                continue;
            }

            if (tableUsedExternalAccessController.contains(tableName)) {
                Map<String, EnumSet<ColumnAccessKind>> columns =
                        scanColumns.getOrDefault(tableName, new HashMap<>());
                for (Map.Entry<String, EnumSet<ColumnAccessKind>> columnEntry : columns.entrySet()) {
                    String column = columnEntry.getKey();
                    EnumSet<ColumnAccessKind> usage = columnEntry.getValue();
                    try {
                        Authorizer.checkColumnAction(context,
                                tableName, column, PrivilegeType.SELECT, usage);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                tableName.getCatalog(),
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.SELECT.name(), ObjectType.COLUMN.name(), column);
                    }
                }
            } else {
                if (table instanceof View) {
                    try {
                        // for privilege checking, treat connector view as table
                        if (table.isConnectorView()) {
                            Authorizer.checkTableAction(context,
                                    tableName, PrivilegeType.SELECT);
                        } else {
                            View view = (View) table;
                            if (view.isSecurity()) {
                                List<TableName> allTables = view.getTableRefs();
                                for (TableName t : allTables) {
                                    BasicTable basicTable = GlobalStateMgr.getCurrentState().getMetadataMgr()
                                            .getBasicTable(context, t.getCatalog(), t.getDb(), t.getTbl());
                                    if (basicTable.isOlapView()) {
                                        View subView = (View) basicTable;
                                        QueryStatement queryStatement = subView.getQueryStatement();
                                        Analyzer.analyze(queryStatement, context);
                                        Authorizer.check(queryStatement, context);
                                    } else if (basicTable.isMaterializedView()) {
                                        Authorizer.checkMaterializedViewAction(context, t, PrivilegeType.SELECT);
                                    } else {
                                        Authorizer.checkTableAction(context, t, PrivilegeType.SELECT);
                                    }
                                }
                            }

                            Authorizer.checkViewAction(context,
                                    tableName, PrivilegeType.SELECT);
                        }
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.SELECT.name(), ObjectType.VIEW.name(), tableName.getTbl());
                    }
                } else if (table.isMaterializedView()) {
                    try {
                        Authorizer.checkMaterializedViewAction(context,
                                tableName, PrivilegeType.SELECT);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.SELECT.name(), ObjectType.MATERIALIZED_VIEW.name(), tableName.getTbl());
                    }
                } else {
                    try {
                        Authorizer.checkTableAction(context,
                                tableName.getCatalog(), tableName.getDb(), table.getName(), PrivilegeType.SELECT);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                tableName.getCatalog(),
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.SELECT.name(), ObjectType.TABLE.name(), tableName.getTbl());
                    }
                }
            }
        }
    }

    /**
     * Walks the optimized logical plan post-order and classifies each base column reference by how
     * it is used: PROJECTION (visible in output), AGG_ARG (collapsed by an aggregate),
     * JOIN_KEY (in a join condition), FILTER (in a predicate, ORDER BY, or window partition).
     *
     * Column refs are tracked through Project / Aggregate / Window / Set / CTE-consume nodes so
     * that aliases and combined expressions resolve back to the originating (TableName, column).
     *
     * Scope: this classifier feeds {@link RangerStarRocksAccessController}. Catalogs served by a
     * different Ranger plugin (e.g. RangerHiveAccessController) only override the legacy
     * {@code checkColumnAction} and therefore see an empty {@code usage} set in their audit
     * payload — that's intentional, Hive Ranger has its own audit pipeline.
     */
    public static class ColumnAccessCollector extends OptExpressionVisitor<Void, Void> {
        // Aggregates whose result rows expose the RAW input column values to the user.
        // Collection-style (array_agg / group_concat / map_agg) literally enumerate values;
        // min/max/min_by/max_by return one input value verbatim. For PII auditing these must
        // contribute PROJECTION to their arg columns, not just AGG_ARG.
        private static final Set<String> VALUE_RETURNING_AGGREGATES = ImmutableSet.<String>builder()
                .add(FunctionSet.ARRAY_AGG)
                .add(FunctionSet.ARRAY_AGG_DISTINCT)
                .add(FunctionSet.ARRAY_UNIQUE_AGG)
                .add(FunctionSet.GROUP_CONCAT)
                .add(FunctionSet.MAP_AGG)
                .add(FunctionSet.MIN)
                .add(FunctionSet.MAX)
                .add(FunctionSet.MIN_BY)
                .add(FunctionSet.MAX_BY)
                .add(FunctionSet.ANY_VALUE)
                .add(FunctionSet.PERCENTILE_DISC)
                .add(FunctionSet.LC_PERCENTILE_DISC)
                .add(FunctionSet.APPROX_TOP_K)
                .add(FunctionSet.HISTOGRAM)
                .add(FunctionSet.SUM_MAP)
                .build();

        // Window functions whose output reveals raw arg values to the user. We deliberately do NOT
        // reuse FunctionSet.IGNORE_NULL_WINDOW_FUNCTION — that set has a different planner-level
        // purpose (which functions accept IGNORE NULLS) and changing it would have side-effects
        // outside auditing.
        private static final Set<String> VALUE_RETURNING_WINDOWS = ImmutableSet.<String>builder()
                .add(FunctionSet.FIRST_VALUE)
                .add(FunctionSet.FIRST_VALUE_REWRITE)
                .add(FunctionSet.LAST_VALUE)
                .add(FunctionSet.LEAD)
                .add(FunctionSet.LAG)
                .add(FunctionSet.APPROX_TOP_K)
                .build();

        // map_agg(key, value): both key and value reach the user. Other value-returning
        // aggregates (array_agg, min_by, max_by, percentile_disc, ...) treat ONLY the first
        // argument as visible — the rest are sort/percentile parameters.
        // E.g. `array_agg(v6 ORDER BY v5)` makes v6 visible, v5 is just an ordering key.
        private static final Set<String> TWO_ARG_VALUE_RETURNING_AGGREGATES = ImmutableSet.of(
                FunctionSet.MAP_AGG);

        // group_concat(value1, value2, ..., separator): every value expression AND the separator
        // expression appears verbatim in the output string. Treat every child as visible.
        private static final Set<String> ALL_ARGS_VALUE_RETURNING_AGGREGATES = ImmutableSet.of(
                FunctionSet.GROUP_CONCAT);

        private final Map<TableName, Map<String, EnumSet<ColumnAccessKind>>> scanColumns;
        private final Map<Table, TableName> tableObjToTableName;
        // ColumnRefOperator → set of base columns it ultimately resolves to. A single ref can
        // resolve to multiple base columns when the introducing expression mixes them, e.g.
        // `pii || other AS combined` makes `combined` map to {pii, other}.
        private final Map<ColumnRefOperator, Set<BaseColumn>> refToBases = new HashMap<>();

        public ColumnAccessCollector(
                Map<Table, TableName> tableObjToTableName,
                Map<TableName, Map<String, EnumSet<ColumnAccessKind>>> scanColumns) {
            this.scanColumns = scanColumns;
            this.tableObjToTableName = tableObjToTableName;
        }

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            // post-order: process children first so refToBases is populated bottom-up
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, null);
            }
            process(optExpression);
            return null;
        }

        @Override
        public Void visitLogicalTableScan(OptExpression node, Void context) {
            // Scans have no inputs; jump straight to processing.
            process(node);
            return null;
        }

        private void process(OptExpression opExpr) {
            Operator op = opExpr.getOp();

            // 1. extend the ref→base mapping with refs introduced by this operator
            if (op instanceof LogicalScanOperator) {
                seedScanRefs((LogicalScanOperator) op);
            } else if (op instanceof LogicalProjectOperator) {
                extendByRefMap(((LogicalProjectOperator) op).getColumnRefMap());
            } else if (op instanceof LogicalAggregationOperator) {
                // Most aggregates (count/sum/avg/...) collapse args into a derived scalar — the
                // user never sees the raw value, so we do NOT map output refs to arg bases.
                // Value-returning aggregates (array_agg, group_concat, min, max, ...) DO expose
                // the raw column values in the result, so we propagate bases there. See
                // VALUE_RETURNING_AGGREGATES below for the whitelist.
                LogicalAggregationOperator agg = (LogicalAggregationOperator) op;
                for (Map.Entry<ColumnRefOperator, CallOperator> e : agg.getAggregations().entrySet()) {
                    CallOperator call = e.getValue();
                    if (!isValueReturningAggregate(call)) {
                        continue;
                    }
                    addBases(e.getKey(), visibleAggregateBases(call));
                }
            } else if (op instanceof LogicalSetOperator) {
                LogicalSetOperator set = (LogicalSetOperator) op;
                List<ColumnRefOperator> outputs = set.getOutputColumnRefOp();
                List<List<ColumnRefOperator>> children = set.getChildOutputColumns();
                if (outputs != null && children != null) {
                    for (List<ColumnRefOperator> child : children) {
                        // Set-op branches must be positionally aligned with the output. A mismatch
                        // would silently drop columns from audit — fail loud instead.
                        Preconditions.checkState(child.size() == outputs.size(),
                                "Set operator branch arity mismatch: outputs=%s, branch=%s",
                                outputs.size(), child.size());
                    }
                    // EXCEPT: rows are returned only from the LEFT branch; the right branches are
                    // used solely for set-membership filtering. Treat right-branch refs as FILTER
                    // and only propagate bases from the left branch into the output.
                    // UNION/INTERSECT: every branch can contribute the visible value, so all
                    // branches contribute bases.
                    boolean isExcept = op instanceof LogicalExceptOperator;
                    for (int i = 0; i < outputs.size(); i++) {
                        Set<BaseColumn> bases = new HashSet<>();
                        for (int b = 0; b < children.size(); b++) {
                            ColumnRefOperator childRef = children.get(b).get(i);
                            if (isExcept && b > 0) {
                                applyRoleByRef(childRef, ColumnAccessKind.FILTER);
                                continue;
                            }
                            Set<BaseColumn> childBases = refToBases.get(childRef);
                            if (childBases != null) {
                                bases.addAll(childBases);
                            }
                        }
                        addBases(outputs.get(i), bases);
                    }
                }
            } else if (op instanceof LogicalApplyOperator) {
                // Apply's scalar output may either be a derived value (aggregate or window inside
                // the subquery) OR a passthrough of a raw base column, e.g.
                //   SELECT (SELECT v7 FROM t2 WHERE ...) FROM t1
                // where the user actually sees v7. Pull bases from the subquery expression: if the
                // inner output is an aggregate ref it has no bases (aggregate branch above doesn't
                // seed them), so this naturally collapses to "no leak"; for passthrough columns
                // bases propagate and markRootOutputProjection() correctly marks them PROJECTION.
                //
                // ONLY scalar Apply: IN/EXISTS produce a boolean — propagating bases would mark
                // inner columns as PROJECTION even though the user only sees true/false.
                LogicalApplyOperator apply = (LogicalApplyOperator) op;
                ColumnRefOperator output = apply.getOutput();
                if (output != null && apply.isScalar()) {
                    addBases(output, collectBases(apply.getSubqueryOperator()));
                } else if (apply.getSubqueryOperator() != null) {
                    // Quantified (IN) / existential (EXISTS) Apply: the subquery output drives a
                    // boolean predicate. The inner columns are not seen by the user but are used
                    // to compute the predicate — classify as FILTER.
                    applyRole(apply.getSubqueryOperator(), ColumnAccessKind.FILTER);
                }
                // Correlation conjuncts couple outer rows to inner rows; outer base columns there
                // are filter-like even if they are also visible elsewhere in the plan.
                applyRole(apply.getCorrelationConjuncts(), ColumnAccessKind.FILTER);
            } else if (op instanceof LogicalRepeatOperator) {
                // ROLLUP/CUBE/GROUPING SETS introduce synthetic GROUPING() output refs whose VALUE
                // is a 0/1 indicator, NOT the base column. Mapping the indicator back to the base
                // would let `markRootOutputProjection()` mark e.g. v4 as PROJECTION when the user
                // only sees the indicator. Leave outputs unmapped; v4 still gets AGG_ARG via the
                // surrounding aggregation.
            } else if (op instanceof LogicalTableFunctionOperator) {
                // Table functions (UNNEST, json_each, ...) emit result columns derived from their
                // input parameter expressions. The user sees the result values, which expose the
                // input columns. Map every fn-result ref to the union of bases from all input
                // params so root-output PROJECTION reaches the source columns.
                LogicalTableFunctionOperator tf = (LogicalTableFunctionOperator) op;
                Set<BaseColumn> inputBases = new HashSet<>();
                if (tf.getFnParamColumnProject() != null) {
                    for (Pair<ColumnRefOperator, ScalarOperator> p : tf.getFnParamColumnProject()) {
                        inputBases.addAll(collectBases(p.second));
                    }
                }
                if (tf.getFnResultColRefs() != null && !inputBases.isEmpty()) {
                    for (ColumnRefOperator out : tf.getFnResultColRefs()) {
                        addBases(out, inputBases);
                    }
                }
            } else if (op instanceof LogicalCTEConsumeOperator) {
                LogicalCTEConsumeOperator cte = (LogicalCTEConsumeOperator) op;
                Map<ColumnRefOperator, ColumnRefOperator> map = cte.getCteOutputColumnRefMap();
                if (map != null) {
                    for (Map.Entry<ColumnRefOperator, ColumnRefOperator> e : map.entrySet()) {
                        Set<BaseColumn> b = refToBases.get(e.getValue());
                        if (b != null) {
                            addBases(e.getKey(), b);
                        }
                    }
                }
            } else if (op instanceof LogicalWindowOperator) {
                // Window functions split into two flavors:
                //   * value-returning (first_value, last_value, lead, lag, nth_value): the user
                //     reads RAW arg values back through the window output. Map output → arg bases
                //     so markRootOutputProjection() flags them as PROJECTION.
                //   * aggregating (sum/avg/count/row_number/...): output is a derived scalar; do
                //     not propagate bases so PROJECTION is not leaked onto the raw column.
                LogicalWindowOperator win = (LogicalWindowOperator) op;
                if (win.getWindowCall() != null) {
                    for (Map.Entry<ColumnRefOperator, CallOperator> e : win.getWindowCall().entrySet()) {
                        CallOperator call = e.getValue();
                        if (isValueReturningWindow(call)) {
                            addBases(e.getKey(), collectBases(call));
                        }
                    }
                }
            }

            // Inline projection on any operator (Operator carries an optional Projection).
            Projection projection = op.getProjection();
            if (projection != null && projection.getColumnRefMap() != null) {
                extendByRefMap(projection.getColumnRefMap());
            }

            // 2. apply roles based on the operator semantics
            if (op instanceof LogicalJoinOperator) {
                LogicalJoinOperator join = (LogicalJoinOperator) op;
                applyRole(join.getOnPredicate(), ColumnAccessKind.JOIN_KEY);
                // Join residual predicates (e.g. anti/semi residuals) — same role.
                applyRole(join.getPredicate(), ColumnAccessKind.JOIN_KEY);
            } else if (op instanceof LogicalFilterOperator) {
                applyRole(((LogicalFilterOperator) op).getPredicate(), ColumnAccessKind.FILTER);
            } else if (op instanceof LogicalScanOperator) {
                // Pushed-down WHERE that the optimizer collapsed into the scan node.
                applyRole(op.getPredicate(), ColumnAccessKind.FILTER);
                // Connector scans (Iceberg/Hive/Hudi/Delta/Paimon/JDBC/...) push partition,
                // non-partition, and min-max conjuncts into ScanOperatorPredicates separately
                // from getPredicate(). After partition pruning these can be the only place a
                // predicate column shows up.
                LogicalScanOperator scan = (LogicalScanOperator) op;
                try {
                    com.starrocks.sql.optimizer.operator.ScanOperatorPredicates sp =
                            scan.getScanOperatorPredicates();
                    if (sp != null) {
                        for (ScalarOperator s : sp.getPartitionConjuncts()) {
                            applyRole(s, ColumnAccessKind.FILTER);
                        }
                        for (ScalarOperator s : sp.getNoEvalPartitionConjuncts()) {
                            applyRole(s, ColumnAccessKind.FILTER);
                        }
                        for (ScalarOperator s : sp.getNonPartitionConjuncts()) {
                            applyRole(s, ColumnAccessKind.FILTER);
                        }
                        for (ScalarOperator s : sp.getMinMaxConjuncts()) {
                            applyRole(s, ColumnAccessKind.FILTER);
                        }
                    }
                } catch (com.starrocks.common.AnalysisException ignored) {
                    // Base LogicalScanOperator throws when the subclass doesn't support these
                    // (e.g. OlapScan); that's fine — there are no extra conjuncts to read.
                }
            } else if (op instanceof LogicalAggregationOperator) {
                LogicalAggregationOperator agg = (LogicalAggregationOperator) op;
                // GROUP BY keys participate in aggregation; whether the user actually sees the
                // value depends on whether the key reaches the root output. Mark as AGG_ARG here;
                // PROJECTION is added later by markRootOutputProjection() when applicable.
                // Counter-example to the old behavior: SELECT count(*) FROM t GROUP BY pii — the
                // user never sees pii values, so it must NOT be PROJECTION.
                for (ColumnRefOperator key : agg.getGroupingKeys()) {
                    applyRoleByRef(key, ColumnAccessKind.AGG_ARG);
                }
                // Arguments of aggregate functions are collapsed into the aggregate's output —
                // user does NOT see the raw value.
                for (CallOperator call : agg.getAggregations().values()) {
                    applyRole(call, ColumnAccessKind.AGG_ARG);
                }
                // HAVING and any post-aggregation predicates collapsed onto the agg node.
                // The predicate references aggregate output ColumnRefs (e.g. `count(v6) > 1`
                // becomes `count_v6_ref > 1` in the optimized plan). Aggregate outputs have no
                // bases on purpose (see seed branch above), so applyRole() would no-op on them.
                // Resolve such refs back to the aggregate's CallOperator arguments and mark THOSE
                // columns as FILTER, so HAVING shows up in audit.
                applyHavingPredicate(agg);
            } else if (op instanceof LogicalTopNOperator) {
                LogicalTopNOperator topn = (LogicalTopNOperator) op;
                if (topn.getOrderByElements() != null) {
                    for (Ordering ord : topn.getOrderByElements()) {
                        applyRoleByRef(ord.getColumnRef(), ColumnAccessKind.FILTER);
                    }
                }
                // Window-style ranking pre-agg: partitionByColumns are filter-shaped and
                // partitionPreAggCall introduces aggregate-output refs. Seed those so they don't
                // disappear and tag args as AGG_ARG.
                if (topn.getPartitionByColumns() != null) {
                    for (ColumnRefOperator key : topn.getPartitionByColumns()) {
                        applyRoleByRef(key, ColumnAccessKind.FILTER);
                    }
                }
                if (topn.getPartitionPreAggCall() != null) {
                    for (Map.Entry<ColumnRefOperator, CallOperator> e : topn.getPartitionPreAggCall().entrySet()) {
                        // Tag args first; aggregate output refs intentionally have no bases (same
                        // rationale as LogicalAggregationOperator branch).
                        applyRole(e.getValue(), ColumnAccessKind.AGG_ARG);
                    }
                }
            } else if (op instanceof LogicalWindowOperator) {
                LogicalWindowOperator win = (LogicalWindowOperator) op;
                // Window function arguments (e.g. v6 in `sum(v6) OVER (...)`) are aggregate-like:
                // user sees the windowed scalar, not the raw value. Mark as AGG_ARG.
                if (win.getWindowCall() != null) {
                    for (CallOperator call : win.getWindowCall().values()) {
                        applyRole(call, ColumnAccessKind.AGG_ARG);
                    }
                }
                if (win.getPartitionExpressions() != null) {
                    for (ScalarOperator key : win.getPartitionExpressions()) {
                        applyRole(key, ColumnAccessKind.FILTER);
                    }
                }
                if (win.getOrderByElements() != null) {
                    for (Ordering ord : win.getOrderByElements()) {
                        applyRoleByRef(ord.getColumnRef(), ColumnAccessKind.FILTER);
                    }
                }
            }
        }

        private void seedScanRefs(LogicalScanOperator scan) {
            Table table = scan.getTable();
            TableName tableName = tableObjToTableName.get(table);
            if (tableName == null) {
                return;
            }
            for (Map.Entry<ColumnRefOperator, Column> e : scan.getColRefToColumnMetaMap().entrySet()) {
                BaseColumn base = new BaseColumn(tableName, e.getValue().getName());
                refToBases.computeIfAbsent(e.getKey(), k -> new HashSet<>()).add(base);
                // Ensure every scanned column has a result entry, even if no role gets applied.
                // Matches legacy behavior where the column showed up in scanColumns regardless of usage.
                scanColumns.computeIfAbsent(tableName, k -> new HashMap<>())
                        .computeIfAbsent(e.getValue().getName(), k -> EnumSet.noneOf(ColumnAccessKind.class));
            }
        }

        private void extendByRefMap(Map<ColumnRefOperator, ScalarOperator> map) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> e : map.entrySet()) {
                addBases(e.getKey(), collectBases(e.getValue()));
            }
        }

        private void addBases(ColumnRefOperator ref, Set<BaseColumn> bases) {
            if (bases == null || bases.isEmpty()) {
                return;
            }
            refToBases.computeIfAbsent(ref, k -> new HashSet<>()).addAll(bases);
        }

        private Set<BaseColumn> collectBases(ScalarOperator expr) {
            if (expr == null) {
                return new HashSet<>();
            }
            Set<BaseColumn> bases = new HashSet<>();
            for (ColumnRefOperator ref : Utils.extractColumnRef(expr)) {
                Set<BaseColumn> b = refToBases.get(ref);
                if (b != null) {
                    bases.addAll(b);
                }
            }
            return bases;
        }

        /**
         * Returns the set of base columns that the user actually sees through this value-returning
         * aggregate's output. Visibility per function family:
         *   group_concat — every argument (values AND separator)
         *   map_agg(key, value) — first two arguments
         *   everything else (array_agg, min/max/min_by/max_by/percentile_disc/any_value) — first arg
         */
        private Set<BaseColumn> visibleAggregateBases(CallOperator call) {
            String name = call.getFnName();
            if (name == null) {
                return new HashSet<>();
            }
            String lowered = name.toLowerCase(java.util.Locale.ROOT);
            int childCount = call.getChildren().size();
            int visibleArgs;
            if (ALL_ARGS_VALUE_RETURNING_AGGREGATES.contains(lowered)) {
                visibleArgs = childCount;
            } else if (TWO_ARG_VALUE_RETURNING_AGGREGATES.contains(lowered)) {
                visibleArgs = 2;
            } else {
                visibleArgs = 1;
            }
            Set<BaseColumn> bases = new HashSet<>();
            int max = Math.min(visibleArgs, childCount);
            for (int i = 0; i < max; i++) {
                bases.addAll(collectBases(call.getChild(i)));
            }
            return bases;
        }

        private static boolean isValueReturningAggregate(CallOperator call) {
            String name = call.getFnName();
            if (name == null) {
                return false;
            }
            return VALUE_RETURNING_AGGREGATES.contains(name.toLowerCase(java.util.Locale.ROOT));
        }

        private static boolean isValueReturningWindow(CallOperator call) {
            String name = call.getFnName();
            if (name == null) {
                return false;
            }
            return VALUE_RETURNING_WINDOWS.contains(name.toLowerCase(java.util.Locale.ROOT));
        }

        private void applyHavingPredicate(LogicalAggregationOperator agg) {
            ScalarOperator predicate = agg.getPredicate();
            if (predicate == null) {
                return;
            }
            Map<ColumnRefOperator, CallOperator> aggregations = agg.getAggregations();
            for (ColumnRefOperator ref : Utils.extractColumnRef(predicate)) {
                CallOperator aggCall = aggregations.get(ref);
                if (aggCall != null) {
                    // ref names an aggregate output — resolve to its arg columns
                    applyRole(aggCall, ColumnAccessKind.FILTER);
                } else {
                    // direct grouping-key ref or other passthrough column
                    applyRoleByRef(ref, ColumnAccessKind.FILTER);
                }
            }
        }

        private void applyRole(ScalarOperator expr, ColumnAccessKind role) {
            if (expr == null) {
                return;
            }
            for (ColumnRefOperator ref : Utils.extractColumnRef(expr)) {
                applyRoleByRef(ref, role);
            }
        }

        private void applyRoleByRef(ColumnRefOperator ref, ColumnAccessKind role) {
            Set<BaseColumn> bases = refToBases.get(ref);
            if (bases == null) {
                return;
            }
            for (BaseColumn base : bases) {
                scanColumns.computeIfAbsent(base.tableName, k -> new HashMap<>())
                        .computeIfAbsent(base.columnName, k -> EnumSet.noneOf(ColumnAccessKind.class))
                        .add(role);
            }
        }

        public void markRootOutputProjection(List<ColumnRefOperator> outputs) {
            if (outputs == null) {
                return;
            }
            for (ColumnRefOperator ref : outputs) {
                applyRoleByRef(ref, ColumnAccessKind.PROJECTION);
            }
        }
    }

    private static final class BaseColumn {
        final TableName tableName;
        final String columnName;

        BaseColumn(TableName tableName, String columnName) {
            this.tableName = tableName;
            this.columnName = columnName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BaseColumn)) {
                return false;
            }
            BaseColumn that = (BaseColumn) o;
            return Objects.equals(tableName, that.tableName) && Objects.equals(columnName, that.columnName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableName, columnName);
        }
    }

    private static class TableNameCollector extends AstTraverser<Void, Void> {
        private final Map<TableName, Table> tableNameToTableObj;
        private final Map<Table, TableName> tableTableNameMap;

        public TableNameCollector(Map<TableName, Table> tableNameToTableObj, Map<Table, TableName> tableTableNameMap) {
            this.tableNameToTableObj = tableNameToTableObj;
            this.tableTableNameMap = tableTableNameMap;
        }

        @Override
        public Void visitQueryStatement(QueryStatement statement, Void context) {
            return visit(statement.getQueryRelation());
        }

        @Override
        public Void visitInsertStatement(InsertStmt node, Void context) {
            Table table = node.getTargetTable();
            TableName tableName = TableName.fromTableRef(node.getTableRef());
            tableNameToTableObj.put(tableName, table);
            tableTableNameMap.put(table, tableName);
            return super.visitInsertStatement(node, context);
        }

        @Override
        public Void visitUpdateStatement(UpdateStmt node, Void context) {
            Table table = node.getTable();
            TableName tableName = TableName.fromTableRef(node.getTableRef());
            tableNameToTableObj.put(tableName, table);
            tableTableNameMap.put(table, tableName);
            return super.visitUpdateStatement(node, context);
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt node, Void context) {
            Table table = node.getTable();
            TableName tableName = TableName.fromTableRef(node.getTableRef());
            tableNameToTableObj.put(tableName, table);
            tableTableNameMap.put(table, tableName);
            return super.visitDeleteStatement(node, context);
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            Table table = node.getTable();
            tableNameToTableObj.put(node.getName(), table);
            tableTableNameMap.put(table, node.getName());
            return null;
        }

        @Override
        public Void visitView(ViewRelation node, Void context) {
            Table table = node.getView();
            tableNameToTableObj.put(node.getName(), table);
            tableTableNameMap.put(table, node.getName());
            return null;
        }
    }
}
