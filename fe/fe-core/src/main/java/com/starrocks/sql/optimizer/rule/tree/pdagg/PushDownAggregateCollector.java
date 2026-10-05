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

package com.starrocks.sql.optimizer.rule.tree.pdagg;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.ExpressionStatisticCalculator;
import com.starrocks.sql.optimizer.statistics.MultiColumnCombinedStats;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;
import com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient;
import com.starrocks.sql.optimizer.statistics.TopNAggregationCost;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.system.BackendResourceStat;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/*
 * Collect all can be push down aggregate context, to get which aggregation can be
 * pushed down and the push down path.
 *
 * Can't rewrite directly, because we don't know which aggregation needs to be
 * push down before arrive at scan node.
 *
 * And in this phase, the key of AggregateContext's map is origin aggregate column, for
 * mark push down which aggregation, the value will multi-rewrite by path, for check
 * which aggregation needs push down
 */
public class PushDownAggregateCollector extends OptExpressionVisitor<Void, AggregatePushDownContext> {
    private static final Logger LOG = LogManager.getLogger(PushDownAggregateCollector.class);

    private static final int DISABLE_PUSH_DOWN_AGG = -1;
    private static final int PUSH_DOWN_AGG_AUTO = 0;
    private static final int PUSH_DOWN_ALL_AGG = 1;
    private static final int PUSH_DOWN_MEDIUM_CARDINALITY_AGG = 2;
    private static final int PUSH_DOWN_HIGH_CARDINALITY_AGG = 3;

    private static final List<String> WHITE_FNS = ImmutableList.of(FunctionSet.MAX, FunctionSet.MIN,
            FunctionSet.SUM, FunctionSet.HLL_UNION, FunctionSet.BITMAP_UNION, FunctionSet.PERCENTILE_UNION);

    private final TaskContext taskContext;
    private final OptimizerContext optimizerContext;
    private final ColumnRefFactory factory;
    private final SessionVariable sessionVariable;

    // Used to assign a unique index to each path from root to leaf node.
    // For example, in the following tree structure, the index of A#1, Join#3, Join#5 is 0,
    // the index of B#2 is 1, and the index of C#4 is 2.
    //      Join#5
    //    Join#3   C#4
    // A#1    B#2
    private final MutableInt nextRootToLeafPathIndex;
    // OrigAgg -> Map[PathIndex -> List[AggregatePushDownContext]].
    private final Map<LogicalAggregationOperator, Map<Integer, List<AggregatePushDownContext>>> rewriteContextCandidates =
            Maps.newHashMap();
    private final Map<LogicalAggregationOperator, List<AggregatePushDownContext>> allRewriteContext = Maps.newHashMap();

    public PushDownAggregateCollector(TaskContext taskContext) {
        this(taskContext, new MutableInt());
    }

    private PushDownAggregateCollector(TaskContext taskContext, MutableInt nextRootToLeafPathIndex) {
        this.taskContext = taskContext;
        this.optimizerContext = taskContext.getOptimizerContext();
        this.factory = taskContext.getOptimizerContext().getColumnRefFactory();
        this.sessionVariable = taskContext.getOptimizerContext().getSessionVariable();
        this.nextRootToLeafPathIndex = nextRootToLeafPathIndex;
    }

    Map<LogicalAggregationOperator, List<AggregatePushDownContext>> getAllRewriteContext() {
        return allRewriteContext;
    }

    public void collect(OptExpression root) {
        collect(root, new AggregatePushDownContext(nextRootToLeafPathIndex.getAndIncrement()));
    }

    private void collect(OptExpression root, AggregatePushDownContext context) {
        process(root, context);
        selectPushDownTarget();
    }

    @Override
    public Void visit(OptExpression optExpression, AggregatePushDownContext context) {
        // forbidden push down
        for (OptExpression input : optExpression.getInputs()) {
            process(input, AggregatePushDownContext.EMPTY);
        }
        return null;
    }

    private Void processChild(OptExpression optExpression, AggregatePushDownContext context) {
        for (OptExpression input : optExpression.getInputs()) {
            process(input, context);
        }
        return null;
    }

    private void process(OptExpression opt, AggregatePushDownContext context) {
        opt.getOp().accept(this, opt, context);
    }

    private static boolean allConstant(Collection<CallOperator> calls) {
        for (CallOperator call : calls) {
            if (!call.isConstant()) {
                return false;
            }
        }
        return true;
    }

    private boolean isInvalid(OptExpression optExpression, AggregatePushDownContext context) {
        return context.isEmpty() || optExpression.getOp().hasLimit();
    }

    @Override
    public Void visitLogicalFilter(OptExpression optExpression, AggregatePushDownContext context) {
        if (isInvalid(optExpression, context)) {
            return visit(optExpression, context);
        }

        // add filter columns in groupBys
        LogicalFilterOperator filter = (LogicalFilterOperator) optExpression.getOp();
        for (int id : filter.getRequiredChildInputColumns().getColumnIds()) {
            ColumnRefOperator v = factory.getColumnRef(id);
            context.groupBys.put(v, v);
        }
        return processChild(optExpression, context);
    }

    @Override
    public Void visitLogicalProject(OptExpression optExpression, AggregatePushDownContext context) {
        if (isInvalid(optExpression, context)) {
            return visit(optExpression, context);
        }

        LogicalProjectOperator project = (LogicalProjectOperator) optExpression.getOp();

        boolean identityProject = true;
        for (Map.Entry<ColumnRefOperator, ScalarOperator> e : project.getColumnRefMap().entrySet()) {
            if (!e.getValue().equals(e.getKey())) {
                identityProject = false;
                break;
            }
        }
        if (identityProject) {
            return processChild(optExpression, context);
        }

        ColumnRefSet aggUsedColumns = new ColumnRefSet();
        for (CallOperator v : context.aggregations.values()) {
            v.collectUsedColumns(aggUsedColumns);
        }

        Map<ColumnRefOperator, ScalarOperator> columnRefMap = project.getColumnRefMap();
        Map<ColumnRefOperator, ScalarOperator> aggRewriteMap = columnRefMap;

        // handle specials functions case-when/if
        // split to groupBys and mock new aggregations by values, don't need to save
        // origin predicate, we just do check in collect phase
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
            ColumnRefOperator key = entry.getKey();
            ScalarOperator value = entry.getValue();

            if (!aggUsedColumns.contains(key) || !(value instanceof CallOperator call)) {
                continue;
            }

            if (call instanceof CaseWhenOperator) {
                CaseWhenOperator caseWhen = (CaseWhenOperator) value;
                for (ScalarOperator condition : caseWhen.getAllConditionClause()) {
                    for (int id : condition.getUsedColumns().getColumnIds()) {
                        ColumnRefOperator v = factory.getColumnRef(id);
                        context.groupBys.put(v, v);
                    }
                }

                List<ScalarOperator> newWhenThen = Lists.newArrayList();
                for (int i = 0; i < caseWhen.getWhenClauseSize(); i++) {
                    if (caseWhen.getThenClause(i).isConstant() && !caseWhen.getThenClause(i).isConstantNull()) {
                        // forbidden push down
                        return visit(optExpression, context);
                    }
                    newWhenThen.add(ConstantOperator.createBoolean(false));
                    newWhenThen.add(caseWhen.getThenClause(i));
                }

                if (caseWhen.hasElse() && caseWhen.getElseClause().isConstant()
                        && !caseWhen.getElseClause().isConstantNull()) {
                    // forbid push down: a non-null constant ELSE cannot be pushed below the join.
                    // PushDownAggregateRewriter.rewriteProject asserts a constant ELSE is NULL, so
                    // letting it through would trip that checkState (IllegalStateException). Mirror
                    // the THEN-clause guard above and the IF path's else-branch check.
                    return visit(optExpression, context);
                }

                // mock just value case when
                CaseWhenOperator newCaseWhen = new CaseWhenOperator(caseWhen.getType(), null,
                        caseWhen.hasElse() ? caseWhen.getElseClause() : null, newWhenThen);

                if (aggRewriteMap == columnRefMap) {
                    aggRewriteMap = Maps.newHashMap(columnRefMap);
                }
                aggRewriteMap.put(key, newCaseWhen);
            } else if (call.getFunction() != null &&
                        FunctionSet.IF.equals(call.getFunction().getFunctionName().getFunction())) {
                if (call.getChildren().stream().skip(1).anyMatch(c -> c.isConstant() && !c.isConstantNull())) {
                    // forbidden push down
                    return visit(optExpression, context);
                }

                for (int id : call.getChild(0).getUsedColumns().getColumnIds()) {
                    ColumnRefOperator v = factory.getColumnRef(id);
                    context.groupBys.put(v, v);
                }

                CallOperator newIf = new CallOperator(call.getFnName(), call.getType(), Lists.newArrayList(call.getArguments()),
                        call.getFunction());
                newIf.setChild(0, ConstantOperator.createBoolean(false));

                if (aggRewriteMap == columnRefMap) {
                    aggRewriteMap = Maps.newHashMap(columnRefMap);
                }
                aggRewriteMap.put(key, newIf);
            }
        }

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(aggRewriteMap);
        context.aggregations.replaceAll((k, v) -> (CallOperator) rewriter.rewrite(v));
        if (aggRewriteMap != columnRefMap) {
            ReplaceColumnRefRewriter originalRewriter = new ReplaceColumnRefRewriter(columnRefMap);
            context.groupBys.replaceAll((k, v) -> originalRewriter.rewrite(v));
        } else {
            context.groupBys.replaceAll((k, v) -> rewriter.rewrite(v));
        }

        // check has constant aggregate, forbidden
        if (!context.aggregations.isEmpty() &&
                context.aggregations.values().stream().allMatch(ScalarOperator::isConstant)) {
            return visit(optExpression, context);
        }

        return processChild(optExpression, context);
    }

    @Override
    public Void visitLogicalAggregate(OptExpression optExpression, AggregatePushDownContext context) {
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) optExpression.getOp();
        // distinct/count* aggregate can't push down
        for (CallOperator c : aggregate.getAggregations().values()) {
            if (c.isDistinct() || c.isCountStar()) {
                return visit(optExpression, context);
            }
        }

        // all constant can't push down
        if (!aggregate.getAggregations().isEmpty() && allConstant(aggregate.getAggregations().values())) {
            return visit(optExpression, context);
        }

        // none group by don't push down
        if (aggregate.getGroupingKeys().isEmpty()) {
            return visit(optExpression, context);
        }

        context = new AggregatePushDownContext(context.rootToLeafPathIndex);
        context.setAggregator(aggregate);
        return processChild(optExpression, context);
    }

    @Override
    public Void visitLogicalJoin(OptExpression optExpression, AggregatePushDownContext context) {
        if (isInvalid(optExpression, context)) {
            return visit(optExpression, context);
        }
        // constant aggregate can't push down
        if (!context.aggregations.isEmpty() &&
                context.aggregations.values().stream().allMatch(ScalarOperator::isConstant)) {
            return visit(optExpression, context);
        }

        boolean isSmallBroadcastJoin = isSmallBroadcastJoin(optExpression);
        if (isSmallBroadcastJoin && !context.pushPaths.isEmpty() && isAllChildrenSP(optExpression)) {
            context.targetPosition = optExpression;
            addCandidateContext(context.origAggregator, context);
        }

        // split aggregate to left/right child
        JoinUsedColumns usedColumns = new JoinUsedColumns();
        AggregatePushDownContext leftContext =
                splitJoinAggregate(optExpression, context, 0, isSmallBroadcastJoin, usedColumns);
        AggregatePushDownContext rightContext = splitJoinAggregate(optExpression, context, 1, false, usedColumns);
        process(optExpression.inputAt(0), leftContext);
        process(optExpression.inputAt(1), rightContext);
        return null;
    }

    /*
     * When aggregation is pushed down to join, it means that the columns on aggregation are from
     * multi-tables, maybe aggregate columns from left table and group by columns from right table.
     * We only push down aggregation to child which one support aggregate columns, because aggregate
     * columns will ignore 1-N join, so there will check:
     *
     * 1. all aggregation related columns must come from one child, and
     *    it can be push down to both sides if columns is empty, like only group by
     * 2. re-compute group by columns
     *  2.1. split the original group by columns
     *  2.2. add join on-predicate and predicate used columns to group by
     *
     * e.g. 1-N case
     * select t0.v1, t1.v1, sum(t0.v2), sum(t1.v2) from t0 join t1 on t0.v1 = t1.v1;
     *
     * t0.v1    t0.v2           t1.v1   t1.v2
     *   1        1      Join     1        1
     *   1        1               1        1
     */
    private AggregatePushDownContext splitJoinAggregate(OptExpression optExpression, AggregatePushDownContext context,
                                                        int child, boolean immediateChildOfSmallBroadcastJoin,
                                                        JoinUsedColumns usedColumns) {
        LogicalJoinOperator join = (LogicalJoinOperator) optExpression.getOp();
        ColumnRefSet childOutput = optExpression.getChildOutputColumns(child);

        // check aggregations
        if (usedColumns.aggregations == null) {
            usedColumns.aggregations = new ColumnRefSet();
            for (CallOperator aggregation : context.aggregations.values()) {
                aggregation.collectUsedColumns(usedColumns.aggregations);
            }
        }
        ColumnRefSet aggregationsRefs = usedColumns.aggregations;
        if (!childOutput.containsAll(aggregationsRefs)) {
            return AggregatePushDownContext.EMPTY;
        }

        int rootToLeafPathIndex = child == 0 ? context.rootToLeafPathIndex : nextRootToLeafPathIndex.getAndIncrement();
        AggregatePushDownContext childContext = new AggregatePushDownContext(rootToLeafPathIndex);
        childContext.aggregations.putAll(context.aggregations);

        // check group by
        if (usedColumns.groupBys == null) {
            usedColumns.groupBys = Lists.newArrayListWithCapacity(context.groupBys.size());
            for (ScalarOperator groupBy : context.groupBys.values()) {
                usedColumns.groupBys.add(groupBy.getUsedColumns());
            }
        }
        int groupByIndex = 0;
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : context.groupBys.entrySet()) {
            ColumnRefSet groupByUseColumns = usedColumns.groupBys.get(groupByIndex++);
            if (childOutput.containsAll(groupByUseColumns)) {
                childContext.groupBys.put(entry.getKey(), entry.getValue());
            } else if (childOutput.isIntersect(groupByUseColumns)) {
                // e.g. group by abs(a + b), we can derive group by a
                Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
                for (int id : groupByUseColumns.getColumnIds()) {
                    if (!childOutput.contains(id)) {
                        ColumnRefOperator k = factory.getColumnRef(id);
                        rewriteMap.put(k, ConstantOperator.createNull(k.getType()));
                    }
                }
                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);
                childContext.groupBys.put(entry.getKey(), rewriter.rewrite(entry.getValue()));
            }
        }

        if (join.getOnPredicate() != null) {
            if (usedColumns.onPredicate == null) {
                usedColumns.onPredicate = join.getOnPredicate().getUsedColumns();
            }
            for (int id : usedColumns.onPredicate.getColumnIds()) {
                ColumnRefOperator c = factory.getColumnRef(id);
                if (childOutput.contains(c)) {
                    childContext.groupBys.put(c, c);
                }
            }
        }

        if (join.getPredicate() != null) {
            if (usedColumns.predicate == null) {
                usedColumns.predicate = join.getPredicate().getUsedColumns();
            }
            for (int id : usedColumns.predicate.getColumnIds()) {
                ColumnRefOperator v = factory.getColumnRef(id);
                if (childOutput.contains(v)) {
                    childContext.groupBys.put(v, v);
                }
            }
        }

        childContext.immediateChildOfSmallBroadcastJoin = immediateChildOfSmallBroadcastJoin;
        childContext.origAggregator = context.origAggregator;
        childContext.pushPaths.addAll(context.pushPaths);
        childContext.pushPaths.add(child);
        return childContext;
    }

    // Used columns of one join's aggregations, group-bys and predicates, filled by the first split that needs them.
    private static final class JoinUsedColumns {
        private ColumnRefSet aggregations;
        private List<ColumnRefSet> groupBys;
        private ColumnRefSet onPredicate;
        private ColumnRefSet predicate;
    }

    @Override
    public Void visitLogicalUnion(OptExpression optExpression, AggregatePushDownContext context) {
        if (isInvalid(optExpression, context)) {
            return visit(optExpression, context);
        }

        List<PushDownAggregateCollector> collectors = Lists.newArrayList();
        LogicalUnionOperator union = (LogicalUnionOperator) optExpression.getOp();
        for (int i = 0; i < optExpression.getInputs().size(); i++) {
            List<ColumnRefOperator> childOutput = union.getChildOutputColumns().get(i);
            Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
            Preconditions.checkState(childOutput.size() == union.getOutputColumnRefOp().size());
            for (int k = 0; k < union.getOutputColumnRefOp().size(); k++) {
                rewriteMap.put(union.getOutputColumnRefOp().get(k), childOutput.get(k));
            }

            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);
            AggregatePushDownContext childContext = new AggregatePushDownContext(nextRootToLeafPathIndex.getAndIncrement());
            childContext.origAggregator = context.origAggregator;
            childContext.aggregations.putAll(context.aggregations);
            childContext.aggregations.replaceAll((k, v) -> (CallOperator) rewriter.rewrite(v));

            childContext.groupBys.putAll(context.groupBys);
            childContext.groupBys.replaceAll((k, v) -> rewriter.rewrite(v));
            childContext.pushPaths.addAll(context.pushPaths);
            childContext.pushPaths.add(i);

            PushDownAggregateCollector collector = new PushDownAggregateCollector(this.taskContext, nextRootToLeafPathIndex);
            collectors.add(collector);
            collector.collect(optExpression.inputAt(i), childContext);
        }

        // collect push down aggregate context
        List<List<AggregatePushDownContext>> allChildRewriteContext = Lists.newArrayList();
        for (PushDownAggregateCollector childCollector : collectors) {
            List<AggregatePushDownContext> childRewriteContext = childCollector.allRewriteContext.remove(context.origAggregator);
            if (childRewriteContext != null) {
                allChildRewriteContext.add(childRewriteContext);
            }
        }

        // merge other rewrite context
        for (PushDownAggregateCollector collector : collectors) {
            Preconditions.checkState(
                    !CollectionUtils.containsAny(allRewriteContext.keySet(), collector.allRewriteContext.keySet()));
            allRewriteContext.putAll(collector.allRewriteContext);
        }

        // none aggregate can push down to union children
        if (allChildRewriteContext.isEmpty() || allChildRewriteContext.size() != collectors.size()) {
            return null;
        }

        for (List<AggregatePushDownContext> childContexts : allChildRewriteContext) {
            Set<ColumnRefOperator> cg = new HashSet<>();
            Set<ColumnRefOperator> ca = new HashSet<>();

            childContexts.forEach(c -> {
                cg.addAll(c.groupBys.keySet());
                ca.addAll(c.aggregations.keySet());
            });

            // Must all same, like Agg1, Agg2 split by Union, and Scan1 support Agg1/Agg2, and
            // Scan2 only support Agg1, we must promise to either push down or not push down
            // like:
            //         UNION
            //        /      \
            //     Scan1    Join
            //             /    \
            //         Scan2    Scan3
            if (!cg.containsAll(context.groupBys.keySet()) || !ca.containsAll(context.aggregations.keySet())) {
                return null;
            }
        }

        List<AggregatePushDownContext> list = allRewriteContext.get(context.origAggregator);
        if (list == null) {
            list = Lists.newArrayList();
        }
        allChildRewriteContext.forEach(list::addAll);
        allRewriteContext.put(context.origAggregator, list);
        return null;
    }

    @Override
    public Void visitLogicalCTEAnchor(OptExpression optExpression, AggregatePushDownContext context) {
        process(optExpression.inputAt(1), context);
        process(optExpression.inputAt(0), AggregatePushDownContext.EMPTY);
        return null;
    }

    @Override
    public Void visitLogicalTableScan(OptExpression optExpression, AggregatePushDownContext context) {
        if (!isInvalid(optExpression, context)) {
            context.targetPosition = optExpression;
            addCandidateContext(context.origAggregator, context);
        }
        return null;
    }

    private boolean checkStatistics(AggregatePushDownContext context, ColumnRefSet groupBys, Statistics statistics) {
        final int pushDownMode = sessionVariable.getCboPushDownAggregateMode();

        // check force push down flag
        // flag 0: auto. 1: force push down. -1: don't push down. 2: push down medium. 3: push down high
        if (pushDownMode == PUSH_DOWN_ALL_AGG) {
            return true;
        }

        if (pushDownMode == DISABLE_PUSH_DOWN_AGG) {
            return false;
        }

        int[] groupByIds = groupBys.getColumnIds();
        Set<ColumnRefOperator> columnRefOperators = new HashSet<>(groupByIds.length * 2);
        for (int id : groupByIds) {
            columnRefOperators.add(factory.getColumnRef(id));
        }
        if (pushDownMode == PUSH_DOWN_AGG_AUTO) {
            // Right below a small broadcast join the join is cheap: its hash table is small, the probe stays where it
            // is, and the runtime filter of the join prunes the scan. An aggregate there groups by the join key and
            // adds a shuffle and a blocking phase, and it turns the join into a shuffle join. We measured it slower at
            // every key size, so we leave this candidate to the aggregate placed above the join, which groups by
            // columns of the small side and is the next candidate.
            if (context.immediateChildOfSmallBroadcastJoin) {
                return false;
            }
            return reducesRowsLocally(context, columnRefOperators, statistics);
        }

        List<ColumnStatistic> lower = Lists.newArrayList();
        int mediumCount = 0;
        int highCount = 0;

        Set<ColumnStatistic> columnStatistics = new HashSet<>();

        Pair<Set<ColumnRefOperator>, MultiColumnCombinedStats> mcStats = statistics.getLargestSubsetMCStats(columnRefOperators);

        if (sessionVariable.isCboPushDownAggWithMultiColumnStats() && mcStats != null && !mcStats.first.isEmpty()) {
            double ndv = Math.max(1, mcStats.second.getNdv());
            ColumnStatistic multiColumnStat = ColumnStatistic.builder().setDistinctValuesCount(ndv).build();
            columnStatistics.add(multiColumnStat);

            Set<ColumnRefOperator> remainedColumns = new HashSet<>(columnRefOperators);
            remainedColumns.removeAll(mcStats.first);

            for (ColumnRefOperator col : remainedColumns) {
                ColumnStatistic stat = ExpressionStatisticCalculator.calculate(col, statistics);
                columnStatistics.add(stat);
            }
        } else {
            for (ColumnRefOperator col : columnRefOperators) {
                ColumnStatistic stat = ExpressionStatisticCalculator.calculate(col, statistics);
                columnStatistics.add(stat);
            }
        }

        double outputRowCount = statistics.getOutputRowCount();
        for (ColumnStatistic stat : columnStatistics) {
            switch (groupByCardinality(stat, outputRowCount)) {
                case 0:
                    lower.add(stat);
                    break;
                case 1:
                    mediumCount++;
                    break;
                default:
                    highCount++;
                    break;
            }
        }

        double lowerCartesian = Double.MAX_VALUE;
        for (int i = 0; i < lower.size(); i++) {
            double distinct = lower.get(i).getDistinctValuesCount();
            lowerCartesian = i == 0 ? distinct : lowerCartesian * distinct;
        }

        // pow(row_count/20, a half of lower column size)
        double lowerUpper = Math.max(statistics.getOutputRowCount() / 20, 1);
        lowerUpper = Math.pow(lowerUpper, Math.max(lower.size() / 2, 1));

        if (LOG.isDebugEnabled()) {
            String aggStr = context.aggregations.values().stream().map(CallOperator::toString)
                    .collect(Collectors.joining(", "));
            String groupStr = groupBys.getStream().map(String::valueOf).collect(Collectors.joining(", "));

            LOG.debug("Push down aggregation[" + aggStr + "]" +
                    " group by[" + groupStr + "]," +
                    " check statistics rows[" + statistics.getOutputRowCount() +
                    "] high[" + highCount +
                    "] mid[" + mediumCount +
                    "] low[" + lower.size() +
                    "] cartesian[" + lowerCartesian +
                    "] upper-cartesian[" + lowerUpper + "], mode[" + pushDownMode + "]");
        }

        // 1. white push down rules
        // 1.1 only one lower/medium cardinality columns
        if (highCount == 0 && (lower.size() + mediumCount) == 1) {
            return true;
        }

        // 1.2 the cartesian of all lower/count <= 1
        // 1.3 the lower cardinality <= 3 and lowerCartesian < lowerUpper
        // 1.4 follow medium cardinality flag
        if (highCount == 0 && mediumCount == 0) {
            if (lowerCartesian <= statistics.getOutputRowCount() || lower.size() <= 2) {
                return true;
            } else if (lower.size() <= 3 && lowerCartesian < lowerUpper) {
                return true;
            } else {
                return pushDownMode >= PUSH_DOWN_MEDIUM_CARDINALITY_AGG;
            }
        }


        // 2.1 high cardinality >= 2
        // 2.2 medium cardinality > 2
        // 2.3 high cardinality = 1 and medium cardinality > 0
        if (highCount >= 2 || mediumCount > 2 || (highCount == 1 && mediumCount != 0)) {
            return false;
        }

        // 3. Extremely low cardinality for lower with at most one medium or high.
        double lowerCartesianLowerBound =
                statistics.getOutputRowCount() / StatisticsEstimateCoefficient.LOWER_AGGREGATE_EFFECT_COEFFICIENT;
        if (highCount + mediumCount == 1 && lower.size() <= 2 && lowerCartesian <= lowerCartesianLowerBound) {
            return true;
        }

        // 4. high cardinality < 2 and lower cardinality < 2
        if (highCount == 1 && lower.size() <= 2) {
            return pushDownMode >= PUSH_DOWN_HIGH_CARDINALITY_AGG;
        }

        // 5. medium cardinality <= 2
        if (lower.size() <= 2) {
            if (pushDownMode >= PUSH_DOWN_MEDIUM_CARDINALITY_AGG) {
                return true;
            }
            return statistics.getOutputRowCount() >=
                    StatisticsEstimateCoefficient.SMALL_SCALE_ROWS_LIMIT;
        }

        return false;
    }

    // We want a pushed-down aggregate only where it cuts the rows that reach the join by far. It runs as a local
    // phase in every driver, on the rows of that driver, and the BE keeps aggregating there only while the reduction
    // is above about 2, else it passes the rows through. So we require the reduction per driver to be at least
    // PUSH_DOWN_AGGREGATE_MIN_LOCAL_REDUCTION, well above that bound: a pushdown the runtime would undo costs a hash
    // attempt on every driver and, with a global phase, a shuffle and a blocking aggregate of all rows.
    //
    // The number of groups is estimated for the whole key, the group-by columns with the join keys added on the way
    // down, from the NDV of the aggregate estimate and the join statistics of the joins below. Classes of single
    // columns cannot tell that a key such as (account, date) is close to unique although each column repeats.
    private boolean reducesRowsLocally(AggregatePushDownContext context, Set<ColumnRefOperator> groupBys,
                                       Statistics statistics) {
        double rows = statistics.getOutputRowCount();
        double groups = StatisticsCalculator.estimateGroupCount(statistics, groupBys);
        if (!(rows > 0) || !Double.isFinite(groups)) {
            return false;
        }
        double localGroups = TopNAggregationCost.concurrentLocalGroups(rows, groups, drivers());
        boolean push = rows >= StatisticsEstimateCoefficient.PUSH_DOWN_AGGREGATE_MIN_LOCAL_REDUCTION * localGroups;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Push down aggregation {} group by {}: rows {}, groups {}, local groups {}, push {}",
                    context.aggregations.values(), groupBys, rows, groups, localGroups, push);
        }
        return push;
    }

    // The drivers that run the local phase, as in the cost model of aggregates.
    private double drivers() {
        ConnectContext connection = ConnectContext.get();
        if (connection == null) {
            return 1;
        }
        long warehouseId = connection.getCurrentWarehouseId();
        return Math.max(1, BackendResourceStat.getInstance().getNumBes(warehouseId)) *
                (double) Math.max(1, sessionVariable.getDegreeOfParallelism(warehouseId));
    }

    // high(2): row_count / cardinality < MEDIUM_AGGREGATE_EFFECT_COEFFICIENT
    // medium(1): row_count / cardinality >= MEDIUM_AGGREGATE_EFFECT_COEFFICIENT and < LOW_AGGREGATE_EFFECT_COEFFICIENT
    // lower(0): row_count / cardinality >= LOW_AGGREGATE_EFFECT_COEFFICIENT
    public static int groupByCardinality(ColumnStatistic statistic, double rowCount) {
        if (statistic.isUnknown()) {
            return 2;
        }

        double distinct = statistic.getDistinctValuesCount();

        if (rowCount == 0 || distinct * StatisticsEstimateCoefficient.MEDIUM_AGGREGATE_EFFECT_COEFFICIENT > rowCount) {
            return 2;
        } else if (distinct * StatisticsEstimateCoefficient.MEDIUM_AGGREGATE_EFFECT_COEFFICIENT <= rowCount &&
                distinct * StatisticsEstimateCoefficient.LOW_AGGREGATE_EFFECT_COEFFICIENT > rowCount) {
            return 1;
        } else if (distinct * StatisticsEstimateCoefficient.LOW_AGGREGATE_EFFECT_COEFFICIENT <= rowCount) {
            return 0;
        }

        return 2;
    }

    private boolean isSmallBroadcastJoin(OptExpression optExpression) {
        if (!sessionVariable.isCboPushDownAggregateOnBroadcastJoin()) {
            return false;
        }

        Statistics rightStatistics = optExpression.inputAt(1).getStatistics();
        if (rightStatistics == null) {
            return false;
        }
        double rightRows = rightStatistics.getOutputRowCount();
        return rightRows <= sessionVariable.getBroadcastRowCountLimit() &&
                rightRows <= sessionVariable.getCboPushDownAggregateOnBroadcastJoinRowCountLimit();
    }

    /**
     * Whether all the children are scan/project/filter.
     * @return true, if all the children are scan/project/filter.
     */
    private static boolean isAllChildrenSP(OptExpression root) {
        return root.getInputs().stream().allMatch(PushDownAggregateCollector::isAllSP);
    }

    private static boolean isAllSP(OptExpression root) {
        if (root.getOp().getOpType() != OperatorType.LOGICAL_PROJECT &&
                root.getOp().getOpType() != OperatorType.LOGICAL_FILTER &&
                !(root.getOp() instanceof LogicalScanOperator)) {
            return false;
        }
        return root.getInputs().stream().allMatch(PushDownAggregateCollector::isAllSP);
    }

    private void addFinalContext(LogicalAggregationOperator origAggregator, AggregatePushDownContext context) {
        allRewriteContext
                .computeIfAbsent(origAggregator, k -> Lists.newArrayList())
                .add(context);
    }

    private void addCandidateContext(LogicalAggregationOperator origAggregator, AggregatePushDownContext context) {
        rewriteContextCandidates
                .computeIfAbsent(origAggregator, k -> Maps.newHashMap())
                .computeIfAbsent(context.rootToLeafPathIndex, k -> Lists.newArrayList())
                .add(context);
    }

    private void selectPushDownTarget() {
        rewriteContextCandidates.forEach((origAggregator, pathToContexts) -> {
            // Select a proper context for each root-to-leaf path.
            pathToContexts.values().forEach(contexts -> {
                // The order of rewriteContextCandidates.contexts is from top to bottom,
                // and here we choose the first position that can be pushed down from bottom to top.
                for (int i = contexts.size() - 1; i >= 0; i--) {
                    AggregatePushDownContext context = contexts.get(i);
                    if (canPushDown(context.targetPosition, context)) {
                        addFinalContext(origAggregator, context);
                        break;
                    }
                }
            });
        });
    }

    private boolean canPushDown(OptExpression optExpression, AggregatePushDownContext context) {
        // least cross join/union/cte
        if (context.isEmpty() || context.pushPaths.isEmpty()) {
            return false;
        }

        if (context.aggregations.isEmpty() && context.groupBys.isEmpty()) {
            return false;
        }

        // distinct function, not support function can't push down
        if (context.aggregations.values().stream()
                .anyMatch(v -> v.isDistinct() || !WHITE_FNS.contains(v.getFnName()))) {
            return false;
        }

        ColumnRefSet outputColumns = optExpression.getOutputColumns();

        ColumnRefSet allGroupByColumns = new ColumnRefSet();
        context.groupBys.values().forEach(c -> allGroupByColumns.union(c.getUsedColumns()));

        for (int colId : allGroupByColumns.getColumnIds()) {
            ColumnRefOperator colRef = factory.getColumnRef(colId);
            if (colRef.getType() != null && !colRef.getType().canGroupBy()) {
                return false;
            }
        }

        ColumnRefSet allAggregateColumns = new ColumnRefSet();
        context.aggregations.values().forEach(c -> allAggregateColumns.union(c.getUsedColumns()));

        Preconditions.checkState(outputColumns.containsAll(allGroupByColumns));
        Preconditions.checkState(outputColumns.containsAll(allAggregateColumns));

        ExpressionContext expressionContext = new ExpressionContext(optExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext, factory, optimizerContext);
        statisticsCalculator.estimatorStats();

        if (!checkStatistics(context, allGroupByColumns, expressionContext.getStatistics())) {
            return false;
        }

        return true;
    }

}
