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

package com.starrocks.sql.optimizer.rule.join;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.LogicalPropertyContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.sql.optimizer.statistics.StatisticsCalcUtils.ensureStatistics;

public class ReorderJoinRule extends Rule {
    // Tests switch it off to compare against the passes that are skipped.
    boolean skipRepeatedPasses = true;
    int skippedRegions = 0;

    public ReorderJoinRule() {
        super(RuleType.TF_MULTI_JOIN_ORDER, Pattern.create(OperatorType.PATTERN));
    }

    private void extractRootInnerJoin(OptExpression parent, int childIdx, OptExpression root,
                                      List<Pair<OptExpression, Pair<OptExpression, Integer>>> results,
                                      boolean findNewRoot) {
        Operator operator = root.getOp();
        if (operator instanceof LogicalJoinOperator) {
            // If the user specifies joinHint, then no reorder
            if (!((LogicalJoinOperator) operator).getJoinHint().isEmpty()) {
                return;
            }
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            // For A inner join (B inner join C), we only think A is root tree
            if (joinOperator.isInnerOrCrossJoin()) {
                boolean hasProjectRelyOnTwoChildren = MultiJoinNode.hasProjectRelyOnTwoChildren(root);
                // if findNewRoot == true && hasProjectRelyOnTwoChildren
                // like below:
                //      B
                //    /   \
                //   A     C
                //        /  \
                //       D    E
                // if C has project rely on two child, then C is an atom for join tree with B as root,
                // but we still can reorder join tree whose root is C
                if (!findNewRoot || hasProjectRelyOnTwoChildren) {
                    findNewRoot = true;
                    results.add(Pair.create(root, Pair.create(parent, childIdx)));
                }
            } else {
                findNewRoot = false;
            }
        } else {
            findNewRoot = false;
        }

        for (int i = 0; i < root.getInputs().size(); ++i) {
            OptExpression child = root.inputAt(i);
            extractRootInnerJoin(root, i, child, results, findNewRoot);
        }
    }

    /**
     * A region of two atoms has one join, and the passes can differ only in which child goes left. We expect the
     * passes after the first one to build the join that the first one built, so we skip them when:
     * - the row counts differ, because {@link JoinOrder#buildJoinExpr} puts the child with more rows on the left
     *   whatever the order of its arguments;
     * - no predicate uses a column that is computed in the region, because only such a column makes buildJoinExpr
     *   push an expression into a child, and which child gets it depends on the order of the arguments;
     * - the join reorder with unique and foreign keys is off, because it decides the order by more than row counts.
     * The row counts are read from the atoms, which the first pass has given statistics.
     */
    // We keep DP and greedy for every region whose scans have statistics. When the scans lack some, we turn DP and
    // greedy off only if a column read by a join condition has no statistic in its atom: a missing statistic of a
    // column no join reads, or a scan whose flag was never set because its atom already had statistics, does not
    // change the join cardinalities.
    static boolean hasUnknownStatistics(OptExpression root, MultiJoinNode multiJoinNode, OptimizerContext context) {
        return Utils.hasUnknownColumnsStats(root) && multiJoinNode.hasUnknownJoinColumnStatistics(context);
    }

    private boolean hasSingleJoinOrder(OptimizerContext context, MultiJoinNode multiJoinNode) {
        ConnectContext connectContext = ConnectContext.get();
        if (!skipRepeatedPasses || multiJoinNode.getAtoms().size() != 2
                || context.getSessionVariable().isEnableUKFKJoinReorder()
                || connectContext == null || connectContext.getSessionVariable().isEnableUKFKJoinReorder()) {
            return false;
        }
        Map<ColumnRefOperator, ScalarOperator> expressions = multiJoinNode.getExpressionMap();
        if (!expressions.isEmpty()) {
            ColumnRefSet computed = new ColumnRefSet(expressions.keySet());
            for (ScalarOperator predicate : multiJoinNode.getPredicates()) {
                if (computed.isIntersect(predicate.getUsedColumns())) {
                    return false;
                }
            }
        }
        Iterator<OptExpression> atoms = multiJoinNode.getAtoms().iterator();
        Statistics first = atoms.next().getStatistics();
        Statistics second = atoms.next().getStatistics();
        if (first == null || second == null) {
            return false;
        }
        double firstRows = first.getOutputRowCount();
        double secondRows = second.getOutputRowCount();
        return firstRows < secondRows || secondRows < firstRows;
    }

    private static boolean isRepeatedAfterLeftDeep(JoinOrder algorithm) {
        return algorithm instanceof JoinReorderDP || algorithm.getClass() == JoinReorderGreedy.class;
    }

    Optional<OptExpression> enumerate(JoinOrder reorderAlgorithm, OptimizerContext context, OptExpression innerJoinRoot,
                                      MultiJoinNode multiJoinNode, boolean copyIntoMemo) {
        try (Timer ignore = Tracers.watchScope(Tracers.Module.OPTIMIZER, reorderAlgorithm.getClass().getSimpleName())) {
            reorderAlgorithm.reorder(Lists.newArrayList(multiJoinNode.getAtoms()),
                    multiJoinNode.getPredicates(), multiJoinNode.getExpressionMap());
        }

        List<OptExpression> reorderTopKResult = reorderAlgorithm.getResult();
        LogicalJoinOperator oldRoot = (LogicalJoinOperator) innerJoinRoot.getOp();

        // Set limit to top join if needed
        if (oldRoot.hasLimit()) {
            for (OptExpression joinExpr : reorderTopKResult) {
                joinExpr.getOp().setLimit(oldRoot.getLimit());
            }
        }

        OutputColumnsPrune prune = new OutputColumnsPrune(context);
        for (OptExpression joinExpr : reorderTopKResult) {
            ColumnRefSet outputColumns = new ColumnRefSet();
            Map<ColumnRefOperator, ScalarOperator> projectMap = new HashMap<>();
            if (oldRoot.getProjection() == null) {
                innerJoinRoot.getInputs().forEach(opt -> outputColumns.union(opt.getOutputColumns()));

                projectMap.putAll(context.getColumnRefFactory().getIdentityColumnRefMap(outputColumns));
            } else {
                outputColumns.union(oldRoot.getProjection().getOutputColumns());
                projectMap.putAll(oldRoot.getProjection().getColumnRefMap());
            }

            ColumnRefSet newRootInputColumns = new ColumnRefSet();
            joinExpr.getInputs().forEach(opt -> newRootInputColumns.union(opt.getOutputColumns()));
            ColumnRefSet expressionKeys = new ColumnRefSet(multiJoinNode.getExpressionMap().keySet());
            ReplaceColumnRefRewriter replaceColumnRefRewriter = null;
            for (int id : outputColumns.getColumnIds()) {
                // If the ColumnRef contained in the output before reorder does not exist after reorder.
                // Explain that this is an expression that is not referenced by onPredicate,
                // but the upstream node does depend on this input,
                // so we need to restore this expression at this position
                if (!newRootInputColumns.contains(id) && expressionKeys.contains(id)) {
                    ScalarOperator scalarOperator =
                            multiJoinNode.getExpressionMap().get(context.getColumnRefFactory().getColumnRef(id));
                    // The expression map in multiJoinNode could have map like this :
                    //  21 -> cast(20 as varchar)
                    //  20 -> constant operator
                    // The expression map key 21 should use replaceColumnRewriter to rewrite the value instead of use
                    // its value cast operator directly.
                    if (replaceColumnRefRewriter == null) {
                        replaceColumnRefRewriter = new ReplaceColumnRefRewriter(multiJoinNode.getExpressionMap(), true);
                    }
                    projectMap.put(context.getColumnRefFactory().getColumnRef(id),
                            replaceColumnRefRewriter.rewrite(scalarOperator));
                }
            }
            joinExpr.getOp().setProjection(new Projection(projectMap));
            ColumnRefSet requireInputColumns = ((LogicalJoinOperator) joinExpr.getOp()).getRequiredChildInputColumns();
            requireInputColumns.union(outputColumns);

            for (int i = 0; i < joinExpr.arity(); ++i) {
                OptExpression optExpression = prune.rewrite(joinExpr.inputAt(i), requireInputColumns);
                joinExpr.setChild(i, optExpression);
            }

            joinExpr = new RemoveDuplicateProject(context).rewrite(joinExpr);
            if (copyIntoMemo) {
                context.getMemo().copyIn(innerJoinRoot.getGroupExpression().getGroup(), joinExpr);
            } else {
                joinExpr.deriveLogicalPropertyItself();
                ExpressionContext expressionContext = new ExpressionContext(joinExpr);
                StatisticsCalculator statisticsCalculator =
                        new StatisticsCalculator(expressionContext, context.getColumnRefFactory(), context);
                statisticsCalculator.estimatorStats();
                joinExpr.setStatistics(expressionContext.getStatistics());
                return Optional.of(joinExpr);
            }
        }
        return Optional.empty();
    }

    // This method is only called in RBO phase, so it return the rewritten plan instead of copying its into memo,
    // it adopts JoinReorderCardinalityPreserving algorithm to reorder multi-joins to adapt to table pruning.
    public OptExpression rewrite(OptExpression input, OptimizerContext context) {
        return rewrite(input, JoinReorderFactory.createJoinReorderCardinalityPreserving(), context);
    }

    public OptExpression rewriteForDistinctJoin(OptExpression input, OptimizerContext context) {
        return rewrite(input, JoinReorderFactory.createJoinReorderDrivingTable(), context);
    }

    public OptExpression rewrite(OptExpression input, JoinReorderFactory joinReorderFactory, OptimizerContext context) {
        List<Pair<OptExpression, Pair<OptExpression, Integer>>> innerJoinTreesAndParents = Lists.newArrayList();
        extractRootInnerJoin(null, -1, input, innerJoinTreesAndParents, false);
        if (!innerJoinTreesAndParents.isEmpty()) {
            // In order to reorder the bottom join tree firstly
            Collections.reverse(innerJoinTreesAndParents);
            for (Pair<OptExpression, Pair<OptExpression, Integer>> innerJoinRoot : innerJoinTreesAndParents) {
                OptExpression child = innerJoinRoot.first;
                OptExpression parent = innerJoinRoot.second.first;
                Integer childIdx = innerJoinRoot.second.second;

                MultiJoinNode multiJoinNode = MultiJoinNode.toMultiJoinNode(child);
                if (!multiJoinNode.checkDependsPredicate()) {
                    continue;
                }

                List<JoinOrder> orderAlgorithms = joinReorderFactory.create(context, multiJoinNode);
                Optional<OptExpression> newChild = Optional.empty();
                Boolean unknownStatistics = null;
                for (int i = 0; i < orderAlgorithms.size(); ++i) {
                    JoinOrder orderAlgorithm = orderAlgorithms.get(i);
                    newChild = enumerate(orderAlgorithm, context, child, multiJoinNode, false);
                    if (newChild.isEmpty()) {
                        break;
                    }
                    // If there is no statistical information, the DP and greedy reorder algorithm are disabled,
                    // and the query plan degenerates to the left deep tree
                    if (unknownStatistics == null) {
                        unknownStatistics = hasUnknownStatistics(innerJoinRoot.first, multiJoinNode, context);
                    }
                    if (unknownStatistics &&
                            (!FeConstants.runningUnitTest || FeConstants.isReplayFromQueryDump)) {
                        break;
                    }
                    if (orderAlgorithm.getClass() == JoinReorderLeftDeep.class && i + 1 < orderAlgorithms.size()
                            && orderAlgorithms.subList(i + 1, orderAlgorithms.size()).stream()
                            .allMatch(ReorderJoinRule::isRepeatedAfterLeftDeep)
                            && hasSingleJoinOrder(context, multiJoinNode)) {
                        ++skippedRegions;
                        break;
                    }
                }

                if (newChild.isPresent()) {
                    int prevNumCrossJoins =
                            Utils.countJoinNodeSize(child, Sets.newHashSet(JoinOperator.CROSS_JOIN));
                    int numCrossJoins =
                            Utils.countJoinNodeSize(newChild.get(), Sets.newHashSet(JoinOperator.CROSS_JOIN));
                    // we adopt result of reorder only if the number of cross joins is reduced
                    if (numCrossJoins != 0 && prevNumCrossJoins <= numCrossJoins) {
                        continue;
                    }
                    if (parent != null) {
                        parent.setChild(childIdx, newChild.get());
                    } else {
                        return newChild.get();
                    }
                }
            }
        }
        return input;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        List<Pair<OptExpression, Pair<OptExpression, Integer>>> innerJoinTreesAndParents = Lists.newArrayList();
        extractRootInnerJoin(null, -1, input, innerJoinTreesAndParents, false);
        if (!innerJoinTreesAndParents.isEmpty()) {
            // In order to reorder the bottom join tree firstly
            for (int i = innerJoinTreesAndParents.size() - 1; i >= 0; --i) {
                OptExpression innerJoinRoot = innerJoinTreesAndParents.get(i).first;
                MultiJoinNode multiJoinNode = MultiJoinNode.toMultiJoinNode(innerJoinRoot);
                if (!multiJoinNode.checkDependsPredicate()) {
                    continue;
                }
                enumerate(new JoinReorderLeftDeep(context), context, innerJoinRoot, multiJoinNode, true);
                // If there is no statistical information, the DP and greedy reorder algorithm are disabled,
                // and the query plan degenerates to the left deep tree
                if (hasUnknownStatistics(innerJoinRoot, multiJoinNode, context) &&
                        (!FeConstants.runningUnitTest || FeConstants.isReplayFromQueryDump)) {
                    continue;
                }
                if (hasSingleJoinOrder(context, multiJoinNode)) {
                    ++skippedRegions;
                    continue;
                }
                int atomSize = multiJoinNode.getAtoms().size();
                // Hard cap DP join reorder to avoid pathological cases.
                // DP enumerates bipartitions (exponential); also the subset enumeration uses a long mask.
                if (atomSize <= 62 && atomSize <= context.getSessionVariable().getCboMaxReorderNodeUseDP()
                        && context.getSessionVariable().isCboEnableDPJoinReorder()) {
                    // 10 table join reorder takes more than 100ms,
                    // so the join reorder using dp is currently controlled below 10.
                    enumerate(new JoinReorderDP(context), context, innerJoinRoot, multiJoinNode, true);
                }

                if (context.getSessionVariable().isCboEnableGreedyJoinReorder() &&
                        multiJoinNode.getAtoms().size() <= context.getSessionVariable().getCboMaxReorderNodeUseGreedy()) {
                    enumerate(new JoinReorderGreedy(context), context, innerJoinRoot, multiJoinNode, true);
                }
            }
        }
        return Collections.emptyList();
    }

    /**
     * Because the order of Join has changed,
     * the outputColumns of Join will also change accordingly.
     * Here we need to perform column cropping again on Join.
     */
    public static class OutputColumnsPrune extends OptExpressionVisitor<OptExpression, ColumnRefSet> {
        private final OptimizerContext optimizerContext;

        public OutputColumnsPrune(OptimizerContext optimizerContext) {
            this.optimizerContext = optimizerContext;
        }

        public OptExpression rewrite(OptExpression optExpression, ColumnRefSet requiredColumns) {
            Operator operator = optExpression.getOp();
            // The operator consumes the columns referenced by its own predicate (the filter is applied
            // before the projection). Treat them as required so projection pruning below cannot drop a
            // pass-through column that the predicate still references -- otherwise the rebuilt statistics
            // would lack that column and statistics estimation throws "missing statistic of col".
            if (operator.getPredicate() != null) {
                operator.getPredicate().collectUsedColumns(requiredColumns);
            }
            if (operator.getProjection() != null) {
                Projection projection = operator.getProjection();

                Map<ColumnRefOperator, ScalarOperator> columnRefMap = projection.getColumnRefMap();
                int retained = 0;
                for (ColumnRefOperator key : columnRefMap.keySet()) {
                    if (requiredColumns.contains(key)) {
                        retained++;
                    }
                }

                Map<ColumnRefOperator, ScalarOperator> newOutputProjections = null;
                if (retained == 0) {
                    ColumnRefOperator smallest = Utils.findSmallestColumnRef(projection.getOutputColumns());
                    if (columnRefMap.size() != 1) {
                        newOutputProjections = Maps.newHashMap();
                        newOutputProjections.put(smallest, columnRefMap.get(smallest));
                    }
                } else if (retained != columnRefMap.size()) {
                    newOutputProjections = Maps.newHashMap();
                    for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
                        if (requiredColumns.contains(entry.getKey())) {
                            newOutputProjections.put(entry.getKey(), entry.getValue());
                        }
                    }
                }
                if (newOutputProjections != null) {
                    optExpression = deriveNewOptExpression(optExpression, newOutputProjections);
                }

                for (ScalarOperator value : optExpression.getOp().getProjection().getColumnRefMap().values()) {
                    value.collectUsedColumns(requiredColumns);
                }
            }

            return optExpression.getOp().accept(this, optExpression, requiredColumns);
        }

        @Override
        public OptExpression visit(OptExpression optExpression, ColumnRefSet pruneOutputColumns) {
            return optExpression;
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression optExpression, ColumnRefSet requireColumns) {
            // use children output columns as join output columns.
            ColumnRefSet newOutputColumns = optExpression.inputAt(0).getOutputColumns().clone();
            newOutputColumns.union(optExpression.inputAt(1).getOutputColumns());
            newOutputColumns.intersect(requireColumns);

            LogicalJoinOperator joinOperator = (LogicalJoinOperator) optExpression.getOp();
            if (joinOperator.getProjection() == null && !newOutputColumns.isEmpty()) {
                joinOperator = new LogicalJoinOperator.Builder()
                        .withOperator((LogicalJoinOperator) optExpression.getOp())
                        .setProjection(new Projection(
                                optimizerContext.getColumnRefFactory().getIdentityColumnRefMap(newOutputColumns)))
                        .build();
            }

            requireColumns = ((LogicalJoinOperator) optExpression.getOp()).getRequiredChildInputColumns();
            requireColumns.union(newOutputColumns);
            OptExpression left = rewrite(optExpression.inputAt(0), requireColumns.clone());
            OptExpression right = rewrite(optExpression.inputAt(1), requireColumns);
            ensureStatistics(left, optimizerContext);
            ensureStatistics(right, optimizerContext);

            OptExpression joinOpt = OptExpression.create(joinOperator, Lists.newArrayList(left, right));
            joinOpt.deriveLogicalPropertyItself();

            ExpressionContext expressionContext = new ExpressionContext(joinOpt);
            StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                    expressionContext, optimizerContext.getColumnRefFactory(), optimizerContext);
            statisticsCalculator.estimatorStats();
            joinOpt.setStatistics(expressionContext.getStatistics());
            return joinOpt;
        }

        private OptExpression deriveNewOptExpression(OptExpression optExpression,
                                                     Map<ColumnRefOperator, ScalarOperator> newOutputProjections) {
            Operator operator = optExpression.getOp();
            ColumnRefSet newCols = new ColumnRefSet(newOutputProjections.keySet());
            LogicalProperty newProperty = new LogicalProperty(optExpression.getLogicalProperty());
            newProperty.setOutputColumns(newCols);

            if (!Optional.ofNullable(optExpression.getStatistics()).isPresent()) {
                ExpressionContext expressionContext = new ExpressionContext(optExpression);
                StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                        expressionContext, optimizerContext.getColumnRefFactory(), optimizerContext);
                statisticsCalculator.estimatorStats();
                optExpression.setStatistics(expressionContext.getStatistics());
            }
            Preconditions.checkState(optExpression.getStatistics() != null);
            Statistics oldStats = optExpression.getStatistics();
            Statistics.Builder newStatsBuilder = Statistics.builder()
                    .setOutputRowCount(oldStats.getOutputRowCount())
                    .setTableRowCountMayInaccurate(oldStats.isTableRowCountMayInaccurate())
                    .setShadowColumns(oldStats.getShadowColumns())
                    .setStatsSource(oldStats.getStatsSource())
                    .setPartitionRestricted(oldStats.isPartitionRestricted())
                    .addMultiColumnStatistics(oldStats.getMultiColumnCombinedStats());
            oldStats.getColumnStatistics().forEach((col, stat) -> {
                if (newCols.contains(col)) {
                    newStatsBuilder.addColumnStatistic(col, stat);
                }
            });
            Statistics newStats = newStatsBuilder.build();
            Operator.Builder builder = OperatorBuilderFactory.build(operator);
            Operator newOp = builder.withOperator(operator)
                    .setProjection(new Projection(newOutputProjections))
                    .build();

            OptExpression newOpt = OptExpression.create(newOp, optExpression.getInputs());
            newOpt.setLogicalProperty(newProperty);
            newOpt.setStatistics(newStats);
            return newOpt;
        }
    }

    public static class RemoveDuplicateProject extends OptExpressionVisitor<OptExpression, Void> {
        private final OptimizerContext optimizerContext;

        public RemoveDuplicateProject(OptimizerContext optimizerContext) {
            this.optimizerContext = optimizerContext;
        }

        public OptExpression rewrite(OptExpression optExpression) {
            Operator operator = optExpression.getOp();
            if (operator.getProjection() != null) {
                Projection projection = operator.getProjection();

                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                    if (!entry.getValue().isColumnRef()) {
                        return optExpression;
                    }

                    if (!entry.getKey().equals(entry.getValue())) {
                        return optExpression;
                    }
                }
            }

            return optExpression.getOp().accept(this, optExpression, null);
        }

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            return optExpression;
        }

        @Override
        public OptExpression visitLogicalJoin(OptExpression optExpression, Void context) {
            ColumnRefSet childInputColumns = new ColumnRefSet();
            optExpression.getInputs().forEach(opt -> childInputColumns.union(
                    ((LogicalOperator) opt.getOp()).getOutputColumns(LogicalPropertyContext.of(opt))));

            OptExpression left = rewrite(optExpression.inputAt(0));
            OptExpression right = rewrite(optExpression.inputAt(1));
            ensureStatistics(left, optimizerContext);
            ensureStatistics(right, optimizerContext);

            ColumnRefSet outputColumns = new ColumnRefSet();
            if (optExpression.getOp().getProjection() != null) {
                outputColumns = new ColumnRefSet(optExpression.getOp().getProjection().getOutputColumns());
            }

            if (childInputColumns.equals(outputColumns)) {
                LogicalJoinOperator joinOperator = new LogicalJoinOperator.Builder().withOperator(
                                (LogicalJoinOperator) optExpression.getOp())
                        .setProjection(null).build();
                OptExpression joinOpt = OptExpression.create(joinOperator, Lists.newArrayList(left, right));
                joinOpt.deriveLogicalPropertyItself();

                ExpressionContext expressionContext = new ExpressionContext(joinOpt);
                StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                        expressionContext, optimizerContext.getColumnRefFactory(), optimizerContext);
                statisticsCalculator.estimatorStats();
                joinOpt.setStatistics(expressionContext.getStatistics());
                return joinOpt;
            } else {
                optExpression.setChild(0, left);
                optExpression.setChild(1, right);
                return optExpression;
            }
        }
    }
}
