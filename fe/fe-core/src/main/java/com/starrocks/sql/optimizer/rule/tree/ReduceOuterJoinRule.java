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

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Map;
import java.util.Set;

/**
 * Turns an outer join into an inner join when the plan above it drops every row that the join extends with NULLs.
 * <p>
 * We walk the plan from the top and carry the columns that the operators above can never see as NULL, because they
 * drop such rows: a filter or a predicate that rejects NULL in the column, an equal key of an inner join, and the
 * equal key of an outer join on its side that supplies NULLs, which drops its rows without a match. The set goes
 * down through a projection by the columns that it passes on, and through an aggregation by its grouping keys. When
 * the side of an outer join that supplies NULLs has a column of the set, no row of the join output keeps NULL there,
 * so no row that the join extends with NULLs survives, and the join is an inner join.
 * <p>
 * Example: credits LEFT JOIN (SELECT contract_id, SUM(amount) FROM balance LEFT JOIN contracts_of_accounts ...
 * GROUP BY contract_id) r ON credits.contract_id = r.contract_id. The rows of r with contract_id NULL find no match,
 * so contract_id is not NULL in r, it is a grouping key, so it is not NULL below the aggregation, and it comes from
 * the side of the inner LEFT JOIN that supplies NULLs: that join is an inner join.
 * <p>
 * The rule only changes join types, and adds no predicate: a predicate would change the estimates of the rows and
 * the rules that look at the predicates of a side, such as the pruning of a join by a unique key.
 * <p>
 * Nothing goes through an operator with a limit, because dropping rows below it changes which rows the limit keeps,
 * nor through the operators that we do not handle, such as a union, a window, a CTE or a repeat of grouping sets.
 */
public class ReduceOuterJoinRule implements TreeRewriteRule {
    private static final int MAX_FOLDED_NODES = 64;

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        if (!taskContext.getOptimizerContext().isEnableJoinIsNullPredicateDerive() || !hasOuterJoin(root)) {
            return root;
        }
        return visit(root, new ColumnRefSet());
    }

    // Most plans have no outer join, and the rule reads every predicate of the plan, so we skip them at once.
    private static boolean hasOuterJoin(OptExpression opt) {
        if (opt.getOp() instanceof LogicalJoinOperator) {
            JoinOperator type = ((LogicalJoinOperator) opt.getOp()).getJoinType();
            if (type.isLeftOuterJoin() || type.isRightOuterJoin()) {
                return true;
            }
        }
        for (OptExpression input : opt.getInputs()) {
            if (hasOuterJoin(input)) {
                return true;
            }
        }
        return false;
    }

    private OptExpression visit(OptExpression opt, ColumnRefSet nonNullAbove) {
        Operator op = opt.getOp();
        if (!op.isLogical()) {
            visitChildren(opt, new ColumnRefSet());
            return opt;
        }
        // The columns that are not NULL in the rows of the operator, before its projection.
        ColumnRefSet nonNull = new ColumnRefSet();
        if (op.getLimit() == Operator.DEFAULT_LIMIT) {
            Projection projection = op.getProjection();
            nonNull.union(projection == null ? nonNullAbove : throughProjection(projection.getColumnRefMap(),
                    nonNullAbove));
        }
        if (op.getPredicate() != null) {
            nonNull.union(rejectNull(op.getPredicate()));
        }

        if (op instanceof LogicalJoinOperator) {
            return visitJoin(opt, (LogicalJoinOperator) op, nonNull);
        }
        ColumnRefSet childNonNull = new ColumnRefSet();
        if (op instanceof LogicalFilterOperator) {
            childNonNull = nonNull;
        } else if (op instanceof LogicalProjectOperator) {
            childNonNull = throughProjection(((LogicalProjectOperator) op).getColumnRefMap(), nonNull);
        } else if (op instanceof LogicalAggregationOperator) {
            LogicalAggregationOperator aggregation = (LogicalAggregationOperator) op;
            if (!aggregation.getGroupingKeys().isEmpty()) {
                childNonNull = new ColumnRefSet(aggregation.getGroupingKeys());
                childNonNull.intersect(nonNull);
            }
        }
        visitChildren(opt, childNonNull);
        return opt;
    }

    private void visitChildren(OptExpression opt, ColumnRefSet nonNull) {
        for (int i = 0; i < opt.arity(); i++) {
            opt.setChild(i, visit(opt.inputAt(i), nonNull));
        }
    }

    private OptExpression visitJoin(OptExpression opt, LogicalJoinOperator join, ColumnRefSet nonNull) {
        ColumnRefSet leftColumns = opt.inputAt(0).getOutputColumns();
        ColumnRefSet rightColumns = opt.inputAt(1).getOutputColumns();
        JoinOperator type = join.getJoinType();
        if ((type.isLeftOuterJoin() && nonNull.isIntersect(rightColumns))
                || (type.isRightOuterJoin() && nonNull.isIntersect(leftColumns))) {
            type = JoinOperator.INNER_JOIN;
            join = new LogicalJoinOperator.Builder().withOperator(join).setJoinType(type).build();
            opt = OptExpression.create(join, opt.getInputs());
        }

        // The rows of a side that fail the ON clause find no match. Where the join drops such rows of a side, the
        // ON clause rejects NULL in that side as a filter on it would.
        ColumnRefSet leftOn = new ColumnRefSet();
        ColumnRefSet rightOn = new ColumnRefSet();
        if (join.getOnPredicate() != null) {
            for (ScalarOperator conjunct : Utils.extractConjuncts(join.getOnPredicate())) {
                if (conjunct instanceof BinaryPredicateOperator
                        && ((BinaryPredicateOperator) conjunct).getBinaryType() == BinaryType.EQ
                        && conjunct.getChild(0).isColumnRef() && conjunct.getChild(1).isColumnRef()) {
                    for (ScalarOperator key : conjunct.getChildren()) {
                        ColumnRefOperator column = (ColumnRefOperator) key;
                        (leftColumns.contains(column) ? leftOn : rightOn).union(column);
                    }
                    continue;
                }
                ColumnRefSet used = conjunct.getUsedColumns();
                if (leftColumns.containsAll(used)) {
                    leftOn.union(rejectNull(conjunct));
                } else if (rightColumns.containsAll(used)) {
                    rightOn.union(rejectNull(conjunct));
                }
            }
        }
        ColumnRefSet leftAbove = nonNull.clone();
        leftAbove.intersect(leftColumns);
        ColumnRefSet rightAbove = nonNull.clone();
        rightAbove.intersect(rightColumns);

        ColumnRefSet leftNonNull = new ColumnRefSet();
        ColumnRefSet rightNonNull = new ColumnRefSet();
        if (type.isInnerJoin() || type.isCrossJoin() || type.isLeftSemiJoin() || type.isRightSemiJoin()) {
            leftNonNull.union(leftAbove);
            leftNonNull.union(leftOn);
            rightNonNull.union(rightAbove);
            rightNonNull.union(rightOn);
        } else if (type.isLeftOuterJoin() || type == JoinOperator.LEFT_ANTI_JOIN) {
            // The left side keeps its rows without a match, so only the constraints above reach it.
            leftNonNull.union(leftAbove);
            rightNonNull.union(rightOn);
        } else if (type.isRightOuterJoin() || type.isRightAntiJoin()) {
            rightNonNull.union(rightAbove);
            leftNonNull.union(leftOn);
        } else if (type.isNullAwareLeftAntiJoin()) {
            // A NULL on the right side decides the result of NOT IN, so nothing reaches the right side.
            leftNonNull.union(leftAbove);
        }
        // A full outer join keeps the rows of both sides, and we leave the as-of joins alone.
        opt.setChild(0, visit(opt.inputAt(0), leftNonNull));
        opt.setChild(1, visit(opt.inputAt(1), rightNonNull));
        return opt;
    }

    private static ColumnRefSet throughProjection(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                                                  ColumnRefSet nonNull) {
        ColumnRefSet result = new ColumnRefSet();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
            if (nonNull.contains(entry.getKey()) && entry.getValue().isColumnRef()) {
                result.union((ColumnRefOperator) entry.getValue());
            }
        }
        return result;
    }

    // The columns such that a row with NULL in the column fails the predicate. The common shapes are read from
    // the operator: a comparison, an IN or a LIKE on a column, and IS NOT NULL, are NULL when the column is NULL.
    // Another conjunct is folded with the column set to NULL, which rewrites the whole conjunct per column, so we
    // fold only small conjuncts: a plan can carry predicates of thousands of nodes.
    private static ColumnRefSet rejectNull(ScalarOperator predicate) {
        ColumnRefSet result = new ColumnRefSet();
        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            if (conjunct instanceof BinaryPredicateOperator
                    && ((BinaryPredicateOperator) conjunct).getBinaryType() != BinaryType.EQ_FOR_NULL) {
                conjunct.getChildren().stream().filter(ScalarOperator::isColumnRef)
                        .forEach(child -> result.union((ColumnRefOperator) child));
            } else if ((conjunct instanceof InPredicateOperator && !((InPredicateOperator) conjunct).isSubquery())
                    || conjunct instanceof LikePredicateOperator
                    || (conjunct instanceof IsNullPredicateOperator && ((IsNullPredicateOperator) conjunct).isNotNull())) {
                if (conjunct.getChild(0).isColumnRef()) {
                    result.union((ColumnRefOperator) conjunct.getChild(0));
                }
            } else if (conjunct.getNumFlatChildren() <= MAX_FOLDED_NODES) {
                for (ColumnRefOperator column : Utils.extractColumnRef(conjunct)) {
                    if (!result.contains(column) && Utils.canEliminateNull(Set.of(column), conjunct)) {
                        result.union(column);
                    }
                }
            }
        }
        return result;
    }
}
