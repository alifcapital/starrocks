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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.Map;

/**
 * Detects the cache-conscious top-n pattern and flags the global aggregation so
 * the backend runs the fused top-n aggregate:
 *
 *   ORDER BY count(*) DESC LIMIT k          (a PhysicalTopNOperator)
 *     [ -> merge exchange -> partial sort ] (transparent)
 *       -> global HashAggregate(count(*))
 *
 * The top-n context (order column + limit) is carried down through the
 * transparent merge/partial-sort nodes; when it reaches the matching global
 * aggregation the flag is set on that operator in place. Only count(*) is
 * recognized so far; everything else is left untouched, so a failed match
 * silently keeps the normal plan.
 */
public class MarkCacheConsciousTopnRule
        extends OptExpressionVisitor<OptExpression, MarkCacheConsciousTopnRule.TopnContext>
        implements TreeRewriteRule {

    /** The downstream top-n seen above the current node, or null if none applies here. */
    static final class TopnContext {
        final ColumnRefOperator orderColumn;
        final long limit;

        TopnContext(ColumnRefOperator orderColumn, long limit) {
            this.orderColumn = orderColumn;
            this.limit = limit;
        }
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        return root.getOp().accept(this, root, null);
    }

    // Generic node: not transparent to the top-n context, recurse with a cleared context.
    @Override
    public OptExpression visit(OptExpression optExpr, TopnContext context) {
        return recurse(optExpr, null);
    }

    @Override
    public OptExpression visitPhysicalTopN(OptExpression optExpr, TopnContext context) {
        PhysicalTopNOperator topN = (PhysicalTopNOperator) optExpr.getOp();
        return recurse(optExpr, buildContext(topN));
    }

    // Merge/gather exchange between the final top-n and the global aggregation does not
    // change the aggregate value, so it is transparent: forward the context unchanged.
    @Override
    public OptExpression visitPhysicalDistribution(OptExpression optExpr, TopnContext context) {
        return recurse(optExpr, context);
    }

    @Override
    public OptExpression visitPhysicalHashAggregate(OptExpression optExpr, TopnContext context) {
        PhysicalHashAggregateOperator agg = (PhysicalHashAggregateOperator) optExpr.getOp();
        if (context != null && matches(agg, context)) {
            agg.setCacheConsciousTopn(context.limit);
        }
        // The scan/join subtree below an aggregation never sees the agg value, clear context.
        return recurse(optExpr, null);
    }

    private OptExpression recurse(OptExpression optExpr, TopnContext context) {
        for (int i = 0; i < optExpr.arity(); ++i) {
            optExpr.setChild(i, optExpr.inputAt(i).getOp().accept(this, optExpr.inputAt(i), context));
        }
        return optExpr;
    }

    // Returns a context only for a real top-n (DESC, positive small LIMIT, no OFFSET, single key).
    private TopnContext buildContext(PhysicalTopNOperator topN) {
        if (topN.getOffset() != 0) {
            return null;
        }
        long limit = topN.getLimit();
        if (limit <= 0 || limit == Operator.DEFAULT_LIMIT) {
            return null;
        }
        List<Ordering> orderings = topN.getOrderSpec().getOrderDescs();
        if (orderings.size() != 1) {
            return null;
        }
        Ordering ordering = orderings.get(0);
        // count(*) is monotone increasing, so the top-n by count are the DESC head.
        if (ordering.isAscending()) {
            return null;
        }
        return new TopnContext(ordering.getColumnRef(), limit);
    }

    private boolean matches(PhysicalHashAggregateOperator agg, TopnContext context) {
        // Global (post-shuffle) or one-phase colocate global aggregation owns complete groups.
        if (!agg.getType().isGlobal()) {
            return false;
        }
        // HAVING, DISTINCT and sort-agg paths are out of scope for the fused operator.
        if (agg.getPredicate() != null || agg.hasRemovedDistinctFunc() || agg.isUseSortAgg()) {
            return false;
        }
        if (agg.getGroupBys() == null || agg.getGroupBys().isEmpty()) {
            return false;
        }
        Map<ColumnRefOperator, CallOperator> aggregations = agg.getAggregations();
        if (aggregations.size() != 1) {
            return false;
        }
        Map.Entry<ColumnRefOperator, CallOperator> entry = aggregations.entrySet().iterator().next();
        // The top-n must order by exactly this aggregation's output column.
        if (!entry.getKey().equals(context.orderColumn)) {
            return false;
        }
        CallOperator call = entry.getValue();
        return FunctionSet.COUNT.equals(call.getFnName()) && !call.isDistinct();
    }
}
