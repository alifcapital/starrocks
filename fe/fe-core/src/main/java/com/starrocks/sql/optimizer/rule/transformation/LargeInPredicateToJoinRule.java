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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRawValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LargeInConstants;
import com.starrocks.sql.optimizer.operator.scalar.LargeInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.sql.ast.HintNode.HINT_JOIN_BROADCAST;

/**
 * Transform large IN/NOT IN predicates to semi-join/anti-join for better performance.
 *
 * <p><b>Transformation:</b>
 *
 * <p>The rule runs after predicate pushdown, so a LargeInPredicate is where the InPredicate with the same list
 * would be: in the predicate of the scan that it filters, or of the lowest operator that it can be pushed to.
 *
 * <p>Before (IN):
 * <pre>
 *   Operator(predicate: col IN (v1, v2, ..., vN) AND rest, projection)
 *     |
 *   Child
 * </pre>
 *
 * <p>After (IN):
 * <pre>
 *   LeftSemiJoin(col = const_col, projection)
 *     |                      |
 *   Operator(predicate: rest)   RawValues(const_col: v1, v2, ..., vN)
 *     |
 *   Child
 * </pre>
 *
 * <p>NOT IN becomes a null-aware left anti join in the same way. NOT IN with a NULL constant is never true, so
 * the predicate of the operator becomes FALSE.
 *
 * <p><b>Transformation Restrictions:</b>
 * A LargeInPredicate is transformed only as a conjunct of the predicate of an operator: the join keeps the rows
 * for which it is true, and the predicate keeps the same rows. A LargeInPredicate anywhere else, inside another
 * expression of a predicate or in a projection, a join condition or an aggregation, cannot be transformed.
 *
 * <p><b>Exception Handling and Query Retry:</b>
 * When a LargeInPredicate cannot be transformed, the rule throws
 * {@link com.starrocks.sql.common.LargeInPredicateException}. The translator counts the LargeInPredicates in
 * ColumnRefFactory, and {@link #checkAllTransformed} throws it too when the rule transformed fewer. This exception
 * is caught by upper layers (StmtExecutor), which triggers a query retry from the parser stage with
 * {@code enable_large_in_predicate} disabled. The retry ensures the query executes via the normal IN predicate
 * path, guaranteeing correctness at the cost of potentially higher FE processing overhead.
 *
 * <p><b>Performance Benefits:</b>
 * For queries with extremely large IN lists (e.g., 100,000+ constants), this transformation significantly
 * reduces FE memory usage and planning time by using {@link com.starrocks.planner.RawValuesNode}
 * instead of creating individual expression nodes for each constant.
 *
 * @see com.starrocks.sql.ast.expression.LargeInPredicate
 * @see com.starrocks.planner.RawValuesNode
 * @see com.starrocks.sql.common.LargeInPredicateException
 */
public class LargeInPredicateToJoinRule extends TransformationRule {
    private int transformedCount = 0;

    public LargeInPredicateToJoinRule() {
        super(RuleType.TF_LARGE_IN_PREDICATE_TO_JOIN, Pattern.create(OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().enableLargeInPredicate() || input.getOp().getPredicate() == null) {
            return false;
        }

        boolean found = false;
        for (ScalarOperator conjunct : Utils.extractConjuncts(input.getOp().getPredicate())) {
            ScalarOperator rest = conjunct;
            if (conjunct instanceof LargeInPredicateOperator largeIn) {
                found = true;
                rest = largeIn.getCompareExpr();
            }
            if (containsLargeIn(rest)) {
                throw new LargeInPredicateException(
                        "LargeInPredicate is supported only as a conjunct of the predicate of an operator");
            }
        }
        return found;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        Operator op = input.getOp();
        LargeInPredicateOperator largeIn = null;
        List<ScalarOperator> remainingConjuncts = new ArrayList<>();
        for (ScalarOperator conjunct : Utils.extractConjuncts(op.getPredicate())) {
            if (largeIn == null && conjunct instanceof LargeInPredicateOperator) {
                largeIn = (LargeInPredicateOperator) conjunct;
            } else {
                remainingConjuncts.add(conjunct);
            }
        }
        transformedCount++;

        if (largeIn.isNotIn() && largeIn.getConstants().hasNull()) {
            Operator falseOp = OperatorBuilderFactory.build(op).withOperator(op)
                    .setPredicate(ConstantOperator.FALSE).build();
            return Lists.newArrayList(OptExpression.create(falseOp, input.getInputs()));
        }

        // We keep the other conjuncts on the operator under the join, so the rule finds the next LargeInPredicate
        // there. The operator applies its projection and limit after its predicate, so we move them to the join.
        OptExpression leftChild;
        if (op instanceof LogicalFilterOperator && remainingConjuncts.isEmpty()) {
            leftChild = input.inputAt(0);
        } else {
            Operator newOp = OperatorBuilderFactory.build(op).withOperator(op)
                    .setPredicate(Utils.compoundAnd(remainingConjuncts))
                    .setProjection(null)
                    .setLimit(Operator.DEFAULT_LIMIT)
                    .build();
            leftChild = OptExpression.create(newOp, input.getInputs());
        }

        LargeInConstants constants = largeIn.getConstants();
        ColumnRefOperator rightColumn = context.getColumnRefFactory().create("const_value", constants.getType(), false);
        LogicalRawValuesOperator rawValuesOp = new LogicalRawValuesOperator(Lists.newArrayList(rightColumn),
                constants.getType(), largeIn.getRawText(), constants.getValues(), constants.getValues().size());

        ScalarOperator leftExpression = largeIn.getCompareExpr();
        if (!leftExpression.getType().matchesType(rightColumn.getType())) {
            leftExpression = new CastOperator(rightColumn.getType(), leftExpression, true);
        }
        BinaryPredicateOperator joinPredicate = new BinaryPredicateOperator(BinaryType.EQ, leftExpression, rightColumn);

        JoinOperator joinType = largeIn.isNotIn() ? JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN : JoinOperator.LEFT_SEMI_JOIN;
        LogicalJoinOperator joinOp = new LogicalJoinOperator.Builder()
                .setJoinType(joinType)
                .setJoinHint(HINT_JOIN_BROADCAST)
                .setOnPredicate(joinPredicate)
                .setProjection(op.getProjection())
                .setLimit(op.getLimit())
                .build();

        return Lists.newArrayList(OptExpression.create(joinOp, leftChild, OptExpression.create(rawValuesOp)));
    }

    /**
     * Throws LargeInPredicateException if the rule transformed fewer LargeInPredicates than the translator created
     * for the plan: the others are in places that the rule does not transform.
     */
    public void checkAllTransformed(OptimizerContext context) {
        int created = context.getColumnRefFactory().getLargeInPredicateCount();
        if (transformedCount < created) {
            throw new LargeInPredicateException(
                    "LargeInPredicate is supported only as a conjunct of the predicate of an operator, " +
                            "transformed %s of %s", transformedCount, created);
        }
    }

    private static boolean containsLargeIn(ScalarOperator operator) {
        if (operator instanceof LargeInPredicateOperator) {
            return true;
        }
        for (ScalarOperator child : operator.getChildren()) {
            if (containsLargeIn(child)) {
                return true;
            }
        }
        return false;
    }
}
