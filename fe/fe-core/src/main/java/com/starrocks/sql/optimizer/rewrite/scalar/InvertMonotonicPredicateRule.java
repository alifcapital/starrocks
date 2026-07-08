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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.MonotonicFunctionRegistry;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Rewrites {@code f(x) cmp constant} into the equivalent predicate on {@code x} when the
 * registry has an exact preimage for {@code f}:
 * <pre>
 *   date_trunc('month', ts) >= '2024-03-01'  ->  ts >= '2024-03-01'
 *   days_add(ts, 3) = '2024-03-08'           ->  ts = '2024-03-05'
 * </pre>
 * A bare-column predicate is consumable by every pruning layer (range and list partitions,
 * lake planFiles, zone maps, bucket point lookups), while the on-expression form is not.
 * One nesting layer per application: the data child may itself be a call or a cast, and the
 * surrounding rewriter fixpoint unwinds the chain (casts reduce via ReduceCastRule).
 * <p>
 * The rewrite is an equivalence, NULL included (the inverse contract), so replacing the
 * predicate is valid under NOT/OR/select-list contexts. NE and null-safe-equal never match.
 */
public class InvertMonotonicPredicateRule extends BottomUpScalarOperatorRewriteRule {

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        BinaryType cmp = predicate.getBinaryType();
        if (cmp != BinaryType.EQ && cmp != BinaryType.GE && cmp != BinaryType.GT
                && cmp != BinaryType.LE && cmp != BinaryType.LT) {
            return predicate;
        }
        ScalarOperator left = predicate.getChild(0);
        // CastOperator extends CallOperator; casts belong to ReduceCastRule
        if (!(left instanceof CallOperator) || left instanceof CastOperator) {
            return predicate;
        }
        if (!(predicate.getChild(1) instanceof ConstantOperator)
                || ((ConstantOperator) predicate.getChild(1)).isNull()) {
            return predicate;
        }
        CallOperator call = (CallOperator) left;
        MonotonicFunctionRegistry.ExactInverse inverse = MonotonicFunctionRegistry.exactInverse(call.getFnName());
        if (inverse == null) {
            return predicate;
        }
        // background threads (alter jobs, load, partition TTL) normalize expressions with no
        // connect context; the rewrite is an equivalence, so it stays on there
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null
                && !connectContext.getSessionVariable().isEnableMonotonicPredicateRewrite()) {
            return predicate;
        }
        ScalarOperator dataChild = dataChildOf(call);
        if (dataChild == null) {
            return predicate;
        }
        return inverse.invert(call, dataChild, cmp, (ConstantOperator) predicate.getChild(1))
                .orElse(predicate);
    }

    /**
     * The single non-constant argument, which must sit in a registry-admitted position while
     * every other argument is a non-NULL constant; null when the call has no such shape.
     * Exactly-one-column discipline as in the image direction: two occurrences of the column
     * break the single-chain monotonicity argument.
     */
    private ScalarOperator dataChildOf(CallOperator call) {
        Set<Integer> admitted = MonotonicFunctionRegistry.dataArgPositions(call.getFnName());
        if (admitted == null) {
            return null;
        }
        List<ColumnRefOperator> usedColumns = call.getColumnRefs();
        if (new HashSet<>(usedColumns).size() != 1 || usedColumns.size() != 1) {
            return null;
        }
        ScalarOperator dataChild = null;
        for (int i = 0; i < call.getChildren().size(); i++) {
            ScalarOperator child = call.getChild(i);
            if (child.isConstantRef()) {
                if (((ConstantOperator) child).isNull()) {
                    return null;
                }
                continue;
            }
            if (!admitted.contains(i) || dataChild != null) {
                return null;
            }
            dataChild = child;
        }
        return dataChild;
    }
}
