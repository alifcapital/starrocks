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
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.MonotonicFunctionRegistry;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

/**
 * Rewrites {@code f(x) cmp constant} into the equivalent predicate on {@code x} when the
 * registry has an exact preimage for {@code f}:
 * <pre>
 *   date_trunc('month', ts) >= '2024-03-01'  ->  ts >= '2024-03-01'
 * </pre>
 * A bare-column predicate is consumable by every pruning layer (range and list partitions,
 * lake planFiles, zone maps, bucket point lookups), while the on-expression form is not.
 * One nesting layer per application: the data child may itself be a call or a cast, and the
 * surrounding rewriter fixpoint unwinds the chain (casts reduce via ReduceCastRule).
 * <p>
 * The rewrite is an equivalence, NULL included (the inverse contract), so replacing the
 * predicate is valid under NOT/OR/select-list contexts. NE and null-safe-equal invert only
 * where the preimage is a single point of the column domain; each inverter decides.
 */
public class InvertMonotonicPredicateRule extends BottomUpScalarOperatorRewriteRule {

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        // NE and null-safe-equal reach the inverters too: they invert only where the
        // preimage is a single point of the column domain (each inverter decides)
        BinaryType cmp = predicate.getBinaryType();
        if (cmp != BinaryType.EQ && cmp != BinaryType.GE && cmp != BinaryType.GT
                && cmp != BinaryType.LE && cmp != BinaryType.LT
                && cmp != BinaryType.NE && cmp != BinaryType.EQ_FOR_NULL) {
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
        MonotonicFunctionRegistry.PredicateInverse inverse = MonotonicFunctionRegistry.exactInverse(call.getFnName());
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
        ScalarOperator dataChild = MonotonicFunctionRegistry.dataChildOf(call);
        if (dataChild == null) {
            return predicate;
        }
        return inverse.invert(call, dataChild, cmp, (ConstantOperator) predicate.getChild(1))
                .orElse(predicate);
    }
}
