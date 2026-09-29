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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Adds necessary bounds for positive scan conjuncts without changing their SQL NULL semantics. */
public final class MonotonicFilterDerivation {
    private MonotonicFilterDerivation() {
    }

    public static ScalarOperator addScanBounds(ScalarOperator predicate) {
        if (predicate == null) {
            return predicate;
        }
        List<ScalarOperator> originals = Utils.extractConjuncts(predicate);
        Set<ScalarOperator> conjuncts = new LinkedHashSet<>(originals);
        for (ScalarOperator original : originals) {
            // Do not descend into OR, NOT, IS NULL or other boolean/value expressions.
            Bound bound = derive(original);
            if (bound.predicate() != original) {
                ScalarOperator rewritten = new ScalarOperatorRewriter().rewrite(bound.predicate(),
                        ScalarOperatorRewriter.DEFAULT_REWRITE_SCAN_PREDICATE_RULES);
                for (ScalarOperator conjunct : Utils.extractConjuncts(rewritten)) {
                    // We want bounds that let the scan skip files and row groups by min/max, which works only for a
                    // comparison of a column with a constant. A bound on an expression of a column skips nothing,
                    // and the scan would evaluate it on every row next to the original conjunct.
                    if (!isColumnBound(conjunct)) {
                        continue;
                    }
                    if (bound.necessary()) {
                        // A necessary bound only helps the scan to skip files, and the original conjunct stays
                        // and decides the rows. We mark the bound redundant and not estimated: we want the
                        // statistics to count the rows of the original once, and a materialized view rewrite
                        // must not require the bound from the view.
                        conjunct.setRedundant(true);
                        conjunct.setNotEvalEstimate(true);
                    }
                    conjuncts.add(conjunct);
                }
            }
        }
        return conjuncts.size() == new LinkedHashSet<>(originals).size()
                ? predicate : Utils.compoundAnd(conjuncts);
    }

    // A comparison of a column with a constant, or AND and OR of such comparisons
    private static boolean isColumnBound(ScalarOperator operator) {
        if (operator instanceof BinaryPredicateOperator) {
            return operator.getChild(0).isColumnRef() && operator.getChild(1).isConstantRef();
        }
        if (operator instanceof CompoundPredicateOperator compound && !compound.isNot()) {
            return compound.getChildren().stream().allMatch(MonotonicFilterDerivation::isColumnBound);
        }
        return false;
    }

    // A predicate derived from a conjunct. When a function on the way has no exact inverse, the predicate only follows
    // from the conjunct, and we call it necessary: the conjunct must stay next to it. Otherwise it is equivalent.
    private record Bound(ScalarOperator predicate, boolean necessary) {
    }

    private static Bound derive(ScalarOperator predicate) {
        Bound none = new Bound(predicate, false);
        if (!(predicate instanceof BinaryPredicateOperator)
                || !(predicate.getChild(0) instanceof CallOperator)
                || !(predicate.getChild(1) instanceof ConstantOperator)
                || ((ConstantOperator) predicate.getChild(1)).isNull()) {
            return none;
        }
        if (predicate.getChild(0) instanceof CastOperator) {
            ScalarOperator bound = StringDatePredicateDerivation.derive((BinaryPredicateOperator) predicate);
            return new Bound(bound, bound != predicate);
        }
        ConnectContext context = ConnectContext.get();
        if (context != null && !context.getSessionVariable().isEnableMonotonicPredicateRewrite()) {
            return none;
        }
        CallOperator call = (CallOperator) predicate.getChild(0);
        MonotonicFunctionRegistry.PredicateInverse inverse = MonotonicFunctionRegistry.filterInverse(call.getFnName());
        boolean necessary = inverse != null;
        if (inverse == null) {
            inverse = MonotonicFunctionRegistry.exactInverse(call.getFnName());
        }
        ScalarOperator dataChild = MonotonicFunctionRegistry.dataChildOf(call);
        if (inverse == null || dataChild == null) {
            return none;
        }
        ScalarOperator bound = inverse.invert(call, dataChild,
                ((BinaryPredicateOperator) predicate).getBinaryType(), (ConstantOperator) predicate.getChild(1))
                .orElse(predicate);
        if (bound == predicate) {
            return none;
        }
        // Each inverse removes one function layer; recursively derive bounds through a chain.
        List<Bound> layers = Utils.extractConjuncts(bound).stream().map(MonotonicFilterDerivation::derive).toList();
        return new Bound(Utils.compoundAnd(layers.stream().map(Bound::predicate).toList()),
                necessary || layers.stream().anyMatch(Bound::necessary));
    }
}
