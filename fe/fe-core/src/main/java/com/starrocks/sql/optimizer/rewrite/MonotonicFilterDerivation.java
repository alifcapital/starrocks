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

import java.util.ArrayList;
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
        ConnectContext context = ConnectContext.get();
        boolean useRegistry = context == null || context.getSessionVariable().isEnableMonotonicPredicateRewrite();
        List<ScalarOperator> originals = Utils.extractConjuncts(predicate);
        Set<ScalarOperator> conjuncts = null;
        boolean added = false;
        for (ScalarOperator original : originals) {
            // Do not descend into OR, NOT, IS NULL or other boolean/value expressions.
            Bound bound = derive(original, useRegistry);
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
                    // The original conjunct stays and decides the rows, and the bound only lets the scan skip files.
                    // A materialized view rewrite compares the original, so the bound is redundant. The statistics
                    // estimate the original through the same inverse, see columnBoundsForEstimate, so the bound is
                    // never estimated: otherwise the condition would count twice.
                    conjunct.setRedundant(true);
                    conjunct.setNotEvalEstimate(true);
                    if (conjuncts == null) {
                        conjuncts = new LinkedHashSet<>(originals);
                    }
                    added |= conjuncts.add(conjunct);
                }
            }
        }
        return added ? Utils.compoundAnd(conjuncts) : predicate;
    }

    /**
     * Bounds on columns that hold whenever a comparison of a function of a column with a constant is true, for the
     * estimation of this comparison. A column has min, max, NDV and maybe a histogram, while an expression of it often
     * has no statistics. Null when nothing is derived.
     * <p>
     * We expect this to run for every comparison that the statistics estimate, many times per query. It rejects a
     * comparison by its shape and by a lookup of the function name before it allocates, and it does not run the
     * scalar rewriter: a bound that is not already a comparison of a column with a constant is left out.
     * <p>
     * It does not depend on enable_monotonic_predicate_rewrite: that option decides whether a plan gets more
     * predicates, while this only changes how the statistics read the predicate that the plan already has.
     * enable_monotonic_predicate_estimate turns it off.
     */
    public static ColumnBounds columnBoundsForEstimate(BinaryPredicateOperator predicate) {
        if (!(predicate.getChild(0) instanceof CallOperator call)
                || !(predicate.getChild(1) instanceof ConstantOperator constant) || constant.isNull()) {
            return null;
        }
        if (!(call instanceof CastOperator)
                && MonotonicFunctionRegistry.filterInverse(call.getFnName()) == null
                && MonotonicFunctionRegistry.exactInverse(call.getFnName()) == null) {
            return null;
        }
        ConnectContext context = ConnectContext.get();
        if (context != null && !context.getSessionVariable().isEnableMonotonicPredicateEstimate()) {
            return null;
        }
        Bound bound = derive(predicate, true);
        if (bound.predicate() == predicate) {
            return null;
        }
        List<ScalarOperator> derived = Utils.extractConjuncts(bound.predicate());
        List<ScalarOperator> bounds = null;
        for (ScalarOperator conjunct : derived) {
            if (isColumnBound(conjunct) || isDateCastBound(conjunct)) {
                if (bounds == null) {
                    bounds = new ArrayList<>(derived.size());
                }
                bounds.add(conjunct);
            }
        }
        if (bounds == null) {
            return null;
        }
        // Dropping a derived conjunct that is not a column bound leaves a weaker condition.
        return new ColumnBounds(bounds, !bound.necessary() && bounds.size() == derived.size());
    }

    /**
     * Bounds on columns derived from one comparison. When exact, they hold exactly when the comparison is true;
     * otherwise they only follow from it, and an estimate by them is an upper bound of the rows.
     */
    public record ColumnBounds(List<ScalarOperator> bounds, boolean exact) {
    }

    // An inverse on a DATE column often compares the column cast to DATETIME, which the scan rewriter turns into a
    // comparison of the column. The statistics of a DATE column and of its cast to DATETIME are the same seconds,
    // so such a bound is estimated as well as one on the column, without running the rewriter.
    private static boolean isDateCastBound(ScalarOperator operator) {
        return operator instanceof BinaryPredicateOperator
                && operator.getChild(0) instanceof CastOperator cast
                && cast.getChild(0).isColumnRef() && cast.getChild(0).getType().isDate() && cast.getType().isDatetime()
                && operator.getChild(1).isConstantRef();
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

    private static Bound derive(ScalarOperator predicate, boolean useRegistry) {
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
        if (!useRegistry) {
            return none;
        }
        CallOperator call = (CallOperator) predicate.getChild(0);
        MonotonicFunctionRegistry.PredicateInverse inverse = MonotonicFunctionRegistry.filterInverse(call.getFnName());
        boolean necessary = inverse != null;
        if (inverse == null) {
            inverse = MonotonicFunctionRegistry.exactInverse(call.getFnName());
        }
        if (inverse == null) {
            return none;
        }
        ScalarOperator dataChild = MonotonicFunctionRegistry.dataChildOf(call);
        if (dataChild == null) {
            return none;
        }
        ScalarOperator bound = inverse.invert(call, dataChild,
                ((BinaryPredicateOperator) predicate).getBinaryType(), (ConstantOperator) predicate.getChild(1))
                .orElse(predicate);
        if (bound == predicate) {
            return none;
        }
        // Each inverse removes one function layer; recursively derive bounds through a chain.
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(bound);
        List<ScalarOperator> layers = new ArrayList<>(conjuncts.size());
        for (ScalarOperator conjunct : conjuncts) {
            Bound layer = derive(conjunct, useRegistry);
            layers.add(layer.predicate());
            necessary |= layer.necessary();
        }
        return new Bound(Utils.compoundAnd(layers), necessary);
    }
}
