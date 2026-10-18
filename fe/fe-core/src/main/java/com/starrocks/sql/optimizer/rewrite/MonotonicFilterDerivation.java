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

import com.starrocks.common.util.StringDateFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.CastStatisticsRules;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/** Adds necessary bounds for positive scan conjuncts without changing their SQL NULL semantics. */
public final class MonotonicFilterDerivation {
    private MonotonicFilterDerivation() {
    }

    /**
     * @param stringDateFormats the format a scan declares for a VARCHAR column, or null when it declares none
     */
    public static ScalarOperator addScanBounds(ScalarOperator predicate,
                                               Function<ColumnRefOperator, StringDateFormat> stringDateFormats) {
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
            ScalarOperator bound = derive(original, useRegistry, stringDateFormats).predicate();
            if (bound != original) {
                ScalarOperator rewritten = new ScalarOperatorRewriter().rewrite(bound,
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
     * estimation of this comparison. Null when nothing is derived.
     * <p>
     * We estimate the comparison by these bounds and not by the statistics of the function. The statistics of a
     * function are derived from the statistics of its column and keep less of them, often only min and max, while
     * the column has its NDV, most common values and histogram. Exact bounds admit more rows than the comparison only
     * where the function returns NULL, such as a shift past the last date, and the statistics of the function do
     * not show these rows either.
     * <p>
     * A comparison of a cast is not taken here: the statistics of the cast estimate it, see
     * ExpressionStatisticCalculator. A bound on the cast of a column that keeps the order, such as
     * CAST(ts AS DATETIME) >= c from hours_add(CAST(ts AS DATETIME), 5) >= c', counts as a bound on the column, see
     * CastStatisticsRules.boundsColumn, and the statistics of the cast estimate it in turn.
     * <p>
     * Some bounds are wider than the comparison, and then the caller also estimates the comparison on the function
     * and takes the lower estimate. A month shift of a DATETIME keeps the time of day and cuts the day to the end of
     * a shorter month, so its bound holds whole days: months_add(ts, 1) = '2025-03-15 10:00:00' bounds ts to all of
     * 2025-02-15, and months_add(ts, 1) < '2025-03-15 10:00:00' to the days before 2025-02-16. Only a lower bound
     * at midnight is exact. The rest of the day can hold a hot value of the column on the other side of the constant,
     * with many times the rows that the comparison keeps. The statistics of the shift carry the histogram of its
     * argument, see ExpressionStatisticCalculator, so the estimate on the function counts the hot value only when the
     * comparison keeps it. A month shift inside another function is not marked: the estimate on the outer function
     * may not see the hot value, and the lower estimate would drop rows that the comparison keeps. Bounds that leave
     * out a derived conjunct which is not a column bound are wider too.
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
        if (call instanceof CastOperator
                || (MonotonicFunctionRegistry.filterInverse(call.getFnName()) == null
                && MonotonicFunctionRegistry.exactInverse(call.getFnName()) == null)) {
            return null;
        }
        ConnectContext context = ConnectContext.get();
        if (context != null && !context.getSessionVariable().isEnableMonotonicPredicateEstimate()) {
            return null;
        }
        Bound bound = derive(predicate, true, column -> null);
        if (bound.predicate() == predicate) {
            return null;
        }
        List<ScalarOperator> derived = Utils.extractConjuncts(bound.predicate());
        List<ScalarOperator> bounds = null;
        for (ScalarOperator conjunct : derived) {
            if (isColumnBound(conjunct) || isCastBound(conjunct)) {
                if (bounds == null) {
                    bounds = new ArrayList<>(derived.size());
                }
                bounds.add(conjunct);
            }
        }
        if (bounds == null) {
            return null;
        }
        // A derived conjunct that is not a column bound is left out, so the bounds are a weaker condition.
        return new ColumnBounds(bounds, bound.wider() || bounds.size() != derived.size()
                || monthShiftHoldsMore(call, predicate.getBinaryType(), constant));
    }

    /**
     * Whether a comparison of a function of a column reduces to bounds on the column itself, for the statistics on
     * expressions, which leave such a condition to the statistics of the column, see ExpressionStatisticsMatcher.
     * Only a bound on the column, or on a DATE column cast to DATETIME, which has the statistics of the column,
     * counts: a bound on another cast of the column reads the statistics of the cast, and the statistics on the
     * expression can know more than they do.
     */
    public static boolean reducesToColumnBounds(BinaryPredicateOperator predicate) {
        if (!(predicate.getChild(0) instanceof CallOperator call)
                || !(predicate.getChild(1) instanceof ConstantOperator constant) || constant.isNull()) {
            return false;
        }
        if (!(call instanceof CastOperator)
                && MonotonicFunctionRegistry.filterInverse(call.getFnName()) == null
                && MonotonicFunctionRegistry.exactInverse(call.getFnName()) == null) {
            return false;
        }
        ConnectContext context = ConnectContext.get();
        if (context != null && !context.getSessionVariable().isEnableMonotonicPredicateEstimate()) {
            return false;
        }
        Bound bound = derive(predicate, true, column -> null);
        if (bound.predicate() == predicate) {
            return false;
        }
        for (ScalarOperator conjunct : Utils.extractConjuncts(bound.predicate())) {
            if (isColumnBound(conjunct) || isDateCastBound(conjunct)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDateCastBound(ScalarOperator operator) {
        return operator instanceof BinaryPredicateOperator
                && operator.getChild(0) instanceof CastOperator cast
                && cast.getChild(0).isColumnRef() && cast.getChild(0).getType().isDate() && cast.getType().isDatetime()
                && operator.getChild(1).isConstantRef();
    }

    // A month shift of anything but a date bounds only the date part, see MonotonicInverse.monthShift, which decides
    // the comparison only for a lower bound at midnight.
    private static boolean monthShiftHoldsMore(CallOperator call, BinaryType type, ConstantOperator constant) {
        if (!MonotonicFunctionRegistry.isMonthShift(call.getFnName())) {
            return false;
        }
        ScalarOperator dataChild = MonotonicFunctionRegistry.dataChildOf(call);
        if (dataChild == null || isDateInput(dataChild)) {
            return false;
        }
        return type != BinaryType.GE || !constant.getType().isDateType()
                || !constant.getDatetime().toLocalTime().equals(LocalTime.MIDNIGHT);
    }

    /**
     * Bounds on columns derived from one comparison. When wider, they admit rows that the comparison does not hold
     * besides the rows where the function returns NULL.
     */
    public record ColumnBounds(List<ScalarOperator> bounds, boolean wider) {
    }

    // An inverse often compares the column cast to another type, as a DATE column cast to DATETIME or a string of
    // timestamps cast to DATETIME. When the cast keeps the order, its statistics hold the range of the column mapped
    // through the cast, so such a bound is estimated as well as one on the column, without running the rewriter.
    private static boolean isCastBound(ScalarOperator operator) {
        return operator instanceof BinaryPredicateOperator
                && operator.getChild(0) instanceof CastOperator cast
                && cast.getChild(0).isColumnRef() && operator.getChild(1).isConstantRef()
                && CastStatisticsRules.boundsColumn(cast.getChild(0).getType(), cast.getType());
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

    // The bounds derived from a conjunct, or the conjunct itself when nothing is derived, and whether they are wider
    // than the conjunct, see columnBoundsForEstimate.
    private record Bound(ScalarOperator predicate, boolean wider) {
    }

    // stringDateFormats: the format of a VARCHAR column for bounds on the string under a cast to a date, see
    // StringDatePredicateDerivation. The scan takes them to skip files; the estimate does not need them.
    private static Bound derive(ScalarOperator predicate, boolean useRegistry,
                                Function<ColumnRefOperator, StringDateFormat> stringDateFormats) {
        Bound none = new Bound(predicate, false);
        if (!(predicate instanceof BinaryPredicateOperator)
                || !(predicate.getChild(0) instanceof CallOperator)
                || !(predicate.getChild(1) instanceof ConstantOperator)
                || ((ConstantOperator) predicate.getChild(1)).isNull()) {
            return none;
        }
        if (predicate.getChild(0) instanceof CastOperator) {
            ScalarOperator bound = StringDatePredicateDerivation.derive((BinaryPredicateOperator) predicate,
                    stringDateFormats);
            return new Bound(bound, bound != predicate);
        }
        if (!useRegistry) {
            return none;
        }
        CallOperator call = (CallOperator) predicate.getChild(0);
        MonotonicFunctionRegistry.PredicateInverse inverse = MonotonicFunctionRegistry.filterInverse(call.getFnName());
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
        boolean wider = false;
        // Each inverse removes one function layer; recursively derive bounds through a chain.
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(bound);
        List<ScalarOperator> layers = new ArrayList<>(conjuncts.size());
        for (ScalarOperator conjunct : conjuncts) {
            Bound layer = derive(conjunct, useRegistry, stringDateFormats);
            layers.add(layer.predicate());
            wider |= layer.wider();
        }
        return new Bound(Utils.compoundAnd(layers), wider);
    }

    // A DATE, or a DATE cast to DATETIME, as MonotonicInverse.monthShift reads its input
    private static boolean isDateInput(ScalarOperator dataChild) {
        return dataChild.getType().isDate()
                || (dataChild instanceof CastOperator && dataChild.getChild(0).getType().isDate());
    }
}
