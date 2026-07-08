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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Function;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.common.TimeUnitUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.Set;

/**
 * {@link MonotonicFunctionRegistry.ExactInverse} implementations. An exact inverse turns
 * {@code f(x) cmp value} into an equivalent predicate on {@code x} itself: equivalent on
 * non-NULL input and NULL-on-NULL, so the result may replace the original under any boolean
 * context. Anything short of that equivalence must refuse.
 */
public final class MonotonicInverse {

    private MonotonicInverse() {
    }

    private static final Set<String> DATE_ONLY_UNITS = Set.of(TimeUnitUtils.DAY, TimeUnitUtils.WEEK,
            TimeUnitUtils.MONTH, TimeUnitUtils.QUARTER, TimeUnitUtils.YEAR);

    /**
     * date_trunc(unit, x) cmp value inverts into period bounds on x:
     * <pre>
     *   date_trunc('month', x) =  '2024-03-01'  ->  x >= '2024-03-01' AND x < '2024-04-01'
     *   date_trunc('month', x) =  '2024-03-15'  ->  x >= '2024-03-15' AND x < '2024-03-15'
     *                                               (empty; NULL for NULL x, FALSE otherwise,
     *                                                exactly like the original)
     *   date_trunc('month', x) >= '2024-03-15'  ->  x >= '2024-04-01'
     * </pre>
     * Refuses when the next period start does not advance: at the DATETIME domain max
     * {@code nextUpperDateTime} clamps and returns its input, and bounds built from the
     * clamped value drop or admit rows of the last period.
     */
    public static final MonotonicFunctionRegistry.ExactInverse PERIOD_FLOOR = MonotonicInverse::invertPeriodFloor;

    private static Optional<ScalarOperator> invertPeriodFloor(CallOperator call, ScalarOperator dataChild,
                                                              BinaryType cmp, ConstantOperator value) {
        try {
            ScalarOperator unitArg = call.getChild(0);
            if (!unitArg.isConstantRef() || ((ConstantOperator) unitArg).isNull()) {
                return Optional.empty();
            }
            if (!value.getType().isDateType()) {
                return Optional.empty();
            }
            String unit = ((ConstantOperator) unitArg).getVarchar().toLowerCase();
            Type childType = dataChild.getType();
            // hour-and-finer periods have intra-day bounds, unrepresentable on a DATE child
            if (childType.isDate() && !DATE_ONLY_UNITS.contains(unit)) {
                return Optional.empty();
            }
            LocalDateTime periodStart = SyncPartitionUtils.getLowerDateTime(value.getDatetime(), unit);
            LocalDateTime nextStart = SyncPartitionUtils.nextUpperDateTime(periodStart, unit);
            if (periodStart.isBefore(ConstantOperator.MIN_DATETIME) || !nextStart.isAfter(periodStart)) {
                return Optional.empty();
            }
            boolean aligned = periodStart.equals(value.getDatetime());
            ConstantOperator lower = boundOfChildType(childType, periodStart);
            ConstantOperator upper = boundOfChildType(childType, nextStart);
            switch (cmp) {
                case EQ:
                    if (!aligned) {
                        // empty interval on the child, never constant FALSE: for NULL x the
                        // original is NULL and NOT/IS NULL contexts must see NULL here too
                        return Optional.of(Utils.compoundAnd(
                                BinaryPredicateOperator.ge(dataChild, lower),
                                BinaryPredicateOperator.lt(dataChild, lower)));
                    }
                    // a day period on a DATE child is the single point [d, d+1) = {d}; keep
                    // the equality shape, point filters feed bucket pruning
                    if (childType.isDate() && TimeUnitUtils.DAY.equals(unit)) {
                        return Optional.of(BinaryPredicateOperator.eq(dataChild, lower));
                    }
                    return Optional.of(Utils.compoundAnd(
                            BinaryPredicateOperator.ge(dataChild, lower),
                            BinaryPredicateOperator.lt(dataChild, upper)));
                case GE:
                    return Optional.of(BinaryPredicateOperator.ge(dataChild, aligned ? lower : upper));
                case GT:
                    return Optional.of(BinaryPredicateOperator.ge(dataChild, upper));
                case LE:
                    return Optional.of(BinaryPredicateOperator.lt(dataChild, upper));
                case LT:
                    return Optional.of(BinaryPredicateOperator.lt(dataChild, aligned ? lower : upper));
                default:
                    return Optional.empty();
            }
        } catch (Exception e) {
            // unknown unit (SemanticException), value outside the child type's range, ...
            return Optional.empty();
        }
    }

    /**
     * A fixed-duration shift inverts by applying the opposite shift to the constant:
     * {@code days_add(x, 3) cmp value} becomes {@code x cmp days_sub(value, 3)}, same
     * comparison. Only day-and-finer shifts (and weeks: a fixed 7 days) qualify; month and
     * year shifts clamp day-of-month and have no such inverse.
     * <p>
     * The shift is partial: near the domain edge it overflows to NULL (days_add(x, 3) is
     * NULL for x above '9999-12-28'), where the original predicate rejects the row. A range
     * comparison open toward that edge gets a guard bound cutting the overflow tail off:
     * {@code days_add(x, 3) >= C} becomes {@code x >= C-3d AND x <= '9999-12-28 23:59:59'}.
     * The guard bound is the opposite shift folded at the domain extreme; when that fold is
     * itself NULL, the tail lies on the other side and no guard is needed. An equality needs
     * no guard: its preimage point always sits in the total zone. Refuses when the shifted
     * constant folds to NULL or throws.
     * <p>
     * Known residual: under NOT a tail row differs (original NOT(NULL) = NULL filters it,
     * the guarded form gives NOT(FALSE) = TRUE). "NULL exactly on the tail" is inexpressible
     * on the bare column; the period inverters have no such gap (total functions).
     */
    public static MonotonicFunctionRegistry.ExactInverse shift(String oppositeFnName) {
        return (call, dataChild, cmp, value) -> invertShift(oppositeFnName, call, dataChild, cmp, value);
    }

    private static Optional<ScalarOperator> invertShift(String oppositeFnName, CallOperator call,
                                                        ScalarOperator dataChild, BinaryType cmp,
                                                        ConstantOperator value) {
        try {
            ScalarOperator amount = call.getChild(1);
            if (!amount.isConstantRef() || ((ConstantOperator) amount).isNull()) {
                return Optional.empty();
            }
            ConstantOperator shifted = foldOpposite(oppositeFnName, value, (ConstantOperator) amount);
            if (shifted == null) {
                return Optional.empty();
            }
            BinaryPredicateOperator inverted = new BinaryPredicateOperator(cmp, dataChild, shifted);
            switch (cmp) {
                case GE:
                case GT: {
                    ConstantOperator guard = foldOpposite(oppositeFnName,
                            boundOfChildType(shifted.getType(), ConstantOperator.MAX_DATETIME), (ConstantOperator) amount);
                    return Optional.of(guard == null ? inverted
                            : Utils.compoundAnd(inverted, BinaryPredicateOperator.le(dataChild, guard)));
                }
                case LE:
                case LT: {
                    ConstantOperator guard = foldOpposite(oppositeFnName,
                            boundOfChildType(shifted.getType(), ConstantOperator.MIN_DATETIME), (ConstantOperator) amount);
                    return Optional.of(guard == null ? inverted
                            : Utils.compoundAnd(inverted, BinaryPredicateOperator.ge(dataChild, guard)));
                }
                default:
                    return Optional.of(inverted);
            }
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static ConstantOperator foldOpposite(String oppositeFnName, ConstantOperator value,
                                                 ConstantOperator amount) {
        try {
            Function fn = ExprUtils.getBuiltinFunction(oppositeFnName,
                    new Type[] {value.getType(), amount.getType()}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            if (fn == null) {
                return null;
            }
            CallOperator shifted = new CallOperator(oppositeFnName, fn.getReturnType(),
                    ImmutableList.of(value, amount), fn);
            // align argument types first (a DATE value against the DATETIME signature), then fold
            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            ScalarOperator folded = rewriter.rewrite(shifted, ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE);
            folded = rewriter.rewrite(folded, ScalarOperatorRewriter.FOLD_CONSTANT_RULES);
            if (folded instanceof ConstantOperator && !((ConstantOperator) folded).isNull()) {
                return (ConstantOperator) folded;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private static ConstantOperator boundOfChildType(Type childType, LocalDateTime bound) {
        // bounds must be typed as the child: a DATETIME constant against a DATE child gets
        // the CHILD wrapped in an implicit cast, putting the predicate back on an expression
        return childType.isDate() ? ConstantOperator.createDate(bound) : ConstantOperator.createDatetime(bound);
    }
}
