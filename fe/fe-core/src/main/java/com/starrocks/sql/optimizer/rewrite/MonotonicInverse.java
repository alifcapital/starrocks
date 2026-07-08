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
import com.google.common.collect.ImmutableMap;
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
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * {@link MonotonicFunctionRegistry.ExactInverse} implementations. An exact inverse turns
 * {@code f(x) cmp value} into an equivalent predicate on {@code x} itself: equivalent on
 * non-NULL input and NULL-on-NULL, so the result may replace the original under any boolean
 * context. Anything short of that equivalence must refuse (the shift family carries one
 * documented exception, see {@link #shift}).
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

    /**
     * year(x) cmp integer inverts into year-period bounds on x: {@code year(x) = 2024} is
     * {@code x >= '2024-01-01' AND x < '2025-01-01'}. Values outside [0, 9999] refuse.
     */
    public static final MonotonicFunctionRegistry.ExactInverse YEAR_PERIOD = MonotonicInverse::invertYear;

    /**
     * to_date(x) cmp date is the day period on x, the function form of the
     * datetime-to-date cast that ReduceCastRule already reduces.
     */
    public static final MonotonicFunctionRegistry.ExactInverse DAY_FLOOR = MonotonicInverse::invertToDate;

    /**
     * datediff(x, C) cmp N inverts into day-period bounds on x from the day {@code C + N};
     * the const-first form datediff(C, x) decreases in x and inverts with the comparison
     * flipped. datediff truncates both sides to days and never overflows in x, so there is
     * no shift-style tail.
     */
    public static final MonotonicFunctionRegistry.ExactInverse DATEDIFF_DAYS = MonotonicInverse::invertDatediff;

    /**
     * date_format(x, fmt) cmp string, for a whitelist of fixed-width formats. A constant is
     * admitted when it has the format's canonical length, parses strictly, and re-renders to
     * exactly itself; then string comparison against the rendered images coincides with date
     * comparison and the constant maps to a period of the format's granularity. On a DATE
     * column a day format is a single point: the equality keeps its point shape (bucket
     * pruning consumes point filters only) and NE / null-safe-equal invert too.
     */
    public static final MonotonicFunctionRegistry.ExactInverse RENDERED_PERIOD = MonotonicInverse::invertDateFormat;

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
            boolean aligned = periodStart.equals(value.getDatetime());
            boolean pointCollapse = childType.isDate() && TimeUnitUtils.DAY.equals(unit);
            return periodCells(dataChild, cmp, aligned, periodStart, nextStart, pointCollapse);
        } catch (Exception e) {
            // unknown unit (SemanticException), value outside the child type's range, ...
            return Optional.empty();
        }
    }

    private static Optional<ScalarOperator> invertYear(CallOperator call, ScalarOperator dataChild,
                                                       BinaryType cmp, ConstantOperator value) {
        try {
            Optional<ConstantOperator> asLong = value.castTo(IntegerType.BIGINT);
            if (asLong.isEmpty() || asLong.get().isNull()) {
                return Optional.empty();
            }
            long year = asLong.get().getBigint();
            if (year < 0 || year > 9999) {
                return Optional.empty();
            }
            LocalDateTime periodStart = LocalDateTime.of((int) year, 1, 1, 0, 0);
            LocalDateTime nextStart = year == 9999 ? periodStart : periodStart.plusYears(1);
            return periodCells(dataChild, cmp, true, periodStart, nextStart, false);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static Optional<ScalarOperator> invertToDate(CallOperator call, ScalarOperator dataChild,
                                                         BinaryType cmp, ConstantOperator value) {
        try {
            if (!value.getType().isDateType()) {
                return Optional.empty();
            }
            LocalDateTime day = value.getDatetime();
            if (!day.toLocalDate().atStartOfDay().equals(day)) {
                // a datetime-typed constant with a time part can never equal a to_date result;
                // aligned=false gives the empty interval for EQ and rounded bounds otherwise
                return periodCells(dataChild, cmp, false, day.toLocalDate().atStartOfDay(),
                        day.toLocalDate().plusDays(1).atStartOfDay(), false);
            }
            return periodCells(dataChild, cmp, true, day, day.plusDays(1), false);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static Optional<ScalarOperator> invertDatediff(CallOperator call, ScalarOperator dataChild,
                                                           BinaryType cmp, ConstantOperator value) {
        try {
            boolean columnFirst = call.getChild(0) == dataChild;
            ScalarOperator otherArg = columnFirst ? call.getChild(1) : call.getChild(0);
            if (!otherArg.isConstantRef()) {
                return Optional.empty();
            }
            ConstantOperator other = (ConstantOperator) otherArg;
            if (other.isNull() || !other.getType().isDateType()) {
                return Optional.empty();
            }
            Optional<ConstantOperator> asLong = value.castTo(IntegerType.BIGINT);
            if (asLong.isEmpty() || asLong.get().isNull()) {
                return Optional.empty();
            }
            long diffDays = asLong.get().getBigint();
            LocalDate otherDay = other.getDatetime().toLocalDate();
            // datediff truncates both sides to days: the preimage of one diff value is one day
            LocalDate day = columnFirst ? otherDay.plusDays(diffDays) : otherDay.minusDays(diffDays);
            BinaryType effective = columnFirst ? cmp : flip(cmp);
            boolean pointCollapse = dataChild.getType().isDate();
            return periodCells(dataChild, effective, true, day.atStartOfDay(),
                    day.plusDays(1).atStartOfDay(), pointCollapse);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /** Whitelisted formats: canonical rendered length and period granularity. */
    private static final Map<String, FormatSpec> RENDERED_FORMATS = ImmutableMap.<String, FormatSpec>builder()
            .put("%Y", new FormatSpec(4, TimeUnitUtils.YEAR, "uuuu"))
            .put("%Y%m", new FormatSpec(6, TimeUnitUtils.MONTH, "uuuuMM"))
            .put("%Y-%m", new FormatSpec(7, TimeUnitUtils.MONTH, "uuuu-MM"))
            .put("%Y%m%d", new FormatSpec(8, TimeUnitUtils.DAY, "uuuuMMdd"))
            .put("%Y-%m-%d", new FormatSpec(10, TimeUnitUtils.DAY, "uuuu-MM-dd"))
            .put("%Y-%m-%d %H:%i:%s", new FormatSpec(19, TimeUnitUtils.SECOND, "uuuu-MM-dd HH:mm:ss"))
            .build();

    private static final class FormatSpec {
        final int length;
        final String unit;
        final DateTimeFormatter parser;

        FormatSpec(int length, String unit, String javaPattern) {
            this.length = length;
            this.unit = unit;
            // strict, zero-padded, day/month defaulted to 1 for the coarse formats: exactly
            // the canonical rendered images and nothing else
            this.parser = new DateTimeFormatterBuilder()
                    .appendPattern(javaPattern)
                    .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
                    .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
                    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                    .toFormatter()
                    .withResolverStyle(ResolverStyle.STRICT);
        }
    }

    private static Optional<ScalarOperator> invertDateFormat(CallOperator call, ScalarOperator dataChild,
                                                             BinaryType cmp, ConstantOperator value) {
        try {
            ScalarOperator fmtArg = call.getChild(1);
            if (!fmtArg.isConstantRef() || ((ConstantOperator) fmtArg).isNull()
                    || !value.getType().isStringType()) {
                return Optional.empty();
            }
            FormatSpec spec = RENDERED_FORMATS.get(((ConstantOperator) fmtArg).getVarchar());
            if (spec == null) {
                return Optional.empty();
            }
            String constant = value.getVarchar();
            // admission: canonical length, strict parse, exact render round-trip - together
            // they admit exactly the strings date_format itself can produce
            if (constant.length() != spec.length) {
                return Optional.empty();
            }
            LocalDateTime parsed = LocalDateTime.from(spec.parser.parse(constant));
            if (!spec.parser.format(parsed).equals(constant)) {
                return Optional.empty();
            }
            Type childType = dataChild.getType();
            if (childType.isDate() && !DATE_ONLY_UNITS.contains(spec.unit)) {
                return Optional.empty();
            }
            LocalDateTime nextStart = SyncPartitionUtils.nextUpperDateTime(parsed, spec.unit);
            boolean pointCollapse = childType.isDate() && TimeUnitUtils.DAY.equals(spec.unit);
            return periodCells(dataChild, cmp, true, parsed, nextStart, pointCollapse);
        } catch (Exception e) {
            // strict parse rejections land here: '999913', '0000-00-00', '2024-3-05', ...
            return Optional.empty();
        }
    }

    /**
     * The comparison cells shared by every period-shaped inverse. {@code aligned} tells
     * whether the constant equals its own period start. Guards: the period must sit inside
     * the supported range and the next start must actually advance (the period helper
     * clamps at the DATETIME max instead of overflowing).
     */
    private static Optional<ScalarOperator> periodCells(ScalarOperator dataChild, BinaryType cmp, boolean aligned,
                                                        LocalDateTime periodStart, LocalDateTime nextStart,
                                                        boolean pointCollapse) {
        if (periodStart.isBefore(ConstantOperator.MIN_DATETIME) || !nextStart.isAfter(periodStart)
                || nextStart.isAfter(ConstantOperator.MAX_DATETIME)) {
            return Optional.empty();
        }
        Type childType = dataChild.getType();
        ConstantOperator lower = boundOfChildType(childType, periodStart);
        ConstantOperator upper = boundOfChildType(childType, nextStart);
        switch (cmp) {
            case EQ:
                if (!aligned) {
                    // empty interval on the child, never constant FALSE: for NULL x the
                    // original is NULL and NOT / IS NULL contexts must see NULL here too
                    return Optional.of(Utils.compoundAnd(
                            BinaryPredicateOperator.ge(dataChild, lower),
                            BinaryPredicateOperator.lt(dataChild, lower)));
                }
                if (pointCollapse) {
                    return Optional.of(BinaryPredicateOperator.eq(dataChild, lower));
                }
                return Optional.of(Utils.compoundAnd(
                        BinaryPredicateOperator.ge(dataChild, lower),
                        BinaryPredicateOperator.lt(dataChild, upper)));
            case NE:
                // expressible only when the period is a single point of the child's domain
                return aligned && pointCollapse
                        ? Optional.of(BinaryPredicateOperator.ne(dataChild, lower)) : Optional.empty();
            case EQ_FOR_NULL:
                return aligned && pointCollapse
                        ? Optional.of(new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, dataChild, lower))
                        : Optional.empty();
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
    }

    private static BinaryType flip(BinaryType cmp) {
        switch (cmp) {
            case GE: return BinaryType.LE;
            case GT: return BinaryType.LT;
            case LE: return BinaryType.GE;
            case LT: return BinaryType.GT;
            default: return cmp;
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
     * no guard: its preimage point always sits in the total zone. NE and null-safe-equal
     * refuse (their regions open toward both edges and a guard would change WHERE
     * semantics). Refuses when the shifted constant folds to NULL or throws.
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
                case EQ:
                    return Optional.of(inverted);
                case GE:
                case GT: {
                    ConstantOperator guard = foldOpposite(oppositeFnName,
                            boundOfChildType(shifted.getType(), ConstantOperator.MAX_DATETIME),
                            (ConstantOperator) amount);
                    return Optional.of(guard == null ? inverted
                            : Utils.compoundAnd(inverted, BinaryPredicateOperator.le(dataChild, guard)));
                }
                case LE:
                case LT: {
                    ConstantOperator guard = foldOpposite(oppositeFnName,
                            boundOfChildType(shifted.getType(), ConstantOperator.MIN_DATETIME),
                            (ConstantOperator) amount);
                    return Optional.of(guard == null ? inverted
                            : Utils.compoundAnd(inverted, BinaryPredicateOperator.ge(dataChild, guard)));
                }
                default:
                    return Optional.empty();
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
