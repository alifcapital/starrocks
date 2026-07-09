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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.util.TimeUtils;
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

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.zone.ZoneOffsetTransition;
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
     * flipped. datediff truncates both sides to days and does not overflow in x, so no
     * guard bound is needed.
     */
    public static final MonotonicFunctionRegistry.ExactInverse DATEDIFF_DAYS = MonotonicInverse::invertDatediff;

    /**
     * date_format(x, fmt) cmp string, for a whitelist of fixed-width formats. A constant is
     * admitted when it has the format's canonical length, parses strictly, and re-renders to
     * exactly itself; then string comparison against the rendered images coincides with date
     * comparison and the constant maps to a period of the format's granularity. On a DATE
     * column a day format is a single point: the equality inverts to col = value (bucket
     * pruning consumes point filters only) and NE / null-safe-equal invert too.
     */
    public static final MonotonicFunctionRegistry.ExactInverse RENDERED_PERIOD = MonotonicInverse::invertDateFormat;

    /**
     * to_iso8601(x) cmp string. The rendering is fixed-width and injective on the child's
     * domain (a DATE renders as %Y-%m-%d, a DATETIME as %Y-%m-%dT%H:%i:%s.%f with the
     * fraction zero-padded to six digits), so string order equals value order and every
     * comparison carries over to the parsed constant, strict forms and NE included.
     */
    public static final MonotonicFunctionRegistry.ExactInverse ISO_RENDER = MonotonicInverse::invertToIso8601;

    /**
     * unix_timestamp(x) cmp N for a DATETIME x. The fold truncates x to whole seconds, so
     * a value N covers the second [from_epoch(N), from_epoch(N+1)). Out-of-range input
     * clamps to 0, not NULL: for N > 0 the clamped tails compare FALSE on both the original
     * and the inverted form, so EQ/GE/GT invert exactly with a bound cutting the upper
     * tail; the preimage of LE/LT contains both clamped tails and is not an interval, and
     * N <= 0 lands on the clamp plateau itself - all refused. A bound falling into a
     * fall-back overlap of the session zone refuses: that wall clock maps two epochs.
     */
    public static final MonotonicFunctionRegistry.ExactInverse UNIX_EPOCH = MonotonicInverse::invertUnixTimestamp;

    /**
     * from_unixtime[_ms](x[, fmt]) cmp string for an integer x. The constant is admitted by
     * the fixed-width format machinery (canonical length, strict parse, render round-trip)
     * and maps to an epoch period of the format's granularity. Outside [0, MAX] the
     * rendering is NULL where the original predicate rejects the row: range comparisons get
     * the domain-edge guard bounds, with the same NOT-context residual as the shift family.
     * A period endpoint falling into a fall-back overlap of the session zone refuses: that
     * wall clock maps two epochs.
     */
    public static final MonotonicFunctionRegistry.ExactInverse EPOCH_RENDER = MonotonicInverse::invertFromUnixTime;

    /**
     * to_days(x) cmp N. Day number N maps back to one day (the fold and BE agree on the
     * year-0 anchor: to_days('1970-01-01') is 719528): a bijection on a DATE x where every
     * comparison carries over, the day period on a DATETIME x.
     */
    public static final MonotonicFunctionRegistry.ExactInverse DAY_NUMBER = MonotonicInverse::invertToDays;

    /**
     * last_day(x[, unit]) cmp D. The images are the period ends of the unit, so the
     * comparison maps to unit-period bounds on x with ceiling semantics: any D below its
     * own period end already forces the next period. A D that is no period end cannot be
     * an equality match (empty interval), and NE inverts through the aligned disjunction.
     */
    public static final MonotonicFunctionRegistry.ExactInverse UNIT_END = MonotonicInverse::invertLastDay;

    /**
     * next_day(x, dow) cmp D: the image grid is the given weekday and
     * {@code next_day(x) = G} holds exactly for x in [G-7, G). previous_day(x, dow) is the
     * strict mirror with preimage [G+1, G+8). Both invert by normalizing the constant to
     * the grid (ceiling for GE/GT, floor for LE/LT) and emitting the seven-day window
     * bounds; a constant off the grid refuses equality shapes.
     */
    public static final MonotonicFunctionRegistry.ExactInverse DOW_NEXT = MonotonicInverse::invertNextDay;
    public static final MonotonicFunctionRegistry.ExactInverse DOW_PREVIOUS = MonotonicInverse::invertPreviousDay;

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

    private static Optional<ScalarOperator> invertUnixTimestamp(CallOperator call, ScalarOperator dataChild,
                                                                BinaryType cmp, ConstantOperator value) {
        try {
            if (!dataChild.getType().isDatetime() || !value.getType().isExactNumericType()) {
                return Optional.empty();
            }
            ZoneId zone = TimeUtils.getTimeZone().toZoneId();
            Optional<ConstantOperator> asLong = value.castTo(IntegerType.BIGINT);
            if (asLong.isEmpty() || asLong.get().isNull()) {
                return Optional.empty();
            }
            long n = asLong.get().getBigint();
            if (n <= 0 || n >= TimeUtils.MAX_UNIX_TIMESTAMP) {
                return Optional.empty();
            }
            LocalDateTime lower = LocalDateTime.ofInstant(Instant.ofEpochSecond(n), zone);
            LocalDateTime upper = LocalDateTime.ofInstant(Instant.ofEpochSecond(n + 1), zone);
            LocalDateTime maxValid = LocalDateTime.ofInstant(
                    Instant.ofEpochSecond(TimeUtils.MAX_UNIX_TIMESTAMP), zone);
            // a wall clock inside a fall-back overlap maps two epochs onto one rendering:
            // the bounds are correct only when every endpoint is unambiguous
            if (!unambiguous(zone, lower) || !unambiguous(zone, upper) || !unambiguous(zone, maxValid)) {
                return Optional.empty();
            }
            switch (cmp) {
                case EQ:
                    return Optional.of(Utils.compoundAnd(
                            BinaryPredicateOperator.ge(dataChild, ConstantOperator.createDatetime(lower)),
                            BinaryPredicateOperator.lt(dataChild, ConstantOperator.createDatetime(upper))));
                case GE:
                    return Optional.of(Utils.compoundAnd(
                            BinaryPredicateOperator.ge(dataChild, ConstantOperator.createDatetime(lower)),
                            BinaryPredicateOperator.le(dataChild, ConstantOperator.createDatetime(maxValid))));
                case GT:
                    return Optional.of(Utils.compoundAnd(
                            BinaryPredicateOperator.ge(dataChild, ConstantOperator.createDatetime(upper)),
                            BinaryPredicateOperator.le(dataChild, ConstantOperator.createDatetime(maxValid))));
                default:
                    return Optional.empty();
            }
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static Optional<ScalarOperator> invertFromUnixTime(CallOperator call, ScalarOperator dataChild,
                                                               BinaryType cmp, ConstantOperator value) {
        try {
            Type childType = dataChild.getType();
            if (!childType.isIntegerType() || !value.getType().isStringType()) {
                return Optional.empty();
            }
            ZoneId zone = TimeUtils.getTimeZone().toZoneId();
            String fmt;
            if (call.getChildren().size() == 1) {
                // the one-argument form renders the SR datetime string
                fmt = "%Y-%m-%d %H:%i:%s";
            } else if (call.getChildren().size() == 2 && call.getChild(1).isConstantRef()
                    && !((ConstantOperator) call.getChild(1)).isNull()) {
                fmt = ((ConstantOperator) call.getChild(1)).getVarchar();
            } else {
                return Optional.empty();
            }
            FormatSpec spec = RENDERED_FORMATS.get(fmt);
            if (spec == null) {
                return Optional.empty();
            }
            String constant = value.getVarchar();
            if (constant.length() != spec.length) {
                return Optional.empty();
            }
            LocalDateTime parsed = LocalDateTime.from(spec.parser.parse(constant));
            if (!spec.parser.format(parsed).equals(constant)) {
                return Optional.empty();
            }
            long unitScale = FunctionSet.FROM_UNIXTIME_MS.equals(call.getFnName().toLowerCase()) ? 1000L : 1L;
            LocalDateTime nextStart = SyncPartitionUtils.nextUpperDateTime(parsed, spec.unit);
            if (!nextStart.isAfter(parsed)) {
                return Optional.empty();
            }
            // a wall clock inside a fall-back overlap maps two epochs onto one rendering:
            // the period is an epoch interval only when both endpoints are unambiguous
            // (a transition strictly inside the period just makes it longer, which the
            // epoch interval covers by construction)
            if (!unambiguous(zone, parsed) || !unambiguous(zone, nextStart)) {
                return Optional.empty();
            }
            // a sub-day rendering repeats a wall-clock hour at a fall-back: rows just past
            // the transition render below the constant again and a range preimage is not an
            // interval. Only transitions near the period matter - a far fall-back rewinds
            // the rendering by an hour but not below the constant. Day-and-coarser
            // renderings never decrease.
            if (TimeUnitUtils.SECOND.equals(spec.unit)
                    && !transitionFree(zone, parsed.minusHours(26), nextStart.plusHours(26))) {
                return Optional.empty();
            }
            long lower = parsed.atZone(zone).toEpochSecond() * unitScale;
            long upper = nextStart.atZone(zone).toEpochSecond() * unitScale;
            long minValid = 0;
            long maxValid = TimeUtils.MAX_UNIX_TIMESTAMP * unitScale;
            if (lower < minValid || upper > maxValid) {
                return Optional.empty();
            }
            ConstantOperator lo = intBound(childType, lower);
            ConstantOperator hi = intBound(childType, upper);
            if (lo == null || hi == null) {
                return Optional.empty();
            }
            // a domain-edge guard outside the child type's own range is dropped: the type
            // already cannot hold values past it
            ConstantOperator min = intBound(childType, minValid);
            ConstantOperator max = intBound(childType, maxValid);
            switch (cmp) {
                case EQ:
                    return Optional.of(Utils.compoundAnd(
                            BinaryPredicateOperator.ge(dataChild, lo), BinaryPredicateOperator.lt(dataChild, hi)));
                case GE:
                    return Optional.of(withGuard(BinaryPredicateOperator.ge(dataChild, lo),
                            max == null ? null : BinaryPredicateOperator.le(dataChild, max)));
                case GT:
                    return Optional.of(withGuard(BinaryPredicateOperator.ge(dataChild, hi),
                            max == null ? null : BinaryPredicateOperator.le(dataChild, max)));
                case LE:
                    return Optional.of(withGuard(BinaryPredicateOperator.lt(dataChild, hi),
                            min == null ? null : BinaryPredicateOperator.ge(dataChild, min)));
                case LT:
                    return Optional.of(withGuard(BinaryPredicateOperator.lt(dataChild, lo),
                            min == null ? null : BinaryPredicateOperator.ge(dataChild, min)));
                default:
                    return Optional.empty();
            }
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static ScalarOperator withGuard(ScalarOperator bound, ScalarOperator guard) {
        return guard == null ? bound : Utils.compoundAnd(bound, guard);
    }

    // to_days('1970-01-01') = 719528: the day count anchors at year 0 on both FE and BE
    private static final long TO_DAYS_EPOCH_SHIFT = 719528L;

    private static Optional<ScalarOperator> invertToDays(CallOperator call, ScalarOperator dataChild,
                                                         BinaryType cmp, ConstantOperator value) {
        try {
            if (!value.getType().isExactNumericType()) {
                return Optional.empty();
            }
            Optional<ConstantOperator> asLong = value.castTo(IntegerType.BIGINT);
            if (asLong.isEmpty() || asLong.get().isNull()) {
                return Optional.empty();
            }
            LocalDate day = LocalDate.ofEpochDay(asLong.get().getBigint() - TO_DAYS_EPOCH_SHIFT);
            LocalDateTime dayStart = day.atStartOfDay();
            if (dayStart.isBefore(ConstantOperator.MIN_DATETIME)
                    || dayStart.isAfter(ConstantOperator.MAX_DATETIME)) {
                return Optional.empty();
            }
            if (dataChild.getType().isDate()) {
                // a bijection on the DATE domain: every comparison carries over
                return Optional.of(new BinaryPredicateOperator(cmp, dataChild,
                        ConstantOperator.createDate(dayStart)));
            }
            return periodCells(dataChild, cmp, true, dayStart, dayStart.plusDays(1), false);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static Optional<ScalarOperator> invertLastDay(CallOperator call, ScalarOperator dataChild,
                                                          BinaryType cmp, ConstantOperator value) {
        try {
            String unit = TimeUnitUtils.MONTH;
            if (call.getChildren().size() == 2) {
                if (!call.getChild(1).isConstantRef() || ((ConstantOperator) call.getChild(1)).isNull()) {
                    return Optional.empty();
                }
                unit = ((ConstantOperator) call.getChild(1)).getVarchar().toLowerCase();
            }
            if (!Set.of(TimeUnitUtils.MONTH, TimeUnitUtils.QUARTER, TimeUnitUtils.YEAR).contains(unit)
                    || !value.getType().isDateType()) {
                return Optional.empty();
            }
            LocalDateTime periodStart = SyncPartitionUtils.getLowerDateTime(value.getDatetime(), unit);
            LocalDateTime nextStart = SyncPartitionUtils.nextUpperDateTime(periodStart, unit);
            if (!nextStart.isAfter(periodStart) || periodStart.isBefore(ConstantOperator.MIN_DATETIME)
                    || nextStart.isAfter(ConstantOperator.MAX_DATETIME)) {
                return Optional.empty();
            }
            // the images are period-end midnights; the constant may sit below, at, or above
            // the end of its own period (a DATETIME with a time part), and each side picks
            // the period boundary accordingly
            int cmpEnd = value.getDatetime().compareTo(nextStart.minusDays(1));
            Type childType = dataChild.getType();
            ConstantOperator lower = boundOfChildType(childType, periodStart);
            ConstantOperator upper = boundOfChildType(childType, nextStart);
            switch (cmp) {
                case EQ:
                    return Optional.of(cmpEnd == 0
                            ? Utils.compoundAnd(BinaryPredicateOperator.ge(dataChild, lower),
                                    BinaryPredicateOperator.lt(dataChild, upper))
                            : Utils.compoundAnd(BinaryPredicateOperator.ge(dataChild, lower),
                                    BinaryPredicateOperator.lt(dataChild, lower)));
                case NE:
                    return cmpEnd == 0 ? Optional.of(Utils.compoundOr(
                            BinaryPredicateOperator.lt(dataChild, lower),
                            BinaryPredicateOperator.ge(dataChild, upper))) : Optional.empty();
                case GE:
                    return Optional.of(BinaryPredicateOperator.ge(dataChild, cmpEnd <= 0 ? lower : upper));
                case GT:
                    return Optional.of(BinaryPredicateOperator.ge(dataChild, cmpEnd < 0 ? lower : upper));
                case LE:
                    return Optional.of(BinaryPredicateOperator.lt(dataChild, cmpEnd >= 0 ? upper : lower));
                case LT:
                    return Optional.of(BinaryPredicateOperator.lt(dataChild, cmpEnd > 0 ? upper : lower));
                default:
                    return Optional.empty();
            }
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static final Map<String, DayOfWeek> DOW_NAMES = ImmutableMap.<String, DayOfWeek>builder()
            .put("sunday", DayOfWeek.SUNDAY).put("sun", DayOfWeek.SUNDAY)
            .put("su", DayOfWeek.SUNDAY)
            .put("monday", DayOfWeek.MONDAY).put("mon", DayOfWeek.MONDAY)
            .put("mo", DayOfWeek.MONDAY)
            .put("tuesday", DayOfWeek.TUESDAY).put("tue", DayOfWeek.TUESDAY)
            .put("tu", DayOfWeek.TUESDAY)
            .put("wednesday", DayOfWeek.WEDNESDAY).put("wed", DayOfWeek.WEDNESDAY)
            .put("we", DayOfWeek.WEDNESDAY)
            .put("thursday", DayOfWeek.THURSDAY).put("thu", DayOfWeek.THURSDAY)
            .put("th", DayOfWeek.THURSDAY)
            .put("friday", DayOfWeek.FRIDAY).put("fri", DayOfWeek.FRIDAY)
            .put("fr", DayOfWeek.FRIDAY)
            .put("saturday", DayOfWeek.SATURDAY).put("sat", DayOfWeek.SATURDAY)
            .put("sa", DayOfWeek.SATURDAY)
            .build();

    // the fold matches the dow argument case-sensitively ("Monday"/"Mon"/"Mo"); accept
    // exactly those spellings
    private static DayOfWeek dowOf(CallOperator call) {
        if (call.getChildren().size() != 2 || !call.getChild(1).isConstantRef()
                || ((ConstantOperator) call.getChild(1)).isNull()) {
            return null;
        }
        String name = ((ConstantOperator) call.getChild(1)).getVarchar();
        if (name.isEmpty() || !Character.isUpperCase(name.charAt(0))) {
            return null;
        }
        return DOW_NAMES.get(name.toLowerCase());
    }

    private static Optional<ScalarOperator> invertNextDay(CallOperator call, ScalarOperator dataChild,
                                                          BinaryType cmp, ConstantOperator value) {
        try {
            DayOfWeek dow = dowOf(call);
            if (dow == null || !value.getType().isDateType()) {
                return Optional.empty();
            }
            LocalDate constant = value.getDatetime().toLocalDate();
            if (!value.getDatetime().equals(constant.atStartOfDay())) {
                return Optional.empty();
            }
            boolean aligned = constant.getDayOfWeek() == dow;
            // the smallest grid day at or after the constant
            LocalDate ceilGrid = constant.plusDays((dow.getValue() - constant.getDayOfWeek().getValue() + 7) % 7);
            LocalDateTime gridStart = ceilGrid.atStartOfDay();
            // next_day(x) = G holds exactly for x in [G-7, G)
            switch (cmp) {
                case EQ:
                    return aligned
                            ? window(dataChild, gridStart.minusDays(7), gridStart)
                            : window(dataChild, gridStart, gridStart);
                case NE:
                    return aligned ? disjunction(dataChild, gridStart.minusDays(7), gridStart) : Optional.empty();
                case GE:
                    return lowerBound(dataChild, gridStart.minusDays(7));
                case GT:
                    return lowerBound(dataChild, aligned ? gridStart : gridStart.minusDays(7));
                case LE:
                    return upperBound(dataChild, aligned ? gridStart : gridStart.minusDays(7));
                case LT:
                    return upperBound(dataChild, gridStart.minusDays(7));
                default:
                    return Optional.empty();
            }
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static Optional<ScalarOperator> invertPreviousDay(CallOperator call, ScalarOperator dataChild,
                                                              BinaryType cmp, ConstantOperator value) {
        try {
            DayOfWeek dow = dowOf(call);
            if (dow == null || !value.getType().isDateType()) {
                return Optional.empty();
            }
            LocalDate constant = value.getDatetime().toLocalDate();
            if (!value.getDatetime().equals(constant.atStartOfDay())) {
                return Optional.empty();
            }
            boolean aligned = constant.getDayOfWeek() == dow;
            LocalDate ceilGrid = constant.plusDays((dow.getValue() - constant.getDayOfWeek().getValue() + 7) % 7);
            LocalDate floorGrid = constant.minusDays((constant.getDayOfWeek().getValue() - dow.getValue() + 7) % 7);
            // previous_day(x) = G holds exactly for x in [G+1, G+8)
            switch (cmp) {
                case EQ:
                    return aligned
                            ? window(dataChild, constant.plusDays(1).atStartOfDay(), constant.plusDays(8).atStartOfDay())
                            : window(dataChild, constant.atStartOfDay(), constant.atStartOfDay());
                case NE:
                    return aligned
                            ? disjunction(dataChild, constant.plusDays(1).atStartOfDay(),
                                    constant.plusDays(8).atStartOfDay())
                            : Optional.empty();
                case GE:
                    return lowerBound(dataChild, ceilGrid.plusDays(1).atStartOfDay());
                case GT:
                    return lowerBound(dataChild, aligned ? constant.plusDays(8).atStartOfDay()
                            : ceilGrid.plusDays(1).atStartOfDay());
                case LE:
                    return upperBound(dataChild, aligned ? constant.plusDays(8).atStartOfDay()
                            : floorGrid.plusDays(8).atStartOfDay());
                case LT:
                    return upperBound(dataChild, aligned ? constant.plusDays(1).atStartOfDay()
                            : floorGrid.plusDays(8).atStartOfDay());
                default:
                    return Optional.empty();
            }
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static Optional<ScalarOperator> window(ScalarOperator dataChild, LocalDateTime from, LocalDateTime to) {
        if (from.isBefore(ConstantOperator.MIN_DATETIME) || to.isAfter(ConstantOperator.MAX_DATETIME)) {
            return Optional.empty();
        }
        Type childType = dataChild.getType();
        return Optional.of(Utils.compoundAnd(
                BinaryPredicateOperator.ge(dataChild, boundOfChildType(childType, from)),
                BinaryPredicateOperator.lt(dataChild, boundOfChildType(childType, to))));
    }

    private static Optional<ScalarOperator> disjunction(ScalarOperator dataChild, LocalDateTime from,
                                                        LocalDateTime to) {
        if (from.isBefore(ConstantOperator.MIN_DATETIME) || to.isAfter(ConstantOperator.MAX_DATETIME)) {
            return Optional.empty();
        }
        Type childType = dataChild.getType();
        return Optional.of(Utils.compoundOr(
                BinaryPredicateOperator.lt(dataChild, boundOfChildType(childType, from)),
                BinaryPredicateOperator.ge(dataChild, boundOfChildType(childType, to))));
    }

    private static Optional<ScalarOperator> lowerBound(ScalarOperator dataChild, LocalDateTime from) {
        if (from.isBefore(ConstantOperator.MIN_DATETIME) || from.isAfter(ConstantOperator.MAX_DATETIME)) {
            return Optional.empty();
        }
        return Optional.of(BinaryPredicateOperator.ge(dataChild, boundOfChildType(dataChild.getType(), from)));
    }

    private static Optional<ScalarOperator> upperBound(ScalarOperator dataChild, LocalDateTime to) {
        if (to.isBefore(ConstantOperator.MIN_DATETIME) || to.isAfter(ConstantOperator.MAX_DATETIME)) {
            return Optional.empty();
        }
        return Optional.of(BinaryPredicateOperator.lt(dataChild, boundOfChildType(dataChild.getType(), to)));
    }

    private static boolean unambiguous(ZoneId zone, LocalDateTime wall) {
        return zone.getRules().getValidOffsets(wall).size() == 1;
    }

    private static boolean transitionFree(ZoneId zone, LocalDateTime fromWall, LocalDateTime toWall) {
        Instant from = fromWall.atZone(zone).toInstant();
        Instant to = toWall.atZone(zone).toInstant();
        ZoneOffsetTransition next = zone.getRules().nextTransition(from);
        return next == null || next.getInstant().isAfter(to);
    }

    private static ConstantOperator intBound(Type childType, long value) {
        Optional<ConstantOperator> typed = ConstantOperator.createBigint(value).castTo(childType);
        return typed.filter(c -> !c.isNull()).orElse(null);
    }

    // to_iso8601 of a DATETIME: 'T' separator, microseconds zero-padded to six digits
    // (DateUtils '%f' output renders appendFraction(MICRO_OF_SECOND, 6, 6)), 26 chars
    private static final int ISO_DATETIME_LENGTH = 26;
    private static final DateTimeFormatter ISO_DATETIME_RENDER = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd'T'HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 6, 6, true)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);

    private static Optional<ScalarOperator> invertToIso8601(CallOperator call, ScalarOperator dataChild,
                                                            BinaryType cmp, ConstantOperator value) {
        try {
            if (!value.getType().isStringType()) {
                return Optional.empty();
            }
            String constant = value.getVarchar();
            Type childType = dataChild.getType();
            if (childType.isDate()) {
                FormatSpec spec = RENDERED_FORMATS.get("%Y-%m-%d");
                if (constant.length() != spec.length) {
                    return Optional.empty();
                }
                LocalDateTime parsed = LocalDateTime.from(spec.parser.parse(constant));
                if (!spec.parser.format(parsed).equals(constant)) {
                    return Optional.empty();
                }
                return Optional.of(new BinaryPredicateOperator(cmp, dataChild,
                        ConstantOperator.createDate(parsed)));
            }
            if (constant.length() != ISO_DATETIME_LENGTH) {
                return Optional.empty();
            }
            LocalDateTime parsed = LocalDateTime.from(ISO_DATETIME_RENDER.parse(constant));
            if (!ISO_DATETIME_RENDER.format(parsed).equals(constant)) {
                return Optional.empty();
            }
            return Optional.of(new BinaryPredicateOperator(cmp, dataChild,
                    ConstantOperator.createDatetime(parsed)));
        } catch (Exception e) {
            // strict parse rejections: wrong width, '2024-03-05T10:30:00.5', '9999-13-...'
            return Optional.empty();
        }
    }

    /**
     * The half-open preimage {@code [lower, upperExclusive)} of a period comparison; a null
     * side is unbounded. {@code aligned} tells whether the constant equals its own period
     * start; a misaligned EQ yields the empty {@code [start, start)}. Shared by the
     * predicate cells below and by the PartitionColumnFilter fast path in
     * ColumnFilterConverter - one case table for both consumers.
     */
    public record PeriodRange(LocalDateTime lower, LocalDateTime upperExclusive) {
    }

    public static Optional<PeriodRange> periodRange(BinaryType cmp, boolean aligned,
                                                    LocalDateTime periodStart, LocalDateTime nextStart) {
        switch (cmp) {
            case EQ:
                return aligned ? Optional.of(new PeriodRange(periodStart, nextStart))
                        : Optional.of(new PeriodRange(periodStart, periodStart));
            case GE:
                return Optional.of(new PeriodRange(aligned ? periodStart : nextStart, null));
            case GT:
                return Optional.of(new PeriodRange(nextStart, null));
            case LE:
                return Optional.of(new PeriodRange(null, nextStart));
            case LT:
                return Optional.of(new PeriodRange(null, aligned ? periodStart : nextStart));
            default:
                return Optional.empty();
        }
    }

    /**
     * The comparison cells shared by every period inverse. {@code aligned} tells
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
                if (aligned && pointCollapse) {
                    return Optional.of(BinaryPredicateOperator.eq(dataChild, lower));
                }
                break;
            case NE:
                if (aligned && pointCollapse) {
                    return Optional.of(BinaryPredicateOperator.ne(dataChild, lower));
                }
                if (aligned) {
                    // the disjunction keeps NULL semantics: a NULL child gives
                    // NULL OR NULL = NULL, like the original comparison
                    return Optional.of(Utils.compoundOr(
                            BinaryPredicateOperator.lt(dataChild, lower),
                            BinaryPredicateOperator.ge(dataChild, upper)));
                }
                // a misaligned constant renders from no period: the original is TRUE for
                // every non-NULL child, which has no bare-column comparison form
                return Optional.empty();
            case EQ_FOR_NULL:
                return aligned && pointCollapse
                        ? Optional.of(new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, dataChild, lower))
                        : Optional.empty();
            default:
                break;
        }
        Optional<PeriodRange> range = periodRange(cmp, aligned, periodStart, nextStart);
        if (range.isEmpty()) {
            return Optional.empty();
        }
        // a misaligned EQ maps to the empty [start, start): both comparisons on the child,
        // never constant FALSE - for NULL x the original is NULL and NOT / IS NULL contexts
        // must see NULL here too
        ScalarOperator lowerBound = range.get().lower() == null ? null
                : BinaryPredicateOperator.ge(dataChild, boundOfChildType(childType, range.get().lower()));
        ScalarOperator upperBound = range.get().upperExclusive() == null ? null
                : BinaryPredicateOperator.lt(dataChild, boundOfChildType(childType, range.get().upperExclusive()));
        if (lowerBound == null) {
            return Optional.ofNullable(upperBound);
        }
        if (upperBound == null) {
            return Optional.of(lowerBound);
        }
        return Optional.of(Utils.compoundAnd(lowerBound, upperBound));
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
     * no guard: its only preimage point is the shifted constant, a valid value outside the
     * tail. NE and null-safe-equal refuse: their regions open toward both edges and a guard
     * would change WHERE semantics. Refuses when the shifted constant folds to NULL or
     * throws.
     * <p>
     * Known residual: under NOT a tail row differs (original NOT(NULL) = NULL filters it,
     * the guarded form gives NOT(FALSE) = TRUE). No predicate on the bare column yields NULL
     * only for tail rows, so this is not fixable here. Period functions do not overflow and
     * their inverses have no such gap.
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
