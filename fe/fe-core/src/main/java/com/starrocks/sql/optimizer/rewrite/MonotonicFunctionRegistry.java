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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.util.DateFormatModel;
import com.starrocks.common.util.DatePeriod;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Per-function facts for monotonic value reasoning, keyed by lower-case function name.
 * {@code ScalarOperatorEvaluator.isMonotonicFunction} only says "this function is monotonic
 * in some argument"; this registry says in which one, which functions are admitted at all,
 * and which have an exact preimage. Functions not listed here are refused.
 */
public final class MonotonicFunctionRegistry {

    private MonotonicFunctionRegistry() {
    }

    /**
     * Inverts a comparison onto the data argument. The registry entry determines the contract:
     * exactInverse preserves TRUE/FALSE/NULL; filterInverse supplies only a necessary condition
     * for TRUE and must never replace the original expression. Empty means refusal.
     */
    public interface PredicateInverse {
        Optional<ScalarOperator> invert(CallOperator call, ScalarOperator dataChild,
                                        BinaryType cmp, ConstantOperator value);
    }

    /**
     * Which argument the data column may sit in. Some functions registered as monotonic
     * decrease in one argument (datediff in its second), and date_trunc's first argument is a
     * unit name with no order at all.
     */
    private static final Map<String, Set<Integer>> DATA_ARG_POSITIONS = ImmutableMap.<String, Set<Integer>>builder()
            .put(FunctionSet.DATE_TRUNC, Set.of(1))
            .put(FunctionSet.TIME_SLICE, Set.of(0))
            .put(FunctionSet.DATE_FORMAT, Set.of(0))
            .put(FunctionSet.YEAR, Set.of(0))
            // increases in the first argument, decreases in the second; the caller admits
            // only one non-constant argument, so each direction stays a single chain
            .put(FunctionSet.DATEDIFF, Set.of(0, 1))
            .put(FunctionSet.STR_TO_DATE, Set.of(0))
            .put(FunctionSet.STR2DATE, Set.of(0))
            .put(FunctionSet.FROM_UNIXTIME, Set.of(0))
            .put(FunctionSet.FROM_UNIXTIME_MS, Set.of(0))
            .put(FunctionSet.UNIX_TIMESTAMP, Set.of(0))
            .put(FunctionSet.CONVERT_TZ, Set.of(0))
            .put(FunctionSet.TO_DATETIME, Set.of(0))
            .put(FunctionSet.TO_DAYS, Set.of(0))
            .put(FunctionSet.TO_DATE, Set.of(0))
            .put(FunctionSet.DATE, Set.of(0))
            .put(FunctionSet.TO_ISO8601, Set.of(0))
            .put(FunctionSet.LAST_DAY, Set.of(0))
            .put(FunctionSet.NEXT_DAY, Set.of(0))
            .put(FunctionSet.PREVIOUS_DAY, Set.of(0))
            // add/sub functions shift a date by a constant amount: monotonic in the date
            // argument. Day shifts are also monotonic in the amount, days_add increasing and
            // days_sub decreasing, so they admit an integer column in the amount argument too;
            // other shifts admit the date argument only.
            .put(FunctionSet.YEARS_ADD, Set.of(0))
            .put(FunctionSet.QUARTERS_ADD, Set.of(0))
            .put(FunctionSet.MONTHS_ADD, Set.of(0))
            .put(FunctionSet.ADD_MONTHS, Set.of(0))
            .put(FunctionSet.WEEKS_ADD, Set.of(0))
            .put(FunctionSet.DAYS_ADD, Set.of(0, 1))
            .put(FunctionSet.ADDDATE, Set.of(0, 1))
            .put(FunctionSet.DATE_ADD, Set.of(0, 1))
            .put(FunctionSet.HOURS_ADD, Set.of(0))
            .put(FunctionSet.MINUTES_ADD, Set.of(0))
            .put(FunctionSet.SECONDS_ADD, Set.of(0))
            .put(FunctionSet.MILLISECONDS_ADD, Set.of(0))
            .put(FunctionSet.MICROSECONDS_ADD, Set.of(0))
            .put(FunctionSet.YEARS_SUB, Set.of(0))
            .put(FunctionSet.QUARTERS_SUB, Set.of(0))
            .put(FunctionSet.MONTHS_SUB, Set.of(0))
            .put(FunctionSet.WEEKS_SUB, Set.of(0))
            .put(FunctionSet.DAYS_SUB, Set.of(0, 1))
            .put(FunctionSet.SUBDATE, Set.of(0, 1))
            .put(FunctionSet.DATE_SUB, Set.of(0, 1))
            .put(FunctionSet.HOURS_SUB, Set.of(0))
            .put(FunctionSet.MINUTES_SUB, Set.of(0))
            .put(FunctionSet.SECONDS_SUB, Set.of(0))
            .put(FunctionSet.MILLISECONDS_SUB, Set.of(0))
            .put(FunctionSet.MICROSECONDS_SUB, Set.of(0))
            .build();

    /**
     * Functions whose order behavior depends on a format argument. The evaluator checks the
     * format only for their two-argument shapes; wider arities go through unchecked, e.g.
     * from_unixtime(ts, '%d/%m/%Y', 'UTC'), and must be refused by the caller.
     */
    private static final Set<String> FUNCTIONS_WITH_FORMAT_ARG = Set.of(
            FunctionSet.DATE_FORMAT, FunctionSet.STR_TO_DATE, FunctionSet.STR2DATE,
            FunctionSet.FROM_UNIXTIME, FunctionSet.FROM_UNIXTIME_MS, FunctionSet.TO_DATETIME);

    /**
     * Exact preimages. Only functions whose preimage boundary is computable from the
     * constant alone are listed, including calendar-period floors (date_trunc). Partial
     * functions are registered separately below. Month/quarter/year shifts clamp
     * day-of-month (months_add('2024-01-31', 1) = '2024-02-29' = months_add('2024-01-30', 1))
     * and are NULL past the date range, so they are filter inverses below; time_slice buckets
     * are multiples of an interval counted from year 1, not calendar periods, and also keep
     * the image direction only.
     * timediff stays out: its FE fold floors the fractional second (Duration.getSeconds)
     * while BE truncates toward zero (integer microsecond division), so no single preimage
     * matches both. to_datetime stays out: epoch-grid scales plus zone-transition windows
     * for a typed DATETIME comparison are not worth the math while its image direction is
     * already refused by the two-argument format check.
     */
    private static final Map<String, PredicateInverse> EXACT_INVERSES = ImmutableMap.<String, PredicateInverse>builder()
            .put(FunctionSet.DATE_TRUNC, MonotonicInverse.PERIOD_FLOOR)
            .put(FunctionSet.YEAR, MonotonicInverse.YEAR_PERIOD)
            .put(FunctionSet.TO_DATE, MonotonicInverse.DAY_FLOOR)
            // date(x) is to_date(x): the same BE function, TimeFunctions::to_date
            .put(FunctionSet.DATE, MonotonicInverse.DAY_FLOOR)
            .put(FunctionSet.DATEDIFF, MonotonicInverse.DATEDIFF_DAYS)
            .put(FunctionSet.DATE_FORMAT, MonotonicInverse.RENDERED_PERIOD)
            .put(FunctionSet.TO_ISO8601, MonotonicInverse.ISO_RENDER)
            .put(FunctionSet.UNIX_TIMESTAMP, MonotonicInverse.UNIX_EPOCH)
            .put(FunctionSet.TO_DAYS, MonotonicInverse.DAY_NUMBER)
            .put(FunctionSet.LAST_DAY, MonotonicInverse.UNIT_END)
            .build();

    // These functions can return NULL for non-NULL input. Their bounds are useful for
    // scan pruning, but UNKNOWN must not become FALSE in SELECT, IS NULL, NOT, etc.
    private static final Map<String, PredicateInverse> FILTER_INVERSES = ImmutableMap.<String, PredicateInverse>builder()
            .put(FunctionSet.CONVERT_TZ, ConvertTzPredicateDerivation::invert)
            .put(FunctionSet.FROM_UNIXTIME, MonotonicInverse.EPOCH_RENDER)
            .put(FunctionSet.FROM_UNIXTIME_MS, MonotonicInverse.EPOCH_RENDER)
            .put(FunctionSet.NEXT_DAY, MonotonicInverse.DOW_NEXT)
            .put(FunctionSet.PREVIOUS_DAY, MonotonicInverse.DOW_PREVIOUS)
            .put(FunctionSet.DAYS_ADD, MonotonicInverse.shift(FunctionSet.DAYS_SUB))
            .put(FunctionSet.DAYS_SUB, MonotonicInverse.shift(FunctionSet.DAYS_ADD))
            .put(FunctionSet.WEEKS_ADD, MonotonicInverse.shift(FunctionSet.WEEKS_SUB))
            .put(FunctionSet.WEEKS_SUB, MonotonicInverse.shift(FunctionSet.WEEKS_ADD))
            .put(FunctionSet.HOURS_ADD, MonotonicInverse.shift(FunctionSet.HOURS_SUB))
            .put(FunctionSet.HOURS_SUB, MonotonicInverse.shift(FunctionSet.HOURS_ADD))
            .put(FunctionSet.MINUTES_ADD, MonotonicInverse.shift(FunctionSet.MINUTES_SUB))
            .put(FunctionSet.MINUTES_SUB, MonotonicInverse.shift(FunctionSet.MINUTES_ADD))
            .put(FunctionSet.SECONDS_ADD, MonotonicInverse.shift(FunctionSet.SECONDS_SUB))
            .put(FunctionSet.SECONDS_SUB, MonotonicInverse.shift(FunctionSet.SECONDS_ADD))
            .put(FunctionSet.MILLISECONDS_ADD, MonotonicInverse.shift(FunctionSet.MILLISECONDS_SUB))
            .put(FunctionSet.MILLISECONDS_SUB, MonotonicInverse.shift(FunctionSet.MILLISECONDS_ADD))
            // INTERVAL n.f SECOND becomes a whole-second shift followed by a microsecond shift
            .put(FunctionSet.MICROSECONDS_ADD, MonotonicInverse.shift(FunctionSet.MICROSECONDS_SUB))
            .put(FunctionSet.MICROSECONDS_SUB, MonotonicInverse.shift(FunctionSet.MICROSECONDS_ADD))
            .put(FunctionSet.ADDDATE, MonotonicInverse.shift(FunctionSet.SUBDATE))
            .put(FunctionSet.SUBDATE, MonotonicInverse.shift(FunctionSet.ADDDATE))
            .put(FunctionSet.DATE_ADD, MonotonicInverse.shift(FunctionSet.DATE_SUB))
            .put(FunctionSet.DATE_SUB, MonotonicInverse.shift(FunctionSet.DATE_ADD))
            .put(FunctionSet.MONTHS_ADD, MonotonicInverse.monthShift(1, true))
            .put(FunctionSet.ADD_MONTHS, MonotonicInverse.monthShift(1, true))
            .put(FunctionSet.MONTHS_SUB, MonotonicInverse.monthShift(1, false))
            .put(FunctionSet.QUARTERS_ADD, MonotonicInverse.monthShift(3, true))
            .put(FunctionSet.QUARTERS_SUB, MonotonicInverse.monthShift(3, false))
            .put(FunctionSet.YEARS_ADD, MonotonicInverse.monthShift(12, true))
            .put(FunctionSet.YEARS_SUB, MonotonicInverse.monthShift(12, false))
            .build();

    private static final Set<String> MONTH_SHIFTS = Set.of(FunctionSet.MONTHS_ADD, FunctionSet.ADD_MONTHS,
            FunctionSet.MONTHS_SUB, FunctionSet.QUARTERS_ADD, FunctionSet.QUARTERS_SUB, FunctionSet.YEARS_ADD,
            FunctionSet.YEARS_SUB);

    /** Shifts by whole months: they cut the day to the end of a shorter month and keep the time of day. */
    public static boolean isMonthShift(String fnName) {
        return MONTH_SHIFTS.contains(fnName.toLowerCase());
    }

    /**
     * Argument positions the data column may occupy, or null when the function is not
     * admitted for monotonic reasoning.
     */
    public static Set<Integer> dataArgPositions(String fnName) {
        return DATA_ARG_POSITIONS.get(fnName.toLowerCase());
    }

    public static boolean hasFormatArg(String fnName) {
        return FUNCTIONS_WITH_FORMAT_ARG.contains(fnName.toLowerCase());
    }

    /** Epoch-to-wall-clock renderings: their order depends on the session zone's transitions. */
    public static boolean isEpochRendering(String fnName) {
        String lower = fnName.toLowerCase();
        return FunctionSet.FROM_UNIXTIME.equals(lower) || FunctionSet.FROM_UNIXTIME_MS.equals(lower);
    }

    /**
     * The number of digits of the strings of a date_format pattern that renders digits only, or 0 for another pattern.
     * Such a pattern is ordered and has no text, and the BE renders its every row, see DateFormatModel. Its numeric
     * value is ordered like the dates, so a numeric cast on top keeps order when the digits fit the type of the cast.
     * Formats with separators do not cast to numbers.
     */
    public static int digitsOnlyLength(String format) {
        DateFormatModel model = DateFormatModel.of(format);
        return model.isDigitsOnly() && model.isExactOnAllRows() ? model.digitCount() : 0;
    }

    /**
     * The period of the date argument that a call takes one value on, or empty when the call has none that we know.
     * We expect the distinct values of such a call to be no more than the periods between the ends of the range of its
     * argument, so every function that has a period is counted the same way, see DatePeriod. The period depends on the
     * constants of the call: the unit of date_trunc, the weekday of next_day, the format of date_format.
     */
    public interface Granularity {
        Optional<DatePeriod> of(CallOperator call);
    }

    /**
     * The functions with a period. time_slice has none here: it cuts from the year 1 in steps of n units, which the FE fold
     * does, but no inverse of the registry states it, and the BE is not read for every unit. date and to_date are the cast
     * of a DATETIME to a DATE, and have the statistics of the cast.
     */
    private static final Map<String, Granularity> GRANULARITIES = ImmutableMap.<String, Granularity>builder()
            .put(FunctionSet.DATE_TRUNC, call -> constantText(call, 0)
                    .flatMap(unit -> DatePeriod.ofUnit(unit.toLowerCase())))
            .put(FunctionSet.LAST_DAY, MonotonicFunctionRegistry::lastDayPeriod)
            // next_day(x, d) = G holds exactly for x in [G-7, G), the week that starts on the day d
            .put(FunctionSet.NEXT_DAY, call -> Optional.ofNullable(MonotonicInverse.dowOf(call))
                    .map(DatePeriod::weeks))
            // previous_day(x, d) = G holds exactly for x in [G+1, G+8), the week that starts on the day after d
            .put(FunctionSet.PREVIOUS_DAY, call -> Optional.ofNullable(MonotonicInverse.dowOf(call))
                    .map(day -> DatePeriod.weeks(day.plus(1))))
            .put(FunctionSet.DATE_FORMAT, MonotonicFunctionRegistry::renderedPeriod)
            .build();

    public static Optional<DatePeriod> granularity(CallOperator call) {
        Granularity granularity = GRANULARITIES.get(call.getFnName().toLowerCase());
        return granularity == null ? Optional.empty() : granularity.of(call);
    }

    private static Optional<String> constantText(CallOperator call, int position) {
        if (call.getChildren().size() <= position || !call.getChild(position).isConstantRef()) {
            return Optional.empty();
        }
        ConstantOperator constant = (ConstantOperator) call.getChild(position);
        return constant.isNull() || !constant.getType().isStringType() ? Optional.empty()
                : Optional.of(constant.getVarchar());
    }

    // last_day(x) is the last day of the month of x, and the unit of the second argument can be a quarter or a year
    private static Optional<DatePeriod> lastDayPeriod(CallOperator call) {
        if (call.getChildren().size() == 1) {
            return DatePeriod.ofUnit("month");
        }
        if (call.getChildren().size() != 2) {
            return Optional.empty();
        }
        return constantText(call, 1).map(String::toLowerCase)
                .filter(unit -> unit.equals("month") || unit.equals("quarter") || unit.equals("year"))
                .flatMap(DatePeriod::ofUnit);
    }

    // The string of date_format is a function of the date cut to the finest field of the format. A DATE is rendered as its
    // midnight, so the fields below the day do not count for it.
    private static Optional<DatePeriod> renderedPeriod(CallOperator call) {
        Type argument = call.getChildren().isEmpty() ? null : call.getChild(0).getType();
        if (argument == null || !(argument.isDate() || argument.isDatetime())) {
            return Optional.empty();
        }
        return constantText(call, 1).flatMap(format -> {
            Optional<DateFormatModel.Field> finest = DateFormatModel.of(format).finestFieldUpTo(
                    argument.isDate() ? DateFormatModel.Field.DAY : DateFormatModel.Field.MICRO);
            return finest.isEmpty() ? Optional.of(DatePeriod.single()) : DatePeriod.ofField(finest.get());
        });
    }

    /**
     * Exact preimage for the function, or null when only the image direction is sound.
     */
    public static PredicateInverse exactInverse(String fnName) {
        return EXACT_INVERSES.get(fnName.toLowerCase());
    }
    /** Necessary condition for a positive filter; the caller must retain the original. */
    public static PredicateInverse filterInverse(String fnName) {
        return FILTER_INVERSES.get(fnName.toLowerCase());
    }

    /**
     * The single non-constant argument, which must sit in a registry-admitted position while
     * every other argument is a non-NULL constant; null when the call has no such shape.
     * The column must occur exactly once, as in the image direction: with two occurrences
     * the expression is not a single monotonic chain.
     */
    public static ScalarOperator dataChildOf(CallOperator call) {
        Set<Integer> admitted = dataArgPositions(call.getFnName());
        if (admitted == null) {
            return null;
        }
        List<ColumnRefOperator> usedColumns = call.getColumnRefs();
        if (usedColumns.size() != 1) {
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
