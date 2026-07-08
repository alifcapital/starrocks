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
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

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
     * Exact preimage of {@code { x : f(..., x, ...) cmp value }} as a predicate on the data
     * argument. Empty = refuse; a refusal only loses the rewrite. Implementations never throw
     * and never accept NULL where the original predicate would not.
     */
    public interface ExactInverse {
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
            .put(FunctionSet.TO_DATETIME, Set.of(0))
            .put(FunctionSet.TO_DAYS, Set.of(0))
            .put(FunctionSet.TO_DATE, Set.of(0))
            // add/sub functions shift a date by a constant amount: monotonic in the date
            // argument. The column is not allowed in the amount argument: for subs the result
            // would decrease while the column grows.
            .put(FunctionSet.YEARS_ADD, Set.of(0))
            .put(FunctionSet.QUARTERS_ADD, Set.of(0))
            .put(FunctionSet.MONTHS_ADD, Set.of(0))
            .put(FunctionSet.ADD_MONTHS, Set.of(0))
            .put(FunctionSet.WEEKS_ADD, Set.of(0))
            .put(FunctionSet.DAYS_ADD, Set.of(0))
            .put(FunctionSet.ADDDATE, Set.of(0))
            .put(FunctionSet.DATE_ADD, Set.of(0))
            .put(FunctionSet.HOURS_ADD, Set.of(0))
            .put(FunctionSet.MINUTES_ADD, Set.of(0))
            .put(FunctionSet.SECONDS_ADD, Set.of(0))
            .put(FunctionSet.MILLISECONDS_ADD, Set.of(0))
            .put(FunctionSet.YEARS_SUB, Set.of(0))
            .put(FunctionSet.QUARTERS_SUB, Set.of(0))
            .put(FunctionSet.MONTHS_SUB, Set.of(0))
            .put(FunctionSet.WEEKS_SUB, Set.of(0))
            .put(FunctionSet.DAYS_SUB, Set.of(0))
            .put(FunctionSet.SUBDATE, Set.of(0))
            .put(FunctionSet.DATE_SUB, Set.of(0))
            .put(FunctionSet.HOURS_SUB, Set.of(0))
            .put(FunctionSet.MINUTES_SUB, Set.of(0))
            .put(FunctionSet.SECONDS_SUB, Set.of(0))
            .put(FunctionSet.MILLISECONDS_SUB, Set.of(0))
            .build();

    /**
     * Functions whose order behavior depends on a format argument. The evaluator's format
     * check covers only their two-argument shapes; wider arities go through unchecked, e.g.
     * from_unixtime(ts, '%d/%m/%Y', 'UTC'), and must be refused by the caller.
     */
    private static final Set<String> FORMAT_BEARING_FUNCTIONS = Set.of(
            FunctionSet.DATE_FORMAT, FunctionSet.STR_TO_DATE, FunctionSet.STR2DATE,
            FunctionSet.FROM_UNIXTIME, FunctionSet.FROM_UNIXTIME_MS, FunctionSet.TO_DATETIME);

    /**
     * Exact preimages. Only functions whose preimage boundary is computable from the
     * constant alone are listed: calendar-period floors (date_trunc) and fixed-duration
     * shifts (day and finer, plus weeks: 7-day fixed). Month/quarter/year shifts clamp
     * day-of-month and are NOT invertible (months_add('2024-01-31', 1) = '2024-02-29' =
     * months_add('2024-01-30', 1)); time_slice buckets are epoch-anchored multiples of an
     * interval, not calendar periods, and stay image-only too.
     */
    private static final Map<String, ExactInverse> EXACT_INVERSES = ImmutableMap.<String, ExactInverse>builder()
            .put(FunctionSet.DATE_TRUNC, MonotonicInverse.PERIOD_FLOOR)
            .put(FunctionSet.YEAR, MonotonicInverse.YEAR_PERIOD)
            .put(FunctionSet.TO_DATE, MonotonicInverse.DAY_FLOOR)
            .put(FunctionSet.DATEDIFF, MonotonicInverse.DATEDIFF_DAYS)
            .put(FunctionSet.DATE_FORMAT, MonotonicInverse.RENDERED_PERIOD)
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
            .put(FunctionSet.ADDDATE, MonotonicInverse.shift(FunctionSet.SUBDATE))
            .put(FunctionSet.SUBDATE, MonotonicInverse.shift(FunctionSet.ADDDATE))
            .put(FunctionSet.DATE_ADD, MonotonicInverse.shift(FunctionSet.DATE_SUB))
            .put(FunctionSet.DATE_SUB, MonotonicInverse.shift(FunctionSet.DATE_ADD))
            .build();

    /**
     * Argument positions the data column may occupy, or null when the function is not
     * admitted for monotonic reasoning.
     */
    public static Set<Integer> dataArgPositions(String fnName) {
        return DATA_ARG_POSITIONS.get(fnName.toLowerCase());
    }

    public static boolean isFormatBearing(String fnName) {
        return FORMAT_BEARING_FUNCTIONS.contains(fnName.toLowerCase());
    }

    /**
     * Exact preimage for the function, or null when only the image direction is sound.
     */
    public static ExactInverse exactInverse(String fnName) {
        return EXACT_INVERSES.get(fnName.toLowerCase());
    }
}
