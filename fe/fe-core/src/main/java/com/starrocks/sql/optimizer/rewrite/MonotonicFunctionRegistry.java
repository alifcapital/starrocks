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

import java.util.Map;
import java.util.Set;

/**
 * Per-function facts for monotonic value reasoning, keyed by lower-case function name.
 * {@code ScalarOperatorEvaluator.isMonotonicFunction} only says "this function is monotonic
 * in some argument"; this registry says in which one, and which functions are admitted at
 * all. Functions not listed here are refused.
 */
public final class MonotonicFunctionRegistry {

    private MonotonicFunctionRegistry() {
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
     * Argument positions the data column may occupy, or null when the function is not
     * admitted for monotonic reasoning.
     */
    public static Set<Integer> dataArgPositions(String fnName) {
        return DATA_ARG_POSITIONS.get(fnName.toLowerCase());
    }

    public static boolean isFormatBearing(String fnName) {
        return FORMAT_BEARING_FUNCTIONS.contains(fnName.toLowerCase());
    }
}
