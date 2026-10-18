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

package com.starrocks.sql.optimizer.statistics;

/**
 * The smallest and the largest string of a column, for estimates only. A bound can be a prefix of the values it
 * stands for: ANALYZE keeps the first 200 characters of the minimum and the maximum, and the metrics of an Iceberg
 * file keep the first characters its metrics mode allows, with the last character of the upper bound increased by
 * one. So these bounds never become predicates: a predicate built from a cut maximum would drop the rows above it.
 * Only the statistics of a cast of text to DATE or DATETIME read them, see CastStatisticsDerivation; a comparison of
 * the strings themselves does not. DeriveRangeJoinPredicateRule, TopNAggregationCost and the other readers of the
 * connector bounds read getMinString and getMaxString of ColumnStatistic, which these bounds never change.
 *
 * @param cutLength      the length in characters past which a value is cut, WHOLE when the bounds are whole values,
 *                       UNKNOWN when values may be cut at any length
 * @param incrementedMax whether a cut upper bound has its last character increased by one, as Iceberg writes it
 */
public record StringBounds(String min, String max, int cutLength, boolean incrementedMax) {
    public static final int WHOLE = 0;
    public static final int UNKNOWN = -1;

    /** Whether the lower bound may be a prefix of the values it stands for. */
    public boolean minMayBeCut() {
        return cutLength == UNKNOWN || (cutLength > 0 && min.codePointCount(0, min.length()) >= cutLength);
    }

    /** Whether the upper bound may be a prefix of larger values. */
    public boolean maxMayBeCut() {
        return cutLength == UNKNOWN || (cutLength > 0 && max.codePointCount(0, max.length()) >= cutLength);
    }
}
