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

import java.util.OptionalDouble;

/**
 * The statistics of a prefix of a string column, taken by left, strleft, substr(c, 1, n) or substring(c, 1, n), from
 * the NDV and the average length of the column alone.
 *
 * We expect the first characters of codes to repeat, as the country code of a phone or the year of a date, while the
 * distinct values of a column differ along the whole length of the strings. So the first characters tell some of the
 * values apart and all of them tell every value apart. A prefix of k of the L characters of an average value then
 * holds D^(k/L) distinct values, where D is the NDV of the column: one value for no characters, D for all of them, and
 * in between the prefix takes the same share of the logarithm of D as of the length. So a prefix is not given the NDV
 * of the column, which a short prefix of a long string does not have, nor one value, which a prefix that tells some
 * values apart does not have. The NDV is never above that of the column and never above the rows of the input.
 *
 * A part elsewhere in the string (a suffix of a phone, a middle of a date written day first, a part of a random id)
 * can tell as many values apart as the whole string. The NDV of the column would make an equality on it keep one row,
 * and the formula above is too low for it, so it is not given by this class and has no statistics.
 *
 * Nothing is known of the range of the prefix, so it is unbounded, and no histogram is kept, as the values of the
 * column are not the values of the prefix. The NULL rows are those of the column: the prefix of a string is NULL only
 * when the string is. A column without statistics gives a prefix without statistics, so the condition on the prefix
 * is estimated by the default path and not as a condition on a column of one value.
 *
 * The length L is the average size of the column, which is counted in bytes, while the prefixes are counted in
 * characters. The statistics hold no length in characters, so for a string of multi-byte characters, such as
 * Cyrillic with two bytes each, the share k/L is too small by the bytes per character, and the NDV of the prefix is
 * too low.
 */
final class SubstringStatistics {
    private SubstringStatistics() {
    }

    /**
     * The statistics of a prefix of at most the given number of characters. NaN stands for a prefix of unknown length,
     * which is taken as the whole string.
     */
    static ColumnStatistic of(ColumnStatistic source, double characters, double rowCount) {
        if (source.isUnknown()) {
            return source;
        }
        double length = averageLength(source);
        double kept = Double.isNaN(characters) ? length : Math.min(Math.max(0, characters), length);
        double distinct = source.getDistinctValuesCount();
        double ndv = distinct <= 1 ? distinct : Math.min(Math.pow(distinct, kept / length), Math.max(1, rowCount));
        return ColumnStatistic.builder()
                .setMinValue(Double.NEGATIVE_INFINITY)
                .setMaxValue(Double.POSITIVE_INFINITY)
                .setNullsFraction(source.getNullsFraction())
                .setAverageRowSize(Math.max(1, kept))
                .setDistinctValuesCount(ndv)
                // The NDV of the part follows the one of the column, so it is a guess when the one of the column is.
                .setDistinctValuesFromDefaultSelectivity(source.isDistinctValuesFromDefaultSelectivity())
                .build();
    }

    /** The characters of left(c, n) and substr(c, 1, n), NaN when n is not a constant. */
    static double charactersOfLength(OptionalDouble length) {
        return length.isPresent() ? length.getAsDouble() : Double.NaN;
    }

    // The average value takes at least one character: a column without a size is read as one of single characters.
    private static double averageLength(ColumnStatistic source) {
        double size = source.getAverageRowSize();
        return Double.isNaN(size) || size < 1 ? 1 : size;
    }
}
