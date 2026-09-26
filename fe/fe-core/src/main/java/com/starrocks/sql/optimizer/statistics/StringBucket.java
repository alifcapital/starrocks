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

/** A residual string interval; counts are cumulative, just as for numeric buckets. */
public final class StringBucket extends Bucket {
    private final String lowerString;
    private final String upperString;
    private final boolean lowerInclusive;
    private final boolean upperInclusive;

    public StringBucket(String lower, String upper, long count, long repeats, long ndv) {
        this(lower, upper, count, repeats, ndv, true, true);
    }

    public StringBucket(String lower, String upper, long count, long repeats, long ndv,
                        boolean lowerInclusive, boolean upperInclusive) {
        // Numeric consumers cannot interpret string endpoints as numbers or dates.
        super(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, count, repeats, ndv);
        this.lowerString = lower;
        this.upperString = upper;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
    }

    public String getLowerString() {
        return lowerString;
    }
    public String getUpperString() {
        return upperString;
    }
    public boolean isLowerInclusive() {
        return lowerInclusive;
    }
    public boolean isUpperInclusive() {
        return upperInclusive;
    }

    public static int compare(String left, String right) {
        // UTF-8 preserves Unicode scalar-value order. Comparing code points avoids allocating
        // two encoded byte arrays per comparison, unlike String.compareTo's UTF-16 code-unit order.
        // Match the JDK UTF-8 encoder's '?' replacement for unpaired surrogates as well.
        int leftIndex = 0;
        int rightIndex = 0;
        while (leftIndex < left.length() && rightIndex < right.length()) {
            int leftPoint = left.codePointAt(leftIndex);
            int rightPoint = right.codePointAt(rightIndex);
            leftIndex += Character.charCount(leftPoint);
            rightIndex += Character.charCount(rightPoint);
            int leftValue = leftPoint >= Character.MIN_SURROGATE && leftPoint <= Character.MAX_SURROGATE ? '?' : leftPoint;
            int rightValue = rightPoint >= Character.MIN_SURROGATE && rightPoint <= Character.MAX_SURROGATE ? '?' : rightPoint;
            if (leftValue != rightValue) {
                return Integer.compare(leftValue, rightValue);
            }
        }
        return Integer.compare(left.length() - leftIndex, right.length() - rightIndex);
    }

    public double pointRows(String value, long mass) {
        int low = compare(value, lowerString);
        int high = compare(value, upperString);
        if (low < 0 || high > 0 || (low == 0 && !lowerInclusive) || (high == 0 && !upperInclusive)) {
            return 0;
        }
        if (high == 0) {
            return getUpperRepeats();
        }
        return Math.min(mass, Math.max(0, mass - getUpperRepeats())
                / (double) Math.max(1, getDistinctCount().orElse(1L) - (getUpperRepeats() > 0 ? 1 : 0)));
    }

    /** Rank estimate; no numerical distance between strings is assumed. */
    public double lessRows(String value, boolean inclusive, long mass, boolean excludedValue) {
        int low = compare(value, lowerString);
        int high = compare(value, upperString);
        if (low < 0) {
            return 0;
        }
        if (high > 0) {
            return mass;
        }
        double equal = excludedValue ? 0 : pointRows(value, mass);
        if (high == 0) {
            return inclusive ? mass : mass - equal;
        }
        if (low == 0) {
            return inclusive ? equal : 0;
        }
        double below = Math.max(0, (mass - getUpperRepeats() - equal) / 2.0);
        return inclusive ? below + equal : below;
    }
}
