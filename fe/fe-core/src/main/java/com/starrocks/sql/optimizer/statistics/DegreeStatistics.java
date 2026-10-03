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

/** Distribution of non-NULL join keys inside one predicate slice. */
public final class DegreeStatistics {
    public static final int MOMENT_COUNT = 10;

    private final long rowCount;
    private final long nullCount;
    private final long distinctCount;
    private final long maximumFrequency;
    private final double[] moments;

    public DegreeStatistics(long rowCount, long nullCount, long distinctCount, long maximumFrequency,
                            double[] moments) {
        if (rowCount < 0 || nullCount < 0 || nullCount > rowCount || distinctCount < 0
                || distinctCount > rowCount - nullCount || maximumFrequency < 0
                || maximumFrequency > rowCount - nullCount || moments.length != MOMENT_COUNT
                || (distinctCount == 0) != (rowCount == nullCount)
                || (maximumFrequency == 0) != (distinctCount == 0)) {
            throw new IllegalArgumentException("Invalid degree statistics");
        }
        double previous = 0;
        for (double moment : moments) {
            if (!Double.isFinite(moment) || moment < previous
                    || (distinctCount == 0) != (moment == 0)) {
                throw new IllegalArgumentException("Invalid frequency moment");
            }
            previous = moment;
        }
        if (Math.abs(moments[0] - (rowCount - nullCount)) > Math.max(1, rowCount - nullCount) * 1e-9) {
            throw new IllegalArgumentException("First moment differs from non-NULL row count");
        }
        this.rowCount = rowCount;
        this.nullCount = nullCount;
        this.distinctCount = distinctCount;
        this.maximumFrequency = maximumFrequency;
        this.moments = moments.clone();
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getNullCount() {
        return nullCount;
    }

    public long getDistinctCount() {
        return distinctCount;
    }

    public long getMaximumFrequency() {
        return maximumFrequency;
    }

    public double getMoment(int power) {
        if (power < 1 || power > MOMENT_COUNT) {
            throw new IllegalArgumentException("Unsupported frequency moment");
        }
        return moments[power - 1];
    }

    public long estimatedSize() {
        return 160;
    }
}
