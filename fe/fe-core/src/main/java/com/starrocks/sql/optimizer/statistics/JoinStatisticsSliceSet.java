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

import java.util.Arrays;

/** Owned sorted slice IDs. Hashing a prepared selection never boxes or rescans its IDs. */
final class JoinStatisticsSliceSet {
    private final int[] ids;
    private final int hash;

    private JoinStatisticsSliceSet(int[] source, boolean sorted) {
        ids = source.clone();
        if (!sorted) {
            Arrays.sort(ids);
        }
        for (int i = 0; i < ids.length; i++) {
            if (ids[i] < 0 || (i > 0 && ids[i - 1] >= ids[i])) {
                throw new IllegalArgumentException("Invalid JOIN statistics slice selection");
            }
        }
        hash = Arrays.hashCode(ids);
    }

    static JoinStatisticsSliceSet copyOf(int[] ids) {
        return new JoinStatisticsSliceSet(ids, false);
    }

    static JoinStatisticsSliceSet ordered(int[] ids) {
        return new JoinStatisticsSliceSet(ids, true);
    }

    // Package-private borrowed immutable storage; callers must not modify it.
    int[] ids() {
        return ids;
    }

    long estimatedSize() {
        return 48L + 4L * ids.length;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof JoinStatisticsSliceSet set
                && hash == set.hash && Arrays.equals(ids, set.ids);
    }
}
