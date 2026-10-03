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

package com.starrocks.statistic;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/** Select labels from bounded heads before fetching their text into FE. Frequencies are never removed. */
final class JoinStatisticsHeadBudget {
    static final long TEXT_BYTES = 4L * 1024 * 1024;
    static final int GUARANTEED_BYTES = 256;

    private JoinStatisticsHeadBudget() { }

    static List<BitSet> select(List<int[]> lengths) {
        return select(lengths, TEXT_BYTES);
    }

    static List<BitSet> select(List<int[]> lengths, long budget) {
        List<BitSet> selected = new ArrayList<>();
        long shortBytes = 0;
        int active = 0;
        for (int[] domain : lengths) {
            BitSet keep = new BitSet(domain.length);
            boolean hasLong = false;
            for (int i = 0; i < domain.length; i++) {
                if (domain[i] < 0) {
                    throw new IllegalArgumentException("Negative JOIN head label length");
                }
                if (domain[i] <= GUARANTEED_BYTES) {
                    keep.set(i);
                    shortBytes += domain[i];
                } else {
                    hasLong = true;
                }
            }
            active += hasLong ? 1 : 0;
            selected.add(keep);
        }
        long remaining = Math.max(0, budget - shortBytes);
        if (active == 0 || remaining == 0) {
            return selected;
        }
        // Reserve an equal share for each textual domain with long labels, independent of collection order.
        long share = remaining / active;
        for (int d = 0; d < lengths.size(); d++) {
            int[] domain = lengths.get(d);
            long available = share;
            for (int i = 0; i < domain.length; i++) {
                if (!selected.get(d).get(i) && domain[i] <= available) {
                    selected.get(d).set(i);
                    available -= domain[i];
                    remaining -= domain[i];
                }
            }
        }
        // Donate unused shares, one candidate per domain per round, in head importance order.
        // Every position is visited at most once here; an oversized label cannot block later small ones.
        int[] next = new int[lengths.size()];
        boolean progress;
        do {
            progress = false;
            for (int d = 0; d < lengths.size(); d++) {
                int[] domain = lengths.get(d);
                while (next[d] < domain.length) {
                    int i = next[d]++;
                    if (!selected.get(d).get(i) && domain[i] <= remaining) {
                        selected.get(d).set(i);
                        remaining -= domain[i];
                        progress = true;
                        break;
                    }
                }
            }
        } while (progress);
        return selected;
    }
}
