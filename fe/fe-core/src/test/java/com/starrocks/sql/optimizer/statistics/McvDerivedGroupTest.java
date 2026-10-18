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

import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A group that a filter derives from another is made without building the views of its head that a loaded group builds
 * when it is made, and builds them when they are read. The views are the same.
 */
public class McvDerivedGroupTest {
    private static List<MultiColumnCombinedStats.McvEntry> head(boolean withComponentCounts) {
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            // Ties in the counts, which the order of the encounter breaks.
            long count = 100 - i / 3;
            entries.add(new MultiColumnCombinedStats.McvEntry(List.of(String.format("v%04d", i)), count,
                    withComponentCounts ? List.of(count) : List.of()));
        }
        return entries;
    }

    @Test
    public void testDerivedGroupReadsTheViewsOfALoadedOne() {
        for (boolean withComponentCounts : new boolean[] {true, false}) {
            long rows = 100_000;
            List<Long> nulls = Arrays.asList(7L);
            ExternalMcvStatistics.Group loaded = new ExternalMcvStatistics.Group(List.of("x"), rows, 301,
                    head(withComponentCounts), List.of(), nulls);
            ExternalMcvStatistics.Group derived = ExternalMcvStatistics.Group.derived(List.of("x"), rows, 301,
                    head(withComponentCounts), nulls);
            Assertions.assertEquals(loaded.getRowCount(), derived.getRowCount());
            Assertions.assertEquals(loaded.getNdv(), derived.getNdv());
            Assertions.assertEquals(loaded.getNullCounts(), derived.getNullCounts());
            Assertions.assertEquals(((PreparedMcvTuples) loaded.getMcv()).getComponentCounts(),
                    ((PreparedMcvTuples) derived.getMcv()).getComponentCounts());
            ColumnStatistic loadedStatistic = loaded.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown())
                    .orElseThrow();
            ColumnStatistic derivedStatistic = derived.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown())
                    .orElseThrow();
            Assertions.assertEquals(loadedStatistic.getNullsFraction(), derivedStatistic.getNullsFraction(), 0);
            Assertions.assertEquals(loadedStatistic.getDistinctValuesCount(), derivedStatistic.getDistinctValuesCount(), 0);
            McvDistribution expected = loadedStatistic.getHistogram().getMcvDistribution();
            McvDistribution actual = derivedStatistic.getHistogram().getMcvDistribution();
            Assertions.assertEquals(List.copyOf(expected.entrySet()), List.copyOf(actual.entrySet()));
            Assertions.assertEquals(expected.getTotalRows(), actual.getTotalRows());
            for (int rank = 0; rank < expected.size(); rank++) {
                Assertions.assertEquals(expected.getKeyByFrequency(rank), actual.getKeyByFrequency(rank), "rank " + rank);
                Assertions.assertEquals(expected.getCountByFrequency(rank), actual.getCountByFrequency(rank));
            }
        }
    }
}
