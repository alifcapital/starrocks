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

import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExternalMcvStatsAttachTest {
    private static final ColumnRefOperator STATUS = new ColumnRefOperator(1, VarcharType.VARCHAR, "status", true);
    private static final ColumnRefOperator GATE = new ColumnRefOperator(2, IntegerType.INT, "gate", true);
    private static final ColumnRefOperator EXTRA = new ColumnRefOperator(4, IntegerType.INT, "extra", true);

    private static final List<MultiColumnCombinedStats.McvEntry> MCV = List.of(
            new MultiColumnCombinedStats.McvEntry(List.of("approved", "0", "0"), 500, List.of(600L, 620L, 750L)));

    private static Map<ColumnRefOperator, Column> read(ColumnRefOperator... refs) {
        Map<ColumnRefOperator, Column> map = new HashMap<>();
        for (ColumnRefOperator ref : refs) {
            map.put(ref, new Column(ref.getName(), ref.getType()));
        }
        return map;
    }

    private static Statistics attach(List<ExternalMcvStatistics.Group> groups,
                                     Map<ColumnRefOperator, Column> read) {
        return StatisticsCalcUtils.attachExternalMcvStats(Statistics.builder().setOutputRowCount(1000).build(),
                new ExternalMcvStatistics(groups), read);
    }

    @Test
    public void testGroupsWithUnreadColumnsAreKeptForTheirMcv() {
        Statistics statistics = attach(List.of(
                new ExternalMcvStatistics.Group(List.of("status", "gate", "type"), 1000, 12, MCV),
                new ExternalMcvStatistics.Group(List.of("status", "extra", "type"), 1000, 30, List.of()),
                new ExternalMcvStatistics.Group(List.of("gate", "extra"), 1000, 20, List.of()),
                new ExternalMcvStatistics.Group(List.of("status", "type"), 1000, 4, MCV)),
                read(STATUS, GATE, EXTRA));
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups = statistics.getMultiColumnCombinedStats();
        Assertions.assertEquals(2, groups.size());

        MultiColumnCombinedStats partial = groups.get(Set.of(STATUS, GATE));
        Assertions.assertEquals(Arrays.asList(STATUS, GATE, null), partial.getColumns());
        Assertions.assertFalse(partial.isComplete());
        Assertions.assertTrue(partial.hasMcv());
        Assertions.assertEquals(MCV, partial.getMcv());

        MultiColumnCombinedStats complete = groups.get(Set.of(GATE, EXTRA));
        Assertions.assertTrue(complete.isComplete());
        Assertions.assertEquals(20, complete.getNdv());

        // NDV lookups see the complete group only.
        Assertions.assertNull(statistics.getLargestSubsetMCStats(Set.of(STATUS, GATE)));
        Assertions.assertEquals(Set.of(GATE, EXTRA),
                statistics.getLargestSubsetMCStats(Set.of(STATUS, GATE, EXTRA)).first);
    }

    @Test
    public void testCompleteGroupWinsOverOneWithUnreadColumns() {
        List<MultiColumnCombinedStats.McvEntry> pairMcv = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("approved", "0"), 550, List.of(600L, 620L)));
        Statistics statistics = attach(List.of(
                new ExternalMcvStatistics.Group(List.of("status", "gate", "type"), 1000, 12, MCV),
                new ExternalMcvStatistics.Group(List.of("status", "gate"), 1000, 5, pairMcv)),
                read(STATUS, GATE));
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups = statistics.getMultiColumnCombinedStats();
        Assertions.assertEquals(1, groups.size());
        MultiColumnCombinedStats stats = groups.get(Set.of(STATUS, GATE));
        Assertions.assertTrue(stats.isComplete());
        Assertions.assertEquals(5, stats.getNdv());
        Assertions.assertEquals(pairMcv, stats.getMcv());
        Assertions.assertEquals(Set.of(STATUS, GATE), statistics.getLargestSubsetMCStats(Set.of(STATUS, GATE)).first);
    }

    @Test
    public void testNothingToAttachLeavesTheStatisticsAlone() {
        Statistics input = Statistics.builder().setOutputRowCount(1000).build();
        Statistics statistics = StatisticsCalcUtils.attachExternalMcvStats(input,
                new ExternalMcvStatistics(List.of(
                        new ExternalMcvStatistics.Group(List.of("status", "gate", "type"), 1000, 12, MCV))),
                read(STATUS, EXTRA));
        Assertions.assertSame(input, statistics);
    }
}
