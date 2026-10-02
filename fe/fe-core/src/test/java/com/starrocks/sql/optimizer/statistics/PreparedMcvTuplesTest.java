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

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PreparedMcvTuplesTest {
    private static MultiColumnCombinedStats.McvEntry entry(long count, String... values) {
        return new MultiColumnCombinedStats.McvEntry(Arrays.asList(values), count);
    }

    @Test
    void snapshotSharingAndRebinding() {
        var tuples = new ArrayList<>(List.of(entry(7, "a", null), entry(4, "a", "b"), entry(3, "a", "b")));
        var group = new ExternalMcvStatistics.Group(List.of("a", "b"), 20, 3, tuples, List.of(), List.of(0L, 7L));
        var a = new ColumnRefOperator(1, VarcharType.VARCHAR, "a", false);
        var b = new ColumnRefOperator(2, VarcharType.VARCHAR, "b", true);
        var stats = new MultiColumnCombinedStats(3, 20, List.of(a, b), group.getMcv(), group.getNullCounts());
        assertSame(group.getMcv(), stats.getMcvDistribution());
        tuples.clear();
        assertEquals(3, stats.getMcv().size());
        assertEquals(14, stats.getMcvDistribution().getTotalRowsLong());
        assertThrows(UnsupportedOperationException.class, () -> stats.getMcv().clear());
        var scaled = new MultiColumnCombinedStats(3, 40, List.of(a, b), group.getMcv(), group.getNullCounts());
        assertNotSame(stats.getMcvDistribution(), scaled.getMcvDistribution());
        assertEquals(7.0 / 40, scaled.getMcvDistribution().getShare(0));
        assertEquals(7.0 / 20, stats.getMcvDistribution().getShare(0));
        var changedNulls = PreparedMcvTuples.copyOf(group.getMcv(), 2, 20, List.of(0L, 9L));
        assertEquals(9L, changedNulls.getComponentCounts().get(1).get(null));
        assertEquals(7L, stats.getMcvDistribution().getComponentCounts().get(1).get(null));
    }

    @Test
    void preservesAllReductionModesAndNulls() {
        var random = new Random(8331);
        for (int trial = 0; trial < 100; trial++) {
            List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
            for (int i = 0; i < 80; i++) {
                entries.add(entry(trial == 0 ? Long.MAX_VALUE : random.nextLong() & Long.MAX_VALUE,
                        i % 3 == 0 ? null : "v"));
            }
            var prepared = PreparedMcvTuples.copyOf(entries, 1, 1e21, List.of());
            assertEquals(entries.stream().mapToLong(MultiColumnCombinedStats.McvEntry::getCount).sum(),
                    prepared.getTotalRowsLong());
            assertEquals(entries.stream().mapToDouble(MultiColumnCombinedStats.McvEntry::getCount).sum(),
                    prepared.getTotalRows());
            double sequential = 0;
            double shares = 0;
            for (var entry : entries) {
                sequential += entry.getCount();
                shares += entry.getCount() / 1e21;
            }
            assertEquals(sequential, prepared.getSequentialTotalRows());
            assertEquals(shares, prepared.getTotalShare());
            assertEquals(entries.stream().filter(e -> e.getValues().get(0) == null)
                    .mapToDouble(MultiColumnCombinedStats.McvEntry::getCount).sum(), prepared.getNullRows(0));
            assertTrue(prepared.hasNull(0));
        }
    }

    @Test
    void componentIndexesPreserveFirstCountAndAreSafeForConcurrentReaders() throws Exception {
        var entries = List.of(new MultiColumnCombinedStats.McvEntry(Arrays.asList("x", null), 3, List.of(7L, 8L)),
                new MultiColumnCombinedStats.McvEntry(List.of("x", "y"), 4, List.of(9L, 4L)));
        var prepared = PreparedMcvTuples.copyOf(entries, 2, 20, List.of(0L, 10L));
        long weight = prepared.retainedIndexBytes();
        var executor = Executors.newFixedThreadPool(4);
        try {
            List<Callable<List<Map<String, Long>>>> tasks = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                tasks.add(prepared::getComponentCounts);
            }
            var results = executor.invokeAll(tasks);
            for (var result : results) {
                assertSame(prepared.getComponentCounts(), result.get());
            }
        } finally {
            executor.shutdownNow();
        }
        assertEquals(7L, prepared.getComponentCounts().get(0).get("x"));
        assertEquals(10L, prepared.getComponentCounts().get(1).get(null));
        assertThrows(UnsupportedOperationException.class, () -> prepared.getComponentCounts().get(0).put("x", 0L));
        assertEquals(weight, prepared.retainedIndexBytes());
    }

    @Test
    void bindingIndexesTrackTypeAndKeyOrder() {
        var a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        var b = new ColumnRefOperator(2, VarcharType.VARCHAR, "b", true);
        var stats = new MultiColumnCombinedStats(3, 100, List.of(a, b),
                List.of(entry(20, "01", "x"), entry(30, "1", "y")), List.of(0L, 0L));
        var first = stats.getJoinHead(List.of(a, b));
        assertSame(first, stats.getJoinHead(List.of(a, b)));
        assertNotSame(first, stats.getJoinHead(List.of(b, a)));
        a.setType(VarcharType.VARCHAR);
        assertNotSame(first, stats.getJoinHead(List.of(a, b)));
        assertSame(stats.getComponentShares(), stats.getComponentShares());
    }
}
