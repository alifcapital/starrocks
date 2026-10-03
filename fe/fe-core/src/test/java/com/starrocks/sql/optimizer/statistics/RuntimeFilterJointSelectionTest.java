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

import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

public class RuntimeFilterJointSelectionTest {
    private static final ColumnRefOperator X = new ColumnRefOperator(1, IntegerType.BIGINT, "x", true);
    private static final ColumnRefOperator Y = new ColumnRefOperator(2, IntegerType.BIGINT, "y", true);

    private MultiColumnCombinedStats group(double rows, MultiColumnCombinedStats.McvEntry... entries) {
        return new MultiColumnCombinedStats(entries.length, rows, List.of(X, Y), List.of(entries));
    }

    private MultiColumnCombinedStats.McvEntry tuple(String x, String y, long rows) {
        return new MultiColumnCombinedStats.McvEntry(Arrays.asList(x, y), rows);
    }

    private RuntimeFilterStatistics build(ColumnRefOperator key, String value, long count, long rows) {
        MultiColumnCombinedStats group = new MultiColumnCombinedStats(2, rows, List.of(key),
                List.of(new MultiColumnCombinedStats.McvEntry(Arrays.asList(value), count)));
        return RuntimeFilterStatistics.from(key, new ColumnStatistic(0, 10, 0, 8, 2), List.of(group), rows);
    }

    private List<RuntimeFilterJointSelection.Key> keys() {
        return List.of(new RuntimeFilterJointSelection.Key(X, build(X, "1", 10, 10), false),
                new RuntimeFilterJointSelection.Key(Y, build(Y, "10", 10, 10), false));
    }

    private BitSet select(MultiColumnCombinedStats probe) {
        return RuntimeFilterJointSelection.select(keys(), List.of(probe), 0.5);
    }

    @Test
    public void testUnreadMcvComponentsKeepOriginalTuplePositions() {
        var single = new MultiColumnCombinedStats(2, 100, Arrays.asList(X, null),
                List.of(tuple("1", "unused", 50), tuple("2", "unused", 50)));
        Assertions.assertEquals(2, select(single).cardinality());
        // The unused component must neither crash the tie-break nor shift Y's tuple position.
        for (int unread = 0; unread < 3; unread++) {
            var columns = new ArrayList<>(List.of(X, Y));
            columns.add(unread, null);
            var entries = new ArrayList<MultiColumnCombinedStats.McvEntry>();
            for (var pair : List.of(List.of("1", "10"), List.of("2", "20"))) {
                var values = new ArrayList<>(pair);
                values.add(unread, "not-a-number");
                entries.add(new MultiColumnCombinedStats.McvEntry(values, 50));
            }
            Assertions.assertEquals(BitSet.valueOf(new long[] {1}),
                    select(new MultiColumnCombinedStats(2, 100, columns, entries)));
        }
    }

    @Test
    public void testExpressionWithoutSourceCannotBindUnreadComponent() {
        var probe = new MultiColumnCombinedStats(2, 100, Arrays.asList(X, null),
                List.of(tuple("1", "10", 50), tuple("2", "20", 50)));
        var candidates = List.of(keys().get(0), new RuntimeFilterJointSelection.Key(
                ConstantOperator.createBigint(10), build(Y, "10", 10, 10), false));
        Assertions.assertEquals(2, RuntimeFilterJointSelection.select(candidates, List.of(probe), 0.5).cardinality());
    }

    @Test
    public void testRedundantFiltersKeepOne() {
        Assertions.assertEquals(BitSet.valueOf(new long[] {1}), select(group(100,
                tuple("1", "10", 50), tuple("2", "20", 50))));
    }

    @Test
    public void testComplementaryFiltersKeepBoth() {
        Assertions.assertEquals(2, select(group(100, tuple("1", "10", 25), tuple("1", "20", 25),
                tuple("2", "10", 25), tuple("2", "20", 25))).cardinality());
    }

    @Test
    public void testPickMoreSelectiveComponentRegardlessOfOrder() {
        Assertions.assertEquals(BitSet.valueOf(new long[] {2}), select(group(100,
                tuple("1", "10", 20), tuple("1", "20", 30), tuple("2", "20", 50))));
    }

    @Test
    public void testSmallUnknownTailCannotJustifyRedundantFilter() {
        Assertions.assertEquals(1, select(group(100, tuple("1", "10", 49), tuple("2", "20", 50)))
                .cardinality());
    }

    @Test
    public void testLargeUnknownTailPreservesPotentiallyUsefulFilter() {
        Assertions.assertEquals(2, select(group(100, tuple("1", "10", 10), tuple("2", "20", 50)))
                .cardinality());
    }

    @Test
    public void testUnseenBuildKeysAreNotAbsent() {
        List<RuntimeFilterJointSelection.Key> incomplete = List.of(
                new RuntimeFilterJointSelection.Key(X, build(X, "1", 5, 10), false),
                new RuntimeFilterJointSelection.Key(Y, build(Y, "10", 5, 10), false));
        Assertions.assertEquals(2, RuntimeFilterJointSelection.select(incomplete,
                List.of(group(100, tuple("2", "20", 100))), 0.5).cardinality());
    }

    @Test
    public void testNoJointStatsKeepsIndependentDecisions() {
        Assertions.assertEquals(2, RuntimeFilterJointSelection.select(keys(), List.of(), 0.5).cardinality());
    }

    @Test
    public void testOrdinaryEqualityRejectsNullTupleComponent() {
        Assertions.assertEquals(2, select(group(100, tuple("1", "10", 25), tuple("1", null, 25),
                tuple(null, "10", 25), tuple(null, null, 25))).cardinality());
    }

    @Test
    public void testNullSafeMembership() {
        List<RuntimeFilterJointSelection.Key> nullKeys = List.of(
                new RuntimeFilterJointSelection.Key(X, build(X, null, 10, 10), true),
                new RuntimeFilterJointSelection.Key(Y, build(Y, null, 10, 10), true));
        Assertions.assertEquals(1, RuntimeFilterJointSelection.select(nullKeys,
                List.of(group(100, tuple(null, null, 50), tuple("1", "10", 50))), 0.5).cardinality());
    }

    @Test
    public void testKnownCastCollisionsAndInvalidValues() {
        ColumnRefOperator text = new ColumnRefOperator(3, VarcharType.VARCHAR, "text", true);
        MultiColumnCombinedStats probe = new MultiColumnCombinedStats(3, 100, List.of(text, Y), List.of(
                tuple("1", "10", 25), tuple("01", "10", 25), tuple("bad", "20", 50)));
        List<RuntimeFilterJointSelection.Key> casts = List.of(
                new RuntimeFilterJointSelection.Key(new CastOperator(IntegerType.BIGINT, text),
                        build(X, "1", 10, 10), false), keys().get(1));
        Assertions.assertEquals(1, RuntimeFilterJointSelection.select(casts, List.of(probe), 0.5).cardinality());
    }

    @Test
    public void testMultipleThresholdsUseIncrementalRejection() {
        MultiColumnCombinedStats probe = group(100, tuple("1", "10", 40), tuple("1", "20", 10),
                tuple("2", "10", 10), tuple("2", "20", 40));
        Assertions.assertEquals(1, RuntimeFilterJointSelection.select(keys(), List.of(probe), 0.5).cardinality());
        Assertions.assertEquals(2, RuntimeFilterJointSelection.select(keys(), List.of(probe), 0.2).cardinality());
    }
    @Test
    public void testReconsiderCandidateAfterAnotherFilterChangesSurvivors() {
        ColumnRefOperator z = new ColumnRefOperator(3, IntegerType.BIGINT, "z", true);
        MultiColumnCombinedStats probe = new MultiColumnCombinedStats(6, 100, List.of(X, Y, z), List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("1", "1", "1"), 5),
                new MultiColumnCombinedStats.McvEntry(List.of("1", "2", "1"), 5),
                new MultiColumnCombinedStats.McvEntry(List.of("1", "1", "2"), 40),
                new MultiColumnCombinedStats.McvEntry(List.of("2", "2", "1"), 40),
                new MultiColumnCombinedStats.McvEntry(List.of("2", "2", "2"), 5),
                new MultiColumnCombinedStats.McvEntry(List.of("2", "1", "2"), 5)));
        List<RuntimeFilterJointSelection.Key> filters = List.of(X, Y, z).stream()
                .map(key -> new RuntimeFilterJointSelection.Key(key, build(key, "1", 10, 10), false)).toList();
        // Each rejects 50% alone. After X, Y rejects only 10%, but after Z it rejects 50% again.
        Assertions.assertEquals(3, RuntimeFilterJointSelection.select(filters, List.of(probe), 0.5).cardinality());
    }

    @Test
    public void testIncompleteHeadsAgainstFullDistribution() {
        Random random = new Random(20260922L);
        for (int trial = 0; trial < 300; trial++) {
            int n = 2 + random.nextInt(4);
            int tuples = 1 << n;
            long[] counts = new long[tuples];
            long rows = 0;
            List<ColumnRefOperator> columns = new ArrayList<>();
            List<RuntimeFilterJointSelection.Key> filters = new ArrayList<>();
            boolean[] containsTwo = new boolean[n];
            for (int k = 0; k < n; k++) {
                ColumnRefOperator column = new ColumnRefOperator(k + 1, IntegerType.BIGINT, "k" + k, false);
                columns.add(column);
                boolean complete = random.nextBoolean();
                // An incomplete build head contains 1; its unseen value can be 2 or 3.
                containsTwo[k] = !complete && random.nextBoolean();
                MultiColumnCombinedStats build = new MultiColumnCombinedStats(complete ? 1 : 2,
                        complete ? 5 : 10, List.of(column),
                        List.of(new MultiColumnCombinedStats.McvEntry(List.of("1"), 5)));
                filters.add(new RuntimeFilterJointSelection.Key(column, RuntimeFilterStatistics.from(column,
                        ColumnStatistic.unknown(), List.of(build), complete ? 5 : 10), false));
            }
            List<MultiColumnCombinedStats.McvEntry> head = new ArrayList<>();
            for (int t = 0; t < tuples; t++) {
                counts[t] = 1 + random.nextInt(100);
                rows += counts[t];
                if (trial % 2 == 0 || random.nextInt(4) > 0) {
                    List<String> values = new ArrayList<>();
                    for (int k = 0; k < n; k++) {
                        values.add((t & (1 << k)) == 0 ? "1" : "2");
                    }
                    head.add(new MultiColumnCombinedStats.McvEntry(values, counts[t]));
                }
            }
            MultiColumnCombinedStats probe = new MultiColumnCombinedStats(tuples, rows, columns, head);
            for (double threshold : new double[] {0, 0.2, 0.5, 0.8, 0.99}) {
                BitSet keep = RuntimeFilterJointSelection.select(filters, List.of(probe), threshold);
                Assertions.assertFalse(keep.isEmpty());
                long survivors = 0;
                long[] rejected = new long[n];
                // Evaluate the full population, including tuples absent from the saved head.
                for (int t = 0; t < tuples; t++) {
                    boolean alive = true;
                    for (int k = keep.nextSetBit(0); k >= 0; k = keep.nextSetBit(k + 1)) {
                        if ((t & (1 << k)) != 0 && !containsTwo[k]) {
                            alive = false;
                        }
                    }
                    if (!alive) {
                        continue;
                    }
                    survivors += counts[t];
                    for (int k = 0; k < n; k++) {
                        if ((t & (1 << k)) != 0 && !containsTwo[k]) {
                            rejected[k] += counts[t];
                        }
                    }
                }
                for (int k = 0; k < n; k++) {
                    if (!keep.get(k) && survivors > 0 && rejected[k] > 0) {
                        Assertions.assertTrue(rejected[k] / (double) survivors + 1e-8 < threshold,
                                "trial=" + trial + ", threshold=" + threshold + ", discarded=" + k);
                    }
                }
            }
        }
    }

}
