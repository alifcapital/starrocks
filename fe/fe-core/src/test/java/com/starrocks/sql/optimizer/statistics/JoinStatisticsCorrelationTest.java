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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

class JoinStatisticsCorrelationTest {
    @Test
    void headAndBucketBoundsCoverJoinAndBothRfDirections() {
        Random random = new Random(3703);
        var evaluation = new JoinStatisticsCorrelation.Evaluation();
        for (int sides = 2; sides <= 4; sides++) {
            long[][] values = new long[sides][2100];
            for (long[] vector : values) {
                for (int key = 0; key < vector.length; key++) {
                    vector[key] = random.nextInt(5) == 0 ? 0 : random.nextInt(1000);
                }
            }
            for (int power = 1; power <= 3; power++) {
                for (int presence = 0; presence < (1 << sides); presence++) {
                    List<List<JoinStatisticsCorrelation.Slice>> rows = new ArrayList<>();
                    for (int side = 0; side < sides; side++) {
                        rows.add(List.of(slice(values[side], 65, sides, power, (presence & (1 << side)) != 0)));
                    }
                    JoinStatisticsCorrelation correlation = new JoinStatisticsCorrelation(rows, presence, power);
                    double actual = exact(values, presence, power);
                    double estimate = correlation.estimate(new int[sides]);
                    var selected = rows.stream().map(row -> row.get(0)).toArray(JoinStatisticsCorrelation.Slice[]::new);
                    Assertions.assertEquals(estimate, evaluation.estimateShared(correlation, selected));
                    Assertions.assertTrue(estimate >= actual * (1 - 1e-12));
                    double unbucketedTail = 1;
                    for (List<JoinStatisticsCorrelation.Slice> side : rows) {
                        unbucketedTail *= side.get(0).getTailNorm();
                    }
                    long[][] heads = Arrays.stream(values).map(vector -> Arrays.copyOf(vector, 65)).toArray(long[][]::new);
                    double unbucketed = exact(heads, presence, power) + unbucketedTail;
                    Assertions.assertTrue(estimate <= unbucketed * (1 + 1e-10));
                }
            }
        }
        Assertions.assertTrue(evaluation.estimatedSize() <= 16L * 1024 * 1024);
        evaluation.clear();
        Assertions.assertEquals(0, evaluation.estimatedSize());
    }

    @Test
    void unitFrequencyRolesReuseHeadAndTailEvenWhenPartialTuplesAreMerged() {
        long[] first = {1, 0, 1, 1, 0};
        long[] second = {0, 1, 1, 0, 1};
        long[] right = {3, 2, 5, 7, 1};
        var frequency = new JoinStatisticsCorrelation(List.of(
                List.of(slice(first, 2, 2, 1, false), slice(second, 2, 2, 1, false)),
                List.of(slice(right, 2, 2, 1, false))), 0, 1);
        var presence = frequency.withUnitPresenceRoles(1);
        Assertions.assertEquals(frequency.estimate(0, 0), presence.estimate(0, 0));
        double estimate = presence.estimateSlices(presence.unionSlices(0, 0, 1), presence.getSlice(1, 0));
        Assertions.assertTrue(estimate >= 18, "Union presence does not count key 2 twice");
        Assertions.assertTrue(estimate <= frequency.estimateSlices(frequency.unionSlices(0, 0, 1),
                frequency.getSlice(1, 0)));
    }

    @Test
    void higherPowersReuseUnitSlicesWithoutLosingPartialPredicateCrossTerms() {
        long[] a = {1, 0, 1, 1, 0};
        long[] b = {0, 1, 1, 0, 1};
        long[] c = {1, 1, 1, 0, 1};
        var base = new JoinStatisticsCorrelation(List.of(
                List.of(slice(a, 2, 2, 1, false), slice(b, 2, 2, 1, false)),
                List.of(slice(c, 2, 2, 1, false))), 0, 1);
        for (int power = 1; power <= 3; power++) {
            for (int roles = 0; roles < 4; roles++) {
                var shared = base.withUnitPresenceRoles(roles, power);
                var separate = new JoinStatisticsCorrelation(List.of(
                        List.of(slice(a, 2, 2, power, (roles & 1) != 0), slice(b, 2, 2, power, (roles & 1) != 0)),
                        List.of(slice(c, 2, 2, power, (roles & 2) != 0))), roles, power);
                Assertions.assertEquals(separate.estimateSlices(separate.unionSlices(0, 0, 1), separate.getSlice(1, 0)),
                        shared.estimateSlices(shared.unionSlices(0, 0, 1), shared.getSlice(1, 0)));
            }
        }
    }

    @Test
    void emptyAndDisjointHeadsRemainZero() {
        JoinStatisticsCorrelation.Slice left = slice(new long[] {10, 0, 0}, 3, 2, 1, false);
        JoinStatisticsCorrelation.Slice right = slice(new long[] {0, 20, 0}, 3, 2, 1, false);
        JoinStatisticsCorrelation correlation = new JoinStatisticsCorrelation(List.of(List.of(left), List.of(right)), 0, 1);
        Assertions.assertEquals(0, correlation.estimate(0, 0));
        JoinStatisticsCorrelation.Slice empty = slice(new long[3], 3, 3, 1, false);
        correlation = new JoinStatisticsCorrelation(List.of(List.of(left), List.of(right), List.of(empty)), 0, 1);
        Assertions.assertEquals(0, correlation.estimate(0, 0, 0));
    }

    @Test
    void queryMemoBoundsRetainedHeadMemory() {
        var evaluation = new JoinStatisticsCorrelation.Evaluation();
        Random random = new Random(317);
        for (int i = 0; i < 160; i++) {
            long[] counts = new long[JoinStatisticsCorrelation.HEAD_BUDGET];
            for (int key = 0; key < counts.length; key++) {
                counts[key] = random.nextInt(100000);
            }
            var row = new JoinStatisticsCorrelation.Slice(CompactDegreeVector.copyOf(counts), 0, new double[0]);
            var correlation = new JoinStatisticsCorrelation(List.of(List.of(row), List.of(row)), 0, 1);
            double expected = correlation.estimate(0, 0);
            Assertions.assertEquals(expected,
                    evaluation.estimateShared(correlation, new JoinStatisticsCorrelation.Slice[] {row, row}));
            long size = evaluation.estimatedSize();
            Assertions.assertEquals(expected,
                    evaluation.estimateShared(correlation, new JoinStatisticsCorrelation.Slice[] {row, row}));
            Assertions.assertEquals(size, evaluation.estimatedSize());
        }
        Assertions.assertTrue(evaluation.estimatedSize() <= 8L * 1024 * 1024,
                "Memo admission must account for every head it can retain");
    }

    @Test
    void partialPredicateUnionsFrequenciesBeforeRaisingToPower() {
        long[] first = {10, 20, 1, 0, 4, 30};
        long[] second = {1, 4, 5, 10, 0, 70};
        long[] right = {8, 1, 10, 1, 3, 10};
        long[] combined = new long[first.length];
        for (int i = 0; i < combined.length; i++) {
            combined[i] = first[i] + second[i];
        }
        for (int power = 1; power <= 3; power++) {
            for (int roles = 0; roles < 4; roles++) {
                boolean leftPresence = (roles & 1) != 0;
                boolean rightPresence = (roles & 2) != 0;
                JoinStatisticsCorrelation correlation = new JoinStatisticsCorrelation(List.of(
                        List.of(slice(first, 2, 2, power, leftPresence), slice(second, 2, 2, power, leftPresence)),
                        List.of(slice(right, 2, 2, power, rightPresence))), roles, power);
                double estimate = correlation.estimateSlices(correlation.unionSlices(0, 0, 1), correlation.getSlice(1, 0));
                double actual = exact(new long[][] {combined, right}, roles, power);
                Assertions.assertTrue(estimate >= actual * (1 - 1e-12), "Union must include moment cross terms");
                Assertions.assertThrows(IllegalArgumentException.class, () -> correlation.unionSlices(0, 0, 0));
            }
        }
    }

    @Test
    void tailAndHeadAreImmutableAndMalformedTailsAreRejected() {
        double[] buckets = new double[JoinStatisticsCorrelation.TAIL_LAYOUTS * JoinStatisticsCorrelation.TAIL_BUCKETS];
        buckets[0] = 1;
        buckets[JoinStatisticsCorrelation.TAIL_BUCKETS] = 1;
        buckets[2 * JoinStatisticsCorrelation.TAIL_BUCKETS] = 1;
        JoinStatisticsCorrelation.Slice slice = new JoinStatisticsCorrelation.Slice(
                CompactDegreeVector.copyOf(new long[] {1}), 1, buckets);
        Arrays.fill(buckets, 0);
        Assertions.assertEquals(1, slice.getBucket(0, 0));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new JoinStatisticsCorrelation.Slice(slice.getHead(), 1,
                        new double[JoinStatisticsCorrelation.TAIL_LAYOUTS * JoinStatisticsCorrelation.TAIL_BUCKETS]));
        buckets[0] = Double.NaN;
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new JoinStatisticsCorrelation.Slice(slice.getHead(), 1, buckets));
    }

    private static double exact(long[][] vectors, int presence, int power) {
        double result = 0;
        for (int key = 0; key < vectors[0].length; key++) {
            double product = 1;
            for (int side = 0; side < vectors.length; side++) {
                long value = vectors[side][key];
                product *= (presence & (1 << side)) == 0 ? value : value == 0 ? 0 : 1;
            }
            result += Math.pow(product, power);
        }
        return result;
    }

    private static JoinStatisticsCorrelation.Slice slice(long[] values, int headSize, int sides, int power,
                                                         boolean presence) {
        double norm = 0;
        double[] buckets = new double[JoinStatisticsCorrelation.TAIL_LAYOUTS * JoinStatisticsCorrelation.TAIL_BUCKETS];
        for (int i = headSize; i < values.length; i++) {
            double value = presence ? values[i] == 0 ? 0 : 1 : values[i];
            double powered = Math.pow(value, power * sides);
            norm += powered;
            for (int layout = 0; layout < 3; layout++) {
                int bucket = Math.floorMod((i * (31 + layout * 10)) ^ (i >>> (3 + layout)),
                        JoinStatisticsCorrelation.TAIL_BUCKETS);
                buckets[layout * JoinStatisticsCorrelation.TAIL_BUCKETS + bucket] += powered;
            }
        }
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = Math.pow(buckets[i], 1.0 / sides);
        }
        return new JoinStatisticsCorrelation.Slice(CompactDegreeVector.copyOf(Arrays.copyOf(values, headSize)),
                Math.pow(norm, 1.0 / sides), buckets);
    }
}
