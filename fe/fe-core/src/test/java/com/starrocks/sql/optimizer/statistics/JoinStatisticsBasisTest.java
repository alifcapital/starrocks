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

class JoinStatisticsBasisTest {
    @Test
    void sharedMomentsCoverAllSubsetsPowersAndRfProjectionsAfterPartialPredicateUnion() {
        Random random = new Random(907);
        int keys = 513;
        long[][] combined = new long[4][keys];
        List<List<JoinStatisticsBasis.Slice>> sides = new ArrayList<>();
        for (int side = 0; side < 4; side++) {
            long[][] slices = new long[2][keys];
            for (int key = 0; key < keys; key++) {
                for (int slice = 0; slice < 2; slice++) {
                    // Include skew, overlapping slice supports, absent keys and a unit-frequency source.
                    slices[slice][key] = random.nextInt(4) == 0 ? 0 : side == 3 ? 1 : random.nextInt(40);
                    combined[side][key] += slices[slice][key];
                }
            }
            sides.add(List.of(slice(slices[0], 16), slice(slices[1], 16)));
        }
        JoinStatisticsBasis basis = new JoinStatisticsBasis(0, List.of(0, 1, 2, 3), sides);
        long size = basis.estimatedSize();
        for (int subset = 1; subset < 16; subset++) {
            int arity = Integer.bitCount(subset);
            if (arity < 2) {
                continue;
            }
            for (int power = 1; power <= 3; power++) {
                for (int roles = 0; roles < (1 << arity); roles++) {
                    List<List<JoinStatisticsCorrelation.Slice>> projected = new ArrayList<>();
                    int position = 0;
                    double[] products = new double[keys];
                    Arrays.fill(products, 1);
                    for (int side = 0; side < 4; side++) {
                        if ((subset & (1 << side)) == 0) {
                            continue;
                        }
                        boolean presence = (roles & (1 << position++)) != 0;
                        projected.add(List.of(basis.union(side, new int[] {0, 1}).project(arity, power, presence)));
                        for (int key = 0; key < keys; key++) {
                            long value = combined[side][key];
                            products[key] *= presence ? value == 0 ? 0 : 1 : Math.pow(value, power);
                        }
                    }
                    JoinStatisticsCorrelation distribution = new JoinStatisticsCorrelation(projected, roles, power);
                    var selected = projected.stream().map(list -> list.get(0))
                            .toArray(JoinStatisticsCorrelation.Slice[]::new);
                    double actual = Arrays.stream(products).sum();
                    Assertions.assertTrue(distribution.estimateSlices(selected) >= actual * (1 - 1e-12),
                            "Shared tail must cover both frequency cross terms and membership unions");
                }
            }
        }
        Assertions.assertEquals(size, basis.estimatedSize(), "Evaluation must not retain subset/role tails");
    }

    @Test
    void pairHeadKeepsKeysMissingFromOtherTables() {
        List<List<JoinStatisticsBasis.Slice>> sides = new ArrayList<>();
        for (long[] counts : new long[][] { {100, 3}, {5, 8}, {0, 2}, {0, 1}}) {
            sides.add(List.of(slice(counts, 2)));
        }
        JoinStatisticsBasis basis = new JoinStatisticsBasis(0, List.of(0, 1, 2, 3), sides);
        var first = basis.getSlices(0).get(0).project(2, 1, false);
        var second = basis.getSlices(1).get(0).project(2, 1, false);
        var pair = new JoinStatisticsCorrelation(List.of(List.of(first), List.of(second)), 0, 1);
        Assertions.assertEquals(524, pair.estimateSlices(first, second), 1e-9);
    }

    @Test
    void pairwiseProductsPreservePartialFrequencyUnionsAndBoundOverlappingMembership() throws Exception {
        // Left slices: [2,3], [7,0]. Right slices: [5,0], [1,4]. Both have overlapping key support.
        double[] values = {10, 5, 2, 1, 14, 5, 5, 2, 35, 5, 7, 1, 7, 1, 7, 1};
        var pair = new JoinStatisticsBasis.Pair(0, 1, 2, 2, values);
        Arrays.fill(values, 0);
        var basis = new JoinStatisticsBasis(0, List.of(0, 1),
                List.of(List.of(slice(new long[] {2, 3}, 2), slice(new long[] {7, 0}, 2)),
                        List.of(slice(new long[] {5, 0}, 2), slice(new long[] {1, 4}, 2))), List.of(pair));
        Assertions.assertEquals(66, basis.pairEstimate(0, 1, new int[] {0, 1}, new int[] {0, 1}, 0).orElseThrow());
        Assertions.assertTrue(basis.pairEstimate(0, 1, new int[] {0, 1}, new int[] {0, 1}, 2).orElseThrow() >= 12);
        Assertions.assertEquals(14, basis.pairEstimate(0, 1, new int[] {0}, new int[] {1}, 0).orElseThrow());
        Assertions.assertEquals(5, basis.pairEstimate(0, 1, new int[] {0}, new int[] {1}, 2).orElseThrow());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new JoinStatisticsBasis.Pair(0, 1, 1, 1, new double[] {1, 2, 0, 0}));
    }

    @Test
    void preparedTailRejectsMissingLayoutBeforeEnteringCache() {
        var head = CompactDegreeVector.copyOf(new long[0]);
        double[][] roots = new double[1][JoinStatisticsBasis.WIDTH];
        roots[0][0] = 1;
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> JoinStatisticsBasis.Slice.prepared(head, roots, new double[] {1}, true));
        Arrays.fill(roots[0], 0);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> JoinStatisticsBasis.Slice.prepared(head, roots, new double[] {1}, true));
    }

    @Test
    void sparseTailsAndProjectedViewsPreserveDenseCalculations() {
        long[] sparse = new long[600];
        sparse[401] = 37;
        long[] dense = new long[600];
        for (int key = 0; key < dense.length; key++) {
            dense[key] = key % 9 + 1;
        }
        var compact = slice(sparse, 8);
        var populated = slice(dense, 8);
        Assertions.assertTrue(compact.estimatedSize() < 4096, "Three occupied buckets must not retain 48 KiB");
        Assertions.assertTrue(populated.estimatedSize() > 48000, "Populated tails should use direct dense rows");
        var basis = new JoinStatisticsBasis(0, List.of(0, 1),
                List.of(List.of(compact, populated), List.of(slice(new long[8], 8))));
        var combined = basis.union(0, new int[] {0, 1});
        for (int arity = 2; arity <= 4; arity++) {
            for (int power = 1; power <= 3; power++) {
                for (boolean presence : new boolean[] {false, true}) {
                    for (var source : List.of(compact, populated, combined)) {
                        var projected = source.project(arity, power, presence);
                        double norm = projected.getTailNorm();
                        double[] buckets = new double[JoinStatisticsBasis.WIDTH];
                        for (int bucket = 0; bucket < buckets.length; bucket++) {
                            buckets[bucket] = source.projectedRoot(arity, power, presence, bucket);
                            Assertions.assertEquals(buckets[bucket], projected.getBucket(
                                    bucket / JoinStatisticsBasis.TAIL_BUCKETS,
                                    bucket % JoinStatisticsBasis.TAIL_BUCKETS));
                        }
                        var materialized = new JoinStatisticsCorrelation.Slice(source.getHead(), norm, buckets);
                        List<List<JoinStatisticsCorrelation.Slice>> views = new ArrayList<>();
                        List<List<JoinStatisticsCorrelation.Slice>> arrays = new ArrayList<>();
                        for (int side = 0; side < arity; side++) {
                            views.add(List.of(projected));
                            arrays.add(List.of(materialized));
                        }
                        int roles = presence ? (1 << arity) - 1 : 0;
                        var viewDistribution = new JoinStatisticsCorrelation(views, roles, power);
                        var arrayDistribution = new JoinStatisticsCorrelation(arrays, roles, power);
                        Assertions.assertEquals(arrayDistribution.estimate(new int[arity]),
                                viewDistribution.estimate(new int[arity]));
                    }
                }
            }
        }
        // Index and values remain aligned for every moment, not just the populated count row.
        int[] orders = JoinStatisticsBasis.momentOrders();
        for (int order = 0; order < orders.length; order++) {
            for (int layout = 0; layout < JoinStatisticsBasis.TAIL_LAYOUTS; layout++) {
                int index = layout * JoinStatisticsBasis.TAIL_BUCKETS
                        + Math.floorMod(401 * (31 + layout * 10), JoinStatisticsBasis.TAIL_BUCKETS);
                double expected = order == 0 ? 1 : Math.pow(Math.pow(37, orders[order]), 1.0 / orders[order]);
                Assertions.assertEquals(expected, compact.storedRoot(order, index));
                Assertions.assertEquals(0, compact.storedRoot(order, layout * JoinStatisticsBasis.TAIL_BUCKETS));
            }
        }
    }

    @Test
    void intersectionMaximumUsesMatchedKeysAndNeverUnderstatesTailOrSliceUnions() {
        Random random = new Random(92107);
        for (int repeat = 0; repeat < 60; repeat++) {
            int keys = 600;
            long[][] counts = new long[4][keys];
            for (int side = 0; side < 4; side++) {
                for (int key = 0; key < keys; key++) {
                    counts[side][key] = random.nextInt(100) < repeat
                            ? (repeat % 3 == 0 ? 1 : 1L + random.nextInt(100_000)) : 0;
                }
            }
            for (int head : new int[] {0, 8, 600}) {
                var basis = new JoinStatisticsBasis(0, List.of(0, 1),
                        List.of(List.of(slice(counts[0], head), slice(counts[1], head)),
                                List.of(slice(counts[2], head), slice(counts[3], head))));
                for (int[] ids : List.of(new int[] {0}, new int[] {0, 1})) {
                    var frequencies = basis.union(0, ids);
                    var support = basis.union(1, ids);
                    long actual = 0;
                    for (int key = 0; key < keys; key++) {
                        long value = 0;
                        long present = 0;
                        for (int id : ids) {
                            value += counts[id][key];
                            present += counts[2 + id][key];
                        }
                        if (present != 0) {
                            actual = Math.max(actual, value);
                        }
                    }
                    double bound = frequencies.maximumOnSupport(support);
                    Assertions.assertTrue(bound >= actual * (1 - 1e-12));
                    Assertions.assertTrue(bound <= frequencies.maximumFrequencyBound() * (1 + 1e-12));
                    if (head == keys) {
                        Assertions.assertEquals(actual, bound);
                    }
                }
            }
        }
    }

    @Test
    void unmatchedDuplicatesDoNotInflateFanoutOfTheParticipatingKeys() {
        var fact = slice(new long[] {100_000, 50_000, 0, 0}, 4);
        var dimension = slice(new long[] {1, 1, 1_000_000, 12}, 4);
        Assertions.assertEquals(1_000_000, dimension.maximumFrequencyBound());
        Assertions.assertEquals(1, dimension.maximumOnSupport(fact));
        Assertions.assertEquals(0, dimension.maximumOnSupport(slice(new long[4], 4)));
    }

    private static JoinStatisticsBasis.Slice slice(long[] frequencies, int head) {
        int[] orders = JoinStatisticsBasis.momentOrders();
        boolean unit = Arrays.stream(frequencies).allMatch(value -> value <= 1);
        double[][] moments = new double[unit ? 1 : orders.length][JoinStatisticsBasis.WIDTH];
        for (int key = head; key < frequencies.length; key++) {
            if (frequencies[key] == 0) {
                continue;
            }
            for (int layout = 0; layout < JoinStatisticsBasis.TAIL_LAYOUTS; layout++) {
                int bucket = layout * JoinStatisticsBasis.TAIL_BUCKETS
                        + Math.floorMod(key * (31 + layout * 10), JoinStatisticsBasis.TAIL_BUCKETS);
                for (int order = 0; order < moments.length; order++) {
                    moments[order][bucket] += Math.pow(frequencies[key], orders[order]);
                }
            }
        }
        return new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(Arrays.copyOf(frequencies, head)),
                head == frequencies.length ? new double[0][] : moments, unit);
    }
}
