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

import com.starrocks.statistic.JoinStatisticsDefinition;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class JoinStatisticsEstimateTest {
    static DegreeStatistics degree(long... counts) {
        long rows = 0;
        long distinct = 0;
        long maximum = 0;
        double[] moments = new double[10];
        for (long count : counts) {
            rows += count;
            distinct += count > 0 ? 1 : 0;
            maximum = Math.max(maximum, count);
            for (int p = 1; p <= 10; p++) {
                moments[p - 1] += Math.pow(count, p);
            }
        }
        return new DegreeStatistics(rows, 0, distinct, maximum, moments);
    }

    static JoinStatisticsDefinition definition(int size) {
        List<JoinStatisticsDefinition.Source> sources = new ArrayList<>();
        Map<Integer, List<String>> keys = new java.util.HashMap<>();
        for (int i = 0; i < size; i++) {
            sources.add(new JoinStatisticsDefinition.Source("iceberg", "db", "t" + i, "uuid" + i, List.of("predicate")));
            keys.put(i, List.of("id"));
        }
        return new JoinStatisticsDefinition("test", sources,
                List.of(new JoinStatisticsDefinition.KeyDomain(keys, List.of("BIGINT"))), Map.of());
    }

    @Test
    void extrapolatedTailRetainsPairInformationForJoinAndProbeMembership() {
        long[][] counts = { {2, 4}, {3, 0}};
        List<JoinStatisticsData.Source> sources = new ArrayList<>();
        List<List<JoinStatisticsBasis.Slice>> sides = new ArrayList<>();
        for (int side = 0; side < 2; side++) {
            var degree = degree(counts[side]);
            sources.add(new JoinStatisticsData.Source("uuid" + side, 1, degree.getRowCount(), List.of("predicate"),
                    List.of(VarcharType.VARCHAR), List.of(List.of("old")), new long[] {degree.getRowCount()},
                    Map.of(0, List.of(degree))));
            int[] orders = JoinStatisticsBasis.momentOrders();
            double[][] moments = new double[orders.length][JoinStatisticsBasis.WIDTH];
            for (int p = 0; p < orders.length; p++) {
                double moment = 0;
                for (long count : counts[side]) {
                    moment += count > 0 ? Math.pow(count, orders[p]) : 0;
                }
                for (int layout = 0; layout < 3; layout++) {
                    moments[p][layout * JoinStatisticsBasis.TAIL_BUCKETS] = moment;
                }
            }
            sides.add(List.of(new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[0]), moments, false)));
        }
        var basis = new JoinStatisticsBasis(0, List.of(0, 1), sides,
                List.of(new JoinStatisticsBasis.Pair(0, 1, 1, 1, new double[] {6, 3, 2, 1})));
        var data = new JoinStatisticsData(1, 1, sources, List.of(basis));
        var selected = List.of(new JoinStatisticsEstimate.Selection(new int[0], 3, 3, new int[] {0}, 0.5),
                new JoinStatisticsEstimate.Selection(new int[] {0}, 0, 3));
        Assertions.assertEquals(3, JoinStatisticsEstimate.estimate(definition(2), data, selected,
                3, 3, 5_000_000_000L).orElseThrow(), 1e-5);
        Assertions.assertEquals(1, JoinStatisticsEstimate.estimate(definition(2), data, selected,
                3, 1, 5_000_000_000L).orElseThrow(), 1e-5);
    }

    @Test
    void extrapolatedHeadWorksForThreeAndFourTablesWithoutRoleEnumeration() {
        for (int n : new int[] {3, 4}) {
            List<JoinStatisticsData.Source> sources = new ArrayList<>();
            List<List<JoinStatisticsBasis.Slice>> sides = new ArrayList<>();
            List<JoinStatisticsEstimate.Selection> selections = new ArrayList<>();
            List<Integer> ids = new ArrayList<>();
            double expected = 0.5;
            for (int i = 0; i < n; i++) {
                long[] counts = i == 0 ? new long[] {8, 4} : new long[] {0, i + 1};
                var degree = degree(counts);
                sources.add(new JoinStatisticsData.Source("uuid" + i, 1, degree.getRowCount(), List.of("predicate"),
                        List.of(VarcharType.VARCHAR), List.of(List.of("old")), new long[] {degree.getRowCount()},
                        Map.of(0, List.of(degree))));
                sides.add(List.of(new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(counts), new double[0][], false)));
                ids.add(i);
                selections.add(i == 0 ? new JoinStatisticsEstimate.Selection(new int[0], 6, 6, new int[] {0}, 0.5)
                        : new JoinStatisticsEstimate.Selection(new int[] {0}, 0, degree.getRowCount()));
                expected *= i == 0 ? 4 : i + 1;
            }
            var data = new JoinStatisticsData(1, 1, sources, List.of(new JoinStatisticsBasis(0, ids, sides)));
            Assertions.assertEquals(expected, JoinStatisticsEstimate.estimate(definition(n), data, selections,
                    (1 << n) - 1, (1 << n) - 1, 5_000_000_000L).orElseThrow(), 1e-5);
            Assertions.assertEquals(2, JoinStatisticsEstimate.estimate(definition(n), data, selections,
                    (1 << n) - 1, 1, 5_000_000_000L).orElseThrow(), 1e-5);
        }
    }

    @Test
    void pairMatrixCanRefineExtrapolationAndRespectsBudget() {
        var zero = new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[0]), new double[0][], true);
        var basis = new JoinStatisticsBasis(0, List.of(0, 1), List.of(List.of(zero, zero), List.of(zero, zero)),
                List.of(new JoinStatisticsBasis.Pair(0, 1, 2, 2,
                        new double[] {100, 10, 10, 1, 200, 20, 20, 2, 300, 30, 30, 3, 400, 40, 40, 4})));
        Assertions.assertEquals(225, basis.weightedPairEstimate(0, 1, new double[] {1, 0.5},
                new double[] {0.5, 0.25}, 0, 1_000_000_000L).orElseThrow(), 1e-8);
        Assertions.assertTrue(basis.weightedPairEstimate(0, 1, new double[] {1, 1},
                new double[] {1, 1}, 0, 0).isEmpty());
    }

    @Test
    void duplicateBuildMultipliesJoinButNotRfMembership() {
        long[][] counts = { {2, 3, 4}, {10, 0, 5}};
        check(counts, 40, 6);
    }

    @Test
    void completeThreeAndFourWayHeadUsesFullSubgraphAndProjection() {
        check(new long[][] { {2, 3, 4}, {10, 0, 5}, {1, 1, 2}}, 60, 6);
        check(new long[][] { {2, 3, 4}, {10, 0, 5}, {1, 1, 2}, {2, 9, 3}}, 160, 6);
    }

    private void check(long[][] counts, double join, double membership) {
        int n = counts.length;
        List<JoinStatisticsData.Source> sources = new ArrayList<>();
        List<JoinStatisticsEstimate.Selection> selections = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            DegreeStatistics degree = degree(counts[i]);
            sources.add(new JoinStatisticsData.Source("uuid" + i, i, degree.getRowCount(), List.of("predicate"),
                    List.of(VarcharType.VARCHAR), List.of(List.of("value")), new long[] {degree.getRowCount()},
                    Map.of(0, List.of(degree))));
            selections.add(new JoinStatisticsEstimate.Selection(new int[] {0}, 0, degree.getRowCount()));
        }
        List<List<JoinStatisticsBasis.Slice>> sides = new ArrayList<>();
        List<Integer> ids = new ArrayList<>();
        for (int side = 0; side < n; side++) {
            ids.add(side);
            sides.add(List.of(new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(counts[side]),
                    new double[0][], false)));
        }
        JoinStatisticsData data = new JoinStatisticsData(1, 2, sources,
                List.of(new JoinStatisticsBasis(0, ids, sides)));
        Assertions.assertEquals(join, JoinStatisticsEstimate.estimate(definition(n), data, selections,
                (1 << n) - 1, (1 << n) - 1, TimeUnit.SECONDS.toNanos(5)).orElseThrow(), join * 1e-6);
        Assertions.assertEquals(membership, JoinStatisticsEstimate.estimate(definition(n), data, selections,
                (1 << n) - 1, 1, TimeUnit.SECONDS.toNanos(5)).orElseThrow(), membership * 1e-6);
    }

    @Test
    void pairFromFourTableObjectDoesNotIntroduceUnrelatedRelations() {
        var pair = JoinStatisticsCodecTest.fixture();
        List<JoinStatisticsData.Source> sources = new ArrayList<>(pair.getSources());
        for (int i = 2; i < 4; i++) {
            sources.add(new JoinStatisticsData.Source("uuid" + i, i, 0, List.of(), List.of(), List.of(),
                    new long[0], Map.of(0, List.of())));
        }
        var larger = new JoinStatisticsData(pair.getObjectId(), pair.getGeneration(), sources, pair.getBases());
        var left = new JoinStatisticsEstimate.Selection(new int[] {0}, 0, 5);
        var right = new JoinStatisticsEstimate.Selection(new int[] {0}, 0, 5);
        var selected = java.util.Arrays.asList(left, right, null, null);
        for (int outputs : new int[] {1, 3}) {
            double expected = JoinStatisticsEstimate.estimate(definition(2), pair, List.of(left, right), 3, outputs,
                    TimeUnit.SECONDS.toNanos(5)).orElseThrow();
            double actual = JoinStatisticsEstimate.estimate(definition(4), larger, selected, 3, outputs,
                    TimeUnit.SECONDS.toNanos(5)).orElseThrow();
            Assertions.assertEquals(expected, actual, 1e-5);
        }
    }

    @Test
    void missingTupleMassIsNotTreatedAsMissingJoinKeyTail() {
        JoinStatisticsData data = JoinStatisticsCodecTest.fixture();
        var selections = List.of(new JoinStatisticsEstimate.Selection(new int[] {0}, 5, 10),
                new JoinStatisticsEstimate.Selection(new int[] {0}, 0, 5));
        double estimate = JoinStatisticsEstimate.estimate(definition(2), data, selections, 3, 3,
                TimeUnit.SECONDS.toNanos(5)).orElseThrow();
        Assertions.assertTrue(estimate >= 28 - 1e-6, "Five missing probe rows may all match build frequency three");
    }

    @Test
    void higherMomentsIncludeCrossTermsBetweenCoveredSlicesAndUncoveredPredicateMass() {
        var probe = new JoinStatisticsData.Source("uuid0", 1, 30, List.of("predicate"), List.of(VarcharType.VARCHAR),
                List.of(List.of("a"), List.of("b")), new long[] {10, 5}, Map.of(0, List.of(degree(10), degree(5))));
        var build = new JoinStatisticsData.Source("uuid1", 2, 3, List.of("predicate"), List.of(VarcharType.VARCHAR),
                List.of(List.of("c")), new long[] {3}, Map.of(0, List.of(degree(3))));
        var sides = List.of(List.of(head(10), head(5)), List.of(head(3)));
        var data = new JoinStatisticsData(1, 2, List.of(probe, build),
                List.of(new JoinStatisticsBasis(0, List.of(0, 1), sides)));
        var selected = List.of(new JoinStatisticsEstimate.Selection(new int[] {0, 1}, 15, 30),
                new JoinStatisticsEstimate.Selection(new int[] {0}, 0, 3));
        Assertions.assertEquals(90, JoinStatisticsEstimate.estimate(definition(2), data, selected, 3, 3,
                TimeUnit.SECONDS.toNanos(5)).orElseThrow(), 1e-4);
        Assertions.assertEquals(30, JoinStatisticsEstimate.estimate(definition(2), data, selected, 3, 1,
                TimeUnit.SECONDS.toNanos(5)).orElseThrow(), 1e-4);
    }

    private static JoinStatisticsBasis.Slice head(long count) {
        return new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[] {count}), new double[0][], false);
    }
}
