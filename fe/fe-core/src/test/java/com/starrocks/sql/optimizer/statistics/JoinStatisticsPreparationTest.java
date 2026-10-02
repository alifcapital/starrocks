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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinStatisticsPreparationTest {
    private static JoinStatisticsData.Source source() {
        return new JoinStatisticsData.Source("t", 1, 10, List.of("p"), List.of(IntegerType.INT),
                List.of(List.of("1"), List.of("2")), new long[] {2, 3},
                Map.of(0, List.of(JoinStatisticsEstimateTest.degree(2), JoinStatisticsEstimateTest.degree(3))));
    }

    @Test
    void rootsPreserveThePreviousPowArithmetic() {
        Random random = new Random(7931);
        for (int trial = 0; trial < 100; trial++) {
            long[] counts = new long[50];
            for (int i = 0; i < counts.length; i++) {
                counts[i] = random.nextInt(100);
            }
            var degree = JoinStatisticsEstimateTest.degree(counts);
            for (int power = 1; power <= 12; power++) {
                double moment = power <= 10 ? degree.getMoment(power)
                        : degree.getMoment(1) * Math.pow(degree.getMaximumFrequency(), power - 1);
                assertEquals(Math.pow(moment, 1.0 / power), degree.root(power));
            }
        }
    }

    @Test
    void sliceSignaturesOwnIdsAndReuseTheSameUnion() {
        int[] ids = {1, 0};
        var set = JoinStatisticsSliceSet.copyOf(ids);
        ids[0] = 99;
        assertEquals(set, JoinStatisticsSliceSet.ordered(new int[] {0, 1}));
        assertEquals(set.hashCode(), JoinStatisticsSliceSet.ordered(new int[] {0, 1}).hashCode());
        assertThrows(IllegalArgumentException.class, () -> JoinStatisticsSliceSet.copyOf(new int[] {1, 1}));
        var slice = new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[] {2, 1}), new double[0][], false);
        var basis = new JoinStatisticsBasis(0, List.of(0, 1), List.of(List.of(slice, slice), List.of(slice, slice)));
        var evaluation = new JoinStatisticsCorrelation.Evaluation();
        var a = evaluation.select(basis, 0, set);
        assertSame(a, evaluation.select(basis, 0, JoinStatisticsSliceSet.ordered(new int[] {0, 1})));
        long[] frequencies = new long[2];
        a.getHead().addTo(frequencies);
        assertTrue(Arrays.equals(new long[] {4, 2}, frequencies));
        assertTrue(a.estimatedSize() < 1024, "An empty tail must not retain dense moment rows");
    }

    @Test
    void selectionCacheOwnsPredicatesAndSeparatesGenerationGrowthAndBindings() {
        var source = source();
        var column = new ColumnRefOperator(1, IntegerType.INT, "p", false);
        var columns = Map.of(column, new JoinStatisticsScope.ColumnOrigin("t", "p", IntegerType.INT));
        var predicate = new BinaryPredicateOperator(BinaryType.EQ, column, ConstantOperator.createInt(1));
        var scan = new JoinStatisticsScope.Source("t", 2, List.of(predicate));
        var cache = new JoinStatisticsSelectionCache();
        var first = cache.select(source, scan, columns);
        assertEquals(2, first.rowLimit());
        assertSame(first, cache.select(source, scan, columns));
        predicate.setChild(1, ConstantOperator.createInt(2));
        var second = cache.select(source, scan, columns);
        assertEquals(3, second.rowLimit());
        assertNotSame(first, second);
        predicate.setChild(1, ConstantOperator.createInt(1));
        assertSame(first, cache.select(source, scan, columns));
        assertNotSame(first, cache.select(source(), scan, columns));
        var growing = new JoinStatisticsScope.Source("t", 4, List.of(predicate), "t", new JoinStatisticsTableState(20, 2));
        assertNotSame(first, cache.select(source, growing, columns));
        assertNotSame(first, cache.select(source, scan,
                Map.of(column, new JoinStatisticsScope.ColumnOrigin("t", "missing", VarcharType.VARCHAR))));
        cache.clear();
        assertEquals(0, cache.estimatedSize());
        assertNotSame(first, cache.select(source, scan, columns));
    }

    @Test
    void noPredicateSelectionRetainsUncoveredRowsAndDegreeBounds() {
        var source = source();
        var selected = JoinStatisticsPlanner.select(source, new JoinStatisticsScope.Source("t", 1, List.of()), Map.of());
        assertEquals(10, selected.rowLimit());
        var degree = selected.degree(source, 0);
        assertSame(degree, selected.degree(source, 0));
        assertEquals(10, degree.getRowCount());
        assertEquals(7, degree.getDistinctCount());
        assertEquals(10, degree.getMaximumFrequency());
        for (int power = 2; power <= 10; power++) {
            double root = 5;
            for (var slice : source.getDegrees().get(0)) {
                root += Math.pow(slice.getMoment(power), 1.0 / power);
            }
            assertEquals(Math.max(degree.getMoment(power - 1), Math.pow(root, power)), degree.getMoment(power));
        }
    }

    @Test
    void projectedRootsAndExtrapolationsPreserveRolesAndReleaseMemory() {
        double[][] moments = new double[1][JoinStatisticsBasis.WIDTH];
        for (int layout = 0; layout < 3; layout++) {
            moments[0][layout * 256 + 3] = 9;
        }
        var unit = new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[] {1, 0}), moments, true);
        var other = new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[] {0, 3}), new double[0][], false);
        var evaluation = new JoinStatisticsCorrelation.Evaluation();
        for (int arity = 2; arity <= 4; arity++) {
            for (int power = 1; power <= 3; power++) {
                for (boolean presence : new boolean[] {false, true}) {
                    var old = unit.project(arity, power, presence);
                    var prepared = evaluation.project(unit, arity, power, presence);
                    assertSame(prepared, evaluation.project(unit, arity, power, presence));
                    assertEquals(old.getTailNorm(), prepared.getTailNorm());
                    for (int layout = 0; layout < 3; layout++) {
                        for (int bucket = 0; bucket < 256; bucket++) {
                            assertEquals(old.getBucket(layout, bucket), prepared.getBucket(layout, bucket));
                        }
                    }
                }
            }
        }
        var basis = new JoinStatisticsBasis(0, List.of(0, 1), List.of(List.of(unit), List.of(other)));
        var known = JoinStatisticsSliceSet.copyOf(new int[0]);
        var remaining = JoinStatisticsSliceSet.copyOf(new int[] {0});
        var a = evaluation.extrapolate(basis, 0, known, remaining, .5);
        var b = evaluation.extrapolate(basis, 1, known, remaining, .5);
        assertNotSame(a, b);
        assertSame(a, evaluation.extrapolate(basis, 0, known, remaining, .5));
        double[] aw = {1, 1};
        double[] bw = {1, 1};
        a.multiplyInto(aw, false);
        b.multiplyInto(bw, false);
        assertTrue(Arrays.equals(new double[] {.5, 0}, aw));
        assertTrue(Arrays.equals(new double[] {0, 1.5}, bw));
        assertTrue(evaluation.estimatedSize() <= 8L * 1024 * 1024);
        evaluation.clear();
        assertEquals(0, evaluation.estimatedSize());
        assertEquals(0, evaluation.entropyShapeBytes());
    }

    @Test
    void entropyTemplatesDoNotReuseBoundsOrDependOnAnOldObjective() {
        var cache = new JoinStatisticsEntropyModel.ShapeCache();
        Random random = new Random(815);
        for (boolean star : new boolean[] {false, true}) {
            for (int trial = 0; trial < 20; trial++) {
                var prepared = new JoinStatisticsEntropyModel(4, star, cache);
                var original = star ? JoinStatisticsEntropyModel.commonKeyStar(4) : new JoinStatisticsEntropyModel(4);
                for (var model : List.of(prepared, original)) {
                    model.addFunctionalDependency(2, 1);
                    model.addFunctionalDependency(4, 1);
                    model.addFunctionalDependency(8, 1);
                }
                for (int mask = 1; mask < 16; mask++) {
                    double bound = 1 + random.nextInt(10000);
                    prepared.addCardinality(mask, bound);
                    original.addCardinality(mask, bound);
                }
                for (int objective : new int[] {1, 3, 7, 15}) {
                    assertEquals(original.estimate(objective, 5_000_000_000L).orElseThrow(),
                            prepared.estimate(objective, 5_000_000_000L).orElseThrow(), 1e-8);
                }
                assertTrue(prepared.estimate(15, 0).isEmpty());
            }
        }
        assertTrue(cache.estimatedSize() > 0 && cache.estimatedSize() <= 4L * 1024 * 1024);
        cache.clear();
        assertEquals(0, cache.estimatedSize());
    }

    @Test
    void entropyTemplatesSeparateDependencyClosuresForDifferentKeys() {
        var cache = new JoinStatisticsEntropyModel.ShapeCache();
        Random random = new Random(118);
        for (int attributes : new int[] {4, 5, 7}) {
            for (int trial = 0; trial < 3; trial++) {
                var original = new JoinStatisticsEntropyModel(attributes);
                var prepared = new JoinStatisticsEntropyModel(attributes, false, cache);
                int full = (1 << attributes) - 1;
                for (var model : List.of(original, prepared)) {
                    model.addFunctionalDependency(8, trial == 0 ? 7 : 3);
                    model.addCardinality(full, 100000);
                    if (attributes >= 5) {
                        model.addFunctionalDependency(16, 1);
                    }
                }
                for (int i = 0; i < 20; i++) {
                    int mask = 1 + random.nextInt(full);
                    double bound = 1 + random.nextInt(10000);
                    for (var model : List.of(original, prepared)) {
                        model.addCardinality(mask, bound);
                        model.addMaximumFrequency(Integer.lowestOneBit(mask), mask, i % 3 == 0 ? 1 : 10);
                    }
                }
                for (int objective : new int[] {1, 8, full}) {
                    double expected = original.estimate(objective, 5_000_000_000L).orElseThrow();
                    assertEquals(expected, prepared.estimate(objective, 5_000_000_000L).orElseThrow(),
                            Math.max(1, expected) * 1e-9);
                }
            }
        }
    }
}
