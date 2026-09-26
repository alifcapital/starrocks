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

import java.util.Random;
import java.util.concurrent.TimeUnit;

class JoinStatisticsEntropyModelTest {
    private static final long TEST_BUDGET = TimeUnit.SECONDS.toNanos(10);

    @Test
    void exactPairAndBothSemijoinDirections() {
        JoinStatisticsEntropyModel model = new JoinStatisticsEntropyModel(3);
        model.addFunctionalDependency(2, 3);
        model.addFunctionalDependency(4, 5);
        model.addDegree(1, 3, degree(new long[] {5, 1, 10}));
        model.addDegree(1, 5, degree(new long[] {2, 4, 0}));
        model.addCorrelation(1, new int[] {1, 1}, new int[] {3, 5}, new int[] {1, 1}, 14);
        model.addCorrelation(1, new int[] {1, 1}, new int[] {3, 1}, new int[] {1, 1}, 6);
        model.addCorrelation(1, new int[] {1, 1}, new int[] {1, 5}, new int[] {1, 1}, 6);
        Assertions.assertEquals(14, model.estimate(7, TEST_BUDGET).orElseThrow(), 1e-5);
        Assertions.assertEquals(6, model.estimate(3, TEST_BUDGET).orElseThrow(), 1e-5);
        Assertions.assertEquals(6, model.estimate(5, TEST_BUDGET).orElseThrow(), 1e-5);
    }

    @Test
    void usesJointShannonConstraintsForTriangle() {
        JoinStatisticsEntropyModel model = new JoinStatisticsEntropyModel(3);
        model.addCardinality(3, 100);
        model.addCardinality(6, 100);
        model.addCardinality(5, 100);
        Assertions.assertEquals(1000, model.estimate(7, TEST_BUDGET).orElseThrow(), 1e-3);
    }

    @Test
    void differentKeysUseIntraAndInterTableConstraintsTogether() {
        JoinStatisticsEntropyModel model = new JoinStatisticsEntropyModel(6);
        model.addCorrelation(1, new int[] {1, 1}, new int[] {5, 9}, new int[] {3, 3}, 1000);
        model.addCorrelation(2, new int[] {2, 2}, new int[] {18, 34}, new int[] {3, 3}, 1000);
        model.addCorrelation(3, new int[] {1, 2}, new int[] {3, 3}, new int[] {1, 1}, 1000);
        Assertions.assertTrue(model.estimate(63, TEST_BUDGET).orElseThrow() <= 1000.001);
    }

    @Test
    void momentsAndPairConstraintsBoundRandomStars() {
        Random random = new Random(3703);
        for (int tables = 3; tables <= 4; tables++) {
            for (int repeat = 0; repeat < 20; repeat++) {
                long[][] vectors = new long[tables][8];
                JoinStatisticsEntropyModel model = new JoinStatisticsEntropyModel(tables + 1);
                for (int t = 0; t < tables; t++) {
                    for (int k = 0; k < 8; k++) {
                        vectors[t][k] = random.nextInt(9);
                    }
                    int rowId = 1 << (t + 1);
                    model.addFunctionalDependency(rowId, rowId | 1);
                    model.addDegree(1, rowId | 1, degree(vectors[t]));
                    for (int previous = 0; previous < t; previous++) {
                        double product = 0;
                        for (int k = 0; k < 8; k++) {
                            product += vectors[t][k] * vectors[previous][k];
                        }
                        model.addCorrelation(1, new int[] {1, 1},
                                new int[] {rowId | 1, (1 << (previous + 1)) | 1}, new int[] {1, 1}, product);
                    }
                }
                double actual = 0;
                for (int k = 0; k < 8; k++) {
                    double count = 1;
                    for (long[] vector : vectors) {
                        count *= vector[k];
                    }
                    actual += count;
                }
                double estimate = model.estimate((1 << (tables + 1)) - 1, TEST_BUDGET).orElseThrow();
                Assertions.assertTrue(estimate + 1e-5 >= actual, "Actual=" + actual + ", estimate=" + estimate);
            }
        }
    }

    @Test
    void failureAndBudgetExhaustionAreNotZeroEstimates() {
        JoinStatisticsEntropyModel model = new JoinStatisticsEntropyModel(3);
        Assertions.assertTrue(model.estimate(7, TEST_BUDGET).isEmpty());
        model.addCardinality(7, 100);
        Assertions.assertTrue(model.estimate(7, 0).isEmpty());
        Assertions.assertTrue(model.estimate(7, 1).isEmpty());
        Thread.currentThread().interrupt();
        try {
            Assertions.assertTrue(model.estimate(7, TEST_BUDGET).isEmpty());
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        model.addCardinality(1, 0);
        Assertions.assertEquals(0, model.estimate(7, TEST_BUDGET).orElseThrow());
    }

    @Test
    void commonKeyReductionMatchesFullShannonForJoinAndRfWithMixedPresenceRoles() {
        Random random = new Random(97031);
        for (int tables = 2; tables <= 4; tables++) {
            for (int repeat = 0; repeat < 8; repeat++) {
                long[][] vectors = new long[tables][17];
                var full = new JoinStatisticsEntropyModel(tables + 1);
                var reduced = JoinStatisticsEntropyModel.commonKeyStar(tables + 1);
                for (int side = 0; side < tables; side++) {
                    for (int key = 0; key < 17; key++) {
                        vectors[side][key] = random.nextInt(12);
                    }
                    int row = 1 << (side + 1);
                    for (var model : java.util.List.of(full, reduced)) {
                        model.addFunctionalDependency(row, row | 1);
                        model.addDegree(1, row | 1, degree(vectors[side]));
                    }
                }
                for (int subset = 1; subset < (1 << tables); subset++) {
                    int count = Integer.bitCount(subset);
                    if (count < 2) {
                        continue;
                    }
                    int[] keys = new int[count];
                    int[] relations = new int[count];
                    int[] powers = new int[count];
                    java.util.Arrays.fill(keys, 1);
                    java.util.Arrays.fill(powers, 1 + repeat % 3);
                    int roles = random.nextInt(1 << tables);
                    int position = 0;
                    for (int side = 0; side < tables; side++) {
                        if ((subset & (1 << side)) != 0) {
                            relations[position++] = (roles & (1 << side)) != 0 ? 1 : 1 | (1 << (side + 1));
                        }
                    }
                    double correlation = 0;
                    for (int key = 0; key < 17; key++) {
                        double product = 1;
                        for (int side = 0; side < tables; side++) {
                            if ((subset & (1 << side)) != 0) {
                                product *= (roles & (1 << side)) != 0 ? Math.min(1, vectors[side][key]) : vectors[side][key];
                            }
                        }
                        correlation += Math.pow(product, powers[0]);
                    }
                    full.addCorrelation(1, keys, relations, powers, correlation);
                    reduced.addCorrelation(1, keys, relations, powers, correlation);
                }
                for (int objective : new int[] {3, 5, (1 << (tables + 1)) - 1}) {
                    double expected = full.estimate(objective, TEST_BUDGET).orElseThrow();
                    double actual = reduced.estimate(objective, TEST_BUDGET).orElseThrow();
                    Assertions.assertEquals(expected, actual, Math.max(1, expected) * 1e-6);
                }
            }
        }
    }

    @Test
    void checksStatisticsAndModelDimensions() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new JoinStatisticsEntropyModel(8));
        JoinStatisticsEntropyModel model = new JoinStatisticsEntropyModel(3);
        Assertions.assertThrows(IllegalArgumentException.class, () -> model.addCardinality(8, 10));
        Assertions.assertThrows(IllegalArgumentException.class, () -> model.addCardinality(1, Double.NaN));
        Assertions.assertThrows(IllegalArgumentException.class, () -> model.addCardinality(1, 0.5));
        double[] moments = new double[10];
        Assertions.assertThrows(IllegalArgumentException.class, () -> new DegreeStatistics(10, 0, 1, 10, moments));
    }

    @Test
    void dependencyQuotientMatchesOriginalForDifferentKeysAndRfProjections() {
        Random random = new Random(93107);
        for (int attributes : new int[] {4, 5, 7}) {
            for (int repeat = 0; repeat < 5; repeat++) {
                var model = new JoinStatisticsEntropyModel(attributes);
                int full = (1 << attributes) - 1;
                model.addCardinality(full, 100_000_000);
                // A central row determines several keys. Other rows determine their own key.
                model.addFunctionalDependency(8, 7);
                if (attributes >= 5) {
                    model.addFunctionalDependency(16, 1);
                }
                if (attributes == 7) {
                    model.addFunctionalDependency(32, 2);
                    model.addFunctionalDependency(64, 4);
                }
                for (int i = 0; i < 24; i++) {
                    int mask = 1 + random.nextInt(full);
                    model.addCardinality(mask, 1 + random.nextInt(1_000_000));
                    int key = Integer.lowestOneBit(mask);
                    model.addMaximumFrequency(key, mask, 1 + random.nextInt(100));
                    model.addMoment(key, mask, 1 + random.nextInt(4), 1 + random.nextInt(1_000_000));
                }
                for (int objective : new int[] {1, 8, full}) {
                    double original = model.estimate(objective, TEST_BUDGET, false).orElseThrow();
                    double quotient = model.estimate(objective, TEST_BUDGET).orElseThrow();
                    Assertions.assertEquals(original, quotient, Math.max(1, original) * 1e-6);
                }
            }
        }
    }

    @Test
    void composedDependenciesRetainTransitiveClosureAndAttributeRemapping() {
        var part = new JoinStatisticsEntropyModel(3);
        part.addFunctionalDependency(4, 2);
        part.addFunctionalDependency(2, 1);
        part.addCardinality(4, 17);
        part.addCardinality(1, 3);
        var combined = new JoinStatisticsEntropyModel(5);
        combined.include(part, new int[] {8, 1, 16});
        combined.addFunctionalDependency(2, 8);
        combined.addFunctionalDependency(4, 2);
        combined.addCardinality(4, 13);
        for (int objective = 1; objective < 32; objective++) {
            double original = combined.estimate(objective, TEST_BUDGET, false).orElseThrow();
            double quotient = combined.estimate(objective, TEST_BUDGET).orElseThrow();
            Assertions.assertEquals(original, quotient, Math.max(1, original) * 1e-6);
        }
    }

    private static DegreeStatistics degree(long[] frequencies) {
        long rows = 0;
        long support = 0;
        long maximum = 0;
        double[] moments = new double[10];
        for (long value : frequencies) {
            rows += value;
            support += value > 0 ? 1 : 0;
            maximum = Math.max(maximum, value);
            double power = 1;
            for (int p = 0; p < 10; p++) {
                power *= value;
                moments[p] += power;
            }
        }
        return new DegreeStatistics(rows, 0, support, maximum, moments);
    }
}
