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
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.statistic.JoinStatisticsDefinition;
import com.starrocks.statistic.JoinStatisticsMeta;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class JoinStatisticsCompositionTest {
    private int oldBudget;

    @BeforeEach
    void allowSolverToFinishOnSlowCi() {
        oldBudget = Config.statistic_join_optimizer_budget_ms;
        Config.statistic_join_optimizer_budget_ms = 5000;
    }

    @AfterEach
    void restoreBudget() {
        Config.statistic_join_optimizer_budget_ms = oldBudget;
    }

    private static ColumnRefOperator column(int id, String name) {
        return new ColumnRefOperator(id, IntegerType.BIGINT, name, true);
    }

    private static JoinStatisticsScope scan(long id, double rows, ColumnRefOperator... columns) {
        return scanAt(id, rows, new JoinStatisticsTableState(-1, 7), columns);
    }

    private static JoinStatisticsScope scanAt(long id, double rows, JoinStatisticsTableState state,
                                               ColumnRefOperator... columns) {
        Map<ColumnRefOperator, Column> refs = new HashMap<>();
        for (var column : columns) {
            refs.put(column, new Column(column.getName(), column.getType()));
        }
        return JoinStatisticsScope.scan(new Table(id, "t" + id, Table.TableType.OLAP,
                new ArrayList<>(refs.values())), refs, rows, state);
    }

    private static ScalarOperator eq(ColumnRefOperator a, ColumnRefOperator b) {
        return new BinaryPredicateOperator(BinaryType.EQ, a, b);
    }

    private static JoinStatisticsDefinition.Source source(String uuid) {
        return new JoinStatisticsDefinition.Source("iceberg", "db", "t" + uuid, uuid, List.of());
    }

    private static JoinStatisticsDefinition.KeyDomain key(Map<Integer, List<String>> columns) {
        return new JoinStatisticsDefinition.KeyDomain(columns, List.of("BIGINT"));
    }

    private record Fixture(JoinStatisticsMeta meta, JoinStatisticsData data) { }

    private static Fixture pair(long id, String name, String right, String key, long copies, long snapshot) {
        var definition = new JoinStatisticsDefinition(name, List.of(source("100"), source(right)),
                List.of(key(Map.of(0, List.of(key), 1, List.of("id")))), Map.of());
        List<JoinStatisticsData.Source> sources = new ArrayList<>();
        List<List<JoinStatisticsBasis.Slice>> slices = new ArrayList<>();
        for (int side = 0; side < 2; side++) {
            long[] counts = side == 0 ? new long[] {50, 50} : new long[] {0, copies};
            var degree = JoinStatisticsEstimateTest.degree(counts);
            sources.add(new JoinStatisticsData.Source(side == 0 ? "100" : right, snapshot, degree.getRowCount(),
                    List.of(), List.of(), List.of(List.of()), new long[] {degree.getRowCount()},
                    Map.of(0, List.of(degree))));
            slices.add(List.of(new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(counts),
                    new double[0][], copies == 1 && side != 0)));
        }
        return new Fixture(new JoinStatisticsMeta(id, definition, 1, 1, 1, "test", 1),
                new JoinStatisticsData(id, 1, sources, List.of(new JoinStatisticsBasis(0, List.of(0, 1), slices))));
    }

    private static JoinStatisticsPlanner planner(Fixture... fixtures) throws Exception {
        var planner = new JoinStatisticsPlanner();
        var definitions = JoinStatisticsPlanner.class.getDeclaredField("definitions");
        definitions.setAccessible(true);
        definitions.set(planner, java.util.Arrays.stream(fixtures).map(Fixture::meta).toList());
        var snapshots = JoinStatisticsPlanner.class.getDeclaredField("snapshots");
        snapshots.setAccessible(true);
        @SuppressWarnings("unchecked")
        var loaded = (Map<Long, Optional<JoinStatisticsData>>) snapshots.get(planner);
        for (var fixture : fixtures) {
            loaded.put(fixture.meta.getId(), Optional.of(fixture.data));
        }
        return planner;
    }

    private static JoinStatisticsScope joined(JoinOperator kind) {
        var x = column(1, "x");
        var y = column(2, "y");
        var a = column(3, "id");
        var b = column(4, "id");
        var left = JoinStatisticsScope.join(scan(100, 100, x, y), scan(101, 2, a), kind, eq(x, a));
        return JoinStatisticsScope.join(left, scan(102, 3, b), kind, eq(y, b));
    }

    private static Fixture conditionalPair(long id, String right, String key, long copies) {
        Fixture base = pair(id, "conditional" + id, right, key, copies, 7);
        var definition = new JoinStatisticsDefinition(base.meta.getDefinition().getName(), List.of(
                new JoinStatisticsDefinition.Source("iceberg", "db", "t100", "100", List.of("status")), source(right)),
                base.meta.getDefinition().getDomains(), Map.of());
        long[][] head = { {90, 0}, {0, 10}};
        var left = new JoinStatisticsData.Source("100", 7, 100, List.of("status"), List.of(IntegerType.BIGINT),
                List.of(List.of("1"), List.of("2")), new long[] {90, 10},
                Map.of(0, List.of(JoinStatisticsEstimateTest.degree(head[0]), JoinStatisticsEstimateTest.degree(head[1]))));
        var basis = new JoinStatisticsBasis(0, List.of(0, 1), List.of(
                List.of(new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(head[0]), new double[0][], false),
                        new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(head[1]), new double[0][], false)),
                base.data.getBases().get(0).getSlices(1)));
        return new Fixture(new JoinStatisticsMeta(id, definition, 1, 1, 1, "test", 1),
                new JoinStatisticsData(id, 1, List.of(left, base.data.getSources().get(1)), List.of(basis)));
    }

    @Test
    void missingSliceUsesBasicRowsAndOtherSideDistributionForJoinAndRf() throws Exception {
        var status = column(10, "status");
        var x = column(1, "x");
        var id = column(2, "id");
        var predicate = new BinaryPredicateOperator(BinaryType.EQ, status,
                com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createBigint(3));
        var probe = scan(100, 100, x, status).filter(predicate, 30);
        var build = scan(101, 2, id);
        var fixture = conditionalPair(1, "101", "x", 2);
        // Unknown status gets 30% of the observed distribution: 27 rows on key 0, three on key 1.
        Assertions.assertEquals(6, planner(fixture).estimate(JoinStatisticsScope.join(probe, build,
                JoinOperator.INNER_JOIN, eq(x, id))).orElseThrow(), 1e-4);
        Assertions.assertEquals(3, planner(fixture).membership(build, id, probe, x).orElseThrow(), 1e-4);
        var rfPlanner = planner(fixture);
        var basic = ColumnStatistic.builder().setDistinctValuesCount(100).setNullsFraction(0).build();
        var probeStats = RuntimeFilterStatistics.from(x, basic, List.of(), 30)
                .withJoinStatistics(rfPlanner, probe, x, 30);
        var buildStats = RuntimeFilterStatistics.from(id, basic, List.of(), 2)
                .withJoinStatistics(rfPlanner, build, id, 2);
        Assertions.assertEquals(0.1, buildStats.probePassFraction(probeStats, false).orElseThrow(), 1e-5);
        var grown = scanAt(100, 1000, new JoinStatisticsTableState(1000, 8), x, status).filter(predicate, 300);
        Assertions.assertEquals(60, planner(fixture).estimate(JoinStatisticsScope.join(grown, build,
                JoinOperator.INNER_JOIN, eq(x, id))).orElseThrow(), 1e-4);
    }

    @Test
    void knownAndMissingInSlicesDoNotDoubleCountAndComposeAcrossObjects() throws Exception {
        var status = column(10, "status");
        var x = column(1, "x");
        var y = column(3, "y");
        var id = column(2, "id");
        var thirdId = column(4, "id");
        var predicate = new com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator(false, status,
                com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createBigint(1),
                com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createBigint(3));
        var probe = scan(100, 100, x, y, status).filter(predicate, 95);
        var a = conditionalPair(1, "101", "x", 2);
        var b = conditionalPair(2, "102", "y", 3);
        var ab = JoinStatisticsScope.join(probe, scan(101, 2, id), JoinOperator.INNER_JOIN, eq(x, id));
        // 90 known rows + five sampled from the other ten. Only those five match the build key.
        Assertions.assertEquals(10, planner(a).estimate(ab).orElseThrow(), 1e-4);
        var abc = JoinStatisticsScope.join(ab, scan(102, 3, thirdId), JoinOperator.INNER_JOIN, eq(y, thirdId));
        var estimate = planner(a, b).estimate(abc).orElseThrow();
        Assertions.assertTrue(estimate > 0 && estimate <= 30.0001, "Composed residual should not become zero/cartesian");
        var reordered = planner(b, a, a).estimate(abc).orElseThrow();
        Assertions.assertEquals(estimate, reordered, 1e-4);
    }

    @Test
    void missingBuildSliceUsesProbabilityOfPresenceNotExpectedMultiplicity() throws Exception {
        var status = column(10, "status");
        var x = column(1, "x");
        var id = column(2, "id");
        var predicate = new BinaryPredicateOperator(BinaryType.EQ, status,
                com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createBigint(3));
        var build = scan(100, 100, x, status).filter(predicate, 10);
        var probe = scan(101, 2, id);
        var fixture = conditionalPair(1, "101", "x", 2);
        double expected = 2 * (1 - Math.pow(0.9, 10));
        Assertions.assertEquals(expected, planner(fixture).membership(build, x, probe, id).orElseThrow(), 1e-4);
    }

    @Test
    void overlappingObjectsShareRowIdentityAndPreserveBagAndRfSemantics() throws Exception {
        var a = pair(1, "a", "101", "x", 2, 7);
        var b = pair(2, "b", "102", "y", 3, 7);
        var duplicate = pair(3, "duplicate_a", "101", "x", 2, 7);
        var inner = joined(JoinOperator.INNER_JOIN);
        Assertions.assertTrue(planner(a).estimate(inner).isEmpty(), "One partial object cannot cover the root");
        Assertions.assertEquals(300, planner(a, b).estimate(inner).orElseThrow(), 1e-4);
        Assertions.assertEquals(300, planner(b, a, duplicate).estimate(inner).orElseThrow(), 1e-4);
        Assertions.assertEquals(50, planner(a, b).estimate(joined(JoinOperator.LEFT_SEMI_JOIN)).orElseThrow(), 1e-4);
        Assertions.assertTrue(planner(a, pair(4, "different_snapshot", "102", "y", 3, 8))
                .estimate(inner).isEmpty(), "Different snapshots of the common source cannot be composed");
    }

    @Test
    void redundantDefinitionsAndIncompatibleFirstSnapshotDoNotHideAnAvailableCover() throws Exception {
        var fixtures = new ArrayList<Fixture>();
        for (int i = 1; i <= 16; i++) {
            fixtures.add(pair(i, "duplicate" + i, "101", "x", 2, 7));
        }
        var b = pair(100, "b", "102", "y", 3, 7);
        fixtures.add(b);
        Assertions.assertEquals(300, planner(fixtures.toArray(Fixture[]::new))
                .estimate(joined(JoinOperator.INNER_JOIN)).orElseThrow(), 1e-4);
        Assertions.assertEquals(300, planner(pair(1, "first_snapshot", "101", "x", 2, 8),
                pair(2, "compatible_snapshot", "101", "x", 2, 7), b)
                .estimate(joined(JoinOperator.INNER_JOIN)).orElseThrow(), 1e-4);
    }

    @Test
    void groupedBuildUsesMembershipAndDoesNotRestoreOriginalDuplicates() throws Exception {
        var x = column(1, "x");
        var id = column(2, "id");
        var other = column(3, "other");
        var probe = scan(100, 100, x);
        var build = scan(101, 3, id, other);
        var scope = JoinStatisticsScope.join(probe, build.groupBy(List.of(id)), JoinOperator.INNER_JOIN, eq(x, id));
        Assertions.assertEquals(50, planner(pair(1, "a", "101", "x", 3, 7)).estimate(scope).orElseThrow(), 1e-4);
        Assertions.assertNull(JoinStatisticsScope.join(probe, build.groupBy(List.of(id, other)),
                JoinOperator.INNER_JOIN, eq(x, id)), "Grouping by two columns is not unique on one");
    }

    @Test
    void outerDuplicatesAddUnmatchedRowsWithoutSubtractingSemiUpperBound() throws Exception {
        var x = column(1, "x");
        var id = column(2, "id");
        var probe = scan(100, 100, x);
        var build = scan(101, 3, id);
        // 50 unmatched rows + 50 matched probe rows * 3 build duplicates = 200.
        var planner = planner(pair(1, "a", "101", "x", 3, 7));
        var outer = planner.estimateOuter(probe, build, eq(x, id)).orElseThrow();
        Assertions.assertEquals(200, outer.rows(), 1e-4);
        Assertions.assertEquals(50, outer.matchedRows(), 1e-4,
                "NULL extension counts unmatched probe rows, not missing inner bag rows");
        Assertions.assertEquals(150, planner.estimateOuter(build, probe, eq(x, id)).orElseThrow().rows(), 1e-4);
        var extra = new BinaryPredicateOperator(BinaryType.GT, x,
                com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createBigint(0));
        Assertions.assertTrue(planner.estimateOuter(probe, build,
                com.starrocks.sql.optimizer.Utils.compoundAnd(eq(x, id), extra)).isEmpty());
    }

    @Test
    void outerMembershipUnionDoesNotDoubleSubtractTheSameProbeRows() throws Exception {
        var fixture = pair(1, "outer_union", "101", "x", 2, 7);
        var degree = JoinStatisticsEstimateTest.degree(new long[] {0, 1});
        var source = new JoinStatisticsData.Source("101", 7, 2, List.of("flag"), List.of(IntegerType.BIGINT),
                List.of(List.of("0"), List.of("1")), new long[] {1, 1}, Map.of(0, List.of(degree, degree)));
        var slice = new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[] {0, 1}),
                new double[0][], true);
        var data = new JoinStatisticsData(1, 1, List.of(fixture.data().getSources().get(0), source),
                List.of(new JoinStatisticsBasis(0, List.of(0, 1),
                        List.of(fixture.data().getBases().get(0).getSlices(0), List.of(slice, slice)))));
        var x = column(1, "x");
        var id = column(2, "id");
        // Both build slices match the same 50 probe rows. 100 inner + 50 unmatched = 150.
        Assertions.assertEquals(150, planner(new Fixture(fixture.meta(), data)).estimateOuter(
                scan(100, 100, x), scan(101, 2, id), eq(x, id)).orElseThrow().rows(), 1e-4);
    }

    @Test
    void outerDoesNotMixInnerMinimumAndMembershipFromDifferentSnapshots() throws Exception {
        var fixtures = new ArrayList<Fixture>();
        for (long[] counts : List.of(new long[] {10, 90}, new long[] {140, 10})) {
            int id = fixtures.size() + 1;
            var fixture = pair(id, "outer_version_" + id, "101", "x", 2, id);
            var degree = JoinStatisticsEstimateTest.degree(counts);
            var source = new JoinStatisticsData.Source("100", id, degree.getRowCount(), List.of(), List.of(),
                    List.of(List.of()), new long[] {degree.getRowCount()}, Map.of(0, List.of(degree)));
            var head = new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(counts), new double[0][], false);
            var data = new JoinStatisticsData(id, 1, List.of(source, fixture.data().getSources().get(1)),
                    List.of(new JoinStatisticsBasis(0, List.of(0, 1),
                            List.of(List.of(head), fixture.data().getBases().get(0).getSlices(1)))));
            fixtures.add(new Fixture(fixture.meta(), data));
        }
        var x = column(1, "x");
        var id = column(2, "id");
        // Separate generations give 190 and 160. Mixing inner=20 with matched=90 gives a false 100.
        Assertions.assertEquals(160, planner(fixtures.toArray(Fixture[]::new)).estimateOuter(
                scanAt(100, 150, new JoinStatisticsTableState(150, 2), x),
                scanAt(101, 2, new JoinStatisticsTableState(2, 2), id), eq(x, id)).orElseThrow().rows(), 1e-4);
    }

    @Test
    void nullExtensionDoesNotRestoreRejectedBuildKeyNulls() throws Exception {
        var left = column(1, "left_key");
        var right = column(2, "right_key");
        var preserved = Statistics.builder().setOutputRowCount(9)
                .addColumnStatistic(left, new ColumnStatistic(1, 3, 1.0 / 9, 8, 3)).build();
        var optional = Statistics.builder().setOutputRowCount(7)
                .addColumnStatistic(right, new ColumnStatistic(1, 3, 1.0 / 7, 8, 3)).build();
        var result = Statistics.buildFrom(preserved).setOutputRowCount(19)
                .addColumnStatistic(right, optional.getColumnStatistic(right));
        var method = StatisticsCalculator.class.getDeclaredMethod("computeNullFractionForOuterJoin",
                double.class, double.class, double.class, Statistics.class, Statistics.class,
                List.class, boolean.class, Statistics.Builder.class);
        method.setAccessible(true);
        method.invoke(new StatisticsCalculator(), 9, 8, 19, preserved, optional, List.of(eq(left, right)), false, result);
        Assertions.assertEquals(1.0 / 19, result.build().getColumnStatistic(right).getNullsFraction(), 1e-12);
    }

    @Test
    void repeatedTableRolesKeepFiltersSeparateAndSurviveJoinReordering() throws Exception {
        var definition = new JoinStatisticsDefinition("self", List.of(
                new JoinStatisticsDefinition.Source("iceberg", "db", "users", "100", "100", List.of("flag")),
                new JoinStatisticsDefinition.Source("iceberg", "db", "users", "role:1:100", "100", List.of("flag"))),
                List.of(key(Map.of(0, List.of("id"), 1, List.of("id")))), Map.of());
        var sources = new ArrayList<JoinStatisticsData.Source>();
        var sides = new ArrayList<List<JoinStatisticsBasis.Slice>>();
        for (int side = 0; side < 2; side++) {
            var degrees = List.of(JoinStatisticsEstimateTest.degree(new long[] {2, 0}),
                    JoinStatisticsEstimateTest.degree(new long[] {0, 3}));
            sources.add(new JoinStatisticsData.Source(definition.getSources().get(side).getUuid(), 7, 5,
                    List.of("flag"), List.of(IntegerType.BIGINT), List.of(List.of("0"), List.of("1")),
                    new long[] {2, 3}, Map.of(0, degrees)));
            sides.add(List.of(new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[] {2, 0}),
                            new double[0][], false),
                    new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[] {0, 3}), new double[0][], false)));
        }
        var fixture = new Fixture(new JoinStatisticsMeta(1, definition, 1, 1, 1, "test", 1),
                new JoinStatisticsData(1, 1, sources, List.of(new JoinStatisticsBasis(0, List.of(0, 1), sides))));
        var a = column(1, "id");
        var af = column(2, "flag");
        var b = column(3, "id");
        var bf = column(4, "flag");
        var left = scan(100, 5, a, af);
        var right = scan(100, 5, b, bf);
        Assertions.assertEquals(2, JoinStatisticsBindings.bind(List.of(fixture.meta()), left).size(),
                "A partial subgraph may refer to either stored role of the same physical table");
        for (boolean reversed : List.of(false, true)) {
            var joined = JoinStatisticsScope.join(reversed ? right : left, reversed ? left : right,
                    JoinOperator.INNER_JOIN, eq(a, b));
            Assertions.assertNotNull(joined);
            Assertions.assertEquals(13, planner(fixture).estimate(joined).orElseThrow(), 1e-4);
        }
        var grownLeft = scanAt(100, 50, new JoinStatisticsTableState(50, 8), a, af);
        var grownRight = scanAt(100, 50, new JoinStatisticsTableState(50, 8), b, bf);
        Assertions.assertEquals(1300, planner(fixture).estimate(JoinStatisticsScope.join(
                grownLeft, grownRight, JoinOperator.INNER_JOIN, eq(a, b))).orElseThrow(), 1e-3,
                "Each physical-table role contributes its own multiplicity in a self JOIN");
        left = left.filter(new BinaryPredicateOperator(BinaryType.EQ, af,
                com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createBigint(0)), 2);
        right = right.filter(new BinaryPredicateOperator(BinaryType.EQ, bf,
                com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createBigint(1)), 3);
        Assertions.assertTrue(planner(fixture).estimate(JoinStatisticsScope.join(left, right,
                JoinOperator.INNER_JOIN, eq(a, b))).isEmpty(), "Old disjoint support must not override positive scan estimates");
    }

    @Test
    void commonKeyCompositionUsesTheReducedModelForJoinAndSemiJoin() throws Exception {
        var x = column(1, "x");
        var a = column(2, "id");
        var b = column(3, "id");
        for (var kind : List.of(JoinOperator.INNER_JOIN, JoinOperator.LEFT_SEMI_JOIN)) {
            var left = JoinStatisticsScope.join(scan(100, 100, x), scan(101, 2, a), kind, eq(x, a));
            var scope = JoinStatisticsScope.join(left, scan(102, 3, b), kind, eq(x, b));
            Assertions.assertEquals(kind == JoinOperator.INNER_JOIN ? 300 : 50,
                    planner(pair(1, "a", "101", "x", 2, 7), pair(2, "b", "102", "x", 3, 7))
                            .estimate(scope).orElseThrow(), 1e-4);
        }
    }

    private static Fixture unmatchedDuplicates(long id, String right, String key, long unrelatedCopies) {
        var definition = new JoinStatisticsDefinition("unmatched" + id, List.of(source("100"), source(right)),
                List.of(key(Map.of(0, List.of(key), 1, List.of("id")))), Map.of());
        List<JoinStatisticsData.Source> sources = new ArrayList<>();
        List<List<JoinStatisticsBasis.Slice>> slices = new ArrayList<>();
        for (int side = 0; side < 2; side++) {
            long[] counts = side == 0 ? new long[] {50_000, 50_000, 0} : new long[] {1, 1, unrelatedCopies};
            var degree = JoinStatisticsEstimateTest.degree(counts);
            sources.add(new JoinStatisticsData.Source(side == 0 ? "100" : right, 7, degree.getRowCount(),
                    List.of(), List.of(), List.of(List.of()), new long[] {degree.getRowCount()},
                    Map.of(0, List.of(degree))));
            slices.add(List.of(new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(counts), new double[0][], false)));
        }
        return new Fixture(new JoinStatisticsMeta(id, definition, 1, 1, 1, "test", 1),
                new JoinStatisticsData(id, 1, sources, List.of(new JoinStatisticsBasis(0, List.of(0, 1), slices))));
    }

    @Test
    void compositionDoesNotMultiplyByDuplicatesOutsideEitherKeyIntersection() throws Exception {
        var a = unmatchedDuplicates(1, "101", "x", 1_000_000);
        var b = unmatchedDuplicates(2, "102", "y", 2_000_000);
        var x = column(1, "x");
        var y = column(2, "y");
        var ax = column(3, "id");
        var by = column(4, "id");
        for (var kind : List.of(JoinOperator.INNER_JOIN, JoinOperator.LEFT_SEMI_JOIN)) {
            var ab = JoinStatisticsScope.join(scan(100, 100_000, x, y), scan(101, 1_000_002, ax), kind, eq(x, ax));
            var abc = JoinStatisticsScope.join(ab, scan(102, 2_000_002, by), kind, eq(y, by));
            Assertions.assertEquals(100_000, planner(a, b).estimate(abc).orElseThrow(), 1e-2);
        }
    }

    @Test
    void rowPreservingOuterUsesExactFanoutNotAverageNdvAndDoesNotFilterTheLeft() throws Exception {
        var x = column(1, "x");
        var id = column(2, "id");
        var left = scan(100, 100_000, x);
        var right = scan(101, 1_000_002, id);
        var planner = planner(unmatchedDuplicates(1, "101", "x", 1_000_000));
        Assertions.assertTrue(planner.preservesOuterRows(left, right, eq(x, id)));
        Assertions.assertTrue(planner.preservesOuterRows(left, right, eq(id, x)));
        var on = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, eq(x, id),
                new BinaryPredicateOperator(BinaryType.GT, x,
                        com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createBigint(100)));
        Assertions.assertTrue(planner.preservesOuterRows(left, right, on));
        Assertions.assertFalse(planner.preservesOuterRows(left, right,
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, x, id)));
        Assertions.assertFalse(planner(pair(2, "duplicates", "101", "x", 2, 7))
                .preservesOuterRows(left, right, eq(x, id)));
        Assertions.assertFalse(planner.preservesOuterRows(left, left, eq(x, x)));

        var context = org.mockito.Mockito.mock(com.starrocks.sql.optimizer.ExpressionContext.class);
        org.mockito.Mockito.when(context.getOp()).thenReturn(
                new com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, on));
        org.mockito.Mockito.when(context.getChildStatistics(0)).thenReturn(Statistics.builder()
                .setOutputRowCount(100_000).setJoinStatisticsScope(left).setJoinStatisticsPlanner(planner).build());
        org.mockito.Mockito.when(context.getChildStatistics(1)).thenReturn(Statistics.builder()
                .setOutputRowCount(1_000_002).setJoinStatisticsScope(right).setJoinStatisticsPlanner(planner).build());
        var retained = JoinStatisticsScope.derive(context, false);
        Assertions.assertSame(left, retained, "ON conditions must not become filters on the preserved source");
        Assertions.assertNull(retained.filter(new BinaryPredicateOperator(BinaryType.EQ, id,
                com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createBigint(1)), 1),
                "WHERE on nullable-side columns must not acquire unfiltered left provenance");
    }

    @Test
    void commonKeyQueryCanComposeFiveSourcesWithoutAFiveTableObject() throws Exception {
        var x = column(1, "x");
        var scope = scan(100, 100, x);
        var fixtures = new ArrayList<Fixture>();
        for (int source = 101; source <= 104; source++) {
            var id = column(source, "id");
            scope = JoinStatisticsScope.join(scope, scan(source, 2, id), JoinOperator.INNER_JOIN, eq(x, id));
            fixtures.add(pair(source, "p" + source, String.valueOf(source), "x", 2, 7));
        }
        Assertions.assertEquals(800, planner(fixtures.toArray(Fixture[]::new)).estimate(scope).orElseThrow(), 1e-3);
        Config.statistic_join_optimizer_budget_ms = 0;
        Assertions.assertTrue(planner(fixtures.toArray(Fixture[]::new)).estimate(scope).isEmpty());
    }

    @Test
    void sliceUnionPreservesKnownKeyIdentityInsteadOfAddingUnrelatedMaxima() {
        var a = new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[] {1, 0, 0}),
                new double[0][], true);
        var b = new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[] {0, 1, 1}),
                new double[0][], true);
        var overlap = new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[] {1, 2, 0}),
                new double[0][], false);
        var basis = new JoinStatisticsBasis(0, List.of(0, 1), List.of(List.of(a, b, overlap), List.of(a)));
        Assertions.assertEquals(1, basis.union(0, new int[] {0, 1}).maximumFrequencyBound());
        Assertions.assertEquals(3, basis.union(0, new int[] {1, 2}).maximumFrequencyBound());
        double[][] tail = new double[1][JoinStatisticsBasis.WIDTH];
        java.util.Arrays.fill(tail[0], 2);
        var unitTail = new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[] {0, 0, 0}), tail, true);
        var withTail = new JoinStatisticsBasis(0, List.of(0, 1),
                List.of(List.of(unitTail, unitTail), List.of(a)));
        Assertions.assertEquals(1, unitTail.maximumFrequencyBound());
        Assertions.assertTrue(withTail.union(0, new int[] {0, 1}).maximumFrequencyBound() >= 2,
                "Keys in different tail slices may overlap");
    }

    @Test
    void absentPredicateUsesScanEstimateWithoutClaimingExactness() {
        var column = column(1, "predicate");
        var origin = new JoinStatisticsScope.ColumnOrigin("100", "predicate", column.getType());
        var predicate = new BinaryPredicateOperator(BinaryType.EQ, column,
                com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createBigint(2));
        var source = new JoinStatisticsData.Source("100", 7, 10, List.of("predicate"), List.of(column.getType()),
                List.of(List.of("1")), new long[] {10}, Map.of());
        var selection = JoinStatisticsPlanner.select(source,
                new JoinStatisticsScope.Source("100", 1, List.of(predicate)), Map.of(column, origin));
        Assertions.assertNotNull(selection);
        Assertions.assertEquals(1, selection.rowLimit());
        Assertions.assertTrue(selection.isExtrapolated());
        Assertions.assertNull(selection.keyStatistics(source, 0));
        var incomplete = new JoinStatisticsData.Source("100", 7, 20, List.of("predicate"), List.of(column.getType()),
                List.of(List.of("1")), new long[] {10}, Map.of());
        Assertions.assertNull(JoinStatisticsPlanner.select(incomplete,
                new JoinStatisticsScope.Source("100", 0, List.of(predicate)), Map.of(column, origin)));
    }

    @Test
    void explicitFullSnapshotIsUsableButIncrementalRangeIsNot() {
        Assertions.assertTrue(JoinStatisticsScope.isFullSnapshot(
                com.starrocks.common.tvr.TvrTableSnapshot.of(123L)));
        Assertions.assertTrue(JoinStatisticsScope.isFullSnapshot(com.starrocks.common.tvr.TvrTableDelta.of(
                com.starrocks.common.tvr.TvrVersion.MIN, com.starrocks.common.tvr.TvrVersion.of(123L))));
        Assertions.assertFalse(JoinStatisticsScope.isFullSnapshot(
                com.starrocks.common.tvr.TvrTableDelta.of(100, 123)));
        Assertions.assertFalse(JoinStatisticsScope.isFullSnapshot(com.starrocks.common.tvr.TvrTableDelta.empty()));
    }

    @Test
    void compoundKeysUseOneCoordinateAndNeverMatchAPartialTuple() {
        var a = column(1, "a");
        var b = column(2, "b");
        var c = column(3, "c");
        var x = column(4, "x");
        var y = column(5, "y");
        var z = column(6, "z");
        var left = scan(100, 100, a, b, c);
        var right = scan(101, 100, x, y, z);
        var condition = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                eq(c, z), new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, eq(a, x), eq(b, y)));
        var scope = JoinStatisticsScope.join(left, right, JoinOperator.INNER_JOIN, condition);
        var domains = JoinStatisticsDefinition.tupleDomains(List.of(
                key(Map.of(0, List.of("c"), 1, List.of("z"))),
                key(Map.of(0, List.of("a"), 1, List.of("x"))),
                key(Map.of(0, List.of("b"), 1, List.of("y")))));
        var definition = new JoinStatisticsDefinition("tuple", List.of(source("100"), source("101")), domains, Map.of());
        Assertions.assertEquals(1, domains.size());
        Assertions.assertEquals(List.of("a", "b", "c"), domains.get(0).getColumns().get(0));
        Assertions.assertArrayEquals(new int[] {3}, JoinStatisticsKeyLayout.match(definition, scope));
        Assertions.assertNull(JoinStatisticsKeyLayout.match(definition,
                JoinStatisticsScope.join(left, right, JoinOperator.INNER_JOIN, eq(a, x))));
    }

    @Test
    void thirdDomainRoundTripsWithoutNewSubsetOrRoleTails() throws Exception {
        var a = pair(1, "a", "101", "x", 2, 7).data;
        List<JoinStatisticsData.Source> sources = new ArrayList<>();
        for (var source : a.getSources()) {
            var degree = source.getDegrees().get(0);
            sources.add(new JoinStatisticsData.Source(source.getTableUuid(), 7, source.getRows(), List.of(), List.of(),
                    List.of(List.of()), new long[] {source.getRows()}, Map.of(0, degree, 1, degree, 2, degree)));
        }
        var original = a.getBases().get(0);
        var bases = new ArrayList<JoinStatisticsBasis>();
        for (int d = 0; d < 3; d++) {
            bases.add(new JoinStatisticsBasis(d, List.of(0, 1),
                    List.of(original.getSlices(0), original.getSlices(1))));
        }
        var data = new JoinStatisticsData(1, 1, sources, bases);
        var decoded = JoinStatisticsCodec.decode(JoinStatisticsCodec.encode(data, 1 << 20), 1 << 20, 1, 1);
        Assertions.assertEquals(3, decoded.getBases().size());
        Assertions.assertEquals(2, decoded.getBases().get(2).getDomain());
        Assertions.assertEquals(3, decoded.getSources().get(0).getDegrees().size());
    }
    @Test
    void growthScalesInnerMultiplicitiesButNotSemiBuildDuplicates() throws Exception {
        var x = column(1, "x");
        var id = column(2, "id");
        var planner = planner(pair(1, "growth", "101", "x", 3, 7));
        var probe = scanAt(100, 100_000, new JoinStatisticsTableState(100_000, 8), x);
        var build = scanAt(101, 30, new JoinStatisticsTableState(30, 8), id);
        // Collected: 100 probe rows, half match 3 build rows. Growth: probe x1000, build x10.
        Assertions.assertEquals(1_500_000, planner.estimate(JoinStatisticsScope.join(
                probe, build, JoinOperator.INNER_JOIN, eq(x, id))).orElseThrow(), 0.1);
        Assertions.assertEquals(50_000, planner.membership(build, id, probe, x).orElseThrow(), 1e-3);
        Assertions.assertEquals(30, planner.estimate(JoinStatisticsScope.join(
                probe, build, JoinOperator.RIGHT_SEMI_JOIN, eq(x, id))).orElseThrow(), 1e-3);
        var columnStats = RuntimeFilterStatistics.from(x,
                ColumnStatistic.builder().setDistinctValuesCount(2).build(), List.of(), 100_000);
        var p = columnStats.withJoinStatistics(planner, probe, x, 100_000);
        var b = columnStats.withJoinStatistics(planner, build, id, 30);
        Assertions.assertEquals(0.5, b.probePassFraction(p, false).orElseThrow(), 1e-6);
    }

    @Test
    void compositionScalesSharedSourceOnceAndKeepsSemiMembership() throws Exception {
        var x = column(1, "x");
        var y = column(2, "y");
        var b = column(3, "id");
        var c = column(4, "id");
        var probe = scanAt(100, 1000, new JoinStatisticsTableState(1000, 8), x, y);
        var right = scanAt(101, 4, new JoinStatisticsTableState(4, 8), b);
        var third = scanAt(102, 9, new JoinStatisticsTableState(9, 8), c);
        var planner = planner(pair(1, "ab", "101", "x", 2, 7), pair(2, "ac", "102", "y", 3, 7));
        var ab = JoinStatisticsScope.join(probe, right, JoinOperator.INNER_JOIN, eq(x, b));
        Assertions.assertEquals(18_000, planner.estimate(JoinStatisticsScope.join(
                ab, third, JoinOperator.INNER_JOIN, eq(y, c))).orElseThrow(), 1e-3);
        var semi = JoinStatisticsScope.join(probe, right, JoinOperator.LEFT_SEMI_JOIN, eq(x, b));
        Assertions.assertEquals(500, planner.estimate(JoinStatisticsScope.join(
                semi, third, JoinOperator.LEFT_SEMI_JOIN, eq(y, c))).orElseThrow(), 1e-3);
    }

    @Test
    void staleSupportCannotProveOuterRowPreservationEvenAtSameRowCount() throws Exception {
        var x = column(1, "x");
        var id = column(2, "id");
        var planner = planner(pair(1, "unique", "101", "x", 1, 7));
        var probe = scan(100, 100, x);
        var build = scan(101, 1, id);
        Assertions.assertTrue(planner.preservesOuterRows(probe, build, eq(x, id)));
        var changed = scanAt(101, 1, new JoinStatisticsTableState(1, 8), id);
        Assertions.assertFalse(planner.preservesOuterRows(probe, changed, eq(x, id)));
        Assertions.assertTrue(planner.estimateOuter(probe, changed, eq(x, id)).isEmpty());
        var changedProbe = scanAt(100, 100, new JoinStatisticsTableState(100, 8), x);
        Assertions.assertFalse(planner.preservesOuterRows(changedProbe, build, eq(x, id)),
                "New probe keys may hit build duplicates outside the collected support");
    }

    @Test
    void shrinkingUnknownAndEmptySourceCountsDoNotInventGrowth() throws Exception {
        var x = column(1, "x");
        var id = column(2, "id");
        var planner = planner(pair(1, "a", "101", "x", 3, 7));
        var probe = scanAt(100, 10, new JoinStatisticsTableState(10, 8), x);
        Assertions.assertEquals(15, planner.estimate(JoinStatisticsScope.join(probe, scan(101, 3, id),
                JoinOperator.INNER_JOIN, eq(x, id))).orElseThrow(), 1e-4);
        var unknown = scanAt(100, 9999, JoinStatisticsTableState.UNKNOWN, x);
        Assertions.assertEquals(150, planner.estimate(JoinStatisticsScope.join(unknown, scan(101, 3, id),
                JoinOperator.INNER_JOIN, eq(x, id))).orElseThrow(), 1e-4,
                "Filtered scan estimates are never substituted for full table metadata");
        var empty = scanAt(101, 0, new JoinStatisticsTableState(0, 8), id);
        Assertions.assertEquals(0, planner.membership(empty, id, probe, x).orElseThrow());
        var previouslyEmpty = pair(2, "empty", "101", "x", 0, 7);
        Assertions.assertTrue(planner(previouslyEmpty).estimate(JoinStatisticsScope.join(
                probe, scanAt(101, 10, new JoinStatisticsTableState(10, 8), id),
                JoinOperator.INNER_JOIN, eq(x, id))).isEmpty());
    }

}
