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
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.statistic.JoinStatisticsMeta;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class SkewJoinStatisticsTest {
    private static final ColumnRefOperator STATUS = new ColumnRefOperator(10, VarcharType.VARCHAR, "predicate", false);
    private static final ColumnRefOperator KEY = new ColumnRefOperator(1, IntegerType.BIGINT, "id", false);
    private static final ColumnRefOperator SECOND = new ColumnRefOperator(2, IntegerType.BIGINT, "id2", false);

    private static Statistics mcv(List<ColumnRefOperator> columns, long rows,
                                  MultiColumnCombinedStats.McvEntry... values) {
        var group = new MultiColumnCombinedStats(100, rows, columns, List.of(values), List.of());
        var builder = Statistics.builder().setOutputRowCount(rows)
                .setMultiColumnStatistics(Map.of(Set.copyOf(columns), group));
        columns.forEach(column -> builder.addColumnStatistic(column, ColumnStatistic.unknown()));
        return builder.build();
    }

    private static MultiColumnCombinedStats.McvEntry entry(long rows, String... values) {
        return new MultiColumnCombinedStats.McvEntry(java.util.Arrays.asList(values), rows, List.of());
    }

    @Test
    void textualDictionaryIsPackedAndDistinguishesMissingFromEmpty() {
        String[] values = new String[16384];
        java.util.Arrays.fill(values, "ключ#123");
        values[0] = null;
        values[1] = "";
        var keys = new JoinStatisticsHeadKeys(values);
        Assertions.assertTrue(keys.tuple(0, 1).isEmpty());
        Assertions.assertEquals(List.of(""), keys.tuple(1, 1));
        Assertions.assertEquals(List.of("ключ", "123"), keys.tuple(2, 2));
        Assertions.assertTrue(keys.estimatedSize() < 300_000);
    }

    @Test
    void readsVersionFourWrittenBeforeHeadDictionaries() throws Exception {
        // Encoded by the version-4 writer, not by the implementation under test.
        byte[] bytes = java.util.Base64.getDecoder().decode("U1JKUwAAAAQotS/9BFg0CACiiigwgEuzBnORLH8QIHNiDQ6nSJOn5GCxUkpsv3va24amdc4EP2pF4wEBBsVykAg1ImUK///uLe+5"
                + "h1GZWBDLYT/sVumwGTbgdxWSYjWjHIqT1qxPwcNIyEREw4Ngg3IBTcCILYIbwAfg8mZGcMyQ+yuLIpOaFgH+59AgLUgJzW/RKCH5"
                + "3nuniEFhiiCcf1Ya6Wyet/jqqvVgYtlFqTixKKnA230tIOCCmaGtA2uQ2GWYCW+voyyjSTIOlcRxCUhPnSB5kHwgXbY3ywm/EBfr"
                + "Eac5n8EDmgUoExjDHQx8YAMfmIEPcWMb3EXkuWdTIh6MbGY12KF6zbZglS7ZHDHCLguDL4oFAQAAvulFiQ==");
        var data = JoinStatisticsCodec.decode(bytes, 1 << 20, 1, 2);
        Assertions.assertNull(data.getBases().get(0).getHeadKeys());
        Assertions.assertEquals("Германия", data.getSources().get(1).predicateValue(0, 0).getVarchar());
        long[] head = new long[2];
        data.getBases().get(0).getSlices(0).get(0).getHead().addTo(head);
        Assertions.assertArrayEquals(new long[] {2, 3}, head);
        var roundTrip = JoinStatisticsCodec.decode(JoinStatisticsCodec.encode(data, 1 << 20), 1 << 20, 1, 2);
        Assertions.assertEquals(data.estimatedSize(), roundTrip.estimatedSize());
    }

    @Test
    void partialHeadProjectsOnlyKnownMassAndRetainsJointKey() {
        var stats = mcv(List.of(KEY, SECOND), 1000, entry(300, "1", "2"), entry(250, "1", "3"));
        var scalar = SkewJoinStatistics.find(stats, List.of(KEY), 5);
        Assertions.assertEquals(550, scalar.entries().get(0).rows());
        Assertions.assertEquals(1000, scalar.rows());
        var tuple = SkewJoinStatistics.find(stats, List.of(SECOND, KEY), 5);
        Assertions.assertEquals(List.of("2", "1"), tuple.entries().get(0).values());
        Assertions.assertEquals(300, tuple.entries().get(0).rows());
    }

    @Test
    void replicationGuardUsesFilteredInputRowsForUnrelatedPredicate() {
        var stats = mcv(List.of(KEY, SECOND), 10_000_000, entry(10_000_000, "1", "2"));
        var predicate = BinaryPredicateOperator.eq(STATUS, ConstantOperator.createVarchar("rare"));
        var filtered = McvStatisticsPropagation.filter(predicate, stats,
                Statistics.buildFrom(stats).setOutputRowCount(1000).build());
        var keys = List.of(KEY, SECOND);
        var hot = List.of(List.of(ConstantOperator.createBigint(1), ConstantOperator.createBigint(2)));
        var distribution = SkewJoinStatistics.find(filtered, keys, 5);
        Assertions.assertEquals(1000, distribution.rows());
        Assertions.assertEquals(1000, SkewJoinStatistics.overlappingRows(distribution, keys, hot));
        Assertions.assertTrue(SkewJoinStatistics.overlappingRows(distribution, keys, hot) < 1_000_000);
        // Scaling at consumption must not mutate the saved distribution or compound on the next lookup.
        Assertions.assertEquals(1000, SkewJoinStatistics.find(filtered, keys, 5).entries().get(0).rows());
        Assertions.assertEquals(10_000_000, SkewJoinStatistics.find(stats, keys, 5).entries().get(0).rows());
    }

    @Test
    void growthScalesKnownHeadWithoutAssigningTheTailToHotKeys() {
        var collected = mcv(List.of(KEY, SECOND), 1000, entry(300, "1", "2"), entry(250, "1", "3"));
        var grown = Statistics.buildFrom(collected).setOutputRowCount(10_000_000).build();
        var scalar = SkewJoinStatistics.find(grown, List.of(KEY), 5);
        Assertions.assertEquals(10_000_000, scalar.rows());
        Assertions.assertEquals(5_500_000, scalar.entries().get(0).rows());
        var tuple = SkewJoinStatistics.find(grown, List.of(SECOND, KEY), 5);
        var overlap = SkewJoinStatistics.overlappingRows(tuple, List.of(SECOND, KEY),
                List.of(List.of(ConstantOperator.createBigint(2), ConstantOperator.createBigint(1))));
        Assertions.assertEquals(3_000_000, overlap);
        Assertions.assertTrue(overlap > 1_000_000);
    }

    @Test
    void conditionalIncompleteMcvReachesSkewWithoutHistogram() {
        var stats = mcv(List.of(STATUS, KEY), 1000,
                entry(300, "hot", "1"), entry(200, "cold", "2"));
        var predicate = BinaryPredicateOperator.eq(STATUS, ConstantOperator.createVarchar("hot"));
        var filtered = McvStatisticsPropagation.filter(predicate, stats,
                Statistics.buildFrom(stats).setOutputRowCount(400).build());
        Assertions.assertNull(filtered.getColumnStatistic(KEY).getHistogram());
        var result = SkewJoinStatistics.find(filtered, List.of(KEY), 5);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(List.of("1"), result.entries().get(0).values());
        Assertions.assertEquals(300, result.entries().get(0).rows());
        Assertions.assertEquals(400, result.rows());
    }

    @Test
    void hotComponentDoesNotInventHotTuple() {
        var values = new MultiColumnCombinedStats.McvEntry[100];
        for (int i = 0; i < values.length; i++) {
            values[i] = entry(10, "1", Integer.toString(i));
        }
        var stats = mcv(List.of(KEY, SECOND), 1000, values);
        Assertions.assertEquals(1000, SkewJoinStatistics.find(stats, List.of(KEY), 5).entries().get(0).rows());
        Assertions.assertEquals(50, SkewJoinStatistics.find(stats, List.of(KEY, SECOND), 5)
                .entries().stream().mapToDouble(SkewJoinStatistics.Entry::rows).sum());
    }

    @Test
    void noInheritedBaseFrequenciesAboveJoin() {
        var stats = mcv(List.of(KEY), 1000, entry(900, "1"));
        var joined = McvStatisticsPropagation.afterJoin(stats, stats, true);
        Assertions.assertNull(SkewJoinStatistics.find(joined, List.of(KEY), 5));
    }

    @Test
    void replicationGuardMatchesWholeTuples() {
        var stats = mcv(List.of(KEY, SECOND), 10_000_000,
                entry(4_000_000, "1", "2"), entry(3_000_000, "1", "3"));
        var distribution = SkewJoinStatistics.find(stats, List.of(KEY, SECOND), 5);
        Assertions.assertEquals(4_000_000, SkewJoinStatistics.overlappingRows(distribution,
                List.of(KEY, SECOND), List.of(List.of(ConstantOperator.createBigint(1),
                        ConstantOperator.createBigint(2)))));
        Assertions.assertEquals(0, SkewJoinStatistics.overlappingRows(distribution,
                List.of(KEY, SECOND), List.of(List.of(ConstantOperator.createBigint(1),
                        ConstantOperator.createBigint(4)))));
    }

    @Test
    void uncastableKeysCannotBecomeSplitConstants() {
        Assertions.assertTrue(SkewJoinStatistics.constants(new SkewJoinStatistics.Entry(List.of("dirty"), 10),
                List.of(KEY)).isEmpty());
    }

    private static JoinStatisticsScope scan(int side, ColumnRefOperator key, double rows) {
        return scan(side, key, rows, JoinStatisticsTableState.UNKNOWN);
    }

    private static JoinStatisticsScope scan(int side, ColumnRefOperator key, double rows, JoinStatisticsTableState state) {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getUUID()).thenReturn("uuid" + side);
        return JoinStatisticsScope.scan(table, Map.of(key, new Column("id", key.getType())), rows, state);
    }

    private static JoinStatisticsPlanner planner(boolean labels) throws Exception {
        return planner(labels ? new JoinStatisticsHeadKeys(new long[] {7, 9}) : null);
    }

    private static JoinStatisticsPlanner planner(JoinStatisticsHeadKeys headKeys) throws Exception {
        long[][] counts = { {100, 1}, {1, 1000}};
        List<JoinStatisticsData.Source> sources = new ArrayList<>();
        List<List<JoinStatisticsBasis.Slice>> sides = new ArrayList<>();
        for (int side = 0; side < 2; side++) {
            var degree = JoinStatisticsEstimateTest.degree(counts[side]);
            sources.add(new JoinStatisticsData.Source("uuid" + side, 1, degree.getRowCount(),
                    List.of("predicate"), List.of(VarcharType.VARCHAR), List.of(List.of("value")),
                    new long[] {degree.getRowCount()}, Map.of(0, List.of(degree))));
            sides.add(List.of(new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(counts[side]),
                    new double[0][], false)));
        }
        var data = new JoinStatisticsData(1, 2, sources, List.of(new JoinStatisticsBasis(0, List.of(0, 1), sides,
                List.of(), headKeys)));
        var planner = new JoinStatisticsPlanner();
        var definitions = JoinStatisticsPlanner.class.getDeclaredField("definitions");
        definitions.setAccessible(true);
        definitions.set(planner, List.of(new JoinStatisticsMeta(1, JoinStatisticsEstimateTest.definition(2),
                2, 1, 1, "test", 1)));
        var snapshots = JoinStatisticsPlanner.class.getDeclaredField("snapshots");
        snapshots.setAccessible(true);
        ((Map<Long, Optional<JoinStatisticsData>>) snapshots.get(planner)).put(1L, Optional.of(data));
        return planner;
    }

    @Test
    void omittedHeadCannotDisplaceAvailableSkewKey() throws Exception {
        int old = Config.statistic_join_optimizer_budget_ms;
        Config.statistic_join_optimizer_budget_ms = 5000;
        try {
            // The omitted key has 100 rows; the retained key has one. With limit=1 the latter must survive.
            var result = planner(new JoinStatisticsHeadKeys(new String[] {null, "9"}))
                    .skewStatistics(scan(0, KEY, 101), List.of(KEY), 101, 1);
            Assertions.assertNotNull(result);
            Assertions.assertEquals(101, result.rows());
            Assertions.assertEquals(1, result.entries().size());
            Assertions.assertEquals(List.of("9"), result.entries().get(0).values());
            Assertions.assertEquals(1, result.entries().get(0).rows());
            Assertions.assertEquals(100, result.maximumOmittedRows());
            Assertions.assertTrue(result.leavesHeavierKey(1, 0.1));
        } finally {
            Config.statistic_join_optimizer_budget_ms = old;
        }
    }

    @Test
    void joinHeavyKeysAreReweightedByMatchesAndSemiUsesMembership() throws Exception {
        int old = Config.statistic_join_optimizer_budget_ms;
        Config.statistic_join_optimizer_budget_ms = 5000;
        try {
            var left = scan(0, KEY, 101);
            var right = scan(1, SECOND, 1001);
            var inner = JoinStatisticsScope.join(left, right, JoinOperator.INNER_JOIN,
                    BinaryPredicateOperator.eq(KEY, SECOND));
            var result = planner(true).skewStatistics(inner, List.of(KEY), 1100, 5);
            Assertions.assertNotNull(result);
            Assertions.assertEquals(List.of("9"), result.entries().get(0).values());
            Assertions.assertEquals(1000, result.entries().get(0).rows());
            var semi = JoinStatisticsScope.join(left, right, JoinOperator.LEFT_SEMI_JOIN,
                    BinaryPredicateOperator.eq(KEY, SECOND));
            result = planner(true).skewStatistics(semi, List.of(KEY), 101, 5);
            Assertions.assertEquals(List.of("7"), result.entries().get(0).values());
            Assertions.assertEquals(100, result.entries().get(0).rows());
            Assertions.assertNull(planner(false).skewStatistics(inner, List.of(KEY), 1100, 5));
            left = scan(0, KEY, 202, new JoinStatisticsTableState(202, 2));
            right = scan(1, SECOND, 3003, new JoinStatisticsTableState(3003, 2));
            inner = JoinStatisticsScope.join(left, right, JoinOperator.INNER_JOIN,
                    BinaryPredicateOperator.eq(KEY, SECOND));
            result = planner(true).skewStatistics(inner, List.of(KEY), 6600, 5);
            Assertions.assertEquals(6000, result.entries().get(0).rows());
            Assertions.assertEquals(6600, result.rows());
            semi = JoinStatisticsScope.join(left, right, JoinOperator.LEFT_SEMI_JOIN,
                    BinaryPredicateOperator.eq(KEY, SECOND));
            result = planner(true).skewStatistics(semi, List.of(KEY), 202, 5);
            Assertions.assertEquals(200, result.entries().get(0).rows());
            Assertions.assertEquals(202, result.rows());

        } finally {
            Config.statistic_join_optimizer_budget_ms = old;
        }
    }

    @Test
    void omittedMinorityDoesNotBlockDominantRetainedKey() {
        var distribution = new SkewJoinStatistics.Distribution(100,
                List.of(new SkewJoinStatistics.Entry(List.of("hot"), 70)), "JOIN_STATISTICS", 5);
        Assertions.assertFalse(distribution.leavesHeavierKey(70, 0.1));
        Assertions.assertFalse(distribution.leavesHeavierKey(1, 0.1));
        var heavier = new SkewJoinStatistics.Distribution(100, distribution.entries(), "JOIN_STATISTICS", 80);
        Assertions.assertTrue(heavier.leavesHeavierKey(70, 0.1));
    }

    @Test
    void originalKeysSurviveCodecAndCostOnlyOneDictionary() throws Exception {
        var original = JoinStatisticsCodecTest.fixture();
        var basis = original.getBases().get(0);
        for (var keys : List.of(new JoinStatisticsHeadKeys(new long[] {Long.MIN_VALUE, Long.MAX_VALUE}),
                new JoinStatisticsHeadKeys(new String[] {"a\\#b#42", "кириллица#7"}))) {
            var enriched = new JoinStatisticsBasis(0, basis.getSources(),
                    List.of(basis.getSlices(0), basis.getSlices(1)), basis.getPairs(), keys);
            var data = new JoinStatisticsData(1, 2, original.getSources(), List.of(enriched));
            var restored = JoinStatisticsCodec.decode(JoinStatisticsCodec.encode(data, 1 << 20), 1 << 20, 1, 2);
            Assertions.assertEquals(keys.estimatedSize(), data.estimatedSize() - original.estimatedSize());
            Assertions.assertEquals(keys.tuple(0, keys.isInteger() ? 1 : 2),
                    restored.getBases().get(0).getHeadKeys().tuple(0, keys.isInteger() ? 1 : 2));
        }
        Assertions.assertEquals(List.of("a#b", "42"), new JoinStatisticsHeadKeys(new String[] {"a\\#b#42"}).tuple(0, 2));
    }
}
