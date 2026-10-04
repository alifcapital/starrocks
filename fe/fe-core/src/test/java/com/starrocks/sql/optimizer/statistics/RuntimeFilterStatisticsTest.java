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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RuntimeFilterStatisticsTest {
    private static final ColumnRefOperator KEY = new ColumnRefOperator(1, IntegerType.BIGINT, "key", true);
    private static final ColumnRefOperator OTHER = new ColumnRefOperator(2, IntegerType.BIGINT, "other", true);

    private RuntimeFilterStatistics stats(double ndv, double nulls, Map<String, Long> head, double rows) {
        ColumnStatistic basic = ColumnStatistic.builder().setDistinctValuesCount(ndv).setNullsFraction(nulls).build();
        MultiColumnCombinedStats group = new MultiColumnCombinedStats((long) ndv + (nulls > 0 ? 1 : 0), rows, List.of(KEY),
                head.entrySet().stream().map(entry -> new MultiColumnCombinedStats.McvEntry(
                        List.of(entry.getKey()), entry.getValue())).toList(), List.of(Math.round(rows * nulls)));
        return RuntimeFilterStatistics.from(KEY, basic, List.of(group), rows);
    }

    @Test
    public void testUniformFallbackAndUnknown() {
        Assertions.assertEquals(0.01, stats(10, 0, Map.of(), 10000)
                .probePassFraction(stats(1000, 0, Map.of(), 10000), false).orElseThrow(), 1e-12);
        RuntimeFilterStatistics unknown = RuntimeFilterStatistics.from(KEY, ColumnStatistic.unknown(), List.of(), 10);
        Assertions.assertTrue(unknown.probePassFraction(stats(100, 0, Map.of(), 100), false).isEmpty());
    }

    @Test
    public void testHotProbeKeyAndBuildDuplicates() {
        RuntimeFilterStatistics probe = stats(1000, 0, Map.of("1", 9000L), 10000);
        for (long buildRows : List.of(10L, 1000000L)) {
            RuntimeFilterStatistics build = stats(1, 0, Map.of("1", buildRows), buildRows);
            Assertions.assertEquals(0.9, build.probePassFraction(probe, false).orElseThrow(), 1e-12);
        }
        Assertions.assertEquals(0.1 / 999, stats(1, 0, Map.of("2", 10L), 10)
                .probePassFraction(probe, false).orElseThrow(), 1e-12);
    }

    @Test
    public void testCompleteDisjointHeadsAndNullSafeEquality() {
        RuntimeFilterStatistics probe = stats(2, 0.5, Map.of("1", 30L, "2", 20L), 100);
        RuntimeFilterStatistics build = stats(1, 0.5, Map.of("3", 50L), 100);
        Assertions.assertEquals(0, build.probePassFraction(probe, false).orElseThrow(), 1e-12);
        Assertions.assertEquals(0.5, build.probePassFraction(probe, true).orElseThrow(), 1e-12);
    }

    @Test
    public void testEmptyBuildDoesNotRetainNullMembership() {
        RuntimeFilterStatistics probe = stats(1, 0.5, Map.of("1", 50L), 100);
        RuntimeFilterStatistics nullBuild = stats(0, 1, Map.of(), 100);
        Assertions.assertEquals(0, nullBuild.probePassFraction(probe, false).orElseThrow());
        Assertions.assertEquals(0.5, nullBuild.probePassFraction(probe, true).orElseThrow());
        Assertions.assertEquals(0, nullBuild.boundByRows(0).probePassFraction(probe, true).orElseThrow());
    }

    @Test
    public void testCompositeMarginalCountsAreNotAddedTwice() {
        MultiColumnCombinedStats group = new MultiColumnCombinedStats(10000, 1000, List.of(KEY, OTHER), List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("1", "10"), 100, List.of(900L, 100L)),
                new MultiColumnCombinedStats.McvEntry(List.of("1", "20"), 100, List.of(900L, 100L))));
        RuntimeFilterStatistics probe = RuntimeFilterStatistics.from(KEY,
                ColumnStatistic.builder().setDistinctValuesCount(100).build(), List.of(group), 1000);
        Assertions.assertEquals(100, probe.getNdv());
        Assertions.assertEquals(0.9, stats(1, 0, Map.of("1", 5L), 5)
                .probePassFraction(probe, false).orElseThrow(), 1e-12);
    }

    @Test
    public void testCompleteCompositeHeadProjectsNullsAndKeys() {
        MultiColumnCombinedStats group = new MultiColumnCombinedStats(3, 100, List.of(KEY, OTHER), List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("1", "10"), 30),
                new MultiColumnCombinedStats.McvEntry(List.of("1", "20"), 20),
                new MultiColumnCombinedStats.McvEntry(Arrays.asList(null, "30"), 50)));
        RuntimeFilterStatistics probe = RuntimeFilterStatistics.from(KEY, ColumnStatistic.unknown(), List.of(group), 100);
        Assertions.assertEquals(1, probe.getNdv());
        Assertions.assertEquals(0.5, stats(1, 0, Map.of("1", 5L), 5)
                .probePassFraction(probe, false).orElseThrow(), 1e-12);
    }

    @Test
    public void testIncompleteTupleCountsAreNotMarginalFrequencies() {
        MultiColumnCombinedStats group = new MultiColumnCombinedStats(10000, 1000, List.of(KEY, OTHER), List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("1", "10"), 100)));
        RuntimeFilterStatistics probe = RuntimeFilterStatistics.from(KEY,
                ColumnStatistic.builder().setDistinctValuesCount(100).build(), List.of(group), 1000);
        Assertions.assertEquals(0.01, stats(1, 0, Map.of("1", 5L), 5)
                .probePassFraction(probe, false).orElseThrow(), 1e-12);
    }
    @Test
    public void testSeparateFiltersCannotRejectCrossedCompositeKeys() {
        MultiColumnCombinedStats buildGroup = new MultiColumnCombinedStats(2, 100, List.of(KEY, OTHER), List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("1", "10"), 50),
                new MultiColumnCombinedStats.McvEntry(List.of("2", "20"), 50)));
        MultiColumnCombinedStats probeGroup = new MultiColumnCombinedStats(2, 100, List.of(KEY, OTHER), List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("1", "20"), 50),
                new MultiColumnCombinedStats.McvEntry(List.of("2", "10"), 50)));
        for (ColumnRefOperator column : List.of(KEY, OTHER)) {
            RuntimeFilterStatistics build = RuntimeFilterStatistics.from(column, ColumnStatistic.unknown(),
                    List.of(buildGroup), 100);
            RuntimeFilterStatistics probe = RuntimeFilterStatistics.from(column, ColumnStatistic.unknown(),
                    List.of(probeGroup), 100);
            // Both component memberships pass every row, although no full join tuple matches.
            Assertions.assertEquals(1, build.probePassFraction(probe, false).orElseThrow(), 1e-12);
        }
    }

    // The key statistic of a column with sourceNdv distinct values that filters reduced to ndv.
    private RuntimeFilterStatistics reduced(double sourceNdv, double ndv, Map<String, Long> head, double rows) {
        ColumnStatistic source = ColumnStatistic.builder().setDistinctValuesCount(sourceNdv).setNullsFraction(0).build();
        ColumnStatistic basic = ColumnStatistic.buildFrom(source).setDistinctValuesCount(ndv).build();
        // A single column MCV group sets the key NDV, so it carries the reduced NDV as well.
        List<MultiColumnCombinedStats> groups = head.isEmpty() ? List.of() : List.of(new MultiColumnCombinedStats(
                (long) ndv, rows, List.of(KEY), head.entrySet().stream().map(entry ->
                        new MultiColumnCombinedStats.McvEntry(List.of(entry.getKey()), entry.getValue())).toList(),
                List.of(0L)));
        return RuntimeFilterStatistics.from(KEY, basic, groups, rows);
    }

    @Test
    public void testFilteredDimensionPassesItsShareOfTheSourceKeys() {
        // TPC-DS q22: date_dim filtered by month to 335 of its 72542 dates, inventory refers to 260 dates.
        RuntimeFilterStatistics build = reduced(72542, 72542, Map.of(), 335);
        RuntimeFilterStatistics probe = reduced(260, 260, Map.of(), 399_330_000);
        Assertions.assertEquals(1, build.probePassFraction(probe, false,
                RuntimeFilterStatistics.NdvEstimate.CORRELATED).orElseThrow(), 1e-12);
        Assertions.assertEquals(335.0 / 72542, build.probePassFraction(probe, false,
                RuntimeFilterStatistics.NdvEstimate.INDEPENDENT).orElseThrow(), 1e-12);
        Assertions.assertTrue(build.probePassFraction(probe, false, RuntimeFilterStatistics.NdvEstimate.OFF).isEmpty());
    }

    @Test
    public void testWholeDimensionPassesEveryProbeKey() {
        // Without a filter on the dimension every key of the fact has its row, in both modes.
        RuntimeFilterStatistics build = reduced(72542, 72542, Map.of(), 72542);
        RuntimeFilterStatistics probe = reduced(260, 260, Map.of(), 399_330_000);
        for (RuntimeFilterStatistics.NdvEstimate mode : List.of(RuntimeFilterStatistics.NdvEstimate.CORRELATED,
                RuntimeFilterStatistics.NdvEstimate.INDEPENDENT)) {
            Assertions.assertEquals(1, build.probePassFraction(probe, false, mode).orElseThrow(), 1e-12);
        }
    }

    @Test
    public void testIndependentKeepsAMatchedHotProbeKey() {
        // 90% of the probe rows have key 1, which the build head has. The other keys of the probe are spread
        // over the 10000 source keys, of which the build has 9 more.
        RuntimeFilterStatistics build = reduced(10000, 10, Map.of("1", 100L), 1000);
        RuntimeFilterStatistics probe = reduced(10000, 1000, Map.of("1", 9000L), 10000);
        double independent = build.probePassFraction(probe, false,
                RuntimeFilterStatistics.NdvEstimate.INDEPENDENT).orElseThrow();
        Assertions.assertEquals(0.9 + 0.1 * 9 / 9999, independent, 1e-12);
        Assertions.assertTrue(independent <= build.probePassFraction(probe, false,
                RuntimeFilterStatistics.NdvEstimate.CORRELATED).orElseThrow());
    }

    @Test
    public void testIndependentNeverPassesMoreThanCorrelated() {
        Random random = new Random(7);
        for (int trial = 0; trial < 500; trial++) {
            Map<String, Long> buildHead = new HashMap<>();
            Map<String, Long> probeHead = new HashMap<>();
            for (int key = 0; key < 8; key++) {
                if (random.nextInt(3) == 0) {
                    buildHead.put(Integer.toString(key), 1L + random.nextInt(100));
                }
                if (random.nextInt(3) == 0) {
                    probeHead.put(Integer.toString(key), 1L + random.nextInt(100));
                }
            }
            double source = 10 + random.nextInt(10000);
            double buildNdv = Math.min(source, buildHead.size() + random.nextInt(1000));
            double probeNdv = Math.min(source, probeHead.size() + 1 + random.nextInt(1000));
            double buildRows = 1 + buildHead.values().stream().mapToLong(Long::longValue).sum() + random.nextInt(10000);
            double probeRows = 1 + probeHead.values().stream().mapToLong(Long::longValue).sum() + random.nextInt(10000);
            RuntimeFilterStatistics build = reduced(source, buildNdv, buildHead, buildRows);
            RuntimeFilterStatistics probe = reduced(source * (1 + random.nextInt(2)), probeNdv, probeHead, probeRows);
            double correlated = build.probePassFraction(probe, false,
                    RuntimeFilterStatistics.NdvEstimate.CORRELATED).orElseThrow();
            double independent = build.probePassFraction(probe, false,
                    RuntimeFilterStatistics.NdvEstimate.INDEPENDENT).orElseThrow();
            Assertions.assertTrue(independent >= 0 && independent <= correlated + 1e-12,
                    "trial " + trial + ": " + independent + " > " + correlated);
        }
    }

    @Test
    public void testUnreducedKeysEstimateAsCorrelated() {
        // Statistics that never were reduced have their own NDV as the source NDV, so both modes agree.
        RuntimeFilterStatistics build = stats(10, 0, Map.of(), 10000);
        RuntimeFilterStatistics probe = stats(1000, 0, Map.of(), 10000);
        Assertions.assertEquals(build.probePassFraction(probe, false).orElseThrow(), build.probePassFraction(probe,
                false, RuntimeFilterStatistics.NdvEstimate.INDEPENDENT).orElseThrow(), 1e-12);
    }

    @Test
    public void testHeadOfAFilteredBuildKeepsOnlyValuesThatSurvive() {
        // The MCV group was collected on 1M rows. A filter on another column left 1000 rows and kept the group.
        // Value 1 expects 900 of its rows to remain, value 2 expects 0.01.
        MultiColumnCombinedStats group = new MultiColumnCombinedStats(100, 1_000_000, List.of(KEY), List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("1"), 900_000),
                new MultiColumnCombinedStats.McvEntry(List.of("2"), 10)), List.of(0L));
        ColumnStatistic basic = ColumnStatistic.builder().setDistinctValuesCount(100).setNullsFraction(0).build();
        RuntimeFilterStatistics filtered = RuntimeFilterStatistics.from(KEY, basic, List.of(group), 1000);
        Assertions.assertEquals(1, filtered.knownMembership(IntegerType.BIGINT, "1", false).orElseThrow());
        Assertions.assertTrue(filtered.knownMembership(IntegerType.BIGINT, "2", false).isEmpty());
        Assertions.assertEquals(100, filtered.getNdv());
        // Half of the probe rows have value 2. The filter is not sure to pass them, so it is not certain to pass
        // more than the rows of value 1 that the probe does not have.
        RuntimeFilterStatistics probe = stats(1000, 0, Map.of("2", 5000L), 10000);
        Assertions.assertTrue(filtered.probePassFraction(probe, false).orElseThrow() < 0.5);
        // Without the filter, the operator has all rows of the group and the whole head.
        RuntimeFilterStatistics unfiltered = RuntimeFilterStatistics.from(KEY, basic, List.of(group), 1_000_000);
        Assertions.assertEquals(1, unfiltered.knownMembership(IntegerType.BIGINT, "2", false).orElseThrow());
        Assertions.assertTrue(unfiltered.probePassFraction(probe, false).orElseThrow() >= 0.5);
    }

    @Test
    public void testParseNdvEstimate() {
        Assertions.assertEquals(RuntimeFilterStatistics.NdvEstimate.CORRELATED,
                RuntimeFilterStatistics.NdvEstimate.parse("Correlated"));
        Assertions.assertEquals(RuntimeFilterStatistics.NdvEstimate.OFF, RuntimeFilterStatistics.NdvEstimate.parse(" off"));
        Assertions.assertEquals(RuntimeFilterStatistics.NdvEstimate.INDEPENDENT,
                RuntimeFilterStatistics.NdvEstimate.parse("independent"));
        // A mistyped value keeps the default and does not fail the query.
        Assertions.assertEquals(RuntimeFilterStatistics.NdvEstimate.INDEPENDENT,
                RuntimeFilterStatistics.NdvEstimate.parse("indpendent"));
        Assertions.assertEquals(RuntimeFilterStatistics.NdvEstimate.INDEPENDENT,
                RuntimeFilterStatistics.NdvEstimate.parse(null));
    }

    @Test
    public void testCompleteMcvAgainstExactMembership() {
        Random random = new Random(391);
        for (int trial = 0; trial < 100; trial++) {
            Map<String, Long> buildHead = new HashMap<>();
            Map<String, Long> probeHead = new HashMap<>();
            for (int key = 0; key < 30; key++) {
                if (random.nextBoolean()) {
                    buildHead.put(Integer.toString(key), 1L + random.nextInt(1000));
                }
                probeHead.put(Integer.toString(key), 1L + random.nextInt(1000));
            }
            long buildNulls = trial % 2 == 0 ? 100 : 0;
            long probeNulls = 200;
            double buildRows = buildNulls + buildHead.values().stream().mapToLong(Long::longValue).sum();
            double probeRows = probeNulls + probeHead.values().stream().mapToLong(Long::longValue).sum();
            RuntimeFilterStatistics build = stats(buildHead.size(), buildNulls / buildRows, buildHead, buildRows);
            RuntimeFilterStatistics probe = stats(probeHead.size(), probeNulls / probeRows, probeHead, probeRows);
            double matching = probeHead.entrySet().stream().filter(entry -> buildHead.containsKey(entry.getKey()))
                    .mapToLong(Map.Entry::getValue).sum();
            Assertions.assertEquals(matching / probeRows,
                    build.probePassFraction(probe, false).orElseThrow(), 1e-12);
            Assertions.assertEquals((matching + (buildNulls > 0 ? probeNulls : 0)) / probeRows,
                    build.probePassFraction(probe, true).orElseThrow(), 1e-12);
        }
    }

}
