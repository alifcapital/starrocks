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
