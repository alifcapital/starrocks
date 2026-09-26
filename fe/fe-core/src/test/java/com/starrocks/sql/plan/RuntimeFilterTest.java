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

package com.starrocks.sql.plan;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.MultiColumnCombinedStats;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalcUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class RuntimeFilterTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);
        // Dedicated fact/dim tables so the gate tests can set row counts without leaking onto t0/t1
        // used by other tests in this class.
        starRocksAssert.withTable("CREATE TABLE `rf_fact` (`f_cust` bigint NULL, `f_region` bigint NULL,"
                + " `f_amt` bigint NULL) DUPLICATE KEY(`f_cust`) DISTRIBUTED BY HASH(`f_cust`) BUCKETS 3"
                + " PROPERTIES (\"replication_num\" = \"1\");");
        starRocksAssert.withTable("CREATE TABLE `rf_dim` (`d_cust` bigint NULL, `d_region` bigint NULL,"
                + " `d_name` bigint NULL) DUPLICATE KEY(`d_cust`) DISTRIBUTED BY HASH(`d_cust`) BUCKETS 3"
                + " PROPERTIES (\"replication_num\" = \"1\");");
        starRocksAssert.withTable("CREATE TABLE `rf_agg_dim` (`d_cust` bigint NULL, `d_region` bigint NULL,"
                + " `d_name` bigint NULL) DUPLICATE KEY(`d_cust`) DISTRIBUTED BY HASH(`d_name`) BUCKETS 3"
                + " PROPERTIES (\"replication_num\" = \"1\");");
    }

    private static ColumnStatistic colStat(double ndv) {
        return new ColumnStatistic(0, ndv, 0, 8, ndv);
    }

    // The build-side bloom filter is sized by the build key's NDV, not by the build row count. A shuffle
    // join whose build side has many rows but a low-cardinality join key (e.g. a 100-value region column
    // on a 5M-row dimension) must still get a runtime filter, even though the row count exceeds the limit.
    @Test
    public void testBuildGateUsesBuildKeyNdvNotRows() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        long savedMax = sv.getGlobalRuntimeFilterBuildMaxSize();
        try {
            sv.setGlobalRuntimeFilterBuildMaxSize(1_000_000);
            setTableStatistics((OlapTable) getTable("rf_fact"), 100_000_000);
            setTableStatistics((OlapTable) getTable("rf_dim"), 5_000_000);
            // build key d_region is low-cardinality (100 regions); dim has 5M rows.
            new MockUp<MockTpchStatisticStorage>() {
                @Mock
                public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                    return columns.stream().map(c -> c.equals("d_region") ? colStat(100) : colStat(5_000_000))
                            .collect(Collectors.toList());
                }
            };

            String plan = getVerboseExplain("select * from rf_fact join [shuffle] rf_dim on f_region = d_region");
            // NDV(100) is below the 1M limit, so the filter is built; the 5M row count alone would drop it.
            assertContains(plan, "build runtime filters");
        } finally {
            sv.setGlobalRuntimeFilterBuildMaxSize(savedMax);
        }
    }

    // Per-conjunct gating: a two-key shuffle join where one key is narrow (100-value region) and the other
    // is wide (5M distinct customers). The narrow key keeps its runtime filter; the wide key's filter is
    // dropped on its own NDV, not together with the narrow one (the old per-join gate dropped both).
    @Test
    public void testBuildGatePerConjunctKeepsNarrowKey() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        long savedMax = sv.getGlobalRuntimeFilterBuildMaxSize();
        try {
            sv.setGlobalRuntimeFilterBuildMaxSize(1_000_000);
            setTableStatistics((OlapTable) getTable("rf_fact"), 100_000_000);
            setTableStatistics((OlapTable) getTable("rf_dim"), 5_000_000);
            new MockUp<MockTpchStatisticStorage>() {
                @Mock
                public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                    return columns.stream().map(c -> {
                        if (c.equals("d_region")) {
                            return colStat(100);         // narrow key -> RF kept
                        } else if (c.equals("d_cust")) {
                            return colStat(5_000_000);   // wide key -> RF dropped
                        }
                        return colStat(1000);
                    }).collect(Collectors.toList());
                }
            };

            String plan = getVerboseExplain(
                    "select * from rf_fact join [shuffle] rf_dim on f_region = d_region and f_cust = d_cust");
            // Narrow key d_region keeps a filter; wide key d_cust does not.
            assertContains(plan, "build_expr = (5: d_region)");
            assertNotContains(plan, "build_expr = (4: d_cust)");
        } finally {
            sv.setGlobalRuntimeFilterBuildMaxSize(savedMax);
        }
    }

    // NDV unknown -> the gate falls back to the build row count (the original behavior). The 5M-row build
    // side exceeds the 1M limit, so the filter is dropped just as before - no regression where stats are
    // absent.
    @Test
    public void testBuildGateFallsBackToRowsWhenNdvUnknown() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        long savedMax = sv.getGlobalRuntimeFilterBuildMaxSize();
        try {
            sv.setGlobalRuntimeFilterBuildMaxSize(1_000_000);
            setTableStatistics((OlapTable) getTable("rf_fact"), 100_000_000);
            setTableStatistics((OlapTable) getTable("rf_dim"), 5_000_000);
            new MockUp<MockTpchStatisticStorage>() {
                @Mock
                public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                    return columns.stream().map(c -> ColumnStatistic.unknown()).collect(Collectors.toList());
                }
            };

            String plan = getVerboseExplain("select * from rf_fact join [shuffle] rf_dim on f_region = d_region");
            assertNotContains(plan, "build runtime filters");
        } finally {
            sv.setGlobalRuntimeFilterBuildMaxSize(savedMax);
        }
    }

    // Probe-side gate on a remote RF crossing the shuffle exchange. The build side (dim, 90M) is
    // comparable in rows to the probe (fact, 100M), so the build/probe row-count ratio (~0.9) would reject
    // the filter; but the NDV semijoin selectivity NDV(d_cust)=500K / NDV(f_cust)=100M = 0.005 is
    // selective, so it is accepted.
    @Test
    public void testProbeGateUsesNdvSelectivityNotRowsRatio() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        long savedProbeMin = sv.getGlobalRuntimeFilterProbeMinSize();
        boolean savedGrf = sv.getEnableGlobalRuntimeFilter();
        try {
            sv.setGlobalRuntimeFilterProbeMinSize(1); // > 0 so the selectivity formula is actually reached
            sv.setEnableGlobalRuntimeFilter(true);    // remote RF crosses the exchange -> formula runs
            setTableStatistics((OlapTable) getTable("rf_fact"), 100_000_000);
            setTableStatistics((OlapTable) getTable("rf_dim"), 90_000_000); // < fact -> dim is the build side
            // build key d_cust has few distinct values; probe key f_cust has many -> selectivity is tiny.
            new MockUp<MockTpchStatisticStorage>() {
                @Mock
                public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                    return columns.stream().map(c -> c.equals("d_cust") ? colStat(500_000) : colStat(100_000_000))
                            .collect(Collectors.toList());
                }
            };

            String plan = getVerboseExplain("select * from rf_fact join [shuffle] rf_dim on f_cust = d_cust");
            assertContains(plan, "probe runtime filters");
        } finally {
            sv.setGlobalRuntimeFilterProbeMinSize(savedProbeMin);
            sv.setEnableGlobalRuntimeFilter(savedGrf);
        }
    }

    // NDV unknown -> the probe gate uses the original build/probe row-count ratio (dim 90M / fact 100M =
    // 0.9 is above the 0.5 threshold -> reject), i.e. no regression where statistics are absent.
    @Test
    public void testProbeGateFallsBackToRowsRatioWhenNdvUnknown() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        long savedProbeMin = sv.getGlobalRuntimeFilterProbeMinSize();
        boolean savedGrf = sv.getEnableGlobalRuntimeFilter();
        try {
            sv.setGlobalRuntimeFilterProbeMinSize(1);
            sv.setEnableGlobalRuntimeFilter(true);
            setTableStatistics((OlapTable) getTable("rf_fact"), 100_000_000);
            setTableStatistics((OlapTable) getTable("rf_dim"), 90_000_000);
            new MockUp<MockTpchStatisticStorage>() {
                @Mock
                public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                    return columns.stream().map(c -> ColumnStatistic.unknown()).collect(Collectors.toList());
                }
            };

            String plan = getVerboseExplain("select * from rf_fact join [shuffle] rf_dim on f_cust = d_cust");
            assertNotContains(plan, "probe runtime filters");
        } finally {
            sv.setGlobalRuntimeFilterProbeMinSize(savedProbeMin);
            sv.setEnableGlobalRuntimeFilter(savedGrf);
        }
    }

    @Test
    public void testProbeGateUsesFrequencyMassInsteadOfDistinctRatio() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        long probeMin = sv.getGlobalRuntimeFilterProbeMinSize();
        long buildMin = sv.getGlobalRuntimeFilterBuildMinSize();
        boolean global = sv.getEnableGlobalRuntimeFilter();
        AtomicBoolean matchesHotKey = new AtomicBoolean(true);
        try {
            sv.setGlobalRuntimeFilterProbeMinSize(1);
            sv.setGlobalRuntimeFilterBuildMinSize(0);
            sv.setEnableGlobalRuntimeFilter(true);
            setTableStatistics((OlapTable) getTable("rf_fact"), 100_000_000);
            setTableStatistics((OlapTable) getTable("rf_dim"), 5_000_000);
            new MockUp<MockTpchStatisticStorage>() {
                @Mock
                public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                    return columns.stream().map(column -> {
                        if (column.equals("d_cust")) {
                            return colStat(1);
                        }
                        if (column.equals("f_cust")) {
                            return colStat(1000);
                        }
                        return colStat(1000);
                    }).collect(Collectors.toList());
                }
            };
            // Supply collected MCV distributions at the physical scan boundary.
            new MockUp<PlanNode>() {
                @Mock
                public void computeStatistics(Invocation invocation, Statistics statistics) {
                    if (invocation.getInvokedInstance() instanceof OlapScanNode && statistics != null) {
                        Statistics.Builder builder = Statistics.buildFrom(statistics);
                        statistics.getColumnStatistics().forEach((column, basic) -> {
                            boolean build = column.getName().equals("d_cust");
                            if (build || column.getName().equals("f_cust")) {
                                MultiColumnCombinedStats group = new MultiColumnCombinedStats(build ? 1 : 1000,
                                        build ? 5_000_000 : 100_000_000, List.of(column), List.of(
                                                new MultiColumnCombinedStats.McvEntry(
                                                        List.of(build && !matchesHotKey.get() ? "2" : "1"),
                                                        build ? 5_000_000 : 90_000_000)));
                                builder.addMultiColumnStatistics(Set.of(column), group);
                            }
                        });
                        statistics = builder.build();
                    }
                    invocation.proceed(statistics);
                }
            };
            String query = "select * from rf_fact join [shuffle] rf_dim on f_cust = d_cust";
            assertNotContains(getVerboseExplain(query), "remote = true");
            matchesHotKey.set(false);
            assertContains(getVerboseExplain(query), "remote = true");
        } finally {
            sv.setGlobalRuntimeFilterProbeMinSize(probeMin);
            sv.setGlobalRuntimeFilterBuildMinSize(buildMin);
            sv.setEnableGlobalRuntimeFilter(global);
        }
    }

    @Test
    public void testConditionalMcvSurvivesPhysicalBuildProjection() throws Exception {
        assertConditionalMcvSurvivesBuildOperators(-1);
    }

    @Test
    public void testConditionalMcvSurvivesSemiJoinDeduplication() throws Exception {
        assertConditionalMcvSurvivesBuildOperators(0);
    }

    @Test
    public void testConditionalMcvSurvivesTwoStageSemiJoinDeduplication() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        int stage = sv.getNewPlannerAggStage();
        try {
            sv.setNewPlanerAggStage(2);
            assertConditionalMcvSurvivesBuildOperators(0);
        } finally {
            sv.setNewPlanerAggStage(stage);
        }
    }

    private void assertConditionalMcvSurvivesBuildOperators(int mode) throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        long probeMin = sv.getGlobalRuntimeFilterProbeMinSize();
        long buildMin = sv.getGlobalRuntimeFilterBuildMinSize();
        int deduplicateMode = sv.getSemiJoinDeduplicateMode();
        String buildTable = sv.getNewPlannerAggStage() == 2 ? "rf_agg_dim" : "rf_dim";
        try {
            sv.setGlobalRuntimeFilterProbeMinSize(1);
            sv.setGlobalRuntimeFilterBuildMinSize(0);
            sv.setSemiJoinDeduplicateMode(mode);
            setTableStatistics((OlapTable) getTable("rf_fact"), 100_000_000);
            setTableStatistics((OlapTable) getTable(buildTable), 5_000_000);
            new MockUp<MockTpchStatisticStorage>() {
                @Mock
                public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                    return columns.stream().map(column -> colStat(column.startsWith("d_") ? 2 : 1000)).toList();
                }
            };
            // Supply collected groups before optimization, so the real filter and projection paths run.
            new MockUp<StatisticsCalcUtils>() {
                @Mock
                public Statistics.Builder estimateMultiColumnCombinedStats(Table table, Statistics.Builder builder,
                        Map<ColumnRefOperator, Column> refs, OptimizerContext optimizer) {
                    ColumnRefOperator key = refs.keySet().stream()
                            .filter(column -> column.getName().endsWith("_cust")).findFirst().orElseThrow();
                    boolean build = table.getName().equals(buildTable);
                    List<MultiColumnCombinedStats.McvEntry> head = build ? List.of(
                            new MultiColumnCombinedStats.McvEntry(List.of("1"), 4_500_000),
                            new MultiColumnCombinedStats.McvEntry(List.of("2"), 500_000)) : List.of(
                                    new MultiColumnCombinedStats.McvEntry(List.of("1"), 90_000_000));
                    builder.addMultiColumnStatistics(Set.of(key), new MultiColumnCombinedStats(build ? 2 : 1000,
                            build ? 5_000_000 : 100_000_000, List.of(key), head));
                    if (build) {
                        refs.keySet().stream().filter(column -> column.getName().equals("d_region")).findFirst()
                                .ifPresent(flag -> builder.addMultiColumnStatistics(Set.of(key, flag),
                                        new MultiColumnCombinedStats(2, 5_000_000, List.of(key, flag), List.of(
                                                new MultiColumnCombinedStats.McvEntry(List.of("1", "0"), 4_500_000),
                                                new MultiColumnCombinedStats.McvEntry(List.of("2", "1"), 500_000)))));
                    }
                    return builder;
                }
            };
            String query = "select f_cust from rf_fact left semi join [shuffle] " + buildTable
                    + " on f_cust = d_cust and d_region = ";
            String hotPlan = getVerboseExplain(query + "0");
            assertNotContains(hotPlan, "build runtime filters");
            assertContains(getVerboseExplain(query + "1"), "remote = true");
            // The fused scan projection must keep both the source distribution and its cast.
            String castQuery = query.replace("f_cust = d_cust", "cast(f_cust as decimal(38,9)) = "
                    + "cast(d_cust as decimal(38,9))");
            assertNotContains(getVerboseExplain(castQuery + "0"), "build runtime filters");
            assertContains(getVerboseExplain(castQuery + "1"), "remote = true");
            if (mode == 0) {
                assertContains(hotPlan, "AGGREGATE");
                if (sv.getNewPlannerAggStage() == 2) {
                    assertContains(hotPlan, "AGGREGATE (merge finalize)", "AGGREGATE (update serialize)");
                }
            }
            // Probe deduplication removes the hot key's 90% row weight: it now emits one group.
            String groupedProbe = "select f_cust from (select f_cust from rf_fact group by f_cust) p "
                    + "left semi join [shuffle] " + buildTable + " on f_cust = d_cust and d_region = 0";
            assertContains(getVerboseExplain(groupedProbe), "remote = true");
        } finally {
            sv.setGlobalRuntimeFilterProbeMinSize(probeMin);
            sv.setGlobalRuntimeFilterBuildMinSize(buildMin);
            sv.setSemiJoinDeduplicateMode(deduplicateMode);
        }
    }

    @Test
    public void testJointMcvSelectsIncrementalComponentFilters() throws Exception {
        SessionVariable saved = connectContext.getSessionVariable();
        connectContext.setSessionVariable((SessionVariable) saved.clone());
        SessionVariable sv = connectContext.getSessionVariable();
        AtomicBoolean independent = new AtomicBoolean(false);
        try {
            sv.setGlobalRuntimeFilterProbeMinSize(1);
            sv.setGlobalRuntimeFilterBuildMaxSize(1000000);
            sv.setSemiJoinDeduplicateMode(-1);
            setTableStatistics((OlapTable) getTable("rf_fact"), 100_000_000);
            setTableStatistics((OlapTable) getTable("rf_dim"), 5_000_000);
            new MockUp<MockTpchStatisticStorage>() {
                @Mock
                public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                    return columns.stream().map(c -> colStat(2)).collect(Collectors.toList());
                }
            };
            new MockUp<StatisticsCalcUtils>() {
                @Mock
                public Statistics.Builder estimateMultiColumnCombinedStats(Table table, Statistics.Builder builder,
                        Map<ColumnRefOperator, Column> refs, OptimizerContext optimizer) {
                    ColumnRefOperator x = refs.keySet().stream()
                            .filter(c -> c.getName().endsWith("_cust")).findFirst().orElseThrow();
                    ColumnRefOperator y = refs.keySet().stream()
                            .filter(c -> c.getName().endsWith("_region")).findFirst().orElseThrow();
                    boolean build = table.getName().equals("rf_dim");
                    List<MultiColumnCombinedStats.McvEntry> head = build ? List.of(
                            new MultiColumnCombinedStats.McvEntry(List.of("1", "10"), 5_000_000))
                            : independent.get() ? List.of(
                                    new MultiColumnCombinedStats.McvEntry(List.of("1", "10"), 25_000_000),
                                    new MultiColumnCombinedStats.McvEntry(List.of("1", "20"), 25_000_000),
                                    new MultiColumnCombinedStats.McvEntry(List.of("2", "10"), 25_000_000),
                                    new MultiColumnCombinedStats.McvEntry(List.of("2", "20"), 25_000_000))
                            : List.of(new MultiColumnCombinedStats.McvEntry(List.of("1", "10"), 50_000_000),
                                    new MultiColumnCombinedStats.McvEntry(List.of("2", "20"), 50_000_000));
                    builder.addMultiColumnStatistics(Set.of(x, y), new MultiColumnCombinedStats(head.size(),
                            build ? 5_000_000 : 100_000_000, List.of(x, y), head));
                    return builder;
                }
            };
            for (String distribution : List.of("shuffle", "broadcast")) {
                String query = "select f_cust from rf_fact left semi join [" + distribution
                        + "] rf_dim on f_cust=d_cust and f_region=d_region";
                independent.set(false);
                sv.setEnableJointRuntimeFilterSelection(false);
                org.junit.jupiter.api.Assertions.assertEquals(2, buildFilterCount(getVerboseExplain(query)));
                sv.setEnableJointRuntimeFilterSelection(true);
                org.junit.jupiter.api.Assertions.assertEquals(1, buildFilterCount(getVerboseExplain(query)));
                independent.set(true);
                org.junit.jupiter.api.Assertions.assertEquals(2, buildFilterCount(getVerboseExplain(query)));
            }
        } finally {
            connectContext.setSessionVariable(saved);
        }
    }

    private long buildFilterCount(String plan) {
        return plan.lines().filter(line -> line.contains("filter_id =") && line.contains("build_expr =")).count();
    }

    @Test
    public void testNullSafeFloatingPointJoinKeepsNanRows() throws Exception {
        for (String type : new String[] {"FLOAT", "DOUBLE"}) {
            String sql = "select * from (select cast(v1 as " + type + ") k from t0) a "
                    + "join [broadcast] (select cast(v4 as " + type + ") k from t1) b on a.k <=> b.k";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "<=>");
            assertNotContains(plan, "build runtime filters:");
        }
        String integerPlan = getVerboseExplain("select * from t0 join [broadcast] t1 on v1 <=> v4");
        assertContains(integerPlan, "build runtime filters:");
    }

    @Test
    public void testDeterministicBroadcastJoinForColocateJoin() throws Exception {
        String sql = "select * from \n" +
                "  t0 vt1 join [bucket] t0 vt2 on vt1.v1 = vt2.v1\n" +
                "  join [broadcast] t1 vt3 on vt1.v1 = vt3.v4\n" +
                "  join [colocate] t0 vt4 on vt1.v1 = vt4.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [1: v1, BIGINT, true] = [7: v4, BIGINT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 1, build_expr = (7: v4), remote = true\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 1");

    }

    @Test
    public void testDeterministicBroadcastJoinForBroadcastJoin() throws Exception {
        String sql = "select * from \n" +
                "  t0 vt1 join [bucket] t0 vt2 on vt1.v1 = vt2.v1\n" +
                "  join [broadcast] t1 vt3 on vt1.v1 = vt3.v4\n" +
                "  join [broadcast] t0 vt4 on vt1.v1 = vt4.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  |----5:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 1\n" +
                "  |       probe runtime filters:\n" +
                "  |       - filter_id = 2, probe_expr = (7: v4)");
    }
}
