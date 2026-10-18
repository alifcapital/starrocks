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

package com.starrocks.statistic;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatisticsType;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ExternalMcvBatchCollectTest {
    private static ConnectContext context;
    private static Table table;
    private static final String SOURCE = "`hive0`.`tpch`.`customer`";
    private static final List<List<String>> GROUPS = List.of(
            List.of("c_name", "c_phone"), List.of("c_phone", "c_custkey"), List.of("c_custkey"));

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        context = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(context);
        table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, "hive0", "tpch", "customer");
    }

    private static class CapturingJob extends ExternalMcvStatisticsCollectJob {
        private final List<String> queries = new ArrayList<>();
        private final List<List<List<String>>> responses;

        CapturingJob(List<List<List<String>>> responses) {
            this(responses, GROUPS);
        }

        CapturingJob(List<List<List<String>>> responses, List<List<String>> groups) {
            super("hive0", new Database(1, "tpch"), ExternalMcvBatchCollectTest.table,
                    groups.stream().flatMap(List::stream).distinct().toList(), List.of(),
                    StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Map.of(),
                    List.of(StatisticsType.MCV), groups);
            this.responses = responses;
        }

        @Override
        List<List<String>> execute(ConnectContext ctx, AnalyzeStatus status, String sql) {
            queries.add(sql);
            return responses.get(queries.size() - 1);
        }
    }

    private static String histogram(String key, int count) {
        return "{\"mcv\":[[\"" + key + "\",\"" + count + "\"]],\"buckets\":[]}";
    }

    @Test
    public void testBatchHasOneScanPerPassAndIndependentResults() throws Exception {
        List<String> sketches = List.of("100", "[[\"alice#123\",\"50\"]]", "20",
                "100", "[[\"123#7\",\"30\"]]", "40", "100", "[[\"7\",\"60\"]]", "10", "[1,9]");
        List<String> counts = new ArrayList<>(List.of(histogram("alice#123", 50), "100",
                histogram("alice", 70), "90", histogram("123", 80), "100",
                histogram("123#7", 30), "100", histogram("123", 80), "100", histogram("7", 60), "95",
                histogram("7", 60), "100", histogram("7", 60), "95",
                "{\"mcv\":[],\"buckets\":[[\"1\",\"9\",\"35\",\"5\",\"9\"]]}"));
        CapturingJob batch = new CapturingJob(List.of(List.of(sketches), List.of(counts)));
        List<ExternalMcvStatisticsCollectJob.GroupStatistics> results = batch.collectBatch(context, null, GROUPS, SOURCE);
        Assertions.assertEquals(2, batch.queries.size());
        Assertions.assertEquals(List.of(20L, 40L, 10L), results.stream().map(group -> group.ndv).toList());
        Assertions.assertEquals(List.of(50L, 30L, 60L), results.stream().map(group -> group.mcv.get(0).count).toList());
        Assertions.assertEquals(List.of(10L, 0L), results.get(0).nullCounts);
        Assertions.assertEquals(List.of(0L, 5L), results.get(1).nullCounts);
        Assertions.assertEquals(List.of(70L, 80L), results.get(0).mcv.get(0).componentCounts);
        Assertions.assertEquals(1, results.get(2).buckets.size());
        int sketchOffset = 0;
        int countOffset = 0;
        for (int i = 0; i < GROUPS.size(); i++) {
            int sketchSize = i == 2 ? 4 : 3;
            int countSize = i == 2 ? 5 : 6;
            CapturingJob single = new CapturingJob(List.of(
                    List.of(sketches.subList(sketchOffset, sketchOffset + sketchSize)),
                    List.of(counts.subList(countOffset, countOffset + countSize))));
            var isolated = single.collectBatch(context, null, List.of(GROUPS.get(i)), SOURCE).get(0);
            Assertions.assertEquals(ExternalMcvStatisticsCollectJob.buildMcvJson(isolated.mcv),
                    ExternalMcvStatisticsCollectJob.buildMcvJson(results.get(i).mcv));
            Assertions.assertEquals(isolated.buckets, results.get(i).buckets);
            sketchOffset += sketchSize;
            countOffset += countSize;
        }
        for (String sql : batch.queries) {
            String plan = UtFrameUtils.getFragmentPlan(context, sql);
            Assertions.assertEquals(1, plan.split("HdfsScanNode", -1).length - 1, plan);
            Assertions.assertFalse(plan.contains("MULTICAST"), plan);
        }
    }

    @Test
    public void testExpressionElementsAreProjectedAndPlanned() throws Exception {
        String prefix = "expr:1:substr(`c_phone`, 1, 3)";
        String lower = "expr:1:lower(`c_name`)";
        List<List<String>> groups = List.of(List.of(prefix), List.of("c_name", lower));
        List<String> sketches = List.of("100", "[[\"123\",\"50\"]]", "20", "[\"100\",\"200\"]",
                "100", "[[\"alice#alice\",\"30\"]]", "40");
        List<String> counts = List.of(histogram("123", 50), "100", histogram("123", 50), "100",
                "{\"mcv\":[],\"buckets\":[[\"100\",\"200\",\"40\",\"5\",\"9\"]]}",
                histogram("alice#alice", 30), "100", histogram("alice", 30), "100", histogram("alice", 30), "100");
        CapturingJob job = new CapturingJob(List.of(List.of(sketches), List.of(counts)), groups);
        job.resolveExpressions(context);
        var results = job.collectBatch(context, null, groups, SOURCE);
        // The string expression of a singleton is ordered: its value is projected for the bounds and buckets.
        Assertions.assertTrue(job.queries.get(0).contains("(substr(`c_phone`, 1, 3)) AS g0_x"), job.queries.get(0));
        Assertions.assertTrue(job.queries.get(0).contains("ds_kll_quantiles(g0_x, "), job.queries.get(0));
        Assertions.assertTrue(job.queries.get(0).contains("cast((lower(`c_name`)) as varchar) AS g1_v1"),
                job.queries.get(0));
        Assertions.assertEquals(1, results.get(0).buckets.size());
        Assertions.assertEquals(50, results.get(0).mcv.get(0).count);
        Assertions.assertEquals(List.of("alice", "alice"), results.get(1).mcv.get(0).values);
        for (String sql : job.queries) {
            String plan = UtFrameUtils.getFragmentPlan(context, sql);
            Assertions.assertEquals(1, plan.split("HdfsScanNode", -1).length - 1, plan);
        }
    }

    @Test
    public void testExpressionElementsInStorage() throws Exception {
        String prefix = "expr:1:substr(`c_phone`, 1, 3)";
        var template = com.starrocks.sql.optimizer.statistics.ExpressionTemplate.parse("substr(`c_phone`, 1, 3)",
                table, context);
        String json = ExternalMcvStatisticsCollectJob.buildColumnNamesJson(List.of("c_name", prefix),
                Map.of(prefix, template));
        var array = com.google.gson.JsonParser.parseString(json).getAsJsonArray();
        Assertions.assertEquals("c_name", array.get(0).getAsString());
        var element = array.get(1).getAsJsonObject();
        Assertions.assertEquals("substr(`c_phone`, 1, 3)", element.get("expr").getAsString());
        Assertions.assertEquals(1, element.get("format").getAsInt());
        Assertions.assertEquals(template.getType().toSql(), element.get("type").getAsString());
        var columns = element.getAsJsonArray("columns");
        Assertions.assertEquals(1, columns.size());
        Assertions.assertEquals("c_phone", columns.get(0).getAsJsonObject().get("name").getAsString());
        Assertions.assertFalse(columns.get(0).getAsJsonObject().has("fieldId"));

        // The key of an expression element never equals the key of a column named by its text.
        Assertions.assertNotEquals(ExternalMcvStatisticsCollectJob.buildColumnIds(List.of(prefix)),
                ExternalMcvStatisticsCollectJob.buildColumnIds(List.of("substr(`c_phone`, 1, 3)")));
        Assertions.assertEquals(ExternalMcvStatisticsCollectJob.buildColumnIds(List.of("c_name", prefix)),
                ExternalMcvStatisticsCollectJob.buildColumnIds(List.of(prefix, "c_name")));

        // An expression of another format or one the current schema cannot analyze fails the collection.
        for (String unusable : List.of("expr:2:substr(`c_phone`, 1, 3)", "expr:1:lower(`missing_column`)")) {
            CapturingJob job = new CapturingJob(List.of(), List.of(List.of(unusable)));
            Assertions.assertThrows(DdlException.class, () -> job.resolveExpressions(context), unusable);
        }
    }

    @Test
    public void testEmptySourceNeedsNoCountingPass() throws Exception {
        CapturingJob job = new CapturingJob(List.of(List.of(Arrays.asList("0", null, "0", "0", null, "0",
                "0", null, "0", null))));
        var result = job.collectBatch(context, null, GROUPS, SOURCE);
        Assertions.assertEquals(1, job.queries.size());
        Assertions.assertEquals(3, result.size());
        Assertions.assertTrue(result.stream().allMatch(group -> group.rowCount == 0 && group.mcv.isEmpty()));
        Assertions.assertEquals(List.of(0L, 0L), result.get(0).nullCounts);
    }

    @Test
    public void testMissingResultsAreFailures() {
        CapturingJob job = new CapturingJob(List.of(List.of()));
        Assertions.assertThrows(DdlException.class, () -> job.collectBatch(context, null, GROUPS, SOURCE));
        CapturingJob truncated = new CapturingJob(List.of(List.of(List.of("100", "[]"))));
        Assertions.assertThrows(DdlException.class, () -> truncated.collectBatch(context, null, GROUPS, SOURCE));
    }
}
