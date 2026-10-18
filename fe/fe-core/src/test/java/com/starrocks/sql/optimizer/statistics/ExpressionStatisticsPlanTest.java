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

import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Plans of queries on an external table with statistics on the expression left(c_phone, 3): 600 of 1000 rows start
 * with 103 and 400 with 104. The query predicate and the plan stay the same; only the estimates change.
 */
public class ExpressionStatisticsPlanTest extends ConnectorPlanTestBase {
    private static final String TEXT = "substr(`c_phone`, 1, 3)";

    private interface Body {
        void run() throws Exception;
    }

    private static Table customer() {
        return GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(connectContext, "hive0", "tpch", "customer");
    }

    private static ExternalMcvStatistics prefixStatistics() {
        Table table = customer();
        ExpressionTemplate template = ExpressionTemplate.parse(TEXT, table, connectContext);
        ExternalMcvStatistics.Expression expression = new ExternalMcvStatistics.Expression(TEXT,
                template.getType().toSql(), List.of(new ExternalMcvStatistics.StoredSource("c_phone",
                table.getColumn("c_phone").getType().toSql(), null)), template);
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("expr:1:" + TEXT), 1000, 2,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("103"), 600, List.of(600L)),
                        new MultiColumnCombinedStats.McvEntry(List.of("104"), 400, List.of(400L))),
                List.of(), List.of(0L), List.of(expression));
        group.prepare(template.getType());
        return new ExternalMcvStatistics(List.of(group));
    }

    private static void withExpressionStatistics(ExternalMcvStatistics statistics, Body body) throws Exception {
        StatisticStorage storage = GlobalStateMgr.getCurrentState().getStatisticStorage();
        StatisticStorage proxy = (StatisticStorage) Proxy.newProxyInstance(StatisticStorage.class.getClassLoader(),
                new Class<?>[] {StatisticStorage.class}, (instance, method, arguments) -> {
                    if (method.getName().equals("getExternalMcvStatistics")) {
                        return statistics;
                    }
                    try {
                        return method.invoke(storage, arguments);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        GlobalStateMgr.getCurrentState().setStatisticStorage(proxy);
        try {
            body.run();
        } finally {
            GlobalStateMgr.getCurrentState().setStatisticStorage(storage);
        }
    }

    // The cardinality of the first node with the given name in a cost plan.
    private static long cardinality(String plan, String node) {
        int at = plan.indexOf(node);
        Assertions.assertTrue(at >= 0, plan);
        Matcher matcher = Pattern.compile("cardinality: (\\d+)").matcher(plan);
        Assertions.assertTrue(matcher.find(at), plan);
        return Long.parseLong(matcher.group(1));
    }

    @Test
    public void testScanConditionIsEstimatedOverTheExpression() throws Exception {
        withExpressionStatistics(prefixStatistics(), () -> {
            long rows = cardinality(getCostExplain("select c_phone from hive0.tpch.customer"), "HdfsScanNode");
            String plan = getCostExplain("select c_phone from hive0.tpch.customer where left(c_phone, 3) = '103'");
            // The predicate is the one of the query.
            Assertions.assertTrue(plan.contains("left") || plan.contains("substr"), plan);
            Assertions.assertEquals(rows * 0.6, cardinality(plan, "HdfsScanNode"), 1.0, plan);
            plan = getCostExplain("select c_phone from hive0.tpch.customer where left(c_phone, 3) = '104'");
            Assertions.assertEquals(rows * 0.4, cardinality(plan, "HdfsScanNode"), 1.0, plan);
            // The scan of a join input gets the same estimate, and the join does not see x.
            plan = getCostExplain("select a.c_phone from hive0.tpch.customer a join hive0.tpch.customer b "
                    + "on a.c_custkey = b.c_custkey where left(a.c_phone, 3) = '103'");
            Assertions.assertTrue(plan.contains("HASH JOIN"), plan);
            Assertions.assertFalse(plan.contains("expr#"), plan);
            List<Long> scans = new ArrayList<>();
            for (int at = plan.indexOf("HdfsScanNode"); at >= 0; at = plan.indexOf("HdfsScanNode", at + 1)) {
                scans.add(cardinality(plan.substring(at), "HdfsScanNode"));
            }
            Assertions.assertTrue(scans.stream().anyMatch(scan -> Math.abs(scan - rows * 0.6) <= 1.0), plan);
        });
    }

    @Test
    public void testProjectionOfTheExpressionTakesItsDistribution() throws Exception {
        withExpressionStatistics(prefixStatistics(), () -> {
            String plan = getCostExplain("select left(c_phone, 3), count(*) from hive0.tpch.customer "
                    + "group by left(c_phone, 3)");
            Assertions.assertEquals(2, cardinality(plan, "AGGREGATE"), plan);
        });
    }

    // Statistics on an expression of a table as ANALYZE writes them: two values in 1000 rows.
    private static ExternalMcvStatistics twoValues(String table, String sql) {
        Table scanned = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(connectContext, "hive0", "tpch",
                table);
        ExpressionTemplate template = ExpressionTemplate.analyzeStable(com.starrocks.sql.parser.SqlParser.parseSqlToExpr(
                sql, com.starrocks.qe.SqlModeHelper.MODE_DEFAULT), scanned, connectContext);
        List<ExternalMcvStatistics.StoredSource> sources = template.getSources().stream()
                .map(source -> new ExternalMcvStatistics.StoredSource(source.getName(), source.getType().toSql(), null))
                .toList();
        ExternalMcvStatistics.Expression expression = new ExternalMcvStatistics.Expression(template.getText(),
                template.getType().toSql(), sources, template);
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(
                List.of(ExpressionTemplate.elementText(template.getText())), 1000, 2,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("v1"), 600, List.of(600L)),
                        new MultiColumnCombinedStats.McvEntry(List.of("v2"), 400, List.of(400L))),
                List.of(), List.of(0L), List.of(expression));
        group.prepare(template.getType());
        return new ExternalMcvStatistics(List.of(group));
    }

    @Test
    public void testQueryFormsOfTheExpressionMatch() throws Exception {
        // The statistics give the two values of each expression; without them the group count follows the values
        // of c_name or o_orderstatus. The query writes the expression in its own form: 5 < c as c > 5, BETWEEN as
        // two comparisons, the date constant as a DATE, and the CASE as it is.
        List<List<String>> cases = List.of(
                List.of("customer", "concat(c_name, if(5 < c_custkey, 'a', 'b'))"),
                List.of("customer", "concat(c_name, if(c_custkey between 1 and 5, 'a', 'b'))"),
                List.of("customer", "concat(c_name, case when c_custkey > 5 then 'a' else 'b' end)"),
                List.of("orders", "concat(o_orderstatus, if(o_orderdate >= '1995-01-01', 'a', 'b'))"));
        for (List<String> item : cases) {
            withExpressionStatistics(twoValues(item.get(0), item.get(1)), () -> {
                String plan = getCostExplain("select " + item.get(1) + ", count(*) from hive0.tpch." + item.get(0)
                        + " group by " + item.get(1));
                Assertions.assertEquals(2, cardinality(plan, "AGGREGATE"), item + "\n" + plan);
            });
        }
    }

    @Test
    public void testWithoutStatisticsOnExpressionsTheEstimateIsAsBefore() throws Exception {
        String sql = "select c_phone from hive0.tpch.customer where left(c_phone, 3) = '103'";
        long before = cardinality(getCostExplain(sql), "HdfsScanNode");
        withExpressionStatistics(ExternalMcvStatistics.EMPTY, () ->
                Assertions.assertEquals(before, cardinality(getCostExplain(sql), "HdfsScanNode")));
    }
}
