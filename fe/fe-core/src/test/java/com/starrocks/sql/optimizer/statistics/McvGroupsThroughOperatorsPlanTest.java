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

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A condition over a Filter, a Limit or a Window is estimated as over the scan: the operator passes its columns on
 * with their MCV groups. The table has 1000 rows, and the head of the group (c_mktsegment, c_nationkey) holds all of
 * them: (AUTOMOBILE, 3) 400 rows, (AUTOMOBILE, 7) 200, (BUILDING, 3) 100, (BUILDING, 7) 100 and (FURNITURE, 9) 200.
 */
public class McvGroupsThroughOperatorsPlanTest extends ConnectorPlanTestBase {
    private interface Body {
        void run() throws Exception;
    }

    private static MultiColumnCombinedStats.McvEntry entry(String segment, String nation, long count, long segmentRows,
                                                           long nationRows) {
        return new MultiColumnCombinedStats.McvEntry(List.of(segment, nation), count, List.of(segmentRows, nationRows));
    }

    private static ExternalMcvStatistics statistics() {
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("c_mktsegment", "c_nationkey"), 1000, 5,
                List.of(entry("AUTOMOBILE", "3", 400, 600, 500), entry("AUTOMOBILE", "7", 200, 600, 300),
                        entry("BUILDING", "3", 100, 200, 500), entry("BUILDING", "7", 100, 200, 300),
                        entry("FURNITURE", "9", 200, 200, 200)),
                List.of(), List.of(0L, 0L));
        return new ExternalMcvStatistics(List.of(group));
    }

    private static void withGroup(Body body) throws Exception {
        StatisticStorage storage = GlobalStateMgr.getCurrentState().getStatisticStorage();
        ExternalMcvStatistics statistics = statistics();
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

    private static final String WHERE = " where c_mktsegment = 'AUTOMOBILE' and c_nationkey = 3";

    @Test
    public void testConditionOnTheScanIsTheShareOfTheHead() throws Exception {
        withGroup(() -> {
            String plan = getCostExplain("select c_mktsegment, c_nationkey from hive0.tpch.customer" + WHERE);
            Assertions.assertEquals(400, cardinality(plan, "HdfsScanNode"), plan);
        });
    }

    @Test
    public void testConditionOverALimitIsTheSameShare() throws Exception {
        withGroup(() -> {
            String plan = getCostExplain("select count(*) from (select c_mktsegment, c_nationkey "
                    + "from hive0.tpch.customer limit 1000) t" + WHERE);
            Assertions.assertEquals(400, cardinality(plan, "SELECT"), plan);
            // Over a limit that keeps part of the rows, the same share of them.
            plan = getCostExplain("select count(*) from (select c_mktsegment, c_nationkey "
                    + "from hive0.tpch.customer limit 500) t" + WHERE);
            Assertions.assertEquals(200, cardinality(plan, "SELECT"), plan);
        });
    }

    @Test
    public void testLimitAboveTheRowsKeepsTheRowsOfItsInput() throws Exception {
        withGroup(() -> {
            // A limit of a billion rows over a table of 1000: the filter works on 1000 rows, not on a billion, and
            // takes the same share of them as over a limit that holds them all.
            String plan = getCostExplain("select count(*) from (select c_mktsegment, c_nationkey "
                    + "from hive0.tpch.customer limit 1000000000) t" + WHERE);
            Assertions.assertEquals(400, cardinality(plan, "SELECT"), plan);
            plan = getCostExplain("select count(*) from (select c_mktsegment, c_nationkey "
                    + "from hive0.tpch.customer limit 1000000000) t where c_mktsegment = 'AUTOMOBILE'");
            Assertions.assertEquals(600, cardinality(plan, "SELECT"), plan);
            // A limit below the rows keeps the limit.
            plan = getCostExplain("select count(*) from (select c_mktsegment, c_nationkey "
                    + "from hive0.tpch.customer limit 100) t where c_mktsegment = 'AUTOMOBILE'");
            Assertions.assertEquals(60, cardinality(plan, "SELECT"), plan);
        });
    }

    @Test
    public void testConditionOverAWindowIsTheSameShare() throws Exception {
        withGroup(() -> {
            String plan = getCostExplain("select max(rn) from (select c_mktsegment, c_nationkey, "
                    + "row_number() over (order by c_custkey) rn from hive0.tpch.customer) t" + WHERE);
            Assertions.assertEquals(400, cardinality(plan, "SELECT"), plan);
        });
    }
}
