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

import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.type.IntegerType;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.List;

public class HiveScanTest extends ConnectorPlanTestBase {
    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.doInit(newFolder(temp, "junit").toURI().toString());
        connectContext.changeCatalogDb("hive0.partitioned_db");
    }

    @Test
    public void testStatsSourceInExplain() throws Exception {
        // External (connector) scan nodes surface their StatsSource in `explain costs` / `explain verbose`.
        // The mocked hive metadata does not tag a source, so it renders as NONE here; the point is that
        // the line is emitted for the connector scan node.
        String hiveSql = "select * from hive0.tpch.region";
        String costPlan = getCostExplain(hiveSql);
        assertContains(costPlan, "HdfsScanNode");
        assertContains(costPlan, "stats source:");
        assertContains(getVerboseExplain(hiveSql), "stats source:");
        // OlapScanNode is intentionally excluded (its source is always ANALYZE and would churn plan
        // tests); that exclusion is covered by the OlapScan-based plan suites such as ExplainTest.
    }

    @Test
    public void testPruneColumnTest() throws Exception {
        connectContext.getSessionVariable().setEnableCountStarOptimization(true);
        String[] sqlString = {
                "select count(*) from lineitem_par",
                "select count(*) from lineitem_par where l_shipdate = '1998-01-01'"
        };
        boolean[] expected = {true, true};
        Assertions.assertEquals(sqlString.length, expected.length);
        for (int i = 0; i < sqlString.length; i++) {
            String sql = sqlString[i];
            ExecPlan plan = getExecPlan(sql);
            List<ScanNode> scanNodeList = plan.getScanNodes();
            Assertions.assertEquals(scanNodeList.get(0).getScanOptimizeOption().getCanUseAnyColumn(), expected[i]);
        }

        connectContext.getSessionVariable().setEnableCountStarOptimization(false);
        for (int i = 0; i < sqlString.length; i++) {
            String sql = sqlString[i];
            ExecPlan plan = getExecPlan(sql);
            List<ScanNode> scanNodeList = plan.getScanNodes();
            Assertions.assertEquals(scanNodeList.get(0).getScanOptimizeOption().getCanUseAnyColumn(), false);
        }
        connectContext.getSessionVariable().setEnableCountStarOptimization(true);
    }

    @Test
    public void testUseMinMaxOptTest() throws Exception {
        String[] sqlString = {
                "select min(id) from iceberg0.partitioned_db.t1", "true",
                "select min(id) from iceberg0.partitioned_db.t1 where date = '2020-01-01'", "true",
                "select max(id) from iceberg0.partitioned_db.t1 where date = '2020-01-01'", "true",
                "select count(id) from iceberg0.partitioned_db.t1 where date = '2020-01-01'", "false",
                "select min(id), date from iceberg0.partitioned_db.t1 group by date", "true",
                "select max(id), date from iceberg0.partitioned_db.t1 group by date", "true",
                "select max(id) as x, date from iceberg0.partitioned_db.t1 group by date having x > 10", "false",
                "select max(id) as x, date from iceberg0.partitioned_db.t1 group by date having date > '2020-01-01'", "true",
        };
        Assertions.assertTrue(sqlString.length % 2 == 0);
        for (int i = 0; i < sqlString.length; i += 2) {
            String sql = sqlString[i];
            boolean expexted = Boolean.valueOf(sqlString[i + 1]);
            ExecPlan plan = getExecPlan(sql);
            List<ScanNode> scanNodeList = plan.getScanNodes();
            Assertions.assertEquals(expexted, scanNodeList.get(0).getScanOptimizeOption().getCanUseMinMaxOpt());
        }
    }

    @Test
    public void testHiveRewriteSimpleAggToHdfsScan() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToHdfsScan(true);
        // positive cases.
        {
            String[] sqlString = {
                    "select count(*) + 1 from lineitem_par",
                    "select count(*) + 1 from lineitem_par where l_shipdate = '1998-01-01'",
                    "select count(*) + 1, l_shipdate from lineitem_par where l_shipdate = '1998-01-01' group by l_shipdate",
                    "select count(*) from lineitem_par",
                    "select count(*) from lineitem_par where l_shipdate = '1998-01-01'",
                    "select count(*), l_shipdate from lineitem_par where l_shipdate = '1998-01-01' group by l_shipdate",
                    "select count(*), l_shipdate from lineitem_par group by l_shipdate having l_shipdate > '1998-01-01'",
            };
            for (int i = 0; i < sqlString.length; i++) {
                String sql = sqlString[i];
                String plan = getFragmentPlan(sql);
                assertContains(plan, "___count___");
                assertContains(plan, "ifnull");
            }
        }
        // negative cases.
        {
            String[] sqlString = {
                    "select count(l_orderkey) from lineitem_par",
                    "select count(*) from lineitem_par where l_shipdate = '1998-01-01' and l_orderkey = 202",
                    "select count(*), count(l_orderkey), l_shipdate from lineitem_par where l_shipdate = '1998-01-01' group by " +
                            "l_shipdate",
                    "select count(*) as x, l_shipdate from lineitem_par group by l_shipdate having x > 10",
                    // The scan answers one count only.
                    "select count(*), count(1) from lineitem_par",
                    "select count(1), count(2), l_shipdate from lineitem_par group by l_shipdate",
            };
            for (int i = 0; i < sqlString.length; i++) {
                String sql = sqlString[i];
                String plan = getFragmentPlan(sql);
                assertNotContains(plan, "___count___");
            }
        }
        // bad cases
        {
            String[] sqlString = {
                    "select distinct l_shipdate from lineitem_par",
                    "select count(distinct l_shipdate) from lineitem_par",
            };
            for (int i = 0; i < sqlString.length; i++) {
                String sql = sqlString[i];
                // just make sure it's not stuck.
                String plan = getFragmentPlan(sql);
                assertNotContains(plan, "___count___");
            }
        }

        // negative cases, because customer_union_view is a view.
        {
            String[] sqlString = {
                    "select l_shipdate, count(*) from hive0.tpch.customer_union_view where l_shipdate = '1998-01-02' group by 1",
                    "select count(*) from hive0.tpch.customer_union_view",
            };
            for (int i = 0; i < sqlString.length; i++) {
                String sql = sqlString[i];
                String plan = getFragmentPlan(sql);
                assertNotContains(plan, "___count___");
                assertNotContains(plan, "ifnull");
            }
        }
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToHdfsScan(false);
    }

    @Test
    public void testCountPlaceholderSlotIsBigint() throws Exception {
        // With one-stage aggregation the scan ships ___count___ through an exchange before the sum,
        // so the receiver deserializes it by the scan slot type, which must match the BIGINT BE fills.
        SessionVariable sv = connectContext.getSessionVariable();
        int aggStage = sv.getNewPlannerAggStage();
        sv.setEnableRewriteSimpleAggToHdfsScan(true);
        sv.setNewPlanerAggStage(1);
        try {
            String[] sqlString = {
                    "select count(*) from lineitem_par",
                    "select count(*) from iceberg0.partitioned_db.t1",
            };
            for (String sql : sqlString) {
                ExecPlan plan = getExecPlan(sql);
                assertContains(plan.getExplainString(TExplainLevel.NORMAL), "___count___");
                ScanNode scanNode = plan.getScanNodes().get(0);
                SlotDescriptor countSlot = plan.getDescTbl().getTupleDesc(scanNode.getTupleIds().get(0))
                        .getSlots().stream()
                        .filter(slot -> slot.getColumn() != null && "___count___".equals(slot.getColumn().getName()))
                        .findFirst().orElseThrow();
                Assertions.assertEquals(IntegerType.BIGINT, countSlot.getType(), sql);
            }
        } finally {
            sv.setNewPlanerAggStage(aggStage);
            sv.setEnableRewriteSimpleAggToHdfsScan(false);
        }
    }

    @Test
    public void testIcebergRewriteSimpleAggToHdfsScan() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToHdfsScan(true);
        // positive cases.
        {
            String[] sqlString = {
                    "select count(*) + 1 from iceberg0.partitioned_db.t1",
                    "select count(*) + 1 from iceberg0.partitioned_db.t1 where date = '2020-01-01'",
                    "select count(*) + 1, date from iceberg0.partitioned_db.t1 where date = '2020-01-01' " +
                            "group by date",
                    "select count(*) from iceberg0.partitioned_db.t1",
                    "select count(*) from iceberg0.partitioned_db.t1 where date = '2020-01-01'",
                    "select count(*), date from iceberg0.partitioned_db.t1 where date = '2020-01-01' " +
                            "group by date",
                    "select count(*), date from iceberg0.partitioned_db.t1 group by date having date > '2020-01-01'",
            };
            for (int i = 0; i < sqlString.length; i++) {
                String sql = sqlString[i];
                String plan = getFragmentPlan(sql);
                assertContains(plan, "___count___");
                assertContains(plan, "ifnull");
            }
        }
        // negative cases.
        {
            String[] sqlString = {
                    "select count(id) from iceberg0.partitioned_db.t1",
                    "select count(*) from iceberg0.partitioned_db.t1 where date = '1998-01-01' and id =" +
                            " 202",
                    "select count(*), count(id), date from iceberg0.partitioned_db.t1 where date " +
                            "= '2020-01-01' group by " +
                            "date",
                    "select count(*) as x, date from iceberg0.partitioned_db.t1 group by date having x > 10",
            };
            for (int i = 0; i < sqlString.length; i++) {
                String sql = sqlString[i];
                String plan = getFragmentPlan(sql);
                assertNotContains(plan, "___count___");
            }
        }
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToHdfsScan(false);
    }

    @Test
    public void testJsonPathRewriteOverCastOnIcebergString() throws Exception {
        String sql = "select cast(cast(data as json) -> '$.body' -> '$.txn' as char) from iceberg0.partitioned_db.t1 "
                + "where cast(cast(data as json) -> '$.body' -> '$.mode' as char) = 'x'";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "data, '$.body.txn')", "data, '$.body.mode')");
        assertNotContains(plan, "AS JSON", "parse_json(");
    }

    @Test
    public void testJsonPathRewriteOfScanConjuncts() throws Exception {
        // Every conjunct over the column gets one json_extract_paths() call with all paths of the predicate;
        // BE shares its result between the conjuncts of a chunk.
        String sql = "select id from iceberg0.partitioned_db.t1 "
                + "where get_json_string(parse_json(data), '$.posData.terminalType') = 'ATM' "
                + "and get_json_string(parse_json(data), '$.merchantData.terminalId') like 'A0400%' "
                + "and nullif(get_json_string(parse_json(data), '$.referenceData.rrn'), '') is not null";
        String plan = getFragmentPlan(sql);
        String extract = "json_extract_paths(2: data, '$.posData.terminalType', '$.merchantData.terminalId', "
                + "'$.referenceData.rrn')";
        Assertions.assertEquals(3, StringUtils.countMatches(plan, extract), plan);
        assertContains(plan, "get_json_string(" + extract + ", '$.0') = 'ATM'",
                "get_json_string(" + extract + ", '$.1') LIKE 'A0400%'",
                "nullif(get_json_string(" + extract + ", '$.2'), '') IS NOT NULL");
        assertNotContains(plan, "parse_json(");

        // One path keeps the extraction from text.
        plan = getFragmentPlan("select id from iceberg0.partitioned_db.t1 "
                + "where get_json_string(parse_json(data), '$.posData.terminalType') = 'ATM'");
        assertContains(plan, "get_json_string(2: data, '$.posData.terminalType') = 'ATM'");
        assertNotContains(plan, "json_extract_paths", "parse_json(");

        // get_json_string over the text takes part in the group as well.
        plan = getFragmentPlan("select id from iceberg0.partitioned_db.t1 "
                + "where get_json_string(data, '$.a') = 'x' and get_json_string(data, '$.b') = 'y'");
        assertContains(plan, "get_json_string(json_extract_paths(2: data, '$.a', '$.b'), '$.0') = 'x'",
                "get_json_string(json_extract_paths(2: data, '$.a', '$.b'), '$.1') = 'y'");
    }

    @Test
    public void testJsonPathRewriteOfSubqueryAndCte() throws Exception {
        // parse_json(x) AS j in a subquery: the conditions on j reach the scan and the projection is merged
        // into the scan before the rewrite, so both read the text.
        String sql = "select j -> 'body', get_json_string(j, '$.id') from "
                + "(select parse_json(data) j from iceberg0.partitioned_db.t1) s "
                + "where get_json_string(j, '$.a') = 'x' and get_json_string(j, '$.b') = 'y'";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "parse_json(");
        assertContains(plan, "json_extract_paths(2: data, '$.a', '$.b')", "json_extract_paths(2: data, 'body', '$.id')");

        String cte = "with c as (select parse_json(data) j, id from iceberg0.partitioned_db.t1) "
                + "select get_json_string(c1.j, '$.a'), get_json_string(c2.j, '$.b') "
                + "from c c1 join c c2 on c1.id = c2.id";
        plan = getFragmentPlan(cte);
        assertNotContains(plan, "parse_json(");
        assertContains(plan, "get_json_string(", "data, '$.a')", "data, '$.b')");
    }
}
