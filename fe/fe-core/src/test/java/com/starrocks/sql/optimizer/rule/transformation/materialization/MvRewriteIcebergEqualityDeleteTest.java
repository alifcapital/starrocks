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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

// MVs over Iceberg tables with equality deletes. The equality delete rule replaces a scan of such a table by a block:
// a union of the data files without deletes and an anti join of the data files with deletes against the delete files.
public class MvRewriteIcebergEqualityDeleteTest extends MVTestBase {
    private static final String DB = MockIcebergMetadata.MOCKED_EQ_DELETE_DB_NAME;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        MockIcebergMetadata.mockEqualityDeleteMvTable("mv_eq_plan", true);
        MockIcebergMetadata.mockEqualityDeleteMvTable("mv_eq_transparent", true);
        MockIcebergMetadata.mockEqualityDeleteMvTable("mv_eq_later", false);
        MockIcebergMetadata.mockEqualityDeleteMvTable("mv_eq_union", true);
        MockIcebergMetadata.mockEqualityDeleteMvTable("mv_eq_ref", false);
        MockIcebergMetadata.mockEqualityDeleteMvTable("mv_eq_other", false);
        MockIcebergMetadata.mockEqualityDeleteMvTable("mv_eq_delta_a", false);
        MockIcebergMetadata.mockEqualityDeleteMvTable("mv_eq_delta_b", false);
        MockIcebergMetadata.mockEqualityDeleteMvTable("mv_eq_global", false);
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    private static MockIcebergMetadata metadata() {
        return (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr()
                .getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
    }

    private static String table(String name) {
        return MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME + "." + DB + "." + name;
    }

    // The plan of an MV is built once and cached. We want it to read the table as a plain scan, so a query that uses
    // the plan applies the equality deletes of its own snapshot, not those of the snapshot the plan was built on.
    @Test
    public void testMvPlanReadsTheTableWithoutTheDeleteBlock() throws Exception {
        createAndRefreshMv("create materialized view mv_eq_plan_mv refresh deferred manual " +
                "as select id, data, p from " + table("mv_eq_plan"));
        try {
            MvPlanContext planContext = getOptimizedPlan(getMv("mv_eq_plan_mv"), true);
            Assertions.assertTrue(planContext.isValidMvPlan(), planContext.getInvalidReason());
            List<LogicalScanOperator> scans = MvUtils.getScanOperator(planContext.getLogicalPlan());
            Assertions.assertEquals(1, scans.size(), scans.toString());
            Assertions.assertTrue(scans.get(0) instanceof LogicalIcebergScanOperator, scans.toString());
            Assertions.assertEquals(IcebergMORParams.EMPTY, ((LogicalIcebergScanOperator) scans.get(0)).getMORParam());
        } finally {
            dropMv(DB_NAME, "mv_eq_plan_mv");
        }
    }

    // A fresh MV answers a query on the table: the query and the MV both read the table, and the query applies the
    // equality deletes after the MV took the read over.
    @Test
    public void testFreshMvAnswersAQueryOnTheTable() throws Exception {
        createAndRefreshMv("create materialized view mv_eq_used_mv refresh deferred manual " +
                "as select id, data, p from " + table("mv_eq_plan"));
        try {
            String plan = getFragmentPlan("select id, data, p from " + table("mv_eq_plan") + " where id > 1");
            PlanTestBase.assertContains(plan, "mv_eq_used_mv");
            PlanTestBase.assertNotContains(plan, "ANTI JOIN");
        } finally {
            dropMv(DB_NAME, "mv_eq_used_mv");
        }
    }

    // A transparent MV reads its stale partitions from the table. We want that read to apply the equality deletes of
    // the current snapshot, including a delete added after the MV plan was built.
    @Test
    public void testTransparentMvReadsStalePartitionsWithCurrentDeletes() throws Exception {
        createAndRefreshMv("create materialized view mv_eq_transparent_mv partition by p refresh deferred manual " +
                "properties ('transparent_mv_rewrite_mode' = 'true') " +
                "as select id, data, p from " + table("mv_eq_transparent"));
        try {
            metadata().addEqualityDelete(DB, "mv_eq_transparent", "p=a");
            metadata().updatePartitions(DB, "mv_eq_transparent", List.of("p=a"));
            IcebergTable icebergTable = (IcebergTable) GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(
                    connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME, DB, "mv_eq_transparent");
            long snapshotId = icebergTable.getNativeTable().currentSnapshot().snapshotId();

            String plan = getFragmentPlan("select id, data, p from mv_eq_transparent_mv");
            PlanTestBase.assertContains(plan, "TABLE: mv_eq_transparent_mv");
            PlanTestBase.assertContains(plan, "ANTI JOIN");
            PlanTestBase.assertContains(plan, "TABLE: " + DB + ".mv_eq_transparent_with_delete_file");
            List<String> versions = Arrays.stream(plan.split("\n"))
                    .filter(line -> line.contains("TABLE VERSION:"))
                    .toList();
            Assertions.assertFalse(versions.isEmpty(), plan);
            for (String version : versions) {
                Assertions.assertTrue(version.contains("Snapshot@(" + snapshotId + ")"), plan);
            }
        } finally {
            dropMv(DB_NAME, "mv_eq_transparent_mv");
        }
    }

    // After the equality delete rule the query reads the table through the block. An MV that reads the whole table
    // must not take the place of one scan of the block: the rest of the block would add its rows a second time.
    @Test
    public void testMvDoesNotReplaceAScanOfTheDeleteBlock() throws Exception {
        // The MV plan is built while the table has no deletes, so it reads the table as a plain scan.
        createAndRefreshMv("create materialized view mv_eq_later_mv refresh deferred manual " +
                "as select id, data, p from " + table("mv_eq_later"));
        try {
            metadata().addEqualityDelete(DB, "mv_eq_later", "p=a");
            metadata().updatePartitions(DB, "mv_eq_later", List.of("p=a"));
            refreshMaterializedView(DB_NAME, "mv_eq_later_mv");
            String query = "select id from " + table("mv_eq_later");

            // The first MV rewrite stage runs before the equality delete rule, so the MV takes the whole read.
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "mv_eq_later_mv");
            PlanTestBase.assertNotContains(plan, "ANTI JOIN");

            // Without it the MV rewrite runs on the block only.
            connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(false);
            try {
                plan = getFragmentPlan(query);
                Assertions.assertFalse(plan.contains("mv_eq_later_mv") && plan.contains("ANTI JOIN"), plan);
            } finally {
                connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(true);
            }
        } finally {
            dropMv(DB_NAME, "mv_eq_later_mv");
        }
    }

    // The plan has the MV and reads the stale partitions from the table with the equality deletes applied.
    private static void assertMvWithDeletedStalePartitions(String plan, String mvName, String tableName) {
        PlanTestBase.assertContains(plan, "TABLE: " + mvName);
        PlanTestBase.assertContains(plan, "TABLE: " + DB + "." + tableName + "_with_delete_file");
    }

    // A stale MV answers a query on the table with its fresh partitions and reads the stale ones from the table
    // through a copy of its plan.
    @Test
    public void testStalePartitionsOfAnMvApplyTheDeletes() throws Exception {
        createAndRefreshMv("create materialized view mv_eq_union_mv partition by p refresh deferred manual " +
                "as select id, data, p from " + table("mv_eq_union"));
        try {
            metadata().addEqualityDelete(DB, "mv_eq_union", "p=a");
            metadata().updatePartitions(DB, "mv_eq_union", List.of("p=a"));
            String query = "select id, data, p from " + table("mv_eq_union") + " where id > 1";
            assertMvWithDeletedStalePartitions(getFragmentPlan(query), "mv_eq_union_mv", "mv_eq_union");

            // After the equality delete rule the table is read through the block, which no MV rule rewrites.
            connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(false);
            try {
                String plan = getFragmentPlan(query);
                PlanTestBase.assertNotContains(plan, "mv_eq_union_mv");
                PlanTestBase.assertContains(plan, "TABLE: " + DB + ".mv_eq_union_with_delete_file");
            } finally {
                connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(true);
            }
        } finally {
            dropMv(DB_NAME, "mv_eq_union_mv");
        }
    }

    // The compensation of a stale partition reads every base table of the MV, also one that does not give the MV its
    // partitions. We want it to read that table on its current snapshot, not on the snapshot the MV plan was cached
    // on: here only the current snapshot has an equality delete.
    @Test
    public void testCompensationReadsTheOtherTablesOnTheirCurrentSnapshot() throws Exception {
        createAndRefreshMv("create materialized view mv_eq_join_mv partition by p refresh deferred manual " +
                "as select a.id, a.data, a.p, c.data as c_data from " + table("mv_eq_ref") + " a join " +
                table("mv_eq_other") + " c on a.id = c.id");
        try {
            metadata().addEqualityDelete(DB, "mv_eq_other", "p=b");
            refreshMaterializedView(DB_NAME, "mv_eq_join_mv");
            metadata().updatePartitions(DB, "mv_eq_ref", List.of("p=a"));
            String plan = getFragmentPlan("select a.id, a.data, a.p, c.data from " + table("mv_eq_ref") + " a join " +
                    table("mv_eq_other") + " c on a.id = c.id where a.id > 1");
            PlanTestBase.assertContains(plan, "TABLE: mv_eq_join_mv");
            PlanTestBase.assertContains(plan, "TABLE: " + DB + ".mv_eq_other_with_delete_file");
        } finally {
            dropMv(DB_NAME, "mv_eq_join_mv");
        }
    }

    // With unique and foreign key constraints an MV that joins a table the query does not read answers the query, and
    // its compensation reads that table through a copy of the MV plan. The rewrite can run after the equality delete
    // rule, so the copy applies the deletes of that table itself.
    @Test
    public void testCompensationAppliesTheDeletesOfATableTheQueryDoesNotRead() throws Exception {
        String a = table("mv_eq_delta_a");
        String b = table("mv_eq_delta_b");
        createAndRefreshMv("create materialized view mv_eq_delta_mv partition by p refresh deferred manual " +
                "properties ('unique_constraints' = '" + b + ".id', " +
                "'foreign_key_constraints' = '" + a + "(id) references " + b + "(id)') " +
                "as select a.id, a.data, a.p, b.data as b_data from " + a + " a join " + b + " b on a.id = b.id");
        try {
            metadata().addEqualityDelete(DB, "mv_eq_delta_b", "p=a");
            refreshMaterializedView(DB_NAME, "mv_eq_delta_mv");
            metadata().updatePartitions(DB, "mv_eq_delta_a", List.of("p=a"));
            String query = "select id, data, p from " + a + " where id > 1";
            for (boolean multiStages : List.of(true, false)) {
                connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(multiStages);
                try {
                    assertMvWithDeletedStalePartitions(getFragmentPlan(query), "mv_eq_delta_mv", "mv_eq_delta_b");
                } finally {
                    connectContext.getSessionVariable().setEnableMaterializedViewMultiStagesRewrite(true);
                }
            }
        } finally {
            dropMv(DB_NAME, "mv_eq_delta_mv");
        }
    }

    // An equality delete under an unpartitioned spec deletes rows in every partition, so after one no partition of the
    // MV is fresh and the query reads the table.
    @Test
    public void testMvIsNotFreshAfterAGlobalDelete() throws Exception {
        createAndRefreshMv("create materialized view mv_eq_global_mv partition by p refresh deferred manual " +
                "as select id, data, p from " + table("mv_eq_global"));
        try {
            String query = "select id, data, p from " + table("mv_eq_global") + " where id > 1";
            PlanTestBase.assertContains(getFragmentPlan(query), "mv_eq_global_mv");

            metadata().addGlobalEqualityDelete(DB, "mv_eq_global");
            String plan = getFragmentPlan(query);
            PlanTestBase.assertNotContains(plan, "mv_eq_global_mv");
            PlanTestBase.assertContains(plan, "TABLE: " + DB + ".mv_eq_global_eq_delete_id_GLOBAL");
        } finally {
            dropMv(DB_NAME, "mv_eq_global_mv");
        }
    }
}
