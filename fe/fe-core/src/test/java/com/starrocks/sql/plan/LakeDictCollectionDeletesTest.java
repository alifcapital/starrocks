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

import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TStatusCode;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

// The query that collects a global dict of an Iceberg table must read the data files as stored, deleted
// rows included. Other queries on the same table, ANALYZE among them, must still apply the deletes.
public class LakeDictCollectionDeletesTest extends ConnectorPlanTestBase {
    private static final String TABLE = "iceberg0.eq_delete_db.eq_delete_global";

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    private static void assertAppliesDeletes(ExecPlan plan) {
        Assertions.assertTrue(plan.getExplainString(TExplainLevel.NORMAL).contains("ANTI JOIN"));
        for (ScanNode scanNode : plan.getScanNodes()) {
            Assertions.assertFalse(((IcebergScanNode) scanNode).isIgnorePositionDeletes());
        }
    }

    @Test
    public void testQueryAppliesDeletes() throws Exception {
        assertAppliesDeletes(getExecPlan("select * from " + TABLE));
    }

    @Test
    public void testStatisticsQueryAppliesDeletes() throws Exception {
        // In unit tests MetadataMgr skips the internal statistics and leaves them null. On a statistics
        // connection the statistics table blacklist then returns that null instead of asking the connector, so
        // the scan gets no statistics. In production the internal statistics are never null. We keep the
        // connector path open here so that only the dict decision depends on the statistics connection.
        new MockUp<StatisticUtils>() {
            @Mock
            public static boolean statisticTableBlackListCheck(long tableId) {
                return false;
            }
        };
        connectContext.setStatisticsConnection(true);
        connectContext.setStatisticsJob(true);
        try {
            assertAppliesDeletes(getExecPlan("select count(*), max(data) from " + TABLE));
        } finally {
            connectContext.setStatisticsConnection(false);
            connectContext.setStatisticsJob(false);
        }
    }

    @Test
    public void testDictCollectionReadsFilesAsStored() throws Exception {
        List<ExecPlan> plans = new ArrayList<>();
        new MockUp<StmtExecutor>() {
            @Mock
            public Pair<List<TResultBatch>, Status> executeStmtWithExecPlan(ConnectContext context, ExecPlan plan) {
                plans.add(plan);
                return new Pair<>(new ArrayList<>(), new Status(TStatusCode.OK, "ok"));
            }
        };
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(connectContext, "iceberg0",
                "eq_delete_db", "eq_delete_global");
        try {
            StatisticExecutor.queryDictSync(table.getUUID(), "data");
        } finally {
            connectContext.setThreadLocalInfo();
        }

        Assertions.assertEquals(1, plans.size());
        ExecPlan plan = plans.get(0);
        Assertions.assertFalse(plan.getExplainString(TExplainLevel.NORMAL).contains("ANTI JOIN"));
        Assertions.assertEquals(1, plan.getScanNodes().size());
        Assertions.assertTrue(((IcebergScanNode) plan.getScanNodes().get(0)).isIgnorePositionDeletes());
        Assertions.assertTrue(plan.getConnectContext().isLakeDictCollection());
        Assertions.assertFalse(connectContext.isLakeDictCollection());
    }
}
