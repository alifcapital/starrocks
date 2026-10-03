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

package com.starrocks.sql.optimizer.rule.transformation.partition;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.TableName;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// An automatic partition table has a shadow partition without values. The partition selector evaluates the
// values of each partition with one shared recorder, so it must not give the shadow partition the result of
// the partition evaluated before it.
public class PartitionSelectorShadowPartitionTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE test.t_list_shadow (city varchar(20) not null, v int) " +
                "PARTITION BY LIST(city) (PARTITION p1 VALUES IN ('a')) " +
                "DISTRIBUTED BY HASH(v) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    @Test
    public void testShadowPartitionNotSelectedForDrop() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_list_shadow");
        ListPartitionInfo info = (ListPartitionInfo) table.getPartitionInfo();
        Partition p1 = table.getPartition("p1");

        // We pick a shadow id that the HashMap iterates after p1, so a stale result would come from p1.
        long shadowId = p1.getId() + 1;
        for (; shadowId < p1.getId() + 64; shadowId++) {
            Map<Long, Object> probe = new HashMap<>();
            probe.put(p1.getId(), 0);
            probe.put(shadowId, 0);
            if (probe.keySet().iterator().next() == p1.getId()) {
                break;
            }
        }
        info.createAutomaticShadowPartition(table.getBaseSchema(), shadowId, "1");
        Assertions.assertTrue(info.getLiteralExprValues().get(shadowId).isEmpty());

        Expr where = SqlParser.parseSqlToExpr("city = 'a'", SqlModeHelper.MODE_DEFAULT);
        List<Long> dropIds = PartitionSelector.getPartitionIdsByExpr(connectContext,
                new TableName("test", "t_list_shadow"), table, where, true);

        Assertions.assertTrue(dropIds.contains(p1.getId()));
        Assertions.assertFalse(dropIds.contains(shadowId), "shadow partition must not be dropped: " + dropIds);
        // A retention condition keeps the shadow partition.
        List<Long> retainIds = PartitionSelector.getPartitionIdsByExpr(connectContext,
                new TableName("test", "t_list_shadow"), table, where, false);
        Assertions.assertTrue(retainIds.contains(shadowId), "shadow partition must be retained: " + retainIds);
    }
}
