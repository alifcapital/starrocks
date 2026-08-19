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

package com.starrocks.warehouse.multi;

import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.expression.VariableExpr;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class MultiWarehouseDisabledTest {
    @Test
    public void testDisabledRoutingPreservesAssignments() {
        boolean enabled = Config.enable_multi_warehouse;
        String statisticsWarehouse = Config.statistic_collect_warehouse;
        WarehouseManager manager = new MultiWarehouseManager();
        manager.initDefaultWarehouse();
        manager.replayCreateWarehouse(new MultiWarehouse(101, "etl", "", 201, Map.of(), 0));
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return manager;
            }
        };
        try {
            Config.enable_multi_warehouse = false;
            Config.statistic_collect_warehouse = "missing_stats";
            SessionVariable session = new SessionVariable();
            session.setWarehouseName("etl");
            Assertions.assertEquals("default_warehouse", session.getWarehouseName());
            VariableMgr variables = new VariableMgr();
            VariableExpr expression = new VariableExpr("warehouse");
            variables.fillValue(session, expression);
            Assertions.assertEquals("default_warehouse", expression.getValue());
            Assertions.assertEquals("default_warehouse", variables.getValue(session, expression));
            Assertions.assertTrue(variables.dump(SetType.SESSION, session, null).stream()
                    .anyMatch(row -> row.get(0).equals("warehouse") && row.get(1).equals("default_warehouse")));
            Assertions.assertEquals(0, manager.getWarehouseForExecution("missing").getId());
            Assertions.assertEquals(0, manager.getWarehouseForExecution(999).getId());
            Assertions.assertEquals(0, CRAcquireContext.of("missing").getWarehouseId());
            Assertions.assertEquals(0, manager.acquireComputeResource(101).getWarehouseId());
            Assertions.assertEquals("default_warehouse", StatisticUtils.getStatisticsCollectWarehouseName());
            Assertions.assertEquals(0, StatisticUtils.getStatisticsCollectWarehouse().getId());
            WarehouseComputeResource saved = new WarehouseComputeResource(101);
            String json = GsonUtils.GSON.toJson(saved);
            Assertions.assertEquals(101, GsonUtils.GSON.fromJson(json, WarehouseComputeResource.class).getWarehouseId());
            Assertions.assertEquals(201, manager.getWarehouse("etl").getAnyWorkerGroupId());

            Config.enable_multi_warehouse = true;
            Assertions.assertEquals("etl", session.getWarehouseName());
            variables.fillValue(session, expression);
            Assertions.assertEquals("etl", expression.getValue());
            Assertions.assertEquals(101, manager.getWarehouseForExecution("etl").getId());
            Assertions.assertEquals("missing_stats", StatisticUtils.getStatisticsCollectWarehouseName());
            Assertions.assertThrows(RuntimeException.class, StatisticUtils::getStatisticsCollectWarehouse);
        } finally {
            Config.enable_multi_warehouse = enabled;
            Config.statistic_collect_warehouse = statisticsWarehouse;
        }
    }
}
