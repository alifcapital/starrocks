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

package com.starrocks.warehouse;

import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.QueryStatisticsInfo;
import com.starrocks.qe.QueryStatisticsItem;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class WarehouseProcDirTest {
    @Test
    public void testCountsIncludeLocalAndRemoteQueries(@Mocked GlobalStateMgr state, @Mocked NodeMgr nodes,
                                                      @Mocked Warehouse warehouse) {
        List<String> metadata = new ArrayList<>(Collections.nCopies(14, ""));
        metadata.set(1, "etl");
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = state;
                state.getNodeMgr();
                result = nodes;
                warehouse.getName();
                result = "etl";
                warehouse.getWarehouseInfo();
                result = metadata;
                nodes.getQueryStatisticsInfoFromOtherFEs(false);
                result = List.of(new QueryStatisticsInfo().withWareHouseName("etl").withExecState("PENDING"),
                        new QueryStatisticsInfo().withWareHouseName("etl").withExecState("RUNNING"),
                        new QueryStatisticsInfo().withWareHouseName("interactive").withExecState("RUNNING"));
            }
        };
        new MockUp<QeProcessorImpl>() {
            @Mock
            public Map<String, QueryStatisticsItem> getQueryStatistics() {
                return Map.of("local", new QueryStatisticsItem.Builder().warehouseName("etl")
                        .execState("RUNNING").queryType("Statistics").build());
            }
        };
        BaseProcResult result = WarehouseProcDir.buildResult(List.of(warehouse));
        Assertions.assertEquals("2", result.getRows().get(0).get(7));
        Assertions.assertEquals("1", result.getRows().get(0).get(8));
        Assertions.assertEquals(result.getColumnNames().size(), result.getRows().get(0).size());
    }
}
