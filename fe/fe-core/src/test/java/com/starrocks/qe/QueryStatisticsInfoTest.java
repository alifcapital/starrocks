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

package com.starrocks.qe;

import com.starrocks.thrift.TQueryStatisticsInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.starrocks.common.proc.CurrentGlobalQueryStatisticsProcDirTest.QUERY_ONE_LOCAL;

public class QueryStatisticsInfoTest {
    QueryStatisticsInfo firstQuery = QUERY_ONE_LOCAL;

    @Test
    public void testEquality() {
        // Use builder pattern instead of constructor to avoid parameter ordering issues
        QueryStatisticsInfo otherQuery = new QueryStatisticsInfo()
                .withQueryStartTime(firstQuery.getQueryStartTime())
                .withFeIp(firstQuery.getFeIp())
                .withQueryId(firstQuery.getQueryId())
                .withConnId(firstQuery.getConnId())
                .withDb(firstQuery.getDb())
                .withUser(firstQuery.getUser())
                .withCpuCostNs(firstQuery.getCpuCostNs())
                .withScanBytes(firstQuery.getScanBytes())
                .withScanRows(firstQuery.getScanRows())
                .withMemUsageBytes(firstQuery.getMemUsageBytes())
                .withSpillBytes(firstQuery.getSpillBytes())
                .withExecTime(firstQuery.getExecTime())
                .withExecProgress(firstQuery.getExecProgress())
                .withExecState(firstQuery.getExecState())
                .withWareHouseName(firstQuery.getWareHouseName())
                .withCnGroupName(firstQuery.getCnGroupName())
                .withCustomQueryId(firstQuery.getCustomQueryId())
                .withResourceGroupName(firstQuery.getResourceGroupName())
                .withResultSinkState(firstQuery.getResultSinkState());
        Assertions.assertEquals(firstQuery, otherQuery);
        Assertions.assertEquals(firstQuery.hashCode(), otherQuery.hashCode());
    }

    @Test
    public void testThrift() {
        TQueryStatisticsInfo firstQueryThrift = firstQuery.toThrift();
        QueryStatisticsInfo firstQueryTest = QueryStatisticsInfo.fromThrift(firstQueryThrift);
        Assertions.assertEquals(firstQuery, firstQueryTest);
    }

    @Test
    public void testCnGroupName() {
        // Test that cnGroupName is properly stored and retrieved
        QueryStatisticsInfo infoWithCnGroup = new QueryStatisticsInfo()
                .withQueryId("test-query-id")
                .withWareHouseName("test_warehouse")
                .withCnGroupName("analytics");
        Assertions.assertEquals("analytics", infoWithCnGroup.getCnGroupName());

        // Test thrift serialization includes cnGroupName
        TQueryStatisticsInfo thriftInfo = infoWithCnGroup.toThrift();
        Assertions.assertEquals("analytics", thriftInfo.getCnGroupName());

        // Test deserialization
        QueryStatisticsInfo fromThrift = QueryStatisticsInfo.fromThrift(thriftInfo);
        Assertions.assertEquals("analytics", fromThrift.getCnGroupName());

        // Test formatToList includes cnGroupName after warehouse
        java.util.List<String> formatted = infoWithCnGroup.formatToList();
        // Find warehouse index and verify cngroup is next
        int warehouseIdx = -1;
        for (int i = 0; i < formatted.size(); i++) {
            if ("test_warehouse".equals(formatted.get(i))) {
                warehouseIdx = i;
                break;
            }
        }
        Assertions.assertTrue(warehouseIdx >= 0, "Warehouse should be in formatted list");
        Assertions.assertEquals("analytics", formatted.get(warehouseIdx + 1),
                "CNGroup should be right after Warehouse");
    }
}
