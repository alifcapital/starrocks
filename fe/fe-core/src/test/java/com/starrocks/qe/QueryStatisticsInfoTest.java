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

import com.google.gson.JsonObject;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.ProfilingExecPlan;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.sql.ExplainAnalyzer;
import com.starrocks.thrift.TQueryStatisticsInfo;
import mockit.Mock;
import mockit.MockUp;
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
                .withResourceGroupName(firstQuery.getResourceGroupName());
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

    @Test
    public void testGetExecProgress() throws Exception {
        ProfileManager manager = ProfileManager.getInstance();
        manager.clearProfiles();

        RuntimeProfile profile = new RuntimeProfile("");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, "123");
        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Query");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, "Running");
        profile.addChild(summaryProfile);

        try {
            new MockUp<ExplainAnalyzer>() {
                @Mock
                public String getQueryProgress() throws StarRocksException {
                    JsonObject progressInfo = new JsonObject();
                    progressInfo.addProperty("total_operator_num", 5);
                    progressInfo.addProperty("finished_operator_num", 3);
                    progressInfo.addProperty("progress_percent", "60.00%");

                    JsonObject result = new JsonObject();
                    result.addProperty("query_id", "123");
                    result.addProperty("state", "Running");
                    result.add("progress_info", progressInfo);
                    return result.toString();
                }
            };

            manager.pushProfile(new ProfilingExecPlan(), profile);
            Assertions.assertEquals("60.00%", QueryStatisticsInfo.getExecProgress("123"));

            manager.clearProfiles();
            Assertions.assertEquals("", QueryStatisticsInfo.getExecProgress("123"));

            manager.pushProfile(null, profile);
            Assertions.assertEquals("", QueryStatisticsInfo.getExecProgress("123"));

            manager.clearProfiles();
            manager.pushProfile(new ProfilingExecPlan(), profile);
            new MockUp<ExplainAnalyzer>() {
                @Mock
                public String getQueryProgress() throws StarRocksException {
                    throw new StarRocksException("mock failure");
                }
            };
            Assertions.assertEquals("", QueryStatisticsInfo.getExecProgress("123"));
        } finally {
            manager.clearProfiles();
        }
    }
}
