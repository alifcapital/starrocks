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

package com.starrocks.warehouse.cngroup;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.WorkerProviderHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.StarRocksTestBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_ID;

public class CnGroupIntegrationTest extends StarRocksTestBase {

    private static StarRocksAssert starRocksAssert;
    private static CnGroupMgr cnGroupMgr;
    private static SystemInfoService systemInfoService;

    @BeforeAll
    public static void beforeAll() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();

        new MockUp<WarehouseComputeResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                if (computeResource != null && computeResource.getWarehouseId() == DEFAULT_WAREHOUSE_ID) {
                    return true;
                }
                return false;
            }
        };

        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(
                UserIdentity.ROOT));

        cnGroupMgr = GlobalStateMgr.getCurrentState().getCnGroupMgr();
        systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
    }

    @AfterAll
    public static void afterAll() {
        // Cleanup
    }

    // ==================== Session Variable Tests ====================

    @Test
    public void testSessionVariableCnGroup() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        SessionVariable sessionVariable = ctx.getSessionVariable();

        // Default value should be "default"
        Assertions.assertEquals(CnGroup.DEFAULT_GROUP_NAME, sessionVariable.getCnGroupName());

        // Set to a different group
        sessionVariable.setCnGroupName("etl");
        Assertions.assertEquals("etl", sessionVariable.getCnGroupName());

        // Set back to default
        sessionVariable.setCnGroupName("default");
        Assertions.assertEquals("default", sessionVariable.getCnGroupName());
    }

    @Test
    public void testSetCnGroupDirectly() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        SessionVariable sessionVariable = ctx.getSessionVariable();

        // Set cngroup directly
        sessionVariable.setCnGroupName("analytics");
        Assertions.assertEquals("analytics", sessionVariable.getCnGroupName());

        // Reset
        sessionVariable.setCnGroupName("default");
        Assertions.assertEquals("default", sessionVariable.getCnGroupName());
    }

    // ==================== CnGroup Management Tests ====================

    @Test
    public void testCreateAndDropGroup() throws Exception {
        // Create group
        cnGroupMgr.createGroup("test_group", "Test group for integration test");
        Assertions.assertTrue(cnGroupMgr.groupExists("test_group"));

        // Drop group
        cnGroupMgr.dropGroup("test_group", false);
        Assertions.assertFalse(cnGroupMgr.groupExists("test_group"));
    }

    @Test
    public void testAddNodeToGroup() throws Exception {
        // Create a group
        cnGroupMgr.createGroup("node_test_group", "");

        // Create and add a compute node
        ComputeNode cn = new ComputeNode(9001L, "test-host-1", 9050);
        cn.setCnGroupName("default");
        systemInfoService.addComputeNode(cn);

        // Add node to group
        cnGroupMgr.addNodeToGroup(9001L, "node_test_group");

        // Verify
        Assertions.assertEquals("node_test_group", cnGroupMgr.getNodeGroup(9001L));
        Assertions.assertTrue(cnGroupMgr.getNodesInGroup("node_test_group").contains(9001L));
        Assertions.assertEquals("node_test_group", cn.getCnGroupName());

        // Cleanup
        systemInfoService.dropComputeNode(cn);
        cnGroupMgr.dropGroup("node_test_group", true);
    }

    // ==================== SHOW CNGROUPS Tests ====================

    @Test
    public void testShowCnGroups() throws Exception {
        // Create some groups for testing
        cnGroupMgr.createGroup("show_test_1", "First group");
        cnGroupMgr.createGroup("show_test_2", "Second group");

        // Get groups info
        List<List<String>> groupsInfo = cnGroupMgr.getGroupsInfo();

        // Should contain at least default + our 2 groups
        Assertions.assertTrue(groupsInfo.size() >= 3);

        // Find our test groups
        boolean foundGroup1 = false;
        boolean foundGroup2 = false;
        for (List<String> row : groupsInfo) {
            if ("show_test_1".equals(row.get(0))) {
                foundGroup1 = true;
                Assertions.assertEquals("First group", row.get(3));
            }
            if ("show_test_2".equals(row.get(0))) {
                foundGroup2 = true;
                Assertions.assertEquals("Second group", row.get(3));
            }
        }
        Assertions.assertTrue(foundGroup1);
        Assertions.assertTrue(foundGroup2);

        // Cleanup
        cnGroupMgr.dropGroup("show_test_1", false);
        cnGroupMgr.dropGroup("show_test_2", false);
    }

    // ==================== Worker Provider Filtering Tests ====================

    @Test
    public void testWorkerProviderFiltering() throws Exception {
        // Create groups
        cnGroupMgr.createGroup("filter_etl", "");
        cnGroupMgr.createGroup("filter_analytics", "");

        // Create compute nodes with different groups
        ComputeNode cn1 = new ComputeNode(8001L, "filter-host-1", 9050);
        cn1.setCnGroupName("filter_etl");
        cn1.setAlive(true);

        ComputeNode cn2 = new ComputeNode(8002L, "filter-host-2", 9050);
        cn2.setCnGroupName("filter_etl");
        cn2.setAlive(true);

        ComputeNode cn3 = new ComputeNode(8003L, "filter-host-3", 9050);
        cn3.setCnGroupName("filter_analytics");
        cn3.setAlive(true);

        ImmutableMap<Long, ComputeNode> allNodes = ImmutableMap.of(
                8001L, cn1,
                8002L, cn2,
                8003L, cn3
        );

        // Test filtering
        ImmutableMap<Long, ComputeNode> etlNodes =
                WorkerProviderHelper.filterByCnGroup(allNodes, "filter_etl");
        Assertions.assertEquals(2, etlNodes.size());
        Assertions.assertTrue(etlNodes.containsKey(8001L));
        Assertions.assertTrue(etlNodes.containsKey(8002L));

        ImmutableMap<Long, ComputeNode> analyticsNodes =
                WorkerProviderHelper.filterByCnGroup(allNodes, "filter_analytics");
        Assertions.assertEquals(1, analyticsNodes.size());
        Assertions.assertTrue(analyticsNodes.containsKey(8003L));

        // Cleanup
        cnGroupMgr.dropGroup("filter_etl", true);
        cnGroupMgr.dropGroup("filter_analytics", true);
    }

    // ==================== Node Lifecycle Tests ====================

    @Test
    public void testNodeDroppedFromGroup() throws Exception {
        // Create group
        cnGroupMgr.createGroup("lifecycle_group", "");

        // Add node
        ComputeNode cn = new ComputeNode(7001L, "lifecycle-host", 9050);
        systemInfoService.addComputeNode(cn);
        cnGroupMgr.addNodeToGroup(7001L, "lifecycle_group");

        Assertions.assertTrue(cnGroupMgr.getNodesInGroup("lifecycle_group").contains(7001L));

        // Handle node dropped
        cnGroupMgr.handleNodeDropped(7001L);

        // Node should be removed from group
        Assertions.assertFalse(cnGroupMgr.getNodesInGroup("lifecycle_group").contains(7001L));

        // Cleanup
        systemInfoService.dropComputeNode(cn);
        cnGroupMgr.dropGroup("lifecycle_group", false);
    }

    @Test
    public void testMoveNodeBetweenGroups() throws Exception {
        // Create groups
        cnGroupMgr.createGroup("move_from", "");
        cnGroupMgr.createGroup("move_to", "");

        // Add node to first group
        ComputeNode cn = new ComputeNode(6001L, "move-host", 9050);
        systemInfoService.addComputeNode(cn);
        cnGroupMgr.addNodeToGroup(6001L, "move_from");

        Assertions.assertEquals("move_from", cnGroupMgr.getNodeGroup(6001L));

        // Move to second group
        cnGroupMgr.addNodeToGroup(6001L, "move_to");

        Assertions.assertEquals("move_to", cnGroupMgr.getNodeGroup(6001L));
        Assertions.assertFalse(cnGroupMgr.getNodesInGroup("move_from").contains(6001L));
        Assertions.assertTrue(cnGroupMgr.getNodesInGroup("move_to").contains(6001L));

        // Cleanup
        systemInfoService.dropComputeNode(cn);
        cnGroupMgr.dropGroup("move_from", false);
        cnGroupMgr.dropGroup("move_to", false);
    }

    // ==================== Error Handling Tests ====================

    @Test
    public void testCannotDropDefaultGroup() {
        Assertions.assertThrows(DdlException.class, () -> {
            cnGroupMgr.dropGroup(CnGroup.DEFAULT_GROUP_NAME, false);
        });
    }

    @Test
    public void testCannotDropGroupWithNodes() throws Exception {
        cnGroupMgr.createGroup("non_empty_group", "");

        ComputeNode cn = new ComputeNode(5001L, "non-empty-host", 9050);
        systemInfoService.addComputeNode(cn);
        cnGroupMgr.addNodeToGroup(5001L, "non_empty_group");

        Assertions.assertThrows(DdlException.class, () -> {
            cnGroupMgr.dropGroup("non_empty_group", false);
        });

        // Cleanup - remove node first, then drop group
        cnGroupMgr.removeNodeFromGroup(5001L);
        systemInfoService.dropComputeNode(cn);
        cnGroupMgr.dropGroup("non_empty_group", false);
    }

    @Test
    public void testDropNonExistentGroupIfExists() throws Exception {
        // Should not throw with ifExists=true
        cnGroupMgr.dropGroup("nonexistent_group_12345", true);
    }

    @Test
    public void testDropNonExistentGroupThrows() {
        Assertions.assertThrows(DdlException.class, () -> {
            cnGroupMgr.dropGroup("nonexistent_group_12345", false);
        });
    }

    // ==================== Concurrent Operations Tests ====================

    @Test
    public void testConcurrentGroupOperations() throws Exception {
        int numThreads = 5;
        int numGroups = 10;

        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                for (int i = 0; i < numGroups; i++) {
                    String groupName = "concurrent_" + threadId + "_" + i;
                    try {
                        cnGroupMgr.createGroup(groupName, "");
                        cnGroupMgr.dropGroup(groupName, true);
                    } catch (DdlException e) {
                        // Ignore - concurrent creates might fail
                    }
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // Cleanup any remaining groups
        for (int t = 0; t < numThreads; t++) {
            for (int i = 0; i < numGroups; i++) {
                try {
                    cnGroupMgr.dropGroup("concurrent_" + t + "_" + i, true);
                } catch (Exception e) {
                    // Ignore
                }
            }
        }
    }
}
