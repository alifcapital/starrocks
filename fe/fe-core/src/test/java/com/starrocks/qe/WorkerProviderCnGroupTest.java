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

import com.google.common.collect.ImmutableMap;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.CnGroup;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WorkerProviderCnGroupTest {

    private ImmutableMap<Long, ComputeNode> allNodes;

    @BeforeEach
    public void setUp() {
        // Create test compute nodes with different groups
        ComputeNode cn1 = new ComputeNode(1L, "host1", 9050);
        cn1.setCnGroupName("default");

        ComputeNode cn2 = new ComputeNode(2L, "host2", 9050);
        cn2.setCnGroupName("default");

        ComputeNode cn3 = new ComputeNode(3L, "host3", 9050);
        cn3.setCnGroupName("etl");

        ComputeNode cn4 = new ComputeNode(4L, "host4", 9050);
        cn4.setCnGroupName("etl");

        ComputeNode cn5 = new ComputeNode(5L, "host5", 9050);
        cn5.setCnGroupName("analytics");

        allNodes = ImmutableMap.of(
                1L, cn1,
                2L, cn2,
                3L, cn3,
                4L, cn4,
                5L, cn5
        );
    }

    @Test
    public void testFilterByCnGroupDefault() {
        ImmutableMap<Long, ComputeNode> filtered = WorkerProviderHelper.filterByCnGroup(allNodes, "default");

        Assertions.assertEquals(2, filtered.size());
        Assertions.assertTrue(filtered.containsKey(1L));
        Assertions.assertTrue(filtered.containsKey(2L));
        Assertions.assertFalse(filtered.containsKey(3L));
        Assertions.assertFalse(filtered.containsKey(4L));
        Assertions.assertFalse(filtered.containsKey(5L));
    }

    @Test
    public void testFilterByCnGroupEtl() {
        ImmutableMap<Long, ComputeNode> filtered = WorkerProviderHelper.filterByCnGroup(allNodes, "etl");

        Assertions.assertEquals(2, filtered.size());
        Assertions.assertTrue(filtered.containsKey(3L));
        Assertions.assertTrue(filtered.containsKey(4L));
    }

    @Test
    public void testFilterByCnGroupAnalytics() {
        ImmutableMap<Long, ComputeNode> filtered = WorkerProviderHelper.filterByCnGroup(allNodes, "analytics");

        Assertions.assertEquals(1, filtered.size());
        Assertions.assertTrue(filtered.containsKey(5L));
    }

    @Test
    public void testFilterByCnGroupNull() {
        // Null group should return only "default" group nodes
        ImmutableMap<Long, ComputeNode> filtered = WorkerProviderHelper.filterByCnGroup(allNodes, null);

        Assertions.assertEquals(2, filtered.size());
        Assertions.assertTrue(filtered.containsKey(1L));
        Assertions.assertTrue(filtered.containsKey(2L));
    }

    @Test
    public void testFilterByCnGroupEmpty() {
        // Empty group should return only "default" group nodes
        ImmutableMap<Long, ComputeNode> filtered = WorkerProviderHelper.filterByCnGroup(allNodes, "");

        Assertions.assertEquals(2, filtered.size());
        Assertions.assertTrue(filtered.containsKey(1L));
        Assertions.assertTrue(filtered.containsKey(2L));
    }

    @Test
    public void testFilterByCnGroupNonExistent() {
        // Non-existent group should return empty map
        ImmutableMap<Long, ComputeNode> filtered = WorkerProviderHelper.filterByCnGroup(allNodes, "nonexistent");

        Assertions.assertEquals(0, filtered.size());
        Assertions.assertTrue(filtered.isEmpty());
    }

    @Test
    public void testFilterByCnGroupEmptyInput() {
        ImmutableMap<Long, ComputeNode> emptyNodes = ImmutableMap.of();
        ImmutableMap<Long, ComputeNode> filtered = WorkerProviderHelper.filterByCnGroup(emptyNodes, "etl");

        Assertions.assertTrue(filtered.isEmpty());
    }

    @Test
    public void testFilterByCnGroupPreservesNodeData() {
        ImmutableMap<Long, ComputeNode> filtered = WorkerProviderHelper.filterByCnGroup(allNodes, "etl");

        ComputeNode cn3 = filtered.get(3L);
        Assertions.assertNotNull(cn3);
        Assertions.assertEquals("host3", cn3.getHost());
        Assertions.assertEquals(9050, cn3.getHeartbeatPort());
        Assertions.assertEquals("etl", cn3.getCnGroupName());
    }

    @Test
    public void testFilterByCnGroupCaseSensitive() {
        // Group names should be case-sensitive
        ImmutableMap<Long, ComputeNode> filtered = WorkerProviderHelper.filterByCnGroup(allNodes, "ETL");

        Assertions.assertEquals(0, filtered.size());
    }

    @Test
    public void testFilterByCnGroupDefaultConstant() {
        // Test with the CnGroup.DEFAULT_GROUP_NAME constant
        ImmutableMap<Long, ComputeNode> filtered =
                WorkerProviderHelper.filterByCnGroup(allNodes, CnGroup.DEFAULT_GROUP_NAME);

        Assertions.assertEquals(2, filtered.size());
    }

    @Test
    public void testFilterWithNullCnGroupNameOnNode() {
        // Create a node with null/empty cnGroupName - should be treated as "default"
        ComputeNode cnNull = new ComputeNode(6L, "host6", 9050);
        cnNull.setCnGroupName(null);  // Explicitly set to null

        ComputeNode cnEmpty = new ComputeNode(7L, "host7", 9050);
        cnEmpty.setCnGroupName("");  // Explicitly set to empty

        ImmutableMap<Long, ComputeNode> nodesWithNull = ImmutableMap.<Long, ComputeNode>builder()
                .putAll(allNodes)
                .put(6L, cnNull)
                .put(7L, cnEmpty)
                .build();

        // Filter by "default" - should include nodes with null/empty cnGroupName
        ImmutableMap<Long, ComputeNode> filtered =
                WorkerProviderHelper.filterByCnGroup(nodesWithNull, "default");

        Assertions.assertTrue(filtered.containsKey(1L));  // explicit "default"
        Assertions.assertTrue(filtered.containsKey(2L));  // explicit "default"
        Assertions.assertTrue(filtered.containsKey(6L));  // null treated as "default"
        Assertions.assertTrue(filtered.containsKey(7L));  // empty treated as "default"
        Assertions.assertEquals(4, filtered.size());

        // Filter with null cnGroupName parameter - should also get "default" nodes
        ImmutableMap<Long, ComputeNode> filteredNull =
                WorkerProviderHelper.filterByCnGroup(nodesWithNull, null);

        Assertions.assertEquals(4, filteredNull.size());
        Assertions.assertTrue(filteredNull.containsKey(6L));
        Assertions.assertTrue(filteredNull.containsKey(7L));
    }

    @Test
    public void testFilterMultipleTimes() {
        // First filter to etl
        ImmutableMap<Long, ComputeNode> etlNodes = WorkerProviderHelper.filterByCnGroup(allNodes, "etl");
        Assertions.assertEquals(2, etlNodes.size());

        // Filter again (should still work, though result would be same or empty)
        ImmutableMap<Long, ComputeNode> filtered2 = WorkerProviderHelper.filterByCnGroup(etlNodes, "etl");
        Assertions.assertEquals(2, filtered2.size());

        // Filter with different group (should be empty since etlNodes only has etl)
        ImmutableMap<Long, ComputeNode> filtered3 = WorkerProviderHelper.filterByCnGroup(etlNodes, "default");
        Assertions.assertEquals(0, filtered3.size());
    }

    @Test
    public void testGetNextWorkerBasic() {
        // Test basic getNextWorker functionality
        ComputeNode worker = WorkerProviderHelper.getNextWorker(
                allNodes,
                (computeResource) -> 0,
                null
        );

        Assertions.assertNotNull(worker);
        Assertions.assertTrue(allNodes.containsValue(worker));
    }

    @Test
    public void testGetNextWorkerFromFilteredGroup() {
        ImmutableMap<Long, ComputeNode> etlNodes = WorkerProviderHelper.filterByCnGroup(allNodes, "etl");

        ComputeNode worker = WorkerProviderHelper.getNextWorker(
                etlNodes,
                (computeResource) -> 0,
                null
        );

        Assertions.assertNotNull(worker);
        Assertions.assertEquals("etl", worker.getCnGroupName());
    }

    @Test
    public void testGetNextWorkerFromEmptyMap() {
        ImmutableMap<Long, ComputeNode> emptyNodes = ImmutableMap.of();

        ComputeNode worker = WorkerProviderHelper.getNextWorker(
                emptyNodes,
                (computeResource) -> 0,
                null
        );

        Assertions.assertNull(worker);
    }

    @Test
    public void testGetNextWorkerRoundRobin() {
        ImmutableMap<Long, ComputeNode> etlNodes = WorkerProviderHelper.filterByCnGroup(allNodes, "etl");

        // Get workers with increasing index
        ComputeNode worker0 = WorkerProviderHelper.getNextWorker(etlNodes, (cr) -> 0, null);
        ComputeNode worker1 = WorkerProviderHelper.getNextWorker(etlNodes, (cr) -> 1, null);

        // Both should be from etl group
        Assertions.assertEquals("etl", worker0.getCnGroupName());
        Assertions.assertEquals("etl", worker1.getCnGroupName());

        // Should be different nodes (since there are 2 etl nodes)
        Assertions.assertNotEquals(worker0.getId(), worker1.getId());
    }

    @Test
    public void testGetNextWorkerNegativeIndex() {
        ImmutableMap<Long, ComputeNode> etlNodes = WorkerProviderHelper.filterByCnGroup(allNodes, "etl");

        // Negative index should still work (handled by abs)
        ComputeNode worker = WorkerProviderHelper.getNextWorker(
                etlNodes,
                (computeResource) -> -5,
                null
        );

        Assertions.assertNotNull(worker);
        Assertions.assertEquals("etl", worker.getCnGroupName());
    }

    @Test
    public void testSystemTaskUsesDefaultGroupViaFilterByCnGroup() {
        // Note: System tasks now use WarehouseManager.DEFAULT_RESOURCE which has null cnGroupName.
        // When null is passed to filterByCnGroup, it uses "default" as the effective group.
        // However, the proper architecture now filters at WarehouseComputeResourceProvider level.
        // This test validates the WorkerProviderHelper.filterByCnGroup behavior for SHARED_NOTHING mode.

        ImmutableMap<Long, ComputeNode> systemTaskNodes =
                WorkerProviderHelper.filterByCnGroup(allNodes, null);

        // filterByCnGroup with null uses "default" as effective group (for backward compatibility)
        Assertions.assertEquals(2, systemTaskNodes.size());
        Assertions.assertTrue(systemTaskNodes.containsKey(1L));
        Assertions.assertTrue(systemTaskNodes.containsKey(2L));

        // User session with explicit cngroup should get only that group
        ImmutableMap<Long, ComputeNode> userEtlNodes =
                WorkerProviderHelper.filterByCnGroup(allNodes, "etl");
        Assertions.assertEquals(2, userEtlNodes.size());
        Assertions.assertTrue(userEtlNodes.containsKey(3L));
        Assertions.assertTrue(userEtlNodes.containsKey(4L));
    }

    @Test
    public void testSystemTasksUseDefaultGroupViaComputeResourceProvider() {
        // In SHARED_DATA mode, system tasks use WarehouseManager.DEFAULT_RESOURCE.
        // DEFAULT_RESOURCE is WarehouseComputeResource which returns "default" for getCnGroupName().
        // This ensures system tasks run on "default" group nodes, leaving other CNGroups
        // (etl, analytics, etc.) for dedicated workloads.

        // Verify that WarehouseComputeResource.getCnGroupName() returns "default"
        ComputeResource defaultResource = WarehouseComputeResource.of(0L);
        Assertions.assertEquals(CnGroup.DEFAULT_GROUP_NAME, defaultResource.getCnGroupName(),
                "WarehouseComputeResource should return 'default' for getCnGroupName");
    }

    @Test
    public void testUserSessionWithCnGroupRouting() {
        // User session with SET cngroup='etl' creates LazyComputeResource with cnGroupName="etl"
        // LazyComputeResource.getCnGroupName() returns "etl"
        // WarehouseComputeResourceProvider filters nodes to only those in "etl" group

        // Test with explicit group name
        ImmutableMap<Long, ComputeNode> etlNodes =
                WorkerProviderHelper.filterByCnGroup(allNodes, "etl");

        Assertions.assertEquals(2, etlNodes.size());
        Assertions.assertTrue(etlNodes.containsKey(3L));
        Assertions.assertTrue(etlNodes.containsKey(4L));
        Assertions.assertFalse(etlNodes.containsKey(1L)); // "default" node excluded
        Assertions.assertFalse(etlNodes.containsKey(5L)); // "analytics" node excluded
    }

    @Test
    public void testMixedNodesWithImplicitDefault() {
        // Create mix: some explicit "default", some null (implicit default), some "etl"
        ComputeNode cn1 = new ComputeNode(1L, "host1", 9050);
        cn1.setCnGroupName("default");  // explicit default

        ComputeNode cn2 = new ComputeNode(2L, "host2", 9050);
        cn2.setCnGroupName(null);  // implicit default (null)

        ComputeNode cn3 = new ComputeNode(3L, "host3", 9050);
        cn3.setCnGroupName("");  // implicit default (empty)

        ComputeNode cn4 = new ComputeNode(4L, "host4", 9050);
        cn4.setCnGroupName("etl");

        ImmutableMap<Long, ComputeNode> mixedNodes = ImmutableMap.of(
                1L, cn1, 2L, cn2, 3L, cn3, 4L, cn4);

        // System task should get all "default" nodes (explicit and implicit)
        ImmutableMap<Long, ComputeNode> defaultNodes =
                WorkerProviderHelper.filterByCnGroup(mixedNodes, null);

        Assertions.assertEquals(3, defaultNodes.size());
        Assertions.assertTrue(defaultNodes.containsKey(1L));  // explicit "default"
        Assertions.assertTrue(defaultNodes.containsKey(2L));  // null -> "default"
        Assertions.assertTrue(defaultNodes.containsKey(3L));  // "" -> "default"
        Assertions.assertFalse(defaultNodes.containsKey(4L)); // "etl" - not included

        // User with SET cngroup='etl' should get only etl node
        ImmutableMap<Long, ComputeNode> etlNodes =
                WorkerProviderHelper.filterByCnGroup(mixedNodes, "etl");

        Assertions.assertEquals(1, etlNodes.size());
        Assertions.assertTrue(etlNodes.containsKey(4L));
    }
}
