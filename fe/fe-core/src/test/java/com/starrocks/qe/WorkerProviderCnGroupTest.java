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
        // Null group should return all nodes (no filtering)
        ImmutableMap<Long, ComputeNode> filtered = WorkerProviderHelper.filterByCnGroup(allNodes, null);

        Assertions.assertEquals(5, filtered.size());
        Assertions.assertSame(allNodes, filtered);
    }

    @Test
    public void testFilterByCnGroupEmpty() {
        // Empty group should return all nodes (no filtering)
        ImmutableMap<Long, ComputeNode> filtered = WorkerProviderHelper.filterByCnGroup(allNodes, "");

        Assertions.assertEquals(5, filtered.size());
        Assertions.assertSame(allNodes, filtered);
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
        // Create a node with null cnGroupName
        ComputeNode cnNull = new ComputeNode(6L, "host6", 9050);
        // cnGroupName is "default" by default in ComputeNode constructor

        ImmutableMap<Long, ComputeNode> nodesWithNull = ImmutableMap.<Long, ComputeNode>builder()
                .putAll(allNodes)
                .put(6L, cnNull)
                .build();

        ImmutableMap<Long, ComputeNode> filtered =
                WorkerProviderHelper.filterByCnGroup(nodesWithNull, "default");

        // Should include the node with default cnGroupName
        Assertions.assertTrue(filtered.containsKey(6L));
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
}
