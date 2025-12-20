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

import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CnGroupTest {

    private CnGroup group;

    @BeforeEach
    public void setUp() {
        group = new CnGroup(1L, "test_group", "Test comment");
    }

    @Test
    public void testCreateGroup() {
        Assertions.assertEquals(1L, group.getId());
        Assertions.assertEquals("test_group", group.getName());
        Assertions.assertEquals("Test comment", group.getComment());
        Assertions.assertTrue(group.getCreateTime() > 0);
        Assertions.assertEquals(0, group.getNodeCount());
    }

    @Test
    public void testCreateGroupWithoutComment() {
        CnGroup g = new CnGroup(2L, "no_comment");
        Assertions.assertEquals("", g.getComment());
    }

    @Test
    public void testAddNode() {
        Assertions.assertTrue(group.addNode(100L));
        Assertions.assertTrue(group.containsNode(100L));
        Assertions.assertEquals(1, group.getNodeCount());

        // Adding same node again should return false
        Assertions.assertFalse(group.addNode(100L));
        Assertions.assertEquals(1, group.getNodeCount());
    }

    @Test
    public void testRemoveNode() {
        group.addNode(100L);
        group.addNode(200L);
        Assertions.assertEquals(2, group.getNodeCount());

        Assertions.assertTrue(group.removeNode(100L));
        Assertions.assertFalse(group.containsNode(100L));
        Assertions.assertEquals(1, group.getNodeCount());

        // Removing non-existent node should return false
        Assertions.assertFalse(group.removeNode(999L));
    }

    @Test
    public void testContainsNode() {
        Assertions.assertFalse(group.containsNode(100L));
        group.addNode(100L);
        Assertions.assertTrue(group.containsNode(100L));
    }

    @Test
    public void testGetNodeIds() {
        group.addNode(100L);
        group.addNode(200L);
        group.addNode(300L);

        Set<Long> nodeIds = group.getNodeIds();
        Assertions.assertEquals(3, nodeIds.size());
        Assertions.assertTrue(nodeIds.contains(100L));
        Assertions.assertTrue(nodeIds.contains(200L));
        Assertions.assertTrue(nodeIds.contains(300L));
    }

    @Test
    public void testGetNodeIdsIsUnmodifiable() {
        group.addNode(100L);
        Set<Long> nodeIds = group.getNodeIds();

        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            nodeIds.add(999L);
        });
    }

    @Test
    public void testGetNodeCount() {
        Assertions.assertEquals(0, group.getNodeCount());
        group.addNode(100L);
        Assertions.assertEquals(1, group.getNodeCount());
        group.addNode(200L);
        Assertions.assertEquals(2, group.getNodeCount());
        group.removeNode(100L);
        Assertions.assertEquals(1, group.getNodeCount());
    }

    @Test
    public void testIsDefaultGroup() {
        Assertions.assertFalse(group.isDefaultGroup());

        CnGroup defaultGroup = new CnGroup(0L, CnGroup.DEFAULT_GROUP_NAME);
        Assertions.assertTrue(defaultGroup.isDefaultGroup());
    }

    @Test
    public void testGetCreateTimeStr() {
        String timeStr = group.getCreateTimeStr();
        Assertions.assertNotNull(timeStr);
        // Format should be "yyyy-MM-dd HH:mm:ss"
        Assertions.assertTrue(timeStr.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"));
    }

    @Test
    public void testSetComment() {
        group.setComment("New comment");
        Assertions.assertEquals("New comment", group.getComment());
    }

    @Test
    public void testEquals() {
        CnGroup group1 = new CnGroup(1L, "test");
        CnGroup group2 = new CnGroup(1L, "test");
        CnGroup group3 = new CnGroup(2L, "test");
        CnGroup group4 = new CnGroup(1L, "other");

        Assertions.assertEquals(group1, group2);
        Assertions.assertNotEquals(group1, group3);
        Assertions.assertNotEquals(group1, group4);
        Assertions.assertNotEquals(group1, null);
        Assertions.assertNotEquals(group1, "string");
    }

    @Test
    public void testHashCode() {
        CnGroup group1 = new CnGroup(1L, "test");
        CnGroup group2 = new CnGroup(1L, "test");

        Assertions.assertEquals(group1.hashCode(), group2.hashCode());
    }

    @Test
    public void testToString() {
        group.addNode(100L);
        String str = group.toString();

        Assertions.assertTrue(str.contains("test_group"));
        Assertions.assertTrue(str.contains("nodeCount=1"));
    }

    @Test
    public void testSerializationDeserialization() throws Exception {
        group.addNode(100L);
        group.addNode(200L);

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        group.write(dos);

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        CnGroup restored = CnGroup.read(dis);

        Assertions.assertEquals(group.getId(), restored.getId());
        Assertions.assertEquals(group.getName(), restored.getName());
        Assertions.assertEquals(group.getComment(), restored.getComment());
        Assertions.assertEquals(group.getNodeIds(), restored.getNodeIds());
    }

    @Test
    public void testGsonSerialization() {
        group.addNode(100L);
        group.addNode(200L);

        String json = GsonUtils.GSON.toJson(group);
        CnGroup restored = GsonUtils.GSON.fromJson(json, CnGroup.class);

        Assertions.assertEquals(group.getId(), restored.getId());
        Assertions.assertEquals(group.getName(), restored.getName());
        Assertions.assertEquals(group.getComment(), restored.getComment());
        // After GSON deserialization, gsonPostProcess should be called
        restored.gsonPostProcess();
        Assertions.assertEquals(group.getNodeIds(), restored.getNodeIds());
    }

    @Test
    public void testLockAfterDeserialization() {
        group.addNode(100L);

        // Serialize and deserialize via GSON
        String json = GsonUtils.GSON.toJson(group);
        CnGroup restored = GsonUtils.GSON.fromJson(json, CnGroup.class);
        restored.gsonPostProcess();

        // Lock should work after deserialization
        Assertions.assertTrue(restored.addNode(200L));
        Assertions.assertTrue(restored.containsNode(100L));
        Assertions.assertTrue(restored.containsNode(200L));
        Assertions.assertEquals(2, restored.getNodeCount());
    }

    @Test
    public void testConcurrentAccess() throws Exception {
        int numThreads = 10;
        int operationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        long nodeId = threadId * 1000L + i;
                        group.addNode(nodeId);
                        group.containsNode(nodeId);
                        group.getNodeCount();
                        group.getNodeIds();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // All nodes should be added
        Assertions.assertEquals(numThreads * operationsPerThread, group.getNodeCount());
    }

    @Test
    public void testDefaultGroupName() {
        Assertions.assertEquals("default", CnGroup.DEFAULT_GROUP_NAME);
    }
}
