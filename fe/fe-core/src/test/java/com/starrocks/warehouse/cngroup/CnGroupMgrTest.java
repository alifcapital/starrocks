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
import com.starrocks.common.DdlException;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class CnGroupMgrTest {

    private CnGroupMgr cnGroupMgr;

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private EditLog editLog;

    @Mocked
    private NodeMgr nodeMgr;

    @Mocked
    private SystemInfoService systemInfoService;

    private AtomicLong nextId = new AtomicLong(1000);
    private Map<Long, ComputeNode> computeNodes = new HashMap<>();

    @BeforeEach
    public void setUp() {
        cnGroupMgr = new CnGroupMgr();
        computeNodes.clear();
        nextId.set(1000);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }
        };

        new Expectations() {
            {
                globalStateMgr.getNextId();
                result = new mockit.Delegate<Long>() {
                    long getNextId() {
                        return nextId.getAndIncrement();
                    }
                };
                minTimes = 0;

                globalStateMgr.getEditLog();
                result = editLog;
                minTimes = 0;

                globalStateMgr.getNodeMgr();
                result = nodeMgr;
                minTimes = 0;

                nodeMgr.getClusterInfo();
                result = systemInfoService;
                minTimes = 0;

                systemInfoService.getBackendOrComputeNode(anyLong);
                result = new mockit.Delegate<ComputeNode>() {
                    ComputeNode getBackendOrComputeNode(long nodeId) {
                        return computeNodes.get(nodeId);
                    }
                };
                minTimes = 0;

                systemInfoService.getIdComputeNode();
                result = new mockit.Delegate<ImmutableMap<Long, ComputeNode>>() {
                    ImmutableMap<Long, ComputeNode> getIdComputeNode() {
                        return ImmutableMap.copyOf(computeNodes);
                    }
                };
                minTimes = 0;

                systemInfoService.getIdToBackend();
                result = ImmutableMap.of();
                minTimes = 0;
            }
        };
    }

    @Test
    public void testDefaultGroupExists() {
        Assertions.assertTrue(cnGroupMgr.groupExists(CnGroup.DEFAULT_GROUP_NAME));
        CnGroup defaultGroup = cnGroupMgr.getGroup(CnGroup.DEFAULT_GROUP_NAME);
        Assertions.assertNotNull(defaultGroup);
        Assertions.assertEquals(0L, defaultGroup.getId());
    }

    @Test
    public void testCreateGroup() throws DdlException {
        CnGroup group = cnGroupMgr.createGroup("etl", "ETL workload");

        Assertions.assertNotNull(group);
        Assertions.assertEquals("etl", group.getName());
        Assertions.assertEquals("ETL workload", group.getComment());
        Assertions.assertTrue(cnGroupMgr.groupExists("etl"));
    }

    @Test
    public void testCreateDuplicateGroup() throws DdlException {
        cnGroupMgr.createGroup("etl", "");

        Assertions.assertThrows(DdlException.class, () -> {
            cnGroupMgr.createGroup("etl", "");
        });
    }

    @Test
    public void testDropGroup() throws DdlException {
        cnGroupMgr.createGroup("etl", "");
        Assertions.assertTrue(cnGroupMgr.groupExists("etl"));

        cnGroupMgr.dropGroup("etl", false);
        Assertions.assertFalse(cnGroupMgr.groupExists("etl"));
    }

    @Test
    public void testDropNonExistentGroup() {
        Assertions.assertThrows(DdlException.class, () -> {
            cnGroupMgr.dropGroup("nonexistent", false);
        });
    }

    @Test
    public void testDropNonExistentGroupIfExists() throws DdlException {
        // Should not throw
        cnGroupMgr.dropGroup("nonexistent", true);
    }

    @Test
    public void testDropDefaultGroup() {
        Assertions.assertThrows(DdlException.class, () -> {
            cnGroupMgr.dropGroup(CnGroup.DEFAULT_GROUP_NAME, false);
        });
    }

    @Test
    public void testDropGroupWithNodes() throws DdlException {
        cnGroupMgr.createGroup("etl", "");

        // Add a node to the group
        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        computeNodes.put(100L, cn);
        cnGroupMgr.addNodeToGroup(100L, "etl");

        Assertions.assertThrows(DdlException.class, () -> {
            cnGroupMgr.dropGroup("etl", false);
        });
    }

    @Test
    public void testAddNodeToGroup() throws DdlException {
        cnGroupMgr.createGroup("etl", "");

        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        computeNodes.put(100L, cn);

        cnGroupMgr.addNodeToGroup(100L, "etl");

        Assertions.assertEquals("etl", cnGroupMgr.getNodeGroup(100L));
        Assertions.assertTrue(cnGroupMgr.getNodesInGroup("etl").contains(100L));
        Assertions.assertEquals("etl", cn.getCnGroupName());
    }

    @Test
    public void testAddNodeToNonExistentGroup() throws DdlException {
        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        computeNodes.put(100L, cn);

        // Group should be auto-created
        cnGroupMgr.addNodeToGroup(100L, "new_group");

        Assertions.assertTrue(cnGroupMgr.groupExists("new_group"));
        Assertions.assertEquals("new_group", cnGroupMgr.getNodeGroup(100L));
    }

    @Test
    public void testMoveNodeBetweenGroups() throws DdlException {
        cnGroupMgr.createGroup("etl", "");
        cnGroupMgr.createGroup("analytics", "");

        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        computeNodes.put(100L, cn);

        cnGroupMgr.addNodeToGroup(100L, "etl");
        Assertions.assertEquals("etl", cnGroupMgr.getNodeGroup(100L));
        Assertions.assertTrue(cnGroupMgr.getNodesInGroup("etl").contains(100L));

        cnGroupMgr.addNodeToGroup(100L, "analytics");
        Assertions.assertEquals("analytics", cnGroupMgr.getNodeGroup(100L));
        Assertions.assertTrue(cnGroupMgr.getNodesInGroup("analytics").contains(100L));
        Assertions.assertFalse(cnGroupMgr.getNodesInGroup("etl").contains(100L));
    }

    @Test
    public void testRemoveNodeFromGroup() throws DdlException {
        cnGroupMgr.createGroup("etl", "");

        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        computeNodes.put(100L, cn);

        cnGroupMgr.addNodeToGroup(100L, "etl");
        cnGroupMgr.removeNodeFromGroup(100L);

        // Node should be moved to default group
        Assertions.assertEquals(CnGroup.DEFAULT_GROUP_NAME, cnGroupMgr.getNodeGroup(100L));
        Assertions.assertFalse(cnGroupMgr.getNodesInGroup("etl").contains(100L));
        Assertions.assertTrue(cnGroupMgr.getNodesInGroup(CnGroup.DEFAULT_GROUP_NAME).contains(100L));
    }

    @Test
    public void testRemoveNodeFromDefaultGroup() throws DdlException {
        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        computeNodes.put(100L, cn);

        cnGroupMgr.addNodeToGroup(100L, CnGroup.DEFAULT_GROUP_NAME);

        // Should be a no-op
        cnGroupMgr.removeNodeFromGroup(100L);
        Assertions.assertEquals(CnGroup.DEFAULT_GROUP_NAME, cnGroupMgr.getNodeGroup(100L));
    }

    @Test
    public void testHandleNodeAdded() {
        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        cn.setCnGroupName("default");
        computeNodes.put(100L, cn);

        cnGroupMgr.handleNodeAdded(100L, "default");

        Assertions.assertEquals(CnGroup.DEFAULT_GROUP_NAME, cnGroupMgr.getNodeGroup(100L));
        Assertions.assertTrue(cnGroupMgr.getNodesInGroup(CnGroup.DEFAULT_GROUP_NAME).contains(100L));
    }

    @Test
    public void testHandleNodeAddedWithNullGroup() {
        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        computeNodes.put(100L, cn);

        cnGroupMgr.handleNodeAdded(100L, null);

        // Should go to default group
        Assertions.assertEquals(CnGroup.DEFAULT_GROUP_NAME, cnGroupMgr.getNodeGroup(100L));
    }

    @Test
    public void testHandleNodeDropped() throws DdlException {
        cnGroupMgr.createGroup("etl", "");

        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        computeNodes.put(100L, cn);

        cnGroupMgr.addNodeToGroup(100L, "etl");
        Assertions.assertTrue(cnGroupMgr.getNodesInGroup("etl").contains(100L));

        cnGroupMgr.handleNodeDropped(100L);
        Assertions.assertFalse(cnGroupMgr.getNodesInGroup("etl").contains(100L));
    }

    @Test
    public void testReplayCreateGroup() {
        CnGroup group = new CnGroup(999L, "replayed_group", "Replayed");
        group.addNode(100L);
        group.addNode(200L);

        cnGroupMgr.replayCreateGroup(group);

        Assertions.assertTrue(cnGroupMgr.groupExists("replayed_group"));
        Assertions.assertEquals("replayed_group", cnGroupMgr.getNodeGroup(100L));
        Assertions.assertEquals("replayed_group", cnGroupMgr.getNodeGroup(200L));
    }

    @Test
    public void testReplayDropGroup() throws DdlException {
        cnGroupMgr.createGroup("etl", "");
        CnGroup group = cnGroupMgr.getGroup("etl");

        cnGroupMgr.replayDropGroup(group);
        Assertions.assertFalse(cnGroupMgr.groupExists("etl"));
    }

    @Test
    public void testReplayAddNodeToGroup() throws DdlException {
        cnGroupMgr.createGroup("etl", "");

        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        computeNodes.put(100L, cn);

        CnGroupMgr.CnGroupNodeOp op = new CnGroupMgr.CnGroupNodeOp(100L, "etl");
        cnGroupMgr.replayAddNodeToGroup(op);

        Assertions.assertEquals("etl", cnGroupMgr.getNodeGroup(100L));
        Assertions.assertTrue(cnGroupMgr.getNodesInGroup("etl").contains(100L));
    }

    @Test
    public void testReplayRemoveNodeFromGroup() throws DdlException {
        cnGroupMgr.createGroup("etl", "");

        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        computeNodes.put(100L, cn);

        cnGroupMgr.addNodeToGroup(100L, "etl");

        CnGroupMgr.CnGroupNodeOp op = new CnGroupMgr.CnGroupNodeOp(100L, "etl");
        cnGroupMgr.replayRemoveNodeFromGroup(op);

        Assertions.assertEquals(CnGroup.DEFAULT_GROUP_NAME, cnGroupMgr.getNodeGroup(100L));
    }

    @Test
    public void testGetAllGroups() throws DdlException {
        cnGroupMgr.createGroup("etl", "");
        cnGroupMgr.createGroup("analytics", "");

        Collection<CnGroup> groups = cnGroupMgr.getAllGroups();
        Assertions.assertEquals(3, groups.size()); // default + etl + analytics
    }

    @Test
    public void testGetAllGroupNames() throws DdlException {
        cnGroupMgr.createGroup("etl", "");
        cnGroupMgr.createGroup("analytics", "");

        List<String> names = cnGroupMgr.getAllGroupNames();
        Assertions.assertEquals(3, names.size());
        Assertions.assertTrue(names.contains(CnGroup.DEFAULT_GROUP_NAME));
        Assertions.assertTrue(names.contains("etl"));
        Assertions.assertTrue(names.contains("analytics"));
    }

    @Test
    public void testGetNodesInGroup() throws DdlException {
        cnGroupMgr.createGroup("etl", "");

        ComputeNode cn1 = new ComputeNode(100L, "host1", 9050);
        ComputeNode cn2 = new ComputeNode(200L, "host2", 9050);
        computeNodes.put(100L, cn1);
        computeNodes.put(200L, cn2);

        cnGroupMgr.addNodeToGroup(100L, "etl");
        cnGroupMgr.addNodeToGroup(200L, "etl");

        Set<Long> nodes = cnGroupMgr.getNodesInGroup("etl");
        Assertions.assertEquals(2, nodes.size());
        Assertions.assertTrue(nodes.contains(100L));
        Assertions.assertTrue(nodes.contains(200L));
    }

    @Test
    public void testGetNodesInNonExistentGroup() {
        Set<Long> nodes = cnGroupMgr.getNodesInGroup("nonexistent");
        Assertions.assertTrue(nodes.isEmpty());
    }

    @Test
    public void testGetGroupsInfo() throws DdlException {
        cnGroupMgr.createGroup("etl", "ETL workload");

        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        computeNodes.put(100L, cn);
        cnGroupMgr.addNodeToGroup(100L, "etl");

        List<List<String>> info = cnGroupMgr.getGroupsInfo();
        Assertions.assertTrue(info.size() >= 2); // default + etl

        // Find etl group info
        boolean found = false;
        for (List<String> row : info) {
            if ("etl".equals(row.get(0))) {
                found = true;
                Assertions.assertEquals("1", row.get(1)); // node count
                Assertions.assertEquals("ETL workload", row.get(3)); // comment
                break;
            }
        }
        Assertions.assertTrue(found);
    }

    @Test
    public void testSerializationDeserialization() throws Exception {
        cnGroupMgr.createGroup("etl", "ETL");
        cnGroupMgr.createGroup("analytics", "Analytics");

        ComputeNode cn1 = new ComputeNode(100L, "host1", 9050);
        ComputeNode cn2 = new ComputeNode(200L, "host2", 9050);
        computeNodes.put(100L, cn1);
        computeNodes.put(200L, cn2);

        cnGroupMgr.addNodeToGroup(100L, "etl");
        cnGroupMgr.addNodeToGroup(200L, "analytics");

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        cnGroupMgr.write(dos);

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        CnGroupMgr restored = CnGroupMgr.read(dis);

        Assertions.assertTrue(restored.groupExists(CnGroup.DEFAULT_GROUP_NAME));
        Assertions.assertTrue(restored.groupExists("etl"));
        Assertions.assertTrue(restored.groupExists("analytics"));
        Assertions.assertEquals("etl", restored.getNodeGroup(100L));
        Assertions.assertEquals("analytics", restored.getNodeGroup(200L));
    }

    @Test
    public void testGetGroupById() throws DdlException {
        CnGroup group = cnGroupMgr.createGroup("etl", "");
        long id = group.getId();

        CnGroup retrieved = cnGroupMgr.getGroup(id);
        Assertions.assertNotNull(retrieved);
        Assertions.assertEquals("etl", retrieved.getName());
    }

    @Test
    public void testGetGroupByName() throws DdlException {
        cnGroupMgr.createGroup("etl", "ETL workload");

        CnGroup group = cnGroupMgr.getGroup("etl");
        Assertions.assertNotNull(group);
        Assertions.assertEquals("ETL workload", group.getComment());
    }

    @Test
    public void testCnGroupNodeOpSerialization() throws Exception {
        CnGroupMgr.CnGroupNodeOp op = new CnGroupMgr.CnGroupNodeOp(100L, "etl");

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        op.write(dos);

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        CnGroupMgr.CnGroupNodeOp restored = CnGroupMgr.CnGroupNodeOp.read(dis);

        Assertions.assertEquals(100L, restored.getNodeId());
        Assertions.assertEquals("etl", restored.getGroupName());
    }
}
