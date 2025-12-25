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

import com.starrocks.leader.CheckpointController;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.MockJournal;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CnGroupEditLogTest {

    private CnGroupMgr masterCnGroupMgr;
    private CnGroupMgr followerCnGroupMgr;
    private SystemInfoService masterSystemInfoService;
    private SystemInfoService followerSystemInfoService;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Get managers from GlobalStateMgr
        masterCnGroupMgr = GlobalStateMgr.getCurrentState().getCnGroupMgr();
        masterSystemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

        // Create follower instances
        followerCnGroupMgr = new CnGroupMgr();
        followerSystemInfoService = new SystemInfoService();

        GlobalStateMgr.getCurrentState().setHaProtocol(new MockJournal.MockProtocol());
        GlobalStateMgr.getCurrentState().setCheckpointController(
                new CheckpointController("controller", new MockJournal(), ""));
        GlobalStateMgr.getCurrentState().initDefaultWarehouse();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // ==================== Create CnGroup Tests ====================

    @Test
    public void testCreateCnGroup() throws Exception {
        String groupName = "etl_create_" + System.nanoTime();

        // 1. Verify initial state - only default group exists
        Assertions.assertTrue(masterCnGroupMgr.groupExists(CnGroup.DEFAULT_GROUP_NAME));
        Assertions.assertFalse(masterCnGroupMgr.groupExists(groupName));

        // 2. Create group on master
        CnGroup group = masterCnGroupMgr.createGroup(groupName, "ETL workload");

        // 3. Verify master state
        Assertions.assertTrue(masterCnGroupMgr.groupExists(groupName));
        Assertions.assertEquals("ETL workload", masterCnGroupMgr.getGroup(groupName).getComment());

        // 4. Replay on follower
        CnGroup replayedGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
        followerCnGroupMgr.replayCreateGroup(replayedGroup);

        // 5. Verify follower state matches master
        Assertions.assertTrue(followerCnGroupMgr.groupExists(groupName));
        Assertions.assertEquals("ETL workload", followerCnGroupMgr.getGroup(groupName).getComment());
    }

    @Test
    public void testDropCnGroup() throws Exception {
        String groupName = "etl_drop_" + System.nanoTime();

        // 1. Create group first
        masterCnGroupMgr.createGroup(groupName, "");
        Assertions.assertTrue(masterCnGroupMgr.groupExists(groupName));

        // Replay create on follower
        CnGroup createdGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
        followerCnGroupMgr.replayCreateGroup(createdGroup);

        // 2. Drop group on master
        masterCnGroupMgr.dropGroup(groupName, false);

        // 3. Verify master state
        Assertions.assertFalse(masterCnGroupMgr.groupExists(groupName));

        // 4. Replay drop on follower
        CnGroup droppedGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_CN_GROUP);
        followerCnGroupMgr.replayDropGroup(droppedGroup);

        // 5. Verify follower state
        Assertions.assertFalse(followerCnGroupMgr.groupExists(groupName));
    }

    @Test
    public void testAddNodeToCnGroup() throws Exception {
        String groupName = "etl_add_" + System.nanoTime();
        long nodeId = 100L + System.nanoTime() % 10000;

        // 1. Create group
        masterCnGroupMgr.createGroup(groupName, "");
        CnGroup createdGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
        followerCnGroupMgr.replayCreateGroup(createdGroup);

        // 2. Add compute node to system first
        ComputeNode cn = new ComputeNode(nodeId, "host1", 9050);
        masterSystemInfoService.addComputeNode(cn);
        followerSystemInfoService.addComputeNode(cn);

        // 3. Add node to group on master
        masterCnGroupMgr.addNodeToGroup(nodeId, groupName);

        // 4. Verify master state
        Assertions.assertEquals(groupName, masterCnGroupMgr.getNodeGroup(nodeId));
        Assertions.assertTrue(masterCnGroupMgr.getNodesInGroup(groupName).contains(nodeId));

        // 5. Replay on follower
        CnGroupMgr.CnGroupNodeOp op = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_NODE_TO_CN_GROUP);
        followerCnGroupMgr.replayAddNodeToGroup(op);

        // 6. Verify follower state
        Assertions.assertEquals(groupName, followerCnGroupMgr.getNodeGroup(nodeId));
        Assertions.assertTrue(followerCnGroupMgr.getNodesInGroup(groupName).contains(nodeId));
    }

    @Test
    public void testRemoveNodeFromCnGroup() throws Exception {
        String groupName = "etl_remove_" + System.nanoTime();
        long nodeId = 200L + System.nanoTime() % 10000;

        // 1. Setup: create group and add node
        masterCnGroupMgr.createGroup(groupName, "");
        CnGroup createdGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
        followerCnGroupMgr.replayCreateGroup(createdGroup);

        ComputeNode cn = new ComputeNode(nodeId, "host1", 9050);
        masterSystemInfoService.addComputeNode(cn);
        followerSystemInfoService.addComputeNode(cn);

        masterCnGroupMgr.addNodeToGroup(nodeId, groupName);
        CnGroupMgr.CnGroupNodeOp addOp = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_NODE_TO_CN_GROUP);
        followerCnGroupMgr.replayAddNodeToGroup(addOp);

        // Verify node is in group
        Assertions.assertEquals(groupName, masterCnGroupMgr.getNodeGroup(nodeId));

        // 2. Remove node from group on master
        masterCnGroupMgr.removeNodeFromGroup(nodeId);

        // 3. Verify master state - node should be in default group now
        Assertions.assertEquals(CnGroup.DEFAULT_GROUP_NAME, masterCnGroupMgr.getNodeGroup(nodeId));
        Assertions.assertFalse(masterCnGroupMgr.getNodesInGroup(groupName).contains(nodeId));
        Assertions.assertTrue(masterCnGroupMgr.getNodesInGroup(CnGroup.DEFAULT_GROUP_NAME).contains(nodeId));

        // 4. Replay on follower
        CnGroupMgr.CnGroupNodeOp removeOp = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_REMOVE_NODE_FROM_CN_GROUP);
        followerCnGroupMgr.replayRemoveNodeFromGroup(removeOp);

        // 5. Verify follower state
        Assertions.assertEquals(CnGroup.DEFAULT_GROUP_NAME, followerCnGroupMgr.getNodeGroup(nodeId));
        Assertions.assertFalse(followerCnGroupMgr.getNodesInGroup(groupName).contains(nodeId));
    }

    @Test
    public void testMoveNodeBetweenGroups() throws Exception {
        String etlName = "etl_move_" + System.nanoTime();
        String analyticsName = "analytics_move_" + System.nanoTime();
        long nodeId = 300L + System.nanoTime() % 10000;

        // 1. Create two groups
        masterCnGroupMgr.createGroup(etlName, "");
        masterCnGroupMgr.createGroup(analyticsName, "");

        // Replay creates on follower
        CnGroup etlGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
        followerCnGroupMgr.replayCreateGroup(etlGroup);
        CnGroup analyticsGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
        followerCnGroupMgr.replayCreateGroup(analyticsGroup);

        // 2. Add node to etl group
        ComputeNode cn = new ComputeNode(nodeId, "host1", 9050);
        masterSystemInfoService.addComputeNode(cn);
        followerSystemInfoService.addComputeNode(cn);

        masterCnGroupMgr.addNodeToGroup(nodeId, etlName);
        CnGroupMgr.CnGroupNodeOp addEtlOp = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_NODE_TO_CN_GROUP);
        followerCnGroupMgr.replayAddNodeToGroup(addEtlOp);

        Assertions.assertEquals(etlName, masterCnGroupMgr.getNodeGroup(nodeId));
        Assertions.assertEquals(etlName, followerCnGroupMgr.getNodeGroup(nodeId));

        // 3. Move node to analytics group
        masterCnGroupMgr.addNodeToGroup(nodeId, analyticsName);

        // 4. Verify master state
        Assertions.assertEquals(analyticsName, masterCnGroupMgr.getNodeGroup(nodeId));
        Assertions.assertFalse(masterCnGroupMgr.getNodesInGroup(etlName).contains(nodeId));
        Assertions.assertTrue(masterCnGroupMgr.getNodesInGroup(analyticsName).contains(nodeId));

        // 5. Replay on follower
        CnGroupMgr.CnGroupNodeOp moveOp = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_NODE_TO_CN_GROUP);
        followerCnGroupMgr.replayAddNodeToGroup(moveOp);

        // 6. Verify follower state
        Assertions.assertEquals(analyticsName, followerCnGroupMgr.getNodeGroup(nodeId));
        Assertions.assertFalse(followerCnGroupMgr.getNodesInGroup(etlName).contains(nodeId));
        Assertions.assertTrue(followerCnGroupMgr.getNodesInGroup(analyticsName).contains(nodeId));
    }

    @Test
    public void testFullScenario() throws Exception {
        // This test simulates a complete workflow:
        // 1. Create groups
        // 2. Add nodes
        // 3. Move nodes between groups
        // 4. Drop empty group
        // And verifies master-follower consistency at each step

        String etlName = "etl_full_" + System.nanoTime();
        String analyticsName = "analytics_full_" + System.nanoTime();
        String tempName = "temp_full_" + System.nanoTime();
        long baseNodeId = 400L + System.nanoTime() % 10000;

        // Step 1: Create groups
        masterCnGroupMgr.createGroup(etlName, "ETL jobs");
        masterCnGroupMgr.createGroup(analyticsName, "Analytics queries");
        masterCnGroupMgr.createGroup(tempName, "Temporary");

        for (int i = 0; i < 3; i++) {
            CnGroup g = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                    .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
            followerCnGroupMgr.replayCreateGroup(g);
        }

        // Step 2: Add nodes
        for (long i = 1; i <= 3; i++) {
            ComputeNode cn = new ComputeNode(baseNodeId + i, "host" + i, 9050);
            masterSystemInfoService.addComputeNode(cn);
            followerSystemInfoService.addComputeNode(cn);
        }

        masterCnGroupMgr.addNodeToGroup(baseNodeId + 1, etlName);
        masterCnGroupMgr.addNodeToGroup(baseNodeId + 2, etlName);
        masterCnGroupMgr.addNodeToGroup(baseNodeId + 3, analyticsName);

        for (int i = 0; i < 3; i++) {
            CnGroupMgr.CnGroupNodeOp op = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                    .replayNextJournal(OperationType.OP_ADD_NODE_TO_CN_GROUP);
            followerCnGroupMgr.replayAddNodeToGroup(op);
        }

        // Verify state
        Assertions.assertEquals(2, masterCnGroupMgr.getNodesInGroup(etlName).size());
        Assertions.assertEquals(1, masterCnGroupMgr.getNodesInGroup(analyticsName).size());
        Assertions.assertEquals(2, followerCnGroupMgr.getNodesInGroup(etlName).size());
        Assertions.assertEquals(1, followerCnGroupMgr.getNodesInGroup(analyticsName).size());

        // Step 3: Move node 2 from etl to analytics
        masterCnGroupMgr.addNodeToGroup(baseNodeId + 2, analyticsName);
        CnGroupMgr.CnGroupNodeOp moveOp = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_NODE_TO_CN_GROUP);
        followerCnGroupMgr.replayAddNodeToGroup(moveOp);

        Assertions.assertEquals(1, masterCnGroupMgr.getNodesInGroup(etlName).size());
        Assertions.assertEquals(2, masterCnGroupMgr.getNodesInGroup(analyticsName).size());
        Assertions.assertEquals(1, followerCnGroupMgr.getNodesInGroup(etlName).size());
        Assertions.assertEquals(2, followerCnGroupMgr.getNodesInGroup(analyticsName).size());

        // Step 4: Drop empty temp group
        masterCnGroupMgr.dropGroup(tempName, false);
        CnGroup droppedGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_CN_GROUP);
        followerCnGroupMgr.replayDropGroup(droppedGroup);

        Assertions.assertFalse(masterCnGroupMgr.groupExists(tempName));
        Assertions.assertFalse(followerCnGroupMgr.groupExists(tempName));
    }

    @Test
    public void testAutoCreateGroupOnReplay() throws Exception {
        // Test that group auto-creation is persisted via OP_CREATE_CN_GROUP before OP_ADD_NODE_TO_CN_GROUP.
        String groupName = "auto_created_" + System.nanoTime();
        long nodeId = 500L + System.nanoTime() % 10000;

        ComputeNode cn = new ComputeNode(nodeId, "host1", 9050);
        masterSystemInfoService.addComputeNode(cn);
        followerSystemInfoService.addComputeNode(cn);

        // Add node to non-existent group (will auto-create)
        masterCnGroupMgr.addNodeToGroup(nodeId, groupName);

        // Replay create group first
        CnGroup createdGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
        followerCnGroupMgr.replayCreateGroup(createdGroup);

        // Replay add node operation
        CnGroupMgr.CnGroupNodeOp op = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_NODE_TO_CN_GROUP);

        followerCnGroupMgr.replayAddNodeToGroup(op);

        Assertions.assertTrue(masterCnGroupMgr.groupExists(groupName));
        Assertions.assertTrue(followerCnGroupMgr.groupExists(groupName));
        Assertions.assertEquals(groupName, masterCnGroupMgr.getNodeGroup(nodeId));
        Assertions.assertEquals(groupName, followerCnGroupMgr.getNodeGroup(nodeId));

        // Group ID should be stable across master/follower (auto-create is persisted).
        Assertions.assertEquals(masterCnGroupMgr.getGroup(groupName).getId(),
                followerCnGroupMgr.getGroup(groupName).getId());
    }
}
