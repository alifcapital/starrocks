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

import com.starrocks.common.DdlException;
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
        // 1. Verify initial state - only default group exists
        Assertions.assertTrue(masterCnGroupMgr.groupExists(CnGroup.DEFAULT_GROUP_NAME));
        Assertions.assertFalse(masterCnGroupMgr.groupExists("etl"));

        // 2. Create group on master
        CnGroup group = masterCnGroupMgr.createGroup("etl", "ETL workload");

        // 3. Verify master state
        Assertions.assertTrue(masterCnGroupMgr.groupExists("etl"));
        Assertions.assertEquals("ETL workload", masterCnGroupMgr.getGroup("etl").getComment());

        // 4. Replay on follower
        CnGroup replayedGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
        followerCnGroupMgr.replayCreateGroup(replayedGroup);

        // 5. Verify follower state matches master
        Assertions.assertTrue(followerCnGroupMgr.groupExists("etl"));
        Assertions.assertEquals("ETL workload", followerCnGroupMgr.getGroup("etl").getComment());
    }

    @Test
    public void testDropCnGroup() throws Exception {
        // 1. Create group first
        masterCnGroupMgr.createGroup("etl", "");
        Assertions.assertTrue(masterCnGroupMgr.groupExists("etl"));

        // Replay create on follower
        CnGroup createdGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
        followerCnGroupMgr.replayCreateGroup(createdGroup);

        // 2. Drop group on master
        masterCnGroupMgr.dropGroup("etl", false);

        // 3. Verify master state
        Assertions.assertFalse(masterCnGroupMgr.groupExists("etl"));

        // 4. Replay drop on follower
        CnGroup droppedGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_CN_GROUP);
        followerCnGroupMgr.replayDropGroup(droppedGroup);

        // 5. Verify follower state
        Assertions.assertFalse(followerCnGroupMgr.groupExists("etl"));
    }

    @Test
    public void testAddNodeToCnGroup() throws Exception {
        // 1. Create group
        masterCnGroupMgr.createGroup("etl", "");
        CnGroup createdGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
        followerCnGroupMgr.replayCreateGroup(createdGroup);

        // 2. Add compute node to system first
        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        masterSystemInfoService.addComputeNode(cn);
        followerSystemInfoService.addComputeNode(cn);

        // 3. Add node to group on master
        masterCnGroupMgr.addNodeToGroup(100L, "etl");

        // 4. Verify master state
        Assertions.assertEquals("etl", masterCnGroupMgr.getNodeGroup(100L));
        Assertions.assertTrue(masterCnGroupMgr.getNodesInGroup("etl").contains(100L));

        // 5. Replay on follower
        CnGroupMgr.CnGroupNodeOp op = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_NODE_TO_CN_GROUP);
        followerCnGroupMgr.replayAddNodeToGroup(op);

        // 6. Verify follower state
        Assertions.assertEquals("etl", followerCnGroupMgr.getNodeGroup(100L));
        Assertions.assertTrue(followerCnGroupMgr.getNodesInGroup("etl").contains(100L));
    }

    @Test
    public void testRemoveNodeFromCnGroup() throws Exception {
        // 1. Setup: create group and add node
        masterCnGroupMgr.createGroup("etl", "");
        CnGroup createdGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
        followerCnGroupMgr.replayCreateGroup(createdGroup);

        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        masterSystemInfoService.addComputeNode(cn);
        followerSystemInfoService.addComputeNode(cn);

        masterCnGroupMgr.addNodeToGroup(100L, "etl");
        CnGroupMgr.CnGroupNodeOp addOp = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_NODE_TO_CN_GROUP);
        followerCnGroupMgr.replayAddNodeToGroup(addOp);

        // Verify node is in etl group
        Assertions.assertEquals("etl", masterCnGroupMgr.getNodeGroup(100L));

        // 2. Remove node from group on master
        masterCnGroupMgr.removeNodeFromGroup(100L);

        // 3. Verify master state - node should be in default group now
        Assertions.assertEquals(CnGroup.DEFAULT_GROUP_NAME, masterCnGroupMgr.getNodeGroup(100L));
        Assertions.assertFalse(masterCnGroupMgr.getNodesInGroup("etl").contains(100L));
        Assertions.assertTrue(masterCnGroupMgr.getNodesInGroup(CnGroup.DEFAULT_GROUP_NAME).contains(100L));

        // 4. Replay on follower
        CnGroupMgr.CnGroupNodeOp removeOp = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_REMOVE_NODE_FROM_CN_GROUP);
        followerCnGroupMgr.replayRemoveNodeFromGroup(removeOp);

        // 5. Verify follower state
        Assertions.assertEquals(CnGroup.DEFAULT_GROUP_NAME, followerCnGroupMgr.getNodeGroup(100L));
        Assertions.assertFalse(followerCnGroupMgr.getNodesInGroup("etl").contains(100L));
    }

    @Test
    public void testMoveNodeBetweenGroups() throws Exception {
        // 1. Create two groups
        masterCnGroupMgr.createGroup("etl", "");
        masterCnGroupMgr.createGroup("analytics", "");

        // Replay creates on follower
        CnGroup etlGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
        followerCnGroupMgr.replayCreateGroup(etlGroup);
        CnGroup analyticsGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
        followerCnGroupMgr.replayCreateGroup(analyticsGroup);

        // 2. Add node to etl group
        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        masterSystemInfoService.addComputeNode(cn);
        followerSystemInfoService.addComputeNode(cn);

        masterCnGroupMgr.addNodeToGroup(100L, "etl");
        CnGroupMgr.CnGroupNodeOp addEtlOp = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_NODE_TO_CN_GROUP);
        followerCnGroupMgr.replayAddNodeToGroup(addEtlOp);

        Assertions.assertEquals("etl", masterCnGroupMgr.getNodeGroup(100L));
        Assertions.assertEquals("etl", followerCnGroupMgr.getNodeGroup(100L));

        // 3. Move node to analytics group
        masterCnGroupMgr.addNodeToGroup(100L, "analytics");

        // 4. Verify master state
        Assertions.assertEquals("analytics", masterCnGroupMgr.getNodeGroup(100L));
        Assertions.assertFalse(masterCnGroupMgr.getNodesInGroup("etl").contains(100L));
        Assertions.assertTrue(masterCnGroupMgr.getNodesInGroup("analytics").contains(100L));

        // 5. Replay on follower
        CnGroupMgr.CnGroupNodeOp moveOp = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_NODE_TO_CN_GROUP);
        followerCnGroupMgr.replayAddNodeToGroup(moveOp);

        // 6. Verify follower state
        Assertions.assertEquals("analytics", followerCnGroupMgr.getNodeGroup(100L));
        Assertions.assertFalse(followerCnGroupMgr.getNodesInGroup("etl").contains(100L));
        Assertions.assertTrue(followerCnGroupMgr.getNodesInGroup("analytics").contains(100L));
    }

    @Test
    public void testFullScenario() throws Exception {
        // This test simulates a complete workflow:
        // 1. Create groups
        // 2. Add nodes
        // 3. Move nodes between groups
        // 4. Drop empty group
        // And verifies master-follower consistency at each step

        // Step 1: Create groups
        masterCnGroupMgr.createGroup("etl", "ETL jobs");
        masterCnGroupMgr.createGroup("analytics", "Analytics queries");
        masterCnGroupMgr.createGroup("temp", "Temporary");

        for (int i = 0; i < 3; i++) {
            CnGroup g = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                    .replayNextJournal(OperationType.OP_CREATE_CN_GROUP);
            followerCnGroupMgr.replayCreateGroup(g);
        }

        // Step 2: Add nodes
        for (long i = 1; i <= 3; i++) {
            ComputeNode cn = new ComputeNode(i, "host" + i, 9050);
            masterSystemInfoService.addComputeNode(cn);
            followerSystemInfoService.addComputeNode(cn);
        }

        masterCnGroupMgr.addNodeToGroup(1L, "etl");
        masterCnGroupMgr.addNodeToGroup(2L, "etl");
        masterCnGroupMgr.addNodeToGroup(3L, "analytics");

        for (int i = 0; i < 3; i++) {
            CnGroupMgr.CnGroupNodeOp op = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                    .replayNextJournal(OperationType.OP_ADD_NODE_TO_CN_GROUP);
            followerCnGroupMgr.replayAddNodeToGroup(op);
        }

        // Verify state
        Assertions.assertEquals(2, masterCnGroupMgr.getNodesInGroup("etl").size());
        Assertions.assertEquals(1, masterCnGroupMgr.getNodesInGroup("analytics").size());
        Assertions.assertEquals(2, followerCnGroupMgr.getNodesInGroup("etl").size());
        Assertions.assertEquals(1, followerCnGroupMgr.getNodesInGroup("analytics").size());

        // Step 3: Move node 2 from etl to analytics
        masterCnGroupMgr.addNodeToGroup(2L, "analytics");
        CnGroupMgr.CnGroupNodeOp moveOp = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_NODE_TO_CN_GROUP);
        followerCnGroupMgr.replayAddNodeToGroup(moveOp);

        Assertions.assertEquals(1, masterCnGroupMgr.getNodesInGroup("etl").size());
        Assertions.assertEquals(2, masterCnGroupMgr.getNodesInGroup("analytics").size());
        Assertions.assertEquals(1, followerCnGroupMgr.getNodesInGroup("etl").size());
        Assertions.assertEquals(2, followerCnGroupMgr.getNodesInGroup("analytics").size());

        // Step 4: Drop empty temp group
        masterCnGroupMgr.dropGroup("temp", false);
        CnGroup droppedGroup = (CnGroup) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_CN_GROUP);
        followerCnGroupMgr.replayDropGroup(droppedGroup);

        Assertions.assertFalse(masterCnGroupMgr.groupExists("temp"));
        Assertions.assertFalse(followerCnGroupMgr.groupExists("temp"));

        // Final verification
        Assertions.assertEquals(3, masterCnGroupMgr.getAllGroups().size()); // default, etl, analytics
        Assertions.assertEquals(3, followerCnGroupMgr.getAllGroups().size());
    }

    @Test
    public void testAutoCreateGroupOnReplay() throws Exception {
        // Test that group is auto-created when replaying add node to non-existent group

        ComputeNode cn = new ComputeNode(100L, "host1", 9050);
        masterSystemInfoService.addComputeNode(cn);
        followerSystemInfoService.addComputeNode(cn);

        // Add node to non-existent group (will auto-create)
        masterCnGroupMgr.addNodeToGroup(100L, "auto_created");

        // Skip the auto-created group's create log (it's done internally)
        // and replay the add node operation
        CnGroupMgr.CnGroupNodeOp op = (CnGroupMgr.CnGroupNodeOp) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ADD_NODE_TO_CN_GROUP);

        // Follower should also auto-create the group during replay
        followerCnGroupMgr.replayAddNodeToGroup(op);

        Assertions.assertTrue(masterCnGroupMgr.groupExists("auto_created"));
        Assertions.assertTrue(followerCnGroupMgr.groupExists("auto_created"));
        Assertions.assertEquals("auto_created", masterCnGroupMgr.getNodeGroup(100L));
        Assertions.assertEquals("auto_created", followerCnGroupMgr.getNodeGroup(100L));
    }
}
