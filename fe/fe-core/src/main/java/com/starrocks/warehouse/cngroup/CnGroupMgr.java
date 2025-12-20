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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * CnGroupMgr manages all CnGroups in the system.
 * It provides CRUD operations and persistence for CnGroups.
 *
 * Usage:
 * - ALTER SYSTEM ADD COMPUTE NODE 'host:port' TO CNGROUP 'group_name'
 * - SET cngroup = 'group_name' (to route queries to specific group)
 * - SHOW CNGROUPS
 */
public class CnGroupMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(CnGroupMgr.class);

    // Group ID -> CnGroup
    private final Map<Long, CnGroup> idToGroup = Maps.newConcurrentMap();
    // Group name -> CnGroup
    private final Map<String, CnGroup> nameToGroup = Maps.newConcurrentMap();
    // Node ID -> Group name (for quick lookup)
    private final Map<Long, String> nodeToGroup = Maps.newConcurrentMap();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public CnGroupMgr() {
        // Initialize with default group
        initDefaultGroup();
    }

    private void initDefaultGroup() {
        if (!nameToGroup.containsKey(CnGroup.DEFAULT_GROUP_NAME)) {
            CnGroup defaultGroup = new CnGroup(0, CnGroup.DEFAULT_GROUP_NAME, "Default compute node group");
            idToGroup.put(defaultGroup.getId(), defaultGroup);
            nameToGroup.put(defaultGroup.getName(), defaultGroup);
            LOG.info("Initialized default CnGroup");
        }
    }

    /**
     * Create a new CnGroup.
     * @param name the group name
     * @param comment optional comment
     * @return the created group
     * @throws DdlException if the group already exists
     */
    public CnGroup createGroup(String name, String comment) throws DdlException {
        lock.writeLock().lock();
        try {
            if (nameToGroup.containsKey(name)) {
                throw new DdlException("CnGroup '" + name + "' already exists");
            }
            long id = GlobalStateMgr.getCurrentState().getNextId();
            CnGroup group = new CnGroup(id, name, comment);
            idToGroup.put(id, group);
            nameToGroup.put(name, group);

            // Log for persistence
            GlobalStateMgr.getCurrentState().getEditLog().logCreateCnGroup(group);
            LOG.info("Created CnGroup: {}", group);
            return group;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Replay create group from edit log.
     */
    public void replayCreateGroup(CnGroup group) {
        lock.writeLock().lock();
        try {
            idToGroup.put(group.getId(), group);
            nameToGroup.put(group.getName(), group);
            // Restore node-to-group mappings
            for (Long nodeId : group.getNodeIds()) {
                nodeToGroup.put(nodeId, group.getName());
            }
            LOG.info("Replayed create CnGroup: {}", group);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Drop a CnGroup.
     * @param name the group name
     * @param ifExists if true, don't throw exception if group doesn't exist
     * @throws DdlException if the group doesn't exist or is the default group
     */
    public void dropGroup(String name, boolean ifExists) throws DdlException {
        lock.writeLock().lock();
        try {
            if (CnGroup.DEFAULT_GROUP_NAME.equals(name)) {
                throw new DdlException("Cannot drop the default CnGroup");
            }
            CnGroup group = nameToGroup.get(name);
            if (group == null) {
                if (ifExists) {
                    return;
                }
                throw new DdlException("CnGroup '" + name + "' does not exist");
            }
            if (group.getNodeCount() > 0) {
                throw new DdlException("Cannot drop CnGroup '" + name + "' with " +
                        group.getNodeCount() + " nodes. Move nodes to another group first.");
            }

            idToGroup.remove(group.getId());
            nameToGroup.remove(name);

            // Log for persistence
            GlobalStateMgr.getCurrentState().getEditLog().logDropCnGroup(group);
            LOG.info("Dropped CnGroup: {}", name);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Replay drop group from edit log.
     */
    public void replayDropGroup(CnGroup group) {
        lock.writeLock().lock();
        try {
            idToGroup.remove(group.getId());
            nameToGroup.remove(group.getName());
            // Remove node-to-group mappings
            for (Long nodeId : group.getNodeIds()) {
                nodeToGroup.remove(nodeId);
            }
            LOG.info("Replayed drop CnGroup: {}", group.getName());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Add a compute node to a CnGroup.
     * If group doesn't exist, it will be auto-created.
     * @param nodeId the compute node ID
     * @param groupName the group name
     */
    public void addNodeToGroup(long nodeId, String groupName) throws DdlException {
        lock.writeLock().lock();
        try {
            // Remove from old group if any
            String oldGroup = nodeToGroup.get(nodeId);
            if (oldGroup != null) {
                CnGroup old = nameToGroup.get(oldGroup);
                if (old != null) {
                    old.removeNode(nodeId);
                }
            }

            // Get or create group
            CnGroup group = nameToGroup.get(groupName);
            if (group == null) {
                // Auto-create group
                long id = GlobalStateMgr.getCurrentState().getNextId();
                group = new CnGroup(id, groupName, "Auto-created group");
                idToGroup.put(id, group);
                nameToGroup.put(groupName, group);
                LOG.info("Auto-created CnGroup: {}", groupName);
            }

            group.addNode(nodeId);
            nodeToGroup.put(nodeId, groupName);

            // Update ComputeNode's cnGroupName
            updateComputeNodeGroup(nodeId, groupName);

            // Log for persistence
            GlobalStateMgr.getCurrentState().getEditLog().logAddNodeToCnGroup(
                    new CnGroupNodeOp(nodeId, groupName));
            LOG.info("Added node {} to CnGroup: {}", nodeId, groupName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Replay add node to group from edit log.
     */
    public void replayAddNodeToGroup(CnGroupNodeOp op) {
        lock.writeLock().lock();
        try {
            // Remove from old group if any
            String oldGroup = nodeToGroup.get(op.getNodeId());
            if (oldGroup != null) {
                CnGroup old = nameToGroup.get(oldGroup);
                if (old != null) {
                    old.removeNode(op.getNodeId());
                }
            }

            // Get or create group
            CnGroup group = nameToGroup.get(op.getGroupName());
            if (group == null) {
                long id = GlobalStateMgr.getCurrentState().getNextId();
                group = new CnGroup(id, op.getGroupName(), "Auto-created group");
                idToGroup.put(id, group);
                nameToGroup.put(op.getGroupName(), group);
            }

            group.addNode(op.getNodeId());
            nodeToGroup.put(op.getNodeId(), op.getGroupName());

            // Also update ComputeNode's cnGroupName to keep in sync
            updateComputeNodeGroup(op.getNodeId(), op.getGroupName());

            LOG.info("Replayed add node {} to CnGroup: {}", op.getNodeId(), op.getGroupName());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove a compute node from its group.
     * The node will be moved to the default group.
     * @param nodeId the compute node ID
     */
    public void removeNodeFromGroup(long nodeId) throws DdlException {
        lock.writeLock().lock();
        try {
            String groupName = nodeToGroup.get(nodeId);
            if (groupName == null || CnGroup.DEFAULT_GROUP_NAME.equals(groupName)) {
                return; // Already in default group or not in any group
            }

            CnGroup group = nameToGroup.get(groupName);
            if (group != null) {
                group.removeNode(nodeId);
            }

            // Move to default group
            CnGroup defaultGroup = nameToGroup.get(CnGroup.DEFAULT_GROUP_NAME);
            defaultGroup.addNode(nodeId);
            nodeToGroup.put(nodeId, CnGroup.DEFAULT_GROUP_NAME);

            // Update ComputeNode's cnGroupName
            updateComputeNodeGroup(nodeId, CnGroup.DEFAULT_GROUP_NAME);

            // Log for persistence
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveNodeFromCnGroup(
                    new CnGroupNodeOp(nodeId, groupName));
            LOG.info("Removed node {} from CnGroup: {}, moved to default", nodeId, groupName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Replay remove node from group from edit log.
     */
    public void replayRemoveNodeFromGroup(CnGroupNodeOp op) {
        lock.writeLock().lock();
        try {
            CnGroup group = nameToGroup.get(op.getGroupName());
            if (group != null) {
                group.removeNode(op.getNodeId());
            }

            // Move to default group
            CnGroup defaultGroup = nameToGroup.get(CnGroup.DEFAULT_GROUP_NAME);
            if (defaultGroup != null) {
                defaultGroup.addNode(op.getNodeId());
            }
            nodeToGroup.put(op.getNodeId(), CnGroup.DEFAULT_GROUP_NAME);

            // Also update ComputeNode's cnGroupName to keep in sync
            updateComputeNodeGroup(op.getNodeId(), CnGroup.DEFAULT_GROUP_NAME);

            LOG.info("Replayed remove node {} from CnGroup: {}", op.getNodeId(), op.getGroupName());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Handle node being dropped from system.
     * Called when a compute node is removed from the cluster.
     */
    public void handleNodeDropped(long nodeId) {
        lock.writeLock().lock();
        try {
            String groupName = nodeToGroup.remove(nodeId);
            if (groupName != null) {
                CnGroup group = nameToGroup.get(groupName);
                if (group != null) {
                    group.removeNode(nodeId);
                }
            }
            LOG.info("Handled dropped node {} from CnGroup: {}", nodeId, groupName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Handle a new node being added to the system.
     * Registers the node with its cnGroupName in CnGroupMgr without logging to EditLog.
     * Called when a compute node is added to the cluster.
     */
    public void handleNodeAdded(long nodeId, String cnGroupName) {
        lock.writeLock().lock();
        try {
            String groupName = (cnGroupName == null || cnGroupName.isEmpty())
                    ? CnGroup.DEFAULT_GROUP_NAME : cnGroupName;

            // Get or create group (without logging - this is internal sync)
            CnGroup group = nameToGroup.get(groupName);
            if (group == null) {
                // For non-default groups, they should already exist
                // Fall back to default group
                groupName = CnGroup.DEFAULT_GROUP_NAME;
                group = nameToGroup.get(groupName);
            }

            if (group != null) {
                group.addNode(nodeId);
                nodeToGroup.put(nodeId, groupName);
                LOG.info("Registered new node {} with CnGroup: {}", nodeId, groupName);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Sync CnGroupMgr with existing compute nodes.
     * This should be called during startup after all nodes are loaded.
     */
    public void syncWithExistingNodes() {
        SystemInfoService systemInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        lock.writeLock().lock();
        try {
            // Sync compute nodes
            for (ComputeNode node : systemInfo.getIdComputeNode().values()) {
                String groupName = node.getCnGroupName();
                if (groupName == null || groupName.isEmpty()) {
                    groupName = CnGroup.DEFAULT_GROUP_NAME;
                }

                // Get or create group
                CnGroup group = nameToGroup.get(groupName);
                if (group == null) {
                    // Create the group if it doesn't exist
                    long id = GlobalStateMgr.getCurrentState().getNextId();
                    group = new CnGroup(id, groupName, "Auto-created during sync");
                    idToGroup.put(id, group);
                    nameToGroup.put(groupName, group);
                    LOG.info("Auto-created CnGroup during sync: {}", groupName);
                }

                group.addNode(node.getId());
                nodeToGroup.put(node.getId(), groupName);
            }

            // Sync backends (in SHARED_DATA mode, backends are also treated as compute nodes)
            for (ComputeNode node : systemInfo.getIdToBackend().values()) {
                String groupName = node.getCnGroupName();
                if (groupName == null || groupName.isEmpty()) {
                    groupName = CnGroup.DEFAULT_GROUP_NAME;
                }

                CnGroup group = nameToGroup.get(groupName);
                if (group == null) {
                    long id = GlobalStateMgr.getCurrentState().getNextId();
                    group = new CnGroup(id, groupName, "Auto-created during sync");
                    idToGroup.put(id, group);
                    nameToGroup.put(groupName, group);
                    LOG.info("Auto-created CnGroup during sync: {}", groupName);
                }

                group.addNode(node.getId());
                nodeToGroup.put(node.getId(), groupName);
            }

            LOG.info("Synced CnGroupMgr with {} compute nodes and {} backends",
                    systemInfo.getIdComputeNode().size(), systemInfo.getIdToBackend().size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void updateComputeNodeGroup(long nodeId, String groupName) {
        SystemInfoService systemInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        ComputeNode node = systemInfo.getBackendOrComputeNode(nodeId);
        if (node != null) {
            node.setCnGroupName(groupName);
        }
    }

    /**
     * Get a CnGroup by name.
     */
    public CnGroup getGroup(String name) {
        lock.readLock().lock();
        try {
            return nameToGroup.get(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get a CnGroup by ID.
     */
    public CnGroup getGroup(long id) {
        lock.readLock().lock();
        try {
            return idToGroup.get(id);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the group name for a compute node.
     */
    public String getNodeGroup(long nodeId) {
        lock.readLock().lock();
        try {
            return nodeToGroup.getOrDefault(nodeId, CnGroup.DEFAULT_GROUP_NAME);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Check if a group exists.
     */
    public boolean groupExists(String name) {
        lock.readLock().lock();
        try {
            return nameToGroup.containsKey(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get all group names.
     */
    public List<String> getAllGroupNames() {
        lock.readLock().lock();
        try {
            return ImmutableList.copyOf(nameToGroup.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get all groups.
     */
    public Collection<CnGroup> getAllGroups() {
        lock.readLock().lock();
        try {
            return ImmutableList.copyOf(nameToGroup.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get all node IDs in a group.
     */
    public Set<Long> getNodesInGroup(String groupName) {
        lock.readLock().lock();
        try {
            CnGroup group = nameToGroup.get(groupName);
            if (group == null) {
                return Set.of();
            }
            return group.getNodeIds();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get alive compute nodes in a group.
     */
    public List<ComputeNode> getAliveNodesInGroup(String groupName) {
        Set<Long> nodeIds = getNodesInGroup(groupName);
        if (nodeIds.isEmpty()) {
            return Lists.newArrayList();
        }

        SystemInfoService systemInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        return nodeIds.stream()
                .map(systemInfo::getBackendOrComputeNode)
                .filter(node -> node != null && node.isAlive())
                .collect(Collectors.toList());
    }

    /**
     * Get info for SHOW CNGROUPS.
     * Returns: Name, NodeCount, NodeIds, Comment, CreateTime
     */
    public List<List<String>> getGroupsInfo() {
        lock.readLock().lock();
        try {
            List<List<String>> result = new ArrayList<>();

            for (CnGroup group : nameToGroup.values()) {
                Set<Long> nodeIds = group.getNodeIds();
                String nodeIdsStr = nodeIds.stream()
                        .map(String::valueOf)
                        .collect(Collectors.joining(", "));

                List<String> row = Lists.newArrayList(
                        group.getName(),
                        String.valueOf(nodeIds.size()),
                        nodeIdsStr.isEmpty() ? "" : nodeIdsStr,
                        Optional.ofNullable(group.getComment()).orElse(""),
                        group.getCreateTimeStr()
                );
                result.add(row);
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // Write number of groups
        out.writeInt(nameToGroup.size());
        for (CnGroup group : nameToGroup.values()) {
            group.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            CnGroup group = CnGroup.read(in);
            idToGroup.put(group.getId(), group);
            nameToGroup.put(group.getName(), group);
            // Restore node-to-group mappings
            for (Long nodeId : group.getNodeIds()) {
                nodeToGroup.put(nodeId, group.getName());
            }
        }
        // Ensure default group exists
        initDefaultGroup();
    }

    public static CnGroupMgr read(DataInput in) throws IOException {
        CnGroupMgr mgr = new CnGroupMgr();
        mgr.readFields(in);
        return mgr;
    }

    /**
     * Operation class for node add/remove persistence.
     */
    public static class CnGroupNodeOp implements Writable {
        private long nodeId;
        private String groupName;

        public CnGroupNodeOp() {
        }

        public CnGroupNodeOp(long nodeId, String groupName) {
            this.nodeId = nodeId;
            this.groupName = groupName;
        }

        public long getNodeId() {
            return nodeId;
        }

        public String getGroupName() {
            return groupName;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }

        public static CnGroupNodeOp read(DataInput in) throws IOException {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, CnGroupNodeOp.class);
        }
    }
}
