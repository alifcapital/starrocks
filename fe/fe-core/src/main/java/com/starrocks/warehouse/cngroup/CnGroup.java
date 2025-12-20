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

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * CnGroup represents a group of Compute Nodes that can be used for workload isolation.
 * Each CN belongs to exactly one CnGroup.
 * Queries can be routed to specific CnGroups using the session variable 'cngroup'.
 */
public class CnGroup implements Writable, GsonPostProcessable {

    public static final String DEFAULT_GROUP_NAME = "default";

    @SerializedName("id")
    private final long id;

    @SerializedName("name")
    private final String name;

    @SerializedName("nodeIds")
    private final Set<Long> nodeIds;

    @SerializedName("comment")
    private String comment;

    @SerializedName("createTime")
    private final long createTime;

    // Not serialized - runtime lock, initialized lazily after deserialization
    private transient ReadWriteLock lock;

    public CnGroup(long id, String name) {
        this(id, name, "");
    }

    public CnGroup(long id, String name, String comment) {
        this.id = id;
        this.name = name;
        this.comment = comment;
        this.nodeIds = Sets.newConcurrentHashSet();
        this.createTime = System.currentTimeMillis();
        this.lock = new ReentrantReadWriteLock();
    }

    /**
     * Get the lock, initializing it if necessary (after deserialization).
     */
    private ReadWriteLock getLock() {
        if (lock == null) {
            synchronized (this) {
                if (lock == null) {
                    lock = new ReentrantReadWriteLock();
                }
            }
        }
        return lock;
    }

    @Override
    public void gsonPostProcess() {
        // Initialize lock after GSON deserialization
        if (lock == null) {
            lock = new ReentrantReadWriteLock();
        }
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public long getCreateTime() {
        return createTime;
    }

    /**
     * Get the creation time as a formatted string.
     * @return formatted date string
     */
    public String getCreateTimeStr() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(createTime));
    }

    /**
     * Add a compute node to this group.
     * @param nodeId the ID of the compute node
     * @return true if the node was added, false if it was already in the group
     */
    public boolean addNode(long nodeId) {
        getLock().writeLock().lock();
        try {
            return nodeIds.add(nodeId);
        } finally {
            getLock().writeLock().unlock();
        }
    }

    /**
     * Remove a compute node from this group.
     * @param nodeId the ID of the compute node
     * @return true if the node was removed, false if it was not in the group
     */
    public boolean removeNode(long nodeId) {
        getLock().writeLock().lock();
        try {
            return nodeIds.remove(nodeId);
        } finally {
            getLock().writeLock().unlock();
        }
    }

    /**
     * Check if a compute node is in this group.
     * @param nodeId the ID of the compute node
     * @return true if the node is in this group
     */
    public boolean containsNode(long nodeId) {
        getLock().readLock().lock();
        try {
            return nodeIds.contains(nodeId);
        } finally {
            getLock().readLock().unlock();
        }
    }

    /**
     * Get all node IDs in this group.
     * @return an unmodifiable set of node IDs
     */
    public Set<Long> getNodeIds() {
        getLock().readLock().lock();
        try {
            return Collections.unmodifiableSet(Sets.newHashSet(nodeIds));
        } finally {
            getLock().readLock().unlock();
        }
    }

    /**
     * Get the number of nodes in this group.
     * @return the node count
     */
    public int getNodeCount() {
        getLock().readLock().lock();
        try {
            return nodeIds.size();
        } finally {
            getLock().readLock().unlock();
        }
    }

    /**
     * Check if this is the default group.
     * @return true if this is the default group
     */
    public boolean isDefaultGroup() {
        return DEFAULT_GROUP_NAME.equals(name);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static CnGroup read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CnGroup.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CnGroup cnGroup = (CnGroup) o;
        return id == cnGroup.id && Objects.equals(name, cnGroup.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return "CnGroup{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", nodeCount=" + getNodeCount() +
                ", comment='" + comment + '\'' +
                '}';
    }
}
