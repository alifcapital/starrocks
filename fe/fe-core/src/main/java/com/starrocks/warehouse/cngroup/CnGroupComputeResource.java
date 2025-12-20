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

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/**
 * {@code CnGroupComputeResource} represents a compute node resource associated with
 * a specific warehouse AND a specific CNGroup for workload isolation.
 *
 * When queries use this resource, they will only be scheduled on compute nodes
 * that belong to the specified CNGroup.
 */
public final class CnGroupComputeResource extends WarehouseComputeResource {

    /**
     * Special constant to indicate all nodes should be used (no CNGroup filtering).
     * System tasks use this to ensure they can run on any available node.
     */
    public static final String ALL_GROUPS = "*";

    @SerializedName("cnGroupName")
    private final String cnGroupName;

    public CnGroupComputeResource(long warehouseId, String cnGroupName) {
        super(warehouseId);
        this.cnGroupName = cnGroupName;
    }

    public static CnGroupComputeResource of(long warehouseId, String cnGroupName) {
        return new CnGroupComputeResource(warehouseId, cnGroupName);
    }

    /**
     * Create a compute resource for system tasks that should use ALL nodes.
     */
    public static CnGroupComputeResource forSystemTask(long warehouseId) {
        return new CnGroupComputeResource(warehouseId, ALL_GROUPS);
    }

    @Override
    public String getCnGroupName() {
        return cnGroupName;
    }

    /**
     * Check if this resource should use all nodes (no CNGroup filtering).
     */
    public boolean isAllGroups() {
        return ALL_GROUPS.equals(cnGroupName) || cnGroupName == null;
    }

    @Override
    public String toString() {
        return "{warehouseId=" + getWarehouseId() + ", cnGroupName=" + cnGroupName + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(getWarehouseId(), cnGroupName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CnGroupComputeResource)) {
            return false;
        }
        CnGroupComputeResource other = (CnGroupComputeResource) obj;
        return getWarehouseId() == other.getWarehouseId() &&
                Objects.equals(cnGroupName, other.cnGroupName);
    }
}
