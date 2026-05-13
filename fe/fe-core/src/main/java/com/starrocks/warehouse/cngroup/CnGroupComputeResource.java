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
 * <p>When queries use this resource, they will only be scheduled on compute nodes
 * that belong to the specified CNGroup.
 *
 * <h2>Architecture: storage vs compute</h2>
 *
 * In shared-data mode there are two independent layers:
 *
 * <pre>
 * ┌───────────────────┬─────────────────────────┬────────────────────────────┐
 * │                   │ Storage (StarOS shards) │ Compute (query scheduling) │
 * ├───────────────────┼─────────────────────────┼────────────────────────────┤
 * │ Grouping unit     │ workerGroup             │ cnGroup (filter on top)    │
 * │ Aware of cnGroup? │ No                      │ Yes                        │
 * └───────────────────┴─────────────────────────┴────────────────────────────┘
 * </pre>
 *
 * <p>StarOS places shards across <b>all nodes of the workerGroup</b> — it has no
 * notion of cnGroup. cnGroup is a FE-only filter applied on top of the StarOS view.
 *
 * <p>Consequence: a tablet's primary owner can live in any cnGroup, regardless of
 * which cnGroup the session creating the tablet belongs to. The
 * {@link ComputeResourceProvider} API enforces this distinction at the type level —
 * see its class javadoc for which method picks which view.
 *
 * <p>Rule of thumb:
 * <ul>
 *   <li>"Where do I run this query?" → session resource +
 *       {@code getEligibleComputeNodeIds(...)} / {@code getAliveEligibleComputeNodes(...)}.</li>
 *   <li>"Who owns this shard / where do I send write coord?" → session resource +
 *       {@code getWorkerGroupComputeNodeIds(...)} /
 *       {@code getAliveWorkerGroupComputeNodes(...)}.</li>
 * </ul>
 *
 * The bypass lives in the <i>method name</i>, not in a sentinel resource value.
 */
public final class CnGroupComputeResource extends WarehouseComputeResource {

    /**
     * Reserved input sentinel — rejected for user-supplied DDL and sanitised back to
     * {@code null} (i.e. default) in {@code acquireComputeResource}. Kept only as a
     * validation constant; no internal call site should construct a
     * {@code CnGroupComputeResource} with this name.
     */
    public static final String ALL_GROUPS = "*";

    public static final String DEFAULT_GROUP_NAME = "default";

    public static String getEffectiveName(String cnGroupName) {
        return (cnGroupName == null || cnGroupName.isEmpty()) ? DEFAULT_GROUP_NAME : cnGroupName;
    }

    @SerializedName("cnGroupName")
    private final String cnGroupName;

    public CnGroupComputeResource(long warehouseId, String cnGroupName) {
        super(warehouseId);
        this.cnGroupName = cnGroupName;
    }

    public static CnGroupComputeResource of(long warehouseId, String cnGroupName) {
        return new CnGroupComputeResource(warehouseId, cnGroupName);
    }

    @Override
    public String getCnGroupName() {
        return cnGroupName;
    }

    /**
     * Check if this resource should use all nodes (no CNGroup filtering).
     */
    public boolean isAllGroups() {
        return ALL_GROUPS.equals(cnGroupName);
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
