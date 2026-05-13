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

import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;

import java.util.List;
import java.util.Optional;

/**
 * {@code ComputeResourceProvider} is responsible to provide an available compute resource for a job scheduler
 * based on the current warehouse's load status or strategy.
 *
 * <h2>Node enumeration: workerGroup universe vs cnGroup eligibility</h2>
 *
 * In shared-data mode the FE-only cnGroup filter sits on top of the StarOS workerGroup.
 * Two distinct views of the node set are required at different call sites, and each
 * has a dedicated method pair so callers must pick consciously:
 *
 * <ul>
 *   <li>{@link #getWorkerGroupComputeNodeIds(ComputeResource)} /
 *       {@link #getAliveWorkerGroupComputeNodes(ComputeResource)} — every node in the
 *       workerGroup, no cnGroup filter. Use when the caller has to reach a specific
 *       shard owner (write coordination, {@code nodes_info}, replica distribution,
 *       historical inventory, WorkerProvider's storage-view set, maintenance daemon
 *       acquire/capacity). StarOS shard placement is not cnGroup-aware, so universe
 *       is the only honest answer.
 *   <li>{@link #getEligibleComputeNodeIds(ComputeResource)} /
 *       {@link #getAliveEligibleComputeNodes(ComputeResource)} — workerGroup nodes
 *       filtered by the resource's cnGroupName. Use for compute scheduling
 *       decisions (scan fragment placement, query worker pool, isResourceAvailable,
 *       routine-load endpoint pool). This is where workload isolation is enforced.
 * </ul>
 */
public interface ComputeResourceProvider {
    /**
     * Get a ComputeResource by warehouseId and workGroupId.
     * @param warehouseId the id of the warehouse
     * @param workGroupId the id of the worker group
     * @return a ComputeResource that can be used for compute
     */
    ComputeResource ofComputeResource(long warehouseId, long workGroupId);

    /**
     * Get all ComputeResources by warehouse.
     * @param warehouse the warehouse to get the ComputeResources from
     * @return a list of ComputeResources that can be used for compute
     */
    List<ComputeResource> getComputeResources(Warehouse warehouse);

    /**
     * NOTE: prefer to call this infrequently, as it can come to dominate the execution time of a query in the
     *  frontend if there are many calls per request (e.g. one per partition when there are many partitions).
     *
     * @param warehouse: the warehouse to get the worker group from
     * @param acquireContext: the context to acquire the worker group
     * @return: an available ComputeResource for the warehouse by the strategy, or Optional.empty() if no available worker group
     * @throws RuntimeException : if the warehouse is invalid or there is no available worker group
     */
    Optional<ComputeResource> acquireComputeResource(Warehouse warehouse, CRAcquireContext acquireContext);

    /**
     * Check the resource is available or not; this method will not throw exception.
     * Availability is checked against the eligible (cnGroup-filtered) view —
     * "is there at least one alive node that compute scheduling can use".
     * @param computeResource: the ComputeResource to check
     * @return: true if the resource is available, false otherwise
     */
    boolean isResourceAvailable(ComputeResource computeResource);

    /**
     * Get every node in the workerGroup of the given resource, ignoring cnGroup
     * filtering. Use for storage-view / shard-owner visibility paths.
     * @param computeResource: the ComputeResource whose workerGroup will be enumerated
     * @return: a list of compute node ids in the workerGroup, empty if unavailable
     */
    List<Long> getWorkerGroupComputeNodeIds(ComputeResource computeResource);

    /**
     * Get alive nodes in the workerGroup of the given resource, ignoring cnGroup
     * filtering. Use for storage-view callers that also need an aliveness check
     * (e.g. {@code HistoricalNodeMgr.isResourceAvailable} or maintenance capacity calc).
     * @param computeResource: the ComputeResource whose workerGroup will be enumerated
     * @return: a list of alive compute nodes in the workerGroup, empty if unavailable
     */
    List<ComputeNode> getAliveWorkerGroupComputeNodes(ComputeResource computeResource);

    /**
     * Get cnGroup-filtered node ids for the given resource. Use for compute
     * scheduling decisions — only nodes eligible to run the session's workload.
     * @param computeResource: the ComputeResource carrying the cnGroupName filter
     * @return: a list of eligible compute node ids, empty if unavailable
     */
    List<Long> getEligibleComputeNodeIds(ComputeResource computeResource);

    /**
     * Get cnGroup-filtered alive compute nodes for the given resource. Use for
     * compute scheduling decisions that also need aliveness (worker provider's
     * available set, isResourceAvailable, routine-load endpoint selection).
     * @param computeResource: the ComputeResource carrying the cnGroupName filter
     * @return: a list of alive eligible compute nodes, empty if unavailable
     */
    List<ComputeNode> getAliveEligibleComputeNodes(ComputeResource computeResource);
}
