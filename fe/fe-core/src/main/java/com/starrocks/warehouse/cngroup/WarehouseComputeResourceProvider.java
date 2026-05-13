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

import com.google.common.collect.Lists;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@code WarehouseComputeResourceProvider} is responsible for providing warehouse compute node resources and
 * associated operations.
 */
public final class WarehouseComputeResourceProvider implements ComputeResourceProvider {
    private static final Logger LOG = LogManager.getLogger(WarehouseComputeResourceProvider.class);

    public WarehouseComputeResourceProvider() {
        // No-op
    }

    @Override
    public ComputeResource ofComputeResource(long warehouseId, long workerGroupId) {
        return WarehouseComputeResource.of(warehouseId);
    }

    @Override
    public List<ComputeResource> getComputeResources(Warehouse warehouse) {
        if (warehouse == null) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE, "warehouse is null");
        }
        return Lists.newArrayList(WarehouseComputeResource.of(warehouse.getId()));
    }

    @Override
    public Optional<ComputeResource> acquireComputeResource(Warehouse warehouse, CRAcquireContext acquireContext) {
        final long warehouseId = acquireContext.getWarehouseId();
        if (warehouse == null) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE,
                    String.format("id: %d", warehouseId));
        }
        // Use cnGroupName from prevComputeResource if available
        ComputeResource prevResource = acquireContext.getPrevComputeResource();
        String cnGroupName = prevResource != null ? prevResource.getCnGroupName() : null;

        // ALL_GROUPS ("*") is reserved for internal system tasks (forSystemTask). Treat any
        // user-supplied "*" as default so workload isolation can't be bypassed via SET or
        // user property.
        if (CnGroupComputeResource.ALL_GROUPS.equals(cnGroupName)) {
            cnGroupName = null;
        }

        ComputeResource computeResource;
        if (cnGroupName != null && !cnGroupName.equals(CnGroupComputeResource.DEFAULT_GROUP_NAME)) {
            // User specified a custom cngroup via SET cngroup='xxx'
            computeResource = CnGroupComputeResource.of(warehouseId, cnGroupName);
        } else {
            computeResource = WarehouseComputeResource.of(warehouseId);
        }

        if (!isResourceAvailable(computeResource)) {
            String effectiveCnGroup = CnGroupComputeResource.getEffectiveName(cnGroupName);
            if (!CnGroupComputeResource.DEFAULT_GROUP_NAME.equals(effectiveCnGroup)) {
                // User specified a custom cngroup that has no available nodes
                throw ErrorReportException.report(ErrorCode.ERR_WAREHOUSE_UNAVAILABLE,
                        String.format("%s (cngroup '%s' has no available compute nodes)",
                                warehouse.getName(), effectiveCnGroup));
            }
            LOG.warn("No alive compute nodes in warehouse '{}'", warehouse.getName());
            return Optional.empty();
        }
        return Optional.of(computeResource);
    }

    /**
     * TODO: Add a blacklist cache to avoid time-consuming alive check
     */
    @Override
    public boolean isResourceAvailable(ComputeResource computeResource) {
        if (!RunMode.isSharedDataMode()) {
            return true;
        }
        try {
            final long availableEligibleSize =
                    Optional.ofNullable(getAliveEligibleComputeNodes(computeResource)).map(List::size).orElse(0);
            return availableEligibleSize > 0;
        } catch (Exception e) {
            LOG.warn("Failed to get alive compute nodes from starMgr : {}", e.getMessage());
            return false;
        }
    }

    /**
     * Universe: every node StarOS knows about in this resource's workerGroup,
     * without cnGroup filtering. Storage-view callers.
     */
    @Override
    public List<Long> getWorkerGroupComputeNodeIds(ComputeResource computeResource) {
        return fetchWorkerGroupNodeIds(computeResource);
    }

    @Override
    public List<ComputeNode> getAliveWorkerGroupComputeNodes(ComputeResource computeResource) {
        return aliveNodesFromIds(fetchWorkerGroupNodeIds(computeResource));
    }

    /**
     * Eligibility: workerGroup universe filtered by the resource's cnGroupName.
     * Compute-scheduling callers. This is the single point where cnGroup isolation
     * is enforced.
     */
    @Override
    public List<Long> getEligibleComputeNodeIds(ComputeResource computeResource) {
        List<Long> universe = fetchWorkerGroupNodeIds(computeResource);
        if (universe.isEmpty()) {
            return universe;
        }
        String effectiveCnGroupName =
                CnGroupComputeResource.getEffectiveName(computeResource.getCnGroupName());
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        return universe.stream()
                .filter(nodeId -> {
                    ComputeNode node = systemInfoService.getBackendOrComputeNode(nodeId);
                    return node != null && matchesCnGroup(node, effectiveCnGroupName);
                })
                .collect(Collectors.toList());
    }

    @Override
    public List<ComputeNode> getAliveEligibleComputeNodes(ComputeResource computeResource) {
        return aliveNodesFromIds(getEligibleComputeNodeIds(computeResource));
    }

    private List<Long> fetchWorkerGroupNodeIds(ComputeResource computeResource) {
        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getWorkersByWorkerGroup(computeResource.getWorkerGroupId());
        } catch (StarRocksException e) {
            LOG.warn("Fail to get compute node ids from starMgr : {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    private List<ComputeNode> aliveNodesFromIds(List<Long> nodeIds) {
        if (CollectionUtils.isEmpty(nodeIds)) {
            return Lists.newArrayList();
        }
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        return nodeIds.stream()
                .map(systemInfoService::getBackendOrComputeNode)
                .filter(node -> node != null && node.isAlive())
                .collect(Collectors.toList());
    }

    /**
     * Check if a compute node matches the specified CNGroup.
     * Nodes with null/empty cnGroupName are considered to be in "default" group.
     */
    private boolean matchesCnGroup(ComputeNode node, String cnGroupName) {
        String nodeGroup = CnGroupComputeResource.getEffectiveName(node.getCnGroupName());
        return cnGroupName.equals(nodeGroup);
    }
}
