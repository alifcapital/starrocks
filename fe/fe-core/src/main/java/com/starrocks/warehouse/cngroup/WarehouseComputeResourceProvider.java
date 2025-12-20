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

        ComputeResource computeResource;
        if (cnGroupName != null && !cnGroupName.equals(CnGroup.DEFAULT_GROUP_NAME)) {
            // User specified a custom cngroup via SET cngroup='xxx'
            computeResource = CnGroupComputeResource.of(warehouseId, cnGroupName);
        } else {
            computeResource = WarehouseComputeResource.of(warehouseId);
        }

        if (!isResourceAvailable(computeResource)) {
            LOG.warn("No alive compute nodes in warehouse '{}' for cngroup '{}'",
                    warehouse.getName(), cnGroupName);
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
            final long availableWorkerGroupIdSize =
                    Optional.ofNullable(getAliveComputeNodes(computeResource)).map(List::size).orElse(0);
            return availableWorkerGroupIdSize > 0;
        } catch (Exception e) {
            LOG.warn("Failed to get alive compute nodes from starMgr : {}", e.getMessage());
            return false;
        }
    }

    @Override
    public List<Long> getAllComputeNodeIds(ComputeResource computeResource) {
        try {
            long workerGroupId = computeResource.getWorkerGroupId();
            List<Long> allNodeIds = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getWorkersByWorkerGroup(workerGroupId);

            // Filter by CNGroup (null/empty treated as "default", only "*" skips filtering)
            String cnGroupName = computeResource.getCnGroupName();
            if (shouldFilterByCnGroup(cnGroupName)) {
                String effectiveCnGroupName = CnGroup.getEffectiveName(cnGroupName);
                SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

                List<Long> filteredIds = allNodeIds.stream()
                        .filter(nodeId -> {
                            ComputeNode node = systemInfoService.getBackendOrComputeNode(nodeId);
                            return node != null && matchesCnGroup(node, effectiveCnGroupName);
                        })
                        .collect(Collectors.toList());
                return filteredIds;
            }

            // Only reaches here when cnGroupName == "*" (ALL_GROUPS)
            return allNodeIds;
        } catch (StarRocksException e) {
            LOG.warn("Fail to get compute node ids from starMgr : {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    @Override
    public List<ComputeNode> getAliveComputeNodes(ComputeResource computeResource) {
        List<Long> computeNodeIds = getAllComputeNodeIds(computeResource);
        if (CollectionUtils.isEmpty(computeNodeIds)) {
            return Lists.newArrayList();
        }
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<ComputeNode> nodes = computeNodeIds.stream()
                .map(id -> systemInfoService.getBackendOrComputeNode(id))
                .filter(node -> node != null && node.isAlive())
                .collect(Collectors.toList());
        return nodes;
    }

    /**
     * Check if CNGroup filtering should be applied.
     * Returns false only for "*" (ALL_GROUPS) which means use all nodes.
     * null/empty are treated as "default" group and filtering IS applied.
     */
    private boolean shouldFilterByCnGroup(String cnGroupName) {
        return !CnGroupComputeResource.ALL_GROUPS.equals(cnGroupName);
    }

    /**
     * Check if a compute node matches the specified CNGroup.
     * Nodes with null/empty cnGroupName are considered to be in "default" group.
     */
    private boolean matchesCnGroup(ComputeNode node, String cnGroupName) {
        String nodeGroup = CnGroup.getEffectiveName(node.getCnGroupName());
        return cnGroupName.equals(nodeGroup);
    }
}
