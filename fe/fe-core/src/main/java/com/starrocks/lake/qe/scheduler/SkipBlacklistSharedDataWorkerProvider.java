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

package com.starrocks.lake.qe.scheduler;

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.qe.SessionVariableConstants.BlacklistBackupRoutingPolicy;
import com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * SkipBlacklistSharedDataWorkerProvider extends DefaultSharedDataWorkerProvider and skips backend blacklist verification.
 * This provider filters workers based only on availability (alive status), ignoring the blacklist.
 */
public class SkipBlacklistSharedDataWorkerProvider extends DefaultSharedDataWorkerProvider {
    private static final Logger LOG = LogManager.getLogger(SkipBlacklistSharedDataWorkerProvider.class);

    public static class Factory implements WorkerProvider.Factory {
        private final BlacklistBackupRoutingPolicy blacklistBackupRoutingPolicy;

        public Factory() {
            // use default blacklist backup routing policy
            this(BlacklistBackupRoutingPolicy.getDefault());
        }

        public Factory(BlacklistBackupRoutingPolicy blacklistBackupRoutingPolicy) {
            this.blacklistBackupRoutingPolicy = blacklistBackupRoutingPolicy;
        }

        @Override
        public SkipBlacklistSharedDataWorkerProvider captureAvailableWorkers(
                SystemInfoService systemInfoService,
                boolean preferComputeNode,
                int numUsedComputeNodes,
                ComputationFragmentSchedulingPolicy computationFragmentSchedulingPolicy,
                ComputeResource computeResource) {

            final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            final SystemInfoService clusterInfo =
                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

            // Universe for shard-owner visibility (selectBackupWorker guard).
            final List<Long> universeNodeIds = warehouseManager.getWorkerGroupComputeNodeIds(computeResource);
            final ImmutableMap.Builder<Long, ComputeNode> universeBuilder = ImmutableMap.builder();
            universeNodeIds.forEach(nodeId -> {
                ComputeNode node = clusterInfo.getBackendOrComputeNode(nodeId);
                if (node != null) {
                    universeBuilder.put(nodeId, node);
                }
            });
            ImmutableMap<Long, ComputeNode> idToComputeNode = universeBuilder.build();

            // Eligibility-alive for compute scheduling. Blacklist intentionally NOT applied here —
            // that is the whole point of this provider.
            final ImmutableMap.Builder<Long, ComputeNode> eligibleBuilder = ImmutableMap.builder();
            warehouseManager.getAliveEligibleComputeNodes(computeResource).forEach(node -> {
                if (node != null) {
                    eligibleBuilder.put(node.getId(), node);
                }
            });
            ImmutableMap<Long, ComputeNode> availableComputeNodes = eligibleBuilder.build();

            if (LOG.isDebugEnabled()) {
                LOG.debug("SkipBlacklistSharedDataWorkerProvider - idToComputeNode (universe): {}, "
                                + "availableComputeNodes (eligible+alive): {}",
                        idToComputeNode, availableComputeNodes);
            }

            if (availableComputeNodes.isEmpty()) {
                Warehouse warehouse = warehouseManager.getWarehouse(computeResource.getWarehouseId());
                throw ErrorReportException.report(ErrorCode.ERR_NO_NODES_IN_WAREHOUSE, warehouse.getName());
            }

            return new SkipBlacklistSharedDataWorkerProvider(idToComputeNode, availableComputeNodes, computeResource,
                    blacklistBackupRoutingPolicy);
        }
    }

    protected SkipBlacklistSharedDataWorkerProvider(ImmutableMap<Long, ComputeNode> id2ComputeNode,
                                             ImmutableMap<Long, ComputeNode> availableID2ComputeNode,
                                             ComputeResource computeResource,
                                             BlacklistBackupRoutingPolicy blacklistBackupRoutingPolicy) {
        super(id2ComputeNode, availableID2ComputeNode, computeResource, blacklistBackupRoutingPolicy);
    }

    /**
     * Same as {@link DefaultSharedDataWorkerProvider} but blocklist is not consulted for buddy eligibility,
     * consistent with initial worker selection when {@code skip_black_list} is enabled.
     */
    @Override
    protected boolean isBuddyEligibleForBackup(long buddyId, long workerId) {
        return buddyId != workerId && availableID2ComputeNode.containsKey(buddyId);
    }
}
