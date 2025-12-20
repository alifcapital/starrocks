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

package com.starrocks.qe;

import com.google.common.collect.ImmutableMap;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.stream.Collectors;

public class WorkerProviderHelper {
    private static final Logger LOG = LogManager.getLogger(WorkerProviderHelper.class);

    public interface NextWorkerIndexSupplier {
        int getAsInt(ComputeResource computeResource);
    }

    public static <C extends ComputeNode> C getNextWorker(ImmutableMap<Long, C> workers,
                                                          NextWorkerIndexSupplier getNextWorkerNodeIndex,
                                                          ComputeResource computeResource) {
        if (workers.isEmpty()) {
            return null;
        }
        int index = getNextWorkerNodeIndex.getAsInt(computeResource) % workers.size();
        if (index < 0) {
            index = -index;
        }
        return workers.values().asList().get(index);
    }

    /**
     * Filter compute nodes by CnGroup name.
     *
     * @param nodes the input map of node ID to ComputeNode
     * @param cnGroupName the CnGroup name to filter by. If null or empty, uses "default" group.
     * @return a new ImmutableMap containing only nodes that belong to the specified group
     */
    public static <C extends ComputeNode> ImmutableMap<Long, C> filterByCnGroup(
            ImmutableMap<Long, C> nodes, String cnGroupName) {
        // If cnGroupName is null or empty, use default group
        String effectiveGroupName = (cnGroupName == null || cnGroupName.isEmpty())
                ? "default" : cnGroupName;

        ImmutableMap<Long, C> filtered = ImmutableMap.copyOf(
                nodes.entrySet().stream()
                        .filter(e -> {
                            String nodeGroup = e.getValue().getCnGroupName();
                            // Treat null/empty cnGroupName on node as "default"
                            if (nodeGroup == null || nodeGroup.isEmpty()) {
                                nodeGroup = "default";
                            }
                            return effectiveGroupName.equals(nodeGroup);
                        })
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        // Always log when filtering results in fewer nodes (helps debug CNGroup issues)
        if (filtered.size() < nodes.size() || filtered.isEmpty()) {
            LOG.warn("filterByCnGroup: requested='{}', effective='{}', input={} nodes, output={} nodes. " +
                            "Input node groups: {}",
                    cnGroupName, effectiveGroupName, nodes.size(), filtered.size(),
                    nodes.values().stream()
                            .map(n -> n.getId() + ":" + n.getCnGroupName())
                            .collect(java.util.stream.Collectors.joining(", ")));
        }

        return filtered;
    }
}
