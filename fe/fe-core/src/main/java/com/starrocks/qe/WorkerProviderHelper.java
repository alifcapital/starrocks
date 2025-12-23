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
import com.starrocks.warehouse.cngroup.CnGroup;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
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
        String effectiveGroupName = CnGroup.getEffectiveName(cnGroupName);

        ImmutableMap<Long, C> filtered = ImmutableMap.copyOf(
                nodes.entrySet().stream()
                        .filter(e -> {
                            String nodeGroup = CnGroup.getEffectiveName(e.getValue().getCnGroupName());
                            return effectiveGroupName.equals(nodeGroup);
                        })
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        if (nodes.isEmpty()) {
            return filtered;
        }

        if (filtered.isEmpty()) {
            // Error-level signal: requested group has no nodes (will likely fail scheduling).
            LOG.warn("filterByCnGroup: requested='{}', effective='{}', input={} nodes, output=0. Input groups: {}",
                    cnGroupName, effectiveGroupName, nodes.size(), groupCounts(nodes));
        } else if (filtered.size() < nodes.size() && LOG.isDebugEnabled()) {
            LOG.debug("filterByCnGroup: requested='{}', effective='{}', input={} nodes, output={} nodes. Input groups: {}",
                    cnGroupName, effectiveGroupName, nodes.size(), filtered.size(), groupCounts(nodes));
        }

        return filtered;
    }

    private static <C extends ComputeNode> Map<String, Long> groupCounts(ImmutableMap<Long, C> nodes) {
        if (nodes.isEmpty()) {
            return Collections.emptyMap();
        }
        // Keep it compact and stable (no node list), good for logs.
        return nodes.values().stream()
                .map(n -> CnGroup.getEffectiveName(n.getCnGroupName()))
                .collect(Collectors.groupingBy(Function.identity(), HashMap::new, Collectors.counting()));
    }
}
