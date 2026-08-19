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

package com.starrocks.warehouse;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcDirInterface;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.QueryStatisticsInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WarehouseProcDir implements ProcDirInterface {
    public static final ImmutableList<String> WAREHOUSE_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Id")
            .add("Name")
            .add("State")
            .add("NodeCount")
            .add("CurrentClusterCount")
            .add("MaxClusterCount")
            .add("StartedClusters")
            .add("RunningSql")
            .add("QueuedSql")
            .add("CreatedOn")
            .add("ResumedOn")
            .add("UpdatedOn")
            .add("Property")
            .add("Comment")
            .build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return true;
    }

    @Override
    public ProcNodeInterface lookup(String idOrName) throws AnalysisException {
        if (Strings.isNullOrEmpty(idOrName)) {
            throw new AnalysisException("Warehouse id or name is null or empty.");
        }
        Warehouse warehouse;
        try {
            warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(Long.parseLong(idOrName));
        } catch (NumberFormatException e) {
            warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(idOrName);
        }
        if (warehouse == null) {
            throw new AnalysisException("Unknown warehouse id or name \"" + idOrName + ".\"");
        }

        Warehouse finalWarehouse = warehouse;
        return () -> buildResult(List.of(finalWarehouse));
    }

    @Override
    public ProcResult fetchResult() {
        return buildResult(GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllWarehouses());
    }

    public static BaseProcResult buildResult(List<Warehouse> warehouses) {
        BaseProcResult result = new BaseProcResult();
        result.setNames(WAREHOUSE_PROC_NODE_TITLE_NAMES);
        Map<String, long[]> counts = new HashMap<>();
        QeProcessorImpl.INSTANCE.getQueryStatistics().values().forEach(query ->
                countQuery(counts, query.getWarehouseName(), query.getExecState()));
        for (QueryStatisticsInfo query : GlobalStateMgr.getCurrentState().getNodeMgr()
                .getQueryStatisticsInfoFromOtherFEs(false)) {
            countQuery(counts, query.getWareHouseName(), query.getExecState());
        }
        for (Warehouse warehouse : warehouses) {
            List<String> row = warehouse.getWarehouseInfo();
            long[] count = counts.getOrDefault(warehouse.getName(), new long[2]);
            row.set(7, Long.toString(count[0]));
            row.set(8, Long.toString(count[1]));
            result.addRow(row);
        }
        return result;
    }

    private static void countQuery(Map<String, long[]> counts, String warehouse, String state) {
        long[] count = counts.computeIfAbsent(warehouse, key -> new long[2]);
        if ("PENDING".equals(state)) {
            count[1]++;
        } else if ("RUNNING".equals(state)) {
            count[0]++;
        }
    }

    public static List<ComputeNode> getNodes(Warehouse warehouse) {
        List<ComputeNode> nodes = new ArrayList<>();
        SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        if (clusterInfo == null) {
            return nodes;
        }
        for (ComputeNode node : clusterInfo.getComputeNodes()) {
            if (node.getWarehouseId() == warehouse.getId()) {
                nodes.add(node);
            }
        }
        for (ComputeNode node : clusterInfo.getBackends()) {
            if (node.getWarehouseId() == warehouse.getId()) {
                nodes.add(node);
            }
        }
        return nodes;
    }

    public static List<List<String>> getNodesInfo(Warehouse warehouse) {
        List<List<String>> rows = new ArrayList<>();
        for (ComputeNode node : getNodes(warehouse)) {
            long workerId = -1;
            try {
                workerId = GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkerIdByNodeId(node.getId());
            } catch (Exception ignored) {
                // A worker ID is unavailable before its first starlet heartbeat.
            }
            rows.add(Lists.newArrayList(
                    warehouse.getName(), "", String.valueOf(node.getWorkerGroupId()),
                    String.valueOf(node.getId()), String.valueOf(workerId), node.getHost(),
                    String.valueOf(node.getHeartbeatPort()), String.valueOf(node.getBePort()),
                    String.valueOf(node.getHttpPort()), String.valueOf(node.getBrpcPort()),
                    String.valueOf(node.getStarletPort()), TimeUtils.longToTimeString(node.getLastStartTime()),
                    TimeUtils.longToTimeString(node.getLastUpdateMs()), String.valueOf(node.isAlive()),
                    Strings.nullToEmpty(node.getHeartbeatErrMsg()), Strings.nullToEmpty(node.getVersion()),
                    String.valueOf(node.getNumRunningQueries()), String.valueOf(node.getCpuCores()),
                    String.format("%.2f", node.getMemUsedPct() * 100),
                    String.format("%.2f", node.getCpuUsedPermille() / 10.0), ""));
        }
        return rows;
    }
}
