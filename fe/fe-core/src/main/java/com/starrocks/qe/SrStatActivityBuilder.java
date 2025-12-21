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

import com.google.common.collect.Lists;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.common.proc.CurrentQueryInfoProvider;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TGetSrStatActivityItem;
import com.starrocks.thrift.TGetSrStatActivityRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Builder for sr_stat_activity system table data.
 * Combines connection info with query statistics to provide a unified view.
 */
public class SrStatActivityBuilder {
    private static final Logger LOG = LogManager.getLogger(SrStatActivityBuilder.class);

    public static List<TGetSrStatActivityItem> build(TGetSrStatActivityRequest request) {
        List<TGetSrStatActivityItem> items = Lists.newArrayList();

        try {
            // Get query statistics from BE via BRPC
            Map<String, QueryStatisticsItem> queryStats = QeProcessorImpl.INSTANCE.getQueryStatistics();
            CurrentQueryInfoProvider provider = new CurrentQueryInfoProvider();
            Map<String, CurrentQueryInfoProvider.QueryStatistics> statisticsMap =
                    provider.getQueryStatistics(queryStats.values());

            // Get requesting user info for authorization
            String requestingUser = getRequestingUser(request);
            boolean hasOperatePrivilege = checkOperatePrivilege();

            // Get all connections
            List<ConnectContext.ThreadInfo> connections = ExecuteEnv.getInstance().getScheduler()
                    .listConnection(null, null);

            String feIp = FrontendOptions.getLocalHostAddress();
            long nowMs = System.currentTimeMillis();

            for (ConnectContext.ThreadInfo threadInfo : connections) {
                ConnectContext ctx = threadInfo.getConnectContext();
                if (ctx == null) {
                    continue;
                }

                // Authorization check: users can only see their own connections
                // unless they have OPERATE privilege
                String connectionUser = ctx.getQualifiedUser();
                if (!hasOperatePrivilege && requestingUser != null &&
                        !requestingUser.equals(connectionUser)) {
                    continue;
                }

                TGetSrStatActivityItem item = new TGetSrStatActivityItem();

                // Identification
                item.setFe_ip(feIp);
                item.setConnection_id(ctx.getConnectionId());

                String queryId = ctx.getQueryId() != null ? ctx.getQueryId().toString() : "";
                item.setQuery_id(queryId);
                item.setCustom_query_id(ctx.getCustomQueryId() != null ? ctx.getCustomQueryId() : "");

                // User and context
                item.setUser(nullToEmpty(ctx.getQualifiedUser()));
                item.setCatalog(nullToEmpty(ctx.getCurrentCatalog()));
                item.setDb(nullToEmpty(ctx.getDatabase()));
                item.setWarehouse(nullToEmpty(ctx.getCurrentWarehouseName()));
                // Get cngroup from session variable or query item
                String cngroup = "";
                if (ctx.getSessionVariable() != null) {
                    cngroup = nullToEmpty(ctx.getSessionVariable().getCnGroupName());
                }
                item.setCngroup(cngroup);
                item.setResource_group(ctx.getResourceGroup() != null ? ctx.getResourceGroup().getName() : "");

                // State - unified state based on command and query state
                String state = computeState(ctx);
                item.setState(state);

                // Timestamps
                item.setConnect_time(ctx.getConnectionStartTime());

                // Query info and metrics
                QueryStatisticsItem queryItem = queryStats.get(queryId);
                CurrentQueryInfoProvider.QueryStatistics stats = statisticsMap.get(queryId);

                if (queryItem != null) {
                    item.setQuery_start_time(queryItem.getQueryStartTime());
                    item.setExec_time_ms(nowMs - queryItem.getQueryStartTime());
                    item.setQuery(truncateQuery(queryItem.getSql(), 65535));
                } else {
                    item.setQuery_start_time(0L);
                    item.setExec_time_ms(0L);
                    item.setQuery("");
                }

                if (stats != null) {
                    item.setCpu_cost_ns(stats.getCpuCostNs());
                    item.setScan_bytes(stats.getScanBytes());
                    item.setScan_rows(stats.getScanRows());
                    item.setMem_usage_bytes(stats.getMemUsageBytes());
                    item.setSpill_bytes(stats.getSpillBytes());
                    item.setProgress_pct(stats.getProgressPercent());
                    item.setResult_sink_state(stats.getResultSinkStateString());
                } else {
                    item.setCpu_cost_ns(0L);
                    item.setScan_bytes(0L);
                    item.setScan_rows(0L);
                    item.setMem_usage_bytes(0L);
                    item.setSpill_bytes(0L);
                    item.setProgress_pct(0.0);
                    item.setResult_sink_state("");
                }

                items.add(item);
            }
        } catch (Exception e) {
            LOG.warn("Failed to build sr_stat_activity", e);
        }

        return items;
    }

    /**
     * Compute unified STATE based on connection context.
     * Maps: idle, queued, active, finished, failed, cancelled
     */
    private static String computeState(ConnectContext ctx) {
        // Check if connection is idle (no active command)
        if (ctx.getCommand() == MysqlCommand.COM_SLEEP) {
            return "idle";
        }

        // Check if query is pending (in queue)
        if (ctx.isPending()) {
            return "queued";
        }

        // Check query state
        QueryState queryState = ctx.getState();
        if (queryState != null) {
            if (queryState.isError()) {
                return "failed";
            }
            // Check if cancelled
            if (ctx.isKilled()) {
                return "cancelled";
            }
        }

        // If there's an active query, it's running
        if (ctx.getQueryId() != null) {
            return "active";
        }

        // Default to idle
        return "idle";
    }

    private static String truncateQuery(String query, int maxLen) {
        if (query == null) {
            return "";
        }
        if (query.length() <= maxLen) {
            return query;
        }
        return query.substring(0, maxLen - 3) + "...";
    }

    private static String nullToEmpty(String s) {
        return s != null ? s : "";
    }

    /**
     * Extract the requesting user from the request's auth_info.
     */
    private static String getRequestingUser(TGetSrStatActivityRequest request) {
        if (request == null || !request.isSetAuth_info()) {
            return null;
        }
        TAuthInfo authInfo = request.getAuth_info();
        if (authInfo.isSetCurrent_user_ident()) {
            return authInfo.getCurrent_user_ident().getUsername();
        }
        // Fallback to deprecated user field
        if (authInfo.isSetUser()) {
            return authInfo.getUser();
        }
        return null;
    }

    /**
     * Check if the current context (if any) has OPERATE privilege.
     * This allows admins to see all connections.
     */
    private static boolean checkOperatePrivilege() {
        ConnectContext currentContext = ConnectContext.get();
        if (currentContext == null) {
            return false;
        }
        try {
            Authorizer.checkSystemAction(currentContext, PrivilegeType.OPERATE);
            return true;
        } catch (AccessDeniedException e) {
            return false;
        }
    }
}
