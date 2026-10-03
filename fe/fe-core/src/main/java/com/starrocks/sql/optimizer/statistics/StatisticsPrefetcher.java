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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Starts loads for all remaining external scans before synchronous estimation visits them one by one. */
public final class StatisticsPrefetcher {
    private static final Logger LOG = LogManager.getLogger(StatisticsPrefetcher.class);

    private StatisticsPrefetcher() {
    }

    public static void prefetch(OptExpression tree, OptimizerContext optimizerContext) {
        if (!Config.enable_sync_statistics_load) {
            return;
        }
        ConnectContext context = ConnectContext.get();
        if (context != null && (context.isStatisticsConnection() || context.isStatisticsJob())) {
            return;
        }
        List<LogicalScanOperator> scans = new ArrayList<>();
        Utils.extractOperator(tree, scans, op -> op instanceof LogicalScanOperator
                && !(op instanceof LogicalIcebergEqualityDeleteScanOperator));
        Map<String, Pair<Table, Set<String>>> requests = new LinkedHashMap<>();
        Set<ExternalStatisticsRequest> scoped = new LinkedHashSet<>();
        Map<String, Pair<ExternalStatisticsRequest, Set<String>>> wholeRequests = new LinkedHashMap<>();
        Map<String, Table> mcvTables = new LinkedHashMap<>();
        StatisticStorage storage = GlobalStateMgr.getCurrentState().getStatisticStorage();
        for (LogicalScanOperator scan : scans) {
            Table table = scan.getTable();
            if (!table.isAnalyzableExternalTable()
                    || MetadataMgr.isIncrementalIcebergScan(table, scan.getTvrVersionRange())) {
                continue;
            }
            try {
                List<PartitionKey> selected = null;
                if ((table.isHiveTable() || table.isHudiTable()) && scan.getScanOperatorPredicates().hasPrunedPartition()) {
                    selected = scan.getScanOperatorPredicates().getSelectedPartitionKeys();
                }
                ExternalStatisticsRequest scopedRequest = GlobalStateMgr.getCurrentState().getMetadataMgr()
                        .prepareExternalStatisticsRequest(optimizerContext, table.getCatalogName(), table,
                                scan.getColRefToColumnMetaMap(), selected, scan.getPredicate(), scan.getLimit(),
                                scan.getTvrVersionRange());
                if (scopedRequest != null) {
                    if (scopedRequest.wholeTable) {
                        wholeRequests.computeIfAbsent(table.getUUID(),
                                ignored -> Pair.create(scopedRequest, new LinkedHashSet<>()))
                                .second.addAll(scopedRequest.columns);
                        mcvTables.putIfAbsent(table.getUUID(), table);
                    } else {
                        // Keep restricted alias domains separate; merging them could introduce
                        // partition/column pairs that neither alias requested.
                        scoped.add(scopedRequest);
                    }
                    continue;
                }
                mcvTables.putIfAbsent(table.getUUID(), table);
                Pair<Table, Set<String>> request = requests.computeIfAbsent(table.getUUID(),
                        ignored -> Pair.create(table, new LinkedHashSet<>()));
                scan.getColRefToColumnMetaMap().forEach((ref, column) -> {
                    if (!FeNameFormat.FORBIDDEN_COLUMN_NAMES.contains(ref.getName())) {
                        request.second.add(column.getName());
                    }
                });
            } catch (Exception e) {
                // Prefetch is optional. A metadata failure must retain the ordinary estimator's fallback.
                LOG.warn("Failed to prepare statistics prefetch for {}", table.getName(), e);
            }
        }
        wholeRequests.values().forEach(request -> scoped.add(new ExternalStatisticsRequest(
                request.first.tableUUID, List.of(), request.second, true, request.first.unpartitioned)));
        for (ExternalStatisticsRequest request : scoped) {
            optimizerContext.getExternalStatisticsLoad(request, () -> storage.loadExternalStatistics(request));
        }
        // Merge self-join aliases before scheduling, so different column subsets form one batch per table.
        requests.values().forEach(request ->
                storage.prefetchConnectorTableStatistics(request.first, new ArrayList<>(request.second)));
        if (MultiColumnMcvEstimator.isEnabled()) {
            mcvTables.values().forEach(storage::prefetchExternalMcvStatistics);
        }
    }
}
