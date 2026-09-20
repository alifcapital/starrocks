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

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Loads the statistics of given partitions of an external table from
 * _statistics_.external_column_statistics: one query per table for all the partitions asked for
 * at once, one cache entry per partition.
 */
public class ExternalPartitionStatsCacheLoader
        implements AsyncCacheLoader<ExternalPartitionStatsKey, Optional<Map<String, ExternalPartitionStatistics.ColumnStats>>> {
    private static final Logger LOG = LogManager.getLogger(ExternalPartitionStatsCacheLoader.class);

    private final StatisticExecutor statisticExecutor = new StatisticExecutor();

    @Override
    public @NonNull CompletableFuture<Optional<Map<String, ExternalPartitionStatistics.ColumnStats>>> asyncLoad(
            @NonNull ExternalPartitionStatsKey key, @NonNull Executor executor) {
        return asyncLoadAll(List.of(key), executor).thenApply(loaded -> loaded.getOrDefault(key, Optional.empty()));
    }

    @Override
    public @NonNull CompletableFuture<Map<@NonNull ExternalPartitionStatsKey,
            @NonNull Optional<Map<String, ExternalPartitionStatistics.ColumnStats>>>> asyncLoadAll(
            @NonNull Iterable<? extends @NonNull ExternalPartitionStatsKey> keys, @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            Map<ExternalPartitionStatsKey, Optional<Map<String, ExternalPartitionStatistics.ColumnStats>>> result =
                    new HashMap<>();
            Map<String, List<String>> partitionsByTable = new HashMap<>();
            for (ExternalPartitionStatsKey key : keys) {
                // A partition without statistics stays empty, so it is not asked for again.
                result.put(key, Optional.empty());
                partitionsByTable.computeIfAbsent(key.tableUUID, k -> new ArrayList<>()).add(key.partitionName);
            }
            if (FeConstants.enableUnitStatistics) {
                return result;
            }
            try {
                ConnectContext connectContext = StatisticUtils.buildConnectContext();
                connectContext.setThreadLocalInfo();
                for (Map.Entry<String, List<String>> entry : partitionsByTable.entrySet()) {
                    ExternalPartitionStatistics loaded = parse(statisticExecutor.queryExternalPartitionStatistics(
                            connectContext, entry.getKey(), entry.getValue()));
                    for (String partitionName : loaded.getPartitionNames()) {
                        result.put(new ExternalPartitionStatsKey(entry.getKey(), partitionName),
                                Optional.of(loaded.getPartition(partitionName)));
                    }
                }
                return result;
            } catch (RuntimeException e) {
                LOG.error("Failed to load external partition statistics of {}", partitionsByTable.keySet(), e);
                throw new CompletionException(e);
            } catch (Exception e) {
                throw new CompletionException(e);
            } finally {
                ConnectContext.remove();
            }
        }, executor);
    }

    @Override
    public @NonNull CompletableFuture<Optional<Map<String, ExternalPartitionStatistics.ColumnStats>>> asyncReload(
            @NonNull ExternalPartitionStatsKey key,
            @NonNull Optional<Map<String, ExternalPartitionStatistics.ColumnStats>> oldValue,
            @NonNull Executor executor) {
        return asyncLoad(key, executor);
    }

    // A row is [partition_name, column_name, row_count, ndv, null_count, min, max];
    // see StatisticSQLBuilder.buildQueryExternalPartitionStatisticsSQL.
    static ExternalPartitionStatistics parse(List<List<String>> rows) {
        Map<String, Map<String, ExternalPartitionStatistics.ColumnStats>> partitions = new HashMap<>();
        for (List<String> row : rows) {
            if (row.size() < 7 || row.get(0) == null || row.get(1) == null) {
                continue;
            }
            try {
                ExternalPartitionStatistics.ColumnStats stats = new ExternalPartitionStatistics.ColumnStats(
                        parseLong(row.get(2)), parseLong(row.get(3)), parseLong(row.get(4)),
                        row.get(5) == null ? "" : row.get(5), row.get(6) == null ? "" : row.get(6));
                partitions.computeIfAbsent(row.get(0), k -> new HashMap<>()).put(row.get(1), stats);
            } catch (NumberFormatException e) {
                LOG.warn("Ignore malformed external partition statistics row {}", row);
            }
        }
        return partitions.isEmpty() ? ExternalPartitionStatistics.EMPTY : new ExternalPartitionStatistics(partitions);
    }

    private static long parseLong(String text) {
        return text == null ? 0 : Long.parseLong(text);
    }
}
