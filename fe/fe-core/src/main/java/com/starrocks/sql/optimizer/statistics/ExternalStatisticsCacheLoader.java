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
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Reads requested partition/column pairs from external_column_statistics in one query per table.
 * TABLE keys load one persisted scalar summary for the entire table.
 * PARTITION keys transport individual sketches; BLOCK keys aggregate exact member sets on BE.
 * Both use a separate bounded executor so partition traffic cannot queue ahead of native loads.
 */
public class ExternalStatisticsCacheLoader
        implements AsyncCacheLoader<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> {
    private static final Logger LOG = LogManager.getLogger(ExternalStatisticsCacheLoader.class);

    private final StatisticExecutor statisticExecutor = new StatisticExecutor();
    private final Executor partitionExecutor;

    public ExternalStatisticsCacheLoader() {
        this(null);
    }

    public ExternalStatisticsCacheLoader(Executor partitionExecutor) {
        this.partitionExecutor = partitionExecutor;
    }

    @Override
    public @NonNull CompletableFuture<Optional<ExternalColumnStatistics>> asyncLoad(
            @NonNull ExternalStatisticsCacheKey key, @NonNull Executor executor) {
        return asyncLoadAll(List.of(key), executor).thenApply(loaded -> loaded.getOrDefault(key, Optional.empty()));
    }

    @Override
    public @NonNull CompletableFuture<Map<@NonNull ExternalStatisticsCacheKey,
            @NonNull Optional<ExternalColumnStatistics>>> asyncLoadAll(
            @NonNull Iterable<? extends @NonNull ExternalStatisticsCacheKey> keys, @NonNull Executor executor) {
        Set<String> tableKeys = new LinkedHashSet<>();
        List<ExternalStatisticsCacheKey> partitionKeys = new ArrayList<>();
        Map<String, List<ExternalStatisticsCacheKey>> partitionRows = new LinkedHashMap<>();
        Map<String, List<ExternalStatisticsCacheKey>> blockKeys = new LinkedHashMap<>();
        for (ExternalStatisticsCacheKey key : keys) {
            if (key.isTable()) {
                tableKeys.add(key.tableUUID);
            } else if (key.scope == ExternalStatisticsCacheKey.Scope.BLOCK) {
                blockKeys.computeIfAbsent(key.tableUUID, ignored -> new ArrayList<>()).add(key);
            } else if (key.scope == ExternalStatisticsCacheKey.Scope.PARTITION_ROW) {
                partitionRows.computeIfAbsent(key.tableUUID, ignored -> new ArrayList<>()).add(key);
            } else if (key.scope == ExternalStatisticsCacheKey.Scope.PARTITION) {
                partitionKeys.add(key);
            } else {
                throw new IllegalArgumentException("Directory entries must not start SQL loads");
            }
        }
        List<CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>>> loads = new ArrayList<>();
        tableKeys.forEach(uuid -> loads.add(CompletableFuture.supplyAsync(() -> {
            if (FeConstants.enableUnitStatistics) {
                return Map.of();
            }
            ConnectContext context = StatisticUtils.buildConnectContext();
            context.setOnlyReadIcebergCache(true);
            context.setThreadLocalInfo();
            try {
                List<String> chunks = statisticExecutor.queryExternalTableStatistics(context, uuid);
                Optional<ExternalColumnStatistics> value = Optional.empty();
                if (!chunks.isEmpty()) {
                    Table table = StatisticsUtils.getTableByUUID(context, uuid);
                    List<String> name = StatisticsUtils.getTableNameByUUID(uuid);
                    var meta = GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                            .getExternalTableBasicStatsMeta(name.get(0), name.get(1), name.get(2));
                    value = Optional.of(ExternalTableStatistics.decode(chunks, table,
                            meta == null ? Map.of() : meta.getColumnStatsMetaMap()));
                }
                return Map.of(ExternalStatisticsCacheKey.tableRow(uuid), value);
            } catch (Exception error) {
                throw new CompletionException(error);
            } finally {
                ConnectContext.remove();
            }
        }, executor)));
        Executor loaderExecutor = partitionExecutor == null ? executor : partitionExecutor;
        if (!partitionKeys.isEmpty()) {
            loads.add(loadPartitions(partitionKeys, loaderExecutor));
        }
        partitionRows.forEach((uuid, rows) -> loads.add(loadPartitionRows(uuid, rows, loaderExecutor)));
        blockKeys.forEach((uuid, blocks) -> loads.add(loadBlocks(uuid, blocks, loaderExecutor)));
        return CompletableFuture.allOf(loads.toArray(new CompletableFuture<?>[0])).thenApply(ignored -> {
            Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> result = new HashMap<>();
            loads.forEach(load -> result.putAll(load.join()));
            return result;
        });
    }

    private CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>> loadPartitionRows(
            String uuid, List<ExternalStatisticsCacheKey> keys, Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> result = new HashMap<>();
            keys.forEach(key -> result.put(key, Optional.empty()));
            if (FeConstants.enableUnitStatistics) {
                return result;
            }
            ConnectContext context = StatisticUtils.buildConnectContext();
            context.setOnlyReadIcebergCache(true);
            context.setThreadLocalInfo();
            try {
                for (TStatisticData row : statisticExecutor.queryExternalPartitionRows(context, uuid,
                        keys.stream().map(key -> key.partitionName).toList())) {
                    ExternalStatisticsCacheKey key = ExternalStatisticsCacheKey.partitionRow(uuid, row.partitionName);
                    if (!result.containsKey(key) || result.get(key).isPresent()) {
                        throw new IllegalArgumentException("Unexpected or duplicate external partition row");
                    }
                    result.put(key, Optional.of(ExternalPartitionStatistics.decode(row.getHll())));
                }
                return result;
            } finally {
                ConnectContext.remove();
            }
        }, executor);
    }

    private CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>> loadBlocks(
            String uuid, List<ExternalStatisticsCacheKey> blocks, Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            if (FeConstants.enableUnitStatistics) {
                return Map.of();
            }
            try {
                ConnectContext context = StatisticUtils.buildConnectContext();
                context.setOnlyReadIcebergCache(true);
                context.setThreadLocalInfo();
                Table table = StatisticsUtils.getTableByUUID(context, uuid);
                Map<String, Type> types = new HashMap<>();
                Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> result = new HashMap<>();
                for (ExternalStatisticsCacheKey block : blocks) {
                    Type type = types.computeIfAbsent(block.columnName,
                            name -> StatisticUtils.getQueryStatisticsColumnType(table, name));
                    long[] missing = new long[block.partitions.size()];
                    java.util.Arrays.fill(missing, -1);
                    result.put(block, Optional.of(new ExternalPartitionStatisticsBlocks.Block(block,
                            new ExternalColumnStatistics.Partition(type.toSql(), 0, 0, 0,
                                    StatisticsHll.fromSerialized(new byte[] {0}),
                                    Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY), missing)));
                }
                for (TStatisticData row : statisticExecutor.queryExternalPartitionBlocks(context, uuid, blocks, types)) {
                    int separator = row.partitionName.indexOf('|');
                    int index = Integer.parseInt(row.partitionName.substring(0, separator));
                    ExternalStatisticsCacheKey key = blocks.get(index);
                    if (!key.columnName.equals(row.columnName)) {
                        throw new IllegalArgumentException("Statistics block column mismatch");
                    }
                    result.put(key, Optional.of(ExternalPartitionStatisticsBlocks.Block.fromRows(key,
                            new ExternalColumnStatistics.Partition(row, types.get(row.columnName)),
                            row.partitionName.substring(separator + 1))));
                }
                return result;
            } catch (Exception e) {
                throw new CompletionException(e);
            } finally {
                ConnectContext.remove();
            }
        }, executor);
    }

    private CompletableFuture<Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>>> loadPartitions(
            List<ExternalStatisticsCacheKey> keys, Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> result =
                    new HashMap<>();
            Map<String, Map<String, Set<String>>> columnsByTablePartition = new LinkedHashMap<>();
            for (ExternalStatisticsCacheKey key : keys) {
                // Only a successfully read missing (partition, column) is cached as empty.
                result.put(key, Optional.empty());
                columnsByTablePartition.computeIfAbsent(key.tableUUID, k -> new LinkedHashMap<>())
                        .computeIfAbsent(key.partitionName, k -> new LinkedHashSet<>()).add(key.columnName);
            }
            if (FeConstants.enableUnitStatistics) {
                return result;
            }
            try {
                ConnectContext connectContext = StatisticUtils.buildConnectContext();
                connectContext.setOnlyReadIcebergCache(true);
                connectContext.setThreadLocalInfo();
                for (Map.Entry<String, Map<String, Set<String>>> entry : columnsByTablePartition.entrySet()) {
                    Table table = StatisticsUtils.getTableByUUID(connectContext, entry.getKey());
                    List<TStatisticData> rows = statisticExecutor.queryExternalPartitionStatistics(
                            connectContext, entry.getKey(), entry.getValue(), table.isUnPartitioned());
                    for (TStatisticData row : rows) {
                        if (!row.isSetPartitionName() || !row.isSetColumnName()) {
                            throw new IllegalArgumentException("External statistics row has no partition/column key");
                        }
                        ExternalStatisticsCacheKey key =
                                new ExternalStatisticsCacheKey(entry.getKey(), row.partitionName, row.columnName);
                        if (result.containsKey(key)) {
                            ExternalColumnStatistics.Partition value = new ExternalColumnStatistics.Partition(row,
                                    StatisticUtils.getQueryStatisticsColumnType(table, row.columnName));
                            result.put(key, Optional.of(value));
                        }
                    }
                }
                return result;
            } catch (RuntimeException e) {
                LOG.error("Failed to load external partition statistics of {}", columnsByTablePartition.keySet(), e);
                throw new CompletionException(e);
            } catch (Exception e) {
                throw new CompletionException(e);
            } finally {
                ConnectContext.remove();
            }
        }, executor);
    }

    @Override
    public @NonNull CompletableFuture<Optional<ExternalColumnStatistics>> asyncReload(
            @NonNull ExternalStatisticsCacheKey key,
            @NonNull Optional<ExternalColumnStatistics> oldValue,
            @NonNull Executor executor) {
        return asyncLoad(key, executor);
    }

}
