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
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

// Loads external (connector) multi-column combined NDV from _statistics_.external_multi_column_statistics, keyed by
// table UUID. The value maps a set of column names to its combined NDV. Each load queries a single UUID, so every
// returned row belongs to that key (the read layout reuses version 13 and does not carry the UUID back).
public class ExternalMultiColumnCombinedStatsCacheLoader
        implements AsyncCacheLoader<String, Optional<Map<Set<String>, Long>>> {
    private static final Logger LOG = LogManager.getLogger(ExternalMultiColumnCombinedStatsCacheLoader.class);
    private final StatisticExecutor statisticExecutor = new StatisticExecutor();

    @Override
    public @NonNull CompletableFuture<Optional<Map<Set<String>, Long>>> asyncLoad(
            @NonNull String tableUUID, @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            if (FeConstants.enableUnitStatistics) {
                return Optional.empty();
            }
            try {
                ConnectContext connectContext = StatisticUtils.buildConnectContext();
                connectContext.setThreadLocalInfo();
                List<TStatisticData> statisticData =
                        statisticExecutor.queryExternalMultiColumnCombinedStats(connectContext, List.of(tableUUID));
                if (statisticData.isEmpty()) {
                    return Optional.empty();
                }

                Map<Set<String>, Long> result = new HashMap<>();
                for (TStatisticData data : statisticData) {
                    // columnName holds the column names joined by ',' (see ExternalMultiColumnStatisticsCollectJob)
                    Set<String> columnNames = Arrays.stream(data.getColumnName().split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toSet());
                    if (!columnNames.isEmpty()) {
                        result.put(columnNames, data.getCountDistinct());
                    }
                }
                return Optional.of(result);
            } catch (RuntimeException e) {
                LOG.error("Failed to load external multi-column combined statistics for {}", tableUUID, e);
                throw new CompletionException(e);
            } catch (Exception e) {
                throw new CompletionException(e);
            } finally {
                ConnectContext.remove();
            }
        }, executor);
    }

    @Override
    public @NonNull CompletableFuture<Optional<Map<Set<String>, Long>>> asyncReload(
            @NonNull String tableUUID, @NonNull Optional<Map<Set<String>, Long>> oldValue, @NonNull Executor executor) {
        return asyncLoad(tableUUID, executor);
    }
}
