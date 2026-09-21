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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Loads the MCV statistics of an external table from
 * _statistics_.external_mcv_statistics, keyed by table UUID.
 */
public class ExternalMcvStatsCacheLoader
        implements AsyncCacheLoader<String, Optional<ExternalMcvStatistics>> {
    private static final Logger LOG = LogManager.getLogger(ExternalMcvStatsCacheLoader.class);
    private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().create();

    private final StatisticExecutor statisticExecutor = new StatisticExecutor();

    @Override
    public @NonNull CompletableFuture<Optional<ExternalMcvStatistics>> asyncLoad(
            @NonNull String tableUUID, @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            if (FeConstants.enableUnitStatistics) {
                return Optional.empty();
            }
            try {
                ConnectContext connectContext = StatisticUtils.buildConnectContext();
                connectContext.setThreadLocalInfo();
                List<List<String>> rows = statisticExecutor.queryExternalMcvStatistics(connectContext, tableUUID);
                List<ExternalMcvStatistics.Group> groups = new ArrayList<>();
                for (List<String> row : rows) {
                    ExternalMcvStatistics.Group group = parseGroup(row);
                    if (group != null) {
                        groups.add(group);
                    }
                }
                if (groups.isEmpty()) {
                    return Optional.empty();
                }
                return Optional.of(new ExternalMcvStatistics(groups));
            } catch (RuntimeException e) {
                LOG.error("Failed to load external MCV statistics of table {}", tableUUID, e);
                throw new CompletionException(e);
            } catch (Exception e) {
                throw new CompletionException(e);
            } finally {
                ConnectContext.remove();
            }
        }, executor);
    }

    @Override
    public @NonNull CompletableFuture<Optional<ExternalMcvStatistics>> asyncReload(
            @NonNull String tableUUID, @NonNull Optional<ExternalMcvStatistics> oldValue,
            @NonNull Executor executor) {
        return asyncLoad(tableUUID, executor);
    }

    // A row is [column_names, row_count, ndv, mcv]; see StatisticSQLBuilder.buildQueryExternalMcvStatisticsSQL.
    static ExternalMcvStatistics.Group parseGroup(List<String> row) {
        if (row.size() < 4 || row.get(0) == null) {
            return null;
        }
        try {
            List<String> columnNames = parseStringArray(JsonParser.parseString(row.get(0)).getAsJsonArray());
            if (columnNames.isEmpty()) {
                return null;
            }
            long rowCount = row.get(1) == null ? 0 : Long.parseLong(row.get(1));
            long ndv = row.get(2) == null ? 0 : Long.parseLong(row.get(2));
            List<MultiColumnCombinedStats.McvEntry> mcv =
                    row.get(3) == null ? List.of() : parseMcv(row.get(3), columnNames.size());
            return new ExternalMcvStatistics.Group(columnNames, rowCount, ndv, mcv);
        } catch (RuntimeException e) {
            LOG.warn("Ignore malformed external MCV statistics row {}", row, e);
            return null;
        }
    }

    /**
     * MCV text: [[[value, value, ...], "count", ["component count", ...]], ...]; a JSON null inside the
     * value array is a NULL column value, and the component counts are the rows holding each value in
     * its column. Entries whose tuple width differs from the column group are dropped; component counts
     * of the wrong width are dropped from their entry.
     */
    public static List<MultiColumnCombinedStats.McvEntry> parseMcv(String text, int width) {
        List<MultiColumnCombinedStats.McvEntry> result = new ArrayList<>();
        JsonElement root = JsonParser.parseString(text);
        if (!root.isJsonArray()) {
            return result;
        }
        for (JsonElement entry : root.getAsJsonArray()) {
            JsonArray pair = entry.getAsJsonArray();
            JsonArray tuple = pair.get(0).getAsJsonArray();
            if (tuple.size() != width) {
                continue;
            }
            List<String> values = new ArrayList<>(width);
            for (JsonElement value : tuple) {
                values.add(value.isJsonNull() ? null : value.getAsString());
            }
            long count = Long.parseLong(pair.get(1).getAsString());
            if (count <= 0) {
                continue;
            }
            List<Long> componentCounts = List.of();
            if (pair.size() > 2 && pair.get(2).isJsonArray() && pair.get(2).getAsJsonArray().size() == width) {
                componentCounts = new ArrayList<>(width);
                for (JsonElement componentCount : pair.get(2).getAsJsonArray()) {
                    componentCounts.add(Long.parseLong(componentCount.getAsString()));
                }
            }
            result.add(new MultiColumnCombinedStats.McvEntry(values, count, componentCounts));
        }
        return result;
    }

    /** The inverse of parseMcv: the MCV text as the statistics table stores it. */
    public static String formatMcv(List<MultiColumnCombinedStats.McvEntry> mcv) {
        JsonArray array = new JsonArray();
        for (MultiColumnCombinedStats.McvEntry entry : mcv) {
            JsonArray values = new JsonArray();
            for (String value : entry.getValues()) {
                values.add(value == null ? JsonNull.INSTANCE : new JsonPrimitive(value));
            }
            JsonArray pair = new JsonArray();
            pair.add(values);
            pair.add(String.valueOf(entry.getCount()));
            if (entry.hasComponentCounts()) {
                JsonArray counts = new JsonArray();
                for (Long count : entry.getComponentCounts()) {
                    counts.add(String.valueOf(count));
                }
                pair.add(counts);
            }
            array.add(pair);
        }
        return JSON.toJson(array);
    }

    private static List<String> parseStringArray(JsonArray array) {
        List<String> result = new ArrayList<>(array.size());
        for (JsonElement element : array) {
            result.add(element.getAsString());
        }
        return result;
    }
}
