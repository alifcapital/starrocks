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
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
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

import static com.starrocks.connector.statistics.StatisticsUtils.getTableByUUID;

/**
 * Loads the MCV statistics of an external table from
 * _statistics_.external_mcv_statistics, keyed by table UUID.
 */
public class ExternalMcvStatsCacheLoader
        implements AsyncCacheLoader<String, Optional<ExternalMcvStatistics>> {
    private static final Logger LOG = LogManager.getLogger(ExternalMcvStatsCacheLoader.class);
    private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().create();

    private final StatisticExecutor statisticExecutor = new StatisticExecutor();
    final StatisticsLoadBackoff backoff = new StatisticsLoadBackoff();

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
                if (groups.stream().anyMatch(group -> group.getColumnNames().size() == 1)) {
                    Table table = getTableByUUID(connectContext, tableUUID);
                    for (ExternalMcvStatistics.Group group : groups) {
                        if (group.getColumnNames().size() != 1) {
                            continue;
                        }
                        try {
                            group.prepare(StatisticUtils.getQueryStatisticsColumnType(table, group.getColumnNames().get(0)));
                        } catch (SemanticException e) {
                            // Statistics can outlive a dropped column. It cannot match a scan column,
                            // but must not prevent other groups of the table from being loaded.
                            LOG.debug("Cannot prepare MCV statistics for removed column {} of {}",
                                    group.getColumnNames(), tableUUID, e);
                        }
                    }
                }
                return Optional.of(new ExternalMcvStatistics(groups));
            } catch (Exception e) {
                backoff.failed();
                // Caffeine reports the failed load; query callers respect the retry pause.
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

    // A row is [column_names, row_count, ndv, mcv, buckets, null_counts].
    static ExternalMcvStatistics.Group parseGroup(List<String> row) {
        if (row.size() != 6 || row.stream().anyMatch(java.util.Objects::isNull)) {
            return null;
        }
        try {
            List<String> columnNames = parseStringArray(JsonParser.parseString(row.get(0)).getAsJsonArray());
            if (columnNames.isEmpty()) {
                return null;
            }
            long rowCount = Long.parseLong(row.get(1));
            long ndv = Long.parseLong(row.get(2));
            List<MultiColumnCombinedStats.McvEntry> mcv =
                    parseMcv(row.get(3), columnNames.size());
            List<List<String>> buckets = parseBuckets(row.get(4));
            List<Long> nullCounts = parseNullCounts(row.get(5));
            if (nullCounts.size() != columnNames.size()
                    || nullCounts.stream().anyMatch(n -> n < 0 || n > rowCount)) {
                throw new IllegalArgumentException("Invalid MCV NULL counts");
            }
            return new ExternalMcvStatistics.Group(columnNames, rowCount, ndv, mcv, buckets, nullCounts);
        } catch (RuntimeException e) {
            LOG.warn("Ignore malformed external MCV statistics record", e);
            return null;
        }
    }

    public static List<List<String>> parseBuckets(String text) {
        List<List<String>> result = new ArrayList<>();
        long previous = 0;
        if (text != null) {
            for (JsonElement element : JsonParser.parseString(text).getAsJsonArray()) {
                List<String> fields = parseStringArray(element.getAsJsonArray());
                if (fields.size() != 5) {
                    throw new IllegalArgumentException("Invalid MCV bucket");
                }
                long cumulative = Long.parseLong(fields.get(2));
                long repeats = Long.parseLong(fields.get(3));
                long ndv = Long.parseLong(fields.get(4));
                if (cumulative < previous || repeats < 0 || repeats > cumulative - previous || ndv < 0) {
                    throw new IllegalArgumentException("Invalid MCV bucket counts");
                }
                previous = cumulative;
                result.add(fields);
            }
        }
        return result;
    }

    public static List<Long> parseNullCounts(String text) {
        List<Long> result = new ArrayList<>();
        if (text != null) {
            for (JsonElement element : JsonParser.parseString(text).getAsJsonArray()) {
                result.add(element.getAsLong());
            }
        }
        return result;
    }

    /**
     * MCV text: [[[value, value, ...], "count", ["component count", ...]], ...]; a JSON null inside the
     * value array is a NULL column value, and the component counts are the rows holding each value in
     * its column. All three fields are required; malformed distributions are rejected as a whole.
     */
    public static List<MultiColumnCombinedStats.McvEntry> parseMcv(String text, int width) {
        List<MultiColumnCombinedStats.McvEntry> result = new ArrayList<>();
        JsonElement root = JsonParser.parseString(text);
        for (JsonElement entry : root.getAsJsonArray()) {
            JsonArray pair = entry.getAsJsonArray();
            if (pair.size() != 3) {
                throw new IllegalArgumentException("Invalid MCV entry");
            }
            JsonArray tuple = pair.get(0).getAsJsonArray();
            JsonArray counts = pair.get(2).getAsJsonArray();
            if (tuple.size() != width || counts.size() != width) {
                throw new IllegalArgumentException("Invalid MCV tuple width");
            }
            List<String> values = new ArrayList<>(width);
            for (JsonElement value : tuple) {
                values.add(value.isJsonNull() ? null : value.getAsString());
            }
            long count = Long.parseLong(pair.get(1).getAsString());
            if (count <= 0) {
                throw new IllegalArgumentException("Invalid MCV count");
            }
            List<Long> componentCounts = new ArrayList<>(width);
            for (JsonElement componentCount : counts) {
                long marginal = Long.parseLong(componentCount.getAsString());
                if (marginal < count) {
                    throw new IllegalArgumentException("MCV marginal is smaller than its tuple count");
                }
                componentCounts.add(marginal);
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
            JsonArray counts = new JsonArray();
            for (Long count : entry.getComponentCounts()) {
                counts.add(String.valueOf(count));
            }
            pair.add(counts);
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
