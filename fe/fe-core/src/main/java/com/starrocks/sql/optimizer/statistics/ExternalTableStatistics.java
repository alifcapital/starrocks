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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.statistic.ColumnStatsMeta;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** One immutable, prepared whole-table cache value. JSON and source types are decoded only on load. */
public final class ExternalTableStatistics implements ExternalColumnStatistics {
    private static final int VERSION = 1;
    // Chunks are elements of one ARRAY in one stored row: replacement is atomic even when width changes.
    private static final int CHUNK_BYTES = 256 * 1024;
    public final Map<String, ExternalColumnStatistics.Summary> summaries;
    public final Map<String, ColumnStatistic> columns;
    public final Map<String, String> sourceTypes;
    public final Map<String, Integer> coverage;
    private final int bytes;

    ExternalTableStatistics(Map<String, ExternalColumnStatistics.Summary> values) {
        summaries = Map.copyOf(values);
        Map<String, ColumnStatistic> stats = new LinkedHashMap<>();
        Map<String, String> types = new LinkedHashMap<>();
        Map<String, Integer> covered = new LinkedHashMap<>();
        long size = 256;
        for (var entry : values.entrySet()) {
            String name = entry.getKey();
            var summary = entry.getValue();
            stats.put(name, summary.statistic);
            types.put(name, summary.sourceType);
            covered.put(name, 1);
            size += 160L + 2L * name.length() + summary.retainedBytes();
        }
        columns = Map.copyOf(stats);
        sourceTypes = Map.copyOf(types);
        coverage = Map.copyOf(covered);
        bytes = (int) Math.min(Integer.MAX_VALUE, size);
    }

    public static List<String> encode(List<TStatisticData> rows, Table table) {
        List<String> chunks = new ArrayList<>();
        JsonArray chunk = new JsonArray();
        int bytes = 0;
        int chunkLimit = Math.min(CHUNK_BYTES, Config.max_varchar_length - 32);
        for (TStatisticData data : rows) {
            Type type = StatisticUtils.getQueryStatisticsColumnType(table, data.columnName);
            JsonArray value = new JsonArray();
            value.add(data.columnName);
            value.add(type.toSql());
            value.add(data.rowCount);
            value.add(data.dataSize);
            value.add(data.countDistinct);
            value.add(data.nullCount);
            value.add(data.isSetMin() ? data.min : null);
            value.add(data.isSetMax() ? data.max : null);
            value.add(data.isSetUpdateTime() ? data.updateTime : null);
            int encodedBytes = value.toString().getBytes(StandardCharsets.UTF_8).length;
            if (encodedBytes + 32 > Config.max_varchar_length) {
                throw new IllegalArgumentException("External statistics column record is too large: " + data.columnName);
            }
            if (bytes + encodedBytes + 1 > chunkLimit && !chunk.isEmpty()) {
                chunks.add(wrap(chunk));
                chunk = new JsonArray();
                bytes = 0;
            }
            chunk.add(value);
            bytes += encodedBytes + 1;
        }
        if (!chunk.isEmpty()) {
            chunks.add(wrap(chunk));
        }
        return List.copyOf(chunks);
    }

    private static String wrap(JsonArray values) {
        JsonObject root = new JsonObject();
        root.addProperty("version", VERSION);
        root.add("columns", values);
        return root.toString();
    }

    public static ExternalTableStatistics decode(List<String> chunks, Table table, Map<String, ColumnStatsMeta> metadata) {
        Map<String, ExternalColumnStatistics.Summary> values = new LinkedHashMap<>();
        for (String chunk : chunks) {
            JsonObject root = JsonParser.parseString(chunk).getAsJsonObject();
            if (root.get("version").getAsInt() != VERSION) {
                throw new IllegalArgumentException("Unsupported external table statistics version");
            }
            for (JsonElement element : root.getAsJsonArray("columns")) {
                JsonArray row = element.getAsJsonArray();
                if (row.size() != 9) {
                    throw new IllegalArgumentException("Invalid external table statistics record");
                }
                String name = row.get(0).getAsString();
                String storedType = row.get(1).getAsString();
                Type type;
                try {
                    type = StatisticUtils.getQueryStatisticsColumnType(table, name);
                } catch (com.starrocks.sql.analyzer.SemanticException removedColumn) {
                    continue;
                }
                if (!type.toSql().equals(storedType)) {
                    continue; // A stale summary cannot be reinterpreted after a source schema change.
                }
                TStatisticData data = new TStatisticData();
                data.setColumnName(name);
                data.setRowCount(row.get(2).getAsLong());
                data.setDataSize(row.get(3).getAsLong());
                data.setCountDistinct(row.get(4).getAsLong());
                data.setNullCount(row.get(5).getAsLong());
                if (data.rowCount < 0 || data.dataSize < 0 || data.countDistinct < 0
                        || data.nullCount < 0 || data.nullCount > data.rowCount) {
                    throw new IllegalArgumentException("Invalid external table statistics counts");
                }
                if (!row.get(6).isJsonNull()) {
                    data.setMin(row.get(6).getAsString());
                }
                if (!row.get(7).isJsonNull()) {
                    data.setMax(row.get(7).getAsString());
                }
                if (!row.get(8).isJsonNull()) {
                    data.setUpdateTime(row.get(8).getAsString());
                }
                ColumnStatistic statistic = ColumnBasicStatsCacheLoader.buildColumnStatistics(
                        data, "", "", table.getName(), name, type);
                ConnectorTableColumnStats raw = new ConnectorTableColumnStats(statistic, data.rowCount, data.updateTime);
                ConnectorTableColumnStats estimated = StatisticsUtils.estimateColumnStatistics(metadata.get(name), raw);
                if (values.put(name, new ExternalColumnStatistics.Summary(raw, estimated, storedType)) != null) {
                    throw new IllegalArgumentException("Duplicate external table statistics column: " + name);
                }
            }
        }
        return new ExternalTableStatistics(values);
    }

    @Override
    public String getSourceType() {
        return "";
    }

    @Override
    public int retainedBytes() {
        return bytes;
    }
}
