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
import com.google.gson.JsonObject;
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
    private static final long SKIP_WARNING_INTERVAL_MS = 60_000;
    private final java.util.concurrent.atomic.AtomicLong lastSkipWarning = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong LAST_IGNORE_WARNING =
            new java.util.concurrent.atomic.AtomicLong();

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
                ExternalMcvStatistics bound = resolveAndBind(groups, connectContext, tableUUID);
                return bound.isEmpty() && !bound.isIncomplete() ? Optional.empty() : Optional.of(bound);
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

    /**
     * Binds the groups that need the table to it. We fear that the table cannot be found by its UUID, as when it was
     * dropped or created again outside StarRocks; that is a state of one table, not a failure of the statistics, so
     * its groups that need the table are left out of this load, the other groups load, and the result is marked
     * incomplete. The retry pause of all loads is not started.
     */
    ExternalMcvStatistics resolveAndBind(List<ExternalMcvStatistics.Group> groups, ConnectContext connectContext,
                                         String tableUUID) {
        if (groups.stream().noneMatch(ExternalMcvStatsCacheLoader::needsTable)) {
            return new ExternalMcvStatistics(groups);
        }
        Table table;
        try {
            table = getTableByUUID(connectContext, tableUUID);
        } catch (RuntimeException e) {
            warnOnceAMinute(lastSkipWarning, "Skip MCV statistics of table {} that need the table in this load: {}",
                    tableUUID, e.getMessage());
            List<ExternalMcvStatistics.Group> kept = new ArrayList<>();
            for (ExternalMcvStatistics.Group group : groups) {
                if (!needsTable(group)) {
                    kept.add(group);
                }
            }
            return new ExternalMcvStatistics(kept, true);
        }
        return bindGroups(groups, table, connectContext, tableUUID);
    }

    // A group of one element is prepared by the type of its column, and an expression is analyzed on the table.
    private static boolean needsTable(ExternalMcvStatistics.Group group) {
        return group.getColumnNames().size() == 1 || group.hasExpressions();
    }

    // A failure that repeats on every load until the table or its schema reads again is logged once a minute.
    private static void warnOnceAMinute(java.util.concurrent.atomic.AtomicLong lastWarning, String message,
                                        Object... arguments) {
        long now = System.currentTimeMillis();
        long last = lastWarning.get();
        if (now - last >= SKIP_WARNING_INTERVAL_MS && lastWarning.compareAndSet(last, now)) {
            LOG.warn(message, arguments);
        } else {
            LOG.debug(message, arguments);
        }
    }

    /**
     * Binds the groups of a table to its current schema and prepares the singleton groups. A group whose Iceberg
     * field ids cannot be read now is left out of this load only: the other groups load, the result is marked
     * incomplete so the cache loads it again soon, and the loads of other tables go on.
     */
    ExternalMcvStatistics bindGroups(List<ExternalMcvStatistics.Group> groups, Table table,
                                     ConnectContext connectContext, String tableUUID) {
        List<ExternalMcvStatistics.Group> bound = new ArrayList<>(groups.size());
        boolean incomplete = false;
        for (ExternalMcvStatistics.Group parsed : groups) {
            ExternalMcvStatistics.Group group;
            try {
                group = parsed.hasExpressions() ? bindExpressions(parsed, table, connectContext, tableUUID) : parsed;
            } catch (ExpressionTemplate.FieldIdUnavailableException e) {
                warnOnceAMinute(lastSkipWarning, "Skip MCV statistics of group {} of table {} in this load: {}",
                        parsed.getColumnNames(), tableUUID, e.getMessage());
                incomplete = true;
                continue;
            }
            if (group == null) {
                continue;
            }
            bound.add(group);
            if (group.getColumnNames().size() != 1) {
                continue;
            }
            ExternalMcvStatistics.Expression expression = group.getExpressions().get(0);
            if (expression != null) {
                // The type of an expression comes from the analyzed expression, not from a column.
                group.prepare(expression.getTemplate().getType());
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
        return new ExternalMcvStatistics(bound, incomplete);
    }

    /**
     * Analyzes the expressions of a group against the current schema of the table, once per load. We fear that the
     * schema changed since the collection: a source column was dropped, renamed and added again, or changed its
     * type. Such a group no longer describes the expression of the query, so it is left out with a warning; the
     * other groups of the table and the query are not affected.
     */
    static ExternalMcvStatistics.Group bindExpressions(ExternalMcvStatistics.Group group, Table table,
                                                       ConnectContext context, String tableUUID) {
        return bindExpressions(group, table, context, tableUUID, true);
    }

    /**
     * @param checkFieldIds whether the Iceberg field ids are compared; a group of a replayed query dump comes with a
     *                      table that cannot give them
     */
    static ExternalMcvStatistics.Group bindExpressions(ExternalMcvStatistics.Group group, Table table,
                                                       ConnectContext context, String tableUUID,
                                                       boolean checkFieldIds) {
        List<ExternalMcvStatistics.Expression> bound = new ArrayList<>(group.getExpressions().size());
        for (ExternalMcvStatistics.Expression expression : group.getExpressions()) {
            if (expression == null) {
                bound.add(null);
                continue;
            }
            String reason = null;
            try {
                if (table == null) {
                    reason = "the table is not found";
                } else {
                    for (ExternalMcvStatistics.StoredSource source : expression.getSources()) {
                        if (!sameSource(source, table, checkFieldIds)) {
                            reason = "column " + source.name() + " is missing or changed";
                            break;
                        }
                    }
                }
                if (reason == null) {
                    ExpressionTemplate template = ExpressionTemplate.parse(expression.getText(), table, context);
                    if (!template.getText().equals(expression.getText())) {
                        reason = "it analyzes as " + template.getText();
                    } else if (!template.getType().toSql().equals(expression.getType())) {
                        reason = "its type is " + template.getType().toSql() + ", not " + expression.getType();
                    } else {
                        bound.add(expression.withTemplate(template));
                        continue;
                    }
                }
            } catch (ExpressionTemplate.FieldIdUnavailableException e) {
                // A schema that cannot be read now may be read on the next load.
                throw e;
            } catch (RuntimeException e) {
                reason = e.getMessage();
            }
            // A changed schema leaves the group out on every load until the statistics are collected again.
            warnOnceAMinute(LAST_IGNORE_WARNING, "Ignore MCV statistics of expression {} on table {}: {}",
                    expression.getText(), tableUUID, reason);
            return null;
        }
        return group.withExpressions(bound);
    }

    /** Whether a column of the table still has the type and the Iceberg field id recorded at collection. */
    static boolean sameSource(ExternalMcvStatistics.StoredSource source, Table table, boolean checkFieldIds) {
        com.starrocks.catalog.Column column = table.getColumn(source.name());
        return column != null && column.getType().toSql().equals(source.type())
                && (!checkFieldIds
                || ExpressionTemplate.fieldIdMatchesOrThrow(table, column.getName(), source.fieldId()));
    }

    // A row is [column_names, row_count, ndv, mcv, buckets, null_counts].
    static ExternalMcvStatistics.Group parseGroup(List<String> row) {
        if (row.size() != 6 || row.stream().anyMatch(java.util.Objects::isNull)) {
            return null;
        }
        try {
            List<String> columnNames = new ArrayList<>();
            List<ExternalMcvStatistics.Expression> expressions = new ArrayList<>();
            if (!parseColumnNames(row.get(0), columnNames, expressions)) {
                return null;
            }
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
            return new ExternalMcvStatistics.Group(columnNames, rowCount, ndv, mcv, buckets, nullCounts, expressions);
        } catch (RuntimeException e) {
            LOG.warn("Ignore malformed external MCV statistics record", e);
            return null;
        }
    }

    /**
     * column_names: a column is its name; an expression is an object with its canonical text, format version, type
     * and source columns. False when the group has an expression of a format this FE does not know, which is left
     * out with a warning.
     */
    public static boolean parseColumnNames(String text, List<String> columnNames,
                                           List<ExternalMcvStatistics.Expression> expressions) {
        for (JsonElement element : JsonParser.parseString(text).getAsJsonArray()) {
            if (!element.isJsonObject()) {
                columnNames.add(element.getAsString());
                expressions.add(null);
                continue;
            }
            JsonObject object = element.getAsJsonObject();
            String expression = object.get(ExpressionTemplate.JSON_EXPR).getAsString();
            int format = object.get(ExpressionTemplate.JSON_FORMAT).getAsInt();
            if (format != ExpressionTemplate.FORMAT_VERSION) {
                LOG.warn("Ignore MCV statistics of expression {} of unknown format {}", expression, format);
                return false;
            }
            List<ExternalMcvStatistics.StoredSource> sources = new ArrayList<>();
            for (JsonElement column : object.getAsJsonArray(ExpressionTemplate.JSON_COLUMNS)) {
                JsonObject source = column.getAsJsonObject();
                JsonElement fieldId = source.get(ExpressionTemplate.JSON_FIELD_ID);
                sources.add(new ExternalMcvStatistics.StoredSource(
                        source.get(ExpressionTemplate.JSON_NAME).getAsString(),
                        source.get(ExpressionTemplate.JSON_TYPE).getAsString(),
                        fieldId == null || fieldId.isJsonNull() ? null : fieldId.getAsInt()));
            }
            columnNames.add(ExpressionTemplate.elementText(expression));
            expressions.add(new ExternalMcvStatistics.Expression(expression,
                    object.get(ExpressionTemplate.JSON_TYPE).getAsString(), sources, null));
        }
        return true;
    }

    /** The inverse of parseColumnNames: the elements of a group as column_names holds them. */
    public static JsonArray formatColumnNames(ExternalMcvStatistics.Group group) {
        JsonArray array = new JsonArray();
        for (int i = 0; i < group.getColumnNames().size(); i++) {
            ExternalMcvStatistics.Expression expression = group.getExpressions().get(i);
            if (expression == null) {
                array.add(group.getColumnNames().get(i));
                continue;
            }
            JsonObject object = new JsonObject();
            object.addProperty(ExpressionTemplate.JSON_EXPR, expression.getText());
            object.addProperty(ExpressionTemplate.JSON_FORMAT, ExpressionTemplate.FORMAT_VERSION);
            object.addProperty(ExpressionTemplate.JSON_TYPE, expression.getType());
            JsonArray sources = new JsonArray();
            for (ExternalMcvStatistics.StoredSource source : expression.getSources()) {
                JsonObject column = new JsonObject();
                column.addProperty(ExpressionTemplate.JSON_NAME, source.name());
                column.addProperty(ExpressionTemplate.JSON_TYPE, source.type());
                if (source.fieldId() != null) {
                    column.addProperty(ExpressionTemplate.JSON_FIELD_ID, source.fieldId());
                }
                sources.add(column);
            }
            object.add(ExpressionTemplate.JSON_COLUMNS, sources);
            array.add(object);
        }
        return array;
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
