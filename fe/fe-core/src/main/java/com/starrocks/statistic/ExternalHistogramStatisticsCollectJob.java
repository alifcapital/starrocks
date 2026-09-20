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

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.STATISTICS_DB_NAME;

/**
 * Collects the histogram of external table columns: the most common values with exact row counts
 * and, for numeric, date and decimal columns, equi-height buckets with lo, hi, count, rows equal to
 * hi and an NDV estimate. Every column takes two full scans of the column and O(mcv size +
 * bucket count) memory per fragment, whatever its number of distinct values:
 *
 * 1. a frequent-items sketch names the candidate most common values and a KLL sketch gives the
 *    bucket boundaries as quantiles;
 * 2. histogram_by_bounds counts the candidates exactly and fills the buckets between the
 *    boundaries with the other rows; the same scan counts the non-NULL rows.
 *
 * Boolean columns have at most two values and take one GROUP BY instead. Rows go to
 * _statistics_.external_histogram_statistics in the format the optimizer reads: buckets as
 * [["lo","hi","count","upper_repeats","ndv"], ...] where count covers this bucket and every bucket
 * before it, mcv as [["value","count"], ...]. String columns get no value buckets; one bucket with
 * infinite bounds holds the rows outside the MCV list, as for native tables.
 */
public class ExternalHistogramStatisticsCollectJob extends StatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(ExternalHistogramStatisticsCollectJob.class);
    // Values are stored as plain JSON text; no HTML escaping of quotes and angle brackets.
    private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().create();

    private final String catalogName;
    private final List<String> sqlBuffer = Lists.newArrayList();
    private final List<List<Expr>> rowsBuffer = Lists.newArrayList();

    public ExternalHistogramStatisticsCollectJob(String catalogName, Database db, Table table, List<String> columnNames,
                                                 List<Type> columnTypes, StatsConstants.AnalyzeType type,
                                                 StatsConstants.ScheduleType scheduleType,
                                                 Map<String, String> properties) {
        super(db, table, columnNames, columnTypes, type, scheduleType, properties);
        this.catalogName = catalogName;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getName() {
        return "ExternalHistogram";
    }

    static class ColumnHistogram {
        // [["value","count"], ...] by descending count; empty when the column has no non-NULL rows.
        final List<List<String>> mcv;
        // [["lo","hi","count","upper_repeats","ndv"], ...] by ascending lo with cumulative counts;
        // for string columns the single tail bucket [["Infinity","Infinity","count","0"]].
        final List<List<String>> buckets;

        ColumnHistogram(List<List<String>> mcv, List<List<String>> buckets) {
            this.mcv = mcv;
            this.buckets = buckets;
        }
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        setDefaultSessionVariable(context);
        long finished = 0;
        long total = Math.max(1, columnNames.size());
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);
            checkCancelled(analyzeStatus);
            ColumnHistogram histogram = collectColumn(context, analyzeStatus, columnName, columnType);
            bufferRow(columnName, histogram);
            flushInsertStatisticsData(context);
            // Best-effort: remove the stale raw-keyed row this column's fresh hashed-keyed row just
            // superseded. The read side no longer depends on this for correctness (it dedups by
            // update_time), so this is purely storage hygiene - failures are logged, not fatal.
            if (!statisticExecutor.dropExternalHistogramRawColumn(context, table.getUUID(), columnName)) {
                LOG.warn("[ExternalStats] failed to clean up stale raw-keyed histogram row | catalog={} db={} table={} " +
                        "column={}", catalogName, db.getOriginName(), table.getName(), columnName);
            }

            finished++;
            analyzeStatus.setProgress(finished * 100 / total);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        }
    }

    private ColumnHistogram collectColumn(ConnectContext context, AnalyzeStatus analyzeStatus, String columnName,
                                          Type columnType) throws Exception {
        if (columnType.isBoolean()) {
            List<List<String>> mcv = new ArrayList<>();
            for (List<String> row : execute(context, analyzeStatus, buildBooleanMcvSQL(columnName))) {
                if (row.size() >= 2 && row.get(0) != null && row.get(1) != null) {
                    mcv.add(List.of(row.get(0), row.get(1)));
                }
            }
            return new ColumnHistogram(mcv, List.of());
        }

        boolean withMcv = mcvSize() > 0;
        boolean withBuckets = !shouldSkipHistogramBuckets(columnType);
        List<String> candidates = List.of();
        List<String> bounds = List.of();
        List<List<String>> rows;
        if (withMcv || withBuckets) {
            rows = execute(context, analyzeStatus, buildSketchSQL(columnName, withMcv, withBuckets));
            if (rows.isEmpty() || rows.get(0).size() < (withMcv ? 1 : 0) + (withBuckets ? 1 : 0)) {
                throw new DdlException("Histogram sketch query returned no row for column " + columnName);
            }
            int field = 0;
            if (withMcv) {
                candidates = parseFrequentItems(rows.get(0).get(field++));
            }
            if (withBuckets) {
                bounds = parseQuantiles(rows.get(0).get(field));
            }
            if (withBuckets && candidates.isEmpty() && bounds.isEmpty()) {
                // The column has no non-NULL rows.
                return new ColumnHistogram(List.of(), List.of());
            }
        }

        rows = execute(context, analyzeStatus, buildExactSQL(columnName, candidates, bounds));
        if (rows.isEmpty() || rows.get(0).size() < 2) {
            throw new DdlException("Histogram count query returned no row for column " + columnName);
        }
        if (rows.get(0).get(0) == null) {
            // The aggregate saw no row: the column has no non-NULL rows.
            return new ColumnHistogram(List.of(), List.of());
        }
        ColumnHistogram histogram = parseHistogram(rows.get(0).get(0));
        if (withBuckets) {
            return histogram;
        }
        return withTailBucket(histogram.mcv, Long.parseLong(rows.get(0).get(1)));
    }

    private String qualifiedTableName() {
        return "`" + catalogName + "`.`" + db.getOriginName() + "`.`" + table.getName() + "`";
    }

    private long mcvSize() {
        String property = properties == null ? null : properties.get(StatsConstants.HISTOGRAM_MCV_SIZE);
        return property == null ? Config.histogram_mcv_size : Long.parseLong(property);
    }

    private long bucketNum() {
        String property = properties == null ? null : properties.get(StatsConstants.HISTOGRAM_BUCKET_NUM);
        return property == null ? Config.histogram_buckets_size : Long.parseLong(property);
    }

    String buildSketchSQL(String columnName, boolean withMcv, boolean withBuckets) {
        String column = StatisticUtils.quoting(table, columnName);
        List<String> sketches = new ArrayList<>();
        if (withMcv) {
            sketches.add("ds_frequent_items(" + column + ", " + mcvSize() + ", " + Config.statistic_mcv_sketch_lg_map_size
                    + ")");
        }
        if (withBuckets) {
            sketches.add("ds_kll_quantiles(" + column + ", " + bucketNum() + ")");
        }
        return "SELECT " + String.join(", ", sketches) + " FROM " + qualifiedTableName() + " WHERE " + column
                + " IS NOT NULL";
    }

    String buildExactSQL(String columnName, List<String> candidates, List<String> bounds) {
        String column = StatisticUtils.quoting(table, columnName);
        return "SELECT histogram_by_bounds(" + column + ", '" + SqlUtils.escapeSqlString(toJsonArray(candidates))
                + "', '" + SqlUtils.escapeSqlString(toJsonArray(bounds)) + "'), count(*) FROM " + qualifiedTableName()
                + " WHERE " + column + " IS NOT NULL";
    }

    // Both values, whatever the MCV size: the histogram of a boolean column is its two counts.
    String buildBooleanMcvSQL(String columnName) {
        String column = StatisticUtils.quoting(table, columnName);
        return "SELECT cast(" + column + " as varchar), count(*) FROM " + qualifiedTableName() + " WHERE " + column
                + " IS NOT NULL GROUP BY " + column + " ORDER BY count(*) DESC";
    }

    private List<List<String>> execute(ConnectContext context, AnalyzeStatus analyzeStatus, String sql)
            throws DdlException {
        checkCancelled(analyzeStatus);
        calculateAndSetRemainingTimeout(context, analyzeStatus);
        LOG.debug("external histogram statistics collect sql : {}", sql);
        return new StatisticExecutor().executeStatisticJsonDQL(context, sql);
    }

    private static String toJsonArray(List<String> values) {
        JsonArray array = new JsonArray();
        for (String value : values) {
            array.add(value);
        }
        return JSON.toJson(array);
    }

    // ds_frequent_items result: [["value","estimate"], ...], most frequent first.
    static List<String> parseFrequentItems(String json) {
        List<String> values = new ArrayList<>();
        if (json == null) {
            return values;
        }
        for (JsonElement entry : JsonParser.parseString(json).getAsJsonArray()) {
            values.add(entry.getAsJsonArray().get(0).getAsString());
        }
        return values;
    }

    // ds_kll_quantiles result: ["b0","b1",...], distinct and ascending.
    static List<String> parseQuantiles(String json) {
        List<String> bounds = new ArrayList<>();
        if (json == null) {
            return bounds;
        }
        for (JsonElement bound : JsonParser.parseString(json).getAsJsonArray()) {
            bounds.add(bound.getAsString());
        }
        return bounds;
    }

    // histogram_by_bounds result: {"mcv":[["value","count"],...],"buckets":[["lo","hi","count","upper_repeats","ndv"],...]}
    // with the rows of each bucket alone; the optimizer reads a bucket's count as the rows in it
    // and in every bucket before it.
    static ColumnHistogram parseHistogram(String json) {
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        List<List<String>> mcv = new ArrayList<>();
        for (JsonElement entry : root.getAsJsonArray("mcv")) {
            JsonArray pair = entry.getAsJsonArray();
            if (Long.parseLong(pair.get(1).getAsString()) > 0) {
                mcv.add(List.of(pair.get(0).getAsString(), pair.get(1).getAsString()));
            }
        }
        mcv.sort((a, b) -> Long.compare(Long.parseLong(b.get(1)), Long.parseLong(a.get(1))));
        List<List<String>> buckets = new ArrayList<>();
        long rowsSoFar = 0;
        for (JsonElement entry : root.getAsJsonArray("buckets")) {
            JsonArray fields = entry.getAsJsonArray();
            rowsSoFar += Long.parseLong(fields.get(2).getAsString());
            buckets.add(List.of(fields.get(0).getAsString(), fields.get(1).getAsString(), Long.toString(rowsSoFar),
                    fields.get(3).getAsString(), fields.get(4).getAsString()));
        }
        return new ColumnHistogram(mcv, buckets);
    }

    // The tail bucket of a string column: the non-NULL rows outside the MCV list, with no bounds.
    // It keeps the histogram's total row count equal to the number of non-NULL rows.
    static ColumnHistogram withTailBucket(List<List<String>> mcv, long nonNullRows) {
        long mcvRows = mcv.stream().mapToLong(entry -> Long.parseLong(entry.get(1))).sum();
        List<String> tail = List.of("Infinity", "Infinity", Long.toString(Math.max(0, nonNullRows - mcvRows)), "0");
        return new ColumnHistogram(mcv, List.of(tail));
    }

    static String toJson(List<List<String>> rows) {
        JsonArray array = new JsonArray();
        for (List<String> row : rows) {
            JsonArray fields = new JsonArray();
            for (String field : row) {
                fields.add(field);
            }
            array.add(fields);
        }
        return JSON.toJson(array);
    }

    private void bufferRow(String columnName, ColumnHistogram histogram) {
        String tableUUID = StatisticUtils.hashTableUuidForPkStorage(table.getUUID());
        String buckets = histogram.buckets.isEmpty() ? null : toJson(histogram.buckets);
        String mcv = histogram.mcv.isEmpty() ? null : toJson(histogram.mcv);

        List<Expr> row = Lists.newArrayList();
        row.add(new StringLiteral(tableUUID));
        row.add(new StringLiteral(columnName));
        row.add(new StringLiteral(catalogName));
        row.add(new StringLiteral(db.getOriginName()));
        row.add(new StringLiteral(table.getName()));
        row.add(buckets == null ? new NullLiteral() : new StringLiteral(buckets));
        row.add(mcv == null ? new NullLiteral() : new StringLiteral(mcv));
        row.add(nowFn());
        rowsBuffer.add(row);

        List<String> params = Lists.newArrayList();
        params.add("'" + SqlUtils.escapeSqlString(tableUUID) + "'");
        params.add("'" + SqlUtils.escapeSqlString(columnName) + "'");
        params.add("'" + SqlUtils.escapeSqlString(catalogName) + "'");
        params.add("'" + SqlUtils.escapeSqlString(db.getOriginName()) + "'");
        params.add("'" + SqlUtils.escapeSqlString(table.getName()) + "'");
        params.add(buckets == null ? "NULL" : "'" + SqlUtils.escapeSqlString(buckets) + "'");
        params.add(mcv == null ? "NULL" : "'" + SqlUtils.escapeSqlString(mcv) + "'");
        params.add("now()");
        sqlBuffer.add("(" + String.join(", ", params) + ")");
    }

    private void flushInsertStatisticsData(ConnectContext context) throws Exception {
        if (rowsBuffer.isEmpty()) {
            return;
        }

        int count = 0;
        int maxRetryTimes = 5;
        StatementBase insertStmt = createInsertStmt();
        do {
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, insertStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            context.setStartTime();
            executor.execute();

            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                LOG.warn("external histogram statistics collect fail | {} | Error Message [{}]",
                        DebugUtil.printId(context.getQueryId()), context.getState().getErrorMessage());
                if (StringUtils.contains(context.getState().getErrorMessage(), "Too many versions")) {
                    Thread.sleep(Config.statistic_collect_too_many_version_sleep);
                    count++;
                } else {
                    throw new DdlException(context.getState().getErrorMessage());
                }
            } else {
                sqlBuffer.clear();
                rowsBuffer.clear();
                return;
            }
        } while (count < maxRetryTimes);

        throw new DdlException(context.getState().getErrorMessage());
    }

    private StatementBase createInsertStmt() {
        List<String> targetColumnNames = StatisticUtils.buildStatsColumnDef(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME)
                .stream().map(ColumnDef::getName).collect(Collectors.toList());

        String sql = "INSERT INTO " + STATISTICS_DB_NAME + "." + EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME + "("
                + String.join(", ", targetColumnNames) + ") values " + String.join(", ", sqlBuffer) + ";";
        QueryStatement qs = new QueryStatement(new ValuesRelation(rowsBuffer, targetColumnNames));
        TableRef tableRef = new TableRef(
                QualifiedName.of(Lists.newArrayList(STATISTICS_DB_NAME, EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME)),
                null, NodePosition.ZERO);
        InsertStmt insert = new InsertStmt(tableRef, qs);
        insert.setTargetColumnNames(targetColumnNames);
        insert.setOrigStmt(new OriginStatement(sql, 0));
        return insert;
    }
}
