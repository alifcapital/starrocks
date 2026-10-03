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
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.IcebergTable;
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
import com.starrocks.sql.ast.StatisticsType;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.Snapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.EXTERNAL_MCV_STATISTICS_TABLE_NAME;
import static com.starrocks.statistic.StatsConstants.STATISTICS_DB_NAME;

/**
 * Collects the MCV statistics of a column set of an external table: the number of distinct value
 * tuples and the most common tuples with exact row counts. A bounded batch of groups shares two scans
 * and one aggregate operator per scan.
 * Each group has separate sketch/counting state, independent of the number of distinct tuples:
 *
 * 1. count(*), a frequent-items sketch over the tuple key and an HLL sketch over the tuple key;
 *    the sketch names the candidate most common tuples. An ordered singleton also collects KLL bounds.
 * 2. histogram_by_bounds over the tuple key with the candidates: exact row counts of the candidates;
 *    in the same scan, histogram_by_bounds over each column with the candidates' components: the
 *    rows holding each component value on its own, whatever the other columns hold. The same scan
 *    counts NULLs and the singleton's residual buckets, excluding exactly the same candidate head.
 *
 * The tuple key is stats_tuple_key over the columns (see StatsTupleKeyCodec). Results go to
 * _statistics_.external_mcv_statistics, one row per column group.
 */
public class ExternalMcvStatisticsCollectJob extends StatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(ExternalMcvStatisticsCollectJob.class);

    // 2^17 HLL registers: about 0.3% relative error on the combined NDV.
    private static final int NDV_SKETCH_LG_K = 17;
    // Values are stored as plain JSON text; no HTML escaping of quotes and angle brackets.
    private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().create();

    private final String catalogName;
    private final List<String> sqlBuffer = Lists.newArrayList();
    private final List<List<Expr>> rowsBuffer = Lists.newArrayList();

    public ExternalMcvStatisticsCollectJob(String catalogName, Database db, Table table,
                                                   List<String> columnNames, List<Type> columnTypes,
                                                   StatsConstants.AnalyzeType type,
                                                   StatsConstants.ScheduleType scheduleType,
                                                   Map<String, String> properties,
                                                   List<StatisticsType> statisticsTypes,
                                                   List<List<String>> columnGroups) {
        super(db, table, columnNames, columnTypes, type, scheduleType, properties, statisticsTypes, columnGroups);
        this.catalogName = catalogName;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getName() {
        return "ExternalMcv";
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        if (columnGroups.stream().anyMatch(group -> group.size() == 1)) {
            // Config can change after ANALYZE was accepted. Reject before metadata access or either scan.
            bucketNum();
        }
        setDefaultSessionVariable(context);
        // Capture once for every group and both passes. Later writes make the statistics stale,
        // but must not mix one snapshot's NDV/bounds with another snapshot's counts.
        String source = captureSource();
        LOG.info("Collecting external MCV statistics from {}", source);
        long finished = 0;
        long total = Math.max(1, columnGroups.size());
        int batchSize = Math.max(1, Config.statistic_mcv_max_groups_per_scan);
        for (int start = 0; start < columnGroups.size(); start += batchSize) {
            checkCancelled(analyzeStatus);
            List<List<String>> batch = columnGroups.subList(start, Math.min(columnGroups.size(), start + batchSize));
            List<GroupStatistics> results = source == null
                    ? batch.stream().map(ExternalMcvStatisticsCollectJob::emptyGroup).toList()
                    : collectBatch(context, analyzeStatus, batch, source);
            for (int i = 0; i < batch.size(); i++) {
                bufferRow(batch.get(i), results.get(i));
            }
            checkCancelled(analyzeStatus);
            calculateAndSetRemainingTimeout(context, analyzeStatus);
            flushInsertStatisticsData(context);
            finished += batch.size();
            analyzeStatus.setProgress(finished * 100 / total);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        }
    }

    static class GroupStatistics {
        final long rowCount;
        final long ndv;
        // Most common tuples, largest count first; a null value is a NULL column value.
        final List<McvTuple> mcv;
        final List<List<String>> buckets;
        final List<Long> nullCounts;

        GroupStatistics(long rowCount, long ndv, List<McvTuple> mcv,
                        List<List<String>> buckets, List<Long> nullCounts) {
            this.rowCount = rowCount;
            this.ndv = ndv;
            this.mcv = mcv;
            this.buckets = buckets;
            this.nullCounts = nullCounts;
        }
    }

    static class McvTuple {
        final List<String> values;
        final long count;
        // Rows holding each component value in its column; rows with NULL for a null component.
        final List<Long> componentCounts;

        McvTuple(List<String> values, long count) {
            this(values, count, List.of());
        }

        McvTuple(List<String> values, long count, List<Long> componentCounts) {
            this.values = values;
            this.count = count;
            this.componentCounts = componentCounts;
        }
    }

    private static GroupStatistics emptyGroup(List<String> group) {
        return new GroupStatistics(0, 0, List.of(), List.of(), java.util.Collections.nCopies(group.size(), 0L));
    }

    private boolean ordered(List<String> group) {
        return group.size() == 1 && supportsBuckets(table.getColumn(group.get(0)).getType());
    }

    // All aggregates consume one input stream. No CTE consumers/multicast buffers and no row multiplication.
    String buildBatchFromClause(List<List<String>> groups, String source) {
        List<String> projections = new ArrayList<>();
        for (int i = 0; i < groups.size(); i++) {
            List<String> group = groups.get(i);
            String prefix = "g" + i + "_";
            projections.add(buildProjection(table, group, prefix));
            if (ordered(group)) {
                projections.add(StatisticUtils.quoting(table, group.get(0)) + " AS " + prefix + "x");
            }
        }
        return " FROM (SELECT " + String.join(", ", projections) + " FROM " + source + ") t";
    }

    List<GroupStatistics> collectBatch(ConnectContext context, AnalyzeStatus status,
                                      List<List<String>> groups, String source) throws Exception {
        String from = buildBatchFromClause(groups, source);
        List<String> sketches = new ArrayList<>();
        for (int i = 0; i < groups.size(); i++) {
            sketches.add(buildSketchExpressions("g" + i + "_", ordered(groups.get(i))));
        }
        List<String> sketchRow = singleRow(execute(context, status,
                "SELECT " + String.join(", ", sketches) + from), "sketch");
        List<Long> ndvs = new ArrayList<>();
        List<String> counts = new ArrayList<>();
        int offset = 0;
        for (int i = 0; i < groups.size(); i++) {
            List<String> group = groups.get(i);
            boolean ordered = ordered(group);
            int length = ordered ? 4 : 3;
            if (sketchRow.size() < offset + length) {
                throw new DdlException("Incomplete MCV sketch result for " + group);
            }
            if (parseLong(sketchRow.get(offset)) == 0) {
                // Every group describes the same captured source, so an empty input empties the whole batch.
                return groups.stream().map(ExternalMcvStatisticsCollectJob::emptyGroup).toList();
            }
            ndvs.add(parseLong(sketchRow.get(offset + 2)));
            counts.add(buildExactCountExpressions("g" + i + "_", parseFrequentItems(sketchRow.get(offset + 1)),
                    group.size(), ordered, ordered ? parseBounds(sketchRow.get(offset + 3)) : List.of()));
            offset += length;
        }
        List<String> countRow = singleRow(execute(context, status,
                "SELECT " + String.join(", ", counts) + from), "count");
        List<GroupStatistics> result = new ArrayList<>();
        offset = 0;
        for (int i = 0; i < groups.size(); i++) {
            List<String> group = groups.get(i);
            int width = group.size();
            int length = 2 + 2 * width + (ordered(group) ? 1 : 0);
            if (countRow.size() < offset + length) {
                throw new DdlException("Incomplete MCV count result for " + group);
            }
            List<String> row = countRow.subList(offset, offset + length);
            if (row.get(0) == null) {
                result.add(emptyGroup(group));
            } else {
                GroupStatistics parsed = parseExactCounts(row, width, ndvs.get(i));
                result.add(new GroupStatistics(parsed.rowCount, parsed.ndv, parsed.mcv,
                        ordered(group) ? parseBuckets(row.get(2 + 2 * width)) : List.of(), parsed.nullCounts));
            }
            offset += length;
        }
        return result;
    }

    private static List<String> singleRow(List<List<String>> rows, String phase) throws DdlException {
        if (rows.size() != 1) {
            throw new DdlException("MCV " + phase + " query must return one aggregate row");
        }
        return rows.get(0);
    }

    // null denotes an Iceberg table with no snapshot yet: collect the captured empty state without
    // accidentally reading a first commit that appears while this job is running.
    String captureSource() {
        String source = "`" + catalogName + "`.`" + db.getOriginName() + "`.`" + table.getName() + "`";
        if (table instanceof IcebergTable) {
            Snapshot snapshot = ((IcebergTable) table).getNativeTable().currentSnapshot();
            return snapshot == null ? null : source + " FOR VERSION AS OF " + snapshot.snapshotId();
        }
        return source;
    }

    String buildFromClause(List<String> columnGroup, String source) {
        String rawColumn = columnGroup.size() == 1
                ? ", " + StatisticUtils.quoting(table, columnGroup.get(0)) + " AS x" : "";
        return " FROM (SELECT " + buildProjection(table, columnGroup) + rawColumn + " FROM " + source + ") t";
    }

    // k: the tuple key; v0, v1, ...: the text of each column value as the key holds it.
    static String buildProjection(Table table, List<String> columnGroup) {
        return buildProjection(table, columnGroup, "");
    }

    private static String buildProjection(Table table, List<String> columnGroup, String prefix) {
        StringBuilder projection = new StringBuilder(StatsTupleKeyCodec.buildKeyExpr(table, columnGroup))
                .append(" AS ").append(prefix).append("k");
        for (int i = 0; i < columnGroup.size(); i++) {
            projection.append(", ").append(StatsTupleKeyCodec.buildComponentExpr(table, columnGroup.get(i)))
                    .append(" AS ").append(prefix).append("v").append(i);
        }
        return projection.toString();
    }

    String buildSketchSQL(String from) {
        return buildSketchSQL(from, false);
    }

    String buildSketchSQL(String from, boolean ordered) {
        return "SELECT " + buildSketchExpressions("", ordered) + from;
    }

    private String buildSketchExpressions(String prefix, boolean ordered) {
        return "count(*), ds_frequent_items(" + prefix + "k, " + mcvSize() + ", "
                + Config.statistic_mcv_sketch_lg_map_size + "), ds_hll_count_distinct(" + prefix + "k, "
                + NDV_SKETCH_LG_K + ")"
                + (ordered ? ", ds_kll_quantiles(" + prefix + "x, " + bucketNum() + ")" : "");
    }

    /**
     * Counts the candidate tuples and, per column, the distinct component values of the candidates.
     * A NULL component is counted from count(*) and the column's count().
     */
    String buildExactCountSQL(String from, List<String> candidates, int width) {
        return buildExactCountSQL(from, candidates, width, false, List.of());
    }

    String buildExactCountSQL(String from, List<String> candidates, int width, boolean ordered, List<String> bounds) {
        return "SELECT " + buildExactCountExpressions("", candidates, width, ordered, bounds) + from;
    }

    private String buildExactCountExpressions(String prefix, List<String> candidates, int width,
                                             boolean ordered, List<String> bounds) {
        List<Set<String>> components = new ArrayList<>(width);
        for (int i = 0; i < width; i++) {
            components.add(new LinkedHashSet<>());
        }
        for (String candidate : candidates) {
            List<String> values = decodeTuple(candidate, width);
            for (int i = 0; i < width; i++) {
                if (values.get(i) != null) {
                    components.get(i).add(values.get(i));
                }
            }
        }
        StringBuilder sql = new StringBuilder("histogram_by_bounds(").append(prefix).append("k, '")
                .append(SqlUtils.escapeSqlString(toJsonArray(candidates))).append("', '[]'), count(*)");
        for (int i = 0; i < width; i++) {
            sql.append(", histogram_by_bounds(").append(prefix).append("v").append(i).append(", '")
                    .append(SqlUtils.escapeSqlString(toJsonArray(components.get(i)))).append("', '[]'), count(")
                    .append(prefix).append("v")
                    .append(i).append(")");
        }
        if (ordered) {
            sql.append(", histogram_by_bounds(").append(prefix).append("x, '")
                    .append(SqlUtils.escapeSqlString(toJsonArray(components.get(0)))).append("', '")
                    .append(SqlUtils.escapeSqlString(toJsonArray(bounds))).append("')");
        }
        return sql.toString();
    }

    private static String toJsonArray(Collection<String> values) {
        JsonArray array = new JsonArray();
        for (String value : values) {
            array.add(value);
        }
        return JSON.toJson(array);
    }

    private static List<String> decodeTuple(String key, int width) {
        List<String> values = StatsTupleKeyCodec.decode(key);
        if (values.size() != width) {
            throw new IllegalStateException("Statistics tuple key " + key + " does not have " + width + " components");
        }
        return values;
    }

    // The MCV list is what the statistics are for: at least one tuple whatever the property says.
    private long mcvSize() {
        String property = properties == null ? null : properties.get(StatsConstants.MCV_SIZE);
        return Math.max(1, property == null ? Config.statistic_mcv_size : Long.parseLong(property));
    }

    private long bucketNum() {
        String property = properties == null ? null : properties.get(StatsConstants.MCV_BUCKET_NUM);
        long buckets = property == null ? Config.statistic_mcv_bucket_num : Long.parseLong(property);
        if (buckets < 1 || buckets > FunctionSet.DS_KLL_MAX_BUCKETS) {
            throw new IllegalArgumentException("mcv_bucket_num must be between 1 and " + FunctionSet.DS_KLL_MAX_BUCKETS);
        }
        return buckets;
    }

    static boolean supportsBuckets(Type type) {
        return type.isNumericType() || type.isDate() || type.isDatetime() || type.isStringType();
    }

    static List<String> parseBounds(String json) {
        List<String> bounds = new ArrayList<>();
        if (json != null) {
            for (JsonElement value : JsonParser.parseString(json).getAsJsonArray()) {
                bounds.add(value.getAsString());
            }
        }
        return bounds;
    }

    // BE returns independent bucket counts; the planner consumes cumulative counts.
    static List<List<String>> parseBuckets(String json) {
        List<List<String>> buckets = new ArrayList<>();
        if (json == null) {
            return buckets;
        }
        long cumulative = 0;
        for (JsonElement value : JsonParser.parseString(json).getAsJsonObject().getAsJsonArray("buckets")) {
            JsonArray bucket = value.getAsJsonArray();
            long count = bucket.get(2).getAsLong();
            cumulative += count;
            buckets.add(List.of(bucket.get(0).getAsString(), bucket.get(1).getAsString(),
                    Long.toString(cumulative), bucket.get(3).getAsString(),
                    Long.toString(Math.min(count, bucket.get(4).getAsLong()))));
        }
        return buckets;
    }

    List<List<String>> execute(ConnectContext context, AnalyzeStatus analyzeStatus, String sql)
            throws DdlException {
        checkCancelled(analyzeStatus);
        calculateAndSetRemainingTimeout(context, analyzeStatus);
        LOG.debug("external MCV statistics collect sql : {}", sql);
        return new StatisticExecutor().executeStatisticJsonDQL(context, sql);
    }

    private static long parseLong(String text) {
        return text == null ? 0 : Long.parseLong(text);
    }

    // ds_frequent_items result: [["key","estimate"], ...], most frequent first.
    static List<String> parseFrequentItems(String json) {
        List<String> keys = new ArrayList<>();
        if (json == null) {
            return keys;
        }
        for (JsonElement entry : JsonParser.parseString(json).getAsJsonArray()) {
            keys.add(entry.getAsJsonArray().get(0).getAsString());
        }
        return keys;
    }

    /**
     * The row of the exact count query: [tuple counts, count(*), column 0 counts, count(v0), column 1
     * counts, count(v1), ...]. Candidates with no rows are dropped; the rest are ordered by count.
     */
    static GroupStatistics parseExactCounts(List<String> row, int width, long ndv) {
        long rowCount = parseLong(row.get(1));
        List<Map<String, Long>> componentCounts = new ArrayList<>(width);
        List<Long> nullCounts = new ArrayList<>(width);
        for (int i = 0; i < width; i++) {
            Map<String, Long> counts = parseCounts(row.get(2 + 2 * i));
            counts.put(null, rowCount - parseLong(row.get(3 + 2 * i)));
            nullCounts.add(counts.get(null));
            componentCounts.add(counts);
        }
        List<McvTuple> mcv = new ArrayList<>();
        for (Map.Entry<String, Long> tuple : parseCounts(row.get(0)).entrySet()) {
            if (tuple.getValue() <= 0) {
                continue;
            }
            List<String> values = decodeTuple(tuple.getKey(), width);
            List<Long> counts = new ArrayList<>(width);
            for (int i = 0; i < width; i++) {
                // A column holds a component value in at least the rows of the tuple.
                counts.add(Math.max(tuple.getValue(), componentCounts.get(i).getOrDefault(values.get(i), 0L)));
            }
            mcv.add(new McvTuple(values, tuple.getValue(), counts));
        }
        mcv.sort(Comparator.comparingLong((McvTuple tuple) -> tuple.count).reversed());
        long headRows = mcv.stream().mapToLong(tuple -> tuple.count).sum();
        long distinct = headRows == rowCount ? mcv.size() : Math.min(rowCount, Math.max(ndv, mcv.size()));
        return new GroupStatistics(rowCount, distinct, mcv, List.of(), nullCounts);
    }

    // histogram_by_bounds result: {"mcv":[["value","count"],...],"buckets":[]} as value -> count.
    static Map<String, Long> parseCounts(String json) {
        Map<String, Long> counts = new LinkedHashMap<>();
        if (json == null) {
            return counts;
        }
        for (JsonElement entry : JsonParser.parseString(json).getAsJsonObject().getAsJsonArray("mcv")) {
            JsonArray pair = entry.getAsJsonArray();
            counts.put(pair.get(0).getAsString(), Long.parseLong(pair.get(1).getAsString()));
        }
        return counts;
    }

    // Stored MCV text: [[[value, value, ...], "count", ["component count", ...]], ...]; a NULL column
    // value is a JSON null.
    static String buildMcvJson(List<McvTuple> mcv) {
        JsonArray array = new JsonArray();
        for (McvTuple tuple : mcv) {
            JsonArray values = new JsonArray();
            for (String value : tuple.values) {
                values.add(value == null ? JsonNull.INSTANCE : new JsonPrimitive(value));
            }
            JsonArray entry = new JsonArray();
            entry.add(values);
            entry.add(String.valueOf(tuple.count));
            if (!tuple.componentCounts.isEmpty()) {
                JsonArray counts = new JsonArray();
                for (Long count : tuple.componentCounts) {
                    counts.add(String.valueOf(count));
                }
                entry.add(counts);
            }
            array.add(entry);
        }
        return JSON.toJson(array);
    }

    // The key of the column group in the statistics table: a digest of the sorted names, so it is the
    // same whatever the order the group was given in and a recollection replaces the row, it fits the
    // primary key size limit whatever the names, and no two groups share it. column_names keeps the
    // names in tuple order.
    static String buildColumnIds(List<String> columnGroup) {
        String names = JSON.toJson(columnGroup.stream().sorted().collect(Collectors.toList()));
        return Hashing.murmur3_128().hashUnencodedChars(names).toString();
    }

    static String buildColumnNamesJson(List<String> columnGroup) {
        JsonArray array = new JsonArray();
        for (String column : columnGroup) {
            array.add(column);
        }
        return JSON.toJson(array);
    }

    private void bufferRow(List<String> columnGroup, GroupStatistics statistics) {
        String tableUUID = StatisticUtils.hashTableUuidForPkStorage(table.getUUID());
        String columnIds = buildColumnIds(columnGroup);
        String columnNamesJson = buildColumnNamesJson(columnGroup);
        String mcvJson = buildMcvJson(statistics.mcv);
        String bucketsJson = JSON.toJson(statistics.buckets);
        String nullCountsJson = JSON.toJson(statistics.nullCounts);

        List<Expr> row = Lists.newArrayList();
        row.add(new StringLiteral(tableUUID));
        row.add(new StringLiteral(columnIds));
        row.add(new StringLiteral(catalogName));
        row.add(new StringLiteral(db.getOriginName()));
        row.add(new StringLiteral(table.getName()));
        row.add(new StringLiteral(columnNamesJson));
        row.add(new IntLiteral(statistics.rowCount, IntegerType.BIGINT));
        row.add(new IntLiteral(statistics.ndv, IntegerType.BIGINT));
        row.add(new StringLiteral(mcvJson));
        row.add(new StringLiteral(bucketsJson));
        row.add(new StringLiteral(nullCountsJson));
        row.add(nowFn());
        rowsBuffer.add(row);

        List<String> params = Lists.newArrayList();
        params.add("'" + SqlUtils.escapeSqlString(tableUUID) + "'");
        params.add("'" + SqlUtils.escapeSqlString(columnIds) + "'");
        params.add("'" + SqlUtils.escapeSqlString(catalogName) + "'");
        params.add("'" + SqlUtils.escapeSqlString(db.getOriginName()) + "'");
        params.add("'" + SqlUtils.escapeSqlString(table.getName()) + "'");
        params.add("'" + SqlUtils.escapeSqlString(columnNamesJson) + "'");
        params.add(String.valueOf(statistics.rowCount));
        params.add(String.valueOf(statistics.ndv));
        params.add("'" + SqlUtils.escapeSqlString(mcvJson) + "'");
        params.add("'" + SqlUtils.escapeSqlString(bucketsJson) + "'");
        params.add("'" + SqlUtils.escapeSqlString(nullCountsJson) + "'");
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
            LOG.debug("external MCV statistics insert rows: {}", rowsBuffer.size());
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, insertStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            context.setStartTime();
            executor.execute();

            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                LOG.warn("external MCV statistics collect fail | {} | Error Message [{}]",
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
        List<String> targetColumnNames = StatisticUtils.buildStatsColumnDef(EXTERNAL_MCV_STATISTICS_TABLE_NAME)
                .stream().map(ColumnDef::getName).collect(Collectors.toList());

        String sql = "INSERT INTO " + STATISTICS_DB_NAME + "." + EXTERNAL_MCV_STATISTICS_TABLE_NAME + "("
                + String.join(", ", targetColumnNames) + ") values " + String.join(", ", sqlBuffer) + ";";
        QueryStatement qs = new QueryStatement(new ValuesRelation(rowsBuffer, targetColumnNames));
        TableRef tableRef = new TableRef(
                QualifiedName.of(Lists.newArrayList(STATISTICS_DB_NAME, EXTERNAL_MCV_STATISTICS_TABLE_NAME)),
                null, NodePosition.ZERO);
        InsertStmt insert = new InsertStmt(tableRef, qs);
        insert.setTargetColumnNames(targetColumnNames);
        insert.setOrigStmt(new OriginStatement(sql, 0));
        return insert;
    }
}
