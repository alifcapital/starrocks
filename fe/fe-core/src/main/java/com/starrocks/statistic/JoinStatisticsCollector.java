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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.statistics.CompactDegreeVector;
import com.starrocks.sql.optimizer.statistics.DegreeStatistics;
import com.starrocks.sql.optimizer.statistics.JoinStatisticsBasis;
import com.starrocks.sql.optimizer.statistics.JoinStatisticsCorrelation;
import com.starrocks.sql.optimizer.statistics.JoinStatisticsData;
import com.starrocks.type.Type;
import org.apache.iceberg.Snapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/** Candidate selection precedes exact spilled degree aggregation; FE receives bounded summaries only. */
public final class JoinStatisticsCollector implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(JoinStatisticsCollector.class);
    private static final String DATABASE = "default_catalog." + StatsConstants.STATISTICS_DB_NAME;
    private final ConnectContext context;
    private final JoinStatisticsRegistry.Collection ticket;
    private final JoinStatisticsDefinition definition;
    private final Runnable checkLeadership;
    private final List<String> scratchTables = new ArrayList<>();
    private final List<Source> sources = new ArrayList<>();
    private final long started = System.nanoTime();
    private boolean boundedJoin;
    private long preparedBytes;

    private static final class Source {
        private Table table;
        private String scan;
        private long snapshot;
        private long databaseId;
        private long rows;
        private List<Type> types;
        private List<List<String>> tuples;
        private long[] tupleRows;
        private String joint;
        private final Map<Integer, String> degrees = new LinkedHashMap<>();
        private final Map<Integer, String> intraDegrees = new LinkedHashMap<>();
        private final Map<Integer, String> totals = new LinkedHashMap<>();
        private final Map<Integer, List<DegreeStatistics>> moments = new LinkedHashMap<>();
    }

    public JoinStatisticsCollector(ConnectContext context, JoinStatisticsRegistry.Collection ticket,
                                   Runnable checkLeadership) {
        this.context = context;
        this.ticket = ticket;
        this.definition = ticket.getPrevious().getDefinition();
        this.checkLeadership = checkLeadership;
    }

    public JoinStatisticsData collect() throws Exception {
        configure();
        // Capture all source versions before the first scan. Independent tables need not share a transaction.
        Map<String, Source> readVersions = new LinkedHashMap<>();
        for (JoinStatisticsDefinition.Source declared : definition.getSources()) {
            Source source = new Source();
            source.table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getTable(context, declared.getTableName()).orElseThrow(() ->
                            new DdlException("JOIN statistics source no longer exists: " + declared.getTableName()));
            if (!source.table.getUUID().equals(declared.getTableUuid())) {
                throw new DdlException("JOIN statistics source was replaced: " + declared.getTableName());
            }
            var name = declared.getTableName();
            source.scan = ident(name.getCatalog()) + "." + ident(name.getDb()) + "." + ident(name.getTbl());
            Source prior = readVersions.get(declared.getTableUuid());
            if (prior != null) {
                source.snapshot = prior.snapshot;
                source.scan = prior.scan;
                source.databaseId = prior.databaseId;
            } else if (source.table instanceof IcebergTable iceberg) {
                Snapshot snapshot = iceberg.getNativeTable().currentSnapshot();
                source.snapshot = snapshot == null ? -1 : snapshot.snapshotId();
                source.scan = snapshot == null ? null : source.scan + " FOR VERSION AS OF " + source.snapshot;
            } else if (source.table instanceof OlapTable nativeTable && source.table.isNativeTableOrMaterializedView()) {
                source.databaseId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(name.getDb()).getId();
                source.snapshot = nativeVersion(nativeTable, source.databaseId);
            } else {
                throw new DdlException("JOIN statistics require Iceberg or native tables: " + declared.getTableName());
            }
            readVersions.putIfAbsent(declared.getTableUuid(), source);
            source.types = declared.getPredicates().stream()
                    .map(column -> StatisticUtils.getQueryStatisticsColumnType(source.table, column)).toList();
            for (JoinStatisticsDefinition.KeyDomain domain : definition.getDomains()) {
                List<String> keys = domain.getColumns().get(sources.size());
                if (keys != null) {
                    for (int key = 0; key < keys.size(); key++) {
                        Type actual = StatisticUtils.getQueryStatisticsColumnType(source.table, keys.get(key));
                        if (!JoinStatisticsDefinition.matchesKeyType(actual, domain.getTypes().get(key))) {
                            throw new DdlException("JOIN statistics key type changed: " + keys.get(key));
                        }
                    }
                }
            }
            sources.add(source);
        }
        List<JoinStatisticsData.Source> prepared = new ArrayList<>();
        List<JoinStatisticsData.IntraCorrelation> intra = new ArrayList<>();
        for (int i = 0; i < sources.size(); i++) {
            collectSource(i);
            Source source = sources.get(i);
            if (source.table instanceof OlapTable nativeTable && nativeVersion(nativeTable, source.databaseId) != source.snapshot) {
                // Native ANALYZE, like ordinary statistics collection, need not stop concurrent writes.
                // Do not claim the two collection passes form a reusable stable-version cohort.
                source.snapshot = -ticket.getGeneration() - 1;
            }
            List<Integer> localDomains = source.degrees.keySet().stream().sorted().toList();
            for (int a = 0; a < localDomains.size(); a++) {
                for (int b = a + 1; b < localDomains.size(); b++) {
                    intra.add(collectIntra(i, localDomains.get(a), localDomains.get(b)));
                }
            }
        }
        List<JoinStatisticsBasis> bases = new ArrayList<>();
        for (int domain = 0; domain < definition.getDomains().size(); domain++) {
            List<Integer> participants = definition.getDomains().get(domain).getColumns().keySet().stream().sorted().toList();
            if (compactDegrees() || participants.stream().noneMatch(source -> sources.get(source).tuples.isEmpty())) {
                JoinStatisticsBasis basis = collectBasis(domain, participants);
                if (basis != null) {
                    bases.add(basis);
                }
            }
        }
        for (int i = 0; i < sources.size(); i++) {
            Source source = sources.get(i);
            if (source.table instanceof OlapTable) {
                // Inserts between candidate selection and exact counting may increase the covered mass.
                source.rows = Math.max(source.rows, Arrays.stream(source.tupleRows).reduce(0, Math::addExact));
            }
            prepared.add(new JoinStatisticsData.Source(definition.getSources().get(i).getUuid(), source.snapshot,
                    source.rows, definition.getSources().get(i).getPredicates(), source.types, source.tuples,
                    source.tupleRows, source.moments));
        }
        check();
        return new JoinStatisticsData(ticket.getPrevious().getId(), ticket.getGeneration(), prepared, bases, intra);
    }

    static long nativeVersion(OlapTable table, long databaseId) {
        // A version fingerprint rejects incompatible cohorts; it is not a historical read.
        var locker = new com.starrocks.common.util.concurrent.lock.Locker();
        var mode = com.starrocks.common.util.concurrent.lock.LockType.READ;
        locker.lockTablesWithIntensiveDbLock(databaseId, List.of(table.getId()), mode);
        try {
            return com.starrocks.sql.optimizer.statistics.JoinStatisticsTableState.nativeVersion(table);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(databaseId, List.of(table.getId()), mode);
        }
    }

    private void configure() throws Exception {
        if (Config.statistic_join_collect_memory_limit <= 0) {
            throw new DdlException("statistic_join_collect_memory_limit must be positive");
        }
        context.getSessionVariable().setUsePageCache(false);
        context.getSessionVariable().setEnableProfile(Config.enable_statistics_collect_profile);
        execute("SET enable_spill = true, spill_mode = 'auto', spill_mem_table_size = 67108864, "
                + "spill_revocable_max_bytes = 0, spill_mem_limit_threshold = 0.5, "
                + "max_spill_read_buffer_bytes_per_driver = 4194304, "
                + "pipeline_dop = 4, pipeline_sink_dop = 4, enable_sort_aggregate = true, "
                + "disable_join_reorder = true, enable_insert_strict = true, "
                + "query_mem_limit = " + Config.statistic_join_collect_memory_limit
                + ", load_mem_limit = " + Math.min(512L * 1024 * 1024, Config.statistic_join_collect_memory_limit));
    }

    private void collectSource(int index) throws Exception {
        Source source = sources.get(index);
        List<String> predicates = definition.getSources().get(index).getPredicates();
        String tupleKey = predicates.isEmpty() ? "''" : StatsTupleKeyCodec.buildKeyExpr(source.table, predicates);
        int candidates = Integer.parseInt(definition.getProperties().getOrDefault(StatsConstants.MCV_SIZE,
                Integer.toString(Config.statistic_mcv_size)));
        if (candidates < 1 || candidates > JoinStatisticsData.MAX_SLICES) {
            throw new DdlException("mcv_size exceeds the JOIN statistics slice limit: " + JoinStatisticsData.MAX_SLICES);
        }
        List<String> keys = new ArrayList<>();
        if (source.scan != null) {
            var rows = query("SELECT count(*), ds_frequent_items(" + tupleKey + ", " + candidates + ", "
                    + Config.statistic_mcv_sketch_lg_map_size + ") FROM " + source.scan);
            source.rows = number(rows.get(0).get(0));
            keys.addAll(ExternalMcvStatisticsCollectJob.parseFrequentItems(rows.get(0).get(1)));
        }
        source.tuples = new ArrayList<>();
        for (String key : keys) {
            source.tuples.add(predicates.isEmpty() ? List.of() : StatsTupleKeyCodec.decode(key));
        }
        source.tupleRows = new long[keys.size()];
        String dictionary = create("cid INT, p VARCHAR(1048576)", "cid", "cid", 1);
        for (int start = 0; start < keys.size(); start += 100) {
            List<String> values = new ArrayList<>();
            for (int i = start; i < Math.min(keys.size(), start + 100); i++) {
                values.add("(" + i + ", " + literal(keys.get(i)) + ")");
            }
            execute("INSERT INTO " + dictionary + " VALUES " + String.join(",", values));
        }
        List<Integer> domains = new ArrayList<>();
        List<String> fields = new ArrayList<>();
        List<String> projections = new ArrayList<>(List.of(tupleKey + " AS predicate_tuple"));
        List<String> groupBy = new ArrayList<>(List.of("c.cid"));
        for (int d = 0; d < definition.getDomains().size(); d++) {
            JoinStatisticsDefinition.KeyDomain domain = definition.getDomains().get(d);
            if (!domain.getColumns().containsKey(index)) {
                continue;
            }
            domains.add(d);
            fields.add("k" + d + " " + storageType(d));
            projections.add(keyExpression(source, domain, index) + " AS k" + d);
            groupBy.add("k" + d);
        }
        if (compactDegrees()) {
            collectCompactSource(source, domains, keys, projections, dictionary);
            return;
        }
        String keyNames = domains.stream().map(d -> "k" + d).collect(Collectors.joining(","));
        fields.add("cid INT");
        fields.add("d BIGINT");
        source.joint = create(String.join(",", fields), keyNames + ",cid", "k" + domains.get(0), 8);
        if (!keys.isEmpty()) {
            execute("INSERT INTO " + source.joint + " SELECT " + keyNames + ",c.cid,count(*) FROM (SELECT "
                    + String.join(",", projections) + " FROM " + source.scan + ") s JOIN [BROADCAST] "
                    + dictionary + " c ON s.predicate_tuple = c.p GROUP BY "
                    + String.join(",", groupBy));
        }
        for (int domain : domains) {
            String degree = source.joint;
            if (domains.size() > 1) {
                degree = create("k" + domain + " " + storageType(domain)
                        + ",cid INT,d BIGINT", "k" + domain + ",cid", "k" + domain, 8);
                execute("INSERT INTO " + degree + " SELECT k" + domain + ",cid,SUM(d) FROM " + source.joint
                        + " GROUP BY cid,k" + domain);
            }
            source.degrees.put(domain, degree);
            source.moments.put(domain, collectMoments(source, degree, domain));
            String totals = create("k " + storageType(domain)
                    + ",n BIGINT", "k", "k", 8);
            execute("INSERT INTO " + totals + " SELECT k" + domain + ",SUM(d) FROM " + degree
                    + " WHERE k" + domain + " IS NOT NULL GROUP BY k" + domain);
            source.totals.put(domain, totals);
        }
    }

    private List<DegreeStatistics> collectMoments(Source source, String degree, int domain) throws Exception {
        String key = "k" + domain;
        List<String> aggregates = new ArrayList<>();
        for (int power = 1; power <= 10; power++) {
            aggregates.add("SUM(IF(" + key + " IS NULL,0," + power("CAST(d AS DOUBLE)", power) + "))");
        }
        var rows = query("SELECT cid,SUM(d),SUM(IF(" + key + " IS NULL,d,0)),COUNT(" + key + "),"
                + "MAX(IF(" + key + " IS NULL,0,d))," + String.join(",", aggregates) + " FROM " + degree + " GROUP BY cid");
        DegreeStatistics[] moments = new DegreeStatistics[source.tuples.size()];
        Arrays.fill(moments, new DegreeStatistics(0, 0, 0, 0, new double[10]));
        for (List<String> row : rows) {
            int cid = Integer.parseInt(row.get(0));
            double[] powers = new double[10];
            for (int power = 0; power < 10; power++) {
                powers[power] = Double.parseDouble(row.get(5 + power));
            }
            source.tupleRows[cid] = number(row.get(1));
            moments[cid] = new DegreeStatistics(source.tupleRows[cid], number(row.get(2)), number(row.get(3)),
                    number(row.get(4)), powers);
        }
        return List.of(moments);
    }

    private JoinStatisticsData.IntraCorrelation collectIntra(int index, int left, int right) throws Exception {
        Source source = sources.get(index);
        List<List<String>> rows;
        if (compactDegrees()) {
            execute("SET chunk_size=32");
            rows = query("SELECT cid,CAST(SUM(get_json_double(v,'$[0]')) AS BIGINT),SUM(get_json_double(v,'$[1]')),"
                    + "SUM(get_json_double(v,'$[2]')),SUM(get_json_double(v,'$[3]')) FROM (SELECT j.cid,"
                    + "stats_degree_intra(j.s,l.s,r.s,j.s0,j.s1) v FROM " + source.joint
                    + " j JOIN [SHUFFLE] " + source.intraDegrees.get(left)
                    + " l ON j.cid=l.cid AND j.s0=l.shard JOIN [SHUFFLE] "
                    + source.intraDegrees.get(right) + " r ON j.cid=r.cid AND j.s1=r.shard"
                    + ") p GROUP BY cid");
            execute("SET chunk_size=1024");
        } else {
            String weight = "(CAST(l.d AS DOUBLE)*r.d)";
            // Project observed tuples before joining marginal frequencies: a third key must not
            // multiply the support or moments of the selected pair.
            String joint = source.degrees.size() > 2
                    ? "(SELECT DISTINCT cid,k" + left + ",k" + right + " FROM " + source.joint + ")"
                    : source.joint;
            rows = queryHeavy("SELECT j.cid,COUNT(*),SUM(" + weight + "),SUM(" + power(weight, 2) + "),SUM("
                    + power(weight, 3) + ") FROM " + joint + " j JOIN [SHUFFLE] " + source.degrees.get(left)
                + " l ON j.cid=l.cid AND j.k" + left + "=l.k" + left + " JOIN [SHUFFLE] "
                + source.degrees.get(right) + " r ON j.cid=r.cid AND j.k" + right + "=r.k" + right + " GROUP BY j.cid");
        }
        long[] support = new long[source.tuples.size()];
        double[][] moments = new double[support.length][3];
        for (List<String> row : rows) {
            int cid = Integer.parseInt(row.get(0));
            support[cid] = number(row.get(1));
            for (int p = 0; p < 3; p++) {
                moments[cid][p] = Double.parseDouble(row.get(p + 2));
            }
        }
        return new JoinStatisticsData.IntraCorrelation(index, left, right, support, moments);
    }

    private JoinStatisticsBasis collectBasis(int domain, List<Integer> group) throws Exception {
        if (compactDegrees()) {
            return collectCompactBasis(domain, group);
        }
        // One dictionary covers the entire equality domain, including keys absent from some sources.
        // Ranking by aggregate pair contribution avoids throwing away useful subjoin keys merely because
        // they cannot occur in the full four-way intersection. There are no separate role/power heads.
        List<String> inputs = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        List<String> scores = new ArrayList<>();
        for (int side = 0; side < group.size(); side++) {
            inputs.add("SELECT k,n," + side + " side FROM " + sources.get(group.get(side)).totals.get(domain));
            columns.add("SUM(IF(side=" + side + ",CAST(n AS DOUBLE),0)) n" + side);
            for (int previous = 0; previous < side; previous++) {
                scores.add("n" + previous + "*n" + side);
            }
        }
        String head = create("k " + storageType(domain) + ",hid INT", "k", "k", 1);
        executeHeavy("INSERT INTO " + head + " SELECT k,ROW_NUMBER() OVER (ORDER BY score DESC,k)-1 FROM "
                + "(SELECT k," + String.join("+", scores) + " score FROM (SELECT k,"
                + String.join(",", columns) + " FROM (" + String.join(" UNION ALL ", inputs)
                + ") all_sources GROUP BY k) grouped WHERE " + String.join("+", scores)
                + " > 0 ORDER BY score DESC,k LIMIT " + JoinStatisticsCorrelation.HEAD_BUDGET + ") ranked");
        int headSize = Math.toIntExact(number(query("SELECT COUNT(*) FROM " + head).get(0).get(0)));
        List<List<JoinStatisticsBasis.Slice>> sides = new ArrayList<>();
        int[] orders = JoinStatisticsBasis.momentOrders();
        for (int sourceId : group) {
            Source source = sources.get(sourceId);
            boolean unit = source.moments.get(domain).stream().allMatch(d -> d.getMaximumFrequency() <= 1);
            int orderCount = unit ? 1 : orders.length;
            List<String> fields = new ArrayList<>(List.of("cid INT", "b INT"));
            List<String> aggregates = new ArrayList<>(List.of("COUNT(*)"));
            for (int i = 0; i < orderCount; i++) {
                fields.add("m" + i + " DOUBLE");
                if (i > 0) {
                    aggregates.add("SUM(" + power("CAST(d.d AS DOUBLE)", orders[i]) + ")");
                }
            }
            List<String> hash = new ArrayList<>();
            for (int layout = 0; layout < JoinStatisticsBasis.TAIL_LAYOUTS; layout++) {
                hash.add("((xx_hash3_64(CAST(d.k" + domain + " AS VARCHAR),'join-statistics-" + layout
                        + "') & " + (JoinStatisticsBasis.TAIL_BUCKETS - 1) + ")+"
                        + layout * JoinStatisticsBasis.TAIL_BUCKETS + ")");
            }
            String tail = create(String.join(",", fields), "cid,b", "cid", 8);
            // The only build is the bounded 16K head; aggregate groups are bounded by slices * bucket count.
            // Auto spill still enforces the query budget without force-spilling every source row.
            execute("INSERT INTO " + tail + " SELECT d.cid,unnest b," + String.join(",", aggregates)
                    + " FROM " + source.degrees.get(domain) + " d LEFT JOIN [BROADCAST] " + head
                    + " h ON d.k" + domain + "=h.k,UNNEST([" + String.join(",", hash)
                    + "]) AS unnest WHERE h.k IS NULL AND d.k" + domain + " IS NOT NULL GROUP BY d.cid,b");
            String headValues = create("cid INT,hid INT,d BIGINT", "cid,hid", "cid", 1);
            execute("INSERT INTO " + headValues + " SELECT d.cid,h.hid,d.d FROM "
                    + source.degrees.get(domain) + " d JOIN [BROADCAST] " + head + " h ON d.k" + domain + "=h.k");
            List<JoinStatisticsBasis.Slice> slices = new ArrayList<>();
            for (int start = 0; start < source.tuples.size(); start += 16) {
                int end = Math.min(source.tuples.size(), start + 16);
                String range = "cid >= " + start + " AND cid < " + end;
                CompactDegreeVector[] heads = new CompactDegreeVector[end - start];
                Arrays.fill(heads, CompactDegreeVector.copyOf(new long[headSize]));
                var values = query("SELECT cid,array_agg(hid ORDER BY hid),array_agg(d ORDER BY hid) FROM "
                        + headValues + " WHERE " + range + " GROUP BY cid");
                for (List<String> row : values) {
                    JsonArray positions = JsonParser.parseString(row.get(1)).getAsJsonArray();
                    JsonArray counts = JsonParser.parseString(row.get(2)).getAsJsonArray();
                    long[] vector = new long[headSize];
                    for (int i = 0; i < positions.size(); i++) {
                        vector[positions.get(i).getAsInt()] = counts.get(i).getAsLong();
                    }
                    heads[Integer.parseInt(row.get(0)) - start] = CompactDegreeVector.copyOf(vector);
                }
                double[][][] moments = new double[end - start][orderCount][JoinStatisticsBasis.WIDTH];
                boolean[] nonempty = new boolean[end - start];
                for (List<String> row : query("SELECT * FROM " + tail + " WHERE " + range)) {
                    int cid = Integer.parseInt(row.get(0)) - start;
                    int bucket = Integer.parseInt(row.get(1));
                    nonempty[cid] = true;
                    for (int order = 0; order < orderCount; order++) {
                        moments[cid][order][bucket] = Double.parseDouble(row.get(order + 2));
                    }
                }
                for (int i = 0; i < moments.length; i++) {
                    var slice = new JoinStatisticsBasis.Slice(heads[i], nonempty[i] ? moments[i] : new double[0][], unit);
                    preparedBytes += slice.estimatedSize();
                    if (preparedBytes > Config.statistic_join_object_max_bytes) {
                        throw new DdlException("JOIN statistics exceed statistic_join_object_max_bytes; reduce mcv_size");
                    }
                    slices.add(slice);
                }
            }
            drop(headValues);
            drop(tail);
            sides.add(slices);
        }
        drop(head);
        return new JoinStatisticsBasis(domain, group, sides, collectPairs(domain, group));
    }

    // Key domains may be separate, but all must have an exact signed 64-bit representation.
    private boolean compactDegrees() {
        return definition.getDomains().size() <= 2
                && definition.getDomains().stream().allMatch(domain -> domain.getTypes().size() == 1
                && Set.of("TINYINT", "SMALLINT", "INT", "BIGINT").contains(domain.getTypes().get(0)
                .toUpperCase(java.util.Locale.ROOT).replaceFirst("\\(.*\\)$", "")));
    }

    private void collectCompactSource(Source source, List<Integer> domains, List<String> keys,
                                      List<String> projections, String dictionary) throws Exception {
        execute("SET streaming_preaggregation_mode='auto',chunk_size=1024");
        String input = " FROM (SELECT " + String.join(",", projections) + " FROM " + source.scan
                + ") s JOIN [BROADCAST] " + dictionary + " c ON s.predicate_tuple=c.p";
        if (domains.size() == 2) {
            String left = "k" + domains.get(0), right = "k" + domains.get(1);
            // 256 x 128 value rectangles, plus distinct NULL IDs. The support has at
            // most 257 x 129 entries, still below the binary-cell limit with counters.
            source.joint = create("cid INT,s0 BIGINT,s1 BIGINT,s VARBINARY(1048576),n BIGINT,nn0 BIGINT,nn1 BIGINT",
                    "cid,s0,s1", "s0,s1", 8);
            if (!keys.isEmpty()) {
                execute("INSERT INTO " + source.joint + " SELECT c.cid,COALESCE(bit_shift_right(" + left
                        + ",8),0),COALESCE(bit_shift_right(" + right + ",7),0),"
                        + "stats_degree_state(bitor(bitor(bit_shift_left(COALESCE(bitand(" + left
                        + ",255),0),7),COALESCE(bitand(" + right + ",127),0)),"
                        + "IF(" + left + " IS NULL,32768,0)+IF(" + right + " IS NULL,65536,0))),"
                        + "COUNT(*),COUNT(" + left + "),COUNT(" + right + ")" + input
                        + " GROUP BY c.cid,COALESCE(bit_shift_right(" + left + ",8),0),COALESCE(bit_shift_right("
                        + right + ",7),0)");
            }
        }
        for (int side = 0; side < domains.size(); side++) {
            int domain = domains.get(side);
            String degree = create("cid INT,shard BIGINT,s VARBINARY(1048576),n BIGINT,nn BIGINT",
                    "cid,shard", "shard", 8);
            if (domains.size() == 2) {
                // Intra-source correlation reads only this small coordinate range, not
                // an entire 32768-key marginal once per pair rectangle.
                String fine = create("cid INT,shard BIGINT,s VARBINARY(1048576),n BIGINT,nn BIGINT",
                        "cid,shard", "shard", 8);
                execute("INSERT INTO " + fine + " SELECT cid,s" + side
                        + ",stats_degree_merge(stats_degree_project(s,s" + side + ","
                        + side + ")),SUM(n),SUM(nn" + side + ") FROM " + source.joint
                        + " GROUP BY cid,s" + side);
                source.intraDegrees.put(domain, fine);
                String shard = "bit_shift_right(shard," + (side == 0 ? 7 : 8) + ")";
                execute("INSERT INTO " + degree + " SELECT cid," + shard
                        + ",stats_degree_merge(s),SUM(n),SUM(nn) FROM " + fine + " GROUP BY cid," + shard);
            } else if (!keys.isEmpty()) {
                String key = "k" + domain;
                execute("INSERT INTO " + degree + " SELECT c.cid,COALESCE(bit_shift_right(" + key + ",15),0),"
                        + "stats_degree_state(" + key + "),COUNT(*),COUNT(" + key + ")" + input
                        + " GROUP BY c.cid,COALESCE(bit_shift_right(" + key + ",15),0)");
            }
            source.degrees.put(domain, degree);
            String totals = create("shard BIGINT,s VARBINARY(1048576)", "shard", "shard", 8);
            execute("INSERT INTO " + totals + " SELECT shard,stats_degree_merge(s) FROM " + degree + " GROUP BY shard");
            source.totals.put(domain, totals);
        }
    }

    private JoinStatisticsBasis collectCompactBasis(int domain, List<Integer> group) throws Exception {
        List<String> arguments = new ArrayList<>();
        StringBuilder joined = new StringBuilder();
        for (int i = 0; i < group.size(); i++) {
            if (i > 0) {
                joined.append(" FULL OUTER JOIN ");
            }
            joined.append(sources.get(group.get(i)).totals.get(domain)).append(" t").append(i);
            if (i > 0) {
                joined.append(" USING(shard)");
            }
            arguments.add("UNHEX(COALESCE(HEX(t" + i + ".s),''))");
        }
        while (arguments.size() < 4) {
            arguments.add("CAST('' AS VARBINARY)");
        }
        // Binary cells are bounded, but copying 4096 of them through a join is still expensive.
        // Use bounded batches for joins of compressed states, not a larger memory allowance.
        execute("SET chunk_size=32");
        boolean empty = group.stream().anyMatch(source -> sources.get(source).tuples.isEmpty());
        String head = "";
        Map<Long, Integer> positions = new LinkedHashMap<>();
        if (!empty) {
            // Obtain the bounded shared dictionary once. No need to retain every source's
            // JSON tree just to discover the same <=16K head keys again.
            var selected = query("SELECT HEX(h),get_json_string(stats_degree_tail(h,h),'$.head') FROM (SELECT "
                    + "stats_degree_head_agg(" + String.join(",", arguments) + ","
                    + JoinStatisticsCorrelation.HEAD_BUDGET + ") h FROM " + joined + ") heads").get(0);
            head = selected.get(0) == null ? "" : selected.get(0);
            if (selected.get(1) != null) {
                for (var entry : JsonParser.parseString(selected.get(1)).getAsJsonArray()) {
                    positions.put(entry.getAsJsonArray().get(0).getAsLong(), positions.size());
                }
            }
        }
        execute("SET chunk_size=1024,streaming_preaggregation_mode='force_preaggregation'");
        List<List<JoinStatisticsBasis.Slice>> sides = new ArrayList<>();
        for (int sourceId : group) {
            Source source = sources.get(sourceId);
            DegreeStatistics[] degrees = new DegreeStatistics[source.tuples.size()];
            Arrays.fill(degrees, new DegreeStatistics(0, 0, 0, 0, new double[10]));
            JoinStatisticsBasis.Slice[] slices = new JoinStatisticsBasis.Slice[degrees.length];
            Arrays.fill(slices, new JoinStatisticsBasis.Slice(
                    CompactDegreeVector.copyOf(new long[positions.size()]), new double[0][], true));
            // Bound returned strings as well as parsed objects. Convert each row immediately;
            // the collector never retains source-sized Gson trees alongside compact slices.
            for (int start = 0; start < degrees.length; start += 8) {
                int end = Math.min(degrees.length, start + 8);
                for (List<String> row : query("SELECT cid,SUM(n),SUM(nn),stats_degree_finish(s,UNHEX('" + head
                        + "')) FROM " + source.degrees.get(domain) + " WHERE cid >= " + start + " AND cid < " + end
                        + " GROUP BY cid")) {
                    int cid = Integer.parseInt(row.get(0));
                    source.tupleRows[cid] = number(row.get(1));
                    if (row.get(3) == null) {
                        if (number(row.get(2)) != 0) {
                            throw new IllegalStateException("Missing degree summary for non-NULL JOIN keys");
                        }
                        degrees[cid] = new DegreeStatistics(source.tupleRows[cid], source.tupleRows[cid],
                                0, 0, new double[10]);
                        continue;
                    }
                    JsonObject value = JsonParser.parseString(row.get(3)).getAsJsonObject();
                    double[] powers = new double[10];
                    for (int i = 0; i < powers.length; i++) {
                        powers[i] = value.getAsJsonArray("moments").get(i).getAsDouble();
                    }
                    degrees[cid] = new DegreeStatistics(source.tupleRows[cid], source.tupleRows[cid] - number(row.get(2)),
                            value.get("ndv").getAsLong(), value.get("max").getAsLong(), powers);
                    if (!empty) {
                        slices[cid] = compactSummary(value, positions, degrees[cid].getMaximumFrequency() <= 1);
                        preparedBytes += slices[cid].estimatedSize();
                        if (preparedBytes > Config.statistic_join_object_max_bytes) {
                            throw new DdlException("JOIN statistics exceed statistic_join_object_max_bytes; reduce mcv_size");
                        }
                    }
                }
            }
            source.moments.put(domain, List.of(degrees));
            sides.add(List.of(slices));
        }
        if (empty) {
            return null;
        }
        execute("SET chunk_size=32,streaming_preaggregation_mode='auto'");
        List<JoinStatisticsBasis.Pair> pairs = collectPairs(domain, group);
        execute("SET chunk_size=1024");
        return new JoinStatisticsBasis(domain, group, sides, pairs);
    }

    static JoinStatisticsBasis.Slice compactSummary(JsonObject value, Map<Long, Integer> positions, boolean unit) {
        long[] counts = new long[positions.size()];
        for (var entry : value.getAsJsonArray("head")) {
            JsonArray pair = entry.getAsJsonArray();
            Integer position = positions.get(pair.get(0).getAsLong());
            if (position == null) {
                throw new IllegalStateException("Degree summary contains a key outside the shared head");
            }
            counts[position] = pair.get(1).getAsLong();
        }
        JsonArray buckets = value.getAsJsonArray("tail");
        boolean nonempty = false;
        double[][] tail = new double[unit ? 1 : JoinStatisticsBasis.momentOrders().length][JoinStatisticsBasis.WIDTH];
        for (int p = 0; p < tail.length; p++) {
            for (int b = 0; b < tail[p].length; b++) {
                tail[p][b] = buckets.get(p).getAsJsonArray().get(b).getAsDouble();
                nonempty |= tail[p][b] != 0;
            }
        }
        return new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(counts), nonempty ? tail : new double[0][], unit);
    }

    private List<JoinStatisticsBasis.Pair> collectPairs(int domain, List<Integer> group) throws Exception {
        List<JoinStatisticsBasis.Pair> pairs = new ArrayList<>();
        for (int left = 0; left < group.size(); left++) {
            for (int right = left + 1; right < group.size(); right++) {
                Source l = sources.get(group.get(left));
                Source r = sources.get(group.get(right));
                int leftSize = l.tuples.size();
                int rightSize = r.tuples.size();
                int cells = Math.multiplyExact(Math.multiplyExact(leftSize, rightSize), 4);
                preparedBytes += 64L + 8L * cells;
                if (preparedBytes > Config.statistic_join_object_max_bytes) {
                    throw new DdlException("Pairwise JOIN statistics exceed statistic_join_object_max_bytes; reduce mcv_size");
                }
                String matrix = create("l INT,r INT,ff DOUBLE,pf DOUBLE,fp DOUBLE,pp DOUBLE", "l,r", "l", 8);
                if (compactDegrees()) {
                    execute("INSERT INTO " + matrix + " SELECT lc,rc,"
                            + "SUM(get_json_double(v,'$[0]')),SUM(get_json_double(v,'$[1]')),"
                            + "SUM(get_json_double(v,'$[2]')),SUM(get_json_double(v,'$[3]')) FROM "
                            + "(SELECT l.cid lc,r.cid rc,stats_degree_pair(l.s,r.s) v FROM " + l.degrees.get(domain)
                            + " l JOIN [SHUFFLE] " + r.degrees.get(domain)
                            + " r ON l.shard=r.shard) p GROUP BY lc,rc");
                } else {
                    executeHeavy("INSERT INTO " + matrix + " SELECT l.cid,r.cid,SUM(CAST(l.d AS DOUBLE)*r.d),"
                        + "SUM(CAST(r.d AS DOUBLE)),SUM(CAST(l.d AS DOUBLE)),COUNT(*) FROM " + l.degrees.get(domain)
                        + " l JOIN [SHUFFLE] " + r.degrees.get(domain) + " r ON l.k" + domain + "=r.k" + domain
                        + " WHERE l.k" + domain + " IS NOT NULL GROUP BY l.cid,r.cid");
                }
                double[] products = new double[cells];
                for (int start = 0; start < leftSize; start += 16) {
                    var rows = query("SELECT * FROM " + matrix + " WHERE l >= " + start
                            + " AND l < " + Math.min(leftSize, start + 16));
                    for (List<String> row : rows) {
                        int offset = (Integer.parseInt(row.get(0)) * rightSize + Integer.parseInt(row.get(1))) * 4;
                        for (int role = 0; role < 4; role++) {
                            products[offset + role] = Double.parseDouble(row.get(role + 2));
                        }
                    }
                }
                pairs.add(new JoinStatisticsBasis.Pair(left, right, leftSize, rightSize, products));
                drop(matrix);
            }
        }
        return pairs;
    }

    private String create(String columns, String keys, String hash, int buckets) throws Exception {
        String table = DATABASE + "." + ident("_join_collect_" + ticket.getPrevious().getId() + "_"
                + ticket.getGeneration() + "_" + scratchTables.size());
        scratchTables.add(table);
        execute("CREATE TABLE " + table + " (" + columns + ") DUPLICATE KEY(" + keys + ") DISTRIBUTED BY HASH("
                + hash + ") BUCKETS " + buckets + " PROPERTIES('replication_num'='1','compression'='ZSTD',"
                + "'enable_statistic_collect_on_first_load'='false')");
        return table;
    }

    private String keyExpression(Source source, JoinStatisticsDefinition.KeyDomain domain, int index) {
        List<String> values = new ArrayList<>();
        List<String> nulls = new ArrayList<>();
        for (int component = 0; component < domain.getTypes().size(); component++) {
            String column = StatisticUtils.quoting(source.table, domain.getColumns().get(index).get(component));
            String type = compactDegrees() ? "BIGINT" : domain.getTypes().get(component);
            String value = "CAST(" + column + " AS " + type + ")";
            values.add(domain.getTypes().size() == 1 ? value : "CAST(" + value + " AS VARCHAR)");
            nulls.add(column + " IS NULL");
        }
        if (values.size() == 1) {
            return values.get(0);
        }
        // Typed casts give both sides the same equality representation. The MCV tuple codec escapes boundaries;
        // any NULL component makes the entire ordinary equality key NULL (not the string 'null').
        return "IF(" + String.join(" OR ", nulls) + ",NULL,stats_tuple_key(" + String.join(",", values) + "))";
    }

    private String storageType(int domain) {
        if (definition.getDomains().get(domain).getTypes().size() > 1) {
            return "VARCHAR(1048576)";
        }
        String type = definition.getDomains().get(domain).getTypes().get(0);
        return type.toUpperCase(java.util.Locale.ROOT).startsWith("VARCHAR") ? "VARCHAR(1048576)" : type;
    }

    private void drop(String table) throws Exception {
        execute("DROP TABLE IF EXISTS " + table + " FORCE");
    }

    private void check() {
        checkLeadership.run();
        if (ticket.isCancelled() || Thread.currentThread().isInterrupted()) {
            throw new CancellationException("JOIN statistics collection was cancelled");
        }
        long remaining = Config.statistic_collect_query_timeout - TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started);
        if (remaining <= 0) {
            throw new CancellationException("JOIN statistics collection timed out");
        }
        context.getSessionVariable().setQueryTimeoutS((int) Math.min(Integer.MAX_VALUE, remaining));
        context.getSessionVariable().setInsertTimeoutS((int) Math.min(Integer.MAX_VALUE, remaining));
    }

    private void execute(String sql) throws Exception {
        check();
        configureStage(false);
        JoinStatisticsStorage.execute(context, sql);
    }

    private List<List<String>> query(String sql) throws Exception {
        check();
        configureStage(false);
        return new StatisticExecutor().executeStatisticJsonDQL(context, sql);
    }

    private void executeHeavy(String sql) throws Exception {
        check();
        configureStage(true);
        JoinStatisticsStorage.execute(context, sql);
    }

    private List<List<String>> queryHeavy(String sql) throws Exception {
        check();
        configureStage(true);
        return new StatisticExecutor().executeStatisticJsonDQL(context, sql);
    }

    private void configureStage(boolean next) throws Exception {
        if (next != boundedJoin) {
            // Large intermediate joins must spill before several drivers allocate their first hash tables.
            // Small summary aggregates must not force-spill every input row.
            JoinStatisticsStorage.execute(context, next
                    ? "SET spill_mode='force',spill_mem_table_size=16777216,spill_revocable_max_bytes=134217728"
                    : "SET spill_mode='auto',spill_mem_table_size=67108864,spill_revocable_max_bytes=0");
            boundedJoin = next;
        }
    }

    static String power(String expression, int exponent) {
        return "(" + String.join("*", java.util.Collections.nCopies(exponent, expression)) + ")";
    }

    private static long number(String value) {
        return value == null ? 0 : Long.parseLong(value);
    }

    private static String ident(String name) {
        return SqlUtils.getIdentSql(name);
    }

    private static String literal(String value) {
        return "'" + SqlUtils.escapeSqlString(value) + "'";
    }

    @Override
    public void close() {
        // Cleanup has its own context: the collection context may already be cancelled or timed out.
        ConnectContext cleanup = StatisticUtils.buildConnectContext();
        long started = System.nanoTime();
        try (ConnectContext.ScopeGuard ignored = cleanup.bindScope()) {
            for (String table : scratchTables) {
                try {
                    checkLeadership.run();
                    long remaining = TimeUnit.SECONDS.toNanos(60) - (System.nanoTime() - started);
                    if (remaining <= 0) {
                        break;
                    }
                    int seconds = (int) Math.max(1, TimeUnit.NANOSECONDS.toSeconds(remaining));
                    cleanup.getSessionVariable().setQueryTimeoutS(seconds);
                    cleanup.getSessionVariable().setInsertTimeoutS(seconds);
                    JoinStatisticsStorage.execute(cleanup, "DROP TABLE IF EXISTS " + table + " FORCE");
                } catch (Exception e) {
                    LOG.warn("Cannot remove JOIN statistics scratch table {}", table, e);
                    // The leader's orphan sweep retries later; an outage must not block the queue per scratch table.
                    break;
                }
            }
        }
    }
}
