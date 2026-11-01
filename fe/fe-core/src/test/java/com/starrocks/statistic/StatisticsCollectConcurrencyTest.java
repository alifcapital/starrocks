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
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.statistic.sample.ColumnSampleManager;
import com.starrocks.statistic.sample.ColumnStats;
import com.starrocks.statistic.sample.SampleInfo;
import com.starrocks.statistic.sample.TabletSampleManager;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import com.starrocks.type.Type;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

// statistic_collect_parallel is the number of collection queries of one analyze task that run at the same
// time, and statistic_collect_columns_per_scan is the number of columns one query covers. The pipeline dop
// of a collection query is the one of the session. These tests check that, and that running queries at the
// same time keeps the coverage, the failure tolerance, KILL ANALYZE and the single statistics writer intact.
public class StatisticsCollectConcurrencyTest extends PlanTestNoneDBBase {
    private static final String CATALOG = "hive0";
    private static final String HIVE_DB = "partitioned_db";
    private static final String HIVE_TABLE = "t1";
    private static final List<String> OLAP_COLUMNS = List.of("c1", "c2", "c3", "c4", "c5", "c6");

    @TempDir
    public static File temp;

    private static Database hiveDb;
    private static Table hiveTable;
    private static Database olapDb;
    private static OlapTable olapTable;
    private static long olapPartitionId;

    private int savedParallel;
    private int savedColumnsPerScan;
    private int savedPipelineDop;
    private long savedCollectBuffer;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        File dir = new File(temp, "junit");
        if (!dir.mkdirs()) {
            throw new IOException("Couldn't create folders " + dir);
        }
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, dir.toURI().toString());
        hiveDb = connectContext.getGlobalStateMgr().getMetadataMgr().getDb(connectContext, CATALOG, HIVE_DB);
        hiveTable = connectContext.getGlobalStateMgr().getMetadataMgr()
                .getTable(connectContext, CATALOG, HIVE_DB, HIVE_TABLE);

        starRocksAssert.withDatabase("stats_conc").useDatabase("stats_conc");
        starRocksAssert.withTable("CREATE TABLE t_conc (k1 INT, c1 INT, c2 INT, c3 INT, c4 INT, c5 INT, c6 INT) " +
                "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        olapDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("stats_conc");
        olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("stats_conc", "t_conc");
        olapPartitionId = olapTable.getPartition("t_conc").getId();
        olapTable.getPartition("t_conc").getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(10000);
    }

    @BeforeEach
    public void saveSession() {
        savedParallel = connectContext.getSessionVariable().getStatisticCollectParallelism();
        savedColumnsPerScan = connectContext.getSessionVariable().getStatisticCollectColumnsPerScan();
        savedPipelineDop = connectContext.getSessionVariable().getPipelineDop();
        savedCollectBuffer = Config.statistic_full_collect_buffer;
        connectContext.getState().setOk();
    }

    @AfterEach
    public void restoreSession() {
        connectContext.getSessionVariable().setStatisticCollectParallelism(savedParallel);
        connectContext.getSessionVariable().setStatisticCollectColumnsPerScan(savedColumnsPerScan);
        connectContext.getSessionVariable().setPipelineDop(savedPipelineDop);
        Config.statistic_full_collect_buffer = savedCollectBuffer;
    }

    private static void setSession(int parallel, int columnsPerScan) {
        connectContext.getSessionVariable().setStatisticCollectParallelism(parallel);
        connectContext.getSessionVariable().setStatisticCollectColumnsPerScan(columnsPerScan);
    }

    // Counts how many queries are inside at the same time. The first `parties` queries wait for each other,
    // so a runner that allows `parties` concurrent queries reaches exactly that many; a runner that runs them
    // one by one never gets past one.
    private static final class Overlap {
        private final CountDownLatch rendezvous;
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger maxInFlight = new AtomicInteger();
        private final AtomicInteger entered = new AtomicInteger();

        Overlap(int parties) {
            rendezvous = new CountDownLatch(parties);
        }

        void enter() throws InterruptedException {
            entered.incrementAndGet();
            maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            if (rendezvous.getCount() > 0) {
                rendezvous.countDown();
                rendezvous.await(5, TimeUnit.SECONDS);
            }
        }

        void exit() {
            inFlight.decrementAndGet();
        }

        int max() {
            return maxInFlight.get();
        }
    }

    private static List<String> partitions(int count) {
        List<String> names = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            names.add("par_col=" + i);
        }
        return names;
    }

    private static long hash(String partitionName) {
        return Hashing.murmur3_128().hashUnencodedChars(partitionName).asLong();
    }

    private static AnalyzeStatus newAnalyzeStatus(long id) {
        return new NativeAnalyzeStatus(id, 1L, 1L, Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), LocalDateTime.now());
    }

    // An external job whose queries are stand-ins: each records the context it got and the partition it read,
    // and fails when its partition is in `failingPartitions`.
    private static class ExternalJob extends ExternalFullStatisticsCollectJob {
        final Overlap overlap;
        final Set<String> failingPartitions;
        final Set<ConnectContext> queryContexts = ConcurrentHashMap.newKeySet();
        final Set<Integer> pipelineDops = ConcurrentHashMap.newKeySet();
        final AtomicInteger executed = new AtomicInteger();

        ExternalJob(List<String> partitionNames, List<String> columnNames, List<Type> columnTypes,
                    Overlap overlap, Set<String> failingPartitions) {
            super(CATALOG, hiveDb, hiveTable, partitionNames, columnNames, columnTypes,
                    StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap());
            this.overlap = overlap;
            this.failingPartitions = failingPartitions;
        }

        @Override
        public void collectStatisticSync(String sql, ConnectContext context, AnalyzeStatus analyzeStatus)
                throws Exception {
            executed.incrementAndGet();
            queryContexts.add(context);
            pipelineDops.add(context.getSessionVariable().getPipelineDop());
            overlap.enter();
            try {
                for (String partition : failingPartitions) {
                    if (sql.contains(" = '" + partition.substring("par_col=".length()) + "'")) {
                        throw new RuntimeException("Statistics query fail | Error Message [bad partition]");
                    }
                }
            } finally {
                overlap.exit();
            }
        }

        @Override
        protected void flushInsertStatisticsData(ConnectContext context, boolean force) {
        }

        @Override
        protected void publishCollectedStatistics(ConnectContext context) {
        }

        @Override
        protected long retryBackoffMillis(int attempt) {
            return 0;
        }
    }

    private static ExternalJob externalJob(int partitionCount, Overlap overlap, Set<String> failing) {
        return new ExternalJob(partitions(partitionCount), Lists.newArrayList("c1", "c2"),
                Lists.newArrayList(IntegerType.INT, StringType.STRING), overlap, failing);
    }

    @Test
    public void testExternalQueriesRunConcurrentlyUpToParallel() throws Exception {
        setSession(3, 2);
        Overlap overlap = new Overlap(3);
        ExternalJob job = externalJob(12, overlap, Set.of());

        job.runCollectPhases(connectContext, newAnalyzeStatus(1L), 1L);

        Assertions.assertEquals(3, overlap.max());
        Assertions.assertEquals(12, job.executed.get());
        // Concurrent queries never share a context, and none of them uses the task's own context.
        Assertions.assertEquals(3, job.queryContexts.size());
        Assertions.assertFalse(job.queryContexts.contains(connectContext));
        for (String column : List.of("c1", "c2")) {
            Assertions.assertEquals(12, job.getCollectedPartitionsHashByColumn().get(column).size());
        }
    }

    @Test
    public void testExternalParallelOneRunsOnTaskContext() throws Exception {
        setSession(1, 2);
        Overlap overlap = new Overlap(1);
        ExternalJob job = externalJob(5, overlap, Set.of());

        job.runCollectPhases(connectContext, newAnalyzeStatus(1L), 1L);

        Assertions.assertEquals(1, overlap.max());
        Assertions.assertEquals(Set.of(connectContext), job.queryContexts);
    }

    @Test
    public void testColumnsPerScanGroupsColumnsAndParallelDoesNot() throws Exception {
        List<String> columns = Lists.newArrayList("c1", "c2", "c3", "par_col");
        List<Type> types = Lists.newArrayList(IntegerType.INT, StringType.STRING, StringType.STRING,
                StringType.STRING);

        // Four columns at two columns per scan: two queries per partition, whatever the parallel is.
        setSession(4, 2);
        ExternalJob job = new ExternalJob(partitions(10), columns, types, new Overlap(1), Set.of());
        job.runCollectPhases(connectContext, newAnalyzeStatus(1L), 1L);
        Assertions.assertEquals(20, job.executed.get());

        // One column per scan: a query per column and partition, even with a high parallel.
        setSession(4, 1);
        job = new ExternalJob(partitions(10), columns, types, new Overlap(1), Set.of());
        job.runCollectPhases(connectContext, newAnalyzeStatus(1L), 1L);
        Assertions.assertEquals(40, job.executed.get());

        // Internal full collection: subqueries of a partition and a column, joined by columns per scan.
        FullStatisticsCollectJob fullJob = new FullStatisticsCollectJob(olapDb, olapTable, List.of(olapPartitionId),
                OLAP_COLUMNS, StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap());
        List<String> fullSqls = Collections.synchronizedList(Lists.newArrayList());
        new MockUp<FullStatisticsCollectJob>() {
            @Mock
            public void collectStatisticSync(String sql, ConnectContext context, AnalyzeStatus status) {
                fullSqls.add(sql);
            }
        };
        setSession(1, 4);
        fullJob.collect(connectContext, newAnalyzeStatus(1L));
        Assertions.assertEquals(2, fullSqls.size());
        Assertions.assertEquals(4, fullSqls.get(0).split("UNION ALL").length);

        // Sample collection groups at least two columns per query.
        SampleInfo sampleInfo = TabletSampleManager.init(Maps.newHashMap(), olapTable).generateSampleInfo();
        List<Type> olapTypes = OLAP_COLUMNS.stream().map(c -> olapTable.getColumn(c).getType())
                .collect(Collectors.toList());
        ColumnSampleManager sampleManager = ColumnSampleManager.init(OLAP_COLUMNS, olapTypes, olapTable, sampleInfo);
        connectContext.setThreadLocalInfo();
        setSession(8, 3);
        List<List<ColumnStats>> batches = sampleManager.splitPrimitiveTypeStats();
        Assertions.assertEquals(List.of(3, 3), batches.stream().map(List::size).collect(Collectors.toList()));
        setSession(8, 1);
        batches = sampleManager.splitPrimitiveTypeStats();
        Assertions.assertEquals(List.of(2, 2, 2), batches.stream().map(List::size).collect(Collectors.toList()));
    }

    @Test
    public void testHyperColumnsPerScan() throws Exception {
        List<String> sqls = Collections.synchronizedList(Lists.newArrayList());
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<TStatisticData> executeStatisticDQL(ConnectContext context, String sql) {
                sqls.add(sql);
                return Lists.newArrayList();
            }
        };
        setSession(8, 2);
        hyperJob(null).collect(connectContext, newAnalyzeStatus(1L));
        Assertions.assertEquals(3, sqls.size());
        for (String sql : sqls) {
            Assertions.assertEquals(2, sql.split("UNION ALL").length, sql);
        }
    }

    @Test
    public void testPipelineDopIsTheSessionOne() throws Exception {
        // The session dop is 5 while the parallel is 4 and a query covers 2 columns. The collection must not
        // derive another dop from those.
        setSession(4, 2);
        connectContext.getSessionVariable().setPipelineDop(5);

        ExternalJob externalJob = externalJob(8, new Overlap(1), Set.of());
        externalJob.runCollectPhases(connectContext, newAnalyzeStatus(1L), 1L);
        Assertions.assertEquals(Set.of(5), externalJob.pipelineDops);

        Set<Integer> dops = ConcurrentHashMap.newKeySet();
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<TStatisticData> executeStatisticDQL(ConnectContext context, String sql) {
                dops.add(context.getSessionVariable().getPipelineDop());
                return Lists.newArrayList();
            }
        };
        new FullStatisticsCollectJob(olapDb, olapTable, List.of(olapPartitionId), OLAP_COLUMNS,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap())
                .collect(connectContext, newAnalyzeStatus(1L));
        Assertions.assertEquals(Set.of(5), dops);

        dops.clear();
        hyperJob(null).collect(connectContext, newAnalyzeStatus(1L));
        Assertions.assertEquals(Set.of(5), dops);
    }

    @Test
    public void testKillStopsEveryRunningQuery() throws Exception {
        long analyzeId = 987654321L;
        setSession(3, 2);
        Set<ConnectContext> killed = ConcurrentHashMap.newKeySet();
        new MockUp<ConnectContext>() {
            @Mock
            public void kill(Invocation invocation, boolean killConnection, String cancelledMessage) {
                killed.add(invocation.getInvokedInstance());
            }
        };
        CountDownLatch allRunning = new CountDownLatch(3);
        AtomicBoolean testDone = new AtomicBoolean();
        AtomicInteger started = new AtomicInteger();
        Set<ConnectContext> running = ConcurrentHashMap.newKeySet();
        ExternalJob job = new ExternalJob(partitions(200), Lists.newArrayList("c1", "c2"),
                Lists.newArrayList(IntegerType.INT, StringType.STRING), new Overlap(1), Set.of()) {
            @Override
            public void collectStatisticSync(String sql, ConnectContext context, AnalyzeStatus analyzeStatus)
                    throws Exception {
                started.incrementAndGet();
                running.add(context);
                allRunning.countDown();
                // Runs until its own context is killed, like a query that a cancel stops on the backends.
                long deadline = System.currentTimeMillis() + 20_000;
                while (!killed.contains(context) && !testDone.get() && System.currentTimeMillis() < deadline) {
                    Thread.sleep(5);
                }
                throw new DdlException("kill analyze");
            }
        };

        AnalyzeMgr analyzeMgr = GlobalStateMgr.getCurrentState().getAnalyzeMgr();
        ConnectContext taskContext = connectContext;
        analyzeMgr.registerConnection(analyzeId, taskContext);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> run = executor.submit(() -> {
                try (var guard = taskContext.bindScope()) {
                    job.runCollectPhases(taskContext, newAnalyzeStatus(analyzeId), analyzeId);
                }
                return null;
            });
            Assertions.assertTrue(allRunning.await(10, TimeUnit.SECONDS), "three queries never ran together");

            analyzeMgr.killConnection(analyzeId);

            Exception e = Assertions.assertThrows(Exception.class, () -> run.get(30, TimeUnit.SECONDS));
            Assertions.assertTrue(e.getMessage().contains("USER_CANCEL"), e.getMessage());
            Assertions.assertEquals(3, running.size());
            Assertions.assertTrue(killed.containsAll(running), "every running query is killed");
            // No query starts once the task is cancelled.
            Assertions.assertEquals(3, started.get());
        } finally {
            testDone.set(true);
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            analyzeMgr.unregisterConnection(analyzeId, false);
        }
    }

    @Test
    public void testFailureToleranceAndCoverageWithConcurrentQueries() throws Exception {
        setSession(4, 2);
        Overlap overlap = new Overlap(4);
        ExternalJob job = externalJob(200, overlap, Set.of("par_col=5", "par_col=50"));
        AnalyzeStatus analyzeStatus = newAnalyzeStatus(1L);

        job.runCollectPhases(connectContext, analyzeStatus, 1L);

        Assertions.assertEquals(4, overlap.max());
        Assertions.assertEquals(200, job.executed.get());
        Assertions.assertTrue(job.hasToleratedFailures());
        Assertions.assertTrue(analyzeStatus.getReason().contains("partially failed but tolerated 2/200"),
                analyzeStatus.getReason());
        for (String column : List.of("c1", "c2")) {
            Set<Long> collected = job.getCollectedPartitionsHashByColumn().get(column);
            Assertions.assertEquals(198, collected.size());
            Assertions.assertFalse(collected.contains(hash("par_col=5")));
            Assertions.assertFalse(collected.contains(hash("par_col=50")));
        }
    }

    @Test
    public void testTooManyFailuresWithConcurrentQueriesFailTheJob() {
        setSession(4, 2);
        Set<String> failing = new HashSet<>();
        for (int i = 0; i < 40; i++) {
            failing.add("par_col=" + (i * 5));
        }
        Overlap overlap = new Overlap(4);
        ExternalJob job = externalJob(200, overlap, failing);

        Exception e = Assertions.assertThrows(Exception.class,
                () -> job.runCollectPhases(connectContext, newAnalyzeStatus(1L), 1L));
        Assertions.assertTrue(e.getMessage().contains("too many failed tasks"), e.getMessage());
        Assertions.assertEquals(4, overlap.max());
        Assertions.assertTrue(job.executed.get() < 200);

        // Below 100 queries nothing is tolerated, and the query's own error is the job's error.
        Overlap smallOverlap = new Overlap(4);
        ExternalJob smallJob = externalJob(20, smallOverlap, Set.of("par_col=7"));
        e = Assertions.assertThrows(Exception.class,
                () -> smallJob.runCollectPhases(connectContext, newAnalyzeStatus(1L), 1L));
        Assertions.assertTrue(e.getMessage().contains("bad partition"), e.getMessage());
        Assertions.assertEquals(4, smallOverlap.max());
    }

    // Records the INSERTs of statistics rows: how many run at the same time, and how many rows they write.
    private static final class InsertRecorder {
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxInFlight = new AtomicInteger();
        final AtomicInteger rows = new AtomicInteger();
        final AtomicInteger inserts = new AtomicInteger();

        void install() {
            new MockUp<StmtExecutor>() {
                @Mock
                public void execute(Invocation invocation) throws Exception {
                    maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                    try {
                        StatementBase stmt = ((StmtExecutor) invocation.getInvokedInstance()).getParsedStmt();
                        ValuesRelation values =
                                (ValuesRelation) ((InsertStmt) stmt).getQueryStatement().getQueryRelation();
                        rows.addAndGet(values.getRows().size());
                        inserts.incrementAndGet();
                        // Long enough for another query to finish and try to write meanwhile.
                        Thread.sleep(20);
                        ConnectContext.get().getState().setOk();
                    } finally {
                        inFlight.decrementAndGet();
                    }
                }
            };
        }
    }

    private static TStatisticData olapRow(String sql) {
        TStatisticData data = new TStatisticData();
        data.setPartitionId(olapPartitionId);
        data.setColumnName(OLAP_COLUMNS.stream().filter(c -> sql.contains("'" + c + "'")).findFirst().orElse("?"));
        data.setRowCount(10);
        data.setDataSize(40);
        data.setNullCount(0);
        data.setHll("AA".getBytes(StandardCharsets.UTF_8));
        data.setMax("9");
        data.setMin("0");
        return data;
    }

    @Test
    public void testExternalStatisticsWritesNeverOverlap() throws Exception {
        setSession(4, 1);
        Config.statistic_full_collect_buffer = 1;
        Overlap overlap = new Overlap(4);
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<TStatisticData> executeStatisticDQL(ConnectContext context, String sql) throws Exception {
                overlap.enter();
                try {
                    TStatisticData data = new TStatisticData();
                    data.setPartitionName("par_col=0");
                    data.setColumnName(sql.contains("'c1'") ? "c1" : "c2");
                    data.setRowCount(10);
                    data.setDataSize(80);
                    data.setNullCount(0);
                    data.setHll("AA".getBytes(StandardCharsets.UTF_8));
                    data.setMax("9");
                    data.setMin("0");
                    return Lists.newArrayList(data);
                } finally {
                    overlap.exit();
                }
            }

            @Mock
            public void requireVisibleStatisticsInsert(InsertStmt statement) {
            }
        };
        InsertRecorder recorder = new InsertRecorder();
        recorder.install();

        ExternalFullStatisticsCollectJob job = new ExternalFullStatisticsCollectJob(CATALOG, hiveDb, hiveTable,
                partitions(20), Lists.newArrayList("c1", "c2"), Lists.newArrayList(IntegerType.INT, StringType.STRING),
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap()) {
            @Override
            protected void publishCollectedStatistics(ConnectContext context) {
            }
        };
        job.runCollectPhases(connectContext, newAnalyzeStatus(1L), 1L);

        Assertions.assertEquals(4, overlap.max());
        Assertions.assertEquals(1, recorder.maxInFlight.get());
        // Every collected row is written exactly once.
        Assertions.assertEquals(40, recorder.rows.get());
        Assertions.assertEquals(20, job.getCollectedPartitionsHashByColumn().get("c1").size());
    }

    @Test
    public void testFullStatisticsWritesNeverOverlap() throws Exception {
        setSession(3, 1);
        Config.statistic_full_collect_buffer = 1;
        Overlap overlap = new Overlap(3);
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<TStatisticData> executeStatisticDQL(ConnectContext context, String sql) throws Exception {
                overlap.enter();
                try {
                    return Lists.newArrayList(olapRow(sql));
                } finally {
                    overlap.exit();
                }
            }
        };
        InsertRecorder recorder = new InsertRecorder();
        recorder.install();

        new FullStatisticsCollectJob(olapDb, olapTable, List.of(olapPartitionId), OLAP_COLUMNS,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap())
                .collect(connectContext, newAnalyzeStatus(1L));

        Assertions.assertEquals(3, overlap.max());
        Assertions.assertEquals(1, recorder.maxInFlight.get());
        Assertions.assertEquals(6, recorder.rows.get());
    }

    private static HyperStatisticsCollectJob hyperJob(List<List<String>> insertedValues) {
        List<Type> types = OLAP_COLUMNS.stream().map(c -> olapTable.getColumn(c).getType())
                .collect(Collectors.toList());
        return new HyperStatisticsCollectJob(olapDb, olapTable, List.of(olapPartitionId), OLAP_COLUMNS, types,
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), false) {
            @Override
            protected StatementBase createInsertStmt() {
                if (insertedValues != null) {
                    insertedValues.add(Lists.newArrayList(sqlBuffer));
                }
                return super.createInsertStmt();
            }
        };
    }

    @Test
    public void testHyperQueriesRunConcurrentlyAndKeepEveryRow() throws Exception {
        setSession(3, 1);
        Overlap overlap = new Overlap(3);
        Set<ConnectContext> contexts = ConcurrentHashMap.newKeySet();
        new MockUp<StatisticExecutor>() {
            @Mock
            public List<TStatisticData> executeStatisticDQL(ConnectContext context, String sql) throws Exception {
                contexts.add(context);
                overlap.enter();
                try {
                    return Lists.newArrayList(olapRow(sql));
                } finally {
                    overlap.exit();
                }
            }
        };
        InsertRecorder recorder = new InsertRecorder();
        recorder.install();
        List<List<String>> inserted = Collections.synchronizedList(Lists.newArrayList());

        hyperJob(inserted).collect(connectContext, newAnalyzeStatus(1L));

        Assertions.assertEquals(3, overlap.max());
        Assertions.assertEquals(3, contexts.size());
        Assertions.assertFalse(contexts.contains(connectContext));
        Assertions.assertEquals(1, recorder.maxInFlight.get());
        // One row per column, in the order of the columns, as if the queries had run one by one.
        List<String> values = inserted.stream().flatMap(List::stream).collect(Collectors.toList());
        Assertions.assertEquals(OLAP_COLUMNS.size(), values.size());
        for (int i = 0; i < OLAP_COLUMNS.size(); i++) {
            Assertions.assertTrue(values.get(i).contains("'" + OLAP_COLUMNS.get(i) + "'"), values.get(i));
        }
    }

    @Test
    public void testCollectQueryContextCopiesTheTaskSession() {
        ConnectContext task = StatisticUtils.buildConnectContext();
        task.getSessionVariable().setExternalStatsScanBytesCap(123);
        task.getSessionVariable().setCboCTERuseRatio(0);
        task.getSessionVariable().setEnableSpill(true);
        task.getSessionVariable().setQueryTimeoutS(77);
        task.setSessionId(java.util.UUID.randomUUID());

        ConnectContext query = StatisticUtils.buildCollectQueryContext(task);

        Assertions.assertNotSame(task.getSessionVariable(), query.getSessionVariable());
        Assertions.assertEquals(123, query.getSessionVariable().getExternalStatsScanBytesCap());
        Assertions.assertEquals(0, query.getSessionVariable().getCboCTERuseRatio());
        Assertions.assertTrue(query.getSessionVariable().isEnableSpill());
        Assertions.assertEquals(77, query.getSessionVariable().getQueryTimeoutS());
        Assertions.assertEquals(task.getCurrentWarehouseName(), query.getCurrentWarehouseName());
        Assertions.assertEquals(task.getSessionId(), query.getSessionId());
        Assertions.assertEquals(task.getCurrentUserIdentity(), query.getCurrentUserIdentity());
        Assertions.assertEquals(task.getDatabase(), query.getDatabase());
        Assertions.assertTrue(query.isStatisticsJob());
        Assertions.assertTrue(query.isOnlyReadIcebergCache());
        Assertions.assertNotEquals(task.getQueryId(), query.getQueryId());
        Assertions.assertNotSame(task.getState(), query.getState());
        // A query context does not see the changes the task makes to its own session afterwards.
        task.getSessionVariable().setQueryTimeoutS(5);
        Assertions.assertEquals(77, query.getSessionVariable().getQueryTimeoutS());
    }
}
