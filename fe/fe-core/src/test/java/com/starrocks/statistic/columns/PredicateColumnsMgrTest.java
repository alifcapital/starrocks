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

package com.starrocks.statistic.columns;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.ExternalAnalyzeJob;
import com.starrocks.statistic.StatisticsCollectJobFactory;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class PredicateColumnsMgrTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        StatisticsMetaManager statistic = new StatisticsMetaManager();
        statistic.createStatisticsTablesForTest();
        TableKeeper keeper = ExternalPredicateColumnsStorage.createKeeper();
        keeper.run();
        FeConstants.runningUnitTest = true;
    }

    @BeforeEach
    public void before() {
        PredicateColumnsMgr.getInstance().reset();
    }

    private IcebergTable mockExternalTable(String uuid, String catalog, String db, String table) {
        IcebergTable t = Mockito.mock(IcebergTable.class);
        Mockito.when(t.getUUID()).thenReturn(uuid);
        Mockito.when(t.getCatalogName()).thenReturn(catalog);
        Mockito.when(t.getCatalogDBName()).thenReturn(db);
        Mockito.when(t.getCatalogTableName()).thenReturn(table);
        Mockito.when(t.getName()).thenReturn(table);
        Mockito.when(t.isNativeTableOrMaterializedView()).thenReturn(false);
        Mockito.when(t.isTemporaryTable()).thenReturn(false);
        return t;
    }

    @Test
    public void testExternalColumnRecordedAndQueryable() {
        IcebergTable table = mockExternalTable("iceberg_catalog.db1.t1.uuid-1", "iceberg_catalog", "db1", "t1");
        Column column = new Column("c1", IntegerType.INT);
        PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();

        mgr.recordColumnUsageForTest(table, column, ColumnUsage.UseCase.PREDICATE);

        List<String> result = mgr.queryExternalPredicateColumns(table);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("c1", result.get(0));
        Assertions.assertEquals("iceberg_catalog", mgr.queryExternalPredicateColumnGroups(table).get(0).catalogName());
    }

    @Test
    public void testRealPlansKeepPredicateSetsAndBasicColumnUnion() throws Exception {
        PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();
        getFragmentPlan("select c_custkey from hive0.tpch.customer where c_nationkey = 1 and c_acctbal > 100");
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(connectContext, "hive0", "tpch", "customer");
        Assertions.assertEquals(List.of("c_acctbal", "c_nationkey"), mgr.queryExternalPredicateColumns(table));
        Assertions.assertEquals(Set.of(List.of("c_acctbal", "c_nationkey")),
                mgr.queryExternalPredicateColumnGroups(table).stream()
                        .filter(group -> group.useCase() == ColumnUsage.UseCase.PREDICATE)
                        .map(ExternalColumnGroupUsage::columns).collect(Collectors.toSet()));
        getFragmentPlan("select c_custkey from hive0.tpch.customer where c_mktsegment = 'AUTOMOBILE'");
        Assertions.assertEquals(Set.of(List.of("c_acctbal", "c_nationkey"), List.of("c_mktsegment")),
                mgr.queryExternalPredicateColumnGroups(table).stream()
                        .filter(group -> group.useCase() == ColumnUsage.UseCase.PREDICATE)
                        .map(ExternalColumnGroupUsage::columns).collect(Collectors.toSet()));
        Assertions.assertEquals(List.of("c_acctbal", "c_mktsegment", "c_nationkey"),
                mgr.queryExternalPredicateColumns(table));
        int threshold = Config.statistic_auto_collect_predicate_columns_threshold;
        boolean staggered = Config.enable_statistic_auto_collect_staggered_schedule;
        try {
            Config.enable_statistic_auto_collect_staggered_schedule = false;
            Config.statistic_auto_collect_predicate_columns_threshold = 1;
            var automaticJob = new ExternalAnalyzeJob("hive0", "tpch", "customer", null, null,
                    StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE, new HashMap<>(),
                    StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
            var jobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(automaticJob);
            Assertions.assertEquals(1, jobs.size());
            Assertions.assertEquals(Set.of("c_acctbal", "c_mktsegment", "c_nationkey"),
                    Set.copyOf(jobs.get(0).getColumnNames()));
            var explicitJob = new ExternalAnalyzeJob("hive0", "tpch", "customer", List.of("c_custkey"), null,
                    StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.SCHEDULE, new HashMap<>(),
                    StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
            var explicitJobs = StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(explicitJob);
            Assertions.assertEquals(List.of("c_custkey"), explicitJobs.get(0).getColumnNames());
        } finally {
            Config.statistic_auto_collect_predicate_columns_threshold = threshold;
            Config.enable_statistic_auto_collect_staggered_schedule = staggered;
        }
    }

    @Test
    public void testNativeRepeatedObservationRetainsRecordAndCreationTime() {
        PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();
        Table table = starRocksAssert.getTable(connectContext.getDatabase(), "t0");
        Column column = table.getColumn("v1");
        TableName name = new TableName(connectContext.getDatabase(), "t0");
        mgr.recordColumnUsageForTest(table, column, ColumnUsage.UseCase.NORMAL);
        ColumnUsage original = mgr.query(name).get(0);
        LocalDateTime created = original.getCreated();
        TableName originalName = original.getTableName();
        Assertions.assertEquals(created, original.getLastUsed());
        original.setLastUsed(LocalDateTime.MIN);

        mgr.recordColumnUsageForTest(table, column, ColumnUsage.UseCase.JOIN);
        mgr.recordColumnUsageForTest(table, column, ColumnUsage.UseCase.PREDICATE);
        List<ColumnUsage> result = mgr.query(name);
        Assertions.assertEquals(1, result.size());
        Assertions.assertSame(original, result.get(0));
        Assertions.assertSame(originalName, original.getTableName());
        Assertions.assertEquals(created, original.getCreated());
        Assertions.assertTrue(original.getLastUsed().isAfter(LocalDateTime.MIN));
        Assertions.assertEquals(Set.of(ColumnUsage.UseCase.NORMAL, ColumnUsage.UseCase.JOIN,
                ColumnUsage.UseCase.PREDICATE), original.getUseCases());
    }

    @Test
    public void testConcurrentNativeObservationsKeepEveryUseCase() throws Exception {
        PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();
        Table table = starRocksAssert.getTable(connectContext.getDatabase(), "t0");
        Column column = table.getColumn("v1");
        var executor = java.util.concurrent.Executors.newFixedThreadPool(ColumnUsage.UseCase.values().length);
        var start = new java.util.concurrent.CountDownLatch(1);
        try {
            var tasks = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (ColumnUsage.UseCase useCase : ColumnUsage.UseCase.values()) {
                tasks.add(executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < 100; i++) {
                        mgr.recordColumnUsageForTest(table, column, useCase);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (var task : tasks) {
                task.get(10, java.util.concurrent.TimeUnit.SECONDS);
            }
            List<ColumnUsage> result = mgr.query(new TableName(connectContext.getDatabase(), "t0"));
            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals(ColumnUsage.UseCase.all(), result.get(0).getUseCases());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testNativeTableStillRecordsIntoInternalPath() {
        PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();
        Table t0 = starRocksAssert.getTable(connectContext.getDatabase(), "t0");
        Column v1 = t0.getColumn("v1");

        mgr.recordColumnUsageForTest(t0, v1, ColumnUsage.UseCase.PREDICATE);

        TableName tableName = new TableName(connectContext.getDatabase(), "t0");
        List<ColumnUsage> result = mgr.queryPredicateColumns(tableName);
        Assertions.assertEquals(1, result.size());

        // and must not leak into the external map
        IcebergTable unrelated = mockExternalTable("catalog.db.other", "catalog", "db", "other");
        Assertions.assertTrue(mgr.queryExternalPredicateColumns(unrelated).isEmpty());
    }

    @Test
    public void testExternalCollectionDisabledByConfig() {
        boolean defaultValue = Config.enable_external_predicate_columns_collection;
        Config.enable_external_predicate_columns_collection = false;
        try {
            IcebergTable table = mockExternalTable("iceberg_catalog.db1.t2.uuid-2", "iceberg_catalog", "db1", "t2");
            Column column = new Column("c1", IntegerType.INT);
            PredicateColumnsMgr.getInstance().recordColumnUsageForTest(table, column, ColumnUsage.UseCase.PREDICATE);

            Assertions.assertTrue(PredicateColumnsMgr.getInstance().queryExternalPredicateColumns(table).isEmpty());
        } finally {
            Config.enable_external_predicate_columns_collection = defaultValue;
        }
    }

    @Test
    public void testLongOrMultibyteColumnNameStillRecordedAndQueryable() {
        // column_name is only ever hashed for PK storage, never truncated/rejected by length, so
        // long or multibyte (e.g. CJK) column names round-trip through record -> query normally.
        IcebergTable table = mockExternalTable("iceberg_catalog.db1.t3.uuid-3", "iceberg_catalog", "db1", "t3");
        Column column = new Column("列".repeat(50), IntegerType.INT);

        PredicateColumnsMgr.getInstance().recordColumnUsageForTest(table, column, ColumnUsage.UseCase.PREDICATE);

        List<String> result = PredicateColumnsMgr.getInstance().queryExternalPredicateColumns(table);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("列".repeat(50), result.get(0));
    }

    private static ColumnRefOperator nativeColumnRef(ColumnRefFactory factory, Table table, String column) {
        ColumnRefOperator ref = factory.create(column, IntegerType.BIGINT, true);
        factory.updateColumnRefToColumns(ref, table.getColumn(column), table);
        factory.updateColumnToRelationIds(ref.getId(), 1);
        return ref;
    }

    @Test
    public void testNativeUsageOfAQueryIsRecordedOnce() {
        PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();
        Table t0 = starRocksAssert.getTable(connectContext.getDatabase(), "t0");
        TableName t0Name = new TableName(connectContext.getDatabase(), "t0");
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator v1 = nativeColumnRef(factory, t0, "v1");
        ColumnRefOperator v2 = nativeColumnRef(factory, t0, "v2");
        BinaryPredicateOperator predicate =
                new BinaryPredicateOperator(BinaryType.EQ, v1, ConstantOperator.createBigint(1));

        mgr.recordPredicateColumns(predicate, factory, null);
        ColumnUsage usage = mgr.query(t0Name).get(0);
        Assertions.assertEquals(Set.of(ColumnUsage.UseCase.PREDICATE), usage.getUseCases());
        // A repeat of the same record in the same query finds the usage as it is.
        usage.setLastUsed(LocalDateTime.MIN);
        mgr.recordPredicateColumns(predicate, factory, null);
        Assertions.assertEquals(LocalDateTime.MIN, usage.getLastUsed());

        // Other columns, other kinds of records and other queries are recorded.
        mgr.recordJoinPredicate(List.of(new BinaryPredicateOperator(BinaryType.EQ, v1, v2)), factory, null);
        Assertions.assertEquals(Set.of(ColumnUsage.UseCase.PREDICATE, ColumnUsage.UseCase.JOIN),
                usage.getUseCases());
        Assertions.assertTrue(usage.getLastUsed().isAfter(LocalDateTime.MIN));
        usage.setLastUsed(LocalDateTime.MIN);
        ColumnRefFactory nextQuery = new ColumnRefFactory();
        ColumnRefOperator nextV1 = nativeColumnRef(nextQuery, t0, "v1");
        mgr.recordPredicateColumns(new BinaryPredicateOperator(BinaryType.EQ, nextV1, ConstantOperator.createBigint(1)),
                nextQuery, null);
        Assertions.assertTrue(usage.getLastUsed().isAfter(LocalDateTime.MIN));

        // A column ref that a rewrite registers again with another column is recorded for the new column.
        Assertions.assertEquals(2, mgr.query(t0Name).size());
        factory.updateColumnRefToColumns(v1, t0.getColumn("v3"), t0);
        mgr.recordPredicateColumns(predicate, factory, null);
        Assertions.assertEquals(3, mgr.query(t0Name).size());

        // A reset forgets what the query recorded.
        mgr.reset();
        mgr.recordPredicateColumns(predicate, factory, null);
        List<ColumnUsage> recorded = mgr.query(t0Name);
        Assertions.assertEquals(1, recorded.size());
        Assertions.assertEquals("v3", recorded.get(0).getOlapColumnName((OlapTable) t0).orElseThrow());
    }

    @Test
    public void testResetClearsExternalState() {
        IcebergTable table = mockExternalTable("iceberg_catalog.db1.t4.uuid-4", "iceberg_catalog", "db1", "t4");
        Column column = new Column("c1", IntegerType.INT);
        PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();

        mgr.recordColumnUsageForTest(table, column, ColumnUsage.UseCase.PREDICATE);
        Assertions.assertEquals(1, mgr.queryExternalPredicateColumns(table).size());

        mgr.reset();
        Assertions.assertTrue(mgr.queryExternalPredicateColumns(table).isEmpty());
    }

    @Test
    public void testQueryRetainsHistoryWhenTtlDisabled() {
        IcebergTable table = mockExternalTable("iceberg_catalog.db1.t5.uuid-5", "iceberg_catalog", "db1", "t5");
        Column column = new Column("c1", IntegerType.INT);
        PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();
        mgr.recordColumnUsageForTest(table, column, ColumnUsage.UseCase.PREDICATE);
        Assertions.assertEquals(1, mgr.queryExternalPredicateColumns(table).size());

        long beforeValue = Config.statistic_external_predicate_columns_ttl_hours;
        Config.statistic_external_predicate_columns_ttl_hours = -1;
        try {
            Assertions.assertEquals(1, mgr.queryExternalPredicateColumns(table).size());
        } finally {
            Config.statistic_external_predicate_columns_ttl_hours = beforeValue;
        }
    }
}
