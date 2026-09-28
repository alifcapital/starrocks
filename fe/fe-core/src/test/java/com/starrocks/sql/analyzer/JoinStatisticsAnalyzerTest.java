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

package com.starrocks.sql.analyzer;

import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ProjectNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.JoinStatisticsStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.JoinStatisticsDefinition;
import com.starrocks.statistic.JoinStatisticsMeta;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

class JoinStatisticsAnalyzerTest {
    private static ConnectContext context;
    private static final String PAIR = "SELECT t.data, u.c1 FROM iceberg0.unpartitioned_db.t0 t "
            + "JOIN iceberg0.unpartitioned_db.t_numeric u ON t.id = u.id";

    @BeforeAll
    static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        context = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockCatalog(context, "iceberg0");
    }

    private JoinStatisticsStmt analyze(String sql) {
        JoinStatisticsStmt statement = (JoinStatisticsStmt) SqlParser.parseSingleStatement(sql,
                context.getSessionVariable().getSqlMode());
        Analyzer.analyze(statement, context);
        return statement;
    }

    @Test
    void verboseInspectionParsesBoundedPagesWithoutChangingCollectionSyntax() {
        var first = analyze("SHOW VERBOSE JOIN STATISTICS test");
        Assertions.assertTrue(first.isVerbose());
        Assertions.assertEquals(100, first.getInspectionLimit());
        Assertions.assertEquals(0, first.getInspectionOffset());
        var page = analyze("SHOW VERBOSE JOIN STATISTICS test LIMIT 7 OFFSET 12345");
        Assertions.assertEquals(7, page.getInspectionLimit());
        Assertions.assertEquals(12345, page.getInspectionOffset());
        Assertions.assertFalse(analyze("SHOW JOIN STATISTICS test").isVerbose());
        Assertions.assertThrows(Exception.class, () -> analyze("SHOW VERBOSE JOIN STATISTICS test LIMIT 1001"));
        Assertions.assertThrows(Exception.class, () -> analyze("SHOW VERBOSE JOIN STATISTICS"));
    }

    @Test
    void nativeSelfJoinKeepsPhysicalIdentityAndSeparateRelationRoles() throws Exception {
        var tables = new com.starrocks.utframe.StarRocksAssert(context);
        tables.withDatabase("join_stats_native_test").useDatabase("join_stats_native_test");
        tables.withTable("CREATE TABLE roles (id BIGINT, flag BIGINT) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        try {
            var definition = analyze("CREATE JOIN STATISTICS native_roles AS SELECT a.flag,b.flag "
                    + "FROM default_catalog.join_stats_native_test.roles a "
                    + "JOIN default_catalog.join_stats_native_test.roles b ON a.id=b.id").getDefinition();
            var left = definition.getSources().get(0);
            var right = definition.getSources().get(1);
            Assertions.assertEquals(left.getTableUuid(), right.getTableUuid());
            Assertions.assertNotEquals(left.getUuid(), right.getUuid());
            var roundTrip = com.starrocks.persist.gson.GsonUtils.GSON.fromJson(
                    com.starrocks.persist.gson.GsonUtils.GSON.toJson(definition), JoinStatisticsDefinition.class)
                    .immutableCopy();
            Assertions.assertEquals(right.getUuid(), roundTrip.getSources().get(1).getUuid());
            Assertions.assertEquals(right.getTableUuid(), roundTrip.getSources().get(1).getTableUuid());
            var nativeTable = (com.starrocks.catalog.OlapTable) GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getTable(context, left.getTableName()).orElseThrow();
            long databaseId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("join_stats_native_test").getId();
            var version = com.starrocks.statistic.JoinStatisticsCollector.class.getDeclaredMethod(
                    "nativeVersion", com.starrocks.catalog.OlapTable.class, long.class);
            version.setAccessible(true);
            long before = (long) version.invoke(null, nativeTable, databaseId);
            nativeTable.lastSchemaUpdateTime.set(System.currentTimeMillis());
            Assertions.assertEquals(before, (long) version.invoke(null, nativeTable, databaseId),
                    "Transient FE schema clocks must not invalidate a stable collection cohort");
            var partition = nativeTable.getPhysicalPartitions().iterator().next();
            partition.updateVisibleVersion(partition.getVisibleVersion() + 1);
            Assertions.assertNotEquals(before, (long) version.invoke(null, nativeTable, databaseId),
                    "Published native data changes must invalidate a collection cohort");
        } finally {
            tables.dropTable("roles");
        }
    }

    @Test
    void analyzePrivilegesKeepEverySourcesExternalCatalogAndResolvedTable() {
        var statement = analyze("CREATE JOIN STATISTICS permissions_stats AS " + PAIR);
        try (MockedStatic<Authorizer> authorizer = Mockito.mockStatic(Authorizer.class, Mockito.CALLS_REAL_METHODS)) {
            authorizer.when(() -> Authorizer.checkResolvedTableAction(Mockito.eq(context), Mockito.any(TableName.class),
                    Mockito.any(Table.class), Mockito.any(PrivilegeType.class))).thenAnswer(invocation -> null);
            new AuthorizerStmtVisitor().visitJoinStatisticsStatement(statement, context);
            for (var source : statement.getDefinition().getSources()) {
                Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, source.getTableName())
                        .orElseThrow();
                for (PrivilegeType privilege : List.of(PrivilegeType.SELECT, PrivilegeType.INSERT)) {
                    authorizer.verify(() -> Authorizer.checkResolvedTableAction(
                            context, source.getTableName(), table, privilege));
                }
            }
        }
    }

    @Test
    void resolvedPrivilegeCheckUsesRegisteredCatalogSpelling() throws Exception {
        boolean previous = GlobalVariable.enableTableNameCaseInsensitive;
        GlobalVariable.enableTableNameCaseInsensitive = true;
        var provider = Mockito.mock(AccessControlProvider.class);
        var controller = Mockito.mock(AccessController.class);
        Mockito.when(provider.getAccessControlOrDefault("MixedIceberg")).thenReturn(controller);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getCatalogName()).thenReturn("MixedIceberg");
        Mockito.when(table.isTable()).thenReturn(true);
        try (MockedStatic<Authorizer> authorizer = Mockito.mockStatic(Authorizer.class, Mockito.CALLS_REAL_METHODS)) {
            authorizer.when(Authorizer::getInstance).thenReturn(provider);
            var name = new TableName("MixedIceberg", "db", "t");
            Assertions.assertEquals("mixediceberg", name.getCatalog());
            for (PrivilegeType privilege : List.of(PrivilegeType.SELECT, PrivilegeType.INSERT)) {
                Authorizer.checkResolvedTableAction(context, name, table, privilege);
                Mockito.verify(controller).checkTableAction(context, name, privilege);
            }
        } finally {
            GlobalVariable.enableTableNameCaseInsensitive = previous;
        }
    }

    @Test
    void physicalProjectionPreservesJoinStatisticsProvenance() throws Exception {
        var definition = analyze("CREATE JOIN STATISTICS projection_stats AS " + PAIR).getDefinition();
        var meta = new JoinStatisticsMeta(123457, definition);
        var registry = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getJoinStatisticsRegistry();
        registry.replay(meta, false);
        try {
            var plan = UtFrameUtils.getPlanAndFragment(context,
                    "SELECT id, id + 1 FROM iceberg0.unpartitioned_db.t0 WHERE data = 'approved'").second;
            var scope = PlanNode.class.getDeclaredField("joinStatisticsScope");
            var planner = PlanNode.class.getDeclaredField("joinStatisticsPlanner");
            scope.setAccessible(true);
            planner.setAccessible(true);
            java.util.ArrayDeque<PlanNode> pending = new java.util.ArrayDeque<>();
            plan.getFragments().forEach(fragment -> pending.add(fragment.getPlanRoot()));
            int projects = 0;
            while (!pending.isEmpty()) {
                PlanNode node = pending.remove();
                if (node instanceof ProjectNode) {
                    projects++;
                    Assertions.assertNotNull(scope.get(node), "Projection must retain its source and predicates for RF");
                    Assertions.assertNotNull(planner.get(node), "Projection must retain the query-local estimator");
                }
                pending.addAll(node.getChildren());
            }
            Assertions.assertTrue(projects > 0, "The regression must exercise a physical Project");
        } finally {
            registry.replay(meta, true);
        }
    }

    @Test
    void pairDefinitionKeepsBothPredicateGroupsAndComparisonKey() {
        JoinStatisticsStmt statement = analyze("CREATE JOIN STATISTICS pair_stats AS " + PAIR);
        JoinStatisticsDefinition definition = statement.getDefinition();
        Assertions.assertEquals(JoinStatisticsStmt.Action.CREATE, statement.getAction());
        Assertions.assertFalse(statement.isAsynchronous());
        Assertions.assertEquals(List.of("data"), definition.getSources().get(0).getPredicates());
        Assertions.assertEquals(List.of("c1"), definition.getSources().get(1).getPredicates());
        Assertions.assertEquals(List.of("id"), definition.getDomains().get(0).getColumns().get(0));
        Assertions.assertEquals(2, definition.getDomains().get(0).getColumns().size());
        Assertions.assertEquals("iceberg0", definition.getSources().get(0).getTableName().getCatalog());
    }

    @Test
    void starUsesOneCoordinatedKeyDomain() {
        JoinStatisticsDefinition definition = analyze("CREATE JOIN STATISTICS star_stats AS "
                + PAIR + " JOIN iceberg0.partitioned_db.t1 h ON h.id = t.id").getDefinition();
        Assertions.assertEquals(3, definition.getSources().size());
        Assertions.assertEquals(1, definition.getDomains().size());
        Assertions.assertEquals(3, definition.getDomains().get(0).getColumns().size());
    }

    @Test
    void chainKeepsDifferentKeyDomainsSeparate() {
        JoinStatisticsDefinition definition = analyze("CREATE JOIN STATISTICS chain_stats AS "
                + PAIR + " JOIN iceberg0.partitioned_db.t1 h ON h.id = u.c2").getDefinition();
        Assertions.assertEquals(2, definition.getDomains().size());
        Assertions.assertEquals(List.of("c2"), definition.getDomains().get(1).getColumns().get(1));
    }

    @Test
    void refreshAndDropResolveThePersistedDefinition() {
        JoinStatisticsDefinition definition = analyze("CREATE JOIN STATISTICS lifecycle_stats "
                + "PROPERTIES('mcv_size'='200') WITH ASYNC MODE AS " + PAIR).getDefinition();
        JoinStatisticsMeta meta = new JoinStatisticsMeta(123456, definition);
        var registry = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getJoinStatisticsRegistry();
        registry.replay(meta, false);
        try {
            JoinStatisticsStmt refresh = analyze("ANALYZE JOIN STATISTICS lifecycle_stats WITH SYNC MODE");
            Assertions.assertEquals("200", refresh.getDefinition().getProperties().get("mcv_size"));
            Assertions.assertEquals(JoinStatisticsStmt.Action.ANALYZE, refresh.getAction());
            Assertions.assertEquals(JoinStatisticsStmt.Action.DROP,
                    analyze("DROP JOIN STATISTICS lifecycle_stats").getAction());
            Assertions.assertEquals(JoinStatisticsStmt.Action.SHOW, analyze("SHOW JOIN STATISTICS").getAction());
        } finally {
            registry.replay(meta, true);
        }
        Assertions.assertNull(analyze("DROP JOIN STATISTICS IF EXISTS lifecycle_stats").getDefinition());
        Assertions.assertThrows(SemanticException.class, () -> analyze("ANALYZE JOIN STATISTICS lifecycle_stats"));
    }

    @Test
    void schemaCompatibilityPreservesKeyEquality() {
        Assertions.assertTrue(JoinStatisticsDefinition.matchesKeyType(com.starrocks.type.IntegerType.INT, "bigint(20)"));
        Assertions.assertTrue(JoinStatisticsDefinition.matchesKeyType(com.starrocks.type.IntegerType.BIGINT, "BIGINT"));
        Assertions.assertFalse(JoinStatisticsDefinition.matchesKeyType(com.starrocks.type.IntegerType.BIGINT, "int(11)"));
        Assertions.assertFalse(JoinStatisticsDefinition.matchesKeyType(com.starrocks.type.VarcharType.VARCHAR, "bigint(20)"));
        Assertions.assertTrue(JoinStatisticsDefinition.matchesKeyType(com.starrocks.type.VarcharType.VARCHAR, "varchar(100)"));
        Assertions.assertThrows(SemanticException.class, () -> analyze("CREATE JOIN STATISTICS bad "
                + "PROPERTIES('mcv_size'='4097') AS " + PAIR));
    }

    @Test
    void rejectsDefinitionsWhoseRowSetCannotBeReusedAcrossQueries() {
        for (String suffix : List.of(" WHERE t.id > 10", " LIMIT 100", " ORDER BY t.id")) {
            Assertions.assertThrows(SemanticException.class, () -> analyze("CREATE JOIN STATISTICS bad AS " + PAIR + suffix));
        }
        Assertions.assertThrows(SemanticException.class, () -> analyze("CREATE JOIN STATISTICS bad AS "
                + PAIR.replace("JOIN iceberg0", "LEFT JOIN iceberg0")));
        Assertions.assertThrows(SemanticException.class, () -> analyze("CREATE JOIN STATISTICS bad AS "
                + PAIR.replace("t.data, u.c1", "t.*")));
        Assertions.assertThrows(SemanticException.class, () -> analyze("CREATE JOIN STATISTICS bad AS "
                + PAIR.replace("t.id = u.id", "t.id < u.id")));
        Assertions.assertThrows(SemanticException.class, () -> analyze("CREATE JOIN STATISTICS bad "
                + "PROPERTIES('mcv_size'='0') AS " + PAIR));
    }
}
