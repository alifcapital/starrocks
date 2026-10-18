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

package com.starrocks.sql.plan;

import com.starrocks.catalog.Function;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.CTEContext;
import com.starrocks.sql.optimizer.transformer.IdenticalSubqueryCollector;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class IdenticalSubqueryReusePlanTest extends PlanTestBase {
    private static final String AGG_T0 = "select v1, sum(v2) as s from t0 where v3 > 1 group by v1";

    // The shape that Tableau sends: a custom SQL with its own WITH, inserted once per use
    private static final String CUSTOM_SQL = "with w as (select v1, v2 from t0 where v3 > 1), " +
            "w2 as (select v4, sum(v5) as s5 from t1 group by v4) " +
            "select w.v1, sum(w.v2) as s, max(w2.s5) as m from w join w2 on w.v1 = w2.v4 " +
            "where w.v1 in (select v1 from w where v2 > 0) group by w.v1";

    private SessionVariable saved;

    @BeforeAll
    public static void createOtherDatabase() throws Exception {
        // a table with the name of test.t0 in another database
        starRocksAssert.withDatabase("db1").useDatabase("db1")
                .withTable("CREATE TABLE `t0` (`v1` bigint NULL, `v2` bigint NULL, `v3` bigint NULL) " +
                        "ENGINE=OLAP DUPLICATE KEY(`v1`) DISTRIBUTED BY HASH(`v1`) BUCKETS 3 " +
                        "PROPERTIES (\"replication_num\" = \"1\")")
                .useDatabase("test");
    }

    @BeforeEach
    public void enableReuse() {
        saved = connectContext.getSessionVariable();
        connectContext.setSessionVariable((SessionVariable) saved.clone());
        connectContext.getSessionVariable().setCboCTEReuseIdenticalSubqueries(true);
        // reuse whenever the planner has a CTE, so the plan shows whether we made one
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
    }

    @AfterEach
    public void restore() {
        connectContext.setSessionVariable(saved);
    }

    private static int count(String plan, String pattern) {
        return StringUtils.countMatches(plan, pattern);
    }

    private String planWithoutReuse(String sql) throws Exception {
        connectContext.getSessionVariable().setCboCTEReuseIdenticalSubqueries(false);
        try {
            return getFragmentPlan(sql);
        } finally {
            connectContext.getSessionVariable().setCboCTEReuseIdenticalSubqueries(true);
        }
    }

    private void assertReused(String sql, String table, int scansWithReuse, int scansWithoutReuse) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks");
        Assertions.assertEquals(scansWithReuse, count(plan, "TABLE: " + table), plan);

        String original = planWithoutReuse(sql);
        assertNotContains(original, "MultiCastDataSinks");
        Assertions.assertEquals(scansWithoutReuse, count(original, "TABLE: " + table), original);
    }

    private void assertNotReused(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "MultiCastDataSinks");
    }

    private List<List<SubqueryRelation>> collect(String sql) throws Exception {
        QueryStatement stmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        return IdenticalSubqueryCollector.collect(stmt.getQueryRelation(), connectContext);
    }

    @Test
    public void testCopiesWithDifferentOuterFilters() throws Exception {
        String sql = "select a.v1, a.s, b.s from (" + AGG_T0 + ") a join (" + AGG_T0 + ") b on a.v1 = b.v1 " +
                "where a.s > 10 and b.s < 5";
        assertReused(sql, "t0", 1, 2);
    }

    @Test
    public void testThreeCopiesFilterOnSome() throws Exception {
        String sql = "select a.v1 from (" + AGG_T0 + ") a join (" + AGG_T0 + ") b on a.v1 = b.v1 " +
                "join (" + AGG_T0 + ") c on b.v1 = c.v1 where a.s > 10";
        assertReused(sql, "t0", 1, 3);
        Assertions.assertEquals(1, collect(sql).size());
        Assertions.assertEquals(3, collect(sql).get(0).size());
    }

    @Test
    public void testCopiesWithLimit() throws Exception {
        String body = "select v1, v2 from t0 order by v1 limit 10";
        String sql = "select * from (" + body + ") a join (" + body + ") b on a.v1 = b.v1";
        assertReused(sql, "t0", 1, 2);
    }

    @Test
    public void testGroupByAboveCopies() throws Exception {
        String sql = "select v1, sum(s) from (select * from (" + AGG_T0 + ") a union all " +
                "select * from (" + AGG_T0 + ") b) u group by v1";
        assertReused(sql, "t0", 1, 2);
    }

    @Test
    public void testSelfJoinOfCopies() throws Exception {
        String sql = "select count(*) from (select v1, v2 from t0 where v3 = 1) a " +
                "join (select v1, v2 from t0 where v3 = 1) b on a.v1 = b.v2";
        assertReused(sql, "t0", 1, 2);
    }

    @Test
    public void testCopiesWithTheirOwnWith() throws Exception {
        // Each copy names its CTE w and w2, and each copy reads w twice, so every copy has a user CTE too
        String sql = "select a.v1, a.s, b.m from (" + CUSTOM_SQL + ") a join (" + CUSTOM_SQL + ") b " +
                "on a.v1 = b.v1 where a.s > 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks");
        Assertions.assertEquals(1, collect(sql).size());
        // one producer of the copy reads t1 once; without the reuse every copy reads it
        Assertions.assertEquals(1, count(plan, "TABLE: t1"), plan);
        Assertions.assertEquals(2, count(planWithoutReuse(sql), "TABLE: t1"));
    }

    @Test
    public void testTableauShape() throws Exception {
        // The copies sit one level down, under two different aggregates
        String sql = "select t0.v1, t0.x, t1.y from " +
                "(select `Custom SQL Query`.v1, sum(`Custom SQL Query`.s) as x from (" + CUSTOM_SQL +
                ") `Custom SQL Query` group by `Custom SQL Query`.v1) t0 " +
                "join (select `Custom SQL Query`.v1, max(`Custom SQL Query`.m) as y from (" + CUSTOM_SQL +
                ") `Custom SQL Query` where `Custom SQL Query`.s > 0 group by `Custom SQL Query`.v1) t1 " +
                "on t0.v1 = t1.v1";
        List<List<SubqueryRelation>> groups = collect(sql);
        Assertions.assertEquals(1, groups.size());
        Assertions.assertEquals("Custom SQL Query", groups.get(0).get(0).getAlias().getTbl());
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks");
        Assertions.assertEquals(1, count(plan, "TABLE: t1"), plan);
    }

    @Test
    public void testIdenticalSubqueryInsideIdenticalSubqueries() throws Exception {
        // b appears alone first and then inside both copies of a: b is chosen first, and the producer of a
        // consumes it
        String b = "select v1, v2 from t0 where v3 = 7";
        String a = "select x.v1, count(*) as c from (" + b + ") x join t1 on x.v2 = t1.v4 group by x.v1";
        String sql = "select * from (" + b + ") p join (" + a + ") q on p.v1 = q.v1 join (" + a + ") r " +
                "on q.v1 = r.v1";
        List<List<SubqueryRelation>> groups = collect(sql);
        Assertions.assertEquals(2, groups.size(), groups.toString());
        Assertions.assertEquals(3, groups.get(0).size());
        Assertions.assertEquals(2, groups.get(1).size());
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks");

        // when a comes first, the copies of b inside a are left to the producer of a
        sql = "select * from (" + a + ") q join (" + a + ") r on q.v1 = r.v1 join (" + b + ") p on p.v1 = q.v1";
        groups = collect(sql);
        Assertions.assertEquals(1, groups.size(), groups.toString());
        Assertions.assertEquals(2, groups.get(0).size());
        getFragmentPlan(sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {"rand()", "random()", "uuid()", "uuid_numeric()"})
    public void testNonDeterministicFunction(String fn) throws Exception {
        String body = "select v1, " + fn + " as r from t0";
        String sql = "select * from (" + body + ") a join (" + body + ") b on a.v1 = b.v1";
        Assertions.assertTrue(collect(sql).isEmpty());
        assertNotReused(sql);
    }

    @Test
    public void testNonDeterministicFunctionInReferencedCte() throws Exception {
        String sql = "with w as (select v1, rand() as r from t0) " +
                "select * from (select v1, r from w where v1 > 1) a join (select v1, r from w where v1 > 1) b " +
                "on a.v1 = b.v1";
        Assertions.assertTrue(collect(sql).isEmpty());
    }

    @Test
    public void testFoldedTimeFunction() throws Exception {
        // now() is one constant for the whole query, so the copies are equal
        String body = "select v1, now() as n, current_date() as d from t0 where v3 > 1";
        String sql = "select * from (" + body + ") a join (" + body + ") b on a.v1 = b.v1";
        assertReused(sql, "t0", 1, 2);
    }

    @Test
    public void testShadowedCteName() throws Exception {
        // Both x and y read "SELECT v1 FROM w", but w is a different CTE in each scope
        String sql = "select * from " +
                "(with w as (select v1 from t0) select * from (select v1 from w) x) a join " +
                "(with w as (select v4 as v1 from t1) select * from (select v1 from w) y) b on a.v1 = b.v1";
        Assertions.assertTrue(collect(sql).isEmpty());
        assertNotReused(sql);
    }

    @Test
    public void testCteShadowsTable() throws Exception {
        String sql = "select * from (select v1 from t0) a join " +
                "(with t0 as (select v4 as v1 from t1) select * from (select v1 from t0) z) b on a.v1 = b.v1";
        Assertions.assertTrue(collect(sql).isEmpty());
        assertNotReused(sql);
    }

    @Test
    public void testSameCteBodyInDifferentScopes() throws Exception {
        // Each copy defines its own w with the same body, so the copies read the same rows
        String copy = "with w as (select v1 from t0 where v2 > 1) select * from (select v1 from w) x";
        String sql = "select * from (" + copy + ") a join (" + copy + ") b on a.v1 = b.v1";
        // a and b are equal, so x inside them is not collected again
        List<List<SubqueryRelation>> groups = collect(sql);
        Assertions.assertEquals(1, groups.size());
        Assertions.assertEquals("a", groups.get(0).get(0).getAlias().getTbl());

        // The inner aliases and CTE names are not in the key either
        sql = "select * from " +
                "(with w as (select v1 from t0 where v2 > 1) select * from (select v1 from w) x) a join " +
                "(with u as (select v1 from t0 where v2 > 1) select * from (select v1 from u) y) b on a.v1 = b.v1";
        groups = collect(sql);
        Assertions.assertEquals(1, groups.size());
        Assertions.assertEquals("a", groups.get(0).get(0).getAlias().getTbl());
        assertContains(getFragmentPlan(sql), "MultiCastDataSinks");
    }

    @Test
    public void testSubqueriesUnderExpressionSubquery() throws Exception {
        // A FROM subquery under a scalar subquery is not collected, the scalar subquery may be correlated
        String inner = "(select max(z.v4) from (select v4, v5 from t1 where v6 > 1) z where z.v5 = t0.v2)";
        String sql = "select v1, " + inner + " as m1, " + inner + " as m2 from t0";
        Assertions.assertTrue(collect(sql).isEmpty());
        getFragmentPlan(sql);
    }

    @Test
    public void testTableFunction() throws Exception {
        String body = "select v1, unnest from tarray, unnest(v3)";
        String sql = "select * from (" + body + ") a join (" + body + ") b on a.v1 = b.v1";
        Assertions.assertTrue(collect(sql).isEmpty());
        assertNotReused(sql);
    }

    @Test
    public void testSystemTable() throws Exception {
        String body = "select table_name from information_schema.tables where table_schema = 'test'";
        String sql = "select * from (" + body + ") a join (" + body + ") b on a.table_name = b.table_name";
        Assertions.assertTrue(collect(sql).isEmpty());
    }

    @Test
    public void testHintInsideSubquery() throws Exception {
        String body = "select /*+ SET_VAR(pipeline_dop=1) */ v1, v2 from t0 where v3 = 1";
        String sql = "select * from (" + body + ") a join (" + body + ") b on a.v1 = b.v1";
        Assertions.assertTrue(collect(sql).isEmpty());

        String joinHint = "select t0.v1 from t0 join [broadcast] t1 on t0.v1 = t1.v4";
        sql = "select * from (" + joinHint + ") a join (" + joinHint + ") b on a.v1 = b.v1";
        Assertions.assertTrue(collect(sql).isEmpty());
    }

    @Test
    public void testUserDefinedFunction() throws Exception {
        String body = "select v1, abs(v2) as a from t0";
        String sql = "select * from (" + body + ") a join (" + body + ") b on a.v1 = b.v1";
        QueryStatement stmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assertions.assertEquals(1, IdenticalSubqueryCollector.collect(stmt.getQueryRelation(), connectContext).size());

        // Make every abs() look like a Java UDF
        new AstTraverser<Void, Void>() {
            @Override
            public Void visitFunctionCall(FunctionCallExpr node, Void context) {
                Function fn = node.getFn().copy();
                fn.setBinaryType(TFunctionBinaryType.SRJAR);
                node.setFn(fn);
                return super.visitFunctionCall(node, context);
            }
        }.visit(stmt);
        Assertions.assertTrue(IdenticalSubqueryCollector.collect(stmt.getQueryRelation(), connectContext).isEmpty());
    }

    @Test
    public void testDifferentPartitions() throws Exception {
        String sql = "select * from (select * from part_t1 partition (p1)) a " +
                "join (select * from part_t1 partition (p2)) b on a.v4 = b.v4";
        Assertions.assertTrue(collect(sql).isEmpty());
    }

    @Test
    public void testDisabled() throws Exception {
        String sql = "select * from (" + AGG_T0 + ") a join (" + AGG_T0 + ") b on a.v1 = b.v1";
        connectContext.getSessionVariable().setCboCTEReuseIdenticalSubqueries(false);
        assertNotReused(sql);

        connectContext.getSessionVariable().setCboCTEReuseIdenticalSubqueries(true);
        connectContext.getSessionVariable().setCboCteReuse(false);
        Assertions.assertTrue(collect(sql).isEmpty());
        assertNotReused(sql);
    }

    // Pairs of subquery bodies and whether the key takes them as one. The key is structural: names that resolve
    // to the same column are equal, anything that the analyzer keeps apart is not.
    private static Stream<Arguments> keyMatrix() {
        String longA = "'" + "x".repeat(60) + "a'";
        String longB = "'" + "x".repeat(60) + "b'";
        return Stream.of(
                Arguments.of("select v1, sum(v2) as s from t0 group by v1",
                        "SELECT  v1 ,\n SUM(v2) AS s /* note */ FROM t0 GROUP BY v1", true),
                Arguments.of("select `v1` from `t0` where v2 > 1", "select v1 from t0 where v2 > 1", true),
                Arguments.of("select v1 from t0 where v2 > 1", "select v1 from test.t0 where v2 > 1", true),
                Arguments.of("select v1 from t0 where v2 > 1", "select t0.v1 from t0 where t0.v2 > 1", true),
                Arguments.of("select * from t0", "select v1, v2, v3 from t0", true),
                // column names are not case sensitive, output names included
                Arguments.of("select v1 from t0 where V2 > 1", "select v1 from t0 where v2 > 1", true),
                Arguments.of("select V1 from t0 where v2 > 1", "select v1 from t0 where v2 > 1", true),
                Arguments.of("select v1 as Total from t0", "select v1 as total from t0", true),
                Arguments.of("select v1 as s from t0", "select v1 as total from t0", false),
                // aliases of relations are not in the key, the column that a name resolves to is
                Arguments.of("select x.v1 from t0 x", "select y.v1 from t0 y", true),
                Arguments.of("select x.v1 from t0 x join t0 y on x.v2 = y.v2",
                        "select y.v1 from t0 x join t0 y on x.v2 = y.v2", false),
                Arguments.of("select v1 from t0 where v2 > 1 and v3 > 1", "select v1 from t0 where v3 > 1 and v2 > 1",
                        false),
                Arguments.of("select v1 from t0 where v2 in (1, 2)", "select v1 from t0 where v2 in (2, 1)", false),
                Arguments.of("select v1 from t0 where v2 > 1", "select v1 from t0 where v2 > 1.0", false),
                // the name of a CTE is not in the key, its body and column list are
                Arguments.of("with w1 as (select v1 from t0) select v1 from w1",
                        "with w2 as (select v1 from t0) select v1 from w2", true),
                Arguments.of("with w as (select v1 from t0) select v1 from w",
                        "with w as (select v1 from t0 where v2 > 1) select v1 from w", false),
                Arguments.of("with w(a, b) as (select v1, v2 from t0) select a from w",
                        "with w(b, a) as (select v1, v2 from t0) select a from w", false),
                Arguments.of("select test.t0.v1 from test.t0 join db1.t0 on test.t0.v2 = db1.t0.v2",
                        "select db1.t0.v1 from test.t0 join db1.t0 on test.t0.v2 = db1.t0.v2", false),
                Arguments.of("select test.t0.v1 from test.t0 join db1.t0 on test.t0.v2 = db1.t0.v2 " +
                                "where test.t0.v3 = 1",
                        "select test.t0.v1 from test.t0 join db1.t0 on test.t0.v2 = db1.t0.v2 where db1.t0.v3 = 1",
                        false),
                // a long string is compared in full
                Arguments.of("select v1 from t0 where concat(v1, " + longA + ") = " + longA,
                        "select v1 from t0 where concat(v1, " + longA + ") = " + longB, false),
                Arguments.of("select v1 from t0 where concat(v1, " + longA + ") = " + longA,
                        "select v1 from t0 where concat(v1, " + longA + ") = " + longA, true));
    }

    @ParameterizedTest
    @MethodSource("keyMatrix")
    public void testKeyMatrix(String left, String right, boolean merged) throws Exception {
        String sql = "select 1 from (" + left + ") a cross join (" + right + ") b";
        List<List<SubqueryRelation>> groups = collect(sql);
        if (merged) {
            Assertions.assertEquals(1, groups.size(), sql);
            Assertions.assertEquals(2, groups.get(0).size(), sql);
        } else {
            Assertions.assertTrue(groups.isEmpty(), sql);
        }
    }

    // Copies that differ only in the case of an output name are one group, and the query still shows each name
    // as its copy writes it
    @Test
    public void testOutputNameCase() throws Exception {
        String upper = "(select V1, v2 as Total from t0 where v3 > 1) a";
        String lower = "(select v1, v2 as total from t0 where v3 > 1) b";
        String[] queries = {
                "select * from " + upper + " cross join " + lower,
                "select b.*, a.* from " + upper + " cross join " + lower,
                "select a.v1, a.total, b.V1, b.TOTAL from " + upper + " join " + lower + " on a.v1 = b.v1"};
        List<List<String>> names = List.of(
                List.of("V1", "Total", "v1", "total"),
                List.of("v1", "total", "V1", "Total"),
                List.of("v1", "total", "V1", "TOTAL"));
        for (int i = 0; i < queries.length; i++) {
            Assertions.assertEquals(1, collect(queries[i]).size(), queries[i]);
            ExecPlan plan = getExecPlan(queries[i]);
            assertContains(plan.getExplainString(TExplainLevel.NORMAL), "MultiCastDataSinks");
            Assertions.assertEquals(names.get(i), plan.getColNames(), queries[i]);
        }
    }

    // The CTE column list decides which column a name reads
    @Test
    public void testCteColumnList() throws Exception {
        String sql = "select p.a, q.a from " +
                "(with w(a, b) as (select v1, v2 from t0) select * from (select a from w) x) p join " +
                "(with w(b, a) as (select v1, v2 from t0) select * from (select a from w) y) q on p.a = q.a";
        Assertions.assertTrue(collect(sql).isEmpty());
        assertContains(getFragmentPlan(sql), "equal join conjunct: 1: v1 = 5: v2");
    }

    // A column of test.t0 and the same column of db1.t0 are different columns
    @Test
    public void testSameTableNameInTwoDatabases() throws Exception {
        String join = " from test.t0 join db1.t0 on test.t0.v2 = db1.t0.v2";
        String sql = "select a.v1, b.v1 from (select test.t0.v1" + join + ") a join (select db1.t0.v1" + join + ") b " +
                "on a.v1 = b.v1";
        Assertions.assertTrue(collect(sql).isEmpty());
        assertNotContains(getFragmentPlan(sql), "MultiCastDataSinks");
    }

    // Analyzes the statement as a session with security policies does, see ConnectProcessor
    private QueryStatement analyzeWithPolicy(String sql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
        new AstTraverser<Void, Void>() {
            @Override
            public Void visitRelation(Relation relation, Void context) {
                relation.setNeedRewrittenByPolicy(true);
                return null;
            }
        }.visit(stmt);
        Analyzer.analyze(stmt, connectContext);
        return (QueryStatement) stmt;
    }

    private static int countPolicyViews(QueryStatement stmt) {
        int[] count = new int[1];
        new AstTraverser<Void, Void>() {
            @Override
            public Void visitSubqueryRelation(SubqueryRelation node, Void context) {
                if (node.isSecurityPolicyView()) {
                    count[0]++;
                }
                return super.visitSubqueryRelation(node, context);
            }
        }.visit(stmt);
        return count[0];
    }

    // A table read twice is not a copy of a subquery, with or without a security policy around it
    @ParameterizedTest
    @ValueSource(strings = {"none", "row", "masking"})
    public void testTableReadTwiceUnderPolicy(String policy) throws Exception {
        String selfJoin = "select a.v1, b.v2 from t0 a join t0 b on a.v1 = b.v1";
        String copies = "select * from (" + AGG_T0 + ") x join (" + AGG_T0 + ") y on x.v1 = y.v1";
        if (policy.equals("none")) {
            Assertions.assertTrue(collect(selfJoin).isEmpty());
            assertNotContains(getFragmentPlan(selfJoin), "MultiCastDataSinks");
            return;
        }
        Expr rowFilter = SqlParser.parseSqlToExpr("v3 > 0", SqlModeHelper.MODE_DEFAULT);
        Map<String, Expr> masking = Map.of("v2", SqlParser.parseSqlToExpr("v2 * 0", SqlModeHelper.MODE_DEFAULT));
        try (MockedStatic<Authorizer> authorizer = Mockito.mockStatic(Authorizer.class)) {
            authorizer.when(() -> Authorizer.getRowAccessPolicy(Mockito.any(), Mockito.any()))
                    .thenReturn(policy.equals("row") ? rowFilter : null);
            authorizer.when(() -> Authorizer.getColumnMaskingPolicy(Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenReturn(policy.equals("masking") ? masking : Map.of());

            QueryStatement stmt = analyzeWithPolicy(selfJoin);
            Assertions.assertEquals(2, countPolicyViews(stmt));
            Assertions.assertTrue(IdenticalSubqueryCollector.collect(stmt.getQueryRelation(), connectContext)
                    .isEmpty());

            // the copies that the user wrote are still one group, each with its own policy view inside
            stmt = analyzeWithPolicy(copies);
            Assertions.assertEquals(2, countPolicyViews(stmt));
            List<List<SubqueryRelation>> groups =
                    IdenticalSubqueryCollector.collect(stmt.getQueryRelation(), connectContext);
            Assertions.assertEquals(1, groups.size());
            Assertions.assertEquals("x", groups.get(0).get(0).getAlias().getTbl());
        }
    }

    @Test
    public void testLargeBodyIsStillReused() throws Exception {
        // cbo_cte_force_reuse_node_count is about user CTE; a large body is where the reuse matters most
        String sql = "select * from (" + AGG_T0 + ") a join (" + AGG_T0 + ") b on a.v1 = b.v1";
        connectContext.getSessionVariable().setCboCTEForceReuseNodeCount(3);
        assertContains(getFragmentPlan(sql), "MultiCastDataSinks");
    }

    @Test
    public void testWrongGroupIsAPlanningError() throws Exception {
        // A group whose members do not plan to the same output is a bug in the collector. We want it to fail the
        // query, not to be planned some other way.
        new MockUp<IdenticalSubqueryCollector>() {
            @Mock
            public List<List<SubqueryRelation>> collect(QueryRelation root, ConnectContext session) {
                List<SubqueryRelation> all = new ArrayList<>();
                new AstTraverser<Void, Void>() {
                    @Override
                    public Void visitSubqueryRelation(SubqueryRelation node, Void context) {
                        all.add(node);
                        return super.visitSubqueryRelation(node, context);
                    }
                }.visit(root);
                return List.of(all);
            }
        };
        String sql = "select * from (select v1 from t0) a join (select v1, v2 from t0) b on a.v1 = b.v1";
        StarRocksPlannerException e = Assertions.assertThrows(StarRocksPlannerException.class,
                () -> getFragmentPlan(sql));
        assertContains(e.getMessage(), "does not match its producer");
    }

    @Test
    public void testAssertRows() throws Exception {
        String body = "select v1, v2 from t0 where v1 = 1";
        String sql = "select * from assert_rows (" + body + ") a join assert_rows (" + body + ") b on a.v1 = b.v1";
        Assertions.assertTrue(collect(sql).isEmpty());
        getFragmentPlan(sql);
    }

    @Test
    public void testLateralSubquery() throws Exception {
        // A LATERAL subquery cannot read the columns on its left, so it is an ordinary FROM subquery here
        Assertions.assertThrows(Exception.class, () -> collect(
                "select * from t0, lateral (select v4 from t1 where t1.v4 = t0.v1) a"));
        Assertions.assertThrows(Exception.class, () -> collect(
                "select * from t0 join lateral (select v4 from t1) a on t0.v1 = a.v4"));

        String body = "select v4 from t1 where v5 > 1";
        String sql = "select * from t0, lateral (" + body + ") a, lateral (" + body + ") b " +
                "where t0.v1 = a.v4 and a.v4 = b.v4";
        Assertions.assertEquals(1, collect(sql).size());
        assertContains(getFragmentPlan(sql), "MultiCastDataSinks");
    }

    @Test
    public void testRecursiveCte() throws Exception {
        connectContext.getSessionVariable().setEnableRecursiveCTE(true);
        String sql = "with recursive r as (select v1 from t0 union all select v1 + 1 from r where v1 < 10) " +
                "select * from (select v1 from r where v1 > 2) a join (select v1 from r where v1 > 2) b " +
                "on a.v1 = b.v1";
        Assertions.assertTrue(collect(sql).isEmpty());
    }

    @Test
    public void testRootCteDefinition() throws Exception {
        // The root CTE is planned before the synthetic anchors exist, so its definition is not searched
        String sql = "with x as (select a.v1 from (" + AGG_T0 + ") a join (" + AGG_T0 + ") b on a.v1 = b.v1) " +
                "select * from x x1 join x x2 on x1.v1 = x2.v1";
        Assertions.assertTrue(collect(sql).isEmpty());
        getFragmentPlan(sql);

        // A CTE that is not the root's is planned below the synthetic anchors
        sql = "select * from (with x as (select a.v1 from (" + AGG_T0 + ") a join (" + AGG_T0 + ") b " +
                "on a.v1 = b.v1) select * from x) z";
        Assertions.assertEquals(1, collect(sql).size());
        assertContains(getFragmentPlan(sql), "MultiCastDataSinks");
    }

    @Test
    public void testCopiesReadRootCte() throws Exception {
        // The producer and the copies consume the same root CTE r
        String sql = "with r as (select v1, v2 from t0 where v3 > 1) " +
                "select * from (select v1, sum(v2) as s from r group by v1) a " +
                "join (select v1, sum(v2) as s from r group by v1) b on a.v1 = b.v1 " +
                "join r on r.v1 = a.v1";
        Assertions.assertEquals(1, collect(sql).size());
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks");
        Assertions.assertEquals(1, count(plan, "TABLE: t0"), plan);
    }

    @Test
    public void testInsertSelect() throws Exception {
        String sql = "insert into t0 select a.v1, a.s, b.s from (" + AGG_T0 + ") a join (" + AGG_T0 + ") b " +
                "on a.v1 = b.v1";
        String plan = InsertPlanTest.getInsertExecPlan(sql);
        assertContains(plan, "MultiCastDataSinks");
        Assertions.assertEquals(1, count(plan, "OlapScanNode"), plan);

        connectContext.getSessionVariable().setCboCTEReuseIdenticalSubqueries(false);
        plan = InsertPlanTest.getInsertExecPlan(sql);
        assertNotContains(plan, "MultiCastDataSinks");
        Assertions.assertEquals(2, count(plan, "OlapScanNode"), plan);
    }

    @Test
    public void testUpdateFrom() throws Exception {
        String body = "select v1 as k, count(*) as c from t0 group by v1";
        String sql = "update tprimary set v2 = a.c from (" + body + ") a, (" + body + ") b " +
                "where tprimary.pk = a.k and a.k = b.k";
        connectContext.setQueryId(UUIDUtil.genUUID());
        String plan = StatementPlanner.plan(UtFrameUtils.parseStmtWithNewParser(sql, connectContext), connectContext)
                .getExplainString(TExplainLevel.NORMAL);
        assertContains(plan, "MultiCastDataSinks");
        Assertions.assertEquals(1, count(plan, "TABLE: t0"), plan);
    }

    @Test
    public void testCreateTableAsSelect() throws Exception {
        String sql = "create table isr_ctas distributed by hash(v1) buckets 1 properties('replication_num' = '1') " +
                "as select a.v1, a.s as s1, b.s as s2 from (" + AGG_T0 + ") a join (" + AGG_T0 + ") b " +
                "on a.v1 = b.v1";
        CreateTableAsSelectStmt ctas =
                (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getMetadataMgr().createTable(connectContext, ctas.getCreateTableStmt());
        try {
            // StmtExecutor plans the insert of a CTAS once the table exists
            String plan = StatementPlanner.plan(ctas.getInsertStmt(), connectContext)
                    .getExplainString(TExplainLevel.NORMAL);
            assertContains(plan, "MultiCastDataSinks");
            Assertions.assertEquals(1, count(plan, "OlapScanNode"), plan);
        } finally {
            starRocksAssert.dropTable("isr_ctas");
        }
    }

    // Planning time of a Tableau shaped query: three copies of a body with three CTE and six tables. We print the
    // numbers rather than assert them, the machines differ too much.
    @Test
    public void testPlanningTime() throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(1.15);
        String body = "with d as (select t0.v1, t0.v2, t1.v5, t2.v8 from t0 join t1 on t0.v1 = t1.v4 " +
                "join t2 on t1.v5 = t2.v7 join t3 on t2.v8 = t3.v10 where t0.v3 > 1), " +
                "e as (select v1, sum(v2) as s2, max(v5) as m5 from d group by v1), " +
                "f as (select v1, count(*) as c from d where v8 > 0 group by v1) " +
                "select e.v1, e.s2, e.m5, f.c from e join f on e.v1 = f.v1 join t4 on e.v1 = t4.v13 " +
                "join t5 on f.v1 = t5.v16";
        String sql = "select x.v1, x.a, y.b, z.c from " +
                "(select v1, sum(s2) as a from (" + body + ") q group by v1) x join " +
                "(select v1, max(m5) as b from (" + body + ") q where c > 0 group by v1) y on x.v1 = y.v1 join " +
                "(select v1, count(*) as c from (" + body + ") q group by v1) z on x.v1 = z.v1";
        Assertions.assertEquals(1, collect(sql).size());
        Assertions.assertEquals(3, collect(sql).get(0).size());

        int warmup = 5;
        int runs = 20;
        double[] millis = new double[2];
        for (int round = 0; round < 2; round++) {
            for (int on = 0; on < 2; on++) {
                connectContext.getSessionVariable().setCboCTEReuseIdenticalSubqueries(on == 1);
                for (int i = 0; i < warmup; i++) {
                    getFragmentPlan(sql);
                }
                long start = System.nanoTime();
                for (int i = 0; i < runs; i++) {
                    getFragmentPlan(sql);
                }
                millis[on] = (System.nanoTime() - start) / 1e6 / runs;
            }
        }
        System.out.printf("identical subquery planning time: off %.2f ms, on %.2f ms%n", millis[0], millis[1]);
    }

    @Test
    public void testLimitWithoutOrderByIsNotForced() throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(SessionVariable.DEFAULT_SESSION_VARIABLE
                .getCboCTERuseRatio());
        connectContext.getSessionVariable().setCboCTEForceReuseLimitWithoutOrderBy(true);
        String body = "select v1, v2 from t0 limit 10";
        // a user CTE with LIMIT and no ORDER BY is always reused
        assertContains(getFragmentPlan("with w as (" + body + ") select * from w a join w b on a.v1 = b.v1"),
                "MultiCastDataSinks");
        // the copies were computed one by one in the query, so the cost decides, and here it inlines
        String sql = "select * from (" + body + ") a join (" + body + ") b on a.v1 = b.v1";
        Assertions.assertEquals(1, collect(sql).size());
        assertNotReused(sql);
    }

    // With cbo_cte_reuse_rate = -1 the optimizer inlines every CTE before the other rewrites, so the plan must be
    // the plan without the reuse, up to column ids.
    @Test
    public void testInlinePlanIsThePlanWithoutReuse() throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(-1);
        List<String> queries = Arrays.asList(
                "select a.v1, a.s, b.s from (" + AGG_T0 + ") a join (" + AGG_T0 + ") b on a.v1 = b.v1 " +
                        "where a.s > 10 and b.s < 5",
                "select a.v1 from (" + AGG_T0 + ") a join (" + AGG_T0 + ") b on a.v1 = b.v1 " +
                        "join (" + AGG_T0 + ") c on b.v1 = c.v1 where a.s > 10",
                "select a.v1, a.s, b.m from (" + CUSTOM_SQL + ") a join (" + CUSTOM_SQL + ") b " +
                        "on a.v1 = b.v1 where a.s > 1");
        for (String sql : queries) {
            Assertions.assertFalse(collect(sql).isEmpty());
            String plan = getFragmentPlan(sql);
            String original = planWithoutReuse(sql);
            Assertions.assertEquals(original.replaceAll("\\d+", "#"), plan.replaceAll("\\d+", "#"));
        }
    }

    @ParameterizedTest
    @ValueSource(doubles = {-1, 0, 1.15})
    public void testAllReuseRates(double rate) throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(rate);
        List<String> queries = Arrays.asList(
                "select a.v1, a.s, b.s from (" + AGG_T0 + ") a join (" + AGG_T0 + ") b on a.v1 = b.v1 " +
                        "where a.s > 10 and b.s < 5",
                "select a.v1, a.s, b.m from (" + CUSTOM_SQL + ") a join (" + CUSTOM_SQL + ") b " +
                        "on a.v1 = b.v1 where a.s > 1",
                "select v1, sum(s) from (select * from (" + AGG_T0 + ") a union all " +
                        "select * from (" + AGG_T0 + ") b) u group by v1");
        for (String sql : queries) {
            String plan = getFragmentPlan(sql);
            if (rate < 0) {
                assertNotContains(plan, "MultiCastDataSinks");
            } else if (rate == 0) {
                assertContains(plan, "MultiCastDataSinks");
            }
        }
    }

    @Test
    public void testNoRuntimeFilterIntoProducer() throws Exception {
        String sql = "select a.v1 from (" + AGG_T0 + ") a join t1 on a.v1 = t1.v4 " +
                "join (" + AGG_T0 + ") b on a.v1 = b.v1 where t1.v5 = 1";
        String plan = getVerboseExplain(sql);
        String[] fragments = plan.split("PLAN FRAGMENT");
        boolean found = false;
        for (String fragment : fragments) {
            if (fragment.contains("MultiCastDataSinks")) {
                found = true;
                assertNotContains(fragment, "probe runtime filters");
            }
        }
        Assertions.assertTrue(found, plan);
    }

    @Test
    public void testUserCteLimitIgnoresSyntheticCte() {
        CTEContext context = new CTEContext();
        context.reset();
        context.setEnableCTE(true);
        context.setInlineCTERatio(1.15);
        context.setMaxCTELimit(2);

        // two user CTE with two consumers each, and one synthetic CTE with a user CTE inside its producer
        context.addCTEProduce(1, false, false);
        context.addCTEProduce(2, false, false);
        context.addCTEProduce(3, true, false);
        context.addCTEProduce(4, false, true);
        for (int cteId = 1; cteId <= 4; cteId++) {
            context.addCTEConsume(cteId);
            context.addCTEConsume(cteId);
        }
        // only two user CTE count, which is not above the limit, so the cost decides for all of them
        for (int cteId = 1; cteId <= 4; cteId++) {
            Assertions.assertFalse(context.needInline(cteId));
            Assertions.assertFalse(context.isForceCTE(cteId));
        }
        Assertions.assertTrue(context.isSyntheticCTE(3));

        // a third user CTE goes above the limit: user CTE with two consumers are inlined, the synthetic is not
        context.addCTEProduce(5, false, false);
        context.addCTEConsume(5);
        context.addCTEConsume(5);
        Assertions.assertTrue(context.needInline(1));
        Assertions.assertFalse(context.needInline(3));
        Assertions.assertFalse(context.isForceCTE(3));
    }
}
