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

import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A view definition keeps its parse tree, and every reference builds a new AST from it. These tests check
 * that what the planner produces from such an AST is the same as from a fresh parse.
 */
public class ViewDefinitionParseCacheTest extends TPCDSPlanTestBase {
    private long savedMaxTokens;

    @BeforeEach
    public void setUp() {
        savedMaxTokens = Config.view_definition_parse_cache_max_tokens;
        SqlParser.clearViewDefinitionCache();
    }

    @AfterEach
    public void tearDown() {
        Config.view_definition_parse_cache_max_tokens = savedMaxTokens;
        SqlParser.clearViewDefinitionCache();
    }

    private static View getView(String name) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", name);
        return (View) table;
    }

    // Explain, plan fragments sent to the BEs and the descriptor table, from one planning.
    private String planAll(String sql) throws Exception {
        Pair<String, ExecPlan> plan = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        StringBuilder sb = new StringBuilder(plan.second.getExplainString(TExplainLevel.VERBOSE));
        for (PlanFragment fragment : plan.second.getFragments()) {
            sb.append('\n').append(fragment.toThrift());
        }
        sb.append('\n').append(plan.second.getDescTbl().toThrift());
        return sb.toString();
    }

    private String planWithoutCache(String sql) throws Exception {
        long maxTokens = Config.view_definition_parse_cache_max_tokens;
        Config.view_definition_parse_cache_max_tokens = 0;
        try {
            return planAll(sql);
        } finally {
            Config.view_definition_parse_cache_max_tokens = maxTokens;
        }
    }

    private void assertSamePlans(String sql, List<View> views) throws Exception {
        String expected = planWithoutCache(sql);
        for (View view : views) {
            Assertions.assertFalse(SqlParser.isViewDefinitionCached(view.getInlineViewDef(), view.getSqlMode()));
        }
        // the first planning fills the cache, the second one reads from it
        Assertions.assertEquals(expected, planAll(sql), sql);
        for (View view : views) {
            Assertions.assertTrue(SqlParser.isViewDefinitionCached(view.getInlineViewDef(), view.getSqlMode()));
        }
        Assertions.assertEquals(expected, planAll(sql), sql);
    }

    @Test
    public void testTpcdsViews() throws Exception {
        List<String> names = new ArrayList<>();
        for (Map.Entry<String, String> e : getSqlMap().entrySet()) {
            String name = "pc_" + e.getKey().replace('-', '_');
            try {
                starRocksAssert.withView("create view " + name + " as " + e.getValue());
                names.add(name);
            } catch (Exception ex) {
                // a few queries have duplicate output column names and cannot be views
            }
        }
        Assertions.assertTrue(names.size() >= 90, "views created: " + names.size());
        try {
            for (String name : names) {
                View view = getView(name);
                // The same view twice in one statement, and once more in the next statement.
                assertSamePlans("select * from " + name + " union all select * from " + name, List.of(view));
                Assertions.assertEquals(planWithoutCache("select * from " + name),
                        planAll("select * from " + name), name);

                // Unanalyzed ASTs are equal to a fresh parse and are never shared between calls.
                String fresh = AstToSQLBuilder.toSQL(
                        SqlParser.parse(view.getInlineViewDef(), view.getSqlMode()).get(0));
                QueryStatement first = view.getQueryStatement();
                QueryStatement second = view.getQueryStatement();
                Assertions.assertNotSame(first, second);
                Assertions.assertNotSame(first.getQueryRelation(), second.getQueryRelation());
                Assertions.assertEquals(fresh, AstToSQLBuilder.toSQL(first), name);
                Assertions.assertEquals(fresh, AstToSQLBuilder.toSQL(second), name);
            }
        } finally {
            for (String name : names) {
                starRocksAssert.dropView(name);
            }
        }
    }

    @Test
    public void testViewReferencedManyTimesAndAltered() throws Exception {
        starRocksAssert.withView("create view pc_base as select v1, v2, v3 + 1 as v4, " +
                "case when v1 in (1, 2, 3) then 'a' else concat('b', v2) end as v5 " +
                "from t0 where v2 > 10 and v3 is not null");
        starRocksAssert.withView("create view pc_nested as select /*+ SET_VAR(pipeline_dop = 2) */ " +
                "a.v1, b.v4, count(*) as c from pc_base a join pc_base b on a.v1 = b.v2 " +
                "where a.v1 in (select v1 from pc_base where v4 > 3) group by a.v1, b.v4");
        try {
            View base = getView("pc_base");
            View nested = getView("pc_nested");
            List<String> queries = List.of(
                    "select * from pc_base",
                    "select * from pc_base x join pc_base y on x.v1 = y.v1 join pc_base z on y.v2 = z.v2",
                    "with w as (select * from pc_base) select * from w union all select * from pc_base",
                    "select v1 from pc_base where v2 in (select v2 from pc_base where v4 = 3)",
                    "select * from pc_nested n join pc_base b on n.v1 = b.v1",
                    "select (select max(c) from pc_nested), v5 from pc_base");
            for (String q : queries) {
                SqlParser.clearViewDefinitionCache();
                List<View> views = q.contains("pc_nested") ? List.of(base, nested) : List.of(base);
                assertSamePlans(q, views);
            }

            String oldDef = base.getInlineViewDef();
            String alter = "alter view pc_base as select v1, v2 + 100 as v2, v3 as v4, " +
                    "cast(v1 as varchar) as v5 from t0 where v1 < 5";
            AlterViewStmt alterViewStmt = (AlterViewStmt) UtFrameUtils.parseStmtWithNewParser(alter,
                    starRocksAssert.getCtx());
            GlobalStateMgr.getCurrentState().getLocalMetastore().alterView(connectContext, alterViewStmt);
            base = getView("pc_base");
            Assertions.assertNotEquals(oldDef, base.getInlineViewDef());
            Assertions.assertFalse(SqlParser.isViewDefinitionCached(base.getInlineViewDef(), base.getSqlMode()));

            for (String q : queries) {
                String expected = planWithoutCache(q);
                Assertions.assertEquals(expected, planAll(q), q);
                Assertions.assertEquals(expected, planAll(q), q);
            }
            Assertions.assertTrue(SqlParser.isViewDefinitionCached(base.getInlineViewDef(), base.getSqlMode()));
            String plan = getFragmentPlan("select * from pc_base");
            Assertions.assertTrue(plan.contains("1: v1 < 5"), plan);
            Assertions.assertTrue(plan.contains("+ 100"), plan);
        } finally {
            starRocksAssert.dropView("pc_nested");
            starRocksAssert.dropView("pc_base");
        }
    }

    private static Expr firstOutput(StatementBase stmt) {
        return ((SelectRelation) ((QueryStatement) stmt).getQueryRelation()).getSelectList().getItems().get(0).getExpr();
    }

    @Test
    public void testSqlModeIsPartOfTheKey() {
        String sql = "select 'a' || 'b'";
        long concatMode = SqlModeHelper.MODE_PIPES_AS_CONCAT;
        long defaultMode = SqlModeHelper.MODE_DEFAULT;
        Assertions.assertInstanceOf(FunctionCallExpr.class,
                firstOutput(SqlParser.parseViewDefinition(sql, concatMode).get(0)));
        Assertions.assertInstanceOf(CompoundPredicate.class,
                firstOutput(SqlParser.parseViewDefinition(sql, defaultMode).get(0)));
        Assertions.assertInstanceOf(FunctionCallExpr.class,
                firstOutput(SqlParser.parseViewDefinition(sql, concatMode).get(0)));
        Assertions.assertInstanceOf(CompoundPredicate.class,
                firstOutput(SqlParser.parseViewDefinition(sql, defaultMode).get(0)));
    }

    @Test
    public void testExprChildrenLimitIsPartOfTheKey() {
        String sql = "select 1 from t0 where v1 in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)";
        long sqlMode = SqlModeHelper.MODE_DEFAULT;
        int savedLimit = Config.expr_children_limit;
        try {
            SqlParser.parseViewDefinition(sql, sqlMode);
            Assertions.assertTrue(SqlParser.isViewDefinitionCached(sql, sqlMode));
            Config.expr_children_limit = 5;
            Exception fresh = Assertions.assertThrows(Exception.class, () -> SqlParser.parse(sql, sqlMode));
            Exception cached = Assertions.assertThrows(Exception.class,
                    () -> SqlParser.parseViewDefinition(sql, sqlMode));
            Assertions.assertEquals(fresh.getClass(), cached.getClass());
            Assertions.assertEquals(fresh.getMessage(), cached.getMessage());
        } finally {
            Config.expr_children_limit = savedLimit;
        }
    }

    @Test
    public void testParseErrorIsNotCached() {
        String sql = "select from where";
        long sqlMode = SqlModeHelper.MODE_DEFAULT;
        Exception fresh = Assertions.assertThrows(Exception.class, () -> SqlParser.parse(sql, sqlMode));
        for (int i = 0; i < 2; i++) {
            Exception cached = Assertions.assertThrows(Exception.class,
                    () -> SqlParser.parseViewDefinition(sql, sqlMode));
            Assertions.assertEquals(fresh.getClass(), cached.getClass());
            Assertions.assertEquals(fresh.getMessage(), cached.getMessage());
            Assertions.assertFalse(SqlParser.isViewDefinitionCached(sql, sqlMode));
        }
    }

    @Test
    public void testConcurrentBuildsFromOneTree() throws Exception {
        List<String> defs = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        long sqlMode = SqlModeHelper.MODE_DEFAULT;
        for (String sql : getSqlMap().values()) {
            defs.add(sql);
            expected.add(AstToSQLBuilder.toSQL(SqlParser.parse(sql, sqlMode).get(0)));
        }
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                futures.add(pool.submit(() -> {
                    for (int round = 0; round < 5; round++) {
                        for (int i = 0; i < defs.size(); i++) {
                            String actual = AstToSQLBuilder.toSQL(
                                    SqlParser.parseViewDefinition(defs.get(i), sqlMode).get(0));
                            Assertions.assertEquals(expected.get(i), actual);
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
