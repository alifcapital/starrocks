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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Config;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.tree.JsonPathExtractRule;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonPathExtractRuleTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE t_json_struct (id INT, s STRUCT<a VARCHAR(1000)>) DUPLICATE KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    private String plan(String sql) throws Exception {
        return getFragmentPlan(sql);
    }

    private String planWithoutRewrite(String sql) throws Exception {
        boolean previous = connectContext.getSessionVariable().isEnableJsonPathRewrite();
        try {
            connectContext.getSessionVariable().setEnableJsonPathRewrite(false);
            return getFragmentPlan(sql);
        } finally {
            connectContext.getSessionVariable().setEnableJsonPathRewrite(previous);
        }
    }

    @Test
    public void testOnePathOfEachConsumerAndSource() throws Exception {
        for (String name : List.of("get_json_string", "get_json_object", "get_json_int", "get_json_double",
                "get_json_bool")) {
            for (String source : List.of("t1a", "parse_json(t1a)", "cast(t1a as json)")) {
                String plan = plan("select " + name + "(" + source + ", '$.a') from test_all_type");
                assertContains(plan, name + "(1: t1a, '$.a')");
                assertNotContains(plan, "parse_json(", "AS JSON", "json_extract_paths");
            }
            // A CHAR document.
            String plan = plan("select " + name + "(parse_json(tt), '$.a') from tall");
            assertContains(plan, name + "(");
            assertNotContains(plan, "parse_json(", "json_extract_paths");
        }
        for (String name : List.of("json_query", "json_exists", "json_length")) {
            for (String source : List.of("parse_json(t1a)", "cast(t1a as json)")) {
                String plan = plan("select " + name + "(" + source + ", '$.a') from test_all_type");
                assertContains(plan, name + "(json_extract_paths(1: t1a, '$.a'), '$.0')");
                assertNotContains(plan, "parse_json(", "AS JSON");
            }
        }
        // -> and json_query over VARCHAR, which casts the text to JSON.
        assertContains(plan("select parse_json(t1a) -> '$.a' from test_all_type"),
                "json_query(json_extract_paths(1: t1a, '$.a'), '$.0')");
        assertContains(plan("select json_query(t1a, '$.a') from test_all_type"),
                "json_query(json_extract_paths(1: t1a, '$.a'), '$.0')");
    }

    @Test
    public void testSeveralPathsInProjection() throws Exception {
        String sql = "select get_json_string(parse_json(t1a), '$.a'), get_json_int(t1a, '$.b'), "
                + "json_query(cast(t1a as json), '$.c'), json_exists(parse_json(t1a), '$.a') from test_all_type";
        String plan = plan(sql);
        assertContains(plan, "common expressions:");
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "json_extract_paths("), plan);
        assertContains(plan, "json_extract_paths(1: t1a, ");
        assertNotContains(plan, "parse_json(", "AS JSON");
        for (String path : List.of("'$.a'", "'$.b'", "'$.c'")) {
            assertContains(plan, path);
        }

        // Two paths of two documents give two calls.
        plan = plan("select get_json_string(t1a, '$.a'), get_json_string(t1a, '$.b'), "
                + "get_json_string(ta, '$.a'), get_json_string(ta, '$.b') from test_all_type join tall on t1d = td");
        assertContains(plan, "json_extract_paths(1: t1a, '$.a', '$.b')", "json_extract_paths(11: ta, '$.a', '$.b')");
    }

    @Test
    public void testSeveralPathsInFilterAndJoin() throws Exception {
        String filter = "select * from (select t1a, t1b from test_all_type limit 10) t "
                + "where get_json_string(parse_json(t1a), '$.a') = 'x' and get_json_string(parse_json(t1a), '$.b') = 'y' "
                + "and get_json_int(t1a, '$.c') > 1";
        String plan = plan(filter);
        assertContains(plan, "json_extract_paths(1: t1a, '$.a', '$.b', '$.c')");
        assertNotContains(plan, "parse_json(");

        // The conjuncts of a JOIN ON predicate after the first one are evaluated on some rows only, and the first
        // one reads no JSON, so the plan stays as it is.
        String join = "select * from test_all_type a join tall b on a.t1d = b.td "
                + "and get_json_string(parse_json(a.t1a), '$.x') > b.ta and get_json_string(parse_json(a.t1a), '$.y') < b.ta";
        plan = plan(join);
        assertNotContains(plan, "json_extract_paths");
        Assertions.assertEquals(planWithoutRewrite(join), plan);
    }

    @Test
    public void testUnchangedExtractions() throws Exception {
        List<String> queries = List.of(
                // the path is not a constant, or NULL
                "select get_json_string(parse_json(t1a), t1a) from test_all_type",
                "select get_json_string(parse_json(t1a), cast(null as varchar)) from test_all_type",
                // the path is not simple or does not parse
                "select get_json_string(parse_json(t1a), '$.a[*]') from test_all_type",
                "select get_json_string(parse_json(t1a), '$.a[1:2]') from test_all_type",
                "select get_json_string(parse_json(t1a), '$.a.$.b') from test_all_type",
                "select get_json_string(parse_json(t1a), '$.a[') from test_all_type",
                // the document is a JSON column, a constant or not deterministic
                "select get_json_string(v_json, '$.a'), get_json_string(v_json, '$.b') from tjson",
                "select get_json_string(parse_json('{\"a\": 1}'), '$.a') from test_all_type",
                "select get_json_string(parse_json(concat(t1a, rand())), '$.a'), "
                        + "get_json_string(parse_json(concat(t1a, rand())), '$.b') from test_all_type");
        for (String sql : queries) {
            String plan = plan(sql);
            assertNotContains(plan, "json_extract_paths");
            Assertions.assertEquals(planWithoutRewrite(sql), plan, sql);
        }
    }

    @Test
    public void testFailureLeavesPlan() throws Exception {
        String sql = "select get_json_string(parse_json(t1a), '$.a'), get_json_string(parse_json(t1a), '$.b') "
                + "from test_all_type where get_json_int(parse_json(t1a), '$.c') > 1";
        try {
            JsonPathExtractRule.failForTest = true;
            String plan = plan(sql);
            assertNotContains(plan, "json_extract_paths");
            Assertions.assertEquals(planWithoutRewrite(sql), plan);
        } finally {
            JsonPathExtractRule.failForTest = false;
        }
        assertContains(plan(sql), "json_extract_paths");
    }

    private static Function builtin(String name, Type... argumentTypes) {
        return ExprUtils.getBuiltinFunction(name, argumentTypes, Function.CompareMode.IS_IDENTICAL);
    }

    private static CallOperator call(Function fn, ScalarOperator... arguments) {
        return new CallOperator(fn.functionName(), fn.getReturnType(), List.of(arguments), fn);
    }

    private static CallOperator getJsonString(ScalarOperator document, String path) {
        return call(builtin(FunctionSet.GET_JSON_STRING, VarcharType.VARCHAR, VarcharType.VARCHAR), document,
                ConstantOperator.createVarchar(path));
    }

    @Test
    public void testPlaceRewrite() {
        ColumnRefOperator text = new ColumnRefOperator(1, VarcharType.VARCHAR, "v", true);
        ScalarOperator a = getJsonString(text, "$.a");
        ScalarOperator b = getJsonString(text, "$.b");
        List<ScalarOperator> rewritten = JsonPathExtractRule.rewritePlace(List.of(a, b), false);
        Assertions.assertNotNull(rewritten);
        Assertions.assertEquals("get_json_string(json_extract_paths(1: v, $.a, $.b), $.0)",
                rewritten.get(0).toString());
        Assertions.assertEquals("get_json_string(json_extract_paths(1: v, $.a, $.b), $.1)",
                rewritten.get(1).toString());

        // An expression inside a dictionary mapping is evaluated over dictionary codes; it stays.
        ColumnRefOperator codes = new ColumnRefOperator(2, IntegerType.INT, "codes", true);
        ScalarOperator mapped = new DictMappingOperator(codes, getJsonString(text, "$.c"), VarcharType.VARCHAR);
        rewritten = JsonPathExtractRule.rewritePlace(List.of(mapped, getJsonString(text, "$.d")), false);
        Assertions.assertNull(rewritten);

        // An expression deeper than the limit of the scalar rewrites stays.
        int depth = Config.max_scalar_operator_optimize_depth;
        try {
            Config.max_scalar_operator_optimize_depth = 0;
            Assertions.assertNull(JsonPathExtractRule.rewritePlace(List.of(a, b), false));
        } finally {
            Config.max_scalar_operator_optimize_depth = depth;
        }

        // get_json_int over JSON returning INT has no overload over VARCHAR with the same result type.
        Function intOverJson = GlobalStateMgr.getCurrentState().getBuiltinFunctions().stream()
                .filter(fn -> fn.functionName().equals(FunctionSet.GET_JSON_INT)
                        && fn.getReturnType().equals(IntegerType.INT)
                        && fn.getArgs()[0].equals(JsonType.JSON))
                .findFirst().orElseThrow();
        CallOperator parse = call(builtin(FunctionSet.PARSE_JSON, VarcharType.VARCHAR), text);
        CallOperator intCall = call(intOverJson, parse, ConstantOperator.createVarchar("$.a"));
        Assertions.assertNull(JsonPathExtractRule.rewritePlace(List.of(intCall), false));
    }

    @Test
    public void testSimplePath() {
        for (String path : List.of("$", "$.a", "a", "a.b", "$.a.b[1][2]", "$[0]", "$[1].a", "$.\"a.b\"", "$.",
                "$.\"\"", "$.статус", "$.\"sw5.chain.id\"", "$.a[2147483647]", ".a")) {
            Assertions.assertTrue(JsonPathExtractRule.isSimplePath(path), path);
        }
        for (String path : List.of("", "$.a[*]", "$.a[1:2]", "$.a.$.b", "$.a[", "$.a[2147483648]", "$.a\\.b",
                "$.\"a", "$.a[x]", "a.$")) {
            Assertions.assertFalse(JsonPathExtractRule.isSimplePath(path), path);
        }
    }

    @Test
    public void testLazyBranchesStay() throws Exception {
        // Extractions in branches that BE evaluates only on some rows are not grouped, so no shared call is moved
        // out of them.
        for (String sql : List.of(
                "select case when t1b = 1 then get_json_string(t1a, '$.a') end, "
                        + "case when t1b = 1 then get_json_string(t1a, '$.b') end from test_all_type",
                "select case when t1b = 1 then get_json_string(t1a, '$.a') "
                        + "when t1b = 2 then get_json_string(t1a, '$.b') end from test_all_type",
                "select if(t1b = 1, get_json_string(t1a, '$.a'), get_json_string(t1a, '$.b')) from test_all_type",
                "select ifnull(t1a, get_json_string(parse_json(t1a), '$.a')), "
                        + "coalesce(t1a, get_json_string(parse_json(t1a), '$.b')) from test_all_type")) {
            String plan = plan(sql);
            assertNotContains(plan, "json_extract_paths");
            Assertions.assertEquals(planWithoutRewrite(sql), plan, sql);
        }

        // A text parsed on every row anyway: the extractions in the branches join its group and read the shared
        // result, so the branches parse nothing.
        String plan = plan("select get_json_string(t1a, '$.a'), get_json_string(t1a, '$.c'), "
                + "if(t1b = 1, get_json_string(t1a, '$.b'), null), "
                + "coalesce(get_json_string(t1a, '$.d'), get_json_string(t1a, '$.e')) from test_all_type");
        assertExtractedPaths(plan, List.of("'$.a'", "'$.c'", "'$.d'", "'$.b'", "'$.e'"), List.of());
        assertNotContains(plan, "get_json_string(1: t1a");

        // One extraction on every row and one in a branch over the same path and text share one call.
        String sql = "select json_query(parse_json(t1a), '$.a'), "
                + "case when t1b = 1 then get_json_string(parse_json(t1a), '$.a') end from test_all_type";
        plan = plan(sql);
        assertExtractedPaths(plan, List.of("'$.a'"), List.of());
        assertNotContains(plan, "parse_json(");
        assertParsesAtMost(sql);

        // The first WHEN of a CASE is evaluated on every row.
        plan = plan("select case when get_json_string(t1a, '$.a') = 'x' then 1 end, get_json_string(t1a, '$.b') "
                + "from test_all_type");
        assertExtractedPaths(plan, List.of("'$.a'", "'$.b'"), List.of());
    }

    // The plan has one json_extract_paths(1: t1a, ...) call whose arguments are exactly the given paths.
    private static void assertExtractedPaths(String plan, List<String> paths, List<String> others) {
        String prefix = "json_extract_paths(1: t1a, ";
        int start = plan.indexOf(prefix);
        Assertions.assertTrue(start >= 0, plan);
        Assertions.assertEquals(start, plan.lastIndexOf(prefix), plan);
        String arguments = plan.substring(start + prefix.length(), plan.indexOf(')', start));
        Assertions.assertEquals(paths.size(), arguments.split(", ").length, plan);
        for (String path : paths) {
            Assertions.assertTrue(arguments.contains(path), plan);
        }
        for (String path : others) {
            Assertions.assertFalse(arguments.contains(path), plan);
        }
    }

    @Test
    public void testParsedTextUsedAsValueKeepsPlan() throws Exception {
        // CSE already shares one parse of the text between the JSON value and its extractions.
        for (String sql : List.of(
                "select parse_json(t1a), get_json_string(parse_json(t1a), '$.a'), "
                        + "get_json_string(parse_json(t1a), '$.b') from test_all_type",
                "select cast(t1a as json), get_json_string(cast(t1a as json), '$.a'), get_json_string(t1a, '$.b') "
                        + "from test_all_type",
                "select json_keys(parse_json(t1a)), json_query(parse_json(t1a), '$.a') from test_all_type")) {
            String plan = plan(sql);
            assertNotContains(plan, "json_extract_paths");
            Assertions.assertEquals(planWithoutRewrite(sql), plan, sql);
        }
    }

    @Test
    public void testStrictModeSources() throws Exception {
        long previous = connectContext.getSessionVariable().getSqlMode();
        try {
            connectContext.getSessionVariable().setSqlMode(previous | SqlModeHelper.MODE_ALLOW_THROW_EXCEPTION);
            String plan = plan("select get_json_string(cast(t1a as json), '$.a'), "
                    + "get_json_string(cast(t1a as json), '$.b') from test_all_type");
            assertNotContains(plan, "json_extract_paths");
            assertContains(plan, "AS JSON");

            plan = plan("select json_query(t1a, '$.a') from test_all_type");
            assertNotContains(plan, "json_extract_paths");

            plan = plan("select get_json_string(parse_json(t1a), '$.a'), get_json_string(t1a, '$.b') "
                    + "from test_all_type");
            assertContains(plan, "json_extract_paths(1: t1a, '$.a', '$.b')");
            assertNotContains(plan, "parse_json(");
        } finally {
            connectContext.getSessionVariable().setSqlMode(previous);
        }
    }

    // The calls that parse the text t1a in the plan with the rewrite are not more than in the plan without it.
    private void assertParsesAtMost(String sql) throws Exception {
        Pattern parse = Pattern.compile("(json_extract_paths|parse_json|get_json_\\w+|json_query)\\((CAST\\()?\\d+: t1a\\b");
        int with = countMatches(parse, plan(sql));
        int without = countMatches(parse, planWithoutRewrite(sql));
        Assertions.assertTrue(with <= without, sql + ": " + with + " > " + without);
    }

    private static int countMatches(Pattern pattern, String text) {
        Matcher matcher = pattern.matcher(text);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    @Test
    public void testShortCircuitOperandsStay() throws Exception {
        // BE skips the right operand of AND for a chunk whose left operand is all false, and of OR for one whose
        // left operand is all true. When every extraction of a text is behind such an operand, the plan stays.
        long previous = connectContext.getSessionVariable().getSqlMode();
        try {
            connectContext.getSessionVariable().setSqlMode(previous | SqlModeHelper.MODE_ALLOW_THROW_EXCEPTION);
            for (String sql : List.of(
                    "select (t1b = 1 and get_json_string(t1a, '$.a') = 'x'), "
                            + "(t1b = 1 and get_json_string(t1a, '$.b') = 'y') from test_all_type",
                    "select t1b = 1 or get_json_string(t1a, '$.a') = 'x' or get_json_string(t1a, '$.b') = 'y' "
                            + "from test_all_type",
                    "select case when t1b = 1 and get_json_string(t1a, '$.a') = 'x' "
                            + "and get_json_string(t1a, '$.b') = 'y' then 1 end from test_all_type",
                    "select * from (select t1a, t1b from test_all_type group by t1a, t1b) t where t1b = 1 "
                            + "or (get_json_string(t1a, '$.a') = 'x' and get_json_string(t1a, '$.b') = 'y')")) {
                String plan = plan(sql);
                assertNotContains(plan, "json_extract_paths");
                Assertions.assertEquals(planWithoutRewrite(sql), plan, sql);
            }
        } finally {
            connectContext.getSessionVariable().setSqlMode(previous);
        }
    }

    @Test
    public void testOlapScanConjunctsInStorage() throws Exception {
        // BE evaluates a scan conjunct over one column, and an AND or OR of such conjuncts, in the storage layer,
        // where each call computes its own json_extract_paths(). There every extraction is rewritten alone.
        String storage = "select t1b from test_all_type where get_json_string(t1a, '$.a') = 'x' "
                + "and get_json_string(parse_json(t1a), '$.b') = 'y' and get_json_int(t1a, '$.c') > 1 "
                + "and (get_json_string(t1a, '$.d') = 'z' or t1b = 1) and json_length(parse_json(t1a), '$.e') > 0";
        String plan = plan(storage);
        assertContains(plan, "get_json_string(1: t1a, '$.a') = 'x'", "get_json_string(1: t1a, '$.b') = 'y'",
                "get_json_int(1: t1a, '$.c') > 1", "get_json_string(1: t1a, '$.d') = 'z'",
                "json_length(json_extract_paths(1: t1a, '$.e'), '$.0') > 0");
        assertNotContains(plan, "parse_json(");
        assertScanExtractedPaths(plan, 1, List.of("'$.e'"));
        assertParsesAtMost(storage);

        // A conjunct over several columns is evaluated by a FILTER above the scan, which shares one call between
        // the conjuncts.
        String mixed = "select t1b from test_all_type where get_json_string(t1a, '$.a') = cast(t1b as varchar) "
                + "and get_json_int(t1a, '$.b') > t1c and get_json_string(t1a, '$.c') = 'z'";
        plan = plan(mixed);
        assertExtractedPaths(plan, List.of("'$.a'", "'$.b'"), List.of("'$.c'"));
        assertContains(plan, "get_json_string(1: t1a, '$.c') = 'z'");
        assertParsesAtMost(mixed);

        long previousSqlMode = connectContext.getSessionVariable().getSqlMode();
        boolean previousScanReuse = connectContext.getSessionVariable().isEnableScanPredicateExprReuse();
        boolean previousColumnExpr = connectContext.getSessionVariable().isEnableColumnExprPredicate();
        try {
            // Kept in the scan, the conjuncts over several columns stay on the chunk and share one call through
            // the conjunct cache of BE; the conjunct in the storage layer does not join their group.
            connectContext.getSessionVariable().setEnableScanPredicateExprReuse(false);
            plan = plan(mixed);
            assertScanExtractedPaths(plan, 2, List.of("'$.a'", "'$.b'"));
            assertContains(plan, "get_json_string(1: t1a, '$.c') = 'z'");
            connectContext.getSessionVariable().setEnableScanPredicateExprReuse(previousScanReuse);

            // Without column expression predicates, or over a string column when an expression may throw, BE
            // evaluates every conjunct on the chunk.
            List<String> paths = List.of("'$.a'", "'$.b'", "'$.c'", "'$.d'", "'$.e'");
            connectContext.getSessionVariable().setEnableColumnExprPredicate(false);
            assertScanExtractedPaths(plan(storage), 5, paths);
            connectContext.getSessionVariable().setEnableColumnExprPredicate(previousColumnExpr);
            connectContext.getSessionVariable().setSqlMode(previousSqlMode | SqlModeHelper.MODE_ALLOW_THROW_EXCEPTION);
            assertScanExtractedPaths(plan(storage), 5, paths);

            // A STRUCT column goes to the storage layer also when an expression may throw.
            String struct = "select id from t_json_struct where get_json_string(s.a, '$.x') = '1' "
                    + "and get_json_string(s.a, '$.y') = '2'";
            plan = plan(struct);
            assertNotContains(plan, "json_extract_paths");
            assertContains(plan, "'$.x'", "'$.y'");
        } finally {
            connectContext.getSessionVariable().setEnableScanPredicateExprReuse(previousScanReuse);
            connectContext.getSessionVariable().setEnableColumnExprPredicate(previousColumnExpr);
            connectContext.getSessionVariable().setSqlMode(previousSqlMode);
        }
    }

    // A scan has no common sub-expressions for its conjuncts, so each conjunct shows its own call. The plan has
    // `calls` json_extract_paths(1: t1a, ...) calls whose arguments are all exactly the given paths.
    private static void assertScanExtractedPaths(String plan, int calls, List<String> paths) {
        Matcher matcher = Pattern.compile("json_extract_paths\\(1: t1a, ([^)]*)\\)").matcher(plan);
        int found = 0;
        while (matcher.find()) {
            found++;
            List<String> arguments = List.of(matcher.group(1).split(", "));
            Assertions.assertEquals(paths.size(), arguments.size(), plan);
            Assertions.assertTrue(arguments.containsAll(paths), plan);
        }
        Assertions.assertEquals(calls, found, plan);
    }

    @Test
    public void testParsesNeverGrow() throws Exception {
        for (String sql : List.of(
                "select get_json_string(t1a, '$.a'), if(t1b = 1, get_json_string(t1a, '$.b'), null) from test_all_type",
                "select get_json_string(parse_json(t1a), '$.a'), "
                        + "case when t1b = 1 then get_json_string(parse_json(t1a), '$.b') end from test_all_type",
                "select t1b = 1 and get_json_string(t1a, '$.a') = 'x', get_json_string(t1a, '$.b') from test_all_type",
                "select get_json_string(t1a, '$.a'), get_json_string(t1a, '$.b'), get_json_int(t1a, '$.c') "
                        + "from test_all_type where get_json_string(t1a, '$.d') = 'x'")) {
            assertParsesAtMost(sql);
        }
    }
}
