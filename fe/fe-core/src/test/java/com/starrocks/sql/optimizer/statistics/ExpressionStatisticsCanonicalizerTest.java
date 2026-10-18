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

import com.starrocks.catalog.Function;
import com.starrocks.catalog.Table;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExpressionStatisticsCanonicalizerTest extends PlanTestBase {
    private static Table table() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "test_all_type");
    }

    private static ExpressionTemplate template(String sql) {
        return ExpressionTemplate.analyze(SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT), table(),
                connectContext);
    }

    // Parse, analysis, canonicalization and printing of the canonical text give the same text again.
    private static String fixedPoint(String sql) {
        String text = template(sql).getText();
        Assertions.assertEquals(text, ExpressionTemplate.parse(text, table(), connectContext).getText(), sql);
        return text;
    }

    @Test
    public void testPrefixSynonymsShareOneText() {
        String text = fixedPoint("left(t1a, 8)");
        Assertions.assertEquals("substr(`t1a`, 1, 8)", text);
        Assertions.assertEquals(text, fixedPoint("strleft(t1a, 8)"));
        Assertions.assertEquals(text, fixedPoint("substr(t1a, 1, 8)"));
        Assertions.assertEquals(text, fixedPoint("substring(t1a, 1, 8)"));
        Assertions.assertEquals(text, fixedPoint("substr(cast(t1a as varchar(65533)), 1, 8)"));
        Assertions.assertNotEquals(text, fixedPoint("left(t1a, 7)"));
        Assertions.assertEquals("lower(`t1a`)", fixedPoint("lower(t1a)"));
    }

    @Test
    public void testFixedPointOfConstantsCastsAndNestedFunctions() {
        String quoted = fixedPoint("concat(t1a, 'it''s \\\\ 100%')");
        Assertions.assertTrue(quoted.contains("'it''s \\\\ 100%'"), quoted);
        Assertions.assertNotEquals(fixedPoint("concat(t1a, '%Y')"), fixedPoint("concat(t1a, '%y')"));
        fixedPoint("upper(substr(lower(t1a), 2, 3))");
        fixedPoint("cast(t1a as int)");
        fixedPoint("cast(t1a as date)");
        fixedPoint("date_trunc('day', id_datetime)");
        fixedPoint("from_unixtime(t1d)");
        fixedPoint("id_decimal * 1.5");
        fixedPoint("round(id_decimal, 1)");
        fixedPoint("t1f * 2.5");
        fixedPoint("if(t1c > 0, t1a, 'x')");
        fixedPoint("id_date >= '2024-01-01'");
        fixedPoint("substr(t1a, -3, 2)");
        fixedPoint("t1c + 100000");
    }

    @Test
    public void testCommutativeArgumentsAndIntegerConstants() {
        Assertions.assertEquals(fixedPoint("t1d + 1"), fixedPoint("1 + t1d"));
        Assertions.assertEquals(fixedPoint("t1c * t1d"), fixedPoint("t1d * t1c"));
        Assertions.assertEquals(fixedPoint("substr(t1a, 1, 1 + 2)"), fixedPoint("left(t1a, 3)"));
    }

    @Test
    public void testRejectedExpressions() {
        Map<String, String> rejected = new java.util.LinkedHashMap<>();
        rejected.put("count(t1a)", "must not contain an aggregate function");
        rejected.put("sum(t1c) over ()", "must not contain a window function");
        rejected.put("rand()", "must be deterministic");
        rejected.put("t1c + rand()", "must be deterministic");
        rejected.put("now()", "must be deterministic");
        rejected.put("1 + 2", "must read at least one column");
        rejected.put("t1c in (select 1)", "must not contain a subquery");
        rejected.put("array_map(x -> x + 1, [t1c])", "must not contain a lambda function");
        rejected.put("[t1c, t1d]", "Can't collect MCV statistics on expression type");
        rejected.put("parse_json(t1a)", "Can't collect MCV statistics on expression type");
        rejected.forEach((sql, message) -> {
            SemanticException e = Assertions.assertThrows(SemanticException.class, () -> template(sql), sql);
            Assertions.assertTrue(e.getMessage().contains(message), sql + ": " + e.getMessage());
        });
    }

    @Test
    public void testTemplateTakesTheRewriteOfTheQuery() {
        // The query writes 5 < c as c > 5 and BETWEEN as two comparisons; the expression of the statistics too.
        Assertions.assertEquals(fixedPoint("if(t1c > 5, 'a', 'b')"), fixedPoint("if(5 < t1c, 'a', 'b')"));
        Assertions.assertEquals(fixedPoint("if(t1c >= 1 and t1c <= 5, 'a', 'b')"),
                fixedPoint("if(t1c between 1 and 5, 'a', 'b')"));
        // A CASE expression has a key, so a query can match it.
        ExpressionTemplate caseWhen = template("case when t1c > 5 then 'a' else 'b' end");
        Assertions.assertEquals(caseWhen.getText(), fixedPoint("CASE WHEN t1c > 5 THEN 'a' ELSE 'b' END"));
        Assertions.assertNotNull(ExpressionStatisticsCanonicalizer.keyOf(caseWhen.getCanonical()));
        Assertions.assertNotEquals(ExpressionStatisticsCanonicalizer.keyOf(caseWhen.getCanonical()),
                ExpressionStatisticsCanonicalizer.keyOf(template("case when t1c > 6 then 'a' else 'b' end")
                        .getCanonical()));
    }

    @Test
    public void testTextAsShownReadsBackUnanalyzed() {
        // DROP finds statistics by the text SHOW prints even when it no longer analyzes; constants keep their casts.
        for (String sql : List.of("id_decimal * 1.5", "t1f * 2.5", "id_date >= '2024-01-01'", "cast(t1a as date)",
                "concat(t1a, 'it''s')")) {
            String text = fixedPoint(sql);
            Assertions.assertEquals(text, ExpressionStatisticsSqlPrinter.printUnanalyzed(
                    SqlParser.parseSqlToExpr(text, SqlModeHelper.MODE_DEFAULT)), sql);
        }
    }

    @Test
    public void testColumnNamedAsExpressionIsRefused() {
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> ExpressionTemplate.checkColumnName("expr:1:lower(`c`)"));
        Assertions.assertTrue(e.getMessage().contains("names statistics on an expression"), e.getMessage());
        // The key of a column name is lower case, so the upper-case spelling clashes as well.
        Assertions.assertThrows(SemanticException.class, () -> ExpressionTemplate.checkColumnName("EXPR:1:lower(`c`)"));
        ExpressionTemplate.checkColumnName("c_expr");
    }

    @Test
    public void testTextIsBuiltWithTheGlobalSessionVariables() {
        // date(x) = constant becomes a range on x by the monotonic rewrite. A session without it would store a text
        // that the collection and the cache load, with the global value, analyze to another text.
        String sql = "if(date(id_datetime) = '2024-01-01', 1, 0)";
        String global = ExpressionTemplate.analyzeStable(SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT),
                table(), connectContext).getText();
        connectContext.getSessionVariable().setEnableMonotonicPredicateRewrite(false);
        try {
            Assertions.assertNotEquals(global, template(sql).getText());
            Assertions.assertEquals(global, ExpressionTemplate.analyzeStable(
                    SqlParser.parseSqlToExpr(sql, SqlModeHelper.MODE_DEFAULT), table(), connectContext).getText());
        } finally {
            connectContext.getSessionVariable().setEnableMonotonicPredicateRewrite(true);
        }
    }

    @Test
    public void testKeyOfQueryExpressionMatchesTemplate() {
        ColumnRefOperator column = new ColumnRefOperator(100, VarcharType.VARCHAR, "t1a", true);
        ScalarOperator statistic = template("left(t1a, 3)").instantiate(Map.of("t1a", column));
        Assertions.assertNotNull(statistic);

        Function left = ExprUtils.getBuiltinFunction("left", new Type[] {VarcharType.VARCHAR, IntegerType.INT},
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        CallOperator query = new CallOperator("left", VarcharType.VARCHAR,
                List.of(column, ConstantOperator.createInt(3)), left);
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(query);
        Assertions.assertEquals(ExpressionStatisticsCanonicalizer.keyOf(statistic),
                ExpressionStatisticsCanonicalizer.keyOf(canonical));
        Assertions.assertNotEquals(ExpressionStatisticsCanonicalizer.keyOf(statistic),
                ExpressionStatisticsCanonicalizer.keyOf(template("left(t1a, 4)").instantiate(Map.of("t1a", column))));
        ColumnRefOperator other = new ColumnRefOperator(101, VarcharType.VARCHAR, "t1a", true);
        Assertions.assertNotEquals(ExpressionStatisticsCanonicalizer.keyOf(statistic),
                ExpressionStatisticsCanonicalizer.keyOf(template("left(t1a, 3)").instantiate(Map.of("t1a", other))));
        Assertions.assertNull(template("left(t1a, 3)").instantiate(Map.of("t1b", other)));

        // The canonical form folds on the FE, so a head value of the column maps to the value of the expression.
        Optional<ConstantOperator> folded = MultiColumnMcvEstimator.evaluate(canonical, column, "10312345");
        Assertions.assertTrue(folded.isPresent());
        Assertions.assertEquals("103", folded.get().getVarchar());
    }

    @Test
    public void testTemplateRecordsSources() {
        ExpressionTemplate template = template("concat(t1a, cast(t1c as varchar))");
        Assertions.assertEquals(2, template.getSources().size());
        Assertions.assertEquals("t1a", template.getSources().get(0).getName());
        Assertions.assertEquals("t1c", template.getSources().get(1).getName());
        Assertions.assertTrue(template.getType().isStringType());
        Assertions.assertNull(template.getSources().get(0).getFieldId());
        Assertions.assertTrue(ExpressionTemplate.sameSource(template.getSources().get(1), table(),
                table().getColumn("t1c")));
        Assertions.assertFalse(ExpressionTemplate.sameSource(template.getSources().get(1), table(),
                table().getColumn("t1d")));
    }

    @Test
    public void testCastThatFormatsTextStays() {
        // 1.5 as a DECIMAL(10, 2) and as a DECIMAL(20, 4) give other text. A query drops the cast of a FLOAT to
        // DOUBLE under a cast to VARCHAR, so the statistics take the text the query computes.
        Assertions.assertEquals(fixedPoint("cast(t1e as varchar)"), fixedPoint("cast(cast(t1e as double) as varchar)"));
        String decimal = fixedPoint("cast(cast(id_decimal as decimal(20, 4)) as varchar)");
        Assertions.assertTrue(decimal.contains("decimal(20, 4)"), decimal);
        // A comparison reads the value, so the cast that keeps every value goes.
        Assertions.assertEquals("`t1c` > 5", fixedPoint("cast(t1c as bigint) > 5"));
    }

    @Test
    public void testExpressionsThatCannotBeMatchedAreRefused() {
        SemanticException column = Assertions.assertThrows(SemanticException.class,
                () -> template("cast(t1c as bigint)"));
        Assertions.assertTrue(column.getMessage().contains("use MCV(t1c)"), column.getMessage());
        SemanticException noKey = Assertions.assertThrows(SemanticException.class,
                () -> template("[t1c, t1d][1]"));
        Assertions.assertTrue(noKey.getMessage().contains("not supported"), noKey.getMessage());
        // The text of cast(t1b AS BIGINT) % 7 is `t1b` % 7, which analyzes over the SMALLINT type.
        SemanticException unstable = Assertions.assertThrows(SemanticException.class,
                () -> ExpressionTemplate.analyzeStable(SqlParser.parseSqlToExpr("cast(t1b as bigint) % 7",
                        SqlModeHelper.MODE_DEFAULT), table(), connectContext));
        Assertions.assertTrue(unstable.getMessage().contains("no stable"), unstable.getMessage());
        Assertions.assertEquals("substr(`t1a`, 1, 8)", ExpressionTemplate.analyzeStable(
                SqlParser.parseSqlToExpr("left(t1a, 8)", SqlModeHelper.MODE_DEFAULT), table(), connectContext).getText());
        // The implicit cast of t1c to BIGINT is not printed and takes no parentheses either.
        Assertions.assertEquals("1 + `t1c`", ExpressionTemplate.analyzeStable(
                SqlParser.parseSqlToExpr("t1c + 1", SqlModeHelper.MODE_DEFAULT), table(), connectContext).getText());
    }

    @Test
    public void testUserFunctionTextAnalyzesWithoutCurrentDatabase() throws Exception {
        boolean enabled = com.starrocks.common.Config.enable_udf;
        com.starrocks.common.Config.enable_udf = true;
        com.starrocks.catalog.Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Function local = new com.starrocks.catalog.ScalarFunction(
                com.starrocks.catalog.FunctionName.createFnName("test.estats_udf"), List.of(VarcharType.VARCHAR),
                VarcharType.VARCHAR, false);
        local.setLocation(new com.starrocks.sql.ast.HdfsURI("hdfs://host/udf.jar"));
        com.starrocks.catalog.FunctionName globalName = com.starrocks.catalog.FunctionName.createFnName("estats_global");
        globalName.setAsGlobalFunction();
        Function global = new com.starrocks.catalog.ScalarFunction(globalName, List.of(VarcharType.VARCHAR),
                VarcharType.VARCHAR, false);
        global.setLocation(new com.starrocks.sql.ast.HdfsURI("hdfs://host/udf.jar"));
        try {
            db.addFunction(local, true, false);
            GlobalStateMgr.getCurrentState().getGlobalFunctionMgr().replayAddFunction(global);
            // The collection and the cache load run without a current database.
            com.starrocks.qe.ConnectContext session = com.starrocks.utframe.UtFrameUtils.createDefaultCtx();
            session.setDatabase("");
            String text = template("test.estats_udf(t1a)").getText();
            Assertions.assertEquals("`test`.estats_udf(`t1a`)", text);
            Assertions.assertEquals(text, ExpressionTemplate.parse(text, table(), session).getText());
            Assertions.assertEquals(text, ExpressionStatisticsSqlPrinter.printUnanalyzed(
                    SqlParser.parseSqlToExpr(text, SqlModeHelper.MODE_DEFAULT)));
            // A global function is found without a database, so its text has none.
            String globalText = template("estats_global(t1a)").getText();
            Assertions.assertEquals("estats_global(`t1a`)", globalText);
            Assertions.assertEquals(globalText, ExpressionTemplate.parse(globalText, table(), session).getText());
        } finally {
            com.starrocks.common.Config.enable_udf = enabled;
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testUserFunctionKeepsItsDatabase() {
        ColumnRefOperator column = new ColumnRefOperator(100, VarcharType.VARCHAR, "t1a", true);
        ScalarOperator first = udf("db1", column);
        Assertions.assertEquals("`db1`.myfn(`t1a`)", ExpressionStatisticsSqlPrinter.print(first));
        Assertions.assertEquals(ExpressionStatisticsCanonicalizer.keyOf(ExpressionStatisticsCanonicalizer.canonicalize(first)),
                ExpressionStatisticsCanonicalizer.keyOf(ExpressionStatisticsCanonicalizer.canonicalize(udf("db1", column))));
        Assertions.assertNotEquals(ExpressionStatisticsCanonicalizer.keyOf(ExpressionStatisticsCanonicalizer.canonicalize(first)),
                ExpressionStatisticsCanonicalizer.keyOf(ExpressionStatisticsCanonicalizer.canonicalize(udf("db2", column))));
    }

    private static ScalarOperator udf(String database, ColumnRefOperator column) {
        Function function = new Function(new com.starrocks.catalog.FunctionName(database, "myfn"),
                new Type[] {VarcharType.VARCHAR}, VarcharType.VARCHAR, false);
        function.setLocation(new com.starrocks.sql.ast.HdfsURI("hdfs://host/udf.jar"));
        return new CallOperator("myfn", VarcharType.VARCHAR, List.of(column), function);
    }
}
