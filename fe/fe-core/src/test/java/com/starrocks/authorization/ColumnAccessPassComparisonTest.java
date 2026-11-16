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

package com.starrocks.authorization;

import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.OptimizerOptions;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.MVTransformerContext;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.TransformerContext;
import com.starrocks.sql.plan.TPCDSPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

// The privilege check reads the columns of a query from a narrow pass of the optimizer
// (QueryOptimizer.optimizeForColumnAccess). This test runs the full rule-based pass that the check used to run on
// the same queries. The access decision depends only on the columns, so the narrow pass must check every column that
// the full pass checked: a missing one would let a query read a column without a check. It may check more, the
// columns that the full pass optimized away (a scan under LIMIT 0, for example). How a column is used goes only to
// the audit, and there the narrow pass leaves out what the optimizer derived (a predicate it inferred, a rewrite of
// EXISTS); those differences are printed.
public class ColumnAccessPassComparisonTest extends TPCDSPlanTestBase {

    // The columns and their uses as the full rule-based optimizer saw them.
    private static Map<TableName, Map<String, EnumSet<ColumnAccessKind>>> fullPass(
            QueryStatement stmt, Map<Table, TableName> tables) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        MVTransformerContext mvTransformerContext = MVTransformerContext.of(connectContext, true);
        TransformerContext transformerContext =
                new TransformerContext(columnRefFactory, connectContext, mvTransformerContext);
        LogicalPlan logicalPlan =
                new RelationTransformer(transformerContext).transformWithSelectLimit(stmt.getQueryRelation());
        OptimizerOptions options = new OptimizerOptions(OptimizerOptions.OptimizerStrategy.RULE_BASED);
        options.disableRule(RuleType.GP_SINGLE_TABLE_MV_REWRITE);
        options.disableRule(RuleType.GP_MULTI_TABLE_MV_REWRITE);
        options.disableRule(RuleType.GP_PRUNE_EMPTY_OPERATOR);
        Optimizer optimizer =
                OptimizerFactory.create(OptimizerFactory.initContext(connectContext, columnRefFactory, options));
        OptExpression plan = optimizer.optimize(logicalPlan.getRoot(), new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()));
        Map<TableName, Map<String, EnumSet<ColumnAccessKind>>> scanColumns = new HashMap<>();
        ColumnPrivilege.ColumnAccessCollector collector =
                new ColumnPrivilege.ColumnAccessCollector(tables, scanColumns);
        plan.getOp().accept(collector, plan, null);
        collector.markRootOutputProjection(logicalPlan.getOutputColumn());
        return scanColumns;
    }

    private static Map<String, String> corpus() throws Exception {
        Map<String, String> queries = new LinkedHashMap<>();
        getSqlMap().forEach((name, sql) -> queries.put("tpcds/" + name, sql));
        for (int i = 1; i <= 22; i++) {
            queries.put("tpch/q" + i, getFileContent("sql/tpchsql/q" + i + ".sql"));
        }
        for (String dir : List.of("sql/optimized-plan", "sql/subquery")) {
            File[] files = new File(Objects.requireNonNull(
                    ColumnAccessPassComparisonTest.class.getClassLoader().getResource(dir)).toURI()).listFiles();
            for (File file : Objects.requireNonNull(files)) {
                List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
                StringBuilder sql = null;
                int index = 0;
                for (String line : lines) {
                    if (line.startsWith("[")) {
                        if (sql != null) {
                            queries.put(dir + "/" + file.getName() + "#" + index++, sql.toString());
                        }
                        sql = line.trim().equals("[sql]") ? new StringBuilder() : null;
                    } else if (sql != null) {
                        sql.append(line).append('\n');
                    }
                }
                if (sql != null) {
                    queries.put(dir + "/" + file.getName() + "#" + index, sql.toString());
                }
            }
        }
        return queries;
    }

    @Test
    public void narrowPassChecksEveryColumnOfTheFullPass() throws Exception {
        List<String> missing = new ArrayList<>();
        List<String> differences = new ArrayList<>();
        int compared = 0;
        for (Map.Entry<String, String> query : corpus().entrySet()) {
            StatementBase stmt;
            try {
                stmt = UtFrameUtils.parseStmtWithNewParser(query.getValue(), connectContext);
            } catch (Exception e) {
                // the corpus has statements that do not analyze in this setup; they are not compared
                continue;
            }
            if (!(stmt instanceof QueryStatement)) {
                continue;
            }
            QueryStatement queryStatement = (QueryStatement) stmt;
            Map<TableName, Table> tableNames = new HashMap<>();
            Map<Table, TableName> tables = new HashMap<>();
            new ColumnPrivilege.TableNameCollector(tableNames, tables).visit(queryStatement);
            Map<TableName, Map<String, EnumSet<ColumnAccessKind>>> full;
            try {
                full = fullPass(queryStatement, tables);
            } catch (Exception e) {
                continue;
            }
            Map<TableName, Map<String, EnumSet<ColumnAccessKind>>> narrow =
                    ColumnPrivilege.collectColumnAccess(connectContext, queryStatement, tables);
            compared++;
            for (Map.Entry<TableName, Map<String, EnumSet<ColumnAccessKind>>> table : full.entrySet()) {
                Map<String, EnumSet<ColumnAccessKind>> narrowColumns = narrow.getOrDefault(table.getKey(), Map.of());
                for (String column : table.getValue().keySet()) {
                    if (!narrowColumns.containsKey(column)) {
                        missing.add(query.getKey() + ": " + table.getKey() + "." + column);
                    }
                }
            }
            if (!full.equals(narrow)) {
                differences.add(query.getKey() + "\n  full:   " + full + "\n  narrow: " + narrow);
            }
        }
        System.out.println("ColumnAccessPassComparisonTest compared " + compared + " queries, "
                + differences.size() + " differ");
        differences.forEach(System.out::println);
        Assertions.assertTrue(compared > 600, "compared " + compared);
        Assertions.assertEquals(List.of(), missing);
    }
}
