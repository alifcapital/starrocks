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

package com.starrocks.sql.parser;

import com.starrocks.common.Config;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.junit.jupiter.api.Test;

import java.util.IdentityHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionDescriptorStatementTest {
    private static final PredictionMode[] MODES = {PredictionMode.SLL, PredictionMode.LL};

    private StatementBase statement(String sql, PredictionMode mode) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(SqlTextStream.create(sql)));
        lexer.setSqlMode(SqlModeHelper.MODE_DEFAULT);
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ErrorHandler());
        StarRocksParser parser = new StarRocksParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorHandler());
        parser.setErrorHandler(new StarRocksDefaultErrorStrategy());
        parser.getInterpreter().setPredictionMode(mode);
        PostProcessListener listener = new PostProcessListener(100_000, 100_000);
        parser.addParseListener(listener);
        var tree = parser.sqlStatements();
        listener.validateTupleContexts();
        var context = tree.singleStatement(0);
        AstBuilder builder = new AstBuilder(SqlModeHelper.MODE_DEFAULT, false, new IdentityHashMap<>());
        builder.initializeParameterContext(LexicalParameterContext.forRule(parser, context));
        return (StatementBase) builder.visitSingleStatement(context);
    }

    private String sql(String clause, boolean ctas) {
        return "CREATE TABLE target " + clause + (ctas ? " AS SELECT 1 AS k, 2 AS v" : " LIKE source");
    }

    private PartitionDesc partition(String clause, boolean ctas, PredictionMode mode) {
        StatementBase value = statement(sql(clause, ctas), mode);
        return ctas ? ((CreateTableAsSelectStmt) value).getCreateTableStmt().getPartitionDesc() :
                ((CreateTableLikeStmt) value).getPartitionDesc();
    }

    @Test
    void retainsSingleListNullValuesAndClausePositions() {
        String clause = "PARTITION BY LIST(k) (PARTITION p1 VALUES IN ('a',NULL,'b'))";
        for (PredictionMode mode : MODES) {
            for (boolean ctas : new boolean[] {false, true}) {
                ListPartitionDesc list = assertInstanceOf(ListPartitionDesc.class, partition(clause, ctas, mode));
                assertEquals(List.of("k"), list.getPartitionColNames());
                assertFalse(list.isAutoPartitionTable());
                assertEquals(1, list.getSingleListPartitionDescs().size());
                SingleItemListPartitionDesc single = list.getSingleListPartitionDescs().get(0);
                assertEquals("p1", single.getPartitionName());
                assertEquals(List.of("a", PartitionValue.STARROCKS_DEFAULT_PARTITION_VALUE, "b"), single.getValues());
                assertEquals(20, list.getPos().getCol());
                assertEquals(20 + clause.length() - 1, list.getPos().getEndCol());
                assertEquals(42, single.getPos().getCol());
                assertEquals(20 + clause.length() - 2, single.getPos().getEndCol());
            }
        }
    }

    @Test
    void retainsTupleValuesAndExplicitEmptyList() {
        for (PredictionMode mode : MODES) {
            for (boolean ctas : new boolean[] {false, true}) {
                ListPartitionDesc list = (ListPartitionDesc) partition(
                        "PARTITION BY LIST(k,v) (PARTITION p1 VALUES IN (('a',NULL),('c','d')))", ctas, mode);
                assertEquals(List.of("k", "v"), list.getPartitionColNames());
                assertEquals(List.of(List.of("a", PartitionValue.STARROCKS_DEFAULT_PARTITION_VALUE),
                        List.of("c", "d")), list.getMultiListPartitionDescs().get(0).getMultiValues());
                ListPartitionDesc empty = (ListPartitionDesc) partition("PARTITION BY LIST(k) ()", ctas, mode);
                assertTrue(empty.getPartitionDescs().isEmpty());
                assertFalse(empty.isAutoPartitionTable());
            }
        }
    }

    @Test
    void retainsRangePrimaryExpressionAndUpperBound() {
        for (PredictionMode mode : MODES) {
            for (boolean ctas : new boolean[] {false, true}) {
                ExpressionPartitionDesc expression = assertInstanceOf(ExpressionPartitionDesc.class, partition(
                        "PARTITION BY RANGE(k+1) (PARTITION p1 VALUES LESS THAN ('10'))", ctas, mode));
                assertInstanceOf(ArithmeticExpr.class, expression.getExpr());
                RangePartitionDesc range = expression.getRangePartitionDesc();
                assertEquals(1, range.getSingleRangePartitionDescs().size());
                assertEquals("p1", range.getSingleRangePartitionDescs().get(0).getPartitionName());
                assertEquals("10", range.getSingleRangePartitionDescs().get(0)
                        .getPartitionKeyDesc().getUpperValues().get(0).getStringValue());
                assertTrue(range.getPos().isZero());
            }
        }
    }

    @Test
    void preservesExistingRangeAndFunctionBranches() {
        for (PredictionMode mode : MODES) {
            for (boolean ctas : new boolean[] {false, true}) {
                RangePartitionDesc range = (RangePartitionDesc) partition(
                        "PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN ('10'))", ctas, mode);
                assertEquals(List.of("k"), range.getPartitionColNames());
                assertEquals(1, range.getSingleRangePartitionDescs().size());
                assertEquals(20, range.getPos().getCol());
                ExpressionPartitionDesc function = (ExpressionPartitionDesc) partition(
                        "PARTITION BY date_trunc('day',k)", ctas, mode);
                assertEquals(List.of("k"), function.getRangePartitionDesc().getPartitionColNames());
            }
        }
    }

    @Test
    void preservesPartitionBeforeDistributionOverflow() {
        for (PredictionMode mode : MODES) {
            for (boolean ctas : new boolean[] {false, true}) {
                String clause = "PARTITION BY RANGE date_trunc('day',k) " +
                        "(PARTITION p1 VALUES LESS THAN ('10')) DISTRIBUTED BY HASH(k) BUCKETS 2147483648";
                ParsingException failure = assertThrows(ParsingException.class, () -> statement(sql(clause, ctas), mode));
                assertTrue(failure.getDetailMsg().contains("Unsupported expr"));
            }
        }
    }

    @Test
    void preservesMalformedPropertiesBeforePartitionConstructor() {
        for (PredictionMode mode : MODES) {
            for (boolean ctas : new boolean[] {false, true}) {
                String clause = "PARTITION BY RANGE date_trunc('day',k) " +
                        "(PARTITION p1 VALUES LESS THAN ('10')) PROPERTIES('x'=1)";
                ParsingException failure = assertThrows(ParsingException.class, () -> statement(sql(clause, ctas), mode));
                assertFalse(failure.getDetailMsg().contains("Unsupported expr"));
            }
        }
    }

    @Test
    void preservesPartitionErrorBeforeTemporaryConfiguration() {
        boolean previous = Config.enable_experimental_temporary_table;
        try {
            Config.enable_experimental_temporary_table = false;
            for (PredictionMode mode : MODES) {
                for (boolean ctas : new boolean[] {false, true}) {
                    String clause = "PARTITION BY RANGE date_trunc('day',k) (PARTITION p1 VALUES LESS THAN ('10'))";
                    String sql = sql(clause, ctas).replace("CREATE TABLE", "CREATE TEMPORARY TABLE");
                    ParsingException failure = assertThrows(ParsingException.class, () -> statement(sql, mode));
                    assertTrue(failure.getDetailMsg().contains("Unsupported expr"));
                }
            }
        } finally {
            Config.enable_experimental_temporary_table = previous;
        }
    }

    private void assertImplicitFailure(ParsingException failure) throws ReflectiveOperationException {
        assertEquals("Does not support creating partitions in advance", failure.getDetailMsg());
        var position = ParsingException.class.getDeclaredField("pos");
        position.setAccessible(true);
        assertTrue(((NodePosition) position.get(failure)).isZero());
    }

    @Test
    void treatsBareImplicitColumnsAsAutomaticForLikeAndCtas() {
        for (PredictionMode mode : MODES) {
            for (boolean ctas : new boolean[] {false, true}) {
                ListPartitionDesc list = (ListPartitionDesc) partition("PARTITION BY(k,v)", ctas, mode);
                assertEquals(List.of("k", "v"), list.getPartitionColNames());
                assertTrue(list.isAutoPartitionTable());
                assertTrue(list.getPartitionDescs().isEmpty());
                assertTrue(list.getPos().isZero());
            }
        }
    }

    @Test
    void rejectsImplicitPrecreatedSingleNullAndTupleValues() throws ReflectiveOperationException {
        for (PredictionMode mode : MODES) {
            for (boolean ctas : new boolean[] {false, true}) {
                for (String clause : List.of("PARTITION BY(k) (PARTITION p1 VALUES IN ('a'))",
                        "PARTITION BY(k) (PARTITION p1 VALUES IN (NULL,'a'))",
                        "PARTITION BY(k,v) (PARTITION p1 VALUES IN (('a',NULL)))")) {
                    assertImplicitFailure(assertThrows(ParsingException.class, () -> statement(sql(clause, ctas), mode)));
                }
            }
        }
    }

    @Test
    void rejectsImplicitValuesBeforeDistributionOverflow() throws ReflectiveOperationException {
        for (PredictionMode mode : MODES) {
            for (boolean ctas : new boolean[] {false, true}) {
                String clause = "PARTITION BY(k) (PARTITION p1 VALUES IN ('a')) " +
                        "DISTRIBUTED BY HASH(k) BUCKETS 2147483648";
                assertImplicitFailure(assertThrows(ParsingException.class, () -> statement(sql(clause, ctas), mode)));
            }
        }
    }

    @Test
    void checksMalformedPropertiesBeforeImplicitDescriptor() {
        for (PredictionMode mode : MODES) {
            for (boolean ctas : new boolean[] {false, true}) {
                String clause = "PARTITION BY(k) (PARTITION p1 VALUES IN ('a')) PROPERTIES('x'=1)";
                ParsingException failure = assertThrows(ParsingException.class, () -> statement(sql(clause, ctas), mode));
                assertFalse(failure.getDetailMsg().contains("Does not support creating partitions in advance"));
            }
        }
    }

    @Test
    void preservesTemporaryConfigurationPriorityAcrossStatementRoots() throws ReflectiveOperationException {
        boolean previous = Config.enable_experimental_temporary_table;
        try {
            Config.enable_experimental_temporary_table = false;
            for (PredictionMode mode : MODES) {
                String clause = "PARTITION BY(k) (PARTITION p1 VALUES IN ('a'))";
                for (boolean ctas : new boolean[] {false, true}) {
                    String query = sql(clause, ctas).replace("CREATE TABLE", "CREATE TEMPORARY TABLE");
                    assertImplicitFailure(assertThrows(ParsingException.class, () -> statement(query, mode)));
                }
                String ordinary = "CREATE TEMPORARY TABLE target(k VARCHAR(20)) " + clause;
                ParsingException failure = assertThrows(ParsingException.class, () -> statement(ordinary, mode));
                assertEquals("FE config 'enable_experimental_temporary_table' is disabled", failure.getDetailMsg());
            }
        } finally {
            Config.enable_experimental_temporary_table = previous;
        }
    }
}
