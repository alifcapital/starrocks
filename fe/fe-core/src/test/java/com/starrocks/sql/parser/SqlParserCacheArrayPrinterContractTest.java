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

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.Parameter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Parse and print only: arrays have not undergone semantic analysis. */
class SqlParserCacheArrayPrinterContractTest {
    private ConnectContext previous;
    private SessionVariable session;

    @BeforeEach
    void installContext() {
        previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setThreadLocalInfo();
        session = context.getSessionVariable();
        session.setSqlDialect("StarRocks");
        session.setEnableDialectDowngrade(false);
    }

    @AfterEach
    void restoreContext() {
        if (previous == null) {
            ConnectContext.remove();
        } else {
            previous.setThreadLocalInfo();
        }
    }

    @Test
    void untypedArraysPrintWithoutInventedType() {
        for (String literal : List.of("[1, 2]", "[]", "[NULL]", "[[1], [2]]")) {
            var expression = SqlParser.parseSqlToExpr(literal, 0);
            assertNull(expression.getType());
            assertEquals(literal, AstToSQLBuilder.toSQL(expression));
        }
        assertEquals("ARRAY<INT>[1, 2]", AstToSQLBuilder.toSQL(
                SqlParser.parseSqlToExpr("ARRAY<INT>[1,2]", 0)));
    }

    @Test
    void cacheConstructorPrintsSelectedArraySeparatelyFromPredicate() {
        for (String expression : List.of("[1,2]", "[]", "[NULL]", "[[1],[2]]",
                "ARRAY_LENGTH([1,2])", "ARRAY<INT>[1,2]")) {
            DataCacheSelectStatement cache = (DataCacheSelectStatement) SqlParser.parseOneWithStarRocksDialect(
                    "CACHE SELECT " + expression + " FROM T WHERE ARRAY_LENGTH([1,2])=2", session);
            SelectRelation select = (SelectRelation) cache.getInsertStmt().getQueryStatement().getQueryRelation();
            assertEquals(1, select.getSelectList().getItems().size());
            var selected = select.getSelectList().getItems().get(0).getExpr();
            var expected = SqlParser.parseSqlToExpr(expression, 0);
            assertEquals(expected.getClass(), selected.getClass());
            String rendered = AstToSQLBuilder.toSQL(selected);
            assertEquals(AstToSQLBuilder.toSQL(expected), rendered);
            assertNotNull(select.getPredicate());
            assertNotNull(cache.getInsertStmt().getOrigStmt());
            String original = cache.getInsertStmt().getOrigStmt().originStmt;
            int from = original.indexOf("FROM");
            assertTrue(from > 0);
            assertTrue(original.substring(0, from).contains(rendered));
        }
    }

    @Test
    void inlinePrepareRetainsParameterIdentityAndValidControls() {
        PrepareStmt prepared = (PrepareStmt) SqlParser.parseOneWithStarRocksDialect(
                "PREPARE p FROM CACHE SELECT ARRAY_LENGTH([?,2]) FROM T WHERE C=?", session);
        assertEquals(2, prepared.getParameters().size());
        DataCacheSelectStatement cache = assertInstanceOf(DataCacheSelectStatement.class, prepared.getInnerStmt());
        SelectRelation select = (SelectRelation) cache.getInsertStmt().getQueryStatement().getQueryRelation();
        ArrayExpr array = assertInstanceOf(ArrayExpr.class,
                select.getSelectList().getItems().get(0).getExpr().getChild(0));
        assertEquals(0, assertInstanceOf(Parameter.class, array.getChild(0)).getSlotId());
        assertEquals(1, assertInstanceOf(Parameter.class, select.getPredicate().getChild(1)).getSlotId());
        var first = new IntLiteral(11);
        var second = new IntLiteral(22);
        assertSame(cache, prepared.assignValues(List.of(first, second)));
        assertSame(first, prepared.getParameters().get(0).getExpr());
        assertSame(second, prepared.getParameters().get(1).getExpr());
        assertEquals(0, prepared.getParameters().get(0).getSlotId());
        assertEquals(1, prepared.getParameters().get(1).getSlotId());
        assertInstanceOf(DataCacheSelectStatement.class,
                SqlParser.parseOneWithStarRocksDialect("CACHE SELECT 1 FROM T", session));
        assertNotNull(SqlParser.parseOneWithStarRocksDialect(
                "CREATE DATACACHE RULE Cat.Db.* PRIORITY=1", session));
    }

    @Test
    void malformedOuterBoundaryStillReportsSyntaxError() {
        assertThrows(ParsingException.class, () -> SqlParser.parseOneWithStarRocksDialect(
                "CACHE SELECT ARRAY_LENGTH([1,2]) FROM T trailing trailing", session));
    }
}
