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

import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.ImportColumnsStmt;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.MultiInPredicate;
import com.starrocks.sql.ast.expression.Parameter;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.Subquery;
import org.antlr.v4.runtime.ParserRuleContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlParserLexicalParameterTest {
    private ConnectContext previousContext;

    @BeforeEach
    void installParserContext() {
        previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        // Parser access only: no FE initialization or external services are needed.
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setThreadLocalInfo();
    }

    @AfterEach
    void restoreParserContext() {
        if (previousContext == null) {
            ConnectContext.remove();
        } else {
            previousContext.setThreadLocalInfo();
        }
    }

    private static SessionVariable session() {
        SessionVariable session = new SessionVariable();
        session.setSqlMode(0);
        return session;
    }

    private static PrepareStmt prepare(String sql) {
        return (PrepareStmt) SqlParser.parseOneWithStarRocksDialect(sql, session());
    }

    private static SelectRelation select(PrepareStmt prepare) {
        return (SelectRelation) ((QueryStatement) prepare.getInnerStmt()).getQueryRelation();
    }

    private static void assertSelectAndOrderSlots(QueryStatement query, int selectSlot, int orderSlot) {
        SelectRelation relation = (SelectRelation) query.getQueryRelation();
        Parameter selection = (Parameter) relation.getSelectList().getItems().get(0).getExpr();
        Parameter ordering = (Parameter) relation.getOrderBy().get(0).getExpr();
        assertEquals(selectSlot, selection.getSlotId());
        assertEquals(orderSlot, ordering.getSlotId());
    }

    @Test
    void bindsSelectAndOrderByInLexicalOrderWithoutReplacingNodes() {
        PrepareStmt prepare = prepare("SELECT ? ORDER BY ?");
        Parameter selection = (Parameter) select(prepare).getSelectList().getItems().get(0).getExpr();
        Parameter ordering = (Parameter) select(prepare).getOrderBy().get(0).getExpr();
        assertSame(selection, prepare.getParameters().get(0));
        assertSame(ordering, prepare.getParameters().get(1));
        assertSelectAndOrderSlots((QueryStatement) prepare.getInnerStmt(), 0, 1);

        for (long base : List.of(1000L, 2000L)) {
            List<Expr> markers = List.of(new IntLiteral(base), new IntLiteral(base + 1));
            assertSame(prepare.getInnerStmt(), prepare.assignValues(markers));
            assertSame(markers.get(0), selection.getExpr());
            assertSame(markers.get(1), ordering.getExpr());
            assertSame(selection, prepare.getParameters().get(0));
            assertSame(ordering, prepare.getParameters().get(1));
        }
    }

    @Test
    void numbersTupleOuterExpressionBeforeItsSubquery() {
        PrepareStmt prepare = prepare("SELECT (a,?) IN (SELECT ?,x FROM t)");
        MultiInPredicate tuple = (MultiInPredicate) select(prepare).getSelectList().getItems().get(0).getExpr();
        Parameter outer = (Parameter) tuple.getChild(1);
        SelectRelation right = (SelectRelation) ((Subquery) tuple.getChild(2)).getQueryStatement().getQueryRelation();
        Parameter inner = (Parameter) right.getSelectList().getItems().get(0).getExpr();
        assertSame(outer, prepare.getParameters().get(0));
        assertSame(inner, prepare.getParameters().get(1));
        assertEquals(0, outer.getSlotId());
        assertEquals(1, inner.getSlotId());
    }

    @Test
    void retainsBindingSlotsForDiscardedSubqueryOrderBy() {
        PrepareStmt prepare = prepare("SELECT ? FROM (SELECT ? ORDER BY ?) q ORDER BY ?");
        SelectRelation outer = select(prepare);
        SelectRelation inner = (SelectRelation) ((SubqueryRelation) outer.getRelation())
                .getQueryStatement().getQueryRelation();
        assertTrue(inner.getOrderBy().isEmpty());
        assertEquals(4, prepare.getParameters().size());
        assertSame(outer.getSelectList().getItems().get(0).getExpr(), prepare.getParameters().get(0));
        assertSame(inner.getSelectList().getItems().get(0).getExpr(), prepare.getParameters().get(1));
        assertEquals(2, prepare.getParameters().get(2).getSlotId());
        assertSame(outer.getOrderBy().get(0).getExpr(), prepare.getParameters().get(3));
        List<Expr> markers = List.of(new IntLiteral(11), new IntLiteral(22), new IntLiteral(33), new IntLiteral(44));
        prepare.assignValues(markers);
        assertSame(markers.get(2), prepare.getParameters().get(2).getExpr());
    }

    @Test
    void usesIndependentStatementAndQuotedPrepareDomains() {
        List<StatementBase> statements = SqlParser.parse("SELECT ?; SELECT ? ORDER BY ?", session());
        PrepareStmt first = (PrepareStmt) statements.get(0);
        PrepareStmt second = (PrepareStmt) statements.get(1);
        assertEquals(0, first.getParameters().get(0).getSlotId());
        assertSelectAndOrderSlots((QueryStatement) second.getInnerStmt(), 0, 1);
        assertNotSame(first.getParameters().get(0), second.getParameters().get(0));

        for (String sql : List.of("PREPARE p FROM SELECT ? ORDER BY ?", "PREPARE p FROM 'SELECT ? ORDER BY ?'")) {
            PrepareStmt named = prepare(sql);
            assertEquals("p", named.getName());
            assertSelectAndOrderSlots((QueryStatement) named.getInnerStmt(), 0, 1);
            assertSame(select(named).getSelectList().getItems().get(0).getExpr(), named.getParameters().get(0));
            assertSame(select(named).getOrderBy().get(0).getExpr(), named.getParameters().get(1));
        }
    }

    @Test
    void ignoresQuotedAndCommentQuestionMarksAfterSupplementaryUnicode() {
        PrepareStmt prepare = prepare("SELECT '😀?', ? /* 😀 ? */ ORDER BY ?");
        assertEquals(2, prepare.getParameters().size());
        assertEquals("😀?", ((StringLiteral) select(prepare).getSelectList().getItems().get(0).getExpr()).getStringValue());
        assertSame(select(prepare).getSelectList().getItems().get(1).getExpr(), prepare.getParameters().get(0));
        assertSame(select(prepare).getOrderBy().get(0).getExpr(), prepare.getParameters().get(1));
        assertEquals(0, prepare.getParameters().get(0).getSlotId());
        assertEquals(1, prepare.getParameters().get(1).getSlotId());
    }

    @Test
    void initializesAllPublicExpressionAndImportEntryPoints() {
        String sql = "? + (SELECT ? ORDER BY ?)";
        for (Expr expression : List.of(SqlParser.parseExpression(sql, session()), SqlParser.parseSqlToExpr(sql, 0))) {
            assertEquals(0, ((Parameter) expression.getChild(0)).getSlotId());
            assertSelectAndOrderSlots(((Subquery) expression.getChild(1)).getQueryStatement(), 1, 2);
        }
        List<Expr> expressions = SqlParser.parseSqlToExprs("?, (SELECT ? ORDER BY ?)", session());
        assertEquals(0, ((Parameter) expressions.get(0)).getSlotId());
        assertSelectAndOrderSlots(((Subquery) expressions.get(1)).getQueryStatement(), 1, 2);
        ImportColumnsStmt columns = SqlParser.parseImportColumns("COLUMNS(a=?, b=?)", 0);
        assertEquals(0, ((Parameter) columns.getColumns().get(0).getExpr()).getSlotId());
        assertEquals(1, ((Parameter) columns.getColumns().get(1).getExpr()).getSlotId());
    }

    private static final class RecordingBuilder extends AstBuilder {
        private final IdentityHashMap<Parameter, Boolean> visited = new IdentityHashMap<>();

        private RecordingBuilder(long mode, boolean caseInsensitive,
                                 IdentityHashMap<ParserRuleContext, List<HintNode>> hints) {
            super(mode, caseInsensitive, hints);
        }

        @Override
        public ParseNode visitParameter(StarRocksParser.ParameterContext context) {
            Parameter parameter = (Parameter) super.visitParameter(context);
            visited.put(parameter, true);
            return parameter;
        }
    }

    private static final class RecordingFactory extends AstBuilder.AstBuilderFactory {
        private int calls;

        @Override
        public AstBuilder create(long mode, boolean caseInsensitive,
                                 IdentityHashMap<ParserRuleContext, List<HintNode>> hints) {
            calls++;
            return new RecordingBuilder(mode, caseInsensitive, hints);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void preservesFactoryAndSuperCallingVisitorCompatibility() throws Exception {
        Method invoke = SqlParser.class.getDeclaredMethod("invokeParser", String.class,
                SessionVariable.class, Function.class);
        invoke.setAccessible(true);
        Function<StarRocksParser, ParserRuleContext> entry = StarRocksParser::sqlStatements;
        Pair<ParserRuleContext, StarRocksParser> parsed =
                (Pair<ParserRuleContext, StarRocksParser>) invoke.invoke(null, "SELECT ? ORDER BY ?", session(), entry);
        StarRocksParser.SingleStatementContext rule =
                ((StarRocksParser.SqlStatementsContext) parsed.first).singleStatement(0);
        RecordingFactory factory = new RecordingFactory();
        RecordingBuilder builder = (RecordingBuilder) factory.create(0, false, new IdentityHashMap<>());
        builder.initializeParameterContext(LexicalParameterContext.forRule(parsed.second, rule));
        QueryStatement query = (QueryStatement) builder.visitSingleStatement(rule);
        assertEquals(1, factory.calls);
        assertSelectAndOrderSlots(query, 0, 1);
        SelectRelation relation = (SelectRelation) query.getQueryRelation();
        assertSame(relation.getSelectList().getItems().get(0).getExpr(), builder.getParameters().get(0));
        assertSame(relation.getOrderBy().get(0).getExpr(), builder.getParameters().get(1));
        assertTrue(builder.visited.containsKey(builder.getParameters().get(0)));
        assertTrue(builder.visited.containsKey(builder.getParameters().get(1)));
    }
}
