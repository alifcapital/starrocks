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
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.StatementBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Hand-written parser for queries, INSERT, UPDATE, DELETE and EXPLAIN or TRACE of them.
 * It builds the same AST as AstBuilder.
 *
 * <p>BI tools generate very large queries. On them the ANTLR parser is slow and grows its shared prediction
 * cache, so we try this parser first. We want ANTLR to stay the reference for everything else: this parser
 * gives up on a statement it does not support and on any error, and the caller parses the SQL again with
 * ANTLR. ANTLR then gives the user the usual syntax error message.
 */
final class FastQueryParser {
    private static final Logger LOG = LogManager.getLogger(FastQueryParser.class);
    private static final int MIN_TOKEN_LIMIT = 100;

    /** Either the parsed statements, or the reason why the caller must use ANTLR. */
    record Attempt(List<StatementBase> statements, String fallbackReason, Throwable failure) {
        boolean parsed() {
            return statements != null;
        }
    }

    private FastQueryParser() {
    }

    /** Returns null when the caller must parse the SQL with ANTLR. */
    static List<StatementBase> tryParse(String sql, SessionVariable session, AstBuilder.AstBuilderFactory factory) {
        // This parser constructs the AstBuilder AST itself, so a custom AstBuilder must see the ANTLR tree.
        if (!Config.enable_fast_query_parser || factory.getClass() != AstBuilder.AstBuilderFactory.class) {
            return null;
        }
        Attempt attempt = attempt(sql, session);
        if (!attempt.parsed() && attempt.failure() != null && LOG.isDebugEnabled()) {
            LOG.debug("fast query parser falls back to ANTLR: {}", attempt.failure().toString());
        }
        return attempt.statements();
    }

    static Attempt attempt(String sql, SessionVariable session) {
        try {
            DenseOwnedTokenStream tokens = new DenseOwnedTokenStream(sql, session.getSqlMode());
            boolean batch = isBatch(tokens);
            if (!batch && !startsSupportedStatement(tokens.LA(1))) {
                return new Attempt(null, "statement type", null);
            }
            DirectQueryParser parser = new DirectQueryParser(tokens, session.getSqlMode(),
                    GlobalVariable.enableTableNameCaseInsensitive,
                    Math.max(MIN_TOKEN_LIMIT, session.getParseTokensLimit()),
                    Math.max(Config.expr_children_limit, session.getExprChildrenLimit()), session);
            if (batch) {
                return new Attempt(parser.parseStatementBatch(sql), null, null);
            }
            StatementBase statement = parser.parseWholeStatement();
            LexicalParameterContext parameters = parser.parameterContext();
            if (parameters != null) {
                statement = new PrepareStmt("", statement, parameters.parameters());
            } else {
                statement.setOrigStmt(new OriginStatement(sql, 0));
            }
            return new Attempt(List.of(statement), null, null);
        } catch (RuntimeException | StackOverflowError e) {
            // Lexical errors, unsupported syntax, AST constructor errors and our own bugs all go to ANTLR.
            return new Attempt(null, fallbackReason(e), e);
        }
    }

    private static String fallbackReason(Throwable e) {
        if (e instanceof DirectQueryParser.UnsupportedQuery || e instanceof DirectExpressionParser.UnsupportedExpression) {
            return "unsupported: " + e.getMessage();
        }
        if (e instanceof ParsingException) {
            return "parsing error";
        }
        return "exception: " + e.getClass().getName();
    }

    // A batch has a semicolon that does not end the input: either several semicolons, or one with a statement after it.
    private static boolean isBatch(DenseOwnedTokenStream tokens) {
        int semicolons = tokens.semicolonCount();
        return semicolons > 1 || semicolons == 1 && tokens.typeAt(tokens.size() - 2) != FrozenTokenCatalog.SEMICOLON;
    }

    static boolean startsSupportedStatement(int type) {
        switch (type) {
            case FrozenTokenCatalog.SELECT:
            case FrozenTokenCatalog.WITH:
            case FrozenTokenCatalog.T_2: // '('
            case FrozenTokenCatalog.VALUES:
            case FrozenTokenCatalog.INSERT:
            case FrozenTokenCatalog.UPDATE:
            case FrozenTokenCatalog.DELETE:
            case FrozenTokenCatalog.EXPLAIN:
            case FrozenTokenCatalog.DESC:
            case FrozenTokenCatalog.DESCRIBE:
            case FrozenTokenCatalog.TRACE:
                return true;
            default:
                return false;
        }
    }
}
