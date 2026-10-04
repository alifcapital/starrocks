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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.TableName;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.EmptyStmt;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.GroupByClause;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.OutFileClause;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.PivotAggregation;
import com.starrocks.sql.ast.PivotRelation;
import com.starrocks.sql.ast.PivotValue;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryPeriod;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetQualifier;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.TableSampleClause;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.ast.expression.AnalyticWindowBoundary;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CaseWhenClause;
import com.starrocks.sql.ast.expression.DefaultValueExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.IntervalLiteral;
import com.starrocks.sql.ast.expression.LimitElement;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NamedArgument;
import com.starrocks.sql.ast.expression.SetVarHint;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.UserVariableExpr;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static com.starrocks.sql.parser.FrozenTokenCatalog.ALL;
import static com.starrocks.sql.parser.FrozenTokenCatalog.ANALYZE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.AND;
import static com.starrocks.sql.parser.FrozenTokenCatalog.ANTI;
import static com.starrocks.sql.parser.FrozenTokenCatalog.AS;
import static com.starrocks.sql.parser.FrozenTokenCatalog.ASC;
import static com.starrocks.sql.parser.FrozenTokenCatalog.ASOF;
import static com.starrocks.sql.parser.FrozenTokenCatalog.ASSERT_ROWS;
import static com.starrocks.sql.parser.FrozenTokenCatalog.ASTERISK_SYMBOL;
import static com.starrocks.sql.parser.FrozenTokenCatalog.AT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.AWARE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.BACKQUOTED_IDENTIFIER;
import static com.starrocks.sql.parser.FrozenTokenCatalog.BEFORE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.BETWEEN;
import static com.starrocks.sql.parser.FrozenTokenCatalog.BITOR;
import static com.starrocks.sql.parser.FrozenTokenCatalog.BY;
import static com.starrocks.sql.parser.FrozenTokenCatalog.COSTS;
import static com.starrocks.sql.parser.FrozenTokenCatalog.CROSS;
import static com.starrocks.sql.parser.FrozenTokenCatalog.CUBE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DEFAULT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DELETE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DESC;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DESCRIBE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DISTINCT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DOT_IDENTIFIER;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DOUBLE_QUOTED_TEXT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DUAL;
import static com.starrocks.sql.parser.FrozenTokenCatalog.EQ;
import static com.starrocks.sql.parser.FrozenTokenCatalog.EQ_FOR_NULL;
import static com.starrocks.sql.parser.FrozenTokenCatalog.EXCEPT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.EXCLUDE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.EXPLAIN;
import static com.starrocks.sql.parser.FrozenTokenCatalog.FILES;
import static com.starrocks.sql.parser.FrozenTokenCatalog.FIRST;
import static com.starrocks.sql.parser.FrozenTokenCatalog.FOR;
import static com.starrocks.sql.parser.FrozenTokenCatalog.FORMAT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.FROM;
import static com.starrocks.sql.parser.FrozenTokenCatalog.FULL;
import static com.starrocks.sql.parser.FrozenTokenCatalog.GROUP;
import static com.starrocks.sql.parser.FrozenTokenCatalog.GROUPING;
import static com.starrocks.sql.parser.FrozenTokenCatalog.GT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.GTE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.HAVING;
import static com.starrocks.sql.parser.FrozenTokenCatalog.IN;
import static com.starrocks.sql.parser.FrozenTokenCatalog.INNER;
import static com.starrocks.sql.parser.FrozenTokenCatalog.INSERT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.INTEGER_VALUE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.INTERSECT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.INTO;
import static com.starrocks.sql.parser.FrozenTokenCatalog.JOIN;
import static com.starrocks.sql.parser.FrozenTokenCatalog.LAST;
import static com.starrocks.sql.parser.FrozenTokenCatalog.LATERAL;
import static com.starrocks.sql.parser.FrozenTokenCatalog.LEFT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.LIMIT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.LOGICAL;
import static com.starrocks.sql.parser.FrozenTokenCatalog.LOGS;
import static com.starrocks.sql.parser.FrozenTokenCatalog.LT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.LTE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.MINUS;
import static com.starrocks.sql.parser.FrozenTokenCatalog.NEQ;
import static com.starrocks.sql.parser.FrozenTokenCatalog.NULL;
import static com.starrocks.sql.parser.FrozenTokenCatalog.NULLS;
import static com.starrocks.sql.parser.FrozenTokenCatalog.OF;
import static com.starrocks.sql.parser.FrozenTokenCatalog.OFFSET;
import static com.starrocks.sql.parser.FrozenTokenCatalog.ON;
import static com.starrocks.sql.parser.FrozenTokenCatalog.OPEN;
import static com.starrocks.sql.parser.FrozenTokenCatalog.ORDER;
import static com.starrocks.sql.parser.FrozenTokenCatalog.OUTER;
import static com.starrocks.sql.parser.FrozenTokenCatalog.OUTFILE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.PARAMETER;
import static com.starrocks.sql.parser.FrozenTokenCatalog.PARTITION;
import static com.starrocks.sql.parser.FrozenTokenCatalog.PARTITIONS;
import static com.starrocks.sql.parser.FrozenTokenCatalog.PIVOT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.PROPERTIES;
import static com.starrocks.sql.parser.FrozenTokenCatalog.QUALIFY;
import static com.starrocks.sql.parser.FrozenTokenCatalog.REASON;
import static com.starrocks.sql.parser.FrozenTokenCatalog.RECURSIVE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.REPLICA;
import static com.starrocks.sql.parser.FrozenTokenCatalog.RIGHT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.ROLLUP;
import static com.starrocks.sql.parser.FrozenTokenCatalog.SAMPLE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.SCHEDULER;
import static com.starrocks.sql.parser.FrozenTokenCatalog.SELECT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.SEMI;
import static com.starrocks.sql.parser.FrozenTokenCatalog.SEMICOLON;
import static com.starrocks.sql.parser.FrozenTokenCatalog.SETS;
import static com.starrocks.sql.parser.FrozenTokenCatalog.SINGLE_QUOTED_TEXT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.SYSTEM_TIME;
import static com.starrocks.sql.parser.FrozenTokenCatalog.TABLE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.TABLET;
import static com.starrocks.sql.parser.FrozenTokenCatalog.TABLETS;
import static com.starrocks.sql.parser.FrozenTokenCatalog.TEMPORARY;
import static com.starrocks.sql.parser.FrozenTokenCatalog.TIMES;
import static com.starrocks.sql.parser.FrozenTokenCatalog.TIMESTAMP;
import static com.starrocks.sql.parser.FrozenTokenCatalog.TO;
import static com.starrocks.sql.parser.FrozenTokenCatalog.TRACE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.UNION;
import static com.starrocks.sql.parser.FrozenTokenCatalog.UPDATE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.USING;
import static com.starrocks.sql.parser.FrozenTokenCatalog.VALUES;
import static com.starrocks.sql.parser.FrozenTokenCatalog.VERBOSE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.VERSION;
import static com.starrocks.sql.parser.FrozenTokenCatalog.WHERE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.WITH;

/** Hand-written query parser. Unsupported attempts rewind the whole input. */
public final class DirectQueryParser {
    public static final class UnsupportedQuery extends RuntimeException {
        public UnsupportedQuery(String reason) {
            super(reason);
        }
    }

    private final CommonTokenStream tokens;
    private final long mode;
    private final boolean caseInsensitive;
    private final int tokenLimit;
    private final int expressionLimit;
    private int last;
    // Installed only by the final USING relation of CREATE BASELINE.
    private int baselinePropertiesDepth = -1;
    private LexicalParameterContext parameters;

    LexicalParameterContext parameterContext() {
        return parameters;
    }

    /** Shared standalone preflight owns lexical slots before SHOW visits any expression. */
    void initializeStandaloneParameters(LexicalParameterContext context) {
        if (parameters != null) {
            throw new IllegalStateException("parameter context already installed");
        }
        parameters = context;
    }

    private final DirectParseBudget budget;
    private final com.starrocks.qe.SessionVariable session;
    private boolean collectHints = true;

    private record PendingHints(SelectList select, StatementBase statement, List<Token> tokens) {}

    private final List<PendingHints> pendingHints = new ArrayList<>();
    // Filled from the grammar at build time, never from query text.
    private static final boolean[] IDENTIFIERS = QueryIdentifiers.flags();
    private static final int OPEN = FrozenTokenCatalog.T_2;
    private static final int CLOSE = FrozenTokenCatalog.T_3;
    private static final int COMMA = FrozenTokenCatalog.T_0;
    private static final int DOT = FrozenTokenCatalog.T_1;
    private static final int EQUAL = FrozenTokenCatalog.EQ;
    private static final int NAMED_ARROW = FrozenTokenCatalog.T_4;

    private static com.starrocks.qe.SessionVariable hintSession(long mode) {
        com.starrocks.qe.SessionVariable session = new com.starrocks.qe.SessionVariable();
        session.setSqlMode(mode);
        return session;
    }

    public DirectQueryParser(
            CommonTokenStream tokens,
            long mode,
            boolean caseInsensitive,
            int tokenLimit,
            int expressionLimit) {
        this(tokens, mode, caseInsensitive, tokenLimit, expressionLimit, hintSession(mode));
    }

    public DirectQueryParser(
            CommonTokenStream tokens,
            long mode,
            boolean caseInsensitive,
            int tokenLimit,
            int expressionLimit,
            com.starrocks.qe.SessionVariable session) {
        this.session = Objects.requireNonNull(session);
        this.budget = new DirectParseBudget();
        this.tokens = tokens;
        this.mode = mode;
        this.caseInsensitive = caseInsensitive;
        this.tokenLimit = tokenLimit;
        this.expressionLimit = expressionLimit;
    }

    /** Rare bounded-expression child: no SQL preflight, same budgets/session. */
    private DirectQueryParser(DirectQueryParser owner, CommonTokenStream tokens) {
        this.tokens = tokens;
        this.mode = owner.mode;
        this.caseInsensitive = owner.caseInsensitive;
        this.tokenLimit = owner.tokenLimit;
        this.expressionLimit = owner.expressionLimit;
        this.parameters = owner.parameters;
        this.budget = owner.budget;
        this.session = owner.session;
        this.collectHints = false;
    }

    public QueryStatement parseWhole() {
        int saved = tokens.index();
        StatementBase statement = parseWholeStatement();
        if (statement instanceof QueryStatement query) {
            return query;
        }
        tokens.seek(saved);
        throw new UnsupportedQuery("query compatibility entry received DML");
    }

    StatementBase parseWholeStatement() {
        int saved = tokens.index();
        boolean success = false;
        parsingDmlStatement = false;
        try {
            parameters = DirectStatementPreflight.check(tokens, tokenLimit, expressionLimit, true);
            StatementBase statement = statementWithHints(saved);
            if (eat(SEMICOLON) && type() != Token.EOF) {
                throw unsupported("multiple statements");
            }
            if (type() != Token.EOF) {
                throw unsupported("query boundary");
            }
            success = true;
            return statement;
        } catch (DirectExpressionParser.UnsupportedExpression e) {
            throw new UnsupportedQuery("expression: " + e.getMessage());
        } catch (NumberFormatException e) {
            throw new UnsupportedQuery("numeric constructor: " + e.getMessage());
        } catch (ParsingException e) {
            // The reference constructs collected hints before any AST descendants.
            // Until shared deferred diagnostics exist, replay competing constructor errors.
            if (parsingDmlStatement
                    && tokens instanceof DenseOwnedTokenStream dense
                    && !dense.allHints().isEmpty()) {
                throw new UnsupportedQuery("DML hint diagnostic priority requires reference");
            }
            throw e;
        } finally {
            if (!success) {
                tokens.seek(saved);
            }
        }
    }

    private StatementBase statementWithHints(int rootStart) {
        QueryPrefix queryPrefix = queryPrefix();
        StatementBase.ExplainLevel explain = queryPrefix.explain();
        int queryStart = tokens.LT(1).getStartIndex();
        StatementBase statement = statementSyntax(rootStart, queryStart, queryPrefix);
        if (statement instanceof UpdateStmt || statement instanceof DeleteStmt) {
            DirectDmlParser.applyRootExplain(statement, explain);
        }
        attachHints(statement);
        return statement;
    }

    /**
     * Parses a batch like SqlParser.parseWithAntlr: every statement, including an empty one between two
     * semicolons, takes the next index, and every statement binds only its own parameters. The token stream
     * must be a DenseOwnedTokenStream. Any statement that this parser does not own fails the whole batch.
     */
    List<StatementBase> parseStatementBatch(String sql) {
        DenseOwnedTokenStream dense = (DenseOwnedTokenStream) tokens;
        int saved = tokens.index();
        boolean success = false;
        try {
            DirectStatementPreflight.check(tokens, tokenLimit, expressionLimit, true, true);
            int[] offsets = dense.parameterOffsets();
            int nextParameter = 0;
            List<StatementBase> statements = new ArrayList<>();
            boolean owned = false;
            while (type() != Token.EOF) {
                StatementBase statement;
                int parameterCount = 0;
                if (eat(SEMICOLON)) {
                    statement = new EmptyStmt();
                } else {
                    if (!FastQueryParser.startsSupportedStatement(type())) {
                        throw unsupported("statement type");
                    }
                    int first = nextParameter;
                    if (offsets.length != 0) {
                        int endOrdinal = statementEnd(dense);
                        int end = dense.typeAt(endOrdinal) == Token.EOF ? Integer.MAX_VALUE : dense.startAt(endOrdinal);
                        while (nextParameter < offsets.length && offsets[nextParameter] < end) {
                            nextParameter++;
                        }
                    }
                    parameterCount = nextParameter - first;
                    parameters = parameterCount == 0
                            ? null
                            : LexicalParameterContext.fromOffsets(
                                    java.util.Arrays.copyOfRange(offsets, first, nextParameter));
                    parsingDmlStatement = false;
                    pendingHints.clear();
                    statement = statementWithHints(tokens.index());
                    if (!eat(SEMICOLON) && type() != Token.EOF) {
                        throw unsupported("query boundary");
                    }
                    owned = true;
                }
                if (parameterCount != 0) {
                    statement = new PrepareStmt("", statement, parameters.parameters());
                } else {
                    statement.setOrigStmt(new OriginStatement(sql, statements.size()));
                }
                statements.add(statement);
            }
            if (!owned) {
                throw unsupported("batch without a statement");
            }
            success = true;
            return statements;
        } catch (DirectExpressionParser.UnsupportedExpression e) {
            throw new UnsupportedQuery("expression: " + e.getMessage());
        } catch (NumberFormatException e) {
            throw new UnsupportedQuery("numeric constructor: " + e.getMessage());
        } finally {
            pendingHints.clear();
            if (!success) {
                tokens.seek(saved);
            }
        }
    }

    // The ordinal of the semicolon or EOF that ends the statement starting at the current token.
    private int statementEnd(DenseOwnedTokenStream dense) {
        int index = tokens.index();
        while (dense.typeAt(index) != SEMICOLON && dense.typeAt(index) != Token.EOF) {
            index++;
        }
        return index;
    }

    private boolean parsingDmlStatement;

    /** Shared syntax body; the ordinary entry retains its existing preflight and boundary. */
    private StatementBase statementSyntax(int rootStart, int queryStart, QueryPrefix queryPrefix) {
        StatementBase.ExplainLevel explain = queryPrefix.explain();
        String traceMode = queryPrefix.traceMode();
        String traceModule = queryPrefix.traceModule();
        StatementBase statement;
        if (type() == INSERT) {
            parsingDmlStatement = true;
            if (traceMode != null) {
                throw unsupported("TRACE INSERT grammar");
            }
            statement =
                    new DirectDmlParser(this, tokens, caseInsensitive).insert(rootStart, explain);
        } else {
            budget.enterQuery();
            try {
                if (traceMode != null && traceDmlAhead()) {
                    throw unsupported("TRACE DML diagnostic order pending");
                }
                // WITH descendants may fail before the eventual DML root is known.
                // Hint-bearing WITH failures conservatively replay without a dispatch prewalk.
                if (type() == WITH) {
                    parsingDmlStatement = true;
                }
                CtePrefix prefix = ctePrefix();
                if (type() == UPDATE || type() == DELETE) {
                    parsingDmlStatement = true;
                    if (traceMode != null) {
                        throw unsupported("TRACE DML diagnostic order pending");
                    }
                    statement =
                            new DirectDmlParser(this, tokens, caseInsensitive)
                                    .root(
                                            rootStart,
                                            prefix == null ? null : prefix.ctes(),
                                            explain);
                } else {
                    QueryRelation relation = queryRelationAfterPrefix(prefix);
                    OutFileClause outfile = type() == INTO ? outfile() : null;
                    QueryStatement query = new QueryStatement(relation);
                    query.setQueryStartIndex(queryStart);
                    if (explain != null) {
                        query.setIsExplain(true, explain);
                    }
                    if (traceMode != null) {
                        query.setIsTrace(traceMode, traceModule);
                    }
                    if (outfile != null) {
                        query.setOutFileClause(outfile);
                    }
                    statement = query;
                }
            } finally {
                budget.exitQuery();
            }
        }
        return statement;
    }

    /** Cold PREPARE body: caller owns lexical context, limits, boundary and origins. */
    StatementBase preparedBody() {
        int saved = tokens.index();
        boolean success = false;
        parsingDmlStatement = false;
        try {
            QueryPrefix prefix = queryPrefix();
            StatementBase statement = statementSyntax(saved, tokens.LT(1).getStartIndex(), prefix);
            if (statement instanceof UpdateStmt || statement instanceof DeleteStmt) {
                DirectDmlParser.applyRootExplain(statement, prefix.explain());
            }
            attachHints(statement);
            success = true;
            return statement;
        } catch (DirectExpressionParser.UnsupportedExpression e) {
            throw new UnsupportedQuery("expression: " + e.getMessage());
        } catch (NumberFormatException e) {
            throw new UnsupportedQuery("numeric constructor: " + e.getMessage());
        } finally {
            if (!success) {
                tokens.seek(saved);
            }
        }
    }

    /** TRACE-only dispatch lookahead: balanced token groups, no AST/constructor visits. */
    private boolean traceDmlAhead() {
        if (type() != WITH) {
            return type() == UPDATE || type() == DELETE;
        }
        int depth = 0;
        for (int k = 2; ; k++) {
            int token = tokens.LA(k);
            if (token == Token.EOF || token == SEMICOLON) {
                return false;
            }
            if (token == OPEN) {
                depth++;
                continue;
            }
            if (token == CLOSE) {
                if (depth == 0) {
                    return false;
                }
                depth--;
                continue;
            }
            if (depth == 0) {
                if (token == UPDATE || token == DELETE) {
                    return true;
                }
                if (token == SELECT) {
                    return false;
                }
            }
        }
    }

    private record QueryPrefix(
            StatementBase.ExplainLevel explain, String traceMode, String traceModule) {}

    private static final QueryPrefix EMPTY_QUERY_PREFIX = new QueryPrefix(null, null, "base");

    private QueryPrefix queryPrefix() {
        if (type() != DESC && type() != DESCRIBE && type() != EXPLAIN && type() != TRACE) {
            return EMPTY_QUERY_PREFIX;
        }
        StatementBase.ExplainLevel explain = null;
        String traceMode = null;
        String traceModule = "base";
        if (type() == DESC || type() == DESCRIBE || type() == EXPLAIN) {
            take();
            explain =
                    StatementBase.ExplainLevel.parse(
                            com.starrocks.common.Config.query_explain_level);
            switch (type()) {
                case LOGICAL -> {
                    take();
                    explain = StatementBase.ExplainLevel.LOGICAL;
                }
                case ANALYZE -> {
                    take();
                    explain = StatementBase.ExplainLevel.ANALYZE;
                }
                case VERBOSE -> {
                    take();
                    explain = StatementBase.ExplainLevel.VERBOSE;
                }
                case COSTS -> {
                    take();
                    explain = StatementBase.ExplainLevel.COSTS;
                }
                case SCHEDULER -> {
                    take();
                    explain = StatementBase.ExplainLevel.SCHEDULER;
                }
                default -> { }
            }
        } else if (eat(TRACE)) {
            traceMode = switch (type()) {
                case ALL -> "TIMING";
                case LOGS -> "LOGS";
                case TIMES -> "TIMER";
                case VALUES -> "VARS";
                case REASON -> "REASON";
                default -> throw unsupported("TRACE mode");
            };
            take();
            if (identifierType(type())) {
                traceModule = identifier();
            }
        }
        return new QueryPrefix(explain, traceMode, traceModule);
    }

    String stringToken() {
        if (type() != SINGLE_QUOTED_TEXT && type() != DOUBLE_QUOTED_TEXT) {
            throw unsupported("string literal");
        }
        return stringValue(take().getText());
    }

    private OutFileClause outfile() {
        int start = tokens.index();
        expect(INTO);
        expect(OUTFILE);
        String file = stringToken();
        String format = null;
        if (eat(FORMAT)) {
            expect(AS);
            format = identifierType(type()) ? identifier() : stringToken();
        }
        Map<String, String> properties = eat(PROPERTIES) ? dmlProperties(false) : new HashMap<>();
        return new OutFileClause(file, format, properties, pos(start));
    }

    private static final int OPEN_BRACKET = FrozenTokenCatalog.T_5;
    private static final int CLOSE_BRACKET = FrozenTokenCatalog.T_6;

    private QueryRelation queryRelationWithoutHints() {
        boolean previous = collectHints;
        collectHints = false;
        try {
            return queryRelation();
        } finally {
            collectHints = previous;
        }
    }

    private Relation relationPrimaryWithoutHints() {
        boolean previous = collectHints;
        collectHints = false;
        try {
            return relationPrimary();
        } finally {
            collectHints = previous;
        }
    }

    /** DML root context ownership follows HintCollector, not all hidden tokens. */
    void dmlHints(StatementBase statement, int anchor, boolean attachRoot) {
        if (tokens instanceof DenseOwnedTokenStream dense && dense.allHints().isEmpty()) {
            return;
        }
        List<Token> hints = tokens.getHiddenTokensToRight(anchor, 2);
        if (hints != null && !hints.isEmpty()) {
            pendingHints.add(new PendingHints(null, attachRoot ? statement : null, hints));
        }
    }

    /** Match HintCollector traversal, and only evaluate hints after syntactic acceptance. */
    private void attachHints(StatementBase statement) {
        List<HintNode> queryHints = new ArrayList<>();
        boolean any = false;
        for (PendingHints pending : pendingHints) {
            List<HintNode> nodes = new ArrayList<>();
            for (Token token : pending.tokens()) {
                HintNode node = HintFactory.buildHintNode(token, session);
                if (node == null) {
                    continue;
                }
                // AstBuilder resolves this before constructing expressions. Preserve its original
                // path.
                if (node instanceof SetVarHint set && set.getValue().containsKey("sql_mode")) {
                    throw unsupported("hint SQL mode requires original builder");
                }
                nodes.add(node);
                if (node.getScope() == HintNode.Scope.QUERY) {
                    queryHints.add(node);
                }
            }
            if (!nodes.isEmpty()) {
                if (pending.select() != null) {
                    pending.select().setHintNodes(nodes);
                } else if (pending.statement() instanceof InsertStmt insert) {
                    insert.setHintNodes(nodes);
                } else if (pending.statement() instanceof UpdateStmt update) {
                    update.setHintNodes(nodes);
                } else if (pending.statement() instanceof DeleteStmt delete) {
                    delete.setHintNodes(nodes);
                }
                any = true;
            }
        }
        if (any) {
            Collections.sort(queryHints);
            statement.setAllQueryScopeHints(queryHints);
        }
    }

    private int type() {
        return tokens.LA(1);
    }

    private Token take() {
        Token t = tokens.LT(1);
        if (t.getType() == Token.EOF) {
            throw unsupported("unexpected EOF");
        }
        tokens.consume();
        last = t.getTokenIndex();
        return t;
    }

    private boolean eat(int t) {
        if (type() != t) {
            return false;
        }
        take();
        return true;
    }

    private void expect(int t) {
        if (!eat(t)) {
            throw unsupported("expected " + FrozenTokenNames.lexerDisplayName(t));
        }
    }

    private UnsupportedQuery unsupported(String reason) {
        return new UnsupportedQuery(
                reason
                        + " at raw="
                        + tokens.index()
                        + " type="
                        + FrozenTokenNames.lexerDisplayName(type()));
    }

    private NodePosition pos(int start) {
        return new NodePosition(tokens.get(start), tokens.get(last));
    }

    private static boolean identifierType(int t) {
        return t >= 0 && t < IDENTIFIERS.length && IDENTIFIERS[t];
    }

    String identifier() {
        if (!identifierType(type())) {
            throw unsupported("identifier");
        }
        Token t = take();
        String s = t.getText();
        return t.getType() == BACKQUOTED_IDENTIFIER ? QueryIdentifiers.decodeBackQuoted(s) : s;
    }

    private String normalized(String s) {
        return caseInsensitive ? s.toLowerCase(Locale.ROOT) : s;
    }

    Expr expression() {
        Expr expr =
                DirectExpressionParser.eager(
                                tokens, mode, this::expressionSubquery, budget, parameters)
                        .parsePrefix();
        last = tokens.LT(-1).getTokenIndex();
        return expr;
    }

    private List<String> columnAliases() {
        if (!eat(OPEN)) {
            return null;
        }
        List<String> names = new ArrayList<>();
        names.add(identifier().toLowerCase(Locale.ROOT));
        while (eat(COMMA)) {
            names.add(identifier().toLowerCase(Locale.ROOT));
        }
        expect(CLOSE);
        return names;
    }

    QueryRelation expressionSubquery() {
        int previousLast = last;
        try {
            return queryRelationWithoutHints();
        } finally {
            last = previousLast;
        }
    }

    private QueryRelation queryRelation() {
        budget.enterQuery();
        try {
            return queryRelationAfterPrefix(ctePrefix());
        } finally {
            budget.exitQuery();
        }
    }

    private record CtePrefix(List<CTERelation> ctes, boolean recursive) {}

    private CtePrefix ctePrefix() {
        if (!eat(WITH)) {
            return null;
        }
        List<CTERelation> ctes = new ArrayList<>();
        boolean recursive = false;
        if (type() == RECURSIVE && tokens.LA(2) != AS && tokens.LA(2) != OPEN) {
            take();
            recursive = true;
        }
        do {
            String name = normalized(identifier());
            List<String> columns = columnAliases();
            expect(AS);
            expect(OPEN);
            QueryRelation inner = queryRelationWithoutHints();
            expect(CLOSE);
            ctes.add(
                    new CTERelation(
                            RelationId.of(inner).hashCode(),
                            name,
                            columns,
                            new QueryStatement(inner),
                            false,
                            true,
                            inner.getPos()));
        } while (eat(COMMA));
        return new CtePrefix(ctes, recursive);
    }

    private QueryRelation queryRelationAfterPrefix(CtePrefix prefix) {
        QueryRelation result = queryNoWith();
        result.setHasRecursiveCTE(prefix != null && prefix.recursive());
        if (prefix != null) {
            for (CTERelation cte : prefix.ctes()) {
                result.addCTERelation(cte);
            }
        }
        return result;
    }

    PartitionRef dmlReplayPartitions(int begin, int end) {
        int cursor = tokens.index();
        int previousLast = last;
        tokens.seek(begin);
        try {
            PartitionRef result = tablePartitions();
            if (tokens.index() != end) {
                throw unsupported("partition span boundary");
            }
            return result;
        } finally {
            tokens.seek(cursor);
            last = previousLast;
        }
    }

    /** Consumes an INTERVAL literal; the caller replays the span later to build the value. */
    void skipIntervalLiteral() {
        DirectExpressionParser.eager(tokens, mode, this::expressionSubquery, budget, parameters)
                .parseLiteralPrefix();
        last = tokens.LT(-1).getTokenIndex();
    }

    Map<String, String> dmlReplayProperties(int begin, int end, boolean insensitive) {
        int cursor = tokens.index();
        int previousLast = last;
        tokens.seek(begin);
        try {
            Map<String, String> result = dmlProperties(insensitive);
            if (tokens.index() != end) {
                throw unsupported("property span boundary");
            }
            return result;
        } finally {
            tokens.seek(cursor);
            last = previousLast;
        }
    }

    List<Relation> dmlRelations() {
        List<Relation> result = new ArrayList<>();
        try {
            result.add(relation());
            while (eat(COMMA)) {
                eat(LATERAL);
                result.add(relation());
            }
            return result;
        } finally {
            last = tokens.LT(-1).getTokenIndex();
        }
    }

    QueryStatement insertSourceQuery() {
        QueryPrefix prefix = queryPrefix();
        // AstBuilder rejects EXPLAIN, TRACE and INTO OUTFILE in an embedded query, so ANTLR reports the error.
        if (prefix.explain() != null || prefix.traceMode() != null) {
            throw unsupported("EXPLAIN or TRACE in an embedded query");
        }
        int start = tokens.LT(1).getStartIndex();
        QueryStatement result = new QueryStatement(queryRelation());
        result.setQueryStartIndex(start);
        if (type() == INTO) {
            throw unsupported("INTO OUTFILE in an embedded query");
        }
        return result;
    }

    Expr dmlDefault() {
        if (type() == DEFAULT) {
            int start = tokens.index();
            take();
            return new DefaultValueExpr(pos(start));
        }
        return expression();
    }

    Map<String, String> dmlProperties(boolean insensitive) {
        Map<String, String> result =
                insensitive ? new TreeMap<>(String.CASE_INSENSITIVE_ORDER) : new HashMap<>();
        expect(OPEN);
        do {
            String key = stringToken().trim();
            expect(EQUAL);
            // AstBuilder rejects a repeated key, so ANTLR reports the error.
            if (result.put(key, stringToken()) != null) {
                throw unsupported("duplicate property key");
            }
        } while (eat(COMMA));
        expect(CLOSE);
        return result;
    }

    private QueryRelation queryNoWith() {
        QueryRelation result = querySetOperation(1);
        List<OrderByElement> order = new ArrayList<>();
        if (eat(ORDER)) {
            expect(BY);
            do {
                order.add(sortItem());
            } while (eat(COMMA));
        }
        LimitElement limit = limitElement();
        result.setOrderBy(order);
        result.setLimit(limit);
        return result;
    }

    private static int setPrecedence(int operator) {
        return operator == INTERSECT
                ? 2
                : operator == UNION || operator == EXCEPT || operator == MINUS ? 1 : 0;
    }

    private QueryRelation querySetOperation(int minimum) {
        int start = tokens.index();
        QueryRelation left = queryPrimary();
        while (true) {
            int operator = type();
            int precedence = setPrecedence(operator);
            if (precedence < minimum) {
                break;
            }
            take();
            SetQualifier qualifier = SetQualifier.DISTINCT;
            if (eat(ALL)) {
                qualifier = SetQualifier.ALL;
            } else {
                eat(DISTINCT);
            }
            // The pinned grammar is left associative, with INTERSECT above the
            // UNION/EXCEPT/MINUS level. There are only two recursive right levels.
            QueryRelation right = querySetOperation(precedence + 1);
            if (operator == UNION) {
                if (left instanceof UnionRelation union && union.getQualifier() == qualifier) {
                    union.addRelation(right);
                } else {
                    left =
                            new UnionRelation(
                                    new ArrayList<>(List.of(left, right)), qualifier, pos(start));
                }
            } else if (operator == INTERSECT) {
                if (left instanceof IntersectRelation intersect
                        && intersect.getQualifier() == qualifier) {
                    intersect.addRelation(right);
                } else {
                    left =
                            new IntersectRelation(
                                    new ArrayList<>(List.of(left, right)), qualifier, pos(start));
                }
            } else {
                // EXCEPT and MINUS share the AstBuilder's ExceptRelation path.
                if (left instanceof ExceptRelation except && except.getQualifier() == qualifier) {
                    except.addRelation(right);
                } else {
                    left =
                            new ExceptRelation(
                                    new ArrayList<>(List.of(left, right)), qualifier, pos(start));
                }
            }
            // Flattening deliberately retains the left node's original position,
            // just as visitSetOperation does; it does not extend that position.
        }
        return left;
    }

    LimitElement limitElement() {
        LimitElement limit = null;
        if (type() == LIMIT) {
            int begin = tokens.index();
            take();
            Expr first = limitValue();
            Expr offset = new IntLiteral(0);
            Expr count = first;
            if (eat(COMMA)) {
                offset = first;
                count = limitValue();
            } else if (eat(OFFSET)) {
                offset = limitValue();
            }
            limit = new LimitElement(offset, count, pos(begin));
        }
        return limit;
    }

    int lastConsumedIndex() {
        return last;
    }

    private Expr limitValue() {
        if (type() == INTEGER_VALUE) {
            return new IntLiteral(Long.parseLong(take().getText()));
        }
        int start = tokens.index();
        if (eat(AT)) {
            String name = identifierType(type()) ? identifier() : stringToken();
            return new UserVariableExpr(name, pos(start));
        }
        // Parameters and arbitrary expressions are not accepted by the original LIMIT builder.
        throw unsupported("limit constant or user variable");
    }

    private QueryRelation queryPrimary() {
        if (type() == SELECT) {
            return select();
        }
        if (eat(OPEN)) {
            QueryRelation result = queryRelation();
            expect(CLOSE);
            return new SubqueryRelation(new QueryStatement(result));
        }
        throw unsupported("query primary");
    }

    private SelectRelation select() {
        int start = tokens.index();
        expect(SELECT);
        boolean distinct = eat(DISTINCT);
        if (!distinct) {
            eat(ALL);
        }
        List<SelectListItem> items = new ArrayList<>();
        do {
            items.add(selectItem());
        } while (eat(COMMA));
        Relation from = null;
        boolean explicitDual = false;
        if (eat(FROM)) {
            if (eat(DUAL)) {
                explicitDual = true;
                for (SelectListItem item : items) {
                    if (item.isStar()) {
                        throw unsupported("FROM DUAL star requires original error");
                    }
                }
            } else {
                from = relations();
                if (type() == PIVOT) {
                    from = pivot(from);
                }
            }
        }
        if (from == null) {
            from = ValuesRelation.newDualRelation();
        }
        Expr where = null;
        Expr having = null;
        GroupByClause group = null;
        if (eat(WHERE)) {
            where = expression();
        }
        if (eat(GROUP)) {
            expect(BY);
            group = groupingElement();
        }
        if (eat(HAVING)) {
            having = expression();
        }
        SelectListItem qualifyItem = null;
        BinaryType qualifyOperator = null;
        long qualifyLimit = 0;
        if (eat(QUALIFY)) {
            int endpoint = qualifyEndpoint();
            // Fresh bounded stream identity prevents interval cache decisions made
            // on a different endpoint from being reused. Callback queries use it too.
            BoundedExpressionTokenStream bounded =
                    new BoundedExpressionTokenStream(tokens, tokens.index(), endpoint);
            DirectQueryParser child = new DirectQueryParser(this, bounded);
            qualifyItem = child.boundedSelectItem();
            tokens.seek(endpoint);
            qualifyOperator = comparisonType(take().getType());
            if (type() != INTEGER_VALUE) {
                throw unsupported("QUALIFY integer limit");
            }
            qualifyLimit = Long.parseLong(take().getText());
            items.add(qualifyItem);
            if (explicitDual && qualifyItem.isStar()) {
                throw unsupported("FROM DUAL QUALIFY star requires original error");
            }
        }
        SelectList select = new SelectList(items, distinct);
        if (collectHints) {
            List<Token> hints = tokens.getHiddenTokensToRight(start, 2);
            if (hints != null && !hints.isEmpty()) {
                pendingHints.add(new PendingHints(select, null, hints));
            }
        }
        SelectRelation inner = new SelectRelation(select, from, where, group, having, pos(start));
        if (qualifyItem == null) {
            return inner;
        }
        inner.setOrderBy(new ArrayList<>());
        SubqueryRelation subquery = new SubqueryRelation(new QueryStatement(inner));
        TableName table = new TableName(null, "__QUALIFY__TABLE");
        subquery.setAlias(table);
        qualifyItem.setAlias("__QUALIFY__VALUE");
        List<SelectListItem> outerItems = new ArrayList<>();
        for (int i = 0; i < items.size() - 1; i++) {
            SelectListItem item = items.get(i);
            if (!(item.getExpr() instanceof SlotRef slot)) {
                throw unsupported("QUALIFY noncolumn result requires original builder error");
            }
            String name = item.getAlias() == null ? slot.getColumnName() : item.getAlias();
            outerItems.add(new SelectListItem(new SlotRef(table, name), null));
        }
        return new SelectRelation(
                new SelectList(outerItems, distinct),
                subquery,
                new BinaryPredicate(
                        qualifyOperator,
                        new SlotRef(table, "__QUALIFY__VALUE"),
                        new IntLiteral(qualifyLimit)),
                null,
                null,
                pos(start));
    }

    private SelectListItem boundedSelectItem() {
        int start = tokens.index();
        SelectListItem item = selectItem();
        // In a bounded item these are unambiguous aliases, not query followers.
        if (!item.isStar() && identifierType(type()) && queryContinuation(type())) {
            item = new SelectListItem(item.getExpr(), identifier(), pos(start));
        }
        if (type() != Token.EOF) {
            throw unsupported("QUALIFY item has unconsumed syntax");
        }
        return item;
    }

    private static BinaryType comparisonType(int type) {
        return switch (type) {
            case EQ -> BinaryType.EQ;
            case NEQ -> BinaryType.NE;
            case LT -> BinaryType.LT;
            case LTE -> BinaryType.LE;
            case GT -> BinaryType.GT;
            case GTE -> BinaryType.GE;
            case EQ_FOR_NULL -> BinaryType.EQ_FOR_NULL;
            default -> null;
        };
    }

    private int nextDefault(RawTokenAccess raw, int index) {
        int end = tokens.size() - 1;
        while (index < end && raw.channelAt(index) != Token.DEFAULT_CHANNEL) {
            index++;
        }
        return Math.min(index, end);
    }

    private boolean qualifyFollower(RawTokenAccess raw, int index) {
        int type = raw.typeAt(index);
        if (type == Token.EOF || type == CLOSE || type == SEMICOLON) {
            return true;
        }
        int next = nextDefault(raw, index + 1);
        int nextType = raw.typeAt(next);
        if (type == ORDER) {
            return nextType == BY;
        }
        if (type == LIMIT) {
            return nextType == INTEGER_VALUE || nextType == PARAMETER || nextType == AT;
        }
        if (type == INTO) {
            return nextType == OUTFILE;
        }
        if (type != UNION && type != INTERSECT && type != EXCEPT && type != MINUS) {
            return false;
        }
        if (nextType == ALL || nextType == DISTINCT) {
            next = nextDefault(raw, next + 1);
            nextType = raw.typeAt(next);
        }
        if (nextType == SELECT) {
            return true;
        }
        // Bare WITH is not queryPrimary. Parenthesized WITH is queryRelation.
        if (nextType != OPEN) {
            return false;
        }
        do {
            next = nextDefault(raw, next + 1);
            nextType = raw.typeAt(next);
        } while (nextType == OPEN);
        return nextType == SELECT || nextType == WITH;
    }

    private int qualifyEndpoint() {
        RawTokenAccess raw = RawTokenAccess.of(tokens);
        int braceOpen = FrozenTokenCatalog.T_8;
        int braceClose = FrozenTokenCatalog.T_9;
        int[] expected = new int[16];
        int depth = 0;
        // No CASE/END lexical stack: END is itself a legal column identifier.
        // Parentheses/brackets/braces protect expression and subquery internals.
        for (int index = tokens.index(); index < tokens.size(); index++) {
            if (raw.channelAt(index) != Token.DEFAULT_CHANNEL) {
                continue;
            }
            int type = raw.typeAt(index);
            if (depth == 0 && comparisonType(type) != null) {
                int integer = nextDefault(raw, index + 1);
                if (raw.typeAt(integer) == INTEGER_VALUE
                        && qualifyFollower(raw, nextDefault(raw, integer + 1))) {
                    return index;
                }
            }
            if (type == OPEN || type == OPEN_BRACKET || type == braceOpen) {
                if (depth == expected.length) {
                    expected = Arrays.copyOf(expected, expected.length * 2);
                }
                expected[depth++] =
                        type == OPEN ? CLOSE : type == OPEN_BRACKET ? CLOSE_BRACKET : braceClose;
            } else if (type == CLOSE || type == CLOSE_BRACKET || type == braceClose) {
                if (depth == 0 || expected[--depth] != type) {
                    throw unsupported("QUALIFY delimiter boundary");
                }
            } else if (type == Token.EOF || depth == 0 && type == SEMICOLON) {
                break;
            }
        }
        throw unsupported("QUALIFY comparator/integer endpoint");
    }

    private GroupByClause groupingElement() {
        int start = tokens.index();
        if (eat(GROUPING)) {
            expect(SETS);
            expect(OPEN);
            List<ArrayList<Expr>> sets = new ArrayList<>();
            do {
                expect(OPEN);
                ArrayList<Expr> entries = new ArrayList<>();
                if (type() != CLOSE && type() != COMMA) {
                    entries.add(expression());
                }
                while (eat(COMMA)) {
                    entries.add(expression());
                }
                expect(CLOSE);
                sets.add(entries);
            } while (eat(COMMA));
            expect(CLOSE);
            return new GroupByClause(sets, GroupByClause.GroupingType.GROUPING_SETS, pos(start));
        }
        GroupByClause.GroupingType grouping = GroupByClause.GroupingType.GROUP_BY;
        boolean wrapped = type() == ROLLUP || type() == CUBE;
        if (wrapped) {
            grouping =
                    type() == ROLLUP
                            ? GroupByClause.GroupingType.ROLLUP
                            : GroupByClause.GroupingType.CUBE;
            take();
            expect(OPEN);
        }
        // Generated parsing must finish before the corrected builder rejects empty
        // ROLLUP/CUBE. Rolling back retains malformed-tail syntax precedence and
        // earlier SELECT/FROM/WHERE/ORDER constructor failures.
        if (wrapped && type() == CLOSE) {
            throw unsupported("empty grouping requires deferred reference validation");
        }
        ArrayList<Expr> entries = new ArrayList<>();
        entries.add(expression());
        while (eat(COMMA)) {
            entries.add(expression());
        }
        if (wrapped) {
            expect(CLOSE);
        }
        return new GroupByClause(entries, grouping, pos(start));
    }

    private SelectListItem selectItem() {
        int start = tokens.index();
        int savedLast = last;
        TableName wildcard = null;
        boolean star = false;
        if (eat(ASTERISK_SYMBOL)) {
            star = true;
        } else if (identifierType(type())) {
            int saved = tokens.index();
            List<String> parts = new ArrayList<>();
            parts.add(identifier());
            int end = last;
            while (type() == DOT_IDENTIFIER || (type() == DOT && identifierType(tokens.LA(2)))) {
                if (type() == DOT_IDENTIFIER) {
                    parts.add(take().getText().substring(1));
                } else {
                    take();
                    parts.add(identifier());
                }
                end = last;
            }
            if (eat(DOT) && eat(ASTERISK_SYMBOL)) {
                star = true;
                wildcard = tableName(parts, new NodePosition(tokens.get(start), tokens.get(end)));
            } else {
                tokens.seek(saved);
                last = savedLast;
            }
        }
        if (star) {
            List<String> excluded = new ArrayList<>();
            if (type() == EXCEPT || type() == EXCLUDE) {
                take();
                expect(OPEN);
                excluded.add(identifier());
                while (eat(COMMA)) {
                    excluded.add(identifier());
                }
                expect(CLOSE);
            }
            return new SelectListItem(wildcard, pos(start), excluded);
        }
        Expr expr = expression();
        String alias = null;
        if (eat(AS)) {
            alias = alias();
        } else if (identifierType(type())
                && !queryContinuation(type())
                && !baselinePropertiesFollower()) {
            alias = identifier();
        } else if (type() == SINGLE_QUOTED_TEXT || type() == DOUBLE_QUOTED_TEXT) {
            alias = stringValue(take().getText());
        }
        return new SelectListItem(expr, alias, pos(start));
    }

    private String alias() {
        if (type() == SINGLE_QUOTED_TEXT || type() == DOUBLE_QUOTED_TEXT) {
            return stringValue(take().getText());
        }
        return identifier();
    }

    private static boolean queryContinuation(int t) {
        return t == QUALIFY || t == EXCEPT || t == MINUS || t == INTERSECT;
    }

    private Relation pivot(Relation from) {
        int start = tokens.index();
        expect(PIVOT);
        expect(OPEN);
        List<PivotAggregation> aggregations = new ArrayList<>();
        do {
            int aggregationStart = tokens.index();
            Expr expression =
                    DirectExpressionParser.eager(
                                    tokens, mode, this::expressionSubquery, budget, parameters)
                            .parseFunctionCallPrefix();
            last = tokens.LT(-1).getTokenIndex();
            NodePosition functionPos =
                    new NodePosition(tokens.get(aggregationStart), tokens.get(last));
            String name = null;
            if (eat(AS)) {
                name = alias();
            } else if (identifierType(type())
                    || type() == SINGLE_QUOTED_TEXT
                    || type() == DOUBLE_QUOTED_TEXT) {
                name = alias();
            }
            if (!(expression instanceof FunctionCallExpr function)) {
                throw new ParsingException(
                        "Measure expression in PIVOT must use aggregate function", functionPos);
            }
            aggregations.add(new PivotAggregation(function, name, pos(aggregationStart)));
        } while (eat(COMMA));
        expect(FOR);
        List<String> names = new ArrayList<>();
        if (eat(OPEN)) {
            names.add(identifier());
            while (eat(COMMA)) {
                names.add(identifier());
            }
            expect(CLOSE);
        } else {
            names.add(identifier());
        }
        expect(IN);
        expect(OPEN);
        List<PivotValue> values = new ArrayList<>();
        do {
            int valueStart = tokens.index();
            ImmutableList.Builder<LiteralExpr> literals = ImmutableList.builder();
            if (eat(OPEN)) {
                pivotLiteral(literals);
                while (eat(COMMA)) {
                    pivotLiteral(literals);
                }
                expect(CLOSE);
            } else {
                pivotLiteral(literals);
            }
            String name = null;
            if (eat(AS)) {
                name = alias();
            } else if (identifierType(type())
                    || type() == SINGLE_QUOTED_TEXT
                    || type() == DOUBLE_QUOTED_TEXT) {
                name = alias();
            }
            values.add(new PivotValue(literals.build(), name, pos(valueStart)));
        } while (eat(COMMA));
        expect(CLOSE);
        expect(CLOSE);
        for (PivotValue value : values) {
            if (value.getExprs().size() != names.size()) {
                throw unsupported("PIVOT arity requires original error");
            }
        }
        List<SlotRef> columns = new ArrayList<>();
        for (String name : names) {
            columns.add(new SlotRef(QualifiedName.of(ImmutableList.of(name), pos(start))));
        }
        PivotRelation result = new PivotRelation(null, aggregations, columns, values, pos(start));
        result.setQuery(from);
        return result;
    }

    private void pivotLiteral(ImmutableList.Builder<LiteralExpr> literals) {
        Expr result =
                DirectExpressionParser.eager(
                                tokens, mode, this::expressionSubquery, budget, parameters)
                        .parseLiteralPrefix();
        last = tokens.LT(-1).getTokenIndex();
        if (!(result instanceof LiteralExpr literal)) {
            throw unsupported("PIVOT requires original literal cast");
        }
        literals.add(literal);
    }

    private Relation relations() {
        Relation result = relation();
        while (eat(COMMA)) {
            eat(LATERAL);
            result = new JoinRelation(null, result, relation(), null, false);
        }
        return result;
    }

    private Relation relation() {
        Relation left = relationPrimary();
        while (joinStart(type())) {
            int start = tokens.index();
            JoinOperator op;
            if (eat(ASOF)) {
                if (eat(LEFT)) {
                    eat(OUTER);
                    op = JoinOperator.ASOF_LEFT_OUTER_JOIN;
                } else {
                    eat(INNER);
                    op = JoinOperator.ASOF_INNER_JOIN;
                }
                expect(JOIN);
            } else if (eat(JOIN)) {
                op = JoinOperator.INNER_JOIN;
            } else if (eat(INNER)) {
                expect(JOIN);
                op = JoinOperator.INNER_JOIN;
            } else if (eat(CROSS)) {
                eat(JOIN);
                op = JoinOperator.CROSS_JOIN;
            } else if (eat(NULL)) {
                expect(AWARE);
                expect(LEFT);
                expect(ANTI);
                expect(JOIN);
                op = JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN;
            } else {
                int direction = take().getType();
                boolean semi = eat(SEMI);
                boolean anti = !semi && eat(ANTI);
                if (!semi && !anti) {
                    eat(OUTER);
                }
                expect(JOIN);
                if (direction == FULL) {
                    if (semi || anti) {
                        throw unsupported("FULL semi/anti");
                    }
                    op = JoinOperator.FULL_OUTER_JOIN;
                } else if (direction == LEFT) {
                    op =
                            semi
                                    ? JoinOperator.LEFT_SEMI_JOIN
                                    : anti
                                            ? JoinOperator.LEFT_ANTI_JOIN
                                            : JoinOperator.LEFT_OUTER_JOIN;
                } else {
                    op =
                            semi
                                    ? JoinOperator.RIGHT_SEMI_JOIN
                                    : anti
                                            ? JoinOperator.RIGHT_ANTI_JOIN
                                            : JoinOperator.RIGHT_OUTER_JOIN;
                }
            }
            String joinHint = null;
            SkewParts skew = null;
            if (eat(OPEN_BRACKET)) {
                joinHint = identifier();
                if (eat(BITOR)) {
                    skew = skewBoundaries();
                    tokens.seek(skew.hintEnd());
                    expect(CLOSE_BRACKET);
                } else {
                    while (eat(COMMA)) {
                        identifier();
                    }
                    expect(CLOSE_BRACKET);
                }
            }
            boolean lateral = eat(LATERAL);
            if (lateral
                    && (op == JoinOperator.ASOF_INNER_JOIN
                            || op == JoinOperator.ASOF_LEFT_OUTER_JOIN)) {
                throw unsupported("ASOF does not permit LATERAL");
            }
            Relation right;
            Expr on = null;
            List<String> using = null;
            try {
                right = relationPrimaryWithoutHints();
                if (eat(ON)) {
                    on = expression();
                } else if (eat(USING)) {
                    expect(OPEN);
                    using = new ArrayList<>();
                    using.add(identifier());
                    while (eat(COMMA)) {
                        using.add(identifier());
                    }
                    expect(CLOSE);
                } else if (op != JoinOperator.INNER_JOIN && op != JoinOperator.CROSS_JOIN) {
                    throw unsupported("join criteria required");
                }
            } catch (ParsingException | IllegalArgumentException | ArithmeticException userError) {
                // Reference validates the full grammar before RIGHT/ON constructors.
                // The skipped hint may contain bad syntax: defer audited user errors.
                if (skew != null) {
                    throw unsupported("skew hint requires original RIGHT/ON error order");
                }
                throw userError;
            }
            JoinRelation join = new JoinRelation(op, left, right, on, lateral, pos(start));
            join.setUsingColNames(using);
            if (joinHint != null) {
                join.setJoinHint(joinHint);
            }
            if (skew != null) {
                attachSkew(join, skew);
            }
            left = join;
        }
        return left;
    }

    private record SkewParts(int primaryStart, int valuesStart, int hintEnd) {}

    /** Hint-local delimiter scan; the final outer (...) is the literal-list suffix. */
    private SkewParts skewBoundaries() {
        int primaryStart = tokens.index();
        int previous = -1;
        int groupOpen = -1;
        int groupClose = -1;
        int braceOpen = FrozenTokenCatalog.T_8;
        int braceClose = FrozenTokenCatalog.T_9;
        int[] kinds = new int[16];
        int[] opens = new int[16];
        int depth = 0;
        RawTokenAccess raw = RawTokenAccess.of(tokens);
        for (int i = primaryStart; i < tokens.size(); i = nextDefault(raw, i + 1)) {
            budget.chargeSkewScan(tokens.size());
            int t = raw.typeAt(i);
            if (t == Token.EOF) {
                throw unsupported("unclosed skew hint");
            }
            if (t == CLOSE_BRACKET && depth == 0) {
                if (groupClose != previous || groupOpen <= primaryStart) {
                    throw unsupported("skew hint requires final literal list");
                }
                return new SkewParts(primaryStart, groupOpen, i);
            }
            if (t == OPEN || t == OPEN_BRACKET || t == braceOpen) {
                if (depth == kinds.length) {
                    kinds = Arrays.copyOf(kinds, depth * 2);
                    opens = Arrays.copyOf(opens, depth * 2);
                }
                kinds[depth] = t;
                opens[depth++] = i;
            } else if (t == CLOSE || t == CLOSE_BRACKET || t == braceClose) {
                if (depth == 0) {
                    throw unsupported("skew hint delimiter mismatch");
                }
                int kind = kinds[--depth];
                int open = opens[depth];
                int expected =
                        kind == OPEN ? CLOSE : kind == OPEN_BRACKET ? CLOSE_BRACKET : braceClose;
                if (t != expected) {
                    throw unsupported("skew hint delimiter mismatch");
                }
                if (depth == 0 && kind == OPEN) {
                    groupOpen = open;
                    groupClose = i;
                }
            }
            previous = i;
        }
        throw unsupported("unclosed skew hint");
    }

    private List<Expr> boundedSkewValues() {
        expect(OPEN);
        List<Expr> values = new ArrayList<>();
        do {
            Expr value =
                    DirectExpressionParser.eager(
                                    tokens, mode, this::expressionSubquery, budget, parameters)
                            .parseGeneralLiteralPrefix();
            last = tokens.LT(-1).getTokenIndex();
            values.add(value);
        } while (eat(COMMA));
        expect(CLOSE);
        if (type() != Token.EOF) {
            throw unsupported("skew values have unconsumed syntax");
        }
        return values;
    }

    /** AstBuilder visits RIGHT, ON, skew primary, then values in that order. */
    private void attachSkew(JoinRelation join, SkewParts skew) {
        int saved = tokens.index();
        int previousLast = last;
        try {
            BoundedExpressionTokenStream primary =
                    new BoundedExpressionTokenStream(
                            tokens, skew.primaryStart(), skew.valuesStart());
            DirectQueryParser primaryChild = new DirectQueryParser(this, primary);
            join.setSkewColumn(
                    DirectExpressionParser.eager(
                                    primary,
                                    mode,
                                    primaryChild::expressionSubquery,
                                    budget,
                                    parameters)
                            .parseBoundedPrimary());
            BoundedExpressionTokenStream values =
                    new BoundedExpressionTokenStream(tokens, skew.valuesStart(), skew.hintEnd());
            join.setSkewValues(new DirectQueryParser(this, values).boundedSkewValues());
        } catch (ParsingException | IllegalArgumentException | ArithmeticException userError) {
            throw unsupported("skew hint requires original constructor error order");
        } finally {
            tokens.seek(saved);
            last = previousLast;
        }
    }

    private static boolean joinStart(int t) {
        return t == ASOF || t == JOIN || t == INNER || t == CROSS || t == LEFT || t == RIGHT
                || t == FULL || t == NULL;
    }

    private SubqueryRelation subqueryRelation(boolean assertRows) {
        expect(OPEN);
        QueryRelation inner = queryRelation();
        expect(CLOSE);
        QueryStatement qs = new QueryStatement(inner);
        SubqueryRelation result = new SubqueryRelation(qs, assertRows, qs.getPos());
        String alias = null;
        if (eat(AS)) {
            alias = identifier();
        } else if (identifierType(type()) && !relationContinuation()) {
            alias = identifier();
        }
        result.setAlias(new TableName(null, alias));
        result.setColumnOutputNames(columnAliases());
        return result;
    }

    /** Returns null when ASSERT_ROWS and the parenthesis start a table function instead. */
    private SubqueryRelation assertRowsSubquery() {
        if (!queryInParentheses(2)) {
            return null;
        }
        if (tokens.LA(3) == OPEN) {
            // ANTLR prefers the subquery when ASSERT_ROWS((SELECT 1)) also parses as a table function with a
            // scalar subquery argument, but ASSERT_ROWS((SELECT 1), 2) can only be the table function.
            int saved = tokens.index();
            int previousLast = last;
            int hints = pendingHints.size();
            take();
            expect(OPEN);
            queryRelation();
            boolean subquery = type() == CLOSE;
            tokens.seek(saved);
            last = previousLast;
            pendingHints.subList(hints, pendingHints.size()).clear();
            if (!subquery) {
                return null;
            }
        }
        take();
        return subqueryRelation(true);
    }

    private Relation relationPrimary() {
        int start = tokens.index();
        if (eat(FILES)) {
            Map<String, String> properties = dmlProperties(true);
            String alias = null;
            if (eat(AS)) {
                alias = identifier();
            } else if (identifierType(type()) && !relationContinuation()) {
                alias = identifier();
            }
            // AstBuilder rejects column aliases of FILES(), so ANTLR reports the error.
            if (alias != null && type() == OPEN) {
                throw unsupported("FILES() column aliases");
            }
            FileTableFunctionRelation files = new FileTableFunctionRelation(properties, NodePosition.ZERO);
            if (alias != null) {
                files.setAlias(new TableName(null, alias));
            }
            return files;
        }
        if (eat(TABLE)) {
            expect(OPEN);
            List<String> parts = qualifiedName();
            List<Expr> args = tableFunctionArguments(true);
            expect(CLOSE);
            return tableFunctionRelation(start, parts, args, true);
        }
        if (type() == OPEN && tokens.LA(2) == VALUES) {
            return inlineValues();
        }
        if (type() == OPEN) {
            if (queryInParentheses()) {
                return subqueryRelation(false);
            }
            take();
            boolean previous = collectHints;
            collectHints = false;
            Relation relation;
            try {
                relation = relations();
            } finally {
                collectHints = previous;
            }
            expect(CLOSE);
            return relation;
        }
        if (!identifierType(type())) {
            throw unsupported("relation primary");
        }
        if (type() == ASSERT_ROWS && tokens.LA(2) == OPEN) {
            SubqueryRelation asserted = assertRowsSubquery();
            if (asserted != null) {
                return asserted;
            }
        }
        List<String> parts = qualifiedName();
        if (type() == OPEN) {
            return tableFunctionRelation(start, parts, tableFunctionArguments(false), false);
        }
        NodePosition namePos = pos(start);
        String alias = null;
        PeriodParts period = null;
        if (type() == FOR || type() == SYSTEM_TIME || type() == TIMESTAMP || type() == VERSION) {
            period = tablePeriod();
        }
        PartitionRef partitions = null;
        int optionStop = -1;
        if (type() == PARTITION || type() == PARTITIONS || type() == TEMPORARY) {
            partitions = tablePartitions();
            optionStop = last;
        }
        List<Long> tablets = new ArrayList<>();
        List<Long> replicas = new ArrayList<>();
        if (type() == TABLET || type() == TABLETS) {
            take();
            tablets = tableIds(false);
            optionStop = last;
        }
        if (eat(REPLICA)) {
            replicas = tableIds(true);
            optionStop = last;
        }
        TableSampleClause sample = null;
        if (type() == SAMPLE) {
            int sampleStart = tokens.index();
            take();
            Map<String, String> properties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            if (eat(OPEN)) {
                do {
                    String key = stringToken().trim();
                    expect(EQUAL);
                    if (properties.put(key, stringToken()) != null) {
                        throw unsupported("duplicate property key");
                    }
                } while (eat(COMMA));
                expect(CLOSE);
            }
            sample = new TableSampleClause(pos(sampleStart));
            try {
                sample.analyzeProperties(properties);
            } catch (com.starrocks.common.AnalysisException e) {
                throw unsupported("sample properties require original error");
            }
        }
        if (eat(AS)) {
            alias = identifier();
        } else if (identifierType(type()) && !relationContinuation()) {
            alias = identifier();
        }
        List<String> hints = null;
        if (eat(OPEN_BRACKET)) {
            hints = new ArrayList<>();
            do {
                hints.add(identifier());
            } while (eat(COMMA));
            expect(CLOSE_BRACKET);
        }
        String before = null;
        if (eat(BEFORE)) {
            before = stringToken();
        }
        // The original builder deliberately ends this node at the last partition/tablet/replica
        // clause if present; otherwise its position includes the complete table atom.
        NodePosition tablePos =
                optionStop < 0
                        ? pos(start)
                        : new NodePosition(tokens.get(start), tokens.get(optionStop));
        TableRelation result =
                new TableRelation(
                        tableName(parts, namePos), partitions, tablets, replicas, tablePos);
        if (period != null) {
            result.setQueryPeriodString(period.text());
            if (period.value() != null) {
                result.setQueryPeriod(period.value());
            }
        }
        if (alias != null) {
            result.setAlias(new TableName(null, alias));
        }
        if (hints != null) {
            for (String hint : hints) {
                result.addTableHint(hint);
            }
        }
        if (sample != null) {
            result.setSampleClause(sample);
        }
        if (before != null) {
            try {
                result.setGtid(
                        com.starrocks.transaction.GtidGenerator.getGtid(
                                new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                                        .parse(before)
                                        .getTime()));
            } catch (java.text.ParseException e) {
                result.setGtid(Long.parseLong(before));
            }
        }
        return result;
    }

    private record PeriodParts(String text, QueryPeriod value) {}

    // Only AS OF is visited by the reference builder; the other forms keep just their text.
    private Expr periodExpression(boolean valueOnly, boolean visited) {
        try {
            DirectExpressionParser<
                            Expr,
                            QueryRelation,
                            com.starrocks.type.Type,
                            com.starrocks.type.StructField,
                            OrderByElement,
                            AnalyticWindow,
                            AnalyticWindowBoundary,
                            CaseWhenClause>
                    parser =
                            DirectExpressionParser.eager(
                                    tokens, mode, this::expressionSubquery, budget, parameters);
            if (!visited) {
                budget.enterIgnored();
            }
            Expr result;
            try {
                result = valueOnly ? parser.parseValuePrefix() : parser.parsePrefix();
            } finally {
                if (!visited) {
                    budget.exitIgnored();
                }
            }
            last = tokens.LT(-1).getTokenIndex();
            return result;
        } catch (ParsingException e) {
            // Non-AS-OF forms retain text without visiting their expression ASTs in the
            // reference builder. Let that path decide any constructor-specific failure.
            throw unsupported("query period expression requires original builder");
        }
    }

    private PeriodParts tablePeriod() {
        Token first = tokens.LT(1);
        eat(FOR);
        int periodType = type();
        if (periodType != SYSTEM_TIME && periodType != TIMESTAMP && periodType != VERSION) {
            throw unsupported("query period type");
        }
        take();
        QueryPeriod result = null;
        if (type() == AS) {
            take();
            if (type() != OF) {
                throw unsupported("query period AS OF");
            }
            take();
            Expr end = periodExpression(false, true);
            result =
                    new QueryPeriod(
                            periodType == VERSION
                                    ? QueryPeriod.PeriodType.VERSION
                                    : QueryPeriod.PeriodType.TIMESTAMP,
                            end);
        } else if (type() == ALL) {
            take();
        } else if (type() == BETWEEN || type() == FROM) {
            boolean between = type() == BETWEEN;
            take();
            periodExpression(between, false);
            int separator = between ? AND : TO;
            if (type() != separator) {
                throw unsupported("query period separator");
            }
            take();
            periodExpression(between, false);
        } else {
            throw unsupported("query period range");
        }
        String text = first.getInputStream()
                .getText(Interval.of(first.getStartIndex(), tokens.get(last).getStopIndex()));
        return new PeriodParts(text, result);
    }

    private List<Long> tableIds(boolean requireParentheses) {
        boolean wrapped = eat(OPEN);
        if (requireParentheses && !wrapped) {
            throw unsupported("replica parentheses");
        }
        List<Long> ids = new ArrayList<>();
        do {
            if (type() != INTEGER_VALUE) {
                throw unsupported("tablet/replica integer");
            }
            ids.add(Long.parseLong(take().getText()));
        } while (eat(COMMA));
        if (wrapped) {
            expect(CLOSE);
        }
        return ids;
    }

    IntervalLiteral adminInterval() {
        Expr value =
                DirectExpressionParser.eager(
                                tokens, mode, this::expressionSubquery, budget, parameters)
                        .parseLiteralPrefix();
        last = tokens.LT(-1).getTokenIndex();
        return (IntervalLiteral) value;
    }

    QualifiedName standaloneName(boolean normalize) {
        int begin = tokens.index();
        List<String> parts = qualifiedName();
        if (normalize) {
            parts.replaceAll(this::normalized);
        }
        return QualifiedName.of(parts, pos(begin));
    }

    String standaloneIdentifier() {
        return identifier();
    }

    SelectListItem standaloneSelectItem() {
        return selectItem();
    }

    Expr ddlPrimary() {
        Expr result =
                DirectExpressionParser.eager(
                                tokens, mode, this::expressionSubquery, budget, parameters)
                        .parsePrimaryPrefix();
        last = tokens.LT(-1).getTokenIndex();
        return result;
    }

    Expr ddlFunction() {
        Expr result =
                DirectExpressionParser.eager(
                                tokens, mode, this::expressionSubquery, budget, parameters)
                        .parseFunctionCallPrefix();
        last = tokens.LT(-1).getTokenIndex();
        return result;
    }

    QueryStatement viewQueryStatement() {
        QueryStatement result = insertSourceQuery();
        last = tokens.LT(-1).getTokenIndex();
        return result;
    }

    String standaloneIdentifierOrString() {
        return type() == SINGLE_QUOTED_TEXT || type() == DOUBLE_QUOTED_TEXT
                ? stringToken()
                : identifier();
    }

    java.util.function.Supplier<Expr> deferredDdlAtomicLiteral(int start, int endpoint) {
        return () -> {
            CommonTokenStream bounded = new BoundedExpressionTokenStream(tokens, start, endpoint);
            Expr result =
                    DirectExpressionParser.eager(
                                    bounded, mode, this::expressionSubquery, budget, parameters)
                            .parseLiteralPrefix();
            if (bounded.LA(1) != Token.EOF) {
                throw new UnsupportedQuery("deferred DDL atomic literal boundary");
            }
            return result;
        };
    }

    PartitionRef showPartitionNames() {
        return tablePartitions();
    }

    QueryRelation baselinePlanQueryRelation() {
        int saved = baselinePropertiesDepth;
        baselinePropertiesDepth = budget.queryDepth() + 1;
        try {
            return showQueryRelation();
        } finally {
            baselinePropertiesDepth = saved;
        }
    }

    private static boolean propertyString(int kind) {
        return kind == SINGLE_QUOTED_TEXT || kind == DOUBLE_QUOTED_TEXT;
    }

    /** Suppress only an optional implicit alias before a complete enclosing propertyList. */
    private boolean baselinePropertiesFollower() {
        if (type() != PROPERTIES
                || baselinePropertiesDepth < 0
                || budget.queryDepth() != baselinePropertiesDepth) {
            return false;
        }
        RawTokenAccess raw = RawTokenAccess.of(tokens);
        int cursor = nextDefault(raw, tokens.index() + 1);
        if (raw.typeAt(cursor) != OPEN) {
            return false;
        }
        cursor = nextDefault(raw, cursor + 1);
        for (; ; ) {
            if (!propertyString(raw.typeAt(cursor))) {
                return false;
            }
            cursor = nextDefault(raw, cursor + 1);
            if (raw.typeAt(cursor) != EQUAL) {
                return false;
            }
            cursor = nextDefault(raw, cursor + 1);
            if (!propertyString(raw.typeAt(cursor))) {
                return false;
            }
            cursor = nextDefault(raw, cursor + 1);
            if (raw.typeAt(cursor) == CLOSE) {
                return true;
            }
            if (raw.typeAt(cursor) != COMMA) {
                return false;
            }
            cursor = nextDefault(raw, cursor + 1);
        }
    }

    QueryRelation showQueryRelation() {
        QueryRelation result = queryRelationWithoutHints();
        last = tokens.LT(-1).getTokenIndex();
        return result;
    }

    private PartitionRef tablePartitions() {
        return tablePartitionDescriptor().get();
    }

    java.util.function.Supplier<PartitionRef> tablePartitionDescriptor() {
        return tablePartitionDescriptor(null);
    }

    boolean identifierToken(int tokenKind) {
        return identifierType(tokenKind);
    }

    java.util.function.Supplier<PartitionRef> tablePartitionDescriptor(
            java.util.function.BooleanSupplier commaStartsFollowingClause) {
        int start = tokens.index();
        boolean temporary = eat(TEMPORARY);
        boolean singular = eat(PARTITION);
        if (!singular) {
            expect(PARTITIONS);
        }
        boolean wrapped = eat(OPEN);
        if (singular && !temporary && wrapped && identifierType(type()) && tokens.LA(2) == EQUAL) {
            List<String> names = new ArrayList<>();
            List<Expr> values = new ArrayList<>();
            do {
                names.add(identifier());
                expect(EQUAL);
                Expr value =
                        DirectExpressionParser.eager(
                                        tokens, mode, this::expressionSubquery, budget, parameters)
                                .parseLiteralPrefix();
                last = tokens.LT(-1).getTokenIndex();
                values.add(value);
            } while (eat(COMMA));
            expect(CLOSE);
            return () ->
                    new PartitionRef(new ArrayList<>(), false, names, values, NodePosition.ZERO);
        }
        List<String> names = new ArrayList<>();
        do {
            names.add(
                    type() == SINGLE_QUOTED_TEXT || type() == DOUBLE_QUOTED_TEXT
                            ? stringToken()
                            : identifier());
        } while (type() == COMMA
                && (wrapped
                        || commaStartsFollowingClause == null
                        || !commaStartsFollowingClause.getAsBoolean())
                && eat(COMMA));
        if (wrapped) {
            expect(CLOSE);
        }
        NodePosition position = pos(start);
        return () -> new PartitionRef(names, temporary, position);
    }

    private List<Expr> tableFunctionArguments(boolean normalized) {
        expect(OPEN);
        List<Expr> args = new ArrayList<>();
        boolean named =
                normalized
                        && identifierType(type())
                        && (tokens.LA(2) == EQUAL || tokens.LA(2) == NAMED_ARROW);
        do {
            if (named) {
                String name = identifier();
                if (name.isEmpty() || name.equals(" ")) {
                    throw unsupported("empty named argument requires original error");
                }
                if (!eat(EQUAL)) {
                    expect(NAMED_ARROW);
                }
                args.add(new NamedArgument(name, expression()));
            } else {
                args.add(expression());
            }
        } while (eat(COMMA));
        expect(CLOSE);
        return args;
    }

    private Relation tableFunctionRelation(
            int start, List<String> parts, List<Expr> args, boolean normalized) {
        String alias = null;
        if (eat(AS)) {
            alias = identifier();
        } else if (identifierType(type()) && !relationContinuation()) {
            alias = identifier();
        }
        List<String> columns = alias == null ? null : columnAliases();
        String name = parts.size() == 1 ? parts.get(0) : String.join(".", parts);
        FunctionCallExpr function =
                normalized
                        ? new FunctionCallExpr(name, args, pos(start))
                        : new FunctionCallExpr(name, args);
        TableFunctionRelation relation = new TableFunctionRelation(function);
        if (alias != null) {
            relation.setAlias(new TableName(null, alias));
        }
        relation.setColumnOutputNames(columns);
        return normalized ? new NormalizedTableFunctionRelation(relation) : relation;
    }

    private ValuesRelation inlineValues() {
        int start = tokens.index();
        expect(OPEN);
        expect(VALUES);
        List<List<Expr>> rows = new ArrayList<>();
        do {
            expect(OPEN);
            List<Expr> row = new ArrayList<>();
            row.add(expression());
            while (eat(COMMA)) {
                row.add(expression());
            }
            expect(CLOSE);
            rows.add(row);
        } while (eat(COMMA));
        expect(CLOSE);
        String alias = null;
        if (eat(AS)) {
            alias = identifier();
        } else if (identifierType(type()) && !relationContinuation()) {
            alias = identifier();
        }
        List<String> names = alias == null ? null : columnAliases();
        if (names == null) {
            names = new ArrayList<>();
            for (int i = 0; i < rows.get(0).size(); i++) {
                names.add("column_" + i);
            }
        }
        ValuesRelation result = new ValuesRelation(rows, names, pos(start));
        if (alias != null) {
            result.setAlias(new TableName(null, alias));
        }
        return result;
    }

    private boolean relationContinuation() {
        int t = type();
        return t == ASOF
                || t == PIVOT
                || t == SAMPLE
                || t == BEFORE
                || t == PARTITIONS
                || t == TEMPORARY
                || t == TABLET
                || t == REPLICA
                || t == SYSTEM_TIME
                || t == TIMESTAMP
                || t == VERSION
                || t == QUALIFY
                || t == EXCEPT
                || t == MINUS
                || baselinePropertiesFollower();
    }

    boolean dmlQueryInParentheses() {
        return queryInParentheses();
    }

    private boolean queryInParentheses() {
        return queryInParentheses(1);
    }

    private boolean queryInParentheses(int from) {
        int k = from;
        while (tokens.LA(k) == OPEN) {
            k++;
        }
        return tokens.LA(k) == SELECT || tokens.LA(k) == WITH;
    }

    List<String> qualifiedName() {
        List<String> parts = new ArrayList<>();
        parts.add(identifier());
        while (type() == DOT_IDENTIFIER || type() == DOT) {
            if (type() == DOT_IDENTIFIER) {
                parts.add(take().getText().substring(1));
            } else {
                take();
                parts.add(identifier());
            }
        }
        return parts;
    }

    private TableName tableName(List<String> parts, NodePosition pos) {
        return switch (parts.size()) {
            case 1 -> new TableName(null, null, parts.get(0), pos);
            case 2 -> new TableName(null, parts.get(0), parts.get(1), pos);
            case 3 -> new TableName(parts.get(0), parts.get(1), parts.get(2), pos);
            default -> throw unsupported("table name arity");
        };
    }

    OrderByElement sortItem() {
        int start = tokens.index();
        boolean all = eat(ALL);
        Expr expr = all ? new IntLiteral(0) : expression();
        boolean ascending = true;
        if (eat(DESC)) {
            ascending = false;
        } else {
            eat(ASC);
        }
        boolean nullsFirst =
                (!SqlModeHelper.check(mode, SqlModeHelper.MODE_SORT_NULLS_LAST)) == ascending;
        if (eat(NULLS)) {
            if (eat(FIRST)) {
                nullsFirst = true;
            } else {
                expect(LAST);
                nullsFirst = false;
            }
        }
        return all
                ? new OrderByElement(expr, ascending, nullsFirst, pos(start), true)
                : new OrderByElement(expr, ascending, nullsFirst, pos(start));
    }

    private static String stringValue(String quoted) {
        char quote = quoted.charAt(0);
        String s = quoted.substring(1, quoted.length() - 1).replace("" + quote + quote, "" + quote);
        if (s.indexOf('\\') < 0) {
            return s;
        }
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(++i);
                switch (n) {
                    case 'n' -> out.append('\n');
                    case 't' -> out.append('\t');
                    case 'r' -> out.append('\r');
                    case 'b' -> out.append('\b');
                    case '0' -> out.append('\0');
                    case 'Z' -> out.append('\032');
                    case '_', '%' -> out.append('\\').append(n);
                    default -> out.append(n);
                }
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
}
