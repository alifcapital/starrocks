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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.ast.expression.AnalyticWindowBoundary;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CaseWhenClause;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.type.StructField;
import com.starrocks.type.Type;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.misc.Interval;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static com.starrocks.sql.parser.StarRocksParser.ALL;
import static com.starrocks.sql.parser.StarRocksParser.AND;
import static com.starrocks.sql.parser.StarRocksParser.ARRAY;
import static com.starrocks.sql.parser.StarRocksParser.ARRAY_AGG;
import static com.starrocks.sql.parser.StarRocksParser.ARRAY_AGG_DISTINCT;
import static com.starrocks.sql.parser.StarRocksParser.ARROW;
import static com.starrocks.sql.parser.StarRocksParser.AS;
import static com.starrocks.sql.parser.StarRocksParser.ASC;
import static com.starrocks.sql.parser.StarRocksParser.ASTERISK_SYMBOL;
import static com.starrocks.sql.parser.StarRocksParser.AT;
import static com.starrocks.sql.parser.StarRocksParser.AVG;
import static com.starrocks.sql.parser.StarRocksParser.BACKQUOTED_IDENTIFIER;
import static com.starrocks.sql.parser.StarRocksParser.BETWEEN;
import static com.starrocks.sql.parser.StarRocksParser.BINARY;
import static com.starrocks.sql.parser.StarRocksParser.BINARY_DOUBLE_QUOTED_TEXT;
import static com.starrocks.sql.parser.StarRocksParser.BINARY_SINGLE_QUOTED_TEXT;
import static com.starrocks.sql.parser.StarRocksParser.BITAND;
import static com.starrocks.sql.parser.StarRocksParser.BITNOT;
import static com.starrocks.sql.parser.StarRocksParser.BITOR;
import static com.starrocks.sql.parser.StarRocksParser.BITXOR;
import static com.starrocks.sql.parser.StarRocksParser.BIT_SHIFT_LEFT;
import static com.starrocks.sql.parser.StarRocksParser.BIT_SHIFT_RIGHT;
import static com.starrocks.sql.parser.StarRocksParser.BIT_SHIFT_RIGHT_LOGICAL;
import static com.starrocks.sql.parser.StarRocksParser.BY;
import static com.starrocks.sql.parser.StarRocksParser.CASE;
import static com.starrocks.sql.parser.StarRocksParser.CAST;
import static com.starrocks.sql.parser.StarRocksParser.CHAR;
import static com.starrocks.sql.parser.StarRocksParser.COLLATE;
import static com.starrocks.sql.parser.StarRocksParser.CONCAT;
import static com.starrocks.sql.parser.StarRocksParser.CONVERT;
import static com.starrocks.sql.parser.StarRocksParser.COUNT;
import static com.starrocks.sql.parser.StarRocksParser.CUME_DIST;
import static com.starrocks.sql.parser.StarRocksParser.CURRENT;
import static com.starrocks.sql.parser.StarRocksParser.CURRENT_DATE;
import static com.starrocks.sql.parser.StarRocksParser.CURRENT_GROUP;
import static com.starrocks.sql.parser.StarRocksParser.CURRENT_ROLE;
import static com.starrocks.sql.parser.StarRocksParser.CURRENT_TIME;
import static com.starrocks.sql.parser.StarRocksParser.CURRENT_TIMESTAMP;
import static com.starrocks.sql.parser.StarRocksParser.CURRENT_USER;
import static com.starrocks.sql.parser.StarRocksParser.CURRENT_WAREHOUSE;
import static com.starrocks.sql.parser.StarRocksParser.DATABASE;
import static com.starrocks.sql.parser.StarRocksParser.DATE;
import static com.starrocks.sql.parser.StarRocksParser.DATETIME;
import static com.starrocks.sql.parser.StarRocksParser.DAY;
import static com.starrocks.sql.parser.StarRocksParser.DECIMAL_VALUE;
import static com.starrocks.sql.parser.StarRocksParser.DENSE_RANK;
import static com.starrocks.sql.parser.StarRocksParser.DESC;
import static com.starrocks.sql.parser.StarRocksParser.DICTIONARY_GET;
import static com.starrocks.sql.parser.StarRocksParser.DIGIT_IDENTIFIER;
import static com.starrocks.sql.parser.StarRocksParser.DISTINCT;
import static com.starrocks.sql.parser.StarRocksParser.DOT_IDENTIFIER;
import static com.starrocks.sql.parser.StarRocksParser.DOUBLE_QUOTED_TEXT;
import static com.starrocks.sql.parser.StarRocksParser.DOUBLE_VALUE;
import static com.starrocks.sql.parser.StarRocksParser.ELSE;
import static com.starrocks.sql.parser.StarRocksParser.END;
import static com.starrocks.sql.parser.StarRocksParser.EQ;
import static com.starrocks.sql.parser.StarRocksParser.EQ_FOR_NULL;
import static com.starrocks.sql.parser.StarRocksParser.EXCEPT;
import static com.starrocks.sql.parser.StarRocksParser.EXISTS;
import static com.starrocks.sql.parser.StarRocksParser.EXTRACT;
import static com.starrocks.sql.parser.StarRocksParser.FALSE;
import static com.starrocks.sql.parser.StarRocksParser.FILTER;
import static com.starrocks.sql.parser.StarRocksParser.FIRST;
import static com.starrocks.sql.parser.StarRocksParser.FIRST_VALUE;
import static com.starrocks.sql.parser.StarRocksParser.FN;
import static com.starrocks.sql.parser.StarRocksParser.FOLLOWING;
import static com.starrocks.sql.parser.StarRocksParser.FROM;
import static com.starrocks.sql.parser.StarRocksParser.GLOBAL;
import static com.starrocks.sql.parser.StarRocksParser.GROUP;
import static com.starrocks.sql.parser.StarRocksParser.GROUPING;
import static com.starrocks.sql.parser.StarRocksParser.GROUPING_ID;
import static com.starrocks.sql.parser.StarRocksParser.GROUP_CONCAT;
import static com.starrocks.sql.parser.StarRocksParser.GT;
import static com.starrocks.sql.parser.StarRocksParser.GTE;
import static com.starrocks.sql.parser.StarRocksParser.HAVING;
import static com.starrocks.sql.parser.StarRocksParser.HOUR;
import static com.starrocks.sql.parser.StarRocksParser.IF;
import static com.starrocks.sql.parser.StarRocksParser.IGNORE;
import static com.starrocks.sql.parser.StarRocksParser.IN;
import static com.starrocks.sql.parser.StarRocksParser.INT;
import static com.starrocks.sql.parser.StarRocksParser.INTEGER;
import static com.starrocks.sql.parser.StarRocksParser.INTEGER_VALUE;
import static com.starrocks.sql.parser.StarRocksParser.INTERSECT;
import static com.starrocks.sql.parser.StarRocksParser.INTERVAL;
import static com.starrocks.sql.parser.StarRocksParser.INT_DIV;
import static com.starrocks.sql.parser.StarRocksParser.IS;
import static com.starrocks.sql.parser.StarRocksParser.LAG;
import static com.starrocks.sql.parser.StarRocksParser.LAST;
import static com.starrocks.sql.parser.StarRocksParser.LAST_VALUE;
import static com.starrocks.sql.parser.StarRocksParser.LEAD;
import static com.starrocks.sql.parser.StarRocksParser.LEFT;
import static com.starrocks.sql.parser.StarRocksParser.LETTER_IDENTIFIER;
import static com.starrocks.sql.parser.StarRocksParser.LIKE;
import static com.starrocks.sql.parser.StarRocksParser.LIMIT;
import static com.starrocks.sql.parser.StarRocksParser.LOCAL;
import static com.starrocks.sql.parser.StarRocksParser.LOCALTIME;
import static com.starrocks.sql.parser.StarRocksParser.LOCALTIMESTAMP;
import static com.starrocks.sql.parser.StarRocksParser.LOGICAL_AND;
import static com.starrocks.sql.parser.StarRocksParser.LOGICAL_NOT;
import static com.starrocks.sql.parser.StarRocksParser.LOGICAL_OR;
import static com.starrocks.sql.parser.StarRocksParser.LT;
import static com.starrocks.sql.parser.StarRocksParser.LTE;
import static com.starrocks.sql.parser.StarRocksParser.MAP;
import static com.starrocks.sql.parser.StarRocksParser.MATCH;
import static com.starrocks.sql.parser.StarRocksParser.MATCH_ALL;
import static com.starrocks.sql.parser.StarRocksParser.MATCH_ANY;
import static com.starrocks.sql.parser.StarRocksParser.MAX;
import static com.starrocks.sql.parser.StarRocksParser.MICROSECOND;
import static com.starrocks.sql.parser.StarRocksParser.MILLISECOND;
import static com.starrocks.sql.parser.StarRocksParser.MIN;
import static com.starrocks.sql.parser.StarRocksParser.MINUS;
import static com.starrocks.sql.parser.StarRocksParser.MINUS_SYMBOL;
import static com.starrocks.sql.parser.StarRocksParser.MINUTE;
import static com.starrocks.sql.parser.StarRocksParser.MOD;
import static com.starrocks.sql.parser.StarRocksParser.MONTH;
import static com.starrocks.sql.parser.StarRocksParser.NEQ;
import static com.starrocks.sql.parser.StarRocksParser.NOT;
import static com.starrocks.sql.parser.StarRocksParser.NTILE;
import static com.starrocks.sql.parser.StarRocksParser.NULL;
import static com.starrocks.sql.parser.StarRocksParser.NULLS;
import static com.starrocks.sql.parser.StarRocksParser.OPEN;
import static com.starrocks.sql.parser.StarRocksParser.OR;
import static com.starrocks.sql.parser.StarRocksParser.ORDER;
import static com.starrocks.sql.parser.StarRocksParser.OVER;
import static com.starrocks.sql.parser.StarRocksParser.PARAMETER;
import static com.starrocks.sql.parser.StarRocksParser.PARTITION;
import static com.starrocks.sql.parser.StarRocksParser.PASSWORD;
import static com.starrocks.sql.parser.StarRocksParser.PERCENT_RANK;
import static com.starrocks.sql.parser.StarRocksParser.PERCENT_SYMBOL;
import static com.starrocks.sql.parser.StarRocksParser.PLUS_SYMBOL;
import static com.starrocks.sql.parser.StarRocksParser.PRECEDING;
import static com.starrocks.sql.parser.StarRocksParser.QUARTER;
import static com.starrocks.sql.parser.StarRocksParser.RANGE;
import static com.starrocks.sql.parser.StarRocksParser.RANK;
import static com.starrocks.sql.parser.StarRocksParser.REGEXP;
import static com.starrocks.sql.parser.StarRocksParser.REPLACE;
import static com.starrocks.sql.parser.StarRocksParser.RIGHT;
import static com.starrocks.sql.parser.StarRocksParser.RLIKE;
import static com.starrocks.sql.parser.StarRocksParser.ROW;
import static com.starrocks.sql.parser.StarRocksParser.ROWS;
import static com.starrocks.sql.parser.StarRocksParser.ROW_NUMBER;
import static com.starrocks.sql.parser.StarRocksParser.SCHEMA;
import static com.starrocks.sql.parser.StarRocksParser.SECOND;
import static com.starrocks.sql.parser.StarRocksParser.SELECT;
import static com.starrocks.sql.parser.StarRocksParser.SEMICOLON;
import static com.starrocks.sql.parser.StarRocksParser.SEPARATOR;
import static com.starrocks.sql.parser.StarRocksParser.SESSION;
import static com.starrocks.sql.parser.StarRocksParser.SINGLE_QUOTED_TEXT;
import static com.starrocks.sql.parser.StarRocksParser.SLASH_SYMBOL;
import static com.starrocks.sql.parser.StarRocksParser.STRUCT;
import static com.starrocks.sql.parser.StarRocksParser.SUM;
import static com.starrocks.sql.parser.StarRocksParser.THEN;
import static com.starrocks.sql.parser.StarRocksParser.TIMESTAMPADD;
import static com.starrocks.sql.parser.StarRocksParser.TIMESTAMPDIFF;
import static com.starrocks.sql.parser.StarRocksParser.TRANSLATE;
import static com.starrocks.sql.parser.StarRocksParser.TRUE;
import static com.starrocks.sql.parser.StarRocksParser.UNBOUNDED;
import static com.starrocks.sql.parser.StarRocksParser.UNION;
import static com.starrocks.sql.parser.StarRocksParser.USER;
import static com.starrocks.sql.parser.StarRocksParser.VERBOSE;
import static com.starrocks.sql.parser.StarRocksParser.WEEK;
import static com.starrocks.sql.parser.StarRocksParser.WHEN;
import static com.starrocks.sql.parser.StarRocksParser.WHERE;
import static com.starrocks.sql.parser.StarRocksParser.WITH;
import static com.starrocks.sql.parser.StarRocksParser.YEAR;

// AST construction follows AstBuilder.
// No generated parser, parse tree, SQL rewrite, fallback, or AST reuse is used here.
/** Deterministic token-stream expression subset; binary chains consume constant parser-stack depth. */
public final class DirectExpressionParser<E, Q, T, F, O, W, B, C>
        implements ExpressionConstruction.Errors {
    public static final class UnsupportedExpression extends RuntimeException {
        UnsupportedExpression(String message) {
            super(message);
        }
    }

    private final TokenStream input;
    private final long mode;
    private final LexicalParameterContext parameters;
    private final DirectTokenCursor cursor;
    private final CommonTokenStream original;
    private final Supplier<Q> queryCallback;
    private final DirectParseBudget budget;
    private int last = -1;
    private int depth;
    private boolean ignoreInvalidDates;
    private int ignoredOvers;
    private DirectParseBudget.IntervalState standaloneIntervalState;
    private static final int OPEN = FrozenTokenCatalog.T_2;
    private static final int CLOSE = FrozenTokenCatalog.T_3;
    private static final int COMMA = FrozenTokenCatalog.T_0;
    private static final int DOT = FrozenTokenCatalog.T_1;
    private static final int BRACKET = FrozenTokenCatalog.T_5;
    private static final int CLOSE_BRACKET = FrozenTokenCatalog.T_6;
    private static final int COLON = FrozenTokenCatalog.T_7;

    private static boolean isDateTimeHead(int t) {
        return switch (t) {
            case CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, LOCALTIME, LOCALTIMESTAMP -> true;
            default -> false;
        };
    }

    private static boolean isCurrentInformationHead(int t) {
        return switch (t) {
            case CURRENT_USER, CURRENT_ROLE, CURRENT_GROUP, CURRENT_WAREHOUSE -> true;
            default -> false;
        };
    }

    private static boolean isAggregateHead(int t) {
        return switch (t) {
            case AVG, COUNT, SUM, MIN, MAX, ARRAY_AGG, ARRAY_AGG_DISTINCT, GROUP_CONCAT -> true;
            default -> false;
        };
    }

    private static final Set<String> PARAMETER_TYPES =
            Set.of(
                    "TINYINT",
                    "SMALLINT",
                    "INT",
                    "INTEGER",
                    "BIGINT",
                    "LARGEINT",
                    "CHAR",
                    "VARCHAR",
                    "VARBINARY",
                    "BINARY",
                    "DECIMAL",
                    "DECIMALV2",
                    "DECIMAL32",
                    "DECIMAL64",
                    "DECIMAL128",
                    "DECIMAL256",
                    "NUMERIC",
                    "NUMBER");
    private final ExpressionConstruction<E, Q, T, F, O, W, B, C> construction;

    public static DirectExpressionParser<
                    Expr,
                    QueryRelation,
                    Type,
                    StructField,
                    OrderByElement,
                    AnalyticWindow,
                    AnalyticWindowBoundary,
                    CaseWhenClause>
            eager(TokenStream input, long mode) {
        return eager(input, mode, null, null, null);
    }

    public static DirectExpressionParser<
                    Expr,
                    QueryRelation,
                    Type,
                    StructField,
                    OrderByElement,
                    AnalyticWindow,
                    AnalyticWindowBoundary,
                    CaseWhenClause>
            eager(
                    TokenStream input,
                    long mode,
                    Supplier<QueryRelation> callback,
                    DirectParseBudget budget) {
        return eager(input, mode, callback, budget, null);
    }

    public static DirectExpressionParser<
                    Expr,
                    QueryRelation,
                    Type,
                    StructField,
                    OrderByElement,
                    AnalyticWindow,
                    AnalyticWindowBoundary,
                    CaseWhenClause>
            eager(
                    TokenStream input,
                    long mode,
                    Supplier<QueryRelation> callback,
                    DirectParseBudget budget,
                    LexicalParameterContext parameters) {
        return new DirectExpressionParser<>(
                input, mode, callback, budget, parameters, new EagerExpressionConstruction(mode));
    }

    public DirectExpressionParser(
            TokenStream input,
            long mode,
            Supplier<Q> callback,
            DirectParseBudget budget,
            LexicalParameterContext parameters,
            ExpressionConstruction<E, Q, T, F, O, W, B, C> construction) {
        this(input, mode, callback, budget, parameters, null, -1, 0, construction);
    }

    private DirectExpressionParser(
            TokenStream input,
            long mode,
            Supplier<Q> queryCallback,
            DirectParseBudget budget,
            LexicalParameterContext parameters,
            DirectParseBudget.IntervalState probe,
            int start,
            int inheritedDepth,
            ExpressionConstruction<E, Q, T, F, O, W, B, C> construction) {
        if (!(input instanceof CommonTokenStream original)) {
            throw new UnsupportedExpression("local cursor requires CommonTokenStream");
        }
        this.construction = Objects.requireNonNull(construction);
        this.construction.bind(this);
        this.parameters = parameters;
        this.original = original;
        this.cursor = new DirectTokenCursor(original);
        if (probe != null) {
            cursor.seek(start);
        }
        this.input = probe == null ? cursor : new SpeculativeInput(cursor, probe);
        this.mode = mode;
        this.queryCallback = queryCallback;
        this.budget = budget;
        this.depth = inheritedDepth;
        this.standaloneIntervalState = probe;
        if (queryCallback != null && budget == null) {
            throw new IllegalArgumentException("callback requires shared budget");
        }
    }

    public E parse() {
        E result = parsePrefix();
        if (input.LA(1) != Token.EOF) {
            throw unsupported("trailing syntax");
        }
        return result;
    }

    /** Consume exactly one supported expression, leaving the following token unconsumed. */
    public E parsePrefix() {
        int saved = original.index();
        boolean success = false;
        try {
            E result = expression(0);
            int next = input.LA(1);
            if (next == OVER
                    || (next == FILTER && input.LA(2) == OPEN)
                    || next == IGNORE
                    || next == BRACKET
                    || next == ARROW
                    || next == CONCAT
                    || (isMatch(next) && binaryBooleanStart(input.LA(2)))
                    || (next == NOT && isMatch(input.LA(2)))) {
                throw unsupported("unsupported expression continuation");
            }
            cursor.sync();
            success = true;
            return result;
        } finally {
            if (!success) {
                original.seek(saved);
                cursor.seek(saved);
            }
        }
    }

    /** Cold DDL entry preserving the grammar primaryExpression boundary. */
    E parsePrimaryPrefix() {
        int saved = original.index();
        int previousLast = last;
        boolean success = false;
        if (budget != null) {
            budget.enterExpression();
        }
        try {
            E result = primary(0);
            cursor.sync();
            success = true;
            return result;
        } finally {
            if (budget != null) {
                budget.exitExpression();
            }
            if (!success) {
                original.seek(saved);
                cursor.seek(saved);
                last = previousLast;
            }
        }
    }

    /** Rare JOIN hint entry: primaryExpression only, with a virtual EOF bound. */
    E parseBoundedPrimary() {
        int saved = original.index();
        int previousLast = last;
        boolean success = false;
        if (budget != null) {
            budget.enterExpression();
        }
        try {
            E result = primary(0);
            if (input.LA(1) != Token.EOF) {
                throw unsupported("skew primary has unconsumed syntax");
            }
            cursor.sync();
            success = true;
            return result;
        } finally {
            if (budget != null) {
                budget.exitExpression();
            }
            if (!success) {
                original.seek(saved);
                cursor.seek(saved);
                last = previousLast;
            }
        }
    }

    /** Grammar generalLiteralExpression; a minus applies only to a number. */
    E parseGeneralLiteralPrefix() {
        if (input.LA(1) != MINUS_SYMBOL) {
            return parseLiteralPrefix();
        }
        int saved = original.index();
        int previousLast = last;
        boolean success = false;
        if (budget != null) {
            budget.enterExpression();
        }
        try {
            take();
            int token = input.index();
            int t = input.LA(1);
            if (t != INTEGER_VALUE && t != DECIMAL_VALUE && t != DOUBLE_VALUE) {
                throw unsupported("skew negative literal requires number");
            }
            take();
            E node = number(token);
            E result;
            result = construction.negativeGeneralLiteral(node);
            cursor.sync();
            success = true;
            return result;
        } finally {
            if (budget != null) {
                budget.exitExpression();
            }
            if (!success) {
                original.seek(saved);
                cursor.seek(saved);
                last = previousLast;
            }
        }
    }

    /** valueExpression excludes unparenthesized predicates/logical operators. */
    public E parseValuePrefix() {
        int saved = original.index();
        boolean success = false;
        if (budget != null) {
            budget.enterExpression();
        }
        try {
            E result = arithmetic(1);
            cursor.sync();
            success = true;
            return result;
        } finally {
            if (budget != null) {
                budget.exitExpression();
            }
            if (!success) {
                original.seek(saved);
                cursor.seek(saved);
            }
        }
    }

    /** Parse the grammar literalExpression, not an expression that happens to return a literal AST. */
    public E parseLiteralPrefix() {
        int saved = original.index();
        boolean success = false;
        if (budget != null) {
            budget.enterExpression();
        }
        try {
            int t = input.LA(1);
            boolean ordinary =
                    t == NULL
                            || t == TRUE
                            || t == FALSE
                            || t == INTEGER_VALUE
                            || t == DECIMAL_VALUE
                            || t == DOUBLE_VALUE
                            || t == PARAMETER
                            || t == SINGLE_QUOTED_TEXT
                            || t == DOUBLE_QUOTED_TEXT
                            || t == BINARY_SINGLE_QUOTED_TEXT
                            || t == BINARY_DOUBLE_QUOTED_TEXT;
            boolean date =
                    (t == DATE || t == DATETIME)
                            && (input.LA(2) == SINGLE_QUOTED_TEXT
                                    || input.LA(2) == DOUBLE_QUOTED_TEXT);
            if (!ordinary && !date && t != INTERVAL) {
                throw unsupported("literal expression");
            }
            // No primary-expression postfix (including no-op COLLATE) belongs to this grammar rule.
            E result = t == INTERVAL ? intervalLiteral(input.index()) : primary(Integer.MAX_VALUE);
            cursor.sync();
            success = true;
            return result;
        } finally {
            if (budget != null) {
                budget.exitExpression();
            }
            if (!success) {
                original.seek(saved);
                cursor.seek(saved);
            }
        }
    }

    /** Parse a grammar functionCall without primary-expression postfix or outer operators. */
    public E parseFunctionCallPrefix() {
        int saved = original.index();
        boolean success = false;
        if (budget != null) {
            budget.enterExpression();
        }
        try {
            int t = input.LA(1);
            int next = input.LA(2);
            // functionCall treats CAST as a generic name, not the primary CAST-AS grammar.
            // Use the existing generic argument parser; it leaves AS syntax to the reference error
            // path.
            if ((t == CAST || t == DATABASE || t == SCHEMA) && next == OPEN) {
                int start = input.index();
                int head = take();
                expect(OPEN);
                E result = function(start, List.of(identifier(head)));
                cursor.sync();
                success = true;
                return result;
            }
            boolean generic =
                    isIdentifier(t) && (next == OPEN || next == DOT || next == DOT_IDENTIFIER);
            boolean reserved = switch (t) {
                case CHAR, IF, LEFT, LIKE, MOD, REGEXP, REPLACE, RIGHT, RLIKE -> true;
                default -> false;
            };
            boolean special =
                    t == EXTRACT
                            || t == GROUPING
                            || t == GROUPING_ID
                            || t == TRANSLATE
                            || isWindowHead(t)
                            || t == DATABASE
                            || t == SCHEMA
                            || t == USER;
            if (!generic
                    && !((reserved || special) && next == OPEN)
                    && !isDateTimeHead(t)
                    && !isCurrentInformationHead(t)) {
                throw unsupported("function call");
            }
            E result = primary(Integer.MAX_VALUE, false);
            cursor.sync();
            success = true;
            return result;
        } finally {
            if (budget != null) {
                budget.exitExpression();
            }
            if (!success) {
                original.seek(saved);
                cursor.seek(saved);
            }
        }
    }

    private boolean queryStartsHere() {
        int first = input.LA(1);
        if (first == SELECT || first == WITH) {
            return true;
        }
        if (first != OPEN) {
            return false;
        }
        int saved = input.index();
        try {
            while (input.LA(1) == OPEN) {
                input.consume();
            }
            if (input.LA(1) != SELECT && input.LA(1) != WITH) {
                return false;
            }
            input.seek(saved);
            // The existing bounded walk memoizes every balanced (), [] and {}.
            // Only closes are needed here; type/query commas are never classified.
            DirectParseBudget.IntervalState state = intervalState();
            intervalOuterComma(saved, state);
            while (input.LA(1) == OPEN) {
                int open = input.index();
                input.consume();
                int nextPrefix = input.index();
                long boundary = state.boundaries.get(open);
                if (boundary == 0) {
                    throw unsupported("missing IN prefix delimiter boundary");
                }
                int close = (int) ((boundary >>> 1) - 1);
                input.seek(close);
                input.consume();
                if (inExpressionContinuation(input.LA(1))) {
                    return false;
                }
                input.seek(nextPrefix);
            }
            // Closing IN/query groups and query set/order/limit continuations
            // retain the grammar's earlier inSubquery alternative. Unknown
            // followers still go through the existing parser/original fallback.
            return true;
        } finally {
            input.seek(saved);
        }
    }

    private boolean inExpressionContinuation(int token) {
        return token == COMMA
                || token == COLLATE
                || token == DOT_IDENTIFIER
                || token == DOT
                || token == BRACKET
                || token == ARROW
                || token == CONCAT
                || isMatch(token)
                || precedence(token) > 0
                || comparison(token) != null
                || token == IS
                || token == IN
                || token == BETWEEN
                || token == LIKE
                || token == RLIKE
                || token == REGEXP
                || token == NOT
                || token == AND
                || token == OR
                || token == LOGICAL_AND
                || token == LOGICAL_OR;
    }

    private E subquery() {
        if (queryCallback == null) {
            throw unsupported("subquery callback unavailable");
        }
        SpeculativeInput probe = input instanceof SpeculativeInput p ? p : null;
        int begin = input.index();
        long spent = probe == null ? 0 : probe.state.spent;
        cursor.sync();
        Q relation = queryCallback.get();
        cursor.seek(original.index());
        // Query syntax consumes CommonTokenStream directly; charge its forward span
        // minus already charged nested expression work. Failures propagate untouched.
        if (probe != null) {
            probe.state.charge(
                    Math.max(0L, (long) input.index() - begin - (probe.state.spent - spent)));
        }
        if (relation == null) {
            throw unsupported("subquery callback returned no relation");
        }
        return construction.subquery(relation);
    }

    private static boolean isMatch(int t) {
        return t == MATCH || t == MATCH_ANY || t == MATCH_ALL;
    }

    private int take() {
        int token = input.index();
        input.consume();
        last = token;
        return token;
    }

    private boolean eat(int type) {
        if (input.LA(1) == type) {
            take();
            return true;
        }
        return false;
    }

    private void expect(int type) {
        if (!eat(type)) {
            throw unsupported("expected " + FrozenTokenNames.parserDisplayName(type));
        }
    }

    public NodePosition constructionPosition(int start) {
        return pos(start);
    }

    public UnsupportedExpression unsupported(String why) {
        int t = input.index();
        return new UnsupportedExpression(
                why + " at " + cursor.lineAt(t) + ":" + cursor.columnAt(t) + " token=" + text(t));
    }

    private NodePosition pos(int start) {
        return position(start, last);
    }

    private NodePosition position(int start, int end) {
        long begin = cursor.positionAt(start);
        long finish = cursor.positionAt(end);
        return new NodePosition(
                (int) (begin >>> 32), (int) begin, (int) (finish >>> 32), (int) finish);
    }

    private int type(int raw) {
        return cursor.typeAt(raw);
    }

    private String text(int raw) {
        return cursor.textAt(raw);
    }

    private boolean scalarLambdaHead() {
        // A quoted string RHS belongs to the earlier JSON Arrow primary alternative.
        return isIdentifier(input.LA(1))
                && input.LA(2) == ARROW
                && input.LA(3) != SINGLE_QUOTED_TEXT
                && input.LA(3) != DOUBLE_QUOTED_TEXT;
    }

    private int parenthesizedLambdaArity() {
        if (input.LA(1) != OPEN) {
            return 0;
        }
        int saved = input.index();
        int count = 0;
        try {
            input.consume();
            do {
                if (!isIdentifier(input.LA(1))) {
                    return 0;
                }
                input.consume();
                count++;
                if (input.LA(1) != COMMA) {
                    break;
                }
                input.consume();
            } while (true);
            if (input.LA(1) != CLOSE) {
                return 0;
            }
            input.consume();
            if (input.LA(1) != ARROW) {
                return 0;
            }
            input.consume();
            // A single parenthesized column followed by a string has the earlier JSON-arrow form.
            if (count == 1
                    && (input.LA(1) == SINGLE_QUOTED_TEXT || input.LA(1) == DOUBLE_QUOTED_TEXT)) {
                return 0;
            }
            return count;
        } finally {
            input.seek(saved);
        }
    }

    private E lambdaBody() {
        // The normal expression entry owns the single enclosing body level for
        // unparenthesized bodies and bodies that themselves start a lambda head.
        if (input.LA(1) != OPEN || parenthesizedLambdaArity() > 0) {
            return expression(0);
        }
        if (budget != null) {
            budget.enterExpression();
        }
        depth++;
        try {
            if (depth > 512) {
                throw unsupported("prototype nesting limit 512");
            }
            int start = input.index();
            expect(OPEN);
            if (eat(CLOSE)) {
                return null;
            }
            E first = groupedFirst();
            if (eat(COMMA)) {
                List<E> values = new ArrayList<>();
                values.add(first);
                do {
                    values.add(expression(0));
                } while (eat(COMMA));
                expect(CLOSE);
                if (values.size() != 2) {
                    throw unsupported("map lambda body arity requires original error");
                }
                return construction.lambdaMap(values);
            }
            expect(CLOSE);
            // Grouping retains the inner node position; its outer continuations
            // use the opening token exactly as the ordinary expression path.
            E value = groupedPredicateTail(start, first);
            value = booleanTail(start, value);
            return logicalTail(start, value, 0);
        } finally {
            depth--;
            if (budget != null) {
                budget.exitExpression();
            }
        }
    }

    private E expression(int min) {
        if (budget != null) {
            budget.enterExpression();
        }
        depth++;
        try {
            if (depth > 512) {
                throw unsupported("prototype nesting limit 512");
            }
            int start = input.index();
            E left;
            int lambdaArity = parenthesizedLambdaArity();
            if (lambdaArity > 0) {
                expect(OPEN);
                List<String> names = new ArrayList<>(lambdaArity);
                do {
                    names.add(identifier(take()));
                } while (eat(COMMA));
                expect(CLOSE);
                expect(ARROW);
                E body = lambdaBody();
                List<E> arguments = new ArrayList<>(lambdaArity + 1);
                arguments.add(body);
                for (String name : names) {
                    arguments.add(construction.lambdaArgument(name));
                }
                left = construction.lambda(arguments);
            } else if (scalarLambdaHead()) {
                String name = identifier(take());
                expect(ARROW);
                E body = expression(0);
                List<E> arguments = new ArrayList<>(2);
                arguments.add(body);
                arguments.add(construction.lambdaArgument(name));
                // AstBuilder deliberately uses default positions for lambda and its argument.
                left = construction.lambda(arguments);
            } else if (eat(NOT)) {
                left = construction.logicalNot(expression(3), pos(start));
            } else if (input.LA(1) == BINARY && binaryPrefixAhead()) {
                take();
                left = booleanExpression();
            } else {
                left = booleanExpression();
            }
            return logicalTail(start, left, min);
        } finally {
            depth--;
            if (budget != null) {
                budget.exitExpression();
            }
        }
    }

    /** FIRST(primaryExpression) for supported boolean operands; NOT belongs to expression. */
    private static boolean binaryBooleanStart(int t) {
        return t == PARAMETER
                || t == OPEN
                || t == OPEN_BRACE
                || t == EXISTS
                || t == MINUS_SYMBOL
                || t == PLUS_SYMBOL
                || t == BITNOT
                || t == LOGICAL_NOT
                || t == AT
                || t == GROUPING
                || t == GROUPING_ID
                || t == NULL
                || t == TRUE
                || t == FALSE
                || t == INTEGER_VALUE
                || t == DECIMAL_VALUE
                || t == DOUBLE_VALUE
                || t == SINGLE_QUOTED_TEXT
                || t == DOUBLE_QUOTED_TEXT
                || t == BINARY_SINGLE_QUOTED_TEXT
                || t == BINARY_DOUBLE_QUOTED_TEXT
                || t == BRACKET
                || t == ARRAY
                || t == MAP
                || t == CASE
                || t == CAST
                || t == CONVERT
                || t == INTERVAL
                || t == EXTRACT
                || isDateTimeHead(t)
                || isCurrentInformationHead(t)
                || isWindowHead(t)
                || isIdentifier(t)
                || isSpecialFunction(t);
    }

    private static boolean isQueryOperandStart(int t) {
        return t == SELECT || t == ALL || t == DISTINCT;
    }

    /** Only expressionDefault consumes this optional token, never the boolean operand itself. */
    private boolean binaryPrefixAhead() {
        int next = input.LA(2);
        if (next != OPEN) {
            // BINARY followed by a set operation and a query is a column, not a prefix.
            if ((next == EXCEPT || next == MINUS)
                    && (isQueryOperandStart(input.LA(3))
                            || (input.LA(3) == OPEN && (input.LA(4) == SELECT || input.LA(4) == WITH)))) {
                return false;
            }
            return binaryBooleanStart(next);
        }
        int saved = cursor.index();
        try {
            cursor.consume();
            int open = cursor.index();
            int first = cursor.LA(2);
            long boundary = delimiterBoundary(open, intervalState());
            int close = (int) ((boundary >>> 1) - 1);
            cursor.seek(close);
            int after = cursor.LA(2);
            // A function call cannot take a query as its argument, so BINARY is a prefix.
            if (first == SELECT || first == WITH) {
                return true;
            }
            // Only BINARY name/prefix ambiguity needs cached tuple eligibility.
            if ((boundary & 1L) != 0) {
                cursor.seek(open);
                return tupleInQueryAhead();
            }
            // Function-only continuations must use existing generic modifier validation.
            if (after == OVER || after == FILTER) {
                return false;
            }
            // BINARY (a)->'k' parses both as a prefix on an arrow expression and as an arrow on
            // BINARY(a); the grammar takes the prefix. Any other RHS is a lambda, which BINARY
            // cannot prefix.
            if (after == ARROW && cursor.LA(3) != SINGLE_QUOTED_TEXT && cursor.LA(3) != DOUBLE_QUOTED_TEXT) {
                throw unsupported("BINARY parenthesized lambda/arrow ambiguity");
            }
            // Empty/star/modifier-only arguments are generic; NOT is allowed inside grouping.
            return first == NOT || binaryBooleanStart(first);
        } finally {
            cursor.seek(saved);
        }
    }

    private E logicalTail(int start, E left, int min) {
        boolean logicalBuilt = false;
        while (true) {
            int t = input.LA(1);
            int precedence = t == AND || t == LOGICAL_AND ? 2 : t == OR || t == LOGICAL_OR ? 1 : 0;
            if (precedence == 0 || precedence < min) {
                break;
            }
            take();
            E right = expression(precedence + 1);
            logicalBuilt = true;
            left = construction.logical(precedence == 2, left, right, pos(start));
        }
        if (logicalBuilt) {
            left = construction.rewriteLogical(left);
        }
        return left;
    }

    private E booleanExpression() {
        int start = input.index();
        return booleanTail(start, predicate());
    }

    private E booleanTail(int start, E left) {
        while (true) {
            if (eat(IS)) {
                boolean neg = eat(NOT);
                expect(NULL);
                left = construction.isNull(left, neg, pos(start));
            } else {
                BinaryType op = comparison(input.LA(1));
                if (op == null) {
                    break;
                }
                take();
                left = construction.compare(op, left, predicate(), pos(start));
            }
        }
        return left;
    }

    private BinaryType comparison(int t) {
        return switch (t) {
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

    /** Eligibility only: no AST construction, and all cursor/last state is retained. */
    private boolean tupleInQueryAhead() {
        if (input.LA(1) != OPEN || input.LA(2) == SELECT || input.LA(2) == WITH) {
            return false;
        }
        int saved = input.index();
        try {
            long boundary = delimiterBoundary(saved, intervalState());
            if ((boundary & 1L) == 0) {
                return false;
            }
            input.seek((int) ((boundary >>> 1) - 1));
            input.consume();
            if (input.LA(1) == NOT) {
                input.consume();
            }
            if (input.LA(1) != IN) {
                return false;
            }
            input.consume();
            if (input.LA(1) != OPEN) {
                return false;
            }
            input.consume();
            return queryStartsHere();
        } finally {
            input.seek(saved);
        }
    }

    /** Same opening content in predicate grouping, primary grouping and lambda body. */
    private E groupedFirst() {
        int first = input.LA(1);
        if (first == SELECT || first == WITH || (first == OPEN && parenthesizedQueryAhead())) {
            return subquery();
        }
        return expression(0);
    }

    /**
     * Inside a group, a leading parenthesized query followed by a set operation, ORDER BY or LIMIT
     * makes the whole group one query; a lone nested query stays an expression.
     */
    private boolean parenthesizedQueryAhead() {
        int second = input.LA(2);
        if (second != OPEN && second != SELECT && second != WITH) {
            return false;
        }
        int saved = input.index();
        try {
            long boundary = delimiterBoundary(saved, intervalState());
            input.seek((int) ((boundary >>> 1) - 1));
            input.consume();
            int after = input.LA(1);
            return after == UNION
                    || after == EXCEPT
                    || after == MINUS
                    || after == INTERSECT
                    || after == ORDER
                    || after == LIMIT;
        } finally {
            input.seek(saved);
        }
    }

    /** Grouping permits value continuations; a bare tuple predicate never calls this. */
    private E groupedPredicateTail(int start, E first) {
        E value = primaryTail(start, first, 0);
        value = arithmeticTail(start, value, 1);
        return predicateTail(input.index(), value);
    }

    private E tupleInSubqueryTail(int start, E first) {
        List<E> values = new ArrayList<>();
        values.add(first);
        do {
            values.add(expression(0));
        } while (eat(COMMA));
        expect(CLOSE);
        boolean neg = eat(NOT);
        expect(IN);
        expect(OPEN);
        E query = subquery();
        expect(CLOSE);
        // Required parameter numbering is lexical; current parameters still fallback.
        return construction.multiIn(values, query, neg, pos(start));
    }

    private E predicate() {
        if (input.LA(1) == OPEN) {
            int start = input.index();
            take();
            E first = groupedFirst();
            if (eat(COMMA)) {
                return tupleInSubqueryTail(start, first);
            }
            expect(CLOSE);
            return groupedPredicateTail(start, first);
        }
        E value = arithmetic(1);
        return predicateTail(input.index(), value);
    }

    private E predicateTail(int start, E value) {
        boolean neg = false;
        if (input.LA(1) == NOT
                && (input.LA(2) == IN
                        || input.LA(2) == BETWEEN
                        || input.LA(2) == LIKE
                        || input.LA(2) == RLIKE
                        || input.LA(2) == REGEXP)) {
            take();
            neg = true;
        }
        if (eat(IN)) {
            int open = input.index();
            expect(OPEN);
            if (queryStartsHere()) {
                E query = subquery();
                expect(CLOSE);
                return construction.inQuery(value, query, neg, pos(start));
            }
            List<E> values = new ArrayList<>();
            values.add(expression(0));
            while (eat(COMMA)) {
                values.add(expression(0));
            }
            expect(CLOSE);
            if (construction.largeInWanted(values.size())) {
                E large = largeIn(value, values, neg, pos(start), open, last);
                if (large != null) {
                    return large;
                }
            }
            return construction.inList(value, values, neg, pos(start));
        }
        if (eat(BETWEEN)) {
            E low = arithmetic(1);
            expect(AND);
            return construction.between(value, low, predicate(), neg, pos(start));
        }
        if (input.LA(1) == LIKE || input.LA(1) == RLIKE || input.LA(1) == REGEXP) {
            int op = type(take());
            E result = construction.like(op == LIKE, value, arithmetic(1), pos(start));
            return neg ? construction.logicalNot(result, pos(start)) : result;
        }
        return value;
    }

    // The grammar reads IN (1, 2, ...) as integerList and IN ('a', 'b', ...) as stringList, and AstBuilder builds
    // a LargeInPredicate only for those two shapes, so we check that the list is exactly such tokens.
    private E largeIn(E value, List<E> values, boolean negative, NodePosition p, int open, int close) {
        if (close - open != 2 * values.size()) {
            return null;
        }
        int first = type(open + 1);
        boolean integers = first == INTEGER_VALUE;
        if (!integers && first != SINGLE_QUOTED_TEXT && first != DOUBLE_QUOTED_TEXT) {
            return null;
        }
        for (int raw = open + 1; raw < close; raw += 2) {
            int t = type(raw);
            boolean literal = integers ? t == INTEGER_VALUE : t == SINGLE_QUOTED_TEXT || t == DOUBLE_QUOTED_TEXT;
            if (!literal || (raw + 1 < close && type(raw + 1) != COMMA)) {
                return null;
            }
        }
        Token openToken = original.get(open);
        String rawText = openToken.getInputStream()
                .getText(Interval.of(openToken.getStartIndex(), original.get(close).getStopIndex()));
        return construction.largeIn(value, values, negative, p, integers, rawText);
    }

    private static int precedence(int t) {
        return switch (t) {
            case BITXOR -> 8;
            case ASTERISK_SYMBOL, SLASH_SYMBOL, PERCENT_SYMBOL, INT_DIV, MOD -> 7;
            case PLUS_SYMBOL, MINUS_SYMBOL -> 6;
            case BITAND -> 5;
            case BITOR -> 4;
            case BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT, BIT_SHIFT_RIGHT_LOGICAL -> 3;
            default -> 0;
        };
    }

    private static ArithmeticExpr.Operator arithmeticOperator(int t) {
        return switch (t) {
            case BITXOR -> ArithmeticExpr.Operator.BITXOR;
            case ASTERISK_SYMBOL -> ArithmeticExpr.Operator.MULTIPLY;
            case SLASH_SYMBOL -> ArithmeticExpr.Operator.DIVIDE;
            case PERCENT_SYMBOL, MOD -> ArithmeticExpr.Operator.MOD;
            case INT_DIV -> ArithmeticExpr.Operator.INT_DIVIDE;
            case PLUS_SYMBOL -> ArithmeticExpr.Operator.ADD;
            case MINUS_SYMBOL -> ArithmeticExpr.Operator.SUBTRACT;
            case BITAND -> ArithmeticExpr.Operator.BITAND;
            case BITOR -> ArithmeticExpr.Operator.BITOR;
            case BIT_SHIFT_LEFT -> ArithmeticExpr.Operator.BIT_SHIFT_LEFT;
            case BIT_SHIFT_RIGHT -> ArithmeticExpr.Operator.BIT_SHIFT_RIGHT;
            case BIT_SHIFT_RIGHT_LOGICAL -> ArithmeticExpr.Operator.BIT_SHIFT_RIGHT_LOGICAL;
            default -> throw new IllegalArgumentException();
        };
    }

    private E arithmetic(int min) {
        int start = input.index();
        return arithmeticTail(start, primary(), min);
    }

    private E arithmeticTail(int start, E left, int min) {
        while (precedence(input.LA(1)) >= min) {
            int t = type(take());
            E right = arithmetic(precedence(t) + 1);
            ArithmeticExpr.Operator op = arithmeticOperator(t);
            left = construction.arithmetic(op, left, right, pos(start));
        }
        return left;
    }

    private E primary() {
        return primary(0);
    }

    private E primary(int min) {
        return primary(min, true);
    }

    private E primary(int min, boolean dictionaryPrimary) {
        int start = input.index();
        int t = input.LA(1);
        E value;
        if (t == PARAMETER) {
            if (parameters == null) {
                throw unsupported("parameter context required");
            }
            int offset = original.get(input.index()).getStartIndex();
            take();
            value = construction.parameter(parameters, offset);
        } else if (t == OPEN_BRACE) {
            take();
            expect(FN);
            int innerStart = input.index();
            int innerHead = input.LA(1);
            int droppedOvers = ignoredOvers;
            E function = parseFunctionCallPrefix();
            if (droppedOvers != ignoredOvers && !construction.isFunctionCall(function)) {
                throw unsupported("ODBC call whose OVER is ignored");
            }
            NodePosition functionPos = pos(innerStart);
            expect(CLOSE_BRACE);
            value = construction.odbc(function, functionPos, isWindowHead(innerHead));
        } else if (t == OPEN) {
            take();
            value = groupedFirst();
            expect(CLOSE);
        } else if (t == EXISTS) {
            take();
            expect(OPEN);
            E query = subquery();
            expect(CLOSE);
            value = construction.exists(query, pos(start));
        } else if (t == MINUS_SYMBOL || t == PLUS_SYMBOL || t == BITNOT || t == LOGICAL_NOT) {
            take();
            value = primary(t == LOGICAL_NOT ? 15 : 16);
            value = construction.unary(t, value, start);
        } else if (t == AT) {
            take();
            if (eat(AT)) {
                SetType setType = null;
                int scope = input.LA(1);
                // Only grammar varType followed by a literal dot denotes scope. Bare GLOBAL/etc
                // remains the variable identifier; DOT_IDENTIFIER may instead be a dereference.
                if ((scope == GLOBAL || scope == LOCAL || scope == SESSION || scope == VERBOSE)
                        && input.LA(2) == DOT) {
                    take();
                    expect(DOT);
                    setType =
                            scope == GLOBAL
                                    ? SetType.GLOBAL
                                    : scope == VERBOSE ? SetType.VERBOSE : SetType.SESSION;
                }
                if (!isIdentifier(input.LA(1))) {
                    throw unsupported("system variable identifier");
                }
                String variable = identifier(take());
                value = construction.variable(variable, setType, pos(start));
            } else {
                int name = input.index();
                String variable;
                if (type(name) == SINGLE_QUOTED_TEXT || type(name) == DOUBLE_QUOTED_TEXT) {
                    variable = stringValue(name);
                } else if (isIdentifier(type(name))) {
                    variable = identifier(name);
                } else {
                    throw unsupported("user variable identifier");
                }
                take();
                value = construction.userVariable(variable, pos(start));
            }
        } else if (t == GROUPING || t == GROUPING_ID) {
            take();
            expect(OPEN);
            List<E> args = new ArrayList<>();
            if (input.LA(1) != CLOSE) {
                args.add(expression(0));
                while (eat(COMMA)) {
                    args.add(expression(0));
                }
            }
            expect(CLOSE);
            value = construction.grouping(args, pos(start));
        } else if (t == NULL) {
            take();
            value = construction.nullLiteral(pos(start));
        } else if (t == TRUE || t == FALSE) {
            take();
            value = construction.bool(t == TRUE, pos(start));
        } else if (t == INTEGER_VALUE || t == DECIMAL_VALUE || t == DOUBLE_VALUE) {
            take();
            value = number(start);
        } else if (t == SINGLE_QUOTED_TEXT || t == DOUBLE_QUOTED_TEXT) {
            take();
            value = construction.string(stringValue(start), pos(start));
        } else if (t == BINARY_SINGLE_QUOTED_TEXT || t == BINARY_DOUBLE_QUOTED_TEXT) {
            take();
            String quoted = text(start);
            value =
                    construction.binaryLiteral(
                            quoted.substring(2, quoted.length() - 1), pos(start));
        } else if ((t == DATE || t == DATETIME)
                && (input.LA(2) == SINGLE_QUOTED_TEXT || input.LA(2) == DOUBLE_QUOTED_TEXT)) {
            take();
            value = dateLiteral(stringValue(take()), t == DATE, start);
        } else if (t == BRACKET || (t == ARRAY && input.LA(2) == LT)) {
            T arrayType = null;
            if (t == ARRAY) {
                take();
                expect(LT);
                arrayType = construction.arrayType(parseType());
                expect(GT);
            }
            expect(BRACKET);
            List<E> values;
            if (eat(CLOSE_BRACKET)) {
                values = Collections.emptyList();
            } else {
                values = new ArrayList<>();
                values.add(expression(0));
                while (eat(COMMA)) {
                    values.add(expression(0));
                }
                expect(CLOSE_BRACKET);
            }
            value = construction.array(arrayType, values, pos(start));
        } else if (t == MAP
                && (input.LA(2) == OPEN_BRACE || (input.LA(2) == LT && mapConstructorAhead()))) {
            T mapType;
            if (input.LA(2) == LT) {
                mapType = parseType();
            } else {
                take();
                mapType = construction.anyMapType();
            }
            expect(OPEN_BRACE);
            List<E> values;
            if (eat(CLOSE_BRACE)) {
                values = Collections.emptyList();
            } else {
                values = new ArrayList<>();
                do {
                    values.add(expression(0));
                    expect(COLON);
                    values.add(expression(0));
                } while (eat(COMMA));
                expect(CLOSE_BRACE);
            }
            value = construction.map(mapType, values, pos(start));
        } else if (t == CASE) {
            take();
            E base = input.LA(1) == WHEN ? null : expression(0);
            List<C> clauses = new ArrayList<>();
            do {
                int when = input.index();
                expect(WHEN);
                E cond = expression(0);
                expect(THEN);
                E result = expression(0);
                clauses.add(construction.when(cond, result, pos(when)));
            } while (input.LA(1) == WHEN);
            E other = eat(ELSE) ? expression(0) : null;
            expect(END);
            value = construction.caseExpr(base, clauses, other, pos(start));
        } else if (t == CAST && input.LA(2) == OPEN) {
            take();
            expect(OPEN);
            E child = expression(0);
            expect(AS);
            T type = parseType();
            expect(CLOSE);
            value = construction.cast(type, child, pos(start));
        } else if (t == CONVERT) {
            take();
            expect(OPEN);
            E child = expression(0);
            expect(COMMA);
            T type = parseType();
            expect(CLOSE);
            value = construction.cast(type, child, pos(start));
        } else if (t == INTERVAL) {
            value = intervalPrimary(start);
        } else if (t == EXTRACT && extractFormAhead()) {
            take();
            expect(OPEN);
            int field = take();
            expect(FROM);
            E argument = arithmetic(1);
            expect(CLOSE);
            // AstBuilder.visitExtract uses raw identifier text, including backquotes,
            // and valueExpression (arithmetic), rather than a generic expression.
            value = construction.extract(text(field), argument, pos(start));
        } else if ((t == TIMESTAMPADD || t == TIMESTAMPDIFF)
                && input.LA(2) == OPEN
                && isUnit(input.LA(3))
                && input.LA(4) == COMMA) {
            take();
            expect(OPEN);
            int unit = take();
            expect(COMMA);
            E e2 = expression(0);
            expect(COMMA);
            E e3 = expression(0);
            expect(CLOSE);
            // AstBuilder specialFunctionExpression reverses the two expressions.
            // UnitIdentifier also preserves its existing default-locale uppercasing.
            value =
                    construction.timestamp(
                            t == TIMESTAMPADD ? "TIMESTAMPADD" : "TIMESTAMPDIFF",
                            e3,
                            e2,
                            text(unit),
                            position(unit, unit),
                            pos(start));
        } else if (isDateTimeHead(t)) {
            take();
            List<E> args = new ArrayList<>();
            if (eat(OPEN)) {
                if (input.LA(1) == INTEGER_VALUE) {
                    if (t != CURRENT_TIMESTAMP) {
                        throw unsupported("date-time precision");
                    }
                    args.add(construction.precision(text(take())));
                }
                expect(CLOSE);
            }
            value = construction.dateTime(text(start).toUpperCase(Locale.ROOT), args);
        } else if (isCurrentInformationHead(t)) {
            take();
            if (eat(OPEN)) {
                expect(CLOSE);
            }
            value = construction.information(text(start).toUpperCase(Locale.ROOT), pos(start));
        } else if (isWindowHead(t) && input.LA(2) == OPEN) {
            value = windowFunction(start);
        } else if (isIdentifier(t) || isSpecialFunction(t)) {
            List<String> parts = new ArrayList<>();
            parts.add(identifier(take()));
            while (input.LA(1) == DOT_IDENTIFIER || (input.LA(1) == DOT)) {
                if (input.LA(1) == DOT_IDENTIFIER) {
                    parts.add(sourceText(take(), 1, 0));
                } else {
                    take();
                    if (!isIdentifier(input.LA(1))) {
                        throw unsupported("qualified identifier");
                    }
                    parts.add(identifier(take()));
                }
            }
            if (eat(OPEN)) {
                value = function(start, parts, dictionaryPrimary);
            } else {
                if (!isIdentifier(t)) {
                    throw unsupported("reserved function name as column");
                }
                value =
                        construction.slot(
                                parts, pos(start), parts.size() == 1 && t == BACKQUOTED_IDENTIFIER);
            }
        } else {
            throw unsupported("primary expression");
        }
        return primaryTail(start, value, min);
    }

    private E primaryTail(int start, E value, int min) {
        while (true) {
            int suffix = input.LA(1);
            if (min <= 20 && suffix == COLLATE) {
                take();
                if (input.LA(1) == SINGLE_QUOTED_TEXT
                        || input.LA(1) == DOUBLE_QUOTED_TEXT
                        || isIdentifier(input.LA(1))) {
                    take();
                } else {
                    throw unsupported("collation name");
                }
            } else if (min <= 18 && (suffix == DOT_IDENTIFIER || suffix == DOT)) {
                String field;
                if (suffix == DOT_IDENTIFIER) {
                    field = sourceText(take(), 1, 0);
                } else {
                    take();
                    if (!isIdentifier(input.LA(1))) {
                        throw unsupported("field name");
                    }
                    field = identifier(take());
                }
                value = construction.fieldAccess(value, field, pos(start));
            } else if (min <= 17 && suffix == CONCAT) {
                // Grammar: left binding 17, RHS primary(18). Higher-minimum callers leave it
                // unconsumed.
                take();
                if (budget != null) {
                    budget.enterExpression();
                }
                depth++;
                try {
                    // CONCAT with unary RHS can recursively enter another CONCAT: retain the
                    // nesting bound only here.
                    if (depth > 512) {
                        throw unsupported("prototype nesting limit 512");
                    }
                    E right = primary(18);
                    value = construction.concat(value, right, pos(start));
                } finally {
                    depth--;
                    if (budget != null) {
                        budget.exitExpression();
                    }
                }
            } else if (min <= 4 && suffix == BRACKET) {
                take();
                // Grammar index is valueExpression, not unrestricted expression.
                // CollectionElementExpr deliberately retains its DEFAULT position.
                E index = arithmetic(1);
                expect(CLOSE_BRACKET);
                value = construction.collection(value, index);
            } else if (min <= 2 && suffix == ARROW) {
                take();
                // primaryExpression ARROW string: the RHS is a token, not an expression.
                // Unary operands use primary(16/15), so -a->'x' wraps the unary result.
                int key = input.index();
                if (input.LA(1) != SINGLE_QUOTED_TEXT && input.LA(1) != DOUBLE_QUOTED_TEXT) {
                    throw unsupported("arrow string RHS");
                }
                take();
                E keyLiteral = construction.string(stringValue(key), position(key, key));
                value = construction.arrow(value, keyLiteral, pos(start));
            } else if (min <= 1
                    && ((isMatch(suffix) && binaryBooleanStart(input.LA(2)))
                            || (suffix == NOT && isMatch(input.LA(2))))) {
                // Grammar MATCH is primary precedence 1, with primary(2) on its RHS.
                // A MATCH word that no operand can follow is left to the caller as an alias.
                // After NOT it cannot be an alias, so an invalid RHS requests whole fallback.
                boolean negate = eat(NOT);
                int operator = type(take());
                MatchExpr.MatchOperator matchOperator = switch (operator) {
                    case MATCH -> MatchExpr.MatchOperator.MATCH;
                    case MATCH_ANY -> MatchExpr.MatchOperator.MATCH_ANY;
                    case MATCH_ALL -> MatchExpr.MatchOperator.MATCH_ALL;
                    default -> throw new IllegalStateException("unreachable MATCH operator");
                };
                E right = primary(2);
                NodePosition position = pos(start);
                E matched = construction.match(matchOperator, value, right, position);
                value = negate ? construction.logicalNot(matched, position) : matched;
            } else {
                break;
            }
        }
        return value;
    }

    private static boolean isNumber(int t) {
        return t == INTEGER_VALUE || t == DECIMAL_VALUE || t == DOUBLE_VALUE;
    }

    private static boolean isUnit(int t) {
        return t == YEAR
                || t == MONTH
                || t == WEEK
                || t == DAY
                || t == HOUR
                || t == MINUTE
                || t == SECOND
                || t == QUARTER
                || t == MILLISECOND
                || t == MICROSECOND;
    }

    /**
     * For syntax whose expressions the reference builder never visits, so it never validates their
     * date literals: an invalid date literal becomes NULL instead of requesting the original path.
     */
    void ignoreInvalidDates() {
        ignoreInvalidDates = true;
    }

    private E dateLiteral(String value, boolean date, int start) {
        try {
            return construction.dateLiteral(value, date);
        } catch (UnsupportedExpression e) {
            if (!ignoreInvalidDates) {
                throw e;
            }
            return construction.nullLiteral(pos(start));
        }
    }

    /** Only EXTRACT(field FROM ...) is the special form; any other EXTRACT( is an ordinary call. */
    private boolean extractFormAhead() {
        return input.LA(2) == OPEN && isIdentifier(input.LA(3)) && input.LA(4) == FROM;
    }

    /**
     * MAP '<' starts a constructor only when a whole map type and '{' follow. We expect a column
     * named map to be compared with '<' far more often than a malformed type, so a failed type
     * parse means a column.
     */
    private boolean mapConstructorAhead() {
        int saved = input.index();
        int savedLast = last;
        try {
            parseType();
            return input.LA(1) == OPEN_BRACE;
        } catch (UnsupportedExpression e) {
            return false;
        } finally {
            input.seek(saved);
            last = savedLast;
        }
    }

    private E intervalLiteral(int start) {
        take();
        E amount = expression(0);
        int unit = input.index();
        if (!isUnit(input.LA(1))) {
            throw unsupported("interval unit");
        }
        take();
        return construction.interval(amount, text(unit), position(unit, unit), pos(start));
    }

    private E intervalCall(int start) {
        int head = take();
        expect(OPEN);
        return function(start, List.of(identifier(head)));
    }

    private DirectParseBudget.IntervalState intervalState() {
        if (budget != null) {
            return budget.intervalState(original);
        }
        if (standaloneIntervalState == null) {
            standaloneIntervalState = new DirectParseBudget.IntervalState(original.size());
        }
        return standaloneIntervalState;
    }

    private E intervalPrimary(int start) {
        if (input.LA(2) != OPEN) {
            return intervalWord(start);
        }
        DirectParseBudget.IntervalState state = intervalState();
        byte known = state.classification.get(start);
        if (known == 1) {
            return intervalLiteral(start);
        }
        if (known == 2) {
            return intervalCall(start);
        }
        int open = start + 1;
        while (cursor.typeAt(open) != OPEN) {
            state.charge(1);
            open++;
        }
        if (intervalOuterComma(open, state)) {
            state.classification.put(start, (byte) 2);
            return intervalCall(start);
        }
        // A completed parenthesized amount followed immediately by a unit is unambiguous.
        // Cursor lookahead skips hidden tokens on Common and uses dense ordinals privately.
        int close = (int) ((state.boundaries.get(open) >>> 1) - 1);
        int savedPosition = cursor.index();
        boolean immediateUnit;
        try {
            cursor.seek(close);
            immediateUnit = isUnit(cursor.LA(2));
        } finally {
            cursor.seek(savedPosition);
        }
        if (immediateUnit) {
            state.classification.put(start, (byte) 1);
            return intervalLiteral(start);
        }
        if (!unitMayFollowAmount(close, state)) {
            state.classification.put(start, (byte) 2);
            return intervalCall(start);
        }
        E literal = speculativeIntervalLiteral(start, state);
        // Only a successful amount with absent unit retries the ordinary call.
        return literal != null ? literal : intervalCall(start);
    }

    /**
     * INTERVAL without '(' is a literal when an amount and a unit follow; otherwise it is a
     * nonreserved word used as a column name.
     */
    private E intervalWord(int start) {
        int next = input.LA(2);
        if (isNumber(next)
                || ((next == MINUS_SYMBOL || next == PLUS_SYMBOL)
                        && isNumber(input.LA(3))
                        && isUnit(input.LA(4)))) {
            return intervalLiteral(start);
        }
        // Reserved function names such as LIKE start an operand only as a call.
        if ((!binaryBooleanStart(next) && next != NOT && next != BINARY)
                || (isSpecialFunction(next) && input.LA(3) != OPEN)) {
            return intervalColumn(start);
        }
        // One name or string followed by a unit is the common literal; skip the speculation.
        if ((isIdentifier(next) || next == SINGLE_QUOTED_TEXT || next == DOUBLE_QUOTED_TEXT)
                && isUnit(input.LA(3))) {
            return intervalLiteral(start);
        }
        DirectParseBudget.IntervalState state = intervalState();
        byte known = state.classification.get(start);
        if (known == 1) {
            return intervalLiteral(start);
        }
        if (known == 2) {
            return intervalColumn(start);
        }
        E literal = speculativeIntervalLiteral(start, state);
        return literal != null ? literal : intervalColumn(start);
    }

    private E intervalColumn(int start) {
        List<String> parts = new ArrayList<>();
        parts.add(identifier(take()));
        while (input.LA(1) == DOT_IDENTIFIER || input.LA(1) == DOT) {
            if (input.LA(1) == DOT_IDENTIFIER) {
                parts.add(sourceText(take(), 1, 0));
            } else {
                take();
                if (!isIdentifier(input.LA(1))) {
                    throw unsupported("qualified identifier");
                }
                parts.add(identifier(take()));
            }
        }
        if (eat(OPEN)) {
            return function(start, parts);
        }
        return construction.slot(parts, pos(start), false);
    }

    /**
     * Whether a unit can follow the amount that starts with the group closed at {@code close}.
     * Without a unit at nesting level zero before something that must end the amount, the
     * INTERVAL cannot be a literal, so nested calls need no speculative parse.
     */
    private boolean unitMayFollowAmount(int close, DirectParseBudget.IntervalState state) {
        int saved = cursor.index();
        try {
            cursor.seek(close);
            cursor.consume();
            // A comma at level zero ends the amount unless it may belong to MAP<k,v>.
            boolean angle = false;
            while (true) {
                state.charge(1);
                int t = cursor.LA(1);
                if (t == OPEN || t == BRACKET || t == OPEN_BRACE) {
                    long boundary = delimiterBoundary(cursor.index(), state);
                    cursor.seek((int) ((boundary >>> 1) - 1));
                } else if (isUnit(t)) {
                    return true;
                } else if (t == COMMA) {
                    return angle;
                } else if (t == LT) {
                    angle = true;
                } else if (t == Token.EOF
                        || t == CLOSE
                        || t == CLOSE_BRACKET
                        || t == CLOSE_BRACE
                        || t == SEMICOLON
                        || t == FROM
                        || t == WHERE
                        || t == GROUP
                        || t == HAVING
                        || t == ORDER
                        || t == LIMIT
                        || t == UNION
                        || t == AS) {
                    return false;
                }
                cursor.consume();
            }
        } finally {
            cursor.seek(saved);
        }
    }

    /** Parses the amount after INTERVAL and returns the literal when a unit follows, else null. */
    private E speculativeIntervalLiteral(int start, DirectParseBudget.IntervalState state) {
        int originalStart = original.index();
        int cursorStart = cursor.index();
        int savedLast = last;
        boolean accepted = false;
        try {
            take();
            // A temporary child owns the counter cursor; the ordinary final input
            // and its per-token consume path remain unchanged. Never cache its AST.
            DirectExpressionParser<E, Q, T, F, O, W, B, C> probe =
                    new DirectExpressionParser<>(
                            original,
                            mode,
                            queryCallback,
                            budget,
                            parameters,
                            state,
                            cursor.index(),
                            depth,
                            construction.fork());
            // parsePrefix also rejects unsupported continuations before classification.
            E amount = probe.parsePrefix();
            cursor.seek(probe.cursor.index());
            last = probe.last;
            if (isUnit(input.LA(1))) {
                int unit = take();
                E result =
                        construction.interval(amount, text(unit), position(unit, unit), pos(start));
                state.classification.put(start, (byte) 1);
                accepted = true;
                return result;
            }
            state.classification.put(start, (byte) 2);
            return null;
        } finally {
            if (!accepted) {
                cursor.seek(cursorStart);
                original.seek(originalStart);
                last = savedLast;
            }
        }
    }

    private static final int OPEN_BRACE = FrozenTokenCatalog.T_8;
    private static final int CLOSE_BRACE = FrozenTokenCatalog.T_9;

    private static final class DelimiterFrame {
        final int open;
        final int type;
        boolean comma;

        DelimiterFrame(int open, int type) {
            this.open = open;
            this.type = type;
        }
    }

    private boolean intervalOuterComma(int open, DirectParseBudget.IntervalState state) {
        return (delimiterBoundary(open, state) & 1L) != 0;
    }

    /** Shared cached delimiter result: ((close + 1L) << 1) | outerComma. */
    private long delimiterBoundary(int open, DirectParseBudget.IntervalState state) {
        long cached = state.boundaries.get(open);
        if (cached != 0) {
            return cached;
        }
        Deque<DelimiterFrame> stack = new ArrayDeque<>();
        for (int raw = open; raw < original.size(); raw++) {
            state.charge(1);
            int t = cursor.typeAt(raw);
            if (t == OPEN || t == BRACKET || t == OPEN_BRACE) {
                long nested = state.boundaries.get(raw);
                if (nested != 0) {
                    raw = (int) ((nested >>> 1) - 1);
                    continue;
                }
                stack.push(new DelimiterFrame(raw, t));
            } else if (t == CLOSE || t == CLOSE_BRACKET || t == CLOSE_BRACE) {
                if (stack.isEmpty()) {
                    throw unsupported("interval delimiter mismatch");
                }
                DelimiterFrame frame = stack.pop();
                int expected =
                        frame.type == OPEN
                                ? CLOSE
                                : frame.type == BRACKET ? CLOSE_BRACKET : CLOSE_BRACE;
                if (t != expected) {
                    throw unsupported("interval delimiter mismatch");
                }
                state.boundaries.put(frame.open, (((long) raw + 1) << 1) | (frame.comma ? 1L : 0L));
                if (stack.isEmpty()) {
                    return state.boundaries.get(frame.open);
                }
            } else if (t == COMMA && !stack.isEmpty()) {
                stack.peek().comma = true;
            } else if (t == Token.EOF) {
                break;
            }
        }
        throw unsupported("unclosed interval parentheses");
    }

    /** Counts the tokens consumed by an interval scan; ordinary consumes stay on DirectTokenCursor. */
    private static final class SpeculativeInput implements TokenStream {
        final TokenStream delegate;
        final DirectParseBudget.IntervalState state;

        SpeculativeInput(TokenStream delegate, DirectParseBudget.IntervalState state) {
            this.delegate = delegate;
            this.state = state;
        }

        @Override
        public void consume() {
            int before = delegate.index();
            delegate.consume();
            state.charge(Math.max(1, delegate.index() - before));
        }

        @Override
        public int LA(int k) {
            return delegate.LA(k);
        }

        @Override
        public Token LT(int k) {
            return delegate.LT(k);
        }

        @Override
        public int index() {
            return delegate.index();
        }

        @Override
        public void seek(int p) {
            delegate.seek(p);
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public Token get(int p) {
            return delegate.get(p);
        }

        @Override
        public TokenSource getTokenSource() {
            return delegate.getTokenSource();
        }

        @Override
        public int mark() {
            return delegate.mark();
        }

        @Override
        public void release(int m) {
            delegate.release(m);
        }

        @Override
        public String getSourceName() {
            return delegate.getSourceName();
        }

        @Override
        public String getText() {
            return delegate.getText();
        }

        @Override
        public String getText(Interval i) {
            return delegate.getText(i);
        }

        @Override
        public String getText(RuleContext c) {
            return delegate.getText(c);
        }

        @Override
        public String getText(Token a, Token b) {
            return delegate.getText(a, b);
        }
    }

    private static final boolean[] IDENTIFIER_FLAGS = identifierFlags();

    private static boolean[] identifierFlags() {
        return QueryIdentifiers.flags();
    }

    private static boolean isIdentifier(int t) {
        return t >= 0 && t < IDENTIFIER_FLAGS.length && IDENTIFIER_FLAGS[t];
    }

    private static boolean isSpecialFunction(int t) {
        return t == DATABASE
                || t == SCHEMA
                || t == CHAR
                || t == IF
                || t == LEFT
                || t == LIKE
                || t == MOD
                || t == REGEXP
                || t == REPLACE
                || t == RIGHT
                || t == RLIKE
                || t == TRANSLATE;
    }

    private String sourceText(int token, int skipStart, int skipEnd) {
        return cursor.sourceTextAt(token, skipStart, skipEnd);
    }

    private String identifier(int t) {
        if (type(t) != BACKQUOTED_IDENTIFIER) {
            return sourceText(t, 0, 0);
        }
        String interior = sourceText(t, 1, 1);
        // Decode escaped backticks while preserving the owned source-span fast path.
        return QueryIdentifiers.decodeBackQuotedInterior(interior);
    }

    private static String currentFunctionName(int t) {
        return switch (t) {
            case CURRENT_DATE -> "CURRENT_DATE";
            case CURRENT_TIME -> "CURRENT_TIME";
            case CURRENT_TIMESTAMP -> "CURRENT_TIMESTAMP";
            case LOCALTIME -> "LOCALTIME";
            case LOCALTIMESTAMP -> "LOCALTIMESTAMP";
            case CURRENT_USER -> "CURRENT_USER";
            case CURRENT_ROLE -> "CURRENT_ROLE";
            case CURRENT_GROUP -> "CURRENT_GROUP";
            case CURRENT_WAREHOUSE -> "CURRENT_WAREHOUSE";
            default -> throw new IllegalArgumentException();
        };
    }

    private E number(int token) {
        long small = type(token) == INTEGER_VALUE ? cursor.smallUnsignedIntegerAt(token) : -1;
        String text = small < 0 ? text(token) : null;
        return construction.number(type(token), small, text, pos(token));
    }

    private E function(int start, List<String> parts) {
        return function(start, parts, false);
    }

    private E function(int start, List<String> parts, boolean dictionaryPrimary) {
        String full = parts.size() == 1 ? parts.get(0) : String.join(".", parts);
        int dot = full.indexOf('.');
        String name = (dot < 0 ? full : full.substring(dot + 1)).toLowerCase(Locale.ROOT);
        if (type(start) == PASSWORD && parts.size() == 1) {
            E password = passwordCall(start);
            if (password != null) {
                return password;
            }
        }
        boolean aggregate = parts.size() == 1 && isAggregateHead(type(start));
        boolean star = eat(ASTERISK_SYMBOL);
        boolean distinct = false;
        boolean quantified = false;
        if (input.LA(1) == DISTINCT || input.LA(1) == ALL) {
            if (!aggregate || type(start) == ARRAY_AGG_DISTINCT) {
                throw unsupported("aggregate modifier");
            }
            quantified = true;
            distinct = type(take()) == DISTINCT;
        }
        if (star && (!aggregate || !name.equals("count"))) {
            throw unsupported("star function arguments");
        }
        List<String> hints = null;
        if (quantified && input.LA(1) == BRACKET) {
            hints = quantifierHint();
            if (hints != null && type(start) != COUNT) {
                throw unsupported("hint outside COUNT");
            }
        }
        List<E> args = new ArrayList<>();
        if (!star && input.LA(1) != CLOSE) {
            try {
                args.add(expression(0));
                while (eat(COMMA)) {
                    args.add(expression(0));
                }
            } catch (ParsingException | SemanticException e) {
                // MAP/slice validate arity first; array_generate visits argument 3 first.
                // Keep one successful parse, delegating failed-child priority to AstBuilder.
                if (isAggregateHead(type(start))
                        || name.equals(FunctionSet.MAP)
                        || name.equals(FunctionSet.TIME_SLICE)
                        || name.equals(FunctionSet.DATE_SLICE)
                        || name.equals(FunctionSet.ARRAY_GENERATE)) {
                    throw unsupported(
                            "function argument constructor requires original error priority");
                }
                throw e;
            }
        }
        List<O> aggregateOrder = new ArrayList<>();
        boolean orderedAggregate =
                aggregate
                        && (type(start) == ARRAY_AGG
                                || type(start) == ARRAY_AGG_DISTINCT
                                || type(start) == GROUP_CONCAT);
        if (input.LA(1) == ORDER) {
            if (!orderedAggregate || args.isEmpty()) {
                throw unsupported("aggregate ORDER BY syntax");
            }
            take();
            expect(BY);
            aggregateOrder.add(sortItem());
            while (eat(COMMA)) {
                aggregateOrder.add(sortItem());
            }
        }
        int baseArgumentCount = args.size();
        boolean separator = false;
        if (input.LA(1) == SEPARATOR) {
            if (!aggregate || type(start) != GROUP_CONCAT || args.isEmpty()) {
                throw unsupported("aggregate SEPARATOR syntax");
            }
            take();
            args.add(expression(0));
            separator = true;
        }
        expect(CLOSE);
        // A bare FILTER is a column alias, as in "SELECT sum(a) FILTER".
        boolean filterAhead = input.LA(1) == FILTER && input.LA(2) == OPEN;
        int category =
                AggregateCallSyntax.classify(
                        type(start),
                        parts.size() == 1,
                        baseArgumentCount,
                        quantified,
                        star,
                        hints != null,
                        !aggregateOrder.isEmpty(),
                        separator,
                        filterAhead);
        if (category == AggregateCallSyntax.INVALID) {
            throw unsupported("call syntax outside original aggregate/generic contract");
        }
        aggregate = category == AggregateCallSyntax.AGGREGATE;
        E filterPredicate = null;
        if (filterAhead) {
            if (!aggregate) {
                throw unsupported("FILTER requires an aggregate function");
            }
            if ((distinct || name.equals(FunctionSet.ARRAY_AGG_DISTINCT)) && args.isEmpty()) {
                throw unsupported("empty DISTINCT arguments");
            }
            if (name.equalsIgnoreCase(FunctionSet.COUNT) && distinct) {
                throw unsupported("COUNT DISTINCT FILTER");
            }
            construction.validateFilterOrder(name, args, aggregateOrder, separator);
            take();
            expect(OPEN);
            expect(WHERE);
            filterPredicate = expression(0);
            expect(CLOSE);
        }
        boolean analytic = input.LA(1) == OVER;
        // Only the dedicated primary-expression alternative has a native dictionary AST.
        // Function-only entrypoints (PIVOT/ODBC), quoted/qualified/empty calls and OVER
        // retain the generic function contract. Nested argument expressions are normal primaries.
        if (dictionaryPrimary
                && parts.size() == 1
                && type(start) == DICTIONARY_GET
                && !args.isEmpty()
                && !analytic) {
            return construction.dictionary(args);
        }
        boolean reservedHead = false;
        if (parts.size() == 1) {
            reservedHead = switch (type(start)) {
                case CHAR, IF, LEFT, LIKE, MOD, REGEXP, REPLACE, RIGHT, RLIKE -> true;
                default -> false;
            };
        }
        boolean reservedArity = switch (type(start)) {
            case CHAR -> args.size() == 1;
            case LEFT, LIKE, MOD, REGEXP, RIGHT, RLIKE -> args.size() == 2;
            default -> true;
        };
        if (reservedHead && (analytic || !reservedArity)) {
            throw unsupported("reserved special function syntax");
        }
        String specializedName = null;
        if (parts.size() == 1 && type(start) != BACKQUOTED_IDENTIFIER) {
            specializedName = switch (type(start)) {
                case CHAR -> args.size() == 1 ? "char" : null;
                case DAY -> args.size() == 1 ? "day" : null;
                case HOUR -> args.size() == 1 ? "hour" : null;
                case MINUTE -> args.size() == 1 ? "minute" : null;
                case MONTH -> args.size() == 1 ? "month" : null;
                case QUARTER -> args.size() == 1 ? "quarter" : null;
                case SECOND -> args.size() == 1 ? "second" : null;
                case YEAR -> args.size() == 1 ? "year" : null;
                case LEFT -> args.size() == 2 ? "left" : null;
                case LIKE -> args.size() == 2 ? "like" : null;
                case MOD -> args.size() == 2 ? "mod" : null;
                case REGEXP, RLIKE -> args.size() == 2 ? "regexp" : null;
                case RIGHT -> args.size() == 2 ? "right" : null;
                case IF -> "if";
                case REPLACE -> "replace";
                default -> null;
            };
        }
        if (specializedName != null && !analytic) {
            return construction.specializedFunction(specializedName, args, pos(start));
        }
        construction.validateAnalyticRewrite(analytic, aggregate, name, args, type(start));
        if (analytic && !aggregate && construction.overIgnored(name, args)) {
            ignoredOvers++;
        }
        ExpressionConstruction.OverParts<E, O, W> over = analytic ? overClause() : null;
        construction.validateDistinctArguments(distinct, aggregate, name, args);
        return construction.finishFunction(
                full,
                name,
                parts,
                type(start),
                isIdentifier(type(start)),
                aggregate,
                star,
                distinct,
                quantified,
                hints,
                args,
                aggregateOrder,
                separator,
                filterPredicate,
                analytic,
                over,
                pos(start));
    }

    /** The PASSWORD '(' string ')' grammar alternative; null when the call is a generic one. */
    private E passwordCall(int start) {
        int literal = input.LA(1);
        boolean filter = input.LA(3) == FILTER && input.LA(4) == OPEN;
        if ((literal != SINGLE_QUOTED_TEXT && literal != DOUBLE_QUOTED_TEXT)
                || input.LA(2) != CLOSE
                || input.LA(3) == OVER
                || filter) {
            return null;
        }
        int token = take();
        take();
        return construction.password(stringValue(token), pos(start));
    }

    /**
     * After DISTINCT or ALL, '[' starts either a hint or an array literal. ANTLR takes the hint
     * whenever the tokens read as a hint and the rest of the call is valid, so we do the same and
     * fall back when the next token does not tell the two readings apart. Returns null for an array.
     */
    private List<String> quantifierHint() {
        if (!isIdentifier(input.LA(2))) {
            return null;
        }
        if (input.LA(3) == BITOR) {
            throw unsupported("skew hint in aggregate call");
        }
        int k = 2;
        while (isIdentifier(input.LA(k)) && input.LA(k + 1) == COMMA) {
            k += 2;
        }
        if (!isIdentifier(input.LA(k)) || input.LA(k + 1) != CLOSE_BRACKET) {
            return null;
        }
        int next = input.LA(k + 2);
        boolean likeOperator = (next == LIKE || next == RLIKE || next == REGEXP) && input.LA(k + 3) != OPEN;
        if (next == COMMA || likeOperator || isArrayOnlyContinuation(next)) {
            return null;
        }
        if (next != CLOSE && !startsHintArgument(next)) {
            throw unsupported("COUNT bracket-hint/array ambiguity requires original parser");
        }
        take();
        List<String> hints = new ArrayList<>();
        do {
            hints.add(text(take()));
        } while (eat(COMMA));
        expect(CLOSE_BRACKET);
        return hints;
    }

    // Tokens that continue an array literal but cannot start an argument after a hint.
    private static boolean isArrayOnlyContinuation(int t) {
        return switch (t) {
            case ASTERISK_SYMBOL,
                    SLASH_SYMBOL,
                    PERCENT_SYMBOL,
                    IN,
                    BETWEEN,
                    DOT,
                    DOT_IDENTIFIER,
                    ARROW,
                    CONCAT,
                    BITAND,
                    BITXOR,
                    BIT_SHIFT_LEFT,
                    BIT_SHIFT_RIGHT,
                    BIT_SHIFT_RIGHT_LOGICAL,
                    AND,
                    LOGICAL_AND,
                    OR,
                    LOGICAL_OR,
                    IS,
                    EQ,
                    NEQ,
                    LT,
                    LTE,
                    GT,
                    GTE,
                    EQ_FOR_NULL ->
                    true;
            default -> false;
        };
    }

    // Tokens that start an argument. They cannot continue an array literal, or ANTLR takes the hint
    // when both readings fit, so after "[ident]" they always mean a hint.
    private static boolean startsHintArgument(int t) {
        return switch (t) {
            case LETTER_IDENTIFIER,
                    DIGIT_IDENTIFIER,
                    BACKQUOTED_IDENTIFIER,
                    INTEGER_VALUE,
                    DECIMAL_VALUE,
                    DOUBLE_VALUE,
                    SINGLE_QUOTED_TEXT,
                    DOUBLE_QUOTED_TEXT,
                    PLUS_SYMBOL,
                    MINUS_SYMBOL,
                    BITNOT,
                    LOGICAL_NOT,
                    BINARY_SINGLE_QUOTED_TEXT,
                    BINARY_DOUBLE_QUOTED_TEXT,
                    NULL,
                    TRUE,
                    FALSE,
                    CASE,
                    CAST,
                    CONVERT,
                    DATE,
                    DATETIME,
                    INTERVAL,
                    EXISTS,
                    MAP,
                    ARRAY,
                    AT,
                    PARAMETER,
                    OPEN,
                    OPEN_BRACE,
                    BRACKET ->
                    true;
            default -> false;
        };
    }

    private static boolean isWindowHead(int t) {
        return switch (t) {
            case ROW_NUMBER,
                    RANK,
                    DENSE_RANK,
                    CUME_DIST,
                    PERCENT_RANK,
                    NTILE,
                    LEAD,
                    LAG,
                    FIRST_VALUE,
                    LAST_VALUE ->
                    true;
            default -> false;
        };
    }

    private static boolean isNullAwareWindowHead(int t) {
        return t == LEAD || t == LAG || t == FIRST_VALUE || t == LAST_VALUE;
    }

    private E windowFunction(int start) {
        int head = type(take());
        expect(OPEN);
        List<E> args = new ArrayList<>();
        boolean ignore = false;
        if (isNullAwareWindowHead(head)) {
            if (input.LA(1) != CLOSE) {
                args.add(expression(0));
                if (eat(IGNORE)) {
                    expect(NULLS);
                    ignore = true;
                }
                while (eat(COMMA)) {
                    args.add(expression(0));
                }
            }
        } else if (head == NTILE) {
            if (input.LA(1) != CLOSE) {
                args.add(expression(0));
            }
        }
        expect(CLOSE);
        if (isNullAwareWindowHead(head) && eat(IGNORE)) {
            expect(NULLS);
            ignore = true;
        }
        NodePosition functionPos = pos(start);
        if (input.LA(1) != OVER) {
            throw unsupported("reserved window function requires OVER");
        }
        E call =
                construction.windowCall(
                        sourceText(start, 0, 0).toLowerCase(Locale.ROOT),
                        args,
                        functionPos,
                        ignore);
        ExpressionConstruction.OverParts<E, O, W> over = overClause();
        return construction.over(call, over, pos(start));
    }

    private ExpressionConstruction.OverParts<E, O, W> overClause() {
        expect(OVER);
        expect(OPEN);
        List<String> hints = input.LA(1) == BRACKET ? windowHints() : null;
        List<E> partitions = new ArrayList<>();
        List<O> order = new ArrayList<>();
        if (hints != null || input.LA(1) == PARTITION) {
            expect(PARTITION);
            expect(BY);
            partitions.add(expression(0));
            while (eat(COMMA)) {
                partitions.add(expression(0));
            }
        }
        if (eat(ORDER)) {
            expect(BY);
            order.add(sortItem());
            while (eat(COMMA)) {
                order.add(sortItem());
            }
        }
        W window = (input.LA(1) == ROWS || input.LA(1) == RANGE) ? windowFrame() : null;
        expect(CLOSE);
        return new ExpressionConstruction.OverParts<>(partitions, order, window, hints);
    }

    /** The identifiers of "[sort]" style hints before PARTITION BY; the skew-boundary form falls back. */
    private List<String> windowHints() {
        expect(BRACKET);
        List<String> hints = new ArrayList<>();
        do {
            int t = input.LA(1);
            if (!isIdentifier(t)) {
                throw unsupported("window hint identifier");
            }
            hints.add(text(take()));
        } while (eat(COMMA));
        expect(CLOSE_BRACKET);
        return hints;
    }

    private O sortItem() {
        int start = input.index();
        boolean all = eat(ALL);
        E expr = all ? construction.orderAllLiteral() : expression(0);
        boolean asc = true;
        if (input.LA(1) == ASC || input.LA(1) == DESC) {
            asc = type(take()) == ASC;
        }
        boolean nullsFirst =
                (!SqlModeHelper.check(mode, SqlModeHelper.MODE_SORT_NULLS_LAST)) == asc;
        if (eat(NULLS)) {
            if (input.LA(1) != FIRST && input.LA(1) != LAST) {
                throw unsupported("NULLS ordering");
            }
            nullsFirst = type(take()) == FIRST;
        }
        return all
                ? construction.order(expr, asc, nullsFirst, pos(start), true)
                : construction.order(expr, asc, nullsFirst, pos(start), false);
    }

    private W windowFrame() {
        int start = take();
        AnalyticWindow.Type type =
                type(start) == RANGE ? AnalyticWindow.Type.RANGE : AnalyticWindow.Type.ROWS;
        if (eat(BETWEEN)) {
            B left = frameBound();
            expect(AND);
            B right = frameBound();
            return construction.window(type, left, right, pos(start));
        }
        return construction.window(type, frameBound(), pos(start));
    }

    private B frameBound() {
        if (input.LA(1) == UNBOUNDED && (input.LA(2) == PRECEDING || input.LA(2) == FOLLOWING)) {
            take();
            if (input.LA(1) != PRECEDING && input.LA(1) != FOLLOWING) {
                throw unsupported("unbounded frame direction");
            }
            return construction.windowBoundary(
                    type(take()) == PRECEDING
                            ? AnalyticWindowBoundary.BoundaryType.UNBOUNDED_PRECEDING
                            : AnalyticWindowBoundary.BoundaryType.UNBOUNDED_FOLLOWING,
                    null);
        }
        if (input.LA(1) == CURRENT && input.LA(2) == ROW) {
            take();
            expect(ROW);
            return construction.windowBoundary(
                    AnalyticWindowBoundary.BoundaryType.CURRENT_ROW, null);
        }
        E amount = expression(0);
        if (input.LA(1) != PRECEDING && input.LA(1) != FOLLOWING) {
            throw unsupported("bounded frame direction");
        }
        return construction.windowBoundary(
                type(take()) == PRECEDING
                        ? AnalyticWindowBoundary.BoundaryType.PRECEDING
                        : AnalyticWindowBoundary.BoundaryType.FOLLOWING,
                amount);
    }

    private T parseType() {
        if (budget != null) {
            budget.enterExpression();
        }
        depth++;
        try {
            if (depth > 512) {
                throw unsupported("prototype type nesting limit 512");
            }
            if (input.LA(1) == Token.EOF) {
                throw unsupported("type at EOF");
            }
            try {
                return constructType();
            } catch (IllegalArgumentException e) {
                throw unsupported("type constructor requires original path");
            }
        } finally {
            depth--;
            if (budget != null) {
                budget.exitExpression();
            }
        }
    }

    private T constructType() {
        int start = take();
        String spelling = text(start);
        String name = spelling.toUpperCase(Locale.ROOT);
        if (name.equals("ARRAY")) {
            expect(LT);
            T child = parseType();
            expect(GT);
            return construction.arrayType(child);
        }
        if (type(start) == MAP) {
            expect(LT);
            T key = parseType();
            // Original TypeParser rejects complex/JSON/metric keys before constructing the value
            // type.
            // Whole fallback retains parser/HintFactory/semantic error priority and exact key
            // position.
            construction.validateMapKey(key);
            expect(COMMA);
            T value = parseType();
            expect(GT);
            return construction.mapType(key, value);
        }
        if (type(start) == STRUCT) {
            expect(LT);
            ArrayList<F> fields = new ArrayList<>();
            do {
                if (!isIdentifier(input.LA(1))) {
                    throw unsupported("struct field identifier");
                }
                String field = identifier(take());
                T fieldType = parseType();
                fields.add(construction.structField(field, fieldType));
            } while (eat(COMMA));
            expect(GT);
            return construction.structType(fields);
        }
        int length = -1;
        int scale = -1;
        if ((input.LA(1) == OPEN) && !PARAMETER_TYPES.contains(name)) {
            throw unsupported("type parameter for " + name);
        }
        if (eat(OPEN)) {
            if (input.LA(1) != INTEGER_VALUE) {
                throw unsupported("type parameter");
            }
            length = construction.typeParameter(text(take()));
            if (eat(COMMA)) {
                if (input.LA(1) != INTEGER_VALUE) {
                    throw unsupported("type scale");
                }
                scale = construction.typeParameter(text(take()));
            }
            expect(CLOSE);
        }
        if (name.equals("SIGNED") || name.equals("UNSIGNED")) {
            if (input.LA(1) == INT || input.LA(1) == INTEGER) {
                take();
            }
            return construction.signedType(name);
        }
        return construction.scalarType(name, length, scale);
    }

    private String stringValue(int token) {
        String body = cursor.quotedBodyAt(token);
        if (body == null) {
            return stringValue(text(token));
        }
        return decodeStringBody(body, type(token) == SINGLE_QUOTED_TEXT ? '\'' : '"');
    }

    static String stringValue(String quoted) {
        char quote = quoted.charAt(0);
        return decodeStringBody(quoted.substring(1, quoted.length() - 1), quote);
    }

    private static String decodeStringBody(String s, char quote) {
        // Most literals contain no quote in their body; avoid constructing replacement strings for
        // them.
        if (s.indexOf(quote) >= 0) {
            s = s.replace("" + quote + quote, "" + quote);
        }
        if (s.indexOf('\\') < 0) {
            return s;
        }
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(++i);
                switch (next) {
                    case 'n' -> out.append('\n');
                    case 't' -> out.append('\t');
                    case 'r' -> out.append('\r');
                    case 'b' -> out.append('\b');
                    case '0' -> out.append('\0');
                    case 'Z' -> out.append('\032');
                    case '_', '%' -> out.append('\\').append(next);
                    default -> out.append(next);
                }
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
}
