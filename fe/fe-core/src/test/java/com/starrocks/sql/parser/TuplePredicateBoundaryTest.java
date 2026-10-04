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

import com.starrocks.qe.SessionVariable;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TuplePredicateBoundaryTest {
    private static final String TUPLE = "(a,b) IN (SELECT x,y FROM t)";
    private static final long[] MODES = {0, 2, 32, 34, 68719476736L, 68719476738L, 68719476768L, 68719476770L};

    // All production public parse entries share this boundary. Reflection avoids GlobalStateMgr
    // initialization and proves validation happens before hints or any custom AST factory.
    private static Object parse(String sql, long mode,
                                Function<StarRocksParser, ParserRuleContext> entry) throws Exception {
        Method invoke = SqlParser.class.getDeclaredMethod("invokeParser", String.class,
                SessionVariable.class, Function.class);
        invoke.setAccessible(true);
        SessionVariable session = new SessionVariable();
        session.setSqlMode(mode);
        try {
            return invoke.invoke(null, sql, session, entry);
        } catch (InvocationTargetException failure) {
            if (failure.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw failure;
        }
    }

    private static void rejected(String sql, long mode,
                                 Function<StarRocksParser, ParserRuleContext> entry) {
        ParsingException failure = assertThrows(ParsingException.class, () -> parse(sql, mode, entry));
        assertTrue(failure.getMessage().contains("Parentheses required around predicate"));
    }

    @Test
    void retainsPredicateContextsAndExplicitGrouping() {
        for (long mode : MODES) {
            for (String sql : List.of("SELECT " + TUPLE, "SELECT " + TUPLE + "=c",
                    "SELECT c=" + TUPLE, "SELECT a BETWEEN 1 AND " + TUPLE,
                    "SELECT (" + TUPLE + ")+1", "SELECT NOT " + TUPLE,
                    "SELECT " + TUPLE + " AND c", "SELECT " + TUPLE + " IS NULL")) {
                assertDoesNotThrow(() -> parse(sql, mode, StarRocksParser::sqlStatements));
            }
            assertDoesNotThrow(() -> parse(TUPLE, mode, StarRocksParser::expressionSingleton));
            assertDoesNotThrow(() -> parse(TUPLE + ",a", mode, StarRocksParser::expressionList));
            assertDoesNotThrow(() -> parse("COLUMNS(x=" + TUPLE + ")", mode, StarRocksParser::importColumns));
        }
    }

    @Test
    void rejectsPrimaryAndValueContinuationsBeforeAnyVisitor() {
        for (long mode : MODES) {
            for (String sql : List.of("SELECT -" + TUPLE, "SELECT +" + TUPLE,
                    "SELECT " + TUPLE + "+1", "SELECT 1+" + TUPLE,
                    "SELECT " + TUPLE + "[1]", "SELECT " + TUPLE + "->'k'",
                    "SELECT " + TUPLE + " MATCH c", "SELECT " + TUPLE + " COLLATE utf8",
                    "SELECT " + TUPLE + " IN (1)", "SELECT " + TUPLE + " NOT IN (1)")) {
                rejected(sql, mode, StarRocksParser::sqlStatements);
            }
            rejected(TUPLE, mode, StarRocksParser::valueExpression);
            rejected(TUPLE, mode, StarRocksParser::primaryExpression);
            rejected(TUPLE + "+1", mode, StarRocksParser::expressionSingleton);
            rejected(TUPLE + "+1,a", mode, StarRocksParser::expressionList);
            rejected("COLUMNS(x=" + TUPLE + "+1)", mode, StarRocksParser::importColumns);
        }
    }

    @Test
    void rejectsInvalidTupleEvenWhenLikeCausesWhereVisitorToBeSkipped() {
        for (long mode : MODES) {
            for (String show : List.of("SHOW TABLES", "SHOW TABLE STATUS", "SHOW DATABASES")) {
                rejected(show + " LIKE 'x' WHERE " + TUPLE + "+1", mode, StarRocksParser::sqlStatements);
            }
            assertDoesNotThrow(() -> parse("SHOW TABLES LIKE 'x' WHERE (" + TUPLE + ")+1",
                    mode, StarRocksParser::sqlStatements));
        }
    }

    @SuppressWarnings("unchecked")
    private static List<StarRocksParser.ParenthesizedExpressionContext> tupleContexts(StarRocksParser parser)
            throws ReflectiveOperationException {
        PostProcessListener listener = (PostProcessListener) parser.getParseListeners().stream()
                .filter(item -> item instanceof PostProcessListener).findFirst().orElseThrow();
        Field field = PostProcessListener.class.getDeclaredField("tupleContexts");
        field.setAccessible(true);
        return (List<StarRocksParser.ParenthesizedExpressionContext>) field.get(listener);
    }

    @Test
    void abandonsSllTupleCollectionBeforeLlRetry() {
        for (long mode : MODES) {
            AtomicInteger attempts = new AtomicInteger();
            assertDoesNotThrow(() -> parse("SELECT " + TUPLE, mode, parser -> {
                ParserRuleContext tree = parser.sqlStatements();
                if (attempts.incrementAndGet() == 1) {
                    try {
                        // Poison the abandoned tree only. Reusing it during LL validation would fail.
                        tupleContexts(parser).get(0).setParent(new StarRocksParser.PrimaryExpressionContext(null, 0));
                    } catch (ReflectiveOperationException failure) {
                        throw new AssertionError(failure);
                    }
                    throw new ParseCancellationException("force SLL to LL retry after tuple collection");
                }
                return tree;
            }));
            assertEquals(2, attempts.get());
        }
    }
}
