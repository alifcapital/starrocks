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

import com.google.gson.JsonParser;
import com.starrocks.qe.SqlModeHelper;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.Vocabulary;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FastSqlLexer replaces the generated lexer of the fast query parser. A token it reads differently would give the
 * parser a different statement, so we compare the whole token stream with the one of the generated lexer: every
 * token on the default channel with its type, offsets, line and column, the hints, and the counts the parser uses
 * for its limits. Inputs are the parser fixtures, every lexer literal, and generated mixtures of the tricky shapes.
 */
class FastSqlLexerTest {
    private static final String[] FIXTURES = {
            "sql/parser/fast-query-parser-fixtures.jsonl",
            "sql/parser/fast-query-parser-fixtures-window.jsonl",
            "sql/parser/fast-query-parser-fixtures-expression.jsonl",
            "sql/parser/fast-query-parser-fixtures-statement.jsonl",
    };
    private static final long[] MODES = {0, SqlModeHelper.MODE_PIPES_AS_CONCAT};

    // Fragments where the rules of StarRocksLex.g4 compete: numbers, dots, quotes, comments, hints, operators,
    // '$', non-ASCII letters, U+3000 and characters no rule accepts.
    private static final String[] FRAGMENTS = {
            "SELECT", "select", "from", "WHERE", "x", "X", "b", "_a", "a1", "a$b", "DIV", "BitShiftLeft",
            "1", "12", "1.", "1.5", ".5", "..", "...", ".", "1e5", "1E+5", "1e-", "1e", "1.e3", ".5e2", "12ab",
            "1_0", "t.1abc", ".1e5x", "0x1F", "x'ab'", "X\"ab\"", "x'a\\b'", "'a''b'", "'a\\'b'", "\"a\"\"b\"",
            "`a``b`", "'line\nbreak'", "'", "\"", "`", "-- comment\n", "--x\r\n", "--", "/* c */", "/**/", "/***/",
            "/*+ SET_VAR(a=1) */", "/*+*/", "/* unterminated", "||", "|", "&&", "&", "->", "->>", "=>", "<=>",
            "<=", "<>", "!=", "!", ">=", "<", ">", "=", "[*]", "[", "]", "(", ")", ",", ";", ":", "{", "}", "?",
            "@", "@@", "$$", "$$x$$", "$a", "$", "#", "\\", "+", "-", "*", "/", "%", "^", "~", "é", "日本",
            "ıd", "ſelect", "\u3000", "a\u3000b", "😀", " ", "  ", "\t", "\n", "\r\n", "\r",
    };

    @Test
    void literalsMatchGeneratedLexer() {
        Vocabulary vocabulary = StarRocksLexer.VOCABULARY;
        List<String> inputs = new ArrayList<>();
        for (int type = 1; type <= vocabulary.getMaxTokenType(); type++) {
            String literal = vocabulary.getLiteralName(type);
            if (literal != null) {
                String text = literal.substring(1, literal.length() - 1);
                inputs.add(text);
                inputs.add(text.toLowerCase());
                inputs.add("a " + text + " b");
            }
        }
        compareAll(inputs);
    }

    @Test
    void fixturesMatchGeneratedLexer() throws IOException {
        List<String> inputs = new ArrayList<>();
        for (String file : FIXTURES) {
            try (InputStream input = getClass().getClassLoader().getResourceAsStream(file)) {
                inputs.addAll(readJsonLines(input));
            }
        }
        int fast = compareAll(inputs);
        assertTrue(fast > inputs.size(), "the fast lexer reads most fixtures");
    }

    @Test
    void generatedMixturesMatchGeneratedLexer() {
        Random random = new Random(20261003);
        List<String> inputs = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            StringBuilder sql = new StringBuilder();
            int parts = 1 + random.nextInt(8);
            for (int j = 0; j < parts; j++) {
                sql.append(FRAGMENTS[random.nextInt(FRAGMENTS.length)]);
                if (random.nextInt(3) == 0) {
                    sql.append(' ');
                }
            }
            inputs.add(sql.toString());
        }
        int fast = compareAll(inputs);
        System.out.printf("generated %d inputs, %d lexings by the fast lexer%n", inputs.size(), fast);
    }

    /** Run with -Dfast.parser.lexer.corpus=file (jsonl or the binary corpus format) to compare on other SQL. */
    @Test
    @Timeout(value = 2, unit = TimeUnit.HOURS)
    void corpusMatchesGeneratedLexer() throws IOException {
        String corpus = System.getProperty("fast.parser.lexer.corpus");
        Assumptions.assumeTrue(corpus != null, "set -Dfast.parser.lexer.corpus to compare the lexers on a corpus");
        List<String> inputs;
        try (InputStream input = Files.newInputStream(Path.of(corpus))) {
            inputs = corpus.endsWith(".bin") ? readBinary(input) : readJsonLines(input);
        }
        int fast = compareAll(inputs);
        System.out.printf("corpus %d statements, %d lexings by the fast lexer%n", inputs.size(), fast);
    }

    /** Returns how many lexings the fast lexer did. */
    private static int compareAll(List<String> inputs) {
        int fast = 0;
        for (String sql : inputs) {
            for (long mode : MODES) {
                fast += compare(sql, mode) ? 1 : 0;
            }
        }
        return fast;
    }

    private static boolean compare(String sql, long mode) {
        DenseOwnedTokenStream expected;
        try {
            expected = new DenseOwnedTokenStream(sql, mode, false);
        } catch (RuntimeException e) {
            // The generated lexer rejects the input; the fast lexer must not produce tokens for it.
            try {
                DenseOwnedTokenStream actual = new DenseOwnedTokenStream(sql, mode, true);
                assertTrue(!actual.lexedByFastLexer(), "fast lexer accepts what the generated lexer rejects: " + sql);
            } catch (RuntimeException ignored) {
                // Both reject.
            }
            return false;
        }
        DenseOwnedTokenStream actual = new DenseOwnedTokenStream(sql, mode, true);
        String context = "mode " + mode + ": " + sql;
        assertEquals(describe(expected), describe(actual), context);
        return actual.lexedByFastLexer();
    }

    private static String describe(DenseOwnedTokenStream tokens) {
        StringBuilder out = new StringBuilder();
        int size = tokens.getNumberOfOnChannelTokens();
        out.append("tokens ").append(size)
                .append(" raw ").append(tokens.originalRawNonEOFCount())
                .append(" commas ").append(tokens.commaCount())
                .append(" semicolons ").append(tokens.semicolonCount())
                .append(" parameters ").append(java.util.Arrays.toString(tokens.parameterOffsets()))
                .append('\n');
        for (int i = 0; i < size; i++) {
            out.append(i).append(' ').append(tokens.typeAt(i))
                    .append(' ').append(tokens.startAt(i)).append('-').append(tokens.stopAt(i))
                    .append(' ').append(tokens.lineAt(i)).append(':').append(tokens.columnAt(i))
                    .append(' ').append(tokens.textAt(i)).append('\n');
        }
        for (Token hint : tokens.allHints()) {
            out.append("hint ").append(hint.getType()).append(' ').append(hint.getTokenIndex())
                    .append(' ').append(hint.getStartIndex()).append('-').append(hint.getStopIndex())
                    .append(' ').append(hint.getLine()).append(':').append(hint.getCharPositionInLine())
                    .append(' ').append(hint.getText()).append('\n');
        }
        for (int i = 0; i < size; i++) {
            List<Token> after = tokens.getHiddenTokensToRight(i, 2);
            if (after != null) {
                out.append("hints after ").append(i).append(' ').append(after.size()).append('\n');
            }
        }
        List<Token> first = tokens.getHiddenTokensToLeft(0, 2);
        if (first != null) {
            out.append("hints first ").append(first.size()).append('\n');
        }
        return out.toString();
    }

    private static List<String> readJsonLines(InputStream input) throws IOException {
        List<String> sqls = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
            if (!line.isBlank()) {
                sqls.add(JsonParser.parseString(line).getAsJsonObject().get("sql").getAsString());
            }
        }
        return sqls;
    }

    private static List<String> readBinary(InputStream input) throws IOException {
        DataInputStream data = new DataInputStream(new BufferedInputStream(input));
        int count = data.readInt();
        List<String> sqls = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            readString(data);
            sqls.add(readString(data));
        }
        return sqls;
    }

    private static String readString(DataInputStream data) throws IOException {
        byte[] bytes = new byte[data.readInt()];
        data.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
