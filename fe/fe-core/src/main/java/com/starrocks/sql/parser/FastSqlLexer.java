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

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.Vocabulary;

/**
 * Lexer of the fast query parser. It produces the same tokens as StarRocksLexer (StarRocksLex.g4) without the
 * ANTLR lexer simulator: the generated lexer reads every character through a CharStream and a DFA, and on large
 * BI queries that is a large part of the parse time.
 *
 * <p>We only want to lex the common shapes of SQL here. For input where matching the generated lexer needs more
 * care (a surrogate pair, U+3000 or '$' at a token start, a non-ASCII letter whose upper case is ASCII, an
 * unterminated quote or comment, a character no rule accepts) lex() returns false and the caller uses the ANTLR
 * lexer for the whole input.
 */
final class FastSqlLexer {
    /** Receives the tokens in input order. */
    interface Sink {
        /** A token on the default channel. */
        void token(int type, int start, int stop, int line, int column);

        /** A whitespace or comment token on the hidden channel. */
        void hidden();

        /** An optimizer hint on channel 2. */
        void hint(int type, int start, int stop, int line, int column);
    }

    private static final Vocabulary VOCABULARY = StarRocksLexer.VOCABULARY;

    // Character classes of ASCII characters.
    private static final byte OTHER = 0;
    private static final byte WORD_START = 1;
    private static final byte DIGIT = 2;
    private static final byte SPACE = 3;
    private static final byte[] CLASS = new byte[128];

    // Types of single-character literals, 0 when no token is that character.
    private static final int[] SINGLE = new int[128];

    private static final int[] KEYWORD_TYPES;
    private static final char[][] KEYWORD_TEXT;
    private static final int KEYWORD_MASK;

    private static final int DOT = literalType(".");
    private static final int DOTDOTDOT = literalType("...");
    private static final int ARROW = literalType("->");
    private static final int NAMED_ARROW = literalType("=>");
    private static final int EQ_FOR_NULL = literalType("<=>");
    private static final int LTE = literalType("<=");
    private static final int NEQ = StarRocksLexer.NEQ;
    private static final int GTE = literalType(">=");
    private static final int LOGICAL_AND = literalType("&&");
    private static final int LOGICAL_OR = literalType("||");
    private static final int ARRAY_ELEMENT = literalType("[*]");

    static {
        for (int c = 'A'; c <= 'Z'; c++) {
            CLASS[c] = WORD_START;
            CLASS[c + 'a' - 'A'] = WORD_START;
        }
        CLASS['_'] = WORD_START;
        for (int c = '0'; c <= '9'; c++) {
            CLASS[c] = DIGIT;
        }
        CLASS[' '] = SPACE;
        CLASS['\t'] = SPACE;
        CLASS['\r'] = SPACE;
        CLASS['\n'] = SPACE;

        int keywords = 0;
        for (int type = 1; type <= VOCABULARY.getMaxTokenType(); type++) {
            String text = literal(type);
            if (text == null) {
                continue;
            }
            if (isKeyword(text)) {
                keywords++;
            } else if (text.length() == 1 && text.charAt(0) < 128) {
                SINGLE[text.charAt(0)] = type;
            }
        }
        int size = Integer.highestOneBit(keywords * 4);
        KEYWORD_TYPES = new int[size];
        KEYWORD_TEXT = new char[size][];
        KEYWORD_MASK = size - 1;
        for (int type = 1; type <= VOCABULARY.getMaxTokenType(); type++) {
            String text = literal(type);
            if (text == null || !isKeyword(text)) {
                continue;
            }
            int hash = 0;
            for (int i = 0; i < text.length(); i++) {
                hash = hash * 31 + text.charAt(i);
            }
            int slot = mix(hash) & KEYWORD_MASK;
            while (KEYWORD_TEXT[slot] != null) {
                slot = (slot + 1) & KEYWORD_MASK;
            }
            KEYWORD_TEXT[slot] = text.toCharArray();
            KEYWORD_TYPES[slot] = type;
        }
    }

    private FastSqlLexer() {
    }

    private static String literal(int type) {
        String name = VOCABULARY.getLiteralName(type);
        return name == null ? null : name.substring(1, name.length() - 1);
    }

    private static boolean isKeyword(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (!(c >= 'A' && c <= 'Z' || c == '_' || i > 0 && c >= '0' && c <= '9')) {
                return false;
            }
        }
        return true;
    }

    private static int literalType(String text) {
        for (int type = 1; type <= VOCABULARY.getMaxTokenType(); type++) {
            if (text.equals(literal(type))) {
                return type;
            }
        }
        throw new IllegalStateException("no lexer token " + text);
    }

    private static int mix(int hash) {
        return hash ^ (hash >>> 16);
    }

    /** LETTER | DIGIT | '_' of StarRocksLex.g4; LETTER includes '$' and every non-ASCII character. */
    private static boolean identifierPart(char c) {
        return c < 128 ? CLASS[c] == WORD_START || CLASS[c] == DIGIT || c == '$' : !Character.isSurrogate(c);
    }

    /**
     * Lexes the whole SQL into the sink and returns true, or returns false when the caller must use the ANTLR
     * lexer. The sink may have received tokens before false is returned.
     */
    static boolean lex(String sql, long sqlMode, Sink sink) {
        final char[] text = sql.toCharArray();
        final int end = text.length;
        final boolean pipesAsConcat = (sqlMode & (1L << 1)) != 0;
        int line = 1;
        int column = 0;
        int p = 0;
        while (p < end) {
            char c = text[p];
            int start = p;
            int type;
            if (c < 128) {
                switch (CLASS[c]) {
                    case SPACE: {
                        do {
                            if (text[p] == '\n') {
                                line++;
                                column = 0;
                            } else {
                                column++;
                            }
                            p++;
                        } while (p < end && text[p] < 128 && CLASS[text[p]] == SPACE);
                        sink.hidden();
                        continue;
                    }
                    case WORD_START:
                        p = word(text, p, end);
                        if (p < 0) {
                            return false;
                        }
                        type = wordType(text, start, p);
                        if (type == 0) {
                            // 'X' followed by a quote may start a binary literal.
                            int binaryEnd = binary(text, start, p, end);
                            if (binaryEnd > 0) {
                                type = text[p] == '\'' ? StarRocksLexer.BINARY_SINGLE_QUOTED_TEXT
                                        : StarRocksLexer.BINARY_DOUBLE_QUOTED_TEXT;
                                p = binaryEnd;
                            } else {
                                type = StarRocksLexer.LETTER_IDENTIFIER;
                            }
                        }
                        break;
                    case DIGIT: {
                        long number = number(text, p, end);
                        p = (int) number;
                        type = (int) (number >>> 32);
                        break;
                    }
                    default:
                        p = -1;
                        type = 0;
                        switch (c) {
                            case '\'':
                            case '"':
                            case '`':
                                p = quoted(text, start, end);
                                type = c == '\'' ? StarRocksLexer.SINGLE_QUOTED_TEXT
                                        : c == '"' ? StarRocksLexer.DOUBLE_QUOTED_TEXT
                                        : StarRocksLexer.BACKQUOTED_IDENTIFIER;
                                break;
                            case '.':
                                if (start + 1 < end && text[start + 1] >= '0' && text[start + 1] <= '9') {
                                    long number = number(text, start, end);
                                    p = (int) number;
                                    type = (int) (number >>> 32);
                                } else if (start + 2 < end && text[start + 1] == '.' && text[start + 2] == '.') {
                                    p = start + 3;
                                    type = DOTDOTDOT;
                                } else {
                                    p = start + 1;
                                    type = DOT;
                                }
                                break;
                            case '-':
                                if (start + 1 < end && text[start + 1] == '-') {
                                    p = lineComment(text, start, end);
                                    if (p < 0) {
                                        return false;
                                    }
                                    // The comment may end with a line break.
                                    if (text[p - 1] == '\n') {
                                        line++;
                                        column = 0;
                                    } else {
                                        column += p - start;
                                    }
                                    sink.hidden();
                                    continue;
                                }
                                p = start + 1;
                                type = StarRocksLexer.MINUS_SYMBOL;
                                if (p < end && text[p] == '>') {
                                    p++;
                                    type = ARROW;
                                }
                                break;
                            case '/':
                                if (start + 1 < end && text[start + 1] == '*') {
                                    boolean hint = start + 2 < end && text[start + 2] == '+';
                                    p = blockCommentEnd(text, start + (hint ? 3 : 2), end);
                                    if (p < 0) {
                                        return false;
                                    }
                                    int startLine = line;
                                    int startColumn = column;
                                    for (int i = start; i < p; i++) {
                                        if (text[i] == '\n') {
                                            line++;
                                            column = 0;
                                        } else {
                                            column++;
                                        }
                                    }
                                    if (hint) {
                                        sink.hint(StarRocksLexer.OPTIMIZER_HINT, start, p - 1, startLine, startColumn);
                                    } else {
                                        sink.hidden();
                                    }
                                    continue;
                                }
                                p = start + 1;
                                type = StarRocksLexer.SLASH_SYMBOL;
                                break;
                            case '|':
                                p = start + 1;
                                type = StarRocksLexer.BITOR;
                                if (p < end && text[p] == '|') {
                                    p++;
                                    type = pipesAsConcat ? StarRocksParser.CONCAT : LOGICAL_OR;
                                }
                                break;
                            case '<':
                                p = start + 1;
                                type = StarRocksLexer.LT;
                                if (p < end && text[p] == '=') {
                                    p++;
                                    type = LTE;
                                    if (p < end && text[p] == '>') {
                                        p++;
                                        type = EQ_FOR_NULL;
                                    }
                                } else if (p < end && text[p] == '>') {
                                    p++;
                                    type = NEQ;
                                }
                                break;
                            case '>':
                                p = start + 1;
                                type = StarRocksLexer.GT;
                                if (p < end && text[p] == '=') {
                                    p++;
                                    type = GTE;
                                }
                                break;
                            case '=':
                                p = start + 1;
                                type = StarRocksLexer.EQ;
                                if (p < end && text[p] == '>') {
                                    p++;
                                    type = NAMED_ARROW;
                                }
                                break;
                            case '!':
                                p = start + 1;
                                type = StarRocksLexer.LOGICAL_NOT;
                                if (p < end && text[p] == '=') {
                                    p++;
                                    type = NEQ;
                                }
                                break;
                            case '&':
                                p = start + 1;
                                type = StarRocksLexer.BITAND;
                                if (p < end && text[p] == '&') {
                                    p++;
                                    type = LOGICAL_AND;
                                }
                                break;
                            case '[':
                                p = start + 1;
                                type = SINGLE['['];
                                if (start + 2 < end && text[start + 1] == '*' && text[start + 2] == ']') {
                                    p = start + 3;
                                    type = ARRAY_ELEMENT;
                                }
                                break;
                            default:
                                // '$' starts an identifier, '$$' or an attachment depending on what follows.
                                if (c != '$' && SINGLE[c] != 0) {
                                    p = start + 1;
                                    type = SINGLE[c];
                                }
                                break;
                        }
                        if (p < 0) {
                            return false;
                        }
                        break;
                }
            } else {
                // U+3000 is both whitespace and a letter; the generated lexer decides by rule order and length.
                if (c == 0x3000 || Character.isSurrogate(c)) {
                    return false;
                }
                p = word(text, p, end);
                if (p < 0) {
                    return false;
                }
                type = StarRocksLexer.LETTER_IDENTIFIER;
            }
            sink.token(type, start, p - 1, line, column);
            // Only quoted tokens can contain a line break.
            if (type == StarRocksLexer.SINGLE_QUOTED_TEXT || type == StarRocksLexer.DOUBLE_QUOTED_TEXT
                    || type == StarRocksLexer.BACKQUOTED_IDENTIFIER
                    || type == StarRocksLexer.BINARY_SINGLE_QUOTED_TEXT
                    || type == StarRocksLexer.BINARY_DOUBLE_QUOTED_TEXT) {
                for (int i = start; i < p; i++) {
                    if (text[i] == '\n') {
                        line++;
                        column = 0;
                    } else {
                        column++;
                    }
                }
            } else {
                column += p - start;
            }
        }
        sink.token(Token.EOF, end, end - 1, line, column);
        return true;
    }

    /** Returns the end of the word, or -1 when the generated lexer may read it differently. */
    private static int word(char[] text, int p, int end) {
        while (p < end) {
            char c = text[p];
            if (c < 128) {
                if (CLASS[c] == WORD_START || CLASS[c] == DIGIT || c == '$') {
                    p++;
                    continue;
                }
                return p;
            }
            // The generated lexer matches keywords on the upper case of the input. A non-ASCII letter whose
            // upper case is ASCII (dotless i, long s) could complete a keyword.
            if (Character.isSurrogate(c) || Character.toUpperCase(c) < 128) {
                return -1;
            }
            p++;
        }
        return p;
    }

    /** Keyword type of an ASCII word, or 0 for an identifier. */
    private static int wordType(char[] text, int start, int end) {
        int hash = 0;
        for (int i = start; i < end; i++) {
            char c = text[i];
            if (c >= 128) {
                return 0;
            }
            hash = hash * 31 + (c >= 'a' && c <= 'z' ? c - 32 : c);
        }
        int length = end - start;
        for (int slot = mix(hash) & KEYWORD_MASK; ; slot = (slot + 1) & KEYWORD_MASK) {
            char[] keyword = KEYWORD_TEXT[slot];
            if (keyword == null) {
                return 0;
            }
            if (keyword.length == length && sameUpperCase(keyword, text, start)) {
                return KEYWORD_TYPES[slot];
            }
        }
    }

    private static boolean sameUpperCase(char[] keyword, char[] text, int start) {
        for (int i = 0; i < keyword.length; i++) {
            char c = text[start + i];
            if ((c >= 'a' && c <= 'z' ? c - 32 : c) != keyword[i]) {
                return false;
            }
        }
        return true;
    }

    /** End of X'...' or X"...", or -1. BINARY_*_QUOTED_TEXT allows neither the quote nor a backslash inside. */
    private static int binary(char[] text, int start, int wordEnd, int end) {
        if (wordEnd != start + 1 || (text[start] != 'X' && text[start] != 'x') || wordEnd >= end) {
            return -1;
        }
        char quote = text[wordEnd];
        if (quote != '\'' && quote != '"') {
            return -1;
        }
        for (int p = wordEnd + 1; p < end; p++) {
            char c = text[p];
            if (c == quote) {
                return p + 1;
            }
            if (c == '\\') {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Longest match among INTEGER_VALUE, DECIMAL_VALUE, DOUBLE_VALUE, DIGIT_IDENTIFIER and DOT_IDENTIFIER that
     * starts at a digit or at '.' followed by a digit. On equal length the rule defined first wins.
     * Returns the token type in the high 32 bits and the end of the token in the low 32 bits.
     */
    private static long number(char[] text, int start, int end) {
        int p = start;
        boolean dotFirst = text[p] == '.';
        int type;
        if (dotFirst) {
            p++;
            while (p < end && text[p] >= '0' && text[p] <= '9') {
                p++;
            }
            type = StarRocksLexer.DECIMAL_VALUE;
        } else {
            while (p < end && text[p] >= '0' && text[p] <= '9') {
                p++;
            }
            type = StarRocksLexer.INTEGER_VALUE;
            if (p < end && text[p] == '.') {
                p++;
                while (p < end && text[p] >= '0' && text[p] <= '9') {
                    p++;
                }
                type = StarRocksLexer.DECIMAL_VALUE;
            }
        }
        if (p < end && (text[p] == 'E' || text[p] == 'e')) {
            int e = p + 1;
            if (e < end && (text[e] == '+' || text[e] == '-')) {
                e++;
            }
            if (e < end && text[e] >= '0' && text[e] <= '9') {
                while (e < end && text[e] >= '0' && text[e] <= '9') {
                    e++;
                }
                p = e;
                type = StarRocksLexer.DOUBLE_VALUE;
            }
        }
        // DIGIT_IDENTIFIER is DIGIT (LETTER | DIGIT | '_')+; DOT_IDENTIFIER is '.' DIGIT_IDENTIFIER.
        int identifierStart = dotFirst ? start + 1 : start;
        int q = identifierStart + 1;
        while (q < end && identifierPart(text[q])) {
            q++;
        }
        if (q - identifierStart >= 2 && q > p) {
            p = q;
            type = dotFirst ? StarRocksLexer.DOT_IDENTIFIER : StarRocksLexer.DIGIT_IDENTIFIER;
        }
        return ((long) type << 32) | p;
    }

    /** End of a quoted token starting at start, or -1 when it is not terminated. */
    private static int quoted(char[] text, int start, int end) {
        char quote = text[start];
        boolean backslash = quote != '`';
        int p = start + 1;
        while (p < end) {
            char c = text[p];
            if (backslash && c == '\\') {
                p += 2;
            } else if (c == quote) {
                if (p + 1 < end && text[p + 1] == quote) {
                    p += 2;
                } else {
                    return p + 1;
                }
            } else {
                if (Character.isSurrogate(c)) {
                    return -1;
                }
                p++;
            }
        }
        return -1;
    }

    /** '--' ~[\r\n]* '\r'? '\n'? */
    private static int lineComment(char[] text, int start, int end) {
        int p = start + 2;
        while (p < end && text[p] != '\r' && text[p] != '\n') {
            if (Character.isSurrogate(text[p])) {
                return -1;
            }
            p++;
        }
        if (p < end && text[p] == '\r') {
            p++;
        }
        if (p < end && text[p] == '\n') {
            p++;
        }
        return p;
    }

    /** End (exclusive) of the first '*' '/' that starts at or after from, or -1. */
    private static int blockCommentEnd(char[] text, int from, int end) {
        for (int p = from; p + 1 < end; p++) {
            if (text[p] == '*' && text[p + 1] == '/') {
                return p + 2;
            }
            if (Character.isSurrogate(text[p])) {
                return -1;
            }
        }
        return -1;
    }
}
