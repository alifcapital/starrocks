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

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

/** Batched lexer for whitespace, complete quoted tokens and ASCII words. */
public final class UnifiedBatchedLexer extends StarRocksLexer {

    // Build-time catalog preserves the exact dense trie layout and lookup path.
    // The generated superclass and originalToken() are still used.
    private static final int[][] LITERAL_EDGES = FrozenTokenCatalog.LITERAL_EDGES;
    private static final int[] LITERAL_TYPES = FrozenTokenCatalog.LITERAL_TYPES;
    private static final int[][] EDGES = FrozenTokenCatalog.KEYWORD_EDGES;
    private static final int[] TYPES = FrozenTokenCatalog.KEYWORD_TYPES;
    private static final int DOT_TYPE = FrozenTokenCatalog.DOT_TYPE;

    private CharStream original;
    private boolean buffered;
    private boolean batchToken;
    private long batchedTokens;
    private long batchedCharacters;
    private int lastBatchedTextStart = -1;

    public UnifiedBatchedLexer(CharStream original) {
        super(new AsciiCaseInsensitiveStream(original));
        this.original = original;
        buffered = knownBuffered(original);
        UnicodeLexerATNSimulator.install(this);
    }

    private static boolean knownBuffered(CharStream input) {
        if (input == null) {
            return false;
        }
        Class<?> c = input.getClass();
        if (c == SqlTextStream.class || c == org.antlr.v4.runtime.ANTLRInputStream.class) {
            return true;
        }
        // Only the runtime's concrete buffered code-point streams, never user subclasses.
        String name = c.getName();
        return name.equals("org.antlr.v4.runtime.CodePointCharStream$CodePoint8BitCharStream")
                || name.equals("org.antlr.v4.runtime.CodePointCharStream$CodePoint16BitCharStream")
                || name.equals("org.antlr.v4.runtime.CodePointCharStream$CodePoint32BitCharStream");
    }

    @Override
    public void setInputStream(IntStream input) {
        original = (CharStream) input;
        buffered = knownBuffered(original);
        super.setInputStream(original == null ? null : new AsciiCaseInsensitiveStream(original));
    }

    private static boolean whitespace(int c) {
        return c == ' ' || c == '\r' || c == '\n' || c == '\t' || c == 0x3000;
    }

    private static boolean asciiStart(int character) {
        return character >= 'A' && character <= 'Z'
                || character >= 'a' && character <= 'z'
                || character == '_';
    }

    /** Exact ASCII LETTER|DIGIT|_: LETTER includes '$' as well as '_'. */
    private static int symbol(int character) {
        if (character >= 'a' && character <= 'z') {
            return character - 'a';
        }
        if (character >= 'A' && character <= 'Z') {
            return character - 'A';
        }
        if (character >= '0' && character <= '9') {
            return 26 + character - '0';
        }
        if (character == '_') {
            return 36;
        }
        if (character == '$') {
            return 37;
        }
        return -1;
    }

    @Override
    public Token nextToken() {
        batchToken = false;
        if (_input == null || _hitEOF || _mode != DEFAULT_MODE || !buffered) {
            return originalToken();
        }
        int first = original.LA(1);
        if (first != 0x3000 && whitespace(first)) {
            int length = 0;
            int line = getLine();
            int column = getCharPositionInLine();
            int c = first;
            do {
                if (c == '\n') {
                    line++;
                    column = 0;
                } else {
                    column++;
                }
                length++;
                c = original.LA(length + 1);
            } while (whitespace(c));
            return emitScanned(WS, HIDDEN, length, line, column, c);
        }
        if (first == '`' || first == '\'' || first == '"') {
            Token token =
                    quoted(
                            first,
                            first != '`',
                            first == '`'
                                    ? BACKQUOTED_IDENTIFIER
                                    : first == '\'' ? SINGLE_QUOTED_TEXT : DOUBLE_QUOTED_TEXT);
            if (token != null) {
                return token;
            }
            // Incomplete quote: original ATN decides accepted prefixes/errors.
            return originalToken();
        }
        // Exact StarRocksLex.g4 competing prefixes: decimal/dot-identifier/ellipsis,
        // line-comment/arrow, and block-comment/optimizer-hint. No speculative consume.
        if (first == '.' || first == '-' || first == '/') {
            int next = original.LA(2);
            int type =
                    first == '.'
                            ? (next >= '0' && next <= '9' || next == '.' ? 0 : DOT_TYPE)
                            : first == '-'
                                    ? (next == '-' || next == '>' ? 0 : MINUS_SYMBOL)
                                    : next == '*' ? 0 : SLASH_SYMBOL;
            if (type != 0) {
                return emitScanned(
                        type,
                        DEFAULT_TOKEN_CHANNEL,
                        1,
                        getLine(),
                        getCharPositionInLine() + 1,
                        next);
            }
            return originalToken();
        }
        // Only the INTEGER_VALUE branch with no competing numeric/identifier prefix.
        // Any decimal, exponent, digit-identifier or Unicode continuation stays on ATN.
        if (first >= '0' && first <= '9') {
            int length = 1;
            int next = original.LA(2);
            while (next >= '0' && next <= '9') {
                length++;
                next = original.LA(length + 1);
            }
            if (next == '.' || asciiStart(next) || next == '$' || next >= 128) {
                return originalToken();
            }
            return emitScanned(
                    INTEGER_VALUE,
                    DEFAULT_TOKEN_CHANNEL,
                    length,
                    getLine(),
                    getCharPositionInLine() + length,
                    next);
        }
        if (first >= 0 && first < 128 && LITERAL_EDGES[1][first] != 0) {
            int c = first;
            int node = 1;
            int length = 0;
            int bestLength = 0;
            int bestType = 0;
            while (c >= 0 && c < 128 && LITERAL_EDGES[node][c] != 0) {
                node = LITERAL_EDGES[node][c];
                length++;
                if (LITERAL_TYPES[node] != 0) {
                    bestType = LITERAL_TYPES[node];
                    bestLength = length;
                }
                c = original.LA(length + 1);
            }
            if (bestType != 0) {
                return emitScanned(
                        bestType,
                        DEFAULT_TOKEN_CHANNEL,
                        bestLength,
                        getLine(),
                        getCharPositionInLine() + bestLength,
                        bestLength == length ? c : original.LA(bestLength + 1));
            }
        }
        if (asciiStart(first)) {
            int length = 0;
            int node = 1;
            int c = first;
            for (; ; ) {
                if (c >= 128) {
                    return originalToken();
                }
                int letter = symbol(c);
                if (letter < 0) {
                    break;
                }
                node = EDGES[node][letter];
                length++;
                c = original.LA(length + 1);
            }
            if (length == 1
                    && (first == 'X' || first == 'x' || first == 'B' || first == 'b')
                    && (c == '\'' || c == '"')) {
                return originalToken();
            }
            int type = TYPES[node] == 0 ? LETTER_IDENTIFIER : TYPES[node];
            return emitScanned(
                    type,
                    DEFAULT_TOKEN_CHANNEL,
                    length,
                    getLine(),
                    getCharPositionInLine() + length,
                    c);
        }
        return originalToken(); // Numbers, comments, mode-sensitive || and non-ASCII retain ANTLR.
    }

    /** Private owned tape only: public nextToken/factory callbacks remain unchanged. */
    boolean ownedFill(String sql, DenseOwnedTokenStream sink, Token nonEOFMarker, Token eofMarker) {
        if (original == null
                || original.getClass() != SqlTextStream.class
                || original.size() != sql.length()
                || original.toString() != sql
                || _input == null
                || _hitEOF
                || _mode != DEFAULT_MODE
                || sink.getTokenSource() != this) {
            return false;
        }
        final int end = sql.length();
        int offset = _input.index();
        int line = getLine();
        int column = getCharPositionInLine();
        int lastStart = -1;
        int lastLine = 0;
        int lastColumn = 0;
        int lastType = 0;
        int lastChannel = 0;
        long fastTokens = batchedTokens;
        long fastCharacters = batchedCharacters;
        boolean pending = false;
        Token pendingToken = null;
        try {
            for (; ; ) {
                if (offset == end) {
                    if (pending) {
                        checkpointOwned(
                                offset,
                                line,
                                column,
                                lastStart,
                                lastLine,
                                lastColumn,
                                lastType,
                                lastChannel,
                                pendingToken,
                                fastTokens,
                                fastCharacters,
                                true);
                        pending = false;
                    }
                    if (nextToken() != eofMarker) {
                        throw new IllegalStateException("Owned EOF emission changed");
                    }
                    return true;
                }
                int first = sql.charAt(offset);
                int nextOffset = -1;
                int type = 0;
                int channel = DEFAULT_TOKEN_CHANNEL;
                int endLine = line;
                int endColumn = column;
                if (_mode != DEFAULT_MODE) {
                    // Future lexer modes continue through the unchanged lexer.
                } else if (first != 0x3000 && whitespace(first)) {
                    int scan = offset;
                    int c = first;
                    do {
                        if (c == '\n') {
                            endLine++;
                            endColumn = 0;
                        } else {
                            endColumn++;
                        }
                        scan++;
                        c = scan < end ? sql.charAt(scan) : CharStream.EOF;
                    } while (whitespace(c));
                    nextOffset = scan;
                    type = WS;
                    channel = HIDDEN;
                } else if (first == '`' || first == '\'' || first == '"') {
                    boolean backslash = first != '`';
                    int scan = offset + 1;
                    endColumn++;
                    while (scan < end) {
                        int c = sql.charAt(scan);
                        if (backslash && c == '\\') {
                            if (scan + 1 == end) {
                                break;
                            }
                            int escaped = sql.charAt(scan + 1);
                            endColumn++;
                            if (escaped == '\n') {
                                endLine++;
                                endColumn = 0;
                            } else {
                                endColumn++;
                            }
                            scan += 2;
                        } else if (c == first) {
                            endColumn++;
                            int after = scan + 1 < end ? sql.charAt(scan + 1) : CharStream.EOF;
                            if (after != first) {
                                nextOffset = scan + 1;
                                type =
                                        first == '`'
                                                ? BACKQUOTED_IDENTIFIER
                                                : first == '\''
                                                        ? SINGLE_QUOTED_TEXT
                                                        : DOUBLE_QUOTED_TEXT;
                                break;
                            }
                            endColumn++;
                            scan += 2;
                        } else {
                            if (c == '\n') {
                                endLine++;
                                endColumn = 0;
                            } else {
                                endColumn++;
                            }
                            scan++;
                        }
                    }
                } else if (first == '.' || first == '-' || first == '/') {
                    int next = offset + 1 < end ? sql.charAt(offset + 1) : CharStream.EOF;
                    type =
                            first == '.'
                                    ? (next >= '0' && next <= '9' || next == '.' ? 0 : DOT_TYPE)
                                    : first == '-'
                                            ? (next == '-' || next == '>' ? 0 : MINUS_SYMBOL)
                                            : next == '*' ? 0 : SLASH_SYMBOL;
                    if (type != 0) {
                        nextOffset = offset + 1;
                        endColumn++;
                    }
                } else if (first >= '0' && first <= '9') {
                    int scan = offset + 1;
                    int next = scan < end ? sql.charAt(scan) : CharStream.EOF;
                    while (next >= '0' && next <= '9') {
                        scan++;
                        next = scan < end ? sql.charAt(scan) : CharStream.EOF;
                    }
                    if (next != '.' && !asciiStart(next) && next != '$' && next < 128) {
                        nextOffset = scan;
                        type = INTEGER_VALUE;
                        endColumn += scan - offset;
                    }
                } else if (first < 128 && LITERAL_EDGES[1][first] != 0) {
                    int c = first;
                    int node = 1;
                    int scan = offset;
                    int best = -1;
                    while (c >= 0 && c < 128 && LITERAL_EDGES[node][c] != 0) {
                        node = LITERAL_EDGES[node][c];
                        scan++;
                        if (LITERAL_TYPES[node] != 0) {
                            type = LITERAL_TYPES[node];
                            best = scan;
                        }
                        c = scan < end ? sql.charAt(scan) : CharStream.EOF;
                    }
                    if (type != 0) {
                        nextOffset = best;
                        endColumn += best - offset;
                    }
                } else if (asciiStart(first)) {
                    int scan = offset;
                    int node = 1;
                    int c = first;
                    boolean unicode = false;
                    for (; ; ) {
                        if (c >= 128) {
                            unicode = true;
                            break;
                        }
                        int letter = symbol(c);
                        if (letter < 0) {
                            break;
                        }
                        node = EDGES[node][letter];
                        scan++;
                        c = scan < end ? sql.charAt(scan) : CharStream.EOF;
                    }
                    if (!unicode
                            && !(scan - offset == 1
                                    && (first == 'X'
                                            || first == 'x'
                                            || first == 'B'
                                            || first == 'b')
                                    && (c == '\'' || c == '"'))) {
                        type = TYPES[node] == 0 ? LETTER_IDENTIFIER : TYPES[node];
                        nextOffset = scan;
                        endColumn += scan - offset;
                    }
                }
                if (type != 0) {
                    lastStart = offset;
                    lastLine = line;
                    lastColumn = column;
                    lastType = type;
                    lastChannel = channel;
                    fastTokens++;
                    fastCharacters += nextOffset - offset;
                    offset = nextOffset;
                    line = endLine;
                    column = endColumn;
                    pending = true;
                    pendingToken = null;
                    // No external callbacks: restore the pre-factory lexer state if append throws.
                    sink.appendFields(
                            _tokenFactorySourcePair,
                            type,
                            null,
                            channel,
                            lastStart,
                            offset - 1,
                            lastLine,
                            lastColumn);
                    pendingToken = nonEOFMarker;
                    continue;
                }
                if (pending) {
                    checkpointOwned(
                            offset,
                            line,
                            column,
                            lastStart,
                            lastLine,
                            lastColumn,
                            lastType,
                            lastChannel,
                            pendingToken,
                            fastTokens,
                            fastCharacters,
                            false);
                    pending = false;
                }
                Token token = nextToken(); // No speculative consume of an unknown lexeme.
                if (token == eofMarker) {
                    return true;
                }
                if (token != nonEOFMarker) {
                    throw new IllegalStateException("Unexpected owned token emission");
                }
                offset = _input.index();
                line = getLine();
                column = getCharPositionInLine();
                fastTokens = batchedTokens;
                fastCharacters = batchedCharacters;
            }
        } finally {
            if (pending) {
                checkpointOwned(
                        offset,
                        line,
                        column,
                        lastStart,
                        lastLine,
                        lastColumn,
                        lastType,
                        lastChannel,
                        pendingToken,
                        fastTokens,
                        fastCharacters,
                        offset == end);
            }
        }
    }

    /** Checkpoint only at a lexer boundary, EOF, or a failing primitive append. */
    private void checkpointOwned(
            int offset,
            int line,
            int column,
            int start,
            int startLine,
            int startColumn,
            int type,
            int channel,
            Token token,
            long tokens,
            long characters,
            boolean eof) {
        _input.seek(offset);
        getInterpreter().setLine(line);
        getInterpreter().setCharPositionInLine(column);
        _tokenStartCharIndex = start;
        _tokenStartLine = startLine;
        _tokenStartCharPositionInLine = startColumn;
        _type = type;
        _channel = channel;
        _text = null;
        _token = token;
        _hitEOF = eof;
        batchToken = true;
        lastBatchedTextStart = start;
        batchedTokens = tokens;
        batchedCharacters = characters;
    }

    private Token originalToken() {
        // EOF-only emissions do not perform a new simulator match. Preserve its
        // equivalent last text start; an actual ATN match becomes authoritative.
        if (!_hitEOF) {
            lastBatchedTextStart = -1;
        }
        return super.nextToken();
    }

    /** Look ahead once, counting exactly LexerATNSimulator's LF-only positions. */
    private Token quoted(int quote, boolean backslash, int type) {
        int offset = 2;
        int line = getLine();
        int column = getCharPositionInLine() + 1;
        for (; ; ) {
            int c = original.LA(offset);
            if (c == CharStream.EOF) {
                return null;
            }
            if (backslash && c == '\\') {
                int escaped = original.LA(offset + 1);
                if (escaped == CharStream.EOF) {
                    return null;
                }
                column++; // Backslash itself cannot be LF.
                if (escaped == '\n') {
                    line++;
                    column = 0;
                } else {
                    column++;
                }
                offset += 2;
            } else if (c == quote) {
                column++;
                int after = original.LA(offset + 1);
                if (after != quote) {
                    return emitScanned(type, DEFAULT_TOKEN_CHANNEL, offset, line, column, after);
                }
                column++;
                offset += 2;
            } else {
                if (c == '\n') {
                    line++;
                    column = 0;
                } else {
                    column++;
                }
                offset++;
            }
        }
    }

    private Token emitScanned(
            int type, int channel, int length, int line, int column, int nextCharacter) {
        int marker = _input.mark();
        try {
            _token = null;
            _tokenStartCharIndex = _input.index();
            _tokenStartLine = getLine();
            _tokenStartCharPositionInLine = getCharPositionInLine();
            _text = null;
            _type = type;
            _channel = channel;
            _input.seek(_tokenStartCharIndex + length);
            getInterpreter().setLine(line);
            getInterpreter().setCharPositionInLine(column);
            if (nextCharacter == CharStream.EOF) {
                _hitEOF = true;
            }
            batchToken = true;
            lastBatchedTextStart = _tokenStartCharIndex;
            batchedTokens++;
            batchedCharacters += length;
            return emit(); // Original factory and source Pair remain authoritative.
        } finally {
            _input.release(marker);
        }
    }

    @Override
    public String getText() {
        if (_text == null && (batchToken || (_hitEOF && lastBatchedTextStart >= 0))) {
            return _input.getText(
                    Interval.of(
                            batchToken ? _tokenStartCharIndex : lastBatchedTextStart,
                            _input.index() - 1));
        }
        return super.getText();
    }

    @Override
    public void reset() {
        batchToken = false;
        lastBatchedTextStart = -1;
        batchedTokens = 0;
        batchedCharacters = 0;
        super.reset();
    }

    public long getBatchedTokens() {
        return batchedTokens;
    }

    public long getBatchedCharacters() {
        return batchedCharacters;
    }
}
