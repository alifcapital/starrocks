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
import org.antlr.v4.runtime.CommonTokenFactory;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenFactory;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.misc.Pair;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.Set;

/** Private default-token projection. Generated parsing requires original-source relex. */
final class DenseOwnedTokenStream extends CommonTokenStream {
    private static final int SHIFT = 10;
    private static final int CHUNK_SIZE = 1 << SHIFT;
    private static final int MASK = CHUNK_SIZE - 1;
    private static final int FIELDS = 5;

    private static final class Chunk {
        final int[] records;
        Token[] views;

        Chunk(int capacity) {
            records = new int[capacity * FIELDS];
        }

        int capacity() {
            return records.length / FIELDS;
        }

        Chunk expanded() {
            Chunk larger = new Chunk(CHUNK_SIZE);
            System.arraycopy(records, 0, larger.records, 0, records.length);
            if (views != null) {
                larger.views = new Token[CHUNK_SIZE];
                System.arraycopy(views, 0, larger.views, 0, views.length);
            }
            return larger;
        }
    }

    private final ArrayList<Chunk> chunks = new ArrayList<>();
    // Immutable record references serve reads; Chunk objects remain for lazy Views.
    private int[][] readRecords;
    private Chunk writeChunk;
    private int[] writeRecords;
    private int writeCapacity;
    private final UnifiedBatchedLexer lexer;
    private final CharStream input;
    private final String originalSql;
    private final boolean textOffsetsAreUtf16;
    private final long originalMode;
    private int originalRawNonEOFCount;
    private int commas;
    private int semicolons;
    private int parameters;
    private final ArrayList<Token> allHintStorage = new ArrayList<>();
    private final Map<Integer, List<Token>> hintsAfter = new LinkedHashMap<>();
    private List<Token> allHints = List.of();
    private static final int COMMA = FrozenTokenCatalog.T_0;
    private final int firstCapacity;
    private Pair<TokenSource, CharStream> source;
    private Map<Integer, String> explicitText;
    private int rawCount;
    private int viewCount;
    private boolean filling;
    private Throwable failure;
    private final boolean fastLexer;
    private boolean lexedByFastLexer;

    /** Construct and fill before exposing the lexer or input; no custom callbacks accepted. */
    DenseOwnedTokenStream(String sql, long mode) {
        this(sql, mode, true);
    }

    /** With fastLexer false the tokens always come from the generated lexer, which tests compare against. */
    DenseOwnedTokenStream(String sql, long mode, boolean fastLexer) {
        super(new UnifiedBatchedLexer(SqlTextStream.create(sql)));
        this.fastLexer = fastLexer;
        originalSql = sql;
        originalMode = mode;
        lexer = (UnifiedBatchedLexer) tokenSource;
        lexer.setSqlMode(mode);
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ErrorHandler());
        input = lexer.getInputStream();
        int length = input.size();
        // This private stream always lexes originalSql without rewriting it.
        // Equal lengths prove that no supplementary code point shortened token indices.
        textOffsetsAreUtf16 = length == sql.length();
        firstCapacity = length >= CHUNK_SIZE - 1 ? CHUNK_SIZE : Math.max(16, length + 1);
        tokens = Collections.unmodifiableList(new Views());
        fill();
    }

    boolean lexedByFastLexer() {
        return lexedByFastLexer;
    }

    int materializedCount() {
        return viewCount;
    }

    int originalRawNonEOFCount() {
        return originalRawNonEOFCount;
    }

    int commaCount() {
        return commas;
    }

    int semicolonCount() {
        return semicolons;
    }

    int parameterCount() {
        return parameters;
    }

    private int[] parameterOffsets;

    int[] parameterOffsets() {
        return parameters == 0 ? new int[0] : java.util.Arrays.copyOf(parameterOffsets, parameters);
    }

    List<Token> allHints() {
        return allHints;
    }

    private void checkRaw(int raw) {
        if (raw < 0 || raw >= rawCount) {
            throw new IndexOutOfBoundsException("raw=" + raw + " size=" + rawCount);
        }
    }

    private int field(int raw, int offset) {
        checkRaw(raw);
        return readRecords[raw >>> SHIFT][(raw & MASK) * FIELDS + offset];
    }

    public int typeAt(int raw) {
        return field(raw, 0);
    }

    public int channelAt(int raw) {
        checkRaw(raw);
        return Token.DEFAULT_CHANNEL;
    }

    public int startAt(int raw) {
        return field(raw, 1);
    }

    public int stopAt(int raw) {
        return field(raw, 2);
    }

    public int lineAt(int raw) {
        return field(raw, 3);
    }

    public int columnAt(int raw) {
        return field(raw, 4);
    }

    public long positionAt(int raw) {
        checkRaw(raw);
        int[] fields = readRecords[raw >>> SHIFT];
        int base = (raw & MASK) * FIELDS;
        return ((long) fields[base + 3] << 32) | (fields[base + 4] & 0xffffffffL);
    }

    public CharStream tokenInputStreamAt(int raw) {
        checkRaw(raw);
        return source.b;
    }

    public String textAt(int raw) {
        checkRaw(raw);
        String text = explicitText == null ? null : explicitText.get(raw);
        if (text != null) {
            return text;
        }
        int[] fields = readRecords[raw >>> SHIFT];
        int base = (raw & MASK) * FIELDS;
        int start = fields[base + 1];
        int stop = fields[base + 2];
        return start < input.size() && stop < input.size() ? sourceText(start, stop) : "<EOF>";
    }

    private String sourceText(int start, int stop) {
        // Keep the original CharStream behavior for code-point indices, EOF and invalid ranges.
        if (textOffsetsAreUtf16 && start >= 0 && stop >= start && stop < originalSql.length()) {
            return originalSql.substring(start, stop + 1);
        }
        return input.getText(Interval.of(start, stop));
    }

    /** Original source span intentionally ignores explicit token text. */
    public String sourceTextAt(int raw, int skipStart, int skipEnd) {
        checkRaw(raw);
        int[] fields = readRecords[raw >>> SHIFT];
        int base = (raw & MASK) * FIELDS;
        int start = fields[base + 1];
        int stop = fields[base + 2];
        if (start >= 0 && stop >= start) {
            return sourceText(start + skipStart, stop - skipEnd);
        }
        String text = textAt(raw);
        return skipStart == 0 && skipEnd == 0
                ? text
                : text.substring(skipStart, text.length() - skipEnd);
    }

    /** Optional private fast paths; null/-1 retains the original token-text interpretation. */
    public String quotedBodyAt(int raw) {
        checkRaw(raw);
        if (!textOffsetsAreUtf16 || (explicitText != null && explicitText.get(raw) != null)) {
            return null;
        }
        int[] fields = readRecords[raw >>> SHIFT];
        int base = (raw & MASK) * FIELDS;
        int type = fields[base];
        int start = fields[base + 1];
        int stop = fields[base + 2];
        if (type != StarRocksLexer.SINGLE_QUOTED_TEXT
                && type != StarRocksLexer.DOUBLE_QUOTED_TEXT) {
            return null;
        }
        if (start < 0 || stop <= start || stop >= originalSql.length()) {
            return null;
        }
        char quote = type == StarRocksLexer.SINGLE_QUOTED_TEXT ? '\'' : '"';
        if (originalSql.charAt(start) != quote || originalSql.charAt(stop) != quote) {
            return null;
        }
        return originalSql.substring(start + 1, stop);
    }

    public long smallUnsignedIntegerAt(int raw) {
        checkRaw(raw);
        if (!textOffsetsAreUtf16 || (explicitText != null && explicitText.get(raw) != null)) {
            return -1;
        }
        int[] fields = readRecords[raw >>> SHIFT];
        int base = (raw & MASK) * FIELDS;
        int start = fields[base + 1];
        int stop = fields[base + 2];
        if (fields[base] != StarRocksLexer.INTEGER_VALUE
                || start < 0
                || stop < start
                || stop >= originalSql.length()
                || (long) stop - start + 1 > 18) {
            return -1;
        }
        long value = 0;
        for (int i = start; i <= stop; i++) {
            int digit = originalSql.charAt(i) - '0';
            if (digit < 0 || digit > 9) {
                return -1;
            }
            value = value * 10 + digit;
        }
        return value;
    }

    void appendFields(
            Pair<TokenSource, CharStream> emittedSource,
            int type,
            String text,
            int tokenChannel,
            int start,
            int stop,
            int line,
            int column) {
        if (source == null) {
            source = emittedSource;
        } else if (source != emittedSource) {
            throw new IllegalStateException("Owned lexer changed token source");
        }
        if (emittedSource.a != lexer || emittedSource.b != input) {
            throw new IllegalStateException("Owned lexer changed input");
        }
        int originalIndex = originalRawNonEOFCount;
        if (type != Token.EOF) {
            originalRawNonEOFCount++;
            if (type == COMMA) {
                commas++;
            }
            if (type == StarRocksLexer.SEMICOLON) {
                semicolons++;
            }
            if (type == StarRocksLexer.PARAMETER && tokenChannel == Token.DEFAULT_CHANNEL) {
                if (parameterOffsets == null) {
                    parameterOffsets = new int[8];
                }
                if (parameters == parameterOffsets.length) {
                    parameterOffsets = java.util.Arrays.copyOf(parameterOffsets, parameters * 2);
                }
                parameterOffsets[parameters++] = start;
            }
        }
        if (tokenChannel == 2 && type != Token.EOF) {
            Token hint = new HintView(type, originalIndex, start, stop, line, column, text);
            allHintStorage.add(hint);
            hintsAfter.computeIfAbsent(rawCount - 1, ignored -> new ArrayList<>()).add(hint);
            return;
        }
        if (type != Token.EOF && tokenChannel != Token.DEFAULT_CHANNEL) {
            return;
        }
        if (tokenChannel != Token.DEFAULT_CHANNEL) {
            throw new IllegalStateException("Owned EOF must use default channel");
        }
        appendRecord(type, start, stop, line, column, text);
    }

    private void appendRecord(int type, int start, int stop, int line, int column, String text) {
        int slot = rawCount & MASK;
        if (slot == 0) {
            writeChunk = new Chunk(rawCount == 0 ? firstCapacity : CHUNK_SIZE);
            chunks.add(writeChunk);
            writeRecords = writeChunk.records;
            writeCapacity = writeChunk.capacity();
        } else if (slot >= writeCapacity) {
            writeChunk = writeChunk.expanded();
            chunks.set(rawCount >>> SHIFT, writeChunk);
            writeRecords = writeChunk.records;
            writeCapacity = writeChunk.capacity();
        }
        int base = slot * FIELDS;
        writeRecords[base] = type;
        writeRecords[base + 1] = start;
        writeRecords[base + 2] = stop;
        writeRecords[base + 3] = line;
        writeRecords[base + 4] = column;
        if (text != null) {
            if (explicitText == null) {
                explicitText = new HashMap<>();
            }
            explicitText.put(rawCount, text);
        }
        rawCount++;
    }

    @Override
    public void fill() {
        if (failure instanceof RuntimeException error) {
            throw error;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (fetchedEOF) {
            return;
        }
        if (filling) {
            throw new IllegalStateException("Recursive owned fill");
        }
        TokenFactory<?> originalFactory = lexer.getTokenFactory();
        if (originalFactory != CommonTokenFactory.DEFAULT || lexer.getInputStream() != input) {
            throw new IllegalStateException(
                    "Owned stream requires its original input and DEFAULT factory");
        }
        SinkFactory factory = new SinkFactory();
        filling = true;
        try {
            if (fastLexer) {
                source = new Pair<>(lexer, input);
                if (FastSqlLexer.lex(originalSql, originalMode, new LexerSink())) {
                    lexedByFastLexer = true;
                    publish();
                    return;
                }
                resetTokens();
            }
            lexer.setTokenFactory(factory);
            if (!lexer.ownedFill(originalSql, this, NON_EOF_MARKER, EOF_MARKER)) {
                for (; ; ) {
                    Token token = lexer.nextToken();
                    if (token == EOF_MARKER) {
                        break;
                    }
                    if (token != NON_EOF_MARKER) {
                        throw new IllegalStateException("Unexpected manual token emission");
                    }
                }
            }
            publish();
        } catch (RuntimeException | Error error) {
            failure = error;
            throw error;
        } finally {
            lexer.setTokenFactory(originalFactory);
            filling = false;
        }
    }

    private void publish() {
        allHints = Collections.unmodifiableList(allHintStorage);
        hintsAfter.replaceAll((ordinal, list) -> Collections.unmodifiableList(list));
        int[][] frozenRecords = new int[chunks.size()][];
        for (int i = 0; i < frozenRecords.length; i++) {
            frozenRecords[i] = chunks.get(i).records;
        }
        readRecords = frozenRecords; // Publish the complete table before any EOF View read.
        fetchedEOF = true;
        p = nextTokenOnChannel(0, channel);
        lexer.emit(tokens.get(rawCount - 1)); // Stable immutable EOF view, never the reusable carrier.
    }

    // FastSqlLexer gave up after some tokens; the generated lexer starts again from an empty stream.
    private void resetTokens() {
        chunks.clear();
        writeChunk = null;
        writeRecords = null;
        writeCapacity = 0;
        rawCount = 0;
        originalRawNonEOFCount = 0;
        commas = 0;
        semicolons = 0;
        parameters = 0;
        parameterOffsets = null;
        allHintStorage.clear();
        hintsAfter.clear();
        explicitText = null;
        source = null;
    }

    /** Counts tokens exactly as appendFields() does for the tokens of the generated lexer. */
    private final class LexerSink implements FastSqlLexer.Sink {
        @Override
        public void token(int type, int start, int stop, int line, int column) {
            if (type != Token.EOF) {
                originalRawNonEOFCount++;
                if (type == COMMA) {
                    commas++;
                } else if (type == StarRocksLexer.SEMICOLON) {
                    semicolons++;
                } else if (type == StarRocksLexer.PARAMETER) {
                    if (parameterOffsets == null) {
                        parameterOffsets = new int[8];
                    }
                    if (parameters == parameterOffsets.length) {
                        parameterOffsets = java.util.Arrays.copyOf(parameterOffsets, parameters * 2);
                    }
                    parameterOffsets[parameters++] = start;
                }
            }
            appendRecord(type, start, stop, line, column, null);
        }

        @Override
        public void hidden() {
            originalRawNonEOFCount++;
        }

        @Override
        public void hint(int type, int start, int stop, int line, int column) {
            Token hint = new HintView(type, originalRawNonEOFCount, start, stop, line, column, null);
            originalRawNonEOFCount++;
            allHintStorage.add(hint);
            hintsAfter.computeIfAbsent(rawCount - 1, ignored -> new ArrayList<>()).add(hint);
        }
    }

    @Override
    protected void setup() {
        fill();
    }

    @Override
    protected boolean sync(int index) {
        fill();
        return index < rawCount;
    }

    @Override
    protected int fetch(int count) {
        int before = rawCount;
        fill();
        return rawCount - before;
    }

    @Override
    public void setTokenSource(TokenSource ignored) {
        throw new UnsupportedOperationException("Owned token source is immutable");
    }

    @Override
    protected int nextTokenOnChannel(int index, int wantedChannel) {
        if (wantedChannel != Token.DEFAULT_CHANNEL) {
            throw new UnsupportedOperationException("Dense default channel only");
        }
        if (index >= rawCount) {
            return rawCount - 1;
        }
        checkRaw(index);
        return index;
    }

    @Override
    protected int previousTokenOnChannel(int index, int wantedChannel) {
        if (wantedChannel != Token.DEFAULT_CHANNEL) {
            throw new UnsupportedOperationException("Dense default channel only");
        }
        return index >= rawCount ? rawCount - 1 : index;
    }

    private int lookIndex(int offset) {
        if (offset == 0) {
            return -1;
        }
        long index = p + (offset > 0 ? (long) offset - 1 : (long) offset);
        return index < 0 ? -1 : index >= rawCount ? rawCount - 1 : (int) index;
    }

    @Override
    public int LA(int offset) {
        int index = lookIndex(offset);
        if (index < 0) {
            throw new NullPointerException();
        }
        return typeAt(index);
    }

    @Override
    public Token LT(int offset) {
        int index = lookIndex(offset);
        return index < 0 ? null : tokens.get(index);
    }

    @Override
    public void consume() {
        if (p >= rawCount - 1) {
            throw new IllegalStateException("cannot consume EOF");
        }
        p++;
    }

    @Override
    public int getNumberOfOnChannelTokens() {
        return rawCount;
    }

    private static List<Token> readOnly(List<Token> list) {
        return list == null ? null : Collections.unmodifiableList(list);
    }

    @Override
    public List<Token> get(int start, int stop) {
        return readOnly(super.get(start, stop));
    }

    @Override
    public List<Token> getTokens(int start, int stop) {
        return readOnly(super.getTokens(start, stop));
    }

    @Override
    public List<Token> getTokens(int start, int stop, int type) {
        return readOnly(super.getTokens(start, stop, type));
    }

    @Override
    public List<Token> getTokens(int start, int stop, Set<Integer> types) {
        return readOnly(super.getTokens(start, stop, types));
    }

    @Override
    public List<Token> getHiddenTokensToRight(int ordinal, int hintChannel) {
        checkRaw(ordinal);
        if (hintChannel != 2) {
            throw new UnsupportedOperationException("Dense tape retains channel2 hints only");
        }
        return hintsAfter.get(ordinal);
    }

    @Override
    public List<Token> getHiddenTokensToLeft(int ordinal, int hintChannel) {
        checkRaw(ordinal);
        if (hintChannel != 2) {
            throw new UnsupportedOperationException("Dense tape retains channel2 hints only");
        }
        return hintsAfter.get(ordinal - 1);
    }

    @Override
    public List<Token> getHiddenTokensToRight(int ordinal) {
        throw new UnsupportedOperationException("No full raw hidden-token stream");
    }

    @Override
    public List<Token> getHiddenTokensToLeft(int ordinal) {
        throw new UnsupportedOperationException("No full raw hidden-token stream");
    }

    @Override
    public String getText() {
        throw new UnsupportedOperationException("Use token text or original source spans");
    }

    @Override
    public String getText(Interval interval) {
        throw new UnsupportedOperationException("No full raw token intervals");
    }

    @Override
    public String getText(RuleContext context) {
        throw new UnsupportedOperationException("No generated tree on dense tape");
    }

    @Override
    public String getText(Token start, Token stop) {
        throw new UnsupportedOperationException("Use original source spans");
    }

    /** Whole-query fallback must relex the original source, not parse projected tokens. */
    CommonTokenStream relexOrdinary() {
        UnifiedBatchedLexer ordinaryLexer =
                new UnifiedBatchedLexer(SqlTextStream.create(originalSql));
        ordinaryLexer.setSqlMode(originalMode);
        ordinaryLexer.removeErrorListeners();
        ordinaryLexer.addErrorListener(new ErrorHandler());
        CommonTokenStream ordinary = new CommonTokenStream(ordinaryLexer);
        ordinary.fill();
        return ordinary;
    }

    private final class HintView implements Token {
        private final int type;
        private final int index;
        private final int start;
        private final int stop;
        private final int line;
        private final int column;
        private final String override;

        HintView(int type, int index, int start, int stop, int line, int column, String override) {
            this.type = type;
            this.index = index;
            this.start = start;
            this.stop = stop;
            this.line = line;
            this.column = column;
            this.override = override;
        }

        public String getText() {
            return override != null ? override : sourceText(start, stop);
        }

        public int getType() {
            return type;
        }

        public int getLine() {
            return line;
        }

        public int getCharPositionInLine() {
            return column;
        }

        public int getChannel() {
            return 2;
        }

        public int getTokenIndex() {
            return index;
        }

        public int getStartIndex() {
            return start;
        }

        public int getStopIndex() {
            return stop;
        }

        public TokenSource getTokenSource() {
            return source.a;
        }

        public CharStream getInputStream() {
            return source.b;
        }
    }

    private final class Views extends AbstractList<Token> implements RandomAccess {
        @Override
        public int size() {
            return rawCount;
        }

        @Override
        public Token get(int raw) {
            checkRaw(raw);
            Chunk chunk = chunks.get(raw >>> SHIFT);
            int slot = raw & MASK;
            if (chunk.views == null) {
                chunk.views = new Token[chunk.capacity()];
            }
            if (chunk.views[slot] == null) {
                chunk.views[slot] = new View(raw);
                viewCount++;
            }
            return chunk.views[slot];
        }
    }

    private final class View implements Token {
        private final int raw;

        View(int raw) {
            this.raw = raw;
        }

        public String getText() {
            return textAt(raw);
        }

        public int getType() {
            return typeAt(raw);
        }

        public int getLine() {
            return lineAt(raw);
        }

        public int getCharPositionInLine() {
            return columnAt(raw);
        }

        public int getChannel() {
            return channelAt(raw);
        }

        public int getTokenIndex() {
            return raw;
        }

        public int getStartIndex() {
            return startAt(raw);
        }

        public int getStopIndex() {
            return stopAt(raw);
        }

        public TokenSource getTokenSource() {
            return source.a;
        }

        public CharStream getInputStream() {
            return source.b;
        }
    }

    // These have no per-emission state. The owned lexer only assigns and returns
    // their references; fill checks identity. Any unexpected metadata observation
    // fails immediately instead of exposing fabricated Token fields.
    private static final Token NON_EOF_MARKER = new MarkerToken();
    private static final Token EOF_MARKER = new MarkerToken();

    private static final class MarkerToken implements Token {
        private UnsupportedOperationException observed() {
            return new UnsupportedOperationException(
                    "Private sink marker has no observable token metadata");
        }

        public String getText() {
            throw observed();
        }

        public int getType() {
            throw observed();
        }

        public int getLine() {
            throw observed();
        }

        public int getCharPositionInLine() {
            throw observed();
        }

        public int getChannel() {
            throw observed();
        }

        public int getTokenIndex() {
            throw observed();
        }

        public int getStartIndex() {
            throw observed();
        }

        public int getStopIndex() {
            throw observed();
        }

        public TokenSource getTokenSource() {
            throw observed();
        }

        public CharStream getInputStream() {
            throw observed();
        }
    }

    private final class SinkFactory implements TokenFactory<Token> {
        public Token create(
                Pair<TokenSource, CharStream> source,
                int type,
                String text,
                int channel,
                int start,
                int stop,
                int line,
                int column) {
            appendFields(source, type, text, channel, start, stop, line, column);
            return type == Token.EOF ? EOF_MARKER : NON_EOF_MARKER;
        }

        public Token create(int type, String text) {
            throw new UnsupportedOperationException(
                    "Owned lexer requires source-associated emission");
        }
    }
}
