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
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Pair;

import java.util.AbstractList;

/** Private, filled view: the exclusive expression endpoint is a virtual EOF.
 * Original token ordinals/source metadata are retained; the source cursor and
 * tokens are never changed. Subquery callbacks and interval scans use this view.
 */
final class BoundedExpressionTokenStream extends CommonTokenStream {
    private final CommonTokenStream source;
    private final RawTokenAccess sourceRaw;
    private final int endpoint;
    private final CommonToken eof;
    private final RawTokenAccess boundedRaw;

    BoundedExpressionTokenStream(CommonTokenStream source, int start, int endpoint) {
        super(source.getTokenSource());
        this.source = source;
        this.sourceRaw = RawTokenAccess.of(source);
        this.endpoint = endpoint;
        if (start < 0
                || start >= endpoint
                || endpoint >= source.size()
                || sourceRaw.channelAt(endpoint) != Token.DEFAULT_CHANNEL) {
            throw new IllegalArgumentException("invalid expression bound");
        }
        eof =
                new CommonToken(
                        new Pair<>(source.getTokenSource(), sourceRaw.tokenInputStreamAt(endpoint)),
                        Token.EOF,
                        Token.DEFAULT_CHANNEL,
                        sourceRaw.startAt(endpoint),
                        sourceRaw.startAt(endpoint) - 1);
        eof.setTokenIndex(endpoint);
        eof.setLine(sourceRaw.lineAt(endpoint));
        eof.setCharPositionInLine(sourceRaw.columnAt(endpoint));
        eof.setText("<EOF>");
        boundedRaw = new BoundedRaw();
        tokens =
                new AbstractList<>() {
                    @Override
                    public int size() {
                        return endpoint + 1;
                    }

                    @Override
                    public Token get(int index) {
                        check(index);
                        return index == endpoint ? eof : source.get(index);
                    }
                };
        fetchedEOF = true;
        p = start;
    }

    RawTokenAccess rawAccess() {
        return boundedRaw;
    }

    private void check(int raw) {
        if (raw < 0 || raw > endpoint) {
            throw new IndexOutOfBoundsException("raw=" + raw + " end=" + endpoint);
        }
    }

    @Override
    public void fill() {}

    @Override
    public int size() {
        return endpoint + 1;
    }

    @Override
    protected boolean sync(int index) {
        return index >= 0 && index <= endpoint;
    }

    @Override
    protected int fetch(int count) {
        return 0;
    }

    @Override
    public Token get(int index) {
        return tokens.get(index);
    }

    @Override
    protected int nextTokenOnChannel(int index, int channel) {
        if (index < 0) {
            throw new IndexOutOfBoundsException("negative token index");
        }
        while (index < endpoint && sourceRaw.channelAt(index) != channel) {
            index++;
        }
        return Math.min(index, endpoint);
    }

    @Override
    protected int previousTokenOnChannel(int index, int channel) {
        for (index = Math.min(index, endpoint); index >= 0; index--) {
            if (index == endpoint || sourceRaw.channelAt(index) == channel) {
                return index;
            }
        }
        return -1;
    }

    @Override
    public void seek(int index) {
        p = nextTokenOnChannel(index, Token.DEFAULT_CHANNEL);
    }

    @Override
    public void consume() {
        if (p == endpoint) {
            throw new IllegalStateException("cannot consume EOF");
        }
        seek(p + 1);
    }

    private int look(int k) {
        if (k == 0) {
            return -1;
        }
        int index = p;
        if (k > 0) {
            for (int n = 1; n < k && index < endpoint; n++) {
                index = nextTokenOnChannel(index + 1, Token.DEFAULT_CHANNEL);
            }
        } else {
            if (k == Integer.MIN_VALUE) {
                return -1;
            }
            for (int n = 0; n > k && index >= 0; n--) {
                index = previousTokenOnChannel(index - 1, Token.DEFAULT_CHANNEL);
            }
        }
        return index;
    }

    @Override
    public Token LT(int k) {
        int index = look(k);
        return index < 0 ? null : get(index);
    }

    @Override
    public int LA(int k) {
        int index = look(k);
        if (index < 0) {
            throw new NullPointerException();
        }
        return boundedRaw.typeAt(index);
    }

    private final class BoundedRaw implements RawTokenAccess {
        public boolean denseDefaultTokens() {
            return sourceRaw.denseDefaultTokens();
        }

        public int typeAt(int raw) {
            check(raw);
            return raw == endpoint ? Token.EOF : sourceRaw.typeAt(raw);
        }

        public int channelAt(int raw) {
            check(raw);
            return raw == endpoint ? Token.DEFAULT_CHANNEL : sourceRaw.channelAt(raw);
        }

        public int startAt(int raw) {
            check(raw);
            return raw == endpoint ? eof.getStartIndex() : sourceRaw.startAt(raw);
        }

        public int stopAt(int raw) {
            check(raw);
            return raw == endpoint ? eof.getStopIndex() : sourceRaw.stopAt(raw);
        }

        public int lineAt(int raw) {
            check(raw);
            return raw == endpoint ? eof.getLine() : sourceRaw.lineAt(raw);
        }

        public int columnAt(int raw) {
            check(raw);
            return raw == endpoint ? eof.getCharPositionInLine() : sourceRaw.columnAt(raw);
        }

        public long positionAt(int raw) {
            check(raw);
            return raw == endpoint
                    ? ((long) eof.getLine() << 32) | (eof.getCharPositionInLine() & 0xffffffffL)
                    : sourceRaw.positionAt(raw);
        }

        public String textAt(int raw) {
            check(raw);
            return raw == endpoint ? eof.getText() : sourceRaw.textAt(raw);
        }

        public CharStream tokenInputStreamAt(int raw) {
            check(raw);
            return raw == endpoint ? eof.getInputStream() : sourceRaw.tokenInputStreamAt(raw);
        }

        public String sourceTextAt(int raw, int skipStart, int skipEnd) {
            check(raw);
            if (raw != endpoint) {
                return sourceRaw.sourceTextAt(raw, skipStart, skipEnd);
            }
            String text = eof.getText();
            return skipStart == 0 && skipEnd == 0
                    ? text
                    : text.substring(skipStart, text.length() - skipEnd);
        }

        public String quotedBodyAt(int raw) {
            check(raw);
            return raw == endpoint ? null : sourceRaw.quotedBodyAt(raw);
        }

        public long smallUnsignedIntegerAt(int raw) {
            check(raw);
            return raw == endpoint ? -1 : sourceRaw.smallUnsignedIntegerAt(raw);
        }
    }
}
