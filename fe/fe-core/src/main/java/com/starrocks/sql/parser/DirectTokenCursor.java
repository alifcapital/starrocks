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

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.misc.Interval;

/** Local default-channel cursor; primitive access never asks the stream for a Token wrapper. */
public final class DirectTokenCursor implements TokenStream {
    private final CommonTokenStream original;
    private final RawTokenAccess raw;
    private final int count;
    private final boolean dense;
    private int position;
    private int nextPosition;
    private int currentType;
    private int nextType;

    public DirectTokenCursor(CommonTokenStream original) {
        this.original = original;
        raw = RawTokenAccess.of(original);
        count = original.size();
        dense = raw.denseDefaultTokens();
        if (count == 0 || raw.typeAt(count - 1) != Token.EOF) {
            throw new DirectExpressionParser.UnsupportedExpression(
                    "cursor requires a fully filled token stream");
        }
        int start = original.index();
        if (start < 0) {
            original.LA(1);
            start = original.index();
        }
        rebuild(start);
        if (position != start
                || !dense
                        && raw.channelAt(position) != Token.DEFAULT_CHANNEL
                        && currentType != Token.EOF) {
            throw new DirectExpressionParser.UnsupportedExpression(
                    "cursor requires default token channel");
        }
    }

    public int typeAt(int index) {
        return raw.typeAt(index);
    }

    public int lineAt(int index) {
        return raw.lineAt(index);
    }

    public int columnAt(int index) {
        return raw.columnAt(index);
    }

    public long positionAt(int index) {
        return raw.positionAt(index);
    }

    public String textAt(int index) {
        return raw.textAt(index);
    }

    public String quotedBodyAt(int index) {
        return raw.quotedBodyAt(index);
    }

    public long smallUnsignedIntegerAt(int index) {
        return raw.smallUnsignedIntegerAt(index);
    }

    public String sourceTextAt(int index, int skipStart, int skipEnd) {
        return raw.sourceTextAt(index, skipStart, skipEnd);
    }

    private int forward(int index) {
        if (index >= count) {
            return count - 1;
        }
        if (dense) {
            if (index < 0) {
                raw.typeAt(index);
            }
            return index;
        }
        while (raw.channelAt(index) != Token.DEFAULT_CHANNEL && raw.typeAt(index) != Token.EOF) {
            index++;
        }
        return index;
    }

    private int backward(int index) {
        if (dense) {
            return index;
        }
        while (index >= 0) {
            if (raw.channelAt(index) == Token.DEFAULT_CHANNEL) {
                return index;
            }
            index--;
        }
        return -1;
    }

    private void rebuild(int index) {
        position = forward(index);
        currentType = raw.typeAt(position);
        nextPosition = forward(position + 1);
        nextType = raw.typeAt(nextPosition);
    }

    public void sync() {
        original.seek(position);
    }

    @Override
    public void consume() {
        if (currentType == Token.EOF) {
            throw new IllegalStateException("cannot consume EOF");
        }
        position = nextPosition;
        currentType = nextType;
        nextPosition = forward(position + 1);
        nextType = raw.typeAt(nextPosition);
    }

    private int lookIndex(int k) {
        if (k == 1) {
            return position;
        }
        if (k == 2) {
            return nextPosition;
        }
        if (k == 0) {
            return -1;
        }
        if (dense) {
            long index = position + (k > 0 ? (long) k - 1 : (long) k);
            return index < 0 ? -1 : index >= count ? count - 1 : (int) index;
        }
        if (k < 0) {
            if (k == Integer.MIN_VALUE) {
                return -1;
            }
            int amount = -k;
            if (position - amount < 0) {
                return -1;
            }
            int index = position;
            for (int i = 1; i <= amount && index > 0; i++) {
                index = backward(index - 1);
            }
            return index;
        }
        int index = nextPosition;
        for (int i = 2; i < k; i++) {
            if (raw.typeAt(index) == Token.EOF) {
                break;
            }
            index = forward(index + 1);
        }
        return index;
    }

    @Override
    public int LA(int k) {
        if (k == 1) {
            return currentType;
        }
        if (k == 2) {
            return nextType;
        }
        int index = lookIndex(k);
        if (index < 0) {
            throw new NullPointerException();
        }
        return raw.typeAt(index);
    }

    @Override
    public Token LT(int k) {
        int index = lookIndex(k);
        return index < 0 ? null : original.get(index);
    }

    @Override
    public int index() {
        return position;
    }

    @Override
    public void seek(int index) {
        rebuild(index);
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public Token get(int index) {
        return original.get(index);
    }

    @Override
    public TokenSource getTokenSource() {
        return original.getTokenSource();
    }

    @Override
    public int mark() {
        return original.mark();
    }

    @Override
    public void release(int marker) {
        original.release(marker);
    }

    @Override
    public String getSourceName() {
        return original.getSourceName();
    }

    @Override
    public String getText() {
        return original.getText();
    }

    @Override
    public String getText(Interval interval) {
        return original.getText(interval);
    }

    @Override
    public String getText(RuleContext context) {
        return original.getText(context);
    }

    @Override
    public String getText(Token start, Token stop) {
        return original.getText(start, stop);
    }
}
