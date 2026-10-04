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
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

import java.util.List;

/** Adapters: ordinary/packed reads respect mutation; private owned reads are immutable. */
interface RawTokenAccess {
    default boolean denseDefaultTokens() {
        return false;
    }

    default String quotedBodyAt(int raw) {
        return null;
    }

    default long smallUnsignedIntegerAt(int raw) {
        return -1;
    }

    int typeAt(int raw);

    int channelAt(int raw);

    int startAt(int raw);

    int stopAt(int raw);

    int lineAt(int raw);

    int columnAt(int raw);

    String textAt(int raw);

    CharStream tokenInputStreamAt(int raw);

    String sourceTextAt(int raw, int skipStart, int skipEnd);

    default long positionAt(int raw) {
        return ((long) lineAt(raw) << 32) | (columnAt(raw) & 0xffffffffL);
    }

    static RawTokenAccess of(CommonTokenStream stream) {
        return stream instanceof BoundedExpressionTokenStream bounded
                ? bounded.rawAccess()
                : stream instanceof DenseOwnedTokenStream tape
                        ? new DenseTokens(tape)
                        : new ExistingTokens(stream.getTokens());
    }

    final class DenseTokens implements RawTokenAccess {
        private final DenseOwnedTokenStream tape;

        DenseTokens(DenseOwnedTokenStream tape) {
            this.tape = tape;
        }

        public boolean denseDefaultTokens() {
            return true;
        }

        public String quotedBodyAt(int raw) {
            return tape.quotedBodyAt(raw);
        }

        public long smallUnsignedIntegerAt(int raw) {
            return tape.smallUnsignedIntegerAt(raw);
        }

        public int typeAt(int raw) {
            return tape.typeAt(raw);
        }

        public int channelAt(int raw) {
            return tape.channelAt(raw);
        }

        public int startAt(int raw) {
            return tape.startAt(raw);
        }

        public int stopAt(int raw) {
            return tape.stopAt(raw);
        }

        public int lineAt(int raw) {
            return tape.lineAt(raw);
        }

        public int columnAt(int raw) {
            return tape.columnAt(raw);
        }

        public long positionAt(int raw) {
            return tape.positionAt(raw);
        }

        public String textAt(int raw) {
            return tape.textAt(raw);
        }

        public CharStream tokenInputStreamAt(int raw) {
            return tape.tokenInputStreamAt(raw);
        }

        public String sourceTextAt(int raw, int skipStart, int skipEnd) {
            return tape.sourceTextAt(raw, skipStart, skipEnd);
        }
    }

    final class ExistingTokens implements RawTokenAccess {
        private final List<Token> tokens;

        ExistingTokens(List<Token> tokens) {
            this.tokens = tokens;
        }

        public int typeAt(int raw) {
            return tokens.get(raw).getType();
        }

        public int channelAt(int raw) {
            return tokens.get(raw).getChannel();
        }

        public int startAt(int raw) {
            return tokens.get(raw).getStartIndex();
        }

        public int stopAt(int raw) {
            return tokens.get(raw).getStopIndex();
        }

        public int lineAt(int raw) {
            return tokens.get(raw).getLine();
        }

        public int columnAt(int raw) {
            return tokens.get(raw).getCharPositionInLine();
        }

        public String textAt(int raw) {
            return tokens.get(raw).getText();
        }

        public CharStream tokenInputStreamAt(int raw) {
            return tokens.get(raw).getInputStream();
        }

        public String sourceTextAt(int raw, int skipStart, int skipEnd) {
            CharStream stream = tokenInputStreamAt(raw);
            int start = startAt(raw);
            int end = stopAt(raw);
            if (stream != null && start >= 0 && end >= start) {
                return stream.getText(Interval.of(start + skipStart, end - skipEnd));
            }
            String text = textAt(raw);
            return skipStart == 0 && skipEnd == 0
                    ? text
                    : text.substring(skipStart, text.length() - skipEnd);
        }
    }
}
