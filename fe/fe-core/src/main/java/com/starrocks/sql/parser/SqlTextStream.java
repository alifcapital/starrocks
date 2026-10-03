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
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.misc.Interval;

// For BMP text token indices already equal UTF-16 string indices;
// avoid copying the complete SQL through byte, char and code-point buffers.
public final class SqlTextStream implements CharStream {
    private final String text;
    private int position;

    private SqlTextStream(String text) {
        this.text = text;
    }

    public static CharStream create(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (Character.isSurrogate(text.charAt(i))) {
                return CharStreams.fromString(text);
            }
        }
        return new SqlTextStream(text);
    }

    @Override
    public String getText(Interval interval) {
        int start = Math.min(interval.a, text.length());
        int length = Math.min(interval.b - interval.a + 1, text.length() - start);
        return text.substring(start, start + length);
    }

    @Override
    public void consume() {
        if (position >= text.length()) {
            throw new IllegalStateException("cannot consume EOF");
        }
        position++;
    }

    @Override
    public int LA(int offset) {
        if (offset == 0) {
            return 0;
        }
        int index = position + (offset > 0 ? offset - 1 : offset);
        return index < 0 || index >= text.length() ? EOF : text.charAt(index);
    }

    @Override
    public int mark() {
        return -1;
    }

    @Override
    public void release(int marker) {
    }

    @Override
    public int index() {
        return position;
    }

    @Override
    public void seek(int index) {
        position = index;
    }

    @Override
    public int size() {
        return text.length();
    }

    @Override
    public String getSourceName() {
        return UNKNOWN_SOURCE_NAME;
    }

    @Override
    public String toString() {
        return text;
    }
}
