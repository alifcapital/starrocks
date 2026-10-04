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

/**
 * Same case folding as CaseInsensitiveStream. ASCII characters are folded with arithmetic because the
 * lexer reads every character of the query through this method.
 */
public final class AsciiCaseInsensitiveStream extends CaseInsensitiveStream {
    private final CharStream original;

    public AsciiCaseInsensitiveStream(CharStream original) {
        super(original);
        this.original = original;
    }

    @Override
    public int LA(int offset) {
        int character = original.LA(offset);
        if (character == 0 || character == EOF) {
            return character;
        }
        if (character >= 0 && character < 128) {
            return character >= 'a' && character <= 'z' ? character - 32 : character;
        }
        return Character.toUpperCase(character);
    }
}
