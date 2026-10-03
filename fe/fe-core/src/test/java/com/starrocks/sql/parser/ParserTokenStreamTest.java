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

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

class ParserTokenStreamTest {
    static List<String> errors(StarRocksLexer lexer) {
        List<String> errors = new ArrayList<>();
        lexer.removeErrorListeners();
        lexer.addErrorListener(new BaseErrorListener() {
            @Override public void syntaxError(Recognizer<?, ?> r, Object token, int line, int column,
                    String message, RecognitionException exception) {
                errors.add(line + ":" + column + ":" + message);
            }
        });
        return errors;
    }
    static void check(String sql, long mode) {
        StarRocksLexer before = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        StarRocksLexer after = new StarRocksLexer(new CaseInsensitiveStream(SqlTextStream.create(sql)));
        before.setSqlMode(mode);
        after.setSqlMode(mode);
        List<String> aErrors = errors(before);
        List<String> bErrors = errors(after);
        after.setInterpreter(new UnicodeLexerATNSimulator(after, after.getATN(),
                StarRocksLexer._decisionToDFA, StarRocksLexer._sharedContextCache));
        while (true) {
            Token a = before.nextToken();
            Token b = after.nextToken();
            if (a.getType() != b.getType() || a.getChannel() != b.getChannel() ||
                    a.getLine() != b.getLine() || a.getCharPositionInLine() != b.getCharPositionInLine() ||
                    a.getStartIndex() != b.getStartIndex() || a.getStopIndex() != b.getStopIndex() ||
                    !Objects.equals(a.getText(), b.getText())) {
                throw new AssertionError("Token mismatch: " + a + " versus " + b);
            }
            if (a.getType() == Token.EOF) {
                break;
            }
        }
        if (!aErrors.equals(bErrors)) {
            throw new AssertionError("Lexer error mismatch");
        }
    }
    @Test
    void unicodeTransitionsPreserveTokensAndPositions() {
        int[] codepoints = {127, 128, 129, 0x17f, 0x130, 0x131, 0x2fff, 0x3000, 0x3001, 0xd7ff, 0xd800, 0xdfff,
                0xe000, 0xfffe, 0xffff, 0x10000, 0x1f600, 0x10ffff};
        for (int cp : codepoints) {
            String value = new String(Character.toChars(cp));
            for (String sql : List.of("select " + value, "select a" + value + "b", "select 'x" + value + "''y'",
                    "select `x" + value + "``y`", "select \"x" + value + "\"", "select/*" + value + "*/1",
                    "select -- " + value + "\n1", "select /*+ " + value + " */ 1", "select" + value + "1 || 2")) {
                for (long mode : new long[] {0, 2, 32, 34}) {
                    check(sql, mode);
                }
            }
        }

    }
}
