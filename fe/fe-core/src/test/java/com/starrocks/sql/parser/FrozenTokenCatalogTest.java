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

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.Vocabulary;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * UnifiedBatchedLexer reads keywords and operators through tries built from the generated lexer vocabulary.
 * We check that every trie entry lexes to the same token with the generated lexer, and that the operators left
 * out of the trie are exactly the ones the lexer reads with its own code.
 */
class FrozenTokenCatalogTest {
    private static final Vocabulary LEXER = StarRocksLexer.VOCABULARY;
    private static final String KEYWORD_SYMBOLS = "abcdefghijklmnopqrstuvwxyz0123456789_$";

    @Test
    void keywordTrieMatchesLexerLiterals() {
        Map<String, Integer> expected = new TreeMap<>();
        for (int type = 1; type <= LEXER.getMaxTokenType(); type++) {
            String text = literal(type);
            if (text != null && isWord(text)) {
                expected.put(text, type);
            }
        }
        Map<String, Integer> actual = new TreeMap<>();
        collect(FrozenTokenCatalog.KEYWORD_EDGES, FrozenTokenCatalog.KEYWORD_TYPES, 1, "", true, actual);
        assertEquals(names(expected), names(actual));
    }

    // UnifiedBatchedLexer reads these operators with its own code because they can also start a comment,
    // a number or a token that depends on the SQL mode. Every other operator must be in the trie.
    private static final Set<String> OPERATORS_OUTSIDE_TRIE = Set.of("$$", "-", "->", ".", "...", "/", "|", "||");

    @Test
    void operatorTrieMatchesLexer() {
        Map<String, Integer> trie = new TreeMap<>();
        collect(FrozenTokenCatalog.LITERAL_EDGES, FrozenTokenCatalog.LITERAL_TYPES, 1, "", false, trie);
        Map<String, Integer> expected = new TreeMap<>();
        for (String text : trie.keySet()) {
            expected.put(text, lexOne(text));
        }
        assertEquals(names(expected), names(trie));

        Set<String> missing = new TreeSet<>();
        for (int type = 1; type <= LEXER.getMaxTokenType(); type++) {
            String text = literal(type);
            if (text != null && !isWord(text) && !trie.containsKey(text)) {
                missing.add(text);
            }
        }
        assertEquals(new TreeSet<>(OPERATORS_OUTSIDE_TRIE), missing);
    }

    private static int lexOne(String text) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(text)));
        lexer.removeErrorListeners();
        Token token = lexer.nextToken();
        assertEquals(Token.EOF, lexer.nextToken().getType(), "operator lexes as one token: " + text);
        return token.getType();
    }

    private static String literal(int type) {
        String name = LEXER.getLiteralName(type);
        return name == null ? null : name.substring(1, name.length() - 1);
    }

    private static boolean isWord(String text) {
        char first = text.charAt(0);
        return first >= 'A' && first <= 'Z' || first == '_';
    }

    private static void collect(int[][] edges, int[] types, int node, String prefix, boolean keyword,
                                Map<String, Integer> out) {
        if (types[node] != 0) {
            out.put(prefix, types[node]);
        }
        for (int symbol = 0; symbol < edges[node].length; symbol++) {
            int next = edges[node][symbol];
            if (next != 0) {
                char c = keyword ? Character.toUpperCase(KEYWORD_SYMBOLS.charAt(symbol)) : (char) symbol;
                collect(edges, types, next, prefix + c, keyword, out);
            }
        }
    }

    // Token names make a failure readable; the types are compared through them.
    private static Map<String, String> names(Map<String, Integer> types) {
        Map<String, String> names = new TreeMap<>();
        types.forEach((text, type) -> names.put(text, LEXER.getSymbolicName(type) + "=" + type));
        return names;
    }
}
