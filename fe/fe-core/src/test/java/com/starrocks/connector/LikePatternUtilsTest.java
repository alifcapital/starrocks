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

package com.starrocks.connector;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The patterns are written as the BE receives them, after the SQL parser has read the string literal. In the Java
 * literals below every backslash of the pattern is doubled.
 */
public class LikePatternUtilsTest {
    private static void assertPrefix(String expected, String pattern) {
        Assertions.assertEquals(Optional.of(expected), LikePatternUtils.literalPrefix(pattern), pattern);
    }

    private static void assertNoPrefix(String pattern) {
        Assertions.assertEquals(Optional.empty(), LikePatternUtils.literalPrefix(pattern), pattern);
    }

    @Test
    public void testLiteralPrefixWithTrailingPercent() {
        assertPrefix("app", "app%");
        assertPrefix("a", "a%");
        assertPrefix("a b", "a b%");
        assertPrefix("a.b*c+d", "a.b*c+d%");
    }

    @Test
    public void testUnderscoreIsAWildcard() {
        assertNoPrefix("app_%");
        assertNoPrefix("_%");
        assertNoPrefix("a_b%");
        assertNoPrefix("app%_");
    }

    @Test
    public void testPercentBeforeTheEnd() {
        assertNoPrefix("a%b%");
        assertNoPrefix("a%%");
        assertNoPrefix("%a%");
        assertNoPrefix("%a");
        assertNoPrefix("a%b");
    }

    @Test
    public void testPercentAloneGivesNoPrefix() {
        assertNoPrefix("%");
        assertNoPrefix("%%");
    }

    @Test
    public void testEscapedCharactersAreLiteral() {
        // a\_b%
        assertPrefix("a_b", "a\\_b%");
        // a\%%
        assertPrefix("a%", "a\\%%");
        // a\\%
        assertPrefix("a\\", "a\\\\%");
        // \_%, \%% and \\%
        assertPrefix("_", "\\_%");
        assertPrefix("%", "\\%%");
        assertPrefix("\\", "\\\\%");
        // a\\\_\%%
        assertPrefix("a\\_%", "a\\\\\\_\\%%");
    }

    @Test
    public void testEscapedCharacterIsTheCharacterItself() {
        // The BE reads \x as x for any x: a\bc% matches the texts that start with abc.
        assertPrefix("abc", "a\\bc%");
        assertPrefix(".", "\\.%");
    }

    @Test
    public void testNoTrailingUnescapedPercent() {
        // a\%
        assertNoPrefix("a\\%");
        // a\\ and a\\\\
        assertNoPrefix("a\\\\");
        assertNoPrefix("a\\\\\\\\");
        assertNoPrefix("abc");
        assertNoPrefix("a_b");
        assertNoPrefix("");
    }

    @Test
    public void testDanglingEscape() {
        // a\
        assertNoPrefix("a\\");
        // \
        assertNoPrefix("\\");
        // a%\
        assertNoPrefix("a%\\");
        // a\\\
        assertNoPrefix("a\\\\\\");
    }

    @Test
    public void testMultibyteCharacters() {
        assertPrefix("привет", "привет%");
        assertPrefix("日本語", "日本語%");
        assertPrefix("😀x", "😀x%");
        assertPrefix("a日", "a\\日%");
        assertPrefix("日_", "日\\_%");
        assertNoPrefix("日本_%");
        assertNoPrefix("日%本%");
        assertNoPrefix("😀%x");
        assertNoPrefix("日本語");
    }

    @Test
    public void testNullPattern() {
        assertNoPrefix(null);
    }

    /**
     * The BE turns a LIKE pattern into a regular expression (LikePredicate::convert_like_pattern) and matches the
     * whole text. This is that conversion. Whenever the function returns a prefix, a text must match the pattern
     * exactly when it starts with the prefix, for every short pattern and text over an alphabet that holds both
     * wildcards, the escape character and a regex special character.
     */
    @Test
    public void testPrefixAgreesWithTheBeMatching() {
        char[] alphabet = {'a', 'b', '.', '%', '_', '\\'};
        List<String> patterns = allStrings(alphabet, 5);
        List<String> texts = allStrings(alphabet, 4);
        int withPrefix = 0;
        for (String pattern : patterns) {
            Optional<String> prefix = LikePatternUtils.literalPrefix(pattern);
            if (prefix.isEmpty()) {
                continue;
            }
            withPrefix++;
            Pattern regex = beLikeToRegex(pattern);
            for (String text : texts) {
                Assertions.assertEquals(text.startsWith(prefix.get()), regex.matcher(text).matches(),
                        "pattern [" + pattern + "] prefix [" + prefix.get() + "] text [" + text + "]");
            }
        }
        Assertions.assertTrue(withPrefix > 100, "the enumeration must reach patterns with a prefix");
    }

    private static Pattern beLikeToRegex(String likePattern) {
        StringBuilder regex = new StringBuilder();
        boolean escaped = false;
        for (char c : likePattern.toCharArray()) {
            if (!escaped && c == '%') {
                regex.append(".*");
            } else if (!escaped && c == '_') {
                regex.append('.');
            } else if (!escaped && c == '\\') {
                escaped = true;
            } else if (".[]{}()\\*+?|^$".indexOf(c) >= 0) {
                regex.append('\\').append(c);
                escaped = false;
            } else {
                regex.append(c);
                escaped = false;
            }
        }
        return Pattern.compile(regex.toString(), Pattern.DOTALL);
    }

    private static List<String> allStrings(char[] alphabet, int maxLength) {
        List<String> all = new ArrayList<>();
        all.add("");
        int from = 0;
        for (int length = 1; length <= maxLength; length++) {
            int to = all.size();
            for (int i = from; i < to; i++) {
                for (char c : alphabet) {
                    all.add(all.get(i) + c);
                }
            }
            from = to;
        }
        return all;
    }
}
