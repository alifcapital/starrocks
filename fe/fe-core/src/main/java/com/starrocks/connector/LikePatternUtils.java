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

import java.util.Optional;

/**
 * Reads LIKE patterns for the connectors that push a LIKE predicate to an external engine as a prefix filter.
 *
 * We fear that a prefix cut out of a pattern with a wildcard or an escape in the middle differs from what the BE
 * matches: the engine would then skip files that hold rows the BE accepts. So we return a prefix only for the one
 * pattern shape that means "starts with this text", and nothing for any other shape.
 *
 * The escape rules are the ones of the BE (be/src/exprs/like_predicate.cpp): the escape character is always '\',
 * there is no ESCAPE clause, and '\x' stands for the character x, whatever x is.
 */
public final class LikePatternUtils {
    private static final char ESCAPE = '\\';

    private LikePatternUtils() {
    }

    /**
     * Returns the text that every string matching the pattern starts with, when the pattern is literal characters
     * followed by one unescaped '%' as its last character. Returns empty for every other pattern, and the caller
     * then pushes no filter. A trailing '\' escapes nothing and we do not guess what the BE does with it, so it
     * gives empty too.
     */
    public static Optional<String> literalPrefix(String likePattern) {
        if (likePattern == null) {
            return Optional.empty();
        }
        StringBuilder prefix = new StringBuilder();
        int last = likePattern.length() - 1;
        for (int i = 0; i <= last; i++) {
            char c = likePattern.charAt(i);
            if (c == ESCAPE) {
                if (i == last) {
                    return Optional.empty();
                }
                // The escaped character is literal, so '\%', '\_' and '\\' add '%', '_' and '\' to the prefix.
                prefix.append(likePattern.charAt(++i));
            } else if (c == '_') {
                // '_' matches any one character, a prefix with a literal '_' would skip rows such as 'approved'.
                return Optional.empty();
            } else if (c == '%') {
                // The prefix ends at '%' only if '%' is the last character. An empty prefix filters nothing.
                return i == last && prefix.length() > 0 ? Optional.of(prefix.toString()) : Optional.empty();
            } else {
                prefix.append(c);
            }
        }
        return Optional.empty();
    }
}
