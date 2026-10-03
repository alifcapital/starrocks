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
import org.antlr.v4.runtime.Token;

import java.util.Arrays;

import static com.starrocks.sql.parser.FrozenTokenCatalog.OPEN;
import static com.starrocks.sql.parser.FrozenTokenCatalog.PARAMETER;
import static com.starrocks.sql.parser.FrozenTokenCatalog.SEMICOLON;

/** Shared original lexical/list guards; one invocation per attempted query or SHOW. */
final class DirectStatementPreflight {
    private DirectStatementPreflight() {}

    private static final int OPEN = FrozenTokenCatalog.T_2;
    private static final int CLOSE = FrozenTokenCatalog.T_3;
    private static final int COMMA = FrozenTokenCatalog.T_0;

    static void check(CommonTokenStream tokens, int tokenLimit, int expressionLimit) {
        check(tokens, tokenLimit, expressionLimit, false);
    }

    static LexicalParameterContext check(
            CommonTokenStream tokens,
            int tokenLimit,
            int expressionLimit,
            boolean allowParameters) {
        return check(tokens, tokenLimit, expressionLimit, allowParameters, false);
    }

    /**
     * With batch set, several statements are allowed and the caller splits the lexical parameters itself, so
     * the result is null.
     */
    static LexicalParameterContext check(
            CommonTokenStream tokens,
            int tokenLimit,
            int expressionLimit,
            boolean allowParameters,
            boolean batch) {
        java.util.ArrayList<Integer> parameterOffsets = null;
        RawTokenAccess raw = RawTokenAccess.of(tokens);
        if (tokens.size() == 0 || raw.typeAt(tokens.size() - 1) != Token.EOF) {
            throw unsupported(tokens, "input must be fully filled");
        }
        int commas = 0;
        int semicolons = 0;
        int originalRawNonEOFCount = tokens.size() - 1;
        if (tokens instanceof DenseOwnedTokenStream dense) {
            // Private ordinals omit whitespace/comments. The original listener sees every raw
            // token.
            originalRawNonEOFCount = dense.originalRawNonEOFCount();
            commas = dense.commaCount();
            semicolons = dense.semicolonCount();
            // Preserve lexical preflight refusal ordering without materializing default-token
            // views.
            int firstParameterStart = Integer.MAX_VALUE;
            if (!allowParameters && dense.parameterCount() != 0) {
                for (int i = 0; i < tokens.size(); i++) {
                    if (raw.typeAt(i) == PARAMETER) {
                        firstParameterStart = raw.startAt(i);
                        break;
                    }
                }
            }
            for (Token hint : dense.allHints()) {
                if (!allowParameters && hint.getStartIndex() > firstParameterStart) {
                    throw unsupported(tokens, "parameters");
                }
                if (possibleSqlModeHint(hint.getText())) {
                    throw unsupported(tokens, "hint SQL mode preflight requires original builder");
                }
            }
            if (!allowParameters && dense.parameterCount() != 0) {
                throw unsupported(tokens, "parameters");
            }
        } else {
            for (int i = 0; i < tokens.size(); i++) {
                int type = raw.typeAt(i);
                // Do not evaluate HintFactory before syntactic acceptance. Any possible mode key
                // must instead reach the original builder before expression constructors run.
                if (raw.channelAt(i) == 2 && possibleSqlModeHint(raw.textAt(i))) {
                    throw unsupported(tokens, "hint SQL mode preflight requires original builder");
                }
                if (type == PARAMETER && raw.channelAt(i) == Token.DEFAULT_CHANNEL) {
                    if (!allowParameters) {
                        throw unsupported(tokens, "parameters");
                    }
                    if (parameterOffsets == null) {
                        parameterOffsets = new java.util.ArrayList<>();
                    }
                    parameterOffsets.add(raw.startAt(i));
                }
                if (type == COMMA) {
                    commas++;
                }
                if (type == SEMICOLON) {
                    semicolons++;
                }
            }
        }
        if (semicolons > 1 && !batch) {
            throw unsupported(tokens, "multiple/empty statements");
        }
        if (originalRawNonEOFCount >= tokenLimit) {
            throw unsupported(tokens, "original listener limit path");
        }
        // The total comma count is already a safe proof for ordinary queries.
        // Only its conservative rejection path needs a second scoped scan.
        if (commas + 1L > expressionLimit) {
            int maximum = maximumScopedCommas(tokens);
            if (maximum < 0) {
                throw unsupported(tokens, "unbalanced delimiter guard");
            }
            if (maximum + 1L > expressionLimit) {
                throw unsupported(tokens, "original listener limit path");
            }
        }
        if (batch) {
            return null;
        }
        if (tokens instanceof DenseOwnedTokenStream dense) {
            return dense.parameterCount() == 0
                    ? null
                    : LexicalParameterContext.fromOffsets(dense.parameterOffsets());
        }
        return parameterOffsets == null
                ? null
                : LexicalParameterContext.fromOffsets(
                        parameterOffsets.stream().mapToInt(Integer::intValue).toArray());
    }

    private static final int OPEN_BRACKET = FrozenTokenCatalog.T_5;
    private static final int CLOSE_BRACKET = FrozenTokenCatalog.T_6;

    private static int maximumScopedCommas(CommonTokenStream tokens) {
        // Every protected list separator stays in one ()/[] scope. Counting
        // separate lists in the same scope, including CASE, remains conservative.
        // Angle brackets are deliberately not nesting boundaries.
        long[] stack = new long[16];
        int depth = 0;
        int current = 0;
        int maximum = 0;
        RawTokenAccess raw = RawTokenAccess.of(tokens);
        for (int i = 0; i < tokens.size(); i++) {
            if (raw.channelAt(i) != Token.DEFAULT_CHANNEL) {
                continue;
            }
            int type = raw.typeAt(i);
            if (type == COMMA) {
                current++;
                maximum = Math.max(maximum, current);
            } else if (type == SEMICOLON && depth == 0) {
                // A list never spans two statements of a batch.
                current = 0;
            } else if (type == OPEN || type == OPEN_BRACKET) {
                if (depth == stack.length) {
                    stack = Arrays.copyOf(stack, stack.length * 2);
                }
                int expected = type == OPEN ? CLOSE : CLOSE_BRACKET;
                stack[depth++] = ((long) current << 32) | (expected & 0xffffffffL);
                current = 0;
            } else if (type == CLOSE || type == CLOSE_BRACKET) {
                if (depth == 0) {
                    return -1;
                }
                long saved = stack[--depth];
                if ((int) saved != type) {
                    return -1;
                }
                current = (int) (saved >>> 32);
            }
        }
        return depth == 0 ? maximum : -1;
    }

    private static boolean possibleSqlModeHint(String text) {
        String key = "sql_mode";
        int matched = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            // HintFactory discards precisely these characters outside quoted strings.
            // Ignore quotes/quoted whitespace too: conservative false fallback is permitted.
            if (c == ' '
                    || c == '\r'
                    || c == '\n'
                    || c == '\t'
                    || c == '\u3000'
                    || c == '\''
                    || c == '"') {
                continue;
            }
            if (c >= 'A' && c <= 'Z') {
                c = (char) (c + ('a' - 'A'));
            }
            matched = c == key.charAt(matched) ? matched + 1 : c == 's' ? 1 : 0;
            if (matched == key.length()) {
                return true;
            }
        }
        return false;
    }

    private static DirectQueryParser.UnsupportedQuery unsupported(
            CommonTokenStream tokens, String reason) {
        return new DirectQueryParser.UnsupportedQuery(
                reason
                        + " at raw="
                        + tokens.index()
                        + " type="
                        + FrozenTokenNames.lexerDisplayName(tokens.LA(1)));
    }
}
