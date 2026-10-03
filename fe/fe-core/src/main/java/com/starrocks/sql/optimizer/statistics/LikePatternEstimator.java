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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

/**
 * Estimates column LIKE 'pattern' with the MCV list of the column's histogram: the most common
 * values, or the predicate's expression of them, are matched against the pattern one by one.
 * Rows outside the MCV list use the unknown-predicate estimate: frequent values are not a
 * representative sample of that tail. The
 * matcher is bounded in time whatever the pattern, since it runs during planning on user input.
 */
public class LikePatternEstimator {
    /**
     * A LIKE pattern: '%' matches any text, '_' any one character, '\\' escapes the next character.
     * Characters are code points, as the BE matches them. Matching runs in time proportional to the
     * sum of text and pattern lengths for literal segments. General underscore patterns use a shared work budget.
     */
    public static class LikePattern {
        private static final int ANY_TEXT = -1;
        private static final int ANY_CHAR = -2;
        // Code points of the literal characters, or one of the two markers above.
        private final int[] tokens;
        private final List<Segment> segments;
        private long remainingWork = 1_000_000;

        private record Segment(int[] text, int[] failure) {
            static Segment of(int[] text) {
                int[] failure = new int[text.length];
                for (int i = 1, j = 0; i < text.length; i++) {
                    while (j > 0 && text[i] != text[j]) {
                        j = failure[j - 1];
                    }
                    if (text[i] == text[j]) {
                        j++;
                    }
                    failure[i] = j;
                }
                return new Segment(text, failure);
            }

            int find(int[] chars, int start) {
                for (int i = start, j = 0; i < chars.length; i++) {
                    while (j > 0 && chars[i] != text[j]) {
                        j = failure[j - 1];
                    }
                    if (chars[i] == text[j] && ++j == text.length) {
                        return i + 1;
                    }
                }
                return -1;
            }
        }

        private LikePattern(int[] tokens) {
            this.tokens = tokens;
            segments = Arrays.stream(tokens).anyMatch(t -> t == ANY_CHAR) ? null : new ArrayList<>();
            if (segments != null) {
                for (int start = 0, end = 0; end <= tokens.length; end++) {
                    if (end == tokens.length || tokens[end] == ANY_TEXT) {
                        if (end > start) {
                            segments.add(Segment.of(Arrays.copyOfRange(tokens, start, end)));
                        }
                        start = end + 1;
                    }
                }
            }
        }

        boolean matchesAllStrings() {
            return tokens.length == 1 && tokens[0] == ANY_TEXT;
        }

        Optional<String> fixedPrefix() {
            if (tokens.length == 0 || tokens[tokens.length - 1] != ANY_TEXT) {
                return Optional.empty();
            }
            StringBuilder prefix = new StringBuilder();
            for (int i = 0; i < tokens.length - 1; i++) {
                if (tokens[i] < 0) {
                    return Optional.empty();
                }
                prefix.appendCodePoint(tokens[i]);
            }
            return Optional.of(prefix.toString());
        }

        public boolean matches(String text) {
            return tryMatch(text).orElseThrow(() -> new IllegalStateException("LIKE estimation budget exhausted"));
        }

        public Optional<Boolean> tryMatch(String text) {
            int[] chars = text.codePoints().toArray();
            if (segments != null) {
                return Optional.of(matchSegments(chars));
            }
            int t = 0;
            int p = 0;
            int starToken = -1;
            int starText = 0;
            while (t < chars.length) {
                if (--remainingWork < 0) {
                    return Optional.empty();
                }
                if (p < tokens.length && (tokens[p] == chars[t] || tokens[p] == ANY_CHAR)) {
                    t++;
                    p++;
                } else if (p < tokens.length && tokens[p] == ANY_TEXT) {
                    starToken = p++;
                    starText = t;
                } else if (starToken >= 0) {
                    p = starToken + 1;
                    t = ++starText;
                } else {
                    return Optional.of(false);
                }
            }
            while (p < tokens.length && tokens[p] == ANY_TEXT) {
                p++;
            }
            return Optional.of(p == tokens.length);
        }

        private boolean matchSegments(int[] chars) {
            if (tokens.length == 0) {
                return chars.length == 0;
            }
            int offset = 0;
            for (int i = 0; i < segments.size(); i++) {
                Segment segment = segments.get(i);
                boolean prefix = i == 0 && tokens[0] != ANY_TEXT;
                boolean suffix = i == segments.size() - 1 && tokens[tokens.length - 1] != ANY_TEXT;
                if (prefix || suffix) {
                    int start = prefix ? offset : chars.length - segment.text.length;
                    if (start < offset || start + segment.text.length > chars.length) {
                        return false;
                    }
                    for (int j = 0; j < segment.text.length; j++) {
                        if (chars[start + j] != segment.text[j]) {
                            return false;
                        }
                    }
                    offset = start + segment.text.length;
                    if (suffix && offset != chars.length) {
                        return false;
                    }
                } else {
                    offset = segment.find(chars, offset);
                    if (offset < 0) {
                        return false;
                    }
                }
            }
            return true;
        }

    }

    /**
     * The pattern of a LIKE predicate with a constant pattern; empty for REGEXP and for a pattern
     * that is not a constant.
     */
    public static Optional<LikePattern> pattern(LikePredicateOperator predicate) {
        if (predicate.isRegexp() || predicate.getChildren().size() != 2 || !predicate.getChild(1).isConstantRef()) {
            return Optional.empty();
        }
        ConstantOperator constant = (ConstantOperator) predicate.getChild(1);
        if (constant.isNull() || !constant.getType().isStringType()) {
            return Optional.empty();
        }
        return Optional.of(compile(constant.getVarchar()));
    }

    static LikePattern compile(String like) {
        int[] chars = like.codePoints().toArray();
        int[] tokens = new int[chars.length];
        int n = 0;
        for (int i = 0; i < chars.length; i++) {
            int c = chars[i];
            if (c == '\\' && i + 1 < chars.length) {
                tokens[n++] = chars[++i];
            } else if (c == '%') {
                // Adjacent '%' match the same as one.
                if (n == 0 || tokens[n - 1] != LikePattern.ANY_TEXT) {
                    tokens[n++] = LikePattern.ANY_TEXT;
                }
            } else if (c == '_') {
                tokens[n++] = LikePattern.ANY_CHAR;
            } else {
                tokens[n++] = c;
            }
        }
        return new LikePattern(Arrays.copyOf(tokens, n));
    }

    /**
     * The column the predicate's left side is built on: the column itself, or a cast or a function
     * of it with constant other arguments, which is evaluated on each most common value.
     */
    public static Optional<ColumnRefOperator> column(LikePredicateOperator predicate) {
        return Optional.ofNullable(MultiColumnMcvEstimator.columnOf(predicate.getChild(0)));
    }

    /**
     * The selectivity of the predicate over the rows of the statistics; empty when the column has no
     * histogram with an MCV list or the pattern cannot be matched.
     */
    public static OptionalDouble selectivity(LikePredicateOperator predicate, Statistics statistics) {
        Optional<ColumnRefOperator> column = column(predicate);
        Optional<LikePattern> pattern = pattern(predicate);
        if (column.isEmpty() || pattern.isEmpty()) {
            return OptionalDouble.empty();
        }
        ColumnStatistic columnStatistic = statistics.getColumnStatistic(column.get());
        if (columnStatistic.isUnknown() || columnStatistic.getHistogram() == null) {
            return OptionalDouble.empty();
        }
        Histogram histogram = columnStatistic.getHistogram();
        Map<String, Long> mcv = histogram.getMCV();
        if (mcv.isEmpty()) {
            return OptionalDouble.empty();
        }
        ScalarOperator expr = predicate.getChild(0);
        long matchedRows = 0;
        long mcvRows = 0;
        for (Map.Entry<String, Long> entry : mcv.entrySet()) {
            mcvRows += entry.getValue();
            String text = entry.getKey();
            if (!expr.isColumnRef()) {
                Optional<ConstantOperator> value = MultiColumnMcvEstimator.evaluate(expr, column.get(), text);
                if (value.isEmpty()) {
                    return OptionalDouble.empty();
                }
                if (value.get().isNull()) {
                    // NULL LIKE anything is not true.
                    continue;
                }
                text = MultiColumnMcvEstimator.constantText(value.get());
            }
            Optional<Boolean> match = pattern.get().tryMatch(text);
            if (match.isEmpty()) {
                return OptionalDouble.empty();
            }
            if (match.get()) {
                matchedRows += entry.getValue();
            }
        }
        double totalRows = Math.max(1, histogram.getTotalRows());
        double tailRows = Math.max(0, totalRows - mcvRows);
        // Matching none (or all) of the frequent values says nothing about the unknown tail.
        // The direct-column '%' case is exact; expressions may introduce NULLs in the tail.
        double tailSelectivity = expr.isColumnRef() && pattern.get().matchesAllStrings()
                ? 1.0 : StatisticsEstimateCoefficient.PREDICATE_UNKNOWN_FILTER_COEFFICIENT;
        double share = (matchedRows + tailRows * tailSelectivity) / totalRows;
        double nullsFraction = columnStatistic.getNullsFraction();
        double selectivity = share * (1.0 - nullsFraction);
        if (nullsFraction > 0 && nullRowsMatch(predicate, column.get()).orElse(false)) {
            selectivity += nullsFraction;
        }
        return OptionalDouble.of(Math.min(1.0, Math.max(0.0, selectivity)));
    }

    /**
     * Whether the NULL rows of the column satisfy the predicate: an expression such as coalesce can
     * turn NULL into a value that matches. Empty when the expression cannot be evaluated on NULL.
     */
    public static Optional<Boolean> nullRowsMatch(LikePredicateOperator predicate, ColumnRefOperator column) {
        ScalarOperator expr = predicate.getChild(0);
        if (expr.isColumnRef()) {
            return Optional.of(false);
        }
        Optional<LikePattern> pattern = pattern(predicate);
        Optional<ConstantOperator> value = MultiColumnMcvEstimator.evaluate(expr, column, null);
        if (pattern.isEmpty() || value.isEmpty()) {
            return Optional.empty();
        }
        return value.get().isNull() ? Optional.of(false)
                : pattern.get().tryMatch(MultiColumnMcvEstimator.constantText(value.get()));
    }
}
