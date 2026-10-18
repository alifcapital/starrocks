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

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.OptionalInt;

/**
 * Conditions on a prefix of a string column, written over the virtual column x of the statistic
 * MCV(left(c, K)), whose canonical form is substr(c, 1, K). The characters of c from position s to position e are
 * the characters of x at the same positions when e <= K, so a condition on them is a condition on x. The written
 * form exists only to estimate the condition; the query keeps its own predicate.
 *
 * A prefix from the first position becomes LIKE and comparisons on x itself, which the string histogram of x
 * estimates. Other positions become substr(x, s, n), which the MCV estimate evaluates on the head of x.
 */
final class PrefixDerivationRule {
    private final ColumnRefOperator column;
    private final int length;
    private final ColumnRefOperator x;

    private PrefixDerivationRule(ColumnRefOperator column, int length, ColumnRefOperator x) {
        this.column = column;
        this.length = length;
        this.x = x;
    }

    /** The rule for an expression of the form substr(c, 1, K) with a constant K >= 1; null for any other one. */
    static PrefixDerivationRule of(ScanExpressionStatistics.Entry entry) {
        int[] positions = substrPositions(entry.getExpression());
        if (positions == null || positions[0] != 1 || positions[1] < 1) {
            return null;
        }
        return new PrefixDerivationRule((ColumnRefOperator) entry.getExpression().getChild(0), positions[1],
                entry.getColumn());
    }

    ColumnRefOperator getColumn() {
        return column;
    }

    /**
     * [s, n] when a canonical expression is substr(c, s, n) with constant s >= 1 and n >= 0 inside the first K
     * characters, so it is substr(x, s, n); null otherwise.
     */
    int[] positionsOver(ScalarOperator canonical) {
        int[] positions = substrPositions(canonical);
        if (positions == null || !column.equals(canonical.getChild(0)) || positions[0] < 1 || positions[1] < 0
                || positions[0] + (long) positions[1] - 1 > length) {
            return null;
        }
        return positions;
    }

    int getLength() {
        return length;
    }

    // [s, n] of substr(column, s, n) with constant positions, null for any other expression.
    private static int[] substrPositions(ScalarOperator expression) {
        if (!(expression instanceof CallOperator) || expression.getClass() != CallOperator.class
                || !FunctionSet.SUBSTR.equals(((CallOperator) expression).getFnName().toLowerCase(Locale.ROOT))
                || expression.getChildren().size() != 3 || !expression.getChild(0).isColumnRef()) {
            return null;
        }
        Integer start = intConstant(expression.getChild(1));
        Integer count = intConstant(expression.getChild(2));
        return start == null || count == null ? null : new int[] {start, count};
    }

    private static Integer intConstant(ScalarOperator operator) {
        if (!(operator instanceof ConstantOperator) || ((ConstantOperator) operator).isNull()
                || !operator.getType().isInt()) {
            return null;
        }
        return ((ConstantOperator) operator).getInt();
    }

    /**
     * The condition written over x, or null when the condition cannot be written over x alone. The left side of
     * the condition is canonical.
     */
    ScalarOperator rewrite(ScalarOperator predicate, ScalarOperator canonicalLeft) {
        if (predicate instanceof BinaryPredicateOperator
                && ((BinaryPredicateOperator) predicate).getBinaryType() == BinaryType.EQ_FOR_NULL) {
            // No estimate over x reads <=> on a substring of x, so the condition is estimated as before.
            return null;
        }
        if (predicate instanceof LikePredicateOperator) {
            return rewriteLike((LikePredicateOperator) predicate, canonicalLeft);
        }
        if (column.equals(canonicalLeft)) {
            return rewriteColumn(predicate);
        }
        int[] positions = substrPositions(canonicalLeft);
        if (positions == null || !column.equals(canonicalLeft.getChild(0))) {
            return null;
        }
        int start = positions[0];
        int count = positions[1];
        if (start < 1 || count < 0 || start + (long) count - 1 > length) {
            return null;
        }
        if (start == 1 && count < length) {
            ScalarOperator prefix = rewritePrefix(predicate, count);
            if (prefix != null) {
                return prefix;
            }
        }
        if (start == 1 && count == length) {
            return withLeft(predicate, x);
        }
        return withLeft(predicate, substr(x, start, count));
    }

    /**
     * c LIKE 'p%' and substr(c, 1, n) LIKE 'p%' with n <= K, and the same with _ among the characters of p, as '10_%':
     * literal characters and _ and then one %. We expect the first characters of c to decide such a pattern: c matches
     * when it has at least as many characters as the pattern has before the %, and they match. So the pattern is
     * written over x as it stands when those characters are at most n, and it is false for substr(c, 1, n) when they
     * are more than n, as a string of n characters cannot have more. We fear a pattern with a % among its characters,
     * or a start after the first character: x holds only K characters of c, so it cannot tell what follows them.
     */
    private ScalarOperator rewriteLike(LikePredicateOperator like, ScalarOperator canonicalLeft) {
        OptionalInt width = LikePatternEstimator.pattern(like)
                .map(LikePatternEstimator.LikePattern::widthBeforeTrailingPercent).orElse(OptionalInt.empty());
        if (width.isEmpty()) {
            return null;
        }
        return rewritePrefixLike(width.getAsInt(),
                new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, x, like.getChild(1)), canonicalLeft);
    }

    // The condition that the strings start with the characters before the %, onX, for c or for the first characters of c.
    private ScalarOperator rewritePrefixLike(int characters, ScalarOperator onX, ScalarOperator canonicalLeft) {
        if (column.equals(canonicalLeft)) {
            return characters <= length ? onX : null;
        }
        int[] positions = substrPositions(canonicalLeft);
        if (positions == null || !column.equals(canonicalLeft.getChild(0)) || positions[0] != 1 || positions[1] < 1
                || positions[1] > length) {
            return null;
        }
        return characters <= positions[1] ? onX : ConstantOperator.createBoolean(false);
    }

    /**
     * c compared with string constants. x holds the first K characters of c, and we want a comparison with a constant
     * v of m characters estimated over x where these characters tell the answer.
     * - m < K: the order of c and v is that of their first differing character, or of their lengths when one starts the
     *   other. A string of fewer than K characters is x whole, and a longer one has more than m characters in x, so
     *   c < v, c <= v, c > v, c >= v, c = v, c <> v and c IN (...) are the same conditions on x.
     * - m = K: x cannot tell v from a longer string that starts with v, and we expect the strings of a column with a
     *   prefix statistic to be longer than K. So the rows of x = v are those of every string that starts with v, not those
     *   of c = v, and c = v, c <> v, c IN (...) and c NOT IN (...) are not written. c < v and c >= v are exact. c <= v is
     *   written as x < v and c > v as x >= v: they lose the rows of the strings equal to v, a share we expect to be
     *   negligible when the strings are longer than K. The two sides of a constant, c < v with c >= v and c <= v with
     *   c > v, are complements, so together they keep exactly the rows that are not NULL.
     * - m > K: the first K characters w of v decide unless x = w, whose rows we cannot split. A range inside one
     *   prefix, such as two bounds of a long identifier, should keep the rows of the prefix and not none, so c < v and
     *   c <= v are written as x <= w, and c > v and c >= v as x >= w. Each of them is an upper bound, and the two count
     *   x = w twice. An equality or a list with such a constant is not written: its rows are a part of those of x = w.
     */
    private ScalarOperator rewriteColumn(ScalarOperator predicate) {
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryType type = ((BinaryPredicateOperator) predicate).getBinaryType();
            String value = stringConstant(predicate.getChild(1));
            if (value == null) {
                return null;
            }
            int characters = value.codePointCount(0, value.length());
            switch (type) {
                case EQ:
                case NE:
                    return characters < length ? withLeft(predicate, x) : null;
                case LT:
                case LE:
                case GT:
                case GE:
                    if (characters < length) {
                        return withLeft(predicate, x);
                    }
                    ConstantOperator first = ConstantOperator.createVarchar(
                            value.substring(0, value.offsetByCodePoints(0, length)));
                    boolean below = type == BinaryType.LT || type == BinaryType.LE;
                    // At K characters the strings equal to v are left on the side of the longer ones: c <= v is x < v.
                    BinaryType onX = characters == length ? (below ? BinaryType.LT : BinaryType.GE)
                            : below ? BinaryType.LE : BinaryType.GE;
                    return new BinaryPredicateOperator(onX, x, first);
                default:
                    return null;
            }
        }
        if (predicate instanceof InPredicateOperator && !((InPredicateOperator) predicate).isSubquery()) {
            for (int i = 1; i < predicate.getChildren().size(); i++) {
                String value = stringConstant(predicate.getChild(i));
                if (value == null || value.codePointCount(0, value.length()) >= length) {
                    return null;
                }
            }
            return withLeft(predicate, x);
        }
        return null;
    }

    // The first count characters of c, count < K, compared with string constants.
    private ScalarOperator rewritePrefix(ScalarOperator predicate, int count) {
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryType type = ((BinaryPredicateOperator) predicate).getBinaryType();
            String value = stringConstant(predicate.getChild(1));
            if (value == null) {
                return null;
            }
            int characters = value.codePointCount(0, value.length());
            switch (type) {
                case EQ:
                    return equal(value, count);
                case NE:
                    // A constant longer than the prefix differs from every value of x, and NULL is not counted.
                    return characters > count ? notNull() : not(equal(value, count));
                case LT:
                case GE:
                    // A constant shorter than the prefix compares with the prefix as with the whole x: both differ
                    // from it at the same first position, or start with it, and the prefix equals it only when it is
                    // the whole x.
                    return characters <= count ? new BinaryPredicateOperator(type, x, predicate.getChild(1)) : null;
                case LE:
                    if (characters < count) {
                        return new BinaryPredicateOperator(type, x, predicate.getChild(1));
                    }
                    return characters == count ? Utils.compoundOr(
                            new BinaryPredicateOperator(BinaryType.LT, x, predicate.getChild(1)), like(value)) : null;
                case GT:
                    if (characters < count) {
                        return new BinaryPredicateOperator(type, x, predicate.getChild(1));
                    }
                    return characters == count ? Utils.compoundAnd(
                            new BinaryPredicateOperator(BinaryType.GE, x, predicate.getChild(1)), not(like(value)))
                            : null;
                default:
                    return null;
            }
        }
        if (predicate instanceof InPredicateOperator) {
            InPredicateOperator in = (InPredicateOperator) predicate;
            if (in.isSubquery()) {
                return null;
            }
            List<ScalarOperator> prefixes = new ArrayList<>();
            List<ScalarOperator> shorter = new ArrayList<>();
            List<ScalarOperator> branches = new ArrayList<>();
            for (int i = 1; i < in.getChildren().size(); i++) {
                String value = stringConstant(in.getChild(i));
                if (value == null) {
                    return null;
                }
                int characters = value.codePointCount(0, value.length());
                if (characters == count) {
                    prefixes.add(in.getChild(i));
                } else if (characters < count) {
                    // A string shorter than the prefix length equals the value as a whole.
                    shorter.add(ConstantOperator.createVarchar(value));
                }
                // A constant longer than the prefix equals no value.
            }
            // The shorter constants compare with the whole x in one IN as well.
            if (shorter.size() == 1) {
                branches.add(new BinaryPredicateOperator(BinaryType.EQ, x, shorter.get(0)));
            } else if (!shorter.isEmpty()) {
                List<ScalarOperator> children = new ArrayList<>(shorter.size() + 1);
                children.add(x);
                children.addAll(shorter);
                branches.add(new InPredicateOperator(false, children.toArray(new ScalarOperator[0])));
            }
            // The prefixes of length n go in one pass over x, not an OR of LIKE conditions, whose estimate costs the
            // square of their number; only the constants of other lengths are branches of their own.
            if (!prefixes.isEmpty()) {
                List<ScalarOperator> children = new ArrayList<>(prefixes.size() + 1);
                children.add(substr(x, 1, count));
                children.addAll(prefixes);
                branches.add(0, new InPredicateOperator(false, children.toArray(new ScalarOperator[0])));
            }
            ScalarOperator positive = branches.isEmpty() ? ConstantOperator.createBoolean(false)
                    : Utils.compoundOr(branches);
            if (!in.isNotIn()) {
                return positive;
            }
            // When every constant is longer than the prefix, NOT IN holds on every value of x that is not NULL.
            return positive instanceof ConstantOperator ? notNull() : not(positive);
        }
        return null;
    }

    // substr(c, 1, count) = value over x.
    private ScalarOperator equal(String value, int count) {
        int characters = value.codePointCount(0, value.length());
        if (characters == count) {
            return like(value);
        }
        if (characters < count) {
            // A string shorter than the prefix length equals the value as a whole.
            return new BinaryPredicateOperator(BinaryType.EQ, x, ConstantOperator.createVarchar(value));
        }
        return ConstantOperator.createBoolean(false);
    }

    private ScalarOperator like(String value) {
        return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, x,
                ConstantOperator.createVarchar(escapeLike(value) + "%"));
    }

    private ScalarOperator notNull() {
        return new IsNullPredicateOperator(true, x);
    }

    private static ScalarOperator not(ScalarOperator operator) {
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, operator);
    }

    static String escapeLike(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 4);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\' || c == '%' || c == '_') {
                escaped.append('\\');
            }
            escaped.append(c);
        }
        return escaped.toString();
    }

    private static String stringConstant(ScalarOperator operator) {
        if (!(operator instanceof ConstantOperator) || ((ConstantOperator) operator).isNull()
                || !operator.getType().isStringType()) {
            return null;
        }
        return ((ConstantOperator) operator).getVarchar();
    }

    static ScalarOperator substr(ColumnRefOperator x, int start, int count) {
        Type[] types = {VarcharType.VARCHAR, IntegerType.INT, IntegerType.INT};
        Function function = ExprUtils.getBuiltinFunction(FunctionSet.SUBSTR, types,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(FunctionSet.SUBSTR, x.getType(),
                List.of(x, ConstantOperator.createInt(start), ConstantOperator.createInt(count)), function);
    }

    // The predicate with its left side replaced.
    static ScalarOperator withLeft(ScalarOperator predicate, ScalarOperator left) {
        ScalarOperator copy = predicate.clone();
        copy.setChild(0, left);
        return copy;
    }
}
