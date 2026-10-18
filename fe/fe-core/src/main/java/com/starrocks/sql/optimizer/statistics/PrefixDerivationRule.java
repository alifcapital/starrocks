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
import java.util.Optional;

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

    private ScalarOperator rewriteLike(LikePredicateOperator like, ScalarOperator canonicalLeft) {
        if (like.isRegexp() || !column.equals(canonicalLeft)) {
            return null;
        }
        Optional<String> prefix = LikePatternEstimator.pattern(like).flatMap(LikePatternEstimator.LikePattern::fixedPrefix);
        if (prefix.isEmpty() || prefix.get().codePointCount(0, prefix.get().length()) > length) {
            return null;
        }
        return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, x, like.getChild(1));
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
