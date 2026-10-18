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
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The canonical form of an expression that statistics are collected on. We want one statistic for all spellings of
 * the same expression, and we want to find it from a query predicate, so the statistic side and the query side are
 * both reduced to this form:
 * - left(c, n) and strleft(c, n) become substr(c, 1, n); substring becomes substr;
 * - an integer constant gets the smallest integer type that holds its value, and a cast of a constant is folded;
 * - a cast that keeps every value is removed where its value is compared or computed; where its value is formatted
 *   as text the cast stays, unless both types format a value the same way (integers, or strings);
 * - the arguments of a commutative operation are ordered by their SQL text;
 * - the arguments of a call are cast to its signature; a renamed call is resolved again by its new name. The FE
 *   folds substr only for INT positions, so the canonical substr can be folded on the FE as well as run on the BE;
 * - constant parts are folded.
 * Expressions with equal canonical forms have equal keys, see {@link #keyOf}.
 */
public final class ExpressionStatisticsCanonicalizer {
    private static final Set<String> COMMUTATIVE_FUNCTIONS =
            Set.of(FunctionSet.ADD, FunctionSet.MULTIPLY, FunctionSet.BITAND, FunctionSet.BITOR, FunctionSet.BITXOR);

    private ExpressionStatisticsCanonicalizer() {
    }

    /** How the value of a part of an expression is used by its parent. */
    private enum Use {
        // The value of the whole expression, which the statistics store as text.
        ROOT,
        // Compared or computed: a cast that keeps every value can go.
        VALUE,
        // Formatted as text, or used in a way we do not know: a cast changes the text, for example of 0.1 as a FLOAT
        // and as a DOUBLE, or of 1.5 as a DECIMAL(10, 2) and as a DECIMAL(20, 4).
        TEXT
    }

    public static ScalarOperator canonicalize(ScalarOperator operator) {
        ScalarOperator canonical = foldConstantCasts(rewrite(operator, Use.ROOT));
        // A constant part such as 1 + 2 is folded, as the optimizer folds it in a query predicate.
        return new ScalarOperatorRewriter().rewrite(canonical, ScalarOperatorRewriter.FOLD_CONSTANT_RULES);
    }

    private static ScalarOperator rewrite(ScalarOperator operator, Use use) {
        if (operator instanceof ColumnRefOperator) {
            return operator;
        }
        if (operator instanceof ConstantOperator) {
            return normalizeConstant((ConstantOperator) operator);
        }
        if (operator instanceof CastOperator) {
            ScalarOperator child = rewrite(operator.getChild(0), operator.getType().isStringType() ? Use.TEXT : Use.VALUE);
            if (child instanceof ConstantOperator) {
                Optional<ConstantOperator> folded = ((ConstantOperator) child).castTo(operator.getType());
                if (folded.isPresent()) {
                    return normalizeConstant(folded.get());
                }
            } else if (CastStatisticsRules.preservesValues(child.getType(), operator.getType())
                    && (use == Use.VALUE || sameText(child.getType(), operator.getType()))) {
                return child;
            }
            return new CastOperator(operator.getType(), child, ((CastOperator) operator).isImplicit());
        }
        if (operator instanceof CallOperator && operator.getClass() == CallOperator.class) {
            return rewriteCall((CallOperator) operator);
        }
        // A comparison or a NULL check reads the value; anything else may format it as text.
        Use childUse = operator instanceof BinaryPredicateOperator || operator instanceof InPredicateOperator
                || operator instanceof CompoundPredicateOperator || operator instanceof IsNullPredicateOperator
                ? Use.VALUE : Use.TEXT;
        List<ScalarOperator> children = new ArrayList<>(operator.getChildren().size());
        for (ScalarOperator child : operator.getChildren()) {
            children.add(rewrite(child, childUse));
        }
        if (isCommutative(operator)) {
            children.sort(Comparator.comparing(ExpressionStatisticsSqlPrinter::printOrNull,
                    Comparator.nullsFirst(Comparator.<String>naturalOrder())));
        }
        ScalarOperator copy = operator.clone();
        for (int i = 0; i < children.size(); i++) {
            copy.setChild(i, children.get(i));
        }
        return copy;
    }

    // Both types write a value as the same text: integers of any width, or strings of any length.
    private static boolean sameText(Type source, Type target) {
        boolean integers = (source.isIntegerType() || source.isLargeint()) && (target.isIntegerType() || target.isLargeint());
        return integers || source.isStringType() && target.isStringType();
    }

    // An argument of a function that takes a string formats its value as text.
    private static Use argumentUse(Function function, int position) {
        if (function == null || function.getArgs().length == 0) {
            return Use.TEXT;
        }
        Type[] args = function.getArgs();
        Type type = position < args.length ? args[position] : args[args.length - 1];
        return type.isStringType() || type.isPseudoType() ? Use.TEXT : Use.VALUE;
    }

    private static boolean isCommutative(ScalarOperator operator) {
        if (operator instanceof CompoundPredicateOperator) {
            return !((CompoundPredicateOperator) operator).isNot();
        }
        if (operator instanceof BinaryPredicateOperator) {
            BinaryType type = ((BinaryPredicateOperator) operator).getBinaryType();
            return type == BinaryType.EQ || type == BinaryType.NE || type == BinaryType.EQ_FOR_NULL;
        }
        return false;
    }

    private static ScalarOperator rewriteCall(CallOperator call) {
        String name = call.getFnName().toLowerCase(Locale.ROOT);
        List<ScalarOperator> children = new ArrayList<>(call.getChildren().size() + 1);
        for (int i = 0; i < call.getChildren().size(); i++) {
            children.add(rewrite(call.getChild(i), argumentUse(call.getFunction(), i)));
        }
        List<ScalarOperator> rewrittenArguments = List.copyOf(children);
        if ((name.equals(FunctionSet.LEFT) || name.equals(FunctionSet.STRLEFT)) && children.size() == 2) {
            name = FunctionSet.SUBSTR;
            children.add(1, normalizeConstant(ConstantOperator.createInt(1)));
        } else if (name.equals(FunctionSet.SUBSTRING)) {
            name = FunctionSet.SUBSTR;
        }
        Function function = call.getFunction();
        if (!name.equals(call.getFnName().toLowerCase(Locale.ROOT))) {
            // A renamed call takes the function of its new name. We keep the function the analyzer chose for any
            // other call: it can carry concrete decimal argument types that a lookup by name would not give.
            Type[] argTypes = children.stream().map(ScalarOperator::getType).toArray(Type[]::new);
            Function resolved = ExprUtils.getBuiltinFunction(name, argTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            if (resolved == null) {
                return copyWithChildren(call, rewrittenArguments);
            }
            function = resolved;
        }
        if (COMMUTATIVE_FUNCTIONS.contains(name) && children.size() == 2 && symmetric(function)) {
            children.sort(Comparator.comparing(ExpressionStatisticsSqlPrinter::printOrNull,
                    Comparator.nullsFirst(Comparator.<String>naturalOrder())));
        }
        CallOperator rebuilt = new CallOperator(name, call.getType(), children, function, call.isDistinct());
        if (function == null) {
            return rebuilt;
        }
        // The arguments are cast to the signature, as the analyzer does for a query.
        return new ScalarOperatorRewriter().rewrite(rebuilt, ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE);
    }

    // Swapped arguments keep their types only when both positions of the signature have one type.
    private static boolean symmetric(Function function) {
        return function == null || function.getArgs().length == 2
                && function.getArgs()[0].equals(function.getArgs()[1]);
    }

    private static ScalarOperator copyWithChildren(CallOperator call, List<ScalarOperator> children) {
        ScalarOperator copy = call.clone();
        for (int i = 0; i < children.size(); i++) {
            copy.setChild(i, children.get(i));
        }
        return copy;
    }

    /** An integer constant typed by its value, so 1 is the same constant whatever type the query gave it. */
    private static ConstantOperator normalizeConstant(ConstantOperator constant) {
        Type type = constant.getType();
        if (constant.isNull() || !type.isIntegerType() && !type.isLargeint()) {
            return constant;
        }
        BigInteger value;
        if (type.isLargeint()) {
            value = constant.getLargeInt();
        } else if (type.isTinyint()) {
            value = BigInteger.valueOf(constant.getTinyInt());
        } else if (type.isSmallint()) {
            value = BigInteger.valueOf(constant.getSmallint());
        } else if (type.isInt()) {
            value = BigInteger.valueOf(constant.getInt());
        } else {
            value = BigInteger.valueOf(constant.getBigint());
        }
        if (value.bitLength() < 8) {
            return ConstantOperator.createTinyInt(value.byteValue());
        }
        if (value.bitLength() < 16) {
            return ConstantOperator.createSmallInt(value.shortValue());
        }
        if (value.bitLength() < 32) {
            return ConstantOperator.createInt(value.intValue());
        }
        if (value.bitLength() < 64) {
            return ConstantOperator.createBigint(value.longValue());
        }
        return ConstantOperator.createLargeInt(value);
    }

    // The type cast rule wraps constants in casts to the signature types; the canonical form holds the cast values.
    private static ScalarOperator foldConstantCasts(ScalarOperator operator) {
        if (operator instanceof CastOperator && operator.getChild(0) instanceof ConstantOperator) {
            Optional<ConstantOperator> folded = ((ConstantOperator) operator.getChild(0)).castTo(operator.getType());
            if (folded.isPresent()) {
                return folded.get();
            }
        }
        for (int i = 0; i < operator.getChildren().size(); i++) {
            ScalarOperator child = operator.getChild(i);
            ScalarOperator folded = foldConstantCasts(child);
            if (folded != child) {
                operator.setChild(i, folded);
            }
        }
        return operator;
    }

    /** The structural key of a canonical expression; null when the expression has a part the key does not cover. */
    public static Key keyOf(ScalarOperator canonical) {
        List<Key> children = new ArrayList<>(canonical.getChildren().size());
        for (ScalarOperator child : canonical.getChildren()) {
            Key key = keyOf(child);
            if (key == null) {
                return null;
            }
            children.add(key);
        }
        if (canonical instanceof ColumnRefOperator) {
            return new Key("column", Integer.toString(((ColumnRefOperator) canonical).getId()), List.of());
        }
        if (canonical instanceof ConstantOperator) {
            ConstantOperator constant = (ConstantOperator) canonical;
            String value = constant.isNull() ? "NULL" : constant.getType().isStringType()
                    ? constant.getVarchar() : constant.toString();
            return new Key("constant", typeKey(constant.getType()) + ":" + value, List.of());
        }
        if (canonical instanceof CastOperator) {
            return new Key("cast", typeKey(canonical.getType()), children);
        }
        if (canonical instanceof CaseWhenOperator) {
            CaseWhenOperator caseWhen = (CaseWhenOperator) canonical;
            return new Key("case", caseWhen.hasCase() + ":" + caseWhen.hasElse(), children);
        }
        if (canonical instanceof CallOperator && canonical.getClass() == CallOperator.class) {
            CallOperator call = (CallOperator) canonical;
            String name = call.getFnName().toLowerCase(Locale.ROOT);
            // A user function is a function of its database: myfn of db1 and myfn of db2 differ.
            Function function = call.getFunction();
            if (function != null && function.isUdf() && function.getFunctionName().getDb() != null) {
                name = function.getFunctionName().getDb() + "." + name;
            }
            return new Key("call", name, children);
        }
        if (canonical instanceof BinaryPredicateOperator) {
            return new Key("binary", ((BinaryPredicateOperator) canonical).getBinaryType().name(), children);
        }
        if (canonical instanceof CompoundPredicateOperator) {
            return new Key("compound", ((CompoundPredicateOperator) canonical).getCompoundType().name(), children);
        }
        if (canonical instanceof InPredicateOperator) {
            return new Key("in", Boolean.toString(((InPredicateOperator) canonical).isNotIn()), children);
        }
        if (canonical instanceof IsNullPredicateOperator) {
            return new Key("isnull", Boolean.toString(((IsNullPredicateOperator) canonical).isNotNull()), children);
        }
        if (canonical instanceof LikePredicateOperator) {
            return new Key("like", ((LikePredicateOperator) canonical).getLikeType().name(), children);
        }
        return null;
    }

    // String lengths are not part of the key: varchar(10) and varchar(20) hold the same values.
    private static String typeKey(Type type) {
        if (type.isStringType()) {
            return type.getPrimitiveType().name();
        }
        if (type instanceof ScalarType && type.isDecimalOfAnyVersion()) {
            ScalarType scalar = (ScalarType) type;
            return "DECIMAL(" + scalar.getScalarPrecision() + "," + scalar.getScalarScale() + ")";
        }
        return type.toSql();
    }

    /** Equal keys mean equal canonical expressions over the same columns. */
    public static final class Key {
        private final String kind;
        private final String detail;
        private final List<Key> children;
        private final int hash;

        private Key(String kind, String detail, List<Key> children) {
            this.kind = kind;
            this.detail = detail;
            this.children = List.copyOf(children);
            this.hash = Objects.hash(kind, detail, this.children);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Key)) {
                return false;
            }
            Key that = (Key) other;
            return hash == that.hash && kind.equals(that.kind) && detail.equals(that.detail)
                    && children.equals(that.children);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return kind + "(" + detail + (children.isEmpty() ? "" : ", " + children) + ")";
        }
    }
}
