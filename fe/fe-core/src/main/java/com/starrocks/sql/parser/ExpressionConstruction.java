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

import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.ast.expression.AnalyticWindowBoundary;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.MatchExpr;

import java.util.ArrayList;
import java.util.List;

// AST construction follows AstBuilder.
// No generated parser, parse tree, SQL rewrite, fallback, or AST reuse is used here.
/** Shared recognizer construction boundary. Ancillary handles are generic; cold backend is not implemented. */
interface ExpressionConstruction<E, Q, T, F, O, W, B, C> {
    interface Errors {
        DirectExpressionParser.UnsupportedExpression unsupported(String why);

        NodePosition constructionPosition(int start);
    }

    record OverParts<E, O, W>(List<E> partitions, List<O> order, W window, List<String> hints) {}

    void bind(Errors errors);

    ExpressionConstruction<E, Q, T, F, O, W, B, C> fork();

    E number(int kind, long small, String s, NodePosition p);

    void validateDistinctArguments(boolean distinct, boolean aggregate, String name, List<E> args);

    E finishFunction(
            String full,
            String name,
            List<String> parts,
            int head,
            boolean identifierHead,
            boolean aggregate,
            boolean star,
            boolean distinct,
            boolean quantified,
            List<String> hints,
            List<E> args,
            List<O> aggregateOrder,
            boolean separator,
            E filterPredicate,
            boolean analytic,
            OverParts<E, O, W> over,
            NodePosition p);

    void validateFilterOrder(String name, List<E> args, List<O> aggregateOrder, boolean separator);

    void validateAnalyticRewrite(
            boolean analytic, boolean aggregate, String name, List<E> args, int head);

    boolean overIgnored(String name, List<E> args);

    boolean isFunctionCall(E value);

    E password(String plain, NodePosition p);

    E negativeGeneralLiteral(E node);

    E arithmetic(ArithmeticExpr.Operator op, E left, E right, NodePosition p);

    E unary(int t, E value, int start);

    E fieldAccess(E value, String field, NodePosition p);

    E odbc(E function, NodePosition functionPos, boolean windowHead);

    E windowCall(String name, List<E> args, NodePosition p, boolean ignore);

    E over(E call, OverParts<E, O, W> over, NodePosition p);

    void validateMapKey(T key);

    T scalarType(String name, int length, int scale);

    int typeParameter(String text);

    T signedType(String name);

    E binaryLiteral(String value, NodePosition p);

    E dateLiteral(String value, boolean date);

    E slot(List<String> parts, NodePosition p, boolean backQuoted);

    T structType(ArrayList<F> fields);

    E timestamp(String name, E e3, E e2, String unit, NodePosition unitPos, NodePosition p);

    E interval(E amount, String unit, NodePosition unitPos, NodePosition p);

    E subquery(Q relation);

    E lambdaMap(List<E> values);

    E lambdaArgument(String name);

    E lambda(List<E> arguments);

    E logicalNot(E value, NodePosition p);

    E logical(boolean and, E left, E right, NodePosition p);

    E rewriteLogical(E value);

    E isNull(E value, boolean negative, NodePosition p);

    E compare(BinaryType operator, E left, E right, NodePosition p);

    E multiIn(List<E> values, E query, boolean negative, NodePosition p);

    E inQuery(E value, E query, boolean negative, NodePosition p);

    E inList(E value, List<E> values, boolean negative, NodePosition p);

    E between(E value, E lower, E upper, boolean negative, NodePosition p);

    E like(boolean like, E left, E right, NodePosition p);

    E parameter(LexicalParameterContext parameters, int offset);

    E exists(E query, NodePosition p);

    E variable(String name, SetType scope, NodePosition p);

    E userVariable(String name, NodePosition p);

    E grouping(List<E> args, NodePosition p);

    E nullLiteral(NodePosition p);

    E bool(boolean value, NodePosition p);

    E string(String value, NodePosition p);

    T arrayType(T child);

    T mapType(T key, T value);

    T anyMapType();

    F structField(String field, T type);

    E array(T type, List<E> values, NodePosition p);

    E map(T type, List<E> values, NodePosition p);

    C when(E condition, E result, NodePosition p);

    E caseExpr(E base, List<C> clauses, E other, NodePosition p);

    E cast(T type, E child, NodePosition p);

    E extract(String field, E argument, NodePosition p);

    E precision(String text);

    E dateTime(String name, List<E> args);

    E information(String name, NodePosition p);

    E concat(E left, E right, NodePosition p);

    E collection(E value, E index);

    E arrow(E value, E key, NodePosition p);

    E match(MatchExpr.MatchOperator operator, E left, E right, NodePosition p);

    E dictionary(List<E> args);

    E specializedFunction(String name, List<E> args, NodePosition p);

    E orderAllLiteral();

    O order(E value, boolean ascending, boolean nullsFirst, NodePosition p, boolean all);

    W window(AnalyticWindow.Type type, B left, B right, NodePosition p);

    W window(AnalyticWindow.Type type, B left, NodePosition p);

    B windowBoundary(AnalyticWindowBoundary.BoundaryType type, E amount);

    /** Whether AstBuilder would build a LargeInPredicate for an IN list of this many constants. */
    boolean largeInWanted(int count);

    /** The grammar rule that AstBuilder reads an IN list of constants with. */
    enum InListKind {
        /** IN (1, 2), the rule integerList. */
        INTEGERS,
        /** IN (-1, 2.5), the rule numberList. */
        NUMBERS,
        /** IN ('a', 'b'), the rule stringList. */
        STRINGS
    }

    /**
     * The LargeInPredicate that AstBuilder builds for an IN list of constants of this kind. rawText is the list
     * with its parentheses as written.
     */
    E largeIn(E value, List<E> values, boolean negative, NodePosition p, InListKind kind, String rawText);
}
