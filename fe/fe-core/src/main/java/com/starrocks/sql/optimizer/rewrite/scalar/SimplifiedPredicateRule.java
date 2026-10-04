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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LargeInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;
import com.starrocks.sql.optimizer.rewrite.EliminateNegationsRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.sql.spm.SPMFunctions;
import com.starrocks.type.BooleanType;
import com.starrocks.type.CharType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrTokenizer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SimplifiedPredicateRule extends BottomUpScalarOperatorRewriteRule {

    // Applying this rule to a ConstantOperator or a ColumnRefOperator returns it unchanged.
    @Override
    public boolean rewritesLeaves() {
        return false;
    }

    private static final ImmutableMap<String, List<String>> TIME_FNS = ImmutableMap.<String, List<String>>builder()
            .put("years_", ImmutableList.of(FunctionSet.YEARS_ADD, FunctionSet.YEARS_SUB))
            .put("quarters_", ImmutableList.of(FunctionSet.QUARTERS_ADD, FunctionSet.QUARTERS_SUB))
            .put("months_", ImmutableList.of(FunctionSet.MONTHS_ADD, FunctionSet.MONTHS_SUB))
            .put("weeks_", ImmutableList.of(FunctionSet.WEEKS_ADD, FunctionSet.WEEKS_SUB))
            .put("days_", ImmutableList.of(FunctionSet.DAYS_ADD, FunctionSet.DAYS_SUB))
            .put("hours_", ImmutableList.of(FunctionSet.HOURS_ADD, FunctionSet.HOURS_SUB))
            .put("minutes_", ImmutableList.of(FunctionSet.MINUTES_ADD, FunctionSet.MINUTES_SUB))
            .put("seconds_", ImmutableList.of(FunctionSet.SECONDS_ADD, FunctionSet.SECONDS_SUB))
            .put("milliseconds_", ImmutableList.of(FunctionSet.MILLISECONDS_ADD, FunctionSet.MILLISECONDS_SUB))
            .put("microseconds_", ImmutableList.of(FunctionSet.MICROSECONDS_ADD, FunctionSet.MICROSECONDS_SUB))
            .put("date", ImmutableList.of(FunctionSet.DATE_ADD, FunctionSet.DATE_SUB))
            .build();
    // Shifts by years, quarters and months move the day to the end of a shorter month, so
    // months_add(months_add('2024-01-31', 1), 1) is '2024-03-29' while months_add('2024-01-31', 2) is
    // '2024-03-31'. We want a merged shift to give the same result for every date, so we do not merge these.
    private static final Set<String> MONTH_BASED_TIME_FNS = ImmutableSet.of("years_", "quarters_", "months_");
    private static final Set<String> TIME_FN_NAMES = ImmutableSet.<String>builder()
            .add(FunctionSet.YEARS_ADD).add(FunctionSet.YEARS_SUB)
            .add(FunctionSet.QUARTERS_ADD).add(FunctionSet.QUARTERS_SUB)
            .add(FunctionSet.MONTHS_ADD).add(FunctionSet.MONTHS_SUB)
            .add(FunctionSet.WEEKS_ADD).add(FunctionSet.WEEKS_SUB)
            .add(FunctionSet.DAYS_ADD).add(FunctionSet.DAYS_SUB)
            .add(FunctionSet.HOURS_ADD).add(FunctionSet.HOURS_SUB)
            .add(FunctionSet.MINUTES_ADD).add(FunctionSet.MINUTES_SUB)
            .add(FunctionSet.SECONDS_ADD).add(FunctionSet.SECONDS_SUB)
            .add(FunctionSet.MILLISECONDS_ADD).add(FunctionSet.MILLISECONDS_SUB)
            .add(FunctionSet.MICROSECONDS_ADD).add(FunctionSet.MICROSECONDS_SUB)
            .add(FunctionSet.DATE_ADD).add(FunctionSet.DATE_SUB)
            .build();

    private static final EliminateNegationsRewriter ELIMINATE_NEGATIONS_REWRITER = new EliminateNegationsRewriter();

    @Override
    public ScalarOperator visitCaseWhenOperator(CaseWhenOperator operator, ScalarOperatorRewriteContext context) {
        ScalarOperator result = simplifiedCaseWhenConstClause(operator);
        if (!(result instanceof CaseWhenOperator)) {
            return result;
        }
        return simplifiedCaseWhenToIfFunction((CaseWhenOperator) result);

    }

    // Trans one case when to if function, if function fast than case-when in BE
    // example:
    // case xx when 1 then 2 end => if (xx = 1, 2, NULL)
    ScalarOperator simplifiedCaseWhenToIfFunction(CaseWhenOperator operator) {
        if (operator.getWhenClauseSize() != 1) {
            return operator;
        }

        List<ScalarOperator> args = Lists.newArrayList();
        if (operator.hasCase()) {
            args.add(new BinaryPredicateOperator(BinaryType.EQ, operator.getCaseClause(),
                    operator.getWhenClause(0)));
        } else {
            args.add(operator.getWhenClause(0));
        }

        args.add(operator.getThenClause(0));

        if (operator.hasElse()) {
            args.add(operator.getElseClause());
        } else {
            args.add(ConstantOperator.createNull(operator.getType()));
        }

        Type[] argTypes = args.stream().map(ScalarOperator::getType).toArray(Type[]::new);
        Function fn = ExprUtils.getBuiltinFunction(FunctionSet.IF, argTypes, Function.CompareMode.IS_IDENTICAL);

        if (fn == null) {
            return operator;
        }
        if (operator.getChildren().stream().anyMatch(s -> s.getType().isDecimalV3())) {
            Function decimalFn = ScalarFunction.createVectorizedBuiltin(fn.getId(), fn.getFunctionName().getFunction(),
                    Arrays.stream(argTypes).collect(Collectors.toList()), fn.hasVarArgs(), operator.getType());
            decimalFn.setCouldApplyDictOptimize(fn.isCouldApplyDictOptimize());
            return new CallOperator(FunctionSet.IF, operator.getType(), args, decimalFn);
        }

        return new CallOperator(FunctionSet.IF, operator.getType(), args, fn);
    }

    private static boolean allValuesEqualFirst(CaseWhenOperator operator) {
        int whenSize = operator.getWhenClauseSize();
        if (whenSize == 0) {
            return true;
        }
        ScalarOperator first = operator.getThenClause(0);
        for (int i = 1; i < whenSize; i++) {
            if (!operator.getThenClause(i).equals(first)) {
                return false;
            }
        }
        return operator.getElseClause().equals(first);
    }

    ScalarOperator simplifiedCaseWhenConstClause(CaseWhenOperator operator) {
        // 0. if all result is same, direct return
        // ConstantOperator.equals is looser than its hashCode (CHAR and VARCHAR), so we keep the hash set as the
        // judge. We fear hashing every THEN deeply for nothing, so we first reject on the cheap necessary condition
        // that every value equals the first one.
        if (operator.hasElse() && allValuesEqualFirst(operator)) {
            Set<ScalarOperator> result = Sets.newHashSet();
            for (int i = 0; i < operator.getWhenClauseSize(); i++) {
                result.add(operator.getThenClause(i));
            }
            result.add(operator.getElseClause());

            if (result.size() == 1) {
                return operator.getElseClause();
            }
        }

        // 1. check caseClause
        ConstantOperator caseOp;
        if (operator.hasCase()) {
            if (!operator.getCaseClause().isConstantRef()) {
                return operator;
            }

            caseOp = (ConstantOperator) operator.getCaseClause();
        } else {
            caseOp = ConstantOperator.createBoolean(true);
        }

        // 2. return if caseClause is NullLiteral
        if (caseOp.isNull()) {
            if (operator.hasElse()) {
                return operator.getElseClause();
            }

            return ConstantOperator.createNull(operator.getType());
        }

        // 3. caseClause is constant, remove not equals when/Then Clause or return directly when equals
        Set<Integer> removeArgumentsSet = null;
        int whenStart = operator.getWhenStart();
        boolean allWhenClausConstant = true;
        for (int i = 0; i < operator.getWhenClauseSize(); ++i) {
            if (!operator.getWhenClause(i).isConstantRef()) {
                allWhenClausConstant = false;
                break;
            }
        }

        for (int i = 0; i < operator.getWhenClauseSize(); ++i) {
            if (operator.getWhenClause(i).isConstantRef()) {
                ConstantOperator when = (ConstantOperator) operator.getWhenClause(i);

                if (0 == caseOp.compareTo(when)) {
                    // only when all when clause is constant or first when equals, return directly
                    if (allWhenClausConstant || i == 0) {
                        return operator.getThenClause(i);
                    }
                } else {
                    // record argument index that should be removed.
                    if (removeArgumentsSet == null) {
                        removeArgumentsSet = Sets.newHashSet();
                    }
                    removeArgumentsSet.add(2 * i + whenStart);
                    removeArgumentsSet.add(2 * i + whenStart + 1);
                }
            }
        }

        // If the operator has constant WHEN TRUE, the following WHEN/ELSE is meaningless
        // E.g. CASE WHEN random() > 1 THEN 1 WHEN TRUE THEN 2 ELSE 10 END
        // ---> CASE WHEN random() > 1 THEN 1 ELSE 2 END
        for (int i = 0; i < operator.getWhenClauseSize(); ++i) {
            if (operator.getWhenClause(i).isConstantTrue()) {
                if (removeArgumentsSet == null) {
                    removeArgumentsSet = Sets.newHashSet();
                }
                for (int j = i; j < operator.getWhenClauseSize(); j++) {
                    removeArgumentsSet.add(2 * j + whenStart);
                    removeArgumentsSet.add(2 * j + whenStart + 1);
                }
                if (operator.hasElse()) {
                    operator.removeElseClause();
                }
                operator.addElseClause(operator.getThenClause(i));
                break;
            }
        }

        // removeArguments also gives the operator its own argument list, and CaseWhenOperator(Type, other) can share
        // one list between two operators, so we call it even when nothing is removed.
        operator.removeArguments(removeArgumentsSet == null ? Collections.<Integer>emptySet() : removeArgumentsSet);

        // 4. if when isn't constant, return direct
        for (int i = 0; i < operator.getWhenClauseSize(); i++) {
            if (!operator.getWhenClause(i).isConstantRef()) {
                return operator;
            }
        }

        if (operator.hasElse()) {
            return operator.getElseClause();
        }

        return ConstantOperator.createNull(operator.getType());
    }

    //
    // Simplified Compound Predicate
    //
    // example:
    //        Compound(And)
    //        /      \
    //      true      false
    //
    // After rule:
    //         false
    //
    // example:
    //        Binary(or)
    //        /      \
    // A(Column)      true
    //
    // After rule:
    //          true
    //
    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate,
                                                 ScalarOperatorRewriteContext context) {
        switch (predicate.getCompoundType()) {
            case AND: {
                return simplifiedAndOr(predicate, false);
            }
            case OR: {
                return simplifiedAndOr(predicate, true);
            }
            case NOT: {
                ScalarOperator child = predicate.getChild(0);
                ScalarOperator result = child.accept(ELIMINATE_NEGATIONS_REWRITER, null);
                if (null == result) {
                    return predicate;
                }

                return result;
            }
        }

        return predicate;
    }

    // absorbing is the constant that decides the whole predicate: false for AND, true for OR.
    private static ScalarOperator simplifiedAndOr(CompoundPredicateOperator predicate, boolean absorbing) {
        int constantCount = 0;
        ConstantOperator firstConstant = null;
        boolean anyNull = false;
        for (ScalarOperator child : predicate.getChildren()) {
            if (!child.isConstantRef()) {
                continue;
            }
            ConstantOperator d = (ConstantOperator) child;
            if (d.isNull()) {
                anyNull = true;
            } else if (d.getBoolean() == absorbing) {
                // any false for AND, any true for OR
                return ConstantOperator.createBoolean(absorbing);
            }
            if (constantCount == 0) {
                firstConstant = d;
            }
            constantCount++;
        }

        if (constantCount == 1) {
            if (firstConstant.isNull()) {
                // xxx and null, xxx or null
                return predicate;
            }

            // (true and xxx)/(xxx and true), (false or xxx)/(xxx or false)
            ScalarOperator c0 = predicate.getChild(0);
            if (c0.isConstantRef() && ((ConstantOperator) c0).getBoolean() != absorbing) {
                return predicate.getChild(1);
            }

            return predicate.getChild(0);
        } else if (constantCount == 2) {
            if (anyNull) {
                // (true and null) or (null and null), (false or null) or (null or null)
                return ConstantOperator.createNull(BooleanType.BOOLEAN);
            }
            // true and true, false or false
            return ConstantOperator.createBoolean(!absorbing);
        }

        return predicate;
    }

    //
    // example: a in ('xxxx')
    //
    // after: a = 'xxxx'
    //
    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, ScalarOperatorRewriteContext context) {
        if (predicate.isSubquery()) {
            return predicate;
        }
        if (predicate.getChildren().size() != 2) {
            return predicate;
        }

        if (predicate.getChild(1) instanceof SubqueryOperator) {
            return predicate;
        }

        if (SPMFunctions.isSPMFunctions(predicate)) {
            return predicate;
        }

        // like a in ("xxxx");
        if (predicate.isNotIn()) {
            return new BinaryPredicateOperator(BinaryType.NE, predicate.getChildren());
        } else {
            return new BinaryPredicateOperator(BinaryType.EQ, predicate.getChildren());
        }
    }

    @Override
    public ScalarOperator visitLargeInPredicate(LargeInPredicateOperator predicate, ScalarOperatorRewriteContext context) {
        return predicate;
    }

    // Simplify the comparison result of the same column
    // eg a >= a with not nullable transform to true constant;
    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        ScalarOperator left = predicate.getChild(0);
        ScalarOperator right = predicate.getChild(1);
        if (predicate.getBinaryType().equals(BinaryType.EQ_FOR_NULL) && left.isVariable() && left.equals(right)) {
            return ConstantOperator.createBoolean(true);
        }

        if (predicate.getBinaryType().isEqual() && left.isConstantRef() && right.getType().isBoolean()) {
            ConstantOperator constantOperator = (ConstantOperator) left;
            if (constantOperator.isTrue()) {
                return right;
            }
        } else if (predicate.getBinaryType().isEqual() && left.getType().isBoolean() && right.isConstantRef()) {
            ConstantOperator constantOperator = (ConstantOperator) right;
            if (constantOperator.isTrue()) {
                return left;
            }
        }

        return predicate;
    }

    @Override
    public ScalarOperator visitCall(CallOperator call, ScalarOperatorRewriteContext context) {
        if (FunctionSet.IF.equalsIgnoreCase(call.getFnName())) {
            return ifCall(call);
        } else if (FunctionSet.IFNULL.equalsIgnoreCase(call.getFnName())) {
            return ifNull(call);
        } else if (FunctionSet.ARRAY_MAP.equals(call.getFnName())) {
            return arrayMap(call);
        } else if (TIME_FN_NAMES.contains(call.getFnName())) {
            return simplifiedTimeFns(call);
        } else if (FunctionSet.DATE_TRUNC.equalsIgnoreCase(call.getFnName())) {
            return simplifiedDateTrunc(call);
        } else if (FunctionSet.COALESCE.equalsIgnoreCase(call.getFnName())) {
            return simplifiedCoalesce(call);
        } else if (FunctionSet.JSON_QUERY.equalsIgnoreCase(call.getFnName())) {
            return simplifiedJsonQuery(call);
        } else if (FunctionSet.HOUR.equalsIgnoreCase(call.getFnName())) {
            return simplifiedHourFromUnixTime(call);
        }
        return call;
    }

    @Override
    public ScalarOperator visitLikePredicateOperator(LikePredicateOperator predicate,
                                                     ScalarOperatorRewriteContext context) {
        // make sure is like, not regexp
        if (predicate.getLikeType() != LikePredicateOperator.LikeType.LIKE) {
            return predicate;
        }

        ScalarOperator rightOp = predicate.getChild(1);

        // make sure is constant column ref
        if (!rightOp.isConstantRef()) {
            return predicate;
        }

        // make sure is string literal
        if (rightOp.getType() != VarcharType.VARCHAR && rightOp.getType() != CharType.CHAR) {
            return predicate;
        }

        // make sure it didn't contain '%', '_', or '\' (LIKE escape character)
        String likeString =  ((ConstantOperator) predicate.getChild(1)).getVarchar();
        if (likeString.contains("%") || likeString.contains("_") || likeString.contains("\\")) {
            return predicate;
        }

        return new BinaryPredicateOperator(BinaryType.EQ, predicate.getChild(0), rightOp);
    }

    // reduce `date_sub(date_add(x, 1), 2)` -> `date_sub(x, 1)`
    private ScalarOperator simplifiedTimeFns(CallOperator call) {
        if (!call.getChild(1).isConstantRef() || !IntegerType.INT.equals(call.getChild(1).getType())) {
            return call;
        }
        if (!(call.getChild(0) instanceof CallOperator)) {
            return call;
        }

        // Both shifts must use the same unit. We match whole names: "milliseconds_add" contains "seconds_".
        Map.Entry<String, List<String>> unit = null;
        for (Map.Entry<String, List<String>> e : TIME_FNS.entrySet()) {
            if (e.getValue().contains(call.getFnName())) {
                unit = e;
                break;
            }
        }
        if (unit == null) {
            return call;
        }
        List<String> unitFns = unit.getValue();

        CallOperator child = call.getChild(0).cast();
        if (!unitFns.contains(child.getFnName())) {
            return call;
        }
        if (!child.getChild(1).isConstantRef() || !IntegerType.INT.equals(child.getChild(1).getType())) {
            return call;
        }

        ConstantOperator l1 = call.getChild(1).cast();
        ConstantOperator l2 = call.getChild(0).getChild(1).cast();

        if (l1.isNull() || l2.isNull()) {
            return ConstantOperator.createNull(call.getType());
        }
        if (MONTH_BASED_TIME_FNS.contains(unit.getKey())) {
            return call;
        }

        long i1 = call.getFnName().contains("add") ? l1.getInt() : -(long) l1.getInt();
        long i2 = child.getFnName().contains("add") ? l2.getInt() : -(long) l2.getInt();

        // A shift out of the date range gives NULL. When both shifts go the same way, the first one goes out
        // of the range only if the sum does too. With opposite shifts, days_sub(days_add('9999-12-31', 1), 1)
        // is NULL while the date itself is not, so we do not merge them.
        if ((i1 < 0 && i2 > 0) || (i1 > 0 && i2 < 0)) {
            return call;
        }
        long result = i1 + i2;
        if (Math.abs(result) > Integer.MAX_VALUE) {
            return call;
        }
        ConstantOperator interval = ConstantOperator.createInt((int) Math.abs(result));

        if (result != 0) {
            String fnName = result < 0 ? unitFns.get(1) : unitFns.get(0);
            Function newFn = ExprUtils.getBuiltinFunction(fnName, call.getFunction().getArgs(),
                    Function.CompareMode.IS_SUPERTYPE_OF);
            return new CallOperator(fnName, call.getType(), Lists.newArrayList(child.getChild(0), interval), newFn);
        } else {
            return child.getChild(0);
        }
    }

    private ScalarOperator simplifiedDateTrunc(CallOperator call) {
        if (!call.getType().isDate() || !call.getChild(0).isConstantRef()) {
            return call;
        }
        ConstantOperator child = call.getChild(0).cast();
        if ("day".equalsIgnoreCase(child.toString())) {
            return call.getChild(1);
        }
        return call;
    }

    private ScalarOperator simplifiedCoalesce(CallOperator call) {
        return simplifiedCoalesce(call, false);
    }

    public static ScalarOperator simplifiedCoalesce(CallOperator call, boolean asFilter) {
        ScalarOperator first = call.getChild(0);

        // Find first not null arg.
        int i = 1;
        while (i < call.getChildren().size()) {
            ScalarOperator child = call.getChild(i);
            if (!ConstantOperator.NULL.equals(child)) {
                break;
            }
            i++;
        }

        // All args from 1 to end is null
        // coalesce(x, null, null, ...) equals to x.
        if (i >= call.getChildren().size()) {
            return first;
        }

        if (asFilter) {
            // coalesce(x, false)/coalesce(x, null, ..., false) equals to x.
            ScalarOperator notNull = call.getChild(i);
            if (ConstantOperator.FALSE.equals(notNull)) {
                return first;
            }
        }

        return call;
    }

    // Reduce array_map whose lambda functions is trivial
    // e.g. array_map((x,y)->x, arr1,arr2) is reduced to arr1
    private static ScalarOperator arrayMap(CallOperator call) {
        int index = ((LambdaFunctionOperator) call.getChild(0)).canReduce();
        if (index > 0) {
            return call.getChild(index);
        }
        return call;
    }

    private static ScalarOperator ifNull(CallOperator call) {
        if (!call.getChild(0).isConstantRef()) {
            return call;
        }

        return ((ConstantOperator) call.getChild(0)).isNull() ? call.getChild(1) : call.getChild(0);
    }

    private static ScalarOperator ifCall(CallOperator call) {
        if (!call.getChild(0).isConstantRef()) {
            return call;
        }

        return ((ConstantOperator) call.getChild(0)).getBoolean() ? call.getChild(1) : call.getChild(2);
    }

    // fold json_query
    // e.g. json_query(json_query(json_query(k1, '$.a'), '$.b'), '$.c') => json_query(k1, '$.a.b.c')
    private static ScalarOperator simplifiedJsonQuery(CallOperator call) {
        if (!(call.getChild(0) instanceof CallOperator)) {
            return call;
        }
        CallOperator child = call.getChild(0).cast();
        if (!FunctionSet.JSON_QUERY.equalsIgnoreCase(child.getFnName())) {
            return call;
        }

        if (!child.getChild(1).isConstantRef() || !call.getChild(1).isConstantRef()) {
            return call;
        }

        String path1 = ((ConstantOperator) child.getChild(1)).getVarchar();
        String path2 = ((ConstantOperator) call.getChild(1)).getVarchar();

        if (StringUtils.isBlank(path2) || StringUtils.contains(path2, "..") ||
                StringUtils.countMatches(path2, "\"") % 2 != 0) {
            // .. is recursive search in json path, not supported
            // unpaired quota char
            return call;
        }

        StrTokenizer tokenizer = new StrTokenizer(path2, '.', '"');
        String[] result = tokenizer.getTokenArray();
        int skip = result.length >= 1 && "$".equalsIgnoreCase(result[0]) ? 1 : 0;
        path2 = Arrays.stream(result).skip(skip).collect(Collectors.joining("."));
        String mergePath = path1 + (StringUtils.isBlank(path2) ? "" : "." + path2);
        return new CallOperator(child.getFnName(), call.getType(), Lists.newArrayList(child.getChild(0),
                ConstantOperator.createVarchar(mergePath)), child.getFunction());
    }

    private static ScalarOperator lookupChild(ScalarOperator call, Predicate<ScalarOperator> predicate) {
        if (predicate.test(call)) {
            return call;
        }
        for (ScalarOperator child : call.getChildren()) {
            ScalarOperator res = lookupChild(child, predicate);
            if (res != null) {
                return res;
            }
        }
        return null;
    }

    // Simplify hour(from_unixtime(ts)) to hour_from_unixtime(ts)
    // Also simplify hour(to_datetime(ts)) and hour(to_datetime(ts, 0)) to hour_from_unixtime(ts)
    private static ScalarOperator simplifiedHourFromUnixTime(CallOperator call) {
        if (call.getChildren().size() != 1) {
            return call;
        }

        // Case 1: hour(from_unixtime(ts)) -> hour_from_unixtime(ts)
        ScalarOperator fromUnixTime = lookupChild(call,
                x -> x instanceof CallOperator &&
                        ((CallOperator) x).getFnName().equalsIgnoreCase(FunctionSet.FROM_UNIXTIME));
        if (fromUnixTime != null) {
            // Keep original behavior: only succeeds when argument list matches hour_from_unixtime signature
            Type[] argTypes = fromUnixTime.getChildren().stream().map(ScalarOperator::getType).toArray(Type[]::new);
            Function fn =
                    ExprUtils.getBuiltinFunction(FunctionSet.HOUR_FROM_UNIXTIME, argTypes,
                            Function.CompareMode.IS_IDENTICAL);
            if (fn == null) {
                return call;
            }
            return new CallOperator(FunctionSet.HOUR_FROM_UNIXTIME, call.getType(), fromUnixTime.getChildren(), fn);
        }

        // Case 2: hour(to_datetime(ts)) or hour(to_datetime(ts, 0)) -> hour_from_unixtime(ts)
        ScalarOperator toDatetime = lookupChild(call,
                x -> x instanceof CallOperator &&
                        ((CallOperator) x).getFnName().equalsIgnoreCase(FunctionSet.TO_DATETIME));
        if (toDatetime != null) {
            List<ScalarOperator> args = toDatetime.getChildren();
            ScalarOperator tsArg;
            ScalarOperator unixtimeArgForHour;

            if (args.size() == 1) {
                // Implicit seconds
                tsArg = args.get(0);
                unixtimeArgForHour = tsArg;
            } else if (args.size() == 2) {
                tsArg = args.get(0);
                ScalarOperator scaleOp = args.get(1);
                if (!(scaleOp instanceof ConstantOperator)) {
                    return call;
                }
                ConstantOperator scale = (ConstantOperator) scaleOp;
                if (!IntegerType.INT.equals(scale.getType()) || scale.isNull()) {
                    return call;
                }
                int scaleVal = scale.getInt();
                if (scaleVal == 0) {
                    unixtimeArgForHour = tsArg;
                } else if (scaleVal == 3) {
                    // milliseconds -> seconds
                    unixtimeArgForHour = new CallOperator(FunctionSet.DIVIDE, IntegerType.BIGINT,
                            Lists.newArrayList(tsArg, ConstantOperator.createInt(1000)), null);
                } else if (scaleVal == 6) {
                    // microseconds -> seconds
                    unixtimeArgForHour = new CallOperator(FunctionSet.DIVIDE, IntegerType.BIGINT,
                            Lists.newArrayList(tsArg, ConstantOperator.createInt(1_000_000)), null);
                } else {
                    // Unsupported scale
                    return call;
                }
            } else {
                return call;
            }

            // Build hour_from_unixtime(...) with the converted unix time argument
            Type[] argTypes = new Type[] { unixtimeArgForHour.getType() };
            Function fn =
                    ExprUtils.getBuiltinFunction(FunctionSet.HOUR_FROM_UNIXTIME, argTypes, Function.CompareMode.IS_IDENTICAL);

            if (fn == null) {
                return call;
            }

            return new CallOperator(FunctionSet.HOUR_FROM_UNIXTIME, call.getType(),
                    Lists.newArrayList(unixtimeArgForHour), fn);
        }

        return call;
    }
}
