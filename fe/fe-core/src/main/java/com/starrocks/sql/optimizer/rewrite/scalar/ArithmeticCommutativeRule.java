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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.Set;

import static com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil.findArithmeticFunction;

public class ArithmeticCommutativeRule extends BottomUpScalarOperatorRewriteRule {
    // Don't support DIVIDE, because DIVIDE will use double type, Double is not efficient and will lose precision
    private static final Map<String, String> LEFT_COMMUTATIVE_MAP = ImmutableMap.<String, String>builder()
            .put(FunctionSet.ADD, FunctionSet.SUBTRACT)
            .put(FunctionSet.SUBTRACT, FunctionSet.ADD)
            .put(FunctionSet.DIVIDE, FunctionSet.MULTIPLY)
            .build();

    // With the monotonic predicate rewrite on, MonotonicFilterDerivation keeps a date shift predicate and
    // adds a bound on the column. With it off, we move a day shift to the constant here. We never move shifts
    // by months or years: they move the day to the end of a shorter month, so months_add('2024-01-30', 1) =
    // '2024-02-29' while months_sub('2024-02-29', 1) = '2024-01-29'.
    private static final Map<String, String> DAY_SHIFT_COMMUTATIVE_MAP = ImmutableMap.<String, String>builder()
            .put(FunctionSet.DAYS_ADD, FunctionSet.DAYS_SUB)
            .put(FunctionSet.DAYS_SUB, FunctionSet.DAYS_ADD)
            .put(FunctionSet.ADDDATE, FunctionSet.SUBDATE)
            .put(FunctionSet.SUBDATE, FunctionSet.ADDDATE)
            .put(FunctionSet.DATE_ADD, FunctionSet.DATE_SUB)
            .put(FunctionSet.DATE_SUB, FunctionSet.DATE_ADD)
            .build();
    private static final Set<String> DAY_ADD_FUNCTIONS =
            ImmutableSet.of(FunctionSet.DAYS_ADD, FunctionSet.ADDDATE, FunctionSet.DATE_ADD);

    private static final Map<String, String> RIGHT_COMMUTATIVE_MAP = ImmutableMap.<String, String>builder()
            .put(FunctionSet.ADD, FunctionSet.SUBTRACT)
            .put(FunctionSet.SUBTRACT, FunctionSet.SUBTRACT)
            .build();

    private static boolean isNonZeroNumber(ScalarOperator operator) {
        if (!(operator instanceof ConstantOperator) || operator.isConstantNull()) {
            return false;
        }
        Object value = ((ConstantOperator) operator).getValue();
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).signum() != 0;
        }
        if (value instanceof BigInteger) {
            return ((BigInteger) value).signum() != 0;
        }
        return value instanceof Number && ((Number) value).doubleValue() != 0;
    }

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        // Has been normalize, variable must be right
        if (!predicate.getChild(1).isConstant() || !OperatorType.CALL.equals(predicate.getChild(0).getOpType())) {
            return predicate;
        }

        // Only handle expression like `Variable * Constant = Constant`
        CallOperator call = (CallOperator) predicate.getChild(0);
        if (call.getChildren().size() != 2 || null == call.getFunction() ||
                call.getFunction().getReturnType().isDecimalOfAnyVersion()) {
            return predicate;
        }

        ScalarOperator s1 = call.getChild(0);
        ScalarOperator s2 = call.getChild(1);
        ScalarOperator result = predicate.getChild(1);
        String functionName = call.getFunction().getFunctionName().toString();

        if (s1.isVariable() && s2.isConstant()) {
            String fnName = LEFT_COMMUTATIVE_MAP.get(functionName);
            if (fnName == null && DAY_SHIFT_COMMUTATIVE_MAP.containsKey(functionName)
                    && !isMonotonicPredicateRewriteOn()
                    && canMoveDayShift(functionName, s2, predicate.getBinaryType())) {
                fnName = DAY_SHIFT_COMMUTATIVE_MAP.get(functionName);
            }
            if (fnName == null) {
                return predicate;
            }
            if (s2.isConstantNull() && predicate.getBinaryType() == BinaryType.EQ_FOR_NULL) {
                return predicate;
            }
            // x / 0 is NULL, so a comparison with it is never true, while x = c * 0 is true for x = 0. We move a
            // division to the other side only when the divisor is a constant that we can see is not zero.
            if (FunctionSet.DIVIDE.equals(functionName) && !isNonZeroNumber(s2)) {
                return predicate;
            }
            Function fn = findArithmeticFunction(call, fnName);
            CallOperator n = new CallOperator(fnName, fn.getReturnType(), Lists.newArrayList(result, s2), fn);
            // Negative numbers need to be swapped binary children
            if (FunctionSet.MULTIPLY.equalsIgnoreCase(fnName) && predicate.getBinaryType().isRange()) {
                if (s2.isConstantRef()) {
                    return s2.toString().startsWith("-") ?
                            new BinaryPredicateOperator(predicate.getBinaryType(), n, s1) :
                            new BinaryPredicateOperator(predicate.getBinaryType(), s1, n);
                } else {
                    // can't sure is negative number
                    return predicate;
                }
            }
            return new BinaryPredicateOperator(predicate.getBinaryType(), s1, n);
        } else if (s1.isConstant() && s2.isVariable()) {
            if (!RIGHT_COMMUTATIVE_MAP.containsKey(functionName)) {
                return predicate;
            }
            if (s1.isConstantNull() && predicate.getBinaryType() == BinaryType.EQ_FOR_NULL) {
                return predicate;
            }
            String fnName = RIGHT_COMMUTATIVE_MAP.get(functionName);
            Function fn = findArithmeticFunction(call, fnName);

            if (fnName.equalsIgnoreCase(call.getFunction().getFunctionName().toString())) {
                CallOperator n = new CallOperator(fnName, call.getType(), Lists.newArrayList(s1, result), fn);
                return new BinaryPredicateOperator(predicate.getBinaryType(), n, s2);
            } else {
                CallOperator n = new CallOperator(fnName, call.getType(), Lists.newArrayList(result, s1), fn);
                return new BinaryPredicateOperator(predicate.getBinaryType(), s2, n);
            }
        }

        return predicate;
    }

    // Background threads have no connect context, and the monotonic rewrite stays on there
    private static boolean isMonotonicPredicateRewriteOn() {
        ConnectContext context = ConnectContext.get();
        return context == null || context.getSessionVariable().isEnableMonotonicPredicateRewrite();
    }

    // days_add(c, n) is NULL when the result is out of the date range, but `c op days_sub(K, n)` is still
    // true or false for that c. We expect the predicate to filter rows, where NULL and false both drop the
    // row, so we move the shift only when such c gives false. A forward shift overflows for the largest c,
    // and they are above days_sub(K, n), so =, < and <= are false for them. A backward shift overflows for
    // the smallest c, so =, > and >= are false for them. We need the sign of the shift to know the direction.
    private static boolean canMoveDayShift(String functionName, ScalarOperator shift, BinaryType binaryType) {
        if (!shift.isConstantRef() || shift.isConstantNull() || !shift.getType().isIntegerType()) {
            return false;
        }
        long n = ((Number) ((ConstantOperator) shift).getValue()).longValue();
        boolean forward = DAY_ADD_FUNCTIONS.contains(functionName) == (n >= 0);
        switch (binaryType) {
            case EQ:
                return true;
            case LT:
            case LE:
                return forward;
            case GT:
            case GE:
                return !forward;
            default:
                return false;
        }
    }
}
