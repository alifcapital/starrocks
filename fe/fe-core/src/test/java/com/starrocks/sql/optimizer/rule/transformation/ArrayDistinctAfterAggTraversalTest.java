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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

// ArrayDistinctAfterAggRule replaces array_distinct(array_agg(x)) by array_agg_distinct(x). The rewrite is
// safe only when every reference to the aggregate is wrapped by array_distinct, so we check which expressions
// are accepted. Unrelated array_distinct calls, lambda arguments and dictionary mappings must not change the
// answer. The rewrite edits the expression tree in place, so we also check that child order and shared nodes
// are kept.
class ArrayDistinctAfterAggTraversalTest {
    private final ArrayType arrayType = new ArrayType(IntegerType.INT);
    private final ColumnRefOperator aggregate = new ColumnRefOperator(1, arrayType, "array_agg", true);
    private final ColumnRefOperator other = new ColumnRefOperator(2, arrayType, "other", true);
    private final ArrayDistinctAfterAggRule rule = new ArrayDistinctAfterAggRule();

    private CallOperator call(String name, ScalarOperator... children) {
        return new CallOperator(name, arrayType, List.of(children));
    }

    private boolean accepts(ScalarOperator expression) throws Exception {
        Method method = ArrayDistinctAfterAggRule.class.getDeclaredMethod("checkScalarOp",
                ColumnRefOperator.class, ScalarOperator.class);
        method.setAccessible(true);
        return (boolean) method.invoke(rule, aggregate, expression);
    }

    private ScalarOperator rewrite(ColumnRefOperator replacement, ScalarOperator expression) throws Exception {
        Method method = ArrayDistinctAfterAggRule.class.getDeclaredMethod("rewriteScalarOp",
                ColumnRefOperator.class, ColumnRefOperator.class, ScalarOperator.class);
        method.setAccessible(true);
        return (ScalarOperator) method.invoke(rule, aggregate, replacement, expression);
    }

    @Test
    void retainsDistinctWrapperEligibilityForAllReferences() throws Exception {
        ScalarOperator distinct = call(FunctionSet.ARRAY_DISTINCT, aggregate);
        ScalarOperator unrelated = call(FunctionSet.ARRAY_DISTINCT,
                call(FunctionSet.ARRAY_DISTINCT, call("reverse", other)));
        assertTrue(accepts(call("concat", distinct, unrelated, distinct)));
        assertFalse(accepts(call("concat", distinct, aggregate)));
        assertFalse(accepts(call(FunctionSet.ARRAY_DISTINCT, distinct)));
        assertFalse(accepts(call(FunctionSet.ARRAY_DISTINCT, call("reverse", aggregate))));
        assertFalse(accepts(call(FunctionSet.ARRAY_DISTINCT, aggregate, other)));
        assertTrue(accepts(unrelated));
        assertTrue(accepts(ConstantOperator.createNull(arrayType)));
    }

    @Test
    void preservesLambdaArgumentAndDictionaryVisibility() throws Exception {
        ColumnRefOperator argument = new ColumnRefOperator(aggregate.getId(), arrayType, "arg", true, true);
        ScalarOperator lambda = new LambdaFunctionOperator(List.of(argument), argument, arrayType);
        assertTrue(accepts(call("concat", call(FunctionSet.ARRAY_DISTINCT, aggregate), lambda)));
        assertFalse(accepts(new LambdaFunctionOperator(List.of(argument),
                call("concat", argument, aggregate), arrayType)));
        assertTrue(accepts(new LambdaFunctionOperator(List.of(argument),
                call("concat", argument, call(FunctionSet.ARRAY_DISTINCT, aggregate)), arrayType)));

        // A dictionary mapping references the aggregate through getColumnRefs, but the check treats it as a leaf.
        ScalarOperator mapping = new DictMappingOperator(other, aggregate, arrayType);
        assertTrue(accepts(mapping));
        assertFalse(accepts(call(FunctionSet.ARRAY_DISTINCT, mapping)));
    }

    @Test
    void rewritingPreservesChildOrderSharedNodesAndUnrelatedExpressions() throws Exception {
        ColumnRefOperator replacement = new ColumnRefOperator(aggregate.getId(), arrayType,
                FunctionSet.ARRAY_AGG_DISTINCT, aggregate.isNullable());
        ScalarOperator shared = call("reverse", call(FunctionSet.ARRAY_DISTINCT, aggregate));
        ScalarOperator unrelated = call(FunctionSet.ARRAY_DISTINCT, call("reverse", other));
        ScalarOperator constant = ConstantOperator.createNull(arrayType);
        ScalarOperator root = call("concat", shared, constant, unrelated, shared,
                call(FunctionSet.ARRAY_DISTINCT, aggregate));

        assertSame(root, rewrite(replacement, root));
        assertSame(shared, root.getChild(0));
        assertSame(constant, root.getChild(1));
        assertSame(unrelated, root.getChild(2));
        assertSame(shared, root.getChild(3));
        assertSame(replacement, root.getChild(4));
        assertSame(replacement, shared.getChild(0));
        assertSame(other, unrelated.getChild(0).getChild(0));

        ScalarOperator lambda = new LambdaFunctionOperator(List.of(),
                call(FunctionSet.ARRAY_DISTINCT, aggregate), arrayType);
        assertSame(lambda, rewrite(replacement, lambda));
        assertSame(replacement, lambda.getChild(0));
    }
}
