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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class EquationRewriterTest {
    private static CallOperator call(String name, ScalarOperator... args) {
        return new CallOperator(name, IntegerType.INT, Lists.newArrayList(args));
    }

    @Test
    public void testFirstMappingWinsAndOutputMappingIsApplied() {
        ColumnRefOperator input = new ColumnRefOperator(1, IntegerType.INT, "input", true);
        ColumnRefOperator first = new ColumnRefOperator(2, IntegerType.INT, "first", true);
        ColumnRefOperator second = new ColumnRefOperator(3, IntegerType.INT, "second", true);
        ColumnRefOperator output = new ColumnRefOperator(4, IntegerType.INT, "output", true);
        EquationRewriter rewriter = new EquationRewriter();
        Assertions.assertEquals(input, rewriter.replaceExprWithTarget(input));
        rewriter.addMapping(input, first);
        rewriter.addMapping(input, second);
        Assertions.assertEquals(first, rewriter.replaceExprWithTarget(input));

        // Only the first mapping of an expression is used. If its column is not in the output mapping,
        // the expression stays as it is and the second mapping is not tried.
        rewriter.setOutputMapping(Map.of(second, output));
        Assertions.assertEquals(input, rewriter.replaceExprWithTarget(input));
        rewriter.setOutputMapping(Map.of(first, output));
        Assertions.assertEquals(output, rewriter.replaceExprWithTarget(input));
        rewriter.setOutputMapping(null);
        Assertions.assertEquals(first, rewriter.replaceExprWithTarget(input));
    }

    @Test
    public void testUnmatchedCallAndPredicateAreRewrittenThroughTheirChildren() {
        ColumnRefOperator input = new ColumnRefOperator(1, IntegerType.INT, "input", true);
        ColumnRefOperator other = new ColumnRefOperator(2, IntegerType.INT, "other", true);
        ColumnRefOperator mappedInput = new ColumnRefOperator(3, IntegerType.INT, "mapped_input", true);
        EquationRewriter rewriter = new EquationRewriter();
        rewriter.addMapping(input, mappedInput);

        // The call and the predicate have no mapping, only their child has one.
        CallOperator call = call("some_fn", input, ConstantOperator.createInt(1));
        Assertions.assertEquals(call("some_fn", mappedInput, ConstantOperator.createInt(1)),
                rewriter.replaceExprWithTarget(call));
        // The rewrite must build a new expression and must not change the input.
        Assertions.assertEquals(input, call.getChild(0));

        BinaryPredicateOperator predicate = BinaryPredicateOperator.gt(call("some_fn", input), other);
        Assertions.assertEquals(BinaryPredicateOperator.gt(call("some_fn", mappedInput), other),
                rewriter.replaceExprWithTarget(predicate));
        Assertions.assertEquals(call("some_fn", input), predicate.getChild(0));

        CallOperator untouchedCall = call("some_fn", other, ConstantOperator.createInt(1));
        Assertions.assertEquals(call("some_fn", other, ConstantOperator.createInt(1)),
                rewriter.replaceExprWithTarget(untouchedCall));
    }

    @Test
    public void testMappedCallAndPredicateWinOverTheirChildren() {
        ColumnRefOperator input = new ColumnRefOperator(1, IntegerType.INT, "input", true);
        ColumnRefOperator mappedInput = new ColumnRefOperator(3, IntegerType.INT, "mapped_input", true);
        ColumnRefOperator mappedCall = new ColumnRefOperator(4, IntegerType.INT, "mapped_call", true);
        ColumnRefOperator mappedPredicate = new ColumnRefOperator(5, IntegerType.INT, "mapped_predicate", true);
        CallOperator call = call("some_fn", input);
        BinaryPredicateOperator predicate = BinaryPredicateOperator.gt(call("other_fn", input),
                ConstantOperator.createInt(7));
        EquationRewriter rewriter = new EquationRewriter();
        rewriter.addMapping(input, mappedInput);
        rewriter.addMapping(call, mappedCall);
        rewriter.addMapping(predicate, mappedPredicate);

        Assertions.assertEquals(mappedCall, rewriter.replaceExprWithTarget(call));
        Assertions.assertEquals(mappedPredicate, rewriter.replaceExprWithTarget(predicate));
        // The parent of a mapped call is rebuilt around the mapped column.
        Assertions.assertEquals(call("outer_fn", mappedCall),
                rewriter.replaceExprWithTarget(call("outer_fn", call("some_fn", input))));

        // If the mapped column of the call is not in the output mapping, we expect the call
        // to be rewritten through its children instead.
        ColumnRefOperator outputInput = new ColumnRefOperator(6, IntegerType.INT, "output_input", true);
        rewriter.setOutputMapping(Map.of(mappedInput, outputInput));
        Assertions.assertEquals(call("some_fn", outputInput), rewriter.replaceExprWithTarget(call));
        Assertions.assertEquals(BinaryPredicateOperator.gt(call("other_fn", outputInput), ConstantOperator.createInt(7)),
                rewriter.replaceExprWithTarget(predicate));
    }
}
