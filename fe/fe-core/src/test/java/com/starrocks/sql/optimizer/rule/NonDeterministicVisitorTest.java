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

package com.starrocks.sql.optimizer.rule;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NonDeterministicVisitorTest {
    private static final ColumnRefOperator V1 = new ColumnRefOperator(1, IntegerType.BIGINT, "v1", true);
    private static final ColumnRefOperator R = new ColumnRefOperator(2, FloatType.DOUBLE, "r", true);

    private static CallOperator rand() {
        return new CallOperator(FunctionSet.RAND, FloatType.DOUBLE, List.of());
    }

    private static boolean hasNonDeterministicFunction(LogicalWindowOperator window) {
        OptExpression root = OptExpression.create(window, OptExpression.create(new LogicalValuesOperator(List.of(V1))));
        return root.getOp().accept(new NonDeterministicVisitor(), root, null);
    }

    // A project above a window can be merged into the projection of the window. We expect the visitor to find a
    // rand() there, so that ForceCTEReuseRule does not let a CTE with it be inlined.
    @Test
    public void testWindowProjection() {
        Map<ColumnRefOperator, ScalarOperator> columns = new HashMap<>();
        columns.put(V1, V1);
        LogicalWindowOperator deterministic = LogicalWindowOperator.builder()
                .setWindowCall(Map.of()).setProjection(new Projection(new HashMap<>(columns))).build();
        Assertions.assertFalse(hasNonDeterministicFunction(deterministic));

        columns.put(R, rand());
        LogicalWindowOperator window = LogicalWindowOperator.builder()
                .setWindowCall(Map.of()).setProjection(new Projection(columns)).build();
        Assertions.assertTrue(hasNonDeterministicFunction(window));
    }

    @Test
    public void testWindowPredicate() {
        LogicalWindowOperator window = LogicalWindowOperator.builder().setWindowCall(Map.of())
                .setPredicate(BinaryPredicateOperator.lt(rand(), ConstantOperator.createDouble(0.5))).build();
        Assertions.assertTrue(hasNonDeterministicFunction(window));
    }
}
