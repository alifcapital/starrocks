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

package com.starrocks.sql.optimizer.operator;

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

// A partial aggregate runs as a streaming local phase, and the streaming operators of the BE do not evaluate a
// predicate. So an aggregate is partial only while it has no predicate; with one it is built as a blocking local
// aggregate, which evaluates it.
public class PartialAggregateOperatorTest {
    private static final ColumnRefOperator KEY = new ColumnRefOperator(1, IntegerType.BIGINT, "k", true);
    private static final ScalarOperator KEY_IS_NEGATIVE =
            new BinaryPredicateOperator(BinaryType.LT, KEY, ConstantOperator.createBigint(0));

    private static LogicalAggregationOperator logical(ScalarOperator predicate) {
        return LogicalAggregationOperator.builder()
                .withOperator(new LogicalAggregationOperator(AggType.LOCAL, List.of(KEY), Map.of()))
                .setSplit(false)
                .setPartialAggregate(true)
                .setPredicate(predicate)
                .build();
    }

    private static PhysicalHashAggregateOperator physical(ScalarOperator predicate) {
        PhysicalHashAggregateOperator op = new PhysicalHashAggregateOperator(AggType.LOCAL, List.of(KEY), List.of(KEY),
                Map.of(), false, Operator.DEFAULT_LIMIT, predicate, null);
        op.setPartialAggregate(true);
        return op;
    }

    @Test
    public void testPartialWithoutPredicate() {
        Assertions.assertTrue(logical(null).isPartialAggregate());
        Assertions.assertTrue(physical(null).isPartialAggregate());
        Assertions.assertTrue(physical(null).canUseStreamingPreAgg());
    }

    @Test
    public void testPredicateKeepsTheAggregateBlocking() {
        Assertions.assertFalse(logical(KEY_IS_NEGATIVE).isPartialAggregate());
        Assertions.assertFalse(physical(KEY_IS_NEGATIVE).isPartialAggregate());
        Assertions.assertFalse(physical(KEY_IS_NEGATIVE).canUseStreamingPreAgg());
    }

    @Test
    public void testOtherTypesAreNotPartial() {
        LogicalAggregationOperator global = LogicalAggregationOperator.builder()
                .withOperator(logical(null))
                .setType(AggType.GLOBAL)
                .build();
        Assertions.assertFalse(global.isPartialAggregate());
        LogicalAggregationOperator split = LogicalAggregationOperator.builder()
                .withOperator(logical(null))
                .setSplit(true)
                .build();
        Assertions.assertFalse(split.isPartialAggregate());
    }
}
