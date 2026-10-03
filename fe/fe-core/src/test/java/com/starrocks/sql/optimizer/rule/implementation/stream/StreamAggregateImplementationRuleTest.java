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

package com.starrocks.sql.optimizer.rule.implementation.stream;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamAggOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class StreamAggregateImplementationRuleTest {
    @Test
    public void testHavingAndProjectionAreKept() {
        // A HAVING is merged into the aggregate as its predicate, so the stream aggregate must keep it.
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator key = factory.create("k", IntegerType.INT, true);
        ColumnRefOperator count = factory.create("count", IntegerType.BIGINT, true);
        CallOperator call = new CallOperator("count", IntegerType.BIGINT, List.of(key));
        LogicalAggregationOperator logical = new LogicalAggregationOperator(AggType.GLOBAL, List.of(key),
                Map.of(count, call));
        ScalarOperator having = BinaryPredicateOperator.gt(count, ConstantOperator.createBigint(1));
        logical.setPredicate(having);
        Projection projection = new Projection(Map.of(count, count));
        logical.setProjection(projection);
        OptExpression input = OptExpression.create(logical,
                OptExpression.create(new LogicalValuesOperator(List.of(key))));
        PhysicalStreamAggOperator physical = (PhysicalStreamAggOperator) StreamAggregateImplementationRule
                .getInstance().transform(input, OptimizerFactory.mockContext(factory)).get(0).getOp();
        Assertions.assertSame(having, physical.getPredicate());
        Assertions.assertSame(projection, physical.getProjection());
    }
}
