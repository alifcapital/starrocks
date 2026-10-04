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

import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// MultiDistinctByCTERewriter splits several distinct aggregates into one aggregate per function and joins the
// results on the grouping keys. We expect a left-deep chain where every join uses the keys of the first
// aggregate in grouping order, and the top projection maps each grouping key of the query to that first
// aggregate. Without grouping keys we expect cross joins.
class MultiDistinctCTEJoinKeysTest {
    @Test
    void joinChainPreservesGroupingOrderAndProjection() {
        for (int keyCount : List.of(0, 1, 3, 5)) {
            ColumnRefFactory factory = new ColumnRefFactory();
            List<ColumnRefOperator> keys = new ArrayList<>();
            for (int i = 0; i < keyCount; i++) {
                keys.add(factory.create("key" + i, IntegerType.INT, true));
            }
            List<ColumnRefOperator> inputs = new ArrayList<>(keys);
            Map<ColumnRefOperator, CallOperator> calls = new LinkedHashMap<>();
            for (int i = 0; i < 4; i++) {
                ColumnRefOperator argument = factory.create("arg" + i, IntegerType.INT, true);
                inputs.add(argument);
                calls.put(factory.create("count" + i, IntegerType.BIGINT, true),
                        new CallOperator("count", IntegerType.BIGINT, List.of(argument), null, i < 3));
            }
            OptExpression input = OptExpression.create(new LogicalAggregationOperator(AggType.GLOBAL, keys, calls),
                    OptExpression.create(new LogicalValuesOperator(inputs)));
            OptExpression result = new MultiDistinctByCTERewriter()
                    .transformImpl(input, OptimizerFactory.mockContext(factory)).get(0);
            LogicalProjectOperator project = (LogicalProjectOperator) result.inputAt(1).getOp();
            OptExpression tree = result.inputAt(1).inputAt(0);
            OptExpression first = tree;
            while (first.getOp() instanceof LogicalJoinOperator) {
                first = first.inputAt(0);
            }
            List<ColumnRefOperator> firstKeys = ((LogicalAggregationOperator) first.getOp()).getGroupingKeys();
            int joins = 0;
            while (tree.getOp() instanceof LogicalJoinOperator) {
                LogicalJoinOperator join = (LogicalJoinOperator) tree.getOp();
                assertEquals(keyCount == 0 ? JoinOperator.CROSS_JOIN : JoinOperator.INNER_JOIN, join.getJoinType());
                if (keyCount > 0) {
                    List<ScalarOperator> predicates = Utils.extractConjuncts(join.getOnPredicate());
                    List<ColumnRefOperator> rightKeys =
                            ((LogicalAggregationOperator) tree.inputAt(1).getOp()).getGroupingKeys();
                    assertEquals(keyCount, predicates.size());
                    for (int i = 0; i < keyCount; i++) {
                        assertEquals(firstKeys.get(i), predicates.get(i).getChild(0));
                        assertEquals(rightKeys.get(i), predicates.get(i).getChild(1));
                        assertEquals(firstKeys.get(i), project.getColumnRefMap().get(keys.get(i)));
                    }
                }
                joins++;
                tree = tree.inputAt(0);
            }
            assertEquals(3, joins);
            assertTrue(project.getColumnRefMap().keySet().containsAll(calls.keySet()));
            assertEquals(keys, ((LogicalAggregationOperator) input.getOp()).getGroupingKeys());
        }
    }
}
