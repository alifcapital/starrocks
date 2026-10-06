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
import com.starrocks.type.FloatType;
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

    // A plain avg is computed together with the other non-distinct aggregates. We expect no distinct sum/count
    // branches for it, so two count distinct calls and one plain avg give exactly three aggregates and two joins.
    @Test
    void plainAvgDoesNotAddDistinctBranches() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator key = factory.create("key", IntegerType.INT, true);
        ColumnRefOperator arg0 = factory.create("arg0", IntegerType.INT, true);
        ColumnRefOperator arg1 = factory.create("arg1", IntegerType.INT, true);
        ColumnRefOperator avgArg = factory.create("avgArg", IntegerType.INT, true);
        Map<ColumnRefOperator, CallOperator> calls = new LinkedHashMap<>();
        calls.put(factory.create("count0", IntegerType.BIGINT, true),
                new CallOperator("count", IntegerType.BIGINT, List.of(arg0), null, true));
        calls.put(factory.create("count1", IntegerType.BIGINT, true),
                new CallOperator("count", IntegerType.BIGINT, List.of(arg1), null, true));
        ColumnRefOperator avgRef = factory.create("avg", FloatType.DOUBLE, true);
        calls.put(avgRef, new CallOperator("avg", FloatType.DOUBLE, List.of(avgArg), null, false));
        OptExpression input = OptExpression.create(
                new LogicalAggregationOperator(AggType.GLOBAL, List.of(key), calls),
                OptExpression.create(new LogicalValuesOperator(List.of(key, arg0, arg1, avgArg))));

        OptExpression result = new MultiDistinctByCTERewriter()
                .transformImpl(input, OptimizerFactory.mockContext(factory)).get(0);

        LogicalProjectOperator project = (LogicalProjectOperator) result.inputAt(1).getOp();
        assertEquals(avgRef, project.getColumnRefMap().get(avgRef));
        List<LogicalAggregationOperator> branches = new ArrayList<>();
        int joins = 0;
        List<OptExpression> pending = new ArrayList<>(List.of(result.inputAt(1).inputAt(0)));
        while (!pending.isEmpty()) {
            OptExpression expr = pending.remove(pending.size() - 1);
            if (expr.getOp() instanceof LogicalJoinOperator) {
                joins++;
                pending.addAll(expr.getInputs());
            } else {
                branches.add((LogicalAggregationOperator) expr.getOp());
            }
        }
        assertEquals(2, joins);
        assertEquals(3, branches.size());
        int distinctCounts = 0;
        int plainAvgs = 0;
        for (LogicalAggregationOperator branch : branches) {
            for (CallOperator call : branch.getAggregations().values()) {
                if (call.isDistinct() && "count".equalsIgnoreCase(call.getFnName())) {
                    distinctCounts++;
                } else if (!call.isDistinct() && "avg".equalsIgnoreCase(call.getFnName())) {
                    plainAvgs++;
                }
            }
        }
        assertEquals(2, distinctCounts);
        assertEquals(1, plainAvgs);
    }
}
