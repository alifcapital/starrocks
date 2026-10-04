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

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MergeLimitWithSortRuleTest {

    @Test
    public void transform() {
        OptExpression limit = new OptExpression(LogicalLimitOperator.init(10, 2));
        OptExpression sort = new OptExpression(new LogicalTopNOperator(
                Lists.newArrayList(new Ordering(new ColumnRefOperator(1, IntegerType.INT, "name", true), false, false))));

        limit.getInputs().add(sort);

        MergeLimitWithSortRule rule = new MergeLimitWithSortRule();
        List<OptExpression> list = rule.transform(limit, OptimizerFactory.mockContext(new ColumnRefFactory()));

        assertEquals(OperatorType.LOGICAL_TOPN, list.get(0).getOp().getOpType());
        assertEquals(2, ((LogicalTopNOperator) list.get(0).getOp()).getOffset());
        assertEquals(10, ((LogicalTopNOperator) list.get(0).getOp()).getLimit());
    }

    private static OptExpression limitOverTopN(LogicalLimitOperator limit, long topNLimit, long topNOffset) {
        OptExpression input = new OptExpression(limit);
        input.getInputs().add(new OptExpression(new LogicalTopNOperator(
                Lists.newArrayList(new Ordering(new ColumnRefOperator(1, IntegerType.INT, "name", true), true, true)),
                topNLimit, topNOffset)));
        return input;
    }

    private static LogicalTopNOperator merge(LogicalLimitOperator limit, long topNLimit, long topNOffset) {
        OptExpression input = limitOverTopN(limit, topNLimit, topNOffset);
        MergeLimitWithSortRule rule = new MergeLimitWithSortRule();
        OptimizerContext context = OptimizerFactory.mockContext(new ColumnRefFactory());
        assertTrue(rule.check(input, context));
        return (LogicalTopNOperator) rule.transform(input, context).get(0).getOp();
    }

    @Test
    public void mergeKeepsOnlyRowsThatBothOperatorsKeep() {
        // ORDER BY ... LIMIT 5 under LIMIT 20: the first 5 rows.
        LogicalTopNOperator merged = merge(LogicalLimitOperator.local(20), 5, 0);
        assertEquals(5, merged.getLimit());
        assertEquals(0, merged.getOffset());

        // ORDER BY ... LIMIT 5 OFFSET 2 under LIMIT 20: rows 2..6 of the order.
        merged = merge(LogicalLimitOperator.local(20), 5, 2);
        assertEquals(5, merged.getLimit());
        assertEquals(2, merged.getOffset());

        // ORDER BY ... LIMIT 10 OFFSET 3 under LIMIT 4 OFFSET 2: rows 5..8 of the order.
        merged = merge(LogicalLimitOperator.init(4, 2), 10, 3);
        assertEquals(4, merged.getLimit());
        assertEquals(5, merged.getOffset());

        // ORDER BY ... LIMIT 3 under LIMIT 10 OFFSET 2: only row 2 of the order.
        merged = merge(LogicalLimitOperator.init(10, 2), 3, 0);
        assertEquals(1, merged.getLimit());
        assertEquals(2, merged.getOffset());

        // ORDER BY ... LIMIT 5 under OFFSET 2 without a limit: rows 2..4 of the order.
        merged = merge(LogicalLimitOperator.init(Operator.DEFAULT_LIMIT, 2), 5, 0);
        assertEquals(3, merged.getLimit());
        assertEquals(2, merged.getOffset());

        // ORDER BY ... OFFSET 3 without a limit under LIMIT 4 OFFSET 2: rows 5..8 of the order.
        merged = merge(LogicalLimitOperator.init(4, 2), Operator.DEFAULT_LIMIT, 3);
        assertEquals(4, merged.getLimit());
        assertEquals(5, merged.getOffset());
    }

    @Test
    public void limitThatSkipsEveryRowOfTopNIsNotMerged() {
        // ORDER BY ... LIMIT 3 under LIMIT 10 OFFSET 5 returns no rows, and a TopN cannot have a zero limit.
        OptExpression input = limitOverTopN(LogicalLimitOperator.init(10, 5), 3, 0);
        assertFalse(new MergeLimitWithSortRule().check(input, OptimizerFactory.mockContext(new ColumnRefFactory())));
    }
}
