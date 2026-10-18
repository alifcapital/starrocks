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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * A Limit outputs the smaller of its limit and the rows of its input, as a logical and as a physical operator: the
 * conditions above it are estimated over the rows that exist, not over a limit that is far above them. The input has
 * 1000 rows.
 */
public class LimitStatisticsTest {
    private static final ColumnRefOperator A = new ColumnRefOperator(1, IntegerType.INT, "a", true);

    private static OptExpression input(double rows) {
        OptExpression input = OptExpression.create(new LogicalValuesOperator(List.of(A)));
        input.setStatistics(Statistics.builder().setOutputRowCount(rows)
                .addColumnStatistic(A, new ColumnStatistic(0, 99, 0, 4, 100)).build());
        return input;
    }

    private static double logicalRows(long limit, double inputRows) {
        ExpressionContext context = new ExpressionContext(
                OptExpression.create(LogicalLimitOperator.init(limit), input(inputRows)));
        ColumnRefFactory factory = new ColumnRefFactory();
        new StatisticsCalculator(context, factory, OptimizerFactory.mockContext(factory)).estimatorStats();
        return context.getStatistics().getOutputRowCount();
    }

    private static double physicalRows(long limit, double inputRows) {
        ExpressionContext context = new ExpressionContext(OptExpression.create(
                new PhysicalLimitOperator(Operator.DEFAULT_OFFSET, limit, null), input(inputRows)));
        ColumnRefFactory factory = new ColumnRefFactory();
        new StatisticsCalculator(context, factory, OptimizerFactory.mockContext(factory)).estimatorStats();
        return context.getStatistics().getOutputRowCount();
    }

    @Test
    public void testLimitAboveTheRowsOfTheInputKeepsTheRows() {
        Assertions.assertEquals(1000, logicalRows(1_000_000_000L, 1000));
        Assertions.assertEquals(1000, physicalRows(1_000_000_000L, 1000));
    }

    @Test
    public void testLimitAtOrBelowTheRowsOfTheInputIsTheLimit() {
        Assertions.assertEquals(1000, logicalRows(1000, 1000));
        Assertions.assertEquals(1000, physicalRows(1000, 1000));
        Assertions.assertEquals(100, logicalRows(100, 1000));
        Assertions.assertEquals(100, physicalRows(100, 1000));
    }

    @Test
    public void testLimitOverAnInputOfUnknownRowsIsTheLimit() {
        Assertions.assertEquals(500, logicalRows(500, Double.NaN));
        Assertions.assertEquals(500, physicalRows(500, Double.NaN));
    }

    @Test
    public void testLimitKeepsTheColumnsOfTheInput() {
        ExpressionContext context = new ExpressionContext(
                OptExpression.create(LogicalLimitOperator.init(1_000_000_000L), input(1000)));
        ColumnRefFactory factory = new ColumnRefFactory();
        new StatisticsCalculator(context, factory, OptimizerFactory.mockContext(factory)).estimatorStats();
        Assertions.assertEquals(100, context.getStatistics().getColumnStatistic(A).getDistinctValuesCount());
    }
}
