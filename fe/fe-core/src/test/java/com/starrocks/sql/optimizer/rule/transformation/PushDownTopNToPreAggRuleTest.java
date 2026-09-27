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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.MultiColumnCombinedStats;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PushDownTopNToPreAggRuleTest {
    private final ColumnRefFactory factory = new ColumnRefFactory();
    private final ColumnRefOperator a = factory.create("a", IntegerType.INT, true);
    private final ColumnRefOperator b = factory.create("b", IntegerType.INT, true);
    private final OptimizerContext context = OptimizerFactory.mockContext(factory);
    private final PushDownTopNToPreAggRule rule = PushDownTopNToPreAggRule.getInstance();

    private OptExpression input(boolean fullKey, Statistics stats) {
        OptExpression source = OptExpression.create(new LogicalValuesOperator(List.of(a, b)));
        source.setStatistics(stats);
        OptExpression local = OptExpression.create(new LogicalAggregationOperator(AggType.LOCAL,
                List.of(a, b), Map.of()), source);
        local.setStatistics(stats);
        LogicalAggregationOperator global = new LogicalAggregationOperator(AggType.GLOBAL, List.of(a, b), Map.of());
        global.setSplit(true);
        LogicalTopNOperator topn = new LogicalTopNOperator.Builder()
                .setOrderByElements(fullKey ? List.of(new Ordering(a, true, true), new Ordering(b, false, false))
                        : List.of(new Ordering(a, true, true)))
                .setLimit(10).setSortPhase(SortPhase.PARTIAL).setTopNType(TopNType.ROW_NUMBER).build();
        return OptExpression.create(topn, OptExpression.create(global, local));
    }

    @Test
    void unknownStatsUseRankOnlyBelowFinalAggregation() {
        for (int mode : List.of(0, 1)) {
            context.getSessionVariable().setEnablePreAggTopNPushDown(mode);
            OptExpression input = input(false, null);
            assertTrue(rule.check(input, context));
            OptExpression result = rule.transform(input, context).get(0);
            assertSame(input.getOp(), result.getOp());
            LogicalTopNOperator local = result.inputAt(0).inputAt(0).getOp().cast();
            assertEquals(TopNType.RANK, local.getTopNType());
            assertTrue(local.isPerPipeline());
            assertEquals(TopNType.ROW_NUMBER, ((LogicalTopNOperator) result.getOp()).getTopNType());
        }
    }

    @Test
    void completeGroupOrderingRetainsBoundedTopN() {
        OptExpression result = rule.transform(input(true, null), context).get(0);
        LogicalTopNOperator local = result.inputAt(0).inputAt(0).getOp().cast();
        assertEquals(TopNType.ROW_NUMBER, local.getTopNType());
    }

    @Test
    void manyPeersKeepAggregateFilterWithoutSortAndDoNotReapply() {
        context.getSessionVariable().setEnablePreAggTopNPushDown(1);
        Statistics stats = Statistics.builder().setOutputRowCount(1000000)
                .addColumnStatistic(a, new ColumnStatistic(0, 1, 0, 4, 2))
                .addColumnStatistic(b, new ColumnStatistic(0, 99999, 0, 4, 100000))
                .addMultiColumnStatistics(Set.of(a, b), new MultiColumnCombinedStats(100000)).build();
        OptExpression input = input(false, stats);
        OptExpression result = rule.transform(input, context).get(0);
        LogicalAggregationOperator local = result.inputAt(0).inputAt(0).getOp().cast();
        assertTrue(local.isTopNLocalAgg());
        assertNotNull(local.getAggTopnSortInfo());
        assertFalse(rule.check(result, context));
        context.getSessionVariable().setEnablePreAggTopNPushDown(0);
        assertTrue(rule.transform(input, context).isEmpty());
    }
}
