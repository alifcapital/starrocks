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

package com.starrocks.sql.optimizer.rule.transformation.materialization.common;

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoProjectRule;
import com.starrocks.sql.optimizer.rule.tree.pdagg.AggregatePushDownContext;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * The query has avg(x) and sum(x). When the push-down context splits avg(x) into sum and count, it reuses
 * the query's own sum(x) call. The rollup must build new calls and must not change this shared call:
 * the query plan still uses it.
 */
public class AggregatePushDownAvgRollupTest extends PlanTestNoneDBBase {
    @Test
    void avgRollupDoesNotRewriteTheQuerySumCall() {
        Fixture f = new Fixture();
        OptExpression result = f.rewrite(f.remapping);
        assertNotNull(result);
        LogicalAggregationOperator query = f.query.getOp().cast();
        assertSame(f.querySum, query.getAggregations().get(f.querySumOutput));
        // We expect the avg split to reuse the query's sum(x); otherwise this test checks nothing.
        assertSame(f.querySum, f.ctx.aggregations.get(f.avgPair.first));
        assertSame(f.querySum, f.ctx.aggColRefToPushDownAggMap.get(f.avgPair.first));
        f.assertSharedCallsUnchanged();
        f.assertRollupOperands(result);
    }

    @Test
    void refusedAvgRollupDoesNotLeaveTheSumRewritten() {
        Fixture f = new Fixture();
        // The sum part of avg is mapped and the count part is not, so the rollup is refused
        // after the sum call has been handled.
        Map<ColumnRefOperator, ScalarOperator> sumOnly = new LinkedHashMap<>();
        sumOnly.put(f.avgPair.first, f.partialSum);
        assertNull(f.rewrite(sumOnly));
        assertSame(f.querySum, f.ctx.aggColRefToPushDownAggMap.get(f.avgPair.first));
        f.assertSharedCallsUnchanged();

        // The same context is used for the next candidate, so a later rollup must still work.
        OptExpression retry = f.rewrite(f.remapping);
        assertNotNull(retry);
        f.assertSharedCallsUnchanged();
        f.assertRollupOperands(retry);
    }

    private static final class Fixture {
        private final ColumnRefFactory factory = new ColumnRefFactory();
        private final ColumnRefOperator input = factory.create("input", IntegerType.BIGINT, true);
        private final AggregatePushDownContext ctx = new AggregatePushDownContext();
        private final CallOperator querySum;
        private final ColumnRefOperator querySumOutput;
        private final Function querySumFunction;
        private final ColumnRefOperator queryAvgOutput;
        private final CallOperator splitCount;
        private final Function splitCountFunction;
        private final Pair<ColumnRefOperator, ColumnRefOperator> avgPair;
        private final ColumnRefOperator partialSum;
        private final ColumnRefOperator partialCount;
        private final OptExpression query;
        private final OptExpression partialChild;
        private final MvRewriteContext rewriteContext;
        private final Map<ColumnRefOperator, ScalarOperator> remapping = new LinkedHashMap<>();

        private Fixture() {
            Function sumFn = ScalarOperatorUtil.findSumFn(new Type[] {input.getType()});
            querySum = new CallOperator(FunctionSet.SUM, sumFn.getReturnType(), List.of(input), sumFn);
            querySumFunction = querySum.getFunction();
            querySumOutput = factory.create("query_sum", querySum.getType(), true);

            Function avgFn = ScalarOperatorUtil.findArithmeticFunction(new Type[] {input.getType()}, FunctionSet.AVG);
            CallOperator avg = new CallOperator(FunctionSet.AVG, avgFn.getReturnType(), List.of(input), avgFn);
            queryAvgOutput = factory.create("query_avg", avg.getType(), true);

            Map<ColumnRefOperator, CallOperator> aggregations = new LinkedHashMap<>();
            aggregations.put(queryAvgOutput, avg);
            aggregations.put(querySumOutput, querySum);
            LogicalAggregationOperator aggregate = LogicalAggregationOperator.builder()
                    .setType(AggType.GLOBAL)
                    .setGroupingKeys(List.of())
                    .setPartitionByColumns(List.of())
                    .setAggregations(aggregations)
                    .build();
            OptExpression source = OptExpression.create(new LogicalValuesOperator(List.of(input)));
            query = OptExpression.create(aggregate, source);
            ctx.setAggregator(factory, aggregate);
            avgPair = ctx.avgToSumCountMapping.get(avg);
            assertNotNull(avgPair);
            ctx.aggColRefToPushDownAggMap.putAll(ctx.aggregations);

            partialSum = factory.create("partial_sum", querySum.getType(), true);
            remapping.put(querySumOutput, partialSum);
            remapping.put(avgPair.first, partialSum);
            splitCount = ctx.aggregations.get(avgPair.second);
            assertNotNull(splitCount);
            splitCountFunction = splitCount.getFunction();
            partialCount = factory.create("partial_count", splitCount.getType(), true);
            remapping.put(avgPair.second, partialCount);
            partialChild = OptExpression.create(new LogicalValuesOperator(List.of(partialSum, partialCount)));

            var optimizer = OptimizerFactory.initContext(connectContext, factory);
            MaterializedView mv = new MaterializedView();
            mv.setName("avg_rollup_mv");
            MaterializationContext materialization = new MaterializationContext(optimizer, mv, source,
                    factory, factory, List.of(), List.of(), null, List.of(), 0);
            rewriteContext = new MvRewriteContext(materialization, List.of(), query,
                    new ReplaceColumnRefRewriter(Map.of()), null, List.of(), new MergeTwoProjectRule());
        }

        private void assertSharedCallsUnchanged() {
            assertSame(input, querySum.getChild(0));
            assertSame(querySumFunction, querySum.getFunction());
            assertSame(input.getType(), querySumFunction.getArgs()[0]);
            assertSame(querySum.getType(), querySumFunction.getReturnType());
            assertSame(splitCount, ctx.aggregations.get(avgPair.second));
            assertSame(splitCount, ctx.aggColRefToPushDownAggMap.get(avgPair.second));
            assertSame(input, splitCount.getChild(0));
            assertSame(splitCountFunction, splitCount.getFunction());
            assertSame(input.getType(), splitCountFunction.getArgs()[0]);
        }

        private void assertRollupOperands(OptExpression result) {
            LogicalAggregationOperator output = result.getOp().cast();
            // sum and count of avg plus the explicit sum; all of them roll up with sum.
            assertEquals(3, output.getAggregations().size());
            int sumInputs = 0;
            int countInputs = 0;
            for (CallOperator call : output.getAggregations().values()) {
                assertEquals(FunctionSet.SUM, call.getFnName());
                assertNotSame(querySumFunction, call.getFunction());
                assertEquals(1, call.getChildren().size());
                if (call.getChild(0) == partialSum) {
                    sumInputs++;
                } else {
                    assertSame(partialCount, call.getChild(0));
                    countInputs++;
                }
            }
            assertEquals(2, sumInputs);
            assertEquals(1, countInputs);
            ScalarOperator avgProjection = output.getProjection().getColumnRefMap().get(queryAvgOutput);
            assertNotNull(avgProjection);
            assertFalse(avgProjection.getUsedColumns().contains(input));
            assertFalse(avgProjection.getUsedColumns().contains(partialSum));
            assertFalse(avgProjection.getUsedColumns().contains(partialCount));
            assertEquals(2, avgProjection.getUsedColumns().cardinality());
        }

        private OptExpression rewrite(Map<ColumnRefOperator, ScalarOperator> mapping) {
            return AggregatePushDownUtils.getPushDownRollupFinalAggregateOpt(
                    rewriteContext, ctx, mapping, query, List.of(partialChild));
        }
    }
}
