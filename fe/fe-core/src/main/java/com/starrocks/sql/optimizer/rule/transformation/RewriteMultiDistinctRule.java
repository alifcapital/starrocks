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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class RewriteMultiDistinctRule extends TransformationRule {

    public RewriteMultiDistinctRule() {
        super(RuleType.TF_REWRITE_MULTI_DISTINCT,
                Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(
                        OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();

        // Without a second distinct function only the complex constant count distinct case below can apply.
        int distinctCallCount = 0;
        for (CallOperator call : agg.getAggregations().values()) {
            if (call.isDistinct() && ++distinctCallCount == 2) {
                break;
            }
        }
        if (distinctCallCount == 0) {
            return false;
        }
        if (distinctCallCount == 1) {
            return isComplexConstantCountDistinct(input);
        }

        // any aggregate function is distinct and constant, we replace it to any_value
        if (isComplexConstantCountDistinct(input)) {
            return true;
        }

        // if have multiple distinct functions and their distinct input cols are not exactly same
        Optional<List<ColumnRefOperator>> distinctCols = Utils.extractCommonDistinctCols(agg.getAggregations().values());
        if (distinctCols.isEmpty()) {
            return true;
        }

        // If have multiple distinct functions and their distinct input cols are exactly same, but split Agg rule can't handle it
        // such as table has one tablet property
        if (Utils.hasMultipleDistinctFuncShareSameDistinctColumns(agg.getAggregations().values()) &&
                !Utils.couldGenerateMultiStageAggregate(input.getLogicalProperty(), input.getOp(),
                        input.inputAt(0).getOp())) {
            return true;
        }

        // all distinct function use the same distinct columns and split rule can handle it, we use the split rule to rewrite
        return false;
    }

    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        if (isComplexConstantCountDistinct(input)) {
            return rewriteComplexConstantDistinct(input);
        }
        if (useCteToRewrite(input, context)) {
            MultiDistinctByCTERewriter rewriter = new MultiDistinctByCTERewriter();
            return rewriter.transformImpl(input, context);
        } else {
            MultiDistinctByMultiFuncRewriter rewriter = new MultiDistinctByMultiFuncRewriter();
            return rewriter.transformImpl(input, context);
        }
    }

    private boolean isComplexConstantCountDistinct(OptExpression input) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        // sr support count(distinct constant array/struct/map) in four-phase aggregate,
        // but doesn't support in two-phase aggregate, we replace it to any_value
        return agg.getAggregations().values().stream()
                .anyMatch(c -> c.isDistinct() && c.isConstant() && FunctionSet.COUNT.equalsIgnoreCase(c.getFnName()) &&
                        (c.getChildren().size() == 1) && c.getChild(0).getType().isComplexType());
    }

    private List<OptExpression> rewriteComplexConstantDistinct(OptExpression input) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();

        agg.getAggregations().forEach((k, v) -> {
            if (v.isDistinct() && v.isConstant() && FunctionSet.COUNT.equalsIgnoreCase(v.getFnName()) &&
                    (v.getChildren().size() == 1) && v.getChild(0).getType().isComplexType()) {
                Preconditions.checkState(v.getType() == IntegerType.BIGINT);
                IsNullPredicateOperator isNull = new IsNullPredicateOperator(true, v.getChild(0));
                CastOperator cast = new CastOperator(IntegerType.BIGINT, isNull);
                Function fn = ExprUtils.getBuiltinFunction(FunctionSet.ANY_VALUE, new Type[] {IntegerType.BIGINT},
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                CallOperator anyValue =
                        new CallOperator(FunctionSet.ANY_VALUE, v.getType(), Lists.newArrayList(cast), fn, false);
                newAggregations.put(k, anyValue);
            } else {
                newAggregations.put(k, v);
            }
        });

        LogicalAggregationOperator newAgg =
                LogicalAggregationOperator.builder().withOperator(agg).setAggregations(newAggregations).build();
        return Lists.newArrayList(OptExpression.create(newAgg, input.getInputs()));
    }

    // Several distinct aggregations with different arguments run either as one multi_distinct aggregation, which
    // keeps a set of argument values per group, or as CTE branches, one per argument, that remove duplicates by
    // (group keys, argument) and are joined back by the group keys.
    // We expect other queries to run on the cluster at the same time, so we count CPU and not the latency of one
    // query on an idle cluster. CTE reads the input once per branch and builds a table of (group keys, argument)
    // pairs in every branch. In our measurements CTE used 1.35 to 3 times more CPU than multi_distinct. CTE was
    // faster only alone on an idle cluster, where it used the free cores: without GROUP BY or with few groups. With
    // 4 queries at the same time multi_distinct was faster in all cases, those included. So we use multi_distinct
    // unless it cannot compute the aggregations or the user asks for CTE.
    private boolean useCteToRewrite(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        List<CallOperator> distinctAggOperatorList = agg.getAggregations().values().stream()
                .filter(CallOperator::isDistinct).collect(Collectors.toList());
        // multi_distinct functions take one column
        if (distinctAggOperatorList.stream().anyMatch(f -> f.getColumnRefs().size() > 1)) {
            return true;
        }

        if (context.getSessionVariable().isPreferCTERewrite()) {
            return true;
        }

        // respect skew hint
        if (agg.hasSkew() && !agg.getGroupingKeys().isEmpty()) {
            return true;
        }

        for (CallOperator distinctCall : distinctAggOperatorList) {
            String fnName = distinctCall.getFnName();
            Type type = distinctCall.getChildren().get(0).getType();
            if (type.isComplexType()
                    || type.isJsonType()
                    || FunctionSet.GROUP_CONCAT.equalsIgnoreCase(fnName)
                    || (FunctionSet.ARRAY_AGG.equalsIgnoreCase(fnName) && type.isDecimalOfAnyVersion())) {
                return true;
            }
        }
        return false;
    }
}
