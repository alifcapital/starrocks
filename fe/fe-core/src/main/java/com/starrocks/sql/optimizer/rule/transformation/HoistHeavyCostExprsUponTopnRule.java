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
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.type.PrimitiveType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class HoistHeavyCostExprsUponTopnRule extends TransformationRule {
    public HoistHeavyCostExprsUponTopnRule() {
        super(RuleType.TF_HOIST_HEAVY_COST_UPON_TOPN,
                Pattern.create(OperatorType.LOGICAL_TOPN, OperatorType.LOGICAL_PROJECT));
    }

    private boolean isHeavyCost(ScalarOperator op) {
        if (op instanceof CallOperator) {
            CallOperator call = op.cast();
            if (call.getFnName().equals(ArithmeticExpr.Operator.DIVIDE.getName()) && (
                    call.getType().getPrimitiveType().equals(PrimitiveType.LARGEINT) ||
                            call.getType().getPrimitiveType().equals(PrimitiveType.DECIMAL128))) {
                return true;
            }
        }
        for (ScalarOperator child : op.getChildren()) {
            if (isHeavyCost(child)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topnOp = input.getOp().cast();
        if (topnOp.getPartitionByColumns() != null && !topnOp.getPartitionByColumns().isEmpty()) {
            return false;
        }
        if (topnOp.getPredicate() != null || topnOp.getLimit() < 0) {
            return false;
        }
        OptExpression child = input.inputAt(0);
        Set<ColumnRefOperator> heavyCostColumnRefs = child.getRowOutputInfo().getColumnOutputInfo()
                .stream()
                .filter(e -> isHeavyCost(e.getScalarOp()))
                .map(ColumnOutputInfo::getColumnRef)
                .collect(Collectors.toSet());

        if (heavyCostColumnRefs.isEmpty()) {
            return false;
        }

        boolean isUsedByPredicate = Optional.ofNullable(child.getOp().getPredicate())
                .map(pred -> pred.getUsedColumns().containsAny(heavyCostColumnRefs))
                .orElse(false);

        if (isUsedByPredicate) {
            return false;
        }

        ColumnRefSet heavyCostColumnRefSet = ColumnRefSet.of();
        heavyCostColumnRefSet.union(heavyCostColumnRefs);
        for (Ordering ordering : topnOp.getOrderByElements()) {
            if (heavyCostColumnRefSet.contains(ordering.getColumnRef().getId())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topnOp = input.getOp().cast();
        OptExpression child = input.inputAt(0);
        Map<ColumnRefOperator, ScalarOperator> heavyCostColumnRefMap = new HashMap<>();
        Map<ColumnRefOperator, ScalarOperator> childColumnRefMap = new HashMap<>();
        for (ColumnOutputInfo entry : child.getRowOutputInfo().getColumnOutputInfo()) {
            if (isHeavyCost(entry.getScalarOp())) {
                heavyCostColumnRefMap.put(entry.getColumnRef(), entry.getScalarOp());
            } else {
                childColumnRefMap.put(entry.getColumnRef(), entry.getScalarOp());
            }
        }

        Set<ColumnRefOperator> childColumnRefs = childColumnRefMap.keySet();
        for (ScalarOperator expression : heavyCostColumnRefMap.values()) {
            if (!childColumnRefs.containsAll(expression.getColumnRefs())) {
                return Collections.emptyList();
            }
        }

        Map<ColumnRefOperator, ScalarOperator> topnColumnRefMap = input.getRowOutputInfo().getColumnRefMap();
        topnColumnRefMap.putAll(heavyCostColumnRefMap);
        LogicalProjectOperator projectOp = child.getOp().cast();

        Operator newProjectOp = LogicalProjectOperator.builder().withOperator(projectOp)
                .setColumnRefMap(childColumnRefMap)
                .build();

        OptExpression newChild = OptExpression.builder().setOp(newProjectOp).setInputs(child.getInputs()).build();
        Operator upperProjectOp = LogicalProjectOperator.builder()
                .setColumnRefMap(topnColumnRefMap).build();

        OptExpression newTopn =
                OptExpression.builder().setOp(topnOp).setInputs(Lists.newArrayList(newChild)).build();

        return Collections.singletonList(OptExpression.create(upperProjectOp, newTopn));
    }
}
