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

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class HashJoinImplementationRule extends JoinImplementationRule {
    private HashJoinImplementationRule() {
        super(RuleType.IMP_EQ_JOIN_TO_HASH_JOIN);
    }

    private static final HashJoinImplementationRule INSTANCE = new HashJoinImplementationRule();

    public static HashJoinImplementationRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        JoinOperator joinType = getJoinType(input);
        if (joinType.isCrossJoin()) {
            return false;
        }

        // Check for standard equality predicates (AND-conjuncts)
        List<BinaryPredicateOperator> eqPredicates = extractEqPredicate(input, context);
        if (CollectionUtils.isNotEmpty(eqPredicates)) {
            return true;
        }

        // Check for disjunctive OR predicates: ON a = x OR b = y
        // These can use hash join with multiple hash tables
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();
        if (joinOperator.getOnPredicate() != null) {
            ColumnRefSet leftColumns = input.inputAt(0).getLogicalProperty().getOutputColumns();
            ColumnRefSet rightColumns = input.inputAt(1).getLogicalProperty().getOutputColumns();
            if (JoinHelper.canUseHashJoinWithOr(leftColumns, rightColumns, joinOperator.getOnPredicate())) {
                return true;
            }
        }

        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();

        PhysicalHashJoinOperator physicalHashJoin = new PhysicalHashJoinOperator(
                joinOperator.getJoinType(),
                joinOperator.getOnPredicate(),
                joinOperator.getJoinHint(),
                joinOperator.getLimit(),
                joinOperator.getPredicate(),
                joinOperator.getProjection(),
                joinOperator.getSkewColumn(),
                joinOperator.getSkewValues());
        OptExpression result = OptExpression.create(physicalHashJoin, input.getInputs());
        return Lists.newArrayList(result);
    }
}
