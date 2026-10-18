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
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.CTEContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.NonDeterministicVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CollectCTEConsumeRule extends TransformationRule {
    public CollectCTEConsumeRule() {
        super(RuleType.TF_COLLECT_CTE_CONSUME, Pattern.create(OperatorType.LOGICAL_CTE_CONSUME));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalCTEConsumeOperator consume = (LogicalCTEConsumeOperator) input.getOp();
        CTEContext cteContext = context.getCteContext();

        // record cte consume
        cteContext.addCTEConsume(consume.getCteId());

        // The producer gets the OR of the predicates of its consumers, and each consumer keeps its own predicate, so
        // a row is checked twice. We want a nondeterministic predicate like rand() < 0.5 to be evaluated once per row,
        // so we push only the deterministic conjuncts. A consumer without such conjuncts needs all rows of the CTE,
        // and its limit applies after its predicate, so we record neither its predicate nor its limit.
        ScalarOperator pushablePredicate = null;
        if (consume.getPredicate() != null) {
            List<ScalarOperator> deterministicConjuncts = Utils.extractConjuncts(consume.getPredicate()).stream()
                    .filter(conjunct -> !NonDeterministicVisitor.hasNonDeterministicFunc(conjunct))
                    .collect(Collectors.toList());
            if (deterministicConjuncts.isEmpty()) {
                return Collections.emptyList();
            }
            pushablePredicate = Utils.compoundAnd(deterministicConjuncts);
        }

        // collect limit
        if (consume.hasLimit()) {
            if (cteContext.getConsumeLimits().containsKey(consume.getCteId())) {
                cteContext.getConsumeLimits().get(consume.getCteId()).add(consume.getLimit());
            } else {
                cteContext.getConsumeLimits().put(consume.getCteId(), Lists.newArrayList(consume.getLimit()));
            }
        }

        // collect predicate, should rewrite by CTE produce column
        if (null != pushablePredicate) {
            Map<ColumnRefOperator, ScalarOperator> mapping = Maps.newHashMap(consume.getCteOutputColumnRefMap());
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(mapping);
            ScalarOperator predicate = rewriter.rewrite(pushablePredicate);

            if (cteContext.getConsumePredicates().containsKey(consume.getCteId())) {
                cteContext.getConsumePredicates().get(consume.getCteId()).add(predicate);
            } else {
                cteContext.getConsumePredicates().put(consume.getCteId(), Lists.newArrayList(predicate));
            }
        }

        return Collections.emptyList();
    }
}
