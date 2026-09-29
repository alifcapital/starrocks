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
import com.starrocks.sql.optimizer.CTEContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarRangePredicateExtractor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/*
 *                    CTEProduce
 *                         |
 *  CTEProduce           Limit
 *      |         =>       |
 *     Node              Filter
 *                         |
 *                        Node
 *
 * */
public class PushLimitAndFilterToCTEProduceRule extends TransformationRule {
    public PushLimitAndFilterToCTEProduceRule() {
        super(RuleType.TF_PUSH_CTE_PRODUCE,
                Pattern.create(OperatorType.LOGICAL_CTE_PRODUCE, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        CTEContext cteContext = context.getCteContext();
        LogicalCTEProduceOperator produce = (LogicalCTEProduceOperator) input.getOp();

        List<Long> limits = cteContext.getConsumeLimits().getOrDefault(produce.getCteId(), Collections.emptyList());
        List<ScalarOperator> predicates =
                cteContext.getConsumePredicates().getOrDefault(produce.getCteId(), Collections.emptyList());

        int consumeNums = cteContext.getCTEConsumeNum(produce.getCteId());

        return consumeNums > 0 && (limits.size() == consumeNums || predicates.size() == consumeNums);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        CTEContext cteContext = context.getCteContext();
        LogicalCTEProduceOperator produce = (LogicalCTEProduceOperator) input.getOp();

        List<Long> limits = cteContext.getConsumeLimits().getOrDefault(produce.getCteId(), Collections.emptyList());
        List<ScalarOperator> predicates =
                cteContext.getConsumePredicates().getOrDefault(produce.getCteId(), Collections.emptyList());

        int consumeNums = cteContext.getCTEConsumeNum(produce.getCteId());

        OptExpression child = input.getInputs().get(0);
        if (consumeNums == predicates.size()) {
            ScalarOperator orPredicate = Utils.compoundOr(removeAbsorbed(predicates));
            ScalarRangePredicateExtractor extractor = new ScalarRangePredicateExtractor();
            child = OptExpression.create(new LogicalFilterOperator(extractor.rewriteAll(orPredicate)), child);
        }

        if (consumeNums == limits.size() && predicates.isEmpty()) {
            // only push down limit when no predicate
            Long maxLimit = limits.stream().reduce(Long::max).orElse(Operator.DEFAULT_LIMIT);
            child = OptExpression.create(LogicalLimitOperator.local(maxLimit), child);
        }

        return Lists.newArrayList(OptExpression.create(produce, child));
    }

    // P OR (P AND R) keeps the rows of P, so a predicate that has all conjuncts of another one is left out of the OR.
    // Of predicates with the same conjuncts the first one is kept.
    private static List<ScalarOperator> removeAbsorbed(List<ScalarOperator> predicates) {
        List<ScalarOperator> distinct = new ArrayList<>(new LinkedHashSet<>(predicates));
        List<Set<ScalarOperator>> conjuncts = new ArrayList<>(distinct.size());
        for (ScalarOperator predicate : distinct) {
            conjuncts.add(new HashSet<>(Utils.extractConjuncts(predicate)));
        }
        List<ScalarOperator> result = new ArrayList<>(distinct.size());
        for (int i = 0; i < distinct.size(); i++) {
            boolean absorbed = false;
            for (int j = 0; j < distinct.size() && !absorbed; j++) {
                absorbed = j != i && conjuncts.get(i).containsAll(conjuncts.get(j))
                        && (conjuncts.get(i).size() > conjuncts.get(j).size() || j < i);
            }
            if (!absorbed) {
                result.add(distinct.get(i));
            }
        }
        return result;
    }
}
