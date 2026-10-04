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

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SimplifyCaseWhenPredicateRule implements TreeRewriteRule {
    public static final SimplifyCaseWhenPredicateRule INSTANCE = new SimplifyCaseWhenPredicateRule();

    private SimplifyCaseWhenPredicateRule() {
    }

    private static final class Rewriter extends OptExpressionVisitor<Optional<OptExpression>, Void> {
        public static final Rewriter INSTANCE = new Rewriter();

        private Rewriter() {
        }

        // case when simplification is a null-sensitive Rule.
        // We should not apply this rule to operators whose child produces a null output.
        private boolean hasNullableGenerateChild(OptExpression optExpression) {
            final Operator op = optExpression.getOp();
            if (op instanceof LogicalRepeatOperator) {
                return true;
            }
            if (op instanceof LogicalJoinOperator join) {
                return join.getJoinType().isOuterJoin();
            }

            for (OptExpression input : optExpression.getInputs()) {
                if (hasNullableGenerateChild(input)) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public Optional<OptExpression> visit(OptExpression optExpression, Void context) {
            ScalarOperator predicate = optExpression.getOp().getPredicate();
            if (predicate == null) {
                return Optional.empty();
            }
            if (hasNullableGenerateChild(optExpression)) {
                return Optional.empty();
            }
            ScalarOperator newPredicate = ScalarOperatorRewriter.simplifyCaseWhen(predicate, true);
            if (newPredicate == predicate) {
                return Optional.empty();
            }
            Operator newOperator = OperatorBuilderFactory.build(optExpression.getOp())
                    .withOperator(optExpression.getOp())
                    .setPredicate(newPredicate).build();
            return Optional.of(OptExpression.create(newOperator, optExpression.getInputs()));
        }

        @Override
        public Optional<OptExpression> visitLogicalJoin(OptExpression optExpression, Void context) {
            LogicalJoinOperator joinOperator = optExpression.getOp().cast();
            if (hasNullableGenerateChild(optExpression)) {
                return Optional.empty();
            }
            Optional<ScalarOperator> optNewOnPredicate =
                    Optional.ofNullable(joinOperator.getOnPredicate()).map(predicate -> {
                        ScalarOperator newPredicate = ScalarOperatorRewriter.simplifyCaseWhen(predicate, true);
                        return newPredicate == predicate ? null : newPredicate;
                    });
            Optional<ScalarOperator> optNewPredicate =
                    Optional.ofNullable(joinOperator.getPredicate()).map(predicate -> {
                        ScalarOperator newPredicate = ScalarOperatorRewriter.simplifyCaseWhen(predicate, true);
                        return newPredicate == predicate ? null : newPredicate;
                    });
            Operator newOperator = LogicalJoinOperator.builder().withOperator(joinOperator)
                    .setOnPredicate(optNewOnPredicate.orElse(joinOperator.getOnPredicate()))
                    .setPredicate(optNewPredicate.orElse(joinOperator.getPredicate()))
                    .build();
            return Optional.of(OptExpression.create(newOperator, optExpression.getInputs()));
        }

        private Optional<OptExpression> processImpl(OptExpression optExpression) {
            List<OptExpression> inputs = optExpression.getInputs();
            List<OptExpression> newInputs = null;
            for (int i = 0; i < inputs.size(); i++) {
                Optional<OptExpression> optNewInput = processImpl(inputs.get(i));
                if (optNewInput.isPresent()) {
                    if (newInputs == null) {
                        newInputs = new ArrayList<>(inputs);
                    }
                    newInputs.set(i, optNewInput.get());
                }
            }

            // visit methods read only the operator, its predicate and the inputs, and build their result from
            // fresh expressions, so the original node can stand in when no input changed.
            OptExpression newExpression =
                    newInputs == null ? optExpression : OptExpression.create(optExpression.getOp(), newInputs);
            return optExpression.getOp().accept(this, newExpression, null);
        }

        public OptExpression process(OptExpression optExpression) {
            return processImpl(optExpression).orElse(optExpression);
        }
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        if (taskContext.getOptimizerContext().getSessionVariable().isEnableSimplifyCaseWhen()) {
            return Rewriter.INSTANCE.process(root);
        } else {
            return root;
        }
    }
}
