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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.FoldConstantsRule;
import com.starrocks.sql.optimizer.rewrite.scalar.NormalizePredicateRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ReduceCastRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ScalarOperatorRewriteRule;
import com.starrocks.sql.optimizer.rewrite.scalar.SimplifiedPredicateRule;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class PushDownPredicateProjectRule extends TransformationRule {
    private static final List<ScalarOperatorRewriteRule> PROJECT_REWRITE_PREDICATE_RULE = Lists.newArrayList(
            // Don't need add cast because all expression has been cast
            // The purpose here is for simplify the expression
            new ReduceCastRule(),
            new NormalizePredicateRule(),
            new FoldConstantsRule(),
            new SimplifiedPredicateRule()
    );

    public PushDownPredicateProjectRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_PROJECT,
                Pattern.create(OperatorType.LOGICAL_FILTER).
                        addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalProjectOperator secondProject = (LogicalProjectOperator) input.getInputs().get(0).getOp();
        Optional<ScalarOperator> assertColumn = secondProject.getColumnRefMap().values()
                .stream()
                .filter((op) -> {
                    if (!(op instanceof CallOperator)) {
                        return false;
                    }
                    return FunctionSet.ASSERT_TRUE.equals(((CallOperator) op).getFnName());
                })
                .findAny();
        return !assertColumn.isPresent();
    }

    private boolean hasLambda(ScalarOperator op) {
        if (op == null) {
            return false;
        }
        if (op instanceof LambdaFunctionOperator) {
            return true;
        }
        for (var c : op.getChildren()) {
            if (hasLambda(c)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filter = (LogicalFilterOperator) input.getOp();
        OptExpression child = input.getInputs().get(0);

        LogicalProjectOperator project = (LogicalProjectOperator) (child.getOp());

        if (!context.getSessionVariable().getEnableLambdaPushDown()) {
            Map<ColumnRefOperator, ScalarOperator> m = project.getColumnRefMap();
            for (var entry : m.entrySet()) {
                if (hasLambda(entry.getValue())) {
                    return Lists.newArrayList();
                }
            }
        }

        // Check if the filter's predicate contains non-deterministic functions
        // If it does, don't push down the predicate below project to avoid incorrect results
        List<ScalarOperator> compoundAndPredicates = Utils.extractConjuncts(filter.getPredicate());
        Set<ScalarOperator> pushedPredicates = new HashSet<>();
        Set<ScalarOperator> keptPredicates = new HashSet<>();
        for (var entry : project.getColumnRefMap().entrySet()) {
            if (Utils.hasNonDeterministicFunc(entry.getValue())) {
                compoundAndPredicates.forEach(scalarOperator -> {
                    if (scalarOperator.getUsedColumns().contains(entry.getKey())) {
                        keptPredicates.add(scalarOperator);
                    }
                });
            }
        }
        compoundAndPredicates.forEach(predicate -> {
            if (!keptPredicates.contains(predicate)) {
                pushedPredicates.add(predicate);
            }
        });

        // Below the project the predicate gets a copy of the expression of a column for each reference, as a merge of
        // two projects does. When the copies are too large, we keep the predicates on the columns with expressions
        // above the project and push only the predicates on the other columns.
        if (!pushedPredicates.isEmpty() && ReplaceColumnRefRewriter.isTooLarge(
                List.of(Utils.createCompound(CompoundPredicateOperator.CompoundType.AND, pushedPredicates)),
                project.getColumnRefMap())) {
            for (ScalarOperator predicate : List.copyOf(pushedPredicates)) {
                ColumnRefSet usedColumns = predicate.getUsedColumns();
                if (project.getColumnRefMap().entrySet().stream().anyMatch(
                        entry -> entry.getValue().getNumFlatChildren() > 1 && usedColumns.contains(entry.getKey()))) {
                    pushedPredicates.remove(predicate);
                    keptPredicates.add(predicate);
                }
            }
        }

        // Nothing to push down
        if (pushedPredicates.isEmpty()) {
            return Lists.newArrayList();
        }

        ScalarOperator pushedPredicateTree;
        if (keptPredicates.isEmpty()) {
            pushedPredicateTree = filter.getPredicate();
        } else {
            pushedPredicateTree = Utils.createCompound(CompoundPredicateOperator.CompoundType.AND, pushedPredicates);
        }

        ScalarOperator keptPredicateTree = Utils.createCompound(CompoundPredicateOperator.CompoundType.AND,
                keptPredicates);

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(project.getColumnRefMap());
        ScalarOperator newPredicate = rewriter.rewrite(pushedPredicateTree);

        // try rewrite new predicate
        // e.g. : select 1 as b, MIN(v1) from t0 having (b + 1) != b;
        ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
        newPredicate = scalarRewriter.rewrite(newPredicate, PROJECT_REWRITE_PREDICATE_RULE);

        OptExpression newFilter = new OptExpression(new LogicalFilterOperator(newPredicate));
        newFilter.getInputs().addAll(child.getInputs());

        OptExpression newProject = OptExpression.create(project, newFilter);
        OptExpression root;
        if (!keptPredicates.isEmpty()) {
            root = OptExpression.create(new LogicalFilterOperator(keptPredicateTree), newProject);
        } else {
            root = newProject;
        }

        return Lists.newArrayList(root);
    }
}
