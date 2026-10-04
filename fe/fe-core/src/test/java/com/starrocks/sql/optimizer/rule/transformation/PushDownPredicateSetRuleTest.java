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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class PushDownPredicateSetRuleTest {
    @Test
    public void mappingsStayIndependentAcrossBranchesAndProjectionLevels() {
        // The rule rewrites the pushed predicate once per union branch. We expect each branch to get its own
        // predicate over its own columns, so a later branch must not change the predicate of an earlier one,
        // the filter predicate itself, the existing child predicates or the child projections.
        for (boolean addFilter : new boolean[] {false, true}) {
            for (boolean projected : new boolean[] {false, true}) {
                var output = new ColumnRefOperator(1, IntegerType.INT, "output", true);
                var alias = new ColumnRefOperator(2, IntegerType.INT, "alias", true);
                List<List<ColumnRefOperator>> branchColumns = new ArrayList<>();
                List<OptExpression> children = new ArrayList<>();
                List<ScalarOperator> expected = new ArrayList<>();
                List<ScalarOperator> originalPredicates = new ArrayList<>();
                List<ScalarOperator> projectionExpressions = new ArrayList<>();
                for (int branch = 0; branch < 3; branch++) {
                    var column = new ColumnRefOperator(10 + branch, IntegerType.INT, "branch", true);
                    var raw = new ColumnRefOperator(20 + branch, IntegerType.INT, "raw", true);
                    branchColumns.add(List.of(column));
                    var child = new LogicalValuesOperator(List.of(projected ? raw : column));
                    ScalarOperator expression = new CallOperator("abs", IntegerType.INT, List.of(raw));
                    if (projected) {
                        child.setProjection(new Projection(Map.of(column, expression)));
                    }
                    var existing = new BinaryPredicateOperator(BinaryType.GE, projected ? raw : column,
                            ConstantOperator.createInt(branch));
                    child.setPredicate(existing);
                    originalPredicates.add(existing.clone());
                    projectionExpressions.add(expression.clone());
                    var pushed = new BinaryPredicateOperator(BinaryType.EQ, projected ? expression : column,
                            ConstantOperator.createInt(7));
                    expected.add(addFilter ? pushed : Utils.compoundAnd(existing, pushed));
                    children.add(OptExpression.create(child));
                }
                var union = new LogicalUnionOperator(List.of(output), branchColumns, true);
                if (projected) {
                    union.setProjection(new Projection(Map.of(alias, output)));
                }
                var predicate = new BinaryPredicateOperator(BinaryType.EQ, projected ? alias : output,
                        ConstantOperator.createInt(7));
                var snapshot = predicate.clone();
                var input = OptExpression.create(union, children);
                var result = PushDownPredicateSetRule.doProcess(new LogicalFilterOperator(predicate), input, addFilter);
                assertSame(input, result.get(0));
                assertEquals(snapshot, predicate);
                for (int branch = 0; branch < 3; branch++) {
                    var resultChild = input.inputAt(branch);
                    assertEquals(expected.get(branch), resultChild.getOp().getPredicate());
                    var originalChild = addFilter ? resultChild.inputAt(0) : resultChild;
                    if (addFilter) {
                        assertEquals(originalPredicates.get(branch), originalChild.getOp().getPredicate());
                    }
                    if (projected) {
                        assertEquals(projectionExpressions.get(branch), originalChild.getOp().getProjection()
                                .getColumnRefMap().get(branchColumns.get(branch).get(0)));
                    }
                }
            }
        }
    }
}
