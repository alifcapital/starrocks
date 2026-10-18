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
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnionToValuesRuleTest {
    private static ColumnRefOperator column(int id) {
        return new ColumnRefOperator(id, IntegerType.INT, "c" + id, true);
    }

    private static List<ScalarOperator> row(int first, int second) {
        return List.of(ConstantOperator.createInt(first), ConstantOperator.createInt(second));
    }

    private static OptExpression values(List<ColumnRefOperator> columns, List<ScalarOperator> row) {
        return OptExpression.create(new LogicalValuesOperator(columns, List.of(row)));
    }

    // A Values with one NULL row whose projection computes constants, the other form of a mergeable child.
    private static OptExpression constantUnionChild(ColumnRefOperator valuesColumn, ColumnRefOperator projected) {
        LogicalValuesOperator child = new LogicalValuesOperator.Builder()
                .setColumnRefSet(List.of(valuesColumn))
                .setRows(List.of(List.of(ConstantOperator.createNull(IntegerType.INT))))
                .setProjection(new Projection(Map.of(projected, ConstantOperator.createInt(7))))
                .build();
        return OptExpression.create(child);
    }

    private static OptExpression union(List<List<ColumnRefOperator>> childOutputs, OptExpression... children) {
        LogicalUnionOperator union = new LogicalUnionOperator.Builder()
                .setOutputColumnRefOp(List.of(column(100), column(101)))
                .setChildOutputColumns(childOutputs)
                .isUnionAll(true)
                .build();
        return OptExpression.create(union, children);
    }

    @Test
    public void testMergedRowsFollowChildOutput() {
        ColumnRefOperator a = column(1);
        ColumnRefOperator b = column(2);
        ColumnRefOperator c = column(3);
        ColumnRefOperator d = column(4);
        OptExpression input = union(List.of(List.of(a, b), List.of(d, c)),
                values(List.of(a, b), row(1, 2)), values(List.of(c, d), row(3, 4)));

        UnionToValuesRule rule = UnionToValuesRule.getInstance();
        assertTrue(rule.check(input, null));
        List<OptExpression> output = rule.transform(input, OptimizerFactory.mockContext(new ColumnRefFactory()));

        LogicalValuesOperator merged = assertInstanceOf(LogicalValuesOperator.class, output.get(0).getOp());
        assertEquals(List.of(row(1, 2), row(4, 3)), merged.getRows());
    }

    @Test
    public void testChildWithoutReadColumnStaysInUnion() {
        ColumnRefOperator a = column(1);
        ColumnRefOperator b = column(2);
        ColumnRefOperator c = column(3);
        ColumnRefOperator d = column(4);
        ColumnRefOperator e = column(5);
        ColumnRefOperator f = column(6);
        ColumnRefOperator missing = column(7);
        // The third child reads a column its Values does not have, so only the first two children are merged.
        OptExpression input = union(List.of(List.of(a, b), List.of(d, c), List.of(e, missing)),
                values(List.of(a, b), row(1, 2)), values(List.of(c, d), row(3, 4)),
                values(List.of(e, f), row(5, 6)));

        UnionToValuesRule rule = UnionToValuesRule.getInstance();
        assertTrue(rule.check(input, null));
        List<OptExpression> output = rule.transform(input, OptimizerFactory.mockContext(new ColumnRefFactory()));

        OptExpression result = output.get(0);
        assertEquals(2, result.getInputs().size());
        assertSame(input.getInputs().get(2), result.getInputs().get(0));
        LogicalValuesOperator merged = assertInstanceOf(LogicalValuesOperator.class, result.getInputs().get(1).getOp());
        assertEquals(List.of(row(1, 2), row(4, 3)), merged.getRows());
    }

    @Test
    public void testNoMergeWhenOnlyOneChildReadsItsColumns() {
        ColumnRefOperator a = column(1);
        ColumnRefOperator b = column(2);
        ColumnRefOperator c = column(3);
        ColumnRefOperator d = column(4);
        ColumnRefOperator missing = column(5);
        UnionToValuesRule rule = UnionToValuesRule.getInstance();

        OptExpression twoValues = union(List.of(List.of(a, b), List.of(c, missing)),
                values(List.of(a, b), row(1, 2)), values(List.of(c, d), row(3, 4)));
        assertFalse(rule.check(twoValues, null));

        // The projection of the second child does not produce the column that the union reads.
        OptExpression withConstantUnion = union(List.of(List.of(a, b), List.of(missing, missing)),
                values(List.of(a, b), row(1, 2)), constantUnionChild(c, d));
        assertFalse(rule.check(withConstantUnion, null));

        OptExpression withConstantUnionRead = union(List.of(List.of(a, b), List.of(d, d)),
                values(List.of(a, b), row(1, 2)), constantUnionChild(c, d));
        assertTrue(rule.check(withConstantUnionRead, null));
    }
}
