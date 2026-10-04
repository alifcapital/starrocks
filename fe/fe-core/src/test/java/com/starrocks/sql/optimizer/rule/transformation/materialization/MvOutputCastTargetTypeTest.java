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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MvPartitionCompensator.adjustOptExpressionOutputColumnType casts the outputs of a compensation plan to the
 * types the materialized view expects. The new output column has the expected type, so the cast in the
 * projection must return the same type, and outputs that already match must keep their place.
 */
class MvOutputCastTargetTypeTest {
    @Test
    void charOutputIsCastToTheExpectedVarcharType() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator current = factory.create("base_char", TypeFactory.createCharType(20), false);
        ColumnRefOperator expected = factory.create("mv_varchar", TypeFactory.createVarcharType(20), false);
        OptExpression input = OptExpression.create(new LogicalValuesOperator(List.of(current)));

        Pair<OptExpression, List<ColumnRefOperator>> result = adjust(factory, input, List.of(current),
                List.of(expected));

        assertSame(input, result.first);
        ColumnRefOperator output = result.second.get(0);
        assertEquals(expected.getType(), output.getType());
        assertEquals(expected.isNullable(), output.isNullable());
        ScalarOperator expression = input.getOp().getProjection().getColumnRefMap().get(output);
        assertTrue(expression instanceof CastOperator);
        assertTrue(((CastOperator) expression).isImplicit());
        assertSame(current, expression.getChild(0));
        // The projection maps a VARCHAR column, so the cast must return VARCHAR and not the CHAR input type.
        assertEquals(output.getType(), expression.getType());
    }

    @Test
    void equalTypesKeepThePlanAndTheOutputList() {
        ColumnRefFactory factory = new ColumnRefFactory();
        Type type = TypeFactory.createVarcharType(20);
        ColumnRefOperator current = factory.create("current", type, true);
        ColumnRefOperator expected = factory.create("expected", type, true);
        List<ColumnRefOperator> outputs = List.of(current);
        OptExpression input = OptExpression.create(new LogicalValuesOperator(outputs));

        Pair<OptExpression, List<ColumnRefOperator>> result = adjust(factory, input, outputs, List.of(expected));

        assertSame(input, result.first);
        assertSame(outputs, result.second);
        assertNull(input.getOp().getProjection());
    }

    @Test
    void mixedOutputsKeepTheirOrder() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator character = factory.create("character", TypeFactory.createCharType(20), true);
        ColumnRefOperator varchar = factory.create("varchar", TypeFactory.createVarcharType(20), true);
        ColumnRefOperator expectedCharacter = factory.create("expected", varchar.getType(), true);
        List<ColumnRefOperator> currentOutputs = List.of(character, varchar);
        OptExpression input = OptExpression.create(new LogicalValuesOperator(currentOutputs));

        Pair<OptExpression, List<ColumnRefOperator>> result = adjust(factory, input, currentOutputs,
                List.of(expectedCharacter, varchar));

        assertSame(varchar, result.second.get(1));
        Map<ColumnRefOperator, ScalarOperator> projection = input.getOp().getProjection().getColumnRefMap();
        assertSame(varchar, projection.get(varchar));
        assertEquals(expectedCharacter.getType(), result.second.get(0).getType());
        assertEquals(result.second.get(0).getType(), projection.get(result.second.get(0)).getType());
    }

    @Test
    void castInTheMiddleKeepsMatchingOutputsBeforeAndAfterIt() {
        ColumnRefFactory factory = new ColumnRefFactory();
        Type charType = TypeFactory.createCharType(20);
        Type varcharType = TypeFactory.createVarcharType(20);
        ColumnRefOperator first = factory.create("first", varcharType, true);
        ColumnRefOperator second = factory.create("second", varcharType, true);
        ColumnRefOperator third = factory.create("third", charType, true);
        ColumnRefOperator fourth = factory.create("fourth", varcharType, true);
        List<ColumnRefOperator> currentOutputs = List.of(first, second, third, fourth);
        List<ColumnRefOperator> expectedOutputs = List.of(
                factory.create("e1", varcharType, true), factory.create("e2", varcharType, true),
                factory.create("e3", varcharType, false), factory.create("e4", varcharType, true));
        OptExpression input = OptExpression.create(new LogicalValuesOperator(currentOutputs));

        Pair<OptExpression, List<ColumnRefOperator>> result = adjust(factory, input, currentOutputs, expectedOutputs);

        // The outputs before the first cast must also be in the projection, or the plan loses them.
        assertEquals(4, result.second.size());
        assertSame(first, result.second.get(0));
        assertSame(second, result.second.get(1));
        assertEquals(varcharType, result.second.get(2).getType());
        assertFalse(result.second.get(2).isNullable());
        assertSame(fourth, result.second.get(3));
        Map<ColumnRefOperator, ScalarOperator> projection = input.getOp().getProjection().getColumnRefMap();
        assertEquals(4, projection.size());
        assertSame(first, projection.get(first));
        assertSame(second, projection.get(second));
        assertSame(fourth, projection.get(fourth));
        assertTrue(projection.get(result.second.get(2)) instanceof CastOperator);
        assertSame(third, projection.get(result.second.get(2)).getChild(0));
    }

    private static Pair<OptExpression, List<ColumnRefOperator>> adjust(ColumnRefFactory factory, OptExpression input,
                                                                       List<ColumnRefOperator> current,
                                                                       List<ColumnRefOperator> expected) {
        return MvPartitionCompensator.adjustOptExpressionOutputColumnType(factory, input, current, expected);
    }
}
