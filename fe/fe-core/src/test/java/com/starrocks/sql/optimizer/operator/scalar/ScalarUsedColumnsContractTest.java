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

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class ScalarUsedColumnsContractTest {
    private static ColumnRefOperator column(int id) {
        return new ColumnRefOperator(id, IntegerType.INT, "c" + id, true);
    }

    @Test
    public void collectAddsToDestinationAndResultsFollowMutations() {
        ColumnRefOperator first = column(1);
        ColumnRefOperator second = column(2);
        CallOperator child = new CallOperator("f", IntegerType.INT, Collections.singletonList(first));
        BinaryPredicateOperator root = new BinaryPredicateOperator(BinaryType.EQ, child, second);

        // collectUsedColumns adds to the destination and keeps the columns already there.
        ColumnRefSet destination = new ColumnRefSet(99);
        root.collectUsedColumns(destination);
        Assertions.assertEquals(ColumnRefSet.of(first, second, column(99)), destination);

        // Rules change children in place, so getUsedColumns must see the change. A result returned earlier
        // belongs to the caller: it keeps its value, and clearing it does not affect the operator.
        ColumnRefSet earlierResult = root.getUsedColumns();
        child.setChild(0, column(3));
        Assertions.assertEquals(ColumnRefSet.of(column(3), second), root.getUsedColumns());
        Assertions.assertEquals(ColumnRefSet.of(first, second), earlierResult);
        earlierResult.clear();
        Assertions.assertEquals(ColumnRefSet.of(column(3), second), root.getUsedColumns());
    }

    @Test
    public void lambdaBindingRemovalIsLocalToItsScope() {
        // A lambda argument is not a column of the input. We expect it to be removed only inside its lambda,
        // so the same column used outside the lambda is still reported.
        ColumnRefOperator bound = column(10);
        ColumnRefOperator outer = column(11);
        ColumnRefOperator capture = column(12);
        ColumnRefOperator argument = new ColumnRefOperator(13, IntegerType.INT, "arg", true, true);
        CallOperator body = new CallOperator("f", IntegerType.INT, Arrays.asList(bound, outer, argument));
        LambdaFunctionOperator lambda = new LambdaFunctionOperator(
                Collections.singletonList(argument), body, IntegerType.INT);
        lambda.addColumnToExpr(Collections.singletonMap(bound, capture));
        Assertions.assertEquals(ColumnRefSet.of(outer, capture), lambda.getUsedColumns());
        for (boolean lambdaFirst : new boolean[] {false, true}) {
            CallOperator parent = new CallOperator("f", IntegerType.INT,
                    lambdaFirst ? Arrays.asList(lambda, bound) : Arrays.asList(bound, lambda));
            Assertions.assertEquals(ColumnRefSet.of(bound, outer, capture), parent.getUsedColumns());
        }
        LambdaFunctionOperator nested = new LambdaFunctionOperator(
                Collections.singletonList(argument), lambda, IntegerType.INT);
        nested.addColumnToExpr(Collections.singletonMap(outer, column(14)));
        Assertions.assertEquals(ColumnRefSet.of(capture, column(14)), nested.getUsedColumns());
        Assertions.assertTrue(argument.getUsedColumns().isEmpty());
    }

    @Test
    public void dictionaryMappingUsesOnlyDictionaryColumn() {
        // A dict mapping reads only the dictionary column. The string expressions inside it are not evaluated.
        ColumnRefOperator dictionary = column(20);
        DictMappingOperator mapping = new DictMappingOperator(
                IntegerType.INT, dictionary, column(21), column(22));
        CallOperator parent = new CallOperator("f", IntegerType.INT,
                Arrays.asList(mapping, ConstantOperator.createInt(1)));
        Assertions.assertEquals(ColumnRefSet.of(dictionary), parent.getUsedColumns());
        Assertions.assertEquals(ColumnRefSet.of(dictionary), new CloneOperator(mapping).getUsedColumns());
    }
}
