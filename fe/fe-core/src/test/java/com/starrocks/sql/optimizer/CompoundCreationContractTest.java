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

package com.starrocks.sql.optimizer;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType.AND;
import static com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType.OR;

// Utils.createCompound builds a balanced tree: it pairs neighbours level by level and carries an odd last node
// to the next level. The tree shape is visible in EXPLAIN and in memo keys, so we check exact shapes.
public class CompoundCreationContractTest {
    private static ColumnRefOperator column(int id) {
        return new ColumnRefOperator(id, IntegerType.INT, "c" + id, true);
    }

    private static List<ScalarOperator> columns(int count) {
        List<ScalarOperator> result = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            result.add(column(i));
        }
        return result;
    }

    private static String shape(ScalarOperator operator) {
        if (operator == null) {
            return "null";
        }
        if (operator instanceof ColumnRefOperator column) {
            return column.getName();
        }
        if (operator instanceof CompoundPredicateOperator compound) {
            Assertions.assertEquals(2, compound.getChildren().size());
            String op = compound.getCompoundType() == AND ? " & " : " | ";
            return "(" + shape(compound.getChild(0)) + op + shape(compound.getChild(1)) + ")";
        }
        return operator.toString();
    }

    @Test
    public void pairsNeighboursAndCarriesTheOddNodeOnEveryLevel() {
        List<String> expected = Arrays.asList(
                "c1",
                "(c1 & c2)",
                "((c1 & c2) & c3)",
                "((c1 & c2) & (c3 & c4))",
                "(((c1 & c2) & (c3 & c4)) & c5)",
                "(((c1 & c2) & (c3 & c4)) & (c5 & c6))",
                "(((c1 & c2) & (c3 & c4)) & ((c5 & c6) & c7))",
                "(((c1 & c2) & (c3 & c4)) & ((c5 & c6) & (c7 & c8)))",
                "((((c1 & c2) & (c3 & c4)) & ((c5 & c6) & (c7 & c8))) & c9)");
        for (int size = 1; size <= expected.size(); size++) {
            List<ScalarOperator> input = columns(size);
            List<ScalarOperator> snapshot = new ArrayList<>(input);
            Assertions.assertEquals(expected.get(size - 1), shape(Utils.createCompound(AND, input)));
            Assertions.assertEquals(expected.get(size - 1).replace('&', '|'), shape(Utils.createCompound(OR, input)));
            // The caller's list is not changed.
            Assertions.assertEquals(snapshot, input);
        }
    }

    @Test
    public void largeInputsGiveABalancedTreeWithEveryLeafInOrder() {
        for (int size : new int[] {129, 1025}) {
            ScalarOperator result = Utils.createCompound(OR, columns(size));
            // A balanced binary tree over n leaves has height ceil(log2(n)).
            Assertions.assertEquals(32 - Integer.numberOfLeadingZeros(size - 1), height(result));
            List<ScalarOperator> leaves = new ArrayList<>();
            collectLeaves(result, leaves);
            Assertions.assertEquals(columns(size), leaves);
        }
    }

    private static int height(ScalarOperator operator) {
        if (operator instanceof CompoundPredicateOperator) {
            return 1 + Math.max(height(operator.getChild(0)), height(operator.getChild(1)));
        }
        return 0;
    }

    private static void collectLeaves(ScalarOperator operator, List<ScalarOperator> leaves) {
        if (operator instanceof CompoundPredicateOperator) {
            collectLeaves(operator.getChild(0), leaves);
            collectLeaves(operator.getChild(1), leaves);
        } else {
            leaves.add(operator);
        }
    }

    @Test
    public void nullsAreSkippedAndAndDropsTrue() {
        ScalarOperator leaf = column(1);
        ScalarOperator nested = new CompoundPredicateOperator(OR, leaf, ConstantOperator.FALSE);
        for (CompoundPredicateOperator.CompoundType type : Arrays.asList(AND, OR)) {
            Assertions.assertNull(Utils.createCompound(type, Collections.emptyList()));
            Assertions.assertNull(Utils.createCompound(type, Arrays.asList(null, null)));
            Assertions.assertSame(leaf, Utils.createCompound(type, Arrays.asList(null, leaf, null)));
        }
        // An AND of TRUE values is TRUE, and TRUE does not change an AND of other values.
        Assertions.assertSame(ConstantOperator.TRUE,
                Utils.createCompound(AND, Arrays.asList(ConstantOperator.TRUE, null, ConstantOperator.createBoolean(true))));
        Assertions.assertSame(leaf,
                Utils.createCompound(AND, Arrays.asList(ConstantOperator.TRUE, leaf, ConstantOperator.TRUE)));
        List<ScalarOperator> mixed = Arrays.asList(ConstantOperator.FALSE, leaf, ConstantOperator.TRUE, nested, null, leaf);
        Assertions.assertEquals("((false & c1) & ((c1 | false) & c1))",
                shape(Utils.createCompound(AND, mixed)));
        // OR keeps TRUE as an ordinary operand.
        Assertions.assertEquals("(((false | c1) | (true | (c1 | false))) | c1)",
                shape(Utils.createCompound(OR, mixed)));
        Assertions.assertEquals("(c1 & c2)",
                shape(Utils.createCompound(AND, Collections.unmodifiableList(columns(2)))));
    }
}
