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

import com.starrocks.sql.optimizer.Utils;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType.AND;
import static com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType.NOT;
import static com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType.OR;

// The memo deduplicates expressions through equals, so two AND (or OR) trees must be equal exactly when
// their flattened children, stably sorted by hash code, are equal. We check equals against a direct
// implementation of that definition.
public class CompoundNormalizationContractTest {
    private static List<ScalarOperator> normalize(CompoundPredicateOperator operator) {
        switch (operator.getCompoundType()) {
            case AND:
                return Utils.extractConjuncts(operator).stream()
                        .sorted(Comparator.comparingInt(ScalarOperator::hashCode)).collect(Collectors.toList());
            case OR:
                return Utils.extractDisjunctive(operator).stream()
                        .sorted(Comparator.comparingInt(ScalarOperator::hashCode)).collect(Collectors.toList());
            default:
                return new ArrayList<>(operator.getChildren());
        }
    }

    private static boolean expectedEquals(CompoundPredicateOperator left, CompoundPredicateOperator right) {
        if (left == right) {
            return true;
        }
        if (left.getClass() != right.getClass() || left.getCompoundType() != right.getCompoundType()) {
            return false;
        }
        List<ScalarOperator> leftChildren = normalize(left);
        List<ScalarOperator> rightChildren = normalize(right);
        if (leftChildren.size() != rightChildren.size()) {
            return false;
        }
        for (int i = 0; i < leftChildren.size(); i++) {
            ScalarOperator a = leftChildren.get(i);
            ScalarOperator b = rightChildren.get(i);
            if (a == b) {
                continue;
            }
            boolean equal = a instanceof CompoundPredicateOperator compoundA
                    && b instanceof CompoundPredicateOperator compoundB
                    ? expectedEquals(compoundA, compoundB) : a != null && a.equals(b);
            if (!equal) {
                return false;
            }
        }
        return true;
    }

    private static CompoundPredicateOperator compound(CompoundPredicateOperator.CompoundType type,
                                                       ScalarOperator... children) {
        return new CompoundPredicateOperator(type, children);
    }

    private static ColumnRefOperator column(int id) {
        return new ColumnRefOperator(id, IntegerType.INT, "c", true);
    }

    @Test
    public void equalsFollowsNormalizedChildrenForNestedTreesAndHashCollisions() {
        // "Aa" and "BB" have the same hash code, so their relative order decides equality.
        ScalarOperator a = ConstantOperator.createVarchar("Aa");
        ScalarOperator b = ConstantOperator.createVarchar("BB");
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        List<CompoundPredicateOperator> trees = new ArrayList<>();
        for (CompoundPredicateOperator.CompoundType type : Arrays.asList(AND, OR)) {
            trees.add(compound(type, a, b));
            trees.add(compound(type, b, a));
            trees.add(compound(type, a, a));
            trees.add(compound(type, compound(type, a, column(1)), b));
            trees.add(compound(type, a, compound(type, column(1), b)));
            trees.add(compound(type, compound(type == AND ? OR : AND, a, b), column(1)));
            trees.add(compound(type, a, b, column(Integer.MIN_VALUE)));
            trees.add(compound(type, column(Integer.MIN_VALUE), column(Integer.MAX_VALUE)));
            trees.add(compound(type, compound(NOT, a), compound(NOT, b)));
        }
        trees.add(compound(NOT, a));
        trees.add(compound(NOT, a, b));
        trees.add(compound(NOT, b, a));
        for (CompoundPredicateOperator left : trees) {
            for (CompoundPredicateOperator right : trees) {
                Assertions.assertEquals(expectedEquals(left, right), left.equals(right));
            }
        }
        // Children with equal hash codes keep their encounter order, so equality is not a multiset comparison.
        Assertions.assertNotEquals(compound(AND, a, b), compound(AND, b, a));
    }

    @Test
    public void equalsReflectsListAndChildMutations() {
        // Rules change children in place, so equals must read the current children every time.
        List<ScalarOperator> children = new ArrayList<>(Arrays.asList(column(1), column(2)));
        CompoundPredicateOperator left = new CompoundPredicateOperator(AND, children);
        CompoundPredicateOperator right = compound(AND, column(1), column(2));
        Assertions.assertEquals(left, right);
        children.set(0, column(3));
        Assertions.assertNotEquals(left, right);
        left.setChild(0, column(1));
        Assertions.assertEquals(left, right);
        children.add(column(4));
        Assertions.assertEquals(expectedEquals(left, right), left.equals(right));
    }
}
