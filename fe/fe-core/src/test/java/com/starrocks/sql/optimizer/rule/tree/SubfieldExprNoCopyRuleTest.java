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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SubfieldExprNoCopyRule clears the copy flag of a subfield expression when no other expression of the same
 * projection reads an overlapping path of the same column. A cleared flag lets BE return the subfield column
 * without a copy, so the flag must stay set while another expression still reads an overlapping path.
 * Each shape lists the expected copy flags of its subfield expressions in creation order.
 */
class SubfieldExprNoCopyRuleTest {
    @Test
    void copyFlagIsClearedOnlyWithoutOverlappingUsage() {
        assertEquals(List.of(false, false), flagsOf("disjoint"));
        assertEquals(List.of(true), flagsOf("parent-used"));
        assertEquals(List.of(true, true), flagsOf("self-used"));
        assertEquals(List.of(false, false), flagsOf("common-sub"));
        assertEquals(List.of(false, false), flagsOf("other-column"));
        assertEquals(List.of(false), flagsOf("predicate-use"));
        assertEquals(List.of(), flagsOf("no-subfield"));
        assertEquals(List.of(false, false, false), flagsOf("three-way"));
        assertEquals(List.of(true, true), flagsOf("nested-child-used"));
    }

    private static List<Boolean> flagsOf(String shape) {
        Fixture fixture = new Fixture(shape);
        new SubfieldExprNoCopyRule().rewrite(fixture.root, null);
        return fixture.flags();
    }

    private static final class Fixture {
        final OptExpression root;
        final List<SubfieldOperator> subfields = new ArrayList<>();

        Fixture(String shape) {
            ColumnRefFactory factory = new ColumnRefFactory();
            StructType inner = new StructType(new ArrayList<>(List.of(new StructField("g", IntegerType.INT),
                    new StructField("h", IntegerType.INT))));
            StructType struct = new StructType(new ArrayList<>(List.of(new StructField("f0", inner),
                    new StructField("f1", IntegerType.INT))));
            ColumnRefOperator l = factory.create("l", struct, true);
            ColumnRefOperator m = factory.create("m", struct, true);
            ColumnRefOperator plain = factory.create("p", IntegerType.INT, true);
            ColumnRefOperator o1 = factory.create("o1", IntegerType.INT, true);
            ColumnRefOperator o2 = factory.create("o2", IntegerType.INT, true);
            ColumnRefOperator o3 = factory.create("o3", IntegerType.INT, true);

            Map<ColumnRefOperator, ScalarOperator> refMap = new LinkedHashMap<>();
            Map<ColumnRefOperator, ScalarOperator> commonSub = new LinkedHashMap<>();
            switch (shape) {
                case "disjoint" -> {
                    refMap.put(o1, sub(l, "f0", "g"));
                    refMap.put(o2, sub(l, "f1"));
                }
                case "parent-used" -> {
                    refMap.put(o1, sub(l, "f1"));
                    refMap.put(o2, l);
                }
                case "self-used" -> {
                    refMap.put(o1, sub(l, "f1"));
                    refMap.put(o2, sub(l, "f1"));
                }
                case "common-sub" -> {
                    refMap.put(o1, sub(l, "f0", "g"));
                    commonSub.put(o2, sub(l, "f1"));
                }
                case "other-column" -> {
                    refMap.put(o1, sub(l, "f1"));
                    refMap.put(o2, sub(m, "f1"));
                    refMap.put(o3, plain);
                }
                case "predicate-use" -> {
                    refMap.put(o1, sub(l, "f0", "g"));
                    refMap.put(o2, new BinaryPredicateOperator(BinaryType.EQ, subNoTrack(l, "f1"),
                            ConstantOperator.createInt(1)));
                }
                case "no-subfield" -> {
                    refMap.put(o1, plain);
                    refMap.put(o2, l);
                }
                case "three-way" -> {
                    refMap.put(o1, sub(l, "f0", "g"));
                    refMap.put(o2, sub(l, "f0", "h"));
                    commonSub.put(o3, sub(l, "f1"));
                }
                case "nested-child-used" -> {
                    refMap.put(o1, sub(l, "f0"));
                    refMap.put(o2, sub(l, "f0", "g"));
                }
                default -> throw new IllegalArgumentException(shape);
            }
            LogicalValuesOperator values = new LogicalValuesOperator(List.of(l, m, plain), List.of());
            values.setProjection(new Projection(refMap, commonSub));
            root = OptExpression.create(values);
        }

        private SubfieldOperator sub(ColumnRefOperator column, String... fields) {
            SubfieldOperator result = subNoTrack(column, fields);
            subfields.add(result);
            return result;
        }

        private static SubfieldOperator subNoTrack(ColumnRefOperator column, String... fields) {
            return new SubfieldOperator(column, IntegerType.INT, List.of(fields));
        }

        List<Boolean> flags() {
            List<Boolean> result = new ArrayList<>();
            for (SubfieldOperator subfield : subfields) {
                result.add(subfield.getCopyFlag());
            }
            return result;
        }
    }
}
