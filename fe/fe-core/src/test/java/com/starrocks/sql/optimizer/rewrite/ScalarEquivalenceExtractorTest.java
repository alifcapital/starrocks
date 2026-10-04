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


package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScalarEquivalenceExtractorTest {

    public static final ColumnRefOperator COLUMN_A = new ColumnRefOperator(1, IntegerType.INT, "a", true);
    public static final ColumnRefOperator COLUMN_B = new ColumnRefOperator(2, IntegerType.INT, "b", true);
    public static final ColumnRefOperator COLUMN_C = new ColumnRefOperator(3, IntegerType.INT, "c", true);
    public static final ConstantOperator CONSTANT_1 = ConstantOperator.createInt(1);
    public static final ConstantOperator CONSTANT_2 = ConstantOperator.createInt(2);
    public static final ConstantOperator CONSTANT_3 = ConstantOperator.createInt(3);

    @Test
    public void preservesBreadthFirstOrderAndSeesSubsequentUnions() {
        ColumnRefOperator d = new ColumnRefOperator(4, IntegerType.INT, "d", true);
        ColumnRefOperator e = new ColumnRefOperator(5, IntegerType.INT, "e", true);
        ColumnRefOperator f = new ColumnRefOperator(6, IntegerType.INT, "f", true);
        ScalarEquivalenceExtractor extractor = new ScalarEquivalenceExtractor();
        extractor.union(List.of(
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_B),
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_C),
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_B, d),
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_C, e),
                new BinaryPredicateOperator(BinaryType.EQ, d, f),
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_C, CONSTANT_1)));
        assertEquals(List.of(COLUMN_B, COLUMN_C, d, e, CONSTANT_1, f),
                new ArrayList<>(extractor.getEquivalentColumnRefs(COLUMN_A)));
        extractor.union(List.of(new BinaryPredicateOperator(BinaryType.EQ, e, CONSTANT_2)));
        assertEquals(List.of(COLUMN_B, COLUMN_C, d, e, CONSTANT_1, f, CONSTANT_2),
                new ArrayList<>(extractor.getEquivalentColumnRefs(COLUMN_A)));
    }

    @Test
    public void equivalentReplaceEQTransit() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_A, COLUMN_B);
        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_B, COLUMN_C);
        BinaryPredicateOperator bpo3 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_B, CONSTANT_2);
        BinaryPredicateOperator bpo4 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_C, CONSTANT_3);

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2, bpo3, bpo4));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);

        assertEquals(4, list.size());
        assertTrue(list.contains(
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, CONSTANT_2)));
        assertTrue(list.contains(
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, CONSTANT_3)));
        assertTrue(list.contains(
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_B)));
        assertTrue(list.contains(
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_C)));
    }

    @Test
    public void equivalentReplaceLTTransit() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_A, COLUMN_B);
        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_B, COLUMN_C);
        BinaryPredicateOperator bpo3 = new BinaryPredicateOperator(BinaryType.LT,
                COLUMN_B, CONSTANT_1);

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2, bpo3));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);

        assertEquals(3, list.size());
        assertTrue(list.contains(
                new BinaryPredicateOperator(BinaryType.LT, COLUMN_A, CONSTANT_1)));
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_B)));
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_C)));
    }

    @Test
    public void equivalentReplaceLTFunctionTransit() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_A, COLUMN_B);
        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryType.LT,
                COLUMN_B, new CallOperator("abs", IntegerType.INT, Lists.newArrayList(ConstantOperator.createInt(2))));

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);

        assertEquals(2, list.size());
        assertTrue(list.contains(
                new BinaryPredicateOperator(BinaryType.LT, COLUMN_A,
                        new CallOperator("abs", IntegerType.INT, Lists.newArrayList(ConstantOperator.createInt(2))))));
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_B)));
    }

    @Test
    public void equivalentReplaceLTFunctionRefTransit() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_A, COLUMN_B);
        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryType.LT,
                COLUMN_B, new CallOperator("abs", IntegerType.INT, Lists.newArrayList(COLUMN_C)));

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);

        assertEquals(1, list.size());
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_B)));
    }

    @Test
    public void equivalentReplaceEQFunctionNest() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_A, new CallOperator("abs", IntegerType.INT, Lists.newArrayList(COLUMN_C)));
        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_C, new CallOperator("abs", IntegerType.INT, Lists.newArrayList(CONSTANT_2)));

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);

        assertEquals(1, list.size());
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A,
                        new CallOperator("abs", IntegerType.INT, Lists.newArrayList(COLUMN_C)))));
    }

    @Test
    public void equivalentReplaceEQFunctionTransit() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_A, new CallOperator("abs", IntegerType.INT, Lists.newArrayList(COLUMN_C)));
        BinaryPredicateOperator bpo2 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_C, CONSTANT_2);

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);
        System.out.println(list);

        assertEquals(2, list.size());
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A,
                        new CallOperator("abs", IntegerType.INT, Lists.newArrayList(COLUMN_C)))));
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A,
                        new CallOperator("abs", IntegerType.INT, Lists.newArrayList(CONSTANT_2)))));
    }

    @Test
    public void equivalentReplaceInTransit() {
        BinaryPredicateOperator bpo1 =
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_C);
        InPredicateOperator bpo2 = new InPredicateOperator(COLUMN_C, CONSTANT_2, CONSTANT_1);

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);

        assertEquals(2, list.size());
        assertTrue(
                list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_C)));
        assertTrue(
                list.contains(new InPredicateOperator(COLUMN_A, CONSTANT_2, CONSTANT_1)));
    }

    @Test
    public void equivalentReplaceInFunctionTransit() {
        BinaryPredicateOperator bpo1 = new BinaryPredicateOperator(BinaryType.EQ,
                COLUMN_A, new CallOperator("abs", IntegerType.INT, Lists.newArrayList(COLUMN_C)));
        InPredicateOperator bpo2 = new InPredicateOperator(COLUMN_C, CONSTANT_2, CONSTANT_1);

        ScalarEquivalenceExtractor equivalence = new ScalarEquivalenceExtractor();

        equivalence.union(Lists.newArrayList(bpo1, bpo2));

        Set<ScalarOperator> list = equivalence.getEquivalentScalar(COLUMN_A);

        assertEquals(1, list.size());
        assertTrue(list.contains(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A,
                new CallOperator("abs", IntegerType.INT, Lists.newArrayList(COLUMN_C)))));
    }

    @Test
    public void disconnectedAndOwnValueOnlyResultsStayFreshAndMutable() {
        ScalarEquivalenceExtractor extractor = new ScalarEquivalenceExtractor();
        extractor.union(List.of(new BinaryPredicateOperator(BinaryType.LT, COLUMN_A, CONSTANT_1)));
        for (ColumnRefOperator column : List.of(COLUMN_A, COLUMN_B)) {
            Set<ScalarOperator> scalars = extractor.getEquivalentScalar(column);
            Set<ScalarOperator> references = extractor.getEquivalentColumnRefs(column);
            assertTrue(scalars.isEmpty());
            assertTrue(references.isEmpty());
            scalars.add(CONSTANT_2);
            references.add(COLUMN_C);
            assertTrue(extractor.getEquivalentScalar(column).isEmpty());
            assertTrue(extractor.getEquivalentColumnRefs(column).isEmpty());
        }
    }

    @Test
    public void incrementalUnionAfterEmptySearchDerivesOrderedIndependentResults() {
        ScalarEquivalenceExtractor extractor = new ScalarEquivalenceExtractor();
        BinaryPredicateOperator bound = new BinaryPredicateOperator(BinaryType.LT, COLUMN_B, CONSTANT_3);
        ScalarOperator original = bound.clone();
        extractor.union(List.of(bound));
        assertTrue(extractor.getEquivalentScalar(COLUMN_A).isEmpty());
        extractor.union(List.of(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_B),
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_B, COLUMN_C)));
        assertEquals(List.of(COLUMN_B, COLUMN_C), new ArrayList<>(extractor.getEquivalentColumnRefs(COLUMN_A)));
        assertEquals(List.of(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_B),
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_C),
                new BinaryPredicateOperator(BinaryType.LT, COLUMN_A, CONSTANT_3)),
                new ArrayList<>(extractor.getEquivalentScalar(COLUMN_A)));
        assertEquals(original, bound);
        Set<ScalarOperator> result = extractor.getEquivalentScalar(COLUMN_A);
        result.clear();
        assertEquals(3, extractor.getEquivalentScalar(COLUMN_A).size());
        extractor.union(List.of(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_C, CONSTANT_2)));
        assertTrue(extractor.getEquivalentScalar(COLUMN_A).contains(
                new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, CONSTANT_2)));
    }

    @Test
    public void selfEdgesConstantsAndRejectedMultiColumnFunctionsKeepTheirSemantics() {
        ScalarEquivalenceExtractor extractor = new ScalarEquivalenceExtractor();
        extractor.union(List.of(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_A),
                new BinaryPredicateOperator(BinaryType.LT, COLUMN_A, CONSTANT_3)));
        assertTrue(extractor.getEquivalentColumnRefs(COLUMN_A).isEmpty());
        assertEquals(Set.of(new BinaryPredicateOperator(BinaryType.LT, COLUMN_A, CONSTANT_3)),
                extractor.getEquivalentScalar(COLUMN_A));

        ScalarEquivalenceExtractor constants = new ScalarEquivalenceExtractor();
        constants.union(List.of(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_B, CONSTANT_2)));
        assertEquals(Set.of(CONSTANT_2), constants.getEquivalentColumnRefs(COLUMN_B));
        assertEquals(Set.of(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_B, CONSTANT_2)),
                constants.getEquivalentScalar(COLUMN_B));

        ScalarEquivalenceExtractor rejected = new ScalarEquivalenceExtractor();
        rejected.union(List.of(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A,
                new CallOperator("add", IntegerType.INT, List.of(COLUMN_B, COLUMN_C)))));
        assertTrue(rejected.getEquivalentScalar(COLUMN_A).isEmpty());
        assertTrue(rejected.getEquivalentColumnRefs(COLUMN_A).isEmpty());
        rejected.union(List.of(new BinaryPredicateOperator(BinaryType.EQ, COLUMN_A, COLUMN_B)));
        assertEquals(Set.of(COLUMN_B), rejected.getEquivalentColumnRefs(COLUMN_A));
    }

    @Test
    public void valueAddedAfterColumnOnlySearchIsRewrittenAndRemainsIndependent() {
        ScalarEquivalenceExtractor extractor = new ScalarEquivalenceExtractor();
        ScalarOperator ab = BinaryPredicateOperator.eq(COLUMN_A, COLUMN_B);
        ScalarOperator bc = BinaryPredicateOperator.eq(COLUMN_B, COLUMN_C);
        extractor.union(List.of(ab, bc));
        List<ScalarOperator> references = List.of(ab, BinaryPredicateOperator.eq(COLUMN_A, COLUMN_C));
        assertEquals(references, new ArrayList<>(extractor.getEquivalentScalar(COLUMN_A)));
        BinaryPredicateOperator bound = new BinaryPredicateOperator(BinaryType.LT, COLUMN_C, CONSTANT_3);
        extractor.union(List.of(bound));
        List<ScalarOperator> expected = new ArrayList<>(references);
        expected.add(new BinaryPredicateOperator(BinaryType.LT, COLUMN_A, CONSTANT_3));
        List<ScalarOperator> result = new ArrayList<>(extractor.getEquivalentScalar(COLUMN_A));
        assertEquals(expected, result);
        result.get(2).setChild(1, CONSTANT_1);
        assertEquals(CONSTANT_3, bound.getChild(1));
        assertEquals(expected, new ArrayList<>(extractor.getEquivalentScalar(COLUMN_A)));
    }

}