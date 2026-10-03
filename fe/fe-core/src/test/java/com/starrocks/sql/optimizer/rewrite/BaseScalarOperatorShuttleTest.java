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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArraySliceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CloneOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictionaryGetOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MapOperator;
import com.starrocks.sql.optimizer.operator.scalar.MultiInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.rewrite.scalar.NegateFilterShuttle;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.starrocks.type.ArrayType.ARRAY_TINYINT;
import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.IntegerType.TINYINT;
import static com.starrocks.type.StringType.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class BaseScalarOperatorShuttleTest {

    private final BaseScalarOperatorShuttle shuttle = new BaseScalarOperatorShuttle();

    private final BaseScalarOperatorShuttle shuttle2 = new BaseScalarOperatorShuttle() {
        @Override
        public Optional<ScalarOperator> preprocess(ScalarOperator scalarOperator) {
            return Optional.of(scalarOperator);
        }
    };

    @Test
    void visitArray() {
        ArrayOperator operator = new ArrayOperator(ARRAY_TINYINT, true, Lists.newArrayList(ConstantOperator.createInt(3)));
        {
            ScalarOperator newOperator = shuttle.visitArray(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitArray(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitCollectionElement() {
        ArrayOperator arrayOperator = new ArrayOperator(ARRAY_TINYINT, true, Lists.newArrayList(ConstantOperator.createInt(3)));
        CollectionElementOperator operator =
                new CollectionElementOperator(STRING, arrayOperator, ConstantOperator.createInt(0), false);
        {
            ScalarOperator newOperator = shuttle.visitCollectionElement(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitCollectionElement(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitArraySlice() {
        ArrayOperator arrayOperator = new ArrayOperator(ARRAY_TINYINT, true,
                Lists.newArrayList(ConstantOperator.createInt(3), ConstantOperator.createInt(10)));
        ConstantOperator offset = ConstantOperator.createInt(0);
        ConstantOperator length = ConstantOperator.createInt(1);
        ArraySliceOperator operator = new ArraySliceOperator(TINYINT, Lists.newArrayList(arrayOperator, offset, length));
        {
            ScalarOperator newOperator = shuttle.visitArraySlice(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitArraySlice(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitBetweenPredicate() {
        BetweenPredicateOperator operator = new BetweenPredicateOperator(true,
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.createInt(1), ConstantOperator.createInt(10));
        {
            ScalarOperator newOperator = shuttle.visitBetweenPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitBetweenPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
    }


    @Test
    void visitExistsPredicate() {
        ExistsPredicateOperator operator = new ExistsPredicateOperator(true, ImmutableList.of());
        {
            ScalarOperator newOperator = shuttle.visitExistsPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitExistsPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitInPredicate() {
        InPredicateOperator operator = new InPredicateOperator(true, ImmutableList.of());
        {
            ScalarOperator newOperator = shuttle.visitInPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitInPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitIsNullPredicate() {
        IsNullPredicateOperator operator = new IsNullPredicateOperator(true, new ColumnRefOperator(1, INT, "id", true));
        {
            ScalarOperator newOperator = shuttle.visitIsNullPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitIsNullPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitLikePredicateOperator() {
        LikePredicateOperator operator = new LikePredicateOperator(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.TRUE);
        {
            ScalarOperator newOperator = shuttle.visitLikePredicateOperator(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitLikePredicateOperator(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitCastOperator() {
        CastOperator operator = new CastOperator(INT, new ColumnRefOperator(1, INT, "id", true));
        {
            ScalarOperator newOperator = shuttle.visitCastOperator(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitCastOperator(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void visitCaseWhenOperator() {
        CaseWhenOperator operator = new CaseWhenOperator(INT, null, null, ImmutableList.of());
        {
            ScalarOperator newOperator = shuttle.visitCaseWhenOperator(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitCaseWhenOperator(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void testSubfieldOperator() {
        ColumnRefOperator column1 = new ColumnRefOperator(1, INT, "id", true);
        SubfieldOperator operator = new SubfieldOperator(column1, INT, Lists.newArrayList("a"));
        {
            ScalarOperator newOperator = shuttle.visitSubfield(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitSubfield(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void testMapOperator() {
        ColumnRefOperator column1 = new ColumnRefOperator(1, INT, "id", true);
        MapOperator operator = new MapOperator(INT, Lists.newArrayList(column1, column1));
        {
            ScalarOperator newOperator = shuttle.visitMap(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitMap(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void testMultiInPredicateOperator() {
        ColumnRefOperator column1 = new ColumnRefOperator(1, INT, "id", true);
        MultiInPredicateOperator operator = new MultiInPredicateOperator(false,
                Lists.newArrayList(column1, column1), Lists.newArrayList(column1, column1));
        {
            ScalarOperator newOperator = shuttle.visitMultiInPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitMultiInPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void testCallOperator() {
        CallOperator operator = new CallOperator("count", INT, Lists.newArrayList());
        {
            ScalarOperator newOperator = shuttle.visitCall(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitCall(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void testBinaryOperator() {
        BinaryPredicateOperator operator = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, INT, "id", true), ConstantOperator.createInt(1));
        {
            ScalarOperator newOperator = shuttle.visitBinaryPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitBinaryPredicate(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void testCompoundPredicate() {
        BinaryPredicateOperator binary1 = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, INT, "id", true), ConstantOperator.createInt(1));
        BinaryPredicateOperator binary2 = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(2, INT, "id2", true), ConstantOperator.createInt(1));
        CompoundPredicateOperator compound =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, binary1, binary2);
        {
            ScalarOperator newOperator = shuttle.visitCompoundPredicate(compound, null);
            assertEquals(compound, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitCompoundPredicate(compound, null);
            assertEquals(compound, newOperator);
        }
    }

    @Test
    void testLambdaFunctionOperator() {
        BinaryPredicateOperator binary1 = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, INT, "id", true), ConstantOperator.createInt(1));
        LambdaFunctionOperator lambda =
                new LambdaFunctionOperator(Lists.newArrayList(new ColumnRefOperator(1, INT, "id", true)), binary1, INT);
        {
            ScalarOperator newOperator = shuttle.visitLambdaFunctionOperator(lambda, null);
            assertEquals(lambda, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitLambdaFunctionOperator(lambda, null);
            assertEquals(lambda, newOperator);
        }
    }

    @Test
    void testCloneOperator() {
        BinaryPredicateOperator binary1 = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, INT, "id", true), ConstantOperator.createInt(1));
        CloneOperator clone = new CloneOperator(binary1);
        {
            ScalarOperator newOperator = shuttle.visitCloneOperator(clone, null);
            assertEquals(clone, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitCloneOperator(clone, null);
            assertEquals(clone, newOperator);
        }
    }

    @Test
    void visitCaseWhenOperator_1() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(1, IntegerType.INT, "", true);
        BinaryPredicateOperator whenOperator1 =
                new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                        ConstantOperator.createInt(1));
        ConstantOperator constantOperator1 = ConstantOperator.createChar("1");
        BinaryPredicateOperator whenOperator2 =
                new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                        ConstantOperator.createInt(2));
        ConstantOperator constantOperator2 = ConstantOperator.createChar("2");

        CaseWhenOperator operator =
                new CaseWhenOperator(VarcharType.VARCHAR, null, ConstantOperator.createChar("others", VarcharType.VARCHAR),
                        ImmutableList.of(whenOperator1, constantOperator1, whenOperator2, constantOperator2));

        CaseWhenOperator otherOperator =
                new CaseWhenOperator(VarcharType.VARCHAR, null, null,
                        ImmutableList.of(whenOperator1, constantOperator1, whenOperator2, constantOperator2));

        BaseScalarOperatorShuttle testShuttle = new BaseScalarOperatorShuttle() {
            @Override
            public ScalarOperator visitConstant(ConstantOperator literal, Void context) {
                return ConstantOperator.createChar("3");
            }
        };
        ScalarOperator newOperator = testShuttle.visitCaseWhenOperator(operator, null);
        assertNotEquals(operator, newOperator);
        newOperator = testShuttle.visitCaseWhenOperator(otherOperator, null);
        assertNotEquals(otherOperator, newOperator);

    }

    @ParameterizedTest(name = "{index}: {0}.")
    @MethodSource("inPredicateCases")
    void testNegateInPredicate(ScalarOperator operator, String expected) {
        NegateFilterShuttle shuttle1 = NegateFilterShuttle.getInstance();
        ScalarOperator reuslt = shuttle1.negateFilter(operator);
        assertEquals(expected, reuslt.toString());
    }

    private static Stream<Arguments> inPredicateCases() {
        List<Arguments> argumentsList = Lists.newArrayList();
        // not in constant values without null
        InPredicateOperator operator = new InPredicateOperator(true, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", false),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2)));

        argumentsList.add(Arguments.of(operator, "1: id IN (1, 2)"));

        operator = new InPredicateOperator(true, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2)));

        argumentsList.add(Arguments.of(operator, "1: id IN (1, 2) OR 1: id IS NULL"));

        // not in constant values with null
        operator = new InPredicateOperator(true, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", false),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                ConstantOperator.NULL));
        argumentsList.add(Arguments.of(operator, "1: id IN (1, 2, null) OR 1: id NOT IN (1, 2, null) IS NULL"));

        operator = new InPredicateOperator(true, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                ConstantOperator.NULL));
        argumentsList.add(Arguments.of(operator, "1: id IN (1, 2, null) OR 1: id NOT IN (1, 2, null) IS NULL"));

        // not in values contains expr
        operator = new InPredicateOperator(true, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                new CastOperator(INT, ConstantOperator.createChar("a"))));

        argumentsList.add(Arguments.of(operator, "1: id IN (1, 2, cast(a as int(11))) " +
                "OR 1: id NOT IN (1, 2, cast(a as int(11))) IS NULL"));

        // in constant values without null
        operator = new InPredicateOperator(false, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", false),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2)));

        argumentsList.add(Arguments.of(operator, "1: id NOT IN (1, 2)"));

        operator = new InPredicateOperator(false, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2)));

        argumentsList.add(Arguments.of(operator, "1: id NOT IN (1, 2) OR 1: id IS NULL"));

        // in constant values with null
        operator = new InPredicateOperator(false, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", false),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                ConstantOperator.NULL));
        argumentsList.add(Arguments.of(operator, "1: id NOT IN (1, 2, null) OR 1: id IN (1, 2, null) IS NULL"));

        operator = new InPredicateOperator(false, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                ConstantOperator.NULL));
        argumentsList.add(Arguments.of(operator, "1: id NOT IN (1, 2, null) OR 1: id IN (1, 2, null) IS NULL"));

        // in values contains expr
        operator = new InPredicateOperator(false, ImmutableList.of(
                new ColumnRefOperator(1, INT, "id", true),
                ConstantOperator.createInt(1),
                ConstantOperator.createInt(2),
                new CastOperator(INT, ConstantOperator.createChar("a"))));

        argumentsList.add(Arguments.of(operator, "1: id NOT IN (1, 2, cast(a as int(11))) " +
                "OR 1: id IN (1, 2, cast(a as int(11))) IS NULL"));
        return argumentsList.stream();
    }


    @Test
    void testDictionaryGetOperator() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, INT, "col1", true);
        DictionaryGetOperator operator = new DictionaryGetOperator(
                Lists.newArrayList(col1, ConstantOperator.createInt(0)),
                INT, 100L, 200L, 1, true);
        {
            ScalarOperator newOperator = shuttle.visitDictionaryGetOperator(operator, null);
            assertEquals(operator, newOperator);
        }
        {
            ScalarOperator newOperator = shuttle2.visitDictionaryGetOperator(operator, null);
            assertEquals(operator, newOperator);
        }
    }

    @Test
    void testDictionaryGetOperatorChildUpdate() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, INT, "col1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, INT, "col2", true);
        DictionaryGetOperator dictGet = new DictionaryGetOperator(
                Lists.newArrayList(col1, ConstantOperator.createInt(0)),
                INT, 100L, 200L, 1, true);
        SubfieldOperator subfield = new SubfieldOperator(dictGet, INT, Lists.newArrayList("mapping_id"));

        BaseScalarOperatorShuttle replaceShuttle = new BaseScalarOperatorShuttle() {
            @Override
            public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
                if (variable.getId() == 1) {
                    return col2;
                }
                return variable;
            }
        };

        ScalarOperator result = replaceShuttle.visitSubfield(subfield, null);
        assertNotEquals(subfield, result);
        assert result instanceof SubfieldOperator;
        SubfieldOperator newSubfield = (SubfieldOperator) result;
        assert newSubfield.getChild(0) instanceof DictionaryGetOperator;
        DictionaryGetOperator newDictGet = (DictionaryGetOperator) newSubfield.getChild(0);
        assertEquals(col2, newDictGet.getChild(0));
        assertEquals(100L, newDictGet.getDictionaryId());
    }


    @Test
    void testDictionaryGetOperatorRewriteNullIfNotExist() {
        ColumnRefOperator key = new ColumnRefOperator(1, INT, "key", true);
        DictionaryGetOperator dictGet = new DictionaryGetOperator(
                Lists.newArrayList(
                        ConstantOperator.createVarchar("test_db.user_mapping_dict"),
                        key,
                        ConstantOperator.createBoolean(true)),
                INT, 100L, 200L, 1, true);

        BaseScalarOperatorShuttle toggleBool = new BaseScalarOperatorShuttle() {
            @Override
            public ScalarOperator visitConstant(ConstantOperator literal, Void context) {
                if (literal.getType().isBoolean() && !literal.isNull()) {
                    return ConstantOperator.createBoolean(!literal.getBoolean());
                }
                return literal;
            }
        };

        ScalarOperator result = toggleBool.visitDictionaryGetOperator(dictGet, null);
        assert result instanceof DictionaryGetOperator;
        DictionaryGetOperator rewritten = (DictionaryGetOperator) result;
        assertEquals(ConstantOperator.FALSE, rewritten.getChild(2));
        assertEquals(false, rewritten.getNullIfNotExist());
    }

    // The shuttle copies only the nodes on the path to a changed leaf. Other rules still hold the input tree,
    // so we check that the input is never changed and that each child of the result is in the right place.
    private static class LeafReplaceShuttle extends BaseScalarOperatorShuttle {
        final Map<ScalarOperator, ScalarOperator> replacements = new IdentityHashMap<>();

        @Override
        public ScalarOperator visitConstant(ConstantOperator literal, Void context) {
            return replacements.getOrDefault(literal, literal);
        }

        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
            return replacements.getOrDefault(variable, variable);
        }
    }

    private final ConstantOperator leaf1 = ConstantOperator.createInt(1);
    private final ConstantOperator leaf2 = ConstantOperator.createInt(2);
    private final ConstantOperator leaf3 = ConstantOperator.createInt(3);
    private final ColumnRefOperator leafColumn = new ColumnRefOperator(1, IntegerType.INT, "c", true);
    // g(1, f(2, c), 3)
    private final CallOperator inner = new CallOperator("f", IntegerType.INT, Lists.newArrayList(leaf2, leafColumn));
    private final CallOperator outer = new CallOperator("g", IntegerType.INT, Lists.newArrayList(leaf1, inner, leaf3));

    private void assertInputTreeUnchanged() {
        assertSame(leaf1, outer.getChild(0));
        assertSame(inner, outer.getChild(1));
        assertSame(leaf3, outer.getChild(2));
        assertSame(leaf2, inner.getChild(0));
        assertSame(leafColumn, inner.getChild(1));
    }

    @Test
    void nestedChangeCopiesOnlyTheAncestors() {
        LeafReplaceShuttle replacer = new LeafReplaceShuttle();
        ConstantOperator replacement = ConstantOperator.createInt(12);
        replacer.replacements.put(leaf2, replacement);
        ScalarOperator result = outer.accept(replacer, null);
        assertNotSame(outer, result);
        assertSame(leaf1, result.getChild(0));
        assertNotSame(inner, result.getChild(1));
        assertSame(leaf3, result.getChild(2));
        ScalarOperator newInner = result.getChild(1);
        assertEquals("f", ((CallOperator) newInner).getFnName());
        assertSame(replacement, newInner.getChild(0));
        assertSame(leafColumn, newInner.getChild(1));
        assertInputTreeUnchanged();
    }

    @Test
    void lastChildChangeKeepsEarlierChildren() {
        // The copy starts only when the last child changes, so the earlier unchanged children must be filled in.
        LeafReplaceShuttle replacer = new LeafReplaceShuttle();
        ConstantOperator replacement = ConstantOperator.createInt(13);
        replacer.replacements.put(leaf3, replacement);
        ScalarOperator result = outer.accept(replacer, null);
        assertNotSame(outer, result);
        assertEquals(3, result.getChildren().size());
        assertSame(leaf1, result.getChild(0));
        assertSame(inner, result.getChild(1));
        assertSame(replacement, result.getChild(2));
        assertInputTreeUnchanged();
    }

    @Test
    void caseWhenElseChange() {
        ColumnRefOperator condition = new ColumnRefOperator(2, BooleanType.BOOLEAN, "b", true);
        CaseWhenOperator caseWhen =
                new CaseWhenOperator(IntegerType.INT, null, leaf3, Lists.newArrayList(condition, leaf1));
        LeafReplaceShuttle replacer = new LeafReplaceShuttle();
        ConstantOperator newElse = ConstantOperator.createInt(33);
        replacer.replacements.put(leaf3, newElse);
        CaseWhenOperator result = (CaseWhenOperator) caseWhen.accept(replacer, null);
        assertNotSame(caseWhen, result);
        assertSame(condition, result.getWhenClause(0));
        assertSame(leaf1, result.getThenClause(0));
        assertSame(newElse, result.getElseClause());
        assertSame(leaf3, caseWhen.getElseClause());
    }

    @Test
    void lambdaBodyChange() {
        ColumnRefOperator argument = new ColumnRefOperator(5, IntegerType.INT, "x", true, true);
        CallOperator body = new CallOperator("add", IntegerType.INT, Lists.newArrayList(argument, leaf1));
        LambdaFunctionOperator lambda =
                new LambdaFunctionOperator(Lists.newArrayList(argument), body, IntegerType.INT);

        assertSame(lambda, lambda.accept(new LeafReplaceShuttle(), null));

        LeafReplaceShuttle replacer = new LeafReplaceShuttle();
        ConstantOperator replacement = ConstantOperator.createInt(21);
        replacer.replacements.put(leaf1, replacement);
        LambdaFunctionOperator result = (LambdaFunctionOperator) lambda.accept(replacer, null);
        assertNotSame(lambda, result);
        assertSame(body, lambda.getLambdaExpr());
        assertSame(leaf1, body.getChild(1));
        assertSame(argument, result.getLambdaExpr().getChild(0));
        assertSame(replacement, result.getLambdaExpr().getChild(1));
        assertEquals(lambda.getRefColumns(), result.getRefColumns());
    }
}
