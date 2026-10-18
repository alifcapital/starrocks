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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The atoms over x that are values of one axis, and the one atom that stands for several of them: x IN (...) for the
 * values of x and the prefix IN, substr(x, 1, n) IN (...), for the prefixes of n characters; the negations of them are
 * a NOT IN and the NOT of the prefix IN.
 */
public class AxisSetsTest {
    private static final ColumnRefOperator X = new ColumnRefOperator(-1, VarcharType.VARCHAR, "expr#1", true);
    private static final ColumnRefOperator OTHER = new ColumnRefOperator(-2, VarcharType.VARCHAR, "expr#2", true);

    private static ConstantOperator text(String value) {
        return ConstantOperator.createVarchar(value);
    }

    private static ScalarOperator compare(BinaryType type, ScalarOperator left, String value) {
        return new BinaryPredicateOperator(type, left, text(value));
    }

    private static ScalarOperator like(ScalarOperator left, String pattern) {
        return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, left, text(pattern));
    }

    private static ScalarOperator in(boolean not, ScalarOperator left, String... values) {
        List<ScalarOperator> children = new ArrayList<>();
        children.add(left);
        for (String value : values) {
            children.add(text(value));
        }
        return new InPredicateOperator(not, children.toArray(new ScalarOperator[0]));
    }

    private static ScalarOperator not(ScalarOperator operator) {
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, operator);
    }

    private static ScalarOperator prefixes(int length) {
        return PrefixDerivationRule.substr(X, 1, length);
    }

    private static void assertMember(boolean prefix, int length, List<String> values, Optional<AxisSets.Member> member) {
        Assertions.assertTrue(member.isPresent());
        Assertions.assertEquals(new AxisSets.Axis(prefix, length), member.get().axis());
        Assertions.assertEquals(values.stream().map(AxisSetsTest::text).toList(), member.get().values());
    }

    @Test
    public void testAtomsOfAnOrAreValuesOnAnAxis() {
        assertMember(false, 0, List.of("a"), AxisSets.memberOf(compare(BinaryType.EQ, X, "a"), X, false));
        assertMember(false, 0, List.of("a", "b"), AxisSets.memberOf(in(false, X, "a", "b"), X, false));
        assertMember(true, 3, List.of("abc"), AxisSets.memberOf(like(X, "abc%"), X, false));
        // An escaped wildcard is a character of the prefix, and a code point is one character.
        assertMember(true, 3, List.of("a%b"), AxisSets.memberOf(like(X, "a\\%b%"), X, false));
        assertMember(true, 2, List.of("😀x"), AxisSets.memberOf(like(X, "😀x%"), X, false));
        assertMember(true, 2, List.of("ab", "cd"), AxisSets.memberOf(in(false, prefixes(2), "ab", "cd"), X, false));
    }

    @Test
    public void testNegationsOfAnAndAreValuesOnAnAxis() {
        assertMember(false, 0, List.of("a"), AxisSets.memberOf(compare(BinaryType.NE, X, "a"), X, true));
        assertMember(false, 0, List.of("a", "b"), AxisSets.memberOf(in(true, X, "a", "b"), X, true));
        assertMember(true, 3, List.of("abc"), AxisSets.memberOf(not(like(X, "abc%")), X, true));
        assertMember(true, 2, List.of("ab", "cd"), AxisSets.memberOf(not(in(false, prefixes(2), "ab", "cd")), X, true));
    }

    @Test
    public void testOtherAtomsAreNotMembers() {
        // The side of the context: an OR takes the atoms, an AND the negations of them.
        Assertions.assertTrue(AxisSets.memberOf(compare(BinaryType.NE, X, "a"), X, false).isEmpty());
        Assertions.assertTrue(AxisSets.memberOf(compare(BinaryType.EQ, X, "a"), X, true).isEmpty());
        Assertions.assertTrue(AxisSets.memberOf(in(true, X, "a"), X, false).isEmpty());
        Assertions.assertTrue(AxisSets.memberOf(not(compare(BinaryType.EQ, X, "a")), X, true).isEmpty());
        // Another column, a range, a pattern with more than a prefix, an empty prefix, a REGEXP, a NULL, a prefix of the
        // wrong length, a start after the first character.
        Assertions.assertTrue(AxisSets.memberOf(compare(BinaryType.EQ, OTHER, "a"), X, false).isEmpty());
        Assertions.assertTrue(AxisSets.memberOf(compare(BinaryType.GE, X, "a"), X, false).isEmpty());
        Assertions.assertTrue(AxisSets.memberOf(like(X, "a_%"), X, false).isEmpty());
        Assertions.assertTrue(AxisSets.memberOf(like(X, "%"), X, false).isEmpty());
        Assertions.assertTrue(AxisSets.memberOf(like(OTHER, "ab%"), X, false).isEmpty());
        Assertions.assertTrue(AxisSets.memberOf(new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, X,
                text("^ab")), X, false).isEmpty());
        Assertions.assertTrue(AxisSets.memberOf(in(false, X, "a"), OTHER, false).isEmpty());
        Assertions.assertTrue(AxisSets.memberOf(new InPredicateOperator(false, X, text("a"),
                ConstantOperator.createNull(VarcharType.VARCHAR)), X, false).isEmpty());
        Assertions.assertTrue(AxisSets.memberOf(in(false, prefixes(2), "abc"), X, false).isEmpty());
        Assertions.assertTrue(AxisSets.memberOf(in(false, PrefixDerivationRule.substr(X, 2, 2), "ab"), X, false)
                .isEmpty());
    }

    @Test
    public void testSetOfSeveralAtomsKeepsTheValuesOnceInTheOrderOfTheAtoms() {
        AxisSets.Axis values = new AxisSets.Axis(false, 0);
        List<AxisSets.Member> members = List.of(new AxisSets.Member(values, List.of(text("b"))),
                new AxisSets.Member(values, List.of(text("a"), text("b"))), new AxisSets.Member(values, List.of(text("c"))));
        Assertions.assertEquals(in(false, X, "b", "a", "c"), AxisSets.setOf(values, members, X, false));
        Assertions.assertEquals(in(true, X, "b", "a", "c"), AxisSets.setOf(values, members, X, true));
        AxisSets.Axis prefix = new AxisSets.Axis(true, 2);
        List<AxisSets.Member> prefixMembers = List.of(new AxisSets.Member(prefix, List.of(text("ab"))),
                new AxisSets.Member(prefix, List.of(text("cd"), text("ab"))));
        Assertions.assertEquals(in(false, prefixes(2), "ab", "cd"), AxisSets.setOf(prefix, prefixMembers, X, false));
        Assertions.assertEquals(not(in(false, prefixes(2), "ab", "cd")), AxisSets.setOf(prefix, prefixMembers, X, true));
        // Constants of two types are not one set.
        List<AxisSets.Member> mixed = List.of(new AxisSets.Member(values, List.of(text("a"))),
                new AxisSets.Member(values, List.of(ConstantOperator.createInt(1))));
        Assertions.assertNull(AxisSets.setOf(values, mixed, X, false));
    }
}
