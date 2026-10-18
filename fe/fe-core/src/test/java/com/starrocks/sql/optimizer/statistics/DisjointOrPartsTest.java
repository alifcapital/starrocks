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

import com.starrocks.common.util.StringDateFormat;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * The parts of an OR that have no row in common with the parts before them, told by their form: the equalities of one
 * expression with other constants, and the intervals of it with no common point, with the ends open or closed.
 */
public class DisjointOrPartsTest {
    private static final ColumnRefOperator E = new ColumnRefOperator(1, IntegerType.INT, "e", true);
    private static final ColumnRefOperator F = new ColumnRefOperator(2, IntegerType.INT, "f", true);
    private static final ColumnRefOperator TEXT = new ColumnRefOperator(3, VarcharType.VARCHAR, "t", true);

    private static ConstantOperator number(int value) {
        return ConstantOperator.createInt(value);
    }

    private static ScalarOperator compare(BinaryType type, ScalarOperator left, ScalarOperator right) {
        return new BinaryPredicateOperator(type, left, right);
    }

    private static ScalarOperator eq(ScalarOperator left, int value) {
        return compare(BinaryType.EQ, left, number(value));
    }

    private static ScalarOperator range(ScalarOperator left, BinaryType lowerType, int lower, BinaryType upperType,
                                       int upper) {
        return Utils.compoundAnd(compare(lowerType, left, number(lower)), compare(upperType, left, number(upper)));
    }

    // The answers for the parts in turn.
    private static List<Boolean> answers(ScalarOperator... parts) {
        DisjointOrParts disjoint = new DisjointOrParts();
        List<Boolean> answers = new ArrayList<>();
        for (ScalarOperator part : parts) {
            answers.add(disjoint.add(part));
        }
        return answers;
    }

    @Test
    public void testEqualitiesWithOtherConstantsAreApart() {
        Assertions.assertEquals(List.of(true, true, true), answers(eq(E, 1), eq(E, 2), eq(E, 3)));
        // The same constant again has rows in common, and the parts after it are told against it as well.
        Assertions.assertEquals(List.of(true, true, false, true), answers(eq(E, 1), eq(E, 2), eq(E, 1), eq(E, 3)));
        Assertions.assertEquals(List.of(true, false, false), answers(eq(E, 1), eq(E, 1), eq(E, 1)));
        Assertions.assertEquals(List.of(true, false, true), answers(eq(E, 1), eq(E, 1), eq(E, 2)));
    }

    @Test
    public void testIntervalsWithNoCommonPointAreApart() {
        // [1, 5) and [5, 9) have no common point, [1, 5] and [5, 9) have 5.
        Assertions.assertEquals(List.of(true, true), answers(range(E, BinaryType.GE, 1, BinaryType.LT, 5),
                range(E, BinaryType.GE, 5, BinaryType.LT, 9)));
        Assertions.assertEquals(List.of(true, false), answers(range(E, BinaryType.GE, 1, BinaryType.LE, 5),
                range(E, BinaryType.GE, 5, BinaryType.LT, 9)));
        Assertions.assertEquals(List.of(true, true), answers(range(E, BinaryType.GE, 1, BinaryType.LE, 5),
                range(E, BinaryType.GT, 5, BinaryType.LT, 9)));
        Assertions.assertEquals(List.of(true, false), answers(range(E, BinaryType.GE, 1, BinaryType.LE, 5),
                range(E, BinaryType.GE, 3, BinaryType.LT, 9)));
        // One interval inside the other, and the one on the left.
        Assertions.assertEquals(List.of(true, false), answers(range(E, BinaryType.GE, 1, BinaryType.LE, 9),
                range(E, BinaryType.GE, 3, BinaryType.LT, 4)));
        Assertions.assertEquals(List.of(true, true), answers(range(E, BinaryType.GE, 5, BinaryType.LT, 9),
                range(E, BinaryType.GE, 1, BinaryType.LT, 5)));
        // Three: the third is told against the two before it.
        Assertions.assertEquals(List.of(true, true, false), answers(range(E, BinaryType.GE, 1, BinaryType.LT, 3),
                range(E, BinaryType.GE, 5, BinaryType.LT, 7), range(E, BinaryType.GE, 2, BinaryType.LT, 6)));
    }

    @Test
    public void testHalfLinesAndPointsAndIntervals() {
        Assertions.assertEquals(List.of(true, true), answers(compare(BinaryType.LT, E, number(5)),
                compare(BinaryType.GE, E, number(5))));
        Assertions.assertEquals(List.of(true, false), answers(compare(BinaryType.LE, E, number(5)),
                compare(BinaryType.GE, E, number(5))));
        Assertions.assertEquals(List.of(true, true), answers(compare(BinaryType.GT, E, number(10)),
                compare(BinaryType.LT, E, number(5))));
        Assertions.assertEquals(List.of(true, false), answers(compare(BinaryType.GT, E, number(1)),
                compare(BinaryType.GE, E, number(5))));
        // A point against an interval, in both orders.
        Assertions.assertEquals(List.of(true, true), answers(range(E, BinaryType.GE, 1, BinaryType.LT, 5), eq(E, 5)));
        Assertions.assertEquals(List.of(true, false), answers(range(E, BinaryType.GE, 1, BinaryType.LT, 5), eq(E, 4)));
        Assertions.assertEquals(List.of(true, true), answers(eq(E, 0), range(E, BinaryType.GE, 1, BinaryType.LT, 5)));
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), range(E, BinaryType.GE, 1, BinaryType.LT, 5)));
    }

    @Test
    public void testTheTighterEndOfSeveralComparisonsOfAnInterval() {
        // e > 1 AND e >= 5 AND e < 9 AND e <= 7 keeps [5, 7].
        ScalarOperator interval = Utils.compoundAnd(List.of(compare(BinaryType.GT, E, number(1)),
                compare(BinaryType.GE, E, number(5)), compare(BinaryType.LT, E, number(9)),
                compare(BinaryType.LE, E, number(7))));
        Assertions.assertEquals(List.of(true, true), answers(interval, eq(E, 8)));
        Assertions.assertEquals(List.of(true, false), answers(interval, eq(E, 7)));
        Assertions.assertEquals(List.of(true, false), answers(interval, eq(E, 5)));
        Assertions.assertEquals(List.of(true, true), answers(interval, eq(E, 4)));
        // The open end is the tighter when the ends are equal: e >= 5 AND e > 5 AND e < 9 AND e <= 9 is (5, 9).
        ScalarOperator open = Utils.compoundAnd(List.of(compare(BinaryType.GE, E, number(5)),
                compare(BinaryType.GT, E, number(5)), compare(BinaryType.LT, E, number(9)),
                compare(BinaryType.LE, E, number(9))));
        Assertions.assertEquals(List.of(true, true, true), answers(open, eq(E, 5), eq(E, 9)));
        Assertions.assertEquals(List.of(true, false), answers(open, eq(E, 6)));
    }

    @Test
    public void testAnotherExpressionOrAnotherFormIsNotTold() {
        // Another expression has rows in common, and so does everything after it.
        Assertions.assertEquals(List.of(true, false, false), answers(eq(E, 1), eq(F, 2), eq(E, 3)));
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), compare(BinaryType.NE, E, number(2))));
        Assertions.assertEquals(List.of(true, false, false), answers(eq(E, 1), compare(BinaryType.NE, E, number(2)),
                eq(E, 3)));
        // The first part of another form leaves all of them untold.
        Assertions.assertEquals(List.of(false, false), answers(compare(BinaryType.NE, E, number(2)), eq(E, 3)));
        Assertions.assertEquals(List.of(false, false), answers(new InPredicateOperator(false, E, number(1), number(2)),
                eq(E, 3)));
        // A comparison with NULL or with an expression, <=>, and an AND of two expressions.
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), compare(BinaryType.EQ, E,
                ConstantOperator.createNull(IntegerType.INT))));
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), compare(BinaryType.EQ, E, F)));
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), compare(BinaryType.EQ_FOR_NULL, E, number(2))));
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), Utils.compoundAnd(
                compare(BinaryType.GE, E, number(5)), compare(BinaryType.LT, F, number(9)))));
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), Utils.compoundAnd(
                compare(BinaryType.GE, E, number(5)), eq(E, 7))));
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), Utils.compoundOr(eq(E, 2), eq(E, 3))));
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT, eq(E, 2))));
    }

    @Test
    public void testAnIntervalWithNoValueIsNotTold() {
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), range(E, BinaryType.GT, 5, BinaryType.LT, 3)));
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), range(E, BinaryType.GT, 5, BinaryType.LE, 5)));
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), range(E, BinaryType.GE, 5, BinaryType.LT, 5)));
        // One value is an interval.
        Assertions.assertEquals(List.of(true, true), answers(eq(E, 1), range(E, BinaryType.GE, 5, BinaryType.LE, 5)));
    }

    @Test
    public void testConstantsOfAnotherTypeAreNotCompared() {
        // The cast of one constant to the type of the other may round it.
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), compare(BinaryType.EQ, E,
                ConstantOperator.createBigint(2))));
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), compare(BinaryType.EQ, E,
                ConstantOperator.createVarchar("2"))));
        Assertions.assertEquals(List.of(true, true), answers(compare(BinaryType.EQ, E, ConstantOperator.createBigint(1)),
                compare(BinaryType.EQ, E, ConstantOperator.createBigint(2))));
    }

    @Test
    public void testStringsAndTimesAreCompared() {
        Assertions.assertEquals(List.of(true, true, false), answers(
                Utils.compoundAnd(compare(BinaryType.GE, TEXT, ConstantOperator.createVarchar("2026-01")),
                        compare(BinaryType.LT, TEXT, ConstantOperator.createVarchar("2026-02"))),
                Utils.compoundAnd(compare(BinaryType.GE, TEXT, ConstantOperator.createVarchar("2026-03")),
                        compare(BinaryType.LT, TEXT, ConstantOperator.createVarchar("2026-04"))),
                compare(BinaryType.EQ, TEXT, ConstantOperator.createVarchar("2026-03-15"))));
        // The order of the statistics puts a longer string after the one it starts with.
        Assertions.assertEquals(List.of(true, true), answers(compare(BinaryType.LT, TEXT,
                ConstantOperator.createVarchar("a")), compare(BinaryType.GE, TEXT, ConstantOperator.createVarchar("a"))));
        Assertions.assertEquals(List.of(true, true), answers(compare(BinaryType.EQ, TEXT, ConstantOperator.createVarchar("a")),
                compare(BinaryType.EQ, TEXT, ConstantOperator.createVarchar("a\u0000"))));
        // Two times a fraction of a second apart.
        ConstantOperator first = ConstantOperator.createDatetime(LocalDateTime.of(2026, 8, 15, 10, 0, 0, 200_000_000));
        ConstantOperator second = ConstantOperator.createDatetime(LocalDateTime.of(2026, 8, 15, 10, 0, 0, 500_000_000));
        ColumnRefOperator at = new ColumnRefOperator(4, com.starrocks.type.DateType.DATETIME, "at", true);
        Assertions.assertEquals(List.of(true, true), answers(compare(BinaryType.EQ, at, first),
                compare(BinaryType.EQ, at, second)));
        Assertions.assertEquals(List.of(true, false), answers(compare(BinaryType.LE, at, second),
                compare(BinaryType.GT, at, first)));
        Assertions.assertEquals(List.of(true, true), answers(compare(BinaryType.LE, at, first),
                compare(BinaryType.GT, at, first)));
    }

    private static ScalarOperator like(String pattern) {
        return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, TEXT,
                ConstantOperator.createVarchar(pattern));
    }

    private static ScalarOperator text(BinaryType type, String value) {
        return compare(type, TEXT, ConstantOperator.createVarchar(value));
    }

    // The LIKE of a prefix is an interval for a column of dates, whose estimate reads it so.
    private static List<Boolean> likeAnswers(ScalarOperator... parts) {
        return PredicateStatisticsCalculator.withStringDateFormats(column -> TEXT.equals(column)
                ? StringDateFormat.parse("%Y-%m-%dT%H:%i:%s.%fZ") : null, () -> answers(parts));
    }

    @Test
    public void testLikeOfAPrefixOnAColumnWithoutDatesIsNotTold() {
        Assertions.assertEquals(List.of(false, false), answers(like("a%"), like("b%")));
        Assertions.assertEquals(List.of(true, false), answers(eq(E, 1), like("b%")));
        Assertions.assertEquals(List.of(false, false), answers(like("a%"), text(BinaryType.EQ, "b")));
    }

    @Test
    public void testLikeOfAPrefixIsTheStringsThatStartWithIt() {
        // From the prefix up to the first string after all those with it: a% is [a, b).
        Assertions.assertEquals(List.of(true, true, true), likeAnswers(like("a%"), like("b%"), like("c%")));
        // A prefix of another prefix has its rows, in both orders; the same prefix again as well.
        Assertions.assertEquals(List.of(true, false), likeAnswers(like("a%"), like("ab%")));
        Assertions.assertEquals(List.of(true, false), likeAnswers(like("ab%"), like("a%")));
        Assertions.assertEquals(List.of(true, false), likeAnswers(like("a%"), like("a%")));
        Assertions.assertEquals(List.of(true, true), likeAnswers(like("ab%"), like("ac%")));
        // Equalities and comparisons of the same expression: the string a is among those that start with a.
        Assertions.assertEquals(List.of(true, false), likeAnswers(like("a%"), text(BinaryType.EQ, "a")));
        Assertions.assertEquals(List.of(true, false), likeAnswers(like("a%"), text(BinaryType.EQ, "abc")));
        Assertions.assertEquals(List.of(true, true), likeAnswers(like("a%"), text(BinaryType.EQ, "b")));
        Assertions.assertEquals(List.of(true, true), likeAnswers(like("a%"), text(BinaryType.LT, "a")));
        Assertions.assertEquals(List.of(true, false), likeAnswers(like("a%"), text(BinaryType.LE, "a")));
        Assertions.assertEquals(List.of(true, true), likeAnswers(like("a%"), text(BinaryType.GE, "b")));
        Assertions.assertEquals(List.of(true, false), likeAnswers(like("a%"), text(BinaryType.GE, "az")));
        Assertions.assertEquals(List.of(true, true), likeAnswers(text(BinaryType.EQ, "2026-08-15"), like("2026-08-16%")));
        // The escaped characters are read as characters.
        Assertions.assertEquals(List.of(true, true), likeAnswers(like("a\\%b%"), like("a\\%c%")));
        Assertions.assertEquals(List.of(true, false), likeAnswers(like("a\\%b%"), text(BinaryType.EQ, "a%b")));
        // No string is above all of those that start with the last code point: the interval has no upper end.
        String last = new String(Character.toChars(Character.MAX_CODE_POINT));
        Assertions.assertEquals(List.of(true, true), likeAnswers(like(last + "%"), text(BinaryType.EQ, "a")));
        Assertions.assertEquals(List.of(true, false), likeAnswers(like(last + "%"), text(BinaryType.GE, "zzz")));
        // A pattern with more than a prefix, and a REGEXP, are not told, and the parts after them neither.
        Assertions.assertEquals(List.of(true, false, false), likeAnswers(like("a%"), like("b_%"), like("c%")));
        Assertions.assertEquals(List.of(true, false), likeAnswers(like("a%"),
                new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, TEXT, ConstantOperator.createVarchar("^b"))));
        // Another expression.
        Assertions.assertEquals(List.of(true, false), likeAnswers(like("a%"), compare(BinaryType.EQ, E, number(1))));
    }

    @Test
    public void testTheSameExpressionWithAnImplicitCastOfTheAnalyzer() {
        ScalarOperator implicit = new CastOperator(IntegerType.BIGINT, E, true);
        ScalarOperator written = new CastOperator(IntegerType.BIGINT, E);
        Assertions.assertEquals(List.of(true, true), answers(
                compare(BinaryType.EQ, implicit, ConstantOperator.createBigint(1)),
                compare(BinaryType.EQ, written, ConstantOperator.createBigint(2))));
        Assertions.assertEquals(List.of(true, false), answers(
                compare(BinaryType.EQ, implicit, ConstantOperator.createBigint(1)),
                compare(BinaryType.EQ, written, ConstantOperator.createBigint(1))));
        // A cast to another type is another expression.
        Assertions.assertEquals(List.of(true, false), answers(
                compare(BinaryType.EQ, implicit, ConstantOperator.createBigint(1)),
                compare(BinaryType.EQ, new CastOperator(IntegerType.LARGEINT, E), ConstantOperator.createLargeInt(
                        java.math.BigInteger.TWO))));
    }
}
