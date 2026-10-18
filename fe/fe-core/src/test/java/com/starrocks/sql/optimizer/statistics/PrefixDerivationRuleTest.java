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

import com.starrocks.catalog.Function;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * left(c, n) [NOT] LIKE 'p%' next to a statistic on x = left(c, 8): the prefix of n characters starts with p exactly
 * when c does while p has at most n characters, and no value of n characters starts with a longer p. The comparison of c
 * itself with a constant of at most 8 characters is a comparison of x.
 *
 * The table has 1000 rows: 600 start with 103, 300 with 104 and 100 are NULL; the head of x holds all of them.
 */
public class PrefixDerivationRuleTest {
    private static final ColumnRefOperator C = new ColumnRefOperator(1, VarcharType.VARCHAR, "c", true);
    private static final ColumnRefOperator X = new ColumnRefOperator(-1, VarcharType.VARCHAR, "expr#1", true);

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.createDefaultCtx();
    }

    private static ScalarOperator call(String name, ScalarOperator... arguments) {
        Type[] types = new Type[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            types[i] = arguments[i].getType();
        }
        Function function = ExprUtils.getBuiltinFunction(name, types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(name, VarcharType.VARCHAR, List.of(arguments), function);
    }

    private static ScalarOperator left(int length) {
        return call("left", C, ConstantOperator.createInt(length));
    }

    private static ScalarOperator like(ScalarOperator left, String pattern) {
        return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, left,
                ConstantOperator.createVarchar(pattern));
    }

    private static ScalarOperator not(ScalarOperator operator) {
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, operator);
    }

    private static ScanExpressionStatistics expressions() {
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        entries.add(new MultiColumnCombinedStats.McvEntry(List.of("10312345"), 600, List.of(600L)));
        entries.add(new MultiColumnCombinedStats.McvEntry(List.of("10400000"), 300, List.of(300L)));
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("x"), 1000, 3, entries, List.of(),
                List.of(100L));
        group.prepare(VarcharType.VARCHAR);
        ColumnStatistic statistic = group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(left(8));
        return new ScanExpressionStatistics("t", List.of(new ScanExpressionStatistics.Entry(canonical,
                ExpressionStatisticsCanonicalizer.keyOf(canonical), X, statistic)),
                Map.of(Set.of(X), new MultiColumnCombinedStats(3, 1000, List.of(X), entries, List.of(100L))));
    }

    private static Statistics scan() {
        return Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0.1, 8,
                        1000)).build();
    }

    private static double rows(ScalarOperator predicate) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(), true, expressions())
                .getOutputRowCount();
    }

    private static double plainRows(ScalarOperator predicate) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(), true).getOutputRowCount();
    }

    private static ScalarOperator written(ScalarOperator predicate) {
        ExpressionStatisticsMatcher.Result result = ExpressionStatisticsMatcher.match(predicate, expressions(), scan());
        return result == null ? null : result.getPredicate();
    }

    @Test
    public void testPrefixOfTheColumnIsThePrefixOfX() {
        for (int length : List.of(3, 5, 7, 8)) {
            ScalarOperator like = like(left(length), "103%");
            ScalarOperator onX = written(like);
            Assertions.assertTrue(onX instanceof LikePredicateOperator, onX.toString());
            Assertions.assertEquals(X, onX.getChild(0));
            Assertions.assertEquals(600, rows(like), 1e-6, "left(c, " + length + ")");
        }
        // strleft is the same function.
        Assertions.assertEquals(300, rows(like(call("strleft", C, ConstantOperator.createInt(5)), "104%")), 1e-6);
        // No value of the head starts with 105: one row, the floor of an estimate.
        Assertions.assertEquals(1, rows(like(left(5), "105%")), 1e-6);
        // A prefix shorter than the one of x, as 1 character of 10.
        Assertions.assertEquals(900, rows(like(left(1), "1%")), 1e-6);
    }

    @Test
    public void testNegationLeavesNullRowsOut() {
        // 1000 rows, 100 NULL, 600 rows start with 103.
        Assertions.assertEquals(300, rows(not(like(left(5), "103%"))), 1e-6);
        Assertions.assertEquals(600, rows(not(like(left(5), "104%"))), 1e-6);
    }

    @Test
    public void testPrefixLongerThanTheValueHasNoRows() {
        // A string of 3 characters does not start with 4 characters.
        ScalarOperator like = like(left(3), "1031%");
        Assertions.assertEquals(ConstantOperator.createBoolean(false), written(like));
        Assertions.assertEquals(1, rows(like), 1e-6);
        // Its negation holds on every value that is not NULL.
        ScalarOperator negated = written(not(like));
        Assertions.assertTrue(negated instanceof IsNullPredicateOperator);
        Assertions.assertTrue(((IsNullPredicateOperator) negated).isNotNull());
        Assertions.assertEquals(900, rows(not(like)), 1e-6);
    }

    @Test
    public void testOrOfPrefixes() {
        // 600 + 300 rows less the one row the estimate of an OR takes for the rows of the second part in the first.
        Assertions.assertEquals(899, rows(Utils.compoundOr(like(left(5), "103%"), like(left(5), "104%"))), 1e-6);
        // The prefix of the first part and a LIKE on c itself in the other: both go over x.
        Assertions.assertEquals(899, rows(Utils.compoundOr(like(left(5), "103%"), like(C, "104%"))), 1e-6);
    }

    @Test
    public void testShapesThatAreNotWritten() {
        // More characters than x holds, a start after the first character, a pattern that is more than a prefix.
        Assertions.assertNull(written(like(left(9), "103%")));
        Assertions.assertNull(written(like(call("substr", C, ConstantOperator.createInt(2), ConstantOperator.createInt(3)),
                "03%")));
        Assertions.assertNull(written(like(left(5), "10_%")));
        Assertions.assertNull(written(like(left(5), "%103")));
        Assertions.assertNull(written(like(left(5), "10%3%")));
        // They are estimated as before.
        for (ScalarOperator like : List.of(like(left(9), "103%"), like(left(5), "10_%"), like(left(5), "%103"))) {
            Assertions.assertEquals(plainRows(like), rows(like), 1e-6, like.toString());
        }
    }

    private static ScalarOperator compare(BinaryType type, String value) {
        return new BinaryPredicateOperator(type, C, ConstantOperator.createVarchar(value));
    }

    private static ScalarOperator in(boolean not, String... values) {
        List<ScalarOperator> children = new ArrayList<>();
        children.add(C);
        for (String value : values) {
            children.add(ConstantOperator.createVarchar(value));
        }
        return new InPredicateOperator(not, children.toArray(new ScalarOperator[0]));
    }

    @Test
    public void testComparisonOfTheColumnWithAShorterConstantIsTheComparisonOfX() {
        // 600 rows start with 103, 300 with 104, 100 are NULL. The order of c and a constant of 3 characters is that of
        // its first 3 characters and of its length, which x holds, so the comparison is written over x as it stands.
        for (BinaryType type : List.of(BinaryType.LT, BinaryType.LE, BinaryType.GT, BinaryType.GE)) {
            ScalarOperator comparison = compare(type, "104");
            ScalarOperator onX = written(comparison);
            Assertions.assertEquals(X, onX.getChild(0), comparison.toString());
            Assertions.assertEquals(type, ((BinaryPredicateOperator) onX).getBinaryType(), comparison.toString());
            Assertions.assertEquals(ConstantOperator.createVarchar("104"), onX.getChild(1));
        }
        // 10312345 is below 104 and 10400000 is above it.
        Assertions.assertEquals(600, rows(compare(BinaryType.LT, "104")), 1e-6);
        Assertions.assertEquals(600, rows(compare(BinaryType.LE, "104")), 1e-6);
        Assertions.assertEquals(300, rows(compare(BinaryType.GT, "104")), 1e-6);
        Assertions.assertEquals(300, rows(compare(BinaryType.GE, "104")), 1e-6);
        // 103 is the start of the first, which is above it.
        Assertions.assertEquals(1, rows(compare(BinaryType.LE, "103")), 1e-6);
        Assertions.assertEquals(900, rows(compare(BinaryType.GE, "103")), 1e-6);
        // The rows that the two sides of a constant keep are the rows that are not NULL.
        Assertions.assertEquals(900, rows(compare(BinaryType.LT, "104")) + rows(compare(BinaryType.GE, "104")), 1e-6);
        Assertions.assertEquals(900, rows(compare(BinaryType.LE, "10350")) + rows(compare(BinaryType.GT, "10350")), 1e-6);
        // The negation is the other side.
        Assertions.assertEquals(300, rows(not(compare(BinaryType.LT, "104"))), 1e-6);
        // Without the statistics on x the comparison is not an estimate of the head.
        Assertions.assertNotEquals(600, plainRows(compare(BinaryType.LT, "104")), 1.0);
    }

    @Test
    public void testComparisonOfTheColumnIsTheSameAsOfThePrefixOfTheLengthOfX() {
        // left(c, 8) holds the first 8 characters, and is x.
        for (BinaryType type : List.of(BinaryType.LT, BinaryType.LE, BinaryType.GT, BinaryType.GE, BinaryType.EQ,
                BinaryType.NE)) {
            for (String value : List.of("1", "103", "10312", "104", "11")) {
                Assertions.assertEquals(rows(new BinaryPredicateOperator(type, left(8), ConstantOperator.createVarchar(value))),
                        rows(compare(type, value)), 1e-6, type + " " + value);
            }
        }
    }

    @Test
    public void testBetweenIsThePairOfComparisons() {
        ScalarOperator between = Utils.compoundAnd(compare(BinaryType.GE, "103"), compare(BinaryType.LE, "1035"));
        // 10312345 is between, and 10400000 is above.
        Assertions.assertEquals(600, rows(between), 1e-6);
        Assertions.assertEquals(300, rows(Utils.compoundAnd(compare(BinaryType.GE, "1035"), compare(BinaryType.LE, "104z"))),
                1e-6);
        // Both sides over x.
        ScalarOperator onX = written(between);
        for (ScalarOperator side : Utils.extractConjuncts(onX)) {
            Assertions.assertEquals(X, side.getChild(0));
        }
    }

    @Test
    public void testEqualityAndInOfTheColumnWithShortConstants() {
        // x holds 8 characters of each value, so a constant of fewer characters equals no value of the head.
        Assertions.assertEquals(1, rows(compare(BinaryType.EQ, "103")), 1e-6);
        Assertions.assertEquals(900, rows(compare(BinaryType.NE, "103")), 1e-6);
        Assertions.assertEquals(1, rows(compare(BinaryType.EQ, "1031234")), 1e-6);
        Assertions.assertEquals(900, rows(compare(BinaryType.NE, "1031234")), 1e-6);
        Assertions.assertEquals(X, written(in(false, "103", "1031234")).getChild(0));
        Assertions.assertEquals(X, written(in(true, "103", "1031234")).getChild(0));
        // No value of the head is one of the constants: the one row an estimate takes when it finds none, and every row
        // that is not NULL.
        Assertions.assertEquals(1, rows(in(false, "10312", "105")), 1e-6);
        Assertions.assertEquals(900, rows(in(true, "10312", "105")), 1e-6);
        Assertions.assertEquals(1, rows(in(false, "103", "1031234")), 1e-6);
        Assertions.assertEquals(900, rows(in(true, "103", "1031234")), 1e-6);
    }

    @Test
    public void testEqualityAndInOfTheColumnWithConstantsOfTheLengthOfXAreNotWritten() {
        // The rows of x = v are those of every string that starts with v, and c = v keeps one value among them when
        // the strings are longer than 8 characters, so these conditions are estimated by the statistics of c.
        for (String value : List.of("10312345", "10400000", "10500000")) {
            for (BinaryType type : List.of(BinaryType.EQ, BinaryType.NE)) {
                ScalarOperator comparison = compare(type, value);
                Assertions.assertNull(written(comparison), comparison.toString());
                Assertions.assertEquals(plainRows(comparison), rows(comparison), 1e-6, comparison.toString());
            }
        }
        Assertions.assertEquals(1, rows(compare(BinaryType.EQ, "10312345")), 1e-6);
        Assertions.assertEquals(899.1, rows(compare(BinaryType.NE, "10312345")), 1e-6);
        for (ScalarOperator list : List.of(in(false, "10312345", "10400000"), in(true, "10312345"),
                in(false, "10400000", "105"), in(true, "10312345", "105"), in(false, "10312345", "10400000", "1"))) {
            Assertions.assertNull(written(list), list.toString());
            Assertions.assertEquals(plainRows(list), rows(list), 1e-6, list.toString());
        }
    }

    @Test
    public void testConstantAsLongAsXOrLonger() {
        // 8 characters: c < v and c >= v are x < v and x >= v, which are exact. c <= v and c > v lose the strings equal
        // to v, which are few next to the longer ones that start with it, and are x < v and x >= v as well, so the
        // sides of a constant keep exactly the rows that are not NULL.
        Assertions.assertEquals(600, rows(compare(BinaryType.LT, "10400000")), 1e-6);
        Assertions.assertEquals(300, rows(compare(BinaryType.GE, "10400000")), 1e-6);
        Assertions.assertEquals(600, rows(compare(BinaryType.LE, "10400000")), 1e-6);
        Assertions.assertEquals(300, rows(compare(BinaryType.GT, "10400000")), 1e-6);
        Assertions.assertEquals(900, rows(compare(BinaryType.GT, "10312345")), 1e-6);
        Assertions.assertEquals(1, rows(compare(BinaryType.LE, "10312345")), 1e-6);
        // The sides keep the 900 rows, but for the one row an estimate takes when it finds none.
        for (String value : List.of("10400000", "10399999", "10350000", "10312346")) {
            Assertions.assertEquals(900, rows(compare(BinaryType.LT, value)) + rows(compare(BinaryType.GE, value)), 1e-6,
                    value);
            Assertions.assertEquals(900, rows(compare(BinaryType.LE, value)) + rows(compare(BinaryType.GT, value)), 1e-6,
                    value);
            ScalarOperator le = written(compare(BinaryType.LE, value));
            Assertions.assertEquals(BinaryType.LT, ((BinaryPredicateOperator) le).getBinaryType(), value);
            Assertions.assertEquals(X, le.getChild(0), value);
            ScalarOperator gt = written(compare(BinaryType.GT, value));
            Assertions.assertEquals(BinaryType.GE, ((BinaryPredicateOperator) gt).getBinaryType(), value);
        }
        // More than 8 characters: the first 8 decide unless they are equal to the first 8 of x, whose rows are counted
        // on both sides.
        Assertions.assertEquals(300, rows(compare(BinaryType.GE, "1040000000")), 1e-6);
        Assertions.assertEquals(300, rows(compare(BinaryType.GT, "1040000000")), 1e-6);
        Assertions.assertEquals(900, rows(compare(BinaryType.LE, "1040000000")), 1e-6);
        Assertions.assertEquals(900, rows(compare(BinaryType.LT, "1040000000")), 1e-6);
        Assertions.assertEquals(600, rows(compare(BinaryType.LT, "1031234567")), 1e-6);
        // A range inside one prefix keeps the rows of the prefix, and not none.
        Assertions.assertEquals(300, rows(Utils.compoundAnd(compare(BinaryType.GE, "1040000010"),
                compare(BinaryType.LT, "1040000020"))), 1e-6);
        // The equality with such a constant is not written, as its rows are one value among those of x = v.
        Assertions.assertNull(written(compare(BinaryType.EQ, "1040000000")));
        Assertions.assertEquals(plainRows(compare(BinaryType.EQ, "1040000000")), rows(compare(BinaryType.EQ, "1040000000")),
                1e-6);
        Assertions.assertNull(written(compare(BinaryType.NE, "1040000000")));
        Assertions.assertNull(written(in(false, "10312345", "1040000000")));
        Assertions.assertNull(written(in(true, "1040000000")));
    }

}
