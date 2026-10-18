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
import com.starrocks.qe.ConnectContext;
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
import com.starrocks.sql.optimizer.rewrite.scalar.ConsolidateLikesRule;
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
 * itself with a constant of at most 8 characters is a comparison of x, and a REGEXP of anchored prefixes is the OR of
 * their LIKE conditions.
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

    // The matcher reads the predicate after the normalizer, as the estimate does.
    private static ScalarOperator written(ScalarOperator predicate) {
        ExpressionStatisticsMatcher.Result result = ExpressionStatisticsMatcher.match(
                EstimatePredicateNormalizer.normalize(predicate, scan()), expressions(), scan());
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
    public void testNegationLeavesTheHeadOfXWithoutTheValuesOfThePrefix() {
        // NOT of the prefix 103 leaves the value 10400000 of 300 rows in the head of x, and the NULL rows are gone.
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(not(like(left(5), "103%")), scan(), true,
                expressions());
        ColumnStatistic x = result.getScanExpressionStatistics().getEntries().get(0).getStatistic();
        Assertions.assertEquals(Map.of("10400000", 300L), x.getHistogram().getMCV());
        Assertions.assertEquals(0, x.getNullsFraction(), 0);
    }

    @Test
    public void testNegatedStatisticOfXKeepsTheHeadWithoutTheValuesOfTheCondition() {
        // The head of x is the one the statistic of x holds, whatever the statistics after the negation hold of it.
        ColumnStatistic statistic = expressions().getEntries().get(0).getStatistic();
        Statistics input = Statistics.builder().setOutputRowCount(1000).addColumnStatistic(X, statistic).build();
        Statistics negated = Statistics.builder().setOutputRowCount(300).addColumnStatistic(X, statistic).build();
        Statistics result = ExpressionStatisticsEstimator.withNegatedVirtualColumn(like(X, "103%"), input, negated);
        ColumnStatistic after = result.getColumnStatistics().get(X);
        Assertions.assertEquals(Map.of("10400000", 300L), after.getHistogram().getMCV());
        Assertions.assertEquals(0, after.getNullsFraction(), 0);
        Assertions.assertEquals(1, after.getDistinctValuesCount(), 0);
        Assertions.assertEquals(300, result.getOutputRowCount(), 0);
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
        // 600 + 300 rows: no string starts with both prefixes, so the second part has no row among those of the first.
        Assertions.assertEquals(900, rows(Utils.compoundOr(like(left(5), "103%"), like(left(5), "104%"))), 1e-6);
        // The prefix of the first part and a LIKE on c itself in the other: both go over x.
        Assertions.assertEquals(900, rows(Utils.compoundOr(like(left(5), "103%"), like(C, "104%"))), 1e-6);
    }

    @Test
    public void testShapesThatAreNotWritten() {
        // More characters than x holds, a start after the first character, a % among the characters of the pattern or
        // before them, characters after the %, and a pattern that does not end with %.
        Assertions.assertNull(written(like(left(9), "103%")));
        Assertions.assertNull(written(like(call("substr", C, ConstantOperator.createInt(2), ConstantOperator.createInt(3)),
                "03%")));
        Assertions.assertNull(written(like(left(5), "%103")));
        Assertions.assertNull(written(like(left(5), "10%3%")));
        Assertions.assertNull(written(like(left(5), "10_%3")));
        Assertions.assertNull(written(like(left(5), "10_")));
        Assertions.assertNull(written(like(C, "%_")));
        Assertions.assertNull(written(like(C, "1031%_")));
        // They are estimated as before.
        for (ScalarOperator like : List.of(like(left(9), "103%"), like(left(5), "%103"), like(left(5), "10_%3"))) {
            Assertions.assertEquals(plainRows(like), rows(like), 1e-6, like.toString());
        }
    }

    @Test
    public void testPatternOfFixedWidthStartAndOnePercentIsWrittenOverX() {
        // The characters before the % are at most 8, the characters of x: the pattern is written as it stands.
        for (String pattern : List.of("1031_%", "103_%", "_031%", "10_1_%", "1031234_%", "_%")) {
            Assertions.assertEquals(like(X, pattern), written(like(C, pattern)), pattern);
        }
        // The same over the first characters of c, when the characters before the % are at most the length of the prefix.
        Assertions.assertEquals(like(X, "1031_%"), written(like(left(5), "1031_%")));
        Assertions.assertEquals(like(X, "10_%"), written(like(left(5), "10_%")));
        Assertions.assertEquals(like(X, "10_%"), written(like(left(3), "10_%")));
        Assertions.assertEquals(like(X, "1_%"), written(like(left(2), "1_%")));
        Assertions.assertEquals(like(X, "_%"), written(like(left(1), "_%")));
        // A pattern of 9 characters before the % is more than x holds.
        Assertions.assertNull(written(like(C, "10312345_%")));
        Assertions.assertNull(written(like(C, "_________%")));
        // A string of 3 characters has no 5 characters to match: false on every value, as for the pattern of a prefix.
        Assertions.assertEquals(ConstantOperator.createBoolean(false), written(like(left(3), "1031_%")));
        Assertions.assertEquals(ConstantOperator.createBoolean(false), written(like(left(4), "103__%")));
        Assertions.assertEquals(ConstantOperator.createBoolean(false), written(like(left(1), "1_%")));
    }

    @Test
    public void testPatternOfFixedWidthStartIsEstimatedOverX() {
        // 600 rows start with 103, 300 with 104; the strings of the head have 8 characters.
        Assertions.assertEquals(600, rows(like(C, "1031_%")), 1e-6);
        Assertions.assertEquals(600, rows(like(C, "103_%")), 1e-6);
        Assertions.assertEquals(600, rows(like(C, "_031%")), 1e-6);
        Assertions.assertEquals(600, rows(like(C, "1031234_%")), 1e-6);
        Assertions.assertEquals(300, rows(like(C, "104_%")), 1e-6);
        Assertions.assertEquals(900, rows(like(C, "10_%")), 1e-6);
        Assertions.assertEquals(900, rows(like(C, "_%")), 1e-6);
        // No value of the head matches: one row, the floor of an estimate.
        Assertions.assertEquals(1, rows(like(C, "105_%")), 1e-6);
        Assertions.assertEquals(1, rows(like(C, "1031236_%")), 1e-6);
        // The first characters of c.
        Assertions.assertEquals(600, rows(like(left(5), "1031_%")), 1e-6);
        Assertions.assertEquals(900, rows(like(left(3), "10_%")), 1e-6);
        Assertions.assertEquals(1, rows(like(left(3), "1031_%")), 1e-6);
        // The negation leaves the NULL rows out: 1000 - 100 - 600.
        Assertions.assertEquals(300, rows(not(like(C, "1031_%"))), 1e-6);
        // A pattern with more characters than x holds is estimated as before.
        Assertions.assertEquals(plainRows(like(C, "10312345_%")), rows(like(C, "10312345_%")), 1e-6);
    }

    @Test
    public void testPatternOfFixedWidthStartIsNotAboveThePrefixItStartsWith() {
        // Without the statistic on x the estimate of 'p_%' is the default share of the rows, which
        // passes the 1 row of the prefix that no value of the head starts with; over x it is the rows of the head.
        Assertions.assertEquals(250, plainRows(like(C, "1031_%")), 1e-6);
        Assertions.assertEquals(250, plainRows(like(C, "105_%")), 1e-6);
        for (String prefix : List.of("1031", "104", "103", "105")) {
            Assertions.assertTrue(rows(like(C, prefix + "_%")) <= rows(like(C, prefix + "%")), prefix);
        }
        Assertions.assertEquals(1, rows(like(C, "105%")), 1e-6);
        Assertions.assertEquals(1, rows(like(C, "105_%")), 1e-6);
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

    private static ScalarOperator regexp(ScalarOperator left, String pattern) {
        return new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, left,
                ConstantOperator.createVarchar(pattern));
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

    // substr(x, 1, n) IN (prefixes of n characters)
    private static ScalarOperator prefixIn(int length, String... prefixes) {
        List<ScalarOperator> children = new ArrayList<>();
        children.add(PrefixDerivationRule.substr(X, 1, length));
        for (String prefix : prefixes) {
            children.add(ConstantOperator.createVarchar(prefix));
        }
        return new InPredicateOperator(false, children.toArray(new ScalarOperator[0]));
    }

    @Test
    public void testRegexpOfAnchoredPrefixesIsTheOrOfTheirLikeConditions() {
        ScalarOperator one = regexp(C, "^104");
        ScalarOperator onX = written(one);
        Assertions.assertTrue(onX instanceof LikePredicateOperator, String.valueOf(onX));
        Assertions.assertFalse(((LikePredicateOperator) onX).isRegexp());
        Assertions.assertEquals(X, onX.getChild(0));
        Assertions.assertEquals(rows(like(C, "104%")), rows(one), 1e-6);
        Assertions.assertEquals(300, rows(one), 1e-6);
        // Two prefixes: 600 + 300 rows, as for the OR of the two LIKE conditions, whose parts have no row in common.
        ScalarOperator two = regexp(C, "^103|^104");
        ScalarOperator likes = Utils.compoundOr(like(C, "103%"), like(C, "104%"));
        Assertions.assertEquals(900, rows(likes), 1e-6);
        Assertions.assertEquals(rows(likes), rows(two), 1e-6);
        // The LIKE conditions of prefixes of 3 characters are the one set of them: the prefix IN.
        Assertions.assertEquals(prefixIn(3, "103", "104"), written(two));
        // The prefix of the column, the one that no value has, and the one longer than x.
        Assertions.assertEquals(rows(Utils.compoundOr(like(left(5), "103%"), like(left(5), "104%"))),
                rows(regexp(left(5), "^103|^104")), 1e-6);
        Assertions.assertEquals(ConstantOperator.createBoolean(false), written(regexp(left(3), "^1031")));
        Assertions.assertEquals(1, rows(regexp(left(3), "^1031")), 1e-6);
        // The prefix that x cannot tell stays on the column in the OR of the LIKE conditions, as in any OR.
        Assertions.assertEquals(List.of(like(X, "103%"), like(C, "1031234567%")),
                Utils.extractDisjunctive(written(regexp(C, "^103|^1031234567"))));
        Assertions.assertEquals(plainRows(like(C, "1031234567%")), rows(regexp(C, "^1031234567")), 1e-6);
        // NOT keeps the rows that are not NULL, as for the LIKE.
        Assertions.assertEquals(rows(not(like(C, "104%"))), rows(not(one)), 1e-6);
        Assertions.assertEquals(600, rows(not(one)), 1e-6);
    }

    // What ConsolidateLikesRule makes of the predicate.
    private static ScalarOperator consolidated(ScalarOperator predicate) {
        if (ConnectContext.get() == null) {
            new ConnectContext().setThreadLocalInfo();
        }
        return ConsolidateLikesRule.INSTANCE.apply(predicate, null);
    }

    @Test
    public void testOrOfLikeConditionsWithSeveralPercentsIsTheOrOfThoseOfThePrefixes() {
        // The optimizer writes the OR of LIKE conditions with several % at the end as one REGEXP, whose groups end with
        // .*.*, and the estimate of it is the one of the OR of the LIKE conditions of the prefixes: 600 and 300 rows.
        ScalarOperator percents = Utils.compoundOr(like(C, "103%%"), like(C, "104%%"));
        ScalarOperator regexp = consolidated(percents);
        Assertions.assertEquals(regexp(C, "^((103.*.*)|(104.*.*))$"), regexp);
        ScalarOperator prefixes = Utils.compoundOr(like(C, "103%"), like(C, "104%"));
        Assertions.assertEquals(900, rows(prefixes), 1e-6);
        Assertions.assertEquals(900, rows(regexp), 1e-6);
        Assertions.assertEquals(900, rows(percents), 1e-6);
        Assertions.assertEquals(written(prefixes), written(regexp));
        // Without the statistic on x they are the same too, the estimate of the LIKE conditions of the prefixes.
        Assertions.assertEquals(plainRows(prefixes), plainRows(regexp), 1e-6);
        // The other prefixes: one of them has no row, the other 600.
        Assertions.assertEquals(rows(Utils.compoundOr(like(C, "103%"), like(C, "105%"))),
                rows(consolidated(Utils.compoundOr(like(C, "103%%"), like(C, "105%%")))), 1e-6);
        // The negation of them is the AND of the negations, that the rule writes as the NOT of the REGEXP: the rows that
        // are not NULL and start with neither, 1000 - 100 - 900 and the floor of an estimate.
        ScalarOperator notPercents = Utils.compoundAnd(not(like(C, "103%%")), not(like(C, "104%%")));
        ScalarOperator notRegexp = consolidated(notPercents);
        Assertions.assertEquals(not(regexp(C, "^((103.*.*)|(104.*.*))$")), notRegexp);
        ScalarOperator notPrefixes = Utils.compoundAnd(not(like(C, "103%")), not(like(C, "104%")));
        Assertions.assertEquals(rows(notPrefixes), rows(notRegexp), 1e-6);
        Assertions.assertEquals(rows(notPrefixes), rows(notPercents), 1e-6);
        // The prefixes of the first characters of c.
        ScalarOperator left = Utils.compoundOr(like(left(5), "103%"), like(left(5), "104%"));
        Assertions.assertEquals(rows(left), rows(Utils.compoundOr(like(left(5), "103%%"), like(left(5), "104%%"))), 1e-6);
    }

    @Test
    public void testOrOfLikeConditionsOverXAddsTheRowsOfPrefixesThatDoNotStartOneWithAnother() {
        // The head: 600 rows of 10312345 and 300 of 10400000. Prefixes that do not start one with another have no row in
        // common: 103 and 104 are 600 + 300, and 103 and the string 104 of x are 600 + 1, the one row of an estimate that
        // finds none.
        Assertions.assertEquals(900, rows(Utils.compoundOr(like(C, "103%"), like(C, "104%"))), 1e-6);
        Assertions.assertEquals(900, rows(Utils.compoundOr(like(C, "104%"), like(C, "103%"))), 1e-6);
        Assertions.assertEquals(601, rows(Utils.compoundOr(like(C, "103%"), compare(BinaryType.EQ, "104"))), 1e-6);
        // 103 is a prefix of 1031, and the string 103 starts with 103: the second part has rows among those of the first.
        Assertions.assertEquals(600, rows(Utils.compoundOr(like(C, "103%"), like(C, "1031%"))), 1e-6);
        Assertions.assertEquals(600, rows(Utils.compoundOr(like(C, "1031%"), like(C, "103%"))), 1e-6);
        Assertions.assertEquals(600, rows(Utils.compoundOr(like(C, "103%"), compare(BinaryType.EQ, "103"))), 1e-6);
    }

    @Test
    public void testPartsWrittenAsFalseAreLeftOut() {
        ScalarOperator no = ConstantOperator.createBoolean(false);
        // A prefix longer than the 5 characters of the left side is no value of it, false on every value of x, and adds
        // no row to the OR: the REGEXP is the one LIKE of the other prefix, wherever the false part stands.
        ScalarOperator regexp = regexp(left(5), "^103|^1031234567");
        Assertions.assertEquals(like(X, "103%"), written(regexp));
        Assertions.assertEquals(like(X, "103%"), written(regexp(left(5), "^1031234567|^103")));
        Assertions.assertEquals(prefixIn(3, "103", "104"), written(regexp(left(5), "^103|^1031234567|^104")));
        Assertions.assertEquals(600, rows(like(left(5), "103%")), 1e-6);
        Assertions.assertEquals(600, rows(regexp), 1e-6);
        Assertions.assertEquals(900, rows(regexp(left(5), "^103|^1031234567|^104")), 1e-6);
        // An OR of only false parts is false, and its negation holds on the rows where x is not NULL.
        ScalarOperator onlyFalse = regexp(left(3), "^1031|^1032");
        Assertions.assertEquals(no, written(onlyFalse));
        Assertions.assertEquals(1, rows(onlyFalse), 1e-6);
        ScalarOperator negated = written(not(onlyFalse));
        Assertions.assertTrue(negated instanceof IsNullPredicateOperator && ((IsNullPredicateOperator) negated).isNotNull());
        Assertions.assertEquals(900, rows(not(onlyFalse)), 1e-6);
        // The false parts of an OR with a part on the column, and of an OR of LIKE conditions.
        Assertions.assertEquals(like(X, "104%"), written(Utils.compoundOr(like(left(3), "1031%"), like(C, "104%"))));
        // A conjunction with a false part is false, and is left out of the OR as a false part.
        ScalarOperator falseAnd = Utils.compoundAnd(like(left(3), "1031%"), like(C, "104%"));
        Assertions.assertEquals(like(X, "103%"), written(Utils.compoundOr(falseAnd, like(C, "103%"))));
        Assertions.assertEquals(prefixIn(3, "103", "104"),
                written(Utils.compoundOr(Utils.compoundOr(falseAnd, like(C, "103%")), like(C, "104%"))));
        Assertions.assertEquals(600, rows(Utils.compoundOr(falseAnd, like(C, "103%"))), 1e-6);
        // The negation of a conjunction with a false part is not the rows where x is not NULL: on the rows where x is
        // NULL it holds when the other part is false, so it stays the negation of false.
        Assertions.assertEquals(not(no), written(not(falseAnd)));
        // Nothing false: the parts are written, and the prefixes of 3 characters are the one set of them.
        Assertions.assertEquals(prefixIn(3, "103", "104"),
                written(Utils.compoundOr(like(left(5), "103%"), like(C, "104%"))));
    }

    @Test
    public void testRegexpThatIsNotAnchoredPrefixesIsEstimatedAsBefore() {
        for (String pattern : List.of("^103$", "103", "^1(03)", "^103.*", "^103|104", "^[0-9]", "^\\d", "", "^103|",
                "|^103", "^1+03", "^1{2}", "^1?03", "^10\\x", "^10^3")) {
            ScalarOperator regexp = regexp(C, pattern);
            Assertions.assertNull(written(regexp), pattern);
            Assertions.assertEquals(plainRows(regexp), rows(regexp), 1e-6, pattern);
            // The default of a predicate that is not understood: a quarter of the rows.
            Assertions.assertEquals(250, rows(regexp), 1e-6, pattern);
        }
    }

    @Test
    public void testRegexpOfAnchoredPrefixesOnTheStatisticsOfTheColumn() {
        // No statistics on x: the column has the head of its own, 600 and 300 rows of two values, and 100 NULL.
        Histogram histogram = Histogram.forStrings(List.of(), Map.of("10312345", 600L, "10400000", 300L));
        Statistics column = Statistics.builder().setOutputRowCount(1000).addColumnStatistic(C,
                ColumnStatistic.builder().setMinValue(Double.NEGATIVE_INFINITY).setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(0.1).setAverageRowSize(8).setDistinctValuesCount(2).setHistogram(histogram)
                        .build()).build();
        ScalarOperator likes = Utils.compoundOr(like(C, "103%"), like(C, "104%"));
        double expected = PredicateStatisticsCalculator.statisticsCalculate(likes, column).getOutputRowCount();
        Assertions.assertEquals(900, expected, 1e-6);
        Assertions.assertEquals(expected, PredicateStatisticsCalculator.statisticsCalculate(regexp(C, "^103|^104"), column)
                .getOutputRowCount(), 1e-6);
        Assertions.assertEquals(300, PredicateStatisticsCalculator.statisticsCalculate(regexp(C, "^104"), column)
                .getOutputRowCount(), 1e-6);
        // The prefixes of other expressions of the column: the LIKE conditions on the same left side.
        Assertions.assertEquals(PredicateStatisticsCalculator.statisticsCalculate(
                Utils.compoundOr(like(left(3), "103%"), like(left(3), "104%")), column).getOutputRowCount(),
                PredicateStatisticsCalculator.statisticsCalculate(regexp(left(3), "^103|^104"), column).getOutputRowCount(),
                1e-6);
    }

    // The accounts of the queries: 20 digits, a statistic on x = substr(number, 1, 8) with the head of three prefixes.
    private static ScanExpressionStatistics numberExpressions() {
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        entries.add(new MultiColumnCombinedStats.McvEntry(List.of("42894972"), 700, List.of(700L)));
        entries.add(new MultiColumnCombinedStats.McvEntry(List.of("40406972"), 200, List.of(200L)));
        entries.add(new MultiColumnCombinedStats.McvEntry(List.of("10100001"), 100, List.of(100L)));
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("x"), 1000, 3, entries, List.of(),
                List.of(0L));
        group.prepare(VarcharType.VARCHAR);
        ColumnStatistic statistic = group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(left(8));
        return new ScanExpressionStatistics("t", List.of(new ScanExpressionStatistics.Entry(canonical,
                ExpressionStatisticsCanonicalizer.keyOf(canonical), X, statistic)),
                Map.of(Set.of(X), new MultiColumnCombinedStats(3, 1000, List.of(X), entries, List.of(0L))));
    }

    @Test
    public void testRegexpOfTheQueriesOnAccountNumbers() {
        // a.number REGEXP '^42894|^40406': 700 and 200 rows of the head, and no row has both prefixes.
        Statistics numbers = Statistics.builder().setOutputRowCount(1000).addColumnStatistic(C,
                new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 20, 1000)).build();
        ScalarOperator regexp = regexp(C, "^42894|^40406");
        ScalarOperator likes = Utils.compoundOr(like(C, "42894%"), like(C, "40406%"));
        ScalarOperator prefixes = new InPredicateOperator(false, left(5), ConstantOperator.createVarchar("42894"),
                ConstantOperator.createVarchar("40406"));
        double regexpRows = PredicateStatisticsCalculator.statisticsCalculate(regexp, numbers, true, numberExpressions())
                .getOutputRowCount();
        Assertions.assertEquals(900, regexpRows, 1e-6);
        Assertions.assertEquals(PredicateStatisticsCalculator.statisticsCalculate(likes, numbers, true,
                numberExpressions()).getOutputRowCount(), regexpRows, 1e-6);
        Assertions.assertEquals(900, PredicateStatisticsCalculator.statisticsCalculate(prefixes, numbers, true,
                numberExpressions()).getOutputRowCount(), 1e-6);
        // Without the statistic on x the REGEXP is estimated as the LIKE conditions of its prefixes.
        Assertions.assertEquals(PredicateStatisticsCalculator.statisticsCalculate(likes, numbers, true)
                .getOutputRowCount(), PredicateStatisticsCalculator.statisticsCalculate(regexp, numbers, true)
                .getOutputRowCount(), 1e-6);
        // Written over x as the one set of the two prefixes of 5 characters: the prefix IN.
        ExpressionStatisticsMatcher.Result result = ExpressionStatisticsMatcher.match(
                EstimatePredicateNormalizer.normalize(regexp, numbers), numberExpressions(), numbers);
        Assertions.assertEquals(prefixIn(5, "42894", "40406"), result.getPredicate());
    }
}
