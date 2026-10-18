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
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExpressionStatisticsMatcherTest {
    private static final ColumnRefOperator C = new ColumnRefOperator(1, VarcharType.VARCHAR, "c", true);
    private static final ColumnRefOperator D = new ColumnRefOperator(2, IntegerType.INT, "d", true);
    private static final ColumnRefOperator X = new ColumnRefOperator(-1, VarcharType.VARCHAR, "expr#1", true);

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.createDefaultCtx();
    }

    private static ScalarOperator call(String name, Type returnType, ScalarOperator... arguments) {
        Type[] types = new Type[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            types[i] = arguments[i].getType();
        }
        Function function = ExprUtils.getBuiltinFunction(name, types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(name, returnType, List.of(arguments), function);
    }

    private static ScalarOperator left(int length) {
        return call("left", VarcharType.VARCHAR, C, ConstantOperator.createInt(length));
    }

    private static ScalarOperator substr(int start, int length) {
        return call("substr", VarcharType.VARCHAR, C, ConstantOperator.createInt(start),
                ConstantOperator.createInt(length));
    }

    private static ConstantOperator text(String value) {
        return ConstantOperator.createVarchar(value);
    }

    private static ScalarOperator eq(ScalarOperator left, ScalarOperator right) {
        return new BinaryPredicateOperator(BinaryType.EQ, left, right);
    }

    private static ScalarOperator like(ScalarOperator left, String pattern) {
        return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, left, text(pattern));
    }

    private static ScanExpressionStatistics.Entry entry(ScalarOperator expression) {
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(expression);
        return new ScanExpressionStatistics.Entry(canonical, ExpressionStatisticsCanonicalizer.keyOf(canonical), X,
                new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 100));
    }

    private static MultiColumnCombinedStats group(List<ColumnRefOperator> columns, List<String> values) {
        return new MultiColumnCombinedStats(10, 1000, columns,
                List.of(new MultiColumnCombinedStats.McvEntry(values, 100,
                        java.util.Collections.nCopies(values.size(), 100L))),
                java.util.Collections.nCopies(columns.size(), 0L));
    }

    private static ScanExpressionStatistics expressions(ScalarOperator expression, boolean withD) {
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups = new HashMap<>();
        groups.put(Set.of(X), group(List.of(X), List.of("10312345")));
        if (withD) {
            groups.put(Set.of(X, D), group(List.of(X, D), List.of("10312345", "5")));
        }
        return new ScanExpressionStatistics("t", List.of(entry(expression)), groups);
    }

    private static Statistics statistics(ColumnStatistic c, Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups) {
        return Statistics.builder().setOutputRowCount(1000).addColumnStatistic(C, c)
                .addColumnStatistic(D, new ColumnStatistic(0, 10, 0, 4, 10)).addMultiColumnStatistics(groups).build();
    }

    private static Statistics plain() {
        return statistics(new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 1000),
                Map.of());
    }

    private static ScalarOperator rewrite(ScalarOperator predicate) {
        ExpressionStatisticsMatcher.Result result =
                ExpressionStatisticsMatcher.match(predicate, expressions(left(8), false), plain());
        return result == null ? null : result.getPredicate();
    }

    @Test
    public void testDirectMatch() {
        ScalarOperator lower = call("lower", VarcharType.VARCHAR, C);
        ScalarOperator predicate = new InPredicateOperator(false, lower, text("a"), text("b"));
        ExpressionStatisticsMatcher.Result result =
                ExpressionStatisticsMatcher.match(predicate, expressions(lower, false), plain());
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.getPredicate() instanceof InPredicateOperator);
        Assertions.assertEquals(X, result.getPredicate().getChild(0));
        Assertions.assertEquals(Set.of(X), result.getUsedColumns());
        Assertions.assertEquals(Set.of(X), result.getFilteredColumns());
        // The same expression of another spelling matches through the canonical form.
        ScalarOperator upperOfLower = eq(call("upper", VarcharType.VARCHAR, lower), text("A"));
        ScalarOperator rewritten = ExpressionStatisticsMatcher.match(upperOfLower, expressions(lower, false), plain())
                .getPredicate();
        Assertions.assertEquals(X, rewritten.getChild(0).getChild(0));
    }

    @Test
    public void testPrefixRule() {
        ScalarOperator rewritten = rewrite(like(C, "103%"));
        Assertions.assertTrue(rewritten instanceof LikePredicateOperator);
        Assertions.assertEquals(X, rewritten.getChild(0));
        Assertions.assertNull(rewrite(like(C, "123456789%")));
        // Characters of fixed width and one % are written over x as they stand; a % among them, or no % at the end, is not.
        rewritten = rewrite(like(C, "1_3%"));
        Assertions.assertTrue(rewritten instanceof LikePredicateOperator);
        Assertions.assertEquals(X, rewritten.getChild(0));
        Assertions.assertEquals("1_3%", ((ConstantOperator) rewritten.getChild(1)).getVarchar());
        Assertions.assertNull(rewrite(like(C, "1%3%")));
        Assertions.assertNull(rewrite(like(C, "1_3")));

        rewritten = rewrite(eq(left(3), text("103")));
        Assertions.assertTrue(rewritten instanceof LikePredicateOperator);
        Assertions.assertEquals("103%", ((ConstantOperator) rewritten.getChild(1)).getVarchar());
        rewritten = rewrite(eq(left(3), text("1%")));
        Assertions.assertTrue(rewritten instanceof BinaryPredicateOperator);
        Assertions.assertEquals(X, rewritten.getChild(0));
        Assertions.assertEquals(ConstantOperator.createBoolean(false), rewrite(eq(left(3), text("1034"))));
        Assertions.assertEquals("1\\%\\_%", ((ConstantOperator) rewrite(eq(left(3), text("1%_"))).getChild(1))
                .getVarchar());
        // Length in characters: three Cyrillic letters are one prefix of length 3.
        Assertions.assertTrue(rewrite(eq(left(3), text("абв"))) instanceof LikePredicateOperator);

        rewritten = rewrite(eq(left(8), text("10312345")));
        Assertions.assertEquals(X, rewritten.getChild(0));
        Assertions.assertTrue(rewritten instanceof BinaryPredicateOperator);

        rewritten = rewrite(new InPredicateOperator(false, substr(1, 3), text("101"), text("102")));
        Assertions.assertTrue(rewritten instanceof InPredicateOperator);
        Assertions.assertEquals("substr", ((CallOperator) rewritten.getChild(0)).getFnName());
        Assertions.assertEquals(X, rewritten.getChild(0).getChild(0));

        rewritten = rewrite(new BinaryPredicateOperator(BinaryType.NE, left(3), text("103")));
        Assertions.assertTrue(rewritten instanceof CompoundPredicateOperator
                && ((CompoundPredicateOperator) rewritten).isNot());

        rewritten = rewrite(new BinaryPredicateOperator(BinaryType.LE, left(3), text("103")));
        Assertions.assertTrue(rewritten instanceof CompoundPredicateOperator
                && ((CompoundPredicateOperator) rewritten).isOr());
        rewritten = rewrite(new BinaryPredicateOperator(BinaryType.LT, left(3), text("103")));
        Assertions.assertEquals(X, rewritten.getChild(0));

        rewritten = rewrite(eq(substr(4, 2), text("45")));
        Assertions.assertEquals("substr", ((CallOperator) rewritten.getChild(0)).getFnName());
        Assertions.assertEquals(ConstantOperator.createInt(4), rewritten.getChild(0).getChild(1));
        Assertions.assertNull(rewrite(eq(substr(8, 2), text("45"))));
        Assertions.assertNull(rewrite(eq(call("substr", VarcharType.VARCHAR, C, ConstantOperator.createInt(2)),
                text("45"))));
        // The column itself against a constant of fewer than 8 characters is x against it, and against one of 8 or
        // more characters it is not written.
        Assertions.assertEquals(X, rewrite(eq(C, text("1031234"))).getChild(0));
        Assertions.assertNull(rewrite(eq(C, text("10312345"))));
        Assertions.assertNull(rewrite(eq(C, text("1031234567"))));
        Assertions.assertNull(rewrite(new IsNullPredicateOperator(true, C)));
    }

    @Test
    public void testCompoundConditions() {
        ScalarOperator or = Utils.compoundOr(like(C, "101%"), like(C, "102%"));
        ExpressionStatisticsMatcher.Result result =
                ExpressionStatisticsMatcher.match(or, expressions(left(8), false), plain());
        // The prefixes of 3 characters of one OR are one prefix IN, so no OR is left over x.
        Assertions.assertFalse(result.hasOr());
        Assertions.assertFalse(Utils.extractColumnRef(result.getPredicate()).contains(C));
        ScalarOperator not = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, like(C, "1%"));
        Assertions.assertEquals(X, rewrite(not).getChild(0).getChild(0));
    }

    @Test
    public void testPartsOfAnOrThatCannotBeWrittenStayAsTheyAre() {
        ScalarOperator other = eq(D, ConstantOperator.createInt(5));
        ScalarOperator number = eq(C, text("26226972800600000205"));
        ScalarOperator prefix = like(C, "101%");
        // The part over x is written, the other stays the same object. The part over x goes first whatever the order of
        // the query is, so the estimate of the OR does not depend on it.
        ScalarOperator written = rewrite(Utils.compoundOr(prefix, other));
        Assertions.assertEquals(X, written.getChild(0).getChild(0));
        Assertions.assertSame(other, written.getChild(1));
        written = rewrite(Utils.compoundOr(other, prefix));
        Assertions.assertEquals(X, written.getChild(0).getChild(0));
        Assertions.assertSame(other, written.getChild(1));

        // The part on c itself stays on c: x does not know how its values relate to the number.
        written = rewrite(Utils.compoundOr(eq(left(5), text("20202")), number));
        Assertions.assertEquals(X, Utils.extractColumnRef(written.getChild(0)).get(0));
        Assertions.assertSame(number, written.getChild(1));

        // Three parts, one over x in the middle: it goes first, the others keep their order.
        List<ScalarOperator> parts = Utils.extractDisjunctive(rewrite(Utils.compoundOr(Utils.compoundOr(other, prefix),
                number)));
        Assertions.assertEquals(3, parts.size());
        Assertions.assertEquals(X, Utils.extractColumnRef(parts.get(0)).get(0));
        Assertions.assertSame(other, parts.get(1));
        Assertions.assertSame(number, parts.get(2));

        // Parts at any depth: an AND under the OR, and a NOT over the OR.
        ScalarOperator and = Utils.compoundAnd(prefix, other);
        written = rewrite(Utils.compoundOr(and, number));
        Assertions.assertEquals(X, Utils.extractColumnRef(written.getChild(0).getChild(0)).get(0));
        Assertions.assertSame(other, written.getChild(0).getChild(1));
        Assertions.assertSame(number, written.getChild(1));
        written = rewrite(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                Utils.compoundOr(prefix, number)));
        Assertions.assertTrue(((CompoundPredicateOperator) written).isNot());
        Assertions.assertEquals(X, Utils.extractColumnRef(written.getChild(0).getChild(0)).get(0));
        Assertions.assertSame(number, written.getChild(0).getChild(1));

        // An atom that reads x and another column cannot be written over x alone, so it stays whole.
        ScalarOperator both = eq(call("concat", VarcharType.VARCHAR, left(3), call("upper", VarcharType.VARCHAR, C)),
                text("q"));
        written = rewrite(Utils.compoundOr(eq(left(3), text("103")), both));
        Assertions.assertSame(both, written.getChild(1));

        // Nothing is written when no part is: the conjunct is estimated as before.
        Assertions.assertNull(rewrite(Utils.compoundOr(other, number)));
        Assertions.assertNull(rewrite(Utils.compoundOr(number, both)));
    }

    @Test
    public void testFlagsOfAConditionWithAPartOnAColumn() {
        ScalarOperator other = eq(D, ConstantOperator.createInt(5));
        ScalarOperator number = eq(C, text("26226972800600000205"));
        ScalarOperator prefix = like(C, "101%");
        ExpressionStatisticsMatcher.Result mixed = ExpressionStatisticsMatcher.match(Utils.compoundOr(prefix, other),
                expressions(left(8), false), plain());
        // x is read, and it describes the rows: the estimate of the OR merges its statistics of the parts.
        Assertions.assertEquals(Set.of(X), mixed.getUsedColumns());
        Assertions.assertEquals(Set.of(X), mixed.getFilteredColumns());
        // The part on d holds rows of any value of x, so the distribution of x after the OR is rebuilt from the whole one.
        Assertions.assertTrue(mixed.hasOr());
        // The part on c leaves its own NULL rows out, so the estimate over x does not leave them out for it.
        mixed = ExpressionStatisticsMatcher.match(Utils.compoundOr(prefix, number), expressions(left(8), false), plain());
        Assertions.assertEquals(Set.of(C), mixed.getNonNullColumns());
        Assertions.assertTrue(mixed.getNullRowsLeftOutColumns().isEmpty());
        // Over x alone the NULL rows of c are left out of the share of x.
        ExpressionStatisticsMatcher.Result pure = ExpressionStatisticsMatcher.match(Utils.compoundOr(prefix,
                like(C, "102%")), expressions(left(8), false), plain());
        Assertions.assertFalse(pure.hasOr());
        Assertions.assertEquals(Set.of(C), pure.getNullRowsLeftOutColumns());
    }

    @Test
    public void testServiceConditionsAndNullChecks() {
        // A bound kept to skip files is not estimated and stays in the copy as it is.
        ScalarOperator bound = new BinaryPredicateOperator(BinaryType.GE, C, text("1"));
        bound.setNotEvalEstimate(true);
        ScalarOperator rewritten = rewrite(Utils.compoundAnd(eq(left(3), text("103")), bound));
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(rewritten);
        Assertions.assertEquals(2, conjuncts.size());
        Assertions.assertSame(bound, conjuncts.get(1));

        // The NULL check of a join key follows from a strict condition on the prefix.
        rewritten = rewrite(Utils.compoundAnd(new IsNullPredicateOperator(true, C), eq(left(3), text("103"))));
        Assertions.assertTrue(rewritten instanceof LikePredicateOperator);
        // Alone, it is estimated as before.
        Assertions.assertNull(rewrite(new IsNullPredicateOperator(true, C)));
        // A predicate without a column of an expression is not matched at all.
        Assertions.assertNull(rewrite(eq(D, ConstantOperator.createInt(5))));
    }

    @Test
    public void testSourceOfConditionsOnTheColumn() {
        ScalarOperator prefix = eq(left(3), text("103"));
        // A constant of more than 8 characters is not compared over x, so this condition stays on c.
        ScalarOperator other = new BinaryPredicateOperator(BinaryType.NE, C, text("1039999999"));
        ScalarOperator predicate = Utils.compoundAnd(prefix, other);
        // No source counts both: the prefix goes to x, the other condition stays, x does not describe the rows.
        ExpressionStatisticsMatcher.Result result =
                ExpressionStatisticsMatcher.match(predicate, expressions(left(8), false), plain());
        Assertions.assertEquals(Set.of(X), result.getUsedColumns());
        Assertions.assertTrue(Utils.extractConjuncts(result.getPredicate()).contains(other));
        Assertions.assertTrue(result.getFilteredColumns().isEmpty());

        // The head of MCV(c) counts both: the prefix stays on c in the form the FE folds.
        Statistics withGroup = statistics(new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                0, 8, 1000), Map.of(Set.of(C), group(List.of(C), List.of("10312345"))));
        result = ExpressionStatisticsMatcher.match(predicate, expressions(left(8), false), withGroup);
        Assertions.assertTrue(result.getUsedColumns().isEmpty());
        ScalarOperator first = Utils.extractConjuncts(result.getPredicate()).get(0);
        Assertions.assertEquals("substr", ((CallOperator) first.getChild(0)).getFnName());

        // The string histogram of c counts a LIKE prefix and a range on c together.
        Histogram histogram = Histogram.forStrings(List.<Bucket>of(new StringBucket("100", "199", 900, 1, 90)),
                Map.of("10312345", 100L));
        Statistics withHistogram = statistics(new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                0, 8, 1000, histogram, ColumnStatistic.StatisticType.ESTIMATE), Map.of());
        Assertions.assertNull(ExpressionStatisticsMatcher.match(
                Utils.compoundAnd(like(C, "103%"), new BinaryPredicateOperator(BinaryType.NE, C, text("1035999999"))),
                expressions(left(8), false), withHistogram));
        // A range of at most 8 characters is written over x with the prefix, and x counts both.
        Assertions.assertEquals(Set.of(X), ExpressionStatisticsMatcher.match(
                Utils.compoundAnd(like(C, "103%"), new BinaryPredicateOperator(BinaryType.LT, C, text("1035"))),
                expressions(left(8), false), withHistogram).getUsedColumns());
        // Without the range, x counts the prefix.
        Assertions.assertNotNull(ExpressionStatisticsMatcher.match(like(C, "103%"), expressions(left(8), false),
                withHistogram));
    }

    @Test
    public void testSourceOfConditionsOnAnotherColumn() {
        ScalarOperator predicate = Utils.compoundAnd(eq(substr(1, 3), text("103")), eq(D, ConstantOperator.createInt(5)));
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> cd =
                Map.of(Set.of(C, D), group(List.of(C, D), List.of("10312345", "5")));
        Statistics withCd = statistics(new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                0, 8, 1000), cd);
        // MCV(c, d) counts both conditions; x alone does not count d.
        ExpressionStatisticsMatcher.Result result =
                ExpressionStatisticsMatcher.match(predicate, expressions(left(8), false), withCd);
        Assertions.assertTrue(result.getUsedColumns().isEmpty());
        // MCV(left(c, 8), d) counts both too, and x is taken.
        result = ExpressionStatisticsMatcher.match(predicate, expressions(left(8), true), withCd);
        Assertions.assertEquals(Set.of(X), result.getUsedColumns());
        // A condition on the whole c is counted only by MCV(c, d).
        ScalarOperator withIn = Utils.compoundAnd(predicate,
                new InPredicateOperator(false, C, text("1031234567"), text("1040000000")));
        result = ExpressionStatisticsMatcher.match(withIn, expressions(left(8), true), withCd);
        Assertions.assertTrue(result.getUsedColumns().isEmpty());
    }

    @Test
    public void testDateFunctionConditions() {
        ColumnRefOperator epoch = new ColumnRefOperator(3, IntegerType.BIGINT, "e", true);
        ScalarOperator render = new CallOperator("from_unixtime", VarcharType.VARCHAR, List.of(epoch),
                new Function(new com.starrocks.catalog.FunctionName("from_unixtime"), new Type[] {IntegerType.BIGINT},
                        VarcharType.VARCHAR, false));
        ScalarOperator after = new BinaryPredicateOperator(BinaryType.GE, render, text("2024-12-01 00:00:00"));
        Statistics statistics = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(epoch, new ColumnStatistic(1704067200, 1735689599, 0, 8, 1000)).build();
        ScanExpressionStatistics expressions = expressions(render, false);

        // Bounds on e from the function and a range on e itself: the statistics of e count both together.
        ScalarOperator range = new BinaryPredicateOperator(BinaryType.LT, epoch, ConstantOperator.createBigint(1733097600));
        Assertions.assertNull(ExpressionStatisticsMatcher.match(Utils.compoundAnd(after, range), expressions,
                statistics));
        // A condition on e that does not reduce to its axis is estimated as before, the function over x.
        ScalarOperator other = new BinaryPredicateOperator(BinaryType.EQ,
                call("abs", IntegerType.BIGINT, epoch), ConstantOperator.createBigint(5));
        ExpressionStatisticsMatcher.Result result =
                ExpressionStatisticsMatcher.match(Utils.compoundAnd(after, other), expressions, statistics);
        Assertions.assertEquals(Set.of(X), result.getUsedColumns());
        Assertions.assertTrue(Utils.extractConjuncts(result.getPredicate()).contains(other));
        // A bound derived only to skip files is not estimated and does not compete with x.
        ScalarOperator bound = new BinaryPredicateOperator(BinaryType.GE, epoch, ConstantOperator.createBigint(1733011200));
        bound.setNotEvalEstimate(true);
        result = ExpressionStatisticsMatcher.match(Utils.compoundAnd(after, bound), expressions, statistics);
        Assertions.assertEquals(Set.of(X), result.getUsedColumns());
        Assertions.assertEquals(X, Utils.extractConjuncts(result.getPredicate()).get(0).getChild(0));
    }

    @Test
    public void testPrefixRuleRemainingRowsAndRefusals() {
        // NOT IN of prefixes of one length is the negation of one prefix IN.
        ScalarOperator rewritten = rewrite(new InPredicateOperator(true, substr(1, 3), text("101"), text("102")));
        Assertions.assertTrue(rewritten instanceof CompoundPredicateOperator
                && ((CompoundPredicateOperator) rewritten).isNot());
        Assertions.assertTrue(rewritten.getChild(0) instanceof InPredicateOperator);
        Assertions.assertEquals(X, rewritten.getChild(0).getChild(0).getChild(0));
        // Constants of other lengths: a shorter one is the whole string, a longer one never matches; the prefixes of
        // the length stay one prefix IN.
        rewritten = rewrite(new InPredicateOperator(false, left(3), text("10"), text("103"), text("1034")));
        Assertions.assertTrue(rewritten instanceof CompoundPredicateOperator
                && ((CompoundPredicateOperator) rewritten).isOr());
        Assertions.assertTrue(rewritten.getChild(0) instanceof InPredicateOperator);
        Assertions.assertEquals(eq(X, text("10")), rewritten.getChild(1));
        Assertions.assertEquals(ConstantOperator.createBoolean(false),
                rewrite(new InPredicateOperator(false, left(3), text("1034"), text("1035"))));
        // Ranges on a prefix of the same length as the constant.
        rewritten = rewrite(new BinaryPredicateOperator(BinaryType.GE, left(3), text("103")));
        Assertions.assertEquals(BinaryType.GE, ((BinaryPredicateOperator) rewritten).getBinaryType());
        Assertions.assertEquals(X, rewritten.getChild(0));
        rewritten = rewrite(new BinaryPredicateOperator(BinaryType.GT, left(3), text("103")));
        Assertions.assertTrue(rewritten instanceof CompoundPredicateOperator
                && ((CompoundPredicateOperator) rewritten).isAnd());
        // A range with a shorter constant is the same range on the whole x; with a longer one it stays a range on
        // substr(x, 1, n).
        Assertions.assertEquals(new BinaryPredicateOperator(BinaryType.LT, X, text("10")),
                rewrite(new BinaryPredicateOperator(BinaryType.LT, left(3), text("10"))));
        rewritten = rewrite(new BinaryPredicateOperator(BinaryType.LT, left(3), text("1034")));
        Assertions.assertEquals("substr", ((CallOperator) rewritten.getChild(0)).getFnName());
        Assertions.assertEquals(ConstantOperator.createInt(3), rewritten.getChild(0).getChild(2));
        // Positions that are not constants or are negative, and right(), are not expressed.
        Assertions.assertNull(rewrite(eq(call("substr", VarcharType.VARCHAR, C, D, ConstantOperator.createInt(2)),
                text("10"))));
        Assertions.assertNull(rewrite(eq(substr(-2, 2), text("45"))));
        Assertions.assertNull(rewrite(eq(call("right", VarcharType.VARCHAR, C, ConstantOperator.createInt(2)),
                text("45"))));
        // A backslash is escaped in the LIKE pattern, and the length is counted in characters.
        Assertions.assertEquals("a\\\\b%", ((ConstantOperator) rewrite(eq(left(3), text("a\\b"))).getChild(1))
                .getVarchar());
        String emoji = new String(Character.toChars(0x1F600));
        Assertions.assertTrue(rewrite(eq(left(3), text(emoji + "ab"))) instanceof LikePredicateOperator);
        Assertions.assertEquals(ConstantOperator.createBoolean(false), rewrite(eq(left(2), text(emoji + "ab"))));
    }

    @Test
    public void testOneExpressionPerColumnTakesTheShortestPrefix() {
        ColumnRefOperator x4 = new ColumnRefOperator(-2, VarcharType.VARCHAR, "expr#2", true);
        ScalarOperator canonical4 = ExpressionStatisticsCanonicalizer.canonicalize(left(4));
        ScanExpressionStatistics.Entry entry4 = new ScanExpressionStatistics.Entry(canonical4,
                ExpressionStatisticsCanonicalizer.keyOf(canonical4), x4,
                new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 100));
        ScanExpressionStatistics both = new ScanExpressionStatistics("t", List.of(entry(left(8)), entry4),
                Map.of(Set.of(X), group(List.of(X), List.of("10312345")), Set.of(x4), group(List.of(x4), List.of("1031"))));
        ExpressionStatisticsMatcher.Result result = ExpressionStatisticsMatcher.match(like(C, "10%"), both, plain());
        Assertions.assertEquals(Set.of(x4), result.getUsedColumns());
        // A prefix longer than the short one is expressed only by the long one.
        result = ExpressionStatisticsMatcher.match(like(C, "10312%"), both, plain());
        Assertions.assertEquals(Set.of(X), result.getUsedColumns());
    }

    @Test
    public void testPrefixWithBothColumnAndExpressionHeads() {
        // MCV(c) and MCV(left(c, 8)) both count c LIKE '103%' alone; x is taken.
        Statistics withGroup = statistics(new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                0, 8, 1000), Map.of(Set.of(C), group(List.of(C), List.of("10312345"))));
        ExpressionStatisticsMatcher.Result result =
                ExpressionStatisticsMatcher.match(like(C, "103%"), expressions(left(8), false), withGroup);
        Assertions.assertEquals(Set.of(X), result.getUsedColumns());
    }

    @Test
    public void testNullCheckStaysNextToConditionsTrueOnNull() {
        ScalarOperator notNull = new IsNullPredicateOperator(true, C);
        // ifnull(c, 'u') = 'u' holds on the NULL rows of c, so c IS NOT NULL is a condition of its own.
        ScalarOperator ifnull = call("ifnull", VarcharType.VARCHAR, C, text("u"));
        ExpressionStatisticsMatcher.Result result = ExpressionStatisticsMatcher.match(
                Utils.compoundAnd(eq(ifnull, text("u")), notNull), expressions(ifnull, false), plain());
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(result.getPredicate());
        Assertions.assertEquals(2, conjuncts.size());
        Assertions.assertEquals(eq(X, text("u")), conjuncts.get(0));
        Assertions.assertSame(notNull, conjuncts.get(1));
        // lower(c) = 'a' is never true on NULL, so the check follows from it and leaves the copy.
        ScalarOperator lower = call("lower", VarcharType.VARCHAR, C);
        result = ExpressionStatisticsMatcher.match(Utils.compoundAnd(eq(lower, text("a")), notNull),
                expressions(lower, false), plain());
        Assertions.assertEquals(eq(X, text("a")), result.getPredicate());
    }

    @Test
    public void testWrittenConditionReadsOnlyTheVirtualColumn() {
        ColumnRefOperator e = new ColumnRefOperator(3, VarcharType.VARCHAR, "e", true);
        ScalarOperator lower = call("lower", VarcharType.VARCHAR, C);
        // concat(x, e) = 'q' would need how x relates to e, which the statistics of x do not know.
        ScalarOperator concat = eq(call("concat", VarcharType.VARCHAR, lower, e), text("q"));
        Assertions.assertNull(ExpressionStatisticsMatcher.match(concat, expressions(lower, false), plain()));
        // A prefix no value can have is a constant: x is not read and does not describe the rows after the filter.
        ExpressionStatisticsMatcher.Result result = ExpressionStatisticsMatcher.match(eq(left(3), text("1034")),
                expressions(left(8), false), plain());
        Assertions.assertEquals(ConstantOperator.createBoolean(false), result.getPredicate());
        Assertions.assertTrue(result.getUsedColumns().isEmpty());
        Assertions.assertTrue(result.getFilteredColumns().isEmpty());
    }

    @Test
    public void testNegationOfImpossiblePrefixIsNotNull() {
        ScalarOperator notNullOfX = new IsNullPredicateOperator(true, X);
        Assertions.assertEquals(notNullOfX, rewrite(new BinaryPredicateOperator(BinaryType.NE, left(3), text("1034"))));
        Assertions.assertEquals(notNullOfX, rewrite(new InPredicateOperator(true, left(3), text("1034"), text("10345"))));
        Assertions.assertEquals(notNullOfX, rewrite(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT, eq(left(3), text("1034")))));
        // A shorter constant stays: NOT IN ('10', '1034') is NOT (x = '10').
        ScalarOperator rewritten = rewrite(new InPredicateOperator(true, left(3), text("10"), text("1034")));
        Assertions.assertEquals(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                eq(X, text("10"))), rewritten);
    }

    @Test
    public void testPrefixesOfOneLengthStayOneIn() {
        // 1000 prefixes of three characters and one of two: one prefix IN with the 1000 and one equality.
        List<ScalarOperator> children = new java.util.ArrayList<>();
        children.add(left(3));
        for (int i = 0; i < 1000; i++) {
            children.add(text(String.format("%03d", i)));
        }
        children.add(text("10"));
        ScalarOperator rewritten = rewrite(new InPredicateOperator(false, children.toArray(new ScalarOperator[0])));
        Assertions.assertTrue(rewritten instanceof CompoundPredicateOperator
                && ((CompoundPredicateOperator) rewritten).isOr(), rewritten.toString());
        Assertions.assertEquals(2, rewritten.getChildren().size());
        Assertions.assertEquals(1001, rewritten.getChild(0).getChildren().size());
        Assertions.assertEquals(eq(X, text("10")), rewritten.getChild(1));
        // NOT IN is the negation of the same.
        ScalarOperator negated = rewrite(new InPredicateOperator(true, children.toArray(new ScalarOperator[0])));
        Assertions.assertTrue(negated instanceof CompoundPredicateOperator && ((CompoundPredicateOperator) negated).isNot());
        Assertions.assertEquals(rewritten, negated.getChild(0));
    }
}
