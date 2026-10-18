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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ExpressionStatisticsEstimateTest {
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

    private static ConstantOperator text(String value) {
        return ConstantOperator.createVarchar(value);
    }

    private static ScalarOperator like(ScalarOperator left, String pattern) {
        return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, left, text(pattern));
    }

    private static ScalarOperator not(ScalarOperator operator) {
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, operator);
    }

    private static ExternalMcvStatistics.Group singleton(Map<String, Long> head, List<List<String>> buckets, long rows,
                                                         long nulls) {
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        head.forEach((value, count) -> entries.add(new MultiColumnCombinedStats.McvEntry(List.of(value), count,
                List.of(count))));
        entries.sort((left, right) -> Long.compare(right.getCount(), left.getCount()));
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("x"), rows,
                head.size() + 30, entries, buckets, List.of(nulls));
        group.prepare(VarcharType.VARCHAR);
        return group;
    }

    private static ScanExpressionStatistics expressions(ScalarOperator expression, ExternalMcvStatistics.Group group) {
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(expression);
        ColumnStatistic statistic = group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        ScanExpressionStatistics.Entry entry = new ScanExpressionStatistics.Entry(canonical,
                ExpressionStatisticsCanonicalizer.keyOf(canonical), X, statistic);
        return new ScanExpressionStatistics("t", List.of(entry), Map.of(Set.of(X),
                new MultiColumnCombinedStats(group.getNdv(), group.getRowCount(), List.of(X), group.getMcv(),
                        group.getNullCounts())));
    }

    private static Statistics scan(double rows) {
        return Statistics.builder().setOutputRowCount(rows)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0.1, 8,
                        1000))
                .addColumnStatistic(D, new ColumnStatistic(0, 9, 0, 4, 10)).build();
    }

    private static ScalarOperator left(int length) {
        return call("left", VarcharType.VARCHAR, C, ConstantOperator.createInt(length));
    }

    // A complete head: 600 rows start with 103, 300 rows with 104, and 100 rows are NULL.
    private static ScanExpressionStatistics completePrefix() {
        return expressions(left(8), singleton(Map.of("10312345", 600L, "10400000", 300L), List.of(), 1000, 100));
    }

    @Test
    public void testCompleteHeadIsExact() {
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(like(C, "103%"), scan(1000), true,
                completePrefix());
        Assertions.assertEquals(600, result.getOutputRowCount(), 1e-6);
        // The plain estimate guesses 250 rows and so 250 values of c; the 1000 values of c in the scan are bounded
        // by the 600 rows from x instead.
        Assertions.assertEquals(600, result.getColumnStatistic(C).getDistinctValuesCount(), 1e-6);
        Assertions.assertFalse(result.getColumnStatistic(C).isDistinctValuesFromDefaultSelectivity());
        Assertions.assertFalse(result.getColumnStatistics().containsKey(X));
        Assertions.assertTrue(result.getMultiColumnCombinedStats().isEmpty());
        ScanExpressionStatistics after = result.getScanExpressionStatistics();
        Assertions.assertNotNull(after);
        Assertions.assertTrue(after.describesScanRows(X));
        Assertions.assertEquals(Map.of("10312345", 600L), after.getEntries().get(0).getStatistic().getHistogram().getMCV());
        Assertions.assertEquals(1, after.getEntries().get(0).getStatistic().getDistinctValuesCount(), 1e-6);
    }

    @Test
    public void testNullOnNullIsReadFromTheStructure() {
        // concat is NULL on a NULL argument, so the LIKE is NULL on a NULL x whatever uuid() gives. The constant
        // folding leaves concat(NULL, uuid()) as it is, so only the structure tells it.
        ScalarOperator condition = like(call("concat", VarcharType.VARCHAR, X, call("uuid", VarcharType.VARCHAR)), "a%");
        Assertions.assertEquals(Optional.of(true), ExpressionStatisticsEstimator.nullOnNull(condition, X));
        Statistics statistics = Statistics.builder().setOutputRowCount(1000).addColumnStatistic(X,
                new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0.1, 8, 10)).build();
        // NOT of the condition holds on none of the NULL rows of x.
        Assertions.assertEquals(0.9, ExpressionStatisticsEstimator.negationShare(condition, statistics), 1e-9);
    }

    @Test
    public void testRowCountFromExpressionWithUnknownColumn() {
        // A column without statistics keeps the plain estimate of its form, and the row count still comes from x.
        Statistics unknown = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(C, ColumnStatistic.unknown()).build();
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, left(3), text("103"));
        Assertions.assertEquals(600, PredicateStatisticsCalculator.statisticsCalculate(predicate, unknown, true,
                completePrefix()).getOutputRowCount(), 1e-6);
    }

    @Test
    public void testNegationLeavesNullRowsOut() {
        // 1000 * (1 - 0.1) - 600: the NULL rows do not satisfy the negation.
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(not(like(C, "103%")), scan(1000), true,
                completePrefix());
        Assertions.assertEquals(300, result.getOutputRowCount(), 1e-6);
        // The NULL check of a join key does not change it, in either order.
        ScalarOperator notNull = new IsNullPredicateOperator(true, C);
        for (ScalarOperator predicate : List.of(Utils.compoundAnd(notNull, not(like(C, "103%"))),
                Utils.compoundAnd(not(like(C, "103%")), notNull))) {
            Assertions.assertEquals(300, PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(1000), true,
                    completePrefix()).getOutputRowCount(), 1e-6, predicate.toString());
        }
        // The negation of an incomplete head: N * (1 - nf) - P with P from the prefix estimate of x.
        ExternalMcvStatistics.Group incomplete = singleton(Map.of("10312345", 400L), List.of(), 1000, 500);
        ScanExpressionStatistics expressions = expressions(left(8), incomplete);
        double prefixRows = PredicateStatisticsCalculator.statisticsCalculate(like(C, "103%"), scan(1000), true,
                expressions).getOutputRowCount();
        double negated = PredicateStatisticsCalculator.statisticsCalculate(not(like(C, "103%")), scan(1000), true,
                expressions).getOutputRowCount();
        Assertions.assertEquals(1000 * 0.5 - prefixRows, negated, 1e-6);
    }

    @Test
    public void testEqualityOnVirtualColumnIsIndependent() {
        ScalarOperator lower = call("lower", VarcharType.VARCHAR, C);
        ScanExpressionStatistics expressions = expressions(lower,
                singleton(Map.of("v", 100L, "w", 800L), List.of(), 1000, 100));
        ScalarOperator predicate = Utils.compoundAnd(new BinaryPredicateOperator(BinaryType.EQ, lower, text("v")),
                new BinaryPredicateOperator(BinaryType.EQ, D, ConstantOperator.createInt(5)));
        // 1000 * 0.1 * 0.1, not weakened as a correlated equality.
        Assertions.assertEquals(10, PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(1000), true,
                expressions).getOutputRowCount(), 1e-6);
    }

    @Test
    public void testDefaultSelectivityMarkIsDropped() {
        ScalarOperator predicate = new InPredicateOperator(false, left(3), text("103"), text("104"));
        Statistics plain = PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(1000), true);
        Assertions.assertTrue(plain.getColumnStatistic(C).isDistinctValuesFromDefaultSelectivity());
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(1000), true,
                completePrefix());
        Assertions.assertEquals(900, result.getOutputRowCount(), 1e-6);
        Assertions.assertFalse(result.getColumnStatistic(C).isDistinctValuesFromDefaultSelectivity());
    }

    @Test
    public void testFailureFallsBackToThePlainEstimate() {
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(left(8));
        // A virtual column without statistics fails the estimate over x.
        ScanExpressionStatistics broken = new ScanExpressionStatistics("t", List.of(new ScanExpressionStatistics.Entry(
                canonical, ExpressionStatisticsCanonicalizer.keyOf(canonical), X, null)),
                Map.of(Set.of(X), new MultiColumnCombinedStats(2, 1000, List.of(X), List.of(), List.of(0L))));
        ScalarOperator predicate = like(C, "103%");
        Assertions.assertEquals(PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(1000), true)
                        .getOutputRowCount(),
                PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(1000), true, broken)
                        .getOutputRowCount(), 1e-6);
    }

    private static ColumnStatistic bucketed() {
        return singleton(Map.of("10312345", 300L), List.of(List.of("1035", "1072", "500", "10", "30")), 800, 0)
                .columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
    }

    private static Statistics withX(ColumnStatistic statistic) {
        return Statistics.builder().setOutputRowCount(800).addColumnStatistic(X, statistic).build();
    }

    private static ScalarOperator substrOfX(int start, int length) {
        return call("substr", VarcharType.VARCHAR, X, ConstantOperator.createInt(start),
                ConstantOperator.createInt(length));
    }

    @Test
    public void testSubstringOfVirtualColumn() {
        Statistics statistics = withX(bucketed());
        // The bucket [1035, 1072] has 3 to 7 at the third position: 9 is not possible, and the head has 3.
        Statistics none = ExpressionStatisticsEstimator.estimate(
                new BinaryPredicateOperator(BinaryType.EQ, substrOfX(3, 1), text("9")), statistics).orElseThrow();
        Assertions.assertEquals(1, none.getOutputRowCount(), 1e-6);
        // The head row matches; the strings of the bucket have one of the five characters 3 to 7 there, so a fifth
        // of its rows below the bound.
        Statistics three = ExpressionStatisticsEstimator.estimate(
                new BinaryPredicateOperator(BinaryType.EQ, substrOfX(3, 1), text("3")), statistics).orElseThrow();
        Assertions.assertEquals(300 + (500 - 10) / 5.0, three.getOutputRowCount(), 1e-6);
        Statistics five = ExpressionStatisticsEstimator.estimate(
                new BinaryPredicateOperator(BinaryType.EQ, substrOfX(3, 1), text("5")), statistics).orElseThrow();
        Assertions.assertEquals((500 - 10) / 5.0, five.getOutputRowCount(), 1e-6);
        Statistics notThree = ExpressionStatisticsEstimator.estimate(
                new BinaryPredicateOperator(BinaryType.NE, substrOfX(3, 1), text("3")), statistics).orElseThrow();
        Assertions.assertEquals(10 + 490 * 4 / 5.0, notThree.getOutputRowCount(), 1e-6);
        // 3 and 7: the head row, the 10 rows of the upper bound and two fifths of the rest.
        Statistics in = ExpressionStatisticsEstimator.estimate(
                new InPredicateOperator(false, substrOfX(3, 1), text("3"), text("7")), statistics).orElseThrow();
        Assertions.assertEquals(300 + 10 + 490 * 2 / 5.0, in.getOutputRowCount(), 1e-6);
        Statistics below = ExpressionStatisticsEstimator.estimate(
                new BinaryPredicateOperator(BinaryType.LT, substrOfX(3, 1), text("5")), statistics).orElseThrow();
        Assertions.assertEquals(300 + 490 * 2 / 5.0, below.getOutputRowCount(), 1e-6);
        // The ordinary column does not get this estimate.
        Statistics ordinary = Statistics.builder().setOutputRowCount(800).addColumnStatistic(C, bucketed()).build();
        Assertions.assertTrue(ExpressionStatisticsEstimator.estimate(new BinaryPredicateOperator(BinaryType.EQ,
                call("substr", VarcharType.VARCHAR, C, ConstantOperator.createInt(3), ConstantOperator.createInt(1)),
                text("3")), ordinary).isEmpty());
    }

    @Test
    public void testPrefixInIsTheSumOfPrefixes() {
        // 400 head rows and 900 bucket rows.
        ColumnStatistic statistic = singleton(Map.of("10312345", 300L, "20000000", 100L),
                List.of(List.of("1000", "1099", "200", "5", "50"), List.of("1100", "1999", "500", "5", "80"),
                        List.of("2001", "2999", "900", "5", "90")), 1300, 0)
                .columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        Statistics statistics = Statistics.builder().setOutputRowCount(1300).addColumnStatistic(X, statistic).build();
        List<ScalarOperator> children = new ArrayList<>();
        children.add(substrOfX(1, 3));
        double expected = 0;
        for (String prefix : List.of("103", "150", "200")) {
            children.add(text(prefix));
            // The rows of the prefix as the prefix estimate of one value counts them, before the one-row floor.
            expected += StringHistogramEstimator.rows(StringHistogramEstimator.prefix(X, statistic, prefix, statistics)
                    .getColumnStatistic(X).getHistogram());
        }
        Statistics result = ExpressionStatisticsEstimator.estimate(
                new InPredicateOperator(false, children.toArray(new ScalarOperator[0])), statistics).orElseThrow();
        Assertions.assertEquals(expected, result.getOutputRowCount(), 1e-6);
        Assertions.assertTrue(expected > 400);

        // 900 prefixes in one pass: the prefixes of one bucket do not take more than its rows.
        List<ScalarOperator> many = new ArrayList<>();
        many.add(substrOfX(1, 3));
        for (int i = 100; i < 1000; i++) {
            many.add(text(Integer.toString(i)));
        }
        // Every value starts with one of them: the 400 head rows and every bucket, each held to its own rows.
        Assertions.assertEquals(1300, ExpressionStatisticsEstimator.estimate(
                new InPredicateOperator(false, many.toArray(new ScalarOperator[0])), statistics).orElseThrow()
                .getOutputRowCount(), 1e-6);

        // Four prefixes of the bucket [1100, 1999] of 300 rows: 146, 73, 73 and 73 rows by the model of one prefix,
        // 365 together, so each takes 300 / 365 of its rows: 120, 60, 60 and 60.
        List<ScalarOperator> shared = new ArrayList<>();
        shared.add(substrOfX(1, 3));
        for (String prefix : List.of("110", "120", "150", "160")) {
            shared.add(text(prefix));
        }
        Statistics four = ExpressionStatisticsEstimator.estimate(
                new InPredicateOperator(false, shared.toArray(new ScalarOperator[0])), statistics).orElseThrow();
        Assertions.assertEquals(300, four.getOutputRowCount(), 1e-6);
        Assertions.assertEquals(List.of(120L, 180L, 240L, 300L), four.getColumnStatistic(X).getHistogram().getBuckets()
                .stream().map(Bucket::getCount).toList());
        // The query form goes over x as one IN, not as an OR of LIKE conditions.
        ExpressionStatisticsMatcher.Result match = ExpressionStatisticsMatcher.match(new InPredicateOperator(false,
                left(3), text("110"), text("120"), text("150"), text("160")), completePrefix(), scan(1000));
        Assertions.assertTrue(match.getPredicate() instanceof InPredicateOperator, match.getPredicate().toString());
        Assertions.assertFalse(match.hasOr());
    }

    @Test
    public void testElementNdvFromTheHead() {
        // MCV(status, city) of 1000 rows and 30000 combinations: 5 statuses of 200 rows hold every row, and 5 cities
        // of 100 rows hold half of them.
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            entries.add(new MultiColumnCombinedStats.McvEntry(List.of("s" + i, "c" + i), 100, List.of(200L, 100L)));
        }
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("status", "city"), 1000, 30_000,
                entries, List.of(), List.of(0L, 0L));
        // The head lists every status.
        Assertions.assertEquals(5, ScanExpressionStatistics.elementStatistic(group, 0, VarcharType.VARCHAR,
                ColumnStatistic.unknown()).getDistinctValuesCount(), 1e-9);
        // 500 rows of other cities hold at most 500 values, next to the 5 cities of the head and the one the tail
        // has at least.
        Assertions.assertEquals(506, ScanExpressionStatistics.elementStatistic(group, 1, VarcharType.VARCHAR,
                ColumnStatistic.unknown()).getDistinctValuesCount(), 1e-9);
    }

    @Test
    public void testVirtualColumnOnlyInMixedGroup() {
        ScalarOperator lower = call("lower", VarcharType.VARCHAR, C);
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(lower);
        ExternalMcvStatistics.Group mixed = new ExternalMcvStatistics.Group(List.of("x", "d"), 1000, 50,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("v", "5"), 100, List.of(100L, 300L))),
                List.of(), List.of(0L, 0L));
        ColumnStatistic statistic = ScanExpressionStatistics.elementStatistic(mixed, 0, VarcharType.VARCHAR,
                ColumnStatistic.unknown());
        Assertions.assertEquals(ColumnStatistic.StatisticType.ESTIMATE, statistic.getType());
        ScanExpressionStatistics expressions = new ScanExpressionStatistics("t", List.of(new ScanExpressionStatistics.Entry(
                canonical, ExpressionStatisticsCanonicalizer.keyOf(canonical), X, statistic)),
                Map.of(Set.of(X, D), new MultiColumnCombinedStats(50, 1000, List.of(X, D), mixed.getMcv(),
                        mixed.getNullCounts())));
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(
                new BinaryPredicateOperator(BinaryType.EQ, lower, text("zz")), scan(1000), true, expressions);
        // A value outside the head takes a share of the tail by the NDV of x: 900 rows over 49 values.
        Assertions.assertEquals(900.0 / 49, result.getOutputRowCount(), 1e-6);
    }

    @Test
    public void testNegatedPrefixFormsLeaveNullRowsOut() {
        // <> and NOT IN on a prefix are negations: 1000 * (1 - 0.1) - 600.
        Assertions.assertEquals(300, PredicateStatisticsCalculator.statisticsCalculate(
                new BinaryPredicateOperator(BinaryType.NE, left(3), text("103")), scan(1000), true, completePrefix())
                .getOutputRowCount(), 1e-6);
        Assertions.assertEquals(300, PredicateStatisticsCalculator.statisticsCalculate(
                new InPredicateOperator(true, left(3), text("103")), scan(1000), true, completePrefix())
                .getOutputRowCount(), 1e-6);
        // The NULL check of a join key next to a positive prefix does not change its estimate.
        Assertions.assertEquals(600, PredicateStatisticsCalculator.statisticsCalculate(
                Utils.compoundAnd(new IsNullPredicateOperator(true, C), like(C, "103%")), scan(1000), true,
                completePrefix()).getOutputRowCount(), 1e-6);
    }

    @Test
    public void testIncompleteHeadTakesTheTailEstimate() {
        // 400 head rows start with 103, 100 non-NULL rows are outside the head, 500 rows are NULL: the head matches
        // and a quarter of the tail, over the non-NULL half of the rows.
        ScanExpressionStatistics expressions = expressions(left(8),
                singleton(Map.of("10312345", 400L), List.of(), 1000, 500));
        Assertions.assertEquals(1000 * 0.5 * (400 + 100 * 0.25) / 500, PredicateStatisticsCalculator
                .statisticsCalculate(like(C, "103%"), scan(1000), true, expressions).getOutputRowCount(), 1e-6);
        // With buckets: 400 head rows, 50 bucket rows that start with 103 and 250 that do not, 300 NULL rows.
        ScanExpressionStatistics bucketed = expressions(left(8), singleton(Map.of("10312345", 400L),
                List.of(List.of("10300000", "10399999", "50", "1", "10"),
                        List.of("20000000", "29999999", "300", "1", "100")), 1000, 300));
        Assertions.assertEquals(450, PredicateStatisticsCalculator
                .statisticsCalculate(like(C, "103%"), scan(1000), true, bucketed).getOutputRowCount(), 1e-6);
    }

    @Test
    public void testSubstringRangesBucketsAndNulls() {
        Statistics statistics = withX(bucketed());
        // The head value has 3 at the third position, the bucket 3 to 7, and its upper bound 1072 holds 10 rows.
        Assertions.assertEquals(300 + 490 / 5.0, substring(BinaryType.LE, "3", statistics), 1e-6);
        Assertions.assertEquals(10 + 490 * 4 / 5.0, substring(BinaryType.GT, "3", statistics), 1e-6);
        Assertions.assertEquals(10 + 490 / 5.0, substring(BinaryType.GE, "7", statistics), 1e-6);
        Assertions.assertEquals(10 + 490 * 4 / 5.0, ExpressionStatisticsEstimator.estimate(
                new InPredicateOperator(true, substrOfX(3, 1), text("3")), statistics).orElseThrow()
                .getOutputRowCount(), 1e-6);
        // A bucket whose bounds agree on the first three characters has one substring there.
        ColumnStatistic fixed = singleton(Map.of("20000000", 100L), List.of(List.of("1035", "1039", "400", "5", "5")),
                500, 0).columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        Statistics fixedStatistics = Statistics.builder().setOutputRowCount(500).addColumnStatistic(X, fixed).build();
        Assertions.assertEquals(400, substring(BinaryType.EQ, "3", fixedStatistics), 1e-6);
        // NULL rows never match: 200 of 1000 rows are NULL, the other 800 have the distribution above.
        ColumnStatistic withNulls = singleton(Map.of("10312345", 300L), List.of(List.of("1035", "1072", "500", "10", "30")),
                1000, 200).columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        Statistics nullStatistics = Statistics.builder().setOutputRowCount(1000).addColumnStatistic(X, withNulls).build();
        Assertions.assertEquals(398, substring(BinaryType.EQ, "3", nullStatistics), 1e-6);
        // The MCV estimate takes the head exactly and the rows outside it from this estimate.
        ExternalMcvStatistics.Group group = singleton(Map.of("10312345", 300L),
                List.of(List.of("1035", "1072", "500", "10", "30")), 800, 0);
        Statistics withGroup = Statistics.buildFrom(statistics).addMultiColumnStatistics(Set.of(X),
                new MultiColumnCombinedStats(group.getNdv(), 800, List.of(X), group.getMcv(), group.getNullCounts()))
                .build();
        Assertions.assertEquals(398, PredicateStatisticsCalculator.statisticsCalculate(
                new BinaryPredicateOperator(BinaryType.EQ, substrOfX(3, 1), text("3")), withGroup)
                .getOutputRowCount(), 1e-6);
    }

    private static double substring(BinaryType type, String value, Statistics statistics) {
        return ExpressionStatisticsEstimator.estimate(new BinaryPredicateOperator(type, substrOfX(3, 1), text(value)),
                statistics).orElseThrow().getOutputRowCount();
    }

    @Test
    public void testGroupOfTwoExpressionsWithoutComponentCounts() {
        ColumnRefOperator upperX = new ColumnRefOperator(-2, VarcharType.VARCHAR, "expr#2", true);
        ScalarOperator lower = call("lower", VarcharType.VARCHAR, C);
        ScalarOperator upper = call("upper", VarcharType.VARCHAR, C);
        // A head entry without component counts reads the statistics of every column of its group.
        List<MultiColumnCombinedStats.McvEntry> head =
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("v", "V"), 300));
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("x1", "x2"), 1000, 50, head,
                List.of(), List.of(0L, 0L));
        ColumnStatistic element = new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 50);
        List<ScanExpressionStatistics.Entry> entries = new ArrayList<>();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> item : Map.of(X, lower, upperX, upper).entrySet()) {
            ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(item.getValue());
            entries.add(new ScanExpressionStatistics.Entry(canonical, ExpressionStatisticsCanonicalizer.keyOf(canonical),
                    item.getKey(), element));
        }
        ScanExpressionStatistics expressions = new ScanExpressionStatistics("t", entries, Map.of(Set.of(X, upperX),
                new MultiColumnCombinedStats(50, 1000, List.of(X, upperX), head, List.of(0L, 0L))));
        double rows = PredicateStatisticsCalculator.statisticsCalculate(
                new BinaryPredicateOperator(BinaryType.EQ, lower, text("v")), scan(1000), true, expressions)
                .getOutputRowCount();
        // The head tuple holds 300 rows. Outside the head, x = 'v' takes 1 / 50 of the rows less the share the head
        // tuple has by the NDVs of both columns, 1 / 50 * 1 / 50: 1000 * (0.3 + 0.02 - 0.0004).
        Assertions.assertEquals(319.6, rows, 1e-6);
        Assertions.assertEquals(1000, group.getRowCount());
    }

    @Test
    public void testColumnMarginalServesDateFunctions() {
        ColumnRefOperator ts = new ColumnRefOperator(5, com.starrocks.type.DateType.DATETIME, "ts", true);
        ColumnRefOperator day = new ColumnRefOperator(6, IntegerType.INT, "d", true);
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("ts", "d"), 1000, 40,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("2024-12-01 10:00:00", "1"), 300,
                                List.of(400L, 500L)),
                        new MultiColumnCombinedStats.McvEntry(List.of("2024-12-02 11:00:00", "2"), 200,
                                List.of(250L, 300L))),
                List.of(), List.of(0L, 0L));
        Map<ColumnRefOperator, com.starrocks.catalog.Column> read = Map.of(
                ts, new com.starrocks.catalog.Column("ts", com.starrocks.type.DateType.DATETIME),
                day, new com.starrocks.catalog.Column("d", IntegerType.INT));
        Statistics base = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(ts, new ColumnStatistic(1704067200, 1735689599, 0, 8, 366))
                .addColumnStatistic(day, new ColumnStatistic(0, 9, 0, 4, 10)).build();
        Statistics attached = StatisticsCalcUtils.attachExternalMcvStats(base,
                new ExternalMcvStatistics(List.of(group)), read);
        ColumnStatistic statistic = attached.getColumnStatistic(ts);
        Assertions.assertEquals(Map.of("2024-12-01 10:00:00", 400L, "2024-12-02 11:00:00", 250L),
                statistic.getHistogram().getMCV());
        Assertions.assertEquals(ColumnStatistic.StatisticType.ESTIMATE, statistic.getType());
        // GROUP BY date_trunc('day', ts) maps the head of ts to days.
        ScalarOperator trunc = call("date_trunc", com.starrocks.type.DateType.DATETIME, text("day"), ts);
        ColumnStatistic days = ExpressionStatisticCalculator.calculate(trunc, attached);
        Assertions.assertEquals(2, days.getHistogram().getMCV().size());
        Assertions.assertEquals(650, days.getHistogram().getMCV().values().stream().mapToLong(Long::longValue).sum());
        // ts IN (...) counts the rows of the value in its column, not in the tuple.
        Assertions.assertEquals(400, PredicateStatisticsCalculator.statisticsCalculate(new InPredicateOperator(false,
                ts, ConstantOperator.createDatetime(java.time.LocalDateTime.of(2024, 12, 1, 10, 0))), attached)
                .getOutputRowCount(), 1e-6);
    }

    @Test
    public void testNullCheckNextToConditionTrueOnNull() {
        // ifnull(c, 'u') = 'u' holds on 400 of 1000 rows, NULL rows of c among them; c IS NOT NULL keeps its own
        // share 1 - 0.1 of them: 400 * 0.9.
        ScalarOperator ifnull = call("ifnull", VarcharType.VARCHAR, C, text("u"));
        ScanExpressionStatistics expressions = expressions(ifnull,
                singleton(Map.of("u", 400L, "a", 600L), List.of(), 1000, 0));
        ScalarOperator predicate = Utils.compoundAnd(new BinaryPredicateOperator(BinaryType.EQ, ifnull, text("u")),
                new IsNullPredicateOperator(true, C));
        Assertions.assertEquals(360, PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(1000), true,
                expressions).getOutputRowCount(), 1e-6);
    }

    @Test
    public void testNegationOfImpossiblePrefixKeepsNonNullRows() {
        // No value of left(c, 3) is '1034', so the negations hold on the 900 rows that are not NULL.
        List<ScalarOperator> negations = List.of(
                new BinaryPredicateOperator(BinaryType.NE, left(3), text("1034")),
                new InPredicateOperator(true, left(3), text("1034"), text("10345")),
                not(new BinaryPredicateOperator(BinaryType.EQ, left(3), text("1034"))));
        for (ScalarOperator predicate : negations) {
            Assertions.assertEquals(900, PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(1000), true,
                    completePrefix()).getOutputRowCount(), 1e-6, predicate.toString());
        }
    }

    @Test
    public void testLongSubstringIsBoundedByTheBucketBounds() {
        // Positions after the longest value seen, the 8 characters of the head value, are not counted, so every
        // length past them gives the same estimate. The head and the bounds hold 1, 2, 3, 4, 5 and 7 from the third
        // position on. Between 1035 and 1072 the strings of 8 characters are 35 or 37 followed by 6^4 endings, 4, 5
        // or 6 followed by 6^5, and 71 followed by 6^4: 21 * 6^4. Below 4 are the 2 * 6^4 that start with 3, and the
        // head value 10312345.
        Statistics statistics = withX(bucketed());
        for (int length : List.of(300, 1_000_000, Integer.MAX_VALUE)) {
            Statistics result = Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(10),
                    () -> ExpressionStatisticsEstimator.estimate(new BinaryPredicateOperator(BinaryType.LT,
                            substrOfX(3, length), text("4")), statistics).orElseThrow());
            Assertions.assertEquals(300 + 490 * 2 / 21.0, result.getOutputRowCount(), 1e-6, Integer.toString(length));
        }
        // Bounds of 70 characters leave more positions than the estimate walks: the plain estimate takes over.
        String low = "1" + "0".repeat(69);
        String high = "1" + "9".repeat(69);
        ColumnStatistic longBounds = singleton(Map.of("10312345", 300L), List.of(List.of(low, high, "500", "10", "30")),
                800, 0).columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        Assertions.assertTrue(ExpressionStatisticsEstimator.estimate(new BinaryPredicateOperator(BinaryType.EQ,
                substrOfX(3, 300), text("35")), withX(longBounds)).isEmpty());
        Statistics plain = PredicateStatisticsCalculator.statisticsCalculate(new BinaryPredicateOperator(BinaryType.EQ,
                substrOfX(3, 300), text("35")), withX(longBounds));
        Assertions.assertFalse(Double.isNaN(plain.getOutputRowCount()));
    }

    @Test
    public void testSubstringCountsOnlyStringsBetweenTheBounds() {
        // The head brings every digit to the third and fourth positions. Between 1038 and 1041 these positions hold
        // 38, 39, 40 or 41, each a quarter of the 400 bucket rows.
        Map<String, Long> head = Map.of("2001", 10L, "2023", 10L, "2045", 10L, "2067", 10L, "2089", 10L);
        ColumnStatistic statistic = singleton(head, List.of(List.of("1038", "1041", "400", "0", "4")), 450, 0)
                .columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        Statistics statistics = Statistics.builder().setOutputRowCount(450).addColumnStatistic(X, statistic).build();
        Assertions.assertEquals(1, twoCharacters(BinaryType.EQ, "35", statistics), 1e-6);
        Assertions.assertEquals(100, twoCharacters(BinaryType.EQ, "39", statistics), 1e-6);
        Assertions.assertEquals(100, twoCharacters(BinaryType.EQ, "41", statistics), 1e-6);
        // Below 39: the head values with 01 and 23, and 38 in the bucket.
        Assertions.assertEquals(20 + 100, twoCharacters(BinaryType.LT, "39", statistics), 1e-6);
    }

    private static double twoCharacters(BinaryType type, String value, Statistics statistics) {
        return ExpressionStatisticsEstimator.estimate(new BinaryPredicateOperator(type, substrOfX(3, 2), text(value)),
                statistics).orElseThrow().getOutputRowCount();
    }

    @Test
    public void testColumnNdvIsBoundedWithUnknownColumn() {
        // 1M rows, 200k values of c, a column without statistics, and 100k rows that start with 103.
        ColumnRefOperator unknown = new ColumnRefOperator(4, VarcharType.VARCHAR, "u", true);
        Statistics scan = Statistics.builder().setOutputRowCount(1_000_000)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8,
                        200_000))
                .addColumnStatistic(unknown, ColumnStatistic.unknown()).build();
        ScanExpressionStatistics expressions = expressions(left(8),
                singleton(Map.of("10312345", 100_000L, "20000000", 900_000L), List.of(), 1_000_000, 0));
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(like(C, "103%"), scan, true, expressions);
        Assertions.assertEquals(100_000, result.getOutputRowCount(), 1e-6);
        Assertions.assertEquals(100_000, result.getColumnStatistic(C).getDistinctValuesCount(), 1e-6);
    }

    private static Statistics xWithNulls() {
        ColumnStatistic statistic = singleton(Map.of("a", 1000L, "b", 499_000L), List.of(), 1_000_000, 500_000)
                .columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        return Statistics.builder().setOutputRowCount(1_000_000).addColumnStatistic(X, statistic).build();
    }

    @Test
    public void testNegationKeepsNullRowsWhenTheConditionIsNotNullOnNull() {
        Statistics statistics = xWithNulls();
        // x <=> 'a' is FALSE on the 500k NULL rows, so its negation holds on them: 1M - 1k.
        ScalarOperator nullSafe = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, X, text("a"));
        Assertions.assertEquals(1000, PredicateStatisticsCalculator.statisticsCalculate(nullSafe, statistics)
                .getOutputRowCount(), 1e-6);
        Assertions.assertEquals(999_000, PredicateStatisticsCalculator.statisticsCalculate(not(nullSafe), statistics)
                .getOutputRowCount(), 1e-6);
        // x IS NULL is TRUE on them: NOT holds on the other 500k rows.
        Assertions.assertEquals(500_000, PredicateStatisticsCalculator.statisticsCalculate(
                not(new IsNullPredicateOperator(false, X)), statistics).getOutputRowCount(), 1e-6);
        // x = 'a' is NULL on them: 1M * 0.5 - 1k.
        Assertions.assertEquals(499_000, PredicateStatisticsCalculator.statisticsCalculate(
                not(new BinaryPredicateOperator(BinaryType.EQ, X, text("a"))), statistics).getOutputRowCount(), 1e-6);
    }

    @Test
    public void testOrderOfNegationAndPrefixDoesNotMatter() {
        // 1M rows without NULL: 400k rows of 10312345 and 100k of 10500000 in the head, 250k in each bucket, and
        // every value starts with 10. Rows that do not start with 103: 600k.
        ScanExpressionStatistics noNulls = expressions(left(8), singleton(
                Map.of("10312345", 400_000L, "10500000", 100_000L),
                List.of(List.of("10000000", "10299999", "250000", "1", "1000"),
                        List.of("10400000", "10999999", "500000", "1", "1000")), 1_000_000, 0));
        assertBothOrders(600_000, not(like(C, "103%")), like(C, "10%"), noNulls);
        // Half of 1M rows NULL, 100k rows start with 103: 1M * 0.5 - 100k.
        ScanExpressionStatistics halfNull = expressions(left(8), singleton(
                Map.of("10312345", 100_000L, "10500000", 100_000L),
                List.of(List.of("10000000", "10299999", "150000", "1", "1000"),
                        List.of("10400000", "10999999", "300000", "1", "1000")), 1_000_000, 500_000));
        assertBothOrders(400_000, not(like(C, "103%")), like(C, "10%"), halfNull);
        // A <> with a longer constant is x IS NOT NULL: the next condition keeps the distribution of x either way.
        assertBothOrders(400_000, not(like(C, "103%")),
                new BinaryPredicateOperator(BinaryType.NE, left(3), text("1034")), halfNull);
    }

    private static void assertBothOrders(double expected, ScalarOperator first, ScalarOperator second,
                                         ScanExpressionStatistics expressions) {
        for (ScalarOperator predicate : List.of(Utils.compoundAnd(first, second), Utils.compoundAnd(second, first))) {
            Assertions.assertEquals(expected, PredicateStatisticsCalculator.statisticsCalculate(predicate,
                    scan(1_000_000), true, expressions).getOutputRowCount(), 1e-6, predicate.toString());
        }
    }

    @Test
    public void testDefaultSelectivityMarkStaysOnAnotherColumn() {
        // abs(e) IN (1, 2) is a default share in both estimates, so e keeps the mark; c does not get one over x.
        ColumnRefOperator e = new ColumnRefOperator(3, IntegerType.INT, "e", true);
        Statistics scan = Statistics.buildFrom(scan(1000))
                .addColumnStatistic(e, new ColumnStatistic(0, 1_000_000, 0, 4, 1000)).build();
        ScalarOperator predicate = Utils.compoundAnd(
                new InPredicateOperator(false, left(3), text("103"), text("104")),
                new InPredicateOperator(false, call("abs", IntegerType.INT, e), ConstantOperator.createInt(1),
                        ConstantOperator.createInt(2)));
        Statistics plain = PredicateStatisticsCalculator.statisticsCalculate(predicate, scan, true);
        Assertions.assertTrue(plain.getColumnStatistic(e).isDistinctValuesFromDefaultSelectivity());
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(predicate, scan, true, completePrefix());
        Assertions.assertTrue(result.getColumnStatistic(e).isDistinctValuesFromDefaultSelectivity());
    }

    @Test
    public void testPrefixInShrinksPiecesInProportion() {
        // One bucket of 1M rows and 1000 values from 10000000 to 10999999, no repeats. By the model of one prefix
        // 100 takes 499500 rows and 500 values, 101 to 108 take 249750 rows and 250 values each, and 109 takes
        // 500500 rows and 501 values: 2998000 rows, so each piece takes 1M / 2998000 of its rows and values.
        Histogram histogram = Histogram.forStrings(List.of(new StringBucket("10000000", "10999999", 1_000_000, 0, 1000)),
                Map.of());
        ColumnStatistic statistic = new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 1000,
                histogram, ColumnStatistic.StatisticType.ESTIMATE);
        List<ScalarOperator> children = new ArrayList<>();
        children.add(substrOfX(1, 3));
        for (int prefix = 100; prefix <= 109; prefix++) {
            children.add(text(Integer.toString(prefix)));
        }
        Statistics result = ExpressionStatisticsEstimator.estimate(
                new InPredicateOperator(false, children.toArray(new ScalarOperator[0])),
                Statistics.builder().setOutputRowCount(1_000_000).addColumnStatistic(X, statistic).build()).orElseThrow();
        Assertions.assertEquals(1_000_000, result.getOutputRowCount(), 1e-6);
        List<Long> pieces = new ArrayList<>();
        long previous = 0;
        for (Bucket bucket : result.getColumnStatistic(X).getHistogram().getBuckets()) {
            pieces.add(bucket.getCount() - previous);
            previous = bucket.getCount();
        }
        Assertions.assertEquals(List.of(166611L, 83306L, 83305L, 83306L, 83305L, 83306L, 83305L, 83306L, 83305L,
                166945L), pieces);
        // 3001 values by the model shrink to 1001, held to the 1000 values before the filter.
        Assertions.assertEquals(1000, result.getColumnStatistic(X).getDistinctValuesCount(), 1e-6);
    }

    @Test
    public void testNegationOfOrCutsEveryPrefix() {
        // 1M rows without NULL: 200k rows of 10500000 in the head, and buckets of 200k, 300k and 300k rows.
        ScanExpressionStatistics expressions = expressions(left(8), singleton(Map.of("10500000", 200_000L),
                List.of(List.of("10000000", "10299999", "200000", "1", "1000"),
                        List.of("10300000", "10499999", "500000", "1", "1000"),
                        List.of("10600000", "10999999", "800000", "1", "1000")), 1_000_000, 0));
        // No row both starts with 103 and does not start with 103 or 104: the estimate has no row left, 1.
        assertBothOrders(1, not(Utils.compoundOr(like(C, "103%"), like(C, "104%"))), like(C, "103%"), expressions);
        assertBothOrders(1, not(like(C, "103%%")), like(C, "103%"), expressions);
    }

    @Test
    public void testNegationWithoutStringHistogramLeavesNoNullRows() {
        // x of INT with 10 values and half of 1000 rows NULL: NOT (x = 5) keeps 1000 * 0.5 - 50 rows, and x IS NOT
        // NULL after it removes nothing more.
        ColumnRefOperator number = new ColumnRefOperator(-3, IntegerType.INT, "expr#3", true);
        Statistics statistics = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(number, new ColumnStatistic(0, 9, 0.5, 4, 10)).build();
        ScalarOperator negation = not(new BinaryPredicateOperator(BinaryType.EQ, number, ConstantOperator.createInt(5)));
        ScalarOperator notNull = new IsNullPredicateOperator(true, number);
        for (ScalarOperator predicate : List.of(Utils.compoundAnd(negation, notNull), Utils.compoundAnd(notNull, negation))) {
            Assertions.assertEquals(450, PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics)
                    .getOutputRowCount(), 1e-6, predicate.toString());
        }
    }

    @Test
    public void testManyConstantsOnManyBucketsAreBounded() {
        // 565 buckets of 100 rows from 10000000 on, each 100000 values wide, and 100 head rows of 20000000. The third
        // to fifth characters of every value are three digits, so IN of all 1000 of them holds on every row.
        List<Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < 565; i++) {
            String lower = Integer.toString(10_000_000 + i * 100_000);
            String upper = Integer.toString(10_000_000 + i * 100_000 + 99_999);
            buckets.add(new StringBucket(lower, upper, (i + 1) * 100L, 1, 50));
        }
        ColumnStatistic statistic = new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 30_000,
                Histogram.forStrings(buckets, Map.of("20000000", 100L)), ColumnStatistic.StatisticType.ESTIMATE);
        Statistics statistics = Statistics.builder().setOutputRowCount(56_600).addColumnStatistic(X, statistic).build();
        List<ScalarOperator> children = new ArrayList<>();
        children.add(substrOfX(3, 3));
        for (int i = 0; i < 1000; i++) {
            children.add(text(String.format("%03d", i)));
        }
        Statistics result = Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(10),
                () -> ExpressionStatisticsEstimator.estimate(new InPredicateOperator(false,
                        children.toArray(new ScalarOperator[0])), statistics).orElseThrow());
        Assertions.assertEquals(56_600, result.getOutputRowCount(), 1e-6);
        // On a small case IN is the sum of its constants: 398 + 98 + 98.
        Statistics small = withX(bucketed());
        double sum = 0;
        for (String value : List.of("3", "5", "6")) {
            sum += ExpressionStatisticsEstimator.estimate(new BinaryPredicateOperator(BinaryType.EQ, substrOfX(3, 1),
                    text(value)), small).orElseThrow().getOutputRowCount();
        }
        Assertions.assertEquals(594, sum, 1e-6);
        Assertions.assertEquals(594, ExpressionStatisticsEstimator.estimate(new InPredicateOperator(false,
                substrOfX(3, 1), text("3"), text("5"), text("6")), small).orElseThrow().getOutputRowCount(), 1e-6);
    }

    @Test
    public void testSubstringPastTheBucketBounds() {
        // The bounds 1234 and 1299 are shorter than the values of left(c, 8). Positions 3 to 6 of a string between
        // them: 3 then four of 4, 5, 6, 9 then two each of 5 and 6; 4 to 8 likewise; 9 then 4, 5 or 6: 108
        // combinations, 27 of them with 56 at positions 5 and 6.
        ColumnStatistic statistic = singleton(Map.of("12345678", 100L), List.of(List.of("1234", "1299", "400", "0", "50")),
                500, 0).columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        Statistics statistics = Statistics.builder().setOutputRowCount(500).addColumnStatistic(X, statistic).build();
        Assertions.assertEquals(100 + 400 / 4.0, ExpressionStatisticsEstimator.estimate(new BinaryPredicateOperator(
                BinaryType.EQ, substrOfX(5, 2), text("56")), statistics).orElseThrow().getOutputRowCount(), 1e-6);
        Assertions.assertEquals(400 * 3 / 4.0, ExpressionStatisticsEstimator.estimate(new BinaryPredicateOperator(
                BinaryType.NE, substrOfX(5, 2), text("56")), statistics).orElseThrow().getOutputRowCount(), 1e-6);
    }

    @Test
    public void testExcludedUpperBoundIsNotCounted() {
        // After x < '104' the bucket ends before 104: the third character is 0 to 3, a quarter each.
        Histogram histogram = Histogram.forStrings(List.of(new StringBucket("10000000", "104", 500, 0, 50, true, false)),
                Map.of());
        Statistics statistics = Statistics.builder().setOutputRowCount(500).addColumnStatistic(X, new ColumnStatistic(
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 50, histogram,
                ColumnStatistic.StatisticType.ESTIMATE)).build();
        Assertions.assertEquals(125, substring(BinaryType.EQ, "3", statistics), 1e-6);
        Assertions.assertEquals(1, substring(BinaryType.EQ, "4", statistics), 1e-6);
    }

    @Test
    public void testGuessedNdvTakesTheNarrowedColumn() {
        // c LIKE '103%' is a default share 0.25 in the plain estimate, which marks e; e = 'A' then leaves one value
        // of e, and the estimate over x leaves it too: 600 rows start with 103, 40% of them are A, and e keeps 1
        // value, not the 600 of the scan.
        ColumnRefOperator e = new ColumnRefOperator(3, VarcharType.VARCHAR, "e", true);
        ColumnStatistic statistic = new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 600,
                Histogram.forStrings(List.of(), Map.of("A", 400L, "B", 600L)), ColumnStatistic.StatisticType.ESTIMATE);
        Statistics scan = Statistics.buildFrom(scan(1000)).addColumnStatistic(e, statistic).build();
        ScalarOperator predicate = Utils.compoundAnd(like(C, "103%"),
                new BinaryPredicateOperator(BinaryType.EQ, e, text("A")));
        Statistics plain = PredicateStatisticsCalculator.statisticsCalculate(predicate, scan, true);
        Assertions.assertTrue(plain.getColumnStatistic(e).isDistinctValuesFromDefaultSelectivity());
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(predicate, scan, true, completePrefix());
        Assertions.assertEquals(240, result.getOutputRowCount(), 1e-6);
        Assertions.assertEquals(1, result.getColumnStatistic(e).getDistinctValuesCount(), 1e-6);
        // With a prefix IN the estimate over x takes the MCV group of x, which keeps the NDV of e; e = 'A' in the
        // plain estimate leaves one value, and e keeps it.
        ScalarOperator in = Utils.compoundAnd(new InPredicateOperator(false, left(3), text("103"), text("104")),
                new BinaryPredicateOperator(BinaryType.EQ, e, text("A")));
        Statistics inResult = PredicateStatisticsCalculator.statisticsCalculate(in, scan, true, completePrefix());
        Assertions.assertEquals(1, inResult.getColumnStatistic(e).getDistinctValuesCount(), 1e-6);
        Assertions.assertFalse(inResult.getColumnStatistic(e).isDistinctValuesFromDefaultSelectivity());
        Assertions.assertFalse(result.getColumnStatistic(e).isDistinctValuesFromDefaultSelectivity());
    }

    @Test
    public void testNullSafeEqualityOnPrefixIsEstimatedAsBefore() {
        // No estimate over x reads <=> on a substring of x, so it is not written, and the estimate is the plain one,
        // not all rows for the condition and one row for its negation.
        ScalarOperator nullSafe = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, left(3), text("103"));
        Assertions.assertNull(ExpressionStatisticsMatcher.match(nullSafe, completePrefix(), scan(1000)));
        for (ScalarOperator predicate : List.of(nullSafe, not(nullSafe))) {
            Statistics plain = PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(1000), true);
            Statistics result = PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(1000), true,
                    completePrefix());
            Assertions.assertEquals(plain.getOutputRowCount(), result.getOutputRowCount(), 1e-9);
            Assertions.assertTrue(result.getOutputRowCount() > 1 && result.getOutputRowCount() < 1000);
        }
    }

    @Test
    public void testRangeOnPrefixWithShorterConstantIsOnX() {
        // 600 rows of 10312345 in the head and 400 rows from 20000000 to 29999999 in a bucket. A constant shorter
        // than the prefix compares with the prefix as with the whole x, so the bucket is cut by x itself.
        ScanExpressionStatistics expressions = expressions(left(8), singleton(Map.of("10312345", 600L),
                List.of(List.of("20000000", "29999999", "400", "0", "100")), 1000, 0));
        Map<BinaryType, Double> expected = Map.of(BinaryType.LT, 600.0, BinaryType.LE, 600.0, BinaryType.GT, 400.0,
                BinaryType.GE, 400.0);
        expected.forEach((type, rows) -> Assertions.assertEquals(rows, PredicateStatisticsCalculator.statisticsCalculate(
                new BinaryPredicateOperator(type, left(3), text("2")), scan(1000), true, expressions)
                .getOutputRowCount(), 1e-6, type.toString()));
    }

    @Test
    public void testKeptConditionOnSourceDoesNotDropNullRowsAgain() {
        // 600 rows start with 103, and the 100 NULL rows of c are not among them; c <> 'zzz' then keeps all but a
        // thousandth of the values of c, without taking out the NULL rows of c again.
        ScalarOperator predicate = Utils.compoundAnd(like(C, "103%"),
                new BinaryPredicateOperator(BinaryType.NE, C, text("zzz")));
        Assertions.assertEquals(600 * (1 - 1 / 1000.0), PredicateStatisticsCalculator.statisticsCalculate(predicate,
                scan(1000), true, completePrefix()).getOutputRowCount(), 1);
    }

    @Test
    public void testEqualityNextToConditionOnXIsIndependent() {
        // e = 'A' holds on 40% of the rows; next to a condition on f it multiplies the rows from x, whether the MCV
        // estimate takes the group of x or not.
        ColumnRefOperator e = new ColumnRefOperator(3, VarcharType.VARCHAR, "e", true);
        ColumnStatistic statistic = new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 600,
                Histogram.forStrings(List.of(), Map.of("A", 400L, "B", 600L)), ColumnStatistic.StatisticType.ESTIMATE);
        Statistics scan = Statistics.buildFrom(scan(1000)).addColumnStatistic(e, statistic).build();
        ScalarOperator isA = new BinaryPredicateOperator(BinaryType.EQ, e, text("A"));
        Assertions.assertEquals(900 * 0.4, PredicateStatisticsCalculator.statisticsCalculate(Utils.compoundAnd(
                new InPredicateOperator(false, left(3), text("103"), text("104")), isA), scan, true, completePrefix())
                .getOutputRowCount(), 1e-6);
        Assertions.assertEquals(600 * 0.4, PredicateStatisticsCalculator.statisticsCalculate(Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.EQ, left(3), text("103")), isA), scan, true, completePrefix())
                .getOutputRowCount(), 1e-6);
    }

    @Test
    public void testNdvAtTinyPlainRowCountTakesTheScan() {
        // The plain estimate of left(c, 3) = '103' divides 10M rows by the NDV of left(c, 3), a few rows, and caps
        // every NDV there without a mark. x gives 6M rows, so c and d take their NDV from the scan, within 6M.
        Statistics scan = Statistics.builder().setOutputRowCount(10_000_000)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8,
                        10_000_000))
                .addColumnStatistic(D, new ColumnStatistic(0, 9, 0, 4, 10)).build();
        ScanExpressionStatistics expressions = expressions(left(8), singleton(Map.of("10312345", 6_000_000L,
                "10400000", 3_000_000L), List.of(), 10_000_000, 1_000_000));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, left(3), text("103"));
        Assertions.assertTrue(PredicateStatisticsCalculator.statisticsCalculate(predicate, scan, true)
                .getOutputRowCount() < 10);
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(predicate, scan, true, expressions);
        Assertions.assertEquals(6_000_000, result.getOutputRowCount(), 1e-6);
        Assertions.assertEquals(6_000_000, result.getColumnStatistic(C).getDistinctValuesCount(), 1e-6);
        Assertions.assertEquals(10, result.getColumnStatistic(D).getDistinctValuesCount(), 1e-6);
    }

    @Test
    public void testNdvNarrowedByThePlainEstimateStays() {
        // The histogram of c has 100000 rows and 20000 values from 10300000 to 10399999: c LIKE '103%' keeps them,
        // and c keeps 20000 values at the 100000 rows from x.
        Histogram histogram = Histogram.forStrings(List.of(new StringBucket("10300000", "10399999", 100_000, 0, 20_000),
                new StringBucket("20000000", "29999999", 200_000, 0, 50_000)), Map.of());
        Statistics scan = Statistics.builder().setOutputRowCount(200_000)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8,
                        70_000, histogram, ColumnStatistic.StatisticType.ESTIMATE)).build();
        ScanExpressionStatistics expressions = expressions(left(8), singleton(Map.of("10312345", 100_000L,
                "20000000", 100_000L), List.of(), 200_000, 0));
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(like(C, "103%"), scan, true,
                expressions);
        Assertions.assertEquals(100_000, result.getOutputRowCount(), 1e-6);
        Assertions.assertEquals(20_000, result.getColumnStatistic(C).getDistinctValuesCount(), 1e-6);
    }

    @Test
    public void testPrefixInWithShorterConstant() {
        // 600 rows of 10312345 and 100 rows of the whole string 10: IN ('103', '10') holds on both.
        ScanExpressionStatistics expressions = expressions(left(8),
                singleton(Map.of("10312345", 600L, "10", 100L, "20000000", 300L), List.of(), 1000, 0));
        Assertions.assertEquals(700, PredicateStatisticsCalculator.statisticsCalculate(new InPredicateOperator(false,
                left(3), text("103"), text("10")), scan(1000), true, expressions).getOutputRowCount(), 1e-6);
    }

    @Test
    public void testLargeAlphabetIsBounded() {
        // 100 head values with 3000 different characters from position 30 on, and 565 buckets of 60 characters.
        Map<String, Long> head = new java.util.HashMap<>();
        for (int v = 0; v < 100; v++) {
            StringBuilder value = new StringBuilder("B" + String.format("%03d", v) + "x".repeat(25));
            for (int j = 0; j < 30; j++) {
                value.appendCodePoint(0x4E00 + v * 30 + j);
            }
            head.put(value.toString(), 1L);
        }
        List<Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < 565; i++) {
            String key = "A" + String.format("%03d", i);
            buckets.add(new StringBucket(key + "a".repeat(56), key + "z".repeat(56), (i + 1) * 100L, 1, 50));
        }
        ColumnStatistic statistic = new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 60,
                30_000, Histogram.forStrings(buckets, head), ColumnStatistic.StatisticType.ESTIMATE);
        Statistics statistics = Statistics.builder().setOutputRowCount(56_600).addColumnStatistic(X, statistic).build();
        String value = new String(Character.toChars(0x4E00)).repeat(30);
        Optional<Statistics> result = Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(10),
                () -> ExpressionStatisticsEstimator.estimate(new BinaryPredicateOperator(BinaryType.EQ,
                        substrOfX(30, 30), text(value)), statistics));
        Assertions.assertTrue(result.isEmpty() || Double.isFinite(result.get().getOutputRowCount()));
    }

    @Test
    public void testNegatedPrefixInCutsTheBucketsInOnePass() {
        // One bucket of 1M rows and 1000 values from 10000000 to 10999999. Without 103 and 105 it keeps 499500
        // rows below 103, 249750 rows from 104 to 105, and 250750 rows from 106, as cutting 103 and then 105
        // gives; the negation holds on 1M - 2 * 249750 rows.
        Histogram histogram = Histogram.forStrings(List.of(new StringBucket("10000000", "10999999", 1_000_000, 0, 1000)),
                Map.of());
        Statistics statistics = Statistics.builder().setOutputRowCount(1_000_000).addColumnStatistic(X,
                new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 1000, histogram,
                        ColumnStatistic.StatisticType.ESTIMATE)).build();
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(not(new InPredicateOperator(false,
                substrOfX(1, 3), text("103"), text("105"))), statistics);
        Assertions.assertEquals(500_500, result.getOutputRowCount(), 1e-6);
        Assertions.assertEquals(List.of(499_500L, 749_250L, 1_000_000L), result.getColumnStatistic(X).getHistogram()
                .getBuckets().stream().map(Bucket::getCount).toList());
        // 10000 prefixes over 565 buckets.
        List<Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < 565; i++) {
            buckets.add(new StringBucket(Integer.toString(10_000_000 + i * 100_000),
                    Integer.toString(10_000_000 + i * 100_000 + 99_999), (i + 1) * 100L, 1, 50));
        }
        Statistics many = Statistics.builder().setOutputRowCount(56_500).addColumnStatistic(X, new ColumnStatistic(
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 30_000, Histogram.forStrings(buckets, Map.of()),
                ColumnStatistic.StatisticType.ESTIMATE)).build();
        List<ScalarOperator> children = new ArrayList<>();
        children.add(substrOfX(1, 5));
        for (int i = 10_000; i < 20_000; i++) {
            children.add(text(Integer.toString(i)));
        }
        Statistics negated = Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(10),
                () -> PredicateStatisticsCalculator.statisticsCalculate(not(new InPredicateOperator(false,
                        children.toArray(new ScalarOperator[0]))), many));
        Assertions.assertTrue(Double.isFinite(negated.getOutputRowCount()));
    }

    @Test
    public void testConditionOnPrefixLeavesNoNullRowsOfTheColumn() {
        // c has 10% NULL rows; a prefix condition is never true on them, ifnull(c, 'u') = 'u' is.
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(new InPredicateOperator(false, left(3),
                text("103"), text("104")), scan(1000), true, completePrefix());
        Assertions.assertEquals(0, result.getColumnStatistic(C).getNullsFraction(), 1e-9);
        result = PredicateStatisticsCalculator.statisticsCalculate(not(like(C, "103%")), scan(1000), true,
                completePrefix());
        Assertions.assertEquals(0, result.getColumnStatistic(C).getNullsFraction(), 1e-9);
        ScalarOperator ifnull = call("ifnull", VarcharType.VARCHAR, C, text("u"));
        result = PredicateStatisticsCalculator.statisticsCalculate(new BinaryPredicateOperator(BinaryType.EQ, ifnull,
                text("u")), scan(1000), true, expressions(ifnull, singleton(Map.of("u", 400L, "a", 600L), List.of(), 1000,
                0)));
        Assertions.assertEquals(0.1, result.getColumnStatistic(C).getNullsFraction(), 1e-9);
    }

    @Test
    public void testKeptConditionDoesNotKeepTheGuessOfThePrefix() {
        // The plain estimate takes left(c, 3) IN (...) as half of 1000 rows and caps the NDV of c at 500 with a
        // mark, then at the 450 non-NULL rows of them; c <> '10399' keeps every value of c in its own estimate.
        // Over x: 900 rows, which leave out the NULL rows of c, so c <> '10399' keeps them all, and the 1000 values
        // of c are held to them.
        Histogram histogram = Histogram.forStrings(List.of(), Map.of("10312345", 600L, "10400000", 300L));
        Statistics scan = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0.1, 8,
                        1000, histogram, ColumnStatistic.StatisticType.ESTIMATE))
                .addColumnStatistic(D, new ColumnStatistic(0, 9, 0, 4, 10)).build();
        ScalarOperator predicate = Utils.compoundAnd(new InPredicateOperator(false, left(3), text("103"), text("104")),
                new BinaryPredicateOperator(BinaryType.NE, C, text("10399")));
        Statistics plain = PredicateStatisticsCalculator.statisticsCalculate(predicate, scan, true);
        Assertions.assertEquals(450, plain.getColumnStatistic(C).getDistinctValuesCount(), 1e-6);
        Assertions.assertTrue(plain.getColumnStatistic(C).isDistinctValuesFromDefaultSelectivity());
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(predicate, scan, true, completePrefix());
        Assertions.assertEquals(900, result.getOutputRowCount(), 1e-6);
        Assertions.assertEquals(900, result.getColumnStatistic(C).getDistinctValuesCount(), 1e-6);
        Assertions.assertFalse(result.getColumnStatistic(C).isDistinctValuesFromDefaultSelectivity());
    }

    @Test
    public void testGuessOverXKeepsItsMark() {
        // No value starts with 999, so x leaves one row, and abs(e) IN (1, 2) halves it: d is held to one value by
        // that guess, and keeps the mark the plain estimate does not set on it.
        ColumnRefOperator e = new ColumnRefOperator(3, IntegerType.INT, "e", true);
        Statistics scan = Statistics.buildFrom(scan(1_000_000))
                .addColumnStatistic(e, new ColumnStatistic(0, 1_000_000, 0, 4, 1000)).build();
        ScalarOperator predicate = Utils.compoundAnd(new BinaryPredicateOperator(BinaryType.EQ, left(3), text("999")),
                new InPredicateOperator(false, call("abs", IntegerType.INT, e), ConstantOperator.createInt(1),
                        ConstantOperator.createInt(2)));
        Assertions.assertFalse(PredicateStatisticsCalculator.statisticsCalculate(predicate, scan, true)
                .getColumnStatistic(D).isDistinctValuesFromDefaultSelectivity());
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(predicate, scan, true, completePrefix());
        Assertions.assertEquals(1, result.getColumnStatistic(D).getDistinctValuesCount(), 1e-6);
        Assertions.assertTrue(result.getColumnStatistic(D).isDistinctValuesFromDefaultSelectivity());
    }

    @Test
    public void testManyConstantsOnALargeHead() {
        // 10000 head values whose characters 3 to 7 are the numbers 10000 to 19999, and IN of all of them.
        Map<String, Long> head = new java.util.HashMap<>();
        List<ScalarOperator> children = new ArrayList<>();
        children.add(substrOfX(3, 5));
        for (int i = 10_000; i < 20_000; i++) {
            head.put("AB" + i + "Z", 1L);
            children.add(text(Integer.toString(i)));
        }
        Statistics statistics = Statistics.builder().setOutputRowCount(10_000).addColumnStatistic(X, new ColumnStatistic(
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 10_000, Histogram.forStrings(List.of(), head),
                ColumnStatistic.StatisticType.ESTIMATE)).build();
        Statistics in = Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(10),
                () -> ExpressionStatisticsEstimator.estimate(new InPredicateOperator(false,
                        children.toArray(new ScalarOperator[0])), statistics).orElseThrow());
        Assertions.assertEquals(10_000, in.getOutputRowCount(), 1e-6);
        Statistics notIn = Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(10),
                () -> ExpressionStatisticsEstimator.estimate(new InPredicateOperator(true,
                        children.toArray(new ScalarOperator[0])), statistics).orElseThrow());
        Assertions.assertEquals(1, notIn.getOutputRowCount(), 1e-6);
    }

    @Test
    public void testShorterConstantsAreOneIn() {
        // Over left(c, 10), the 1000 constants of 7 characters of left(c, 8) IN (...) are whole values of x: 10 rows
        // of 1000001 and 20 of 1000002.
        ScanExpressionStatistics expressions = expressions(left(10), singleton(Map.of("1000001", 10L, "1000002", 20L,
                "1031234567", 970L), List.of(), 1000, 0));
        List<ScalarOperator> children = new ArrayList<>();
        children.add(left(8));
        for (int i = 0; i < 1000; i++) {
            children.add(text(Integer.toString(1_000_000 + i)));
        }
        ScalarOperator predicate = new InPredicateOperator(false, children.toArray(new ScalarOperator[0]));
        ExpressionStatisticsMatcher.Result match = ExpressionStatisticsMatcher.match(predicate, expressions, scan(1000));
        Assertions.assertTrue(match.getPredicate() instanceof InPredicateOperator, match.getPredicate().toString());
        Assertions.assertEquals(X, match.getPredicate().getChild(0));
        Assertions.assertEquals(1001, match.getPredicate().getChildren().size());
        Assertions.assertEquals(30, PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(1000), true,
                expressions).getOutputRowCount(), 1e-6);
    }

    @Test
    public void testMarkOfGuessOverXOnlyForItsBound() {
        // Over x the guess held a column to 250 values under its mark, and the row count from x grew to 900 after it.
        ColumnStatistic overX = ColumnStatistic.buildFrom(new ColumnStatistic(0, 9, 0, 4, 250))
                .setDistinctValuesFromDefaultSelectivity(true).build();
        Assertions.assertTrue(PredicateStatisticsCalculator.keepsGuessOverX(250, overX, 900));
        Assertions.assertTrue(PredicateStatisticsCalculator.keepsGuessOverX(900, overX, 900));
        // 400 values counted by the plain estimate are no guess.
        Assertions.assertFalse(PredicateStatisticsCalculator.keepsGuessOverX(400, overX, 900));
        Assertions.assertFalse(PredicateStatisticsCalculator.keepsGuessOverX(250,
                new ColumnStatistic(0, 9, 0, 4, 250), 900));
    }
}
