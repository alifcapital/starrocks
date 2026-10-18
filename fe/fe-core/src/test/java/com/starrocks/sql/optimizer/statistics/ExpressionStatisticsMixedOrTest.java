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
import java.util.Set;

/**
 * An OR with a part that is written over the virtual column x and a part that is not: the first is estimated by x and
 * the second by the ordinary estimate, and the parts add up as the estimate of an OR adds them: the rows of both less
 * the rows of the second part among those of the first.
 *
 * The table has 1,000,000 rows. c is a string of 600,000 values without a histogram, x = left(c, 8) has a complete
 * head of eight values, d has ten values, and e has the head {v: 300,000, w: 100,000} of its own.
 */
public class ExpressionStatisticsMixedOrTest {
    private static final ColumnRefOperator C = new ColumnRefOperator(1, VarcharType.VARCHAR, "c", true);
    private static final ColumnRefOperator D = new ColumnRefOperator(2, IntegerType.INT, "d", true);
    private static final ColumnRefOperator E = new ColumnRefOperator(3, VarcharType.VARCHAR, "e", true);
    private static final ColumnRefOperator ID = new ColumnRefOperator(4, VarcharType.VARCHAR, "id", true);
    private static final ColumnRefOperator NAME = new ColumnRefOperator(5, VarcharType.VARCHAR, "name", true);
    private static final ColumnRefOperator X = new ColumnRefOperator(-1, VarcharType.VARCHAR, "expr#1", true);
    private static final double ROWS = 1_000_000;
    private static final String NUMBER_1 = "26226972800600000205";
    private static final String NUMBER_2 = "26226972500600000204";

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

    private static ConstantOperator text(String value) {
        return ConstantOperator.createVarchar(value);
    }

    private static ScalarOperator eq(ScalarOperator left, ScalarOperator right) {
        return new BinaryPredicateOperator(BinaryType.EQ, left, right);
    }

    private static ScalarOperator not(ScalarOperator operator) {
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, operator);
    }

    private static ScalarOperator in(ScalarOperator left, String... values) {
        List<ScalarOperator> children = new ArrayList<>();
        children.add(left);
        for (String value : values) {
            children.add(text(value));
        }
        return new InPredicateOperator(false, children.toArray(new ScalarOperator[0]));
    }

    // left(c, 5) IN ('20202', '20206', '20212', '20214'): 8000 + 6000 + 4000 + 3000 rows.
    private static ScalarOperator prefixes() {
        return in(left(5), "20202", "20206", "20212", "20214");
    }

    // c IN (two numbers): 2 of 600,000 values, 10/3 rows.
    private static ScalarOperator numbers() {
        return in(C, NUMBER_1, NUMBER_2);
    }

    private static ScalarOperator number() {
        return eq(C, text(NUMBER_1));
    }

    private static ScanExpressionStatistics expressions() {
        Map<String, Long> head = Map.of("20202972", 8000L, "20206972", 6000L, "20212972", 4000L, "20214972", 3000L,
                "17531972", 400_000L, "10100001", 300_000L, "26226972", 5L, "99999999", 278_995L);
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        head.forEach((value, count) -> entries.add(new MultiColumnCombinedStats.McvEntry(List.of(value), count,
                List.of(count))));
        entries.sort((first, second) -> Long.compare(second.getCount(), first.getCount()));
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("x"), (long) ROWS, 8, entries,
                List.of(), List.of(0L));
        group.prepare(VarcharType.VARCHAR);
        ColumnStatistic statistic = group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(left(8));
        return new ScanExpressionStatistics("t", List.of(new ScanExpressionStatistics.Entry(canonical,
                ExpressionStatisticsCanonicalizer.keyOf(canonical), X, statistic)),
                Map.of(Set.of(X), new MultiColumnCombinedStats(8, (long) ROWS, List.of(X), group.getMcv(),
                        group.getNullCounts())));
    }

    private static Statistics scan() {
        List<MultiColumnCombinedStats.McvEntry> head = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("v"), 300_000, List.of(300_000L)),
                new MultiColumnCombinedStats.McvEntry(List.of("w"), 100_000, List.of(100_000L)));
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 20,
                        600_000))
                .addColumnStatistic(D, new ColumnStatistic(0, 9, 0, 4, 10))
                .addColumnStatistic(E, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 50))
                .addColumnStatistic(ID, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 36,
                        ROWS))
                .addColumnStatistic(NAME, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 15,
                        100_000))
                .addMultiColumnStatistics(Set.of(E), new MultiColumnCombinedStats(50, (long) ROWS, List.of(E), head,
                        List.of(0L))).build();
    }

    private static Statistics estimate(ScalarOperator predicate) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(), true, expressions());
    }

    private static double rows(ScalarOperator predicate) {
        return estimate(predicate).getOutputRowCount();
    }

    @Test
    public void testPrefixesAndNumbersAreEstimatedByTheirOwnSources() {
        // The prefixes take 21,000 rows from the head of x. The numbers take 10/3 rows of c, and 2 of them among
        // the 21,000 rows, as c has 21,000 values there at most.
        Assertions.assertEquals(21_000 + 10.0 / 3 - 2, rows(Utils.compoundOr(prefixes(), numbers())), 1e-6);
        // The part over x goes first whatever the order of the query is, so the order does not change the estimate.
        Assertions.assertEquals(21_000 + 10.0 / 3 - 2, rows(Utils.compoundOr(numbers(), prefixes())), 1e-6);
        // The parts alone.
        Assertions.assertEquals(21_000, rows(prefixes()), 1e-6);
        Assertions.assertEquals(10.0 / 3, rows(numbers()), 1e-6);
    }

    @Test
    public void testEqualityParts() {
        ScalarOperator prefix = eq(left(5), text("20202"));
        // 8000 rows of the prefix and 1 + 2/3 rows of the number, 1 of them among the 8000 rows.
        Assertions.assertEquals(8000 + 5.0 / 3 - 1, rows(Utils.compoundOr(prefix, number())), 1e-6);
        Assertions.assertEquals(8000 + 5.0 / 3 - 1, rows(Utils.compoundOr(number(), prefix)), 1e-6);
    }

    @Test
    public void testThreeParts() {
        ScalarOperator prefix = eq(left(5), text("20202"));
        ScalarOperator id = eq(ID, text("0000"));
        // The prefix takes 8000 rows, the id one of them and the two numbers one: C keeps its 600,000 values after the
        // prefix, so the numbers among its 8000 rows are less than a row, and the estimate floors them to one row.
        double expected = 8000 + 10.0 / 3 - 1;
        Assertions.assertEquals(expected, rows(Utils.compoundOr(Utils.compoundOr(prefix, id), numbers())), 1e-6);
        // The part over x in any place of the query goes first, and the others keep their order.
        Assertions.assertEquals(expected, rows(Utils.compoundOr(Utils.compoundOr(id, prefix), numbers())), 1e-6);
        Assertions.assertEquals(expected, rows(Utils.compoundOr(Utils.compoundOr(id, numbers()), prefix)), 1e-6);
    }

    @Test
    public void testAndInsideOr() {
        ScalarOperator prefix = eq(left(5), text("20202"));
        ScalarOperator and = Utils.compoundAnd(prefix, eq(D, ConstantOperator.createInt(5)));
        // The prefix with d = 5 is a tenth of 8000 rows; the numbers take 2 rows of them.
        Assertions.assertEquals(800 + 10.0 / 3 - 2, rows(Utils.compoundOr(and, numbers())), 1e-6);
    }

    @Test
    public void testNegationOfMixedOr() {
        ScalarOperator or = Utils.compoundOr(eq(left(5), text("20202")), number());
        double inner = rows(or);
        Assertions.assertEquals(8000 + 5.0 / 3 - 1, inner, 1e-6);
        Assertions.assertEquals(ROWS - inner, rows(not(or)), 1e-6);
    }

    @Test
    public void testConjunctionsOutsideTheOr() {
        ScalarOperator or = Utils.compoundOr(prefixes(), numbers());
        double both = 21_000 + 10.0 / 3 - 2;
        // The NULL check of the number follows from the OR and does not change it.
        Assertions.assertEquals(both, rows(Utils.compoundAnd(or, new IsNullPredicateOperator(true, C))), 1e-6);
        // A condition on another column takes its share of the rows of the OR.
        Assertions.assertEquals(both / 10, rows(Utils.compoundAnd(or, eq(D, ConstantOperator.createInt(5)))), 1e-6);
    }

    @Test
    public void testPartOnAColumnWithItsOwnHead() {
        ScalarOperator value = eq(E, text("v"));
        // The head of e gives 300,000 rows, and 0.3 of the 21,000 rows of the prefixes among them.
        Assertions.assertEquals(21_000 + 300_000 - 6300, rows(Utils.compoundOr(prefixes(), value)), 1e-6);
        Assertions.assertEquals(21_000 + 300_000 - 6300, rows(Utils.compoundOr(value, prefixes())), 1e-6);
    }

    @Test
    public void testPartOnAColumnWithoutHead() {
        ScalarOperator d = eq(D, ConstantOperator.createInt(3));
        // d = 3 takes 100,000 rows, and a tenth of the 21,000 rows of the prefixes among them.
        Assertions.assertEquals(21_000 + 100_000 - 2100, rows(Utils.compoundOr(prefixes(), d)), 1e-6);
    }

    @Test
    public void testPrefixesOfSeveralLengthsNextToNumbers() {
        // Prefixes of 3, 5 and 8 characters: the head holds every row, so 700,000 + 21,000 + 5 rows exactly.
        ScalarOperator xParts = Utils.compoundOr(List.of(in(left(3), "175", "101"), prefixes(),
                eq(left(8), text("26226972"))));
        Assertions.assertEquals(721_005, rows(xParts), 1e-6);
        // Four numbers add their own 20/3 rows at most, with the NULL check of the column that follows from the OR;
        // the estimate of the OR takes one row off for the rows of each part among those before it.
        ScalarOperator numbers = in(C, NUMBER_1, NUMBER_2, "26226972800600000206", "26226972800600000207");
        ScalarOperator all = Utils.compoundAnd(Utils.compoundOr(xParts, numbers), new IsNullPredicateOperator(true, C));
        Assertions.assertEquals(721_005, rows(all), 20.0 / 3);
        // The numbers listed first make the same estimate.
        Assertions.assertEquals(rows(all), rows(Utils.compoundAnd(Utils.compoundOr(numbers, xParts),
                new IsNullPredicateOperator(true, C))), 1e-6);
    }

    @Test
    public void testGroupByThePrefixAfterMixedOr() {
        Statistics result = estimate(Utils.compoundOr(prefixes(), numbers()));
        ScanExpressionStatistics after = result.getScanExpressionStatistics();
        Assertions.assertNotNull(after);
        Assertions.assertTrue(after.describesScanRows(X));
        ColumnStatistic groups = ExpressionProjectionStatistics.calculate(left(5), after, result.getOutputRowCount());
        Assertions.assertNotNull(groups);
        // The rows of the numbers have any value of x, so x keeps its whole head of eight prefixes, and its NDV is the
        // one the estimate of the scan gave: the four prefixes of the first part and the 10/3 values that the rows of
        // the numbers can hold.
        Assertions.assertEquals(4 + 10.0 / 3, groups.getDistinctValuesCount(), 1e-9);
        Assertions.assertTrue(groups.getDistinctValuesCount() <= result.getOutputRowCount());

        // The order of the parts in the query does not change it.
        Statistics numbersFirst = estimate(Utils.compoundOr(numbers(), prefixes()));
        ColumnStatistic reversed = ExpressionProjectionStatistics.calculate(left(5),
                numbersFirst.getScanExpressionStatistics(), numbersFirst.getOutputRowCount());
        Assertions.assertEquals(4 + 10.0 / 3, reversed.getDistinctValuesCount(), 1e-9);
    }

    @Test
    public void testGroupByThePrefixAfterAnOrWithAWideColumnPart() {
        // d = 3 holds 100,000 rows with any value of x, and the head of x has eight prefixes of five characters. The
        // groups of the OR are not those of its first part, one prefix, and are those of the rows of d = 3: all eight.
        ScalarOperator prefix = eq(left(5), text("20202"));
        ScalarOperator wide = eq(D, ConstantOperator.createInt(3));
        Statistics onlyWide = estimate(wide);
        ColumnStatistic wideGroups = ExpressionProjectionStatistics.calculate(left(5),
                onlyWide.getScanExpressionStatistics(), onlyWide.getOutputRowCount());
        Assertions.assertEquals(8, wideGroups.getDistinctValuesCount(), 1e-9);
        for (ScalarOperator or : List.of(Utils.compoundOr(prefix, wide), Utils.compoundOr(wide, prefix))) {
            Statistics result = estimate(or);
            Assertions.assertEquals(8000 + 100_000 - 800, result.getOutputRowCount(), 1e-6);
            ColumnStatistic groups = ExpressionProjectionStatistics.calculate(left(5),
                    result.getScanExpressionStatistics(), result.getOutputRowCount());
            Assertions.assertNotNull(groups);
            Assertions.assertEquals(8, groups.getDistinctValuesCount(), 1e-9);
        }
        // The prefix of three characters: 175, 101, 202, 262 and 999 are the five of the head.
        Statistics result = estimate(Utils.compoundOr(prefix, wide));
        Assertions.assertEquals(5, ExpressionProjectionStatistics
                .calculate(left(3), result.getScanExpressionStatistics(), result.getOutputRowCount())
                .getDistinctValuesCount(), 1e-9);
    }

    @Test
    public void testNullRowsOfXStayAfterAnOrWithAColumnPart() {
        // A condition over x alone is never true on a NULL of x, while a part on d is true on a row with c NULL.
        Statistics halfNull = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0.5, 20,
                        400))
                .addColumnStatistic(D, new ColumnStatistic(0, 9, 0, 4, 10)).build();
        ScalarOperator mixed = Utils.compoundOr(eq(left(3), text("103")), eq(D, ConstantOperator.createInt(3)));
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(mixed, halfNull, true,
                halfNullExpressions());
        ScanExpressionStatistics after = result.getScanExpressionStatistics();
        Assertions.assertTrue(after.describesScanRows(X));
        ScalarOperator onlyPrefix = eq(left(3), text("103"));
        Statistics prefixResult = PredicateStatisticsCalculator.statisticsCalculate(onlyPrefix, halfNull, true,
                halfNullExpressions());
        ColumnStatistic nulls = ExpressionProjectionStatistics.calculate(left(3), after, result.getOutputRowCount());
        ColumnStatistic noNulls = ExpressionProjectionStatistics.calculate(left(3),
                prefixResult.getScanExpressionStatistics(), prefixResult.getOutputRowCount());
        Assertions.assertEquals(0, noNulls.getNullsFraction(), 1e-9);
        Assertions.assertTrue(nulls.getNullsFraction() > 0, String.valueOf(nulls.getNullsFraction()));
        Assertions.assertEquals(after.getEntry(X).getStatistic().getNullsFraction(), nulls.getNullsFraction(), 1e-9);
    }

    @Test
    public void testGroupByAfterAnOrOverXAndAMixedOr() {
        // Four prefixes of one OR, and an OR of one of them with a number: the rows have at most two prefixes, the one
        // of the first part and one more for the number. The distribution rebuilt from the first OR alone has four, so
        // the NDV the estimate of the scan gave bounds it.
        ScalarOperator four = Utils.compoundOr(List.of(eq(left(5), text("20202")), eq(left(5), text("20206")),
                eq(left(5), text("20212")), eq(left(5), text("20214"))));
        ScalarOperator mixed = Utils.compoundOr(eq(left(5), text("20202")), number());
        Statistics result = estimate(Utils.compoundAnd(four, mixed));
        ScanExpressionStatistics after = result.getScanExpressionStatistics();
        ColumnStatistic groups = ExpressionProjectionStatistics.calculate(left(5), after, result.getOutputRowCount());
        Assertions.assertNotNull(groups);
        // The number adds 1.00014 values to the one of the prefix: its rows are a fraction of a row of the table.
        Assertions.assertEquals(2, groups.getDistinctValuesCount(), 1e-3);
        Statistics onlyFour = estimate(four);
        Assertions.assertEquals(4, ExpressionProjectionStatistics.calculate(left(5),
                onlyFour.getScanExpressionStatistics(), onlyFour.getOutputRowCount()).getDistinctValuesCount(), 1e-9);
    }

    // 1000 rows, half of them NULL in c; the head of x = left(c, 8) holds the 500 others: 300 and 200.
    private static ScanExpressionStatistics halfNullExpressions() {
        List<MultiColumnCombinedStats.McvEntry> entries = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("10312345"), 300, List.of(300L)),
                new MultiColumnCombinedStats.McvEntry(List.of("10400000"), 200, List.of(200L)));
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("x"), 1000, 2, entries, List.of(),
                List.of(500L));
        group.prepare(VarcharType.VARCHAR);
        ColumnStatistic statistic = group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(left(8));
        return new ScanExpressionStatistics("t", List.of(new ScanExpressionStatistics.Entry(canonical,
                ExpressionStatisticsCanonicalizer.keyOf(canonical), X, statistic)),
                Map.of(Set.of(X), new MultiColumnCombinedStats(2, 1000, List.of(X), entries, List.of(500L))));
    }

    @Test
    public void testNullRowsOfTheColumnStayForAPartOnIt() {
        Statistics halfNull = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0.5, 20,
                        400)).build();
        ScalarOperator prefix = eq(left(3), text("103"));
        ScalarOperator notNull = new IsNullPredicateOperator(true, C);
        // The prefix takes 300 rows and the part 500, the rows of c that are not NULL. The part among the 300 rows of
        // the prefix is the same share, 150: the estimate of the part keeps the NULL rows of c in its statistics. A
        // NULL fraction of 0 for c would give the part 1000 rows, and 300 among those of the prefix.
        Assertions.assertEquals(300 + 500 - 150, PredicateStatisticsCalculator.statisticsCalculate(
                Utils.compoundOr(prefix, notNull), halfNull, true, halfNullExpressions()).getOutputRowCount(), 1e-6);
    }

    @Test
    public void testNegatedPartAndSubstringPartOverX() {
        // NOT of the four prefixes leaves 979,000 rows of the million; the numbers take 10/3 rows of the table and 2 of
        // 600,000 values among the 979,000 rows.
        ScalarOperator notPrefixes = not(prefixes());
        Assertions.assertEquals(979_000 + 10.0 / 3 - 979_000 * 2.0 / 600_000,
                rows(Utils.compoundOr(notPrefixes, numbers())), 1e-6);
        Assertions.assertEquals(979_000 + 10.0 / 3 - 979_000 * 2.0 / 600_000,
                rows(Utils.compoundOr(numbers(), notPrefixes)), 1e-6);
        // The characters 6 to 8 of the head values: 972 in six of them, 421,005 rows. Among them c has at most 421,005
        // values, so the numbers take 2 rows.
        ScalarOperator inside = eq(call("substr", C, ConstantOperator.createInt(6), ConstantOperator.createInt(3)),
                text("972"));
        Assertions.assertEquals(421_005 + 10.0 / 3 - 2, rows(Utils.compoundOr(inside, numbers())), 1e-6);
    }

    @Test
    public void testAndOfAPrefixAndAColumnUnderTheOr() {
        // 400,000 rows start with 175, and 0.3 of them have e = 'v'; the prefix of 5 characters takes 8000 rows. The AND
        // among the 8000 rows of the prefix is the floor of one row.
        ScalarOperator and = Utils.compoundAnd(eq(left(3), text("175")), eq(E, text("v")));
        Assertions.assertEquals(8000 + 120_000 - 1, rows(Utils.compoundOr(eq(left(5), text("20202")), and)), 1e-6);
        Assertions.assertEquals(8000 + 120_000 - 1, rows(Utils.compoundOr(and, eq(left(5), text("20202")))), 1e-6);
    }

    @Test
    public void testPartOnThePrefixOfAnotherColumn() {
        // name has 100,000 values of 15 characters, so a prefix of 3 has 100,000 ^ (3 / 15) = 10 of them: 100,000
        // rows. The estimate of the prefix of c keeps the statistics of name, so among its 8000 rows the part takes a
        // tenth.
        ScalarOperator name = eq(call("left", NAME, ConstantOperator.createInt(3)), text("abc"));
        double among = 8000 / Math.pow(100_000, 3.0 / 15);
        Assertions.assertEquals(8000 + 100_000 - among, rows(Utils.compoundOr(eq(left(5), text("20202")), name)), 1e-6);
        Assertions.assertEquals(8000 + 100_000 - among, rows(Utils.compoundOr(name, eq(left(5), text("20202")))), 1e-6);
    }
}
