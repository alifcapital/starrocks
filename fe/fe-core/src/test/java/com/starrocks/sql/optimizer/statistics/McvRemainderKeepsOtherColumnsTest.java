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
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A column outside the column group of an MCV list keeps its distribution through the estimate of the equalities and
 * the group, so a range on it after them is estimated by its histogram.
 */
public class McvRemainderKeepsOtherColumnsTest {
    private static final double ROWS = 1_000_000;
    private static final ColumnRefOperator STATUS = new ColumnRefOperator(1, IntegerType.INT, "status", true);
    private static final ColumnRefOperator GATE = new ColumnRefOperator(2, IntegerType.INT, "gate", true);
    private static final ColumnRefOperator TYPE = new ColumnRefOperator(3, IntegerType.INT, "type", true);
    private static final ColumnRefOperator CREATED = new ColumnRefOperator(4, DateType.DATETIME, "created", true);
    private static final ColumnRefOperator OTHER = new ColumnRefOperator(5, IntegerType.INT, "other", true);

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.createDefaultCtx();
    }

    private static MultiColumnCombinedStats.McvEntry entry(int status, int gate, int type, long count,
                                                           long statusCount, long gateCount, long typeCount) {
        return new MultiColumnCombinedStats.McvEntry(List.of(String.valueOf(status), String.valueOf(gate),
                String.valueOf(type)), count, List.of(statusCount, gateCount, typeCount));
    }

    private static double seconds(int month, int day) {
        return Utils.getLongFromDateTime(LocalDateTime.of(2026, month, day, 0, 0));
    }

    // 1,000,000 rows of five tuples: status 1 has 600,000 rows and status 2 has 400,000; gate 9 has 450,000 rows and
    // type 0 has 450,000. Of the rows of status 1, 300,000 have gate 9 or 14 and so match the OR of the query. The
    // column created is outside the group: seven months of 300,000 rows together and August, which has 700,000 rows.
    private static Statistics table(boolean withSingleton) {
        return tableOf(withSingleton ? singletonOfCreated(2) : null, null);
    }

    // The table with the given groups of created and of other, which are not in it when null.
    private static Statistics tableOf(MultiColumnCombinedStats ofCreated, MultiColumnCombinedStats ofOther) {
        List<Bucket> buckets = new ArrayList<>();
        long total = 0;
        for (int month = 1; month <= 8; month++) {
            total += month == 8 ? 700_000 : 300_000 / 7;
            buckets.add(new Bucket(seconds(month, 1), seconds(month + 1, 1) - 1, month == 8 ? 1_000_000 : total, 10L, 100L));
        }
        Statistics.Builder builder = Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(STATUS, new ColumnStatistic(1, 2, 0, 4, 2))
                .addColumnStatistic(GATE, new ColumnStatistic(9, 40, 0, 4, 4))
                .addColumnStatistic(TYPE, new ColumnStatistic(0, 1, 0, 4, 2))
                .addColumnStatistic(OTHER, new ColumnStatistic(0, 10, 0, 4, 10))
                .addColumnStatistic(CREATED, ColumnStatistic.builder().setMinValue(seconds(1, 1))
                        .setMaxValue(seconds(9, 1) - 1).setNullsFraction(0).setAverageRowSize(8)
                        .setDistinctValuesCount(1000).setHistogram(new Histogram(buckets, Map.of())).build());
        builder.addMultiColumnStatistics(Set.of(STATUS, GATE, TYPE), new MultiColumnCombinedStats(5, (long) ROWS,
                List.of(STATUS, GATE, TYPE), List.of(entry(1, 9, 0, 200_000, 600_000, 450_000, 450_000),
                entry(1, 14, 0, 100_000, 600_000, 100_000, 450_000),
                entry(1, 30, 1, 300_000, 600_000, 300_000, 550_000),
                entry(2, 9, 1, 250_000, 400_000, 450_000, 550_000),
                entry(2, 40, 0, 150_000, 400_000, 150_000, 450_000)), List.of(0L, 0L, 0L)));
        if (ofCreated != null) {
            builder.addMultiColumnStatistics(Set.of(CREATED), ofCreated);
        }
        if (ofOther != null) {
            builder.addMultiColumnStatistics(Set.of(OTHER), ofOther);
        }
        return builder.build();
    }

    // MCV(created) with the given number of timestamps in the head: the first two are the ones of the table of the tests.
    private static MultiColumnCombinedStats singletonOfCreated(int head) {
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        entries.add(new MultiColumnCombinedStats.McvEntry(List.of("2026-08-15 10:00:00"), 3000, List.of(3000L)));
        entries.add(new MultiColumnCombinedStats.McvEntry(List.of("2026-03-15 10:00:00"), 2000, List.of(2000L)));
        for (int i = 2; i < head; i++) {
            entries.add(new MultiColumnCombinedStats.McvEntry(List.of(LocalDateTime.of(2026, 1, 1, 0, 0).plusMinutes(7L * i)
                    .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), 10, List.of(10L)));
        }
        return new MultiColumnCombinedStats(1000 + head, (long) ROWS, List.of(CREATED), entries, List.of(0L));
    }

    private static ScalarOperator compare(BinaryType type, ColumnRefOperator column, int value) {
        return new BinaryPredicateOperator(type, column, ConstantOperator.createInt(value));
    }

    // hours_add(created, 5) in [August, September), the interval of the month as the query writes it.
    private static ScalarOperator interval() {
        Type[] types = {DateType.DATETIME, IntegerType.INT};
        Function function = ExprUtils.getBuiltinFunction("hours_add", types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        ScalarOperator shifted = new CallOperator("hours_add", DateType.DATETIME,
                List.of(CREATED, ConstantOperator.createInt(5)), function);
        return Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.GE, shifted,
                        ConstantOperator.createDatetime(LocalDateTime.of(2026, 8, 1, 0, 0))),
                new BinaryPredicateOperator(BinaryType.LT, shifted,
                        ConstantOperator.createDatetime(LocalDateTime.of(2026, 9, 1, 0, 0))));
    }

    private static ScalarOperator orOfGroup() {
        return Utils.compoundOr(new InPredicateOperator(false, GATE, ConstantOperator.createInt(9),
                ConstantOperator.createInt(14), ConstantOperator.createInt(17), ConstantOperator.createInt(25)),
                compare(BinaryType.EQ, TYPE, 0));
    }

    private static double rows(ScalarOperator predicate, boolean withSingleton) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, table(withSingleton)).getOutputRowCount();
    }

    private static Statistics estimate(ScalarOperator predicate, boolean withSingleton) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, table(withSingleton));
    }

    // The rows of the interval of August by the histogram of created, which holds 700,000 of the million rows in the
    // month and the shift of 5 hours back into July.
    private static double share(boolean withSingleton) {
        return rows(interval(), withSingleton) / ROWS;
    }

    @Test
    public void testHistogramOfTheColumnOutsideTheGroupGivesTheShareOfTheInterval() {
        for (boolean singleton : new boolean[] {false, true}) {
            double share = share(singleton);
            Assertions.assertEquals(695_584.0 / ROWS, share, 1e-9);
            // The month is most of the rows by the histogram, and a share of the range of the column would be one eighth.
            Assertions.assertTrue(share > 0.6, String.valueOf(share));
        }
    }

    @Test
    public void testEqualityAndIntervalIsTheShareOfTheEqualityByTheShareOfTheInterval() {
        for (boolean singleton : new boolean[] {false, true}) {
            double share = share(singleton);
            ScalarOperator status = compare(BinaryType.EQ, STATUS, 1);
            Assertions.assertEquals(600_000 * share, rows(Utils.compoundAnd(status, interval()), singleton), 1e-6,
                    "singleton " + singleton);
        }
    }

    @Test
    public void testEqualityOrOfTheGroupAndIntervalIsTheShareOfTheGroupByTheShareOfTheInterval() {
        for (boolean singleton : new boolean[] {false, true}) {
            double share = share(singleton);
            ScalarOperator status = compare(BinaryType.EQ, STATUS, 1);
            // Of the 600,000 rows of status 1, 300,000 have gate 9 or 14.
            Assertions.assertEquals(300_000, rows(Utils.compoundAnd(status, orOfGroup()), singleton), 1e-6);
            Assertions.assertEquals(300_000 * share, rows(Utils.compoundAnd(status, Utils.compoundAnd(orOfGroup(),
                    interval())), singleton), 1e-6, "singleton " + singleton);
            // The same conjuncts in any order.
            Assertions.assertEquals(300_000 * share, rows(Utils.compoundAnd(List.of(interval(), orOfGroup(), status)),
                    singleton), 1e-6, "singleton " + singleton);
            Assertions.assertEquals(300_000 * share, rows(Utils.compoundAnd(List.of(orOfGroup(), status, interval())),
                    singleton), 1e-6, "singleton " + singleton);
        }
    }

    @Test
    public void testEqualitiesOrOfTheGroupAndIntervalIsTheShareOfTheGroupByTheShareOfTheInterval() {
        for (boolean singleton : new boolean[] {false, true}) {
            double share = share(singleton);
            ScalarOperator status = compare(BinaryType.EQ, STATUS, 1);
            ScalarOperator type = compare(BinaryType.EQ, TYPE, 0);
            // Status 1 and type 0: the tuples of gate 9 and 14, 300,000 rows, and gate 9 or 14 are both of them.
            Assertions.assertEquals(300_000 * share, rows(Utils.compoundAnd(List.of(status, type, orOfGroup(), interval())),
                    singleton), 1, "singleton " + singleton);
            // The OR of a gate and a type: of status 1, gate 9 (200,000) and type 1 (300,000 of gate 30).
            ScalarOperator gateOrType = Utils.compoundOr(compare(BinaryType.EQ, GATE, 9), compare(BinaryType.EQ, TYPE, 1));
            Assertions.assertEquals(500_000 * share, rows(Utils.compoundAnd(List.of(status, gateOrType, interval())),
                    singleton), 1, "singleton " + singleton);
        }
    }

    @Test
    public void testOrOfTheGroupAndIntervalWithoutTheEqualityIsAsBefore() {
        for (boolean singleton : new boolean[] {false, true}) {
            // 700,000 rows have gate 9 or 14, or type 0.
            Assertions.assertEquals(700_000 * share(singleton), rows(Utils.compoundAnd(orOfGroup(), interval()),
                    singleton), 1e-6, "singleton " + singleton);
        }
    }

    @Test
    public void testColumnOutsideTheGroupKeepsItsStatisticsAfterTheGroup() {
        for (boolean singleton : new boolean[] {false, true}) {
            ScalarOperator status = compare(BinaryType.EQ, STATUS, 1);
            Statistics result = estimate(Utils.compoundAnd(status, Utils.compoundAnd(orOfGroup(), interval())), singleton);
            ColumnStatistic created = result.getColumnStatistic(CREATED);
            // The histogram of the column is kept, cut to the interval, and its values are no more than the rows.
            Assertions.assertNotNull(created.getHistogram(), "singleton " + singleton);
            Assertions.assertTrue(created.getDistinctValuesCount() <= result.getOutputRowCount(), "singleton " + singleton);
            Assertions.assertTrue(created.getMinValue() >= seconds(7, 31), "singleton " + singleton);
        }
    }

    private static MultiColumnCombinedStats groupOf(Statistics statistics) {
        return statistics.getMultiColumnCombinedStats().get(Set.of(STATUS, GATE, TYPE));
    }

    @Test
    public void testFilterOfAnyConjunctionDropsTheGroupOfAConjunctOutsideIt() {
        Statistics input = table(false);
        Statistics output = input.withOutputRowCount(417_350.4);
        ScalarOperator status = compare(BinaryType.EQ, STATUS, 1);
        // A conjunct of another column may correlate with the group: the NOT NULL check of a column with NULLs, or any
        // conjunct that the estimate did not multiply as independent of the group.
        Assertions.assertNull(groupOf(McvStatisticsPropagation.filter(Utils.compoundAnd(status, interval()), input, output)));
        // The conjuncts of the group alone, the same as before.
        MultiColumnCombinedStats group = groupOf(McvStatisticsPropagation.filter(status, input,
                input.withOutputRowCount(600_000)));
        Assertions.assertNotNull(group);
        Assertions.assertEquals(600_000, group.getRowCount(), 1e-6);
    }

    @Test
    public void testGroupIsFilteredByItsOwnConjunctsWhenConjunctsOfOtherColumnsFilterToo() {
        Statistics input = table(false);
        // The output of the conjunction: status 1 and the month, 417,350 rows.
        Statistics output = input.withOutputRowCount(417_350.4);
        ScalarOperator status = compare(BinaryType.EQ, STATUS, 1);
        Statistics filtered = McvStatisticsPropagation.filterIndependent(Utils.compoundAnd(status, interval()), input, output);
        MultiColumnCombinedStats group = groupOf(filtered);
        Assertions.assertNotNull(group);
        // The three tuples of status 1 are the group, scaled to the rows of the output, 417,350: the counts of the
        // tuples are in proportion to the 200,000, 100,000 and 300,000 rows they have, and add up to the rows.
        Assertions.assertEquals(3, group.getMcv().size());
        Assertions.assertEquals(417_350, group.getRowCount(), 1e-9);
        Assertions.assertEquals(List.of(139_117L, 69_558L, 208_675L),
                group.getMcv().stream().map(MultiColumnCombinedStats.McvEntry::getCount).toList());
        Assertions.assertEquals(417_350, group.getMcv().stream().mapToLong(MultiColumnCombinedStats.McvEntry::getCount).sum());
        Assertions.assertEquals(group.getRowCount(), group.getMcvDistribution().getTotalRowsLong(), 0);
        // The conjuncts of the other columns alone do not change the group.
        Statistics untouched = McvStatisticsPropagation.filterIndependent(interval(), input, output);
        Assertions.assertSame(input.getMultiColumnCombinedStats().get(Set.of(STATUS, GATE, TYPE)), groupOf(untouched));
        // The column outside the group has the statistics of the output, and the columns of the group have no
        // distribution of the group alone: no histogram, and no group of one column in their place.
        Assertions.assertSame(output.getColumnStatistic(CREATED), filtered.getColumnStatistic(CREATED));
        for (ColumnRefOperator column : List.of(STATUS, GATE, TYPE)) {
            Assertions.assertNull(filtered.getColumnStatistic(column).getHistogram(), column.getName());
        }
        Assertions.assertEquals(Set.of(Set.of(STATUS, GATE, TYPE)), filtered.getMultiColumnCombinedStats().keySet());
    }

    @Test
    public void testHeadOfTheGroupThatIsCompleteStaysCompleteWhateverTheOutputIs() {
        Statistics input = table(false);
        ScalarOperator status = compare(BinaryType.EQ, STATUS, 1);
        // The counts add up to the rows for rows that the counts rounded one by one would not add up to: 250,001 has
        // 83,333.67, 41,666.83 and 125,000.5 for the three tuples.
        // A statistics holds one row at least, so the smallest output is a row. The rows are rounded to the nearest.
        for (double output : new double[] {1_000_000, 417_350, 250_001, 250_000.7, 100_000, 1_001, 7, 3, 1}) {
            long rows = Math.round(output);
            MultiColumnCombinedStats group = groupOf(McvStatisticsPropagation.filterIndependent(
                    Utils.compoundAnd(status, interval()), input, input.withOutputRowCount(output)));
            Assertions.assertNotNull(group, "output " + output);
            long sum = group.getMcv().stream().mapToLong(MultiColumnCombinedStats.McvEntry::getCount).sum();
            Assertions.assertEquals(rows, sum, "output " + output);
            Assertions.assertEquals(rows, group.getRowCount(), 0, "output " + output);
            Assertions.assertEquals(group.getRowCount(), group.getMcvDistribution().getTotalRowsLong(), 0, "output " + output);
        }
        Assertions.assertEquals(List.of(83_334L, 41_667L, 125_000L), groupOf(McvStatisticsPropagation.filterIndependent(
                Utils.compoundAnd(status, interval()), input, input.withOutputRowCount(250_001))).getMcv().stream()
                .map(MultiColumnCombinedStats.McvEntry::getCount).toList());
    }

    @Test
    public void testGroupWithATailIsScaledToTheRowsOfTheOutputAndKeepsItsTail() {
        // The head leaves out the tuple of status 2 and gate 40, so 150,000 rows are the tail of the group.
        Statistics complete = table(false);
        MultiColumnCombinedStats whole = groupOf(complete);
        List<MultiColumnCombinedStats.McvEntry> head = whole.getMcv().subList(0, 4);
        Statistics input = Statistics.buildFrom(complete).addMultiColumnStatistics(Set.of(STATUS, GATE, TYPE),
                new MultiColumnCombinedStats(5, ROWS, List.of(STATUS, GATE, TYPE), head, List.of())).build();
        ScalarOperator status = compare(BinaryType.EQ, STATUS, 1);
        MultiColumnCombinedStats alone = groupOf(McvStatisticsPropagation.filter(status, input,
                input.withOutputRowCount(PredicateStatisticsCalculator.statisticsCalculate(status, input).getOutputRowCount())));
        long aloneHead = alone.getMcv().stream().mapToLong(MultiColumnCombinedStats.McvEntry::getCount).sum();
        Assertions.assertTrue(aloneHead < alone.getRowCount(), "the equality leaves a tail");
        for (double rows : new double[] {1_000_000, 900_000, 417_350.4, 100_000, 5_000, 3, 1}) {
            MultiColumnCombinedStats group = groupOf(McvStatisticsPropagation.filterIndependent(
                    Utils.compoundAnd(status, interval()), input, input.withOutputRowCount(rows)));
            Assertions.assertNotNull(group, "output " + rows);
            // The head and the tail add up to the rows of the output, and the head leaves a tail whatever they are.
            long sum = group.getMcv().stream().mapToLong(MultiColumnCombinedStats.McvEntry::getCount).sum();
            Assertions.assertEquals(rows, group.getRowCount(), 1e-9, "output " + rows);
            Assertions.assertTrue(sum < group.getRowCount(), "output " + rows);
            Assertions.assertTrue(group.getMcvDistribution().getTotalRowsLong() < group.getRowCount(), "output " + rows);
            // The share of the head is that of the equality alone: the units of the group are those of the output.
            Assertions.assertEquals((double) aloneHead / alone.getRowCount(), sum / group.getRowCount(), 1.5 / rows,
                    "output " + rows);
        }
    }

    @Test
    public void testConsumersOfAGroupScaledToTheOutputAreConsistentWithIt() {
        Statistics input = table(false);
        ScalarOperator status = compare(BinaryType.EQ, STATUS, 1);
        for (long rows : new long[] {417_350, 250_001, 1_001}) {
            Statistics filtered = McvStatisticsPropagation.filterIndependent(Utils.compoundAnd(status, interval()), input,
                    input.withOutputRowCount(rows));
            // A complete head makes the groups of an aggregate exactly the tuples of the head: 3, whatever rows the
            // aggregate has. A head the counts of which did not add up to the rows would give the rows of the aggregate.
            Statistics aggregate = McvAggregateStatistics.derive(List.of(GATE, TYPE), filtered, filtered.withOutputRowCount(100));
            Assertions.assertEquals(3, aggregate.getMultiColumnCombinedStats().get(Set.of(GATE, TYPE)).getRowCount(), 0,
                    "output " + rows);
            // The rows of the tuples of the head are its counts, and add up to the rows of the output.
            SkewJoinStatistics.Distribution distribution = SkewJoinStatistics.find(filtered, List.of(GATE, TYPE), 10);
            Assertions.assertNotNull(distribution, "output " + rows);
            Assertions.assertEquals(3, distribution.entries().size(), "output " + rows);
            Assertions.assertEquals(rows, distribution.entries().stream().mapToDouble(SkewJoinStatistics.Entry::rows).sum(),
                    1e-6, "output " + rows);
        }
    }

    @Test
    public void testSingletonGroupOfAColumnOutsideTheEqualityKeepsTheStatisticOfTheOutput() {
        Statistics input = table(true);
        ColumnStatistic narrowed = PredicateStatisticsCalculator.statisticsCalculate(interval(), input)
                .getColumnStatistic(CREATED);
        Assertions.assertNotSame(input.getColumnStatistic(CREATED).getHistogram(), narrowed.getHistogram());
        Statistics output = Statistics.buildFrom(input).setOutputRowCount(417_350.4)
                .addColumnStatistic(CREATED, narrowed).build();
        Statistics filtered = McvStatisticsPropagation.filterIndependent(Utils.compoundAnd(compare(BinaryType.EQ, STATUS, 1),
                interval()), input, output);
        Assertions.assertSame(narrowed, filtered.getColumnStatistic(CREATED));
    }

    @Test
    public void testGroupIsDroppedWhenAConjunctReadsItsColumnsAndOthers() {
        Statistics input = table(false);
        Statistics output = input.withOutputRowCount(100_000);
        ScalarOperator status = compare(BinaryType.EQ, STATUS, 1);
        // gate = other reads a column of the group and one outside it: their correlation is unknown.
        ScalarOperator mixed = new BinaryPredicateOperator(BinaryType.EQ, GATE, OTHER);
        Assertions.assertNull(groupOf(McvStatisticsPropagation.filterIndependent(Utils.compoundAnd(status, mixed), input,
                output)));
        Assertions.assertNull(groupOf(McvStatisticsPropagation.filterIndependent(mixed, input, output)));
        // The same conjunct next to the interval.
        Assertions.assertNull(groupOf(McvStatisticsPropagation.filterIndependent(Utils.compoundAnd(List.of(status, mixed,
                interval())), input, output)));
    }

    private static long headOf(MultiColumnCombinedStats group) {
        return group.getMcv().stream().mapToLong(MultiColumnCombinedStats.McvEntry::getCount).sum();
    }

    // The conditional groups that the estimate of the equalities and the groups keeps for a conjunction all of whose
    // conjuncts the groups consume, so that no remainder is estimated after it.
    private static Statistics keptByTheEstimate(ScalarOperator conjunction, Statistics input) {
        return StatisticsEstimateUtils.computeCompoundStatsWithMultiColumnOptimize(conjunction, input,
                MultiColumnMcvEstimator.estimate(Utils.extractConjuncts(conjunction), input), true);
    }

    @Test
    public void testTailOfAGroupOfOneColumnStaysWhenTheHistogramGivesFewerRowsThanTheHead() {
        // The head of MCV(created) holds a timestamp of the interval with 800,000 rows, whose rows outside the head are
        // not known (the tuple has no count of its component). The histogram says that the interval has 695,584, fewer
        // than the head: rows estimated so would leave no tail, and the group would be taken for a complete one, with the
        // NDV and the marginals of a complete head. The estimate of the group counts the rest of the table as its tail.
        List<MultiColumnCombinedStats.McvEntry> head = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("2026-08-15 10:00:00"), 800_000, List.of()),
                new MultiColumnCombinedStats.McvEntry(List.of("2026-03-15 10:00:00"), 2_000, List.of()));
        Statistics input = tableOf(new MultiColumnCombinedStats(1002, (long) ROWS, List.of(CREATED), head, List.of(0L)),
                null);
        Assertions.assertTrue(PredicateStatisticsCalculator.statisticsCalculate(interval(), input, false)
                .getOutputRowCount() < 800_000);
        double own = PredicateStatisticsCalculator.statisticsCalculate(interval(), input).getOutputRowCount();
        Assertions.assertTrue(own > 800_000, String.valueOf(own));
        // The group of its own conjuncts alone, as the filter of that group makes it.
        MultiColumnCombinedStats alone = McvStatisticsPropagation.filter(interval(), input, input.withOutputRowCount(own))
                .getMultiColumnCombinedStats().get(Set.of(CREATED));
        Assertions.assertNotNull(alone);
        Assertions.assertTrue(headOf(alone) < alone.getRowCount());
        // The same group when the conjuncts of another column filter too: the share of the head is the same, and a tail
        // stays.
        MultiColumnCombinedStats group = keptByTheEstimate(Utils.compoundAnd(compare(BinaryType.EQ, STATUS, 1), interval()),
                input).getMultiColumnCombinedStats().get(Set.of(CREATED));
        Assertions.assertNotNull(group);
        Assertions.assertTrue(headOf(group) < group.getRowCount(), "a tail stays");
        Assertions.assertEquals((double) headOf(alone) / alone.getRowCount(), (double) headOf(group) / group.getRowCount(),
                1e-4);
    }

    @Test
    public void testEqualityAndInequalityOfOneColumnGiveTheGroupTheRowsOfBoth() {
        // MCV(other): 5 has 100,000 rows, 7 has 50,000 and 2 has 30,000. The group holds other = 5 and other > 3, and the
        // rows of the group are those of both, as the filter of the group alone counts them.
        List<MultiColumnCombinedStats.McvEntry> head = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("5"), 100_000, List.of(100_000L)),
                new MultiColumnCombinedStats.McvEntry(List.of("7"), 50_000, List.of(50_000L)),
                new MultiColumnCombinedStats.McvEntry(List.of("2"), 30_000, List.of(30_000L)));
        Statistics input = tableOf(null, new MultiColumnCombinedStats(10, (long) ROWS, List.of(OTHER), head, List.of(0L)));
        ScalarOperator status = compare(BinaryType.EQ, STATUS, 1);
        ScalarOperator equality = compare(BinaryType.EQ, OTHER, 5);
        ScalarOperator inequality = compare(BinaryType.GT, OTHER, 3);
        ScalarOperator conjunction = Utils.compoundAnd(List.of(status, equality, inequality));
        Statistics kept = keptByTheEstimate(conjunction, input);
        MultiColumnCombinedStats group = kept.getMultiColumnCombinedStats().get(Set.of(OTHER));
        Assertions.assertNotNull(group);
        // The filter of the same conjuncts without the shortcut: the rows are those of their estimate.
        MultiColumnCombinedStats reference = McvStatisticsPropagation.filterIndependent(conjunction, input,
                input.withOutputRowCount(kept.getOutputRowCount())).getMultiColumnCombinedStats().get(Set.of(OTHER));
        Assertions.assertNotNull(reference);
        Assertions.assertEquals(reference.getRowCount(), group.getRowCount(), 1e-9);
        Assertions.assertEquals(headOf(reference), headOf(group));
        Assertions.assertEquals(reference.getMcv().size(), group.getMcv().size());
        // The tuple of 5 is the rows of both: the group is complete or has a tail as the estimate of both says.
        double ownRows = PredicateStatisticsCalculator.statisticsCalculate(Utils.compoundAnd(equality, inequality), input)
                .getOutputRowCount();
        Assertions.assertEquals(ownRows <= 100_000 + 1e-6, headOf(group) == (long) group.getRowCount(),
                "own rows " + ownRows);
    }

    // A group of 1000 tuples of status 1, each of the given rows, with the rest of the million rows as its tail.
    private static Statistics tableWithAGroupOf1000Tuples(long rowsOfATuple) {
        List<MultiColumnCombinedStats.McvEntry> head = new ArrayList<>();
        for (int gate = 0; gate < 1000; gate++) {
            head.add(new MultiColumnCombinedStats.McvEntry(List.of("1", String.valueOf(gate), "0"), rowsOfATuple,
                    List.of(1000 * rowsOfATuple, rowsOfATuple, 1000 * rowsOfATuple)));
        }
        return Statistics.buildFrom(table(false)).addMultiColumnStatistics(Set.of(STATUS, GATE, TYPE),
                new MultiColumnCombinedStats(1000 + (rowsOfATuple * 1000 < ROWS ? 5000 : 0), ROWS,
                        List.of(STATUS, GATE, TYPE), head, List.of(0L, 0L, 0L))).build();
    }

    @Test
    public void testTuplesWithNoRowInTheOutputAreNotTuplesOfTheGroup() {
        ScalarOperator conjunction = Utils.compoundAnd(compare(BinaryType.EQ, STATUS, 1), interval());
        // 1000 tuples of 1000 rows are the million rows, a complete head; 1000 tuples of 500 rows leave half of them to a tail.
        for (long rowsOfATuple : new long[] {1000, 500}) {
            Statistics input = tableWithAGroupOf1000Tuples(rowsOfATuple);
            for (long rows : new long[] {100, 10}) {
                Statistics filtered = McvStatisticsPropagation.filterIndependent(conjunction, input,
                        input.withOutputRowCount(rows));
                MultiColumnCombinedStats group = filtered.getMultiColumnCombinedStats().get(Set.of(STATUS, GATE, TYPE));
                String label = rowsOfATuple + " rows a tuple, output " + rows;
                Assertions.assertNotNull(group, label);
                // No tuple has no row, and there are no more tuples than rows.
                Assertions.assertTrue(group.getMcv().stream().allMatch(entry -> entry.getCount() > 0), label);
                Assertions.assertTrue(group.getMcv().size() <= rows, label);
                boolean complete = rowsOfATuple == 1000;
                // A complete head stays complete, with the rows of the output, and a head that is not has a tail.
                Assertions.assertEquals(complete, headOf(group) == (long) group.getRowCount(), label);
                Assertions.assertEquals(rows, group.getRowCount(), 1e-9, label);
                // The distinct tuples are those of the head and, at most, one for each row of the tail, which are not more
                // than the rows.
                double tail = group.getRowCount() - headOf(group);
                Assertions.assertTrue(group.getNdv() >= group.getMcv().size(), label);
                Assertions.assertTrue(group.getNdv() <= group.getMcv().size() + Math.ceil(tail), label);
                Assertions.assertTrue(group.getNdv() <= Math.ceil(group.getRowCount()), label);
                if (complete) {
                    Assertions.assertEquals(group.getMcv().size(), group.getNdv(), label);
                }
                // The groups of an aggregate over the keys of the group are the tuples of the head, and not more than the
                // rows; the skewed join has an entry for each tuple, with rows.
                Statistics aggregate = McvAggregateStatistics.derive(List.of(GATE), filtered, filtered.withOutputRowCount(1000));
                MultiColumnCombinedStats groups = aggregate.getMultiColumnCombinedStats().get(Set.of(GATE));
                Assertions.assertNotNull(groups, label);
                if (complete) {
                    Assertions.assertEquals(group.getMcv().size(), groups.getRowCount(), 0, label);
                    Assertions.assertTrue(groups.getRowCount() <= rows, label);
                }
                SkewJoinStatistics.Distribution distribution = SkewJoinStatistics.find(filtered, List.of(GATE), 2000);
                Assertions.assertNotNull(distribution, label);
                Assertions.assertTrue(distribution.entries().stream().allMatch(entry -> entry.rows() > 0), label);
                Assertions.assertEquals(group.getMcv().size(), distribution.entries().size(), label);
            }
        }
    }

    @Test
    public void testComponentCountsOfTheTuplesOfACompleteHeadAreThoseOfTheSurvivors() {
        Statistics input = table(false);
        ScalarOperator status = compare(BinaryType.EQ, STATUS, 1);
        MultiColumnCombinedStats group = groupOf(McvStatisticsPropagation.filter(status, input,
                input.withOutputRowCount(600_000)));
        Assertions.assertNotNull(group);
        // The tuples of status 1 are (9, 0) of 200,000 rows, (14, 0) of 100,000 and (30, 1) of 300,000: the rows of each
        // value of a component are those of the tuples that are left, and not those of the table.
        Assertions.assertEquals(List.of(600_000L, 200_000L, 300_000L), group.getMcv().get(0).getComponentCounts());
        Assertions.assertEquals(List.of(600_000L, 100_000L, 300_000L), group.getMcv().get(1).getComponentCounts());
        Assertions.assertEquals(List.of(600_000L, 300_000L, 300_000L), group.getMcv().get(2).getComponentCounts());
    }
}
