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
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Conditions on several columns of one six-column MCV group, such as an OR of the source and the destination type
 * of a transfer next to an equality on the status. The head holds 99.9% of the rows and one status holds 98.6% of
 * them, so the head knows the rows of each condition up to the tail. The query reads three columns of the group:
 * the others are placeholders, as for a scan that does not read them.
 */
public class McvGroupDisjunctionTest {
    private static final ColumnRefOperator STATUS = new ColumnRefOperator(1, VarcharType.VARCHAR, "status", true);
    private static final ColumnRefOperator OPERATION = new ColumnRefOperator(2, IntegerType.INT, "operation_type", true);
    private static final ColumnRefOperator DEST_GATE =
            new ColumnRefOperator(3, IntegerType.INT, "dest_acc_gate", false);
    private static final ColumnRefOperator SOURCE_GATE =
            new ColumnRefOperator(4, IntegerType.INT, "source_acc_gate", false);
    private static final ColumnRefOperator DEST_TYPE =
            new ColumnRefOperator(5, IntegerType.INT, "dest_acc_type", false);
    private static final ColumnRefOperator SOURCE_TYPE =
            new ColumnRefOperator(6, IntegerType.INT, "source_acc_type", false);
    private static final ColumnRefOperator USER = new ColumnRefOperator(7, IntegerType.BIGINT, "user_id", false);
    private static final List<ColumnRefOperator> GROUP =
            List.of(STATUS, OPERATION, DEST_GATE, SOURCE_GATE, DEST_TYPE, SOURCE_TYPE);
    private static final List<ColumnRefOperator> READ = Arrays.asList(STATUS, null, null, null, DEST_TYPE, SOURCE_TYPE);

    // status, operation_type, dest_acc_gate, source_acc_gate, dest_acc_type, source_acc_type: rows
    private static final Object[][] HEAD = {
            {"approved", 1, 0, 0, 0, 0, 257000},
            {"approved", 2, 9, 0, 7, 0, 163000},
            {"approved", 0, 0, 9, 0, 6, 89000},
            {"approved", 1, 0, 14, 0, 2, 75000},
            {"approved", 2, 9, 14, 7, 2, 51000},
            {"approved", 1, 14, 0, 2, 0, 42000},
            {"approved", 0, 0, 0, 1, 9, 37500},
            {"approved", 4, 3, 3, 3, 3, 30000},
            {"approved", 3, 12, 0, 8, 0, 29000},
            {"approved", 5, 5, 5, 5, 5, 25000},
            {"approved", 2, 9, 1, 7, 2, 22000},
            {"approved", 0, 1, 2, 6, 6, 22000},
            {"approved", 2, 7, 0, 4, 0, 20000},
            {"approved", 1, 0, 25, 0, 2, 18500},
            {"approved", 2, 9, 25, 7, 2, 13600},
            {"approved", 2, 9, 6, 7, 4, 12000},
            {"failed", 1, 0, 0, 0, 0, 6000},
            {"failed", 2, 9, 0, 7, 0, 3500},
            {"canceled", 0, 0, 9, 0, 6, 1500},
            {"canceled", 1, 0, 14, 0, 2, 900},
            {"pending", 1, 0, 0, 0, 0, 500},
    };
    // The rows outside the head, all of them approved.
    private static final long TAIL = 1000;
    private static final long ROWS = headRows(values -> true) + TAIL;

    @BeforeAll
    public static void beforeAll() {
        UtFrameUtils.createDefaultCtx();
    }

    private static long headRows(Predicate<List<String>> predicate) {
        long rows = 0;
        for (Object[] tuple : HEAD) {
            if (predicate.test(values(tuple))) {
                rows += ((Number) tuple[6]).longValue();
            }
        }
        return rows;
    }

    private static List<String> values(Object[] tuple) {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < GROUP.size(); i++) {
            values.add(tuple[i].toString());
        }
        return values;
    }

    /**
     * The statistics of a scan that reads the given columns of the group, or none: the group, keyed by the read
     * columns, and the marginal of each read column that the scan takes from the group, as
     * StatisticsCalcUtils.attachExternalMcvStats builds them.
     */
    private static Statistics scanStatistics(List<ColumnRefOperator> refs) {
        List<Map<String, Long>> marginals = new ArrayList<>();
        for (int i = 0; i < GROUP.size(); i++) {
            marginals.add(new HashMap<>());
        }
        for (Object[] tuple : HEAD) {
            List<String> values = values(tuple);
            for (int i = 0; i < GROUP.size(); i++) {
                marginals.get(i).merge(values.get(i), ((Number) tuple[6]).longValue(), Long::sum);
            }
        }
        marginals.get(0).merge("approved", TAIL, Long::sum);
        List<MultiColumnCombinedStats.McvEntry> mcv = new ArrayList<>();
        for (Object[] tuple : HEAD) {
            List<String> values = values(tuple);
            List<Long> components = new ArrayList<>();
            for (int i = 0; i < GROUP.size(); i++) {
                components.add(marginals.get(i).get(values.get(i)));
            }
            mcv.add(new MultiColumnCombinedStats.McvEntry(values, ((Number) tuple[6]).longValue(), components));
        }
        List<Long> nullCounts = List.of(0L, 0L, 0L, 0L, 0L, 0L);
        long ndv = HEAD.length + 40;

        Statistics.Builder builder = Statistics.builder().setOutputRowCount(ROWS);
        Map<ColumnRefOperator, ColumnStatistic> basic = new HashMap<>();
        basic.put(STATUS, ColumnStatistic.builder().setMinValue(Double.NEGATIVE_INFINITY)
                .setMaxValue(Double.POSITIVE_INFINITY).setDistinctValuesCount(5).setNullsFraction(0)
                .setAverageRowSize(8).build());
        basic.put(OPERATION, ColumnStatistic.builder().setMinValue(0).setMaxValue(9).setDistinctValuesCount(8)
                .setNullsFraction(0).setAverageRowSize(4).build());
        for (ColumnRefOperator column : List.of(DEST_GATE, SOURCE_GATE)) {
            basic.put(column, ColumnStatistic.builder().setMinValue(0).setMaxValue(30).setDistinctValuesCount(20)
                    .setNullsFraction(0).setAverageRowSize(4).build());
        }
        for (ColumnRefOperator column : List.of(DEST_TYPE, SOURCE_TYPE)) {
            basic.put(column, ColumnStatistic.builder().setMinValue(0).setMaxValue(10).setDistinctValuesCount(11)
                    .setNullsFraction(0).setAverageRowSize(4).build());
        }
        Set<ColumnRefOperator> read = new HashSet<>(GROUP);
        if (refs != null) {
            read = new HashSet<>(refs);
            read.remove(null);
        }
        for (ColumnRefOperator column : GROUP) {
            if (read.contains(column)) {
                builder.addColumnStatistic(column, basic.get(column));
            }
        }
        builder.addColumnStatistic(USER, ColumnStatistic.builder().setMinValue(-1).setMaxValue(1_000_000)
                .setDistinctValuesCount(100_000).setNullsFraction(0).setAverageRowSize(8).build());
        if (refs != null) {
            ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(
                    GROUP.stream().map(ColumnRefOperator::getName).toList(), ROWS, ndv, mcv, List.of(), nullCounts);
            StatisticsCalcUtils.addElementStatistics(builder, List.of(group), List.of(refs), Set.of());
            builder.addMultiColumnStatistics(read, new MultiColumnCombinedStats(ndv, ROWS, refs, mcv, nullCounts));
        }
        return builder.build();
    }

    private static ScalarOperator eq(ColumnRefOperator column, int value) {
        return new BinaryPredicateOperator(BinaryType.EQ, column, ConstantOperator.createInt(value));
    }

    private static ScalarOperator approved() {
        return new BinaryPredicateOperator(BinaryType.EQ, STATUS, ConstantOperator.createVarchar("approved"));
    }

    private static ScalarOperator anyTypeZero() {
        return Utils.compoundOr(eq(SOURCE_TYPE, 0), eq(DEST_TYPE, 0));
    }

    private static ScalarOperator userAbove(long value) {
        return new BinaryPredicateOperator(BinaryType.GT, USER, ConstantOperator.createBigint(value));
    }

    private static double rows(ScalarOperator predicate, Statistics statistics) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics).getOutputRowCount();
    }

    private static boolean typeZero(List<String> values) {
        return values.get(4).equals("0") || values.get(5).equals("0");
    }

    private static boolean isApproved(List<String> values) {
        return values.get(0).equals("approved");
    }

    /**
     * The head rows of the condition, up to the tail. Each condition after the first one on the group is
     * estimated with the head that the conditions before it keep, and each of them can add the tail once.
     */
    private static void assertHeadRows(long head, double estimated, String form) {
        assertHeadRows(head, estimated, form, 2);
    }

    // The equalities of one column in an OR have no rows in common, so each of them adds the tail of its own.
    private static void assertHeadRows(long head, double estimated, String form, int tails) {
        Assertions.assertTrue(Math.abs(estimated - head) <= tails * TAIL + 1e-6,
                form + ": estimated " + estimated + ", head rows " + head + ", tail " + TAIL);
    }

    @Test
    public void testEqualityAndDisjunctionOnOneGroupUseTheHead() {
        long head = headRows(values -> isApproved(values) && typeZero(values));
        // NOT (source_acc_type != 0 AND dest_acc_type != 0) is the same condition; the columns hold no NULL.
        ScalarOperator negated = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                Utils.compoundAnd(new BinaryPredicateOperator(BinaryType.NE, SOURCE_TYPE, ConstantOperator.createInt(0)),
                        new BinaryPredicateOperator(BinaryType.NE, DEST_TYPE, ConstantOperator.createInt(0))));
        for (List<ColumnRefOperator> refs : List.of(READ, GROUP)) {
            Statistics statistics = scanStatistics(refs);
            String read = refs == READ ? "three columns read: " : "all columns read: ";
            assertHeadRows(head, rows(Utils.compoundAnd(approved(), anyTypeZero()), statistics),
                    read + "approved AND OR");
            assertHeadRows(head, rows(Utils.compoundAnd(anyTypeZero(), approved()), statistics),
                    read + "OR AND approved");
            assertHeadRows(head, rows(Utils.compoundAnd(approved(), negated), statistics), read + "approved AND NOT");
        }
    }

    @Test
    public void testConditionAfterAFilterOnTheGroupUsesTheConditionalHead() {
        Statistics statistics = scanStatistics(READ);
        Statistics filtered = PredicateStatisticsCalculator.statisticsCalculate(approved(), statistics);
        long approvedHead = headRows(McvGroupDisjunctionTest::isApproved);
        assertHeadRows(approvedHead, filtered.getOutputRowCount(), "approved");
        // The filter leaves the column without its marginal; the conditional head estimates the next condition.
        Assertions.assertTrue(MultiColumnMcvEstimator.estimate(List.of(eq(SOURCE_TYPE, 0)), filtered).isPresent());
        long head = headRows(values -> isApproved(values) && values.get(5).equals("0"));
        assertHeadRows(head, rows(eq(SOURCE_TYPE, 0), filtered), "source_acc_type = 0 after approved");
        assertHeadRows(head, rows(Utils.compoundAnd(approved(), eq(SOURCE_TYPE, 0)), statistics),
                "approved AND source_acc_type = 0");
    }

    @Test
    public void testDisjunctionOnOneGroupAloneAndBesideAnotherColumn() {
        Statistics statistics = scanStatistics(READ);
        long head = headRows(McvGroupDisjunctionTest::typeZero);
        double alone = rows(anyTypeZero(), statistics);
        assertHeadRows(head, alone, "OR");
        // user_id > 0 is independent of the group.
        double userShare = rows(userAbove(0), statistics) / ROWS;
        Assertions.assertEquals(alone * userShare, rows(Utils.compoundAnd(userAbove(0), anyTypeZero()), statistics),
                alone * 1e-6);
        long approvedHead = headRows(values -> isApproved(values) && typeZero(values));
        double all = rows(Utils.compoundAnd(List.of(approved(), anyTypeZero(), userAbove(0))), statistics);
        assertHeadRows(approvedHead, all / userShare, "approved AND OR AND user_id > 0");
    }

    @Test
    public void testAddingADisjunctDoesNotLowerTheEstimate() {
        Statistics statistics = scanStatistics(READ);
        double one = rows(Utils.compoundAnd(approved(), eq(SOURCE_TYPE, 0)), statistics);
        // The second disjunct is on a column outside the group: the OR is estimated after the equality.
        ScalarOperator or = Utils.compoundOr(eq(SOURCE_TYPE, 0),
                new BinaryPredicateOperator(BinaryType.LT, USER, ConstantOperator.createBigint(10)));
        double two = rows(Utils.compoundAnd(approved(), or), statistics);
        Assertions.assertTrue(two >= one - 1e-6, "with the second disjunct " + two + ", without it " + one);
        Assertions.assertTrue(rows(Utils.compoundAnd(approved(), anyTypeZero()), statistics) >= one - 1e-6);
    }

    @Test
    public void testDisjunctionOfOneColumnAgreesWithIn() {
        Statistics statistics = scanStatistics(READ);
        ScalarOperator or = Utils.compoundOr(eq(SOURCE_TYPE, 0), eq(SOURCE_TYPE, 2));
        ScalarOperator in = new InPredicateOperator(false, SOURCE_TYPE, ConstantOperator.createInt(0),
                ConstantOperator.createInt(2));
        long head = headRows(values -> isApproved(values) && (values.get(5).equals("0") || values.get(5).equals("2")));
        assertHeadRows(head, rows(Utils.compoundAnd(approved(), or), statistics), "approved AND (s = 0 OR s = 2)", 3);
        assertHeadRows(head, rows(Utils.compoundAnd(approved(), in), statistics), "approved AND s IN (0, 2)");
    }

    @Test
    public void testWithoutTheGroupTheDisjunctionFollowsTheEquality() {
        Statistics statistics = scanStatistics(null);
        Assertions.assertTrue(statistics.getMultiColumnCombinedStats().isEmpty());
        Statistics filtered = PredicateStatisticsCalculator.statisticsCalculate(approved(), statistics);
        double expected = PredicateStatisticsCalculator.statisticsCalculate(anyTypeZero(), filtered).getOutputRowCount();
        Assertions.assertEquals(expected, rows(Utils.compoundAnd(approved(), anyTypeZero()), statistics), 1e-6);
    }
}
