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
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The planning time of the bounds of a period on hours_add(created, 5) next to the group of a status, a gate and a type
 * and the MCV list of created, which has a head of 1000 timestamps. The estimate of a conjunction of n bounds filters the
 * head at each of its n AND nodes, and the conjunctions of a query are two bounds or three; the value of the expression
 * on a tuple of the head is evaluated once for the estimate, so the time does not grow with the product of the bounds,
 * the nodes and the tuples.
 */
public class McvPlanningTimeTest {
    private static final double ROWS = 1_000_000;
    private static final ColumnRefOperator STATUS = new ColumnRefOperator(1, IntegerType.INT, "status", true);
    private static final ColumnRefOperator GATE = new ColumnRefOperator(2, IntegerType.INT, "gate", true);
    private static final ColumnRefOperator TYPE = new ColumnRefOperator(3, IntegerType.INT, "type", true);
    private static final ColumnRefOperator CREATED = new ColumnRefOperator(4, DateType.DATETIME, "created", true);

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.createDefaultCtx();
    }

    private static double seconds(int month, int day) {
        return Utils.getLongFromDateTime(LocalDateTime.of(2026, month, day, 0, 0));
    }

    private static MultiColumnCombinedStats.McvEntry entry(int status, int gate, int type, long count, long statusCount,
                                                           long gateCount, long typeCount) {
        return new MultiColumnCombinedStats.McvEntry(List.of(String.valueOf(status), String.valueOf(gate),
                String.valueOf(type)), count, List.of(statusCount, gateCount, typeCount));
    }

    private static Statistics table(int head) {
        List<Bucket> buckets = new ArrayList<>();
        long total = 0;
        for (int month = 1; month <= 8; month++) {
            total += month == 8 ? 700_000 : 300_000 / 7;
            buckets.add(new Bucket(seconds(month, 1), seconds(month + 1, 1) - 1, month == 8 ? 1_000_000 : total, 10L, 100L));
        }
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        for (int i = 0; i < head; i++) {
            entries.add(new MultiColumnCombinedStats.McvEntry(List.of(LocalDateTime.of(2026, 1, 1, 0, 0).plusMinutes(7L * i)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), 10, List.of(10L)));
        }
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(STATUS, new ColumnStatistic(1, 2, 0, 4, 2))
                .addColumnStatistic(GATE, new ColumnStatistic(9, 40, 0, 4, 4))
                .addColumnStatistic(TYPE, new ColumnStatistic(0, 1, 0, 4, 2))
                .addColumnStatistic(CREATED, ColumnStatistic.builder().setMinValue(seconds(1, 1))
                        .setMaxValue(seconds(9, 1) - 1).setNullsFraction(0).setAverageRowSize(8)
                        .setDistinctValuesCount(1000).setHistogram(new Histogram(buckets, Map.of())).build())
                .addMultiColumnStatistics(Set.of(STATUS, GATE, TYPE), new MultiColumnCombinedStats(5, (long) ROWS,
                        List.of(STATUS, GATE, TYPE), List.of(entry(1, 9, 0, 200_000, 600_000, 450_000, 450_000),
                        entry(1, 14, 0, 100_000, 600_000, 100_000, 450_000),
                        entry(1, 30, 1, 300_000, 600_000, 300_000, 550_000),
                        entry(2, 9, 1, 250_000, 400_000, 450_000, 550_000),
                        entry(2, 40, 0, 150_000, 400_000, 150_000, 450_000)), List.of(0L, 0L, 0L)))
                .addMultiColumnStatistics(Set.of(CREATED), new MultiColumnCombinedStats(1000 + head, (long) ROWS,
                        List.of(CREATED), entries, List.of(0L)))
                .build();
    }

    private static ScalarOperator shifted() {
        Type[] types = {DateType.DATETIME, IntegerType.INT};
        Function function = ExprUtils.getBuiltinFunction("hours_add", types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator("hours_add", DateType.DATETIME, List.of(CREATED, ConstantOperator.createInt(5)), function);
    }

    private static ScalarOperator bounds(int count) {
        ScalarOperator shifted = shifted();
        List<ScalarOperator> bounds = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            bounds.add(new BinaryPredicateOperator(BinaryType.GE, shifted,
                    ConstantOperator.createDatetime(LocalDateTime.of(2026, 1, 1, 0, 0).plusHours(i))));
        }
        return Utils.compoundAnd(bounds);
    }

    private static ScalarOperator predicate(int count, boolean withGroup) {
        ScalarOperator status = new BinaryPredicateOperator(BinaryType.EQ, STATUS, ConstantOperator.createInt(1));
        ScalarOperator or = Utils.compoundOr(new BinaryPredicateOperator(BinaryType.EQ, TYPE, ConstantOperator.createInt(0)),
                new BinaryPredicateOperator(BinaryType.EQ, GATE, ConstantOperator.createInt(9)));
        return withGroup ? Utils.compoundAnd(List.of(status, or, bounds(count))) : bounds(count);
    }

    private static double rows(int bounds, boolean withGroup) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate(bounds, withGroup), table(1000))
                .getOutputRowCount();
    }

    @Test
    public void testEstimatesAreTheOnesOfTheEvaluationOfEachCondition() {
        // The rows that the estimate gave before the value of the expression was kept, which the branch that has the
        // group of an OR next to the equalities gives with the group, and the branch that has not gives without it.
        int[] bounds = {2, 3, 4, 5, 8, 16, 64};
        double[] withoutGroup = {1000000, 1000000, 1000000, 1000000, 999820, 999140, 995020};
        double[] withGroup = {300000, 300000, 300000, 300000, 299946, 299742, 298506};
        for (int i = 0; i < bounds.length; i++) {
            Assertions.assertEquals(withoutGroup[i], rows(bounds[i], false), 1e-6, bounds[i] + " bounds");
            Assertions.assertEquals(withGroup[i], rows(bounds[i], true), 1e-6, bounds[i] + " bounds and the group");
        }
    }

    // The fastest of a few estimates after a warm-up: the slower ones measure the pauses of the host.
    private static long fastestMillis(int bounds, boolean withGroup) {
        ScalarOperator predicate = predicate(bounds, withGroup);
        Statistics input = table(1000);
        PredicateStatisticsCalculator.statisticsCalculate(predicate, input);
        long fastest = Long.MAX_VALUE;
        for (int run = 0; run < 3; run++) {
            long start = System.nanoTime();
            PredicateStatisticsCalculator.statisticsCalculate(predicate, input);
            fastest = Math.min(fastest, System.nanoTime() - start);
        }
        return Math.max(1, fastest / 1_000_000);
    }

    // The time of one estimate depends on the host and on what else runs there, so we compare the times of 16 and of 64
    // bounds on the same host. Four times the bounds take about four times the time when the time grows with the
    // bounds, and sixteen times when it grows with the product of the bounds and the AND nodes.
    @Test
    public void testPlanningTimeGrowsWithTheBoundsAndNotWithTheirProduct() {
        StringBuilder measured = new StringBuilder();
        for (boolean withGroup : new boolean[] {false, true}) {
            long sixteen = fastestMillis(16, withGroup);
            long sixtyFour = fastestMillis(64, withGroup);
            measured.append(String.format(" group %s: 16 bounds %d ms, 64 bounds %d ms;", withGroup, sixteen, sixtyFour));
            Assertions.assertTrue(sixtyFour < 8 * sixteen,
                    "group " + withGroup + ": 16 bounds " + sixteen + " ms, 64 bounds " + sixtyFour + " ms");
        }
        System.out.println("MEASURED planning of the bounds of a period on a head of 1000:" + measured);
    }
}
