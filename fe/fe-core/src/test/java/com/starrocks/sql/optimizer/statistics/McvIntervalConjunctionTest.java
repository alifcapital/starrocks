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
import com.starrocks.common.util.StringDateFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Supplier;

/**
 * An interval on a column of dates next to a condition on a column of an MCV group. The two sides of the interval
 * are estimated together, as one interval of the expression, and the condition on the group takes its share of the
 * rows of the interval. The table has 400M rows, and the head of the group (status, type) holds every row: 350M of
 * them are approved, so the share of the condition is 0.875.
 */
public class McvIntervalConjunctionTest {
    private static final ColumnRefOperator TEXT = new ColumnRefOperator(1, VarcharType.VARCHAR, "created_at", true);
    private static final ColumnRefOperator STATUS = new ColumnRefOperator(2, VarcharType.VARCHAR, "status", true);
    private static final ColumnRefOperator TYPE = new ColumnRefOperator(3, IntegerType.INT, "type", true);
    private static final StringDateFormat ISO = StringDateFormat.parse("%Y-%m-%dT%H:%i:%s.%fZ");
    private static final double ROWS = 400_000_000;
    private static final double APPROVED = 0.875;

    private TimeZone savedZone;

    @BeforeEach
    public void setUp() {
        savedZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        new ConnectContext().setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        TimeZone.setDefault(savedZone);
        ConnectContext.remove();
    }

    private static MultiColumnCombinedStats.McvEntry entry(String status, String type, long count, long statusRows,
                                                           long typeRows) {
        return new MultiColumnCombinedStats.McvEntry(List.of(status, type), count, List.of(statusRows, typeRows));
    }

    // approved 350M rows, failed 30M and pending 20M; type 0 in 340M rows and type 1 in 60M.
    private static Statistics table() {
        List<MultiColumnCombinedStats.McvEntry> head = List.of(
                entry("approved", "0", 300_000_000, 350_000_000, 340_000_000),
                entry("approved", "1", 50_000_000, 350_000_000, 60_000_000),
                entry("failed", "0", 20_000_000, 30_000_000, 340_000_000),
                entry("failed", "1", 10_000_000, 30_000_000, 60_000_000),
                entry("pending", "0", 20_000_000, 20_000_000, 340_000_000));
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(TEXT, ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(27)
                        .setDistinctValuesCount(390_000_000)
                        .setStringBounds(new StringBounds("2018-08-15T13:16:23.509905Z", "2026-10-07T08:58:22.946788Z",
                                200, false)).build())
                .addColumnStatistic(STATUS, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8,
                        3))
                .addColumnStatistic(TYPE, new ColumnStatistic(0, 1, 0, 4, 2))
                .addMultiColumnStatistics(Set.of(STATUS, TYPE), new MultiColumnCombinedStats(5, (long) ROWS,
                        List.of(STATUS, TYPE), head, List.of(0L, 0L))).build();
    }

    private static ScalarOperator approved() {
        return new BinaryPredicateOperator(BinaryType.EQ, STATUS, ConstantOperator.createVarchar("approved"));
    }

    private static <T> T inScope(Supplier<T> action) {
        return PredicateStatisticsCalculator.withStringDateFormats(column -> TEXT.equals(column) ? ISO : null, action);
    }

    private static double rows(ScalarOperator predicate) {
        return inScope(() -> PredicateStatisticsCalculator.statisticsCalculate(predicate, table()).getOutputRowCount());
    }

    private static ScalarOperator cast() {
        return new CastOperator(DateType.DATETIME, TEXT);
    }

    private static ScalarOperator hoursAdd() {
        Type[] types = {DateType.DATETIME, IntegerType.INT};
        Function function = ExprUtils.getBuiltinFunction("hours_add", types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator("hours_add", DateType.DATETIME, List.of(cast(), ConstantOperator.createInt(5)), function);
    }

    private static ScalarOperator between(ScalarOperator left, ConstantOperator from, ConstantOperator to) {
        return Utils.compoundAnd(new BinaryPredicateOperator(BinaryType.GE, left, from),
                new BinaryPredicateOperator(BinaryType.LT, left, to));
    }

    private static ConstantOperator time(int day, int hour) {
        return ConstantOperator.createDatetime(LocalDateTime.of(2024, 3, day, hour, 0));
    }

    private static ConstantOperator text(String value) {
        return ConstantOperator.createVarchar(value);
    }

    // The same day of the history in each form of the interval.
    private static List<ScalarOperator> days() {
        return List.of(between(cast(), time(15, 0), time(16, 0)),
                between(TEXT, text("2024-03-15T00:00:00.000000Z"), text("2024-03-16T00:00:00.000000Z")),
                between(hoursAdd(), time(15, 5), time(16, 5)));
    }

    @Test
    public void testConditionOnTheGroupTakesItsShareOfTheInterval() {
        for (ScalarOperator day : days()) {
            double alone = rows(day);
            // A day of the 3000 days of the range holds about 133K of the 400M rows, not a share of the whole range
            Assertions.assertTrue(alone > 10_000 && alone < 1_000_000, day + " " + alone);
            Assertions.assertEquals(APPROVED * alone, rows(Utils.compoundAnd(approved(), day)), alone * 1e-6,
                    day.toString());
            // The order of the conjuncts does not change the estimate.
            Assertions.assertEquals(APPROVED * alone, rows(Utils.compoundAnd(day, approved())), alone * 1e-6,
                    day.toString());
        }
    }

    @Test
    public void testSideOfAnIntervalOnItsOwnIsTheShareOfTheRange() {
        // One side of the interval does not make an interval, so the rest next to the group is a single condition and
        // takes the share of the range as before.
        ScalarOperator since = new BinaryPredicateOperator(BinaryType.GE, cast(), time(15, 0));
        Assertions.assertEquals(APPROVED * rows(since), rows(Utils.compoundAnd(approved(), since)), rows(since) * 1e-6);
    }

    @Test
    public void testIntervalOfTwoConditionsOnTheGroupAndTheDay() {
        ScalarOperator type0 = new BinaryPredicateOperator(BinaryType.EQ, TYPE, ConstantOperator.createInt(0));
        for (ScalarOperator day : days()) {
            double alone = rows(day);
            // approved and type 0: 300M of the 400M rows
            Assertions.assertEquals(0.75 * alone, rows(Utils.compoundAnd(List.of(approved(), type0, day))),
                    alone * 1e-6, day.toString());
        }
    }
}
