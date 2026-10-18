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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.MonotonicFilterDerivation;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// A comparison of a function of a column with a constant is estimated by the bound on the column that follows from
// it, and a bound that a scan holds next to the comparison is never estimated.
public class DerivedBoundEstimateTest {
    private static final double ROWS = 1928199653;
    // A date stored as the number of days since 1970-01-01, as in many Iceberg tables.
    private static final ColumnRefOperator DAYS = new ColumnRefOperator(1, IntegerType.INT, "date", true);
    private static final ColumnRefOperator OTHER = new ColumnRefOperator(2, IntegerType.INT, "other", true);
    private static final LocalDateTime EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0);

    @BeforeEach
    public void setUp() {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("+00:00");
        ctx.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static Statistics table() {
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(DAYS, new ColumnStatistic(17197, 20727, 0, 4, 805))
                .addColumnStatistic(OTHER, new ColumnStatistic(0, 9, 0, 4, 10))
                .build();
    }

    private static CallOperator shifted() {
        return new CallOperator("days_add", DateType.DATETIME,
                ImmutableList.of(ConstantOperator.createDatetime(EPOCH), DAYS));
    }

    // days_add('1970-01-01 00:00:00', date) >= '2025-01-01 00:00:00'
    private static ScalarOperator fromYear2025() {
        return new BinaryPredicateOperator(BinaryType.GE, shifted(),
                ConstantOperator.createDatetime(LocalDateTime.of(2025, 1, 1, 0, 0)));
    }

    private static double rows(ScalarOperator predicate, Statistics statistics) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics).getOutputRowCount();
    }

    @Test
    public void testComparisonOnAFunctionIsEstimatedByTheColumn() {
        // 2025-01-01 is day 20089: date >= 20089 keeps (20727 - 20089) / (20727 - 17197) of the rows. A compensation
        // predicate of a materialized view rewrite holds the comparison without any bound and gets this estimate.
        double expected = ROWS * (20727 - 20089) / (20727 - 17197);
        Assertions.assertEquals(expected, rows(fromYear2025(), table()), expected * 0.01);
        ScalarOperator bound = new BinaryPredicateOperator(BinaryType.GE, DAYS, ConstantOperator.createInt(20089));
        Assertions.assertEquals(rows(bound, table()), rows(fromYear2025(), table()), expected * 0.01);
    }

    @Test
    public void testBoundHeldByTheScanIsNotEstimated() {
        ScalarOperator original = fromYear2025();
        ScalarOperator withBound = MonotonicFilterDerivation.addScanBounds(original, unused -> null);
        List<ScalarOperator> bounds = Utils.extractConjuncts(withBound).stream()
                .filter(conjunct -> conjunct != original).toList();
        Assertions.assertEquals(1, bounds.size());
        Assertions.assertTrue(bounds.get(0).isNotEvalEstimate());
        Assertions.assertEquals(rows(original, table()), rows(withBound, table()), 1);
    }

    @Test
    public void testEqualityBoundIsNotCountedTwice() {
        // days_add('1970-01-01', date) = '2024-01-11' gives the bound date = 19733. Two equalities on columns go
        // through the estimate with multi-column statistics, which must not take the bound for a third one.
        ScalarOperator original = new BinaryPredicateOperator(BinaryType.EQ, shifted(),
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 11, 0, 0)));
        ScalarOperator other = new BinaryPredicateOperator(BinaryType.EQ, OTHER, ConstantOperator.createInt(5));
        ScalarOperator predicate = Utils.compoundAnd(original, other);
        ScalarOperator withBound = MonotonicFilterDerivation.addScanBounds(predicate, unused -> null);
        Assertions.assertEquals(3, Utils.extractConjuncts(withBound).size());
        Assertions.assertEquals(rows(predicate, table()), rows(withBound, table()), 1);
    }

    @Test
    public void testOptionOffDoesNotEstimateByTheColumn() {
        // The call has no function, so the FE cannot fold it and hours_add gets no range of its own: only the
        // bound on the column gives the comparison a range.
        ColumnRefOperator ts = new ColumnRefOperator(3, DateType.DATETIME, "ts", true);
        Statistics statistics = Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(ts, new ColumnStatistic(
                        Utils.getLongFromDateTime(LocalDateTime.of(2024, 1, 1, 0, 0)),
                        Utils.getLongFromDateTime(LocalDateTime.of(2025, 1, 1, 0, 0)), 0, 8, 10000))
                .build();
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE,
                new CallOperator("hours_add", DateType.DATETIME, ImmutableList.of(ts, ConstantOperator.createInt(7))),
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 10, 1, 7, 0)));
        double estimated = rows(predicate, statistics);
        ConnectContext.get().getSessionVariable().setEnableMonotonicPredicateEstimate(false);
        double notEstimated = rows(predicate, statistics);
        Assertions.assertTrue(Math.abs(estimated - notEstimated) > ROWS * 0.01, estimated + " " + notEstimated);
        // The scan holds the bound without estimating it in either case
        ScalarOperator original = fromYear2025();
        ScalarOperator withBound = MonotonicFilterDerivation.addScanBounds(original, unused -> null);
        Assertions.assertTrue(Utils.extractConjuncts(withBound).stream()
                .filter(conjunct -> conjunct != original).allMatch(ScalarOperator::isNotEvalEstimate));
    }

    @Test
    public void testFilterAboveAJoinIsEstimatedByTheColumn() {
        // Above a join the column keeps its statistics, narrowed by the join
        Statistics joined = Statistics.builder().setOutputRowCount(5e9)
                .addColumnStatistic(DAYS, new ColumnStatistic(20000, 20727, 0, 4, 700)).build();
        double expected = 5e9 * (20727 - 20089) / (20727 - 20000);
        Assertions.assertEquals(expected, rows(fromYear2025(), joined), expected * 0.01);
    }

    @Test
    public void testShiftIsEstimatedByTheHistogramOfTheColumn() {
        // A shift maps each day to one datetime, so the shifted values keep the distribution of the column. Here 900
        // of the 1000 rows are on day 19733, the other 100 on the days before it, and the comparison selects day 19733.
        Statistics skewed = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(DAYS, ColumnStatistic.builder().setMinValue(19724).setMaxValue(19733)
                        .setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(10)
                        .setHistogram(new Histogram(List.of(new Bucket(19724, 19732, 100L, 0L)),
                                Map.of("19733", 900L))).build())
                .build();
        ScalarOperator original = new BinaryPredicateOperator(BinaryType.EQ, shifted(),
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 11, 0, 0)));
        ScalarOperator bound = new BinaryPredicateOperator(BinaryType.EQ, DAYS, ConstantOperator.createInt(19733));
        Assertions.assertEquals(900, rows(bound, skewed), 10);
        Assertions.assertEquals(rows(bound, skewed), rows(original, skewed), 1);
    }

    @Test
    public void testRangeOnRecentDaysIsEstimatedByTheBucketsOfTheMcvStatistics() {
        Statistics scan = recentDays();
        // days_add('1970-01-01', date) >= '2025-01-01' is date >= 20089: the part of the second bucket from day 20089
        // and the 600 rows of the list
        double expected = 300.0 * (20400 - 20089) / (20400 - 20001) + 600;
        Assertions.assertEquals(expected, rows(fromYear2025(), scan), expected * 0.02);
        // A range of two comparisons goes to the estimate with the MCV list as one conjunction
        Assertions.assertEquals(expected, rows(Utils.compoundAnd(fromYear2025(), before2027()), scan), expected * 0.02);
    }

    @Test
    public void testRangeOfTwoComparisonsKeepsBothEndsInTheColumn() {
        // The estimate with the MCV list takes the row count of the range at once. The statistics of the column must
        // still hold both ends, so that a GROUP BY or a join above counts the days of the range only: the part of the
        // second bucket from day 20089, 233 of its 300 days, and the ten days of the list.
        Statistics filtered = PredicateStatisticsCalculator.statisticsCalculate(
                Utils.compoundAnd(fromYear2025(), before2027()), recentDays());
        ColumnStatistic days = filtered.getColumnStatistic(DAYS);
        Assertions.assertEquals(20089, days.getMinValue(), 0.5, days.toString());
        Assertions.assertEquals(243, days.getDistinctValuesCount(), 5, days.toString());
        // The ten days of the list lie beyond the last bucket, up to day 20509
        Assertions.assertEquals(20509, days.getMaxValue(), 0.5, days.toString());
    }

    @Test
    public void testRangeBeyondTheBucketsKeepsTheRangeOfTheColumn() {
        // The histogram is older than the column: buckets to day 20400 and two most common days to 20500, while the
        // column reaches day 20507. date >= 20450 leaves no bucket, and the column keeps its days to 20507, not only
        // the most common ones.
        Histogram histogram = new Histogram(List.of(new Bucket(20001, 20400, 400L, 1L)),
                Map.of("20450", 60L, "20500", 60L));
        Statistics statistics = Statistics.builder().setOutputRowCount(520)
                .addColumnStatistic(DAYS, ColumnStatistic.builder().setMinValue(20001).setMaxValue(20507)
                        .setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(402)
                        .setHistogram(histogram).build())
                .build();
        ScalarOperator recent = new BinaryPredicateOperator(BinaryType.GE, DAYS, ConstantOperator.createInt(20450));
        ColumnStatistic days = PredicateStatisticsCalculator.statisticsCalculate(recent, statistics)
                .getColumnStatistic(DAYS);
        Assertions.assertEquals(20450, days.getMinValue(), 0.5, days.toString());
        Assertions.assertEquals(20507, days.getMaxValue(), 0.5, days.toString());
    }

    // Most rows are recent: 100 rows from 2017 to 2024-10 and 300 rows to 2025-11 in the buckets, 600 rows on the ten
    // most common days of 2026. The statistics are those of a scan that attaches the MCV statistics of an external
    // table, so a comparison goes through the estimate with the MCV list, which takes the rows outside the list from
    // the buckets.
    private static Statistics recentDays() {
        List<MultiColumnCombinedStats.McvEntry> mcv = new ArrayList<>();
        for (int day = 20500; day < 20510; day++) {
            mcv.add(new MultiColumnCombinedStats.McvEntry(List.of(Integer.toString(day)), 60, List.of(60L)));
        }
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("date"), 1000, 410, mcv,
                List.of(List.of("17197", "20000", "100", "1", "100"), List.of("20001", "20400", "400", "1", "300")),
                List.of(0L));
        Statistics basic = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(DAYS, new ColumnStatistic(17197, 20509, 0, 4, 410)).build();
        return StatisticsCalcUtils.attachExternalMcvStats(basic, new ExternalMcvStatistics(List.of(group)),
                Map.of(DAYS, new Column("date", IntegerType.INT)));
    }

    // days_add('1970-01-01 00:00:00', date) < '2027-01-01 00:00:00'
    private static ScalarOperator before2027() {
        return new BinaryPredicateOperator(BinaryType.LT, shifted(),
                ConstantOperator.createDatetime(LocalDateTime.of(2027, 1, 1, 0, 0)));
    }

    @Test
    public void testEqualityOnAMonthShiftOfADatetimeTakesThePointNotTheDay() {
        // A year of datetimes: twelve months of 26,250 rows with 25,000 distinct values each, and 50,000 rows on one
        // hot point, 2025-02-15 10:00:00. A month shift keeps the time of day, so its bound on ts holds the whole day
        // 2025-02-15, with the hot point and a share of the February bucket.
        ColumnRefOperator ts = new ColumnRefOperator(3, DateType.DATETIME, "ts", true);
        List<Bucket> months = new ArrayList<>();
        for (int month = 1; month <= 12; month++) {
            LocalDateTime start = LocalDateTime.of(2025, month, 1, 0, 0);
            months.add(new Bucket(Utils.getLongFromDateTime(start), Utils.getLongFromDateTime(start.plusMonths(1)) - 1,
                    26_250L * month, 0L, 25_000L));
        }
        Statistics year = Statistics.builder().setOutputRowCount(365_000)
                .addColumnStatistic(ts, ColumnStatistic.builder()
                        .setMinValue(Utils.getLongFromDateTime(LocalDateTime.of(2025, 1, 1, 0, 0)))
                        .setMaxValue(Utils.getLongFromDateTime(LocalDateTime.of(2026, 1, 1, 0, 0)) - 1)
                        .setNullsFraction(0).setAverageRowSize(8).setDistinctValuesCount(300_001)
                        .setHistogram(new Histogram(months, Map.of("2025-02-15 10:00:00", 50_000L))).build())
                .build();
        Function monthsAdd = ExprUtils.getBuiltinFunction("months_add",
                new Type[] {DateType.DATETIME, IntegerType.INT}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        CallOperator shiftedTs = new CallOperator("months_add", DateType.DATETIME,
                ImmutableList.of(ts, ConstantOperator.createInt(1)), monthsAdd);

        // The hot point keeps its rows
        ScalarOperator hot = new BinaryPredicateOperator(BinaryType.EQ, shiftedTs,
                ConstantOperator.createDatetime(LocalDateTime.of(2025, 3, 15, 10, 0)));
        Assertions.assertEquals(50_000, rows(hot, year), 500);
        // The rows come from the shift, while ts is narrowed to the day by the bound
        ColumnStatistic hotDay = PredicateStatisticsCalculator.statisticsCalculate(hot, year).getColumnStatistic(ts);
        Assertions.assertTrue(hotDay.getMinValue() >= Utils.getLongFromDateTime(LocalDateTime.of(2025, 2, 15, 0, 0))
                && hotDay.getMaxValue() <= Utils.getLongFromDateTime(LocalDateTime.of(2025, 2, 16, 0, 0)), hotDay.toString());
        // Another point of the same day has about one row, not the 51,000 rows of the day
        ScalarOperator cold = new BinaryPredicateOperator(BinaryType.EQ, shiftedTs,
                ConstantOperator.createDatetime(LocalDateTime.of(2025, 3, 15, 11, 0)));
        Assertions.assertTrue(rows(cold, year) < 10, String.valueOf(rows(cold, year)));
        // A range from midnight is estimated by the bound: the days from 2025-02-15 on, with the hot point
        ScalarOperator range = new BinaryPredicateOperator(BinaryType.GE, shiftedTs,
                ConstantOperator.createDatetime(LocalDateTime.of(2025, 3, 15, 0, 0)));
        Assertions.assertTrue(rows(range, year) > 50_000 + 26_250 * 9, String.valueOf(rows(range, year)));
        // A range up to a point of the day takes January and the part of February before the point. The bound on ts
        // holds all of 2025-02-15 with the hot point, which the comparison leaves out.
        ScalarOperator beforeHot = new BinaryPredicateOperator(BinaryType.LT, shiftedTs,
                ConstantOperator.createDatetime(LocalDateTime.of(2025, 3, 15, 10, 0)));
        double expected = 26_250 + 26_250 * (14 + 10 / 24.0) / 28;
        Assertions.assertEquals(expected, rows(beforeHot, year), expected * 0.05);
        // Inside another function the shift is estimated by the bound, which keeps the hot point of the hour
        Function dateTrunc = ExprUtils.getBuiltinFunction("date_trunc",
                new Type[] {VarcharType.VARCHAR, DateType.DATETIME}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        ScalarOperator hotHour = new BinaryPredicateOperator(BinaryType.EQ, new CallOperator("date_trunc",
                DateType.DATETIME, ImmutableList.of(ConstantOperator.createVarchar("hour"), shiftedTs), dateTrunc),
                ConstantOperator.createDatetime(LocalDateTime.of(2025, 3, 15, 10, 0)));
        Assertions.assertTrue(rows(hotHour, year) > 50_000, String.valueOf(rows(hotHour, year)));
    }

    @Test
    public void testHeavyEndOfABucketKeepsItsRowsThroughAMonthShift() {
        // 2025-02-15 10:00:00 ends the first bucket with 10,000 rows and is not a most common value. The shift of it
        // keeps these rows, while the bound on ts holds the whole day.
        ColumnRefOperator ts = new ColumnRefOperator(3, DateType.DATETIME, "ts", true);
        LocalDateTime heavy = LocalDateTime.of(2025, 2, 15, 10, 0);
        Histogram histogram = new Histogram(List.of(
                new Bucket(Utils.getLongFromDateTime(LocalDateTime.of(2025, 1, 1, 0, 0)),
                        Utils.getLongFromDateTime(heavy), 40_000L, 10_000L),
                new Bucket(Utils.getLongFromDateTime(heavy.plusSeconds(1)),
                        Utils.getLongFromDateTime(LocalDateTime.of(2025, 3, 31, 23, 59, 59)), 80_000L, 0L)),
                Map.of());
        Statistics statistics = Statistics.builder().setOutputRowCount(80_000)
                .addColumnStatistic(ts, ColumnStatistic.builder()
                        .setMinValue(Utils.getLongFromDateTime(LocalDateTime.of(2025, 1, 1, 0, 0)))
                        .setMaxValue(Utils.getLongFromDateTime(LocalDateTime.of(2025, 3, 31, 23, 59, 59)))
                        .setNullsFraction(0).setAverageRowSize(8).setDistinctValuesCount(50_000)
                        .setHistogram(histogram).build())
                .build();
        Function monthsAdd = ExprUtils.getBuiltinFunction("months_add",
                new Type[] {DateType.DATETIME, IntegerType.INT}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        ScalarOperator equality = new BinaryPredicateOperator(BinaryType.EQ, new CallOperator("months_add",
                DateType.DATETIME, ImmutableList.of(ts, ConstantOperator.createInt(1)), monthsAdd),
                ConstantOperator.createDatetime(heavy.plusMonths(1)));
        Assertions.assertEquals(10_000, rows(equality, statistics), 1_000);
    }

    @Test
    public void testRangeOnAYearShiftOfADatetimeIsEstimatedByTheBound() {
        // The ends of the buckets of a datetime are seconds of one day. years_add(ts, 1) >= '2026-01-01' is exactly
        // ts >= '2025-01-01', and the bound on ts gives its rows from the second bucket.
        ColumnRefOperator ts = new ColumnRefOperator(3, DateType.DATETIME, "ts", true);
        double lower2 = Utils.getLongFromDateTime(LocalDateTime.of(2024, 10, 15, 13, 0, 5));
        double upper2 = Utils.getLongFromDateTime(LocalDateTime.of(2026, 6, 30, 12, 0));
        Histogram histogram = new Histogram(List.of(
                new Bucket(Utils.getLongFromDateTime(LocalDateTime.of(2017, 1, 1, 0, 0)),
                        Utils.getLongFromDateTime(LocalDateTime.of(2024, 10, 15, 13, 0)), 100_000L, 0L),
                new Bucket(lower2, upper2, 1_000_000L, 0L)), Map.of());
        Statistics statistics = Statistics.builder().setOutputRowCount(1_000_000)
                .addColumnStatistic(ts, ColumnStatistic.builder()
                        .setMinValue(Utils.getLongFromDateTime(LocalDateTime.of(2017, 1, 1, 0, 0)))
                        .setMaxValue(upper2).setNullsFraction(0).setAverageRowSize(8)
                        .setDistinctValuesCount(900_000).setHistogram(histogram).build())
                .build();
        Function yearsAdd = ExprUtils.getBuiltinFunction("years_add",
                new Type[] {DateType.DATETIME, IntegerType.INT}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        ScalarOperator from2026 = new BinaryPredicateOperator(BinaryType.GE, new CallOperator("years_add",
                DateType.DATETIME, ImmutableList.of(ts, ConstantOperator.createInt(1)), yearsAdd),
                ConstantOperator.createDatetime(LocalDateTime.of(2026, 1, 1, 0, 0)));
        double from2025 = Utils.getLongFromDateTime(LocalDateTime.of(2025, 1, 1, 0, 0));
        double expected = 900_000 * (upper2 - from2025) / (upper2 - lower2);
        Assertions.assertEquals(expected, rows(from2026, statistics), expected * 0.02);
    }
}
