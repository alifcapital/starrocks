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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

// Second round of review probes. Each test states the correct expectation; a failure confirms a defect.
public class CastStatsReview2Test {
    private static final double ROWS = 1_000_000;

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

    private static double rows(ScalarOperator predicate, Statistics statistics) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics).getOutputRowCount();
    }

    private static ConstantOperator day(int day) {
        return ConstantOperator.createDate(LocalDateTime.of(2026, 8, day, 0, 0));
    }

    private static Statistics single(ColumnRefOperator column, ColumnStatistic statistic) {
        return Statistics.builder().setOutputRowCount(ROWS).addColumnStatistic(column, statistic).build();
    }

    // CAST(x AS DATE) IN (two days) when the statistics of the cast have no range: the NDV of the cast is then the
    // NDV of x, which counts seconds or strings, not days. The base gave the default IN selectivity, 0.5.
    @Test
    public void testInOverOrderedCastWithoutRange() {
        // A DATETIME column with a known NDV and no bounds, as an Iceberg column that a file left the bounds out of
        ColumnRefOperator dt = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
        Statistics times = single(dt, ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(8)
                .setDistinctValuesCount(ROWS).build());
        // A string column of timestamps without string bounds (no ANALYZE, or bounds of two layouts)
        ColumnRefOperator text = new ColumnRefOperator(2, VarcharType.VARCHAR, "s", true);
        Statistics strings = single(text, ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(27)
                .setDistinctValuesCount(ROWS).build());
        Statistics twoLayouts = single(text, ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(27)
                .setDistinctValuesCount(ROWS).setStringBounds(new StringBounds("20180815", "2026-10-07T08:58:22Z",
                        200, false)).build());
        double dtIn = rows(new InPredicateOperator(false, new CastOperator(DateType.DATE, dt), day(1), day(2)),
                times);
        double dtEq = rows(new BinaryPredicateOperator(BinaryType.EQ, new CastOperator(DateType.DATE, dt), day(1)),
                times);
        double textIn = rows(new InPredicateOperator(false, new CastOperator(DateType.DATE, text), day(1), day(2)),
                strings);
        double layoutsIn = rows(new InPredicateOperator(false, new CastOperator(DateType.DATE, text), day(1),
                day(2)), twoLayouts);
        String report = "dt IN " + dtIn + " (dt EQ " + dtEq + "), text IN " + textIn + ", two layouts IN "
                + layoutsIn + ", base default "
                + ROWS * StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
        System.out.println("REVIEW2 " + report);
        Assertions.assertTrue(dtIn >= ROWS * 0.01, report);
        Assertions.assertTrue(textIn >= ROWS * 0.01, report);
        Assertions.assertTrue(layoutsIn >= ROWS * 0.01, report);
    }

    // An INT of YYYYMMDD cast to DATE is one to one: the upper end of a bucket keeps the rows of its own value.
    // Bucket of 100 rows over Aug 1 .. Aug 10, 10 distinct values, 10 rows each.
    @Test
    public void testOneToOneSegmentKeepsUpperRows() {
        ColumnRefOperator number = new ColumnRefOperator(1, IntegerType.INT, "i", true);
        Histogram histogram = new Histogram(List.of(new Bucket(20260801, 20260810, 100L, 10L, 10L)), Map.of());
        Statistics statistics = Statistics.builder().setOutputRowCount(100).addColumnStatistic(number,
                ColumnStatistic.builder().setMinValue(20260801).setMaxValue(20260810).setNullsFraction(0)
                        .setAverageRowSize(4).setDistinctValuesCount(10).setHistogram(histogram).build()).build();
        CastOperator cast = new CastOperator(DateType.DATE, number);
        double last = rows(new BinaryPredicateOperator(BinaryType.EQ, cast, day(10)), statistics);
        double inner = rows(new BinaryPredicateOperator(BinaryType.EQ, cast, day(5)), statistics);
        double column = rows(new BinaryPredicateOperator(BinaryType.EQ, number, ConstantOperator.createInt(20260810)),
                statistics);
        String report = "cast = Aug 10: " + last + ", cast = Aug 5: " + inner + ", column = 20260810: " + column
                + ", exact 10 each";
        System.out.println("REVIEW2 " + report);
        Assertions.assertTrue(last <= 15, report);
    }

    // A string histogram of created_at as ANALYZE builds it: bucket ends fall at any time of a day, so the next
    // bucket starts on the same day. 400M rows, most of them recent. The same buckets with ends at midnight, as the
    // acceptance test builds them, for comparison.
    private static double fromJanuary(boolean midnight) {
        ColumnRefOperator text = new ColumnRefOperator(2, VarcharType.VARCHAR, "created_at", true);
        String end1 = midnight ? "2023-06-30T23:59:59Z" : "2023-06-30T10:11:12Z";
        String start2 = midnight ? "2023-07-01T00:00:00Z" : "2023-06-30T10:11:13Z";
        String end2 = midnight ? "2025-12-31T23:59:59Z" : "2025-12-31T08:00:00Z";
        String start3 = midnight ? "2026-01-01T00:00:00Z" : "2025-12-31T08:00:01Z";
        List<Bucket> buckets = List.of(
                new StringBucket("2018-08-15T13:16:23Z", end1, 50_000_000L, 1, 1_000_000),
                new StringBucket(start2, end2, 150_000_000L, 1, 1_000_000),
                new StringBucket(start3, "2026-10-07T08:58:22Z", 400_000_000L, 1, 1_000_000));
        Statistics statistics = Statistics.builder().setOutputRowCount(400_000_000).addColumnStatistic(text,
                ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(20).setDistinctValuesCount(3e8)
                        .setHistogram(Histogram.forStrings(buckets, Map.of())).build()).build();
        ConstantOperator january = ConstantOperator.createDate(LocalDateTime.of(2026, 1, 1, 0, 0));
        return rows(new BinaryPredicateOperator(BinaryType.GE, new CastOperator(DateType.DATE, text), january),
                statistics);
    }

    @Test
    public void testBucketsEndingInsideADayKeepTheSkew() {
        double midnight = fromJanuary(true);
        double inside = fromJanuary(false);
        String report = "CAST(created_at AS DATE) >= 2026-01-01: midnight ends " + midnight + ", ends inside a day "
                + inside + ", exact ~249M";
        System.out.println("REVIEW2 " + report);
        Assertions.assertTrue(inside >= 249e6 / 2, report);
    }

    // DATETIME to DATE images merge buckets that end on one day: two buckets of 100 rows each on Aug 1 only, then a
    // bucket of 100 rows over Aug 1 .. Aug 3. Aug 1 holds 200 + ~33 rows.
    @Test
    public void testMergedDayKeepsRowsOfEarlierBuckets() {
        ColumnRefOperator dt = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
        double start = Utils.getLongFromDateTime(LocalDateTime.of(2026, 8, 1, 0, 0));
        double end = Utils.getLongFromDateTime(LocalDateTime.of(2026, 8, 3, 23, 59, 59));
        Histogram histogram = new Histogram(List.of(new Bucket(start, start + 3600, 100L, 1L, 100L),
                new Bucket(start + 3601, start + 7200, 200L, 1L, 100L),
                new Bucket(start + 7201, end, 300L, 1L, 100L)), Map.of());
        Statistics statistics = Statistics.builder().setOutputRowCount(300).addColumnStatistic(dt,
                ColumnStatistic.builder().setMinValue(start).setMaxValue(end).setNullsFraction(0).setAverageRowSize(8)
                        .setDistinctValuesCount(300).setHistogram(histogram).build()).build();
        CastOperator cast = new CastOperator(DateType.DATE, dt);
        double first = rows(new BinaryPredicateOperator(BinaryType.EQ, cast, day(1)), statistics);
        double third = rows(new BinaryPredicateOperator(BinaryType.EQ, cast, day(3)), statistics);
        double atLeastSecond = rows(new BinaryPredicateOperator(BinaryType.GE, cast, day(2)), statistics);
        String report = "Aug 1: " + first + " (exact ~234), Aug 3: " + third + " (exact ~34), >= Aug 2: "
                + atLeastSecond + " (exact ~66)";
        System.out.println("REVIEW2 " + report);
        Assertions.assertTrue(first >= 117 && first <= 468, report);
        Assertions.assertTrue(third >= 17 && third <= 68, report);
    }
}
