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
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

// Review probes of feature/cast-statistics-4.1. Each test states the correct expectation; a failure confirms a defect.
public class CastStatsReviewTest {
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

    private static double seconds(LocalDateTime time) {
        return Utils.getLongFromDateTime(time);
    }

    private static double rows(ScalarOperator predicate, Statistics statistics) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics).getOutputRowCount();
    }

    // DATETIME histogram of 200 rows over 2026-08-01 00:00 .. 2026-08-02 23:59:59: bucket 1 holds 100 rows of the
    // morning of Aug 1, bucket 2 holds 100 rows from Aug 1 12:00:01 to the end of Aug 2, spread evenly, so about 67
    // rows fall on Aug 2. Each bucket upper value repeats once, as for a column of distinct timestamps.
    private static Statistics datetimeWithHistogram(ColumnRefOperator dt, boolean withHistogram) {
        double start = seconds(LocalDateTime.of(2026, 8, 1, 0, 0));
        double noon = seconds(LocalDateTime.of(2026, 8, 1, 12, 0));
        double end = seconds(LocalDateTime.of(2026, 8, 2, 23, 59, 59));
        Histogram histogram = new Histogram(List.of(new Bucket(start, noon, 100L, 1L, 100L),
                new Bucket(noon + 1, end, 200L, 1L, 100L)), Map.of());
        ColumnStatistic.Builder statistic = ColumnStatistic.builder().setMinValue(start).setMaxValue(end)
                .setNullsFraction(0).setAverageRowSize(8).setDistinctValuesCount(200);
        if (withHistogram) {
            statistic.setHistogram(histogram);
        }
        return Statistics.builder().setOutputRowCount(200).addColumnStatistic(dt, statistic.build()).build();
    }

    // CAST(dt AS DATE) = <last day> and >= <last day>: the image bucket ends on Aug 2 and keeps the upper repeats of
    // the last timestamp (1 row), while Aug 2 as a DATE holds ~67 rows.
    @Test
    public void testDatetimeToDateLastDayKeepsRowsOfTheDay() {
        ColumnRefOperator dt = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
        CastOperator day = new CastOperator(DateType.DATE, dt);
        ConstantOperator august2 = ConstantOperator.createDate(LocalDateTime.of(2026, 8, 2, 0, 0));
        Statistics withHistogram = datetimeWithHistogram(dt, true);
        Statistics withoutHistogram = datetimeWithHistogram(dt, false);

        double eqWithout = rows(new BinaryPredicateOperator(BinaryType.EQ, day, august2), withoutHistogram);
        double eqWith = rows(new BinaryPredicateOperator(BinaryType.EQ, day, august2), withHistogram);
        double geWith = rows(new BinaryPredicateOperator(BinaryType.GE, day, august2), withHistogram);
        String report = "eq without histogram " + eqWithout + ", eq with histogram " + eqWith
                + ", ge with histogram " + geWith + ", exact ~67";
        System.out.println("REVIEW " + report);
        Assertions.assertTrue(eqWith >= 20, report);
        Assertions.assertTrue(geWith >= 20, report);
    }

    // D1: layouts of different widths are not one layout when no bound may be cut
    @Test
    public void testLayoutPairs() {
        Assertions.assertFalse(StringDateLayout.sameLayout("201231", false, "20240101", false));
        Assertions.assertFalse(StringDateLayout.sameLayout("26-08-01", false, "2026-08-01", false));
        Assertions.assertFalse(StringDateLayout.sameLayout("2026-8-1", false, "2026-08-01", false));
        Assertions.assertFalse(StringDateLayout.sameLayout("2026-08-01 10:00:00", false, "2026-08-01T10:00:00", false));
        Assertions.assertFalse(StringDateLayout.sameLayout("20260801", false, "2026-08-01", false));
        Assertions.assertFalse(StringDateLayout.sameLayout("2026-08-01 9:00:00", false, "2026-08-01 10:00:00", false));
        Assertions.assertTrue(StringDateLayout.sameLayout("2026-08-01", false, "2026-08-01T10:00:00Z", false));
        Assertions.assertTrue(StringDateLayout.sameLayout("2026-08-01 10:00:00.5", false,
                "2026-08-01 10:00:00.123456", false));
    }

    // With a metrics mode that is not known, every bound may be cut, so a short whole minimum of another layout
    // passes: '2020-12-3' is taken as a cut of '2020-12-3x'.
    @Test
    public void testUnknownCutAcceptsShorterLayout() {
        ColumnRefOperator text = new ColumnRefOperator(1, VarcharType.VARCHAR, "s", true);
        ColumnStatistic statistic = ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(10)
                .setDistinctValuesCount(100)
                .setStringBounds(new StringBounds("2020-12-3", "2024-01-01", StringBounds.UNKNOWN, true)).build();
        Statistics statistics = Statistics.builder().setOutputRowCount(100).addColumnStatistic(text, statistic).build();
        ColumnStatistic cast = ExpressionStatisticCalculator.calculate(new CastOperator(DateType.DATE, text),
                statistics);
        System.out.println("REVIEW unknown cut: cast min " + cast.getMinValue() + " max " + cast.getMaxValue()
                + " 2020-12-03=" + seconds(LocalDateTime.of(2020, 12, 3, 0, 0)));
    }

    // Base CastStatisticsUtils.preservesValues treated DECIMALV2 <-> DECIMALV3 of a wide enough type as preserving.
    @Test
    public void testDecimalV2CastKeepsColumnStatistics() {
        DecimalType v2 = new DecimalType(PrimitiveType.DECIMALV2, 27, 9);
        DecimalType v3 = new DecimalType(PrimitiveType.DECIMAL128, 38, 9);
        System.out.println("REVIEW decimalv2 order " + CastStatisticsRules.order(v2, v3));
        ColumnRefOperator column = new ColumnRefOperator(1, v2, "d", true);
        ColumnStatistic statistic = ColumnStatistic.builder().setMinValue(0).setMaxValue(100).setNullsFraction(0)
                .setAverageRowSize(16).setDistinctValuesCount(100).build();
        Statistics statistics = Statistics.builder().setOutputRowCount(100).addColumnStatistic(column, statistic)
                .build();
        ColumnStatistic cast = ExpressionStatisticCalculator.calculate(new CastOperator(v3, column), statistics);
        System.out.println("REVIEW decimalv2 cast range " + cast.getMinValue() + " .. " + cast.getMaxValue());
        Assertions.assertTrue(CastStatisticsRules.preservesValues(v2, v3), "base kept the statistics of the column");
    }
}
