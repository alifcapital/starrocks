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
import com.starrocks.catalog.Function;
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
import com.starrocks.statistic.StatisticsCollectJob;
import com.starrocks.thrift.TStatisticData;
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

// A string column is estimated from its smallest and largest strings, as ANALYZE or the metrics of a file give them,
// and a string of dates cast to DATE or DATETIME takes its range from them when they share one layout.
public class StringCastStatisticsTest {
    private static final ColumnRefOperator TEXT = new ColumnRefOperator(1, VarcharType.VARCHAR, "created_at", true);
    private static final LocalDateTime FIRST = LocalDateTime.of(2026, 8, 11, 0, 0);

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

    // Every hour of 2026-08-11 to 2026-08-19, written as ISO timestamps
    private static List<String> hours() {
        List<String> values = new ArrayList<>();
        for (int hour = 0; hour < 9 * 24; hour++) {
            LocalDateTime time = FIRST.plusHours(hour);
            values.add(time.toLocalDate() + "T" + String.format("%02d:00:00.000000Z", time.getHour()));
        }
        return values;
    }

    private static Statistics analyzed(List<String> values) {
        return Statistics.builder().setOutputRowCount(values.size()).addColumnStatistic(TEXT, ColumnStatistic.builder()
                .setNullsFraction(0).setAverageRowSize(27).setDistinctValuesCount(values.size())
                .setStringBounds(new StringBounds(values.get(0), values.get(values.size() - 1),
                        StatisticsCollectJob.STRING_BOUND_LENGTH, false)).build()).build();
    }

    private static double rows(ScalarOperator predicate, Statistics statistics) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics).getOutputRowCount();
    }

    private static long count(List<String> values, java.util.function.Predicate<String> predicate) {
        return values.stream().filter(predicate).count();
    }

    private static void assertClose(double exact, double estimate, double factor) {
        Assertions.assertTrue(estimate <= exact * factor + 1 && exact <= estimate * factor + 1,
                "exact " + exact + ", estimate " + estimate);
    }

    private static ConstantOperator text(String value) {
        return ConstantOperator.createVarchar(value);
    }

    private static TStatisticData data(String min, String max, long rows, long nulls) {
        TStatisticData data = new TStatisticData();
        data.setColumnName("c");
        data.setRowCount(rows);
        data.setNullCount(nulls);
        data.setCountDistinct(10);
        data.setDataSize(rows * 10);
        data.setMin(min);
        data.setMax(max);
        return data;
    }

    @Test
    public void testAnalyzeBoundsAreForEstimatesOnly() {
        ColumnStatistic statistic = ColumnBasicStatsCacheLoader.buildColumnStatistics(data("a", "z", 100, 0),
                "", "", "t", "c", VarcharType.VARCHAR);
        // DeriveRangeJoinPredicateRule and TopNAggregationCost read the connector bounds only
        Assertions.assertNull(statistic.getMinString());
        Assertions.assertNull(statistic.getMaxString());
        Assertions.assertEquals(new StringBounds("a", "z", StatisticsCollectJob.STRING_BOUND_LENGTH, false),
                statistic.getStringBounds());
        Assertions.assertTrue(statistic.isInfiniteRange());
        // '' is a bound only as the minimum of a column with values
        Assertions.assertEquals("", ColumnBasicStatsCacheLoader.buildColumnStatistics(data("", "z", 100, 10),
                "", "", "t", "c", VarcharType.VARCHAR).getStringBounds().min());
        Assertions.assertNull(ColumnBasicStatsCacheLoader.buildColumnStatistics(data("", "", 100, 100),
                "", "", "t", "c", VarcharType.VARCHAR).getStringBounds());
        Assertions.assertNull(ColumnBasicStatsCacheLoader.buildColumnStatistics(data("a", "", 100, 0),
                "", "", "t", "c", VarcharType.VARCHAR).getStringBounds());
        // A maximum of 200 characters may be cut
        String cut = "a".repeat(StatisticsCollectJob.STRING_BOUND_LENGTH);
        StringBounds bounds = ColumnBasicStatsCacheLoader.buildColumnStatistics(data("a", cut, 100, 0),
                "", "", "t", "c", VarcharType.VARCHAR).getStringBounds();
        Assertions.assertTrue(bounds.maxMayBeCut());
        Assertions.assertFalse(new StringBounds("a", "b", StatisticsCollectJob.STRING_BOUND_LENGTH, false)
                .maxMayBeCut());
    }

    @Test
    public void testPlainStringComparisonsKeepTheBaseEstimate() {
        // The string bounds serve the casts of text to dates only: a comparison of the column itself is estimated as
        // without them
        List<String> values = hours();
        Statistics withBounds = analyzed(values);
        Statistics without = Statistics.builder().setOutputRowCount(values.size()).addColumnStatistic(TEXT,
                ColumnStatistic.buildFrom(withBounds.getColumnStatistic(TEXT)).setStringBounds(null).build()).build();
        ScalarOperator from15 = new BinaryPredicateOperator(BinaryType.GE, TEXT, text("2026-08-15"));
        ScalarOperator window = Utils.compoundAnd(from15,
                new BinaryPredicateOperator(BinaryType.LT, TEXT, text("2026-08-17")));
        for (ScalarOperator predicate : List.of(from15, window,
                new BinaryPredicateOperator(BinaryType.LE, TEXT, text("2026-08-13 19:00:00")),
                new BinaryPredicateOperator(BinaryType.GT, TEXT, text("2027")),
                new BinaryPredicateOperator(BinaryType.EQ, TEXT, text("2026-08-15T00:00:00.000000Z")))) {
            Assertions.assertEquals(rows(predicate, without), rows(predicate, withBounds), predicate.toString());
        }
    }

    @Test
    public void testTextCastToDatetimeAndDate() {
        List<String> values = hours();
        Statistics statistics = analyzed(values);
        CastOperator datetime = new CastOperator(DateType.DATETIME, TEXT);
        ColumnStatistic cast = ExpressionStatisticCalculator.calculate(datetime, statistics);
        Assertions.assertEquals(Utils.getLongFromDateTime(FIRST), cast.getMinValue());
        Assertions.assertEquals(Utils.getLongFromDateTime(FIRST.plusHours(9 * 24 - 1)), cast.getMaxValue());
        CastOperator date = new CastOperator(DateType.DATE, TEXT);
        Assertions.assertEquals(9, ExpressionStatisticCalculator.calculate(date, statistics).getDistinctValuesCount());
        assertClose(24, rows(new BinaryPredicateOperator(BinaryType.EQ, date,
                ConstantOperator.createDate(FIRST.plusDays(2))), statistics), 1.01);
        // hours_add(CAST(created_at AS DATETIME), 5) over one day of local time bounds the cast to one day of UTC
        Function hoursAdd = ExprUtils.getBuiltinFunction("hours_add",
                new Type[] {DateType.DATETIME, IntegerType.INT}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        CallOperator local = new CallOperator("hours_add", DateType.DATETIME,
                ImmutableList.of(datetime, ConstantOperator.createInt(5)), hoursAdd);
        LocalDateTime from = FIRST.plusDays(3).plusHours(5);
        ScalarOperator day = Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.GE, local, ConstantOperator.createDatetime(from)),
                new BinaryPredicateOperator(BinaryType.LT, local, ConstantOperator.createDatetime(from.plusDays(1))));
        assertClose(24, rows(day, statistics), 1.1);
    }

    // The created_at column of the acceptance table: ISO timestamps from 2018-08-15T13:16:23.509905Z to
    // 2026-10-07T08:58:22.946788Z in 397,770,593 rows
    private static final double MOBI_ROWS = 397_770_593;
    private static final String MOBI_MIN = "2018-08-15T13:16:23.509905Z";
    private static final String MOBI_MAX = "2026-10-07T08:58:22.946788Z";

    private static double second(int year, int month, int day, int hour, int minute, int second) {
        return Utils.getLongFromDateTime(LocalDateTime.of(year, month, day, hour, minute, second));
    }

    // The predicates 1, 2, 3, 4 and 6 of the acceptance: hours_add(CAST(created_at AS DATETIME), 5) between two times,
    // CAST(created_at AS DATE) over one day, hours_add(...) >= '2021-01-01', convert_tz(...) over one month and
    // CAST(created_at AS DATE) = '2026-08-01'
    private static List<ScalarOperator> mobiPredicates() {
        CastOperator datetime = new CastOperator(DateType.DATETIME, TEXT);
        CastOperator date = new CastOperator(DateType.DATE, TEXT);
        Function hoursAdd = ExprUtils.getBuiltinFunction("hours_add",
                new Type[] {DateType.DATETIME, IntegerType.INT}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        CallOperator local = new CallOperator("hours_add", DateType.DATETIME,
                ImmutableList.of(datetime, ConstantOperator.createInt(5)), hoursAdd);
        Function convertTz = ExprUtils.getBuiltinFunction("convert_tz",
                new Type[] {DateType.DATETIME, VarcharType.VARCHAR, VarcharType.VARCHAR},
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        CallOperator dushanbe = new CallOperator("convert_tz", DateType.DATETIME, ImmutableList.of(datetime,
                ConstantOperator.createVarchar("Etc/UTC"), ConstantOperator.createVarchar("Asia/Dushanbe")), convertTz);
        ConstantOperator august = ConstantOperator.createDate(LocalDateTime.of(2026, 8, 1, 0, 0));
        return List.of(
                Utils.compoundAnd(new BinaryPredicateOperator(BinaryType.GE, local,
                                ConstantOperator.createDatetime(LocalDateTime.of(2026, 9, 22, 18, 55))),
                        new BinaryPredicateOperator(BinaryType.LE, local,
                                ConstantOperator.createDatetime(LocalDateTime.of(2026, 9, 22, 19, 30)))),
                Utils.compoundAnd(new BinaryPredicateOperator(BinaryType.GE, date, august),
                        new BinaryPredicateOperator(BinaryType.LE, date, august)),
                new BinaryPredicateOperator(BinaryType.GE, local,
                        ConstantOperator.createDatetime(LocalDateTime.of(2021, 1, 1, 0, 0))),
                Utils.compoundAnd(new BinaryPredicateOperator(BinaryType.GE, dushanbe,
                                ConstantOperator.createDatetime(LocalDateTime.of(2026, 8, 1, 0, 0))),
                        new BinaryPredicateOperator(BinaryType.LT, dushanbe,
                                ConstantOperator.createDatetime(LocalDateTime.of(2026, 9, 1, 0, 0)))),
                new BinaryPredicateOperator(BinaryType.EQ, date, august));
    }

    @Test
    public void testAcceptancePredicatesByAnalyzeBounds() {
        Statistics statistics = Statistics.builder().setOutputRowCount(MOBI_ROWS).addColumnStatistic(TEXT,
                ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(27).setDistinctValuesCount(3e8)
                        .setStringBounds(new StringBounds(MOBI_MIN, MOBI_MAX, StatisticsCollectJob.STRING_BOUND_LENGTH,
                                false)).build()).build();
        // Evenly between the bounds read as times, in whole seconds as the statistics hold them
        double low = second(2018, 8, 15, 13, 16, 23);
        double high = second(2026, 10, 7, 8, 58, 22);
        double days = Math.floor((Utils.getLongFromDateTime(LocalDateTime.of(2026, 10, 7, 0, 0))
                - Utils.getLongFromDateTime(LocalDateTime.of(2018, 8, 15, 0, 0))) / 86400.0 + 0.5) + 1;
        double[] expected = {
                MOBI_ROWS * (second(2026, 9, 22, 14, 30, 0) - second(2026, 9, 22, 13, 55, 0)) / (high - low),
                MOBI_ROWS / days,
                MOBI_ROWS * (high - second(2020, 12, 31, 19, 0, 0)) / (high - low),
                MOBI_ROWS * (second(2026, 8, 31, 19, 0, 0) - second(2026, 7, 31, 19, 0, 0)) / (high - low),
                MOBI_ROWS / days};
        List<ScalarOperator> predicates = mobiPredicates();
        for (int i = 0; i < predicates.size(); i++) {
            Assertions.assertEquals(expected[i], rows(predicates.get(i), statistics), expected[i] * 1e-6,
                    predicates.get(i).toString());
        }
    }

    @Test
    public void testAcceptancePredicatesByStringHistogram() {
        // MCV statistics of created_at: an older bucket, a recent one and one hot value. ANALYZE ends a bucket at any
        // time of a day, so the recent bucket starts on the last day of the older one.
        double rows2025 = 200_000_000;
        double hot = 1_000_000;
        double rows2026 = MOBI_ROWS - rows2025 - hot;
        List<Bucket> buckets = List.of(
                new StringBucket(MOBI_MIN, "2025-12-31T08:00:00.000000Z", (long) rows2025, 0, 1_000_000),
                new StringBucket("2025-12-31T08:00:01.000000Z", MOBI_MAX, (long) (rows2025 + rows2026), 0, 1_000_000));
        Statistics statistics = Statistics.builder().setOutputRowCount(MOBI_ROWS).addColumnStatistic(TEXT,
                ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(27).setDistinctValuesCount(3e8)
                        .setHistogram(Histogram.forStrings(buckets, Map.of("2026-08-01T10:00:00.000000Z", (long) hot)))
                        .build()).build();
        double low2025 = second(2018, 8, 15, 13, 16, 23);
        double high2025 = second(2025, 12, 31, 8, 0, 0);
        double low2026 = second(2025, 12, 31, 8, 0, 1);
        double high2026 = second(2026, 10, 7, 8, 58, 22);
        List<ScalarOperator> predicates = mobiPredicates();
        // A string read as a DATETIME keeps its value, so the rows of a bucket spread evenly between its ends.
        // The window of predicate 1 takes its share of the recent bucket.
        double window = rows2026 * (second(2026, 9, 22, 14, 30, 0) - second(2026, 9, 22, 13, 55, 0))
                / (high2026 - low2026);
        Assertions.assertEquals(window, rows(predicates.get(0), statistics), 2);
        // The hot value is a time of the day of predicates 2 and 6, which holds the rows of the recent bucket for the
        // 86,400 seconds of the day as well
        double dayOfBucket = rows2026 * 86400 / (high2026 - low2026);
        Assertions.assertEquals(hot + dayOfBucket, rows(predicates.get(1), statistics), 2);
        Assertions.assertEquals(hot + dayOfBucket, rows(predicates.get(4), statistics), 2);
        // Predicate 3 takes the part of the older bucket from 2020-12-31 19:00 UTC, the recent bucket and the value
        double from2021 = rows2025 * (high2025 - second(2020, 12, 31, 19, 0, 0)) / (high2025 - low2025)
                + rows2026 + hot;
        Assertions.assertEquals(from2021, rows(predicates.get(2), statistics), 2);
        // Predicate 4 takes the share of August in the recent bucket and the hot value
        double month = rows2026 * (second(2026, 8, 31, 19, 0, 0) - second(2026, 7, 31, 19, 0, 0))
                / (high2026 - low2026) + hot;
        Assertions.assertEquals(month, rows(predicates.get(3), statistics), 2);
        // Read as DATE, the recent bucket starts on 2025-12-31, the last day of the older one. The older bucket takes
        // the rows of the recent one up to the end of that day, so the dates from 2026 are the rest of the recent
        // bucket and the value.
        double firstDay = Math.round(rows2026 * (second(2026, 1, 1, 0, 0, 0) - low2026) / (high2026 - low2026));
        Assertions.assertEquals(rows2026 - firstDay + hot, rows(new BinaryPredicateOperator(BinaryType.GE,
                new CastOperator(DateType.DATE, TEXT), ConstantOperator.createDate(LocalDateTime.of(2026, 1, 1, 0, 0))),
                statistics), 2);
    }

    @Test
    public void testDateCastToTextKeepsNoBounds() {
        // Nothing reads string bounds of a cast to text: the cast keeps the NDV and a mapped histogram only
        ColumnRefOperator day = new ColumnRefOperator(2, DateType.DATE, "d", true);
        Statistics statistics = Statistics.builder().setOutputRowCount(900).addColumnStatistic(day,
                new ColumnStatistic(Utils.getLongFromDateTime(FIRST), Utils.getLongFromDateTime(FIRST.plusDays(8)),
                        0, 4, 9)).build();
        ColumnStatistic cast = ExpressionStatisticCalculator.calculate(new CastOperator(VarcharType.VARCHAR, day),
                statistics);
        Assertions.assertNull(cast.getStringBounds());
        Assertions.assertTrue(cast.isInfiniteRange());
        Assertions.assertEquals(9, cast.getDistinctValuesCount());
    }

    @Test
    public void testLayoutsThatDifferKeepNoRange() {
        for (List<String> bounds : List.of(List.of("20261001", "2026-10-13T02:52:47.123Z"),
                List.of("2026-08-11 00:00:00", "2026-08-19T23:00:00"),
                List.of("2026-08-11T00:00:00Z", "2026/08/19T23:00:00Z"))) {
            Statistics statistics = Statistics.builder().setOutputRowCount(100).addColumnStatistic(TEXT,
                    ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(20).setDistinctValuesCount(100)
                            .setStringBounds(new StringBounds(bounds.get(0), bounds.get(1), StringBounds.WHOLE, false))
                            .build()).build();
            Assertions.assertTrue(ExpressionStatisticCalculator.calculate(new CastOperator(DateType.DATETIME, TEXT),
                    statistics).isInfiniteRange(), bounds.toString());
        }
        // A fraction of a second of another length sorts the same to the second
        Assertions.assertTrue(StringDateLayout.sameLayout("2026-08-11T00:00:00.5Z", false,
                "2026-08-19T23:00:00.123456Z", false));
        // A date and a datetime of the same fields sort as their dates; digits alone of two lengths do not
        Assertions.assertTrue(StringDateLayout.sameLayout("2026-08-11", false, "2026-08-19 10:00:00", false));
        Assertions.assertFalse(StringDateLayout.sameLayout("201231", false, "20240101", false));
        Assertions.assertTrue(StringDateLayout.sameLayout("2018-08-15T13:16:2", true, "2026-10-13T02:52:47", false));
    }

    @Test
    public void testCutIcebergUpperBounds() {
        Object[][] cases = {
                {"2025-08-27T08:1:", "2025-08-27T08:19:59.999999"},
                {"2026-10-13T02:53", "2026-10-13T02:53:59.999999"},
                {"26-10-13 02:52:5", "2026-10-13T02:52:59.999999"},
                {"2026-10-13T02:5", "2026-10-13T02:05:00"},
        };
        for (Object[] item : cases) {
            StringBounds bounds = new StringBounds("2018-08-15T13:16", (String) item[0], 16, true);
            Assertions.assertEquals(LocalDateTime.parse((String) item[1]), StringDateLayout.upper(bounds),
                    (String) item[0]);
        }
        // The lower bound reads as it is
        Assertions.assertEquals(LocalDateTime.of(2018, 8, 15, 13, 16), StatisticsDatetimeParser.parse("2018-08-15T13:16"));
        Assertions.assertTrue(StringDateLayout.sameLayout("2018-08-15T13:16", true,
                StringDateLayout.upperText(new StringBounds("2018-08-15T13:16", "2025-08-27T08:1:", 16, true)), true));
    }

    @Test
    public void testLastDayOfTextBucketsKeepsItsRows() {
        // As for DATETIME: the second bucket spreads 100 rows from Aug 1 noon to the end of Aug 2, about 67 on Aug 2
        List<Bucket> buckets = List.of(
                new StringBucket("2026-08-01T00:00:00Z", "2026-08-01T12:00:00Z", 100, 1, 100),
                new StringBucket("2026-08-01T12:00:01Z", "2026-08-02T23:59:59Z", 200, 1, 100));
        Statistics statistics = Statistics.builder().setOutputRowCount(200).addColumnStatistic(TEXT,
                ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(20).setDistinctValuesCount(200)
                        .setHistogram(Histogram.forStrings(buckets, Map.of())).build()).build();
        CastOperator day = new CastOperator(DateType.DATE, TEXT);
        ConstantOperator august2 = ConstantOperator.createDate(LocalDateTime.of(2026, 8, 2, 0, 0));
        assertClose(67, rows(new BinaryPredicateOperator(BinaryType.EQ, day, august2), statistics), 1.5);
        assertClose(67, rows(new BinaryPredicateOperator(BinaryType.GE, day, august2), statistics), 1.5);
    }

    @Test
    public void testDateAndTimeOfOneLayoutMergeTheTimesOfADay() {
        // A bucket from a bare date to a time of a day: 300 rows over 60 hours, 12 of them on Aug 3
        List<Bucket> buckets = List.of(new StringBucket("2026-08-01", "2026-08-03 12:00:00", 300, 1, 300));
        Statistics statistics = Statistics.builder().setOutputRowCount(300).addColumnStatistic(TEXT,
                ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(20).setDistinctValuesCount(300)
                        .setHistogram(Histogram.forStrings(buckets, Map.of())).build()).build();
        ConstantOperator august3 = ConstantOperator.createDate(LocalDateTime.of(2026, 8, 3, 0, 0));
        Assertions.assertEquals(1 + Math.round(299 * 12 / 60.0), rows(new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(DateType.DATE, TEXT), august3), statistics), 1);
    }

    @Test
    public void testDigitsOfTwoLengthsKeepNoRange() {
        // '201231' is 2020-12-31 and '20240101' 2024-01-01, while '20130101' lies between them as bytes
        Statistics statistics = Statistics.builder().setOutputRowCount(100).addColumnStatistic(TEXT,
                ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(8).setDistinctValuesCount(100)
                        .setStringBounds(new StringBounds("201231", "20240101", StatisticsCollectJob.STRING_BOUND_LENGTH,
                                false)).build()).build();
        Assertions.assertTrue(ExpressionStatisticCalculator.calculate(new CastOperator(DateType.DATE, TEXT), statistics)
                .isInfiniteRange());
    }

    @Test
    public void testDatetimeTextBucketsCoverTheirLastSecond() {
        ColumnRefOperator time = new ColumnRefOperator(3, DateType.DATETIME, "t", true);
        double from = Utils.getLongFromDateTime(FIRST);
        double to = Utils.getLongFromDateTime(FIRST.plusHours(1));
        Statistics statistics = Statistics.builder().setOutputRowCount(10).addColumnStatistic(time,
                ColumnStatistic.builder().setMinValue(from).setMaxValue(to).setNullsFraction(0).setAverageRowSize(8)
                        .setDistinctValuesCount(10).setHistogram(new Histogram(List.of(new Bucket(from, to, 10L, 1L)),
                                Map.of())).build()).build();
        Histogram text = ExpressionStatisticCalculator.calculate(new CastOperator(VarcharType.VARCHAR, time), statistics)
                .getHistogram();
        StringBucket bucket = (StringBucket) text.getBuckets().get(0);
        Assertions.assertEquals("2026-08-11 00:00:00", bucket.getLowerString());
        // A value of the last second with microseconds sorts below the upper end
        Assertions.assertEquals("2026-08-11 01:00:00.999999", bucket.getUpperString());
        Assertions.assertTrue(StringBucket.compare("2026-08-11 01:00:00.5", bucket.getUpperString()) < 0);
    }

    @Test
    public void testStringHistogramMapsToDatetimeBuckets() {
        // 900 rows on one day and 100 on the eight others, as buckets of a string histogram of the column
        List<Bucket> buckets = List.of(
                new StringBucket("2026-08-11T00:00:00Z", "2026-08-15T00:00:00Z", 100, 1, 50),
                new StringBucket("2026-08-15T00:00:01Z", "2026-08-15T23:59:59Z", 1000, 1, 500));
        Histogram histogram = Histogram.forStrings(buckets, Map.of("2026-08-19T00:00:00Z", 100L, "bad", 50L));
        Statistics statistics = Statistics.builder().setOutputRowCount(1150).addColumnStatistic(TEXT,
                ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(20).setDistinctValuesCount(552)
                        .setHistogram(histogram).build()).build();
        ColumnStatistic cast = ExpressionStatisticCalculator.calculate(new CastOperator(DateType.DATETIME, TEXT),
                statistics);
        Assertions.assertEquals(2, cast.getHistogram().getBuckets().size());
        Assertions.assertEquals(50.0 / 1150, cast.getNullsFraction(), 1e-6);
        ScalarOperator day = new BinaryPredicateOperator(BinaryType.GE, new CastOperator(DateType.DATETIME, TEXT),
                ConstantOperator.createDatetime(LocalDateTime.of(2026, 8, 15, 0, 0, 1)));
        // The bucket of the day and the most common value after it
        assertClose(1000, rows(day, statistics), 1.1);
    }
}
