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

import com.starrocks.common.util.StringDateFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.function.Supplier;

// A comparison of a VARCHAR column whose table declares the format of its dates is estimated as a comparison of
// CAST(column AS DATETIME) that keeps the same rows, see PredicateStatisticsCalculator.asDatetimeComparison.
public class StringDateFormatEstimateTest {
    private static final ColumnRefOperator TEXT = new ColumnRefOperator(1, VarcharType.VARCHAR, "created_at", true);
    private static final StringDateFormat ISO = StringDateFormat.parse("%Y-%m-%dT%H:%i:%s.%fZ");
    private static final StringDateFormat SPACED = StringDateFormat.parse("%Y-%m-%d %H:%i:%s.%f");

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

    private static <T> T inScope(StringDateFormat format, Supplier<T> action) {
        return PredicateStatisticsCalculator.withStringDateFormats(column -> TEXT.equals(column) ? format : null,
                action);
    }

    private static Statistics estimated(ScalarOperator predicate, Statistics statistics, StringDateFormat format) {
        return inScope(format, () -> PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics));
    }

    private static double withFormat(ScalarOperator predicate, Statistics statistics) {
        return estimated(predicate, statistics, ISO).getOutputRowCount();
    }

    // The estimate in the scope of the caller, with no columns of a format outside a scope
    private static double estimate(ScalarOperator predicate, Statistics statistics) {
        return PredicateStatisticsCalculator.statisticsCalculate(List.of(predicate), statistics).getOutputRowCount();
    }

    private static ScalarOperator compare(BinaryType type, String value) {
        return new BinaryPredicateOperator(type, TEXT, ConstantOperator.createVarchar(value));
    }

    private static ScalarOperator castCompare(BinaryType type, LocalDateTime value) {
        return new BinaryPredicateOperator(type, new CastOperator(DateType.DATETIME, TEXT),
                ConstantOperator.createDatetime(value));
    }

    // The bounds of the column on the stand, from ANALYZE
    private static Statistics mobi() {
        return Statistics.builder().setOutputRowCount(397_770_593).addColumnStatistic(TEXT, ColumnStatistic.builder()
                .setNullsFraction(0).setAverageRowSize(27).setDistinctValuesCount(390_000_000)
                .setStringBounds(new StringBounds("2018-08-15T13:16:23.509905Z", "2026-10-07T08:58:22.946788Z",
                        200, false)).build()).build();
    }

    // A string of every hour of 2026-08-11 to 2026-08-19, 100 rows each, in one bucket a day, and a hot value
    private static Statistics hourly() {
        List<Bucket> buckets = new ArrayList<>();
        for (int day = 11; day <= 19; day++) {
            buckets.add(new StringBucket(String.format("2026-08-%02dT00:00:00.000000Z", day),
                    String.format("2026-08-%02dT23:00:00.000000Z", day), (day - 10) * 2400L, 100, 24));
        }
        Histogram histogram = Histogram.forStrings(buckets, Map.of("2026-08-15T06:30:00.000000Z", 5000L));
        return Statistics.builder().setOutputRowCount(9 * 2400 + 5000).addColumnStatistic(TEXT,
                ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(27).setDistinctValuesCount(9 * 24 + 1)
                        .setStringBounds(new StringBounds("2026-08-11T00:00:00.000000Z",
                                "2026-08-19T23:00:00.000000Z", 200, false))
                        .setHistogram(histogram).build()).build();
    }

    @Test
    public void testComparisonIsTheComparisonOfTheCast() {
        for (Statistics statistics : List.of(mobi(), hourly())) {
            for (LocalDateTime value : List.of(LocalDateTime.of(2026, 8, 14, 10, 0),
                    LocalDateTime.of(2026, 8, 15, 6, 30), LocalDateTime.of(2026, 8, 11, 0, 0),
                    LocalDateTime.of(2026, 4, 30, 19, 0))) {
                String text = value.toLocalDate() + "T" + value.toLocalTime() + ":00.000000Z";
                for (BinaryType type : List.of(BinaryType.GE, BinaryType.GT, BinaryType.LE, BinaryType.LT,
                        BinaryType.EQ)) {
                    Assertions.assertEquals(estimate(castCompare(type, value), statistics),
                            withFormat(compare(type, text), statistics), 1e-6, type + " " + text);
                }
            }
        }
    }

    @Test
    public void testBetweenIsTheIntervalOfTheCast() {
        ScalarOperator window = Utils.compoundAnd(compare(BinaryType.GE, "2026-08-14T10:00:00.000000Z"),
                compare(BinaryType.LT, "2026-08-14T12:00:00.000000Z"));
        ScalarOperator castWindow = Utils.compoundAnd(
                castCompare(BinaryType.GE, LocalDateTime.of(2026, 8, 14, 10, 0)),
                castCompare(BinaryType.LT, LocalDateTime.of(2026, 8, 14, 12, 0)));
        Assertions.assertEquals(estimate(castWindow, hourly()), withFormat(window, hourly()), 1e-6);
        // Two of the 23 hours between the ends of the bucket of the day, not half of the bucket
        Assertions.assertEquals(2400.0 * 2 / 23, withFormat(window, hourly()), 30);
    }

    @Test
    public void testShareOfTheRecentMonthsOnTheStand() {
        double share = withFormat(compare(BinaryType.GE, "2026-04-30T19:00:00.000000Z"), mobi()) / 397_770_593;
        Assertions.assertEquals(0.054, share, 0.002);
    }

    @Test
    public void testWithoutFormatTheEstimateOfStrings() {
        ScalarOperator recent = compare(BinaryType.GE, "2026-04-30T19:00:00.000000Z");
        Assertions.assertNotEquals(withFormat(recent, mobi()), estimate(recent, mobi()), 1);
        Assertions.assertEquals(estimate(recent, mobi()), estimated(recent, mobi(), null).getOutputRowCount(), 1e-9);
    }

    // An MCV list of (status, created_at) with a few hot pairs, the other rows spread over the range of the column
    private static Statistics withMcvGroup() {
        ColumnRefOperator status = new ColumnRefOperator(2, VarcharType.VARCHAR, "status", true);
        List<MultiColumnCombinedStats.McvEntry> entries = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("done", "2026-08-01T10:00:00.000000Z"), 50_000_000,
                        List.of(300_000_000L, 1_000_000L)),
                new MultiColumnCombinedStats.McvEntry(List.of("failed", "2026-09-01T10:00:00.000000Z"), 20_000_000,
                        List.of(50_000_000L, 1_000_000L)),
                new MultiColumnCombinedStats.McvEntry(List.of("done", "2019-01-01T00:00:00.000000Z"), 10_000_000,
                        List.of(300_000_000L, 1_000_000L)));
        return Statistics.buildFrom(mobi())
                .addColumnStatistic(status, ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(6)
                        .setDistinctValuesCount(3).build())
                .addMultiColumnStatistics(Set.of(status, TEXT), new MultiColumnCombinedStats(10_000, 397_770_593,
                        List.of(status, TEXT), entries, List.of(0L, 0L)))
                .build();
    }

    @Test
    public void testConjunctionOverAnMcvListIsTheOneOfTheCast() {
        ColumnRefOperator status = new ColumnRefOperator(2, VarcharType.VARCHAR, "status", true);
        ScalarOperator done = new BinaryPredicateOperator(BinaryType.EQ, status, ConstantOperator.createVarchar("done"));
        for (String time : List.of("2026-04-30T19:00:00.000000Z", "2026-08-01T10:00:00.000000Z")) {
            ScalarOperator text = Utils.compoundAnd(done, compare(BinaryType.GE, time));
            ScalarOperator cast = Utils.compoundAnd(done, castCompare(BinaryType.GE,
                    LocalDateTime.parse(time.substring(0, time.length() - 1))));
            Assertions.assertEquals(estimate(cast, withMcvGroup()), withFormat(text, withMcvGroup()), 1, time);
        }
    }

    @Test
    public void testPlainRowCountIsTheRowCountOfTheEstimate() {
        for (Statistics statistics : List.of(mobi(), hourly(), withMcvGroup())) {
            for (ScalarOperator predicate : List.of(compare(BinaryType.GE, "2026-04-30T19:00:00.000000Z"),
                    Utils.compoundAnd(compare(BinaryType.GE, "2026-08-14T10:00:00.000000Z"),
                            compare(BinaryType.LT, "2026-08-14T12:00:00.000000Z")))) {
                double estimated = inScope(ISO,
                        () -> PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics, false)
                                .getOutputRowCount());
                double plain = inScope(ISO,
                        () -> PredicateStatisticsCalculator.plainRowCount(predicate, statistics));
                Assertions.assertEquals(Double.doubleToLongBits(estimated), Double.doubleToLongBits(plain),
                        predicate.toString());
            }
        }
    }

    @Test
    public void testColumnsAreClearedAfterTheEstimate() {
        ScalarOperator recent = compare(BinaryType.GE, "2026-04-30T19:00:00.000000Z");
        double base = estimate(recent, mobi());
        Assertions.assertThrows(IllegalStateException.class, () -> inScope(ISO, () -> {
            Assertions.assertNotEquals(base, estimate(recent, mobi()), 1);
            throw new IllegalStateException();
        }));
        Assertions.assertEquals(base, estimate(recent, mobi()), 1e-9);
        // An inner scope gives the outer one back
        inScope(ISO, () -> {
            double outer = estimate(recent, mobi());
            Assertions.assertEquals(base, inScope(null, () -> estimate(recent, mobi())), 1e-9);
            Assertions.assertEquals(outer, estimate(recent, mobi()), 1e-9);
            return null;
        });
    }

    @Test
    public void testFormatWithoutADayOrEqualityToAnotherLayoutKeepsTheEstimateOfStrings() {
        StringDateFormat month = StringDateFormat.parse("%Y-%m");
        Statistics months = Statistics.builder().setOutputRowCount(1200).addColumnStatistic(TEXT,
                ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(7).setDistinctValuesCount(12)
                        .setStringBounds(new StringBounds("2024-01", "2024-12", 200, false)).build()).build();
        for (String value : List.of("2024-03", "2024-03-01")) {
            ScalarOperator predicate = compare(BinaryType.GE, value);
            Assertions.assertEquals(estimate(predicate, months),
                    estimated(predicate, months, month).getOutputRowCount(), 1e-9, value);
        }
        for (Statistics statistics : List.of(mobi(), hourly())) {
            for (String value : List.of("20260815", "2026-08-15 12:00:00", "2026-08-15", "2026-08-15T12:00:00")) {
                ScalarOperator predicate = compare(BinaryType.EQ, value);
                Assertions.assertEquals(estimate(predicate, statistics), withFormat(predicate, statistics), 1e-9,
                        value);
            }
        }
    }

    @Test
    public void testColumnIsNarrowedAsByTheComparisonOfStrings() {
        for (Statistics statistics : List.of(mobi(), hourly())) {
            ScalarOperator equal = compare(BinaryType.EQ, "2026-08-15T06:30:00.000000Z");
            ColumnStatistic byStrings = PredicateStatisticsCalculator.statisticsCalculate(equal, statistics)
                    .getColumnStatistic(TEXT);
            ColumnStatistic byTime = estimated(equal, statistics, ISO).getColumnStatistic(TEXT);
            Assertions.assertEquals(byStrings.getDistinctValuesCount(), byTime.getDistinctValuesCount(), 1e-9);
        }
        Assertions.assertEquals(1, estimated(compare(BinaryType.EQ, "2026-08-15T06:30:00.000000Z"), hourly(), ISO)
                .getColumnStatistic(TEXT).getDistinctValuesCount(), 1e-9);
        ScalarOperator day = Utils.compoundAnd(compare(BinaryType.GE, "2026-08-15T00:00:00.000000Z"),
                compare(BinaryType.LT, "2026-08-16T00:00:00.000000Z"));
        Statistics byTime = estimated(day, hourly(), ISO);
        Statistics byStrings = PredicateStatisticsCalculator.statisticsCalculate(day, hourly());
        ColumnStatistic column = byTime.getColumnStatistic(TEXT);
        Assertions.assertTrue(column.getDistinctValuesCount() <= byTime.getOutputRowCount());
        Assertions.assertEquals(Math.min(byStrings.getColumnStatistic(TEXT).getDistinctValuesCount(),
                byTime.getOutputRowCount()), column.getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(byStrings.getColumnStatistic(TEXT).getHistogram().getMCV(),
                column.getHistogram().getMCV());
    }

    @Test
    public void testComparisonAndImplicitCastMakeOneInterval() {
        LocalDateTime upper = LocalDateTime.of(2026, 8, 14, 12, 0);
        ScalarOperator mixed = Utils.compoundAnd(compare(BinaryType.GE, "2026-08-14T10:00:00.000000Z"),
                new BinaryPredicateOperator(BinaryType.LT, new CastOperator(DateType.DATETIME, TEXT, true),
                        ConstantOperator.createDatetime(upper)));
        ScalarOperator cast = Utils.compoundAnd(castCompare(BinaryType.GE, LocalDateTime.of(2026, 8, 14, 10, 0)),
                castCompare(BinaryType.LT, upper));
        Assertions.assertEquals(estimate(cast, hourly()), withFormat(mixed, hourly()), 1e-6);
    }

    @Test
    public void testFirstTimeOfTheStringsAboveAConstant() {
        // ' ' sorts before 'T', so every time of the day is above
        Assertions.assertEquals(LocalDateTime.of(2026, 9, 23, 0, 0),
                PredicateStatisticsCalculator.firstTimeAbove(ISO, "2026-09-23 19:00:00", false));
        // '.' sorts before 'Z', so the whole second 19:00:00 is below
        Assertions.assertEquals(LocalDateTime.of(2026, 9, 23, 19, 0, 1),
                PredicateStatisticsCalculator.firstTimeAbove(ISO, "2026-09-23T19:00:00Z", false));
        // '-' sorts before '0': the strings of 2026 are below, those of 2027 above
        Assertions.assertEquals(LocalDateTime.of(2027, 1, 1, 0, 0),
                PredicateStatisticsCalculator.firstTimeAbove(ISO, "20260815", false));
        Assertions.assertEquals(LocalDateTime.of(2026, 9, 23, 19, 0),
                PredicateStatisticsCalculator.firstTimeAbove(ISO, "2026-09-23T19:00:00.000000Z", false));
        Assertions.assertEquals(LocalDateTime.of(2026, 9, 23, 19, 0, 0, 1000),
                PredicateStatisticsCalculator.firstTimeAbove(ISO, "2026-09-23T19:00:00.000000Z", true));
        Assertions.assertEquals(LocalDateTime.of(0, 1, 1, 0, 0), PredicateStatisticsCalculator.firstTimeAbove(ISO, "", false));
        Assertions.assertNull(PredicateStatisticsCalculator.firstTimeAbove(ISO, "abc", false));
        StringDateFormat day = StringDateFormat.parse("%Y-%m-%d");
        Assertions.assertEquals(LocalDateTime.of(2026, 9, 24, 0, 0),
                PredicateStatisticsCalculator.firstTimeAbove(day, "2026-09-23", true));
    }

    private static BinaryPredicateOperator asCast(StringDateFormat format, BinaryType type, String value) {
        return inScope(format, () -> PredicateStatisticsCalculator.asDatetimeComparison(
                (BinaryPredicateOperator) compare(type, value)));
    }

    // The comparison of the cast that the comparison of the column with a constant becomes: its operator and the time
    private static void assertAsCast(StringDateFormat format, BinaryType type, String value, BinaryType expectedType,
                                     LocalDateTime expectedTime) {
        String message = type + " '" + value + "'";
        BinaryPredicateOperator cast = asCast(format, type, value);
        Assertions.assertNotNull(cast, message);
        Assertions.assertEquals(new CastOperator(DateType.DATETIME, TEXT), cast.getChild(0), message);
        Assertions.assertEquals(expectedType, cast.getBinaryType(), message);
        Assertions.assertEquals(expectedTime, ((ConstantOperator) cast.getChild(1)).getDatetime(), message);
    }

    // The operators of the comparison of the cast for the four range operators, one time for all of them
    private static void assertRanges(StringDateFormat format, String value, LocalDateTime time, BinaryType ge,
                                     BinaryType gt, BinaryType le, BinaryType lt) {
        assertAsCast(format, BinaryType.GE, value, ge, time);
        assertAsCast(format, BinaryType.GT, value, gt, time);
        assertAsCast(format, BinaryType.LE, value, le, time);
        assertAsCast(format, BinaryType.LT, value, lt, time);
    }

    // A constant of the format compares with its own time, with the operator it has. A constant between the strings of
    // a bucket: the strings above it start at one time, whether the comparison is strict or not, so GE and GT are
    // both a range from it, LE and LT both end before it.
    @Test
    public void testComparisonOfEveryConstantAndOperatorAsTheCast() {
        BinaryType ge = BinaryType.GE;
        BinaryType gt = BinaryType.GT;
        BinaryType le = BinaryType.LE;
        BinaryType lt = BinaryType.LT;
        assertRanges(ISO, "2026-08-15T12:00:00.000000Z", LocalDateTime.of(2026, 8, 15, 12, 0), ge, gt, le, lt);
        assertRanges(ISO, "2026-08-15T12:30:00.000000Z", LocalDateTime.of(2026, 8, 15, 12, 30), ge, gt, le, lt);
        assertRanges(ISO, "2026-08-15T06:30:00.000000Z", LocalDateTime.of(2026, 8, 15, 6, 30), ge, gt, le, lt);
        // The statistics hold whole seconds: the time after the start of a second is a range after the second
        assertRanges(ISO, "2026-08-15T06:30:00.000001Z", LocalDateTime.of(2026, 8, 15, 6, 30), gt, gt, le, le);
        assertRanges(ISO, "2026-08-15 12:00:00", LocalDateTime.of(2026, 8, 15, 0, 0), ge, ge, lt, lt);
        assertRanges(ISO, "2026-08-15T12:00:00Z", LocalDateTime.of(2026, 8, 15, 12, 0, 1), ge, ge, lt, lt);
        assertRanges(ISO, "2026-08-15T12", LocalDateTime.of(2026, 8, 15, 12, 0), ge, ge, lt, lt);
        assertRanges(ISO, "2026-08-15", LocalDateTime.of(2026, 8, 15, 0, 0), ge, ge, lt, lt);
        assertRanges(ISO, "20260815", LocalDateTime.of(2027, 1, 1, 0, 0), ge, ge, lt, lt);
        assertRanges(ISO, "2026-08-10", LocalDateTime.of(2026, 8, 10, 0, 0), ge, ge, lt, lt);
        assertRanges(ISO, "2026-08-20", LocalDateTime.of(2026, 8, 20, 0, 0), ge, ge, lt, lt);
        // Every string is above the empty constant, and none is above 'abc'
        assertRanges(ISO, "", ConstantOperator.MIN_DATETIME, ge, ge, lt, lt);
        assertRanges(ISO, "abc", ConstantOperator.MIN_DATETIME, lt, lt, ge, ge);

        assertAsCast(ISO, BinaryType.EQ, "2026-08-15T12:00:00.000000Z", BinaryType.EQ,
                LocalDateTime.of(2026, 8, 15, 12, 0));
        assertAsCast(ISO, BinaryType.EQ, "2026-08-15T06:30:00.000001Z", BinaryType.EQ,
                LocalDateTime.of(2026, 8, 15, 6, 30, 0, 1000));
        for (String other : List.of("2026-08-15 12:00:00", "2026-08-15T12:00:00Z", "2026-08-15T12", "2026-08-15",
                "20260815", "2026-08-10", "2026-08-20", "", "abc")) {
            Assertions.assertNull(asCast(ISO, BinaryType.EQ, other), other);
        }
    }

    // The BE holds microseconds up to 9999-12-31 23:59:59.999999, past the range the FE checks for a DATETIME constant
    @Test
    public void testConstantsAtTheEndsOfTheRangeOfAFormatWithMicroseconds() {
        LocalDateTime last = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_000);
        LocalDateTime lastSecond = LocalDateTime.of(9999, 12, 31, 23, 59, 59);
        assertAsCast(SPACED, BinaryType.EQ, "9999-12-31 23:59:59.999999", BinaryType.EQ, last);
        assertRanges(SPACED, "9999-12-31 23:59:59.999999", lastSecond, BinaryType.GT, BinaryType.GT, BinaryType.LE,
                BinaryType.LE);
        assertAsCast(SPACED, BinaryType.EQ, "0000-01-01 00:00:00.000000", BinaryType.EQ, ConstantOperator.MIN_DATETIME);
        assertRanges(SPACED, "0000-01-01 00:00:00.000000", ConstantOperator.MIN_DATETIME, BinaryType.GE,
                BinaryType.GT, BinaryType.LE, BinaryType.LT);

        // The strings from 23:59:59.500000 on are above the constant: the last second has rows on its side
        for (BinaryType type : List.of(BinaryType.GE, BinaryType.GT)) {
            assertAsCast(SPACED, type, "9999-12-31 23:59:59.5", BinaryType.GT, lastSecond);
        }
        for (BinaryType type : List.of(BinaryType.LE, BinaryType.LT)) {
            assertAsCast(SPACED, type, "9999-12-31 23:59:59.5", BinaryType.LE, lastSecond);
        }
        Assertions.assertNull(asCast(SPACED, BinaryType.EQ, "9999-12-31 23:59:59.5"));
        // No string is above a constant that continues the last string
        assertRanges(SPACED, "9999-12-31 23:59:59.9999995", ConstantOperator.MIN_DATETIME, BinaryType.LT,
                BinaryType.LT, BinaryType.GE, BinaryType.GE);

        assertAsCast(ISO, BinaryType.GE, "9999-12-31T23:59:59.999999Z", BinaryType.GT, lastSecond);
    }

    private static Statistics spaced() {
        return Statistics.builder().setOutputRowCount(397_770_593).addColumnStatistic(TEXT, ColumnStatistic.builder()
                .setNullsFraction(0).setAverageRowSize(27).setDistinctValuesCount(390_000_000)
                .setStringBounds(new StringBounds("2018-08-15 13:16:23.509905", "2026-10-07 08:58:22.946788", 200,
                        false)).build()).build();
    }

    // The estimate of the ends of the range of a format with microseconds finishes, and keeps between none and all
    @Test
    public void testEstimateAtTheEndsOfTheRangeDoesNotFail() {
        Map<StringDateFormat, Statistics> statistics = Map.of(ISO, mobi(), SPACED, spaced());
        Map<StringDateFormat, List<String>> constants = Map.of(
                ISO, List.of("9999-12-31T23:59:59.999999Z", "0000-01-01T00:00:00.000000Z"),
                SPACED, List.of("9999-12-31 23:59:59.999999", "0000-01-01 00:00:00.000000",
                        "9999-12-31 23:59:59.5"));
        for (StringDateFormat format : List.of(ISO, SPACED)) {
            for (String value : constants.get(format)) {
                for (BinaryType type : List.of(BinaryType.EQ, BinaryType.GE, BinaryType.GT, BinaryType.LE,
                        BinaryType.LT)) {
                    double rows = estimated(compare(type, value), statistics.get(format), format).getOutputRowCount();
                    Assertions.assertTrue(rows >= 0 && rows <= 397_770_593, type + " '" + value + "' " + rows);
                }
                ScalarOperator window = Utils.compoundAnd(compare(BinaryType.GE, value), compare(BinaryType.LE, value));
                Assertions.assertTrue(estimated(window, statistics.get(format), format).getOutputRowCount() >= 0);
            }
        }
        // An equality of the last time is the one of the explicit CAST
        ScalarOperator last = new BinaryPredicateOperator(BinaryType.EQ, new CastOperator(DateType.DATETIME, TEXT),
                ConstantOperator.createDatetime(LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_000),
                        DateType.DATETIME));
        Assertions.assertEquals(estimate(last, spaced()),
                estimated(compare(BinaryType.EQ, "9999-12-31 23:59:59.999999"), spaced(), SPACED).getOutputRowCount(),
                1e-6);
    }

    // A failure of the conversion costs the estimate of the comparison and not the query
    @Test
    public void testFailedConversionKeepsTheEstimateOfStrings() {
        Function<ColumnRefOperator, StringDateFormat> failing = column -> {
            throw new IllegalStateException("no format for " + column);
        };
        ScalarOperator recent = compare(BinaryType.GE, "2026-04-30T19:00:00.000000Z");
        ScalarOperator window = Utils.compoundAnd(compare(BinaryType.GE, "2026-08-14T10:00:00.000000Z"),
                compare(BinaryType.LT, "2026-08-14T12:00:00.000000Z"));
        for (ScalarOperator predicate : List.of(recent, window, compare(BinaryType.EQ, "2026-08-15T06:30:00.000000Z"))) {
            for (Statistics statistics : List.of(mobi(), hourly())) {
                double failed = PredicateStatisticsCalculator.withStringDateFormats(failing,
                        () -> estimate(predicate, statistics));
                Assertions.assertEquals(estimate(predicate, statistics), failed, 1e-9, predicate.toString());
            }
        }
        Assertions.assertNull(PredicateStatisticsCalculator.withStringDateFormats(failing,
                () -> PredicateStatisticsCalculator.asDatetimeComparison(
                        (BinaryPredicateOperator) compare(BinaryType.GE, "2026-04-30T19:00:00.000000Z"))));
    }

    @Test
    public void testComparableStartOfAConstant() {
        // The strings of ISO have 27 code points: 28 of a constant order it
        String start = "2026-08-15T12:00:00.000000Zx";
        Assertions.assertEquals(28, start.length());
        Assertions.assertEquals(start, PredicateStatisticsCalculator.comparableStart(ISO, start));
        Assertions.assertEquals(start, PredicateStatisticsCalculator.comparableStart(ISO, start + "yz".repeat(50_000)));
        Assertions.assertEquals(start.substring(0, 27),
                PredicateStatisticsCalculator.comparableStart(ISO, start.substring(0, 27)));
        Assertions.assertEquals("", PredicateStatisticsCalculator.comparableStart(ISO, ""));
        Assertions.assertEquals("20260815", PredicateStatisticsCalculator.comparableStart(ISO, "20260815"));
        // A pair of surrogates is one code point and is not cut
        String emoji = new String(Character.toChars(0x1F600));
        String cut = PredicateStatisticsCalculator.comparableStart(ISO, emoji.repeat(100));
        Assertions.assertEquals(emoji.repeat(28), cut);
        Assertions.assertEquals(28, cut.codePointCount(0, cut.length()));
        Assertions.assertEquals("2026-08-", PredicateStatisticsCalculator.comparableStart(
                StringDateFormat.parse("%Y-%m"), "2026-08-15T12:00:00"));
    }

    private static final LocalDateTime LAST_TIME = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_000);

    // The first time over the whole constant, by the search of firstTimeAbove over the microseconds
    private static LocalDateTime firstTimeOfTheWholeConstant(StringDateFormat format, String constant, boolean strict) {
        if (!isAboveWhole(format, LAST_TIME, constant, strict)) {
            return null;
        }
        long low = 0;
        long high = ChronoUnit.MICROS.between(ConstantOperator.MIN_DATETIME, LAST_TIME);
        while (low < high) {
            long middle = low + (high - low) / 2;
            if (isAboveWhole(format, ConstantOperator.MIN_DATETIME.plus(middle, ChronoUnit.MICROS), constant, strict)) {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        return ConstantOperator.MIN_DATETIME.plus(low, ChronoUnit.MICROS);
    }

    private static boolean isAboveWhole(StringDateFormat format, LocalDateTime time, String constant, boolean strict) {
        int order = StringBucket.compare(format.format(time), constant);
        return strict ? order > 0 : order >= 0;
    }

    // The first time of a constant cut to its start is the one of the whole constant, for constants that stop before,
    // at, and after the end of a string of the format, with a tail that sorts as the strings or not
    @Test
    public void testFirstTimeDoesNotDependOnTheConstantPastItsStart() {
        Random random = new Random(20260815);
        String characters = "0123456789-T:.Z abc";
        int[] points = (characters + new String(Character.toChars(0x1F600))).codePoints().toArray();
        long span = ChronoUnit.MICROS.between(ConstantOperator.MIN_DATETIME, LAST_TIME);
        List<StringDateFormat> formats = List.of(ISO, SPACED, StringDateFormat.parse("%Y-%m-%d"),
                StringDateFormat.parse("%Y/%m/%d %H:%i"), StringDateFormat.parse("%Y%m%d%H%i%s"));
        for (StringDateFormat format : formats) {
            for (int round = 0; round < 150; round++) {
                LocalDateTime time = ConstantOperator.MIN_DATETIME.plus(Math.floorMod(random.nextLong(), span + 1),
                        ChronoUnit.MICROS);
                StringBuilder constant = new StringBuilder(format.format(time));
                switch (random.nextInt(4)) {
                    case 0 -> constant.setLength(random.nextInt(constant.length() + 1));
                    case 1 -> constant.setCharAt(random.nextInt(constant.length()),
                            characters.charAt(random.nextInt(characters.length())));
                    default -> {
                    }
                }
                int tail = random.nextInt(3) == 0 ? 0 : 1 + random.nextInt(60);
                for (int i = 0; i < tail; i++) {
                    constant.appendCodePoint(points[random.nextInt(points.length)]);
                }
                for (boolean strict : List.of(false, true)) {
                    Assertions.assertEquals(firstTimeOfTheWholeConstant(format, constant.toString(), strict),
                            PredicateStatisticsCalculator.firstTimeAbove(format, constant.toString(), strict),
                            format + " '" + constant + "' " + strict);
                }
            }
        }
    }

    // The strings of hourly(): every hour of 2026-08-11 to 2026-08-19 100 times, and the hot value 5000 times
    private static List<String> hourlyStrings() {
        List<String> strings = new ArrayList<>();
        for (int day = 11; day <= 19; day++) {
            for (int hour = 0; hour < 24; hour++) {
                for (int i = 0; i < 100; i++) {
                    strings.add(String.format("2026-08-%02dT%02d:00:00.000000Z", day, hour));
                }
            }
        }
        for (int i = 0; i < 5000; i++) {
            strings.add("2026-08-15T06:30:00.000000Z");
        }
        return strings;
    }

    private static boolean holds(BinaryType type, int order) {
        return switch (type) {
            case GE -> order >= 0;
            case GT -> order > 0;
            case LE -> order <= 0;
            default -> order < 0;
        };
    }

    // The estimate of the comparison of strings with any constant matches the count of the strings it keeps, as the
    // cast estimates it: the histogram spreads the rows of a day evenly over the bucket, so a cut is off by at most
    // the rows of one hour, the rows of a bucket over its distinct values.
    @Test
    public void testEstimateCountsTheStringsTheComparisonKeeps() {
        List<String> strings = hourlyStrings();
        for (String constant : List.of("2026-08-15T12:00:00.000000Z", "2026-08-15T12:30:00.000000Z",
                "2026-08-15 12:00:00", "2026-08-15T12:00:00Z", "2026-08-15T12", "2026-08-15", "20260815",
                "2026-08-15T06:30:00.000000Z", "2026-08-15T06:30:00.000001Z", "2026-08-10", "2026-08-20", "abc", "")) {
            for (BinaryType type : List.of(BinaryType.GE, BinaryType.GT, BinaryType.LE, BinaryType.LT)) {
                long exact = strings.stream().filter(s -> holds(type, StringBucket.compare(s, constant))).count();
                double estimate = withFormat(compare(type, constant), hourly());
                Assertions.assertEquals(exact, estimate, 100, type + " '" + constant + "'");
            }
        }
        // A ray that holds no string keeps the least rows the estimate gives, and one that holds all keeps them all
        Assertions.assertTrue(withFormat(compare(BinaryType.GE, "20260815"), hourly()) <= 1);
        Assertions.assertEquals(strings.size(), withFormat(compare(BinaryType.LT, "20260815"), hourly()), 1);
    }
}
