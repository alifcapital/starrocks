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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Supplier;

/**
 * A prefix p of the strings of a column with the format of its dates is the interval of the strings that start with it:
 * col LIKE '2026-08-15%' and the first ten characters of the column equal to '2026-08-15' keep the strings from p up to
 * the first string after all those with p, col >= p AND col < successor(p), estimated by the path of the comparisons of
 * the column. For a prefix that ends at a field of the format it is the period: the interval of the day, the one that
 * col >= '2026-08-15T00:00:00.000000Z' AND col < '2026-08-16T00:00:00.000000Z' makes. A prefix that ends inside a field
 * is the interval of the times whose strings start with it.
 */
public class StringDatePrefixEstimateTest {
    private static final ColumnRefOperator TEXT = new ColumnRefOperator(1, VarcharType.VARCHAR, "created_at", true);
    private static final StringDateFormat ISO = StringDateFormat.parse("%Y-%m-%dT%H:%i:%s.%fZ");
    private static final double ROWS = 397_770_593;

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

    // The bounds of the column on the stand, from ANALYZE.
    private static Statistics mobi() {
        return Statistics.builder().setOutputRowCount(ROWS).addColumnStatistic(TEXT, ColumnStatistic.builder()
                .setNullsFraction(0).setAverageRowSize(27).setDistinctValuesCount(390_000_000)
                .setStringBounds(new StringBounds("2018-08-15T13:16:23.509905Z", "2026-10-07T08:58:22.946788Z",
                        200, false)).build()).build();
    }

    // A string of every hour of 2026-08-11 to 2026-08-19, 100 rows each, in one bucket a day, and a hot value.
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

    private static <T> T inScope(StringDateFormat format, Supplier<T> action) {
        return PredicateStatisticsCalculator.withStringDateFormats(column -> TEXT.equals(column) ? format : null, action);
    }

    private static double rows(ScalarOperator predicate, Statistics statistics, StringDateFormat format) {
        return inScope(format, () -> PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics)
                .getOutputRowCount());
    }

    private static double rows(ScalarOperator predicate, Statistics statistics) {
        return rows(predicate, statistics, ISO);
    }

    private static ConstantOperator text(String value) {
        return ConstantOperator.createVarchar(value);
    }

    private static ScalarOperator like(String pattern) {
        return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, TEXT, text(pattern));
    }

    private static ScalarOperator call(String name, ScalarOperator... arguments) {
        Type[] types = new Type[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            types[i] = arguments[i].getType();
        }
        Function function = ExprUtils.getBuiltinFunction(name, types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(name, VarcharType.VARCHAR, List.of(arguments), function);
    }

    private static ScalarOperator equalsPrefix(String name, String prefix) {
        int length = prefix.length();
        ScalarOperator call = name.startsWith("sub")
                ? call(name, TEXT, ConstantOperator.createInt(1), ConstantOperator.createInt(length))
                : call(name, TEXT, ConstantOperator.createInt(length));
        return new BinaryPredicateOperator(BinaryType.EQ, call, text(prefix));
    }

    private static ScalarOperator interval(String from, String to) {
        return Utils.compoundAnd(new BinaryPredicateOperator(BinaryType.GE, TEXT, text(from)),
                new BinaryPredicateOperator(BinaryType.LT, TEXT, text(to)));
    }

    private static List<ScalarOperator> formsOf(String prefix) {
        return List.of(like(prefix + "%"), equalsPrefix("left", prefix), equalsPrefix("strleft", prefix),
                equalsPrefix("substr", prefix), equalsPrefix("substring", prefix));
    }

    private static final ColumnRefOperator X = new ColumnRefOperator(-1, VarcharType.VARCHAR, "expr#1", true);

    // The statistic on x = substr(created_at, 1, k) with a head that holds every row of the table.
    private static ScanExpressionStatistics prefixStatistics(int k, Map<String, Long> head) {
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        head.forEach((value, count) -> entries.add(new MultiColumnCombinedStats.McvEntry(List.of(value), count,
                List.of(count))));
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("x"), (long) ROWS, head.size(),
                entries, List.of(), List.of(0L));
        group.prepare(VarcharType.VARCHAR);
        ColumnStatistic statistic = group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(
                call("left", TEXT, ConstantOperator.createInt(k)));
        return new ScanExpressionStatistics("t", List.of(new ScanExpressionStatistics.Entry(canonical,
                ExpressionStatisticsCanonicalizer.keyOf(canonical), X, statistic)),
                Map.of(Set.of(X), new MultiColumnCombinedStats(head.size(), (long) ROWS, List.of(X), entries,
                        List.of(0L))));
    }

    private static double rowsWithX(ScalarOperator predicate, ScanExpressionStatistics expressions) {
        return inScope(ISO, () -> PredicateStatisticsCalculator.statisticsCalculate(predicate, mobi(), true, expressions)
                .getOutputRowCount());
    }

    @Test
    public void testPrefixOfDatesIsReadByTheStatisticOnXAndByTheInterval() {
        double rest = ROWS - 8_000_000;
        // K = 10 and K = 12 hold the day: the LIKE and the equality of the prefix are the rows of the head that start
        // with it, 5,000,000.
        ScanExpressionStatistics ten = prefixStatistics(10, new LinkedHashMap<>(Map.of("2026-08-14", 3_000_000L,
                "2026-08-15", 5_000_000L, "2026-09-20", (long) rest)));
        ScanExpressionStatistics twelve = prefixStatistics(12, new LinkedHashMap<>(Map.of("2026-08-14T10", 3_000_000L,
                "2026-08-15T06", 2_000_000L, "2026-08-15T18", 3_000_000L, "2026-09-20T09", (long) rest)));
        for (ScanExpressionStatistics expressions : List.of(ten, twelve)) {
            for (ScalarOperator form : formsOf("2026-08-15")) {
                Assertions.assertEquals(5_000_000, rowsWithX(form, expressions), 1e-6, form.toString());
            }
        }
        // K = 8 holds the month: the prefix of the day is longer than x, which cannot tell the day from the month, so the
        // LIKE is estimated by the interval of the times, as without x, and not as the month of 8,000,000 rows.
        ScanExpressionStatistics eight = prefixStatistics(8, new LinkedHashMap<>(Map.of("2026-08-", 8_000_000L,
                "2026-09-", (long) rest)));
        double day = rows(interval("2026-08-15T00:00:00.000000Z", "2026-08-16T00:00:00.000000Z"), mobi());
        Assertions.assertNotEquals(8_000_000, day, 1.0);
        for (ScalarOperator form : formsOf("2026-08-15")) {
            Assertions.assertEquals(day, rowsWithX(form, eight), 1e-6, form.toString());
            Assertions.assertEquals(day, rows(form, mobi()), 1e-6, form.toString());
        }
        // The month, which x holds, is the head that starts with it.
        for (ScalarOperator form : formsOf("2026-08")) {
            Assertions.assertEquals(8_000_000, rowsWithX(form, eight), 1e-6, form.toString());
        }
    }

    @Test
    public void testPatternOfAPrefixAndMoreIsNotAboveTheInterval() {
        // 2026-08-1_% is the strings of 2026-08-1 with one character more, and the default of a pattern is a quarter of
        // the rows: the estimate is the rows of 2026-08-1%.
        double prefix = rows(like("2026-08-1%"), mobi());
        Assertions.assertEquals(rows(interval("2026-08-1", "2026-08-2"), mobi()), prefix, 1e-6);
        Assertions.assertTrue(prefix < ROWS / 100);
        for (String pattern : List.of("2026-08-1_%", "2026-08-1%5", "2026-08-1_5%", "2026-08-1_T%", "2026-08-1")) {
            Assertions.assertEquals(prefix, rows(like(pattern), mobi()), 1e-6, pattern);
        }
        // With a histogram of the days the pattern takes the rows of the head that match and a quarter of the tail,
        // which is below the interval, and is kept.
        Assertions.assertEquals(26_600, rows(like("2026-08-1%"), hourly()), 1e-6);
        Assertions.assertEquals(5000 + 21_600 / 4.0, rows(like("2026-08-1_%"), hourly()), 1e-6);
        // A pattern that starts with a wildcard has no literal, and a prefix is estimated by itself.
        Assertions.assertEquals(ROWS / 4, rows(like("%2026-08-1%"), mobi()), 1e-6);
        // The same on a column without the format of dates.
        Assertions.assertEquals(ROWS / 4, rows(like("2026-08-1_%"), mobi(), null), 1e-6);
    }

    @Test
    public void testOrOfIntervalsWithNoCommonPointIsTheSumOfTheirRows() {
        ScalarOperator january = interval("2026-01", "2026-02");
        ScalarOperator march = interval("2026-03", "2026-04");
        double sum = rows(january, mobi()) + rows(march, mobi());
        Assertions.assertEquals(sum, rows(Utils.compoundOr(january, march), mobi()), 1e-6);
        Assertions.assertEquals(sum, rows(Utils.compoundOr(march, january), mobi()), 1e-6);
        // The months of the LIKE and of the prefix of the column are the same intervals.
        Assertions.assertEquals(sum, rows(Utils.compoundOr(like("2026-01%"), like("2026-03%")), mobi()), 1e-6);
        Assertions.assertEquals(sum, rows(Utils.compoundOr(equalsPrefix("left", "2026-01"),
                equalsPrefix("substr", "2026-03")), mobi()), 1e-6);
        // Touching intervals have no common point either: [2026-01, 2026-02) and [2026-02, 2026-03).
        double touching = rows(january, mobi()) + rows(interval("2026-02", "2026-03"), mobi());
        Assertions.assertEquals(touching, rows(Utils.compoundOr(january, interval("2026-02", "2026-03")), mobi()), 1e-6);
        // Intervals with a common month have the estimate of an OR: the rows of the first, and of the second less its
        // rows among those of the first.
        ScalarOperator first = interval("2026-01", "2026-03");
        ScalarOperator second = interval("2026-02", "2026-04");
        Statistics afterFirst = inScope(ISO, () -> PredicateStatisticsCalculator.statisticsCalculate(first, mobi()));
        double inBoth = rows(second, afterFirst);
        Assertions.assertEquals(rows(first, mobi()) + rows(second, mobi()) - inBoth,
                rows(Utils.compoundOr(first, second), mobi()), 1e-6);
        Assertions.assertTrue(inBoth > 0);
    }

    @Test
    public void testDayIsTheIntervalOfTheDay() {
        ScalarOperator day = interval("2026-08-15T00:00:00.000000Z", "2026-08-16T00:00:00.000000Z");
        for (Statistics statistics : List.of(mobi(), hourly())) {
            double expected = rows(day, statistics);
            for (ScalarOperator form : formsOf("2026-08-15")) {
                Assertions.assertEquals(expected, rows(form, statistics), 1e-6, form.toString());
            }
            // The text after the day holds the same strings.
            Assertions.assertEquals(expected, rows(like("2026-08-15T%"), statistics), 1e-6);
        }
        // A day of the 3000 days of the history of the column holds about 133K of the 400M rows.
        Assertions.assertEquals(133_000, rows(day, mobi()), 12_000);
        // The day of the hourly strings: the bucket of the day and its hot value are counted.
        Assertions.assertEquals(2400 + 5000, rows(day, hourly()), 200);
        // Without the format the forms keep the estimates of strings, which are not the interval.
        for (ScalarOperator form : formsOf("2026-08-15")) {
            Assertions.assertNotEquals(rows(day, mobi()), rows(form, mobi(), null), 1.0, form.toString());
        }
    }

    @Test
    public void testYearMonthAndHour() {
        for (Statistics statistics : List.of(mobi(), hourly())) {
            Map<String, ScalarOperator> periods = Map.of(
                    "2026", interval("2026-01-01T00:00:00.000000Z", "2027-01-01T00:00:00.000000Z"),
                    "2026-08", interval("2026-08-01T00:00:00.000000Z", "2026-09-01T00:00:00.000000Z"),
                    "2026-12", interval("2026-12-01T00:00:00.000000Z", "2027-01-01T00:00:00.000000Z"),
                    "2026-08-15T10", interval("2026-08-15T10:00:00.000000Z", "2026-08-15T11:00:00.000000Z"),
                    "2026-08-15T10:30", interval("2026-08-15T10:30:00.000000Z", "2026-08-15T10:31:00.000000Z"));
            periods.forEach((prefix, interval) -> {
                double expected = rows(interval, statistics);
                for (ScalarOperator form : formsOf(prefix)) {
                    Assertions.assertEquals(expected, rows(form, statistics), 1e-6, form.toString());
                }
            });
        }
    }

    @Test
    public void testLastYearHasNoUpperBound() {
        ScalarOperator since = new BinaryPredicateOperator(BinaryType.GE, TEXT, text("9999-01-01T00:00:00.000000Z"));
        for (ScalarOperator form : formsOf("9999")) {
            Assertions.assertEquals(rows(since, mobi()), rows(form, mobi()), 1e-6, form.toString());
        }
    }

    @Test
    public void testNegationIsTheRestOfTheRows() {
        ScalarOperator day = interval("2026-08-15T00:00:00.000000Z", "2026-08-16T00:00:00.000000Z");
        ScalarOperator notLike = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                like("2026-08-15%"));
        Assertions.assertEquals(ROWS - rows(day, mobi()), rows(notLike, mobi()), 1.0);
    }

    @Test
    public void testPrefixInsideAFieldIsTheIntervalOfItsStrings() {
        // The first ten characters of the days are 2026-08-1 and the strings with it are those of the days 10 to 19.
        Map<String, ScalarOperator> intervals = Map.of(
                "2026-08-1", interval("2026-08-10T00:00:00.000000Z", "2026-08-20T00:00:00.000000Z"),
                "2026-0", interval("2026-01-01T00:00:00.000000Z", "2026-10-01T00:00:00.000000Z"),
                "2", interval("2000-01-01T00:00:00.000000Z", "3000-01-01T00:00:00.000000Z"),
                "2026-08-15T1", interval("2026-08-15T10:00:00.000000Z", "2026-08-15T20:00:00.000000Z"),
                "2026-08-15T10:3", interval("2026-08-15T10:30:00.000000Z", "2026-08-15T10:40:00.000000Z"),
                "2026-08-15T10:30:00.000000", interval("2026-08-15T10:30:00.000000Z", "2026-08-15T10:30:00.000001Z"));
        for (Statistics statistics : List.of(mobi(), hourly())) {
            intervals.forEach((prefix, interval) -> {
                double expected = rows(interval, statistics);
                for (ScalarOperator form : formsOf(prefix)) {
                    Assertions.assertEquals(expected, rows(form, statistics), 1e-6, form.toString());
                }
            });
        }
        // The ten days of 2026-08-1 are between the day and the month, whatever the way the bounds are written.
        double day = rows(like("2026-08-15%"), mobi());
        double tenDays = rows(like("2026-08-1%"), mobi());
        double month = rows(like("2026-08%"), mobi());
        Assertions.assertTrue(day < tenDays && tenDays < month, day + " " + tenDays + " " + month);
        // The same for the months of the year, and the year of 2026-0 less its months 10 to 12.
        Assertions.assertTrue(month < rows(like("2026-0%"), mobi()));
        Assertions.assertTrue(rows(like("2026-0%"), mobi()) < rows(like("2026%"), mobi()));
        Assertions.assertTrue(rows(like("2026%"), mobi()) < rows(like("2%"), mobi()) + 1);
    }

    @Test
    public void testPrefixThatNoStringHasHoldsNoRow() {
        for (String prefix : List.of("abc", "x", "2026-02-30", "2026-08-15 ", "2026-13", "2026-08-15T25")) {
            for (Statistics statistics : List.of(mobi(), hourly())) {
                for (ScalarOperator form : formsOf(prefix)) {
                    // A few rows at most, where the rows of one day are 100,000, and not the default of a pattern.
                    Assertions.assertTrue(rows(form, statistics) < 10, form + ": " + rows(form, statistics));
                }
            }
        }
    }

    @Test
    public void testEmptyPrefixHoldsEveryString() {
        // LIKE '%' has no upper bound and starts below the first time.
        Assertions.assertEquals(ROWS, rows(like("%"), mobi()), 1.0);
        Assertions.assertEquals(rows(new BinaryPredicateOperator(BinaryType.GE, TEXT, text("")), mobi()),
                rows(like("%"), mobi()), 1e-6);
    }

    @Test
    public void testNotLikeOfAPrefixInsideAFieldIsTheRestOfTheRows() {
        ScalarOperator notLike = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                like("2026-08-1%"));
        Assertions.assertEquals(ROWS - rows(like("2026-08-1%"), mobi()), rows(notLike, mobi()), 1.0);
    }

    @Test
    public void testPrefixCallOfAnotherLengthIsEstimatedAsBefore() {
        for (String name : List.of("left", "strleft", "substr", "substring")) {
            ScalarOperator shorter = equalsPrefix(name, "2026-08-15");
            // The first 12 characters equal to a string of 10: no string of the format.
            ScalarOperator longer = new BinaryPredicateOperator(BinaryType.EQ,
                    name.startsWith("sub") ? call(name, TEXT, ConstantOperator.createInt(1), ConstantOperator.createInt(12))
                            : call(name, TEXT, ConstantOperator.createInt(12)), text("2026-08-15"));
            Assertions.assertEquals(rows(longer, mobi(), null), rows(longer, mobi()), 1e-9, name);
            Assertions.assertNotEquals(rows(shorter, mobi(), null), rows(shorter, mobi()), 1.0, name);
        }
        // From the second character the prefix is not a prefix of the strings.
        ScalarOperator tail = new BinaryPredicateOperator(BinaryType.EQ, call("substr", TEXT,
                ConstantOperator.createInt(2), ConstantOperator.createInt(10)), text("2026-08-15"));
        Assertions.assertEquals(rows(tail, mobi(), null), rows(tail, mobi()), 1e-9);
    }

    @Test
    public void testPrefixIsEstimatedAsBeforeWithoutTheFormat() {
        for (String prefix : List.of("2026-08-1", "2026-0", "2", "abc")) {
            for (ScalarOperator form : formsOf(prefix)) {
                double without = rows(form, mobi(), null);
                // A format without a day casts every string to NULL, so the column keeps the estimate of strings.
                Assertions.assertEquals(without, rows(form, mobi(), StringDateFormat.parse("%Y")), 1e-9,
                        form.toString());
            }
        }
    }
}
