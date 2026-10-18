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
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * A most common second of a column of strings of dates is a most common value of the day of it when the column is cast
 * to DATE, and the buckets still hold the other rows of that day. The column is that of DailyStrings: 864 rows in each
 * day from 2026-07-01, and a most common second of 300 rows that is not among them. The rows of its day are then
 * 864 + 300, in every form of the day.
 */
public class CastImageMostCommonValuesTest {
    private static final long HOT_ROWS = 300;
    private static final double ROWS_OF_HOT_DAY = DailyStrings.ROWS_OF_DAY + HOT_ROWS;
    // A second inside a bucket and a day, the upper end of the first bucket, the lower end of the second one in the
    // day that the two share, the first second and the last second of the column
    private static final long LAST_ROW = (long) DailyStrings.ROWS_OF_DAY * DailyStrings.DAYS - 1;
    private static final List<String> HOT_SECONDS = List.of("2026-07-17 05:00:00", DailyStrings.at(2999),
            DailyStrings.at(3000), DailyStrings.at(0), DailyStrings.at(LAST_ROW));

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

    private static ScalarOperator onDay(BinaryType type, LocalDate day) {
        return new BinaryPredicateOperator(type, new CastOperator(DateType.DATE, DailyStrings.TEXT),
                ConstantOperator.createDate(day.atStartOfDay()));
    }

    private static ScalarOperator text(BinaryType type, String value) {
        return new BinaryPredicateOperator(type, DailyStrings.TEXT, ConstantOperator.createVarchar(value));
    }

    private static double rows(ScalarOperator predicate, Statistics statistics) {
        return PredicateStatisticsCalculator.withStringDateFormats(
                column -> DailyStrings.TEXT.equals(column) ? DailyStrings.FORMAT : null,
                () -> PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics).getOutputRowCount());
    }

    // The day written as the equality of the cast, as the closed range and as the half open range of it, and as the
    // range of the strings of the column
    private static List<Double> forms(LocalDate day, Statistics statistics) {
        String from = day.atStartOfDay().format(DailyStrings.TEXT_OF_TIME);
        String to = day.plusDays(1).atStartOfDay().format(DailyStrings.TEXT_OF_TIME);
        return List.of(rows(onDay(BinaryType.EQ, day), statistics),
                rows(Utils.compoundAnd(onDay(BinaryType.GE, day), onDay(BinaryType.LE, day)), statistics),
                rows(Utils.compoundAnd(onDay(BinaryType.GE, day), onDay(BinaryType.LT, day.plusDays(1))), statistics),
                rows(Utils.compoundAnd(text(BinaryType.GE, from), text(BinaryType.LT, to)), statistics));
    }

    @Test
    public void testDayOfAMostCommonSecondHoldsAllItsRowsInEveryForm() {
        for (String second : HOT_SECONDS) {
            Statistics statistics = DailyStrings.statistics(Map.of(second, HOT_ROWS));
            LocalDate day = LocalDate.parse(second.substring(0, 10));
            for (double rows : forms(day, statistics)) {
                Assertions.assertEquals(ROWS_OF_HOT_DAY, rows, 2, second);
            }
        }
    }

    @Test
    public void testOtherDaysKeepTheirRows() {
        Statistics plain = DailyStrings.statistics();
        for (String second : HOT_SECONDS) {
            Statistics statistics = DailyStrings.statistics(Map.of(second, HOT_ROWS));
            LocalDate hot = LocalDate.parse(second.substring(0, 10));
            for (int shift : new int[] {-3, -2, -1, 1, 2, 3}) {
                LocalDate day = hot.plusDays(shift);
                if (day.isBefore(DailyStrings.FIRST_DAY) || !day.isBefore(DailyStrings.FIRST_DAY.plusDays(DailyStrings.DAYS))) {
                    continue;
                }
                Assertions.assertEquals(rows(onDay(BinaryType.EQ, day), plain), rows(onDay(BinaryType.EQ, day), statistics),
                        1, second + " " + day);
            }
        }
    }

    @Test
    public void testImageHoldsTheRowsOfTheHistogramAndTheDayHoldsTheRowsOfTheDay() {
        for (String second : HOT_SECONDS) {
            Histogram source = DailyStrings.statistics(Map.of(second, HOT_ROWS)).getColumnStatistic(DailyStrings.TEXT)
                    .getHistogram();
            Histogram image = CastStatisticsDerivation.image(source, VarcharType.VARCHAR, DateType.DATE).orElseThrow()
                    .histogram();
            Assertions.assertEquals(source.getTotalRows(), image.getTotalRows(), second);
            Assertions.assertEquals(Set.of(second.substring(0, 10)), image.getMCV().keySet(), second);
            Assertions.assertEquals(ROWS_OF_HOT_DAY, image.getMCV().get(second.substring(0, 10)), 2, second);
            long previous = 0;
            for (Bucket bucket : image.getBuckets()) {
                Assertions.assertTrue(bucket.getCount() >= previous, second);
                Assertions.assertTrue(bucket.getCount() - previous >= bucket.getUpperRepeats(), second);
                previous = bucket.getCount();
            }
        }
    }

    @Test
    public void testRowsOfABucketRunOutInTheOrderOfTheValues() {
        // 3 rows other than those of the upper end spread over 2 days: each of the two days below the upper end holds
        // 1.5 rows, which round to 2, so the second one gets the 1 row that is left
        LocalDateTime first = LocalDate.of(2026, 7, 1).atStartOfDay();
        Histogram source = new Histogram(List.of(new Bucket(Utils.getLongFromDateTime(first),
                Utils.getLongFromDateTime(first.plusDays(2)), 4L, 1L)),
                Map.of("2026-07-01 00:00:00", 10L, "2026-07-02 00:00:00", 20L));
        Histogram image = CastStatisticsDerivation.image(source, DateType.DATETIME, DateType.DATE).orElseThrow()
                .histogram();
        Assertions.assertEquals(Map.of("2026-07-01", 12L, "2026-07-02", 21L), image.getMCV());
        Assertions.assertEquals(source.getTotalRows(), image.getTotalRows());
    }

    @Test
    public void testTargetThatKeepsItsValuesKeepsTheListAndTheBuckets() {
        // A DATETIME has the second of the value, so the second is a most common value of its own, with its own rows
        String second = "2026-07-17 05:00:00";
        Histogram source = DailyStrings.statistics(Map.of(second, HOT_ROWS)).getColumnStatistic(DailyStrings.TEXT)
                .getHistogram();
        Histogram image = CastStatisticsDerivation.image(source, VarcharType.VARCHAR, DateType.DATETIME).orElseThrow()
                .histogram();
        Assertions.assertEquals(Map.of(second, HOT_ROWS), image.getMCV());
        Assertions.assertEquals(source.getTotalRows(), image.getTotalRows());
        for (int i = 0; i < source.getBuckets().size(); i++) {
            Assertions.assertEquals(source.getBuckets().get(i).getCount(), image.getBuckets().get(i).getCount());
        }
    }
}
