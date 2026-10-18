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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

/**
 * A closed range of days over the cast of a column of strings to DATE, as CAST(created_at AS DATE) >= a AND
 * CAST(created_at AS DATE) <= b. The column has a histogram of strings of the format 'yyyy-MM-dd HH:mm:ss' with
 * 864 rows in each of the 92 days from 2026-07-01, one every 100 seconds, in buckets of 3000 rows whose ends fall
 * inside the days. The rows of a range of days are then 864 for each day of it.
 */
public class CastDateIntervalEstimateTest {
    private static final ColumnRefOperator TEXT = DailyStrings.TEXT;
    private static final LocalDate FIRST_DAY = DailyStrings.FIRST_DAY;
    private static final int DAYS = DailyStrings.DAYS;
    private static final int ROWS_OF_DAY = DailyStrings.ROWS_OF_DAY;

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

    private static Statistics created() {
        return DailyStrings.statistics();
    }

    private static ScalarOperator day() {
        return new CastOperator(DateType.DATE, TEXT);
    }

    private static ScalarOperator onDay(BinaryType type, LocalDate value) {
        return new BinaryPredicateOperator(type, day(), ConstantOperator.createDate(value.atStartOfDay()));
    }

    private static ScalarOperator onText(BinaryType type, LocalDate value) {
        return new BinaryPredicateOperator(type, TEXT,
                ConstantOperator.createVarchar(value.atStartOfDay().format(DailyStrings.TEXT_OF_TIME)));
    }

    private static ScalarOperator closed(LocalDate from, LocalDate to) {
        return Utils.compoundAnd(onDay(BinaryType.GE, from), onDay(BinaryType.LE, to));
    }

    private static ScalarOperator halfOpen(LocalDate from, LocalDate to) {
        return Utils.compoundAnd(onDay(BinaryType.GE, from), onDay(BinaryType.LT, to.plusDays(1)));
    }

    private static double rows(ScalarOperator predicate) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, created()).getOutputRowCount();
    }

    private static double rowsOfText(ScalarOperator predicate) {
        return PredicateStatisticsCalculator.withStringDateFormats(column -> TEXT.equals(column) ? DailyStrings.FORMAT : null,
                () -> rows(predicate));
    }

    // The days the data holds, and the days of the table that a pair of ends is inside of
    private static List<LocalDate> days() {
        List<LocalDate> days = new ArrayList<>(DAYS);
        for (int i = 0; i < DAYS; i++) {
            days.add(FIRST_DAY.plusDays(i));
        }
        return days;
    }

    @Test
    public void testOneDayInsideABucketHoldsItsRows() {
        // 2026-08-17 is inside a bucket, 2026-08-18 is where one ends; the day of the first rows and of the last rows
        for (LocalDate value : List.of(LocalDate.of(2026, 8, 17), LocalDate.of(2026, 8, 18), FIRST_DAY,
                FIRST_DAY.plusDays(DAYS - 1))) {
            Assertions.assertEquals(ROWS_OF_DAY, rows(closed(value, value)), 2, value.toString());
            Assertions.assertEquals(ROWS_OF_DAY, rows(onDay(BinaryType.EQ, value)), 2, value.toString());
            Assertions.assertEquals(ROWS_OF_DAY, rows(halfOpen(value, value)), 2, value.toString());
            Assertions.assertEquals(ROWS_OF_DAY, rowsOfText(Utils.compoundAnd(onText(BinaryType.GE, value),
                    onText(BinaryType.LT, value.plusDays(1)))), 2, value.toString());
        }
    }

    @Test
    public void testEveryDayHoldsItsRows() {
        for (LocalDate value : days()) {
            Assertions.assertEquals(ROWS_OF_DAY, rows(closed(value, value)), 2, value.toString());
        }
    }

    @Test
    public void testClosedRangeIsTheHalfOpenRangeToTheNextDay() {
        List<LocalDate> days = days();
        for (int from = 0; from < DAYS; from += 3) {
            for (int to = from + 1; to < DAYS; to++) {
                String message = days.get(from) + ".." + days.get(to);
                Assertions.assertEquals(rows(halfOpen(days.get(from), days.get(to))),
                        rows(closed(days.get(from), days.get(to))), 1e-9, message);
                Assertions.assertEquals((long) ROWS_OF_DAY * (to - from + 1), rows(closed(days.get(from), days.get(to))),
                        4, message);
            }
        }
    }

    @Test
    public void testOpenLowerEndStartsAtTheNextDay() {
        List<LocalDate> days = days();
        for (int from = 0; from + 2 < DAYS; from += 5) {
            for (int to = from + 2; to < DAYS; to += 2) {
                ScalarOperator open = Utils.compoundAnd(onDay(BinaryType.GT, days.get(from)),
                        onDay(BinaryType.LE, days.get(to)));
                Assertions.assertEquals(rows(closed(days.get(from + 1), days.get(to))), rows(open), 1e-9,
                        days.get(from) + ".." + days.get(to));
            }
        }
    }

    @Test
    public void testRowsGrowWithTheUpperEnd() {
        List<LocalDate> days = days();
        for (int from : new int[] {0, 3, 17, 47, 70}) {
            double previous = 0;
            for (int to = from; to < DAYS; to++) {
                double current = rows(closed(days.get(from), days.get(to)));
                Assertions.assertTrue(current >= previous, days.get(from) + ".." + days.get(to) + " " + current
                        + " after " + previous);
                previous = current;
            }
        }
    }

    @Test
    public void testRangeBeyondTheEndsOfTheData() {
        LocalDate before = FIRST_DAY.minusDays(10);
        LocalDate after = FIRST_DAY.plusDays(DAYS + 10);
        Assertions.assertEquals((long) ROWS_OF_DAY * 3, rows(closed(before, FIRST_DAY.plusDays(2))), 4);
        Assertions.assertEquals((long) ROWS_OF_DAY * 3, rows(closed(FIRST_DAY.plusDays(DAYS - 3), after)), 4);
        Assertions.assertEquals((long) ROWS_OF_DAY * DAYS, rows(closed(before, after)), 4);
        Assertions.assertTrue(rows(closed(after, after.plusDays(5))) <= 1);
    }

    @Test
    public void testClosedRangeOfTheCastIsTheRangeOfTheStringsOfTheDays() {
        List<LocalDate> days = days();
        for (int from : new int[] {2, 13, 40}) {
            for (int to = from; to < DAYS; to += 7) {
                String message = days.get(from) + ".." + days.get(to);
                double strings = rowsOfText(Utils.compoundAnd(onText(BinaryType.GE, days.get(from)),
                        onText(BinaryType.LT, days.get(to).plusDays(1))));
                Assertions.assertEquals(strings, rows(closed(days.get(from), days.get(to))), 4, message);
            }
        }
    }
}
