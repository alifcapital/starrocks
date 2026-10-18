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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.util.DatePeriod;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * The periods of the registry agree with the three places that cut a date: the fold of the FE that the statistics read
 * for date_trunc, the helpers of the partitions that the inverses read, and the fold of last_day, next_day and
 * previous_day. A period that differs from the fold by a day counts a week or a quarter too many or too few, and a week
 * that starts on a Monday is not the week of next_day.
 */
public class DatePeriodAgreementTest {
    private static final ColumnRefOperator DATE = new ColumnRefOperator(1, DateType.DATETIME, "d", true);
    private static final String[] UNITS = {"second", "minute", "hour", "day", "week", "month", "quarter", "year"};
    private static final String[] DAYS = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};

    private static CallOperator call(String name, ScalarOperator... arguments) {
        return new CallOperator(name, DateType.DATETIME, ImmutableList.copyOf(arguments));
    }

    private static DatePeriod period(CallOperator call) {
        return MonotonicFunctionRegistry.granularity(call).orElseThrow(() -> new AssertionError(call.toString()));
    }

    // The days around the changes of a week, a month, a quarter, a year and a leap day, and the ends of the type
    private static final LocalDateTime[] SAMPLES = {
            LocalDateTime.of(2018, 11, 20, 13, 14, 15, 123_000_000), LocalDateTime.of(2026, 3, 5, 1, 2, 3),
            LocalDateTime.of(2024, 2, 29, 23, 59, 59, 999_000_000), LocalDateTime.of(2024, 3, 1, 0, 0),
            LocalDateTime.of(2023, 12, 31, 23, 59, 59), LocalDateTime.of(2024, 1, 1, 0, 0),
            LocalDateTime.of(2024, 3, 31, 23, 59, 59), LocalDateTime.of(2024, 4, 1, 0, 0),
            LocalDateTime.of(2024, 6, 30, 23, 59, 59), LocalDateTime.of(2024, 7, 1, 0, 0),
            LocalDateTime.of(2024, 9, 30, 23, 59, 59), LocalDateTime.of(2024, 10, 1, 0, 0),
            LocalDateTime.of(2026, 3, 1, 23, 59, 59), LocalDateTime.of(2026, 3, 2, 0, 0),
            LocalDateTime.of(2026, 3, 8, 12, 0), LocalDateTime.of(2026, 3, 9, 0, 0),
            LocalDateTime.of(1, 1, 1, 0, 0), LocalDateTime.of(1969, 12, 31, 23, 59, 59),
            LocalDateTime.of(1970, 1, 1, 0, 0), LocalDateTime.of(9998, 12, 31, 23, 59, 59)};

    @Test
    public void testPeriodStartsAreThoseOfTheFoldAndOfThePartitions() {
        for (String unit : UNITS) {
            DatePeriod period = period(call("date_trunc", ConstantOperator.createVarchar(unit), DATE));
            for (LocalDateTime time : SAMPLES) {
                LocalDateTime folded = ScalarOperatorFunctions.dateTrunc(ConstantOperator.createVarchar(unit),
                        ConstantOperator.createDatetime(time)).getDatetime();
                Assertions.assertEquals(folded, period.start(time), unit + " " + time);
                Assertions.assertEquals(SyncPartitionUtils.getLowerDateTime(time, unit), period.start(time),
                        unit + " " + time);
                Assertions.assertEquals(SyncPartitionUtils.nextUpperDateTime(folded, unit), period.next(time),
                        unit + " " + time);
            }
        }
    }

    // The week of date_trunc starts on a Monday and the quarter on the first of January, April, July and October
    @Test
    public void testWeeksAndQuartersCountTheDistinctValuesOfTheFold() {
        LocalDateTime first = LocalDateTime.of(2018, 11, 20, 13, 14, 15);
        LocalDateTime last = LocalDateTime.of(2026, 3, 5, 13, 14, 15);
        for (String unit : new String[] {"day", "week", "month", "quarter", "year"}) {
            Set<LocalDateTime> values = new HashSet<>();
            for (LocalDateTime time = first; !time.isAfter(last); time = time.plusDays(1)) {
                values.add(ScalarOperatorFunctions.dateTrunc(ConstantOperator.createVarchar(unit),
                        ConstantOperator.createDatetime(time)).getDatetime());
            }
            DatePeriod period = period(call("date_trunc", ConstantOperator.createVarchar(unit), DATE));
            Assertions.assertEquals(values.size(), period.count(first, last), unit);
        }
    }

    // The values of a function change exactly where its period changes: a day with the same value as the next one is in
    // the same period, a day with another value is in another
    private static void assertPeriodsAreTheValues(String message, CallOperator call,
                                                  Function<LocalDateTime, Object> fold) {
        DatePeriod period = period(call);
        LocalDateTime first = LocalDateTime.of(2018, 11, 20, 13, 14, 15);
        LocalDateTime last = LocalDateTime.of(2020, 3, 5, 13, 14, 15);
        Set<Object> values = new HashSet<>();
        for (LocalDateTime time = first; !time.isAfter(last); time = time.plusDays(1)) {
            values.add(fold.apply(time));
            Assertions.assertEquals(fold.apply(time).equals(fold.apply(time.plusDays(1))),
                    period.indexOf(time) == period.indexOf(time.plusDays(1)), message + " " + time);
        }
        Assertions.assertEquals(values.size(), period.count(first, last), message);
    }

    @Test
    public void testLastDayChangesWithTheMonthTheQuarterAndTheYear() {
        assertPeriodsAreTheValues("last_day", call("last_day", DATE),
                time -> ScalarOperatorFunctions.lastDay(ConstantOperator.createDatetime(time)).getDatetime());
        for (String unit : new String[] {"month", "quarter", "year", "QUARTER"}) {
            assertPeriodsAreTheValues("last_day " + unit, call("last_day", DATE, ConstantOperator.createVarchar(unit)),
                    time -> ScalarOperatorFunctions.lastDay(ConstantOperator.createDatetime(time),
                            ConstantOperator.createVarchar(unit)).getDatetime());
        }
    }

    @Test
    public void testNextDayAndPreviousDayChangeWithTheWeekOfTheirDay() {
        for (String day : DAYS) {
            assertPeriodsAreTheValues("next_day " + day, call("next_day", DATE, ConstantOperator.createVarchar(day)),
                    time -> ScalarOperatorFunctions.nextDay(ConstantOperator.createDatetime(time),
                            ConstantOperator.createVarchar(day)).getDatetime());
            assertPeriodsAreTheValues("previous_day " + day,
                    call("previous_day", DATE, ConstantOperator.createVarchar(day)),
                    time -> ScalarOperatorFunctions.previousDay(ConstantOperator.createDatetime(time),
                            ConstantOperator.createVarchar(day)).getDatetime());
        }
    }
}
