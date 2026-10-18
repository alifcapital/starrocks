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

package com.starrocks.common.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class DatePeriodTest {
    private static final LocalDateTime FIRST = LocalDateTime.of(2018, 11, 20, 13, 14, 15, 123_000_000);
    private static final LocalDateTime LAST = LocalDateTime.of(2026, 3, 5, 1, 2, 3);

    private static DatePeriod unit(String name) {
        return DatePeriod.ofUnit(name).orElseThrow(() -> new AssertionError(name));
    }

    @Test
    public void testPeriodsOfTheRangeAreCountedFromTheirStarts() {
        // the months from 2018-11 to 2026-03 are 89 and the years 9, whichever day of the month the ends fall on
        Assertions.assertEquals(89, unit("month").count(FIRST, LAST));
        Assertions.assertEquals(89, unit("month").count(FIRST.withDayOfMonth(1), LAST.withDayOfMonth(31)));
        Assertions.assertEquals(9, unit("year").count(FIRST, LAST));
        Assertions.assertEquals(30, unit("quarter").count(FIRST, LAST));
        Assertions.assertEquals(2663, unit("day").count(FIRST, LAST));
        Assertions.assertEquals(1, unit("month").count(LAST, LAST));
        // 2026-03-05 is a Thursday: the weeks that start on a Monday
        Assertions.assertEquals(1, unit("week").count(LocalDateTime.of(2026, 3, 2, 0, 0), LocalDateTime.of(2026, 3, 8, 23, 59)));
        Assertions.assertEquals(2, unit("week").count(LocalDateTime.of(2026, 3, 2, 0, 0), LocalDateTime.of(2026, 3, 9, 0, 0)));
        Assertions.assertEquals(109, unit("hour").count(LocalDateTime.of(2026, 3, 1, 0, 0),
                LocalDateTime.of(2026, 3, 5, 12, 30, 45)));
        Assertions.assertEquals(3601, unit("second").count(LocalDateTime.of(2026, 3, 1, 10, 0),
                LocalDateTime.of(2026, 3, 1, 11, 0)));
    }

    @Test
    public void testPeriodStartsAndNextStarts() {
        Assertions.assertEquals(LocalDateTime.of(2018, 11, 1, 0, 0), unit("month").start(FIRST));
        Assertions.assertEquals(LocalDateTime.of(2018, 12, 1, 0, 0), unit("month").next(FIRST));
        Assertions.assertEquals(LocalDateTime.of(2018, 10, 1, 0, 0), unit("quarter").start(FIRST));
        Assertions.assertEquals(LocalDateTime.of(2019, 1, 1, 0, 0), unit("quarter").next(FIRST));
        Assertions.assertEquals(LocalDateTime.of(2018, 1, 1, 0, 0), unit("year").start(FIRST));
        Assertions.assertEquals(LocalDateTime.of(2018, 11, 19, 0, 0), unit("week").start(FIRST));
        Assertions.assertEquals(LocalDateTime.of(2018, 11, 26, 0, 0), unit("week").next(FIRST));
        Assertions.assertEquals(LocalDateTime.of(2018, 11, 20, 13, 0), unit("hour").start(FIRST));
        Assertions.assertEquals(LocalDateTime.of(2018, 11, 20, 13, 14), unit("minute").start(FIRST));
        Assertions.assertEquals(LocalDateTime.of(2018, 11, 20, 13, 14, 15), unit("second").start(FIRST));
    }

    @Test
    public void testWeeksStartOnTheGivenDay() {
        // 2026-03-07 is a Saturday and 2026-03-08 a Sunday
        LocalDateTime saturday = LocalDateTime.of(2026, 3, 7, 12, 0);
        LocalDateTime sunday = LocalDateTime.of(2026, 3, 8, 12, 0);
        Assertions.assertEquals(1, DatePeriod.weeks(DayOfWeek.MONDAY).count(saturday, sunday));
        Assertions.assertEquals(2, DatePeriod.weeks(DayOfWeek.SUNDAY).count(saturday, sunday));
        for (DayOfWeek first : DayOfWeek.values()) {
            DatePeriod weeks = DatePeriod.weeks(first);
            for (int day = 0; day < 21; day++) {
                LocalDateTime time = LocalDateTime.of(2026, 3, 1, 5, 6).plusDays(day);
                LocalDateTime start = weeks.start(time);
                Assertions.assertEquals(first, start.getDayOfWeek(), first + " " + time);
                Assertions.assertFalse(start.isAfter(time));
                Assertions.assertTrue(time.isBefore(weeks.next(time)));
                Assertions.assertEquals(start.plusDays(7), weeks.next(time));
            }
        }
        Assertions.assertEquals(unit("week").start(saturday), DatePeriod.weeks(DayOfWeek.MONDAY).start(saturday));
    }

    @Test
    public void testDatesBeforeTheEpochAndTheYearZero() {
        LocalDateTime first = LocalDateTime.of(0, 1, 1, 0, 0);
        Assertions.assertEquals(LocalDateTime.of(0, 1, 1, 0, 0), unit("month").start(first));
        Assertions.assertEquals(LocalDateTime.of(0, 1, 1, 0, 0), unit("year").start(first));
        Assertions.assertEquals(120_000, unit("month").count(first, LocalDateTime.of(9999, 12, 31, 23, 59, 59)));
        Assertions.assertEquals(10_000, unit("year").count(first, LocalDateTime.of(9999, 12, 31, 23, 59, 59)));
        Assertions.assertEquals(3_652_425, unit("day").count(first, LocalDateTime.of(9999, 12, 31, 23, 59, 59)));
        // 0000-01-01 is a Saturday, so the week of a Monday starts before the type does
        Assertions.assertEquals(LocalDate.of(-1, 12, 27), unit("week").start(first).toLocalDate());
        Assertions.assertEquals(LocalDateTime.of(1969, 12, 31, 23, 0),
                unit("hour").start(LocalDateTime.of(1969, 12, 31, 23, 59)));
    }

    @Test
    public void testUnitsWithoutAPeriodAndTheSinglePeriod() {
        Assertions.assertTrue(DatePeriod.ofUnit("millisecond").isEmpty());
        Assertions.assertTrue(DatePeriod.ofUnit("microsecond").isEmpty());
        Assertions.assertTrue(DatePeriod.ofUnit("decade").isEmpty());
        Assertions.assertTrue(DatePeriod.ofUnit("MONTH").isEmpty());
        Assertions.assertTrue(DatePeriod.ofField(DateFormatModel.Field.MICRO).isEmpty());
        for (DateFormatModel.Field field : DateFormatModel.Field.values()) {
            Assertions.assertEquals(field != DateFormatModel.Field.MICRO, DatePeriod.ofField(field).isPresent(), field.name());
        }
        Assertions.assertTrue(DatePeriod.single().isSingle());
        Assertions.assertEquals(1, DatePeriod.single().count(FIRST, LAST));
        Assertions.assertFalse(unit("month").isSingle());
    }
}
