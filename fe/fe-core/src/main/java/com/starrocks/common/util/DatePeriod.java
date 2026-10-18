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

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

/**
 * The periods of time that a function of a date is constant on: date_trunc('month', x) gives one value for every x in a
 * month, last_day(x) the same, next_day(x, 'Sunday') one for every week that starts on a Sunday. We want one way to count
 * the periods between two dates, so every function with such a period has the same number of distinct values for the same
 * range, and a function that is not constant on a period of the calendar is not given one.
 * <p>
 * A period is a number of months, days or seconds, and a position where one period starts. The months count from
 * January of the year 0 and the days from 1970-01-01, so a quarter starts in January, April, July and October and
 * a year in January; the seconds count from 1970-01-01 00:00:00. A week starts on a day of the week that the caller
 * gives, as date_trunc starts it on a Monday and next_day on the day that follows the one it looks for.
 */
public final class DatePeriod {
    private enum Basis {
        MONTHS,
        DAYS,
        SECONDS,
        // the whole type is one period: the value does not depend on the date
        ALL
    }

    // 1970-01-05 is a Monday
    private static final long MONDAY = LocalDate.of(1970, 1, 5).toEpochDay();
    private static final DatePeriod SINGLE = new DatePeriod(Basis.ALL, 1, 0);

    private final Basis basis;
    private final long length;
    private final long origin;

    private DatePeriod(Basis basis, long length, long origin) {
        this.basis = basis;
        this.length = length;
        this.origin = origin;
    }

    /**
     * The period of a unit of date_trunc: second, minute, hour, day, week (from a Monday), month, quarter or year; empty
     * for any other unit, as the periods below a second are too many to count.
     */
    public static Optional<DatePeriod> ofUnit(String unit) {
        switch (unit) {
            case "second":
                return Optional.of(new DatePeriod(Basis.SECONDS, 1, 0));
            case "minute":
                return Optional.of(new DatePeriod(Basis.SECONDS, 60, 0));
            case "hour":
                return Optional.of(new DatePeriod(Basis.SECONDS, 3600, 0));
            case "day":
                return Optional.of(new DatePeriod(Basis.DAYS, 1, 0));
            case "week":
                return Optional.of(weeks(DayOfWeek.MONDAY));
            case "month":
                return Optional.of(new DatePeriod(Basis.MONTHS, 1, 0));
            case "quarter":
                return Optional.of(new DatePeriod(Basis.MONTHS, 3, 0));
            case "year":
                return Optional.of(new DatePeriod(Basis.MONTHS, 12, 0));
            default:
                return Optional.empty();
        }
    }

    /** The period of the finest field of a format of date_format; empty for the fraction of a second. */
    public static Optional<DatePeriod> ofField(DateFormatModel.Field field) {
        switch (field) {
            case YEAR:
                return ofUnit("year");
            case MONTH:
                return ofUnit("month");
            case DAY:
                return ofUnit("day");
            case HOUR:
                return ofUnit("hour");
            case MINUTE:
                return ofUnit("minute");
            case SECOND:
                return ofUnit("second");
            default:
                return Optional.empty();
        }
    }

    /** The weeks that start on the given day of the week. */
    public static DatePeriod weeks(DayOfWeek firstDay) {
        return new DatePeriod(Basis.DAYS, 7, MONDAY + firstDay.getValue() - 1);
    }

    /** One period for every date: the function does not depend on the date. */
    public static DatePeriod single() {
        return SINGLE;
    }

    public boolean isSingle() {
        return basis == Basis.ALL;
    }

    private long position(LocalDateTime time) {
        switch (basis) {
            case MONTHS:
                return time.getYear() * 12L + time.getMonthValue() - 1;
            case DAYS:
                return time.toLocalDate().toEpochDay();
            case SECONDS:
                return time.toEpochSecond(ZoneOffset.UTC);
            default:
                return 0;
        }
    }

    /** The number of the period that holds the time, counted from the period that starts at the origin. */
    public long indexOf(LocalDateTime time) {
        return isSingle() ? 0 : Math.floorDiv(position(time) - origin, length);
    }

    /** The number of periods that hold a time of the range, the two ends included. */
    public long count(LocalDateTime min, LocalDateTime max) {
        return indexOf(max) - indexOf(min) + 1;
    }

    private LocalDateTime startOf(long index) {
        long position = origin + index * length;
        switch (basis) {
            case MONTHS:
                return LocalDateTime.of((int) Math.floorDiv(position, 12), (int) Math.floorMod(position, 12) + 1, 1, 0, 0);
            case DAYS:
                return LocalDate.ofEpochDay(position).atStartOfDay();
            case SECONDS:
                return LocalDateTime.ofEpochSecond(position, 0, ZoneOffset.UTC);
            default:
                throw new IllegalStateException("one period has no start");
        }
    }

    /** The start of the period that holds the time. */
    public LocalDateTime start(LocalDateTime time) {
        return startOf(indexOf(time));
    }

    /** The start of the period that follows the one that holds the time. */
    public LocalDateTime next(LocalDateTime time) {
        return startOf(indexOf(time) + 1);
    }
}
