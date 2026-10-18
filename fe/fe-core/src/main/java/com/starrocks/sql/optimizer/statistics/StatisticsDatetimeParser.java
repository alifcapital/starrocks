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

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

/**
 * Text and numbers read as DATE and DATETIME the way the BE casts them: TimestampValue::from_string with
 * parse_datetime_fallback and date::from_string in runtime/time_types.cpp, and date::standardize_date for numbers.
 * We expect statistics to hold values that the BE turns into dates when the query runs, so a value must read here
 * as the BE reads it, and a value the BE turns into NULL must give null here. The fixed-width fast path of the BE
 * gives the same results as the fallback, so only the fallback is ported. The parser walks bytes: a string with
 * other than ASCII characters is read as its UTF-8 bytes.
 */
public final class StatisticsDatetimeParser {
    private static final int YY_PART_YEAR = 70;
    private static final int[][] DAYS_IN_MONTH = {
            {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
            {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};
    private static final int[] POW10 = {1, 10, 100, 1000, 10000, 100000, 1000000};
    // date::from_string allows a space only after the day and after the microseconds
    private static final int ALLOW_SPACE_MASK = 4 | 64;
    private static final int MAX_DATE_PARTS = 8;

    // Positions of the fields in the array the parsers fill
    static final int YEAR = 0;
    static final int MONTH = 1;
    static final int DAY = 2;
    static final int HOUR = 3;
    static final int MINUTE = 4;
    static final int SECOND = 5;
    static final int MICROSECOND = 6;

    private StatisticsDatetimeParser() {
    }

    /** The DATETIME the BE casts the text to, or null when the BE gives NULL. CAST AS DATE keeps its date. */
    public static LocalDateTime parse(String text) {
        int[] fields = new int[MAX_DATE_PARTS];
        if (!parseFallback(bytes(text), fields)) {
            return null;
        }
        return LocalDateTime.of(fields[YEAR], fields[MONTH], fields[DAY], fields[HOUR], fields[MINUTE],
                fields[SECOND], fields[MICROSECOND] * 1000);
    }

    // The text as one char per byte of its UTF-8 form, so positions and lengths are those the BE sees.
    static String bytes(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) >= 0x80) {
                return new String(text.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
            }
        }
        return text;
    }

    static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    // is_space in time_types.cpp: the space and \t, \n, \v, \f, \r
    static boolean isSpace(char c) {
        return c == ' ' || (c >= 9 && c <= 13);
    }

    // ispunct of the C locale
    private static boolean isPunct(char c) {
        return (c >= 33 && c <= 47) || (c >= 58 && c <= 64) || (c >= 91 && c <= 96) || (c >= 123 && c <= 126);
    }

    // date::is_leap: the year 0 is not a leap year
    static boolean isLeap(int year) {
        return year % 4 == 0 && (year % 100 != 0 || (year % 400 == 0 && year != 0));
    }

    static int daysInMonth(int year, int month) {
        return DAYS_IN_MONTH[isLeap(year) ? 1 : 0][month];
    }

    private static int digit(String s, int i) {
        return s.charAt(i) - '0';
    }

    /** parse_datetime_fallback; false when the value is NULL. */
    static boolean parseFallback(String s, int[] fields) {
        int ptr = 0;
        int end = s.length();
        while (ptr < end && isSpace(s.charAt(ptr))) {
            ptr++;
        }
        while (ptr < end && isSpace(s.charAt(end - 1))) {
            end--;
        }
        if (end - ptr < 10) {
            return parseGeneric(s, fields);
        }
        for (int i = 0; i < 10; i++) {
            boolean separator = i == 4 || i == 7;
            if (isDigit(s.charAt(ptr + i)) == separator) {
                return parseGeneric(s, fields);
            }
        }
        int year = digit(s, ptr) * 1000 + digit(s, ptr + 1) * 100 + digit(s, ptr + 2) * 10 + digit(s, ptr + 3);
        int month = digit(s, ptr + 5) * 10 + digit(s, ptr + 6);
        int day = digit(s, ptr + 8) * 10 + digit(s, ptr + 9);
        if (month < 1 || month > 12 || day < 1 || day > daysInMonth(year, month)) {
            return parseGeneric(s, fields);
        }
        fields[YEAR] = year;
        fields[MONTH] = month;
        fields[DAY] = day;
        fields[HOUR] = 0;
        fields[MINUTE] = 0;
        fields[SECOND] = 0;
        fields[MICROSECOND] = 0;
        if (end - ptr == 10) {
            return true;
        }
        char separator = s.charAt(ptr + 10);
        int time = -1;
        if (separator == 'T') {
            time = ptr + 11;
        } else if (isSpace(separator)) {
            time = ptr + 10;
            while (time < end && isSpace(s.charAt(time))) {
                time++;
            }
        }
        // Text after the date that does not start a time is ignored
        if (time < 0 || end == time) {
            return true;
        }
        if (end - time < 8) {
            return parseGeneric(s, fields);
        }
        if (s.charAt(time + 2) != ':' || s.charAt(time + 5) != ':' || !isDigit(s.charAt(time))
                || !isDigit(s.charAt(time + 1)) || !isDigit(s.charAt(time + 3)) || !isDigit(s.charAt(time + 4))
                || !isDigit(s.charAt(time + 6)) || !isDigit(s.charAt(time + 7))) {
            return parseGeneric(s, fields);
        }
        int hour = digit(s, time) * 10 + digit(s, time + 1);
        int minute = digit(s, time + 3) * 10 + digit(s, time + 4);
        int second = digit(s, time + 6) * 10 + digit(s, time + 7);
        if (hour > 23 || minute > 59 || second > 59) {
            return parseGeneric(s, fields);
        }
        fields[HOUR] = hour;
        fields[MINUTE] = minute;
        fields[SECOND] = second;
        int fraction = time + 8;
        if (fraction < end && s.charAt(fraction) == 'Z') {
            return true;
        }
        if (fraction < end && s.charAt(fraction) == '.') {
            int p = fraction + 1;
            int value = 0;
            int count = 0;
            // Digits past the sixth are cut off, not rounded
            while (p < end && isDigit(s.charAt(p)) && count < 6) {
                value = value * 10 + digit(s, p);
                p++;
                count++;
            }
            if (count > 0) {
                value *= POW10[6 - count];
            }
            fields[MICROSECOND] = value;
            if (p == end || s.charAt(p) == 'Z') {
                return true;
            }
        } else if (fraction == end) {
            return true;
        }
        return parseGeneric(s, fields);
    }

    /** date::from_string; false when the value is NULL. */
    static boolean parseGeneric(String s, int[] fields) {
        int ptr = 0;
        int end = s.length();
        while (ptr < end && isSpace(s.charAt(ptr))) {
            ptr++;
        }
        while (ptr < end && isSpace(s.charAt(end - 1))) {
            end--;
        }
        if (ptr == end || !isDigit(s.charAt(ptr))) {
            return false;
        }
        int pos = ptr;
        while (pos < end && (isDigit(s.charAt(pos)) || s.charAt(pos) == 'T')) {
            pos++;
        }
        int yearLength = 4;
        int digits = pos - ptr;
        // A string of digits alone (MySQL compatibility): 4, 8 or at least 14 digits start with a 4-digit year
        boolean digitsOnly = false;
        if (pos == end || s.charAt(pos) == '.') {
            yearLength = digits == 4 || digits == 8 || digits >= 14 ? 4 : 2;
            digitsOnly = true;
        }
        int[] lengths = new int[MAX_DATE_PARTS];
        int fieldIndex = 0;
        int fieldLength = yearLength;
        while (ptr < end && isDigit(s.charAt(ptr)) && fieldIndex < MAX_DATE_PARTS - 1) {
            int start = ptr;
            int value = 0;
            boolean scanToDelimiter = !digitsOnly && fieldIndex != 6;
            while (ptr < end && isDigit(s.charAt(ptr)) && (scanToDelimiter || fieldLength-- != 0)) {
                value = value * 10 + digit(s, ptr++);
                if (value > 999999) {
                    return false;
                }
            }
            fields[fieldIndex] = value;
            lengths[fieldIndex] = ptr - start;
            fieldLength = 2;
            if (ptr == end) {
                fieldIndex++;
                break;
            }
            if (fieldIndex == 2 && s.charAt(ptr) == 'T') {
                ptr++;
                fieldIndex++;
                continue;
            }
            if (fieldIndex == 5) {
                if (s.charAt(ptr) == '.') {
                    ptr++;
                    fieldLength = 6;
                } else if (isDigit(s.charAt(ptr))) {
                    fieldIndex++;
                    break;
                }
                fieldIndex++;
                continue;
            }
            while (ptr < end && (isPunct(s.charAt(ptr)) || isSpace(s.charAt(ptr)))) {
                if (isSpace(s.charAt(ptr)) && ((1 << fieldIndex) & ALLOW_SPACE_MASK) == 0) {
                    return false;
                }
                ptr++;
            }
            fieldIndex++;
        }
        int fieldCount = fieldIndex;
        if (fieldCount < 3) {
            return false;
        }
        if (!digitsOnly) {
            yearLength = lengths[0];
        }
        for (; fieldIndex < MAX_DATE_PARTS; fieldIndex++) {
            fields[fieldIndex] = 0;
            lengths[fieldIndex] = 0;
        }
        if (fields[MICROSECOND] != 0 && lengths[MICROSECOND] < 6) {
            fields[MICROSECOND] *= POW10[6 - lengths[MICROSECOND]];
        }
        if (yearLength == 2) {
            fields[YEAR] += fields[YEAR] < YY_PART_YEAR ? 2000 : 1900;
        }
        int year = fields[YEAR];
        int month = fields[MONTH];
        int day = fields[DAY];
        if (year > 9999 || month < 1 || month > 12 || day < 1 || day > daysInMonth(year, month)) {
            return false;
        }
        return fieldCount <= 3 || (fields[HOUR] <= 23 && fields[MINUTE] <= 59 && fields[SECOND] <= 59
                && fields[MICROSECOND] < 1000000);
    }

    /**
     * The DATETIME the BE casts a number to, or null when the BE gives NULL: date::standardize_date reads the digits
     * as YYMMDD, YYYYMMDD, YYMMDDhhmmss or YYYYMMDDhhmmss, and timestamp::check rejects a date or time that does not
     * exist. CAST AS DATE checks the time as well and keeps the date.
     */
    static LocalDateTime fromNumber(long value) {
        long standard = standardizeDate(value);
        if (standard <= 0) {
            return null;
        }
        long date = standard / 1000000;
        long time = standard % 1000000;
        int year = (int) (date / 10000);
        int month = (int) (date / 100 % 100);
        int day = (int) (date % 100);
        int hour = (int) (time / 10000);
        int minute = (int) (time / 100 % 100);
        int second = (int) (time % 100);
        if (year > 9999 || month < 1 || month > 12 || day < 1 || day > daysInMonth(year, month)
                || hour > 23 || minute > 59 || second > 59) {
            return null;
        }
        return LocalDateTime.of(year, month, day, hour, minute, second);
    }

    // date::standardize_date
    static long standardizeDate(long value) {
        if (value <= 0) {
            return 0;
        }
        if (value >= 10000101000000L) {
            return value > 99999999999999L ? 0 : value;
        }
        if (value < 101) {
            return 0;
        }
        if (value <= (YY_PART_YEAR - 1) * 10000L + 1231L) {
            return (value + 20000000L) * 1000000L;
        }
        if (value < YY_PART_YEAR * 10000L + 101) {
            return 0;
        }
        if (value <= 991231L) {
            return (value + 19000000L) * 1000000L;
        }
        if (value < 10000101) {
            return 0;
        }
        if (value <= 99991231L) {
            return value * 1000000L;
        }
        if (value < 101000000) {
            return 0;
        }
        if (value <= (YY_PART_YEAR - 1) * 10000000000L + 1231235959L) {
            return value + 20000000000000L;
        }
        if (value < YY_PART_YEAR * 10000000000L + 101000000L) {
            return 0;
        }
        if (value <= 991231235959L) {
            return value + 19000000000000L;
        }
        return value;
    }
}
