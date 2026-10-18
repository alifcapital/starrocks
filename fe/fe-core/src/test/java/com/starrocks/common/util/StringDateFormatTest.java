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

import com.starrocks.sql.optimizer.statistics.StatisticsDatetimeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StringDateFormatTest {
    private static final LocalDateTime TIME = LocalDateTime.of(2024, 3, 5, 7, 8, 9, 123_456_000);

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "%Y-%m-%d|2024-03-05|DAYS",
            "%Y/%m/%d %H:%i|2024/03/05 07:08|MINUTES",
            "%Y%m%d%H%i|202403050708|MINUTES",
            "%Y-%m-%dT%H:%i:%s.%fZ|2024-03-05T07:08:09.123456Z|MICROS",
            "%Y-%m|2024-03|MONTHS",
            "%Y|2024|YEARS",
            "%Y%m%d|20240305|DAYS",
            "%Y%m%d%H%i%s|20240305070809|SECONDS",
            "%Y-%m-%d %H:%i:%s|2024-03-05 07:08:09|SECONDS",
            "%Y-%m-%dT%H:%i:%s|2024-03-05T07:08:09|SECONDS",
            "%Y-%m-%d %H:%i:%s.%f|2024-03-05 07:08:09.123456|MICROS",
            "%Y-%m-%dT%H:%i:%s.%f|2024-03-05T07:08:09.123456|MICROS",
            "%Y-%m-%dT%H:%i:%sZ|2024-03-05T07:08:09Z|SECONDS",
            "%Y-%m-%d %H|2024-03-05 07|HOURS",
            "%Y.%m.%d|2024.03.05|DAYS",
            "%Y-%m-%d 100%%|2024-03-05 100%|DAYS"})
    public void testSuitablePatterns(String pattern, String text, ChronoUnit precision) {
        StringDateFormat format = StringDateFormat.parse(pattern);
        assertEquals(pattern, format.getPattern());
        assertEquals(text, format.format(TIME));
        assertTrue(format.matches(text));
        assertEquals(precision, format.getPrecision());
        assertEquals(text, format.format(StringDateFormat.truncate(TIME, precision)));
    }

    // The patterns with a zero byte are the last four: the model gives such a pattern no order, see DateFormatModel
    @ParameterizedTest
    @ValueSource(strings = {"%d.%m.%Y", "%m/%d/%Y", "%Y-%c-%e", "%y", "%y%m%d", "%Y-%m-%dT%H:%i:%s%z",
            "%Y-%m-%dT%H:%i:%s+05:00", "%Y-%m-%d %H:%i:%s-0500", "%Y %b %d", "%Y-%M-%d", "%Y-%m %H", "%Y-%d",
            "%Y%m%d%H%i%f", "%Y-%m-%d%", "%Y%Y", "", "D%Y%m%d", "%Y-%m-%d %h:%i", "%Y-%m-%d %T",
            "%Y-%m-%d\0x", "%Y-%m\0", "%Y\0-%m", "%Y-%m-%d\0"})
    public void testUnsuitablePatterns(String pattern) {
        assertThrows(IllegalArgumentException.class, () -> StringDateFormat.parse(pattern));
    }

    @Test
    public void testNullPattern() {
        assertThrows(IllegalArgumentException.class, () -> StringDateFormat.parse(null));
    }

    @Test
    public void testEdgesPrintAndMatch() {
        StringDateFormat micros = StringDateFormat.parse("%Y-%m-%d %H:%i:%s.%f");
        for (LocalDateTime time : List.of(LocalDateTime.of(0, 1, 1, 0, 0),
                LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_000),
                LocalDateTime.of(2023, 4, 30, 12, 0), LocalDateTime.of(2023, 2, 28, 0, 0),
                LocalDateTime.of(2024, 2, 29, 0, 0, 0, 1_000))) {
            String text = micros.format(time);
            assertTrue(micros.matches(text), text);
        }
        assertEquals("0000-01-01 00:00:00.000000", micros.format(LocalDateTime.of(0, 1, 1, 0, 0)));
        assertEquals("9999-12-31 23:59:59.999999",
                micros.format(LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_000)));
        assertEquals("2024-02-29 00:00:00.000001", micros.format(LocalDateTime.of(2024, 2, 29, 0, 0, 0, 1_000)));
        for (String text : List.of("2023-02-29 00:00:00.000000", "2023-04-31 00:00:00.000000",
                "2024-13-01 00:00:00.000000", "2024-00-01 00:00:00.000000", "2024-01-00 00:00:00.000000",
                "2024-01-01 24:00:00.000000", "2024-01-01 00:60:00.000000", "2024-01-01 00:00:60.000000",
                "2024-01-01 00:00:00.00000", "2024-01-01 00:00:00.0000000", "2024-01-01T00:00:00.000000",
                "2024-1-01 00:00:00.000000", "2024-01-01 00:00:00.00000a", "+024-01-01 00:00:00.000000",
                " 2024-01-01 00:00:00.000000", "")) {
            assertFalse(micros.matches(text), text);
        }

        StringDateFormat month = StringDateFormat.parse("%Y-%m");
        assertEquals("0000-01", month.format(LocalDateTime.of(0, 1, 31, 23, 0)));
        assertEquals("9999-12", month.format(LocalDateTime.of(9999, 12, 31, 23, 0)));
        assertTrue(month.matches("2024-02"));
        assertFalse(month.matches("2024-13"));
        assertFalse(month.matches("2024-02-01"));

        StringDateFormat year = StringDateFormat.parse("%Y");
        assertEquals("0000", year.format(LocalDateTime.of(0, 6, 1, 0, 0)));
        assertTrue(year.matches("9999"));
        assertFalse(year.matches("10000"));
        assertFalse(year.matches("999"));
    }

    @Test
    public void testTruncateToEachUnit() {
        LocalDateTime time = LocalDateTime.of(2024, 2, 29, 23, 59, 58, 999_999_000);
        assertEquals(LocalDateTime.of(2024, 1, 1, 0, 0), StringDateFormat.truncate(time, ChronoUnit.YEARS));
        assertEquals(LocalDateTime.of(2024, 2, 1, 0, 0), StringDateFormat.truncate(time, ChronoUnit.MONTHS));
        assertEquals(LocalDateTime.of(2024, 2, 29, 0, 0), StringDateFormat.truncate(time, ChronoUnit.DAYS));
        assertEquals(LocalDateTime.of(2024, 2, 29, 23, 0), StringDateFormat.truncate(time, ChronoUnit.HOURS));
        assertEquals(LocalDateTime.of(2024, 2, 29, 23, 59), StringDateFormat.truncate(time, ChronoUnit.MINUTES));
        assertEquals(LocalDateTime.of(2024, 2, 29, 23, 59, 58), StringDateFormat.truncate(time, ChronoUnit.SECONDS));
        assertEquals(time, StringDateFormat.truncate(time, ChronoUnit.MICROS));
    }

    @Test
    public void testUtcSuffixNeedsUtcJvm() {
        TimeZone saved = TimeZone.getDefault();
        try {
            StringDateFormat utc = StringDateFormat.parse("%Y-%m-%dT%H:%i:%s.%fZ");
            StringDateFormat plain = StringDateFormat.parse("%Y-%m-%dT%H:%i:%s.%f");
            assertTrue(utc.hasUtcSuffix());
            assertFalse(plain.hasUtcSuffix());
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            assertTrue(utc.isSupportedInCurrentTimezone());
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Dushanbe"));
            assertFalse(utc.isSupportedInCurrentTimezone());
            assertTrue(plain.isSupportedInCurrentTimezone());
        } finally {
            TimeZone.setDefault(saved);
        }
    }

    @Test
    public void testReadAsWrittenByBackend() {
        DateTimeFormatter seconds = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
        StringDateFormat format = StringDateFormat.parse("%Y-%m-%d %H:%i:%s");
        assertTrue(format.isReadAsWrittenBy(text -> LocalDateTime.parse(text, seconds)));
        // A BE that drops the time, or gives NULL for every string, does not read the format as written.
        assertFalse(format.isReadAsWrittenBy(text -> LocalDateTime.parse(text, seconds).truncatedTo(ChronoUnit.DAYS)));
        assertFalse(format.isReadAsWrittenBy(text -> null));

        StringDateFormat month = StringDateFormat.parse("%Y-%m");
        assertTrue(month.isReadAsWrittenBy(text -> null));
        assertFalse(month.isReadAsWrittenBy(
                text -> LocalDateTime.of(Integer.parseInt(text.substring(0, 4)), 1, 1, 0, 0)));
    }

    // The BE reads these strings as written: a date of three fields, a time after a 'T' or a space, a fraction after a
    // dot. A format without a day always gives NULL in the BE.
    @ParameterizedTest
    @ValueSource(strings = {"%Y-%m-%d", "%Y%m%d", "%Y%m%d%H%i%s", "%Y-%m-%d %H:%i:%s", "%Y-%m-%dT%H:%i:%s",
            "%Y-%m-%d %H:%i:%s.%f", "%Y-%m-%dT%H:%i:%s.%f", "%Y-%m-%dT%H:%i:%sZ", "%Y-%m-%dT%H:%i:%s.%fZ",
            "%Y/%m/%d %H:%i", "%Y.%m.%d", "%Y-%m-%d %H", "%Y-%m-%dT%H:%i", "%Y%m%dT%H%i%s", "%Y-%m", "%Y", "%Y/%m"})
    public void testFormatsTheBackendReadsAsWritten(String pattern) {
        assertTrue(StringDateFormat.parse(pattern).isReadAsWrittenBy(StatisticsDatetimeParser::parse), pattern);
    }

    // A string of 6, 10 or 12 digits has a year of two digits in the BE, digits of a fraction without a dot are
    // dropped, a time after another separator than 'T' or a space is dropped, and a date of only digits before a
    // space gives NULL.
    @ParameterizedTest
    @ValueSource(strings = {"%Y%m%d%H%i", "%Y%m", "%Y%m%d%H", "%Y%m%d%H%i%s%f", "%Y-%m-%d_%H",
            "%Y-%m-%d %H:%i:%s,%f", "%Y%m%dT%H%i", "%Y%m%d %H:%i:%s"})
    public void testFormatsTheBackendReadsOtherwise(String pattern) {
        assertFalse(StringDateFormat.parse(pattern).isReadAsWrittenBy(StatisticsDatetimeParser::parse), pattern);
    }
}
