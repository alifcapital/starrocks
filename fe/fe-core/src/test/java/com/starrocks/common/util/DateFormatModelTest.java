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

import com.starrocks.common.util.DateFormatModel.Field;
import com.starrocks.sql.optimizer.statistics.StatisticsDatetimeParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The model of a format of date_format: what the tokenizer reads, and what each fact about a format is. The table of the
 * facts is the one that the inverses, the images and the statistics of date_format read.
 */
public class DateFormatModelTest {
    private static final LocalDateTime MIN = LocalDateTime.of(2018, 11, 20, 0, 0);
    private static final LocalDateTime MAX = LocalDateTime.of(2026, 3, 5, 0, 0);

    // ordered | read as written | read as the start of the period | exact on all rows
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "%Y|true|true|false|true",
            "%Y%m|true|false|false|true",
            "%Y-%m|true|true|false|true",
            "%Y%m%d|true|true|true|true",
            "%Y%m%d%H|true|false|false|true",
            "%Y-%m-01 00:00:00|true|true|true|true",
            "%Y-01-01 00:00:00|true|true|true|true",
            "%Y-%m-%d 00:00:00|true|true|true|true",
            "%Y-%m-01|true|true|true|true",
            "%Y-%m-%d %H:00:00|true|true|true|true",
            "%Y-%m-%d %H:%i:00|true|true|true|true",
            "%Y-%m-%dT%H:%i:%s|true|true|true|true",
            "%Y-%m-%dT%H:%i:%S|true|true|true|true",
            "%Y-%m-%d %H:%i:%s.%f|true|true|true|true",
            "yyyy-MM-dd|true|true|true|true",
            "yyyyMMdd|true|true|true|true",
            "yyyy-MM-dd HH:mm:ss|true|true|true|true",
            "%d.%m.%Y|false|false|false|true",
            "%y-%m|false|false|false|true",
            "%Y-%c|false|false|false|true",
            "%m/%d/%Y %h|false|false|false|true",
            "%e%c%Y|false|false|false|true"})
    public void testFactsOfAFormat(String format, boolean ordered, boolean readAsWritten, boolean readAsPeriodStart,
                                   boolean exact) {
        DateFormatModel model = DateFormatModel.of(format);
        Assertions.assertEquals(ordered, model.isOrdered(), format);
        Assertions.assertEquals(exact, model.isExactOnAllRows(), format);
        if (ordered) {
            Assertions.assertEquals(readAsWritten, model.isReadAsWrittenBy(StatisticsDatetimeParser::parse), format);
            Assertions.assertEquals(readAsPeriodStart, model.isReadAsPeriodStartBy(StatisticsDatetimeParser::parse),
                    format);
        } else {
            Assertions.assertFalse(model.isReadAsPeriodStartBy(StatisticsDatetimeParser::parse), format);
        }
    }

    @Test
    public void testJavaStyleFormatsAreReadAsTheyAreWithoutTrimming() {
        Assertions.assertTrue(DateFormatModel.of("yyyy-MM-dd").isOrdered());
        Assertions.assertEquals(Set.of(Field.YEAR, Field.MONTH, Field.DAY), DateFormatModel.of("yyyyMMdd").fields());
        Assertions.assertEquals(Set.of(Field.YEAR, Field.MONTH, Field.DAY, Field.HOUR, Field.MINUTE, Field.SECOND),
                DateFormatModel.of("yyyy-MM-dd HH:mm:ss").fields());
        // With a space around it the BE reads the text as it is: it renders itself, and no field depends on it
        for (String format : List.of(" yyyy-MM-dd", "yyyy-MM-dd ", " yyyy-MM-dd HH:mm:ss ")) {
            DateFormatModel model = DateFormatModel.of(format);
            Assertions.assertFalse(model.isOrdered(), format);
            Assertions.assertTrue(model.hasNoFields(), format);
            Assertions.assertEquals(format.length(), model.maxRenderedLength(), format);
        }
    }

    @Test
    public void testPercentSignsAndUnknownSpecifiers() {
        Assertions.assertTrue(DateFormatModel.of("%%Y").hasNoFields());
        Assertions.assertEquals(2, DateFormatModel.of("%%Y").maxRenderedLength());
        Assertions.assertEquals(Set.of(Field.MONTH), DateFormatModel.of("%%Y-%m").fields());
        // A single % at the end is a character of the string
        DateFormatModel trailing = DateFormatModel.of("%Y%");
        Assertions.assertTrue(trailing.endsWithSinglePercent());
        Assertions.assertEquals(5, trailing.maxRenderedLength());
        Assertions.assertFalse(DateFormatModel.of("%Y-%m").endsWithSinglePercent());
        Assertions.assertFalse(DateFormatModel.of("%Y%%").endsWithSinglePercent());
        // The BE writes a specifier that it does not know as text, so the model of such a format is not usable
        for (String format : List.of("%Y-%q", "%z", "%Y%Z")) {
            DateFormatModel model = DateFormatModel.of(format);
            Assertions.assertFalse(model.isKnown(), format);
            Assertions.assertFalse(model.isOrdered(), format);
        }
        Assertions.assertTrue(DateFormatModel.of("").hasNoFields());
        Assertions.assertTrue(DateFormatModel.of("").isExactOnAllRows());
    }

    @Test
    public void testResolutionAndTheFieldsOfADate() {
        Assertions.assertEquals(Optional.of(Field.MONTH), DateFormatModel.of("%Y-%m").resolution());
        Assertions.assertEquals(Optional.of(Field.DAY), DateFormatModel.of("%Y-%j").resolution());
        Assertions.assertEquals(Optional.of(Field.DAY), DateFormatModel.of("%x").resolution());
        Assertions.assertEquals(Optional.of(Field.HOUR), DateFormatModel.of("%p").resolution());
        Assertions.assertEquals(Optional.of(Field.SECOND), DateFormatModel.of("%T").resolution());
        Assertions.assertEquals(Optional.of(Field.SECOND), DateFormatModel.of("%Y %r").resolution());
        Assertions.assertEquals(Optional.of(Field.MICRO), DateFormatModel.of("%s.%f").resolution());
        Assertions.assertEquals(Optional.empty(), DateFormatModel.of("year").resolution());
        // A DATE renders as its midnight: the fields of the time are one value
        Assertions.assertEquals(Optional.of(Field.MONTH), DateFormatModel.of("%Y-%M %H").finestFieldUpTo(Field.DAY));
        Assertions.assertEquals(Optional.empty(), DateFormatModel.of("%H:%i:%s").finestFieldUpTo(Field.DAY));
        Assertions.assertEquals(Optional.empty(), DateFormatModel.of("%T").finestFieldUpTo(Field.DAY));
    }

    @Test
    public void testValueCapacityOfAFormat() {
        // 2018-11-20 to 2026-03-05 are 9 years
        Assertions.assertEquals(9, DateFormatModel.of("%Y").valueCapacity(MIN, MAX));
        Assertions.assertEquals(9, DateFormatModel.of("%y").valueCapacity(MIN, MAX));
        for (String format : List.of("%m", "%c", "%M", "%b", "%m %M")) {
            Assertions.assertEquals(12, DateFormatModel.of(format).valueCapacity(MIN, MAX), format);
        }
        Assertions.assertEquals(9 * 12, DateFormatModel.of("%Y-%m").valueCapacity(MIN, MAX));
        Assertions.assertEquals(31, DateFormatModel.of("%d").valueCapacity(MIN, MAX));
        Assertions.assertEquals(31, DateFormatModel.of("%e %D").valueCapacity(MIN, MAX));
        Assertions.assertEquals(366, DateFormatModel.of("%j").valueCapacity(MIN, MAX));
        for (String format : List.of("%a", "%W", "%w", "%a %W")) {
            Assertions.assertEquals(7, DateFormatModel.of(format).valueCapacity(MIN, MAX), format);
        }
        Assertions.assertEquals(54, DateFormatModel.of("%U").valueCapacity(MIN, MAX));
        Assertions.assertEquals(54, DateFormatModel.of("%u").valueCapacity(MIN, MAX));
        Assertions.assertEquals(53, DateFormatModel.of("%V").valueCapacity(MIN, MAX));
        Assertions.assertEquals(53, DateFormatModel.of("%v").valueCapacity(MIN, MAX));
        Assertions.assertEquals(24, DateFormatModel.of("%H").valueCapacity(MIN, MAX));
        Assertions.assertEquals(24, DateFormatModel.of("%H %k").valueCapacity(MIN, MAX));
        Assertions.assertEquals(12, DateFormatModel.of("%h").valueCapacity(MIN, MAX));
        Assertions.assertEquals(12, DateFormatModel.of("%I %l").valueCapacity(MIN, MAX));
        Assertions.assertEquals(2, DateFormatModel.of("%p").valueCapacity(MIN, MAX));
        Assertions.assertEquals(24, DateFormatModel.of("%h %p").valueCapacity(MIN, MAX));
        Assertions.assertEquals(60, DateFormatModel.of("%i").valueCapacity(MIN, MAX));
        Assertions.assertEquals(60, DateFormatModel.of("%s %S").valueCapacity(MIN, MAX));
        Assertions.assertEquals(24 * 60 * 60, DateFormatModel.of("%T").valueCapacity(MIN, MAX));
        Assertions.assertEquals(12 * 60 * 60 * 2, DateFormatModel.of("%r").valueCapacity(MIN, MAX));
        Assertions.assertEquals(1, DateFormatModel.of("year").valueCapacity(MIN, MAX));
        Assertions.assertEquals(Double.POSITIVE_INFINITY, DateFormatModel.of("%s.%f").valueCapacity(MIN, MAX));
        // The year of a week counts the weeks of the range: the week of 2019-12-30 is in 2020
        double weeks = DateFormatModel.of("%x").valueCapacity(LocalDateTime.of(2019, 1, 1, 0, 0),
                LocalDateTime.of(2019, 12, 30, 0, 0));
        Assertions.assertTrue(weeks >= 2, "" + weeks);
    }

    @Test
    public void testLengthsAreTheLongestStringAndTheFormatInBytes() {
        Assertions.assertEquals(19, DateFormatModel.of("%Y-%m-%d %H:%i:%s").maxRenderedLength());
        Assertions.assertEquals(28, DateFormatModel.of("%M-%W %Y%D").maxRenderedLength());
        Assertions.assertEquals(36, DateFormatModel.of("%Y-%M-%T %r%%").maxRenderedLength());
        // The text counts in bytes, as the BE writes it
        Assertions.assertEquals(2, DateFormatModel.of("é").maxRenderedLength());
        Assertions.assertEquals(2, DateFormatModel.of("é").formatLength());
        Assertions.assertEquals(8, DateFormatModel.of("%Y%m%d%H").formatLength());
        // Up to 126 bytes of the string and 127 bytes of the format render every row
        Assertions.assertTrue(DateFormatModel.of("a".repeat(126)).isExactOnAllRows());
        Assertions.assertFalse(DateFormatModel.of("a".repeat(127)).isExactOnAllRows());
        Assertions.assertFalse(DateFormatModel.of("a".repeat(128)).isExactOnAllRows());
        // A short format can render a long string
        DateFormatModel names = DateFormatModel.of("%M".repeat(15));
        Assertions.assertEquals(30, names.formatLength());
        Assertions.assertEquals(135, names.maxRenderedLength());
        Assertions.assertFalse(names.isExactOnAllRows());
        Assertions.assertTrue(DateFormatModel.of("%M".repeat(14)).isExactOnAllRows());
    }

    // The BE gives NULL for a string of 128 bytes or more. Only a format whose shortest string is so long gives NULL for
    // every date; a long format of escapes or of fields renders shorter strings for some or all dates.
    @Test
    public void testFormatThatNeverFitsTheBufferIsNullOnAllRows() {
        Assertions.assertTrue(DateFormatModel.of("").isNullOnAllRows());
        Assertions.assertTrue(DateFormatModel.of("a".repeat(128)).isNullOnAllRows());
        Assertions.assertTrue(DateFormatModel.of("%Y" + "a".repeat(127)).isNullOnAllRows());
        // 126 bytes of text and a field are 127 bytes, which the BE renders
        Assertions.assertFalse(DateFormatModel.of("%Y" + "a".repeat(126)).isNullOnAllRows());
        Assertions.assertFalse(DateFormatModel.of("a".repeat(127)).isNullOnAllRows());
        // %% is one byte of the string: 70 of them are a format of 140 bytes that renders 70
        Assertions.assertFalse(DateFormatModel.of("%%".repeat(70)).isNullOnAllRows());
        Assertions.assertEquals(140, DateFormatModel.of("%%".repeat(70)).formatLength());
        // the names of the months are 3 bytes at least, but each specifier counts as one byte
        Assertions.assertFalse(DateFormatModel.of("%M".repeat(64)).isNullOnAllRows());
        Assertions.assertFalse(DateFormatModel.of("%Y-%m-%d").isNullOnAllRows());
    }

    @Test
    public void testOrderedFormatRendersAndReadsItsStrings() {
        LocalDateTime time = LocalDateTime.of(2024, 3, 5, 7, 8, 9, 123_456_000);
        DateFormatModel full = DateFormatModel.of("%Y-%m-%dT%H:%i:%s");
        Assertions.assertEquals("2024-03-05T07:08:09", full.render(time));
        Assertions.assertEquals(time.withNano(0), full.read("2024-03-05T07:08:09"));
        Assertions.assertNull(full.read("2024-03-05 07:08:09"));
        Assertions.assertNull(full.read("2024-3-05T07:08:09"));
        Assertions.assertNull(full.read("2024-02-30T07:08:09"));
        Assertions.assertEquals("2024-03-05T07:08:09", DateFormatModel.of("%Y-%m-%dT%H:%i:%S").render(time));
        DateFormatModel tail = DateFormatModel.of("%Y-%m-01 00:00:00");
        Assertions.assertEquals("2024-03-01 00:00:00", tail.render(time));
        Assertions.assertEquals(LocalDateTime.of(2024, 3, 1, 0, 0), tail.read("2024-03-01 00:00:00"));
        Assertions.assertNull(tail.read("2024-03-15 00:00:00"));
        Assertions.assertEquals(ChronoUnit.MONTHS, tail.precision());
        Assertions.assertTrue(tail.hasDay());
        Assertions.assertEquals(ChronoUnit.MICROS, DateFormatModel.of("%Y%m%d%H%i%s%f").precision());
        Assertions.assertFalse(DateFormatModel.of("%Y-%m").hasDay());
        Assertions.assertTrue(DateFormatModel.of("%Y-01-01").hasDay());
    }

    @Test
    public void testDigitsOnlyFormats() {
        Assertions.assertTrue(DateFormatModel.of("%Y%m%d").isDigitsOnly());
        Assertions.assertEquals(8, DateFormatModel.of("%Y%m%d").digitCount());
        Assertions.assertEquals(10, DateFormatModel.of("%Y%m%d%H").digitCount());
        Assertions.assertEquals(14, DateFormatModel.of("%Y%m%d%H%i%s").digitCount());
        Assertions.assertEquals(20, DateFormatModel.of("%Y%m%d%H%i%s%f").digitCount());
        Assertions.assertFalse(DateFormatModel.of("%Y-%m-%d").isDigitsOnly());
        Assertions.assertFalse(DateFormatModel.of("%Y%m-01").isDigitsOnly());
        Assertions.assertFalse(DateFormatModel.of("%d%m%Y").isDigitsOnly());
    }
}
