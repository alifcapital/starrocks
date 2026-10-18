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

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * A date_format pattern whose strings sort in the order of the times they hold. We compare such strings with bounds
 * built from a time, so we accept only a pattern where the order of the strings is the order of the times:
 * - the fields are %Y %m %d %H %i %s %f, from the year down without a gap, and the pattern may stop at any field;
 * - each field has a fixed width with leading zeros: four digits for %Y, six for %f, two for the others;
 * - the text between the fields and after the last field is constant, and %% stands for '%'.
 * Two strings of one such pattern differ first in a digit of a field, and the higher fields are equal before it.
 * <p>
 * A time zone offset in the text, such as +05:00, is not accepted: the FE folds a string with an offset into the
 * time zone of the JVM, while the BE reads the fields as written.
 */
public final class StringDateFormat {
    private enum Field {
        YEAR('Y', 4, ChronoUnit.YEARS),
        MONTH('m', 2, ChronoUnit.MONTHS),
        DAY('d', 2, ChronoUnit.DAYS),
        HOUR('H', 2, ChronoUnit.HOURS),
        MINUTE('i', 2, ChronoUnit.MINUTES),
        SECOND('s', 2, ChronoUnit.SECONDS),
        MICROSECOND('f', 6, ChronoUnit.MICROS);

        private final char symbol;
        private final int width;
        private final ChronoUnit unit;

        Field(char symbol, int width, ChronoUnit unit) {
            this.symbol = symbol;
            this.width = width;
            this.unit = unit;
        }

        private static Field of(char symbol) {
            for (Field field : values()) {
                if (field.symbol == symbol) {
                    return field;
                }
            }
            return null;
        }

        private int valueOf(LocalDateTime time) {
            return switch (this) {
                case YEAR -> time.getYear();
                case MONTH -> time.getMonthValue();
                case DAY -> time.getDayOfMonth();
                case HOUR -> time.getHour();
                case MINUTE -> time.getMinute();
                case SECOND -> time.getSecond();
                case MICROSECOND -> time.getNano() / 1000;
            };
        }
    }

    private static final Field[] FIELDS = Field.values();

    // Times with different digits in every field. A string of only digits whose length is not 4, 8 or 14 and more
    // reads in the BE with a year of two digits, and 2012 and 2001 make such a reading a valid date of another day.
    private static final List<LocalDateTime> PROBES = List.of(
            LocalDateTime.of(2012, 3, 5, 7, 8, 9, 123_456_000),
            LocalDateTime.of(2001, 10, 11, 12, 13, 14, 15_016_000),
            LocalDateTime.of(2024, 12, 31, 23, 59, 58, 999_999_000));

    private final String pattern;
    // The constant text after each field; the fields are the first texts.length of FIELDS
    private final String[] texts;

    private StringDateFormat(String pattern, String[] texts) {
        this.pattern = pattern;
        this.texts = texts;
    }

    /**
     * The format of a pattern.
     *
     * @throws IllegalArgumentException with the reason when the order of the strings of the pattern is not the
     *                                  order of their times
     */
    public static StringDateFormat parse(String pattern) {
        if (pattern == null || !pattern.startsWith("%Y")) {
            throw new IllegalArgumentException("the pattern must start with %Y");
        }
        List<String> texts = new ArrayList<>();
        StringBuilder text = new StringBuilder();
        int i = 2;
        while (i < pattern.length()) {
            char c = pattern.charAt(i++);
            if (c != '%') {
                text.append(c);
                continue;
            }
            if (i == pattern.length()) {
                throw new IllegalArgumentException("the pattern ends with a single %");
            }
            char symbol = pattern.charAt(i++);
            if (symbol == '%') {
                text.append('%');
                continue;
            }
            Field field = Field.of(symbol);
            if (field == null) {
                throw new IllegalArgumentException("%" + symbol + " is not one of %Y %m %d %H %i %s %f");
            }
            if (field.ordinal() != texts.size() + 1) {
                throw new IllegalArgumentException("%" + symbol + " does not follow "
                        + "%" + FIELDS[texts.size()].symbol + ": the fields must go from %Y down without a gap");
            }
            texts.add(text.toString());
            text.setLength(0);
        }
        texts.add(text.toString());
        for (String constant : texts) {
            for (int j = 0; j + 1 < constant.length(); j++) {
                char sign = constant.charAt(j);
                if ((sign == '+' || sign == '-') && Character.isDigit(constant.charAt(j + 1))) {
                    throw new IllegalArgumentException("the pattern holds a time zone offset");
                }
            }
        }
        return new StringDateFormat(pattern, texts.toArray(new String[0]));
    }

    public String getPattern() {
        return pattern;
    }

    /** The unit of the last field: the strings of the format hold times of this precision. */
    public ChronoUnit getPrecision() {
        return FIELDS[texts.length - 1].unit;
    }

    /** Whether the strings hold a day: the BE casts a string without one to NULL. */
    public boolean hasDay() {
        return texts.length > Field.DAY.ordinal();
    }

    public boolean hasUtcSuffix() {
        return texts[texts.length - 1].endsWith("Z");
    }

    public boolean isSupportedInCurrentTimezone() {
        // BE keeps the fields of a Z-suffixed string. FE constant folding converts it
        // into the JVM timezone. Both agree only when that zone is fixed UTC.
        return !hasUtcSuffix() || ZoneId.systemDefault().normalized().equals(ZoneOffset.UTC);
    }

    /** The string of a time with a year from 0 to 9999. The fields below the precision are dropped. */
    public String format(LocalDateTime time) {
        StringBuilder result = new StringBuilder(pattern.length() + 8);
        for (int i = 0; i < texts.length; i++) {
            Field field = FIELDS[i];
            String digits = Integer.toString(field.valueOf(time));
            for (int pad = digits.length(); pad < field.width; pad++) {
                result.append('0');
            }
            result.append(digits).append(texts[i]);
        }
        return result.toString();
    }

    /**
     * Whether a string is a string of this format: it reads as a valid time, and that time prints back to the same
     * string.
     */
    public boolean matches(String value) {
        LocalDateTime time = read(value);
        return time != null && format(time).equals(value);
    }

    // The time a string holds, the fields after the last one at their start, or null when the string is not of this
    // format or does not hold a valid time.
    private LocalDateTime read(String value) {
        int[] values = {0, 1, 1, 0, 0, 0, 0};
        int position = 0;
        for (int i = 0; i < texts.length; i++) {
            int end = position + FIELDS[i].width;
            if (end > value.length()) {
                return null;
            }
            int number = 0;
            for (; position < end; position++) {
                char c = value.charAt(position);
                if (c < '0' || c > '9') {
                    return null;
                }
                number = number * 10 + (c - '0');
            }
            values[i] = number;
            if (!value.startsWith(texts[i], position)) {
                return null;
            }
            position += texts[i].length();
        }
        if (position != value.length()) {
            return null;
        }
        try {
            return LocalDateTime.of(values[0], values[1], values[2], values[3], values[4], values[5],
                    values[6] * 1000);
        } catch (DateTimeException e) {
            return null;
        }
    }

    /**
     * Whether the BE casts the strings of this format to the times they hold. The bounds on the strings are right only
     * then: the BE reads some layouts its own way, as 202403051230 with a year of two digits, or drops a time that
     * does not follow the date after a 'T' or a space. A format without a day must give NULL in the BE, which reads a
     * date from three fields at least. The BE chooses how to read a string by its layout, and all strings of a format
     * share one, so we check a few probes with different digits in every field.
     *
     * @param backend the DATETIME the BE casts a string to, or null when the BE gives NULL
     */
    public boolean isReadAsWrittenBy(Function<String, LocalDateTime> backend) {
        for (LocalDateTime probe : PROBES) {
            LocalDateTime read = backend.apply(format(probe));
            if (hasDay() ? !truncate(probe, getPrecision()).equals(read) : read != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * The start of the unit a time falls in. LocalDateTime.truncatedTo stops at days, so months and years are cut
     * here.
     */
    public static LocalDateTime truncate(LocalDateTime time, ChronoUnit unit) {
        return switch (unit) {
            case YEARS -> LocalDateTime.of(time.getYear(), 1, 1, 0, 0);
            case MONTHS -> LocalDateTime.of(time.getYear(), time.getMonth(), 1, 0, 0);
            default -> time.truncatedTo(unit);
        };
    }

    @Override
    public String toString() {
        return pattern;
    }
}
