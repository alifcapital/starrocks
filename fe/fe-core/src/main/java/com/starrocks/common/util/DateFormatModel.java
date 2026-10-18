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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * What a format of date_format renders, read from the string of the format by one tokenizer of the BE format, see
 * DateTimeValue::to_format_string. We want every place that reasons about a format to ask this model, so each fact about
 * a format is decided in one place and cannot differ between the bounds of a scan, the statistics of date_format and the
 * images of a function. The facts are:
 * <ul>
 * <li>the fields of a date that the string depends on, and the finest of them, the resolution: the string is a function
 * of the date cut to its resolution, so the periods of the resolution in a range of dates are at least the strings;
 * <li>the number of values that the fields give, and the length of the longest string and of the format;
 * <li>whether the strings sort in the order of their dates (ordered), whether the BE reads them as the dates they hold
 * or as the start of their period, and whether the BE renders every row (exact), which a bound on the strings needs;
 * <li>the string that the BE gives for one date, see fold: the FE folds a constant with it, so the strings of the ends of
 * a range, which an image and a bound are made of, are the strings of the rows.
 * </ul>
 * The BE compares the format with the three Java style formats as they are, without trimming it, and reads every other
 * format as a pattern with percent signs: %% is a percent sign, a single % at the end is a character, and a character
 * that is no specifier is text.
 * <p>
 * The BE writes the string into a buffer and takes it back as a C string, so it returns the string up to the first zero
 * byte. We fear that a bound on the strings of a format with a zero byte would replace a condition by another one, because
 * the model would hold the text after the zero byte as a part of every string. So such a format is outside of the model:
 * it is not ordered and not exact, see hasZeroByte. We expect no real format to have a zero byte, and the cut would need a
 * second view of every fact, as the BE still counts the bytes after the zero byte for its NULL rows.
 */
public final class DateFormatModel {
    /** The fields of a date, from the coarsest to the finest. */
    public enum Field {
        YEAR(ChronoUnit.YEARS),
        MONTH(ChronoUnit.MONTHS),
        DAY(ChronoUnit.DAYS),
        HOUR(ChronoUnit.HOURS),
        MINUTE(ChronoUnit.MINUTES),
        SECOND(ChronoUnit.SECONDS),
        MICRO(ChronoUnit.MICROS);

        private final ChronoUnit unit;

        Field(ChronoUnit unit) {
            this.unit = unit;
        }

        public ChronoUnit unit() {
            return unit;
        }
    }

    /**
     * The functions of the BE that render a date with a format, each with the longest constant format it takes. We fear
     * that a reader would take a format that a function fails on: from_unixtime fails to prepare a constant format of more
     * than 100 bytes, and a plan that prunes every partition could then return no rows where the BE returns an error. So a
     * reader asks for the function it reads. date_format has no such limit, it renders a long format row by row, see
     * isExactOnAllRows.
     */
    public enum Renderer {
        DATE_FORMAT(Integer.MAX_VALUE),
        FROM_UNIXTIME(100);

        private final int maxFormatLength;

        Renderer(int maxFormatLength) {
            this.maxFormatLength = maxFormatLength;
        }
    }

    // The number of values of a group of specifiers that give the same value, such as %m, %c, %M and %b, is a number, or
    // depends on the range of the dates.
    private static final double YEARS_OF_RANGE = -1;
    private static final double WEEKS_OF_RANGE = -2;

    /** The values that a group of specifiers that give the same value can take. */
    private record Group(String name, double capacity) {
    }

    /** A specifier of the BE: the field it depends on, the longest string it renders and the groups it gives values of. */
    private record Specifier(char symbol, Field field, int width, List<Group> groups) {
        Specifier(char symbol, Field field, int width, String group, double capacity) {
            this(symbol, field, width, List.of(new Group(group, capacity)));
        }
    }

    private static final Map<Character, Specifier> SPECIFIERS;

    static {
        Map<Character, Specifier> table = new HashMap<>();
        for (char symbol : new char[] {'Y', 'y'}) {
            table.put(symbol, new Specifier(symbol, Field.YEAR, symbol == 'Y' ? 4 : 2, "year", YEARS_OF_RANGE));
        }
        table.put('m', new Specifier('m', Field.MONTH, 2, "month", 12));
        table.put('c', new Specifier('c', Field.MONTH, 2, "month", 12));
        table.put('M', new Specifier('M', Field.MONTH, 9, "month", 12));
        table.put('b', new Specifier('b', Field.MONTH, 3, "month", 12));
        table.put('d', new Specifier('d', Field.DAY, 2, "day-of-month", 31));
        table.put('e', new Specifier('e', Field.DAY, 2, "day-of-month", 31));
        table.put('D', new Specifier('D', Field.DAY, 4, "day-of-month", 31));
        table.put('j', new Specifier('j', Field.DAY, 3, "day-of-year", 366));
        table.put('a', new Specifier('a', Field.DAY, 3, "day-of-week", 7));
        table.put('W', new Specifier('W', Field.DAY, 9, "day-of-week", 7));
        table.put('w', new Specifier('w', Field.DAY, 1, "day-of-week", 7));
        // 00..53 for the weeks that start on a Sunday or a Monday, 01..53 for the weeks of the year of the week
        table.put('U', new Specifier('U', Field.DAY, 2, "week-sunday", 54));
        table.put('u', new Specifier('u', Field.DAY, 2, "week-monday", 54));
        table.put('V', new Specifier('V', Field.DAY, 2, "week-of-year-sunday", 53));
        table.put('v', new Specifier('v', Field.DAY, 2, "week-of-year-monday", 53));
        // The year of the week is a year of the week, so we bound its values by the weeks of the range
        table.put('x', new Specifier('x', Field.DAY, 4, "year-of-week-monday", WEEKS_OF_RANGE));
        table.put('X', new Specifier('X', Field.DAY, 4, "year-of-week-sunday", WEEKS_OF_RANGE));
        table.put('H', new Specifier('H', Field.HOUR, 2, "hour-24", 24));
        table.put('k', new Specifier('k', Field.HOUR, 2, "hour-24", 24));
        table.put('h', new Specifier('h', Field.HOUR, 2, "hour-12", 12));
        table.put('I', new Specifier('I', Field.HOUR, 2, "hour-12", 12));
        table.put('l', new Specifier('l', Field.HOUR, 2, "hour-12", 12));
        table.put('p', new Specifier('p', Field.HOUR, 2, "am-pm", 2));
        table.put('i', new Specifier('i', Field.MINUTE, 2, "minute", 60));
        table.put('s', new Specifier('s', Field.SECOND, 2, "second", 60));
        table.put('S', new Specifier('S', Field.SECOND, 2, "second", 60));
        // %T is %H:%i:%s and %r is %h:%i:%s %p, whose values are the values of their fields
        table.put('T', new Specifier('T', Field.SECOND, 8,
                List.of(new Group("hour-24", 24), new Group("minute", 60), new Group("second", 60))));
        table.put('r', new Specifier('r', Field.SECOND, 11,
                List.of(new Group("hour-12", 12), new Group("minute", 60), new Group("second", 60),
                        new Group("am-pm", 2))));
        table.put('f', new Specifier('f', Field.MICRO, 6, "micro", Double.POSITIVE_INFINITY));
        SPECIFIERS = Map.copyOf(table);
    }

    // The Java style formats that the BE reads as they are, with the formats with percent signs that render the same
    private static final Map<String, String> JAVA_STYLE = Map.of(
            "yyyy-MM-dd", "%Y-%m-%d",
            "yyyyMMdd", "%Y%m%d",
            "yyyy-MM-dd HH:mm:ss", "%Y-%m-%d %H:%i:%s");

    // The specifiers of an ordered format, from the year down: the year has four digits, the others two, the fraction six
    private static final char[] ORDERED_SYMBOLS = {'Y', 'm', 'd', 'H', 'i', 's', 'f'};
    private static final int[] ORDERED_WIDTHS = {4, 2, 2, 2, 2, 2, 6};

    // The BE writes at most 127 bytes of a string and gives NULL when the string needs more room. A format of 128 bytes or
    // longer renders row by row with the same result, and we expect such a format to hold a long text, so we do not call it
    // exact.
    private static final int MAX_RENDERED_LENGTH = 127;
    private static final int MAX_FORMAT_LENGTH = 128;
    // The shortest string that never fits the buffer of the BE: it holds 127 bytes and the terminating zero
    private static final int NEVER_FITS_LENGTH = 128;

    // The specifiers that the FE does not render: the names, the English suffix of %D, the weeks and the years of the
    // weeks. We fear that a string which differs from the string of the BE gets into a plan as the string of a row, and the
    // library of the FE has no rendering of these that matches the BE. So the call with such a specifier stays for the BE.
    private static final String NOT_FOLDED = "aDbMprUuVWwXx";

    // Times with different digits in every field. A string of only digits whose length is not 4, 8 or 14 and more reads
    // in the BE with a year of two digits, and 2012 and 2001 make such a reading a valid date of another day.
    private static final List<LocalDateTime> PROBES = List.of(
            LocalDateTime.of(2012, 3, 5, 7, 8, 9, 123_456_000),
            LocalDateTime.of(2001, 10, 11, 12, 13, 14, 15_016_000),
            LocalDateTime.of(2024, 12, 31, 23, 59, 58, 999_999_000));

    // We expect a few distinct formats in the queries of a cluster, and each of them is read many times by the paths of
    // the planner, so a format is read once.
    private static final Cache<String, DateFormatModel> MODELS = Caffeine.newBuilder().maximumSize(1000).build();

    /** A piece of the format: a specifier, or the text that renders as it is. */
    private record Token(Specifier specifier, String text) {
    }

    private final String format;
    private final List<Token> tokens;
    private final boolean known;
    private final boolean endsWithSinglePercent;
    private final Set<Field> fields;
    private final int maxRenderedLength;
    // The text and one byte for each specifier: no string of the format is shorter
    private final int minRenderedLength;
    private final int formatLength;
    // The number of specifiers of an ordered format, from the year down; 0 when the format is not ordered
    private final int orderedFields;
    private final boolean hasText;
    private final boolean zeroByte;

    private DateFormatModel(String format) {
        this.format = format;
        // No specifier writes a zero byte, so one reaches the string only from the format: as text, or after a % as the
        // character of an unknown specifier
        this.zeroByte = format.indexOf('\0') >= 0;
        String pattern = JAVA_STYLE.getOrDefault(format, format);
        List<Token> parsed = new ArrayList<>();
        StringBuilder text = new StringBuilder();
        boolean allKnown = true;
        boolean singlePercent = false;
        int i = 0;
        while (i < pattern.length()) {
            int codePoint = pattern.codePointAt(i);
            int next = i + Character.charCount(codePoint);
            if (codePoint != '%' || next >= pattern.length()) {
                // A single % at the end is a character of the string, as the BE writes it
                singlePercent |= codePoint == '%';
                text.appendCodePoint(codePoint);
                i = next;
                continue;
            }
            char symbol = pattern.charAt(next);
            i = next + 1;
            if (symbol == '%') {
                text.append('%');
                continue;
            }
            Specifier specifier = SPECIFIERS.get(symbol);
            if (specifier == null) {
                // The BE writes the character of a specifier that it does not know, and no field depends on it
                allKnown = false;
                text.append(symbol);
                continue;
            }
            if (text.length() > 0) {
                parsed.add(new Token(null, text.toString()));
                text.setLength(0);
            }
            parsed.add(new Token(specifier, null));
        }
        if (text.length() > 0) {
            parsed.add(new Token(null, text.toString()));
        }
        this.tokens = List.copyOf(parsed);
        this.known = allKnown;
        this.endsWithSinglePercent = singlePercent;
        Set<Field> present = EnumSet.noneOf(Field.class);
        int length = 0;
        int shortest = 0;
        boolean anyText = false;
        for (Token token : tokens) {
            if (token.specifier() != null) {
                present.add(token.specifier().field());
                length += token.specifier().width();
                shortest += 1;
            } else {
                int bytes = token.text().getBytes(StandardCharsets.UTF_8).length;
                length += bytes;
                shortest += bytes;
                anyText = true;
            }
        }
        this.fields = present;
        this.maxRenderedLength = length;
        this.minRenderedLength = shortest;
        this.formatLength = format.getBytes(StandardCharsets.UTF_8).length;
        this.hasText = anyText;
        this.orderedFields = zeroByte ? 0 : orderedFieldsOf(tokens, allKnown);
    }

    // The fields from the year down without a gap and with the width of an ordered format, or 0
    private static int orderedFieldsOf(List<Token> tokens, boolean allKnown) {
        if (!allKnown) {
            return 0;
        }
        int count = 0;
        for (Token token : tokens) {
            if (token.specifier() == null) {
                continue;
            }
            char symbol = token.specifier().symbol() == 'S' ? 's' : token.specifier().symbol();
            if (count == ORDERED_SYMBOLS.length || symbol != ORDERED_SYMBOLS[count]) {
                return 0;
            }
            count++;
        }
        return count;
    }

    /** The model of a format. */
    public static DateFormatModel of(String format) {
        if (format == null) {
            throw new IllegalArgumentException("the format is null");
        }
        return MODELS.get(format, DateFormatModel::new);
    }

    public String getFormat() {
        return format;
    }

    /** Whether every specifier of the format is one of the BE. The BE writes any other one as text. */
    public boolean isKnown() {
        return known;
    }

    /** Whether the format ends with a single %, which the BE writes as a character of the string. */
    public boolean endsWithSinglePercent() {
        return endsWithSinglePercent;
    }

    /**
     * Whether the format has a zero byte. The BE returns the string up to it, and the FE keeps the whole string, so no
     * string of such a format is the rendering that the tokens of the model describe. The model gives no order and no
     * exactness to the format. The distinct values, the lengths and the periods stay upper bounds, as the cut only joins
     * and shortens strings. The NULL rows do not change: the BE counts the bytes after the zero byte, so isNullOnAllRows
     * reads the whole format.
     */
    public boolean hasZeroByte() {
        return zeroByte;
    }

    /** Whether the format has text only, and renders one string. */
    public boolean hasNoFields() {
        return fields.isEmpty();
    }

    /** The fields of a date that the string depends on. */
    public Set<Field> fields() {
        return fields;
    }

    /** The finest field that a specifier depends on; empty when the format has none. */
    public Optional<Field> resolution() {
        return finestFieldUpTo(Field.MICRO);
    }

    /**
     * The finest field that is not finer than the limit; empty when the format has no such field, and then it renders
     * one string. The limit of a DATE is the day, as the BE renders a DATE as its midnight.
     */
    public Optional<Field> finestFieldUpTo(Field limit) {
        Field finest = null;
        for (Field field : fields) {
            if (field.compareTo(limit) <= 0 && (finest == null || field.compareTo(finest) > 0)) {
                finest = field;
            }
        }
        return Optional.ofNullable(finest);
    }

    /** The length in bytes of the longest string that the format renders. */
    public int maxRenderedLength() {
        return maxRenderedLength;
    }

    /** The length of the format in bytes. */
    public int formatLength() {
        return formatLength;
    }

    /**
     * Whether the BE renders every row: it gives NULL for a string that does not fit its buffer of 128 bytes. We count
     * a string of 127 bytes as not fitting, because the BE also gives NULL a few bytes before the end of the buffer for
     * the names of the days and months. We take a format of 128 bytes or more as not exact too: the BE renders it row
     * by row, and we expect such a format to hold a long text. We take a format with a zero byte as not exact, because the
     * BE cuts its strings there, see hasZeroByte. The BE gives NULL for every row of the empty format, so that format is not
     * exact either, see isNullOnAllRows. A bound on the strings is right only for an exact format.
     */
    public boolean isExactOnAllRows() {
        return formatLength > 0 && !zeroByte && maxRenderedLength < MAX_RENDERED_LENGTH && formatLength < MAX_FORMAT_LENGTH;
    }

    /**
     * Whether the function takes the format as a constant: it runs and does not fail on it. The length is in bytes, as the
     * BE counts it before it reads the format.
     */
    public boolean isTakenBy(Renderer renderer) {
        return formatLength <= renderer.maxFormatLength;
    }

    /**
     * Whether the strings of the function follow the order of the dates, so the strings of the ends of a range of dates
     * bound the strings of the range. The FE folds the ends and the BE renders the rows, and a bound is right only when both
     * give the same string. So we admit a format when it is known, has no zero byte (the FE keeps it in the string, the BE
     * cuts the string there), is taken by the function, and has text only or is ordered.
     */
    public boolean isMonotonicFor(Renderer renderer) {
        return known && !zeroByte && isTakenBy(renderer) && (fields.isEmpty() || isOrdered());
    }

    /**
     * Whether the BE gives NULL for every date: an empty format, or a format whose shortest string does not fit the
     * buffer of 128 bytes of the BE. We expect a long text in the format, and then we know the NULL fraction is 1. A format
     * whose strings are long only for some dates is not here: its NULL rows are not counted.
     */
    public boolean isNullOnAllRows() {
        return formatLength == 0 || minRenderedLength >= NEVER_FITS_LENGTH;
    }

    /**
     * An upper bound of the number of distinct strings of dates in a range: the product of the values of the groups of
     * specifiers that the format has, such as 12 for the months, and the years or the weeks of the range for the
     * specifiers that count them. Infinite when the format has the fraction of a second.
     */
    public double valueCapacity(LocalDateTime min, LocalDateTime max) {
        Set<String> groups = new HashSet<>();
        double capacity = 1;
        for (Token token : tokens) {
            if (token.specifier() == null) {
                continue;
            }
            for (Group group : token.specifier().groups()) {
                if (!groups.add(group.name())) {
                    continue;
                }
                double values = group.capacity();
                if (values == YEARS_OF_RANGE) {
                    values = max.getYear() - min.getYear() + 1;
                } else if (values == WEEKS_OF_RANGE) {
                    values = ChronoUnit.DAYS.between(min.toLocalDate(), max.toLocalDate()) / 7 + 2;
                }
                capacity *= values;
            }
        }
        return capacity;
    }

    /**
     * Whether the strings sort in the order of the dates they hold: the specifiers are %Y %m %d %H %i %s %f from the year
     * down without a gap, %S standing for %s, and the text between them and after them is constant. Each field has a
     * fixed width with leading zeros: four digits for %Y, six for %f, two for the others. Two strings of one such format
     * differ first in a digit of a field, and the higher fields are equal before it. A format with a zero byte is not
     * ordered, see hasZeroByte.
     */
    public boolean isOrdered() {
        return orderedFields > 0;
    }

    /** The unit of the last field of an ordered format: the strings hold dates of this precision. */
    public ChronoUnit precision() {
        return Field.values()[orderedFields - 1].unit();
    }

    /**
     * Whether the strings hold a day: the BE casts a string without one to NULL. A text after the last field that starts
     * with the start of the omitted fields, as '-01' after the month of '%Y-%m-01', is a day.
     */
    public boolean hasDay() {
        if (orderedFields > Field.DAY.ordinal()) {
            return true;
        }
        if (orderedFields == 0) {
            return false;
        }
        Token last = tokens.get(tokens.size() - 1);
        if (last.specifier() != null) {
            return false;
        }
        return last.text().startsWith(orderedFields == 1 ? "-01-01" : "-01");
    }

    /** Whether the text of the last piece of an ordered format ends with Z, which the BE reads as the UTC. */
    public boolean endsWithUtcSuffix() {
        if (tokens.isEmpty()) {
            return false;
        }
        Token last = tokens.get(tokens.size() - 1);
        return last.specifier() == null && last.text().endsWith("Z");
    }

    /** The symbols of the specifiers in the order of the format. */
    public List<Character> specifierSymbols() {
        List<Character> symbols = new ArrayList<>();
        for (Token token : tokens) {
            if (token.specifier() != null) {
                symbols.add(token.specifier().symbol());
            }
        }
        return symbols;
    }

    /**
     * Whether the strings of an ordered format are read the same by the FE and by the BE in the zone of the JVM. The BE
     * keeps the fields of a Z-suffixed string, while the constant folding of the FE converts it into the zone of the JVM,
     * so both agree only when that zone is fixed UTC.
     */
    public boolean isSupportedInCurrentTimezone() {
        return !endsWithUtcSuffix() || ZoneId.systemDefault().normalized().equals(ZoneOffset.UTC);
    }

    /** The text before each specifier of the format, and the text after the last one. */
    public List<String> texts() {
        List<String> texts = new ArrayList<>();
        StringBuilder text = new StringBuilder();
        for (Token token : tokens) {
            if (token.specifier() == null) {
                text.append(token.text());
            } else {
                texts.add(text.toString());
                text.setLength(0);
            }
        }
        texts.add(text.toString());
        return texts;
    }

    /** Whether an ordered format renders digits only, as '%Y%m%d' does. */
    public boolean isDigitsOnly() {
        return isOrdered() && !hasText;
    }

    /** The number of digits that a format of digits only renders. */
    public int digitCount() {
        int digits = 0;
        for (int i = 0; i < orderedFields; i++) {
            digits += ORDERED_WIDTHS[i];
        }
        return digits;
    }

    /** The string of a time with a year from 0 to 9999 in an ordered format. The fields below the precision are dropped. */
    public String render(LocalDateTime time) {
        StringBuilder result = new StringBuilder(formatLength + 8);
        int field = 0;
        for (Token token : tokens) {
            if (token.specifier() == null) {
                result.append(token.text());
                continue;
            }
            String digits = Integer.toString(valueOf(field, time));
            for (int pad = digits.length(); pad < ORDERED_WIDTHS[field]; pad++) {
                result.append('0');
            }
            result.append(digits);
            field++;
        }
        return result.toString();
    }

    private static int valueOf(int field, LocalDateTime time) {
        return switch (field) {
            case 0 -> time.getYear();
            case 1 -> time.getMonthValue();
            case 2 -> time.getDayOfMonth();
            case 3 -> time.getHour();
            case 4 -> time.getMinute();
            case 5 -> time.getSecond();
            default -> time.getNano() / 1000;
        };
    }

    /**
     * The string that the function gives for a time, or empty where it gives NULL. We fear that a constant, a bound or an
     * image of a plan holds a string which no row has, so the FE follows the BE over the bytes of the format, because the
     * buffer of the BE counts bytes. It has 127 bytes and a terminating zero, and the steps are those of the tokenizer
     * above, see DateTimeValue::to_format_string:
     * <ul>
     * <li>an empty format gives NULL, and a Java style form is the format with percent signs that renders the same;
     * <li>a byte of the format is copied, a % that ends the format included, and the BE gives NULL when the buffer
     * already holds 127 bytes and the format has more;
     * <li>a specifier needs its width and one byte more below the 127 bytes, or the BE gives NULL;
     * <li>the byte after a % is copied when it is no specifier, a % included, and it needs one byte more below the 127
     * bytes than the same byte as text.
     * </ul>
     * The BE renders a DATE as its midnight, so the caller passes the time of a DATE cut to the day.
     *
     * @throws IllegalArgumentException when the FE cannot give the string of the BE, and the call stays for the BE: the
     *         function fails on the format, a specifier is one that the FE does not render, or the year is outside of
     *         0000..9999
     */
    public Optional<String> fold(Renderer renderer, LocalDateTime time) {
        if (!isTakenBy(renderer)) {
            throw new IllegalArgumentException(
                    renderer + " does not take a format of more than " + renderer.maxFormatLength + " bytes");
        }
        if (time.getYear() < 0 || time.getYear() > 9999) {
            throw new IllegalArgumentException("the year " + time.getYear() + " is outside of the range of the BE");
        }
        if (formatLength == 0) {
            return Optional.empty();
        }
        byte[] pattern = JAVA_STYLE.getOrDefault(format, format).getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[MAX_RENDERED_LENGTH];
        int size = 0;
        int i = 0;
        while (i < pattern.length) {
            if (size == MAX_RENDERED_LENGTH) {
                return Optional.empty();
            }
            byte current = pattern[i++];
            if (current != '%' || i == pattern.length) {
                out[size++] = current;
                continue;
            }
            byte symbol = pattern[i++];
            Specifier specifier = symbol > 0 ? SPECIFIERS.get((char) symbol) : null;
            if (specifier == null) {
                if (size + 1 >= MAX_RENDERED_LENGTH) {
                    return Optional.empty();
                }
                out[size++] = symbol;
                continue;
            }
            if (NOT_FOLDED.indexOf(symbol) >= 0) {
                throw new IllegalArgumentException(String.format("%%%s not supported in date format string", (char) symbol));
            }
            String text = renderField((char) symbol, time);
            if (size + specifier.width() >= MAX_RENDERED_LENGTH) {
                return Optional.empty();
            }
            for (int k = 0; k < text.length(); k++) {
                out[size++] = (byte) text.charAt(k);
            }
        }
        return Optional.of(new String(out, 0, size, StandardCharsets.UTF_8));
    }

    private static String renderField(char symbol, LocalDateTime time) {
        int hour12 = (time.getHour() + 11) % 12 + 1;
        return switch (symbol) {
            case 'Y' -> pad(time.getYear(), 4);
            case 'y' -> pad(time.getYear() % 100, 2);
            case 'm' -> pad(time.getMonthValue(), 2);
            case 'c' -> Integer.toString(time.getMonthValue());
            case 'd' -> pad(time.getDayOfMonth(), 2);
            case 'e' -> Integer.toString(time.getDayOfMonth());
            case 'j' -> pad(time.getDayOfYear(), 3);
            case 'v' -> pad(time.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR), 2);
            case 'H' -> pad(time.getHour(), 2);
            case 'k' -> Integer.toString(time.getHour());
            case 'h', 'I' -> pad(hour12, 2);
            case 'l' -> Integer.toString(hour12);
            case 'i' -> pad(time.getMinute(), 2);
            case 's', 'S' -> pad(time.getSecond(), 2);
            case 'T' -> pad(time.getHour(), 2) + ":" + pad(time.getMinute(), 2) + ":" + pad(time.getSecond(), 2);
            case 'f' -> pad(time.getNano() / 1000, 6);
            default -> throw new IllegalArgumentException(String.format("%%%s not supported in date format string", symbol));
        };
    }

    private static String pad(int value, int width) {
        String digits = Integer.toString(value);
        return digits.length() >= width ? digits : "0".repeat(width - digits.length()) + digits;
    }

    /**
     * The time that a string of an ordered format holds, the fields after the last one at their start, or null when the
     * string is not a string of this format: it must read as a valid time, and that time must print back to the same
     * string.
     */
    public LocalDateTime read(String value) {
        int[] values = {0, 1, 1, 0, 0, 0, 0};
        int position = 0;
        int field = 0;
        for (Token token : tokens) {
            if (token.specifier() == null) {
                if (!value.startsWith(token.text(), position)) {
                    return null;
                }
                position += token.text().length();
                continue;
            }
            int end = position + ORDERED_WIDTHS[field];
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
            values[field++] = number;
        }
        if (position != value.length()) {
            return null;
        }
        LocalDateTime time;
        try {
            time = LocalDateTime.of(values[0], values[1], values[2], values[3], values[4], values[5], values[6] * 1000);
        } catch (DateTimeException e) {
            return null;
        }
        return render(time).equals(value) ? time : null;
    }

    /**
     * Whether the BE casts the strings of this ordered format to the times they hold. The bounds on the strings are right
     * only then: the BE reads some layouts its own way, as 202403051230 with a year of two digits, or drops a time that
     * does not follow the date after a 'T' or a space. A format without a day must give NULL in the BE, which reads a date
     * from three fields at least. The BE chooses how to read a string by its layout, and all strings of a format share
     * one, so we check a few probes with different digits in every field.
     *
     * @param backend the DATETIME the BE casts a string to, or null when the BE gives NULL
     */
    public boolean isReadAsWrittenBy(Function<String, LocalDateTime> backend) {
        for (LocalDateTime probe : PROBES) {
            LocalDateTime read = backend.apply(render(probe));
            if (hasDay() ? !StringDateFormat.truncate(probe, precision()).equals(read) : read != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether the BE casts the strings of this ordered format to the start of the period of the string, and not to NULL:
     * the cast of date_format(x, format) to a DATETIME is then the date cut to the precision of the format. A format
     * without a day gives NULL in the BE and is not read so, nor is one whose digits the BE reads another way.
     */
    public boolean isReadAsPeriodStartBy(Function<String, LocalDateTime> backend) {
        if (!isOrdered()) {
            return false;
        }
        for (LocalDateTime probe : PROBES) {
            LocalDateTime read = backend.apply(render(probe));
            if (read == null || !StringDateFormat.truncate(probe, precision()).equals(read)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return format;
    }
}
