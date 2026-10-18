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

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
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
    // The specifiers a format of a column may have, from the year down
    private static final char[] SYMBOLS = {'Y', 'm', 'd', 'H', 'i', 's', 'f'};

    private final String pattern;
    private final DateFormatModel model;

    private StringDateFormat(String pattern, DateFormatModel model) {
        this.pattern = pattern;
        this.model = model;
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
        DateFormatModel model = DateFormatModel.of(pattern);
        if (model.endsWithSinglePercent()) {
            throw new IllegalArgumentException("the pattern ends with a single %");
        }
        if (!model.isKnown()) {
            throw new IllegalArgumentException("the pattern has a specifier that is not one of %Y %m %d %H %i %s %f");
        }
        List<Character> symbols = model.specifierSymbols();
        for (int i = 0; i < symbols.size(); i++) {
            char symbol = symbols.get(i);
            int field = new String(SYMBOLS).indexOf(symbol);
            if (field < 0) {
                throw new IllegalArgumentException("%" + symbol + " is not one of %Y %m %d %H %i %s %f");
            }
            if (field != i) {
                throw new IllegalArgumentException("%" + symbol + " does not follow "
                        + "%" + SYMBOLS[i - 1] + ": the fields must go from %Y down without a gap");
            }
        }
        for (String constant : model.texts()) {
            for (int j = 0; j + 1 < constant.length(); j++) {
                char sign = constant.charAt(j);
                if ((sign == '+' || sign == '-') && Character.isDigit(constant.charAt(j + 1))) {
                    throw new IllegalArgumentException("the pattern holds a time zone offset");
                }
            }
        }
        return new StringDateFormat(pattern, model);
    }

    public String getPattern() {
        return pattern;
    }

    /** The model of the pattern, see DateFormatModel. */
    public DateFormatModel getModel() {
        return model;
    }

    /** The unit of the last field: the strings of the format hold times of this precision. */
    public ChronoUnit getPrecision() {
        return model.precision();
    }

    /**
     * Whether the strings hold a day: the BE casts a string without one to NULL. A text after the last field that starts
     * with the omitted fields at their start, as '-01' after the month, is a day.
     */
    public boolean hasDay() {
        return model.hasDay();
    }

    public boolean hasUtcSuffix() {
        return model.endsWithUtcSuffix();
    }

    public boolean isSupportedInCurrentTimezone() {
        return model.isSupportedInCurrentTimezone();
    }

    /** The string of a time with a year from 0 to 9999. The fields below the precision are dropped. */
    public String format(LocalDateTime time) {
        return model.render(time);
    }

    /**
     * Whether a string is a string of this format: it reads as a valid time, and that time prints back to the same
     * string.
     */
    public boolean matches(String value) {
        return model.read(value) != null;
    }

    /**
     * Whether the BE casts the strings of this format to the times they hold, see DateFormatModel.isReadAsWrittenBy.
     *
     * @param backend the DATETIME the BE casts a string to, or null when the BE gives NULL
     */
    public boolean isReadAsWrittenBy(Function<String, LocalDateTime> backend) {
        return model.isReadAsWrittenBy(backend);
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
