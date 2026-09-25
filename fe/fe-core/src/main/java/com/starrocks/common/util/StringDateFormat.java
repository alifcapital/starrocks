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
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

/** Fixed-width date encodings whose lexical order agrees with calendar order. */
public enum StringDateFormat {
    COMPACT_DATE("%Y%m%d", "uuuuMMdd", ChronoUnit.DAYS),
    DATE("%Y-%m-%d", "uuuu-MM-dd", ChronoUnit.DAYS),
    COMPACT_DATETIME("%Y%m%d%H%i%s", "uuuuMMddHHmmss", ChronoUnit.SECONDS),
    DATETIME("%Y-%m-%d %H:%i:%s", "uuuu-MM-dd HH:mm:ss", ChronoUnit.SECONDS),
    ISO_DATETIME("%Y-%m-%dT%H:%i:%s", "uuuu-MM-dd'T'HH:mm:ss", ChronoUnit.SECONDS),
    DATETIME_MICROS("%Y-%m-%d %H:%i:%s.%f", "uuuu-MM-dd HH:mm:ss.SSSSSS", ChronoUnit.MICROS),
    ISO_DATETIME_MICROS("%Y-%m-%dT%H:%i:%s.%f", "uuuu-MM-dd'T'HH:mm:ss.SSSSSS", ChronoUnit.MICROS),
    ISO_DATETIME_UTC("%Y-%m-%dT%H:%i:%sZ", "uuuu-MM-dd'T'HH:mm:ss'Z'", ChronoUnit.SECONDS),
    ISO_DATETIME_MICROS_UTC("%Y-%m-%dT%H:%i:%s.%fZ", "uuuu-MM-dd'T'HH:mm:ss.SSSSSS'Z'", ChronoUnit.MICROS);

    private final String sqlFormat;
    private final DateTimeFormatter formatter;
    private final ChronoUnit precision;

    StringDateFormat(String sqlFormat, String javaFormat, ChronoUnit precision) {
        this.sqlFormat = sqlFormat;
        this.formatter = new DateTimeFormatterBuilder().appendPattern(javaFormat)
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                .toFormatter(Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);
        this.precision = precision;
    }

    public static StringDateFormat fromFormat(String format) {
        for (StringDateFormat candidate : values()) {
            if (candidate.sqlFormat.equals(format)) {
                return candidate;
            }
        }
        return null;
    }

    public String getSqlFormat() {
        return sqlFormat;
    }

    public ChronoUnit getPrecision() {
        return precision;
    }

    public boolean hasUtcSuffix() {
        return this == ISO_DATETIME_UTC || this == ISO_DATETIME_MICROS_UTC;
    }

    public boolean isSupportedInCurrentTimezone() {
        // BE keeps the fields of a Z-suffixed string. FE constant folding converts it
        // into the JVM timezone. Both agree only when that zone is fixed UTC.
        return !hasUtcSuffix() || ZoneId.systemDefault().normalized().equals(ZoneOffset.UTC);
    }

    public boolean matches(String value) {
        try {
            LocalDateTime date = LocalDateTime.parse(value, formatter);
            return date.getYear() >= 0 && date.getYear() <= 9999 && format(date).equals(value);
        } catch (DateTimeException e) {
            return false;
        }
    }

    public String format(LocalDateTime value) {
        return formatter.format(value);
    }
}
