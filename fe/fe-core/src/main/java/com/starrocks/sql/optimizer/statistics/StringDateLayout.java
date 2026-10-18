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

import java.time.LocalDateTime;

/**
 * Strings of dates read in the order of the dates. Strings of one layout, the same separators at the same places
 * and fields of the same widths, sort as the dates they hold, so the smallest and the largest string of a column
 * are the smallest and the largest date. Strings of different layouts do not: '20261001' sorts before
 * '2026-01-01', and '2026-08-01 10:00:00' before '2026-08-01T09:00:00'. The digits of a fraction of a second may
 * differ in number: they change the order inside one second only.
 */
final class StringDateLayout {
    private StringDateLayout() {
    }

    /** The layout of a string: each digit as '9', the digits of a fraction of a second as one '9'. */
    static String layout(String text) {
        String bytes = StatisticsDatetimeParser.bytes(text);
        StringBuilder layout = new StringBuilder(bytes.length());
        boolean time = false;
        boolean fraction = false;
        for (int i = 0; i < bytes.length(); i++) {
            char c = bytes.charAt(i);
            if (StatisticsDatetimeParser.isDigit(c)) {
                if (!fraction || layout.charAt(layout.length() - 1) != '9') {
                    layout.append('9');
                }
                continue;
            }
            fraction = c == '.' && time && i > 0 && StatisticsDatetimeParser.isDigit(bytes.charAt(i - 1));
            time |= c == ':';
            layout.append(c);
        }
        return layout.toString();
    }

    /** Whether a string of a date holds a time: more digits than the eight of YYYYMMDD. */
    static boolean hasTime(String text) {
        String bytes = StatisticsDatetimeParser.bytes(text);
        int digits = 0;
        for (int i = 0; i < bytes.length(); i++) {
            if (StatisticsDatetimeParser.isDigit(bytes.charAt(i)) && ++digits > 8) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether two strings share a layout. A shorter layout that starts the longer one is shared only where the
     * shorter string ends a field, a separator following it in the longer one, or where the shorter string is a
     * bound that may be cut: '2026-08-01' and '2026-08-01 10:00' sort as their dates, while '201231' (2020-12-31)
     * and '20240101' do not, as '20130101' lies between them.
     *
     * @param leftCut  whether the left string is a bound that may be a cut value
     * @param rightCut whether the right string is a bound that may be a cut value
     */
    static boolean sameLayout(String left, boolean leftCut, String right, boolean rightCut) {
        String leftLayout = layout(left);
        String rightLayout = layout(right);
        if (leftLayout.equals(rightLayout)) {
            return true;
        }
        boolean leftShorter = leftLayout.length() < rightLayout.length();
        String shorter = leftShorter ? leftLayout : rightLayout;
        String longer = leftShorter ? rightLayout : leftLayout;
        return longer.startsWith(shorter)
                && ((leftShorter ? leftCut : rightCut) || longer.charAt(shorter.length()) != '9');
    }

    /**
     * The text an upper bound stands for. An Iceberg upper bound that may be cut has its last character increased
     * by one, so a last ':' is a '9' increased, another last character that is not a digit is dropped, and a digit
     * stays: it is an upper bound of its own digit.
     */
    static String upperText(StringBounds bounds) {
        String max = bounds.max();
        if (!bounds.incrementedMax() || !bounds.maxMayBeCut() || max.isEmpty()) {
            return max;
        }
        char last = max.charAt(max.length() - 1);
        if (last == ':') {
            return max.substring(0, max.length() - 1) + '9';
        }
        return StatisticsDatetimeParser.isDigit(last) ? max : max.substring(0, max.length() - 1);
    }

    /**
     * The largest DATETIME the upper bound stands for, or null when it cannot be read. A whole value reads as the
     * BE reads it. A cut Iceberg bound stands for the end of its last full field: a field is full when a separator
     * follows it or it has its full width, four digits for the year and two for the others. Its last field may
     * be cut, and then the values reach to the end of the field before it: '26-10-13 02:52:5' stands for values up
     * to 02:52:59.999999.
     */
    static LocalDateTime upper(StringBounds bounds) {
        if (!bounds.incrementedMax() || !bounds.maxMayBeCut()) {
            return StatisticsDatetimeParser.parse(bounds.max());
        }
        String text = StatisticsDatetimeParser.bytes(upperText(bounds));
        // The digit runs: the fields year, month, day, hour, minute, second and fraction of a separated layout
        int[] ends = new int[8];
        int[] lengths = new int[8];
        int fields = 0;
        int i = 0;
        while (i < text.length() && fields < ends.length) {
            if (!StatisticsDatetimeParser.isDigit(text.charAt(i))) {
                i++;
                continue;
            }
            int start = i;
            while (i < text.length() && StatisticsDatetimeParser.isDigit(text.charAt(i))) {
                i++;
            }
            ends[fields] = i;
            lengths[fields] = i - start;
            fields++;
        }
        // A string of digits alone reads by its length, see date::from_string: no field ends there
        if (fields < 2) {
            return null;
        }
        int last = fields - 1;
        boolean lastFull = ends[last] < text.length() || lengths[last] >= (last == 0 ? 4 : last == 6 ? 6 : 2);
        int full = lastFull ? last : last - 1;
        // The BE reads a date from three fields at least
        if (full < 2) {
            return null;
        }
        LocalDateTime value = StatisticsDatetimeParser.parse(text.substring(0, ends[full]));
        if (value == null) {
            return null;
        }
        switch (full) {
            case 2:
                return value.withHour(23).withMinute(59).withSecond(59).withNano(999_999_000);
            case 3:
                return value.withMinute(59).withSecond(59).withNano(999_999_000);
            case 4:
                return value.withSecond(59).withNano(999_999_000);
            case 5:
                return value.withNano(999_999_000);
            default:
                return value;
        }
    }
}
