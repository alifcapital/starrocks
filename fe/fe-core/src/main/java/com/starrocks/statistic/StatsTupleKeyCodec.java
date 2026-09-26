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

package com.starrocks.statistic;

import com.starrocks.catalog.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * FE side of the BE function stats_tuple_key: builds the call for a column group and decodes the keys
 * the BE returns. A key joins the components with '#'; inside a component '\' and '#' are escaped
 * with '\'; a NULL component is the two characters "\N", which escaping never produces for a value.
 */
public final class StatsTupleKeyCodec {
    private static final char SEPARATOR = '#';
    private static final char ESCAPE = '\\';
    private static final char NULL_MARK = 'N';

    private StatsTupleKeyCodec() {
    }

    /**
     * stats_tuple_key(cast(`c1` as varchar), cast(`c2` as varchar), ...). Every column is cast to varchar
     * on the BE, so the components use the same text form as the single-column statistics.
     */
    public static String buildKeyExpr(Table table, List<String> columnNames) {
        return "stats_tuple_key(" + columnNames.stream()
                .map(column -> buildComponentExpr(table, column))
                .collect(Collectors.joining(", ")) + ")";
    }

    /** The text of one column value as the key holds it. */
    public static String buildComponentExpr(Table table, String columnName) {
        return "cast(" + StatisticUtils.quoting(table, columnName) + " as varchar)";
    }

    /**
     * Splits a key into its components. A null element stands for a NULL column value.
     */
    public static List<String> decode(String key) {
        List<String> components = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean currentIsNull = false;
        int i = 0;
        while (i < key.length()) {
            char c = key.charAt(i);
            if (c == SEPARATOR) {
                components.add(currentIsNull ? null : current.toString());
                current.setLength(0);
                currentIsNull = false;
                i++;
                continue;
            }
            if (c != ESCAPE) {
                current.append(c);
                i++;
                continue;
            }
            if (i + 1 >= key.length()) {
                throw new IllegalArgumentException("Malformed statistics tuple key: " + key);
            }
            char next = key.charAt(i + 1);
            if (next == ESCAPE || next == SEPARATOR) {
                current.append(next);
            } else if (next == NULL_MARK && current.length() == 0) {
                currentIsNull = true;
            } else {
                throw new IllegalArgumentException("Malformed statistics tuple key: " + key);
            }
            i += 2;
            if (currentIsNull && i < key.length() && key.charAt(i) != SEPARATOR) {
                throw new IllegalArgumentException("Malformed statistics tuple key: " + key);
            }
        }
        components.add(currentIsNull ? null : current.toString());
        return components;
    }
}
