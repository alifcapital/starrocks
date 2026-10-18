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

package com.starrocks.sql.optimizer.rewrite;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.common.util.StringDateFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.StatisticsDatetimeParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The format in which an Iceberg table writes dates into a VARCHAR column, declared by the table property
 * string-date-format.&lt;column&gt;, for example "string-date-format.created_at" = "%Y-%m-%dT%H:%i:%s.%fZ". The
 * property is a promise of the owner of the table that every non-NULL value of the column is a string of the format,
 * see StringDateFormat. Nobody checks the data against it. A value that is not a format we accept gives a WARN once
 * per table, column and value, and the column has no format: the query runs as without the property. The session
 * variable enable_string_date_format_property turns all formats off.
 */
public final class StringDateFormatProperty {
    public static final String PREFIX = "string-date-format.";

    private static final Logger LOG = LogManager.getLogger(StringDateFormatProperty.class);

    // We expect a few distinct values that every plan of the table reads, so each value is parsed once.
    private static final Cache<String, Parsed> PARSED = Caffeine.newBuilder().maximumSize(1024).build();
    private static final Cache<String, Boolean> WARNED = Caffeine.newBuilder().maximumSize(4096).build();

    // The domains of columns are derived without the optimizer context, while the format of a column comes from the
    // table the column factory of the query maps it to. OnPredicateMoveAroundRule, which reads the domains, sets the
    // factory here for the time it runs.
    private static final ThreadLocal<ColumnRefFactory> QUERY_COLUMNS = new ThreadLocal<>();

    private record Parsed(StringDateFormat format, String error) {
    }

    private StringDateFormatProperty() {
    }

    /** The format a table declares for a column, or null when there is none or the switch is off. */
    public static StringDateFormat of(Table table, String columnName) {
        if (!(table instanceof IcebergTable iceberg)) {
            return null;
        }
        ConnectContext context = ConnectContext.get();
        if (context != null && !context.getSessionVariable().isEnableStringDateFormatProperty()) {
            return null;
        }
        Map<String, String> properties;
        try {
            properties = iceberg.getProperties();
        } catch (RuntimeException e) {
            // A table whose metadata we cannot read here only loses the bounds.
            LOG.debug("Cannot read the properties of {}", iceberg.getName(), e);
            return null;
        }
        // The property names the column as StarRocks reads columns of Iceberg: without regard to case.
        String value = null;
        for (Map.Entry<String, String> property : properties.entrySet()) {
            String key = property.getKey();
            if (!key.startsWith(PREFIX) || !key.substring(PREFIX.length()).equalsIgnoreCase(columnName)) {
                continue;
            }
            if (value != null && !value.equals(property.getValue())) {
                warnOnce(iceberg, columnName, value + "' and '" + property.getValue(),
                        "properties that differ only in the case of the column name disagree");
                return null;
            }
            value = property.getValue();
        }
        if (value == null) {
            return null;
        }
        Parsed parsed = PARSED.get(value, StringDateFormatProperty::parse);
        if (parsed.format() == null) {
            warnOnce(iceberg, columnName, value, parsed.error());
        }
        return parsed.format();
    }

    /** The formats of the columns of a scan. */
    public static Function<ColumnRefOperator, StringDateFormat> forScan(LogicalScanOperator scan) {
        Table table = scan.getTable();
        if (!(table instanceof IcebergTable)) {
            return column -> null;
        }
        Map<ColumnRefOperator, Column> columns = scan.getColRefToColumnMetaMap();
        return column -> {
            Column meta = columns.get(column);
            return meta == null ? null : of(table, meta.getName());
        };
    }

    /**
     * The format of a column of the query that runs on this thread. A column that is not a column of one table,
     * such as an expression or the output of a union, has none.
     */
    public static StringDateFormat ofQueryColumn(ColumnRefOperator column) {
        ColumnRefFactory factory = QUERY_COLUMNS.get();
        if (factory == null) {
            return null;
        }
        Pair<Table, Column> source = factory.getTableAndColumn(column);
        return source == null ? null : of(source.first, source.second.getName());
    }

    /** Runs an action that reads the formats of the columns the factory makes, see ofQueryColumn. */
    public static <T> T withQueryColumns(ColumnRefFactory factory, Supplier<T> action) {
        ColumnRefFactory outer = QUERY_COLUMNS.get();
        QUERY_COLUMNS.set(factory);
        try {
            return action.get();
        } finally {
            if (outer == null) {
                QUERY_COLUMNS.remove();
            } else {
                QUERY_COLUMNS.set(outer);
            }
        }
    }

    private static Parsed parse(String value) {
        StringDateFormat format;
        try {
            format = StringDateFormat.parse(value);
        } catch (IllegalArgumentException e) {
            return new Parsed(null, e.getMessage());
        }
        // The bounds on the strings hold only when the BE casts them to the times they hold
        if (!format.isReadAsWrittenBy(StatisticsDatetimeParser::parse)) {
            return new Parsed(null, "the BE casts the strings of this format to other times");
        }
        return new Parsed(format, null);
    }

    private static void warnOnce(IcebergTable table, String columnName, String value, String reason) {
        String name = table.getCatalogName() + "." + table.getCatalogDBName() + "." + table.getCatalogTableName();
        if (WARNED.asMap().putIfAbsent(name + "\u0000" + columnName + "\u0000" + value, Boolean.TRUE) == null) {
            LOG.warn("Ignore {}{} = '{}' of table {}: {}", PREFIX, columnName, value, name, reason);
        }
    }
}
