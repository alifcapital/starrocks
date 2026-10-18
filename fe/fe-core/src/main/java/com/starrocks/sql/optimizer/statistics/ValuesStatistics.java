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

import com.starrocks.sql.optimizer.ConstantOperatorUtils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

/**
 * The statistics of the columns of a VALUES operator, read from the constants of its rows.
 *
 * A VALUES list is often a reference table written in the query, and a join or a grouping on it is estimated from
 * the statistics of its columns. We want the exact number of distinct values and the exact range for it, so the
 * estimate does not fall back to the defaults of unknown statistics.
 *
 * We fear that a column which we read only in part gets a wrong count, so a field that is not a constant, or is a
 * constant that we cannot cast to the type of the column, leaves the whole column unknown. A column of the NULL type
 * stays unknown too: it is the output of a query without FROM and of a table function, and several rules test for
 * unknown statistics to keep the plan of such a query as it is.
 *
 * We also fear that a list of a million rows costs more to read than to plan. A column keeps its distinct values
 * exactly up to EXACT_LIMIT of them, which holds every reference table, and counts the values above that limit with
 * a sketch. The count does not jump at the limit: it only becomes an estimate with a small error.
 */
final class ValuesStatistics {
    // A column keeps this many distinct values exactly, and counts the rest with a sketch.
    static final int EXACT_LIMIT = 4096;
    // The columns that one pass over the rows reads. Each of them keeps at most EXACT_LIMIT values or a sketch, so
    // the memory of a pass does not depend on the number of rows, and the rows are read once for these columns.
    private static final int COLUMNS_PER_PASS = 32;

    // The statistics depend on the rows and on the types of the columns that read them.
    private record Key(ExpressionStatisticsCache.Identity rows, List<Type> types) {
    }

    private ValuesStatistics() {
    }

    /**
     * The statistics of each column, in the order of the columns. The derivation of the statistics of one query
     * reaches the same operator many times, and each time it reads all the rows, so the cache of the query keeps
     * the result by the identity of the rows; without a cache the result is computed on every call.
     */
    static List<ColumnStatistic> of(List<ColumnRefOperator> columns, List<List<ScalarOperator>> rows,
                                    ExpressionStatisticsCache cache) {
        List<Type> types = new ArrayList<>(columns.size());
        for (ColumnRefOperator column : columns) {
            types.add(column.getType());
        }
        if (cache == null) {
            return compute(types, rows);
        }
        return cache.part(List.of(new Key(new ExpressionStatisticsCache.Identity(rows), types)),
                () -> compute(types, rows), statistics -> statistics.size() + 1L);
    }

    private static List<ColumnStatistic> compute(List<Type> types, List<List<ScalarOperator>> rows) {
        List<ColumnStatistic> statistics =
                new ArrayList<>(Collections.nCopies(types.size(), ColumnStatistic.unknown()));
        if (rows.isEmpty()) {
            return statistics;
        }
        for (int from = 0; from < types.size(); from += COLUMNS_PER_PASS) {
            read(types, from, Math.min(types.size(), from + COLUMNS_PER_PASS), rows, statistics);
        }
        return statistics;
    }

    // One pass over the rows for the columns from..to: the fields of a row lie close in memory, so they are read
    // together. A column that fails is dropped from the pass and stays unknown.
    private static void read(List<Type> types, int from, int to, List<List<ScalarOperator>> rows,
                             List<ColumnStatistic> statistics) {
        Collector[] collectors = new Collector[to - from];
        int live = 0;
        for (int i = from; i < to; i++) {
            if (!types.get(i).isNull()) {
                collectors[i - from] = new Collector(types.get(i));
                live++;
            }
        }
        for (List<ScalarOperator> row : rows) {
            if (live == 0) {
                return;
            }
            for (int i = 0; i < collectors.length; i++) {
                Collector collector = collectors[i];
                if (collector == null) {
                    continue;
                }
                collector.add(row.get(from + i));
                if (collector.failed) {
                    collectors[i] = null;
                    live--;
                }
            }
        }
        for (int i = 0; i < collectors.length; i++) {
            if (collectors[i] != null) {
                statistics.set(from + i, collectors[i].statistic(rows.size()));
            }
        }
    }

    private static final class Collector {
        private final Type type;
        private final boolean string;
        // The keys of the distinct values while they are at most EXACT_LIMIT; null after that, see see().
        private Set<Object> distinct = new HashSet<>();
        private StatisticsHll.Union sketch;
        private int nulls;
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private boolean nan;
        private long length;
        private boolean failed;
        // The fields of a column have one type in the common case, so the check of the type runs once for it.
        private Type lastType;
        private boolean lastTypeFits;

        private Collector(Type type) {
            this.type = type;
            this.string = type.isStringType();
        }

        // A field that cannot be read as a value of the column sets failed.
        private void add(ScalarOperator field) {
            if (!(field instanceof ConstantOperator constant)) {
                failed = true;
                return;
            }
            if (constant.isNull()) {
                nulls++;
                return;
            }
            ConstantOperator value = convert(constant);
            if (value == null || value.isNull()) {
                failed = true;
                return;
            }
            if (string) {
                String text = (String) value.getValue();
                length += text.length();
                see(text);
                return;
            }
            OptionalDouble number = ConstantOperatorUtils.doubleValueFromConstant(value);
            if (number.isEmpty()) {
                failed = true;
                return;
            }
            if (Double.isNaN(number.getAsDouble())) {
                nan = true;
            } else {
                min = Math.min(min, number.getAsDouble());
                max = Math.max(max, number.getAsDouble());
            }
            see(key(value.getValue()));
        }

        // We cast a constant to the type of the column, so the values compare as the column does. Two string types
        // differ in their length only and keep the text, so we do not cast between them. A cast can fail, or give
        // NULL for a value that does not fit the type: in both cases we do not know the value.
        private ConstantOperator convert(ConstantOperator constant) {
            Type fieldType = constant.getType();
            if (fieldType != lastType) {
                lastType = fieldType;
                lastTypeFits = string ? fieldType.isStringType() : fieldType.equals(type);
            }
            if (lastTypeFits) {
                return constant;
            }
            Optional<ConstantOperator> cast;
            try {
                cast = constant.castTo(type);
            } catch (RuntimeException e) {
                return null;
            }
            return cast.orElse(null);
        }

        // Equal values must give equal keys. 1.0 and 1.00 are one value, and so are -0.0 and 0.0, and all NaN
        // are one value, which the boxed Double already compares as equal.
        private static Object key(Object value) {
            if (value instanceof BigDecimal decimal) {
                return decimal.stripTrailingZeros();
            }
            if (value instanceof Double number && number == 0.0) {
                return 0.0;
            }
            return value;
        }

        // We keep the keys of the first EXACT_LIMIT distinct values, so the count of a reference table is exact. A
        // column with more values moves them to a sketch, which holds a fixed number of bytes whatever the rows are.
        private void see(Object key) {
            if (sketch != null) {
                sketch.addHash(hash(key));
                return;
            }
            distinct.add(key);
            if (distinct.size() > EXACT_LIMIT) {
                sketch = new StatisticsHll.Union();
                for (Object kept : distinct) {
                    sketch.addHash(hash(kept));
                }
                distinct = null;
            }
        }

        // The sketch takes the bits of a hash as they come, so the hash of a key is mixed over all 64 bits. A string
        // gives its hash code, which the exact count has computed already, and a number gives its value.
        private static long hash(Object key) {
            long bits;
            if (key instanceof String text) {
                bits = text.hashCode();
            } else if (key instanceof Long || key instanceof Integer || key instanceof Short || key instanceof Byte) {
                bits = ((Number) key).longValue();
            } else if (key instanceof Double number) {
                bits = Double.doubleToLongBits(number);
            } else if (key instanceof LocalDateTime time) {
                bits = time.toEpochSecond(ZoneOffset.UTC) * 1_000_003L + time.getNano();
            } else {
                bits = key.hashCode();
            }
            bits ^= bits >>> 30;
            bits *= 0xbf58476d1ce4e5b9L;
            bits ^= bits >>> 27;
            bits *= 0x94d049bb133111ebL;
            return bits ^ (bits >>> 31);
        }

        private ColumnStatistic statistic(int rows) {
            int present = rows - nulls;
            // The sketch is built after EXACT_LIMIT values, so the count is not below it.
            double distinctValues = sketch == null ? distinct.size()
                    : Math.min(present, Math.max(sketch.estimate(), EXACT_LIMIT + 1));
            ColumnStatistic.Builder builder = ColumnStatistic.builder()
                    .setNullsFraction((double) nulls / rows)
                    .setAverageRowSize(string && present > 0 ? (double) length / present : type.getTypeSize())
                    .setDistinctValuesCount(distinctValues);
            // A column with NaN has no usable range: the consumers test for NaN in the range and skip it.
            if (nan) {
                builder.setMinValue(Double.NaN).setMaxValue(Double.NaN);
            } else if (!string && present > 0) {
                builder.setMinValue(min).setMaxValue(max);
            }
            return builder.build();
        }
    }
}
