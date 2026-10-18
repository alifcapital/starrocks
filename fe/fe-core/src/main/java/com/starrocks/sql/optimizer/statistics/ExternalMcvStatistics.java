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

import com.starrocks.statistic.StatisticUtils;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * MCV statistics of one external table as kept by the statistics cache: one entry per
 * collected column set. Column sets are identified by column names because external columns
 * have no stable numeric id.
 */
public class ExternalMcvStatistics {
    public static final ExternalMcvStatistics EMPTY = new ExternalMcvStatistics();

    /** A column an expression element reads, with the type and the Iceberg field id recorded at collection. */
    public record StoredSource(String name, String type, Integer fieldId) {
    }

    /**
     * An expression element of a group: its canonical text, its result type and the columns it reads as recorded
     * at collection, and the expression analyzed against the schema when the group was loaded.
     */
    public static final class Expression {
        private final String text;
        private final String type;
        private final List<StoredSource> sources;
        private final ExpressionTemplate template;

        public Expression(String text, String type, List<StoredSource> sources, ExpressionTemplate template) {
            this.text = text;
            this.type = type;
            this.sources = List.copyOf(sources);
            this.template = template;
        }

        public String getText() {
            return text;
        }

        public String getType() {
            return type;
        }

        public List<StoredSource> getSources() {
            return sources;
        }

        /** Null until the group is bound to the schema of its table. */
        public ExpressionTemplate getTemplate() {
            return template;
        }

        public Expression withTemplate(ExpressionTemplate template) {
            return new Expression(text, type, sources, template);
        }

        private long retainedBytes() {
            long bytes = 96 + stringBytes(text) + stringBytes(type) + 32L * sources.size();
            for (StoredSource source : sources) {
                bytes += 48 + stringBytes(source.name()) + stringBytes(source.type());
            }
            // The analyzed template holds a small operator tree over the source columns.
            return bytes + 512;
        }
    }

    public static class Group {
        // Order of the tuple components in the most common values.
        private final List<String> columnNames;
        // Rows of the table when the statistics were collected; the MCV counts are shares of it.
        private final long rowCount;
        private final long ndv;
        private final PreparedMcvTuples mcv;
        private final List<StoredBucket> buckets;
        private final List<Long> nullCounts;
        private final McvDistribution singleColumnMcv;
        private final long headRows;
        // One entry per element: the expression of an expression element, null for a column.
        private final List<Expression> expressions;
        // The marginal distribution of each element of a multi-element group, built on first use.
        private volatile Group[] elementGroups;
        // The distribution is immutable. This memoized immutable view is keyed by source type so
        // schema changes and dump replay cannot reuse string ordering for numeric/date columns.
        private volatile PreparedColumn preparedColumn;

        private static final class StoredBucket {
            private final String lower;
            private final String upper;
            private final long count;
            private final long repeats;
            private final long ndv;

            private StoredBucket(List<String> fields) {
                if (fields.size() != 5) {
                    throw new IllegalArgumentException("Invalid MCV bucket");
                }
                lower = fields.get(0);
                upper = fields.get(1);
                count = Long.parseLong(fields.get(2));
                repeats = Long.parseLong(fields.get(3));
                ndv = Long.parseLong(fields.get(4));
            }

            private List<String> toFields() {
                return List.of(lower, upper, Long.toString(count), Long.toString(repeats), Long.toString(ndv));
            }
        }

        private static final class PreparedColumn {
            private final Type type;
            private final Histogram histogram;
            private final double min;
            private final double max;

            private PreparedColumn(Type type, Histogram histogram, double min, double max) {
                this.type = type;
                this.histogram = histogram;
                this.min = min;
                this.max = max;
            }
        }

        public Group(List<String> columnNames, long rowCount, long ndv, List<MultiColumnCombinedStats.McvEntry> mcv,
                     List<List<String>> buckets, List<Long> nullCounts) {
            this(columnNames, rowCount, ndv, mcv, buckets, nullCounts, Collections.nCopies(columnNames.size(), null));
        }

        /** The column names of expression elements are their element texts, see ExpressionTemplate#elementText. */
        public Group(List<String> columnNames, long rowCount, long ndv, List<MultiColumnCombinedStats.McvEntry> mcv,
                     List<List<String>> buckets, List<Long> nullCounts, List<Expression> expressions) {
            if (expressions.size() != columnNames.size()) {
                throw new IllegalArgumentException("Invalid MCV expressions");
            }
            this.expressions = Collections.unmodifiableList(new ArrayList<>(expressions));
            if (columnNames.isEmpty() || rowCount < 0 || ndv < 0 || nullCounts.size() != columnNames.size()
                    || nullCounts.stream().anyMatch(count -> count < 0 || count > rowCount)
                    || (columnNames.size() != 1 && !buckets.isEmpty())) {
                throw new IllegalArgumentException("Invalid MCV distribution");
            }
            this.columnNames = List.copyOf(columnNames);
            this.rowCount = rowCount;
            this.ndv = ndv;
            this.mcv = PreparedMcvTuples.copyOf(mcv, columnNames.size(), rowCount, nullCounts);
            this.mcv.getComponentCounts();
            this.buckets = buckets.stream().map(StoredBucket::new).toList();
            this.nullCounts = List.copyOf(nullCounts);
            this.headRows = this.mcv.getTotalRowsLong();
            Map<String, Long> values = new LinkedHashMap<>();
            if (columnNames.size() == 1) {
                for (MultiColumnCombinedStats.McvEntry entry : mcv) {
                    if (entry.getValues().get(0) != null) {
                        values.put(entry.getValues().get(0), entry.getCount());
                    }
                }
            }
            this.singleColumnMcv = McvDistribution.copyOf(values);
            this.singleColumnMcv.prepareFrequencyOrder();
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        public long getRowCount() {
            return rowCount;
        }

        public long getNdv() {
            return ndv;
        }

        public List<MultiColumnCombinedStats.McvEntry> getMcv() {
            return mcv;
        }

        /** Text form for query-dump serialization; the cached counts themselves are numeric. */
        public List<List<String>> getBuckets() {
            return buckets.stream().map(StoredBucket::toFields).toList();
        }

        public List<Long> getNullCounts() {
            return nullCounts;
        }

        /** One entry per element: the expression of an expression element, null for a column. */
        public List<Expression> getExpressions() {
            return expressions;
        }

        public boolean hasExpressions() {
            return expressions.stream().anyMatch(java.util.Objects::nonNull);
        }

        /** The same distribution with the given expressions, as when the group is bound to the table schema. */
        public Group withExpressions(List<Expression> boundExpressions) {
            return new Group(columnNames, rowCount, ndv, mcv, getBuckets(), nullCounts, boundExpressions);
        }

        private long retainedBytes() {
            long bytes = 192 + 32L + 8L * columnNames.size() + 32L + 32L * nullCounts.size()
                    + 32L + 8L * mcv.size() + 32L + 8L * buckets.size() + mcv.retainedIndexBytes();
            for (String name : columnNames) {
                bytes += stringBytes(name);
            }
            for (Expression expression : expressions) {
                bytes += expression == null ? 8 : 8 + expression.retainedBytes();
            }
            for (MultiColumnCombinedStats.McvEntry entry : mcv) {
                bytes += 48 + 64L + 8L * entry.getValues().size()
                        + 32L + 32L * entry.getComponentCounts().size();
                for (String value : entry.getValues()) {
                    bytes += stringBytes(value);
                }
            }
            for (StoredBucket bucket : buckets) {
                bytes += 64 + stringBytes(bucket.lower) + stringBytes(bucket.upper);
            }
            if (columnNames.size() > 1) {
                // Element views are built lazily, and Caffeine does not reweigh a value; reserve them up front.
                bytes += columnNames.size() * (256L + 160L * mcv.size());
            }
            if (columnNames.size() == 1) {
                // Map nodes/boxed counts refer to the strings already counted above. Reserve
                // the prepared histogram even for dump replay, which builds it lazily: Caffeine
                // does not reweigh a value when that memoized view is populated or replaced.
                bytes += singleColumnMcv.retainedBytesExcludingKeys() + 128L + 96L * Math.max(1, buckets.size());
            }
            return bytes;
        }

        /** The single-column planner view of the same MCV record, never a legacy-table lookup. */
        public Optional<ColumnStatistic> columnStatistic(Type type, ColumnStatistic basic) {
            if (columnNames.size() != 1 || rowCount <= 0) {
                return Optional.empty();
            }
            PreparedColumn prepared = prepareColumn(type);
            if (prepared.histogram == null) {
                return Optional.empty();
            }
            long nullRows = nullCounts.get(0);
            ColumnStatistic.Builder builder = ColumnStatistic.buildFrom(basic)
                    .setType(ColumnStatistic.StatisticType.ESTIMATE)
                    .setHistogram(prepared.histogram).setNullsFraction(nullRows / (double) rowCount)
                    .setDistinctValuesCount(Math.max(0, ndv - (nullRows > 0 ? 1 : 0)));
            if (prepared.min <= prepared.max) {
                builder.setMinValue(prepared.min).setMaxValue(prepared.max);
            }
            return Optional.of(builder.build());
        }

        /**
         * The marginal distribution of one element of a multi-element group as a singleton group: the distinct values
         * of the element in the head with their counts over the whole table, from the collected component counts,
         * and the NULL count of the element. Head entries without component counts are left out. The rows outside
         * the head of the element are the tail, with unknown bounds. The NDV of the result is a placeholder that the
         * caller replaces.
         */
        public Group elementGroup(int position) {
            Group[] prepared = elementGroups;
            if (prepared != null && prepared[position] != null) {
                return prepared[position];
            }
            synchronized (this) {
                if (elementGroups == null) {
                    elementGroups = new Group[columnNames.size()];
                }
                if (elementGroups[position] == null) {
                    Map<String, Long> counts = mcv.getComponentCounts().get(position);
                    List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
                    for (Map.Entry<String, Long> value : counts.entrySet()) {
                        if (value.getKey() != null && value.getValue() > 0) {
                            entries.add(new MultiColumnCombinedStats.McvEntry(List.of(value.getKey()), value.getValue(),
                                    List.of(value.getValue())));
                        }
                    }
                    entries.sort((left, right) -> Long.compare(right.getCount(), left.getCount()));
                    long nulls = Math.min(rowCount, nullCounts.get(position));
                    elementGroups[position] = new Group(List.of(columnNames.get(position)), rowCount,
                            entries.size() + 1, entries, List.of(), List.of(nulls));
                }
                return elementGroups[position];
            }
        }

        /** Prepare at load time; constructors used by dump replay can prepare on first use. */
        void prepare(Type type) {
            if (columnNames.size() == 1 && rowCount > 0) {
                prepareColumn(type);
            }
        }

        private PreparedColumn prepareColumn(Type type) {
            PreparedColumn prepared = preparedColumn;
            if (prepared != null && prepared.type.equals(type)) {
                return prepared;
            }
            synchronized (this) {
                prepared = preparedColumn;
                if (prepared == null || !prepared.type.equals(type)) {
                    prepared = buildColumn(type);
                    preparedColumn = prepared;
                }
                return prepared;
            }
        }

        private PreparedColumn buildColumn(Type type) {
            long nullRows = nullCounts.get(0);
            List<Bucket> plannerBuckets = new ArrayList<>(buckets.size());
            for (StoredBucket bucket : buckets) {
                if (type.isStringType()) {
                    plannerBuckets.add(new StringBucket(bucket.lower, bucket.upper, bucket.count,
                            bucket.repeats, bucket.ndv));
                    continue;
                }
                Optional<Double> low = StatisticUtils.convertStatisticsToDouble(type, bucket.lower);
                Optional<Double> high = StatisticUtils.convertStatisticsToDouble(type, bucket.upper);
                if (low.isEmpty() || high.isEmpty()) {
                    return new PreparedColumn(type, null, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
                }
                plannerBuckets.add(new Bucket(low.get(), high.get(), bucket.count, bucket.repeats, bucket.ndv));
            }
            List<Bucket> immutableBuckets = List.copyOf(plannerBuckets);
            Histogram histogram = immutableBuckets.isEmpty()
                    ? Histogram.ofSingleBucket(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                            rowCount - nullRows, singleColumnMcv) : new Histogram(immutableBuckets, singleColumnMcv);
            if (type.isStringType() && (singleColumnMcv.getTotalRows()
                    + nullRows == rowCount || !immutableBuckets.isEmpty())) {
                histogram = Histogram.forStrings(immutableBuckets, singleColumnMcv);
            }
            double min = Double.POSITIVE_INFINITY;
            double max = Double.NEGATIVE_INFINITY;
            for (Bucket bucket : immutableBuckets) {
                min = Math.min(min, bucket.getLower());
                max = Math.max(max, bucket.getUpper());
            }
            for (String value : singleColumnMcv.keySet()) {
                Optional<Double> number = StatisticUtils.convertStatisticsToDouble(type, value);
                if (number.isPresent()) {
                    min = Math.min(min, number.get());
                    max = Math.max(max, number.get());
                }
            }
            if (immutableBuckets.isEmpty() && headRows != rowCount) {
                min = Double.POSITIVE_INFINITY;
                max = Double.NEGATIVE_INFINITY;
            }
            return new PreparedColumn(type, histogram, min, max);
        }
    }

    private final List<Group> groups;
    private final boolean incomplete;
    private final long retainedBytes;

    private ExternalMcvStatistics() {
        this.groups = Collections.emptyList();
        this.incomplete = false;
        this.retainedBytes = 64;
    }

    public ExternalMcvStatistics(List<Group> groups) {
        this(groups, false);
    }

    /**
     * @param incomplete whether a group was left out of this load for a reason that can pass, such as a schema
     *                   that could not be read; the cache loads such statistics again soon
     */
    public ExternalMcvStatistics(List<Group> groups, boolean incomplete) {
        this.incomplete = incomplete;
        this.groups = List.copyOf(groups);
        this.retainedBytes = 64 + 8L * groups.size() + groups.stream().mapToLong(Group::retainedBytes).sum();
    }

    /** Conservative retained-heap estimate, computed once rather than traversed by the planner. */
    public long retainedBytes() {
        return retainedBytes;
    }

    private static long stringBytes(String value) {
        // Allow UTF-16 storage, object/array headers and alignment, including with compact strings disabled.
        return value == null ? 0 : 48L + ((2L * value.length() + 7) & ~7L);
    }

    public List<Group> getGroups() {
        return groups;
    }

    public boolean isIncomplete() {
        return incomplete;
    }

    public boolean isEmpty() {
        return groups.isEmpty();
    }
}
