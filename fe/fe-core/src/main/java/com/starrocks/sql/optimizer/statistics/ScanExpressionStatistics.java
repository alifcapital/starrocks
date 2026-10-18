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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.IntSupplier;

/**
 * The statistics on expressions of the columns of one scan. Each distinct expression f gets a virtual column x
 * with the statistics collected on f. x never enters the plan: it is not in the column statistics or the column
 * groups of the scan, so runtime filters, joins and the NDV of real columns do not see it. The estimate of the scan
 * predicate and the statistics of a projection right above the scan read it.
 */
public final class ScanExpressionStatistics {
    private static final Logger LOG = LogManager.getLogger(ScanExpressionStatistics.class);

    /** One expression of the scan columns and its virtual column. */
    public static final class Entry {
        private final ScalarOperator expression;
        private final ExpressionStatisticsCanonicalizer.Key key;
        private final ColumnRefOperator column;
        private final ColumnStatistic statistic;
        // The statistics of x over the whole table, before the predicate of the scan.
        private final ColumnStatistic unfilteredStatistic;

        Entry(ScalarOperator expression, ExpressionStatisticsCanonicalizer.Key key, ColumnRefOperator column,
                ColumnStatistic statistic) {
            this(expression, key, column, statistic, statistic);
        }

        private Entry(ScalarOperator expression, ExpressionStatisticsCanonicalizer.Key key, ColumnRefOperator column,
                      ColumnStatistic statistic, ColumnStatistic unfilteredStatistic) {
            this.expression = expression;
            this.key = key;
            this.column = column;
            this.statistic = statistic;
            this.unfilteredStatistic = unfilteredStatistic;
        }

        /** The canonical expression over the columns of the scan. */
        public ScalarOperator getExpression() {
            return expression;
        }

        public ExpressionStatisticsCanonicalizer.Key getKey() {
            return key;
        }

        /** The virtual column x of the expression. */
        public ColumnRefOperator getColumn() {
            return column;
        }

        public ColumnStatistic getStatistic() {
            return statistic;
        }

        public ColumnStatistic getUnfilteredStatistic() {
            return unfilteredStatistic;
        }

        Entry withStatistic(ColumnStatistic newStatistic) {
            return new Entry(expression, key, column, newStatistic, newStatistic);
        }

        Entry withFilteredStatistic(ColumnStatistic filtered) {
            return new Entry(expression, key, column, filtered, unfilteredStatistic);
        }
    }

    private final String tableName;
    private final List<Entry> entries;
    private final Map<ExpressionStatisticsCanonicalizer.Key, Entry> byKey = new HashMap<>();
    private final Map<ColumnRefOperator, Entry> byColumn = new HashMap<>();
    // The column groups that hold a virtual column: {x} and the groups of real columns and x.
    private final Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups;
    private final Set<ColumnRefOperator> sourceColumns = new HashSet<>();
    // After the predicate of the scan: the virtual columns whose statistics describe the rows of the scan, null
    // when the scan has no estimated predicate yet, and the conditions of the predicate written over them.
    private final Set<ColumnRefOperator> describing;
    private final ScalarOperator virtualPredicate;
    private final boolean virtualPredicateHasOr;
    private final ExpressionStatisticsCache cache;

    ScanExpressionStatistics(String tableName, List<Entry> entries,
                             Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups) {
        this(tableName, entries, groups, null, null, false, new ExpressionStatisticsCache());
    }

    private ScanExpressionStatistics(String tableName, List<Entry> entries,
                                     Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups,
                                     Set<ColumnRefOperator> describing, ScalarOperator virtualPredicate,
                                     boolean virtualPredicateHasOr, ExpressionStatisticsCache cache) {
        this.cache = cache;
        this.describing = describing == null ? null : Set.copyOf(describing);
        this.virtualPredicate = virtualPredicate;
        this.virtualPredicateHasOr = virtualPredicateHasOr;
        this.tableName = tableName;
        this.entries = List.copyOf(entries);
        this.groups = Collections.unmodifiableMap(new HashMap<>(groups));
        for (Entry entry : this.entries) {
            byKey.put(entry.getKey(), entry);
            byColumn.put(entry.getColumn(), entry);
            for (ColumnRefOperator source : com.starrocks.sql.optimizer.Utils.extractColumnRef(entry.getExpression())) {
                sourceColumns.add(source);
            }
        }
    }

    public String getTableName() {
        return tableName;
    }

    /** The cache of the query, shared by the scans of the query. */
    ExpressionStatisticsCache getCache() {
        return cache;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public Entry getEntry(ExpressionStatisticsCanonicalizer.Key key) {
        return byKey.get(key);
    }

    public Entry getEntry(ColumnRefOperator column) {
        return byColumn.get(column);
    }

    public Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> getGroups() {
        return groups;
    }

    /** The real columns some expression reads. */
    public Set<ColumnRefOperator> getSourceColumns() {
        return sourceColumns;
    }

    /**
     * Whether the statistics of x describe the rows of the scan: no estimated condition of the scan reads the
     * columns of its expression, or every such condition is written over x.
     */
    public boolean describesScanRows(ColumnRefOperator column) {
        return describing == null || describing.contains(column);
    }

    /** The conditions of the scan predicate written over virtual columns, null without them. */
    public ScalarOperator getVirtualPredicate() {
        return virtualPredicate;
    }

    public boolean virtualPredicateHasOr() {
        return virtualPredicateHasOr;
    }

    /** The same expressions after the predicate of the scan. */
    ScanExpressionStatistics afterFilter(Map<ColumnRefOperator, ColumnStatistic> filtered,
                                         Set<ColumnRefOperator> describingColumns, ScalarOperator predicate,
                                         boolean hasOr) {
        List<Entry> updated = new ArrayList<>(entries.size());
        for (Entry entry : entries) {
            ColumnStatistic statistic = filtered.get(entry.getColumn());
            updated.add(statistic == null ? entry : entry.withFilteredStatistic(statistic));
        }
        return new ScanExpressionStatistics(tableName, updated, groups, describingColumns, predicate, hasOr, cache);
    }

    /**
     * The statistics of the scan with the virtual columns used by a written predicate: every virtual column of each
     * group that holds a used one, with the groups. A head entry without component counts reads the statistics of
     * every column of its group, so a group cannot come without all its columns. The field itself is not copied,
     * so a nested estimate does not match again.
     */
    Statistics withVirtualColumns(Statistics statistics, Set<ColumnRefOperator> used) {
        Statistics.Builder builder = Statistics.buildFrom(statistics);
        for (Map.Entry<Set<ColumnRefOperator>, MultiColumnCombinedStats> group : groups.entrySet()) {
            if (Collections.disjoint(group.getKey(), used)) {
                continue;
            }
            builder.addMultiColumnStatistics(group.getKey(), group.getValue());
            for (ColumnRefOperator column : group.getKey()) {
                Entry entry = byColumn.get(column);
                if (entry != null) {
                    builder.addColumnStatistic(column, entry.getStatistic());
                }
            }
        }
        for (ColumnRefOperator column : used) {
            builder.addColumnStatistic(column, byColumn.get(column).getStatistic());
        }
        return builder.build();
    }

    /**
     * Attaches the groups with expression elements to the statistics of a scan. A group is attached when the scan
     * reads every column its expressions read, and each of those columns still has the type and the Iceberg field
     * id recorded at collection: the cache can hold a group for a long time while the schema changes. Any failure
     * leaves the statistics as they are, so a query never fails because of statistics on expressions.
     */
    static Statistics attach(Statistics statistics, ExternalMcvStatistics cached,
                             Map<ColumnRefOperator, Column> colRefToColumnMetaMap, Table table, IntSupplier ids) {
        return attach(statistics, cached, colRefToColumnMetaMap, table, ids, new ExpressionStatisticsCache());
    }

    static Statistics attach(Statistics statistics, ExternalMcvStatistics cached,
                             Map<ColumnRefOperator, Column> colRefToColumnMetaMap, Table table, IntSupplier ids,
                             ExpressionStatisticsCache cache) {
        try {
            return attachOrThrow(statistics, cached, colRefToColumnMetaMap, table, ids, cache);
        } catch (RuntimeException e) {
            LOG.warn("Ignore MCV statistics on expressions of table {}", table == null ? null : table.getName(), e);
            return StatisticsCalcUtils.withPlainElementStatistics(statistics, cached, colRefToColumnMetaMap);
        }
    }

    // What an attach of one scan gives: the statistics on expressions, null when none apply, and the groups to
    // build the statistics of the plain elements from.
    record Attached(ScanExpressionStatistics expressions, List<ExternalMcvStatistics.Group> groups,
                    List<List<ColumnRefOperator>> refs, Set<ColumnRefOperator> virtual) {
    }

    /**
     * The statistics of a scan are derived for every plan alternative, with the same stored statistics and column
     * statistics of the same values. The attach is kept for the query on these, so every derivation sees the same
     * virtual columns and the same object, and the matches and estimates on it are found again.
     */
    private static Statistics attachOrThrow(Statistics statistics, ExternalMcvStatistics cached,
                                            Map<ColumnRefOperator, Column> colRefToColumnMetaMap, Table table,
                                            IntSupplier ids, ExpressionStatisticsCache cache) {
        Attached attached = cache.attached(attachKey(statistics, cached, colRefToColumnMetaMap, table),
                () -> attachExpressions(statistics, cached, colRefToColumnMetaMap, table, ids, cache));
        if (attached.expressions() == null) {
            return StatisticsCalcUtils.withPlainElementStatistics(statistics, cached, colRefToColumnMetaMap);
        }
        Statistics.Builder builder = Statistics.buildFrom(statistics);
        StatisticsCalcUtils.addElementStatistics(builder, attached.groups(), attached.refs(), attached.virtual());
        return builder.setScanExpressionStatistics(attached.expressions()).build();
    }

    // The objects and values an attach reads: compared by identity, and the values of each column statistic.
    private static List<Object> attachKey(Statistics statistics, ExternalMcvStatistics cached,
                                          Map<ColumnRefOperator, Column> colRefToColumnMetaMap, Table table) {
        List<Object> key = new ArrayList<>();
        key.add(new ExpressionStatisticsCache.Identity(cached));
        key.add(new ExpressionStatisticsCache.Identity(table));
        key.add(statistics.getOutputRowCount());
        List<ColumnRefOperator> refs = new ArrayList<>(colRefToColumnMetaMap.keySet());
        refs.sort(java.util.Comparator.comparingInt(ColumnRefOperator::getId));
        for (ColumnRefOperator ref : refs) {
            key.add(ref.getId());
            key.add(new ExpressionStatisticsCache.Identity(colRefToColumnMetaMap.get(ref)));
            ColumnStatistic column = statistics.getColumnStatistics().get(ref);
            if (column == null) {
                key.add(null);
                continue;
            }
            key.add(List.of(column.getMinValue(), column.getMaxValue(), column.getNullsFraction(),
                    column.getAverageRowSize(), column.getDistinctValuesCount(), column.getType(),
                    column.isDistinctValuesFromDefaultSelectivity()));
            key.add(new ExpressionStatisticsCache.Identity(column.getHistogram()));
        }
        return key;
    }

    private static Attached attachExpressions(Statistics statistics, ExternalMcvStatistics cached,
                                              Map<ColumnRefOperator, Column> colRefToColumnMetaMap, Table table,
                                              IntSupplier ids, ExpressionStatisticsCache cache) {
        Map<String, ColumnRefOperator> byName = new HashMap<>();
        colRefToColumnMetaMap.forEach((ref, column) ->
                byName.putIfAbsent(column.getName().toLowerCase(Locale.ROOT), ref));
        Map<ExpressionStatisticsCanonicalizer.Key, Entry> entries = new LinkedHashMap<>();
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups = new HashMap<>();
        // The groups with their elements over the scan, to build the statistics of each element.
        List<ExternalMcvStatistics.Group> attachedGroups = new ArrayList<>();
        List<List<ColumnRefOperator>> attachedRefs = new ArrayList<>();
        for (ExternalMcvStatistics.Group stored : cached.getGroups()) {
            if (!stored.hasExpressions() || stored.getRowCount() <= 0) {
                continue;
            }
            ExternalMcvStatistics.Group group = stored;
            boolean fromDump = stored.getExpressions().stream().anyMatch(expression -> expression != null
                    && expression.getTemplate() == null);
            if (fromDump) {
                // A group from a query dump comes without its analyzed expressions; analyze them for this table. The
                // table of a replayed dump cannot give the Iceberg field ids, so they are not compared.
                group = ExternalMcvStatsCacheLoader.bindExpressions(stored, table, ConnectContext.get(),
                        table == null ? null : table.getName(), false);
                if (group == null) {
                    continue;
                }
            }
            List<ColumnRefOperator> refs = new ArrayList<>(group.getColumnNames().size());
            Map<ExpressionStatisticsCanonicalizer.Key, Entry> created = new LinkedHashMap<>();
            boolean usable = true;
            for (int i = 0; i < group.getColumnNames().size() && usable; i++) {
                ExternalMcvStatistics.Expression expression = group.getExpressions().get(i);
                if (expression == null) {
                    refs.add(byName.get(group.getColumnNames().get(i).toLowerCase(Locale.ROOT)));
                    continue;
                }
                Entry entry = instantiate(expression, byName, colRefToColumnMetaMap, table, !fromDump);
                if (entry == null) {
                    usable = false;
                    continue;
                }
                Entry existing = entries.containsKey(entry.getKey()) ? entries.get(entry.getKey())
                        : created.get(entry.getKey());
                if (existing == null) {
                    int id = ids.getAsInt();
                    ColumnRefOperator column = new ColumnRefOperator(id, expression.getTemplate().getType(),
                            "expr#" + Math.abs((long) id), true);
                    existing = new Entry(entry.getExpression(), entry.getKey(), column, null);
                    created.put(existing.getKey(), existing);
                }
                refs.add(existing.getColumn());
            }
            Set<ColumnRefOperator> read = new HashSet<>();
            for (ColumnRefOperator ref : refs) {
                if (ref != null && !read.add(ref)) {
                    usable = false;
                }
            }
            if (!usable) {
                continue;
            }
            entries.putAll(created);
            attachedGroups.add(group);
            attachedRefs.add(refs);
            groups.merge(read, new MultiColumnCombinedStats(group.getNdv(), group.getRowCount(), refs, group.getMcv(),
                    group.getNullCounts()), StatisticsCalcUtils::preferMultiColumnStats);
        }
        if (entries.isEmpty()) {
            return new Attached(null, null, null, null);
        }
        // The marginal of an element comes from the group, of columns or with x, whose head covers more of its rows.
        List<ExternalMcvStatistics.Group> allGroups = new ArrayList<>(attachedGroups);
        List<List<ColumnRefOperator>> allRefs = new ArrayList<>(attachedRefs);
        StatisticsCalcUtils.plainMultiElementGroups(cached, colRefToColumnMetaMap, allGroups, allRefs);

        List<Entry> result = new ArrayList<>(entries.size());
        for (Entry entry : entries.values()) {
            ColumnStatistic base = ExpressionStatisticCalculator.calculate(entry.getExpression(), statistics);
            ColumnStatistic statistic = singletonStatistic(entry, attachedGroups, attachedRefs, base);
            if (statistic == null) {
                statistic = elementStatistic(entry.getColumn(), allGroups, allRefs, base,
                        sourceNdv(entry.getExpression(), statistics));
            }
            if (statistic == null) {
                // Every virtual column of a group must have statistics, or the estimate would fail on it.
                groups.keySet().removeIf(key -> key.contains(entry.getColumn()));
                continue;
            }
            result.add(entry.withStatistic(statistic));
        }
        if (result.isEmpty() || groups.isEmpty()) {
            return new Attached(null, null, null, null);
        }
        Set<ColumnRefOperator> virtual = new HashSet<>();
        entries.values().forEach(entry -> virtual.add(entry.getColumn()));
        return new Attached(new ScanExpressionStatistics(table == null ? null : table.getName(), result, groups, null,
                null, false, cache), allGroups, allRefs, virtual);
    }

    /**
     * The most distinct values the expression can take, from the columns it reads alone: the product of their NDV, not
     * more than the rows. We fear the NDV that the estimate gives the expression itself, which is a guess, as the one of
     * a prefix of a string from the length of the strings, being taken for a bound that the collected NDV of x must
     * not pass; the columns it reads are the only bound we know. Infinite when a column has no known NDV, as the NDV of
     * 1 of an unknown statistic would put the whole tail on one value.
     */
    static double sourceNdv(ScalarOperator expression, Statistics statistics) {
        double product = 1;
        boolean read = false;
        for (ColumnRefOperator source : new LinkedHashSet<>(expression.getColumnRefs())) {
            ColumnStatistic statistic = statistics.getColumnStatistics().get(source);
            if (statistic == null || statistic.isUnknown() || Double.isNaN(statistic.getDistinctValuesCount())
                    || Double.isInfinite(statistic.getDistinctValuesCount())) {
                return Double.POSITIVE_INFINITY;
            }
            product *= Math.max(1, statistic.getDistinctValuesCount());
            read = true;
        }
        return read ? Math.min(product, Math.max(1, statistics.getOutputRowCount())) : Double.POSITIVE_INFINITY;
    }

    // The expression over the scan columns, or null when a source is not read or no longer matches the recording.
    private static Entry instantiate(ExternalMcvStatistics.Expression expression, Map<String, ColumnRefOperator> byName,
                                     Map<ColumnRefOperator, Column> colRefToColumnMetaMap, Table table,
                                     boolean checkFieldIds) {
        if (expression.getTemplate() == null) {
            return null;
        }
        for (ExternalMcvStatistics.StoredSource source : expression.getSources()) {
            ColumnRefOperator ref = byName.get(source.name().toLowerCase(Locale.ROOT));
            Column column = ref == null ? null : colRefToColumnMetaMap.get(ref);
            if (column == null || !column.getType().toSql().equals(source.type())
                    || checkFieldIds && !ExpressionTemplate.fieldIdMatches(table, column.getName(), source.fieldId())) {
                return null;
            }
        }
        ScalarOperator instantiated = expression.getTemplate().instantiate(byName);
        if (instantiated == null) {
            return null;
        }
        ExpressionStatisticsCanonicalizer.Key key = ExpressionStatisticsCanonicalizer.keyOf(instantiated);
        return key == null ? null : new Entry(instantiated, key, null, null);
    }

    // The statistics of x from the singleton group {x}, which holds its own buckets.
    private static ColumnStatistic singletonStatistic(Entry entry, List<ExternalMcvStatistics.Group> groups,
                                                      List<List<ColumnRefOperator>> refs, ColumnStatistic base) {
        for (int i = 0; i < groups.size(); i++) {
            if (refs.get(i).size() == 1 && entry.getColumn().equals(refs.get(i).get(0))) {
                ColumnStatistic basic = base.isUnknown() ? ColumnStatistic.unknown() : base;
                // The NDV is the collected one, and not a guess whatever the base says: the choice of the source of
                // an estimate reads the mark of a guess.
                return groups.get(i).columnStatistic(entry.getColumn().getType(), basic)
                        .map(statistic -> ColumnStatistic.buildFrom(statistic)
                                .setDistinctValuesFromDefaultSelectivity(false).build())
                        .orElse(null);
            }
        }
        return null;
    }

    /**
     * The statistics of one element of the multi-element groups: the group whose head covers more rows of the
     * element gives the head of the element from the component counts, the tail as one bucket of unknown bounds and
     * the NULL fraction. The NDV is within what the group allows: at least the distinct head values plus one for a
     * non-empty tail, at most the group NDV and the bound, the NDV of the columns the element is read from. Without a
     * bound the NDV does not pass the rows outside the head: an NDV of 1 from an unknown estimate would put the whole
     * tail on one value.
     */
    static ColumnStatistic elementStatistic(ColumnRefOperator element, List<ExternalMcvStatistics.Group> groups,
                                            List<List<ColumnRefOperator>> refs, ColumnStatistic base) {
        return elementStatistic(element, groups, refs, base, ndvOf(base));
    }

    // The NDV of the statistics of a column that is its own source; infinite for a statistic that is not known.
    private static double ndvOf(ColumnStatistic base) {
        return base == null || base.isUnknown() ? Double.POSITIVE_INFINITY : base.getDistinctValuesCount();
    }

    static ColumnStatistic elementStatistic(ColumnRefOperator element, List<ExternalMcvStatistics.Group> groups,
                                            List<List<ColumnRefOperator>> refs, ColumnStatistic base, double ndvBound) {
        ExternalMcvStatistics.Group best = null;
        int bestPosition = -1;
        long bestCoverage = -1;
        for (int i = 0; i < groups.size(); i++) {
            int position = refs.get(i).indexOf(element);
            if (refs.get(i).size() < 2 || position < 0 || groups.get(i).getRowCount() <= 0) {
                continue;
            }
            ExternalMcvStatistics.Group view = groups.get(i).elementGroup(position);
            long coverage = view.getMcv().stream().mapToLong(MultiColumnCombinedStats.McvEntry::getCount).sum();
            // The groups come in the order of the rows of the statistics query, which can differ between loads, so
            // groups of the same coverage are told apart by their element texts.
            if (coverage > bestCoverage || coverage == bestCoverage && best != null
                    && String.join(",", groups.get(i).getColumnNames())
                    .compareTo(String.join(",", best.getColumnNames())) < 0) {
                best = groups.get(i);
                bestPosition = position;
                bestCoverage = coverage;
            }
        }
        if (best == null) {
            return null;
        }
        return elementStatistic(best, bestPosition, element.getType(), base, ndvBound);
    }

    /**
     * ndvBound is the most distinct values the element can take, see sourceNdv; the bounds of its range and its other
     * statistics come from base.
     */
    static ColumnStatistic elementStatistic(ExternalMcvStatistics.Group group, int position,
                                            com.starrocks.type.Type type, ColumnStatistic base, double ndvBound) {
        ExternalMcvStatistics.Group view = group.elementGroup(position);
        boolean knownBase = base != null && !base.isUnknown();
        ColumnStatistic basic = knownBase ? base : ColumnStatistic.unknown();
        ColumnStatistic statistic = view.columnStatistic(type, basic).orElse(null);
        if (statistic == null) {
            return null;
        }
        long headRows = view.getMcv().stream().mapToLong(MultiColumnCombinedStats.McvEntry::getCount).sum();
        long nulls = view.getNullCounts().get(0);
        long tail = Math.max(0, group.getRowCount() - headRows - nulls);
        double lower = view.getMcv().size() + (tail > 0 ? 1 : 0);
        double ndv;
        // The NDV from the head or the group is collected, and not a guess whatever the base says. It is the guess of
        // the base when the NDV of the columns binds it: x has no more values than they have.
        boolean guess = false;
        if (tail == 0) {
            // The head lists every value of the element, so their number is its NDV; the NDV of the group counts
            // the combinations with the other elements.
            ndv = view.getMcv().size();
        } else if (!Double.isNaN(ndvBound) && !Double.isInfinite(ndvBound)) {
            ndv = Math.max(lower, Math.min(group.getNdv(), ndvBound));
            guess = knownBase && base.isDistinctValuesFromDefaultSelectivity() && ndv == ndvBound
                    && ndvBound < group.getNdv();
        } else {
            // The NDV of the group bounds the values of one element, and the rows outside the head hold at most one
            // value each.
            ndv = Math.max(lower, Math.min(group.getNdv(), lower + tail));
        }
        double min = knownBase ? base.getMinValue() : statistic.getMinValue();
        double max = knownBase ? base.getMaxValue() : statistic.getMaxValue();
        if (knownBase || tail == 0) {
            for (MultiColumnCombinedStats.McvEntry entry : view.getMcv()) {
                java.util.Optional<Double> value =
                        com.starrocks.statistic.StatisticUtils.convertStatisticsToDouble(type, entry.getValues().get(0));
                if (value.isPresent()) {
                    min = Math.min(min, value.get());
                    max = Math.max(max, value.get());
                }
            }
        }
        ColumnStatistic.Builder builder = ColumnStatistic.buildFrom(statistic).setDistinctValuesCount(ndv)
                .setDistinctValuesFromDefaultSelectivity(guess);
        if (min <= max) {
            builder.setMinValue(min).setMaxValue(max);
        }
        return builder.build();
    }
}
