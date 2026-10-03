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

package com.starrocks.statistic.columns;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.statistic.StatisticUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.SetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class PredicateColumnsMgr {

    private static final Logger LOG = LogManager.getLogger(PredicateColumnsMgr.class);
    private static final PredicateColumnsMgr INSTANCE = new PredicateColumnsMgr();

    private final ExternalPredicateColumnGroups externalGroups = new ExternalPredicateColumnGroups();

    // Identity keys avoid allocating a timestamped usage record on each observation.
    private final Map<ColumnFullId, ColumnUsage> id2columnUsage = Maps.newConcurrentMap();

    // Keyed by hashed table_uuid; negative results (empty list, e.g. a table never queried) are
    // cached too, since a wide external table with no predicate columns yet would otherwise hit
    // the storage table on every auto-analyze cycle.
    private final Cache<String, List<ExternalColumnGroupUsage>> externalQueryCache = Caffeine.newBuilder()
            .expireAfterWrite(Config.statistic_external_predicate_columns_cache_ttl_sec, TimeUnit.SECONDS)
            .maximumWeight(ExternalPredicateColumnGroups.MAX_HISTORY_BYTES)
            .weigher((String key, List<ExternalColumnGroupUsage> groups) -> (int) Math.min(Integer.MAX_VALUE,
                    128L + groups.stream().mapToLong(ExternalColumnGroupUsage::estimatedMemoryBytes).sum()))
            .build();

    public static PredicateColumnsMgr getInstance() {
        return INSTANCE;
    }

    // ============================ Record predicate columns from query ========================= //
    public void recordScanColumns(Map<ColumnRefOperator, Column> scanColumns, Table table, OptExpression optExpr) {
        if (!Config.enable_predicate_columns_collection) {
            return;
        }
        if (table.isNativeTableOrMaterializedView()) {
            if (scanColumns.isEmpty()) {
                return;
            }
            Optional<Database> database = resolveUsageDatabase(table);
            if (database.isEmpty()) {
                return;
            }
            for (Column column : scanColumns.values()) {
                addOrUpdateNativeColumnUsage(database.get(), table, column, ColumnUsage.UseCase.NORMAL);
            }
        } else {
            externalGroups.recordColumns(table, scanColumns.values().stream().map(Column::getName).toList(),
                    ColumnUsage.UseCase.NORMAL);
        }
    }

    public void recordPredicateColumns(ScalarOperator predicate, ColumnRefFactory factory, OptExpression optExpr) {
        if (!Config.enable_predicate_columns_collection) {
            return;
        }
        if (predicate == null) {
            return;
        }
        List<ColumnRefOperator> refs = Utils.collect(predicate, ColumnRefOperator.class);
        addOrUpdateColumnUsage(refs, factory, ColumnUsage.UseCase.PREDICATE, optExpr);
        externalGroups.record(refs, ColumnUsage.UseCase.PREDICATE, factory, optExpr);
    }

    public void recordJoinPredicate(List<BinaryPredicateOperator> onPredicates, ColumnRefFactory factory,
                                    OptExpression optExpr) {
        if (!Config.enable_predicate_columns_collection) {
            return;
        }
        externalGroups.recordJoin(onPredicates, factory, optExpr);
        for (BinaryPredicateOperator op : onPredicates) {
            List<ColumnRefOperator> refs = Utils.collect(op, ColumnRefOperator.class);
            addOrUpdateColumnUsage(refs, factory, ColumnUsage.UseCase.JOIN, optExpr);
        }
    }

    public void recordGroupByColumns(Map<ColumnRefOperator, CallOperator> aggregations,
                                     List<ColumnRefOperator> groupBys,
                                     ColumnRefFactory factory, OptExpression optExpr) {
        if (!Config.enable_predicate_columns_collection) {
            return;
        }
        for (var entry : aggregations.entrySet()) {
            if (entry.getValue().isDistinct()) {
                List<ColumnRefOperator> refs = Utils.collect(entry.getValue(), ColumnRefOperator.class);
                addOrUpdateColumnUsage(refs, factory, ColumnUsage.UseCase.DISTINCT, optExpr);
                externalGroups.record(refs, ColumnUsage.UseCase.DISTINCT, factory, optExpr);
            }
        }

        addOrUpdateColumnUsage(groupBys, factory, ColumnUsage.UseCase.GROUP_BY, optExpr);
        externalGroups.record(groupBys, ColumnUsage.UseCase.GROUP_BY, factory, optExpr);
    }

    public void recordWindowPartitionBy(List<ScalarOperator> partitionByList, ColumnRefFactory factory,
                                        OptExpression optExpr) {
        if (!Config.enable_predicate_columns_collection) {
            return;
        }
        externalGroups.record(ListUtils.emptyIfNull(partitionByList).stream()
                        .flatMap(scalar -> Utils.extractColumnRef(scalar).stream()).toList(),
                ColumnUsage.UseCase.GROUP_BY, factory, optExpr);
        for (var partitionBy : ListUtils.emptyIfNull(partitionByList)) {
            List<ColumnRefOperator> refs = Utils.collect(partitionBy, ColumnRefOperator.class);
            addOrUpdateColumnUsage(refs, factory, ColumnUsage.UseCase.GROUP_BY, optExpr);
        }
    }

    private void addOrUpdateColumnUsage(List<ColumnRefOperator> refs, ColumnRefFactory factory,
                                        ColumnUsage.UseCase useCase, OptExpression optExpr) {
        for (ColumnRefOperator ref : ListUtils.emptyIfNull(refs)) {
            try {
                var tables = Utils.resolveColumnRefRecursive(ref, factory, optExpr);
                for (var column : ListUtils.emptyIfNull(tables)) {
                    addOrUpdateColumnUsage(column.first, column.second, useCase);
                }
            } catch (Exception e) {
                LOG.warn("failed to resolve column ref {} from expr {}", ref, optExpr);
            }
        }
    }

    private void addOrUpdateColumnUsage(Table table, Column column, ColumnUsage.UseCase useCase) {
        if (table.isNativeTableOrMaterializedView()) {
            addOrUpdateNativeColumnUsage(table, column, useCase);
        }
    }

    private void addOrUpdateNativeColumnUsage(Table table, Column column, ColumnUsage.UseCase useCase) {
        Optional<Database> database = resolveUsageDatabase(table);
        if (database.isEmpty()) {
            return;
        }
        addOrUpdateNativeColumnUsage(database.get(), table, column, useCase);
    }

    // Empty when the table has no database or the database is a system or internal one, which we never track.
    private Optional<Database> resolveUsageDatabase(Table table) {
        Optional<Database> database = table.mayGetDatabaseId()
                .flatMap(GlobalStateMgr.getCurrentState().getLocalMetastore()::mayGetDb);
        if (database.isEmpty() || Database.isSystemOrInternalDatabase(database.get().getFullName())) {
            return Optional.empty();
        }
        return database;
    }

    private void addOrUpdateNativeColumnUsage(Database database, Table table, Column column,
                                              ColumnUsage.UseCase useCase) {
        ColumnFullId id = ColumnFullId.create(database, table, column);
        // Serialize observations for the same column, including its mutable use-case set.
        id2columnUsage.compute(id, (key, usage) -> {
            if (usage == null) {
                return new ColumnUsage(key, new TableName(database.getFullName(), table.getName()), useCase);
            }
            usage.useNow(useCase);
            return usage;
        });
    }

    //==================================== Query ============================================ //
    public List<ColumnUsage> query(TableName tableName) {
        return queryByUseCase(tableName, ColumnUsage.UseCase.all());
    }

    public List<ColumnUsage> queryPredicateColumns(TableName tableName) {
        return queryByUseCase(tableName, ColumnUsage.UseCase.getPredicateColumnUseCase());
    }

    private List<ColumnUsage> queryByUseCase(TableName tableName, EnumSet<ColumnUsage.UseCase> useCases) {
        TableNamePredicate predicate = new TableNamePredicate(tableName);
        Predicate<ColumnUsage> useCasePredicate = (c) -> !SetUtils.intersection(c.getUseCases(), useCases).isEmpty();
        Predicate<ColumnUsage> pred = useCasePredicate.and(x -> predicate.test(x.getTableName()));
        if (FeConstants.runningUnitTest) {
            return id2columnUsage.values().stream().filter(pred).collect(Collectors.toList());
        } else {
            return getStorage().queryGlobalState(tableName, useCases).stream().filter(pred)
                    .collect(Collectors.toList());
        }
    }

    /** Basic statistics use the union of observed sets; single-column rows are not stored separately. */
    public List<String> queryExternalPredicateColumns(Table table) {
        return queryExternalPredicateColumnGroups(table).stream()
                .flatMap(group -> group.columns().stream()).distinct().sorted().toList();
    }

    /** Recent predicate sets from all FEs, including observations on this FE not yet flushed. */
    public List<ExternalColumnGroupUsage> queryExternalPredicateColumnGroups(Table table) {
        String uuid = StatisticUtils.hashTableUuidForPkStorage(table.getUUID());
        Map<String, ExternalColumnGroupUsage> groups = Maps.newHashMap();
        if (!FeConstants.runningUnitTest) {
            for (ExternalColumnGroupUsage group : externalQueryCache.get(uuid,
                    key -> ExternalPredicateColumnsStorage.getInstance().query(key))) {
                groups.merge(group.key(), group, ExternalColumnGroupUsage::newest);
            }
        }
        for (ExternalColumnGroupUsage group : externalGroups.snapshot()) {
            if (group.tableUuid().equals(uuid)) {
                groups.merge(group.key(), group, ExternalColumnGroupUsage::newest);
            }
        }
        LocalDateTime cutoff = Config.statistic_external_predicate_columns_ttl_hours < 0 ? LocalDateTime.MIN
                : TimeUtils.getSystemNow().minusHours(Config.statistic_external_predicate_columns_ttl_hours);
        return groups.values().stream()
                .filter(group -> group.useCase() != ColumnUsage.UseCase.NORMAL && !group.lastUsed().isBefore(cutoff))
                .sorted(java.util.Comparator.comparing(ExternalColumnGroupUsage::key)).toList();
    }

    //==================================== Maintenance ============================================ //
    public void persist() {
        getStorage().persist(id2columnUsage.values());
    }

    public void vacuum() {
        long ttlHour = Config.statistic_predicate_columns_ttl_hours;
        if (ttlHour < 0) {
            return;
        }
        LocalDateTime ttlTime = TimeUtils.getSystemNow().minusHours(ttlHour);
        Predicate<ColumnUsage> outdated = x -> x.getLastUsed().isBefore(ttlTime);

        long before = id2columnUsage.size();
        if (before > 0 && id2columnUsage.values().removeIf(outdated)) {
            long after = id2columnUsage.size();
            LOG.info("removed {} objects from predicate columns because of ttl {}", before - after,
                    Config.statistic_predicate_columns_ttl_hours);
        }

        // If the process crashed before vacuum the storage, the storage may be different from in-memory state,
        // but it doesn't matter. Because we will remove them finally.
        getStorage().vacuum(ttlTime);
    }

    public void restore() {
        List<ColumnUsage> state = getStorage().restore();
        for (ColumnUsage usage : ListUtils.emptyIfNull(state)) {
            id2columnUsage.merge(usage.getColumnFullId(), usage, ColumnUsage::merge);
        }
    }

    private PredicateColumnsStorage getStorage() {
        return PredicateColumnsStorage.getInstance();
    }

    @VisibleForTesting
    public void reset() {
        id2columnUsage.clear();
        externalQueryCache.invalidateAll();
        externalGroups.clear();
    }

    @VisibleForTesting
    public void recordColumnUsageForTest(Table table, Column column, ColumnUsage.UseCase useCase) {
        if (table.isNativeTableOrMaterializedView()) {
            addOrUpdateNativeColumnUsage(table, column, useCase);
        } else {
            externalGroups.recordColumns(table, List.of(column.getName()), useCase);
        }
    }

    public void startDaemon() {
        DaemonThread.getInstance().start();
    }

    static class DaemonThread extends FrontendDaemon {

        private static final DaemonThread INSTANCE = new DaemonThread();

        public DaemonThread() {
            super("predicate-columns-daemon-thread", Config.statistic_predicate_columns_persist_interval_sec * 1000L);
        }

        public static DaemonThread getInstance() {
            return INSTANCE;
        }

        @Override
        protected void runAfterCatalogReady() {
            setInterval(Config.statistic_predicate_columns_persist_interval_sec * 1000L);

            PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();

            try {
                ExternalPredicateColumnsStorage.getInstance().maintain(mgr.externalGroups);
            } catch (Exception e) {
                LOG.warn("failed to maintain external predicate column groups", e);
            }

            // Native usage retains its own persistence lifecycle. External singleton and multi-column
            // sets share the same store above; no per-column external mirror is maintained.
            PredicateColumnsStorage storage = PredicateColumnsStorage.getInstance();
            if (!storage.isSystemTableReady()) {
                LOG.warn("system table of predicate_columns is still not ready");
            } else if (!storage.isRestored()) {
                mgr.restore();
                storage.finishRestore();
            } else {
                mgr.vacuum();
                mgr.persist();
            }


        }
    }

}
