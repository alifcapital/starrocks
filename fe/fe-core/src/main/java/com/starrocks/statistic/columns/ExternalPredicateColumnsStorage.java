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

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.starrocks.common.Config;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Persists immutable observations; old observations need not be restored into the recording cache. */
public class ExternalPredicateColumnsStorage {
    private static final Logger LOG = LogManager.getLogger(ExternalPredicateColumnsStorage.class);
    public static final String TABLE_NAME = "external_predicate_columns";
    public static final String TABLE_FULL_NAME = StatsConstants.STATISTICS_DB_NAME + "." + TABLE_NAME;
    private static final TableKeeper KEEPER = new TableKeeper(StatsConstants.STATISTICS_DB_NAME, TABLE_NAME,
            "CREATE TABLE " + TABLE_NAME + " (fe_id INT NOT NULL, table_uuid VARCHAR(32) NOT NULL, " +
                    "group_id VARCHAR(32) NOT NULL, catalog_name STRING NOT NULL, db_name STRING NOT NULL, " +
                    "table_name STRING NOT NULL, column_names STRING NOT NULL, usage VARCHAR(32) NOT NULL, " +
                    "last_used DATETIME NOT NULL) PRIMARY KEY(fe_id, table_uuid, group_id) " +
                    "DISTRIBUTED BY HASH(table_uuid) BUCKETS 8 PROPERTIES('replication_num'='1')", null);
    private static final String SELECT = "SELECT table_uuid, catalog_name, db_name, table_name, column_names, " +
            "usage, last_used FROM " + TABLE_FULL_NAME;
    private static final ExternalPredicateColumnsStorage INSTANCE = new ExternalPredicateColumnsStorage();
    private final SimpleExecutor executor;
    private Map<String, ExternalColumnGroupUsage> persisted = Map.of();

    public ExternalPredicateColumnsStorage() {
        this(new SimpleExecutor("external_predicate_columns", TResultSinkType.HTTP_PROTOCAL));
        executor.setDop(1);
    }

    ExternalPredicateColumnsStorage(SimpleExecutor executor) {
        this.executor = executor;
    }

    public static ExternalPredicateColumnsStorage getInstance() {
        return INSTANCE;
    }

    public static TableKeeper createKeeper() {
        return KEEPER;
    }

    public void maintain(ExternalPredicateColumnGroups groups) {
        if (!KEEPER.isReady()) {
            return;
        }
        LocalDateTime nextPersist = TimeUtils.getSystemNow();
        persist(groups.snapshot());
        long ttl = Config.statistic_external_predicate_columns_ttl_hours;
        if (ttl >= 0 && GlobalStateMgr.getCurrentState().isLeader()) {
            // Also expire records belonging to removed FEs.
            executor.executeDML("DELETE FROM " + TABLE_FULL_NAME + " WHERE last_used < " +
                    quote(DateUtils.formatDateTimeUnix(nextPersist.minusHours(ttl))));
        }
    }

    void persist(List<ExternalColumnGroupUsage> snapshot) {
        List<ExternalColumnGroupUsage> changed = snapshot.stream()
                .filter(group -> !group.equals(persisted.get(group.key()))).toList();
        int feId = GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf().getFid();
        for (List<ExternalColumnGroupUsage> batch : ListUtils.partition(changed, 128)) {
            List<String> values = new ArrayList<>();
            for (ExternalColumnGroupUsage group : batch) {
                values.add("(" + feId + "," + quote(group.tableUuid()) + "," + quote(group.groupId()) + "," +
                        quote(group.catalogName()) + "," + quote(group.dbName()) + "," + quote(group.tableName()) + "," +
                        quote(group.columnsJson()) + "," + quote(group.useCase().toString()) + "," +
                        quote(DateUtils.formatDateTimeUnix(group.lastUsed())) + ")");
            }
            executor.executeDML("INSERT INTO " + TABLE_FULL_NAME +
                    " (fe_id,table_uuid,group_id,catalog_name,db_name,table_name,column_names,usage,last_used) VALUES " +
                    String.join(",", values));
        }
        // Acknowledge exactly this snapshot after every batch succeeds. Concurrent observations
        // remain different on the next pass, even if their timestamp was captured before this flush.
        Map<String, ExternalColumnGroupUsage> acknowledged = new HashMap<>();
        snapshot.forEach(group -> acknowledged.put(group.key(), group));
        persisted = acknowledged;
        if (!changed.isEmpty()) {
            LOG.info("persisted {} external predicate column groups", changed.size());
        }
    }

    public List<ExternalColumnGroupUsage> query(String tableUuid) {
        String sql = SELECT + " WHERE table_uuid = " + quote(tableUuid) + " AND usage <> 'normal'";
        long ttl = Config.statistic_external_predicate_columns_ttl_hours;
        if (ttl >= 0) {
            sql += " AND last_used >= " + quote(DateUtils.formatDateTimeUnix(TimeUtils.getSystemNow().minusHours(ttl)));
        }
        // Each FE has its own row. Merge below after reading a bounded set of recent observations.
        sql += " ORDER BY last_used DESC LIMIT " + ExternalPredicateColumnGroups.MAX_GROUPS;
        List<ExternalColumnGroupUsage> result = new ArrayList<>();
        for (TResultBatch batch : ListUtils.emptyIfNull(executor.executeDQL(sql))) {
            for (ByteBuffer row : batch.getRows()) {
                result.add(parse(StandardCharsets.UTF_8.decode(row.duplicate()).toString()));
            }
        }
        return result;
    }

    static ExternalColumnGroupUsage parse(String json) {
        JsonArray row = JsonParser.parseString(json).getAsJsonObject().getAsJsonArray("data");
        List<String> columns = new ArrayList<>();
        JsonParser.parseString(row.get(4).getAsString()).getAsJsonArray()
                .forEach(column -> columns.add(column.getAsString()));
        return new ExternalColumnGroupUsage(row.get(0).getAsString(), row.get(1).getAsString(), row.get(2).getAsString(),
                row.get(3).getAsString(), columns, ColumnUsage.UseCase.valueOf(row.get(5).getAsString().toUpperCase(
                        java.util.Locale.ROOT)), DateUtils.parseUnixDateTime(row.get(6).getAsString()));
    }

    private static String quote(String value) {
        return "'" + SqlUtils.escapeSqlString(value) + "'";
    }
}
