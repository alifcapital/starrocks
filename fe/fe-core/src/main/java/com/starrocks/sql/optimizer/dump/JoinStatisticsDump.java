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


package com.starrocks.sql.optimizer.dump;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.optimizer.statistics.JoinStatisticsCodec;
import com.starrocks.sql.optimizer.statistics.JoinStatisticsData;
import com.starrocks.statistic.JoinStatisticsDefinition;
import com.starrocks.statistic.JoinStatisticsMeta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/** Only generations consulted by this query; replay never reads the live statistics registry. */
public final class JoinStatisticsDump {
    public record Entry(JoinStatisticsMeta meta, JoinStatisticsData data) { }

    private final Map<Long, Entry> entries = new LinkedHashMap<>();

    public void add(JoinStatisticsMeta meta, JoinStatisticsData data) {
        entries.putIfAbsent(meta.getId(), new Entry(meta, data));
    }

    public List<Entry> entries() {
        return List.copyOf(entries.values());
    }

    public Entry get(long id) {
        return entries.get(id);
    }

    public void clear() {
        entries.clear();
    }

    public JsonArray toJson() throws IOException {
        return toJson(ignored -> { });
    }

    public JsonArray toJson(Consumer<String> omitted) throws IOException {
        JsonArray result = new JsonArray();
        if (Config.enable_desensitize_query_dump) {
            return result;
        }
        ConnectContext context = ConnectContext.get();
        for (Entry entry : entries.values()) {
            // A consulted object can include tables outside the query. Authorize the whole
            // payload at export time, including generations captured earlier or read from a dump.
            if (!canExport(context, entry.meta)) {
                // Do not reveal the definition name or the inaccessible source in the dump notice.
                omitted.accept("JOIN statistics omitted: SELECT on every source could not be verified");
                continue;
            }
            JsonObject object = new JsonObject();
            object.add("metadata", GsonUtils.GSON.toJsonTree(entry.meta));
            object.addProperty("payload", Base64.getEncoder().encodeToString(
                    JoinStatisticsCodec.encode(entry.data, Config.statistic_join_object_max_bytes)));
            result.add(object);
        }
        return result;
    }

    private static boolean canExport(ConnectContext context, JoinStatisticsMeta meta) {
        if (context == null || context.getCurrentUserIdentity() == null) {
            return false;
        }
        try {
            for (var source : meta.getDefinition().getSources()) {
                var name = source.getTableName();
                Authorizer.checkTableAction(context, source.getCatalogName(), name.getDb(), name.getTbl(),
                        PrivilegeType.SELECT);
            }
            return true;
        } catch (AccessDeniedException | RuntimeException e) {
            // Missing catalog, unavailable access controller or denied SELECT: fail closed for
            // this optional payload, without failing the user's query or the rest of the dump.
            return false;
        }
    }

    public void read(JsonArray array) throws IOException {
        long bytes = 0;
        for (var element : array) {
            JsonObject object = element.getAsJsonObject();
            JoinStatisticsMeta meta = GsonUtils.GSON.fromJson(object.get("metadata"), JoinStatisticsMeta.class);
            String encoded = object.get("payload").getAsString();
            if (encoded.length() > ((long) Config.statistic_join_object_max_bytes + 2) / 3 * 4) {
                throw new IOException("Oversized JOIN statistics dump payload");
            }
            JoinStatisticsData data = JoinStatisticsCodec.decode(Base64.getDecoder().decode(encoded),
                    Config.statistic_join_object_max_bytes, meta.getId(), meta.getGeneration());
            bytes += data.estimatedSize();
            if (bytes > 512L * 1024 * 1024) {
                throw new IOException("JOIN statistics dump exceeds the planner snapshot budget");
            }
            add(meta, data);
        }
    }

    /** Recreated tables have new IDs. Remap identities without recollecting or touching the global cache. */
    public JoinStatisticsDump remap(Function<JoinStatisticsDefinition.Source, Table> resolver) {
        JoinStatisticsDump result = new JoinStatisticsDump();
        for (Entry entry : entries.values()) {
            List<JoinStatisticsDefinition.Source> declared = new ArrayList<>();
            List<JoinStatisticsData.Source> sources = new ArrayList<>();
            var definition = entry.meta.getDefinition();
            for (int i = 0; i < definition.getSources().size(); i++) {
                var original = definition.getSources().get(i);
                Table table = resolver.apply(original);
                // A larger object may include a source outside the query's captured subplan.
                String physical = table == null ? "unbound-dump:" + original.getTableUuid() : table.getUUID();
                String uuid = original.getUuid().equals(original.getTableUuid()) ? physical : "role:" + i + ":" + physical;
                var name = original.getTableName();
                declared.add(new JoinStatisticsDefinition.Source(name.getCatalog(), name.getDb(), name.getTbl(),
                        uuid, physical, original.getPredicates()));
                var stored = entry.data.getSources().get(i);
                long[] rows = new long[stored.getTuples().size()];
                for (int slice = 0; slice < rows.length; slice++) {
                    rows[slice] = stored.getTupleRows(slice);
                }
                sources.add(new JoinStatisticsData.Source(uuid, stored.getSnapshot(), stored.getRows(),
                        stored.getColumns(), stored.getTypes(), stored.getTuples(), rows, stored.getDegrees()));
            }
            var mapped = new JoinStatisticsDefinition(definition.getName(), declared, definition.getDomains(),
                    definition.getProperties());
            var meta = entry.meta;
            result.add(new JoinStatisticsMeta(meta.getId(), mapped, meta.getGeneration(), meta.getParts(),
                            meta.getPayloadBytes(), meta.getChecksum(), meta.getCollectedAt()),
                    new JoinStatisticsData(meta.getId(), meta.getGeneration(), sources, entry.data.getBases(),
                            entry.data.getIntraCorrelations()));
        }
        return result;
    }
}
