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

package com.starrocks.persist.gson;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.encryption.EncryptionKeyPBAdapter;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.persist.gson.internal.ColumnIdAdapter;
import com.starrocks.persist.gson.internal.GuavaMultimapAdapter;
import com.starrocks.persist.gson.internal.MapTypeDeserializer;
import com.starrocks.persist.gson.internal.ProcessHookTypeAdapterFactory;
import com.starrocks.persist.gson.internal.RangeAdapter;
import com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes;
import com.starrocks.persist.gson.internal.StructTypeDeserializer;
import com.starrocks.proto.EncryptionKeyPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.dump.HiveTableDumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.StructType;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes.DUMP_INFO_DESERIALIZER;
import static com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes.DUMP_INFO_SERIALIZER;
import static com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes.HIVE_TABLE_DUMP_INFO_DESERIALIZER;
import static com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes.HIVE_TABLE_DUMP_INFO_SERIALIZER;
import static com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes.LOCAL_DATE_TIME_TYPE_DESERIALIZER;
import static com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes.LOCAL_DATE_TIME_TYPE_SERIALIZER;
import static com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes.PRIMITIVE_TYPE_DESERIALIZER;

// The journal and the image must stay byte for byte the same. GuavaTableAdapter and the write of
// RuntimeTypeAdapterFactory stream; LegacyGuavaTableAdapter and LegacyRuntimeTypeAdapterFactory build JSON trees as
// they did before. This test builds a Gson with the tree-based ones and the same registrations as
// DefaultGsonBuilderFactory, writes the metadata of a cluster and journal objects with both, and expects the same
// JSON. It also reads the JSON with both and expects the objects they read to write the same JSON.
public class GsonStreamingEquivalenceTest extends PlanTestBase {

    private static Gson legacy;

    @BeforeAll
    public static void beforeAll() throws Exception {
        starRocksAssert.withMaterializedView("create materialized view mv_equivalence distributed by hash(v1) "
                + "refresh manual as select v1, sum(v2) as s from t0 group by v1");
        starRocksAssert.withView("create view v_equivalence as select v1, v3 from t0 where v2 > 1");
        legacy = legacyGson();
    }

    // DefaultGsonBuilderFactory.create with the tree-based adapters.
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Gson legacyGson() throws Exception {
        GsonBuilder builder = new GsonBuilder()
                .addSerializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
                .addDeserializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
                .enableComplexMapKeySerialization()
                .registerTypeHierarchyAdapter(Table.class, new LegacyGuavaTableAdapter())
                .registerTypeHierarchyAdapter(Multimap.class, new GuavaMultimapAdapter())
                .registerTypeHierarchyAdapter(ColumnId.class, new ColumnIdAdapter())
                .registerTypeHierarchyAdapter(Range.class, new RangeAdapter())
                .registerTypeAdapterFactory(new ProcessHookTypeAdapterFactory())
                .registerTypeAdapter(MapType.class, new MapTypeDeserializer())
                .registerTypeAdapter(StructType.class, new StructTypeDeserializer());

        for (RuntimeTypeAdapterFactory<?> factory :
                RuntimeTypeAdapterTypes.CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.values()) {
            Class<?> baseType = (Class<?>) field(factory, "baseType");
            String typeFieldName = (String) field(factory, "typeFieldName");
            boolean maintainType = (Boolean) field(factory, "maintainType");
            String defaultLabel = (String) field(factory, "defaultLabel");
            Map<String, Class<?>> labelToSubtype = (Map<String, Class<?>>) field(factory, "labelToSubtype");
            LegacyRuntimeTypeAdapterFactory legacyFactory =
                    LegacyRuntimeTypeAdapterFactory.of((Class) baseType, typeFieldName, maintainType);
            for (Map.Entry<String, Class<?>> entry : labelToSubtype.entrySet()) {
                legacyFactory.registerSubtype((Class) entry.getValue(), entry.getKey(),
                        entry.getKey().equals(defaultLabel));
            }
            builder.registerTypeAdapterFactory(legacyFactory);
        }

        builder.registerTypeAdapter(LocalDateTime.class, LOCAL_DATE_TIME_TYPE_SERIALIZER)
                .registerTypeAdapter(LocalDateTime.class, LOCAL_DATE_TIME_TYPE_DESERIALIZER)
                .registerTypeAdapter(QueryDumpInfo.class, DUMP_INFO_SERIALIZER)
                .registerTypeAdapter(QueryDumpInfo.class, DUMP_INFO_DESERIALIZER)
                .registerTypeAdapter(EncryptionKeyPB.class, new EncryptionKeyPBAdapter())
                .registerTypeAdapter(HiveTableDumpInfo.class, HIVE_TABLE_DUMP_INFO_SERIALIZER)
                .registerTypeAdapter(HiveTableDumpInfo.class, HIVE_TABLE_DUMP_INFO_DESERIALIZER)
                .registerTypeAdapter(PrimitiveType.class, PRIMITIVE_TYPE_DESERIALIZER);
        if (Config.metadata_ignore_unknown_subtype) {
            builder.registerTypeAdapterFactory(new SubtypeSkippingTypeAdapterFactory());
        }
        return builder.create();
    }

    private static Object field(Object object, String name) throws Exception {
        Field field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
    }

    // Both write the same JSON, and what each reads from it writes the same JSON again. The JSON read back can
    // differ from the first: a column type of an unregistered subclass of ScalarType is written without its type
    // and read as a ScalarType, which is then written with it.
    private static void assertSameJson(Object object, Type type) {
        String expected = legacy.toJson(object, type);
        Assertions.assertEquals(expected, GsonUtils.GSON.toJson(object, type), type.getTypeName());
        String readByLegacy = legacy.toJson(legacy.fromJson(expected, type), type);
        Object readByStreaming = GsonUtils.GSON.fromJson(expected, type);
        Assertions.assertEquals(readByLegacy, legacy.toJson(readByStreaming, type), type.getTypeName());
        Assertions.assertEquals(readByLegacy, GsonUtils.GSON.toJson(readByStreaming, type), type.getTypeName());
    }

    @Test
    public void catalogObjectsWriteTheSameJson() {
        int tables = 0;
        Set<String> tableClasses = new HashSet<>();
        for (Database db : GlobalStateMgr.getCurrentState().getLocalMetastore().getAllDbs()) {
            // the system databases are built at start and not written to the image
            if (db.isSystemDatabase()) {
                continue;
            }
            assertSameJson(db, Database.class);
            for (com.starrocks.catalog.Table table : db.getTables()) {
                assertSameJson(table, com.starrocks.catalog.Table.class);
                tables++;
                tableClasses.add(table.getClass().getSimpleName());
            }
        }
        Assertions.assertTrue(tables > 20, "tables " + tables);
        Assertions.assertTrue(tableClasses.containsAll(List.of("OlapTable", "MaterializedView", "View")),
                tableClasses.toString());
    }

    private static TransactionState loadTransaction() {
        EtlStatus status = new EtlStatus();
        TUniqueId loadId = new TUniqueId(0x1234L, 0x5678L);
        Set<TUniqueId> fragments = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            fragments.add(new TUniqueId(0x1234L, 0x9000L + i));
        }
        status.getLoadStatistic().initLoad(loadId, fragments, List.of(10001L, 10002L));
        int i = 0;
        for (TUniqueId fragment : fragments) {
            TReportExecStatusParams params = new TReportExecStatusParams();
            params.setQuery_id(loadId);
            params.setFragment_instance_id(fragment);
            params.setLoaded_rows(1000L * i);
            params.setSink_load_bytes(7L * i);
            params.setSource_load_rows(3L * i);
            params.setSource_load_bytes(5L * i);
            params.setFiltered_rows(i % 3);
            params.setUnselected_rows(i % 5);
            params.setSource_scan_bytes(Long.MAX_VALUE - i);
            status.getLoadStatistic().updateLoadProgress(params);
            i++;
        }
        TransactionState transaction = new TransactionState();
        transaction.setTxnCommitAttachment(new LoadJobFinalOperation(42L, status, 100, 1L, 2L, JobState.FINISHED,
                null));
        return transaction;
    }

    // The counters of a load: one cell per fragment instance in seven Guava tables, inside the attachment of a
    // transaction, which a RuntimeTypeAdapterFactory writes with its type.
    @Test
    public void loadCountersInATransactionWriteTheSameJson() {
        TransactionState transaction = loadTransaction();
        LoadJobFinalOperation attachment = (LoadJobFinalOperation) transaction.getTxnCommitAttachment();
        assertSameJson(attachment.getLoadingStatus(), EtlStatus.class);
        assertSameJson(transaction, TransactionState.class);
    }

    private static long allocatedBytes(Runnable work) {
        ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        long threadId = Thread.currentThread().getId();
        long before = bean.getThreadAllocatedBytes(threadId);
        work.run();
        return bean.getThreadAllocatedBytes(threadId) - before;
    }

    // A transaction with the counters of a load is what the journal holds for every INSERT. The streaming adapters
    // must allocate much less to write and to read it.
    @Test
    public void loadCountersAllocateLess() {
        TransactionState transaction = loadTransaction();
        String json = legacy.toJson(transaction, TransactionState.class);
        int rounds = 200;
        for (Gson gson : List.of(legacy, GsonUtils.GSON)) {
            for (int i = 0; i < rounds; i++) {
                gson.fromJson(gson.toJson(transaction, TransactionState.class), TransactionState.class);
            }
        }
        long legacyWrite = allocatedBytes(() -> {
            for (int i = 0; i < rounds; i++) {
                legacy.toJson(transaction, TransactionState.class);
            }
        });
        long streamingWrite = allocatedBytes(() -> {
            for (int i = 0; i < rounds; i++) {
                GsonUtils.GSON.toJson(transaction, TransactionState.class);
            }
        });
        long legacyRead = allocatedBytes(() -> {
            for (int i = 0; i < rounds; i++) {
                legacy.fromJson(json, TransactionState.class);
            }
        });
        long streamingRead = allocatedBytes(() -> {
            for (int i = 0; i < rounds; i++) {
                GsonUtils.GSON.fromJson(json, TransactionState.class);
            }
        });
        System.out.printf("GsonStreamingEquivalenceTest: %d B of JSON; per transaction write %d -> %d B, read %d -> %d B%n",
                json.length(), legacyWrite / rounds, streamingWrite / rounds, legacyRead / rounds, streamingRead / rounds);
        Assertions.assertTrue(streamingWrite * 2 < legacyWrite, legacyWrite + " -> " + streamingWrite);
        Assertions.assertTrue(streamingRead * 2 < legacyRead, legacyRead + " -> " + streamingRead);
    }

    static class GuavaTables {
        @SerializedName("counters")
        Table<String, String, Long> counters = HashBasedTable.create();
        @SerializedName("tablets")
        Table<Long, Long, Map<Long, Long>> tablets = HashBasedTable.create();
        @SerializedName("indexes")
        Table<Long, Long, MaterializedIndex> indexes = HashBasedTable.create();
        @SerializedName("empty")
        Table<String, String, Long> empty = HashBasedTable.create();
        @SerializedName("absent")
        Table<String, String, Long> absent = null;
        @SerializedName("list")
        List<Table<String, Integer, String>> list = new ArrayList<>();
    }

    @Test
    public void guavaTablesWriteTheSameJson() {
        GuavaTables tables = new GuavaTables();
        tables.counters.put("load-1", "frag \"1\"", 1L);
        tables.counters.put("load-1", "frag <2> & 'x'", -2L);
        tables.counters.put("загрузка", "фрагмент", Long.MIN_VALUE);
        for (long p = 0; p < 4; p++) {
            for (long idx = 0; idx < 3; idx++) {
                Map<Long, Long> tablets = new HashMap<>();
                tablets.put(p * 100 + idx, p * 1000 + idx);
                tablets.put(p * 100 + idx + 50, p * 1000 + idx + 50);
                tables.tablets.put(p, idx, tablets);
                tables.indexes.put(p, idx, new MaterializedIndex(p * 10 + idx, MaterializedIndex.IndexState.NORMAL));
            }
        }
        Table<String, Integer, String> inList = HashBasedTable.create();
        inList.put("a", 1, "x\ny");
        tables.list.add(inList);
        tables.list.add(HashBasedTable.create());
        assertSameJson(tables, GuavaTables.class);

        Type tableType = new TypeToken<Table<String, String, Long>>() { }.getType();
        assertSameJson(tables.counters, tableType);
        Assertions.assertEquals("null", GsonUtils.GSON.toJson(null, tableType));
        Assertions.assertNull(GsonUtils.GSON.fromJson("null", tableType));
    }

    // The reader takes the fields of a table in any order, as the tree did.
    @Test
    public void guavaTableFieldsInAnyOrder() {
        Type tableType = new TypeToken<Table<String, String, Long>>() { }.getType();
        Table<String, String, Long> table = GsonUtils.GSON.fromJson(
                "{\"cells\":[1,0,5,0,0,7],\"columnKeys\":[\"c\"],\"extra\":{\"x\":1},\"rowKeys\":[\"r0\",\"r1\"],"
                        + "\"clazz\":\"HashBasedTable\"}", tableType);
        Assertions.assertEquals(7L, table.get("r0", "c"));
        Assertions.assertEquals(5L, table.get("r1", "c"));
        Assertions.assertEquals(2, table.size());
    }
}
