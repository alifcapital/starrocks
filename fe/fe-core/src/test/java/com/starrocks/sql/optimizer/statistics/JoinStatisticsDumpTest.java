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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.optimizer.dump.JoinStatisticsDump;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpSerializer;
import com.starrocks.statistic.JoinStatisticsDefinition;
import com.starrocks.statistic.JoinStatisticsMeta;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

class JoinStatisticsDumpTest {
    private final AtomicBoolean denied = new AtomicBoolean();
    private final AtomicBoolean unavailable = new AtomicBoolean();
    private final List<String> checked = new ArrayList<>();

    @BeforeEach
    void permissions() {
        UtFrameUtils.createDefaultCtx();
        new MockUp<Authorizer>() {
            @Mock
            public void checkTableAction(ConnectContext context, String catalog, String db, String table,
                                         PrivilegeType privilege) throws AccessDeniedException {
                Assertions.assertSame(ConnectContext.get(), context);
                Assertions.assertEquals(PrivilegeType.SELECT, privilege);
                checked.add(catalog + "." + db + "." + table);
                if (unavailable.get()) {
                    throw new IllegalStateException("access controller unavailable");
                }
                if (denied.get() && table.equals("b")) {
                    throw new AccessDeniedException("denied");
                }
            }
        };
    }

    private JoinStatisticsDump dump(String catalog) {
        var definition = new JoinStatisticsDefinition("sensitive_definition", List.of(
                new JoinStatisticsDefinition.Source(catalog, "db", "a", "transactions-uuid", List.of("status", "gate")),
                new JoinStatisticsDefinition.Source(catalog, "db", "b", "users-uuid", List.of("country"))),
                List.of(new JoinStatisticsDefinition.KeyDomain(Map.of(0, List.of("id"), 1, List.of("id")),
                        List.of("BIGINT"))), Map.of());
        var dump = new JoinStatisticsDump();
        dump.add(new JoinStatisticsMeta(1, definition, 2, 1, 1, "test", 1), JoinStatisticsCodecTest.fixture());
        return dump;
    }

    @Test
    void exportChecksEverySourceAndRechecksAfterRevocation() throws Exception {
        for (String catalog : List.of("default_catalog", "IcebergMixedCase")) {
            var dump = dump(catalog);
            checked.clear();
            denied.set(false);
            Assertions.assertEquals(1, dump.toJson().size());
            Assertions.assertEquals(List.of(catalog + ".db.a", catalog + ".db.b"), checked);
            denied.set(true);
            List<String> notices = new ArrayList<>();
            Assertions.assertEquals(0, dump.toJson(notices::add).size());
            Assertions.assertEquals(1, notices.size());
            Assertions.assertFalse(notices.get(0).contains("sensitive_definition"));
            Assertions.assertFalse(notices.get(0).contains("db.b"));
            Assertions.assertEquals(1, dump.entries().size(), "Export filtering must not change planning statistics");
            denied.set(false);
            unavailable.set(true);
            Assertions.assertEquals(0, dump.toJson().size());
            unavailable.set(false);
        }
    }

    @Test
    void missingIdentityCannotExportPayload() throws Exception {
        var dump = dump("iceberg");
        try (var ignored = new ConnectContext().bindScope()) {
            Assertions.assertEquals(0, dump.toJson().size());
        }
        Assertions.assertTrue(checked.isEmpty());
    }

    @Test
    void querySerializerOmitsUnauthorizedAndAnonymizedPayload() throws Exception {
        QueryDumpInfo info = new QueryDumpInfo(ConnectContext.get());
        info.setOriginStmt("select 1");
        var entry = dump("iceberg").entries().get(0);
        info.getJoinStatistics().add(entry.meta(), entry.data());
        var serializer = new QueryDumpSerializer();
        denied.set(true);
        var deniedJson = serializer.serialize(info, QueryDumpInfo.class, null).getAsJsonObject();
        Assertions.assertEquals(0, deniedJson.getAsJsonArray("join_statistics").size());
        Assertions.assertFalse(deniedJson.toString().contains("sensitive_definition"));
        Assertions.assertFalse(info.getExceptionList().isEmpty());
        denied.set(false);
        Assertions.assertEquals(1, serializer.serialize(info, QueryDumpInfo.class, null).getAsJsonObject()
                .getAsJsonArray("join_statistics").size());
        // Missing AST makes anonymization fail. Even its legacy ordinary-dump fallback must omit JOIN data.
        info.setDesensitizedInfo(true);
        Assertions.assertFalse(serializer.serialize(info, QueryDumpInfo.class, null).getAsJsonObject()
                .has("join_statistics"));
        boolean old = Config.enable_desensitize_query_dump;
        try {
            info.setDesensitizedInfo(false);
            Config.enable_desensitize_query_dump = true;
            Assertions.assertFalse(serializer.serialize(info, QueryDumpInfo.class, null).getAsJsonObject()
                    .has("join_statistics"));
            Assertions.assertEquals(0, info.getJoinStatistics().toJson().size());
        } finally {
            Config.enable_desensitize_query_dump = old;
        }
    }

    @Test
    void payloadRoundTripsAndTableIdsAreReboundWithoutChangingEstimates() throws Exception {
        var data = JoinStatisticsCodecTest.fixture();
        var definition = new JoinStatisticsDefinition("pair", List.of(
                new JoinStatisticsDefinition.Source("iceberg", "db", "a", "transactions-uuid", List.of("status", "gate")),
                new JoinStatisticsDefinition.Source("iceberg", "db", "b", "users-uuid", List.of("country"))),
                List.of(new JoinStatisticsDefinition.KeyDomain(Map.of(0, List.of("id"), 1, List.of("id")),
                        List.of("BIGINT"))), Map.of());
        var dump = new JoinStatisticsDump();
        dump.add(new JoinStatisticsMeta(1, definition, 2, 1, 1, "test", 1), data);
        var restored = new JoinStatisticsDump();
        restored.read(dump.toJson());
        var mapped = restored.remap(source -> new Table(source.getTableName().getTbl().equals("a") ? 10 : 20,
                source.getTableName().getTbl(), Table.TableType.OLAP, List.of()));
        Assertions.assertEquals("10", mapped.get(1).meta().getDefinition().getSources().get(0).getUuid());
        Assertions.assertEquals("20", mapped.get(1).data().getSources().get(1).getTableUuid());
        var partial = restored.remap(source -> source.getTableName().getTbl().equals("a")
                ? new Table(10, "a", Table.TableType.OLAP, List.of()) : null);
        Assertions.assertEquals("unbound-dump:users-uuid",
                partial.get(1).meta().getDefinition().getSources().get(1).getTableUuid());
        var original = data.getBases().get(0);
        var reloaded = mapped.get(1).data().getBases().get(0);
        Assertions.assertEquals(original.getSlices(0).get(0).getHead().product(
                        original.getSlices(1).get(0).getHead(), false, false, 1),
                reloaded.getSlices(0).get(0).getHead().product(reloaded.getSlices(1).get(0).getHead(), false, false, 1));
    }

    @Test
    void emptyReplayCannotReadLiveDefinitions() {
        ConnectContext context = new ConnectContext();
        context.setJoinStatisticsReplay(new JoinStatisticsDump());
        try (var ignored = context.bindScope()) {
            Assertions.assertFalse(new JoinStatisticsPlanner().hasDefinitions());
            Assertions.assertTrue(context.getSessionVariable().isCboEnableJoinStatisticsComposition());
        }
    }
}
