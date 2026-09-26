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

package com.starrocks.persist;

import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Text;
import com.starrocks.journal.JournalEntity;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatisticsType;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.ExternalMcvStatsMeta;
import com.starrocks.statistic.JoinStatisticsDefinition;
import com.starrocks.statistic.JoinStatisticsMeta;
import com.starrocks.statistic.StatsConstants;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

class StatisticsJournalOpcodeTest {
    private JournalEntity roundTrip(short expectedOpcode, Consumer<EditLog> write) throws Exception {
        DataOutputBuffer buffer = new DataOutputBuffer();
        new MockUp<EditLog>() {
            @Mock
            public void logJsonObject(short opcode, Object object, WALApplier applier) throws Exception {
                buffer.writeShort(opcode);
                Text.writeString(buffer, GsonUtils.GSON.toJson(object));
            }
        };
        write.accept(new EditLog(new LinkedBlockingQueue<>()));
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(buffer.getData(), 0, buffer.getLength()))) {
            short opcode = input.readShort();
            Assertions.assertEquals(expectedOpcode, opcode, "Persisted opcode must remain stable");
            Assertions.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(opcode));
            JournalEntity entry = new JournalEntity(opcode, EditLogDeserializer.deserialize(opcode, input));
            Assertions.assertEquals(0, input.available());
            return entry;
        } finally {
            buffer.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void externalMcvUsesForkOpcodeAndReplays(boolean drop) throws Exception {
        ExternalMcvStatsMeta meta = new ExternalMcvStatsMeta("iceberg", "db", "transactions",
                List.of("status", "gate"), StatsConstants.AnalyzeType.FULL, List.of(StatisticsType.MCV),
                LocalDateTime.of(2026, 9, 27, 0, 0), Map.of());
        meta.setTableUUID("tx-uuid");
        JournalEntity entry = roundTrip((short) (drop ? 30001 : 30000), log -> {
            if (drop) {
                log.logRemoveExternalMcvStatsMeta(meta, null);
            } else {
                log.logAddExternalMcvStatsMeta(meta, null);
            }
        });
        ExternalMcvStatsMeta restored = Assertions.assertInstanceOf(ExternalMcvStatsMeta.class, entry.data());
        Assertions.assertEquals(GsonUtils.GSON.toJson(meta), GsonUtils.GSON.toJson(restored));
        AnalyzeMgr analyze = Mockito.mock(AnalyzeMgr.class);
        GlobalStateMgr state = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(state.getAnalyzeMgr()).thenReturn(analyze);
        new EditLog(new LinkedBlockingQueue<>()).loadJournal(state, entry);
        if (drop) {
            Mockito.verify(analyze).replayRemoveExternalMcvStatsMeta(restored);
        } else {
            Mockito.verify(analyze).replayAddExternalMcvStatsMeta(restored);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void joinStatisticsUsesForkOpcodeAndReplays(boolean drop) throws Exception {
        JoinStatisticsDefinition definition = new JoinStatisticsDefinition("tx_users", List.of(
                new JoinStatisticsDefinition.Source("iceberg", "db", "transactions", "tx-uuid", List.of("status")),
                new JoinStatisticsDefinition.Source("iceberg", "db", "users", "users-uuid", List.of("country"))),
                List.of(new JoinStatisticsDefinition.KeyDomain(Map.of(0, List.of("user_id"), 1, List.of("id")),
                        List.of("BIGINT"))), Map.of());
        JoinStatisticsMeta meta = new JoinStatisticsMeta(10, definition, 2, 1, 100, "digest", 1);
        JournalEntity entry = roundTrip((short) (drop ? 30003 : 30002),
                log -> log.logJoinStatistics(meta, drop, null));
        JoinStatisticsMeta restored = Assertions.assertInstanceOf(JoinStatisticsMeta.class, entry.data());
        Assertions.assertEquals(GsonUtils.GSON.toJson(meta), GsonUtils.GSON.toJson(restored));
        AnalyzeMgr analyze = Mockito.mock(AnalyzeMgr.class);
        GlobalStateMgr state = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(state.getAnalyzeMgr()).thenReturn(analyze);
        new EditLog(new LinkedBlockingQueue<>()).loadJournal(state, entry);
        Mockito.verify(analyze).replayJoinStatistics(restored, drop);
    }
}
