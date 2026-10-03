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

import com.google.gson.Gson;
import com.starrocks.common.Config;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TResultBatch;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExternalPredicateColumnsStorageTest extends PlanTestBase {
    @Test
    void createsTableAndEscapesNames() {
        new StatisticsMetaManager().createStatisticsTablesForTest();
        ExternalPredicateColumnsStorage.createKeeper().run();
        assertNotNull(starRocksAssert.getTable(StatsConstants.STATISTICS_DB_NAME,
                ExternalPredicateColumnsStorage.TABLE_NAME));
        SimpleExecutor executor = mock(SimpleExecutor.class);
        var storage = new ExternalPredicateColumnsStorage(executor);
        LocalDateTime now = LocalDateTime.of(2026, 9, 23, 12, 0);
        var group = new ExternalColumnGroupUsage("uuid", "iceberg", "db", "t'1",
                List.of("列", "x'\\y"), ColumnUsage.UseCase.PREDICATE, now);
        storage.persist(List.of(group));
        storage.persist(List.of(group));
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(executor).executeDML(sql.capture());
        assertTrue(sql.getValue().contains("external_predicate_columns"));
        assertTrue(sql.getValue().contains("t''1"));
        // Parse the generated INSERT too: quotes/backslashes must not change the SQL structure.
        assertNotNull(com.starrocks.sql.parser.SqlParser.parseSingleStatement(sql.getValue(),
                connectContext.getSessionVariable().getSqlMode()));
    }

    @Test
    void failedWriteIsRetried() {
        SimpleExecutor executor = mock(SimpleExecutor.class);
        var storage = new ExternalPredicateColumnsStorage(executor);
        LocalDateTime now = LocalDateTime.of(2026, 9, 23, 12, 0);
        var group = new ExternalColumnGroupUsage("uuid", "cat", "db", "t", List.of("x", "y"),
                ColumnUsage.UseCase.JOIN, now);
        doThrow(new IllegalStateException("unavailable")).doNothing().when(executor).executeDML(anyString());
        assertThrows(IllegalStateException.class, () -> storage.persist(List.of(group)));
        storage.persist(List.of(group));
        verify(executor, times(2)).executeDML(anyString());
    }

    @Test
    void newlyVisibleObservationIsPersistedEvenWithEarlierTimestamp() {
        SimpleExecutor executor = mock(SimpleExecutor.class);
        var storage = new ExternalPredicateColumnsStorage(executor);
        LocalDateTime now = LocalDateTime.of(2026, 9, 23, 12, 0);
        var first = new ExternalColumnGroupUsage("uuid", "cat", "db", "t", List.of("x"),
                ColumnUsage.UseCase.PREDICATE, now);
        var delayed = new ExternalColumnGroupUsage("uuid", "cat", "db", "t", List.of("y"),
                ColumnUsage.UseCase.PREDICATE, now.minusSeconds(1));
        storage.persist(List.of(first));
        storage.persist(List.of(first, delayed));
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(executor, times(2)).executeDML(sql.capture());
        assertTrue(sql.getAllValues().get(1).contains(delayed.groupId()));
    }

    @Test
    void newRecorderCanReadPersistedHistoryAndReadFailureIsNotAbsence() {
        SimpleExecutor executor = mock(SimpleExecutor.class);
        String json = new Gson().toJson(Map.of("data", List.of("uuid", "cat", "db", "t",
                "[\"x\",\"y\"]", "predicate", "2026-09-23 12:00:00")));
        ByteBuffer buffer = ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8));
        TResultBatch batch = new TResultBatch();
        batch.setRows(List.of(buffer));
        when(executor.executeDQL(anyString())).thenThrow(new IllegalStateException("unavailable"))
                .thenReturn(List.of(batch));
        var storage = new ExternalPredicateColumnsStorage(executor);
        assertThrows(IllegalStateException.class, () -> storage.query("uuid"));
        assertEquals(List.of("x", "y"), storage.query("uuid").get(0).columns());
        assertEquals(List.of("x", "y"), storage.query("uuid").get(0).columns());
        assertEquals(0, buffer.position());
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(executor, times(3)).executeDQL(sql.capture());
        assertTrue(sql.getValue().contains("last_used >="));
        assertTrue(sql.getValue().contains("LIMIT 10000"));
        long ttl = Config.statistic_external_predicate_columns_ttl_hours;
        try {
            Config.statistic_external_predicate_columns_ttl_hours = -1;
            storage.query("uuid");
            verify(executor, times(4)).executeDQL(sql.capture());
            org.junit.jupiter.api.Assertions.assertFalse(sql.getValue().contains("last_used >="));
        } finally {
            Config.statistic_external_predicate_columns_ttl_hours = ttl;
        }
    }

    @Test
    void storedGroupRoundTripsWithoutMergingNames() {
        Gson gson = new Gson();
        List<String> columns = List.of("a,b", "quoted'", "列");
        String json = gson.toJson(Map.of("data", List.of("uuid", "cat", "db", "t",
                gson.toJson(columns), "join", "2026-09-23 12:00:00")));
        var group = ExternalPredicateColumnsStorage.parse(json);
        assertEquals(columns.stream().sorted().toList(), group.columns());
        assertEquals(ColumnUsage.UseCase.JOIN, group.useCase());
        assertEquals(LocalDateTime.of(2026, 9, 23, 12, 0), group.lastUsed());
        assertThrows(RuntimeException.class, () -> ExternalPredicateColumnsStorage.parse("{}"));
    }
}
