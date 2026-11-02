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


package com.starrocks.statistic;

import com.starrocks.common.Config;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeStmtAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AnalyzeMcvDesc;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.parser.SqlParser;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ExtendedStatisticsScheduleTest {
    private static JoinStatisticsDefinition definition() {
        return new JoinStatisticsDefinition("tu", List.of(
                new JoinStatisticsDefinition.Source("iceberg", "db", "transactions", "t-uuid", List.of("status")),
                new JoinStatisticsDefinition.Source("iceberg", "db", "users", "u-uuid", List.of("country"))),
                List.of(new JoinStatisticsDefinition.KeyDomain(Map.of(0, List.of("uid"), 1, List.of("id")),
                        List.of("BIGINT"))), Map.of());
    }

    private static ExternalAnalyzeJob job(StatsConstants.AnalyzeType type, long id, List<String> columns) {
        return job(type, id, columns, Map.of(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL, "86400"));
    }

    private static ExternalAnalyzeJob job(StatsConstants.AnalyzeType type, long id, List<String> columns,
                                          Map<String, String> properties) {
        var job = new ExternalAnalyzeJob("iceberg", "db", "transactions", columns, List.of(), type,
                StatsConstants.ScheduleType.SCHEDULE, properties,
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN);
        job.setId(id);
        job.setTargetUuid("t-uuid");
        if (type == StatsConstants.AnalyzeType.JOIN) {
            job.setJoinStatisticsTarget(new JoinStatisticsMeta(10, definition()));
        }
        return job;
    }

    @Test
    void parserDistinguishesRecurringMcvJoinAndExistingBasicJobs() {
        var mcv = (CreateAnalyzeJobStmt) SqlParser.parseSingleStatement(
                "CREATE ANALYZE FULL TABLE ice.db.t MCV (status, gate) "
                        + "PROPERTIES ('statistic_auto_collect_interval'='86400')", 0);
        assertInstanceOf(AnalyzeMcvDesc.class, mcv.getAnalyzeTypeDesc());
        assertEquals(StatsConstants.AnalyzeType.MCV, mcv.getAnalyzeType());
        assertEquals(2, mcv.getColumns().size());
        assertFalse(mcv.isSample());
        var join = (CreateAnalyzeJobStmt) SqlParser.parseSingleStatement(
                "CREATE ANALYZE JOIN STATISTICS tu PROPERTIES ('statistic_auto_collect_interval'='3600')", 0);
        assertEquals("tu", join.getJoinStatisticsName());
        var basic = (CreateAnalyzeJobStmt) SqlParser.parseSingleStatement("CREATE ANALYZE FULL TABLE ice.db.t (id)", 0);
        assertEquals(StatsConstants.AnalyzeType.FULL, basic.getAnalyzeType());
        assertNull(basic.getAnalyzeTypeDesc());
        assertThrows(Exception.class, () -> SqlParser.parseSingleStatement("CREATE ANALYZE SAMPLE TABLE t MCV (id)", 0));
    }

    @Test
    void propertiesRejectSilentNoopsAndInvalidIntervals() {
        ExtendedStatisticsSchedule.validateProperties(
                Map.of("statistic_auto_collect_interval", "86400", "mcv_size", "128"), true);
        ExtendedStatisticsSchedule.validateProperties(Map.of("statistic_auto_collect_interval", "86400"), false);
        for (String value : List.of("0", "-1", "1.5", "999999999999999999999", "31536001")) {
            var error = assertThrows(SemanticException.class, () -> ExtendedStatisticsSchedule.validateProperties(
                    Map.of("statistic_auto_collect_interval", value), true));
            assertTrue(error.getMessage().contains("statistic_auto_collect_interval must be between"),
                    error.getMessage());
        }
        for (boolean mcv : List.of(true, false)) {
            var error = assertThrows(SemanticException.class, () -> ExtendedStatisticsSchedule.validateProperties(
                    Map.of("collect_interval_sec", "604800"), mcv));
            assertTrue(error.getMessage().contains("use 'statistic_auto_collect_interval'"), error.getMessage());
        }
        assertThrows(SemanticException.class,
                () -> ExtendedStatisticsSchedule.validateProperties(Map.of("mcv_size", "100"), false));
        assertThrows(SemanticException.class, () -> ExtendedStatisticsSchedule.validateProperties(Map.of("bogus", "1"), true));
    }

    @Test
    void intervalReadsStatisticAutoCollectIntervalAndJobsStoredWithCollectIntervalSec() {
        assertEquals(3600, ExtendedStatisticsSchedule.interval(job(StatsConstants.AnalyzeType.MCV, 7, List.of("status"),
                Map.of(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL, "3600"))));
        for (var type : List.of(StatsConstants.AnalyzeType.MCV, StatsConstants.AnalyzeType.JOIN)) {
            // The image and journal hold jobs created with collect_interval_sec.
            var stored = job(type, 8, List.of("status"), Map.of(StatsConstants.PROP_COLLECT_INTERVAL_SEC_KEY, "604800"));
            var restored = assertInstanceOf(ExternalAnalyzeJob.class,
                    GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(stored, AnalyzeJob.class), AnalyzeJob.class));
            assertEquals(type, restored.getAnalyzeType());
            assertEquals(604800, ExtendedStatisticsSchedule.interval(restored));
            assertEquals(Map.of("collect_interval_sec", "604800"), restored.getProperties());
        }
        assertEquals(3600, ExtendedStatisticsSchedule.interval(job(StatsConstants.AnalyzeType.MCV, 9, List.of("status"),
                Map.of(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL, "3600",
                        StatsConstants.PROP_COLLECT_INTERVAL_SEC_KEY, "604800"))));
        assertEquals(Config.statistic_auto_collect_large_table_interval,
                ExtendedStatisticsSchedule.interval(job(StatsConstants.AnalyzeType.MCV, 10, List.of("status"), Map.of())));
    }

    @Test
    void joinJobDdlTakesStatisticAutoCollectIntervalAndRejectsCollectIntervalSec() {
        // The parser needs the real global state, so parse before installing the fake one.
        var statement = (CreateAnalyzeJobStmt) SqlParser.parseSingleStatement(
                "CREATE ANALYZE JOIN STATISTICS tu PROPERTIES ('statistic_auto_collect_interval'='604800')", 0);
        var old = (CreateAnalyzeJobStmt) SqlParser.parseSingleStatement(
                "CREATE ANALYZE JOIN STATISTICS tu PROPERTIES ('collect_interval_sec'='604800')", 0);
        var mgr = installState(mock(JoinStatisticsManager.class));
        mgr.getJoinStatisticsRegistry().create(10, definition());
        var table = mock(com.starrocks.catalog.Table.class);
        when(table.getUUID()).thenReturn("t-uuid");
        new MockUp<com.starrocks.sql.common.MetaUtils>() {
            @Mock
            public com.starrocks.catalog.Table getSessionAwareTable(ConnectContext context,
                    com.starrocks.catalog.Database db, com.starrocks.catalog.TableName name) {
                return table;
            }
        };
        var context = mock(ConnectContext.class);
        AnalyzeStmtAnalyzer.analyze(statement, context);
        var created = ExtendedStatisticsSchedule.create(statement, context);
        assertEquals(StatsConstants.AnalyzeType.JOIN, created.getAnalyzeType());
        assertEquals(604800, ExtendedStatisticsSchedule.interval(created));

        var error = assertThrows(SemanticException.class, () -> AnalyzeStmtAnalyzer.analyze(old, context));
        assertTrue(error.getMessage().contains("use 'statistic_auto_collect_interval'"), error.getMessage());
    }

    @Test
    void scheduledRunWaitsForTheIntervalOfStoredAndNewJobs() {
        var mgr = installState(mock(JoinStatisticsManager.class));
        var table = mock(com.starrocks.catalog.Table.class);
        when(table.getUUID()).thenReturn("t-uuid");
        when(table.getColumn("status")).thenReturn(new com.starrocks.catalog.Column(
                "status", com.starrocks.type.VarcharType.VARCHAR));
        new MockUp<com.starrocks.sql.common.MetaUtils>() {
            @Mock
            public com.starrocks.catalog.Table getSessionAwareTable(ConnectContext context,
                    com.starrocks.catalog.Database db, com.starrocks.catalog.TableName name) {
                return table;
            }
        };
        var executor = mock(StatisticExecutor.class);
        var collected = new AtomicInteger();
        when(executor.collectStatistics(any(), any(), any(), eq(true), eq(true))).thenAnswer(call -> {
            var status = (AnalyzeStatus) call.getArgument(2);
            status.setStatus(StatsConstants.ScheduleStatus.FINISH);
            collected.incrementAndGet();
            return status;
        });
        // Two days must be past the default interval, so only the interval of the job keeps it waiting.
        assertTrue(Config.statistic_auto_collect_large_table_interval < 2 * 86400);
        boolean staggered = Config.enable_statistic_auto_collect_staggered_schedule;
        boolean enabled = Config.enable_statistic_collect;
        try {
            Config.enable_statistic_auto_collect_staggered_schedule = false;
            Config.enable_statistic_collect = true;
            long id = 7;
            for (String key : List.of(StatsConstants.PROP_COLLECT_INTERVAL_SEC_KEY,
                    StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL)) {
                var stored = job(StatsConstants.AnalyzeType.MCV, id++, List.of("status"), Map.of(key, "604800"));
                var value = (ExternalAnalyzeJob) GsonUtils.GSON.fromJson(
                        GsonUtils.GSON.toJson(stored, AnalyzeJob.class), AnalyzeJob.class);
                mgr.replayAddAnalyzeJob(value);
                int before = collected.get();
                value.setWorkTime(AutoStatisticsSchedule.now().minusDays(2));
                ExtendedStatisticsSchedule.run(value, mock(ConnectContext.class), executor, false);
                assertEquals(before, collected.get(), key);
                assertEquals(StatsConstants.ScheduleStatus.PENDING, value.getStatus(), key);
                value.setWorkTime(AutoStatisticsSchedule.now().minusDays(8));
                ExtendedStatisticsSchedule.run(value, mock(ConnectContext.class), executor, false);
                assertEquals(before + 1, collected.get(), key);
                assertEquals(StatsConstants.ScheduleStatus.FINISH, value.getStatus(), value.getReason());
            }
        } finally {
            Config.enable_statistic_auto_collect_staggered_schedule = staggered;
            Config.enable_statistic_collect = enabled;
        }
    }

    @Test
    void imageRoundTripRetainsTargetIdentityAndCalendarWithoutChangingBasicJobs() {
        for (var type : List.of(StatsConstants.AnalyzeType.MCV, StatsConstants.AnalyzeType.JOIN)) {
            var original = job(type, 7, List.of("status", "gate"));
            var now = LocalDateTime.of(2026, 9, 30, 12, 0);
            original.getCollectSchedule().due("extended:7", 86400, now, new AutoStatisticsSchedule.Window(0, 86400, "UTC"));
            var restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(original, AnalyzeJob.class), AnalyzeJob.class);
            var copy = assertInstanceOf(ExternalAnalyzeJob.class, restored);
            assertEquals(type, copy.getAnalyzeType());
            assertEquals(original.getTargetUuid(), copy.getTargetUuid());
            assertEquals(original.getJoinStatisticsId(), copy.getJoinStatisticsId());
            assertEquals(original.getCollectSchedule().getNext("extended:7"),
                    copy.getCollectSchedule().getNext("extended:7"));
            assertTrue(ExtendedStatisticsSchedule.sameTarget(original, copy));
        }
        var basic = job(StatsConstants.AnalyzeType.FULL, 8, List.of());
        assertFalse(GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(basic), ExternalAnalyzeJob.class).isExtendedStatistics());
    }

    @Test
    void oldScheduleCannotCollectARecreatedJoinAndDropRevokesInFlightPublication() {
        var registry = new JoinStatisticsRegistry((meta, drop, apply) -> apply.run());
        registry.create(10, definition());
        var ticket = registry.begin("tu", 11, 10);
        assertThrows(IllegalStateException.class, () -> registry.begin("tu", 12, 10));
        registry.drop("tu", false);
        registry.create(20, definition());
        assertFalse(registry.publish(ticket, 1, 100, "checksum", 1));
        assertThrows(IllegalStateException.class, () -> registry.begin("tu", 21, 10));
        assertEquals(20, registry.begin("tu", 21, 20).getPrevious().getId());
    }

    private static AnalyzeMgr installState(JoinStatisticsManager manager) {
        var fakeState = mock(GlobalStateMgr.class);
        var mgr = new AnalyzeMgr();
        var log = mock(EditLog.class);
        var variables = mock(com.starrocks.qe.VariableMgr.class);
        when(variables.getDefaultSessionVariable()).thenReturn(new com.starrocks.qe.SessionVariable());
        when(fakeState.getVariableMgr()).thenReturn(variables);
        when(fakeState.isLeader()).thenReturn(true);
        when(fakeState.getAnalyzeMgr()).thenReturn(mgr);
        when(fakeState.getEditLog()).thenReturn(log);
        var metadata = mock(com.starrocks.server.MetadataMgr.class);
        when(metadata.getDb(any(), anyString(), anyString())).thenReturn(new com.starrocks.catalog.Database(1, "db"));
        when(fakeState.getMetadataMgr()).thenReturn(metadata);
        when(fakeState.getNextId()).thenReturn(100L);
        doAnswer(call -> {
            ((WALApplier) call.getArgument(1)).apply(null);
            return null;
        })
                .when(log).logAddAnalyzeJob(any(), any());
        doAnswer(call -> {
            ((WALApplier) call.getArgument(1)).apply(null);
            return null;
        })
                .when(log).logRemoveAnalyzeJob(any(), any());
        doAnswer(call -> {
            ((WALApplier) call.getArgument(2)).apply(null);
            return null;
        })
                .when(log).logJoinStatistics(any(), anyBoolean(), any());
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return fakeState;
            }
        };
        new MockUp<AnalyzeMgr>() {
            @Mock
            public JoinStatisticsManager getJoinStatisticsManager() {
                return manager;
            }
        };
        new MockUp<StatisticAutoCollector>() {
            @Mock
            public static boolean checkoutAnalyzeTime() {
                return true;
            }
        };
        return mgr;
    }

    @Test
    void removedJobsCannotBeResurrectedAndMcvDropOnlyRemovesItsOwnGroup() {
        var mgr = installState(mock(JoinStatisticsManager.class));
        var one = job(StatsConstants.AnalyzeType.MCV, 1, List.of("status", "gate"));
        var other = job(StatsConstants.AnalyzeType.MCV, 2, List.of("status", "kind"));
        var basic = job(StatsConstants.AnalyzeType.FULL, 3, List.of("status"));
        for (var value : List.of(one, other, basic)) {
            mgr.replayAddAnalyzeJob(value);
        }
        mgr.removeMcvAnalyzeJobs("iceberg", "db", "transactions", List.of("gate", "status"));
        assertNull(mgr.getAnalyzeJob(1));
        assertSame(other, mgr.getAnalyzeJob(2));
        assertSame(basic, mgr.getAnalyzeJob(3));
        mgr.updateAnalyzeJobWithoutLog(one);
        mgr.updateAnalyzeJobWithLog(one);
        assertNull(mgr.getAnalyzeJob(1));
    }

    @Test
    void scheduledMcvUsesTheExactGroupCollectorAndDoesNotRepeatBeforeInterval() {
        var mgr = installState(mock(JoinStatisticsManager.class));
        var table = mock(com.starrocks.catalog.Table.class);
        when(table.getUUID()).thenReturn("t-uuid");
        when(table.getColumn("status")).thenReturn(new com.starrocks.catalog.Column(
                "status", com.starrocks.type.VarcharType.VARCHAR));
        new MockUp<com.starrocks.sql.common.MetaUtils>() {
            @Mock
            public com.starrocks.catalog.Table getSessionAwareTable(ConnectContext context,
                    com.starrocks.catalog.Database db, com.starrocks.catalog.TableName name) {
                return table;
            }
        };
        var value = job(StatsConstants.AnalyzeType.MCV, 7, List.of("status"));
        mgr.replayAddAnalyzeJob(value);
        var executor = mock(StatisticExecutor.class);
        var collected = new AtomicInteger();
        when(executor.collectStatistics(any(), any(), any(), eq(true), eq(true))).thenAnswer(call -> {
            var collect = assertInstanceOf(ExternalMcvStatisticsCollectJob.class, call.getArgument(1));
            assertEquals(List.of(List.of("status")), collect.getColumnGroups());
            assertEquals(List.of(com.starrocks.sql.ast.StatisticsType.MCV), collect.getStatisticsTypes());
            var status = (AnalyzeStatus) call.getArgument(2);
            assertEquals(StatsConstants.AnalyzeType.MCV, status.getType());
            status.setStatus(StatsConstants.ScheduleStatus.FINISH);
            collected.incrementAndGet();
            return status;
        });
        boolean staggered = Config.enable_statistic_auto_collect_staggered_schedule;
        boolean enabled = Config.enable_statistic_collect;
        try {
            Config.enable_statistic_auto_collect_staggered_schedule = false;
            Config.enable_statistic_collect = true;
            ExtendedStatisticsSchedule.run(value, mock(ConnectContext.class), executor, false);
            assertEquals(StatsConstants.ScheduleStatus.FINISH, value.getStatus(), value.getReason());
            ExtendedStatisticsSchedule.run(value, mock(ConnectContext.class), executor, false);
            // An immediate task queued at CREATE must not repeat a collection already done by the periodic worker.
            ExtendedStatisticsSchedule.run(value, mock(ConnectContext.class), executor, true);
            assertEquals(1, collected.get());
        } finally {
            Config.enable_statistic_auto_collect_staggered_schedule = staggered;
            Config.enable_statistic_collect = enabled;
        }
    }

    @Test
    void scheduledMcvExpressionOfUnknownFormatFailsWithReason() {
        var mgr = installState(mock(JoinStatisticsManager.class));
        var table = mock(com.starrocks.catalog.Table.class);
        when(table.getUUID()).thenReturn("t-uuid");
        new MockUp<com.starrocks.sql.common.MetaUtils>() {
            @Mock
            public com.starrocks.catalog.Table getSessionAwareTable(ConnectContext context,
                    com.starrocks.catalog.Database db, com.starrocks.catalog.TableName name) {
                return table;
            }
        };
        var value = job(StatsConstants.AnalyzeType.MCV, 7, List.of("expr:2:lower(`status`)"));
        mgr.replayAddAnalyzeJob(value);
        var executor = mock(StatisticExecutor.class);
        boolean staggered = Config.enable_statistic_auto_collect_staggered_schedule;
        boolean enabled = Config.enable_statistic_collect;
        try {
            Config.enable_statistic_auto_collect_staggered_schedule = false;
            Config.enable_statistic_collect = true;
            ExtendedStatisticsSchedule.run(value, mock(ConnectContext.class), executor, false);
            assertEquals(StatsConstants.ScheduleStatus.FAILED, value.getStatus());
            assertTrue(value.getReason().contains("Unsupported format of MCV expression"), value.getReason());
            verifyNoInteractions(executor);
        } finally {
            Config.enable_statistic_auto_collect_staggered_schedule = staggered;
            Config.enable_statistic_collect = enabled;
        }
    }

    @Test
    void joinJobCreationChecksEverySourceAndKillCancelsOnlyTheCapturedGeneration() {
        var manager = mock(JoinStatisticsManager.class);
        var mgr = installState(manager);
        var meta = new JoinStatisticsMeta(10, definition());
        var statement = new CreateAnalyzeJobStmt(false, Map.of(), com.starrocks.sql.parser.NodePosition.ZERO);
        statement.setJoinStatisticsName("tu");
        statement.setJoinStatistics(meta);
        var checked = new java.util.ArrayList<String>();
        new MockUp<com.starrocks.sql.analyzer.Authorizer>() {
            @Mock
            public void checkActionForAnalyzeStatement(ConnectContext context, com.starrocks.catalog.TableName name) {
                checked.add(name.getTbl());
                if (name.getTbl().equals("users")) {
                    throw new SemanticException("denied");
                }
            }
        };
        assertThrows(SemanticException.class, () -> new com.starrocks.sql.analyzer.AuthorizerStmtVisitor()
                .visitCreateAnalyzeJobStatement(statement, mock(ConnectContext.class)));
        assertEquals(List.of("transactions", "users"), checked);
        var status = new ExternalAnalyzeStatus(100, "iceberg", "db", "transactions", "t-uuid", List.of(),
                StatsConstants.AnalyzeType.JOIN, StatsConstants.ScheduleType.SCHEDULE, Map.of(), LocalDateTime.now());
        status.setStatus(StatsConstants.ScheduleStatus.RUNNING);
        status.setJoinCollectionGeneration(27);
        mgr.replayAddAnalyzeStatus(status);
        mgr.killConnection(100);
        verify(manager).cancelCollection(27);
    }

    @Test
    void slotsAreSeededOutsideTheCollectionWindowAndRestartsSkipMissedBacklog() {
        var manager = mock(JoinStatisticsManager.class);
        var mgr = installState(manager);
        var first = job(StatsConstants.AnalyzeType.MCV, 7, List.of("status"));
        var second = job(StatsConstants.AnalyzeType.MCV, 8, List.of("gate"));
        mgr.replayAddAnalyzeJob(first);
        mgr.replayAddAnalyzeJob(second);
        var clock = new java.util.concurrent.atomic.AtomicReference<>(LocalDateTime.of(2026, 9, 30, 12, 0));
        new MockUp<AutoStatisticsSchedule>() {
            @Mock
            public LocalDateTime now() {
                return clock.get();
            }
        };
        new MockUp<StatisticAutoCollector>() {
            @Mock
            public boolean checkoutAnalyzeTime() {
                return false;
            }
        };
        boolean staggered = Config.enable_statistic_auto_collect_staggered_schedule;
        boolean enabled = Config.enable_statistic_collect;
        String from = Config.statistic_auto_analyze_start_time;
        String to = Config.statistic_auto_analyze_end_time;
        try {
            Config.enable_statistic_auto_collect_staggered_schedule = true;
            Config.enable_statistic_collect = true;
            Config.statistic_auto_analyze_start_time = "01:00:00";
            Config.statistic_auto_analyze_end_time = "05:00:00";
            for (var value : List.of(first, second)) {
                ExtendedStatisticsSchedule.run(value, mock(ConnectContext.class), mock(StatisticExecutor.class), false);
                assertNotNull(value.getCollectSchedule().getNext("extended:" + value.getId()));
                assertEquals(StatsConstants.ScheduleStatus.PENDING, value.getStatus());
            }
            assertNotEquals(first.getCollectSchedule().getNext("extended:7"), second.getCollectSchedule().getNext("extended:8"));
            var restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(first), ExternalAnalyzeJob.class);
            mgr.replayAddAnalyzeJob(restored);
            clock.set(clock.get().plusDays(10));
            ExtendedStatisticsSchedule.run(restored, mock(ConnectContext.class), mock(StatisticExecutor.class), false);
            assertTrue(restored.getCollectSchedule().getNext("extended:7").isAfter(clock.get()));
            verifyNoInteractions(manager);
        } finally {
            Config.enable_statistic_auto_collect_staggered_schedule = staggered;
            Config.enable_statistic_collect = enabled;
            Config.statistic_auto_analyze_start_time = from;
            Config.statistic_auto_analyze_end_time = to;
        }
    }

    @Test
    void duplicateTargetIsRejectedEvenWithAnotherIntervalAndColumnOrder() throws Exception {
        var mgr = installState(mock(JoinStatisticsManager.class));
        var original = job(StatsConstants.AnalyzeType.MCV, 7, List.of("status", "gate"));
        mgr.replayAddAnalyzeJob(original);
        var same = job(StatsConstants.AnalyzeType.MCV, 8, List.of("gate", "status"));
        assertThrows(com.starrocks.common.AlreadyExistsException.class, () -> mgr.addAnalyzeJob(same));
        var different = job(StatsConstants.AnalyzeType.MCV, 9, List.of("status", "kind"));
        mgr.addAnalyzeJob(different);
        assertSame(different, mgr.getAnalyzeJob(different.getId()));
    }

    @Test
    void failedJoinRefreshKeepsPublishedGenerationAndDoesNotRetryEveryPoll() throws Exception {
        var manager = mock(JoinStatisticsManager.class);
        var mgr = installState(manager);
        var registry = mgr.getJoinStatisticsRegistry();
        registry.create(10, definition());
        var ticket = registry.begin("tu", 11);
        assertTrue(registry.publish(ticket, 1, 100, "checksum", 1));
        registry.finish(ticket);
        var previous = registry.get("tu");
        var value = job(StatsConstants.AnalyzeType.JOIN, 7, List.of());
        mgr.replayAddAnalyzeJob(value);
        org.mockito.Mockito.doThrow(new com.starrocks.common.DdlException("read failed"))
                .when(manager).analyze(eq("tu"), eq(false), any(), eq(10L));
        boolean staggered = Config.enable_statistic_auto_collect_staggered_schedule;
        boolean enabled = Config.enable_statistic_collect;
        try {
            Config.enable_statistic_auto_collect_staggered_schedule = false;
            Config.enable_statistic_collect = true;
            ExtendedStatisticsSchedule.run(value, mock(ConnectContext.class), mock(StatisticExecutor.class), false);
            assertEquals(StatsConstants.ScheduleStatus.FAILED, value.getStatus());
            assertTrue(value.getReason().contains("read failed"));
            assertSame(previous, registry.get("tu"));
            ExtendedStatisticsSchedule.run(value, mock(ConnectContext.class), mock(StatisticExecutor.class), false);
            verify(manager).analyze(eq("tu"), eq(false), any(), eq(10L));
        } finally {
            Config.enable_statistic_auto_collect_staggered_schedule = staggered;
            Config.enable_statistic_collect = enabled;
        }
    }

    @Test
    void disabledImmediateTriggerDoesNotSubmitWorkAndConcurrentRunDoesNotBlock() {
        var value = job(StatsConstants.AnalyzeType.JOIN, 7, List.of());
        boolean immediate = Config.enable_trigger_analyze_job_immediate;
        try {
            Config.enable_trigger_analyze_job_immediate = false;
            // No global state is needed: disabled trigger must not access or submit to the pool.
            ExtendedStatisticsSchedule.trigger(value);
            assertTrue(value.beginExtendedRun());
            assertFalse(value.beginExtendedRun());
            value.endExtendedRun();
            assertTrue(value.beginExtendedRun());
        } finally {
            value.endExtendedRun();
            Config.enable_trigger_analyze_job_immediate = immediate;
        }
    }

    @Test
    void periodicRunConsumesOneIntervalAndDropDuringCollectionDoesNotRestoreJob() throws Exception {
        var manager = mock(JoinStatisticsManager.class);
        var mgr = installState(manager);
        mgr.getJoinStatisticsRegistry().create(10, definition());
        var job = job(StatsConstants.AnalyzeType.JOIN, 7, List.of());
        mgr.replayAddAnalyzeJob(job);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var runs = new AtomicInteger();
        doAnswer(call -> {
            runs.incrementAndGet();
            entered.countDown();
            assertTrue(release.await(10, TimeUnit.SECONDS));
            return null;
        }).when(manager).analyze(eq("tu"), eq(false), any(), eq(10L));
        boolean staggered = Config.enable_statistic_auto_collect_staggered_schedule;
        boolean enabled = Config.enable_statistic_collect;
        var pool = Executors.newSingleThreadExecutor();
        try {
            Config.enable_statistic_auto_collect_staggered_schedule = false;
            Config.enable_statistic_collect = true;
            var future = pool.submit(() -> ExtendedStatisticsSchedule.run(job, mock(ConnectContext.class),
                    mock(StatisticExecutor.class), false));
            if (!entered.await(10, TimeUnit.SECONDS)) {
                future.get(1, TimeUnit.SECONDS);
                fail("Collection was not entered: " + job.getStatus() + " " + job.getReason());
            }
            ExtendedStatisticsSchedule.run(job, mock(ConnectContext.class), mock(StatisticExecutor.class), false);
            assertEquals(1, runs.get(), "Auto collection must skip an already running immediate/periodic job");
            mgr.removeAnalyzeJob(job.getId());
            release.countDown();
            future.get(10, TimeUnit.SECONDS);
            assertNull(mgr.getAnalyzeJob(job.getId()));
            ExtendedStatisticsSchedule.run(job, mock(ConnectContext.class), mock(StatisticExecutor.class), false);
            assertEquals(1, runs.get());
        } finally {
            release.countDown();
            pool.shutdownNow();
            Config.enable_statistic_auto_collect_staggered_schedule = staggered;
            Config.enable_statistic_collect = enabled;
        }
    }
}
