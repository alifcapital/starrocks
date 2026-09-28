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
import com.starrocks.catalog.TableName;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.JoinStatisticsStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.JoinStatisticsManager;
import com.starrocks.statistic.JoinStatisticsRegistry;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class JoinStatisticsInspectionStatementTest {
    @Test
    void commandChecksAllSourcePrivilegesBeforeLoadingAndReturnsOnlyRequestedPage() throws Exception {
        var context = UtFrameUtils.createDefaultCtx();
        var meta = JoinStatisticsInspectionTest.meta();
        var data = JoinStatisticsInspectionTest.fixture();
        var analyze = mock(AnalyzeMgr.class);
        var registry = mock(JoinStatisticsRegistry.class);
        var manager = mock(JoinStatisticsManager.class);
        when(analyze.getJoinStatisticsRegistry()).thenReturn(registry);
        when(analyze.getJoinStatisticsManager()).thenReturn(manager);
        when(registry.get("test")).thenReturn(meta);
        when(manager.inspect(eq(meta), anyLong())).thenReturn(Optional.of(data));
        new MockUp<GlobalStateMgr>() {
            @Mock
            public AnalyzeMgr getAnalyzeMgr() {
                return analyze;
            }
        };
        AtomicBoolean denySecond = new AtomicBoolean(true);
        AtomicInteger checks = new AtomicInteger();
        new MockUp<Authorizer>() {
            @Mock
            public void checkTableAction(ConnectContext ctx, TableName name, PrivilegeType action) throws AccessDeniedException {
                assertEquals(PrivilegeType.SELECT, action);
                checks.incrementAndGet();
                if (denySecond.get() && name.getTbl().equals("t1")) {
                    throw new AccessDeniedException();
                }
            }
        };
        AtomicReference<ShowResultSet> sent = new AtomicReference<>();
        new MockUp<StmtExecutor>() {
            @Mock
            public void sendShowResult(ShowResultSet result) {
                sent.set(result);
            }
        };
        var statement = (JoinStatisticsStmt) SqlParser.parseSingleStatement(
                "SHOW VERBOSE JOIN STATISTICS test LIMIT 2 OFFSET 3", context.getSessionVariable().getSqlMode());
        var executor = new StmtExecutor(context, statement);
        assertEquals(context.getSessionVariable().getQueryTimeoutS(), executor.getExecTimeout());
        var execute = StmtExecutor.class.getDeclaredMethod("handleJoinStatisticsStmt");
        execute.setAccessible(true);
        var denied = assertThrows(InvocationTargetException.class, () -> execute.invoke(executor));
        assertInstanceOf(DdlException.class, denied.getCause());
        assertEquals(2, checks.get());
        verifyNoInteractions(manager);
        denySecond.set(false);
        execute.invoke(executor);
        assertEquals(4, checks.get());
        verify(manager).inspect(meta, context.getSessionVariable().getQueryTimeoutS() * 1000L);
        assertEquals(JoinStatisticsInspection.show(meta, data, 3, 2).getResultRows(), sent.get().getResultRows());
        when(manager.inspect(eq(meta), anyLong())).thenReturn(Optional.empty());
        var failed = assertThrows(InvocationTargetException.class, () -> execute.invoke(executor));
        assertTrue(failed.getCause().getMessage().contains("unavailable"));
        clearInvocations(manager);
        when(registry.get("test")).thenReturn(null);
        var missing = assertThrows(InvocationTargetException.class, () -> execute.invoke(executor));
        assertTrue(missing.getCause().getMessage().contains("Unknown JOIN statistics"));
        verifyNoInteractions(manager);
    }
}
