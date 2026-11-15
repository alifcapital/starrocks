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

package com.starrocks.authentication;

import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.catalog.system.information.TaskRunsSystemTable;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.SchemaScanNode;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.InformationSchemaDataSource;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TTableInfo;
import com.starrocks.thrift.TTaskRunInfo;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.thrift.TUserRoles;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Rebuilding the caller of an FE RPC from the user identity a BE sends, on a thread that, like the RPC threads,
 * has no session.
 */
public class UserIdentityUtilsTest {
    private static final String DB = "user_ident_utils_db";
    private static final String GROUP = "uiu_group";

    private static long openRoleId;
    private static long secretRoleId;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        ctx.getGlobalStateMgr().getAuthorizationMgr().initBuiltinRolesAndUsers();
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(DB).useDatabase(DB)
                .withTable("CREATE TABLE t_open (k INT) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')")
                .withTable("CREATE TABLE t_secret (k INT) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.ddl("CREATE ROLE uiu_open_role");
        starRocksAssert.ddl("CREATE ROLE uiu_secret_role");
        starRocksAssert.ddl("GRANT SELECT ON TABLE " + DB + ".t_open TO ROLE uiu_open_role");
        starRocksAssert.ddl("GRANT SELECT ON TABLE " + DB + ".t_secret TO ROLE uiu_secret_role");
        starRocksAssert.ddl("CREATE USER uiu_native");
        starRocksAssert.ddl("GRANT uiu_open_role TO USER uiu_native");
        starRocksAssert.ddl("GRANT uiu_secret_role TO EXTERNAL GROUP " + GROUP);
        openRoleId = GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdByNameAllowNull("uiu_open_role");
        secretRoleId =
                GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdByNameAllowNull("uiu_secret_role");
    }

    private static TUserIdentity userIdent(UserIdentity user, List<Long> roleIds) {
        TUserIdentity userIdent = UserIdentityUtils.toThrift(user);
        TUserRoles roles = new TUserRoles();
        roles.setRole_id_list(roleIds);
        userIdent.setCurrent_role_ids(roles);
        return userIdent;
    }

    private static <T> T withoutSession(Callable<T> task) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            return executor.submit(() -> {
                assertNull(ConnectContext.get());
                return task.call();
            }).get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    private static ConnectContext callerOf(TUserIdentity userIdent) {
        ConnectContext context = new ConnectContext();
        UserIdentityUtils.setAuthInfoFromThrift(context, userIdent);
        return context;
    }

    private static ConnectContext session(UserIdentity user, Set<Long> roleIds, Set<String> groups) {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(user);
        context.setCurrentRoleIds(new HashSet<>(roleIds));
        context.setGroups(new HashSet<>(groups));
        return context;
    }

    // Runs the task on a thread whose session is `session`, as a query of that session plans and evaluates.
    private static <T> T inSession(ConnectContext session, Callable<T> task) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            return executor.submit(() -> {
                session.setThreadLocalInfo();
                try {
                    return task.call();
                } finally {
                    ConnectContext.remove();
                }
            }).get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    // Lets a user see a database only through the group, as a Ranger policy granted to a group does.
    private static final class GroupOnlyController implements AccessController {
        @Override
        public void checkAnyActionOnDb(ConnectContext context, String catalogName, String db)
                throws AccessDeniedException {
            if (!context.getGroups().contains(GROUP)) {
                throw new AccessDeniedException();
            }
        }
    }

    private static <T> T withGroupOnlyController(Callable<T> task) throws Exception {
        AccessController original =
                Authorizer.getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        Authorizer.getInstance().setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                new GroupOnlyController());
        try {
            return task.call();
        } finally {
            Authorizer.getInstance().setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, original);
        }
    }

    private static void mockOneTaskRun() {
        TaskRunStatus status = new TaskRunStatus();
        status.setQueryId("uiu_query");
        status.setTaskName("uiu_task");
        status.setDbName(DB);
        new MockUp<TaskManager>() {
            @Mock
            public List<TaskRunStatus> getMatchedTaskRunStatus(TGetTasksParams params) {
                return List.of(status);
            }
        };
    }

    @Test
    public void testEphemeralUserGetsActivatedRoles() throws Exception {
        // An ephemeral user owns no roles; its privileges are the roles its session activated.
        TUserIdentity userIdent = userIdent(new UserIdentity("uiu_ephemeral", "%", false, true), List.of(openRoleId));
        ConnectContext caller = withoutSession(() -> callerOf(userIdent));

        assertEquals(Set.of(openRoleId), caller.getCurrentRoleIds());
        assertDoesNotThrow(() -> Authorizer.checkTableAction(caller, DB, "t_open", PrivilegeType.SELECT));
        assertThrows(AccessDeniedException.class,
                () -> Authorizer.checkTableAction(caller, DB, "t_secret", PrivilegeType.SELECT));
    }

    @Test
    public void testNativeUserKeepsOnlyOwnedRoles() throws Exception {
        // A native user cannot claim a role it was not granted.
        TUserIdentity userIdent = userIdent(new UserIdentity("uiu_native", "%"), List.of(openRoleId, secretRoleId));
        ConnectContext caller = withoutSession(() -> callerOf(userIdent));

        assertEquals(Set.of(openRoleId), caller.getCurrentRoleIds());
        assertThrows(AccessDeniedException.class,
                () -> Authorizer.checkTableAction(caller, DB, "t_secret", PrivilegeType.SELECT));
    }

    @Test
    public void testEphemeralUserListsTables() throws Exception {
        // information_schema.tables: an ephemeral user sees the tables its activated roles may read.
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setCurrent_user_ident(
                userIdent(new UserIdentity("uiu_ephemeral", "%", false, true), List.of(openRoleId)));
        authInfo.setPattern(DB);
        TGetTablesInfoRequest request = new TGetTablesInfoRequest();
        request.setAuth_info(authInfo);

        List<String> tables = withoutSession(() -> InformationSchemaDataSource.generateTablesInfoResponse(request)
                .getTables_infos().stream().map(TTableInfo::getTable_name).collect(Collectors.toList()));
        assertEquals(List.of("t_open"), tables);
    }

    @Test
    public void testSessionIdentityRoundTrip() throws Exception {
        ConnectContext session = session(new UserIdentity("uiu_native", "%"), Set.of(openRoleId), Set.of(GROUP));
        TUserIdentity userIdent = UserIdentityUtils.toThrift(session);

        assertEquals("uiu_native", userIdent.getUsername());
        assertEquals(List.of(openRoleId), userIdent.getCurrent_role_ids().getRole_id_list());
        assertEquals(List.of(GROUP), userIdent.getGroups());

        ConnectContext caller = withoutSession(() -> callerOf(userIdent));
        assertEquals(new UserIdentity("uiu_native", "%"), caller.getCurrentUserIdentity());
        assertEquals(Set.of(openRoleId), caller.getCurrentRoleIds());
        assertEquals(Set.of(GROUP), caller.getGroups());
    }

    @Test
    public void testUserIdentityAloneCarriesNoGroups() {
        // An identity that only names a user says nothing about groups.
        assertFalse(UserIdentityUtils.toThrift(new UserIdentity("uiu_native", "%")).isSetGroups());
    }

    @Test
    public void testIdentityWithoutGroupsKeepsGroupsOfContext() throws Exception {
        TUserIdentity userIdent = userIdent(new UserIdentity("uiu_native", "%"), List.of(openRoleId));

        assertTrue(withoutSession(() -> callerOf(userIdent)).getGroups().isEmpty());

        ConnectContext context = withoutSession(() -> {
            ConnectContext c = new ConnectContext();
            c.setGroups(new HashSet<>(Set.of("uiu_other")));
            UserIdentityUtils.setAuthInfoFromThrift(c, userIdent);
            return c;
        });
        assertEquals(Set.of("uiu_other"), context.getGroups());
    }

    @Test
    public void testAuthInfoCarriesGroups() throws Exception {
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setCurrent_user_ident(UserIdentityUtils.toThrift(
                session(new UserIdentity("uiu_native", "%"), Set.of(openRoleId), Set.of(GROUP))));

        ConnectContext caller = withoutSession(() -> {
            ConnectContext c = new ConnectContext();
            UserIdentityUtils.setAuthInfoFromThrift(c, authInfo);
            return c;
        });
        assertEquals(Set.of(GROUP), caller.getGroups());
    }

    @Test
    public void testEphemeralUserGetsGroups() throws Exception {
        TUserIdentity userIdent = UserIdentityUtils.toThrift(
                session(new UserIdentity("uiu_ephemeral", "%", false, true), Set.of(openRoleId), Set.of(GROUP)));

        ConnectContext caller = withoutSession(() -> callerOf(userIdent));
        assertEquals(Set.of(openRoleId), caller.getCurrentRoleIds());
        assertEquals(Set.of(GROUP), caller.getGroups());
    }

    @Test
    public void testRoleOfGroupAppliesOverRpc() throws Exception {
        // uiu_secret_role is granted to the group, not to the user: a caller sees t_secret only with the groups.
        UserIdentity user = new UserIdentity("uiu_native", "%");
        TUserIdentity withGroups = UserIdentityUtils.toThrift(session(user, Set.of(openRoleId), Set.of(GROUP)));
        TUserIdentity withoutGroups = userIdent(user, List.of(openRoleId));

        ConnectContext caller = withoutSession(() -> callerOf(withGroups));
        assertDoesNotThrow(() -> Authorizer.checkTableAction(caller, DB, "t_secret", PrivilegeType.SELECT));
        ConnectContext callerWithoutGroups = withoutSession(() -> callerOf(withoutGroups));
        assertThrows(AccessDeniedException.class,
                () -> Authorizer.checkTableAction(callerWithoutGroups, DB, "t_secret", PrivilegeType.SELECT));

        // information_schema.tables lists the table of the group.
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setCurrent_user_ident(withGroups);
        authInfo.setPattern(DB);
        TGetTablesInfoRequest request = new TGetTablesInfoRequest();
        request.setAuth_info(authInfo);
        List<String> tables = withoutSession(() -> InformationSchemaDataSource.generateTablesInfoResponse(request)
                .getTables_infos().stream().map(TTableInfo::getTable_name).sorted().collect(Collectors.toList()));
        assertEquals(List.of("t_open", "t_secret"), tables);
    }

    @Test
    public void testTaskRunVisibleThroughGroup() throws Exception {
        // information_schema.task_runs read through a BE: the FE RPC sees the task run only with the groups.
        mockOneTaskRun();
        UserIdentity user = new UserIdentity("uiu_native", "%");

        TGetTasksParams withGroups = new TGetTasksParams();
        withGroups.setCurrent_user_ident(UserIdentityUtils.toThrift(session(user, Set.of(openRoleId), Set.of(GROUP))));
        TGetTasksParams withoutGroups = new TGetTasksParams();
        withoutGroups.setCurrent_user_ident(userIdent(user, List.of(openRoleId)));

        List<TTaskRunInfo> visible = withGroupOnlyController(
                () -> withoutSession(() -> TaskRunsSystemTable.query(withGroups).getTask_runs()));
        assertEquals(1, visible.size());
        assertEquals("uiu_task", visible.get(0).getTask_name());

        List<TTaskRunInfo> hidden = withGroupOnlyController(
                () -> withoutSession(() -> TaskRunsSystemTable.query(withoutGroups).getTask_runs()));
        assertTrue(hidden.isEmpty());
    }

    @Test
    public void testTaskRunVisibleThroughGroupInFeEvaluation() throws Exception {
        // information_schema.task_runs evaluated in the FE takes the identity from the session of the query.
        mockOneTaskRun();
        ConnectContext session = session(new UserIdentity("uiu_native", "%"), Set.of(openRoleId), Set.of(GROUP));

        int rows = withGroupOnlyController(() -> inSession(session, () -> TaskRunsSystemTable.evaluate(List.of()).size()));
        assertEquals(1, rows);
    }

    @Test
    public void testSchemaScanNodeCarriesGroups() throws Exception {
        ConnectContext session = session(new UserIdentity("uiu_native", "%"), Set.of(openRoleId), Set.of(GROUP));
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(new SystemTable(0, "task_runs", null, null, null));
        SchemaScanNode scanNode = new SchemaScanNode(new PlanNodeId(0), desc, WarehouseManager.DEFAULT_RESOURCE);

        TPlanNode planNode = inSession(session, () -> scanNode.treeToThrift().getNodes().get(0));
        TUserIdentity userIdent = planNode.getSchema_scan_node().getCurrent_user_ident();
        assertEquals("uiu_native", userIdent.getUsername());
        assertEquals(List.of(openRoleId), userIdent.getCurrent_role_ids().getRole_id_list());
        assertEquals(List.of(GROUP), userIdent.getGroups());
    }
}
