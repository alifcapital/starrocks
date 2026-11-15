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
package com.starrocks.authorization.ranger;

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ColumnAccessKind;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.ranger.starrocks.RangerStarRocksAccessController;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.type.IntegerType;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reuse of Ranger answers inside one statement, on the real Ranger policy engine.
 * <p>
 * We count the evaluations that reach the plugin, so a test fails when an answer that must be reused
 * is evaluated again, or when an answer that must not be reused is taken from the cache.
 */
public class RangerDecisionCacheTest {
    private static final String SERVICE_DEF_PATH = "../../conf/ranger/ranger-servicedef-starrocks.json";
    private static final String CATALOG = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
    private static final TableName T1 = new TableName(CATALOG, "db1", "t1");
    private static final TableName T2 = new TableName(CATALOG, "db1", "t2");

    private static final AtomicInteger ACCESS_EVALUATIONS = new AtomicInteger();
    private static final AtomicInteger MASK_EVALUATIONS = new AtomicInteger();
    private static final AtomicInteger ROW_FILTER_EVALUATIONS = new AtomicInteger();

    private RangerServiceDef serviceDef;
    private long savedTtlMs;
    private long nextPolicyId = 1;

    @BeforeEach
    public void setUp() throws Exception {
        savedTtlMs = Config.ranger_decision_cache_ttl_ms;
        Config.ranger_decision_cache_ttl_ms = 5000;
        ACCESS_EVALUATIONS.set(0);
        MASK_EVALUATIONS.set(0);
        ROW_FILTER_EVALUATIONS.set(0);

        new MockUp<RangerBasePlugin>() {
            // init() starts the download of policies from Ranger Admin; the policies here are set directly.
            @Mock
            void init() {
            }

            @Mock
            RangerAccessResult isAccessAllowed(Invocation invocation, RangerAccessRequest request) {
                ACCESS_EVALUATIONS.incrementAndGet();
                return invocation.proceed();
            }

            @Mock
            RangerAccessResult evalDataMaskPolicies(Invocation invocation, RangerAccessRequest request,
                                                    RangerAccessResultProcessor resultProcessor) {
                MASK_EVALUATIONS.incrementAndGet();
                return invocation.proceed();
            }

            @Mock
            RangerAccessResult evalRowFilterPolicies(Invocation invocation, RangerAccessRequest request,
                                                     RangerAccessResultProcessor resultProcessor) {
                ROW_FILTER_EVALUATIONS.incrementAndGet();
                return invocation.proceed();
            }
        };

        Path path = Paths.get(System.getProperty("user.dir"), SERVICE_DEF_PATH);
        serviceDef = JsonUtils.jsonToObject(Files.readString(path, StandardCharsets.UTF_8), RangerServiceDef.class);
        Assertions.assertNotNull(serviceDef, "cannot parse " + path);
    }

    @AfterEach
    public void tearDown() {
        Config.ranger_decision_cache_ttl_ms = savedTtlMs;
    }

    // alice may select t1 and all its columns, the group analysts may select t2. alice sees column c1
    // of t1 masked and only the rows of t1 with c1 = 1.
    private List<RangerPolicy> defaultPolicies() {
        return List.of(
                accessPolicy(Map.of("catalog", CATALOG, "database", "db1", "table", "t1"), "alice", null),
                accessPolicy(Map.of("catalog", CATALOG, "database", "db1", "table", "t1", "column", "*"),
                        "alice", null),
                accessPolicy(Map.of("catalog", CATALOG, "database", "db1", "table", "t2"), null, "analysts"),
                maskPolicy("alice", "c1", "concat({col}, 'x')"),
                rowFilterPolicy("alice", "t1", "c1 = 1"));
    }

    // Only bob may select t1. Ranger keeps its old policies when it gets an empty set, so a change
    // that takes access away from alice needs a policy for somebody else.
    private List<RangerPolicy> bobOnlyPolicies() {
        return List.of(accessPolicy(Map.of("catalog", CATALOG, "database", "db1", "table", "t1"), "bob", null));
    }

    private RangerStarRocksAccessController controllerWith(List<RangerPolicy> policies) {
        RangerStarRocksAccessController controller = new RangerStarRocksAccessController();
        controller.getRangerPlugin().setPolicies(servicePolicies(policies, 1L));
        return controller;
    }

    private ServicePolicies servicePolicies(List<RangerPolicy> policies, long version) {
        ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setServiceName("starrocks");
        servicePolicies.setServiceId(1L);
        servicePolicies.setPolicyVersion(version);
        servicePolicies.setServiceDef(serviceDef);
        servicePolicies.setPolicies(new ArrayList<>(policies));
        return servicePolicies;
    }

    private RangerPolicy newPolicy(Map<String, String> resources) {
        RangerPolicy policy = new RangerPolicy();
        policy.setId(nextPolicyId);
        policy.setName("policy_" + nextPolicyId);
        nextPolicyId++;
        policy.setService("starrocks");
        policy.setIsEnabled(true);
        Map<String, RangerPolicy.RangerPolicyResource> policyResources = new HashMap<>();
        resources.forEach((name, value) -> policyResources.put(name, new RangerPolicy.RangerPolicyResource(value)));
        policy.setResources(policyResources);
        return policy;
    }

    private static List<String> listOf(String value) {
        return value == null ? null : List.of(value);
    }

    private static List<RangerPolicy.RangerPolicyItemAccess> select() {
        return List.of(new RangerPolicy.RangerPolicyItemAccess("select", true));
    }

    private RangerPolicy accessPolicy(Map<String, String> resources, String user, String group) {
        RangerPolicy policy = newPolicy(resources);
        policy.setPolicyItems(List.of(
                new RangerPolicy.RangerPolicyItem(select(), listOf(user), listOf(group), null, null, false)));
        return policy;
    }

    private RangerPolicy maskPolicy(String user, String column, String maskedValue) {
        RangerPolicy policy =
                newPolicy(Map.of("catalog", CATALOG, "database", "db1", "table", "t1", "column", column));
        policy.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK);
        policy.setDataMaskPolicyItems(List.of(new RangerPolicy.RangerDataMaskPolicyItem(select(),
                new RangerPolicy.RangerPolicyItemDataMaskInfo(RangerPolicy.MASK_TYPE_CUSTOM, null, maskedValue),
                List.of(user), null, null, null, false)));
        return policy;
    }

    private RangerPolicy rowFilterPolicy(String user, String table, String filter) {
        RangerPolicy policy = newPolicy(Map.of("catalog", CATALOG, "database", "db1", "table", table));
        policy.setPolicyType(RangerPolicy.POLICY_TYPE_ROWFILTER);
        policy.setRowFilterPolicyItems(List.of(new RangerPolicy.RangerRowFilterPolicyItem(
                new RangerPolicy.RangerPolicyItemRowFilterInfo(filter), select(), List.of(user), null, null, null,
                false)));
        return policy;
    }

    private static ConnectContext contextOf(String user, Set<String> groups) {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(new UserIdentity(user, "%"));
        context.setGroups(groups);
        context.setQueryId(UUIDUtil.genUUID());
        return context;
    }

    private static List<Column> columns() {
        return List.of(new Column("c1", IntegerType.INT), new Column("c2", IntegerType.INT));
    }

    @Test
    public void testRepeatedCheckIsEvaluatedOnce() {
        RangerStarRocksAccessController controller = controllerWith(defaultPolicies());
        ConnectContext alice = contextOf("alice", Set.of());
        for (int i = 0; i < 3; i++) {
            Assertions.assertDoesNotThrow(() -> controller.checkTableAction(alice, T1, PrivilegeType.SELECT));
        }
        Assertions.assertEquals(1, ACCESS_EVALUATIONS.get());

        // A denial is kept too.
        for (int i = 0; i < 3; i++) {
            Assertions.assertThrows(AccessDeniedException.class,
                    () -> controller.checkTableAction(alice, T2, PrivilegeType.SELECT));
        }
        Assertions.assertEquals(2, ACCESS_EVALUATIONS.get());
    }

    @Test
    public void testEveryRequestInputIsPartOfTheKey() {
        RangerStarRocksAccessController controller = controllerWith(defaultPolicies());
        ConnectContext context = contextOf("alice", Set.of());

        Assertions.assertDoesNotThrow(() -> controller.checkTableAction(context, T1, PrivilegeType.SELECT));
        // Another access type.
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkTableAction(context, T1, PrivilegeType.INSERT));
        Assertions.assertEquals(2, ACCESS_EVALUATIONS.get());

        // Another user on the same context.
        context.setCurrentUserIdentity(new UserIdentity("bob", "%"));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkTableAction(context, T1, PrivilegeType.SELECT));
        Assertions.assertEquals(3, ACCESS_EVALUATIONS.get());

        // Another host of the same user. A policy can have an ip-range condition.
        context.setCurrentUserIdentity(new UserIdentity("alice", "10.0.0.1"));
        Assertions.assertDoesNotThrow(() -> controller.checkTableAction(context, T1, PrivilegeType.SELECT));
        Assertions.assertEquals(4, ACCESS_EVALUATIONS.get());

        // Other groups.
        context.setCurrentUserIdentity(new UserIdentity("alice", "%"));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkTableAction(context, T2, PrivilegeType.SELECT));
        context.setGroups(Set.of("analysts"));
        Assertions.assertDoesNotThrow(() -> controller.checkTableAction(context, T2, PrivilegeType.SELECT));
        Assertions.assertEquals(6, ACCESS_EVALUATIONS.get());

        // Another usage and another column.
        Assertions.assertDoesNotThrow(() -> controller.checkColumnAction(context, T1, "c1", PrivilegeType.SELECT,
                EnumSet.of(ColumnAccessKind.PROJECTION)));
        Assertions.assertDoesNotThrow(() -> controller.checkColumnAction(context, T1, "c1", PrivilegeType.SELECT,
                EnumSet.of(ColumnAccessKind.FILTER)));
        Assertions.assertDoesNotThrow(() -> controller.checkColumnAction(context, T1, "c2", PrivilegeType.SELECT,
                EnumSet.of(ColumnAccessKind.PROJECTION)));
        Assertions.assertEquals(9, ACCESS_EVALUATIONS.get());

        // The same questions again are answered from the cache.
        Assertions.assertDoesNotThrow(() -> controller.checkTableAction(context, T2, PrivilegeType.SELECT));
        Assertions.assertDoesNotThrow(() -> controller.checkColumnAction(context, T1, "c1", PrivilegeType.SELECT,
                EnumSet.of(ColumnAccessKind.FILTER)));
        context.setGroups(Set.of());
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkTableAction(context, T2, PrivilegeType.SELECT));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkTableAction(context, T1, PrivilegeType.INSERT));
        Assertions.assertEquals(9, ACCESS_EVALUATIONS.get());
    }

    @Test
    public void testControllersDoNotShareAnswers() {
        RangerStarRocksAccessController aliceMaySelect = controllerWith(defaultPolicies());
        RangerStarRocksAccessController onlyBobMaySelect = controllerWith(bobOnlyPolicies());
        ConnectContext alice = contextOf("alice", Set.of());

        Assertions.assertDoesNotThrow(() -> aliceMaySelect.checkTableAction(alice, T1, PrivilegeType.SELECT));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> onlyBobMaySelect.checkTableAction(alice, T1, PrivilegeType.SELECT));
        Assertions.assertEquals(2, ACCESS_EVALUATIONS.get());
    }

    @Test
    public void testMaskAndRowFilterAreKeptAsTextAndParsedOnEveryCall() {
        RangerStarRocksAccessController controller = controllerWith(defaultPolicies());
        ConnectContext alice = contextOf("alice", Set.of());

        Map<String, Expr> first = controller.getColumnMaskingPolicy(alice, T1, columns());
        Map<String, Expr> second = controller.getColumnMaskingPolicy(alice, T1, columns());
        // c1 has a mask and c2 has none. Both answers are kept.
        Assertions.assertEquals(Set.of("c1"), first.keySet());
        Assertions.assertEquals(Set.of("c1"), second.keySet());
        Assertions.assertEquals(2, MASK_EVALUATIONS.get());
        Assertions.assertInstanceOf(FunctionCallExpr.class, first.get("c1"));
        Assertions.assertInstanceOf(FunctionCallExpr.class, second.get("c1"));
        Assertions.assertNotSame(first.get("c1"), second.get("c1"));

        Expr firstFilter = controller.getRowAccessPolicy(alice, T1);
        Expr secondFilter = controller.getRowAccessPolicy(alice, T1);
        Assertions.assertInstanceOf(BinaryPredicate.class, firstFilter);
        Assertions.assertInstanceOf(BinaryPredicate.class, secondFilter);
        Assertions.assertNotSame(firstFilter, secondFilter);
        // t2 has no row filter. The empty answer is kept too.
        Assertions.assertNull(controller.getRowAccessPolicy(alice, T2));
        Assertions.assertNull(controller.getRowAccessPolicy(alice, T2));
        Assertions.assertEquals(2, ROW_FILTER_EVALUATIONS.get());

        // Another user gets no mask and no row filter.
        alice.setCurrentUserIdentity(new UserIdentity("bob", "%"));
        Assertions.assertTrue(controller.getColumnMaskingPolicy(alice, T1, columns()).isEmpty());
        Assertions.assertNull(controller.getRowAccessPolicy(alice, T1));
        Assertions.assertEquals(4, MASK_EVALUATIONS.get());
        Assertions.assertEquals(3, ROW_FILTER_EVALUATIONS.get());
    }

    @Test
    public void testNewQueryIdStartsWithNoAnswers() {
        RangerStarRocksAccessController controller = controllerWith(defaultPolicies());
        ConnectContext alice = contextOf("alice", Set.of());

        Assertions.assertDoesNotThrow(() -> controller.checkTableAction(alice, T1, PrivilegeType.SELECT));
        controller.getColumnMaskingPolicy(alice, T1, columns());
        controller.getRowAccessPolicy(alice, T1);
        Assertions.assertDoesNotThrow(() -> controller.checkTableAction(alice, T1, PrivilegeType.SELECT));
        controller.getColumnMaskingPolicy(alice, T1, columns());
        controller.getRowAccessPolicy(alice, T1);
        Assertions.assertEquals(1, ACCESS_EVALUATIONS.get());
        Assertions.assertEquals(2, MASK_EVALUATIONS.get());
        Assertions.assertEquals(1, ROW_FILTER_EVALUATIONS.get());

        alice.setQueryId(UUIDUtil.genUUID());
        Assertions.assertDoesNotThrow(() -> controller.checkTableAction(alice, T1, PrivilegeType.SELECT));
        controller.getColumnMaskingPolicy(alice, T1, columns());
        controller.getRowAccessPolicy(alice, T1);
        Assertions.assertEquals(2, ACCESS_EVALUATIONS.get());
        Assertions.assertEquals(4, MASK_EVALUATIONS.get());
        Assertions.assertEquals(2, ROW_FILTER_EVALUATIONS.get());
    }

    @Test
    public void testPolicyChangeIsSeenInTheNextStatement() {
        RangerStarRocksAccessController controller = controllerWith(defaultPolicies());
        ConnectContext alice = contextOf("alice", Set.of());
        Assertions.assertDoesNotThrow(() -> controller.checkTableAction(alice, T1, PrivilegeType.SELECT));

        controller.getRangerPlugin().setPolicies(servicePolicies(bobOnlyPolicies(), 2L));
        // The statement keeps the answer it already has.
        Assertions.assertDoesNotThrow(() -> controller.checkTableAction(alice, T1, PrivilegeType.SELECT));
        Assertions.assertEquals(1, ACCESS_EVALUATIONS.get());

        alice.setQueryId(UUIDUtil.genUUID());
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkTableAction(alice, T1, PrivilegeType.SELECT));
        Assertions.assertEquals(2, ACCESS_EVALUATIONS.get());
    }

    @Test
    public void testPolicyChangeIsSeenAfterTtl() throws Exception {
        Config.ranger_decision_cache_ttl_ms = 1;
        RangerStarRocksAccessController controller = controllerWith(defaultPolicies());
        ConnectContext alice = contextOf("alice", Set.of());
        Assertions.assertDoesNotThrow(() -> controller.checkTableAction(alice, T1, PrivilegeType.SELECT));

        controller.getRangerPlugin().setPolicies(servicePolicies(bobOnlyPolicies(), 2L));
        Thread.sleep(20);
        // Same query id, but the answer is older than the TTL.
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkTableAction(alice, T1, PrivilegeType.SELECT));
        Assertions.assertEquals(2, ACCESS_EVALUATIONS.get());
    }

    @Test
    public void testZeroTtlTurnsTheCacheOff() {
        Config.ranger_decision_cache_ttl_ms = 0;
        RangerStarRocksAccessController controller = controllerWith(defaultPolicies());
        ConnectContext alice = contextOf("alice", Set.of());
        for (int i = 0; i < 3; i++) {
            Assertions.assertDoesNotThrow(() -> controller.checkTableAction(alice, T1, PrivilegeType.SELECT));
            Assertions.assertEquals(1, controller.getColumnMaskingPolicy(alice, T1, columns()).size());
            Assertions.assertNotNull(controller.getRowAccessPolicy(alice, T1));
        }
        Assertions.assertEquals(3, ACCESS_EVALUATIONS.get());
        Assertions.assertEquals(6, MASK_EVALUATIONS.get());
        Assertions.assertEquals(3, ROW_FILTER_EVALUATIONS.get());
        Assertions.assertEquals(0, alice.getRangerDecisionCache().size());
    }

    @Test
    public void testFullCacheStillAnswers() {
        RangerStarRocksAccessController controller = controllerWith(defaultPolicies());
        ConnectContext alice = contextOf("alice", Set.of());
        for (int i = 0; i < RangerDecisionCache.MAX_ENTRIES; i++) {
            TableName table = new TableName(CATALOG, "db1", "other_" + i);
            Assertions.assertThrows(AccessDeniedException.class,
                    () -> controller.checkTableAction(alice, table, PrivilegeType.SELECT));
        }
        Assertions.assertEquals(RangerDecisionCache.MAX_ENTRIES, alice.getRangerDecisionCache().size());
        ACCESS_EVALUATIONS.set(0);

        // The cache is full: a new question is asked every time and not kept.
        Assertions.assertDoesNotThrow(() -> controller.checkTableAction(alice, T1, PrivilegeType.SELECT));
        Assertions.assertDoesNotThrow(() -> controller.checkTableAction(alice, T1, PrivilegeType.SELECT));
        Assertions.assertEquals(2, ACCESS_EVALUATIONS.get());
        Assertions.assertEquals(RangerDecisionCache.MAX_ENTRIES, alice.getRangerDecisionCache().size());
        // A kept answer is still used.
        TableName kept = new TableName(CATALOG, "db1", "other_0");
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkTableAction(alice, kept, PrivilegeType.SELECT));
        Assertions.assertEquals(2, ACCESS_EVALUATIONS.get());
    }

    @Test
    public void testRootBypassesRanger() {
        RangerStarRocksAccessController controller = controllerWith(bobOnlyPolicies());
        ConnectContext root = new ConnectContext();
        root.setCurrentUserIdentity(UserIdentity.ROOT);
        Assertions.assertDoesNotThrow(() -> controller.checkTableAction(root, T1, PrivilegeType.SELECT));
        Assertions.assertDoesNotThrow(() -> controller.checkSystemAction(root, PrivilegeType.OPERATE));
        Assertions.assertEquals(0, ACCESS_EVALUATIONS.get());
        Assertions.assertEquals(0, root.getRangerDecisionCache().size());
    }
}
