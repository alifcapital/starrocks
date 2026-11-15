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

import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ranger.starrocks.RangerStarRocksAccessController;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * "Any action on or in a database" under Ranger, on the real Ranger policy engine and real database objects.
 * <p>
 * The controller answers with one ANY request on the database when it can. We want it to give the same answer
 * as asking about the database and then about each object in it, for every policy shape an administrator can
 * write, so every shape here compares the two answers.
 */
public class RangerAnyActionOnOrInDbTest {
    private static final String SERVICE_DEF_PATH = "../../conf/ranger/ranger-servicedef-starrocks.json";
    private static final String CATALOG = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
    // Holds tables t1 and t2, view v1, materialized view mv1 and function f1(INT).
    private static final String DB = "or_in_db";
    private static final String ONLY_T1_DB = "or_in_db_only_t1";
    private static final String EMPTY_DB = "or_in_db_empty";
    private static final String MISSING_DB = "or_in_db_missing";

    private static final AtomicInteger ACCESS_EVALUATIONS = new AtomicInteger();

    private RangerServiceDef serviceDef;
    private String[] allAccessTypes;
    private long nextPolicyId = 1;
    // The tag policies every service in a test gets. The engine changes the policies it loads, so
    // each service gets new ones.
    private Supplier<ServicePolicies.TagPolicies> tagPolicies;

    @BeforeAll
    public static void beforeClass() throws Exception {
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
        };

        UtFrameUtils.createMinStarRocksCluster();
        StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        String table = "(`v4` bigint NULL, `v5` bigint NULL) ENGINE=OLAP DUPLICATE KEY(`v4`) "
                + "DISTRIBUTED BY HASH(`v4`) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\");";
        starRocksAssert.withDatabase(DB).useDatabase(DB)
                .withTable("CREATE TABLE `t1` " + table)
                .withTable("CREATE TABLE `t2` " + table);
        starRocksAssert.withView("create view " + DB + ".v1 as select v4 from " + DB + ".t1");
        starRocksAssert.withMaterializedView("create materialized view " + DB + ".mv1 distributed by hash(v4) "
                + "refresh async START('9999-12-31') EVERY(INTERVAL 1 HOUR) "
                + "PROPERTIES (\"replication_num\" = \"1\") as select v4, v5 from " + DB + ".t1;");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        Function function = new ScalarFunction(new FunctionName(DB, "f1"), new Type[] {IntegerType.INT},
                IntegerType.INT, false);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        db.addFunction(function);

        starRocksAssert.withDatabase(ONLY_T1_DB).useDatabase(ONLY_T1_DB).withTable("CREATE TABLE `t1` " + table);
        starRocksAssert.withDatabase(EMPTY_DB);
    }

    @BeforeEach
    public void setUp() throws Exception {
        Path path = Paths.get(System.getProperty("user.dir"), SERVICE_DEF_PATH);
        serviceDef = JsonUtils.jsonToObject(Files.readString(path, StandardCharsets.UTF_8), RangerServiceDef.class);
        Assertions.assertNotNull(serviceDef, "cannot parse " + path);
        allAccessTypes = serviceDef.getAccessTypes().stream()
                .map(RangerServiceDef.RangerAccessTypeDef::getName).toArray(String[]::new);
        ACCESS_EVALUATIONS.set(0);
        tagPolicies = null;
    }

    /** A policy as an administrator writes it in Ranger Admin. */
    private class Policy {
        private final Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
        private final List<RangerPolicy.RangerPolicyItem> allowItems = new ArrayList<>();
        private final List<RangerPolicy.RangerPolicyItem> denyItems = new ArrayList<>();
        private final List<RangerPolicy.RangerPolicyItem> allowExceptions = new ArrayList<>();
        private int priority = RangerPolicy.POLICY_PRIORITY_NORMAL;

        Policy on(String resource, String value) {
            resources.put(resource, new RangerPolicy.RangerPolicyResource(value));
            return this;
        }

        Policy except(String resource, String value) {
            resources.put(resource, new RangerPolicy.RangerPolicyResource(value, true, false));
            return this;
        }

        Policy allowUser(String user, String... accessTypes) {
            allowItems.add(item(List.of(user), null, accessTypes));
            return this;
        }

        Policy allowGroup(String group, String... accessTypes) {
            allowItems.add(item(null, List.of(group), accessTypes));
            return this;
        }

        Policy denyUser(String user, String... accessTypes) {
            denyItems.add(item(List.of(user), null, accessTypes));
            return this;
        }

        Policy allowExceptionForUser(String user, String... accessTypes) {
            allowExceptions.add(item(List.of(user), null, accessTypes));
            return this;
        }

        Policy priority(int priority) {
            this.priority = priority;
            return this;
        }

        private RangerPolicy.RangerPolicyItem item(List<String> users, List<String> groups, String... accessTypes) {
            List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();
            for (String accessType : accessTypes) {
                accesses.add(new RangerPolicy.RangerPolicyItemAccess(accessType, true));
            }
            return new RangerPolicy.RangerPolicyItem(accesses, users, groups, null, null, false);
        }

        RangerPolicy build() {
            RangerPolicy policy = new RangerPolicy();
            policy.setId(nextPolicyId);
            policy.setName("policy_" + nextPolicyId);
            nextPolicyId++;
            policy.setService("starrocks");
            policy.setIsEnabled(true);
            policy.setResources(resources);
            policy.setPolicyItems(allowItems);
            policy.setDenyPolicyItems(denyItems);
            policy.setAllowExceptions(allowExceptions);
            policy.setPolicyPriority(priority);
            return policy;
        }
    }

    private Policy policy() {
        return new Policy();
    }

    private Policy onDb(String db) {
        return policy().on("catalog", CATALOG).on("database", db);
    }

    private Policy on(String objectType, String object) {
        return onDb(DB).on(objectType, object);
    }

    private ServicePolicies servicePolicies(List<Policy> policies) {
        List<RangerPolicy> rangerPolicies = new ArrayList<>();
        for (Policy policy : policies) {
            rangerPolicies.add(policy.build());
        }
        ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setServiceName("starrocks");
        servicePolicies.setServiceId(1L);
        servicePolicies.setPolicyVersion(1L);
        servicePolicies.setServiceDef(serviceDef);
        servicePolicies.setPolicies(rangerPolicies);
        if (tagPolicies != null) {
            servicePolicies.setTagPolicies(tagPolicies.get());
        }
        return servicePolicies;
    }

    private static RangerPolicy tagPolicy(long id, String tag) {
        RangerPolicy policy = new RangerPolicy();
        policy.setId(id);
        policy.setName("tag_policy_" + id);
        policy.setService("starrocks_tag");
        policy.setIsEnabled(true);
        Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
        resources.put("tag", new RangerPolicy.RangerPolicyResource(tag));
        policy.setResources(resources);
        return policy;
    }

    private static List<RangerPolicy.RangerPolicyItemAccess> starRocksSelect() {
        return new ArrayList<>(List.of(new RangerPolicy.RangerPolicyItemAccess("starrocks:select", true)));
    }

    // The default tag policy of Ranger: deny access to data whose tag EXPIRES_ON has passed.
    private static RangerPolicy expiresOnPolicy() {
        RangerPolicy policy = tagPolicy(1000, "EXPIRES_ON");
        policy.setDenyPolicyItems(new ArrayList<>(List.of(new RangerPolicy.RangerPolicyItem(starRocksSelect(), null,
                new ArrayList<>(List.of("public")), null,
                new ArrayList<>(List.of(new RangerPolicy.RangerPolicyItemCondition("accessed-after-expiry",
                        new ArrayList<>(List.of("yes"))))), false))));
        return policy;
    }

    private static RangerPolicy maskTagPolicy(long id, String tag) {
        RangerPolicy policy = tagPolicy(id, tag);
        policy.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK);
        policy.setDataMaskPolicyItems(new ArrayList<>(List.of(new RangerPolicy.RangerDataMaskPolicyItem(
                starRocksSelect(), new RangerPolicy.RangerPolicyItemDataMaskInfo("starrocks:MASK_NULL", null, null),
                null, new ArrayList<>(List.of("public")), null, null, false))));
        return policy;
    }

    private static RangerPolicy allowTagPolicy(long id, String tag, String user) {
        RangerPolicy policy = tagPolicy(id, tag);
        policy.setPolicyItems(new ArrayList<>(List.of(new RangerPolicy.RangerPolicyItem(starRocksSelect(),
                new ArrayList<>(List.of(user)), null, null, null, false))));
        return policy;
    }

    private static ServicePolicies.TagPolicies tagService(RangerPolicy... policies) {
        RangerServiceDef tagServiceDef;
        try {
            tagServiceDef = EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef("tag");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        // The tag enricher downloads tags from Ranger Admin. No object here has a tag, so we leave it out.
        tagServiceDef.setContextEnrichers(new ArrayList<>());
        ServicePolicies.TagPolicies tags = new ServicePolicies.TagPolicies();
        tags.setServiceName("starrocks_tag");
        tags.setServiceId(2L);
        tags.setPolicyVersion(1L);
        tags.setServiceDef(tagServiceDef);
        tags.setPolicies(new ArrayList<>(List.of(policies)));
        return tags;
    }

    private RangerStarRocksAccessController controllerWith(ServicePolicies servicePolicies) {
        RangerStarRocksAccessController controller = new RangerStarRocksAccessController();
        controller.getRangerPlugin().setPolicies(servicePolicies);
        return controller;
    }

    private static ConnectContext contextOf(String user, Set<String> groups) {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(new UserIdentity(user, "%"));
        context.setGroups(groups);
        return context;
    }

    private static boolean isAllowed(Authorizer.AccessControlChecker check) {
        try {
            check.check();
            return true;
        } catch (AccessDeniedException e) {
            return false;
        }
    }

    private static boolean objectByObject(RangerStarRocksAccessController controller, String user, Set<String> groups,
                                          String db) {
        return isAllowed(() -> AccessController.checkAnyActionOnOrInDbOneByOne(
                controller, contextOf(user, groups), CATALOG, db));
    }

    private static boolean inOneStep(RangerStarRocksAccessController controller, String user, Set<String> groups,
                                     String db) {
        return isAllowed(() -> controller.checkAnyActionOnOrInDb(contextOf(user, groups), CATALOG, db));
    }

    private void compare(List<String> failures, String shape, boolean expected, String db, String user,
                         Set<String> groups, Policy... policies) {
        RangerStarRocksAccessController controller = controllerWith(servicePolicies(List.of(policies)));
        boolean old = objectByObject(controller, user, groups, db);
        boolean now = inOneStep(controller, user, groups, db);
        if (old != expected || now != expected) {
            failures.add(shape + ": expected " + expected + ", object by object " + old + ", in one step " + now);
        }
    }

    private void compare(List<String> failures, String shape, boolean expected, String db, Policy... policies) {
        compare(failures, shape, expected, db, "u", Set.of(), policies);
    }

    @Test
    public void testSameAnswerForEveryPolicyShape() {
        assertSameAnswerForEveryPolicyShape();
    }

    // The tag service in production: the default EXPIRES_ON deny policy and masking policies.
    @Test
    public void testSameAnswerWithTagPoliciesThatCannotAllow() {
        tagPolicies = () -> tagService(expiresOnPolicy(), maskTagPolicy(1001, "PII"), maskTagPolicy(1002, "EMAIL"));
        assertSameAnswerForEveryPolicyShape();

        RangerStarRocksAccessController controller =
                controllerWith(servicePolicies(List.of(on("table", "t1").allowUser("alice", "select"))));
        ACCESS_EVALUATIONS.set(0);
        Assertions.assertFalse(inOneStep(controller, "nobody", Set.of(), DB));
        // One request on the database and one on all pipes.
        Assertions.assertEquals(2, ACCESS_EVALUATIONS.get());
    }

    @Test
    public void testTagPolicyThatAllowsRunsTheObjectChecks() {
        tagPolicies = () -> tagService(expiresOnPolicy(), allowTagPolicy(1003, "PUBLIC_DATA", "bob"));
        RangerStarRocksAccessController controller =
                controllerWith(servicePolicies(List.of(on("table", "t1").allowUser("alice", "select"))));

        Assertions.assertEquals(objectByObject(controller, "nobody", Set.of(), DB),
                inOneStep(controller, "nobody", Set.of(), DB));
        ACCESS_EVALUATIONS.set(0);
        Assertions.assertFalse(inOneStep(controller, "nobody", Set.of(), DB));
        Assertions.assertTrue(ACCESS_EVALUATIONS.get() > 2, "object checks: " + ACCESS_EVALUATIONS.get());
    }

    private void assertSameAnswerForEveryPolicyShape() {
        String[] all = allAccessTypes;
        List<String> failures = new ArrayList<>();

        compare(failures, "allow on the db itself", true, DB, onDb(DB).allowUser("u", "create table"));
        compare(failures, "allow on the db, asked about another db", false, MISSING_DB,
                onDb(DB).allowUser("u", "create table"));
        compare(failures, "allow only on table t1", true, DB, on("table", "t1").allowUser("u", "select"));
        compare(failures, "allow only on view v1", true, DB, on("view", "v1").allowUser("u", "select"));
        compare(failures, "allow only on mv mv1", true, DB, on("materialized_view", "mv1").allowUser("u", "select"));
        compare(failures, "allow only on function f1(INT)", true, DB, on("function", "f1(INT)").allowUser("u", "usage"));
        compare(failures, "allow only on column t1.c1", true, DB,
                on("table", "t1").on("column", "c1").allowUser("u", "select"));
        compare(failures, "allow only on pipe p1", true, DB, on("pipe", "p1").allowUser("u", "usage"));
        compare(failures, "allow only on pipe p1, asked about another db", false, MISSING_DB,
                on("pipe", "p1").allowUser("u", "usage"));
        compare(failures, "allow on all pipes, asked about another db", true, MISSING_DB,
                policy().on("catalog", "*").on("database", "*").on("pipe", "*").allowUser("u", "usage"));
        compare(failures, "allow t1 and deny t1 in two policies", true, DB,
                on("table", "t1").allowUser("u", "select"), on("table", "t1").denyUser("u", "select"));
        compare(failures, "allow t1 and deny t1 in one policy", false, DB,
                on("table", "t1").allowUser("u", "select").denyUser("u", "select"));
        compare(failures, "allow t1 select, deny t1 all", true, DB,
                on("table", "t1").allowUser("u", "select"), on("table", "t1").denyUser("u", all));
        compare(failures, "deny db select, allow t1 select", true, DB,
                onDb(DB).denyUser("u", "select"), on("table", "t1").allowUser("u", "select"));
        // The database request alone denies the next four shapes; a policy set that deny.
        compare(failures, "deny db all, allow t1 select", true, DB,
                onDb(DB).denyUser("u", all), on("table", "t1").allowUser("u", "select"));
        compare(failures, "deny db all, allow v1 select", true, DB,
                onDb(DB).denyUser("u", all), on("view", "v1").allowUser("u", "select"));
        compare(failures, "deny all tables all, allow v1 select", true, DB,
                on("table", "*").denyUser("u", all), on("view", "v1").allowUser("u", "select"));
        compare(failures, "deny db all with priority, allow t1 select", true, DB,
                onDb(DB).denyUser("u", all).priority(RangerPolicy.POLICY_PRIORITY_OVERRIDE),
                on("table", "t1").allowUser("u", "select"));
        compare(failures, "deny all tables select, allow t1 select", true, DB,
                on("table", "*").denyUser("u", "select"), on("table", "t1").allowUser("u", "select"));
        compare(failures, "deny all tables select, allow v1 select", true, DB,
                on("table", "*").denyUser("u", "select"), on("view", "v1").allowUser("u", "select"));
        compare(failures, "allow and deny create table on the db in one policy", false, DB,
                onDb(DB).allowUser("u", "create table").denyUser("u", "create table"));
        compare(failures, "any db, table t1", true, DB,
                policy().on("catalog", CATALOG).on("database", "*").on("table", "t1").allowUser("u", "select"));
        compare(failures, "any db, table t1, asked about a db without t1", true, EMPTY_DB,
                policy().on("catalog", CATALOG).on("database", "*").on("table", "t1").allowUser("u", "select"));
        compare(failures, "db or_in*, table t*", true, DB,
                policy().on("catalog", CATALOG).on("database", "or_in*").on("table", "t*").allowUser("u", "select"));
        compare(failures, "all tables except t1", true, DB, onDb(DB).except("table", "t1").allowUser("u", "select"));
        compare(failures, "all tables except t1, db has only t1", true, ONLY_T1_DB,
                onDb(ONLY_T1_DB).except("table", "t1").allowUser("u", "select"));
        compare(failures, "allow t1 to a group of the user", true, DB, "u", Set.of("g"),
                on("table", "t1").allowGroup("g", "select"));
        compare(failures, "allow t1 to a group the user is not in", false, DB, "u", Set.of("h"),
                on("table", "t1").allowGroup("g", "select"));
        compare(failures, "allow t1 to public", true, DB, on("table", "t1").allowGroup("public", "select"));
        compare(failures, "allow t1 to public with an exception for the user", false, DB,
                on("table", "t1").allowGroup("public", "select").allowExceptionForUser("u", "select"));
        compare(failures, "policy only for another user", false, DB, on("table", "t1").allowUser("other", "select"));
        compare(failures, "no policy at all", false, DB);
        compare(failures, "db does not exist, policy on its table", true, MISSING_DB,
                onDb(MISSING_DB).on("table", "t1").allowUser("u", "select"));
        compare(failures, "db does not exist, no policy on it", false, MISSING_DB,
                on("table", "t1").allowUser("u", "select"));
        compare(failures, "policy on a dropped table", true, DB, on("table", "t_gone").allowUser("u", "select"));
        compare(failures, "any catalog, table t1", true, DB,
                policy().on("catalog", "*").on("database", DB).on("table", "t1").allowUser("u", "select"));
        compare(failures, "another catalog, table t1", false, DB,
                policy().on("catalog", "hive_cat").on("database", DB).on("table", "t1").allowUser("u", "select"));
        compare(failures, "deny t1 all with priority, allow all tables select", true, DB,
                on("table", "t1").denyUser("u", all).priority(RangerPolicy.POLICY_PRIORITY_OVERRIDE),
                on("table", "*").allowUser("u", "select"));
        compare(failures, "allow t1 select with priority, deny db all", true, DB,
                onDb(DB).denyUser("u", all),
                on("table", "t1").allowUser("u", "select").priority(RangerPolicy.POLICY_PRIORITY_OVERRIDE));
        compare(failures, "allow all tables", true, DB, on("table", "*").allowUser("u", "select"));
        compare(failures, "allow all tables, db is empty", true, EMPTY_DB,
                onDb(EMPTY_DB).on("table", "*").allowUser("u", "select"));

        Assertions.assertTrue(failures.isEmpty(), String.join("\n", failures));
    }

    @Test
    public void testUserWithoutPolicySkipsTheObjectChecks() {
        RangerStarRocksAccessController controller =
                controllerWith(servicePolicies(List.of(on("table", "t1").allowUser("alice", "select"))));

        Assertions.assertFalse(inOneStep(controller, "nobody", Set.of(), DB));
        // One request on the database and one on all pipes.
        Assertions.assertEquals(2, ACCESS_EVALUATIONS.get());

        ACCESS_EVALUATIONS.set(0);
        Assertions.assertFalse(objectByObject(controller, "nobody", Set.of(), DB));
        Assertions.assertTrue(ACCESS_EVALUATIONS.get() > 2, "object checks: " + ACCESS_EVALUATIONS.get());
    }

    @Test
    public void testDenyByAPolicyRunsTheObjectChecks() {
        RangerStarRocksAccessController controller = controllerWith(servicePolicies(List.of(
                onDb(DB).denyUser("u", allAccessTypes), on("view", "v1").allowUser("u", "select"))));

        Assertions.assertTrue(inOneStep(controller, "u", Set.of(), DB));
        Assertions.assertTrue(ACCESS_EVALUATIONS.get() > 2, "object checks: " + ACCESS_EVALUATIONS.get());
    }

    @Test
    public void testSecurityZoneRunsTheObjectChecks() {
        ServicePolicies servicePolicies = servicePolicies(List.of(on("table", "t1").allowUser("alice", "select")));
        // The zone covers one table of DB, so it may change the answer for DB but not for ONLY_T1_DB.
        ServicePolicies.SecurityZoneInfo zone = new ServicePolicies.SecurityZoneInfo();
        zone.setZoneName("zone1");
        HashMap<String, List<String>> zoneResource = new HashMap<>();
        zoneResource.put("catalog", List.of(CATALOG));
        zoneResource.put("database", List.of(DB));
        zoneResource.put("table", List.of("t9"));
        zone.setResources(List.of(zoneResource));
        zone.setPolicies(new ArrayList<>());
        servicePolicies.setSecurityZones(Map.of("zone1", zone));
        RangerStarRocksAccessController controller = controllerWith(servicePolicies);

        Assertions.assertEquals(objectByObject(controller, "nobody", Set.of(), DB),
                inOneStep(controller, "nobody", Set.of(), DB));
        ACCESS_EVALUATIONS.set(0);
        Assertions.assertFalse(inOneStep(controller, "nobody", Set.of(), DB));
        Assertions.assertTrue(ACCESS_EVALUATIONS.get() > 2, "object checks: " + ACCESS_EVALUATIONS.get());

        ACCESS_EVALUATIONS.set(0);
        Assertions.assertFalse(inOneStep(controller, "nobody", Set.of(), ONLY_T1_DB));
        Assertions.assertEquals(2, ACCESS_EVALUATIONS.get());
    }

    @Test
    public void testRootIsAllowedWithoutRanger() {
        RangerStarRocksAccessController controller =
                controllerWith(servicePolicies(List.of(on("table", "t1").allowUser("alice", "select"))));
        ConnectContext root = new ConnectContext();
        root.setCurrentUserIdentity(UserIdentity.ROOT);
        Assertions.assertDoesNotThrow(() -> controller.checkAnyActionOnOrInDb(root, CATALOG, MISSING_DB));
        Assertions.assertEquals(0, ACCESS_EVALUATIONS.get());
    }
}
