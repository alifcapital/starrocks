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
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.ranger.starrocks.RangerStarRocksAccessController;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.pipe.PipeName;
import mockit.Mock;
import mockit.MockUp;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.Assertions;
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

/**
 * SECURITY and pipe privileges under Ranger, evaluated by the real Ranger policy engine.
 * <p>
 * We want the test to fail when the shipped service definition and the plugin disagree, so the
 * engine is loaded with conf/ranger/ranger-servicedef-starrocks.json and with policies written the
 * way an administrator writes them in Ranger Admin. Only the policy download is disabled.
 */
public class RangerSecurityAndPipePrivilegeTest {
    private static final String SERVICE_DEF_PATH = "../../conf/ranger/ranger-servicedef-starrocks.json";
    private static final String CATALOG = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;

    private RangerServiceDef serviceDef;
    private RangerStarRocksAccessController controller;
    private long nextPolicyId = 1;

    @BeforeEach
    public void setUp() throws Exception {
        // init() starts the download of policies from Ranger Admin; the policies here are set directly.
        new MockUp<RangerBasePlugin>() {
            @Mock
            void init() {
            }
        };

        Path path = Paths.get(System.getProperty("user.dir"), SERVICE_DEF_PATH);
        serviceDef = JsonUtils.jsonToObject(Files.readString(path, StandardCharsets.UTF_8), RangerServiceDef.class);
        Assertions.assertNotNull(serviceDef, "cannot parse " + path);

        List<RangerPolicy> policies = new ArrayList<>();
        policies.add(policy(Map.of("system", "*"), "sec_admin", "security"));
        // Catalog "*" is what an administrator writes for "all pipes in all databases".
        policies.add(policy(Map.of("catalog", "*", "database", "*", "pipe", "*"), "pipe_user", "usage"));
        policies.add(policy(Map.of("catalog", CATALOG, "database", "db1", "pipe", "p1"), "pipe_admin",
                "alter", "drop"));
        policies.add(policy(Map.of("catalog", CATALOG, "database", "db1"), "pipe_creator", "create pipe"));

        ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setServiceName("starrocks");
        servicePolicies.setServiceId(1L);
        servicePolicies.setPolicyVersion(1L);
        servicePolicies.setServiceDef(serviceDef);
        servicePolicies.setPolicies(policies);

        controller = new RangerStarRocksAccessController();
        controller.getRangerPlugin().setPolicies(servicePolicies);
    }

    private RangerPolicy policy(Map<String, String> resources, String user, String... accessTypes) {
        RangerPolicy policy = new RangerPolicy();
        policy.setId(nextPolicyId);
        policy.setName("policy_" + nextPolicyId);
        nextPolicyId++;
        policy.setService("starrocks");
        policy.setIsEnabled(true);

        Map<String, RangerPolicy.RangerPolicyResource> policyResources = new HashMap<>();
        resources.forEach((name, value) -> policyResources.put(name, new RangerPolicy.RangerPolicyResource(value)));
        policy.setResources(policyResources);

        List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();
        for (String accessType : accessTypes) {
            accesses.add(new RangerPolicy.RangerPolicyItemAccess(accessType, true));
        }
        RangerPolicy.RangerPolicyItem item =
                new RangerPolicy.RangerPolicyItem(accesses, List.of(user), null, null, null, false);
        policy.setPolicyItems(List.of(item));
        return policy;
    }

    private static ConnectContext contextOf(String user) {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(new UserIdentity(user, "%"));
        context.setGroups(Set.of());
        return context;
    }

    private RangerServiceDef.RangerResourceDef resourceDef(String name) {
        return serviceDef.getResources().stream().filter(r -> r.getName().equals(name)).findFirst().orElse(null);
    }

    @Test
    public void testServiceDefDeclaresSecurityAndPipe() {
        // The plugin does not validate access types against the service definition, but Ranger Admin
        // does: a policy can use only what the service definition declares.
        Assertions.assertTrue(serviceDef.getAccessTypes().stream().anyMatch(a -> a.getName().equals("security")));
        Assertions.assertTrue(serviceDef.getAccessTypes().stream().anyMatch(a -> a.getName().equals("create pipe")));
        Assertions.assertTrue(resourceDef("system").getAccessTypeRestrictions().contains("security"));
        Assertions.assertTrue(resourceDef("database").getAccessTypeRestrictions().contains("create pipe"));

        RangerServiceDef.RangerResourceDef pipe = resourceDef("pipe");
        Assertions.assertNotNull(pipe);
        Assertions.assertEquals("database", pipe.getParent());
        Assertions.assertEquals(resourceDef("view").getLevel(), pipe.getLevel());
        Assertions.assertEquals(Set.of("usage", "alter", "drop"), pipe.getAccessTypeRestrictions());
    }

    @Test
    public void testSecurityOnSystem() {
        Assertions.assertDoesNotThrow(() -> controller.checkSystemAction(contextOf("sec_admin"), PrivilegeType.SECURITY));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkSystemAction(contextOf("sec_admin"), PrivilegeType.OPERATE));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkSystemAction(contextOf("pipe_user"), PrivilegeType.SECURITY));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkSystemAction(contextOf("nobody"), PrivilegeType.SECURITY));
    }

    @Test
    public void testPipePolicyWithAnyCatalog() {
        ConnectContext pipeUser = contextOf("pipe_user");
        PipeName pipe = new PipeName("db2", "p9");
        Assertions.assertDoesNotThrow(() -> controller.checkPipeAction(pipeUser, pipe, PrivilegeType.USAGE));
        Assertions.assertDoesNotThrow(() -> controller.checkAnyActionOnPipe(pipeUser, pipe));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkPipeAction(pipeUser, pipe, PrivilegeType.ALTER));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkPipeAction(pipeUser, pipe, PrivilegeType.DROP));
    }

    @Test
    public void testPipePolicyAllowsOnlyItsAccessTypes() {
        ConnectContext pipeAdmin = contextOf("pipe_admin");
        PipeName p1 = new PipeName("db1", "p1");
        Assertions.assertDoesNotThrow(() -> controller.checkPipeAction(pipeAdmin, p1, PrivilegeType.ALTER));
        Assertions.assertDoesNotThrow(() -> controller.checkPipeAction(pipeAdmin, p1, PrivilegeType.DROP));
        Assertions.assertDoesNotThrow(() -> controller.checkAnyActionOnPipe(pipeAdmin, p1));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkPipeAction(pipeAdmin, p1, PrivilegeType.USAGE));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkPipeAction(pipeAdmin, new PipeName("db1", "p2"), PrivilegeType.ALTER));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkPipeAction(pipeAdmin, new PipeName("db2", "p1"), PrivilegeType.ALTER));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkAnyActionOnPipe(pipeAdmin, new PipeName("db1", "p2")));
    }

    @Test
    public void testCreatePipeOnDatabase() {
        Assertions.assertDoesNotThrow(
                () -> controller.checkDbAction(contextOf("pipe_creator"), CATALOG, "db1", PrivilegeType.CREATE_PIPE));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkDbAction(contextOf("pipe_creator"), CATALOG, "db2", PrivilegeType.CREATE_PIPE));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkDbAction(contextOf("pipe_creator"), CATALOG, "db1", PrivilegeType.CREATE_TABLE));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkDbAction(contextOf("pipe_admin"), CATALOG, "db1", PrivilegeType.CREATE_PIPE));
    }

    @Test
    public void testUserWithoutPolicyIsDenied() {
        ConnectContext nobody = contextOf("nobody");
        PipeName p1 = new PipeName("db1", "p1");
        for (PrivilegeType privilegeType : List.of(PrivilegeType.USAGE, PrivilegeType.ALTER, PrivilegeType.DROP)) {
            Assertions.assertThrows(AccessDeniedException.class,
                    () -> controller.checkPipeAction(nobody, p1, privilegeType));
        }
        Assertions.assertThrows(AccessDeniedException.class, () -> controller.checkAnyActionOnPipe(nobody, p1));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkDbAction(nobody, CATALOG, "db1", PrivilegeType.CREATE_PIPE));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkSystemAction(nobody, PrivilegeType.SECURITY));
    }

    @Test
    public void testRootIsAllowedWithoutPolicy() {
        ConnectContext root = new ConnectContext();
        root.setCurrentUserIdentity(UserIdentity.ROOT);
        PipeName pipe = new PipeName("db_no_policy", "p_no_policy");
        for (PrivilegeType privilegeType : List.of(PrivilegeType.USAGE, PrivilegeType.ALTER, PrivilegeType.DROP)) {
            Assertions.assertDoesNotThrow(() -> controller.checkPipeAction(root, pipe, privilegeType));
        }
        Assertions.assertDoesNotThrow(() -> controller.checkAnyActionOnPipe(root, pipe));
        Assertions.assertDoesNotThrow(
                () -> controller.checkDbAction(root, CATALOG, "db_no_policy", PrivilegeType.CREATE_PIPE));
        Assertions.assertDoesNotThrow(() -> controller.checkSystemAction(root, PrivilegeType.SECURITY));
    }
}
