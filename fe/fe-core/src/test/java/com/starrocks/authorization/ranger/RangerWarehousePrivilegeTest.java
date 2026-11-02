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
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
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
 * Warehouse privileges under Ranger, evaluated by the real Ranger policy engine.
 * <p>
 * We want the test to fail when the shipped service definition and the plugin disagree, so the
 * engine is loaded with conf/ranger/ranger-servicedef-starrocks.json and with policies written the
 * way an administrator writes them in Ranger Admin. Only the policy download is disabled.
 */
public class RangerWarehousePrivilegeTest {
    private static final String SERVICE_DEF_PATH = "../../conf/ranger/ranger-servicedef-starrocks.json";

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
        policies.add(policy(Map.of("warehouse", "wh_usage"), "alice", null, "usage"));
        policies.add(policy(Map.of("warehouse", "wh_admin"), "bob", null, "alter", "drop"));
        policies.add(policy(Map.of("warehouse", "wh_group"), null, "wh_users", "usage"));
        policies.add(policy(Map.of("system", "*"), "carol", null, "create warehouse"));

        ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setServiceName("starrocks");
        servicePolicies.setServiceId(1L);
        servicePolicies.setPolicyVersion(1L);
        servicePolicies.setServiceDef(serviceDef);
        servicePolicies.setPolicies(policies);

        controller = new RangerStarRocksAccessController();
        controller.getRangerPlugin().setPolicies(servicePolicies);
    }

    private RangerPolicy policy(Map<String, String> resources, String user, String group, String... accessTypes) {
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
        RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem(accesses,
                user == null ? null : List.of(user), group == null ? null : List.of(group), null, null, false);
        policy.setPolicyItems(List.of(item));
        return policy;
    }

    private static ConnectContext contextOf(String user, Set<String> groups) {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(new UserIdentity(user, "%"));
        context.setGroups(groups);
        return context;
    }

    private static ConnectContext contextOf(String user) {
        return contextOf(user, Set.of());
    }

    @Test
    public void testServiceDefDeclaresWarehouse() {
        // The plugin does not validate access types against the service definition, but Ranger Admin
        // does: a policy can use only what the service definition declares.
        RangerServiceDef.RangerResourceDef warehouse = serviceDef.getResources().stream()
                .filter(r -> r.getName().equals("warehouse")).findFirst().orElse(null);
        Assertions.assertNotNull(warehouse);
        Assertions.assertTrue(warehouse.getParent() == null || warehouse.getParent().isEmpty());
        Assertions.assertEquals(Set.of("usage", "alter", "drop"), warehouse.getAccessTypeRestrictions());

        Assertions.assertTrue(serviceDef.getAccessTypes().stream().anyMatch(a -> a.getName().equals("create warehouse")));
        RangerServiceDef.RangerResourceDef system = serviceDef.getResources().stream()
                .filter(r -> r.getName().equals("system")).findFirst().orElseThrow();
        Assertions.assertTrue(system.getAccessTypeRestrictions().contains("create warehouse"));
    }

    @Test
    public void testWarehousePolicyAllowsOnlyItsAccessTypes() {
        ConnectContext alice = contextOf("alice");
        Assertions.assertDoesNotThrow(() -> controller.checkWarehouseAction(alice, "wh_usage", PrivilegeType.USAGE));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkWarehouseAction(alice, "wh_usage", PrivilegeType.ALTER));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkWarehouseAction(alice, "wh_usage", PrivilegeType.DROP));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkWarehouseAction(alice, "wh_admin", PrivilegeType.USAGE));

        ConnectContext bob = contextOf("bob");
        Assertions.assertDoesNotThrow(() -> controller.checkWarehouseAction(bob, "wh_admin", PrivilegeType.ALTER));
        Assertions.assertDoesNotThrow(() -> controller.checkWarehouseAction(bob, "wh_admin", PrivilegeType.DROP));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkWarehouseAction(bob, "wh_admin", PrivilegeType.USAGE));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkWarehouseAction(bob, "wh_usage", PrivilegeType.ALTER));
    }

    @Test
    public void testWarehousePolicyOnGroup() {
        Assertions.assertDoesNotThrow(() -> controller.checkWarehouseAction(
                contextOf("erin", Set.of("wh_users")), "wh_group", PrivilegeType.USAGE));
        Assertions.assertThrows(AccessDeniedException.class, () -> controller.checkWarehouseAction(
                contextOf("erin", Set.of("other")), "wh_group", PrivilegeType.USAGE));
    }

    @Test
    public void testAnyActionOnWarehouse() {
        Assertions.assertDoesNotThrow(() -> controller.checkAnyActionOnWarehouse(contextOf("alice"), "wh_usage"));
        Assertions.assertDoesNotThrow(() -> controller.checkAnyActionOnWarehouse(contextOf("bob"), "wh_admin"));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkAnyActionOnWarehouse(contextOf("alice"), "wh_admin"));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkAnyActionOnWarehouse(contextOf("bob"), "wh_usage"));
    }

    @Test
    public void testUserWithoutPolicyIsDenied() {
        ConnectContext dave = contextOf("dave");
        for (PrivilegeType privilegeType : List.of(PrivilegeType.USAGE, PrivilegeType.ALTER, PrivilegeType.DROP)) {
            Assertions.assertThrows(AccessDeniedException.class,
                    () -> controller.checkWarehouseAction(dave, "wh_usage", privilegeType));
        }
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkAnyActionOnWarehouse(dave, "wh_usage"));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkSystemAction(dave, PrivilegeType.CREATE_WAREHOUSE));
    }

    @Test
    public void testRootIsAllowedWithoutPolicy() {
        ConnectContext root = new ConnectContext();
        root.setCurrentUserIdentity(UserIdentity.ROOT);
        for (PrivilegeType privilegeType : List.of(PrivilegeType.USAGE, PrivilegeType.ALTER, PrivilegeType.DROP)) {
            Assertions.assertDoesNotThrow(() -> controller.checkWarehouseAction(root, "wh_no_policy", privilegeType));
        }
        Assertions.assertDoesNotThrow(() -> controller.checkAnyActionOnWarehouse(root, "wh_no_policy"));
        Assertions.assertDoesNotThrow(() -> controller.checkSystemAction(root, PrivilegeType.CREATE_WAREHOUSE));
    }

    @Test
    public void testCreateWarehouseOnSystem() {
        Assertions.assertDoesNotThrow(() -> controller.checkSystemAction(contextOf("carol"), PrivilegeType.CREATE_WAREHOUSE));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkSystemAction(contextOf("alice"), PrivilegeType.CREATE_WAREHOUSE));
        Assertions.assertThrows(AccessDeniedException.class,
                () -> controller.checkSystemAction(contextOf("carol"), PrivilegeType.OPERATE));
    }
}
