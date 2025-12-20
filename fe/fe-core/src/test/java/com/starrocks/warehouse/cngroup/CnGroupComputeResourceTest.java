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

package com.starrocks.warehouse.cngroup;

import com.starrocks.server.WarehouseManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CnGroupComputeResourceTest {

    @Test
    public void testBasicConstruction() {
        CnGroupComputeResource resource = new CnGroupComputeResource(1L, "etl");
        Assertions.assertEquals(1L, resource.getWarehouseId());
        Assertions.assertEquals("etl", resource.getCnGroupName());
    }

    @Test
    public void testOfFactoryMethod() {
        CnGroupComputeResource resource = CnGroupComputeResource.of(2L, "analytics");
        Assertions.assertEquals(2L, resource.getWarehouseId());
        Assertions.assertEquals("analytics", resource.getCnGroupName());
    }

    @Test
    public void testForSystemTask() {
        // System tasks should use ALL_GROUPS to bypass CNGroup filtering
        CnGroupComputeResource resource = CnGroupComputeResource.forSystemTask(3L);
        Assertions.assertEquals(3L, resource.getWarehouseId());
        Assertions.assertEquals(CnGroupComputeResource.ALL_GROUPS, resource.getCnGroupName());
        Assertions.assertTrue(resource.isAllGroups());
    }

    @Test
    public void testIsAllGroups() {
        // "*" means all groups
        CnGroupComputeResource allGroups = CnGroupComputeResource.of(1L, CnGroupComputeResource.ALL_GROUPS);
        Assertions.assertTrue(allGroups.isAllGroups());

        // null also means all groups
        CnGroupComputeResource nullGroup = CnGroupComputeResource.of(1L, null);
        Assertions.assertTrue(nullGroup.isAllGroups());

        // Specific group is NOT all groups
        CnGroupComputeResource etlGroup = CnGroupComputeResource.of(1L, "etl");
        Assertions.assertFalse(etlGroup.isAllGroups());

        CnGroupComputeResource defaultGroup = CnGroupComputeResource.of(1L, CnGroup.DEFAULT_GROUP_NAME);
        Assertions.assertFalse(defaultGroup.isAllGroups());
    }

    @Test
    public void testEquality() {
        CnGroupComputeResource r1 = CnGroupComputeResource.of(1L, "etl");
        CnGroupComputeResource r2 = CnGroupComputeResource.of(1L, "etl");
        CnGroupComputeResource r3 = CnGroupComputeResource.of(1L, "analytics");
        CnGroupComputeResource r4 = CnGroupComputeResource.of(2L, "etl");

        Assertions.assertEquals(r1, r2);
        Assertions.assertNotEquals(r1, r3);  // Different cnGroupName
        Assertions.assertNotEquals(r1, r4);  // Different warehouseId
    }

    @Test
    public void testHashCode() {
        CnGroupComputeResource r1 = CnGroupComputeResource.of(1L, "etl");
        CnGroupComputeResource r2 = CnGroupComputeResource.of(1L, "etl");

        Assertions.assertEquals(r1.hashCode(), r2.hashCode());
    }

    @Test
    public void testToString() {
        CnGroupComputeResource resource = CnGroupComputeResource.of(1L, "etl");
        String str = resource.toString();
        Assertions.assertTrue(str.contains("warehouseId=1"));
        Assertions.assertTrue(str.contains("cnGroupName=etl"));
    }

    @Test
    public void testWarehouseComputeResourceCnGroupNameIsDefault() {
        // WarehouseComputeResource (used by DEFAULT_RESOURCE) should return "default"
        // System tasks run on "default" group, leaving other groups for dedicated workloads
        WarehouseComputeResource resource = WarehouseComputeResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assertions.assertEquals(CnGroup.DEFAULT_GROUP_NAME, resource.getCnGroupName());
    }

    @Test
    public void testDefaultResourceCnGroupNameIsDefault() {
        // DEFAULT_RESOURCE should use "default" cnGroupName
        ComputeResource defaultResource = WarehouseManager.DEFAULT_RESOURCE;
        Assertions.assertEquals(CnGroup.DEFAULT_GROUP_NAME, defaultResource.getCnGroupName());
    }

    @Test
    public void testCnGroupComputeResourceExtendsWarehouseComputeResource() {
        CnGroupComputeResource resource = CnGroupComputeResource.of(1L, "etl");
        Assertions.assertTrue(resource instanceof WarehouseComputeResource);
        Assertions.assertTrue(resource instanceof ComputeResource);
    }

    @Test
    public void testAllGroupsConstant() {
        Assertions.assertEquals("*", CnGroupComputeResource.ALL_GROUPS);
    }
}
