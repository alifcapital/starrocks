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

package com.starrocks.catalog;

import com.google.gson.JsonObject;
import com.starrocks.common.Config;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.UpdateDictionaryMgrLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.CreateDictionaryStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.multi.MultiWarehouse;
import com.starrocks.warehouse.multi.MultiWarehouseManager;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DictionaryWarehouseTest {
    private boolean enabled;
    private String background;
    private final List<DictionaryMgr.RefreshDictionaryCacheWorker> submitted = new ArrayList<>();
    private DictionaryMgr manager;

    @BeforeEach
    public void setUp() {
        UtFrameUtils.setUpForPersistTest();
        enabled = Config.enable_multi_warehouse;
        background = Config.lake_background_warehouse;
        Config.enable_multi_warehouse = true;
        Config.lake_background_warehouse = "interactive";
        WarehouseManager warehouses = new MultiWarehouseManager();
        warehouses.initDefaultWarehouse();
        warehouses.addWarehouse(new MultiWarehouse(101, "etl", "", 201, Map.of(), 0));
        warehouses.addWarehouse(new MultiWarehouse(102, "interactive", "", 202, Map.of(), 0));
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return warehouses;
            }

            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        manager = capturingManager();
    }

    @AfterEach
    public void tearDown() {
        Config.enable_multi_warehouse = enabled;
        Config.lake_background_warehouse = background;
        UtFrameUtils.tearDownForPersisTest();
    }

    private DictionaryMgr capturingManager() {
        return new DictionaryMgr() {
            @Override
            protected void submit(RefreshDictionaryCacheWorker task) {
                submitted.add(task);
            }
        };
    }

    private Dictionary create(boolean warmUp) throws Exception {
        CreateDictionaryStmt stmt = new CreateDictionaryStmt("dict", "source", List.of("k"), List.of("v"),
                Map.of("dictionary_warm_up", String.valueOf(warmUp), "dictionary_refresh_interval", "10"),
                NodePosition.ZERO);
        manager.createDictionary(stmt, "default_catalog", "db", 101);
        return manager.getDictionaryByName("dict");
    }

    @Test
    public void testManualRefreshSnapshotsWarehouseBeforeScheduling() throws Exception {
        Dictionary dictionary = create(false);
        ConnectContext caller = ConnectContext.buildInner();
        caller.setCurrentWarehouse("etl");
        manager.refreshDictionary("dict", caller.getCurrentWarehouseId());
        caller.setCurrentWarehouse("interactive");
        manager.scheduleTasks();
        Assertions.assertEquals(1, submitted.size());
        Assertions.assertEquals(101L, dictionary.getRefreshWarehouseId());
        dictionary.setRefreshing(System.currentTimeMillis(), 102L);
        ConnectContext execution = submitted.get(0).buildConnectContext();
        Assertions.assertEquals("etl", execution.getCurrentWarehouseName());
        JobSpec job = JobSpec.Factory.fromRefreshDictionaryCacheSpec(execution, execution.getExecutionId(),
                new DescriptorTable(), List.of(), List.of(), null);
        Assertions.assertEquals(execution.getCurrentComputeResource(), job.getComputeResource());
    }

    @Test
    public void testWarmUpUsesCreatorWarehouseAndAutomaticRefreshUsesBackground() throws Exception {
        Dictionary dictionary = create(true);
        manager.scheduleTasks();
        Assertions.assertEquals("etl", submitted.get(0).buildConnectContext().getCurrentWarehouseName());
        submitted.get(0).finish();
        dictionary.setNextSchedulableTime(0);
        manager.scheduleTasks();
        Assertions.assertEquals(2, submitted.size());
        Assertions.assertNull(dictionary.getRefreshWarehouseId());
        Assertions.assertEquals("interactive", submitted.get(1).buildConnectContext().getCurrentWarehouseName());
    }

    @Test
    public void testReplayAndLeaderRecoveryKeepManualWarehouse() throws Exception {
        create(false);
        Dictionary replayDictionary = (Dictionary) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_DICTIONARY);
        manager.refreshDictionary("dict", 101);
        UpdateDictionaryMgrLog journal = (UpdateDictionaryMgrLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MODIFY_DICTIONARY_MGR_V2);
        DictionaryMgr follower = capturingManager();
        follower.replayCreateDictionary(replayDictionary);
        follower.replayModifyDictionaryMgr(journal);
        Assertions.assertEquals(101L, follower.getDictionaryByName("dict").getRefreshWarehouseId());
        follower.scheduleTasks();
        Assertions.assertEquals(1, submitted.size());
        Assertions.assertEquals("etl", submitted.get(0).buildConnectContext().getCurrentWarehouseName());
    }

    @Test
    public void testDisabledExecutionAndLegacyMetadata() throws Exception {
        Dictionary dictionary = create(true);
        manager.scheduleTasks();
        Config.enable_multi_warehouse = false;
        Assertions.assertEquals("default_warehouse", submitted.get(0).buildConnectContext().getCurrentWarehouseName());
        Config.enable_multi_warehouse = true;
        Assertions.assertEquals("etl", submitted.get(0).buildConnectContext().getCurrentWarehouseName());
        JsonObject json = GsonUtils.GSON.toJsonTree(dictionary).getAsJsonObject();
        Assertions.assertEquals(101L, GsonUtils.GSON.fromJson(json, Dictionary.class).getRefreshWarehouseId());
        json.remove("refreshWarehouseId");
        Dictionary legacy = GsonUtils.GSON.fromJson(json, Dictionary.class);
        Assertions.assertNull(legacy.getRefreshWarehouseId());
        Assertions.assertEquals("interactive",
                manager.new RefreshDictionaryCacheWorker(legacy, 1).buildConnectContext().getCurrentWarehouseName());
    }

    @Test
    public void testMissingManualWarehouseDoesNotFallBack() throws Exception {
        create(false);
        manager.refreshDictionary("dict", 999);
        manager.scheduleTasks();
        Assertions.assertThrows(RuntimeException.class, () -> submitted.get(0).buildConnectContext());
    }
}
