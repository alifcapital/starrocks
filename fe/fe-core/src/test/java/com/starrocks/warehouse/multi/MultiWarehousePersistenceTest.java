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

package com.starrocks.warehouse.multi;

import com.google.gson.stream.JsonReader;
import com.starrocks.persist.DropWarehouseLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

public class MultiWarehousePersistenceTest {
    @Test
    public void testImageRestoresWarehousesAndWorkerGroups() throws Exception {
        MultiWarehouseManager original = new MultiWarehouseManager();
        original.initDefaultWarehouse();
        original.replayCreateWarehouse(new MultiWarehouse(101L, "etl", "loads", 201L,
                Map.of("replica_number", "2"), 1000L));
        original.replayCreateWarehouse(new MultiWarehouse(102L, "interactive", "queries", 202L,
                Map.of(), 2000L));

        MultiWarehouseManager restored = roundTripImage(original);
        Assertions.assertEquals(Set.of("default_warehouse", "etl", "interactive"), restored.getAllWarehouseNames());
        Assertions.assertInstanceOf(DefaultWarehouse.class,
                restored.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID));
        Assertions.assertEquals(201L, restored.getWarehouse("etl").getAnyWorkerGroupId().longValue());
        Assertions.assertEquals(202L, restored.getWarehouse("interactive").getAnyWorkerGroupId().longValue());
        Assertions.assertSame(restored.getWarehouse("etl"), restored.getWarehouse(101L));
        MultiWarehouse etl = (MultiWarehouse) restored.getWarehouse("etl");
        Assertions.assertEquals("2", etl.getProperties().get("replica_number"));
        Assertions.assertEquals("loads", etl.getComment());
        Assertions.assertEquals(1000L, etl.getCreatedTime());

        restored.initDefaultWarehouse();
        Assertions.assertEquals(3, restored.getAllWarehouses().size());
    }

    @Test
    public void testEmptyImageRestoresDefaultWarehouse() throws Exception {
        MultiWarehouseManager original = new MultiWarehouseManager();
        original.initDefaultWarehouse();
        MultiWarehouseManager restored = roundTripImage(original);
        Assertions.assertEquals(Set.of("default_warehouse"), restored.getAllWarehouseNames());
        Assertions.assertInstanceOf(DefaultWarehouse.class,
                restored.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID));
    }

    @Test
    public void testReplayCreateAlterAndDropAfterImage() throws Exception {
        MultiWarehouseManager source = new MultiWarehouseManager();
        source.initDefaultWarehouse();
        source.replayCreateWarehouse(roundTripJournalWarehouse(new MultiWarehouse(101L, "etl", "loads", 201L,
                Map.of("replica_number", "1"), 1000L)));
        MultiWarehouseManager follower = roundTripImage(source);

        MultiWarehouse updated = new MultiWarehouse(101L, "etl", "loads", 201L,
                Map.of("replica_number", "3"), 1000L);
        updated.setUpdatedTime(3000L);
        follower.replayAlterWarehouse(roundTripJournalWarehouse(updated));
        MultiWarehouse restored = (MultiWarehouse) follower.getWarehouse(101L);
        Assertions.assertSame(restored, follower.getWarehouse("etl"));
        Assertions.assertEquals("3", restored.getProperties().get("replica_number"));
        Assertions.assertEquals(3000L, restored.getUpdatedTime());
        Assertions.assertEquals(201L, restored.getAnyWorkerGroupId().longValue());

        DropWarehouseLog drop = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(new DropWarehouseLog("etl")), DropWarehouseLog.class);
        follower.replayDropWarehouse(drop);
        follower.replayDropWarehouse(drop);
        Assertions.assertFalse(follower.warehouseExists("etl"));
        Assertions.assertFalse(follower.warehouseExists(101L));
        Assertions.assertEquals(Set.of("default_warehouse"), roundTripImage(follower).getAllWarehouseNames());
    }

    private static Warehouse roundTripJournalWarehouse(Warehouse warehouse) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(warehouse), Warehouse.class);
    }

    @Test
    public void testBaseManagerPreservesImageAndJournalWhenCreationIsDisabled() throws Exception {
        MultiWarehouseManager enabled = new MultiWarehouseManager();
        enabled.initDefaultWarehouse();
        enabled.replayCreateWarehouse(new MultiWarehouse(101L, "etl", "loads", 201L, Map.of(), 1000L));
        WarehouseManager disabled = roundTripImage(enabled, new WarehouseManager());
        disabled.replayCreateWarehouse(roundTripJournalWarehouse(
                new MultiWarehouse(102L, "interactive", "queries", 202L, Map.of(), 2000L)));
        disabled.replayAlterWarehouse(roundTripJournalWarehouse(
                new MultiWarehouse(101L, "etl", "loads", 201L, Map.of("replica_number", "2"), 1000L)));
        MultiWarehouseManager restored = roundTripImage(disabled, new MultiWarehouseManager());
        Assertions.assertEquals(Set.of("default_warehouse", "etl", "interactive"), restored.getAllWarehouseNames());
        Assertions.assertEquals(201L, restored.getWarehouse("etl").getAnyWorkerGroupId().longValue());
        Assertions.assertEquals("2", ((MultiWarehouse) restored.getWarehouse("etl")).getProperties().get("replica_number"));
        disabled.replayDropWarehouse(new DropWarehouseLog("etl"));
        Assertions.assertFalse(roundTripImage(disabled, new MultiWarehouseManager()).warehouseExists("etl"));
    }

    private static MultiWarehouseManager roundTripImage(MultiWarehouseManager original) throws Exception {
        return roundTripImage(original, new MultiWarehouseManager());
    }

    private static <T extends WarehouseManager> T roundTripImage(WarehouseManager original, T restored) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ImageWriter writer = new ImageWriter("", 0L);
        writer.setOutputStream(bytes);
        original.save(writer);

        SRMetaBlockReader reader = new SRMetaBlockReaderV2(
                new JsonReader(new StringReader(bytes.toString(StandardCharsets.UTF_8))));
        restored.load(reader);
        reader.close();
        return restored;
    }
}
