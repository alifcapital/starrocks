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

package com.starrocks.qe.scheduler.slot;

import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class WarehouseSlotIsolationTest {
    private static final long ETL = 101;
    private static final long INTERACTIVE = 102;
    private boolean previousV2;
    private int previousLevel;
    private String previousEstimator;
    private int previousQueueLimit;
    private int previousConcurrencyLimit;
    private int previousCpuLimit;
    private int previousHistory;
    private SlotManager manager;
    private ResourceUsageMonitor monitor;

    @BeforeAll
    public static void initMetrics() {
        MetricRepo.init();
    }

    @BeforeEach
    public void setUp() {
        previousV2 = Config.enable_query_queue_v2;
        previousLevel = Config.query_queue_v2_concurrency_level;
        previousEstimator = Config.query_queue_slots_estimator_strategy;
        previousHistory = Config.max_query_queue_history_slots_number;
        previousQueueLimit = GlobalVariable.getQueryQueueMaxQueuedQueries();
        previousConcurrencyLimit = GlobalVariable.getQueryQueueConcurrencyLimit();
        previousCpuLimit = GlobalVariable.getQueryQueueCpuUsedPermilleLimit();
        Config.enable_query_queue_v2 = true;
        Config.query_queue_v2_concurrency_level = 4;
        Config.query_queue_slots_estimator_strategy = "PBE";
        Config.max_query_queue_history_slots_number = 0;
        GlobalVariable.setQueryQueueMaxQueuedQueries(100);
        GlobalVariable.setQueryQueueConcurrencyLimit(-1);
        BackendResourceStat.getInstance().reset();
        BackendResourceStat.getInstance().setNumCoresOfBe(0, 1, 32);
        BackendResourceStat.getInstance().setNumCoresOfBe(ETL, 2, 2);
        BackendResourceStat.getInstance().setNumCoresOfBe(INTERACTIVE, 3, 8);
        monitor = new ResourceUsageMonitor();
        manager = new SlotManager(monitor);
    }

    @AfterEach
    public void tearDown() {
        Config.enable_query_queue_v2 = previousV2;
        Config.query_queue_v2_concurrency_level = previousLevel;
        Config.query_queue_slots_estimator_strategy = previousEstimator;
        Config.max_query_queue_history_slots_number = previousHistory;
        GlobalVariable.setQueryQueueMaxQueuedQueries(previousQueueLimit);
        GlobalVariable.setQueryQueueConcurrencyLimit(previousConcurrencyLimit);
        GlobalVariable.setQueryQueueCpuUsedPermilleLimit(previousCpuLimit);
        BackendResourceStat.getInstance().reset();
    }

    @Test
    public void testSaturatedWarehouseDoesNotBlockAnotherWarehouse() {
        SlotTracker etl = manager.getSlotTracker(ETL);
        SlotTracker interactive = manager.getSlotTracker(INTERACTIVE);
        assertThat(etl.getMaxSlots()).contains(2);
        assertThat(interactive.getMaxSlots()).contains(8);

        LogicalSlot running = slot(ETL, 2);
        assertThat(etl.requireSlot(running)).isTrue();
        assertThat(etl.peakSlotsToAllocate()).containsExactly(running);
        etl.allocateSlot(running);
        LogicalSlot waiting = slot(ETL, 1);
        assertThat(etl.requireSlot(waiting)).isTrue();
        assertThat(etl.peakSlotsToAllocate()).isEmpty();

        LogicalSlot other = slot(INTERACTIVE, 8);
        assertThat(interactive.requireSlot(other)).isTrue();
        assertThat(interactive.peakSlotsToAllocate()).containsExactly(other);
        interactive.allocateSlot(other);
        assertThat(interactive.getNumAllocatedSlots()).isEqualTo(8);
        assertThat(etl.getNumAllocatedSlots()).isEqualTo(2);
        assertThat(manager.getSlots()).containsExactlyInAnyOrder(running, waiting, other);
        assertThat(manager.getWarehouseIdToSlotTracker()).containsOnlyKeys(ETL, INTERACTIVE);

        etl.releaseSlot(running.getSlotId());
        assertThat(etl.peakSlotsToAllocate()).containsExactly(waiting);
        assertThat(interactive.getNumAllocatedSlots()).isEqualTo(8);
        assertThat(interactive.releaseSlot(running.getSlotId())).isNull();
    }

    @Test
    public void testPendingCapacityIsPerWarehouse() {
        GlobalVariable.setQueryQueueMaxQueuedQueries(1);
        SlotTracker etl = manager.getSlotTracker(ETL);
        SlotTracker interactive = manager.getSlotTracker(INTERACTIVE);
        assertThat(etl.requireSlot(slot(ETL, 1))).isTrue();
        assertThat(etl.requireSlot(slot(ETL, 1))).isFalse();
        assertThat(interactive.requireSlot(slot(INTERACTIVE, 1))).isTrue();
    }

    @Test
    public void testConcurrencyLimitIsPerWarehouse() {
        GlobalVariable.setQueryQueueConcurrencyLimit(1);
        SlotTracker etl = manager.getSlotTracker(ETL);
        SlotTracker interactive = manager.getSlotTracker(INTERACTIVE);
        LogicalSlot running = slot(ETL, 1);
        etl.requireSlot(running);
        etl.allocateSlot(running);
        etl.requireSlot(slot(ETL, 1));
        assertThat(etl.peakSlotsToAllocate()).isEmpty();
        LogicalSlot other = slot(INTERACTIVE, 1);
        interactive.requireSlot(other);
        assertThat(interactive.peakSlotsToAllocate()).containsExactly(other);
    }

    @Test
    public void testBatchAllocationRespectsConcurrencyLimit() {
        GlobalVariable.setQueryQueueConcurrencyLimit(1);
        SlotTracker etl = manager.getSlotTracker(ETL);
        etl.requireSlot(slot(ETL, 1));
        etl.requireSlot(slot(ETL, 1));
        assertThat(etl.peakSlotsToAllocate()).hasSize(1);
    }

    @Test
    public void testLocalDriverAllocationIsPerWarehouse() throws Exception {
        int highWater = GlobalVariable.getQueryQueueDriverHighWater();
        int lowWater = GlobalVariable.getQueryQueueDriverLowWater();
        try {
            GlobalVariable.setQueryQueueDriverHighWater(4);
            GlobalVariable.setQueryQueueDriverLowWater(-1);
            LocalSlotProvider provider = new LocalSlotProvider();
            for (int i = 0; i < 4; i++) {
                provider.requireSlot(slot(ETL, 1)).get();
            }
            long now = System.currentTimeMillis();
            LogicalSlot interactive = new LogicalSlot(UUIDUtil.genTUniqueId(), "fe", INTERACTIVE,
                    LogicalSlot.ABSENT_GROUP_ID, 1, now + 60000, now + 120000, now, 1, 0);
            provider.requireSlot(interactive).get();
            assertThat(interactive.getPipelineDop()).isEqualTo(4);
        } finally {
            GlobalVariable.setQueryQueueDriverHighWater(highWater);
            GlobalVariable.setQueryQueueDriverLowWater(lowWater);
        }
    }

    @Test
    public void testV1OverloadAndRecoveryArePerWarehouse() {
        Config.enable_query_queue_v2 = false;
        GlobalVariable.setQueryQueueCpuUsedPermilleLimit(500);
        ComputeNode etlNode = node(2, ETL);
        ComputeNode interactiveNode = node(3, INTERACTIVE);
        List<ComputeNode> nodes = List.of(etlNode, interactiveNode);
        new MockUp<SystemInfoService>() {
            @Mock
            public Stream<ComputeNode> backendAndComputeNodeStream() {
                return nodes.stream();
            }
        };
        etlNode.updateResourceUsage(1, 0, 900);
        interactiveNode.updateResourceUsage(1, 0, 100);
        monitor.notifyResourceUsageUpdate();
        SlotTracker etl = manager.getSlotTracker(ETL);
        SlotTracker interactive = manager.getSlotTracker(INTERACTIVE);
        LogicalSlot waiting = slot(ETL, 1);
        LogicalSlot other = slot(INTERACTIVE, 1);
        etl.requireSlot(waiting);
        interactive.requireSlot(other);
        assertThat(etl.peakSlotsToAllocate()).isEmpty();
        assertThat(interactive.peakSlotsToAllocate()).containsExactly(other);

        interactiveNode.updateResourceUsage(1, 0, 900);
        monitor.notifyResourceUsageUpdate();
        AtomicInteger notifications = new AtomicInteger();
        monitor.registerResourceAvailableListener(notifications::incrementAndGet);
        etlNode.updateResourceUsage(1, 0, 100);
        monitor.notifyResourceUsageUpdate();
        assertThat(monitor.isGlobalResourceOverloaded()).isTrue();
        assertThat(notifications.get()).isEqualTo(1);
        assertThat(etl.peakSlotsToAllocate()).containsExactly(waiting);
        assertThat(interactive.peakSlotsToAllocate()).isEmpty();
    }

    private static ComputeNode node(long nodeId, long warehouseId) {
        ComputeNode node = new ComputeNode(nodeId, "127.0.0.1", 9050);
        node.setWarehouseId(warehouseId);
        node.setAlive(true);
        return node;
    }

    private static LogicalSlot slot(long warehouseId, int physicalSlots) {
        long now = System.currentTimeMillis();
        return new LogicalSlot(UUIDUtil.genTUniqueId(), "fe", warehouseId, LogicalSlot.ABSENT_GROUP_ID,
                physicalSlots, now + 60000, now + 120000, now, 1, 1);
    }
}
