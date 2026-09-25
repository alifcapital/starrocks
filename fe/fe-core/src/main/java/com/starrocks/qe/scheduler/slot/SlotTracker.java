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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;

import java.util.List;
import java.util.Optional;

public class SlotTracker extends BaseSlotTracker {

    public SlotTracker(SlotManager slotManager, ResourceUsageMonitor resourceUsageMonitor) {
        this(slotManager, resourceUsageMonitor, WarehouseManager.DEFAULT_WAREHOUSE_ID);
    }

    public SlotTracker(SlotManager slotManager, ResourceUsageMonitor resourceUsageMonitor, long warehouseId) {
        super(resourceUsageMonitor, warehouseId);

        this.slotSelectionStrategy = createSlotSelectionStrategy(slotManager, resourceUsageMonitor);
        this.listeners = ImmutableList.of(slotSelectionStrategy,
                new SlotListenerForPipelineDriverAllocator(warehouseId));
    }

    @VisibleForTesting
    public SlotTracker(BaseSlotManager slotManager, List<Listener> listeners) {
        super(GlobalStateMgr.getCurrentState().getResourceUsageMonitor(), WarehouseManager.DEFAULT_WAREHOUSE_ID);
        this.listeners = listeners;
        if (listeners.isEmpty()) {
            this.slotSelectionStrategy = createSlotSelectionStrategy(slotManager, this.resourceUsageMonitor);
        } else {
            Preconditions.checkArgument(!listeners.isEmpty());
            Preconditions.checkArgument(listeners.get(0) instanceof SlotSelectionStrategy);
            this.slotSelectionStrategy = (SlotSelectionStrategy) listeners.get(0);
        }
    }

    private SlotSelectionStrategy createSlotSelectionStrategy(BaseSlotManager slotManager,
                                                              ResourceUsageMonitor resourceUsageMonitor) {
        if (Config.enable_query_queue_v2) {
            return new SlotSelectionStrategyV2(slotManager, this.warehouseId);
        } else {
            return new DefaultSlotSelectionStrategy(
                    () -> resourceUsageMonitor.isWarehouseResourceOverloaded(warehouseId),
                    groupId -> resourceUsageMonitor.isGroupResourceOverloaded(warehouseId, groupId));
        }
    }

    @Override
    public Optional<Integer> getMaxSlots() {
        if (slotSelectionStrategy instanceof SlotSelectionStrategyV2) {
            return Optional.of(QueryQueueOptions.createFromEnv(warehouseId).v2().getTotalSlots());
        }
        return Optional.empty();
    }
}
