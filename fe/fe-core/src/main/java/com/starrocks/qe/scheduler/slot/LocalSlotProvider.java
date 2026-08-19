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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

/**
 * Manage slot locally in this FE when disabling query queue.
 */
public class LocalSlotProvider implements SlotProvider {
    private final ConcurrentMap<Long, PipelineDriverAllocator> driverAllocators = new ConcurrentHashMap<>();

    @Override
    public Future<LogicalSlot> requireSlot(LogicalSlot slot) {
        driverAllocators.computeIfAbsent(slot.getWarehouseId(), PipelineDriverAllocator::new).allocate(slot);

        CompletableFuture<LogicalSlot> slotFuture = new CompletableFuture<>();
        slotFuture.complete(slot);
        return slotFuture;
    }

    @Override
    public void cancelSlotRequirement(LogicalSlot slot) {
        releaseSlot(slot);
    }

    @Override
    public void releaseSlot(LogicalSlot slot) {
        if (slot != null) {
            PipelineDriverAllocator allocator = driverAllocators.get(slot.getWarehouseId());
            if (allocator != null) {
                allocator.release(slot);
            }
        }
    }
}
