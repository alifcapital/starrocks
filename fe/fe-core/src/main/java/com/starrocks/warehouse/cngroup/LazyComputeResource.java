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
// limitations under the License

package com.starrocks.warehouse.cngroup;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.gson.annotations.SerializedName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Introduce lazy loading for ComputeResource to avoid unnecessary computation and ensure that compute resources
 * are acquired after the query queue scheduling is completed.
 */
public class LazyComputeResource implements ComputeResource {
    private static final Logger LOG = LogManager.getLogger(LazyComputeResource.class);

    @SerializedName("warehouseId")
    private final long warehouseId;
    @SerializedName("cnGroupName")
    private final String cnGroupName;
    // Supplier and runtime flag are not part of persisted state — at replay time the
    // lazy supplier is unavailable; get() falls back to a fresh acquire via WarehouseManager.
    private transient Supplier<ComputeResource> lazy;
    private transient AtomicBoolean initialized = new AtomicBoolean(false);

    private LazyComputeResource(long warehouseId, String cnGroupName, Supplier<ComputeResource> lazy) {
        this.warehouseId = warehouseId;
        this.cnGroupName = cnGroupName;
        this.lazy = lazy == null ? null : Suppliers.memoize(lazy);
    }

    public static LazyComputeResource of(long warehouseId, Supplier<ComputeResource> lazy) {
        return new LazyComputeResource(warehouseId, CnGroupComputeResource.DEFAULT_GROUP_NAME, lazy);
    }

    public static LazyComputeResource of(long warehouseId, String cnGroupName, Supplier<ComputeResource> lazy) {
        return new LazyComputeResource(warehouseId, cnGroupName, lazy);
    }

    public ComputeResource get() {
        if (LOG.isDebugEnabled()) {
            String queryId = ConnectContext.get() != null ? ConnectContext.get().getQueryId().toString() : "N/A";
            LOG.debug("Materializing ComputeResource in LazyComputeResource, queryId: {}", queryId);
        }

        if (lazy == null) {
            // Post-deserialize path (edit-log replay, image load): the original supplier
            // closure is gone, so re-acquire through the warehouse manager using the
            // persisted warehouseId/cnGroupName. Falling back to WarehouseComputeResource
            // keeps replay alive even if the warehouse is temporarily unavailable.
            try {
                return GlobalStateMgr.getCurrentState().getWarehouseMgr()
                        .acquireComputeResource(warehouseId, this);
            } catch (Exception e) {
                LOG.warn("post-deserialize acquireComputeResource failed for warehouseId={}, cnGroupName={}: {}",
                        warehouseId, cnGroupName, e.getMessage());
                return WarehouseComputeResource.of(warehouseId);
            }
        }

        ComputeResource result = lazy.get();
        if (result != null && initialized != null) {
            initialized.set(true);
        }
        return result;
    }

    public boolean isInitialized() {
        return initialized != null && initialized.get();
    }

    @Override
    public long getWarehouseId() {
        return warehouseId;
    }

    @Override
    public long getWorkerGroupId() {
        return get().getWorkerGroupId();
    }

    @Override
    public String getCnGroupName() {
        return cnGroupName;
    }

    @Override
    public String toString() {
        return "{warehouseId=" + warehouseId +
                ", cnGroupName=" + cnGroupName +
                ", computeResource=" + (isInitialized() ? get().toString() : "not initialized") +
                "}";
    }
}
