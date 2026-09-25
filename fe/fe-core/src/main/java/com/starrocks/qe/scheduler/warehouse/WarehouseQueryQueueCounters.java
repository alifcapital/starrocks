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

package com.starrocks.qe.scheduler.warehouse;

import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricLabel;
import com.starrocks.metric.MetricRepo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class WarehouseQueryQueueCounters {
    private static final Map<Long, WarehouseQueryQueueCounters> COUNTERS = new ConcurrentHashMap<>();

    private final LongCounterMetric total;
    private final LongCounterMetric timeout;

    private WarehouseQueryQueueCounters(long warehouseId, String warehouseName) {
        total = createCounter("warehouse_query_queue_total", "Queries submitted to the queue on this FE",
                warehouseId, warehouseName);
        timeout = createCounter("warehouse_query_queue_timeout", "Queries timing out in the queue on this FE",
                warehouseId, warehouseName);
    }

    private static LongCounterMetric createCounter(String name, String description, long id, String warehouseName) {
        LongCounterMetric metric = new LongCounterMetric(name, Metric.MetricUnit.REQUESTS, description);
        metric.addLabel(new MetricLabel("warehouse_id", String.valueOf(id)));
        metric.addLabel(new MetricLabel("warehouse_name", warehouseName));
        MetricRepo.addMetric(metric);
        return metric;
    }

    public static WarehouseQueryQueueCounters get(long warehouseId, String warehouseName) {
        return COUNTERS.computeIfAbsent(warehouseId, id -> new WarehouseQueryQueueCounters(id, warehouseName));
    }

    public void increaseTotal() {
        total.increase(1L);
    }

    public void increaseTimeout() {
        timeout.increase(1L);
    }
}
