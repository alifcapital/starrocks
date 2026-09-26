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

package com.starrocks.sql.optimizer.statistics;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CachedConnectorTableStatisticTest {
    @Test
    @Timeout(5)
    void quietLookupDoesNotLoadRefreshOrWaitAndNeverUsesPartitionNdv() {
        CachedStatisticStorage storage = new CachedStatisticStorage();
        AtomicLong clock = new AtomicLong();
        AtomicInteger loads = new AtomicInteger();
        storage.externalStatisticsCache = Caffeine.newBuilder().executor(Runnable::run).ticker(clock::get)
                .refreshAfterWrite(1, TimeUnit.SECONDS).expireAfterWrite(10, TimeUnit.SECONDS)
                .buildAsync((key, executor) -> {
                    loads.incrementAndGet();
                    return CompletableFuture.completedFuture(Optional.empty());
                });
        Table table = mock(Table.class);
        when(table.getUUID()).thenReturn("ice.db.sales");
        var key = ExternalStatisticsCacheKey.table(table.getUUID(), "region");
        var partition = new ExternalStatisticsCacheKey(table.getUUID(), "p", "region");
        var stat = ColumnStatistic.builder().setDistinctValuesCount(3).build();
        var raw = new ConnectorTableColumnStats(stat, 100, "2026-09-28 00:00:00");
        var value = Optional.<ExternalColumnStatistics>of(new ExternalColumnStatistics.Summary(raw, raw, "ICEBERG"));
        boolean oldSync = Config.enable_sync_statistics_load;
        Config.enable_sync_statistics_load = true;
        try {
            assertTrue(storage.getCachedConnectorTableColumnStatistic(table, "region").isUnknown());
            assertTrue(storage.externalStatisticsCache.asMap().isEmpty());
            storage.externalStatisticsCache.put(partition, CompletableFuture.completedFuture(value));
            assertTrue(storage.getCachedConnectorTableColumnStatistic(table, "region").isUnknown());
            CompletableFuture<Optional<ExternalColumnStatistics>> pending = new CompletableFuture<>();
            storage.externalStatisticsCache.put(key, pending);
            assertTrue(storage.getCachedConnectorTableColumnStatistic(table, "region").isUnknown());
            assertFalse(pending.isDone());
            pending.complete(value);
            assertSame(stat, storage.getCachedConnectorTableColumnStatistic(table, "region"));
            clock.set(TimeUnit.SECONDS.toNanos(2));
            assertSame(stat, storage.getCachedConnectorTableColumnStatistic(table, "region"));
            clock.set(TimeUnit.SECONDS.toNanos(20));
            assertTrue(storage.getCachedConnectorTableColumnStatistic(table, "region").isUnknown());
            assertEquals(0, loads.get(), "No miss load or refresh, even when sync loading is enabled");
            storage.externalStatisticsCache.put(key, CompletableFuture.completedFuture(Optional.empty()));
            assertTrue(storage.getCachedConnectorTableColumnStatistic(table, "region").isUnknown());
        } finally {
            Config.enable_sync_statistics_load = oldSync;
        }
    }
}
