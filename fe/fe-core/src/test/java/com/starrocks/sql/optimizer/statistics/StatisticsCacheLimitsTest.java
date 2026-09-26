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

import com.starrocks.common.Config;
import com.starrocks.common.ConfigRefreshDaemon;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

class StatisticsCacheLimitsTest {
    @Test
    void basicCacheMetricsIncludeBothTableAndPartitionKeys() {
        CachedStatisticStorage storage = new CachedStatisticStorage();
        var cache = storage.externalStatisticsCache;
        var table = ExternalStatisticsCacheKey.table("table", "column");
        var partition = new ExternalStatisticsCacheKey("table", "p", "column");
        Assertions.assertNull(cache.getIfPresent(table));
        Assertions.assertNull(cache.getIfPresent(partition));
        cache.put(table, CompletableFuture.completedFuture(Optional.empty()));
        cache.put(partition, CompletableFuture.completedFuture(Optional.empty()));
        Assertions.assertNotNull(cache.getIfPresent(table));
        Assertions.assertNotNull(cache.getIfPresent(partition));
        cache.synchronous().cleanUp();
        var metrics = storage.getCacheMetrics().get("external_basic");
        Assertions.assertEquals(2, metrics.hits());
        Assertions.assertEquals(2, metrics.misses());
        Assertions.assertEquals(2, metrics.entries());
        Assertions.assertTrue(metrics.estimatedBytes() > 0);
        Assertions.assertEquals(Config.external_statistics_cache_max_bytes, metrics.maximumBytes());
        Assertions.assertEquals(metrics, storage.getCacheMetrics().get("external_basic"));
    }

    @Test
    void runningCachesResizeThroughTheConfigDaemonWithoutReplacingWarmEntries() throws Exception {
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        StatisticStorage original = state.getStatisticStorage();
        long oldExternal = Config.external_statistics_cache_max_bytes;
        long oldMcv = Config.statistic_mcv_cache_max_bytes;
        CachedStatisticStorage storage = new CachedStatisticStorage();
        var external = storage.externalStatisticsCache;
        var mcv = storage.externalMcvStats;
        ExternalMcvStatistics value = new ExternalMcvStatistics(List.of(new ExternalMcvStatistics.Group(
                List.of("k"), 10, 1, List.of(new MultiColumnCombinedStats.McvEntry(List.of("x"), 10)),
                List.of(), List.of(0L))));
        ExternalStatisticsCacheKey key = ExternalStatisticsCacheKey.table("table", "k");
        try {
            state.setStatisticStorage(storage);
            external.put(key, CompletableFuture.completedFuture(Optional.empty()));
            mcv.put("table", CompletableFuture.completedFuture(Optional.of(value)));
            external.synchronous().cleanUp();
            mcv.synchronous().cleanUp();
            Config.external_statistics_cache_max_bytes = oldExternal * 2;
            Config.statistic_mcv_cache_max_bytes = oldMcv * 2;
            refresh(state);
            Assertions.assertEquals(oldExternal * 2, external.synchronous().policy().eviction().orElseThrow().getMaximum());
            Assertions.assertEquals(oldMcv * 2, mcv.synchronous().policy().eviction().orElseThrow().getMaximum());
            Assertions.assertSame(value, mcv.getIfPresent("table").join().orElseThrow());
            Assertions.assertNotNull(external.getIfPresent(key));
            // A load started before shrinking must also be subject to the new budget on completion.
            CompletableFuture<Optional<ExternalMcvStatistics>> pending = new CompletableFuture<>();
            mcv.put("pending", pending);
            Config.external_statistics_cache_max_bytes = 0;
            Config.statistic_mcv_cache_max_bytes = 1;
            refresh(state);
            pending.complete(Optional.of(value));
            external.synchronous().cleanUp();
            mcv.synchronous().cleanUp();
            Assertions.assertEquals(0, external.synchronous().estimatedSize());
            Assertions.assertEquals(0, mcv.synchronous().estimatedSize());
            Assertions.assertSame(external, storage.externalStatisticsCache);
            Assertions.assertSame(mcv, storage.externalMcvStats);
            Assertions.assertSame(value, pending.join().orElseThrow());
        } finally {
            Config.external_statistics_cache_max_bytes = oldExternal;
            Config.statistic_mcv_cache_max_bytes = oldMcv;
            state.setStatisticStorage(original);
            refresh(state);
        }
    }

    private static void refresh(GlobalStateMgr state) throws Exception {
        Method tick = ConfigRefreshDaemon.class.getDeclaredMethod("runAfterCatalogReady");
        tick.setAccessible(true);
        tick.invoke(state.getConfigRefreshDaemon());
    }
}
