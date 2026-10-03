// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.statistics;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class ExternalMcvCacheWeightTest {
    private static ExternalMcvStatistics statistics(String value) {
        return new ExternalMcvStatistics(List.of(new ExternalMcvStatistics.Group(List.of("k"), 10, 1,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of(value), 10, List.of(10L))),
                List.of(), List.of(0L))));
    }

    @Test
    public void testMcvCacheRecordsHitAndMiss() {
        var cache = CachedStatisticStorage.createExternalMcvStatisticsCache(1 << 20, Runnable::run,
                (key, executor) -> CompletableFuture.completedFuture(Optional.of(statistics("x"))));
        cache.get("table").join();
        cache.get("table").join();
        var metrics = com.starrocks.metric.StatisticsCacheMetrics.snapshot(cache.synchronous());
        Assertions.assertEquals(1, metrics.misses());
        Assertions.assertEquals(1, metrics.hits());
        Assertions.assertEquals(1, metrics.entries());
        Assertions.assertTrue(metrics.estimatedBytes() > 0);
    }

    @Test
    public void testByteEvictionAndOversizedLoad() {
        ExternalMcvStatistics small = statistics("x");
        ExternalMcvStatistics large = statistics("ж".repeat(10000));
        int weight = CachedStatisticStorage.externalMcvCacheWeight("a", Optional.of(small));
        AsyncLoadingCache<String, Optional<ExternalMcvStatistics>> cache =
                CachedStatisticStorage.createExternalMcvStatisticsCache(2L * weight, Runnable::run,
                        (key, executor) -> CompletableFuture.completedFuture(Optional.of(large)));
        for (String key : List.of("a", "b", "c")) {
            cache.synchronous().put(key, Optional.of(small));
        }
        cache.synchronous().cleanUp();
        Assertions.assertEquals(2, cache.synchronous().estimatedSize());
        Assertions.assertTrue(cache.synchronous().policy().eviction().orElseThrow().isWeighted());
        // A caller can use a loaded oversized value even though it cannot remain resident.
        Assertions.assertSame(large, cache.get("large").join().orElseThrow());
        cache.synchronous().cleanUp();
        Assertions.assertNull(cache.getIfPresent("large"));
        Assertions.assertTrue(cache.synchronous().policy().eviction().orElseThrow().weightedSize().orElseThrow()
                <= 2L * weight);
    }

    @Test
    public void testStablePreparedWeightAndNonzeroNegativeEntry() {
        ExternalMcvStatistics statistics = statistics("строка");
        long before = statistics.retainedBytes();
        statistics.getGroups().get(0).prepare(VarcharType.VARCHAR);
        Assertions.assertEquals(before, statistics.retainedBytes());
        Assertions.assertTrue(CachedStatisticStorage.externalMcvCacheWeight("table", Optional.empty()) > 0);
        Assertions.assertTrue(statistics("ж".repeat(10000)).retainedBytes() > before + 19000);
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                CachedStatisticStorage.createExternalMcvStatisticsCache(0, Runnable::run,
                        (key, executor) -> CompletableFuture.completedFuture(Optional.empty())));
    }
}
