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

package com.starrocks.statistic;

import com.starrocks.sql.optimizer.statistics.JoinStatisticsData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class JoinStatisticsCacheTest {
    static JoinStatisticsDefinition definition() {
        return new JoinStatisticsDefinition("test", List.of(
                new JoinStatisticsDefinition.Source("iceberg", "db", "a", "a-uuid", List.of()),
                new JoinStatisticsDefinition.Source("iceberg", "db", "b", "b-uuid", List.of())),
                List.of(new JoinStatisticsDefinition.KeyDomain(Map.of(0, List.of("id"), 1, List.of("id")),
                        List.of("BIGINT"))), Map.of());
    }

    static JoinStatisticsData data(long id, long generation) {
        return new JoinStatisticsData(id, generation, List.of(
                new JoinStatisticsData.Source("a-uuid", 1, 0, List.of(), List.of(), List.of(), new long[0], Map.of()),
                new JoinStatisticsData.Source("b-uuid", 2, 0, List.of(), List.of(), List.of(), new long[0], Map.of())),
                List.of());
    }

    private static JoinStatisticsRegistry registry() {
        JoinStatisticsRegistry registry = new JoinStatisticsRegistry((meta, drop, apply) -> apply.run());
        registry.create(1, definition());
        registry.publish(registry.begin("test", 2), 1, 1, "checksum", 1);
        return registry;
    }

    @Test
    void metricsCountColdLookupOnceAndReportLiveBudget() {
        JoinStatisticsRegistry registry = registry();
        JoinStatisticsCache cache = new JoinStatisticsCache(registry,
                meta -> data(meta.getId(), meta.getGeneration()), Runnable::run, 1 << 20, 1);
        JoinStatisticsMeta meta = registry.get("test");
        cache.get(meta, true, 1000).orElseThrow();
        Assertions.assertEquals(1, cache.metrics().misses());
        Assertions.assertEquals(0, cache.metrics().hits());
        cache.get(meta, false, 0).orElseThrow();
        Assertions.assertEquals(1, cache.metrics().hits());
        Assertions.assertEquals(1, cache.metrics().misses());
        Assertions.assertEquals(1, cache.metrics().entries());
        Assertions.assertTrue(cache.metrics().estimatedBytes() > 0);
        cache.setMaximumBytes(2 << 20);
        Assertions.assertEquals(2 << 20, cache.metrics().maximumBytes());
        cache.invalidate(meta.getId());
        Assertions.assertEquals(0, cache.metrics().evictions(), "Explicit invalidation is not eviction");
        cache.get(meta, true, 1000).orElseThrow();
        Assertions.assertEquals(2, cache.metrics().misses());
    }

    @Test
    void aNewGenerationDoesNotInheritBackoff() {
        var registry = registry();
        AtomicInteger calls = new AtomicInteger();
        var cache = new JoinStatisticsCache(registry, meta -> {
            calls.incrementAndGet();
            if (meta.getGeneration() == 2) {
                throw new IOException("broken old generation");
            }
            return data(meta.getId(), meta.getGeneration());
        }, Runnable::run, 1 << 20, 1);
        Assertions.assertTrue(cache.get(registry.get("test"), true, 1000).isEmpty());
        registry.publish(registry.begin("test", 3), 1, 1, "checksum", 2);
        Assertions.assertTrue(cache.get(registry.get("test"), true, 1000).isPresent());
        Assertions.assertEquals(2, calls.get());
    }

    @Test
    void failureDoesNotBecomeCachedAbsence() {
        JoinStatisticsRegistry registry = registry();
        AtomicInteger loads = new AtomicInteger();
        var now = new java.util.concurrent.atomic.AtomicLong();
        JoinStatisticsCache cache = new JoinStatisticsCache(registry, meta -> {
            if (loads.incrementAndGet() == 1) {
                throw new IOException("temporary storage failure");
            }
            return data(meta.getId(), meta.getGeneration());
        }, Runnable::run, 1 << 20, 1, now::get);
        Assertions.assertTrue(cache.get(registry.get("test"), true, 1000).isEmpty());
        for (int i = 0; i < 20; i++) {
            Assertions.assertTrue(cache.get(registry.get("test"), true, 1000).isEmpty());
        }
        Assertions.assertEquals(1, loads.get());
        now.set(TimeUnit.SECONDS.toNanos(61));
        Assertions.assertTrue(cache.get(registry.get("test"), true, 1000).isPresent());
        Assertions.assertEquals(2, loads.get());
        Assertions.assertTrue(cache.get(registry.get("test"), true, 1000).isPresent());
        Assertions.assertEquals(2, loads.get());
    }

    @Test
    void lateLoadCannotResurrectDroppedOrReplacedObject() throws Exception {
        JoinStatisticsRegistry registry = registry();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        try {
            JoinStatisticsCache cache = new JoinStatisticsCache(registry, meta -> {
                entered.countDown();
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("test timed out");
                }
                return data(meta.getId(), meta.getGeneration());
            }, command -> executor.execute(() -> {
                try {
                    command.run();
                } finally {
                    finished.countDown();
                }
            }), 1 << 20, 1);
            JoinStatisticsMeta old = registry.get("test");
            Assertions.assertTrue(cache.get(old, false, 0).isEmpty());
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            registry.drop("test", false);
            cache.invalidate(1);
            registry.create(3, definition());
            release.countDown();
            Assertions.assertTrue(finished.await(5, TimeUnit.SECONDS));
            Assertions.assertTrue(cache.get(old, true, 1000).isEmpty());
            Assertions.assertTrue(cache.get(registry.get("test"), true, 1000).isEmpty());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void admissionAndEvictionDoNotLoseTheValueReturnedToThisPlanner() throws Exception {
        var registry = registry();
        var first = registry.get("test");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        try {
            JoinStatisticsCache cache = new JoinStatisticsCache(registry, meta -> {
                entered.countDown();
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("test timed out");
                }
                return data(meta.getId(), meta.getGeneration());
            }, executor, 1, 1);
            Assertions.assertTrue(cache.get(first, false, 0).isEmpty());
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            registry.publish(registry.begin("test", 3), 1, 1, "checksum", 2);
            // The old generation occupies the only load permit, but cannot install a negative entry for the new one.
            Assertions.assertTrue(cache.get(registry.get("test"), false, 0).isEmpty());
            release.countDown();
            executor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            JoinStatisticsData value = cache.get(registry.get("test"), true, 5000).orElseThrow();
            Assertions.assertEquals(3, value.getGeneration());
            cache.invalidate(1);
            Assertions.assertEquals(3, value.getGeneration(), "Eviction does not revoke an acquired immutable object");
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void refreshingGenerationKeepsOldReaderAndReplacesCacheKey() {
        JoinStatisticsRegistry registry = registry();
        JoinStatisticsCache cache = new JoinStatisticsCache(registry,
                meta -> data(meta.getId(), meta.getGeneration()), Runnable::run, 1 << 20, 1);
        JoinStatisticsMeta oldMeta = registry.get("test");
        JoinStatisticsData old = cache.get(oldMeta, true, 1000).orElseThrow();
        registry.publish(registry.begin("test", 3), 1, 1, "checksum", 2);
        Assertions.assertTrue(cache.get(oldMeta, true, 1000).isEmpty());
        Assertions.assertEquals(3, cache.get(registry.get("test"), true, 1000).orElseThrow().getGeneration());
        Assertions.assertEquals(2, old.getGeneration());
    }

    @Test
    void resizingPreservesWarmEntriesAndEvictsToTheNewBudget() {
        JoinStatisticsRegistry registry = registry();
        AtomicInteger loads = new AtomicInteger();
        JoinStatisticsCache cache = new JoinStatisticsCache(registry, meta -> {
            loads.incrementAndGet();
            return data(meta.getId(), meta.getGeneration());
        }, Runnable::run, 1 << 20, 1);
        JoinStatisticsMeta meta = registry.get("test");
        JoinStatisticsData value = cache.get(meta, true, 1000).orElseThrow();
        cache.setMaximumBytes(2 << 20);
        Assertions.assertSame(value, cache.get(meta, true, 1000).orElseThrow());
        Assertions.assertEquals(1, loads.get());
        Assertions.assertThrows(IllegalArgumentException.class, () -> cache.setMaximumBytes(-1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> cache.setMaximumBytes(0));
        cache.setMaximumBytes(1);
        Assertions.assertEquals(0, cache.entryCount());
        Assertions.assertEquals(0, cache.estimatedSize());
        // Eviction does not remove the durable registry entry or prevent loading for a caller.
        Assertions.assertSame(meta, registry.get("test"));
        Assertions.assertTrue(cache.get(meta, true, 1000).isPresent());
        Assertions.assertEquals(2, loads.get());
    }
}
