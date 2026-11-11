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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatsCacheLoadsTest {
    // One thread and no queue: a load is rejected while the thread is busy.
    private final ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadPoolExecutor.AbortPolicy());
    private final AtomicInteger loads = new AtomicInteger();
    private final AtomicLong nanos = new AtomicLong();
    private CountDownLatch release = new CountDownLatch(0);

    // The cache runs its own work in the calling thread, so only the loads use the pool.
    private final AsyncLoadingCache<String, String> cache = Caffeine.newBuilder()
            .executor(Runnable::run)
            .ticker(nanos::get)
            .refreshAfterWrite(1, TimeUnit.MINUTES)
            .buildAsync((key, ignored) -> StatsCacheLoads.supplyAsync(
                    () -> key + "-" + loads.incrementAndGet(), pool));

    @AfterEach
    void after() {
        release.countDown();
        pool.shutdownNow();
    }

    private void occupyPool() throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        release = new CountDownLatch(1);
        CountDownLatch latch = release;
        pool.execute(() -> {
            started.countDown();
            try {
                latch.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(started.await(10, TimeUnit.SECONDS));
    }

    private void freePool() throws InterruptedException {
        release.countDown();
        for (int i = 0; i < 1000 && pool.getActiveCount() > 0; i++) {
            Thread.sleep(10);
        }
        assertEquals(0, pool.getActiveCount());
    }

    @Test
    void rejectedLoadIsDroppedAndLoadedAgain() throws Exception {
        occupyPool();
        CompletableFuture<String> rejected = cache.get("k");
        assertTrue(rejected.isCompletedExceptionally());
        assertNull(cache.getIfPresent("k"));

        freePool();
        assertEquals("k-1", cache.get("k").get(10, TimeUnit.SECONDS));
    }

    @Test
    void rejectedRefreshKeepsTheValueAndRefreshesLater() throws Exception {
        assertEquals("k-1", cache.get("k").get(10, TimeUnit.SECONDS));

        occupyPool();
        nanos.addAndGet(TimeUnit.MINUTES.toNanos(2));
        assertEquals("k-1", cache.get("k").get(10, TimeUnit.SECONDS));
        assertEquals(1, loads.get());

        freePool();
        cache.get("k");
        for (int i = 0; i < 1000 && !"k-2".equals(cache.get("k").getNow(null)); i++) {
            Thread.sleep(10);
        }
        assertEquals("k-2", cache.get("k").getNow(null));
    }
}
