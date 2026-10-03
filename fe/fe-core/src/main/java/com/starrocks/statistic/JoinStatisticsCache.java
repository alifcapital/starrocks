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

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.starrocks.metric.StatisticsCacheMetrics;
import com.starrocks.sql.optimizer.statistics.JoinStatisticsData;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/** Prepared immutable generations. Failed and superseded loads are never cached as absent statistics. */
public final class JoinStatisticsCache {
    @FunctionalInterface
    public interface Loader {
        JoinStatisticsData load(JoinStatisticsMeta meta) throws Exception;
    }

    private record Key(long objectId, long generation) {
    }

    private final JoinStatisticsRegistry registry;
    private final Loader loader;
    private final Semaphore permits;
    private final AsyncCache<Key, JoinStatisticsData> cache;
    private final Cache<Key, Boolean> failures;
    // Count one logical lookup; Caffeine sees both getIfPresent and get on a cold load.
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();

    public JoinStatisticsCache(JoinStatisticsRegistry registry, Loader loader, Executor executor,
                               long maximumBytes, int concurrentLoads) {
        this(registry, loader, executor, maximumBytes, concurrentLoads, Ticker.systemTicker());
    }

    JoinStatisticsCache(JoinStatisticsRegistry registry, Loader loader, Executor executor,
                        long maximumBytes, int concurrentLoads, Ticker ticker) {
        if (maximumBytes <= 0 || concurrentLoads <= 0) {
            throw new IllegalArgumentException("Invalid JOIN statistics cache limits");
        }
        this.failures = Caffeine.newBuilder().maximumSize(4096).ticker(ticker)
                .expireAfterWrite(60, TimeUnit.SECONDS).build();
        this.registry = registry;
        this.loader = loader;
        this.permits = new Semaphore(concurrentLoads);
        this.cache = Caffeine.newBuilder().maximumWeight(maximumBytes).recordStats()
                .weigher((Key key, JoinStatisticsData value) ->
                        (int) Math.min(Integer.MAX_VALUE, 192L + value.estimatedSize()))
                .executor(executor).buildAsync();
    }

    public Optional<JoinStatisticsData> get(JoinStatisticsMeta meta, boolean sync, long timeoutMillis) {
        if (meta.getGeneration() == 0 || !isCurrent(meta)) {
            return Optional.empty();
        }
        Key key = new Key(meta.getId(), meta.getGeneration());
        try {
            CompletableFuture<JoinStatisticsData> future = cache.getIfPresent(key);
            if (future == null) {
                misses.increment();
                if (failures.getIfPresent(key) != null) {
                    return Optional.empty();
                }
                if (!permits.tryAcquire()) {
                    return Optional.empty();
                }
                AtomicBoolean submitted = new AtomicBoolean();
                try {
                    future = cache.get(key, (ignored, executor) -> {
                        submitted.set(true);
                        return load(meta, executor);
                    });
                } finally {
                    // Another caller may have installed this key after our lookup.
                    if (!submitted.get()) {
                        permits.release();
                    }
                }
            } else {
                hits.increment();
            }
            JoinStatisticsData value = sync ? future.get(Math.max(0, timeoutMillis), TimeUnit.MILLISECONDS)
                    : future.getNow(null);
            if (!isCurrent(meta)) {
                cache.asMap().remove(key, future);
                return Optional.empty();
            }
            return Optional.ofNullable(value);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (ExecutionException | CompletionException | CancellationException | TimeoutException
                 | RejectedExecutionException e) {
            return Optional.empty();
        }
    }

    private CompletableFuture<JoinStatisticsData> load(JoinStatisticsMeta meta, Executor executor) {
        try {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    if (!isCurrent(meta)) {
                        throw new CancellationException("JOIN statistics generation was superseded");
                    }
                    JoinStatisticsData data = loader.load(meta);
                    if (data == null || data.getObjectId() != meta.getId() || data.getGeneration() != meta.getGeneration()) {
                        throw new IllegalStateException("JOIN statistics loader returned a different generation");
                    }
                    if (!isCurrent(meta)) {
                        throw new CancellationException("JOIN statistics generation was superseded");
                    }
                    failures.invalidate(new Key(meta.getId(), meta.getGeneration()));
                    return data;
                } catch (Exception e) {
                    if (!(e instanceof CancellationException) && !(e instanceof InterruptedException) && isCurrent(meta)) {
                        failures.put(new Key(meta.getId(), meta.getGeneration()), true);
                    }
                    throw new CompletionException(e);
                } finally {
                    permits.release();
                }
            }, executor);
        } catch (RuntimeException e) {
            permits.release();
            throw e;
        }
    }

    private boolean isCurrent(JoinStatisticsMeta meta) {
        JoinStatisticsMeta current = registry.get(meta.getDefinition().getName());
        return current != null && current.getId() == meta.getId() && current.getGeneration() == meta.getGeneration();
    }

    public void invalidate(long objectId) {
        cache.asMap().keySet().removeIf(key -> key.objectId == objectId);
        failures.asMap().keySet().removeIf(key -> key.objectId == objectId);
    }

    public void put(JoinStatisticsMeta meta, JoinStatisticsData data) {
        if (!isCurrent(meta) || data.getObjectId() != meta.getId() || data.getGeneration() != meta.getGeneration()) {
            return;
        }
        Key key = new Key(meta.getId(), meta.getGeneration());
        CompletableFuture<JoinStatisticsData> value = CompletableFuture.completedFuture(data);
        cache.put(key, value);
        failures.invalidate(key);
        if (!isCurrent(meta)) {
            cache.asMap().remove(key, value);
        }
    }

    public void setMaximumBytes(long maximumBytes) {
        if (maximumBytes <= 0) {
            throw new IllegalArgumentException("JOIN statistics cache budget must be positive");
        }
        cache.synchronous().policy().eviction().ifPresent(eviction -> {
            if (eviction.getMaximum() != maximumBytes) {
                eviction.setMaximum(maximumBytes);
            }
        });
    }

    public StatisticsCacheMetrics metrics() {
        var snapshot = StatisticsCacheMetrics.snapshot(cache.synchronous());
        return new StatisticsCacheMetrics(hits.sum(), misses.sum(), snapshot.evictions(),
                snapshot.entries(), snapshot.estimatedBytes(), snapshot.maximumBytes());
    }

    public long estimatedSize() {
        return cache.synchronous().policy().eviction().map(policy -> policy.weightedSize().orElse(0)).orElse(0L);
    }

    public long entryCount() {
        return cache.synchronous().estimatedSize();
    }
}
