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

package com.starrocks.metric;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.Map;

/** Constant-time snapshots; collecting metrics never traverses or refreshes cached statistics. */
public record StatisticsCacheMetrics(long hits, long misses, long evictions, long entries,
                                     long estimatedBytes, long maximumBytes) {
    public static StatisticsCacheMetrics snapshot(Cache<?, ?> cache) {
        CacheStats stats = cache.stats();
        var eviction = cache.policy().eviction();
        return new StatisticsCacheMetrics(stats.hitCount(), stats.missCount(), stats.evictionCount(),
                cache.estimatedSize(), eviction.map(e -> e.weightedSize().orElse(0)).orElse(0L),
                eviction.map(Policy.Eviction::getMaximum).orElse(0L));
    }

    public static void visit(MetricVisitor visitor, Map<String, StatisticsCacheMetrics> caches) {
        // Emit each metric family together for all fixed cache labels.
        for (var entry : caches.entrySet()) {
            counter(visitor, "statistics_cache_requests_total", entry.getKey(), "hit", entry.getValue().hits());
            counter(visitor, "statistics_cache_requests_total", entry.getKey(), "miss", entry.getValue().misses());
        }
        for (var entry : caches.entrySet()) {
            counter(visitor, "statistics_cache_evictions_total", entry.getKey(), null, entry.getValue().evictions());
        }
        for (var entry : caches.entrySet()) {
            gauge(visitor, "statistics_cache_entries", entry.getKey(), Metric.MetricUnit.NOUNIT,
                    entry.getValue().entries());
        }
        for (var entry : caches.entrySet()) {
            gauge(visitor, "statistics_cache_estimated_bytes", entry.getKey(), Metric.MetricUnit.BYTES,
                    entry.getValue().estimatedBytes());
        }
        for (var entry : caches.entrySet()) {
            gauge(visitor, "statistics_cache_max_bytes", entry.getKey(), Metric.MetricUnit.BYTES,
                    entry.getValue().maximumBytes());
        }
    }

    private static void counter(MetricVisitor visitor, String name, String cache, String result, long value) {
        LongCounterMetric metric = new LongCounterMetric(name, Metric.MetricUnit.NOUNIT,
                "Statistics cache cumulative events on this FE");
        metric.addLabel(new MetricLabel("cache", cache));
        if (result != null) {
            metric.addLabel(new MetricLabel("result", result));
        }
        metric.increase(value);
        visitor.visit(metric);
    }

    private static void gauge(MetricVisitor visitor, String name, String cache, Metric.MetricUnit unit, long value) {
        GaugeMetricImpl<Long> metric = new GaugeMetricImpl<>(name, unit, "Statistics cache capacity on this FE");
        metric.addLabel(new MetricLabel("cache", cache));
        metric.setValue(value);
        visitor.visit(metric);
    }
}
