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
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatisticsCacheMetricsTest {
    @Test
    void snapshotIsPassiveAndReflectsEvictionAndResizing() {
        Cache<String, String> cache = Caffeine.newBuilder().maximumWeight(12)
                .weigher((String key, String value) -> 8).executor(Runnable::run).recordStats().build();
        assertNull(cache.getIfPresent("a"));
        cache.put("a", "value");
        assertEquals("value", cache.getIfPresent("a"));
        var first = StatisticsCacheMetrics.snapshot(cache);
        assertEquals(1, first.hits());
        assertEquals(1, first.misses());
        assertEquals(8, first.estimatedBytes());
        assertEquals(12, first.maximumBytes());
        assertEquals(first, StatisticsCacheMetrics.snapshot(cache));
        assertEquals(1, cache.stats().hitCount(), "Scraping must not count a cache lookup");
        cache.put("b", "value");
        cache.cleanUp();
        assertEquals(1, StatisticsCacheMetrics.snapshot(cache).evictions());
        assertEquals(1, StatisticsCacheMetrics.snapshot(cache).entries());
        cache.policy().eviction().orElseThrow().setMaximum(24);
        assertEquals(24, StatisticsCacheMetrics.snapshot(cache).maximumBytes());
        cache.invalidateAll();
        cache.cleanUp();
        assertEquals(1, StatisticsCacheMetrics.snapshot(cache).evictions());
        assertEquals(0, StatisticsCacheMetrics.snapshot(cache).estimatedBytes());
    }

    @Test
    void prometheusExportsCountersAndLiveGaugesWithBoundedLabels() {
        PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("starrocks_fe");
        StatisticsCacheMetrics.visit(visitor, Map.of(
                "external_basic", new StatisticsCacheMetrics(7, 3, 2, 4, 512, 1024),
                "external_mcv", new StatisticsCacheMetrics(8, 2, 1, 1, 100, 200),
                "join", new StatisticsCacheMetrics(9, 1, 0, 1, 200, 400)));
        String text = visitor.build();
        assertTrue(text.contains("# TYPE starrocks_fe_statistics_cache_requests_total counter"));
        assertTrue(text.contains("statistics_cache_requests_total{cache=\"external_basic\", result=\"hit\"} 7"));
        assertTrue(text.contains("statistics_cache_requests_total{cache=\"external_basic\", result=\"miss\"} 3"));
        assertTrue(text.contains("statistics_cache_estimated_bytes{cache=\"join\"} 200"));
        assertTrue(text.contains("statistics_cache_max_bytes{cache=\"external_mcv\"} 200"));
        assertEquals(1, text.lines().filter(l -> l.startsWith(
                "# HELP starrocks_fe_statistics_cache_requests_total ")).count());
    }
}
