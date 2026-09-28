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

import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

class JoinStatisticsManagerTest {
    @Test
    void createRespectsImmediateFlagButExplicitAnalyzeAlwaysStarts() throws Exception {
        UtFrameUtils.createDefaultCtx();
        AtomicLong ids = new AtomicLong(1000);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public long getNextId() {
                return ids.incrementAndGet();
            }

            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        // Stop at the storage readiness check: exercise real scheduling and callbacks without BE SQL.
        new MockUp<StatisticUtils>() {
            @Mock
            public ConnectContext buildConnectContext() {
                return UtFrameUtils.createDefaultCtx();
            }

            @Mock
            public boolean checkStatisticTables(List<String> tables) {
                return false;
            }
        };
        JoinStatisticsRegistry registry = new JoinStatisticsRegistry((meta, drop, apply) -> apply.run());
        JoinStatisticsManager manager = new JoinStatisticsManager(registry);
        boolean previous = Config.enable_trigger_analyze_job_immediate;
        try {
            Config.enable_trigger_analyze_job_immediate = false;
            AtomicLong started = new AtomicLong();
            for (boolean asynchronous : List.of(false, true)) {
                String name = asynchronous ? "deferred_async" : "deferred_sync";
                manager.create(definition(name), asynchronous, started::set);
                Assertions.assertNotNull(registry.get(name));
                Assertions.assertEquals(0, started.get());
                Assertions.assertEquals("EMPTY", manager.status(registry.get(name)).state());
            }
            Assertions.assertThrows(DdlException.class,
                    () -> manager.analyze("deferred_sync", false, started::set));
            Assertions.assertTrue(started.get() > 0, "Explicit ANALYZE must start despite the disabled CREATE trigger");
            Assertions.assertEquals("FAILED", manager.status(registry.get("deferred_sync")).state());

            Config.enable_trigger_analyze_job_immediate = true;
            started.set(0);
            Assertions.assertThrows(DdlException.class,
                    () -> manager.create(definition("immediate"), false, started::set));
            Assertions.assertTrue(started.get() > 0);
            Assertions.assertNotNull(registry.get("immediate"), "Failed collection must leave the definition for retry");
        } finally {
            Config.enable_trigger_analyze_job_immediate = previous;
        }
    }

    private static JoinStatisticsDefinition definition(String name) {
        return new JoinStatisticsDefinition(name, List.of(
                new JoinStatisticsDefinition.Source("iceberg", "db", "t", "t-uuid", List.of("status")),
                new JoinStatisticsDefinition.Source("iceberg", "db", "u", "u-uuid", List.of("country"))),
                List.of(new JoinStatisticsDefinition.KeyDomain(Map.of(0, List.of("user_id"), 1, List.of("id")),
                        List.of("BIGINT"))), Map.of());
    }

    @Test
    void runningJoinCacheUsesTheMutableBudget() throws Exception {
        UtFrameUtils.createDefaultCtx();
        Config config = new Config();
        config.init(java.nio.file.Paths.get(getClass().getClassLoader()
                .getResource("conf/config_test.properties").toURI()).toString());
        long old = Config.statistic_join_cache_max_bytes;
        JoinStatisticsRegistry registry = new JoinStatisticsRegistry((meta, drop, apply) -> apply.run());
        registry.create(1, JoinStatisticsCacheTest.definition());
        registry.publish(registry.begin("test", 2), 1, 1, "checksum", 1);
        JoinStatisticsManager manager = new JoinStatisticsManager(registry);
        java.lang.reflect.Field field = JoinStatisticsManager.class.getDeclaredField("cache");
        field.setAccessible(true);
        JoinStatisticsCache cache = (JoinStatisticsCache) field.get(manager);
        JoinStatisticsMeta meta = registry.get("test");
        cache.put(meta, JoinStatisticsCacheTest.data(meta.getId(), meta.getGeneration()));
        java.lang.reflect.Method tick = com.starrocks.common.ConfigRefreshDaemon.class
                .getDeclaredMethod("runAfterCatalogReady");
        tick.setAccessible(true);
        try {
            for (String invalid : List.of("-1", "0", "9223372036854775808")) {
                Assertions.assertThrows(com.starrocks.common.InvalidConfException.class,
                        () -> Config.setMutableConfig("statistic_join_cache_max_bytes", invalid, false, "root"));
                Assertions.assertEquals(old, Config.statistic_join_cache_max_bytes);
            }
            Config.setMutableConfig("statistic_join_cache_max_bytes", "1", false, "root");
            tick.invoke(GlobalStateMgr.getCurrentState().getConfigRefreshDaemon());
            Assertions.assertEquals(0, cache.entryCount());
            Assertions.assertNotNull(registry.get("test"));
        } finally {
            Config.statistic_join_cache_max_bytes = old;
            tick.invoke(GlobalStateMgr.getCurrentState().getConfigRefreshDaemon());
        }
    }
}
