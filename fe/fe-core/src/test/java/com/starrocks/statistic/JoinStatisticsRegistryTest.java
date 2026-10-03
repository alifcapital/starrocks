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

import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class JoinStatisticsRegistryTest {
    private static JoinStatisticsDefinition definition() {
        return new JoinStatisticsDefinition("Tx_Users", List.of(
                new JoinStatisticsDefinition.Source("iceberg", "db", "transactions", "tx-uuid", List.of("status")),
                new JoinStatisticsDefinition.Source("iceberg", "db", "users", "users-uuid", List.of("country"))),
                List.of(new JoinStatisticsDefinition.KeyDomain(Map.of(0, List.of("user_id"), 1, List.of("id")),
                        List.of("BIGINT"))), Map.of());
    }

    @Test
    void journalFailuresDoNotExposePartialChanges() {
        AtomicBoolean fail = new AtomicBoolean();
        JoinStatisticsRegistry registry = new JoinStatisticsRegistry((meta, drop, apply) -> {
            if (fail.get()) {
                throw new IllegalStateException("journal failed");
            }
            apply.run();
        });
        registry.create(1, definition());
        JoinStatisticsMeta original = registry.get("TX_USERS");
        JoinStatisticsRegistry.Collection collection = registry.begin("tx_users", 2);
        fail.set(true);
        Assertions.assertThrows(IllegalStateException.class, () -> registry.publish(collection, 1, 100, "digest", 1));
        Assertions.assertSame(original, registry.get("tx_users"));
        Assertions.assertThrows(IllegalStateException.class, () -> registry.drop("tx_users", false));
        Assertions.assertSame(original, registry.get("tx_users"));
        fail.set(false);
        Assertions.assertTrue(registry.publish(collection, 1, 100, "digest", 1));
        Assertions.assertEquals(2, registry.get("tx_users").getGeneration());
        // A planner retaining an older immutable metadata object continues to see its own generation.
        Assertions.assertEquals(0, original.getGeneration());
    }

    @Test
    void dropAndRecreateCannotBeResurrectedByDelayedCollection() throws Exception {
        JoinStatisticsRegistry registry = new JoinStatisticsRegistry((meta, drop, apply) -> apply.run());
        registry.create(1, definition());
        JoinStatisticsRegistry.Collection collection = registry.begin("tx_users", 2);
        CountDownLatch collecting = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> result = executor.submit(() -> {
                collecting.countDown();
                Assertions.assertTrue(finish.await(5, TimeUnit.SECONDS));
                return registry.publish(collection, 1, 100, "digest", 1);
            });
            Assertions.assertTrue(collecting.await(5, TimeUnit.SECONDS));
            JoinStatisticsMeta dropped = registry.drop("tx_users", false);
            registry.create(3, definition());
            finish.countDown();
            Assertions.assertFalse(result.get(5, TimeUnit.SECONDS));
            Assertions.assertTrue(collection.isCancelled());
            registry.replay(dropped, true);
            Assertions.assertEquals(3, registry.get("tx_users").getId());
            Assertions.assertEquals(0, registry.get("tx_users").getGeneration());
        } finally {
            finish.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void failedRefreshAndLeaderChangePreservePublishedGeneration() {
        JoinStatisticsRegistry registry = new JoinStatisticsRegistry((meta, drop, apply) -> apply.run());
        registry.create(1, definition());
        Assertions.assertTrue(registry.publish(registry.begin("tx_users", 2), 1, 100, "digest", 1));
        JoinStatisticsRegistry.Collection failed = registry.begin("tx_users", 3);
        Assertions.assertThrows(IllegalStateException.class, () -> registry.begin("tx_users", 4));
        registry.finish(failed);
        Assertions.assertEquals(2, registry.get("tx_users").getGeneration());
        JoinStatisticsRegistry.Collection oldLeader = registry.begin("tx_users", 4);
        registry.revokeCollections();
        Assertions.assertFalse(registry.publish(oldLeader, 1, 100, "digest", 2));
        Assertions.assertEquals(2, registry.get("tx_users").getGeneration());
    }

    @Test
    void metadataRoundTripPreservesIdentityAndLocalColumnOrder() {
        JoinStatisticsRegistry registry = new JoinStatisticsRegistry((meta, drop, apply) -> apply.run());
        registry.create(10, definition());
        JoinStatisticsMeta saved = registry.get("tx_users");
        JoinStatisticsMeta restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(saved), JoinStatisticsMeta.class);
        JoinStatisticsRegistry follower = new JoinStatisticsRegistry((meta, drop, apply) -> {
            Assertions.fail("Replay must not journal metadata");
        });
        follower.replay(restored, false);
        Assertions.assertEquals(saved.getId(), follower.get("tx_users").getId());
        Assertions.assertEquals(List.of("user_id"),
                follower.get("tx_users").getDefinition().getDomains().get(0).getColumns().get(0));
        Assertions.assertEquals("users-uuid", follower.get("tx_users").getDefinition().getSources().get(1).getUuid());
    }

    @Test
    void lateCancellationCannotCancelARefreshThatStartedAfterPublication() {
        JoinStatisticsRegistry registry = new JoinStatisticsRegistry((meta, drop, apply) -> apply.run());
        registry.create(1, definition());
        var completed = registry.begin("tx_users", 2);
        Assertions.assertTrue(registry.publish(completed, 1, 100, "digest", 1));
        var next = registry.begin("tx_users", 3);
        registry.cancel(completed);
        registry.finish(completed);
        Assertions.assertFalse(next.isCancelled());
        Assertions.assertTrue(registry.isCollecting(1));
        registry.cancel(next);
        Assertions.assertTrue(next.isCancelled());
        Assertions.assertFalse(registry.publish(next, 1, 100, "digest", 2));
        Assertions.assertEquals(2, registry.get("tx_users").getGeneration());
    }

    @Test
    void analyzeManagerImageRestoresPublishedManifestButNotAnUnfinishedRefresh() throws Exception {
        AnalyzeMgr original = new AnalyzeMgr();
        var published = new JoinStatisticsMeta(10, definition(), 11, 2, 200000, "checksum", 1234);
        original.getJoinStatisticsRegistry().replay(published, false);
        original.getJoinStatisticsRegistry().begin("tx_users", 12);
        var image = new UtFrameUtils.PseudoImage();
        original.save(image.getImageWriter());
        AnalyzeMgr restored = new AnalyzeMgr();
        var reader = image.getMetaBlockReader();
        try {
            restored.load(reader);
        } finally {
            reader.close();
        }
        var meta = restored.getJoinStatisticsRegistry().get("tx_users");
        Assertions.assertEquals(GsonUtils.GSON.toJson(published), GsonUtils.GSON.toJson(meta));
        Assertions.assertFalse(restored.getJoinStatisticsRegistry().isCollecting(10));
        Assertions.assertDoesNotThrow(() -> restored.getJoinStatisticsRegistry().begin("tx_users", 13));
    }
}
