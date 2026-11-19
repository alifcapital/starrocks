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

package com.starrocks.authorization.ranger;

import com.starrocks.authorization.ranger.starrocks.RangerStarRocksAccessController;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.PolicyRefresher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The controller of the default catalog loads its policies in a background thread. Without a policy engine the
 * plugin answers "no access", "no mask" and "no row filter", so nothing may read the plugin before the load is over.
 * The load is stubbed at the policy refresher, which is the part that talks to Ranger Admin.
 */
public class RangerBackgroundInitTest {
    private static final AtomicInteger REFRESHER_STARTS = new AtomicInteger();
    private static volatile CountDownLatch loadStarted;
    private static volatile CountDownLatch loadMayFinish;
    private static volatile RuntimeException loadFailure;

    @BeforeEach
    public void setUp() throws IllegalAccessException {
        // The audit factory is a JVM-wide singleton; start every test from a fresh one.
        FieldUtils.writeDeclaredStaticField(AuditProviderFactory.class, "sFactory", null, true);
        FieldUtils.writeDeclaredStaticField(RangerAccessController.class, "auditInitFailed", false, true);
        REFRESHER_STARTS.set(0);
        loadStarted = new CountDownLatch(1);
        loadMayFinish = new CountDownLatch(1);
        loadFailure = null;

        new MockUp<PolicyRefresher>() {
            @Mock
            void $init(RangerBasePlugin plugin) {
            }

            @Mock
            void startRefresher() {
                REFRESHER_STARTS.incrementAndGet();
                loadStarted.countDown();
                try {
                    loadMayFinish.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                if (loadFailure != null) {
                    throw loadFailure;
                }
            }
        };
    }

    @Test
    public void testUsersWaitForTheLoadAndTheConstructorDoesNot() throws Exception {
        RangerAccessController controller = RangerStarRocksAccessController.startingInBackground();

        // The constructor has returned and the load is still blocked, so the load runs in another thread.
        Assertions.assertTrue(loadStarted.await(10, TimeUnit.SECONDS));
        // The audit factory takes the config of the first plugin that sets it up, so the controller of the default
        // catalog sets it up before its constructor returns, not in the background.
        Assertions.assertTrue(AuditProviderFactory.getInstance().isInitDone());

        // dataVersions() is the first call of every decision: it must not pass before the load is over.
        CompletableFuture<RangerAccessController.DataVersions> versions =
                CompletableFuture.supplyAsync(controller::dataVersions);
        CompletableFuture<RangerBasePlugin> plugin = CompletableFuture.supplyAsync(controller::getRangerPlugin);
        Assertions.assertThrows(TimeoutException.class, () -> versions.get(300, TimeUnit.MILLISECONDS));
        Assertions.assertFalse(plugin.isDone());

        loadMayFinish.countDown();
        Assertions.assertNotNull(versions.get(10, TimeUnit.SECONDS));
        Assertions.assertNotNull(plugin.get(10, TimeUnit.SECONDS));
        Assertions.assertEquals(1, REFRESHER_STARTS.get());
    }

    @Test
    public void testLoadFailureReachesEveryUser() {
        loadFailure = new IllegalStateException("cannot load policies");
        RangerAccessController controller = RangerStarRocksAccessController.startingInBackground();
        loadMayFinish.countDown();

        // A failed load must never be taken for a plugin without policies, which would answer "no mask".
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, controller::awaitInitialized);
        Assertions.assertEquals("cannot load policies", e.getCause().getMessage());
        Assertions.assertThrows(IllegalStateException.class, controller::getRangerPlugin);
        Assertions.assertThrows(IllegalStateException.class, controller::dataVersions);
    }

    @Test
    public void testCheckpointInstanceLoadsNoPolicies() throws Exception {
        // The checkpoint instance is told apart by the calling thread, and the thread that loads the policies is
        // never that thread. The constructor asks the question itself, so the checkpoint instance still loads nothing.
        FieldUtils.writeDeclaredStaticField(GlobalStateMgr.class, "checkpointThreadId",
                Thread.currentThread().getId(), true);
        try {
            RangerStarRocksAccessController controller = RangerStarRocksAccessController.startingInBackground();
            // The load is blocked in this test, so this returns only if no load was started.
            CompletableFuture.runAsync(controller::awaitInitialized).get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(0, REFRESHER_STARTS.get());
        } finally {
            FieldUtils.writeDeclaredStaticField(GlobalStateMgr.class, "checkpointThreadId", -1L, true);
        }
    }
}
