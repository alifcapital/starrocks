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

package com.starrocks.staros;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * StarMgr can start in a background thread while the FE image loads. Whoever reaches StarMgr meanwhile must wait for
 * the start, so these tests use a server of their own and a stub for the start itself.
 */
public class StarMgrServerBackgroundInitTest {

    @Test
    public void testCallersWaitForTheInitialization() throws Exception {
        StarMgrServer server = new StarMgrServer();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch mayFinish = new CountDownLatch(1);

        server.initializeInBackground(() -> {
            started.countDown();
            mayFinish.await();
        });
        // The call has returned and the initialization is still blocked: it runs in another thread.
        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));

        CompletableFuture<Void> caller = CompletableFuture.runAsync(server::awaitInitialized);
        Assertions.assertThrows(TimeoutException.class, () -> caller.get(300, TimeUnit.MILLISECONDS));

        mayFinish.countDown();
        caller.get(10, TimeUnit.SECONDS);
        // Once the initialization is over nothing waits any more.
        server.awaitInitialized();
    }

    @Test
    public void testFailureIsThrownToEveryCaller() {
        StarMgrServer server = new StarMgrServer();
        IOException failure = new IOException("cannot load the image of StarMgr");

        server.initializeInBackground(() -> {
            throw failure;
        });

        // A failed start must never look like a StarMgr that is ready.
        IllegalStateException first = Assertions.assertThrows(IllegalStateException.class, server::awaitInitialized);
        Assertions.assertSame(failure, first.getCause());
        IllegalStateException second = Assertions.assertThrows(IllegalStateException.class, server::awaitInitialized);
        Assertions.assertSame(failure, second.getCause());
    }

    @Test
    public void testInitializationThreadDoesNotWaitForItself() throws Exception {
        StarMgrServer server = new StarMgrServer();

        // The start reaches StarMgr itself, for example through StarMgrServer.getCurrentState().
        server.initializeInBackground(server::awaitInitialized);

        CompletableFuture.runAsync(server::awaitInitialized).get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testNothingWaitsWithoutBackgroundInitialization() throws Exception {
        StarMgrServer server = new StarMgrServer();

        CompletableFuture.runAsync(server::awaitInitialized).get(10, TimeUnit.SECONDS);
    }
}
