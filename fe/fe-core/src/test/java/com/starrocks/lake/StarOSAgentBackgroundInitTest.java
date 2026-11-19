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

package com.starrocks.lake;

import com.staros.client.StarClient;
import com.starrocks.staros.StarMgrServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A call to the StarOSAgent that comes while StarMgr still starts in a background thread must wait for the start,
 * because the agent is the way the FE code reaches StarMgr.
 */
public class StarOSAgentBackgroundInitTest {

    @Test
    public void testClientWaitsForStarMgrToStart() throws Exception {
        StarOSAgent agent = new StarOSAgent();
        StarClient client = agent.initForTest();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch mayFinish = new CountDownLatch(1);
        try {
            StarMgrServer.getServingState().initializeInBackground(() -> {
                started.countDown();
                mayFinish.await();
            });
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));

            CompletableFuture<StarClient> caller = CompletableFuture.supplyAsync(agent::client);
            Assertions.assertThrows(TimeoutException.class, () -> caller.get(300, TimeUnit.MILLISECONDS));

            mayFinish.countDown();
            Assertions.assertSame(client, caller.get(10, TimeUnit.SECONDS));
        } finally {
            mayFinish.countDown();
            client.stop();
        }
    }
}
