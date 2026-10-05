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

package com.starrocks.sql.optimizer;

import com.starrocks.common.FeConstants;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class OptimizerContextMetadataWaitTest {
    private boolean wasRunningUnitTest;

    @BeforeEach
    public void setUp() {
        // getOptimizerExecuteTimeout() returns a fixed large budget in unit tests; these tests set their own.
        wasRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
    }

    @AfterEach
    public void tearDown() {
        FeConstants.runningUnitTest = wasRunningUnitTest;
    }

    private static OptimizerContext newContext() throws Exception {
        return OptimizerFactory.mockContext(UtFrameUtils.createDefaultCtx(), new ColumnRefFactory());
    }

    @Test
    public void testNestedWaitIsCountedOnce() throws Exception {
        OptimizerContext context = newContext();
        try (OptimizerContext.MetadataWait outer = context.waitForMetadata()) {
            TimeUnit.MILLISECONDS.sleep(40);
            try (OptimizerContext.MetadataWait inner = context.waitForMetadata()) {
                TimeUnit.MILLISECONDS.sleep(40);
            }
        }
        long waited = context.getMetadataWaitMillis();
        Assertions.assertTrue(waited >= 80, "waited " + waited);
        Assertions.assertTrue(waited <= context.getOptimizerTimer().elapsed(TimeUnit.MILLISECONDS),
                "the wait must not exceed the elapsed optimizer time");
    }

    @Test
    public void testWaitOnAnotherThreadIsNotCounted() throws Exception {
        OptimizerContext context = newContext();
        Thread other = new Thread(() -> {
            try (OptimizerContext.MetadataWait ignored = context.waitForMetadata()) {
                TimeUnit.MILLISECONDS.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        other.start();
        other.join();
        Assertions.assertEquals(0, context.getMetadataWaitMillis());
    }

    @Test
    public void testTimeoutLeavesOutMetadataWait() throws Exception {
        OptimizerContext context = newContext();
        context.getSessionVariable().setOptimizerExecuteTimeout(100);
        try (OptimizerContext.MetadataWait ignored = context.waitForMetadata()) {
            TimeUnit.MILLISECONDS.sleep(200);
        }
        context.checkTimeout();

        context.getSessionVariable().setOptimizerTimeoutExcludeMetadataWait(false);
        StarRocksPlannerException e = Assertions.assertThrows(StarRocksPlannerException.class, context::checkTimeout);
        Assertions.assertTrue(e.getMessage().contains("waiting for external metadata"), e.getMessage());
    }

    @Test
    public void testOptimizerWorkStillTimesOut() throws Exception {
        OptimizerContext context = newContext();
        context.getSessionVariable().setOptimizerExecuteTimeout(100);
        TimeUnit.MILLISECONDS.sleep(200);
        Assertions.assertThrows(StarRocksPlannerException.class, context::checkTimeout);
    }
}
