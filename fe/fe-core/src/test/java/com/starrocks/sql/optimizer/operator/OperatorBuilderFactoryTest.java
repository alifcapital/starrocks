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

package com.starrocks.sql.optimizer.operator;

import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class OperatorBuilderFactoryTest {
    // Planner threads of concurrent queries fill the cache at the same time. We start all threads on an empty
    // cache, so every thread races to add entries, and each build must still return the Builder of its operator.
    @Test
    public void testConcurrentBuildsOnAnEmptyCache() throws Exception {
        List<Operator> operators = List.of(
                new LogicalFilterOperator(ConstantOperator.TRUE),
                LogicalLimitOperator.init(10),
                new LogicalProjectOperator(Map.of()),
                new LogicalValuesOperator(List.of()));
        Field field = OperatorBuilderFactory.class.getDeclaredField("CONSTRUCTOR_MAP");
        field.setAccessible(true);
        Map<?, ?> cache = (Map<?, ?>) field.get(null);

        int threads = 16;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            for (int round = 0; round < 50; round++) {
                cache.clear();
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    futures.add(executor.submit(() -> {
                        start.await();
                        for (int i = 0; i < 200; i++) {
                            for (Operator operator : operators) {
                                Operator.Builder<?, ?> builder = OperatorBuilderFactory.build(operator);
                                Assertions.assertEquals(operator.getClass().getName() + "$Builder",
                                        builder.getClass().getName());
                            }
                        }
                        return null;
                    }));
                }
                start.countDown();
                for (Future<?> future : futures) {
                    future.get();
                }
                Assertions.assertEquals(operators.size(), cache.size());
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
