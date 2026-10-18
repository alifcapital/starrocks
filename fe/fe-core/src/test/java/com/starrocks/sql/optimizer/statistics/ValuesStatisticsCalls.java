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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Counts how many times the statistics of the columns of VALUES are computed while a planning runs. The class of the
 * statistics is not public, so a test in another package asks this class. A test class calls install() once, in its
 * BeforeAll: the mock lives to the end of the class, and a second mock of the same method would call the first one.
 */
public final class ValuesStatisticsCalls {
    private static final AtomicInteger CALLS = new AtomicInteger();

    @FunctionalInterface
    public interface Planning {
        void run() throws Exception;
    }

    private ValuesStatisticsCalls() {
    }

    public static void install() {
        new MockUp<ValuesStatistics>() {
            @Mock
            public List<ColumnStatistic> of(Invocation invocation, List<ColumnRefOperator> columns,
                                            List<List<ScalarOperator>> rows, ExpressionStatisticsCache cache) {
                CALLS.incrementAndGet();
                return invocation.proceed(columns, rows, cache);
            }
        };
    }

    /** The number of calls of the computation made by the planning; a call that finds its result kept counts too. */
    public static int during(Planning planning) throws Exception {
        CALLS.set(0);
        planning.run();
        return CALLS.get();
    }
}
