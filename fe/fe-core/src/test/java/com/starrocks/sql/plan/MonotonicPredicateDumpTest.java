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

package com.starrocks.sql.plan;

import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MonotonicPredicateDumpTest extends ReplayFromDumpTestBase {
    private static Stream<Arguments> scanRanges() {
        return Stream.of(
                Arguments.of("eliminate_nestloop_join", "year(90: mock_139) >= 2023",
                        "90: mock_139 >= '2023-01-01 00:00:00'"),
                Arguments.of("force_rule_based_mv_rewrite_month",
                        "date_trunc('month', 1: LOCAL_ORDERED_DATE) >= '2023-04-01'",
                        "1: LOCAL_ORDERED_DATE >= '2023-04-01', 1: LOCAL_ORDERED_DATE < '2023-06-01'"),
                Arguments.of("materialized-view/mv_rewrite_bugs1",
                        "date_trunc('hour', 36: stream_time) >= '2025-08-31 17:00:00'",
                        "36: stream_time >= '2025-08-31 17:00:00', 36: stream_time < '2025-09-07 17:00:00'"),
                Arguments.of("materialized-view/view_based_rewrite2",
                        "date_trunc('day', 11: mock_050) >= '2020-07-28 00:00:00'",
                        "11: mock_050 >= '2020-07-28 00:00:00', 11: mock_050 < '2025-08-02 00:00:00'"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("scanRanges")
    public void testFunctionPredicateBecomesScanRange(String name, String function, String range) throws Exception {
        String json = getDumpInfoFromFile("query_dump/" + name);
        for (boolean enabled : new boolean[] {false, true}) {
            QueryDumpInfo dump = getDumpInfoFromJson(json);
            dump.getSessionVariable().setEnableMonotonicPredicateMoveAround(enabled);
            dump.getSessionVariable().setEnableMonotonicPredicateRewrite(enabled);
            dump.getSessionVariable().setEnableStringDateJoinPruning(false);
            dump.getSessionVariable().setOptimizerExecuteTimeout(60000);
            connectContext.setThreadLocalInfo();
            String plan = UtFrameUtils.getNewPlanAndFragmentFromDump(connectContext, dump)
                    .second.getExplainString(TExplainLevel.NORMAL);
            if (enabled) {
                assertTrue(plan.contains(range), plan);
                assertFalse(plan.contains(function), plan);
            } else {
                assertTrue(plan.contains(function), plan);
            }
        }
    }
}
