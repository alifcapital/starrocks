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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinReorderStatisticsDumpTest extends ReplayFromDumpTestBase {
    @ParameterizedTest
    // These two external queries reproduced missing column statistics. Query 1 and
    // native conversions did not; native/catalog replay has its own tests.
    @ValueSource(strings = {"2", "3"})
    void replayQueriesWithCollectedStatistics(String query) throws Exception {
        String dump = getDumpInfoFromFile("query_dump/join_reorder_stats_q" + query);
        var replay = getCostPlanFragment(dump, null);
        assertTrue(replay.second.contains("IcebergScanNode"), replay.second);
        assertTrue(replay.second.contains("JOIN"), replay.second);
        assertTrue(replay.first.getTableStatisticsMap().values().stream()
                .flatMap(columns -> columns.values().stream()).anyMatch(stat -> !stat.isUnknown()),
                "The regression must replay collected statistics, not an empty-statistics fallback");
    }
}
