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
import com.starrocks.common.FeConstants;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class StatisticAutoCollectorTest {
    @Test
    public void seedExistingSchedulesDuringDayWithoutRepeatingFullTraversal() {
        boolean oldUnitTest = FeConstants.runningUnitTest;
        boolean oldCollect = Config.enable_statistic_collect;
        boolean oldSchedule = Config.enable_statistic_auto_collect_staggered_schedule;
        String oldStart = Config.statistic_auto_analyze_start_time;
        long oldInterval = Config.statistic_auto_collect_large_table_interval;
        long[] jobInterval = {604800};
        int[] passes = {0};
        new MockUp<StatisticAutoCollector>() {
            @Mock
            public boolean checkoutAnalyzeTime() {
                return false;
            }
        };
        new MockUp<StatisticUtils>() {
            @Mock
            public boolean checkStatisticTableStateNormal() {
                return true;
            }
        };
        StatisticAutoCollector collector = new StatisticAutoCollector() {
            @Override
            Map<Long, Map<String, String>> scheduleJobProperties() {
                return Map.of(7L, Map.of("statistic_auto_collect_interval", Long.toString(jobInterval[0])));
            }

            @Override
            public void prepareDefaultJob() {
            }

            @Override
            public List<StatisticsCollectJob> runJobs() {
                passes[0]++;
                return List.of();
            }
        };
        try {
            FeConstants.runningUnitTest = false;
            Config.enable_statistic_collect = true;
            Config.enable_statistic_auto_collect_staggered_schedule = true;
            collector.runAfterCatalogReady();
            Assertions.assertEquals(1, passes[0]);
            collector.runAfterCatalogReady();
            Assertions.assertEquals(1, passes[0]);
            Config.enable_statistic_auto_collect_staggered_schedule = false;
            collector.runAfterCatalogReady();
            Assertions.assertEquals(1, passes[0]);
            Config.enable_statistic_auto_collect_staggered_schedule = true;
            collector.runAfterCatalogReady();
            Assertions.assertEquals(2, passes[0]);
            Config.statistic_auto_analyze_start_time = "02:17:00";
            collector.runAfterCatalogReady();
            Assertions.assertEquals(3, passes[0]);
            Config.statistic_auto_collect_large_table_interval = oldInterval + 1;
            collector.runAfterCatalogReady();
            Assertions.assertEquals(4, passes[0]);
            jobInterval[0] = 86400;
            collector.runAfterCatalogReady();
            Assertions.assertEquals(5, passes[0]);
            collector.runAfterCatalogReady();
            Assertions.assertEquals(5, passes[0]);
        } finally {
            FeConstants.runningUnitTest = oldUnitTest;
            Config.enable_statistic_collect = oldCollect;
            Config.enable_statistic_auto_collect_staggered_schedule = oldSchedule;
            Config.statistic_auto_analyze_start_time = oldStart;
            Config.statistic_auto_collect_large_table_interval = oldInterval;
        }
    }
}
