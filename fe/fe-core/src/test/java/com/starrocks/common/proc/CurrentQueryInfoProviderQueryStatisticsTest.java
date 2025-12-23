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

package com.starrocks.common.proc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CurrentQueryInfoProviderQueryStatisticsTest {

    @Test
    public void testResultSinkStateStringMapping() {
        CurrentQueryInfoProvider.QueryStatistics statistics = new CurrentQueryInfoProvider.QueryStatistics();

        statistics.resultSinkState = 0;
        Assertions.assertEquals("not_ready", statistics.getResultSinkStateString());
        statistics.resultSinkState = 1;
        Assertions.assertEquals("ready", statistics.getResultSinkStateString());
        statistics.resultSinkState = 2;
        Assertions.assertEquals("running", statistics.getResultSinkStateString());
        statistics.resultSinkState = 3;
        Assertions.assertEquals("waiting_input", statistics.getResultSinkStateString());
        statistics.resultSinkState = 4;
        Assertions.assertEquals("waiting_output", statistics.getResultSinkStateString());
        statistics.resultSinkState = 5;
        Assertions.assertEquals("blocked", statistics.getResultSinkStateString());
        statistics.resultSinkState = 6;
        Assertions.assertEquals("finished", statistics.getResultSinkStateString());
        statistics.resultSinkState = 7;
        Assertions.assertEquals("canceled", statistics.getResultSinkStateString());
        statistics.resultSinkState = 8;
        Assertions.assertEquals("error", statistics.getResultSinkStateString());
        statistics.resultSinkState = 9;
        Assertions.assertEquals("finishing", statistics.getResultSinkStateString());
        statistics.resultSinkState = 10;
        Assertions.assertEquals("epoch_finishing", statistics.getResultSinkStateString());
        statistics.resultSinkState = 11;
        Assertions.assertEquals("epoch_finished", statistics.getResultSinkStateString());
        statistics.resultSinkState = 12;
        Assertions.assertEquals("local_waiting", statistics.getResultSinkStateString());

        statistics.resultSinkState = 999;
        Assertions.assertEquals("", statistics.getResultSinkStateString());
    }
}


