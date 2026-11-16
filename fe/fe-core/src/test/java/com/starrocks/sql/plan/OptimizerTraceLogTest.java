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

import com.starrocks.common.profile.Tracers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// OptimizerTraceUtil returns before it builds a message when the trace would drop it; the trace of the rules still
// reaches the logs when the optimizer module is traced in the LOGS mode.
public class OptimizerTraceLogTest extends PlanTestBase {

    @Test
    public void ruleLogsReachTheTraceOnlyWhenEnabled() throws Exception {
        String sql = "select t0.v1 from t0 join t1 on t0.v1 = t1.v4 where t1.v5 > 1";
        try {
            Tracers.register(connectContext);
            Tracers.init(connectContext, "LOGS", "OPTIMIZER");
            Assertions.assertTrue(Tracers.isLogEnabled(Tracers.Module.OPTIMIZER));
            getFragmentPlan(sql);
            String logs = Tracers.printLogs();
            Assertions.assertTrue(logs.contains("APPLY RULE"), logs);
            Assertions.assertTrue(logs.contains("New Expression"), logs);
        } finally {
            Tracers.close();
        }

        try {
            Tracers.register(connectContext);
            Tracers.init(connectContext, "TIMER", "OPTIMIZER");
            Assertions.assertFalse(Tracers.isLogEnabled(Tracers.Module.OPTIMIZER));
            getFragmentPlan(sql);
            Assertions.assertFalse(Tracers.printLogs().contains("APPLY RULE"));
        } finally {
            Tracers.close();
        }
    }
}
