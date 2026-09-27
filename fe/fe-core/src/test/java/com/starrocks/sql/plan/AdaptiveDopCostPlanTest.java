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

import com.starrocks.planner.AdaptiveDopCostGuard;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.SessionVariable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdaptiveDopCostPlanTest extends PlanTestBase {
    @Test
    void expensiveExpressionsUseOrdinaryStaticFragments() throws Exception {
        SessionVariable saved = connectContext.getSessionVariable();
        try {
            connectContext.setSessionVariable((SessionVariable) saved.clone());
            SessionVariable session = connectContext.getSessionVariable();
            session.setEnableQueryCache(false);
            session.setEnableRuntimeAdaptiveDop(true);
            session.setPipelineDop(7);
            List<String> queries = List.of(
                    "select regexp_extract(cast(v1 as varchar), '[0-9]+', 0) from t0",
                    "select v1 from t0 where regexp_extract(cast(v2 as varchar), '[0-9]+', 0) = '12'",
                    "select sum(length(regexp_extract(cast(v1 as varchar), '[0-9]+', 0))) from t0",
                    "select sum(length(case when v2 > 0 then regexp_extract(cast(v1 as varchar), "
                            + "'[0-9]+', 0) else cast(v3 as varchar) end)) from t0",
                    "select parse_json(cast(v1 as varchar)) from t0",
                    "select regexp_extract(cast(v1 as varchar), '[0-9]+', 0) from t0 union all "
                            + "select cast(v4 as varchar) from t1");
            for (String query : queries) {
                ExecPlan plan = getExecPlan(query);
                assertTrue(plan.getFragments().stream().noneMatch(PlanFragment::isUseRuntimeAdaptiveDop), query);
                boolean found = false;
                for (PlanFragment fragment : plan.getFragments()) {
                    if (AdaptiveDopCostGuard.containsExpensiveFunction(fragment.toThrift())) {
                        found = true;
                        assertFalse(fragment.isUseRuntimeAdaptiveDop(), query);
                        // No adaptive power-of-two rounding either: the normal static path is used.
                        assertEquals(7, fragment.getPipelineDop(), query);
                    }
                }
                assertTrue(found, query);
            }
        } finally {
            connectContext.setSessionVariable(saved);
        }
    }

    @Test
    void cheapExpressionsRemainAdaptiveAndOffRemainsOff() throws Exception {
        SessionVariable saved = connectContext.getSessionVariable();
        try {
            connectContext.setSessionVariable((SessionVariable) saved.clone());
            SessionVariable session = connectContext.getSessionVariable();
            session.setEnableQueryCache(false);
            session.setEnableRuntimeAdaptiveDop(true);
            String query = "select v1, sum(case when v2 > 0 then 1 else 0 end) from t0 group by v1";
            assertTrue(getExecPlan(query).getFragments().stream().anyMatch(PlanFragment::isUseRuntimeAdaptiveDop));
            session.setEnableRuntimeAdaptiveDop(false);
            assertTrue(getExecPlan(query).getFragments().stream().noneMatch(PlanFragment::isUseRuntimeAdaptiveDop));
        } finally {
            connectContext.setSessionVariable(saved);
        }
    }
}
