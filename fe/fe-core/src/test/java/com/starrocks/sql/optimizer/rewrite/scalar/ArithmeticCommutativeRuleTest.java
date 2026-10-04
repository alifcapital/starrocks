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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Test;

public class ArithmeticCommutativeRuleTest extends PlanTestBase {
    @Test
    public void testDivisionByZeroIsNotMovedToTheConstant() throws Exception {
        // x / 0 is NULL, so the predicate is never true. x = 5 * 0 would be true for x = 0.
        String plan = getFragmentPlan("select * from test_all_type where t1f / 0 = 5");
        assertNotContains(plan, "6: t1f = 0");
        assertContains(plan, "6: t1f / 0");
        plan = getFragmentPlan("select * from test_all_type where t1f / 2 = 5");
        assertContains(plan, "6: t1f = 10");
    }
}
