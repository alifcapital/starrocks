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

import org.junit.jupiter.api.Test;

/**
 * With a big outer table, the semi join of the merged OR of INs sends a runtime filter to the scan of the outer
 * table. This is the reason to merge: the plan with the outer join per subquery has no such filter.
 */
public class InSubqueryOrMergeRuntimeFilterTest extends PlanWithCostTestBase {

    @Test
    public void testRuntimeFilterReachesOuterScan() throws Exception {
        String sql = "select * from t0 where v1 in (select v4 from t1) or v1 in (select v7 from t2)";

        connectContext.getSessionVariable().setEnableRewriteInOrToUnion(false);
        String original;
        try {
            original = getVerboseExplain(sql);
        } finally {
            connectContext.getSessionVariable().setEnableRewriteInOrToUnion(true);
        }
        assertNotContains(original, "probe runtime filters:");

        String merged = getVerboseExplain(sql);
        assertContains(merged, "SEMI JOIN");
        assertContains(merged, "build runtime filters:");
        assertContains(merged, "probe runtime filters:");
    }
}
