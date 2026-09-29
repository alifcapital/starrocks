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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.common.Config;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.plan.PlanTestBase;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class OperatorMaxFlatChildTest extends PlanTestBase {
    // Each level refers to the column of the level below it six times
    private static final String NESTED_CASE_WHEN = "select\n" +
            "    case\n" +
            "        when cw1 = \"X11\" then concat(cw1, \"11\")\n" +
            "        when cw1 = \"X111\" then concat(cw1, \"111\")\n" +
            "        when cw1 = \"X1111\" then concat(cw1, \"1111\")\n" +
            "    end cw1\n" +
            "from\n" +
            "    (\n" +
            "        select\n" +
            "            case\n" +
            "                when cw1 = \"X11\" then concat(cw1, \"11\")\n" +
            "                when cw1 = \"X111\" then concat(cw1, \"111\")\n" +
            "                when cw1 = \"X1111\" then concat(cw1, \"1111\")\n" +
            "            end cw1\n" +
            "        from\n" +
            "            (\n" +
            "                select\n" +
            "                    case\n" +
            "                        when cw1 = \"X2\" then concat(cw1, \"1\")\n" +
            "                        when cw1 = \"X1\" then concat(cw1, \"11\")\n" +
            "                        when cw1 = \"X3\" then concat(cw1, \"11\")\n" +
            "                    end cw1\n" +
            "                from\n" +
            "                    (\n" +
            "                        select\n" +
            "                            case\n" +
            "                                when cw1 = 1 then upper(cw1)\n" +
            "                                when cw1 = 2 then cw1\n" +
            "                                when cw1 = 3 then lower(cw1)\n" +
            "                            end cw1\n" +
            "                        from\n" +
            "                            (\n" +
            "                                select\n" +
            "                                    case\n" +
            "                                        when t1a = 1 then t1a\n" +
            "                                        when t1a = 2 then t1b\n" +
            "                                        when t1a = 3 then t1c\n" +
            "                                    end cw1\n" +
            "                                from\n" +
            "                                    test_all_type\n" +
            "                            ) t\n" +
            "                    ) t\n" +
            "            ) t\n" +
            "    ) t;\n";

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testMaxCaseWhenChildren() {
        assertThrows(SemanticException.class, () -> {
            final int prev = Config.max_scalar_operator_flat_children;
            Config.max_scalar_operator_flat_children = 10;
            try {
                getFragmentPlan(NESTED_CASE_WHEN);
            } finally {
                Config.max_scalar_operator_flat_children = prev;
            }
        });
    }

    @Test
    public void testNestedCaseWhenNotCopied() throws Exception {
        // We expect the projects to stay apart: one project for all levels would have hundreds of copies of the case
        // when of the lowest level
        String plan = getFragmentPlan(NESTED_CASE_WHEN);
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "1: t1a = '1'"), plan);
    }

    @Test
    public void testMergeProjectsWithFewCopies() throws Exception {
        String plan = getFragmentPlan("select x, x + 1 from (select v1 + v2 as x, v3 from t0) t where v3 > 1");
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "Project"), plan);
        assertContains(plan, "common expressions:");
    }

    @Test
    public void testMergeProjectsAboveLimit() throws Exception {
        final int prev = Config.max_scalar_operator_flat_children;
        Config.max_scalar_operator_flat_children = 25;
        try {
            // Each project has less than 25 nodes and the merged one would have more, so we expect two projects
            String plan = getFragmentPlan("select x + (v1 + 6) * (v2 + 7) * (v3 + 8) * (v1 + 9) as y from " +
                    "(select (v1 + 1) * (v2 + 2) * (v3 + 3) * (v1 + 4) as x, v1, v2, v3 from t0) t");
            Assertions.assertEquals(2, StringUtils.countMatches(plan, "Project"), plan);
        } finally {
            Config.max_scalar_operator_flat_children = prev;
        }
    }

    @Test
    public void testPredicateThatCopiesStaysAboveProject() throws Exception {
        StringBuilder caseWhen = new StringBuilder("case");
        for (int i = 0; i < 2000; i++) {
            caseWhen.append(" when v1 = ").append(i).append(" then ").append(i);
        }
        // Below the project the predicate would have three copies of the case when, so we expect it to stay above
        String plan = getFragmentPlan("select * from (select " + caseWhen + " end as x, v2 from t0) t " +
                "where (x = 1 or x = 2 or x = 3) and v2 > 5");
        assertContains(plan, "predicates: ((4: case = 1) OR (4: case = 2)) OR (4: case = 3)");
        assertContains(plan, "PREDICATES: 2: v2 > 5");
    }

    @Test
    public void testInListBelowLargeInThreshold() throws Exception {
        int threshold = connectContext.getSessionVariable().getLargeInPredicateThreshold();
        connectContext.getSessionVariable().setLargeInPredicateThreshold(10000);
        try {
            StringBuilder in = new StringBuilder("v1 in (0");
            for (int i = 1; i < 9990; i++) {
                in.append(", ").append(i);
            }
            in.append(")");
            // The IN alone has more than 10000 nodes, and we expect the query to plan with it
            String plan = getFragmentPlan("select * from t0 where " + in +
                    " and v2 > 1 and v3 < 5 and (v2 = 3 or v3 = 4)");
            assertContains(plan, "1: v1 IN (0, 1, 2, ");
            assertNotContains(plan, "RAW VALUES");
        } finally {
            connectContext.getSessionVariable().setLargeInPredicateThreshold(threshold);
        }
    }
}
