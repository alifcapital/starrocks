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

import com.starrocks.common.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LargeInPredicateCardinalityTest extends PlanWithCostTestBase {
    private static final Pattern CARDINALITY = Pattern.compile("cardinality: (\\d+)");

    @AfterEach
    public void resetThreshold() {
        connectContext.getSessionVariable().setLargeInPredicateThreshold(100000);
    }

    // The rows of the top node of the plan with the list as an InPredicate and as a LargeInPredicate
    private long[] cardinalities(String sql) throws Exception {
        long[] result = new long[2];
        int[] thresholds = {100000, 3};
        for (int i = 0; i < 2; i++) {
            connectContext.getSessionVariable().setLargeInPredicateThreshold(thresholds[i]);
            String plan = getCostExplain(sql);
            Matcher matcher = CARDINALITY.matcher(plan);
            Assertions.assertTrue(matcher.find(), plan);
            result[i] = Long.parseLong(matcher.group(1));
        }
        return result;
    }

    @Test
    public void testValuesOutOfColumnRange() throws Exception {
        // l_orderkey is in [1, 6000000]. We expect the join with the values of a LargeInPredicate to be estimated as
        // the IN with the same list: no rows when no value is in the range of the column, and the rows of the IN when
        // some value is in it.
        long[] rows = cardinalities(
                "select l_comment from lineitem where l_orderkey in (7000001, 7000002, 7000003, 7000004)");
        Assertions.assertEquals(1, rows[0]);
        Assertions.assertEquals(1, rows[1]);

        rows = cardinalities("select l_comment from lineitem where l_orderkey in (1, 2, 3, 4)");
        Assertions.assertEquals(rows[0], rows[1]);

        rows = cardinalities(
                "select l_comment from lineitem where l_orderkey in (5999999, 6000000, 6000001, 6000002)");
        Assertions.assertEquals(rows[0], rows[1]);

        // NOT IN of values out of the range keeps all rows
        rows = cardinalities(
                "select l_comment from lineitem where l_orderkey not in (7000001, 7000002, 7000003, 7000004)");
        Assertions.assertEquals(rows[0], rows[1], 1);
    }

    @Test
    public void testDuplicateValuesDoNotMultiplyMatches() throws Exception {
        // The semi/anti join matches a distinct value once, even if the SQL list repeats it thousands of times.
        int maxNodes = Config.max_scalar_operator_flat_children;
        // Allow the ordinary IN reference plan to hold all 10000 constants on the standalone branch too.
        Config.max_scalar_operator_flat_children = 100000;
        try {
            for (String values : List.of("1, 1, 1, 1", "1, 1, 2, 2",
                    String.join(",", Collections.nCopies(10000, "1")))) {
                for (String predicate : List.of("in", "not in")) {
                    long[] rows = cardinalities("select l_comment from lineitem where l_orderkey " +
                            predicate + " (" + values + ")");
                    Assertions.assertEquals(rows[0], rows[1], 1, predicate + " must ignore repeated values");
                }
            }
        } finally {
            Config.max_scalar_operator_flat_children = maxNodes;
        }
    }
}
