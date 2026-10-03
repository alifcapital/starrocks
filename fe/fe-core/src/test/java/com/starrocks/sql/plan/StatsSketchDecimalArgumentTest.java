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

import com.starrocks.planner.AggregationNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class StatsSketchDecimalArgumentTest extends PlanTestBase {
    private static void collectAggregations(PlanNode node, List<AggregationNode> out) {
        if (node instanceof AggregationNode) {
            out.add((AggregationNode) node);
        }
        for (PlanNode child : node.getChildren()) {
            collectAggregations(child, out);
        }
    }

    @Test
    public void testKllParameterLimits() throws Exception {
        for (int buckets : List.of(1, 10000)) {
            for (int k : List.of(8, 65535)) {
                getExecPlan("select ds_kll_quantiles(v1, " + buckets + ", " + k + ") from t0");
            }
        }
        for (long buckets : List.of(10001L, 1000000000L, 2147483647L, 2147483648L)) {
            Exception error = Assertions.assertThrows(Exception.class,
                    () -> getExecPlan("select ds_kll_quantiles(v1, " + buckets + ") from t0"));
            Assertions.assertTrue(error.getMessage().contains("must be between 1 and 10000"), error.getMessage());
        }
        for (int k : List.of(7, 65536, Integer.MAX_VALUE)) {
            Exception error = Assertions.assertThrows(Exception.class,
                    () -> getExecPlan("select ds_kll_quantiles(v1, 64, " + k + ") from t0"));
            Assertions.assertTrue(error.getMessage().contains("must be between 8 and 65535"), error.getMessage());
        }
        // frequent-items k is an output limit, not an allocation size. Its sketch has a separate bound.
        getExecPlan("select ds_frequent_items(v1, 2147483647, 14) from t0");
    }

    // The BE formats a decimal argument with the precision and scale the function signature carries;
    // the builtins are registered with wildcard decimals, so the analyzer has to fill them in.
    @Test
    public void testSketchAggregatesKeepDecimalPrecisionAndScale() throws Exception {
        for (String sql : List.of(
                "select ds_frequent_items(id_decimal, 10, 14) from test_all_type",
                "select ds_kll_quantiles(id_decimal, 8) from test_all_type",
                "select histogram_by_bounds(id_decimal, '[]', '[]') from test_all_type")) {
            List<AggregationNode> aggregations = new ArrayList<>();
            for (PlanFragment fragment : getExecPlan(sql).getFragments()) {
                collectAggregations(fragment.getPlanRoot(), aggregations);
            }
            Assertions.assertFalse(aggregations.isEmpty(), sql);
            for (AggregationNode aggregation : aggregations) {
                for (FunctionCallExpr call : aggregation.getAggInfo().getAggregateExprs()) {
                    Type argument = call.getFn().getArgs()[0];
                    Assertions.assertTrue(argument.isDecimalV3(), sql + ": " + argument);
                    Assertions.assertEquals(10, ((ScalarType) argument).getScalarPrecision(), sql);
                    Assertions.assertEquals(2, ((ScalarType) argument).getScalarScale(), sql);
                }
            }
        }
    }
}
