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

package com.starrocks.planner;

import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TFunction;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TFunctionName;
import com.starrocks.thrift.TProjectNode;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdaptiveDopCostGuardTest {
    private TFunction function(String name, TFunctionBinaryType type) {
        return new TFunction().setName(new TFunctionName().setFunction_name(name)).setBinary_type(type);
    }

    @Test
    void classificationMatchesTwoPhase() throws Exception {
        Path root = Path.of("").toAbsolutePath();
        while (root != null && !Files.exists(root.resolve("be/src/exprs/case_expensive_functions.inc"))) {
            root = root.getParent();
        }
        assertNotNull(root, "Run from a checkout to verify FE/BE classification parity");
        String source = Files.readString(root.resolve("be/src/exprs/case_expensive_functions.inc"))
                .replaceAll("(?m)//.*$", "");
        Matcher matcher = Pattern.compile("\"([a-z0-9_]+)\"").matcher(source);
        Set<String> names = new HashSet<>();
        while (matcher.find()) {
            names.add(matcher.group(1));
        }
        assertEquals(names, AdaptiveDopCostGuard.EXPENSIVE_FUNCTIONS);
    }

    @Test
    void fragmentWithoutOutputOrDictionaryExpressions() {
        EmptySetNode node = new EmptySetNode(new PlanNodeId(0), new ArrayList<>(List.of(new TupleId(0))));
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), node, DataPartition.UNPARTITIONED);
        fragment.setSink(new NoopSink());
        assertNull(fragment.getOutputExprs());
        assertNull(fragment.getQueryGlobalDictExprs());
        assertFalse(fragment.containsExpensiveFunctionsForAdaptiveDop());
        assertTrue(fragment.canUseRuntimeAdaptiveDop());
    }

    @Test
    void plainCallsAliasesAndUdfs() {
        for (String name : List.of("regexp_extract", "replace_old", "parse_json", "ai_query", "array_sort_lambda")) {
            assertTrue(AdaptiveDopCostGuard.containsExpensiveFunction(function(name, TFunctionBinaryType.BUILTIN)));
        }
        assertTrue(AdaptiveDopCostGuard.containsExpensiveFunction(function("my_udf", TFunctionBinaryType.SRJAR)));
        for (String name : List.of("crc32", "length", "sum", "if")) {
            assertFalse(AdaptiveDopCostGuard.containsExpensiveFunction(function(name, TFunctionBinaryType.BUILTIN)));
        }
    }

    @Test
    void findsCallsInsideNestedExpressionMaps() {
        TExpr expr = new TExpr().setNodes(List.of(
                new TExprNode().setFn(function("length", TFunctionBinaryType.BUILTIN)),
                new TExprNode().setFn(function("regexp_extract", TFunctionBinaryType.BUILTIN))));
        TProjectNode project = new TProjectNode().setCommon_slot_map(Map.of(7, expr));
        assertTrue(AdaptiveDopCostGuard.containsExpensiveFunction(project));
        assertFalse(AdaptiveDopCostGuard.containsExpensiveFunction(new TProjectNode()));
    }
}
