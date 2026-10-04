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

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.type.JsonType;
import com.starrocks.type.Type;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class JsonExtractFusionRuleTest {
    private final JsonExtractFusionRule rule = new JsonExtractFusionRule();
    private final ScalarOperatorRewriteContext rewriteContext = new ScalarOperatorRewriteContext();

    @AfterEach
    void cleanup() {
        ConnectContext.remove();
    }

    private CallOperator candidate() {
        CallOperator parse = new CallOperator("parse_json", JsonType.JSON,
                List.of(ConstantOperator.createVarchar("{\"a\":1}")));
        return new CallOperator("JSON_QUERY", JsonType.JSON,
                List.of(parse, ConstantOperator.createVarchar("$.a")));
    }

    @Test
    void unrelatedFunctionAndWrongArityDoNotReadSession() {
        ConnectContext context = new ConnectContext() {
            @Override
            public SessionVariable getSessionVariable() {
                throw new AssertionError("Unrelated calls must not read session settings");
            }
        };
        context.setThreadLocalInfo();
        CallOperator unrelated = new CallOperator("other", JsonType.JSON, candidate().getChildren());
        CallOperator wrongArity = new CallOperator("json_query", JsonType.JSON, List.of());
        assertSame(unrelated, rule.visitCall(unrelated, rewriteContext));
        assertSame(wrongArity, rule.visitCall(wrongArity, rewriteContext));
    }

    @Test
    void matchingCallRetainsDisabledAndStrictModeGates() {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        CallOperator call = candidate();
        context.getSessionVariable().setEnableJsonExtractFusion(false);
        assertSame(call, rule.visitCall(call, rewriteContext));
        context.getSessionVariable().setEnableJsonExtractFusion(true);
        context.getSessionVariable().setSqlMode(SqlModeHelper.MODE_ALLOW_THROW_EXCEPTION);
        assertSame(call, rule.visitCall(call, rewriteContext));
    }

    @Test
    void matchingCallWithoutContextIsUnchanged() {
        ConnectContext.remove();
        CallOperator call = candidate();
        assertSame(call, rule.visitCall(call, rewriteContext));
    }

    @Test
    void matchingCallStillFusesWithCaseInsensitiveName() {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.getSessionVariable().setEnableJsonExtractFusion(true);
        context.getSessionVariable().setSqlMode(0);
        new MockUp<ExprUtils>() {
            @Mock
            public Function getBuiltinFunction(String name, Type[] types, Function.CompareMode mode) {
                assertEquals("json_query_from_string", name);
                assertEquals(Function.CompareMode.IS_IDENTICAL, mode);
                return new Function(new FunctionName(name), types, JsonType.JSON, false);
            }
        };
        CallOperator call = candidate();
        CallOperator result = (CallOperator) rule.visitCall(call, rewriteContext);
        assertEquals("json_query_from_string", result.getFnName());
        assertSame(call.getChild(0).getChild(0), result.getChild(0));
        assertSame(call.getChild(1), result.getChild(1));
        assertEquals("parse_json", ((CallOperator) call.getChild(0)).getFnName());
    }

}
