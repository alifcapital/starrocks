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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LeafRewriteRuleTest {

    // Rules that declare they do not rewrite leaves. A rule that starts to change a constant or a column ref must
    // stop declaring it, and this list then fails.
    private static List<ScalarOperatorRewriteRule> leafSkippingRules() {
        return List.of(
                new ImplicitCastRule(),
                new ReduceCastRule(),
                new NormalizePredicateRule(),
                new MvNormalizePredicateRule(),
                new FoldConstantsRule(),
                new SimplifiedPredicateRule(),
                new SimplifiedDateColumnPredicateRule(),
                new SimplifiedScanColumnRule(),
                new ExtractCommonPredicateRule(),
                new ArithmeticCommutativeRule(),
                ConsolidateLikesRule.INSTANCE,
                SimplifiedCaseWhenRule.INSTANCE,
                SimplifiedCaseWhenRule.SKIP_COMPLEX_FUNCTIONS_INSTANCE);
    }

    private static List<Type> types() {
        return List.of(BooleanType.BOOLEAN, IntegerType.TINYINT, IntegerType.SMALLINT, IntegerType.INT,
                IntegerType.BIGINT, IntegerType.LARGEINT, FloatType.FLOAT, FloatType.DOUBLE,
                DecimalType.DEFAULT_DECIMAL32, DecimalType.DEFAULT_DECIMAL64, DecimalType.DEFAULT_DECIMAL128,
                DateType.DATE, DateType.DATETIME, VarcharType.VARCHAR, JsonType.JSON);
    }

    private static List<ScalarOperator> leaves() {
        List<ScalarOperator> leaves = new ArrayList<>();
        leaves.add(ConstantOperator.createBoolean(true));
        leaves.add(ConstantOperator.createTinyInt((byte) 1));
        leaves.add(ConstantOperator.createSmallInt((short) 2));
        leaves.add(ConstantOperator.createInt(3));
        leaves.add(ConstantOperator.createBigint(4L));
        leaves.add(ConstantOperator.createLargeInt(BigInteger.valueOf(5)));
        leaves.add(ConstantOperator.createDouble(1.5));
        leaves.add(ConstantOperator.createDecimal(new BigDecimal("1.25"), DecimalType.DEFAULT_DECIMAL64));
        leaves.add(ConstantOperator.createDate(LocalDateTime.of(2024, 1, 2, 0, 0)));
        leaves.add(ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 2, 3, 4, 5)));
        leaves.add(ConstantOperator.createVarchar("abc"));
        leaves.add(ConstantOperator.createVarchar(""));
        leaves.add(ConstantOperator.createChar("c"));
        int id = 1;
        for (Type type : types()) {
            leaves.add(ConstantOperator.createNull(type));
            leaves.add(new ColumnRefOperator(id++, type, "c" + id, true));
            leaves.add(new ColumnRefOperator(id++, type, "c" + id, false));
        }
        return leaves;
    }

    @Test
    public void testSkippingRulesKeepLeaves() {
        for (ScalarOperatorRewriteRule rule : leafSkippingRules()) {
            assertFalse(rule.rewritesLeaves(), rule.getClass().getSimpleName());
            for (ScalarOperator leaf : leaves()) {
                ScalarOperatorRewriteContext context = new ScalarOperatorRewriteContext();
                String message = rule.getClass().getSimpleName() + " on " + leaf.getType() + " " + leaf;
                assertSame(leaf, rule.apply(leaf, context), message);
                assertEquals(0, context.changeNum(), message);
                assertFalse(context.hasChanged(), message);
            }
        }
    }

    @Test
    public void testRewriterKeepsLeaves() {
        List<ScalarOperatorRewriteRule> rules = new ArrayList<>(leafSkippingRules());
        for (ScalarOperator leaf : leaves()) {
            ScalarOperator before = leaf.clone();
            assertSame(leaf, new ScalarOperatorRewriter().rewrite(leaf, rules), leaf.toString());
            assertEquals(before, leaf);
        }
    }

    @Test
    public void testRewriterSkipsOnlyLeavesOfMarkedRules() {
        AtomicInteger skippedCalls = new AtomicInteger();
        AtomicInteger visitedCalls = new AtomicInteger();
        ScalarOperatorRewriteRule skipping = new CountingRule(skippedCalls, false, true);
        ScalarOperatorRewriteRule visiting = new CountingRule(visitedCalls, true, true);
        ScalarOperatorRewriteRule skippingTopDown = new CountingRule(skippedCalls, false, false);
        ScalarOperatorRewriteRule visitingTopDown = new CountingRule(visitedCalls, true, false);

        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "c", true);
        ConstantOperator constant = ConstantOperator.createInt(1);
        ScalarOperator tree = new BinaryPredicateOperator(BinaryType.EQ, column, constant);

        new ScalarOperatorRewriter().rewrite(tree, Lists.newArrayList(skipping, skippingTopDown));
        // Only the binary predicate is applied, once per rule.
        assertEquals(2, skippedCalls.get());

        new ScalarOperatorRewriter().rewrite(tree, Lists.newArrayList(visiting, visitingTopDown));
        assertEquals(6, visitedCalls.get());
    }

    // A rule that skips leaves must not hide a rewrite of a leaf from a rule that does not.
    @Test
    public void testRulesTouchingLeavesKeepDefault() {
        ColumnRefOperator from = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator to = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        ReplaceScalarOperatorRule replace = new ReplaceScalarOperatorRule(ImmutableMap.of(from, to));
        assertTrue(replace.rewritesLeaves());
        assertSame(to, new ScalarOperatorRewriter().rewrite(from, Lists.newArrayList(replace)));

        ConstantOperator one = ConstantOperator.createInt(1);
        ReplaceScalarOperatorRule replaceConstant = new ReplaceScalarOperatorRule(ImmutableMap.of(one, to));
        assertSame(to, new ScalarOperatorRewriter().rewrite(one, Lists.newArrayList(replaceConstant)));

        // The skipping rule runs next to a leaf rewrite in the same list and sees the rewritten leaf.
        ScalarOperator result = new ScalarOperatorRewriter().rewrite(from,
                Lists.newArrayList(new ReduceCastRule(), replace, new NormalizePredicateRule()));
        assertSame(to, result);
        assertNotSame(from, result);
    }

    private static class CountingRule extends BaseScalarOperatorRewriteRule {
        private final AtomicInteger calls;
        private final boolean rewritesLeaves;
        private final boolean bottomUp;

        CountingRule(AtomicInteger calls, boolean rewritesLeaves, boolean bottomUp) {
            this.calls = calls;
            this.rewritesLeaves = rewritesLeaves;
            this.bottomUp = bottomUp;
        }

        @Override
        public boolean isBottomUp() {
            return bottomUp;
        }

        @Override
        public boolean isTopDown() {
            return !bottomUp;
        }

        @Override
        public boolean rewritesLeaves() {
            return rewritesLeaves;
        }

        @Override
        public ScalarOperator apply(ScalarOperator root, ScalarOperatorRewriteContext context) {
            calls.incrementAndGet();
            return root;
        }
    }
}
