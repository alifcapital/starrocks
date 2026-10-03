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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator.LikeType;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

public class ConsolidateLikesRuleTest {
    private final ConsolidateLikesRule rule = ConsolidateLikesRule.INSTANCE;
    private final ColumnRefOperator a = new ColumnRefOperator(1, VarcharType.VARCHAR, "a", true);
    private final ColumnRefOperator b = new ColumnRefOperator(2, VarcharType.VARCHAR, "b", true);
    private ConnectContext connectContext;

    @BeforeEach
    public void setUp() {
        connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static ScalarOperator like(ColumnRefOperator column, String pattern) {
        return new LikePredicateOperator(column, ConstantOperator.createVarchar(pattern));
    }

    private static ScalarOperator notLike(ColumnRefOperator column, String pattern) {
        return new CompoundPredicateOperator(CompoundType.NOT, like(column, pattern));
    }

    private static ScalarOperator regexp(ColumnRefOperator column, String pattern) {
        return new LikePredicateOperator(LikeType.REGEXP, column, ConstantOperator.createVarchar(pattern));
    }

    private ScalarOperator equalsK(ColumnRefOperator column) {
        return new BinaryPredicateOperator(BinaryType.EQ, column, ConstantOperator.createVarchar("k"));
    }

    private static ScalarOperator or(ScalarOperator left, ScalarOperator right) {
        return new CompoundPredicateOperator(CompoundType.OR, left, right);
    }

    private static ScalarOperator and(ScalarOperator left, ScalarOperator right) {
        return new CompoundPredicateOperator(CompoundType.AND, left, right);
    }

    private ScalarOperator apply(ScalarOperator operator) {
        return rule.apply(operator, null);
    }

    @Test
    public void likesOnTheSameColumnAreMergedIntoOneRegexp() {
        ScalarOperator result = apply(or(like(a, "x%y"), like(a, "%z%")));
        assertEquals(regexp(a, "^((x.*y)|(.*z.*))$"), result);
    }

    @Test
    public void notLikesAreMergedIntoANegatedRegexp() {
        ScalarOperator result = apply(and(notLike(a, "x%y"), notLike(a, "%z%")));
        assertEquals(new CompoundPredicateOperator(CompoundType.NOT, regexp(a, "^((x.*y)|(.*z.*))$")), result);
    }

    @Test
    public void inputIsReturnedWhenNothingCanBeMerged() {
        List<ScalarOperator> inputs = List.of(
                // likes on different columns do not form a group
                or(like(a, "x%y"), like(b, "%z%")),
                and(notLike(a, "x%y"), notLike(b, "%z%")),
                // a single like
                or(like(a, "x%y"), equalsK(b)),
                and(notLike(a, "x%y"), equalsK(b)),
                // likes are merged under OR and not-likes under AND only
                and(like(a, "x%y"), like(a, "%z%")),
                or(notLike(a, "x%y"), notLike(a, "%z%")),
                // already a regexp
                or(regexp(a, "x.*y"), regexp(a, ".*z.*")),
                // prefix patterns are not merged
                or(like(a, "x%"), like(a, "y%")),
                // a NOT over an OR of likes, and an OR without likes
                new CompoundPredicateOperator(CompoundType.NOT, or(like(a, "x%y"), like(a, "%z%"))),
                or(equalsK(a), equalsK(b)));
        for (ScalarOperator input : inputs) {
            assertSame(input, apply(input), input.toString());
        }
    }

    @Test
    public void likesBelowTheTopOfALongChainAreFoundAndMerged() {
        // The likes are at the bottom of a left-deep OR chain, so the rule must look through the whole chain.
        ScalarOperator chain = or(like(a, "x%y"), like(a, "%z%"));
        for (int i = 0; i < 20; i++) {
            chain = or(chain, equalsK(b));
        }
        ScalarOperator result = apply(chain);
        CompoundPredicateOperator top = assertInstanceOf(CompoundPredicateOperator.class, result);
        assertEquals(regexp(a, "^((x.*y)|(.*z.*))$"), findRegexp(top));
    }

    private static ScalarOperator findRegexp(ScalarOperator root) {
        if (root instanceof LikePredicateOperator like && like.isRegexp()) {
            return root;
        }
        for (ScalarOperator child : root.getChildren()) {
            ScalarOperator found = findRegexp(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    @Test
    public void thresholdComesFromTheSessionOnEveryCall() {
        // The rule instance is shared, so it must read like_predicate_consolidate_min from the session on each call.
        ScalarOperator input = or(like(a, "x%y"), like(a, "%z%"));

        connectContext.getSessionVariable().setLikePredicateConsolidateMin(0);
        assertSame(input, apply(input));

        connectContext.getSessionVariable().setLikePredicateConsolidateMin(3);
        assertSame(input, apply(input));

        connectContext.getSessionVariable().setLikePredicateConsolidateMin(2);
        assertEquals(regexp(a, "^((x.*y)|(.*z.*))$"), apply(input));

        connectContext.getSessionVariable().setLikePredicateConsolidateMin(3);
        ScalarOperator three = or(or(like(a, "x%y"), like(a, "%z%")), like(a, "%q%w"));
        assertEquals(regexp(a, "^((x.*y)|(.*z.*)|(.*q.*w))$"), apply(three));
    }

}
