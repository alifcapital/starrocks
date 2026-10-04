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

package com.starrocks.sql.optimizer.rule.join;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

// A region of two atoms has one join. When the atoms have different row counts, left deep, DP and greedy build
// the same join, and ReorderJoinRule runs the first pass only. We expect the memo that transform builds and the
// tree that rewrite returns to be the same as with all passes.
public class ReorderJoinTwoAtomsTest {
    private ConnectContext connectContext;
    private int rootJoins;

    @BeforeEach
    public void setUp() {
        connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private enum Kind {
        PLAIN, CROSS, PROJECTION_OF_ONE_COLUMN, UNUSED_EXPRESSION, EXPRESSION_USED_BY_PREDICATE
    }

    private static final class Region {
        final ColumnRefFactory factory = new ColumnRefFactory();
        final ColumnRefOperator a = factory.create("a", IntegerType.INT, true);
        final ColumnRefOperator b = factory.create("b", IntegerType.INT, true);
        final ColumnRefOperator x = factory.create("x", IntegerType.INT, true);
        final OptExpression join;

        Region(int rowsOfA, int rowsOfB, Kind kind, int limit) {
            ScalarOperator on = BinaryPredicateOperator.eq(a, b);
            if (kind == Kind.CROSS) {
                on = null;
            } else if (kind == Kind.EXPRESSION_USED_BY_PREDICATE) {
                on = BinaryPredicateOperator.eq(x, b);
            }
            LogicalJoinOperator joinOp =
                    new LogicalJoinOperator(on == null ? JoinOperator.CROSS_JOIN : JoinOperator.INNER_JOIN, on);
            if (limit >= 0) {
                joinOp.setLimit(limit);
            }
            if (kind == Kind.PROJECTION_OF_ONE_COLUMN) {
                joinOp.setProjection(new Projection(Map.of(b, b)));
            } else if (kind == Kind.UNUSED_EXPRESSION) {
                joinOp.setProjection(new Projection(Map.of(b, b, x, ConstantOperator.createInt(1))));
            } else if (kind == Kind.EXPRESSION_USED_BY_PREDICATE) {
                joinOp.setProjection(new Projection(Map.of(b, b, x, a)));
            }
            join = OptExpression.create(joinOp, values(a, rowsOfA), values(b, rowsOfB));
            join.deriveLogicalPropertyItself();
        }

        private static OptExpression values(ColumnRefOperator column, int rows) {
            List<List<ScalarOperator>> data = new ArrayList<>();
            for (int i = 0; i < rows; ++i) {
                data.add(List.of(ConstantOperator.createInt(i)));
            }
            OptExpression values = OptExpression.create(new LogicalValuesOperator(List.of(column), data));
            values.deriveLogicalPropertyItself();
            return values;
        }
    }

    private static String describe(Operator op) {
        String projection = op.getProjection() == null ? "none" : op.getProjection().getColumnRefMap().entrySet()
                .stream().sorted(Comparator.comparingInt(e -> e.getKey().getId()))
                .map(e -> e.getKey().getId() + "=" + e.getValue()).collect(Collectors.joining(","));
        return op.getOpType() + " " + op + " limit=" + op.getLimit() + " projection=" + projection;
    }

    private static String describe(Memo memo) {
        StringBuilder sb = new StringBuilder("groups=" + memo.getGroups().size() + "\n");
        List<Group> groups = new ArrayList<>(memo.getGroups());
        groups.sort(Comparator.comparingInt(Group::getId));
        for (Group group : groups) {
            sb.append("group ").append(group.getId()).append(" rows=")
                    .append(group.getStatistics() == null ? "none" : group.getStatistics().getOutputRowCount())
                    .append('\n');
            for (GroupExpression expression : group.getLogicalExpressions()) {
                sb.append("  ").append(describe(expression.getOp())).append(" inputs=")
                        .append(expression.getInputs().stream().map(g -> "" + g.getId()).collect(Collectors.toList()))
                        .append('\n');
            }
        }
        return sb.toString();
    }

    private static String describe(OptExpression expression, String indent) {
        StringBuilder sb = new StringBuilder(indent).append(describe(expression.getOp())).append(" rows=")
                .append(expression.getStatistics() == null ? "none" : expression.getStatistics().getOutputRowCount())
                .append(" output=").append(expression.getOutputColumns()).append('\n');
        for (OptExpression input : expression.getInputs()) {
            sb.append(describe(input, indent + "  "));
        }
        return sb.toString();
    }

    private String transform(Region region, boolean skipRepeatedPasses, int[] skipped) {
        OptimizerContext context = OptimizerFactory.mockContext(connectContext, region.factory);
        Memo memo = new Memo();
        context.setMemo(memo);
        memo.init(region.join);
        context.setInMemoPhase(true);
        ReorderJoinRule rule = new ReorderJoinRule();
        rule.skipRepeatedPasses = skipRepeatedPasses;
        rule.transform(memo.getRootGroup().extractLogicalTree(), context);
        skipped[0] = rule.skippedRegions;
        rootJoins = memo.getRootGroup().getLogicalExpressions().size();
        return describe(memo);
    }

    private String rewrite(Region region, boolean skipRepeatedPasses, int[] skipped) {
        OptimizerContext context = OptimizerFactory.mockContext(connectContext, region.factory);
        ReorderJoinRule rule = new ReorderJoinRule();
        rule.skipRepeatedPasses = skipRepeatedPasses;
        OptExpression result = rule.rewrite(region.join, JoinReorderFactory.createJoinReorderAdaptive(), context);
        skipped[0] = rule.skippedRegions;
        return describe(result, "");
    }

    private void check(int rowsOfA, int rowsOfB, Kind kind, int limit, boolean expectSkip) {
        int[] all = new int[1];
        int[] some = new int[1];
        String expectedMemo = transform(new Region(rowsOfA, rowsOfB, kind, limit), false, all);
        String actualMemo = transform(new Region(rowsOfA, rowsOfB, kind, limit), true, some);
        assertEquals(expectedMemo, actualMemo);
        assertEquals(0, all[0]);
        assertEquals(expectSkip ? 1 : 0, some[0]);

        String expectedTree = rewrite(new Region(rowsOfA, rowsOfB, kind, limit), false, all);
        String actualTree = rewrite(new Region(rowsOfA, rowsOfB, kind, limit), true, some);
        assertEquals(expectedTree, actualTree);
        assertEquals(0, all[0]);
        assertEquals(expectSkip ? 1 : 0, some[0]);
    }

    @Test
    public void biggerAtomFirst() {
        check(100, 10, Kind.PLAIN, -1, true);
    }

    @Test
    public void smallerAtomFirst() {
        check(10, 100, Kind.PLAIN, -1, true);
    }

    @Test
    public void crossJoin() {
        check(10, 100, Kind.CROSS, -1, true);
    }

    @Test
    public void limitAndProjectionOfTheRoot() {
        check(10, 100, Kind.PROJECTION_OF_ONE_COLUMN, 5, true);
    }

    @Test
    public void expressionThatNoPredicateUses() {
        check(10, 100, Kind.UNUSED_EXPRESSION, -1, true);
        check(100, 10, Kind.UNUSED_EXPRESSION, 7, true);
    }

    @Test
    public void expressionThatAPredicateUsesKeepsAllPasses() {
        check(10, 100, Kind.EXPRESSION_USED_BY_PREDICATE, -1, false);
    }

    @Test
    public void equalRowCountsKeepAllPasses() {
        check(10, 10, Kind.PLAIN, -1, false);
        check(10, 10, Kind.CROSS, 3, false);
    }

    @Test
    public void allPassesAddAtMostTheSwappedJoin() {
        // The original join is A join B. With different row counts the passes agree on the bigger side on the left,
        // so the group holds one join, or two when that is the swapped one. With equal counts greedy adds both.
        int[] skipped = new int[1];
        transform(new Region(100, 10, Kind.PLAIN, -1), false, skipped);
        assertEquals(1, rootJoins);
        transform(new Region(10, 100, Kind.PLAIN, -1), false, skipped);
        assertEquals(2, rootJoins);
        transform(new Region(10, 10, Kind.PLAIN, -1), false, skipped);
        assertEquals(2, rootJoins);
    }
}
