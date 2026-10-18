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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.LogicalPropertyContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The statistics of a VALUES are computed when some operator above it reads them. A projection, a limit, a UNION ALL
 * and the operators of a CTE only pass the columns on. The trees here are built by hand, as the planner builds them
 * before it derives any statistics.
 */
public class ValuesStatisticsReadersTest {
    private static final ColumnRefOperator A = new ColumnRefOperator(1, IntegerType.INT, "a", true);

    private static OptExpression values() {
        return OptExpression.create(new LogicalValuesOperator(List.of(A),
                List.of(List.of(ConstantOperator.createInt(1)))));
    }

    private static OptExpression project(OptExpression input) {
        return OptExpression.create(new LogicalProjectOperator(Map.of(A, A)), input);
    }

    private static OptExpression filter(OptExpression input) {
        return OptExpression.create(new LogicalFilterOperator(
                BinaryPredicateOperator.gt(A, ConstantOperator.createInt(0))), input);
    }

    private static OptExpression aggregate(OptExpression input) {
        return OptExpression.create(new LogicalAggregationOperator(AggType.GLOBAL, List.of(A), Map.of()), input);
    }

    private static OptExpression join(OptExpression left, OptExpression right) {
        return OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                null), left, right);
    }

    private static OptExpression union(boolean all, OptExpression... inputs) {
        return OptExpression.create(new LogicalUnionOperator(List.of(A),
                Collections.nCopies(inputs.length, List.of(A)), all), inputs);
    }

    private static OptExpression consume(int cte) {
        return OptExpression.create(new LogicalCTEConsumeOperator(cte, Map.of()));
    }

    private static OptExpression produce(int cte, OptExpression body) {
        return OptExpression.create(new LogicalCTEProduceOperator(cte), body);
    }

    // The producer first, as the planner puts it, or the query first.
    private static OptExpression anchor(int cte, boolean producerFirst, OptExpression producer, OptExpression query) {
        return producerFirst
                ? OptExpression.create(new LogicalCTEAnchorOperator(cte), producer, query)
                : OptExpression.create(new LogicalCTEAnchorOperator(cte), query, producer);
    }

    @Test
    public void testOperatorsThatPassTheColumnsOnDoNotRead() {
        Assertions.assertFalse(ValuesStatisticsReaders.hasReader(values()));
        Assertions.assertFalse(ValuesStatisticsReaders.hasReader(project(values())));
        Assertions.assertFalse(ValuesStatisticsReaders.hasReader(
                OptExpression.create(LogicalLimitOperator.init(10), project(values()))));
        Assertions.assertFalse(ValuesStatisticsReaders.hasReader(project(union(true, values(), values()))));
    }

    @Test
    public void testOtherOperatorsRead() {
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(filter(values())));
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(project(filter(project(values())))));
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(aggregate(project(values()))));
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(join(values(), consume(7))));
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(join(consume(7), values())));
        // UNION DISTINCT compares the rows.
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(union(false, values(), values())));
    }

    // An operator that nothing in the optimizer knows. We fear that a new operator is taken for a pass-through and
    // gets unknown statistics for the lists under it without any test failing, so a class that is not named as a
    // pass-through must read.
    private static final class UnknownOperator extends LogicalOperator {
        private UnknownOperator() {
            super(OperatorType.LOGICAL);
        }

        @Override
        public ColumnRefSet getOutputColumns(LogicalPropertyContext expressionContext) {
            return new ColumnRefSet();
        }
    }

    private static OptExpression with(LogicalOperator operator, OptExpression... inputs) {
        return OptExpression.create(operator, inputs);
    }

    // One operator of each class that reads, with the list as its input. The rule is the class of the operator, so
    // the operators here carry no content.
    @Test
    public void testEveryClassOfReaderReads() {
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(with(LogicalApplyOperator.builder().build(),
                values(), values())), "Apply");
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(with(new LogicalTopNOperator(List.of()), values())),
                "TopN");
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(with(new LogicalWindowOperator.Builder().build(),
                values())), "Window");
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(with(
                new LogicalTableFunctionOperator(List.of(), null, List.of()), values())), "TableFunction");
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(with(new LogicalExceptOperator.Builder().build(),
                values(), values())), "Except");
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(with(new LogicalIntersectOperator.Builder().build(),
                values(), values())), "Intersect");
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(with(LogicalAssertOneRowOperator.createLessEqOne(""),
                values())), "AssertOneRow");
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(with(
                new LogicalRepeatOperator(List.of(), List.of(), List.of(), Map.of()), values())), "Repeat");
    }

    // The rewrite rules work on a tree under an anchor. We expect the anchor to pass the columns on: a tree that is
    // looked at again during the rewrite must not read every list only because of the anchor above it.
    @Test
    public void testAnchorOfTheRewriteDoesNotRead() {
        Assertions.assertFalse(ValuesStatisticsReaders.hasReader(with(new LogicalTreeAnchorOperator(), values())));
        Assertions.assertFalse(ValuesStatisticsReaders.hasReader(with(new LogicalTreeAnchorOperator(),
                project(union(true, values(), values())))));
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(with(new LogicalTreeAnchorOperator(),
                filter(values()))));
    }

    @Test
    public void testUnknownOperatorReads() {
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(with(new UnknownOperator(), values())));
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(project(with(new UnknownOperator(), project(values())))));
    }

    @Test
    public void testReaderInAnotherBranchIsNotAnAncestor() {
        Assertions.assertFalse(ValuesStatisticsReaders.hasReader(project(union(true, values(), filter(consume(7))))));
        Assertions.assertFalse(ValuesStatisticsReaders.hasReader(project(union(true, filter(consume(7)), values()))));
        // The join is above both branches.
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(join(values(), filter(consume(7)))));
    }

    @Test
    public void testConsumeWithAnInputIsReadThroughItsAncestors() {
        // A CTE that is not forced to be reused has the copy of its body under each consumer.
        Assertions.assertTrue(ValuesStatisticsReaders.hasReader(filter(OptExpression.create(
                new LogicalCTEConsumeOperator(1, Map.of()), values()))));
        Assertions.assertFalse(ValuesStatisticsReaders.hasReader(project(OptExpression.create(
                new LogicalCTEConsumeOperator(1, Map.of()), values()))));
    }

    @Test
    public void testValuesInTheProducerAreReadWhereTheConsumersAreRead() {
        for (boolean producerFirst : new boolean[] {true, false}) {
            Assertions.assertTrue(ValuesStatisticsReaders.hasReader(anchor(1, producerFirst, produce(1, values()),
                    join(consume(1), consume(1)))));
            Assertions.assertTrue(ValuesStatisticsReaders.hasReader(anchor(1, producerFirst,
                    produce(1, project(values())), project(aggregate(consume(1))))));
            Assertions.assertFalse(ValuesStatisticsReaders.hasReader(anchor(1, producerFirst, produce(1, values()),
                    union(true, project(consume(1)), project(consume(1))))));
        }
    }

    @Test
    public void testNestedCteIsReadThroughTheCteThatConsumesIt() {
        // CTE 2 consumes CTE 1 in its body, and the join is over the consumers of CTE 2 only.
        for (boolean outerProducerFirst : new boolean[] {true, false}) {
            for (boolean innerProducerFirst : new boolean[] {true, false}) {
                Assertions.assertTrue(ValuesStatisticsReaders.hasReader(anchor(1, outerProducerFirst,
                        produce(1, values()),
                        anchor(2, innerProducerFirst, produce(2, project(consume(1))),
                                join(consume(2), consume(2))))));
                Assertions.assertFalse(ValuesStatisticsReaders.hasReader(anchor(1, outerProducerFirst,
                        produce(1, values()),
                        anchor(2, innerProducerFirst, produce(2, project(consume(1))),
                                union(true, consume(2), consume(2))))));
            }
        }
    }
}
