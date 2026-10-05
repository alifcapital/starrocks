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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// DP and greedy join reorder need the cardinalities of the region's joins. We expect them to depend on the statistics
// of the columns the join conditions read, as the atoms derive them, and on nothing below the atoms. These tests build
// regions whose atoms have statistics of different quality and check which of them make the region unknown.
class JoinReorderUnknownStatisticsTest {
    @Mocked
    private IcebergTable table;

    private ConnectContext connectContext;
    private ColumnRefFactory factory;
    private OptimizerContext optimizer;
    private final ColumnStatistic known = new ColumnStatistic(1, 100, 0, 4, 100);
    private final ColumnStatistic unknown = ColumnStatistic.unknown();

    @BeforeEach
    void setUp() {
        connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        factory = new ColumnRefFactory();
        optimizer = OptimizerFactory.mockContext(connectContext, factory);
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    private ColumnRefOperator column(String name) {
        return factory.create(name, IntegerType.INT, true);
    }

    // The Iceberg scan operator keeps its unknown-column flag at the default, true, because nobody derived the scan.
    private OptExpression scan(ColumnRefOperator key, ColumnStatistic keyStatistic, ColumnRefOperator other,
                               ColumnStatistic otherStatistic, double rows) {
        Column keyColumn = new Column(key.getName(), IntegerType.INT);
        Column otherColumn = new Column(other.getName(), IntegerType.INT);
        OptExpression scan = OptExpression.create(new LogicalIcebergScanOperator(table,
                Map.of(key, keyColumn, other, otherColumn), Map.of(keyColumn, key, otherColumn, other), -1, null));
        scan.deriveLogicalPropertyItself();
        scan.setStatistics(Statistics.builder().setOutputRowCount(rows)
                .addColumnStatistic(key, keyStatistic).addColumnStatistic(other, otherStatistic).build());
        return scan;
    }

    // What IcebergEqualityDeleteRewriteRule builds: the data files without equality deletes, and the data files with
    // them, which have the default statistics, as the two inputs of a UNION ALL.
    private OptExpression equalityDeleteUnion(ColumnRefOperator key, ColumnRefOperator other,
                                              ColumnStatistic keyStatisticWithoutDeletes) {
        ColumnRefOperator keyWithoutDeletes = column("key_without_deletes");
        ColumnRefOperator otherWithoutDeletes = column("other_without_deletes");
        ColumnRefOperator keyWithDeletes = column("key_with_deletes");
        ColumnRefOperator otherWithDeletes = column("other_with_deletes");
        OptExpression withoutDeletes = scan(keyWithoutDeletes, keyStatisticWithoutDeletes, otherWithoutDeletes, known, 1000);
        OptExpression withDeletes = scan(keyWithDeletes, unknown, otherWithDeletes, unknown, 1);
        OptExpression union = OptExpression.create(new LogicalUnionOperator(List.of(key, other),
                List.of(List.of(keyWithoutDeletes, otherWithoutDeletes), List.of(keyWithDeletes, otherWithDeletes)),
                true, true), withoutDeletes, withDeletes);
        union.deriveLogicalPropertyItself();
        return union;
    }

    private MultiJoinNode region(OptExpression left, ColumnRefOperator leftKey, OptExpression right,
                                 ColumnRefOperator rightKey) {
        OptExpression join = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                BinaryPredicateOperator.eq(leftKey, rightKey)), left, right);
        join.deriveLogicalPropertyItself();
        return MultiJoinNode.toMultiJoinNode(join);
    }

    @Test
    void equalityDeleteUnionTakesTheStatisticsOfTheDataFilesWithoutDeletes() {
        ColumnRefOperator key = column("key");
        ColumnRefOperator other = column("other");
        ColumnRefOperator rightKey = column("right_key");
        ColumnRefOperator rightOther = column("right_other");
        OptExpression union = equalityDeleteUnion(key, other, known);
        MultiJoinNode region = region(union, key, scan(rightKey, known, rightOther, known, 100), rightKey);

        assertFalse(region.hasUnknownJoinColumnStatistics(optimizer));
        // The data files with deletes have no statistics, and the union does not take them.
        assertFalse(union.getStatistics().getColumnStatistic(key).isUnknown());
        assertFalse(union.getStatistics().getColumnStatistic(other).isUnknown());
        // Nobody derived the scans below the union, so their unknown-column flags have the default value, true.
        assertTrue(Utils.hasUnknownColumnsStats(union));
    }

    @Test
    void equalityDeleteUnionIsUnknownWhenTheJoinColumnOfTheDataFilesWithoutDeletesIs() {
        ColumnRefOperator key = column("key");
        ColumnRefOperator other = column("other");
        ColumnRefOperator rightKey = column("right_key");
        ColumnRefOperator rightOther = column("right_other");
        OptExpression union = equalityDeleteUnion(key, other, unknown);
        MultiJoinNode region = region(union, key, scan(rightKey, known, rightOther, known, 100), rightKey);

        assertTrue(region.hasUnknownJoinColumnStatistics(optimizer));
    }

    @Test
    void unknownColumnThatNoJoinConditionReadsDoesNotMakeTheRegionUnknown() {
        ColumnRefOperator leftKey = column("left_key");
        ColumnRefOperator leftOther = column("left_other");
        ColumnRefOperator rightKey = column("right_key");
        ColumnRefOperator rightOther = column("right_other");
        MultiJoinNode region = region(scan(leftKey, known, leftOther, unknown, 100), leftKey,
                scan(rightKey, known, rightOther, known, 100), rightKey);

        assertFalse(region.hasUnknownJoinColumnStatistics(optimizer));
    }

    @Test
    void unknownJoinColumnMakesTheRegionUnknown() {
        ColumnRefOperator leftKey = column("left_key");
        ColumnRefOperator leftOther = column("left_other");
        ColumnRefOperator rightKey = column("right_key");
        ColumnRefOperator rightOther = column("right_other");
        MultiJoinNode region = region(scan(leftKey, known, leftOther, known, 100), leftKey,
                scan(rightKey, unknown, rightOther, known, 100), rightKey);

        assertTrue(region.hasUnknownJoinColumnStatistics(optimizer));
    }

    @Test
    void joinColumnThatTheRegionComputesIsTracedToItsInput() {
        ColumnRefOperator a = column("a");
        ColumnRefOperator aOther = column("a_other");
        ColumnRefOperator b = column("b");
        ColumnRefOperator bOther = column("b_other");
        ColumnRefOperator x = column("x");
        for (ColumnStatistic statisticOfA : List.of(known, unknown)) {
            LogicalJoinOperator joinOp = new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                    BinaryPredicateOperator.eq(x, b));
            joinOp.setProjection(new Projection(Map.of(b, b, x, a)));
            OptExpression join = OptExpression.create(joinOp, scan(a, statisticOfA, aOther, known, 100),
                    scan(b, known, bOther, known, 100));
            join.deriveLogicalPropertyItself();

            assertEquals(statisticOfA == unknown,
                    MultiJoinNode.toMultiJoinNode(join).hasUnknownJoinColumnStatistics(optimizer));
        }
    }

    // An atom that has derived statistics, such as a CTE consumer, does not derive them again from the scans below it,
    // so the unknown-column flag of such a scan can be stale.
    @Test
    void atomWithDerivedStatisticsDoesNotShowTheFlagsOfItsScans() {
        ColumnRefOperator key = column("key");
        ColumnRefOperator other = column("other");
        ColumnRefOperator rightKey = column("right_key");
        ColumnRefOperator rightOther = column("right_other");
        OptExpression scan = scan(key, unknown, other, unknown, 100);
        Map<ColumnRefOperator, ScalarOperator> projection = Map.of(key, key);
        OptExpression atom = OptExpression.create(new LogicalProjectOperator(projection), scan);
        atom.deriveLogicalPropertyItself();
        atom.setStatistics(Statistics.builder().setOutputRowCount(100).addColumnStatistic(key, known).build());
        MultiJoinNode region = region(atom, key, scan(rightKey, known, rightOther, known, 100), rightKey);

        assertTrue(Utils.hasUnknownColumnsStats(atom));
        assertFalse(region.hasUnknownJoinColumnStatistics(optimizer));
    }

    // The consumer of a CTE is a leaf atom whose statistics come from the CTE producer. They are all the region sees.
    @Test
    void cteConsumerAtomIsJudgedByItsStatistics() {
        for (ColumnStatistic keyStatistic : List.of(known, unknown)) {
            ColumnRefOperator key = column("key");
            ColumnRefOperator producerKey = column("producer_key");
            ColumnRefOperator rightKey = column("right_key");
            ColumnRefOperator rightOther = column("right_other");
            OptExpression consumer = OptExpression.create(new LogicalCTEConsumeOperator(1, Map.of(key, producerKey)));
            consumer.deriveLogicalPropertyItself();
            consumer.setStatistics(Statistics.builder().setOutputRowCount(100)
                    .addColumnStatistic(key, keyStatistic).build());
            MultiJoinNode region = region(consumer, key, scan(rightKey, known, rightOther, known, 100), rightKey);

            assertEquals(keyStatistic == unknown, region.hasUnknownJoinColumnStatistics(optimizer));
        }
    }

    // A table without statistics deep below the atom shows up in the join column the atom outputs.
    @Test
    void tableWithoutStatisticsBelowFilterAndProjectMakesTheRegionUnknown() {
        for (ColumnStatistic keyStatistic : List.of(known, unknown)) {
            ColumnRefOperator key = column("key");
            ColumnRefOperator other = column("other");
            ColumnRefOperator rightKey = column("right_key");
            ColumnRefOperator rightOther = column("right_other");
            OptExpression scan = scan(key, keyStatistic, other, known, 100);
            OptExpression filter = OptExpression.create(new LogicalFilterOperator(
                    new BinaryPredicateOperator(BinaryType.GT, other, ConstantOperator.createInt(10))), scan);
            filter.deriveLogicalPropertyItself();
            OptExpression atom = OptExpression.create(new LogicalProjectOperator(Map.of(key, key)), filter);
            atom.deriveLogicalPropertyItself();
            MultiJoinNode region = region(atom, key, scan(rightKey, known, rightOther, known, 100), rightKey);

            assertEquals(keyStatistic == unknown, region.hasUnknownJoinColumnStatistics(optimizer));
        }
    }

    // A join on an expression of a column without statistics has no statistic to estimate the join with.
    @Test
    void joinOnAnExpressionOfAColumnWithoutStatisticsStaysUnknown() {
        for (ColumnStatistic keyStatistic : List.of(known, unknown)) {
            ColumnRefOperator key = column("key");
            ColumnRefOperator other = column("other");
            ColumnRefOperator expression = column("expression");
            ColumnRefOperator rightKey = column("right_key");
            ColumnRefOperator rightOther = column("right_other");
            OptExpression scan = scan(key, keyStatistic, other, known, 100);
            ScalarOperator abs = new CallOperator("abs", IntegerType.INT, List.of(key));
            OptExpression atom = OptExpression.create(new LogicalProjectOperator(Map.of(expression, abs)), scan);
            atom.deriveLogicalPropertyItself();
            MultiJoinNode region = region(atom, expression, scan(rightKey, known, rightOther, known, 100), rightKey);

            assertEquals(keyStatistic == unknown, region.hasUnknownJoinColumnStatistics(optimizer));
        }
    }
}
