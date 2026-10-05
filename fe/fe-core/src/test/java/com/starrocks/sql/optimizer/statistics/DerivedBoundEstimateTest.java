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

import com.google.common.collect.ImmutableList;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.MonotonicFilterDerivation;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

// A comparison of a function of a column with a constant is estimated by the bound on the column that follows from
// it, and a bound that a scan holds next to the comparison is never estimated.
public class DerivedBoundEstimateTest {
    private static final double ROWS = 1928199653;
    // A date stored as the number of days since 1970-01-01, as in many Iceberg tables.
    private static final ColumnRefOperator DAYS = new ColumnRefOperator(1, IntegerType.INT, "date", true);
    private static final ColumnRefOperator OTHER = new ColumnRefOperator(2, IntegerType.INT, "other", true);
    private static final LocalDateTime EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0);

    @BeforeEach
    public void setUp() {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("+00:00");
        ctx.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static Statistics table() {
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(DAYS, new ColumnStatistic(17197, 20727, 0, 4, 805))
                .addColumnStatistic(OTHER, new ColumnStatistic(0, 9, 0, 4, 10))
                .build();
    }

    private static CallOperator shifted() {
        return new CallOperator("days_add", DateType.DATETIME,
                ImmutableList.of(ConstantOperator.createDatetime(EPOCH), DAYS));
    }

    // days_add('1970-01-01 00:00:00', date) >= '2025-01-01 00:00:00'
    private static ScalarOperator fromYear2025() {
        return new BinaryPredicateOperator(BinaryType.GE, shifted(),
                ConstantOperator.createDatetime(LocalDateTime.of(2025, 1, 1, 0, 0)));
    }

    private static double rows(ScalarOperator predicate, Statistics statistics) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics).getOutputRowCount();
    }

    @Test
    public void testComparisonOnAFunctionIsEstimatedByTheColumn() {
        // 2025-01-01 is day 20089: date >= 20089 keeps (20727 - 20089) / (20727 - 17197) of the rows. A compensation
        // predicate of a materialized view rewrite holds the comparison without any bound and gets this estimate.
        double expected = ROWS * (20727 - 20089) / (20727 - 17197);
        Assertions.assertEquals(expected, rows(fromYear2025(), table()), expected * 0.01);
        ScalarOperator bound = new BinaryPredicateOperator(BinaryType.GE, DAYS, ConstantOperator.createInt(20089));
        Assertions.assertEquals(rows(bound, table()), rows(fromYear2025(), table()), expected * 0.01);
    }

    @Test
    public void testBoundHeldByTheScanIsNotEstimated() {
        ScalarOperator original = fromYear2025();
        ScalarOperator withBound = MonotonicFilterDerivation.addScanBounds(original);
        List<ScalarOperator> bounds = Utils.extractConjuncts(withBound).stream()
                .filter(conjunct -> conjunct != original).toList();
        Assertions.assertEquals(1, bounds.size());
        Assertions.assertTrue(bounds.get(0).isNotEvalEstimate());
        Assertions.assertEquals(rows(original, table()), rows(withBound, table()), 1);
    }

    @Test
    public void testEqualityBoundIsNotCountedTwice() {
        // days_add('1970-01-01', date) = '2024-01-11' gives the bound date = 19733. Two equalities on columns go
        // through the estimate with multi-column statistics, which must not take the bound for a third one.
        ScalarOperator original = new BinaryPredicateOperator(BinaryType.EQ, shifted(),
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 11, 0, 0)));
        ScalarOperator other = new BinaryPredicateOperator(BinaryType.EQ, OTHER, ConstantOperator.createInt(5));
        ScalarOperator predicate = Utils.compoundAnd(original, other);
        ScalarOperator withBound = MonotonicFilterDerivation.addScanBounds(predicate);
        Assertions.assertEquals(3, Utils.extractConjuncts(withBound).size());
        Assertions.assertEquals(rows(predicate, table()), rows(withBound, table()), 1);
    }

    @Test
    public void testFilterAboveAJoinIsEstimatedByTheColumn() {
        // Above a join the column keeps its statistics, narrowed by the join
        Statistics joined = Statistics.builder().setOutputRowCount(5e9)
                .addColumnStatistic(DAYS, new ColumnStatistic(20000, 20727, 0, 4, 700)).build();
        double expected = 5e9 * (20727 - 20089) / (20727 - 20000);
        Assertions.assertEquals(expected, rows(fromYear2025(), joined), expected * 0.01);
    }

    @Test
    public void testNecessaryBoundTakesTheLowerEstimate() {
        // The shift can overflow to NULL, so its bound only follows from the comparison. Here the column keeps all
        // of its listed rows on one value, while the shifted dates spread evenly over 10 values: the comparison on
        // the function estimates fewer rows than the bound, and we take that estimate.
        Statistics skewed = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(DAYS, ColumnStatistic.builder().setMinValue(19724).setMaxValue(19733)
                        .setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(10)
                        .setHistogram(new Histogram(Map.of("19733", 900L))).build())
                .build();
        ScalarOperator original = new BinaryPredicateOperator(BinaryType.EQ, shifted(),
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 11, 0, 0)));
        ScalarOperator bound = new BinaryPredicateOperator(BinaryType.EQ, DAYS, ConstantOperator.createInt(19733));
        double byBound = rows(bound, skewed);
        double byComparison = rows(original, skewed);
        Assertions.assertTrue(byBound > 500, String.valueOf(byBound));
        Assertions.assertEquals(100, byComparison, 10);
    }
}
