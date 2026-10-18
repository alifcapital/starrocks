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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.OptimizerOptions;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.NullType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

/**
 * The statistics of the columns of a VALUES operator are read from the constants of its rows: the number of distinct
 * values, the share of NULL, the range and the size of a value. A column that the constants cannot describe stays
 * unknown.
 */
public class ValuesStatisticsTest {
    private static final double NEGATIVE_INFINITY = Double.NEGATIVE_INFINITY;
    private static final double POSITIVE_INFINITY = Double.POSITIVE_INFINITY;

    private static ColumnRefOperator column(int id, Type type) {
        return new ColumnRefOperator(id, type, "c" + id, true);
    }

    private static List<ScalarOperator> row(ScalarOperator... fields) {
        return List.of(fields);
    }

    private static List<List<ScalarOperator>> rows(List<ScalarOperator> column) {
        List<List<ScalarOperator>> rows = new ArrayList<>();
        for (ScalarOperator field : column) {
            rows.add(List.of(field));
        }
        return rows;
    }

    private static ConstantOperator nan(Type type) {
        // createDouble refuses NaN and infinity, which the operator itself can hold.
        return new ConstantOperator(Double.NaN, type);
    }

    private static Statistics derive(Operator values) {
        ExpressionContext context = new ExpressionContext(OptExpression.create(values));
        ColumnRefFactory factory = new ColumnRefFactory();
        new StatisticsCalculator(context, factory, OptimizerFactory.mockContext(factory)).estimatorStats();
        return context.getStatistics();
    }

    // The statistics of one column as the logical and the physical operator derive them, which must agree.
    private static ColumnStatistic derive(Type type, List<ScalarOperator> fields) {
        ColumnRefOperator column = column(1, type);
        List<List<ScalarOperator>> rows = rows(fields);
        ColumnStatistic logical = derive(new LogicalValuesOperator(List.of(column), rows)).getColumnStatistic(column);
        ColumnStatistic physical = derive(
                new PhysicalValuesOperator(List.of(column), rows, -1, null, null)).getColumnStatistic(column);
        Assertions.assertEquals(logical.toString(), physical.toString());
        return logical;
    }

    @Test
    public void testRowsGiveDistinctValuesNullShareRangeAndSize() {
        ColumnStatistic statistic = derive(IntegerType.BIGINT, List.of(
                ConstantOperator.createBigint(101), ConstantOperator.createBigint(102),
                ConstantOperator.createBigint(101), ConstantOperator.createNull(IntegerType.BIGINT),
                ConstantOperator.createBigint(-7), ConstantOperator.createBigint(102)));
        Assertions.assertFalse(statistic.isUnknown());
        Assertions.assertEquals(3, statistic.getDistinctValuesCount());
        Assertions.assertEquals(1.0 / 6, statistic.getNullsFraction(), 1e-12);
        Assertions.assertEquals(-7, statistic.getMinValue());
        Assertions.assertEquals(102, statistic.getMaxValue());
        Assertions.assertEquals(IntegerType.BIGINT.getTypeSize(), statistic.getAverageRowSize());
        Assertions.assertNull(statistic.getHistogram());
    }

    @Test
    public void testNullIsNotADistinctValue() {
        ColumnStatistic statistic = derive(IntegerType.INT, List.of(ConstantOperator.createInt(1),
                ConstantOperator.createNull(IntegerType.INT), ConstantOperator.createNull(IntegerType.INT)));
        Assertions.assertEquals(1, statistic.getDistinctValuesCount());
        Assertions.assertEquals(2.0 / 3, statistic.getNullsFraction(), 1e-12);
    }

    @Test
    public void testColumnOfNullsOfARealTypeHasNoValues() {
        ColumnStatistic statistic = derive(IntegerType.INT, List.of(ConstantOperator.createNull(IntegerType.INT)));
        Assertions.assertFalse(statistic.isUnknown());
        Assertions.assertEquals(0, statistic.getDistinctValuesCount());
        Assertions.assertEquals(1.0, statistic.getNullsFraction());
        Assertions.assertEquals(NEGATIVE_INFINITY, statistic.getMinValue());
        Assertions.assertEquals(POSITIVE_INFINITY, statistic.getMaxValue());
        Assertions.assertEquals(IntegerType.INT.getTypeSize(), statistic.getAverageRowSize());
    }

    @Test
    public void testColumnOfTheNullTypeStaysUnknown() {
        // A query without FROM and a table function read such a column, and several rules test for unknown
        // statistics to keep the plan of that query.
        ColumnStatistic statistic = derive(NullType.NULL, List.of(ConstantOperator.createNull(NullType.NULL)));
        Assertions.assertTrue(statistic.isUnknown());
        statistic = derive(NullType.NULL, List.of(ConstantOperator.createNull(NullType.NULL),
                ConstantOperator.createNull(NullType.NULL)));
        Assertions.assertTrue(statistic.isUnknown());
    }

    @Test
    public void testEmptyValuesStaysUnknown() {
        ColumnRefOperator column = column(1, IntegerType.INT);
        Statistics statistics = derive(new LogicalValuesOperator(List.of(column)));
        Assertions.assertTrue(statistics.getColumnStatistic(column).isUnknown());
    }

    @Test
    public void testColumnsAreReadIndependently() {
        ColumnRefOperator known = column(1, IntegerType.INT);
        ColumnRefOperator nulls = column(2, NullType.NULL);
        ColumnRefOperator computed = column(3, FloatType.DOUBLE);
        List<List<ScalarOperator>> rows = List.of(
                row(ConstantOperator.createInt(1), ConstantOperator.createNull(NullType.NULL),
                        new CallOperator("rand", FloatType.DOUBLE, List.of())),
                row(ConstantOperator.createInt(2), ConstantOperator.createNull(NullType.NULL),
                        ConstantOperator.createDouble(0.5)));
        Statistics statistics = derive(new LogicalValuesOperator(List.of(known, nulls, computed), rows));
        Assertions.assertEquals(2, statistics.getOutputRowCount());
        Assertions.assertEquals(2, statistics.getColumnStatistic(known).getDistinctValuesCount());
        Assertions.assertTrue(statistics.getColumnStatistic(nulls).isUnknown());
        Assertions.assertTrue(statistics.getColumnStatistic(computed).isUnknown());
    }

    @Test
    public void testFieldThatIsNotAConstantLeavesTheColumnUnknown() {
        // rand() is not folded, and the other fields do not tell what it returns.
        Assertions.assertTrue(derive(FloatType.DOUBLE, List.of(ConstantOperator.createDouble(1),
                new CallOperator("rand", FloatType.DOUBLE, List.of()), ConstantOperator.createDouble(2))).isUnknown());
        Assertions.assertTrue(derive(IntegerType.INT, List.of(ConstantOperator.createInt(1),
                column(9, IntegerType.INT))).isUnknown());
    }

    @Test
    public void testConstantOfAnotherTypeIsCastToTheTypeOfTheColumn() {
        // 5 as INT and as BIGINT is one value of a BIGINT column, and so is 5 written as a string.
        ColumnStatistic statistic = derive(IntegerType.BIGINT, List.of(ConstantOperator.createInt(5),
                ConstantOperator.createBigint(5), ConstantOperator.createVarchar("7"),
                ConstantOperator.createBigint(5)));
        Assertions.assertFalse(statistic.isUnknown());
        Assertions.assertEquals(2, statistic.getDistinctValuesCount());
        Assertions.assertEquals(5, statistic.getMinValue());
        Assertions.assertEquals(7, statistic.getMaxValue());
    }

    @Test
    public void testConstantThatCannotBeCastLeavesTheColumnUnknown() {
        Assertions.assertTrue(derive(IntegerType.INT, List.of(ConstantOperator.createInt(1),
                ConstantOperator.createVarchar("abc"))).isUnknown());
        // A value that does not fit the type is cast to NULL, and a cast that fails at run time can differ.
        Type decimal = TypeFactory.createDecimalV3NarrowestType(5, 2);
        Assertions.assertTrue(derive(decimal, List.of(ConstantOperator.createDecimal(new BigDecimal("1.50"), decimal),
                ConstantOperator.createVarchar("999999999"))).isUnknown());
    }

    @Test
    public void testDecimalIsComparedWithoutItsTrailingZeros() {
        Type decimal = TypeFactory.createDecimalV3NarrowestType(10, 2);
        ColumnStatistic statistic = derive(decimal, List.of(
                ConstantOperator.createDecimal(new BigDecimal("1.0"), decimal),
                ConstantOperator.createDecimal(new BigDecimal("1.00"), decimal),
                ConstantOperator.createDecimal(new BigDecimal("0.00"), decimal),
                ConstantOperator.createDecimal(new BigDecimal("0"), decimal),
                ConstantOperator.createDecimal(new BigDecimal("2.50"), decimal)));
        Assertions.assertEquals(3, statistic.getDistinctValuesCount());
        Assertions.assertEquals(0, statistic.getMinValue());
        Assertions.assertEquals(2.5, statistic.getMaxValue());
    }

    @Test
    public void testNegativeZeroAndZeroAreOneValue() {
        ColumnStatistic statistic = derive(FloatType.DOUBLE, List.of(ConstantOperator.createDouble(-0.0),
                ConstantOperator.createDouble(0.0), ConstantOperator.createDouble(1.5)));
        Assertions.assertEquals(2, statistic.getDistinctValuesCount());
        statistic = derive(FloatType.FLOAT, List.of(ConstantOperator.createFloat(-0.0),
                ConstantOperator.createFloat(0.0)));
        Assertions.assertEquals(1, statistic.getDistinctValuesCount());
    }

    @Test
    public void testAllNaNAreOneValueAndMarkTheRange() {
        ColumnStatistic statistic = derive(FloatType.DOUBLE, List.of(nan(FloatType.DOUBLE), ConstantOperator.createDouble(1),
                nan(FloatType.DOUBLE), ConstantOperator.createDouble(3)));
        // NaN and 1 and 3.
        Assertions.assertEquals(3, statistic.getDistinctValuesCount());
        // NaN is no part of the range, and the range is marked as the statistics of a column with NaN are.
        Assertions.assertTrue(statistic.hasNaNValue());
        Assertions.assertFalse(derive(FloatType.DOUBLE, List.of(ConstantOperator.createDouble(1),
                ConstantOperator.createDouble(3))).hasNaNValue());
    }

    @Test
    public void testInfinityIsAValueOfTheRange() {
        ColumnStatistic statistic = derive(FloatType.DOUBLE, List.of(
                new ConstantOperator(POSITIVE_INFINITY, FloatType.DOUBLE),
                new ConstantOperator(NEGATIVE_INFINITY, FloatType.DOUBLE), ConstantOperator.createDouble(1)));
        Assertions.assertEquals(3, statistic.getDistinctValuesCount());
        Assertions.assertEquals(NEGATIVE_INFINITY, statistic.getMinValue());
        Assertions.assertEquals(POSITIVE_INFINITY, statistic.getMaxValue());
        Assertions.assertFalse(statistic.hasNaNValue());
    }

    @Test
    public void testStringsDifferByCaseAndHaveNoRange() {
        ColumnStatistic statistic = derive(VarcharType.VARCHAR, List.of(ConstantOperator.createVarchar("A"),
                ConstantOperator.createVarchar("a"), ConstantOperator.createVarchar("A"),
                ConstantOperator.createNull(VarcharType.VARCHAR), ConstantOperator.createVarchar("abcdef")));
        Assertions.assertEquals(3, statistic.getDistinctValuesCount());
        Assertions.assertEquals(0.2, statistic.getNullsFraction(), 1e-12);
        Assertions.assertEquals(NEGATIVE_INFINITY, statistic.getMinValue());
        Assertions.assertEquals(POSITIVE_INFINITY, statistic.getMaxValue());
        // The mean length of the 4 values that are not NULL: (1 + 1 + 1 + 6) / 4.
        Assertions.assertEquals(2.25, statistic.getAverageRowSize(), 1e-12);
    }

    @Test
    public void testStringsOfDifferentLengthTypesAreCompared() {
        ColumnStatistic statistic = derive(VarcharType.VARCHAR, List.of(
                ConstantOperator.createChar("ab", TypeFactory.createVarcharType(2)),
                ConstantOperator.createChar("ab", TypeFactory.createVarcharType(10)),
                ConstantOperator.createChar("cd", TypeFactory.createVarcharType(10))));
        Assertions.assertEquals(2, statistic.getDistinctValuesCount());
    }

    @Test
    public void testBooleanRange() {
        ColumnStatistic statistic = derive(BooleanType.BOOLEAN, List.of(ConstantOperator.createBoolean(true),
                ConstantOperator.createBoolean(false), ConstantOperator.createBoolean(true)));
        Assertions.assertEquals(2, statistic.getDistinctValuesCount());
        Assertions.assertEquals(0, statistic.getMinValue());
        Assertions.assertEquals(1, statistic.getMaxValue());
        statistic = derive(BooleanType.BOOLEAN, List.of(ConstantOperator.createBoolean(true)));
        Assertions.assertEquals(1, statistic.getDistinctValuesCount());
        Assertions.assertEquals(1, statistic.getMinValue());
        Assertions.assertEquals(1, statistic.getMaxValue());
    }

    @Test
    public void testDateAndDatetimeRange() {
        LocalDateTime first = LocalDateTime.of(2024, 3, 1, 0, 0);
        LocalDateTime last = LocalDateTime.of(2025, 1, 15, 0, 0);
        ColumnStatistic statistic = derive(DateType.DATE, List.of(ConstantOperator.createDate(last),
                ConstantOperator.createDate(first), ConstantOperator.createDate(last)));
        Assertions.assertEquals(2, statistic.getDistinctValuesCount());
        Assertions.assertEquals(Utils.getLongFromDateTime(first), statistic.getMinValue());
        Assertions.assertEquals(Utils.getLongFromDateTime(last), statistic.getMaxValue());

        LocalDateTime second = LocalDateTime.of(2024, 3, 1, 10, 30, 15);
        statistic = derive(DateType.DATETIME, List.of(ConstantOperator.createDatetime(second),
                ConstantOperator.createDatetime(first), ConstantOperator.createNull(DateType.DATETIME)));
        Assertions.assertEquals(2, statistic.getDistinctValuesCount());
        Assertions.assertEquals(1.0 / 3, statistic.getNullsFraction(), 1e-12);
        Assertions.assertEquals(Utils.getLongFromDateTime(first), statistic.getMinValue());
        Assertions.assertEquals(Utils.getLongFromDateTime(second), statistic.getMaxValue());

        // Two datetimes that differ below the second are two values, although the range cannot tell them apart.
        statistic = derive(DateType.DATETIME, List.of(
                ConstantOperator.createDatetime(second.withNano(100_000_000)),
                ConstantOperator.createDatetime(second.withNano(200_000_000))));
        Assertions.assertEquals(2, statistic.getDistinctValuesCount());
    }

    @Test
    public void testGroupByAColumnOfOneNullIsOneGroup() {
        // min() or max() of a partition column over a table without values is a VALUES of one NULL of the type of
        // the column. Its NULL is the only group, not a group besides the distinct values, of which there are none.
        ColumnRefOperator column = column(1, DateType.DATE);
        OptExpression values = OptExpression.create(new LogicalValuesOperator(List.of(column),
                List.of(row(ConstantOperator.createNull(DateType.DATE)))));
        values.setStatistics(derive(values.getOp()));
        ColumnStatistic key = values.getStatistics().getColumnStatistic(column);
        Assertions.assertEquals(1.0, key.getNullsFraction());
        Assertions.assertEquals(0, key.getDistinctValuesCount());

        ExpressionContext context = new ExpressionContext(OptExpression.create(
                new LogicalAggregationOperator(AggType.GLOBAL, List.of(column), Map.of()), values));
        ColumnRefFactory factory = new ColumnRefFactory();
        new StatisticsCalculator(context, factory, OptimizerFactory.mockContext(factory)).estimatorStats();
        Assertions.assertEquals(1, context.getStatistics().getOutputRowCount());
    }

    // Each of the values twice, with NULL between them, so the repeats come after the sketch has replaced the keys.
    private static List<ScalarOperator> twice(int distinct, IntFunction<ScalarOperator> value, Type type) {
        List<ScalarOperator> fields = new ArrayList<>();
        for (int pass = 0; pass < 2; pass++) {
            for (int i = 0; i < distinct; i++) {
                fields.add(value.apply(i));
            }
            fields.add(ConstantOperator.createNull(type));
        }
        return fields;
    }

    @Test
    public void testCountIsExactUpToTheLimit() {
        int limit = ValuesStatistics.EXACT_LIMIT;
        Assertions.assertEquals(limit, derive(IntegerType.BIGINT,
                twice(limit, i -> ConstantOperator.createBigint(i), IntegerType.BIGINT)).getDistinctValuesCount());
        Assertions.assertEquals(limit, derive(VarcharType.VARCHAR,
                twice(limit, i -> ConstantOperator.createVarchar("v" + i), VarcharType.VARCHAR))
                .getDistinctValuesCount());
    }

    @Test
    public void testCountDoesNotJumpAtTheLimit() {
        // Just above the limit the sketch holds the keys of the exact count, and the count is not below them.
        for (int distinct = ValuesStatistics.EXACT_LIMIT + 1; distinct <= ValuesStatistics.EXACT_LIMIT + 50; distinct++) {
            double count = derive(IntegerType.BIGINT, twice(distinct, i -> ConstantOperator.createBigint(i),
                    IntegerType.BIGINT)).getDistinctValuesCount();
            Assertions.assertTrue(count >= ValuesStatistics.EXACT_LIMIT + 1 && count <= distinct * 1.02,
                    distinct + " distinct values gave " + count);
        }
    }

    @Test
    public void testManyDistinctValuesAreCountedWithASketch() {
        int distinct = 50_000;
        assertEstimate(distinct, derive(IntegerType.BIGINT,
                twice(distinct, i -> ConstantOperator.createBigint(i * 7919L), IntegerType.BIGINT)));
        assertEstimate(distinct, derive(IntegerType.INT,
                twice(distinct, i -> ConstantOperator.createInt(i), IntegerType.INT)));
        assertEstimate(distinct, derive(VarcharType.VARCHAR,
                twice(distinct, i -> ConstantOperator.createVarchar("s" + i + "_x"), VarcharType.VARCHAR)));
        assertEstimate(distinct, derive(FloatType.DOUBLE,
                twice(distinct, i -> ConstantOperator.createDouble(i / 8.0), FloatType.DOUBLE)));
        Type decimal = TypeFactory.createDecimalV3NarrowestType(18, 3);
        assertEstimate(distinct, derive(decimal, twice(distinct,
                i -> ConstantOperator.createDecimal(new BigDecimal(i).movePointLeft(3), decimal), decimal)));
        LocalDateTime start = LocalDateTime.of(2000, 1, 1, 0, 0);
        assertEstimate(distinct, derive(DateType.DATETIME, twice(distinct,
                i -> ConstantOperator.createDatetime(start.plusSeconds(i * 61L)), DateType.DATETIME)));
    }

    @Test
    public void testValuesKeptBeforeTheLimitAreCountedByTheSketch() {
        // Each value comes once, so the sketch counts the first values only if the exact keys were moved into it.
        int distinct = 10_000;
        List<ScalarOperator> fields = new ArrayList<>();
        for (int i = 0; i < distinct; i++) {
            fields.add(ConstantOperator.createBigint(i));
        }
        Assertions.assertEquals(distinct, derive(IntegerType.BIGINT, fields).getDistinctValuesCount(), distinct * 0.03);
    }

    // The sketch is within a few percent of the count, and the other statistics do not depend on it.
    private static void assertEstimate(int distinct, ColumnStatistic statistic) {
        Assertions.assertEquals(distinct, statistic.getDistinctValuesCount(), distinct * 0.03);
        Assertions.assertEquals(2.0 / (2 * distinct + 2), statistic.getNullsFraction(), 1e-12);
    }

    @Test
    public void testValuesWithoutReaderKeepTheirUnknownStatistics() {
        ColumnRefOperator column = column(1, IntegerType.INT);
        List<List<ScalarOperator>> rows = rows(List.of(ConstantOperator.createInt(1), ConstantOperator.createInt(2)));
        for (Operator values : List.of(new LogicalValuesOperator(List.of(column), rows),
                new PhysicalValuesOperator(List.of(column), rows, -1, null, null))) {
            ExpressionContext context = new ExpressionContext(OptExpression.create(values));
            ColumnRefFactory factory = new ColumnRefFactory();
            OptimizerContext optimizer = OptimizerFactory.mockContext(factory);
            optimizer.setValuesStatisticsRead(false);
            new StatisticsCalculator(context, factory, optimizer).estimatorStats();
            Assertions.assertTrue(context.getStatistics().getColumnStatistic(column).isUnknown());
            Assertions.assertEquals(2, context.getStatistics().getOutputRowCount());
        }
    }

    @Test
    public void testContextComputesUntilQueryOptimizerSaysOtherwise() {
        // An optimizer that does not read the tree before it derives statistics does not set the flag.
        ColumnRefFactory factory = new ColumnRefFactory();
        Assertions.assertTrue(OptimizerFactory.mockContext(factory).isValuesStatisticsRead());
        for (OptimizerOptions options : List.of(OptimizerOptions.defaultOpt(), OptimizerOptions.newRuleBaseOpt(),
                OptimizerOptions.newShortCircuitOpt(),
                new OptimizerOptions(OptimizerOptions.OptimizerStrategy.BASELINE_PLAN))) {
            Assertions.assertTrue(OptimizerFactory.mockContext(new ConnectContext(), factory, options)
                    .isValuesStatisticsRead());
        }
    }

    @Test
    public void testStatisticsAreKeptForTheRowsOfTheQuery() {
        ColumnRefOperator column = column(1, IntegerType.INT);
        List<List<ScalarOperator>> rows = rows(List.of(ConstantOperator.createInt(1), ConstantOperator.createInt(2)));
        ExpressionStatisticsCache cache = new ExpressionStatisticsCache();
        List<ColumnStatistic> first = ValuesStatistics.of(List.of(column), rows, cache);
        Assertions.assertSame(first, ValuesStatistics.of(List.of(column), rows, cache));
        // Equal rows in another list are not the rows of the operator that was derived.
        Assertions.assertNotSame(first, ValuesStatistics.of(List.of(column), rows(List.of(
                ConstantOperator.createInt(1), ConstantOperator.createInt(2))), cache));
        // The columns of another type read the same rows again.
        Assertions.assertNotSame(first, ValuesStatistics.of(List.of(column(1, IntegerType.BIGINT)), rows, cache));
        // Without the cache of a query the statistics are the same.
        List<ColumnStatistic> uncached = ValuesStatistics.of(List.of(column), rows, null);
        Assertions.assertEquals(first.toString(), uncached.toString());
    }
}
