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

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * The dates of the times of a column are no more than the days from its first time to its last one, whatever the number
 * of its rows or times is. The table has 285,000,000 rows of strings of times from 2018-08-15 to 2026-10-07.
 */
public class DateOfTimeStatisticsTest {
    private static final double ROWS = 285_000_000;
    private static final LocalDateTime FIRST = LocalDateTime.of(2018, 8, 15, 13, 16, 23);
    private static final LocalDateTime LAST = LocalDateTime.of(2026, 10, 7, 8, 58, 22);
    // 2018-08-15 to 2026-10-07, both days counted; the five hours shift neither the first nor the last time to
    // another day
    private static final long DAYS = ChronoUnit.DAYS.between(FIRST.toLocalDate(), LAST.toLocalDate()) + 1;

    private TimeZone savedZone;
    private ColumnRefFactory factory;
    private ColumnRefOperator created;
    private ColumnRefOperator key;

    @BeforeEach
    public void setUp() {
        savedZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        UtFrameUtils.createDefaultCtx();
        ConnectContext.get().getSessionVariable().setTimeZone("+00:00");
        factory = new ColumnRefFactory();
        created = factory.create("created_at", VarcharType.VARCHAR, true);
        key = factory.create("day", DateType.DATE, true);
    }

    @AfterEach
    public void tearDown() {
        TimeZone.setDefault(savedZone);
        ConnectContext.remove();
    }

    private Statistics table() {
        return Statistics.builder().setOutputRowCount(ROWS).addColumnStatistic(created, ColumnStatistic.builder()
                .setNullsFraction(0).setAverageRowSize(27).setDistinctValuesCount(ROWS * 0.98)
                .setStringBounds(new StringBounds(FIRST.toString().replace('T', ' '),
                        LAST.toString().replace('T', ' '), StringBounds.WHOLE, false)).build()).build();
    }

    // A call as the analyzer builds it, with its function, see MonotonicImageStatisticsTest
    private static CallOperator call(String function, Type type, List<ScalarOperator> arguments) {
        Type[] argumentTypes = arguments.stream().map(ScalarOperator::getType).toArray(Type[]::new);
        return new CallOperator(function, type, arguments,
                new Function(new FunctionName(function), argumentTypes, type, false));
    }

    private ScalarOperator shiftedTime() {
        return call("hours_add", DateType.DATETIME,
                List.of(new CastOperator(DateType.DATETIME, created), ConstantOperator.createInt(5)));
    }

    private static double seconds(LocalDate day) {
        return Utils.getLongFromDateTime(day.atStartOfDay());
    }

    @Test
    public void testDateOfAShiftedTimeHasTheDaysOfItsRange() {
        for (String function : List.of("date", "to_date")) {
            ColumnStatistic date = ExpressionStatisticCalculator.calculate(
                    call(function, DateType.DATE, List.of(shiftedTime())), table());
            Assertions.assertEquals(seconds(FIRST.toLocalDate()), date.getMinValue(), function);
            Assertions.assertEquals(seconds(LAST.toLocalDate()), date.getMaxValue(), function);
            Assertions.assertEquals(DAYS, date.getDistinctValuesCount(), function);
        }
        Assertions.assertEquals(2976, DAYS);
    }

    @Test
    public void testDateOfAColumnOfTimesHasTheDaysOfItsRangeOrItsOwnValues() {
        ColumnRefOperator time = factory.create("ts", DateType.DATETIME, true);
        LocalDateTime min = LocalDateTime.of(2024, 1, 10, 6, 0);
        LocalDateTime max = LocalDateTime.of(2024, 3, 10, 18, 0);
        for (double distinct : new double[] {10, 61, 500, 1_000_000}) {
            Statistics statistics = Statistics.builder().setOutputRowCount(1_000_000).addColumnStatistic(time,
                    new ColumnStatistic(Utils.getLongFromDateTime(min), Utils.getLongFromDateTime(max), 0, 8,
                            distinct)).build();
            ColumnStatistic date = ExpressionStatisticCalculator.calculate(
                    call("date", DateType.DATE, List.of(time)), statistics);
            // 2024-01-10 to 2024-03-10: 22 + 29 + 10 days
            Assertions.assertEquals(Math.min(distinct, 61), date.getDistinctValuesCount(), "" + distinct);
        }
    }

    @Test
    public void testDateIsNoMoreThanTheRowsOfTheInput() {
        Statistics statistics = Statistics.builder().setOutputRowCount(5).addColumnStatistic(created,
                ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(27).setDistinctValuesCount(5)
                        .setStringBounds(new StringBounds(FIRST.toString().replace('T', ' '),
                                LAST.toString().replace('T', ' '), StringBounds.WHOLE, false)).build()).build();
        ColumnStatistic date = ExpressionStatisticCalculator.calculate(
                call("date", DateType.DATE, List.of(shiftedTime())), statistics);
        Assertions.assertEquals(5, date.getDistinctValuesCount());
    }

    @Test
    public void testDateOfAnArgumentWithoutARangeHasTheDistinctValuesOfTheArgument() {
        Statistics statistics = Statistics.builder().setOutputRowCount(ROWS).addColumnStatistic(created,
                ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(27).setDistinctValuesCount(1000)
                        .build()).build();
        ColumnStatistic date = ExpressionStatisticCalculator.calculate(
                call("date", DateType.DATE, List.of(new CastOperator(DateType.DATETIME, created))), statistics);
        Assertions.assertEquals(1000, date.getDistinctValuesCount());
    }

    private Statistics estimate(Operator operator, Statistics input) {
        Group child = new Group(0);
        child.setStatistics(input);
        GroupExpression expression = new GroupExpression(operator, List.of(child));
        expression.setGroup(new Group(1));
        ExpressionContext context = new ExpressionContext(expression);
        new StatisticsCalculator(context, factory, OptimizerFactory.mockContext(ConnectContext.get(), factory))
                .estimatorStats();
        return context.getStatistics();
    }

    @Test
    public void testGroupByTheDateHasOneGroupForEachDay() {
        ScalarOperator date = call("date", DateType.DATE, List.of(shiftedTime()));
        Statistics projected = estimate(new LogicalProjectOperator(Map.of(key, date, created, created)), table());
        Assertions.assertEquals(DAYS, projected.getColumnStatistic(key).getDistinctValuesCount());
        Statistics grouped = estimate(new LogicalAggregationOperator(AggType.GLOBAL, List.of(key), Map.of()),
                projected);
        Assertions.assertEquals(DAYS, grouped.getOutputRowCount());
    }
}
