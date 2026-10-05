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
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

public class MonotonicImageStatisticsTest {
    private static final ColumnRefOperator TS = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
    private static final ColumnRefOperator DAYS = new ColumnRefOperator(2, IntegerType.INT, "days", true);

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

    private static double seconds(LocalDateTime value) {
        return Utils.getLongFromDateTime(value);
    }

    private static Statistics input(LocalDateTime min, LocalDateTime max, double ndv) {
        return Statistics.builder().setOutputRowCount(1_000_000)
                .addColumnStatistic(TS, new ColumnStatistic(seconds(min), seconds(max), 0.1, 8, ndv))
                .addColumnStatistic(DAYS, new ColumnStatistic(17197, 20727, 0, 4, 805))
                .build();
    }

    // A call as the analyzer builds it, with its function: the FE folds a call with constant arguments only when
    // it has one, see ScalarOperatorEvaluator.evaluation.
    private static CallOperator call(String function, Type type, List<ScalarOperator> arguments) {
        Type[] argumentTypes = arguments.stream().map(ScalarOperator::getType).toArray(Type[]::new);
        return new CallOperator(function, type, arguments,
                new Function(new FunctionName(function), argumentTypes, type, false));
    }

    private static ColumnStatistic calculate(String function, Type type, ScalarOperator data, int amount,
                                             Statistics statistics) {
        return ExpressionStatisticCalculator.calculate(
                call(function, type, List.of(data, ConstantOperator.createInt(amount))), statistics);
    }

    @Test
    public void testShiftsByEveryUnit() {
        LocalDateTime min = LocalDateTime.of(2024, 1, 10, 6, 0);
        LocalDateTime max = LocalDateTime.of(2024, 3, 10, 18, 0);
        Statistics statistics = input(min, max, 500);
        List<Object[]> cases = List.of(
                new Object[] {"seconds_add", min.plusSeconds(7), max.plusSeconds(7)},
                new Object[] {"minutes_add", min.plusMinutes(7), max.plusMinutes(7)},
                new Object[] {"hours_add", min.plusHours(7), max.plusHours(7)},
                new Object[] {"weeks_add", min.plusWeeks(7), max.plusWeeks(7)},
                new Object[] {"hours_sub", min.minusHours(7), max.minusHours(7)},
                new Object[] {"weeks_sub", min.minusWeeks(7), max.minusWeeks(7)});
        for (Object[] c : cases) {
            String function = (String) c[0];
            ColumnStatistic result = calculate(function, DateType.DATETIME, TS, 7, statistics);
            Assertions.assertEquals(seconds((LocalDateTime) c[1]), result.getMinValue(), function);
            Assertions.assertEquals(seconds((LocalDateTime) c[2]), result.getMaxValue(), function);
            Assertions.assertEquals(500, result.getDistinctValuesCount(), function);
            Assertions.assertEquals(0.1, result.getNullsFraction(), 1e-9, function);
        }
    }

    @Test
    public void testMonthShiftsFollowTheCalendar() {
        // months_add('2024-01-31', 1) is '2024-02-29'. A month shift of a datetime can move the time of day down
        // at the end of a month, so the range covers whole days.
        Statistics statistics = input(LocalDateTime.of(2024, 1, 31, 10, 0), LocalDateTime.of(2024, 3, 31, 10, 0), 61);
        ColumnStatistic months = calculate("months_add", DateType.DATETIME, TS, 1, statistics);
        Assertions.assertEquals(seconds(LocalDateTime.of(2024, 2, 29, 0, 0)), months.getMinValue());
        Assertions.assertEquals(seconds(LocalDateTime.of(2024, 4, 30, 23, 59, 59)), months.getMaxValue());
        ColumnStatistic years = calculate("years_sub", DateType.DATETIME, TS, 1, statistics);
        Assertions.assertEquals(seconds(LocalDateTime.of(2023, 1, 31, 0, 0)), years.getMinValue());
        Assertions.assertEquals(seconds(LocalDateTime.of(2023, 3, 31, 23, 59, 59)), years.getMaxValue());
    }

    @Test
    public void testShiftOutOfTheDateRangeStaysUnknown() {
        // years_add('2024-..', 8000) is past 9999-12-31 and NULL, so the range is not computed
        Statistics statistics = input(LocalDateTime.of(2024, 1, 1, 0, 0), LocalDateTime.of(2024, 12, 31, 0, 0), 366);
        ColumnStatistic result = calculate("years_add", DateType.DATETIME, TS, 8000, statistics);
        Assertions.assertTrue(result.isUnknown() || result.isInfiniteRange(), result.toString());
    }

    @Test
    public void testArgumentThatIsAnExpression() {
        // hours_add(days_add('1970-01-01', days), 1) over days 17197 .. 20727
        CallOperator shifted = call("days_add", DateType.DATETIME,
                List.of(ConstantOperator.createDatetime(LocalDateTime.of(1970, 1, 1, 0, 0)), DAYS));
        Statistics statistics = input(LocalDateTime.of(2024, 1, 1, 0, 0), LocalDateTime.of(2024, 1, 2, 0, 0), 1);
        ColumnStatistic inner = ExpressionStatisticCalculator.calculate(shifted, statistics);
        ColumnStatistic result = calculate("hours_add", DateType.DATETIME, shifted, 1, statistics);
        Assertions.assertEquals(seconds(Utils.getDatetimeFromLong((long) inner.getMinValue()).plusHours(1)),
                result.getMinValue());
        Assertions.assertEquals(seconds(Utils.getDatetimeFromLong((long) inner.getMaxValue()).plusHours(1)),
                result.getMaxValue());
        Assertions.assertEquals(805, result.getDistinctValuesCount());
    }

    @Test
    public void testMergingFunctionHasNoMoreValuesThanDatesInItsRange() {
        // last_day maps the 366 days of 2024 to 12 dates; we expect no more distinct values than the 336 dates
        // between 2024-01-31 and 2024-12-31
        Statistics statistics = input(LocalDateTime.of(2024, 1, 1, 0, 0), LocalDateTime.of(2024, 12, 31, 0, 0), 366);
        CallOperator lastDay = call("last_day", DateType.DATE, List.of(TS));
        ColumnStatistic result = ExpressionStatisticCalculator.calculate(lastDay, statistics);
        Assertions.assertEquals(seconds(LocalDateTime.of(2024, 1, 31, 0, 0)), result.getMinValue());
        Assertions.assertEquals(seconds(LocalDateTime.of(2024, 12, 31, 0, 0)), result.getMaxValue());
        Assertions.assertEquals(336, result.getDistinctValuesCount());
    }
}
