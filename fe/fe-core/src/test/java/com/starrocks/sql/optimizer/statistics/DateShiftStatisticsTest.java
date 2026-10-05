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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

public class DateShiftStatisticsTest {
    private static final double DAY = 24.0 * 3600;
    // A date stored as the number of days since 1970-01-01, as in many Iceberg tables.
    private static final ColumnRefOperator EPOCH_DAY = new ColumnRefOperator(1, IntegerType.INT, "date", true);
    private static final LocalDateTime EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0, 0);

    private static Statistics input() {
        return Statistics.builder().setOutputRowCount(1928199653)
                .addColumnStatistic(EPOCH_DAY, new ColumnStatistic(17197, 20727, 0, 4, 805)).build();
    }

    private static ColumnStatistic shiftEpochByDays(String function) {
        CallOperator call = new CallOperator(function, DateType.DATETIME,
                List.of(ConstantOperator.createDatetime(EPOCH), EPOCH_DAY));
        return ExpressionStatisticCalculator.calculate(call, input());
    }

    @Test
    public void testAddingDaysToADatetimeAddsSecondsOfTheseDays() {
        double epoch = Utils.getLongFromDateTime(EPOCH);
        for (String function : List.of(FunctionSet.DAYS_ADD, FunctionSet.DATE_ADD)) {
            ColumnStatistic result = shiftEpochByDays(function);
            Assertions.assertEquals(epoch + 17197 * DAY, result.getMinValue(), function);
            Assertions.assertEquals(epoch + 20727 * DAY, result.getMaxValue(), function);
        }
    }

    @Test
    public void testSubtractingDaysFromADatetimeSubtractsSecondsOfTheseDays() {
        double epoch = Utils.getLongFromDateTime(EPOCH);
        for (String function : List.of(FunctionSet.DAYS_SUB, FunctionSet.DATE_SUB)) {
            ColumnStatistic result = shiftEpochByDays(function);
            Assertions.assertEquals(epoch - 20727 * DAY, result.getMinValue(), function);
            Assertions.assertEquals(epoch - 17197 * DAY, result.getMaxValue(), function);
        }
    }

    @Test
    public void testAddingNumbersIsNotScaled() {
        CallOperator call = new CallOperator(FunctionSet.ADD, IntegerType.BIGINT,
                List.of(ConstantOperator.createInt(10), EPOCH_DAY));
        ColumnStatistic result = ExpressionStatisticCalculator.calculate(call, input());
        Assertions.assertEquals(17207, result.getMinValue());
        Assertions.assertEquals(20737, result.getMaxValue());
    }
}
