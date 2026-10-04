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
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SourceDistinctValuesTest {
    private static final ColumnRefOperator KEY = new ColumnRefOperator(1, IntegerType.INT, "key", true);
    private static final ColumnRefOperator TEXT = new ColumnRefOperator(2, VarcharType.VARCHAR, "text", true);
    private static final ColumnRefOperator DAY = new ColumnRefOperator(3, DateType.DATE, "day", true);
    private static final ColumnRefOperator FACT_KEY = new ColumnRefOperator(4, IntegerType.INT, "fact_key", true);

    private static ColumnStatistic column(double ndv) {
        return new ColumnStatistic(1, 100000, 0, 4, ndv);
    }

    @Test
    public void testDerivedStatisticsKeepTheSourceNdv() {
        ColumnStatistic table = column(72542);
        Assertions.assertEquals(72542, table.getSourceDistinctValuesCount());
        ColumnStatistic filtered = ColumnStatistic.buildFrom(table).setDistinctValuesCount(335).build();
        Assertions.assertEquals(335, filtered.getDistinctValuesCount());
        Assertions.assertEquals(72542, filtered.getSourceDistinctValuesCount());
        // A second reduction keeps the NDV of the original column, not of the first reduction.
        ColumnStatistic twice = ColumnStatistic.buildFrom(filtered).setDistinctValuesCount(10).build();
        Assertions.assertEquals(72542, twice.getSourceDistinctValuesCount());
        // The source NDV is never below the current one, for example after a union added values.
        ColumnStatistic grown = ColumnStatistic.buildFrom(filtered).setDistinctValuesCount(100000).build();
        Assertions.assertEquals(100000, grown.getSourceDistinctValuesCount());
        Assertions.assertEquals(335, filtered.withoutSource().getSourceDistinctValuesCount());
        Assertions.assertSame(table, table.withoutSource());
        Assertions.assertEquals(filtered.toString(), filtered.withoutSource().toString());
    }

    @Test
    public void testRowCountAdjustmentKeepsTheSourceNdv() {
        Statistics statistics = Statistics.builder().setOutputRowCount(73049).addColumnStatistic(KEY, column(72542))
                .build();
        Statistics filtered = StatisticsEstimateUtils.adjustStatisticsByRowCount(statistics, 335);
        Assertions.assertEquals(335, filtered.getColumnStatistic(KEY).getDistinctValuesCount());
        Assertions.assertEquals(72542, filtered.getColumnStatistic(KEY).getSourceDistinctValuesCount());
    }

    @Test
    public void testOnlyTheColumnAndWideningCastsKeepTheSourceDomain() {
        ColumnStatistic reduced = ColumnStatistic.buildFrom(column(72542)).setDistinctValuesCount(335).build();
        Statistics input = Statistics.builder().setOutputRowCount(335).addColumnStatistic(KEY, reduced)
                .addColumnStatistic(TEXT, reduced).addColumnStatistic(DAY, reduced).build();
        List<ScalarOperator> keeping = List.of(KEY, new CastOperator(IntegerType.BIGINT, KEY),
                new CastOperator(VarcharType.VARCHAR, TEXT));
        for (ScalarOperator expression : keeping) {
            Assertions.assertEquals(72542, ExpressionStatisticCalculator.calculate(expression, input)
                    .getSourceDistinctValuesCount(), expression.toString());
        }
        // These map the column to other values, so the NDV of the column says nothing about their domain.
        List<ScalarOperator> dropping = List.of(new CastOperator(IntegerType.TINYINT, KEY),
                new CastOperator(VarcharType.VARCHAR, KEY),
                new CallOperator(FunctionSet.YEAR, IntegerType.INT, List.of(DAY)),
                new CallOperator(FunctionSet.ADD, IntegerType.BIGINT, List.of(KEY, ConstantOperator.createInt(1))));
        for (ScalarOperator expression : dropping) {
            ColumnStatistic statistic = ExpressionStatisticCalculator.calculate(expression, input);
            Assertions.assertEquals(statistic.getDistinctValuesCount(), statistic.getSourceDistinctValuesCount(),
                    expression.toString());
        }
    }

    @Test
    public void testJoinKeepsTheSourceNdvOfEachKey() {
        // A dimension key filtered to 335 of its 72542 values joins a fact key with 260 values. Above the join,
        // a runtime filter on either key still sees the domain of its own column.
        ColumnStatistic dimension = ColumnStatistic.buildFrom(column(72542)).setDistinctValuesCount(335).build();
        ColumnStatistic fact = column(260);
        Statistics cross = Statistics.builder().setOutputRowCount(335 * 399_330_000.0)
                .addColumnStatistic(KEY, dimension).addColumnStatistic(FACT_KEY, fact).build();
        Statistics joined = BinaryPredicateStatisticCalculator.estimateColumnEqualToColumn(KEY, dimension,
                FACT_KEY, fact, cross, false);
        Assertions.assertEquals(72542, joined.getColumnStatistic(KEY).getSourceDistinctValuesCount());
        Assertions.assertEquals(260, joined.getColumnStatistic(FACT_KEY).getSourceDistinctValuesCount());
    }

    @Test
    public void testPredicatesOnTheKeyKeepTheSourceNdv() {
        ColumnStatistic text = new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 1000);
        ColumnStatistic reduced = ColumnStatistic.buildFrom(column(72542)).setDistinctValuesCount(5000).build();
        Statistics input = Statistics.builder().setOutputRowCount(100000).addColumnStatistic(TEXT, text)
                .addColumnStatistic(KEY, reduced).build();
        Statistics in = PredicateStatisticsCalculator.statisticsCalculate(new InPredicateOperator(false, TEXT,
                ConstantOperator.createVarchar("a"), ConstantOperator.createVarchar("b")), input);
        Assertions.assertEquals(2, in.getColumnStatistic(TEXT).getDistinctValuesCount());
        Assertions.assertEquals(1000, in.getColumnStatistic(TEXT).getSourceDistinctValuesCount());
        for (BinaryType type : List.of(BinaryType.LT, BinaryType.NE)) {
            Statistics compared = PredicateStatisticsCalculator.statisticsCalculate(
                    new BinaryPredicateOperator(type, KEY, ConstantOperator.createInt(50000)), input);
            Assertions.assertEquals(72542, compared.getColumnStatistic(KEY).getSourceDistinctValuesCount(),
                    type.toString());
        }
    }
}
