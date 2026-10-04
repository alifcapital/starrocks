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

import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.UnaryOperator;

public class InPredicateLiteralStatisticsTest {
    // IN and NOT IN over a list of literals read the literal bounds and NDV directly. We expect the same
    // estimate as for an IN list over column refs that carry the expression statistics of each literal,
    // so we compute both and compare row count and the statistics of the input column.
    private static void compare(Type type, List<ScalarOperator> literals, ColumnStatistic column, double rows) {
        ColumnRefOperator left = new ColumnRefOperator(1, type, "input", true);
        Statistics input = Statistics.builder().setOutputRowCount(rows).addColumnStatistic(left, column).build();
        Statistics.Builder referenceInput = Statistics.buildFrom(input);
        Map<ScalarOperator, ColumnRefOperator> refs = new LinkedHashMap<>();
        List<ScalarOperator> direct = new ArrayList<>(List.of(left));
        List<ScalarOperator> reference = new ArrayList<>(List.of(left));
        for (ScalarOperator literal : literals) {
            direct.add(literal);
            ColumnRefOperator ref = refs.computeIfAbsent(literal, value -> {
                ColumnRefOperator result = new ColumnRefOperator(10 + refs.size(), value.getType(), "literal", true);
                referenceInput.addColumnStatistic(result, ExpressionStatisticCalculator.calculate(value, input));
                return result;
            });
            reference.add(ref);
        }
        for (boolean notIn : List.of(false, true)) {
            Statistics actual = PredicateStatisticsCalculator.statisticsCalculate(
                    new InPredicateOperator(notIn, direct), input);
            Statistics expected = PredicateStatisticsCalculator.statisticsCalculate(
                    new InPredicateOperator(notIn, reference), referenceInput.build());
            Assertions.assertEquals(Double.doubleToLongBits(expected.getOutputRowCount()),
                    Double.doubleToLongBits(actual.getOutputRowCount()), "row count");
            ColumnStatistic a = actual.getColumnStatistic(left);
            ColumnStatistic e = expected.getColumnStatistic(left);
            Assertions.assertEquals(Double.doubleToLongBits(e.getMinValue()), Double.doubleToLongBits(a.getMinValue()), "min");
            Assertions.assertEquals(Double.doubleToLongBits(e.getMaxValue()), Double.doubleToLongBits(a.getMaxValue()), "max");
            Assertions.assertEquals(e.getDistinctValuesCount(), a.getDistinctValuesCount(), "NDV");
            Assertions.assertEquals(e.getNullsFraction(), a.getNullsFraction(), "nulls");
            Assertions.assertEquals(e.getAverageRowSize(), a.getAverageRowSize(), "width");
            Assertions.assertEquals(e.getType(), a.getType(), "statistic kind");
        }
    }

    @Test
    public void unknownNdvSaturationPreservesDuplicatesNullsAndFallbacks() {
        for (double ndv : new double[] {0, 0.5, 1, 2, Double.NaN}) {
            ColumnStatistic column = ColumnStatistic.buildFrom(ColumnStatistic.unknown())
                    .setDistinctValuesCount(ndv).build();
            for (double rows : new double[] {0, 1, 1000, Double.NaN}) {
                compare(IntegerType.INT, List.of(), column, rows);
                compare(IntegerType.INT, List.of(ConstantOperator.createNull(IntegerType.INT),
                        ConstantOperator.createNull(IntegerType.INT)), column, rows);
                compare(IntegerType.INT, List.of(ConstantOperator.createInt(2), ConstantOperator.createInt(2),
                        ConstantOperator.createInt(8), ConstantOperator.createNull(IntegerType.INT)), column, rows);
                compare(VarcharType.VARCHAR, List.of(ConstantOperator.createVarchar("a"),
                        ConstantOperator.createVarchar("a"), ConstantOperator.createVarchar("b")), column, rows);
            }
        }
    }

    @Test
    public void literalListsMatchGeneralExpressionStatistics() {
        Type decimal = new DecimalType(PrimitiveType.DECIMAL128, 38, 20);
        List<List<ScalarOperator>> cases = List.of(
                List.of(ConstantOperator.createBigint(9007199254740992L),
                        ConstantOperator.createBigint(9007199254740993L)),
                List.of(ConstantOperator.createLargeInt(new BigInteger("170141183460469231731687303715884105727"))),
                List.of(ConstantOperator.createDecimal(new BigDecimal("1.00000000000000000001"), decimal),
                        ConstantOperator.createDecimal(new BigDecimal("1.00000000000000000002"), decimal)),
                List.of(ConstantOperator.createDatetime(LocalDateTime.of(2026, 10, 1, 12, 34, 56))),
                List.of(),
                List.of(ConstantOperator.createNull(IntegerType.INT)),
                List.of(ConstantOperator.createInt(-2), ConstantOperator.createInt(7), ConstantOperator.createInt(7)),
                List.of(ConstantOperator.createInt(1), ConstantOperator.createNull(IntegerType.INT)),
                List.of(ConstantOperator.createVarchar("alpha"), ConstantOperator.createVarchar("beta")),
                List.of(ConstantOperator.createVarchar(""), ConstantOperator.createNull(VarcharType.VARCHAR)),
                List.of(ConstantOperator.createBoolean(false), ConstantOperator.createBoolean(true)),
                List.of(new ConstantOperator(-0.0, FloatType.DOUBLE), new ConstantOperator(0.0, FloatType.DOUBLE)),
                List.of(ConstantOperator.createDate(LocalDateTime.of(2026, 9, 30, 0, 0)),
                        ConstantOperator.createDate(LocalDateTime.of(2026, 10, 1, 0, 0))));
        List<ColumnStatistic> columns = List.of(ColumnStatistic.unknown(),
                new ColumnStatistic(-10, 10, 0.2, 8, 20),
                new ColumnStatistic(-0.0, 0.0, 0, 8, 1),
                new ColumnStatistic(Double.NaN, 10, 0, 8, 10));
        for (Type type : List.of(IntegerType.INT, VarcharType.VARCHAR, FloatType.DOUBLE,
                BooleanType.BOOLEAN, DateType.DATE)) {
            for (List<ScalarOperator> literals : cases) {
                for (ColumnStatistic column : columns) {
                    for (double rows : List.of(0.0, 1000.0, Double.NaN)) {
                        compare(type, literals, column, rows);
                    }
                }
            }
        }
    }

    @Test
    public void distinctValuesKeepTheOrderOfTheFirstOccurrence() {
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator sameAsA = new ColumnRefOperator(1, IntegerType.INT, "a", false);
        ColumnRefOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        List<ScalarOperator> items = List.of(a, ConstantOperator.createInt(3), ConstantOperator.createInt(1),
                ConstantOperator.createInt(3), b, sameAsA, ConstantOperator.createNull(IntegerType.INT),
                ConstantOperator.createNull(IntegerType.INT), ConstantOperator.createBigint(3),
                ConstantOperator.createInt(1), new CastOperator(IntegerType.BIGINT, ConstantOperator.createInt(1)),
                new CastOperator(IntegerType.BIGINT, ConstantOperator.createInt(1)), b);
        UnaryOperator<ScalarOperator> identity = item -> item;
        UnaryOperator<ScalarOperator> collapse = item -> item instanceof CastOperator ? a : item;
        for (UnaryOperator<ScalarOperator> mapping : List.of(identity, collapse)) {
            for (int from = 0; from <= items.size() + 1; from++) {
                Assertions.assertEquals(items.stream().skip(from).map(mapping).distinct().toList(),
                        PredicateStatisticsCalculator.distinctFrom(items, from, mapping), "from " + from);
            }
        }
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> PredicateStatisticsCalculator.distinctFrom(items, 1, identity).add(a));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> PredicateStatisticsCalculator.distinctFrom(items, items.size(), identity).add(a));
    }

    @Test
    public void distinctValuesOfLongListsMatchTheStream() {
        Random random = new Random(7);
        for (int size : new int[] {2, 17, 1000, 20000}) {
            List<ScalarOperator> items = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                int value = random.nextInt(Math.max(1, size / 3));
                items.add(i % 11 == 0 ? ConstantOperator.createNull(IntegerType.BIGINT)
                        : i % 5 == 0 ? ConstantOperator.createInt(value) : ConstantOperator.createBigint(value));
            }
            UnaryOperator<ScalarOperator> identity = item -> item;
            Assertions.assertEquals(items.stream().skip(1).distinct().toList(),
                    PredicateStatisticsCalculator.distinctFrom(items, 1, identity));
        }
    }

    @Test
    public void allConstantOperatorsSkipsTheFirstOnesAndStopsAtAnyOther() {
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ConstantOperator one = ConstantOperator.createInt(1);
        Assertions.assertTrue(PredicateStatisticsCalculator.allConstantOperators(List.of(), 0));
        Assertions.assertTrue(PredicateStatisticsCalculator.allConstantOperators(List.of(a), 1));
        Assertions.assertTrue(PredicateStatisticsCalculator.allConstantOperators(List.of(a, one, one), 1));
        Assertions.assertFalse(PredicateStatisticsCalculator.allConstantOperators(List.of(a, one, a), 1));
        Assertions.assertFalse(PredicateStatisticsCalculator.allConstantOperators(List.of(a, one, one), 0));
    }

    @Test
    public void duplicatesAndCastsInTheListDoNotChangeTheEstimate() {
        ColumnRefOperator left = new ColumnRefOperator(1, IntegerType.BIGINT, "input", true);
        ColumnRefOperator right = new ColumnRefOperator(2, IntegerType.BIGINT, "other", true);
        Statistics input = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(left, new ColumnStatistic(0, 100, 0, 8, 50))
                .addColumnStatistic(right, new ColumnStatistic(10, 20, 0, 8, 5)).build();
        List<ScalarOperator> distinct = List.of(ConstantOperator.createBigint(3), ConstantOperator.createBigint(7),
                right, ConstantOperator.createNull(IntegerType.BIGINT));
        List<ScalarOperator> repeated = List.of(ConstantOperator.createBigint(3), ConstantOperator.createBigint(3),
                ConstantOperator.createBigint(7), right,
                new CastOperator(IntegerType.BIGINT, ConstantOperator.createBigint(7)), right,
                ConstantOperator.createNull(IntegerType.BIGINT), ConstantOperator.createNull(IntegerType.BIGINT));
        for (boolean notIn : List.of(false, true)) {
            Statistics expected = PredicateStatisticsCalculator.statisticsCalculate(
                    new InPredicateOperator(notIn, concat(left, distinct)), input);
            Statistics actual = PredicateStatisticsCalculator.statisticsCalculate(
                    new InPredicateOperator(notIn, concat(left, repeated)), input);
            Assertions.assertEquals(Double.doubleToLongBits(expected.getOutputRowCount()),
                    Double.doubleToLongBits(actual.getOutputRowCount()));
            ColumnStatistic e = expected.getColumnStatistic(left);
            ColumnStatistic a = actual.getColumnStatistic(left);
            Assertions.assertEquals(Double.doubleToLongBits(e.getMinValue()), Double.doubleToLongBits(a.getMinValue()));
            Assertions.assertEquals(Double.doubleToLongBits(e.getMaxValue()), Double.doubleToLongBits(a.getMaxValue()));
            Assertions.assertEquals(e.getDistinctValuesCount(), a.getDistinctValuesCount());
        }
    }

    private static List<ScalarOperator> concat(ScalarOperator first, List<ScalarOperator> rest) {
        List<ScalarOperator> result = new ArrayList<>();
        result.add(first);
        result.addAll(rest);
        return result;
    }
}
