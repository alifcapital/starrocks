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
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * The statistics of left, strleft, substr and substring of a column without statistics on expressions. The column has
 * 2^20 distinct values of 20 characters on average, so a prefix of k characters has 2^k of them: the NDV of the column
 * raised to the share of the characters taken. A part from another start has no statistics.
 */
public class SubstringStatisticsTest {
    private static final double ROWS = 10_000_000;
    private static final ColumnRefOperator C = new ColumnRefOperator(1, VarcharType.VARCHAR, "c", true);
    private static final ColumnRefOperator N = new ColumnRefOperator(2, IntegerType.INT, "n", true);

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.createDefaultCtx();
    }

    private static ScalarOperator call(String name, ScalarOperator... arguments) {
        Type[] types = new Type[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            types[i] = arguments[i].getType();
        }
        Function function = ExprUtils.getBuiltinFunction(name, types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(name, VarcharType.VARCHAR, List.of(arguments), function);
    }

    private static ConstantOperator number(int value) {
        return ConstantOperator.createInt(value);
    }

    private static Statistics scan(double nulls) {
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, nulls, 20,
                        1 << 20))
                .addColumnStatistic(N, new ColumnStatistic(0, 20, 0, 4, 20)).build();
    }

    private static ColumnStatistic statistic(ScalarOperator expression) {
        return ExpressionStatisticCalculator.calculate(expression, scan(0.1));
    }

    @Test
    public void testPrefixTakesItsShareOfTheLogarithmOfTheNdv() {
        Assertions.assertEquals(32, statistic(call("left", C, number(5))).getDistinctValuesCount(), 1e-6);
        Assertions.assertEquals(1024, statistic(call("left", C, number(10))).getDistinctValuesCount(), 1e-6);
        Assertions.assertEquals(2, statistic(call("left", C, number(1))).getDistinctValuesCount(), 1e-6);
        // The same function under its other name.
        Assertions.assertEquals(32, statistic(call("strleft", C, number(5))).getDistinctValuesCount(), 1e-6);
    }

    @Test
    public void testNdvIsBetweenOneAndTheNdvOfTheColumn() {
        // No characters is one value, and every character, or more than there are, is the values of the column.
        Assertions.assertEquals(1, statistic(call("left", C, number(0))).getDistinctValuesCount(), 1e-6);
        Assertions.assertEquals(1, statistic(call("left", C, number(-3))).getDistinctValuesCount(), 1e-6);
        Assertions.assertEquals(1 << 20, statistic(call("left", C, number(20))).getDistinctValuesCount(), 1e-6);
        Assertions.assertEquals(1 << 20, statistic(call("left", C, number(100))).getDistinctValuesCount(), 1e-6);
        // A length that is not a constant leaves the values of the column.
        Assertions.assertEquals(1 << 20, statistic(call("left", C, N)).getDistinctValuesCount(), 1e-6);
        // The values are not more than the rows of the input.
        Assertions.assertEquals(1000, ExpressionStatisticCalculator.calculate(call("left", C, number(15)), scan(0), 1000)
                .getDistinctValuesCount(), 1e-6);
    }

    @Test
    public void testNullsSizeAndRangeOfTheResult() {
        ColumnStatistic prefix = statistic(call("left", C, number(5)));
        Assertions.assertEquals(0.1, prefix.getNullsFraction(), 1e-9);
        Assertions.assertEquals(5, prefix.getAverageRowSize(), 1e-9);
        Assertions.assertTrue(prefix.isInfiniteRange());
        Assertions.assertNull(prefix.getHistogram());
        Assertions.assertFalse(prefix.isUnknown());
        // A prefix longer than the strings is the string.
        Assertions.assertEquals(20, statistic(call("left", C, number(50))).getAverageRowSize(), 1e-9);
    }

    @Test
    public void testEqualityOnThePrefixIsNotAWholeTableOrOneRow() {
        // Rows of one of 32 values, of the non-NULL rows.
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(new BinaryPredicateOperator(
                BinaryType.EQ, call("left", C, number(5)), ConstantOperator.createVarchar("20202")), scan(0));
        Assertions.assertEquals(ROWS / 32, result.getOutputRowCount(), 1e-3);
    }

    private static ScalarOperator substring(String name, ScalarOperator start, ScalarOperator length) {
        return length == null ? call(name, C, start) : call(name, C, start, length);
    }

    @Test
    public void testSubstringFromTheFirstCharacterIsAPrefix() {
        for (String name : List.of("substring", "substr")) {
            // 5 characters from the start: 2^5 values, as left(c, 5).
            Assertions.assertEquals(32, statistic(substring(name, number(1), number(5))).getDistinctValuesCount(), 1e-6);
            Assertions.assertEquals(1, statistic(substring(name, number(1), number(0))).getDistinctValuesCount(), 1e-6);
            Assertions.assertEquals(1 << 20, statistic(substring(name, number(1), number(100))).getDistinctValuesCount(),
                    1e-6);
            // Without the length it is the whole string, and a length that is not a constant leaves the whole string.
            Assertions.assertEquals(1 << 20, statistic(substring(name, number(1), null)).getDistinctValuesCount(), 1e-6);
            Assertions.assertEquals(1 << 20, statistic(substring(name, number(1), N)).getDistinctValuesCount(), 1e-6);
            // The values are not more than the rows of the input.
            Assertions.assertEquals(1000, ExpressionStatisticCalculator.calculate(substring(name, number(1), number(15)),
                    scan(0), 1000).getDistinctValuesCount(), 1e-6);
        }
    }

    // A suffix of a phone, the middle of a code, the rest of a string, a start of 0, a start past the end and a start
    // before the string; a start that is not a constant, with or without a length.
    private static List<ScalarOperator> partsAwayFromTheStart(String name) {
        return List.of(substring(name, number(-9), null), substring(name, number(6), number(3)),
                substring(name, number(4), number(7)), substring(name, number(5), null),
                substring(name, number(18), number(5)), substring(name, number(25), number(5)),
                substring(name, number(0), number(5)), substring(name, number(-3), number(5)),
                substring(name, number(-30), number(5)), substring(name, number(2), null),
                substring(name, N, number(5)), substring(name, N, null));
    }

    @Test
    public void testPartFromAnotherStartHasNoStatistics() {
        for (String name : List.of("substring", "substr")) {
            for (ScalarOperator part : partsAwayFromTheStart(name)) {
                Assertions.assertTrue(statistic(part).isUnknown(), part.toString());
            }
        }
    }

    // The conditions on an expression of a column that no case of the calculator knows, which have no statistics.
    private static List<ScalarOperator> conditions(ScalarOperator expression) {
        ConstantOperator value = ConstantOperator.createVarchar("abcde");
        return List.of(new BinaryPredicateOperator(BinaryType.EQ, expression, value),
                new BinaryPredicateOperator(BinaryType.NE, expression, value),
                new BinaryPredicateOperator(BinaryType.GE, expression, value),
                new InPredicateOperator(false, expression, value, ConstantOperator.createVarchar("fghij")),
                new InPredicateOperator(true, expression, value, ConstantOperator.createVarchar("fghij")),
                new IsNullPredicateOperator(false, expression));
    }

    @Test
    public void testConditionOnAPartAwayFromTheStartIsEstimatedByTheDefault() {
        List<ScalarOperator> defaults = conditions(call("md5", C));
        Assertions.assertTrue(statistic(call("md5", C)).isUnknown());
        for (String name : List.of("substring", "substr")) {
            for (ScalarOperator part : partsAwayFromTheStart(name)) {
                List<ScalarOperator> conditions = conditions(part);
                for (int i = 0; i < conditions.size(); i++) {
                    Assertions.assertEquals(
                            PredicateStatisticsCalculator.statisticsCalculate(defaults.get(i), scan(0.1)).getOutputRowCount(),
                            PredicateStatisticsCalculator.statisticsCalculate(conditions.get(i), scan(0.1))
                                    .getOutputRowCount(), 1e-9, conditions.get(i).toString());
                }
            }
        }
        // An equality on such a part is not one row, as the NDV of the column makes it: it keeps the rows of the table,
        // which no statistic on the part refines.
        Assertions.assertEquals(ROWS, PredicateStatisticsCalculator.statisticsCalculate(new BinaryPredicateOperator(
                BinaryType.EQ, substring("substr", number(6), number(3)), ConstantOperator.createVarchar("abcde")),
                scan(0.1)).getOutputRowCount(), 1e-3);
    }

    @Test
    public void testPrefixIsNotTheStatisticOfTheColumn() {
        for (String name : List.of("substring", "substr")) {
            checkPart(name);
        }
    }

    private static void checkPart(String name) {
        Histogram histogram = new Histogram(Map.of("abcde12345", 100L));
        Statistics withHistogram = Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(C, ColumnStatistic.buildFrom(scan(0.1).getColumnStatistic(C))
                        .setHistogram(histogram).build()).build();
        ColumnStatistic part = ExpressionStatisticCalculator.calculate(
                substring(name, number(1), number(5)), withHistogram);
        // The values of the column are not the values of the prefix: no histogram, no range, the NULL rows of c.
        Assertions.assertNull(part.getHistogram());
        Assertions.assertTrue(part.isInfiniteRange());
        Assertions.assertEquals(0.1, part.getNullsFraction(), 1e-9);
        Assertions.assertEquals(5, part.getAverageRowSize(), 1e-9);
        Assertions.assertEquals(32, part.getDistinctValuesCount(), 1e-6);
        // A part from the 6th character has no statistics, and not the histogram of the column.
        Assertions.assertTrue(ExpressionStatisticCalculator.calculate(substring(name, number(6), number(5)),
                withHistogram).isUnknown());
        // Rows of one of 32 values.
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(new BinaryPredicateOperator(
                BinaryType.EQ, substring(name, number(1), number(5)), ConstantOperator.createVarchar("abcde")),
                scan(0));
        Assertions.assertEquals(ROWS / 32, result.getOutputRowCount(), 1e-3);
    }

    @Test
    public void testColumnWithoutStatisticsGivesAPartWithoutStatistics() {
        Assertions.assertTrue(SubstringStatistics.of(ColumnStatistic.unknown(), 5, ROWS).isUnknown());
        Statistics unknown = Statistics.builder().setOutputRowCount(ROWS).addColumnStatistic(C, ColumnStatistic.unknown())
                .addColumnStatistic(N, new ColumnStatistic(0, 20, 0, 4, 20)).build();
        for (ScalarOperator part : List.of(call("left", C, number(5)), call("strleft", C, number(5)),
                substring("substring", number(1), number(5)), substring("substr", number(1), number(5)),
                substring("substr", number(2), null))) {
            Assertions.assertTrue(ExpressionStatisticCalculator.calculate(part, unknown).isUnknown(), part.toString());
            // The condition is estimated as one on any function of a column without statistics.
            ScalarOperator other = call("md5", C);
            Assertions.assertEquals(
                    PredicateStatisticsCalculator.statisticsCalculate(new BinaryPredicateOperator(BinaryType.EQ, other,
                            ConstantOperator.createVarchar("x")), unknown).getOutputRowCount(),
                    PredicateStatisticsCalculator.statisticsCalculate(new BinaryPredicateOperator(BinaryType.EQ, part,
                            ConstantOperator.createVarchar("x")), unknown).getOutputRowCount(), 1e-9, part.toString());
        }
    }

    @Test
    public void testNdvThatIsAGuessStaysAGuess() {
        ColumnStatistic guessed = ColumnStatistic.buildFrom(scan(0.1).getColumnStatistic(C))
                .setDistinctValuesFromDefaultSelectivity(true).build();
        Assertions.assertTrue(SubstringStatistics.of(guessed, 5, ROWS).isDistinctValuesFromDefaultSelectivity());
        Statistics withGuess = Statistics.builder().setOutputRowCount(ROWS).addColumnStatistic(C, guessed).build();
        Assertions.assertTrue(ExpressionStatisticCalculator.calculate(call("left", C, number(5)), withGuess)
                .isDistinctValuesFromDefaultSelectivity());
        Assertions.assertFalse(statistic(call("left", C, number(5))).isDistinctValuesFromDefaultSelectivity());
    }
}
