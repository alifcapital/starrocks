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
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LikePatternEstimatorTest {
    private static final ColumnRefOperator STATUS = new ColumnRefOperator(1, VarcharType.VARCHAR, "status", true);
    private static final ColumnRefOperator CODE = new ColumnRefOperator(2, IntegerType.INT, "code", true);

    private static Statistics statistics() {
        // 800 rows in the MCV list, 200 rows of 20 other values in the tail bucket, 10% NULL.
        Histogram histogram = new Histogram(
                List.of(new Bucket(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, 200L, 0L)),
                Map.of("approved", 500L, "declined", 200L, "appealed", 60L, "a_b", 40L));
        return Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(STATUS, ColumnStatistic.builder().setDistinctValuesCount(24).setNullsFraction(0.1)
                        .setAverageRowSize(8).setHistogram(histogram).build())
                .addColumnStatistic(CODE, ColumnStatistic.builder().setMinValue(0).setMaxValue(9)
                        .setDistinctValuesCount(10).setNullsFraction(0).setAverageRowSize(4).build())
                .build();
    }

    private static boolean matches(String like, String value) {
        return LikePatternEstimator.compile(like).matches(value);
    }

    @Test
    public void testPatternCompilation() {
        Assertions.assertTrue(matches("app%", "approved"));
        Assertions.assertFalse(matches("app%", "declined"));
        Assertions.assertTrue(matches("%ed", "approved"));
        Assertions.assertTrue(matches("a_b", "acb"));
        Assertions.assertFalse(matches("a_b", "ab"));
        Assertions.assertTrue(matches("a\\_b", "a_b"));
        Assertions.assertFalse(matches("a\\_b", "acb"));
        Assertions.assertTrue(matches("10.5%", "10.50"));
        Assertions.assertFalse(matches("10.5%", "1005"));
        Assertions.assertTrue(matches("%", ""));
        Assertions.assertTrue(matches("x%y", "x\ny"));
        Assertions.assertTrue(matches("%%a%%", "bab"));
        // One character is one code point, whatever its UTF-16 length.
        Assertions.assertTrue(matches("_", "😀"));
        Assertions.assertFalse(matches("__", "😀"));
        Assertions.assertTrue(matches("a_b", "a😀b"));
        Assertions.assertFalse(matches("", "a"));
        Assertions.assertTrue(matches("", ""));

        // A pattern that makes a backtracking regex engine take exponential time.
        StringBuilder hostile = new StringBuilder();
        for (int i = 0; i < 40; i++) {
            hostile.append("%a");
        }
        hostile.append("b");
        long start = System.nanoTime();
        Assertions.assertFalse(matches(hostile.toString(), "a".repeat(200)));
        Assertions.assertTrue(System.nanoTime() - start < 1_000_000_000L);
    }

    @Test
    public void testSelectivityFromTheMcvList() {
        Statistics statistics = statistics();
        LikePredicateOperator like = new LikePredicateOperator(STATUS, ConstantOperator.createVarchar("app%"));
        // 560 of the 1000 histogram rows match in the head; 2 of the 4 most common values match, so half
        // of the 200 tail rows are taken to match; 10% of the rows are NULL.
        double expected = (560 + 100) / 1000.0 * 0.9;
        Assertions.assertEquals(expected, LikePatternEstimator.selectivity(like, statistics).orElseThrow(), 1e-12);
        Statistics estimated = PredicateStatisticsCalculator.statisticsCalculate(like, statistics);
        Assertions.assertEquals(1000 * expected, estimated.getOutputRowCount(), 1e-9);
        Assertions.assertEquals(0, estimated.getColumnStatistic(STATUS).getNullsFraction(), 1e-12);

        // The column under a cast: 760 head rows in the three values ending with "ed", three quarters
        // of the tail.
        LikePredicateOperator cast = new LikePredicateOperator(new CastOperator(VarcharType.VARCHAR, STATUS),
                ConstantOperator.createVarchar("%ed"));
        Assertions.assertEquals((760 + 150) / 1000.0 * 0.9,
                LikePatternEstimator.selectivity(cast, statistics).orElseThrow(), 1e-12);

        // No pattern match in the head: nothing from the tail either.
        LikePredicateOperator none = new LikePredicateOperator(STATUS, ConstantOperator.createVarchar("z%"));
        Assertions.assertEquals(0, LikePatternEstimator.selectivity(none, statistics).orElseThrow(), 1e-12);
    }

    @Test
    public void testExpressionsOfTheColumnAreEvaluatedOnTheMcvList() {
        Statistics statistics = statistics();
        // upper() of the values: "APPROVED" and "APPEALED" match, 560 of the 1000 histogram rows plus
        // half of the tail.
        Function upper = ExprUtils.getBuiltinFunction("upper", new Type[] {VarcharType.VARCHAR},
                Function.CompareMode.IS_IDENTICAL);
        LikePredicateOperator upperLike = new LikePredicateOperator(
                new CallOperator("upper", VarcharType.VARCHAR, List.of(STATUS), upper),
                ConstantOperator.createVarchar("APP%"));
        Assertions.assertEquals((560 + 100) / 1000.0 * 0.9,
                LikePatternEstimator.selectivity(upperLike, statistics).orElseThrow(), 1e-12);

        // A cast to another type on the way changes the text: "00123" becomes "123" and matches,
        // the other values cast to NULL and never match.
        ColumnRefOperator code = new ColumnRefOperator(3, VarcharType.VARCHAR, "code", true);
        Histogram histogram = new Histogram(
                List.of(new Bucket(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, 200L, 0L)),
                Map.of("00123", 500L, "45", 200L, "x", 100L));
        Statistics codes = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(code, ColumnStatistic.builder().setDistinctValuesCount(23).setNullsFraction(0)
                        .setAverageRowSize(8).setHistogram(histogram).build())
                .build();
        LikePredicateOperator changed = new LikePredicateOperator(
                new CastOperator(VarcharType.VARCHAR, new CastOperator(IntegerType.BIGINT, code)),
                ConstantOperator.createVarchar("1%"));
        Assertions.assertEquals(code, LikePatternEstimator.column(changed).orElseThrow());
        Assertions.assertEquals((500 + 200.0 / 3) / 1000.0,
                LikePatternEstimator.selectivity(changed, codes).orElseThrow(), 1e-12);

        // An expression that turns NULL into a value: the 10% NULL rows become "none" and match.
        Function coalesce = ExprUtils.getBuiltinFunction("coalesce", new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR},
                Function.CompareMode.IS_IDENTICAL);
        LikePredicateOperator nullToNone = new LikePredicateOperator(
                new CallOperator("coalesce", VarcharType.VARCHAR, List.of(STATUS, ConstantOperator.createVarchar("none")),
                        coalesce),
                ConstantOperator.createVarchar("no%"));
        Assertions.assertEquals(Optional.of(true), LikePatternEstimator.nullRowsMatch(nullToNone, STATUS));
        Assertions.assertEquals(0.1, LikePatternEstimator.selectivity(nullToNone, statistics).orElseThrow(), 1e-12);
        Statistics kept = PredicateStatisticsCalculator.statisticsCalculate(nullToNone, statistics);
        Assertions.assertEquals(100, kept.getOutputRowCount(), 1e-9);
        Assertions.assertEquals(0.1, kept.getColumnStatistic(STATUS).getNullsFraction(), 1e-12);
        Assertions.assertEquals(Optional.of(false), LikePatternEstimator.nullRowsMatch(upperLike, STATUS));

        // An expression of two columns is not a column's.
        LikePredicateOperator twoColumns = new LikePredicateOperator(
                new CallOperator("concat", VarcharType.VARCHAR, List.of(STATUS, code)),
                ConstantOperator.createVarchar("a%"));
        Assertions.assertTrue(LikePatternEstimator.column(twoColumns).isEmpty());
    }

    @Test
    public void testFallsBackWithoutAnMcvList() {
        Statistics statistics = statistics();
        LikePredicateOperator noHistogram = new LikePredicateOperator(new CastOperator(VarcharType.VARCHAR, CODE),
                ConstantOperator.createVarchar("1%"));
        Assertions.assertTrue(LikePatternEstimator.selectivity(noHistogram, statistics).isEmpty());
        Assertions.assertEquals(1000 * StatisticsEstimateCoefficient.PREDICATE_UNKNOWN_FILTER_COEFFICIENT,
                PredicateStatisticsCalculator.statisticsCalculate(noHistogram, statistics).getOutputRowCount(), 1e-9);

        LikePredicateOperator regexp = new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, STATUS,
                ConstantOperator.createVarchar("^app"));
        Assertions.assertTrue(LikePatternEstimator.pattern(regexp).isEmpty());
        LikePredicateOperator column = new LikePredicateOperator(STATUS, CODE);
        Assertions.assertTrue(LikePatternEstimator.pattern(column).isEmpty());
    }
}
