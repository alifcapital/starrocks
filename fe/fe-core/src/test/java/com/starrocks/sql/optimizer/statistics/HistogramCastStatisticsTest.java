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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class HistogramCastStatisticsTest {
    private static final double ROWS = 10_000_000;
    private static final String STORED = "{\"buckets\":[[\"Infinity\",\"Infinity\",\"9998500\",\"0\"]],"
            + "\"mcv\":[[\"10\",\"750\"],[\"900000\",\"750\"]]}";
    private static final ColumnRefOperator TEXT = new ColumnRefOperator(1, VarcharType.VARCHAR, "text", true);

    @BeforeEach
    void setUp() {
        UtFrameUtils.createDefaultCtx();
    }

    private Histogram stored() throws Exception {
        return new Histogram(HistogramUtils.convertBuckets(STORED, VarcharType.VARCHAR),
                HistogramUtils.convertMCV(STORED));
    }

    private Statistics input(ColumnRefOperator column, Histogram histogram) {
        return Statistics.builder().setOutputRowCount(ROWS).addColumnStatistic(column,
                ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(8)
                        .setDistinctValuesCount(1_000_000).setHistogram(histogram).build()).build();
    }

    private Statistics estimate(ScalarOperator expr, BinaryType op, ConstantOperator constant, Statistics input) {
        return PredicateStatisticsCalculator.statisticsCalculate(new BinaryPredicateOperator(op, expr, constant), input);
    }

    @Test
    void stringTailIsMassNotInfinityAndSurvivesDump() throws Exception {
        Histogram histogram = stored();
        Assertions.assertTrue(histogram.hasUnknownRange());
        Assertions.assertEquals(ROWS, histogram.getTotalRows());
        for (Histogram copy : List.of(HistogramUtils.deserializeHistogram(HistogramUtils.serializeHistogram(histogram)),
                HistogramUtils.deserializeHistogram(STORED))) {
            Assertions.assertTrue(copy.hasUnknownRange());
            Assertions.assertEquals(histogram.getTotalRows(), copy.getTotalRows());
            Assertions.assertEquals(histogram.getMCV(), copy.getMCV());
            Assertions.assertTrue(copy.getRowCountInBucket(Double.POSITIVE_INFINITY, 100, false).isEmpty());
        }
    }

    @Test
    void realNumericInfinityIsNotUnknownTail() throws Exception {
        String json = "{\"buckets\":[[\"Infinity\",\"Infinity\",\"50\",\"50\"]],\"mcv\":[]}";
        Histogram histogram = new Histogram(HistogramUtils.convertBuckets(json, FloatType.DOUBLE), Map.of());
        Assertions.assertFalse(histogram.hasUnknownRange());
        Assertions.assertFalse(HistogramUtils.deserializeHistogram(HistogramUtils.serializeHistogram(histogram))
                .hasUnknownRange());
        Assertions.assertEquals(50L, histogram.getRowCountInBucket(Double.POSITIVE_INFINITY, 1, false).orElseThrow());
    }

    @Test
    void unknownTailCannotBeUsedAsNumericRangeEvenWithoutCast() throws Exception {
        ColumnStatistic stat = input(TEXT, stored()).getColumnStatistic(TEXT);
        for (boolean inclusive : List.of(false, true)) {
            Assertions.assertTrue(BinaryPredicateStatisticCalculator.updateHistWithLessThan(
                    stat, Optional.of(ConstantOperator.createInt(500000)), inclusive).isEmpty());
            Assertions.assertTrue(BinaryPredicateStatisticCalculator.updateHistWithGreaterThan(
                    stat, Optional.of(ConstantOperator.createInt(500000)), inclusive).isEmpty());
        }
    }

    @Test
    void numericStringRangesAgreeWithFallbackAndProjection() throws Exception {
        CastOperator cast = new CastOperator(IntegerType.BIGINT, TEXT);
        Statistics input = input(TEXT, stored());
        ColumnRefOperator projected = new ColumnRefOperator(2, IntegerType.BIGINT, "number", true);
        ColumnStatistic castStat = ExpressionStatisticCalculator.calculate(cast, input);
        Assertions.assertNull(castStat.getHistogram());
        Statistics projection = Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(projected, castStat).build();
        for (BinaryType op : List.of(BinaryType.LT, BinaryType.LE, BinaryType.GT, BinaryType.GE)) {
            ConstantOperator constant = ConstantOperator.createBigint(500000);
            Statistics result = estimate(cast, op, constant, input);
            double fallback = estimate(cast, op, constant, input(TEXT, null)).getOutputRowCount();
            Assertions.assertEquals(fallback, result.getOutputRowCount(), 0.01, op.toString());
            Assertions.assertEquals(estimate(projected, op, constant, projection).getOutputRowCount(),
                    result.getOutputRowCount(), 0.01);
            Assertions.assertEquals(ROWS / 2, result.getOutputRowCount(), 0.01);
            Assertions.assertSame(input.getColumnStatistic(TEXT).getHistogram(), result.getColumnStatistic(TEXT).getHistogram());
            Assertions.assertEquals(Double.NEGATIVE_INFINITY, result.getColumnStatistic(TEXT).getMinValue());
        }
    }

    @Test
    void dateStringRangesDoNotReadTailOrLexicalEndpoints() throws Exception {
        for (Type target : List.of(DateType.DATE, DateType.DATETIME)) {
            CastOperator cast = new CastOperator(target, TEXT);
            for (Histogram histogram : List.of(stored(), Histogram.forStrings(
                    List.of(new StringBucket("2020-01-01", "2026-01-01", 9_998_500L, 0L, 1000L, true, true)),
                    Map.of("2021-01-01", 750L, "2025-01-01", 750L)))) {
                Statistics input = input(TEXT, histogram);
                for (BinaryType op : List.of(BinaryType.LT, BinaryType.LE, BinaryType.GT, BinaryType.GE)) {
                    ConstantOperator constant = ConstantOperator.createDate(LocalDateTime.of(2024, 1, 1, 0, 0))
                            .castTo(target).orElseThrow();
                    Assertions.assertEquals(estimate(cast, op, constant, input(TEXT, null)).getOutputRowCount(),
                            estimate(cast, op, constant, input).getOutputRowCount(), 0.01);
                }
            }
        }
    }

    @Test
    void unsafeCastDoesNotReuseFiniteHistogramOrEndpoints() {
        Histogram histogram = new Histogram(List.of(new Bucket(1D, 10D, 100L, 10L)), Map.of("1", 100L));
        for (Type source : List.of(VarcharType.VARCHAR, IntegerType.BIGINT, FloatType.DOUBLE)) {
            ColumnRefOperator column = new ColumnRefOperator(1, source, "value", true);
            Statistics input = Statistics.builder().setOutputRowCount(200).addColumnStatistic(column,
                    ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(8)
                            .setMinValue(1).setMaxValue(10).setDistinctValuesCount(10)
                            .setHistogram(histogram).build()).build();
            Type target = source.isStringType() ? IntegerType.BIGINT : VarcharType.VARCHAR;
            ColumnStatistic result = ExpressionStatisticCalculator.calculate(new CastOperator(target, column), input);
            Assertions.assertNull(result.getHistogram());
            Assertions.assertEquals(Double.NEGATIVE_INFINITY, result.getMinValue());
            Assertions.assertEquals(Double.POSITIVE_INFINITY, result.getMaxValue());
        }
    }

    @Test
    void dateToDoubleCannotReuseTimestampBoundsOrHistogram() {
        for (Type sourceType : List.of(DateType.DATE, DateType.DATETIME)) {
            ColumnRefOperator date = new ColumnRefOperator(1, sourceType, "date_value", true);
            double min = Utils.getLongFromDateTime(LocalDateTime.of(2024, 1, 1, 0, 0));
            double max = Utils.getLongFromDateTime(LocalDateTime.of(2025, 1, 1, 0, 0));
            Histogram histogram = new Histogram(List.of(new Bucket(min, max, (long) ROWS, 1L)), Map.of());
            ColumnStatistic original = ColumnStatistic.builder().setMinValue(min).setMaxValue(max)
                    .setNullsFraction(0).setAverageRowSize(8).setDistinctValuesCount(366)
                    .setHistogram(histogram).build();
            Statistics input = Statistics.builder().setOutputRowCount(ROWS).addColumnStatistic(date, original).build();
            CastOperator cast = new CastOperator(FloatType.DOUBLE, date);
            ColumnStatistic converted = ExpressionStatisticCalculator.calculate(cast, input);
            Assertions.assertEquals(Double.NEGATIVE_INFINITY, converted.getMinValue());
            Assertions.assertEquals(Double.POSITIVE_INFINITY, converted.getMaxValue());
            Assertions.assertNull(converted.getHistogram());
            Assertions.assertEquals(ROWS / 2,
                    estimate(cast, BinaryType.LE, ConstantOperator.createDouble(1979), input).getOutputRowCount());
            Assertions.assertSame(histogram, original.getHistogram());
            Assertions.assertEquals(min, original.getMinValue());
        }
    }

    @Test
    void nestedUnsafeCastCannotRecoverOriginalHistogram() throws Exception {
        ScalarOperator cast = new CastOperator(IntegerType.LARGEINT, new CastOperator(IntegerType.BIGINT, TEXT));
        Statistics input = input(TEXT, stored());
        Assertions.assertNull(ExpressionStatisticCalculator.calculate(cast, input).getHistogram());
        Assertions.assertEquals(ROWS / 2, estimate(cast, BinaryType.LT,
                ConstantOperator.createBigint(500000), input).getOutputRowCount(), 0.01);
    }

    @Test
    void eqAndInKeepTailDenominatorWithoutCast() throws Exception {
        Statistics input = input(TEXT, stored());
        Assertions.assertEquals(750, estimate(TEXT, BinaryType.EQ, ConstantOperator.createVarchar("10"), input)
                .getOutputRowCount(), 0.01);
        Assertions.assertEquals(ROWS - 750, estimate(TEXT, BinaryType.NE, ConstantOperator.createVarchar("10"), input)
                .getOutputRowCount(), 0.01);
        Assertions.assertEquals(1500, PredicateStatisticsCalculator.statisticsCalculate(new InPredicateOperator(false,
                TEXT, ConstantOperator.createVarchar("10"), ConstantOperator.createVarchar("900000")), input)
                .getOutputRowCount(), 0.01);
        Assertions.assertEquals((ROWS - 1500) / (1_000_000 - 2),
                estimate(TEXT, BinaryType.EQ, ConstantOperator.createVarchar("not-in-head"), input)
                        .getOutputRowCount(), 0.01);
    }

    @Test
    void notInKeepsUnknownTailForFollowingRange() throws Exception {
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(new InPredicateOperator(true,
                TEXT, ConstantOperator.createVarchar("10")), input(TEXT, stored()));
        Assertions.assertEquals(ROWS - 750, result.getOutputRowCount(), 0.01);
        Assertions.assertTrue(result.getColumnStatistic(TEXT).getHistogram().hasUnknownRange());
        Assertions.assertTrue(BinaryPredicateStatisticCalculator.updateHistWithGreaterThan(
                result.getColumnStatistic(TEXT), Optional.of(ConstantOperator.createInt(500000)), false).isEmpty());
    }

    @Test
    void nullRowsAreExcludedAndSourceBoundsAreNotOverwritten() throws Exception {
        ColumnStatistic source = ColumnStatistic.buildFrom(input(TEXT, stored()).getColumnStatistic(TEXT))
                .setNullsFraction(0.25).setMinString("1").setMaxString("999999").build();
        Statistics input = Statistics.builder().setOutputRowCount(ROWS).addColumnStatistic(TEXT, source).build();
        CastOperator cast = new CastOperator(IntegerType.BIGINT, TEXT);
        ColumnStatistic transformed = ExpressionStatisticCalculator.calculate(cast, input);
        Assertions.assertNull(transformed.getMinString());
        Assertions.assertNull(transformed.getMaxString());
        for (BinaryType op : List.of(BinaryType.LT, BinaryType.LE, BinaryType.GT, BinaryType.GE)) {
            Statistics result = estimate(cast, op, ConstantOperator.createBigint(500000), input);
            Assertions.assertEquals(ROWS * 0.75 / 2, result.getOutputRowCount(), 0.01);
            Assertions.assertEquals("1", result.getColumnStatistic(TEXT).getMinString());
            Assertions.assertEquals("999999", result.getColumnStatistic(TEXT).getMaxString());
        }
    }

    @Test
    void zeroTailDoesNotLoseCompleteMcvRange() {
        ColumnStatistic statistic = ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(8)
                .setHistogram(new Histogram(List.of(new UnknownRangeBucket(0)), Map.of("10", 750L, "900000", 750L)))
                .setDistinctValuesCount(2).build();
        Assertions.assertEquals(750, BinaryPredicateStatisticCalculator.updateHistWithLessThan(statistic,
                Optional.of(ConstantOperator.createInt(500000)), false).orElseThrow().getTotalRows());
    }

    @Test
    void castEqAndInCannotUseUnconvertedMcvKeys() throws Exception {
        Histogram histogram = new Histogram(List.of(new UnknownRangeBucket(9_997_000)),
                Map.of("1", 750L, "01", 750L, "bad", 1500L));
        CastOperator cast = new CastOperator(IntegerType.BIGINT, TEXT);
        for (BinaryType op : List.of(BinaryType.EQ, BinaryType.NE)) {
            Assertions.assertEquals(estimate(cast, op, ConstantOperator.createBigint(1), input(TEXT, null))
                            .getOutputRowCount(),
                    estimate(cast, op, ConstantOperator.createBigint(1), input(TEXT, histogram)).getOutputRowCount(), 0.01);
        }
        InPredicateOperator in = new InPredicateOperator(false, cast, ConstantOperator.createBigint(1),
                ConstantOperator.createBigint(2));
        Assertions.assertEquals(PredicateStatisticsCalculator.statisticsCalculate(in, input(TEXT, null)).getOutputRowCount(),
                PredicateStatisticsCalculator.statisticsCalculate(in, input(TEXT, histogram)).getOutputRowCount(), 0.01);
    }

    @Test
    void wideningNumericCastsKeepHistogramAndEstimates() {
        List<Type[]> types = List.of(new Type[] {IntegerType.INT, IntegerType.BIGINT},
                new Type[] {IntegerType.BIGINT, IntegerType.LARGEINT},
                new Type[] {IntegerType.INT, FloatType.DOUBLE},
                new Type[] {FloatType.FLOAT, FloatType.DOUBLE},
                new Type[] {new DecimalType(PrimitiveType.DECIMAL32, 9, 2),
                        new DecimalType(PrimitiveType.DECIMAL64, 18, 4)});
        Histogram histogram = new Histogram(List.of(new Bucket(1D, 100D, 9_998_500L, 0L)), Map.of("10", 1500L));
        for (Type[] pair : types) {
            ColumnRefOperator column = new ColumnRefOperator(1, pair[0], "number", false);
            CastOperator cast = new CastOperator(pair[1], column);
            Statistics input = input(column, histogram);
            Assertions.assertSame(histogram, ExpressionStatisticCalculator.calculate(cast, input).getHistogram());
            for (BinaryType op : List.of(BinaryType.LT, BinaryType.LE, BinaryType.GT, BinaryType.GE, BinaryType.EQ)) {
                ConstantOperator constant = ConstantOperator.createInt(10).castTo(pair[1]).orElseThrow();
                Assertions.assertEquals(estimate(column, op, constant, input).getOutputRowCount(),
                        estimate(cast, op, constant, input).getOutputRowCount(), 0.01);
            }
        }
    }

    @Test
    void narrowingNumericCastDoesNotReuseDistribution() {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.BIGINT, "number", true);
        Histogram histogram = new Histogram(List.of(new Bucket(1D, 1000D, 1000L, 1L)), Map.of());
        Assertions.assertNull(ExpressionStatisticCalculator.calculate(new CastOperator(IntegerType.TINYINT, column),
                input(column, histogram)).getHistogram());
    }

    @Test
    void castConstantIsConvertedRatherThanUnwrapped() {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.BIGINT, "number", false);
        Statistics input = input(column, new Histogram(List.of(new Bucket(1D, 100D, 1000L, 1L)), Map.of()));
        ScalarOperator constant = new CastOperator(IntegerType.BIGINT, ConstantOperator.createVarchar("50"));
        Assertions.assertEquals(estimate(column, BinaryType.LT, ConstantOperator.createBigint(50), input).getOutputRowCount(),
                PredicateStatisticsCalculator.statisticsCalculate(new BinaryPredicateOperator(BinaryType.LT,
                        column, constant), input).getOutputRowCount(), 0.01);
    }

    @Test
    void datetimeToDateTransformsEndpointsAndDropsUntransformedHistogram() {
        ColumnRefOperator column = new ColumnRefOperator(1, DateType.DATETIME, "ts", false);
        LocalDateTime min = LocalDateTime.of(2025, 1, 1, 12, 30);
        LocalDateTime max = LocalDateTime.of(2025, 1, 3, 23, 0);
        Statistics input = Statistics.builder().setOutputRowCount(100).addColumnStatistic(column,
                ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(8)
                        .setMinValue(Utils.getLongFromDateTime(min)).setMaxValue(Utils.getLongFromDateTime(max))
                        .setDistinctValuesCount(100).setHistogram(new Histogram(Map.of(min.toString(), 100L))).build()).build();
        ColumnStatistic result = ExpressionStatisticCalculator.calculate(new CastOperator(DateType.DATE, column), input);
        Assertions.assertNull(result.getHistogram());
        Assertions.assertEquals(Utils.getLongFromDateTime(min.toLocalDate().atStartOfDay()), result.getMinValue());
        Assertions.assertEquals(Utils.getLongFromDateTime(max.toLocalDate().atStartOfDay()), result.getMaxValue());
    }
}
