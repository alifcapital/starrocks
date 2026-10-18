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
import com.starrocks.catalog.Function;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.ConstantOperatorUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.MonotonicFilterDerivation;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// The statistics of a cast follow the values of its source through the cast as the BE computes it, and comparisons of
// the cast are estimated from them. Each case compares the estimate with the exact count over the same values.
public class CastStatisticsTest {
    private static final LocalDateTime AUGUST = LocalDateTime.of(2026, 8, 1, 0, 0);

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

    // A column with its values and NULL rows, and its statistics as ANALYZE collects them
    private record Column(ColumnRefOperator ref, List<ConstantOperator> values, int nulls) {
        ColumnStatistic statistic() {
            double min = Double.POSITIVE_INFINITY;
            double max = Double.NEGATIVE_INFINITY;
            for (ConstantOperator value : values) {
                double number = ConstantOperatorUtils.doubleValueFromConstant(value).orElseThrow();
                min = Math.min(min, number);
                max = Math.max(max, number);
            }
            return ColumnStatistic.builder().setMinValue(min).setMaxValue(max)
                    .setNullsFraction(nulls / (double) rows()).setAverageRowSize(8)
                    .setDistinctValuesCount(new HashSet<>(values).size()).build();
        }

        int rows() {
            return values.size() + nulls;
        }

        Statistics statistics() {
            return statistics(statistic());
        }

        Statistics statistics(ColumnStatistic statistic) {
            return Statistics.builder().setOutputRowCount(rows()).addColumnStatistic(ref, statistic).build();
        }

        // The rows where the cast of the value satisfies the comparison, by the values the BE computes
        long count(Type type, BinaryType op, ConstantOperator constant) {
            long count = 0;
            for (ConstantOperator value : values) {
                ConstantOperator cast = StatisticsCastEvaluator.cast(value, type).orElseThrow();
                if (cast.isNull()) {
                    continue;
                }
                int cmp = cast.compareTo(constant);
                boolean match = switch (op) {
                    case EQ -> cmp == 0;
                    case GE -> cmp >= 0;
                    case GT -> cmp > 0;
                    case LE -> cmp <= 0;
                    case LT -> cmp < 0;
                    default -> throw new IllegalArgumentException(op.toString());
                };
                count += match ? 1 : 0;
            }
            return count;
        }

        long nullsOf(Type type) {
            return nulls + values.stream().filter(v -> StatisticsCastEvaluator.cast(v, type).orElseThrow().isNull())
                    .count();
        }
    }

    private static ColumnStatistic castStatistic(Column column, Type type) {
        return ExpressionStatisticCalculator.calculate(new CastOperator(type, column.ref()), column.statistics());
    }

    private static double rows(ScalarOperator predicate, Statistics statistics) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics).getOutputRowCount();
    }

    private static void assertClose(double exact, double estimate, double factor) {
        Assertions.assertTrue(estimate <= exact * factor + 1 && exact <= estimate * factor + 1,
                "exact " + exact + ", estimate " + estimate);
    }

    private static double seconds(LocalDateTime time) {
        return Utils.getLongFromDateTime(time);
    }

    private static ConstantOperator date(int day) {
        return ConstantOperator.createDate(AUGUST.plusDays(day - 1));
    }

    // Every hour of ten days of August and 60 NULL rows
    private static Column hours() {
        List<ConstantOperator> values = new ArrayList<>();
        for (int hour = 0; hour < 240; hour++) {
            values.add(ConstantOperator.createDatetime(AUGUST.plusHours(hour)));
        }
        return new Column(new ColumnRefOperator(1, DateType.DATETIME, "ts", true), values, 60);
    }

    @Test
    public void testDatetimeToDate() {
        Column ts = hours();
        ColumnStatistic cast = castStatistic(ts, DateType.DATE);
        Assertions.assertEquals(seconds(AUGUST), cast.getMinValue());
        Assertions.assertEquals(seconds(AUGUST.plusDays(9)), cast.getMaxValue());
        // Ten days, not the 240 values of the source
        Assertions.assertEquals(10, cast.getDistinctValuesCount());
        Assertions.assertEquals(0.2, cast.getNullsFraction(), 1e-9);

        CastOperator day = new CastOperator(DateType.DATE, ts.ref());
        Statistics statistics = ts.statistics();
        ScalarOperator third = new BinaryPredicateOperator(BinaryType.EQ, day, date(3));
        assertClose(ts.count(DateType.DATE, BinaryType.EQ, date(3)), rows(third, statistics), 1.01);
        ScalarOperator in = new InPredicateOperator(false, day, date(3), date(5), date(40));
        assertClose(48, rows(in, statistics), 1.01);
        assertClose(ts.nullsOf(DateType.DATE), rows(new IsNullPredicateOperator(false, day), statistics), 1.01);
        assertClose(240, rows(new IsNullPredicateOperator(true, day), statistics), 1.01);
        // Two comparisons of one value are one value, not two halves of the range
        ScalarOperator oneDay = Utils.compoundAnd(new BinaryPredicateOperator(BinaryType.GE, day, date(3)),
                new BinaryPredicateOperator(BinaryType.LE, day, date(3)));
        assertClose(24, rows(oneDay, statistics), 1.01);
        ScalarOperator days = Utils.compoundAnd(new BinaryPredicateOperator(BinaryType.GE, day, date(3)),
                new BinaryPredicateOperator(BinaryType.LE, day, date(6)));
        assertClose(96, rows(days, statistics), 1.5);
        ScalarOperator late = new BinaryPredicateOperator(BinaryType.GE, day, date(5));
        assertClose(ts.count(DateType.DATE, BinaryType.GE, date(5)), rows(late, statistics), 1.5);
        // GROUP BY the day has the days of the range
        Assertions.assertEquals(10, RuntimeFilterStatistics.fromExpression(day, statistics).getNdv(), 1);
    }

    @Test
    public void testDatetimeToDateGivesASharedDayToTheBucketBefore() {
        // One row an hour: the second bucket starts at noon of the last day of the first one
        Column ts = hours();
        Histogram histogram = new Histogram(List.of(
                new Bucket(seconds(AUGUST), seconds(AUGUST.plusHours(11)), 12L, 1L, 12L),
                new Bucket(seconds(AUGUST.plusHours(12)), seconds(AUGUST.plusHours(239)), 240L, 1L, 228L)),
                Map.of());
        ColumnStatistic withHistogram = ColumnStatistic.buildFrom(ts.statistic()).setHistogram(histogram).build();
        ColumnStatistic cast = ExpressionStatisticCalculator.calculate(new CastOperator(DateType.DATE, ts.ref()),
                ts.statistics(withHistogram));
        List<Bucket> buckets = cast.getHistogram().getBuckets();
        Assertions.assertEquals(2, buckets.size(), cast.toString());
        // Aug 1 holds the 12 rows of the first bucket and the 12 hours of the second one on that day
        Assertions.assertEquals(seconds(AUGUST), buckets.get(0).getLower());
        Assertions.assertEquals(seconds(AUGUST), buckets.get(0).getUpper());
        Assertions.assertEquals(24L, buckets.get(0).getCount());
        Assertions.assertEquals(24L, buckets.get(0).getUpperRepeats());
        // The second bucket starts on Aug 2, and its last day takes the 24 hours of Aug 10
        Assertions.assertEquals(seconds(AUGUST.plusDays(1)), buckets.get(1).getLower());
        Assertions.assertEquals(seconds(AUGUST.plusDays(9)), buckets.get(1).getUpper());
        Assertions.assertEquals(240L, buckets.get(1).getCount());
        Assertions.assertEquals(24L, buckets.get(1).getUpperRepeats());
        Assertions.assertEquals(9L, buckets.get(1).getDistinctCount().orElseThrow());
    }

    @Test
    public void testNarrowingTurnsOutsideValuesIntoNull() {
        List<ConstantOperator> values = new ArrayList<>();
        for (long value = -300; value <= 300; value++) {
            values.add(ConstantOperator.createBigint(value));
        }
        Column big = new Column(new ColumnRefOperator(1, IntegerType.BIGINT, "big", true), values, 0);
        ColumnStatistic cast = castStatistic(big, IntegerType.TINYINT);
        Assertions.assertEquals(-128, cast.getMinValue());
        Assertions.assertEquals(127, cast.getMaxValue());
        Assertions.assertEquals(256, cast.getDistinctValuesCount());
        Assertions.assertEquals(big.nullsOf(IntegerType.TINYINT) / (double) big.rows(), cast.getNullsFraction(), 0.01);
        CastOperator tiny = new CastOperator(IntegerType.TINYINT, big.ref());
        ConstantOperator hundred = ConstantOperator.createTinyInt((byte) 100);
        assertClose(big.count(IntegerType.TINYINT, BinaryType.GE, hundred),
                rows(new BinaryPredicateOperator(BinaryType.GE, tiny, hundred), big.statistics()), 1.1);
        assertClose(big.nullsOf(IntegerType.TINYINT),
                rows(new IsNullPredicateOperator(false, tiny), big.statistics()), 1.01);
    }

    @Test
    public void testFloatingPointToIntegerTruncatesTowardZero() {
        List<ConstantOperator> values = new ArrayList<>();
        for (double value : new double[] {-1.5, -1.0, -0.5, -0.0, 0.5, 1.0, 1.5}) {
            values.add(ConstantOperator.createDouble(value));
        }
        Column number = new Column(new ColumnRefOperator(1, FloatType.DOUBLE, "d", true), values, 0);
        ColumnStatistic cast = castStatistic(number, IntegerType.INT);
        Assertions.assertEquals(-1, cast.getMinValue());
        Assertions.assertEquals(1, cast.getMaxValue());
        Assertions.assertEquals(3, cast.getDistinctValuesCount());
        Assertions.assertEquals(0, cast.getNullsFraction());
    }

    @Test
    public void testWrapAroundKeepsNoRange() {
        List<ConstantOperator> values = new ArrayList<>();
        for (int day = 1; day <= 10; day++) {
            values.add(date(day));
        }
        Column dates = new Column(new ColumnRefOperator(1, DateType.DATE, "d", true), values, 0);
        ColumnStatistic wrapped = castStatistic(dates, IntegerType.SMALLINT);
        Assertions.assertTrue(wrapped.isInfiniteRange());
        Assertions.assertEquals(10, wrapped.getDistinctValuesCount());
        // YYYYMMDD in an INT keeps the order
        ColumnStatistic literal = castStatistic(dates, IntegerType.INT);
        Assertions.assertEquals(20260801, literal.getMinValue());
        Assertions.assertEquals(20260810, literal.getMaxValue());
        Assertions.assertEquals(10, literal.getDistinctValuesCount());
        // A year below 1000 keeps its four digits
        Column early = new Column(dates.ref(), List.of(ConstantOperator.createDate(LocalDateTime.of(500, 1, 1, 0, 0)),
                ConstantOperator.createDate(LocalDateTime.of(500, 12, 31, 0, 0))), 0);
        ColumnStatistic earlyLiteral = castStatistic(early, IntegerType.INT);
        Assertions.assertEquals(5000101, earlyLiteral.getMinValue());
        Assertions.assertEquals(5001231, earlyLiteral.getMaxValue());
    }

    @Test
    public void testNumberToDateKeepsTheOrderInsideASegment() {
        ColumnRefOperator ref = new ColumnRefOperator(1, IntegerType.BIGINT, "n", true);
        List<ConstantOperator> eightDigits = new ArrayList<>();
        for (long day = 20260801; day <= 20260810; day++) {
            eightDigits.add(ConstantOperator.createBigint(day));
        }
        Column days = new Column(ref, eightDigits, 0);
        ColumnStatistic cast = castStatistic(days, DateType.DATE);
        Assertions.assertEquals(seconds(AUGUST), cast.getMinValue());
        Assertions.assertEquals(seconds(AUGUST.plusDays(9)), cast.getMaxValue());
        Assertions.assertEquals(10, cast.getDistinctValuesCount());
        CastOperator day = new CastOperator(DateType.DATE, ref);
        assertClose(1, rows(new BinaryPredicateOperator(BinaryType.EQ, day, date(5)), days.statistics()), 1.01);
        // Fourteen digits are datetimes
        Column times = new Column(ref, List.of(ConstantOperator.createBigint(20260801000000L),
                ConstantOperator.createBigint(20260801235959L)), 0);
        ColumnStatistic datetime = castStatistic(times, DateType.DATETIME);
        Assertions.assertEquals(seconds(AUGUST), datetime.getMinValue());
        Assertions.assertEquals(seconds(AUGUST.plusSeconds(86399)), datetime.getMaxValue());
        // Between the segments, at a date that does not exist and below zero the order is lost
        for (List<Long> range : List.of(List.of(260801L, 20260810L), List.of(20260800L, 20260810L),
                List.of(-20260801L, 20260810L), List.of(20260801L, 20260801000000L))) {
            Column mixed = new Column(ref, List.of(ConstantOperator.createBigint(range.get(0)),
                    ConstantOperator.createBigint(range.get(1))), 0);
            Assertions.assertTrue(castStatistic(mixed, DateType.DATE).isInfiniteRange(), range.toString());
        }
    }

    @Test
    public void testDatetimeToNumberDropsMicroseconds() {
        List<ConstantOperator> values = new ArrayList<>();
        for (int micro = 0; micro < 100; micro++) {
            values.add(ConstantOperator.createDatetime(AUGUST.plusNanos(micro * 15_000_000L)));
        }
        Column ts = new Column(new ColumnRefOperator(1, DateType.DATETIME, "ts", true), values, 0);
        ColumnStatistic cast = castStatistic(ts, IntegerType.BIGINT);
        Assertions.assertEquals(20260801000000L, cast.getMinValue());
        Assertions.assertEquals(20260801000001L, cast.getMaxValue());
        Assertions.assertEquals(2, cast.getDistinctValuesCount());
        // DECIMAL32 cannot hold a datetime: every value is NULL
        ColumnStatistic decimal = castStatistic(ts, new DecimalType(PrimitiveType.DECIMAL32, 9, 0));
        Assertions.assertEquals(1, decimal.getNullsFraction());
        Assertions.assertEquals(0, decimal.getDistinctValuesCount());
        ScalarOperator isNull = new IsNullPredicateOperator(false,
                new CastOperator(new DecimalType(PrimitiveType.DECIMAL32, 9, 0), ts.ref()));
        assertClose(100, rows(isNull, ts.statistics()), 1.01);
    }

    @Test
    public void testBoundThroughACastOfAColumn() {
        // hours_add(CAST(d AS DATETIME), 5) between two times of one day bounds CAST(d AS DATETIME) to that day,
        // which the statistics of the cast estimate as one value of ten
        List<ConstantOperator> values = new ArrayList<>();
        for (int day = 1; day <= 10; day++) {
            for (int i = 0; i < 30; i++) {
                values.add(date(day));
            }
        }
        Column dates = new Column(new ColumnRefOperator(1, DateType.DATE, "d", true), values, 0);
        Function hoursAdd = ExprUtils.getBuiltinFunction("hours_add",
                new Type[] {DateType.DATETIME, IntegerType.INT}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        CallOperator shifted = new CallOperator("hours_add", DateType.DATETIME,
                ImmutableList.of(new CastOperator(DateType.DATETIME, dates.ref()), ConstantOperator.createInt(5)),
                hoursAdd);
        ConstantOperator fiveAm = ConstantOperator.createDatetime(AUGUST.plusDays(2).plusHours(5));
        BinaryPredicateOperator from = new BinaryPredicateOperator(BinaryType.GE, shifted, fiveAm);
        MonotonicFilterDerivation.ColumnBounds bounds = MonotonicFilterDerivation.columnBoundsForEstimate(from);
        Assertions.assertNotNull(bounds);
        Assertions.assertFalse(bounds.wider());
        ScalarOperator window = Utils.compoundAnd(from, new BinaryPredicateOperator(BinaryType.LE, shifted, fiveAm));
        assertClose(30, rows(window, dates.statistics()), 1.01);
        // A comparison of a cast is left to the statistics of the cast
        Assertions.assertNull(MonotonicFilterDerivation.columnBoundsForEstimate(new BinaryPredicateOperator(
                BinaryType.GE, new CastOperator(DateType.DATETIME, dates.ref()), fiveAm)));
    }

    private static Column column(Type type, List<ConstantOperator> values) {
        return new Column(new ColumnRefOperator(1, type, "c", true), values, 0);
    }

    @Test
    public void testPrecisionLossKeepsTheOrder() {
        // BIGINT to DOUBLE rounds above 2^53, and several values meet
        List<ConstantOperator> bigints = new ArrayList<>();
        for (long value = (1L << 53); value <= (1L << 53) + 10; value++) {
            bigints.add(ConstantOperator.createBigint(value));
        }
        ColumnStatistic toDouble = castStatistic(column(IntegerType.BIGINT, bigints), FloatType.DOUBLE);
        Assertions.assertEquals(0x1p53, toDouble.getMinValue());
        Assertions.assertEquals(0x1p53 + 10, toDouble.getMaxValue());
        Assertions.assertTrue(toDouble.getDistinctValuesCount() <= 11);
        // DOUBLE to FLOAT turns the values past the range of FLOAT into NULL
        ColumnStatistic toFloat = castStatistic(column(FloatType.DOUBLE, List.of(ConstantOperator.createDouble(-1e39),
                ConstantOperator.createDouble(1), ConstantOperator.createDouble(1e39))), FloatType.FLOAT);
        Assertions.assertEquals(-Float.MAX_VALUE, toFloat.getMinValue(), 1e30);
        Assertions.assertEquals(Float.MAX_VALUE, toFloat.getMaxValue(), 1e30);
        Assertions.assertTrue(toFloat.getNullsFraction() > 0.5, toFloat.toString());
        // DATE to FLOAT: YYYYMMDD with the precision of FLOAT
        ColumnStatistic dateToFloat = castStatistic(column(DateType.DATE, List.of(date(1), date(31))), FloatType.FLOAT);
        Assertions.assertEquals((float) 20260801, dateToFloat.getMinValue());
        Assertions.assertEquals((float) 20260831, dateToFloat.getMaxValue());
    }

    @Test
    public void testDecimals() {
        DecimalType money = new DecimalType(PrimitiveType.DECIMAL64, 10, 2);
        List<ConstantOperator> amounts = new ArrayList<>();
        for (int cents = -1299; cents <= 1299; cents += 100) {
            amounts.add(ConstantOperator.createDecimal(java.math.BigDecimal.valueOf(cents, 2), money));
        }
        // To an integer toward zero
        ColumnStatistic toInt = castStatistic(column(money, amounts), IntegerType.INT);
        Assertions.assertEquals(-12, toInt.getMinValue());
        Assertions.assertEquals(12, toInt.getMaxValue());
        Assertions.assertTrue(toInt.getDistinctValuesCount() <= 25);
        // To a smaller scale half away from zero: ten values of thousandths fall on two hundredths
        DecimalType thousandths = new DecimalType(PrimitiveType.DECIMAL64, 10, 3);
        List<ConstantOperator> fine = new ArrayList<>();
        for (int value = 1000; value <= 1009; value++) {
            fine.add(ConstantOperator.createDecimal(java.math.BigDecimal.valueOf(value, 3), thousandths));
        }
        ColumnStatistic rounded = castStatistic(column(thousandths, fine), money);
        Assertions.assertEquals(1.0, rounded.getMinValue(), 1e-9);
        Assertions.assertEquals(1.01, rounded.getMaxValue(), 1e-9);
        Assertions.assertEquals(2, rounded.getDistinctValuesCount());
        // From a double in double arithmetic: 1.005 becomes 1.00
        ColumnStatistic fromDouble = castStatistic(column(FloatType.DOUBLE,
                List.of(ConstantOperator.createDouble(1.005), ConstantOperator.createDouble(2.5))), money);
        Assertions.assertEquals(1.0, fromDouble.getMinValue(), 1e-9);
        Assertions.assertEquals(2.5, fromDouble.getMaxValue(), 1e-9);
        // A date as YYYYMMDD with the scale
        ColumnStatistic fromDate = castStatistic(column(DateType.DATE, List.of(date(1), date(10))), money);
        Assertions.assertEquals(20260801, fromDate.getMinValue(), 1e-9);
        Assertions.assertEquals(20260810, fromDate.getMaxValue(), 1e-9);
        Assertions.assertEquals(2, fromDate.getDistinctValuesCount());
    }

    @Test
    public void testTextToNumberKeepsNoRange() {
        List<ConstantOperator> texts = new ArrayList<>();
        for (String value : new String[] {"9", "10", "100", "x"}) {
            texts.add(ConstantOperator.createVarchar(value));
        }
        Column text = new Column(new ColumnRefOperator(1, com.starrocks.type.VarcharType.VARCHAR, "s", true), texts, 0);
        ColumnStatistic number = ExpressionStatisticCalculator.calculate(new CastOperator(IntegerType.INT, text.ref()),
                Statistics.builder().setOutputRowCount(4).addColumnStatistic(text.ref(), ColumnStatistic.builder()
                        .setNullsFraction(0).setAverageRowSize(3).setDistinctValuesCount(4).build()).build());
        Assertions.assertTrue(number.isInfiniteRange());
        Assertions.assertEquals(4, number.getDistinctValuesCount());
    }

    @Test
    public void testLastValueOfAMergingImageKeepsItsRows() {
        // 100 rows on the morning of Aug 1 and 100 rows spread from Aug 1 noon to the end of Aug 2: CAST AS DATE puts
        // about 67 of them on Aug 2, not the one row of the last timestamp
        ColumnRefOperator ts = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
        double start = seconds(AUGUST);
        double noon = seconds(AUGUST.plusHours(12));
        double end = seconds(AUGUST.plusDays(2).minusSeconds(1));
        Histogram histogram = new Histogram(List.of(new Bucket(start, noon, 100L, 1L, 100L),
                new Bucket(noon + 1, end, 200L, 1L, 100L)), Map.of());
        Statistics statistics = Statistics.builder().setOutputRowCount(200).addColumnStatistic(ts, ColumnStatistic
                .builder().setMinValue(start).setMaxValue(end).setNullsFraction(0).setAverageRowSize(8)
                .setDistinctValuesCount(200).setHistogram(histogram).build()).build();
        CastOperator day = new CastOperator(DateType.DATE, ts);
        assertClose(67, rows(new BinaryPredicateOperator(BinaryType.EQ, day, date(2)), statistics), 1.5);
        assertClose(67, rows(new BinaryPredicateOperator(BinaryType.GE, day, date(2)), statistics), 1.5);
        assertClose(133, rows(new BinaryPredicateOperator(BinaryType.LT, day, date(2)), statistics), 1.5);
    }

    @Test
    public void testInOverACastThatReordersKeepsItsDefault() {
        List<ConstantOperator> values = new ArrayList<>();
        for (int value = 1; value <= 100; value++) {
            values.add(ConstantOperator.createInt(value));
        }
        Column number = new Column(new ColumnRefOperator(1, IntegerType.INT, "i", true), values, 0);
        ScalarOperator in = new InPredicateOperator(false, new CastOperator(com.starrocks.type.VarcharType.VARCHAR,
                number.ref()), ConstantOperator.createVarchar("1"), ConstantOperator.createVarchar("2"));
        Assertions.assertEquals(100 * StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT,
                rows(in, number.statistics()), 1e-6);
    }

    @Test
    public void testOnlyBoundsOnTheColumnReduceToTheColumn() {
        // The statistics on expressions leave a condition to the column only when it bounds the column itself
        Function hoursAdd = ExprUtils.getBuiltinFunction("hours_add",
                new Type[] {DateType.DATETIME, IntegerType.INT}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        ConstantOperator time = ConstantOperator.createDatetime(AUGUST);
        for (Type type : new Type[] {DateType.DATE, DateType.DATETIME, com.starrocks.type.VarcharType.VARCHAR}) {
            ColumnRefOperator column = new ColumnRefOperator(1, type, "c", true);
            ScalarOperator argument = type.isDatetime() ? column : new CastOperator(DateType.DATETIME, column);
            BinaryPredicateOperator predicate = new BinaryPredicateOperator(BinaryType.GE, new CallOperator(
                    "hours_add", DateType.DATETIME, ImmutableList.of(argument, ConstantOperator.createInt(5)),
                    hoursAdd), time);
            Assertions.assertEquals(!type.isStringType(), MonotonicFilterDerivation.reducesToColumnBounds(predicate),
                    type.toString());
            Assertions.assertNotNull(MonotonicFilterDerivation.columnBoundsForEstimate(predicate), type.toString());
        }
    }

    @Test
    public void testUnorderedCastKeepsNoRange() {
        List<ConstantOperator> values = new ArrayList<>();
        for (int value = 1; value <= 100; value++) {
            values.add(ConstantOperator.createInt(value));
        }
        Column number = new Column(new ColumnRefOperator(1, IntegerType.INT, "i", true), values, 0);
        ColumnStatistic text = castStatistic(number, com.starrocks.type.VarcharType.VARCHAR);
        Assertions.assertTrue(text.isInfiniteRange());
        Assertions.assertEquals(100, text.getDistinctValuesCount());
        Assertions.assertNull(text.getHistogram());
        Assertions.assertEquals(Optional.empty(), Optional.ofNullable(CastStatisticsDerivation.derive(
                number.statistic(), IntegerType.INT, com.starrocks.type.BooleanType.BOOLEAN)));
    }
}
