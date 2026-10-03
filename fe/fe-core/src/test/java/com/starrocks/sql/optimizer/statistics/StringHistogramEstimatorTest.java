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
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class StringHistogramEstimatorTest {
    private static final ColumnRefOperator COLUMN = new ColumnRefOperator(1, VarcharType.VARCHAR, "created_at", true);

    private static Histogram histogram() {
        return new Histogram(List.of(new StringBucket("2018", "2020", 100, 10, 90),
                new StringBucket("2021", "2026", 900, 20, 700)), Map.of("2023", 100L));
    }

    private static Statistics statistics(Histogram histogram) {
        return Statistics.builder().setOutputRowCount(2000)
                .addColumnStatistic(COLUMN, ColumnStatistic.builder().setHistogram(histogram)
                        .setDistinctValuesCount(791).setNullsFraction(0.1).setAverageRowSize(27).build()).build();
    }

    private static Statistics estimate(Statistics input, BinaryType op, String value) {
        return PredicateStatisticsCalculator.statisticsCalculate(
                new BinaryPredicateOperator(op, COLUMN, ConstantOperator.createVarchar(value)), input);
    }

    @Test
    void indexedPointsPreserveBoundaryAndGapSemantics() {
        var buckets = java.util.List.<Bucket>of(
                new StringBucket("a", "b", 10, 0, 2),
                new StringBucket("b", "d", 30, 4, 3),
                new StringBucket("d", "d", 30, 0, 0),
                new StringBucket("f", "😀", 50, 2, 5));
        Histogram histogram = Histogram.forStrings(buckets, java.util.Map.of("c", 19L));
        for (String value : java.util.List.of("", "a", "b", "c", "d", "e", "f", "z", "😀", "😁")) {
            double expected = histogram.getMCV().getOrDefault(value, 0L);
            long previous = 0;
            if (expected == 0) {
                for (Bucket raw : buckets) {
                    var bucket = (StringBucket) raw;
                    double rows = bucket.pointRows(value, bucket.getCount() - previous);
                    previous = bucket.getCount();
                    if (rows > 0) {
                        expected = rows;
                        break;
                    }
                }
            }
            org.junit.jupiter.api.Assertions.assertEquals(expected, StringHistogramEstimator.pointRows(histogram, value));
        }
    }

    @Test
    public void testOutsideAndBoundaryRangesIncludeExactUpperRepeats() {
        Histogram h = histogram();
        Assertions.assertEquals(0, StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, "2010", BinaryType.LT)));
        Assertions.assertEquals(1000, StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, "2010", BinaryType.GE)));
        Assertions.assertEquals(0, StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, "2030", BinaryType.GE)));
        Assertions.assertEquals(900, StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, "2021", BinaryType.GE)));
        Assertions.assertEquals(910, StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, "2020", BinaryType.GE)));
        Assertions.assertEquals(900, StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, "2020", BinaryType.GT)));
        Assertions.assertEquals(20, StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, "2026", BinaryType.GE)));
        Assertions.assertEquals(0, StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, "2026", BinaryType.GT)));
        Assertions.assertEquals(1620, estimate(statistics(h), BinaryType.GE, "2021").getOutputRowCount(), 1e-9);
    }

    @Test
    public void testOnlyBoundaryBucketIsEstimatedAndComplementaryRangesAgree() {
        Histogram h = histogram();
        for (String cut : List.of("2010", "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2026", "2030")) {
            long less = StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, cut, BinaryType.LT));
            long ge = StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, cut, BinaryType.GE));
            Assertions.assertEquals(1000, less + ge, cut);
            long le = StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, cut, BinaryType.LE));
            long greater = StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, cut, BinaryType.GT));
            Assertions.assertEquals(1000, le + greater, cut);
            Assertions.assertTrue(less <= le, cut);
        }
        long kept = StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, "2024", BinaryType.GE));
        Assertions.assertTrue(kept >= 400 && kept <= 420);
        // A head value cannot also contribute estimated equality rows from the residual bucket.
        Assertions.assertEquals(100, StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, "2023", BinaryType.LE))
                - StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, "2023", BinaryType.LT)));
        Histogram odd = new Histogram(List.of(new StringBucket("a", "z", 101, 1, 101)), Map.of());
        Assertions.assertEquals(101, StringHistogramEstimator.rows(StringHistogramEstimator.filter(odd, "m", BinaryType.LT))
                + StringHistogramEstimator.rows(StringHistogramEstimator.filter(odd, "m", BinaryType.GE)));
    }

    @Test
    public void testClippedIntervalsRetainExclusivityAndNdv() {
        Histogram once = StringHistogramEstimator.filter(histogram(), "2024", BinaryType.GT);
        Histogram twice = StringHistogramEstimator.filter(once, "2024", BinaryType.GT);
        Assertions.assertEquals(StringHistogramEstimator.rows(once), StringHistogramEstimator.rows(twice));
        Assertions.assertEquals(0, StringHistogramEstimator.pointRows(once, "2024"));
        Assertions.assertEquals(0, StringHistogramEstimator.rows(StringHistogramEstimator.filter(once, "2024", BinaryType.LE)));
        StringBucket b = (StringBucket) once.getBuckets().get(0);
        Assertions.assertFalse(b.isLowerInclusive());
        Assertions.assertTrue(b.getDistinctCount().orElseThrow() < 700);
        Assertions.assertTrue(b.getDistinctCount().orElseThrow() <= b.getCount());
    }

    @Test
    public void testEqualityAndInUseBucketNdvAndKnownHeadCounts() {
        Statistics input = statistics(histogram());
        Assertions.assertEquals(180, estimate(input, BinaryType.EQ, "2023").getOutputRowCount(), 1e-9);
        Assertions.assertEquals(36, estimate(input, BinaryType.EQ, "2026").getOutputRowCount(), 1e-9);
        double eq = estimate(input, BinaryType.EQ, "2024").getOutputRowCount();
        double ne = estimate(input, BinaryType.NE, "2024").getOutputRowCount();
        Assertions.assertEquals(1800, eq + ne, 1e-9);
        Assertions.assertNull(estimate(input, BinaryType.NE, "2024").getColumnStatistic(COLUMN).getHistogram());
        InPredicateOperator in = new InPredicateOperator(false, COLUMN, ConstantOperator.createVarchar("2023"),
                ConstantOperator.createVarchar("2026"));
        Assertions.assertEquals(216, PredicateStatisticsCalculator.statisticsCalculate(in, input).getOutputRowCount(), 1e-9);
    }

    @Test
    public void testByteOrderEmptyStringsAndLongSharedPrefixes() {
        String prefix = "x".repeat(300);
        Histogram h = new Histogram(List.of(new StringBucket("", "é", 200, 20, 100),
                new StringBucket(String.valueOf((char) 0xE000), "😀", 300, 10, 80)), Map.of(prefix + "z", 3L));
        Assertions.assertTrue(StringBucket.compare(String.valueOf((char) 0xE000), "😀") < 0);
        Assertions.assertEquals(0, StringHistogramEstimator.rows(StringHistogramEstimator.filter(h, "", BinaryType.LT)));
        Assertions.assertEquals(100, StringHistogramEstimator.rows(
                StringHistogramEstimator.filter(h, String.valueOf((char) 0xE000), BinaryType.GE)));
        Assertions.assertEquals(3, StringHistogramEstimator.pointRows(h, prefix + "z"));
        Assertions.assertTrue(StringBucket.compare(prefix + "a", prefix + "z") < 0);
    }

    @Test
    public void testPrefixLikeUsesStringRangesAndPreservesFilteredBuckets() {
        Histogram h = new Histogram(List.of(new StringBucket("ab0", "ab9", 100, 10, 10),
                new StringBucket("ac", "az", 200, 1, 90)), Map.of("abX", 10L));
        Statistics input = statistics(h);
        LikePredicateOperator like = new LikePredicateOperator(COLUMN, ConstantOperator.createVarchar("ab%"));
        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(like, input);
        Assertions.assertEquals(1800 * 110.0 / 210, result.getOutputRowCount(), 1e-9);
        Assertions.assertEquals(110, StringHistogramEstimator.rows(result.getColumnStatistic(COLUMN).getHistogram()));
        Assertions.assertEquals("a_", LikePatternEstimator.compile("a\\_%").fixedPrefix().orElseThrow());
        Assertions.assertTrue(LikePatternEstimator.compile("%ab%").fixedPrefix().isEmpty());
        Assertions.assertEquals(1800, StringHistogramEstimator.prefix(COLUMN, input.getColumnStatistic(COLUMN), "", input)
                .getOutputRowCount(), 1e-9);
    }

    @Test
    public void testStringTypeSurvivesHeadOnlyFilteringAndDump() {
        Statistics equal = estimate(statistics(histogram()), BinaryType.EQ, "2023");
        Histogram head = equal.getColumnStatistic(COLUMN).getHistogram();
        Assertions.assertTrue(head.hasStringValues());
        Assertions.assertTrue(HistogramUtils.deserializeHistogram(HistogramUtils.serializeHistogram(head)).hasStringValues());
        // Planner row counts have a global floor of one, even for an empty estimated distribution.
        Assertions.assertEquals(1, estimate(equal, BinaryType.LT, "2020").getOutputRowCount());
        Assertions.assertEquals(equal.getOutputRowCount(), estimate(equal, BinaryType.GT, "2020").getOutputRowCount());
    }

    @Test
    public void testCastsDoNotReuseTextOrderingForNumbersOrDates() {
        Statistics input = statistics(histogram());
        Assertions.assertNull(ExpressionStatisticCalculator.calculate(new CastOperator(IntegerType.BIGINT, COLUMN), input)
                .getHistogram());
        Assertions.assertNull(ExpressionStatisticCalculator.calculate(new CastOperator(DateType.DATE, COLUMN), input)
                .getHistogram());
        Assertions.assertTrue(ExpressionStatisticCalculator.calculate(new CastOperator(VarcharType.VARCHAR, COLUMN), input)
                .getHistogram().hasStringValues());
    }

    @Test
    public void testDumpRoundTripKeepsStringTypeAndClippedEndpoints() {
        Histogram h = StringHistogramEstimator.filter(histogram(), "2024", BinaryType.GT);
        String json = HistogramUtils.serializeHistogram(h);
        Histogram restored = HistogramUtils.deserializeHistogram(json);
        Assertions.assertTrue(restored.hasStringValues());
        Assertions.assertEquals(json, HistogramUtils.serializeHistogram(restored));
        Assertions.assertEquals(0, StringHistogramEstimator.pointRows(restored, "2024"));
    }

    @Test
    public void testMcvRecordAttachesStringBucketsWithoutNumericConversion() {
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("created_at"), 1000, 791,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("2023"), 100)),
                List.of(List.of("2018", "2020", "100", "10", "90"),
                        List.of("2021", "2026", "900", "20", "700")), List.of(0L));
        ColumnStatistic column = group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        Assertions.assertTrue(column.getHistogram().hasStringValues());
        Assertions.assertEquals(1000, column.getHistogram().getTotalRows());
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, column.getMinValue());
        Assertions.assertEquals(900, StringHistogramEstimator.rows(
                StringHistogramEstimator.filter(column.getHistogram(), "2021", BinaryType.GE)));
    }
}
