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
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExpressionProjectionStatisticsTest {
    private static final ColumnRefOperator C = new ColumnRefOperator(1, VarcharType.VARCHAR, "c", true);
    private static final ColumnRefOperator X = new ColumnRefOperator(-1, VarcharType.VARCHAR, "expr#1", true);

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

    private static ScalarOperator left(int length) {
        return call("left", C, ConstantOperator.createInt(length));
    }

    // 600 rows start with 103, 300 rows with 104, 100 rows are NULL.
    private static ScanExpressionStatistics expressions() {
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        entries.add(new MultiColumnCombinedStats.McvEntry(List.of("10312345"), 600, List.of(600L)));
        entries.add(new MultiColumnCombinedStats.McvEntry(List.of("10400000"), 300, List.of(300L)));
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("x"), 1000, 3, entries,
                List.of(), List.of(100L));
        ColumnStatistic statistic = group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(left(8));
        return new ScanExpressionStatistics("t", List.of(new ScanExpressionStatistics.Entry(canonical,
                ExpressionStatisticsCanonicalizer.keyOf(canonical), X, statistic)),
                Map.of(Set.of(X), new MultiColumnCombinedStats(3, 1000, List.of(X), entries, List.of(100L))));
    }

    @Test
    public void testPrefixOfTheExpressionMapsTheHead() {
        ColumnStatistic statistic = ExpressionProjectionStatistics.calculate(left(3), expressions(), 1000);
        Assertions.assertNotNull(statistic);
        Assertions.assertEquals(Map.of("103", 600L, "104", 300L), statistic.getHistogram().getMCV());
        Assertions.assertEquals(2, statistic.getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(0.1, statistic.getNullsFraction(), 1e-9);

        // A substring after the first position maps the head values by substr as well.
        ColumnStatistic middle = ExpressionProjectionStatistics.calculate(
                call("substr", C, ConstantOperator.createInt(4), ConstantOperator.createInt(2)), expressions(), 1000);
        Assertions.assertEquals(Map.of("12", 600L, "00", 300L), middle.getHistogram().getMCV());

        // The expression itself takes the statistics of x.
        ColumnStatistic identity = ExpressionProjectionStatistics.calculate(left(8), expressions(), 1000);
        Assertions.assertEquals(2, identity.getHistogram().getMCV().size());
        // The NDV does not exceed the rows of the scan output.
        Assertions.assertEquals(1, ExpressionProjectionStatistics.calculate(left(3), expressions(), 1)
                .getDistinctValuesCount(), 1e-9);
        // A substring past the prefix and an expression of another column are not answered.
        Assertions.assertNull(ExpressionProjectionStatistics.calculate(left(9), expressions(), 1000));
        Assertions.assertNull(ExpressionProjectionStatistics.calculate(C, expressions(), 1000));
    }

    @Test
    public void testOnlyStatisticsThatDescribeTheScanRows() {
        ScanExpressionStatistics notDescribing = expressions().afterFilter(Map.of(), Set.of(), null, false);
        Assertions.assertNull(ExpressionProjectionStatistics.calculate(left(3), notDescribing, 1000));
    }

    @Test
    public void testOrIsBuiltAgainFromTheUnfilteredStatistics() {
        ScalarOperator or = Utils.compoundOr(
                new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, X, ConstantOperator.createVarchar("103%")),
                new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, X, ConstantOperator.createVarchar("104%")));
        ScanExpressionStatistics base = expressions();
        // The estimate of an OR keeps the histogram of its first branch only.
        ColumnStatistic firstBranch = ColumnStatistic.buildFrom(base.getEntries().get(0).getStatistic())
                .setHistogram(Histogram.forStrings(List.of(), Map.of("10312345", 600L))).setDistinctValuesCount(1)
                .build();
        ScanExpressionStatistics after = base.afterFilter(Map.of(X, firstBranch), Set.of(X), or, true);
        ColumnStatistic statistic = ExpressionProjectionStatistics.calculate(left(3), after, 900);
        Assertions.assertEquals(Map.of("103", 600L, "104", 300L), statistic.getHistogram().getMCV());
        Assertions.assertEquals(2, statistic.getDistinctValuesCount(), 1e-9);
        // Without an OR the statistics after the filter are used as they are.
        ScanExpressionStatistics plain = base.afterFilter(Map.of(X, firstBranch), Set.of(X), or, false);
        Assertions.assertEquals(Map.of("103", 600L),
                ExpressionProjectionStatistics.calculate(left(3), plain, 900).getHistogram().getMCV());
    }

    private static ScalarOperator like(ScalarOperator left, String pattern) {
        return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, left, ConstantOperator.createVarchar(pattern));
    }

    // x = left(c, 8) with a head and buckets of [lower, upper, cumulative rows, upper repeats, NDV].
    private static ScanExpressionStatistics bucketed(Map<String, Long> head, List<List<String>> buckets, long rows,
                                                     long ndv) {
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        head.forEach((value, count) -> entries.add(new MultiColumnCombinedStats.McvEntry(List.of(value), count,
                List.of(count))));
        entries.sort((left, right) -> Long.compare(right.getCount(), left.getCount()));
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("x"), rows, ndv, entries, buckets,
                List.of(0L));
        group.prepare(VarcharType.VARCHAR);
        ColumnStatistic statistic = group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(left(8));
        return new ScanExpressionStatistics("t", List.of(new ScanExpressionStatistics.Entry(canonical,
                ExpressionStatisticsCanonicalizer.keyOf(canonical), X, statistic)),
                Map.of(Set.of(X), new MultiColumnCombinedStats(group.getNdv(), rows, List.of(X), group.getMcv(),
                        group.getNullCounts())));
    }

    private static ScanExpressionStatistics afterScan(ScalarOperator predicate, ScanExpressionStatistics expressions,
                                                      double rows) {
        Statistics scan = Statistics.builder().setOutputRowCount(rows)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8,
                        1000)).build();
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, scan, true, expressions)
                .getScanExpressionStatistics();
    }

    @Test
    public void testPrefixesOfBucketsAreCountedOnce() {
        // Each bucket has one prefix: 103 and 104 join the head entries, 105 is the third value.
        ScanExpressionStatistics expressions = bucketed(Map.of("10312345", 100L, "10412345", 100L),
                List.of(List.of("10300000", "10399999", "100", "1", "50"),
                        List.of("10400000", "10499999", "200", "1", "50"),
                        List.of("10500000", "10599999", "300", "1", "50")), 500, 152);
        ColumnStatistic statistic = ExpressionProjectionStatistics.calculate(left(3), expressions, 500);
        Assertions.assertEquals(3, statistic.getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(Map.of("103", 200L, "104", 200L, "105", 100L), statistic.getHistogram().getMCV());
    }

    @Test
    public void testPrefixesAfterPrefixFilters() {
        Map<String, Long> head = Map.of("10312345", 100L, "10412345", 100L);
        // c LIKE '103%' leaves the head value and the part of [10000000, 10399999] from 103: one prefix.
        ScanExpressionStatistics like = bucketed(head, List.of(List.of("10000000", "10399999", "200", "1", "50"),
                List.of("10400000", "10999999", "400", "1", "50")), 600, 102);
        ScanExpressionStatistics afterLike = afterScan(like(C, "103%"), like, 600);
        Assertions.assertEquals(1, ExpressionProjectionStatistics.calculate(left(3), afterLike, 600)
                .getDistinctValuesCount(), 1e-9);
        // left(c, 3) IN ('101', ..., '110') over one bucket of 101 to 109 and one of 110: each prefix keeps a share
        // of its bucket, ten values in all.
        ScanExpressionStatistics wide = bucketed(head, List.of(List.of("10000000", "10999999", "800", "1", "500"),
                List.of("11000000", "11999999", "1600", "1", "500")), 1800, 1002);
        List<ScalarOperator> prefixes = new ArrayList<>();
        prefixes.add(left(3));
        for (int prefix = 101; prefix <= 110; prefix++) {
            prefixes.add(ConstantOperator.createVarchar(Integer.toString(prefix)));
        }
        ScanExpressionStatistics afterWide = afterScan(new InPredicateOperator(false,
                prefixes.toArray(new ScalarOperator[0])), wide, 1800);
        Assertions.assertEquals(10, ExpressionProjectionStatistics.calculate(left(3), afterWide, 1800)
                .getDistinctValuesCount(), 1e-9);
        // The same over buckets of one prefix each, from 100 to 111, 100 rows in each: the
        // buckets of the ten prefixes stay, 103 and 104 join the head values, ten values in all.
        List<List<String>> buckets = new ArrayList<>();
        for (int prefix = 100; prefix <= 111; prefix++) {
            buckets.add(List.of(prefix + "00000", prefix + "99999", Integer.toString((prefix - 99) * 100), "1", "50"));
        }
        ScanExpressionStatistics in = bucketed(head, buckets, 1400, 602);
        List<ScalarOperator> children = new ArrayList<>();
        children.add(left(3));
        for (int prefix = 101; prefix <= 110; prefix++) {
            children.add(ConstantOperator.createVarchar(Integer.toString(prefix)));
        }
        ScanExpressionStatistics afterIn = afterScan(new InPredicateOperator(false,
                children.toArray(new ScalarOperator[0])), in, 1400);
        Assertions.assertEquals(10, ExpressionProjectionStatistics.calculate(left(3), afterIn, 1400)
                .getDistinctValuesCount(), 1e-9);
    }

    // x = left(c, 8) known only from MCV(left(c, 8), d): 300 rows of 10312345 and 200 of 20412345 in the head, 500
    // rows of unknown values, and 50 values of x.
    private static ScanExpressionStatistics mixed() {
        ColumnRefOperator d = new ColumnRefOperator(2, IntegerType.INT, "d", true);
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("x", "d"), 1000, 50,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("10312345", "5"), 100, List.of(300L, 400L)),
                        new MultiColumnCombinedStats.McvEntry(List.of("20412345", "6"), 100, List.of(200L, 300L))),
                List.of(), List.of(0L, 0L));
        ColumnStatistic statistic = ScanExpressionStatistics.elementStatistic(group, 0, VarcharType.VARCHAR,
                ColumnStatistic.unknown(), Double.POSITIVE_INFINITY);
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(left(8));
        return new ScanExpressionStatistics("t", List.of(new ScanExpressionStatistics.Entry(canonical,
                ExpressionStatisticsCanonicalizer.keyOf(canonical), X, statistic)),
                Map.of(Set.of(X, d), new MultiColumnCombinedStats(50, 1000, List.of(X, d), group.getMcv(),
                        group.getNullCounts())));
    }

    @Test
    public void testValuesOfUnknownBoundsTakeTheNdvOfX() {
        Assertions.assertEquals(50, mixed().getEntries().get(0).getStatistic().getDistinctValuesCount(), 1e-9);
        // left(c, 3): 103 and 204 in the head, and the other 48 values of x, within 5^3 strings of 1, 0, 3, 2, 4.
        Assertions.assertEquals(50, ExpressionProjectionStatistics.calculate(left(3), mixed(), 1000)
                .getDistinctValuesCount(), 1e-9);
        // substr(c, 4, 2): 12 in the head, and at most 2^2 strings of 1 and 2 outside it.
        Assertions.assertEquals(5, ExpressionProjectionStatistics.calculate(
                call("substr", C, ConstantOperator.createInt(4), ConstantOperator.createInt(2)), mixed(), 1000)
                .getDistinctValuesCount(), 1e-9);
    }

    @Test
    public void testOrKeepsRowsOfUnknownValues() {
        ScalarOperator or = Utils.compoundOr(like(X, "103%"), like(X, "204%"));
        ScanExpressionStatistics base = mixed();
        ColumnStatistic firstBranch = ColumnStatistic.buildFrom(base.getEntries().get(0).getStatistic())
                .setHistogram(Histogram.forStrings(List.of(), Map.of("10312345", 300L))).setDistinctValuesCount(1)
                .build();
        ScanExpressionStatistics after = base.afterFilter(Map.of(X, firstBranch), Set.of(X), or, true);
        // The 500 rows of unknown values can hold either prefix, so they stay with their 48 values.
        ColumnStatistic statistic = ExpressionProjectionStatistics.calculate(left(8), after, 1000);
        Assertions.assertEquals(1000, statistic.getHistogram().getTotalRows());
        Assertions.assertEquals(50, statistic.getDistinctValuesCount(), 1e-9);
    }

    @Test
    public void testOrBranchesOfOtherFormsKeepBuckets() {
        ScanExpressionStatistics base = bucketed(Map.of("10312345", 300L),
                List.of(List.of("10000000", "19999999", "500", "1", "100")), 800, 101);
        // substr(x, 2, 2) IN ('03') can hold anywhere in [10000000, 19999999]; the bucket is not dropped.
        ScalarOperator or = Utils.compoundOr(new InPredicateOperator(false,
                call("substr", X, ConstantOperator.createInt(2), ConstantOperator.createInt(2)),
                ConstantOperator.createVarchar("03")), like(X, "9%"));
        ScanExpressionStatistics after = base.afterFilter(Map.of(X, base.getEntries().get(0).getStatistic()),
                Set.of(X), or, true);
        Assertions.assertEquals(800, StringHistogramEstimator.rows(
                ExpressionProjectionStatistics.calculate(left(8), after, 800).getHistogram()));
        // A constant FALSE branch holds on nothing: the head value with 103 alone stays.
        ScanExpressionStatistics withFalse = expressions().afterFilter(
                Map.of(X, expressions().getEntries().get(0).getStatistic()), Set.of(X),
                Utils.compoundOr(ConstantOperator.createBoolean(false), like(X, "103%")), true);
        ColumnStatistic statistic = ExpressionProjectionStatistics.calculate(left(3), withFalse, 600);
        Assertions.assertEquals(Map.of("103", 600L), statistic.getHistogram().getMCV());
        Assertions.assertEquals(1, statistic.getDistinctValuesCount(), 1e-9);
    }

    @Test
    public void testExcludedUpperBoundIsNotAPrefix() {
        // x < '10400000' cuts [10300000, 10499999] before 10400000: below it no string starts with 104, so the
        // piece holds only 103, which the head value has as well.
        ScanExpressionStatistics expressions = bucketed(Map.of("10312345", 100L),
                List.of(List.of("10300000", "10499999", "400", "1", "100")), 500, 101);
        ScanExpressionStatistics after = afterScan(new com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator(
                com.starrocks.sql.ast.expression.BinaryType.LT, left(8), ConstantOperator.createVarchar("10400000")),
                expressions, 500);
        Assertions.assertEquals(1, ExpressionProjectionStatistics.calculate(left(3), after, 500)
                .getDistinctValuesCount(), 1e-9);
    }

    @Test
    public void testNegationOfUnknownConditionKeepsTheValue() {
        // A function without an evaluation in the FE is unknown on every value, and so is its negation: the head
        // values stay.
        ScalarOperator unknown = new CallOperator("no_such_function", VarcharType.VARCHAR, List.of(X), null);
        ScalarOperator or = Utils.compoundOr(new com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator(
                com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType.NOT,
                new com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator(
                        com.starrocks.sql.ast.expression.BinaryType.EQ, unknown, ConstantOperator.createVarchar("a"))),
                like(X, "9%"));
        ScanExpressionStatistics after = expressions().afterFilter(
                Map.of(X, expressions().getEntries().get(0).getStatistic()), Set.of(X), or, true);
        Assertions.assertEquals(Map.of("103", 600L, "104", 300L),
                ExpressionProjectionStatistics.calculate(left(3), after, 900).getHistogram().getMCV());
    }
}
