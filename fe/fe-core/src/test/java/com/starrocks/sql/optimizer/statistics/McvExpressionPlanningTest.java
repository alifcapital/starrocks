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
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
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
import java.util.function.IntFunction;

/**
 * An OR of n parts over x = left(c, 8), whose head holds 600 values with a prefix of 5 characters each and is complete,
 * in the forms of the probes of the stand: the parts are conditions on prefixes that the head holds, written as the
 * query has them and as the planner writes them after its rewrites, which take a NOT in and a common conjunct out of the
 * OR. The first 64 atoms of an OR over x are written over x, see ExpressionStatisticsMatcher, and the estimate of the OR
 * passes over the head for each of them: the time of the estimate is that of the 64 passes and does not grow with n, and
 * a pass derives the head and the values of the tuples it keeps when they are read, so it is not the time of building
 * them. The rows are the ones that the estimate gave when each pass built them.
 */
public class McvExpressionPlanningTest {
    private static final ColumnRefOperator C = new ColumnRefOperator(1, VarcharType.VARCHAR, "c", true);
    private static final ColumnRefOperator D = new ColumnRefOperator(2, IntegerType.INT, "d", true);
    private static final ColumnRefOperator X = new ColumnRefOperator(-1, VarcharType.VARCHAR, "expr#1", true);
    private static final double ROWS = 600_000;
    private static final int HEAD = 600;

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

    private static ConstantOperator text(String value) {
        return ConstantOperator.createVarchar(value);
    }

    private static ScalarOperator compare(BinaryType type, ScalarOperator left, ScalarOperator right) {
        return new BinaryPredicateOperator(type, left, right);
    }

    private static ScalarOperator not(ScalarOperator operator) {
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, operator);
    }

    private static String headValue(int i) {
        return String.format("%05d%03d", i, 0);
    }

    private static ScanExpressionStatistics expressions(boolean withPairs) {
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        List<MultiColumnCombinedStats.McvEntry> pairs = new ArrayList<>();
        for (int i = 0; i < HEAD; i++) {
            entries.add(new MultiColumnCombinedStats.McvEntry(List.of(headValue(i)), 1000, List.of(1000L)));
            pairs.add(new MultiColumnCombinedStats.McvEntry(List.of(headValue(i), String.valueOf(i % 10)), 1000,
                    List.of(1000L, 60_000L)));
        }
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("x"), (long) ROWS, HEAD, entries,
                List.of(), List.of(0L));
        group.prepare(VarcharType.VARCHAR);
        ColumnStatistic statistic = group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(left(8));
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups = new java.util.HashMap<>();
        groups.put(Set.of(X), new MultiColumnCombinedStats(HEAD, (long) ROWS, List.of(X), group.getMcv(),
                group.getNullCounts()));
        if (withPairs) {
            groups.put(Set.of(X, D), new MultiColumnCombinedStats(HEAD, (long) ROWS, List.of(X, D), pairs,
                    List.of(0L, 0L)));
        }
        return new ScanExpressionStatistics("t", List.of(new ScanExpressionStatistics.Entry(canonical,
                ExpressionStatisticsCanonicalizer.keyOf(canonical), X, statistic)), groups);
    }

    private static Statistics scan() {
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 20,
                        500_000))
                .addColumnStatistic(D, new ColumnStatistic(0, 9, 0, 4, 10)).build();
    }

    private static ScalarOperator orOf(int n, IntFunction<ScalarOperator> part) {
        List<ScalarOperator> parts = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            parts.add(part.apply(i));
        }
        return Utils.compoundOr(parts);
    }

    private static String five(int i) {
        return String.format("%05d", i % HEAD);
    }

    // The rows of the OR of 200 and of 600 parts as the query has it and as the planner writes it.
    private record Form(String name, IntFunction<ScalarOperator> part, double[] rows) {
    }

    private static ScalarOperator col() {
        return compare(BinaryType.EQ, D, ConstantOperator.createInt(3));
    }

    private static ScalarOperator planner(ScalarOperator predicate) {
        return new ScalarOperatorRewriter().rewrite(predicate, ScalarOperatorRewriter.DEFAULT_REWRITE_SCAN_PREDICATE_RULES);
    }

    private static double estimate(ScalarOperator predicate, boolean withPairs) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(), true, expressions(withPairs))
                .getOutputRowCount();
    }

    // The least of three estimates after the first: other tests may run on the machine at the same time.
    private static long millisOf(IntFunction<ScalarOperator> part, int n, boolean rewritten, boolean withPairs) {
        long millis = Long.MAX_VALUE;
        for (int rep = 0; rep < 4; rep++) {
            ScalarOperator predicate = rewritten ? planner(orOf(n, part)) : orOf(n, part);
            long start = System.nanoTime();
            estimate(predicate, withPairs);
            if (rep > 0) {
                millis = Math.min(millis, (System.nanoTime() - start) / 1_000_000);
            }
        }
        return millis;
    }

    @Test
    public void testRowsAndPlanningTimeOfAnOrOfPartsOverTheHead() {
        // rows: the query with 200 parts, with 600, the planner's with 200, with 600.
        List<Form> forms = new ArrayList<>();
        forms.add(new Form("x<>p", i -> compare(BinaryType.NE, left(5), text(five(i))),
                new double[] {600000, 600000, 600000, 600000}));
        forms.add(new Form("NOT(x=p)", i -> not(compare(BinaryType.EQ, left(5), text(five(i)))),
                new double[] {600000, 600000, 600000, 600000}));
        forms.add(new Form("NOT(x OR col)", i -> not(Utils.compoundOr(compare(BinaryType.EQ, left(5), text(five(i))),
                col())), new double[] {540000, 540000, 540000, 540000}));
        forms.add(new Form("x>=p", i -> compare(BinaryType.GE, left(5), text(five(i))),
                new double[] {600000, 600000, 600000, 600000}));
        forms.add(new Form("NOT(c LIKE OR col)", i -> not(Utils.compoundOr(new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE, C, text(five(i) + "%")), col())),
                new double[] {540000, 540000, 540000, 540000}));
        forms.add(new Form("x=p AND col", i -> Utils.compoundAnd(compare(BinaryType.EQ, left(5), text(five(i))), col()),
                new double[] {229332.36, 516736.05, 20000, 60000}));
        forms.add(new Form("x=p", i -> compare(BinaryType.EQ, left(5), text(five(i))),
                new double[] {200000, 600000, 200000, 600000}));
        StringBuilder measured = new StringBuilder();
        for (Form form : forms) {
            for (boolean rewritten : new boolean[] {false, true}) {
                for (int n : new int[] {200, 600}) {
                    double rows = estimate(rewritten ? planner(orOf(n, form.part())) : orOf(n, form.part()), false);
                    String label = form.name() + (rewritten ? " as the planner writes it" : "") + ", " + n + " parts";
                    Assertions.assertEquals(form.rows()[(rewritten ? 2 : 0) + (n == 200 ? 0 : 1)], rows, 0.01, label);
                    long millis = millisOf(form.part(), n, rewritten, false);
                    measured.append(String.format(" %s%s n=%d %d ms;", form.name(), rewritten ? " (planner)" : "", n,
                            millis));
                    Assertions.assertTrue(millis < 1000, label + ": " + millis + " ms");
                }
            }
        }
        System.out.println("MEASURED planning of an OR of parts over x:" + measured);
    }

    @Test
    public void testRowsAndPlanningTimeOfAnOrOfPartsNextToAGroupOfTwoColumns() {
        // The group of x and d is filtered by the code that builds the distributions of a group of several columns at
        // once, which takes about 0.6 ms for each of the first 64 parts.
        List<Form> forms = new ArrayList<>();
        forms.add(new Form("x<>p", i -> compare(BinaryType.NE, left(5), text(five(i))), new double[] {600000, 600000}));
        forms.add(new Form("NOT(x OR col)", i -> not(Utils.compoundOr(compare(BinaryType.EQ, left(5), text(five(i))),
                col())), new double[] {540000, 540000}));
        forms.add(new Form("x=p AND col", i -> Utils.compoundAnd(compare(BinaryType.EQ, left(5), text(five(i))), col()),
                new double[] {50882.76, 59999.99}));
        for (Form form : forms) {
            for (int n : new int[] {200, 600}) {
                double rows = estimate(orOf(n, form.part()), true);
                Assertions.assertEquals(form.rows()[n == 200 ? 0 : 1], rows, 0.01, form.name() + ", " + n + " parts");
                long millis = millisOf(form.part(), n, false, true);
                Assertions.assertTrue(millis < 1000, form.name() + ", " + n + " parts: " + millis + " ms");
            }
        }
    }
}
