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
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
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
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

/**
 * An OR of hundreds of parts, over the virtual column x = left(c, 8) of a head of 600 values, over c and over columns
 * with a head of their own. The estimate of an OR filters the MCV distributions by the OR of its first i parts for
 * every i, and each filter evaluates the OR on the head, so the work must not grow with the square of the parts.
 *
 * The table has 600,000 rows, c is a string of 500,000 values, x has the head of 600 values of 1000 rows each:
 * 00000000, 00000001, 00001000 and so on, two values for each prefix of 5 characters. e has a head of its own, and d
 * has ten values.
 */
public class ExpressionStatisticsLargeOrTest {
    private static final ColumnRefOperator C = new ColumnRefOperator(1, VarcharType.VARCHAR, "c", true);
    private static final ColumnRefOperator D = new ColumnRefOperator(2, IntegerType.INT, "d", true);
    private static final ColumnRefOperator E = new ColumnRefOperator(3, VarcharType.VARCHAR, "e", true);
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

    private static ScalarOperator eq(ScalarOperator left, ScalarOperator right) {
        return new BinaryPredicateOperator(BinaryType.EQ, left, right);
    }

    private static ScalarOperator not(ScalarOperator operator) {
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, operator);
    }

    private static ScalarOperator in(ScalarOperator left, String... values) {
        List<ScalarOperator> children = new ArrayList<>();
        children.add(left);
        for (String value : values) {
            children.add(text(value));
        }
        return new InPredicateOperator(false, children.toArray(new ScalarOperator[0]));
    }

    // The value of the head: two for each prefix of five characters.
    private static String headValue(int i) {
        return String.format("%05d%03d", i / 2, i % 2);
    }

    private static ScanExpressionStatistics expressions() {
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        for (int i = 0; i < HEAD; i++) {
            entries.add(new MultiColumnCombinedStats.McvEntry(List.of(headValue(i)), 1000, List.of(1000L)));
        }
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("x"), (long) ROWS, HEAD, entries,
                List.of(), List.of(0L));
        group.prepare(VarcharType.VARCHAR);
        ColumnStatistic statistic = group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(left(8));
        return new ScanExpressionStatistics("t", List.of(new ScanExpressionStatistics.Entry(canonical,
                ExpressionStatisticsCanonicalizer.keyOf(canonical), X, statistic)),
                Map.of(Set.of(X), new MultiColumnCombinedStats(HEAD, (long) ROWS, List.of(X), group.getMcv(),
                        group.getNullCounts())));
    }

    private static Statistics scan() {
        List<MultiColumnCombinedStats.McvEntry> head = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("v"), 300_000, List.of(300_000L)),
                new MultiColumnCombinedStats.McvEntry(List.of("w"), 100_000, List.of(100_000L)));
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 20,
                        500_000))
                .addColumnStatistic(D, new ColumnStatistic(0, 9, 0, 4, 10))
                .addColumnStatistic(E, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 50))
                .addMultiColumnStatistics(Set.of(E), new MultiColumnCombinedStats(50, (long) ROWS, List.of(E), head,
                        List.of(0L))).build();
    }

    private static Statistics estimate(ScalarOperator predicate) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, scan(), true, expressions());
    }

    // Parts over x: left(c, 5) = p and left(c, 8) = x, and the same prefix as substr(c, 1, 5).
    private static List<ScalarOperator> partsOverX(int prefixes) {
        List<ScalarOperator> parts = new ArrayList<>();
        for (int p = 0; p < prefixes; p++) {
            parts.add(eq(left(5), text(String.format("%05d", p))));
            parts.add(eq(left(8), text(headValue(p))));
            parts.add(eq(call("substr", C, ConstantOperator.createInt(1), ConstantOperator.createInt(5)),
                    text(String.format("%05d", p))));
        }
        return parts;
    }

    // Parts over e: a value that its head does not hold.
    private static List<ScalarOperator> partsOverE(int count) {
        List<ScalarOperator> parts = new ArrayList<>();
        for (int k = 0; k < count; k++) {
            parts.add(eq(E, text("u" + k)));
        }
        return parts;
    }

    // Every part is a prefix of five characters or a value of x, 200 prefixes with their 400 values.
    private static List<ScalarOperator> partsOnlyOverX(int count) {
        List<ScalarOperator> parts = new ArrayList<>();
        for (int p = 0; p < 200; p++) {
            parts.add(eq(left(5), text(String.format("%05d", p))));
        }
        for (int i = 0; i < 400; i++) {
            parts.add(eq(left(8), text(headValue(i))));
        }
        return parts.subList(0, count);
    }

    private static List<ScalarOperator> mixed(int count) {
        List<ScalarOperator> parts = new ArrayList<>(partsOverX(100).subList(0, count));
        parts.addAll(partsOverE(count));
        return parts;
    }

    private static double rows(List<ScalarOperator> parts) {
        return estimate(Utils.compoundOr(parts)).getOutputRowCount();
    }

    // The time of an estimate in milliseconds, after one run that warms the code.
    private static long millis(List<ScalarOperator> parts) {
        ScalarOperator or = Utils.compoundOr(parts);
        estimate(or);
        long start = System.nanoTime();
        estimate(or);
        return (System.nanoTime() - start) / 1_000_000;
    }

    @Test
    public void testOrOfOnlyPartsOverXHoldsTheRowsOfTheirUnion() {
        // The head holds every row, so the rows of the union are the rows of its values: 2 values of 1000 rows for each
        // of the prefixes, and the values of x are in the prefixes.
        Assertions.assertEquals(40_000, rows(partsOnlyOverX(20)), 1e-6);
        Assertions.assertEquals(100_000, rows(partsOnlyOverX(50)), 1e-6);
        Assertions.assertEquals(200_000, rows(partsOnlyOverX(100)), 1e-6);
        Assertions.assertEquals(400_000, rows(partsOnlyOverX(200)), 1e-6);
        Assertions.assertEquals(400_000, rows(partsOnlyOverX(400)), 1e-6);
        Assertions.assertEquals(400_000, rows(partsOnlyOverX(600)), 1e-6);
    }

    @Test
    public void testMixedOrKeepsItsRows() {
        // The values are those of the estimate of the OR part by part: the parts over x go first, and each part over e
        // takes the rows of its value less the rows among the earlier parts.
        Assertions.assertEquals(44_732.034250, rows(mixed(10)), 1e-6);
        Assertions.assertEquals(145_008.883115, rows(mixed(50)), 1e-6);
        Assertions.assertEquals(207_056.883249, rows(mixed(200)), 1e-6);
        Assertions.assertEquals(208_320.017069, rows(mixed(300)), 1e-6);
    }

    @Test
    public void testOrOfSixHundredPartsTakesTimeInProportionToTheParts() {
        // 600 parts over x, and 300 over x with 300 over e. The planner stops at 3 seconds, and we fear that the work
        // of the filters of the OR grows with the square of the parts, so we compare 600 parts with 150.
        long mixed = millis(mixed(300));
        long onlyX = millis(partsOnlyOverX(600));
        long quarter = millis(partsOnlyOverX(150));
        System.out.println("OR of 300 + 300 parts " + mixed + " ms, 600 parts over x " + onlyX + " ms, 150 parts over x "
                + quarter + " ms");
        Assertions.assertTrue(mixed < 500, mixed + " ms");
        Assertions.assertTrue(onlyX < 5000, onlyX + " ms");
        // Four times the parts take about five times the time, and sixteen times for the square.
        Assertions.assertTrue(onlyX < 10L * Math.max(quarter, 20), onlyX + " ms for 600 parts, " + quarter + " ms for 150");
    }

    // The columns, the groups and the row count of statistics as text, to tell two of them apart.
    private static String snapshot(Statistics statistics) {
        StringBuilder out = new StringBuilder();
        out.append(statistics.getOutputRowCount()).append('\n');
        Map<Integer, String> columns = new TreeMap<>();
        statistics.getColumnStatistics().forEach((column, statistic) -> columns.put(column.getId(),
                statistic.getMinValue() + " " + statistic.getMaxValue() + " " + statistic.getNullsFraction() + " "
                        + statistic.getDistinctValuesCount() + " " + statistic.isUnknown() + " "
                        + (statistic.getHistogram() == null ? "-" : new TreeMap<>(statistic.getHistogram().getMCV())
                        + " " + statistic.getHistogram().getBuckets().size())));
        columns.forEach((id, text) -> out.append(id).append(": ").append(text).append('\n'));
        Map<String, String> groups = new TreeMap<>();
        statistics.getMultiColumnCombinedStats().forEach((key, group) -> {
            StringBuilder text = new StringBuilder();
            text.append(group.getNdv()).append(' ').append(group.getRowCount()).append(' ')
                    .append(group.getColumns()).append(' ').append(group.getNullCounts());
            for (MultiColumnCombinedStats.McvEntry entry : group.getMcv()) {
                text.append(" [").append(entry.getValues()).append(entry.getCount())
                        .append(entry.getComponentCounts()).append(']');
            }
            groups.put(key.stream().map(ColumnRefOperator::getId).sorted().toList().toString(), text.toString());
        });
        groups.forEach((key, text) -> out.append(key).append(": ").append(text).append('\n'));
        return out.toString();
    }

    private static MultiColumnCombinedStats.McvEntry entry(long count, List<Long> components, String... values) {
        return new MultiColumnCombinedStats.McvEntry(List.of(values), count, components);
    }

    private static MultiColumnCombinedStats group(List<ColumnRefOperator> columns, long rows, long ndv, List<Long> nulls,
                                                  MultiColumnCombinedStats.McvEntry... head) {
        return new MultiColumnCombinedStats(ndv, rows, columns, List.of(head), nulls);
    }

    // The inputs of the filters: a head that holds every row, a head with a tail and NULL rows, and a head of x and e.
    private static List<Statistics> inputs() {
        ColumnStatistic any = new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 20);
        MultiColumnCombinedStats e = group(List.of(E), 1000, 3, List.of(0L), entry(500, List.of(500L), "a"),
                entry(300, List.of(300L), "b"), entry(200, List.of(200L), "c"));
        List<Statistics> inputs = new ArrayList<>();
        inputs.add(Statistics.builder().setOutputRowCount(1000).addColumnStatistic(X, any).addColumnStatistic(E, any)
                .addMultiColumnStatistics(Set.of(X), group(List.of(X), 1000, 6, List.of(0L),
                        entry(300, List.of(300L), "10100"), entry(250, List.of(250L), "10200"),
                        entry(200, List.of(200L), "17531"), entry(150, List.of(150L), "20202"),
                        entry(60, List.of(60L), "20206"), entry(40, List.of(40L), "99999")))
                .addMultiColumnStatistics(Set.of(E), e).build());
        inputs.add(Statistics.builder().setOutputRowCount(1000).addColumnStatistic(X, any).addColumnStatistic(E, any)
                .addMultiColumnStatistics(Set.of(X), group(List.of(X), 1000, 40, List.of(100L),
                        entry(300, List.of(300L), "10100"), entry(150, List.of(150L), "10200"),
                        entry(100, List.of(100L), "17531"), entry(50, List.of(50L), "20202")))
                .addMultiColumnStatistics(Set.of(E), e).build());
        inputs.add(Statistics.builder().setOutputRowCount(1000).addColumnStatistic(X, any).addColumnStatistic(E, any)
                .addMultiColumnStatistics(Set.of(X, E), group(List.of(X, E), 1000, 8, List.of(0L, 0L),
                        entry(300, List.of(400L, 500L), "10100", "a"), entry(100, List.of(400L, 300L), "10100", "b"),
                        entry(200, List.of(300L, 500L), "10200", "a"), entry(100, List.of(300L, 200L), "10200", "c"),
                        entry(150, List.of(150L, 300L), "17531", "b"), entry(100, List.of(100L, 200L), "20202", "c"),
                        entry(50, List.of(50L, 500L), "99999", "a")))
                .addMultiColumnStatistics(Set.of(X), group(List.of(X), 1000, 5, List.of(0L),
                        entry(400, List.of(400L), "10100"), entry(300, List.of(300L), "10200"),
                        entry(150, List.of(150L), "17531"), entry(100, List.of(100L), "20202"),
                        entry(50, List.of(50L), "99999"))).build());
        return inputs;
    }

    private static ScalarOperator part(Random random) {
        String[] x = {"10100", "10200", "17531", "20202", "20206", "99999", "1", "2", "zz"};
        String[] e = {"a", "b", "c", "d"};
        switch (random.nextInt(9)) {
            case 0:
                return eq(X, text(x[random.nextInt(x.length)]));
            case 1:
                return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, X,
                        text(x[random.nextInt(x.length)] + "%"));
            case 2:
                return in(X, x[random.nextInt(x.length)], x[random.nextInt(x.length)]);
            case 3:
                return not(eq(X, text(x[random.nextInt(x.length)])));
            case 4:
                return new IsNullPredicateOperator(random.nextBoolean(), X);
            case 5:
                return eq(E, text(e[random.nextInt(e.length)]));
            case 6:
                return Utils.compoundAnd(eq(X, text(x[random.nextInt(x.length)])), eq(E, text(e[random.nextInt(e.length)])));
            case 7:
                return new BinaryPredicateOperator(BinaryType.GT, X, text(x[random.nextInt(x.length)]));
            default:
                return in(E, e[random.nextInt(e.length)], e[random.nextInt(e.length)]);
        }
    }

    @Test
    public void testFilterByTheGrowingOrIsTheFilterByTheOrOfThePartsSoFar() {
        Random random = new Random(7);
        int checked = 0;
        for (Statistics input : inputs()) {
            for (int t = 0; t < 300; t++) {
                List<ScalarOperator> parts = new ArrayList<>();
                int size = 2 + random.nextInt(9);
                for (int i = 0; i < size; i++) {
                    parts.add(part(random));
                }
                // The result of the estimate so far, which the filter of a step gets as its output.
                Statistics output = Statistics.buildFrom(input).setOutputRowCount(input.getOutputRowCount() * 0.37).build();
                McvStatisticsPropagation.DisjunctionFilter incremental = new McvStatisticsPropagation.DisjunctionFilter(input);
                incremental.add(parts.get(0));
                for (int i = 1; i < size; i++) {
                    incremental.add(parts.get(i));
                    Statistics whole = McvStatisticsPropagation.filter(Utils.compoundOr(parts.subList(0, i + 1)), input,
                            output);
                    Assertions.assertEquals(snapshot(whole), snapshot(incremental.filter(output)),
                            parts.subList(0, i + 1).toString());
                    checked++;
                }
            }
        }
        Assertions.assertTrue(checked > 3000, String.valueOf(checked));
    }
}
