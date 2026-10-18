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
 * every i, and each filter evaluates the OR on the head, so the work must not grow with the square of the parts. Each
 * atom over x filters the head of x, so the atoms written over x in the ORs of a predicate are counted, and an OR that
 * would pass the budget of the matcher is estimated without x, in milliseconds.
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

    private static final ColumnRefOperator X5 = new ColumnRefOperator(-2, VarcharType.VARCHAR, "expr#2", true);

    // The statistics on x = left(c, 8) and on x5 = left(c, 5), whose head holds the 300 prefixes of 2000 rows each; the
    // entry of x5 comes last, after the one that loses the choice.
    private static ScanExpressionStatistics twoExpressions() {
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        for (int p = 0; p < HEAD / 2; p++) {
            entries.add(new MultiColumnCombinedStats.McvEntry(List.of(String.format("%05d", p)), 2000, List.of(2000L)));
        }
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("x"), (long) ROWS, HEAD / 2, entries,
                List.of(), List.of(0L));
        group.prepare(VarcharType.VARCHAR);
        ColumnStatistic statistic = group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(left(5));
        ScanExpressionStatistics wide = expressions();
        List<ScanExpressionStatistics.Entry> both = new ArrayList<>(wide.getEntries());
        both.add(new ScanExpressionStatistics.Entry(canonical, ExpressionStatisticsCanonicalizer.keyOf(canonical), X5,
                statistic));
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups = new java.util.LinkedHashMap<>(wide.getGroups());
        groups.put(Set.of(X5), new MultiColumnCombinedStats(HEAD / 2, (long) ROWS, List.of(X5), group.getMcv(),
                group.getNullCounts()));
        return new ScanExpressionStatistics("t", both, groups);
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

    private static final int LIMIT = ExpressionStatisticsMatcher.MAX_ATOMS_OVER_X;

    private static double plainRows(ScalarOperator or) {
        return PredicateStatisticsCalculator.statisticsCalculate(or, scan(), true).getOutputRowCount();
    }

    // left(c, 5) IN (the prefixes 0 to count - 1): the prefixes of the parts of an OR of equalities, in one set.
    private static ScalarOperator prefixesIn(int count) {
        String[] prefixes = new String[count];
        for (int p = 0; p < count; p++) {
            prefixes[p] = String.format("%05d", p);
        }
        return in(left(5), prefixes);
    }

    @Test
    public void testOrOfOnlyPartsOverXHoldsTheRowsOfTheirUnion() {
        // The head holds every row, so the rows of the union are the rows of its values: 2 values of 1000 rows for each
        // of the prefixes, and the values of x are in the prefixes. The atoms of one axis are one set, so the count of
        // the parts does not matter: 65 parts and 600 parts are as exact as 20.
        for (int count : new int[] {20, 50, LIMIT, LIMIT + 1, 100, 200, 400, 600}) {
            Assertions.assertEquals(2000.0 * Math.min(count, 200), rows(partsOnlyOverX(count)), 1e-6,
                    String.valueOf(count));
        }
    }

    @Test
    public void testMixedOrKeepsItsRows() {
        // The values are those of the estimate of the OR part by part: the parts over x go first as the sets of their
        // axes, and each part over e takes the rows of its value less the rows among the earlier parts. The parts over x
        // are equalities of values of x and of prefixes of 5 characters.
        Assertions.assertEquals(44_733.494675, rows(mixed(10)), 1e-6);
        Assertions.assertEquals(145_011.368977, rows(mixed(50)), 1e-6);
        // The same parts as the two sets they are: the prefixes of 5 characters and the values of x.
        for (int count : new int[] {10, 50, 200, 300}) {
            int prefixes = (count + 2) / 3;
            int values = (count + 1) / 3;
            List<ScalarOperator> sets = new ArrayList<>();
            sets.add(prefixesIn(prefixes));
            if (values > 0) {
                String[] head = new String[values];
                for (int p = 0; p < values; p++) {
                    head[p] = headValue(p);
                }
                sets.add(in(left(8), head));
            }
            sets.addAll(partsOverE(count));
            Assertions.assertEquals(rows(sets), rows(mixed(count)), 1e-6, String.valueOf(count));
        }
    }

    @Test
    public void testOrOfEqualitiesAndRegexpOfOneAxisIsTheInAndTakesMilliseconds() {
        for (int count : new int[] {LIMIT + 1, 800}) {
            ScalarOperator in = prefixesIn(count);
            double expected = estimate(in).getOutputRowCount();
            List<ScalarOperator> equalities = new ArrayList<>();
            List<String> regexp = new ArrayList<>();
            for (int p = 0; p < count; p++) {
                equalities.add(eq(left(5), text(String.format("%05d", p))));
                regexp.add(String.format("^%05d", p));
            }
            ScalarOperator or = Utils.compoundOr(equalities);
            ScalarOperator regexpOr = new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, C,
                    text(String.join("|", regexp)));
            for (ScalarOperator predicate : List.of(or, regexpOr)) {
                estimate(predicate);
                long start = System.nanoTime();
                double rows = estimate(predicate).getOutputRowCount();
                long millis = (System.nanoTime() - start) / 1_000_000;
                Assertions.assertEquals(expected, rows, 1e-6, count + " " + predicate.getClass().getSimpleName());
                Assertions.assertTrue(millis < 200, count + ": " + millis + " ms");
            }
        }
    }

    @Test
    public void testAndOfInequalitiesAndNegatedOrOfOneAxisIsTheNotInAndTakesMilliseconds() {
        for (int count : new int[] {LIMIT + 1, 400, 800}) {
            List<ScalarOperator> inequalities = new ArrayList<>();
            List<ScalarOperator> equalities = new ArrayList<>();
            String[] prefixes = new String[count];
            for (int p = 0; p < count; p++) {
                prefixes[p] = String.format("%05d", p);
                inequalities.add(new BinaryPredicateOperator(BinaryType.NE, left(5), text(prefixes[p])));
                equalities.add(eq(left(5), text(prefixes[p])));
            }
            List<ScalarOperator> children = new ArrayList<>();
            children.add(left(5));
            for (String prefix : prefixes) {
                children.add(text(prefix));
            }
            ScalarOperator expected = new InPredicateOperator(true, children.toArray(new ScalarOperator[0]));
            double rows = estimate(expected).getOutputRowCount();
            for (ScalarOperator predicate : List.of(Utils.compoundAnd(inequalities),
                    not(Utils.compoundOr(equalities)))) {
                estimate(predicate);
                long start = System.nanoTime();
                double estimated = estimate(predicate).getOutputRowCount();
                long millis = (System.nanoTime() - start) / 1_000_000;
                Assertions.assertEquals(rows, estimated, 1e-6, count + " " + predicate);
                Assertions.assertTrue(millis < 500, count + ": " + millis + " ms");
            }
            // The inequalities inside an OR, next to a condition on another column, are the one set as well.
            ScalarOperator other = eq(D, ConstantOperator.createInt(3));
            Assertions.assertEquals(estimate(Utils.compoundOr(expected, other)).getOutputRowCount(),
                    estimate(Utils.compoundOr(Utils.compoundAnd(inequalities), other)).getOutputRowCount(), 1e-6,
                    String.valueOf(count));
        }
    }

    @Test
    public void testOrWithinTheBudgetTakesTimeInProportionToTheParts() {
        // The planner stops at 3 seconds, and we fear that the work of the filters grows with the square of the atoms,
        // so we compare the atoms of the budget with a quarter of them. The atoms of the form x >= p are not a set.
        long limit = millis(ofForm("ge", LIMIT));
        long quarter = millis(ofForm("ge", LIMIT / 4));
        System.out.println("MEASURED: OR of " + LIMIT + " atoms x >= p " + limit + " ms, " + LIMIT / 4 + " atoms " + quarter
                + " ms; AND of " + LIMIT + " atoms " + millis(andOfForm("ge", LIMIT)) + " ms");
        Assertions.assertTrue(limit < 500, limit + " ms");
        // Four times the atoms take about five times the time, and sixteen times for the square.
        Assertions.assertTrue(limit < 10L * Math.max(quarter, 20), limit + " ms for " + LIMIT + " atoms, " + quarter
                + " ms for " + LIMIT / 4);
    }

    private static List<ScalarOperator> ofForm(String form, int count) {
        List<ScalarOperator> parts = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            parts.add(part(form, i));
        }
        return parts;
    }

    private static List<ScalarOperator> andOfForm(String form, int count) {
        return List.of(Utils.compoundAnd(ofForm(form, count)));
    }

    // The parts of an OR of one form, i over x unless the form says another column.
    private static ScalarOperator part(String form, int i) {
        String prefix = String.format("%05d", i);
        switch (form) {
            case "eq":
                return eq(left(5), text(prefix));
            case "ne":
                return new BinaryPredicateOperator(BinaryType.NE, left(5), text(prefix));
            case "ge":
                return new BinaryPredicateOperator(BinaryType.GE, left(5), text(prefix));
            case "lt":
                return new BinaryPredicateOperator(BinaryType.LT, left(5), text(prefix));
            case "not":
                return not(Utils.compoundOr(eq(left(5), text(prefix)), eq(D, ConstantOperator.createInt(3))));
            case "like":
                return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, left(5), text(prefix + "%"));
            case "regexp":
                return new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, C, text("^" + prefix));
            case "column":
                return new BinaryPredicateOperator(BinaryType.GE, C, text(prefix));
            case "in":
                return in(left(5), prefix, String.format("%05d", i + 1));
            default:
                throw new IllegalArgumentException(form);
        }
    }

    private static final List<String> FORMS = List.of("eq", "ne", "ge", "lt", "not", "like", "regexp", "column", "in",
            "mixed");

    // count parts over x of a form; the form mixed has as many parts over e next to the parts over x
    private static ScalarOperator or(String form, int count) {
        List<ScalarOperator> parts = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            parts.add(part(form.equals("mixed") ? "ne" : form, i));
            if (form.equals("mixed")) {
                parts.add(eq(E, text("u" + i)));
            }
        }
        return Utils.compoundOr(parts);
    }

    private static ExpressionStatisticsMatcher.Result match(ScalarOperator predicate) {
        return ExpressionStatisticsMatcher.match(EstimatePredicateNormalizer.normalize(predicate, scan()), expressions(),
                scan());
    }

    private static long readingX(ScalarOperator predicate) {
        return Utils.extractDisjunctive(predicate).stream().filter(part -> Utils.extractColumnRef(part).contains(X)).count();
    }

    @Test
    public void testAtomsOfDifferentKindsPastTheBudgetStayOneByOne() {
        // The first atoms of an OR are written over x while the budget lasts, and the atom past it stays as it is: the
        // OR is not left without x as a whole.
        ScalarOperator within = or("ge", LIMIT);
        ExpressionStatisticsMatcher.Result written = match(within);
        Assertions.assertNotNull(written);
        Assertions.assertEquals(LIMIT, readingX(written.getPredicate()));
        List<ScalarOperator> parts = ofForm("ge", LIMIT + 1);
        ScalarOperator past = Utils.compoundOr(parts);
        ExpressionStatisticsMatcher.Result result = match(past);
        Assertions.assertNotNull(result);
        List<ScalarOperator> after = Utils.extractDisjunctive(result.getPredicate());
        Assertions.assertEquals(LIMIT + 1, after.size());
        Assertions.assertEquals(LIMIT, readingX(result.getPredicate()));
        Assertions.assertTrue(after.stream().anyMatch(part -> part == parts.get(LIMIT)));
        // The estimate differs from the one of the atoms of the budget by the share of the atom that stays: the rows of
        // an OR are the rows of its parts so far and at most the rows of the atom more.
        double atoms = estimate(within).getOutputRowCount();
        double all = estimate(past).getOutputRowCount();
        double alone = plainRows(parts.get(LIMIT));
        Assertions.assertTrue(all >= atoms - 1e-6, atoms + " " + all);
        Assertions.assertTrue(all <= atoms + alone + 1e-6, atoms + " + " + alone + " " + all);
        // The same in an AND: the first atoms are written, the others are the conjuncts of the query.
        List<ScalarOperator> conjuncts = ofForm("ge", 100);
        ExpressionStatisticsMatcher.Result and = match(Utils.compoundAnd(conjuncts));
        Assertions.assertNotNull(and);
        List<ScalarOperator> written2 = Utils.extractConjuncts(and.getPredicate());
        Assertions.assertEquals(100, written2.size());
        for (int i = 0; i < 100; i++) {
            if (i < LIMIT) {
                Assertions.assertTrue(Utils.extractColumnRef(written2.get(i)).contains(X), String.valueOf(i));
            } else {
                Assertions.assertSame(conjuncts.get(i), written2.get(i), String.valueOf(i));
            }
        }
    }

    @Test
    public void testTheBudgetIsPaidByTheExpressionThatIsChosen() {
        // The atoms of both expressions of the column are candidates, and the one with the shorter prefix is chosen. The
        // atoms the other would write are not paid, so the chosen one writes all of them.
        ScalarOperator or = or("ge", LIMIT);
        ExpressionStatisticsMatcher.Result result = ExpressionStatisticsMatcher.match(
                EstimatePredicateNormalizer.normalize(or, scan()), twoExpressions(), scan());
        Assertions.assertNotNull(result);
        Assertions.assertEquals(Set.of(X5), result.getUsedColumns());
        Assertions.assertEquals(LIMIT, Utils.extractDisjunctive(result.getPredicate()).stream()
                .filter(part -> Utils.extractColumnRef(part).contains(X5)).count());
    }

    @Test
    public void testOrWithinTheBudgetKeepsItsRows() {
        Assertions.assertEquals(64, LIMIT);
        // 60 atoms over x: the rows of the estimate over the head, not of the ordinary one.
        Assertions.assertEquals(120_000, rows(List.of(or("eq", 60))), 1e-6);
        Assertions.assertEquals(600_000, rows(List.of(or("ne", 60))), 1e-6);
        Assertions.assertEquals(600_000, rows(List.of(or("ge", 60))), 1e-6);
        Assertions.assertEquals(118_000, rows(List.of(or("lt", 60))), 1e-6);
        Assertions.assertEquals(120_000, rows(List.of(or("like", 60))), 1e-6);
        Assertions.assertEquals(122_000, rows(List.of(or("in", 60))), 1e-6);
        Assertions.assertEquals(327_253.381858, rows(List.of(or("mixed", 60))), 1e-6);
    }

    @Test
    public void testTheBudgetIsForTheAtomsOfTheWholePredicate() {
        ScalarOperator d = eq(D, ConstantOperator.createInt(3));
        // The atoms are paid in the order of the conjuncts, wherever they stand: the 60 atoms of an OR, the atom under a
        // NOT, the 3 of the OR after it fill the budget of 64, and the atom after them stays as it is.
        ScalarOperator first = Utils.compoundOr(ofForm("ge", 60));
        ScalarOperator negated = not(part("ge", 1000));
        ScalarOperator third = Utils.compoundOr(List.of(part("lt", 2000), part("lt", 2001), part("lt", 2002)));
        ScalarOperator last = part("ge", 3000);
        ExpressionStatisticsMatcher.Result result = match(Utils.compoundAnd(List.of(first, negated, third, last, d)));
        Assertions.assertNotNull(result);
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(result.getPredicate());
        Assertions.assertEquals(5, conjuncts.size());
        for (int i = 0; i < 3; i++) {
            Assertions.assertTrue(Utils.extractColumnRef(conjuncts.get(i)).contains(X), String.valueOf(i));
        }
        Assertions.assertSame(last, conjuncts.get(3));
        // The conjuncts that are negations of one axis are one set at the place of the first, and cost one atom.
        List<ScalarOperator> inequalities = new ArrayList<>();
        for (int p = 0; p < 800; p++) {
            inequalities.add(part("ne", p));
        }
        List<ScalarOperator> others = ofForm("ge", 70);
        List<ScalarOperator> all = new ArrayList<>(inequalities);
        all.addAll(others);
        ExpressionStatisticsMatcher.Result sets = match(Utils.compoundAnd(all));
        Assertions.assertNotNull(sets);
        List<ScalarOperator> after = Utils.extractConjuncts(sets.getPredicate());
        Assertions.assertEquals(71, after.size());
        List<ScalarOperator> setChildren = new ArrayList<>();
        setChildren.add(PrefixDerivationRule.substr(X, 1, 5));
        for (int p = 0; p < 800; p++) {
            setChildren.add(text(String.format("%05d", p)));
        }
        Assertions.assertEquals(not(new InPredicateOperator(false, setChildren.toArray(new ScalarOperator[0]))),
                after.get(0));
        for (int i = 0; i < 70; i++) {
            if (i < LIMIT - 1) {
                Assertions.assertTrue(Utils.extractColumnRef(after.get(i + 1)).contains(X), String.valueOf(i));
            } else {
                Assertions.assertSame(others.get(i), after.get(i + 1), String.valueOf(i));
            }
        }
        // The negations of a set past the budget are estimated each as they are, and the budget they did not spend
        // is the next conjunct's.
        List<ScalarOperator> first64 = new ArrayList<>(ofForm("ge", LIMIT));
        List<ScalarOperator> withInequalities = new ArrayList<>(first64);
        withInequalities.add(part("ne", 1));
        withInequalities.add(part("ne", 2));
        ExpressionStatisticsMatcher.Result unpaid = match(Utils.compoundAnd(withInequalities));
        Assertions.assertNotNull(unpaid);
        List<ScalarOperator> kept = Utils.extractConjuncts(unpaid.getPredicate());
        Assertions.assertEquals(LIMIT + 2, kept.size());
        Assertions.assertSame(withInequalities.get(LIMIT), kept.get(LIMIT));
        Assertions.assertSame(withInequalities.get(LIMIT + 1), kept.get(LIMIT + 1));
        // The set of an OR that the budget cannot pay stays as its atoms.
        List<ScalarOperator> full = new ArrayList<>(ofForm("ge", LIMIT));
        ScalarOperator equalities = Utils.compoundOr(part("eq", 1), part("eq", 2));
        full.add(equalities);
        ExpressionStatisticsMatcher.Result unpaidSet = match(Utils.compoundAnd(full));
        Assertions.assertNotNull(unpaidSet);
        Assertions.assertSame(equalities, Utils.extractConjuncts(unpaidSet.getPredicate()).get(LIMIT));
    }

    @Test
    public void testOrOfHundredsOfAtomsOfAnyFormTakesMilliseconds() {
        for (String form : FORMS) {
            for (int count : new int[] {800, 1000}) {
                ScalarOperator or = or(form, count);
                estimate(or);
                long start = System.nanoTime();
                estimate(or);
                long millis = (System.nanoTime() - start) / 1_000_000;
                Assertions.assertTrue(millis < 500, form + " " + count + ": " + millis + " ms");
            }
            // The atoms of the budget are estimated over x in less than the 3 seconds of the planner, with room for a
            // slower machine and the rest of the plan.
            ScalarOperator limit = or(form, LIMIT);
            estimate(limit);
            long start = System.nanoTime();
            estimate(limit);
            long millis = (System.nanoTime() - start) / 1_000_000;
            Assertions.assertTrue(millis < 3000, form + " " + LIMIT + ": " + millis + " ms");
        }
        // The same atoms in an AND, 800 of them: the negations are one set, the others are written within the budget.
        for (String form : List.of("ne", "ge", "lt", "not", "like", "column", "in")) {
            ScalarOperator and = Utils.compoundAnd(ofForm(form, 800));
            estimate(and);
            long start = System.nanoTime();
            estimate(and);
            long millis = (System.nanoTime() - start) / 1_000_000;
            Assertions.assertTrue(millis < 500, "AND of 800 " + form + ": " + millis + " ms");
        }
        // Four ORs of 200 atoms in one predicate.
        List<ScalarOperator> big = new ArrayList<>();
        for (String form : List.of("eq", "ne", "ge", "not")) {
            big.add(or(form, 200));
        }
        ScalarOperator and = Utils.compoundAnd(big);
        estimate(and);
        long start = System.nanoTime();
        estimate(and);
        long millis = (System.nanoTime() - start) / 1_000_000;
        Assertions.assertTrue(millis < 500, "four ORs of 200 atoms: " + millis + " ms");
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
