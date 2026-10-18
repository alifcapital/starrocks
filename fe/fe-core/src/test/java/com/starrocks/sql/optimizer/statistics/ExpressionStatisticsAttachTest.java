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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExpressionStatisticsAttachTest {
    private static final String TEXT = "substr(`c_phone`, 1, 3)";
    private static final String ELEMENT = "expr:1:" + TEXT;
    private static ConnectContext context;
    private static Table table;
    private static ExternalMcvStatistics.Expression expression;

    private final ColumnRefOperator name = new ColumnRefOperator(1, table.getColumn("c_name").getType(), "c_name", true);
    private final ColumnRefOperator phone =
            new ColumnRefOperator(2, table.getColumn("c_phone").getType(), "c_phone", true);

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        context = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(context);
        table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, "hive0", "tpch", "customer");
        ExpressionTemplate template = ExpressionTemplate.parse(TEXT, table, context);
        expression = new ExternalMcvStatistics.Expression(TEXT, template.getType().toSql(),
                List.of(new ExternalMcvStatistics.StoredSource("c_phone",
                        table.getColumn("c_phone").getType().toSql(), null)), template);
    }

    private static ExternalMcvStatistics.Group singleton() {
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of(ELEMENT), 1000, 5,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("103"), 600, List.of(600L)),
                        new MultiColumnCombinedStats.McvEntry(List.of("104"), 200, List.of(200L))),
                List.of(), List.of(100L), List.of(expression));
        group.prepare(expression.getTemplate().getType());
        return group;
    }

    private static ExternalMcvStatistics.Group mixed() {
        return new ExternalMcvStatistics.Group(List.of("c_name", ELEMENT), 1000, 50,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("alice", "103"), 300, List.of(400L, 600L))),
                List.of(), List.of(0L, 100L), Arrays.asList(null, expression));
    }

    private Map<ColumnRefOperator, Column> read(ColumnRefOperator... refs) {
        Map<ColumnRefOperator, Column> map = new HashMap<>();
        for (ColumnRefOperator ref : refs) {
            map.put(ref, table.getColumn(ref.getName()));
        }
        return map;
    }

    private Statistics base(Map<ColumnRefOperator, Column> read) {
        Statistics.Builder builder = Statistics.builder().setOutputRowCount(1000);
        read.keySet().forEach(ref -> builder.addColumnStatistic(ref,
                new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 10, 500)));
        return builder.build();
    }

    private Statistics attach(List<ExternalMcvStatistics.Group> groups, Map<ColumnRefOperator, Column> read) {
        int[] ids = {0};
        return ScanExpressionStatistics.attach(base(read), new ExternalMcvStatistics(groups), read, table,
                () -> --ids[0]);
    }

    @Test
    public void testSingletonExpressionGetsVirtualColumnOutsideTheScanStatistics() {
        Statistics statistics = attach(List.of(singleton()), read(name, phone));
        ScanExpressionStatistics expressions = statistics.getScanExpressionStatistics();
        Assertions.assertNotNull(expressions);
        Assertions.assertEquals(1, expressions.getEntries().size());
        ScanExpressionStatistics.Entry entry = expressions.getEntries().get(0);
        ColumnRefOperator x = entry.getColumn();
        Assertions.assertTrue(x.getId() < 0);
        Assertions.assertTrue(x.getName().startsWith("expr#"));
        Assertions.assertEquals(2, entry.getStatistic().getHistogram().getMCV().size());
        Assertions.assertEquals(0.1, entry.getStatistic().getNullsFraction(), 1e-9);
        Assertions.assertEquals(Set.of(phone), expressions.getSourceColumns());
        Assertions.assertEquals(Set.of(Set.of(x)), expressions.getGroups().keySet());
        // x is not a column of the scan statistics and is in no column group of the scan.
        Assertions.assertEquals(Set.of(name, phone), statistics.getColumnStatistics().keySet());
        Assertions.assertTrue(statistics.getMultiColumnCombinedStats().isEmpty());
        Assertions.assertSame(entry, expressions.getEntry(entry.getKey()));
        Assertions.assertSame(entry, expressions.getEntry(x));
    }

    @Test
    public void testExpressionNeedsEveryUnchangedSourceColumn() {
        Assertions.assertNull(attach(List.of(singleton()), read(name)).getScanExpressionStatistics());
        // The cache can hold the group while the schema changes: a source of another type does not attach it.
        Map<ColumnRefOperator, Column> changed = new HashMap<>(read(name));
        changed.put(phone, new Column("c_phone", IntegerType.INT));
        Assertions.assertNull(attach(List.of(singleton()), changed).getScanExpressionStatistics());
    }

    @Test
    public void testPartitionRestrictedScanGetsNothing() {
        // MCV on a scan of selected partitions, including an incremental Iceberg scan, stays off.
        Map<ColumnRefOperator, Column> read = read(name, phone);
        Statistics restricted = Statistics.buildFrom(base(read)).setPartitionRestricted(true).build();
        Assertions.assertSame(restricted, StatisticsCalcUtils.withExternalMcvStats(table, restricted, read, null));
    }

    @Test
    public void testExpressionOnlyInMixedGroupGetsElementStatistics() {
        Statistics statistics = attach(List.of(mixed()), read(name, phone));
        ScanExpressionStatistics expressions = statistics.getScanExpressionStatistics();
        Assertions.assertNotNull(expressions);
        ScanExpressionStatistics.Entry entry = expressions.getEntries().get(0);
        ColumnStatistic x = entry.getStatistic();
        Assertions.assertFalse(x.isUnknown());
        Assertions.assertEquals(ColumnStatistic.StatisticType.ESTIMATE, x.getType());
        Assertions.assertEquals(0.1, x.getNullsFraction(), 1e-9);
        // The head holds one value of x, and 300 rows of x are outside it; the group NDV is the bound.
        Assertions.assertEquals(50, x.getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(1, x.getHistogram().getMCV().size());
        Assertions.assertEquals(Set.of(Set.of(name, entry.getColumn())), expressions.getGroups().keySet());
        // The real column of the group gets its marginal from the same head.
        Assertions.assertNotNull(statistics.getColumnStatistic(name).getHistogram());
    }

    private Statistics baseWithPhoneNdv(Map<ColumnRefOperator, Column> read, double phoneNdv) {
        Statistics.Builder builder = Statistics.builder().setOutputRowCount(1000);
        read.keySet().forEach(ref -> builder.addColumnStatistic(ref, new ColumnStatistic(Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY, 0, 10, ref.equals(phone) ? phoneNdv : 500)));
        return builder.build();
    }

    private ColumnStatistic elementOfPhone(double phoneNdv) {
        Map<ColumnRefOperator, Column> read = read(name, phone);
        int[] ids = {0};
        Statistics statistics = ScanExpressionStatistics.attach(baseWithPhoneNdv(read, phoneNdv),
                new ExternalMcvStatistics(List.of(mixed())), read, table, () -> --ids[0]);
        return statistics.getScanExpressionStatistics().getEntries().get(0).getStatistic();
    }

    @Test
    public void testElementNdvIsBoundedByTheColumnsItIsReadFromAndNotByTheEstimateOfTheExpression() {
        // The head holds one value of x and 300 rows are outside it, and the group has 50 values. The estimate of the
        // prefix of 3 characters of a column of 500 values and 10 characters is 500^(3/10) = 6.45, which is a guess and not
        // a bound: x has the 50 values of the group.
        Assertions.assertEquals(50, elementOfPhone(500).getDistinctValuesCount(), 1e-9);
        // The column has 20 values, so x has no more: the bound is the NDV of the column.
        Assertions.assertEquals(20, elementOfPhone(20).getDistinctValuesCount(), 1e-9);
        // A column of one value bounds x to the head value and the tail: at least the 2 values it has.
        Assertions.assertEquals(2, elementOfPhone(1).getDistinctValuesCount(), 1e-9);
        // The rows of the prefix are not bounded by the guess of the expression either way.
        Assertions.assertEquals(0.1, elementOfPhone(20).getNullsFraction(), 1e-9);
    }

    // The statistics of the scan: c_phone of the NDV, which is a guess when flagged, c_name with a known NDV.
    private Statistics guessedBase(Map<ColumnRefOperator, Column> read, double phoneNdv, boolean guess) {
        Statistics.Builder builder = Statistics.builder().setOutputRowCount(1000);
        read.keySet().forEach(ref -> builder.addColumnStatistic(ref, ColumnStatistic.builder()
                .setMinValue(Double.NEGATIVE_INFINITY).setMaxValue(Double.POSITIVE_INFINITY).setNullsFraction(0)
                .setAverageRowSize(10).setDistinctValuesCount(ref.equals(phone) ? phoneNdv : 500)
                .setDistinctValuesFromDefaultSelectivity(ref.equals(phone) && guess).build()));
        return builder.build();
    }

    private ColumnStatistic xOf(List<ExternalMcvStatistics.Group> groups, double phoneNdv, boolean guess) {
        Map<ColumnRefOperator, Column> read = read(name, phone);
        int[] ids = {0};
        Statistics statistics = ScanExpressionStatistics.attach(guessedBase(read, phoneNdv, guess),
                new ExternalMcvStatistics(groups), read, table, () -> --ids[0]);
        return statistics.getScanExpressionStatistics().getEntries().get(0).getStatistic();
    }

    @Test
    public void testCollectedNdvOfTheExpressionIsNotAGuessWhateverTheColumnIs() {
        // The estimate of the prefix takes the mark of a guess from the column, which is its precondition.
        Map<ColumnRefOperator, Column> read = read(name, phone);
        ScalarOperator prefix = ExpressionStatisticsCanonicalizer.canonicalize(ExpressionTemplate.parse(TEXT, table, context)
                .instantiate(Map.of("c_phone", phone)));
        Assertions.assertTrue(ExpressionStatisticCalculator.calculate(prefix, guessedBase(read, 500, true))
                .isDistinctValuesFromDefaultSelectivity());
        Assertions.assertFalse(ExpressionStatisticCalculator.calculate(prefix, guessedBase(read, 500, false))
                .isDistinctValuesFromDefaultSelectivity());
        for (boolean guess : new boolean[] {false, true}) {
            // The singleton group gives the NDV of x: 4, the 5 values of the group and the NULL.
            ColumnStatistic singleton = xOf(List.of(singleton()), 500, guess);
            Assertions.assertEquals(4, singleton.getDistinctValuesCount(), 1e-9);
            Assertions.assertFalse(singleton.isDistinctValuesFromDefaultSelectivity(), "singleton " + guess);
            // The group of two elements gives the NDV of the group, 50, and the column of 500 values does not bind it.
            ColumnStatistic element = xOf(List.of(mixed()), 500, guess);
            Assertions.assertEquals(50, element.getDistinctValuesCount(), 1e-9);
            Assertions.assertFalse(element.isDistinctValuesFromDefaultSelectivity(), "element " + guess);
            // The column of 20 values binds it: x is as much a guess as the NDV of the column.
            ColumnStatistic bound = xOf(List.of(mixed()), 20, guess);
            Assertions.assertEquals(20, bound.getDistinctValuesCount(), 1e-9);
            Assertions.assertEquals(guess, bound.isDistinctValuesFromDefaultSelectivity(), "bound " + guess);
        }
    }

    @Test
    public void testSingletonExpressionTakesTheAverageSizeFromTheEstimateOfThePrefix() {
        Map<ColumnRefOperator, Column> read = read(name, phone);
        int[] ids = {0};
        Statistics statistics = ScanExpressionStatistics.attach(base(read), new ExternalMcvStatistics(List.of(singleton())),
                read, table, () -> --ids[0]);
        ColumnStatistic x = statistics.getScanExpressionStatistics().getEntries().get(0).getStatistic();
        // The prefix of 3 characters of strings of 10: 3 bytes, and the collected NDV and NULL fraction.
        Assertions.assertEquals(3, x.getAverageRowSize(), 1e-9);
        Assertions.assertEquals(4, x.getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(0.1, x.getNullsFraction(), 1e-9);
    }

    @Test
    public void testSourceNdvIsTheProductOfTheColumnsNotAboveTheRows() {
        ColumnRefOperator a = new ColumnRefOperator(11, com.starrocks.type.VarcharType.VARCHAR, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(12, com.starrocks.type.VarcharType.VARCHAR, "b", true);
        ColumnRefOperator unknown = new ColumnRefOperator(13, com.starrocks.type.VarcharType.VARCHAR, "u", true);
        Statistics statistics = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(a, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 10, 20))
                .addColumnStatistic(b, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 10, 30))
                .addColumnStatistic(unknown, ColumnStatistic.unknown()).build();
        // One column: its NDV. Two columns: the product. A column twice counts once.
        Assertions.assertEquals(20, ScanExpressionStatistics.sourceNdv(call(a), statistics), 1e-9);
        Assertions.assertEquals(600, ScanExpressionStatistics.sourceNdv(call(a, b), statistics), 1e-9);
        Assertions.assertEquals(20, ScanExpressionStatistics.sourceNdv(call(a, a), statistics), 1e-9);
        // Not more than the rows.
        Statistics fewRows = statistics.withOutputRowCount(100);
        Assertions.assertEquals(100, ScanExpressionStatistics.sourceNdv(call(a, b), fewRows), 1e-9);
        // A column of unknown statistics, or none, gives no bound.
        Assertions.assertTrue(Double.isInfinite(ScanExpressionStatistics.sourceNdv(call(a, unknown), statistics)));
        Assertions.assertTrue(Double.isInfinite(ScanExpressionStatistics.sourceNdv(call(), statistics)));
        ColumnRefOperator missing = new ColumnRefOperator(14, com.starrocks.type.VarcharType.VARCHAR, "m", true);
        Assertions.assertTrue(Double.isInfinite(ScanExpressionStatistics.sourceNdv(call(missing), statistics)));
    }

    private static com.starrocks.sql.optimizer.operator.scalar.ScalarOperator call(ColumnRefOperator... columns) {
        List<com.starrocks.sql.optimizer.operator.scalar.ScalarOperator> arguments = new java.util.ArrayList<>(List.of(columns));
        arguments.add(com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createVarchar("x"));
        return new com.starrocks.sql.optimizer.operator.scalar.CallOperator("concat",
                com.starrocks.type.VarcharType.VARCHAR, arguments);
    }

    @Test
    public void testElementNdvWithTheBoundOfItsColumns() {
        ExternalMcvStatistics.Group group = mixed();
        // The bound given to the element: within the group, at least the head and the tail.
        Assertions.assertEquals(7, ScanExpressionStatistics.elementStatistic(group, 1,
                com.starrocks.type.VarcharType.VARCHAR, ColumnStatistic.unknown(), 7).getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(50, ScanExpressionStatistics.elementStatistic(group, 1,
                com.starrocks.type.VarcharType.VARCHAR, ColumnStatistic.unknown(), 500).getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(2, ScanExpressionStatistics.elementStatistic(group, 1,
                com.starrocks.type.VarcharType.VARCHAR, ColumnStatistic.unknown(), 1).getDistinctValuesCount(), 1e-9);
        // Without a bound the tail's rows are the most values it can have, and the group's NDV: min(50, 1 + 300).
        Assertions.assertEquals(50, ScanExpressionStatistics.elementStatistic(group, 1,
                com.starrocks.type.VarcharType.VARCHAR, ColumnStatistic.unknown(), Double.POSITIVE_INFINITY)
                .getDistinctValuesCount(), 1e-9);
    }

    @Test
    public void testOneVirtualColumnPerExpression() {
        Statistics statistics = attach(List.of(singleton(), mixed()), read(name, phone));
        ScanExpressionStatistics expressions = statistics.getScanExpressionStatistics();
        Assertions.assertEquals(1, expressions.getEntries().size());
        ColumnRefOperator x = expressions.getEntries().get(0).getColumn();
        Assertions.assertEquals(Set.of(Set.of(x), Set.of(name, x)), expressions.getGroups().keySet());
        // The singleton holds the buckets of x, so it gives the statistics of x.
        Assertions.assertEquals(2, expressions.getEntries().get(0).getStatistic().getHistogram().getMCV().size());
    }

    @Test
    public void testFieldStaysOnlyWithTheScanStatistics() {
        Statistics statistics = attach(List.of(singleton()), read(name, phone));
        ScanExpressionStatistics expressions = statistics.getScanExpressionStatistics();
        Assertions.assertNull(Statistics.buildFrom(statistics).build().getScanExpressionStatistics());
        Assertions.assertSame(expressions, statistics.withOutputRowCount(10).getScanExpressionStatistics());
        Assertions.assertSame(expressions, statistics.withJoinStatisticsProvenance(null, new JoinStatisticsPlanner())
                .getScanExpressionStatistics());
    }

    @Test
    public void testColumnOfMultiColumnGroupGetsItsMarginal() {
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("c_name", "c_phone"), 1000, 40,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("alice", "103"), 300, List.of(400L, 600L)),
                        new MultiColumnCombinedStats.McvEntry(List.of("bob", "103"), 200, List.of(250L, 600L))),
                List.of(), List.of(50L, 0L));
        Map<ColumnRefOperator, Column> read = read(name, phone);
        Statistics statistics = StatisticsCalcUtils.attachExternalMcvStats(base(read),
                new ExternalMcvStatistics(List.of(group)), read);
        ColumnStatistic column = statistics.getColumnStatistic(name);
        Assertions.assertNotNull(column.getHistogram());
        Assertions.assertEquals(0.05, column.getNullsFraction(), 1e-9);
        Assertions.assertEquals(2, column.getHistogram().getMCV().size());
        // Within the bounds: at least the two head values and the tail, at most the base and the group NDV.
        Assertions.assertEquals(40, column.getDistinctValuesCount(), 1e-9);
        Assertions.assertEquals(ColumnStatistic.StatisticType.ESTIMATE, column.getType());
        Assertions.assertEquals(1, statistics.getColumnStatistic(phone).getHistogram().getMCV().size());
    }

    @Test
    public void testNdvOfTheMarginalOfAColumnIsNotAboveTheNdvOfTheColumn() {
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("c_name", "c_phone"), 1000, 40,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("alice", "103"), 300, List.of(400L, 600L)),
                        new MultiColumnCombinedStats.McvEntry(List.of("bob", "103"), 200, List.of(250L, 600L))),
                List.of(), List.of(50L, 0L));
        Map<ColumnRefOperator, Column> read = read(name, phone);
        Statistics narrow = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(name, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 10, 25))
                .addColumnStatistic(phone, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 10, 500))
                .build();
        // The column has 25 values, and the group 40: the marginal has the 25 of the column, as it is its own source.
        Assertions.assertEquals(25, StatisticsCalcUtils.attachExternalMcvStats(narrow,
                new ExternalMcvStatistics(List.of(group)), read).getColumnStatistic(name).getDistinctValuesCount(), 1e-9);
    }

    private static ExternalMcvStatistics.Group plainNameGroup(long rows) {
        return new ExternalMcvStatistics.Group(List.of("c_name", "c_address"), 1000, 20,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("alice", "a1"), rows, List.of(rows, rows))),
                List.of(), List.of(0L, 0L));
    }

    @Test
    public void testColumnWithChangedTypeKeepsItsStatistics() {
        // After a type change to JSON the head values of c_name are not values of its type; the column is left as it
        // is and the other column of the group still gets its marginal.
        ColumnRefOperator json = new ColumnRefOperator(1, com.starrocks.type.JsonType.JSON, "c_name", true);
        ColumnRefOperator address = new ColumnRefOperator(3, table.getColumn("c_address").getType(), "c_address", true);
        Map<ColumnRefOperator, Column> read = Map.of(json, table.getColumn("c_name"),
                address, table.getColumn("c_address"));
        Statistics base = base(read);
        Statistics statistics = StatisticsCalcUtils.attachExternalMcvStats(base,
                new ExternalMcvStatistics(List.of(plainNameGroup(100))), read);
        Assertions.assertSame(base.getColumnStatistic(json), statistics.getColumnStatistic(json));
        Assertions.assertEquals(Map.of("a1", 100L), statistics.getColumnStatistic(address).getHistogram().getMCV());
    }

    @Test
    public void testColumnWithOwnHistogramKeepsIt() {
        ColumnRefOperator address = new ColumnRefOperator(3, table.getColumn("c_address").getType(), "c_address", true);
        Map<ColumnRefOperator, Column> read = read(name, address);
        Histogram analyzed = Histogram.forStrings(List.of(), Map.of("carol", 500L));
        Statistics base = Statistics.buildFrom(base(read)).addColumnStatistic(name,
                new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 10, 500, analyzed,
                        ColumnStatistic.StatisticType.ESTIMATE)).build();
        Statistics statistics = StatisticsCalcUtils.attachExternalMcvStats(base,
                new ExternalMcvStatistics(List.of(plainNameGroup(100))), read);
        Assertions.assertSame(analyzed, statistics.getColumnStatistic(name).getHistogram());
        // The column without its own histogram gets the marginal: 100 rows of a1.
        Assertions.assertEquals(Map.of("a1", 100L), statistics.getColumnStatistic(address).getHistogram().getMCV());
    }

    @Test
    public void testMarginalComesFromTheGroupThatCoversMoreRows() {
        ColumnRefOperator address = new ColumnRefOperator(3, table.getColumn("c_address").getType(), "c_address", true);
        Map<ColumnRefOperator, Column> read = read(name, phone, address);
        // MCV(c_name, c_address) holds 100 rows of c_name in its head, MCV(c_name, left(c_phone, 3)) holds 900.
        ExternalMcvStatistics.Group mixed = new ExternalMcvStatistics.Group(List.of("c_name", ELEMENT), 1000, 30,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("bob", "103"), 900, List.of(900L, 900L))),
                List.of(), List.of(0L, 0L), Arrays.asList(null, expression));
        int[] ids = {0};
        Statistics statistics = ScanExpressionStatistics.attach(base(read),
                new ExternalMcvStatistics(List.of(plainNameGroup(100), mixed)), read, table, () -> --ids[0]);
        Assertions.assertEquals(Map.of("bob", 900L), statistics.getColumnStatistic(name).getHistogram().getMCV());
        // With the head counts the other way round, the group of columns gives the marginal.
        statistics = ScanExpressionStatistics.attach(base(read), new ExternalMcvStatistics(List.of(plainNameGroup(950),
                mixed)), read, table, () -> --ids[0]);
        Assertions.assertEquals(Map.of("alice", 950L), statistics.getColumnStatistic(name).getHistogram().getMCV());
    }

    @Test
    public void testGroupsOfTheSameCoverageAreChosenByTheirElements() {
        ColumnRefOperator address = new ColumnRefOperator(3, table.getColumn("c_address").getType(), "c_address", true);
        Map<ColumnRefOperator, Column> read = read(name, phone, address);
        // Both groups hold 900 rows of c_name in their head; MCV(c_name, c_address) sorts before the group with the
        // expression, in whichever order the groups come.
        ExternalMcvStatistics.Group mixed = new ExternalMcvStatistics.Group(List.of("c_name", ELEMENT), 1000, 30,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("bob", "103"), 900, List.of(900L, 900L))),
                List.of(), List.of(0L, 0L), Arrays.asList(null, expression));
        int[] ids = {0};
        for (List<ExternalMcvStatistics.Group> groups : List.of(List.of(plainNameGroup(900), mixed),
                List.of(mixed, plainNameGroup(900)))) {
            Statistics statistics = ScanExpressionStatistics.attach(base(read), new ExternalMcvStatistics(groups), read,
                    table, () -> --ids[0]);
            Assertions.assertEquals(Map.of("alice", 900L), statistics.getColumnStatistic(name).getHistogram().getMCV());
        }
    }
}
