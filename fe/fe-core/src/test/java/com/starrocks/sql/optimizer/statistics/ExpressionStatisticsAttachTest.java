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
