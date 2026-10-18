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

import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ExpressionStatisticsLoadTest {
    private static final String TEXT = "substr(`c_phone`, 1, 3)";
    private static ConnectContext context;
    private static Table table;
    private static String phoneType;
    private static String expressionType;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        context = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(context);
        table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, "hive0", "tpch", "customer");
        phoneType = table.getColumn("c_phone").getType().toSql();
        expressionType = ExpressionTemplate.parse(TEXT, table, context).getType().toSql();
    }

    private static String element(String text, String type, String source, String sourceType, String extra) {
        return "{\"expr\":\"" + text.replace("\"", "\\\"") + "\",\"format\":1,\"type\":\"" + type
                + "\",\"columns\":[{\"name\":\"" + source + "\",\"type\":\"" + sourceType + "\"" + extra + "}]}";
    }

    private static ExternalMcvStatistics.Group parse(String columnNames, int width) {
        String mcv = width == 1 ? "[[[\"103\"],\"60\",[\"60\"]],[[\"104\"],\"30\",[\"30\"]]]"
                : "[[[\"alice\",\"103\"],\"60\",[\"70\",\"60\"]]]";
        return ExternalMcvStatsCacheLoader.parseGroup(List.of(columnNames, "100", width == 1 ? "3" : "20", mcv, "[]",
                width == 1 ? "[0]" : "[0,0]"));
    }

    @Test
    public void testObjectElementIsParsedAndBound() {
        ExternalMcvStatistics.Group group = parse("[\"c_name\"," + element(TEXT, expressionType, "c_phone",
                phoneType, "") + "]", 2);
        Assertions.assertNotNull(group);
        Assertions.assertEquals(List.of("c_name", "expr:1:" + TEXT), group.getColumnNames());
        Assertions.assertTrue(group.hasExpressions());
        Assertions.assertNull(group.getExpressions().get(0));
        ExternalMcvStatistics.Expression expression = group.getExpressions().get(1);
        Assertions.assertEquals(TEXT, expression.getText());
        Assertions.assertEquals(List.of(new ExternalMcvStatistics.StoredSource("c_phone", phoneType, null)),
                expression.getSources());
        Assertions.assertNull(expression.getTemplate());

        ExternalMcvStatistics.Group bound = ExternalMcvStatsCacheLoader.bindExpressions(group, table, context, "uuid");
        Assertions.assertNotNull(bound);
        Assertions.assertEquals(TEXT, bound.getExpressions().get(1).getTemplate().getText());
        Assertions.assertEquals(group.getMcv().size(), bound.getMcv().size());
        Assertions.assertEquals(group.getColumnNames(), bound.getColumnNames());
    }

    @Test
    public void testSingletonExpressionIsPreparedByItsType() {
        ExternalMcvStatistics.Group group = ExternalMcvStatsCacheLoader.bindExpressions(
                parse("[" + element(TEXT, expressionType, "c_phone", phoneType, "") + "]", 1), table, context,
                "uuid");
        Assertions.assertNotNull(group);
        group.prepare(group.getExpressions().get(0).getTemplate().getType());
        ColumnStatistic statistic = group.columnStatistic(group.getExpressions().get(0).getTemplate().getType(),
                ColumnStatistic.unknown()).orElseThrow();
        Assertions.assertNotNull(statistic.getHistogram());
        Assertions.assertEquals(2, statistic.getHistogram().getMCV().size());
    }

    @Test
    public void testTableNotFoundByItsUuidLoadsThePlainGroups() {
        ExternalMcvStatsCacheLoader loader = new ExternalMcvStatsCacheLoader();
        String element = "[" + element(TEXT, expressionType, "c_phone", phoneType, "") + "]";
        // A plain group of two columns does not need the table; a singleton and an expression group do.
        ExternalMcvStatistics gone = loader.resolveAndBind(List.of(parse("[\"c_name\",\"c_phone\"]", 2),
                parse("[\"c_phone\"]", 1), parse(element, 1)), context, "hive0.tpch.gone.0");
        Assertions.assertEquals(List.of(List.of("c_name", "c_phone")),
                gone.getGroups().stream().map(ExternalMcvStatistics.Group::getColumnNames).toList());
        Assertions.assertTrue(gone.isIncomplete());
        Assertions.assertFalse(loader.backoff.active());
        // Another table loads all its groups with the same loader.
        ExternalMcvStatistics found = loader.resolveAndBind(List.of(parse("[\"c_name\",\"c_phone\"]", 2),
                parse("[\"c_phone\"]", 1), parse(element, 1)), context, table.getUUID());
        Assertions.assertEquals(3, found.getGroups().size());
        Assertions.assertFalse(found.isIncomplete());
    }

    @Test
    public void testChangedSchemaOrFormatLeavesTheGroupOut() {
        // A source of another type, a missing source and an Iceberg field id the table does not have.
        for (String columnNames : List.of(
                "[" + element(TEXT, expressionType, "c_phone", "int(11)", "") + "]",
                "[" + element("substr(`gone`, 1, 3)", expressionType, "gone", phoneType, "") + "]",
                "[" + element(TEXT, expressionType, "c_phone", phoneType, ",\"fieldId\":5") + "]",
                "[" + element(TEXT, "int(11)", "c_phone", phoneType, "") + "]")) {
            ExternalMcvStatistics.Group group = parse(columnNames, 1);
            Assertions.assertNotNull(group, columnNames);
            Assertions.assertNull(ExternalMcvStatsCacheLoader.bindExpressions(group, table, context, "uuid"),
                    columnNames);
        }
        Assertions.assertNull(ExternalMcvStatsCacheLoader.bindExpressions(
                parse("[" + element(TEXT, expressionType, "c_phone", phoneType, "") + "]", 1), null, context, "uuid"));
        // A format this FE does not know is not loaded at all.
        Assertions.assertNull(parse("[" + element(TEXT, expressionType, "c_phone", phoneType, "")
                .replace("\"format\":1", "\"format\":2") + "]", 1));
    }

    @Test
    public void testIcebergFieldIds() {
        // A table that cannot give its schema now cannot show that a recorded field id is the same; without a
        // recorded field id only the type is compared.
        com.starrocks.catalog.IcebergTable unavailable = org.mockito.Mockito.mock(com.starrocks.catalog.IcebergTable.class);
        org.mockito.Mockito.when(unavailable.getNativeTable()).thenThrow(new IllegalStateException("no table"));
        Assertions.assertFalse(ExpressionTemplate.fieldIdMatches(unavailable, "c", 7));
        Assertions.assertTrue(ExpressionTemplate.fieldIdMatches(unavailable, "c", null));
        // A table with its schema compares the field id, whatever the case of the column name.
        com.starrocks.catalog.IcebergTable iceberg = org.mockito.Mockito.mock(com.starrocks.catalog.IcebergTable.class);
        org.apache.iceberg.Table nativeTable = org.mockito.Mockito.mock(org.apache.iceberg.Table.class);
        org.mockito.Mockito.when(iceberg.getNativeTable()).thenReturn(nativeTable);
        org.mockito.Mockito.when(nativeTable.schema()).thenReturn(new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.optional(7, "C", org.apache.iceberg.types.Types.StringType.get())));
        Assertions.assertTrue(ExpressionTemplate.fieldIdMatches(iceberg, "c", 7));
        Assertions.assertFalse(ExpressionTemplate.fieldIdMatches(iceberg, "c", 8));
        // A Hive table has no field id: a recorded one means another table.
        Assertions.assertTrue(ExpressionTemplate.fieldIdMatches(table, "c_phone", null));
        Assertions.assertFalse(ExpressionTemplate.fieldIdMatches(table, "c_phone", 5));
    }

    @Test
    public void testStoredTextThatAnalyzesToAnotherTextIsLeftOut() {
        // A text an earlier canonical form wrote analyzes to 1 + `c_custkey` now; the statistics are not used.
        String custkeyType = table.getColumn("c_custkey").getType().toSql();
        String current = ExpressionTemplate.parse("(`c_custkey`) + 1", table, context).getText();
        Assertions.assertEquals("1 + `c_custkey`", current);
        String type = ExpressionTemplate.parse(current, table, context).getType().toSql();
        Assertions.assertNull(ExternalMcvStatsCacheLoader.bindExpressions(
                parse("[" + element("(`c_custkey`) + 1", type, "c_custkey", custkeyType, "") + "]", 1), table, context,
                "uuid"));
        Assertions.assertNotNull(ExternalMcvStatsCacheLoader.bindExpressions(
                parse("[" + element(current, type, "c_custkey", custkeyType, "") + "]", 1), table, context, "uuid"));
    }

    @Test
    public void testUnreadableSchemaFailsTheLoad() {
        // The schema cannot be read now: the load fails and is retried, instead of keeping the statistics out for
        // the life of the cache entry.
        com.starrocks.catalog.IcebergTable iceberg = org.mockito.Mockito.mock(com.starrocks.catalog.IcebergTable.class);
        org.mockito.Mockito.when(iceberg.getColumn("c_phone")).thenReturn(table.getColumn("c_phone"));
        org.mockito.Mockito.when(iceberg.getName()).thenReturn("customer");
        org.mockito.Mockito.when(iceberg.getNativeTable()).thenThrow(new IllegalStateException("no table"));
        ExternalMcvStatistics.Group group = parse("[" + element(TEXT, expressionType, "c_phone", phoneType,
                ",\"fieldId\":7") + "]", 1);
        Assertions.assertThrows(ExpressionTemplate.FieldIdUnavailableException.class,
                () -> ExternalMcvStatsCacheLoader.bindExpressions(group, iceberg, context, "uuid"));
        // At attach the statistics are left out of the query.
        Assertions.assertFalse(ExpressionTemplate.fieldIdMatches(iceberg, "c_phone", 7));
    }

    @Test
    public void testUnreadableSchemaLeavesOnlyItsGroupOut() {
        // The expression group of a table whose field ids cannot be read is left out of this load; the column group
        // loads, the result is marked incomplete and lives a minute in the cache, not the loader-wide pause.
        com.starrocks.catalog.IcebergTable iceberg = org.mockito.Mockito.mock(com.starrocks.catalog.IcebergTable.class);
        org.mockito.Mockito.when(iceberg.getColumn("c_phone")).thenReturn(table.getColumn("c_phone"));
        org.mockito.Mockito.when(iceberg.getColumn("c_name")).thenReturn(table.getColumn("c_name"));
        org.mockito.Mockito.when(iceberg.getName()).thenReturn("customer");
        org.mockito.Mockito.when(iceberg.getNativeTable()).thenThrow(new IllegalStateException("no table"));
        ExternalMcvStatistics.Group column = parse("[\"c_name\"]", 1);
        ExternalMcvStatistics.Group expression = parse("[" + element(TEXT, expressionType, "c_phone", phoneType,
                ",\"fieldId\":7") + "]", 1);
        ExternalMcvStatsCacheLoader loader = new ExternalMcvStatsCacheLoader();
        ExternalMcvStatistics loaded = loader.bindGroups(List.of(column, expression), iceberg, context, "uuid");
        Assertions.assertEquals(List.of(List.of("c_name")), loaded.getGroups().stream()
                .map(ExternalMcvStatistics.Group::getColumnNames).toList());
        Assertions.assertTrue(loaded.isIncomplete());
        long minute = java.util.concurrent.TimeUnit.SECONDS.toNanos(Math.min(60,
                com.starrocks.common.Config.statistic_update_interval_sec * 2));
        Assertions.assertEquals(minute, CachedStatisticStorage.externalMcvLifetimeNanos(java.util.Optional.of(loaded)));
        // Another table loads in full and lives as long as other statistics.
        ExternalMcvStatistics other = loader.bindGroups(List.of(parse("[" + element(TEXT,
                expressionType, "c_phone", phoneType, "") + "]", 1)), table, context, "other");
        Assertions.assertEquals(1, other.getGroups().size());
        Assertions.assertFalse(other.isIncomplete());
        Assertions.assertEquals(java.util.concurrent.TimeUnit.SECONDS.toNanos(
                        com.starrocks.common.Config.statistic_update_interval_sec * 2),
                CachedStatisticStorage.externalMcvLifetimeNanos(java.util.Optional.of(other)));
        Assertions.assertFalse(loader.backoff.active());
    }
}
