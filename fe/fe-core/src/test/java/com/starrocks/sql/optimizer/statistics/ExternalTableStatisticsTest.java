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
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ExternalTableStatisticsTest {
    private Table table() {
        Table table = mock(Table.class);
        when(table.getName()).thenReturn("t");
        when(table.getColumn(anyString())).thenAnswer(call -> new Column(call.getArgument(0), IntegerType.BIGINT));
        return table;
    }

    private TStatisticData row(String name, long rows) {
        return new TStatisticData().setColumnName(name).setRowCount(rows).setDataSize(rows * 8)
                .setCountDistinct(7).setNullCount(2).setMin("-20").setMax("9007199254740993")
                .setUpdateTime("2026-09-28 01:02:03");
    }

    @Test
    void roundTripPreparesIdenticalScalarsAndReusesThemAcrossRequests() {
        Table table = table();
        TStatisticData first = row("first", 100);
        ExternalTableStatistics decoded = ExternalTableStatistics.decode(
                ExternalTableStatistics.encode(List.of(first, row("другая\"колонка", 200)), table), table, Map.of());
        ColumnStatistic expected = ColumnBasicStatsCacheLoader.buildColumnStatistics(
                first, "", "", "t", "first", IntegerType.BIGINT);
        ColumnStatistic actual = decoded.columns.get("first");
        Assertions.assertEquals(expected.getMinValue(), actual.getMinValue());
        Assertions.assertEquals(expected.getMaxValue(), actual.getMaxValue());
        Assertions.assertEquals(expected.getNullsFraction(), actual.getNullsFraction());
        Assertions.assertEquals(expected.getAverageRowSize(), actual.getAverageRowSize());
        Assertions.assertEquals(expected.getDistinctValuesCount(), actual.getDistinctValuesCount());
        Assertions.assertSame(decoded.summaries.get("first").raw, decoded.summaries.get("first").estimated);
        var narrow = ExternalStatisticsAggregate.fromTableRow(
                new ExternalStatisticsRequest("uuid", List.of(), List.of("first"), true), decoded);
        var wide = ExternalStatisticsAggregate.fromTableRow(
                new ExternalStatisticsRequest("uuid", List.of(), List.of("first", "другая\"колонка"), true), decoded);
        Assertions.assertSame(narrow.columns, wide.columns);
        Assertions.assertSame(actual, narrow.columns.get("first"));
        Assertions.assertEquals(100, narrow.rowCount);
        Assertions.assertEquals(200, wide.rowCount);
        Assertions.assertTrue(wide.hasCompleteCoverage());
        var missing = ExternalStatisticsAggregate.fromTableRow(
                new ExternalStatisticsRequest("uuid", List.of(), List.of("first", "new"), true), decoded);
        Assertions.assertTrue(missing.hasUnknownColumns());
        Assertions.assertFalse(missing.hasCompleteCoverage());
    }

    @Test
    void unknownStoredColumnKeepsMetadataFallbackWithoutPoisoningOtherRequests() {
        var valid = new ExternalColumnStatistics.Summary(
                new com.starrocks.connector.statistics.ConnectorTableColumnStats(
                        ColumnStatistic.builder().setDistinctValuesCount(7).build(), 100, ""),
                new com.starrocks.connector.statistics.ConnectorTableColumnStats(
                        ColumnStatistic.builder().setDistinctValuesCount(7).build(), 100, ""), "bigint(20)");
        var invalid = new ExternalColumnStatistics.Summary(
                com.starrocks.connector.statistics.ConnectorTableColumnStats.unknown(),
                com.starrocks.connector.statistics.ConnectorTableColumnStats.unknown(), "bigint(20)");
        var table = new ExternalTableStatistics(Map.of("valid", valid, "invalid", invalid));
        var narrow = ExternalStatisticsAggregate.fromTableRow(
                new ExternalStatisticsRequest("uuid", List.of(), List.of("valid"), true), table);
        var wide = ExternalStatisticsAggregate.fromTableRow(
                new ExternalStatisticsRequest("uuid", List.of(), List.of("valid", "invalid"), true), table);
        Assertions.assertFalse(narrow.hasUnknownColumns());
        Assertions.assertTrue(wide.hasUnknownColumns());
        Assertions.assertTrue(wide.hasCompleteCoverage());
    }

    @Test
    void wideTableUsesAtomicArrayChunksAndRejectsCorruptRows() {
        Table table = table();
        List<TStatisticData> rows = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            rows.add(row("column_" + i, 100));
        }
        List<String> chunks = ExternalTableStatistics.encode(rows, table);
        Assertions.assertTrue(chunks.size() > 1);
        var decoded = ExternalTableStatistics.decode(chunks, table, Map.of());
        Assertions.assertEquals(3000, decoded.columns.size());
        List<String> duplicate = new ArrayList<>(chunks);
        duplicate.add(chunks.get(0));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ExternalTableStatistics.decode(duplicate, table, Map.of()));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ExternalTableStatistics.decode(
                List.of(chunks.get(0).replace("\"version\":1", "\"version\":99")), table, Map.of()));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ExternalTableStatistics.decode(
                ExternalTableStatistics.encode(List.of(row("bad", -1)), table), table, Map.of()));
    }

    @Test
    void schemaChangesCannotReinterpretOldBounds() {
        Table table = table();
        var chunks = ExternalTableStatistics.encode(List.of(row("a", 100), row("b", 200)), table);
        when(table.getColumn("a")).thenReturn(new Column("a", IntegerType.INT));
        when(table.getColumn("b")).thenReturn(null);
        Assertions.assertTrue(ExternalTableStatistics.decode(chunks, table, Map.of()).columns.isEmpty());
    }
    @Test
    void typedBoundsAndSampleExtrapolationRemainUnchanged() {
        var types = Map.of("d", com.starrocks.type.DateType.DATE,
                "ts", com.starrocks.type.DateType.DATETIME,
                "text", com.starrocks.type.VarcharType.VARCHAR,
                "i", IntegerType.LARGEINT);
        Table table = table();
        types.forEach((name, type) -> when(table.getColumn(name)).thenReturn(new Column(name, type)));
        List<TStatisticData> rows = List.of(
                row("d", 100).setMin("2026-01-01").setMax("2026-09-28"),
                row("ts", 100).setMin("2026-01-01 01:02:03").setMax("2026-09-28 20:21:22"),
                row("text", 100).setMin("a\"\\б").setMax("z"),
                row("i", 100).setMin("-170141183460469231731687303715884105728")
                        .setMax("170141183460469231731687303715884105727"));
        var sample = new com.starrocks.statistic.ColumnStatsMeta("d",
                com.starrocks.statistic.StatsConstants.AnalyzeType.SAMPLE, java.time.LocalDateTime.now(),
                java.util.Set.of(1L, 2L), 8);
        var decoded = ExternalTableStatistics.decode(ExternalTableStatistics.encode(rows, table), table,
                Map.of("d", sample));
        for (TStatisticData row : rows) {
            var expected = ColumnBasicStatsCacheLoader.buildColumnStatistics(row, "", "", "t", row.columnName,
                    types.get(row.columnName));
            var actual = decoded.columns.get(row.columnName);
            Assertions.assertEquals(expected.getMinValue(), actual.getMinValue());
            Assertions.assertEquals(expected.getMaxValue(), actual.getMaxValue());
            Assertions.assertEquals(expected.getNullsFraction(), actual.getNullsFraction());
        }
        Assertions.assertEquals(100, decoded.summaries.get("d").raw.getRowCount());
        Assertions.assertEquals(400, decoded.summaries.get("d").estimated.getRowCount());
    }

}
