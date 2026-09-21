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

import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.StatisticsType;
import com.starrocks.statistic.ExternalMcvStatsMeta;
import com.starrocks.statistic.StatsConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ExternalMcvStatsCacheLoaderTest {
    @Test
    public void testParseGroup() {
        List<String> row = List.of("[\"status\",\"gate\"]", "1000", "12",
                "[[[\"approved\",\"0\"],\"540\",[\"700\",\"900\"]],[[null,\"1\"],\"7\"],[[\"short\"],\"3\"],"
                        + "[[\"zero\",\"0\"],\"0\"],[[\"odd\",\"2\"],\"5\",[\"9\"]]]");
        ExternalMcvStatistics.Group group = ExternalMcvStatsCacheLoader.parseGroup(row);
        Assertions.assertNotNull(group);
        Assertions.assertEquals(List.of("status", "gate"), group.getColumnNames());
        Assertions.assertEquals(1000, group.getRowCount());
        Assertions.assertEquals(12, group.getNdv());
        // Tuples of the wrong width and tuples with a zero count are dropped.
        Assertions.assertEquals(3, group.getMcv().size());
        Assertions.assertEquals(List.of("approved", "0"), group.getMcv().get(0).getValues());
        Assertions.assertEquals(540, group.getMcv().get(0).getCount());
        Assertions.assertEquals(List.of(700L, 900L), group.getMcv().get(0).getComponentCounts());
        Assertions.assertTrue(group.getMcv().get(0).hasComponentCounts());
        Assertions.assertEquals(Arrays.asList(null, "1"), group.getMcv().get(1).getValues());
        Assertions.assertFalse(group.getMcv().get(1).hasComponentCounts());
        // Component counts of the wrong width are dropped from their tuple.
        Assertions.assertEquals(List.of("odd", "2"), group.getMcv().get(2).getValues());
        Assertions.assertFalse(group.getMcv().get(2).hasComponentCounts());
    }

    @Test
    public void testParseGroupToleratesMissingMcvAndRejectsBadRows() {
        ExternalMcvStatistics.Group group = ExternalMcvStatsCacheLoader.parseGroup(
                Arrays.asList("[\"a\",\"b\"]", "10", "3", null));
        Assertions.assertNotNull(group);
        Assertions.assertTrue(group.getMcv().isEmpty());

        Assertions.assertNull(ExternalMcvStatsCacheLoader.parseGroup(Arrays.asList(null, "10", "3", "[]")));
        Assertions.assertNull(ExternalMcvStatsCacheLoader.parseGroup(List.of("[\"a\"]", "10", "3", "[]")));
        Assertions.assertNull(ExternalMcvStatsCacheLoader.parseGroup(List.of("not json", "10", "3", "[]")));
    }

    @Test
    public void testMetaGsonRoundTrip() {
        ExternalMcvStatsMeta meta = new ExternalMcvStatsMeta("hive0", "db", "t",
                List.of("status", "gate"), StatsConstants.AnalyzeType.FULL,
                List.of(StatisticsType.MCDISTINCT, StatisticsType.MCV),
                LocalDateTime.of(2026, 9, 20, 12, 0, 0), Map.of("k", "v"));
        meta.setTableUUID("uuid-1");
        String json = GsonUtils.GSON.toJson(meta);
        ExternalMcvStatsMeta copy = GsonUtils.GSON.fromJson(json, ExternalMcvStatsMeta.class);
        Assertions.assertEquals("hive0", copy.getCatalogName());
        Assertions.assertEquals("db", copy.getDbName());
        Assertions.assertEquals("t", copy.getTableName());
        Assertions.assertEquals(List.of("status", "gate"), copy.getColumnNames());
        Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, copy.getAnalyzeType());
        Assertions.assertEquals(List.of(StatisticsType.MCDISTINCT, StatisticsType.MCV), copy.getStatisticsTypes());
        Assertions.assertEquals(meta.getUpdateTime(), copy.getUpdateTime());
        Assertions.assertEquals("uuid-1", copy.getTableUUID());
        Assertions.assertEquals("v", copy.getProperties().get("k"));
    }
}
