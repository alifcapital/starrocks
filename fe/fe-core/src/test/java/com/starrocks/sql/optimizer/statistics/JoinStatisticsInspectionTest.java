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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.statistic.JoinStatisticsMeta;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinStatisticsInspectionTest {
    static JoinStatisticsData fixture() throws Exception {
        List<JoinStatisticsData.Source> sources = new ArrayList<>();
        List<List<JoinStatisticsBasis.Slice>> sides = new ArrayList<>();
        long[][] head = { {2, 3}, {4, 1}};
        for (int side = 0; side < 2; side++) {
            var degree = JoinStatisticsEstimateTest.degree(new long[] {head[side][0], head[side][1], 2});
            sources.add(new JoinStatisticsData.Source("uuid" + side, 123, 7, List.of("predicate"),
                    List.of(VarcharType.VARCHAR), List.of(Arrays.asList(side == 0 ? null : "Германия")),
                    new long[] {7}, Map.of(0, List.of(degree))));
            int[] orders = JoinStatisticsBasis.momentOrders();
            double[][] moments = new double[orders.length][JoinStatisticsBasis.WIDTH];
            for (int order = 0; order < orders.length; order++) {
                for (int layout = 0; layout < JoinStatisticsBasis.TAIL_LAYOUTS; layout++) {
                    moments[order][layout * JoinStatisticsBasis.TAIL_BUCKETS] = Math.pow(2, orders[order]);
                }
            }
            sides.add(List.of(new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(head[side]), moments, false)));
        }
        var basis = new JoinStatisticsBasis(0, List.of(0, 1), sides,
                List.of(new JoinStatisticsBasis.Pair(0, 1, 1, 1, new double[] {15, 7, 7, 3})),
                new JoinStatisticsHeadKeys(new String[] {"7", null}));
        var original = new JoinStatisticsData(1, 2, sources, List.of(basis));
        // Inspect the production codec's restored representation, including prepared tail norms.
        return JoinStatisticsCodec.decode(JoinStatisticsCodec.encode(original, 1 << 20), 1 << 20, 1, 2);
    }

    static JoinStatisticsMeta meta() {
        return new JoinStatisticsMeta(1, JoinStatisticsEstimateTest.definition(2), 2, 1, 100, "test", 1000);
    }

    private static JsonObject details(List<String> row) {
        return JsonParser.parseString(row.get(6)).getAsJsonObject();
    }

    @Test
    void decodedContentsExposeExactHeadsMomentsAndStoredTailNorms() throws Exception {
        var rows = JoinStatisticsInspection.show(meta(), fixture(), 0, 1000).getResultRows();
        for (int i = 0; i < rows.size(); i++) {
            assertEquals("2", rows.get(i).get(0));
            assertEquals(Integer.toString(i), rows.get(i).get(1));
        }
        var degree = details(rows.stream().filter(r -> r.get(2).equals("DEGREE") && r.get(3).equals("0"))
                .findFirst().orElseThrow());
        assertEquals(7, degree.get("rows").getAsLong());
        assertEquals(3, degree.get("ndv").getAsLong());
        assertEquals(3, degree.get("maximum_frequency").getAsLong());
        assertEquals(17, degree.getAsJsonObject("frequency_moments").get("2").getAsDouble());
        var labels = rows.stream().filter(r -> r.get(2).equals("HEAD_KEY")).map(JoinStatisticsInspectionTest::details).toList();
        assertEquals("7", labels.get(0).getAsJsonArray("values").get(0).getAsString());
        assertFalse(labels.get(1).get("label_retained").getAsBoolean());
        assertTrue(labels.get(1).get("values").isJsonNull());
        var nullPredicate = details(rows.stream().filter(r -> r.get(2).equals("SLICE") && r.get(3).equals("0"))
                .findFirst().orElseThrow());
        assertTrue(nullPredicate.getAsJsonArray("values").get(0).isJsonNull());
        assertEquals(14, rows.stream().filter(r -> r.get(2).equals("DEGREE"))
                .mapToLong(r -> details(r).get("rows").getAsLong()).sum());
        assertEquals(10, rows.stream().filter(r -> r.get(2).equals("HEAD"))
                .mapToLong(r -> details(r).get("frequency").getAsLong()).sum());
        var tails = rows.stream().filter(r -> r.get(2).equals("TAIL")).toList();
        assertEquals(6, tails.size());
        for (var row : tails) {
            var norms = details(row).getAsJsonObject("stored_lp_norms");
            assertEquals(1, norms.get("0").getAsDouble());
            assertEquals(2, norms.get("12").getAsDouble(), 1e-10);
        }
        assertEquals("[15.0,7.0,7.0,3.0]", details(rows.stream().filter(r -> r.get(2).equals("PAIR"))
                .findFirst().orElseThrow()).get("products_by_presence_mask").toString());
    }

    @Test
    void pageBoundariesReconstructTheSameGenerationWithoutDroppingOrRepeatingRows() throws Exception {
        var data = fixture();
        var all = JoinStatisticsInspection.show(meta(), data, 0, 1000).getResultRows();
        for (int size : List.of(1, 2, 5, 9)) {
            List<List<String>> combined = new ArrayList<>();
            for (int offset = 0; offset < all.size(); offset += size) {
                combined.addAll(JoinStatisticsInspection.show(meta(), data, offset, size).getResultRows());
            }
            assertEquals(all, combined);
        }
        assertTrue(JoinStatisticsInspection.show(meta(), data, Long.MAX_VALUE, 100).getResultRows().isEmpty());
        assertTrue(JoinStatisticsInspection.show(meta(), data, 0, 0).getResultRows().isEmpty());
        assertThrows(IllegalArgumentException.class, () -> JoinStatisticsInspection.show(meta(), data, 0, 1001));
        assertThrows(IllegalArgumentException.class, () -> JoinStatisticsInspection.show(meta(), data, -1, 1));
        var stale = new JoinStatisticsMeta(1, meta().getDefinition(), 3, 1, 100, "test", 1000);
        assertThrows(IllegalArgumentException.class, () -> JoinStatisticsInspection.show(stale, data, 0, 1));
    }
}
