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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

class StatisticsHllTest {
    @Test
    void agreesWithBeForEveryEncodingAndOverlappingUnions() throws Exception {
        try (InputStreamReader reader = new InputStreamReader(
                getClass().getResourceAsStream("/statistics/hll-be-vectors.json"), StandardCharsets.UTF_8)) {
            JsonObject vectors = JsonParser.parseReader(reader).getAsJsonObject();
            Map<String, StatisticsHll> sketches = new HashMap<>();
            for (Map.Entry<String, JsonElement> entry : vectors.getAsJsonObject("sketches").entrySet()) {
                JsonObject value = entry.getValue().getAsJsonObject();
                byte[] encoded = Base64.getDecoder().decode(value.get("serialized").getAsString());
                StatisticsHll sketch = StatisticsHll.fromSerialized(encoded);
                // Cached values must own their bytes; a thrift buffer may be reused by its caller.
                Arrays.fill(encoded, (byte) 0);
                sketches.put(entry.getKey(), sketch);
                StatisticsHll.Union union = new StatisticsHll.Union();
                union.merge(sketch);
                Assertions.assertEquals(value.get("ndv").getAsLong(), union.estimate(), entry.getKey());
                union.merge(sketch);
                Assertions.assertEquals(value.get("ndv").getAsLong(), union.estimate(), "Idempotent merge");
            }
            for (JsonElement element : vectors.getAsJsonArray("unions")) {
                JsonObject test = element.getAsJsonObject();
                StatisticsHll.Union union = new StatisticsHll.Union();
                union.merge(sketches.get(test.get("left").getAsString()));
                union.merge(sketches.get(test.get("right").getAsString()));
                Assertions.assertEquals(test.get("ndv").getAsLong(), union.estimate(), test.toString());
                StatisticsHll saved = union.snapshot();
                StatisticsHll.Union restored = new StatisticsHll.Union();
                restored.merge(saved);
                Assertions.assertEquals(union.estimate(), restored.estimate(), "cached block union must remain mergeable");
                restored.merge(saved);
                Assertions.assertEquals(union.estimate(), restored.estimate(), "overlap must not add NDVs");
            }
            // Reusing sketches in other unions must not mutate them.
            for (Map.Entry<String, StatisticsHll> entry : sketches.entrySet()) {
                StatisticsHll.Union union = new StatisticsHll.Union();
                union.merge(entry.getValue());
                Assertions.assertEquals(vectors.getAsJsonObject("sketches").getAsJsonObject(entry.getKey())
                        .get("ndv").getAsLong(), union.estimate());
            }
            Assertions.assertTrue(sketches.get("one").retainedBytes() < 100);
            Assertions.assertTrue(sketches.get("sparse").retainedBytes() < 1000);
        }
    }

    @Test
    void rejectsMalformedAndTruncatedSketches() {
        byte[][] invalid = {
                {}, {4}, {0, 0}, {1}, {1, 1}, {1, (byte) 255},
                {2}, {2, -1, -1, -1, -1}, {2, 1, 0, 0, 0},
                {2, 1, 0, 0, 0, 0, 64, 1}, // register index 16384
                {2, 1, 0, 0, 0, 0, 0, 52}, // impossible register rank
                {3, 0}
        };
        for (byte[] bytes : invalid) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> StatisticsHll.fromSerialized(bytes));
        }
        byte[] full = new byte[16385];
        full[0] = 3;
        full[16384] = (byte) 255;
        Assertions.assertThrows(IllegalArgumentException.class, () -> StatisticsHll.fromSerialized(full));
        Assertions.assertThrows(IllegalArgumentException.class, () -> StatisticsHll.fromSerialized(null));
        Assertions.assertEquals(0, new StatisticsHll.Union().estimate());
    }
}
