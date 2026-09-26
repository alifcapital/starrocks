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

package com.starrocks.statistic;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.metablock.SRMetaBlockWriterV2;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class ExternalMcvImageLoadTest {
    static JsonObject mcv(String table) {
        return GsonUtils.GSON.toJsonTree(new ExternalMcvStatsMeta("iceberg", "db", table, List.of("key"),
                StatsConstants.AnalyzeType.FULL, null, LocalDateTime.of(2026, 1, 1, 0, 0), Map.of())).getAsJsonObject();
    }

    // The final JOIN section is a trailing extension when running in the standalone MCV branch.
    static String image(List<JsonObject> mcv, List<JsonObject> join) throws Exception {
        StringWriter text = new StringWriter();
        JsonWriter json = new JsonWriter(text);
        json.setLenient(true);
        var writer = new SRMetaBlockWriterV2(json, SRMetaBlockID.ANALYZE_MGR, 9 + mcv.size() + join.size());
        for (int i = 0; i < 7; i++) {
            writer.writeInt(0);
        }
        writer.writeInt(mcv.size());
        for (JsonObject meta : mcv) {
            writer.writeJson(meta);
        }
        writer.writeInt(join.size());
        for (JsonObject meta : join) {
            writer.writeJson(meta);
        }
        writer.close();
        return text.toString();
    }

    static void load(AnalyzeMgr manager, String image) throws Exception {
        var reader = new SRMetaBlockReaderV2(new JsonReader(new StringReader(image)));
        try {
            manager.load(reader);
        } finally {
            reader.close();
        }
    }

    @Test
    void invalidRecordsAreSkippedAndValidNeighborsSurviveAnotherImage() throws Exception {
        List<JsonObject> records = new ArrayList<>();
        records.add(mcv("before"));
        for (String field : List.of("catalogName", "dbName", "tableName", "columnNames", "analyzeType", "updateTime")) {
            JsonObject bad = mcv("bad_" + field);
            bad.remove(field);
            records.add(bad);
        }
        for (String columns : List.of("[]", "[null]", "[\"\"]", "[\"key\",\"KEY\"]")) {
            JsonObject bad = mcv("bad_columns");
            bad.add("columnNames", JsonParser.parseString(columns));
            records.add(bad);
        }
        // A bad duplicate must not overwrite the already restored valid metadata.
        JsonObject duplicate = mcv("before");
        duplicate.remove("updateTime");
        records.add(duplicate);
        records.add(mcv("after"));
        AnalyzeMgr manager = new AnalyzeMgr();
        load(manager, image(records, List.of()));
        Assertions.assertEquals(2, manager.getExternalMcvStatsMetaMap().size());
        Assertions.assertTrue(manager.getExternalMcvStatsMetaMap().values().stream()
                .allMatch(meta -> List.of("before", "after").contains(meta.getTableName())));
        var saved = new UtFrameUtils.PseudoImage();
        manager.save(saved.getImageWriter());
        AnalyzeMgr restored = new AnalyzeMgr();
        var reader = saved.getMetaBlockReader();
        try {
            restored.load(reader);
        } finally {
            reader.close();
        }
        Assertions.assertEquals(manager.getExternalMcvStatsMetaMap().keySet(), restored.getExternalMcvStatsMetaMap().keySet());
    }

    @Test
    void absentOptionalLegacyFieldsAreAccepted() throws Exception {
        JsonObject legacy = mcv("legacy");
        for (String field : List.of("tableUUID", "statisticsTypes", "properties")) {
            legacy.remove(field);
        }
        AnalyzeMgr manager = new AnalyzeMgr();
        load(manager, image(List.of(legacy), List.of()));
        Assertions.assertEquals(1, manager.getExternalMcvStatsMetaMap().size());
    }

    @Test
    void malformedRecordAndTruncatedStreamStillAbortLoading() throws Exception {
        JsonObject malformed = mcv("malformed");
        malformed.add("columnNames", new JsonObject()); // Cannot be decoded as a list.
        Assertions.assertThrows(Exception.class, () -> load(new AnalyzeMgr(), image(List.of(malformed), List.of())));
        String full = image(List.of(mcv("before"), mcv("after")), List.of());
        String truncated = full.substring(0, full.indexOf("after") + 2);
        Assertions.assertThrows(Exception.class, () -> load(new AnalyzeMgr(), truncated));
    }

    @Test
    void errorsAreNotSwallowed() throws Exception {
        new MockUp<AnalyzeMgr>() {
            @Mock
            public void replayAddExternalMcvStatsMeta(ExternalMcvStatsMeta meta) {
                throw new AssertionError("fatal error");
            }
        };
        Assertions.assertThrows(AssertionError.class,
                () -> load(new AnalyzeMgr(), image(List.of(mcv("valid")), List.of())));
    }
}
