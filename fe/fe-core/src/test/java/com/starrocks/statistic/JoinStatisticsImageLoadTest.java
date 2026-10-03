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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class JoinStatisticsImageLoadTest {
    private static JsonObject meta(long id, String name) {
        JoinStatisticsDefinition source = JoinStatisticsCacheTest.definition();
        var definition = new JoinStatisticsDefinition(name, source.getSources(), source.getDomains(), source.getProperties());
        return GsonUtils.GSON.toJsonTree(new JoinStatisticsMeta(id, definition)).getAsJsonObject();
    }

    @Test
    void invalidDefinitionsAndManifestsDoNotBlockFollowingRecords() throws Exception {
        List<JsonObject> records = new ArrayList<>();
        records.add(meta(1, "before"));
        JsonObject tooManySources = meta(2, "five_sources");
        JsonArray sources = tooManySources.getAsJsonObject("definition").getAsJsonArray("sources");
        for (int i = 0; i < 3; i++) {
            sources.add(sources.get(0).deepCopy());
        }
        records.add(tooManySources);
        JsonObject tooManyDomains = meta(3, "four_domains");
        JsonArray domains = tooManyDomains.getAsJsonObject("definition").getAsJsonArray("domains");
        for (int i = 0; i < 3; i++) {
            domains.add(domains.get(0).deepCopy());
        }
        records.add(tooManyDomains);
        JsonObject badSource = meta(4, "unknown_source");
        JsonObject columns = badSource.getAsJsonObject("definition").getAsJsonArray("domains")
                .get(0).getAsJsonObject().getAsJsonObject("columns");
        columns.add("99", columns.remove("1"));
        records.add(badSource);
        JsonObject missingDefinition = meta(5, "missing_definition");
        missingDefinition.remove("definition");
        records.add(missingDefinition);
        JsonObject badManifest = meta(1, "before");
        badManifest.addProperty("generation", -1);
        records.add(badManifest);
        records.add(meta(6, "after"));
        AnalyzeMgr manager = new AnalyzeMgr();
        ExternalMcvImageLoadTest.load(manager, ExternalMcvImageLoadTest.image(
                List.of(ExternalMcvImageLoadTest.mcv("valid_mcv")), records));
        Assertions.assertEquals(1, manager.getExternalMcvStatsMetaMap().size());
        Assertions.assertEquals(2, manager.getJoinStatisticsRegistry().snapshot().size());
        Assertions.assertEquals(1, manager.getJoinStatisticsRegistry().get("before").getId());
        Assertions.assertEquals(0, manager.getJoinStatisticsRegistry().get("before").getGeneration());
        Assertions.assertEquals(6, manager.getJoinStatisticsRegistry().get("after").getId());
        var saved = new UtFrameUtils.PseudoImage();
        manager.save(saved.getImageWriter());
        AnalyzeMgr restored = new AnalyzeMgr();
        var reader = saved.getMetaBlockReader();
        try {
            restored.load(reader);
        } finally {
            reader.close();
        }
        Assertions.assertEquals(2, restored.getJoinStatisticsRegistry().snapshot().size());
    }

    @Test
    void legacyPhysicalIdentityFallsBackToUuid() throws Exception {
        JsonObject legacy = meta(1, "legacy");
        legacy.getAsJsonObject("definition").getAsJsonArray("sources").forEach(source ->
                source.getAsJsonObject().remove("tableUuid"));
        AnalyzeMgr manager = new AnalyzeMgr();
        ExternalMcvImageLoadTest.load(manager, ExternalMcvImageLoadTest.image(List.of(), List.of(legacy)));
        var sources = manager.getJoinStatisticsRegistry().get("legacy").getDefinition().getSources();
        Assertions.assertEquals("a-uuid", sources.get(0).getTableUuid());
        Assertions.assertEquals("b-uuid", sources.get(1).getTableUuid());
    }
}
