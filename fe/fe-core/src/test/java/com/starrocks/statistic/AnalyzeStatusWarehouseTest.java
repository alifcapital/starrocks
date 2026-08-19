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

import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AnalyzeStatusWarehouseTest {
    @Test
    public void testWarehouseSurvivesNativeStatusPersistence() {
        NativeAnalyzeStatus status = new NativeAnalyzeStatus();
        status.setWarehouseName("etl");
        String json = GsonUtils.GSON.toJson(status);
        Assertions.assertEquals("etl", GsonUtils.GSON.fromJson(json, NativeAnalyzeStatus.class).getWarehouseName());
        Assertions.assertEquals("", GsonUtils.GSON.fromJson("{}", NativeAnalyzeStatus.class).getWarehouseName());
    }

    @Test
    public void testWarehouseSurvivesExternalStatusPersistence() {
        ExternalAnalyzeStatus status = GsonUtils.GSON.fromJson(
                "{\"clazz\":\"ExternalAnalyzeStatus\"}", ExternalAnalyzeStatus.class);
        Assertions.assertEquals("", status.getWarehouseName());
        status.setWarehouseName("stats");
        String json = GsonUtils.GSON.toJson(status);
        Assertions.assertEquals("stats", GsonUtils.GSON.fromJson(json, ExternalAnalyzeStatus.class).getWarehouseName());
    }
}
