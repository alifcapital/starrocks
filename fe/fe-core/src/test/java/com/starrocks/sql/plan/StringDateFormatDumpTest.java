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

package com.starrocks.sql.plan;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** A dump keeps the string date formats of Iceberg tables, see StringDateFormatProperty, and replay uses them. */
public class StringDateFormatDumpTest extends ReplayFromDumpTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        ReplayFromDumpTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    private static String replay(String dump) throws Exception {
        return UtFrameUtils.getNewPlanAndFragmentFromDump(connectContext, getDumpInfoFromJson(dump)).second
                .getExplainString(TExplainLevel.NORMAL);
    }

    @Test
    public void testReplayGivesTheSamePlan() throws Exception {
        String sql = "select id from iceberg0.string_date_db.strdate_compact where cast(s as date) = '2024-03-05'";
        connectContext.getSessionVariable().setEnableQueryDump(true);
        String plan;
        try {
            plan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getExplainString(TExplainLevel.NORMAL);
        } finally {
            connectContext.getSessionVariable().setEnableQueryDump(false);
        }
        String dump = GsonUtils.GSON.toJson(connectContext.getDumpInfo());
        PlanTestBase.assertContains(plan, "s >= '20240305'", "s < '20240306'");
        QueryDumpInfo dumpInfo = getDumpInfoFromJson(dump);
        assertEquals(Map.of("string-date-format.s", "%Y%m%d"),
                dumpInfo.getIcebergStringDateFormatMap().get("string_date_db.strdate_compact"));

        assertEquals(plan, replay(dump));

        JsonObject withoutFormats = JsonParser.parseString(dump).getAsJsonObject();
        withoutFormats.remove("iceberg_string_date_formats");
        PlanTestBase.assertNotContains(replay(withoutFormats.toString()), "s >= '20240305'");
    }
}
