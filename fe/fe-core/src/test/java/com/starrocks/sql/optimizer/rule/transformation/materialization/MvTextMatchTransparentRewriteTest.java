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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class MvTextMatchTransparentRewriteTest extends MVTestBase {
    private static final String DB = MockIcebergMetadata.MOCKED_EQ_DELETE_DB_NAME;
    private static final String TABLE = MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME + "." + DB + ".mv_text_tbl";

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        MockIcebergMetadata.mockEqualityDeleteMvTable("mv_text_tbl", false);
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    // A query with the text of a stale transparent MV is rewritten by text match into the fresh partitions of the MV and
    // the stale ones read from the table. Neither the transparent rewrite nor another MV rewrite may apply the MV to
    // that plan again, or some partitions are read twice.
    @Test
    public void testTextMatchOfAStaleTransparentMvReadsEachPartitionOnce() throws Exception {
        String mvQuery = "select id, data, p from " + TABLE;
        createAndRefreshMv("create materialized view mv_text_mv partition by p refresh deferred manual " +
                "properties ('transparent_mv_rewrite_mode' = 'true') as " + mvQuery);
        boolean textMatch = connectContext.getSessionVariable().isEnableMaterializedViewTextMatchRewrite();
        try {
            ((MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr()
                    .getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get())
                    .updatePartitions(DB, "mv_text_tbl", List.of("p=a"));
            connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(true);

            String plan = getFragmentPlan(mvQuery);
            Assertions.assertEquals(1, StringUtils.countMatches(plan, "TABLE: mv_text_mv\n"), plan);
            Assertions.assertEquals(1, StringUtils.countMatches(plan, "TABLE: " + DB + ".mv_text_tbl\n"), plan);
        } finally {
            connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(textMatch);
            dropMv(DB_NAME, "mv_text_mv");
        }
    }
}
