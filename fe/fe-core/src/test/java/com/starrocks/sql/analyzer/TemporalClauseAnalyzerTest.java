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

package com.starrocks.sql.analyzer;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static com.starrocks.sql.plan.ConnectorPlanTestBase.newFolder;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TemporalClauseAnalyzerTest {
    private static ConnectContext connectContext;

    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, newFolder(temp, "junit").toURI().toString());
        starRocksAssert.withDatabase("temporal_db").useDatabase("temporal_db")
                .withView("create view temporal_view as select 1 as a");
    }

    private static void assertRejected(String sql, String message) {
        Exception e = assertThrows(Exception.class, () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        assertTrue(e.getMessage().contains(message), e.getMessage());
    }

    @Test
    public void testRangeClausesOnIcebergAreRejected() {
        // Only AS OF reaches the Iceberg scan, so a range or ALL would read the current snapshot.
        String table = "select * from iceberg0.unpartitioned_db.t0 ";
        assertRejected(table + "for version between 1 and 2", "Only the AS OF temporal clause is supported");
        assertRejected(table + "for system_time from '2024-01-01 00:00:00' to '2024-01-02 00:00:00'",
                "Only the AS OF temporal clause is supported");
        assertRejected(table + "for system_time all", "Only the AS OF temporal clause is supported");
    }

    @Test
    public void testClauseOnViewIsRejected() {
        assertRejected("select * from temporal_db.temporal_view for system_time as of '2024-01-01 00:00:00'",
                "Unsupported table type for temporal clauses");
    }
}
