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

import com.starrocks.common.FeConstants;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.ScanNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The scans of one equality-delete block share planned files through queues keyed by the block id and the
 * planning predicate. Every block gets its own id, and the delete scans of a block plan with the final
 * predicate of its data scan with deletes.
 */
public class IcebergEqualityDeleteBlockPlanTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    private static List<IcebergScanNode> icebergScans(ExecPlan plan) {
        List<IcebergScanNode> scans = new ArrayList<>();
        for (ScanNode scan : plan.getScanNodes()) {
            if (scan instanceof IcebergScanNode icebergScan) {
                scans.add(icebergScan);
            }
        }
        return scans;
    }

    private static boolean isDataScanWithDeletes(IcebergScanNode scan) {
        return scan.getMORParams().getScanTaskType() == IcebergMORParams.ScanTaskType.DATA_FILE_WITH_EQ_DELETE;
    }

    private static boolean isDeleteScan(IcebergScanNode scan) {
        return scan.getMORParams().getScanTaskType() == IcebergMORParams.ScanTaskType.EQ_DELETE;
    }

    // Both consumers of an inlined CTE scan the same table object. Their blocks must not share an id, or the
    // data scans would take files from one queue and each would see only part of the table.
    @Test
    public void testInlinedCteConsumersGetTheirOwnBlockIds() throws Exception {
        connectContext.getSessionVariable().setCboCteReuse(false);
        try {
            ExecPlan plan = getExecPlan("with c as (select * from iceberg0.eq_delete_db.eq_delete_partitioned) "
                    + "select count(*) from c a join c b on a.id <=> b.id");
            List<IcebergScanNode> dataScans = icebergScans(plan).stream()
                    .filter(IcebergEqualityDeleteBlockPlanTest::isDataScanWithDeletes)
                    .collect(Collectors.toList());
            Assertions.assertEquals(2, dataScans.size());
            Set<Long> blockIds = dataScans.stream()
                    .map(scan -> scan.getTableFullMORParams().getMORId())
                    .collect(Collectors.toSet());
            Assertions.assertEquals(2, blockIds.size());
        } finally {
            connectContext.getSessionVariable().setCboCteReuse(true);
        }
    }

    // The delete scans of a block plan Iceberg with the predicate of its data scan with deletes, including the
    // condition derived from the join, so the block is planned once and reads the deletes of the scanned files.
    @Test
    public void testDeleteScansPlanWithDataScanPredicate() throws Exception {
        ExecPlan plan = getExecPlan("select t.id from iceberg0.eq_delete_db.eq_delete_partitioned t "
                + "join iceberg0.eq_delete_db.eq_delete_global g on t.p = g.p where g.p = 'a'");
        Map<Long, IcebergScanNode> dataScanByBlock = new HashMap<>();
        for (IcebergScanNode scan : icebergScans(plan)) {
            if (isDataScanWithDeletes(scan)) {
                Assertions.assertNull(dataScanByBlock.put(scan.getTableFullMORParams().getMORId(), scan));
            }
        }
        Assertions.assertEquals(2, dataScanByBlock.size());
        int deleteScans = 0;
        for (IcebergScanNode scan : icebergScans(plan)) {
            if (isDeleteScan(scan)) {
                IcebergScanNode dataScan = dataScanByBlock.get(scan.getTableFullMORParams().getMORId());
                Assertions.assertNotNull(dataScan);
                Assertions.assertNotNull(dataScan.getIcebergJobPlanningPredicate());
                Assertions.assertSame(dataScan.getIcebergJobPlanningPredicate(), scan.getIcebergJobPlanningPredicate());
                deleteScans++;
            }
        }
        Assertions.assertEquals(2, deleteScans);
    }
}
