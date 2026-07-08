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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Monotonic predicate derivation with iceberg tables on the receiving side. The mocked
 * iceberg catalog (see MockIcebergMetadata) has partitioned_db.t1, identity-partitioned by
 * the string column `date`.
 * <p>
 * A derived predicate on a bare iceberg column reaches the scan conjuncts and takes part in
 * file/partition pruning through the normal iceberg predicate conversion. A derived
 * predicate that stays on an expression (e.g. date_trunc over a month-transform column)
 * reaches the scan only as a row filter: iceberg file skipping needs a predicate on the
 * bare column.
 */
public class MonotonicPredicateDeriveIcebergTest extends ConnectorPlanTestBase {
    @BeforeAll
    public static void prepareTables() throws Exception {
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("CREATE TABLE `ice_event_dates` (\n"
                + "  `id` bigint NOT NULL,\n"
                + "  `datadate` date NOT NULL,\n"
                + "  `v` bigint NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`id`, `datadate`)\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");
    }

    @Test
    public void testDerivedEqualityLandsOnIcebergIdentityPartitionColumn() throws Exception {
        // datadate = '2024-03-05' maps through date_format(..., '%Y-%m-%d') to the point
        // '2024-03-05'; the equality moves it onto the bare iceberg partition column, and the
        // iceberg scan pushes it into planFiles
        String plan = getFragmentPlan("select f.id from iceberg0.partitioned_db.t1 f"
                + " join ice_event_dates e"
                + " on f.id = e.id and f.`date` = date_format(e.datadate, '%Y-%m-%d')"
                + " where e.datadate = '2024-03-05'");
        assertContains(plan, "IcebergScanNode");
        assertContains(plan, "date = '2024-03-05'");
    }

    @Test
    public void testDerivedRangeLandsOnIcebergIdentityPartitionColumn() throws Exception {
        String plan = getFragmentPlan("select f.id from iceberg0.partitioned_db.t1 f"
                + " join ice_event_dates e"
                + " on f.id = e.id and f.`date` = date_format(e.datadate, '%Y-%m-%d')"
                + " where e.datadate between '2024-03-05' and '2024-04-10'");
        assertContains(plan, "IcebergScanNode");
        assertContains(plan, "date >= '2024-03-05'");
        assertContains(plan, "date <= '2024-04-10'");
    }

    @Test
    public void testWrappedTransformJoinKeyDerivesResidualFilter() throws Exception {
        // t0_month is partitioned by the iceberg month(ts) transform; the derived predicate
        // stays on date_trunc('month', ts) and reaches the scan as a row filter only
        // (iceberg file skipping needs a predicate on the bare column)
        String plan = getFragmentPlan("select f.id from iceberg0.partitioned_transforms_db.t0_month f"
                + " join ice_event_dates e"
                + " on f.id = e.id and date_trunc('month', f.ts) = date_trunc('month', e.datadate)"
                + " where e.datadate between '2024-03-05' and '2024-04-10'");
        assertContains(plan, "IcebergScanNode");
        assertContains(plan, "date_trunc('month', 3: ts) >= '2024-03-01 00:00:00'");
        assertContains(plan, "date_trunc('month', 3: ts) <= '2024-04-01 00:00:00'");
    }

    @Test
    public void testHiveDerivedRangeOnBarePartitionColumnPrunes() throws Exception {
        // hive lineitem_par is partitioned by l_shipdate (5 date partitions + 1 NULL); the
        // derived range on the bare partition column prunes hive partitions
        String plan = getFragmentPlan("select f.l_orderkey from hive0.partitioned_db.lineitem_par f"
                + " join ice_event_dates e"
                + " on f.l_orderkey = e.id and f.l_shipdate = date_trunc('day', e.datadate)"
                + " where e.datadate between '1998-01-02' and '1998-01-03'");
        assertContains(plan, "partitions=2/6");
    }
}
