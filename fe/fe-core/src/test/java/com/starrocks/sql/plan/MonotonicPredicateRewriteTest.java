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
 * Plan tests for exact monotonic predicate inversion: f(col) cmp constant becomes the
 * equivalent predicate on the bare column, which every pruning layer can consume.
 */
public class MonotonicPredicateRewriteTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;

        // month partitions over a bare DATE column
        starRocksAssert.withTable("CREATE TABLE `rw_month` (\n"
                + "  `id` bigint NOT NULL,\n"
                + "  `d` date NOT NULL,\n"
                + "  `v` bigint NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`id`, `d`)\n"
                + "PARTITION BY RANGE(`d`)\n"
                + "(PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),\n"
                + "PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01')),\n"
                + "PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),\n"
                + "PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01')),\n"
                + "PARTITION p202405 VALUES [('2024-05-01'), ('2024-06-01')),\n"
                + "PARTITION p202406 VALUES [('2024-06-01'), ('2024-07-01')))\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");

        // two partition columns: the expression further-prune serves only single-column
        // range tables, so before the inversion a date_trunc predicate never pruned here
        starRocksAssert.withTable("CREATE TABLE `rw_multi` (\n"
                + "  `ts` datetime NOT NULL,\n"
                + "  `id` bigint NOT NULL,\n"
                + "  `v` bigint NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`ts`, `id`)\n"
                + "PARTITION BY RANGE(`ts`, `id`)\n"
                + "(PARTITION p202401 VALUES [('2024-01-01', '0'), ('2024-02-01', '0')),\n"
                + "PARTITION p202402 VALUES [('2024-02-01', '0'), ('2024-03-01', '0')),\n"
                + "PARTITION p202403 VALUES [('2024-03-01', '0'), ('2024-04-01', '0')),\n"
                + "PARTITION p202404 VALUES [('2024-04-01', '0'), ('2024-05-01', '0')))\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");
    }

    @Test
    public void testDateTruncEqInvertsToPeriod() throws Exception {
        // the period bounds select exactly one partition and then disappear from the scan
        // (every remaining row satisfies them); nothing of the expression is left either
        String plan = getFragmentPlan("select * from rw_month where date_trunc('month', d) = '2024-03-01'");
        assertContains(plan, "partitions=1/6");
        assertNotContains(plan, "date_trunc");
    }

    @Test
    public void testDateTruncMisalignedEqPrunesEverything() throws Exception {
        // '2024-03-15' is not a month start: the preimage is empty, expressed as an empty
        // interval on the column (never constant FALSE - a NULL d must keep yielding NULL).
        // d is NOT NULL here, so folding collapses the whole scan
        String plan = getFragmentPlan("select * from rw_month where date_trunc('month', d) = '2024-03-15'");
        assertContains(plan, "0:EMPTYSET");
    }

    @Test
    public void testDateTruncDayOnDateKeepsPointShape() throws Exception {
        // a day period on a DATE column is a single point; the equality shape survives,
        // point filters feed bucket pruning
        String plan = getFragmentPlan("select * from rw_month where date_trunc('day', d) = '2024-03-05'");
        assertContains(plan, "2: d = '2024-03-05'");
        assertContains(plan, "partitions=1/6");
    }

    @Test
    public void testDateTruncRangeCmps() throws Exception {
        // month periods coincide with whole partitions here, so the inverted bounds prune
        // and then disappear from the scan; the partition counts pin the case table
        // (unaligned GE rounds up to the next period start, etc.)
        String plan = getFragmentPlan("select * from rw_month where date_trunc('month', d) >= '2024-03-15'");
        assertContains(plan, "partitions=3/6");
        assertNotContains(plan, "date_trunc");

        plan = getFragmentPlan("select * from rw_month where date_trunc('month', d) <= '2024-03-15'");
        assertContains(plan, "partitions=3/6");

        plan = getFragmentPlan("select * from rw_month where date_trunc('month', d) > '2024-03-01'");
        assertContains(plan, "partitions=3/6");

        plan = getFragmentPlan("select * from rw_month where date_trunc('month', d) < '2024-03-01'");
        assertContains(plan, "partitions=2/6");
    }

    @Test
    public void testDayShiftInverts() throws Exception {
        // days_add signatures are DATETIME-only, so the column arrives cast-wrapped; the
        // shift inverts onto the cast and ReduceCastRule folds it back to the bare column
        String plan = getFragmentPlan("select * from rw_month where days_add(d, 3) <= '2024-03-08'");
        assertContains(plan, "2: d <= '2024-03-05'");
        assertContains(plan, "partitions=3/6");
    }

    @Test
    public void testDayShiftUpwardCmpGetsOverflowGuard() throws Exception {
        // days_add(d, 3) is NULL for d above '9999-12-28' and the original predicate rejects
        // such rows; the open-upward inversion carries a guard bound cutting that tail off
        // (asserted at the rule level). Here every selected partition sits fully below the
        // guard, so the scan keeps only the lower bound.
        String plan = getFragmentPlan("select * from rw_month where days_add(d, 3) >= '2024-03-08'");
        assertContains(plan, "2: d >= '2024-03-05'");
        assertContains(plan, "partitions=4/6");
        assertNotContains(plan, "days_add");
    }

    @Test
    public void testMultiColumnRangePartitionPrunesByBareColumn() throws Exception {
        // 3/4 and not 2/4: with a second partition column the first partition's upper bound
        // ('2024-02-01', 0) still admits rows with ts = '2024-02-01' and a negative id, so
        // the pruner must keep it; the inverted bounds stay in the scan (partition edges do
        // not coincide with them)
        String plan = getFragmentPlan("select * from rw_multi"
                + " where date_trunc('month', ts) >= '2024-02-01' and date_trunc('month', ts) <= '2024-03-01'");
        assertContains(plan, "1: ts >= '2024-02-01 00:00:00'");
        assertContains(plan, "1: ts < '2024-04-01 00:00:00'");
        assertContains(plan, "partitions=3/4");
    }

    @Test
    public void testNotContextStaysEquivalent() throws Exception {
        // the inverse is an equivalence, so it fires under NOT as well; the negated bound
        // selects exactly two partitions and disappears from the scan
        String plan = getFragmentPlan("select * from rw_month where not (date_trunc('month', d) >= '2024-03-01')");
        assertContains(plan, "partitions=2/6");
        assertNotContains(plan, "date_trunc");
    }

    @Test
    public void testMonthShiftDoesNotInvert() throws Exception {
        // month arithmetic clamps day-of-month; no exact inverse exists and the predicate
        // stays on the expression. Partitions still shrink: the expression further-prune
        // maps each partition's own bounds through months_add, which is sound per partition.
        // The clamp tail lives in p202401 (months_add('2024-01-30',1) = '2024-02-29') and
        // that partition is kept.
        String plan = getFragmentPlan("select * from rw_month where months_add(d, 1) <= '2024-02-29'");
        assertContains(plan, "months_add");
        assertContains(plan, "partitions=1/6");
    }

    @Test
    public void testSessionVariableOff() throws Exception {
        try {
            connectContext.getSessionVariable().setEnableMonotonicPredicateRewrite(false);
            String plan = getFragmentPlan("select * from rw_month where date_trunc('month', d) = '2024-03-01'");
            assertContains(plan, "date_trunc('month', 2: d)");
        } finally {
            connectContext.getSessionVariable().setEnableMonotonicPredicateRewrite(true);
        }
    }

}
