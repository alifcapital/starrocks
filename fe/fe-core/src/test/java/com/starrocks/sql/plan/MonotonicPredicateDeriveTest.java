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
import com.starrocks.qe.SessionVariableConstants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Plan tests for monotonic predicate derivation: a range filter on one join side is mapped
 * through a monotonic join-key expression and becomes a new conjunct on the other side,
 * where partition pruning can use it.
 */
public class MonotonicPredicateDeriveTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;

        // month-partitioned fact keyed by a bare DATE column (one partition per month)
        starRocksAssert.withTable("CREATE TABLE `fact_month` (\n"
                + "  `id` bigint NOT NULL,\n"
                + "  `dmonth` date NOT NULL,\n"
                + "  `v` bigint NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`id`, `dmonth`)\n"
                + "PARTITION BY RANGE(`dmonth`)\n"
                + "(PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),\n"
                + "PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01')),\n"
                + "PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),\n"
                + "PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01')),\n"
                + "PARTITION p202405 VALUES [('2024-05-01'), ('2024-06-01')),\n"
                + "PARTITION p202406 VALUES [('2024-06-01'), ('2024-07-01')))\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");

        // the motivating shape: month-partitioned fact keyed by an INT like 202403
        starRocksAssert.withTable("CREATE TABLE `fact_month_int` (\n"
                + "  `id` bigint NOT NULL,\n"
                + "  `datamonth` int NOT NULL,\n"
                + "  `v` bigint NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`id`, `datamonth`)\n"
                + "PARTITION BY RANGE(`datamonth`)\n"
                + "(PARTITION p202401 VALUES [('202401'), ('202402')),\n"
                + "PARTITION p202402 VALUES [('202402'), ('202403')),\n"
                + "PARTITION p202403 VALUES [('202403'), ('202404')),\n"
                + "PARTITION p202404 VALUES [('202404'), ('202405')),\n"
                + "PARTITION p202405 VALUES [('202405'), ('202406')),\n"
                + "PARTITION p202406 VALUES [('202406'), ('202407')))\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");

        // month-partitioned fact keyed by a DATETIME event timestamp
        starRocksAssert.withTable("CREATE TABLE `fact_ts` (\n"
                + "  `id` bigint NOT NULL,\n"
                + "  `ts` datetime NOT NULL,\n"
                + "  `v` bigint NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`id`, `ts`)\n"
                + "PARTITION BY RANGE(`ts`)\n"
                + "(PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),\n"
                + "PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01')),\n"
                + "PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),\n"
                + "PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01')),\n"
                + "PARTITION p202405 VALUES [('2024-05-01'), ('2024-06-01')),\n"
                + "PARTITION p202406 VALUES [('2024-06-01'), ('2024-07-01')))\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");

        // dimension carrying the selective date filter
        starRocksAssert.withTable("CREATE TABLE `event_dates` (\n"
                + "  `id` bigint NOT NULL,\n"
                + "  `datadate` date NOT NULL,\n"
                + "  `v` bigint NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`id`, `datadate`)\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");

        // dimension keyed by an INT date like 20240305
        starRocksAssert.withTable("CREATE TABLE `event_dates_int` (\n"
                + "  `id` bigint NOT NULL,\n"
                + "  `datadate` int NOT NULL,\n"
                + "  `v` bigint NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`id`, `datadate`)\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");
    }

    @Test
    public void testMotivatingIntMonthPattern() throws Exception {
        // datadate in ['2024-03-05','2024-04-10'] maps through date_format(..., '%Y%m') to
        // ['202403','202404'], and the equality moves it to cast(datamonth as varchar) on the
        // fact scan. All partition bounds are 6-digit ints, so their string images have equal
        // length and further-prune can use the cast conjuncts. 3/6 and not 2/6: the February
        // partition's closed image ['202402','202403'] touches the lower bound, and a
        // partition whose mapped bounds touch the range is kept.
        String plan = getFragmentPlan("select * from fact_month_int f join event_dates e"
                + " on f.id = e.id and cast(f.datamonth as varchar) = date_format(e.datadate, '%Y%m')"
                + " where e.datadate between '2024-03-05' and '2024-04-10'");
        assertContains(plan, "partitions=3/6");
    }

    @Test
    public void testImplicitNumericJoinKeyDerives() throws Exception {
        // no explicit cast on the join key: cbo_eq_base_type decides the common type of the
        // int-vs-varchar comparison. Under DECIMAL the digit-only rendering keeps its order
        // through the numeric cast, the derived DECIMAL bounds fold back onto the bare int
        // column (ReduceCastRule), and the native range pruner reads exactly two partitions.
        String sql = "select * from fact_month_int f join event_dates e"
                + " on f.id = e.id and f.datamonth = date_format(e.datadate, '%Y%m')"
                + " where e.datadate between '2024-03-05' and '2024-04-10'";
        String previousEqBaseType = connectContext.getSessionVariable().getCboEqBaseType();
        try {
            connectContext.getSessionVariable().setCboEqBaseType(SessionVariableConstants.DECIMAL);
            String plan = getFragmentPlan(sql);
            assertContains(plan, "partitions=2/6");

            // under VARCHAR both sides compare as strings and the derivation takes the
            // cast(int as varchar) path: the further-prune keeps the edge partition too
            connectContext.getSessionVariable().setCboEqBaseType(SessionVariableConstants.VARCHAR);
            plan = getFragmentPlan(sql);
            assertContains(plan, "CAST(2: datamonth AS VARCHAR(1048576)) >= '202403'");
            assertContains(plan, "partitions=3/6");
        } finally {
            connectContext.getSessionVariable().setCboEqBaseType(previousEqBaseType);
        }
    }

    @Test
    public void testIntDateCastJoinKeyDerives() throws Exception {
        // both filter endpoints are 8-digit ints, so cast(datadate as date) reads every value
        // of the domain as positional YYYYMMDD and the chain maps [20240305, 20240410] to
        // ['2024-03-01', '2024-04-01'] on the fact side
        String plan = getFragmentPlan("select * from fact_month f join event_dates_int e"
                + " on f.id = e.id and f.dmonth = date_trunc('month', cast(e.datadate as date))"
                + " where e.datadate between 20240305 and 20240410");
        assertContains(plan, "partitions=2/6");
    }

    @Test
    public void testIntDateCastWithImplicitNumericJoinKey() throws Exception {
        // the full int-to-int shape: an INT filter column through cast + date_format against
        // an INT fact key; under DECIMAL both sides land on decimal128 and the derived bounds
        // fold back onto the bare int column
        String sql = "select * from fact_month_int f join event_dates_int e"
                + " on f.id = e.id and f.datamonth = date_format(cast(e.datadate as date), '%Y%m')"
                + " where e.datadate between 20240305 and 20240410";
        String previousEqBaseType = connectContext.getSessionVariable().getCboEqBaseType();
        try {
            connectContext.getSessionVariable().setCboEqBaseType(SessionVariableConstants.DECIMAL);
            String plan = getFragmentPlan(sql);
            assertContains(plan, "partitions=2/6");
        } finally {
            connectContext.getSessionVariable().setCboEqBaseType(previousEqBaseType);
        }
    }

    @Test
    public void testIntDateCastRefusesOutsidePositionalSegment() throws Exception {
        // 690101 casts to 2069-01-01 and 710101 to 1971-01-01 (MySQL two-digit-year forms):
        // numeric order does not match date order there, so nothing is derived
        String plan = getFragmentPlan("select * from fact_month f join event_dates_int e"
                + " on f.id = e.id and f.dmonth = date_trunc('month', cast(e.datadate as date))"
                + " where e.datadate between 690101 and 710101");
        assertContains(plan, "partitions=6/6");

        // one endpoint outside the 8-digit segment refuses the whole interval
        plan = getFragmentPlan("select * from fact_month f join event_dates_int e"
                + " on f.id = e.id and f.dmonth = date_trunc('month', cast(e.datadate as date))"
                + " where e.datadate between 690101 and 20240410");
        assertContains(plan, "partitions=6/6");
    }

    @Test
    public void testDateTruncJoinKeyPrunesBareColumn() throws Exception {
        // the fact side of the equality is a bare partition column, so the derived range
        // [2024-03-01, 2024-04-01] goes through the normal range partition pruner. After
        // pruning the lower bound is removed from the scan (both selected partitions are
        // fully above it); only the upper bound stays as a row filter.
        String plan = getFragmentPlan("select * from fact_month f join event_dates e"
                + " on f.id = e.id and f.dmonth = date_trunc('month', e.datadate)"
                + " where e.datadate between '2024-03-05' and '2024-04-10'");
        assertContains(plan, "dmonth <= '2024-04-01'");
        assertContains(plan, "partitions=2/6");
    }

    @Test
    public void testMonthsAddJoinKey() throws Exception {
        // a registry monotonic function without a format argument
        String plan = getFragmentPlan("select * from fact_month f join event_dates e"
                + " on f.id = e.id and f.dmonth = months_add(e.datadate, 1)"
                + " where e.datadate between '2024-03-05' and '2024-04-10'");
        assertContains(plan, "dmonth >= '2024-04-05'");
        assertContains(plan, "dmonth <= '2024-05-10'");
        assertContains(plan, "partitions=2/6");
    }

    @Test
    public void testEqualityDomainDerivesPointPredicate() throws Exception {
        // a point domain [v, v] folds to one value, so the derived predicate is an equality
        String plan = getFragmentPlan("select * from fact_month f join event_dates e"
                + " on f.id = e.id and f.dmonth = date_trunc('month', e.datadate)"
                + " where e.datadate = '2024-03-05'");
        assertContains(plan, "dmonth = '2024-03-01'");
        assertContains(plan, "partitions=1/6");
    }

    @Test
    public void testExpressionOnBothSides() throws Exception {
        // the derived fact-side predicate lands on date_trunc('month', ts), the exact
        // inversion turns it into period bounds on the bare ts, and the native range pruner
        // selects exactly the two covered partitions - the bounds then disappear from the
        // scan entirely (every remaining row satisfies them)
        String plan = getFragmentPlan("select * from fact_ts f join event_dates e"
                + " on f.id = e.id and date_trunc('month', f.ts) = date_trunc('month', e.datadate)"
                + " where e.datadate between '2024-03-05' and '2024-04-10'");
        assertContains(plan, "partitions=2/6");
        assertNotContains(plan, "date_trunc('month', 2: ts) >=");
    }

    @Test
    public void testStrictRangeRelaxedToNonStrict() throws Exception {
        // date_trunc is non-strict: datadate > '2024-03-05' still allows
        // dmonth = '2024-03-01'. So the March partition stays (partitions=2/6) and the
        // derived bound is <=, never <
        String plan = getFragmentPlan("select * from fact_month f join event_dates e"
                + " on f.id = e.id and f.dmonth = date_trunc('month', e.datadate)"
                + " where e.datadate > '2024-03-05' and e.datadate < '2024-04-10'");
        assertContains(plan, "dmonth <= '2024-04-01'");
        assertContains(plan, "partitions=2/6");
        assertNotContains(plan, "dmonth > '2024-03-01'");
        assertNotContains(plan, "dmonth < '2024-04-01'");
    }

    @Test
    public void testNonOrderPreservingFormatNotDerived() throws Exception {
        // '%d%m' does not keep date order ('0503' vs '1004'); the registry format check
        // rejects it, nothing is derived
        String plan = getFragmentPlan("select * from fact_month_int f join event_dates e"
                + " on f.id = e.id and cast(f.datamonth as varchar) = date_format(e.datadate, '%d%m')"
                + " where e.datadate between '2024-03-05' and '2024-04-10'");
        assertNotContains(plan, ">= '0503'");
        assertContains(plan, "partitions=6/6");
    }

    @Test
    public void testNotEqualCorrelationNotDerived() throws Exception {
        // != gives no range to transfer
        String plan = getFragmentPlan("select * from fact_month f join event_dates e"
                + " on f.id = e.id and f.dmonth != date_trunc('month', e.datadate)"
                + " where e.datadate between '2024-03-05' and '2024-04-10'");
        assertNotContains(plan, "dmonth >= '2024-03-01'");
        assertContains(plan, "partitions=6/6");
    }

    @Test
    public void testCastIntToVarcharSourceNotDerived() throws Exception {
        // the filtered column is under cast(int as varchar), which does not keep order
        // ('999' > '1001' as strings), so there is no image toward the event side
        String plan = getFragmentPlan("select * from fact_month_int f join event_dates e"
                + " on f.id = e.id and cast(f.datamonth as varchar) = date_format(e.datadate, '%Y%m')"
                + " where f.datamonth between 202403 and 202404");
        assertNotContains(plan, ">= '202403'");
        assertNotContains(plan, "<= '202404'");
    }

    @Test
    public void testInListDerivedThroughRangeEnvelope() throws Exception {
        // RangeExtractor turns the IN list into the covering range
        // ['2024-03-05','2024-05-06'] before the image is computed, so the result is
        // [2024-03-01, 2024-05-01]: three partitions instead of the exact two. Correct,
        // just not point-precise.
        String plan = getFragmentPlan("select * from fact_month f join event_dates e"
                + " on f.id = e.id and f.dmonth = date_trunc('month', e.datadate)"
                + " where e.datadate in ('2024-03-05', '2024-05-06')");
        assertContains(plan, "dmonth <= '2024-05-01'");
        assertContains(plan, "partitions=3/6");
    }

    @Test
    public void testEndpointFoldOverflowNotDerived() throws Exception {
        // years_add(datadate, 9000) overflows DATE at both endpoints: the derivation is
        // dropped and the query still plans
        String plan = getFragmentPlan("select * from fact_month f join event_dates e"
                + " on f.id = e.id and f.dmonth = years_add(e.datadate, 9000)"
                + " where e.datadate between '2024-03-05' and '2024-04-10'");
        assertContains(plan, "partitions=6/6");
    }

    @Test
    public void testFullOuterJoinNotDerived() throws Exception {
        // full outer join keeps all rows on both sides: the rule rejects it in check()
        String plan = getFragmentPlan("select * from fact_month f full outer join event_dates e"
                + " on f.id = e.id and f.dmonth = date_trunc('month', e.datadate)");
        assertNotContains(plan, "dmonth >= ");
        assertContains(plan, "partitions=6/6");
    }

    @Test
    public void testLeftOuterJoinDerivesOnlyTowardsRight() throws Exception {
        // left outer join: the event side keeps all rows, the fact side produces NULLs for
        // non-matches, so only the fact (right) child may get a filter
        String plan = getFragmentPlan("select * from event_dates e left join fact_month f"
                + " on f.id = e.id and f.dmonth = date_trunc('month', e.datadate)"
                + " where e.datadate between '2024-03-05' and '2024-04-10'");
        assertContains(plan, "LEFT OUTER JOIN");
        assertContains(plan, "dmonth <= '2024-04-01'");
        assertContains(plan, "partitions=2/6");
    }

    @Test
    public void testLeftOuterJoinNoDerivationTowardsPreservedSide() throws Exception {
        // a range on the null-producing side must not filter the preserved side: rows of f
        // without a match must survive with NULL-extended e columns
        String plan = getFragmentPlan("select * from fact_month f left join event_dates e"
                + " on f.id = e.id and f.dmonth = date_trunc('month', e.datadate)"
                + " and e.datadate between '2024-03-05' and '2024-04-10'");
        assertNotContains(plan, "dmonth >= '2024-03-01'");
        assertContains(plan, "partitions=6/6");
    }

    @Test
    public void testLeftSemiJoinDerives() throws Exception {
        String plan = getFragmentPlan("select f.* from fact_month f left semi join event_dates e"
                + " on f.id = e.id and f.dmonth = date_trunc('month', e.datadate)"
                + " and e.datadate between '2024-03-05' and '2024-04-10'");
        assertContains(plan, "dmonth <= '2024-04-01'");
        assertContains(plan, "partitions=2/6");
    }

    @Test
    public void testSessionVariableOff() throws Exception {
        try {
            connectContext.getSessionVariable().setEnableMonotonicPredicateMoveAround(false);
            String plan = getFragmentPlan("select * from fact_month f join event_dates e"
                    + " on f.id = e.id and f.dmonth = date_trunc('month', e.datadate)"
                    + " where e.datadate between '2024-03-05' and '2024-04-10'");
            assertNotContains(plan, "dmonth >= '2024-03-01'");
            assertContains(plan, "partitions=6/6");
        } finally {
            connectContext.getSessionVariable().setEnableMonotonicPredicateMoveAround(true);
        }
    }

    @Test
    public void testRangeOnConjunctDerivesSingleBound() throws Exception {
        // range ON conjuncts are not moved into child projections, so this goes through the
        // in-place LT/LE fallback: dmonth <= months_add(datadate, 1) and the image upper
        // bound months_add('2024-04-10', 1) give dmonth <= '2024-05-10'
        String plan = getFragmentPlan("select * from fact_month f join event_dates e"
                + " on f.id = e.id and f.dmonth <= months_add(e.datadate, 1)"
                + " where e.datadate between '2024-03-05' and '2024-04-10'");
        assertContains(plan, "dmonth <= '2024-05-10'");
        assertContains(plan, "partitions=5/6");
    }

    @Test
    public void testSawtoothCompositionNotDerived() throws Exception {
        // datediff(d, date_trunc('month', d)) is the day of month: each node is monotonic by
        // the registry, but the whole expression is not (the column occurs twice). Nothing is
        // derived.
        String plan = getFragmentPlan("select * from fact_month_int f join event_dates e"
                + " on f.id = e.id and f.datamonth = datediff(e.datadate, date_trunc('month', e.datadate))"
                + " where e.datadate between '2024-03-05' and '2024-04-10'");
        assertNotContains(plan, "datamonth >= ");
        assertContains(plan, "partitions=6/6");
    }

    @Test
    public void testNonMonotonicOffspringNotDerived() throws Exception {
        // the seed side is fine (to_days is monotonic), but the target mod(v, 1000000) does
        // not keep order, and partition further-prune has no monotonicity check of its own.
        // So no predicate is derived for it. The target arrives as a projection slot; the
        // check resolves it through the child projection.
        String plan = getFragmentPlan("select * from fact_month_int f join event_dates e"
                + " on f.id = e.id and mod(f.v, 1000000) = to_days(e.datadate)"
                + " where e.datadate between '2024-03-05' and '2024-04-10'");
        assertNotContains(plan, "mod(3: v, 1000000) >= ");
        assertNotContains(plan, "mod(3: v, 1000000) <= ");
    }

    @Test
    public void testHalfOpenDomainNotDerived() throws Exception {
        // a one-sided range has no second endpoint to fold, and without the direction of
        // monotonicity one folded endpoint cannot be placed. Nothing is derived.
        String plan = getFragmentPlan("select * from fact_month f join event_dates e"
                + " on f.id = e.id and f.dmonth = date_trunc('month', e.datadate)"
                + " where e.datadate >= '2024-03-05'");
        assertNotContains(plan, "dmonth >= '2024-03-01'");
        assertContains(plan, "partitions=6/6");
    }

    @Test
    public void testDerivedBoundNotTighterOmitted() throws Exception {
        // the fact side already has a tighter range than the derived [2024-03-01,
        // 2024-04-01]: the redundancy check drops the derivation, the plan keeps only the
        // user predicates
        String plan = getFragmentPlan("select * from fact_month f join event_dates e"
                + " on f.id = e.id and f.dmonth = date_trunc('month', e.datadate)"
                + " where e.datadate between '2024-03-05' and '2024-04-10'"
                + " and f.dmonth between '2024-03-10' and '2024-03-20'");
        assertContains(plan, "partitions=1/6");
        assertNotContains(plan, "dmonth <= '2024-04-01'");
    }
}
