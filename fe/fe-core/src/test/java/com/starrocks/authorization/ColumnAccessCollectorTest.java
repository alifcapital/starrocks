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

package com.starrocks.authorization;

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end coverage of {@link ColumnPrivilege.ColumnAccessCollector}: parses real SQL through the
 * StarRocks analyzer and rule-based optimizer, swaps in a recording {@link ExternalAccessController}
 * for the default catalog, and asserts on the per-column {@link ColumnAccessKind} sets that the
 * collector hands to {@link Authorizer#checkColumnAction}.
 *
 * The intent of these tests is to lock down PII-audit semantics (PROJECTION = user saw the value;
 * AGG_ARG / JOIN_KEY / FILTER = user touched the column without seeing it). Re-classifying any of
 * the below cases is a wire-format break for downstream audit consumers — update tests consciously.
 */
public class ColumnAccessCollectorTest {

    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;
    private static AccessController originalController;
    private static RecordingController recorder;

    @BeforeAll
    public static void setUpClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("piidb").useDatabase("piidb");
        starRocksAssert.withTable("CREATE TABLE t1 (\n" +
                "  v4 bigint NULL,\n" +
                "  v5 bigint NULL,\n" +
                "  v6 bigint NULL\n" +
                ") DUPLICATE KEY(v4) DISTRIBUTED BY HASH(v4) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
        starRocksAssert.withTable("CREATE TABLE t2 (\n" +
                "  v4 bigint NULL,\n" +
                "  v7 bigint NULL\n" +
                ") DUPLICATE KEY(v4) DISTRIBUTED BY HASH(v4) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
    }

    @BeforeEach
    public void installRecorder() {
        recorder = new RecordingController();
        originalController = Authorizer.getInstance()
                .getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        Authorizer.getInstance()
                .setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, recorder);
    }

    @AfterEach
    public void uninstallRecorder() {
        if (originalController != null) {
            Authorizer.getInstance()
                    .setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, originalController);
        } else {
            Authorizer.getInstance()
                    .removeAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        }
    }

    @AfterAll
    public static void tearDownClass() {
        // ConnectContext is a thread-local set by createDefaultCtx; explicit cleanup keeps later
        // tests in the same JVM from inheriting our recorder state.
        ConnectContext.remove();
    }

    @Test
    public void simpleSelectMakesProjection() {
        Map<String, EnumSet<ColumnAccessKind>> roles = run("SELECT v4 FROM piidb.t1");
        assertEquals(EnumSet.of(ColumnAccessKind.PROJECTION), roles.get("t1.v4"));
    }

    @Test
    public void filterOnlyDoesNotBecomeProjection() {
        // count(*) hides v5; the user only filters on it.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT count(*) FROM piidb.t1 WHERE v5 = 1");
        assertEquals(EnumSet.of(ColumnAccessKind.FILTER), roles.get("t1.v5"));
        assertTrue(roles.getOrDefault("t1.v5", EnumSet.noneOf(ColumnAccessKind.class))
                .stream().noneMatch(k -> k == ColumnAccessKind.PROJECTION));
    }

    @Test
    public void aggregateArgIsAggArgNotProjection() {
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT count(v6) FROM piidb.t1");
        assertEquals(EnumSet.of(ColumnAccessKind.AGG_ARG), roles.get("t1.v6"));
    }

    @Test
    public void groupByKeyNotProjectedDoesNotLeakProjection() {
        // The B5 regression: SELECT count(*) ... GROUP BY pii must NOT mark pii as PROJECTION.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT count(*) FROM piidb.t1 GROUP BY v4");
        EnumSet<ColumnAccessKind> v4 = roles.get("t1.v4");
        assertTrue(v4.contains(ColumnAccessKind.AGG_ARG),
                "GROUP BY key should be classified as AGG_ARG, got " + v4);
        Assertions.assertFalse(v4.contains(ColumnAccessKind.PROJECTION),
                "GROUP BY key not in SELECT-list must NOT be PROJECTION, got " + v4);
    }

    @Test
    public void groupByKeyAlsoProjectedGetsBoth() {
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT v4, count(*) FROM piidb.t1 GROUP BY v4");
        EnumSet<ColumnAccessKind> v4 = roles.get("t1.v4");
        assertTrue(v4.contains(ColumnAccessKind.PROJECTION));
        assertTrue(v4.contains(ColumnAccessKind.AGG_ARG));
    }

    @Test
    public void joinKeyIsClassifiedJoinKeyOnBothSides() {
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT t1.v5 FROM piidb.t1 JOIN piidb.t2 ON t1.v4 = t2.v4");
        assertTrue(roles.get("t1.v4").contains(ColumnAccessKind.JOIN_KEY));
        assertTrue(roles.get("t2.v4").contains(ColumnAccessKind.JOIN_KEY));
        assertEquals(EnumSet.of(ColumnAccessKind.PROJECTION), roles.get("t1.v5"));
    }

    @Test
    public void crossJoinWithWhereBecomesJoinKey() {
        // The optimizer rewrites this to an inner join — by design, classification follows the plan.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT t1.v5 FROM piidb.t1, piidb.t2 WHERE t1.v4 = t2.v4");
        assertTrue(roles.get("t1.v4").contains(ColumnAccessKind.JOIN_KEY));
        assertTrue(roles.get("t2.v4").contains(ColumnAccessKind.JOIN_KEY));
    }

    @Test
    public void selectAndFilterOnSameColumnGetsBoth() {
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT v4 FROM piidb.t1 WHERE v4 = 1");
        EnumSet<ColumnAccessKind> v4 = roles.get("t1.v4");
        assertTrue(v4.contains(ColumnAccessKind.PROJECTION));
        assertTrue(v4.contains(ColumnAccessKind.FILTER));
    }

    @Test
    public void havingClassifiedAsFilter() {
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT count(v6) FROM piidb.t1 GROUP BY v4 HAVING count(v6) > 1");
        // v6 is an aggregate arg AND it's referenced inside HAVING via the aggregate output;
        // we resolve the agg output back to its args so HAVING contributes FILTER.
        EnumSet<ColumnAccessKind> v6 = roles.get("t1.v6");
        assertTrue(v6.contains(ColumnAccessKind.AGG_ARG));
        assertTrue(v6.contains(ColumnAccessKind.FILTER), "HAVING max(v6)>1 should mark v6 as FILTER, got " + v6);
        // v4 is the GROUP BY key, not in SELECT-list
        assertTrue(roles.get("t1.v4").contains(ColumnAccessKind.AGG_ARG));
        Assertions.assertFalse(roles.get("t1.v4").contains(ColumnAccessKind.PROJECTION));
    }

    @Test
    public void havingOnGroupingKeyClassifiedAsFilter() {
        // HAVING referencing a grouping-key directly (not via aggregate). Codex case "direct
        // ref" branch in applyHavingPredicate.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT v4, count(*) FROM piidb.t1 GROUP BY v4 HAVING v4 > 1");
        EnumSet<ColumnAccessKind> v4 = roles.get("t1.v4");
        assertTrue(v4.contains(ColumnAccessKind.FILTER), "HAVING v4>1 should mark v4 as FILTER, got " + v4);
        assertTrue(v4.contains(ColumnAccessKind.PROJECTION));
    }

    @Test
    public void scalarSubqueryPassthroughGetsProjection() {
        // Codex regression: SELECT (SELECT v7 FROM t2 WHERE ...) FROM t1 — user sees v7's raw
        // value via the scalar subquery. If the optimizer leaves Apply in the plan, Apply.output
        // must resolve to v7's bases so markRootOutputProjection() flags it as PROJECTION.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT v4, (SELECT t2.v7 FROM piidb.t2 WHERE t2.v4 = t1.v4) FROM piidb.t1");
        assertTrue(roles.get("t1.v4").contains(ColumnAccessKind.PROJECTION));
        EnumSet<ColumnAccessKind> v7 = roles.get("t2.v7");
        assertTrue(v7 != null && v7.contains(ColumnAccessKind.PROJECTION),
                "passthrough scalar subquery output must classify v7 as PROJECTION, got " + v7);
    }

    @Test
    public void aliasFromSubqueryResolvesToBase() {
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT a FROM (SELECT v4 AS a, v5 FROM piidb.t1) sub WHERE v5 = 1");
        assertTrue(roles.get("t1.v4").contains(ColumnAccessKind.PROJECTION));
        assertTrue(roles.get("t1.v5").contains(ColumnAccessKind.FILTER));
    }

    @Test
    public void cteResolvesAliasesAcrossConsume() {
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("WITH cte AS (SELECT v4, v5 FROM piidb.t1) SELECT v4 FROM cte WHERE v5 = 1");
        assertTrue(roles.get("t1.v4").contains(ColumnAccessKind.PROJECTION));
        assertTrue(roles.get("t1.v5").contains(ColumnAccessKind.FILTER));
    }

    @Test
    public void exceptRightSideClassifiedAsFilterNotProjection() {
        // Codex regression: SELECT v4 FROM t1 EXCEPT SELECT v4 FROM t2 — the user only sees
        // rows from t1; t2.v4 is used for set-membership and should NOT be PROJECTION.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT v4 FROM piidb.t1 EXCEPT SELECT v4 FROM piidb.t2");
        EnumSet<ColumnAccessKind> t1v4 = roles.get("t1.v4");
        EnumSet<ColumnAccessKind> t2v4 = roles.get("t2.v4");
        assertTrue(t1v4.contains(ColumnAccessKind.PROJECTION),
                "EXCEPT left side: t1.v4 must be PROJECTION, got " + t1v4);
        Assertions.assertFalse(t2v4.contains(ColumnAccessKind.PROJECTION),
                "EXCEPT right side: t2.v4 must NOT be PROJECTION, got " + t2v4);
        assertTrue(t2v4.contains(ColumnAccessKind.FILTER),
                "EXCEPT right side: t2.v4 used for membership — should be FILTER, got " + t2v4);
    }

    @Test
    public void unionPropagatesFilterToBothBranches() {
        Map<String, EnumSet<ColumnAccessKind>> roles = run(
                "SELECT * FROM ((SELECT v4 FROM piidb.t1) UNION ALL (SELECT v4 FROM piidb.t2)) u WHERE v4 > 0");
        assertTrue(roles.get("t1.v4").contains(ColumnAccessKind.FILTER));
        assertTrue(roles.get("t2.v4").contains(ColumnAccessKind.FILTER));
    }

    @Test
    public void orderByClassifiedAsFilter() {
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT v5 FROM piidb.t1 ORDER BY v4 LIMIT 10");
        assertTrue(roles.get("t1.v4").contains(ColumnAccessKind.FILTER));
        Assertions.assertFalse(roles.get("t1.v4").contains(ColumnAccessKind.PROJECTION));
    }

    @Test
    public void valueReturningAggregateGetsProjection() {
        // array_agg(v6) literally lists every v6 value in the result — user sees raw pii.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT array_agg(v6) FROM piidb.t1");
        assertTrue(roles.get("t1.v6").contains(ColumnAccessKind.PROJECTION),
                "array_agg(v6) must mark v6 as PROJECTION, got " + roles.get("t1.v6"));
        assertTrue(roles.get("t1.v6").contains(ColumnAccessKind.AGG_ARG));
    }

    @Test
    public void groupConcatGetsProjection() {
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT group_concat(v6) FROM piidb.t1");
        assertTrue(roles.get("t1.v6").contains(ColumnAccessKind.PROJECTION),
                "group_concat(v6) must mark v6 as PROJECTION, got " + roles.get("t1.v6"));
    }

    @Test
    public void groupingFunctionDoesNotLeakProjection() {
        // SELECT GROUPING(v4), count(*) FROM t1 GROUP BY ROLLUP(v4) — user sees the 0/1 indicator,
        // NOT v4 itself. v4 should be AGG_ARG (it's a grouping key) but NOT PROJECTION.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT GROUPING(v4), count(*) FROM piidb.t1 GROUP BY ROLLUP(v4)");
        EnumSet<ColumnAccessKind> v4 = roles.get("t1.v4");
        assertTrue(v4.contains(ColumnAccessKind.AGG_ARG),
                "GROUP BY ROLLUP(v4): v4 should be AGG_ARG, got " + v4);
        Assertions.assertFalse(v4.contains(ColumnAccessKind.PROJECTION),
                "GROUPING(v4) returns 0/1 indicator — v4 must NOT be PROJECTION, got " + v4);
    }

    @Test
    public void groupConcatMultipleValueArgsAllProjection() {
        // group_concat(v6, v5) — separator is v5, BOTH v6 and v5 appear in the result string.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT group_concat(v6, v5) FROM piidb.t1");
        assertTrue(roles.get("t1.v6").contains(ColumnAccessKind.PROJECTION),
                "group_concat(v6, v5): v6 must be PROJECTION, got " + roles.get("t1.v6"));
        assertTrue(roles.get("t1.v5").contains(ColumnAccessKind.PROJECTION),
                "group_concat(v6, v5): v5 (separator value) must be PROJECTION, got " + roles.get("t1.v5"));
    }

    @Test
    public void approxTopKGetsProjection() {
        // approx_top_k(v6, 5) returns ARRAY<STRUCT<item, count>> where item is raw v6 value.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT approx_top_k(v6, 5) FROM piidb.t1");
        assertTrue(roles.get("t1.v6").contains(ColumnAccessKind.PROJECTION),
                "approx_top_k(v6) returns raw v6 in struct items — must be PROJECTION, got " + roles.get("t1.v6"));
    }

    @Test
    public void histogramGetsProjection() {
        // histogram returns bucket boundaries derived from input values.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT histogram(v6, 10, 0.1) FROM piidb.t1");
        assertTrue(roles.get("t1.v6").contains(ColumnAccessKind.PROJECTION),
                "histogram(v6) bucket bounds expose v6 — must be PROJECTION, got " + roles.get("t1.v6"));
    }

    @Test
    public void anyValueGetsProjection() {
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT any_value(v6) FROM piidb.t1");
        assertTrue(roles.get("t1.v6").contains(ColumnAccessKind.PROJECTION),
                "any_value(v6) returns one raw v6 — must be PROJECTION, got " + roles.get("t1.v6"));
    }

    @Test
    public void arrayAggOrderByDoesNotMarkSortKeyAsProjection() {
        // SELECT array_agg(v6 ORDER BY v5) — v5 is a sort key, not visible to the user.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT array_agg(v6 ORDER BY v5) FROM piidb.t1");
        assertTrue(roles.get("t1.v6").contains(ColumnAccessKind.PROJECTION),
                "array_agg(v6 ORDER BY v5): v6 must be PROJECTION, got " + roles.get("t1.v6"));
        EnumSet<ColumnAccessKind> v5 = roles.get("t1.v5");
        Assertions.assertFalse(v5.contains(ColumnAccessKind.PROJECTION),
                "array_agg(v6 ORDER BY v5): v5 (sort key) must NOT be PROJECTION, got " + v5);
    }

    @Test
    public void percentileDiscGetsProjection() {
        // percentile_disc returns one of the input column values verbatim.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT percentile_disc(v6, 0.5) FROM piidb.t1");
        assertTrue(roles.get("t1.v6").contains(ColumnAccessKind.PROJECTION),
                "percentile_disc(v6, 0.5): v6 must be PROJECTION, got " + roles.get("t1.v6"));
    }

    @Test
    public void minByOnlyValueArgGetsProjection() {
        // min_by(v6, v5): user sees v6, sort key v5 is hidden.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT min_by(v6, v5) FROM piidb.t1");
        assertTrue(roles.get("t1.v6").contains(ColumnAccessKind.PROJECTION),
                "min_by(v6, v5): v6 must be PROJECTION, got " + roles.get("t1.v6"));
        EnumSet<ColumnAccessKind> v5 = roles.get("t1.v5");
        Assertions.assertFalse(v5.contains(ColumnAccessKind.PROJECTION),
                "min_by(v6, v5): v5 (sort) must NOT be PROJECTION, got " + v5);
        assertTrue(v5.contains(ColumnAccessKind.AGG_ARG),
                "min_by(v6, v5): v5 still passes through aggregate args, AGG_ARG, got " + v5);
    }

    @Test
    public void minMaxGetsProjection() {
        // min/max return one concrete value of the column — user sees a real pii value.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT min(v6), max(v5) FROM piidb.t1");
        assertTrue(roles.get("t1.v6").contains(ColumnAccessKind.PROJECTION),
                "min(v6) must mark v6 as PROJECTION, got " + roles.get("t1.v6"));
        assertTrue(roles.get("t1.v5").contains(ColumnAccessKind.PROJECTION),
                "max(v5) must mark v5 as PROJECTION, got " + roles.get("t1.v5"));
    }

    @Test
    public void aggregateOutputDoesNotLeakProjectionToArgs() {
        // Codex regression: previously `SELECT count(v6) FROM t1` was wiring the aggregate's
        // output ref to v6's bases; markRootOutputProjection() then marked v6 as PROJECTION.
        // Re-asserts the same case as aggregateArgIsAggArgNotProjection from a different angle —
        // also covers sum/avg to make sure we didn't only fix count.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT sum(v6), avg(v5) FROM piidb.t1");
        assertEquals(EnumSet.of(ColumnAccessKind.AGG_ARG), roles.get("t1.v6"));
        assertEquals(EnumSet.of(ColumnAccessKind.AGG_ARG), roles.get("t1.v5"));
    }

    @Test
    public void scalarSubqueryDoesNotLeakProjectionToInnerColumns() {
        // SELECT (SELECT count(*) FROM t2) FROM t1 — user sees a scalar, not t2 columns.
        // If LogicalApplyOperator survives decorrelation we still must not mark t2 columns as
        // PROJECTION.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT v4, (SELECT count(v7) FROM piidb.t2) FROM piidb.t1");
        assertTrue(roles.get("t1.v4").contains(ColumnAccessKind.PROJECTION));
        EnumSet<ColumnAccessKind> v7 = roles.getOrDefault("t2.v7", EnumSet.noneOf(ColumnAccessKind.class));
        Assertions.assertFalse(v7.contains(ColumnAccessKind.PROJECTION),
                "v7 lives only inside an aggregate inside a subquery — must NOT be PROJECTION, got " + v7);
    }

    @Test
    public void aggregatingWindowArgsAreAggArgNotProjection() {
        // sum() OVER returns a derived scalar — user does NOT see raw v6 values.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT sum(v6) OVER (PARTITION BY v4 ORDER BY v5) FROM piidb.t1");
        assertTrue(roles.get("t1.v6").contains(ColumnAccessKind.AGG_ARG));
        Assertions.assertFalse(roles.get("t1.v6").contains(ColumnAccessKind.PROJECTION),
                "aggregating window arg must not be PROJECTION");
        assertTrue(roles.get("t1.v4").contains(ColumnAccessKind.FILTER), "PARTITION BY → FILTER");
        assertTrue(roles.get("t1.v5").contains(ColumnAccessKind.FILTER), "ORDER BY → FILTER");
    }

    @Test
    public void valueReturningWindowArgsArePROJECTION() {
        // Codex regression: first_value/last_value/lead/lag/nth_value expose RAW arg values to
        // the user through the window output. PII must not be under-reported in audit.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT first_value(v6) OVER (PARTITION BY v4 ORDER BY v5) FROM piidb.t1");
        EnumSet<ColumnAccessKind> v6 = roles.get("t1.v6");
        assertTrue(v6.contains(ColumnAccessKind.PROJECTION),
                "first_value(v6) makes v6 visible to the user — must be PROJECTION, got " + v6);
        // It's also still an aggregate-shaped argument; AGG_ARG can coexist.
    }

    @Test
    public void inSubqueryInSelectListClassifiesBothColumns() {
        // SELECT v4 IN (SELECT v7 FROM t2) FROM t1.
        // Outer column (t1.v4) is what the user typed in SELECT-list; the boolean result depends
        // on its value, so for PII auditing we want PROJECTION on it — user "sees" it in the
        // sense that the output row leaks information about v4.
        // Inner column (t2.v7) only needs to be classified as something (the optimizer's chosen
        // rewrite path determines the exact role: JOIN_KEY post-Apply→SEMI-JOIN, FILTER if
        // Apply survives, sometimes also PROJECTION through Project rewrites). All are
        // acceptable as long as v7 is not silently dropped from audit.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT v4 IN (SELECT v7 FROM piidb.t2) FROM piidb.t1");
        EnumSet<ColumnAccessKind> v4 = roles.getOrDefault("t1.v4", EnumSet.noneOf(ColumnAccessKind.class));
        assertTrue(v4.contains(ColumnAccessKind.PROJECTION),
                "outer t1.v4 in `v4 IN (...)` SELECT-list expression must carry PROJECTION, got " + v4);
        EnumSet<ColumnAccessKind> v7 = roles.getOrDefault("t2.v7", EnumSet.noneOf(ColumnAccessKind.class));
        Assertions.assertFalse(v7.isEmpty(),
                "inner t2.v7 must be classified (any non-empty role set), got empty");
    }

    @Test
    public void rankingTopNPartitionByClassifiedAsFilter() {
        // SELECT v4, rn FROM (SELECT v4, ROW_NUMBER() OVER (PARTITION BY v4 ORDER BY v5) rn FROM t1)
        //   WHERE rn <= 5
        // is rewritten by RankingWindowFilterRule into LogicalTopNOperator with
        // partitionByColumns=[v4], orderByElements=[v5]. Both should be FILTER.
        Map<String, EnumSet<ColumnAccessKind>> roles = run(
                "SELECT v4 FROM (SELECT v4, ROW_NUMBER() OVER (PARTITION BY v4 ORDER BY v5) rn "
                        + "FROM piidb.t1) sub WHERE rn <= 5");
        assertTrue(roles.get("t1.v4").contains(ColumnAccessKind.PROJECTION));
        // v5 only drives ordering — never visible to user
        EnumSet<ColumnAccessKind> v5 = roles.get("t1.v5");
        assertTrue(v5.contains(ColumnAccessKind.FILTER),
                "ORDER BY v5 inside ranking window must mark v5 as FILTER, got " + v5);
        Assertions.assertFalse(v5.contains(ColumnAccessKind.PROJECTION),
                "v5 not in SELECT-list — must NOT be PROJECTION, got " + v5);
    }

    @Test
    public void correlatedScalarSubqueryWithMaxClassifiesAllColumns() {
        // SELECT v5, (SELECT max(t2.v7) FROM t2 WHERE t2.v4 = t1.v4) FROM t1.
        // What the user sees:
        //   - t1.v5 directly        → PROJECTION
        //   - the max-value of t2.v7 → user sees one verbatim v7 value (max is a value-returning
        //     aggregate per our whitelist), so t2.v7 must be PROJECTION + AGG_ARG.
        // The correlation column on the outer side (t1.v4) participates in the correlation
        // predicate and may surface as FILTER (if Apply survives) or JOIN_KEY (if decorrelated
        // into a join); both are valid non-PROJECTION audit signals.
        Map<String, EnumSet<ColumnAccessKind>> roles = run(
                "SELECT v5, (SELECT max(t2.v7) FROM piidb.t2 WHERE t2.v4 = t1.v4) "
                        + "FROM piidb.t1");

        EnumSet<ColumnAccessKind> t1v5 = roles.getOrDefault("t1.v5", EnumSet.noneOf(ColumnAccessKind.class));
        assertTrue(t1v5.contains(ColumnAccessKind.PROJECTION),
                "t1.v5 in SELECT-list must be PROJECTION, got " + t1v5);

        EnumSet<ColumnAccessKind> t1v4 = roles.getOrDefault("t1.v4", EnumSet.noneOf(ColumnAccessKind.class));
        assertTrue(t1v4.contains(ColumnAccessKind.FILTER) || t1v4.contains(ColumnAccessKind.JOIN_KEY),
                "outer correlation column t1.v4 should carry FILTER or JOIN_KEY, got " + t1v4);

        EnumSet<ColumnAccessKind> t2v7 = roles.getOrDefault("t2.v7", EnumSet.noneOf(ColumnAccessKind.class));
        assertTrue(t2v7.contains(ColumnAccessKind.AGG_ARG),
                "max(t2.v7) — v7 is an aggregate arg, must be AGG_ARG, got " + t2v7);
        assertTrue(t2v7.contains(ColumnAccessKind.PROJECTION),
                "max is a value-returning aggregate — user sees a real v7 value, must be PROJECTION, got " + t2v7);
    }

    @Test
    public void leadAndLagAreProjection() {
        // lead/lag are also value-returning — same rule.
        Map<String, EnumSet<ColumnAccessKind>> roles =
                run("SELECT lead(v6) OVER (PARTITION BY v4 ORDER BY v5) FROM piidb.t1");
        assertTrue(roles.get("t1.v6").contains(ColumnAccessKind.PROJECTION),
                "lead(v6) must be PROJECTION");
    }

    /**
     * Run a query through analysis and ColumnPrivilege.check, then return a map of
     * "{table}.{column}" → roles, sorted for deterministic assertion errors.
     */
    private Map<String, EnumSet<ColumnAccessKind>> run(String sql) {
        try {
            QueryStatement stmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            ColumnPrivilege.check(ctx, stmt, Collections.emptyList());
            return new TreeMap<>(recorder.recorded);
        } catch (Exception e) {
            throw new RuntimeException("Failed to run query: " + sql, e);
        }
    }

    /**
     * Records every {@link #checkColumnAction(ConnectContext, TableName, String, PrivilegeType, EnumSet)}
     * call into a "{table}.{column}" → EnumSet map. All other privilege checks no-op so the rest of
     * the privilege pipeline doesn't fail the test.
     */
    private static final class RecordingController extends ExternalAccessController {
        final Map<String, EnumSet<ColumnAccessKind>> recorded = new TreeMap<>();

        @Override
        public void checkColumnAction(ConnectContext context, TableName tableName,
                                      String column, PrivilegeType privilegeType) {
            // legacy overload — required to satisfy the AccessController contract chain
        }

        @Override
        public void checkColumnAction(ConnectContext context, TableName tableName,
                                      String column, PrivilegeType privilegeType,
                                      EnumSet<ColumnAccessKind> usage) {
            String key = tableName.getTbl() + "." + column;
            recorded.computeIfAbsent(key, k -> EnumSet.noneOf(ColumnAccessKind.class)).addAll(usage);
        }

        @Override
        public void checkTableAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType) {
        }

        @Override
        public void checkAnyActionOnTable(ConnectContext context, TableName tableName) {
        }
    }
}
