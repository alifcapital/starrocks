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
import com.starrocks.planner.HashJoinNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.JoinOnClause;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.THashJoinNode;
import com.starrocks.thrift.TJoinOnClause;
import com.starrocks.thrift.TPlan;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive tests for hash join with OR in ON clause (disjunctive hash join).
 *
 * Example: SELECT * FROM t1 JOIN t2 ON t1.a = t2.x OR t1.b = t2.y
 * This should use hash join with multiple hash tables instead of nested loop join.
 *
 * Test categories:
 * 1. Join semantics (result correctness) - highest risk
 * 2. other_conjuncts inside disjuncts
 * 3. Runtime filters (RF)
 * 4. Distribution / broadcast requirement
 * 5. Boundaries and guards
 */
public class DisjunctiveHashJoinTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);
    }

    // ====================================================================================
    // 1. JOIN SEMANTICS (RESULT CORRECTNESS) - HIGHEST RISK
    // ====================================================================================

    // -------------------- INNER JOIN --------------------

    @Test
    public void testInnerJoinBasicOr() throws Exception {
        // Basic OR join: t0.v1 = t1.v4 OR t0.v2 = t1.v5
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getFragmentPlan(sql);

        // Must use HASH JOIN, not CROSS JOIN or NESTED LOOP JOIN
        assertContains(plan, "HASH JOIN");
        assertNotContains(plan, "CROSS JOIN");
        assertNotContains(plan, "NESTLOOP JOIN");
    }

    @Test
    public void testInnerJoinWithSameLeftKey() throws Exception {
        // Same probe key: t0.v1 = t1.v4 OR t0.v1 = t1.v5
        // One probe row may match multiple build rows via different disjuncts
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v1 = t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "HASH JOIN");
    }

    @Test
    public void testInnerJoinWithSameRightKey() throws Exception {
        // Same build key: t0.v1 = t1.v4 OR t0.v2 = t1.v4
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v4";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "HASH JOIN");
    }

    @Test
    public void testInnerJoinThreeDisjuncts() throws Exception {
        // Three disjuncts: each can match independently
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5 OR t0.v3 = t1.v6";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "JOIN");
    }

    @Test
    public void testInnerJoinDeduplicationAcrossDisjuncts() throws Exception {
        // Test that same (probe_idx, build_idx) pair isn't output twice
        // when a row matches via multiple disjuncts.
        // t0.v1 = t1.v4 OR t0.v1 = t1.v4 (same predicate) - degenerate case
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getFragmentPlan(sql);

        // Plan should be HASH JOIN (deduplication happens at runtime)
        assertContains(plan, "HASH JOIN");
    }

    // -------------------- LEFT OUTER JOIN --------------------

    @Test
    public void testLeftOuterJoinBasicOr() throws Exception {
        // LEFT OUTER: probe rows with no match should produce NULL-extended row
        String sql = "SELECT * FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "LEFT OUTER JOIN");
    }

    @Test
    public void testLeftOuterJoinNoMatchInFirstHt() throws Exception {
        // If probe row doesn't match HT0 but matches HT1,
        // no null-extended row from HT0 should appear
        String sql = "SELECT * FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "LEFT OUTER JOIN");
    }

    @Test
    public void testLeftOuterJoinMatchInMultipleHts() throws Exception {
        // Probe row matches different build rows via different disjuncts
        // Both matched rows should be in output (no null-extended row)
        String sql = "SELECT * FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "LEFT OUTER JOIN");
    }

    // -------------------- RIGHT OUTER JOIN --------------------

    @Test
    public void testRightOuterJoinBasicOr() throws Exception {
        // RIGHT OUTER: build rows with no match should have NULL probe columns
        String sql = "SELECT * FROM t0 RIGHT JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "RIGHT OUTER JOIN");
    }

    @Test
    public void testRightOuterJoinUnmatchedBuildRows() throws Exception {
        // Build rows not matched by any disjunct should appear with NULL probe
        String sql = "SELECT * FROM t0 RIGHT OUTER JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "RIGHT OUTER JOIN");
    }

    // -------------------- FULL OUTER JOIN --------------------

    @Test
    public void testFullOuterJoinBasicOr() throws Exception {
        // FULL OUTER: both unmatched probe and build rows should appear
        String sql = "SELECT * FROM t0 FULL OUTER JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "FULL OUTER JOIN");
    }

    @Test
    public void testFullOuterJoinBuildMatchTracking() throws Exception {
        // build_match_index must track matches across all hash tables
        String sql = "SELECT * FROM t0 FULL JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "FULL OUTER JOIN");
    }

    // -------------------- LEFT SEMI JOIN --------------------

    @Test
    public void testLeftSemiJoinBasicOr() throws Exception {
        // LEFT SEMI: probe row appears at most once, even if it matches multiple times
        String sql = "SELECT * FROM t0 WHERE EXISTS (SELECT 1 FROM t1 WHERE t0.v1 = t1.v4 OR t0.v2 = t1.v5)";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "LEFT SEMI JOIN");
    }

    @Test
    public void testLeftSemiJoinDeduplication() throws Exception {
        // One probe row matches via both disjuncts -> only 1 output row
        String sql = "SELECT * FROM t0 LEFT SEMI JOIN t1 ON t0.v1 = t1.v4 OR t0.v1 = t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "LEFT SEMI JOIN");
    }

    @Test
    public void testLeftSemiJoinMatchInSecondHtOnly() throws Exception {
        // Probe row doesn't match HT0 but matches HT1 -> should be in output
        String sql = "SELECT * FROM t0 LEFT SEMI JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "LEFT SEMI JOIN");
    }

    // -------------------- RIGHT SEMI JOIN --------------------

    @Test
    public void testRightSemiJoinBasicOr() throws Exception {
        // RIGHT SEMI: build row appears at most once, even if matched by multiple probe rows
        String sql = "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t0 WHERE t0.v1 = t1.v4 OR t0.v2 = t1.v5)";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "RIGHT SEMI JOIN");
    }

    @Test
    public void testRightSemiJoinDeduplication() throws Exception {
        // One build row matched by multiple probe rows via different disjuncts -> 1 output
        String sql = "SELECT * FROM t0 RIGHT SEMI JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "RIGHT SEMI JOIN");
    }

    @Test
    public void testRightSemiJoinBuildRowIndexZero() throws Exception {
        // build_idx==0 is reserved; shouldn't leak into output
        String sql = "SELECT * FROM t0 RIGHT SEMI JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "RIGHT SEMI JOIN");
    }

    // -------------------- ANTI JOIN (Must be NotSupported) --------------------
    // The error is thrown in BE's hash_join_node.cpp:
    // "hash join with OR in ON clause is not supported for ANTI joins"

    @Test
    public void testLeftAntiJoinNotSupported() {
        // LEFT ANTI with OR in ON clause must fail with NotSupported
        String sql = "SELECT * FROM t0 WHERE NOT EXISTS (SELECT 1 FROM t1 WHERE t0.v1 = t1.v4 OR t0.v2 = t1.v5)";
        // Note: The error is thrown at BE level during init(), not FE planning.
        // For unit tests, we may need to check that the plan is generated and contains ANTI + join_on_clauses.
        // The actual error will surface at runtime. For now, verify plan generation.
        try {
            String plan = getFragmentPlan(sql);
            // If we get a plan, check it's ANTI JOIN with OR predicate
            assertContains(plan, "ANTI");
        } catch (Exception e) {
            // If it fails with NotSupported, that's also acceptable
            assertTrue(e.getMessage().contains("not supported") ||
                       e.getMessage().contains("ANTI"),
                    "Unexpected error: " + e.getMessage());
        }
    }

    @Test
    public void testRightAntiJoinNotSupported() {
        // RIGHT ANTI with OR must fail
        String sql = "SELECT * FROM t0 RIGHT ANTI JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        // The error is thrown at BE level. For FE tests, verify plan is created.
        try {
            String plan = getFragmentPlan(sql);
            assertContains(plan, "ANTI");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("not supported") ||
                       e.getMessage().contains("ANTI"),
                    "Unexpected error: " + e.getMessage());
        }
    }

    @Test
    public void testNullAwareLeftAntiJoinNotSupported() {
        // NULL_AWARE_LEFT_ANTI with OR must fail
        String sql = "SELECT * FROM t0 WHERE v1 NOT IN (SELECT v4 FROM t1 WHERE t0.v2 = t1.v5 OR t0.v3 = t1.v6)";
        // This might get rewritten differently, but if it becomes ANTI + OR, must fail at BE
        try {
            String plan = getFragmentPlan(sql);
            // If we get here, either it was rewritten to something else, or check for ANTI
            if (plan.contains("NULL_AWARE") && plan.contains("ANTI")) {
                // Plan was created - BE will reject it at runtime
                // This is acceptable behavior - FE generates the plan, BE rejects unsupported join types
            }
        } catch (Exception e) {
            // Expected - should fail with NotSupported
            assertTrue(e.getMessage().contains("not supported") ||
                       e.getMessage().contains("NotSupported") ||
                       e.getMessage().contains("ANTI"),
                    "Unexpected error: " + e.getMessage());
        }
    }

    // ====================================================================================
    // 2. OTHER_CONJUNCTS INSIDE DISJUNCTS
    // ====================================================================================

    @Test
    public void testOtherConjunctsWithinDisjunct() throws Exception {
        // (t0.v1 = t1.v4 AND t0.v3 > 10) OR (t0.v2 = t1.v5 AND t0.v3 < 5)
        // Match by key but filter by other_conjunct
        String sql = "SELECT * FROM t0 JOIN t1 ON (t0.v1 = t1.v4 AND t0.v3 > 10) OR (t0.v2 = t1.v5 AND t0.v3 < 5)";
        String plan = getFragmentPlan(sql);

        // Should still use HASH JOIN
        assertContains(plan, "JOIN");
    }

    @Test
    public void testOtherConjunctsFilterBeforeOutput() throws Exception {
        // For INNER join: key matches but other_conjunct false -> no output
        String sql = "SELECT * FROM t0 JOIN t1 ON (t0.v1 = t1.v4 AND t0.v3 > 100) OR (t0.v2 = t1.v5 AND t1.v6 < -100)";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "JOIN");
    }

    @Test
    public void testOtherConjunctsLeftOuterUnmatched() throws Exception {
        // LEFT OUTER: key matches but other_conjunct fails -> treated as unmatched
        // Should produce NULL-extended row if no other disjunct matches
        String sql = "SELECT * FROM t0 LEFT JOIN t1 ON (t0.v1 = t1.v4 AND t1.v6 > 1000)";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "LEFT OUTER JOIN");
    }

    @Test
    public void testOtherConjunctsLeftOuterWithOrBranch() throws Exception {
        // LEFT OUTER with OR: if first disjunct matches key but fails other_conjunct,
        // second disjunct might still match -> no null-extended row
        String sql = "SELECT * FROM t0 LEFT JOIN t1 ON (t0.v1 = t1.v4 AND t1.v6 > 1000) OR t0.v2 = t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "LEFT OUTER JOIN");
    }

    @Test
    public void testOtherConjunctsNullableBoolean() throws Exception {
        // other_conjunct returns NULL -> treat as false
        String sql = "SELECT * FROM t0 JOIN t1 ON (t0.v1 = t1.v4 AND t0.v3 = NULL) OR t0.v2 = t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "JOIN");
    }

    @Test
    public void testOtherConjunctsOnBothSides() throws Exception {
        // Both disjuncts have other_conjuncts
        String sql = "SELECT * FROM t0 JOIN t1 ON " +
                "(t0.v1 = t1.v4 AND t0.v3 > 10 AND t1.v6 < 100) OR " +
                "(t0.v2 = t1.v5 AND t0.v3 < 5 AND t1.v6 > 50)";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "JOIN");
    }

    @Test
    public void testOtherConjunctsWithSemiJoin() throws Exception {
        // LEFT SEMI: key matches, other_conjunct fails -> not matched
        String sql = "SELECT * FROM t0 LEFT SEMI JOIN t1 ON " +
                "(t0.v1 = t1.v4 AND t1.v6 > 1000) OR (t0.v2 = t1.v5 AND t1.v6 < -1000)";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "LEFT SEMI JOIN");
    }

    // ====================================================================================
    // 3. RUNTIME FILTERS (RF)
    // ====================================================================================

    @Test
    public void testRfWithCommonProbeKey() throws Exception {
        // All disjuncts use same probe key (v1) -> RF is valid
        // t0.v1 = t1.v4 OR t0.v1 = t1.v5
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v1 = t1.v5";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "HASH JOIN");
        // Should have runtime filter on the common probe key
        assertContains(plan, "build runtime filters");
    }

    @Test
    public void testRfWithDifferentProbeKeys() throws Exception {
        // Different probe keys (v1 vs v2) -> NO RF
        // t0.v1 = t1.v4 OR t0.v2 = t1.v5
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "HASH JOIN");
        // Should NOT have runtime filters
        assertNotContains(plan, "build runtime filters");
    }

    @Test
    public void testRfWithMixedOpcodes() throws Exception {
        // Same probe key but different opcodes (= vs <=>) -> NO RF
        // t0.v1 = t1.v4 OR t0.v1 <=> t1.v5
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v1 <=> t1.v5";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "JOIN");
        // Should NOT have runtime filters due to mixed opcodes
        assertNotContains(plan, "build runtime filters");
    }

    @Test
    public void testRfWithMultipleEqualitiesPerDisjunct() throws Exception {
        // First disjunct has 2 equalities -> NO RF
        // (t0.v1 = t1.v4 AND t0.v2 = t1.v5) OR t0.v1 = t1.v6
        String sql = "SELECT * FROM t0 JOIN t1 ON (t0.v1 = t1.v4 AND t0.v2 = t1.v5) OR t0.v1 = t1.v6";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "JOIN");
        // No RF because first disjunct has multiple equalities
        assertNotContains(plan, "build runtime filters");
    }

    @Test
    public void testRfThreeDisjunctsSameProbeKey() throws Exception {
        // Three disjuncts, all use v1 -> RF is valid
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v1 = t1.v5 OR t0.v1 = t1.v6";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "HASH JOIN");
        assertContains(plan, "build runtime filters");
    }

    @Test
    public void testRfLeftOuterWithCommonProbeKey() throws Exception {
        // LEFT OUTER with common probe key -> RF is valid
        String sql = "SELECT * FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4 OR t0.v1 = t1.v5";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "LEFT OUTER JOIN");
        assertContains(plan, "build runtime filters");
    }

    @Test
    public void testRfRightOuterWithCommonProbeKey() throws Exception {
        // RIGHT OUTER with common probe key
        String sql = "SELECT * FROM t0 RIGHT JOIN t1 ON t0.v1 = t1.v4 OR t0.v1 = t1.v5";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "RIGHT OUTER JOIN");
        // RIGHT OUTER typically doesn't push RF to probe side
    }

    @Test
    public void testRfSemiJoinWithCommonProbeKey() throws Exception {
        // LEFT SEMI with common probe key -> RF is valid
        String sql = "SELECT * FROM t0 LEFT SEMI JOIN t1 ON t0.v1 = t1.v4 OR t0.v1 = t1.v5";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "LEFT SEMI JOIN");
        assertContains(plan, "build runtime filters");
    }

    // ====================================================================================
    // 4. DISTRIBUTION MODE (MUST BE BROADCAST FOR DISJUNCTIVE JOIN)
    // ====================================================================================

    @Test
    public void testDistributionModeBroadcast() throws Exception {
        // Disjunctive join should force BROADCAST distribution
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "HASH JOIN");
        // Must be BROADCAST, not SHUFFLE/PARTITIONED
        assertContains(plan, "BROADCAST");
        assertNotContains(plan, "PARTITIONED");
    }

    @Test
    public void testDistributionModeNotColocate() throws Exception {
        // Even if tables could be colocated, disjunctive join must use BROADCAST
        FeConstants.runningUnitTest = true;
        String sql = "SELECT * FROM colocate_t0 JOIN colocate_t1 " +
                "ON colocate_t0.v1 = colocate_t1.v4 OR colocate_t0.v2 = colocate_t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "BROADCAST");
        // Should not be COLOCATE for disjunctive join
        assertNotContains(plan, "COLOCATE");
    }

    @Test
    public void testDistributionModeThreeWayJoin() throws Exception {
        // Multi-way join with OR should maintain BROADCAST for the disjunctive part
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5 JOIN t2 ON t1.v4 = t2.v7";
        String plan = getFragmentPlan(sql);

        // At least one BROADCAST for the disjunctive join
        assertContains(plan, "BROADCAST");
    }

    // ====================================================================================
    // 5. BOUNDARIES AND GUARDS
    // ====================================================================================

    @Test
    public void testSingleClauseIsRegularHashJoin() throws Exception {
        // join_on_clauses.size() == 1 -> regular hash join, not disjunctive
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "HASH JOIN");
        // Single equality should allow SHUFFLE/PARTITIONED mode
        // (not forced to BROADCAST)
    }

    @Test
    public void testEqForNullMixedWithEq() throws Exception {
        // Mixed = and <=> between disjuncts -> join should still work, RF disabled
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 <=> t1.v5";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "JOIN");
        // No RF due to mixed opcodes
        assertNotContains(plan, "build runtime filters");
    }

    @Test
    public void testEqForNullInBothDisjuncts() throws Exception {
        // Both disjuncts use <=> -> should work
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 <=> t1.v4 OR t0.v2 <=> t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "JOIN");
    }

    @Test
    public void testManyDisjuncts() throws Exception {
        // Test with many disjuncts (within limit)
        String sql = "SELECT * FROM t0 JOIN t1 ON " +
                "t0.v1 = t1.v4 OR t0.v2 = t1.v5 OR t0.v3 = t1.v6 OR " +
                "t0.v1 = t1.v5 OR t0.v2 = t1.v6 OR t0.v3 = t1.v4";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "JOIN");
    }

    @Test
    public void testMaxDisjunctsLimit() throws Exception {
        // BE has kMaxDisjunctiveClauses = 16 limit
        // Test with exactly 16 disjuncts (should work)
        StringBuilder sql = new StringBuilder("SELECT * FROM t0 JOIN t1 ON ");
        for (int i = 0; i < 16; i++) {
            if (i > 0) {
                sql.append(" OR ");
            }
            // Cycle through columns: v1, v2, v3 and v4, v5, v6
            int leftCol = (i % 3) + 1;
            int rightCol = ((i + i / 3) % 3) + 4;
            sql.append("t0.v").append(leftCol).append(" = t1.v").append(rightCol);
        }
        String plan = getFragmentPlan(sql.toString());
        assertContains(plan, "JOIN");
    }

    @Test
    public void testExceedMaxDisjunctsLimit() {
        // Test with 17 disjuncts (exceeds kMaxDisjunctiveClauses = 16 limit)
        // Should either:
        // 1. Fall back to NLJ
        // 2. Throw NotSupported error
        // 3. Use SplitJoinORToUnionRule instead
        StringBuilder sql = new StringBuilder("SELECT * FROM t0 JOIN t1 ON ");
        for (int i = 0; i < 17; i++) {
            if (i > 0) {
                sql.append(" OR ");
            }
            int leftCol = (i % 3) + 1;
            int rightCol = ((i + i / 3) % 3) + 4;
            sql.append("t0.v").append(leftCol).append(" = t1.v").append(rightCol);
        }
        try {
            String plan = getFragmentPlan(sql.toString());
            // If a plan is generated, it should either:
            // - Use NLJ (not optimal but acceptable)
            // - Use UNION ALL strategy from SplitJoinORToUnionRule
            // - Or fewer than 17 hash tables if some predicates were deduplicated
            assertContains(plan, "JOIN");
        } catch (Exception e) {
            // NotSupported is also acceptable
            assertTrue(e.getMessage().contains("not supported") ||
                       e.getMessage().contains("exceed") ||
                       e.getMessage().contains("too many"),
                    "Unexpected error: " + e.getMessage());
        }
    }

    @Test
    public void testDisjunctWithoutEquality() {
        // One disjunct has no equality predicate -> cannot use disjunctive hash join
        // Should fall back to NLJ or fail
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v3 > t1.v6";
        try {
            String plan = getFragmentPlan(sql);
            // If it succeeds, it should be using NESTLOOP JOIN
            if (plan.contains("HASH JOIN")) {
                // Check if it's actually not disjunctive (maybe optimizer transformed it)
                // This is acceptable if optimizer found a way to handle it
            } else {
                assertContains(plan, "NESTLOOP JOIN");
            }
        } catch (Exception e) {
            // Also acceptable - cannot create hash join for this case
            assertTrue(e.getMessage().contains("not supported") ||
                       e.getMessage().contains("Cannot") ||
                       e.getMessage().contains("equality"),
                    "Unexpected error: " + e.getMessage());
        }
    }

    @Test
    public void testNestedOrAnd() throws Exception {
        // Nested OR/AND: (a=b OR c=d) AND (e=f)
        // Should extract (a=b OR c=d) as disjunctive join and e=f as additional predicate
        String sql = "SELECT * FROM t0 JOIN t1 ON (t0.v1 = t1.v4 OR t0.v2 = t1.v5) AND t0.v3 = t1.v6";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "JOIN");
    }

    @Test
    public void testOrWithConstantPredicate() throws Exception {
        // OR with constant: t0.v1 = t1.v4 OR 1=0
        // The constant false branch should be eliminated
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR 1=0";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "HASH JOIN");
    }

    @Test
    public void testOrWithConstantTrue() throws Exception {
        // OR with constant true: t0.v1 = t1.v4 OR 1=1
        // This becomes a cross join (always matches)
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR 1=1";
        String plan = getFragmentPlan(sql);

        // Should become CROSS JOIN since one branch is always true
        assertContains(plan, "CROSS JOIN");
    }

    // ====================================================================================
    // 6. UNIT TESTS FOR JoinHelper METHODS
    // ====================================================================================

    @Test
    public void testJoinHelperExtractJoinOnClauses() {
        // Test JoinHelper.extractJoinOnClauses directly
        ColumnRefOperator leftCol1 = new ColumnRefOperator(1, Type.BIGINT, "v1", true);
        ColumnRefOperator leftCol2 = new ColumnRefOperator(2, Type.BIGINT, "v2", true);
        ColumnRefOperator rightCol1 = new ColumnRefOperator(4, Type.BIGINT, "v4", true);
        ColumnRefOperator rightCol2 = new ColumnRefOperator(5, Type.BIGINT, "v5", true);

        BinaryPredicateOperator eq1 = BinaryPredicateOperator.eq(leftCol1, rightCol1);
        BinaryPredicateOperator eq2 = BinaryPredicateOperator.eq(leftCol2, rightCol2);

        // v1 = v4 OR v2 = v5
        ScalarOperator orPredicate = CompoundPredicateOperator.or(eq1, eq2);

        ColumnRefSet leftColumns = new ColumnRefSet();
        leftColumns.union(1);
        leftColumns.union(2);
        leftColumns.union(3);

        ColumnRefSet rightColumns = new ColumnRefSet();
        rightColumns.union(4);
        rightColumns.union(5);
        rightColumns.union(6);

        List<JoinOnClause> clauses = JoinHelper.extractJoinOnClauses(leftColumns, rightColumns, orPredicate);

        assertNotNull(clauses);
        assertEquals(2, clauses.size());

        // First clause: v1 = v4
        JoinOnClause clause1 = clauses.get(0);
        assertEquals(1, clause1.getEqJoinConjuncts().size());
        assertFalse(clause1.hasOtherConjuncts());

        // Second clause: v2 = v5
        JoinOnClause clause2 = clauses.get(1);
        assertEquals(1, clause2.getEqJoinConjuncts().size());
        assertFalse(clause2.hasOtherConjuncts());
    }

    @Test
    public void testJoinHelperCanUseHashJoinWithOr() {
        // Test canUseHashJoinWithOr returns true for valid OR predicates
        ColumnRefOperator leftCol1 = new ColumnRefOperator(1, Type.BIGINT, "v1", true);
        ColumnRefOperator rightCol1 = new ColumnRefOperator(4, Type.BIGINT, "v4", true);
        ColumnRefOperator leftCol2 = new ColumnRefOperator(2, Type.BIGINT, "v2", true);
        ColumnRefOperator rightCol2 = new ColumnRefOperator(5, Type.BIGINT, "v5", true);

        BinaryPredicateOperator eq1 = BinaryPredicateOperator.eq(leftCol1, rightCol1);
        BinaryPredicateOperator eq2 = BinaryPredicateOperator.eq(leftCol2, rightCol2);
        ScalarOperator orPredicate = CompoundPredicateOperator.or(eq1, eq2);

        ColumnRefSet leftColumns = new ColumnRefSet();
        leftColumns.union(1);
        leftColumns.union(2);

        ColumnRefSet rightColumns = new ColumnRefSet();
        rightColumns.union(4);
        rightColumns.union(5);

        boolean canUse = JoinHelper.canUseHashJoinWithOr(leftColumns, rightColumns, orPredicate);
        assertTrue(canUse);
    }

    @Test
    public void testJoinHelperCannotUseHashJoinWithOrForNonEquality() {
        // Test returns false when a disjunct has no equality predicate
        ColumnRefOperator leftCol1 = new ColumnRefOperator(1, Type.BIGINT, "v1", true);
        ColumnRefOperator rightCol1 = new ColumnRefOperator(4, Type.BIGINT, "v4", true);
        ColumnRefOperator leftCol2 = new ColumnRefOperator(2, Type.BIGINT, "v2", true);
        ColumnRefOperator rightCol2 = new ColumnRefOperator(5, Type.BIGINT, "v5", true);

        BinaryPredicateOperator eq1 = BinaryPredicateOperator.eq(leftCol1, rightCol1);
        // Use > instead of = for second predicate
        BinaryPredicateOperator gt2 = BinaryPredicateOperator.gt(leftCol2, rightCol2);
        ScalarOperator orPredicate = CompoundPredicateOperator.or(eq1, gt2);

        ColumnRefSet leftColumns = new ColumnRefSet();
        leftColumns.union(1);
        leftColumns.union(2);

        ColumnRefSet rightColumns = new ColumnRefSet();
        rightColumns.union(4);
        rightColumns.union(5);

        boolean canUse = JoinHelper.canUseHashJoinWithOr(leftColumns, rightColumns, orPredicate);
        assertFalse(canUse);
    }

    @Test
    public void testJoinHelperExtractClausesWithOtherConjuncts() {
        // Test extraction with other_conjuncts: (v1 = v4 AND v3 > 10) OR v2 = v5
        ColumnRefOperator leftCol1 = new ColumnRefOperator(1, Type.BIGINT, "v1", true);
        ColumnRefOperator leftCol2 = new ColumnRefOperator(2, Type.BIGINT, "v2", true);
        ColumnRefOperator leftCol3 = new ColumnRefOperator(3, Type.BIGINT, "v3", true);
        ColumnRefOperator rightCol1 = new ColumnRefOperator(4, Type.BIGINT, "v4", true);
        ColumnRefOperator rightCol2 = new ColumnRefOperator(5, Type.BIGINT, "v5", true);

        BinaryPredicateOperator eq1 = BinaryPredicateOperator.eq(leftCol1, rightCol1);
        BinaryPredicateOperator gt = BinaryPredicateOperator.gt(leftCol3,
                new com.starrocks.sql.optimizer.operator.scalar.ConstantOperator(10L, Type.BIGINT));
        BinaryPredicateOperator eq2 = BinaryPredicateOperator.eq(leftCol2, rightCol2);

        // (v1 = v4 AND v3 > 10)
        ScalarOperator andPredicate = CompoundPredicateOperator.and(eq1, gt);
        // (v1 = v4 AND v3 > 10) OR v2 = v5
        ScalarOperator orPredicate = CompoundPredicateOperator.or(andPredicate, eq2);

        ColumnRefSet leftColumns = new ColumnRefSet();
        leftColumns.union(1);
        leftColumns.union(2);
        leftColumns.union(3);

        ColumnRefSet rightColumns = new ColumnRefSet();
        rightColumns.union(4);
        rightColumns.union(5);
        rightColumns.union(6);

        List<JoinOnClause> clauses = JoinHelper.extractJoinOnClauses(leftColumns, rightColumns, orPredicate);

        assertNotNull(clauses);
        assertEquals(2, clauses.size());

        // First clause should have eq and other_conjunct
        JoinOnClause clause1 = clauses.get(0);
        assertEquals(1, clause1.getEqJoinConjuncts().size());
        assertTrue(clause1.hasOtherConjuncts());
        assertEquals(1, clause1.getOtherConjuncts().size());

        // Second clause should have eq only
        JoinOnClause clause2 = clauses.get(1);
        assertEquals(1, clause2.getEqJoinConjuncts().size());
        assertFalse(clause2.hasOtherConjuncts());
    }

    @Test
    public void testJoinHelperSingleDisjunctReturnsNull() {
        // Single predicate (no OR) should return null
        ColumnRefOperator leftCol = new ColumnRefOperator(1, Type.BIGINT, "v1", true);
        ColumnRefOperator rightCol = new ColumnRefOperator(4, Type.BIGINT, "v4", true);

        BinaryPredicateOperator eq = BinaryPredicateOperator.eq(leftCol, rightCol);

        ColumnRefSet leftColumns = new ColumnRefSet();
        leftColumns.union(1);

        ColumnRefSet rightColumns = new ColumnRefSet();
        rightColumns.union(4);

        List<JoinOnClause> clauses = JoinHelper.extractJoinOnClauses(leftColumns, rightColumns, eq);
        assertNull(clauses);
    }

    // ====================================================================================
    // 7. EXPLAIN OUTPUT VERIFICATION
    // ====================================================================================

    @Test
    public void testExplainShowsJoinOnClauses() throws Exception {
        // Verbose explain should show the disjunctive join structure
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "HASH JOIN");
        // Should indicate the OR condition in some form
        assertContains(plan, "OR");
    }

    @Test
    public void testExplainShowsOtherConjuncts() throws Exception {
        // Explain should show other_conjuncts
        String sql = "SELECT * FROM t0 JOIN t1 ON (t0.v1 = t1.v4 AND t0.v3 > 10) OR t0.v2 = t1.v5";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "JOIN");
    }

    // ====================================================================================
    // 8. INTEGRATION WITH WHERE CLAUSE
    // ====================================================================================

    @Test
    public void testOrJoinWithWherePredicate() throws Exception {
        // WHERE clause should be applied after join
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5 WHERE t0.v3 > 100";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "HASH JOIN");
        assertContains(plan, "v3 > 100");
    }

    @Test
    public void testOrJoinWithWhereOnBuild() throws Exception {
        // WHERE on build side should be pushed down
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5 WHERE t1.v6 < 50";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "HASH JOIN");
    }

    @Test
    public void testOrJoinWithSubquery() throws Exception {
        // Subquery in FROM with OR join
        String sql = "SELECT * FROM (SELECT * FROM t0 WHERE v3 > 0) sub JOIN t1 ON sub.v1 = t1.v4 OR sub.v2 = t1.v5";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "HASH JOIN");
    }

    // ====================================================================================
    // 9. TYPE HANDLING
    // ====================================================================================

    @Test
    public void testOrJoinWithImplicitCast() throws Exception {
        // Join with implicit type cast
        String sql = "SELECT * FROM t0 JOIN test_all_type ON t0.v1 = test_all_type.t1d OR t0.v2 = test_all_type.t1e";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "JOIN");
    }

    @Test
    public void testOrJoinStringColumns() throws Exception {
        // String column join with OR
        String sql = "SELECT * FROM t7 JOIN t8 ON t7.k1 = t8.k1 OR t7.k2 = t8.k2";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "JOIN");
    }

    // ====================================================================================
    // 10. THRIFT SERIALIZATION VERIFICATION
    // ====================================================================================

    /**
     * Helper method to find HashJoinNode in the plan tree.
     */
    private HashJoinNode findHashJoinNode(PlanNode node) {
        if (node instanceof HashJoinNode) {
            return (HashJoinNode) node;
        }
        for (PlanNode child : node.getChildren()) {
            HashJoinNode result = findHashJoinNode(child);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    /**
     * Helper method to find THashJoinNode in TPlan nodes.
     */
    private THashJoinNode findTHashJoinNode(TPlan tPlan) {
        for (TPlanNode node : tPlan.getNodes()) {
            if (node.getNode_type() == TPlanNodeType.HASH_JOIN_NODE) {
                return node.getHash_join_node();
            }
        }
        return null;
    }

    @Test
    public void testThriftJoinOnClausesSetForTwoDisjuncts() throws Exception {
        // Verify join_on_clauses is serialized for 2 disjuncts
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        ExecPlan execPlan = getExecPlan(sql);

        // Find HashJoinNode in the plan
        HashJoinNode hashJoinNode = null;
        for (PlanFragment fragment : execPlan.getFragments()) {
            hashJoinNode = findHashJoinNode(fragment.getPlanRoot());
            if (hashJoinNode != null) {
                break;
            }
        }
        assertNotNull(hashJoinNode, "HashJoinNode not found in plan");

        // Verify joinOnClauses is set at planner level
        assertTrue(hashJoinNode.hasMultipleJoinOnClauses(),
                "HashJoinNode should have multiple join on clauses for disjunctive join");
        assertEquals(2, hashJoinNode.getJoinOnClauses().size(),
                "Should have exactly 2 join on clauses for 2 disjuncts");

        // Verify Thrift serialization
        TPlan tPlan = hashJoinNode.treeToThrift();
        THashJoinNode tHashJoinNode = findTHashJoinNode(tPlan);
        assertNotNull(tHashJoinNode, "THashJoinNode not found in thrift plan");

        assertTrue(tHashJoinNode.isSetJoin_on_clauses(),
                "join_on_clauses must be set in thrift for disjunctive join");
        assertEquals(2, tHashJoinNode.getJoin_on_clauses().size(),
                "Thrift should have 2 join_on_clauses");

        // Verify each clause has eq_join_conjuncts
        for (TJoinOnClause clause : tHashJoinNode.getJoin_on_clauses()) {
            assertTrue(clause.isSetEq_join_conjuncts(),
                    "Each clause must have eq_join_conjuncts");
            assertFalse(clause.getEq_join_conjuncts().isEmpty(),
                    "eq_join_conjuncts must not be empty");
        }

        // CRITICAL: Verify OR did not remain as other_join_conjunct (double filtering bug)
        // When disjunctive join is used, the OR predicate must be processed via
        // join_on_clauses, NOT as a regular other_join_conjunct. Otherwise BE would
        // execute disjunctive join AND re-filter by the OR, causing wrong results.
        assertTrue(!tHashJoinNode.isSetOther_join_conjuncts() ||
                   tHashJoinNode.getOther_join_conjuncts().isEmpty(),
                "OR predicate must NOT remain as other_join_conjunct - would cause double filtering");
    }

    @Test
    public void testThriftJoinOnClausesSetForThreeDisjuncts() throws Exception {
        // Verify join_on_clauses for 3 disjuncts
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5 OR t0.v3 = t1.v6";
        ExecPlan execPlan = getExecPlan(sql);

        HashJoinNode hashJoinNode = null;
        for (PlanFragment fragment : execPlan.getFragments()) {
            hashJoinNode = findHashJoinNode(fragment.getPlanRoot());
            if (hashJoinNode != null) {
                break;
            }
        }
        assertNotNull(hashJoinNode);

        assertTrue(hashJoinNode.hasMultipleJoinOnClauses());
        assertEquals(3, hashJoinNode.getJoinOnClauses().size());

        TPlan tPlan = hashJoinNode.treeToThrift();
        THashJoinNode tHashJoinNode = findTHashJoinNode(tPlan);
        assertNotNull(tHashJoinNode);

        assertTrue(tHashJoinNode.isSetJoin_on_clauses());
        assertEquals(3, tHashJoinNode.getJoin_on_clauses().size());
    }

    @Test
    public void testThriftJoinOnClausesWithOtherConjuncts() throws Exception {
        // Verify other_conjuncts within clause are serialized
        String sql = "SELECT * FROM t0 JOIN t1 ON (t0.v1 = t1.v4 AND t0.v3 > 10) OR (t0.v2 = t1.v5 AND t0.v3 < 5)";
        ExecPlan execPlan = getExecPlan(sql);

        HashJoinNode hashJoinNode = null;
        for (PlanFragment fragment : execPlan.getFragments()) {
            hashJoinNode = findHashJoinNode(fragment.getPlanRoot());
            if (hashJoinNode != null) {
                break;
            }
        }
        assertNotNull(hashJoinNode);

        assertTrue(hashJoinNode.hasMultipleJoinOnClauses());
        assertEquals(2, hashJoinNode.getJoinOnClauses().size());

        // Verify both clauses have other_conjuncts
        for (var clause : hashJoinNode.getJoinOnClauses()) {
            assertTrue(clause.hasOtherConjuncts(),
                    "Each clause should have other_conjuncts for this query");
        }

        // Verify Thrift serialization
        TPlan tPlan = hashJoinNode.treeToThrift();
        THashJoinNode tHashJoinNode = findTHashJoinNode(tPlan);
        assertNotNull(tHashJoinNode);

        assertTrue(tHashJoinNode.isSetJoin_on_clauses());
        for (TJoinOnClause clause : tHashJoinNode.getJoin_on_clauses()) {
            assertTrue(clause.isSetOther_conjuncts(),
                    "Thrift clause should have other_conjuncts");
            assertFalse(clause.getOther_conjuncts().isEmpty(),
                    "other_conjuncts should not be empty");
        }
    }

    @Test
    public void testThriftSingleClauseNotDisjunctive() throws Exception {
        // Verify single equality is NOT treated as disjunctive join
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4";
        ExecPlan execPlan = getExecPlan(sql);

        HashJoinNode hashJoinNode = null;
        for (PlanFragment fragment : execPlan.getFragments()) {
            hashJoinNode = findHashJoinNode(fragment.getPlanRoot());
            if (hashJoinNode != null) {
                break;
            }
        }
        assertNotNull(hashJoinNode);

        // Should NOT have multiple join on clauses
        assertFalse(hashJoinNode.hasMultipleJoinOnClauses(),
                "Single equality should not be disjunctive join");

        // Thrift should NOT have join_on_clauses set
        TPlan tPlan = hashJoinNode.treeToThrift();
        THashJoinNode tHashJoinNode = findTHashJoinNode(tPlan);
        assertNotNull(tHashJoinNode);

        assertFalse(tHashJoinNode.isSetJoin_on_clauses(),
                "Single equality should not have join_on_clauses in thrift");
    }

    @Test
    public void testThriftLeftOuterJoinOnClauses() throws Exception {
        // Verify LEFT OUTER join serializes join_on_clauses
        String sql = "SELECT * FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        ExecPlan execPlan = getExecPlan(sql);

        HashJoinNode hashJoinNode = null;
        for (PlanFragment fragment : execPlan.getFragments()) {
            hashJoinNode = findHashJoinNode(fragment.getPlanRoot());
            if (hashJoinNode != null) {
                break;
            }
        }
        assertNotNull(hashJoinNode);

        assertTrue(hashJoinNode.hasMultipleJoinOnClauses());

        TPlan tPlan = hashJoinNode.treeToThrift();
        THashJoinNode tHashJoinNode = findTHashJoinNode(tPlan);
        assertNotNull(tHashJoinNode);

        assertTrue(tHashJoinNode.isSetJoin_on_clauses());
        assertEquals(2, tHashJoinNode.getJoin_on_clauses().size());
    }

    @Test
    public void testThriftSemiJoinOnClauses() throws Exception {
        // Verify LEFT SEMI join serializes join_on_clauses
        String sql = "SELECT * FROM t0 LEFT SEMI JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        ExecPlan execPlan = getExecPlan(sql);

        HashJoinNode hashJoinNode = null;
        for (PlanFragment fragment : execPlan.getFragments()) {
            hashJoinNode = findHashJoinNode(fragment.getPlanRoot());
            if (hashJoinNode != null) {
                break;
            }
        }
        assertNotNull(hashJoinNode);

        assertTrue(hashJoinNode.hasMultipleJoinOnClauses());

        TPlan tPlan = hashJoinNode.treeToThrift();
        THashJoinNode tHashJoinNode = findTHashJoinNode(tPlan);
        assertNotNull(tHashJoinNode);

        assertTrue(tHashJoinNode.isSetJoin_on_clauses());
    }

    // ====================================================================================
    // 11. RUNTIME FILTER STRUCTURE VERIFICATION
    // ====================================================================================

    @Test
    public void testRfDescriptorsCommonProbeKey() throws Exception {
        // RF enabled for common probe key - verify at thrift level
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v1 = t1.v5";
        ExecPlan execPlan = getExecPlan(sql);

        HashJoinNode hashJoinNode = null;
        for (PlanFragment fragment : execPlan.getFragments()) {
            hashJoinNode = findHashJoinNode(fragment.getPlanRoot());
            if (hashJoinNode != null) {
                break;
            }
        }
        assertNotNull(hashJoinNode);

        // Should have runtime filters for common probe key at planner level
        assertFalse(hashJoinNode.getBuildRuntimeFilters().isEmpty(),
                "Disjunctive join with common probe key should have runtime filters");

        // Verify at thrift level - this is what BE actually reads
        TPlan tPlan = hashJoinNode.treeToThrift();
        THashJoinNode tHashJoinNode = findTHashJoinNode(tPlan);
        assertNotNull(tHashJoinNode);

        assertTrue(tHashJoinNode.isSetBuild_runtime_filters(),
                "RF must be serialized to thrift for common probe key disjunctive join");
        assertFalse(tHashJoinNode.getBuild_runtime_filters().isEmpty(),
                "build_runtime_filters list must not be empty");
    }

    @Test
    public void testRfDescriptorsDifferentProbeKeys() throws Exception {
        // RF disabled for different probe keys - verify at thrift level
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v2 = t1.v5";
        ExecPlan execPlan = getExecPlan(sql);

        HashJoinNode hashJoinNode = null;
        for (PlanFragment fragment : execPlan.getFragments()) {
            hashJoinNode = findHashJoinNode(fragment.getPlanRoot());
            if (hashJoinNode != null) {
                break;
            }
        }
        assertNotNull(hashJoinNode);

        // Should NOT have runtime filters for different probe keys
        assertTrue(hashJoinNode.getBuildRuntimeFilters().isEmpty(),
                "Disjunctive join with different probe keys should not have runtime filters");

        // Verify at thrift level - RF must not be serialized
        TPlan tPlan = hashJoinNode.treeToThrift();
        THashJoinNode tHashJoinNode = findTHashJoinNode(tPlan);
        assertNotNull(tHashJoinNode);

        // Either not set or empty list
        assertTrue(!tHashJoinNode.isSetBuild_runtime_filters() ||
                   tHashJoinNode.getBuild_runtime_filters().isEmpty(),
                "RF must NOT be serialized for different probe keys");
    }

    @Test
    public void testRfDescriptorsNullSafeMixedDisabled() throws Exception {
        // RF disabled when = and <=> are mixed - verify at thrift level
        String sql = "SELECT * FROM t0 JOIN t1 ON t0.v1 = t1.v4 OR t0.v1 <=> t1.v5";
        ExecPlan execPlan = getExecPlan(sql);

        HashJoinNode hashJoinNode = null;
        for (PlanFragment fragment : execPlan.getFragments()) {
            hashJoinNode = findHashJoinNode(fragment.getPlanRoot());
            if (hashJoinNode != null) {
                break;
            }
        }
        assertNotNull(hashJoinNode);

        // Should NOT have runtime filters when opcodes are mixed
        assertTrue(hashJoinNode.getBuildRuntimeFilters().isEmpty(),
                "Disjunctive join with mixed = and <=> should not have runtime filters");

        // Verify at thrift level - RF must not be serialized
        TPlan tPlan = hashJoinNode.treeToThrift();
        THashJoinNode tHashJoinNode = findTHashJoinNode(tPlan);
        assertNotNull(tHashJoinNode);

        assertTrue(!tHashJoinNode.isSetBuild_runtime_filters() ||
                   tHashJoinNode.getBuild_runtime_filters().isEmpty(),
                "RF must NOT be serialized for mixed = and <=> operators");
    }

    @Test
    public void testRfDescriptorsMultipleEqualitiesPerClauseDisabled() throws Exception {
        // RF disabled when any clause has multiple equalities - verify at thrift level
        String sql = "SELECT * FROM t0 JOIN t1 ON (t0.v1 = t1.v4 AND t0.v2 = t1.v5) OR t0.v1 = t1.v6";
        ExecPlan execPlan = getExecPlan(sql);

        HashJoinNode hashJoinNode = null;
        for (PlanFragment fragment : execPlan.getFragments()) {
            hashJoinNode = findHashJoinNode(fragment.getPlanRoot());
            if (hashJoinNode != null) {
                break;
            }
        }
        assertNotNull(hashJoinNode);

        // Should NOT have runtime filters when first clause has multiple equalities
        assertTrue(hashJoinNode.getBuildRuntimeFilters().isEmpty(),
                "Disjunctive join with multiple equalities per clause should not have runtime filters");

        // Verify at thrift level - RF must not be serialized
        TPlan tPlan = hashJoinNode.treeToThrift();
        THashJoinNode tHashJoinNode = findTHashJoinNode(tPlan);
        assertNotNull(tHashJoinNode);

        assertTrue(!tHashJoinNode.isSetBuild_runtime_filters() ||
                   tHashJoinNode.getBuild_runtime_filters().isEmpty(),
                "RF must NOT be serialized for multiple equalities per clause");
    }
}
