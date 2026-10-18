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

import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * "x IN (S1) OR x IN (S2)" in a top-level conjunct of WHERE or HAVING becomes one LEFT SEMI JOIN with
 * "S1 UNION ALL S2" as its right side. The tests compare with the plan of the same query with the rewrite off.
 */
public class InSubqueryOrMergeTest extends PlanTestBase {

    @BeforeAll
    public static void createTypesTable() throws Exception {
        starRocksAssert.withTable("create table rvb (i int, b bigint, si smallint, s varchar(30), s2 varchar(10), " +
                "c char(10), d date, dt datetime, d2 decimal(10,2), d4 decimal(12,4), d30 decimal(38,30)) " +
                "duplicate key(i) distributed by hash(i) buckets 1 properties('replication_num' = '1')");
    }

    private String plan(String sql, boolean rewrite) throws Exception {
        connectContext.getSessionVariable().setEnableRewriteInOrToUnion(rewrite);
        try {
            return getFragmentPlan(sql);
        } finally {
            connectContext.getSessionVariable().setEnableRewriteInOrToUnion(true);
        }
    }

    // The OR of INs is one semi join over a union, and nothing of the three-valued plan is left.
    private void assertMerged(String sql) throws Exception {
        String merged = plan(sql, true);
        assertEquals(1, StringUtils.countMatches(merged, "SEMI JOIN"), merged);
        assertContains(merged, "UNION");
        assertNotContains(merged, "NESTLOOP");
        assertNotContains(merged, "OUTER JOIN");
        String original = plan(sql, false);
        assertContains(original, "OUTER JOIN");
        assertNotEquals(original, merged);
    }

    private void assertUnchanged(String sql) throws Exception {
        assertEquals(plan(sql, false), plan(sql, true));
    }

    @Test
    public void testMotivatingQueryWithCte() throws Exception {
        String sql = "with final_cte as (select v4 as debit_id, v5 as credit_id from t1) " +
                "select v1, v2, v3 from t0 " +
                "where v1 in (select debit_id from final_cte) or v1 in (select credit_id from final_cte)";
        assertMerged(sql);
        assertContains(plan(sql, true), "LEFT SEMI JOIN (BROADCAST)");
    }

    @Test
    public void testTwoDisjuncts() throws Exception {
        assertMerged("select * from t0 where v1 in (select v4 from t1) or v1 in (select v7 from t2)");
    }

    @Test
    public void testThreeDisjuncts() throws Exception {
        String sql = "select * from t0 where v1 in (select v4 from t1) or v1 in (select v5 from t1) " +
                "or v1 in (select v7 from t2)";
        assertMerged(sql);
        assertEquals(4, StringUtils.countMatches(plan(sql, true), "OlapScanNode"), plan(sql, true));
    }

    @Test
    public void testSubqueriesWithFilters() throws Exception {
        assertMerged("select * from t0 where v1 in (select v4 from t1 where v5 > 10) " +
                "or v1 in (select v7 from t2 where v8 < 5)");
    }

    @Test
    public void testWithOtherConjuncts() throws Exception {
        assertMerged("select * from t0 where v2 > 5 and " +
                "(v1 in (select v4 from t1) or v1 in (select v7 from t2)) and v3 < 100");
    }

    @Test
    public void testTwoMergedConjuncts() throws Exception {
        String sql = "select * from t0 where (v1 in (select v4 from t1) or v1 in (select v7 from t2)) " +
                "and (v2 in (select v5 from t1) or v2 in (select v8 from t2))";
        String merged = plan(sql, true);
        assertEquals(2, StringUtils.countMatches(merged, "SEMI JOIN"), merged);
        assertNotContains(merged, "NESTLOOP");
        assertNotContains(merged, "OUTER JOIN");
        assertNotEquals(plan(sql, false), merged);
    }

    // A constant left side cannot be planned as the OR of INs, and it is a plain semi join once merged.
    @Test
    public void testConstantLeftExpression() throws Exception {
        String sql = "select * from t0 where 3 in (select v4 from t1) or 3 in (select v7 from t2)";
        assertThrows(SemanticException.class, () -> plan(sql, false));
        String merged = plan(sql, true);
        assertEquals(1, StringUtils.countMatches(merged, "SEMI JOIN"), merged);
        assertContains(merged, "UNION");
    }

    @Test
    public void testLeftExpression() throws Exception {
        assertMerged("select * from t0 where v1 + 1 in (select v4 from t1) or v1 + 1 in (select v7 from t2)");
    }

    @Test
    public void testHaving() throws Exception {
        assertMerged("select v1, count(*) from t0 group by v1 " +
                "having v1 in (select v4 from t1) or v1 in (select v7 from t2)");
    }

    @Test
    public void testInDerivedTable() throws Exception {
        assertMerged("select count(*) from (select v1 from t0 " +
                "where v1 in (select v4 from t1) or v1 in (select v7 from t2)) x");
    }

    @Test
    public void testFilterAboveOuterJoin() throws Exception {
        String sql = "select * from t0 left join t1 on t0.v1 = t1.v4 " +
                "where t0.v1 in (select v7 from t2) or t0.v1 in (select v8 from t2)";
        String merged = plan(sql, true);
        assertEquals(1, StringUtils.countMatches(merged, "SEMI JOIN"), merged);
        assertContains(merged, "UNION");
        assertNotContains(merged, "NESTLOOP");
        assertNotEquals(plan(sql, false), merged);
    }

    // The motivating query: the ids are VARCHAR.
    @Test
    public void testMotivatingQueryWithVarcharIds() throws Exception {
        assertMerged("with final_cte as (select t1a as debit_id, t1a as credit_id from test_all_type) " +
                "select t1a, t1b from test_all_type " +
                "where t1a in (select debit_id from final_cte) or t1a in (select credit_id from final_cte)");
    }

    // x is compared with the values of a subquery in a type that depends on the type of the subquery. Subqueries of
    // one type give one comparison type, so the union is compared as each subquery was.
    @Test
    public void testSubqueriesOfOneType() throws Exception {
        assertMerged("select * from rvb where b in (select i from rvb) or b in (select i from rvb)");
        assertMerged("select * from rvb where i in (select b from rvb) or i in (select b from rvb)");
        assertMerged("select * from rvb where b in (select si from rvb) or b in (select si from rvb) " +
                "or b in (select si from rvb)");
        assertMerged("select * from rvb where d in (select d from rvb) or d in (select d from rvb)");
        assertMerged("select * from rvb where dt in (select dt from rvb) or dt in (select dt from rvb)");
        assertMerged("select * from rvb where s in (select d from rvb) or s in (select d from rvb)");
        assertMerged("select * from rvb where s in (select dt from rvb) or s in (select dt from rvb)");
        assertMerged("select * from rvb where d2 in (select d2 from rvb) or d2 in (select d2 from rvb)");
        assertMerged("select * from rvb where s in (select d4 from rvb) or s in (select d4 from rvb)");
        assertMerged("select * from rvb where i in (select d30 from rvb) or i in (select d30 from rvb)");
        assertMerged("select * from rvb where s in (select s from rvb) or s in (select s from rvb)");
    }

    // The length of a VARCHAR does not change how x is compared with it.
    @Test
    public void testVarcharsOfDifferentLength() throws Exception {
        assertMerged("select * from rvb where s in (select s from rvb) or s in (select s2 from rvb)");
        assertMerged("select * from rvb where d in (select s2 from rvb) or d in (select s from rvb)");
        assertMerged("select * from test_all_type where t1a in (select t1a from test_all_type) " +
                "or t1a in (select s from rvb)");
    }

    // s = '2024-01-01 10:00:00' matches d = 2024-01-01 compared as DATE, but not when compared as DATETIME.
    @Test
    public void testVarcharWithDateAndDatetime() throws Exception {
        assertUnchanged("select * from rvb where s in (select d from rvb) or s in (select dt from rvb)");
    }

    // s = '1.234' matches d2 = 1.23 compared as DECIMAL(10,2), but not when compared as DECIMAL(12,4).
    @Test
    public void testVarcharWithDecimalsOfDifferentScale() throws Exception {
        assertUnchanged("select * from rvb where s in (select d2 from rvb) or s in (select d4 from rvb)");
    }

    // i = 20240101 matches d = 2024-01-01 compared as DOUBLE, but CAST(DATETIME AS DOUBLE) is 20240101000000.
    @Test
    public void testIntWithDateAndDatetime() throws Exception {
        assertUnchanged("select * from rvb where i in (select d from rvb) or i in (select dt from rvb)");
    }

    // Subqueries of different types are not merged, even when one type converts to the other without loss.
    @Test
    public void testSubqueriesOfDifferentTypes() throws Exception {
        assertUnchanged("select * from rvb where b in (select i from rvb) or b in (select b from rvb)");
        assertUnchanged("select * from rvb where i in (select si from rvb) or i in (select b from rvb)");
        assertUnchanged("select * from rvb where si in (select si from rvb) or si in (select i from rvb) " +
                "or si in (select si from rvb)");
        assertUnchanged("select * from rvb where dt in (select d from rvb) or dt in (select dt from rvb)");
        assertUnchanged("select * from rvb where d in (select d from rvb) or d in (select dt from rvb)");
        assertUnchanged("select * from rvb where d2 in (select d2 from rvb) or d2 in (select d4 from rvb)");
        assertUnchanged("select * from rvb where d4 in (select d2 from rvb) or d4 in (select d4 from rvb)");
        assertUnchanged("select * from rvb where i in (select d2 from rvb) or i in (select d30 from rvb)");
        assertUnchanged("select * from rvb where b in (select i from rvb) or b in (select s from rvb)");
        assertUnchanged("select * from rvb where s in (select s from rvb) or s in (select b from rvb)");
        assertUnchanged("select * from rvb where d in (select d from rvb) or d in (select s from rvb)");
    }

    // CHAR is not one of the types we merge.
    @Test
    public void testChar() throws Exception {
        assertUnchanged("select * from rvb where c in (select c from rvb) or c in (select c from rvb)");
        assertUnchanged("select * from rvb where s in (select c from rvb) or s in (select s from rvb)");
        assertUnchanged("select * from rvb where s in (select c from rvb) or s in (select c from rvb)");
    }

    @Test
    public void testUnsupportedTypesOfLeftExpression() throws Exception {
        assertUnchanged("select * from test_all_type where t1f in (select t1f from test_all_type) " +
                "or t1f in (select t1f from test_all_type)");
        assertUnchanged("select * from test_all_type where t1e in (select t1e from test_all_type) " +
                "or t1e in (select t1e from test_all_type)");
    }

    @Test
    public void testNotIn() throws Exception {
        assertUnchanged("select * from t0 where v1 not in (select v4 from t1) or v1 not in (select v7 from t2)");
        assertUnchanged("select * from t0 where v1 in (select v4 from t1) or v1 not in (select v7 from t2)");
        assertUnchanged("select * from t0 where v1 not in (select v4 from t1) and v1 not in (select v7 from t2)");
    }

    @Test
    public void testDifferentLeftExpressions() throws Exception {
        assertUnchanged("select * from t0 where v1 in (select v4 from t1) or v2 in (select v7 from t2)");
        assertUnchanged("select * from t0 where v1 in (select v4 from t1) or v1 + 1 in (select v7 from t2)");
    }

    // Each call of a nondeterministic function has its own id, so the left expressions of the disjuncts differ.
    @Test
    public void testNondeterministicLeftExpression() throws Exception {
        assertUnchanged("select * from t0 where v1 + cast(random() * 10 as bigint) in (select v4 from t1) " +
                "or v1 + cast(random() * 10 as bigint) in (select v7 from t2)");
        assertUnchanged("select * from test_all_type where concat(uuid(), t1a) in (select t1a from test_all_type) " +
                "or concat(uuid(), t1a) in (select t1a from test_all_type)");
    }

    @Test
    public void testCorrelatedSubqueries() throws Exception {
        assertUnchanged("select * from t0 where v1 in (select v4 from t1 where t1.v5 = t0.v2) " +
                "or v1 in (select v7 from t2 where t2.v8 = t0.v2)");
        assertUnchanged("select * from t0 where v1 in (select v4 from t1 where t1.v5 = t0.v2) " +
                "or v1 in (select v7 from t2)");
    }

    @Test
    public void testOrWithOtherDisjuncts() throws Exception {
        assertUnchanged("select * from t0 where v1 in (select v4 from t1) or v1 in (select v7 from t2) or v2 = 1");
        assertUnchanged("select * from t0 where v1 in (select v4 from t1) or v2 = 1 or v1 in (select v7 from t2)");
        assertUnchanged("select * from t0 where v1 in (select v4 from t1) or v1 in (1, 2, 3)");
        assertUnchanged("select * from t0 where v1 in (select v4 from t1) or exists (select 1 from t2)");
        assertUnchanged("select * from t0 where (v1 in (select v4 from t1) or v1 in (select v7 from t2)) " +
                "or v1 in (select v8 from t2) and v2 = 1");
    }

    // A row that is not TRUE is dropped only when the OR is a top-level conjunct, so below a NOT, in a
    // projection or in a CASE the three-valued result has to stay.
    @Test
    public void testNullIsNotFalse() throws Exception {
        assertUnchanged("select * from t0 where not (v1 in (select v4 from t1) or v1 in (select v7 from t2))");
        assertUnchanged("select * from t0 where (v1 in (select v4 from t1) or v1 in (select v7 from t2)) is null");
        assertUnchanged("select * from t0 where (v1 in (select v4 from t1) or v1 in (select v7 from t2)) = false");
        assertUnchanged("select v1 in (select v4 from t1) or v1 in (select v7 from t2) from t0");
        assertUnchanged("select case when v1 in (select v4 from t1) or v1 in (select v7 from t2) then 1 else 0 end " +
                "from t0");
        assertUnchanged("select * from t0 where v2 = 1 or (v3 = 2 and (v1 in (select v4 from t1) " +
                "or v1 in (select v7 from t2)))");
        assertUnchanged("select * from t0 where case when v2 = 1 then v1 in (select v4 from t1) or " +
                "v1 in (select v7 from t2) else false end");
    }

    @Test
    public void testJoinOnClause() throws Exception {
        String message = "contains more than one subquery";
        for (String join : new String[] {"inner join", "left join", "right join", "left semi join"}) {
            String sql = "select * from t0 " + join + " t1 on t0.v1 = t1.v4 and " +
                    "(t0.v2 in (select v7 from t2) or t0.v2 in (select v8 from t2))";
            SemanticException off = assertThrows(SemanticException.class, () -> plan(sql, false));
            SemanticException on = assertThrows(SemanticException.class, () -> plan(sql, true));
            assertTrue(off.getMessage().contains(message), off.getMessage());
            assertEquals(off.getMessage(), on.getMessage());
        }
    }

    @Test
    public void testSessionVariableTurnsRewriteOff() throws Exception {
        String sql = "select * from t0 where v1 in (select v4 from t1) or v1 in (select v7 from t2)";
        String off = plan(sql, false);
        assertContains(off, "OUTER JOIN");
        assertNotContains(off, "LEFT SEMI JOIN");
        assertTrue(connectContext.getSessionVariable().isEnableRewriteInOrToUnion());
    }
}
