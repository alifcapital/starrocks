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

import com.starrocks.connector.iceberg.ScalarOperatorToIcebergExpr;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.sql.optimizer.Utils;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * A LIKE on an Iceberg column reaches the Iceberg scan as a starts-with filter only when the pattern is literal
 * characters and one trailing '%'. The filter skips files, so a wrong one loses rows. The mock table is
 * iceberg0.unpartitioned_db.t0, its columns are id, data and date.
 */
public class LikePrefixIcebergPlanTest extends ConnectorPlanTestBase {
    private static final String TABLE = "iceberg0.unpartitioned_db.t0";

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
    }

    private Expression storageFilter(String where) throws Exception {
        IcebergScanNode scan = (IcebergScanNode) getExecPlan("select id from " + TABLE + " where " + where)
                .getScanNodes().get(0);
        return new ScalarOperatorToIcebergExpr().convert(
                Utils.extractConjuncts(scan.getIcebergJobPlanningPredicate()),
                new ScalarOperatorToIcebergExpr.IcebergContext(scan.getIcebergTable().getNativeTable().schema().asStruct()));
    }

    @Test
    public void testPrefixPatternGivesStartsWith() throws Exception {
        assertEquals(Expressions.startsWith("data", "app").toString(), storageFilter("data like 'app%'").toString());
    }

    @Test
    public void testUnderscoreWildcardGivesNoStartsWith() throws Exception {
        assertEquals(Expression.Operation.TRUE, storageFilter("data like 'app_%'").op());
        // The BE still evaluates the LIKE.
        assertContains(getFragmentPlan("select id from " + TABLE + " where data like 'app_%'"), "LIKE 'app_%'");
    }

    @Test
    public void testEscapedUnderscoreIsPartOfThePrefix() throws Exception {
        // The SQL text is 'a\_b%', which the parser keeps as the pattern a\_b%.
        assertEquals(Expressions.startsWith("data", "a_b").toString(), storageFilter("data like 'a\\_b%'").toString());
    }

    @Test
    public void testOtherPatternsGiveNoStartsWith() throws Exception {
        for (String where : new String[] {
                "data like 'a%b%'", "data like '%app'", "data like 'a\\%'",
                "data not like 'app%'", "ilike(data, 'app%')", "data regexp 'app%'"}) {
            assertEquals(Expression.Operation.TRUE, storageFilter(where).op(), where);
        }
        // The optimizer may rewrite a LIKE of '%' to another predicate, but it is never a prefix.
        assertNotEquals(Expression.Operation.STARTS_WITH, storageFilter("data like '%'").op());
    }
}
