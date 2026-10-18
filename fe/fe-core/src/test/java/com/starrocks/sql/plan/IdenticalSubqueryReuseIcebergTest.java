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

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.optimizer.transformer.IdenticalSubqueryCollector;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class IdenticalSubqueryReuseIcebergTest extends ConnectorPlanTestBase {
    private SessionVariable saved;

    @BeforeEach
    public void enableReuse() {
        saved = connectContext.getSessionVariable();
        connectContext.setSessionVariable((SessionVariable) saved.clone());
        connectContext.getSessionVariable().setCboCTEReuseIdenticalSubqueries(true);
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
    }

    @AfterEach
    public void restore() {
        connectContext.setSessionVariable(saved);
    }

    private List<List<SubqueryRelation>> collect(String sql) throws Exception {
        QueryStatement stmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        return IdenticalSubqueryCollector.collect(stmt.getQueryRelation(), connectContext);
    }

    private static String join(String left, String right) {
        return "select a.id, a.c, b.c from (" + left + ") a join (" + right + ") b on a.id = b.id";
    }

    @Test
    public void testSameTable() throws Exception {
        String body = "select id, count(*) as c from iceberg0.unpartitioned_db.t0 where data = 'x' group by id";
        String sql = join(body, body);
        Assertions.assertEquals(1, collect(sql).size());
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks");
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "IcebergScanNode"), plan);
    }

    @Test
    public void testVersionAsOf() throws Exception {
        String v1 = "select id, count(*) as c from iceberg0.unpartitioned_db.t0 for version as of 1 group by id";
        String v2 = "select id, count(*) as c from iceberg0.unpartitioned_db.t0 for version as of 2 group by id";
        String current = "select id, count(*) as c from iceberg0.unpartitioned_db.t0 group by id";
        Assertions.assertTrue(collect(join(v1, v2)).isEmpty());
        Assertions.assertTrue(collect(join(v1, current)).isEmpty());
        Assertions.assertEquals(1, collect(join(v1, v1)).size());
    }
}
