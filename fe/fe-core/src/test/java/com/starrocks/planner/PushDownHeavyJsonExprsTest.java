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

package com.starrocks.planner;

import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// JSON extraction next to heavy expressions that push_down_heavy_exprs moves into an OLAP scan: the plan parses a
// document no more often than without the JSON path rewrite, and it serializes to thrift.
public class PushDownHeavyJsonExprsTest {
    protected static ConnectContext ctx;
    protected static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("test").useDatabase("test");
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS t_heavy (\n" +
                "    id BIGINT, k INT, v VARCHAR(2000), w VARCHAR(2000)\n" +
                ") PROPERTIES('replication_num'='1');");
    }

    private static String plan(String sql, boolean rewrite) throws Exception {
        boolean previous = ctx.getSessionVariable().isEnableJsonPathRewrite();
        try {
            ctx.getSessionVariable().setEnableJsonPathRewrite(rewrite);
            UtFrameUtils.getPlanThriftString(ctx, sql);
            return UtFrameUtils.getFragmentPlan(ctx, sql);
        } finally {
            ctx.getSessionVariable().setEnableJsonPathRewrite(previous);
        }
    }

    private static int parses(String plan) {
        Matcher matcher = Pattern.compile("(json_extract_paths|get_json_\\w+)\\(3: v\\b").matcher(plan);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    private static void check(String sql) throws Exception {
        String with = plan(sql, true);
        String without = plan(sql, false);
        Assertions.assertTrue(with.contains("heavy exprs:"), with);
        Assertions.assertTrue(parses(with) <= parses(without), with + "\n" + without);
        Assertions.assertEquals(without, with);
    }

    @Test
    public void testSharedByHeavyAndProject() throws Exception {
        check("select regexp_replace(get_json_string(v, '$.a'), 'x', 'y') r, get_json_string(v, '$.b') b " +
                "from t_heavy");
    }

    @Test
    public void testChainOfCommonExprs() throws Exception {
        check("select regexp_replace(get_json_string(v, '$.a'), 'x', 'y') r, " +
                "concat(get_json_string(v, '$.b'), get_json_string(v, '$.b')) c, " +
                "upper(concat(get_json_string(v, '$.b'), get_json_string(v, '$.b'))) u " +
                "from t_heavy");
    }

    @Test
    public void testHeavyCommonExpr() throws Exception {
        check("select concat(regexp_replace(get_json_string(v, '$.a'), 'x', 'y'), 'p') p, " +
                "concat(regexp_replace(get_json_string(v, '$.a'), 'x', 'y'), 'q') q, " +
                "get_json_string(v, '$.b') b from t_heavy");
    }

    @Test
    public void testHeavyOnlyWithPredicateAndLimit() throws Exception {
        check("select regexp_replace(get_json_string(v, '$.a'), 'x', 'y') r, " +
                "regexp_replace(get_json_string(v, '$.b'), 'x', 'y') s from t_heavy " +
                "where k = 1 order by r limit 10");
    }

    @Test
    public void testOtherTextIsRewritten() throws Exception {
        // The text that no heavy expression reads is still grouped.
        String with = plan("select regexp_replace(get_json_string(v, '$.a'), 'x', 'y') r, " +
                "get_json_string(w, '$.a') a, get_json_string(w, '$.b') b from t_heavy", true);
        Assertions.assertTrue(with.contains("json_extract_paths(4: w, '$.a', '$.b')")
                || with.contains("json_extract_paths(4: w, '$.b', '$.a')"), with);
    }
}
