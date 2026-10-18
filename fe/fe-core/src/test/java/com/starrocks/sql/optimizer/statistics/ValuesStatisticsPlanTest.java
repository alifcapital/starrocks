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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.InsertPlanner;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A reference table that a query writes as constants, 'SELECT 'a' name, 1 id UNION ALL SELECT ...', is read by the
 * planner as a VALUES operator. The statistics of its columns are the exact counts of the constants, so a join and a
 * grouping on it are estimated from them. A query without FROM keeps the statistics it had, and so does a VALUES
 * that no operator above it reads, such as the rows of an INSERT: its statistics are not computed.
 */
public class ValuesStatisticsPlanTest extends PlanTestBase {
    private static final long ROWS = 1_000_000L;
    private static StatisticStorage old;

    // 5 names (Bank and bank are two), 4 ids (101 twice and one NULL), 6 rows.
    private static final String FIRST = "select 'Sino' name, 101 id union all select 'Bank', 102 "
            + "union all select 'Apex', 101 union all select 'bank', 103 union all select 'Cyanide', null "
            + "union all select 'Sino', 104";
    // 4 names, 3 ids (201 twice and one NULL), 5 rows.
    private static final String SECOND = "select 'Sino' name, 201 id union all select 'bank', 202 "
            + "union all select 'Apex', null union all select 'Zeta', 203 union all select 'Sino', 201";

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ValuesStatisticsCalls.install();
        starRocksAssert.withTable("CREATE TABLE values_fact (k varchar(20), v bigint) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        StringBuilder wide = new StringBuilder("CREATE TABLE values_wide (");
        for (int i = 0; i < 20; i++) {
            wide.append(i == 0 ? "" : ", ").append("c").append(i).append(" varchar(64)");
        }
        starRocksAssert.withTable(wide + ") DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE values_sink (a bigint, b int) "
                + "DUPLICATE KEY(a) DISTRIBUTED BY HASH(a) BUCKETS 1 PROPERTIES('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE values_pk (k bigint NOT NULL, v varchar(20), "
                + "u varchar(36) DEFAULT (uuid())) PRIMARY KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        OlapTable table = (OlapTable) starRocksAssert.getTable("test", "values_fact");
        setTableStatistics(table, ROWS);
        table.getAllPartitions().forEach(p -> UtFrameUtils.setPartitionVersion(p, 2));
        GlobalStateMgr state = connectContext.getGlobalStateMgr();
        old = state.getStatisticStorage();
        state.setStatisticStorage(new StatisticStorage() {
            @Override
            public ColumnStatistic getColumnStatistic(Table ignored, String column) {
                if (column.equalsIgnoreCase("k")) {
                    return new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 6, 1000);
                }
                return new ColumnStatistic(1, ROWS, 0, 8, ROWS);
            }

            @Override
            public List<ColumnStatistic> getColumnStatistics(Table t, List<String> columns) {
                return columns.stream().map(c -> getColumnStatistic(t, c)).toList();
            }

            @Override
            public void addColumnStatistic(Table t, String c, ColumnStatistic statistic) {
            }
        });
    }

    @AfterAll
    public static void dropValuesFact() throws Exception {
        connectContext.getGlobalStateMgr().setStatisticStorage(old);
        starRocksAssert.dropTable("values_fact");
        starRocksAssert.dropTable("values_wide");
        starRocksAssert.dropTable("values_sink");
        starRocksAssert.dropTable("values_pk");
    }

    // The first operator of the physical plan that has the given class, in the order of the plan.
    private static OptExpression find(OptExpression plan, Class<?> operator) {
        if (operator.isInstance(plan.getOp())) {
            return plan;
        }
        for (OptExpression input : plan.getInputs()) {
            OptExpression found = find(input, operator);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    // The statistics of the column of the VALUES of the plan at the given position.
    private ColumnStatistic valuesColumn(String sql, int position) throws Exception {
        ExecPlan plan = getExecPlan(sql);
        OptExpression values = find(plan.getPhysicalPlan(), PhysicalValuesOperator.class);
        Assertions.assertNotNull(values, plan.getExplainString(TExplainLevel.COSTS));
        ColumnRefOperator column = ((PhysicalValuesOperator) values.getOp()).getColumnRefSet().get(position);
        return values.getStatistics().getColumnStatistic(column);
    }

    // The cardinality of the first node with the given text in a cost plan.
    private static long cardinality(String plan, String node) {
        int at = plan.indexOf(node);
        Assertions.assertTrue(at >= 0, plan);
        Matcher matcher = Pattern.compile("cardinality: (\\d+)").matcher(plan);
        Assertions.assertTrue(matcher.find(at), plan);
        return Long.parseLong(matcher.group(1));
    }

    @Test
    public void testReferenceWrittenAsUnionOfConstantsHasTheStatisticsOfTheConstants() throws Exception {
        String sql = "select name, id from (" + FIRST + ") t group by name, id";
        ColumnStatistic name = valuesColumn(sql, 0);
        Assertions.assertEquals(5, name.getDistinctValuesCount());
        Assertions.assertEquals(0, name.getNullsFraction());
        // The names are 4 + 4 + 4 + 4 + 7 + 4 characters in 6 rows.
        Assertions.assertEquals(27.0 / 6, name.getAverageRowSize(), 1e-12);
        Assertions.assertTrue(name.isInfiniteRange());

        ColumnStatistic id = valuesColumn(sql, 1);
        Assertions.assertEquals(4, id.getDistinctValuesCount());
        Assertions.assertEquals(1.0 / 6, id.getNullsFraction(), 1e-12);
        Assertions.assertEquals(101, id.getMinValue());
        Assertions.assertEquals(104, id.getMaxValue());
    }

    @Test
    public void testValuesListHasTheStatisticsOfItsConstants() throws Exception {
        String sql = "select x, y from (values (1, 'a'), (2, 'b'), (2, null)) t(x, y) group by x, y";
        ColumnStatistic x = valuesColumn(sql, 0);
        Assertions.assertEquals(2, x.getDistinctValuesCount());
        Assertions.assertEquals(1, x.getMinValue());
        Assertions.assertEquals(2, x.getMaxValue());
        ColumnStatistic y = valuesColumn(sql, 1);
        Assertions.assertEquals(2, y.getDistinctValuesCount());
        Assertions.assertEquals(1.0 / 3, y.getNullsFraction(), 1e-12);
    }

    // Plans a query or an INSERT, as the front end does.
    private static void plan(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        connectContext.getDumpInfo().setOriginStmt(sql);
        new StatementPlanner().plan(statement, connectContext);
    }

    // How many times the planning of the statement computes the statistics of VALUES.
    private static int calls(String sql) throws Exception {
        return ValuesStatisticsCalls.during(() -> plan(sql));
    }

    private static void assertRead(String sql) throws Exception {
        Assertions.assertTrue(calls(sql) > 0, sql);
    }

    private static void assertNotRead(String sql) throws Exception {
        Assertions.assertEquals(0, calls(sql), sql);
    }

    private static String insertValues(String table, int rows, int columns) {
        StringBuilder sql = new StringBuilder("insert into " + table + " values ");
        for (int i = 0; i < rows; i++) {
            sql.append(i == 0 ? "(" : ",(");
            for (int j = 0; j < columns; j++) {
                sql.append(j == 0 ? "'" : ",'").append(j % 2 == 0 ? "s" + i + "_" + j : "g" + (i % 100) + "_" + j)
                        .append("'");
            }
            sql.append(')');
        }
        return sql.toString();
    }

    @Test
    public void testRowsOfAnInsertAreNotComputed() throws Exception {
        assertNotRead(insertValues("values_wide", 10_000, 20));
        assertNotRead("insert into values_sink values (1, 2), (3, 4)");
        // Literals of another type are cast to the type of the column, and the cast is a projection.
        assertNotRead("insert into values_sink values ('1', 2), ('3', 4)");
        // The default and the shuffle of the primary key are projections and an exchange.
        boolean shuffle = InsertPlanner.enableSingleReplicationShuffle;
        boolean unitTest = FeConstants.runningUnitTest;
        InsertPlanner.enableSingleReplicationShuffle = true;
        FeConstants.runningUnitTest = true;
        try {
            assertNotRead("insert into values_pk (k, v) values (1, 'a'), (2, 'b')");
        } finally {
            InsertPlanner.enableSingleReplicationShuffle = shuffle;
            FeConstants.runningUnitTest = unitTest;
        }
    }

    @Test
    public void testValuesUnderNoReaderAreNotComputed() throws Exception {
        assertNotRead("select * from (values (1, 'a'), (2, 'b')) t");
        assertNotRead("select name, id from (" + FIRST + ") t");
        assertNotRead("select * from (values (1)) t(a) limit 1");
        // A projection over the list does not read it either.
        assertNotRead("select a + 1, 'x' from (values (1), (2)) t(a)");
    }

    @Test
    public void testValuesWithoutReaderKeepsUnknownStatistics() throws Exception {
        String sql = "select name, id from (" + FIRST + ") t";
        Assertions.assertTrue(valuesColumn(sql, 0).isUnknown());
        Assertions.assertTrue(valuesColumn(sql, 1).isUnknown());
    }

    @Test
    public void testValuesUnderAReaderAreComputed() throws Exception {
        // The reference of a join in the body of a CTE, with a grouping above the CTE.
        assertRead("with c as (select f.k, m.d from values_fact f left join "
                + "(values ('Sino', 101), ('Bank', 102)) as m(name, d) on lower(f.k) = lower(m.name)) "
                + "select d, count(*) from c group by d");
        // The join alone reads: there is no grouping and no filter.
        assertRead("select f.v from values_fact f join (values ('a'), ('b')) v(k) on f.k = v.k");
        assertRead("select a, count(*) from (values (1), (2), (2)) t(a) group by a");
        assertRead("select * from (values (1), (2)) t(a) where a > 1");
        assertRead("select name, count(*) from (select 'a' name union all select 'b' union all select 'a') t "
                + "group by name");
        // An uncorrelated scalar subquery is an Apply with an aggregation over the list: the price of the flag for
        // the whole query is that the list of the subquery is computed too.
        assertRead("select (select max(a) from (values (1), (2)) t(a)) c");
        // The Apply of an IN subquery is the only reader of the list: there is no aggregation or filter in the query.
        assertRead("select v1 in (select a from (values (1), (2)) t(a)) from t0");
    }

    // The consumers of a CTE that the planner forces to reuse have no input: the list is in the producer, and the
    // reader is over the consumers.
    private void assertReadThroughReusedCte(String sql) throws Exception {
        int threshold = connectContext.getSessionVariable().getCboCTEForceReuseNodeCount();
        connectContext.getSessionVariable().setCboCTEForceReuseNodeCount(1);
        try {
            assertRead(sql);
        } finally {
            connectContext.getSessionVariable().setCboCTEForceReuseNodeCount(threshold);
        }
    }

    @Test
    public void testValuesOfAReusedCteAreReadByTheJoinsOverItsConsumers() throws Exception {
        assertReadThroughReusedCte("with dict as (select * from (values ('a', 1), ('b', 2)) t(k, d)) "
                + "select f.v from values_fact f join dict d1 on f.k = d1.k join dict d2 on f.k = d2.k");
        assertReadThroughReusedCte("with a as (select * from (values ('a', 1), ('b', 2)) t(k, d)), "
                + "b as (select k, d + 1 d from a) "
                + "select f.v from values_fact f join b b1 on f.k = b1.k join b b2 on f.k = b2.k");
    }

    @Test
    public void testValuesOfAReusedCteWithoutReaderAreNotComputed() throws Exception {
        int threshold = connectContext.getSessionVariable().getCboCTEForceReuseNodeCount();
        connectContext.getSessionVariable().setCboCTEForceReuseNodeCount(1);
        try {
            assertNotRead("with dict as (select * from (values ('a', 1), ('b', 2)) t(k, d)) "
                    + "select * from dict union all select * from dict");
        } finally {
            connectContext.getSessionVariable().setCboCTEForceReuseNodeCount(threshold);
        }
    }

    @Test
    public void testReaderInAnotherBranchIsNotAnAncestor() throws Exception {
        String select = "select * from (values (1)) t(a) union all select v1 from t0 where v1 > 1";
        assertNotRead(select);
        assertNotRead("insert into values_sink (a) " + select);
    }

    @Test
    public void testGroupsOfCoalesceOverTwoReferencesAreTheirDistinctIds() throws Exception {
        String sql = "select coalesce(a.id, b.id) c, count(*) from values_fact f "
                + "left join (" + FIRST + ") a on lower(f.k) = lower(a.name) "
                + "left join (" + SECOND + ") b on lower(f.k) = lower(b.name) group by coalesce(a.id, b.id)";
        String plan = getCostExplain(sql);
        // Coalesce reads its arguments up to the first one without NULL. After the left joins both ids have NULL, so
        // it reads both and takes 4 + 3 distinct values, and the NULL is one more group.
        Assertions.assertEquals(4 + 3 + 1, cardinality(plan, "AGGREGATE (merge finalize)"), plan);
        Assertions.assertEquals(ROWS, cardinality(plan, "HASH JOIN"), plan);
    }

    @Test
    public void testQueryWithoutFromKeepsUnknownStatistics() throws Exception {
        // The statistics of the NULL column of the dual stay unknown: rules that keep the plan of such a query test
        // for unknown statistics.
        String dual = "  0:UNION\n"
                + "     constant exprs: \n"
                + "         NULL\n"
                + "     cardinality: 1\n"
                + "     column statistics: \n"
                + "     * -->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n";
        Assertions.assertTrue(getCostExplain("select 1").contains(dual), getCostExplain("select 1"));
        String function = getCostExplain("select * from table(generate_series(1, 3))");
        Assertions.assertTrue(function.contains(dual), function);
        Assertions.assertTrue(function.contains("* generate_series-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN"),
                function);
    }

    // We fear that the NULL share of the keys of a reference, which is now known, makes SkewJoinOptimizeRule salt
    // the join: it takes a NULL share of 0.2 as a skew.
    private void assertNoSalting(String sql) throws Exception {
        boolean enabled = connectContext.getSessionVariable().isEnableStatsToOptimizeSkewJoin();
        connectContext.getSessionVariable().setEnableStatsToOptimizeSkewJoin(true);
        try {
            String plan = getCostExplain(sql);
            Assertions.assertTrue(plan.contains("JOIN"), plan);
            Assertions.assertFalse(plan.contains("rand_col"), plan);
        } finally {
            connectContext.getSessionVariable().setEnableStatsToOptimizeSkewJoin(enabled);
        }
    }

    @Test
    public void testValuesListWithFewNullKeysIsNotSalted() throws Exception {
        // 1 of 6 keys is NULL, below the share of a skew.
        assertNoSalting("select count(*) from (values ('Sino', 101), (null, 102), ('Bank', 103), ('Apex', 104), "
                + "('Zeta', 105), ('Moon', 106)) r(name, id) left join values_fact f on r.name = f.k");
    }

    @Test
    public void testInnerJoinWithValuesListOfNullKeysIsNotSalted() throws Exception {
        // Half of the keys are NULL, and an inner join drops them.
        assertNoSalting("select count(*) from (values ('Sino', 101), (null, 102), (null, 103), ('Apex', 104)) "
                + "r(name, id) join values_fact f on r.name = f.k");
    }
}
