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

package com.starrocks.sql.optimizer;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MaterializationContext.prune returns false when the MV cannot be used for the query expression.
 * A false result skips the MV rewrite, so a wrong false loses a rewrite and a wrong true wastes work.
 */
public class MaterializationContextPruneTest {
    private static final long MV_ID = 11;

    private final ColumnRefFactory columns = new ColumnRefFactory();
    private OptimizerContext optimizer;
    private Table tableA;
    private Table tableB;
    private Table tableC;

    @BeforeEach
    public void setUp() {
        optimizer = OptimizerFactory.initContext(new ConnectContext(), columns);
        tableA = table(1);
        tableB = table(2);
        tableC = table(3);
    }

    private static Table table(long id) {
        OlapTable table = new OlapTable();
        table.setId(id);
        return table;
    }

    private static LogicalOlapScanOperator scanOp(Table table) {
        return LogicalOlapScanOperator.builder().setTable(table).build();
    }

    private static OptExpression scan(Table table) {
        return OptExpression.create(scanOp(table));
    }

    private static OptExpression join(OptExpression left, OptExpression right) {
        return OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, null), left, right);
    }

    private MaterializationContext context(OptExpression mvExpression, Table... mvTables) {
        MaterializedView mv = new MaterializedView(MV_ID, 1, "mv", List.of(), KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(1), null);
        return new MaterializationContext(optimizer, mv, mvExpression, columns, columns,
                Arrays.asList(mvTables), Arrays.asList(mvTables), null, List.of(), 0);
    }

    @Test
    public void testScanAlreadyAppliedWithTheMvPrunesIt() {
        // The scan is below the join, so the check must search the whole query tree, not only the root.
        LogicalOlapScanOperator applied = scanOp(tableB);
        applied.setOpAppliedMV(MV_ID);
        OptExpression query = join(scan(tableA), OptExpression.create(applied));

        assertFalse(context(join(scan(tableA), scan(tableB)), tableA, tableB).prune(optimizer, query));
    }

    @Test
    public void testAppliedBitOfAnotherMvDoesNotPrune() {
        LogicalOlapScanOperator applied = scanOp(tableB);
        applied.setOpAppliedMV(MV_ID + 1);
        OptExpression query = join(scan(tableA), OptExpression.create(applied));

        assertTrue(context(join(scan(tableA), scan(tableB)), tableA, tableB).prune(optimizer, query));
    }

    @Test
    public void testCompleteMatchIsKeptEvenWhenAChildAlreadyHoldsAllMvTables() {
        // The inner join alone has all MV tables, but the query root still matches the MV completely.
        // We expect the MV to stay a candidate in both default and greedy mode.
        OptExpression innerJoin = join(scan(tableA), scan(tableB));
        OptExpression query = join(innerJoin, OptExpression.create(new LogicalValuesOperator(List.of())));
        MaterializationContext context = context(join(scan(tableA), scan(tableB)), tableA, tableB);

        assertTrue(context.prune(optimizer, query));
        optimizer.getSessionVariable().setEnableMaterializedViewRewriteGreedyMode(true);
        assertTrue(context.prune(optimizer, query));
    }

    @Test
    public void testQueryDeltaAndDisjointTablesArePruned() {
        OptExpression bigQuery = join(join(scan(tableA), scan(tableB)), scan(tableC));
        assertFalse(context(join(scan(tableA), scan(tableB)), tableA, tableB).prune(optimizer, bigQuery));

        OptExpression otherQuery = join(scan(tableA), scan(tableB));
        assertFalse(context(scan(tableC), tableC).prune(optimizer, otherQuery));
    }

    @Test
    public void testViewDeltaIsPrunedWhenItIsDisabled() {
        optimizer.getSessionVariable().setEnableMaterializedViewViewDeltaRewrite(false);
        OptExpression query = scan(tableA);

        assertFalse(context(join(scan(tableA), scan(tableB)), tableA, tableB).prune(optimizer, query));
    }
}
