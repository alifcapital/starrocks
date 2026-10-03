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

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JsonPathRewriteRule replaces get_json_* calls with constant paths by extended json subfield columns of the scan.
 * When the scan has no call that can be rewritten, we expect the rule to return the input scan and not to add
 * columns to the scan. When such a call exists in the projection or in the predicate, the scan must still be
 * extended.
 */
class JsonPathRewriteRuleTest {
    @Test
    void scanWithoutRewritableJsonCallIsReturnedUnchanged() {
        Fixture fixture = new Fixture();
        // Every call below has a reason why it cannot be rewritten.
        // non-constant path
        ColumnRefOperator pathRef = fixture.factory.create("path", VarcharType.VARCHAR, true);
        // json column that does not come from the table, like a lambda argument
        ColumnRefOperator detached = fixture.factory.create("detached", JsonType.JSON, true);
        fixture.project(fixture.c1, fixture.c1);
        fixture.project(fixture.factory.create("o1", VarcharType.VARCHAR, true),
                fixture.call(FunctionSet.GET_JSON_STRING, VarcharType.VARCHAR, fixture.c2, pathRef));
        fixture.project(fixture.factory.create("o2", VarcharType.VARCHAR, true),
                fixture.call(FunctionSet.GET_JSON_STRING, VarcharType.VARCHAR, detached,
                        ConstantOperator.createVarchar("f1")));
        fixture.project(fixture.factory.create("o3", VarcharType.VARCHAR, true),
                fixture.call(FunctionSet.GET_JSON_STRING, VarcharType.VARCHAR, fixture.c2,
                        ConstantOperator.createVarchar("f1[1]")));
        fixture.project(fixture.factory.create("o4", VarcharType.VARCHAR, true),
                fixture.call(FunctionSet.GET_JSON_STRING, VarcharType.VARCHAR, fixture.c2,
                        ConstantOperator.createVarchar("$")));
        fixture.project(fixture.factory.create("o5", VarcharType.VARCHAR, true),
                fixture.call("upper", VarcharType.VARCHAR, fixture.factory.create("s", VarcharType.VARCHAR, true)));
        fixture.predicate = new BinaryPredicateOperator(BinaryType.GT, fixture.c1, ConstantOperator.createInt(1));

        OptExpression root = fixture.scan();
        List<OptExpression> result = JsonPathRewriteRule.createForOlapScan().transform(root, fixture.context);
        assertEquals(1, result.size());
        assertSame(root, result.get(0));
        assertEquals(root.getOutputColumns(), fixture.required);
    }

    @Test
    void rewritableCallInProjectionStillExtendsTheScan() {
        Fixture fixture = new Fixture();
        ColumnRefOperator out = fixture.factory.create("o1", VarcharType.VARCHAR, true);
        fixture.project(out, fixture.call(FunctionSet.GET_JSON_STRING, VarcharType.VARCHAR, fixture.c2,
                ConstantOperator.createVarchar("f1")));
        fixture.project(fixture.c1, fixture.c1);

        OptExpression root = fixture.scan();
        List<OptExpression> result = JsonPathRewriteRule.createForOlapScan().transform(root, fixture.context);
        assertNotSame(root, result.get(0));
        LogicalOlapScanOperator scan = result.get(0).getOp().cast();
        ScalarOperator rewritten = scan.getProjection().getColumnRefMap().get(out);
        ColumnRefOperator extended = assertInstanceOf(ColumnRefOperator.class, rewritten);
        assertEquals("c2.f1", extended.getName());
        assertTrue(scan.getColRefToColumnMetaMap().containsKey(extended));
        assertTrue(scan.getColRefToColumnMetaMap().values().stream().anyMatch(c -> c.getName().equals("c2.f1")));
        assertEquals(1, scan.getColumnAccessPaths().size());
        assertTrue(fixture.required.contains(extended));
    }

    @Test
    void rewritableCallInPredicateStillExtendsTheScan() {
        Fixture fixture = new Fixture();
        fixture.project(fixture.c1, fixture.c1);
        fixture.predicate = new BinaryPredicateOperator(BinaryType.GT,
                fixture.call(FunctionSet.GET_JSON_INT, IntegerType.BIGINT, fixture.c2,
                        ConstantOperator.createVarchar("f3")),
                ConstantOperator.createBigint(1));

        OptExpression root = fixture.scan();
        List<OptExpression> result = JsonPathRewriteRule.createForOlapScan().transform(root, fixture.context);
        assertNotSame(root, result.get(0));
        LogicalOlapScanOperator scan = result.get(0).getOp().cast();
        assertInstanceOf(ColumnRefOperator.class, scan.getPredicate().getChild(0));
        assertTrue(scan.getColRefToColumnMetaMap().values().stream().anyMatch(c -> c.getName().equals("c2.f3")));
    }

    @Test
    void caseCollidingSubfieldsKeepTheScanUnchanged() {
        Fixture fixture = new Fixture();
        fixture.project(fixture.factory.create("o1", VarcharType.VARCHAR, true),
                fixture.call(FunctionSet.GET_JSON_STRING, VarcharType.VARCHAR, fixture.c2,
                        ConstantOperator.createVarchar("Campaign")));
        fixture.project(fixture.factory.create("o2", VarcharType.VARCHAR, true),
                fixture.call(FunctionSet.GET_JSON_STRING, VarcharType.VARCHAR, fixture.c2,
                        ConstantOperator.createVarchar("campaign")));

        // The two paths differ only in case, and the extended column names would collide, so nothing is rewritten.
        OptExpression root = fixture.scan();
        List<OptExpression> result = JsonPathRewriteRule.createForOlapScan().transform(root, fixture.context);
        assertSame(root, result.get(0));
    }

    private static final class Fixture {
        final ColumnRefFactory factory = new ColumnRefFactory();
        final OlapTable table = new OlapTable();
        final ColumnRefSet required = new ColumnRefSet();
        final OptimizerContext context;
        final ColumnRefOperator c1;
        final ColumnRefOperator c2;
        final Map<ColumnRefOperator, ScalarOperator> projection = new LinkedHashMap<>();
        ScalarOperator predicate;
        private final Column c1Column = new Column("c1", IntegerType.INT);
        private final Column c2Column = new Column("c2", JsonType.JSON);

        Fixture() {
            ConnectContext connectContext = new ConnectContext();
            connectContext.getSessionVariable().setEnableJSONV2Rewrite(true);
            connectContext.getSessionVariable().setCboUseDBLock(false);
            context = OptimizerFactory.initContext(connectContext, factory);
            context.setTaskContext(new TaskContext(context, new PhysicalPropertySet(), required, Double.MAX_VALUE));
            table.setId(7);
            table.setDefaultDistributionInfo(new HashDistributionInfo(1, new ArrayList<>(List.of(c1Column))));
            table.setNewFullSchema(new ArrayList<>(List.of(c1Column, c2Column)));
            c1 = factory.create("c1", IntegerType.INT, true);
            c2 = factory.create("c2", JsonType.JSON, true);
            factory.updateColumnRefToColumns(c1, c1Column, table);
            factory.updateColumnRefToColumns(c2, c2Column, table);
        }

        void project(ColumnRefOperator key, ScalarOperator value) {
            projection.put(key, value);
        }

        CallOperator call(String name, Type type, ScalarOperator... args) {
            return new CallOperator(name, type, new ArrayList<>(List.of(args)));
        }

        OptExpression scan() {
            Map<ColumnRefOperator, Column> columns = new LinkedHashMap<>();
            columns.put(c1, c1Column);
            columns.put(c2, c2Column);
            Map<Column, ColumnRefOperator> reverse = new LinkedHashMap<>();
            reverse.put(c1Column, c1);
            reverse.put(c2Column, c2);
            LogicalOlapScanOperator base = new LogicalOlapScanOperator(table, columns, reverse, null,
                    Operator.DEFAULT_LIMIT, null);
            LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder().withOperator(base);
            if (predicate != null) {
                builder.setPredicate(predicate);
            }
            if (!projection.isEmpty()) {
                builder.setProjection(new Projection(projection));
            }
            OptExpression expression = OptExpression.create(builder.build());
            expression.deriveLogicalPropertyItself();
            return expression;
        }
    }
}
