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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_PARTITION_PRUNED;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator.convertToDateRange;

public class MvUtilsTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.alter_scheduler_interval_millisecond = 1;
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        String dbName = "test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        connectContext.getSessionVariable().setMaxTransformReorderJoins(8);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
        connectContext.getSessionVariable().setEnableReplicationJoin(false);
        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t1` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test
    public void testContainsTimeTravelScanDetectsMysqlTemporalClause() {
        // A MySQL temporal read keeps the clause on the scan operator and leaves the version range empty,
        // so a version-range check alone misses it. This is the same second representation that
        // AnalyzerUtils#prohibitTimeTravelQuery has to handle on the definition side.
        MysqlTable mysqlTable = new MysqlTable();
        LogicalMysqlScanOperator ordinaryScan = new LogicalMysqlScanOperator(mysqlTable, Maps.newHashMap(),
                Maps.newHashMap(), Operator.DEFAULT_LIMIT, null, null);
        Assertions.assertFalse(MvUtils.containsTimeTravelScan(OptExpression.create(ordinaryScan)));

        LogicalMysqlScanOperator temporalScan = new LogicalMysqlScanOperator(mysqlTable, Maps.newHashMap(),
                Maps.newHashMap(), Operator.DEFAULT_LIMIT, null, null);
        temporalScan.setTemporalClause("FOR SYSTEM_TIME AS OF '2026-01-01 00:00:00'");
        Assertions.assertTrue(MvUtils.containsTimeTravelScan(OptExpression.create(temporalScan)));
    }

    @Test
    public void testGetAllPredicate() {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator columnRef1 = columnRefFactory.create("col1", IntegerType.INT, false);
        ColumnRefOperator columnRef2 = columnRefFactory.create("col2", IntegerType.INT, false);
        ColumnRefOperator columnRef3 = columnRefFactory.create("col3", IntegerType.INT, false);
        BinaryPredicateOperator binaryPredicate = new BinaryPredicateOperator(
                BinaryType.EQ, columnRef1, columnRef2);

        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore().getDb("test");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t0");
        LogicalScanOperator scanOperator1 = new LogicalOlapScanOperator(table1);
        BinaryPredicateOperator binaryPredicate2 = new BinaryPredicateOperator(
                BinaryType.GE, columnRef1, ConstantOperator.createInt(1));
        scanOperator1.setPredicate(binaryPredicate2);
        OptExpression scanExpr = OptExpression.create(scanOperator1);
        Table table2 = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t1");
        LogicalScanOperator scanOperator2 = new LogicalOlapScanOperator(table2);
        BinaryPredicateOperator binaryPredicate3 = new BinaryPredicateOperator(
                BinaryType.GE, columnRef2, ConstantOperator.createInt(1));
        scanOperator2.setPredicate(binaryPredicate3);
        OptExpression scanExpr2 = OptExpression.create(scanOperator2);
        LogicalJoinOperator joinOperator = new LogicalJoinOperator(JoinOperator.INNER_JOIN, binaryPredicate);
        OptExpression joinExpr = OptExpression.create(joinOperator, scanExpr, scanExpr2);
        Set<ScalarOperator> predicates = MvUtils.getAllValidPredicates(joinExpr);
        Assertions.assertEquals(3, predicates.size());
        Assertions.assertTrue(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr));
        LogicalJoinOperator joinOperator2 = new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, binaryPredicate);
        OptExpression joinExpr2 = OptExpression.create(joinOperator2, scanExpr, scanExpr2);
        Assertions.assertFalse(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr2));
        OptExpression joinExpr3 = OptExpression.create(joinOperator, scanExpr, joinExpr2);
        Assertions.assertFalse(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr3));

        LogicalJoinOperator joinOperator3 = new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                Utils.compoundAnd(binaryPredicate, binaryPredicate2));
        OptExpression joinExpr4 = OptExpression.create(joinOperator3, scanExpr, scanExpr2);
        Assertions.assertFalse(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr4));

        BinaryPredicateOperator binaryPredicate4 = new BinaryPredicateOperator(
                BinaryType.EQ, columnRef1, columnRef3);
        LogicalJoinOperator joinOperator4 = new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                Utils.compoundAnd(binaryPredicate, binaryPredicate4));
        OptExpression joinExpr5 = OptExpression.create(joinOperator4, scanExpr, scanExpr2);
        Assertions.assertTrue(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr5));

        LogicalJoinOperator joinOperator5 = new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                Utils.compoundOr(binaryPredicate, binaryPredicate4));
        OptExpression joinExpr6 = OptExpression.create(joinOperator5, scanExpr, scanExpr2);
        Assertions.assertFalse(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr6));
    }

    @Test
    public void testGetCompensationPredicateForDisjunctive() {
        ConstantOperator alwaysTrue = ConstantOperator.TRUE;
        ConstantOperator alwaysFalse = ConstantOperator.createBoolean(false);
        CompoundPredicateOperator compound = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR, alwaysFalse, alwaysTrue);
        Assertions.assertEquals(alwaysTrue, MvUtils.getCompensationPredicateForDisjunctive(alwaysTrue, compound));
        Assertions.assertEquals(alwaysFalse, MvUtils.getCompensationPredicateForDisjunctive(alwaysFalse, compound));
        Assertions.assertEquals(null, MvUtils.getCompensationPredicateForDisjunctive(compound, alwaysFalse));
        Assertions.assertEquals(alwaysTrue, MvUtils.getCompensationPredicateForDisjunctive(compound, compound));
    }

    @Test
    public void testConvertToDateRange() throws AnalysisException {
        {
            LocalDate date1 = LocalDate.of(2025, 10, 10);
            PartitionKey upper = PartitionKey.ofDate(date1);
            Range<PartitionKey> upRange = Range.atMost(upper);
            Range<PartitionKey> upResult = convertToDateRange(upRange);
            Assertions.assertTrue(upResult.hasUpperBound());
            Assertions.assertTrue(upResult.upperEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(upResult.upperEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral date = (DateLiteral) upResult.upperEndpoint().getKeys().get(0);
            Assertions.assertEquals(2025, date.getYear());
            Assertions.assertEquals(10, date.getMonth());
            Assertions.assertEquals(10, date.getDay());
            Assertions.assertEquals(0, date.getHour());
        }
        {
            LocalDate date1 = LocalDate.of(2025, 10, 1);
            PartitionKey lower = PartitionKey.ofDate(date1);
            Range<PartitionKey> lowRange = Range.atLeast(lower);
            Range<PartitionKey> lowResult = convertToDateRange(lowRange);
            Assertions.assertTrue(lowResult.hasLowerBound());
            Assertions.assertTrue(lowResult.lowerEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(lowResult.lowerEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral date = (DateLiteral) lowResult.lowerEndpoint().getKeys().get(0);
            Assertions.assertEquals(2025, date.getYear());
            Assertions.assertEquals(10, date.getMonth());
            Assertions.assertEquals(1, date.getDay());
            Assertions.assertEquals(0, date.getHour());
        }
        {
            LocalDate date1 = LocalDate.of(2025, 10, 1);
            PartitionKey lower = PartitionKey.ofDate(date1);
            LocalDate date2 = LocalDate.of(2025, 10, 10);
            PartitionKey upper = PartitionKey.ofDate(date2);
            Range<PartitionKey> range = Range.atLeast(lower);
            range = range.intersection(Range.atMost(upper));
            Range<PartitionKey> result = convertToDateRange(range);
            Assertions.assertTrue(result.hasLowerBound());
            Assertions.assertTrue(result.lowerEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(result.lowerEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral date = (DateLiteral) result.lowerEndpoint().getKeys().get(0);
            Assertions.assertEquals(2025, date.getYear());
            Assertions.assertEquals(10, date.getMonth());
            Assertions.assertEquals(1, date.getDay());
            Assertions.assertEquals(0, date.getHour());

            Assertions.assertTrue(result.hasUpperBound());
            Assertions.assertTrue(result.upperEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(result.upperEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral upperDate = (DateLiteral) result.upperEndpoint().getKeys().get(0);
            Assertions.assertEquals(2025, upperDate.getYear());
            Assertions.assertEquals(10, upperDate.getMonth());
            Assertions.assertEquals(10, upperDate.getDay());
            Assertions.assertEquals(0, upperDate.getHour());
        }
        {
            PartitionKey upper = PartitionKey.ofString("20231010");
            Range<PartitionKey> upRange = Range.atMost(upper);
            Range<PartitionKey> upResult = convertToDateRange(upRange);
            Assertions.assertTrue(upResult.hasUpperBound());
            Assertions.assertTrue(upResult.upperEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(upResult.upperEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral date = (DateLiteral) upResult.upperEndpoint().getKeys().get(0);
            Assertions.assertEquals(2023, date.getYear());
            Assertions.assertEquals(10, date.getMonth());
            Assertions.assertEquals(10, date.getDay());
            Assertions.assertEquals(0, date.getHour());
        }
        {
            PartitionKey lower = PartitionKey.ofString("20231010");
            Range<PartitionKey> lowRange = Range.atLeast(lower);
            Range<PartitionKey> lowResult = convertToDateRange(lowRange);
            Assertions.assertTrue(lowResult.hasLowerBound());
            Assertions.assertTrue(lowResult.lowerEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(lowResult.lowerEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral date = (DateLiteral) lowResult.lowerEndpoint().getKeys().get(0);
            Assertions.assertEquals(2023, date.getYear());
            Assertions.assertEquals(10, date.getMonth());
            Assertions.assertEquals(10, date.getDay());
            Assertions.assertEquals(0, date.getHour());
        }
        {
            PartitionKey lower = PartitionKey.ofString("20231010");
            Range<PartitionKey> range = Range.atLeast(lower);
            range = range.intersection(Range.atMost(PartitionKey.ofString("20231020")));
            Range<PartitionKey> result = convertToDateRange(range);
            Assertions.assertTrue(result.hasLowerBound());
            Assertions.assertTrue(result.lowerEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(result.lowerEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral date = (DateLiteral) result.lowerEndpoint().getKeys().get(0);
            Assertions.assertEquals(2023, date.getYear());
            Assertions.assertEquals(10, date.getMonth());
            Assertions.assertEquals(10, date.getDay());
            Assertions.assertEquals(0, date.getHour());

            Assertions.assertTrue(result.hasUpperBound());
            Assertions.assertTrue(result.upperEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(result.upperEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral upperDate = (DateLiteral) result.upperEndpoint().getKeys().get(0);
            Assertions.assertEquals(2023, upperDate.getYear());
            Assertions.assertEquals(10, upperDate.getMonth());
            Assertions.assertEquals(20, upperDate.getDay());
            Assertions.assertEquals(0, upperDate.getHour());
        }
    }

    @Test
    public void testResetOpAppliedRule() {
        LogicalScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        Operator op = builder.build();
        Assertions.assertFalse(op.isOpRuleBitSet(OP_PARTITION_PRUNED));
        // set
        op.setOpRuleBit(OP_PARTITION_PRUNED);
        Assertions.assertTrue(op.isOpRuleBitSet(OP_PARTITION_PRUNED));
        // reset
        op.resetOpRuleBit(OP_PARTITION_PRUNED);
        Assertions.assertFalse(op.isOpRuleBitSet(OP_PARTITION_PRUNED));
    }

    private final ColumnRefFactory factory = new ColumnRefFactory();
    private final ColumnRefOperator a = factory.create("a", IntegerType.BIGINT, true);
    private final ColumnRefOperator b = factory.create("b", IntegerType.BIGINT, true);
    private final ColumnRefOperator c = factory.create("c", IntegerType.BIGINT, true);
    private final ColumnRefOperator d = factory.create("d", IntegerType.BIGINT, true);

    private static ScalarOperator and(ScalarOperator l, ScalarOperator r) {
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, l, r);
    }

    private static ScalarOperator or(ScalarOperator l, ScalarOperator r) {
        return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, l, r);
    }

    private static ScalarOperator gt(ColumnRefOperator col, long value) {
        return BinaryPredicateOperator.gt(col, ConstantOperator.createBigint(value));
    }

    private static ScalarOperator flagged(ScalarOperator op, boolean pushdown, boolean redundant) {
        ScalarOperator cloned = op.clone();
        cloned.setIsPushdown(pushdown);
        cloned.setRedundant(redundant);
        return cloned;
    }

    private static MysqlTable mysqlTable(long id) {
        MysqlTable table = new MysqlTable();
        table.setId(id);
        return table;
    }

    private static LogicalMysqlScanOperator scan(Table table, ScalarOperator predicate,
                                                 ColumnRefOperator... columns) {
        Map<ColumnRefOperator, Column> colRefToColumn = Maps.newLinkedHashMap();
        Map<Column, ColumnRefOperator> columnToColRef = Maps.newLinkedHashMap();
        for (ColumnRefOperator column : columns) {
            Column meta = new Column(column.getName(), IntegerType.BIGINT);
            colRefToColumn.put(column, meta);
            columnToColRef.put(meta, column);
        }
        return new LogicalMysqlScanOperator(table, colRefToColumn, columnToColRef, Operator.DEFAULT_LIMIT,
                predicate, null);
    }

    @Test
    public void testGetAllValidPredicatesOfScalarFlattensAndFiltersFlags() {
        Assertions.assertTrue(MvUtils.getAllValidPredicates((ScalarOperator) null).isEmpty());

        ScalarOperator p1 = gt(a, 1);
        ScalarOperator pushed = flagged(gt(b, 2), true, false);
        ScalarOperator redundant = flagged(gt(c, 3), false, true);
        ScalarOperator disjunction = or(gt(a, 5), flagged(gt(b, 6), true, true));
        ScalarOperator p5 = gt(d, 7);
        // ((p1 AND pushed) AND (redundant AND disjunction)) AND p5
        ScalarOperator root = and(and(and(p1, pushed), and(redundant, disjunction)), p5);

        // We expect every nested AND to be flattened and the pushdown or redundant conjuncts to be dropped.
        // An OR is one conjunct, so the flags of its children do not matter.
        Assertions.assertEquals(Set.of(p1, disjunction, p5), MvUtils.getAllValidPredicates(root));
        Assertions.assertEquals(Set.of(p1), MvUtils.getAllValidPredicates(p1));
        Assertions.assertTrue(MvUtils.getAllValidPredicates(pushed).isEmpty());
        ScalarOperator flaggedOr = flagged(or(gt(a, 1), gt(b, 2)), false, true);
        Assertions.assertTrue(MvUtils.getAllValidPredicates(flaggedOr).isEmpty());
    }

    @Test
    public void testGetAllValidPredicatesFromScansDropsFlaggedCopies() {
        // equals() ignores the pushdown and redundant flags, so the two copies are one set element.
        ScalarOperator valid = gt(a, 1);
        ScalarOperator redundantCopy = flagged(valid, false, true);
        Assertions.assertEquals(valid, redundantCopy);
        ScalarOperator other = gt(b, 2);
        ScalarOperator onlyPushed = flagged(gt(c, 3), true, false);

        // The valid copy is seen first and is kept.
        OptExpression validFirst = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null),
                OptExpression.create(scan(mysqlTable(1), and(valid, other), a, b)),
                OptExpression.create(scan(mysqlTable(2), and(redundantCopy, onlyPushed), c, d)));
        Set<ScalarOperator> actual = MvUtils.getAllValidPredicatesFromScans(validFirst);
        Assertions.assertEquals(Set.of(valid, other), actual);
        for (ScalarOperator predicate : actual) {
            Assertions.assertTrue(MvUtils.isValidPredicate(predicate));
        }

        OptExpression noPredicate = OptExpression.create(scan(mysqlTable(1), null, a));
        Assertions.assertTrue(MvUtils.getAllValidPredicatesFromScans(noPredicate).isEmpty());
    }

    @Test
    public void testGetPredicateForRewriteSkipsNotNullOnJoinKeys() {
        ScalarOperator aNotNull = new IsNullPredicateOperator(true, a);
        ScalarOperator bNotNull = new IsNullPredicateOperator(true, b);
        ScalarOperator cNotNull = new IsNullPredicateOperator(true, c);
        ScalarOperator dIsNull = new IsNullPredicateOperator(false, d);
        ScalarOperator pushedNotNull = flagged(new IsNullPredicateOperator(true, d), true, false);
        ScalarOperator leftPred = and(and(aNotNull, gt(a, 1)), and(bNotNull, pushedNotNull));
        ScalarOperator rightPred = and(cNotNull, and(dIsNull, gt(c, 3)));
        ScalarOperator onPred = BinaryPredicateOperator.eq(a, c);
        ScalarOperator joinPred = flagged(gt(d, 9), false, true);

        OptExpression left = OptExpression.create(scan(mysqlTable(1), leftPred, a, b));
        OptExpression right = OptExpression.create(scan(mysqlTable(2), rightPred, c, d));
        LogicalJoinOperator joinOp = new LogicalJoinOperator(JoinOperator.INNER_JOIN, onPred);
        joinOp.setPredicate(and(gt(b, 11), joinPred));
        OptExpression join = OptExpression.create(joinOp, left, right);
        MvUtils.deriveLogicalProperty(join);

        // a and c are inner join keys, so their IS NOT NULL is implied by the join and is dropped.
        // b is not a join key, so its IS NOT NULL is kept.
        Assertions.assertEquals(Set.of(gt(a, 1), bNotNull, dIsNull, gt(c, 3), onPred, gt(b, 11)),
                MvUtils.getPredicateForRewrite(join));

        // A left outer join does not reject NULL keys, so every valid IS NOT NULL is kept.
        OptExpression outer = OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, onPred),
                left, right);
        MvUtils.deriveLogicalProperty(outer);
        Assertions.assertEquals(Set.of(gt(a, 1), aNotNull, bNotNull, cNotNull, dIsNull, gt(c, 3), onPred),
                MvUtils.getPredicateForRewrite(outer));

        Assertions.assertEquals(Set.of(aNotNull, gt(a, 1), bNotNull), MvUtils.getPredicateForRewrite(left));
    }

    @Test
    public void testGetPredicateForRewritePropagatesJoinKeysToDescendants() {
        ColumnRefOperator e = factory.create("e", IntegerType.BIGINT, true);
        ScalarOperator aNotNull = new IsNullPredicateOperator(true, a);
        ScalarOperator bNotNull = new IsNullPredicateOperator(true, b);
        ScalarOperator cNotNull = new IsNullPredicateOperator(true, c);
        ScalarOperator dNotNull = new IsNullPredicateOperator(true, d);
        ScalarOperator eNotNull = new IsNullPredicateOperator(true, e);
        OptExpression s1 = OptExpression.create(scan(mysqlTable(1), and(aNotNull, bNotNull), a, b));
        OptExpression s2 = OptExpression.create(scan(mysqlTable(2), and(cNotNull, dNotNull), c, d));
        OptExpression s3 = OptExpression.create(scan(mysqlTable(3), eNotNull, e));
        ScalarOperator lowerOn = BinaryPredicateOperator.eq(a, c);
        ScalarOperator upperOn = BinaryPredicateOperator.eq(b, e);
        OptExpression lower = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, lowerOn), s1, s2);
        OptExpression upper = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, upperOn), lower, s3);
        MvUtils.deriveLogicalProperty(upper);

        // The key b of the upper join must reach the scan below the lower join. d is not a key of any join.
        Assertions.assertEquals(Set.of(dNotNull, lowerOn, upperOn), MvUtils.getPredicateForRewrite(upper));
    }

    @Test
    public void testIsSupportViewDelta() {
        OptExpression s1 = OptExpression.create(scan(mysqlTable(1), null, a));
        OptExpression s2 = OptExpression.create(scan(mysqlTable(2), null, b));
        Assertions.assertTrue(MvUtils.isSupportViewDelta(s1));

        OptExpression inner = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, null), s1, s2);
        OptExpression leftOuter = OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, null),
                inner, s2);
        Assertions.assertTrue(MvUtils.isSupportViewDelta(inner));
        Assertions.assertTrue(MvUtils.isSupportViewDelta(leftOuter));

        for (JoinOperator unsupported : new JoinOperator[] {JoinOperator.RIGHT_OUTER_JOIN, JoinOperator.FULL_OUTER_JOIN,
                JoinOperator.LEFT_SEMI_JOIN, JoinOperator.LEFT_ANTI_JOIN, JoinOperator.CROSS_JOIN}) {
            OptExpression bad = OptExpression.create(new LogicalJoinOperator(unsupported, null), s1, s2);
            Assertions.assertFalse(MvUtils.isSupportViewDelta(bad), unsupported.toString());
            // We expect the unsupported join to be found at any depth, in the left and in the right subtree.
            OptExpression onLeft = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, null), bad, s2);
            OptExpression onRight = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, null), s1,
                    OptExpression.create(new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, null), s1, bad));
            Assertions.assertFalse(MvUtils.isSupportViewDelta(onLeft), unsupported.toString());
            Assertions.assertFalse(MvUtils.isSupportViewDelta(onRight), unsupported.toString());
        }
    }

    @Test
    public void testGetTableScanDescsNumbersRepeatedTables() {
        MysqlTable table1 = mysqlTable(1);
        MysqlTable table2 = mysqlTable(2);
        ColumnRefOperator a2 = factory.create("a2", IntegerType.BIGINT, true);
        ColumnRefOperator b2 = factory.create("b2", IntegerType.BIGINT, true);
        ColumnRefOperator c2 = factory.create("c2", IntegerType.BIGINT, true);
        int relation1 = factory.getNextRelationId();
        int relation2 = factory.getNextRelationId();
        int relation3 = factory.getNextRelationId();
        int relation4 = factory.getNextRelationId();
        factory.updateColumnToRelationIds(a.getId(), relation1);
        factory.updateColumnToRelationIds(c.getId(), relation2);
        factory.updateColumnToRelationIds(a2.getId(), relation3);
        factory.updateColumnToRelationIds(b2.getId(), relation3);
        factory.updateColumnToRelationIds(c2.getId(), relation4);

        LogicalMysqlScanOperator scanA = scan(table1, null, a, b);
        LogicalMysqlScanOperator scanC = scan(table2, null, c, d);
        LogicalMysqlScanOperator scanA2 = scan(table1, null, a2, b2);
        LogicalMysqlScanOperator scanC2 = scan(table2, null, c2);
        OptExpression lower = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, null),
                OptExpression.create(scanA), OptExpression.create(scanC));
        OptExpression middle = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, null), lower,
                OptExpression.create(scanA2));
        OptExpression upper = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, null), middle,
                OptExpression.create(scanC2));

        // Each table is scanned twice. We expect the scans in tree order, numbered per table from 0,
        // with the relation id of their columns and the join right above them.
        List<TableScanDesc> descs = MvUtils.getTableScanDescs(upper, factory);
        Assertions.assertEquals(4, descs.size());
        Assertions.assertSame(scanA, descs.get(0).getScanOperator());
        Assertions.assertSame(scanC, descs.get(1).getScanOperator());
        Assertions.assertSame(scanA2, descs.get(2).getScanOperator());
        Assertions.assertSame(scanC2, descs.get(3).getScanOperator());
        Assertions.assertEquals(List.of(0, 0, 1, 1),
                descs.stream().map(TableScanDesc::getIndex).collect(Collectors.toList()));
        Assertions.assertEquals(List.of(relation1, relation2, relation3, relation4),
                descs.stream().map(TableScanDesc::getRelationid).collect(Collectors.toList()));
        Assertions.assertSame(lower, descs.get(0).getJoinOptExpression());
        Assertions.assertSame(lower, descs.get(1).getJoinOptExpression());
        Assertions.assertSame(middle, descs.get(2).getJoinOptExpression());
        Assertions.assertSame(upper, descs.get(3).getJoinOptExpression());
    }
}
