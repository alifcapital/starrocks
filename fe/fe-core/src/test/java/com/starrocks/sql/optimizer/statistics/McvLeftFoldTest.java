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

import com.starrocks.catalog.Function;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * left(c, n) and strleft(c, n) on a column of an MCV head are evaluated on the head values as substr(c, 1, n), which
 * the BE computes for them, so the head answers them and the group is not dropped from the estimate.
 */
public class McvLeftFoldTest {
    private static final double ROWS = 10_000;
    private static final ColumnRefOperator STATUS = new ColumnRefOperator(1, VarcharType.VARCHAR, "status", true);
    private static final ColumnRefOperator TYPE = new ColumnRefOperator(2, IntegerType.INT, "type", true);
    private static final ColumnRefOperator OPERABLE = new ColumnRefOperator(3, VarcharType.VARCHAR, "operable", true);

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.createDefaultCtx();
    }

    private static ScalarOperator call(String name, ScalarOperator... arguments) {
        Type[] types = new Type[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            types[i] = arguments[i].getType();
        }
        Function function = ExprUtils.getBuiltinFunction(name, types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(name, VarcharType.VARCHAR, List.of(arguments), function);
    }

    private static ScalarOperator left(ColumnRefOperator column, int length) {
        return call("left", column, ConstantOperator.createInt(length));
    }

    private static ScalarOperator strleft(ColumnRefOperator column, int length) {
        return call("strleft", column, ConstantOperator.createInt(length));
    }

    private static ScalarOperator substr(ColumnRefOperator column, int length) {
        return call("substr", column, ConstantOperator.createInt(1), ConstantOperator.createInt(length));
    }

    private static ConstantOperator text(String value) {
        return ConstantOperator.createVarchar(value);
    }

    private static ScalarOperator eq(ScalarOperator left, String value) {
        return new BinaryPredicateOperator(BinaryType.EQ, left, text(value));
    }

    private static MultiColumnCombinedStats.McvEntry entry(String status, String type, long count, long statusRows,
                                                           long typeRows) {
        return new MultiColumnCombinedStats.McvEntry(Arrays.asList(status, type), count, List.of(statusRows, typeRows));
    }

    // The head holds every row. approved 7000 rows, failed 900, pending 2000 and 100 NULL; type 3 in 7600 and 9 in 2400.
    private static Statistics group() {
        List<MultiColumnCombinedStats.McvEntry> head = List.of(
                entry("approved", "3", 5000, 7000, 7600),
                entry("failed", "3", 500, 900, 7600),
                entry("failed", "9", 400, 900, 2400),
                entry("approved", "9", 2000, 7000, 2400),
                entry("pending", "3", 2000, 2000, 7600),
                entry(null, "3", 100, 100, 7600));
        return Statistics.builder().setOutputRowCount(ROWS)
                .addColumnStatistic(STATUS, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0.01,
                        8, 3))
                .addColumnStatistic(TYPE, new ColumnStatistic(3, 9, 0, 4, 2))
                .addMultiColumnStatistics(Set.of(STATUS, TYPE), new MultiColumnCombinedStats(6, (long) ROWS,
                        List.of(STATUS, TYPE), head, List.of(100L, 0L))).build();
    }

    // The head of one column holds every row: credit_operation 600, credit_card 200, cash_operation 150, debit 50.
    private static Statistics single() {
        List<MultiColumnCombinedStats.McvEntry> head = List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("credit_operation"), 600, List.of(600L)),
                new MultiColumnCombinedStats.McvEntry(List.of("credit_card"), 200, List.of(200L)),
                new MultiColumnCombinedStats.McvEntry(List.of("cash_operation"), 150, List.of(150L)),
                new MultiColumnCombinedStats.McvEntry(List.of("debit"), 50, List.of(50L)));
        ExternalMcvStatistics.Group stored = new ExternalMcvStatistics.Group(List.of("operable"), 1000, 4, head,
                List.of(), List.of(0L));
        stored.prepare(VarcharType.VARCHAR);
        ColumnStatistic statistic = stored.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        return Statistics.builder().setOutputRowCount(1000).addColumnStatistic(OPERABLE, statistic)
                .addMultiColumnStatistics(Set.of(OPERABLE), new MultiColumnCombinedStats(4, 1000, List.of(OPERABLE),
                        head, List.of(0L))).build();
    }

    private static double rows(ScalarOperator predicate, Statistics statistics) {
        return PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics).getOutputRowCount();
    }

    @Test
    public void testLeftOnAColumnOfAGroup() {
        // failed holds 500 + 400 rows; the NULL tuple is not among them.
        Assertions.assertEquals(900, rows(eq(left(STATUS, 4), "fail"), group()), 1e-6);
        Assertions.assertEquals(900, rows(eq(strleft(STATUS, 4), "fail"), group()), 1e-6);
        // The same condition written as substr gives the same rows.
        Assertions.assertEquals(rows(eq(substr(STATUS, 4), "fail"), group()), rows(eq(left(STATUS, 4), "fail"), group()),
                1e-6);
        // The prefix of two values: approved and pending are not 'a'.
        Assertions.assertEquals(7000, rows(eq(left(STATUS, 1), "a"), group()), 1e-6);
        Assertions.assertEquals(2900, rows(new InPredicateOperator(false, left(STATUS, 1), text("f"), text("p")), group()),
                1e-6);
        // A constant longer than the prefix matches no value.
        Assertions.assertEquals(1, rows(eq(left(STATUS, 4), "failed"), group()), 1e-6);
    }

    @Test
    public void testLeftWithAConditionOnAnotherColumnOfTheGroup() {
        ScalarOperator failed = eq(left(STATUS, 4), "fail");
        ScalarOperator type3 = new BinaryPredicateOperator(BinaryType.EQ, TYPE, ConstantOperator.createInt(3));
        ScalarOperator type9 = new BinaryPredicateOperator(BinaryType.EQ, TYPE, ConstantOperator.createInt(9));
        // The tuple (failed, 3).
        Assertions.assertEquals(500, rows(Utils.compoundAnd(failed, type3), group()), 1e-6);
        // 900 rows of failed and 2400 of type 9, 400 of them in both.
        Assertions.assertEquals(2900, rows(Utils.compoundOr(failed, type9), group()), 1e-6);
        Assertions.assertEquals(2900, rows(Utils.compoundOr(type9, failed), group()), 1e-6);
    }

    @Test
    public void testLeftOnASingleColumnWithAHead() {
        Assertions.assertEquals(800, rows(eq(left(OPERABLE, 6), "credit"), single()), 1e-6);
        Assertions.assertEquals(800, rows(eq(strleft(OPERABLE, 6), "credit"), single()), 1e-6);
        Assertions.assertEquals(950, rows(new InPredicateOperator(false, left(OPERABLE, 6), text("credit"), text("cash_o")),
                single()), 1e-6);
        Assertions.assertEquals(200, rows(new BinaryPredicateOperator(BinaryType.NE, left(OPERABLE, 6), text("credit")),
                single()), 1e-6);
        Assertions.assertEquals(800, rows(new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE,
                left(OPERABLE, 6), text("cre%")), single()), 1e-6);
    }

    @Test
    public void testFoldOnValuesOfTheHead() {
        // The characters are code points, as the BE counts them.
        Assertions.assertEquals("при", fold(left(OPERABLE, 3), "привет"));
        Assertions.assertEquals("日本", fold(left(OPERABLE, 2), "日本語"));
        Assertions.assertEquals("😀😁", fold(strleft(OPERABLE, 2), "😀😁ab"));
        Assertions.assertEquals("ab", fold(left(OPERABLE, 5), "ab"));
        Assertions.assertEquals("", fold(left(OPERABLE, 0), "abc"));
        Assertions.assertEquals("", fold(left(OPERABLE, -1), "abc"));
        Assertions.assertEquals("", fold(left(OPERABLE, 3), ""));
        // NULL of the column gives NULL, as for any value of the head.
        Optional<ConstantOperator> nullValue = MultiColumnMcvEstimator.evaluate(left(OPERABLE, 2), OPERABLE, null);
        Assertions.assertTrue(nullValue.orElseThrow().isNull());
        // The same as substr over the same values.
        for (String value : List.of("approved", "привет", "日本語", "😀😁ab", "")) {
            Assertions.assertEquals(fold(substr(OPERABLE, 3), value), fold(left(OPERABLE, 3), value));
        }
    }

    private static String fold(ScalarOperator expression, String value) {
        return MultiColumnMcvEstimator.evaluate(expression, OPERABLE, value).orElseThrow().getVarchar();
    }
}
