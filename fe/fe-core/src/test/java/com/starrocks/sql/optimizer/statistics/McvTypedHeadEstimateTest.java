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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The full estimate over an MCV list of seven columns is the same, bit for bit, whether the conditions read the head
 * values converted to the type or evaluate the stored text, for types and constants whose text forms disagree.
 */
public class McvTypedHeadEstimateTest {
    private static final Type DEC = new DecimalType(PrimitiveType.DECIMAL128, 38, 10);
    private static final Type DEC_SMALL = new DecimalType(PrimitiveType.DECIMAL64, 10, 2);
    private static final ColumnRefOperator S = new ColumnRefOperator(11, VarcharType.VARCHAR, "s", true);
    private static final ColumnRefOperator B = new ColumnRefOperator(12, BooleanType.BOOLEAN, "b", true);
    private static final ColumnRefOperator L = new ColumnRefOperator(13, IntegerType.LARGEINT, "l", true);
    private static final ColumnRefOperator D = new ColumnRefOperator(14, FloatType.DOUBLE, "d", true);
    private static final ColumnRefOperator M = new ColumnRefOperator(15, DEC, "m", true);
    private static final ColumnRefOperator T = new ColumnRefOperator(16, DateType.DATETIME, "t", true);
    private static final ColumnRefOperator Y = new ColumnRefOperator(17, DateType.DATE, "y", true);
    private static final ColumnRefOperator O = new ColumnRefOperator(18, IntegerType.INT, "o", true);
    private static final List<ColumnRefOperator> GROUP = List.of(S, B, L, D, M, T, Y);
    private static final String[][] VALUES = {
            {"approved", "A", "A ", "a", "😀", "中文😀", "\ud800", "", "é", "1"},
            {"1", "0", "true", "false", "TRUE"},
            {"170141183460469231731687303715884105727", "-1", "0", "9007199254740993", "9007199254740992", "1"},
            {"-0.0", "0", "0.0", "1.5", "NaN", "1e3", "1000", "Infinity"},
            {"1.5000000000", "1.5", "0.0000000001", "-0", "12345678901234567890.1234567890", "2"},
            {"2025-01-02 03:04:05", "2025-01-02 00:00:00", "2025-01-02", "2025-01-03 00:00:00"},
            {"2025-01-02", "2025-1-2", "2025-01-03", "bad"}};
    private static final double ROWS = 100_000;

    @BeforeAll
    public static void beforeAll() {
        UtFrameUtils.createDefaultCtx();
    }

    private static Statistics statistics(Random random, boolean complete) {
        Set<List<String>> tuples = new LinkedHashSet<>();
        for (int i = 0; i < 3000 && tuples.size() < 200; i++) {
            List<String> tuple = new ArrayList<>();
            for (int position = 0; position < GROUP.size(); position++) {
                String[] values = VALUES[position];
                tuple.add(random.nextInt(20) == 0 ? null : values[random.nextInt(values.length)]);
            }
            tuples.add(tuple);
        }
        List<List<String>> ordered = new ArrayList<>(tuples);
        long head = complete ? (long) ROWS : (long) (ROWS * 0.8);
        long[] counts = new long[ordered.size()];
        long assigned = 0;
        for (int t = 0; t < ordered.size(); t++) {
            counts[t] = Math.max(1, head / (3L * (t + 1)));
            assigned += counts[t];
        }
        counts[0] += Math.max(0, head - assigned);
        List<Map<String, Long>> marginals = new ArrayList<>();
        for (int position = 0; position < GROUP.size(); position++) {
            marginals.add(new HashMap<>());
            for (int t = 0; t < ordered.size(); t++) {
                marginals.get(position).merge(ordered.get(t).get(position), counts[t], Long::sum);
            }
        }
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        for (int t = 0; t < ordered.size(); t++) {
            List<Long> components = new ArrayList<>();
            for (int position = 0; position < GROUP.size(); position++) {
                components.add(marginals.get(position).get(ordered.get(t).get(position)));
            }
            entries.add(new MultiColumnCombinedStats.McvEntry(ordered.get(t), counts[t], components));
        }
        List<Long> nullCounts = new ArrayList<>();
        for (int position = 0; position < GROUP.size(); position++) {
            nullCounts.add(marginals.get(position).getOrDefault(null, 0L));
        }
        Statistics.Builder builder = Statistics.builder().setOutputRowCount(ROWS);
        builder.addColumnStatistic(S, ColumnStatistic.builder().setDistinctValuesCount(10)
                .setNullsFraction(0.05).setAverageRowSize(8).build());
        builder.addColumnStatistic(B, ColumnStatistic.builder().setMinValue(0).setMaxValue(1)
                .setDistinctValuesCount(2).setNullsFraction(0.05).setAverageRowSize(1).build());
        builder.addColumnStatistic(L, ColumnStatistic.builder().setMinValue(-1).setMaxValue(1.7e38)
                .setDistinctValuesCount(6).setNullsFraction(0.05).setAverageRowSize(16).build());
        builder.addColumnStatistic(D, ColumnStatistic.builder().setMinValue(0).setMaxValue(1000)
                .setDistinctValuesCount(6).setNullsFraction(0.05).setAverageRowSize(8).build());
        builder.addColumnStatistic(M, ColumnStatistic.builder().setMinValue(-1).setMaxValue(1.3e19)
                .setDistinctValuesCount(5).setNullsFraction(0.05).setAverageRowSize(16).build());
        builder.addColumnStatistic(T, ColumnStatistic.builder()
                .setMinValue(Utils.getLongFromDateTime(LocalDateTime.of(2025, 1, 2, 0, 0)))
                .setMaxValue(Utils.getLongFromDateTime(LocalDateTime.of(2025, 1, 3, 0, 0)))
                .setDistinctValuesCount(3).setNullsFraction(0.05).setAverageRowSize(8).build());
        builder.addColumnStatistic(Y, ColumnStatistic.builder()
                .setMinValue(Utils.getLongFromDateTime(LocalDateTime.of(2025, 1, 2, 0, 0)))
                .setMaxValue(Utils.getLongFromDateTime(LocalDateTime.of(2025, 1, 3, 0, 0)))
                .setDistinctValuesCount(2).setNullsFraction(0.05).setAverageRowSize(4).build());
        builder.addColumnStatistic(O, ColumnStatistic.builder().setMinValue(0).setMaxValue(100)
                .setDistinctValuesCount(100).setNullsFraction(0).setAverageRowSize(4).build());
        builder.addMultiColumnStatistics(Set.copyOf(GROUP),
                new MultiColumnCombinedStats(400, ROWS, GROUP, entries, nullCounts));
        return builder.build();
    }

    private static <V> V pick(Random random, List<V> values) {
        return values.get(random.nextInt(values.size()));
    }

    private static ConstantOperator constant(Random random, ColumnRefOperator column) {
        if (column == S) {
            List<String> texts = new ArrayList<>(List.of(VALUES[0]));
            texts.add("absent");
            texts.add("中");
            return ConstantOperator.createVarchar(pick(random, texts));
        } else if (column == B) {
            return random.nextInt(4) == 0 ? ConstantOperator.createInt(random.nextInt(3))
                    : ConstantOperator.createBoolean(random.nextBoolean());
        } else if (column == L) {
            switch (random.nextInt(4)) {
                case 0:
                    return ConstantOperator.createBigint(9007199254740993L);
                case 1:
                    return ConstantOperator.createDouble(9007199254740992.0);
                default:
                    return ConstantOperator.createLargeInt(new BigInteger(pick(random, List.of(VALUES[2]))));
            }
        } else if (column == D) {
            switch (random.nextInt(4)) {
                case 0:
                    return ConstantOperator.createFloat(1.5);
                case 1:
                    return ConstantOperator.createDecimal(new BigDecimal("1.50"), DEC_SMALL);
                default:
                    return ConstantOperator.createDouble(pick(random, List.of(-0.0, 0.0, 1.5, 1000.0, 1e3 + 1e-9)));
            }
        } else if (column == M) {
            switch (random.nextInt(4)) {
                case 0:
                    return ConstantOperator.createDecimal(new BigDecimal("1.50"), DEC_SMALL);
                case 1:
                    return ConstantOperator.createDecimal(new BigDecimal("1.5"), DEC);
                case 2:
                    return ConstantOperator.createInt(2);
                default:
                    return ConstantOperator.createDecimal(new BigDecimal("1.5000000000"), DEC);
            }
        } else if (column == T) {
            switch (random.nextInt(3)) {
                case 0:
                    return ConstantOperator.createDate(LocalDateTime.of(2025, 1, 2, 0, 0));
                case 1:
                    return ConstantOperator.createVarchar("2025-01-02 03:04:05");
                default:
                    return ConstantOperator.createDatetime(LocalDateTime.of(2025, 1, 2, 3, 4, random.nextInt(2) + 4));
            }
        } else if (column == Y) {
            switch (random.nextInt(3)) {
                case 0:
                    return ConstantOperator.createDatetime(LocalDateTime.of(2025, 1, 2, 0, 0, random.nextInt(2)));
                case 1:
                    return ConstantOperator.createVarchar("2025-01-02");
                default:
                    return ConstantOperator.createDate(LocalDateTime.of(2025, 1, 2 + random.nextInt(2), 0, 0));
            }
        }
        return ConstantOperator.createInt(random.nextInt(100));
    }

    private static ScalarOperator condition(Random random) {
        ColumnRefOperator column = random.nextInt(10) == 0 ? O : pick(random, GROUP);
        if (column == B && random.nextInt(3) == 0) {
            // WHERE b, the form a boolean equality with TRUE takes.
            return B;
        }
        ScalarOperator left = column;
        if (random.nextInt(6) == 0 && (column == D || column == M || column == L)) {
            left = new CastOperator(VarcharType.VARCHAR, column);
            return new BinaryPredicateOperator(pick(random, List.of(BinaryType.EQ, BinaryType.GE)), left,
                    ConstantOperator.createVarchar(pick(random, List.of("1.5", "1000", "0", "-1"))));
        }
        switch (random.nextInt(8)) {
            case 0:
            case 1:
            case 2: {
                BinaryType type = pick(random, List.of(BinaryType.EQ, BinaryType.NE, BinaryType.LT, BinaryType.LE,
                        BinaryType.GT, BinaryType.GE));
                return new BinaryPredicateOperator(type, left, constant(random, column));
            }
            case 3:
            case 4: {
                List<ScalarOperator> children = new ArrayList<>();
                children.add(left);
                for (int i = 1 + random.nextInt(3); i > 0; i--) {
                    children.add(constant(random, column));
                }
                return new InPredicateOperator(random.nextBoolean(), children);
            }
            case 5:
                if (column == S) {
                    return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, left,
                            ConstantOperator.createVarchar(pick(random, List.of("A%", "%😀", "_", "a_"))));
                }
                return new IsNullPredicateOperator(random.nextBoolean(), left);
            default:
                return new IsNullPredicateOperator(random.nextBoolean(), left);
        }
    }

    private static ScalarOperator predicate(Random random, int depth) {
        int choice = depth >= 2 ? 0 : random.nextInt(6);
        switch (choice) {
            case 1:
            case 2: {
                List<ScalarOperator> conjuncts = new ArrayList<>();
                for (int i = 2 + random.nextInt(3); i > 0; i--) {
                    conjuncts.add(predicate(random, depth + 1));
                }
                return Utils.compoundAnd(conjuncts);
            }
            case 3:
                return Utils.compoundOr(predicate(random, depth + 1), predicate(random, depth + 1));
            case 4:
                return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                        predicate(random, depth + 1));
            default:
                return condition(random);
        }
    }

    private static String estimate(ScalarOperator predicate, Statistics statistics, boolean useMcv) {
        try {
            return McvEstimateEquivalenceTest.describe(
                    PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics, useMcv));
        } catch (RuntimeException e) {
            return "failed " + e.getClass().getName();
        }
    }

    // The full estimate, with and without the MCV list, is the same bit for bit whether the conditions read the head
    // values converted to the type or evaluate the stored text: a failing read of a converted value makes the
    // evaluator take the text.
    @Test
    public void testTypedHeadGivesTheFullEstimateOfTheTextEvaluation() {
        Random random = new Random(1014);
        List<ScalarOperator> predicates = new ArrayList<>();
        List<Statistics> inputs = new ArrayList<>();
        List<String> typed = new ArrayList<>();
        for (int round = 0; round < 30; round++) {
            Statistics statistics = statistics(random, round % 3 == 0);
            for (int i = 0; i < 60; i++) {
                ScalarOperator predicate = predicate(random, 0);
                predicates.add(predicate);
                inputs.add(statistics);
                typed.add(estimate(predicate, statistics, true) + "\n--plain--\n"
                        + estimate(predicate, statistics, false));
            }
        }
        AtomicLong reads = new AtomicLong();
        new MockUp<PreparedMcvTuples>() {
            @Mock
            McvPredicateEvaluator.TypedValue typedValue(int position, Type type, String value) {
                reads.incrementAndGet();
                throw new IllegalStateException("the text evaluation");
            }
        };
        int differentRows = 0;
        Set<String> rowCounts = new HashSet<>();
        for (int i = 0; i < predicates.size(); i++) {
            String text = estimate(predicates.get(i), inputs.get(i), true) + "\n--plain--\n"
                    + estimate(predicates.get(i), inputs.get(i), false);
            Assertions.assertEquals(text, typed.get(i), predicates.get(i).toString());
            rowCounts.add(text.substring(0, text.indexOf('\n')));
            if (!text.startsWith("rows " + Long.toHexString(Double.doubleToRawLongBits(ROWS)))) {
                differentRows++;
            }
        }
        // The typed values must have been read, and the predicates must narrow the rows in many ways.
        Assertions.assertTrue(reads.get() > 10_000, Long.toString(reads.get()));
        Assertions.assertTrue(differentRows > 500 && rowCounts.size() > 100, differentRows + " " + rowCounts.size());
    }
}
