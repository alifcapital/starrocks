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
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

class McvPredicateEvaluatorTest {
    @BeforeAll
    static void beforeAll() {
        UtFrameUtils.createDefaultCtx();
    }

    private void compare(Type type, List<ConstantOperator> constants, List<String> values) {
        ColumnRefOperator column = new ColumnRefOperator(1, type, "k", true);
        for (boolean not : new boolean[] {false, true}) {
            List<ScalarOperator> children = new ArrayList<>();
            children.add(column);
            children.addAll(constants);
            InPredicateOperator predicate = new InPredicateOperator(not, children);
            McvPredicateEvaluator evaluator = new McvPredicateEvaluator();
            Assertions.assertSame(column, evaluator.column(predicate));
            for (String value : values) {
                Assertions.assertEquals(MultiColumnMcvEstimator.matchesComponent(predicate, column, value),
                        evaluator.matchesComponent(predicate, column, value, null, -1),
                        type + " / " + value + " / " + not);
            }
        }
    }

    @Test
    void preparedMembershipPreservesTypedEquality() {
        compare(IntegerType.BIGINT, List.of(ConstantOperator.createBigint(9007199254740993L)),
                Arrays.asList(null, "9007199254740992", "9007199254740993", "9007199254740993.0", "bad"));
        compare(IntegerType.LARGEINT, List.of(ConstantOperator.createLargeInt(
                        new BigInteger("170141183460469231731687303715884105727"))),
                List.of("170141183460469231731687303715884105727", "170141183460469231731687303715884105726"));
        Type decimal = new DecimalType(PrimitiveType.DECIMAL128, 38, 20);
        compare(decimal, List.of(ConstantOperator.createDecimal(new BigDecimal("1.00000000000000000001"), decimal),
                        ConstantOperator.createDecimal(new BigDecimal("1.500"), decimal)),
                Arrays.asList(null, "1.00000000000000000001", "1.00000000000000000002", "1.50", "1.5", "bad"));
        compare(FloatType.DOUBLE, List.of(ConstantOperator.createDouble(-0.0), ConstantOperator.createDouble(1.5)),
                Arrays.asList(null, "-0.0", "0.0", "1.500", "NaN", "Infinity"));
        compare(BooleanType.BOOLEAN, List.of(ConstantOperator.createBoolean(true)),
                Arrays.asList(null, "true", "TRUE", "1", "false", "0"));
        compare(DateType.DATE, List.of(ConstantOperator.createDate(LocalDateTime.of(2025, 1, 2, 0, 0))),
                Arrays.asList(null, "2025-01-02", "2025-01-03"));
        compare(DateType.DATETIME, List.of(ConstantOperator.createDatetime(LocalDateTime.of(2025, 1, 2, 3, 4, 5))),
                List.of("2025-01-02 03:04:05", "2025-01-02 03:04:06"));
        compare(VarcharType.VARCHAR, List.of(ConstantOperator.createVarchar("A"), ConstantOperator.createVarchar("😀"),
                        ConstantOperator.createVarchar("?")),
                Arrays.asList(null, "A", "a", "😀", "\ud800", "?", "", "A "));
    }

    @Test
    void unreadableConstantKeepsShortCircuitAndNullBehavior() {
        compare(IntegerType.INT, List.of(ConstantOperator.createInt(1), ConstantOperator.createVarchar("bad"),
                        ConstantOperator.createInt(2)), Arrays.asList(null, "1", "2", "3", "bad"));
        compare(IntegerType.INT, List.of(), Arrays.asList(null, "1", "bad"));
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        Assertions.assertNull(new McvPredicateEvaluator().column(new InPredicateOperator(false, column,
                ConstantOperator.createNull(IntegerType.INT))));
    }

    @Test
    void castOperandUsesItsResultType() {
        ColumnRefOperator column = new ColumnRefOperator(1, VarcharType.VARCHAR, "k", true);
        ScalarOperator cast = new CastOperator(IntegerType.BIGINT, column);
        for (boolean not : new boolean[] {false, true}) {
            InPredicateOperator predicate = new InPredicateOperator(not, cast, ConstantOperator.createBigint(1));
            McvPredicateEvaluator evaluator = new McvPredicateEvaluator();
            for (String value : Arrays.asList(null, "001", "1", "2", "bad")) {
                Assertions.assertEquals(MultiColumnMcvEstimator.matchesComponent(predicate, column, value),
                        evaluator.matchesComponent(predicate, column, value, null, -1));
            }
        }
    }

    @Test
    void longInIsPreparedOnceThroughEstimatorAndPropagation() {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.BIGINT, "k", true);
        AtomicInteger visits = new AtomicInteger();
        List<ScalarOperator> children = new ArrayList<>();
        children.add(column);
        for (int i = 0; i < 10000; i++) {
            children.add(ConstantOperator.createBigint(i));
        }
        InPredicateOperator predicate = new InPredicateOperator(false, children) {
            @Override
            public ScalarOperator getChild(int index) {
                if (index > 0) {
                    visits.incrementAndGet();
                }
                return super.getChild(index);
            }
        };
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            entries.add(new MultiColumnCombinedStats.McvEntry(List.of(Integer.toString(20000 + i)), 100,
                    List.of(100L)));
        }
        Statistics statistics = Statistics.builder().setOutputRowCount(1000000)
                .addColumnStatistic(column, ColumnStatistic.builder().setMinValue(0).setMaxValue(100000)
                        .setDistinctValuesCount(100000).setNullsFraction(0).setAverageRowSize(8).build())
                .addMultiColumnStatistics(Set.of(column), new MultiColumnCombinedStats(100000, 1000000,
                        List.of(column), entries)).build();
        Assertions.assertEquals(0.1, MultiColumnMcvEstimator.estimate(List.of(predicate), statistics)
                .orElseThrow().getSelectivity(), 1e-12);
        Assertions.assertTrue(visits.get() < 100000, "IN constants were revisited per MCV tuple: " + visits.get());
        visits.set(0);
        Statistics output = PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics);
        Assertions.assertEquals(100000, output.getOutputRowCount(), 1e-6);
        Assertions.assertTrue(visits.get() < 100000, "Propagation rescanned IN per tuple: " + visits.get());
        // Repeated constants must not multiply the exact component share.
        ScalarOperator repeated = new InPredicateOperator(false, column, ConstantOperator.createBigint(20000),
                ConstantOperator.createBigint(20000));
        Assertions.assertEquals(0.0001, MultiColumnMcvEstimator.estimate(List.of(repeated), statistics)
                .orElseThrow().getSelectivity(), 1e-12);
    }

    private static final List<Type> TYPES = List.of(IntegerType.TINYINT, IntegerType.INT, IntegerType.BIGINT,
            IntegerType.LARGEINT, new DecimalType(PrimitiveType.DECIMAL64, 10, 2),
            new DecimalType(PrimitiveType.DECIMAL128, 38, 20), FloatType.DOUBLE, FloatType.FLOAT, BooleanType.BOOLEAN,
            DateType.DATE, DateType.DATETIME, VarcharType.VARCHAR);

    private static final List<String> TEXTS = Arrays.asList(null, "0", "1", "-1", "001", "1.0", "1.50", "1.5", "2",
            "10", "127", "128", "-129", "bad", "", " 1", "1e3", "-0.0", "NaN", "Infinity", "true", "TRUE", "false",
            "2025-01-02", "2025-01-02 03:04:05", "2025-1-2", "9007199254740993", "9007199254740993.0",
            "170141183460469231731687303715884105727", "1.00000000000000000001", "😀", "\ud800", "A", "a", "A ");

    private static ConstantOperator randomConstant(Random random) {
        String text = TEXTS.get(1 + random.nextInt(TEXTS.size() - 1));
        switch (random.nextInt(9)) {
            case 0:
                return ConstantOperator.createInt(random.nextInt(21) - 10);
            case 1:
                return ConstantOperator.createBigint(random.nextBoolean() ? 9007199254740993L : random.nextInt(300));
            case 2:
                return ConstantOperator.createDecimal(new BigDecimal(random.nextInt(2000) - 1000).movePointLeft(2),
                        new DecimalType(PrimitiveType.DECIMAL64, 10, 2));
            case 3:
                return ConstantOperator.createDouble(random.nextBoolean() ? -0.0 : (random.nextInt(40) - 20) / 4.0);
            case 4:
                return ConstantOperator.createBoolean(random.nextBoolean());
            case 5:
                return ConstantOperator.createDate(LocalDateTime.of(2025, 1, 1 + random.nextInt(3), 0, 0));
            case 6:
                return ConstantOperator.createDatetime(LocalDateTime.of(2025, 1, 2, 3, 4, 4 + random.nextInt(3)));
            case 7:
                return ConstantOperator.createLargeInt(new BigInteger("170141183460469231731687303715884105727"));
            default:
                return ConstantOperator.createVarchar(text);
        }
    }

    private static ScalarOperator randomCondition(Random random, ColumnRefOperator column) {
        switch (random.nextInt(4)) {
            case 0:
            case 1: {
                BinaryType[] types = {BinaryType.EQ, BinaryType.NE, BinaryType.LT, BinaryType.LE, BinaryType.GT,
                        BinaryType.GE};
                return new BinaryPredicateOperator(types[random.nextInt(types.length)], column, randomConstant(random));
            }
            case 2: {
                List<ScalarOperator> children = new ArrayList<>();
                children.add(column);
                for (int i = random.nextInt(4); i > 0; i--) {
                    children.add(randomConstant(random));
                }
                return new InPredicateOperator(random.nextBoolean(), children);
            }
            default:
                return new IsNullPredicateOperator(random.nextBoolean(), column);
        }
    }

    // A head of one column holding the given texts, as a scan of the column reads it.
    private static PreparedMcvTuples head(ColumnRefOperator column, List<String> texts) {
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        for (String text : texts) {
            entries.add(new MultiColumnCombinedStats.McvEntry(java.util.Collections.singletonList(text), 10));
        }
        return new MultiColumnCombinedStats(texts.size(), 10L * texts.size(), List.of(column), entries)
                .getMcvDistribution();
    }

    // The converted values of a head give the result of the evaluation of the text without a head for every
    // condition, on the values of the head and on values it does not hold.
    @Test
    void typedHeadValuesMatchThePlainEvaluation() {
        Random random = new Random(20261014);
        for (int round = 0; round < 400; round++) {
            Type type = TYPES.get(random.nextInt(TYPES.size()));
            ColumnRefOperator column = new ColumnRefOperator(1, type, "k", true);
            List<String> held = new ArrayList<>();
            for (String text : TEXTS) {
                if (random.nextBoolean()) {
                    held.add(text);
                }
            }
            PreparedMcvTuples head = head(column, held);
            McvPredicateEvaluator evaluator = new McvPredicateEvaluator();
            McvPredicateEvaluator reference = new McvPredicateEvaluator();
            for (int c = 0; c < 6; c++) {
                ScalarOperator condition = randomCondition(random, column);
                for (String text : TEXTS) {
                    Optional<Boolean> plain = reference.matchesComponent(condition, column, text, null, -1);
                    Assertions.assertEquals(plain, evaluator.matchesComponent(condition, column, text, head, 0),
                            type + " / " + condition + " / " + text);
                    Assertions.assertEquals(McvPredicateEvaluator.operand(column, column, text, null, -1),
                            McvPredicateEvaluator.operand(column, column, text, head, 0), type + " / " + text);
                }
            }
        }
    }

    // Queries that read one shared head with other types of a column convert its values concurrently; each one reads
    // the values converted to its own type.
    @Test
    void typedHeadValuesAreSharedAcrossThreadsAndTypes() throws Exception {
        PreparedMcvTuples[] shared = new PreparedMcvTuples[1];
        shared[0] = head(new ColumnRefOperator(1, VarcharType.VARCHAR, "k", true), TEXTS);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                int seed = t;
                futures.add(pool.submit(() -> {
                    Random random = new Random(seed);
                    for (int round = 0; round < 200; round++) {
                        Type type = TYPES.get(random.nextInt(TYPES.size()));
                        ColumnRefOperator column = new ColumnRefOperator(1, type, "k", true);
                        ScalarOperator condition = randomCondition(random, column);
                        McvPredicateEvaluator evaluator = new McvPredicateEvaluator();
                        McvPredicateEvaluator reference = new McvPredicateEvaluator();
                        for (String text : TEXTS) {
                            Assertions.assertEquals(reference.matchesComponent(condition, column, text, null, -1),
                                    evaluator.matchesComponent(condition, column, text, shared[0], 0),
                                    type + " / " + condition + " / " + text);
                        }
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // The cache weight of a converted value bounds the heap it holds. A text of characters of three UTF-8 bytes with
    // one character outside the basic plane is kept as its UTF-8 bytes and as a copy of the text.
    @Test
    void retainedBytesBoundTheConvertedText() {
        String value = "中".repeat(1000) + "😀";
        Object equality = McvPredicateEvaluator.equalityKey(VarcharType.VARCHAR, VarcharType.VARCHAR, value);
        Assertions.assertNotSame(value, equality);
        long held = 16L + value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length + 16L
                + 2L * ((String) equality).length();
        Assertions.assertTrue(McvPredicateEvaluator.TypedValue.retainedBytes(value) >= held,
                McvPredicateEvaluator.TypedValue.retainedBytes(value) + " < " + held);
    }
}
