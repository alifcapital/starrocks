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
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The shortcuts of the MCV estimate give the same estimate, bit for bit, as the full evaluation. The predicates are
 * random conjunctions, disjunctions and negations of conditions on the columns of a mocked six-column head and on a
 * column outside of it.
 */
public class McvEstimateEquivalenceTest {
    private static final Type DECIMAL = new DecimalType(PrimitiveType.DECIMAL64, 10, 2);
    private static final ColumnRefOperator STATUS = new ColumnRefOperator(1, VarcharType.VARCHAR, "status", true);
    private static final ColumnRefOperator OPERATION = new ColumnRefOperator(2, IntegerType.INT, "operation", true);
    private static final ColumnRefOperator GATE = new ColumnRefOperator(3, IntegerType.SMALLINT, "gate", true);
    private static final ColumnRefOperator KIND = new ColumnRefOperator(4, IntegerType.BIGINT, "kind", true);
    private static final ColumnRefOperator AMOUNT = new ColumnRefOperator(5, DECIMAL, "amount", true);
    private static final ColumnRefOperator DAY = new ColumnRefOperator(6, DateType.DATE, "day", true);
    private static final ColumnRefOperator OUTSIDE = new ColumnRefOperator(7, IntegerType.INT, "outside", true);
    private static final List<ColumnRefOperator> GROUP = List.of(STATUS, OPERATION, GATE, KIND, AMOUNT, DAY);
    private static final String[] STATUSES = {"approved", "declined", "pending", "new"};
    private static final double ROWS = 1_000_000;

    @BeforeAll
    public static void beforeAll() {
        UtFrameUtils.createDefaultCtx();
    }

    private static String value(Random random, int position) {
        if (random.nextInt(40) == 0) {
            return null;
        }
        int skewed = (int) Math.floor(10 * Math.pow(random.nextDouble(), 3));
        switch (position) {
            case 0:
                return STATUSES[Math.min(STATUSES.length - 1, skewed)];
            case 1:
            case 2:
            case 3:
                return Integer.toString(skewed);
            case 4:
                return skewed + ".50";
            default:
                return "2025-01-0" + (1 + skewed % 9);
        }
    }

    // 300 tuples, or fewer when the values repeat, covering the share of the rows the head is given.
    private static Statistics statistics(Random random, boolean completeHead) {
        Set<List<String>> tuples = new LinkedHashSet<>();
        for (int i = 0; i < 2000 && tuples.size() < 300; i++) {
            List<String> tuple = new ArrayList<>();
            for (int position = 0; position < GROUP.size(); position++) {
                tuple.add(value(random, position));
            }
            tuples.add(tuple);
        }
        List<List<String>> ordered = new ArrayList<>(tuples);
        long head = completeHead ? (long) ROWS : (long) (ROWS * 0.9);
        long[] counts = new long[ordered.size()];
        long assigned = 0;
        for (int t = 0; t < ordered.size(); t++) {
            counts[t] = Math.max(1, head / (2L * (t + 1)));
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
        builder.addColumnStatistic(STATUS, ColumnStatistic.builder().setDistinctValuesCount(4)
                .setNullsFraction(0.02).setAverageRowSize(8).build());
        builder.addColumnStatistic(OPERATION, ColumnStatistic.builder().setMinValue(0).setMaxValue(9)
                .setDistinctValuesCount(10).setNullsFraction(0.02).setAverageRowSize(4).build());
        builder.addColumnStatistic(GATE, ColumnStatistic.builder().setMinValue(0).setMaxValue(9)
                .setDistinctValuesCount(10).setNullsFraction(0.02).setAverageRowSize(2).build());
        builder.addColumnStatistic(KIND, ColumnStatistic.builder().setMinValue(0).setMaxValue(9)
                .setDistinctValuesCount(10).setNullsFraction(0.02).setAverageRowSize(8).build());
        builder.addColumnStatistic(AMOUNT, ColumnStatistic.builder().setMinValue(0).setMaxValue(10)
                .setDistinctValuesCount(10).setNullsFraction(0.02).setAverageRowSize(8).build());
        builder.addColumnStatistic(DAY, ColumnStatistic.builder()
                .setMinValue(Utils.getLongFromDateTime(LocalDateTime.of(2025, 1, 1, 0, 0)))
                .setMaxValue(Utils.getLongFromDateTime(LocalDateTime.of(2025, 1, 9, 0, 0)))
                .setDistinctValuesCount(9).setNullsFraction(0.02).setAverageRowSize(4).build());
        builder.addColumnStatistic(OUTSIDE, ColumnStatistic.builder().setMinValue(0).setMaxValue(1000)
                .setDistinctValuesCount(1000).setNullsFraction(0).setAverageRowSize(4).build());
        builder.addMultiColumnStatistics(Set.copyOf(GROUP),
                new MultiColumnCombinedStats(500, ROWS, GROUP, entries, nullCounts));
        return builder.build();
    }

    private static ConstantOperator constant(Random random, ColumnRefOperator column) {
        int skewed = random.nextInt(12);
        if (column == STATUS) {
            return ConstantOperator.createVarchar(random.nextInt(8) == 0 ? "absent" : STATUSES[skewed % 4]);
        } else if (column == OPERATION || column == OUTSIDE) {
            return ConstantOperator.createInt(skewed);
        } else if (column == GATE) {
            return ConstantOperator.createSmallInt((short) skewed);
        } else if (column == KIND) {
            return ConstantOperator.createBigint(skewed);
        } else if (column == AMOUNT) {
            return ConstantOperator.createDecimal(new BigDecimal(skewed + ".5"), DECIMAL);
        }
        return ConstantOperator.createDate(LocalDateTime.of(2025, 1, 1 + skewed % 9, 0, 0));
    }

    private static ScalarOperator condition(Random random) {
        ColumnRefOperator column = random.nextInt(8) == 0 ? OUTSIDE : GROUP.get(random.nextInt(GROUP.size()));
        ScalarOperator left = column;
        if (column == OPERATION && random.nextInt(4) == 0) {
            // A cast of the column, evaluated on each head value.
            left = new CastOperator(IntegerType.BIGINT, column);
            switch (random.nextInt(3)) {
                case 0:
                    return new BinaryPredicateOperator(BinaryType.EQ, left,
                            ConstantOperator.createBigint(random.nextInt(10)));
                case 1:
                    return new InPredicateOperator(random.nextBoolean(), left,
                            ConstantOperator.createBigint(random.nextInt(10)),
                            ConstantOperator.createBigint(random.nextInt(10)));
                default:
                    return new BinaryPredicateOperator(BinaryType.GE, left,
                            ConstantOperator.createBigint(random.nextInt(10)));
            }
        }
        switch (random.nextInt(7)) {
            case 0:
            case 1:
                return new BinaryPredicateOperator(BinaryType.EQ, left, constant(random, column));
            case 2:
                return new BinaryPredicateOperator(BinaryType.NE, left, constant(random, column));
            case 3: {
                BinaryType[] ranges = {BinaryType.LT, BinaryType.LE, BinaryType.GT, BinaryType.GE};
                return new BinaryPredicateOperator(ranges[random.nextInt(ranges.length)], left,
                        constant(random, column));
            }
            case 4: {
                List<ScalarOperator> children = new ArrayList<>();
                children.add(left);
                for (int i = 1 + random.nextInt(3); i > 0; i--) {
                    children.add(constant(random, column));
                }
                return new InPredicateOperator(random.nextBoolean(), children);
            }
            case 5:
                if (column == STATUS) {
                    return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, left,
                            ConstantOperator.createVarchar(random.nextBoolean() ? "app%" : "%e%"));
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
                for (int i = 2 + random.nextInt(5); i > 0; i--) {
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

    private static String bits(double value) {
        return Long.toHexString(Double.doubleToRawLongBits(value));
    }

    private static String describe(ColumnStatistic statistic) {
        StringBuilder text = new StringBuilder();
        text.append(bits(statistic.getMinValue())).append(' ').append(bits(statistic.getMaxValue())).append(' ')
                .append(bits(statistic.getNullsFraction())).append(' ').append(bits(statistic.getAverageRowSize()))
                .append(' ').append(bits(statistic.getDistinctValuesCount())).append(' ')
                .append(bits(statistic.getSourceDistinctValuesCount())).append(' ')
                .append(statistic.isDistinctValuesFromDefaultSelectivity()).append(' ').append(statistic.getType())
                .append(' ').append(statistic.getMinString()).append(' ').append(statistic.getMaxString());
        Histogram histogram = statistic.getHistogram();
        if (histogram != null) {
            text.append(" histogram ").append(new TreeMap<>(histogram.getMCV()));
            for (Bucket bucket : histogram.getBuckets()) {
                text.append(" [").append(bits(bucket.getLower())).append(',').append(bits(bucket.getUpper()))
                        .append(',').append(bucket.getCount()).append(',').append(bucket.getUpperRepeats())
                        .append(',').append(bucket.getDistinctCount()).append(']');
            }
        }
        return text.toString();
    }

    /** Everything an estimate gives, with the doubles as their bits. */
    static String describe(Statistics statistics) {
        StringBuilder text = new StringBuilder();
        text.append("rows ").append(bits(statistics.getOutputRowCount())).append(" inaccurate ")
                .append(statistics.isTableRowCountMayInaccurate()).append('\n');
        statistics.getColumnStatistics().entrySet().stream()
                .sorted(Comparator.comparingInt(entry -> entry.getKey().getId()))
                .forEach(entry -> text.append(entry.getKey().getId()).append(": ")
                        .append(describe(entry.getValue())).append('\n'));
        statistics.getMultiColumnCombinedStats().entrySet().stream()
                .map(entry -> {
                    MultiColumnCombinedStats group = entry.getValue();
                    StringBuilder line = new StringBuilder();
                    line.append(entry.getKey().stream().map(ColumnRefOperator::getId).sorted().toList())
                            .append(" ndv ").append(group.getNdv()).append(" rows ").append(bits(group.getRowCount()))
                            .append(" columns ").append(group.getColumns().stream()
                                    .map(column -> column == null ? "-" : Integer.toString(column.getId())).toList())
                            .append(" nulls ").append(group.getNullCounts());
                    for (MultiColumnCombinedStats.McvEntry mcv : group.getMcv()) {
                        line.append(' ').append(mcv.getValues()).append(':').append(mcv.getCount())
                                .append(mcv.getComponentCounts());
                    }
                    return line.toString();
                })
                .sorted()
                .forEach(line -> text.append(line).append('\n'));
        return text.toString();
    }

    // The plain row count skips the propagation of the MCV lists, which keeps the row count of the estimate. Every
    // propagation of the estimates with and without the MCV lists is checked to keep it, and the plain row count is
    // compared with the row count of the estimate without them.
    @Test
    public void testPlainRowCountIsTheRowCountOfTheEstimate() {
        AtomicLong propagations = new AtomicLong();
        new MockUp<McvStatisticsPropagation>() {
            @Mock
            Statistics filter(Invocation invocation, ScalarOperator predicate, Statistics input, Statistics output) {
                Statistics propagated = invocation.proceed();
                Assertions.assertEquals(bits(output.getOutputRowCount()), bits(propagated.getOutputRowCount()),
                        predicate.toString());
                propagations.incrementAndGet();
                return propagated;
            }
        };
        Random random = new Random(20261014);
        int narrowed = 0;
        for (int round = 0; round < 40; round++) {
            Statistics statistics = statistics(random, round % 4 == 0);
            for (int i = 0; i < 50; i++) {
                ScalarOperator predicate = predicate(random, 0);
                double estimated = PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics, false)
                        .getOutputRowCount();
                Assertions.assertEquals(bits(estimated),
                        bits(PredicateStatisticsCalculator.plainRowCount(predicate, statistics)), predicate.toString());
                if (PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics, true).getOutputRowCount()
                        != statistics.getOutputRowCount()) {
                    narrowed++;
                }
            }
        }
        // The predicates must narrow the rows and reach the propagation, or the comparison proves nothing.
        Assertions.assertTrue(narrowed > 1000, Integer.toString(narrowed));
        Assertions.assertTrue(propagations.get() > 10_000, Long.toString(propagations.get()));
    }
}
