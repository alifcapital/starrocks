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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** String ranges use measured bucket masses and a midpoint rank inside the boundary bucket. */
public final class StringHistogramEstimator {
    private StringHistogramEstimator() {}

    public static long rows(Histogram histogram) {
        long tail = histogram.getBuckets().isEmpty() ? 0
                : histogram.getBuckets().get(histogram.getBuckets().size() - 1).getCount();
        return tail + histogram.getMcvDistribution().getTotalRows();
    }

    public static double pointRows(Histogram histogram, String value) {
        Long head = histogram.getMCV().get(value);
        if (head != null) {
            return head;
        }
        List<Bucket> buckets = histogram.getBuckets();
        int low = 0;
        int high = buckets.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (StringBucket.compare(((StringBucket) buckets.get(mid)).getUpperString(), value) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        // Adjacent intervals may share a boundary or have zero mass. Retain first-positive semantics.
        for (int i = low; i < buckets.size(); i++) {
            StringBucket bucket = (StringBucket) buckets.get(i);
            if (StringBucket.compare(bucket.getLowerString(), value) > 0) {
                break;
            }
            long previous = i == 0 ? 0 : buckets.get(i - 1).getCount();
            double count = bucket.pointRows(value, bucket.getCount() - previous);
            if (count > 0) {
                return count;
            }
        }
        return 0;
    }

    public static Histogram filter(Histogram histogram, String value, BinaryType op) {
        Map<String, Long> head = new LinkedHashMap<>();
        histogram.getMCV().forEach((text, count) -> {
            int cmp = StringBucket.compare(text, value);
            if (matches(cmp, op)) {
                head.put(text, count);
            }
        });
        boolean less = op == BinaryType.LT || op == BinaryType.LE;
        boolean inclusive = op == BinaryType.LE || op == BinaryType.GE;
        boolean inHead = histogram.getMCV().containsKey(value);
        List<Bucket> buckets = new ArrayList<>();
        long previous = 0;
        long cumulative = 0;
        for (Bucket raw : histogram.getBuckets()) {
            StringBucket b = (StringBucket) raw;
            long mass = b.getCount() - previous;
            previous = b.getCount();
            long below = Math.round(b.lessRows(value, less ? inclusive : !inclusive, mass, inHead));
            long count = Math.max(0, Math.min(mass, less ? below : mass - below));
            if (count == 0) {
                continue;
            }
            cumulative += count;
            String low = b.getLowerString();
            String high = b.getUpperString();
            boolean lowInclusive = b.isLowerInclusive();
            boolean highInclusive = b.isUpperInclusive();
            long repeats = b.getUpperRepeats();
            if (less && StringBucket.compare(value, high) <= 0) {
                high = value;
                highInclusive = inclusive && !inHead && b.pointRows(value, mass) > 0;
                repeats = highInclusive ? Math.min(count, Math.round(b.pointRows(value, mass))) : 0;
            } else if (!less && StringBucket.compare(value, low) >= 0) {
                low = value;
                lowInclusive = inclusive && !inHead && b.pointRows(value, mass) > 0;
            }
            long ndv = Math.min(count, Math.max(1, (long) Math.ceil(b.getDistinctCount().orElse(1L)
                    * count / (double) Math.max(1, mass))));
            buckets.add(new StringBucket(low, high, cumulative, repeats, ndv, lowInclusive, highInclusive));
        }
        return Histogram.forStrings(buckets, head);
    }

    private static boolean matches(int cmp, BinaryType op) {
        return switch (op) {
            case LT -> cmp < 0;
            case LE -> cmp <= 0;
            case GT -> cmp > 0;
            case GE -> cmp >= 0;
            default -> throw new IllegalArgumentException("Not a range predicate: " + op);
        };
    }

    public static Statistics comparison(Optional<ColumnRefOperator> column, ColumnStatistic statistic,
                                        BinaryType op, String value, Statistics input) {
        Histogram original = statistic.getHistogram();
        Histogram filtered;
        double matched;
        double distinct;
        if (op == BinaryType.EQ || op == BinaryType.EQ_FOR_NULL || op == BinaryType.NE) {
            double equal = pointRows(original, value);
            matched = op == BinaryType.NE ? rows(original) - equal : equal;
            distinct = op == BinaryType.NE ? Math.max(0, statistic.getDistinctValuesCount() - (equal > 0 ? 1 : 0))
                    : equal > 0 ? 1 : 0;
            // A removed interior point leaves a hole; do not retain the unfiltered distribution.
            filtered = op == BinaryType.NE ? null
                    : Histogram.forStrings(List.of(), equal > 0 ? Map.of(value, Math.max(1L, Math.round(equal))) : Map.of());
        } else {
            filtered = filter(original, value, op);
            matched = rows(filtered);
            distinct = filtered.getMCV().size() + filtered.getBuckets().stream()
                    .mapToLong(b -> b.getDistinctCount().orElse(0L)).sum();
        }
        return result(column, statistic, input, matched, distinct, filtered);
    }

    public static Statistics prefix(ColumnRefOperator column, ColumnStatistic statistic,
                                    String prefix, Statistics input) {
        Histogram filtered = filter(statistic.getHistogram(), prefix, BinaryType.GE);
        int[] points = prefix.codePoints().toArray();
        for (int i = points.length - 1; i >= 0; i--) {
            if (points[i] < Character.MAX_CODE_POINT) {
                int successor = points[i] + 1;
                if (successor == Character.MIN_SURROGATE) {
                    successor = Character.MAX_SURROGATE + 1;
                }
                String upper = new String(points, 0, i) + new String(Character.toChars(successor));
                filtered = filter(filtered, upper, BinaryType.LT);
                break;
            }
        }
        double distinct = filtered.getMCV().size() + filtered.getBuckets().stream()
                .mapToLong(b -> b.getDistinctCount().orElse(0L)).sum();
        return result(Optional.of(column), statistic, input, rows(filtered), distinct, filtered);
    }

    public static Statistics in(ColumnRefOperator column, ColumnStatistic statistic,
                                List<ConstantOperator> constants, boolean negated, Statistics input) {
        Map<String, Long> selected = new LinkedHashMap<>();
        double matched = 0;
        for (ConstantOperator constant : constants) {
            if (constant.isNull()) {
                if (negated) {
                    return result(Optional.of(column), statistic, input, 0, 0, Histogram.forStrings(List.of(), Map.of()));
                }
                continue;
            }
            String value = constant.getVarchar();
            if (!selected.containsKey(value)) {
                double count = pointRows(statistic.getHistogram(), value);
                if (count > 0) {
                    selected.put(value, Math.max(1L, Math.round(count)));
                    matched += count;
                }
            }
        }
        double distinct = selected.size();
        if (negated) {
            matched = rows(statistic.getHistogram()) - matched;
            distinct = Math.max(0, statistic.getDistinctValuesCount() - distinct);
        }
        return result(Optional.of(column), statistic, input, matched, distinct,
                negated ? null : Histogram.forStrings(List.of(), selected));
    }

    private static Statistics result(Optional<ColumnRefOperator> column, ColumnStatistic statistic, Statistics input,
                                     double matched, double distinct, Histogram filtered) {
        double share = Math.max(0, Math.min(1, matched / Math.max(1, rows(statistic.getHistogram()))));
        double count = input.getOutputRowCount() * (1 - statistic.getNullsFraction()) * share;
        ColumnStatistic updated = ColumnStatistic.buildFrom(statistic).setHistogram(filtered).setNullsFraction(0)
                .setDistinctValuesCount(distinct).build();
        Statistics.Builder builder = Statistics.buildFrom(input).setOutputRowCount(count);
        column.ifPresent(c -> builder.addColumnStatistic(c, updated));
        return builder.build();
    }
}
